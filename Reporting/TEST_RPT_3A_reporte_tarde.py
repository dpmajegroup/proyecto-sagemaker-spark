import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pyarrow==17.0.0"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "numpy", "pandas"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "awswrangler[redshift]", "--no-build-isolation"])

import os
import io
import boto3
import pytz
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import awswrangler as wr
from datetime import datetime, timedelta

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
my_session = boto3.Session(region_name="us-east-2")

BUCKET_BACKUP = "aje-analytics-ps-backup"
tz_lima = pytz.timezone("America/Lima")
fecha_tomorrow = (datetime.now(tz_lima) + timedelta(days=1)).strftime("%Y-%m-%d")

# =============================================================================
# PAÍSES QUE DEPENDEN DE ARCHIVO EXTERNO (subido ~5pm)
# Este reporte corre a las 5:30pm como complemento del reporte principal (2pm).
# Agregar aquí futuros países que requieran el input externo.
# =============================================================================
PAISES_TARDE = {
    "Nicaragua": f"s3://{BUCKET_BACKUP}/PS_Nicaragua/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
    # "Colombia": f"s3://{BUCKET_BACKUP}/PS_Colombia/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",  # Desactivado hasta salida a producción
}

# Reglas especiales por país
# Colombia: Compania siempre es 1 dígito (no aplicar zfill(4))
PAISES_COMPANIA_1_DIGITO = ["CO"]

# Ruta del consolidado principal (subido por RPT_1 a las 2pm)
S3_PATH_CONSOLIDADO = "s3://aje-prd-pedido-sugerido-orders-s3/PE/pedidos/base_pedidos.csv"

# Credenciales correo
REMITENTE = "david.porta@ajegroup.com"
PASSWORD = "wrqy dwga dbbb wght"
DESTINATARIOS = [
    "david.porta@ajegroup.com",
    "wilmer.rodriguez@ajegroup.com",
    "masaru.gonzales@ajegroup.com",
    "gherald.barzola@ajegroup.com",
    "jorge.delgado.pe@csoluciones.pe",
    "melissa.cotrina@ajegroup.com"
]


def leer_archivo_s3(ruta, nombre):
    """Lee un CSV desde S3 con manejo de errores."""
    try:
        df = wr.s3.read_csv(ruta, boto3_session=my_session)
        print(f"  {nombre}: {df.shape[0]} filas")
        return df
    except Exception as e:
        print(f"  {nombre}: No encontrado ({e})")
        return pd.DataFrame()


def cargar_paises_tarde():
    """Lee los backups de los países que corren en horario tarde."""
    print(f"Cargando recomendaciones TARDE para fecha {fecha_tomorrow}...")
    dfs = []

    for nombre, ruta in PAISES_TARDE.items():
        df = leer_archivo_s3(ruta, f"PS {nombre}")
        if not df.empty:
            if "tipoRecomendacion" not in df.columns:
                df["tipoRecomendacion"] = df.groupby(["Pais", "Compania", "Sucursal", "Cliente"]).cumcount().apply(lambda x: f"PS{x+1}")
                df["ultFecha"] = ''
                df["Destacar"] = "true"
            dfs.append(df)

    if not dfs:
        print("No se encontraron archivos de ningún país tarde.")
        return pd.DataFrame()

    # Concatenar países tarde
    final_tarde = pd.concat(dfs, ignore_index=True)

    # Estandarizar formatos
    # Colombia: Compania siempre 1 dígito (no zfill)
    mask_compania_1 = final_tarde["Pais"].isin(PAISES_COMPANIA_1_DIGITO)
    final_tarde.loc[~mask_compania_1, "Compania"] = final_tarde.loc[~mask_compania_1, "Compania"].astype(str).str.zfill(4)
    final_tarde.loc[mask_compania_1, "Compania"] = final_tarde.loc[mask_compania_1, "Compania"].astype(str).str.strip().str[:1]
    final_tarde["Sucursal"] = final_tarde["Sucursal"].astype(str).str.zfill(2)

    # Seleccionar 12 columnas en orden
    cols = ["Pais", "Compania", "Sucursal", "Cliente", "Modulo", "Producto", "Cajas", "Unidades", "Fecha", "tipoRecomendacion", "ultFecha", "Destacar"]
    for c in cols:
        if c not in final_tarde.columns:
            final_tarde[c] = ''
    final_tarde = final_tarde[cols]

    print(f"\nTotal países tarde: {final_tarde.shape[0]} filas")
    return final_tarde


def cargar_consolidado_existente():
    """Lee el consolidado ya subido por el reporte principal (RPT_1)."""
    print("Cargando consolidado existente (subido a las 2pm)...")
    try:
        df = wr.s3.read_csv(S3_PATH_CONSOLIDADO, boto3_session=my_session)
        print(f"  Consolidado existente: {df.shape[0]} filas")
        return df
    except Exception as e:
        print(f"  No se pudo leer consolidado existente: {e}")
        return pd.DataFrame()


def generar_metricas(final):
    """Genera métricas por país, compañía, sucursal."""
    final["cliente_unico"] = final["Pais"].astype(str) + "|" + final["Compania"].astype(str) + "|" + final["Sucursal"].astype(str) + "|" + final["Cliente"].astype(str)
    final["tipo"] = final["tipoRecomendacion"].str[:2]

    # Detalle por País, Compañía, Sucursal
    detalle = final.groupby(["Pais", "Compania", "Sucursal"]).agg(
        clientes=("cliente_unico", "nunique"),
        recomendaciones=("Producto", "count"),
        skus_unicos=("Producto", "nunique"),
    ).reset_index()
    detalle["prom_prod_cliente"] = (detalle["recomendaciones"] / detalle["clientes"]).round(2)

    # Desglose por tipo de recomendación
    tipo_rec = final.groupby(["Pais", "Compania", "Sucursal", "tipo"]).agg(
        clientes=("cliente_unico", "nunique"),
        recomendaciones=("Producto", "count"),
    ).reset_index()

    # Resumen por País
    resumen_pais = detalle.groupby("Pais").agg(
        clientes=("clientes", "sum"),
        recomendaciones=("recomendaciones", "sum"),
        skus_unicos=("skus_unicos", "max"),
    ).reset_index()
    resumen_pais["prom_prod_cliente"] = (resumen_pais["recomendaciones"] / resumen_pais["clientes"]).round(2)

    # Resumen por País, Compañía y Tipo
    resumen_cia_tipo = final.groupby(["Pais", "Compania", "tipo"]).agg(
        clientes=("cliente_unico", "nunique"),
        recomendaciones=("Producto", "count"),
    ).reset_index()

    return resumen_pais, resumen_cia_tipo, detalle, tipo_rec


def construir_html(resumen_pais, resumen_cia_tipo, detalle, tipo_rec, paises_incluidos):
    """Construye el cuerpo HTML del correo."""

    def df_to_html_table(df):
        return df.to_html(index=False, border=1, classes="table", justify="center")

    lista_paises = ", ".join(paises_incluidos)

    html = f"""
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; font-size: 13px; }}
        .table {{ border-collapse: collapse; width: 100%; font-size: 12px; }}
        .table th {{ background-color: #2980b9; color: white; padding: 8px; text-align: center; }}
        .table td {{ padding: 6px; text-align: center; border: 1px solid #ddd; }}
        .table tr:nth-child(even) {{ background-color: #f2f2f2; }}
        h2 {{ color: #333; }}
        h3 {{ color: #555; }}
        .info {{ background-color: #dfe6e9; padding: 10px; border-radius: 5px; margin-bottom: 15px; }}
    </style>
    </head>
    <body>
    <h2>📊 Reporte Tarde - Pedido Sugerido (Países con input externo)</h2>
    <p>Fecha de recomendaciones: <b>{fecha_tomorrow}</b></p>
    <p>Países incluidos: <b>{lista_paises}</b></p>

    <div class="info">
        <b>Nota:</b> Este reporte complementa al reporte principal de las 2pm.
        Incluye países que dependen de un archivo externo subido a las 5pm.
    </div>

    <h3>1. Resumen por País</h3>
    {df_to_html_table(resumen_pais)}

    <h3>2. Resumen por País, Compañía y Tipo</h3>
    {df_to_html_table(resumen_cia_tipo)}

    <h3>3. Detalle por País, Compañía y Sucursal</h3>
    {df_to_html_table(detalle)}

    <h3>4. Desglose por Tipo de Recomendación (PR/PS/PE)</h3>
    {df_to_html_table(tipo_rec)}

    <br>
    <p><i>Este correo fue generado automáticamente. No responder.</i></p>
    </body>
    </html>
    """
    return html


def enviar_correo(html_body):
    """Envía el correo con el reporte tarde."""
    print("Enviando correo...")
    msg = MIMEMultipart()
    msg["From"] = REMITENTE
    msg["To"] = ", ".join(DESTINATARIOS)
    msg["Subject"] = f"📊 Reporte Pedido Sugerido - {fecha_tomorrow}"
    msg.attach(MIMEText(html_body, "html"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(REMITENTE, PASSWORD)
        server.sendmail(REMITENTE, DESTINATARIOS, msg.as_string())
        server.quit()
        print("Correo enviado exitosamente.")
    except smtplib.SMTPException as e:
        print(f"Error SMTP al enviar correo: {e}")
    except Exception as e:
        print(f"Error general al enviar correo: {e}")


def guardar_backup_tarde(final_tarde):
    """Guarda el consolidado tarde en S3 como backup."""
    s3_path = f"s3://{BUCKET_BACKUP}/Output/0_Final_PS/base_pedidos_tarde_{fecha_tomorrow}.csv"
    wr.s3.to_csv(final_tarde, s3_path, index=False, boto3_session=my_session)
    print(f"Backup tarde guardado en {s3_path}")


def main():
    print("--- INICIANDO REPORTE TARDE (PAÍSES CON INPUT EXTERNO) ---")

    # 1. Cargar países tarde
    final_tarde = cargar_paises_tarde()
    if final_tarde.empty:
        print("No hay datos de países tarde para reportar.")
        return

    # 2. Guardar backup de países tarde
    guardar_backup_tarde(final_tarde)

    # 3. Leer consolidado existente (subido por RPT_1 a las 2pm)
    consolidado_existente = cargar_consolidado_existente()

    # 4. Unir países tarde al consolidado y resubir
    if not consolidado_existente.empty:
        consolidado_final = pd.concat([consolidado_existente, final_tarde], ignore_index=True)
    else:
        consolidado_final = final_tarde

    # Resubir consolidado completo (ahora incluye países tarde)
    wr.s3.to_csv(consolidado_final, S3_PATH_CONSOLIDADO, index=False, boto3_session=my_session)
    print(f"Consolidado actualizado con países tarde: {consolidado_final.shape[0]} filas")
    print(f"  Subido a {S3_PATH_CONSOLIDADO}")

    # 5. Generar métricas solo de países tarde (para el correo)
    paises_incluidos = list(PAISES_TARDE.keys())
    resumen_pais, resumen_cia_tipo, detalle, tipo_rec = generar_metricas(final_tarde)

    # 6. Construir HTML y enviar correo
    html_body = construir_html(resumen_pais, resumen_cia_tipo, detalle, tipo_rec, paises_incluidos)
    enviar_correo(html_body)

    print("--- REPORTE TARDE FINALIZADO ---")


if __name__ == "__main__":
    main()
