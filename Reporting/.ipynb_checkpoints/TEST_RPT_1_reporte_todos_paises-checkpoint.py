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

# Rutas de backup de cada país (Pedido Sugerido)
PAISES_PS = {
    "Panama": f"s3://{BUCKET_BACKUP}/PS_Panama/Output/PS_todo_panama/D_base_pedidos_{fecha_tomorrow}.csv",
    "Peru": f"s3://{BUCKET_BACKUP}/PS_Peru/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
    "Ecuador": f"s3://{BUCKET_BACKUP}/PS_Ecuador/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
    "CostaRica": f"s3://{BUCKET_BACKUP}/PS_CostaRica/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
    "Mexico": f"s3://{BUCKET_BACKUP}/PS_Mexico/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
    "Guatemala": f"s3://{BUCKET_BACKUP}/PS_Guatemala/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
    "Nicaragua": f"s3://{BUCKET_BACKUP}/PS_Nicaragua/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
}

# Ecuador extras
RUTA_EC_ESTRATEGICO = f"s3://{BUCKET_BACKUP}/Pedido_Estrategico/Ecuador/Output/estr_base_pedidos_{fecha_tomorrow}.csv"
RUTA_EC_RECURRENTE = f"s3://{BUCKET_BACKUP}/Pedido_Recurrente/Ecuador/Output/recu_base_pedidos_{fecha_tomorrow}.csv"
RUTA_EC_ECO = f"s3://{BUCKET_BACKUP}/Econoredes/Ecuador/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv"

# Credenciales correo
REMITENTE = "david.porta@ajegroup.com"
PASSWORD = "pxqn kmuw drlq fvmv"
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


def cargar_todos_los_paises():
    """Lee los backups de todos los países y los concatena."""
    print(f"Cargando recomendaciones para fecha {fecha_tomorrow}...")
    dfs = []

    # Pedido Sugerido de cada país
    for nombre, ruta in PAISES_PS.items():
        df = leer_archivo_s3(ruta, f"PS {nombre}")
        if not df.empty:
            # Asegurar 12 columnas
            if "tipoRecomendacion" not in df.columns:
                df["tipoRecomendacion"] = df.groupby(["Pais", "Compania", "Sucursal", "Cliente"]).cumcount().apply(lambda x: f"PS{x+1}")
                df["ultFecha"] = ''
                df["Destacar"] = "true"
            dfs.append(df)

    # Ecuador Econoredes
    df_eco = leer_archivo_s3(RUTA_EC_ECO, "PS Ecuador Econoredes")
    if not df_eco.empty:
        if "tipoRecomendacion" not in df_eco.columns:
            df_eco["tipoRecomendacion"] = df_eco.groupby(["Pais", "Compania", "Sucursal", "Cliente"]).cumcount().apply(lambda x: f"PS{x+1}")
            df_eco["ultFecha"] = ''
            df_eco["Destacar"] = "true"
        dfs.append(df_eco)

    # Ecuador Estratégico
    df_est = leer_archivo_s3(RUTA_EC_ESTRATEGICO, "PE Ecuador Estratégico")
    if not df_est.empty:
        if "ultFecha" not in df_est.columns:
            df_est["ultFecha"] = ''
        if "Destacar" not in df_est.columns:
            df_est["Destacar"] = "true"
        dfs.append(df_est)

    # Ecuador Recurrente
    df_rec = leer_archivo_s3(RUTA_EC_RECURRENTE, "PR Ecuador Recurrente")
    if not df_rec.empty:
        if "Destacar" not in df_rec.columns:
            df_rec["Destacar"] = "true"
        dfs.append(df_rec)

    if not dfs:
        print("No se encontraron archivos de ningún país.")
        return pd.DataFrame()

    # Concatenar todo
    final = pd.concat(dfs, ignore_index=True)

    # Estandarizar formatos
    final["Compania"] = final["Compania"].astype(str).str.zfill(4)
    final["Sucursal"] = final["Sucursal"].astype(str).str.zfill(2)

    # Seleccionar 12 columnas en orden
    cols = ["Pais", "Compania", "Sucursal", "Cliente", "Modulo", "Producto", "Cajas", "Unidades", "Fecha", "tipoRecomendacion", "ultFecha", "Destacar"]
    for c in cols:
        if c not in final.columns:
            final[c] = ''
    final = final[cols]

    print(f"\nTotal consolidado: {final.shape[0]} filas")
    return final


def generar_metricas(final):
    """Genera métricas por país, compañía, sucursal."""
    # Cliente único = combinación Pais + Compania + Sucursal + Cliente
    final["cliente_unico"] = final["Pais"].astype(str) + "|" + final["Compania"].astype(str) + "|" + final["Sucursal"].astype(str) + "|" + final["Cliente"].astype(str)
    final["tipo"] = final["tipoRecomendacion"].str[:2]

    # Detalle por País, Compañía, Sucursal
    detalle = final.groupby(["Pais", "Compania", "Sucursal"]).agg(
        clientes=("cliente_unico", "nunique"),
        recomendaciones=("Producto", "count"),
        skus_unicos=("Producto", "nunique"),
    ).reset_index()
    detalle["prom_prod_cliente"] = (detalle["recomendaciones"] / detalle["clientes"]).round(2)

    # Desglose por tipo de recomendación (PR, PS, PE) para todos los países
    tipo_rec = final.groupby(["Pais", "Compania", "Sucursal", "tipo"]).agg(
        clientes=("cliente_unico", "nunique"),
        recomendaciones=("Producto", "count"),
    ).reset_index()

    # Resumen por País (sumando desde el detalle para evitar conteo incorrecto de clientes)
    resumen_pais = detalle.groupby("Pais").agg(
        clientes=("clientes", "sum"),
        recomendaciones=("recomendaciones", "sum"),
        skus_unicos=("skus_unicos", "max"),
    ).reset_index()
    resumen_pais["prom_prod_cliente"] = (resumen_pais["recomendaciones"] / resumen_pais["clientes"]).round(2)

    return resumen_pais, detalle, tipo_rec


def construir_html(resumen_pais, detalle, tipo_rec):
    """Construye el cuerpo HTML del correo."""

    def df_to_html_table(df):
        return df.to_html(index=False, border=1, classes="table", justify="center")

    html = f"""
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; font-size: 13px; }}
        .table {{ border-collapse: collapse; width: 100%; font-size: 12px; }}
        .table th {{ background-color: #4CAF50; color: white; padding: 8px; text-align: center; }}
        .table td {{ padding: 6px; text-align: center; border: 1px solid #ddd; }}
        .table tr:nth-child(even) {{ background-color: #f2f2f2; }}
        h2 {{ color: #333; }}
        h3 {{ color: #555; }}
    </style>
    </head>
    <body>
    <h2>📊 Reporte Diario - Pedido Sugerido</h2>
    <p>Fecha de recomendaciones: <b>{fecha_tomorrow}</b></p>
    <p>Generado automáticamente por el Pipeline de Pedido Sugerido.</p>

    <h3>1. Resumen por País</h3>
    {df_to_html_table(resumen_pais)}

    <h3>2. Detalle por País, Compañía y Sucursal</h3>
    {df_to_html_table(detalle)}

    <h3>3. Desglose por Tipo de Recomendación (PR/PS/PE)</h3>
    {df_to_html_table(tipo_rec)}

    <br>
    <p><i>Este correo fue generado automáticamente. No responder.</i></p>
    </body>
    </html>
    """
    return html


def enviar_correo(html_body):
    """Envía el correo con el reporte."""
    print("Enviando correo...")
    msg = MIMEMultipart()
    msg["From"] = REMITENTE
    msg["To"] = ", ".join(DESTINATARIOS)
    msg["Subject"] = f"📊 Reporte Pedido Sugerido - Todos los Países - {fecha_tomorrow}"
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


def guardar_consolidado(final):
    """Guarda el archivo consolidado en S3."""
    s3_path = f"s3://{BUCKET_BACKUP}/Output/0_Final_PS/base_pedidos_final_{fecha_tomorrow}.csv"
    wr.s3.to_csv(final, s3_path, index=False, boto3_session=my_session)
    print(f"Consolidado guardado en {s3_path}")


def main():
    print("--- INICIANDO REPORTE DE TODOS LOS PAÍSES ---")

    # 1. Cargar todos los países
    final = cargar_todos_los_paises()
    if final.empty:
        print("No hay datos para reportar.")
        return

    # 2. Guardar consolidado
    guardar_consolidado(final)

    # 2.5 Subir consolidado al bucket de orders (centralizado)
    s3_path_orders = "s3://aje-prd-pedido-sugerido-orders-s3/PE/pedidos/base_pedidos.csv"
    wr.s3.to_csv(final, s3_path_orders, index=False, boto3_session=my_session)
    print(f"Consolidado subido a {s3_path_orders}")

    # 3. Generar métricas
    resumen_pais, detalle, tipo_rec = generar_metricas(final)

    # 4. Construir HTML y enviar correo
    html_body = construir_html(resumen_pais, detalle, tipo_rec)
    enviar_correo(html_body)

    print("--- REPORTE FINALIZADO ---")


if __name__ == "__main__":
    main()
