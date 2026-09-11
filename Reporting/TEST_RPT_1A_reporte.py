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
# CONFIGURACIÓN DE PAÍSES Y TIPOS DE RECOMENDACIÓN
# Comentar/descomentar para activar/desactivar cada país o tipo
# =============================================================================
PAISES_CONFIG = {
    "Panama": {
        "PS": f"s3://{BUCKET_BACKUP}/PS_Panama/Output/PS_todo_panama/D_base_pedidos_{fecha_tomorrow}.csv",
        "PR": f"s3://{BUCKET_BACKUP}/Pedido_Recurrente/Panama/Output/recu_base_pedidos_{fecha_tomorrow}.csv",
        "PE": f"s3://{BUCKET_BACKUP}/Pedido_Estrategico/Panama/Output/estr_base_pedidos_{fecha_tomorrow}.csv",
    },
    "Peru": {
        "PS": f"s3://{BUCKET_BACKUP}/PS_Peru/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
        "PR": f"s3://{BUCKET_BACKUP}/Pedido_Recurrente/Peru/Output/recu_base_pedidos_{fecha_tomorrow}.csv",
        "PE": f"s3://{BUCKET_BACKUP}/Pedido_Estrategico/Peru/Output/estr_base_pedidos_{fecha_tomorrow}.csv",
    },
    "Ecuador": {
        "PS": f"s3://{BUCKET_BACKUP}/PS_Ecuador/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
        "PR": f"s3://{BUCKET_BACKUP}/Pedido_Recurrente/Ecuador/Output/recu_base_pedidos_{fecha_tomorrow}.csv",
        "PE": f"s3://{BUCKET_BACKUP}/Pedido_Estrategico/Ecuador/Output/estr_base_pedidos_{fecha_tomorrow}.csv",
        "PS_ECO": f"s3://{BUCKET_BACKUP}/Econoredes/Ecuador/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
    },
    "CostaRica": {
        "PS": f"s3://{BUCKET_BACKUP}/PS_CostaRica/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
    },
    # "Mexico": {
    #     "PS": f"s3://{BUCKET_BACKUP}/PS_Mexico/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
    # },
    "Guatemala": {
        "PS": f"s3://{BUCKET_BACKUP}/PS_Guatemala/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
    },
    "Bolivia": {
        "PS": f"s3://{BUCKET_BACKUP}/PS_Bolivia/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
        "PR": f"s3://{BUCKET_BACKUP}/Pedido_Recurrente/Bolivia/Output/recu_base_pedidos_{fecha_tomorrow}.csv",
        "PE": f"s3://{BUCKET_BACKUP}/Pedido_Estrategico/Bolivia/Output/estr_base_pedidos_{fecha_tomorrow}.csv",
    },
    # Nicaragua y Colombia van en reporte 3A (tarde)
}

# Credenciales correo
REMITENTE = "david.porta@ajegroup.com"
PASSWORD = "wrqy dwga dbbb wght"
DESTINATARIOS = [
    "david.porta@ajegroup.com",
    "lizeth.gil@ajegroup.com",
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
    """Lee los backups de todos los países configurados en PAISES_CONFIG."""
    print(f"Cargando recomendaciones para fecha {fecha_tomorrow}...")
    dfs = []

    # Mapa de prefijo tipo para tipoRecomendacion default
    tipo_prefijo = {"PS": "PS", "PS_ECO": "PS", "PR": "PR", "PE": "PE"}

    for pais, tipos in PAISES_CONFIG.items():
        for tipo, ruta in tipos.items():
            label = f"{tipo} {pais}"
            df = leer_archivo_s3(ruta, label)
            if not df.empty:
                # Asegurar 12 columnas
                prefijo = tipo_prefijo.get(tipo, "PS")
                if "tipoRecomendacion" not in df.columns:
                    df["tipoRecomendacion"] = df.groupby(["Pais", "Compania", "Sucursal", "Cliente"]).cumcount().apply(lambda x: f"{prefijo}{x+1}")
                if "ultFecha" not in df.columns:
                    df["ultFecha"] = ''
                df["ultFecha"] = df["ultFecha"].fillna('')
                if "Destacar" not in df.columns:
                    df["Destacar"] = "true"
                dfs.append(df)

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

    # Resumen por País, Compañía y Tipo (tabla compacta para validación diaria)
    resumen_cia_tipo = final.groupby(["Pais", "Compania", "tipo"]).agg(
        clientes=("cliente_unico", "nunique"),
        recomendaciones=("Producto", "count"),
    ).reset_index()

    return resumen_pais, resumen_cia_tipo, detalle, tipo_rec


def construir_html(resumen_pais, resumen_cia_tipo, detalle, tipo_rec):
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
    """Envía el correo con el reporte."""
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
    resumen_pais, resumen_cia_tipo, detalle, tipo_rec = generar_metricas(final)

    # 4. Construir HTML y enviar correo
    html_body = construir_html(resumen_pais, resumen_cia_tipo, detalle, tipo_rec)
    enviar_correo(html_body)

    print("--- REPORTE FINALIZADO ---")


if __name__ == "__main__":
    main()
