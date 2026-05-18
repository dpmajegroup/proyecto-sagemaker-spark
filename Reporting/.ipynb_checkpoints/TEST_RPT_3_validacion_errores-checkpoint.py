import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pyarrow==17.0.0"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "numpy", "pandas"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "awswrangler[redshift]", "--no-build-isolation"])

import os
import io
import zipfile
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

# --- CONFIGURACIÓN ---
BUCKET_ERRORES = "aje-prd-pedido-sugerido-orders-s3"
PREFIX_ERRORES = "PE/errores/consulta/"

BUCKET_BACKUP = "aje-analytics-ps-backup"

tz_lima = pytz.timezone("America/Lima")
fecha_hoy = datetime.now(tz_lima).strftime("%Y-%m-%d")
fecha_tomorrow = (datetime.now(tz_lima) + timedelta(days=1)).strftime("%Y-%m-%d")

# Ruta del consolidado subido por el reporte
S3_PATH_CONSOLIDADO = "s3://aje-prd-pedido-sugerido-orders-s3/PE/pedidos/base_pedidos.csv"

# Credenciales correo
REMITENTE = "david.porta@ajegroup.com"
PASSWORD = "vzoa fquz zycz cvfn"
DESTINATARIOS = [
    "david.porta@ajegroup.com",
    "wilmer.rodriguez@ajegroup.com",
    "masaru.gonzales@ajegroup.com",
    "gherald.barzola@ajegroup.com",
    "jorge.delgado.pe@csoluciones.pe",
    "melissa.cotrina@ajegroup.com"
]


def buscar_zip_mas_reciente_hoy():
    """Busca el archivo zip con última fecha de modificación de hoy en la ruta de errores."""
    print(f"Buscando archivo de errores modificado hoy ({fecha_hoy})...")
    s3 = my_session.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    archivos_hoy = []

    for page in paginator.paginate(Bucket=BUCKET_ERRORES, Prefix=PREFIX_ERRORES):
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            key = obj["Key"]
            if not key.endswith(".zip"):
                continue
            # Filtrar por fecha de modificación = hoy
            last_modified = obj["LastModified"].astimezone(tz_lima).strftime("%Y-%m-%d")
            if last_modified == fecha_hoy:
                archivos_hoy.append(obj)

    if not archivos_hoy:
        print("No se encontraron archivos zip modificados hoy.")
        return None

    # Tomar el más reciente
    archivos_hoy.sort(key=lambda x: x["LastModified"], reverse=True)
    elegido = archivos_hoy[0]
    print(f"Archivo encontrado: {elegido['Key']} (modificado: {elegido['LastModified']})")
    return elegido["Key"]


def leer_csv_desde_zip(zip_key):
    """Descarga el zip desde S3, extrae el CSV y lo lee."""
    print(f"Descargando y leyendo {zip_key}...")
    s3 = my_session.client("s3")

    response = s3.get_object(Bucket=BUCKET_ERRORES, Key=zip_key)
    zip_bytes = response["Body"].read()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            print("El zip no contiene archivos CSV.")
            return pd.DataFrame()

        csv_name = csv_names[0]
        print(f"  Leyendo: {csv_name}")
        with zf.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file, sep=";")

    print(f"  Errores cargados: {df.shape[0]} filas")
    return df


def cargar_consolidado_subido():
    """Lee el consolidado de pedidos subidos para calcular % de rechazo."""
    print("Cargando consolidado de pedidos subidos...")
    try:
        df = wr.s3.read_csv(S3_PATH_CONSOLIDADO, boto3_session=my_session)
        print(f"  Consolidado: {df.shape[0]} filas")
        return df
    except Exception as e:
        print(f"  No se pudo leer consolidado: {e}")
        return pd.DataFrame()


def generar_metricas(df_errores, df_subido):
    """Genera tablas de resumen de errores y % de rechazo."""
    # Estandarizar formatos
    df_errores["Compania"] = df_errores["Compania"].astype(str).str.strip().str.zfill(4)
    df_errores["Sucursal"] = df_errores["Sucursal"].astype(str).str.strip().str.zfill(2)
    df_errores["Cliente"] = df_errores["Cliente"].astype(str).str.strip()
    df_errores["Pais"] = df_errores["Pais"].astype(str).str.strip()
    df_errores["Tipo"] = df_errores["Tipo"].astype(str).str.strip()
    df_errores["Mensaje"] = df_errores["Mensaje"].astype(str).str.strip()

    # Cliente único para errores
    df_errores["cliente_unico"] = (
        df_errores["Pais"] + "|" + df_errores["Compania"] + "|" +
        df_errores["Sucursal"] + "|" + df_errores["Cliente"]
    )

    # --- Tabla 1: Resumen por País ---
    resumen_pais = df_errores.groupby("Pais").agg(
        clientes_rechazados=("cliente_unico", "nunique"),
        total_rechazos=("Cliente", "count"),
    ).reset_index()

    # --- Tabla 2: Detalle por País, Compañía, Sucursal ---
    detalle = df_errores.groupby(["Pais", "Compania", "Sucursal"]).agg(
        clientes_rechazados=("cliente_unico", "nunique"),
        total_rechazos=("Cliente", "count"),
    ).reset_index()

    # --- Tabla 3: Desglose por Tipo y Mensaje ---
    tipo_mensaje = df_errores.groupby(["Pais", "Tipo", "Mensaje"]).agg(
        clientes_rechazados=("cliente_unico", "nunique"),
        total_rechazos=("Cliente", "count"),
    ).reset_index()

    # --- Calcular % de rechazo cruzando con lo subido ---
    if not df_subido.empty:
        df_subido["Compania"] = df_subido["Compania"].astype(str).str.strip().str.zfill(4)
        df_subido["Sucursal"] = df_subido["Sucursal"].astype(str).str.strip().str.zfill(2)
        df_subido["Cliente"] = df_subido["Cliente"].astype(str).str.strip()
        df_subido["Pais"] = df_subido["Pais"].astype(str).str.strip()
        df_subido["cliente_unico"] = (
            df_subido["Pais"] + "|" + df_subido["Compania"] + "|" +
            df_subido["Sucursal"] + "|" + df_subido["Cliente"]
        )

        # Clientes subidos por país
        subidos_pais = df_subido.groupby("Pais").agg(
            clientes_subidos=("cliente_unico", "nunique"),
        ).reset_index()

        resumen_pais = resumen_pais.merge(subidos_pais, on="Pais", how="left")
        resumen_pais["clientes_subidos"] = resumen_pais["clientes_subidos"].fillna(0).astype(int)
        resumen_pais["pct_rechazo"] = (
            (resumen_pais["clientes_rechazados"] / resumen_pais["clientes_subidos"]) * 100
        ).round(2).fillna(0)
        resumen_pais["pct_rechazo"] = resumen_pais["pct_rechazo"].astype(str) + "%"

        # Detalle también con % de rechazo
        subidos_detalle = df_subido.groupby(["Pais", "Compania", "Sucursal"]).agg(
            clientes_subidos=("cliente_unico", "nunique"),
        ).reset_index()

        detalle = detalle.merge(subidos_detalle, on=["Pais", "Compania", "Sucursal"], how="left")
        detalle["clientes_subidos"] = detalle["clientes_subidos"].fillna(0).astype(int)
        detalle["pct_rechazo"] = (
            (detalle["clientes_rechazados"] / detalle["clientes_subidos"]) * 100
        ).round(2).fillna(0)
        detalle["pct_rechazo"] = detalle["pct_rechazo"].astype(str) + "%"
    else:
        resumen_pais["clientes_subidos"] = "N/A"
        resumen_pais["pct_rechazo"] = "N/A"
        detalle["clientes_subidos"] = "N/A"
        detalle["pct_rechazo"] = "N/A"

    return resumen_pais, detalle, tipo_mensaje


def construir_html(resumen_pais, detalle, tipo_mensaje, zip_key):
    """Construye el cuerpo HTML del correo."""

    def df_to_html_table(df):
        return df.to_html(index=False, border=1, classes="table", justify="center")

    total_rechazos = resumen_pais["total_rechazos"].sum() if "total_rechazos" in resumen_pais.columns else 0
    total_clientes_rech = resumen_pais["clientes_rechazados"].sum() if "clientes_rechazados" in resumen_pais.columns else 0

    html = f"""
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; font-size: 13px; }}
        .table {{ border-collapse: collapse; width: 100%; font-size: 12px; }}
        .table th {{ background-color: #e74c3c; color: white; padding: 8px; text-align: center; }}
        .table td {{ padding: 6px; text-align: center; border: 1px solid #ddd; }}
        .table tr:nth-child(even) {{ background-color: #f2f2f2; }}
        h2 {{ color: #333; }}
        h3 {{ color: #555; }}
        .resumen {{ background-color: #ffeaa7; padding: 10px; border-radius: 5px; margin-bottom: 15px; }}
    </style>
    </head>
    <body>
    <h2>⚠️ Reporte de Validación - Errores del Lambda</h2>
    <p>Fecha de recomendaciones: <b>{fecha_tomorrow}</b></p>
    <p>Archivo de errores: <code>{zip_key}</code></p>

    <div class="resumen">
        <b>Resumen general:</b> {total_rechazos} rechazos totales, {total_clientes_rech} clientes afectados.
    </div>

    <h3>1. Resumen por País (con % de rechazo a nivel cliente)</h3>
    {df_to_html_table(resumen_pais)}

    <h3>2. Detalle por País, Compañía y Sucursal</h3>
    {df_to_html_table(detalle)}

    <h3>3. Desglose por Tipo de Error y Mensaje</h3>
    {df_to_html_table(tipo_mensaje)}

    <br>
    <p><i>Este correo fue generado automáticamente. No responder.</i></p>
    </body>
    </html>
    """
    return html


def enviar_correo(html_body):
    """Envía el correo con el reporte de errores."""
    print("Enviando correo de validación...")
    msg = MIMEMultipart()
    msg["From"] = REMITENTE
    msg["To"] = ", ".join(DESTINATARIOS)
    msg["Subject"] = f"⚠️ Validación Errores Lambda - Pedido Sugerido - {fecha_tomorrow}"
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


def main():
    print("--- INICIANDO VALIDACIÓN DE ERRORES DEL LAMBDA ---")

    # 1. Buscar el zip más reciente de hoy
    zip_key = buscar_zip_mas_reciente_hoy()
    if zip_key is None:
        print("No hay archivo de errores para procesar. Finalizando.")
        return

    # 2. Leer el CSV dentro del zip
    df_errores = leer_csv_desde_zip(zip_key)
    if df_errores.empty:
        print("El archivo de errores está vacío. Finalizando.")
        return

    # 3. Cargar consolidado de pedidos subidos
    df_subido = cargar_consolidado_subido()

    # 4. Generar métricas
    resumen_pais, detalle, tipo_mensaje = generar_metricas(df_errores, df_subido)

    # 5. Construir HTML y enviar correo
    html_body = construir_html(resumen_pais, detalle, tipo_mensaje, zip_key)
    enviar_correo(html_body)

    print("--- VALIDACIÓN FINALIZADA ---")


if __name__ == "__main__":
    main()
