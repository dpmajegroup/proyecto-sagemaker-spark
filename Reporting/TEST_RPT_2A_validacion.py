import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pyarrow==17.0.0"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "numpy", "pandas"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "awswrangler[redshift]", "--no-build-isolation"])

import os
import io
import time
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

# Credenciales correo (leídas desde S3, no hardcodeadas)
def cargar_credenciales_smtp():
    """Lee REMITENTE y PASSWORD del SMTP desde un JSON en S3.

    Limpia espacios/saltos: el remitente va sin espacios extra y la App Password
    de Gmail se usa sin los espacios con que Google la muestra (login SMTP).
    """
    import json
    s3 = my_session.client("s3")
    obj = s3.get_object(Bucket=BUCKET_BACKUP, Key="smtp.json")
    creds = json.loads(obj["Body"].read().decode("utf-8"))
    remitente = str(creds["REMITENTE"]).strip()
    password = str(creds["PASSWORD"]).strip().replace(" ", "")
    return remitente, password


REMITENTE, PASSWORD = cargar_credenciales_smtp()
DESTINATARIOS = [
    "david.porta@ajegroup.com",
    "lizeth.gil@ajegroup.com",
    "gherald.barzola@ajegroup.com",
    "melissa.cotrina@ajegroup.com",
    "reymundo.jimeno.ve@ajegroup.com",
    "rodrigo.basurto@ajegroup.com"
]

COLUMNAS_ERRORES = ["Pais", "Compania", "Sucursal", "Fecha", "Modulo", "Cliente", "Producto", "Tipo", "Mensaje"]

# Países excluidos de esta validación (se validan en TEST_RPT_4A_validacion_tarde.py)
PAISES_EXCLUIDOS = ["NI"]  # "CO" se agregará cuando salga a producción

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
            last_modified = obj["LastModified"].astimezone(tz_lima).strftime("%Y-%m-%d")
            if last_modified == fecha_hoy:
                archivos_hoy.append(obj)

    if not archivos_hoy:
        print("No se encontraron archivos zip modificados hoy.")
        return None

    archivos_hoy.sort(key=lambda x: x["LastModified"], reverse=True)
    elegido = archivos_hoy[0]
    print(f"Archivo encontrado: {elegido['Key']} (modificado: {elegido['LastModified']})")
    return elegido["Key"]


def leer_csv_desde_zip(zip_key):
    """Descarga el zip desde S3, extrae el CSV y lo lee con parsing robusto."""
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
            lines = csv_file.read().decode("utf-8").splitlines()

    if len(lines) <= 1:
        print("El archivo de errores está vacío.")
        return pd.DataFrame()

    # Parsing robusto: usar maxsplit=8 para que el Mensaje (con ; internos) quede completo
    rows = []
    for line in lines[1:]:  # Saltar header
        parts = line.split(";", maxsplit=8)
        if len(parts) >= 9:
            rows.append(parts[:9])
        elif len(parts) >= 7:
            # Líneas con menos columnas (formato antiguo sin Tipo/Mensaje completo)
            while len(parts) < 9:
                parts.append("")
            rows.append(parts)

    df = pd.DataFrame(rows, columns=COLUMNAS_ERRORES)
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


def clasificar_errores(df_errores):
    """Clasifica errores en 'sin_visita' y 'producto_inactivo' (u otros)."""
    df_errores["Compania"] = df_errores["Compania"].astype(str).str.strip().str.zfill(4)
    df_errores["Sucursal"] = df_errores["Sucursal"].astype(str).str.strip().str.zfill(2)
    df_errores["Cliente"] = df_errores["Cliente"].astype(str).str.strip()
    df_errores["Pais"] = df_errores["Pais"].astype(str).str.strip()

    # Excluir países que se validan en el reporte tarde (RPT_7)
    df_errores = df_errores[~df_errores["Pais"].isin(PAISES_EXCLUIDOS)].reset_index(drop=True)
    print(f"  Errores después de excluir países tarde: {df_errores.shape[0]} filas")

    df_errores["Tipo"] = df_errores["Tipo"].astype(str).str.strip()
    df_errores["Mensaje"] = df_errores["Mensaje"].astype(str).str.strip()
    df_errores["Producto"] = df_errores["Producto"].astype(str).str.strip()

    # Clasificar
    df_errores["categoria_error"] = "otro"
    df_errores.loc[df_errores["Mensaje"].str.contains("no tiene visita", case=False, na=False), "categoria_error"] = "sin_visita"
    df_errores.loc[df_errores["Mensaje"].str.contains("producto inactivo", case=False, na=False), "categoria_error"] = "producto_inactivo"

    # Deduplicar: a nivel (Pais, Compania, Sucursal, Cliente, Producto, categoria_error)
    df_errores = df_errores.drop_duplicates(subset=["Pais", "Compania", "Sucursal", "Cliente", "Producto", "categoria_error"]).reset_index(drop=True)

    return df_errores


def generar_tablas(df_errores, df_subido):
    """Genera las 3 tablas del reporte."""

    # Preparar consolidado subido
    if not df_subido.empty:
        df_subido["Compania"] = df_subido["Compania"].astype(str).str.strip().str.zfill(4)
        df_subido["Sucursal"] = df_subido["Sucursal"].astype(str).str.strip().str.zfill(2)
        df_subido["Cliente"] = df_subido["Cliente"].astype(str).str.strip()
        df_subido["Pais"] = df_subido["Pais"].astype(str).str.strip()
        df_subido["cliente_unico"] = df_subido["Pais"] + "|" + df_subido["Compania"] + "|" + df_subido["Sucursal"] + "|" + df_subido["Cliente"]

    # --- ERRORES SIN VISITA ---
    df_sin_visita = df_errores[df_errores["categoria_error"] == "sin_visita"].copy()
    df_sin_visita["cliente_unico"] = df_sin_visita["Pais"] + "|" + df_sin_visita["Compania"] + "|" + df_sin_visita["Sucursal"] + "|" + df_sin_visita["Cliente"]

    # Tabla 1: Sin visita por País/Compañía
    tabla1 = df_sin_visita.groupby(["Pais", "Compania"]).agg(
        clientes_rechazados=("cliente_unico", "nunique")
    ).reset_index()

    if not df_subido.empty:
        subidos_cia = df_subido.groupby(["Pais", "Compania"]).agg(
            clientes_subidos=("cliente_unico", "nunique")
        ).reset_index()
        tabla1 = tabla1.merge(subidos_cia, on=["Pais", "Compania"], how="left")
        tabla1["clientes_subidos"] = tabla1["clientes_subidos"].fillna(0).astype(int)
        tabla1["pct_rechazo"] = ((tabla1["clientes_rechazados"] / tabla1["clientes_subidos"]) * 100).round(2).fillna(0).astype(str) + "%"
    else:
        tabla1["clientes_subidos"] = "N/A"
        tabla1["pct_rechazo"] = "N/A"

    # Tabla 3: Sin visita por País/Compañía/Sucursal
    tabla3 = df_sin_visita.groupby(["Pais", "Compania", "Sucursal"]).agg(
        clientes_rechazados=("cliente_unico", "nunique")
    ).reset_index()

    if not df_subido.empty:
        subidos_suc = df_subido.groupby(["Pais", "Compania", "Sucursal"]).agg(
            clientes_subidos=("cliente_unico", "nunique")
        ).reset_index()
        tabla3 = tabla3.merge(subidos_suc, on=["Pais", "Compania", "Sucursal"], how="left")
        tabla3["clientes_subidos"] = tabla3["clientes_subidos"].fillna(0).astype(int)
        tabla3["pct_rechazo"] = ((tabla3["clientes_rechazados"] / tabla3["clientes_subidos"]) * 100).round(2).fillna(0).astype(str) + "%"
    else:
        tabla3["clientes_subidos"] = "N/A"
        tabla3["pct_rechazo"] = "N/A"

    # --- ERRORES POR PRODUCTO INACTIVO ---
    df_prod_inactivo = df_errores[df_errores["categoria_error"] == "producto_inactivo"].copy()
    df_prod_inactivo["cliente_unico"] = df_prod_inactivo["Pais"] + "|" + df_prod_inactivo["Compania"] + "|" + df_prod_inactivo["Sucursal"] + "|" + df_prod_inactivo["Cliente"]

    if not df_prod_inactivo.empty:
        tabla2 = df_prod_inactivo.groupby(["Pais", "Compania", "Sucursal", "Producto"]).agg(
            clientes_afectados=("cliente_unico", "nunique")
        ).reset_index()
        tabla2["nota"] = "Solo afecta recs individuales, no al cliente completo"
    else:
        tabla2 = pd.DataFrame(columns=["Pais", "Compania", "Sucursal", "Producto", "clientes_afectados", "nota"])

    # --- OTROS ERRORES (si existen) ---
    df_otros = df_errores[df_errores["categoria_error"] == "otro"].copy()
    if not df_otros.empty:
        df_otros["cliente_unico"] = df_otros["Pais"] + "|" + df_otros["Compania"] + "|" + df_otros["Sucursal"] + "|" + df_otros["Cliente"]
        tabla_otros = df_otros.groupby(["Pais", "Compania", "Sucursal", "Tipo"]).agg(
            clientes=("cliente_unico", "nunique"),
            total=("Cliente", "count")
        ).reset_index()
    else:
        tabla_otros = pd.DataFrame()

    return tabla1, tabla2, tabla3, tabla_otros


def construir_html(tabla1, tabla2, tabla3, tabla_otros, zip_key):
    """Construye el cuerpo HTML del correo."""

    def df_to_html_table(df):
        if df.empty:
            return "<p><i>Sin registros</i></p>"
        return df.to_html(index=False, border=1, classes="table", justify="center")

    total_sin_visita = tabla1["clientes_rechazados"].sum() if not tabla1.empty else 0
    total_prod_inactivo = tabla2["clientes_afectados"].sum() if not tabla2.empty else 0

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
    <p>Archivo: <code>{zip_key}</code></p>

    <div class="resumen">
        <b>Resumen:</b> {total_sin_visita} clientes rechazados por falta de visita | {total_prod_inactivo} clientes con producto inactivo (no pierden todas sus recs).
    </div>

    <h3>1. Clientes rechazados por falta de visita (País/Compañía)</h3>
    {df_to_html_table(tabla1)}

    <h3>2. Productos inactivos rechazados</h3>
    {df_to_html_table(tabla2)}

    <h3>3. Detalle clientes sin visita (País/Compañía/Sucursal)</h3>
    {df_to_html_table(tabla3)}
    """

    if not tabla_otros.empty:
        html += f"""
    <h3>4. Otros errores</h3>
    {df_to_html_table(tabla_otros)}
    """

    html += """
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
    msg["Subject"] = f"⚠️ Validación Errores - Pedido Sugerido - {fecha_tomorrow}"
    msg.attach(MIMEText(html_body, "html"))

    enviar_mensaje_smtp(msg, DESTINATARIOS)


def enviar_mensaje_smtp(msg, destinatarios, max_reintentos=3, espera_seg=5):
    """Envia un mensaje con politica de reintentos que alterna puertos.

    Por cada reintento prueba dos estrategias de conexion:
      1) SSL directo en 465 (mas tolerante en VPC/SageMaker)
      2) STARTTLS en 587
    Si todas fallan, espera 'espera_seg' (con backoff) y reintenta hasta
    'max_reintentos' veces. Devuelve True si logro enviar, False si no.
    """
    estrategias = [("SSL_465", 465), ("STARTTLS_587", 587)]

    for intento in range(1, max_reintentos + 1):
        for nombre, puerto in estrategias:
            server = None
            try:
                if puerto == 465:
                    server = smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30)
                else:
                    server = smtplib.SMTP("smtp.gmail.com", 587, timeout=30)
                    server.ehlo()
                    server.starttls()
                    server.ehlo()
                server.login(REMITENTE, PASSWORD)
                server.sendmail(REMITENTE, destinatarios, msg.as_string())
                print(f"Correo enviado exitosamente ({nombre}, intento {intento}).")
                return True
            except smtplib.SMTPAuthenticationError as e:
                print(f"Error de autenticacion SMTP ({nombre}): {e}. Revisa la App Password en smtp.json.")
                return False
            except Exception as e:
                print(f"  Fallo {nombre} (intento {intento}): {e}")
            finally:
                if server is not None:
                    try:
                        server.quit()
                    except Exception:
                        pass

        if intento < max_reintentos:
            tiempo = espera_seg * intento
            print(f"  Reintentando en {tiempo}s...")
            time.sleep(tiempo)

    print("No se pudo enviar el correo tras agotar los reintentos.")
    return False


def main():
    print("--- INICIANDO VALIDACIÓN DE ERRORES DEL LAMBDA ---")

    # 1. Buscar el zip más reciente de hoy
    zip_key = buscar_zip_mas_reciente_hoy()
    if zip_key is None:
        print("No hay archivo de errores para procesar. Finalizando.")
        return

    # 2. Leer el CSV dentro del zip (parsing robusto con maxsplit)
    df_errores = leer_csv_desde_zip(zip_key)
    if df_errores.empty:
        print("El archivo de errores está vacío. Finalizando.")
        return

    # 3. Clasificar errores y deduplicar
    df_errores = clasificar_errores(df_errores)

    # 4. Cargar consolidado de pedidos subidos
    df_subido = cargar_consolidado_subido()

    # 5. Generar tablas
    tabla1, tabla2, tabla3, tabla_otros = generar_tablas(df_errores, df_subido)

    # 6. Construir HTML y enviar correo
    html_body = construir_html(tabla1, tabla2, tabla3, tabla_otros, zip_key)
    enviar_correo(html_body)

    print("--- VALIDACIÓN FINALIZADA ---")


if __name__ == "__main__":
    main()
