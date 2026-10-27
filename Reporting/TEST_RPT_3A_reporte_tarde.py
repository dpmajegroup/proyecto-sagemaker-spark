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
# CONFIGURACION DE PAISES Y TIPOS DE RECOMENDACION (paises TARDE, input externo)
# Este reporte corre a las 5:30pm como complemento del reporte principal (2pm).
# Comentar/descomentar para activar/desactivar cada pais o cada tipo de pedido.
# =============================================================================
PAISES_CONFIG = {
    "Nicaragua": {
        "PS": f"s3://{BUCKET_BACKUP}/PS_Nicaragua/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
    },
    "Colombia": {
        "PS": f"s3://{BUCKET_BACKUP}/PS_Colombia/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv",
        "PR": f"s3://{BUCKET_BACKUP}/Pedido_Recurrente/Colombia/Output/recu_base_pedidos_{fecha_tomorrow}.csv",
        "PE": f"s3://{BUCKET_BACKUP}/Pedido_Estrategico/Colombia/Output/estr_base_pedidos_{fecha_tomorrow}.csv",
    },
}

# Reglas especiales por pais
# Colombia: Compania siempre es 1 digito (no aplicar zfill(4))
PAISES_COMPANIA_1_DIGITO = ["CO"]

# Ruta del consolidado principal (subido por RPT_1 a las 2pm)
S3_PATH_CONSOLIDADO = "s3://aje-prd-pedido-sugerido-orders-s3/PE/pedidos/base_pedidos.csv"

# Credenciales correo (leídas desde S3, no hardcodeadas)
def cargar_credenciales_smtp():
    """Lee REMITENTE y PASSWORD del SMTP desde un JSON en S3."""
    import json
    s3 = my_session.client("s3")
    obj = s3.get_object(Bucket=BUCKET_BACKUP, Key="smtp.json")
    creds = json.loads(obj["Body"].read().decode("utf-8"))
    return creds["REMITENTE"], creds["PASSWORD"]


REMITENTE, PASSWORD = cargar_credenciales_smtp()
DESTINATARIOS = [
    "david.porta@ajegroup.com",
    "lizeth.gil@ajegroup.com",
    "gherald.barzola@ajegroup.com",
    "melissa.cotrina@ajegroup.com",
    "reymundo.jimeno.ve@ajegroup.com",
    "rodrigo.basurto@ajegroup.com"
]


def leer_archivo_s3(ruta, nombre):
    """Lee un CSV desde S3 con manejo de errores. Fuerza Cliente como string para proteger '00' prefix."""
    try:
        df = wr.s3.read_csv(ruta, dtype={"Cliente": str, "Compania": str}, boto3_session=my_session)
        # Proteger cod_cliente Colombia (prefijo "00")
        df["Cliente"] = df["Cliente"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        print(f"  {nombre}: {df.shape[0]} filas")
        return df
    except Exception as e:
        print(f"  {nombre}: No encontrado ({e})")
        return pd.DataFrame()


# =============================================================================
# FILTRO POR FRECUENCIA DE VISITA (F1/F2/F3/F4)
# Mismo criterio que RPT_1A pero aplicado a los paises tarde (NI, CO).
# F1 = semanal (siempre), F2 = cada 2 semanas, F3 = cada 3, F4 = cada 4.
# Se mide con ultima_visita cuantas semanas pasaron hasta la fecha de prediccion.
# El cruce es tolerante a ceros a la izquierda en Compania/Cliente (Colombia usa
# compania de 1 digito).
# =============================================================================
BUCKET_ARTIFACTS = "aje-prd-analytics-artifacts-s3"

VISITAS_CONFIG = {
    "NI": (BUCKET_ARTIFACTS, "pedido_sugerido/data-v1/cam/visitas_cam000"),
    "CO": (BUCKET_ARTIFACTS, "pedido_sugerido/data-v1/colombia/visitas_colombia000"),
}

UMBRAL_FRECUENCIA = {"F1": 0, "F2": 14, "F3": 21, "F4": 28}


def _norm_codigo(serie):
    """Normaliza un codigo a string sin ceros a la izquierda ni '.0' para cruce robusto."""
    s = pd.to_numeric(serie, errors="coerce")
    out = s.astype("Int64").astype(str).str.replace("<NA>", "", regex=False)
    mask = out == ""
    out[mask] = serie.astype(str).str.strip().str.replace(r"\.0$", "", regex=True).str.lstrip("0")
    return out.str.lstrip("0")


def cargar_visitas_frecuencia(paises_codigos):
    """Lee visitas de los paises indicados; devuelve Pais, _cia, _cli, periodo, ultima_visita."""
    s3 = my_session.client("s3")
    cache_archivos = {}
    partes = []

    for cod in paises_codigos:
        cfg = VISITAS_CONFIG.get(cod)
        if cfg is None:
            print(f"  [frecuencia] Sin config de visitas para '{cod}', se omite.")
            continue
        bucket, key = cfg
        try:
            if key not in cache_archivos:
                raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                cache_archivos[key] = pd.read_csv(io.BytesIO(raw), sep=";", low_memory=False)
            vis = cache_archivos[key]
        except Exception as e:
            print(f"  [frecuencia] No se pudo leer visitas de {cod}: {e}")
            continue

        cols_req = {"compania__c", "codigo_cliente__c", "periodo_de_visita__c", "ultima_visita"}
        if not cols_req.issubset(vis.columns):
            print(f"  [frecuencia] Visitas de {cod} sin columnas requeridas, se omite.")
            continue

        v = vis[list(cols_req)].copy()
        v["Pais"] = cod
        v["_cia"] = _norm_codigo(v["compania__c"])
        v["_cli"] = _norm_codigo(v["codigo_cliente__c"])
        v["ultima_visita"] = pd.to_datetime(v["ultima_visita"], errors="coerce")
        v = v.sort_values(["Pais", "_cia", "_cli", "ultima_visita"], ascending=[True, True, True, False])
        v = v.groupby(["Pais", "_cia", "_cli"], as_index=False).head(1)
        partes.append(v[["Pais", "_cia", "_cli", "periodo_de_visita__c", "ultima_visita"]])

    if not partes:
        return pd.DataFrame(columns=["Pais", "_cia", "_cli", "periodo_de_visita__c", "ultima_visita"])
    return pd.concat(partes, ignore_index=True)


def filtrar_por_frecuencia(final):
    """Descarta clientes que no deberian recibir recomendacion manana segun su frecuencia.

    IMPORTANTE: devuelve un subconjunto de filas del 'final' original, sin alterar
    valores, columnas, dtypes ni el orden de las filas. Solo excluye filas.
    """
    if final.empty:
        return final

    paises = list(final["Pais"].astype(str).str.strip().unique())
    visitas = cargar_visitas_frecuencia(paises)
    if visitas.empty:
        print("  [frecuencia] Sin visitas para cruzar, se mantiene el consolidado completo.")
        return final

    filas_antes = len(final)
    cli_antes = final[["Pais", "Compania", "Cliente"]].drop_duplicates().shape[0]

    # DataFrame auxiliar SOLO con llaves normalizadas (no se tocan las columnas de 'final')
    aux = pd.DataFrame({
        "Pais": final["Pais"].astype(str).str.strip().values,
        "_cia": _norm_codigo(final["Compania"]).values,
        "_cli": _norm_codigo(final["Cliente"]).values,
    })
    aux = aux.merge(visitas, on=["Pais", "_cia", "_cli"], how="left")

    fecha_prediccion = datetime.now(tz_lima).replace(tzinfo=None) + timedelta(days=1)
    periodo = aux["periodo_de_visita__c"].astype(str).str.strip().str.upper()
    dias_hasta_pred = (fecha_prediccion - aux["ultima_visita"]).dt.days
    dias_min = periodo.map(UMBRAL_FRECUENCIA).fillna(0)

    mantener = (
        aux["periodo_de_visita__c"].isna()
        | dias_hasta_pred.isna()
        | (dias_hasta_pred >= dias_min)
    ).values

    # Aplicar la mascara al 'final' ORIGINAL para conservar formato y orden intactos
    resultado = final[mantener].reset_index(drop=True)

    filas_desp = len(resultado)
    cli_desp = resultado[["Pais", "Compania", "Cliente"]].drop_duplicates().shape[0]
    print(f"  [frecuencia] Filas: {filas_antes:,} -> {filas_desp:,} | Clientes: {cli_antes:,} -> {cli_desp:,}")
    return resultado


def cargar_paises_tarde():
    """Lee los backups de todos los paises/tipos configurados en PAISES_CONFIG."""
    print(f"Cargando recomendaciones TARDE para fecha {fecha_tomorrow}...")
    dfs = []

    # Mapa de prefijo tipo para tipoRecomendacion default
    tipo_prefijo = {"PS": "PS", "PS_ECO": "PS", "PR": "PR", "PE": "PE"}

    for pais, tipos in PAISES_CONFIG.items():
        for tipo, ruta in tipos.items():
            label = f"{tipo} {pais}"
            df = leer_archivo_s3(ruta, label)
            if not df.empty:
                prefijo = tipo_prefijo.get(tipo, "PS")
                if "tipoRecomendacion" not in df.columns:
                    df["tipoRecomendacion"] = df.groupby(["Pais", "Compania", "Sucursal", "Cliente"]).cumcount().apply(lambda x: f"{prefijo}{x+1}")
                if "ultFecha" not in df.columns:
                    df["ultFecha"] = ""
                df["ultFecha"] = df["ultFecha"].fillna("")
                if "Destacar" not in df.columns:
                    df["Destacar"] = "true"
                dfs.append(df)

    if not dfs:
        print("No se encontraron archivos de ningun pais tarde.")
        return pd.DataFrame()

    # Concatenar paises tarde
    final_tarde = pd.concat(dfs, ignore_index=True)

    # Estandarizar formatos
    # Colombia: Compania siempre 1 digito (no zfill)
    mask_compania_1 = final_tarde["Pais"].isin(PAISES_COMPANIA_1_DIGITO)
    final_tarde.loc[~mask_compania_1, "Compania"] = final_tarde.loc[~mask_compania_1, "Compania"].astype(str).str.zfill(4)
    final_tarde.loc[mask_compania_1, "Compania"] = final_tarde.loc[mask_compania_1, "Compania"].astype(str).str.strip().str[:1]
    final_tarde["Sucursal"] = final_tarde["Sucursal"].astype(str).str.zfill(2)

    # Modulo como entero (viene como float '4163.0' desde los backups) -> '4163'
    def _modulo_int(v):
        s = str(v).strip()
        if s == "" or s.lower() in ("nan", "none"):
            return ""
        try:
            return str(int(float(s)))
        except (ValueError, TypeError):
            return s
    if "Modulo" in final_tarde.columns:
        final_tarde["Modulo"] = final_tarde["Modulo"].apply(_modulo_int)

    # Seleccionar 12 columnas en orden
    cols = ["Pais", "Compania", "Sucursal", "Cliente", "Modulo", "Producto", "Cajas", "Unidades", "Fecha", "tipoRecomendacion", "ultFecha", "Destacar"]
    for c in cols:
        if c not in final_tarde.columns:
            final_tarde[c] = ""
    final_tarde = final_tarde[cols]

    print(f"\nTotal paises tarde: {final_tarde.shape[0]} filas")
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
    """Genera metricas por pais, compania, sucursal."""
    final["cliente_unico"] = final["Pais"].astype(str) + "|" + final["Compania"].astype(str) + "|" + final["Sucursal"].astype(str) + "|" + final["Cliente"].astype(str)
    final["tipo"] = final["tipoRecomendacion"].str[:2]

    # Detalle por Pais, Compania, Sucursal
    detalle = final.groupby(["Pais", "Compania", "Sucursal"]).agg(
        clientes=("cliente_unico", "nunique"),
        recomendaciones=("Producto", "count"),
        skus_unicos=("Producto", "nunique"),
    ).reset_index()
    detalle["prom_prod_cliente"] = (detalle["recomendaciones"] / detalle["clientes"]).round(2)

    # Desglose por tipo de recomendacion
    tipo_rec = final.groupby(["Pais", "Compania", "Sucursal", "tipo"]).agg(
        clientes=("cliente_unico", "nunique"),
        recomendaciones=("Producto", "count"),
    ).reset_index()

    # Resumen por Pais
    resumen_pais = detalle.groupby("Pais").agg(
        clientes=("clientes", "sum"),
        recomendaciones=("recomendaciones", "sum"),
        skus_unicos=("skus_unicos", "max"),
    ).reset_index()
    resumen_pais["prom_prod_cliente"] = (resumen_pais["recomendaciones"] / resumen_pais["clientes"]).round(2)

    # Resumen por Pais, Compania y Tipo
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
    <h2>Reporte Tarde - Pedido Sugerido (Paises con input externo)</h2>
    <p>Fecha de recomendaciones: <b>{fecha_tomorrow}</b></p>
    <p>Paises incluidos: <b>{lista_paises}</b></p>

    <div class="info">
        <b>Nota:</b> Este reporte complementa al reporte principal de las 2pm.
        Incluye paises que dependen de un archivo externo subido a las 5pm.
    </div>

    <h3>1. Resumen por Pais</h3>
    {df_to_html_table(resumen_pais)}

    <h3>2. Resumen por Pais, Compania y Tipo</h3>
    {df_to_html_table(resumen_cia_tipo)}

    <h3>3. Detalle por Pais, Compania y Sucursal</h3>
    {df_to_html_table(detalle)}

    <h3>4. Desglose por Tipo de Recomendacion (PR/PS/PE)</h3>
    {df_to_html_table(tipo_rec)}

    <br>
    <p><i>Este correo fue generado automaticamente. No responder.</i></p>
    </body>
    </html>
    """
    return html


def enviar_correo(html_body):
    """Envia el correo con el reporte tarde."""
    print("Enviando correo...")
    msg = MIMEMultipart()
    msg["From"] = REMITENTE
    msg["To"] = ", ".join(DESTINATARIOS)
    msg["Subject"] = f"Reporte Pedido Sugerido - {fecha_tomorrow}"
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
    print("--- INICIANDO REPORTE TARDE (PAISES CON INPUT EXTERNO) ---")

    # 1. Cargar paises tarde
    final_tarde = cargar_paises_tarde()
    if final_tarde.empty:
        print("No hay datos de paises tarde para reportar.")
        return

    # 1.5 Filtrar por frecuencia de visita (F1/F2/F3/F4)
    print("Aplicando filtro por frecuencia de visita...")
    final_tarde = filtrar_por_frecuencia(final_tarde)
    if final_tarde.empty:
        print("No quedaron recomendaciones tras el filtro de frecuencia.")
        return

    # 2. Guardar backup de paises tarde
    guardar_backup_tarde(final_tarde)

    # 3. Subir solo paises tarde a base_pedidos.csv (sobreescribe, backups ya existen)
    wr.s3.to_csv(final_tarde, S3_PATH_CONSOLIDADO, index=False, boto3_session=my_session)
    print(f"Paises tarde subidos a {S3_PATH_CONSOLIDADO}: {final_tarde.shape[0]} filas")

    # 4. Generar metricas solo de paises tarde (para el correo)
    paises_incluidos = list(PAISES_CONFIG.keys())
    resumen_pais, resumen_cia_tipo, detalle, tipo_rec = generar_metricas(final_tarde)

    # 5. Construir HTML y enviar correo
    html_body = construir_html(resumen_pais, resumen_cia_tipo, detalle, tipo_rec, paises_incluidos)
    enviar_correo(html_body)

    print("--- REPORTE TARDE FINALIZADO ---")


if __name__ == "__main__":
    main()
