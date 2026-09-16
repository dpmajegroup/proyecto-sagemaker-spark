import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pyarrow==17.0.0"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "numpy", "pandas"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "awswrangler[redshift]", "--no-build-isolation"])

import os
import io
import boto3
import pandas as pd
import awswrangler as wr
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
my_session = boto3.Session(region_name="us-east-2")

# Fecha de manana
manana_lima = datetime.now(ZoneInfo("America/Lima")) + timedelta(days=1)
FECHA_REC = manana_lima.strftime("%Y-%m-%d")

# Parametros
BUCKET_ARTIFACTS = "aje-prd-analytics-artifacts-s3"
FILE_KEY_TOMORROW = "pedido_sugerido/data-v1/cam/ventas_CR_tomorrow.csv"
S3_BUCKET_BACKUP = "aje-analytics-ps-backup"

# ============================================================================
# MODO DE OPERACION:
# - MODO_GENERAR = True  -> Genera el estrategico internamente (default actual)
# - MODO_GENERAR = False -> Lee el archivo ya formateado desde S3 (futuro)
# Cambiar esta variable para activar uno u otro modo.
# ============================================================================
MODO_GENERAR = True
# MODO_GENERAR = False

# Ruta del archivo estrategico pre-formateado (cuando MODO_GENERAR = False)
S3_RUTA_ESTRATEGICO_EXTERNO = "s3://aje-dl-prod-us-east-2-399723489351-external-data/aje/analiticaAvanzada/cr/pedido_estrategico/Pedido Estrategico.csv"

# Filtro por rutas
RUTAS_ESTRATEGICO = [1003]

# Productos fijos del estrategico Costa Rica (mismos SKUs que Panama)
PRODUCTOS = [524090, 524091, 524092, 524587, 524586]


def generar_pedido_estrategico():
    """Genera el pedido estrategico: producto cartesiano de clientes x productos fijos."""
    print("Generando Pedido Estrategico (Costa Rica)...")

    # 1. Leer clientes a visitar manana
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=FILE_KEY_TOMORROW)
    cl = pd.read_csv(io.BytesIO(response["Body"].read()), sep=",")

    # 2. Filtrar por rutas
    cl = cl[cl.cod_ruta.isin(RUTAS_ESTRATEGICO)][
        ["cod_compania", "cod_sucursal", "cod_modulo", "cod_cliente"]
    ].drop_duplicates().reset_index(drop=True)
    print(f"Clientes filtrados: {cl.shape[0]}")

    # 3. Preparar columnas base
    df_unicos = cl[["cod_compania", "cod_cliente"]].drop_duplicates()
    cl["Pais"] = "CR"
    cl["Cajas"] = 1
    cl["Unidades"] = 0
    cl["Fecha"] = FECHA_REC

    # 4. Producto cartesiano clientes x productos
    df_prod = pd.DataFrame({"Producto": PRODUCTOS})
    df_combinado = df_unicos.merge(df_prod, how="cross")
    df_final = df_combinado.merge(cl, on=["cod_compania", "cod_cliente"])

    # 5. Formatear columnas
    df_final.columns = ["Compania", "Cliente", "Producto", "Sucursal", "Modulo", "Pais", "Cajas", "Unidades", "Fecha"]
    df_final = df_final[["Pais", "Compania", "Sucursal", "Cliente", "Modulo", "Producto", "Cajas", "Unidades", "Fecha"]]

    # 6. tipoRecomendacion PE1, PE2...
    secuencia = df_final.groupby(["Compania", "Cliente"]).cumcount() + 1
    df_final["tipoRecomendacion"] = "PE" + secuencia.astype(str)
    df_final["ultFecha"] = ""
    df_final["Destacar"] = "true"

    # 7. Formatear tipos
    df_final["Cliente"] = df_final["Cliente"].astype(int)
    df_final["Producto"] = df_final["Producto"].astype(int)
    df_final["Unidades"] = df_final["Unidades"].astype(int)
    df_final["Cajas"] = df_final["Cajas"].astype(int)
    df_final["Compania"] = df_final["Compania"].astype(str).str.zfill(4)
    df_final["Sucursal"] = df_final["Sucursal"].astype(str).str.zfill(2)

    return df_final


def excluir_recurrente_y_sugerido(df_final):
    """Quita productos que ya estan en Pedido Recurrente y Pedido Sugerido."""
    print("Excluyendo productos de Recurrente y Sugerido...")

    # Leer Recurrente
    try:
        pr_cr = wr.s3.read_csv(
            f"s3://{S3_BUCKET_BACKUP}/Pedido_Recurrente/Costa_Rica/Output/recu_base_pedidos_{FECHA_REC}.csv",
            boto3_session=my_session
        )[["Compania", "Cliente", "Producto"]]
    except Exception as e:
        print(f"No se pudo leer Recurrente: {e}")
        pr_cr = pd.DataFrame(columns=["Compania", "Cliente", "Producto"])

    # Leer Sugerido
    try:
        ps_cr = wr.s3.read_csv(
            f"s3://{S3_BUCKET_BACKUP}/PS_CostaRica/Output/PS_piloto_v1/D_base_pedidos_{FECHA_REC}.csv",
            boto3_session=my_session
        )[["Compania", "Cliente", "Producto"]]
    except Exception as e:
        print(f"No se pudo leer Sugerido: {e}")
        ps_cr = pd.DataFrame(columns=["Compania", "Cliente", "Producto"])

    quitar_temp = pd.concat([pr_cr, ps_cr], ignore_index=True).drop_duplicates()
    quitar_temp["Compania"] = quitar_temp["Compania"].astype(str).str.zfill(4)
    quitar_temp["id_cliente"] = "CAM|" + quitar_temp["Compania"] + "|" + quitar_temp["Cliente"].astype(str)
    quitar_temp.rename(columns={"Producto": "cod_articulo_magic"}, inplace=True)

    # Crear id_cliente en df_final para el merge
    df_final["id_cliente"] = "CAM|" + df_final["Compania"] + "|" + df_final["Cliente"].astype(str)
    df_final.rename(columns={"Producto": "cod_articulo_magic"}, inplace=True)

    # Excluir pares que ya existen en Recurrente o Sugerido
    merge_temp = df_final.merge(
        quitar_temp[["id_cliente", "cod_articulo_magic"]],
        on=["id_cliente", "cod_articulo_magic"], how="left", indicator=True
    )
    rec_sin = merge_temp[merge_temp["_merge"] == "left_only"].drop(columns=["_merge"])
    rec_sin.rename(columns={"cod_articulo_magic": "Producto"}, inplace=True)
    rec_sin.drop(columns=["id_cliente"], inplace=True)

    # Top 3 por cliente (limite fijo)
    df_final = rec_sin.groupby(["Pais", "Compania", "Sucursal", "Cliente"]).head(3).reset_index(drop=True)

    # Recalcular tipoRecomendacion
    secuencia = df_final.groupby(["Compania", "Cliente"]).cumcount() + 1
    df_final["tipoRecomendacion"] = "PE" + secuencia.astype(str)

    print(f"Estrategico final: {df_final.shape[0]} filas, {df_final.Cliente.nunique()} clientes")
    return df_final


def leer_estrategico_externo():
    """Lee el archivo de pedido estrategico pre-formateado desde S3 (modo externo)."""
    print("Leyendo pedido estrategico desde archivo externo...")
    s3 = boto3.client("s3")

    bucket = "aje-dl-prod-us-east-2-399723489351-external-data"
    key = "aje/analiticaAvanzada/cr/pedido_estrategico/Pedido Estrategico.csv"
    local_path = "/opt/ml/processing/Pedido_Estrategico_CR.csv"

    s3.download_file(bucket, key, local_path)
    # Lectura robusta
    try:
        df = pd.read_csv(local_path, sep=";")
        if len(df.columns) <= 2:
            df = pd.read_csv(local_path, sep=",")
    except Exception:
        df = pd.read_csv(local_path, sep=",")

    print(f"  Archivo leido: {df.shape[0]} filas, {df.Cliente.nunique()} clientes")

    # Formatear
    df["Pais"] = "CR"
    df["Compania"] = df["Compania"].astype(str).str.zfill(4)
    df["Sucursal"] = df["Sucursal"].astype(str).str.zfill(2)
    df = df.dropna(subset=["Cliente"]).reset_index(drop=True)
    df["Cliente"] = df["Cliente"].astype(float).astype(int)
    df["Producto"] = df["Producto"].astype(float).astype(int)
    df["Cajas"] = df["Cajas"].astype(float).astype(int)
    df["Unidades"] = df["Unidades"].astype(float).astype(int)

    df["Fecha"] = FECHA_REC

    if "tipoRecomendacion" not in df.columns:
        df["tipoRecomendacion"] = df.groupby(["Compania", "Cliente"]).cumcount().apply(lambda x: f"PE{x+1}")
    if "ultFecha" not in df.columns:
        df["ultFecha"] = ""
    df["ultFecha"] = df["ultFecha"].fillna("")
    if "Destacar" not in df.columns:
        df["Destacar"] = "true"

    df = df[["Pais", "Compania", "Sucursal", "Cliente", "Modulo", "Producto", "Cajas", "Unidades", "Fecha", "tipoRecomendacion", "ultFecha", "Destacar"]]

    print(f"  Formateado: {df.shape[0]} filas, {df.Cliente.nunique()} clientes")
    return df


def exportar_y_concatenar(df_estrategico):
    """Guarda backup de estrategico."""
    print("Exportando resultados...")

    s3_path_backup = f"s3://{S3_BUCKET_BACKUP}/Pedido_Estrategico/Costa_Rica/Output/estr_base_pedidos_{FECHA_REC}.csv"
    wr.s3.to_csv(df_estrategico, s3_path_backup, index=False, boto3_session=my_session)
    print(f"Backup estrategico guardado en {s3_path_backup}")
    print(f"Estrategico: {df_estrategico.shape[0]} filas, {df_estrategico.Cliente.nunique()} clientes")


def main():
    print("--- INICIANDO PEDIDO ESTRATEGICO (Costa Rica) ---")

    if MODO_GENERAR:
        # === MODO GENERACION (default actual) ===
        df_estrategico = generar_pedido_estrategico()
        df_estrategico = excluir_recurrente_y_sugerido(df_estrategico)
    else:
        # === MODO LECTURA EXTERNA ===
        df_estrategico = leer_estrategico_externo()
        df_estrategico = excluir_recurrente_y_sugerido(df_estrategico)

    exportar_y_concatenar(df_estrategico)

    print("--- PROCESO FINALIZADO ---")


if __name__ == "__main__":
    main()
