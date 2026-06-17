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

# Fecha de mañana
manana_lima = datetime.now(ZoneInfo("America/Lima")) + timedelta(days=1)
FECHA_REC = manana_lima.strftime("%Y-%m-%d")

# Parámetros
BUCKET_ARTIFACTS = "aje-prd-analytics-artifacts-s3"
FILE_KEY_TOMORROW = "pedido_sugerido/data-v1/colombia/ventas_colombia_tomorrow.csv"
S3_BUCKET_BACKUP = "aje-analytics-ps-backup"

# ============================================================================
# MODO DE OPERACIÓN:
# - MODO_GENERAR = True  → Genera el estratégico internamente (default actual)
# - MODO_GENERAR = False → Lee el archivo ya formateado desde S3 (futuro)
# Cambiar esta variable para activar uno u otro modo.
# ============================================================================
MODO_GENERAR = True
# MODO_GENERAR = False

# Ruta del archivo estratégico pre-formateado (cuando MODO_GENERAR = False)
S3_RUTA_ESTRATEGICO_EXTERNO = "s3://aje-dl-prod-us-east-2-399723489351-external-data/aje/comercial/co/analitica_avanzada/Pedido Estrategico.csv"

# Filtro por rutas (todas las rutas de Colombia)
RUTAS_ESTRATEGICO = [
    10106, 10108, 10107, 10102, 10101, 10105, 10104, 10103, 10109,
    10201, 10202, 10203, 10204, 10205, 10206, 10207, 10209, 10208,
    10308, 10306, 10304, 10303, 10305, 10301, 10302, 10307,
    10406, 10403, 10402, 10408, 10407, 10401, 10410, 10404, 10405, 10409,
    10508, 10506, 10507, 10510, 10509, 10504, 10505, 10503, 10502, 10501,
]

# Productos fijos del estratégico Colombia (alfanuméricos)
PRODUCTOS = ["ABC123", "BCD124"]


def generar_pedido_estrategico():
    """Genera el pedido estratégico: producto cartesiano de clientes x productos fijos."""
    print("Generando Pedido Estratégico (Colombia)...")

    # 1. Leer clientes a visitar mañana
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=FILE_KEY_TOMORROW)
    cl = pd.read_csv(io.BytesIO(response['Body'].read()))

    # 2. Filtrar por rutas
    cl = cl[cl.cod_ruta.isin(RUTAS_ESTRATEGICO)][
        ["cod_compania", "cod_sucursal", "cod_modulo", "cod_cliente"]
    ].drop_duplicates().reset_index(drop=True)
    print(f"Clientes filtrados: {cl.shape[0]}")

    # 3. Preparar columnas base
    df_unicos = cl[['cod_compania', 'cod_cliente']].drop_duplicates()
    cl["Pais"] = "CO"
    cl["Cajas"] = 1
    cl["Unidades"] = 0
    cl["Fecha"] = FECHA_REC

    # 4. Producto cartesiano clientes x productos
    df_prod = pd.DataFrame({'Producto': PRODUCTOS})
    df_combinado = df_unicos.merge(df_prod, how='cross')
    df_final = df_combinado.merge(cl, on=['cod_compania', 'cod_cliente'])

    # 5. Formatear columnas
    df_final.columns = ['Compania', 'Cliente', 'Producto', 'Sucursal', 'Modulo', 'Pais', 'Cajas', 'Unidades', 'Fecha']
    df_final = df_final[['Pais', 'Compania', 'Sucursal', 'Cliente', 'Modulo', 'Producto', 'Cajas', 'Unidades', 'Fecha']]

    # 6. tipoRecomendacion PE1, PE2...
    secuencia = df_final.groupby(['Compania', 'Cliente']).cumcount() + 1
    df_final['tipoRecomendacion'] = 'PE' + secuencia.astype(str)
    df_final["ultFecha"] = ''
    df_final["Destacar"] = "true"

    # 7. Formatear tipos - cod_articulo_magic es STRING en Colombia
    df_final["Cliente"] = df_final["Cliente"].astype(int)
    df_final["Producto"] = df_final["Producto"].astype(str).str.strip()  # Mantener como string
    df_final["Unidades"] = df_final["Unidades"].astype(int)
    df_final["Cajas"] = df_final["Cajas"].astype(int)
    df_final["Compania"] = df_final["Compania"].astype(str).str.zfill(3)  # 3 dígitos para Colombia
    df_final["Sucursal"] = df_final["Sucursal"].astype(str).str.zfill(2)

    return df_final


def excluir_recurrente_y_sugerido(df_final):
    """Quita productos que ya están en Pedido Recurrente y Pedido Sugerido."""
    print("Excluyendo productos de Recurrente y Sugerido...")

    # Leer Recurrente
    try:
        pr_co = wr.s3.read_csv(
            f"s3://{S3_BUCKET_BACKUP}/Pedido_Recurrente/Colombia/Output/recu_base_pedidos_{FECHA_REC}.csv",
            boto3_session=my_session
        )[["Compania", "Cliente", "Producto"]]
    except Exception as e:
        print(f"No se pudo leer Recurrente: {e}")
        pr_co = pd.DataFrame(columns=["Compania", "Cliente", "Producto"])

    # Leer Sugerido
    try:
        ps_co = wr.s3.read_csv(
            f"s3://{S3_BUCKET_BACKUP}/PS_Colombia/Output/PS_piloto_v1/D_base_pedidos_{FECHA_REC}.csv",
            boto3_session=my_session
        )[["Compania", "Cliente", "Producto"]]
    except Exception as e:
        print(f"No se pudo leer Sugerido: {e}")
        ps_co = pd.DataFrame(columns=["Compania", "Cliente", "Producto"])

    quitar_temp = pd.concat([pr_co, ps_co], ignore_index=True).drop_duplicates()
    quitar_temp["Compania"] = quitar_temp["Compania"].astype(str).str.zfill(3)
    quitar_temp["id_cliente"] = "CO|" + quitar_temp["Compania"] + "|" + quitar_temp["Cliente"].astype(str)
    # Producto es string en Colombia
    quitar_temp["cod_articulo_magic"] = quitar_temp["Producto"].astype(str).str.strip()

    # Crear id_cliente en df_final para el merge
    df_final["id_cliente"] = "CO|" + df_final["Compania"] + "|" + df_final["Cliente"].astype(str)
    df_final["cod_articulo_magic"] = df_final["Producto"].astype(str).str.strip()

    # Excluir pares que ya existen en Recurrente o Sugerido
    merge_temp = df_final.merge(
        quitar_temp[["id_cliente", "cod_articulo_magic"]],
        on=["id_cliente", "cod_articulo_magic"], how="left", indicator=True
    )
    rec_sin = merge_temp[merge_temp["_merge"] == "left_only"].drop(columns=["_merge"])
    rec_sin.drop(columns=["id_cliente", "cod_articulo_magic"], inplace=True)

    # Top 4 por cliente
    df_final = rec_sin.groupby(['Pais', 'Compania', 'Sucursal', 'Cliente']).head(4).reset_index(drop=True)

    # Recalcular tipoRecomendacion
    secuencia = df_final.groupby(['Compania', 'Cliente']).cumcount() + 1
    df_final['tipoRecomendacion'] = 'PE' + secuencia.astype(str)

    print(f"Estratégico final: {df_final.shape[0]} filas, {df_final.Cliente.nunique()} clientes")
    return df_final


def leer_estrategico_externo():
    """Lee el archivo de pedido estratégico pre-formateado desde S3 (modo externo)."""
    print("Leyendo pedido estratégico desde archivo externo...")
    s3 = boto3.client('s3')

    # Descargar archivo (tiene espacios en el nombre, usamos boto3)
    bucket = "aje-dl-prod-us-east-2-399723489351-external-data"
    key = "aje/comercial/co/analitica_avanzada/Pedido Estrategico.csv"
    local_path = "/opt/ml/processing/Pedido_Estrategico_CO.csv"

    s3.download_file(bucket, key, local_path)
    df = pd.read_csv(local_path)

    print(f"  Archivo leído: {df.shape[0]} filas, {df.Cliente.nunique()} clientes")

    # Formatear: asegurar formatos estándar
    df["Pais"] = "CO"
    df["Compania"] = df["Compania"].astype(str).str.zfill(3)  # 3 dígitos
    df["Sucursal"] = df["Sucursal"].astype(str).str.zfill(2)
    df["Cliente"] = df["Cliente"].astype(int)
    df["Producto"] = df["Producto"].astype(str).str.strip()  # String para Colombia
    df["Cajas"] = df["Cajas"].astype(int)
    df["Unidades"] = df["Unidades"].astype(int)

    # Actualizar fecha a mañana (formato YYYY-MM-DD)
    df["Fecha"] = FECHA_REC

    # Asegurar columnas tipoRecomendacion, ultFecha, Destacar
    if "tipoRecomendacion" not in df.columns:
        df["tipoRecomendacion"] = df.groupby(["Compania", "Cliente"]).cumcount().apply(lambda x: f"PE{x+1}")
    if "ultFecha" not in df.columns:
        df["ultFecha"] = ''
    df["ultFecha"] = df["ultFecha"].fillna('')
    if "Destacar" not in df.columns:
        df["Destacar"] = "true"

    # Seleccionar 12 columnas en orden estándar
    df = df[["Pais", "Compania", "Sucursal", "Cliente", "Modulo", "Producto", "Cajas", "Unidades", "Fecha", "tipoRecomendacion", "ultFecha", "Destacar"]]

    print(f"  Formateado: {df.shape[0]} filas, {df.Cliente.nunique()} clientes")
    return df


def exportar_y_concatenar(df_estrategico):
    """Guarda backup de estratégico."""
    print("Exportando resultados...")

    # Guardar backup de estratégico
    s3_path_backup = f"s3://{S3_BUCKET_BACKUP}/Pedido_Estrategico/Colombia/Output/estr_base_pedidos_{FECHA_REC}.csv"
    wr.s3.to_csv(df_estrategico, s3_path_backup, index=False, boto3_session=my_session)
    print(f"Backup estratégico guardado en {s3_path_backup}")
    print(f"Estratégico: {df_estrategico.shape[0]} filas, {df_estrategico.Cliente.nunique()} clientes")


def main():
    print("--- INICIANDO PEDIDO ESTRATÉGICO (Colombia) ---")

    if MODO_GENERAR:
        # === MODO GENERACIÓN (default actual) ===
        # 1. Generar pedido estratégico
        df_estrategico = generar_pedido_estrategico()

        # 2. Excluir productos de Recurrente y Sugerido
        df_estrategico = excluir_recurrente_y_sugerido(df_estrategico)
    else:
        # === MODO LECTURA EXTERNA ===
        # Lee el archivo ya formateado desde S3 y solo actualiza fecha
        df_estrategico = leer_estrategico_externo()

        # Excluir productos de Recurrente y Sugerido
        df_estrategico = excluir_recurrente_y_sugerido(df_estrategico)

    # 3. Exportar
    exportar_y_concatenar(df_estrategico)

    print("--- PROCESO FINALIZADO ---")


if __name__ == "__main__":
    main()
