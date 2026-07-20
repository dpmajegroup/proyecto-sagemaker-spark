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
S3_BUCKET_BACKUP = "aje-analytics-ps-backup"

# ============================================================================
# MODO DE OPERACIÓN:
# - MODO_GENERAR = True  → Genera el estratégico internamente
# - MODO_GENERAR = False → Lee el archivo ya formateado desde S3 (activo)
# ============================================================================
# MODO_GENERAR = True
MODO_GENERAR = False

# Ruta del archivo estratégico pre-formateado (cuando MODO_GENERAR = False)
# Si no existe, busca el más reciente por LastModified
BUCKET_ESTRATEGICO_EXTERNO = "aje-dl-prod-us-east-2-399723489351-external-data"
KEY_ESTRATEGICO_EXTERNO = "aje/analiticaAvanzada/pe/pedido_estrategico/Pedido Estrategico.csv"


def leer_estrategico_externo():
    """Lee el archivo de pedido estratégico pre-formateado desde S3."""
    print("Leyendo pedido estratégico desde archivo externo...")
    s3 = boto3.client('s3')
    local_path = "/opt/ml/processing/Pedido_Estrategico_PE.csv"

    # Intentar descargar archivo
    try:
        s3.download_file(BUCKET_ESTRATEGICO_EXTERNO, KEY_ESTRATEGICO_EXTERNO, local_path)
        print(f"  Cargado: {KEY_ESTRATEGICO_EXTERNO}")
    except Exception:
        print(f"  No se encontró archivo principal. Buscando el más reciente...")
        prefix = "aje/analiticaAvanzada/pe/pedido_estrategico/"
        response = s3.list_objects_v2(Bucket=BUCKET_ESTRATEGICO_EXTERNO, Prefix=prefix)
        if 'Contents' in response:
            archivos = [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')]
            if archivos:
                ultimo = sorted(archivos, key=lambda x: x['LastModified'], reverse=True)[0]
                s3.download_file(BUCKET_ESTRATEGICO_EXTERNO, ultimo['Key'], local_path)
                print(f"  Cargado más reciente: {ultimo['Key']}")
            else:
                raise FileNotFoundError("No se encontraron archivos CSV de estratégico.")
        else:
            raise FileNotFoundError("No se encontraron archivos en la ruta.")

    # Leer CSV (coma como separador, igual que Bolivia)
    df = pd.read_csv(local_path)
    print(f"  Archivo leído: {df.shape[0]} filas, {df.Cliente.nunique()} clientes")

    # Formatear
    df = df.dropna(subset=["Cliente"]).reset_index(drop=True)
    df["Pais"] = "PE"
    df["Compania"] = df["Compania"].astype(str).str.strip().str.zfill(4)
    df["Sucursal"] = df["Sucursal"].astype(str).str.strip().str.zfill(2)
    df["Cliente"] = df["Cliente"].astype(float).astype(int)
    df["Producto"] = df["Producto"].astype(float).astype(int)
    df["Cajas"] = df["Cajas"].astype(float).astype(int)
    df["Unidades"] = df["Unidades"].astype(float).astype(int)

    # Actualizar fecha a mañana
    df["Fecha"] = FECHA_REC

    # Asegurar columnas
    if "tipoRecomendacion" not in df.columns:
        df["tipoRecomendacion"] = df.groupby(["Compania", "Cliente"]).cumcount().apply(lambda x: f"PE{x+1}")
    if "ultFecha" not in df.columns:
        df["ultFecha"] = ''
    df["ultFecha"] = df["ultFecha"].fillna('')
    if "Destacar" not in df.columns:
        df["Destacar"] = "true"

    # 12 columnas estándar
    df = df[["Pais", "Compania", "Sucursal", "Cliente", "Modulo", "Producto", "Cajas", "Unidades", "Fecha", "tipoRecomendacion", "ultFecha", "Destacar"]]

    print(f"  Formateado: {df.shape[0]} filas, {df.Cliente.nunique()} clientes")
    return df


def excluir_recurrente_y_sugerido(df_final):
    """Quita productos que ya están en Pedido Recurrente y Pedido Sugerido."""
    print("Excluyendo productos de Recurrente y Sugerido...")

    # Leer Recurrente
    try:
        pr = wr.s3.read_csv(
            f"s3://{S3_BUCKET_BACKUP}/Pedido_Recurrente/Peru/Output/recu_base_pedidos_{FECHA_REC}.csv",
            boto3_session=my_session
        )[["Compania", "Cliente", "Producto"]]
    except Exception as e:
        print(f"  No se pudo leer Recurrente: {e}")
        pr = pd.DataFrame(columns=["Compania", "Cliente", "Producto"])

    # Leer Sugerido
    try:
        ps = wr.s3.read_csv(
            f"s3://{S3_BUCKET_BACKUP}/PS_Peru/Output/PS_piloto_v1/D_base_pedidos_{FECHA_REC}.csv",
            boto3_session=my_session
        )[["Compania", "Cliente", "Producto"]]
    except Exception as e:
        print(f"  No se pudo leer Sugerido: {e}")
        ps = pd.DataFrame(columns=["Compania", "Cliente", "Producto"])

    quitar_temp = pd.concat([pr, ps], ignore_index=True).drop_duplicates()
    quitar_temp["Compania"] = quitar_temp["Compania"].astype(str).str.zfill(4)
    quitar_temp["id_cliente"] = "PE|" + quitar_temp["Compania"] + "|" + quitar_temp["Cliente"].astype(int).astype(str)
    quitar_temp.rename(columns={"Producto": "cod_articulo_magic"}, inplace=True)

    # Crear id_cliente en df_final
    df_final["id_cliente"] = "PE|" + df_final["Compania"].astype(str) + "|" + df_final["Cliente"].astype(int).astype(str)
    df_final.rename(columns={"Producto": "cod_articulo_magic"}, inplace=True)

    # Excluir
    merge_temp = df_final.merge(
        quitar_temp[["id_cliente", "cod_articulo_magic"]].drop_duplicates(),
        on=["id_cliente", "cod_articulo_magic"], how="left", indicator=True
    )
    rec_sin = merge_temp[merge_temp["_merge"] == "left_only"].drop(columns=["_merge"])
    rec_sin.rename(columns={"cod_articulo_magic": "Producto"}, inplace=True)
    rec_sin.drop(columns=["id_cliente"], inplace=True)

    # Recalcular tipoRecomendacion
    df_final = rec_sin.reset_index(drop=True)
    secuencia = df_final.groupby(['Compania', 'Cliente']).cumcount() + 1
    df_final['tipoRecomendacion'] = 'PE' + secuencia.astype(str)

    print(f"  Estratégico final: {df_final.shape[0]} filas, {df_final.Cliente.nunique()} clientes")
    return df_final


def exportar(df_estrategico):
    """Guarda backup de estratégico."""
    print("Exportando resultados...")
    s3_path = f"s3://{S3_BUCKET_BACKUP}/Pedido_Estrategico/Peru/Output/estr_base_pedidos_{FECHA_REC}.csv"
    wr.s3.to_csv(df_estrategico, s3_path, index=False, boto3_session=my_session)
    print(f"  Backup guardado en {s3_path}")
    print(f"  Estratégico: {df_estrategico.shape[0]} filas, {df_estrategico.Cliente.nunique()} clientes")


def main():
    print("--- INICIANDO PEDIDO ESTRATÉGICO (Perú) ---")

    if MODO_GENERAR:
        # === MODO GENERACIÓN (no implementado aún para Perú) ===
        print("MODO_GENERAR=True no implementado para Perú. Cambiar a False.")
        return
    else:
        # === MODO LECTURA EXTERNA ===
        df_estrategico = leer_estrategico_externo()
        df_estrategico = excluir_recurrente_y_sugerido(df_estrategico)

    exportar(df_estrategico)
    print("--- PROCESO FINALIZADO ---")


if __name__ == "__main__":
    main()
