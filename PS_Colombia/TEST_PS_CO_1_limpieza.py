import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pyarrow==17.0.0"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "numpy", "pandas"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "awswrangler[redshift]", "--no-build-isolation"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "redshift-connector", "openpyxl"])

import os
import json
import boto3
import pytz
import io
import pandas as pd
import numpy as np
import awswrangler as wr
from datetime import datetime, timedelta

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
my_session = boto3.Session(region_name="us-east-2")

OUTPUT_DIR = "/opt/ml/processing/output/limpieza"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, "rutas"), exist_ok=True)

# Parámetros Globales - Colombia
BUCKET_ARTIFACTS = "aje-prd-analytics-artifacts-s3"
PREFIX_COLOMBIA = "pedido_sugerido/data-v1/colombia/"
COD_PAIS = "CO"
COD_COMPANIA = "001"

RUTAS_COLOMBIA = [
    # Piloto
    10407,
    # 10106, 10108, 10107, 10102, 10101, 10105, 10104, 10103, 10109,
    # 10201, 10202, 10203, 10204, 10205, 10206, 10207, 10209, 10208,
    # 10308, 10306, 10304, 10303, 10305, 10301, 10302, 10307,
    # 10406, 10403, 10402, 10408, 10407, 10401, 10410, 10404, 10405, 10409,
    # 10508, 10506, 10507, 10510, 10509, 10504, 10505, 10503, 10502, 10501,
]

tz_lima = pytz.timezone("America/Lima")


def comprobar_inputs():
    """Verifica que los archivos en S3 existan y hayan sido modificados hoy."""
    s3 = boto3.client("s3")
    hoy = datetime.now(tz_lima).date()
    errores = []

    objetos = s3.list_objects_v2(Bucket=BUCKET_ARTIFACTS, Prefix=PREFIX_COLOMBIA)
    if "Contents" not in objetos:
        raise ValueError("ERROR: No se encontraron archivos en la ruta especificada de S3.")

    for objeto in objetos["Contents"]:
        key = objeto["Key"]
        if key.endswith("/"):
            continue
        last_modified = objeto["LastModified"].date()
        size_kb = objeto["Size"] / 1024

        if objeto["Size"] == 0:
            errores.append(f"ERROR: El archivo {key} está vacío.")
        if last_modified != hoy:
            errores.append(f"ERROR: El archivo {key} no ha sido modificado hoy ({hoy}).")
        if size_kb < 1:
            print(f"ALERTA: El archivo {key} tiene un tamaño menor a 1 KB ({size_kb:.2f} KB).")

    if errores:
        for error in errores:
            print(error)
    print("Inputs comprobados correctamente :D")


def extraer_datos():
    """Descarga visitas, ventas. Aplica filtros de Colombia."""
    s3 = my_session.client("s3")

    # 1. Descargar Visitas
    visitas_obj = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=f"{PREFIX_COLOMBIA}visitas_colombia000")
    pan_visitas = pd.read_csv(io.BytesIO(visitas_obj["Body"].read()), sep=";")
    pan_visitas = pan_visitas[pan_visitas["cod_ruta"].isin(RUTAS_COLOMBIA)].reset_index(drop=True)
    clientes_ruta_test = pan_visitas["codigo_cliente__c"].unique()

    # 2. Descargar Ventas (NO tienen cod_ruta ni cod_modulo, pero SÍ tienen cod_articulo_magic)
    columnas_ventas = [
        'id_cliente', 'id_sucursal', 'id_producto', 'fecha_liquidacion',
        'cod_zona', 'cant_cajafisicavta', 'cant_cajaunitvta', 'imp_netovta', 'cod_compania',
        'desc_compania', 'cod_sucursal', 'desc_sucursal', 'cod_pais', 'fecha_creacion_cliente',
        'cod_cliente', 'desc_marca', 'desc_formato', 'desc_categoria', 'cod_giro', 'cod_subgiro',
        'desc_giro', 'desc_subgiro', 'fecha_proceso', 'cod_articulo_magic'
    ]
    pan_ventas = pd.DataFrame()
    for archivo_ventas in ["ventas_colombia000", "ventas_colombia001"]:
        try:
            ventas_obj = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=f"{PREFIX_COLOMBIA}{archivo_ventas}")
            df_temp = pd.read_csv(io.BytesIO(ventas_obj["Body"].read()), sep=";")
            df_temp = df_temp[df_temp["cod_cliente"].isin(clientes_ruta_test)].reset_index(drop=True)
            # Seleccionar solo columnas que existan
            cols_existentes = [c for c in columnas_ventas if c in df_temp.columns]
            df_temp = df_temp[cols_existentes]
            pan_ventas = pd.concat([pan_ventas, df_temp], ignore_index=True)
        except Exception as e:
            print(f"Archivo {archivo_ventas} no encontrado o error: {e}")

    # cod_articulo_magic ya viene directo en ventas Colombia (ALPHANUMERIC - mantener como string)
    pan_ventas["cod_articulo_magic"] = pan_ventas["cod_articulo_magic"].astype(str).str.strip()
    pan_ventas["desc_marca"] = pan_ventas["desc_marca"].str.strip()

    # Maestro de productos: extraer desde ventas (unique cod_articulo_magic + desc_marca como desc_articulo)
    maestro_prod = pan_ventas[["cod_articulo_magic", "desc_marca"]].drop_duplicates().rename(
        columns={"desc_marca": "desc_articulo"}
    ).groupby("cod_articulo_magic").head(1).reset_index(drop=True)
    maestro_prod.to_csv(os.path.join(OUTPUT_DIR, "CO_maestro_productos.csv"), index=False)

    # Preparar llaves - Compania 3 dígitos para Colombia
    # cod_compania puede venir como "CO01_AJECOL_UN" o "001" - tomar el más reciente por fecha_proceso
    pan_ventas["fecha_proceso"] = pd.to_datetime(pan_ventas["fecha_proceso"], errors="coerce")
    pan_ventas = pan_ventas.sort_values("fecha_proceso", ascending=False)
    compania_map = pan_ventas.groupby("cod_cliente")["cod_compania"].first().reset_index()
    compania_map.columns = ["cod_cliente", "cod_compania_final"]
    pan_ventas = pan_ventas.drop(columns=["cod_compania"]).merge(compania_map, on="cod_cliente", how="left")
    pan_ventas.rename(columns={"cod_compania_final": "cod_compania"}, inplace=True)

    # Convertir cod_compania a formato 1 dígito (Colombia usa "1")
    def format_compania_co(x):
        x = str(x).strip()
        # Si es puramente numérico (posiblemente con decimales), convertir
        if x.replace('.', '', 1).isdigit():
            return str(int(float(x)))
        # Si no es numérico, intentar extraer dígitos o usar "1" por defecto
        digits = ''.join(filter(str.isdigit, x))
        if digits:
            return str(int(digits))
        return "1"

    pan_ventas["cod_compania"] = pan_ventas["cod_compania"].apply(format_compania_co)
    # cod_cliente con prefijo "00" protegido
    pan_ventas["cod_cliente_str"] = pan_ventas["cod_cliente"].astype(str).str.strip()
    pan_ventas["cod_cliente_str"] = pan_ventas["cod_cliente_str"].apply(lambda x: "00" + x if not x.startswith("00") else x)
    pan_ventas["id_cliente"] = "CO|" + pan_ventas["cod_compania"] + "|" + pan_ventas["cod_cliente_str"]

    # Visitas - preparar id_cliente
    pan_visitas["compania__c"] = pan_visitas["compania__c"].apply(format_compania_co)
    pan_visitas["cod_cliente_str"] = pan_visitas["codigo_cliente__c"].astype(str).str.strip()
    pan_visitas["cod_cliente_str"] = pan_visitas["cod_cliente_str"].apply(lambda x: "00" + x if not x.startswith("00") else x)
    pan_visitas["id_cliente"] = "CO|" + pan_visitas["compania__c"] + "|" + pan_visitas["cod_cliente_str"]

    # Filtrar visitas canal 2
    pan_visitas = pan_visitas[pan_visitas.codigo_canal__c == 2].reset_index(drop=True)

    # Última visita (Deduplicación)
    visita_default = (datetime.now(tz_lima) - timedelta(days=7)).strftime("%Y-%m-%d")
    pan_visitas["ultima_visita"] = pan_visitas["ultima_visita"].fillna(visita_default)

    # Deduplicar visitas: priorizar la fila que contenga el día de mañana
    dia_actual = datetime.now(tz_lima).weekday() + 1
    dia_siguiente = 7 if dia_actual == 6 else (dia_actual + 1) % 7
    pan_visitas["tiene_dia_manana"] = pan_visitas["dias_de_visita__c"].astype(str).apply(lambda x: 1 if str(dia_siguiente) in x.split(";") else 0)
    pan_visitas = pan_visitas.sort_values(["id_cliente", "tiene_dia_manana", "ultima_visita"], ascending=[True, False, False]).groupby("id_cliente").head(1)
    pan_visitas = pan_visitas.drop(columns=["tiene_dia_manana"])

    # Cruce Ventas y Visitas - cod_ruta y cod_modulo vienen SOLO de visitas
    cols_visitas = ["id_cliente", "dias_de_visita__c", "periodo_de_visita__c", "ultima_visita", "cod_ruta", "cod_modulo", "eje_potencial__c"]
    cols_visitas_existentes = [c for c in cols_visitas if c in pan_visitas.columns]
    df_merged = pd.merge(pan_ventas, pan_visitas[cols_visitas_existentes], on="id_cliente", how="inner")

    # cod_ruta y cod_modulo vienen directamente de visitas (no hay sufijos porque ventas no tiene estos campos)
    df_merged["cod_ruta"] = df_merged["cod_ruta"].astype(int)
    df_merged["cod_modulo"] = df_merged["cod_modulo"].astype(int)

    # Segmentación
    if "eje_potencial__c" in df_merged.columns:
        mapping_segmento = {"S1": "BLINDAR", "S2": "DESARROLLAR", "S4": "MANTENER", "S5": "OPTIMIZAR"}
        df_merged["new_segment"] = df_merged["eje_potencial__c"].map(mapping_segmento).fillna("OPTIMIZAR")
    else:
        df_merged["new_segment"] = "OPTIMIZAR"

    df_merged["mes"] = pd.to_datetime(df_merged["fecha_liquidacion"]).dt.strftime("%Y-%m-01")

    return df_merged


def filtrar_visitas_manana(df):
    """Filtra clientes con visita programada para mañana."""
    dia_actual = datetime.now(tz_lima).weekday() + 1
    dia_siguiente = 7 if dia_actual == 6 else (dia_actual + 1) % 7

    df_final = df[
        df["dias_de_visita__c"].astype(str).apply(lambda x: str(dia_siguiente) in x.split(";"))
    ].reset_index(drop=True)
    return df_final


def preparar_rutas_y_pesos(df_ventas):
    """Calcula pesos por giro y divide los datasets por ruta."""
    df_ventas["fecha_liquidacion"] = pd.to_datetime(df_ventas["fecha_liquidacion"], format="%Y-%m-%d")
    df_ventas["desc_marca"] = df_ventas["desc_marca"].str.strip()
    df_ventas["desc_categoria"] = df_ventas["desc_categoria"].str.strip()

    mapeo_diccionario = {}
    for giro_v in df_ventas["desc_subgiro"].unique():
        temp = df_ventas[(df_ventas["desc_subgiro"] == giro_v)]
        ranks = temp.groupby("desc_categoria")["cant_cajafisicavta"].sum().reset_index()
        ranks.columns = ["index", "desc_categoria"]
        ranks = ranks.sort_values(by="desc_categoria", ascending=False)
        if len(ranks) <= 5:
            ranks["Ranking"] = range(1, len(ranks) + 1)
        else:
            a = list(ranks["desc_categoria"])
            b = [1, 1, 2, 2]
            multiplicador = 4 if (np.std(a) / np.mean(a) <= 1.2) else 2
            for i in range(4, len(a)):
                if a[3] <= a[i] * multiplicador:
                    b.append(3)
                else:
                    b.append(3 + i)
            ranks["Ranking"] = b
        mapeo_diccionario[giro_v] = ranks.set_index("index")["Ranking"].to_dict()

    with open(os.path.join(OUTPUT_DIR, "mapeo_diccionario.json"), "w") as f:
        json.dump(mapeo_diccionario, f)

    # División por Rutas
    rutas = df_ventas.groupby(["cod_ruta"])["id_cliente"].nunique().sort_values(ascending=False).reset_index()["cod_ruta"].unique()
    low_sku_ruta = []

    for ruta in rutas:
        temp = df_ventas[df_ventas["cod_ruta"] == ruta]
        if temp["cod_articulo_magic"].nunique() < 5:
            low_sku_ruta.append(ruta)
        else:
            temp.to_csv(os.path.join(OUTPUT_DIR, f"rutas/D_{ruta}_ventas.csv"), index=False)

    if low_sku_ruta:
        temp_low = df_ventas[df_ventas["cod_ruta"].isin(low_sku_ruta)]
        temp_low.to_csv(os.path.join(OUTPUT_DIR, "rutas/D_low_ruta_ventas.csv"), index=False)


def main():
    print("Iniciando Limpieza de Datos (Colombia)...")
    # comprobar_inputs()

    print("Extrayendo y cruzando ventas/visitas...")
    df_maestro = extraer_datos()

    print("Filtrando clientes a visitar mañana...")
    df_manana = filtrar_visitas_manana(df_maestro)

    ruta_ventas_manana = os.path.join(OUTPUT_DIR, "colombia_ventas_manana.parquet")
    df_manana.to_parquet(ruta_ventas_manana, index=False)

    print("Preparando archivos por ruta y calculando pesos...")
    preparar_rutas_y_pesos(df_manana)

    print(f"Limpieza finalizada exitosamente. Archivos guardados en {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
