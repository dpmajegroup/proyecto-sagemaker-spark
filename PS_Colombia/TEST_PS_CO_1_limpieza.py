import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pyarrow==17.0.0"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "numpy", "pandas"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "awswrangler[redshift]", "--no-build-isolation"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "redshift-connector", "openpyxl"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])

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
BUCKET_DATALAKE = "aje-prod-datalake-399723489351-us-east-2-landing-s3"
KEY_VENTAS_CO = "analytics/pedido_sugerido/sellin/data/colombia/ventas/000"
# Visitas ahora se leen del bucket de artifacts (similar a otros paises)
BUCKET_ARTIFACTS = "aje-prd-analytics-artifacts-s3"
KEY_VISITAS_CO = "pedido_sugerido/data-v1/colombia/visitas_colombia000"
COD_PAIS = "CO"
COD_COMPANIA = "1"

RUTAS_COLOMBIA = [
# Rutas Bogota (solo PS)
    1101,1102,1103,1104,1105,1106,1107,1110,1111,1201,1202,1203,1204,1205,1206,1207,1208,1209,1210,1301,
    1302,1303,1304,1305,1306,1307,1308,1309,1310,1401,1402,1403,1404,1405,1406,1407,1408,1409,1410,1701,
    1702,1703,1704,1705,1706,1707,1708,2101,2102,2103,2104,2105,2106,2107,2108,2109,2110,2201,2202,2203,
    2204,2205,2206,2207,2208,2209,2210,2301,2302,2303,2304,2305,2306,2307,2308,2309,2310,2401,2402,2403,
    2404,2405,2406,2407,2408,2409,2410,2501,2502,2503,2504,2505,2506,2507,2508,2509,2510,3101,3102,3103,
    3104,3105,3106,3107,3108,3109,3110,3201,3202,3203,3204,3205,3206,3207,3208,3209,3210,3301,3302,3303,
    3304,3305,3306,3307,3308,3309,3310,3401,3402,3403,3404,3405,3406,3407,3408,3409,3410,3501,3502,3503,
    3504,3505,3506,3507,3508,3509,3510,3601,3602,3603,3604,3605,3606,3607,3608,3609,3610,3701,3702,3703,
    3704,3705,3706,3707,3708,3709,3710,
    
# Rutas Barranquilla (Pedido recurrente)
    10109,10105,10103,10104,10101,10102,10106,10108,10107,10207,
    10209,10208,10203,10201,10202,10205,10206,10204,10307,10303,10301,10302,10305,10306,10304,10308,10407,10410,10405,10406,
    10403,10404,10402,10401,10408,10409,10504,10502,10503,10505,10506,10510,10509,10508,10501,10507
]

tz_lima = pytz.timezone("America/Lima")

# Exclusión de SKUs no permitidos por el cliente (Excel en S3)
BUCKET_SKU_EXCL = "aje-dl-prod-us-east-2-399723489351-external-data"
PREFIX_SKU_EXCL = "aje/analiticaAvanzada/co/sku_venta/"


def excluir_sku_no_permitidos(df):
    """Excluye SKUs que empiezan con 'MER' y los listados en el Excel de SKU no permitidos del cliente.

    Trabaja sobre ventas con columnas cod_compania, cod_sucursal y cod_articulo_magic.
    """
    print("Excluyendo SKUs no permitidos...")
    s3 = boto3.client("s3")

    # 1. Excluir SKUs que empiezan con 'MER'
    n_antes_mer = len(df)
    df = df[~df["cod_articulo_magic"].astype(str).str.startswith("MER")].reset_index(drop=True)
    print(f"  Excluidos por MER: {n_antes_mer - len(df):,}")

    # 2. Excluir SKUs del Excel de S3 (por compania-sucursal-producto)
    try:
        fecha_manana_str = (datetime.now(tz_lima) + timedelta(days=1)).strftime("%d_%m_%Y")
        key_ideal = f"{PREFIX_SKU_EXCL}PS_Carga_SKU_{fecha_manana_str}.xlsx"

        try:
            response_sku = s3.get_object(Bucket=BUCKET_SKU_EXCL, Key=key_ideal)
            print(f"  Exclusion SKU: usando {key_ideal}")
        except Exception:
            paginator = s3.get_paginator("list_objects_v2")
            all_files = []
            for page in paginator.paginate(Bucket=BUCKET_SKU_EXCL, Prefix=PREFIX_SKU_EXCL):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".xlsx"):
                        all_files.append(obj)
            if all_files:
                all_files.sort(key=lambda x: x["LastModified"], reverse=True)
                key_ideal = all_files[0]["Key"]
                response_sku = s3.get_object(Bucket=BUCKET_SKU_EXCL, Key=key_ideal)
                print(f"  Exclusion SKU: usando mas reciente {key_ideal}")
            else:
                raise FileNotFoundError("No se encontraron archivos de exclusion SKU")

        df_excl = pd.read_excel(io.BytesIO(response_sku["Body"].read()), sheet_name="Hoja1")
        df_excl.columns = ["fecha_carga", "cod_pais", "cod_compania", "cod_sucursal", "cod_producto"]
        df_excl["cod_compania"] = df_excl["cod_compania"].astype(str).str.strip()
        df_excl["cod_sucursal"] = df_excl["cod_sucursal"].astype(str).str.strip().str.zfill(2)
        df_excl["cod_producto"] = df_excl["cod_producto"].astype(str).str.strip()

        excl_keys = set(
            df_excl.apply(lambda r: f"{r['cod_compania']}|{r['cod_sucursal']}|{r['cod_producto']}", axis=1)
        )

        df["_key"] = (
            df["cod_compania"].astype(str).str.strip() + "|" +
            df["cod_sucursal"].astype(str).str.strip().str.zfill(2) + "|" +
            df["cod_articulo_magic"].astype(str).str.strip()
        )

        n_antes_excl = len(df)
        df = df[~df["_key"].isin(excl_keys)].reset_index(drop=True)
        df.drop(columns=["_key"], inplace=True)
        print(f"  Excluidos por Excel SKU: {n_antes_excl - len(df):,}")

    except FileNotFoundError as e:
        print(f"  Advertencia: {e}. No se aplicó exclusión por Excel.")
    except Exception as e:
        print(f"  Error al leer Excel de exclusión: {e}. No se aplicó exclusión por Excel.")

    print(f"  Resultado tras exclusiones: {df.shape[0]:,} filas, {df.cod_articulo_magic.nunique():,} SKUs")
    return df


def comprobar_inputs():
    """Verifica que los archivos en S3 existan y hayan sido modificados hoy."""
    s3 = boto3.client("s3")
    hoy = datetime.now(tz_lima).date()
    errores = []

    objetos = s3.list_objects_v2(Bucket=BUCKET_DATALAKE, Prefix="analytics/pedido_sugerido/sellin/data/colombia/")
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
    visitas_obj = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=KEY_VISITAS_CO)
    pan_visitas = pd.read_csv(io.BytesIO(visitas_obj["Body"].read()), sep=";")
    pan_visitas = pan_visitas[pan_visitas["cod_ruta"].isin(RUTAS_COLOMBIA)].reset_index(drop=True)
    clientes_ruta_test = pan_visitas["codigo_cliente__c"].unique()
    print(f"  [Visitas] Filas filtradas por rutas: {len(pan_visitas):,} | Clientes únicos: {len(clientes_ruta_test):,}")

    # 2. Descargar Ventas (tienen cod_ruta y cod_modulo, cod_articulo_magic viene de cod_producto)
    columnas_ventas = [
        'id_cliente', 'id_sucursal', 'id_producto', 'fecha_liquidacion',
        'cod_zona', 'cod_ruta', 'cod_modulo', 'cant_cajafisica_vta', 'cant_cajaunitaria_vta',
        'imp_neto_vta_mn', 'cod_compania', 'desc_compania', 'cod_sucursal',
        'desc_sucursal', 'cod_pais', 'fecha_creacion_cliente',
        'cod_cliente', 'cod_producto', 'desc_marca', 'desc_formato', 'desc_categoria', 'cod_giro', 'cod_subgiro',
        'desc_giro', 'desc_subgiro', 'fecha_proceso'
    ]
    pan_ventas = pd.DataFrame()
    for key_ventas in [KEY_VENTAS_CO]:
        try:
            ventas_obj = s3.get_object(Bucket=BUCKET_DATALAKE, Key=key_ventas)
            df_temp = pd.read_csv(io.BytesIO(ventas_obj["Body"].read()), sep=";")
            df_temp = df_temp[df_temp["cod_cliente"].isin(clientes_ruta_test)].reset_index(drop=True)
            # Seleccionar solo columnas que existan
            cols_existentes = [c for c in columnas_ventas if c in df_temp.columns]
            df_temp = df_temp[cols_existentes]
            pan_ventas = pd.concat([pan_ventas, df_temp], ignore_index=True)
        except Exception as e:
            print(f"Archivo {key_ventas} no encontrado o error: {e}")

    # cod_articulo_magic viene de cod_producto (ALPHANUMERIC - mantener como string)
    pan_ventas["cod_articulo_magic"] = pan_ventas["cod_producto"].astype(str).str.strip()
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

    # Construir id_cliente: ventas ya trae id_cliente como '1|001860051170', solo agregar 'CO|'
    pan_ventas["id_cliente"] = "CO|" + pan_ventas["id_cliente"].astype(str).str.strip()
    print(f"  [Ventas] Filas: {len(pan_ventas):,} | Clientes únicos: {pan_ventas.id_cliente.nunique():,} | SKUs: {pan_ventas.cod_articulo_magic.nunique():,}")

    # cod_articulo_magic viene de cod_producto (alfanumérico)
    pan_ventas["cod_articulo_magic"] = pan_ventas["cod_producto"].astype(str).str.strip()

    # Visitas: codigo_unico__c viene como '1|00141374451', solo agregar 'CO|'
    pan_visitas["id_cliente"] = "CO|" + pan_visitas["codigo_unico__c"].astype(str).str.strip()

    # Filtrar visitas canal 2 y compania 1
    pan_visitas['compania__c'] = pan_visitas['compania__c'].astype(str).str.strip()
    pan_visitas = pan_visitas[(pan_visitas.codigo_canal__c == 2) & (pan_visitas.compania__c.isin(['1', '01', '001', '0001']))].reset_index(drop=True)
    print(f"  [Visitas] Después filtro canal 2 + compañía 1: {len(pan_visitas):,} clientes")

    # Última visita (Deduplicación)
    visita_default = (datetime.now(tz_lima) - timedelta(days=7)).strftime("%Y-%m-%d")
    pan_visitas["ultima_visita"] = pan_visitas["ultima_visita"].fillna(visita_default)

    # Filtrar visitas para mañana PRIMERO (como en notebook PROD_1)
    # Colombia/Lima = UTC-5. El contenedor SageMaker usa UTC.
    _utc_now = datetime.utcnow()
    _local_now = _utc_now - timedelta(hours=5)  # UTC-5
    _manana = _local_now + timedelta(days=1)
    dia_siguiente = _manana.isoweekday()  # 1=Lun...7=Dom
    print(f"  [Visitas] Local (UTC-5): {_local_now.strftime('%A %Y-%m-%d %H:%M')} | Mañana día: {dia_siguiente}")
    pan_visitas = pan_visitas[pan_visitas["dias_de_visita__c"].astype(str).apply(lambda x: str(dia_siguiente) in x.split(";"))].reset_index(drop=True)
    print(f"  [Visitas] Con visita día {dia_siguiente}: {len(pan_visitas):,} filas | {pan_visitas.id_cliente.nunique():,} clientes")

    # Deduplicar visitas después del filtro
    pan_visitas = pan_visitas.sort_values(["id_cliente", "ultima_visita"], ascending=[True, False]).groupby("id_cliente").head(1)

    # Cruce Ventas y Visitas - cod_ruta y cod_modulo vienen de AMBOS, priorizar visitas
    cols_visitas = ["id_cliente", "dias_de_visita__c", "periodo_de_visita__c", "ultima_visita", "cod_ruta", "cod_modulo", "eje_potencial__c"]
    cols_visitas_existentes = [c for c in cols_visitas if c in pan_visitas.columns]
    df_merged = pd.merge(pan_ventas, pan_visitas[cols_visitas_existentes], on="id_cliente", how="inner", suffixes=("_vta", "_vis"))
    print(f"  [Merge] Ventas x Visitas (inner): {len(df_merged):,} filas | Clientes: {df_merged.id_cliente.nunique():,}")

    # Priorizar cod_ruta y cod_modulo de visitas (con fallback a ventas)
    df_merged["cod_ruta"] = df_merged["cod_ruta_vis"].combine_first(df_merged["cod_ruta_vta"]).astype(int)
    df_merged["cod_modulo"] = df_merged["cod_modulo_vis"].combine_first(df_merged["cod_modulo_vta"]).astype(int)
    df_merged = df_merged.drop(columns=["cod_ruta_vta", "cod_ruta_vis", "cod_modulo_vta", "cod_modulo_vis"])

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
        ranks = temp.groupby("desc_categoria")["cant_cajafisica_vta"].sum().reset_index()
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
    rutas_procesadas = 0

    for ruta in rutas:
        temp = df_ventas[df_ventas["cod_ruta"] == ruta]
        n_skus = temp["cod_articulo_magic"].nunique()
        n_clientes = temp["id_cliente"].nunique()
        if n_skus < 5:
            low_sku_ruta.append(ruta)
        else:
            temp.to_csv(os.path.join(OUTPUT_DIR, f"rutas/D_{ruta}_ventas.csv"), index=False)
            rutas_procesadas += 1

    print(f"  [Rutas] Total: {len(rutas)} | Procesadas (≥5 SKUs): {rutas_procesadas} | Low SKU (<5): {len(low_sku_ruta)}")
    if low_sku_ruta:
        temp_low = df_ventas[df_ventas["cod_ruta"].isin(low_sku_ruta)]
        temp_low.to_csv(os.path.join(OUTPUT_DIR, "rutas/D_low_ruta_ventas.csv"), index=False)
        print(f"  [Low SKU] Rutas agrupadas: {len(low_sku_ruta)} | Clientes: {temp_low.id_cliente.nunique()} | SKUs: {temp_low.cod_articulo_magic.nunique()}")


def main():
    print("Iniciando Limpieza de Datos (Colombia)...")
    # comprobar_inputs()

    print("Extrayendo y cruzando ventas/visitas...")
    df_maestro = extraer_datos()

    # Excluir SKUs no permitidos por el cliente (MER + Excel de S3)
    df_maestro = excluir_sku_no_permitidos(df_maestro)

    # Ya no necesita filtrar por día de mañana (se hizo en extraer_datos antes del merge)
    df_manana = df_maestro
    print(f"  [Final] Clientes para mañana: {df_manana.id_cliente.nunique():,} | Filas: {len(df_manana):,} | Rutas: {df_manana.cod_ruta.nunique():,}")

    ruta_ventas_manana = os.path.join(OUTPUT_DIR, "colombia_ventas_manana.parquet")
    df_manana.to_parquet(ruta_ventas_manana, index=False)

    print("Preparando archivos por ruta y calculando pesos...")
    preparar_rutas_y_pesos(df_manana)

    print(f"Limpieza finalizada exitosamente. Archivos guardados en {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
