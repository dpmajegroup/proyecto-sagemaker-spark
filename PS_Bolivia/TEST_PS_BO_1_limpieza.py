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

# Parámetros Globales - Bolivia
BUCKET_ARTIFACTS = "aje-prd-analytics-artifacts-s3"
PREFIX_BOLIVIA = "pedido_sugerido/data-v1/bolivia/"
COD_PAIS = "BO"
COD_COMPANIA = 150

RUTAS_BOLIVIA = [
    # Cochabamba
    3101, 3102, 3103, 3104, 3105, 3106, 3107, 3108, 3109, 3110,
    # La paz
    # 4101, 4102, 4103, 4104, 4105, 4106, 4107, 4108, 4109, 4110, 4111, 4112,
    # 4201, 4202, 4203, 4204, 4205, 4206,
    # Santa cruz
    2101, 2102, 2103, 2104, 2105, 2106, 2107, 2108,
    2201, 2202, 2203, 2204, 2205, 2206, 2207, 2208,
    # Santa crz (clientes sin PP)
    # 2301, 2302, 2303, 2304, 2305, 2306, 2307, 2308
]

tz_lima = pytz.timezone("America/Lima")


def comprobar_inputs():
    """Verifica que los archivos en S3 existan y hayan sido modificados hoy."""
    s3 = boto3.client("s3")
    hoy = datetime.now(tz_lima).date()
    errores = []

    objetos = s3.list_objects_v2(Bucket=BUCKET_ARTIFACTS, Prefix=PREFIX_BOLIVIA)
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
    """Descarga maestro, visitas, ventas. Aplica filtros de Bolivia."""
    s3 = my_session.client("s3")

    # 1. Maestro de Productos
    maestro_obj = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=f"{PREFIX_BOLIVIA}maestro_productos_bolivia000")
    maestro_prod = pd.read_csv(io.BytesIO(maestro_obj["Body"].read()), sep=";")
    maestro_prod = maestro_prod[["cod_articulo_magic", "desc_articulo"]].drop_duplicates().groupby("cod_articulo_magic").head(1).reset_index(drop=True)
    maestro_prod.to_csv(os.path.join(OUTPUT_DIR, "BO_maestro_productos.csv"), index=False)

    # 2. Descargar Visitas
    visitas_obj = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=f"{PREFIX_BOLIVIA}visitas_bolivia000")
    pan_visitas = pd.read_csv(io.BytesIO(visitas_obj["Body"].read()), sep=";")
    pan_visitas = pan_visitas[pan_visitas["cod_ruta"].isin(RUTAS_BOLIVIA)].reset_index(drop=True)
    clientes_ruta_test = pan_visitas["codigo_cliente__c"].unique()

    # 3. Descargar Ventas
    columnas_ventas = [
        'id_cliente', 'id_sucursal', 'id_producto', 'fecha_liquidacion', 'cod_ruta', 'cod_modulo',
        'cod_zona', 'cant_cajafisicavta', 'cant_cajaunitvta', 'imp_netovta', 'cod_compania',
        'desc_compania', 'cod_sucursal', 'desc_sucursal', 'cod_pais', 'fecha_creacion_cliente',
        'cod_cliente', 'desc_marca', 'desc_formato', 'desc_categoria', 'cod_giro', 'cod_subgiro',
        'desc_giro', 'desc_subgiro', 'fecha_proceso'
    ]
    pan_ventas = pd.DataFrame()
    for archivo_ventas in ["ventas_bolivia000", "ventas_bolivia001"]:
        try:
            ventas_obj = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=f"{PREFIX_BOLIVIA}{archivo_ventas}")
            df_temp = pd.read_csv(io.BytesIO(ventas_obj["Body"].read()), sep=";")
            df_temp = df_temp[
                (df_temp["cod_ruta"].isin(RUTAS_BOLIVIA)) | (df_temp["cod_cliente"].isin(clientes_ruta_test))
            ].reset_index(drop=True)
            # Seleccionar solo columnas que existan
            cols_existentes = [c for c in columnas_ventas if c in df_temp.columns]
            df_temp = df_temp[cols_existentes]
            pan_ventas = pd.concat([pan_ventas, df_temp], ignore_index=True)
        except Exception as e:
            print(f"Archivo {archivo_ventas} no encontrado o error: {e}")

    # Extraer cod_articulo_magic
    pan_ventas["cod_articulo_magic"] = pan_ventas["id_producto"].str.split("|").str[-1].astype(int)
    pan_ventas["desc_marca"] = pan_ventas["desc_marca"].str.strip()

    # Preparar llaves - id_cliente con prefijo BO
    pan_ventas["cod_compania"] = pan_ventas["cod_compania"].astype(str).apply(lambda x: str(int(float(x))).rjust(4, "0") if x.replace('.','',1).isdigit() else str(x).rjust(4, "0"))
    pan_ventas["id_cliente"] = "BO|" + pan_ventas["cod_compania"] + "|" + pan_ventas["cod_cliente"].astype(int).astype(str)

    pan_visitas["compania__c"] = pan_visitas["compania__c"].astype(str).apply(lambda x: str(int(float(x))).rjust(4, "0") if x.replace('.','',1).isdigit() else str(x).rjust(4, "0"))
    pan_visitas["id_cliente"] = "BO|" + pan_visitas["compania__c"] + "|" + pan_visitas["codigo_cliente__c"].astype(int).astype(str)

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

    # Cruce Ventas y Visitas
    cols_visitas = ["id_cliente", "dias_de_visita__c", "periodo_de_visita__c", "ultima_visita", "cod_ruta", "cod_modulo", "eje_potencial__c"]
    cols_visitas_existentes = [c for c in cols_visitas if c in pan_visitas.columns]
    df_merged = pd.merge(pan_ventas, pan_visitas[cols_visitas_existentes], on="id_cliente", how="inner", suffixes=("_vta", "_vis"))

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
    print("Iniciando Limpieza de Datos (Bolivia)...")
    # comprobar_inputs()

    print("Extrayendo y cruzando ventas/visitas...")
    df_maestro = extraer_datos()

    print("Filtrando clientes a visitar mañana...")
    df_manana = filtrar_visitas_manana(df_maestro)

    ruta_ventas_manana = os.path.join(OUTPUT_DIR, "bolivia_ventas_manana.parquet")
    df_manana.to_parquet(ruta_ventas_manana, index=False)

    print("Preparando archivos por ruta y calculando pesos...")
    preparar_rutas_y_pesos(df_manana)

    print(f"Limpieza finalizada exitosamente. Archivos guardados en {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
