import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pyarrow==17.0.0"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "numpy", "pandas"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "awswrangler[redshift]", "--no-build-isolation"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "redshift-connector", "openpyxl"])

import os
import json
import re
import boto3
import pytz
import pandas as pd
import numpy as np
import awswrangler as wr
from datetime import datetime, timedelta

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
my_session = boto3.Session(region_name="us-east-2")

INPUT_DIR_LIMPIEZA = "/opt/ml/processing/input/limpieza"
INPUT_DIR_MODELADO = "/opt/ml/processing/input/modelado"

S3_BUCKET_BACKUP = "aje-analytics-ps-backup"
S3_PREFIX_OUTPUT = "PS_Nicaragua/Output/PS_piloto_v1/"
# D&A va a PS_Guatemala (así está en el notebook original)
S3_PREFIX_OUTPUT_DATA = "PS_Guatemala/Output/PS_data_piloto_v1/"

SKUS_SIN_PRECIO = []

# Bucket y prefix para Excel de SKUs disponibles por compañía-sucursal
BUCKET_SKU_EXCEL = "aje-dl-prod-us-east-2-399723489351-external-data"
PREFIX_SKU_EXCEL = "aje/analiticaAvanzada/gt/PS_Carga_SKU_"

tz_lima = pytz.timezone("America/Lima")
fecha_actual = datetime.now(tz_lima)
fecha_tomorrow = (fecha_actual + timedelta(days=1)).strftime("%Y-%m-%d")


def clasificar_valor(x):
    if x > 0: return "S"
    elif x == 0: return "M"
    else: return "B"


def cargar_sku_disponible():
    """Carga el Excel de SKUs disponibles por compañía-sucursal desde S3."""
    fecha_manana = (datetime.now(tz_lima) + timedelta(days=1)).strftime("%d_%m_%Y")
    local_path = "/opt/ml/processing/PS_Carga_SKU.xlsx"

    s3 = boto3.client('s3')

    # 1. Intentar descargar archivo de mañana
    key_manana = f"{PREFIX_SKU_EXCEL}{fecha_manana}.xlsx"
    try:
        s3.download_file(BUCKET_SKU_EXCEL, key_manana, local_path)
        print(f"Cargado archivo de mañana: {fecha_manana}")
        return pd.read_excel(local_path, sheet_name="Hoja1")
    except Exception:
        print(f"No se encontró archivo para {fecha_manana}. Buscando el más reciente...")

    # 2. Buscar el último archivo disponible con el patrón
    response = s3.list_objects_v2(Bucket=BUCKET_SKU_EXCEL, Prefix=PREFIX_SKU_EXCEL)

    if 'Contents' in response:
        archivos = [obj for obj in response['Contents'] if obj['Key'].endswith('.xlsx')]
        ultimo_archivo = sorted(archivos, key=lambda x: x['LastModified'], reverse=True)[0]
        s3.download_file(BUCKET_SKU_EXCEL, ultimo_archivo['Key'], local_path)
        print(f"Cargado archivo más reciente: {ultimo_archivo['Key']}")
        return pd.read_excel(local_path, sheet_name="Hoja1")
    else:
        raise FileNotFoundError("No se encontraron archivos con el patrón PS_Carga_SKU_")


def paso_5_1_maestro_validacion(pan_rec):
    """5.1 Usar archivo maestro para no recomendar SKUs que no se deben a las rutas."""
    print("5.1 Aplicando validación de maestro de productos...")
    # id_cliente ya viene con prefijo CAM| del modelado

    s3_path_val = "s3://aje-prd-analytics-artifacts-s3/pedido_sugerido/data-v1/cam/maestro_productos_cam000"
    skus_val = wr.s3.read_csv(s3_path_val, sep=";", boto3_session=my_session)
    skus_val = skus_val[skus_val.cod_compania == 81].copy()
    skus_val["cod_compania"] = skus_val["cod_compania"].astype(str).str.zfill(4)
    skus_val["id_cliente"] = "CAM|" + skus_val["cod_compania"] + "|" + skus_val["cod_cliente"].astype(str)

    pan_rec = pd.merge(pan_rec, skus_val[['cod_articulo_magic', 'id_cliente']].drop_duplicates(),
                       on=["id_cliente", "cod_articulo_magic"], how="inner")
    pan_rec = pan_rec.merge(df_ventas_global[["id_cliente", "cod_ruta"]].drop_duplicates(), on="id_cliente", how="left")
    return pan_rec


def paso_5_2_quitar_compras_recientes(pan_rec, df_ventas):
    """5.2 Quitar SKUs de las últimas 2 semanas (evitar recompra)."""
    print("5.2 Quitando compras de las últimas 2 semanas...")
    marca_articulo = df_ventas[["desc_categoria", "cod_articulo_magic"]].drop_duplicates()
    cliente_rec_marca = pd.merge(pan_rec, marca_articulo, on="cod_articulo_magic", how="left")
    cliente_rec_marca["desc_categoria"] = cliente_rec_marca["desc_categoria"].str.strip()

    last_2_weeks = (datetime.now() - timedelta(days=11)).strftime('%Y-%m-%d')
    df_ventas["fecha_liquidacion"] = pd.to_datetime(df_ventas["fecha_liquidacion"]).dt.strftime('%Y-%m-%d')

    df_quitar = df_ventas[df_ventas["fecha_liquidacion"] >= last_2_weeks][["id_cliente", "cod_articulo_magic"]].drop_duplicates()
    cliente_rec_sin = cliente_rec_marca.merge(df_quitar, on=['id_cliente', 'cod_articulo_magic'], how='left', indicator=True)
    cliente_rec_sin = cliente_rec_sin[cliente_rec_sin['_merge'] == 'left_only'].drop(columns=['_merge'])

    return cliente_rec_sin, cliente_rec_marca


def paso_5_3_filtro_sku_disponible(pan_rec, df_ventas):
    """5.3 Filtrar SKUs específicos por compañía-sucursal usando Excel desde S3."""
    print("5.3 Filtrando SKUs por compañía-sucursal (Excel PS_Carga_SKU)...")
    test = pd.merge(pan_rec, df_ventas[["id_cliente", "cod_compania", "cod_sucursal"]].drop_duplicates(), on="id_cliente", how="left")
    test["cod_compania"] = test["cod_compania"].astype(str).apply(lambda x: str(int(x)).rjust(4, "0"))
    test["cod_sucursal"] = test["cod_sucursal"].astype(str).apply(lambda x: str(int(x)).rjust(2, "0"))

    sku_disponible = cargar_sku_disponible()
    sku_disponible.rename(columns={'cod_compañia': 'cod_compania', 'cod_producto': 'cod_articulo_magic'}, inplace=True)
    sku_disponible["cod_compania"] = sku_disponible["cod_compania"].astype(str).apply(lambda x: str(int(x)).rjust(4, "0"))
    sku_disponible["cod_sucursal"] = sku_disponible["cod_sucursal"].astype(str).apply(lambda x: str(int(x)).rjust(2, "0"))

    test = pd.merge(test, sku_disponible[["cod_compania", "cod_sucursal", "cod_articulo_magic"]].drop_duplicates(),
                     on=["cod_compania", "cod_sucursal", "cod_articulo_magic"], how="inner")

    pan_rec = test[["id_cliente", "cod_articulo_magic"]].copy()
    return pan_rec


def paso_5_4_subida_bajada_mantener(pan_rec, df_ventas):
    """5.4 Recomendar de acuerdo a Subida, Bajada, Mantener."""
    print("5.4 Aplicando flag S/M/B...")
    pan_rec = pd.merge(pan_rec, df_ventas[["id_cliente", "cod_ruta"]].drop_duplicates(), on="id_cliente", how="left")

    fecha_30dias = (fecha_actual - timedelta(days=30)).strftime('%Y-%m-%d')
    fecha_60dias = (fecha_actual - timedelta(days=60)).strftime('%Y-%m-%d')

    df_ultimos_30 = df_ventas[(df_ventas['fecha_liquidacion'] > fecha_30dias) & (df_ventas['fecha_liquidacion'] <= fecha_actual.strftime('%Y-%m-%d'))]
    df_31_60 = df_ventas[(df_ventas['fecha_liquidacion'] > fecha_60dias) & (df_ventas['fecha_liquidacion'] <= fecha_30dias)]

    ventas_30 = df_ultimos_30.groupby(['cod_ruta', 'cod_articulo_magic'])['imp_netovta'].sum().reset_index().assign(mes="0_30")
    ventas_60 = df_31_60.groupby(['cod_ruta', 'cod_articulo_magic'])['imp_netovta'].sum().reset_index().assign(mes="31_60")

    df_grouped = pd.concat([ventas_30, ventas_60], ignore_index=True)
    df_grouped = pd.pivot_table(df_grouped, values="imp_netovta", index=["cod_ruta", "cod_articulo_magic"], columns=["mes"], aggfunc="sum").reset_index().fillna(0)
    df_grouped["vp"] = ((df_grouped.get("0_30", 0) - df_grouped.get("31_60", 0)) / df_grouped.get("31_60", 1) * 100).fillna(-1).replace([np.inf, -np.inf], -1)
    df_grouped["flag_rank"] = df_grouped["vp"].apply(clasificar_valor).map({"S": 0, "M": 1, "B": 2})

    pan_rec["original_order"] = pan_rec.index
    pan_rec = pd.merge(pan_rec, df_grouped[["cod_ruta", "cod_articulo_magic", "flag_rank"]], on=["cod_ruta", "cod_articulo_magic"], how="left")
    pan_rec["flag_rank"] = pan_rec["flag_rank"].fillna(3)
    pan_rec = pan_rec.sort_values(by=["id_cliente", "flag_rank", "original_order"]).reset_index(drop=True)
    pan_rec = pan_rec[["id_cliente", "cod_articulo_magic"]].reset_index(drop=True)
    return pan_rec


def paso_5_6_quitar_skus_sin_precio(pan_rec):
    """5.6 Quitar SKUs específicos sin precio."""
    print("5.6 Quitando SKUs sin precio...")
    pan_rec = pan_rec[~pan_rec["cod_articulo_magic"].isin(SKUS_SIN_PRECIO)].reset_index(drop=True)
    return pan_rec


def paso_5_7_despriorizar_historico(pan_rec):
    """5.7 Despriorizar (no eliminar) recomendaciones de las últimas 2 semanas desde S3."""
    print("5.7 Despriorizando recomendaciones históricas...")
    s3 = my_session.client("s3")
    objetos = s3.list_objects_v2(Bucket=S3_BUCKET_BACKUP, Prefix=S3_PREFIX_OUTPUT)
    fechas_recs = []

    if "Contents" in objetos:
        for obj in objetos["Contents"]:
            if obj["Key"].endswith(".csv") and "D_base_pedidos_" in obj["Key"]:
                fecha_str = obj["Key"].split("_")[-1].replace(".csv", "")
                if len(fecha_str) == 10 and fecha_str[4] == "-":
                    fechas_recs.append(fecha_str)

    last_14_days = [f for f in sorted(fechas_recs) if f != fecha_tomorrow][-14:]

    if len(last_14_days) > 0:
        last_recs = pd.DataFrame()
        for fecha in last_14_days:
            s3_uri = f"s3://{S3_BUCKET_BACKUP}/{S3_PREFIX_OUTPUT}D_base_pedidos_{fecha}.csv"
            try:
                df_temp = wr.s3.read_csv(s3_uri, dtype={"Compania": str, "Cliente": str}, boto3_session=my_session)
                df_temp["id_cliente"] = 'CAM|' + df_temp['Compania'] + '|' + df_temp['Cliente']
                df_temp = df_temp[df_temp["id_cliente"].isin(pan_rec["id_cliente"].unique())]
                last_recs = pd.concat([last_recs, df_temp], axis=0)
                print(f"{fecha} done")
            except Exception as e:
                print(f"No se pudo leer {s3_uri}: {e}")

        if not last_recs.empty:
            df_combinado = pd.merge(pan_rec, last_recs, left_on=['id_cliente', 'cod_articulo_magic'],
                                     right_on=['id_cliente', 'Producto'], how='left', indicator=True)
            # Únicos (no recomendados antes) van primero
            df_unicos = df_combinado[df_combinado["_merge"] == "left_only"][["id_cliente", "cod_articulo_magic"]].drop_duplicates()
            # Coincidentes (ya recomendados) van al final (despriorizar, NO eliminar)
            df_coinciden = df_combinado[df_combinado["_merge"] == "both"][["id_cliente", "cod_articulo_magic"]].drop_duplicates()
            pan_rec = pd.concat([df_unicos, df_coinciden], ignore_index=True)
    else:
        print("No hay historial de recomendaciones.")

    return pan_rec


def paso_5_9_a_5_12_ensamblar_y_exportar(pan_rec, df_ventas):
    """5.9-5.12: Combinar con categorías, pesos, segmento, y exportar."""
    print("5.9-5.12 Ensamblando recomendaciones finales...")

    maestro_prod = pd.read_csv(os.path.join(INPUT_DIR_LIMPIEZA, "D_pan_masterProd.csv"))
    with open(os.path.join(INPUT_DIR_LIMPIEZA, "mapeo_diccionario.json"), "r") as f:
        mapeo_diccionario = json.load(f)

    # 5.9 Combinar recomendaciones con categorías
    marca_articulo = df_ventas[["desc_categoria", "cod_articulo_magic"]].drop_duplicates()
    pan_rec = pd.merge(pan_rec, marca_articulo, on="cod_articulo_magic", how="left")
    pan_rec["desc_categoria"] = pan_rec["desc_categoria"].str.strip()

    # Categorías distintas por cliente
    cods2 = pan_rec.groupby("id_cliente")["desc_categoria"].nunique().reset_index()

    # 5.10 Juntar recomendaciones finales
    final_rec = pan_rec.groupby(["id_cliente", "desc_categoria"]).first().reset_index()
    final_rec = pd.merge(final_rec, cods2, how="left", on="id_cliente")
    pan_prod = maestro_prod[["cod_articulo_magic", "desc_articulo"]].drop_duplicates().groupby("cod_articulo_magic").first().reset_index()
    final_rec = pd.merge(final_rec, pan_prod, how="left", on="cod_articulo_magic")
    final_rec.columns = ["id_cliente", "marca_rec", "sku", "len_marca_rec", "desc_rec"]

    giros = df_ventas[["id_cliente", "desc_giro", "desc_subgiro"]].drop_duplicates()
    final_rec = pd.merge(final_rec, giros, on="id_cliente", how="left")

    info_segmentos = df_ventas[["id_cliente", "new_segment", "dias_de_visita__c", "periodo_de_visita__c", "ultima_visita"]].drop_duplicates().groupby("id_cliente").first().reset_index()
    final_rec = pd.merge(final_rec, info_segmentos, how="left", on="id_cliente")

    datos_sf = df_ventas.groupby(["id_cliente", "cod_compania", "cod_sucursal", "cod_cliente", "cod_ruta"])[
        ["id_cliente", "cod_compania", "cod_sucursal", "cod_cliente", "cod_modulo", "cod_ruta"]
    ].head(1).reset_index(drop=True)
    final_rec = pd.merge(final_rec, datos_sf, how="left", on="id_cliente")

    # Pesos por giro
    final_rec["peso"] = final_rec.apply(lambda row: mapeo_diccionario.get(row["desc_subgiro"], {}).get(row["marca_rec"], 5), axis=1)
    final_rec = final_rec.sort_values(["id_cliente", "peso"]).groupby("id_cliente").head(5)
    final_rec["marca_rec_rank"] = final_rec.groupby("id_cliente").cumcount() + 1
    final_rec = final_rec.reset_index(drop=True)

    # 5.11 Filtro por segmento
    limites_segmento = {"BLINDAR": 1, "MANTENER": 2, "DESARROLLAR": 3, "OPTIMIZAR": 4}
    final_rec = final_rec.groupby("id_cliente").apply(
        lambda g: g.head(limites_segmento.get(g["new_segment"].iloc[0], 5))
    ).reset_index(drop=True)

    # 5.12 Exportar
    print(f"Exportando resultados para {fecha_tomorrow}...")
    final_rec["cod_cliente"] = final_rec["id_cliente"].str.split("|").str[-1]

    # D&A
    s3_path_da = f"s3://{S3_BUCKET_BACKUP}/{S3_PREFIX_OUTPUT_DATA}D_pan_recs_data_{fecha_tomorrow}.csv"
    wr.s3.to_csv(final_rec, s3_path_da, index=False, boto3_session=my_session)

    # Salesforce
    final_rec["cod_compania"] = final_rec["cod_compania"].astype(int)
    final_rec["cod_sucursal"] = final_rec["cod_sucursal"].astype(int)
    final_rec["cod_cliente"] = final_rec["cod_cliente"].astype(int)
    final_rec["cod_modulo"] = final_rec["cod_modulo"].astype(int)
    final_rec["sku"] = final_rec["sku"].astype(int)

    rec_sf = final_rec[["cod_compania", "cod_sucursal", "cod_cliente", "cod_modulo", "sku"]].drop_duplicates()
    rec_sf["Pais"] = "NI"
    rec_sf["Cajas"] = int(1)
    rec_sf["Unidades"] = int(0)
    rec_sf["Fecha"] = fecha_tomorrow
    rec_sf = rec_sf[["Pais", "cod_compania", "cod_sucursal", "cod_cliente", "cod_modulo", "sku", "Cajas", "Unidades", "Fecha"]]
    rec_sf.columns = ["Pais", "Compania", "Sucursal", "Cliente", "Modulo", "Producto", "Cajas", "Unidades", "Fecha"]
    rec_sf["Compania"] = rec_sf["Compania"].apply(lambda x: str(int(x)).rjust(4, "0"))
    rec_sf["Sucursal"] = rec_sf["Sucursal"].apply(lambda x: str(int(x)).rjust(2, "0"))

    rec_sf["tipoRecomendacion"] = rec_sf.groupby(["Pais", "Compania", "Sucursal", "Cliente"]).cumcount().apply(lambda x: f"PS{x+1}")
    rec_sf["ultFecha"] = ''
    rec_sf["Destacar"] = "true"

    s3_path_sf = f"s3://{S3_BUCKET_BACKUP}/{S3_PREFIX_OUTPUT}D_base_pedidos_{fecha_tomorrow}.csv"
    wr.s3.to_csv(rec_sf, s3_path_sf, index=False, boto3_session=my_session)

    print("Total de clientes a recomendar:", rec_sf.Cliente.nunique())
    print("SKUs usados:", rec_sf.Producto.nunique())

    print("Archivos subidos exitosamente a S3.")


# Variable global para compartir df_ventas entre funciones
df_ventas_global = None


def main():
    global df_ventas_global
    print("--- INICIANDO REGLAS DE NEGOCIO (Nicaragua) ---")

    ruta_ventas = os.path.join(INPUT_DIR_LIMPIEZA, "ni_ventas_manana.parquet")
    ruta_recs = os.path.join(INPUT_DIR_MODELADO, "D_rutas_rec.parquet")

    df_ventas = pd.read_parquet(ruta_ventas)
    df_ventas_global = df_ventas
    pan_rec = pd.read_parquet(ruta_recs)

    # Orden exacto del notebook de Nicaragua:
    # 5.1 Maestro validación
    pan_rec = paso_5_1_maestro_validacion(pan_rec)

    # 5.2 Quitar compras últimas 2 semanas
    pan_rec_sin_compras, cliente_rec_marca = paso_5_2_quitar_compras_recientes(pan_rec, df_ventas)

    # 5.3 Filtro SKUs por Excel compañía-sucursal
    pan_rec = paso_5_3_filtro_sku_disponible(pan_rec_sin_compras, df_ventas)

    # 5.4 S/M/B flag
    pan_rec = paso_5_4_subida_bajada_mantener(pan_rec, df_ventas)

    # 5.5 Priorizar SKUs (comentado en notebook, se omite)

    # 5.6 Quitar SKUs sin precio
    pan_rec = paso_5_6_quitar_skus_sin_precio(pan_rec)

    # 5.7 Despriorizar recomendaciones históricas
    pan_rec = paso_5_7_despriorizar_historico(pan_rec)

    # 5.9-5.12 Ensamblar y exportar
    paso_5_9_a_5_12_ensamblar_y_exportar(pan_rec, df_ventas)

    print("--- PROCESO FINALIZADO ---")


if __name__ == "__main__":
    main()
