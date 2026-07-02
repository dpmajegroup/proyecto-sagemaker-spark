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
import pandas as pd
import numpy as np
import awswrangler as wr
from datetime import datetime, timedelta

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
my_session = boto3.Session(region_name="us-east-2")

INPUT_DIR_LIMPIEZA = "/opt/ml/processing/input/limpieza"
INPUT_DIR_MODELADO = "/opt/ml/processing/input/modelado"

S3_BUCKET_BACKUP = "aje-analytics-ps-backup"
S3_PREFIX_OUTPUT = "PS_Peru/Output/PS_piloto_v1/"
S3_PREFIX_OUTPUT_DATA = "PS_Peru/Output/PS_data_piloto_v1/"

# SKUs a excluir (lista larga específica de Perú)
SKUS_SIN_PRECIO = list(set([
    622908,622102,699373,622103,622672,699790,622841,623140,699423,622735,622192,622189,623130,699127,622396,608462,623557,
    622838,621939,623566,622313,621994,622163,622391,622392,622393,622395,622630,699367,699892,622356,623854,699361,699366,
    699550,619038,623872,621766,621804,622163,622356,622387,622390,622391,622392,622393,622394,622395,622396,622439,622783,
    622841,623130,623150,623456,623753,698987,699346,699363,699367,699373,699425,699493,699494,623873,623875,624052,699868,
    699494,623873,699927,622439,623875,623753,622313,699495,622387,622393,623087,622838,623557,623565,624239,624245,624285,622079,
    622780,699048,598998,518027,599048
]))

# Filtro especial: excluir SKU 608597 de rutas específicas
RUTAS_EXCLUIR_608597 = [14608, 14450, 14471, 12967, 12968, 12958, 12972]

tz_lima = pytz.timezone("America/Lima")
fecha_actual = datetime.now(tz_lima)
fecha_tomorrow = (fecha_actual + timedelta(days=1)).strftime("%Y-%m-%d")


def log_filtro(nombre, df, col_cliente="id_cliente"):
    """Imprime métricas de trazabilidad después de cada filtro."""
    n_clientes = df[col_cliente].nunique()
    n_filas = df.shape[0]
    prom = round(n_filas / n_clientes, 1) if n_clientes > 0 else 0
    print(f"  [{nombre}] Clientes: {n_clientes:,} | Recs: {n_filas:,} | Prom/cliente: {prom}")


def clasificar_valor(x):
    if x > 0: return "S"
    elif x == 0: return "M"
    else: return "B"


def aplicar_filtros_disponibilidad(pan_rec, df_ventas):
    """Reglas 5.-9, 5.-8, 5.-7, 5.-5, 5.-3 + filtros específicos de Perú."""
    print("Aplicando filtros de disponibilidad y stock...")
    log_filtro("INICIO modelo", pan_rec)

    # --- 5.-9 SKUs con ventas en los últimos 14 días ---
    fecha_limite = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    ventas_filtradas = df_ventas[df_ventas["fecha_liquidacion"] >= fecha_limite]
    productos_por_ruta = ventas_filtradas.groupby("cod_ruta")["cod_articulo_magic"].unique().reset_index()

    pan_rec = pan_rec.merge(df_ventas[["id_cliente", "cod_ruta"]].drop_duplicates(), on="id_cliente", how="left")
    rec_validas = pan_rec.merge(productos_por_ruta, on="cod_ruta", how="inner")
    rec_validas = rec_validas[rec_validas.apply(lambda row: row["cod_articulo_magic_x"] in row["cod_articulo_magic_y"], axis=1)]
    pan_rec = rec_validas[["id_cliente", "cod_articulo_magic_x", "cod_ruta"]].rename(columns={"cod_articulo_magic_x": "cod_articulo_magic"}).reset_index(drop=True)
    log_filtro("Ventas 14d (5.-9)", pan_rec)

    # --- 5.-8 Subida, Bajada, Mantener ---
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
    log_filtro("S/M/B (5.-8)", pan_rec)

    # --- 5.-7 Maestro de validación (DESACTIVADO) ---
    # Limita el ALS a solo SKUs que el cliente ya compró históricamente.
    # Redundante con filtro 5.-3 (SKUs sin precio) y restringe cross-selling.
    # s3_path_val = "s3://aje-prd-analytics-artifacts-s3/pedido_sugerido/data-v1/peru/maestro_productos_peru000"
    # skus_val = wr.s3.read_csv(s3_path_val, sep=";", boto3_session=my_session)
    # skus_val["cod_compania"] = skus_val["cod_compania"].astype(str).str.zfill(4)
    # skus_val["cod_cliente"] = pd.to_numeric(skus_val["cod_cliente"], errors="coerce").fillna(0).astype(int)
    # skus_val["id_cliente"] = "PE|" + skus_val["cod_compania"] + "|" + skus_val["cod_cliente"].astype(str)
    # pan_rec = pd.merge(pan_rec, skus_val[['cod_articulo_magic', 'id_cliente']].drop_duplicates(), on=["id_cliente", "cod_articulo_magic"], how="inner")
    # log_filtro("Maestro (5.-7)", pan_rec)

    # --- Filtro especial: excluir SKU 608597 de rutas específicas ---
    pan_rec = pan_rec[~((pan_rec.cod_ruta.isin(RUTAS_EXCLUIR_608597)) & (pan_rec.cod_articulo_magic == 608597))]
    print(f"  Después filtro 608597: {pan_rec.id_cliente.nunique()} clientes, {pan_rec.shape[0]} filas")

    # --- 5.-5 Filtro STOCK (D_stock_pe.csv) ---
    stock = wr.s3.read_csv("s3://aje-prd-analytics-artifacts-s3/pedido_sugerido/data-v1/peru/D_stock_pe.csv", boto3_session=my_session)
    stock = stock.drop(columns=["Fecha", "Database"])
    stock.columns = ["cod_compania", "cod_sucursal", "cod_articulo_magic", "stock_cf"]
    stock["cod_compania"] = stock["cod_compania"].astype(str).str.zfill(4)
    stock["cod_sucursal"] = stock["cod_sucursal"].astype(str).str.zfill(2)

    fecha_12_dias = (fecha_actual - timedelta(days=12)).strftime('%Y-%m-%d')
    prom_diario_vta = df_ventas[(df_ventas.cant_cajafisicavta > 0) & (df_ventas.fecha_liquidacion >= fecha_12_dias)]
    prom_diario_vta = prom_diario_vta.groupby(["cod_compania", "cod_sucursal", "cod_articulo_magic", "fecha_liquidacion"]).cant_cajafisicavta.sum().reset_index()
    prom_diario_vta = prom_diario_vta.groupby(["cod_compania", "cod_sucursal", "cod_articulo_magic"]).cant_cajafisicavta.mean().reset_index()
    prom_diario_vta["cod_compania"] = prom_diario_vta["cod_compania"].astype(str).str.zfill(4)
    prom_diario_vta["cod_sucursal"] = prom_diario_vta["cod_sucursal"].astype(str).str.zfill(2)

    df_stock = pd.merge(prom_diario_vta, stock, on=["cod_compania", "cod_sucursal", "cod_articulo_magic"], how="left")
    df_stock["dias_stock"] = df_stock["stock_cf"] / df_stock["cant_cajafisicavta"]
    df_stock = df_stock[(df_stock.dias_stock > 3) & (df_stock.cant_cajafisicavta > 0)]

    pan_rec = pan_rec.merge(df_ventas[["id_cliente", "cod_compania", "cod_sucursal"]].drop_duplicates(), on="id_cliente", how="left")
    pan_rec["cod_compania"] = pan_rec["cod_compania"].astype(str).str.zfill(4)
    pan_rec["cod_sucursal"] = pan_rec["cod_sucursal"].astype(str).str.zfill(2)
    pan_rec = pd.merge(pan_rec, df_stock, on=["cod_compania", "cod_sucursal", "cod_articulo_magic"], how="inner")
    log_filtro("Stock (5.-5)", pan_rec)

    # --- 5.-4 Quitar SKUs del Excel LISTA SKUS - NN.xlsx (COMENTADO - no se ejecutaba en notebook) ---
    # sagemaker_sess = boto3.Session(region_name="us-east-2")
    # sts = sagemaker_sess.client("sts")
    # account_id = sts.get_caller_identity()["Account"]
    # default_bucket = f"sagemaker-us-east-2-{account_id}"
    # excel_s3_path = f"s3://{default_bucket}/scripts/PE/LISTA SKUS - NN.xlsx"
    # excel_local = "/opt/ml/processing/LISTA_SKUS_NN.xlsx"
    # wr.s3.download(path=excel_s3_path, local_file=excel_local, boto3_session=my_session)
    # sku_con_precio = pd.read_excel(excel_local, sheet_name="Hoja1")
    # pan_rec = pan_rec[~pan_rec["cod_articulo_magic"].isin(sku_con_precio["CÓDIGO"].unique())].reset_index(drop=True)

    # --- 5.-3 Quitar SKUs sin precio ---
    pan_rec = pan_rec[~pan_rec["cod_articulo_magic"].isin(SKUS_SIN_PRECIO)].reset_index(drop=True)
    log_filtro("SKUs sin precio (5.-3)", pan_rec)

    return pan_rec[["id_cliente", "cod_articulo_magic"]].drop_duplicates().reset_index(drop=True)


def aplicar_filtros_historia(pan_rec, df_ventas):
    """Reglas 5.-2 y 5.3"""
    print("Aplicando filtros históricos...")
    log_filtro("INICIO historia", pan_rec)

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
    last_14_recs = pd.DataFrame()
    for fecha in last_14_days:
        s3_uri = f"s3://{S3_BUCKET_BACKUP}/{S3_PREFIX_OUTPUT}D_base_pedidos_{fecha}.csv"
        try:
            df_temp = wr.s3.read_csv(s3_uri, dtype={"Compania": str, "Cliente": str}, boto3_session=my_session)
            df_temp["id_cliente"] = 'PE|' + df_temp['Compania'] + '|' + df_temp['Cliente']
            df_temp = df_temp[df_temp["id_cliente"].isin(pan_rec["id_cliente"].unique())]
            last_14_recs = pd.concat([last_14_recs, df_temp], axis=0)
        except Exception as e:
            print(f"No se pudo leer {s3_uri}: {e}")

    if not last_14_recs.empty:
        df_combinado = pd.merge(pan_rec, last_14_recs, left_on=['id_cliente', 'cod_articulo_magic'], right_on=['id_cliente', 'Producto'], how='left', indicator=True)
        df_unicos = df_combinado[df_combinado['_merge'] == 'left_only'][["id_cliente", "cod_articulo_magic"]].drop_duplicates()
        df_coinciden = df_combinado[df_combinado['_merge'] == 'both'][["id_cliente", "cod_articulo_magic"]].drop_duplicates()
        pan_rec = pd.concat([df_unicos, df_coinciden], ignore_index=True)
    log_filtro("Despriorizar hist. (5.-2)", pan_rec)

    last_2_weeks = (datetime.now() - timedelta(days=11)).strftime('%Y-%m-%d')
    df_ventas["fecha_liquidacion"] = pd.to_datetime(df_ventas["fecha_liquidacion"]).dt.strftime('%Y-%m-%d')
    compras_recientes = df_ventas[df_ventas["fecha_liquidacion"] >= last_2_weeks][["id_cliente", "cod_articulo_magic"]].drop_duplicates()
    pan_rec = pan_rec.merge(compras_recientes, on=['id_cliente', 'cod_articulo_magic'], how='left', indicator=True)
    pan_rec = pan_rec[pan_rec['_merge'] == 'left_only'].drop(columns=['_merge'])
    log_filtro("Compras 11d (5.3)", pan_rec)
    return pan_rec.reset_index(drop=True)


def calcular_metricas_y_ensamblar(pan_rec, df_ventas):
    """Calcula irregularidad, métricas y aplica reglas finales."""
    print("Calculando métricas y armando dataset final...")

    maestro_prod = pd.read_csv(os.path.join(INPUT_DIR_LIMPIEZA, "PE_maestro_productos.csv"))
    with open(os.path.join(INPUT_DIR_LIMPIEZA, "mapeo_diccionario.json"), "r") as f:
        mapeo_diccionario = json.load(f)

    marca_articulo = df_ventas[["desc_categoria", "cod_articulo_magic"]].drop_duplicates()
    cliente_rec_marca = pd.merge(pan_rec, marca_articulo, on="cod_articulo_magic", how="left")
    cliente_rec_marca["desc_categoria"] = cliente_rec_marca["desc_categoria"].str.strip()
    cods2 = cliente_rec_marca.groupby("id_cliente")["desc_categoria"].nunique().reset_index()

    # Irregularidad
    now_mes = pd.to_datetime(fecha_actual.strftime("%Y-%m-01"))
    lista_m12 = [now_mes - pd.DateOffset(months=12) + pd.DateOffset(months=i) for i in range(12)]
    lista_m6 = [now_mes - pd.DateOffset(months=6) + pd.DateOffset(months=i) for i in range(6)]
    temp_v = df_ventas[df_ventas["id_cliente"].isin(pan_rec["id_cliente"].unique())]
    qw = temp_v[["id_cliente", "mes"]].drop_duplicates().sort_values(["id_cliente", "mes"]).groupby("id_cliente").tail(12).reset_index(drop=True)
    qw["mes"] = pd.to_datetime(qw["mes"])
    qw["m12"] = qw["mes"].isin(lista_m12)
    qw["m6"] = qw["mes"].isin(lista_m6)
    cat_cliente = qw.groupby("id_cliente")[["m12", "m6"]].sum().reset_index()
    cat_cliente["categoria_cliente_2"] = np.where(cat_cliente["m6"] < 4, "Irregular", "Regular")

    # Antiguedad
    pan_ventas = df_ventas.rename(columns={"fecha_creacion_cliente": "fecha_creacion"}).copy()
    pan_ventas["fecha_creacion"] = pd.to_numeric(pan_ventas["fecha_creacion"], errors="coerce").fillna(0).astype(int)
    pan_ventas["fecha_creacion"] = pd.to_datetime(pan_ventas["fecha_creacion"], format="%Y%m%d", errors="coerce")
    hace_12_meses = datetime.now() - timedelta(days=365)
    pan_ventas["antiguedad"] = pan_ventas["fecha_creacion"].apply(lambda x: "nf" if pd.isnull(x) else ("new_client" if x >= hace_12_meses else "old_client"))
    pan_antiguedad = pan_ventas[["id_cliente", "fecha_creacion", "antiguedad"]].drop_duplicates().reset_index(drop=True)

    # Juntar todo
    final_rec = cliente_rec_marca.groupby(["id_cliente", "desc_categoria"]).first().reset_index()
    final_rec = pd.merge(final_rec, cods2, how="left", on="id_cliente")
    maestro_prod = maestro_prod[["cod_articulo_magic", "desc_articulo"]].drop_duplicates().groupby("cod_articulo_magic").first().reset_index()
    final_rec = pd.merge(final_rec, maestro_prod, how="left", on="cod_articulo_magic")
    final_rec.columns = ["id_cliente", "marca_rec", "sku", "len_marca_rec", "desc_rec"]

    giros = df_ventas[["id_cliente", "desc_giro", "desc_subgiro"]].drop_duplicates()
    final_rec = pd.merge(final_rec, giros, on="id_cliente", how="left")
    final_rec = pd.merge(final_rec, cat_cliente[["id_cliente", "categoria_cliente_2"]], on="id_cliente", how="left")
    final_rec.loc[final_rec["categoria_cliente_2"] == "Irregular", "desc_giro"] = np.nan
    final_rec = pd.merge(final_rec, pan_antiguedad, how="left", on="id_cliente")

    info_segmentos = df_ventas[["id_cliente", "new_segment", "dias_de_visita__c", "periodo_de_visita__c", "ultima_visita"]].drop_duplicates().groupby("id_cliente").first().reset_index()
    final_rec = pd.merge(final_rec, info_segmentos, how="left", on="id_cliente")

    datos_sf = df_ventas.groupby(["id_cliente", "cod_compania", "cod_sucursal", "cod_cliente", "cod_ruta"])[["id_cliente", "cod_compania", "cod_sucursal", "cod_cliente", "cod_modulo", "cod_ruta"]].head(1).reset_index(drop=True)
    final_rec = pd.merge(final_rec, datos_sf, how="left", on="id_cliente")

    final_rec["peso"] = final_rec.apply(lambda row: mapeo_diccionario.get(row["desc_subgiro"], {}).get(row["marca_rec"], 5), axis=1)
    final_rec = final_rec.sort_values(["id_cliente", "peso"]).reset_index(drop=True)
    final_rec["marca_rec_rank"] = final_rec.groupby("id_cliente").cumcount() + 1

    # Filtro por segmento estándar
    limites_segmento = {"BLINDAR": 1, "MANTENER": 2, "DESARROLLAR": 3, "OPTIMIZAR": 4}
    final_rec = final_rec.groupby("id_cliente").apply(
        lambda g: g.head(limites_segmento.get(g["new_segment"].iloc[0], 5))
    ).reset_index(drop=True)
    log_filtro("Segmento", final_rec)

    return final_rec


def exportar_resultados(final_rec):
    """Exporta resultados a S3."""
    print(f"Exportando resultados a S3 para la fecha {fecha_tomorrow}...")

    s3_path_da = f"s3://{S3_BUCKET_BACKUP}/{S3_PREFIX_OUTPUT_DATA}D_pan_recs_data_{fecha_tomorrow}.csv"
    wr.s3.to_csv(final_rec, s3_path_da, index=False, boto3_session=my_session)

    rec_sf = final_rec[["cod_compania", "cod_sucursal", "cod_cliente", "cod_modulo", "sku"]].copy()
    rec_sf["Pais"] = "PE"
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


def main():
    print("--- INICIANDO REGLAS DE NEGOCIO (Perú) ---")

    ruta_ventas = os.path.join(INPUT_DIR_LIMPIEZA, "peru_ventas_manana.parquet")
    ruta_recs = os.path.join(INPUT_DIR_MODELADO, "D_rutas_rec.parquet")

    df_ventas = pd.read_parquet(ruta_ventas)
    pan_rec = pd.read_parquet(ruta_recs)

    pan_rec_disp = aplicar_filtros_disponibilidad(pan_rec, df_ventas)
    pan_rec_hist = aplicar_filtros_historia(pan_rec_disp, df_ventas)
    final_rec = calcular_metricas_y_ensamblar(pan_rec_hist, df_ventas)
    exportar_resultados(final_rec)

    print("--- PROCESO FINALIZADO ---")

if __name__ == "__main__":
    main()
