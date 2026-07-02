import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "pyarrow==14.0.1", "pandas"])

import os
import glob
import traceback
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"

INPUT_DIR = "/opt/ml/processing/input/rutas"
OUTPUT_DIR = "/opt/ml/processing/output/modelado"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("SageMaker-ALS-Recommender-CO")
        .config("spark.hadoop.hadoop.security.authentication", "simple")
        .config("spark.hadoop.hadoop.security.authorization", "false")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    return spark


def als_training_job(spark, ruta_csv_path):
    """Entrena ALS implícito para una ruta. cod_articulo_magic es ALPHANUMERIC."""
    ventas = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")  # Leer todo como string para proteger cod_cliente con "00" prefix
        .load(f"file://{ruta_csv_path}")
    )
    if ventas.count() == 0:
        return pd.DataFrame()

    # Castear columnas numéricas necesarias
    from pyspark.sql.types import FloatType, IntegerType as IntType
    ventas = ventas.withColumn("cant_cajafisica_vta", col("cant_cajafisica_vta").cast(FloatType()))

    # cod_articulo_magic es ALFANUMÉRICO - mantener como string, usar StringIndexer
    ventas = ventas.withColumn("cod_articulo_magic", col("cod_articulo_magic").cast("string"))
    # cod_cliente se mantiene como string para proteger "00" prefix
    ventas = ventas.withColumn("cod_cliente", col("cod_cliente").cast("string"))
    ventas = ventas.withColumn("cod_compania", col("cod_compania").cast("string"))
    sku_len = ventas.select("cod_articulo_magic").distinct().count()
    ventas = ventas.na.drop(subset=["fecha_liquidacion"])

    ventas = ventas.groupBy(
        ["id_cliente", "cod_articulo_magic"]
    ).agg(countDistinct("fecha_liquidacion").alias("frecuencia"))

    # StringIndexer para usuario (id_cliente) - ya viene como "CO|1|00123456"
    indexer_user = StringIndexer(inputCol="id_cliente", outputCol="clienteId_numeric", handleInvalid="skip")
    ventas = indexer_user.fit(ventas).transform(ventas)

    # StringIndexer para item (cod_articulo_magic) - ALPHANUMERIC
    indexer_item = StringIndexer(inputCol="cod_articulo_magic", outputCol="item_numeric", handleInvalid="skip")
    model_indexer_item = indexer_item.fit(ventas)
    ventas = model_indexer_item.transform(ventas)

    als_records = ventas.withColumn("rating", col("frecuencia").cast("float"))
    als_records = als_records.withColumn("clienteId_numeric", col("clienteId_numeric").cast("integer"))
    als_records = als_records.withColumn("item_numeric", col("item_numeric").cast("integer"))
    als_records = als_records.select("id_cliente", "clienteId_numeric", "cod_articulo_magic", "item_numeric", "rating")
    als_records = als_records.dropDuplicates(["id_cliente", "cod_articulo_magic"])
    als_records = als_records.dropna(subset=["clienteId_numeric", "item_numeric", "rating"])

    if als_records.count() < 10:
        print(f"  Pocos registros ({als_records.count()}), omitiendo ALS.")
        return pd.DataFrame()

    n_users = als_records.select("clienteId_numeric").distinct().count()
    n_items = als_records.select("item_numeric").distinct().count()
    print(f"  ALS input: {als_records.count()} registros | {n_users} usuarios | {n_items} items")

    als = ALS(
        rank=10,
        maxIter=5,
        implicitPrefs=True,
        ratingCol="rating",
        itemCol="item_numeric",
        userCol="clienteId_numeric",
        coldStartStrategy="drop"
    )
    model_als = als.fit(als_records)

    recs = model_als.recommendForAllUsers(sku_len)
    recs = recs.select("clienteId_numeric", "recommendations.item_numeric")
    recs = recs.join(
        als_records.select("id_cliente", "clienteId_numeric").dropDuplicates(),
        on="clienteId_numeric", how="left",
    )

    # Obtener mapping item_numeric -> cod_articulo_magic
    item_mapping = als_records.select("item_numeric", "cod_articulo_magic").dropDuplicates().toPandas()
    item_map_dict = dict(zip(item_mapping["item_numeric"], item_mapping["cod_articulo_magic"]))

    recs_to_parse = recs.select("id_cliente", "item_numeric").toPandas()
    if recs_to_parse.empty:
        return pd.DataFrame()

    lista_rec = [f"r{i+1}" for i in range(sku_len)]
    new_cols = pd.DataFrame(
        recs_to_parse["item_numeric"].tolist(),
        index=recs_to_parse.index, columns=lista_rec,
    )
    recs_to_parse = pd.concat([recs_to_parse, new_cols], axis=1)
    client_recs = pd.melt(recs_to_parse, id_vars=["id_cliente"], value_vars=lista_rec)

    # Mapear item_numeric de vuelta a cod_articulo_magic (string)
    client_recs["cod_articulo_magic"] = client_recs["value"].map(item_map_dict)
    client_recs = client_recs[["id_cliente", "cod_articulo_magic"]].drop_duplicates().reset_index(drop=True)
    # Eliminar filas donde cod_articulo_magic sea NaN (por si algún item_numeric no mapeó)
    client_recs = client_recs.dropna(subset=["cod_articulo_magic"]).reset_index(drop=True)
    return client_recs


def main():
    try:
        print("Iniciando Modelado ALS (Colombia - Modo Implícito, rank=10)...")
        spark = create_spark_session()

        path_busqueda = os.path.join(INPUT_DIR, "**", "D_*_ventas.csv")
        archivos_rutas = glob.glob(path_busqueda, recursive=True)
        if not archivos_rutas:
            archivos_rutas = glob.glob(os.path.join(INPUT_DIR, "D_*_ventas.csv"))
        print(f"Ruta de búsqueda: {path_busqueda}")
        print(f"Archivos encontrados: {len(archivos_rutas)}")

        if not archivos_rutas:
            print("No se encontraron archivos de rutas. Generando parquet vacío.")
            pd.DataFrame(columns=["id_cliente", "cod_articulo_magic"]).to_parquet(os.path.join(OUTPUT_DIR, "D_rutas_rec.parquet"), index=False)
            spark.stop()
            return

        lista_recomendaciones = []
        for ruta_path in archivos_rutas:
            nombre_archivo = os.path.basename(ruta_path)
            print(f"Procesando ALS para: {nombre_archivo}...")
            df_rec_ruta = als_training_job(spark, ruta_path)
            if not df_rec_ruta.empty:
                print(f"  -> {df_rec_ruta.id_cliente.nunique()} clientes, {len(df_rec_ruta)} recomendaciones")
                lista_recomendaciones.append(df_rec_ruta)
            else:
                print(f"  -> Sin recomendaciones")

        if lista_recomendaciones:
            print("Consolidando todas las recomendaciones...")
            df_final_recs = pd.concat(lista_recomendaciones, ignore_index=True).drop_duplicates()
            ruta_salida = os.path.join(OUTPUT_DIR, "D_rutas_rec.parquet")
            df_final_recs.to_parquet(ruta_salida, index=False)
            print(f"Recomendaciones guardadas en {ruta_salida}")
        else:
            print("No se generaron recomendaciones. Generando parquet vacío.")
            pd.DataFrame(columns=["id_cliente", "cod_articulo_magic"]).to_parquet(os.path.join(OUTPUT_DIR, "D_rutas_rec.parquet"), index=False)

        spark.stop()
    except Exception as e:
        print("!!! ERROR DETECTADO !!!")
        print(traceback.format_exc())
        raise e


if __name__ == "__main__":
    main()
