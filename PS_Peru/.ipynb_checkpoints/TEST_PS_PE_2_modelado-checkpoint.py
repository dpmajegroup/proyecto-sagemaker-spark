import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "pyarrow==14.0.1", "pandas"])

import os
import glob
import traceback
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, concat, countDistinct, lit
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"

INPUT_DIR = "/opt/ml/processing/input/rutas"
OUTPUT_DIR = "/opt/ml/processing/output/modelado"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("SageMaker-ALS-Recommender-PE")
        .config("spark.hadoop.hadoop.security.authentication", "simple")
        .config("spark.hadoop.hadoop.security.authorization", "false")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    return spark


def als_training_job(spark, ruta_csv_path):
    """Entrena ALS implícito sin KMeans para una ruta."""
    ventas = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"file://{ruta_csv_path}")
    )
    if ventas.count() == 0:
        return pd.DataFrame()

    ventas = ventas.withColumn("cod_articulo_magic", col("cod_articulo_magic").cast(IntegerType()))
    sku_len = ventas.select("cod_articulo_magic").distinct().count()
    ventas = ventas.na.drop(subset=["fecha_liquidacion"])

    ventas = ventas.groupBy(
        ["id_cliente", "cod_articulo_magic", "cod_compania", "cod_cliente"]
    ).agg(countDistinct("fecha_liquidacion").alias("frecuencia"))
    ventas = ventas.withColumn("clienteId", concat(col("cod_compania"), lit("|"), col("cod_cliente")))

    indexer = StringIndexer(inputCol="clienteId", outputCol="clienteId_numeric", handleInvalid="skip")
    ventas = indexer.fit(ventas).transform(ventas)

    als_records = ventas.withColumn("rating", col("frecuencia").cast("float"))
    als_records = als_records.withColumn("clienteId_numeric", col("clienteId_numeric").cast("integer"))
    als_records = als_records.select("clienteId", "clienteId_numeric", "cod_articulo_magic", "rating")
    als_records = als_records.dropDuplicates(["clienteId", "cod_articulo_magic"])
    als_records = als_records.dropna(subset=["clienteId_numeric", "cod_articulo_magic", "rating"])

    if als_records.count() < 10:
        print(f"Pocos registros en esta ruta, omitiendo ALS.")
        return pd.DataFrame()

    # ALS rank=10 para Perú
    als = ALS(
        rank=10,
        maxIter=5,
        implicitPrefs=True,
        ratingCol="rating",
        itemCol="cod_articulo_magic",
        userCol="clienteId_numeric",
        coldStartStrategy="drop"
    )
    model_als = als.fit(als_records)

    recs = model_als.recommendForAllUsers(sku_len)
    recs = recs.select("clienteId_numeric", "recommendations.cod_articulo_magic")
    recs = recs.join(
        als_records.select("clienteId", "clienteId_numeric").dropDuplicates(),
        on="clienteId_numeric", how="left",
    )

    recs_to_parse = recs.select("clienteId", "cod_articulo_magic").toPandas()
    if recs_to_parse.empty:
        return pd.DataFrame()

    lista_rec = [f"r{i+1}" for i in range(sku_len)]
    new_cols = pd.DataFrame(
        recs_to_parse["cod_articulo_magic"].tolist(),
        index=recs_to_parse.index, columns=lista_rec,
    )
    recs_to_parse = pd.concat([recs_to_parse, new_cols], axis=1)
    client_recs = pd.melt(recs_to_parse, id_vars=["clienteId"], value_vars=lista_rec)

    client_recs["compania"] = client_recs["clienteId"].str.split("|").str[-2].apply(lambda x: str(x).rjust(4, "0"))
    client_recs["cliente"] = client_recs["clienteId"].str.split("|").str[1]
    client_recs["id_cliente"] = "PE|" + client_recs["compania"] + "|" + client_recs["cliente"]
    client_recs["cod_articulo_magic"] = client_recs["value"]
    client_recs = client_recs[["id_cliente", "cod_articulo_magic"]].drop_duplicates().reset_index(drop=True)
    return client_recs


def main():
    try:
        print("Iniciando Modelado ALS (Perú - Modo Implícito sin KMeans, rank=10)...")
        spark = create_spark_session()

        path_busqueda = os.path.join(INPUT_DIR, "**", "D_*_ventas.csv")
        archivos_rutas = glob.glob(path_busqueda, recursive=True)
        if not archivos_rutas:
            archivos_rutas = glob.glob(os.path.join(INPUT_DIR, "D_*_ventas.csv"))
        print(f"Ruta de búsqueda: {path_busqueda}")
        print(f"Archivos encontrados: {len(archivos_rutas)}")

        if not archivos_rutas:
            print("No se encontraron archivos. Generando parquet vacío.")
            pd.DataFrame(columns=["id_cliente", "cod_articulo_magic"]).to_parquet(os.path.join(OUTPUT_DIR, "D_rutas_rec.parquet"), index=False)
            spark.stop()
            return

        lista_recomendaciones = []
        for ruta_path in archivos_rutas:
            nombre_archivo = os.path.basename(ruta_path)
            print(f"Procesando ALS para: {nombre_archivo}...")
            df_rec_ruta = als_training_job(spark, ruta_path)
            if not df_rec_ruta.empty:
                lista_recomendaciones.append(df_rec_ruta)

        if lista_recomendaciones:
            print("Consolidando todas las recomendaciones...")
            df_final_recs = pd.concat(lista_recomendaciones, ignore_index=True).drop_duplicates()
            ruta_salida = os.path.join(OUTPUT_DIR, "D_rutas_rec.parquet")
            df_final_recs.to_parquet(ruta_salida, index=False)
            print(f"Recomendaciones guardadas en {ruta_salida}")
        else:
            print("No se generaron recomendaciones.")
        spark.stop()
    except Exception as e:
        print("!!! ERROR DETECTADO !!!")
        print(traceback.format_exc())
        raise e

if __name__ == "__main__":
    main()
