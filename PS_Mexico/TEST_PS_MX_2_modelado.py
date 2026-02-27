import os
import glob
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import when, col, concat, countDistinct, lit, hash

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.recommendation import ALS

# FIJAR REGIÓN
os.environ["AWS_DEFAULT_REGION"] = "us-east-2"

# --- CONFIGURACIÓN DE RUTAS SAGEMAKER ---
# Input: Donde SageMaker mapeará la salida del Script 1 (la carpeta 'rutas')
INPUT_DIR = "/opt/ml/processing/input/rutas"
# Output: Donde guardaremos las recomendaciones finales de este paso
OUTPUT_DIR = "/opt/ml/processing/output/modelado"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def create_spark_session():
    """Inicializa una única sesión de Spark optimizada para el job."""
    spark = (
        SparkSession.builder
        .appName("SageMaker-ALS-Recommender")
        # .config("spark.memory.offHeap.enabled", "true")
        # .config("spark.memory.offHeap.size", "10G")
        .config("spark.hadoop.hadoop.security.authentication", "simple")
        .config("spark.hadoop.hadoop.security.authorization", "false")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    return spark


def als_training_job(spark, ruta_csv_path):
    """Ejecuta KMeans para ratings implícitos y luego entrena ALS para una ruta específica."""
    
    # 1. Cargar datos y calcular sku_len dinámicamente
    ventas = spark.read.format("csv").options(header=True, delimiter=",").load(ruta_csv_path)
    
    # Si la ruta está vacía o es inválida, salimos
    if ventas.count() == 0:
        return pd.DataFrame()

    sku_len = ventas.select("cod_articulo_magic").distinct().count()
    
    ventas = ventas.na.drop(subset=["fecha_liquidacion"])
    ventas = ventas.groupBy(
        ["id_cliente", "cod_articulo_magic", "cod_compania", "cod_cliente"]
    ).agg(countDistinct("fecha_liquidacion"))
    
    ventas = ventas.select(
        col("id_cliente").alias("id_cliente"),
        col("count(fecha_liquidacion)").alias("frecuencia"),
        col("cod_articulo_magic").alias("cod_articulo_magic"),
        col("cod_compania").alias("cod_compania"),
        col("cod_cliente").alias("cod_cliente"),
    )

    # 2. KMeans para generar Ratings
    input_cols = ["frecuencia"]
    vec_assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    ventas_kmeans = vec_assembler.transform(ventas).fillna(0.0, subset=["frecuencia"])

    kmeans = KMeans(featuresCol="features", k=5)
    model_kmeans = kmeans.fit(ventas_kmeans)

    resumen_ventas = model_kmeans.transform(ventas_kmeans).groupby("prediction").max()
    resumen_ventas.orderBy(["max(frecuencia)"], ascending=0)
    resumen_ventas = resumen_ventas.select(
        col("prediction").alias("prediction"), col("max(frecuencia)").alias("max")
    )

    maxes = resumen_ventas.select("max").toPandas()["max"].astype(float).tolist()
    labels = [5, 4, 3, 2, 1]
    rank_map = dict(zip(labels, maxes))

    ventas = ventas.withColumn("frecuencia", col("frecuencia").cast(DoubleType()))
    ventas = ventas.withColumn(
        "rating",
        when(ventas["frecuencia"] < rank_map.get(labels[-1]), labels[-1])
        .when(ventas["frecuencia"] < rank_map.get(labels[-2]), labels[-2])
        .when(ventas["frecuencia"] < rank_map.get(labels[-3]), labels[-3])
        .when(ventas["frecuencia"] < rank_map.get(labels[-4]), labels[-4])
        .otherwise(labels[-5]),
    )

    # 3. Preparar datos para ALS
    als_records = ventas.select("id_cliente", "cod_compania", "cod_cliente", "cod_articulo_magic", "rating")
    als_records = als_records.withColumn("clienteId", concat(col("cod_compania"), lit("|"), col("cod_cliente")))
    als_records = als_records.withColumn("clienteId_numeric", hash(col("clienteId")))
    als_records = als_records.withColumn("cod_articulo_magic", col("cod_articulo_magic").cast(IntegerType()))

    als_records = als_records.na.drop(subset=["clienteId_numeric", "cod_articulo_magic"])
    als_records = als_records.select("clienteId", "clienteId_numeric", "cod_articulo_magic", "rating")
    als_records = als_records.dropDuplicates(["clienteId", "cod_articulo_magic"])

    # 4. Entrenar ALS
    als = ALS(
        rank=10,
        maxIter=5,
        implicitPrefs=True,
        ratingCol="rating",
        itemCol="cod_articulo_magic",
        userCol="clienteId_numeric",
    )
    model_als = als.fit(als_records)

    # 5. Generar recomendaciones
    recs = model_als.recommendForAllUsers(sku_len)
    recs = recs.select("clienteId_numeric", "recommendations.cod_articulo_magic")
    
    # Recuperar clienteId original
    recs = recs.join(
        als_records.select("clienteId", "clienteId_numeric").dropDuplicates(),
        on="clienteId_numeric",
        how="left",
    )

    # 6. Transformación a Pandas (Lógica original respetada)
    recs_to_parse = recs.select("clienteId", "cod_articulo_magic").toPandas()
    if recs_to_parse.empty:
        return pd.DataFrame()

    lista_rec = [f"r{i+1}" for i in range(sku_len)]
    new_cols = pd.DataFrame(
        recs_to_parse["cod_articulo_magic"].tolist(),
        index=recs_to_parse.index,
        columns=lista_rec,
    )
    recs_to_parse = pd.concat([recs_to_parse, new_cols], axis=1)
    client_recs = pd.melt(recs_to_parse, id_vars=["clienteId"], value_vars=lista_rec)

    client_recs["compania"] = client_recs["clienteId"].str.split("|").str[-2].apply(lambda x: str(x).rjust(4, "0"))
    client_recs["cliente"] = client_recs["clienteId"].str.split("|").str[1]
    client_recs["id_cliente"] = "MX|" + client_recs["compania"] + "|" + client_recs["cliente"]
    client_recs["cod_articulo_magic"] = client_recs["value"]

    client_recs = client_recs[["id_cliente", "cod_articulo_magic"]].drop_duplicates().reset_index(drop=True)
    
    return client_recs


def main():
    print("Iniciando Modelado ALS...")
    spark = create_spark_session()
    
    # Obtener dinámicamente todos los archivos de rutas generados en el paso de limpieza
    archivos_rutas = glob.glob(os.path.join(INPUT_DIR, "**", "D_*_ventas.csv"))
    
    if not archivos_rutas:
        print("ALERTA: No se encontraron archivos de rutas para procesar.")
        return

    lista_recomendaciones = []

    # Iterar sobre las rutas
    for ruta_path in archivos_rutas:
        nombre_archivo = os.path.basename(ruta_path)
        print(f"Procesando ALS para: {nombre_archivo}...")
        
        df_rec_ruta = als_training_job(spark, ruta_path)
        
        if not df_rec_ruta.empty:
            lista_recomendaciones.append(df_rec_ruta)

    # Consolidar y guardar
    if lista_recomendaciones:
        print("Consolidando todas las recomendaciones...")
        df_final_recs = pd.concat(lista_recomendaciones, ignore_index=True)
        df_final_recs = df_final_recs.drop_duplicates()
        
        ruta_salida = os.path.join(OUTPUT_DIR, "D_rutas_rec.parquet")
        df_final_recs.to_parquet(ruta_salida, index=False)
        print(f"Recomendaciones guardadas exitosamente en {ruta_salida}")
    else:
        print("No se generaron recomendaciones.")

    spark.stop()


if __name__ == "__main__":
    main()