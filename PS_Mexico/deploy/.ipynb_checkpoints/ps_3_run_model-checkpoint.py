import subprocess

# subprocess.run(["pip", "install", "awswrangler[redshift]"], check=True)
# subprocess.run(["pip", "install", "psycopg2-binary"], check=True)
# subprocess.run(["pip", "install", "openpyxl"], check=True)
# subprocess.run(["pip", "install", "redshift-connector"], check=True)


# In[3]:


import pandas as pd
import numpy as np
# import redshift_connector
# import awswrangler as wr
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import boto3
import io
import pytz
import re
import gc
import random

import warnings
warnings.filterwarnings("ignore")
# # 4. Modelo Pedido Sugerido

# ## 4.1. Importando librerias necesarias

# In[79]:
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import (
    when,
    col,
    regexp_replace,
    concat,
    countDistinct,
    lit,
    monotonically_increasing_id,
    hash,
)

from pyspark.ml.linalg import Vector
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


# ## 4.2 Construccion del algoritmo ALS

# In[81]:

# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# In[82]:

def als_training_job(cedi, sku_len):

    spark = (
        SparkSession.builder.config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "10G")
        .config("spark.hadoop.hadoop.security.authentication", "simple")  # Desactiva Kerberos
        .config("spark.hadoop.hadoop.security.authorization", "false")
        .appName("K-Means for ALS")
        .getOrCreate()
    )

    ventas = (
        spark.read.format("csv")
        .options(header=True, delimiter=",")
        .load(f"Processed/rutas/D_{cedi}_ventas.csv")
    )

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

    input_cols = ["frecuencia"]
    vec_assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    ventas_kmeans = vec_assembler.transform(ventas)
    ventas_kmeans = ventas_kmeans.fillna(0.0, subset=["frecuencia"])

    kmeans = KMeans(featuresCol="features", k=5)
    model = kmeans.fit(ventas_kmeans)

    resumen_ventas = model.transform(ventas_kmeans).groupby("prediction").max()
    resumen_ventas.orderBy(["max(frecuencia)"], ascending=0)
    resumen_ventas = resumen_ventas.select(
        col("prediction").alias("prediction"), col("max(frecuencia)").alias("max")
    )

    maxes = resumen_ventas.select("max").toPandas()["max"]
    maxes = maxes.astype(float)
    maxes = list(maxes)

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

    cols = ["id_cliente", "cod_compania", "cod_cliente", "cod_articulo_magic", "rating"]
    als_records = ventas.select(*cols)

    # als_records = als_records.withColumn("clienteId",concat("cod_compania","cod_cliente"))
    als_records = als_records.withColumn(
        "clienteId", concat(col("cod_compania"), lit("|"), col("cod_cliente"))
    )
    # als_records = als_records.withColumn("clienteId", col("clienteId").cast(IntegerType()))
    # Crear una columna numérica para usar en el modelo ALS
    # als_records = als_records.withColumn("clienteId_numeric", monotonically_increasing_id())
    als_records = als_records.withColumn("clienteId_numeric", hash(col("clienteId")))
    als_records = als_records.withColumn(
        "cod_articulo_magic", col("cod_articulo_magic").cast(IntegerType())
    )

    als_records = als_records.na.drop(
        subset=["clienteId_numeric", "cod_articulo_magic"]
    )

    input_cols = ["clienteId", "clienteId_numeric", "cod_articulo_magic", "rating"]
    als_records = als_records.select(*input_cols)

    als_records = als_records.dropDuplicates(["clienteId", "cod_articulo_magic"])

    als = ALS(
        rank=10,
        maxIter=5,
        implicitPrefs=True,
        ratingCol="rating",
        itemCol="cod_articulo_magic",
        userCol="clienteId_numeric",
    )
    model = als.fit(als_records)

    # obtener matrices de descomposicion (usuario y articulos)
    user_matrix = model.userFactors
    item_matrix = model.itemFactors

    user_matrix_pd = user_matrix.toPandas()
    item_matrix_pd = item_matrix.toPandas()

    recs = model.recommendForAllUsers(sku_len)
    # recs = recs.select("ClienteId","recommendations.cod_articulo_magic")
    # recs_to_parse = recs.toPandas()

    recs = recs.select("clienteId_numeric", "recommendations.cod_articulo_magic")
    # Hacer un join con als_records para recuperar la columna 'clienteId' original
    recs = recs.join(
        als_records.select("clienteId", "clienteId_numeric"),
        on="clienteId_numeric",
        how="left",
    )

    # Convertir a pandas
    recs_to_parse = recs.select("clienteId", "cod_articulo_magic").toPandas()

    lista_rec = [f"r{i+1}" for i in range(sku_len)]
    # recs_to_parse[lista_rec] = pd.DataFrame(recs_to_parse['cod_articulo_magic'].tolist(), index= recs_to_parse.index)
    new_cols = pd.DataFrame(
        recs_to_parse["cod_articulo_magic"].tolist(),
        index=recs_to_parse.index,
        columns=lista_rec,
    )
    recs_to_parse = pd.concat([recs_to_parse, new_cols], axis=1)
    client_recs = pd.melt(
        recs_to_parse, id_vars=["clienteId"], value_vars=lista_rec
    )

    # client_recs['compania'] = client_recs["clienteId"].apply(lambda x: str(x)[:2])
    client_recs["compania"] = client_recs["clienteId"].str.split("|").str[-2].apply(lambda x: str(x))
    # client_recs['cliente'] = client_recs["clienteId"].apply(lambda x: str(x)[2:])
    client_recs["cliente"] = client_recs["clienteId"].str.split("|").str[1]
    client_recs["compania"] = client_recs["compania"].apply(lambda x: x.rjust(4, "0"))
    client_recs["id_cliente"] = client_recs["compania"] + "|" + client_recs["cliente"]
    client_recs["cod_articulo_magic"] = client_recs["value"]
    # print(client_recs)

    client_recs = (
        client_recs[["id_cliente", "cod_articulo_magic"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # Obtener el nombre del archivo CSV
    nombre_archivo = "Output/D_rutas_rec.csv"
    # Verificar si el archivo ya existe
    try:
        # Intentar abrir el archivo para lectura
        with open(nombre_archivo, "r") as f:
            # Si el archivo existe, header=False para no escribir cabeceras
            client_recs.to_csv(
                nombre_archivo, mode="a", header=False, index=False, sep=";"
            )
    except FileNotFoundError:
        # Si el archivo no existe, header=True para escribir las cabeceras
        client_recs.to_csv(nombre_archivo, mode="a", header=True, index=False, sep=";")


# ## 4.3 Corriendo PS

# In[83]:


def runALS():
    # Ruta del archivo que deseas verificar y eliminar si existe
    ruta_archivo = "Output/D_rutas_rec.csv"
    # Verificar si el archivo existe
    if os.path.exists(ruta_archivo):
        # Eliminar el archivo si existe
        os.remove(ruta_archivo)
        print(f"El archivo en la ruta {ruta_archivo} ha sido eliminado.")
    else:
        print(f"El archivo en la ruta {ruta_archivo} no existe.")
    # CORRIENDO ALS PARA RUTAS DE MAS DE 45 SKUs
    for ruta in rutas:
        print("*" * 21)
        temp = pd.read_csv(f"Processed/rutas/D_{ruta}_ventas.csv")
        sku_len = temp["cod_articulo_magic"].nunique()
        print(f"SKUs disponibles en ruta {ruta}:", sku_len)
        als_training_job(ruta, sku_len)
    # CORRIENDO ALS PARA rutas que tengan menos de 45 SKUS
    # if len(low_sku_ruta) != 0:
    #     temp = pd.read_csv(f"Processed/rutas/D_low_ruta_ventas.csv")
    #     sku_len = temp["cod_articulo_magic"].nunique()
    #     print(f"SKUs disponibles en low ruta {low_sku_ruta}:", sku_len)
    #     als_training_job("low_ruta", sku_len)
    # else:
    #     print("No existen rutas con pocos skus :D")


# In[84]:

df_ventas = pd.read_parquet("Processed/mexico_ventas_manana.parquet")
rutas = df_ventas.groupby(["cod_ruta"])["cod_articulo_magic"].nunique()
low_sku_ruta = rutas[rutas < 5].index.tolist()
rutas = rutas[rutas >= 5].index.tolist()

# In[85]:

runALS()

print("*"*10,"DONE PASO 3","*"*10)