import subprocess

subprocess.run(["pip", "install", "awswrangler[redshift]"], check=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL)
subprocess.run(["pip", "install", "psycopg2-binary"], check=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL)
subprocess.run(["pip", "install", "openpyxl"], check=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL)
subprocess.run(["pip", "install", "redshift-connector"], check=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL)

import warnings
warnings.filterwarnings("ignore")

# In[3]:

import pandas as pd
import numpy as np
import redshift_connector
import awswrangler as wr
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import boto3
import io
import pytz
import re
import gc
import random

#####################################################################################################
fecha_tomorrow = (
    datetime.now(pytz.timezone("America/Lima")) + timedelta(days=1)
).strftime("%Y-%m-%d")
final_rec = pd.read_csv(f'Output/PS_piloto_data_v1/D_pan_recs_data_{fecha_tomorrow}.csv')
df_ventas = pd.read_parquet("Processed/mexico_ventas_manana.parquet")
df_ventas["fecha_liquidacion"] = pd.to_datetime(df_ventas["fecha_liquidacion"])

# ### 5.8.2 Para subir a PS

# In[227]:

recomendaciones_para_salesforce = final_rec[
    ["cod_compania", "cod_sucursal", "cod_cliente", "cod_modulo", "sku"]
]
recomendaciones_para_salesforce["Pais"] = "MX"
recomendaciones_para_salesforce["Cajas"] = int(1)
recomendaciones_para_salesforce["Unidades"] = int(0)
recomendaciones_para_salesforce["Fecha"] = fecha_tomorrow
recomendaciones_para_salesforce = recomendaciones_para_salesforce[
    [
        "Pais",
        "cod_compania",
        "cod_sucursal",
        "cod_cliente",
        "cod_modulo",
        "sku",
        "Cajas",
        "Unidades",
        "Fecha",
    ]
]
recomendaciones_para_salesforce.columns = [
    "Pais",
    "Compania",
    "Sucursal",
    "Cliente",
    "Modulo",
    "Producto",
    "Cajas",
    "Unidades",
    "Fecha",
]


# In[228]:

recomendaciones_para_salesforce["Compania"] = recomendaciones_para_salesforce[
    "Compania"
].apply(lambda x: str(int(x)).rjust(4, "0"))
recomendaciones_para_salesforce["Sucursal"] = recomendaciones_para_salesforce[
    "Sucursal"
].apply(lambda x: str(int(x)).rjust(2, "0"))

# In[235]:

rutas_cliente = df_ventas[["cod_cliente", "cod_ruta"]].drop_duplicates()

# **Conteo de marcas recomendadas**

# In[239]:

count = (
    final_rec.groupby("id_cliente")["marca_rec"]
    .apply(lambda x: "-".join((x)))
    .reset_index(name="marcas_ordenadas")
)
count = (
    count.groupby("marcas_ordenadas")["id_cliente"]
    .nunique()
    .sort_values(ascending=False)
)


# **Conteo de SKUs recomendadas**

# In[240]:

count2 = (
    final_rec.groupby("id_cliente")["sku"]
    .apply(lambda x: "-".join(x.astype(str)))
    .reset_index(name="marcas_ordenadas")
)
count2 = (
    count2.groupby("marcas_ordenadas")["id_cliente"]
    .nunique()
    .sort_values(ascending=False)
    .reset_index()
)
count2["f"] = count2["id_cliente"] / count2["id_cliente"].sum()
count2["fa"] = count2["f"].cumsum()

# In[241]:

print(f"RESUMEN para {fecha_tomorrow}")
print(
    "Total de clientes a recomendar: ",
    recomendaciones_para_salesforce.Cliente.nunique(),
)
print("Combinaciones de recomendaciones de MARCAS:", count.shape[0])
print("Marcar recomendadas:", final_rec.marca_rec.unique())
print("Combinaciones de recomendaciones de SKUs:", count2.shape[0])
print(
    "Combinaciones de recomendaciones de SKUs al 80% de clientes:",
    count2[count2["fa"] <= 0.8].shape[0],
)
print("SKUs usados en la recomendacion:", final_rec["sku"].nunique())

# In[247]:

recomendaciones_para_salesforce.to_csv(f"Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv", index=False)

# In[248]:

# Establecer la conexión con S3
bucket_name = 'aje-analytics-ps-backup'  # nombre de bucket en S3
file_name = f'PS_Mexico/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv'  # nombre para el archivo en S3
s3_path = f's3://{bucket_name}/{file_name}'

# Escribir el dataframe en S3 con AWS Data Wrangler
wr.s3.to_csv(recomendaciones_para_salesforce, s3_path, index=False)

print("*"*10,"DONE PASO 5","*"*10)