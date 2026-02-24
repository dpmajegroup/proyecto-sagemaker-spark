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

# ## 1.1 Juntar VENTAS y VISITAS

# In[19]:


pan_ventas = pd.read_parquet("Input/ventas_mexico000.parquet")
pan_visitas = pd.read_parquet("Input/visitas_mexico000.parquet")


# In[20]:


pan_visitas = pan_visitas[(pan_visitas.codigo_canal__c==2)&(pan_visitas.compania__c==30)].reset_index(drop=True)


# In[21]:


pan_ventas["cod_articulo_magic"] = pan_ventas["id_producto"].str.split("|").str[-1]
pan_ventas["cod_articulo_magic"] = pan_ventas["cod_articulo_magic"].astype(int)



# In[23]:


# Nos quedamos solo con las filas del proceso de hoy
# Obtener la fecha de hoy en el mismo formato
hoy = int(datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d'))
print(hoy)
# Filtrar solo las filas con la fecha de hoy
pan_ventas = pan_ventas[pan_ventas['fecha_proceso'] == hoy]


# In[25]:


# Obtener la fecha y año actual
current_date = datetime.now(pytz.timezone("America/Lima"))
# Formatear la fecha en el formato "YYYYMM"
formatted_date = current_date.strftime("%Y-%m-%d")

# In[26]:


# Establecer la conexión con S3
bucket_name = 'aje-analytics-ps-backup'  # nombre de bucket en S3
file_name = f'PS_Mexico/Input/visitas_mexico000_{formatted_date}.csv'  # nombre para el archivo en S3
s3_path = f's3://{bucket_name}/{file_name}'

# Escribir el dataframe en S3 con AWS Data Wrangler
wr.s3.to_csv(pan_visitas, s3_path, index=False)

# In[27]:

pan_visitas = pan_visitas.rename(columns={'sucursal__c': 'cod_sucursal'})

# In[29]:


# PAN VENTAS
pan_ventas["cod_compania"] = (
    pan_ventas["cod_compania"].astype(str).apply(lambda x: str(int(x)).rjust(4, "0"))
)
pan_ventas["id_cliente"] = (
    "MX"
    + "|"
    + pan_ventas["cod_compania"].astype(str)
    + "|"
    + pan_ventas["cod_cliente"].astype(str)
)

# PAN VISITAS
pan_visitas["compania__c"] = (
    pan_visitas["compania__c"].astype(str).apply(lambda x: str(int(x)).rjust(4, "0"))
)
pan_visitas["id_cliente"] = (
    "MX"
    + "|"
    + pan_visitas["compania__c"].astype(str)
    + "|"
    + pan_visitas["codigo_cliente__c"].astype(str)
)


# In[30]:


# Eliminar Duplicados y quedarse con la ultima visita más reciente
visita_default_semana_atras = (datetime.now(pytz.timezone("America/Lima")) - timedelta(days=7)).strftime("%Y-%m-%d")
pan_visitas["ultima_visita"] = pan_visitas["ultima_visita"].fillna(
    visita_default_semana_atras
)
pan_visitas = (
    pan_visitas.sort_values(["id_cliente", "ultima_visita"], ascending=False)
    .groupby("id_cliente")
    .head(1)
)


# In[31]:


# Combina ventas y visitas (nos quedamos con ruta y módulo de visitas si es nulo, usamos el valor de ventas)
df_merged = pd.merge(
    pan_ventas,
    pan_visitas[
        [
            "id_cliente",
            "dias_de_visita__c",
            "periodo_de_visita__c",
            "ultima_visita",
            "cod_ruta",
            "cod_modulo",
            "cod_sucursal",
        ]
    ],
    on="id_cliente",
    # how='outer',  # Este es un merge 'outer', para conservar todos los clientes de ambos DataFrames.
    how="inner",
    suffixes=(
        "_df1",
        "_df2",
    ),  # Añadimos sufijos para diferenciar las columnas que se solapan
)

# Reemplazar los valores nulos de 'cod_ruta_df2' con los valores de 'cod_ruta_df1', y viceversa
# Si 'cod_ruta_df2' es nulo, se usará el valor de 'cod_ruta_df1'. Si ambos son nulos, se mantendrá nulo.
df_merged["cod_ruta"] = df_merged["cod_ruta_df2"].combine_first(
    df_merged["cod_ruta_df1"]
)
df_merged["cod_modulo"] = df_merged["cod_modulo_df2"].combine_first(
    df_merged["cod_modulo_df1"]
)
df_merged["cod_sucursal"] = df_merged["cod_sucursal_df2"].combine_first(
    df_merged["cod_sucursal_df1"]
)

# Eliminar las columnas innecesarias ('cod_ruta_df1', 'cod_ruta_df2', 'cod_modulo_df1', 'cod_modulo_df2')
df_merged = df_merged.drop(
    columns=["cod_ruta_df1", "cod_ruta_df2", "cod_modulo_df1", "cod_modulo_df2", "cod_sucursal_df1", "cod_sucursal_df2"]
)


# In[32]:

pan_ventas = df_merged.copy()
# Liberar memoria del df_merged
df_merged = None 
gc.collect()


# In[33]:


pan_ventas["cod_cliente"] = pan_ventas["cod_cliente"].astype(str)
pan_ventas["cod_ruta"] = pan_ventas["cod_ruta"].astype(int)
pan_ventas["cod_modulo"] = pan_ventas["cod_modulo"].astype(int)



# In[35]:


# Al cruzar visita con ventas. hay clientes de. visita que no tienen informacion de venta, esto se remplaza a nivel de ruta.
# si la ruta entera no tiene ventas, se ignora
final_pan_ventas = pd.DataFrame()
# Este df guarda clientes con visitas y sin ventas para luego recomendarlos por separado
df_clientes_con_visitas_sin_ventas = pd.DataFrame()
for ruta in pan_ventas["cod_ruta"].unique():
    temp_df = pan_ventas[pan_ventas["cod_ruta"] == ruta]
    # Si la ruta entera no tiene ninguna venta, se guarda en un dataframe aparte
    if len(temp_df) == (temp_df["cod_articulo_magic"].isnull().sum()):
        df_clientes_con_visitas_sin_ventas = pd.concat(
            [df_clientes_con_visitas_sin_ventas, temp_df], axis=0
        ).reset_index(drop=True)


# In[38]:


pan_ventas.to_parquet("Processed/ventas_mexico_12m.parquet",index=False)


# # 2 Ajustar DF de clientes: Se filtrará a los clientes que tienen visita al día siguiente

# ## 2.2 Combinar Ventas con Subgiro, Descripcion de productos, Segmento del cliente y Fecha de creacion

# In[39]:

# Leer Archivos
pan_ventas = pd.read_parquet("Processed/ventas_mexico_12m.parquet")

# CAMBIAR DEPORADE POR SPORADE
pan_ventas["desc_marca"] = pan_ventas["desc_marca"].str.strip()
pan_ventas["desc_marca"] = pan_ventas["desc_marca"].replace({"DEPORADE": "SPORADE"})

pan_prod = pd.read_csv("Input/MX_maestro_productos.csv")
df_seg = pd.read_csv("Input/D_SubSegmentacion_mexico_2024-07-25.csv")

# Crear ID_CLIENTE
# PAN_VENTAS
pan_ventas["cod_sucursal"] = pan_ventas["cod_sucursal"].astype(str)
# PAN SEGMENTOS
pan_segmentos = df_seg[["id_cliente", "new_segment"]].drop_duplicates()


# In[40]:
pan_ventas = pd.merge(
    pan_ventas,
    pan_prod[["cod_articulo_magic", "desc_articulo"]],
    how="left",
    on="cod_articulo_magic",
)
# pan_ventas=pd.merge(pan_ventas,pan_giros[["id_cliente","desc_sucursal","desc_giro","desc_subgiro"]],how='left',on='id_cliente')
# pan_ventas=pd.merge(pan_ventas,pan_giros[["id_cliente","desc_sucursal"]],how='left',on='id_cliente')
pan_ventas = pd.merge(pan_ventas, pan_segmentos, on="id_cliente", how="left")
# pan_ventas=pd.merge(pan_ventas,pan_fechacreacion[["id_cliente","fecha_creacion"]],how='left',on='id_cliente')
pan_ventas["new_segment"] = pan_ventas["new_segment"].fillna("OPTIMIZAR")
pan_ventas["mes"] = pd.to_datetime(pan_ventas["fecha_liquidacion"]).dt.strftime(
    "%Y-%m-01"
)

# In[41]:

pan_ventas.to_parquet("Processed/ventas_mexico_12m.parquet",index=False)


# ## 2.3 Seleccionando SKUs que no se commpran en los ultimos 3 dias
# La variable **sucursal_sku** contiene todos los SKUs disponibles por sucursal



# In[43]:

pan_ventas = pd.read_parquet("Processed/ventas_mexico_12m.parquet")

# In[44]:

sucursales = pan_ventas["cod_sucursal"].unique()

# In[45]:


# Obtener la fecha actual
fecha_actual = datetime.now(pytz.timezone("America/Lima")).date()
# Lista para almacenar las fechas
last_3_days = []
dias_atras = 3
sucursal_sku = {}
# Obtener las fechas de los últimos 3 días
for i in range(1, dias_atras + 1):
    fecha = fecha_actual - timedelta(days=i)
    fecha_formato = fecha.strftime("%Y-%m-%d")
    last_3_days.append(fecha_formato)

# In[47]:


for sucursal in sucursales:
    temp = pan_ventas[pan_ventas["cod_sucursal"] == sucursal]
    # sku_fecha=temp[["cod_articulo_magic","fecha_liquidacion"]].sort_values(["cod_articulo_magic","fecha_liquidacion"]).drop_duplicates().reset_index(drop=True)
    # sku_fecha=sku_fecha.groupby("cod_articulo_magic")[["cod_articulo_magic","fecha_liquidacion"]].tail(3)
    # #Listando SKUs comprados al menos una vez en los ultimos 3 dias
    # sku_in_last_3_days=sku_fecha[sku_fecha.fecha_liquidacion.isin(last_3_days)]["cod_articulo_magic"].unique()
    # print(f"Sucursal {sucursal} eliminando SKUs:",sku_fecha[(~sku_fecha["cod_articulo_magic"].isin(sku_in_last_3_days))].reset_index(drop=True)["cod_articulo_magic"].sort_values().unique())
    # #SKU COMPRADO AL MENOS una vez en los ultimos 3 dias
    # sku_fecha=sku_fecha[sku_fecha["cod_articulo_magic"].isin(sku_in_last_3_days)].reset_index(drop=True)
    # temp=temp[temp["cod_articulo_magic"].isin(sku_fecha["cod_articulo_magic"].unique())].reset_index(drop=True)
    sucursal_sku[sucursal] = temp["cod_articulo_magic"].sort_values().unique()

# ## 2.4 Filtrar clientes a visitar mañana

# In[49]:

pan_ventas = pd.read_parquet("Processed/ventas_mexico_12m.parquet")

# In[54]:

data_test = (
    pan_ventas[
        ["id_cliente", "dias_de_visita__c", "periodo_de_visita__c", "ultima_visita"]
    ]
    .drop_duplicates()
    .reset_index(drop=True)
)
data_test["ultima_visita"] = pd.to_datetime(
    data_test["ultima_visita"], format="%Y-%m-%d"
)
# Calcula la diferencia en días entre la fecha de cada fila y la fecha de hoy
fecha_actual = datetime.now() + timedelta(days=0)
data_test["dias_pasados"] = (fecha_actual - data_test["ultima_visita"]).dt.days

# In[55]:

# Obtener el día de la semana actual (1 para lunes, 2 para martes, ..., 7 para domingo)
dia_actual = datetime.now(pytz.timezone("America/Lima")).weekday() + 1

# Si hoy es domingo (7), el día siguiente es lunes (1), de lo contrario, es el siguiente día al actual
if dia_actual == 6:
    dia_siguiente = 7
    # dia_siguiente=6
else:
    dia_siguiente = (dia_actual + 1) % 7
    # dia_siguiente = (dia_actual ) % 7

# Filtrar los clientes que serán visitados mañana
clientes_a_visitar_manana = data_test[
    data_test["dias_de_visita__c"]
    .astype(str)
    .apply(lambda x: str(dia_siguiente) in x.split(";"))
].reset_index(drop=True)
print("dia_actual", dia_actual)
print("dia_siguiente", dia_siguiente)

# In[56]:

# Definir condiciones de filtro
condicion_f1 = clientes_a_visitar_manana["periodo_de_visita__c"] == "F1"
condicion_f2 = (clientes_a_visitar_manana["periodo_de_visita__c"] == "F2") & (
    clientes_a_visitar_manana["dias_pasados"] > 13
)
condicion_f3 = (clientes_a_visitar_manana["periodo_de_visita__c"] == "F3") & (
    clientes_a_visitar_manana["dias_pasados"] > 20
)
condicion_f4 = (clientes_a_visitar_manana["periodo_de_visita__c"] == "F4") & (
    clientes_a_visitar_manana["dias_pasados"] > 27
)

# Aplicar las condiciones de filtro
clientes_a_visitar_manana = clientes_a_visitar_manana[
    condicion_f1 | condicion_f2 | condicion_f3 | condicion_f4
].reset_index(drop=True)


# In[60]:

pan_ventas.groupby("cod_ruta")["dias_de_visita__c"].unique().reset_index()

# In[61]:

pan_ventas = pan_ventas[
    pan_ventas["id_cliente"].isin(clientes_a_visitar_manana["id_cliente"])
].reset_index(drop=True)

# In[67]:

pan_ventas.to_parquet("Processed/mexico_ventas_manana.parquet",index=False)

# # 3. Pre Procesamiento

# In[69]:

df_ventas = pd.read_parquet("Processed/mexico_ventas_manana.parquet")

# In[70]:

df_ventas["fecha_liquidacion"] = pd.to_datetime(
    df_ventas["fecha_liquidacion"], format="%Y-%m-%d"
)
df_ventas["desc_marca"] = df_ventas["desc_marca"].str.strip()
df_ventas["desc_categoria"] = df_ventas["desc_categoria"].str.strip()


# ## 3.1 Mapeo de Pesos por Giro
# Los pesos representan el orden en el que se recomendara la marca para cada cliente dependiendo del Giro

# In[71]:

# Mapeo de pesos para ordenar las recomendaciones finales segun su importancia por giro
mapeo_diccionario = {}

# In[74]:

for giro_v in df_ventas["desc_subgiro"].unique():
    temp = df_ventas[(df_ventas["desc_subgiro"] == giro_v)]
    ranks = temp.groupby("desc_categoria")["cant_cajafisicavta"].sum().reset_index()
    ranks.columns = ["index","desc_categoria"]
    ranks = ranks.sort_values(by="desc_categoria", ascending=False)
    if len(list(ranks["desc_categoria"])) <= 5:
        ranks["Ranking"] = range(1, len(ranks) + 1)
    elif len(list(ranks["desc_categoria"])) > 5:
        a = list(ranks["desc_categoria"])
        b = [1, 1, 2, 2]
        # Calculamos el multiplicador para el mapeo de pesos segun la varianza
        if np.std(a) / np.mean(a) <= 1.2:
            multiplicador = 4
        else:
            multiplicador = 2
        if len(a) > 5:
            for i in range(4, len(a)):
                if a[3] <= a[i] * multiplicador:
                    b.append(3)
                else:
                    b.append(3 + i)
        ranks["Ranking"] = b

    mapeo_diccionario[giro_v] = ranks.set_index("index")["Ranking"].to_dict()

# ## 3.2. Filtro de SKU por Ruta
# Cada Ruta tiene una cantidad de SKUs distintos, la recomendacion será distinta para cada ruta, sin embargo, aquellas rutas que tengan pocos SKUs serán tratadas como una sola ruta

# In[75]:

rutas = (
    df_ventas.groupby(["cod_ruta"])["id_cliente"]
    .nunique()
    .sort_values(ascending=False)
    .reset_index()["cod_ruta"]
    .unique()
)

# In[76]:


low_sku_ruta = []


# **Para rutas que tengan más de 45 SKUS**

# In[77]:

for ruta in rutas:
    print("*" * 21)
    print("Ruta:", ruta)
    temp = df_ventas[(df_ventas["cod_ruta"] == ruta)]
    print("SKUs disponibles:", temp["cod_articulo_magic"].nunique())
    # print(f"Giros en ruta {ruta}:")
    # print(temp.groupby(["desc_giro"])["id_cliente"].nunique(dropna=False))
    if temp["cod_articulo_magic"].nunique() < 10:
        low_sku_ruta.append(ruta)
    else:
        temp.to_csv(f"Processed/rutas/D_{ruta}_ventas.csv", index=False)

# **Para rutas que tengan menos de 45 SKUS**

# In[78]:

print("*" * 21)
print("Rutas:", low_sku_ruta)
temp = df_ventas[(df_ventas["cod_ruta"].isin(low_sku_ruta))]
print("SKUs disponibles:", temp["cod_articulo_magic"].nunique())
# print(f"Giros en ruta {ruta}:")
# print(temp.groupby(["desc_giro"])["id_cliente"].nunique(dropna=False))
temp.to_parquet("Processed/rutas/D_low_ruta_ventas.parquet", index=False)
print("*"*10,"DONE PASO 2","*"*10)