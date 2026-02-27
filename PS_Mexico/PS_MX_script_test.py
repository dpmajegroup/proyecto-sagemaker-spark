# %% [markdown]
# # 1 Descargar Ventas y Visitas (aje-prd-analytics-artifacts-s3)

# %%
!pip install --upgrade pip setuptools wheel
!pip install --upgrade pyarrow==17.0.0
!pip install "awswrangler[redshift]" --no-build-isolation
!pip install psycopg2-binary
!pip install openpyxl
!pip install redshift-connector

# %%
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

# %%
def comprobar_inputs():
    # Conectarse a S3
    s3 = boto3.client("s3")
    bucket_name = "aje-prd-analytics-artifacts-s3"
    prefix = "pedido_sugerido/data-v1/mexico/"
    
    hoy = datetime.now(pytz.timezone("America/Lima")).date()
    errores = []

    # Listar objetos en la ruta de S3
    objetos = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" not in objetos:
        print("ERROR: No se encontraron archivos en la ruta especificada.")
        return

    # Iterar sobre los objetos
    for objeto in objetos["Contents"]:
        key = objeto["Key"]
        print(key)
        # Omitir "carpetas" en S3
        if key.endswith("/"):
            continue

        last_modified = objeto["LastModified"].date()
        size_kb = objeto["Size"] / 1024  # Convertir a KB

        # Verificar si el objeto tiene contenido real (FULL_OBJECT)
        if objeto["Size"] == 0:
            errores.append(f"ERROR: El archivo {key} está vacío.")

        # Verificar si el archivo ha sido modificado hoy
        if last_modified != hoy:
            errores.append(f"ERROR: El archivo {key} no ha sido modificado hoy ({hoy}), su ultima fecha de modificacion fue {last_modified}.")

        # Verificar si el tamaño del archivo es menor a 1 KB
        if size_kb < 1:
            errores.append(f"ALERTA: El archivo {key} tiene un tamaño menor a 1 KB ({size_kb:.2f} KB).")

    # Mostrar los errores y lanzar una excepción si es necesario
    if errores:
        for error in errores:
            print(error)
        raise ValueError("Se encontraron problemas con los archivos en S3.")
    else:
        print("Todo bien :D")

# %%
comprobar_inputs()

# %% [markdown]
# ## 1.0 Descargar Maestro de productos

# %%
# PARA 2024
query = f"""select * from 
    comercial_mexico.dim_producto
    where estado='A' and instancia='MX';
    """

con = wr.data_api.redshift.connect(
    cluster_id="dwh-cloud-storage-salesforce-prod",
    database="dwh_prod",
    db_user="dwhuser",
)
maestro_prod = wr.data_api.rds.read_sql_query(query, con)

# %%
maestro_prod.head()

# %%
maestro_prod[["cod_articulo_magic", "desc_articulo"]].drop_duplicates().reset_index(
    drop=True
).to_csv("Input/MX_maestro_productos.csv", index=False)

# %%
rutas_test = [1155,1158,1074,1065]
rutas_test

# %%
len(rutas_test)

# %%
clientes_ruta_test = []

# %%
def descargar_visitas():
    # Guardar clientes de las rutas
    global clientes_ruta_test

    # Conectarse a S3
    s3 = boto3.client("s3")
    bucket_name = "aje-prd-analytics-artifacts-s3"
    prefix = "pedido_sugerido/data-v1/mexico/"

    # Listar objetos en la ruta de S3
    objetos = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    print(objetos)

    # Iterar sobre los objetos
    for objeto in objetos["Contents"]:
        if objeto["Size"] > 0 and objeto["Key"].split("/")[-1] == "visitas_mexico000":
            # Obtener el nombre del objeto y descargarlo
            nombre_archivo = objeto["Key"].split("/")[-1]
            # Descargar el archivo en memoria
            response = s3.get_object(Bucket=bucket_name, Key=objeto["Key"])
            content = response["Body"].read()
            # Filtrar y guardar el archivo si corresponde
            if nombre_archivo == "visitas_mexico000":
                # Convertir los bytes a DataFrame de Pandas
                df = pd.read_csv(io.BytesIO(content), sep=";")
                df = df[
                    (df["compania__c"] == 30)
                    & (df["cod_ruta"].isin(rutas_test))
                    & (df.codigo_canal__c == 2)
                ].reset_index(drop=True)
                # Guardamos estos clientes para descargarlos en ventas (por si cambiaron de ruta)
                clientes_ruta_test = df["codigo_cliente__c"].unique()
                nombre_csv = f"Input/{nombre_archivo}.parquet"
                df["personalizacion"] = df["personalizacion"].astype(str)
                df.to_parquet(nombre_csv,index=False)
            else:
                continue

# %%
%%time
descargar_visitas()

# %%
def descargar_ventas():
    # Conectarse a S3
    s3 = boto3.client("s3")
    bucket_name = "aje-prd-analytics-artifacts-s3"
    prefix = "pedido_sugerido/data-v1/mexico/"

    # Listar objetos en la ruta de S3
    objetos = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    print(objetos)

    # Iterar sobre los objetos
    for objeto in objetos["Contents"]:
        if objeto["Size"] > 0 and objeto["Key"].split("/")[-1] != "visitas_mexico000":
            # Obtener el nombre del objeto y descargarlo
            nombre_archivo = objeto["Key"].split("/")[-1]

            # Descargar el archivo en memoria
            response = s3.get_object(Bucket=bucket_name, Key=objeto["Key"])
            content = response["Body"].read()

            # Filtrar y guardar el archivo si corresponde
            if nombre_archivo == "ventas_mexico000":
                # Convertir los bytes a DataFrame de Pandas
                df = pd.read_csv(io.BytesIO(content), sep=";")
                df = df[
                    (df["cod_compania"] == 30)
                    & (
                        (df["cod_ruta"].isin(rutas_test))
                        | (df["cod_cliente"].isin(clientes_ruta_test))
                    )
                ].reset_index(drop=True)[['id_cliente', 'id_sucursal', 'id_producto',
       'fecha_liquidacion',"cod_ruta","cod_modulo",
       'cod_zona', 'cant_cajafisicavta', 'cant_cajaunitvta','imp_netovta',
       'cod_compania', 'desc_compania', 'cod_sucursal',
       'desc_sucursal', 'cod_pais', 'fecha_creacion_cliente', 'cod_cliente',
       'desc_marca', 'desc_formato', 'desc_categoria', 'cod_giro',
       'cod_subgiro', 'desc_giro', 'desc_subgiro', 'fecha_proceso']]
                nombre_csv = f"Input/{nombre_archivo}.parquet"
                df.to_parquet(nombre_csv,index=False)
            else:
                continue

# %%
%%time
descargar_ventas()

# %% [markdown]
# ## 1.1 Juntar VENTAS y VISITAS

# %%
pan_ventas = pd.read_parquet("Input/ventas_mexico000.parquet")
pan_visitas = pd.read_parquet("Input/visitas_mexico000.parquet")

# %%
pan_visitas = pan_visitas[(pan_visitas.codigo_canal__c==2)&(pan_visitas.compania__c==30)].reset_index(drop=True)

# %%
pan_ventas["cod_articulo_magic"] = pan_ventas["id_producto"].str.split("|").str[-1]
pan_ventas["cod_articulo_magic"] = pan_ventas["cod_articulo_magic"].astype(int)

# %%
pan_ventas.shape

# %%
# Nos quedamos solo con las filas del proceso de hoy
# Obtener la fecha de hoy en el mismo formato
hoy = int(datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d'))
print(hoy)
# Filtrar solo las filas con la fecha de hoy
pan_ventas = pan_ventas[pan_ventas['fecha_proceso'] == hoy]

# %%
pan_ventas.shape

# %%
# Obtener la fecha y año actual
current_date = datetime.now(pytz.timezone("America/Lima"))
# Formatear la fecha en el formato "YYYYMM"
formatted_date = current_date.strftime("%Y-%m-%d")
formatted_date

# %%
pan_visitas.eje_potencial__c.unique()

# %%
# Establecer la conexión con S3
bucket_name = 'aje-analytics-ps-backup'  # nombre de bucket en S3
file_name = f'PS_Mexico/Input/visitas_mexico000_{formatted_date}.csv'  # nombre para el archivo en S3
s3_path = f's3://{bucket_name}/{file_name}'

# Escribir el dataframe en S3 con AWS Data Wrangler
wr.s3.to_csv(pan_visitas, s3_path, index=False)

# %%
pan_visitas = pan_visitas.rename(columns={'sucursal__c': 'cod_sucursal'})

# %%
pan_ventas.cod_articulo_magic.nunique()

# %%
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

# %%
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

# %%
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
            "eje_potencial__c",
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

# %%
pan_ventas = df_merged.copy()
# Liberar memoria del df_merged
df_merged = None 
gc.collect()

# %%
pan_ventas["cod_cliente"] = pan_ventas["cod_cliente"].astype(str)
pan_ventas["cod_ruta"] = pan_ventas["cod_ruta"].astype(int)
pan_ventas["cod_modulo"] = pan_ventas["cod_modulo"].astype(int)

# %%
pan_ventas.cod_ruta.unique()

# %%
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

# %%
pan_ventas["id_cliente"].nunique()

# %%
pan_ventas.groupby("cod_sucursal").agg(
    {"cod_articulo_magic": "nunique", "id_cliente": "nunique", "cod_ruta": "nunique"}
)

# %%
pan_ventas.to_parquet("Processed/ventas_mexico_12m.parquet",index=False)

# %% [markdown]
# # 2 Ajustar DF de clientes: Se filtrará a los clientes que tienen visita al día siguiente

# %% [markdown]
# ## 2.2 Combinar Ventas con Subgiro, Descripcion de productos, Segmento del cliente y Fecha de creacion

# %%
import pandas as pd

# Leer Archivos
pan_ventas = pd.read_parquet("Processed/ventas_mexico_12m.parquet")

# CAMBIAR DEPORADE POR SPORADE
pan_ventas["desc_marca"] = pan_ventas["desc_marca"].str.strip()
pan_ventas["desc_marca"] = pan_ventas["desc_marca"].replace({"DEPORADE": "SPORADE"})

pan_prod = pd.read_csv("Input/MX_maestro_productos.csv")

# Crear ID_CLIENTE
# PAN_VENTAS
pan_ventas["cod_sucursal"] = pan_ventas["cod_sucursal"].astype(str)

# PAN SEGMENTOS
pan_ventas = pan_ventas.rename(columns={"eje_potencial__c": "new_segment"})
# mapear valores
mapping = {
    "S1": "BLINDAR",
    "S2": "DESARROLLAR",
    "S4": "MANTENER",
    "S5": "OPTIMIZAR",
}

pan_ventas["new_segment"] = pan_ventas["new_segment"].map(mapping)

# %%
pan_ventas.new_segment.unique()

# %%
pan_ventas = pd.merge(
    pan_ventas,
    pan_prod[["cod_articulo_magic", "desc_articulo"]],
    how="left",
    on="cod_articulo_magic",
)

pan_ventas["new_segment"] = pan_ventas["new_segment"].fillna("OPTIMIZAR")
pan_ventas["mes"] = pd.to_datetime(pan_ventas["fecha_liquidacion"]).dt.strftime(
    "%Y-%m-01"
)

# %%
pan_ventas.to_parquet("Processed/ventas_mexico_12m.parquet",index=False)

# %% [markdown]
# ## 2.3 Seleccionando SKUs que no se commpran en los ultimos 3 dias
# La variable **sucursal_sku** contiene todos los SKUs disponibles por sucursal

# %%
# pan_ventas[["cod_compania","cod_sucursal"]].drop_duplicates().to_csv("MX_compa_sucursal_2025-08-04.csv",index=False)

# %%
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# %%
# pan_ventas = pd.read_csv(
#     "Processed/ventas_mexico_12m.csv", dtype={"dias_de_visita__c": "str"}
# )
pan_ventas = pd.read_parquet("Processed/ventas_mexico_12m.parquet")

# %%
sucursales = pan_ventas["cod_sucursal"].unique()
sucursales

# %%
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

# %%
last_3_days

# %%
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

# %% [markdown]
# ## 2.4 Filtrar clientes a visitar mañana

# %%
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# %%
pan_ventas = pd.read_parquet("Processed/ventas_mexico_12m.parquet")

# %%
# Filtrar ventas mayores a 0
# pan_ventas=pan_ventas[pan_ventas["imp_netovta"]>0]

# En lugar de omitir estos clientes, mejor los consideramos como OPTIMIZAR
# pan_ventas.loc[pan_ventas["imp_netovta"] <= 0, "new_segment"] = "OPTIMIZAR"

# %%
pan_ventas.id_cliente.nunique()

# %%
pan_ventas.dias_de_visita__c.unique()

# %%
pan_ventas.dias_de_visita__c.value_counts()

# %%
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
fecha_actual

# %%
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

# %%
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

# %%
clientes_a_visitar_manana.isnull().sum()

# %%
clientes_a_visitar_manana.periodo_de_visita__c.unique()

# %%
clientes_a_visitar_manana.dias_de_visita__c.unique()

# %%
pan_ventas.groupby("cod_ruta")["dias_de_visita__c"].unique().reset_index()

# %%
pan_ventas = pan_ventas[
    pan_ventas["id_cliente"].isin(clientes_a_visitar_manana["id_cliente"])
].reset_index(drop=True)
pan_ventas

# %%
pan_ventas.periodo_de_visita__c.unique()

# %%
pan_ventas.dias_de_visita__c.unique()

# %%
pan_ventas.id_cliente.nunique()

# %%
pan_ventas.columns

# %%
# pan_ventas[["id_cliente", 'cod_sucursal', 'desc_sucursal','dias_de_visita__c',
#        'periodo_de_visita__c', 'ultima_visita', 'cod_ruta', 'cod_modulo']].drop_duplicates().to_csv("PS_MX_clientes_2024-10-31.csv",index=False)

# %%
pan_ventas.to_parquet("Processed/mexico_ventas_manana.parquet",index=False)

# %% [markdown]
# # 3. Pre Procesamiento

# %%
import pandas as pd
import numpy as np
from datetime import datetime

# %%
df_ventas = pd.read_parquet("Processed/mexico_ventas_manana.parquet")

# %%
df_ventas["fecha_liquidacion"] = pd.to_datetime(
    df_ventas["fecha_liquidacion"], format="%Y-%m-%d"
)
df_ventas["desc_marca"] = df_ventas["desc_marca"].str.strip()
df_ventas["desc_categoria"] = df_ventas["desc_categoria"].str.strip()

# %% [markdown]
# ## 3.1 Mapeo de Pesos por Giro
# Los pesos representan el orden en el que se recomendara la marca para cada cliente dependiendo del Giro

# %%
# Mapeo de pesos para ordenar las recomendaciones finales segun su importancia por giro
mapeo_diccionario = {}

# %%
df_ventas.desc_marca.unique()

# %%
df_ventas.desc_categoria.unique()

# %%
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

    print("*" * 20)
    print("GIRO: ", giro_v)
    print("Categorias: ", list(ranks["index"]))
    print("Counts: ", list(ranks["desc_categoria"]))
    print(ranks.set_index("index")["Ranking"].to_dict())
    mapeo_diccionario[giro_v] = ranks.set_index("index")["Ranking"].to_dict()

# %% [markdown]
# ## 3.2. Filtro de SKU por Ruta
# Cada Ruta tiene una cantidad de SKUs distintos, la recomendacion será distinta para cada ruta, sin embargo, aquellas rutas que tengan pocos SKUs serán tratadas como una sola ruta

# %%
rutas = (
    df_ventas.groupby(["cod_ruta"])["id_cliente"]
    .nunique()
    .sort_values(ascending=False)
    .reset_index()["cod_ruta"]
    .unique()
)
rutas

# %%
low_sku_ruta = []

# %% [markdown]
# **Para rutas que tengan más de 45 SKUS**

# %%
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

# %% [markdown]
# **Para rutas que tengan menos de 45 SKUS**

# %%
print("*" * 21)
print("Rutas:", low_sku_ruta)
temp = df_ventas[(df_ventas["cod_ruta"].isin(low_sku_ruta))]
print("SKUs disponibles:", temp["cod_articulo_magic"].nunique())
# print(f"Giros en ruta {ruta}:")
# print(temp.groupby(["desc_giro"])["id_cliente"].nunique(dropna=False))
temp.to_parquet("Processed/rutas/D_low_ruta_ventas.parquet", index=False)

# %% [markdown]
# # 4. Modelo Pedido Sugerido

# %% [markdown]
# ## 4.1. Importando librerias necesarias

# %%
import os

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

# %% [markdown]
# ## 4.2 Construccion del algoritmo ALS

# %%
!java -version

# %%
import os
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# %%
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

# %% [markdown]
# ## 4.3 Corriendo PS

# %%
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
    if len(low_sku_ruta) != 0:
        temp = pd.read_csv(f"Processed/rutas/D_low_ruta_ventas.csv")
        sku_len = temp["cod_articulo_magic"].nunique()
        print(f"SKUs disponibles en low ruta {low_sku_ruta}:", sku_len)
        als_training_job("low_ruta", sku_len)
    else:
        print("No existen rutas con pocos skus :D")

# %%
rutas = rutas[~pd.Series(rutas).isin(pd.Series(low_sku_ruta))]
rutas

# %%
%%time
runALS()

# %% [markdown]
# # 5. Ajustes al output del algoritmo ALS

# %% [markdown]
# ## 5.-9 Quedarnos con skus que hayan tenido ventas en los ultimos 30 dias

# %%
pan_rec = pd.read_csv("Output/D_rutas_rec.csv", sep=";")
pan_rec["id_cliente"] = "MX|" + pan_rec["id_cliente"]

# %%
pan_rec = pd.merge(pan_rec,pan_ventas[["id_cliente","cod_ruta"]].drop_duplicates(),on="id_cliente",how="left")

# %%
df_ventas = pd.read_parquet("Processed/mexico_ventas_manana.parquet")

# %%
df_ventas["fecha_liquidacion"] = pd.to_datetime(df_ventas["fecha_liquidacion"])

# %%
# Definir el umbral de tiempo (últimos 30 días desde hoy)
# bajamos de 30 a 14 dias 2025-07-25
hoy = pd.Timestamp.today()
fecha_limite = hoy - pd.Timedelta(days=14)
print(fecha_limite)
# Filtrar ventas en los últimos 30 días
ventas_filtradas = df_ventas[df_ventas["fecha_liquidacion"] >= fecha_limite]

# %%
# Obtener los productos vendidos en cada ruta
productos_por_ruta = ventas_filtradas.groupby("cod_ruta")["cod_articulo_magic"].unique().reset_index()

# %%
# Unir con los productos vendidos en cada ruta
recomendaciones_validas = pan_rec.merge(productos_por_ruta, on="cod_ruta", how="inner")

# %%
recomendaciones_validas.columns = ['id_cliente', 'cod_articulo_magic', 'cod_ruta','lista_sku_ruta']

# %%
recomendaciones_validas.shape

# %%
recomendaciones_validas  = recomendaciones_validas[recomendaciones_validas.apply(lambda row: row["cod_articulo_magic"] in row["lista_sku_ruta"], axis=1)].reset_index(drop=True)

# %%
recomendaciones_validas.shape

# %%
recomendaciones_validas.head()

# %%
pan_rec = recomendaciones_validas[["id_cliente","cod_articulo_magic"]].reset_index(drop=True)

# %%
pan_rec.head()

# %%
pan_rec.shape

# %%
pan_rec.to_parquet("Output/D_rutas_rec.parquet", index=False)

# %% [markdown]
# ## 5.-8 Recomendar de acuerdo a Subida, Bajada, Mantener

# %%
pan_rec = pd.read_parquet("Output/D_rutas_rec.parquet")
#pan_rec["id_cliente"] = "MX|" + pan_rec["id_cliente"]

# %%
pan_rec.shape

# %%
pan_rec = pd.merge(pan_rec,pan_ventas[["id_cliente","cod_ruta"]].drop_duplicates(),on="id_cliente",how="left")

# %%
df_ventas = pd.read_parquet("Processed/mexico_ventas_manana.parquet")

# %%
def desplazar_nulos_fila(fila):
    return pd.Series(sorted(fila, key=pd.isna), index=fila.index)

# %%
# Definir una función para asignar los valores subida, mantener y bajar
def clasificar_valor(x):
    if x > 0:
        return "S"
    elif x == 0:
        return "M"
    else:
        return "B"

# %%
df_ventas['fecha_liquidacion'] = pd.to_datetime(df_ventas['fecha_liquidacion'])

# Definir la fecha de referencia (hoy)
fecha_actual = datetime.now(pytz.timezone("America/Lima"))#.strftime('%Y-%m-%d')
fecha_30dias_atras = (fecha_actual - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
fecha_60dias_atras = (fecha_actual - pd.Timedelta(days=60)).strftime('%Y-%m-%d')

print("fecha_actual",fecha_actual)
print("fecha_30dias_atras",fecha_30dias_atras)
print("fecha_60dias_atras",fecha_60dias_atras)

# %%
# Filtrar los últimos 30 días
df_ultimos_30 = df_ventas[(df_ventas['fecha_liquidacion'] > fecha_30dias_atras) 
                          & (df_ventas['fecha_liquidacion'] <= fecha_actual.strftime('%Y-%m-%d'))]

# Filtrar los días 31 a 60
df_31_60 = df_ventas[(df_ventas['fecha_liquidacion'] > fecha_60dias_atras)
              & (df_ventas['fecha_liquidacion'] <= fecha_30dias_atras)]

# Agrupar por codigo_usuario, ruta, codigo_producto y sumar ventas
ventas_ultimos_30 = df_ultimos_30.groupby(['cod_ruta', 'cod_articulo_magic'])['imp_netovta'].sum().reset_index()
ventas_ultimos_30["mes"] = "0_30"
ventas_31_60 = df_31_60.groupby(['cod_ruta', 'cod_articulo_magic'])['imp_netovta'].sum().reset_index()
ventas_31_60["mes"] = "31_60"

df_grouped = pd.concat([ventas_ultimos_30,ventas_31_60],axis=0,ignore_index=True)
df_grouped = pd.pivot_table(
    df_grouped,
    values="imp_netovta",
    index=["cod_ruta", "cod_articulo_magic"],
    columns=["mes"],
    aggfunc="sum",
).reset_index()

# Rellenando con 0 si es nulo
df_grouped["0_30"] = df_grouped["0_30"].fillna(0)
df_grouped["31_60"] = df_grouped["31_60"].fillna(0)

# Asignando variacion de productos por mes
df_grouped["v1_2"] = (
    (df_grouped[df_grouped.columns[2]] - df_grouped[df_grouped.columns[3]])
    / df_grouped[df_grouped.columns[3]]
    * 100
)
df_grouped["v1_2"].replace([float('inf'), float('-inf')], -1, inplace=True)
df_grouped["v1_2"] = df_grouped["v1_2"].fillna(-1)
df_grouped["vp"] = (df_grouped["v1_2"])

# %%
# Aplicar la función a la columna 'valor' y guardar el resultado en una nueva columna 'flag'
df_grouped["flag"] = df_grouped["vp"].apply(lambda x: clasificar_valor(x))
# Definir el mapeo de valores
mapeo_flag = {"S": 0, "M": 1, "B": 2}
# Aplicar el mapeo a la columna 'clasificacion' y guardar el resultado en una nueva columna 'flag_rank'
df_grouped["flag_rank"] = df_grouped["flag"].map(mapeo_flag)

# %%
df_grouped

# %%
pan_rec.shape

# %%
pan_rec = pd.merge(pan_rec,df_grouped[["cod_ruta", "cod_articulo_magic","flag_rank"]],on=["cod_ruta", "cod_articulo_magic"],how="left")

# %%
pan_rec["flag_rank"] = pan_rec["flag_rank"].fillna(3)

# %%
# Agregar una columna con el orden original
pan_rec["original_order"] = pan_rec.index

# %%
# Ordenar por cod_cliente, rank_producto (ascendente) y mantener el orden original en caso de empate
pan_rec = pan_rec.sort_values(by=["id_cliente", "flag_rank", "original_order"], ascending=[True, True, True])#.drop(columns=["original_order"])

# %%
pan_rec.head()

# %%
pan_rec.shape

# %%
pan_rec = pan_rec[["id_cliente","cod_articulo_magic"]].reset_index(drop=True)

# %%
pan_rec.head()

# %%
pan_rec.shape

# %%
pan_rec.to_parquet("Output/D_rutas_rec.parquet", index=False)

# %% [markdown]
# ## 5.-7 Usamos el archivo de validacion para no recomendar SKUs que no se deben a las rutas

# %%
pan_rec = pd.read_parquet("Output/D_rutas_rec.parquet")
# pan_rec["id_cliente"] = "MX|" + pan_rec["id_cliente"]

# %%
pan_rec.shape

# %%
# Ruta del archivo en S3
s3_path = "s3://aje-prd-analytics-artifacts-s3/pedido_sugerido/data-v1/mexico/maestro_productos_mexico000"

# Leer el CSV directamente a un DataFrame
skus_val = pd.read_csv(s3_path,sep = ";")

# %%
skus_val = skus_val[skus_val.cod_compania==30]
# Nos quedamos solo con las filas del proceso de hoy
# Obtener la fecha de hoy en el mismo formato
hoy = int(datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d'))
print("fecha_proceso: ", hoy)
# Filtrar solo las filas con la fecha de hoy
# skus_val = skus_val[skus_val['fecha_proceso'] == hoy]
skus_val["cod_compania"] = (skus_val["cod_compania"].astype(str).apply(lambda x: str(int(x)).rjust(4, "0")))
skus_val["id_cliente"] = (
    "MX"
    + "|"
    + skus_val["cod_compania"].astype(str)
    + "|"
    + skus_val["cod_cliente"].astype(str)
)

# %%
skus_val.shape

# %%
pan_rec.groupby("id_cliente").cod_articulo_magic.nunique().mean()

# %%
pan_rec = pd.merge(pan_rec,skus_val[['cod_articulo_magic', 'id_cliente']].drop_duplicates(),
                   left_on=["id_cliente","cod_articulo_magic"],right_on=["id_cliente","cod_articulo_magic"],how="inner")

# %%
pan_rec.shape

# %%
pan_rec.groupby("id_cliente").cod_articulo_magic.nunique().mean()

# %%
pan_rec = pan_rec[["id_cliente","cod_articulo_magic"]]

# %%
pan_rec.head()

# %%
pan_rec.shape

# %%
pan_rec.to_parquet("Output/D_rutas_rec.parquet", index=False)

# %% [markdown]
# ## 5.-5 Filtrar SKUs por STOCK

# %%
stock = pd.read_csv("s3://aje-prd-analytics-artifacts-s3/pedido_sugerido/data-v1/mexico/D_stock_mx.csv")

# %%
stock.drop(columns=["Fecha","Database"], inplace=True)
stock.columns=["cod_compania","cod_sucursal","cod_articulo_magic","stock_cf"]

# %%
stock["cod_compania"] = (stock["cod_compania"].astype(str).apply(lambda x: str(int(x)).rjust(4, "0")))
stock["cod_sucursal"] = (stock["cod_sucursal"].astype(str).apply(lambda x: str(int(x)).rjust(2, "0")))

# %%
fecha_hace_12_dias = (datetime.today() - timedelta(days=12)).strftime('%Y-%m-%d')
print(fecha_hace_12_dias)

# %%
# Promedio diario de cajas fisicas por sucursal y compañia de los ultimos 12 dias
prom_diario_vta = df_ventas[df_ventas.cant_cajafisicavta > 0]
prom_diario_vta = df_ventas[df_ventas.fecha_liquidacion>=fecha_hace_12_dias].groupby(
    ["cod_compania","cod_sucursal","cod_articulo_magic","fecha_liquidacion"]).cant_cajafisicavta.sum().reset_index().groupby(
    ["cod_compania","cod_sucursal","cod_articulo_magic"]).cant_cajafisicavta.mean().reset_index()
prom_diario_vta["cod_compania"] = (prom_diario_vta["cod_compania"].astype(str).apply(lambda x: str(int(x)).rjust(4, "0")))
prom_diario_vta["cod_sucursal"] = (prom_diario_vta["cod_sucursal"].astype(str).apply(lambda x: str(int(x)).rjust(2, "0")))

# %%
df_stock = pd.merge(prom_diario_vta,stock, on=["cod_compania","cod_sucursal","cod_articulo_magic"],how="left")
df_stock["dias_stock"] = df_stock["stock_cf"]/df_stock["cant_cajafisicavta"]
df_stock =  df_stock[(df_stock.dias_stock > 3) & (df_stock.cant_cajafisicavta > 0)].reset_index(drop=True)

# %%
pan_rec.nunique()

# %%
pan_rec.groupby("id_cliente").cod_articulo_magic.nunique().mean()

# %%
pan_rec.shape

# %%
# Cruzamos stock

# %%
test = pd.merge(pan_rec,df_ventas[["id_cliente","cod_compania","cod_sucursal"]].drop_duplicates(),on="id_cliente",how="left")

# %%
test["cod_compania"] = (test["cod_compania"].astype(str).apply(lambda x: str(int(x)).rjust(4, "0")))
test["cod_sucursal"] = (test["cod_sucursal"].astype(str).apply(lambda x: str(int(x)).rjust(2, "0")))

# %%
test_f = pd.merge(test, df_stock, on=["cod_compania","cod_sucursal","cod_articulo_magic"], how="inner")

# %%
test_f.head()

# %%
test_f.shape

# %%
test_f.nunique()

# %%
PS_VAL_promedio_recs_por_cliente_stock = (
    test_f.groupby("id_cliente")["cod_articulo_magic"].nunique().mean()
)
PS_VAL_promedio_recs_por_cliente_stock

# %%
pan_rec = test_f[["id_cliente","cod_articulo_magic"]].copy()

# %%
pan_rec.to_csv("Output/D_rutas_rec.csv", sep=";", index=False)

# %% [markdown]
# ## 5.-4 Mantener Solo SKUS en especifico (solo usar si se tiene el excel con SKU activos -> el input de ventas ya deberia incluir este filtro)

# %%
pan_rec=pd.read_parquet("Output/D_rutas_rec.parquet")

# %%
pan_rec.groupby("id_cliente")["cod_articulo_magic"].nunique().mean()

# %%
pan_rec.groupby("id_cliente")["cod_articulo_magic"].count().mean()

# %%
sku_con_precio=pd.read_excel("Input/MX_SKUS.xlsx",sheet_name="Hoja2")

# %%
# Original size de recomendacioness
pan_rec.shape

# %%
pan_rec = pan_rec[pan_rec.cod_articulo_magic.isin(sku_con_precio["COD_SKUS"].unique())].reset_index(drop=True)

# %%
# Size de recomendaciones luego de filtar solo a los del excel de SKUs activos
pan_rec.shape

# %%
pan_rec.groupby("id_cliente")["cod_articulo_magic"].count().mean()

# %%
pan_rec["cod_articulo_magic"].nunique()

# %%
pan_rec.to_parquet("Output/D_rutas_rec.parquet", index=False)

# %% [markdown]
# ## 5.-3 Quitar Recomendaciones de SKUS en especifico

# %%
# import os
# import re
# import pandas as pd

# %%
# pan_rec=pd.read_csv("Output/D_rutas_rec.csv", sep=";")

# %%
# skus_sin_precio=[622398]

# %%
# pan_rec=pan_rec[~(pan_rec["cod_articulo_magic"].isin(skus_sin_precio))].reset_index(drop=True)

# %%
# pan_rec.to_csv("Output/D_rutas_rec.csv", sep=";", index=False)

# %% [markdown]
# ## 5.-2 Quitar Recomendaciones de los ultimos 14 dias

# %%
pan_rec=pd.read_parquet("Output/D_rutas_rec.parquet")

# %%
master_prod=pd.read_csv("Input/MX_maestro_productos.csv")
master_prod

# %%
fecha_tomorrow = (
    datetime.now(pytz.timezone("America/Lima")) + timedelta(days=1)
).strftime("%Y-%m-%d")
fecha_tomorrow

# %%
ruta_archivo = f"Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv"
ruta_archivo2 = f"Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.parquet"

# Verificar si el archivo existe
if os.path.exists(ruta_archivo):
    # Eliminar el archivo si existe
    os.remove(ruta_archivo)
    print(f"El archivo en la ruta {ruta_archivo} ha sido eliminado.")
else:
    print(f"El archivo en la ruta {ruta_archivo} no existe.")
    
# Verificar si el archivo existe
if os.path.exists(ruta_archivo2):
    # Eliminar el archivo si existe
    os.remove(ruta_archivo2)
    print(f"El archivo en la ruta {ruta_archivo2} ha sido eliminado.")
else:
    print(f"El archivo en la ruta {ruta_archivo2} no existe.")

# %%
# Directorio donde se encuentran los archivos
directorio = 'Output/PS_piloto_v1/'

# Expresión regular para encontrar fechas en el nombre de los archivos
patron_fecha = r'\d{4}-\d{2}-\d{2}'

# Lista para almacenar las fechas extraídas
fechas = []

# Iterar sobre los archivos en el directorio
for archivo in os.listdir(directorio):
    # Verificar si el archivo es un archivo CSV y coincide con el patrón de nombres
    if archivo.endswith('.csv') and re.match(r'^D_base_pedidos_\d{4}-\d{2}-\d{2}\.csv$', archivo):
        # Extraer la fecha del nombre del archivo
        fecha = re.search(patron_fecha, archivo).group()
        # Agregar la fecha a la lista
        fechas.append(fecha)

# %%
last_7_days_2=sorted(fechas)[-14:]
#last_7_days_2=last_7_days_2[:-1]
last_7_days_2

# %%
#Leer recomendaciones de los ultimos 7 dias y cruzar con clientes que tienen recomendacion para mañana
last_7_days_recs=pd.DataFrame()
for fecha_rec in last_7_days_2:
    df_temp=pd.read_csv(f"Output/PS_piloto_v1/D_base_pedidos_{fecha_rec}.csv",dtype={"Compania":"str","Cliente":"str"})
    df_temp["id_cliente"]='MX|'+df_temp['Compania']+'|'+df_temp['Cliente']
    df_temp=df_temp[df_temp["id_cliente"].isin(pan_rec["id_cliente"].unique())]
    last_7_days_recs=pd.concat([last_7_days_recs,df_temp],axis=0)
    print(f"{fecha_rec} done")

# %%
# Utilizamos merge para combinar los dataframes, utilizando la columna 'Cliente' como clave
df_combinado = pd.merge(pan_rec, last_7_days_recs, left_on=['id_cliente', 'cod_articulo_magic'],right_on=['id_cliente', 'Producto'], how='left', indicator=True)
# Filtramos los registros que solo están en el DataFrame 1
df_resultado = df_combinado[df_combinado['_merge'] == 'left_only'][["id_cliente","cod_articulo_magic"]].reset_index(drop=True)
df_resultado

# %%
pan_rec.shape

# %%
df_resultado.to_parquet("Output/D_rutas_rec.parquet",index=False)

# %% [markdown]
# ## 5.0 Calcular Irregularidad de clientes

# %%
pan_rec = pd.read_parquet("Output/D_rutas_rec.parquet")

# %%
pan_rec["id_cliente"].nunique()

# %%
now = pd.to_datetime(datetime.now(pytz.timezone("America/Lima")).strftime("%Y-%m-01"))
fecha_doce_meses_atras = now - pd.DateOffset(months=12)
lista_m12 = [fecha_doce_meses_atras + pd.DateOffset(months=i) for i in range(12)]
lista_m12

fecha_seis_meses_atras = now - pd.DateOffset(months=6)
lista_m6 = [fecha_seis_meses_atras + pd.DateOffset(months=i) for i in range(6)]
lista_m6

# %%
temp_df_ventas = df_ventas[df_ventas["id_cliente"].isin(pan_rec["id_cliente"].unique())]

# %%
qw = (
    temp_df_ventas[["id_cliente", "mes"]]
    .drop_duplicates()
    .sort_values(["id_cliente", "mes"])
    .groupby("id_cliente")
    .tail(12)
    .reset_index(drop=True)
)
qw["mes"] = pd.to_datetime(qw["mes"])
qw["m12"] = qw["mes"].isin(lista_m12)
qw["m6"] = qw["mes"].isin(lista_m6)

# %%
categoria_cliente = qw.groupby("id_cliente")[["m12", "m6"]].sum().reset_index()

# %%
# Definir condiciones
condicion_super = categoria_cliente["m12"] == 12
condicion_frecuente = (
    (categoria_cliente["m12"] < 12)
    & (categoria_cliente["m12"] >= 6)
    & (categoria_cliente["m6"] == 6)
)
condicion_regulares = (categoria_cliente["m6"] <= 6) & (categoria_cliente["m12"] <= 6)
condicion_riesgo = (categoria_cliente["m6"] < 6) & (categoria_cliente["m6"] >= 4)
condicion_irregulares = categoria_cliente["m6"] < 4

# Aplicar condiciones y asignar categorías
categoria_cliente.loc[condicion_super, "categoria_cliente"] = "Super"
categoria_cliente.loc[condicion_frecuente, "categoria_cliente"] = "Frecuente"
categoria_cliente.loc[condicion_regulares, "categoria_cliente"] = "Regular"
categoria_cliente.loc[condicion_riesgo, "categoria_cliente"] = "Riesgo"
categoria_cliente.loc[condicion_irregulares, "categoria_cliente"] = "Irregular"

# %% [markdown]
# **Recalcular Irregularidad**

# %%
# Definir condiciones
condicion_regular = (
    (categoria_cliente["categoria_cliente"] == "Super")
    | (categoria_cliente["categoria_cliente"] == "Frecuente")
    | (categoria_cliente["categoria_cliente"] == "Regular")
    | (categoria_cliente["categoria_cliente"] == "Riesgo")
)
condicion_irregular = categoria_cliente["categoria_cliente"] == "Irregular"
# Aplicar condiciones y asignar categorías
categoria_cliente.loc[condicion_regular, "categoria_cliente_2"] = "Regular"
categoria_cliente.loc[condicion_irregular, "categoria_cliente_2"] = "Irregular"

# %%
categoria_cliente

# %%
categoria_cliente["categoria_cliente"].value_counts(dropna=False)

# %%
categoria_cliente["categoria_cliente_2"].value_counts(dropna=False)

# %% [markdown]
# ## 5.1. Obteniendo marcas de recomendaciones por cliente

# %%
pan_rec = pd.read_parquet("Output/D_rutas_rec.parquet")

# %%
pan_ventas = df_ventas[(df_ventas["cod_ruta"].isin(rutas))]
pan_ventas2 = df_ventas[(df_ventas["cod_ruta"].isin(low_sku_ruta))]
pan_ventas = pd.concat([pan_ventas, pan_ventas2], axis=0)

# %%
marca_articulo = pan_ventas[["desc_categoria", "cod_articulo_magic"]].drop_duplicates()
print(marca_articulo.shape)
marca_articulo.head()

# %%
cliente_rec_marca = pd.merge(
    pan_rec, marca_articulo, on="cod_articulo_magic", how="left"
)
cliente_rec_marca["desc_categoria"] = cliente_rec_marca["desc_categoria"].str.strip()
print(cliente_rec_marca.shape)
cliente_rec_marca

# %% [markdown]
# ## 5.2. Calcular marcas distintas recomendadas por cliente

# %%
# Conteo de categorias recomendadas por Cliente
cods2 = cliente_rec_marca.groupby("id_cliente")["desc_categoria"].nunique().reset_index()
print(cods2.shape)
cods2.head()

# %% [markdown]
# ## 5.3 Quitando los SKUs de las ultimas 2 semanas (Evitar recompra)

# %%
last_2_weeks = pd.to_datetime(datetime.now(pytz.timezone("America/Lima"))) - pd.DateOffset(
    days=14
)
last_2_weeks

# %%
# Asegurarse de que 'fecha_liquidacion' esté en formato datetime
pan_ventas["fecha_liquidacion"] = pd.to_datetime(pan_ventas["fecha_liquidacion"])
# Convertir 'fecha_liquidacion' a la misma zona horaria que 'last_year'
pan_ventas["fecha_liquidacion"] = pan_ventas["fecha_liquidacion"].dt.tz_localize(
    "America/Lima", nonexistent="NaT", ambiguous="NaT"
)

# %%
df_quitar_2_weeks = pan_ventas[pan_ventas["fecha_liquidacion"]>=last_2_weeks][["id_cliente","cod_articulo_magic"]].drop_duplicates().reset_index(drop=True)

# %%
# Hacemos un merge para identificar coincidencias
cliente_rec_sin4 = cliente_rec_marca.merge(df_quitar_2_weeks, on=['id_cliente', 'cod_articulo_magic'], how='left', indicator=True)
# Filtramos solo los registros que NO están en df_quitar_2_weeks
cliente_rec_sin4 = cliente_rec_sin4[cliente_rec_sin4['_merge'] == 'left_only'].drop(columns=['_merge'])

# %%
cliente_rec_marca.id_cliente.nunique()

# %%
cliente_rec_sin4.id_cliente.nunique()

# %%
cliente_rec_sin4

# %% [markdown]
# ## 5.4 Filtro para calcula antiguedad de clientes

# %%
pan_ventas = pan_ventas.rename(columns={"fecha_creacion_cliente": "fecha_creacion"})

# %%
# Convertir la columna 'fecha_creacion' a tipo fecha
pan_ventas["fecha_creacion"] = pd.to_datetime(
    pan_ventas["fecha_creacion"], format="%Y%m%d"
)

# %%
# Definir una función para etiquetar los clientes según su fecha de creación
def etiquetar_cliente(fecha_creacion):
    if pd.isnull(fecha_creacion):
        return (
            "nf"  # Si la fecha de creación es nula, etiquetar como 'nf' (no encontrada)
        )
    else:
        hoy = datetime.now()  # Obtener la fecha actual
        hace_12_meses = hoy - timedelta(days=365)  # Calcular hace 12 meses
        if fecha_creacion >= hace_12_meses:
            return "new_client"  # Si la fecha de creación está dentro de los últimos 12 meses, etiquetar como 'new_client'
        else:
            return "old_client"  # Si la fecha de creación es anterior a los últimos 12 meses, etiquetar como 'old_client'

# %%
# Aplicar la función a la columna 'fecha_creacion' para crear la nueva columna 'tipo_cliente'
pan_ventas["antiguedad"] = pan_ventas["fecha_creacion"].apply(etiquetar_cliente)

# %%
pan_antiguedad_clientes = (
    pan_ventas[["id_cliente", "fecha_creacion", "antiguedad"]]
    .drop_duplicates()
    .reset_index(drop=True)
)

# %% [markdown]
# ## 5.4.2 Obteniendo datos necesarios para armar DF para subir a SalesForce

# %%
# pan_ventas.groupby(["id_cliente","cod_compania","cod_sucursal","cod_cliente","cod_modulo"])[["cant_cajafisicavta","cant_cajaunitvta"]].sum().reset_index()

# %%
datos_para_salesforce = (
    pan_ventas.groupby(
        ["id_cliente", "cod_compania", "cod_sucursal", "cod_cliente", "cod_ruta"]
    )[
        [
            "id_cliente",
            "cod_compania",
            "cod_sucursal",
            "cod_cliente",
            "cod_modulo",
            "cod_ruta",
        ]
    ]
    .head(1)
    .reset_index(drop=True)
)

# %% [markdown]
# ## 5.5 Juntando las Recomendaciones Finales
#  - Ordenado de acuerdo al peso de la recomendación y el id de usuario.
#  - Hay 5 skus de 5 marcas distintas que se recomiendan a cada usuario
#  - Ningún SKU que haya comprado el usuario en sus ultimas X visitas será recomendado. La cantidad de visitas (X) a ignorar depende de sus visitas promedio mensuales.

# %%
# Obtener primer SKU de cada marca de la recomendacion
final_rec = cliente_rec_sin4.groupby(["id_cliente", "desc_categoria"]).first().reset_index()
# Agregar Marca de sus primeras compras
# final_rec=pd.merge(final_rec,pan_evo_venta[["id_cliente","mes_1_marcaCount","mes_1_marca","mes_2_marca","mes_3_marca","mes_4_marca","mes_5_marca","mes_6_marca","len_m","tipo_cliente"]],how='left',on='id_cliente')
final_rec = pd.merge(final_rec, cods2, how="left", on="id_cliente")
# Agregar descripcion de SKU recomendado
pan_prod = pan_prod[["cod_articulo_magic", "desc_articulo"]].drop_duplicates()
pan_prod = pan_prod.groupby(["cod_articulo_magic"]).first().reset_index()
final_rec = pd.merge(final_rec, pan_prod, how="left", on="cod_articulo_magic")
# Cambiar nombre de columnas
# final_rec.columns=['id_cliente','marca_rec','sku','len_marca_origen','mes_1_marca',"mes_2_marca","mes_3_marca","mes_4_marca","mes_5_marca","mes_6_marca",'len_mes_compras','tipo','len_marca_rec','desc_rec']
final_rec.columns = ["id_cliente", "marca_rec", "sku", "len_marca_rec", "desc_rec"]
# Agregar Giro del cliente
giros = df_ventas[df_ventas["id_cliente"].isin(final_rec["id_cliente"].unique())][
    ["id_cliente", "desc_giro", "desc_subgiro"]
].drop_duplicates()
final_rec = pd.merge(final_rec, giros, on="id_cliente", how="left")
# Agregar si el cliente es Regular o Irregular
final_rec = pd.merge(
    final_rec,
    categoria_cliente[["id_cliente", "categoria_cliente_2"]],
    on="id_cliente",
    how="left",
)
final_rec.loc[final_rec["categoria_cliente_2"] == "Irregular", "desc_giro"] = np.nan
# Agregar antiguedad de cliente
final_rec = pd.merge(final_rec, pan_antiguedad_clientes, how="left", on="id_cliente")
# Agregar segmento de cliente
info_segmentos = pan_ventas[
    [
        "id_cliente",
        "new_segment",
        "dias_de_visita__c",
        "periodo_de_visita__c",
        "ultima_visita",
    ]
].drop_duplicates()
info_segmentos = info_segmentos.groupby(["id_cliente"]).first().reset_index()
final_rec = pd.merge(final_rec, info_segmentos, how="left", on="id_cliente")
# Agregar datos necesarios para SALESFORCE
final_rec = pd.merge(final_rec, datos_para_salesforce, how="left", on="id_cliente")
# Agregar Peso de marcas dependiendo del giro del cliente
# final_rec["peso"]= final_rec['marca_rec'].replace(mapeo_pesos)
# final_rec['peso'] = final_rec.apply(lambda row: mapeo_diccionario[row['desc_giro']].get(row['marca_rec'], 5), axis=1)
final_rec["peso"] = final_rec.apply(
    lambda row: mapeo_diccionario.get(row["desc_subgiro"], {}).get(row["marca_rec"], 5),
    axis=1,
)

# %%
# Ordenar la recomendacion de acuerdo al peso de la marca y nos quedamos con las 5 primeras marcas
final_rec = (
    final_rec.sort_values(["id_cliente", "peso"]).groupby(["id_cliente"]).head(5)
)
# Agregamos rank de la marca recomendada
final_rec["marca_rec_rank"] = final_rec.groupby("id_cliente").cumcount() + 1
final_rec = final_rec.reset_index(drop=True)

# %%
final_rec["sku"] = final_rec["sku"].astype(int)
final_rec["peso"] = final_rec["peso"].astype(int)
final_rec["marca_rec_rank"] = final_rec["marca_rec_rank"].astype(int)

# %%
final_rec

# %% [markdown]
# ## 5.7 Filtro de Recomendaciones por Segmento de cliente

# %%
# Definir una función para filtrar las filas según el valor de 'new_segment'
def filtrar_segmento(group):
    segmento = group["new_segment"].iloc[
        0
    ]  # Obtener el valor del segmento para el grupo
    if segmento == "BLINDAR":
        return group.head(1)  # Si el segmento es 'Blindar', mantener la primera fila
    elif segmento == "MANTENER":
        return group.head(
            2
        )  # Si el segmento es 'Mantener', mantener las dos primeras filas
    elif segmento == "DESARROLLAR":
        return group.head(
            3
        )  # Si el segmento es 'Desarrollar', mantener las tres primeras filas
    elif segmento == "OPTIMIZAR":
        return group.head(
            4
        )  # Si el segmento es 'Optimizar', mantener las cuatro primeras filas
    else:
        return group  # Si el segmento no es ninguno de los especificados, mantener todas las filas

# %%
final_rec = (
    final_rec.groupby("id_cliente").apply(filtrar_segmento).reset_index(drop=True)
)

# %%
final_rec.groupby("new_segment")["id_cliente"].nunique()

# %% [markdown]
# ## 5.8. Guardando recomendacion para D&A y Comercial

# %%
final_rec.id_cliente.nunique()

# %%
final_rec.dias_de_visita__c.value_counts(dropna=False)

# %%
final_rec.periodo_de_visita__c.value_counts(dropna=False)

# %%
final_rec.ultima_visita.value_counts(dropna=False)

# %% [markdown]
# ### 5.8.1 Para D&A

# %%
fecha_tomorrow = (
    datetime.now(pytz.timezone("America/Lima")) + timedelta(days=1)
).strftime("%Y-%m-%d")
fecha_tomorrow

# %%
final_rec.to_csv(
    f"Output/PS_piloto_data_v1/D_pan_recs_data_{fecha_tomorrow}.csv", index=False
)

# %%
# Establecer la conexión con S3
bucket_name = 'aje-analytics-ps-backup'  # nombre de bucket en S3
file_name = f'PS_Mexico/Output/PS_piloto_data_v1/D_pan_recs_data_{fecha_tomorrow}.csv'  # nombre para el archivo en S3
s3_path = f's3://{bucket_name}/{file_name}'

# Escribir el dataframe en S3 con AWS Data Wrangler
wr.s3.to_csv(final_rec, s3_path, index=False)

# %% [markdown]
# ### 5.8.2 Para subir a PS

# %%
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

# %%
recomendaciones_para_salesforce["Compania"] = recomendaciones_para_salesforce[
    "Compania"
].apply(lambda x: str(int(x)).rjust(4, "0"))
recomendaciones_para_salesforce["Sucursal"] = recomendaciones_para_salesforce[
    "Sucursal"
].apply(lambda x: str(int(x)).rjust(2, "0"))

# %%
recomendaciones_para_salesforce.dtypes

# %%
recomendaciones_para_salesforce.Sucursal.unique()

# %%
recomendaciones_para_salesforce.Compania.unique()

# %%
recomendaciones_para_salesforce.Cliente.nunique()

# %%
recomendaciones_para_salesforce

# %%
recomendaciones_para_salesforce.Cliente.nunique()

# %%
rutas_cliente = df_ventas[["cod_cliente", "cod_ruta"]].drop_duplicates()

# %%
pd.merge(
    recomendaciones_para_salesforce,
    rutas_cliente,
    left_on="Cliente",
    right_on="cod_cliente",
    how="left",
).cod_ruta.nunique()

# %%
pd.merge(
    recomendaciones_para_salesforce,
    rutas_cliente,
    left_on="Cliente",
    right_on="cod_cliente",
    how="left",
).groupby("cod_ruta")["Cliente"].nunique()

# %%
pd.merge(
    recomendaciones_para_salesforce,
    rutas_cliente,
    left_on="Cliente",
    right_on="cod_cliente",
    how="left",
).groupby("cod_ruta")["Cliente"].nunique().mean()

# %% [markdown]
# **Conteo de marcas recomendadas**

# %%
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
count.head(10)

# %% [markdown]
# **Conteo de SKUs recomendadas**

# %%
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
count2

# %%
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

# %%
recomendaciones_para_salesforce

# %%
recomendaciones_para_salesforce.groupby("Cliente")["Producto"].nunique().mean()

# %%
recomendaciones_para_salesforce.groupby("Sucursal")["Cliente"].nunique()

# %%
asd = (
    recomendaciones_para_salesforce.groupby("Cliente")
    .agg({"Producto": ["count", "nunique"]})
    .reset_index()
)
asd.columns = ["Cliente", "count", "nunique"]
asd["dif"] = asd["count"] == asd["nunique"]
asd.sort_values(["dif", "count"])

# %%
asd.dif.value_counts()

# %%
recomendaciones_para_salesforce.to_csv(
    f"Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv", index=False
)

# %%
# Establecer la conexión con S3
bucket_name = 'aje-analytics-ps-backup'  # nombre de bucket en S3
file_name = f'PS_Mexico/Output/PS_piloto_v1/D_base_pedidos_{fecha_tomorrow}.csv'  # nombre para el archivo en S3
s3_path = f's3://{bucket_name}/{file_name}'

# Escribir el dataframe en S3 con AWS Data Wrangler
wr.s3.to_csv(recomendaciones_para_salesforce, s3_path, index=False)

# %%
# Apaga el Kernel para luego poder usar los demás notebooks sin llenar la memoria
import IPython
app = IPython.get_ipython()
app.kernel.do_shutdown(False)  # True indica apagar completamente el kernel

# %%
# 100

# %%
# !pip install pipreqs

# %%
# !pipreqs . --force

# %%
#

# %%



