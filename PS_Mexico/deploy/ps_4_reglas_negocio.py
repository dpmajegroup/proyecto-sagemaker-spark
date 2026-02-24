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

# # 5. Ajustes al output del algoritmo ALS

# ## 5.-9 Quedarnos con skus que hayan tenido ventas en los ultimos 30 dias

# In[86]:

pan_rec = pd.read_csv("Output/D_rutas_rec.csv", sep=";")
pan_rec["id_cliente"] = "MX|" + pan_rec["id_cliente"]

# In[87]:

df_ventas = pd.read_parquet("Processed/mexico_ventas_manana.parquet")
df_ventas["fecha_liquidacion"] = pd.to_datetime(df_ventas["fecha_liquidacion"])

# In[88]:

pan_rec = pd.merge(pan_rec,df_ventas[["id_cliente","cod_ruta"]].drop_duplicates(),on="id_cliente",how="left")

# In[90]:

# Definir el umbral de tiempo (últimos 30 días desde hoy)
hoy = pd.Timestamp.today()
fecha_limite = hoy - pd.Timedelta(days=30)
print(fecha_limite)
# Filtrar ventas en los últimos 30 días
ventas_filtradas = df_ventas[df_ventas["fecha_liquidacion"] >= fecha_limite]

# In[91]:

# Obtener los productos vendidos en cada ruta
productos_por_ruta = ventas_filtradas.groupby("cod_ruta")["cod_articulo_magic"].unique().reset_index()

# In[92]:

# Unir con los productos vendidos en cada ruta
recomendaciones_validas = pan_rec.merge(productos_por_ruta, on="cod_ruta", how="inner")

# In[93]:

recomendaciones_validas.columns = ['id_cliente', 'cod_articulo_magic', 'cod_ruta','lista_sku_ruta']

# In[95]:

recomendaciones_validas  = recomendaciones_validas[recomendaciones_validas.apply(lambda row: row["cod_articulo_magic"] in row["lista_sku_ruta"], axis=1)].reset_index(drop=True)

# In[98]:

pan_rec = recomendaciones_validas[["id_cliente","cod_articulo_magic"]].reset_index(drop=True)

# In[101]:

pan_rec.to_parquet("Output/D_rutas_rec.parquet", index=False)

# ## 5.-8 Recomendar de acuerdo a Subida, Bajada, Mantener

# In[102]:

pan_rec = pd.read_parquet("Output/D_rutas_rec.parquet")
#pan_rec["id_cliente"] = "MX|" + pan_rec["id_cliente"]

# In[104]:

pan_rec = pd.merge(pan_rec,df_ventas[["id_cliente","cod_ruta"]].drop_duplicates(),on="id_cliente",how="left")

# In[106]:


def desplazar_nulos_fila(fila):
    return pd.Series(sorted(fila, key=pd.isna), index=fila.index)

# In[107]:

# Definir una función para asignar los valores subida, mantener y bajar
def clasificar_valor(x):
    if x > 0:
        return "S"
    elif x == 0:
        return "M"
    else:
        return "B"

# In[108]:

# Definir la fecha de referencia (hoy)
fecha_actual = datetime.now(pytz.timezone("America/Lima"))#.strftime('%Y-%m-%d')
fecha_30dias_atras = (fecha_actual - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
fecha_60dias_atras = (fecha_actual - pd.Timedelta(days=60)).strftime('%Y-%m-%d')

print("fecha_actual",fecha_actual)
print("fecha_30dias_atras",fecha_30dias_atras)
print("fecha_60dias_atras",fecha_60dias_atras)

# In[109]:


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


# In[110]:

# Aplicar la función a la columna 'valor' y guardar el resultado en una nueva columna 'flag'
df_grouped["flag"] = df_grouped["vp"].apply(lambda x: clasificar_valor(x))
# Definir el mapeo de valores
mapeo_flag = {"S": 0, "M": 1, "B": 2}
# Aplicar el mapeo a la columna 'clasificacion' y guardar el resultado en una nueva columna 'flag_rank'
df_grouped["flag_rank"] = df_grouped["flag"].map(mapeo_flag)

# In[113]:

pan_rec = pd.merge(pan_rec,df_grouped[["cod_ruta", "cod_articulo_magic","flag_rank"]],on=["cod_ruta", "cod_articulo_magic"],how="left")

# In[114]:

pan_rec["flag_rank"] = pan_rec["flag_rank"].fillna(3)

# In[115]:

# Agregar una columna con el orden original
pan_rec["original_order"] = pan_rec.index

# In[116]:

# Ordenar por cod_cliente, rank_producto (ascendente) y mantener el orden original en caso de empate
pan_rec = pan_rec.sort_values(by=["id_cliente", "flag_rank", "original_order"], ascending=[True, True, True])#.drop(columns=["original_order"])

# In[119]:

pan_rec = pan_rec[["id_cliente","cod_articulo_magic"]].reset_index(drop=True)

# In[122]:


pan_rec.to_parquet("Output/D_rutas_rec.parquet", index=False)


# ## 5.-7 Usamos el archivo de validacion para no recomendar SKUs que no se deben a las rutas

# In[123]:

pan_rec = pd.read_parquet("Output/D_rutas_rec.parquet")
# pan_rec["id_cliente"] = "MX|" + pan_rec["id_cliente"]

# In[125]:


# Ruta del archivo en S3
s3_path = "s3://aje-prd-analytics-artifacts-s3/pedido_sugerido/data-v1/mexico/maestro_productos_mexico000"

# Leer el CSV directamente a un DataFrame
skus_val = pd.read_csv(s3_path,sep = ";")

# In[126]:

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

# In[129]:


pan_rec = pd.merge(pan_rec,skus_val[['cod_articulo_magic', 'id_cliente']].drop_duplicates(),
                   left_on=["id_cliente","cod_articulo_magic"],right_on=["id_cliente","cod_articulo_magic"],how="inner")


# In[132]:

pan_rec = pan_rec[["id_cliente","cod_articulo_magic"]]

# In[135]:

pan_rec.to_parquet("Output/D_rutas_rec.parquet", index=False)


# ## 5.-5 Cruzar recomendaciones con df_quiebres (STOCK)
# ACA VA NUEVO CODIGO DE STOCK

# ## 5.-4 Mantener Solo SKUS en especifico (solo usar si se tiene el excel con SKU activos -> el input de ventas ya deberia incluir este filtro)

# In[158]:

pan_rec=pd.read_parquet("Output/D_rutas_rec.parquet")

# In[161]:

sku_con_precio=pd.read_excel("Input/MX_SKUS.xlsx",sheet_name="Hoja2")

# In[163]:

pan_rec = pan_rec[pan_rec.cod_articulo_magic.isin(sku_con_precio["COD_SKUS"].unique())].reset_index(drop=True)

# In[167]:

pan_rec.to_parquet("Output/D_rutas_rec.parquet", index=False)


# ## 5.-3 Quitar Recomendaciones de SKUS en especifico

# In[168]:


# import os
# import re
# import pandas as pd


# In[169]:


# pan_rec=pd.read_csv("Output/D_rutas_rec.csv", sep=";")


# In[170]:


# skus_sin_precio=[622398]


# In[171]:


# pan_rec=pan_rec[~(pan_rec["cod_articulo_magic"].isin(skus_sin_precio))].reset_index(drop=True)


# In[172]:


# pan_rec.to_csv("Output/D_rutas_rec.csv", sep=";", index=False)


# ## 5.-2 Quitar Recomendaciones de los ultimos 14 dias

# In[173]:

pan_rec=pd.read_parquet("Output/D_rutas_rec.parquet")

# In[174]:

master_prod=pd.read_csv("Input/MX_maestro_productos.csv")

# In[175]:

fecha_tomorrow = (
    datetime.now(pytz.timezone("America/Lima")) + timedelta(days=1)
).strftime("%Y-%m-%d")

# In[176]:


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


# In[177]:


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


# In[178]:

last_7_days_2=sorted(fechas)[-14:]
print(last_7_days_2)

# In[179]:

#Leer recomendaciones de los ultimos 7 dias y cruzar con clientes que tienen recomendacion para mañana

if len(last_7_days_2) > 0:
    # Leer recomendaciones de los ultimos 7 dias y cruzar con clientes que tienen recomendacion para mañana
    last_7_days_recs = pd.DataFrame()
    for fecha_rec in last_7_days_2:
        df_temp = pd.read_csv(f"Output/PS_piloto_v1/D_base_pedidos_{fecha_rec}.csv",dtype={"Compania": "str", "Cliente": "str"})
        df_temp["id_cliente"] = "MX|" + df_temp["Compania"] + "|" + df_temp["Cliente"]
        df_temp = df_temp[df_temp["id_cliente"].isin(pan_rec["id_cliente"].unique())]
        last_7_days_recs = pd.concat([last_7_days_recs, df_temp], axis=0)
        print(f"{fecha_rec} done")
else:
    last_7_days_recs = []
    
# In[180]:

# Utilizamos merge para combinar los dataframes, utilizando la columna 'Cliente' como clave

if len(last_7_days_recs)>0:
    # Utilizamos merge para combinar los dataframes, utilizando la columna 'Cliente' como clave
    df_combinado = pd.merge(pan_rec, last_7_days_recs, left_on=["id_cliente", "cod_articulo_magic"], right_on=["id_cliente", "Producto"], how="left", indicator=True)
    # Filtramos los registros que solo están en el DataFrame 1
    df_resultado = df_combinado[df_combinado["_merge"] == "left_only"][["id_cliente", "cod_articulo_magic"]].reset_index(drop=True)
else:
    df_resultado = []

# In[182]:

if len(df_resultado)>0:
    df_resultado.to_parquet("Output/D_rutas_rec.parquet",index=False)

# ## 5.0 Calcular Irregularidad de clientes

# In[183]:

pan_rec = pd.read_parquet("Output/D_rutas_rec.parquet")

# In[185]:


now = pd.to_datetime(datetime.now(pytz.timezone("America/Lima")).strftime("%Y-%m-01"))

fecha_doce_meses_atras = now - pd.DateOffset(months=12)
lista_m12 = [fecha_doce_meses_atras + pd.DateOffset(months=i) for i in range(12)]

fecha_seis_meses_atras = now - pd.DateOffset(months=6)
lista_m6 = [fecha_seis_meses_atras + pd.DateOffset(months=i) for i in range(6)]

# In[186]:

temp_df_ventas = df_ventas[df_ventas["id_cliente"].isin(pan_rec["id_cliente"].unique())]

# In[187]:


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

# In[188]:

categoria_cliente = qw.groupby("id_cliente")[["m12", "m6"]].sum().reset_index()

# In[189]:

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

# **Recalcular Irregularidad**

# In[190]:

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


# ## 5.1. Obteniendo marcas de recomendaciones por cliente

# In[194]:


pan_rec = pd.read_parquet("Output/D_rutas_rec.parquet")


# In[195]:

rutas = df_ventas.groupby(["cod_ruta"])["cod_articulo_magic"].nunique()
low_sku_ruta = rutas[rutas < 5].index.tolist()
rutas = rutas[rutas >= 5].index.tolist()


pan_ventas = df_ventas[(df_ventas["cod_ruta"].isin(rutas))]
pan_ventas2 = df_ventas[(df_ventas["cod_ruta"].isin(low_sku_ruta))]
pan_ventas = pd.concat([pan_ventas, pan_ventas2], axis=0)


# In[196]:

marca_articulo = pan_ventas[["desc_categoria", "cod_articulo_magic"]].drop_duplicates()

# In[197]:

cliente_rec_marca = pd.merge(
    pan_rec, marca_articulo, on="cod_articulo_magic", how="left"
)
cliente_rec_marca["desc_categoria"] = cliente_rec_marca["desc_categoria"].str.strip()

# ## 5.2. Calcular marcas distintas recomendadas por cliente

# In[198]:

# Conteo de categorias recomendadas por Cliente
cods2 = cliente_rec_marca.groupby("id_cliente")["desc_categoria"].nunique().reset_index()

# ## 5.3 Quitando los SKUs de las ultimas 2 semanas (Evitar recompra)

# In[199]:

last_2_weeks = pd.to_datetime(datetime.now(pytz.timezone("America/Lima"))) - pd.DateOffset(
    days=14
)

# In[200]:

# Asegurarse de que 'fecha_liquidacion' esté en formato datetime
pan_ventas["fecha_liquidacion"] = pd.to_datetime(pan_ventas["fecha_liquidacion"])
# Convertir 'fecha_liquidacion' a la misma zona horaria que 'last_year'
pan_ventas["fecha_liquidacion"] = pan_ventas["fecha_liquidacion"].dt.tz_localize(
    "America/Lima", nonexistent="NaT", ambiguous="NaT"
)

# In[201]:

df_quitar_2_weeks = pan_ventas[pan_ventas["fecha_liquidacion"]>=last_2_weeks][["id_cliente","cod_articulo_magic"]].drop_duplicates().reset_index(drop=True)

# In[202]:

# Hacemos un merge para identificar coincidencias
cliente_rec_sin4 = cliente_rec_marca.merge(df_quitar_2_weeks, on=['id_cliente', 'cod_articulo_magic'], how='left', indicator=True)
# Filtramos solo los registros que NO están en df_quitar_2_weeks
cliente_rec_sin4 = cliente_rec_sin4[cliente_rec_sin4['_merge'] == 'left_only'].drop(columns=['_merge'])

# ## 5.4 Filtro para calcula antiguedad de clientes

# In[206]:

pan_ventas = pan_ventas.rename(columns={"fecha_creacion_cliente": "fecha_creacion"})

# In[207]:

# Convertir la columna 'fecha_creacion' a tipo fecha
pan_ventas["fecha_creacion"] = pd.to_datetime(
    pan_ventas["fecha_creacion"], format="%Y%m%d"
)

# In[208]:

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

# In[209]:

# Aplicar la función a la columna 'fecha_creacion' para crear la nueva columna 'tipo_cliente'
pan_ventas["antiguedad"] = pan_ventas["fecha_creacion"].apply(etiquetar_cliente)

# In[210]:

pan_antiguedad_clientes = (
    pan_ventas[["id_cliente", "fecha_creacion", "antiguedad"]]
    .drop_duplicates()
    .reset_index(drop=True)
)

# ## 5.4.2 Obteniendo datos necesarios para armar DF para subir a SalesForce

# In[212]:


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

# ## 5.5 Juntando las Recomendaciones Finales
#  - Ordenado de acuerdo al peso de la recomendación y el id de usuario.
#  - Hay 5 skus de 5 marcas distintas que se recomiendan a cada usuario
#  - Ningún SKU que haya comprado el usuario en sus ultimas X visitas será recomendado. La cantidad de visitas (X) a ignorar depende de sus visitas promedio mensuales.

# In[213]:

# Obtener primer SKU de cada marca de la recomendacion
final_rec = cliente_rec_sin4.groupby(["id_cliente", "desc_categoria"]).first().reset_index()
# Agregar Marca de sus primeras compras
# final_rec=pd.merge(final_rec,pan_evo_venta[["id_cliente","mes_1_marcaCount","mes_1_marca","mes_2_marca","mes_3_marca","mes_4_marca","mes_5_marca","mes_6_marca","len_m","tipo_cliente"]],how='left',on='id_cliente')
final_rec = pd.merge(final_rec, cods2, how="left", on="id_cliente")
# Agregar descripcion de SKU recomendado
pan_prod = pd.read_csv("Input/MX_maestro_productos.csv")
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
final_rec["peso"] = final_rec.apply(
    lambda row: mapeo_diccionario.get(row["desc_subgiro"], {}).get(row["marca_rec"], 5),
    axis=1,
)

# In[214]:

# Ordenar la recomendacion de acuerdo al peso de la marca y nos quedamos con las 5 primeras marcas
final_rec = (
    final_rec.sort_values(["id_cliente", "peso"]).groupby(["id_cliente"]).head(5)
)
# Agregamos rank de la marca recomendada
final_rec["marca_rec_rank"] = final_rec.groupby("id_cliente").cumcount() + 1
final_rec = final_rec.reset_index(drop=True)

# In[215]:

final_rec["sku"] = final_rec["sku"].astype(int)
final_rec["peso"] = final_rec["peso"].astype(int)
final_rec["marca_rec_rank"] = final_rec["marca_rec_rank"].astype(int)

# ## 5.7 Filtro de Recomendaciones por Segmento de cliente

# In[217]:

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

# In[218]:

final_rec = (
    final_rec.groupby("id_cliente").apply(filtrar_segmento).reset_index(drop=True)
)

# ## 5.8. Guardando recomendacion para D&A y Comercial

# ### 5.8.1 Para D&A

# In[224]:

fecha_tomorrow = (
    datetime.now(pytz.timezone("America/Lima")) + timedelta(days=1)
).strftime("%Y-%m-%d")

# In[225]:

final_rec.to_csv(
    f"Output/PS_piloto_data_v1/D_pan_recs_data_{fecha_tomorrow}.csv", index=False
)

# In[226]:

# Establecer la conexión con S3
bucket_name = 'aje-analytics-ps-backup'  # nombre de bucket en S3
file_name = f'PS_Mexico/Output/PS_piloto_data_v1/D_pan_recs_data_{fecha_tomorrow}.csv'  # nombre para el archivo en S3
s3_path = f's3://{bucket_name}/{file_name}'

# Escribir el dataframe en S3 con AWS Data Wrangler
wr.s3.to_csv(final_rec, s3_path, index=False)

print("*"*10,"DONE PASO 4","*"*10)