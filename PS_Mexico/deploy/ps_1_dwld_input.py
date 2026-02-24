# # 1 Descargar Ventas y Visitas (aje-prd-analytics-artifacts-s3)
# In[2]:
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


# In[4]:


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


# In[5]:


comprobar_inputs()


# ## 1.0 Descargar Maestro de productos

# In[6]:


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



# In[8]:


maestro_prod[["cod_articulo_magic", "desc_articulo"]].drop_duplicates().reset_index(
    drop=True
).to_csv("Input/MX_maestro_productos.csv", index=False)


# In[9]:

rutas_test = [1155,1158,1074,1065]
rutas_test

# In[11]:

clientes_ruta_test = []

# In[12]:

def descargar_visitas():
    # Guardar clientes de las rutas
    global clientes_ruta_test

    # Conectarse a S3
    s3 = boto3.client("s3")
    bucket_name = "aje-prd-analytics-artifacts-s3"
    prefix = "pedido_sugerido/data-v1/mexico/"

    # Listar objetos en la ruta de S3
    objetos = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

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
                df.to_parquet(nombre_csv,index=False)
            else:
                continue


# In[13]:

descargar_visitas()
print("VISITAS DESCARGADAS")

# In[14]:

def descargar_ventas():
    # Conectarse a S3
    s3 = boto3.client("s3")
    bucket_name = "aje-prd-analytics-artifacts-s3"
    prefix = "pedido_sugerido/data-v1/mexico/"

    # Listar objetos en la ruta de S3
    objetos = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

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


# In[15]:

descargar_ventas()
print("VENTAS DESCARGADAS")

# In[16]:

def descargarStockDiario():
    # Obtener la fecha y año actual
    current_date = datetime.now(pytz.timezone("America/Lima"))

    # Formatear la fecha en el formato "YYYYMM"
    formatted_date = current_date.strftime("%Y%m")
    # PARA 2024
    query = f"""select * from 
        cadena_prod.fact_probabilidad_quiebre_stock
        where id_pais='MX' and id_periodo='{formatted_date}';
        """

    con = wr.data_api.redshift.connect(
        cluster_id="dwh-cloud-storage-salesforce-prod",
        database="dwh_prod",
        db_user="dwhuser",
    )
    df_quiebres = wr.data_api.rds.read_sql_query(query, con)
    return df_quiebres

# In[17]:

df_quiebres = descargarStockDiario()

print("*"*10,"DONE PASO 1","*"*10)
