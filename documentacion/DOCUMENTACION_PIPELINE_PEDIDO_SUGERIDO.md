# Documentación - Pipeline de Pedido Sugerido

## 1. Visión General

### Objetivo
El Pipeline de Pedido Sugerido genera recomendaciones diarias de productos para clientes de AJE en múltiples países. Utiliza un modelo de filtrado colaborativo (ALS) entrenado con datos históricos de ventas para recomendar SKUs que cada cliente debería comprar en su próxima visita.

Las recomendaciones se procesan hoy para los clientes que serán visitados mañana. El resultado final es un archivo CSV consolidado que se sube a un bucket S3, desde donde un Step Function (proyecto externo) lo consume para cargar los pedidos en Salesforce.

### Automatización
Anteriormente el proceso se ejecutaba manualmente desde notebooks de SageMaker. Actualmente se implementó como **SageMaker Pipeline Jobs** que se ejecutan automáticamente mediante un scheduler. Los scripts se versionan en GitHub y se almacenan en S3 para su ejecución.

- **Repositorio**: https://github.com/dpmajegroup/proyecto-sagemaker-spark.git
- **Región AWS**: `us-east-2`

### Países activos
El pipeline opera para 7 países con la misma arquitectura:

| País | Código | Prefijo id_cliente | Estado |
|------|--------|-------------------|--------|
| Perú | PE | `PE\|{compania}\|{cliente}` | Activo |
| Ecuador | EC | `EC\|{compania}\|{cliente}` | Activo |
| Costa Rica | CR | `CAM\|{compania}\|{cliente}` | Activo |
| Guatemala | GT | `CAM\|{compania}\|{cliente}` | Activo |
| Nicaragua | NI | `CAM\|{compania}\|{cliente}` | Activo |
| Panamá | PA | `CAM\|{compania}\|{cliente}` | Activo |
| México | MX | `MX\|{compania}\|{cliente}` | Desactivado (desde abril 2026) |

> **Nota**: México sigue teniendo su pipeline funcional, pero fue removido del consolidado final que se sube a Salesforce.

Adicionalmente, Ecuador tiene pedidos complementarios:
- **Pedido Estratégico (PE)**: productos fijos cruzados con clientes de sucursales/rutas específicas.
- **Pedido Recurrente (PR)**: pipeline independiente (documentación pendiente).

---

## 2. Arquitectura

### Estructura por país
Cada país tiene 3 scripts de Python y 1 notebook orquestador:

```
PS_{Pais}/
├── TEST_PS_{COD}_1_limpieza.py          # Paso 1: Limpieza y preparación de datos
├── TEST_PS_{COD}_2_modelado.py          # Paso 2: Modelo ALS con PySpark
├── TEST_PS_{COD}_3_reglas_negocio.py    # Paso 3: Reglas de negocio y exportación
└── TEST_PS_{COD}_4_orquestador_pipeline.ipynb  # Orquestador SageMaker Pipeline
```

### Reporting
```
Reporting/
├── TEST_RPT_1_reporte_todos_paises.py           # Consolida países y envía correo
├── TEST_RPT_2_orquestador_pipeline.ipynb        # Orquestador del reporte
├── TEST_RPT_3_validacion_errores.py             # Lee errores del lambda y envía correo
└── TEST_RPT_4_orquestador_validacion.ipynb      # Orquestador de validación
```

### Ecuador Estratégico
```
PEs_Ecuador/
├── TEST_PE_EC_1_estrategico.py                  # Genera pedido estratégico
└── TEST_PE_EC_2_orquestador_pipeline.ipynb      # Orquestador
```

### Almacenamiento de scripts
Los scripts se guardan en S3 bajo la ruta `s3://{default_bucket}/scripts/{COD_PAIS}/` donde `default_bucket` es el bucket por defecto de SageMaker (`sagemaker-us-east-2-{account_id}`). Desde ahí, el pipeline los referencia para ejecutarlos.

### Contenedores
Cada paso del pipeline corre en un contenedor independiente dentro de SageMaker:

- **Paso 1 (Limpieza)** y **Paso 3 (Reglas de Negocio)**: usan la imagen `sklearn 1.2-1` de SageMaker (`ScriptProcessor`). Instalan dependencias adicionales al inicio del script (`awswrangler`, `pyarrow`, etc.).
- **Paso 2 (Modelado)**: usa `PySparkProcessor` con `framework_version="3.3"`, que ya trae configurado Apache Spark y las librerías de MLlib (incluyendo ALS). Instancia tipo `ml.m5.4xlarge`.

### Encadenamiento de pasos
El orquestador define un `Pipeline` de SageMaker con 3 `ProcessingStep` encadenados:

1. **Paso 1** genera outputs en `/opt/ml/processing/output/limpieza`
2. **Paso 2** recibe como input el output del Paso 1 (archivos por ruta) y genera outputs en `/opt/ml/processing/output/modelado`
3. **Paso 3** recibe como inputs tanto el output del Paso 1 (ventas, maestro, diccionario) como el output del Paso 2 (recomendaciones del modelo)

---

## 3. Inputs (Datos de Entrada)

Los datos de entrada son proporcionados por el equipo de ingeniería de datos y se actualizan automáticamente de forma diaria en formato CSV. Siempre contienen los últimos 12 meses de historia.

### Ventas
- **Ubicación**: `s3://aje-prd-analytics-artifacts-s3/pedido_sugerido/data-v1/{pais}/ventas_{pais}000` (puede haber `001`, `002` si el archivo es grande)
- **Separador**: `;`
- **Contenido**: transacciones de venta con campos como `id_cliente`, `cod_ruta`, `cod_articulo_magic`, `fecha_liquidacion`, `cant_cajafisicavta`, `imp_netovta`, `cod_compania`, `cod_sucursal`, `desc_categoria`, `desc_giro`, `desc_subgiro`, entre otros.

### Visitas
- **Ubicación**: `s3://aje-prd-analytics-artifacts-s3/pedido_sugerido/data-v1/{pais}/visitas_{pais}000`
- **Separador**: `;`
- **Contenido**: programación de visitas con `codigo_cliente__c`, `dias_de_visita__c` (días separados por `;`), `cod_ruta`, `cod_modulo`, `ultima_visita`, `eje_potencial__c` (segmento).
- **Campos clave que vienen de visitas**: `cod_ruta`, `cod_modulo`, `dias_de_visita__c`, `periodo_de_visita__c`, `ultima_visita`.

### Stock
- **Ubicación**: varía por país (ej: `s3://aje-prd-analytics-artifacts-s3/pedido_sugerido/data-v1/peru/D_stock_pe.csv`)
- **Contenido**: stock actual por compañía, sucursal y SKU.

### Maestro de productos
- Se obtiene de Redshift (Perú) o de archivos CSV en S3 según el país.
- Contiene `cod_articulo_magic` y `desc_articulo`.

---

## 4. Pipeline por País (Perú como referencia)

### Paso 1: Limpieza (`TEST_PS_PE_1_limpieza.py`)

1. **Validación de inputs**: verifica que los archivos en S3 existan, no estén vacíos y hayan sido modificados hoy.
2. **Descarga de maestro de productos** desde Redshift.
3. **Descarga de visitas**: filtra por rutas del país y canal 2.
4. **Descarga de ventas**: lee los archivos de ventas, filtra por rutas del país.
5. **Filtros específicos de Perú**:
   - Excluye marca TRES CRUCES de sucursales fuera de Lima.
   - Excluye combinaciones específicas de ruta + SKU en Lima Centro.
   - Filtra solo SKUs de un Excel de marcas/SKUs a usar.
6. **Construcción de `id_cliente`**: formato `PE|{compania_4dig}|{cod_cliente}`.
7. **Cruce ventas × visitas**: inner join por `id_cliente`. Los campos `cod_ruta` y `cod_modulo` se toman de **visitas** (con fallback a ventas).
8. **Deduplicación de visitas**: cuando un cliente tiene múltiples filas en visitas, se prioriza la fila que contenga el día de mañana en `dias_de_visita__c`. Si ninguna lo tiene, se queda con la de `ultima_visita` más reciente.
9. **Filtro de visitas para mañana**: solo se conservan clientes cuyo `dias_de_visita__c` contenga el día de mañana (1=lunes, 7=domingo).
10. **Cálculo de pesos por giro**: genera un diccionario `mapeo_diccionario.json` con rankings de categorías por subgiro basado en volumen de ventas.
11. **División por rutas**: genera un CSV por cada ruta con sus ventas. Rutas con menos de 5 SKUs únicos se agrupan en `D_low_ruta_ventas.csv`.
12. **Output**: `peru_ventas_manana.parquet` + archivos por ruta + `mapeo_diccionario.json` + maestro de productos.

### Paso 2: Modelado ALS (`TEST_PS_PE_2_modelado.py`)

1. **Crea sesión Spark** dentro del contenedor PySpark 3.3.
2. **Para cada archivo de ruta** (CSV generado en el Paso 1):
   - Agrupa ventas por `id_cliente` + `cod_articulo_magic` y cuenta fechas distintas como `frecuencia` (preferencia implícita).
   - Aplica `StringIndexer` para convertir `clienteId` a índice numérico.
   - Entrena un modelo **ALS implícito** (`implicitPrefs=True`, `rank=10`, `maxIter=5`).
   - Genera recomendaciones para todos los usuarios con `recommendForAllUsers(sku_len)`.
   - Convierte las recomendaciones a formato tabular `(id_cliente, cod_articulo_magic)`.
3. **Las recomendaciones son a nivel ruta**: cada modelo ALS se entrena con los datos de una ruta específica, por lo que las recomendaciones reflejan los patrones de compra de esa ruta.
4. **Consolida** todas las recomendaciones de todas las rutas en un solo `D_rutas_rec.parquet`.
5. **Fallback**: si no se encuentran archivos de rutas o no se generan recomendaciones, escribe un parquet vacío para que el pipeline no falle.

### Paso 3: Reglas de Negocio (`TEST_PS_PE_3_reglas_negocio.py`)

Recibe como inputs el parquet de ventas del Paso 1 y las recomendaciones del Paso 2. Aplica filtros secuenciales para refinar las recomendaciones.

#### Orden de reglas de negocio (Perú)

| # | Regla | Descripción | ¿Activa? | Notas por país |
|---|-------|-------------|----------|----------------|
| 5.-9 | Ventas últimos 14 días | Solo recomienda SKUs que se hayan vendido en la ruta del cliente en los últimos 14 días. | ✅ Todos | — |
| 5.-8 | Subida / Mantener / Bajada | Prioriza SKUs con tendencia de ventas al alza (compara últimos 30 días vs 31-60 días). Ordena: Subida primero, luego Mantener, luego Bajada. | ✅ Todos | — |
| 5.-7 | Maestro de validación | Cruza con archivo maestro de productos por compañía para asegurar que el SKU esté habilitado para ese cliente. | ✅ Todos excepto Ecuador Econoredes | Ecuador Econoredes no tiene este filtro. |
| — | Filtro SKU 608597 | Excluye el SKU 608597 de rutas específicas. | Solo Perú | Rutas: 14608, 14450, 14471, 12967, 12968, 12958, 12972. |
| 5.-5 | Stock | Calcula días de stock (stock / promedio diario de venta últimos 12 días). Solo recomienda SKUs con más de 3 días de stock. | ✅ Todos | — |
| 5.-4 | Excel LISTA SKUS | Quita SKUs de un Excel específico. | ❌ Comentado en Perú | No se ejecutaba en el notebook original. |
| 5.-3 | SKUs sin precio | Excluye una lista fija de SKUs que no tienen precio asignado. | ✅ Todos | La lista varía por país. |
| 5.-2 | Despriorizar histórico | Lee los backups de recomendaciones de los últimos 14 días desde S3. Las recomendaciones nuevas (no enviadas antes) van primero; las que ya se enviaron van al final. **No elimina**, solo reordena. Excluye el archivo de mañana para evitar auto-referencia en re-ejecuciones. | ✅ Todos | — |
| 5.3 | Compras recientes | Elimina SKUs que el cliente ya compró en las últimas 2 semanas. | ✅ Todos | En Nicaragua el orden es diferente (se aplica antes del filtro S/M/B). |
| — | Quitar Recurrente | Excluye pares (cliente, SKU) que ya están en el Pedido Recurrente de Ecuador. | Solo Ecuador PS | — |
| 5.3 SKU Excel | Filtro SKUs por Excel compañía-sucursal | Cruza con un Excel `PS_Carga_SKU_{fecha}.xlsx` que define qué SKUs están disponibles por compañía y sucursal. | Solo Nicaragua | — |

#### Después de los filtros

1. **Asociación con categorías**: cruza recomendaciones con `desc_categoria` de ventas.
2. **Cálculo de irregularidad**: clasifica clientes como Regular/Irregular según cuántos meses compraron en los últimos 6 meses (< 4 meses = Irregular).
3. **Pesos por giro**: asigna un peso a cada recomendación según el diccionario de pesos por subgiro (generado en Paso 1). Ordena por peso y toma las top 5 por cliente.
4. **Filtro por segmento**: limita la cantidad de recomendaciones según el segmento del cliente:
   - BLINDAR: 1 recomendación
   - MANTENER: 2 recomendaciones
   - DESARROLLAR: 3 recomendaciones
   - OPTIMIZAR: 4 recomendaciones
   - Sin segmento: hasta 5 recomendaciones

   > **Panamá** tiene reglas de segmento diferentes para rutas especiales (11103, 11205, 11301, 11401).

#### Exportación

El Paso 3 genera 2 archivos y los sube a S3:

1. **Backup D&A** (data completa con métricas): `s3://aje-analytics-ps-backup/PS_{Pais}/Output/PS_data_piloto_v1/D_pan_recs_data_{fecha}.csv`
2. **Backup Salesforce** (formato de 12 columnas): `s3://aje-analytics-ps-backup/PS_{Pais}/Output/PS_piloto_v1/D_base_pedidos_{fecha}.csv`

Las 12 columnas del formato final:

| Columna | Descripción |
|---------|-------------|
| Pais | Código de país (PE, EC, CR, GT, NI, PA, MX) |
| Compania | Código de compañía (4 dígitos) |
| Sucursal | Código de sucursal (2 dígitos) |
| Cliente | Código de cliente (entero) |
| Modulo | Código de módulo (entero) |
| Producto | Código de SKU (`cod_articulo_magic`, entero) |
| Cajas | Siempre 1 |
| Unidades | Siempre 0 |
| Fecha | Fecha de mañana (YYYY-MM-DD) |
| tipoRecomendacion | PS1, PS2, PS3... (o PE1, PE2 para Estratégico, PR1 para Recurrente) |
| ultFecha | Vacío (reservado) |
| Destacar | Siempre "true" |

---

## 5. Reporting y Consolidación

### Reporte de subida (`TEST_RPT_1_reporte_todos_paises.py`)

1. **Lee los backups** de cada país desde `aje-analytics-ps-backup`, incluyendo Ecuador Recurrente, Estratégico y Econoredes.
2. **Concatena** todos los DataFrames en uno solo, estandarizando formatos (Compania 4 dígitos, Sucursal 2 dígitos, 12 columnas).
3. **Guarda el consolidado** en:
   - `s3://aje-analytics-ps-backup/Output/0_Final_PS/base_pedidos_final_{fecha}.csv` (backup)
   - `s3://aje-prd-pedido-sugerido-orders-s3/PE/pedidos/base_pedidos.csv` (bucket de orders, consumido por el Step Function)
4. **Genera métricas**:
   - Resumen por País: clientes, recomendaciones, SKUs únicos, promedio productos/cliente.
   - Detalle por País/Compañía/Sucursal.
   - Desglose por tipo de recomendación (PR/PS/PE).
5. **Envía correo HTML** con las 3 tablas a los destinatarios configurados.

> **Nota**: la ruta `aje-prd-pedido-sugerido-orders-s3/PE/pedidos/` usa el prefijo "PE" para todos los países por razones de legado.

### Validación de errores (`TEST_RPT_3_validacion_errores.py`)

Luego de que el consolidado se sube al bucket de orders, un **Step Function** (proyecto externo) lo procesa y carga los pedidos en Salesforce. Este proceso demora aproximadamente **70 minutos**.

Al finalizar, el Step Function genera un archivo zip con los errores/rechazos en `s3://aje-prd-pedido-sugerido-orders-s3/PE/errores/consulta/`. El script de validación:

1. **Busca el zip más reciente** modificado hoy en la ruta de errores.
2. **Extrae el CSV** (separado por `;`) con columnas: Pais, Compania, Sucursal, Fecha, Modulo, Cliente, Producto, Tipo, Mensaje.
3. **Lee el consolidado subido** para calcular porcentajes de rechazo a nivel cliente.
4. **Genera 3 tablas**:
   - Resumen por País: clientes rechazados, total rechazos, clientes subidos, % de rechazo.
   - Detalle por País/Compañía/Sucursal con % de rechazo.
   - Desglose por Tipo de error y Mensaje.
5. **Envía correo HTML** con el resumen de errores.

---

## 6. Flujo de Datos en S3

### Buckets involucrados

| Bucket | Propósito |
|--------|-----------|
| `aje-prd-analytics-artifacts-s3` | Inputs: ventas, visitas, stock, maestro de productos. Actualizados diariamente por ingeniería de datos. |
| `sagemaker-us-east-2-{account_id}` | Scripts de los pipelines (`scripts/{COD_PAIS}/`). Outputs intermedios de SageMaker (limpieza, modelado). |
| `aje-analytics-ps-backup` | Backups diarios de recomendaciones por país, datos D&A, consolidado final. |
| `aje-prd-pedido-sugerido-orders-s3` | Archivo final `base_pedidos.csv` consumido por el Step Function. Errores del lambda en `PE/errores/consulta/`. |
| `aje-dl-prod-us-east-2-399723489351-external-data` | Excel de SKUs disponibles por compañía-sucursal (solo Nicaragua). |

### Archivos que se guardan por ejecución

**Por cada país (Paso 3)**:
- `s3://aje-analytics-ps-backup/PS_{Pais}/Output/PS_piloto_v1/D_base_pedidos_{fecha}.csv` — Backup formato Salesforce (12 columnas)
- `s3://aje-analytics-ps-backup/PS_{Pais}/Output/PS_data_piloto_v1/D_pan_recs_data_{fecha}.csv` — Backup D&A (data completa)

**Perú adicionalmente**:
- `s3://aje-analytics-ps-backup/PS_Peru/Input/visitas_peru000_{fecha}.csv` — Backup de visitas del día

**Reporting**:
- `s3://aje-analytics-ps-backup/Output/0_Final_PS/base_pedidos_final_{fecha}.csv` — Consolidado de todos los países
- `s3://aje-prd-pedido-sugerido-orders-s3/PE/pedidos/base_pedidos.csv` — Archivo final para Salesforce (se sobreescribe cada día)

**Ecuador Estratégico**:
- `s3://aje-analytics-ps-backup/Pedido_Estrategico/Ecuador/Output/estr_base_pedidos_{fecha}.csv`

---

## 7. Consideraciones Técnicas

### Unicidad de cliente
Un cliente se identifica de forma única por la combinación `Pais + Compania + Sucursal + Cliente`. El `cod_cliente` puede repetirse entre compañías.

### Formatos obligatorios
- **Compañía**: siempre 4 dígitos (ej: `0090`, `0030`). Soporta valores alfanuméricos (ej: `D004`, `E001` en Econoredes).
- **Sucursal**: siempre 2 dígitos (ej: `07`, `25`).
- **cod_articulo_magic** y **cod_cliente**: siempre enteros sin `.0`.

### Zona horaria
Todos los scripts usan `America/Lima` como zona horaria de referencia. Las recomendaciones generadas hoy son para clientes que serán visitados mañana.

### Lectura de archivos S3
Todas las lecturas de CSV desde S3 usan `awswrangler.s3.read_csv` o `boto3.download_file` + lectura local. Nunca se usa `pd.read_csv` con rutas S3 directamente (incompatible con el entorno de SageMaker).
