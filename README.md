# Pipeline de Pedido Sugerido - AJE Group

Sistema automatizado de recomendaciones diarias de productos para clientes de AJE en Latinoamérica. Usa modelos de filtrado colaborativo (ALS) entrenados con historial de ventas, ejecutados como SageMaker Pipeline Jobs.

![Infografía del Pipeline](documentacion/infografia_pipeline_pedido_sugerido.png)

## Estructura del repositorio

| Carpeta | Descripción |
|---------|-------------|
| `PS_Peru/` | Pipeline Pedido Sugerido - Perú (3 scripts + orquestador) |
| `PS_Ecuador/` | Pipeline Pedido Sugerido - Ecuador |
| `PS_CostaRica/` | Pipeline Pedido Sugerido - Costa Rica |
| `PS_Guatemala/` | Pipeline Pedido Sugerido - Guatemala |
| `PS_Nicaragua/` | Pipeline Pedido Sugerido - Nicaragua |
| `PS_Panama/` | Pipeline Pedido Sugerido - Panamá |
| `PS_Mexico/` | Pipeline Pedido Sugerido - México (desactivado del consolidado) |
| `PS_Econoredes_Ecuador/` | Pipeline Pedido Sugerido - Ecuador canal Econoredes |
| `PEs_Ecuador/` | Pipeline Pedido Estratégico - Ecuador |
| `Reporting/` | Consolidación de países, reporte por correo y validación de errores |
| `documentacion/` | Documentación técnica, resumen para infografía y prompts |

## Cada carpeta de país contiene

- `TEST_PS_{COD}_1_limpieza.py` — Descarga y cruza ventas con visitas, filtra clientes de mañana.
- `TEST_PS_{COD}_2_modelado.py` — Entrena modelo ALS con PySpark 3.3 por ruta.
- `TEST_PS_{COD}_3_reglas_negocio.py` — Aplica filtros de negocio y exporta recomendaciones.
- `TEST_PS_{COD}_4_orquestador_pipeline.ipynb` — Define y registra el SageMaker Pipeline.

## Flujo general

1. Ingeniería de datos actualiza ventas, visitas y stock en S3 (diario).
2. El scheduler ejecuta el pipeline de cada país (Limpieza → ALS → Reglas de Negocio).
3. El pipeline de reporting consolida todos los países y sube el archivo final.
4. Un Step Function externo carga los pedidos en Salesforce (~70 min).
5. Se validan los errores/rechazos y se envía un segundo reporte.

## Documentación

Ver [`documentacion/DOCUMENTACION_PIPELINE_PEDIDO_SUGERIDO.md`](documentacion/DOCUMENTACION_PIPELINE_PEDIDO_SUGERIDO.md) para la documentación técnica completa.
