# temp_lambdas — material TEMPORAL de validacion

NO forma parte del codigo del proyecto. Esta carpeta se usa solo para
diagnostico/validacion puntual y puede eliminarse en cualquier momento.

Contenido:
- aje-prd-pedido-sugerido-file-reader-fn / order-request-fn / notification-fn:
  copias de los lambdas de Salesforce (hechos por un tercero) usadas solo como
  referencia para investigar el error DUPLICATE_VALUE en la carga de pedidos.
- base_pedidos_final_*.csv: consolidado de ejemplo subido por Reporting (muestra).
- error_base_pedidos_*.csv: log de errores devuelto por el lambda (muestra).

Estos archivos no se ejecutan desde el pipeline ni son mantenidos como parte
del proyecto. Material de apoyo temporal.
