# Power BI con AWS Athena

## Conexión

1. Instala el driver ODBC de Amazon Athena.
2. En Power BI Desktop selecciona `Get Data > ODBC`.
3. Usa el DSN configurado con:
   - `AwsRegion`
   - `S3OutputLocation`, por ejemplo `s3://<bucket>/athena/results/`
   - credenciales IAM con permisos de Athena, Glue Data Catalog y S3.
4. Selecciona la base `chinook_analytics`.

## Vistas sugeridas

Ejecuta `sql/powerbi_views.sql` en Athena y carga estas vistas:

- `v_tracks_sold_by_day`
- `v_top_artist_by_month`
- `v_best_day_of_week`
- `v_best_sales_month`

## Reportes

- Número de canciones vendidas por día: gráfico de línea con `FullDate` y `TracksSold`.
- Artista más vendido por mes: tabla o matriz con `Year`, `Month`, `Artist`, `TracksSold`.
- Día de la semana con más compras: barra descendente usando `DayOfWeek` y `TracksSold`.
- Mes con mayor número de ventas: barra por `Year`, `Month`, `TracksSold`, con tarjeta para el máximo.

## Refresh

Configura el gateway o credenciales cloud de Power BI Service y agenda refresh después de los triggers Glue. Si Athena usa particiones nuevas, ejecuta `MSCK REPAIR TABLE` o habilita actualización de particiones desde el despliegue.
