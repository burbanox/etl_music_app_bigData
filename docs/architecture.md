# Chinook Analytics Architecture

## Objetivo

Construir un sistema analítico desnormalizado para Chinook con S3 como almacenamiento, Glue como ETL, Athena como motor SQL y Power BI como capa de presentación.

## Flujo

1. La aplicación `music_app` escribe ventas en PostgreSQL sobre `invoice` e `invoice_line`.
2. AWS Glue lee PostgreSQL mediante una conexión JDBC llamada `chinook-postgres`.
3. Los jobs visuales construyen `DimCustomer`, `DimTrack` y `FactSales` en Parquet.
4. El job Python construye `DimDate` usando el paquete `holidays`.
5. Athena expone tablas externas y vistas para Power BI.
6. GitHub Actions ejecuta pruebas unitarias antes de desplegar scripts/jobs.

## Modelo dimensional

`FactSales`

- `CustomerKey`
- `TrackKey`
- `InvoiceDateKey` en formato `yyyymmdd`
- `EmployeeKey`
- `Quantity`
- `UnitPrice`
- `TotalAmount`

`DimCustomer`

- `CustomerKey`
- `FirstName`
- `LastName`
- `Company`
- `Country`
- `City`
- `State`
- `Email`

`DimTrack`

- `TrackKey`
- `Name`
- `Album`
- `Artist`
- `Genre`
- `MediaType`
- `Composer`
- `Milliseconds`

`DimDate`

- `DateKey`
- `FullDate`
- `Year`
- `Quarter`
- `Month`
- `Day`
- `DayOfWeek`
- `IsHoliday`

## Particionamiento

`FactSales` y `DimDate` se escriben en S3 con particiones:

- `partition_year`
- `partition_month`
- `partition_day`

Se usan nombres `partition_*` porque Athena no permite tener columnas `Year/Month/Day` y particiones homónimas al normalizar mayúsculas y minúsculas.

## Actualización continua

Para mantener el analítico actualizado, programa los jobs Glue con triggers cada pocos minutos o por evento después de una compra. `FactSales` reescribe particiones por fecha; las dimensiones se pueden ejecutar en modo full refresh porque Chinook es pequeño. El job `full_copy_history.py` conserva snapshots de clientes y de la jerarquía `employee.reports_to`.
