# Chinook Analytics ETL

Sistema analítico para la aplicación Chinook ubicada en `/home/harold_burbano/universidad/big_data/music_app`.

## Componentes

- `jobs/glue/dimensions_visual.py`: ETL tipo Glue Studio Visual para `DimCustomer` y `DimTrack`.
- `jobs/glue/fact_sales_visual.py`: ETL tipo Glue Studio Visual para `FactSales` con joins entre `invoice`, `invoice_line` y `customer`.
- `jobs/glue/dim_date_job.py`: ETL Python para `DimDate` usando `holidays`.
- `jobs/glue/full_copy_history.py`: full copy histórico de clientes y jerarquía de empleados.
- `sql/athena_ddl.sql`: tablas externas Athena sobre S3/Parquet.
- `sql/powerbi_views.sql`: vistas listas para Power BI.
- `src/chinook_analytics/deploy.py`: despliegue con `boto3` para S3, Glue Jobs y Athena.
- `infra/cloudformation/chinook-analytics.yml`: infraestructura base en CloudFormation.
- `.env.example`: plantilla de credenciales y configuración local.
- `docs/credentials.md`: guía para conseguir cada credencial.

## Credenciales

```bash
cp .env.example .env
```

Llena `.env` con tus credenciales y parámetros. La guía completa está en [credentials.md](/home/harold_burbano/universidad/big_data/etl_project/docs/credentials.md).

## Despliegue

```bash
pip install -r requirements-dev.txt
pip install -e .
python -m chinook_analytics.deploy --dry-run
python -m chinook_analytics.deploy
```

Antes de ejecutar los jobs crea en Glue una conexión JDBC a PostgreSQL llamada `chinook-postgres` apuntando a la base transaccional de `music_app`.

## Infraestructura con CloudFormation

La plantilla [chinook-analytics.yml](/home/harold_burbano/universidad/big_data/etl_project/infra/cloudformation/chinook-analytics.yml) crea S3, IAM Role, Glue Database, Glue Connection, Glue Tables, Glue Jobs, Glue Triggers y Athena WorkGroup.

```bash
aws cloudformation deploy \
  --stack-name chinook-analytics \
  --template-file infra/cloudformation/chinook-analytics.yml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    AnalyticsBucketName=mi-bucket-chinook-analytics \
    JdbcUrl=jdbc:postgresql://host:5432/chinook \
    JdbcUsername=usuario \
    JdbcPassword=clave \
    GlueSubnetId=subnet-xxxxxxxx \
    GlueSecurityGroupId=sg-xxxxxxxx \
    AvailabilityZone=us-east-1a
```

También puedes desplegar el stack sin escribir parámetros en consola; el comando lee `.env`:

```bash
python -m chinook_analytics.deploy_stack --dry-run
python -m chinook_analytics.deploy_stack
```

Después de crear el stack, sube los scripts Glue al prefijo `jobs/glue/` con `python -m chinook_analytics.deploy` o con `aws s3 cp`. CloudFormation referencia esos archivos desde S3, pero no empaqueta el contenido local automáticamente.

## Orden de jobs

1. `chinook-dim-date-python` con `--START_DATE=2009-01-01` y `--END_DATE=2030-12-31`.
2. `chinook-dimensions-visual`.
3. `chinook-fact-sales-visual`.
4. `chinook-full-copy-history`.

## Análisis soportados

- Número de tracks vendidos por día.
- Artista más vendido por mes.
- Día de la semana que más se compra.
- Mes con mayor número de ventas.

Las consultas base están en `src/chinook_analytics/athena_queries.py` y las vistas para presentación en `sql/powerbi_views.sql`.

## Pruebas

```bash
pytest -q
```

El CI de `.github/workflows/ci.yml` instala dependencias, ejecuta pruebas unitarias y valida la configuración del despliegue.

## Despliegue continuo

`.github/workflows/deploy.yml` despliega al hacer push a `main` o manualmente desde `workflow_dispatch`. El workflow crea un `.env` temporal con secretos/variables de GitHub y ejecuta el mismo código local.

Configura los valores listados en [credentials.md](/home/harold_burbano/universidad/big_data/etl_project/docs/credentials.md).

El despliegue crea triggers Glue para actualizar dimensiones cada hora, hechos cada 15 minutos e históricos diariamente.
