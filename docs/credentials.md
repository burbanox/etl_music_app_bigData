# Credenciales y Variables

Guarda tus valores reales en `.env`. No subas `.env` a Git; ya está ignorado en `.gitignore`.

## Archivo local

```bash
cp .env.example .env
```

Luego edita `.env` y llena los valores.

## AWS

`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`

- Dónde conseguirlas: AWS Academy Learner Lab, IAM Identity Center, o `IAM > Users > Security credentials > Create access key`.
- `AWS_SESSION_TOKEN` solo es obligatorio si usas credenciales temporales, muy común en AWS Academy.
- Dónde colocarlas: `.env`.

`AWS_REGION`

- Dónde conseguirla: la región donde trabajarás, por ejemplo `us-east-1`.
- Dónde colocarla: `.env`.

## S3 y Athena

`ANALYTICS_BUCKET`

- Qué es: bucket único global para scripts Glue, datos Parquet y resultados de Athena.
- Cómo conseguirlo: elige un nombre único, por ejemplo `chinook-analytics-harold-2026`.
- Dónde colocarlo: `.env`.

`ATHENA_DATABASE`, `ATHENA_RESULTS_PREFIX`, `CURATED_PREFIX`, `SCRIPTS_PREFIX`

- Qué son: nombres lógicos de base y prefijos dentro del bucket.
- Puedes usar los valores de `.env.example`.

## Glue

`GLUE_ROLE_ARN`

- Qué es: ARN del rol IAM que Glue usa para leer PostgreSQL y escribir en S3.
- Cómo conseguirlo: en AWS Academy Learner Lab normalmente debes usar un rol existente, casi siempre `LabRole`.
- Por consola: ve a `IAM > Roles > LabRole` y copia el ARN.
- Por CLI: `aws iam get-role --role-name LabRole --query 'Role.Arn' --output text`.
- Dónde colocarlo: `.env`.

`GLUE_JDBC_CONNECTION_NAME`

- Qué es: nombre de la conexión JDBC en Glue.
- Puedes usar `chinook-postgres`.

`GLUE_SUBNET_ID`, `GLUE_SECURITY_GROUP_ID`, `GLUE_VPC_ID`, `GLUE_ROUTE_TABLE_ID`, `GLUE_AVAILABILITY_ZONE`

- Qué son: red desde la que Glue podrá llegar a PostgreSQL.
- Cómo conseguirlos: en `VPC > Subnets`, `VPC > Route tables` y `EC2 > Security Groups`; la zona aparece en la subnet.
- El security group debe permitir salida hacia PostgreSQL y el SG/base de datos debe permitir entrada al puerto `5432`.
- `GLUE_ROUTE_TABLE_ID` se usa para crear un Gateway VPC Endpoint de S3. Glue lo necesita para leer scripts y escribir Parquet en S3 cuando corre dentro de una VPC.

## PostgreSQL Chinook

`JDBC_URL`

- Formato: `jdbc:postgresql://<host>:5432/<database>`.
- Cómo conseguirlo:
  - Si PostgreSQL está en RDS: endpoint en `RDS > Databases > Connectivity`.
  - Si está en EC2: DNS público/privado o IP privada de la instancia.
  - Si está en Docker local, Glue no podrá entrar a `localhost`; necesitas una base accesible desde AWS.

`JDBC_USERNAME`, `JDBC_PASSWORD`

- Son el usuario y contraseña de la base transaccional de `music_app`.
- Revisa el `.env`, `docker-compose` o configuración de PostgreSQL de la aplicación.

## Power BI

Power BI no lee este `.env` directamente. Para conectarte a Athena necesitas:

- AWS Region.
- S3 output location: `s3://<ANALYTICS_BUCKET>/<ATHENA_RESULTS_PREFIX>/`.
- Credenciales AWS con permisos sobre Athena, Glue Data Catalog y S3.
- Driver ODBC de Amazon Athena instalado.

## GitHub Actions

En GitHub no subas `.env`. Guarda estos valores como `Secrets` o `Variables`:

- Secrets: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, opcional `AWS_SESSION_TOKEN`, `JDBC_USERNAME`, `JDBC_PASSWORD`.
- Variables: `AWS_REGION`, `ANALYTICS_BUCKET`, `GLUE_ROLE_ARN`, `GLUE_JDBC_CONNECTION_NAME`, `ATHENA_DATABASE`, `PROJECT_NAME`, `CFN_STACK_NAME`, `JDBC_URL`, `GLUE_SUBNET_ID`, `GLUE_SECURITY_GROUP_ID`, `GLUE_AVAILABILITY_ZONE`.
