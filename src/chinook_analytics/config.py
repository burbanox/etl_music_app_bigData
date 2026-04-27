from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_PATH = ROOT / ".env"


def load_dotenv(path: Path = DEFAULT_ENV_PATH) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


def required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}. Add it to .env")
    return value


@dataclass(frozen=True)
class AnalyticsConfig:
    bucket: str
    database: str
    raw_prefix: str
    curated_prefix: str
    scripts_prefix: str
    athena_results_prefix: str
    glue_role_arn: str
    jdbc_connection_name: str
    region: str
    dim_date_start_date: str
    dim_date_end_date: str

    @property
    def curated_uri(self) -> str:
        return f"s3://{self.bucket}/{self.curated_prefix.rstrip('/')}"

    @property
    def scripts_uri(self) -> str:
        return f"s3://{self.bucket}/{self.scripts_prefix.rstrip('/')}"


def load_config() -> AnalyticsConfig:
    load_dotenv()
    return AnalyticsConfig(
        bucket=required_env("ANALYTICS_BUCKET"),
        database=os.getenv("ATHENA_DATABASE", "chinook_analytics"),
        raw_prefix=os.getenv("RAW_PREFIX", "raw/chinook"),
        curated_prefix=os.getenv("CURATED_PREFIX", "curated/chinook"),
        scripts_prefix=os.getenv("SCRIPTS_PREFIX", "jobs/glue"),
        athena_results_prefix=os.getenv("ATHENA_RESULTS_PREFIX", "athena/results"),
        glue_role_arn=required_env("GLUE_ROLE_ARN"),
        jdbc_connection_name=os.getenv("GLUE_JDBC_CONNECTION_NAME", "chinook-postgres"),
        region=os.getenv("AWS_REGION", "us-east-1"),
        dim_date_start_date=os.getenv("DIM_DATE_START_DATE", "2009-01-01"),
        dim_date_end_date=os.getenv("DIM_DATE_END_DATE", "2030-12-31"),
    )


@dataclass(frozen=True)
class CloudFormationConfig:
    stack_name: str
    project_name: str
    analytics_bucket: str
    athena_database: str
    curated_prefix: str
    scripts_prefix: str
    athena_results_prefix: str
    glue_role_arn: str
    jdbc_connection_name: str
    jdbc_url: str
    jdbc_username: str
    jdbc_password: str
    glue_subnet_id: str
    glue_security_group_id: str
    glue_vpc_id: str
    glue_route_table_id: str
    availability_zone: str
    dim_date_start_date: str
    dim_date_end_date: str
    region: str

    def parameter_overrides(self) -> list[dict[str, str]]:
        values = {
            "ProjectName": self.project_name,
            "AnalyticsBucketName": self.analytics_bucket,
            "AthenaDatabaseName": self.athena_database,
            "CuratedPrefix": self.curated_prefix,
            "ScriptsPrefix": self.scripts_prefix,
            "AthenaResultsPrefix": self.athena_results_prefix,
            "GlueRoleArn": self.glue_role_arn,
            "JdbcConnectionName": self.jdbc_connection_name,
            "JdbcUrl": self.jdbc_url,
            "JdbcUsername": self.jdbc_username,
            "JdbcPassword": self.jdbc_password,
            "GlueSubnetId": self.glue_subnet_id,
            "GlueSecurityGroupId": self.glue_security_group_id,
            "GlueVpcId": self.glue_vpc_id,
            "GlueRouteTableId": self.glue_route_table_id,
            "AvailabilityZone": self.availability_zone,
            "DimDateStartDate": self.dim_date_start_date,
            "DimDateEndDate": self.dim_date_end_date,
        }
        return [{"ParameterKey": key, "ParameterValue": value} for key, value in values.items()]

    def safe_dict(self) -> dict[str, str]:
        return {
            "stack_name": self.stack_name,
            "project_name": self.project_name,
            "analytics_bucket": self.analytics_bucket,
            "athena_database": self.athena_database,
            "curated_prefix": self.curated_prefix,
            "scripts_prefix": self.scripts_prefix,
            "athena_results_prefix": self.athena_results_prefix,
            "glue_role_arn": self.glue_role_arn,
            "jdbc_connection_name": self.jdbc_connection_name,
            "jdbc_url": self.jdbc_url,
            "jdbc_username": self.jdbc_username,
            "jdbc_password": "***",
            "glue_subnet_id": self.glue_subnet_id,
            "glue_security_group_id": self.glue_security_group_id,
            "glue_vpc_id": self.glue_vpc_id,
            "glue_route_table_id": self.glue_route_table_id,
            "availability_zone": self.availability_zone,
            "dim_date_start_date": self.dim_date_start_date,
            "dim_date_end_date": self.dim_date_end_date,
            "region": self.region,
        }


def load_cloudformation_config() -> CloudFormationConfig:
    load_dotenv()
    return CloudFormationConfig(
        stack_name=os.getenv("CFN_STACK_NAME", "chinook-analytics"),
        project_name=os.getenv("PROJECT_NAME", "chinook"),
        analytics_bucket=required_env("ANALYTICS_BUCKET"),
        athena_database=os.getenv("ATHENA_DATABASE", "chinook_analytics"),
        curated_prefix=os.getenv("CURATED_PREFIX", "curated/chinook"),
        scripts_prefix=os.getenv("SCRIPTS_PREFIX", "jobs/glue"),
        athena_results_prefix=os.getenv("ATHENA_RESULTS_PREFIX", "athena/results"),
        glue_role_arn=required_env("GLUE_ROLE_ARN"),
        jdbc_connection_name=os.getenv("GLUE_JDBC_CONNECTION_NAME", "chinook-postgres"),
        jdbc_url=required_env("JDBC_URL"),
        jdbc_username=required_env("JDBC_USERNAME"),
        jdbc_password=required_env("JDBC_PASSWORD"),
        glue_subnet_id=required_env("GLUE_SUBNET_ID"),
        glue_security_group_id=required_env("GLUE_SECURITY_GROUP_ID"),
        glue_vpc_id=os.getenv("GLUE_VPC_ID", ""),
        glue_route_table_id=os.getenv("GLUE_ROUTE_TABLE_ID", ""),
        availability_zone=required_env("GLUE_AVAILABILITY_ZONE"),
        dim_date_start_date=os.getenv("DIM_DATE_START_DATE", "2009-01-01"),
        dim_date_end_date=os.getenv("DIM_DATE_END_DATE", "2030-12-31"),
        region=os.getenv("AWS_REGION", "us-east-1"),
    )
