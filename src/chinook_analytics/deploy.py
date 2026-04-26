from __future__ import annotations

import argparse
from pathlib import Path

import boto3

from chinook_analytics.config import AnalyticsConfig, load_config


ROOT = Path(__file__).resolve().parents[2]
GLUE_VERSION = "4.0"


def upload_file(s3_client, bucket: str, key: str, path: Path) -> None:
    s3_client.upload_file(str(path), bucket, key)


def ensure_bucket(s3_client, bucket: str, region: str) -> None:
    existing = {item["Name"] for item in s3_client.list_buckets()["Buckets"]}
    if bucket in existing:
        return
    params: dict[str, object] = {"Bucket": bucket}
    if region != "us-east-1":
        params["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3_client.create_bucket(**params)


def put_athena_ddl(athena_client, config: AnalyticsConfig, ddl: str) -> None:
    athena_client.start_query_execution(
        QueryString=ddl,
        ResultConfiguration={"OutputLocation": f"s3://{config.bucket}/{config.athena_results_prefix}/"},
    )


def create_or_update_job(glue_client, *, name: str, script_location: str, config: AnalyticsConfig) -> None:
    command = {"Name": "glueetl", "ScriptLocation": script_location, "PythonVersion": "3"}
    default_args = {
        "--job-language": "python",
        "--enable-glue-datacatalog": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--SOURCE_CONNECTION_NAME": config.jdbc_connection_name,
        "--TARGET_DATABASE": config.database,
        "--TARGET_BUCKET": config.bucket,
        "--CURATED_PREFIX": config.curated_prefix,
        "--START_DATE": config.dim_date_start_date,
        "--END_DATE": config.dim_date_end_date,
    }
    job_definition = {
        "Role": config.glue_role_arn,
        "Command": command,
        "DefaultArguments": default_args,
        "GlueVersion": GLUE_VERSION,
        "NumberOfWorkers": 2,
        "WorkerType": "G.1X",
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
    }
    try:
        glue_client.get_job(JobName=name)
        glue_client.update_job(JobName=name, JobUpdate=job_definition)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_job(Name=name, **job_definition)


def create_or_update_trigger(glue_client, *, name: str, schedule: str, job_name: str) -> None:
    action = {"JobName": job_name}
    try:
        glue_client.get_trigger(Name=name)
        glue_client.update_trigger(
            Name=name,
            TriggerUpdate={
                "Description": f"Scheduled refresh for {job_name}",
                "Schedule": schedule,
                "Actions": [action],
            },
        )
        try:
            glue_client.start_trigger(Name=name)
        except glue_client.exceptions.InvalidInputException:
            pass
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_trigger(
            Name=name,
            Type="SCHEDULED",
            Description=f"Scheduled refresh for {job_name}",
            Schedule=schedule,
            Actions=[action],
            StartOnCreation=True,
        )


def deploy(config: AnalyticsConfig) -> None:
    session = boto3.Session(region_name=config.region)
    s3_client = session.client("s3")
    glue_client = session.client("glue")
    athena_client = session.client("athena")

    ensure_bucket(s3_client, config.bucket, config.region)

    scripts = {
        "chinook-dimensions-visual": ROOT / "jobs/glue/dimensions_visual.py",
        "chinook-fact-sales-visual": ROOT / "jobs/glue/fact_sales_visual.py",
        "chinook-dim-date-python": ROOT / "jobs/glue/dim_date_job.py",
        "chinook-full-copy-history": ROOT / "jobs/glue/full_copy_history.py",
    }
    for job_name, script_path in scripts.items():
        key = f"{config.scripts_prefix.rstrip('/')}/{script_path.name}"
        upload_file(s3_client, config.bucket, key, script_path)
        create_or_update_job(
            glue_client,
            name=job_name,
            script_location=f"s3://{config.bucket}/{key}",
            config=config,
        )

    ddl_template = (ROOT / "sql/athena_ddl.sql").read_text(encoding="utf-8")
    ddl = ddl_template.format(database=config.database, bucket=config.bucket, curated_prefix=config.curated_prefix)
    for statement in [part.strip() for part in ddl.split(";") if part.strip()]:
        put_athena_ddl(athena_client, config, statement)

    create_or_update_trigger(
        glue_client,
        name="chinook-dimensions-hourly",
        schedule="cron(0 * * * ? *)",
        job_name="chinook-dimensions-visual",
    )
    create_or_update_trigger(
        glue_client,
        name="chinook-fact-sales-every-15-min",
        schedule="cron(0/15 * * * ? *)",
        job_name="chinook-fact-sales-visual",
    )
    create_or_update_trigger(
        glue_client,
        name="chinook-full-copy-history-daily",
        schedule="cron(0 5 * * ? *)",
        job_name="chinook-full-copy-history",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy Chinook analytics resources to AWS.")
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without AWS changes.")
    args = parser.parse_args()
    config = load_config()
    if args.dry_run:
        print(config)
        return
    deploy(config)


if __name__ == "__main__":
    main()
