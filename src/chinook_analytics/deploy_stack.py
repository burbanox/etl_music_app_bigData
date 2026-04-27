from __future__ import annotations

import argparse
from pathlib import Path

import boto3
from botocore.exceptions import WaiterError
from botocore.exceptions import ClientError
from dataclasses import replace

from chinook_analytics.config import CloudFormationConfig, load_cloudformation_config


ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_PATH = ROOT / "infra/cloudformation/chinook-analytics.yml"


def get_stack(client, stack_name: str) -> dict | None:
    try:
        return client.describe_stacks(StackName=stack_name)["Stacks"][0]
    except ClientError as exc:
        if "does not exist" in str(exc):
            return None
        raise


def print_failure_events(client, stack_name: str, limit: int = 20) -> None:
    events = client.describe_stack_events(StackName=stack_name)["StackEvents"]
    printed = 0
    print("Recent CloudFormation failure events:")
    for event in events:
        status = event.get("ResourceStatus", "")
        if "FAILED" not in status and "ROLLBACK" not in status:
            continue
        print(
            f"- {event['LogicalResourceId']} "
            f"({event['ResourceType']}): {status} - {event.get('ResourceStatusReason', '')}"
        )
        printed += 1
        if printed >= limit:
            break


def delete_rollback_complete_stack(client, stack_name: str) -> None:
    stack = get_stack(client, stack_name)
    if not stack or stack["StackStatus"] != "ROLLBACK_COMPLETE":
        return
    print(f"Deleting failed stack {stack_name} in ROLLBACK_COMPLETE before recreating it...")
    client.delete_stack(StackName=stack_name)
    client.get_waiter("stack_delete_complete").wait(StackName=stack_name)


def enrich_network_config(session: boto3.Session, config: CloudFormationConfig) -> CloudFormationConfig:
    if config.glue_vpc_id and config.glue_route_table_id:
        return config

    ec2_client = session.client("ec2")
    subnet = ec2_client.describe_subnets(SubnetIds=[config.glue_subnet_id])["Subnets"][0]
    vpc_id = config.glue_vpc_id or subnet["VpcId"]
    route_tables = ec2_client.describe_route_tables(
        Filters=[{"Name": "association.subnet-id", "Values": [config.glue_subnet_id]}]
    )["RouteTables"]

    if not route_tables:
        route_tables = ec2_client.describe_route_tables(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "association.main", "Values": ["true"]},
            ]
        )["RouteTables"]

    if not route_tables:
        raise RuntimeError(f"Could not find route table for subnet {config.glue_subnet_id}")

    return replace(
        config,
        glue_vpc_id=vpc_id,
        glue_route_table_id=config.glue_route_table_id or route_tables[0]["RouteTableId"],
    )


def deploy_stack(config: CloudFormationConfig) -> None:
    session = boto3.Session(region_name=config.region)
    config = enrich_network_config(session, config)
    client = session.client("cloudformation")
    delete_rollback_complete_stack(client, config.stack_name)
    template_body = TEMPLATE_PATH.read_text(encoding="utf-8")
    params = {
        "StackName": config.stack_name,
        "TemplateBody": template_body,
        "Parameters": config.parameter_overrides(),
        "Capabilities": ["CAPABILITY_NAMED_IAM"],
    }

    if get_stack(client, config.stack_name):
        try:
            client.update_stack(**params)
            waiter_name = "stack_update_complete"
        except ClientError as exc:
            if "No updates are to be performed" in str(exc):
                print("CloudFormation stack is already up to date.")
                return
            raise
    else:
        client.create_stack(**params)
        waiter_name = "stack_create_complete"

    print(f"Waiting for {config.stack_name} to finish...")
    try:
        client.get_waiter(waiter_name).wait(StackName=config.stack_name)
    except WaiterError:
        print_failure_events(client, config.stack_name)
        raise
    outputs = client.describe_stacks(StackName=config.stack_name)["Stacks"][0].get("Outputs", [])
    for output in outputs:
        print(f"{output['OutputKey']}={output['OutputValue']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy the Chinook analytics CloudFormation stack.")
    parser.add_argument("--dry-run", action="store_true", help="Load .env and print non-secret parameters.")
    args = parser.parse_args()
    config = load_cloudformation_config()

    if args.dry_run:
        for key, value in config.safe_dict().items():
            print(f"{key}={value}")
        return

    deploy_stack(config)


if __name__ == "__main__":
    main()
