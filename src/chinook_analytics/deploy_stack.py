from __future__ import annotations

import argparse
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from chinook_analytics.config import CloudFormationConfig, load_cloudformation_config


ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_PATH = ROOT / "infra/cloudformation/chinook-analytics.yml"


def stack_exists(client, stack_name: str) -> bool:
    try:
        client.describe_stacks(StackName=stack_name)
        return True
    except ClientError as exc:
        if "does not exist" in str(exc):
            return False
        raise


def deploy_stack(config: CloudFormationConfig) -> None:
    client = boto3.Session(region_name=config.region).client("cloudformation")
    template_body = TEMPLATE_PATH.read_text(encoding="utf-8")
    params = {
        "StackName": config.stack_name,
        "TemplateBody": template_body,
        "Parameters": config.parameter_overrides(),
        "Capabilities": ["CAPABILITY_NAMED_IAM"],
    }

    if stack_exists(client, config.stack_name):
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
    client.get_waiter(waiter_name).wait(StackName=config.stack_name)
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
