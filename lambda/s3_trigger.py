"""
TICKIT Enterprise Data Warehouse
Lambda: s3_trigger.py

Purpose: S3 trigger handler for .done file
         When source system drops bronze/.done file,
         this Lambda starts the Step Functions pipeline.

Flow:
    Source System drops files:
    1. bronze/category.csv
    2. bronze/venue.csv
    3. bronze/users.csv
    4. bronze/event.csv
    5. bronze/listing.csv
    6. bronze/sales.csv
    7. bronze/.done  ← THIS triggers the Lambda

How it's connected:
    S3 Event Notification → EventBridge → This Lambda → Step Functions

Safeguard:
    - Only starts pipeline if key ends with '.done'
    - Validates expected key prefix is 'bronze/'
    - Checks Step Functions not already running (prevents duplicate runs)
"""

import json
import boto3
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

STATE_MACHINE_ARN = "arn:aws:states:ap-south-1:519749210589:stateMachine:tickit-nightly-pipeline"
EXPECTED_BUCKET = "ent-dw-ticket-sales-519749210589"
DONE_FILE_KEY = "bronze/.done"

sfn_client = boto3.client("stepfunctions", region_name="ap-south-1")


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    # ── Step 1: Extract S3 event info ────────────────────────────
    try:
        # S3 event notification structure
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except (KeyError, IndexError) as e:
        logger.error(f"Invalid event structure: {e}")
        return {"statusCode": 400, "body": "Invalid S3 event"}

    logger.info(f"S3 event: bucket={bucket}, key={key}")

    # ── Step 2: Validate this is the .done trigger ────────────────
    if bucket != EXPECTED_BUCKET:
        logger.warning(f"Wrong bucket: {bucket}, expected {EXPECTED_BUCKET}")
        return {"statusCode": 200, "body": "Not our bucket, ignoring"}

    if not key.endswith(".done"):
        logger.info(f"Key {key} is not .done file, ignoring")
        return {"statusCode": 200, "body": "Not a .done file, ignoring"}

    if not key.startswith("bronze/"):
        logger.warning(f"Key {key} not in bronze/, ignoring")
        return {"statusCode": 200, "body": "Not in bronze/, ignoring"}

    logger.info(f".done file detected: {key} — Starting TICKIT nightly pipeline!")

    # ── Step 3: Check if pipeline already running ─────────────────
    # Prevent duplicate runs if .done file arrives multiple times
    running_executions = sfn_client.list_executions(
        stateMachineArn=STATE_MACHINE_ARN,
        statusFilter="RUNNING",
    )
    if running_executions["executions"]:
        logger.warning("Pipeline already running — skipping duplicate trigger")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "SKIPPED",
                "reason": "Pipeline already running",
                "running_execution": running_executions["executions"][0]["executionArn"],
            }),
        }

    # ── Step 4: Start Step Functions execution ─────────────────────
    load_date = datetime.utcnow().strftime("%Y-%m-%d")
    execution_name = f"tickit-nightly-{load_date}"

    response = sfn_client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=execution_name,
        input=json.dumps({
            "trigger": {
                "source": "s3_done_file",
                "bucket": bucket,
                "key": key,
                "load_date": load_date,
            }
        }),
    )

    logger.info(f"Pipeline started: {response['executionArn']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "status": "STARTED",
            "execution_arn": response["executionArn"],
            "load_date": load_date,
        }),
    }
