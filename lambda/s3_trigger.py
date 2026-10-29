"""Start the nightly Standard Step Functions pipeline for bronze/.done uploads.

Configure the S3 notification for s3:ObjectCreated:* with prefix ``bronze/``
and suffix ``.done``. This handler independently checks the *exact* key.

The execution name is deliberately stable for a UTC delivery date. This
prevents concurrent duplicate starts and repeated deliveries of the same S3
notification. A Standard workflow cannot reuse that name for 90 days; see
README for intentional reprocessing instructions.
"""

import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

STATE_MACHINE_ARN = os.getenv(
    "STATE_MACHINE_ARN",
    "arn:aws:states:us-east-1:519749210589:stateMachine:tickit-nightly-pipeline-v2",
)
EXPECTED_BUCKET = os.getenv("TICKIT_BUCKET", "ent-dw-ticket-sales-519749210589")
DONE_FILE_KEY = "bronze/.done"

# Use the target ARN region even when this Lambda runs in another region.
sfn_client = boto3.client("stepfunctions", region_name=STATE_MACHINE_ARN.split(":")[3])


def _handle_record(record):
    s3_info = record.get("s3", {})
    bucket = s3_info.get("bucket", {}).get("name")
    # S3 event notification object keys are form-url-encoded (spaces become '+').
    encoded_key = s3_info.get("object", {}).get("key", "")
    key = unquote_plus(encoded_key)

    if bucket != EXPECTED_BUCKET or key != DONE_FILE_KEY:
        logger.info("Ignoring S3 object: bucket=%s key=%s", bucket, key)
        return {"status": "IGNORED", "bucket": bucket, "key": key}

    event_time = record.get("eventTime")
    if not event_time:
        raise ValueError("S3 event is missing eventTime; cannot determine delivery date")
    # eventTime is stable across at-least-once delivery of the same event.
    delivery_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    load_date = delivery_time.astimezone(timezone.utc).date().isoformat()
    execution_name = f"tickit-nightly-{load_date}"

    # Avoid overlapping *different-day* batches which overwrite shared Silver
    # paths. This is a best-effort pre-check, not an atomic global lock.
    running = sfn_client.list_executions(
        stateMachineArn=STATE_MACHINE_ARN, statusFilter="RUNNING", maxResults=10
    ).get("executions", [])
    if running:
        logger.info("Pipeline already running; skipping marker for %s", load_date)
        return {"status": "SKIPPED", "reason": "Pipeline already running"}

    execution_input = json.dumps(
        {"trigger": {
            "source": "s3_done_file",
            "bucket": bucket,
            "key": key,
            "load_date": load_date,
        }},
        sort_keys=True,
        separators=(",", ":"),
    )

    try:
        execution = sfn_client.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=execution_name,
            input=execution_input,
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ExecutionAlreadyExists":
            logger.info("Daily execution already exists: %s", execution_name)
            return {"status": "SKIPPED", "reason": "Daily execution already exists"}
        raise

    logger.info("Started %s: %s", execution_name, execution["executionArn"])
    return {
        "status": "STARTED",
        "execution_arn": execution["executionArn"],
        "load_date": load_date,
    }


def lambda_handler(event, context):
    if event.get("Event") == "s3:TestEvent":
        return {"statusCode": 200, "body": json.dumps({"status": "IGNORED"})}

    records = event.get("Records")
    if not isinstance(records, list) or not records:
        logger.error("Expected an S3 notification with nonempty Records list")
        return {"statusCode": 400, "body": "Invalid S3 notification"}

    outcomes = []
    for record in records:
        outcomes.append(_handle_record(record))
    return {"statusCode": 200, "body": json.dumps({"results": outcomes})}
