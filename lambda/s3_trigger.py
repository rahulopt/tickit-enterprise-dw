"""Undeployed queued ingress: immutable manifest -> DynamoDB outbox -> SQS FIFO.

This local replacement intentionally removes direct Step Functions startup and
check-then-start skipping. It requires the new manifest contract; do not deploy
it against legacy bronze/.done notifications. See docs/OVERLAP_PROTECTION.md.
"""
import json
import os
import time

from tickit_control.admission import Admission, Dispatcher
from tickit_control.aws import DynamoStore, SqsQueue
from tickit_control.ingress import enqueue_s3_event, validate_event

EXPECTED_BUCKET = os.environ['TICKIT_BUCKET']


def _services():
    # No clients or AWS requests are created at import. Tests inject this factory.
    import boto3
    store = DynamoStore(boto3.client('dynamodb'), os.environ['ADMISSION_TABLE'])
    queue = SqsQueue(boto3.client('sqs'), os.environ['BATCH_QUEUE_URL'])
    admission = Admission(store, None, time.time, pipeline_id=os.environ['TICKIT_PIPELINE_ID'])
    dispatcher = Dispatcher(admission, queue)
    s3 = boto3.client('s3')
    def load_version(bucket, key, version):
        return s3.get_object(Bucket=bucket, Key=key, VersionId=version)['Body'].read()
    return dispatcher, load_version


def lambda_handler(event, context):
    if event.get('Event') == 's3:TestEvent':
        return {'statusCode': 200, 'body': json.dumps({'status': 'IGNORED'})}
    if not isinstance(event.get('Records'), list) or not event['Records']:
        return {'statusCode': 400, 'body': 'Invalid S3 notification'}
    validate_event(event, EXPECTED_BUCKET)  # Reject legacy/wrong input before SDK use.
    dispatcher, load_version = _services()
    results = enqueue_s3_event(event, EXPECTED_BUCKET, load_version, dispatcher)
    return {'statusCode': 200, 'body': json.dumps({'results': results})}
