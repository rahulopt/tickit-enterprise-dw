"""Undeployed scheduled dispatcher/watchdog. Never use an auto-ack SQS mapping.

The future isolated workflow must instrument WorkTracker before this can release
leases safely. No production configuration or AWS schedules are installed.
"""
import os
import time

from tickit_control.admission import Admission, Dispatcher
from tickit_control.aws import DynamoStore, SqsQueue, StandardPipeline
from tickit_control.work import WorkTracker


def _services():
    import boto3
    store = DynamoStore(boto3.client('dynamodb'), os.environ['ADMISSION_TABLE'])
    tracker = WorkTracker(store, os.environ['TICKIT_PIPELINE_ID'])
    pipeline = StandardPipeline(boto3.client('stepfunctions'), os.environ['STATE_MACHINE_ARN'], tracker,
                                boto3.client('glue'), boto3.client('redshift-data'), seal_terminal=True)
    admission = Admission(store, pipeline, time.time, pipeline_id=os.environ['TICKIT_PIPELINE_ID'])
    return Dispatcher(admission, SqsQueue(boto3.client('sqs'), os.environ['BATCH_QUEUE_URL']))


def lambda_handler(event, context):
    dispatcher = _services()
    republished = dispatcher.reconcile()
    return {'republished': republished, 'admission': dispatcher.poll()}
