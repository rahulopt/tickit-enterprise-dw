"""Undeployed test-only controller. SDK mutation retries deliberately disabled."""
import json
import os


def lambda_handler(event, context):
    import boto3
    from botocore.config import Config
    from tickit_control.aws import DynamoStore
    from tickit_control.native import NativeController
    prefix = os.environ['TICKIT_PIPELINE_ID']
    settings = {'bucket':os.environ['TICKIT_BUCKET'],'table':os.environ['ADMISSION_TABLE'],
        'pipeline_id':prefix,'database':os.environ['REDSHIFT_DATABASE'],
        'workgroup':os.environ['REDSHIFT_WORKGROUP'],'copy_role':os.environ['REDSHIFT_COPY_ROLE'],
        'functions':{x:prefix+'-'+x for x in ('category','venue')},
        'jobs':{x:prefix+'-'+x for x in ('users','event','listing','sales')}}
    session = boto3.session.Session()
    clients = {service:session.client(service,config=Config(
        connect_timeout=5,read_timeout=75 if service == 'lambda' else 20,
        retries={'total_max_attempts':1})) for service in ('s3','dynamodb','lambda','glue','redshift-data')}
    return NativeController(DynamoStore(clients['dynamodb'],settings['table']),clients,settings).handle(event)
