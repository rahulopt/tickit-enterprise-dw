"""Build exact COPY manifests from pinned outputs; submit one atomic batch load.

No SQL is executed at import. Native SQL remains an undeployed candidate.
"""
import hashlib
import json
import re

from .admission import Conflict
from .aws import ImmutableObjects
from .batch import TABLES, canonical
from .storage import pinned_file, put_file


def gather_outputs(client, context, descriptors, fence):
    """Both Lambda results and Glue result.json sidecars use pinned descriptors."""
    if set(descriptors) != set(TABLES):
        raise ValueError('Missing or unexpected ETL datasets')
    outputs = {}
    for table in TABLES:
        reference = descriptors[table]
        fence.check()
        with pinned_file(client, context.batch.data['bucket'], reference) as (stream, _):
            result = json.load(stream)
        if result['dataset'] != table:
            raise Conflict('Branch descriptor names another dataset')
        output = result['output']
        if reference['key'] != context.batch.output_prefix(context.attempt, table, output['task_run'])+'result.json':
            raise Conflict('Branch descriptor belongs to another task or attempt')
        outputs[table] = output
    context.batch.output_manifest(context.attempt, outputs)
    fence.check()
    return outputs


def commit_outputs(client, context, outputs, fence):
    fence.check()
    body = canonical(context.batch.output_manifest(context.attempt, outputs)).encode()
    objects = ImmutableObjects(client, context.batch.data['bucket'])
    key = context.batch.attempt_root(context.attempt)+'output-manifest.json'
    result = objects.put(key, body)
    if not result.get('VersionId') or result['VersionId'] == 'null':
        raise Conflict('Output manifest is not version pinned')
    fence.check()
    return {'key':key, 'version_id':result['VersionId'], 'sha256':hashlib.sha256(body).hexdigest(), 'bytes':len(body)}


def prepare_copy(client, context, committed, fence):
    fence.check()
    bucket = context.batch.data['bucket']
    expected_key = context.batch.attempt_root(context.attempt)+'output-manifest.json'
    if committed['key'] != expected_key:
        raise Conflict('Wrong output manifest')
    with pinned_file(client, bucket, committed) as (stream, _):
        document = json.load(stream)
    context.batch.validate_output_manifest(context.attempt, document)
    specs = {}
    objects = ImmutableObjects(client, bucket)
    for table in TABLES:
        entries = []
        for index, part in enumerate(document['outputs'][table]['objects']):
            # COPY does not get an invented ?versionId URL. Materialize the exact
            # verified version under an immutable checksum-addressed batch key.
            target = context.batch.attempt_root(context.attempt)+f'warehouse/{table}/{index:05d}-{part["sha256"]}.parquet'
            with pinned_file(client, bucket, part) as (stream, size):
                fence.check()
                put_file(client, bucket, target, stream, part['sha256'], size)
            entries.append({'url':f's3://{bucket}/{target}', 'mandatory':True,
                            'meta':{'content_length':part['bytes']}})
        if len({entry['url'] for entry in entries}) != len(entries):
            raise Conflict('Duplicate COPY input would load a file twice')
        key = context.batch.attempt_root(context.attempt)+f'warehouse/{table}.copy.json'
        objects.put(key, canonical({'entries':entries}).encode())
        specs[table] = {'url':f's3://{bucket}/{key}', 'rows':document['outputs'][table]['rows']}
    fence.check()
    return document, specs


def submit(client, context, committed, specs, fence, database, workgroup, copy_role):
    if not re.fullmatch(r'arn:aws[a-z-]*:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+', copy_role):
        raise ValueError('Invalid COPY role')
    if set(specs) != set(TABLES):
        raise ValueError('All six COPY manifests are required')
    fence.check()
    token = hashlib.sha256(canonical([context.batch.fingerprint,context.attempt,context.epoch,committed['sha256']]).encode()).hexdigest()
    return client.execute_statement(Database=database, WorkgroupName=workgroup, ClientToken=token,
        Sql='CALL sp_load_manifest_warehouse(:batch_id,:input_sha,:output_sha,CAST(:attempt AS INTEGER),CAST(:epoch AS BIGINT),CAST(:load_date AS DATE),:bucket,:copy_role,:specs)',
        Parameters=[{'name':name, 'value':str(value)} for name,value in {
            'batch_id':context.batch.batch_id, 'input_sha':context.batch.fingerprint, 'output_sha':committed['sha256'],
            'attempt':context.attempt,'epoch':context.epoch,'load_date':context.batch.data['load_date'],
            'bucket':context.batch.data['bucket'],'copy_role':copy_role,'specs':canonical(specs)}.items()])
