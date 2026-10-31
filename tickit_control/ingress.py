"""Replacement ingress contract: pinned input-manifest events, not mutable .done.

No Lambda handler is installed. A future isolated handler injects a versioned
manifest loader and Dispatcher; acknowledgment happens only after SQS send.
"""
import json
import re
from urllib.parse import unquote_plus

from .batch import Batch


def validate_event(event, expected_bucket):
    records = event.get('Records', [])
    if not records:
        raise ValueError('A nonempty S3 event is required')
    for record in records:
        source = record['s3']
        if source['bucket']['name'] != expected_bucket:
            raise ValueError('Event came from an unexpected bucket')
        key = unquote_plus(source['object']['key'])
        if not re.fullmatch(r'tickit-v2/batches/[a-z0-9][a-z0-9-]{0,63}/input-manifest\.json', key):
            raise ValueError('A version-pinned batch manifest is required; legacy .done is not accepted')
        if source['object'].get('versionId') in (None, '', 'null'):
            raise ValueError('Input-manifest event must pin an S3 version')
    return records


def enqueue_s3_event(event, expected_bucket, load_version, dispatcher):
    records = validate_event(event, expected_bucket)
    results = []
    for record in records:
        source = record['s3']
        bucket = source['bucket']['name']
        key = unquote_plus(source['object']['key'])
        version = source['object'].get('versionId')
        if bucket != expected_bucket:
            raise ValueError('Event came from an unexpected bucket')
        if not version or version == 'null':
            raise ValueError('Input-manifest event must pin an S3 version')
        # Loader must use this VersionId, not whichever object is latest now.
        batch = Batch.parse(json.loads(load_version(bucket, key, version)))
        expected_key = f'tickit-v2/batches/{batch.batch_id}/input-manifest.json'
        if key != expected_key or batch.data['bucket'] != expected_bucket:
            raise ValueError('Manifest does not belong to this immutable batch')
        message_id = dispatcher.enqueue(batch.data)
        results.append({'batch_id': batch.batch_id, 'message_id': message_id, 'status': 'QUEUED'})
    return results
