"""Streaming pinned reads and conditional, immutable single-object publication.

Native integration must enforce no-delete/no-overwrite permissions. COPY and
Spark read materialized immutable keys; neither is assumed to understand S3
VersionId query strings. Single PUT is explicitly limited to 5 GiB here.
"""
import base64
import hashlib
import re
import tempfile
from contextlib import contextmanager

from .admission import Conflict
from .aws import error_code

MAX_OBJECT = 5 * 1024**3


def safe_key(key):
    if not re.fullmatch(r'tickit-v2/batches/[a-z0-9][a-z0-9-]{0,63}/[A-Za-z0-9_./-]+', key) or '..' in key.split('/'):
        raise ValueError('Only immutable batch object paths are allowed')


@contextmanager
def pinned_file(client, bucket, reference):
    result = client.get_object(Bucket=bucket, Key=reference['key'], VersionId=reference['version_id'])
    body = result['Body']
    try:
        with tempfile.TemporaryFile() as stream:
            digest, count = hashlib.sha256(), 0
            while True:
                chunk = body.read(1024*1024)
                if not chunk:
                    break
                count += len(chunk)
                if count > MAX_OBJECT:
                    raise ValueError('Object exceeds the verified single-PUT transport limit')
                digest.update(chunk)
                stream.write(chunk)
            if digest.hexdigest() != reference['sha256'] or ('bytes' in reference and count != reference['bytes']):
                raise Conflict('Pinned object checksum/size differs from manifest')
            stream.seek(0)
            yield stream, count
    finally:
        body.close()


def put_file(client, bucket, key, stream, checksum, size):
    safe_key(key)
    if not 0 <= size <= MAX_OBJECT:
        raise ValueError('Unsupported object size')
    stream.seek(0)
    try:
        result = client.put_object(Bucket=bucket, Key=key, Body=stream, IfNoneMatch='*',
                                   ChecksumSHA256=base64.b64encode(bytes.fromhex(checksum)).decode(),
                                   Metadata={'sha256': checksum})
        version = result.get('VersionId')
    except Exception as exc:
        if error_code(exc) != 'PreconditionFailed':
            raise
        existing = client.get_object(Bucket=bucket, Key=key)
        digest, count = hashlib.sha256(), 0
        try:
            while True:
                chunk = existing['Body'].read(1024*1024)
                if not chunk:
                    break
                digest.update(chunk)
                count += len(chunk)
        finally:
            existing['Body'].close()
        if digest.hexdigest() != checksum or count != size:
            raise Conflict('Immutable object collision') from exc
        version = existing.get('VersionId')
    if not version or version == 'null':
        raise Conflict('An actual S3 version is required')
    return {'key': key, 'version_id': version, 'sha256': checksum, 'bytes': size}
