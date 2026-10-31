"""Manifest transport around existing pandas transforms plus a local Spark reference.

Only version-pinned source reads and conditional task-scoped writes reach S3.
The production-scale Spark path lives in spark_etl; no pandas fallback is used
by an actual Glue Spark job.
"""
import csv
import hashlib
import io
from decimal import Decimal

from .admission import Conflict
from .aws import ImmutableObjects
from .context import Context, runtime_fence
from .schema import SCHEMA, arrow_schema


def validate_raw(table, body, expected_rows):
    columns = [name for name, _ in SCHEMA[table]]
    if table == 'event':
        columns = columns[:6]
    reader = csv.DictReader(io.StringIO(body.decode('utf-8-sig')))
    if reader.fieldnames != columns:
        raise ValueError(f'{table}: source columns differ from manifest schema v1')
    rows = list(reader)
    if len(rows) != expected_rows or any(None in row for row in rows):
        raise ValueError(f'{table}: source row count/CSV shape differs from manifest')
    return rows


def parquet_bytes(table, raw):
    import pyarrow as pa
    import pyarrow.parquet as pq
    frame = pq.read_table(pa.BufferReader(raw))
    if frame.column_names != [name for name, _ in SCHEMA[table]]:
        raise ValueError(f'{table}: output schema/order is incorrect')
    frame = frame.cast(arrow_schema(table))
    output = io.BytesIO()
    pq.write_table(frame, output, compression='snappy', version='1.0')
    return output.getvalue(), frame.num_rows


def publish_part(objects, context, table, body, rows, name='part-00000.parquet'):
    key = context.batch.output_prefix(context.attempt, table, context.task_run)+name
    response = objects.put(key, body)
    version = response.get('VersionId')
    if not version or version == 'null':
        raise Conflict('Output bucket must return an actual object version')
    return {'task_run': context.task_run, 'prefix': context.batch.output_prefix(context.attempt, table, context.task_run),
            'rows': rows, 'objects': [{'key': key, 'version_id': version,
                                     'sha256': hashlib.sha256(body).hexdigest(), 'bytes': len(body)}]}


class PandasBatchIO:
    def __init__(self, client, context, table, fence):
        self.context, self.table, self.fence = context, table, fence
        self.objects = ImmutableObjects(client, context.batch.data['bucket'])
        self.allowed = {'event', 'venue', 'category'} if table == 'event' else {table}
        self.output = None

    def get_object(self, *, Bucket, Key, **kwargs):
        self.fence.check()
        if Bucket != self.context.batch.data['bucket'] or kwargs:
            raise Conflict('Branch attempted an unauthorized source request')
        selected = next((name for name in self.allowed if Key in (
            f'bronze/{name}.csv', self.context.batch.data['files'][name]['key'])), None)
        if selected is None:
            raise Conflict('Branch attempted to read outside its batch sources')
        body = self.objects.read_input(self.context.batch, selected)
        validate_raw(selected, body, self.context.batch.data['files'][selected]['rows'])
        self.fence.check()
        return {'Body': io.BytesIO(body)}

    def put_object(self, *, Bucket, Key, Body):
        self.fence.check()
        expected = self.context.batch.output_prefix(self.context.attempt, self.table, self.context.task_run)+self.table+'.parquet'
        if Bucket != self.context.batch.data['bucket'] or Key != expected or self.output is not None:
            raise Conflict('Branch attempted a shared, repeated or cross-task write')
        body, rows = parquet_bytes(self.table, Body)
        self.output = publish_part(self.objects, self.context, self.table, body, rows)
        self.fence.check()
        return {'VersionId': self.output['objects'][0]['version_id']}

    def get_paginator(self, operation):
        if operation != 'list_objects_v2':
            raise Conflict('Unexpected batch cleanup operation')
        return self

    def paginate(self, *, Bucket, Prefix):
        if Bucket != self.context.batch.data['bucket'] or Prefix != self.context.batch.output_prefix(self.context.attempt, self.table, self.context.task_run):
            raise Conflict('Cleanup escaped its task namespace')
        # Legacy transforms call cleanup; manifest mode never lists or deletes S3.
        return iter([{}])

    def delete_objects(self, **kwargs):
        raise Conflict('Deletion is forbidden in immutable batch mode')


def finish_branch(client, context, table, output, fence):
    import json
    fence.check()
    body = json.dumps({'dataset': table, 'output': output}, sort_keys=True, separators=(',', ':')).encode()
    key = context.batch.output_prefix(context.attempt, table, context.task_run)+'result.json'
    result = ImmutableObjects(client, context.batch.data['bucket']).put(key, body)
    if not result.get('VersionId') or result['VersionId'] == 'null':
        raise Conflict('Branch descriptor must be versioned')
    fence.check()
    return {'dataset': table, 'output': output, 'descriptor': {
        'key': key, 'version_id': result['VersionId'], 'sha256': hashlib.sha256(body).hexdigest(), 'bytes': len(body)}}


def run_pandas_branch(namespace, table, request, callback, client=None, fence=None):
    context = Context.parse(request)
    if namespace['S3_BUCKET'] != context.batch.data['bucket']:
        raise Conflict('Configured ETL bucket differs from the manifest')
    fence = fence or runtime_fence(context)
    fence.check()
    transport = PandasBatchIO(client or namespace['s3_client'], context, table, fence)
    old_client, old_prefix = namespace['s3_client'], namespace['SILVER_PREFIX']
    namespace['s3_client'] = transport
    namespace['SILVER_PREFIX'] = context.batch.output_prefix(context.attempt, table, context.task_run)
    try:
        callback()  # Reuse the actual existing cleaning/denormalization functions.
        if transport.output is None:
            raise ValueError('Branch completed without a Parquet result')
        fence.check()
        return finish_branch(client or old_client, context, table, transport.output, fence)
    finally:
        namespace['s3_client'], namespace['SILVER_PREFIX'] = old_client, old_prefix


def transaction_reference(table, body, expected_rows):
    """Small-fixture reference for Spark transforms; never used by Glue at scale."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    rows = validate_raw(table, body, expected_rows)
    key, stamp = ('salesid', 'saletime') if table == 'sales' else ('listid', 'listtime')
    frame = pd.DataFrame(rows, columns=[name for name, _ in SCHEMA[table]])
    parsed = pd.to_datetime(frame[stamp], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
    if parsed.isna().any():
        raise ValueError(f'{table}: NULL transaction timestamp after parsing')
    frame[stamp] = parsed
    frame[key] = pd.to_numeric(frame[key], errors='coerce')
    frame = frame[frame[key].notna()].copy()
    if table == 'sales':
        qty = pd.to_numeric(frame['qtysold'], errors='coerce')
        frame = frame[qty.isna() | (qty >= 1)].copy()
    frame = frame.sort_values(stamp, ascending=False, kind='stable').drop_duplicates(key)
    defaults = {'pricepaid': '0', 'commission': '0'} if table == 'sales' else {'numtickets': '0', 'priceperticket': '0', 'totalprice': '0'}
    records = []
    for row in frame.to_dict('records'):
        result = {}
        for name, kind in SCHEMA[table]:
            value = row[name]
            if value is None or value == '' or pd.isna(value):
                value = defaults.get(name)
            if value is not None:
                if kind.startswith('int'):
                    value = int(value)
                elif kind == 'decimal':
                    value = Decimal(str(value)).quantize(Decimal('0.01'))
                elif kind == 'timestamp':
                    value = pd.Timestamp(value).to_pydatetime()
            result[name] = value
        records.append(result)
    output = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(records, schema=arrow_schema(table)), output, compression='snappy', version='1.0')
    return output.getvalue(), len(records)


def run_transaction_reference(table, request, client, fence):
    context = Context.parse(request)
    fence.check()
    objects = ImmutableObjects(client, context.batch.data['bucket'])
    body = objects.read_input(context.batch, table)
    result, rows = transaction_reference(table, body, context.batch.data['files'][table]['rows'])
    fence.check()
    output = publish_part(objects, context, table, result, rows)
    fence.check()
    return finish_branch(client, context, table, output, fence)
