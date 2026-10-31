"""Local versioned object store and atomic warehouse test adapter; no SDK calls."""
import hashlib
import importlib.util
import io
import json
import re
from decimal import Decimal
from datetime import datetime, date
from pathlib import Path

from botocore.exceptions import ClientError

from tickit_control.admission import Conflict
from tickit_control.batch import Batch, TABLES
from tickit_control.schema import SCHEMA, arrow_schema
from test_scd2 import Warehouse

ROOT = Path(__file__).resolve().parents[1]


def load_module(filename, suffix):
    spec = importlib.util.spec_from_file_location('manifest_'+suffix, ROOT/filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class VersionedObjects:
    def __init__(self):
        self.versions, self.reads, self.writes = {}, [], []
        self.counter = 0

    def seed(self, bucket, key, body):
        self.counter += 1
        version = 'version-'+str(self.counter)
        self.versions.setdefault((bucket,key), {})[version] = body
        return {'key':key,'version_id':version,'sha256':hashlib.sha256(body).hexdigest(),'bytes':len(body)}

    def put_object(self, *, Bucket, Key, Body, IfNoneMatch=None, **kwargs):
        assert IfNoneMatch == '*', 'Only conditional immutable writes are allowed'
        if (Bucket,Key) in self.versions:
            raise ClientError({'Error':{'Code':'PreconditionFailed'}}, 'PutObject')
        body = Body if isinstance(Body,bytes) else Body.read()
        ref = self.seed(Bucket,Key,body)
        self.writes.append((Key,ref['version_id']))
        return {'VersionId':ref['version_id']}

    def get_object(self, *, Bucket, Key, VersionId=None):
        versions = self.versions[(Bucket,Key)]
        selected = VersionId or next(reversed(versions))
        self.reads.append((Key,VersionId))
        return {'Body':io.BytesIO(versions[selected]),'VersionId':selected,'ContentLength':len(versions[selected])}


def upload_fixture(client, directory, name, day, batch_id, bucket='test-immutable-bucket'):
    files = {}
    import csv
    for table in TABLES:
        path = Path(directory)/name/'bronze'/f'{table}.csv'
        body = path.read_bytes()
        key = f'tickit-v2/batches/{batch_id}/bronze/{table}.csv'
        info = client.seed(bucket,key,body)
        info.pop('bytes')
        info['rows'] = len(list(csv.DictReader(io.StringIO(body.decode()))))
        files[table] = info
    return Batch.parse({'schema_version':1,'datasets':list(TABLES),'batch_id':batch_id,'bucket':bucket,
                        'load_date':day,'business_timezone':'UTC','files':files})


class ManifestWarehouse:
    def __init__(self):
        self.engine = Warehouse()
        self.db = self.engine.db
        self.db.execute("ATTACH DATABASE ':memory:' AS control")
        text = re.sub(r'--[^\n]*','',(ROOT/'sql/09_manifest_warehouse.sql').read_text())
        columns = re.search(r'CREATE TABLE IF NOT EXISTS control.batch_commits\s*\((.*?)\) DISTSTYLE',text,re.S)[1]
        self.db.execute('CREATE TABLE control.batch_commits ('+columns.replace('GETDATE()','CURRENT_TIMESTAMP')+')')
        self.loads = 0

    def receipt(self, batch_id, fingerprint):
        row = self.db.execute('SELECT input_sha FROM control.batch_commits WHERE batch_id=?',(batch_id,)).fetchone()
        if row and row[0] != fingerprint:
            raise Conflict('Committed batch ID reused with different inputs')
        return bool(row)

    def load(self, client, context, committed, document, specs, fence):
        import pyarrow as pa
        import pyarrow.parquet as pq
        context.batch.validate_output_manifest(context.attempt,document)
        fence.check()
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            if self.receipt(context.batch.batch_id,context.batch.fingerprint):
                return 'ALREADY_COMMITTED'
            epoch = self.db.execute('SELECT COALESCE(MAX(fence_epoch),0) FROM control.batch_commits').fetchone()[0]
            if context.epoch < epoch:
                raise Conflict('Stale warehouse fencing epoch')
            for table in TABLES:
                expected = f's3://{context.batch.data["bucket"]}/'+context.batch.attempt_root(context.attempt)+f'warehouse/{table}.copy.json'
                if specs[table]['url'] != expected:
                    raise Conflict('COPY points at a different batch')
                key = expected.split('/',3)[3]
                manifest = json.load(client.get_object(Bucket=context.batch.data['bucket'],Key=key)['Body'])
                columns = [name for name,_ in SCHEMA[table]]
                self.db.execute('DELETE FROM stage.'+table)
                count = 0
                seen = set()
                for entry in manifest['entries']:
                    if entry['url'] in seen:
                        raise Conflict('Duplicate COPY entry')
                    seen.add(entry['url'])
                    bucket,key = entry['url'][5:].split('/',1)
                    if bucket != context.batch.data['bucket'] or not key.startswith(context.batch.attempt_root(context.attempt)+f'warehouse/{table}/'):
                        raise Conflict('Cross-batch COPY entry')
                    body = client.get_object(Bucket=bucket,Key=key)['Body'].read()
                    if len(body) != entry['meta']['content_length']:
                        raise Conflict('COPY content length differs from manifest')
                    data = pq.read_table(pa.BufferReader(body))
                    assert data.schema.remove_metadata() == arrow_schema(table)
                    for row in data.to_pylist():
                        values = []
                        for column in columns:
                            value = row[column]
                            if isinstance(value,Decimal):value = str(value)
                            elif isinstance(value,datetime):value = value.strftime('%Y-%m-%d %H:%M:%S.%f')
                            elif isinstance(value,date):value = value.isoformat()
                            values.append(value)
                        self.db.execute(f'INSERT INTO stage.{table} ({",".join(columns)}) VALUES ({",".join("?" for _ in values)})',values)
                    count += data.num_rows
                if count != specs[table]['rows']:
                    raise ValueError('COPY row count differs from committed output manifest')
            fence.check()
            self.engine.run('sp_load_dimensions',context.batch.data['load_date'])
            self.engine.run('sp_load_facts',context.batch.data['load_date'])
            self.db.execute('INSERT INTO control.batch_commits(batch_id,input_sha,output_sha,attempt,fence_epoch,load_date) VALUES (?,?,?,?,?,?)',
                (context.batch.batch_id,context.batch.fingerprint,committed['sha256'],context.attempt,context.epoch,context.batch.data['load_date']))
        self.loads += 1
        return 'COMMITTED'

    def snapshot(self):
        names = [f'stage.{table}' for table in TABLES]+['dim.users','dim.event','dim.scd_load_state','fact.sales','fact.listing','control.batch_commits']
        return {table:self.db.execute('SELECT * FROM '+table).fetchall() for table in names}
