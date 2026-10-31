"""Immutable, version-pinned source contract and collision-free output namespaces."""
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date

TABLES = ('category', 'venue', 'users', 'event', 'listing', 'sales')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}', value):
        raise ValueError('IDs must be 1..64 lowercase alphanumeric/hyphen characters')
    return value


@dataclass(frozen=True)
class Batch:
    document: str

    @classmethod
    def parse(cls, value):
        if not isinstance(value, dict) or set(value) != {'schema_version', 'datasets', 'batch_id', 'bucket', 'load_date', 'business_timezone', 'files'}:
            raise ValueError('Batch requires an explicit immutable six-file manifest')
        if type(value['schema_version']) is not int or value['schema_version'] != 1 or value['datasets'] != list(TABLES):
            raise ValueError('Use manifest schema_version=1 with the canonical dataset list')
        batch_id = identifier(value['batch_id'])
        if not isinstance(value['bucket'], str) or not re.fullmatch(r'[a-z0-9][a-z0-9.-]{2,62}', value['bucket']):
            raise ValueError('Invalid source bucket')
        day = value['load_date']
        if not isinstance(day, str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', day):
            raise ValueError('load_date must use YYYY-MM-DD')
        date.fromisoformat(day)
        if value['business_timezone'] != 'UTC' or not isinstance(value['files'], dict) or set(value['files']) != set(TABLES):
            raise ValueError('UTC and all six source datasets are required')
        for table, info in value['files'].items():
            if not isinstance(info, dict) or set(info) != {'key', 'version_id', 'sha256', 'rows'}:
                raise ValueError('Each source needs key, version_id, sha256 and rows')
            if info['key'] != f'tickit-v2/batches/{batch_id}/bronze/{table}.csv':
                raise ValueError('Shared or cross-batch source paths are forbidden')
            if not isinstance(info['version_id'], str) or not info['version_id'] or info['version_id'] == 'null':
                raise ValueError('Source object versions must be pinned')
            if not isinstance(info['sha256'], str) or not re.fullmatch(r'[a-f0-9]{64}', info['sha256']):
                raise ValueError('Source SHA-256 is required')
            if type(info['rows']) is not int or info['rows'] < 0:
                raise ValueError('Invalid row count')
        raw = canonical(value)
        if len(raw.encode()) > 32_000:
            raise ValueError('Batch manifest is too large')
        return cls(raw)

    @property
    def data(self):
        return json.loads(self.document)

    @property
    def batch_id(self):
        return self.data['batch_id']

    @property
    def fingerprint(self):
        return hashlib.sha256(self.document.encode()).hexdigest()

    def execution_name(self, attempt):
        if type(attempt) is not int or attempt < 1:
            raise ValueError('Attempt must be a positive integer')
        return f'tickit-{self.fingerprint[:48]}-a{attempt}'

    def attempt_root(self, attempt):
        self.execution_name(attempt)
        return f'tickit-v2/batches/{self.batch_id}/attempts/{attempt}/'

    def output_prefix(self, attempt, table, task_run):
        if table not in TABLES:
            raise ValueError('Unknown dataset')
        return self.attempt_root(attempt)+f'silver/{table}/{identifier(task_run)}/'

    def execution_input(self, attempt):
        return dict(self.data, attempt=attempt, manifest_sha256=self.fingerprint,
                    output_root=self.attempt_root(attempt))

    def output_manifest(self, attempt, outputs):
        if set(outputs) != set(TABLES):
            raise ValueError('All six outputs must be committed together')
        for table, info in outputs.items():
            if set(info) != {'task_run', 'prefix', 'rows', 'objects'} or info['prefix'] != self.output_prefix(attempt, table, info['task_run']):
                raise ValueError('Output belongs to another batch/attempt/task run')
            if type(info['rows']) is not int or info['rows'] < 0 or not info['objects']:
                raise ValueError('Invalid output count or missing Parquet objects')
            keys = set()
            for obj in info['objects']:
                if set(obj) != {'key', 'version_id', 'sha256', 'bytes'}:
                    raise ValueError('Output objects must pin key/version/checksum/size')
                key = obj['key']
                if not key.startswith(info['prefix']) or not key.endswith('.parquet') or '..' in key.split('/') or key in keys:
                    raise ValueError('Duplicate or cross-task output object')
                keys.add(key)
                if not isinstance(obj['version_id'], str) or obj['version_id'] in ('', 'null'):
                    raise ValueError('Output version is required')
                if not isinstance(obj['sha256'], str) or not re.fullmatch(r'[a-f0-9]{64}', obj['sha256']):
                    raise ValueError('Output checksum is required')
                if type(obj['bytes']) is not int or obj['bytes'] < 1:
                    raise ValueError('Output size is required')
        return {'schema_version': 1, 'datasets': list(TABLES), 'batch_id': self.batch_id,
                'bucket': self.data['bucket'], 'load_date': self.data['load_date'], 'business_timezone': 'UTC',
                'attempt': attempt, 'input_sha256': self.fingerprint, 'outputs': json.loads(canonical(outputs))}

    def validate_output_manifest(self, attempt, document):
        expected = self.output_manifest(attempt, document['outputs'])
        if document != expected:
            raise ValueError('Output manifest has incorrect source lineage, date or attempt')
        return expected
