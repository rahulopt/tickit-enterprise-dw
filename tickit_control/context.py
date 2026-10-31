"""Manifest-scoped execution identity and attempt fencing shared by all branches."""
import json
from dataclasses import dataclass

from .admission import LostLease
from .batch import Batch, identifier

MANIFEST_FIELDS = ('schema_version','datasets','batch_id','bucket','load_date','business_timezone','files')


@dataclass(frozen=True)
class Context:
    batch: Batch
    attempt: int
    epoch: int
    task_run: str

    @classmethod
    def parse(cls, request):
        batch = Batch.parse({key: request[key] for key in MANIFEST_FIELDS})
        attempt, epoch = request['attempt'], request['fence_epoch']
        batch.execution_name(attempt)
        if type(epoch) is not int or epoch < 1 or request['manifest_sha256'] != batch.fingerprint:
            raise ValueError('Execution has invalid source fingerprint or fencing epoch')
        if request['output_root'] != batch.attempt_root(attempt):
            raise ValueError('Execution output namespace does not match its attempt')
        return cls(batch, attempt, epoch, identifier(request['task_run_id']))


def job_request(argv=None):
    import argparse
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--BATCH_REQUEST')
    parser.add_argument('--ADMISSION_TABLE')
    parser.add_argument('--TICKIT_PIPELINE_ID')
    parsed, _ = parser.parse_known_args(argv)
    import os
    if parsed.ADMISSION_TABLE:
        os.environ['ADMISSION_TABLE'] = parsed.ADMISSION_TABLE
    if parsed.TICKIT_PIPELINE_ID:
        os.environ['TICKIT_PIPELINE_ID'] = parsed.TICKIT_PIPELINE_ID
    return json.loads(parsed.BATCH_REQUEST) if parsed.BATCH_REQUEST else None


class AttemptFence:
    def __init__(self, store, pipeline_id, context):
        self.store, self.scope, self.context = store, pipeline_id, context

    def check(self):
        item = self.context
        lock = self.store.get('LOCK#'+self.scope)
        record = self.store.get(f'BATCH#{self.scope}#{item.batch.batch_id}')
        if (not lock or lock.data.get('batch_id') != item.batch.batch_id or not record
                or record.data['state'] != 'ACTIVE' or record.data['attempt'] != item.attempt
                or record.data.get('fence_epoch') != item.epoch
                or record.data['fingerprint'] != item.batch.fingerprint):
            raise LostLease('Stale or inactive workflow attempt cannot read/publish/load batch data')
        # Coordinator owner/expiry can change while the SAME workflow is running.
        # Workflow fencing follows the immutable attempt epoch, not coordinator UUID.
        return record


def runtime_fence(context):
    import os
    import boto3
    from .aws import DynamoStore
    return AttemptFence(DynamoStore(boto3.client('dynamodb'), os.environ['ADMISSION_TABLE']),
                        os.environ['TICKIT_PIPELINE_ID'], context)
