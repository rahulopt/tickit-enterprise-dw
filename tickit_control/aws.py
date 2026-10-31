"""AWS adapters, never activated at import. Constructors require injected clients.

These classes are undeployed and tested with stubs only. No credentials,
resource creation, polling schedule or production configuration is installed.
"""
import base64
import hashlib
import json
import re
import time
import uuid

from .admission import Conflict, Observation, Record
from .batch import Batch, canonical


def error_code(exc):
    return getattr(exc, 'response', {}).get('Error', {}).get('Code')


class DynamoStore:
    def __init__(self, client, table_name, queue_index="queued-batches"):
        self.client, self.table_name, self.queue_index = client, table_name, queue_index

    def get(self, key):
        item = self.client.get_item(TableName=self.table_name, Key={'pk': {'S': key}}, ConsistentRead=True).get('Item')
        return Record(int(item['revision']['N']), json.loads(item['data']['S'])) if item else None

    def queued(self, pipeline_id, limit=10):
        response = self.client.query(TableName=self.table_name, IndexName=self.queue_index,
                                     KeyConditionExpression='#partition = :partition',
                                     ExpressionAttributeNames={'#partition': 'queue_partition'},
                                     ExpressionAttributeValues={':partition': {'S': pipeline_id+'#QUEUED'}},
                                     Limit=limit)
        return [Record(int(item['revision']['N']), json.loads(item['data']['S']))
                for item in response.get('Items', [])]

    def cas(self, changes):
        actions = []
        for key, (expected, data) in changes.items():
            revision = 1 if expected is None else expected.revision+1
            item = {'pk': {'S': key}, 'revision': {'N': str(revision)}, 'data': {'S': canonical(data)}}
            for name in ('owner', 'batch_id'):
                if name in data:
                    item[name] = {'S': data[name]}
            if 'expires_at' in data:
                item['expires_at'] = {'N': str(data['expires_at'])}
            if key.startswith('BATCH#') and data.get('state') == 'QUEUED':
                item['queue_partition'] = {'S': key.split('#', 2)[1]+'#QUEUED'}
                item['queue_sort'] = {'S': data['manifest']['load_date']+'#'+data['manifest']['batch_id']}
            put = {'TableName': self.table_name, 'Item': item,
                   'ConditionExpression': 'attribute_not_exists(pk)' if expected is None else '#rev = :rev'}
            if expected is not None:
                put['ExpressionAttributeNames'] = {'#rev': 'revision'}
                put['ExpressionAttributeValues'] = {':rev': {'N': str(expected.revision)}}
                for name in ('owner', 'batch_id'):
                    if name in expected.data:
                        put['ConditionExpression'] += f' AND #{name} = :{name}'
                        put['ExpressionAttributeNames']['#'+name] = name
                        put['ExpressionAttributeValues'][':'+name] = {'S': expected.data[name]}
            actions.append({'Put': put})
        try:
            self.client.transact_write_items(TransactItems=actions, ClientRequestToken=uuid.uuid4().hex)
            return True
        except Exception as exc:
            if error_code(exc) == 'TransactionCanceledException':
                reasons = exc.response.get('CancellationReasons', [])
                codes = [reason.get('Code', 'None') for reason in reasons]
                if codes and all(code in ('None', 'ConditionalCheckFailed', 'TransactionConflict') for code in codes):
                    return False
            raise  # Unknown/operational errors must not acknowledge the queue.


class SqsQueue:
    def __init__(self, client, queue_url, visibility_seconds=60):
        if not queue_url.endswith('.fifo'):
            raise ValueError('A dedicated FIFO queue is required')
        if not 1 <= visibility_seconds <= 43200:
            raise ValueError('Invalid SQS visibility timeout')
        self.client, self.url, self.visibility = client, queue_url, visibility_seconds

    def send(self, batch, group):
        return self.client.send_message(QueueUrl=self.url, MessageBody=batch.document,
                                        MessageGroupId=group, MessageDeduplicationId=batch.fingerprint)['MessageId']

    def receive(self):
        result = self.client.receive_message(QueueUrl=self.url, MaxNumberOfMessages=1,
                                             WaitTimeSeconds=0, VisibilityTimeout=self.visibility)
        messages = result.get('Messages', [])
        if not messages:
            return None
        message = messages[0]
        return {'manifest': json.loads(message['Body']), 'receipt': message['ReceiptHandle']}

    def ack(self, receipt):
        self.client.delete_message(QueueUrl=self.url, ReceiptHandle=receipt)


class StandardPipeline:
    def __init__(self, client, state_machine_arn, work_tracker, glue, redshift, clock=time.time, seal_terminal=False):
        parts = state_machine_arn.split(':')
        if len(parts) != 7 or parts[2] != 'states' or parts[5] != 'stateMachine':
            raise ValueError('Use an unqualified Standard state machine ARN')
        self.client, self.arn, self.tracker = client, state_machine_arn, work_tracker
        self.glue, self.redshift = glue, redshift
        self._standard_verified = False
        self.clock = clock
        self.seal_terminal = seal_terminal

    def execution_arn(self, name):
        return self.arn.replace(':stateMachine:', ':execution:')+':'+name

    def _describe(self, name, payload):
        result = self.client.describe_execution(executionArn=self.execution_arn(name))
        if canonical(json.loads(result['input'])) != canonical(payload):
            raise Conflict('Execution name exists with different input')
        return result

    def ensure(self, name, payload):
        if not self._standard_verified:
            if self.client.describe_state_machine(stateMachineArn=self.arn)['type'] != 'STANDARD':
                raise Conflict('Express workflows cannot provide execution-name idempotency')
            self._standard_verified = True
        try:
            return self._describe(name, payload)
        except Exception as exc:
            if error_code(exc) != 'ExecutionDoesNotExist':
                raise
        if type(payload.get('start_before')) is not int or self.clock() >= payload['start_before']:
            raise Conflict('Start deadline expired with unknown execution state; manual reconciliation required')
        try:
            # Owner/lease tokens are deliberately excluded from stable payload.
            # Standard name+input idempotency closes the start-response-loss race.
            return self.client.start_execution(stateMachineArn=self.arn, name=name, input=canonical(payload))
        except Exception as exc:
            if error_code(exc) != 'ExecutionAlreadyExists':
                raise
            return self._describe(name, payload)

    def observe(self, name, payload):
        result = self._describe(name, payload)
        status = result['status']
        if status not in ('SUCCEEDED','FAILED','TIMED_OUT','ABORTED'):
            return Observation(status)
        inventory = self.tracker.inventory(payload['batch_id'], payload['attempt'])
        if self.seal_terminal and (not inventory or not inventory['sealed']):
            # Trusted watchdog has observed the exact execution terminal. Seal
            # atomically: any late controller reservation now fails closed.
            self.tracker.seal(payload['batch_id'], payload['attempt'])
            inventory = self.tracker.inventory(payload['batch_id'], payload['attempt'])
        if not inventory or not inventory['sealed']:
            return Observation(status, False, 'Submission registry is not sealed')
        expected = {'category', 'venue', 'users', 'event', 'listing', 'sales', 'warehouse'}
        if status == 'SUCCEEDED' and not expected.issubset(inventory['operations']):
            return Observation(status, False, 'Successful workflow has incomplete work registry')
        child_failed = False
        for operation in inventory['operations'].values():
            if not operation['resolved']:
                return Observation(status, False, 'A submission response is unresolved')
            ref = operation['reference']
            if operation['kind'] == 'glue':
                state = self.glue.get_job_run(JobName=ref['job_name'], RunId=ref['run_id'])['JobRun']['JobRunState']
                child_failed = child_failed or state != 'SUCCEEDED'
                if state not in ('SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT', 'ERROR', 'EXPIRED'):
                    return Observation(status, False, 'Glue work still active')
            elif operation['kind'] == 'sql':
                state = self.redshift.describe_statement(Id=ref['statement_id'])['Status']
                child_failed = child_failed or state != 'FINISHED'
                if state not in ('FINISHED', 'FAILED', 'ABORTED'):
                    return Observation(status, False, 'SQL work still active')
            elif ref.get('response_observed') is not True:
                return Observation(status, False, 'Lambda completion is not observed')
        if status == 'SUCCEEDED' and child_failed:
            return Observation('FAILED', True, 'Child service failed despite workflow success')
        return Observation(status, True)


class ImmutableObjects:
    def __init__(self, client, bucket):
        self.client, self.bucket = client, bucket

    def put(self, key, body):
        if not isinstance(body, bytes):
            raise TypeError('Immutable writes require bytes')
        if not re.fullmatch(r'tickit-v2/batches/[a-z0-9][a-z0-9-]{0,63}/[A-Za-z0-9_./-]+', key) or '..' in key.split('/'):
            raise ValueError('Only batch-specific immutable object paths are permitted')
        digest = hashlib.sha256(body).digest()
        try:
            return self.client.put_object(Bucket=self.bucket, Key=key, Body=body, IfNoneMatch='*',
                                          ChecksumSHA256=base64.b64encode(digest).decode(),
                                          Metadata={'sha256': digest.hex()})
        except Exception as exc:
            if error_code(exc) != 'PreconditionFailed':
                raise
            current = self.client.get_object(Bucket=self.bucket, Key=key)
            if current['Body'].read() != body:
                raise Conflict('Immutable object already exists with different bytes') from exc
            return {'VersionId': current.get('VersionId'), 'identical_retry': True}

    def read_input(self, batch, table):
        if batch.data['bucket'] != self.bucket:
            raise Conflict('Source bucket differs from isolated batch bucket')
        info = batch.data['files'][table]
        obj = self.client.get_object(Bucket=self.bucket, Key=info['key'], VersionId=info['version_id'])
        body = obj['Body'].read()
        if hashlib.sha256(body).hexdigest() != info['sha256']:
            raise Conflict('Pinned source bytes do not match committed manifest')
        return body

    def publish(self, batch, attempt, outputs):
        if batch.data['bucket'] != self.bucket:
            raise Conflict('Output bucket differs from isolated batch bucket')
        document = batch.output_manifest(attempt, outputs)
        return self.put(batch.attempt_root(attempt)+'output-manifest.json', canonical(document).encode())
