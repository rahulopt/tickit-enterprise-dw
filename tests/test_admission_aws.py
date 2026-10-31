"""SDK request contracts with Stubber/fakes only. Socket access is blocked in the run."""
import base64
import copy
import hashlib
import io
import json
from types import SimpleNamespace

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from tickit_control.admission import Conflict, Record
from tickit_control.aws import DynamoStore, ImmutableObjects, SqsQueue, StandardPipeline
from tickit_control.batch import TABLES, canonical
from test_admission import batch


def client(service):
    return boto3.client(service, region_name='us-east-1', aws_access_key_id='testing', aws_secret_access_key='testing')


def test_dynamo_atomic_lease_and_ledger_write_include_owner_and_revision_guards(monkeypatch):
    sdk = client('dynamodb')
    monkeypatch.setattr('tickit_control.aws.uuid.uuid4', lambda: SimpleNamespace(hex='test-transaction'))
    old = Record(3, {'owner': 'old', 'batch_id': 'batch-a', 'expires_at': 1000})
    lease = {'owner': 'new', 'batch_id': 'batch-a', 'expires_at': 1200}
    expected = {'TransactItems': [
        {'Put': {'TableName': 'test-admission', 'Item': {
            'pk': {'S': 'LOCK#test'}, 'revision': {'N': '4'}, 'data': {'S': canonical(lease)},
            'owner': {'S': 'new'}, 'batch_id': {'S': 'batch-a'}, 'expires_at': {'N': '1200'}},
            'ConditionExpression': '#rev = :rev AND #owner = :owner AND #batch_id = :batch_id',
            'ExpressionAttributeNames': {'#rev': 'revision', '#owner': 'owner', '#batch_id': 'batch_id'},
            'ExpressionAttributeValues': {':rev': {'N': '3'}, ':owner': {'S': 'old'}, ':batch_id': {'S': 'batch-a'}}}},
        {'Put': {'TableName': 'test-admission', 'Item': {'pk': {'S': 'BATCH#test#batch-a'},
                  'revision': {'N': '1'}, 'data': {'S': '{"state":"ACTIVE"}'}},
                 'ConditionExpression': 'attribute_not_exists(pk)'}}], 'ClientRequestToken': 'test-transaction'}
    with Stubber(sdk) as stub:
        stub.add_response('transact_write_items', {}, expected)
        assert DynamoStore(sdk, 'test-admission').cas({'LOCK#test': (old, lease), 'BATCH#test#batch-a': (None, {'state': 'ACTIVE'})})
        stub.assert_no_pending_responses()
    assert 'ttl' not in canonical(expected).lower()


def test_dynamo_reads_are_strongly_consistent():
    sdk = client('dynamodb')
    with Stubber(sdk) as stub:
        stub.add_response('get_item', {'Item': {'revision': {'N': '5'}, 'data': {'S': '{"state":"QUEUED"}'}}},
                          {'TableName': 'test-admission', 'Key': {'pk': {'S': 'batch'}}, 'ConsistentRead': True})
        assert DynamoStore(sdk, 'test-admission').get('batch') == Record(5, {'state': 'QUEUED'})


@pytest.mark.parametrize('code,reason,retryable', [('TransactionCanceledException', 'ConditionalCheckFailed', True),
                                                  ('AccessDeniedException', None, False),
                                                  ('TransactionCanceledException', 'ProvisionedThroughputExceeded', False)])
def test_dynamo_distinguishes_contention_from_operational_failure(code, reason, retryable):
    class Fake:
        def transact_write_items(self, **kwargs):
            response = {'Error': {'Code': code, 'Message': 'test'}}
            if reason:
                response['CancellationReasons'] = [{'Code': reason}]
            raise ClientError(response, 'TransactWriteItems')
    store = DynamoStore(Fake(), 'test-admission')
    if retryable:
        assert store.cas({'key': (None, {})}) is False
    else:
        with pytest.raises(ClientError):
            store.cas({'key': (None, {})})


def test_sqs_fifo_dedup_and_explicit_ack_contract():
    sdk = client('sqs')
    url = 'https://sqs.us-east-1.amazonaws.com/123456789012/test-batches.fifo'
    item = batch()
    with Stubber(sdk) as stub:
        stub.add_response('send_message', {'MessageId': 'message-1'}, {'QueueUrl': url,
                          'MessageBody': item.document, 'MessageGroupId': 'warehouse', 'MessageDeduplicationId': item.fingerprint})
        stub.add_response('receive_message', {'Messages': [{'Body': item.document, 'ReceiptHandle': 'receipt-1'}]},
                          {'QueueUrl': url, 'MaxNumberOfMessages': 1, 'WaitTimeSeconds': 0, 'VisibilityTimeout': 60})
        stub.add_response('delete_message', {}, {'QueueUrl': url, 'ReceiptHandle': 'receipt-1'})
        queue = SqsQueue(sdk, url)
        assert queue.send(item, 'warehouse') == 'message-1'
        message = queue.receive()
        assert message == {'manifest': item.data, 'receipt': 'receipt-1'}
        queue.ack(message['receipt'])
        stub.assert_no_pending_responses()
    with pytest.raises(ValueError, match='FIFO'):
        SqsQueue(sdk, 'not-a-fifo-url')


class Sfn:
    def __init__(self, workflow_type='STANDARD'):
        self.workflow_type = workflow_type
        self.runs = {}
        self.starts = []
        self.response_lost = False
        self.concurrent_start = False

    def describe_state_machine(self, **kwargs):
        return {'type': self.workflow_type}

    def describe_execution(self, executionArn):
        name = executionArn.split(':')[-1]
        if name not in self.runs:
            raise ClientError({'Error': {'Code': 'ExecutionDoesNotExist'}}, 'DescribeExecution')
        return self.runs[name]

    def start_execution(self, **kwargs):
        name = kwargs['name']
        self.runs[name] = {'status': 'RUNNING', 'input': kwargs['input']}
        if self.concurrent_start:
            raise ClientError({'Error': {'Code': 'ExecutionAlreadyExists'}}, 'StartExecution')
        self.starts.append(kwargs)
        if self.response_lost:
            self.response_lost = False
            raise TimeoutError('Accepted then disconnected')
        return {'executionArn': name}


class Registry:
    data = None

    def inventory(self, *args):
        return self.data


class Glue:
    state = 'RUNNING'

    def get_job_run(self, **kwargs):
        return {'JobRun': {'JobRunState': self.state}}


class Redshift:
    state = 'STARTED'

    def describe_statement(self, **kwargs):
        return {'Status': self.state}


def adapter(sfn=None, registry=None, glue=None, redshift=None):
    return StandardPipeline(sfn or Sfn(), 'arn:aws:states:us-east-1:123456789012:stateMachine:test-admission',
                            registry or Registry(), glue or Glue(), redshift or Redshift(), clock=lambda: 1000)


def payload():
    return dict(batch().execution_input(1), start_before=1600)


@pytest.mark.parametrize('lost_response,concurrent_start', [(True, False), (False, True)])
def test_standard_names_close_ambiguous_start_race(lost_response, concurrent_start):
    sdk = Sfn()
    sdk.response_lost, sdk.concurrent_start = lost_response, concurrent_start
    pipe = adapter(sdk)
    name, data = batch().execution_name(1), payload()
    if lost_response:
        with pytest.raises(TimeoutError):
            pipe.ensure(name, data)
    else:
        pipe.ensure(name, data)
    pipe.ensure(name, data)
    assert len(sdk.runs) == 1
    assert len(sdk.starts) == (1 if lost_response else 0)
    data['load_date'] = '2026-10-30'
    with pytest.raises(Conflict, match='different input'):
        pipe.ensure(name, data)


def test_express_cannot_bypass_admission_guarantees():
    sdk = Sfn('EXPRESS')
    with pytest.raises(Conflict, match='Express'):
        adapter(sdk).ensure(batch().execution_name(1), payload())
    assert not sdk.starts


def test_unknown_expired_start_intent_is_quarantined_not_restarted():
    sdk = Sfn()
    with pytest.raises(Conflict, match='deadline'):
        adapter(sdk).ensure(batch().execution_name(1), dict(payload(), start_before=900))
    assert not sdk.starts
    sdk.runs[batch().execution_name(1)] = {'status': 'RUNNING', 'input': canonical(dict(payload(), start_before=900))}
    adapter(sdk).ensure(batch().execution_name(1), dict(payload(), start_before=900))
    assert not sdk.starts


def test_terminal_sfn_is_not_enough_without_sealed_resolved_external_work():
    sdk, registry, glue, redshift = Sfn(), Registry(), Glue(), Redshift()
    pipe = adapter(sdk, registry, glue, redshift)
    name, data = batch().execution_name(1), payload()
    sdk.runs[name] = {'status': 'FAILED', 'input': canonical(data)}
    assert not pipe.observe(name, data).quiescent
    registry.data = {'sealed': True, 'operations': {'job': {'kind': 'glue', 'resolved': False}}}
    assert not pipe.observe(name, data).quiescent
    registry.data['operations']['job'].update(resolved=True, reference={'job_name': 'test-sales', 'run_id': 'run-1'})
    assert not pipe.observe(name, data).quiescent
    glue.state = 'STOPPED'
    registry.data['operations']['sql'] = {'kind': 'sql', 'resolved': True, 'reference': {'statement_id': 'statement-1'}}
    assert not pipe.observe(name, data).quiescent
    redshift.state = 'ABORTED'
    registry.data['operations']['lambda'] = {'kind': 'lambda', 'resolved': False}
    assert not pipe.observe(name, data).quiescent
    registry.data['operations']['lambda'].update(resolved=True, reference={'response_observed': True})
    assert pipe.observe(name, data).quiescent


def test_success_does_not_allow_an_empty_registry():
    sdk, registry = Sfn(), Registry()
    name, data = batch().execution_name(1), payload()
    sdk.runs[name] = {'status': 'SUCCEEDED', 'input': canonical(data)}
    registry.data = {'sealed': True, 'operations': {}}
    assert not adapter(sdk, registry).observe(name, data).quiescent


def test_immutable_objects_accept_identical_retry_but_never_overwrite():
    sdk = client('s3')
    objects = ImmutableObjects(sdk, 'test-immutable-bucket')
    key, body = batch().data['files']['sales']['key'], b'sales'
    expected = {'Bucket': 'test-immutable-bucket', 'Key': key, 'Body': body, 'IfNoneMatch': '*',
                'ChecksumSHA256': base64.b64encode(hashlib.sha256(body).digest()).decode(),
                'Metadata': {'sha256': hashlib.sha256(body).hexdigest()}}
    with Stubber(sdk) as stub:
        stub.add_response('put_object', {'VersionId': 'version-1'}, expected)
        stub.add_client_error('put_object', 'PreconditionFailed', expected_params=expected)
        stub.add_response('get_object', {'Body': io.BytesIO(body), 'VersionId': 'version-1'},
                          {'Bucket': 'test-immutable-bucket', 'Key': key})
        assert objects.put(key, body)['VersionId'] == 'version-1'
        assert objects.put(key, body)['identical_retry']
        stub.add_client_error('put_object', 'PreconditionFailed', expected_params=expected)
        stub.add_response('get_object', {'Body': io.BytesIO(b'different')}, {'Bucket': 'test-immutable-bucket', 'Key': key})
        with pytest.raises(Conflict, match='different bytes'):
            objects.put(key, body)
    with pytest.raises(ValueError, match='batch-specific'):
        objects.put('tickit-v2/silver/sales/latest.parquet', b'data')


def test_source_reads_pin_version_and_verify_checksum():
    sdk = client('s3')
    item = batch()
    info = item.data['files']['sales']
    with Stubber(sdk) as stub:
        expected = {'Bucket': item.data['bucket'], 'Key': info['key'], 'VersionId': info['version_id']}
        stub.add_response('get_object', {'Body': io.BytesIO(b'sales')}, expected)
        stub.add_response('get_object', {'Body': io.BytesIO(b'corrupt')}, expected)
        reader = ImmutableObjects(sdk, item.data['bucket'])
        assert reader.read_input(item, 'sales') == b'sales'
        with pytest.raises(Conflict, match='Pinned source'):
            reader.read_input(item, 'sales')


def test_output_manifest_cannot_reference_other_attempt_or_task():
    item = batch()
    outputs = {table: {'task_run': 'task-1', 'prefix': item.output_prefix(1, table, 'task-1'), 'rows': 1,
                       'objects': [{'key': item.output_prefix(1, table, 'task-1')+'part-0.parquet', 'version_id':'v1', 'sha256':'a'*64, 'bytes':10}]} for table in TABLES}
    assert item.output_manifest(1, outputs)['input_sha256'] == item.fingerprint
    outputs['sales']['prefix'] = item.output_prefix(2, 'sales', 'task-1')
    with pytest.raises(ValueError, match='another batch'):
        item.output_manifest(1, outputs)


def test_pending_outbox_index_is_not_used_as_a_lock():
    sdk = client('dynamodb')
    with Stubber(sdk) as stub:
        stub.add_response('query', {'Items': [{'revision': {'N': '1'}, 'data': {'S': '{"state":"QUEUED"}'}}]},
                          {'TableName': 'test-admission', 'IndexName': 'queued-batches',
                           'KeyConditionExpression': '#partition = :partition',
                           'ExpressionAttributeNames': {'#partition': 'queue_partition'},
                           'ExpressionAttributeValues': {':partition': {'S': 'warehouse#QUEUED'}}, 'Limit': 10})
        assert DynamoStore(sdk, 'test-admission').queued('warehouse')[0].data['state'] == 'QUEUED'


def test_successful_state_with_failed_child_blocks_instead_of_reporting_success():
    sdk, registry, glue, redshift = Sfn(), Registry(), Glue(), Redshift()
    name, data = batch().execution_name(1), payload()
    sdk.runs[name] = {'status': 'SUCCEEDED', 'input': canonical(data)}
    registry.data = {'sealed': True, 'operations': {
        op: {'kind': 'lambda', 'resolved': True, 'reference': {'response_observed': True}}
        for op in ('category', 'venue')}}
    registry.data['operations'].update({op: {'kind': 'glue', 'resolved': True,
                                             'reference': {'job_name': op, 'run_id': op+'-1'}}
                                       for op in ('users', 'event', 'listing', 'sales')})
    registry.data['operations']['warehouse'] = {'kind': 'sql', 'resolved': True, 'reference': {'statement_id': 'sql-1'}}
    glue.state, redshift.state = 'FAILED', 'FINISHED'
    result = adapter(sdk, registry, glue, redshift).observe(name, data)
    assert result.status == 'FAILED' and result.quiescent
    glue.state = 'SUCCEEDED'
    result = adapter(sdk, registry, glue, redshift).observe(name, data)
    assert result.status == 'SUCCEEDED' and result.quiescent


def test_immutable_manifest_publication_is_write_once():
    class Sink:
        calls = []
        def put_object(self, **kwargs):
            self.calls.append(kwargs)
            return {'VersionId': 'commit-1'}
    item = batch()
    outputs = {table: {'task_run': 'task-1', 'prefix': item.output_prefix(1, table, 'task-1'), 'rows': 1,
                       'objects': [{'key': item.output_prefix(1, table, 'task-1')+'part-0.parquet', 'version_id':'v1', 'sha256':'a'*64, 'bytes':10}]} for table in TABLES}
    sdk = Sink()
    ImmutableObjects(sdk, item.data['bucket']).publish(item, 1, outputs)
    assert sdk.calls[0]['IfNoneMatch'] == '*'
    assert sdk.calls[0]['Key'] == 'tickit-v2/batches/batch-a/attempts/1/output-manifest.json'
    with pytest.raises(Conflict, match='bucket'):
        ImmutableObjects(sdk, 'other-bucket').publish(item, 1, outputs)
