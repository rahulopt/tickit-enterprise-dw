"""Offline concurrency, durability and crash-recovery tests; no AWS requests."""
import copy
import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from tickit_control.admission import Admission, Conflict, Dispatcher, LostLease, Observation
from tickit_control.batch import Batch, TABLES
from tickit_control.ingress import enqueue_s3_event
from tickit_control.local import SQLiteQueue, SQLiteStore
from tickit_control.work import WorkTracker


def batch(name='batch-a', day='2026-10-29'):
    return Batch.parse({'schema_version': 1, 'datasets': list(TABLES), 'batch_id': name, 'bucket': 'test-immutable-bucket', 'load_date': day,
                        'business_timezone': 'UTC', 'files': {
                            table: {'key': f'tickit-v2/batches/{name}/bronze/{table}.csv',
                                    'version_id': 'version-1', 'sha256': hashlib.sha256(table.encode()).hexdigest(), 'rows': 1}
                            for table in TABLES}})


class Clock:
    value = 1000

    def __call__(self):
        return self.value

    def advance(self, seconds=121):
        self.value += seconds


class Pipeline:
    def __init__(self):
        self.lock = threading.Lock()
        self.runs = {}
        self.starts = []
        self.lose_response = False
        self.crash_before = False

    def ensure(self, name, payload):
        with self.lock:
            if name in self.runs:
                assert self.runs[name]['payload'] == payload
                return
            if self.crash_before:
                self.crash_before = False
                raise TimeoutError('Crash before starting')
            assert not any(run['status'] == 'RUNNING' or not run['quiet'] for run in self.runs.values()), 'Conflicting executions started'
            self.runs[name] = {'payload': copy.deepcopy(payload), 'status': 'RUNNING', 'quiet': False}
            self.starts.append(name)
            if self.lose_response:
                self.lose_response = False
                raise TimeoutError('Start accepted; response lost')

    def observe(self, name, payload):
        with self.lock:
            run = self.runs[name]
            assert run['payload'] == payload
            return Observation(run['status'], run['quiet'])

    def finish(self, item, status='SUCCEEDED', quiet=True, attempt=1):
        with self.lock:
            self.runs[item.execution_name(attempt)].update(status=status, quiet=quiet)


@pytest.fixture
def system(tmp_path):
    clock = Clock()
    store = SQLiteStore(tmp_path/'ledger.db')
    queue = SQLiteQueue(tmp_path/'queue.db', clock)
    pipeline = Pipeline()
    admission = Admission(store, pipeline, clock)
    return admission, queue, pipeline, clock


def test_simultaneous_arrivals_start_one_and_preserve_second(system):
    admission, queue, pipeline, clock = system
    worker = Dispatcher(admission, queue)
    gate = threading.Barrier(2)
    def arrive(item):
        gate.wait()
        return worker.enqueue(item.data)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(arrive, [batch(), batch('batch-b', '2026-10-30')]))
    assert queue.count() == 2
    assert worker.poll('owner-a') == 'RUNNING'
    assert queue.count() == 1
    assert worker.poll('other-owner') == 'BUSY'
    assert len(pipeline.starts) == 1
    active = Batch.parse(admission.store.get(admission.batch_key(admission.store.get(admission.lock_key).data['batch_id'])).data['manifest'])
    pipeline.finish(active)
    clock.advance()
    assert worker.poll('recovery') == 'SUCCEEDED'
    assert worker.poll('next-owner') == 'RUNNING'
    assert len(pipeline.starts) == 2
    assert queue.count() == 0


def test_atomic_admission_even_if_two_consumers_receive_distinct_messages(system):
    admission, _, pipeline, _ = system
    gate = threading.Barrier(2)
    def claim(item):
        gate.wait()
        return admission.accept(item, owner=item.batch_id)
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(claim, [batch(), batch('batch-b')]))
    assert sorted(outcomes) == ['BUSY', 'RUNNING']
    assert len(pipeline.starts) == 1
    records = [admission.store.get(admission.batch_key(key)).data['state'] for key in ('batch-a', 'batch-b')]
    assert sorted(records) == ['ACTIVE', 'QUEUED']


def test_duplicate_events_dedup_and_ledger_survives_five_minute_window(system):
    admission, queue, pipeline, clock = system
    worker = Dispatcher(admission, queue)
    assert worker.enqueue(batch().data) == worker.enqueue(batch().data)
    assert queue.count() == 1
    worker.poll('owner')
    pipeline.finish(batch())
    admission.tick('owner')
    clock.advance(301)
    worker.enqueue(batch().data)
    assert worker.poll('later') == 'DUPLICATE'
    assert len(pipeline.starts) == 1
    assert queue.count() == 0


@pytest.mark.parametrize('crash', ['lose_response', 'crash_before'])
def test_expired_lease_recovers_same_batch_without_double_start(system, crash):
    admission, queue, pipeline, clock = system
    worker = Dispatcher(admission, queue)
    worker.enqueue(batch().data)
    worker.enqueue(batch('batch-b').data)
    setattr(pipeline, crash, True)
    with pytest.raises(TimeoutError):
        worker.poll('old-owner')
    assert queue.count() == 2
    assert admission.accept(batch('batch-b'), 'new-b') == 'BUSY'
    clock.advance()
    assert admission.tick('new-owner') == 'RUNNING'
    assert pipeline.starts == [batch().execution_name(1)]
    with pytest.raises(LostLease):
        admission._drive('old-owner')
    assert queue.count() == 2  # Lost-response receipt was never acknowledged.


def test_running_expired_lease_never_admits_another_batch(system):
    admission, _, pipeline, clock = system
    admission.accept(batch(), 'old')
    clock.advance(9999)
    assert admission.accept(batch('batch-b'), 'new') == 'BUSY'
    assert admission.tick('recovered') == 'RUNNING'
    assert len(pipeline.starts) == 1


def test_failure_waits_for_children_then_blocks_later_dates(system):
    admission, queue, pipeline, clock = system
    worker = Dispatcher(admission, queue)
    worker.enqueue(batch().data)
    worker.enqueue(batch('batch-b', '2026-10-30').data)
    worker.poll('owner')
    pipeline.finish(batch(), status='FAILED', quiet=False)
    assert worker.poll('owner') == 'DRAINING'
    clock.advance()
    assert worker.poll('recovery') == 'DRAINING'
    assert queue.count() == 1
    pipeline.finish(batch(), status='FAILED', quiet=True)
    assert worker.poll('recovery') == 'BLOCKED'
    clock.advance()
    assert worker.poll('new-recovery') == 'BLOCKED'
    assert len(pipeline.starts) == 1
    assert queue.count() == 1
    assert admission.store.get(admission.batch_key('batch-a')).data['state'] == 'FAILED'


def test_explicit_retry_has_new_attempt_but_same_sources(system):
    admission, _, pipeline, _ = system
    admission.accept(batch(), 'owner')
    pipeline.finish(batch(), status='FAILED')
    assert admission.tick('owner') == 'BLOCKED'
    assert admission.resolve_failed('owner', 'retry', 'Investigated transient failure') == 'RUNNING'
    assert len(pipeline.starts) == 2
    assert pipeline.runs[batch().execution_name(2)]['payload']['files'] == batch().data['files']
    assert pipeline.runs[batch().execution_name(2)]['payload']['output_root'] != pipeline.runs[batch().execution_name(1)]['payload']['output_root']
    assert admission.store.get(admission.batch_key('batch-a')).data['previous_attempts'][0]['attempt'] == 1


def test_stale_owner_cannot_release_and_abandon_requires_reason(system):
    admission, _, pipeline, clock = system
    admission.accept(batch(), 'old')
    pipeline.finish(batch(), status='FAILED')
    admission.tick('old')
    clock.advance()
    admission.tick('new')
    with pytest.raises(LostLease):
        admission.resolve_failed('old', 'abandon', 'Stale owner')
    with pytest.raises(ValueError):
        admission.resolve_failed('new', 'abandon', '')
    assert admission.resolve_failed('new', 'abandon', 'Approved missing-day recovery') == 'ABANDONED'
    assert admission.accept(batch('batch-b'), 'next') == 'RUNNING'


def test_retry_rechecks_quiescence(system):
    admission, _, pipeline, _ = system
    admission.accept(batch(), 'owner')
    pipeline.finish(batch(), status='FAILED')
    admission.tick('owner')
    pipeline.finish(batch(), status='FAILED', quiet=False)
    with pytest.raises(Conflict, match='Outstanding'):
        admission.resolve_failed('owner', 'retry', 'Not safe yet')
    assert len(pipeline.starts) == 1


def test_disk_queue_and_ledger_survive_dispatcher_restart(system):
    admission, queue, pipeline, clock = system
    worker = Dispatcher(admission, queue)
    worker.enqueue(batch().data)
    worker.enqueue(batch('batch-b').data)
    worker.poll('old')
    restarted = Admission(SQLiteStore(admission.store.filename), pipeline, clock)
    durable_queue = SQLiteQueue(queue.filename, clock)
    assert durable_queue.count() == 1
    clock.advance()
    assert Dispatcher(restarted, durable_queue).poll('new') == 'RUNNING'
    assert len(pipeline.starts) == 1


def test_reused_batch_id_cannot_change_manifest(system):
    admission, _, _, _ = system
    admission.accept(batch(), 'owner')
    changed = batch().data
    changed['files']['users']['sha256'] = 'a'*64
    with pytest.raises(Conflict, match='reused'):
        admission.accept(Batch.parse(changed), 'retry')


def test_queue_send_failure_is_not_reported_as_accepted(system):
    admission, _, _, _ = system
    class BrokenQueue:
        def send(self, *args):
            raise TimeoutError('No durable acknowledgment')
    with pytest.raises(TimeoutError):
        Dispatcher(admission, BrokenQueue()).enqueue(batch().data)


def test_old_queue_receipt_cannot_delete_redelivered_message(system):
    _, queue, _, clock = system
    queue.send(batch(), 'tickit-v2')
    first = queue.receive()
    clock.advance()
    second = queue.receive()
    queue.ack(first['receipt'])
    assert queue.count() == 1
    queue.ack(second['receipt'])
    assert queue.count() == 0


def test_work_tracker_fails_closed_on_unknown_submissions_and_stale_attempt(system):
    admission, _, pipeline, _ = system
    admission.accept(batch(), 'owner')
    tracker = WorkTracker(admission.store)
    assert tracker.inventory('batch-a', 1) is None
    tracker.reserve('batch-a', 1, 'sales-job', 'glue')
    with pytest.raises(Conflict, match='already reserved'):
        tracker.reserve('batch-a', 1, 'sales-job', 'glue')
    tracker.seal('batch-a', 1)
    assert not tracker.inventory('batch-a', 1)['operations']['sales-job']['resolved']
    with pytest.raises(Conflict, match='sealed'):
        tracker.reserve('batch-a', 1, 'another-job', 'glue')
    tracker.resolve('batch-a', 1, 'sales-job', {'job_name': 'test-sales', 'run_id': 'run-1'})
    with pytest.raises(Conflict, match='changed'):
        tracker.resolve('batch-a', 1, 'sales-job', {'run_id': 'run-2'})
    with pytest.raises(Conflict, match='stale'):
        tracker.reserve('batch-a', 2, 'another-job', 'glue')
    pipeline.finish(batch(), status='FAILED')
    admission.tick('owner')
    admission.resolve_failed('owner', 'retry', 'Retry approved')
    assert tracker.inventory('batch-a', 2) is None
    with pytest.raises(Conflict):
        tracker.resolve('batch-a', 1, 'sales-job', {'run_id': 'old'})


def test_event_ingress_reads_pinned_manifest_and_queues_every_record(system):
    admission, queue, _, _ = system
    worker = Dispatcher(admission, queue)
    reads = []
    def loader(bucket, key, version):
        reads.append((key, version))
        return batch('batch-a' if 'batch-a/' in key else 'batch-b').document
    records = [{'s3': {'bucket': {'name': batch().data['bucket']},
                       'object': {'key': f'tickit-v2/batches/{name}/input-manifest.json', 'versionId': 'pinned-1'}}}
               for name in ['batch-a', 'batch-b']]
    result = enqueue_s3_event({'Records': records}, batch().data['bucket'], loader, worker)
    assert len(result) == queue.count() == len(reads) == 2
    assert all(version == 'pinned-1' for _, version in reads)


@pytest.mark.parametrize('change', ['legacy', 'version', 'bucket'])
def test_ingress_rejects_mutable_or_cross_bucket_markers(system, change):
    admission, queue, _, _ = system
    event = {'Records': [{'s3': {'bucket': {'name': batch().data['bucket']},
                                 'object': {'key': 'tickit-v2/batches/batch-a/input-manifest.json', 'versionId': '1'}}}]}
    source = event['Records'][0]['s3']
    if change == 'legacy':
        source['object']['key'] = 'bronze/.done'
    elif change == 'version':
        source['object'].pop('versionId')
    else:
        source['bucket']['name'] = 'wrong-bucket'
    with pytest.raises(ValueError):
        enqueue_s3_event(event, batch().data['bucket'], lambda *args: batch().document, Dispatcher(admission, queue))
    assert queue.count() == 0


def test_input_and_output_namespaces_are_batch_attempt_and_task_specific():
    item = batch()
    paths = {item.output_prefix(1, 'sales', 'task-1'), item.output_prefix(1, 'sales', 'task-2'),
             item.output_prefix(2, 'sales', 'task-1'), batch('batch-b').output_prefix(1, 'sales', 'task-1')}
    assert len(paths) == 4
    original = item.document
    copy = item.data
    copy['files']['sales']['key'] = 'silver/sales/latest.parquet'
    assert item.document == original
    with pytest.raises(ValueError, match='Shared'):
        Batch.parse(copy)
    with pytest.raises(ValueError):
        item.output_prefix(1, 'sales', '../escape')


def test_outbox_recovers_crash_between_registration_and_send(system):
    admission, queue, pipeline, clock = system
    class FailsOnce:
        def send(self, *args):
            raise TimeoutError('Sender crashed before SQS acknowledgment')
    with pytest.raises(TimeoutError):
        Dispatcher(admission, FailsOnce()).enqueue(batch().data)
    assert queue.count() == 0
    assert admission.store.get(admission.batch_key('batch-a')).data['state'] == 'QUEUED'
    restarted = Admission(SQLiteStore(admission.store.filename), pipeline, clock)
    worker = Dispatcher(restarted, queue)
    assert worker.reconcile() == 1
    assert worker.poll('owner') == 'RUNNING'


def test_outbox_can_restore_an_expired_sqs_delivery(system):
    admission, queue, _, clock = system
    worker = Dispatcher(admission, queue)
    worker.enqueue(batch().data)
    message = queue.receive()
    queue.ack(message['receipt'])  # Model SQS retention expiry/lost transport copy.
    assert queue.count() == 0
    clock.advance(301)
    assert worker.reconcile() == 1
    assert queue.count() == 1
    worker.poll('owner')
    assert worker.reconcile() == 0  # Active work is no longer a pending outbox record.


def test_no_operator_release_of_a_running_pipeline(system):
    admission, _, _, _ = system
    admission.accept(batch(), 'owner')
    with pytest.raises(Conflict, match='Only a failed'):
        admission.resolve_failed('owner', 'abandon', 'Unsafe attempt')


def test_stale_dispatcher_start_after_recovery_and_next_batch_is_idempotent(system):
    admission, _, pipeline, clock = system
    admission.accept(batch(), 'old')
    first_payload = copy.deepcopy(pipeline.runs[batch().execution_name(1)]['payload'])
    pipeline.finish(batch())
    clock.advance()
    admission.tick('recovery')
    admission.accept(batch('batch-b'), 'next')
    # Model an old RPC delayed between the last ownership check and StartExecution.
    pipeline.ensure(batch().execution_name(1), first_payload)
    assert len(pipeline.starts) == 2
    assert sum(run['status'] == 'RUNNING' for run in pipeline.runs.values()) == 1


def test_simultaneous_duplicate_trigger_events_create_one_durable_batch(system):
    admission, queue, pipeline, _ = system
    worker = Dispatcher(admission, queue)
    gate = threading.Barrier(8)
    def duplicate(_):
        gate.wait()
        return worker.enqueue(batch().data)
    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(duplicate, range(8)))
    assert len(set(receipts)) == 1
    assert queue.count() == 1
    worker.poll('owner')
    assert len(pipeline.starts) == 1


def test_committed_admission_with_lost_dynamo_response_is_recovered(system):
    admission, queue, pipeline, clock = system
    worker = Dispatcher(admission, queue)
    worker.enqueue(batch().data)
    original = admission.store.cas
    lose_once = [True]
    def uncertain(changes):
        result = original(changes)
        if len(changes) == 2 and result and lose_once[0]:
            lose_once[0] = False
            raise TimeoutError('Transaction committed but response was lost')
        return result
    admission.store.cas = uncertain
    with pytest.raises(TimeoutError):
        worker.poll('old')
    assert not pipeline.starts
    assert queue.count() == 1
    clock.advance()
    assert admission.tick('new') == 'RUNNING'
    assert len(pipeline.starts) == 1


def test_receiving_batch_b_after_another_worker_admits_a_does_not_ack_b(system):
    admission, queue, pipeline, clock = system
    worker = Dispatcher(admission, queue)
    worker.enqueue(batch().data)
    worker.enqueue(batch('batch-b').data)
    real_receive = queue.receive
    def raced_receive():
        a = real_receive()
        assert admission.accept(batch(), 'other') == 'RUNNING'
        queue.ack(a['receipt'])
        return real_receive()
    queue.receive = raced_receive
    assert worker.poll('loser') == 'BUSY'
    assert queue.count() == 1
    assert len(pipeline.starts) == 1
    queue.receive = real_receive
    pipeline.finish(batch())
    clock.advance()
    worker.poll('recovery')
    assert worker.poll('next') == 'RUNNING'
    assert queue.count() == 0
