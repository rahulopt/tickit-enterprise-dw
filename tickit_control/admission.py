"""Recoverable single-warehouse admission. No SDK imports or automatic AWS calls.

Store CAS atomically updates the global lease and durable batch ledger. Expiry
allows takeover of the SAME batch, never admission of a conflicting one. A
watchdog must call tick even when SQS is empty. Execution names/inputs are stable
across takeover. Failed executions block the stream until explicit resolution.
"""
import uuid
from dataclasses import dataclass

from .batch import Batch, identifier


class Conflict(RuntimeError):
    pass


class LostLease(Conflict):
    pass


@dataclass(frozen=True)
class Record:
    revision: int
    data: dict


@dataclass(frozen=True)
class Observation:
    status: str
    quiescent: bool = False
    reason: str = ''


class Admission:
    def __init__(self, store, pipeline, clock, pipeline_id='tickit-v2', lease_seconds=120):
        if lease_seconds < 10:
            raise ValueError('Lease must allow time for heartbeats')
        self.store, self.pipeline, self.clock = store, pipeline, clock
        self.pipeline_id, self.lease_seconds = identifier(pipeline_id), lease_seconds
        self.lock_key = 'LOCK#'+pipeline_id

    def batch_key(self, batch_id):
        return f'BATCH#{self.pipeline_id}#{batch_id}'

    def register(self, batch):
        key = self.batch_key(batch.batch_id)
        record = self.store.get(key)
        if record is None:
            self.store.cas({key: (None, {'manifest': batch.data, 'fingerprint': batch.fingerprint,
                                        'state': 'QUEUED', 'attempt': 0})})
            record = self.store.get(key)
        if record is None:
            raise Conflict('Registration raced; retain SQS message for retry')
        if record.data['fingerprint'] != batch.fingerprint:
            raise Conflict('Batch ID reused with different immutable inputs')
        return record

    def accept(self, batch, owner=None):
        owner = owner or uuid.uuid4().hex
        record = self.register(batch)
        if record.data['state'] in ('SUCCEEDED', 'ABANDONED'):
            return 'DUPLICATE'
        lock = self.store.get(self.lock_key)
        if lock and lock.data.get('batch_id'):
            if lock.data['batch_id'] != batch.batch_id:
                return 'BUSY'  # Caller must NOT acknowledge/delete this delivery.
            return self.tick(owner)
        if record.data['state'] != 'QUEUED':
            raise Conflict('Ledger/lease inconsistency; fail closed')
        active = dict(record.data, state='ACTIVE', attempt=1, start_before=int(self.clock())+600,
                      fence_epoch=(lock.revision if lock else 0)+1)
        lease = {'batch_id': batch.batch_id, 'owner': owner,
                 'expires_at': int(self.clock())+self.lease_seconds}
        if not self.store.cas({self.lock_key: (lock, lease), self.batch_key(batch.batch_id): (record, active)}):
            return 'BUSY'
        return self._drive(owner)

    def _owned(self, owner):
        lock = self.store.get(self.lock_key)
        if not lock or not lock.data.get('batch_id') or lock.data['owner'] != owner or lock.data['expires_at'] <= self.clock():
            raise LostLease('Owner is stale or lease expired')
        record = self.store.get(self.batch_key(lock.data['batch_id']))
        if record is None:
            raise Conflict('Lease has no durable batch record')
        return lock, record

    def tick(self, owner=None):
        """Watchdog recovery/heartbeat. Expired leases retain the original batch."""
        owner = owner or uuid.uuid4().hex
        lock = self.store.get(self.lock_key)
        if not lock or not lock.data.get('batch_id'):
            return 'IDLE'
        if lock.data['owner'] != owner and lock.data['expires_at'] > self.clock():
            return 'BUSY'
        lease = dict(lock.data, owner=owner, expires_at=int(self.clock())+self.lease_seconds)
        if not self.store.cas({self.lock_key: (lock, lease)}):
            return 'BUSY'
        return self._drive(owner)

    def _drive(self, owner):
        lock, record = self._owned(owner)
        if record.data['state'] == 'FAILED':
            return 'BLOCKED'
        if record.data['state'] != 'ACTIVE':
            raise Conflict('Unexpected batch state under lease')
        batch, attempt = Batch.parse(record.data['manifest']), record.data['attempt']
        # A timeout here leaves the durable intent/lease intact. Recovery MUST
        # reuse this exact name/input, including when the start response was lost.
        name, payload = batch.execution_name(attempt), batch.execution_input(attempt)
        payload['start_before'] = record.data['start_before']
        payload['fence_epoch'] = record.data['fence_epoch']
        self.pipeline.ensure(name, payload)
        self._owned(owner)  # Never let a stale observer finalize a newer lease.
        observation = self.pipeline.observe(name, payload)
        if observation.status == 'RUNNING':
            return 'RUNNING'
        if observation.status not in ('SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED'):
            return 'UNCERTAIN'
        if not observation.quiescent:
            return 'DRAINING'  # SFN terminal is not proof Glue/SQL is stopped.
        lock, current = self._owned(owner)
        if current.data['attempt'] != attempt:
            raise LostLease('Attempt changed during observation')
        state = 'SUCCEEDED' if observation.status == 'SUCCEEDED' else 'FAILED'
        update = dict(current.data, state=state, execution_status=observation.status)
        # Failure blocks later dates to protect SCD2 ordering. TTL does not release it.
        new_lock = {} if state == 'SUCCEEDED' else dict(lock.data, blocked=True)
        if not self.store.cas({self.lock_key: (lock, new_lock), self.batch_key(batch.batch_id): (current, update)}):
            raise LostLease('Completion raced with another owner')
        return state if state == 'SUCCEEDED' else 'BLOCKED'

    def resolve_failed(self, owner, action, reason):
        """Explicit operator decision, never an automatic retry/release on expiry."""
        if action not in ('retry', 'abandon') or not isinstance(reason, str) or not reason.strip():
            raise ValueError('Choose retry/abandon and supply an audit reason')
        lock, record = self._owned(owner)
        if record.data['state'] != 'FAILED':
            raise Conflict('Only a failed, blocked batch may be resolved')
        batch, attempt = Batch.parse(record.data['manifest']), record.data['attempt']
        payload = batch.execution_input(attempt)
        payload['start_before'] = record.data['start_before']
        payload['fence_epoch'] = record.data['fence_epoch']
        proof = self.pipeline.observe(batch.execution_name(attempt), payload)
        if proof.status not in ('SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED') or not proof.quiescent:
            raise Conflict('Outstanding/unknown external work prevents resolution')
        state = 'ACTIVE' if action == 'retry' else 'ABANDONED'
        update = dict(record.data, state=state, resolution_reason=reason,
                      attempt=attempt+1 if action == 'retry' else attempt)
        if action == 'retry':
            update['previous_attempts'] = record.data.get('previous_attempts', []) + [
                {'attempt': attempt, 'execution_status': record.data.get('execution_status'),
                 'work': record.data.get('work'), 'resolution_reason': reason}]
            update.pop('work', None)
            update['start_before'] = int(self.clock())+600
            update['fence_epoch'] = lock.revision+1
        lease = dict(lock.data, blocked=False, expires_at=int(self.clock())+self.lease_seconds) if action == 'retry' else {}
        if not self.store.cas({self.lock_key: (lock, lease), self.batch_key(batch.batch_id): (record, update)}):
            raise LostLease('Resolution ownership changed')
        return self._drive(owner) if action == 'retry' else 'ABANDONED'


class Dispatcher:
    """Bounded polling invocation. Receive/delete messages through an SQS adapter.

Do NOT attach this as an auto-acknowledging SQS Lambda event source. A scheduler
calls poll; an independent watchdog calls Admission.tick for active records.
"""
    def __init__(self, admission, queue):
        self.admission, self.queue = admission, queue

    def enqueue(self, manifest):
        batch = Batch.parse(manifest)
        # Durable outbox intent precedes SQS: a sender crash or message expiry
        # cannot erase the batch. Reconcile QUEUED records on a schedule.
        self.admission.register(batch)
        # Return success only AFTER SendMessage succeeds. A lost response is safe
        # to resend: FIFO dedup + persistent DynamoDB ledger provide idempotency.
        return self.queue.send(batch, self.admission.pipeline_id)

    def reconcile(self, limit=10):
        """Republish pending ledger intents; the GSI is NOT an admission lock."""
        sent = 0
        for record in self.admission.store.queued(self.admission.pipeline_id, limit):
            batch = Batch.parse(record.data['manifest'])
            fresh = self.admission.store.get(self.admission.batch_key(batch.batch_id))
            if fresh and fresh.data['state'] == 'QUEUED':
                self.queue.send(batch, self.admission.pipeline_id)
                sent += 1
        return sent

    def poll(self, owner=None):
        lock = self.admission.store.get(self.admission.lock_key)
        if lock and lock.data.get('batch_id'):
            return self.admission.tick(owner)
        message = self.queue.receive()
        if message is None:
            return 'EMPTY'
        batch = Batch.parse(message['manifest'])
        result = self.admission.accept(batch, owner)
        if result in ('RUNNING', 'DRAINING', 'UNCERTAIN', 'SUCCEEDED', 'DUPLICATE', 'BLOCKED'):
            # At this point durable ledger + watchdog own recovery. A crash after
            # this delete cannot lose the admitted batch. Never ack BUSY.
            self.queue.ack(message['receipt'])
        return result
