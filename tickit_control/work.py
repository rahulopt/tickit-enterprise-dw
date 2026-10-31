"""Durable pre-submission intents. Unknown external work prevents safe release.

The future isolated workflow must reserve BEFORE every async side effect, record
its service ID afterward, then seal submissions after SFN becomes terminal.
A lost submission response remains unresolved until reconciled; expiry is NOT
proof that the submission did not happen.
"""
import copy

from .admission import Conflict
from .batch import identifier


class WorkTracker:
    def __init__(self, store, pipeline_id='tickit-v2'):
        self.store, self.pipeline_id = store, pipeline_id

    def key(self, batch_id):
        return f'BATCH#{self.pipeline_id}#{batch_id}'

    def _update(self, batch_id, attempt, transform):
        for _ in range(10):
            record = self.store.get(self.key(batch_id))
            if record is None or record.data['attempt'] != attempt or record.data['state'] != 'ACTIVE':
                raise Conflict('Submission belongs to an inactive/stale attempt')
            data = copy.deepcopy(record.data)
            work = data.setdefault('work', {'sealed': False, 'operations': {}})
            transform(work)
            if self.store.cas({self.key(batch_id): (record, data)}):
                return
        raise Conflict('Work registration contention; do not submit side effects')

    def reserve(self, batch_id, attempt, operation_id, kind):
        identifier(operation_id)
        if kind not in ('glue', 'sql', 'lambda'):
            raise ValueError('Unknown external work kind')
        def change(work):
            if work['sealed']:
                raise Conflict('Submissions are sealed')
            operations = work['operations']
            if operation_id in operations:
                raise Conflict('Submission already reserved; reconcile it, do not submit again')
            operations[operation_id] = {'kind': kind, 'resolved': False}
        self._update(batch_id, attempt, change)

    def resolve(self, batch_id, attempt, operation_id, reference):
        def change(work):
            operation = work['operations'][operation_id]
            if operation['resolved'] and operation['reference'] != reference:
                raise Conflict('Service ID changed for an existing submission')
            if not isinstance(reference, dict) or not reference:
                raise ValueError('An observed external work reference is required')
            operation.update(resolved=True, reference=dict(reference))
        self._update(batch_id, attempt, change)

    def seal(self, batch_id, attempt):
        # Only the trusted terminal-state reconciler may seal. Pending operations
        # are retained and still block admission even after sealing.
        self._update(batch_id, attempt, lambda work: work.update(sealed=True))

    def inventory(self, batch_id, attempt):
        record = self.store.get(self.key(batch_id))
        if record is None or record.data['attempt'] != attempt:
            return None
        return record.data.get('work')
