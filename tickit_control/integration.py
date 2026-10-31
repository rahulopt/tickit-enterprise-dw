"""Local orchestration of the manifest contract; no AWS workloads are started.

Injected branch callables exercise real pandas ETL plus an explicit small-data
Spark reference. The warehouse implementation is injected (SQLite SQL harness
locally, the candidate atomic procedure after isolated deployment approval).
"""
import copy

from .admission import Conflict, Observation
from .batch import TABLES
from .context import Context, AttemptFence
from .warehouse import commit_outputs, prepare_copy, gather_outputs
from .work import WorkTracker


class LocalManifestPipeline:
    def __init__(self, store, client, warehouse, branches, pipeline_id='tickit-v2'):
        self.store, self.client, self.warehouse, self.branches = store, client, warehouse, branches
        self.scope = pipeline_id
        self.tracker = WorkTracker(store, pipeline_id)
        self.runs = {}
        self.branch_calls = []

    def ensure(self, name, payload):
        if name in self.runs:
            if self.runs[name]['payload'] != payload:
                raise Conflict('Execution input changed')
            return
        self.runs[name] = {'payload':copy.deepcopy(payload), 'status':'RUNNING'}

    def execute(self, name, interrupt=None):
        run = self.runs[name]
        request = dict(run['payload'], task_run_id='task-'+str(run['payload']['attempt']))
        context = Context.parse(request)
        fence = AttemptFence(self.store, self.scope, context)
        fence.check()
        descriptors = {}
        try:
            for table in TABLES:
                self.tracker.reserve(context.batch.batch_id, context.attempt, table, 'lambda')
                self.branch_calls.append((context.batch.batch_id, context.attempt, table))
                result = self.branches[table](request, self.client, fence)
                if result['dataset'] != table:
                    raise ValueError('Branch returned the wrong dataset')
                descriptors[table] = result['descriptor']
                self.tracker.resolve(context.batch.batch_id, context.attempt, table, {'response_observed':True})
            outputs = gather_outputs(self.client, context, descriptors, fence)
            committed = commit_outputs(self.client, context, outputs, fence)
            run['output_manifest'] = committed
            document, specs = prepare_copy(self.client, context, committed, fence)
            run['copy_specs'] = specs
            self.tracker.reserve(context.batch.batch_id, context.attempt, 'warehouse', 'lambda')
            if interrupt == 'before_sql':
                # Models an unresolved submission; the caller must reconcile it.
                run['status'] = 'ABORTED'
                self.tracker.seal(context.batch.batch_id, context.attempt)
                return
            result = self.warehouse.load(self.client, context, committed, document, specs, fence)
            run['warehouse_result'] = result
            if interrupt == 'after_commit':
                run['status'] = 'ABORTED'
                self.tracker.seal(context.batch.batch_id, context.attempt)
                return
            self.tracker.resolve(context.batch.batch_id, context.attempt, 'warehouse', {'response_observed':True})
            run['status'] = 'SUCCEEDED'
            self.tracker.seal(context.batch.batch_id, context.attempt)
        except Exception as exc:
            run['status'], run['error'] = 'FAILED', repr(exc)
            # Synchronous local exceptions are observed completions. Real async
            # adapters must keep unknown submissions unresolved until reconciled.
            inventory = self.tracker.inventory(context.batch.batch_id, context.attempt)
            for operation, info in inventory['operations'].items():
                if not info['resolved']:
                    self.tracker.resolve(context.batch.batch_id, context.attempt, operation, {'response_observed':True})
            self.tracker.seal(context.batch.batch_id, context.attempt)
            raise

    def observe(self, name, payload):
        run = self.runs[name]
        if run['payload'] != payload:
            raise Conflict('Execution identity mismatch')
        inventory = self.tracker.inventory(payload['batch_id'], payload['attempt'])
        quiet = bool(inventory and inventory['sealed'] and all(op['resolved'] for op in inventory['operations'].values()))
        return Observation(run['status'], quiet)

    def reconcile_warehouse(self, name):
        run = self.runs[name]
        context = Context.parse(dict(run['payload'], task_run_id='reconciler'))
        # A receipt establishes COMMIT; an absent receipt does not establish that
        # an interrupted/unknown SQL submission stopped. Never infer rollback.
        if not self.warehouse.receipt(context.batch.batch_id, context.batch.fingerprint):
            raise Conflict('Warehouse result remains unknown; no commit receipt')
        self.tracker.resolve(context.batch.batch_id, context.attempt, 'warehouse', {'response_observed':True})
