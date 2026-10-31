"""Isolated workflow submission adapter; injected clients, no import-time AWS calls.

No mutation retries: an ambiguous submission remains reserved and quarantined.
Only the terminal SFN observer seals the registry, never workflow task code.
"""
import hashlib
import json
from datetime import datetime, timezone

from .admission import Conflict
from .batch import TABLES, canonical
from .context import Context, AttemptFence
from .warehouse import gather_outputs, commit_outputs, prepare_copy, submit
from .work import WorkTracker

GLUE_TERMINAL = {'SUCCEEDED','FAILED','STOPPED','TIMEOUT','ERROR','EXPIRED'}


class NativeController:
    def __init__(self, store, clients, settings):
        self.store, self.clients, self.settings = store, clients, settings
        self.tracker = WorkTracker(store, settings['pipeline_id'])

    def context(self, event):
        request = dict(event['request'], task_run_id=event.get('task_run_id','controller'))
        context = Context.parse(request)
        if context.batch.data['bucket'] != self.settings['bucket']:
            raise Conflict('Manifest does not belong to the isolated bucket')
        if context.batch.data['load_date'] > datetime.now(timezone.utc).date().isoformat():
            raise ValueError('Future UTC load date is forbidden')
        fence = AttemptFence(self.store, self.settings['pipeline_id'], context)
        fence.check()
        return request, context, fence

    def _operation(self, context, name):
        inventory = self.tracker.inventory(context.batch.batch_id, context.attempt)
        return (inventory or {}).get('operations',{}).get(name)

    def _reserve(self, context, name, kind):
        self.tracker.reserve(context.batch.batch_id, context.attempt, name, kind)

    def _resolve(self, context, name, reference):
        self.tracker.resolve(context.batch.batch_id, context.attempt, name, reference)

    def _descriptor(self, context, table):
        key = context.batch.output_prefix(context.attempt,table,context.task_run)+'result.json'
        result = self.clients['s3'].get_object(Bucket=context.batch.data['bucket'],Key=key)
        try:
            body = result['Body'].read()
        finally:
            result['Body'].close()
        version = result.get('VersionId')
        if not version or version == 'null':
            raise Conflict('Glue result sidecar must be versioned')
        reference = {'key':key,'version_id':version,'sha256':hashlib.sha256(body).hexdigest(),'bytes':len(body)}
        # The discovery read resolves one immutable key, then validates its pinned
        # version and lineage. No listing or shared latest dataset is consumed.
        output = json.loads(body)
        if output['dataset'] != table or output['output']['task_run'] != context.task_run:
            raise Conflict('Glue descriptor lineage mismatch')
        return reference

    def branch(self, event, request, context, fence):
        table = event['dataset']
        if table not in TABLES:
            raise ValueError('Unknown dataset')
        operation = self._operation(context,table)
        if operation is None:
            if event['action'] != 'submit_branch':
                raise Conflict('Cannot poll unsubmitted branch')
            kind = 'lambda' if table in ('category','venue') else 'glue'
            self._reserve(context,table,kind)
            fence.check()
            if kind == 'lambda':
                result = self.clients['lambda'].invoke(FunctionName=self.settings['functions'][table],
                    InvocationType='RequestResponse',Payload=canonical(request).encode())
                try:
                    response = json.load(result['Payload'])
                finally:
                    result['Payload'].close()
                status = 'FAILED' if result.get('FunctionError') else 'SUCCEEDED'
                reference = {'response_observed':True,'task_run':context.task_run,'status':status}
                # Record known completion even when a malformed/failed payload
                # cannot provide a descriptor; this is not an unknown invocation.
                if status == 'SUCCEEDED':
                    reference['descriptor'] = response.get('descriptor') if isinstance(response,dict) else None
                self._resolve(context,table,reference)
            else:
                name = self.settings['jobs'][table]
                result = self.clients['glue'].start_job_run(JobName=name,
                    Arguments={'--BATCH_REQUEST':canonical(request),'--TICKIT_BUCKET':self.settings['bucket'],
                               '--ADMISSION_TABLE':self.settings['table'],'--TICKIT_PIPELINE_ID':self.settings['pipeline_id']})
                self._resolve(context,table,{'job_name':name,'run_id':result['JobRunId'],'task_run':context.task_run})
            operation = self._operation(context,table)
        if not operation['resolved']:
            raise Conflict('Unknown submission response: reconcile before retry')
        reference = operation['reference']
        if reference['task_run'] != context.task_run:
            raise Conflict('Task identity changed after submission')
        if operation['kind'] == 'lambda':
            if reference['status'] != 'SUCCEEDED' or not reference.get('descriptor'):
                return {'status':'FAILED'}
            return {'status':'SUCCEEDED','descriptor':reference['descriptor']}
        result = self.clients['glue'].get_job_run(JobName=reference['job_name'],RunId=reference['run_id'])
        status = result['JobRun']['JobRunState']
        if status not in GLUE_TERMINAL:
            return {'status':'RUNNING'}
        return {'status':'FAILED','child_status':status} if status != 'SUCCEEDED' else {
            'status':'SUCCEEDED','descriptor':self._descriptor(context,table)}

    def warehouse(self, event, context, fence):
        operation = self._operation(context,'warehouse')
        if operation is None:
            if event['action'] != 'submit_warehouse':
                raise Conflict('Cannot poll an unsubmitted SQL statement')
            descriptors = {}
            for item in event['branches']:
                if item['dataset'] in descriptors:
                    raise Conflict('Duplicate branch descriptor')
                descriptors[item['dataset']] = item['descriptor']
            outputs = gather_outputs(self.clients['s3'],context,descriptors,fence)
            committed = commit_outputs(self.clients['s3'],context,outputs,fence)
            _, specs = prepare_copy(self.clients['s3'],context,committed,fence)
            self._reserve(context,'warehouse','sql')
            result = submit(self.clients['redshift-data'],context,committed,specs,fence,
                            self.settings['database'],self.settings['workgroup'],self.settings['copy_role'])
            self._resolve(context,'warehouse',{'statement_id':result['Id']})
            operation = self._operation(context,'warehouse')
        if not operation['resolved']:
            raise Conflict('Unknown SQL submission: reconcile; do not resubmit')
        result = self.clients['redshift-data'].describe_statement(Id=operation['reference']['statement_id'])
        status = result['Status']
        if status not in ('FINISHED','FAILED','ABORTED'):
            return {'status':'RUNNING'}
        return {'status':'SUCCEEDED' if status == 'FINISHED' else 'FAILED',
                'statement_id':operation['reference']['statement_id'],'child_status':status}

    def handle(self,event):
        request, context, fence = self.context(event)
        action = event['action']
        if action == 'validate':
            return event['request']
        if action in ('submit_branch','poll_branch'):
            return self.branch(event,request,context,fence)
        if action in ('submit_warehouse','poll_warehouse'):
            return self.warehouse(event,context,fence)
        raise ValueError('Unknown isolated controller action')
