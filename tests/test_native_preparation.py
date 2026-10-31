"""Offline native controller, workflow and deployment artifact checks; no AWS calls."""
import copy
import hashlib
import io
import json
import zipfile
from types import SimpleNamespace

import pytest

from tickit_control.admission import Conflict
from tickit_control.batch import TABLES, canonical
from tickit_control.context import AttemptFence, Context, job_request
from tickit_control.native import NativeController
from tickit_control.work import WorkTracker
from tickit_control.aws import StandardPipeline
from test_manifest_integration import system, uploaded
from test_isolated_readiness import load

BUILD=load('tools/prepare_aws_integration.py')


@pytest.fixture
def native(system):
    item=uploaded(system)
    system['admission'].accept(item,'owner')
    request=system['pipeline'].runs[item.execution_name(1)]['payload']
    jobs={table:'isolated-'+table for table in TABLES[2:]}
    settings={'pipeline_id':'tickit-v2','bucket':item.data['bucket'],'table':'test-table',
              'database':'test_db','workgroup':'test-workgroup','copy_role':'arn:aws:iam::123456789012:role/test-copy',
              'functions':{table:'isolated-'+table for table in TABLES[:2]},'jobs':jobs}
    calls=[]
    class Lambda:
        def invoke(self,**kwargs):
            calls.append(('lambda',kwargs))
            table=kwargs['FunctionName'].removeprefix('isolated-')
            payload=json.loads(kwargs['Payload'])
            fence=AttemptFence(system['store'],'tickit-v2',Context.parse(payload))
            output=system['branches'][table](payload,system['client'],fence)
            return {'Payload':io.BytesIO(canonical(output).encode())}
    class Glue:
        state='SUCCEEDED'
        def start_job_run(self,**kwargs):
            calls.append(('glue',kwargs))
            payload=json.loads(kwargs['Arguments']['--BATCH_REQUEST'])
            table=kwargs['JobName'].removeprefix('isolated-')
            fence=AttemptFence(system['store'],'tickit-v2',Context.parse(payload))
            system['branches'][table](payload,system['client'],fence)
            return {'JobRunId':'jr-'+table}
        def get_job_run(self,**kwargs):return {'JobRun':{'JobRunState':self.state}}
    class Sql:
        state='FINISHED'
        def execute_statement(self,**kwargs):
            calls.append(('sql',kwargs))
            params={v['name']:v['value'] for v in kwargs['Parameters']}
            context=Context.parse(dict(request,task_run_id='controller'))
            key=item.attempt_root(1)+'output-manifest.json'
            raw=system['client'].get_object(Bucket=item.data['bucket'],Key=key)
            body=raw['Body'].read()
            ref={'key':key,'version_id':raw['VersionId'],'sha256':hashlib.sha256(body).hexdigest(),'bytes':len(body)}
            system['warehouse'].load(system['client'],context,ref,json.loads(body),json.loads(params['specs']),AttemptFence(system['store'],'tickit-v2',context))
            return {'Id':'native-statement'}
        def describe_statement(self,**kwargs):return {'Status':self.state}
    clients={'s3':system['client'],'lambda':Lambda(),'glue':Glue(),'redshift-data':Sql()}
    controller=NativeController(system['store'],clients,settings)
    return SimpleNamespace(system=system,item=item,request=request,clients=clients,controller=controller,calls=calls)


def event(native,action,**kwargs):return dict(action=action,request=native.request,**kwargs)


def test_native_adapter_complete_path_preserves_descriptors_and_receipt(native):
    assert native.controller.handle(event(native,'validate'))==native.request
    descriptors=[]
    for table in TABLES:
        payload=event(native,'submit_branch',dataset=table,task_run_id='native-'+table)
        result=native.controller.handle(payload)
        assert result['status']=='SUCCEEDED'
        descriptors.append({'dataset':table,'descriptor':result['descriptor']})
        assert native.controller.handle(payload)==result
    result=native.controller.handle(event(native,'submit_warehouse',branches=descriptors))
    assert result=={'status':'SUCCEEDED','statement_id':'native-statement','child_status':'FINISHED'}
    native.controller.handle(event(native,'poll_warehouse'))
    assert [kind for kind,_ in native.calls].count('sql')==1
    assert native.system['warehouse'].db.execute('SELECT COUNT(*) FROM control.batch_commits').fetchone()[0]==1
    for kind,call in native.calls:
        if kind=='glue':
            request=json.loads(call['Arguments']['--BATCH_REQUEST'])
            assert request['files']==native.item.data['files']
            assert request['load_date']==native.item.data['load_date']
            assert call['Arguments']['--ADMISSION_TABLE']=='test-table'


@pytest.mark.parametrize('service,table',[('lambda','category'),('glue','users')])
def test_unknown_submission_never_reissues_or_releases(native,service,table):
    def lost(**kwargs):
        native.calls.append(('lost',kwargs))
        raise TimeoutError('response lost')
    if service=='lambda':native.clients[service].invoke=lost
    else:native.clients[service].start_job_run=lost
    payload=event(native,'submit_branch',dataset=table,task_run_id='unknown-task')
    with pytest.raises(TimeoutError):native.controller.handle(payload)
    with pytest.raises(Conflict,match='Unknown submission'):native.controller.handle(payload)
    assert len(native.calls)==1
    operation=WorkTracker(native.system['store']).inventory(native.item.batch_id,1)['operations'][table]
    assert not operation['resolved']


def test_terminal_watchdog_seals_and_drains_known_glue(native):
    payload=event(native,'submit_branch',dataset='users',task_run_id='native-users')
    native.clients['glue'].state='RUNNING'
    assert native.controller.handle(payload)['status']=='RUNNING'
    class States:
        def describe_execution(self,**kw):return {'status':'ABORTED','input':canonical(native.request)}
    pipeline=StandardPipeline(States(),'arn:aws:states:us-east-1:123456789012:stateMachine:test',
        native.controller.tracker,native.clients['glue'],native.clients['redshift-data'],seal_terminal=True)
    assert not pipeline.observe(native.item.execution_name(1),native.request).quiescent
    with pytest.raises(Conflict,match='sealed'):
        native.controller.handle(event(native,'submit_branch',dataset='sales',task_run_id='late-task'))
    native.clients['glue'].state='STOPPED'
    assert pipeline.observe(native.item.execution_name(1),native.request).quiescent


def test_wrong_bucket_and_future_date_rejected_before_clients(native):
    request=copy.deepcopy(native.request);request['bucket']='other-isolated-bucket'
    with pytest.raises((ValueError,Conflict)):native.controller.handle(dict(action='validate',request=request))
    assert native.calls==[]


def test_job_arguments_set_only_scoped_fence_environment(monkeypatch):
    monkeypatch.delenv('ADMISSION_TABLE',raising=False);monkeypatch.delenv('TICKIT_PIPELINE_ID',raising=False)
    import os
    assert job_request(['--BATCH_REQUEST','{}','--ADMISSION_TABLE','test-table','--TICKIT_PIPELINE_ID','test-pipeline'])=={}
    assert os.environ['ADMISSION_TABLE']=='test-table'
    assert os.environ['TICKIT_PIPELINE_ID']=='test-pipeline'


def test_workflow_structure_and_manifest_paths():
    flow=BUILD.workflow()
    assert BUILD.validate_workflow(flow)
    assert flow['States']['Validate']['OutputPath']=='$.Payload'
    for branch,table in zip(flow['States']['SixBranches']['Branches'],TABLES):
        assert branch['States']['Identity']['Parameters']['request.$']=='$'
        assert branch['States']['Submit']['Parameters']['Payload']['dataset']==table
        assert branch['States']['Submit']['ResultPath']=='$.result_envelope'
        assert branch['States']['Poll']['Parameters']['Payload']['task_run_id.$']=='$.task_run_id'
    assert flow['States']['SubmitWarehouse']['Parameters']['Payload']['branches.$']=='$.branches'
    assert flow['States']['SixBranches']['Catch'][0]['Next']=='NotifyFailure'
    assert flow['States']['NotifySuccess']['Catch'][0]['Next']=='Failed'


def test_artifacts_deterministic_and_runtime_resources_isolated(tmp_path):
    BUILD.prepare(tmp_path/'a');BUILD.prepare(tmp_path/'b')
    for path in (tmp_path/'a').glob('*.zip'):
        assert path.read_bytes()==(tmp_path/'b'/path.name).read_bytes()
        with zipfile.ZipFile(path) as archive:
            assert 'tickit_control/native.py' in archive.namelist()
            assert not any('.aws' in name or '__pycache__' in name for name in archive.namelist())
    runtime=BUILD.runtime(); foundation=BUILD.foundation(); warehouse=BUILD.warehouse_template()
    for template in (runtime,foundation,warehouse):
        assert all(resource['Condition']=='IsolatedAccount' for resource in template['Resources'].values())
    assert runtime['Resources']['Watchdog']['Properties']['State']=='DISABLED'
    for name in ('UsersJob','EventJob','ListingJob','SalesJob'):
        job=runtime['Resources'][name]['Properties']
        assert (job['GlueVersion'],job['Timeout'],job['MaxRetries'],job['NumberOfWorkers'])==('5.0',5,0,2)
    rules=foundation['Resources']['BucketPolicy']['Properties']['PolicyDocument']['Statement']
    assert not any('scratch' in path['Fn::Sub'] for path in rules[1]['Resource'])
    assert warehouse['Resources']['Workgroup']['Properties']['PubliclyAccessible'] is False
    assert warehouse['Resources']['Workgroup']['Properties']['MaxCapacity']==8


class WorkflowFailure(Exception):pass


def run_definition(flow, request, controller, notifications):
    """Small interpreter for the exact JSONPath/intrinsic subset in this ASL.

    Catches misplaced Payload/ResultPath/descriptors without AWS state transitions.
    It does not simulate native parallel cancellation or the SFN service validator.
    """
    def lookup(path,value):
        if path=='$':return value
        for part in path.removeprefix('$.').split('.'):value=value[part]
        return value
    def parameters(spec,value):
        result={}
        for key,item in spec.items():
            if key.endswith('.$'):
                result[key[:-2]]=('local-task' if item=='States.UUID()' else
                    canonical(value) if item=='States.JsonToString($)' else lookup(item,value))
            else:result[key]=parameters(item,value) if isinstance(item,dict) else item
        return result
    def block(definition,value):
        current=definition['StartAt']
        for _ in range(100):
            node=definition['States'][current]
            try:
                kind=node['Type']; output=None
                if kind=='Succeed':return value
                if kind=='Fail':raise WorkflowFailure(node['Error'])
                if kind=='Pass':value=parameters(node['Parameters'],value)
                if kind=='Parallel':output=[block(branch,copy.deepcopy(value)) for branch in node['Branches']]
                if kind=='Task':
                    args=parameters(node['Parameters'],value)
                    if node['Resource'].endswith('lambda:invoke'):output={'Payload':controller.handle(args['Payload'])}
                    else:notifications.append(args);output={'MessageId':'local-notification'}
                if output is not None:
                    if 'ResultSelector' in node:output=parameters(node['ResultSelector'],output)
                    if 'ResultPath' in node:
                        value=copy.deepcopy(value);value[node['ResultPath'].removeprefix('$.')]=output
                    else:value=output
                    if 'OutputPath' in node:value=lookup(node['OutputPath'],value)
                if kind=='Choice':
                    current=node['Default']
                    for option in node['Choices']:
                        if lookup(option['Variable'],value)==option['StringEquals']:
                            current=option['Next'];break
                    continue
                if node.get('End'):return value
                current=node['Next']
            except Exception as exc:
                if not node.get('Catch'):raise
                handler=node['Catch'][0]
                if 'ResultPath' in handler:
                    value=copy.deepcopy(value);value[handler['ResultPath'].removeprefix('$.')]={'Error':str(exc)}
                current=handler['Next']
        raise AssertionError('Workflow failed to terminate in local interpreter')
    return block(flow,copy.deepcopy(request))


def test_actual_generated_workflow_jsonpaths_through_completion(native):
    notifications=[]
    result=run_definition(BUILD.workflow(),native.request,native.controller,notifications)
    assert len(result['branches'])==6
    assert result['files']==native.item.data['files']
    assert result['load_date']==native.item.data['load_date']
    assert result['result_envelope']['payload']['child_status']=='FINISHED'
    assert len(notifications)==1
    assert notifications[0]['Subject']=='TICKIT isolated SUCCEEDED'
    inventory=native.controller.tracker.inventory(native.item.batch_id,1)
    assert not inventory['sealed']  # Only the external terminal watchdog seals.
    assert set(inventory['operations'])==set(TABLES)|{'warehouse'}


def test_actual_workflow_failure_never_reaches_warehouse(native):
    native.clients['glue'].state='FAILED'
    notifications=[]
    with pytest.raises(WorkflowFailure):
        run_definition(BUILD.workflow(),native.request,native.controller,notifications)
    assert notifications[-1]['Subject']=='TICKIT isolated FAILED'
    assert not any(kind=='sql' for kind,_ in native.calls)
    assert native.system['store'].get(native.system['admission'].lock_key).data['batch_id']==native.item.batch_id


def test_future_date_rejected_before_admission_or_etl(native):
    from tickit_control.batch import Batch
    document=native.item.data;document['load_date']='2099-01-01'
    future=Batch.parse(document)
    request=dict(future.execution_input(1),fence_epoch=native.request['fence_epoch'])
    with pytest.raises(ValueError,match='Future UTC'):
        native.controller.handle(dict(action='validate',request=request))
    assert native.calls==[]


def test_real_upload_receipt_manifest_builder_matches_contract(native):
    from test_isolated_readiness import PREP
    module=load('tools/make_isolated_manifest.py')
    receipts={table:{'Bucket':native.item.data['bucket'],'Key':info['key'],
        'VersionId':info['version_id'],'sha256':info['sha256']} for table,info in native.item.data['files'].items()}
    rebuilt=module.make(native.system['directory']/'initial'/'bronze',receipts,native.item.data['bucket'],native.item.batch_id,PREP.INITIAL_DAY)
    assert rebuilt==native.item
    receipts['sales']['Key']='bronze/sales.csv'
    with pytest.raises(ValueError,match='crosses'):module.make(native.system['directory']/'initial'/'bronze',receipts,native.item.data['bucket'],native.item.batch_id,PREP.INITIAL_DAY)


def test_offline_sql_signatures_and_rollback_checks():
    assert load('tools/check_manifest_sql.py').check()['structure_and_signatures']=='PASS'


def test_verification_selects_execute_against_real_table_columns(native):
    import re
    from pathlib import Path
    run_definition(BUILD.workflow(),native.request,native.controller,[])
    text=(Path(__file__).resolve().parents[1]/'tests/isolated/sql/verify_manifest.sql').read_text()
    text=re.sub(r'--[^\n]*','',text).replace('::TIMESTAMP','')
    statements=[s.strip() for s in text.split(';') if s.strip()]
    results=[native.system['warehouse'].db.execute(s).fetchall() for s in statements if not s.startswith('SELECT current_database')]
    assert dict(results[0])=={'category':1,'venue':1,'users':2,'event':1,'listing':1,'sales':1}
    assert dict(results[1])=={'users':2,'event':1,'sales':1,'listing':1,'receipts':1}
    assert all(rows==[] for rows in results[2:-1])
    assert len(results[-1])==1


def test_reviewed_templates_match_generated_definitions():
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    for name,builder in [('foundation',BUILD.foundation),('warehouse',BUILD.warehouse_template),('runtime',BUILD.runtime)]:
        assert json.loads((root/'infra/isolated'/(name+'.template.json')).read_text())==builder()
    assert json.loads((root/'stepfunctions/tickit_manifest_isolated.template.json').read_text())==BUILD.workflow()


def test_sns_failure_after_commit_retains_warehouse_receipt_and_lease(native):
    class LostNotification(list):
        def append(self,item):raise TimeoutError('SNS publish response lost')
    with pytest.raises(WorkflowFailure):
        run_definition(BUILD.workflow(),native.request,native.controller,LostNotification())
    assert native.system['warehouse'].db.execute('SELECT COUNT(*) FROM control.batch_commits').fetchone()[0]==1
    assert native.system['store'].get(native.system['admission'].lock_key).data['batch_id']==native.item.batch_id


def test_glue_and_final_object_permissions_do_not_need_shared_paths():
    template=BUILD.runtime()
    role=template['Resources']['EtlRole']['Properties']
    text=canonical(role)
    assert 'tickit-v2/silver/' not in text and 'bronze/' not in text
    assert 'dynamodb:PutItem' not in text and 'redshift-data:ExecuteStatement' not in text
    controller=canonical(template['Resources']['ControllerRole']['Properties'])
    assert 'dynamodb:PutItem' in controller
    assert 'dynamodb:TransactWriteItems' not in controller
    assert 's3:DeleteObject' not in controller


def test_native_sql_commit_response_loss_blocks_resubmit_and_release(native):
    descriptors=[]
    for table in TABLES:
        result=native.controller.handle(event(native,'submit_branch',dataset=table,task_run_id='sql-loss-'+table))
        descriptors.append({'dataset':table,'descriptor':result['descriptor']})
    original=native.clients['redshift-data'].execute_statement
    def lost(**kwargs):
        original(**kwargs)
        raise TimeoutError('commit response lost')
    native.clients['redshift-data'].execute_statement=lost
    payload=event(native,'submit_warehouse',branches=descriptors)
    with pytest.raises(TimeoutError):native.controller.handle(payload)
    assert native.system['warehouse'].db.execute('SELECT COUNT(*) FROM control.batch_commits').fetchone()[0]==1
    with pytest.raises(Conflict,match='Unknown SQL submission'):native.controller.handle(payload)
    assert sum(kind=='sql' for kind,_ in native.calls)==1
    class States:
        def describe_execution(self,**kw):return {'status':'ABORTED','input':canonical(native.request)}
    pipeline=StandardPipeline(States(),'arn:aws:states:us-east-1:123456789012:stateMachine:test',
        native.controller.tracker,native.clients['glue'],native.clients['redshift-data'],seal_terminal=True)
    assert not pipeline.observe(native.item.execution_name(1),native.request).quiescent
