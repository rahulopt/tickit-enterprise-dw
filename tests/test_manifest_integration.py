"""Full local manifest -> admission -> actual ETL transport -> SQL -> completion."""
import copy
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from tickit_control.admission import Admission, Dispatcher, Conflict, LostLease
from tickit_control.batch import TABLES, Batch, canonical
from tickit_control.aws import ImmutableObjects
from tickit_control.context import Context, AttemptFence
from tickit_control.etl import run_transaction_reference
from tickit_control.integration import LocalManifestPipeline
from tickit_control.local import SQLiteStore, SQLiteQueue
from tickit_control.warehouse import prepare_copy
from manifest_helpers import VersionedObjects, ManifestWarehouse, upload_fixture, load_module
from test_admission import Clock
from test_isolated_readiness import PREP

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def system(tmp_path, monkeypatch):
    import boto3
    client = VersionedObjects()
    monkeypatch.setenv('TICKIT_BUCKET','test-immutable-bucket')
    monkeypatch.setattr('sys.argv',['local-manifest-test'])
    monkeypatch.setattr(boto3,'client',lambda *a,**kw:client)
    directory = tmp_path/'fixtures'
    PREP.prepare(directory)
    clock = Clock()
    store = SQLiteStore(tmp_path/'ledger.db')
    queue = SQLiteQueue(tmp_path/'queue.db',clock)
    warehouse = ManifestWarehouse()
    branches = {}
    paths = {'category':'lambda/category_etl.py','venue':'lambda/venue_etl.py','users':'glue/users_etl.py',
             'event':'glue/events_etl.py','listing':'glue/listings_etl.py','sales':'glue/sales_etl.py'}
    for table,path in paths.items():
        module = load_module(path,table)
        if table in ('category','venue'):
            branches[table] = lambda request,client,fence,module=module: module.lambda_handler(request,None,batch_client=client,batch_fence=fence)
        elif table in ('users','event'):
            branches[table] = lambda request,client,fence,module=module: module.main(batch_request=request,batch_client=client,batch_fence=fence)
        else:
            branches[table] = lambda request,client,fence,module=module: module.main(batch_request=request,batch_client=client,batch_fence=fence,spark_runner=run_transaction_reference)
    pipeline = LocalManifestPipeline(store,client,warehouse,branches)
    admission = Admission(store,pipeline,clock)
    return {'client':client,'directory':directory,'clock':clock,'store':store,'queue':queue,'warehouse':warehouse,
            'pipeline':pipeline,'admission':admission,'dispatcher':Dispatcher(admission,queue),'branches':branches}


def uploaded(system, name='initial', batch_id='batch-a', day=PREP.INITIAL_DAY):
    return upload_fixture(system['client'],system['directory'],name,day,batch_id)


def execute(system,item,owner='owner',interrupt=None):
    system['dispatcher'].enqueue(item.data)
    assert system['dispatcher'].poll(owner) == 'RUNNING'
    system['pipeline'].execute(item.execution_name(1),interrupt=interrupt)
    return system['admission'].tick(owner)


def test_complete_local_path_and_version_pinning_for_all_six_branches(system):
    item = uploaded(system)
    for table in TABLES:
        # Poison latest versions. Each branch must still read the pinned good version.
        system['client'].seed(item.data['bucket'],item.data['files'][table]['key'],b'wrong,latest\nnot,the,batch\n')
    assert execute(system,item) == 'SUCCEEDED'
    warehouse = system['warehouse']
    assert warehouse.db.execute('SELECT COUNT(*) FROM fact.sales').fetchone()[0] == 1
    assert warehouse.db.execute('SELECT COUNT(*) FROM fact.listing').fetchone()[0] == 1
    assert warehouse.db.execute('SELECT COUNT(*) FROM control.batch_commits').fetchone()[0] == 1
    assert {table for _,_,table in system['pipeline'].branch_calls} == set(TABLES)
    for table in TABLES:
        ref = item.data['files'][table]
        assert (ref['key'],ref['version_id']) in system['client'].reads
        assert (ref['key'],None) not in system['client'].reads
    assert not any(key.startswith('bronze/') or key.startswith('tickit-v2/silver/') for key,_ in system['client'].reads+system['client'].writes)
    assert system['queue'].count() == 0
    assert system['store'].get(system['admission'].lock_key).data == {}


def test_changed_snapshot_temporal_joins_and_retry_outside_dedup_window(system):
    initial = uploaded(system)
    assert execute(system,initial) == 'SUCCEEDED'
    changed = uploaded(system,'changed','batch-b',PREP.CHANGED_DAY)
    assert execute(system,changed,'second') == 'SUCCEEDED'
    db = system['warehouse'].db
    assert db.execute('SELECT COUNT(*) FROM dim.users').fetchone()[0] == 3
    assert db.execute('SELECT COUNT(*) FROM dim.event').fetchone()[0] == 2
    assert db.execute('''SELECT f.salesid,u.effective_date,e.effective_date FROM fact.sales f
        JOIN dim.users u ON u.user_key=f.seller_key JOIN dim.event e ON e.event_key=f.event_key ORDER BY f.salesid''').fetchall() == [
        (100,PREP.INITIAL_DAY,PREP.INITIAL_DAY),(101,PREP.CHANGED_DAY,PREP.CHANGED_DAY),(102,PREP.INITIAL_DAY,PREP.INITIAL_DAY)]
    before = system['warehouse'].snapshot()
    calls = len(system['pipeline'].branch_calls)
    system['clock'].advance(301)
    system['dispatcher'].enqueue(changed.data)
    assert system['dispatcher'].poll('retry') == 'DUPLICATE'
    assert system['warehouse'].snapshot() == before
    assert len(system['pipeline'].branch_calls) == calls


def test_warehouse_receipt_prevents_duplicates_even_after_admission_ledger_loss(system,tmp_path):
    item = uploaded(system)
    execute(system,item)
    before = system['warehouse'].snapshot()
    fresh = SQLiteStore(tmp_path/'new-ledger.db')
    pipeline = LocalManifestPipeline(fresh,system['client'],system['warehouse'],system['branches'])
    admission = Admission(fresh,pipeline,system['clock'])
    assert admission.accept(item,'fresh-owner') == 'RUNNING'
    pipeline.execute(item.execution_name(1))
    assert admission.tick('fresh-owner') == 'SUCCEEDED'
    assert pipeline.runs[item.execution_name(1)]['warehouse_result'] == 'ALREADY_COMMITTED'
    assert system['warehouse'].snapshot() == before
    assert system['warehouse'].loads == 1


@pytest.mark.parametrize('fixture,exception', [('same_day_conflict',ValueError),('missing_history',ValueError),('rollback_after_sales',sqlite3.IntegrityError)])
def test_manifest_sql_failures_roll_back_stage_scd_facts_and_receipt(system,fixture,exception):
    execute(system,uploaded(system))
    execute(system,uploaded(system,'changed','batch-b',PREP.CHANGED_DAY),'second')
    before = system['warehouse'].snapshot()
    day = PREP.CHANGED_DAY if fixture == 'same_day_conflict' else PREP.ROLLBACK_DAY
    item = uploaded(system,fixture,'batch-c',day)
    system['dispatcher'].enqueue(item.data)
    assert system['dispatcher'].poll('third') == 'RUNNING'
    with pytest.raises(exception):
        system['pipeline'].execute(item.execution_name(1))
    assert system['admission'].tick('third') == 'BLOCKED'
    assert system['warehouse'].snapshot() == before
    assert system['store'].get(system['admission'].batch_key('batch-c')).data['state'] == 'FAILED'


def test_interruption_after_sql_commit_preserves_receipt_and_safe_retry(system):
    item = uploaded(system)
    assert execute(system,item,interrupt='after_commit') == 'DRAINING'
    assert system['warehouse'].loads == 1
    before = system['warehouse'].snapshot()
    waiting = uploaded(system,'changed','batch-b',PREP.CHANGED_DAY)
    system['dispatcher'].enqueue(waiting.data)
    assert system['dispatcher'].poll('other') == 'BUSY'
    assert system['queue'].count() == 1
    system['clock'].advance()
    assert system['admission'].tick('recovery') == 'DRAINING'
    system['pipeline'].reconcile_warehouse(item.execution_name(1))
    assert system['admission'].tick('recovery') == 'BLOCKED'
    assert system['admission'].resolve_failed('recovery','retry','Reconciled committed SQL after interruption') == 'RUNNING'
    system['pipeline'].execute(item.execution_name(2))
    assert system['admission'].tick('recovery') == 'SUCCEEDED'
    assert system['warehouse'].snapshot() == before
    assert system['warehouse'].loads == 1
    assert system['dispatcher'].poll('next') == 'RUNNING'


def test_unknown_sql_submission_never_releases_lease_or_acknowledges_next_batch(system):
    item = uploaded(system)
    assert execute(system,item,interrupt='before_sql') == 'DRAINING'
    waiting = uploaded(system,'changed','batch-b',PREP.CHANGED_DAY)
    system['dispatcher'].enqueue(waiting.data)
    with pytest.raises(Conflict,match='unknown'):
        system['pipeline'].reconcile_warehouse(item.execution_name(1))
    system['clock'].advance()
    assert system['admission'].tick('recovery') == 'DRAINING'
    assert system['queue'].count() == 1
    assert system['warehouse'].loads == 0


def test_coordinator_recovery_does_not_fence_a_valid_running_attempt(system):
    item = uploaded(system)
    system['admission'].accept(item,'old-owner')
    request = system['pipeline'].runs[item.execution_name(1)]['payload']
    old_context = Context.parse(dict(request,task_run_id='task-1'))
    old_fence = AttemptFence(system['store'],'tickit-v2',old_context)
    system['clock'].advance()
    assert system['admission'].tick('new-owner') == 'RUNNING'
    old_fence.check()
    system['pipeline'].execute(item.execution_name(1))
    with pytest.raises(LostLease):
        system['admission']._drive('old-owner')
    assert system['admission'].tick('new-owner') == 'SUCCEEDED'


def test_corrupt_checksum_or_expected_rows_fails_before_warehouse(system):
    item = uploaded(system)
    changed = item.data
    changed['files']['users']['rows'] += 1
    item = Batch.parse(changed)
    system['admission'].accept(item,'owner')
    with pytest.raises(ValueError,match='row count'):
        system['pipeline'].execute(item.execution_name(1))
    assert system['admission'].tick('owner') == 'BLOCKED'
    assert system['warehouse'].loads == 0


def test_old_attempt_is_fenced_from_etl_and_warehouse_after_retry(system):
    item = uploaded(system)
    execute(system,item,interrupt='after_commit')
    old_request = dict(system['pipeline'].runs[item.execution_name(1)]['payload'],task_run_id='task-1')
    old_context = Context.parse(old_request)
    old_fence = AttemptFence(system['store'],'tickit-v2',old_context)
    system['pipeline'].reconcile_warehouse(item.execution_name(1))
    system['admission'].tick('owner')
    system['admission'].resolve_failed('owner','retry','Confirmed committed SQL; retry completion')
    before = system['warehouse'].snapshot()
    writes = len(system['client'].writes)
    with pytest.raises(LostLease):
        system['branches']['users'](old_request,system['client'],old_fence)
    with pytest.raises(LostLease):
        old_fence.check()
    assert system['warehouse'].snapshot() == before
    assert len(system['client'].writes) == writes


def test_pinned_source_checksum_corruption_is_not_silently_loaded(system):
    item = uploaded(system)
    ref = item.data['files']['users']
    system['client'].versions[(item.data['bucket'],ref['key'])][ref['version_id']] = b'corrupt source'
    system['admission'].accept(item,'owner')
    with pytest.raises(Conflict,match='Pinned source'):
        system['pipeline'].execute(item.execution_name(1))
    assert system['admission'].tick('owner') == 'BLOCKED'
    assert system['warehouse'].loads == 0


def test_output_manifest_wrong_attempt_is_rejected_before_copy(system):
    from tickit_control.warehouse import commit_outputs
    item = uploaded(system)
    execute(system,item)
    run = system['pipeline'].runs[item.execution_name(1)]
    context = Context.parse(dict(run['payload'],task_run_id='task-1'))
    # Recreate a fresh context/active admission for a new batch, then try using A's output.
    other = uploaded(system,'changed','batch-b',PREP.CHANGED_DAY)
    system['admission'].accept(other,'other')
    other_context = Context.parse(dict(system['pipeline'].runs[other.execution_name(1)]['payload'],task_run_id='task-1'))
    with pytest.raises(Conflict,match='Wrong output'):
        prepare_copy(system['client'],other_context,run['output_manifest'],AttemptFence(system['store'],'tickit-v2',other_context))


def test_copy_row_mismatch_rolls_back_before_receipt(system):
    from tickit_control.etl import finish_branch
    item = uploaded(system)
    original = system['pipeline'].branches['users']
    def bad_rows(request,client,fence):
        result = original(request,client,fence)
        output = copy.deepcopy(result['output'])
        output['rows'] += 1
        # Publish a descriptor in a new task namespace, with matching pinned files.
        context = Context.parse(dict(request,task_run_id='wrong-count'))
        from tickit_control.etl import publish_part
        ref = result['output']['objects'][0]
        body = client.get_object(Bucket=item.data['bucket'],Key=ref['key'],VersionId=ref['version_id'])['Body'].read()
        output = publish_part(ImmutableObjects(client,item.data['bucket']),context,'users',body,output['rows'])
        return finish_branch(client,context,'users',output,fence)
    system['pipeline'].branches['users'] = bad_rows
    before = system['warehouse'].snapshot()
    system['admission'].accept(item,'owner')
    with pytest.raises(ValueError,match='COPY row count'):
        system['pipeline'].execute(item.execution_name(1))
    assert system['warehouse'].snapshot() == before
    assert system['warehouse'].loads == 0


def test_database_fence_rejects_a_new_uncommitted_batch_with_old_epoch(system):
    item = uploaded(system)
    execute(system,item)
    other = uploaded(system,'changed','batch-b',PREP.CHANGED_DAY)
    execute(system,other,'second')
    # Candidate native procedure has the same high-watermark check. This model
    # exercises it independently of DynamoDB's pre-submit guard.
    stale = uploaded(system,'changed','batch-c',PREP.CHANGED_DAY)
    system['admission'].accept(stale,'third')
    payload = dict(system['pipeline'].runs[stale.execution_name(1)]['payload'],task_run_id='task-1')
    context = Context.parse(payload)
    from dataclasses import replace
    context = replace(context,epoch=1)
    class DatabaseOnlyFence:
        def check(self):pass
    before = system['warehouse'].snapshot()
    with pytest.raises(Conflict,match='Stale warehouse'):
        system['warehouse'].load(system['client'],context,{'sha256':'a'*64},
            stale.output_manifest(1,{table:{'task_run':'task-1','prefix':stale.output_prefix(1,table,'task-1'),'rows':1,
                'objects':[{'key':stale.output_prefix(1,table,'task-1')+'part.parquet','version_id':'v1','sha256':'a'*64,'bytes':1}]} for table in TABLES}),
            {},DatabaseOnlyFence())
    assert system['warehouse'].snapshot() == before


def test_physical_stage_contract_matches_actual_ddl():
    import re
    from tickit_control.schema import SCHEMA
    text = re.sub(r'--[^\n]*', '', (ROOT/'sql/02_stage_tables.sql').read_text())
    kinds = {'SMALLINT':'int16','INTEGER':'int32','BOOLEAN':'bool','TIMESTAMP':'timestamp',
             'VARCHAR':'string','CHAR':'string','DECIMAL':'decimal'}
    for table, body in re.findall(r'CREATE TABLE stage\.(\w+)\s*\((.*?)\)\s*DISTSTYLE',text,re.S):
        fields = re.findall(r'^\s*(\w+)\s+(SMALLINT|INTEGER|BOOLEAN|TIMESTAMP|VARCHAR|CHAR|DECIMAL)\b',body,re.M)
        assert SCHEMA[table] == [(name,kinds[kind]) for name,kind in fields]
        for precision, scale in re.findall(r'DECIMAL\((\d+),\s*(\d+)\)',body):
            assert (precision,scale) == ('8','2')


def test_native_sql_candidate_has_atomic_receipt_before_copy():
    import re
    text = re.sub(r'--[^\n]*','',(ROOT/'sql/09_manifest_warehouse.sql').read_text())
    assert text.index('LOCK TABLE control.batch_commits') < text.index('SELECT MAX(input_sha)')
    assert text.index('RETURN;') < text.index("EXECUTE 'DELETE FROM stage.'")
    assert text.index('CALL sp_load_dimensions') < text.index('CALL sp_load_facts') < text.index('INSERT INTO control.batch_commits')
    assert not re.search(r'\b(COMMIT|TRUNCATE|NONATOMIC|EXCEPTION\s+WHEN)\b',text,re.I)


def test_warehouse_submission_is_bound_and_retry_token_is_stable(system):
    import boto3
    from botocore.stub import Stubber, ANY
    from tickit_control.warehouse import submit
    item = uploaded(system)
    assert execute(system,item) == 'SUCCEEDED'
    run = system['pipeline'].runs[item.execution_name(1)]
    context = Context.parse(dict(run['payload'],task_run_id='submit-check'))
    class Fence:
        def check(self): pass
    sdk = boto3.session.Session().client('redshift-data',region_name='us-east-1',
        aws_access_key_id='testing',aws_secret_access_key='testing')
    observed = []
    sdk.meta.events.register('before-parameter-build.redshift-data.ExecuteStatement',
        lambda params, **kw: observed.append(copy.deepcopy(params)))
    with Stubber(sdk) as stub:
        for _ in range(2):
            stub.add_response('execute_statement',{'Id':'test-statement'},
                {'Database':'test_db','WorkgroupName':'test_workgroup','ClientToken':ANY,'Sql':ANY,'Parameters':ANY})
        for _ in range(2):
            submit(sdk,context,run['output_manifest'],run['copy_specs'],Fence(),'test_db','test_workgroup',
                   'arn:aws:iam::123456789012:role/test-copy')
        stub.assert_no_pending_responses()
    assert observed[0] == observed[1]
    assert observed[0]['Sql'].startswith('CALL sp_load_manifest_warehouse(:batch_id,')
    assert {p['name']:p['value'] for p in observed[0]['Parameters']}['input_sha'] == item.fingerprint
