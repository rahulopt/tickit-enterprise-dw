"""Local-only acceptance preparation. AWS/network execution is not part of this suite."""
import csv
import importlib.util
import json
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from test_scd2 import Warehouse

ROOT = Path(__file__).resolve().parents[1]


def load(relative):
    spec = importlib.util.spec_from_file_location(Path(relative).stem, ROOT/relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PREP = load('tools/tickit_isolated.py')
VALIDATOR = load('tests/isolated/validate_input.py')
NOW = datetime(2026, 10, 31, 12, tzinfo=timezone.utc)


@pytest.fixture(scope='module')
def prepared(tmp_path_factory):
    directory = tmp_path_factory.mktemp('tickit-synthetic')
    PREP.prepare(directory)
    return directory


def read_batch(directory, name):
    result = {}
    for table in PREP.TABLES:
        with (directory/name/'bronze'/f'{table}.csv').open(newline='') as stream:
            result[table] = list(csv.DictReader(stream))
    return result


def replace_stage(warehouse, tables):
    columns = PREP.stage_columns()
    venues = {row['venueid']: row for row in tables['venue']}
    cats = {row['catid']: row for row in tables['category']}
    for table in PREP.TABLES:
        warehouse.db.execute(f'DELETE FROM stage.{table}')
        for original in tables[table]:
            row = dict(original)
            if table == 'event':
                row.update({k: v for k, v in venues[row['venueid']].items() if k != 'venueid'})
                row.update({k: v for k, v in cats[row['catid']].items() if k != 'catid'})
            values = [None if row[col] == '' else (1 if row[col] == 'TRUE' else 0) if col.startswith('like') else row[col]
                      for col in columns[table]]
            warehouse.db.execute(f'INSERT INTO stage.{table} ({",".join(columns[table])}) VALUES ({",".join("?" for _ in values)})', values)


def accept(warehouse, directory, name, day):
    with warehouse.db:
        replace_stage(warehouse, read_batch(directory, name))
        warehouse.run('sp_load_dimensions', day)
        warehouse.run('sp_load_facts', day)


def snapshot(warehouse):
    names = [f'stage.{name}' for name in PREP.TABLES]
    names += ['dim.users', 'dim.event', 'dim.scd_load_state', 'fact.sales', 'fact.listing']
    return {table: warehouse.db.execute('SELECT * FROM '+table).fetchall() for table in names}


def test_generated_csv_headers_match_real_schema(prepared):
    schema = PREP.stage_columns()
    schema['event'] = schema['event'][:6]
    for name in PREP.fixtures():
        for table in PREP.TABLES:
            with (prepared/name/'bronze'/f'{table}.csv').open(newline='') as stream:
                reader = csv.DictReader(stream)
                assert reader.fieldnames == schema[table]
                rows = list(reader)
                assert all(None not in row for row in rows)
                for row in rows:
                    for key in ('saletime', 'listtime', 'starttime'):
                        if key in row:
                            datetime.strptime(row[key], '%Y-%m-%d %H:%M:%S.%f')
    assert sum(p.stat().st_size for p in prepared.glob('*/bronze/*.csv')) < 10_000


def test_initial_and_identical_retry_preserve_all_rows_and_keys(prepared):
    warehouse = Warehouse()
    accept(warehouse, prepared, 'initial', PREP.INITIAL_DAY)
    original = snapshot(warehouse)
    assert len(original['dim.users']) == 2
    assert len(original['dim.event']) == 1
    assert len(original['fact.sales']) == len(original['fact.listing']) == 1
    accept(warehouse, prepared, 'initial', PREP.INITIAL_DAY)
    assert snapshot(warehouse) == original


def test_changes_historical_joins_utc_midnight_and_retry(prepared):
    warehouse = Warehouse()
    accept(warehouse, prepared, 'initial', PREP.INITIAL_DAY)
    accept(warehouse, prepared, 'changed', PREP.CHANGED_DAY)
    rows = warehouse.db.execute('''SELECT f.salesid, u.effective_date, e.effective_date, b.effective_date
        FROM fact.sales f JOIN dim.users u ON u.user_key=f.seller_key
        JOIN dim.users b ON b.user_key=f.buyer_key
        JOIN dim.event e ON e.event_key=f.event_key ORDER BY f.salesid''').fetchall()
    assert rows == [(100, PREP.INITIAL_DAY, PREP.INITIAL_DAY, PREP.INITIAL_DAY),
                    (101, PREP.CHANGED_DAY, PREP.CHANGED_DAY, PREP.INITIAL_DAY),
                    (102, PREP.INITIAL_DAY, PREP.INITIAL_DAY, PREP.INITIAL_DAY)]
    assert warehouse.db.execute('SELECT COUNT(*) FROM dim.users').fetchone()[0] == 3
    assert warehouse.db.execute('SELECT COUNT(*) FROM dim.event').fetchone()[0] == 2
    assert warehouse.db.execute('SELECT city FROM dim.users WHERE userid=1 AND is_current').fetchone()[0] is None
    listing = warehouse.db.execute('''SELECT f.listid, u.effective_date FROM fact.listing f
        JOIN dim.users u ON u.user_key=f.seller_key ORDER BY f.listid''').fetchall()
    assert listing == [(10, PREP.INITIAL_DAY), (11, PREP.INITIAL_DAY), (12, PREP.CHANGED_DAY)]
    before = snapshot(warehouse)
    accept(warehouse, prepared, 'changed', PREP.CHANGED_DAY)
    assert snapshot(warehouse) == before


@pytest.mark.parametrize('name,day,exception,message', [
    ('same_day_conflict', PREP.CHANGED_DAY, ValueError, 'same-day'),
    ('missing_history', PREP.ROLLBACK_DAY, ValueError, 'Missing dimension references'),
    ('rollback_after_sales', PREP.ROLLBACK_DAY, sqlite3.IntegrityError, 'dateid'),
])
def test_conflicts_and_rollback_restore_entire_snapshot(prepared, name, day, exception, message):
    warehouse = Warehouse()
    accept(warehouse, prepared, 'initial', PREP.INITIAL_DAY)
    accept(warehouse, prepared, 'changed', PREP.CHANGED_DAY)
    before = snapshot(warehouse)
    inserted_sales = []
    warehouse.db.create_function('observe_sale', 1, lambda key: inserted_sales.append(key) or 0)
    warehouse.db.execute('''CREATE TEMP TRIGGER observe_fact_insert AFTER INSERT ON fact.sales
        BEGIN SELECT observe_sale(NEW.salesid); END''')
    with pytest.raises(exception, match=message):
        accept(warehouse, prepared, name, day)
    assert snapshot(warehouse) == before
    if name == 'rollback_after_sales':
        assert 103 in inserted_sales  # A real sales INSERT happened before the listing failure.
    else:
        assert not inserted_sales


@pytest.mark.parametrize('raw', [None, '', 123, True, '2026-2-03', '2026-02-30',
                                     '2025-02-29', '2026-10-31T00:00:00Z', ' 2026-10-31',
                                     '2026-11-01', '9999-12-30'])
def test_early_date_validation_rejects_bad_dates(raw):
    with pytest.raises(ValueError):
        VALIDATOR.validate_input({'load_date': raw}, now=NOW)


def test_leap_date_and_business_timezone_contract():
    assert VALIDATOR.validate_input({'load_date': '2024-02-29'}, now=NOW)['business_timezone'] == 'UTC'
    with pytest.raises(ValueError, match='business_timezone'):
        VALIDATOR.validate_input({'load_date': PREP.INITIAL_DAY, 'business_timezone': 'Asia/Kolkata'}, now=NOW)
    offset_clock = datetime(2026, 11, 1, 1, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    with pytest.raises(ValueError, match='Future'):
        VALIDATOR.validate_input({'load_date': '2026-11-01'}, now=offset_clock)


def test_validation_precedes_etl_and_invalid_path_is_terminal(prepared):
    flow = json.loads((prepared/'workflow.template.json').read_text())
    assert flow['StartAt'] == 'ValidateBusinessDate'
    gate = flow['States'][flow['StartAt']]
    assert gate['Next'] == 'ValidateLoadDate'
    assert gate['Parameters']['Payload.$'] == '$'
    assert gate['OutputPath'] == '$.Payload'
    for failure in gate['Catch']:
        assert flow['States'][failure['Next']]['Type'] == 'Fail'
    assert flow['States']['ValidateLoadDate']['Choices'][0]['Next'] == 'ParallelBronzeToSilver'
    assert not any('boto3' in line for line in (ROOT/'tests/isolated/validate_input.py').read_text().splitlines())


def isolation_config():
    account = '000000000000'  # Fictitious test account, never used for API calls.
    region = 'us-east-1'
    config = {'ACCOUNT_ID': account, 'REGION': region, 'BUSINESS_TIMEZONE': 'UTC'}
    for key in ('CATEGORY_LAMBDA_ARN', 'VENUE_LAMBDA_ARN', 'INPUT_VALIDATOR_LAMBDA_ARN', 'TRIGGER_LAMBDA_ARN'):
        config[key] = f'arn:aws:lambda:{region}:{account}:function:test-{key.lower()}'
    config.update(STATE_MACHINE_ARN=f'arn:aws:states:{region}:{account}:stateMachine:test-acceptance',
                  SNS_TOPIC_ARN=f'arn:aws:sns:{region}:{account}:test-acceptance',
                  SNS_QUEUE_ARN=f'arn:aws:sqs:{region}:{account}:test-acceptance',
                  REDSHIFT_COPY_ROLE_ARN=f'arn:aws:iam::{account}:role/test-copy')
    for key in ('S3_BUCKET', 'REDSHIFT_WORKGROUP', 'REDSHIFT_DATABASE', 'USERS_GLUE_JOB',
                'EVENTS_GLUE_JOB', 'LISTINGS_GLUE_JOB', 'SALES_GLUE_JOB'):
        config[key] = 'test-'+key.lower()
    production = {key: value.replace(account, '123456789012').replace('test-', 'production-')
                  if isinstance(value, str) else value for key, value in config.items()}
    production['ACCOUNT_ID'] = '123456789012'
    for key in ('S3_BUCKET', 'REDSHIFT_WORKGROUP', 'REDSHIFT_DATABASE', 'USERS_GLUE_JOB',
                'EVENTS_GLUE_JOB', 'LISTINGS_GLUE_JOB', 'SALES_GLUE_JOB'):
        production[key] = 'production-'+key.lower()
    return config, production


def test_isolation_requires_separate_account_and_does_not_claim_iam_verified():
    candidate, production = isolation_config()
    assert PREP.validate_isolation(candidate, production) == {
        'local_reference_check': 'PASS', 'aws_iam_isolation': 'PENDING', 'resource_existence': 'PENDING'}
    candidate['ACCOUNT_ID'] = '123456789012'
    with pytest.raises(ValueError, match='separate test AWS account'):
        PREP.validate_isolation(candidate, production)


@pytest.mark.parametrize('key', ['CATEGORY_LAMBDA_ARN', 'STATE_MACHINE_ARN', 'SNS_TOPIC_ARN',
                                 'REDSHIFT_COPY_ROLE_ARN', 'REDSHIFT_WORKGROUP', 'S3_BUCKET'])
def test_isolation_rejects_production_references(key):
    candidate, production = isolation_config()
    candidate[key] = production[key]
    with pytest.raises(ValueError):
        PREP.validate_isolation(candidate, production)


def test_actual_pandas_etls_accept_synthetic_csv(prepared, monkeypatch):
    import boto3
    import pandas as pd
    from test_pipeline import FakeS3
    monkeypatch.setenv('TICKIT_BUCKET', 'test-synthetic-bucket')
    monkeypatch.setattr('sys.argv', ['isolated-local-test'])
    inputs = {f'bronze/{table}.csv': (prepared/'changed'/'bronze'/f'{table}.csv').read_bytes() for table in PREP.TABLES}
    for filename, table in [('lambda/category_etl.py', 'category'), ('lambda/venue_etl.py', 'venue'),
                            ('glue/users_etl.py', 'users'), ('glue/events_etl.py', 'event')]:
        fake = FakeS3(inputs)
        monkeypatch.setattr(boto3, 'client', lambda *a, **kw: fake)
        captured = []
        def write(frame, buffer, **kwargs):
            captured.append(frame.copy())
            buffer.write(b'PAR1-local-stub')
        monkeypatch.setattr(pd.DataFrame, 'to_parquet', write)
        module = load(filename)
        if filename.startswith('lambda/'):
            module.lambda_handler({}, None)
        else:
            module.main()
        assert list(captured[0].columns) == PREP.stage_columns()[table]
        assert len(captured[0]) == (2 if table == 'users' else 1)
        if table == 'event':
            assert captured[0].venuename.iloc[0] == 'New Test Hall'
