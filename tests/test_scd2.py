"""Execute actual loader SQL with SQLite syntax adapters, not a second SCD model.

PL/pgSQL exception/locking/COPY behavior still requires live Redshift tests.
"""
import json
import re
import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / 'sql/06_stored_procedures.sql').read_text()


def body(name):
    proc = SQL.split('CREATE OR REPLACE PROCEDURE ' + name + '(', 1)[1]
    return re.sub(r'--[^\n]*', '', proc.split('BEGIN', 1)[1].split('EXCEPTION WHEN', 1)[0].split('$$ LANGUAGE', 1)[0])


class Warehouse:
    def __init__(self):
        self.db = sqlite3.connect(':memory:')
        self.db.create_function('DECODE', 4, lambda a, b, yes, no: yes if a == b else no)
        for schema in ('stage', 'dim', 'fact'):
            self.db.execute(f"ATTACH DATABASE ':memory:' AS {schema}")
        # Use real DDL column lists; adapt physical design/identity only.
        for filename in ('02_stage_tables.sql', '03_dim_tables.sql', '04_fact_tables.sql'):
            ddl = re.sub(r'--[^\n]*', '', (ROOT / 'sql' / filename).read_text())
            for table, columns in re.findall(r'CREATE TABLE (\w+\.\w+)\s*\((.*?)\)\s*(?:DIST|SORT|COMPOUND)', ddl, re.S):
                columns = columns.replace('DEFAULT GETDATE()', 'DEFAULT CURRENT_TIMESTAMP')
                columns = re.sub(r'BIGINT\s+IDENTITY\(1,1\)\s+NOT NULL', 'INTEGER PRIMARY KEY AUTOINCREMENT', columns)
                self.db.execute(f'CREATE TABLE {table} ({columns})')
        self.db.execute('CREATE TABLE dim.scd_load_state (load_date TEXT)')

    def adapt(self, sql, day):
        sql = sql.replace('p_load_date', 'NULL' if day is None else "'" + day + "'")
        sql = re.sub(r'(\w+\.\w+)::DATE', r'date(\1)', sql)
        return re.sub(r'::(?:DATE|INTEGER)', '', sql)

    def run(self, name, day):
        # Interpret only control-flow nodes used by the loader. SQL stays intact.
        pattern = (r"IF\s+(.*?)\s+THEN\s+RAISE EXCEPTION\s+'([^']*)'[^;]*;\s*END IF;"
                   r'|CALL (sp_validate_dimension_history)\(\);'
                   r'|((?:UPDATE|INSERT INTO|SELECT COUNT\(\*\) INTO)\s+.*?;)')
        variables = {}
        for m in re.finditer(pattern, body(name), re.S):
            condition, message, call, statement = m.groups()
            if condition:
                condition = self.adapt(condition, day)
                for key, value in variables.items():
                    condition = condition.replace(key, str(value))
                if self.db.execute('SELECT ' + condition).fetchone()[0]:
                    raise ValueError(message)
            elif call:
                self.run(call, day)
            elif statement.startswith('SELECT COUNT'):
                variable = re.search(r'INTO (\w+)', statement)[1]
                query = re.sub(r' INTO \w+', '', statement)
                variables[variable] = self.db.execute(self.adapt(query, day)).fetchone()[0]
            else:
                self.db.execute(self.adapt(statement, day))

    def load(self, day, facts=False):
        with self.db:
            self.run('sp_load_dimensions', day)
            if facts:
                self.run('sp_load_facts', day)

    def seed(self):
        self.db.execute("INSERT INTO stage.users(userid,city) VALUES (1,'Austin'),(2,'Miami')")
        self.db.execute("INSERT INTO stage.event(eventid,eventname) VALUES (11,'Concert')")
        self.db.commit()

    def sale(self, key, timestamp):
        self.db.execute('INSERT INTO stage.sales(salesid,eventid,sellerid,buyerid,saletime,dateid,listid,qtysold,pricepaid,commission) VALUES (?,11,1,2,?,1827,1,1,10,1)', (key, timestamp))
        self.db.commit()


@pytest.fixture
def warehouse():
    w = Warehouse()
    w.seed()
    return w


def test_versions_nulls_retries_and_denormalized_change(warehouse):
    w = warehouse
    w.load('2026-01-01')
    old = w.db.execute('SELECT user_key FROM dim.users WHERE userid=1').fetchone()[0]
    w.load('2026-01-01')
    assert w.db.execute('SELECT COUNT(*) FROM dim.users').fetchone()[0] == 2
    w.db.execute('UPDATE stage.users SET city=NULL WHERE userid=1')
    w.db.execute("UPDATE stage.event SET venuename='New Hall'")
    w.db.commit()
    w.load('2026-01-02')
    assert w.db.execute('SELECT effective_date,expiry_date,is_current FROM dim.users WHERE user_key=?', (old,)).fetchone() == ('2026-01-01', '2026-01-02', 0)
    assert w.db.execute('SELECT COUNT(*) FROM dim.event').fetchone()[0] == 2
    w.load('2026-01-02')
    w.db.execute("UPDATE stage.users SET city='Boston' WHERE userid=1")
    w.db.commit()
    w.load('2026-01-03')
    assert w.db.execute('SELECT COUNT(*) FROM dim.users WHERE userid=1').fetchone()[0] == 3


def test_late_facts_boundary_role_keys_and_retry(warehouse):
    w = warehouse
    w.load('2026-01-01')
    old = w.db.execute('SELECT user_key FROM dim.users WHERE userid=1').fetchone()[0]
    w.db.execute("UPDATE stage.users SET city='Boston' WHERE userid=1")
    w.db.commit()
    w.load('2026-01-02')
    new = w.db.execute('SELECT user_key FROM dim.users WHERE userid=1 AND is_current').fetchone()[0]
    w.sale(100, '2026-01-01 23:59:59')
    w.sale(101, '2026-01-02 00:00:00')
    w.db.execute("INSERT INTO stage.listing(listid,sellerid,eventid,dateid,numtickets,priceperticket,totalprice,listtime) VALUES (1,1,11,1827,1,10,10,'2026-01-01 12:00:00')")
    w.db.commit()
    w.load('2026-01-02', facts=True)
    assert w.db.execute('SELECT seller_key FROM fact.sales ORDER BY salesid').fetchall() == [(old,), (new,)]
    assert w.db.execute('SELECT seller_key FROM fact.listing').fetchone()[0] == old
    assert w.db.execute('SELECT COUNT(DISTINCT buyer_key) FROM fact.sales').fetchone()[0] == 1
    w.load('2026-01-02', facts=True)
    assert w.db.execute('SELECT COUNT(*) FROM fact.sales').fetchone()[0] == 2


@pytest.mark.parametrize('day,match', [('2026-01-01', 'same-day'), ('2025-12-31', 'Out-of-order')])
def test_bad_snapshot_rejected_without_mutation(warehouse, day, match):
    w = warehouse
    w.load('2026-01-01')
    w.db.execute("UPDATE stage.users SET city='Boston'")
    w.db.commit()
    with pytest.raises(ValueError, match=match):
        w.load(day)
    assert w.db.execute('SELECT COUNT(*) FROM dim.users').fetchone()[0] == 2
    assert w.db.execute('SELECT COUNT(*) FROM dim.users WHERE is_current').fetchone()[0] == 2


def test_unchanged_day_advances_watermark(warehouse):
    w = warehouse
    w.load('2026-01-01')
    w.load('2026-01-03')
    with pytest.raises(ValueError, match='Out-of-order'):
        w.load('2026-01-02')


@pytest.mark.parametrize('timestamp', ['2025-12-31 23:59:59', None, '2026-01-03 00:00:00'])
def test_missing_history_null_future_fails_and_rolls_back(warehouse, timestamp):
    w = warehouse
    w.load('2026-01-01')
    w.db.execute("UPDATE stage.users SET city='Boston' WHERE userid=1")
    w.db.commit()
    w.sale(100, timestamp)
    with pytest.raises(ValueError, match='Missing dimension references'):
        w.load('2026-01-02', facts=True)
    assert w.db.execute('SELECT COUNT(*) FROM dim.users').fetchone()[0] == 2
    assert w.db.execute('SELECT COUNT(*) FROM dim.scd_load_state').fetchone()[0] == 1
    assert w.db.execute('SELECT COUNT(*) FROM fact.sales').fetchone()[0] == 0


@pytest.mark.parametrize('mutation', [
    "UPDATE dim.users SET expiry_date='2025-12-31' WHERE userid=1",
    "UPDATE dim.users SET is_current=FALSE WHERE userid=1",
    "INSERT INTO dim.users(userid,effective_date,expiry_date,is_current) VALUES (1,'2026-01-01','9999-12-31',TRUE)",
])
def test_invalid_history_rejected(warehouse, mutation):
    w = warehouse
    w.load('2026-01-01')
    w.db.execute(mutation)
    w.db.commit()
    with pytest.raises(ValueError, match='Invalid dim.users history'):
        w.load('2026-01-02')


def test_duplicate_stage_keys_rejected(warehouse):
    w = warehouse
    w.db.execute('INSERT INTO stage.users(userid) VALUES (1)')
    w.db.commit()
    with pytest.raises(ValueError, match='unique non-null userid'):
        w.load('2026-01-01')
    assert w.db.execute('SELECT COUNT(*) FROM dim.users').fetchone()[0] == 0


def test_workflow_binds_snapshot_and_uses_atomic_wrapper():
    flow = json.loads((ROOT / 'stepfunctions/tickit_pipeline_v2.template.json').read_text())
    assert flow['StartAt'] == 'ValidateLoadDate'
    states = flow['States']
    params = states['LoadStaging']['Parameters']
    assert 'sp_load_warehouse' in params['Sql']
    assert 'CURRENT_DATE' not in json.dumps(flow)
    assert params['Parameters'] == [{'Name': 'load_date', 'Value.$': '$.load_date'}]
    assert states['IsStagingDone']['Choices'][0]['Next'] == 'NotifySuccess'
    wrapper = body('sp_load_warehouse')
    assert wrapper.index('sp_load_staging') < wrapper.index('sp_load_dimensions') < wrapper.index('sp_load_facts')


@pytest.mark.parametrize('table,key,attribute,value', [
    ('users', 'userid', 'username', 'newuser'),
    ('users', 'userid', 'likesports', True),
    ('event', 'eventid', 'starttime', '2026-03-01 18:00:00'),
    ('event', 'eventid', 'catdesc', 'New description'),
    ('event', 'eventid', 'dateid', 2000),
])
def test_business_attribute_change_creates_version(warehouse, table, key, attribute, value):
    w = warehouse
    w.load('2026-01-01')
    target = 1 if table == 'users' else 11
    w.db.execute(f'UPDATE stage.{table} SET {attribute}=? WHERE {key}=?', (value, target))
    w.db.commit()
    w.load('2026-01-02')
    assert w.db.execute(f'SELECT COUNT(*) FROM dim.{table} WHERE {key}=?', (target,)).fetchone()[0] == 2


def test_changed_retry_after_unchanged_snapshot_rejected(warehouse):
    w = warehouse
    w.load('2026-01-01')
    w.load('2026-01-02')
    w.db.execute("UPDATE stage.users SET city='Boston' WHERE userid=1")
    w.db.commit()
    with pytest.raises(ValueError, match='same-day'):
        w.load('2026-01-02')


def test_new_key_on_processed_day_rejected(warehouse):
    w = warehouse
    w.load('2026-01-01')
    w.db.execute('INSERT INTO stage.users(userid) VALUES (3)')
    w.db.commit()
    with pytest.raises(ValueError, match='same-day'):
        w.load('2026-01-01')
    w.load('2026-01-02')
    assert w.db.execute('SELECT COUNT(*) FROM dim.users').fetchone()[0] == 3


@pytest.mark.parametrize('expiry', ['2026-01-01', '2026-01-03'])
def test_history_gap_or_overlap_rejected(warehouse, expiry):
    w = warehouse
    w.load('2026-01-01')
    w.db.execute("UPDATE stage.users SET city='Boston' WHERE userid=1")
    w.db.commit()
    w.load('2026-01-02')
    w.db.execute('UPDATE dim.users SET expiry_date=? WHERE userid=1 AND NOT is_current', (expiry,))
    w.db.commit()
    with pytest.raises(ValueError, match='Invalid dim.users history'):
        w.load('2026-01-03')


def test_missing_source_key_retains_history(warehouse):
    w = warehouse
    w.load('2026-01-01')
    w.db.execute('DELETE FROM stage.users WHERE userid=1')
    w.db.commit()
    w.load('2026-01-02')
    assert w.db.execute('SELECT is_current FROM dim.users WHERE userid=1').fetchone()[0] == 1


def test_renderer_extracts_combined_warehouse_definition():
    import importlib.util
    spec = importlib.util.spec_from_file_location('workflow', ROOT / 'tools/tickit_workflow.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    flow = json.loads((ROOT / 'stepfunctions/tickit_pipeline_v2.template.json').read_text())
    # Extraction works on the new graph without obsolete dimension/fact polling states.
    config = module.extract(flow)
    assert config['S3_BUCKET'] == '{{S3_BUCKET}}'
    assert config['WAIT_DIMENSIONS_SECONDS'] == 5


@pytest.mark.parametrize('day', [None, '9999-12-31'])
def test_invalid_snapshot_date(warehouse, day):
    with pytest.raises(ValueError, match='non-null load date'):
        warehouse.load(day)


def test_duplicate_facts_rejected(warehouse):
    w = warehouse
    w.load('2026-01-01')
    w.sale(100, '2026-01-01 01:00:00')
    w.sale(100, '2026-01-01 01:00:00')
    with pytest.raises(ValueError, match='unique non-null salesid'):
        w.load('2026-01-01', facts=True)
    assert w.db.execute('SELECT COUNT(*) FROM fact.sales').fetchone()[0] == 0
