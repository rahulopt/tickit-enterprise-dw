#!/usr/bin/env python3
"""Offline Redshift SQL checks. Does not compile PL/pgSQL or connect to a database.

Optional sqlglot validates standalone DDL/verification SELECTs; procedure bodies
receive conservative structure/signature checks and still require native compile.
"""
import json
import re
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def check():
    sql=re.sub(r'--[^\n]*','',(ROOT/'sql/09_manifest_warehouse.sql').read_text())
    grants=(ROOT/'sql/10_manifest_permissions.sql').read_text()
    assert sql.count('$$')==2
    signature=re.search(r'PROCEDURE sp_load_manifest_warehouse\((.*?)\)\s*AS',sql,re.S)[1]
    kinds=re.findall(r'\bp_\w+\s+(VARCHAR\(\d+\)|INTEGER|BIGINT|DATE)',signature)
    normalized=[re.sub(r'\(\d+\)','',v) for v in kinds]
    assert normalized==['VARCHAR','VARCHAR','VARCHAR','INTEGER','BIGINT','DATE','VARCHAR','VARCHAR','VARCHAR']
    for signature in re.findall(r'public.sp_load_manifest_warehouse\((.*?)\)',grants,re.S):
        assert re.findall(r'VARCHAR|INTEGER|BIGINT|DATE',signature)==normalized
    body=sql.split('$$')[1]
    assert not re.search(r'\b(COMMIT|ROLLBACK|TRUNCATE|NONATOMIC)\b',body,re.I)
    assert 'EXCEPTION WHEN' not in body.upper()
    assert body.index('RETURN;')<body.index("EXECUTE 'DELETE FROM stage.'")
    assert body.index('CALL sp_load_facts')<body.index('INSERT INTO control.batch_commits')
    for match in re.findall(r'CALL (sp_\w+)\(',body):
        assert re.search(r'PROCEDURE '+match+r'\(', (ROOT/'sql/06_stored_procedures.sql').read_text())
    assert 'FROM PUBLIC' in grants and 'sp_load_staging' in grants
    result={'structure_and_signatures':'PASS','native_compile':'PENDING','sqlglot':'NOT_INSTALLED'}
    try:import sqlglot
    except ImportError:return result
    ddl=sql.split('CREATE OR REPLACE PROCEDURE')[0]
    statements=sqlglot.parse(ddl,read='redshift')
    verify=sqlglot.parse((ROOT/'tests/isolated/sql/verify_manifest.sql').read_text(),read='redshift')
    assert all(stmt is not None and type(stmt).__name__!='Command' for stmt in statements+verify)
    # Constructed COPY syntax check. Dynamic SQL/body semantics remain native-only.
    sqlglot.parse_one("COPY stage.sales FROM 's3://test-bucket/batch.copy.json' IAM_ROLE 'ROLE_PLACEHOLDER' FORMAT AS PARQUET MANIFEST",read='redshift')
    result['sqlglot']='PASS';result['sqlglot_version']=sqlglot.__version__
    return result


if __name__=='__main__':print(json.dumps(check(),indent=2))
