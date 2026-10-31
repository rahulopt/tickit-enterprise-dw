"""Configuration boundaries for standalone Lambda/Glue scripts and publishing."""
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load(filename):
    spec = importlib.util.spec_from_file_location(Path(filename).stem, ROOT / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('filename', ['lambda/category_etl.py', 'lambda/venue_etl.py'])
def test_lambda_requires_configured_bucket(filename, monkeypatch):
    monkeypatch.delenv('TICKIT_BUCKET', raising=False)
    with pytest.raises(KeyError, match='TICKIT_BUCKET'):
        load(filename)


@pytest.mark.parametrize('filename', ['glue/users_etl.py', 'glue/events_etl.py'])
def test_python_shell_accepts_job_bucket_argument(filename, monkeypatch):
    monkeypatch.delenv('TICKIT_BUCKET', raising=False)
    monkeypatch.setattr('sys.argv', [filename, '--TICKIT_BUCKET', 'test-job-bucket'])
    assert load(filename).S3_BUCKET == 'test-job-bucket'


@pytest.mark.parametrize('filename', ['glue/users_etl.py', 'glue/events_etl.py'])
def test_python_shell_rejects_missing_bucket(filename, monkeypatch):
    monkeypatch.delenv('TICKIT_BUCKET', raising=False)
    monkeypatch.setattr('sys.argv', [filename])
    with pytest.raises(ValueError, match='TICKIT_BUCKET'):
        load(filename)


def test_workflow_supplies_bucket_to_every_glue_job():
    flow = json.loads((ROOT / 'stepfunctions/tickit_pipeline_v2.template.json').read_text())
    jobs = [state for branch in flow['States']['ParallelBronzeToSilver']['Branches']
            for state in branch['States'].values()
            if state.get('Resource') == 'arn:aws:states:::glue:startJobRun.sync']
    assert len(jobs) == 4
    assert all(job['Parameters']['Arguments']['--TICKIT_BUCKET'] == '{{S3_BUCKET}}' for job in jobs)


def test_permissions_render_for_configured_identity():
    module = load('tools/tickit_workflow.py')
    template = (ROOT / 'sql/08_workflow_permissions.sql').read_text()
    rendered = module.render_permissions(template, 'IAMR:test-workflow-role')
    assert '{{WORKFLOW_DB_USER}}' not in rendered
    assert 'TO "IAMR:test-workflow-role";' in rendered


@pytest.mark.parametrize('identity', [None, '', 'user"; DROP TABLE dim.users;--', 'user name', 'IAMR:REPLACE_WITH_ROLE'])
def test_permissions_reject_missing_or_unsafe_identity(identity):
    module = load('tools/tickit_workflow.py')
    with pytest.raises(ValueError):
        module.render_permissions('TO "{{WORKFLOW_DB_USER}}";', identity)


@pytest.mark.parametrize('path,content', [
    ('lambda/example.py', 'arn:aws:states:us-east-1:123456789012:stateMachine:example'),
    ('.env', 'TICKIT_BUCKET=local-value'),
    ('sql/example.rendered.sql', 'GRANT USAGE ON SCHEMA public TO local_user'),
    ('example.txt', 'AWS_SECRET_ACCESS_KEY=some-secret-value'),
])
def test_publication_check_rejects_private_content(path, content):
    assert load('tools/check_public_config.py').violations(path, content.encode())


def test_publication_check_allows_placeholder_examples_and_service_arns():
    module = load('tools/check_public_config.py')
    assert not module.violations('.env.example', b'TICKIT_BUCKET=REPLACE_WITH_YOUR_BUCKET')
    assert not module.violations('workflow.json', b'arn:aws:states:::lambda:invoke')
    assert not module.violations('tests/fixture.py', b'arn:aws:iam::123456789012:role/example')
