"""Offline regression tests for the TICKIT pipeline (no AWS credentials needed)."""
import importlib.util
import io
import json
import os
import re
from pathlib import Path

import boto3
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("AWS_DEFAULT_REGION", "ap-south-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
os.environ["TICKIT_BUCKET"] = "test-tickit-bucket"
os.environ["STATE_MACHINE_ARN"] = "arn:aws:states:us-east-1:123456789012:stateMachine:test-pipeline"


def load(filename):
    path = ROOT / filename
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeS3:
    def __init__(self, content):
        self.objects = content.copy()
        self.deletion_batches = []

    def get_object(self, *, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, *, Bucket, Key, Body):
        self.objects[Key] = Body

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return self

    def paginate(self, *, Bucket, Prefix):
        return [{"Contents": [{"Key": key} for key in self.objects if key.startswith(Prefix)]}]

    def delete_objects(self, *, Bucket, Delete):
        assert len(Delete["Objects"]) <= 1000
        self.deletion_batches.append(Delete["Objects"])
        for obj in Delete["Objects"]:
            self.objects.pop(obj["Key"], None)
        return {"Deleted": Delete["Objects"]}


@pytest.fixture
def fake_parquet(monkeypatch):
    seen = []

    def stub(self, buffer, *args, **kwargs):
        seen.append(self.copy())
        buffer.write(b"PAR1-stub")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", stub)
    return seen


def test_category_filters_bad_keys_and_preserves_new_output(fake_parquet):
    module = load("lambda/category_etl.py")
    client = FakeS3({
        "bronze/category.csv": b"catid,catgroup,catname,catdesc\n1, shows,Rock,Song\nbad,shows,Old,Wrong\n2,sports,Football,Game\n",
        "tickit-v2/silver/category/stale.parquet": b"OLD",
    })
    module.s3_client = client
    result = module.lambda_handler({}, None)
    assert result["statusCode"] == 200
    assert json.loads(result["body"])["rows_written"] == 2
    assert "tickit-v2/silver/category/stale.parquet" not in client.objects
    assert client.objects["tickit-v2/silver/category/category.parquet"] == b"PAR1-stub"
    assert list(fake_parquet[0].catid) == [1, 2]
    assert fake_parquet[0].catgroup.tolist() == ["SHOWS", "SPORTS"]


def test_venue_invalid_seats_default_zero(fake_parquet):
    module = load("lambda/venue_etl.py")
    module.s3_client = FakeS3({
        "bronze/venue.csv": b"venueid,venuename,venuecity,venuestate,venueseats\n1,Hall,Austin,tx,abc\n2,Park,Miami,fl,100\n",
    })
    result = module.lambda_handler({}, None)
    assert result["statusCode"] == 200
    assert fake_parquet[0].venueseats.tolist() == [0, 100]


def test_venue_upload_failure_does_not_clear_silver(fake_parquet):
    module = load("lambda/venue_etl.py")
    class FailingS3(FakeS3):
        def put_object(self, **kwargs):
            raise ValueError("storage failure")
    module.s3_client = FailingS3({
        "bronze/venue.csv": b"venueid,venuename,venuecity,venuestate,venueseats\n1,Hall,Austin,TX,100\n",
        "tickit-v2/silver/venue/old.parquet": b"GOOD",
    })
    with pytest.raises(ValueError, match="storage failure"):
        module.lambda_handler({}, None)
    assert module.s3_client.objects["tickit-v2/silver/venue/old.parquet"] == b"GOOD"


def test_events_lookup_duplicate_does_not_multiply_event_rows(fake_parquet):
    module = load("glue/events_etl.py")
    module.s3_client = FakeS3({
        "bronze/event.csv": b"eventid,venueid,catid,dateid,eventname,starttime\n11,1,2,1827,Concert,2008-01-01 20:00:00\n",
        "bronze/venue.csv": b"venueid,venuename,venuecity,venuestate,venueseats\n1,Old,Miami,FL,80\n1,New,Miami,FL,100\n",
        "bronze/category.csv": b"catid,catgroup,catname,catdesc\n2,Shows,Rock,Description\n",
    })
    module.main()
    assert len(fake_parquet[0]) == 1
    assert fake_parquet[0].venuename.iloc[0] == "New"


def test_trigger_queues_immutable_manifest_without_starting_workflow(monkeypatch):
    from test_admission import batch
    module = load("lambda/s3_trigger.py")
    item = batch()
    document = item.data
    document['bucket'] = 'test-tickit-bucket'
    calls = []
    class QueueOnly:
        def enqueue(self, manifest):
            calls.append(manifest)
            return 'durable-message'
    monkeypatch.setattr(module, '_services', lambda: (QueueOnly(), lambda *args: json.dumps(document)))
    event = {"Records": [{"s3": {"bucket": {"name": "test-tickit-bucket"},
                               "object": {"key": "tickit-v2/batches/batch-a/input-manifest.json", "versionId": "version-1"}}}]}
    result = module.lambda_handler(event, None)
    assert json.loads(result['body'])['results'][0]['status'] == 'QUEUED'
    assert calls[0]['load_date'] == '2026-10-29'


@pytest.mark.parametrize('key', ['bronze/.done', 'bronze/tickit.done', 'bronze/other.done'])
def test_trigger_legacy_markers_fail_loudly_without_any_aws_client(key, monkeypatch):
    module = load("lambda/s3_trigger.py")
    monkeypatch.setattr(module, '_services', lambda: pytest.fail('No SDK use for invalid notifications'))
    event = {"Records": [{"s3": {"bucket": {"name": "test-tickit-bucket"},
                                "object": {"key": key, "versionId": "version-1"}}}]}
    with pytest.raises(ValueError, match='legacy'):
        module.lambda_handler(event, None)


def test_trigger_transport_failure_is_retried_not_skipped(monkeypatch):
    from test_admission import batch
    module = load("lambda/s3_trigger.py")
    document = dict(batch().data, bucket='test-tickit-bucket')
    class Unavailable:
        def enqueue(self, manifest):
            raise TimeoutError('Durable queue is unavailable')
    monkeypatch.setattr(module, '_services', lambda: (Unavailable(), lambda *args: json.dumps(document)))
    event = {"Records": [{"s3": {"bucket": {"name": "test-tickit-bucket"},
                                "object": {"key": "tickit-v2/batches/batch-a/input-manifest.json", "versionId": "version-1"}}}]}
    with pytest.raises(TimeoutError):
        module.lambda_handler(event, None)


def test_trigger_non_notification_and_test_event():
    module = load("lambda/s3_trigger.py")
    assert module.lambda_handler({"Event": "s3:TestEvent"}, None)["statusCode"] == 200
    assert module.lambda_handler({}, None)["statusCode"] == 400


def test_sql_calendar_generates_all_dates_and_source_keys():
    from datetime import date, timedelta
    text = (ROOT / "sql/05_load_dim_date.sql").read_text()
    assert "CROSS JOIN digits d3" in text
    assert "'2003-01-01'::DATE" in text
    assert "n + 1 AS dateid" in text
    assert "n BETWEEN 1826 AND 4381" in text
    ids = [1000*a + 100*b + 10*c + d + 1
           for a in range(10) for b in range(10)
           for c in range(10) for d in range(10)
           if 1826 <= 1000*a + 100*b + 10*c + d <= 4381]
    assert sorted(ids) == list(range(1827, 4383))
    base = date(2003, 1, 1)
    assert base + timedelta(days=1827 - 1) == date(2008, 1, 1)
    assert base + timedelta(days=4382 - 1) == date(2014, 12, 30)


def test_sql_staging_is_transaction_safe_and_rejects_missing_refs():
    text = (ROOT / "sql/06_stored_procedures.sql").read_text()
    assert "TRUNCATE TABLE stage." not in text
    assert text.count("QUOTE_LITERAL(v_s3_base ||") == 6
    assert "Missing dimension references" in text
    for attr in ("username", "dateid", "catdesc", "starttime"):
        assert re.search(rf"DECODE\(dim\.\w+\.{attr}, s\.{attr}", text)


def test_workflow_all_references_exist_and_failed_statuses_captured():
    renderer = load("tools/tickit_workflow.py")
    config = json.loads((ROOT / "stepfunctions/config.example.json").read_text())
    for key in config:
        if not key.endswith("SECONDS"):
            config[key] = "test-resource"
    config.update({
        "CATEGORY_LAMBDA_ARN": "arn:aws:lambda:us-east-1:123456789012:function:category",
        "VENUE_LAMBDA_ARN": "arn:aws:lambda:us-east-1:123456789012:function:venue",
        "REDSHIFT_COPY_ROLE_ARN": "arn:aws:iam::123456789012:role/redshift",
        "SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:notifications",
        "S3_BUCKET": "test-tickit-bucket",
    })
    flow = renderer.render(
        json.loads((ROOT / "stepfunctions/tickit_pipeline_v2.template.json").read_text()), config
    )
    assert flow["TimeoutSeconds"] > 0
    states = flow["States"]
    def validate_graph(graph):
        for name, state in graph.items():
            for pointer in [state.get("Next"), state.get("Default")]:
                if pointer:
                    assert pointer in graph, (name, pointer)
            for branch in state.get("Branches", []):
                assert branch["StartAt"] in branch["States"]
                validate_graph(branch["States"])
            for entry in state.get("Choices", []) + state.get("Catch", []):
                assert entry["Next"] in graph, (name, entry)
    validate_graph(states)
    for stage in ("Staging",):
        choices = states[f"Is{stage}Done"]["Choices"]
        assert any(c.get("StringEquals") == "FAILED" and c["Next"] == f"Capture{stage}Failure" for c in choices)
        assert any(c.get("StringEquals") == "ABORTED" for c in choices)
        assert states[f"Notify{stage}Failed"]["Parameters"]["Message.$"] == "States.JsonToString($.error)"


def test_redshift_copy_places_auth_before_format():
    text = (ROOT / "sql/06_stored_procedures.sql").read_text()
    assert "'IAM_ROLE ' || QUOTE_LITERAL(p_iam_role_arn) || ' FORMAT AS PARQUET'" in text
    assert "'FORMAT AS PARQUET IAM_ROLE '" not in text


def test_readme_includes_date_load_before_analytical_views():
    text = (ROOT / "README.md").read_text()
    assert text.index("05_load_dim_date.sql") < text.index("06_stored_procedures.sql")
    assert text.index("06_stored_procedures.sql") < text.index("07_analytical_views.sql")


def test_calendar_reruns_preserve_existing_rows():
    text = (ROOT / "sql/05_load_dim_date.sql").read_text()
    assert "TRUNCATE TABLE dim.date" not in text
    assert "WHERE existing.dateid = dates.dateid" in text


def test_v2_staging_reads_the_etl_output_prefix():
    text = (ROOT / "sql/06_stored_procedures.sql").read_text()
    assert "p_s3_bucket || '/tickit-v2/silver/'" in text
    for name in ("category", "venue"):
        module = load(f"lambda/{name}_etl.py")
        assert module.SILVER_PREFIX == f"tickit-v2/silver/{name}/"


def test_monthly_sales_uses_transaction_time():
    text = (ROOT / "sql/07_analytical_views.sql").read_text()
    view = text.split("CREATE OR REPLACE VIEW v_monthly_sales AS", 1)[1].split(";", 1)[0]
    assert "DATE_PART(year, s.saletime)" in view
    assert "DATE_PART(month, s.saletime)" in view
    assert "s.saletime IS NOT NULL" in view
    assert "dateid" not in view
