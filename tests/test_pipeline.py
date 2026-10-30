"""
Prueba de punta a punta sin AWS ni Confluent reales (S3 y Secrets Manager simulados con moto).
  pip install -r requirements-dev.txt && python -m pytest -q
"""
import csv, gzip, io, json, os, sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lambda"))
SAMPLE = json.loads((ROOT / "examples" / "sample_costs_response.json").read_text())["data"]
BUCKET, ORG = "test-focus-bucket", "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET)
        arn = boto3.client("secretsmanager").create_secret(
            Name="t", SecretString='{"api_key":"k","api_secret":"s"}')["ARN"]
        monkeypatch.setenv("SECRET_ARN", arn)
        monkeypatch.setenv("BUCKET", BUCKET)
        monkeypatch.setenv("ORG_ID", ORG)
        import importlib, handler, confluent_to_focus as c2f
        importlib.reload(handler)
        monkeypatch.setattr(c2f, "fetch_live",
                            lambda s, e: [i for i in SAMPLE if s <= c2f._d(i["start_date"]) < e])
        monkeypatch.setattr(c2f, "_get", lambda u, k, sec: {
            "data": [{"id": "env-abc123", "display_name": "produccion"}], "metadata": {}})
        yield s3, handler


def _rows(s3, key):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return list(csv.DictReader(io.StringIO(gzip.decompress(body).decode())))


def test_drop_valido_para_cloudability(env):
    s3, handler = env
    res = handler.lambda_handler({"today": "2026-10-31"}, None)
    keys = res["uploaded"]
    manifest_key = next(k for k in keys if k.endswith("-Manifest.json"))
    assert manifest_key.startswith("finops-demo/Confluent/20261001-20261101/")
    m = json.loads(s3.get_object(Bucket=BUCKET, Key=manifest_key)["Body"].read())
    assert m["focus_version"] == "1.1" and m["root_dir"] == BUCKET
    assert all(k in keys and not k.startswith(BUCKET) for k in m["all_report_keys"])

    rows = _rows(s3, m["all_report_keys"][0])
    assert len(rows) == len(SAMPLE)
    credit = [r for r in rows if r["ChargeCategory"] == "Credit"]
    assert len(credit) == 1 and credit[0]["ServiceName"] == "Confluent Cloud Credits"
    usage = [r for r in rows if r["ChargeCategory"] == "Usage"]
    assert round(sum(float(r["BilledCost"]) for r in rows), 4) == 0.0       # créditos cubren todo
    assert round(sum(float(r["ListCost"]) for r in usage), 4) == 7.8497
    assert {r["SubAccountName"] for r in usage} == {"produccion"}
    for r in rows:                                                          # Tags = JSON plano de strings
        assert all(isinstance(v, str) for v in json.loads(r["Tags"]).values())


def test_inicio_de_mes_regenera_mes_anterior(env):
    _, handler = env
    res = handler.lambda_handler({"today": "2026-11-02"}, None)
    assert [m["month"] for m in res["months"]] == ["2026-10", "2026-11"]


def test_validacion_rechaza_columna_obligatoria_vacia():
    import confluent_to_focus as c2f
    row = c2f.to_focus(SAMPLE[0], ORG, "Org", {}, "USD")
    assert c2f.validate([row]) == []
    row["BillingAccountId"] = ""
    assert c2f.validate([row])
