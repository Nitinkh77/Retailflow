import json
import pandas as pd
import pytest
pytest.importorskip("pyspark"); pytest.importorskip("fastapi")
from fastapi.testclient import TestClient
import backend.app.main as m

@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "ST", tmp_path)
    monkeypatch.setattr(m, "LOG", tmp_path / "logs" / "runs.jsonl")
    return TestClient(m.app)

def test_health(client):
    assert client.get("/api/health").json() == {"status": "ok"}

def test_summary_is_404_before_pipeline_runs(client):
    assert client.get("/api/dashboard/summary").status_code == 404

def test_summary_and_trend_read_gold_files(client, tmp_path):
    g = tmp_path / "gold"; g.mkdir()
    (g / "kpis.json").write_text(json.dumps({"total_orders": 3}))
    pd.DataFrame({"month": ["2018-01"], "revenue": [10.5], "orders": [3]}).to_parquet(g / "monthly_revenue")
    assert client.get("/api/dashboard/summary").json() == {"total_orders": 3}
    assert client.get("/api/dashboard/revenue-trend").json()[0]["month"] == "2018-01"

def test_rejected_validates_table_and_counts_reasons(client, tmp_path):
    assert client.get("/api/data/rejected?table=nope").status_code == 400
    assert client.get("/api/data/rejected?table=orders").status_code == 404
    d = tmp_path / "rejected"; d.mkdir()
    pd.DataFrame({"order_id": ["a", "b"], "_reject_reason": ["x; y", "x"]}).to_parquet(d / "orders")
    body = client.get("/api/data/rejected?table=orders").json()
    assert body["total"] == 2 and body["by_reason"] == {"x": 2, "y": 1}

def test_logs_hide_stack_traces(client, tmp_path):
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "runs.jsonl").write_text(json.dumps({"execution_id": "e1", "status": "FAILED", "trace": "secret"}) + "\n")
    rows = client.get("/api/pipeline/logs").json()
    assert rows[0]["execution_id"] == "e1" and "trace" not in rows[0]

def test_second_simultaneous_run_is_rejected(client):
    assert m._lock.acquire(blocking=False)
    try:
        assert client.post("/api/pipeline/run").status_code == 409
    finally:
        m._lock.release()
