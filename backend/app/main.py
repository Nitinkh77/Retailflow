import json, threading
import pandas as pd
from collections import Counter
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from backend.pipeline.run import run_pipeline, ST, LOG

app = FastAPI(title="RetailFlow API")
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173"], allow_methods=["*"], allow_headers=["*"])
_lock = threading.Lock(); _state = {"running": False}

class RunResponse(BaseModel):
    started: bool
    message: str

def _gold(name):
    p = ST / "gold" / name
    if not p.exists(): raise HTTPException(404, "Gold data not available. Run the pipeline first.")
    return pd.read_parquet(p)

def _job():
    try: run_pipeline()
    finally: _state["running"] = False; _lock.release()

def _logs():
    if not LOG.exists(): return []
    return [json.loads(l) for l in LOG.read_text().splitlines() if l.strip()]

@app.get("/api/health")
def health(): return {"status": "ok"}

@app.post("/api/pipeline/run", response_model=RunResponse, status_code=202)
def run():
    if not _lock.acquire(blocking=False):
        raise HTTPException(409, "Pipeline already running")
    _state["running"] = True
    threading.Thread(target=_job, daemon=True).start()
    return RunResponse(started=True, message="Pipeline started")

@app.get("/api/pipeline/status")
def status():
    logs = _logs()
    last = {k: v for k, v in logs[-1].items() if k != "trace"} if logs else None
    return {"running": _state["running"], "last_run": last}

@app.get("/api/pipeline/logs")
def logs(limit: int = 50):
    return [{k: v for k, v in l.items() if k != "trace"} for l in _logs()[-max(1, min(limit, 500)):][::-1]]

@app.get("/api/dashboard/summary")
def summary():
    p = ST / "gold" / "kpis.json"
    if not p.exists(): raise HTTPException(404, "Run the pipeline first.")
    return json.loads(p.read_text())

@app.get("/api/dashboard/revenue-trend")
def trend(): return _gold("monthly_revenue").to_dict("records")

@app.get("/api/dashboard/top-products")
def top(limit: int = 10): return _gold("top_products").head(max(1, min(limit, 100))).to_dict("records")

@app.get("/api/dashboard/category-revenue")
def cats(): return _gold("category_revenue").head(15).to_dict("records")

@app.get("/api/data/quality")
def quality():
    p = ST / "quality.json"
    if not p.exists(): raise HTTPException(404, "Run the pipeline first.")
    return json.loads(p.read_text())

TABLES = {"orders", "customers", "payments", "order_items", "products", "sellers", "reviews"}

@app.get("/api/data/rejected")
def rejected(table: str = Query(...), limit: int = Query(20, ge=1, le=200)):
    if table not in TABLES:
        raise HTTPException(400, f"table must be one of {sorted(TABLES)}")
    p = ST / "rejected" / table
    if not p.exists():
        raise HTTPException(404, "Run the pipeline first.")
    df = pd.read_parquet(p)
    reasons = Counter(r for s in df["_reject_reason"] for r in str(s).split("; ") if r)
    rows = json.loads(df.head(limit).to_json(orient="records", date_format="iso"))
    return {"table": table, "total": int(len(df)), "by_reason": dict(reasons), "rows": rows}
