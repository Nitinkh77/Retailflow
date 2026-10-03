import json, time, uuid, traceback
from datetime import datetime, timezone
from pathlib import Path
from .spark_session import get_spark
from .bronze import run_bronze
from .silver import run_silver
from .gold import run_gold

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
ST = ROOT / "backend" / "storage"
LOG = ST / "logs" / "runs.jsonl"

def run_pipeline(raw_dir: Path = RAW, storage: Path = ST) -> dict:
    run = {"execution_id": uuid.uuid4().hex[:8], "start_time": datetime.now(timezone.utc).isoformat(),
           "mode": "local-parquet", "stages": [], "status": "RUNNING", "error": None}
    b, s, g, r = (storage / x for x in ("bronze", "silver", "gold", "rejected"))
    try:
        g.mkdir(parents=True, exist_ok=True)
        spark = get_spark()
        def stage(name, fn):
            t = time.time(); out = fn()
            run["stages"].append({"stage": name, "status": "SUCCESS",
                                  "duration_s": round(time.time() - t, 2), "detail": out})
            return out
        bs = stage("bronze", lambda: run_bronze(spark, raw_dir, b))
        sm = stage("silver", lambda: run_silver(spark, b, s, r))
        stage("gold", lambda: run_gold(spark, s, b, g))
        (storage / "quality.json").write_text(json.dumps(sm))
        run["input_records"] = sum(bs.values())
        run["output_records"] = sum(m["valid_records"] for m in sm.values())
        run["status"] = "SUCCESS"
    except Exception as e:
        run["status"] = "FAILED"; run["error"] = f"{type(e).__name__}: {str(e)[:300]}"
        run["trace"] = traceback.format_exc()[-1500:]
    run["end_time"] = datetime.now(timezone.utc).isoformat()
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as f: f.write(json.dumps(run) + "\n")
    return run

if __name__ == "__main__":
    res = run_pipeline(); print(res["status"], res.get("error") or "")
