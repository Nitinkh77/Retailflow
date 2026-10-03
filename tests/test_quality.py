import pytest
pytest.importorskip("pyspark")
from pyspark.sql import functions as F
from backend.pipeline.spark_session import get_spark
from backend.pipeline.quality import split_valid_rejected

def test_duplicates_nulls_and_negative_amounts():
    spark = get_spark("test")
    rows = [("o1", 10.0), ("o1", 10.0), ("o2", -5.0), (None, 3.0), ("o3", 7.0)]
    df = (spark.createDataFrame(rows, "order_id string, amount double")
          .withColumn("_ingested_at", F.current_timestamp()))
    rules = {"neg": F.col("amount") < 0, "null_id": F.col("order_id").isNull()}
    v, r, m = split_valid_rejected(df, ["order_id"], rules, ["order_id"])
    assert m["input_records"] == 5
    assert m["duplicate_records"] == 1      # second copy of o1
    assert m["null_records"] == 1           # the missing order_id
    assert m["valid_records"] == 2          # first o1 and o3
    assert m["rejected_records"] == 3       # duplicate o1, negative o2, null id
    reasons = " ".join(x["_reject_reason"] for x in r.collect())
    assert "duplicate_key" in reasons and "neg" in reasons and "null_id" in reasons
    assert {x["order_id"] for x in v.collect()} == {"o1", "o3"}
