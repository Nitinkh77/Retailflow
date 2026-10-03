import json
import pytest
pytest.importorskip("pyspark")
from backend.pipeline.spark_session import get_spark
from backend.pipeline.gold import run_gold

def test_gold_revenue_excludes_cancelled_and_keeps_unknown_category(tmp_path):
    spark = get_spark("test")
    silver, bronze, gold = tmp_path / "silver", tmp_path / "bronze", tmp_path / "gold"
    gold.mkdir()
    def put(base, name, rows, schema):
        spark.createDataFrame(rows, schema).write.parquet(str(base / name))
    put(silver, "orders", [("o1", "delivered", "2018-01-05 10:00:00"), ("o2", "canceled", "2018-01-06 10:00:00")],
        "order_id string, order_status string, order_purchase_timestamp string")
    spark.read.parquet(str(silver / "orders")).selectExpr(
        "order_id", "order_status", "to_timestamp(order_purchase_timestamp) AS order_purchase_timestamp"
    ).write.mode("overwrite").parquet(str(tmp_path / "o_ts"))
    spark.read.parquet(str(tmp_path / "o_ts")).write.mode("overwrite").parquet(str(silver / "orders_fixed"))
    import shutil; shutil.rmtree(silver / "orders"); (silver / "orders_fixed").rename(silver / "orders")
    put(silver, "payments", [("o1", 100.0, "credit_card"), ("o2", 50.0, "boleto")],
        "order_id string, payment_value double, payment_type string")
    put(silver, "order_items", [("o1", "p1", "s1", 80.0), ("o1", "p2", "s1", 20.0)],
        "order_id string, product_id string, seller_id string, price double")
    put(silver, "products", [("p1", "beleza")], "product_id string, product_category_name string")
    put(silver, "sellers", [("s1", "SP")], "seller_id string, seller_state string")
    put(silver, "customers", [("c1", "SP")], "customer_id string, customer_state string")
    put(silver, "reviews", [(5,)], "review_score int")
    put(bronze, "category_translation", [("beleza", "beauty")],
        "product_category_name string, product_category_name_english string")

    run_gold(spark, silver, bronze, gold)

    kpis = json.loads((gold / "kpis.json").read_text())
    assert kpis["total_revenue"] == 100.0 and kpis["total_orders"] == 1
    cats = {r["category"]: r["revenue"] for r in spark.read.parquet(str(gold / "category_revenue")).collect()}
    assert cats == {"beauty": 80.0, "unknown": 20.0}
    months = spark.read.parquet(str(gold / "monthly_revenue")).collect()
    assert len(months) == 1 and months[0]["month"] == "2018-01" and months[0]["revenue"] == 100.0
