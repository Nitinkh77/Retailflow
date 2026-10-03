# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver (Delta): clean, validate, reject with reasons
# MAGIC `orders` is loaded **incrementally**: only Bronze rows newer than a stored watermark, applied with Delta `MERGE` (SCD Type 1 upsert).
# MAGIC Other tables are rebuilt each run. Bronze is overwritten by notebook 01, so re-running on the same files reprocesses them, and MERGE keeps that idempotent.

# COMMAND ----------
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
from datetime import datetime, timezone

P = "workspace.retailflow"
ts = lambda c: F.expr(f"try_to_timestamp({c}, 'yyyy-MM-dd HH:mm:ss')")   # try_* works with ANSI on or off
num = lambda c: F.expr(f"try_cast({c} as double)")

def upsert(df, table, keys):
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table); return
    cond = " AND ".join(f"t.{k}=s.{k}" for k in keys)
    (DeltaTable.forName(spark, table).alias("t").merge(df.alias("s"), cond)
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

def split(df, key, rules, null_cols=None):
    w = Window.partitionBy(*key).orderBy(F.col("_ingested_at").desc())
    f = df.withColumn("_rn", F.row_number().over(w))
    allr = {"duplicate_key": F.col("_rn") > 1, **rules}
    reason = F.concat_ws("; ", *[F.when(c, F.lit(r)) for r, c in allr.items()])
    f = f.withColumn("_reject_reason", reason)
    valid = f.filter(F.col("_reject_reason") == "").drop("_rn", "_reject_reason")
    rej = f.filter(F.col("_reject_reason") != "").drop("_rn")
    nulls = F.lit(False)
    for c in (null_cols or key): nulls = nulls | F.col(c).isNull()
    m = f.agg(F.count("*").alias("input_records"),
              F.sum(F.when(F.col("_reject_reason") == "", 1).otherwise(0)).alias("valid_records"),
              F.sum(F.when(F.col("_reject_reason") != "", 1).otherwise(0)).alias("rejected_records"),
              F.sum(F.when(F.col("_rn") > 1, 1).otherwise(0)).alias("duplicate_records"),
              F.sum(F.when(nulls, 1).otherwise(0)).alias("null_records")).first().asDict()
    return valid, rej, m

metrics, run_ts = [], datetime.now(timezone.utc)
def emit(name, df, key, rules, null_cols=None, merge=False):
    v, r, m = split(df, key, rules, null_cols)
    if merge:
        upsert(v, f"{P}.silver_{name}", key); upsert(r, f"{P}.rejected_{name}", key)
    else:
        v.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{P}.silver_{name}")
        r.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{P}.rejected_{name}")
    metrics.append((name, run_ts, *[int(m[k] or 0) for k in
        ["input_records", "valid_records", "rejected_records", "duplicate_records", "null_records"]]))

b = lambda n: spark.table(f"{P}.bronze_{n}")

# COMMAND ----------
# Incremental orders: watermark table holds the last processed _ingested_at
WM = f"{P}.pipeline_watermark"
spark.sql(f"CREATE TABLE IF NOT EXISTS {WM} (table_name STRING, last_ingested_at TIMESTAMP)")
wm = spark.sql(f"SELECT max(last_ingested_at) AS w FROM {WM} WHERE table_name='orders'").first()["w"]
bo = b("orders")
new_wm = bo.agg(F.max("_ingested_at")).first()[0]
if wm is not None: bo = bo.filter(F.col("_ingested_at") > F.lit(wm))
print("orders watermark:", wm, "-> rows to process:", bo.count())

o = (bo.withColumn("order_purchase_timestamp", ts("order_purchase_timestamp"))
       .withColumn("order_delivered_customer_date", ts("order_delivered_customer_date")))
emit("orders", o, ["order_id"], {
    "null_order_id": F.col("order_id").isNull(), "null_customer_id": F.col("customer_id").isNull(),
    "invalid_purchase_timestamp": F.col("order_purchase_timestamp").isNull(),
    "delivered_before_purchase": F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"),
    "delivered_status_missing_date": (F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNull()},
    ["order_id", "customer_id", "order_purchase_timestamp"], merge=True)
if new_wm is not None:
    spark.sql(f"DELETE FROM {WM} WHERE table_name='orders'")
    spark.createDataFrame([("orders", new_wm)], "table_name string, last_ingested_at timestamp") \
         .write.format("delta").mode("append").saveAsTable(WM)

# COMMAND ----------
emit("customers", b("customers"), ["customer_id"],
     {"null_customer_id": F.col("customer_id").isNull(), "null_state": F.col("customer_state").isNull()},
     ["customer_id", "customer_state"])
emit("payments", b("payments").withColumn("payment_value", num("payment_value")), ["order_id", "payment_sequential"], {
    "null_order_id": F.col("order_id").isNull(),
    "invalid_payment_value": F.col("payment_value").isNull() | (F.col("payment_value") < 0),
    "zero_payment_value": F.col("payment_value") == 0,
    "undefined_payment_type": F.col("payment_type") == "not_defined"}, ["order_id", "payment_value"])
emit("order_items", b("order_items").withColumn("price", num("price")).withColumn("freight_value", num("freight_value")),
     ["order_id", "order_item_id"], {
    "null_ids": F.col("order_id").isNull() | F.col("product_id").isNull(),
    "invalid_price": F.col("price").isNull() | (F.col("price") < 0)}, ["order_id", "product_id", "price"])
emit("products", b("products"), ["product_id"],
     {"null_product_id": F.col("product_id").isNull(), "missing_category": F.col("product_category_name").isNull()},
     ["product_id", "product_category_name"])
emit("sellers", b("sellers"), ["seller_id"], {"null_seller_id": F.col("seller_id").isNull()})
emit("reviews", b("reviews").withColumn("review_score", F.expr("try_cast(review_score as int)")),
     ["review_id", "order_id"],
     {"invalid_score": F.col("review_score").isNull() | ~F.col("review_score").between(1, 5)},
     ["review_id", "order_id", "review_score"])

# COMMAND ----------
mdf = spark.createDataFrame(metrics, "table_name string, run_ts timestamp, input_records long, valid_records long, "
                                      "rejected_records long, duplicate_records long, null_records long")
mdf.write.format("delta").mode("append").saveAsTable(f"{P}.quality_metrics")
display(mdf)
