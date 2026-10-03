# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze ingestion (Delta)
# MAGIC Upload the 8 Olist CSVs to the Unity Catalog volume `workspace.retailflow.raw` first (see README).

# COMMAND ----------
from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "retailflow"
P = f"{CATALOG}.{SCHEMA}"
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {P}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {P}.raw")

SOURCES = {
    "customers": "olist_customers_dataset.csv", "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv", "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv", "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv", "category_translation": "product_category_name_translation.csv",
}

# COMMAND ----------
stats = {}
for name, fname in SOURCES.items():
    df = (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
          .csv(f"{RAW}/{fname}")
          .select("*", F.col("_metadata.file_name").alias("_source_file"),
                  F.current_timestamp().alias("_ingested_at")))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{P}.bronze_{name}")
    stats[name] = spark.table(f"{P}.bronze_{name}").count()
display(spark.createDataFrame(list(stats.items()), "table string, rows long"))
