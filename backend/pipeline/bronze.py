from pathlib import Path
from pyspark.sql import functions as F

SOURCES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

def run_bronze(spark, raw_dir: Path, out_dir: Path) -> dict:
    missing = [f for f in SOURCES.values() if not (raw_dir / f).exists()]
    if missing:
        raise FileNotFoundError(f"Missing CSV files in {raw_dir}: {missing}")
    stats = {}
    for name, fname in SOURCES.items():
        df = (spark.read.option("header", True).option("multiLine", True)
              .option("quote", '"').option("escape", '"')
              .csv(str(raw_dir / fname))  # all columns kept as raw strings
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit(fname)))
        df.write.mode("overwrite").parquet(str(out_dir / name))
        stats[name] = spark.read.parquet(str(out_dir / name)).count()
    return stats
