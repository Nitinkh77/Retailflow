from pyspark.sql import functions as F
from .quality import split_valid_rejected

def _ts(c): return F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss")
def _num(c): return F.col(c).cast("double")

def run_silver(spark, bronze, silver, rejected_dir):
    rd = lambda n: spark.read.parquet(str(bronze / n))
    metrics = {}
    def emit(name, df, key, rules, null_cols=None):
        v, r, m = split_valid_rejected(df, key, rules, null_cols)
        v.write.mode("overwrite").parquet(str(silver / name))
        r.write.mode("overwrite").parquet(str(rejected_dir / name))
        metrics[name] = m

    o = (rd("orders").withColumn("order_purchase_timestamp", _ts("order_purchase_timestamp"))
         .withColumn("order_delivered_customer_date", _ts("order_delivered_customer_date")))
    emit("orders", o, ["order_id"], {
        "null_order_id": F.col("order_id").isNull(),
        "null_customer_id": F.col("customer_id").isNull(),
        "invalid_purchase_timestamp": F.col("order_purchase_timestamp").isNull(),
        "delivered_before_purchase": F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"),
        "delivered_status_missing_date": (F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNull()},
        ["order_id", "customer_id", "order_purchase_timestamp"])

    emit("customers", rd("customers"), ["customer_id"],
         {"null_customer_id": F.col("customer_id").isNull(),
          "null_state": F.col("customer_state").isNull()}, ["customer_id", "customer_state"])

    p = rd("payments").withColumn("payment_value", _num("payment_value"))
    emit("payments", p, ["order_id", "payment_sequential"], {
        "null_order_id": F.col("order_id").isNull(),
        "invalid_payment_value": F.col("payment_value").isNull() | (F.col("payment_value") < 0),
        "zero_payment_value": F.col("payment_value") == 0,
        "undefined_payment_type": F.col("payment_type") == "not_defined"},
        ["order_id", "payment_value"])

    it = rd("order_items").withColumn("price", _num("price")).withColumn("freight_value", _num("freight_value"))
    emit("order_items", it, ["order_id", "order_item_id"], {
        "null_ids": F.col("order_id").isNull() | F.col("product_id").isNull(),
        "invalid_price": F.col("price").isNull() | (F.col("price") < 0)},
        ["order_id", "product_id", "price"])

    emit("products", rd("products"), ["product_id"],
         {"null_product_id": F.col("product_id").isNull(),
          "missing_category": F.col("product_category_name").isNull()},
         ["product_id", "product_category_name"])
    emit("sellers", rd("sellers"), ["seller_id"], {"null_seller_id": F.col("seller_id").isNull()})

    rv = rd("reviews").withColumn("review_score", F.col("review_score").cast("int"))
    emit("reviews", rv, ["review_id", "order_id"], {
        "invalid_score": F.col("review_score").isNull() | ~F.col("review_score").between(1, 5)},
        ["review_id", "order_id", "review_score"])
    return metrics
