# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold (Delta): business aggregates with Spark SQL
# MAGIC Revenue = sum of payments on non-cancelled orders. `gold_daily_sales` is partitioned by year.

# COMMAND ----------
P = "workspace.retailflow"
cat = "COALESCE(t.product_category_name_english, pr.product_category_name, 'unknown')"
jc = f"LEFT JOIN {P}.silver_products pr ON i.product_id=pr.product_id LEFT JOIN {P}.bronze_category_translation t ON pr.product_category_name=t.product_category_name"
live = f"FROM {P}.silver_orders o JOIN {P}.silver_payments p USING(order_id) WHERE o.order_status<>'canceled'"
Q = {
 "monthly_revenue": f"SELECT date_format(o.order_purchase_timestamp,'yyyy-MM') AS month, ROUND(SUM(p.payment_value),2) AS revenue, COUNT(DISTINCT o.order_id) AS orders {live} GROUP BY 1",
 "category_revenue": f"SELECT {cat} AS category, ROUND(SUM(i.price),2) AS revenue, COUNT(*) AS units_sold FROM {P}.silver_order_items i {jc} GROUP BY 1",
 "top_products": f"SELECT i.product_id, {cat} AS category, COUNT(*) AS units_sold, ROUND(SUM(i.price),2) AS revenue FROM {P}.silver_order_items i {jc} GROUP BY 1,2",
 "seller_revenue": f"SELECT s.seller_id, s.seller_state, ROUND(SUM(i.price),2) AS revenue, COUNT(*) AS items_sold FROM {P}.silver_order_items i JOIN {P}.silver_sellers s USING(seller_id) GROUP BY 1,2",
 "customers_by_state": f"SELECT customer_state AS state, COUNT(DISTINCT customer_id) AS customers FROM {P}.silver_customers GROUP BY 1",
 "order_status": f"SELECT order_status AS status, COUNT(*) AS orders FROM {P}.silver_orders GROUP BY 1",
 "payment_methods": f"SELECT payment_type, COUNT(*) AS payments, ROUND(SUM(payment_value),2) AS value FROM {P}.silver_payments GROUP BY 1",
 "review_scores": f"SELECT review_score, COUNT(*) AS reviews FROM {P}.silver_reviews GROUP BY 1",
}
for name, sql in Q.items():
    spark.sql(f"CREATE OR REPLACE TABLE {P}.gold_{name} AS {sql}")

# COMMAND ----------
spark.sql(f"""CREATE OR REPLACE TABLE {P}.gold_daily_sales PARTITIONED BY (year) AS
  SELECT CAST(o.order_purchase_timestamp AS DATE) AS day, year(o.order_purchase_timestamp) AS year,
         ROUND(SUM(p.payment_value),2) AS revenue, COUNT(DISTINCT o.order_id) AS orders {live} GROUP BY 1,2""")
spark.sql(f"""CREATE OR REPLACE TABLE {P}.gold_kpis AS
  SELECT ROUND(SUM(p.payment_value),2) AS total_revenue, COUNT(DISTINCT o.order_id) AS total_orders,
         ROUND(SUM(p.payment_value)/COUNT(DISTINCT o.order_id),2) AS avg_order_value {live}""")
display(spark.table(f"{P}.gold_kpis"))
