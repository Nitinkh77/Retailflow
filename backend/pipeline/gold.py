import json

def run_gold(spark, silver, bronze, gold):
    for n in ["orders", "customers", "payments", "order_items", "products", "sellers", "reviews"]:
        spark.read.parquet(str(silver / n)).createOrReplaceTempView(n)
    spark.read.parquet(str(bronze / "category_translation")).createOrReplaceTempView("cat_tr")
    cat = "COALESCE(t.product_category_name_english, pr.product_category_name, 'unknown')"
    join_cat = "LEFT JOIN products pr ON i.product_id=pr.product_id LEFT JOIN cat_tr t ON pr.product_category_name=t.product_category_name"
    q = {
     "monthly_revenue": """SELECT date_format(o.order_purchase_timestamp,'yyyy-MM') AS month,
        ROUND(SUM(p.payment_value),2) AS revenue, COUNT(DISTINCT o.order_id) AS orders
        FROM orders o JOIN payments p USING(order_id) WHERE o.order_status<>'canceled' GROUP BY 1 ORDER BY 1""",
     "daily_sales": """SELECT CAST(o.order_purchase_timestamp AS DATE) AS day,
        ROUND(SUM(p.payment_value),2) AS revenue, COUNT(DISTINCT o.order_id) AS orders
        FROM orders o JOIN payments p USING(order_id) WHERE o.order_status<>'canceled' GROUP BY 1 ORDER BY 1""",
     "top_products": f"""SELECT i.product_id, {cat} AS category, COUNT(*) AS units_sold, ROUND(SUM(i.price),2) AS revenue
        FROM order_items i {join_cat} GROUP BY 1,2 ORDER BY units_sold DESC LIMIT 100""",
     "category_revenue": f"""SELECT {cat} AS category, ROUND(SUM(i.price),2) AS revenue, COUNT(*) AS units_sold
        FROM order_items i {join_cat} GROUP BY 1 ORDER BY revenue DESC""",
     "seller_revenue": """SELECT s.seller_id, s.seller_state, ROUND(SUM(i.price),2) AS revenue, COUNT(*) AS items_sold
        FROM order_items i JOIN sellers s USING(seller_id) GROUP BY 1,2 ORDER BY revenue DESC""",
     "customers_by_state": "SELECT customer_state AS state, COUNT(DISTINCT customer_id) AS customers FROM customers GROUP BY 1 ORDER BY 2 DESC",
     "order_status": "SELECT order_status AS status, COUNT(*) AS orders FROM orders GROUP BY 1 ORDER BY 2 DESC",
     "payment_methods": "SELECT payment_type, COUNT(*) AS payments, ROUND(SUM(payment_value),2) AS value FROM payments GROUP BY 1 ORDER BY value DESC",
     "review_scores": "SELECT review_score, COUNT(*) AS reviews FROM reviews GROUP BY 1 ORDER BY 1",
    }
    counts = {}
    for name, sql in q.items():
        df = spark.sql(sql)
        df.write.mode("overwrite").parquet(str(gold / name))
        counts[name] = df.count()
    k = spark.sql("""SELECT ROUND(SUM(p.payment_value),2) AS rev, COUNT(DISTINCT o.order_id) AS n
        FROM orders o JOIN payments p USING(order_id) WHERE o.order_status<>'canceled'""").first()
    rev, n = k["rev"] or 0, k["n"] or 0
    kpi = {"total_revenue": rev, "total_orders": n, "avg_order_value": round(rev / n, 2) if n else 0,
           "total_customers": spark.table("customers").count(), "total_products": spark.table("products").count()}
    (gold / "kpis.json").write_text(json.dumps(kpi))
    return counts
