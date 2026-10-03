-- Databricks notebook source
-- COMMAND ----------
-- Monthly revenue and month-over-month growth
SELECT month, revenue, orders,
       ROUND(100*(revenue - LAG(revenue) OVER (ORDER BY month)) / LAG(revenue) OVER (ORDER BY month), 1) AS mom_growth_pct
FROM workspace.retailflow.gold_monthly_revenue WHERE orders >= 10 ORDER BY month;

-- COMMAND ----------
-- Top 10 categories and their share of item revenue
SELECT category, revenue, ROUND(100*revenue/SUM(revenue) OVER (),1) AS share_pct
FROM workspace.retailflow.gold_category_revenue ORDER BY revenue DESC LIMIT 10;

-- COMMAND ----------
-- Delta time travel: inspect table history and compare versions
DESCRIBE HISTORY workspace.retailflow.silver_orders;

-- COMMAND ----------
-- Rejection reasons in Silver orders
SELECT _reject_reason, COUNT(*) AS rows FROM workspace.retailflow.rejected_orders GROUP BY 1;

-- COMMAND ----------
-- Data quality trend across pipeline runs
SELECT run_ts, table_name, input_records, valid_records, rejected_records
FROM workspace.retailflow.quality_metrics ORDER BY run_ts DESC, table_name;
