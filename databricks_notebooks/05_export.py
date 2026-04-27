# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 5: Export
# MAGIC ### Big Data E-Commerce Analytics Project
# MAGIC **Purpose:** Export processed and feature-engineered DataFrames to DBFS as Parquet (for archival) and CSV (for Power BI and Google Colab).
# MAGIC
# MAGIC **Dependency:** Run Notebooks 1–3 first.

# COMMAND ----------

from pyspark.sql import functions as F

EXPORT_PATH = "/Volumes/workspace/default/project_data/exports/"

dbutils.fs.mkdirs(EXPORT_PATH)
print(f"Export directory ready: {EXPORT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 — Export Master Featured Table (Olist)
# MAGIC Used by: Power BI, Google Colab ML Notebook 1

# COMMAND ----------

master_df = spark.table("master_featured")

# Parquet (efficient, compressed)
master_df.write.mode("overwrite").parquet(EXPORT_PATH + "master_featured.parquet")
print("Saved master_featured.parquet")

# CSV (for Power BI and Colab upload)
master_df.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(EXPORT_PATH + "master_featured_csv")
print("Saved master_featured_csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 — Export RFM Table (Olist)
# MAGIC Used by: Power BI (customer segment page), Colab ML Notebook 1 (K-Means)

# COMMAND ----------

rfm_df = spark.table("rfm_olist")

rfm_df.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(EXPORT_PATH + "rfm_olist_csv")
print(f"Saved rfm_olist_csv  ({rfm_df.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 — Export Online Retail Featured Table
# MAGIC Used by: Power BI, Google Colab ML Notebook 2

# COMMAND ----------

retail_df = spark.table("online_retail_featured")

retail_df.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(EXPORT_PATH + "online_retail_featured_csv")
print(f"Saved online_retail_featured_csv  ({retail_df.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 — Export RFM for Online Retail
# MAGIC Used by: Colab Notebook 2 (K-Means clustering)

# COMMAND ----------

rfm_retail_df = spark.table("rfm_retail")

rfm_retail_df.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(EXPORT_PATH + "rfm_retail_csv")
print(f"Saved rfm_retail_csv  ({rfm_retail_df.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 — Export Aggregated Summary Tables (for Power BI)

# COMMAND ----------

# Monthly revenue summary
monthly_revenue = spark.sql("""
    SELECT
        order_year, order_month,
        CONCAT(order_year, '-', LPAD(order_month, 2, '0')) AS year_month,
        COUNT(DISTINCT order_id)         AS orders_count,
        ROUND(SUM(total_order_value), 2) AS monthly_revenue,
        ROUND(AVG(total_order_value), 2) AS avg_order_value
    FROM master_featured
    WHERE order_status = 'delivered'
    GROUP BY order_year, order_month
    ORDER BY order_year, order_month
""")
monthly_revenue.coalesce(1).write.mode("overwrite").option("header","true").csv(EXPORT_PATH + "summary_monthly_revenue")
print("Saved summary_monthly_revenue")

# COMMAND ----------

# Category revenue summary
category_revenue = spark.sql("""
    SELECT
        product_category_name_english AS category,
        COUNT(DISTINCT order_id)           AS orders,
        ROUND(SUM(total_order_value), 2)   AS revenue,
        ROUND(AVG(review_score), 2)        AS avg_review_score,
        ROUND(AVG(actual_delivery_days),1) AS avg_delivery_days
    FROM master_featured
    WHERE order_status = 'delivered'
      AND product_category_name_english NOT IN ('unknown','other')
    GROUP BY product_category_name_english
    ORDER BY revenue DESC
""")
category_revenue.coalesce(1).write.mode("overwrite").option("header","true").csv(EXPORT_PATH + "summary_category_revenue")
print("Saved summary_category_revenue")

# COMMAND ----------

# State summary
state_summary = spark.sql("""
    SELECT
        customer_state,
        COUNT(DISTINCT customer_unique_id)   AS customers,
        COUNT(DISTINCT order_id)             AS orders,
        ROUND(SUM(total_order_value), 2)     AS revenue,
        ROUND(AVG(actual_delivery_days), 1)  AS avg_delivery_days,
        ROUND(SUM(is_late_delivery)*100.0/COUNT(*), 2) AS late_pct
    FROM master_featured
    WHERE order_status = 'delivered'
    GROUP BY customer_state
""")
state_summary.coalesce(1).write.mode("overwrite").option("header","true").csv(EXPORT_PATH + "summary_state")
print("Saved summary_state")

# COMMAND ----------

# Payment summary
payment_summary = spark.sql("""
    SELECT
        primary_payment_type,
        COUNT(*) AS orders,
        ROUND(SUM(total_payment_value), 2) AS total_payment,
        ROUND(AVG(total_payment_value), 2) AS avg_payment
    FROM master_featured
    GROUP BY primary_payment_type
    ORDER BY orders DESC
""")
payment_summary.coalesce(1).write.mode("overwrite").option("header","true").csv(EXPORT_PATH + "summary_payments")
print("Saved summary_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.6 — List All Exported Files

# COMMAND ----------

display(dbutils.fs.ls(EXPORT_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.7 — Download Instructions
# MAGIC
# MAGIC To download CSV files to your local machine:
# MAGIC
# MAGIC 1. Go to: `https://<your-databricks-workspace>/files/exports/`
# MAGIC 2. Or use the Databricks CLI:
# MAGIC    ```
# MAGIC    databricks fs cp dbfs:/Volumes/workspace/default/project_data/exports/master_featured_csv/part-00000-*.csv ./master_featured.csv
# MAGIC    ```
# MAGIC 3. Or via the UI: **Data** → **DBFS** → **FileStore** → **exports** → Download
# MAGIC
# MAGIC **Files needed for each tool:**
# MAGIC
# MAGIC | File | Use In |
# MAGIC |------|--------|
# MAGIC | `master_featured.csv` | Power BI (main) + Colab NB1 |
# MAGIC | `rfm_olist.csv` | Power BI (customer page) |
# MAGIC | `online_retail_featured.csv` | Power BI + Colab NB2 |
# MAGIC | `summary_monthly_revenue.csv` | Power BI |
# MAGIC | `summary_category_revenue.csv` | Power BI |
# MAGIC | `summary_state.csv` | Power BI |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Export Complete
# MAGIC All files saved to `/Volumes/workspace/default/project_data/exports/`.
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Download CSVs and upload to **Google Colab** for ML notebooks
# MAGIC - Import CSVs into **Power BI Desktop** for dashboard
