# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 1: Data Ingestion
# MAGIC ### Big Data E-Commerce Analytics Project
# MAGIC **Purpose:** Load all raw CSV files from DBFS into Spark DataFrames and register them as temporary views for downstream processing.
# MAGIC
# MAGIC **Datasets:**
# MAGIC - Olist Brazilian E-Commerce (9 CSV files, ~99K orders)
# MAGIC - Online Retail II (1 CSV, ~1.06M transactions)
# MAGIC
# MAGIC ---
# MAGIC ### Before Running This Notebook:
# MAGIC Files have been uploaded to Unity Catalog Volume:
# MAGIC `/Volumes/workspace/default/project_data/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 — Verify Files on DBFS

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/project_data/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 — Initialize Spark Session

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("EcommerceAnalytics_Ingestion") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "8")  # Optimized for Community Edition

print("Spark Version:", spark.version)
print("Session started successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 — Define File Paths

# COMMAND ----------

BASE_PATH = "/Volumes/workspace/default/project_data/"

OLIST_PATHS = {
    "orders":               BASE_PATH + "olist_orders_dataset.csv",
    "customers":            BASE_PATH + "olist_customers_dataset.csv",
    "order_items":          BASE_PATH + "olist_order_items_dataset.csv",
    "payments":             BASE_PATH + "olist_order_payments_dataset.csv",
    "reviews":              BASE_PATH + "olist_order_reviews_dataset.csv",
    "products":             BASE_PATH + "olist_products_dataset.csv",
    "sellers":              BASE_PATH + "olist_sellers_dataset.csv",
    "geolocation":          BASE_PATH + "olist_geolocation_dataset.csv",
    "category_translation": BASE_PATH + "product_category_name_translation.csv",
}

ONLINE_RETAIL_PATH = BASE_PATH + "online_retail_II.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 — Load Olist Tables

# COMMAND ----------

def load_csv(path, name):
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .load(path)
    print(f"[{name}] rows={df.count():,}  cols={len(df.columns)}  schema: {[f.name for f in df.schema.fields]}")
    return df

# COMMAND ----------

orders_raw       = load_csv(OLIST_PATHS["orders"],               "orders")
customers_raw    = load_csv(OLIST_PATHS["customers"],            "customers")
order_items_raw  = load_csv(OLIST_PATHS["order_items"],          "order_items")
payments_raw     = load_csv(OLIST_PATHS["payments"],             "payments")
reviews_raw      = load_csv(OLIST_PATHS["reviews"],              "reviews")
products_raw     = load_csv(OLIST_PATHS["products"],             "products")
sellers_raw      = load_csv(OLIST_PATHS["sellers"],              "sellers")
geolocation_raw  = load_csv(OLIST_PATHS["geolocation"],          "geolocation")
category_raw     = load_csv(OLIST_PATHS["category_translation"], "category_translation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 — Load Online Retail II

# COMMAND ----------

online_retail_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .load(ONLINE_RETAIL_PATH)

print(f"[online_retail] rows={online_retail_raw.count():,}  cols={len(online_retail_raw.columns)}")
online_retail_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 — Print Schemas

# COMMAND ----------

print("=" * 60)
print("ORDERS SCHEMA")
print("=" * 60)
orders_raw.printSchema()
orders_raw.show(3, truncate=False)

# COMMAND ----------

print("=" * 60)
print("CUSTOMERS SCHEMA")
print("=" * 60)
customers_raw.printSchema()
customers_raw.show(3, truncate=False)

# COMMAND ----------

print("=" * 60)
print("ORDER ITEMS SCHEMA")
print("=" * 60)
order_items_raw.printSchema()
order_items_raw.show(3, truncate=False)

# COMMAND ----------

print("=" * 60)
print("PAYMENTS SCHEMA")
print("=" * 60)
payments_raw.printSchema()
payments_raw.show(3, truncate=False)

# COMMAND ----------

print("=" * 60)
print("REVIEWS SCHEMA")
print("=" * 60)
reviews_raw.printSchema()
reviews_raw.show(3, truncate=False)

# COMMAND ----------

print("=" * 60)
print("PRODUCTS SCHEMA")
print("=" * 60)
products_raw.printSchema()
products_raw.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 — Register Temporary Views (for Spark SQL)

# COMMAND ----------

orders_raw.createOrReplaceTempView("orders_raw")
customers_raw.createOrReplaceTempView("customers_raw")
order_items_raw.createOrReplaceTempView("order_items_raw")
payments_raw.createOrReplaceTempView("payments_raw")
reviews_raw.createOrReplaceTempView("reviews_raw")
products_raw.createOrReplaceTempView("products_raw")
sellers_raw.createOrReplaceTempView("sellers_raw")
geolocation_raw.createOrReplaceTempView("geolocation_raw")
category_raw.createOrReplaceTempView("category_raw")
online_retail_raw.createOrReplaceTempView("online_retail_raw")

print("All views registered successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8 — Quick Data Summary

# COMMAND ----------

summary = {
    "orders":        orders_raw.count(),
    "customers":     customers_raw.count(),
    "order_items":   order_items_raw.count(),
    "payments":      payments_raw.count(),
    "reviews":       reviews_raw.count(),
    "products":      products_raw.count(),
    "sellers":       sellers_raw.count(),
    "online_retail": online_retail_raw.count(),
}

print("\n{'=' * 55}")
print(f"{'TABLE':<25} {'ROW COUNT':>15}")
print("=" * 40)
for table, count in summary.items():
    print(f"{table:<25} {count:>15,}")
print("=" * 40)
print(f"{'TOTAL RECORDS':<25} {sum(summary.values()):>15,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.9 — Null Value Check (All Tables)

# COMMAND ----------

def null_report(df, name):
    total = df.count()
    print(f"\n--- NULL REPORT: {name} (total rows: {total:,}) ---")
    for col_name in df.columns:
        nulls = df.filter(F.col(col_name).isNull()).count()
        pct = (nulls / total) * 100
        if nulls > 0:
            print(f"  {col_name:<45} nulls={nulls:>6,}  ({pct:.1f}%)")

null_report(orders_raw,      "orders")
null_report(customers_raw,   "customers")
null_report(order_items_raw, "order_items")
null_report(payments_raw,    "payments")
null_report(reviews_raw,     "reviews")
null_report(products_raw,    "products")
null_report(sellers_raw,     "sellers")
null_report(online_retail_raw, "online_retail")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ingestion Complete
# MAGIC All raw DataFrames loaded and registered as temp views.
# MAGIC Proceed to **Notebook 2: Preprocessing**.
