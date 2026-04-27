# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 2: Data Preprocessing
# MAGIC ### Big Data E-Commerce Analytics Project
# MAGIC **Purpose:** Clean all raw DataFrames — handle nulls, fix data types, remove duplicates, filter invalid records, and standardize formats.
# MAGIC
# MAGIC **Dependency:** Run Notebook 1 (Data Ingestion) first, or re-load the raw views here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 — Re-Load Raw Data (if running standalone)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

BASE_PATH = "/Volumes/workspace/default/project_data/"

orders_raw       = spark.read.option("header","true").option("inferSchema","true").csv(BASE_PATH + "olist_orders_dataset.csv")
customers_raw    = spark.read.option("header","true").option("inferSchema","true").csv(BASE_PATH + "olist_customers_dataset.csv")
order_items_raw  = spark.read.option("header","true").option("inferSchema","true").csv(BASE_PATH + "olist_order_items_dataset.csv")
payments_raw     = spark.read.option("header","true").option("inferSchema","true").csv(BASE_PATH + "olist_order_payments_dataset.csv")
reviews_raw      = spark.read.option("header","true").option("inferSchema","true").csv(BASE_PATH + "olist_order_reviews_dataset.csv")
products_raw     = spark.read.option("header","true").option("inferSchema","true").csv(BASE_PATH + "olist_products_dataset.csv")
sellers_raw      = spark.read.option("header","true").option("inferSchema","true").csv(BASE_PATH + "olist_sellers_dataset.csv")
category_raw     = spark.read.option("header","true").option("inferSchema","true").csv(BASE_PATH + "product_category_name_translation.csv")
online_retail_raw = spark.read.option("header","true").option("inferSchema","true").csv(BASE_PATH + "online_retail_II.csv")

print("Raw data reloaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 — Clean Orders Table

# COMMAND ----------

# Cast timestamp columns
TIMESTAMP_COLS_ORDERS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

orders_clean = orders_raw
for col in TIMESTAMP_COLS_ORDERS:
    orders_clean = orders_clean.withColumn(col, F.to_timestamp(F.col(col), "yyyy-MM-dd HH:mm:ss"))

# Drop rows missing critical fields
orders_clean = orders_clean.dropna(subset=["order_id", "customer_id", "order_status", "order_purchase_timestamp"])

# Remove duplicate order_ids
orders_clean = orders_clean.dropDuplicates(["order_id"])

# Keep only known statuses
valid_statuses = ["delivered", "shipped", "canceled", "unavailable", "invoiced", "processing", "approved", "created"]
orders_clean = orders_clean.filter(F.col("order_status").isin(valid_statuses))

print(f"Orders: raw={orders_raw.count():,}  clean={orders_clean.count():,}")
orders_clean.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 — Clean Customers Table

# COMMAND ----------

customers_clean = customers_raw \
    .dropna(subset=["customer_id", "customer_unique_id", "customer_state"]) \
    .dropDuplicates(["customer_id"]) \
    .withColumn("customer_city",  F.lower(F.trim(F.col("customer_city")))) \
    .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))

print(f"Customers: raw={customers_raw.count():,}  clean={customers_clean.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 — Clean Order Items Table

# COMMAND ----------

order_items_clean = order_items_raw \
    .dropna(subset=["order_id", "product_id", "seller_id", "price"]) \
    .filter(F.col("price") > 0) \
    .filter(F.col("freight_value") >= 0) \
    .dropDuplicates(["order_id", "order_item_id"]) \
    .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date", "yyyy-MM-dd HH:mm:ss"))

print(f"Order Items: raw={order_items_raw.count():,}  clean={order_items_clean.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 — Clean Payments Table

# COMMAND ----------

payments_clean = payments_raw \
    .dropna(subset=["order_id", "payment_type", "payment_value"]) \
    .filter(F.col("payment_value") >= 0) \
    .filter(F.col("payment_installments") >= 1)

# Aggregate payments per order (some orders have multiple payment methods)
payments_agg = payments_clean.groupBy("order_id").agg(
    F.sum("payment_value").alias("total_payment_value"),
    F.max("payment_installments").alias("max_payment_installments"),
    F.first("payment_type").alias("primary_payment_type"),
    F.count("payment_sequential").alias("payment_methods_count")
)

print(f"Payments: raw={payments_raw.count():,}  clean={payments_clean.count():,}  aggregated={payments_agg.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 — Clean Reviews Table

# COMMAND ----------

reviews_clean = reviews_raw \
    .dropna(subset=["review_id", "order_id", "review_score"]) \
    .withColumn("review_score", F.expr("try_cast(review_score as int)")) \
    .filter(F.col("review_score").isNotNull()) \
    .filter((F.col("review_score") >= 1) & (F.col("review_score") <= 5)) \
    .dropDuplicates(["review_id"]) \
    .withColumn("review_creation_date",   F.expr("try_to_timestamp(review_creation_date, 'yyyy-MM-dd HH:mm:ss')")) \
    .withColumn("review_answer_timestamp",F.expr("try_to_timestamp(review_answer_timestamp, 'yyyy-MM-dd HH:mm:ss')"))

# Keep one review per order (latest)
w = Window.partitionBy("order_id").orderBy(F.col("review_creation_date").desc())
reviews_dedup = reviews_clean.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

print(f"Reviews: raw={reviews_raw.count():,}  clean={reviews_clean.count():,}  dedup={reviews_dedup.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7 — Clean Products Table

# COMMAND ----------

category_clean = category_raw.dropna(subset=["product_category_name", "product_category_name_english"])

products_clean = products_raw \
    .dropna(subset=["product_id"]) \
    .dropDuplicates(["product_id"]) \
    .fillna({"product_category_name": "unknown",
             "product_weight_g": 0.0,
             "product_length_cm": 0.0,
             "product_height_cm": 0.0,
             "product_width_cm": 0.0}) \
    .join(category_clean, on="product_category_name", how="left") \
    .fillna({"product_category_name_english": "other"})

print(f"Products: raw={products_raw.count():,}  clean={products_clean.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.8 — Clean Sellers Table

# COMMAND ----------

sellers_clean = sellers_raw \
    .dropna(subset=["seller_id", "seller_state"]) \
    .dropDuplicates(["seller_id"]) \
    .withColumn("seller_city",  F.lower(F.trim(F.col("seller_city")))) \
    .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))

print(f"Sellers: raw={sellers_raw.count():,}  clean={sellers_clean.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.9 — Clean Online Retail II

# COMMAND ----------

online_retail_clean = online_retail_raw \
    .dropna(subset=["Invoice", "StockCode", "Quantity", "Price", "InvoiceDate"]) \
    .filter(F.col("Quantity") > 0) \
    .filter(F.col("Price") > 0) \
    .filter(~F.col("Invoice").startswith("C")) \
    .dropDuplicates() \
    .withColumn("InvoiceDate", F.to_timestamp(F.col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("TotalLineValue", F.round(F.col("Quantity") * F.col("Price"), 2)) \
    .withColumn("Country", F.trim(F.col("Country"))) \
    .fillna({"Customer ID": -1, "Description": "UNKNOWN"})

print(f"Online Retail: raw={online_retail_raw.count():,}  clean={online_retail_clean.count():,}")
online_retail_clean.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.10 — Final Null Check After Cleaning

# COMMAND ----------

def null_check_post(df, name):
    total = df.count()
    nulls_per_col = [(c, df.filter(F.col(c).isNull()).count()) for c in df.columns]
    total_nulls = sum(n for _, n in nulls_per_col)
    print(f"[{name}] rows={total:,}  total_nulls={total_nulls:,}")

null_check_post(orders_clean,       "orders_clean")
null_check_post(customers_clean,    "customers_clean")
null_check_post(order_items_clean,  "order_items_clean")
null_check_post(payments_agg,       "payments_agg")
null_check_post(reviews_dedup,      "reviews_dedup")
null_check_post(products_clean,     "products_clean")
null_check_post(sellers_clean,      "sellers_clean")
null_check_post(online_retail_clean,"online_retail_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.11 — Register Clean Views

# COMMAND ----------

orders_clean.createOrReplaceTempView("orders_clean")
customers_clean.createOrReplaceTempView("customers_clean")
order_items_clean.createOrReplaceTempView("order_items_clean")
payments_agg.createOrReplaceTempView("payments_agg")
reviews_dedup.createOrReplaceTempView("reviews_dedup")
products_clean.createOrReplaceTempView("products_clean")
sellers_clean.createOrReplaceTempView("sellers_clean")
online_retail_clean.createOrReplaceTempView("online_retail_clean")

print("All clean views registered.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Preprocessing Complete
# MAGIC All tables cleaned and registered as temp views.
# MAGIC Proceed to **Notebook 3: Transformation & Feature Engineering**.
