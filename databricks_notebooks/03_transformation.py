# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 3: Transformation & Feature Engineering
# MAGIC ### Big Data E-Commerce Analytics Project
# MAGIC **Purpose:** Join all Olist tables into a master DataFrame, engineer features for ML and analytics, and compute RFM customer scores.
# MAGIC
# MAGIC **Dependency:** Run Notebook 2 (Preprocessing) first to have clean views available.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 — Re-Run Preprocessing (if running standalone)

# COMMAND ----------

# Run the preprocessing notebook inline
# %run "./02_preprocessing"
# If running standalone, uncomment the line above. Otherwise continue below.

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 — Build Master Order Table (Olist)
# MAGIC Join: orders + customers + order_items + payments + reviews + products + sellers

# COMMAND ----------

# Step 1: orders + customers
master = spark.table("orders_clean") \
    .join(spark.table("customers_clean"), on="customer_id", how="left")

# Step 2: + order_items (aggregate per order first)
items_agg = spark.table("order_items_clean").groupBy("order_id").agg(
    F.sum("price").alias("items_total_price"),
    F.sum("freight_value").alias("items_total_freight"),
    F.count("order_item_id").alias("items_count"),
    F.avg("price").alias("avg_item_price"),
    F.first("product_id").alias("main_product_id"),
    F.first("seller_id").alias("main_seller_id")
)
master = master.join(items_agg, on="order_id", how="left")

# Step 3: + payments
master = master.join(spark.table("payments_agg"), on="order_id", how="left")

# Step 4: + reviews
reviews_slim = spark.table("reviews_dedup").select(
    "order_id", "review_score", "review_creation_date", "review_answer_timestamp"
)
master = master.join(reviews_slim, on="order_id", how="left")

# Step 5: + products (via main_product_id)
products_slim = spark.table("products_clean").select(
    "product_id",
    "product_category_name_english",
    "product_weight_g",
    F.col("product_length_cm"),
    F.col("product_height_cm"),
    F.col("product_width_cm")
)
master = master.join(products_slim, master.main_product_id == products_slim.product_id, how="left").drop("product_id")

# Step 6: + sellers (via main_seller_id)
sellers_slim = spark.table("sellers_clean").select(
    "seller_id",
    F.col("seller_state").alias("seller_state"),
    F.col("seller_city").alias("seller_city")
)
master = master.join(sellers_slim, master.main_seller_id == sellers_slim.seller_id, how="left").drop("seller_id")

print(f"Master table rows: {master.count():,}   cols: {len(master.columns)}")
master.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 — Feature Engineering

# COMMAND ----------

master_featured = master \
    .withColumn(
        "delivery_delay_days",
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_estimated_delivery_date")
        )
    ) \
    .withColumn(
        "is_late_delivery",
        F.when(F.col("delivery_delay_days") > 0, 1).otherwise(0)
    ) \
    .withColumn(
        "actual_delivery_days",
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_purchase_timestamp")
        )
    ) \
    .withColumn(
        "days_to_approve",
        F.datediff(
            F.col("order_approved_at"),
            F.col("order_purchase_timestamp")
        )
    ) \
    .withColumn(
        "total_order_value",
        F.round(F.col("items_total_price") + F.col("items_total_freight"), 2)
    ) \
    .withColumn(
        "is_high_review",
        F.when(F.col("review_score") >= 4, 1).otherwise(0)
    ) \
    .withColumn(
        "is_delivered",
        F.when(F.col("order_status") == "delivered", 1).otherwise(0)
    ) \
    .withColumn(
        "order_year",  F.year("order_purchase_timestamp")
    ) \
    .withColumn(
        "order_month", F.month("order_purchase_timestamp")
    ) \
    .withColumn(
        "order_day_of_week", F.dayofweek("order_purchase_timestamp")
    ) \
    .withColumn(
        "order_hour", F.hour("order_purchase_timestamp")
    ) \
    .withColumn(
        "product_volume_cm3",
        F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")
    ) \
    .withColumn(
        "review_response_hours",
        (F.col("review_answer_timestamp").cast("long") - F.col("review_creation_date").cast("long")) / 3600
    ) \
    .fillna({
        "delivery_delay_days": 0,
        "is_late_delivery": 0,
        "actual_delivery_days": 0,
        "days_to_approve": 0,
        "review_score": 3.0,
        "is_high_review": 0,
        "product_weight_g": 0.0,
        "product_volume_cm3": 0.0,
        "review_response_hours": 0.0,
        "product_category_name_english": "unknown"
    })

print(f"Featured table rows: {master_featured.count():,}   cols: {len(master_featured.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 — Feature Summary Statistics

# COMMAND ----------

master_featured.select(
    "delivery_delay_days",
    "actual_delivery_days",
    "days_to_approve",
    "total_order_value",
    "review_score",
    "items_count",
    "product_weight_g"
).describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 — RFM Analysis (Customer Segmentation)

# COMMAND ----------

# Reference date = max purchase date + 1 day
max_date = master_featured.agg(F.max("order_purchase_timestamp")).collect()[0][0]
ref_date = max_date

rfm = master_featured \
    .filter(F.col("order_status") == "delivered") \
    .groupBy("customer_unique_id") \
    .agg(
        F.datediff(F.lit(ref_date), F.max("order_purchase_timestamp")).alias("recency_days"),
        F.count("order_id").alias("frequency"),
        F.sum("total_order_value").alias("monetary")
    ) \
    .fillna({"monetary": 0.0})

# Score R, F, M on 1-4 scale using ntile
w_r = Window.orderBy(F.col("recency_days").asc())    # lower recency = better
w_f = Window.orderBy(F.col("frequency").desc())       # higher frequency = better
w_m = Window.orderBy(F.col("monetary").desc())        # higher monetary = better

rfm = rfm \
    .withColumn("R_score", F.ntile(4).over(w_r)) \
    .withColumn("F_score", F.ntile(4).over(w_f)) \
    .withColumn("M_score", F.ntile(4).over(w_m)) \
    .withColumn("RFM_score", F.col("R_score") + F.col("F_score") + F.col("M_score")) \
    .withColumn(
        "customer_segment",
        F.when(F.col("RFM_score") >= 10, "Champions")
         .when(F.col("RFM_score") >= 7,  "Loyal Customers")
         .when(F.col("RFM_score") >= 5,  "At Risk")
         .otherwise("Lost Customers")
    )

print(f"RFM table rows: {rfm.count():,}")
rfm.show(10, truncate=False)

# COMMAND ----------

# Segment distribution
rfm.groupBy("customer_segment").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 — Online Retail II Feature Engineering

# COMMAND ----------

online_retail_featured = spark.table("online_retail_clean") \
    .withColumn("InvoiceYear",      F.year("InvoiceDate")) \
    .withColumn("InvoiceMonth",     F.month("InvoiceDate")) \
    .withColumn("InvoiceDayOfWeek", F.dayofweek("InvoiceDate")) \
    .withColumn("InvoiceHour",      F.hour("InvoiceDate")) \
    .withColumn("TotalLineValue",   F.round(F.col("Quantity") * F.col("Price"), 2))

# Compute median total line value for binary target
median_val = online_retail_featured.approxQuantile("TotalLineValue", [0.5], 0.01)[0]
print(f"Median TotalLineValue: {median_val:.2f}")

online_retail_featured = online_retail_featured \
    .withColumn(
        "is_high_value",
        F.when(F.col("TotalLineValue") > median_val, 1).otherwise(0)
    )

# Customer-level RFM for Online Retail
max_date_or = online_retail_featured.agg(F.max("InvoiceDate")).collect()[0][0]
rfm_retail = online_retail_featured \
    .filter(F.col("Customer ID") != -1) \
    .groupBy("Customer ID") \
    .agg(
        F.datediff(F.lit(max_date_or), F.max("InvoiceDate")).alias("recency_days"),
        F.countDistinct("Invoice").alias("frequency"),
        F.sum("TotalLineValue").alias("monetary")
    )

print(f"Online Retail RFM rows: {rfm_retail.count():,}")
rfm_retail.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 — Register Transformed Views

# COMMAND ----------

master_featured.write.mode("overwrite").saveAsTable("master_featured")
rfm.write.mode("overwrite").saveAsTable("rfm_olist")
online_retail_featured.write.mode("overwrite").saveAsTable("online_retail_featured")
rfm_retail.write.mode("overwrite").saveAsTable("rfm_retail")

print("Transformed tables saved to metastore.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Transformation Complete
# MAGIC Master table and feature-engineered DataFrames ready.
# MAGIC Proceed to **Notebook 4: EDA with Spark SQL**.
