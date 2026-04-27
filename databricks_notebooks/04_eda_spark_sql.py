# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 4: Exploratory Data Analysis (EDA) with Spark SQL
# MAGIC ### Big Data E-Commerce Analytics Project
# MAGIC **Purpose:** Extract business insights from the transformed master table using Spark SQL queries and Databricks built-in visualizations.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Sales & Revenue Analysis
# MAGIC 2. Customer Analysis
# MAGIC 3. Delivery Performance
# MAGIC 4. Product & Category Analysis
# MAGIC 5. Payment Analysis
# MAGIC 6. Review & Satisfaction Analysis
# MAGIC 7. Online Retail Analysis
# MAGIC
# MAGIC **Dependency:** Run Notebooks 1–3 first, or uncomment the %run lines below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# %run "./03_transformation"   # Uncomment if running standalone

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 — Sales & Revenue Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1: Total Revenue, Orders, and Average Order Value

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT order_id)                          AS total_orders,
# MAGIC     COUNT(DISTINCT customer_unique_id)                AS unique_customers,
# MAGIC     ROUND(SUM(total_order_value), 2)                  AS total_revenue,
# MAGIC     ROUND(AVG(total_order_value), 2)                  AS avg_order_value,
# MAGIC     ROUND(MAX(total_order_value), 2)                  AS max_order_value,
# MAGIC     ROUND(MIN(total_order_value), 2)                  AS min_order_value
# MAGIC FROM master_featured
# MAGIC WHERE order_status = 'delivered'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2: Monthly Revenue Trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_year,
# MAGIC     order_month,
# MAGIC     CONCAT(order_year, '-', LPAD(order_month, 2, '0'))  AS year_month,
# MAGIC     COUNT(DISTINCT order_id)                             AS orders_count,
# MAGIC     ROUND(SUM(total_order_value), 2)                     AS monthly_revenue
# MAGIC FROM master_featured
# MAGIC WHERE order_status = 'delivered'
# MAGIC   AND order_year IN (2017, 2018)
# MAGIC GROUP BY order_year, order_month
# MAGIC ORDER BY order_year, order_month

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3: Revenue by Customer State (Top 10)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_state,
# MAGIC     COUNT(DISTINCT order_id)           AS orders,
# MAGIC     COUNT(DISTINCT customer_unique_id) AS customers,
# MAGIC     ROUND(SUM(total_order_value), 2)   AS revenue,
# MAGIC     ROUND(AVG(total_order_value), 2)   AS avg_order_value
# MAGIC FROM master_featured
# MAGIC WHERE order_status = 'delivered'
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4: Orders by Day of Week

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE order_day_of_week
# MAGIC         WHEN 1 THEN 'Sunday'
# MAGIC         WHEN 2 THEN 'Monday'
# MAGIC         WHEN 3 THEN 'Tuesday'
# MAGIC         WHEN 4 THEN 'Wednesday'
# MAGIC         WHEN 5 THEN 'Thursday'
# MAGIC         WHEN 6 THEN 'Friday'
# MAGIC         WHEN 7 THEN 'Saturday'
# MAGIC     END AS day_name,
# MAGIC     order_day_of_week,
# MAGIC     COUNT(*) AS orders_count,
# MAGIC     ROUND(AVG(total_order_value), 2) AS avg_order_value
# MAGIC FROM master_featured
# MAGIC GROUP BY order_day_of_week
# MAGIC ORDER BY order_day_of_week

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 — Customer Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5: Customer Segment Distribution (RFM)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_segment,
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
# MAGIC     ROUND(AVG(monetary), 2)     AS avg_monetary,
# MAGIC     ROUND(AVG(frequency), 2)    AS avg_frequency,
# MAGIC     ROUND(AVG(recency_days), 1) AS avg_recency_days
# MAGIC FROM rfm_olist
# MAGIC GROUP BY customer_segment
# MAGIC ORDER BY customer_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q6: Repeat vs One-Time Customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE WHEN frequency > 1 THEN 'Repeat Customer' ELSE 'One-Time Customer' END AS customer_type,
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     ROUND(AVG(monetary), 2) AS avg_spend
# MAGIC FROM rfm_olist
# MAGIC GROUP BY customer_type
# MAGIC ORDER BY customer_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 — Delivery Performance Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q7: On-Time vs Late Delivery Rate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE WHEN is_late_delivery = 1 THEN 'Late' ELSE 'On Time' END AS delivery_status,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
# MAGIC FROM master_featured
# MAGIC WHERE order_status = 'delivered'
# MAGIC   AND delivery_delay_days IS NOT NULL
# MAGIC GROUP BY is_late_delivery

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q8: Average Delivery Time by State (Top 10 Slowest)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_state,
# MAGIC     ROUND(AVG(actual_delivery_days), 1) AS avg_delivery_days,
# MAGIC     ROUND(AVG(delivery_delay_days), 1)  AS avg_delay_days,
# MAGIC     COUNT(*)                            AS delivered_orders,
# MAGIC     SUM(is_late_delivery)               AS late_orders
# MAGIC FROM master_featured
# MAGIC WHERE order_status = 'delivered'
# MAGIC   AND actual_delivery_days > 0
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY avg_delivery_days DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q9: Delivery Performance by Year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_year,
# MAGIC     COUNT(*) AS total_delivered,
# MAGIC     SUM(is_late_delivery) AS late_deliveries,
# MAGIC     ROUND(SUM(is_late_delivery) * 100.0 / COUNT(*), 2) AS late_pct,
# MAGIC     ROUND(AVG(actual_delivery_days), 1) AS avg_delivery_days
# MAGIC FROM master_featured
# MAGIC WHERE order_status = 'delivered'
# MAGIC   AND actual_delivery_days > 0
# MAGIC GROUP BY order_year
# MAGIC ORDER BY order_year

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 — Product & Category Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q10: Top 10 Product Categories by Revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     product_category_name_english AS category,
# MAGIC     COUNT(DISTINCT order_id)           AS orders,
# MAGIC     ROUND(SUM(total_order_value), 2)   AS total_revenue,
# MAGIC     ROUND(AVG(total_order_value), 2)   AS avg_order_value,
# MAGIC     ROUND(AVG(review_score), 2)        AS avg_review_score
# MAGIC FROM master_featured
# MAGIC WHERE order_status = 'delivered'
# MAGIC   AND product_category_name_english != 'unknown'
# MAGIC GROUP BY product_category_name_english
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q11: Review Score Distribution by Category (Top 10 by volume)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     product_category_name_english AS category,
# MAGIC     COUNT(*) AS reviews_count,
# MAGIC     ROUND(AVG(review_score), 2) AS avg_score,
# MAGIC     SUM(CASE WHEN review_score = 5 THEN 1 ELSE 0 END) AS score_5,
# MAGIC     SUM(CASE WHEN review_score = 4 THEN 1 ELSE 0 END) AS score_4,
# MAGIC     SUM(CASE WHEN review_score = 3 THEN 1 ELSE 0 END) AS score_3,
# MAGIC     SUM(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END) AS score_1_2
# MAGIC FROM master_featured
# MAGIC WHERE product_category_name_english NOT IN ('unknown', 'other')
# MAGIC GROUP BY product_category_name_english
# MAGIC ORDER BY reviews_count DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 — Payment Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q12: Payment Type Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     primary_payment_type,
# MAGIC     COUNT(*) AS orders_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
# MAGIC     ROUND(AVG(total_payment_value), 2) AS avg_payment,
# MAGIC     ROUND(SUM(total_payment_value), 2) AS total_payment
# MAGIC FROM master_featured
# MAGIC GROUP BY primary_payment_type
# MAGIC ORDER BY orders_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q13: Installments vs Revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     max_payment_installments AS installments,
# MAGIC     COUNT(*) AS orders_count,
# MAGIC     ROUND(AVG(total_payment_value), 2) AS avg_payment,
# MAGIC     ROUND(SUM(total_payment_value), 2) AS total_payment
# MAGIC FROM master_featured
# MAGIC WHERE max_payment_installments <= 12
# MAGIC GROUP BY max_payment_installments
# MAGIC ORDER BY installments

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 — Review & Satisfaction Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q14: Overall Review Score Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     review_score,
# MAGIC     COUNT(*) AS count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
# MAGIC FROM master_featured
# MAGIC WHERE review_score IS NOT NULL
# MAGIC GROUP BY review_score
# MAGIC ORDER BY review_score DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q15: Review Score vs Delivery Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     is_late_delivery,
# MAGIC     ROUND(AVG(review_score), 3) AS avg_review_score,
# MAGIC     COUNT(*) AS orders_count
# MAGIC FROM master_featured
# MAGIC WHERE review_score IS NOT NULL
# MAGIC   AND order_status = 'delivered'
# MAGIC GROUP BY is_late_delivery

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.7 — Online Retail II Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q16: Top 10 Countries by Revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     Country,
# MAGIC     COUNT(DISTINCT Invoice)  AS invoices,
# MAGIC     COUNT(*)                 AS line_items,
# MAGIC     ROUND(SUM(TotalLineValue), 2) AS total_revenue,
# MAGIC     ROUND(AVG(TotalLineValue), 2) AS avg_line_value
# MAGIC FROM online_retail_featured
# MAGIC GROUP BY Country
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q17: Monthly Revenue Trend (Online Retail)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     InvoiceYear  AS year,
# MAGIC     InvoiceMonth AS month,
# MAGIC     CONCAT(InvoiceYear, '-', LPAD(InvoiceMonth, 2, '0')) AS year_month,
# MAGIC     COUNT(DISTINCT Invoice)          AS invoices,
# MAGIC     ROUND(SUM(TotalLineValue), 2)    AS monthly_revenue
# MAGIC FROM online_retail_featured
# MAGIC GROUP BY InvoiceYear, InvoiceMonth
# MAGIC ORDER BY InvoiceYear, InvoiceMonth

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q18: Top 10 Best-Selling Products

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     StockCode,
# MAGIC     Description,
# MAGIC     SUM(Quantity)                AS units_sold,
# MAGIC     ROUND(SUM(TotalLineValue), 2) AS total_revenue,
# MAGIC     ROUND(AVG(Price), 2)          AS avg_price
# MAGIC FROM online_retail_featured
# MAGIC GROUP BY StockCode, Description
# MAGIC ORDER BY units_sold DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q19: Sales by Hour of Day (Online Retail)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     InvoiceHour AS hour_of_day,
# MAGIC     COUNT(DISTINCT Invoice)           AS invoices,
# MAGIC     ROUND(SUM(TotalLineValue), 2)     AS revenue
# MAGIC FROM online_retail_featured
# MAGIC GROUP BY InvoiceHour
# MAGIC ORDER BY InvoiceHour

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q20: High Value vs Low Value Purchase Split

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE WHEN is_high_value = 1 THEN 'High Value' ELSE 'Low Value' END AS purchase_type,
# MAGIC     COUNT(*) AS transaction_count,
# MAGIC     ROUND(SUM(TotalLineValue), 2) AS total_revenue,
# MAGIC     ROUND(AVG(TotalLineValue), 2) AS avg_value
# MAGIC FROM online_retail_featured
# MAGIC GROUP BY is_high_value

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ EDA Complete
# MAGIC All 20 business queries executed. Use Databricks chart options (bar, line, pie) on each result cell.
# MAGIC Proceed to **Notebook 5: Export**.
