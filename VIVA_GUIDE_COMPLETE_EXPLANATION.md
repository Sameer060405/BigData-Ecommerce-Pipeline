# 🎓 BIG DATA E-COMMERCE ANALYTICS PROJECT - COMPLETE VIVA GUIDE

**Team Project**: 4 people | **Your work**: All 5 Databricks Notebooks  
**Datasets**: Olist Brazilian E-Commerce + Online Retail II  
**Tools**: Databricks, Apache Spark, Spark SQL, Python, Power BI, Google Colab

---

## 📋 TABLE OF CONTENTS
1. [Project Overview](#project-overview)
2. [Notebook 1: Data Ingestion](#notebook-1-data-ingestion)
3. [Notebook 2: Data Preprocessing](#notebook-2-data-preprocessing)
4. [Notebook 3: Transformation & Feature Engineering](#notebook-3-transformation--feature-engineering)
5. [Notebook 4: EDA with Spark SQL](#notebook-4-eda-with-spark-sql)
6. [Notebook 5: Export](#notebook-5-export)
7. [Key Concepts to Explain](#key-concepts)
8. [Viva Questions & Answers](#viva-qa)

---

## PROJECT OVERVIEW

### **What is the Project About?**
This is a **Big Data Analytics Pipeline** that:
- **Ingests** raw e-commerce data from two different sources
- **Cleans & Validates** data to ensure quality
- **Transforms** data by engineering features and joining tables
- **Analyzes** business patterns using SQL queries
- **Exports** insights to Power BI dashboards and ML notebooks

### **Why Apache Spark & Databricks?**
- **Spark**: Distributed computing framework → handles **millions of records efficiently**
- **Databricks**: Cloud platform with Spark integrated → easy collaboration, built-in notebooks
- **Why distributed?**: Traditional SQL databases can't process 1M+ records quickly; Spark parallelizes work across multiple servers

### **Datasets**
| Dataset | Records | Use Case |
|---------|---------|----------|
| Olist (Brazilian E-Commerce) | ~99K orders | Customer behavior, delivery analysis, RFM segmentation |
| Online Retail II | ~1.06M transactions | Product sales trends, customer segmentation |

### **Project Flow**
```
Raw CSV Files (DBFS)
        ↓
[NB1] Ingestion → Load to DataFrames
        ↓
[NB2] Preprocessing → Clean & validate
        ↓
[NB3] Transformation → Engineer features & join tables
        ↓
[NB4] EDA → Generate 20 business insights
        ↓
[NB5] Export → CSV/Parquet for Power BI & ML
```

---

# NOTEBOOK 1: DATA INGESTION

**Purpose**: Load all raw CSV files from DBFS into Spark DataFrames and register them as temporary views (SQL tables).

## Line-by-Line Explanation

### **Section 1.1: Verify Files on DBFS**
```python
display(dbutils.fs.ls("/Volumes/workspace/default/project_data/"))
```
- `dbutils.fs.ls()`: Databricks utility to **list files** in a directory
- `display()`: Render results in a nice table format in the notebook
- **What it does**: Shows all CSV files available in the cloud storage
- **Why**: Confirm all files are uploaded before loading them

### **Section 1.2: Initialize Spark Session**
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("EcommerceAnalytics_Ingestion") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "8")  # Optimized for Community Edition
```
- `SparkSession`: Main entry point for Spark functionality
- `.builder`: Builder pattern to configure Spark
- `.appName()`: Name for this Spark application (for debugging/monitoring)
- `.config("spark.sql.legacy.timeParserPolicy", "LEGACY")`: Use older date parsing format (compatibility)
- `.getOrCreate()`: Create a new session or reuse existing one
- `spark.conf.set()`: Set shuffle partitions = 8 (optimization for Databricks Community, which has limited resources)
- **Why**: Spark needs to be initialized before any operations; config optimizes for the environment

### **Section 1.3: Define File Paths**
```python
BASE_PATH = "/Volumes/workspace/default/project_data/"

OLIST_PATHS = {
    "orders":               BASE_PATH + "olist_orders_dataset.csv",
    "customers":            BASE_PATH + "olist_customers_dataset.csv",
    # ... etc
}

ONLINE_RETAIL_PATH = BASE_PATH + "online_retail_II.csv"
```
- Creates dictionary of file paths for all datasets
- Using variables instead of hardcoding prevents errors and makes code reusable
- `OLIST_PATHS` contains 9 files (orders, customers, order_items, payments, reviews, products, sellers, geolocation, category_translation)
- Separate path for Online Retail II
- **Why**: Single source of truth for all file locations; easy to update if paths change

### **Section 1.4: Load Olist Tables**
```python
def load_csv(path, name):
    df = spark.read.format("csv") \
        .option("header", "true") \      # First row contains column names
        .option("inferSchema", "true") \ # Auto-detect data types
        .option("multiLine", "true") \   # Allow values spanning multiple lines
        .option("escape", '"') \         # Handle escaped quotes in CSV
        .load(path)
    print(f"[{name}] rows={df.count():,}  cols={len(df.columns)}")
    return df
```
- **Function** to load CSV files with consistent settings
- `spark.read.format("csv")`: Use CSV format reader
- `option("header", "true")`: Treat first row as column names
- `option("inferSchema", "true")`: Automatically detect data types (int, string, timestamp, etc.)
- `option("multiLine", "true")`: Handle multi-line values in CSV (some text fields span multiple lines)
- `option("escape", '"')`: Handle escaped quotes properly
- `.load(path)`: Actually read the file
- `.count()`: Count total rows; also triggers lazy evaluation (Spark now reads the file)
- **Why**: Function ensures consistent loading; settings handle real-world messy CSV data

### **Section 1.5: Load All 9 Olist Files + Online Retail II**
```python
orders_raw       = load_csv(OLIST_PATHS["orders"],               "orders")
customers_raw    = load_csv(OLIST_PATHS["customers"],            "customers")
# ... repeat for all files
online_retail_raw = spark.read.format("csv")...
```
- Load all datasets into memory as DataFrames
- Each DataFrame named with `_raw` suffix to indicate it's unprocessed
- Print row count and schema for each (good for verification)
- **Why**: Have all data in memory for fast processing; Spark will distribute across servers if needed

### **Section 1.6: Print Schemas**
```python
orders_raw.printSchema()
orders_raw.show(3, truncate=False)
```
- `.printSchema()`: Display column names and data types
  ```
  |-- order_id: string (nullable = true)
  |-- customer_id: string (nullable = true)
  |-- order_status: string (nullable = true)
  |-- order_purchase_timestamp: string (nullable = true)
  |-- ...
  ```
- `.show(3, truncate=False)`: Display first 3 rows without truncating long values
- **Why**: Verify data loaded correctly; understand what columns exist and their types

### **Section 1.7: Register Temporary Views**
```python
orders_raw.createOrReplaceTempView("orders_raw")
customers_raw.createOrReplaceTempView("customers_raw")
# ... repeat for all
```
- `createOrReplaceTempView()`: Create a SQL-accessible table from DataFrame
- After this, you can use SQL: `SELECT * FROM orders_raw`
- **Temporary**: Views exist only during this Spark session (not saved to disk)
- **Why**: Next notebooks use Spark SQL; views make data queryable via SQL syntax

### **Section 1.8: Quick Data Summary**
```python
summary = {
    "orders":        orders_raw.count(),
    "customers":     customers_raw.count(),
    # ...
}
print(f"{'TABLE':<25} {'ROW COUNT':>15}")
for table, count in summary.items():
    print(f"{table:<25} {count:>15,}")
print(f"TOTAL RECORDS: {sum(summary.values()):,}")
```
- Create summary statistics of all tables
- Format output in a nice table (left-aligned table name, right-aligned count)
- `:,` formatting adds commas to large numbers (e.g., "99,441" instead of "99441")
- **Why**: Quick sanity check; do row counts make sense? Are tables empty?

### **Section 1.9: Null Value Check**
```python
def null_report(df, name):
    total = df.count()
    print(f"\n--- NULL REPORT: {name} (total rows: {total:,}) ---")
    for col_name in df.columns:
        nulls = df.filter(F.col(col_name).isNull()).count()
        pct = (nulls / total) * 100
        if nulls > 0:
            print(f"  {col_name:<45} nulls={nulls:>6,}  ({pct:.1f}%)")
```
- For each column: count how many NULL/missing values
- Only print columns with nulls (noise reduction)
- Show percentage to understand severity
- `F.col()`: Access column by name; `isNull()`: filter for NULL values
- **Why**: Identify data quality issues BEFORE cleaning; decide which nulls are tolerable

---

# NOTEBOOK 2: DATA PREPROCESSING

**Purpose**: Clean all raw DataFrames—handle nulls, fix data types, remove duplicates, filter invalid records, and standardize formats.

## Line-by-Line Explanation

### **Section 2.1: Reload Raw Data (if standalone)**
```python
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

BASE_PATH = "/Volumes/workspace/default/project_data/"
orders_raw = spark.read.option("header","true").option("inferSchema","true").csv(BASE_PATH + "olist_orders_dataset.csv")
# ... repeat for all files
```
- **Why reload?** If running notebook in isolation, previous views aren't available
- Import necessary libraries: `functions` (F) for data manipulation, `Window` for row operations
- Legacy time parser: handle dates in old format
- **Optimization**: If not standalone (notebooks run in sequence), could comment this out

### **Section 2.2: Clean Orders Table**

#### **Cast Timestamp Columns**
```python
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
```
- Loaded as strings; must convert to timestamp type for date operations
- `F.to_timestamp()`: Convert string to timestamp with format "yyyy-MM-dd HH:mm:ss"
- Example: "2017-10-02 10:56:33" → Timestamp('2017-10-02 10:56:33')
- Loop through each timestamp column and convert
- **Why**: Timestamps enable date comparisons (e.g., delivery_date - estimated_date)

#### **Drop Critical Nulls**
```python
orders_clean = orders_clean.dropna(subset=["order_id", "customer_id", "order_status", "order_purchase_timestamp"])
```
- `dropna()`: Remove rows with NULL values
- `subset`: Only check specific columns (if ANY of these 4 are NULL, drop the row)
- **Why**: Can't process order without ID, customer, status, or date; these are mandatory

#### **Remove Duplicates**
```python
orders_clean = orders_clean.dropDuplicates(["order_id"])
```
- Keep only first occurrence of each unique order_id
- **Why**: order_id should be unique; duplicates are errors

#### **Validate Order Status**
```python
valid_statuses = ["delivered", "shipped", "canceled", "unavailable", "invoiced", "processing", "approved", "created"]
orders_clean = orders_clean.filter(F.col("order_status").isin(valid_statuses))
```
- `isin()`: Keep rows where status is in the list
- Drop rows with unexpected status values
- **Why**: Ensure data integrity; invalid status = data entry error

#### **Print Summary**
```python
print(f"Orders: raw={orders_raw.count():,}  clean={orders_clean.count():,}")
```
- Show how many rows were dropped during cleaning
- Example: raw=99441, clean=98000 → 1,441 rows dropped due to nulls/duplicates/invalid status

### **Section 2.3: Clean Customers Table**
```python
customers_clean = customers_raw \
    .dropna(subset=["customer_id", "customer_unique_id", "customer_state"]) \
    .dropDuplicates(["customer_id"]) \
    .withColumn("customer_city",  F.lower(F.trim(F.col("customer_city")))) \
    .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
```
- **Chain operations**: Each line builds on previous (functional programming style)
- `.dropna()`: Remove rows missing customer_id, unique_id, or state
- `.dropDuplicates()`: Keep only 1 per customer_id
- `.withColumn()`: Modify or create columns
  - `F.lower()`: Convert to lowercase (standardize "Sao Paulo" → "sao paulo")
  - `F.trim()`: Remove leading/trailing whitespace
  - `F.upper()`: Convert to uppercase (state codes: "sp" → "SP")
- **Why**: Clean data has consistent format; easier to join tables and query

### **Section 2.4: Clean Order Items Table**
```python
order_items_clean = order_items_raw \
    .dropna(subset=["order_id", "product_id", "seller_id", "price"]) \
    .filter(F.col("price") > 0) \
    .filter(F.col("freight_value") >= 0) \
    .dropDuplicates(["order_id", "order_item_id"]) \
    .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date", "yyyy-MM-dd HH:mm:ss"))
```
- Drop rows missing critical fields
- `.filter(F.col("price") > 0)`: Remove zero or negative prices (invalid)
- `.filter(F.col("freight_value") >= 0)`: Freight can be zero (free shipping) but not negative
- `.dropDuplicates(["order_id", "order_item_id"])`: Combination should be unique
- Convert date to timestamp
- **Why**: Order items must have valid prices; duplicates cause revenue overcounting

### **Section 2.5: Clean Payments Table**
```python
payments_clean = payments_raw \
    .dropna(subset=["order_id", "payment_type", "payment_value"]) \
    .filter(F.col("payment_value") >= 0) \
    .filter(F.col("payment_installments") >= 1)

# Aggregate payments per order
payments_agg = payments_clean.groupBy("order_id").agg(
    F.sum("payment_value").alias("total_payment_value"),
    F.max("payment_installments").alias("max_payment_installments"),
    F.first("payment_type").alias("primary_payment_type"),
    F.count("payment_sequential").alias("payment_methods_count")
)
```
- Clean: drop nulls, negative values, invalid installments
- **Aggregation**: Some orders have multiple payment methods (e.g., credit card + voucher)
- `groupBy("order_id")`: Group all payments for each order
- `F.sum("payment_value")`: Total payment amount per order
- `F.max("payment_installments")`: Maximum installments used in any payment
- `F.first("payment_type")`: Get primary payment type
- `F.count()`: Count how many payment methods per order
- **Why**: Each order should have ONE row in final dataset; aggregation combines multiple payments

### **Section 2.6: Clean Reviews Table**
```python
reviews_clean = reviews_raw \
    .dropna(subset=["review_id", "order_id", "review_score"]) \
    .withColumn("review_score", F.expr("try_cast(review_score as int)")) \
    .filter(F.col("review_score").isNotNull()) \
    .filter((F.col("review_score") >= 1) & (F.col("review_score") <= 5)) \
    .dropDuplicates(["review_id"])

# Keep latest review per order
w = Window.partitionBy("order_id").orderBy(F.col("review_creation_date").desc())
reviews_dedup = reviews_clean.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
```
- Drop nulls; cast review_score to integer (some might be strings)
- Validate score is 1-5 (standard rating)
- Remove duplicate reviews
- **Window Function**:
  - `Window.partitionBy("order_id")`: Split data into groups by order_id
  - `.orderBy(...desc())`: Within each group, sort by review date (newest first)
  - `F.row_number()`: Assign row number 1, 2, 3... within each group
  - `rn == 1`: Keep only the first (most recent) review per order
  - `.drop("rn")`: Remove the temporary row number column
- **Why**: One order may have multiple reviews; keep only the latest

### **Section 2.7: Clean Products Table**
```python
category_clean = category_raw.dropna(subset=["product_category_name", "product_category_name_english"])

products_clean = products_raw \
    .dropna(subset=["product_id"]) \
    .dropDuplicates(["product_id"]) \
    .fillna({"product_category_name": "unknown",
             "product_weight_g": 0.0,
             ...}) \
    .join(category_clean, on="product_category_name", how="left") \
    .fillna({"product_category_name_english": "other"})
```
- Clean category table: drop rows with missing names
- Clean products: drop nulls in product_id, remove duplicates
- **Fill NA**: Replace missing values with defaults
  - Missing category → "unknown"
  - Missing weight → 0.0 (unknown weight)
- **Join**: Match Portuguese category names to English translations
  - `on="product_category_name"`: Join on category name
  - `how="left"`: Keep all products even if no English translation found
- Fill any remaining NAs with "other"
- **Why**: Complete product info enables better analysis; join adds English names for clarity

### **Section 2.8: Clean Sellers Table**
```python
sellers_clean = sellers_raw \
    .dropna(subset=["seller_id", "seller_state"]) \
    .dropDuplicates(["seller_id"]) \
    .withColumn("seller_city",  F.lower(F.trim(F.col("seller_city")))) \
    .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
```
- Similar to customers: drop nulls, remove duplicates, standardize case
- **Why**: Consistent city/state info for geographic analysis

### **Section 2.9: Clean Online Retail II**
```python
online_retail_clean = online_retail_raw \
    .dropna(subset=["Invoice", "StockCode", "Quantity", "Price", "InvoiceDate"]) \
    .filter(F.col("Quantity") > 0) \
    .filter(F.col("Price") > 0) \
    .filter(~F.col("Invoice").startswith("C")) \
    .dropDuplicates() \
    .withColumn("InvoiceDate", F.to_timestamp(...)) \
    .withColumn("TotalLineValue", F.round(F.col("Quantity") * F.col("Price"), 2)) \
    .fillna({"Customer ID": -1, "Description": "UNKNOWN"})
```
- Drop critical nulls
- `Quantity > 0`: Remove zero or negative quantities
- `Price > 0`: Remove zero or negative prices
- `~F.col("Invoice").startswith("C")`: Remove canceled invoices (those start with 'C')
  - `~` = NOT operator
- Remove full duplicates
- Convert date to timestamp
- Create `TotalLineValue`: Quantity × Price (revenue per line item), rounded to 2 decimals
- Fill missing customer ID with -1 (means "unknown"); description with "UNKNOWN"
- **Why**: UK retail data has invoices with cancelled flag; remove them for accuracy

### **Section 2.10: Null Check After Cleaning**
```python
def null_check_post(df, name):
    total = df.count()
    nulls_per_col = [(c, df.filter(F.col(c).isNull()).count()) for c in df.columns]
    total_nulls = sum(n for _, n in nulls_per_col)
    print(f"[{name}] rows={total:,}  total_nulls={total_nulls:,}")
```
- Check remaining nulls after cleaning
- Report total nulls per table
- **Why**: Verify cleaning worked; should have very few nulls now

### **Section 2.11: Register Clean Views**
```python
orders_clean.createOrReplaceTempView("orders_clean")
customers_clean.createOrReplaceTempView("customers_clean")
# ... etc
```
- Register cleaned tables as SQL views for next notebook
- **Why**: Transformation notebook will join these clean views

---

# NOTEBOOK 3: TRANSFORMATION & FEATURE ENGINEERING

**Purpose**: Join all Olist tables into master DataFrame, engineer features for ML/analytics, compute RFM scores.

## Line-by-Line Explanation

### **Section 3.2: Build Master Order Table**

#### **Step 1: Orders + Customers**
```python
master = spark.table("orders_clean") \
    .join(spark.table("customers_clean"), on="customer_id", how="left")
```
- Start with cleaned orders table
- `.join()`: Combine with customers table
- `on="customer_id"`: Match based on customer_id
- `how="left"`: Keep ALL orders even if customer not found (shouldn't happen, but safe)
- Result: order_id, customer_id, order_status, ... + customer_city, customer_state, ...

#### **Step 2: + Order Items (Aggregated)**
```python
items_agg = spark.table("order_items_clean").groupBy("order_id").agg(
    F.sum("price").alias("items_total_price"),
    F.sum("freight_value").alias("items_total_freight"),
    F.count("order_item_id").alias("items_count"),
    F.avg("price").alias("avg_item_price"),
    F.first("product_id").alias("main_product_id"),
    F.first("seller_id").alias("main_seller_id")
)
master = master.join(items_agg, on="order_id", how="left")
```
- Some orders have multiple items; need to aggregate
- `groupBy("order_id")`: Each order gets one row
- Aggregation metrics:
  - `F.sum("price")`: Total price of all items in order
  - `F.sum("freight_value")`: Total shipping cost
  - `F.count()`: How many line items (e.g., 3 items = count 3)
  - `F.avg()`: Average price per item
  - `F.first()`: Get the main product ID and seller ID (first item in order)
- Join aggregated items to master
- **Why**: Each order must have ONE row; aggregate items to that level

#### **Step 3: + Payments**
```python
master = master.join(spark.table("payments_agg"), on="order_id", how="left")
```
- Join already-aggregated payments (from preprocessing)
- Adds: total_payment_value, max_payment_installments, primary_payment_type, payment_methods_count

#### **Step 4: + Reviews**
```python
reviews_slim = spark.table("reviews_dedup").select(
    "order_id", "review_score", "review_creation_date", "review_answer_timestamp"
)
master = master.join(reviews_slim, on="order_id", how="left")
```
- Select only needed review columns (avoid carrying unnecessary data)
- Join to master
- Adds: review_score (1-5), review_creation_date, review_answer_timestamp

#### **Step 5: + Products**
```python
products_slim = spark.table("products_clean").select(
    "product_id",
    "product_category_name_english",
    "product_weight_g",
    F.col("product_length_cm"),
    ...
)
master = master.join(products_slim, master.main_product_id == products_slim.product_id, how="left").drop("product_id")
```
- Select product columns (slim down)
- Join on `master.main_product_id == products_slim.product_id` (match main product ID from order items)
- Drop duplicate product_id column
- Adds: product category (English), weight, dimensions (L, H, W)

#### **Step 6: + Sellers**
```python
sellers_slim = spark.table("sellers_clean").select(
    "seller_id",
    F.col("seller_state").alias("seller_state"),
    F.col("seller_city").alias("seller_city")
)
master = master.join(sellers_slim, master.main_seller_id == sellers_slim.seller_id, how="left").drop("seller_id")
```
- Join on `master.main_seller_id == sellers_slim.seller_id`
- Adds: seller's state and city
- **Result**: One big master table with everything joined!

### **Section 3.3: Feature Engineering**

#### **Delivery Performance Features**
```python
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
    )
```
- `F.datediff()`: Calculate days between two dates
  - Positive = late (delivered after estimated)
  - Negative = early (delivered before estimated)
  - Zero = on time
- `F.when(...).otherwise()`: IF-THEN-ELSE
  - IF delivery_delay_days > 0 THEN 1 (late) ELSE 0 (on time)
- Creates binary flag for late delivery analysis

#### **Actual Delivery Duration**
```python
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
)
```
- `actual_delivery_days`: Total days from purchase to delivery
- `days_to_approve`: Days from purchase to approval
- **Why**: Identify bottlenecks (is approval fast? is delivery slow?)

#### **Order Value Features**
```python
.withColumn(
    "total_order_value",
    F.round(F.col("items_total_price") + F.col("items_total_freight"), 2)
) \
.withColumn(
    "is_high_review",
    F.when(F.col("review_score") >= 4, 1).otherwise(0)
)
```
- `total_order_value`: Items + shipping = total cost to customer
- `is_high_review`: Flag for high satisfaction (4-5 stars)
- `F.round(..., 2)`: Round to 2 decimal places (money format)

#### **Temporal Features**
```python
.withColumn("order_year",  F.year("order_purchase_timestamp")) \
.withColumn("order_month", F.month("order_purchase_timestamp")) \
.withColumn("order_day_of_week", F.dayofweek("order_purchase_timestamp")) \
.withColumn("order_hour", F.hour("order_purchase_timestamp"))
```
- Extract date components from purchase timestamp
- `F.year()`: 2017, 2018, etc.
- `F.month()`: 1-12
- `F.dayofweek()`: 1=Sunday, 2=Monday, ... 7=Saturday
- `F.hour()`: 0-23 (hour of purchase)
- **Why**: Analyze seasonal patterns, day-of-week trends, peak hours

#### **Product Features**
```python
.withColumn(
    "product_volume_cm3",
    F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")
) \
.withColumn(
    "review_response_hours",
    (F.col("review_answer_timestamp").cast("long") - F.col("review_creation_date").cast("long")) / 3600
)
```
- `product_volume_cm3`: Length × Height × Width (shipping complexity)
- `review_response_hours`: How quickly seller responded to review
  - Cast timestamps to long (Unix timestamps in seconds)
  - Subtract creation from answer timestamp, divide by 3600 (convert seconds to hours)

#### **Fill Missing Values**
```python
.fillna({
    "delivery_delay_days": 0,
    "is_late_delivery": 0,
    "actual_delivery_days": 0,
    "days_to_approve": 0,
    "review_score": 3.0,  # Assume neutral if missing
    "is_high_review": 0,
    "product_weight_g": 0.0,
    "product_volume_cm3": 0.0,
    "review_response_hours": 0.0,
    "product_category_name_english": "unknown"
})
```
- Fill remaining NAs with sensible defaults
- Delay days = 0: no delay if missing
- Review score = 3: neutral middle rating
- Volume/weight = 0: unknown dimensions
- **Why**: Can't use NAs in ML models; fill with reasonable values

### **Section 3.4: Feature Summary Statistics**
```python
master_featured.select(
    "delivery_delay_days",
    "actual_delivery_days",
    ...
).describe().show()
```
- `.describe()`: Auto-compute count, mean, stddev, min, max for each column
- Example output:
  ```
  |summary|delivery_delay_days|actual_delivery_days|
  |   count|              98000|               98000|
  |   mean |        2.45       |        10.32       |
  |   stddev|        5.12       |         4.11       |
  |   min  |       -15         |         1          |
  |   max  |        35         |         60         |
  ```
- **Why**: Understand data distribution; spot outliers

### **Section 3.5: RFM Analysis (Customer Segmentation)**

#### **Calculate RFM Metrics**
```python
max_date = master_featured.agg(F.max("order_purchase_timestamp")).collect()[0][0]
ref_date = max_date

rfm = master_featured \
    .filter(F.col("order_status") == "delivered") \
    .groupBy("customer_unique_id") \
    .agg(
        F.datediff(F.lit(ref_date), F.max("order_purchase_timestamp")).alias("recency_days"),
        F.count("order_id").alias("frequency"),
        F.sum("total_order_value").alias("monetary")
    )
```
- `F.lit()`: Create a literal value (constant)
- `F.max()`: Get most recent purchase date across all orders
- `ref_date`: Use that as reference for recency calculation
- **RFM Metrics per Customer**:
  - **Recency**: Days since last purchase (lower is better)
  - **Frequency**: Number of orders (higher is better)
  - **Monetary**: Total spent (higher is better)
- **Why**: RFM is classic customer segmentation; identifies best customers

#### **Score R, F, M on 1-4 Scale**
```python
w_r = Window.orderBy(F.col("recency_days").asc())    # lower recency = better
w_f = Window.orderBy(F.col("frequency").desc())       # higher frequency = better
w_m = Window.orderBy(F.col("monetary").desc())        # higher monetary = better

rfm = rfm \
    .withColumn("R_score", F.ntile(4).over(w_r)) \
    .withColumn("F_score", F.ntile(4).over(w_f)) \
    .withColumn("M_score", F.ntile(4).over(w_m))
```
- `F.ntile(4).over(window)`: Divide data into 4 quartiles (score 1-4)
- **Recency window**: Order by recency ascending (1 = most recent, 4 = oldest)
- **Frequency window**: Order by frequency descending (1 = most frequent, 4 = least)
- **Monetary window**: Order by monetary descending (1 = highest spender, 4 = lowest)
- Example: If 10K customers, roughly 2.5K get R_score=1, 2.5K get R_score=2, etc.

#### **Create Customer Segments**
```python
rfm = rfm \
    .withColumn("RFM_score", F.col("R_score") + F.col("F_score") + F.col("M_score")) \
    .withColumn(
        "customer_segment",
        F.when(F.col("RFM_score") >= 10, "Champions")
         .when(F.col("RFM_score") >= 7,  "Loyal Customers")
         .when(F.col("RFM_score") >= 5,  "At Risk")
         .otherwise("Lost Customers")
    )
```
- **RFM_score**: Sum of R + F + M (ranges 3 to 12)
  - 12 = Best customers (recent, frequent, high-value)
  - 3 = Worst customers (old, infrequent, low-value)
- **Segmentation**:
  - **Champions** (≥10): Buy recently, buy often, spend lots → TOP PRIORITY
  - **Loyal Customers** (7-9): Good metrics but not perfect
  - **At Risk** (5-6): Slipping; needs attention
  - **Lost** (<5): Inactive, low-value → May not be worth chasing
- **Why**: Tailor marketing by segment (e.g., special offers for At-Risk to win them back)

### **Section 3.6: Online Retail II Feature Engineering**
```python
online_retail_featured = spark.table("online_retail_clean") \
    .withColumn("InvoiceYear",      F.year("InvoiceDate")) \
    .withColumn("InvoiceMonth",     F.month("InvoiceDate")) \
    .withColumn("InvoiceDayOfWeek", F.dayofweek("InvoiceDate")) \
    .withColumn("InvoiceHour",      F.hour("InvoiceDate")) \
    .withColumn("TotalLineValue",   F.round(F.col("Quantity") * F.col("Price"), 2))

median_val = online_retail_featured.approxQuantile("TotalLineValue", [0.5], 0.01)[0]
online_retail_featured = online_retail_featured \
    .withColumn(
        "is_high_value",
        F.when(F.col("TotalLineValue") > median_val, 1).otherwise(0)
    )
```
- Similar temporal features as Olist
- `F.approxQuantile()`: Compute approximate median (fast, even on huge datasets)
  - Parameters: column, [quantile], accuracy
  - Returns: list, so take [0]
- Binary flag: is_high_value if above median
- **Why**: Classify purchases as high or low value for segmentation

### **Section 3.7: Register Transformed Views**
```python
master_featured.write.mode("overwrite").saveAsTable("master_featured")
rfm.write.mode("overwrite").saveAsTable("rfm_olist")
online_retail_featured.write.mode("overwrite").saveAsTable("online_retail_featured")
rfm_retail.write.mode("overwrite").saveAsTable("rfm_retail")
```
- `.write.mode("overwrite")`: Write to Databricks metastore (persisted tables)
- `"overwrite"`: Replace if exists
- `.saveAsTable()`: Make permanent (available across all notebooks)
- **Why**: EDA notebook needs these tables; persist so other notebooks can access

---

# NOTEBOOK 4: EDA WITH SPARK SQL

**Purpose**: Extract business insights using Spark SQL queries and built-in visualizations.

## Key Insights Generated (20 Queries)

### **Sales & Revenue Analysis**

#### **Q1: Total Revenue, Orders, Average Order Value**
```sql
SELECT
    COUNT(DISTINCT order_id)          AS total_orders,
    COUNT(DISTINCT customer_unique_id) AS unique_customers,
    ROUND(SUM(total_order_value), 2)  AS total_revenue,
    ROUND(AVG(total_order_value), 2)  AS avg_order_value,
    ROUND(MAX(total_order_value), 2)  AS max_order_value,
    ROUND(MIN(total_order_value), 2)  AS min_order_value
FROM master_featured
WHERE order_status = 'delivered'
```
- **Purpose**: High-level KPIs for executive dashboard
- **Metrics**: Total sales, customer base, average value
- **Why**: Key business health indicators

#### **Q2: Monthly Revenue Trend**
```sql
SELECT order_year, order_month,
    CONCAT(order_year, '-', LPAD(order_month, 2, '0')) AS year_month,
    COUNT(DISTINCT order_id)  AS orders_count,
    ROUND(SUM(total_order_value), 2) AS monthly_revenue
FROM master_featured
WHERE order_status = 'delivered' AND order_year IN (2017, 2018)
GROUP BY order_year, order_month
ORDER BY order_year, order_month
```
- **Purpose**: Identify seasonal trends
- **Visualization**: Line chart (month → revenue)
- **Insight**: Is revenue growing? Summer spike? December surge?
- **Why**: Plan inventory, marketing budgets, staffing

#### **Q3: Revenue by State (Top 10)**
```sql
SELECT customer_state,
    COUNT(DISTINCT order_id) AS orders,
    ROUND(SUM(total_order_value), 2) AS revenue
FROM master_featured
WHERE order_status = 'delivered'
GROUP BY customer_state
ORDER BY revenue DESC LIMIT 10
```
- **Purpose**: Geographic revenue concentration
- **Visualization**: Bar chart (state → revenue)
- **Insight**: São Paulo dominates? Regional markets?
- **Why**: Localize marketing; identify underperforming regions

#### **Q4: Orders by Day of Week**
```sql
SELECT
    CASE order_day_of_week
        WHEN 1 THEN 'Sunday' WHEN 2 THEN 'Monday' ... END AS day_name,
    COUNT(*) AS orders_count,
    ROUND(AVG(total_order_value), 2) AS avg_order_value
FROM master_featured
GROUP BY order_day_of_week
ORDER BY order_day_of_week
```
- **Purpose**: Identify peak shopping days
- **Visualization**: Bar chart (day → orders)
- **Insight**: Weekend vs weekday behavior?
- **Why**: Optimize staffing, promotions

### **Customer Analysis**

#### **Q5: Customer Segment Distribution (RFM)**
```sql
SELECT customer_segment,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(monetary), 2) AS avg_monetary
FROM rfm_olist
GROUP BY customer_segment
ORDER BY customer_count DESC
```
- **Purpose**: Customer portfolio composition
- **Output**: % Champions, % Loyal, % At-Risk, % Lost
- **Insight**: Is retention strong? Too many at-risk?
- **Why**: Allocate retention budget to segments

#### **Q6: Repeat vs One-Time Customers**
```sql
SELECT
    CASE WHEN frequency > 1 THEN 'Repeat' ELSE 'One-Time' END AS type,
    COUNT(*) AS customer_count,
    ROUND(AVG(monetary), 2) AS avg_spend
FROM rfm_olist
GROUP BY customer_type
```
- **Purpose**: Loyalty metrics
- **Insight**: % repeat rate? Do repeats spend more?
- **Why**: Compare CAC (customer acquisition cost) vs LTV (lifetime value)

### **Delivery Performance Analysis**

#### **Q7: On-Time vs Late Delivery Rate**
```sql
SELECT
    CASE WHEN is_late_delivery = 1 THEN 'Late' ELSE 'On Time' END AS status,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM master_featured
WHERE order_status = 'delivered' AND delivery_delay_days IS NOT NULL
GROUP BY is_late_delivery
```
- **Purpose**: Service level metrics
- **Insight**: On-time rate = ?% (target is often 95%+)
- **Why**: Customer satisfaction depends on timely delivery

#### **Q8: Average Delivery Time by State (Top 10 Slowest)**
```sql
SELECT customer_state,
    ROUND(AVG(actual_delivery_days), 1) AS avg_delivery_days,
    COUNT(*) AS delivered_orders,
    SUM(is_late_delivery) AS late_orders
FROM master_featured
WHERE order_status = 'delivered' AND actual_delivery_days > 0
GROUP BY customer_state
ORDER BY avg_delivery_days DESC LIMIT 10
```
- **Purpose**: Regional delivery performance
- **Insight**: Remote areas slower? Urban areas fast?
- **Why**: Set realistic delivery estimates; improve logistics in slow regions

#### **Q9: Delivery Performance by Year**
```sql
SELECT order_year,
    COUNT(*) AS total_delivered,
    SUM(is_late_delivery) AS late_deliveries,
    ROUND(SUM(is_late_delivery) * 100.0 / COUNT(*), 2) AS late_pct,
    ROUND(AVG(actual_delivery_days), 1) AS avg_delivery_days
FROM master_featured
WHERE order_status = 'delivered'
GROUP BY order_year
```
- **Purpose**: Trend in delivery performance
- **Insight**: Improving? Degrading?
- **Why**: Track KPI improvements over time

### **Product & Category Analysis**

#### **Q10: Top 10 Product Categories by Revenue**
```sql
SELECT product_category_name_english AS category,
    COUNT(DISTINCT order_id) AS orders,
    ROUND(SUM(total_order_value), 2) AS total_revenue,
    ROUND(AVG(review_score), 2) AS avg_review_score
FROM master_featured
WHERE order_status = 'delivered' AND product_category_name_english != 'unknown'
GROUP BY category
ORDER BY total_revenue DESC LIMIT 10
```
- **Purpose**: Category performance
- **Insight**: Electronics dominate? Books weak?
- **Why**: Allocate inventory, marketing budget to top categories

#### **Q11: Review Score Distribution by Category (Top 10)**
```sql
SELECT product_category_name_english AS category,
    COUNT(*) AS reviews_count,
    ROUND(AVG(review_score), 2) AS avg_score,
    SUM(CASE WHEN review_score = 5 THEN 1 ELSE 0 END) AS score_5,
    SUM(CASE WHEN review_score = 4 THEN 1 ELSE 0 END) AS score_4,
    SUM(CASE WHEN review_score = 3 THEN 1 ELSE 0 END) AS score_3,
    SUM(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END) AS score_1_2
FROM master_featured
WHERE product_category_name_english NOT IN ('unknown', 'other')
GROUP BY category
ORDER BY reviews_count DESC LIMIT 10
```
- **Purpose**: Category satisfaction breakdown
- **Insight**: Which categories have quality issues (low scores)?
- **Why**: Improve suppliers/QC for low-rating categories

### **Payment Analysis**

#### **Q12: Payment Type Distribution**
```sql
SELECT primary_payment_type,
    COUNT(*) AS orders_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(total_payment_value), 2) AS avg_payment
FROM master_featured
GROUP BY primary_payment_type
ORDER BY orders_count DESC
```
- **Purpose**: Payment method usage
- **Insight**: % credit card vs other methods?
- **Why**: Support infrastructure for top methods; deprecate unused ones

#### **Q13: Installments vs Revenue**
```sql
SELECT max_payment_installments AS installments,
    COUNT(*) AS orders_count,
    ROUND(AVG(total_payment_value), 2) AS avg_payment
FROM master_featured
WHERE max_payment_installments <= 12
GROUP BY installments
ORDER BY installments
```
- **Purpose**: Installment popularity
- **Insight**: Do customers prefer financing? 3-month vs 12-month?
- **Why**: Assess credit risk; set limits

### **Review & Satisfaction**

#### **Q14: Overall Review Score Distribution**
```sql
SELECT review_score,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM master_featured
WHERE review_score IS NOT NULL
GROUP BY review_score
ORDER BY review_score DESC
```
- **Purpose**: Overall satisfaction level
- **Visualization**: Pie chart
- **Insight**: % 5-star vs 1-star?
- **Why**: Net Promoter Score (NPS) calculation

#### **Q15: Review Score vs Delivery Performance**
```sql
SELECT is_late_delivery,
    ROUND(AVG(review_score), 3) AS avg_review_score,
    COUNT(*) AS orders_count
FROM master_featured
WHERE review_score IS NOT NULL AND order_status = 'delivered'
GROUP BY is_late_delivery
```
- **Purpose**: Link delivery to satisfaction
- **Insight**: How much does late delivery hurt reviews?
- **Why**: Prioritize on-time delivery improvements

### **Online Retail II Analysis**

#### **Q16: Top 10 Countries by Revenue**
```sql
SELECT Country,
    COUNT(DISTINCT Invoice) AS invoices,
    ROUND(SUM(TotalLineValue), 2) AS total_revenue
FROM online_retail_featured
GROUP BY Country
ORDER BY total_revenue DESC LIMIT 10
```
- **Purpose**: Geographic concentration in retail data
- **Insight**: UK dominates? International presence?
- **Why**: Localize operations

#### **Q17: Monthly Revenue Trend (Online Retail)**
```sql
SELECT InvoiceYear AS year, InvoiceMonth AS month,
    CONCAT(InvoiceYear, '-', LPAD(InvoiceMonth, 2, '0')) AS year_month,
    COUNT(DISTINCT Invoice) AS invoices,
    ROUND(SUM(TotalLineValue), 2) AS monthly_revenue
FROM online_retail_featured
GROUP BY year, month
ORDER BY year, month
```
- **Purpose**: Retail seasonality
- **Visualization**: Line chart
- **Why**: Forecast demand

#### **Q18: Top 10 Best-Selling Products**
```sql
SELECT StockCode, Description,
    SUM(Quantity) AS units_sold,
    ROUND(SUM(TotalLineValue), 2) AS total_revenue,
    ROUND(AVG(Price), 2) AS avg_price
FROM online_retail_featured
GROUP BY StockCode, Description
ORDER BY units_sold DESC LIMIT 10
```
- **Purpose**: Product performance
- **Why**: Focus on winners; discontinue losers

#### **Q19 & Q20: Sales by Hour; High vs Low Value Purchases**
- **Purpose**: Temporal and value-based segmentation
- **Why**: Optimize staffing, pricing strategies

---

# NOTEBOOK 5: EXPORT

**Purpose**: Export processed DataFrames to Parquet and CSV for Power BI and Google Colab.

## Line-by-Line Explanation

### **Setup**
```python
from pyspark.sql import functions as F

EXPORT_PATH = "/Volumes/workspace/default/project_data/exports/"

dbutils.fs.mkdirs(EXPORT_PATH)
print(f"Export directory ready: {EXPORT_PATH}")
```
- Define export location
- `dbutils.fs.mkdirs()`: Create directory if not exists (mkdir -p equivalent)

### **Section 5.1: Export Master Featured Table**
```python
master_df = spark.table("master_featured")

# Parquet (efficient, compressed)
master_df.write.mode("overwrite").parquet(EXPORT_PATH + "master_featured.parquet")
print("Saved master_featured.parquet")

# CSV (for Power BI and Colab upload)
master_df.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(EXPORT_PATH + "master_featured_csv")
print("Saved master_featured_csv")
```
- **Parquet**: Columnar format, highly compressed, fast reads (for archives)
- **CSV**: Human-readable, importable to Power BI and Colab
- `.coalesce(1)`: Combine all partitions into 1 file (Spark normally creates many parts)
  - Without this: output folder contains multiple part-00000, part-00001, etc.
  - With this: single CSV file (easier to download)
- `mode("overwrite")`: Replace if exists
- **Why**: Parquet for efficiency; CSV for portability

### **Sections 5.2-5.4: Export RFM Tables**
```python
rfm_df = spark.table("rfm_olist")
rfm_df.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(EXPORT_PATH + "rfm_olist_csv")

# Similar for online_retail_featured and rfm_retail
```
- Export customer segmentation tables
- **Why**: Power BI uses for customer segment dashboards; Colab uses for K-Means clustering

### **Section 5.5: Export Aggregated Summary Tables**
```python
monthly_revenue = spark.sql("""
    SELECT order_year, order_month,
        CONCAT(order_year, '-', LPAD(order_month, 2, '0')) AS year_month,
        COUNT(DISTINCT order_id) AS orders_count,
        ROUND(SUM(total_order_value), 2) AS monthly_revenue
    FROM master_featured
    WHERE order_status = 'delivered'
    GROUP BY order_year, order_month
    ORDER BY order_year, order_month
""")
monthly_revenue.coalesce(1).write.mode("overwrite").option("header","true").csv(EXPORT_PATH + "summary_monthly_revenue")
```
- Create 4 summary tables for Power BI:
  1. **monthly_revenue**: Timeline of sales
  2. **category_revenue**: Category breakdown
  3. **state_summary**: Geographic analysis
  4. **payment_summary**: Payment method analysis
- Each is pre-aggregated (faster Power BI dashboard loads)
- **Why**: Power BI works best with aggregated data; reduces query load

### **Section 5.6: List Exported Files**
```python
display(dbutils.fs.ls(EXPORT_PATH))
```
- Show all files created
- Verify exports succeeded

### **Section 5.7: Download Instructions**
Documents how to download CSVs:
- Via Databricks UI
- Via CLI
- Which file to use in which tool

---

# KEY CONCEPTS TO EXPLAIN

## 1. **Apache Spark**
- **Distributed computing framework**: Splits work across multiple servers
- **Lazy evaluation**: Spark doesn't run code immediately; waits until `.action()` called (count, write, show, collect)
- **Actions vs Transformations**:
  - **Transformation**: `withColumn`, `join`, `filter` → returns new DataFrame, doesn't execute
  - **Action**: `count()`, `write`, `show()` → triggers actual computation
- **Example**: 10 million rows split across 10 servers = 1 million rows per server; 10x faster

## 2. **DataFrames**
- Spark's tabular data structure (like pandas, SQL table)
- Immutable: transformations create new DataFrames
- Distributed: rows split across clusters

## 3. **Joins**
- **LEFT JOIN**: Keep all left table rows, add matching right rows (nulls if no match)
- **INNER JOIN**: Keep only matching rows
- Example: orders LEFT JOIN customers = all orders + customer info (or NULL if customer missing)

## 4. **Window Functions**
- Compute values **within groups** while keeping all rows
- **PARTITION BY**: Define groups
- **ORDER BY**: Order within groups
- **ROW_NUMBER()**: Assign 1, 2, 3... within each group
- Example: `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY date DESC)` → rank reviews newest first within each order

## 5. **RFM Segmentation**
- **Recency**: How recently customer purchased (lower = better)
- **Frequency**: How often (higher = better)
- **Monetary**: How much spent (higher = better)
- **Quartile Scoring**: Divide customers into 4 groups per metric (1-4 score each)
- **RFM Score**: Sum R+F+M (3-12), higher = more valuable

## 6. **Feature Engineering**
- Create new columns that help analysis/ML
- Examples: delivery_delay_days, is_high_review, product_volume_cm3
- **Why**: Raw data lacks these insights; engineered features make patterns obvious

## 7. **Aggregation**
- Combine multiple rows into summary statistics
- **groupBy**: Split data by column value
- **agg**: Apply function (SUM, COUNT, AVG, MAX, MIN)
- Example: `groupBy("order_id").agg(F.sum("price"))` = total price per order

## 8. **SQL vs PySpark**
- **Spark SQL**: Write queries like traditional SQL (easier for analysts)
- **PySpark**: Write in Python using DataFrame API
- Both execute identically; Spark optimizes either syntax

---

# VIVA Q&A

## **Q1: What is the project about?**
**A**: This is a Big Data e-commerce analytics pipeline that ingests, cleans, transforms, and analyzes 1M+ transactions from two datasets (Olist + Online Retail II). We use Apache Spark on Databricks to process data in parallel, engineer features for ML, segment customers using RFM, and export insights for Power BI dashboards and ML models.

## **Q2: Why Spark instead of pandas or SQL?**
**A**: Spark handles distributed computing. Pandas loads all data into memory (RAM), which fails for 1M+ records on normal computers. Spark splits data across servers and processes in parallel—10x faster. SQL alone can't engineer features like we need. Spark combines SQL, Python, and distributed power.

## **Q3: Walk through the 5 notebooks.**
**A**:
1. **Ingestion**: Load 10 CSV files → Spark DataFrames → SQL temp views (99K orders + 1.06M retail transactions)
2. **Preprocessing**: Clean each table—drop nulls, remove duplicates, validate data types, cast timestamps (reduce ~99K → ~98K after removing bad rows)
3. **Transformation**: Join all tables into 1 master table, engineer 15+ features (delivery delay, temporal features, product volume, RFM scores)
4. **EDA**: Run 20 Spark SQL queries to extract insights (revenue trends, top categories, customer segments, delivery performance)
5. **Export**: Save cleaned/featured data as CSV for Power BI, RFM tables for ML models

## **Q4: What does preprocessing do exactly?**
**A**: Cleans 8 tables:
- **Drop nulls**: Remove rows missing critical fields (order_id, customer_id, price, etc.)
- **Remove duplicates**: Keep 1 per key (e.g., 1 review per order—latest)
- **Fix types**: Cast strings to timestamps for date operations
- **Validate**: Filter to only valid values (prices > 0, review scores 1-5, statuses from known list)
- **Standardize**: Uppercase state codes, lowercase cities, trim whitespace
- **Aggregate**: Some orders have multiple payments—sum into 1 row per order
- Result: Clean data for transformation

## **Q5: How does the master table work?**
**A**: Join all 8 clean tables into 1:
```
orders + customers + order_items (aggregated) + payments (aggregated) 
+ reviews (latest per order) + products + sellers
```
Result: ~98K rows, 1 row per order with all info (customer details, product info, payment, review, seller, etc.). Used for analysis and feature engineering.

## **Q6: What is RFM? How do you compute it?**
**A**: **Recency-Frequency-Monetary** = customer value metric.
- **Recency**: Days since last purchase (lower = more recent = better)
- **Frequency**: # orders (higher = more loyal = better)
- **Monetary**: $ spent (higher = more valuable = better)
- **Scoring**: For each metric, divide customers into quartiles (4 groups). Score 1-4 per metric. Sum R+F+M = 3-12 total.
- **Segmentation**: 
  - RFM≥10: Champions (recent, frequent, high-value)
  - RFM 7-9: Loyal Customers
  - RFM 5-6: At Risk
  - RFM<5: Lost Customers
- **Why**: Identify who to keep (Champions), who to save (At-Risk), who's gone (Lost)

## **Q7: What features did you engineer?**
**A**: Created 15+ features for better insights:
- **Delivery**: delivery_delay_days, is_late_delivery, actual_delivery_days, days_to_approve
- **Order Value**: total_order_value, is_high_review, is_delivered
- **Temporal**: order_year, order_month, order_day_of_week, order_hour
- **Product**: product_volume_cm3, product_weight_g
- **Review**: review_response_hours, is_high_review
- **Why**: Raw data has timestamps and prices; these derived features reveal patterns (e.g., late delivery → lower reviews)

## **Q8: What does JOIN do?**
**A**: Combines rows from 2 tables based on a key.
- **LEFT JOIN orders + customers ON customer_id**: Every order row + matching customer data (or NULL if no customer found)
- **Example**:
  ```
  Orders Table:        Customers Table:
  order_id customer_id | customer_id customer_name
  1        100         | 100         Alice
  2        101         | 101         Bob
  3        999         | (no 999)
  
  LEFT JOIN result:
  order_id customer_id customer_name
  1        100         Alice
  2        101         Bob
  3        999         NULL (no customer 999)
  ```

## **Q9: What is COALESCE in export?**
**A**: `coalesce(1)` combines multiple Spark output partitions into 1 file.
- Spark normally writes many files: part-00000, part-00001, etc. (parallel writes)
- `coalesce(1)` merges them into a single CSV for easy download
- Trade-off: slower to write (1 machine) but easier to use

## **Q10: How does Window function help with reviews?**
**A**: 
```python
w = Window.partitionBy("order_id").orderBy(F.col("review_creation_date").desc())
reviews_dedup.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
```
- Partition by order_id: Group reviews by order
- Order by date desc: Within each order, sort newest first
- Row number: Assign 1, 2, 3... (1 = newest review)
- Filter rn==1: Keep only newest review per order
- Result: 1 review per order (latest)

## **Q11: What are actions vs transformations?**
**A**:
- **Transformation**: `withColumn()`, `join()`, `filter()`, `select()` → returns DataFrame, doesn't run code yet (lazy)
- **Action**: `count()`, `write()`, `show()`, `collect()` → actually executes the code
- **Example**: 
  ```python
  df.filter(...).select(...) # Lazy, no computation yet
  df.filter(...).count() # Action! Now Spark computes
  ```
- Why: Spark optimizes the full computation chain before running; multiple transformations → 1 efficient query

## **Q12: What insights does EDA reveal?**
**A**: 20 queries answered:
- **Revenue**: Total $X, Y orders, Z customers; monthly trend (seasonal?)
- **Customers**: Champion/Loyal/At-Risk/Lost split; repeat rate
- **Delivery**: On-time rate; slowest states; improvement over time
- **Products**: Top categories by revenue; low-rating categories (quality issues)
- **Payment**: % credit card vs other; installment popularity
- **Reviews**: Overall satisfaction; does late delivery hurt reviews?
- **Online Retail**: Top countries, best-selling products, hour-of-day patterns

## **Q13: Why export to CSV instead of keeping as Parquet?**
**A**:
- **Parquet**: Compressed, fast, binary format—good for Spark/archival
- **CSV**: Plain text, human-readable, universally importable
- **Use both**: Parquet for efficient archival; CSV for Power BI (which reads CSV better) and Colab notebooks (easy upload)

## **Q14: How would you scale this to 1B rows?**
**A**: 
- More partitions: `spark.conf.set("spark.sql.shuffle.partitions", "100")` → distribute work to 100 machines
- Sample data for testing: `df.sample(0.01)` → test on 1% first
- Incremental export: Export monthly chunks instead of all at once
- Use more efficient formats: Parquet with snappy compression
- Databricks autoscaling: Add/remove machines based on load

## **Q15: What's the most complex part?**
**A**: Feature engineering (Notebook 3):
- Joining 8 tables correctly (avoid data loss)
- Computing RFM: Understand quartiles, RFM scoring logic
- Window functions for deduplication (latest review per order)
- Handling nulls: Fill with sensible defaults without introducing bias
- Performance: Ensure joins don't create Cartesian products (exploding data)

---

## **PRACTICE TALKING POINTS**

1. **"Start from basics"**: "This project processes big e-commerce data using Apache Spark, which handles distributed computing across servers..."

2. **"Explain the flow"**: "We ingest raw CSVs, clean them, join into a master table, engineer features for analysis, run 20 business queries, and export for dashboards..."

3. **"Show your code**": Open notebooks and walk through 1-2 functions line by line. Pick ones YOU wrote/understand best.

4. **"Know your metrics"**: RFM scores, delivery on-time%, customer segments, revenue by category.

5. **"Explain WHY**": Don't just say "we dropped duplicates"—explain "duplicates cause revenue to be counted twice, inflating metrics."

6. **"Handle questions"**: Interviewers may ask edge cases ("What if customer_id is null?" → "We drop that row because we can't track the customer").

---

**Good luck with your viva! 🚀**
