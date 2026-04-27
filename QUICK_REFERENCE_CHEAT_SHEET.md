# 🎯 QUICK REFERENCE CHEAT SHEET - 5 NOTEBOOKS AT A GLANCE

## NOTEBOOK 1: DATA INGESTION (Load Phase)

| Step | Code | Purpose |
|------|------|---------|
| Initialize Spark | `SparkSession.builder.appName("...").getOrCreate()` | Start Spark engine |
| Define Paths | `BASE_PATH = "/Volumes/..."` | Central file location |
| Load CSV | `spark.read.csv(path, header=true, inferSchema=true)` | Read CSV to DataFrame |
| Register View | `.createOrReplaceTempView("table_name")` | Make queryable via SQL |
| Check Quality | `.printSchema()`, `.show()`, null check | Verify data loaded correctly |

**Input**: 10 CSV files in cloud storage  
**Output**: 10 DataFrames registered as SQL views  
**Key Function**: `load_csv(path, name)` with consistent CSV options

---

## NOTEBOOK 2: DATA PREPROCESSING (Clean Phase)

| Table | Cleaning Steps | Key Issue |
|-------|----------------|-----------|
| **orders** | Cast timestamps → drop nulls → remove duplicates → validate status | Can't process without order_id |
| **customers** | Drop nulls → dedup by customer_id → standardize case (lower/upper) | Standardize city/state format |
| **order_items** | Drop nulls → filter price > 0 → dedup on (order_id, item_id) → cast date | Zero prices invalid |
| **payments** | Drop nulls → filter value ≥ 0 → validate installments ≥ 1 → **aggregate** | Some orders = multiple payments |
| **reviews** | Drop nulls → cast score to int → filter 1-5 → dedup → **keep latest per order** | Use Window function |
| **products** | Drop nulls → **join category translations** → fill NAs → keep 1 per product | Add English category names |
| **sellers** | Drop nulls → dedup → standardize case | Clean location info |
| **online_retail** | Drop nulls → filter qty/price > 0 → remove cancelled (startswith 'C') → calc TotalLineValue | UK retail data has cancellations |

**Key Technique**: `.fillna()` with sensible defaults

**Output**: 8 clean tables with dramatic null reduction

---

## NOTEBOOK 3: TRANSFORMATION (Feature Engineering Phase)

### STEP 1: Build Master Table (JOIN)
```
orders_clean 
  + customers_clean (on customer_id)
  + order_items_clean (aggregated: sum price, count items, first product/seller)
  + payments_agg (already aggregated)
  + reviews_dedup (1 per order)
  + products_clean (via main_product_id)
  + sellers_clean (via main_seller_id)
= master table (1 row per order with EVERYTHING)
```

### STEP 2: Feature Engineering

| Feature | Calculation | Purpose |
|---------|-----------|---------|
| **delivery_delay_days** | `datediff(delivered_date, estimated_date)` | Positive = late |
| **is_late_delivery** | `CASE WHEN delay_days > 0 THEN 1 ELSE 0` | Binary flag |
| **actual_delivery_days** | `datediff(delivered_date, purchase_date)` | Total delivery time |
| **days_to_approve** | `datediff(approved_at, purchase_date)` | Approval speed |
| **total_order_value** | `items_price + freight_value` | Revenue per order |
| **is_high_review** | `CASE WHEN score ≥ 4 THEN 1 ELSE 0` | Satisfaction flag |
| **order_year/month/hour** | Extract from timestamp | Temporal patterns |
| **product_volume_cm3** | `length × height × width` | Shipping complexity |
| **review_response_hours** | `(answer_timestamp - creation_timestamp) / 3600` | Response speed |

### STEP 3: RFM Segmentation

```python
# Calculate per customer
recency_days = days since last purchase (LOWER = BETTER)
frequency = count of orders (HIGHER = BETTER)
monetary = sum of spend (HIGHER = BETTER)

# Score using NTILE(4) = quartile (1-4 per metric)
R_score ← ntile by recency ascending (1 = most recent)
F_score ← ntile by frequency descending (1 = most frequent)
M_score ← ntile by monetary descending (1 = highest spend)

RFM_score = R_score + F_score + M_score (3-12 range)

# Segment
RFM ≥ 10 → Champions
RFM 7-9  → Loyal Customers
RFM 5-6  → At Risk
RFM < 5  → Lost Customers
```

**Output**: 
- master_featured (98K rows with 50+ columns)
- rfm_olist (customer segments)
- online_retail_featured (retail data with features)
- rfm_retail (retail customer segments)

---

## NOTEBOOK 4: EDA (Analysis Phase)

**20 Business Questions Answered:**

| # | Category | Query | Output |
|---|----------|-------|--------|
| 1-4 | Revenue | Total $, monthly trend, by state, by day-of-week | KPIs, charts |
| 5-6 | Customers | RFM segments, repeat vs one-time | % in each segment |
| 7-9 | Delivery | On-time %, slowest states, year trend | Service levels |
| 10-11 | Products | Top categories, review distribution | Performance, quality |
| 12-13 | Payment | Type distribution, installments | Payment patterns |
| 14-15 | Reviews | Overall distribution, late delivery impact | Satisfaction analysis |
| 16-20 | Online Retail | Top countries, monthly trend, best products, hourly sales, high/low value split | Market insights |

**Visualization**: Use Databricks charts (bar, line, pie)

---

## NOTEBOOK 5: EXPORT (Output Phase)

| File | Format | Size | Use In |
|------|--------|------|--------|
| master_featured.csv | CSV | ~50-100 MB | Power BI (main), Colab NB1 |
| rfm_olist.csv | CSV | ~5 MB | Power BI (customer segment page), Colab clustering |
| online_retail_featured.csv | CSV | ~200 MB | Power BI, Colab NB2 |
| summary_monthly_revenue.csv | CSV | Small | Power BI trend chart |
| summary_category_revenue.csv | CSV | Small | Power BI category breakdown |
| summary_state.csv | CSV | Small | Power BI geographic map |
| summary_payments.csv | CSV | Small | Power BI payment methods |
| master_featured.parquet | Parquet | Compressed | Archive/Spark re-use |

**Export Pattern**: `df.coalesce(1).write.option("header","true").csv(path)`
- `coalesce(1)` = merge Spark partitions into 1 file (easier download)

---

## 🎓 KEY TECHNICAL CONCEPTS

### **Spark Lazy Evaluation**
```python
df.filter(...).select(...)          # No computation yet (lazy)
df.filter(...).select(...).count()  # ACTION → Now computes!
```

### **Transformation vs Action**
- **Transformation**: `.withColumn()`, `.join()`, `.filter()`, `.select()` → returns DataFrame
- **Action**: `.count()`, `.write()`, `.show()`, `.collect()` → triggers execution

### **JOIN Types**
```python
.join(other, on="key", how="left")   # Keep all left, match right (nulls if no match)
.join(other, on="key", how="inner")  # Keep only matches
```

### **Window Functions** (for ranking within groups)
```python
w = Window.partitionBy("group_col").orderBy(F.col("sort_col").desc())
df.withColumn("rank", F.row_number().over(w))
```

### **Aggregation**
```python
df.groupBy("category").agg(
    F.sum("amount"),      # Total
    F.count("*"),         # Count rows
    F.avg("score"),       # Average
    F.max("date")         # Latest
)
```

### **Null Handling**
```python
.dropna(subset=["col1", "col2"])     # Drop if ANY of these are NULL
.fillna({"col1": 0, "col2": "N/A"})  # Replace NAs with values
```

---

## 📊 KEY METRICS TO MEMORIZE

| Metric | Formula | Good Value |
|--------|---------|-----------|
| **On-Time Delivery %** | (orders_on_time / total_orders) × 100 | 95%+ |
| **Customer Repeat Rate** | customers_with_freq>1 / total_customers | 30-50% |
| **Avg Review Score** | sum(scores) / count(reviews) | 4.0+ (out of 5) |
| **Revenue/Customer** | total_revenue / unique_customers | $100+ |
| **RFM Segmentation** | Scores 1-12 (higher = better) | Aim for 30% Champions |
| **Product Volume** | length × height × width (cm³) | Easier shipping if low |

---

## 🔥 COMMON VIVA QUESTIONS & ONE-LINE ANSWERS

| Q | Answer |
|---|--------|
| What is the project? | Big Data e-commerce analytics pipeline—ingest, clean, transform, analyze 1M+ records using Spark |
| Why Spark? | Distributed computing—processes 1M+ rows in parallel across servers (pandas would run out of RAM) |
| Walk through notebooks | Ingest CSVs → Clean/dedup → Join + engineer features → Query insights → Export to CSV/Parquet |
| What is RFM? | Recency/Frequency/Monetary—customer value metric; score 1-4 per dimension (3-12 total); segment into Champions/Loyal/At-Risk/Lost |
| How many rows processed? | Olist: ~99K orders → ~98K after cleaning; Online Retail: 1.06M transactions |
| Biggest challenge? | Joining 8 tables correctly without data loss; handling window functions for deduplication |
| What's in master table? | 1 row per order with customer, product, payment, review, seller info (~50 columns) |
| How many features engineered? | 15+: delivery metrics, temporal, product volume, review response time, customer value flags |
| What EDA revealed? | Revenue $X in Y months, Z% on-time delivery, Top category is A, B% customers are repeat, RFM shows C% Champions |
| CSV or Parquet? | Both: Parquet for efficient archive; CSV for Power BI/Colab portability |

---

## 🚀 VIVA PERFORMANCE TIPS

1. **Show Code**: Open Notebook 2 (Preprocessing) and walk through 1-2 sections—shows understanding of how to manipulate data

2. **Draw Pictures**: 
   - Sketch the join flow: `Orders → Customers → Items → Payments → Reviews → Products → Sellers`
   - Explain data shrinkage: "Raw 99K orders → 98K after dropping nulls/duplicates"
   - RFM segmentation pyramid (40% Lost, 30% At-Risk, 20% Loyal, 10% Champions)

3. **Use Numbers**: 
   - "Project analyzed ~1.1M records"
   - "Engineered 15+ features including delivery_delay_days, review_response_hours, product_volume_cm3"
   - "RFM segmentation identified 30% Champions, 25% Loyal, 25% At-Risk, 20% Lost customers"

4. **Explain Trade-offs**:
   - Parquet vs CSV: Speed vs Portability
   - Coalesce(1) vs Partitions: Single file vs Parallel writes
   - Fill NAs vs Drop: Preserve data vs Remove uncertainty

5. **Show Domain Knowledge**:
   - "Late delivery drives negative reviews (evidence: Query 15 shows -1.2 star impact)"
   - "On-time % of 87% indicates logistics optimization opportunity"
   - "Champion segment (highest RFM) should get VIP treatment; Lost segment should be deprioritized"

6. **Technical Jargon** (use confidently):
   - Lazy evaluation, Transformations vs Actions, Window Functions, Aggregation, Joins
   - RFM scoring, Quartile-based segmentation, Temporal feature extraction
   - Coalesce, Mode('overwrite'), createOrReplaceTempView

---

## 📝 LAST-MINUTE CHECKLIST

- [ ] Can I explain each of the 5 notebooks in 2-3 sentences?
- [ ] Do I understand the master table JOIN order?
- [ ] Can I explain RFM scoring and segmentation?
- [ ] Do I know what 3-5 features do and why?
- [ ] Can I run through 1-2 Spark operations (filter, join, withColumn)?
- [ ] Do I have numbers memorized (99K orders, 1.06M transactions, 15+ features)?
- [ ] Do I understand why late delivery matters (impacts review score)?
- [ ] Can I explain at least 1 EDA query and its business insight?
- [ ] Do I know what CSV vs Parquet trade-off is?
- [ ] Can I draw the data flow: Ingest → Clean → Transform → Analyze → Export?

---

**You've got this! 💪**
