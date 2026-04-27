# BigData E-Commerce Analytics Pipeline

An end-to-end distributed data pipeline built on **Apache Spark** and **Databricks**, analyzing **1.1M+ e-commerce transactions** across two datasets. The pipeline covers data ingestion, preprocessing, feature engineering, RFM customer segmentation, and business intelligence via Power BI dashboards.

---

## Pipeline Architecture

```
Raw CSV Files (Databricks DBFS)
           │
           ▼
 [NB1] Data Ingestion
  └── Load 10 CSVs → Spark DataFrames → SQL Temp Views
           │
           ▼
 [NB2] Data Preprocessing
  └── Null handling, deduplication, type casting, validation
           │
           ▼
 [NB3] Transformation & Feature Engineering
  └── 7-table join → Master DataFrame → 15+ features → RFM Segmentation
           │
           ▼
 [NB4] EDA with Spark SQL
  └── 20 business insight queries across 7 domains
           │
           ▼
 [NB5] Export
  └── Parquet + CSV → Power BI Dashboards & Colab ML Notebooks
```

---

## Tech Stack

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Google Colab](https://img.shields.io/badge/Google%20Colab-F9AB00?style=for-the-badge&logo=googlecolab&logoColor=black)

---

## Datasets

| Dataset | Records | Source |
|---|---|---|
| Olist Brazilian E-Commerce | ~99K orders (9 CSV files) | [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) |
| Online Retail II (UK) | ~1.06M transactions | [Kaggle](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci) |

> Datasets are not included in this repo due to size. Download from the links above and place them in a `Datasets/` folder.

---

## Notebooks

| Notebook | Description |
|---|---|
| [01_data_ingestion.py](databricks_notebooks/01_data_ingestion.py) | Load raw CSVs into Spark DataFrames, register SQL temp views, null profiling |
| [02_preprocessing.py](databricks_notebooks/02_preprocessing.py) | Clean all tables — nulls, duplicates, type casting, validation, aggregation |
| [03_transformation.py](databricks_notebooks/03_transformation.py) | Join 7 tables into master DataFrame, engineer 15+ features, RFM segmentation |
| [04_eda_spark_sql.py](databricks_notebooks/04_eda_spark_sql.py) | 20 Spark SQL queries — revenue, customers, delivery, products, payments |
| [05_export.py](databricks_notebooks/05_export.py) | Export to Parquet + CSV for Power BI and Colab |

---

## Key Features

### Feature Engineering (Notebook 3)
- `delivery_delay_days` — days between estimated and actual delivery
- `is_late_delivery` — binary flag for late orders
- `actual_delivery_days` — total days from purchase to delivery
- `total_order_value` — items + freight cost
- `product_volume_cm3` — length × height × width
- `order_year`, `order_month`, `order_day_of_week`, `order_hour` — temporal features
- `review_response_hours` — seller response time to reviews

### RFM Customer Segmentation
Customers scored 1–4 on **Recency**, **Frequency**, and **Monetary** using quartile-based `NTILE(4)` window functions:

| Segment | RFM Score | Description |
|---|---|---|
| Champions | ≥ 10 | Recent, frequent, high-value |
| Loyal Customers | 7 – 9 | Consistently good metrics |
| At Risk | 5 – 6 | Slipping — needs attention |
| Lost Customers | < 5 | Inactive and low-value |

### EDA Insights (Notebook 4)
- Monthly revenue trends and seasonality
- Revenue breakdown by state and product category
- On-time vs late delivery rates by region
- Payment method distribution and installment preferences
- Review score correlation with delivery performance
- Top products, peak hours, and country-level retail analysis

---

## Power BI Dashboard

<!-- ADD YOUR SCREENSHOTS BELOW -->
<!-- Save your screenshots inside the screenshots/ folder, then replace the lines below -->

![Dashboard Overview](screenshots/dashboard_overview.png)

![Revenue Trends](screenshots/revenue_trends.png)

![Customer Segmentation](screenshots/customer_segmentation.png)

![Delivery Performance](screenshots/delivery_performance.png)

---

## Project Structure

```
BigData-Ecommerce-Pipeline/
├── databricks_notebooks/
│   ├── 01_data_ingestion.py
│   ├── 02_preprocessing.py
│   ├── 03_transformation.py
│   ├── 04_eda_spark_sql.py
│   └── 05_export.py
├── colab_notebooks/
│   ├── ML_Notebook1_Olist.ipynb
│   └── ML_Notebook2_OnlineRetail.ipynb
├── power_bi dashboard/
│   └── Big_Data_Final_Dashboard.pbix
├── screenshots/           ← Add your dashboard screenshots here
└── README.md
```

---

## How to Run

1. Upload all CSV files from the datasets above to Databricks DBFS:
   ```
   /Volumes/workspace/default/project_data/
   ```
2. Import the notebooks from `databricks_notebooks/` into Databricks
3. Run notebooks in order: `01 → 02 → 03 → 04 → 05`
4. Download the exported CSVs and open `Big_Data_Final_Dashboard.pbix` in Power BI Desktop
