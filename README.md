# 🛒 Olist Brazilian E-Commerce — Oracle ETL Data Pipeline

> **End-to-end PL/SQL ETL pipeline using the real Kaggle Olist dataset (~100K orders, 1M+ geolocation records) loaded into an Oracle 19c Data Warehouse via SQL*Loader**

[![Dataset](https://img.shields.io/badge/Dataset-Kaggle%20Olist-blue)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
[![Database](https://img.shields.io/badge/Database-Oracle%2019c-red)](https://www.oracle.com/)
[![Language](https://img.shields.io/badge/Language-PL%2FSQL-orange)]()
[![Schema](https://img.shields.io/badge/Schema-Star%20Schema-green)]()

---

## 📋 Table of Contents
- [Project Overview](#-project-overview)
- [Dataset — Olist Kaggle](#-dataset--olist-kaggle)
- [Architecture](#-architecture)
- [Repository Structure](#-repository-structure)
- [Data Flow](#-data-flow)
- [Star Schema](#-star-schema)
- [Prerequisites](#-prerequisites)
- [Step-by-Step Setup](#-step-by-step-setup)
- [Running the Pipeline](#-running-the-pipeline)
- [Scheduler Jobs](#-scheduler-jobs)
- [Monitoring & Queries](#-monitoring--queries)
- [GitHub Workflow](#-github-workflow)

---

## 🎯 Project Overview

This project builds a **production-grade ETL Data Pipeline** using Oracle PL/SQL on the publicly available **Olist Brazilian E-Commerce dataset** from Kaggle. It demonstrates real-world skills including:

| Skill | Implementation |
|---|---|
| **SQL*Loader** | Load 9 CSV files into Oracle staging |
| **PL/SQL Packages** | 3 packages — Logger, Transform, Master |
| **Star Schema Design** | 3 Fact + 3 Dimension tables |
| **SCD Type 1 & 2** | Customer (Type 1), Product (Type 2) |
| **Data Quality** | Validation + rejection quarantine table |
| **DBMS_Scheduler** | 3 automated jobs |
| **Idempotency** | Safe re-runs without duplicates |
| **Performance** | Indexes, DIRECT=TRUE loads, aggregation layer |

---

## 📦 Dataset — Olist Kaggle

**Download here:** [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

The dataset contains **9 CSV files** covering 100,000+ real e-commerce orders from Brazil (2016–2018):

| CSV File | Records | Description |
|---|---|---|
| `olist_orders_dataset.csv` | ~99,441 | Core orders with status & timestamps |
| `olist_order_items_dataset.csv` | ~112,650 | Line items — product, seller, price, freight |
| `olist_order_payments_dataset.csv` | ~103,886 | Payment type, installments, value |
| `olist_order_reviews_dataset.csv` | ~99,224 | Customer reviews, scores 1–5 |
| `olist_customers_dataset.csv` | ~99,441 | Customer location (city, state, zip) |
| `olist_sellers_dataset.csv` | ~3,095 | Seller location |
| `olist_products_dataset.csv` | ~32,951 | Product specs and categories |
| `olist_geolocation_dataset.csv` | ~1,000,163 | Zip code → lat/lng coordinates |
| `product_category_name_translation.csv` | ~71 | Portuguese → English category names |

---

## 🏗️ Architecture

```
╔══════════════════════════════════════════════════════════════════╗
║                    DATA SOURCES (Kaggle)                         ║
║  9 CSV Files: orders, items, payments, reviews,                  ║
║               customers, sellers, products, geo, categories      ║
╚═════════════════════════════╦════════════════════════════════════╝
                              │
                    SQL*Loader (Direct Path)
                    run_sqlldr.sh
                              │
╔═════════════════════════════▼════════════════════════════════════╗
║                    STAGING LAYER (STG Schema)                    ║
║  STG_ORDERS       STG_ORDER_ITEMS    STG_ORDER_PAYMENTS          ║
║  STG_ORDER_REVIEWS  STG_CUSTOMERS    STG_SELLERS                 ║
║  STG_PRODUCTS     STG_GEOLOCATION   STG_CATEGORY_TRANSLATION     ║
║  STG_REJECTED_RECORDS (quarantine)                               ║
╚═════════════════════════════╦════════════════════════════════════╝
                              │
                    PKG_TRANSFORM (PL/SQL)
                    Validate → Dimensions → Facts → Aggregates
                              │
╔═════════════════════════════▼════════════════════════════════════╗
║                DATA WAREHOUSE LAYER (DWH Schema)                 ║
║  DIMENSIONS:                                                     ║
║    DIM_DATE        DIM_CUSTOMER (SCD1)   DIM_PRODUCT (SCD2)      ║
║    DIM_SELLER (SCD1)                                             ║
║  FACTS:                                                          ║
║    FACT_ORDERS     FACT_ORDER_ITEMS      FACT_REVIEWS            ║
║  AGGREGATES:                                                     ║
║    AGG_DAILY_SALES (pre-aggregated for BI performance)           ║
╚═════════════════════════════╦════════════════════════════════════╝
                              │
              ╔═══════════════╩═══════════════╗
              ▼                               ▼
        Power BI / Tableau              Excel / Reports
```

---

## 📁 Repository Structure

```
olist-oracle-etl-pipeline/
│
├── README.md                           ← You are here
│
├── sql/
│   ├── 01_create_schemas.sql           ← Create STG, DWH, ETL_CTRL users (run as SYSDBA)
│   ├── 02_staging_ddl.sql             ← All 9 staging tables
│   └── 03_dwh_ddl.sql                 ← DIM + FACT + AGG tables + DIM_DATE population
│
├── packages/
│   ├── 01_pkg_etl_logger.sql          ← Batch log, step log, watermark, rejection handler
│   ├── 02_pkg_transform.sql           ← DQ, DIM loads, FACT loads, AGG refresh
│   └── 03_pkg_etl_master.sql          ← Master orchestrator + scheduler jobs
│
├── sqlldr/
│   ├── 01_orders.ctl                  ← olist_orders_dataset.csv
│   ├── 02_order_items.ctl             ← olist_order_items_dataset.csv
│   ├── 03_order_payments.ctl          ← olist_order_payments_dataset.csv
│   ├── 04_order_reviews.ctl           ← olist_order_reviews_dataset.csv
│   ├── 05_customers.ctl               ← olist_customers_dataset.csv
│   ├── 06_sellers.ctl                 ← olist_sellers_dataset.csv
│   ├── 07_products.ctl                ← olist_products_dataset.csv
│   ├── 08_geolocation.ctl             ← olist_geolocation_dataset.csv (~1M rows)
│   └── 09_category_translation.ctl   ← product_category_name_translation.csv
│
├── scripts/
│   ├── deploy_all.sql                 ← One-click deploy all objects
│   └── run_sqlldr.sh                  ← Shell script: loads CSVs + triggers ETL
│
└── .gitignore
```

---

## 🔄 Data Flow

```
STEP 1  Download CSVs from Kaggle → /data/olist/csv/

STEP 2  run_sqlldr.sh
        ├─ Truncate all STG tables
        ├─ sqlldr 01_orders.ctl          → STG_ORDERS
        ├─ sqlldr 02_order_items.ctl     → STG_ORDER_ITEMS
        ├─ sqlldr 03_order_payments.ctl  → STG_ORDER_PAYMENTS
        ├─ sqlldr 04_order_reviews.ctl   → STG_ORDER_REVIEWS
        ├─ sqlldr 05_customers.ctl       → STG_CUSTOMERS
        ├─ sqlldr 06_sellers.ctl         → STG_SELLERS
        ├─ sqlldr 07_products.ctl        → STG_PRODUCTS
        ├─ sqlldr 08_geolocation.ctl     → STG_GEOLOCATION
        └─ sqlldr 09_category.ctl        → STG_CATEGORY_TRANSLATION

STEP 3  PKG_ETL_MASTER.run_pipeline()
        ├─ tag_staging_batch()           → Set batch_id on STG rows
        └─ PKG_TRANSFORM.run_all()
           ├─ validate_staging()         → DQ checks → STG_REJECTED_RECORDS
           ├─ load_dim_customer()        → STG_CUSTOMERS + GEO → DIM_CUSTOMER
           ├─ load_dim_seller()          → STG_SELLERS + GEO   → DIM_SELLER
           ├─ load_dim_product()         → STG_PRODUCTS + TRANS → DIM_PRODUCT (SCD2)
           ├─ load_fact_orders()         → STG_ORDERS + PMTS   → FACT_ORDERS
           ├─ load_fact_order_items()    → STG_ORDER_ITEMS      → FACT_ORDER_ITEMS
           ├─ load_fact_reviews()        → STG_ORDER_REVIEWS    → FACT_REVIEWS
           └─ refresh_agg_daily_sales() → DWH FACTS            → AGG_DAILY_SALES
```

---

## ⭐ Star Schema

```
                          DIM_DATE
                         (date_key)
                              │
               DIM_CUSTOMER ──┤
               (customer_key)  │
                              │
DIM_PRODUCT ──────────── FACT_ORDER_ITEMS ──── DIM_SELLER
(product_key)              (grain: line item)   (seller_key)
                              │
                         FACT_ORDERS
                         (grain: order)
                              │
                         FACT_REVIEWS
                         (grain: review)
                              │
                        AGG_DAILY_SALES
                        (pre-aggregated)
```

---

## ✅ Prerequisites

- **Oracle Database 19c+** (Express Edition is free at oracle.com/xe)
- **SQL*Loader** (included with Oracle Client / Instant Client)
- **SQLcl or SQL*Plus** for running .sql files
- **Git** installed
- **Kaggle account** (free) to download the dataset

---

## 🚀 Step-by-Step Setup

### Step 1: Clone the Repo
```bash
git clone https://github.com/YOUR_USERNAME/olist-oracle-etl-pipeline.git
cd olist-oracle-etl-pipeline
```

### Step 2: Download Olist Dataset from Kaggle
1. Go to [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
2. Click **Download** (free account required)
3. Unzip all 9 CSV files to `/data/olist/csv/`

```bash
mkdir -p /data/olist/csv /data/olist/logs
unzip archive.zip -d /data/olist/csv/
ls /data/olist/csv/   # Should show 9 .csv files
```

### Step 3: Create Oracle Schemas (run as SYSDBA)
```bash
sqlplus sys/yourpassword@//localhost:1521/ORCL as sysdba @sql/01_create_schemas.sql
```

### Step 4: Deploy All Database Objects
```bash
sqlplus ETL_CTRL/EtlPass123#@//localhost:1521/ORCL @scripts/deploy_all.sql
```

### Step 5: Configure Connection in Shell Script
Edit `scripts/run_sqlldr.sh` and update:
```bash
DB_HOST="localhost"       # Your Oracle host
DB_PORT="1521"            # Your Oracle port
DB_SVC="ORCL"             # Your Oracle service name
CSV_DIR="/data/olist/csv" # Path to downloaded CSVs
```

---

## ▶️ Running the Pipeline

### Option A — Full Pipeline (Recommended)
```bash
chmod +x scripts/run_sqlldr.sh
./scripts/run_sqlldr.sh
```
This runs SQL*Loader for all 9 files then triggers the PL/SQL transform pipeline.

### Option B — Manual PL/SQL trigger (if staging already loaded)
```sql
BEGIN
    ETL_CTRL.PKG_ETL_MASTER.run_pipeline('MANUAL');
END;
/
```

### Option C — Step by step
```sql
-- Run only transforms (after manually loading staging)
DECLARE v_b NUMBER;
BEGIN
    v_b := ETL_CTRL.PKG_ETL_LOGGER.start_batch('TEST','OLIST_ETL','MANUAL');
    ETL_CTRL.PKG_ETL_MASTER.tag_staging_batch(v_b);
    ETL_CTRL.PKG_TRANSFORM.run_all(v_b);
    ETL_CTRL.PKG_ETL_LOGGER.end_batch(v_b,'SUCCESS');
END;
/
```

---

## ⏰ Scheduler Jobs

| Job Name | Schedule | Purpose |
|---|---|---|
| `JOB_OLIST_DAILY_ETL` | Daily at 2:00 AM | Full pipeline run |
| `JOB_OLIST_HOURLY_AGG` | Every 1 hour | Refresh AGG_DAILY_SALES |
| `JOB_OLIST_HEALTH_CHECK` | Every 30 minutes | Log pipeline health |

**Setup jobs:**
```sql
BEGIN ETL_CTRL.PKG_ETL_MASTER.setup_scheduler_jobs; END;
/
```

**Check job status:**
```sql
SELECT JOB_NAME, STATE, LAST_START_DATE, NEXT_RUN_DATE, RUN_COUNT, FAILURE_COUNT
FROM DBA_SCHEDULER_JOBS
WHERE OWNER = 'ETL_CTRL'
ORDER BY JOB_NAME;
```

---

## 📊 Monitoring & Queries

### View Pipeline Run History
```sql
SELECT BATCH_ID, BATCH_NAME, STATUS,
       TO_CHAR(START_DT,'YYYY-MM-DD HH24:MI') STARTED,
       TOTAL_RECORDS, SUCCESS_RECORDS, REJECTED_RECORDS,
       ROUND((END_DT - START_DT)*60,1) AS MINS
FROM ETL_CTRL.ETL_BATCH_LOG
ORDER BY START_DT DESC;
```

### View Step-Level Details
```sql
SELECT STEP_NAME, STEP_STATUS, RECORDS_IN, RECORDS_OUT,
       RECORDS_REJECTED, DURATION_SECS
FROM ETL_CTRL.ETL_STEP_LOG
WHERE BATCH_ID = :batch_id
ORDER BY START_DT;
```

### Business Query — Top 10 Product Categories by Revenue
```sql
SELECT dp.CATEGORY_NAME_EN,
       COUNT(DISTINCT fo.ORDER_ID)     AS ORDERS,
       SUM(fi.PRICE)                   AS TOTAL_REVENUE,
       ROUND(AVG(fo.REVIEW_SCORE),2)   AS AVG_REVIEW
FROM DWH.FACT_ORDER_ITEMS fi
JOIN DWH.FACT_ORDERS      fo ON fo.ORDER_ID    = fi.ORDER_ID
JOIN DWH.DIM_PRODUCT      dp ON dp.PRODUCT_KEY = fi.PRODUCT_KEY
                             AND dp.IS_CURRENT  = 'Y'
GROUP BY dp.CATEGORY_NAME_EN
ORDER BY TOTAL_REVENUE DESC
FETCH FIRST 10 ROWS ONLY;
```

### Business Query — Monthly Revenue Trend
```sql
SELECT d.YEAR_NUM, d.MONTH_NAME,
       COUNT(DISTINCT fo.ORDER_ID)  AS ORDERS,
       ROUND(SUM(fo.TOTAL_PAYMENT_VALUE),2) AS REVENUE
FROM DWH.FACT_ORDERS fo
JOIN DWH.DIM_DATE    d  ON d.DATE_KEY = fo.ORDER_DATE_KEY
WHERE fo.ORDER_STATUS = 'DELIVERED'
GROUP BY d.YEAR_NUM, d.MONTH_NUM, d.MONTH_NAME
ORDER BY d.YEAR_NUM, d.MONTH_NUM;
```

### Business Query — Late Delivery Rate by State
```sql
SELECT dc.STATE, dc.STATE_NAME,
       COUNT(*)                                          AS TOTAL_ORDERS,
       SUM(CASE WHEN fo.IS_LATE_DELIVERY='Y' THEN 1 ELSE 0 END) AS LATE_ORDERS,
       ROUND(SUM(CASE WHEN fo.IS_LATE_DELIVERY='Y' THEN 1 ELSE 0 END)
             / COUNT(*) * 100, 1)                        AS LATE_PCT
FROM DWH.FACT_ORDERS fo
JOIN DWH.DIM_CUSTOMER dc ON dc.CUSTOMER_KEY = fo.CUSTOMER_KEY
WHERE fo.ORDER_STATUS = 'DELIVERED'
GROUP BY dc.STATE, dc.STATE_NAME
ORDER BY LATE_PCT DESC;
```

---

## 🔀 GitHub Workflow

```bash
# Branch strategy
main        ← Production only
develop     ← Integration branch
feature/xxx ← New features

# Day-to-day
git checkout develop
git pull origin develop
git checkout -b feature/add-seller-performance-fact

# Make changes to .sql files
git add packages/02_pkg_transform.sql
git commit -m "feat: add seller performance aggregation to transform package"
git push origin feature/add-seller-performance-fact

# Open Pull Request → develop on GitHub
```

### Commit Message Convention
```
feat: add new fact or dimension
fix:  correct SQL logic bug
perf: optimize slow query or index
docs: update README or comments
test: add validation query
refactor: restructure package logic
```

---

## 📈 Expected Record Counts After Full Load

| Table | Expected Rows |
|---|---|
| STG_ORDERS | ~99,441 |
| STG_ORDER_ITEMS | ~112,650 |
| STG_ORDER_PAYMENTS | ~103,886 |
| STG_ORDER_REVIEWS | ~99,224 |
| STG_CUSTOMERS | ~99,441 |
| STG_SELLERS | ~3,095 |
| STG_PRODUCTS | ~32,951 |
| STG_GEOLOCATION | ~1,000,163 |
| DIM_CUSTOMER | ~96,096 (unique customers) |
| DIM_PRODUCT | ~32,951 |
| DIM_SELLER | ~3,095 |
| FACT_ORDERS | ~99,441 |
| FACT_ORDER_ITEMS | ~112,650 |
| FACT_REVIEWS | ~98,410 |
| AGG_DAILY_SALES | ~15,000–20,000 |

---

## 📄 License

This project is open source for learning and portfolio use.
The Olist dataset is publicly available on Kaggle under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/).

---

*Built with ❤️ using Oracle PL/SQL — a real-world ETL project using real data.*
