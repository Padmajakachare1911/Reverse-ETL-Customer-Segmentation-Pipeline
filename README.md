# Reverse ETL Customer Segmentation: Comparative Data Engineering Practicals

This repository showcases **two distinct, production-grade Data Engineering approaches to Customer Segmentation and Reverse ETL (Extract, Transform, Load)**. 

While traditional ETL moves data from operational systems into an analytical warehouse, **Reverse ETL** extracts enriched analytical intelligence (such as customer clusters, lifetime value, or churn predictions) from the data warehouse and **syncs it back into operational systems** (such as CRMs, sales tools, and automated marketing workflows).

---

## 🧭 Repository Structure & Overview

```
Reverse-ETL-Customer-Segmentation-Pipeline/
├── 01-rfm-orchestrated-crm-pipeline/        # Practical 1: Rule-Based RFM + Star Schema + REST API + DAG Orchestration
│   ├── config/                             # Configuration files (YAML)
│   ├── dags/                               # Apache Airflow DAG definitions
│   ├── data/                               # Input purchase metrics & SQLite warehouse
│   ├── mock_crm_server/                    # Flask Mock CRM REST API & Live Web UI
│   ├── orchestration/                      # Prefect Flow definitions and task runners
│   ├── src/                                # Core ETL & Reverse ETL pipeline modules
│   ├── tests/                              # Pytest test suite (57 unit & integration tests)
│   ├── pipeline.py                         # Standalone CLI pipeline runner
│   ├── requirements.txt                    # Practical 1 dependencies
│   └── README.md                           # Comprehensive Practical 1 documentation
│
├── 02-kmeans-duckdb-clustering-pipeline/    # Practical 2: Machine Learning K-Means + DuckDB + File Sync
│   ├── customers.csv                       # Demographic & behavioral customer dataset
│   ├── customer_analytics.duckdb           # DuckDB embedded analytical database
│   ├── crm_customers.csv                   # Enriched customer records exported for CRM
│   ├── customer_segments.csv               # Segment mapping table
│   ├── segment_profiles.csv                # Cluster summary statistics & personas
│   ├── customer_segments.png               # 2D PCA cluster visualization plot
│   ├── main.py                             # Complete ML & DuckDB Reverse ETL pipeline
│   ├── requirements.txt                    # Practical 2 dependencies
│   └── README.md                           # Comprehensive Practical 2 documentation
│
├── .gitignore                              # Git exclusion rules
└── README.md                               # This master documentation
```

---

## ⚖️ Comparative Analysis: The Two Methods

| Feature / Dimension | Practical 1 (`01-rfm-orchestrated-crm-pipeline`) | Practical 2 (`02-kmeans-duckdb-clustering-pipeline`) |
| :--- | :--- | :--- |
| **Method Name** | **Rule-Based RFM Scoring & API Push** | **Unsupervised ML (K-Means) & DuckDB Sync** |
| **Segmentation Approach** | **Deterministic Business Rules**: Recency, Frequency, Monetary (RFM) thresholds | **Machine Learning**: Unsupervised K-Means clustering ($k=5$) + PCA reduction |
| **Analytical Warehouse** | **Star-Schema SQLite** (`dim_customers`, `fact_rfm_scores`, `etl_run_log`) | **DuckDB Embedded OLAP** (`customer_analytics.duckdb` columnar engine) |
| **Reverse ETL Mechanism** | **Live HTTP REST API**: JSON POST requests with exponential backoff & retries | **Operational File/Storage Sync**: CRM-ready CSV export + DuckDB table sync |
| **Downstream Destination** | **Flask Mock CRM Service** with live UI dashboard (`http://127.0.0.1:5050`) | **Operational Marketing CRM Dataset** (`crm_customers.csv`) + PCA visual chart |
| **Workflow Orchestration** | **Prefect 3.x Flow** + **Apache Airflow DAG** (`reverse_etl_airflow_dag.py`) | **Standalone Script Pipeline** (`main.py`) with Scikit-Learn transformers |
| **Actionable Output** | Prescribed campaign actions (e.g., "Send premium offer", "Win-back") | Persona profiles (e.g., "Younger Customers", "Older Customers") + PCA scatter plot |
| **Testing Suite** | 57 automated tests covering extraction, transformation, DB, and API mocks | Built-in data quality validation and console metrics reporting |
| **Best Industry Use Case** | Transaction-heavy eCommerce platforms feeding real-time CRM webhooks | Exploratory customer analytics, churn/propensity cohorts, local OLAP exploration |

---

## 🏛️ High-Level Architectures

### Method 1: RFM Scoring, Star Schema, & REST API Push
```
  [Raw Purchase CSV]
          │
          ▼
  [Data Extraction & Validation]
          │
          ▼
  [RFM Scoring Engine] ───────────► Assigns VIP, Loyal, Regular, At Risk
          │
          ▼
  [Star-Schema SQLite Warehouse] ──► Tables: dim_customers, fact_rfm_scores, etl_run_log
          │
          ▼ (Reverse ETL Step)
  [HTTP REST API Client] ─────────► POST /api/crm/customers (with Exponential Backoff)
          │
          ▼
  [Flask Mock CRM Service] ───────► Real-Time Web Dashboard (Port 5050)
```

### Method 2: Machine Learning K-Means Clustering & DuckDB Sync
```
  [Raw Customer Demographics CSV]
          │
          ▼
  [Standardize & Clean] ──────────► DuckDB Table: raw_customers
          │
          ▼
  [ColumnTransformer Preprocessing] (One-Hot Encoding, Median Imputation, Standard Scaling)
          │
          ▼
  [K-Means Clustering + PCA] ─────► 5 Natural Customer Personas
          │
          ▼
  [DuckDB Analytical Warehouse] ──► Tables: customer_segments, segment_profiles
          │
          ▼ (Reverse ETL Step)
  [Operational CRM Exporter] ─────► crm_customers.csv, segment_profiles.csv, customer_segments.png
```

---

## 🚀 Quick Start Guide

### 1. Global Setup

Create and activate a virtual environment:
```bash
python -m venv .venv

# On Windows:
.venv\Scripts\activate

# On macOS/Linux:
source .venv/bin/activate
```

---

### 2. Running Practical 1 (`01-rfm-orchestrated-crm-pipeline`)

1. Change directory to Practical 1:
   ```bash
   cd 01-rfm-orchestrated-crm-pipeline
   pip install -r requirements.txt
   ```

2. Start the Mock CRM Server in one terminal:
   ```bash
   python mock_crm_server/app.py
   ```
   *(View the web UI at [http://127.0.0.1:5050](http://127.0.0.1:5050))*

3. In a second terminal, execute the Reverse ETL pipeline:
   ```bash
   # Option A: Run standalone pipeline CLI
   python pipeline.py

   # Option B: Run with Prefect Flow orchestration
   python orchestration/prefect_flow.py

   # Option C: Run all unit and integration tests
   pytest tests/ -v
   ```

*For complete details, see [01-rfm-orchestrated-crm-pipeline/README.md](file:///01-rfm-orchestrated-crm-pipeline/README.md).*

---

### 3. Running Practical 2 (`02-kmeans-duckdb-clustering-pipeline`)

1. Change directory to Practical 2:
   ```bash
   cd 02-kmeans-duckdb-clustering-pipeline
   pip install -r requirements.txt
   ```

2. Run the end-to-end pipeline:
   ```bash
   python main.py
   ```

3. Outputs generated:
   - `customer_analytics.duckdb`: Columnar database with raw and segmented tables.
   - `crm_customers.csv`: Customer records enriched with predicted personas.
   - `segment_profiles.csv`: Mean metrics per cluster.
   - `customer_segments.png`: High-resolution 2D PCA cluster visualization.

*For complete details, see [02-kmeans-duckdb-clustering-pipeline/README.md](file:///02-kmeans-duckdb-clustering-pipeline/README.md).*

---

## 📌 Summary: When to Use Which Method?

- **Use Method 1 (RFM + REST API)** when:
  - Your business rules are deterministic and tied directly to recency, transaction frequency, and total revenue.
  - You require enterprise workflow DAG coordination (Prefect / Airflow) with automated retries and alerting.
  - Your downstream destination is a live third-party cloud service accessible via REST APIs (e.g., Salesforce, HubSpot, Klaviyo).

- **Use Method 2 (K-Means + DuckDB)** when:
  - You have multi-dimensional demographic, behavioral, or lifestyle data without pre-defined segment boundaries.
  - You need fast, in-process analytical data warehousing (DuckDB) without external database server overhead.
  - Your downstream destination consumes flat analytical extracts, batch sFTP drops, or operational data lakes.
