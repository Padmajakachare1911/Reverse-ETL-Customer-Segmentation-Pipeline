# Reverse ETL Customer Segmentation Pipeline (RFM Scoring + REST API + Orchestration)

A **production-grade Reverse ETL pipeline** that extracts raw customer purchase and interaction metrics, performs **RFM (Recency, Frequency, Monetary) segmentation**, persists analytical data in a **star-schema SQLite data warehouse**, and **pushes enriched segment profiles and recommended marketing actions to a CRM REST API**. 

Features scheduled workflow orchestration using **Prefect** and **Apache Airflow DAGs**, comprehensive logging, resilient retry logic, and an interactive Flask mock CRM server.

---

## 🏗️ Architecture & Orchestration Flow

```
                      AIRFLOW DAG / PREFECT FLOW
┌────────────────────────────────────────────────────────────────────────┐
│                                                                        │
│  [Step 1: Extract & Validate Task]                                     │
│     │  Reads customer_data.csv, verifies schema, columns & types       │
│     ▼                                                                  │
│  [Step 2: Transform & RFM Segmentation Task]                           │
│     │  Cleans outliers, calculates R, F, M scores (VIP, Loyal, etc.)   │
│     ▼                                                                  │
│  [Step 3: Load Warehouse Task]                                         │
│     │  Upserts dim_customers, appends fact_rfm_scores, logs etl_run_log│
│     ▼                                                                  │
│  [Step 4: Reverse ETL Push Task] (with Retries & Exponential Backoff)  │
│     │  Queries SQLite warehouse -> HTTP POST to CRM REST API           │
│     ▼                                                                  │
│  [Step 5: Notify & Audit Task]                                         │
│        Aggregates run metrics & logs execution summary                 │
│                                                                        │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
                ┌─────────────────────────────────────┐
                │ Mock CRM System (Flask - Port 5050) │
                │ ├─ REST Endpoints (POST /api/crm)   │
                │ └─ Live Dashboard: http://127.0.0.1 │
                └─────────────────────────────────────┘
```

---

## 🎯 What Makes This Practical Unique?

| Core Component | Implementation in This Practical |
| :--- | :--- |
| **Segmentation Method** | **Rule-Based RFM Scoring** (Recency, Frequency, Monetary value calculations) |
| **Data Warehouse** | **Star Schema SQLite Database** (`dim_customers`, `fact_rfm_scores`, `etl_run_log`) |
| **Reverse ETL Target** | **Live CRM REST API** (HTTP POST with exponential backoff & retry mechanism) |
| **Workflow Orchestrators** | **Prefect 3.x Flow** + **Apache Airflow DAG** with task chaining and retries |
| **Operational UI** | **Flask Mock CRM Web Dashboard** on `http://127.0.0.1:5050` |
| **Testing & Quality** | Comprehensive unit & integration tests covering all pipeline stages |

---

## 📂 Project Directory Structure

```
01-rfm-orchestrated-crm-pipeline/
├── config/
│   └── config.yaml                     # All configs: thresholds, paths, CRM URL
├── dags/
│   └── reverse_etl_airflow_dag.py      # Apache Airflow DAG definition
├── data/
│   ├── customer_data.csv               # Raw customer purchase dataset
│   └── customer_warehouse.db           # SQLite Star Schema warehouse (generated)
├── logs/                               # Pipeline run logs
├── mock_crm_server/
│   └── app.py                          # Flask CRM API server & web dashboard
├── orchestration/
│   ├── __init__.py
│   └── prefect_flow.py                 # Prefect DAG Flow and Tasks
├── src/
│   ├── __init__.py
│   ├── config_loader.py                # YAML configuration loader
│   ├── crm_client.py                   # HTTP REST CRM client (retry + bulk)
│   ├── extractor.py                    # CSV extraction + validation
│   ├── loader.py                       # SQLite warehouse loader (star schema)
│   ├── logger.py                       # Centralized logging configuration
│   ├── reverse_etl.py                  # Core Reverse ETL extraction & sync
│   └── transformer.py                  # RFM scoring & business segmentation
├── tests/
│   ├── test_airflow_dag.py             # Airflow DAG logic unit tests
│   ├── test_crm_client.py              # CRM client tests (mocked requests)
│   ├── test_extractor.py               # Extractor validation tests
│   ├── test_prefect_flow.py            # Prefect tasks & flow integration tests
│   └── test_transformer.py             # RFM + segmentation tests
├── pipeline.py                         # Standalone CLI pipeline orchestrator
├── requirements.txt                    # Project dependencies
└── README.md                           # This practical guide
```

---

## 📊 Dataset Schema (`customer_data.csv`)

The input dataset contains customer records with purchase, monetary, and recency attributes:

| Column | Description | Example |
| :--- | :--- | :--- |
| `customer_id` | Unique identifier | `CUST001` |
| `name` | Customer full name | `John Doe` |
| `email` | Customer contact email | `john@example.com` |
| `phone` | Phone number | `+1-555-0101` |
| `recency_days` | Days elapsed since last order | `4` |
| `frequency_orders` | Total completed orders | `18` |
| `monetary_value` | Lifetime spend amount ($) | `125000.00` |
| `city` | Customer city | `New York` |

---

## 🏷️ RFM Segmentation & Marketing Rules

Each customer is scored on a scale of 1–3 across Recency, Frequency, and Monetary attributes:
- **Recency**: $\le 7$ days $\rightarrow$ 3; $\le 30$ days $\rightarrow$ 2; $> 30$ days $\rightarrow$ 1
- **Frequency**: $\ge 15$ orders $\rightarrow$ 3; $\ge 8$ orders $\rightarrow$ 2; $< 8$ orders $\rightarrow$ 1
- **Monetary**: $\ge \$100,000$ $\rightarrow$ 3; $\ge \$50,000$ $\rightarrow$ 2; $< \$50,000$ $\rightarrow$ 1

Total Score = $R + F + M$ (ranging from 3 to 9):

| Segment | RFM Score Criteria | Prescribed Marketing Action |
| :--- | :--- | :--- |
| **VIP** | Total Score $\ge 8$ | "Send premium offer" |
| **Loyal** | Total Score $\ge 6$ | "Send loyalty reward" |
| **Regular** | Total Score $\ge 4$ | "Send personalized promotion" |
| **At Risk** | Total Score $< 4$ | "Send win-back campaign" |

---

## 🏛️ Star-Schema Data Warehouse (`customer_warehouse.db`)

The analytical database is built using a star-schema model:

1. **`dim_customers`** (Dimension Table):
   - `customer_id` (PRIMARY KEY), `name`, `email`, `phone`, `city`, `created_at`
2. **`fact_rfm_scores`** (Fact Table):
   - `id`, `customer_id` (FOREIGN KEY), `recency_days`, `frequency_orders`, `monetary_value`, `r_score`, `f_score`, `m_score`, `total_rfm_score`, `segment`, `action_recommended`, `scored_at`
3. **`etl_run_log`** (Audit Trail):
   - `run_id`, `started_at`, `finished_at`, `status`, `records_processed`, `reverse_etl_pushed`, `error_message`

---

## 🚀 Setup & Execution Guide

### 1. Prerequisites & Dependencies
Ensure dependencies are installed:
```bash
pip install -r requirements.txt
```

### 2. Step 1: Start the Mock CRM Server
In a dedicated terminal, launch the Flask mock CRM server:
```bash
python mock_crm_server/app.py
```
- Open your browser at [http://127.0.0.1:5050/](http://127.0.0.1:5050/) to see the real-time CRM UI dashboard.
- The server provides endpoints:
  - `POST /api/crm/customers`: Push an individual customer profile.
  - `POST /api/crm/customers/bulk`: Push a batch of customer profiles.
  - `GET /api/crm/customers`: Retrieve all synchronized customer profiles.
  - `GET /api/health`: Health status endpoint.

### 3. Step 2: Execute the Reverse ETL Pipeline

You can run the pipeline using any of the following orchestration methods:

#### Option A: Direct CLI Orchestrator (`pipeline.py`)
```bash
# Run once end-to-end (bulk push):
python pipeline.py

# Run with single record push:
python pipeline.py --individual

# Run as scheduled background daemon (runs every 60s):
python pipeline.py --schedule
```

#### Option B: Prefect Flow Orchestration (`prefect_flow.py`)
```bash
# Execute the flow directly:
python orchestration/prefect_flow.py

# Run as a scheduled Prefect deployment (interval-based):
python orchestration/prefect_flow.py --serve
```

#### Option C: Apache Airflow DAG (`dags/reverse_etl_airflow_dag.py`)
If running Apache Airflow:
1. Copy the DAG file to your Airflow DAGs directory: `airflow/dags/`.
2. Inspect the DAG on the Airflow Webserver UI: `reverse_etl_customer_segmentation_dag`.
3. Trigger or let it execute on the scheduled hourly interval (`0 * * * *`).

---

## 🧪 Running the Test Suite

Execute all unit and integration tests with `pytest`:
```bash
pytest tests/ -v
```
Tests validate:
- Schema extraction and validation failure handling
- RFM scoring logic and threshold boundaries
- SQLite warehouse upserts and star-schema integrity
- CRM REST API client retries and exponential backoff
- Prefect task orchestration and Flow execution
