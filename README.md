# Reverse ETL Customer Segmentation Pipeline

A **production-grade** Reverse ETL pipeline with modern **Workflow Orchestration (Prefect & Apache Airflow)**:
1. Extracts raw customer data from a CSV source
2. Cleans, scores (RFM), and segments customers
3. Loads the enriched data into a **star-schema SQLite data warehouse**
4. **Pushes segmented data to a CRM system via REST API** — the true Reverse ETL step
5. **Scheduled DAG Orchestration** with retries, failure alerting, and dependency chaining using **Prefect** & **Apache Airflow**

---

## Architecture & Workflow Orchestration

```
                      AIRFLOW DAG / PREFECT FLOW
┌────────────────────────────────────────────────────────────────────────┐
│                                                                        │
│  [Step 1: Extract & Validate Task]                                      │
│     │  Reads customer_data.csv, verifies schema & types                │
│     ▼                                                                  │
│  [Step 2: Transform & RFM Segmentation Task]                           │
│     │  Cleans outliers, calculates RFM scores (VIP, Loyal, Regular, At Risk)
│     ▼                                                                  │
│  [Step 3: Load Warehouse Task]                                         │
│     │  Upserts dim_customers, appends fact_rfm_scores, logs etl_run_log│
│     ▼                                                                  │
│  [Step 4: Reverse ETL Push Task] (with Retries & Backoff)              │
│     │  Queries analytical warehouse -> HTTP POST to CRM REST API       │
│     ▼                                                                  │
│  [Step 5: Notify & Audit Task]                                         │
│        Aggregates run metrics & logs summary                           │
│                                                                        │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
                ┌─────────────────────────────────────┐
                │ Mock CRM System (Flask - Port 5050) │
                │ ├─ REST Endpoints (POST /api/crm)   │
                │ └─ Live UI: http://127.0.0.1:5050   │
                └─────────────────────────────────────┘
```

---

## Project Structure

```
reverse-etl-crm-pipeline/
├── config/
│   └── config.yaml                     # All configs: thresholds, paths, CRM URL
├── dags/
│   └── reverse_etl_airflow_dag.py      # Apache Airflow DAG definition
├── data/
│   └── customer_data.csv               # 30 customer records
├── logs/                               # Pipeline run logs
├── mock_crm_server/
│   └── app.py                          # Flask CRM API server & web dashboard
├── orchestration/
│   ├── __init__.py
│   └── prefect_flow.py                 # Prefect 2.x/3.x DAG Flow and Tasks
├── src/
│   ├── config_loader.py                # YAML config reader
│   ├── crm_client.py                   # HTTP CRM client (retry + bulk)
│   ├── extractor.py                    # CSV extraction + validation
│   ├── loader.py                       # SQLite warehouse (star schema)
│   ├── logger.py                       # Centralized logging
│   ├── reverse_etl.py                  # True Reverse ETL step
│   └── transformer.py                  # RFM scoring + segmentation
├── tests/
│   ├── test_airflow_dag.py             # Airflow DAG logic unit tests
│   ├── test_crm_client.py              # CRM client tests (mocked)
│   ├── test_extractor.py               # Extractor tests
│   ├── test_prefect_flow.py            # Prefect tasks & flow integration tests
│   └── test_transformer.py             # RFM + segmentation tests
├── pipeline.py                         # Standalone orchestrator CLI
├── requirements.txt
└── README.md
```

---

## Workflow Orchestration Setup & Execution

### 1. Prefect Orchestration (Native on all OS)

Run the Prefect flow directly:
```bash
python orchestration/prefect_flow.py
```

Serve the flow on a scheduled interval (e.g., every 10 minutes):
```bash
python orchestration/prefect_flow.py --serve
```

**Prefect Features Included:**
- `@task` wrappers for each pipeline stage with retry policies (`retries=3, retry_delay_seconds=3`).
- `@flow` dependency coordinator with structured execution logging.
- Native tag-based filtering (`tags=['reverse-etl', 'warehouse']`).
- Support for Prefect Cloud / self-hosted Prefect server deployments.

---

### 2. Apache Airflow DAG

The Airflow DAG is located at:
📁 [`dags/reverse_etl_airflow_dag.py`](file:///C:/Users/Padmaja%20Kachare/.gemini/antigravity-ide/scratch/reverse-etl-crm-pipeline/dags/reverse_etl_airflow_dag.py)

**DAG Specifications:**
- **DAG ID:** `reverse_etl_customer_segmentation_dag`
- **Schedule Interval:** `0 * * * *` (Hourly)
- **Catchup:** `False`
- **Task Dependencies:**
  ```
  extract_and_validate >> transform_and_segment >> load_warehouse >> reverse_etl_crm_push >> notify_and_audit
  ```
- **XCom Integration:** Passes execution run IDs and records counts between tasks.

---

## Quick Start (End-to-End)

### 1. Start the Mock CRM Server
```bash
python mock_crm_server/app.py
```
View the dashboard at [http://127.0.0.1:5050/](http://127.0.0.1:5050/).

### 2. Run the Orchestrated Flow
```bash
python orchestration/prefect_flow.py
```

### 3. Run All Test Suites
```bash
pytest tests/ -v
```

---

## Customer Segments (RFM-based)

| Segment   | RFM Score | Marketing Action         |
|-----------|-----------|--------------------------|
| VIP       | ≥ 8       | Send premium offer       |
| Loyal     | ≥ 6       | Send loyalty reward      |
| Regular   | ≥ 4       | Send personalized promo  |
| At Risk   | < 4       | Send win-back campaign   |

---

## Key Improvements Over Basic Approach

| Feature | Old Repo | This Repo |
|---------|----------|-----------|
| **Workflow Orchestration** | None | **Prefect Flow + Apache Airflow DAG** |
| **Scheduled DAGs** | None | Automated hourly / interval triggers |
| **Reverse ETL target** | CSV file | REST API (HTTP POST) with backoff |
| **Warehouse schema** | Flat table | Star schema (`dim_customers` + `fact_rfm_scores`) |
| **ETL run tracking** | None | `etl_run_log` table |
| **Testing** | 0 tests | 50+ pytest unit & integration tests |
