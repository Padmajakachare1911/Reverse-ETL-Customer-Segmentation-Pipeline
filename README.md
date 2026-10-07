# Reverse ETL Customer Segmentation Pipeline

A **production-grade** Reverse ETL pipeline with modern **Workflow Orchestration (Prefect & Apache Airflow)**:
1. Extracts raw customer data from a CSV source
2. Cleans, scores (RFM), and segments customers
3. Loads the enriched data into a **star-schema SQLite data warehouse**
4. **Pushes segmented data to a CRM system via REST API** — the true Reverse ETL step
5. **Scheduled DAG Orchestration** with retries, failure alerting, and dependency chaining using **Prefect** & **Apache Airflow**

---

## 🏛️ Architecture & Workflow Orchestration

```
                            SCHEDULED DAG PIPELINE
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  [1. task_extract_and_validate]                                             │
│      └─ Ingests customer_data.csv & enforces schema + type checks           │
│         │                                                                   │
│         ▼                                                                   │
│  [2. task_transform_and_segment]                                            │
│      └─ Computes RFM scores & segments (VIP, Loyal, Regular, At Risk)       │
│         │                                                                   │
│         ▼                                                                   │
│  [3. task_load_warehouse]                                                   │
│      └─ Upserts dim_customers, appends fact_rfm_scores, logs etl_run_log    │
│         │                                                                   │
│         ▼                                                                   │
│  [4. task_reverse_etl_push]                                                 │
│      └─ Reads analytical warehouse & pushes to CRM REST API (with retries)  │
│         │                                                                   │
│         ▼                                                                   │
│  [5. task_generate_summary]                                                 │
│      └─ Audits execution results, records counts, and push statistics       │
│                                                                             │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
                    ┌─────────────────────────────────────┐
                    │ Mock CRM System (Flask - Port 5050) │
                    │ ├─ Live UI: http://127.0.0.1:5050   │
                    │ ├─ Endpoints: /api/crm/customers    │
                    │ └─ Persistence: SQLite (crm_store)  │
                    └─────────────────────────────────────┘
```

---

## 📦 Project Structure

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

## 🧩 Orchestration Modules

| Component | File | Purpose |
|-----------|------|---------|
| **Prefect Flow & Tasks** | `orchestration/prefect_flow.py` | Prefect DAG with automatic retries (`retries=3`), exponential backoff, structured state logging, and `--serve` scheduled execution. |
| **Apache Airflow DAG** | `dags/reverse_etl_airflow_dag.py` | Airflow DAG (`reverse_etl_customer_segmentation_dag`) configured with hourly schedule (`0 * * * *`), sequential `>>` task chaining, XCom metadata passing, and retry policies. |
| **Prefect Test Suite** | `tests/test_prefect_flow.py` | Unit tests for each individual Prefect task plus end-to-end flow integration test. |
| **Airflow Test Suite** | `tests/test_airflow_dag.py` | Unit tests verifying Airflow task callables, default arguments, and task execution logic. |

---

## 🧪 Test Suite & Validation (57 / 57 Tests Passed)

```bash
pytest tests/ -v
```

```
collected 57 items

tests/test_airflow_dag.py::TestAirflowDAGFunctions::test_default_args PASSED
tests/test_airflow_dag.py::TestAirflowDAGFunctions::test_run_extract PASSED
tests/test_airflow_dag.py::TestAirflowDAGFunctions::test_run_transform PASSED
tests/test_airflow_dag.py::TestAirflowDAGFunctions::test_run_load_warehouse PASSED
tests/test_airflow_dag.py::TestAirflowDAGFunctions::test_run_reverse_etl_push_mocked PASSED
tests/test_airflow_dag.py::TestAirflowDAGFunctions::test_run_notify_and_audit PASSED
tests/test_crm_client.py (7 tests) PASSED
tests/test_extractor.py (5 tests) PASSED
tests/test_prefect_flow.py (7 tests) PASSED
tests/test_transformer.py (32 tests) PASSED

============================= 57 passed in 30s =============================
```

---

## ⚡ Live Orchestration Execution

```bash
python orchestration/prefect_flow.py
```

**Task Lifecycle Output:**
1. `Task 'Extract and Validate Customer Data'`: 30 records ingested → **Completed**
2. `Task 'Transform and Segment Customers'`: 4 segments calculated (VIP, Loyal, Regular, At Risk) → **Completed**
3. `Task 'Load into Analytics Warehouse'`: Star-schema SQLite tables updated → **Completed**
4. `Task 'Reverse ETL - Push to CRM REST API'`: 30 records synced via HTTP POST → **Completed**
5. `Task 'Generate Pipeline Run Summary'`: Final run audit logged → **Completed**
6. **Flow Status:** **`Completed()`**

---

## 🚀 How to Run

### 1. Start the Mock CRM Server
```bash
python mock_crm_server/app.py
```
Open [http://127.0.0.1:5050/](http://127.0.0.1:5050/) to view the live CRM dashboard.

### 2. Run the Workflow Orchestration

**Using Prefect (Immediate single run):**
```bash
python orchestration/prefect_flow.py
```

**Using Prefect (Scheduled background service, every 10 min):**
```bash
python orchestration/prefect_flow.py --serve
```

**Using Airflow:**
Place `dags/reverse_etl_airflow_dag.py` into your Airflow `$AIRFLOW_HOME/dags` folder. The DAG will be scheduled to run hourly (`0 * * * *`).

### 3. Run Standalone CLI Orchestrator
```bash
python pipeline.py
```

### 4. Run Pytest Suite
```bash
pytest tests/ -v
```

---

## 🎯 Customer Segments (RFM-based)

| Segment   | RFM Score | Marketing Action         |
|-----------|-----------|--------------------------|
| VIP       | ≥ 8       | Send premium offer       |
| Loyal     | ≥ 6       | Send loyalty reward      |
| Regular   | ≥ 4       | Send personalized promo  |
| At Risk   | < 4       | Send win-back campaign   |

---

## 📊 Key Improvements Over Basic Approach

| Feature | Old Repo | This Repo |
|---------|----------|-----------|
| **Workflow Orchestration** | None | **Prefect Flow + Apache Airflow DAG** |
| **Scheduled DAGs** | None | Automated hourly / interval triggers |
| **Reverse ETL target** | CSV file | REST API (HTTP POST) with backoff |
| **Warehouse schema** | Flat table | Star schema (`dim_customers` + `fact_rfm_scores`) |
| **ETL run tracking** | None | `etl_run_log` audit table |
| **Testing** | 0 tests | 57 pytest unit & integration tests (100% passing) |
