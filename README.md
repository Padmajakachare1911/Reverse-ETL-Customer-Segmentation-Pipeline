# Reverse ETL Customer Segmentation Pipeline

A **production-grade** Reverse ETL pipeline that:
1. Extracts raw customer data from a CSV source
2. Cleans, scores (RFM), and segments customers
3. Loads the enriched data into a **star-schema SQLite data warehouse**
4. **Pushes segmented data to a CRM system via REST API** — the true Reverse ETL step

---

## Architecture

```
┌─────────────────┐
│  customer_data  │
│     .csv        │  ← Source System
└────────┬────────┘
         │  EXTRACT
         ▼
┌─────────────────┐
│   extractor.py  │  Validates schema, coerces types
└────────┬────────┘
         │  TRANSFORM
         ▼
┌─────────────────┐
│ transformer.py  │  Clean → RFM Score → Segment
└────────┬────────┘
         │  LOAD
         ▼
┌──────────────────────────────┐
│   SQLite Data Warehouse      │
│  ┌──────────────────────┐    │
│  │   dim_customers      │    │
│  │   fact_rfm_scores    │    │  ← Analytical Layer
│  │   etl_run_log        │    │
│  └──────────────────────┘    │
└────────┬─────────────────────┘
         │  REVERSE ETL (Read from warehouse)
         ▼
┌─────────────────┐
│  reverse_etl.py │  Reads warehouse → builds CRM payload
└────────┬────────┘
         │  HTTP POST (REST API)
         ▼
┌──────────────────────────────┐
│   Mock CRM API Server        │
│   (Flask - port 5050)        │  ← Operational System (CRM)
│                              │
│  POST /api/crm/customers/bulk│
│  GET  /api/crm/dashboard     │
│  GET  /                      │  ← Live Dashboard UI
└──────────────────────────────┘
```

---

## Project Structure

```
reverse-etl-crm-pipeline/
├── config/
│   └── config.yaml          # All configs: thresholds, paths, CRM URL
├── data/
│   └── customer_data.csv    # 30 customer records
├── logs/                    # Pipeline run logs
├── mock_crm_server/
│   └── app.py               # Flask CRM API server
├── src/
│   ├── config_loader.py     # YAML config reader
│   ├── crm_client.py        # HTTP CRM client (retry + bulk)
│   ├── extractor.py         # CSV extraction + validation
│   ├── loader.py            # SQLite warehouse (star schema)
│   ├── logger.py            # Centralized logging
│   ├── reverse_etl.py       # True Reverse ETL step
│   └── transformer.py       # RFM scoring + segmentation
├── tests/
│   ├── test_crm_client.py   # CRM client tests (mocked)
│   ├── test_extractor.py    # Extractor tests
│   └── test_transformer.py  # RFM + segmentation tests
├── pipeline.py              # Main orchestrator
└── requirements.txt
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

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start the Mock CRM API Server

Open a terminal and run:

```bash
python mock_crm_server/app.py
```

The CRM server starts at `http://127.0.0.1:5050`.
Open the browser at that URL to see the live dashboard.

### 3. Run the Pipeline

Open another terminal and run:

```bash
python pipeline.py
```

### 4. Run Tests

```bash
pytest tests/ -v
```

---

## CLI Options

```bash
# Run once (default)
python pipeline.py

# Run on a schedule (interval from config.yaml)
python pipeline.py --schedule

# Push customers individually instead of bulk
python pipeline.py --individual
```

---

## CRM API Endpoints

| Method | Endpoint                    | Description                  |
|--------|-----------------------------|------------------------------|
| GET    | `/`                         | Live HTML dashboard          |
| GET    | `/api/health`               | Health check                 |
| POST   | `/api/crm/customers`        | Push single customer         |
| POST   | `/api/crm/customers/bulk`   | Push bulk customers          |
| GET    | `/api/crm/customers`        | List all CRM customers       |
| GET    | `/api/crm/customers/<id>`   | Get single customer          |
| GET    | `/api/crm/dashboard`        | Segment summary (JSON)       |

---

## Warehouse Schema (Star Schema)

```
dim_customers          fact_rfm_scores         etl_run_log
─────────────          ───────────────         ───────────
customer_id (PK)       id (PK, AUTO)           run_id (PK, AUTO)
name                   customer_id (FK)        started_at
age                    total_spend             completed_at
email                  purchase_frequency      status
region                 days_since_last_purchase records_loaded
created_at             recency_score           error_message
updated_at             frequency_score
                       monetary_score
                       rfm_score
                       segment
                       run_id
                       loaded_at
```

---

## Key Improvements Over Basic Approach

| Feature | Old Repo | This Repo |
|---------|----------|-----------|
| Reverse ETL target | CSV file | REST API (HTTP POST) |
| Warehouse schema | Flat table | Star schema (dim + fact) |
| ETL run tracking | None | `etl_run_log` table |
| Error handling | None | `try/except` everywhere |
| Config management | Hardcoded | `config.yaml` |
| Logging | `print()` | Python `logging` module |
| CRM integration | None | Flask API + SQLite persistence |
| Retry logic | None | Exponential backoff |
| Unit tests | None | pytest (3 test files, 25+ cases) |
| Data volume | 12 records | 30 records |
| Scheduling | None | `--schedule` CLI flag |
