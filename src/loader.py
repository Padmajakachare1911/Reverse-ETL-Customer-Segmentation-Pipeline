"""
src/loader.py
-------------
STEP 3 — LOAD INTO DATA WAREHOUSE
Creates a proper star-schema SQLite warehouse and loads data.

Tables:
  dim_customers  — Customer master dimension table
  fact_rfm_scores — RFM scores and segments (fact table)
  etl_run_log    — Tracks each pipeline run for auditability
"""

import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional
from src.logger import get_logger

logger = get_logger(__name__)


DDL_DIM_CUSTOMERS = """
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id     TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    age             INTEGER,
    email           TEXT,
    region          TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
"""

DDL_FACT_RFM = """
CREATE TABLE IF NOT EXISTS fact_rfm_scores (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id                 TEXT NOT NULL,
    total_spend                 REAL,
    purchase_frequency          INTEGER,
    days_since_last_purchase    INTEGER,
    recency_score               INTEGER,
    frequency_score             INTEGER,
    monetary_score              INTEGER,
    rfm_score                   INTEGER,
    segment                     TEXT,
    run_id                      INTEGER,
    loaded_at                   TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
);
"""

DDL_ETL_RUN_LOG = """
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT,
    completed_at    TEXT,
    status          TEXT,
    records_loaded  INTEGER,
    error_message   TEXT
);
"""


def _get_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _create_schema(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(DDL_DIM_CUSTOMERS)
    cursor.execute(DDL_FACT_RFM)
    cursor.execute(DDL_ETL_RUN_LOG)
    conn.commit()
    logger.info("[LOAD] Warehouse schema ensured (dim_customers, fact_rfm_scores, etl_run_log).")


def _start_run(conn: sqlite3.Connection) -> int:
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO etl_run_log (started_at, status) VALUES (?, ?)",
        (datetime.utcnow().isoformat(), "RUNNING")
    )
    conn.commit()
    run_id = cursor.lastrowid
    logger.info(f"[LOAD] ETL run started. run_id={run_id}")
    return run_id


def _finish_run(conn: sqlite3.Connection, run_id: int, records: int, status: str = "SUCCESS", error: str = None) -> None:
    conn.execute(
        """UPDATE etl_run_log
           SET completed_at=?, status=?, records_loaded=?, error_message=?
           WHERE run_id=?""",
        (datetime.utcnow().isoformat(), status, records, error, run_id)
    )
    conn.commit()
    logger.info(f"[LOAD] ETL run finished. run_id={run_id} | status={status} | records={records}")


def load(df: pd.DataFrame, db_path: str | Path, table_config: dict) -> Optional[int]:
    """
    Load transformed DataFrame into the SQLite warehouse.

    Args:
        df: Transformed DataFrame with RFM scores and segments.
        db_path: Path to SQLite database file.
        table_config: Warehouse table name config.

    Returns:
        run_id of this ETL run, or None on failure.
    """
    db_path = Path(db_path)
    conn = None
    run_id = None

    try:
        conn = _get_connection(db_path)
        _create_schema(conn)
        run_id = _start_run(conn)

        # ── Upsert dim_customers
        dim_cols = ["customer_id", "name", "age", "email", "region"]
        dim_df = df[dim_cols].copy()
        dim_df["updated_at"] = datetime.utcnow().isoformat()

        for _, row in dim_df.iterrows():
            conn.execute(
                """INSERT INTO dim_customers (customer_id, name, age, email, region, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(customer_id) DO UPDATE SET
                       name=excluded.name,
                       age=excluded.age,
                       email=excluded.email,
                       region=excluded.region,
                       updated_at=excluded.updated_at""",
                (row["customer_id"], row["name"], int(row["age"]),
                 row["email"], row["region"], row["updated_at"])
            )

        logger.info(f"[LOAD] dim_customers upserted: {len(dim_df)} rows.")

        # ── Insert fact_rfm_scores
        fact_cols = [
            "customer_id", "total_spend", "purchase_frequency",
            "days_since_last_purchase", "recency_score", "frequency_score",
            "monetary_score", "rfm_score", "segment"
        ]
        fact_df = df[fact_cols].copy()
        fact_df["run_id"] = run_id
        fact_df["loaded_at"] = datetime.utcnow().isoformat()

        fact_df.to_sql(
            table_config["fact_rfm"],
            conn,
            if_exists="append",
            index=False
        )
        logger.info(f"[LOAD] fact_rfm_scores inserted: {len(fact_df)} rows.")

        conn.commit()
        _finish_run(conn, run_id, len(df))
        return run_id

    except Exception as e:
        logger.error(f"[LOAD] Error during warehouse load: {e}", exc_info=True)
        if conn and run_id:
            _finish_run(conn, run_id, 0, "FAILED", str(e))
        return None

    finally:
        if conn:
            conn.close()


def read_from_warehouse(db_path: str | Path, run_id: int) -> Optional[pd.DataFrame]:
    """
    Read the latest segmented data from the warehouse for a specific run.

    Args:
        db_path: Path to SQLite database.
        run_id: The ETL run ID to fetch data for.

    Returns:
        DataFrame with customer segments from the warehouse.
    """
    db_path = Path(db_path)
    try:
        conn = sqlite3.connect(str(db_path))
        query = """
            SELECT
                d.customer_id,
                d.name,
                d.email,
                d.region,
                f.total_spend,
                f.purchase_frequency,
                f.days_since_last_purchase,
                f.rfm_score,
                f.segment,
                f.run_id,
                f.loaded_at
            FROM fact_rfm_scores f
            JOIN dim_customers d ON f.customer_id = d.customer_id
            WHERE f.run_id = ?
            ORDER BY f.rfm_score DESC
        """
        df = pd.read_sql_query(query, conn, params=(run_id,))
        conn.close()
        logger.info(f"[LOAD] Read {len(df)} records from warehouse for run_id={run_id}.")
        return df

    except Exception as e:
        logger.error(f"[LOAD] Error reading from warehouse: {e}", exc_info=True)
        return None
