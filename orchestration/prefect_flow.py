"""
orchestration/prefect_flow.py
-----------------------------
Prefect 2.x/3.x Workflow Orchestration for Reverse ETL Pipeline.

Flow: reverse_etl_customer_segmentation_flow
Tasks:
  1. task_extract_and_validate   - Extracts CSV & validates schema
  2. task_transform_and_segment  - Cleans, RFM scores, and segments
  3. task_load_warehouse         - Loads star-schema to SQLite warehouse
  4. task_reverse_etl_push       - Reads warehouse and pushes to CRM REST API
  5. task_generate_summary       - Aggregates and logs final execution metrics

Usage:
  # Direct run:
  python orchestration/prefect_flow.py

  # Serve as scheduled deployment (e.g. runs every 10 minutes):
  python orchestration/prefect_flow.py --serve
"""

import sys
from pathlib import Path
from datetime import timedelta
from typing import Dict, Any

# Ensure project root is in path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from prefect import flow, task
from prefect.logging import get_run_logger

from src.config_loader import load_config
from src.extractor import extract
from src.transformer import transform
from src.loader import load, read_from_warehouse
from src.reverse_etl import run_reverse_etl


def _get_logger():
    try:
        return get_run_logger()
    except Exception:
        import logging
        return logging.getLogger("prefect_flow")


@task(
    name="Extract and Validate Customer Data",
    description="Reads raw CSV and validates columns, types, and schema integrity.",
    retries=2,
    retry_delay_seconds=5,
    tags=["extract", "csv"]
)
def task_extract_and_validate(csv_path: Path):
    logger = _get_logger()
    logger.info(f"Starting extraction from: {csv_path}")
    
    df = extract(csv_path)
    if df is None or df.empty:
        raise ValueError(f"Extraction failed: {csv_path} returned empty or invalid data.")
    
    logger.info(f"Extracted {len(df)} records successfully.")
    return df


@task(
    name="Transform and Segment Customers",
    description="Cleans data, applies RFM scoring formulas, and assigns segments.",
    tags=["transform", "rfm-segmentation"]
)
def task_transform_and_segment(df, rfm_config: Dict[str, Any], seg_config: Dict[str, Any]):
    logger = _get_logger()
    logger.info("Starting data transformation & RFM customer segmentation...")
    
    transformed_df = transform(df, rfm_config=rfm_config, seg_config=seg_config)
    if transformed_df is None or transformed_df.empty:
        raise ValueError("Transformation failed: resulting DataFrame is empty.")
    
    counts = transformed_df["segment"].value_counts().to_dict()
    logger.info(f"Segmentation results: {counts}")
    return transformed_df


@task(
    name="Load into Analytics Warehouse",
    description="Loads dim_customers, fact_rfm_scores, and records etl_run_log.",
    tags=["warehouse", "sqlite"]
)
def task_load_warehouse(df, db_path: Path, table_config: Dict[str, Any]) -> int:
    logger = _get_logger()
    logger.info(f"Loading {len(df)} customer records into SQLite warehouse: {db_path}")
    
    run_id = load(df, db_path=db_path, table_config=table_config)
    if run_id is None:
        raise RuntimeError(f"Warehouse load failed for {db_path}")
    
    logger.info(f"Successfully loaded data to warehouse. Generated run_id={run_id}")
    return run_id


@task(
    name="Reverse ETL - Push to CRM REST API",
    description="Queries analytical model from warehouse and dispatches HTTP payloads to CRM.",
    retries=3,
    retry_delay_seconds=3,
    tags=["reverse-etl", "crm-sync", "rest-api"]
)
def task_reverse_etl_push(
    db_path: Path,
    run_id: int,
    crm_config: Dict[str, Any],
    marketing_actions: Dict[str, str],
    use_bulk: bool = True
) -> Dict[str, Any]:
    logger = _get_logger()
    logger.info(f"Reading run_id={run_id} records from warehouse for CRM sync...")
    
    warehouse_df = read_from_warehouse(db_path, run_id)
    if warehouse_df is None or warehouse_df.empty:
        raise RuntimeError(f"No records found in warehouse for run_id={run_id}")
    
    logger.info(f"Pushing {len(warehouse_df)} customer segments to CRM via REST API...")
    summary = run_reverse_etl(
        warehouse_df=warehouse_df,
        crm_config=crm_config,
        marketing_actions=marketing_actions,
        use_bulk=use_bulk
    )
    
    if summary.get("aborted"):
        raise ConnectionError("CRM push aborted: Mock CRM API server unreachable.")
    
    logger.info(f"Reverse ETL push completed: {summary}")
    return summary


@task(
    name="Generate Pipeline Run Summary",
    description="Logs consolidated performance metrics and segment distributions.",
    tags=["reporting", "summary"]
)
def task_generate_summary(run_id: int, records_count: int, push_summary: Dict[str, Any]):
    logger = _get_logger()
    logger.info("=" * 60)
    logger.info("ORCHESTRATION PIPELINE RUN SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Warehouse Run ID : {run_id}")
    logger.info(f"  Records Synced   : {records_count}")
    logger.info(f"  CRM Push Status  : Success={push_summary.get('success_count', 0)}, Failed={push_summary.get('failed_count', 0)}")
    logger.info("=" * 60)
    return {
        "run_id": run_id,
        "records_count": records_count,
        "crm_summary": push_summary,
        "status": "COMPLETED"
    }


@flow(
    name="reverse-etl-customer-segmentation-flow",
    description="Scheduled Prefect DAG that extracts, transforms, loads to warehouse, and syncs to CRM via Reverse ETL.",
    log_prints=True
)
def reverse_etl_flow(use_bulk: bool = True) -> Dict[str, Any]:
    """
    Main Prefect Flow orchestrating the entire Reverse ETL Lifecycle.
    """
    config = load_config()
    paths = config["paths"]
    csv_path = ROOT_DIR / paths["input_csv"]
    db_path = ROOT_DIR / paths["database"]

    # 1. Extract
    raw_df = task_extract_and_validate(csv_path)

    # 2. Transform
    transformed_df = task_transform_and_segment(
        raw_df,
        rfm_config=config["rfm"],
        seg_config=config["segmentation"]
    )

    # 3. Load Warehouse
    run_id = task_load_warehouse(
        transformed_df,
        db_path=db_path,
        table_config=config["warehouse"]["tables"]
    )

    # 4. Reverse ETL Sync
    push_summary = task_reverse_etl_push(
        db_path=db_path,
        run_id=run_id,
        crm_config=config["crm"],
        marketing_actions=config["marketing_actions"],
        use_bulk=use_bulk
    )

    # 5. Summary & Audit
    result = task_generate_summary(
        run_id=run_id,
        records_count=len(transformed_df),
        push_summary=push_summary
    )

    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Prefect Reverse ETL Flow Runner")
    parser.add_argument("--serve", action="store_true", help="Serve flow on schedule (every 10 minutes)")
    args = parser.parse_args()

    if args.serve:
        print("Serving reverse-etl-flow on a recurring 10-minute schedule...")
        reverse_etl_flow.serve(
            name="scheduled-reverse-etl-deployment",
            interval=timedelta(minutes=10)
        )
    else:
        print("Executing single run of reverse_etl_flow...")
        res = reverse_etl_flow()
        print("Flow execution finished successfully:", res)
