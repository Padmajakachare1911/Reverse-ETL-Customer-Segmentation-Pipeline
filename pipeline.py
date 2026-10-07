"""
pipeline.py
-----------
Main Pipeline Orchestrator
---------------------------
Runs the full Reverse ETL pipeline end-to-end:

  [1] EXTRACT    → Read CSV source data
  [2] TRANSFORM  → Clean + RFM score + segment
  [3] LOAD       → Write to SQLite data warehouse (star schema)
  [4] REVERSE ETL → Read warehouse → Push to CRM API

Usage:
  python pipeline.py                  # Run once
  python pipeline.py --schedule       # Run on interval (from config)
  python pipeline.py --individual     # Push one-by-one (not bulk)
"""

import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config_loader import load_config
from src.logger import get_logger
from src.extractor import extract
from src.transformer import transform
from src.loader import load, read_from_warehouse
from src.reverse_etl import run_reverse_etl

logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent


def run_pipeline(config: dict, use_bulk: bool = True) -> bool:
    """
    Execute the full Reverse ETL pipeline.

    Args:
        config: Loaded configuration dictionary.
        use_bulk: If True, push customers in bulk; else one-by-one.

    Returns:
        True if pipeline completed successfully, False otherwise.
    """
    start_time = datetime.utcnow()
    logger.info("=" * 70)
    logger.info("  REVERSE ETL PIPELINE — CUSTOMER SEGMENTATION")
    logger.info(f"  Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info("=" * 70)

    paths = config["paths"]
    csv_path = BASE_DIR / paths["input_csv"]
    db_path  = BASE_DIR / paths["database"]

    # ─────────────────────────────────────────────
    # STEP 1: EXTRACT
    # ─────────────────────────────────────────────
    logger.info("\n[STEP 1/4] EXTRACT — Reading source CSV...")
    raw_df = extract(csv_path)

    if raw_df is None or raw_df.empty:
        logger.error("Extraction failed or returned empty data. Aborting.")
        return False

    logger.info(f"  [OK] Extracted {len(raw_df)} records from {csv_path.name}")

    # ─────────────────────────────────────────────
    # STEP 2: TRANSFORM
    # ─────────────────────────────────────────────
    logger.info("\n[STEP 2/4] TRANSFORM — Cleaning + RFM Scoring + Segmentation...")
    transformed_df = transform(
        raw_df,
        rfm_config=config["rfm"],
        seg_config=config["segmentation"],
    )

    if transformed_df is None or transformed_df.empty:
        logger.error("Transformation failed. Aborting.")
        return False

    seg_summary = transformed_df["segment"].value_counts().to_dict()
    logger.info(f"  [OK] Segmentation complete: {seg_summary}")

    # ─────────────────────────────────────────────
    # STEP 3: LOAD → DATA WAREHOUSE
    # ─────────────────────────────────────────────
    logger.info("\n[STEP 3/4] LOAD — Writing to SQLite Data Warehouse...")
    run_id = load(
        transformed_df,
        db_path=db_path,
        table_config=config["warehouse"]["tables"],
    )

    if run_id is None:
        logger.error("Warehouse load failed. Aborting.")
        return False

    logger.info(f"  [OK] Loaded to warehouse. run_id={run_id}")

    # Read back from warehouse (simulates downstream analytics reading)
    warehouse_df = read_from_warehouse(db_path, run_id)
    if warehouse_df is None or warehouse_df.empty:
        logger.error("Could not read data back from warehouse. Aborting.")
        return False

    logger.info(f"  [OK] Read {len(warehouse_df)} records from warehouse (run_id={run_id})")

    # ─────────────────────────────────────────────
    # STEP 4: REVERSE ETL → PUSH TO CRM API
    # ─────────────────────────────────────────────
    logger.info("\n[STEP 4/4] REVERSE ETL — Pushing to CRM via REST API...")
    summary = run_reverse_etl(
        warehouse_df=warehouse_df,
        crm_config=config["crm"],
        marketing_actions=config["marketing_actions"],
        use_bulk=use_bulk,
    )

    # ─────────────────────────────────────────────
    # PIPELINE SUMMARY
    # ─────────────────────────────────────────────
    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()

    logger.info("\n" + "=" * 70)
    logger.info("  PIPELINE SUMMARY")
    logger.info("=" * 70)
    logger.info(f"  Duration          : {duration:.2f}s")
    logger.info(f"  Records extracted : {len(raw_df)}")
    logger.info(f"  Records after ETL : {len(transformed_df)}")
    logger.info(f"  Warehouse run_id  : {run_id}")
    logger.info(f"  CRM pushed        : {summary.get('success_count', 0)}")
    logger.info(f"  CRM failed        : {summary.get('failed_count', 0)}")

    if summary.get("aborted"):
        logger.warning("  [WARN] CRM push was ABORTED (API unreachable).")
        logger.info("  [INFO] Start the CRM server: python mock_crm_server/app.py")

    logger.info(f"\n  Segment Breakdown:")
    for seg, count in seg_summary.items():
        logger.info(f"    {seg:<12}: {count} customers")

    logger.info("\n" + "=" * 70)
    logger.info("  REVERSE ETL PIPELINE COMPLETED")
    logger.info("=" * 70)

    return True


def main():
    parser = argparse.ArgumentParser(description="Reverse ETL Customer Segmentation Pipeline")
    parser.add_argument("--schedule", action="store_true",
                        help="Run pipeline repeatedly on a schedule")
    parser.add_argument("--individual", action="store_true",
                        help="Push customers individually (not in bulk)")
    args = parser.parse_args()

    config = load_config()
    use_bulk = not args.individual

    if args.schedule:
        interval = config["pipeline"].get("run_interval_seconds", 60)
        logger.info(f"Scheduled mode: running every {interval} seconds. Press Ctrl+C to stop.")
        try:
            while True:
                run_pipeline(config, use_bulk=use_bulk)
                logger.info(f"Next run in {interval}s...\n")
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Pipeline scheduler stopped by user.")
    else:
        success = run_pipeline(config, use_bulk=use_bulk)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
