"""
src/reverse_etl.py
------------------
STEP 4 — REVERSE ETL
Reads enriched, segmented data from the data warehouse and
PUSHES it to the CRM system via REST API.

This is the true Reverse ETL step:
  Warehouse → API Call → CRM/Operational System
"""

import pandas as pd
from typing import Optional
from src.crm_client import CRMClient
from src.logger import get_logger

logger = get_logger(__name__)


def _build_crm_payload(row: pd.Series, marketing_actions: dict) -> dict:
    """
    Build a CRM-ready payload dict from a warehouse row.

    Args:
        row: A single DataFrame row (warehouse record).
        marketing_actions: Segment → action mapping.

    Returns:
        Dict formatted for the CRM API.
    """
    segment = row["segment"]
    return {
        "customer_id": row["customer_id"],
        "name": row["name"],
        "email": row["email"],
        "region": row["region"],
        "segment": segment,
        "rfm_score": int(row["rfm_score"]),
        "total_spend": float(row["total_spend"]),
        "purchase_frequency": int(row["purchase_frequency"]),
        "days_since_last_purchase": int(row["days_since_last_purchase"]),
        "recommended_action": marketing_actions.get(segment, "No action defined"),
        "warehouse_run_id": int(row["run_id"]),
        "synced_at": row["loaded_at"],
    }


def run_reverse_etl(
    warehouse_df: pd.DataFrame,
    crm_config: dict,
    marketing_actions: dict,
    use_bulk: bool = True,
) -> dict:
    """
    Core Reverse ETL function: reads warehouse data and pushes to CRM API.

    Args:
        warehouse_df: DataFrame read from the data warehouse.
        crm_config: CRM connection config (base_url, timeout, retries, etc.).
        marketing_actions: Segment to marketing action mapping.
        use_bulk: If True, use bulk push endpoint; else push one by one.

    Returns:
        Summary dict with push statistics.
    """
    logger.info(f"[REVERSE ETL] Starting push of {len(warehouse_df)} records to CRM.")

    client = CRMClient(
        base_url=crm_config["base_url"],
        timeout=crm_config.get("timeout_seconds", 10),
        retry_attempts=crm_config.get("retry_attempts", 3),
        retry_delay=crm_config.get("retry_delay_seconds", 2),
    )

    # Health check before pushing
    if not client.health_check():
        logger.error("[REVERSE ETL] CRM API is not reachable. Aborting push.")
        return {"success_count": 0, "failed_count": len(warehouse_df), "aborted": True}

    # Build payloads
    payloads = [
        _build_crm_payload(row, marketing_actions)
        for _, row in warehouse_df.iterrows()
    ]

    summary = {}

    if use_bulk:
        # ── Bulk push (single API call for all records)
        logger.info(f"[REVERSE ETL] Using bulk push endpoint.")
        summary = client.bulk_push(payloads)
    else:
        # ── Individual push (one API call per customer)
        logger.info(f"[REVERSE ETL] Using individual push (one call per customer).")
        success = 0
        failed = 0
        for payload in payloads:
            if client.push_customer(payload):
                success += 1
            else:
                failed += 1
        summary = {"success_count": success, "failed_count": failed}

    logger.info(
        f"[REVERSE ETL] Complete — "
        f"pushed={summary.get('success_count', 0)}, "
        f"failed={summary.get('failed_count', 0)}"
    )
    return summary
