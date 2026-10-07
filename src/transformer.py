"""
src/transformer.py
------------------
STEP 2 — TRANSFORM
Data cleaning + RFM-style scoring + customer segmentation.
All thresholds are driven from config.yaml (no hardcoding).
"""

import pandas as pd
from typing import Optional
from src.logger import get_logger

logger = get_logger(__name__)


# ── Scoring helpers ──────────────────────────────────────────────────────────

def compute_recency_score(days: float, high: int, medium: int) -> int:
    """Score recency: lower days = better."""
    if days <= high:
        return 3
    elif days <= medium:
        return 2
    return 1


def compute_frequency_score(freq: float, high: int, medium: int) -> int:
    """Score purchase frequency: more = better."""
    if freq >= high:
        return 3
    elif freq >= medium:
        return 2
    return 1


def compute_monetary_score(spend: float, high: int, medium: int) -> int:
    """Score total spend: higher = better."""
    if spend >= high:
        return 3
    elif spend >= medium:
        return 2
    return 1


def assign_segment(score: int, thresholds: dict) -> str:
    """Map RFM total score to a customer segment label."""
    if score >= thresholds["vip"]:
        return "VIP"
    elif score >= thresholds["loyal"]:
        return "Loyal"
    elif score >= thresholds["regular"]:
        return "Regular"
    return "At Risk"


# ── Main transform function ──────────────────────────────────────────────────

def transform(df: pd.DataFrame, rfm_config: dict, seg_config: dict) -> Optional[pd.DataFrame]:
    """
    Clean, score, and segment customer data.

    Args:
        df: Raw extracted DataFrame.
        rfm_config: RFM threshold configuration dict.
        seg_config: Segmentation threshold dict.

    Returns:
        Enriched DataFrame with RFM scores and segments, or None on failure.
    """
    try:
        logger.info(f"[TRANSFORM] Starting transformation on {len(df)} records.")

        # ── 1. Data Cleaning
        before = len(df)
        df = df.drop_duplicates(subset=["customer_id"])
        df = df.dropna(subset=["customer_id", "name", "email",
                                "total_spend", "purchase_frequency",
                                "days_since_last_purchase"])

        # Sanity filters: remove negative values
        df = df[df["total_spend"] >= 0]
        df = df[df["purchase_frequency"] >= 0]
        df = df[df["days_since_last_purchase"] >= 0]

        after = len(df)
        logger.info(f"[TRANSFORM] Cleaned: {before - after} records dropped. {after} remaining.")

        # ── 2. RFM Scoring
        r_cfg = rfm_config["recency"]
        f_cfg = rfm_config["frequency"]
        m_cfg = rfm_config["monetary"]

        df["recency_score"] = df["days_since_last_purchase"].apply(
            lambda d: compute_recency_score(d, r_cfg["high_threshold"], r_cfg["medium_threshold"])
        )
        df["frequency_score"] = df["purchase_frequency"].apply(
            lambda f: compute_frequency_score(f, f_cfg["high_threshold"], f_cfg["medium_threshold"])
        )
        df["monetary_score"] = df["total_spend"].apply(
            lambda m: compute_monetary_score(m, m_cfg["high_threshold"], m_cfg["medium_threshold"])
        )

        df["rfm_score"] = df["recency_score"] + df["frequency_score"] + df["monetary_score"]

        # ── 3. Segmentation
        thresholds = seg_config["thresholds"]
        df["segment"] = df["rfm_score"].apply(lambda s: assign_segment(s, thresholds))

        seg_counts = df["segment"].value_counts().to_dict()
        logger.info(f"[TRANSFORM] Segments assigned: {seg_counts}")

        return df

    except KeyError as e:
        logger.error(f"[TRANSFORM] Missing config key: {e}")
    except Exception as e:
        logger.error(f"[TRANSFORM] Unexpected error: {e}", exc_info=True)

    return None
