"""
src/extractor.py
----------------
STEP 1 — EXTRACT
Reads and validates the source customer CSV file.
"""

import pandas as pd
from pathlib import Path
from typing import Optional
from src.logger import get_logger

logger = get_logger(__name__)

REQUIRED_COLUMNS = {
    "customer_id", "name", "age", "email",
    "total_spend", "purchase_frequency",
    "days_since_last_purchase", "region"
}

COLUMN_DTYPES = {
    "total_spend": float,
    "purchase_frequency": int,
    "days_since_last_purchase": int,
    "age": int,
}


def extract(csv_path: str | Path) -> Optional[pd.DataFrame]:
    """
    Extract customer data from a CSV file with validation.

    Args:
        csv_path: Path to the input CSV file.

    Returns:
        Validated DataFrame or None on failure.
    """
    csv_path = Path(csv_path)
    logger.info(f"[EXTRACT] Reading CSV: {csv_path}")

    try:
        if not csv_path.exists():
            raise FileNotFoundError(f"Input file not found: {csv_path}")

        df = pd.read_csv(csv_path)
        logger.info(f"[EXTRACT] Raw records loaded: {len(df)}")

        # Column presence check
        missing_cols = REQUIRED_COLUMNS - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Type coercion
        for col, dtype in COLUMN_DTYPES.items():
            df[col] = pd.to_numeric(df[col], errors="coerce")

        logger.info(f"[EXTRACT] Schema validation passed. Columns: {list(df.columns)}")
        return df

    except FileNotFoundError as e:
        logger.error(f"[EXTRACT] File error: {e}")
    except ValueError as e:
        logger.error(f"[EXTRACT] Validation error: {e}")
    except Exception as e:
        logger.error(f"[EXTRACT] Unexpected error: {e}", exc_info=True)

    return None
