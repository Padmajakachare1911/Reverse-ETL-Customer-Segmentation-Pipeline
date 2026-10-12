"""
tests/test_extractor.py
------------------------
Unit tests for the CSV extractor module.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.extractor import extract


@pytest.fixture
def valid_csv(tmp_path):
    csv = tmp_path / "customers.csv"
    csv.write_text(
        "customer_id,name,age,email,total_spend,purchase_frequency,days_since_last_purchase,region\n"
        "C001,Alice,28,alice@email.com,125000,18,5,North\n"
        "C002,Bob,35,bob@email.com,8000,1,180,South\n"
    )
    return csv


@pytest.fixture
def missing_column_csv(tmp_path):
    csv = tmp_path / "bad.csv"
    csv.write_text("customer_id,name\nC001,Alice\n")
    return csv


class TestExtractor:
    def test_valid_csv_returns_dataframe(self, valid_csv):
        df = extract(valid_csv)
        assert df is not None
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_correct_column_count(self, valid_csv):
        df = extract(valid_csv)
        assert "customer_id" in df.columns
        assert "email" in df.columns
        assert "total_spend" in df.columns

    def test_missing_file_returns_none(self, tmp_path):
        result = extract(tmp_path / "nonexistent.csv")
        assert result is None

    def test_missing_columns_returns_none(self, missing_column_csv):
        result = extract(missing_column_csv)
        assert result is None

    def test_numeric_columns_coerced(self, valid_csv):
        df = extract(valid_csv)
        assert pd.api.types.is_numeric_dtype(df["total_spend"])
        assert pd.api.types.is_numeric_dtype(df["purchase_frequency"])
