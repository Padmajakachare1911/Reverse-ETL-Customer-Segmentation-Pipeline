"""
tests/test_transformer.py
--------------------------
Unit tests for the RFM scoring and segmentation logic.
Run with: pytest tests/ -v
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.transformer import (
    compute_recency_score,
    compute_frequency_score,
    compute_monetary_score,
    assign_segment,
    transform,
)

# ── Config fixtures ───────────────────────────────────────────────────────────

@pytest.fixture
def rfm_config():
    return {
        "recency":   {"high_threshold": 7,      "medium_threshold": 30},
        "frequency": {"high_threshold": 15,     "medium_threshold": 8},
        "monetary":  {"high_threshold": 100000, "medium_threshold": 50000},
    }

@pytest.fixture
def seg_config():
    return {
        "thresholds": {"vip": 8, "loyal": 6, "regular": 4}
    }

@pytest.fixture
def sample_df():
    return pd.DataFrame([
        {"customer_id": "C001", "name": "Alice", "age": 28, "email": "a@b.com", "region": "North",
         "total_spend": 125000, "purchase_frequency": 18, "days_since_last_purchase": 5},
        {"customer_id": "C002", "name": "Bob",   "age": 35, "email": "b@b.com", "region": "South",
         "total_spend": 8000,   "purchase_frequency": 1,  "days_since_last_purchase": 180},
        {"customer_id": "C003", "name": "Carol", "age": 27, "email": "c@b.com", "region": "East",
         "total_spend": 65000,  "purchase_frequency": 10, "days_since_last_purchase": 20},
    ])


# ── Recency score tests ───────────────────────────────────────────────────────

class TestRecencyScore:
    def test_high_recency(self):
        assert compute_recency_score(3, 7, 30) == 3

    def test_boundary_high(self):
        assert compute_recency_score(7, 7, 30) == 3

    def test_medium_recency(self):
        assert compute_recency_score(15, 7, 30) == 2

    def test_boundary_medium(self):
        assert compute_recency_score(30, 7, 30) == 2

    def test_low_recency(self):
        assert compute_recency_score(90, 7, 30) == 1

    def test_very_old(self):
        assert compute_recency_score(365, 7, 30) == 1


# ── Frequency score tests ─────────────────────────────────────────────────────

class TestFrequencyScore:
    def test_high_frequency(self):
        assert compute_frequency_score(20, 15, 8) == 3

    def test_boundary_high(self):
        assert compute_frequency_score(15, 15, 8) == 3

    def test_medium_frequency(self):
        assert compute_frequency_score(10, 15, 8) == 2

    def test_boundary_medium(self):
        assert compute_frequency_score(8, 15, 8) == 2

    def test_low_frequency(self):
        assert compute_frequency_score(2, 15, 8) == 1


# ── Monetary score tests ──────────────────────────────────────────────────────

class TestMonetaryScore:
    def test_high_spend(self):
        assert compute_monetary_score(150000, 100000, 50000) == 3

    def test_boundary_high(self):
        assert compute_monetary_score(100000, 100000, 50000) == 3

    def test_medium_spend(self):
        assert compute_monetary_score(75000, 100000, 50000) == 2

    def test_boundary_medium(self):
        assert compute_monetary_score(50000, 100000, 50000) == 2

    def test_low_spend(self):
        assert compute_monetary_score(10000, 100000, 50000) == 1


# ── Segment assignment tests ──────────────────────────────────────────────────

class TestAssignSegment:
    @pytest.fixture
    def thresholds(self):
        return {"vip": 8, "loyal": 6, "regular": 4}

    def test_vip(self, thresholds):
        assert assign_segment(9, thresholds) == "VIP"

    def test_vip_boundary(self, thresholds):
        assert assign_segment(8, thresholds) == "VIP"

    def test_loyal(self, thresholds):
        assert assign_segment(7, thresholds) == "Loyal"

    def test_loyal_boundary(self, thresholds):
        assert assign_segment(6, thresholds) == "Loyal"

    def test_regular(self, thresholds):
        assert assign_segment(5, thresholds) == "Regular"

    def test_regular_boundary(self, thresholds):
        assert assign_segment(4, thresholds) == "Regular"

    def test_at_risk(self, thresholds):
        assert assign_segment(3, thresholds) == "At Risk"

    def test_very_low(self, thresholds):
        assert assign_segment(1, thresholds) == "At Risk"


# ── Full transform function tests ─────────────────────────────────────────────

class TestTransform:
    def test_transform_returns_dataframe(self, sample_df, rfm_config, seg_config):
        result = transform(sample_df, rfm_config, seg_config)
        assert result is not None
        assert isinstance(result, pd.DataFrame)

    def test_transform_adds_rfm_columns(self, sample_df, rfm_config, seg_config):
        result = transform(sample_df, rfm_config, seg_config)
        for col in ["recency_score", "frequency_score", "monetary_score", "rfm_score", "segment"]:
            assert col in result.columns, f"Missing column: {col}"

    def test_transform_rfm_score_range(self, sample_df, rfm_config, seg_config):
        result = transform(sample_df, rfm_config, seg_config)
        assert result["rfm_score"].between(3, 9).all()

    def test_transform_valid_segments(self, sample_df, rfm_config, seg_config):
        result = transform(sample_df, rfm_config, seg_config)
        valid_segments = {"VIP", "Loyal", "Regular", "At Risk"}
        assert set(result["segment"].unique()).issubset(valid_segments)

    def test_transform_drops_duplicates(self, rfm_config, seg_config):
        df_with_dups = pd.DataFrame([
            {"customer_id": "C001", "name": "Alice", "age": 28, "email": "a@b.com", "region": "N",
             "total_spend": 125000, "purchase_frequency": 18, "days_since_last_purchase": 5},
            {"customer_id": "C001", "name": "Alice", "age": 28, "email": "a@b.com", "region": "N",
             "total_spend": 125000, "purchase_frequency": 18, "days_since_last_purchase": 5},
        ])
        result = transform(df_with_dups, rfm_config, seg_config)
        assert len(result) == 1

    def test_transform_drops_nulls(self, rfm_config, seg_config):
        df_with_nulls = pd.DataFrame([
            {"customer_id": None, "name": "Alice", "age": 28, "email": "a@b.com", "region": "N",
             "total_spend": 125000, "purchase_frequency": 18, "days_since_last_purchase": 5},
            {"customer_id": "C002", "name": "Bob", "age": 35, "email": "b@b.com", "region": "S",
             "total_spend": 50000, "purchase_frequency": 9, "days_since_last_purchase": 25},
        ])
        result = transform(df_with_nulls, rfm_config, seg_config)
        assert len(result) == 1
        assert result.iloc[0]["customer_id"] == "C002"

    def test_high_value_customer_is_vip(self, rfm_config, seg_config):
        df = pd.DataFrame([{
            "customer_id": "VIP001", "name": "VIP User", "age": 30,
            "email": "vip@crm.com", "region": "North",
            "total_spend": 150000, "purchase_frequency": 20,
            "days_since_last_purchase": 2
        }])
        result = transform(df, rfm_config, seg_config)
        assert result.iloc[0]["segment"] == "VIP"

    def test_inactive_customer_is_at_risk(self, rfm_config, seg_config):
        df = pd.DataFrame([{
            "customer_id": "LOW001", "name": "At Risk User", "age": 50,
            "email": "low@crm.com", "region": "East",
            "total_spend": 5000, "purchase_frequency": 1,
            "days_since_last_purchase": 200
        }])
        result = transform(df, rfm_config, seg_config)
        assert result.iloc[0]["segment"] == "At Risk"
