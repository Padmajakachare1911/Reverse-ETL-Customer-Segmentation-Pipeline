"""
tests/test_prefect_flow.py
--------------------------
Unit and integration tests for Prefect flow & orchestration tasks.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestration.prefect_flow import (
    task_extract_and_validate,
    task_transform_and_segment,
    task_load_warehouse,
    task_reverse_etl_push,
    task_generate_summary,
    reverse_etl_flow,
)
from src.config_loader import load_config


@pytest.fixture
def test_config():
    return load_config()


class TestPrefectTasks:
    def test_task_extract_and_validate(self, tmp_path):
        csv = tmp_path / "test.csv"
        csv.write_text(
            "customer_id,name,age,email,total_spend,purchase_frequency,days_since_last_purchase,region\n"
            "T001,Test User,30,test@example.com,50000,10,12,North\n"
        )
        df = task_extract_and_validate.fn(csv)
        assert df is not None
        assert len(df) == 1
        assert df.iloc[0]["customer_id"] == "T001"

    def test_task_extract_invalid_raises_error(self, tmp_path):
        csv = tmp_path / "empty.csv"
        csv.write_text("invalid_column\n")
        with pytest.raises(ValueError):
            task_extract_and_validate.fn(csv)

    def test_task_transform_and_segment(self, test_config):
        import pandas as pd
        df = pd.DataFrame([{
            "customer_id": "T001", "name": "Alice", "age": 28, "email": "a@b.com",
            "region": "North", "total_spend": 120000, "purchase_frequency": 16,
            "days_since_last_purchase": 5
        }])
        transformed = task_transform_and_segment.fn(
            df,
            rfm_config=test_config["rfm"],
            seg_config=test_config["segmentation"]
        )
        assert "segment" in transformed.columns
        assert transformed.iloc[0]["segment"] == "VIP"

    def test_task_load_warehouse(self, tmp_path, test_config):
        import pandas as pd
        db = tmp_path / "test_warehouse.db"
        df = pd.DataFrame([{
            "customer_id": "T001", "name": "Alice", "age": 28, "email": "a@b.com",
            "region": "North", "total_spend": 120000, "purchase_frequency": 16,
            "days_since_last_purchase": 5, "recency_score": 3, "frequency_score": 3,
            "monetary_score": 3, "rfm_score": 9, "segment": "VIP"
        }])
        run_id = task_load_warehouse.fn(
            df,
            db_path=db,
            table_config=test_config["warehouse"]["tables"]
        )
        assert run_id is not None
        assert run_id > 0

    def test_task_reverse_etl_push_mocked(self, tmp_path, test_config):
        import pandas as pd
        from src.loader import load
        
        db = tmp_path / "test_wh.db"
        df = pd.DataFrame([{
            "customer_id": "T001", "name": "Alice", "age": 28, "email": "a@b.com",
            "region": "North", "total_spend": 120000, "purchase_frequency": 16,
            "days_since_last_purchase": 5, "recency_score": 3, "frequency_score": 3,
            "monetary_score": 3, "rfm_score": 9, "segment": "VIP"
        }])
        run_id = load(df, db_path=db, table_config=test_config["warehouse"]["tables"])

        with patch("src.crm_client.CRMClient.health_check", return_value=True):
            with patch("src.crm_client.CRMClient.bulk_push", return_value={"success_count": 1, "failed_count": 0}):
                summary = task_reverse_etl_push.fn(
                    db_path=db,
                    run_id=run_id,
                    crm_config=test_config["crm"],
                    marketing_actions=test_config["marketing_actions"],
                    use_bulk=True
                )
                assert summary["success_count"] == 1
                assert summary["failed_count"] == 0

    def test_task_generate_summary(self):
        res = task_generate_summary.fn(
            run_id=42,
            records_count=30,
            push_summary={"success_count": 30, "failed_count": 0}
        )
        assert res["run_id"] == 42
        assert res["records_count"] == 30
        assert res["status"] == "COMPLETED"


class TestPrefectFlowIntegration:
    def test_reverse_etl_flow_execution_mocked(self):
        with patch("src.crm_client.CRMClient.health_check", return_value=True):
            with patch("src.crm_client.CRMClient.bulk_push", return_value={"success_count": 30, "failed_count": 0}):
                result = reverse_etl_flow(use_bulk=True)
                assert result is not None
                assert result["status"] == "COMPLETED"
                assert result["records_count"] == 30
                assert result["crm_summary"]["success_count"] == 30
