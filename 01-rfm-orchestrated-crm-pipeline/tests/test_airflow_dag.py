"""
tests/test_airflow_dag.py
-------------------------
Tests for Airflow DAG operator functions and structure.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dags.reverse_etl_airflow_dag import (
    _run_extract,
    _run_transform,
    _run_load_warehouse,
    _run_reverse_etl_push,
    _run_notify_and_audit,
    default_args,
)


class TestAirflowDAGFunctions:
    def test_default_args(self):
        assert default_args["owner"] == "data-engineering-team"
        assert default_args["retries"] == 2
        assert default_args["depends_on_past"] is False

    def test_run_extract(self):
        count = _run_extract()
        assert count == 30

    def test_run_transform(self):
        segments = _run_transform()
        assert isinstance(segments, dict)
        assert "VIP" in segments
        assert "At Risk" in segments

    def test_run_load_warehouse(self):
        run_id = _run_load_warehouse()
        assert run_id is not None
        assert run_id > 0

    def test_run_reverse_etl_push_mocked(self):
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = 1

        with patch("src.crm_client.CRMClient.health_check", return_value=True):
            with patch("src.crm_client.CRMClient.bulk_push", return_value={"success_count": 30, "failed_count": 0}):
                summary = _run_reverse_etl_push(ti=mock_ti)
                assert summary["success_count"] == 30

    def test_run_notify_and_audit(self):
        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = lambda task_ids: 1 if "load" in task_ids else {"success_count": 30}
        result = _run_notify_and_audit(ti=mock_ti)
        assert result["status"] == "SUCCESS"
        assert result["run_id"] == 1
