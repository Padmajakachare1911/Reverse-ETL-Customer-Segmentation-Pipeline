"""
tests/test_crm_client.py
-------------------------
Unit tests for the CRM API client using mock HTTP responses.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.crm_client import CRMClient

SAMPLE_CUSTOMER = {
    "customer_id": "C001",
    "name": "Alice",
    "email": "alice@email.com",
    "region": "North",
    "segment": "VIP",
    "rfm_score": 9,
    "total_spend": 125000,
    "purchase_frequency": 18,
    "days_since_last_purchase": 5,
    "recommended_action": "Send premium offer",
    "warehouse_run_id": 1,
    "synced_at": "2026-01-01T00:00:00",
}


@pytest.fixture
def client():
    return CRMClient(
        base_url="http://127.0.0.1:5050",
        timeout=5,
        retry_attempts=2,
        retry_delay=0,
    )


class TestCRMClientHealthCheck:
    def test_health_check_success(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        with patch.object(client.session, "get", return_value=mock_resp):
            assert client.health_check() is True

    def test_health_check_failure(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        with patch.object(client.session, "get", return_value=mock_resp):
            assert client.health_check() is False

    def test_health_check_connection_error(self, client):
        import requests
        with patch.object(client.session, "get", side_effect=requests.ConnectionError):
            assert client.health_check() is False


class TestCRMClientPushCustomer:
    def test_push_customer_success(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.json.return_value = {"message": "Customer synced to CRM", "customer_id": "C001"}
        mock_resp.raise_for_status.return_value = None
        with patch.object(client.session, "post", return_value=mock_resp):
            assert client.push_customer(SAMPLE_CUSTOMER) is True

    def test_push_customer_failure(self, client):
        import requests
        with patch.object(client.session, "post", side_effect=requests.ConnectionError):
            assert client.push_customer(SAMPLE_CUSTOMER) is False


class TestCRMClientBulkPush:
    def test_bulk_push_success(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.json.return_value = {
            "message": "Bulk sync complete",
            "success_count": 2,
            "failed_count": 0,
        }
        mock_resp.raise_for_status.return_value = None
        with patch.object(client.session, "post", return_value=mock_resp):
            result = client.bulk_push([SAMPLE_CUSTOMER, SAMPLE_CUSTOMER])
            assert result["success_count"] == 2
            assert result["failed_count"] == 0

    def test_bulk_push_connection_error(self, client):
        import requests
        with patch.object(client.session, "post", side_effect=requests.ConnectionError):
            result = client.bulk_push([SAMPLE_CUSTOMER])
            assert result["success_count"] == 0
            assert result["failed_count"] == 1
