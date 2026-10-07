"""
src/crm_client.py
-----------------
HTTP CRM API client used by the Reverse ETL step.
Sends enriched customer segments to the Mock CRM server via REST API.

Features:
  - Retry logic with exponential backoff
  - Bulk push endpoint
  - Individual push fallback
  - Detailed response logging
"""

import time
import requests
from typing import Optional
from src.logger import get_logger

logger = get_logger(__name__)


class CRMClient:
    """
    REST API client for pushing customer segments to the CRM system.

    Args:
        base_url: Base URL of the CRM API server.
        timeout: Request timeout in seconds.
        retry_attempts: Number of retry attempts on failure.
        retry_delay: Seconds between retries.
    """

    def __init__(
        self,
        base_url: str,
        timeout: int = 10,
        retry_attempts: int = 3,
        retry_delay: int = 2,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Pipeline-Source": "reverse-etl-pipeline",
        })

    def health_check(self) -> bool:
        """Check if the CRM API is reachable."""
        try:
            resp = self.session.get(f"{self.base_url}/api/health", timeout=5)
            if resp.status_code == 200:
                logger.info("[CRM] Health check passed [OK]")
                return True
            logger.warning(f"[CRM] Health check failed: {resp.status_code}")
            return False
        except requests.RequestException as e:
            logger.error(f"[CRM] Health check error: {e}")
            return False

    def _post_with_retry(self, endpoint: str, payload: dict) -> Optional[dict]:
        """POST with retry + exponential backoff."""
        url = f"{self.base_url}{endpoint}"

        for attempt in range(1, self.retry_attempts + 1):
            try:
                resp = self.session.post(url, json=payload, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()

            except requests.HTTPError as e:
                logger.warning(f"[CRM] HTTP error on attempt {attempt}/{self.retry_attempts}: {e}")
            except requests.ConnectionError as e:
                logger.warning(f"[CRM] Connection error on attempt {attempt}/{self.retry_attempts}: {e}")
            except requests.Timeout:
                logger.warning(f"[CRM] Timeout on attempt {attempt}/{self.retry_attempts}")
            except Exception as e:
                logger.error(f"[CRM] Unexpected error: {e}", exc_info=True)
                break

            if attempt < self.retry_attempts:
                delay = self.retry_delay * (2 ** (attempt - 1))  # exponential backoff
                logger.info(f"[CRM] Retrying in {delay}s...")
                time.sleep(delay)

        return None

    def push_customer(self, customer: dict) -> bool:
        """
        Push a single customer's segment data to the CRM.

        Args:
            customer: Dict with customer_id, name, email, segment, recommended_action, etc.

        Returns:
            True if successful, False otherwise.
        """
        result = self._post_with_retry("/api/crm/customers", customer)
        if result:
            logger.debug(f"[CRM] Pushed customer {customer.get('customer_id')}: {result.get('message')}")
            return True
        logger.error(f"[CRM] Failed to push customer {customer.get('customer_id')}")
        return False

    def bulk_push(self, customers: list[dict]) -> dict:
        """
        Push multiple customers in a single bulk API call.

        Args:
            customers: List of customer payload dicts.

        Returns:
            Summary dict with success/failure counts.
        """
        payload = {"customers": customers}
        result = self._post_with_retry("/api/crm/customers/bulk", payload)

        if result:
            logger.info(
                f"[CRM] Bulk push complete — "
                f"success={result.get('success_count', 0)}, "
                f"failed={result.get('failed_count', 0)}"
            )
            return result

        logger.error("[CRM] Bulk push failed entirely.")
        return {"success_count": 0, "failed_count": len(customers)}
