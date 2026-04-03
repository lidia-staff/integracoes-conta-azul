import requests
import logging

logger = logging.getLogger(__name__)


class AsaasClient:
    BASE_URLS = {
        "production": "https://api.asaas.com/v3",
        "sandbox": "https://sandbox.asaas.com/api/v3",
    }

    def __init__(self, api_key: str, environment: str = "production"):
        self.api_key = api_key
        self.base_url = self.BASE_URLS.get(environment, self.BASE_URLS["production"])

    def _headers(self) -> dict:
        return {
            "access_token": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request(self, method: str, path: str, *, params=None, json=None, timeout: int = 30) -> dict:
        url = f"{self.base_url}{path}"
        logger.debug(f"[ASAAS_CLIENT] {method} {url} params={params}")
        resp = requests.request(
            method,
            url,
            headers=self._headers(),
            params=params,
            json=json,
            timeout=timeout,
        )
        if resp.status_code >= 400:
            logger.error(f"[ASAAS_CLIENT] HTTP {resp.status_code} {url} — {resp.text[:500]}")
            raise RuntimeError(f"Asaas API error {resp.status_code}: {resp.text[:300]}")
        return resp.json()

    def get_account_info(self) -> dict:
        """GET /myAccount — valida a API key e retorna dados da conta."""
        return self._request("GET", "/myAccount")

    def get_payment(self, payment_id: str) -> dict:
        """GET /payments/{id} — detalhes completos de um pagamento."""
        return self._request("GET", f"/payments/{payment_id}")

    def get_customer(self, customer_id: str) -> dict:
        """GET /customers/{id} — dados do cliente Asaas."""
        return self._request("GET", f"/customers/{customer_id}")

    # ── Webhooks ──────────────────────────────────────────────────────

    def list_webhooks(self) -> list:
        """GET /webhooks — lista webhooks registrados na conta."""
        data = self._request("GET", "/webhooks")
        return data.get("data", [])

    def create_webhook(self, url: str, events: list) -> dict:
        """POST /webhooks — registra webhook para os eventos indicados."""
        payload = {
            "url": url,
            "enabled": True,
            "interrupted": False,
            "events": events,
        }
        return self._request("POST", "/webhooks", json=payload)

    def delete_webhook(self, webhook_id: str) -> dict:
        """DELETE /webhooks/{id} — remove webhook existente."""
        return self._request("DELETE", f"/webhooks/{webhook_id}")
