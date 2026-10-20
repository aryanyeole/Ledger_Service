"""The real ledger-api over HTTP (stdlib urllib), for creating test fixtures (D-103).

Accounts, deposits and authorizations go through the API, so the worker is tested against the one
implementation of authorize. Every write sends a fresh Idempotency-Key. The API key is never
printed: errors show the status and the problem body only.
"""

import json
import urllib.error
import urllib.request
import uuid


class ApiError(AssertionError):
    pass


class ApiClient:
    def __init__(self, base_url: str, api_key: str) -> None:
        self._base_url = base_url
        self._api_key = api_key

    def _post(self, path: str, body: dict, *, keyed: bool = True, correlation_id: str | None = None) -> dict:
        headers = {"Content-Type": "application/json", "X-Api-Key": self._api_key}
        if keyed:
            headers["Idempotency-Key"] = f"worker-{uuid.uuid4()}"
        if correlation_id is not None:
            headers["X-Correlation-Id"] = correlation_id
        request = urllib.request.Request(
            self._base_url + path, data=json.dumps(body).encode(), headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as ex:
            raise ApiError(f"POST {path} -> {ex.code}: {ex.read().decode(errors='replace')}") from None

    def create_account(self, account_type: str) -> int:
        body = self._post("/v1/accounts", {"name": f"worker-{uuid.uuid4()}", "type": account_type}, keyed=False)
        return body["account_id"]

    def deposit(self, account_id: int, amount: str) -> None:
        self._post("/v1/deposits", {"account_id": account_id, "amount": amount})

    def authorize(self, customer: int, merchant: int, amount: str, *, correlation_id: str | None = None) -> uuid.UUID:
        body = self._post(
            "/v1/payments",
            {"customer_account_id": customer, "merchant_account_id": merchant, "amount": amount},
            correlation_id=correlation_id,
        )
        return uuid.UUID(body["payment_id"])
