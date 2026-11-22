"""The real ledger-api over HTTP (stdlib urllib), for creating test fixtures (D-103).

Accounts, deposits and authorizations go through the API, so the worker is tested against the one
implementation of authorize. Every write sends a fresh Idempotency-Key. Reads (T9): a balance with its
X-Cache header, and a payment's status. The API key is never printed: errors show the status and the
problem body only.
"""

import json
import urllib.error
from decimal import Decimal
import urllib.request
import uuid


class ApiError(AssertionError):
    pass


class ApiClient:
    def __init__(self, base_url: str, api_key: str) -> None:
        self._base_url = base_url
        self._api_key = api_key

    def try_post(
        self, path: str, body: dict, *, keyed: bool = True, correlation_id: str | None = None
    ) -> tuple[int, dict | str]:
        """(status, body) without raising on an HTTP error status; a fresh Idempotency-Key when keyed."""
        headers = {"Content-Type": "application/json", "X-Api-Key": self._api_key}
        if keyed:
            headers["Idempotency-Key"] = f"worker-{uuid.uuid4()}"
        if correlation_id is not None:
            headers["X-Correlation-Id"] = correlation_id
        request = urllib.request.Request(
            self._base_url + path, data=json.dumps(body).encode(), headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as ex:
            return ex.code, ex.read().decode(errors="replace")

    def get(self, path: str) -> tuple[int, dict, dict | str]:
        """(status, headers with lower-case names, body) without raising on an HTTP error status. Numbers parse as Decimal, never float
        (D-011); money is a JSON string anyway."""
        request = urllib.request.Request(self._base_url + path, headers={"X-Api-Key": self._api_key}, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.status, _lower(response.headers), json.loads(response.read(), parse_float=Decimal)
        except urllib.error.HTTPError as ex:
            return ex.code, _lower(ex.headers), ex.read().decode(errors="replace")

    def balance(self, account_id: int) -> tuple[str, str | None]:
        """GET /v1/accounts/{id}/balance: (the balance string, the X-Cache header or None when the cache is off)."""
        status, headers, body = self.get(f"/v1/accounts/{account_id}/balance")
        if status != 200:
            raise ApiError(f"GET balance {account_id} -> {status}: {body}")
        return body["balance"], headers.get("x-cache")

    def payment_status(self, payment_id: uuid.UUID) -> str:
        """GET /v1/payments/{id}: the payment's current status, a DB read."""
        status, _headers, body = self.get(f"/v1/payments/{payment_id}")
        if status != 200:
            raise ApiError(f"GET payment {payment_id} -> {status}: {body}")
        return body["status"]

    def _post(self, path: str, body: dict, *, keyed: bool = True, correlation_id: str | None = None) -> dict:
        status, body_out = self.try_post(path, body, keyed=keyed, correlation_id=correlation_id)
        if status >= 400:
            raise ApiError(f"POST {path} -> {status}: {body_out}")
        return body_out

    def create_account(self, account_type: str) -> int:
        body = self._post("/v1/accounts", {"name": f"worker-{uuid.uuid4()}", "type": account_type}, keyed=False)
        return body["account_id"]

    def deposit(self, account_id: int, amount: str) -> None:
        self._post("/v1/deposits", {"account_id": account_id, "amount": amount})

    def transfer(self, source: int, target: int, amount: str) -> None:
        self._post("/v1/transfers", {"from_account_id": source, "to_account_id": target, "amount": amount})

    def authorize(self, customer: int, merchant: int, amount: str, *, correlation_id: str | None = None) -> uuid.UUID:
        body = self._post(
            "/v1/payments",
            {"customer_account_id": customer, "merchant_account_id": merchant, "amount": amount},
            correlation_id=correlation_id,
        )
        return uuid.UUID(body["payment_id"])


def _lower(headers) -> dict[str, str]:
    return {name.lower(): value for name, value in headers.items()}
