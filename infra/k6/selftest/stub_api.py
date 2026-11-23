"""The cache benchmark's self-test stub (T14, D-146): the ledger API's benchmark surface, Python standard library only,
so infra/k6/cache-benchmark.js and its judge can be exercised without the real stack (see README.md).

Implements POST /v1/accounts, POST /v1/deposits, POST /v1/payments, GET /v1/payments/{id} and
GET /v1/accounts/{id}/balance with exact Decimal balances: an authorize debits the customer; a payment is terminal on its
second GET (SETTLED credits the merchant amount - fee; FAILED, for cents 13, refunds the customer). X-Cache cycles
hit/miss/bypass. With MODE=stale, every 7th balance read is served 0.01 high, so the judge must count stale reads.
An instrument check, never a result: exact must give 0 stale reads and exit 0; stale must count stale reads and exit 99.
"""

import itertools
import json
import os
import sys
import threading
import uuid
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOCK = threading.Lock()
BALANCES: dict[int, Decimal] = {}
TYPES: dict[int, str] = {}
PAYMENTS: dict[str, dict] = {}
IDS = itertools.count(1000)
READS = itertools.count(1)
CACHE = itertools.cycle(["hit", "miss", "bypass"])
MODE = os.environ.get("MODE", "exact")
FEE_RATE = Decimal("0.029")


def money(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01")))


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, status, body, headers=None):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def body(self):
        return json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")

    def do_POST(self):
        if not self.headers.get("X-Api-Key"):
            return self.reply(401, {"code": "unauthorized"})
        b = self.body()
        with LOCK:
            if self.path == "/v1/accounts":
                account = next(IDS)
                BALANCES[account] = Decimal("0")
                TYPES[account] = b["type"]
                return self.reply(201, {"account_id": account, "name": b["name"], "type": b["type"]})
            if not self.headers.get("Idempotency-Key"):
                return self.reply(400, {"code": "missing_idempotency_key"})
            if self.path == "/v1/deposits":
                BALANCES[b["account_id"]] += Decimal(b["amount"])
                return self.reply(201, {"transaction_id": next(IDS), "kind": "DEPOSIT"})
            if self.path == "/v1/payments":
                amount = Decimal(b["amount"])
                customer, merchant = b["customer_account_id"], b["merchant_account_id"]
                if BALANCES[customer] < amount:
                    return self.reply(422, {"code": "insufficient_funds"})
                fee = (amount * FEE_RATE).quantize(Decimal("0.01"))
                BALANCES[customer] -= amount
                pid = str(uuid.uuid4())
                declined = (amount * 100) % 100 == 13
                PAYMENTS[pid] = {"customer": customer, "merchant": merchant, "amount": amount, "fee": fee,
                                 "declined": declined, "status": "AUTHORIZED", "gets": 0}
                return self.reply(201, {"payment_id": pid, "status": "AUTHORIZED", "amount": money(amount), "fee": money(fee)})
        return self.reply(404, {"code": "not_found"})

    def do_GET(self):
        if not self.headers.get("X-Api-Key"):
            return self.reply(401, {"code": "unauthorized"})
        parts = self.path.strip("/").split("/")
        with LOCK:
            if len(parts) == 3 and parts[:2] == ["v1", "payments"]:
                p = PAYMENTS.get(parts[2])
                if p is None:
                    return self.reply(404, {"code": "payment_not_found"})
                p["gets"] += 1
                if p["status"] == "AUTHORIZED" and p["gets"] >= 2:
                    if p["declined"]:
                        p["status"] = "FAILED"
                        BALANCES[p["customer"]] += p["amount"]
                    else:
                        p["status"] = "SETTLED"
                        BALANCES[p["merchant"]] += p["amount"] - p["fee"]
                return self.reply(200, {"payment_id": parts[2], "status": p["status"]})
            if len(parts) == 4 and parts[:2] == ["v1", "accounts"] and parts[3] == "balance":
                account = int(parts[2])
                if account not in BALANCES:
                    return self.reply(404, {"code": "account_not_found"})
                served = BALANCES[account]
                if MODE == "stale" and next(READS) % 7 == 0:
                    served += Decimal("0.01")
                return self.reply(200, {"account_id": account, "balance": money(served)}, {"X-Cache": next(CACHE)})
        return self.reply(404, {"code": "not_found"})


if __name__ == "__main__":
    port = int(sys.argv[1])
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
