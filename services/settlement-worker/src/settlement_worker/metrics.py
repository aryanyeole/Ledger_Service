"""The worker's /metrics endpoint (D-113).

prometheus_client's own HTTP server, on its default registry, in a daemon thread. Application
metrics are module-level collectors on that registry: RETRIES (D-108), DEAD_LETTERS (D-111),
RECONCILE (D-116). The host and port reported are the ones the server actually bound
(server_address), so port 0 works. Any OSError while binding, an unresolvable host included, is a
MetricsBindError: the worker treats it as invalid configuration (exit 2).
"""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from wsgiref.simple_server import WSGIServer

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, start_http_server

# Exposed as settlement_retries_total; sqlstate is "" when the failure has none.
RETRIES = Counter("settlement_retries", "Records scheduled for a retry in place (D-108)", ["error_class", "sqlstate"])
# Exposed as settlement_dead_letters_total (D-111).
DEAD_LETTERS = Counter("settlement_dead_letters", "Records dead-lettered to the DLQ topic (D-111)", ["error_class"])


class ReconcileMetrics:
    """The reconciliation's collectors (D-116), on an injectable registry so a test can use a fresh
    one. Values are Decimal; prometheus_client converts them, so worker code never calls float(
    (D-093). ledger_drift_amount{account_id} exists only for accounts that have drifted in this
    process, and goes to 0, never away, once they reconcile."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self.drift_amount = Gauge(
            "ledger_drift_amount", "balance - SUM(entries) of an account that has drifted", ["account_id"],
            registry=registry,
        )
        self.drift_accounts = Gauge("ledger_drift_accounts", "Accounts drifted in the last run", registry=registry)
        self.global_sum = Gauge("ledger_entries_global_sum", "SUM(entries.amount) in the last run", registry=registry)
        self.escrow_drift = Gauge(
            "ledger_escrow_drift_amount", "ESCROW's SUM(entries) - sum of AUTHORIZED amounts", registry=registry
        )
        # Exposed as ledger_reconciliation_runs_total.
        self.runs = Counter("ledger_reconciliation_runs", "Reconciliation runs by result", ["result"], registry=registry)
        self.last_completed = Gauge(
            "ledger_reconciliation_last_completed_timestamp_seconds", "When the last run completed", registry=registry
        )
        self._series: set[int] = set()

    def completed(self, drifts: Mapping[int, Decimal], global_sum: Decimal, escrow_drift: Decimal, result: str) -> None:
        """A completed run: drifts maps each drifted account to balance - SUM(entries)."""
        for account_id, drift in drifts.items():
            self.drift_amount.labels(account_id=str(account_id)).set(drift)
            self._series.add(account_id)
        for account_id in self._series - drifts.keys():
            self.drift_amount.labels(account_id=str(account_id)).set(0)
        self.drift_accounts.set(len(drifts))
        self.global_sum.set(global_sum)
        self.escrow_drift.set(escrow_drift)
        self.runs.labels(result=result).inc()
        self.last_completed.set_to_current_time()

    def failed(self) -> None:
        self.runs.labels(result="error").inc()


RECONCILE = ReconcileMetrics()


class MetricsBindError(RuntimeError):
    pass


@dataclass(frozen=True)
class MetricsServer:
    server: WSGIServer

    @property
    def host(self) -> str:
        return self.server.server_address[0]

    @property
    def port(self) -> int:
        return self.server.server_address[1]

    def close(self) -> None:
        """Stops serving (returns once the serve loop has exited) and closes the socket."""
        self.server.shutdown()
        self.server.server_close()


def start(host: str, port: int) -> MetricsServer:
    try:
        server, _thread = start_http_server(port, addr=host)
    except OSError as ex:
        raise MetricsBindError(f"cannot serve metrics on {host}:{port}: {ex}") from ex
    return MetricsServer(server)
