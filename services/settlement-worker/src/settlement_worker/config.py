"""The worker's configuration, from the environment (D-081, D-085, D-091, D-108, D-113, D-125).

Names follow .env.example where it has them. Anything invalid is a ConfigError, and the worker
refuses to start (exit 2). The password is never part of an error message or the repr.
"""

import os
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta

AFTER_PROCESSOR_BEFORE_DB = "AFTER_PROCESSOR_BEFORE_DB"
AFTER_DB_BEFORE_OFFSET = "AFTER_DB_BEFORE_OFFSET"
# Not a crash: raises a plain RuntimeError inside the settle transaction on every attempt (D-125).
RAISE_BEFORE_COMMIT = "RAISE_BEFORE_COMMIT"
FAULT_POINTS = (AFTER_PROCESSOR_BEFORE_DB, AFTER_DB_BEFORE_OFFSET, RAISE_BEFORE_COMMIT)

_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Config:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    worker_password: str = field(repr=False)
    bootstrap_servers: str
    topic: str
    consumer_group: str
    session_timeout_ms: int
    heartbeat_interval_ms: int
    lock_timeout: timedelta
    fault_point: str | None
    fault_payment_id: uuid.UUID | None
    stop_on_stdin_eof: bool
    metrics_host: str
    metrics_port: int
    retry_base_ms: int
    retry_max_ms: int
    max_attempts: int
    retry_jitter: bool

    @property
    def fault_armed(self) -> bool:
        return self.fault_point is not None

    def db_params(self) -> dict:
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "database": self.postgres_db,
            "user": "settlement_worker",
            "password": self.worker_password,
        }

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Config":
        env = os.environ if env is None else env

        def text(name: str, default: str | None = None) -> str:
            value = env.get(name, default)
            if value is None or not value.strip():
                raise ConfigError(f"{name} is required")
            return value.strip()

        def integer(name: str, default: str, low: int, high: int) -> int:
            raw = text(name, default)
            if not re.fullmatch(r"\d+", raw) or not low <= int(raw) <= high:
                raise ConfigError(f"{name} must be an integer from {low} to {high}, was {raw!r}")
            return int(raw)

        password = env.get("SETTLEMENT_WORKER_PASSWORD")
        if not password:
            raise ConfigError("SETTLEMENT_WORKER_PASSWORD is required")
        session = integer("WORKER_SESSION_TIMEOUT_MS", "10000", 1000, 3_600_000)
        heartbeat = integer("WORKER_HEARTBEAT_INTERVAL_MS", "3000", 100, 3_600_000)
        if heartbeat >= session:
            raise ConfigError(
                f"WORKER_HEARTBEAT_INTERVAL_MS ({heartbeat}) must be below WORKER_SESSION_TIMEOUT_MS ({session})"
            )

        fault_point = env.get("FAULT_POINT") or None
        fault_payment = env.get("FAULT_PAYMENT_ID") or None
        if fault_point is not None and fault_point not in FAULT_POINTS:
            raise ConfigError(f"FAULT_POINT must be one of {', '.join(FAULT_POINTS)}, was {fault_point!r}")
        if fault_point is not None and fault_payment is None:
            raise ConfigError("FAULT_POINT is set but FAULT_PAYMENT_ID is not")
        if fault_payment is not None and fault_point is None:
            raise ConfigError("FAULT_PAYMENT_ID is set but FAULT_POINT is not")
        if fault_payment is not None and not _UUID.fullmatch(fault_payment):
            raise ConfigError(f"FAULT_PAYMENT_ID must be a canonical UUID, was {fault_payment!r}")

        stop_on_eof = env.get("WORKER_STOP_ON_STDIN_EOF", "")
        if stop_on_eof not in ("", "0", "1"):
            raise ConfigError(f"WORKER_STOP_ON_STDIN_EOF must be 0 or 1, was {stop_on_eof!r}")

        # Retry in place (D-108): integer milliseconds, cap >= base.
        retry_base = integer("WORKER_RETRY_BASE_MS", "200", 1, 3_600_000)
        retry_max = integer("WORKER_RETRY_MAX_MS", "30000", 1, 3_600_000)
        if retry_max < retry_base:
            raise ConfigError(f"WORKER_RETRY_MAX_MS ({retry_max}) must be at least WORKER_RETRY_BASE_MS ({retry_base})")
        jitter = env.get("WORKER_RETRY_JITTER", "1")
        if jitter not in ("0", "1"):
            raise ConfigError(f"WORKER_RETRY_JITTER must be 0 or 1, was {jitter!r}")

        return cls(
            postgres_host=text("POSTGRES_HOST", "localhost"),
            postgres_port=integer("POSTGRES_PORT", "5433", 1, 65535),
            postgres_db=text("POSTGRES_DB", "ledger"),
            worker_password=password,
            bootstrap_servers=text("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=text("LEDGER_PAYMENTS_TOPIC", "ledger.payments.v1"),
            consumer_group=text("WORKER_CONSUMER_GROUP", "settlement-worker"),
            session_timeout_ms=session,
            heartbeat_interval_ms=heartbeat,
            lock_timeout=timedelta(milliseconds=integer("WORKER_LOCK_TIMEOUT_MS", "5000", 1, 2_147_483_647)),
            fault_point=fault_point,
            fault_payment_id=uuid.UUID(fault_payment) if fault_payment else None,
            stop_on_stdin_eof=stop_on_eof == "1",
            # /metrics (D-113): loopback by default (D-029); port 0 binds any free port, which the
            # tests use and read back from metrics_listening.
            metrics_host=text("WORKER_METRICS_HOST", "127.0.0.1"),
            metrics_port=integer("WORKER_METRICS_PORT", "8000", 0, 65535),
            retry_base_ms=retry_base,
            retry_max_ms=retry_max,
            max_attempts=integer("WORKER_MAX_ATTEMPTS", "5", 1, 1000),
            retry_jitter=jitter == "1",
        )
