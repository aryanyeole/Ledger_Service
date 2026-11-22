"""The balance invalidation's startup lookup (D-152): ESCROW's and PLATFORM_FEES' ids are read once, after the
role check and before /metrics, and only when invalidation is active. A missing seeded account is a broken
ledger: processing_failed (the existing fatal shape, no record) and worker_stopped, exit 1. An unreachable DB
there is db_unavailable, exit 1. Without Redis, or with invalidation off, the lookup never runs.

The DB session and the lookup are stand-ins: what is under test is the startup order and the exit, not ledger
logic (the lookup itself is settlement._system_account, exercised against real Postgres by every settle).
"""

import asyncio

import pytest
from structlog.testing import capture_logs

from settlement_worker import consumer, metrics, settlement
from settlement_worker.config import Config


class FakeSession:
    def __init__(self, config):
        self.conn = object()
        self.closed_calls = 0

    async def open(self):
        return None

    async def close(self):
        self.closed_calls += 1


def _config(**extra):
    return Config.from_env({"SETTLEMENT_WORKER_PASSWORD": "x", "WORKER_METRICS_PORT": "0", **extra})


def _no_metrics(*args, **kwargs):
    raise AssertionError("the worker must stop before /metrics")


@pytest.fixture
def session(monkeypatch):
    sessions = []

    def make(config):
        sessions.append(FakeSession(config))
        return sessions[-1]

    monkeypatch.setattr(consumer.db, "DbSession", make)
    monkeypatch.setattr(metrics, "start", _no_metrics)
    return sessions


def test_a_missing_system_account_is_fatal_exit_1(monkeypatch, session):
    async def missing(conn):
        raise settlement.MissingAccount("system:escrow account is missing")

    monkeypatch.setattr(consumer.settlement, "system_account_ids", missing)
    with capture_logs() as logs:
        code = asyncio.run(consumer.run(_config(REDIS_URL="redis://127.0.0.1:6390"), asyncio.Event(), {}))
    assert code == consumer.EXIT_PROCESSING_ERROR == 1
    assert [e["event"] for e in logs] == ["processing_failed", "worker_stopped"]
    failed, stopped = logs
    assert (failed["log_level"], failed["topic"], failed["partition"], failed["offset"]) == ("error", None, None, None)
    assert failed["error"] == "MissingAccount: system:escrow account is missing"
    assert failed["correlation_id"] is None
    assert (stopped["log_level"], stopped["reason"]) == ("error", "processing_failed")
    assert session[0].closed_calls == 1


def test_an_unreachable_db_at_the_lookup_is_db_unavailable_exit_1(monkeypatch, session):
    async def unreachable(conn):
        raise OSError("connection reset")

    monkeypatch.setattr(consumer.settlement, "system_account_ids", unreachable)
    with capture_logs() as logs:
        code = asyncio.run(consumer.run(_config(REDIS_URL="redis://127.0.0.1:6390"), asyncio.Event(), {}))
    assert code == consumer.EXIT_PROCESSING_ERROR
    assert [(e["event"], e["reason"]) for e in logs] == [("worker_stopped", "db_unavailable: OSError: connection reset")]
    assert session[0].closed_calls == 1


@pytest.mark.parametrize(
    "extra", [{}, {"REDIS_URL": "redis://127.0.0.1:6390", "WORKER_BALANCE_INVALIDATION": "0"}], ids=["no-redis", "off"]
)
def test_the_lookup_runs_only_when_invalidation_is_active(monkeypatch, session, extra):
    async def must_not_run(conn):
        raise AssertionError("the system ids are read only while invalidation is active")

    class Reached(Exception):
        pass

    def metrics_reached(*args, **kwargs):
        raise Reached

    monkeypatch.setattr(consumer.settlement, "system_account_ids", must_not_run)
    monkeypatch.setattr(metrics, "start", metrics_reached)
    with pytest.raises(Reached):
        asyncio.run(consumer.run(_config(**extra), asyncio.Event(), {}))
