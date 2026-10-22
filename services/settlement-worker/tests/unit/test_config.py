"""Worker configuration (D-081, D-085, D-091): defaults, validation, fault arming."""

import json
import os
import subprocess
import sys
import uuid
from datetime import timedelta

import pytest
import structlog
from structlog.testing import capture_logs

from settlement_worker import consumer
from settlement_worker.config import Config, ConfigError

PAYMENT = "5b01a9e5-32ce-4c82-90b0-ac8c5143460c"


def _env(**overrides):
    env = {"SETTLEMENT_WORKER_PASSWORD": "secret"}
    env.update(overrides)
    return {k: v for k, v in env.items() if v is not None}


def test_defaults():
    config = Config.from_env(_env())
    assert (config.postgres_host, config.postgres_port, config.postgres_db) == ("localhost", 5433, "ledger")
    assert (config.bootstrap_servers, config.topic, config.consumer_group) == (
        "localhost:9092",
        "ledger.payments.v1",
        "settlement-worker",
    )
    assert (config.session_timeout_ms, config.heartbeat_interval_ms) == (10000, 3000)
    assert config.lock_timeout == timedelta(seconds=5)
    assert (config.fault_point, config.fault_payment_id, config.fault_armed) == (None, None, False)
    assert config.stop_on_stdin_eof is False
    assert "secret" not in repr(config)


def test_armed_fault():
    config = Config.from_env(_env(FAULT_POINT="AFTER_DB_BEFORE_OFFSET", FAULT_PAYMENT_ID=PAYMENT))
    assert config.fault_armed
    assert config.fault_payment_id == uuid.UUID(PAYMENT)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"SETTLEMENT_WORKER_PASSWORD": None}, "SETTLEMENT_WORKER_PASSWORD is required"),
        ({"FAULT_POINT": "BEFORE_EVERYTHING", "FAULT_PAYMENT_ID": PAYMENT}, "FAULT_POINT must be one of"),
        ({"FAULT_POINT": "AFTER_DB_BEFORE_OFFSET"}, "FAULT_PAYMENT_ID is not"),
        ({"FAULT_PAYMENT_ID": PAYMENT}, "FAULT_POINT is not"),
        ({"FAULT_POINT": "AFTER_DB_BEFORE_OFFSET", "FAULT_PAYMENT_ID": "p-1"}, "canonical UUID"),
        ({"POSTGRES_PORT": "70000"}, "POSTGRES_PORT"),
        ({"POSTGRES_PORT": "x"}, "POSTGRES_PORT"),
        ({"WORKER_SESSION_TIMEOUT_MS": "6000", "WORKER_HEARTBEAT_INTERVAL_MS": "6000"}, "must be below"),
        ({"WORKER_STOP_ON_STDIN_EOF": "yes"}, "WORKER_STOP_ON_STDIN_EOF"),
        ({"WORKER_LOCK_TIMEOUT_MS": "0"}, "WORKER_LOCK_TIMEOUT_MS"),
    ],
)
def test_invalid(overrides, message):
    with pytest.raises(ConfigError, match=message):
        Config.from_env(_env(**overrides))


@pytest.mark.parametrize(
    "overrides",
    [{"FAULT_POINT": "BEFORE_EVERYTHING", "FAULT_PAYMENT_ID": PAYMENT}, {"FAULT_POINT": "AFTER_DB_BEFORE_OFFSET"}],
    ids=["unknown point", "point without payment"],
)
def test_invalid_fault_config_exits_2(overrides):
    env = {k: v for k, v in os.environ.items() if not k.startswith("FAULT_")}
    env.update(_env(WORKER_METRICS_PORT="0", **overrides))
    result = subprocess.run([sys.executable, "-m", "settlement_worker"], capture_output=True, text=True, env=env, timeout=30)
    assert result.returncode == 2, result.stdout + result.stderr
    assert [json.loads(line)["event"] for line in result.stdout.splitlines()] == ["config_invalid"]


def test_metrics_defaults():
    config = Config.from_env(_env())
    assert (config.metrics_host, config.metrics_port) == ("127.0.0.1", 8000)
    assert Config.from_env(_env(WORKER_METRICS_PORT="0")).metrics_port == 0
    assert Config.from_env(_env(WORKER_METRICS_PORT="65535")).metrics_port == 65535


# D-113: an invalid metrics setting is refused before the worker connects anywhere or opens a port.
@pytest.mark.parametrize(
    "variable, value",
    [
        ("WORKER_METRICS_PORT", "65536"),
        ("WORKER_METRICS_PORT", "-1"),
        ("WORKER_METRICS_PORT", "abc"),
        ("WORKER_METRICS_HOST", ""),
    ],
)
def test_invalid_metrics_config_exits_2(variable, value):
    with pytest.raises(ConfigError, match=variable):
        Config.from_env(_env(**{variable: value}))
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FAULT_", "WORKER_"))}
    env.update(_env(**{"WORKER_METRICS_PORT": "0", variable: value}))
    result = subprocess.run([sys.executable, "-m", "settlement_worker"], capture_output=True, text=True, env=env, timeout=30)
    assert result.returncode == 2, result.stdout + result.stderr
    lines = [json.loads(line) for line in result.stdout.splitlines()]
    assert [line["event"] for line in lines] == ["config_invalid"], lines
    assert variable in lines[0]["error"]


@pytest.mark.parametrize("armed, level", [(True, "warning"), (False, "info")])
def test_worker_started_warns_when_a_fault_is_armed(armed, level):
    overrides = {"FAULT_POINT": "AFTER_DB_BEFORE_OFFSET", "FAULT_PAYMENT_ID": PAYMENT} if armed else {}
    config = Config.from_env(_env(WORKER_CONSUMER_GROUP="g", **overrides))
    with capture_logs() as logs:
        consumer.announce_start(structlog.get_logger(), config)
    assert logs == [
        {
            "event": "worker_started",
            "log_level": level,
            "consumer_group": "g",
            "fault_point": "AFTER_DB_BEFORE_OFFSET" if armed else None,
            "correlation_id": None,
        }
    ]
