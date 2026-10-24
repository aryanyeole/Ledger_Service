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


def test_retry_defaults():
    config = Config.from_env(_env())
    assert (config.retry_base_ms, config.retry_max_ms, config.max_attempts, config.retry_jitter) == (200, 30000, 5, True)
    tuned = Config.from_env(
        _env(WORKER_RETRY_BASE_MS="50", WORKER_RETRY_MAX_MS="50", WORKER_MAX_ATTEMPTS="1", WORKER_RETRY_JITTER="0")
    )
    assert (tuned.retry_base_ms, tuned.retry_max_ms, tuned.max_attempts, tuned.retry_jitter) == (50, 50, 1, False)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"WORKER_RETRY_BASE_MS": "0"}, "WORKER_RETRY_BASE_MS"),
        ({"WORKER_RETRY_BASE_MS": "x"}, "WORKER_RETRY_BASE_MS"),
        ({"WORKER_RETRY_BASE_MS": "500", "WORKER_RETRY_MAX_MS": "499"}, "must be at least WORKER_RETRY_BASE_MS"),
        ({"WORKER_MAX_ATTEMPTS": "0"}, "WORKER_MAX_ATTEMPTS"),
        ({"WORKER_RETRY_JITTER": "yes"}, "WORKER_RETRY_JITTER"),
    ],
)
def test_invalid_retry_config(overrides, message):
    with pytest.raises(ConfigError, match=message):
        Config.from_env(_env(**overrides))


def test_db_and_poll_defaults():
    config = Config.from_env(_env())
    assert (config.db_command_timeout_ms, config.max_poll_interval_ms) == (10000, 300000)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"WORKER_DB_COMMAND_TIMEOUT_MS": "5000"}, "must be greater than WORKER_LOCK_TIMEOUT_MS"),
        ({"WORKER_LOCK_TIMEOUT_MS": "300", "WORKER_DB_COMMAND_TIMEOUT_MS": "300"}, "must be greater than"),
        ({"WORKER_DB_COMMAND_TIMEOUT_MS": "0"}, "WORKER_DB_COMMAND_TIMEOUT_MS"),
        ({"WORKER_DB_COMMAND_TIMEOUT_MS": "3600001"}, "WORKER_DB_COMMAND_TIMEOUT_MS"),
        ({"WORKER_MAX_POLL_INTERVAL_MS": "0"}, "WORKER_MAX_POLL_INTERVAL_MS"),
        ({"WORKER_MAX_POLL_INTERVAL_MS": "2147483648"}, "WORKER_MAX_POLL_INTERVAL_MS"),
    ],
)
def test_invalid_db_and_poll_config(overrides, message):
    with pytest.raises(ConfigError, match=message):
        Config.from_env(_env(**overrides))


def test_dlq_defaults():
    config = Config.from_env(_env())
    assert (config.dlq_topic, config.dlq_timeout_ms) == ("ledger.payments.dlq.v1", 10000)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"WORKER_DLQ_TOPIC": "ledger.payments.v1"}, "must differ from LEDGER_PAYMENTS_TOPIC"),
        ({"LEDGER_PAYMENTS_TOPIC": "t", "WORKER_DLQ_TOPIC": "t"}, "must differ"),
        ({"WORKER_DLQ_TOPIC": "dead letters"}, "WORKER_DLQ_TOPIC must be 1-249 characters"),
        ({"WORKER_DLQ_TOPIC": "d" * 250}, "WORKER_DLQ_TOPIC must be 1-249 characters"),
        ({"WORKER_DLQ_TOPIC": ".."}, "WORKER_DLQ_TOPIC must be 1-249 characters"),
        ({"WORKER_DLQ_TIMEOUT_MS": "0"}, "WORKER_DLQ_TIMEOUT_MS"),
        ({"WORKER_DLQ_TIMEOUT_MS": "600001"}, "WORKER_DLQ_TIMEOUT_MS"),
    ],
)
def test_invalid_dlq_config(overrides, message):
    with pytest.raises(ConfigError, match=message):
        Config.from_env(_env(**overrides))


def test_fail_offset_commit_follows_the_arming_rules():
    armed = Config.from_env(_env(FAULT_POINT="FAIL_OFFSET_COMMIT", FAULT_PAYMENT_ID=PAYMENT))
    assert armed.fault_armed and armed.fault_point == "FAIL_OFFSET_COMMIT"
    with pytest.raises(ConfigError, match="FAULT_PAYMENT_ID is not"):
        Config.from_env(_env(FAULT_POINT="FAIL_OFFSET_COMMIT"))


def test_raise_before_commit_follows_the_arming_rules():
    armed = Config.from_env(_env(FAULT_POINT="RAISE_BEFORE_COMMIT", FAULT_PAYMENT_ID=PAYMENT))
    assert armed.fault_armed and armed.fault_point == "RAISE_BEFORE_COMMIT"
    with pytest.raises(ConfigError, match="FAULT_PAYMENT_ID is not"):
        Config.from_env(_env(FAULT_POINT="RAISE_BEFORE_COMMIT"))


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
