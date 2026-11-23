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
from settlement_worker.config import Config, ConfigError, redis_address

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
            "processor_outage": False,
            "reconcile_interval_ms": None,
            "balance_invalidation": "no_redis",
            "reconcile_lease": "off",
            "reconcile_lock_ttl_ms": None,
            "correlation_id": None,
        }
    ]


def test_processor_defaults():
    config = Config.from_env(_env())
    assert (config.processor_timeout_ms, config.processor_outage) == (2000, False)
    tuned = Config.from_env(_env(WORKER_PROCESSOR_TIMEOUT_MS="60000", FAKE_PROCESSOR_OUTAGE="1"))
    assert (tuned.processor_timeout_ms, tuned.processor_outage) == (60000, True)
    assert Config.from_env(_env(WORKER_PROCESSOR_TIMEOUT_MS="1", FAKE_PROCESSOR_OUTAGE="0")).processor_outage is False


# D-112: an invalid processor setting is refused (config_invalid, exit 2).
@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"WORKER_PROCESSOR_TIMEOUT_MS": "0"}, "WORKER_PROCESSOR_TIMEOUT_MS"),
        ({"WORKER_PROCESSOR_TIMEOUT_MS": "60001"}, "WORKER_PROCESSOR_TIMEOUT_MS"),
        ({"WORKER_PROCESSOR_TIMEOUT_MS": "2s"}, "WORKER_PROCESSOR_TIMEOUT_MS"),
        ({"FAKE_PROCESSOR_OUTAGE": "yes"}, "FAKE_PROCESSOR_OUTAGE"),
        ({"FAKE_PROCESSOR_OUTAGE": "true"}, "FAKE_PROCESSOR_OUTAGE"),
    ],
)
def test_invalid_processor_config(overrides, message):
    with pytest.raises(ConfigError, match=message):
        Config.from_env(_env(**overrides))


def test_invalid_processor_timeout_exits_2():
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FAULT_", "FAKE_", "WORKER_"))}
    env.update(_env(WORKER_METRICS_PORT="0", WORKER_PROCESSOR_TIMEOUT_MS="0"))
    result = subprocess.run([sys.executable, "-m", "settlement_worker"], capture_output=True, text=True, env=env, timeout=30)
    assert result.returncode == 2, result.stdout + result.stderr
    assert [json.loads(line)["event"] for line in result.stdout.splitlines()] == ["config_invalid"]


# D-112: the outage is announced like an armed fault: WARNING, and worker_started reports it.
@pytest.mark.parametrize("outage, level", [("1", "warning"), ("0", "info")])
def test_worker_started_warns_when_the_processor_outage_is_on(outage, level):
    config = Config.from_env(_env(WORKER_CONSUMER_GROUP="g", FAKE_PROCESSOR_OUTAGE=outage))
    with capture_logs() as logs:
        consumer.announce_start(structlog.get_logger(), config)
    assert logs == [
        {
            "event": "worker_started",
            "log_level": level,
            "consumer_group": "g",
            "fault_point": None,
            "processor_outage": outage == "1",
            "reconcile_interval_ms": None,
            "balance_invalidation": "no_redis",
            "reconcile_lease": "off",
            "reconcile_lock_ttl_ms": None,
            "correlation_id": None,
        }
    ]


def test_reconcile_defaults():
    config = Config.from_env(_env())
    assert (config.reconcile_interval_ms, config.reconcile_statement_timeout_ms) == (None, 30000)
    assert Config.from_env(_env(RECONCILE_INTERVAL_MS="")).reconcile_interval_ms is None
    assert Config.from_env(_env(RECONCILE_INTERVAL_MS="  ")).reconcile_interval_ms is None
    tuned = Config.from_env(_env(RECONCILE_INTERVAL_MS="100", RECONCILE_STATEMENT_TIMEOUT_MS="1"))
    assert (tuned.reconcile_interval_ms, tuned.reconcile_statement_timeout_ms) == (100, 1)
    top = Config.from_env(_env(RECONCILE_INTERVAL_MS="86400000", RECONCILE_STATEMENT_TIMEOUT_MS="600000"))
    assert (top.reconcile_interval_ms, top.reconcile_statement_timeout_ms) == (86_400_000, 600_000)


# D-114: an invalid reconciliation setting is refused (config_invalid, exit 2); integer ms only (D-093).
@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"RECONCILE_INTERVAL_MS": "99"}, "RECONCILE_INTERVAL_MS"),
        ({"RECONCILE_INTERVAL_MS": "86400001"}, "RECONCILE_INTERVAL_MS"),
        ({"RECONCILE_INTERVAL_MS": "1.5"}, "RECONCILE_INTERVAL_MS"),
        ({"RECONCILE_INTERVAL_MS": "60s"}, "RECONCILE_INTERVAL_MS"),
        ({"RECONCILE_INTERVAL_MS": "-1000"}, "RECONCILE_INTERVAL_MS"),
        ({"RECONCILE_STATEMENT_TIMEOUT_MS": "0"}, "RECONCILE_STATEMENT_TIMEOUT_MS"),
        ({"RECONCILE_STATEMENT_TIMEOUT_MS": "600001"}, "RECONCILE_STATEMENT_TIMEOUT_MS"),
        ({"RECONCILE_STATEMENT_TIMEOUT_MS": ""}, "RECONCILE_STATEMENT_TIMEOUT_MS"),
    ],
)
def test_invalid_reconcile_config(overrides, message):
    with pytest.raises(ConfigError, match=message):
        Config.from_env(_env(**overrides))


def test_invalid_reconcile_interval_exits_2():
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FAULT_", "FAKE_", "WORKER_", "RECONCILE_"))}
    env.update(_env(WORKER_METRICS_PORT="0", RECONCILE_INTERVAL_MS="99"))
    result = subprocess.run([sys.executable, "-m", "settlement_worker"], capture_output=True, text=True, env=env, timeout=30)
    assert result.returncode == 2, result.stdout + result.stderr
    lines = [json.loads(line) for line in result.stdout.splitlines()]
    assert [line["event"] for line in lines] == ["config_invalid"], lines
    assert "RECONCILE_INTERVAL_MS" in lines[0]["error"]


def test_worker_started_reports_the_reconcile_interval():
    config = Config.from_env(_env(WORKER_CONSUMER_GROUP="g", RECONCILE_INTERVAL_MS="60000"))
    with capture_logs() as logs:
        consumer.announce_start(structlog.get_logger(), config)
    assert [(log["reconcile_interval_ms"], log["log_level"]) for log in logs] == [(60000, "info")]


# WORKER_BALANCE_INVALIDATION (D-142, D-152): 0 or 1, default 1.
def test_balance_invalidation_defaults_to_on():
    assert Config.from_env(_env()).balance_invalidation is True
    assert Config.from_env(_env(WORKER_BALANCE_INVALIDATION="1")).balance_invalidation is True
    assert Config.from_env(_env(WORKER_BALANCE_INVALIDATION="0")).balance_invalidation is False


@pytest.mark.parametrize("raw", ["", "2", "true", "on", " 1"])
def test_invalid_balance_invalidation_is_refused(raw):
    with pytest.raises(ConfigError, match="WORKER_BALANCE_INVALIDATION must be 0 or 1"):
        Config.from_env(_env(WORKER_BALANCE_INVALIDATION=raw))


def test_invalid_balance_invalidation_exits_2():
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FAULT_", "FAKE_", "WORKER_", "REDIS_"))}
    env.update(_env(WORKER_METRICS_PORT="0", WORKER_BALANCE_INVALIDATION="yes"))
    result = subprocess.run([sys.executable, "-m", "settlement_worker"], capture_output=True, text=True, env=env, timeout=30)
    assert result.returncode == 2, result.stdout + result.stderr
    lines = [json.loads(line) for line in result.stdout.splitlines()]
    assert [line["event"] for line in lines] == ["config_invalid"], lines
    assert "WORKER_BALANCE_INVALIDATION" in lines[0]["error"]


# worker_started.balance_invalidation (D-152): no_redis and on are INFO; off (TTL-only) warns like an armed fault.
@pytest.mark.parametrize(
    "overrides, status, level, invalidates",
    [
        ({}, "no_redis", "info", False),
        ({"WORKER_BALANCE_INVALIDATION": "0"}, "no_redis", "info", False),
        ({"REDIS_URL": "redis://localhost:6379"}, "on", "info", True),
        ({"REDIS_URL": "redis://localhost:6379", "WORKER_BALANCE_INVALIDATION": "0"}, "off", "warning", False),
    ],
)
def test_worker_started_reports_balance_invalidation(overrides, status, level, invalidates):
    config = Config.from_env(_env(WORKER_CONSUMER_GROUP="g", **overrides))
    assert config.balance_invalidation_status == status
    assert config.invalidates is invalidates
    with capture_logs() as logs:
        consumer.announce_start(structlog.get_logger(), config)
    assert [(log["balance_invalidation"], log["log_level"]) for log in logs] == [(status, level)]


# D-148: the new fault point follows the D-085 arming rules.
def test_after_db_before_invalidation_is_a_fault_point():
    config = Config.from_env(_env(FAULT_POINT="AFTER_DB_BEFORE_INVALIDATION", FAULT_PAYMENT_ID=PAYMENT))
    assert config.fault_point == "AFTER_DB_BEFORE_INVALIDATION"
    assert config.fault_armed
    with capture_logs() as logs:
        consumer.announce_start(structlog.get_logger(), config)
    assert [(log["fault_point"], log["log_level"]) for log in logs] == [("AFTER_DB_BEFORE_INVALIDATION", "warning")]
    with pytest.raises(ConfigError, match="FAULT_POINT is set but FAULT_PAYMENT_ID is not"):
        Config.from_env(_env(FAULT_POINT="AFTER_DB_BEFORE_INVALIDATION"))


# REDIS_URL (D-137): ledger-api's rule for ledger.redis.url, the same cases as LedgerRedisPropertiesTest.
@pytest.mark.parametrize("raw", [None, "", "   "])
def test_redis_url_unset_means_no_client(raw):
    assert Config.from_env(_env(REDIS_URL=raw)).redis_url is None


@pytest.mark.parametrize(
    ("raw", "host", "port"),
    [
        ("redis://localhost", "localhost", 6379),
        ("redis://localhost:6380", "localhost", 6380),
        ("redis://redis:6379/0", "redis", 6379),
        ("redis://127.0.0.1:1", "127.0.0.1", 1),
        ("redis://h:65535", "h", 65535),
        ("REDIS://Host", "host", 6379),
    ],
)
def test_redis_url_accepted(raw, host, port):
    config = Config.from_env(_env(REDIS_URL=raw))
    assert config.redis_url == raw
    assert redis_address(config.redis_url) == (host, port)


@pytest.mark.parametrize(
    "raw",
    [
        "localhost:6379",
        "rediss://localhost",
        "http://localhost",
        "redis://",
        "redis://:6379",
        "redis://localhost:0",
        "redis://localhost:65536",
        "redis://localhost:abc",
        "redis://localhost/1",
        "redis://localhost/0/x",
        "redis://localhost?db=0",
        "redis://localhost#x",
        "redis://user@localhost",
        "redis://:secret@localhost",
        "redis://user:secret@localhost:6379",
    ],
)
def test_redis_url_rejected(raw):
    with pytest.raises(ConfigError, match=r"^REDIS_URL "):
        Config.from_env(_env(REDIS_URL=raw))


def test_redis_url_message_never_echoes_user_info():
    with pytest.raises(ConfigError) as caught:
        Config.from_env(_env(REDIS_URL="redis://admin:hunter2@localhost:6379"))
    assert str(caught.value) == "REDIS_URL must not contain user info"
    assert "hunter2" not in str(caught.value) and "admin" not in str(caught.value)


def test_malformed_redis_url_exits_2():
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FAULT_", "REDIS_"))}
    env.update(_env(WORKER_METRICS_PORT="0", REDIS_URL="redis://user:hunter2@localhost:6379"))
    result = subprocess.run([sys.executable, "-m", "settlement_worker"], capture_output=True, text=True, env=env, timeout=30)
    assert result.returncode == 2, result.stdout + result.stderr
    lines = [json.loads(line) for line in result.stdout.splitlines()]
    assert [line["event"] for line in lines] == ["config_invalid"]
    assert lines[0]["error"] == "REDIS_URL must not contain user info"
    assert "hunter2" not in result.stdout + result.stderr

# RECONCILE_LOCK_TTL_MS (D-144): read only while the lease is active (REDIS_URL and RECONCILE_INTERVAL_MS both set),
# default = the interval, and at least 2 x WORKER_DB_COMMAND_TIMEOUT_MS + RECONCILE_STATEMENT_TIMEOUT_MS.
LEASE = {"REDIS_URL": "redis://localhost:6379", "RECONCILE_INTERVAL_MS": "60000"}


def test_the_lock_ttl_defaults_to_the_interval_while_the_lease_is_active():
    config = Config.from_env(_env(**LEASE))
    assert (config.reconcile_lease, config.reconcile_lock_ttl_ms) == (True, 60000)
    assert Config.from_env(_env(**LEASE, RECONCILE_LOCK_TTL_MS="90000")).reconcile_lock_ttl_ms == 90000


def test_the_lock_ttl_bound_accepts_equality_and_refuses_one_below():
    # 2 x 1000 + 1000 = 3000.
    timeouts = {"WORKER_LOCK_TIMEOUT_MS": "500", "WORKER_DB_COMMAND_TIMEOUT_MS": "1000",
                "RECONCILE_STATEMENT_TIMEOUT_MS": "1000"}
    assert Config.from_env(_env(**LEASE, **timeouts, RECONCILE_LOCK_TTL_MS="3000")).reconcile_lock_ttl_ms == 3000
    with pytest.raises(ConfigError) as caught:
        Config.from_env(_env(**LEASE, **timeouts, RECONCILE_LOCK_TTL_MS="2999"))
    message = str(caught.value)
    assert "RECONCILE_LOCK_TTL_MS (2999)" in message
    assert "WORKER_DB_COMMAND_TIMEOUT_MS (1000)" in message
    assert "RECONCILE_STATEMENT_TIMEOUT_MS (1000)" in message


def test_the_default_lock_ttl_below_the_bound_is_refused():
    # Defaults: 2 x 10000 + 30000 = 50000 > a 1000 ms interval.
    with pytest.raises(ConfigError, match=r"RECONCILE_LOCK_TTL_MS \(1000\)"):
        Config.from_env(_env(REDIS_URL="redis://localhost:6379", RECONCILE_INTERVAL_MS="1000"))


@pytest.mark.parametrize(
    "overrides",
    [
        {"RECONCILE_INTERVAL_MS": "200"},  # no REDIS_URL: the M4 proofs' intervals
        {"RECONCILE_INTERVAL_MS": "1000", "RECONCILE_LOCK_TTL_MS": "1"},
        {"RECONCILE_INTERVAL_MS": "1000", "RECONCILE_LOCK_TTL_MS": "not-a-number"},
        {"REDIS_URL": "redis://localhost:6379", "RECONCILE_LOCK_TTL_MS": "not-a-number"},  # no interval
    ],
)
def test_the_lock_ttl_is_ignored_while_the_lease_is_inactive(overrides):
    config = Config.from_env(_env(**overrides))
    assert (config.reconcile_lease, config.reconcile_lock_ttl_ms) == (False, None)


def test_an_invalid_lock_ttl_exits_2():
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FAULT_", "FAKE_", "WORKER_", "RECONCILE_", "REDIS_"))}
    env.update(_env(WORKER_METRICS_PORT="0", REDIS_URL="redis://localhost:6379", RECONCILE_INTERVAL_MS="1000"))
    result = subprocess.run([sys.executable, "-m", "settlement_worker"], capture_output=True, text=True, env=env, timeout=30)
    assert result.returncode == 2, result.stdout + result.stderr
    lines = [json.loads(line) for line in result.stdout.splitlines()]
    assert [line["event"] for line in lines] == ["config_invalid"], lines
    for name in ("RECONCILE_LOCK_TTL_MS", "WORKER_DB_COMMAND_TIMEOUT_MS", "RECONCILE_STATEMENT_TIMEOUT_MS"):
        assert name in lines[0]["error"]


@pytest.mark.parametrize(
    "overrides, lease, ttl",
    [({}, "off", None), ({"RECONCILE_INTERVAL_MS": "60000"}, "off", None), (LEASE, "on", 60000)],
)
def test_worker_started_reports_the_lease(overrides, lease, ttl):
    config = Config.from_env(_env(WORKER_CONSUMER_GROUP="g", **overrides))
    with capture_logs() as logs:
        consumer.announce_start(structlog.get_logger(), config)
    assert [(log["reconcile_lease"], log["reconcile_lock_ttl_ms"], log["log_level"]) for log in logs] == [
        (lease, ttl, "info")
    ]
