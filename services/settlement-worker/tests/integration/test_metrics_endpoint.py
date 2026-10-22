"""The worker's /metrics endpoint (D-113), on real worker processes.

- A worker on port 0 reports the port it bound in metrics_listening, before it joins the consumer
  group, and serves the default registry: 200, text/plain, python_info (process_* metrics exist on
  Linux only and aren't asserted). A stop still exits 0.
- A port that can't be bound is invalid configuration: config_invalid, exit 2, nothing else. The
  holder socket is exclusive on Windows, where the server's SO_REUSEADDR would otherwise let the
  worker bind a port that is already held; on Linux a listening holder is enough.
- A worker that refuses its role opens no port: the role check comes first.
Red evidence (D-051): starting the server before the role check makes the last test fail.
"""

import json
import os
import socket
import subprocess
import sys
import urllib.request
from pathlib import Path

from support import stack
from support.worker_process import start_worker

TESTS = Path(__file__).resolve().parents[1]

# Startup fails before the consumer is created, so these workers never reach a broker.
NO_BROKER = "127.0.0.1:9"


def _events(lines: list[dict]) -> list[str]:
    return [line["event"] for line in lines]


def test_metrics_endpoint(migrated_db, kafka, tmp_path):
    with start_worker(
        database=migrated_db, bootstrap_servers=kafka.bootstrap_servers, group="metrics-endpoint", log_dir=tmp_path
    ) as worker:
        worker.wait_for("worker_started")
        listening = worker.wait_for("metrics_listening")
        assert listening["host"] == "127.0.0.1"
        assert listening["level"] == "info"
        assert listening["correlation_id"] is None
        port = worker.metrics_port()
        assert port > 0
        names = _events(worker.lines())
        assert names.index("metrics_listening") < names.index("worker_started"), names

        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=10) as response:
            status = response.status
            content_type = response.headers["Content-Type"]
            body = response.read().decode("utf-8")
        assert status == 200
        assert content_type.startswith("text/plain"), content_type
        assert "python_info" in body

        assert worker.stop() == 0, worker.diagnostics()
    assert _events(worker.lines())[-1] == "worker_stopped"


def _held_port() -> socket.socket:
    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):  # Windows only
        holder.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
    holder.bind(("127.0.0.1", 0))
    holder.listen()
    return holder


def test_bind_failure_is_config_invalid(migrated_db, tmp_path):
    with _held_port() as holder:
        port = holder.getsockname()[1]
        with start_worker(
            database=migrated_db,
            bootstrap_servers=NO_BROKER,
            group="metrics-bind-failure",
            log_dir=tmp_path,
            extra_env={"WORKER_METRICS_PORT": str(port)},
        ) as worker:
            assert worker.wait_exit(60) == 2, worker.diagnostics()
    lines = worker.lines()
    assert _events(lines) == ["config_invalid"], lines
    assert f"127.0.0.1:{port}" in lines[0]["error"]


def test_role_refusal_opens_no_metrics_port(migrated_db):
    env = {
        k: v
        for k, v in os.environ.items()
        if k != "PYTHONUNBUFFERED" and not k.startswith(("FAULT_", "NAIVE_", "WORKER_"))
    }
    env.update(
        POSTGRES_HOST=migrated_db.host,
        POSTGRES_PORT=str(migrated_db.port),
        POSTGRES_DB=migrated_db.name,
        SETTLEMENT_WORKER_PASSWORD=stack.WORKER_PASSWORD,
        WRONG_ROLE_PASSWORD=stack.APP_PASSWORD,
        KAFKA_BOOTSTRAP_SERVERS=NO_BROKER,
        WORKER_METRICS_PORT="0",
    )
    result = subprocess.run(
        [sys.executable, "-m", "support.wrong_role_worker"],
        cwd=TESTS,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    lines = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    assert "metrics_listening" not in _events(lines), lines
    assert result.returncode == 2, result
    assert _events(lines) == ["config_invalid"], lines
    assert "connected as ledger_api_app, expected settlement_worker" in lines[0]["error"]
    assert result.stderr == ""
