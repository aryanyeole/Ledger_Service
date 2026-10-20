"""The entry point refuses to start without a valid configuration (D-081, D-090).

Its only output is one JSON line on stdout, config_invalid at error level, with nothing on
stderr, and the exit code is 2.
"""

import json
import os
import subprocess
import sys


def test_entrypoint_refuses_to_start_without_configuration():
    env = {k: v for k, v in os.environ.items() if k != "SETTLEMENT_WORKER_PASSWORD" and not k.startswith("FAULT_")}
    result = subprocess.run(
        [sys.executable, "-m", "settlement_worker"],
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )

    assert result.returncode == 2, result.stdout + result.stderr
    assert result.stderr == ""

    lines = result.stdout.splitlines()
    assert len(lines) == 1, lines

    record = json.loads(lines[0])
    assert set(record) == {"event", "error", "service", "correlation_id", "level", "timestamp"}
    assert record["event"] == "config_invalid"
    assert record["error"] == "SETTLEMENT_WORKER_PASSWORD is required"
    assert record["service"] == "settlement-worker"
    assert record["correlation_id"] is None
    assert record["level"] == "error"
