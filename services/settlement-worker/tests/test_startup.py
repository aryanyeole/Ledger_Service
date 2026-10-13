import json
import subprocess
import sys


def test_entrypoint_emits_one_json_startup_line():
    result = subprocess.run(
        [sys.executable, "-m", "settlement_worker"],
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""

    lines = result.stdout.splitlines()
    assert len(lines) == 1, lines

    record = json.loads(lines[0])
    assert set(record) == {"event", "service", "correlation_id", "level", "timestamp"}
    assert record["event"] == "worker_starting"
    assert record["service"] == "settlement-worker"
    assert record["correlation_id"] is None
    assert record["level"] == "info"
