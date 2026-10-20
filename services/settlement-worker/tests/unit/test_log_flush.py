"""Every log line is flushed as it is written (D-085, D-090).

With stdout on a pipe Python buffers it, and os._exit skips the flush: a line written just before
the fault's os._exit must still reach the reader. PYTHONUNBUFFERED is removed from the child's
environment so the test relies on the worker's own flushing.
"""

import json
import os
import subprocess
import sys

SCRIPT = """
import os
import structlog
from settlement_worker import events, logging_setup

logging_setup.configure()
events.emit(structlog.get_logger(), events.WORKER_STOPPED, reason="probe", correlation_id=None)
os._exit(0)
"""


def test_line_survives_os_exit_on_a_pipe():
    env = {k: v for k, v in os.environ.items() if k != "PYTHONUNBUFFERED"}
    result = subprocess.run([sys.executable, "-c", SCRIPT], stdout=subprocess.PIPE, env=env, timeout=30)
    lines = result.stdout.decode().splitlines()
    assert [json.loads(line)["reason"] for line in lines] == ["probe"]
