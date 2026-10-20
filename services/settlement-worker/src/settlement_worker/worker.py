"""Entry point wiring: configuration, logging, stop requests, exit codes (D-081, D-091).

Exit codes: 0 stopped, 1 processing error or an unreachable DB/broker at startup, 2 invalid
configuration or a role refusal, 86 fault (consumer.py).

A stop request (SIGTERM, or EOF on stdin when WORKER_STOP_ON_STDIN_EOF=1) sets an event the loop
checks only between records. signal.signal is used because loop.add_signal_handler isn't
available on Windows. The stdin switch is opt-in: under Compose stdin is /dev/null and reads EOF
at once.
"""

import asyncio
import signal
import sys
import threading

import structlog

from settlement_worker import consumer, events, logging_setup
from settlement_worker.config import Config, ConfigError


def _stop_on_stdin_eof(loop: asyncio.AbstractEventLoop, request_stop) -> None:
    sys.stdin.read()
    try:
        loop.call_soon_threadsafe(request_stop, "stdin_eof")
    except RuntimeError:
        pass  # the loop has already finished


async def _run(config: Config) -> int:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    stop_reason: dict = {}

    def request_stop(why: str) -> None:
        stop_reason.setdefault("why", why)
        stop.set()

    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(request_stop, "sigterm"))
    if config.stop_on_stdin_eof:
        threading.Thread(target=_stop_on_stdin_eof, args=(loop, request_stop), daemon=True).start()
    return await consumer.run(config, stop, stop_reason)


def main() -> int:
    logging_setup.configure()
    try:
        config = Config.from_env()
    except ConfigError as ex:
        events.emit(structlog.get_logger(), events.CONFIG_INVALID, level="error", error=str(ex), correlation_id=None)
        return consumer.EXIT_CONFIG
    return asyncio.run(_run(config))
