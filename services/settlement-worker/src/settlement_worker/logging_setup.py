"""JSON log lines on stdout, each flushed as it is written (D-090).

stdout to a pipe or a file is block-buffered, and os._exit (the fault injection, D-085) skips
flushing buffers, so a line that isn't flushed when it is written can be lost. The worker owns
that guarantee here instead of relying on how structlog's loggers happen to write.
"""

import sys

import structlog

SERVICE_NAME = "settlement-worker"


class LineWriter:
    """A structlog logger that writes each rendered line to the current sys.stdout and flushes it."""

    def msg(self, message: str) -> None:
        out = sys.stdout
        out.write(message + "\n")
        out.flush()

    debug = info = warning = warn = error = critical = exception = fatal = log = msg


def configure() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=lambda *args: LineWriter(),
        cache_logger_on_first_use=True,
    )
    structlog.contextvars.bind_contextvars(service=SERVICE_NAME)
