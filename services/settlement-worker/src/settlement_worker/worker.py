import sys

import structlog

SERVICE_NAME = "settlement-worker"


def configure_logging() -> None:
    """JSON log lines on stdout, one object per line."""
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


async def main() -> None:
    configure_logging()
    log = structlog.get_logger().bind(service=SERVICE_NAME)
    # No Kafka or Postgres yet (M3). The worker never runs migrations (D-006).
    log.info("worker_starting", correlation_id=None)
