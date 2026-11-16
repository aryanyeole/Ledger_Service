"""The worker's Redis client (D-136, D-137, docs/design/cache-invalidation.md).

redis-py's asyncio client against database 0 at REDIS_URL's host and port: socket timeout COMMAND_TIMEOUT_MS,
connect timeout CONNECT_TIMEOUT_MS, and no retries. redis-py 8.1.0 retries ConnectionError and TimeoutError 10
times with exponential backoff by default, which would turn a Redis outage into a stall on every settle, so the
retry is set explicitly to none. Default protocol (RESP3). Timeouts are integer milliseconds divided at the call
site, like every other worker timeout (D-093 bans float literals). Nothing calls it before T8 (invalidation) and
T12 (the lease); a failure there is a miss or bypass, never a record failure (D-135).
"""

import redis.asyncio
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from settlement_worker.config import redis_address

COMMAND_TIMEOUT_MS = 100
CONNECT_TIMEOUT_MS = 200


def no_retry() -> Retry:
    return Retry(NoBackoff(), 0)


def create(url: str) -> redis.asyncio.Redis:
    """A client for a REDIS_URL that Config already validated. Constructing it doesn't connect."""
    host, port = redis_address(url)
    return redis.asyncio.Redis(
        host=host,
        port=port,
        db=0,
        socket_timeout=COMMAND_TIMEOUT_MS / 1000,
        socket_connect_timeout=CONNECT_TIMEOUT_MS / 1000,
        retry=no_retry(),
        retry_on_error=[],
        decode_responses=True,
    )
