"""Balance invalidation after a committed settle (D-142, D-148, D-152, docs/design/cache-invalidation.md §2.1, §3.7).

The worker never reads a cached balance; it only invalidates. After settlement.handle has returned a committed
outcome for a known payment (settled, released, duplicate_skipped, payment_not_authorized), and before
AFTER_DB_BEFORE_OFFSET and the offset commit, one EVAL of the invalidation script deletes the value and replaces the
token of customer, merchant, ESCROW and PLATFORM_FEES. It runs outside any DB transaction (invariant 5), is bounded
by INVALIDATION_BOUND_MS and is never retried; there is no bypass (invalidations never honour one, D-137). Nothing it
does can reach the D-107 classifier: invalidate() never raises, and BalanceInvalidator.after_commit catches
everything, its own logging and metrics included, so a systematic bug can't turn every payment into retries and a
dead letter. A failure is cache_invalidation_failed (payment_id, correlation_id, error_class; never keys, ids or
values) and settlement_balance_invalidations_total{result="failed"}; the stale value lives at most the API's value
TTL (§5 row 5).
"""

import asyncio
import uuid
from collections.abc import Iterable
from dataclasses import dataclass

import structlog

from settlement_worker import events, metrics, redis_client

# Byte-identical to ledger-api's BalanceCacheScripts.INVALIDATE and to the block between the script:invalidate
# markers in docs/design/cache-invalidation.md §2.1 (D-143; tests/unit/test_balance_cache_script.py).
INVALIDATE = """-- ledger:v1 balance invalidation (D-142). KEYS: ledger:v1:bal:{id}, ledger:v1:bal-gen:{id} per account.
-- ARGV[1]: a fresh token for this call.
for i = 1, #KEYS, 2 do
  redis.call('DEL', KEYS[i])
  redis.call('SET', KEYS[i + 1], ARGV[1], 'PX', 300000)
end
return #KEYS
"""

# A connect plus one command: integer milliseconds, divided at the call site (D-093).
INVALIDATION_BOUND_MS = redis_client.CONNECT_TIMEOUT_MS + redis_client.COMMAND_TIMEOUT_MS

SENT = "sent"
FAILED = "failed"


@dataclass(frozen=True)
class Invalidation:
    """result is SENT or FAILED; error_class names the exception for FAILED, else None."""

    result: str
    error_class: str | None = None


def keys(account_ids: Iterable[int]) -> list[str]:
    """ledger:v1:bal:{id}, ledger:v1:bal-gen:{id} for each distinct id, ascending (as BalanceCacheInvalidator.keys)."""
    out: list[str] = []
    for account_id in sorted(set(account_ids)):
        out.append(f"ledger:v1:bal:{account_id}")
        out.append(f"ledger:v1:bal-gen:{account_id}")
    return out


async def invalidate(client, account_ids: Iterable[int]) -> Invalidation:
    """One EVAL with a fresh token, bounded by INVALIDATION_BOUND_MS. Never raises."""
    try:
        script_keys = keys(account_ids)
        token = str(uuid.uuid4())
        async with asyncio.timeout(INVALIDATION_BOUND_MS / 1000):
            await client.eval(INVALIDATE, len(script_keys), *script_keys, token)
        return Invalidation(SENT)
    except Exception as ex:  # noqa: BLE001 - every failure is a failed invalidation (D-135), never a record failure
        return Invalidation(FAILED, type(ex).__name__)


class BalanceInvalidator:
    """The consumer's post-commit step: ESCROW's and PLATFORM_FEES' ids are memoized at startup (D-048: they
    never change), the customer's and merchant's come from the validated event."""

    def __init__(self, client, escrow_id: int, fees_id: int) -> None:
        self._client = client
        self._escrow_id = escrow_id
        self._fees_id = fees_id

    async def after_commit(self, event) -> None:
        """Never raises: an exception here would be Unclassified (D-107) and re-run the record (D-152)."""
        counted = False
        try:
            try:
                outcome = await invalidate(
                    self._client,
                    (event.customer_account_id, event.merchant_account_id, self._escrow_id, self._fees_id),
                )
            except Exception as ex:  # noqa: BLE001 - invalidate() never raises; this is belt and braces
                outcome = Invalidation(FAILED, type(ex).__name__)
            metrics.BALANCE_INVALIDATIONS.labels(result=outcome.result).inc()
            counted = True
            if outcome.result == FAILED:
                _failed_event(event, outcome.error_class)
        except Exception as ex:  # noqa: BLE001 - its own logging and metrics included
            _best_effort_failure(event, type(ex).__name__, counted)


def _failed_event(event, error_class: str | None) -> None:
    events.emit(
        structlog.get_logger(),
        events.CACHE_INVALIDATION_FAILED,
        level="warning",
        payment_id=str(event.payment_id),
        error_class=error_class,
        correlation_id=event.correlation_id,
    )


def _best_effort_failure(event, error_class: str, counted: bool) -> None:
    """Counts {result="failed"} unless the outcome was already counted, and logs; each step swallows its own error."""
    if not counted:
        try:
            metrics.BALANCE_INVALIDATIONS.labels(result=FAILED).inc()
        except Exception:  # noqa: BLE001,S110 - nothing may escape after_commit
            pass
    try:
        _failed_event(event, error_class)
    except Exception:  # noqa: BLE001,S110 - nothing may escape after_commit
        pass
