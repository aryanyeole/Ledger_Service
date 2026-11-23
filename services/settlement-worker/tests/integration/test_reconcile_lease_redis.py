"""The reconciliation lease against a real redis:7.4.11 (D-144), with one real worker process.

The worker has REDIS_URL, RECONCILE_INTERVAL_MS=200 and RECONCILE_LOCK_TTL_MS=3000, with DB timeouts that meet the
bound exactly (2 x 1000 + 1000 = 3000). Its first tick acquires the lease and runs; every tick for the rest of the
lease's 3 s skips with no DB call. Within 2.5 s of the first reconciliation_completed there is exactly that one and
at least 8 reconciliation_skipped, and the raw key holds host:pid:uuid with PTTL in (0, 3000]. (Spacing between runs
is proved via as_of in T13, with multi-worker and dead-holder behaviour.)
"""

import re
import time
from datetime import timedelta

from support import cache_kit, stack
from support.worker_process import start_worker

UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
WINDOW = timedelta(seconds=2.5)


def test_one_worker_runs_once_per_lease_and_skips_the_rest(migrated_db, kafka, tmp_path):
    with stack.redis() as redis:
        with start_worker(
            database=migrated_db,
            bootstrap_servers=kafka.bootstrap_servers,
            group="reconcile-lease-redis",
            log_dir=tmp_path,
            extra_env={
                "REDIS_URL": redis.url,
                "RECONCILE_INTERVAL_MS": "200",
                "RECONCILE_LOCK_TTL_MS": "3000",
                "WORKER_LOCK_TIMEOUT_MS": "500",
                "WORKER_DB_COMMAND_TIMEOUT_MS": "1000",
                "RECONCILE_STATEMENT_TIMEOUT_MS": "1000",
            },
        ) as worker:
            started = worker.wait_for("worker_started")
            assert (started["reconcile_lease"], started["reconcile_lock_ttl_ms"]) == ("on", 3000), worker.diagnostics()
            first = worker.wait_for("reconciliation_completed", timeout=60)

            async def lease(client):
                return await client.get("ledger:v1:reconcile:lease"), await client.pttl("ledger:v1:reconcile:lease")

            value, pttl = cache_kit.raw_redis(redis.url, lease)
            assert value is not None and re.fullmatch(rf"[^:]+:\d+:{UUID}", value), value
            assert 0 < pttl <= 3000, pttl

            # Let the window pass, then count by the worker's own timestamps.
            end = cache_kit.timestamp(first) + WINDOW
            while worker.process.poll() is None and not any(
                cache_kit.timestamp(e) > end for e in worker.lines()
            ):
                time.sleep(0.1)
            in_window = [e for e in worker.lines() if cache_kit.timestamp(first) <= cache_kit.timestamp(e) <= end]
            completed = [e for e in in_window if e["event"] == "reconciliation_completed"]
            skipped = [e for e in in_window if e["event"] == "reconciliation_skipped"]
            print(f"RECONCILE_LEASE window={WINDOW.total_seconds()}s completed={len(completed)} skipped={len(skipped)} "
                  f"pttl={pttl} value={value}")
            assert len(completed) == 1, worker.diagnostics()
            assert len(skipped) >= 8, worker.diagnostics()
            assert all(e["lease_ttl_ms"] == 3000 for e in skipped)
            assert worker.events("reconciliation_lock_unavailable") == [], worker.diagnostics()
            assert worker.stop() == 0
