"""The stack builders' container startup (BUG-009, BUG-010), on real containers.

BUG-010: a start that fails once the container is up must still stop it, for Kafka and Postgres.
The failure is forced by wrapping DockerContainer.start, which PostgresContainer reaches through
super().start(), so that it raises right after the container has started.
BUG-009: Docker's logs API can answer late and with stale output. With DockerContainer.get_logs made
slow and stale, kafka() must still yield a working broker: its readiness gate never reads logs.
The gate's own failure is bounded: against an address nothing listens on, it gives up by its
deadline plus at most one attempt's bound, naming the last error. The log fetch for a failed wait's
message is bounded too.
"""

import asyncio
import subprocess
import time

import pytest
from aiokafka.admin import AIOKafkaAdminClient
from testcontainers.core.container import DockerContainer

from support import stack

SLOW_LOGS_SECONDS = 5  # each patched logs call: slower than any healthy one
BLOCKED_LOGS_SECONDS = 30  # a logs call that outlasts the diagnostics bound
MARGIN_SECONDS = 1
STOP_SECONDS = 5  # removing a stopped-on-failure Kafka container, on top of the diagnostics bound


def _track_starts(monkeypatch, image: str, *, fail: bool) -> list[str]:
    """Records the id of every container of image that DockerContainer.start creates, even when the
    start itself raises (so cleanup finds it); with fail, start raises right after the container is up."""
    started: list[str] = []
    original = DockerContainer.start

    def start(self):
        try:
            result = original(self)
        finally:
            if self.image == image and self.get_wrapped_container() is not None:
                started.append(self.get_wrapped_container().id)
        if fail and self.image == image:
            raise RuntimeError(f"forced failure right after {image} started")
        return result

    monkeypatch.setattr(DockerContainer, "start", start)
    return started


def _running(container_id: str) -> bool:
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container_id], capture_output=True, text=True, timeout=60
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def _remove(container_ids: list[str]) -> None:
    """So that a red run leaves nothing behind; a container already removed is fine."""
    for container_id in container_ids:
        subprocess.run(["docker", "rm", "-f", container_id], capture_output=True, timeout=60)


async def _describe(bootstrap: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.describe_cluster()
    finally:
        await admin.close()


@pytest.mark.parametrize(
    ("image", "builder"),
    [(stack.KAFKA_IMAGE, stack.kafka), (stack.POSTGRES_IMAGE, stack.postgres)],
    ids=["kafka", "postgres"],
)
def test_a_container_whose_start_fails_is_stopped(monkeypatch, image, builder):
    started = _track_starts(monkeypatch, image, fail=True)
    try:
        with pytest.raises(RuntimeError, match="forced failure"):
            with builder():
                pytest.fail(f"{builder.__name__}() yielded although its start failed")
        assert len(started) == 1, started
        assert not _running(started[0]), f"{image} container {started[0][:12]} left running"
    finally:
        _remove(started)


def test_kafka_is_ready_while_the_logs_api_is_slow_and_stale(monkeypatch):
    started = _track_starts(monkeypatch, stack.KAFKA_IMAGE, fail=False)
    calls: list[float] = []

    def slow_stale_logs(self):
        calls.append(time.monotonic())
        time.sleep(SLOW_LOGS_SECONDS)
        return b"stale: nothing logged since the controller started\n", b""

    monkeypatch.setattr(DockerContainer, "get_logs", slow_stale_logs)
    began = time.monotonic()
    try:
        with stack.kafka() as broker:
            ready_s = time.monotonic() - began
            asyncio.run(_describe(broker.bootstrap_servers))
    finally:
        print(f"STACK_SLOW_LOGS elapsed_s={time.monotonic() - began:.1f} logs_calls={len(calls)}")
        _remove(started)
    assert calls == [], "the readiness gate read container logs"
    assert ready_s < stack.BROKER_READY_TIMEOUT_SECONDS, ready_s


def test_the_readiness_gate_gives_up_by_its_deadline_naming_the_last_error():
    deadline_s = 5
    bootstrap = f"127.0.0.1:{stack.free_port()}"  # nothing listens there
    began = time.monotonic()
    with pytest.raises(TimeoutError) as raised:
        asyncio.run(stack._wait_for_broker(bootstrap, began + deadline_s))
    elapsed = time.monotonic() - began
    print(f"BROKER_GATE_FAILURE elapsed_s={elapsed:.2f} message={raised.value}")
    assert deadline_s <= elapsed <= deadline_s + stack.BROKER_ATTEMPT_TIMEOUT_SECONDS + MARGIN_SECONDS, elapsed
    message = str(raised.value)
    assert bootstrap in message and "last error: " in message, message
    assert "no attempt made" not in message, message


def _failing_gate(failed_at: list[float]):
    async def gate(bootstrap: str, deadline: float) -> None:
        failed_at.append(time.monotonic())
        raise TimeoutError(f"forced: Kafka at {bootstrap} not ready")

    return gate


def test_a_failed_wait_carries_the_container_logs(monkeypatch):
    started = _track_starts(monkeypatch, stack.KAFKA_IMAGE, fail=False)
    monkeypatch.setattr(stack, "_wait_for_broker", _failing_gate([]))
    try:
        with pytest.raises(TimeoutError) as raised:
            with stack.kafka():
                pytest.fail("kafka() yielded although its readiness wait failed")
        assert len(started) == 1 and not _running(started[0]), started
    finally:
        _remove(started)
    message = str(raised.value)
    assert message.startswith("forced: Kafka at ") and "--- container logs" in message, message[:500]


def test_a_logs_fetch_that_fails_is_named_in_the_message_not_raised():
    class FailingLogs:
        def get_logs(self):
            raise RuntimeError("docker said no")

    message = stack._with_container_logs("forced: not ready", FailingLogs())
    assert message == "forced: not ready (container logs not fetched: RuntimeError('docker said no'))"


def test_a_failed_wait_whose_logs_call_blocks_raises_without_the_logs(monkeypatch):
    started = _track_starts(monkeypatch, stack.KAFKA_IMAGE, fail=False)
    failed_at: list[float] = []
    monkeypatch.setattr(stack, "_wait_for_broker", _failing_gate(failed_at))
    def blocked_logs(self):
        time.sleep(BLOCKED_LOGS_SECONDS)
        return b"", b""  # a well-formed answer, after the bound has given up on it

    monkeypatch.setattr(DockerContainer, "get_logs", blocked_logs)
    try:
        with pytest.raises(TimeoutError) as raised:
            with stack.kafka():
                pytest.fail("kafka() yielded although its readiness wait failed")
        raised_after = time.monotonic() - failed_at[0]  # includes stopping the container
        assert len(started) == 1 and not _running(started[0]), started
    finally:
        _remove(started)
    print(f"BLOCKED_DIAGNOSTICS raised_after_s={raised_after:.1f}")
    message = str(raised.value)
    assert message.startswith("forced: Kafka at ") and "container logs not fetched" in message, message
    assert raised_after < BLOCKED_LOGS_SECONDS, raised_after
    assert raised_after <= stack.DIAGNOSTIC_LOGS_TIMEOUT_SECONDS + STOP_SECONDS, raised_after
