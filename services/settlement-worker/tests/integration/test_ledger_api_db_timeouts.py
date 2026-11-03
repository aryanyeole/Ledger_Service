"""ledger-api against a database that accepts TCP and never answers (BUG-012, D-132), out of process.

A migrated Postgres container is docker-paused: its published port still accepts connections, but
nothing answers. Before D-132 the jar blocked forever in pgjdbc's doAuthentication under Flyway, in
server and in migrate-only mode alike. With loginTimeout 10 s from org/postgresql/driverconfig.properties
it must give up and exit 1 (D-083) within EXIT_BOUND_SECONDS, saying why. If it doesn't, the test
takes a thread dump before killing it, so the failure shows where it stopped.
"""

import subprocess
import time

import pytest

from support import stack

EXIT_BOUND_SECONDS = 30  # loginTimeout (10 s) plus the JVM's start and Spring's context up to Flyway


@pytest.fixture(scope="module")
def paused_database():
    jar = stack.find_jar()
    with stack.postgres() as database:
        stack.migrate(database, jar)  # migrated while it answers, then paused
        subprocess.run(["docker", "pause", database.container_id], check=True, capture_output=True, timeout=60)
        try:
            yield database, jar
        finally:
            subprocess.run(["docker", "unpause", database.container_id], check=False, capture_output=True, timeout=60)


@pytest.mark.parametrize("mode", ["server", "migrate-only"])
def test_a_database_that_never_answers_fails_startup_within_the_login_timeout(paused_database, mode, tmp_path):
    database, jar = paused_database
    if mode == "server":
        command = stack.server_command(database, jar, stack.free_port(), "0" * 64)
    else:
        command = stack.migrate_command(database, jar)
    log_path = tmp_path / f"ledger-api-{mode}.log"
    with open(log_path, "wb") as log:
        began = time.monotonic()
        process = subprocess.Popen(command, env=stack.jar_env(), stdout=log, stderr=subprocess.STDOUT)
    try:
        code = process.wait(timeout=EXIT_BOUND_SECONDS)
    except subprocess.TimeoutExpired:
        dump = stack.thread_dump(process.pid, tmp_path / "ledger-api-threads.txt")
        process.kill()
        process.wait(timeout=30)
        pytest.fail(
            f"ledger-api ({mode}) did not exit within {EXIT_BOUND_SECONDS} s against a stalled database:\n"
            f"{log_path.read_text(errors='replace')[-3000:]}\n{dump}"
        )
    elapsed = time.monotonic() - began
    text = log_path.read_text(errors="replace")
    print(f"STALLED_DB_STARTUP mode={mode} exit={code} elapsed_s={elapsed:.1f}")
    assert code == 1, text[-3000:]
    assert "Connection attempt timed out" in text, text[-3000:]
