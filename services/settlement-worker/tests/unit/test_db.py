"""D-109: which exceptions from a DB call are a DB timeout or a lost connection (pure)."""

import asyncpg

from settlement_worker.db import DbTimeout, DbUnavailable, WorkerRoleError, as_db_failure


def _raised_during(outer: BaseException, inner: BaseException) -> BaseException:
    """outer raised while handling inner, as asyncpg's ROLLBACK fails after a client timeout."""
    try:
        try:
            raise inner
        except BaseException:
            raise outer
    except BaseException as ex:
        return ex


def test_a_timeout_is_db_timeout():
    wrapped = as_db_failure(TimeoutError())
    assert isinstance(wrapped, DbTimeout)
    assert isinstance(wrapped.__cause__, TimeoutError)


def test_a_timeout_in_the_chain_is_db_timeout():
    rollback = _raised_during(asyncpg.InterfaceError("another operation is cancelling"), TimeoutError())
    wrapped = as_db_failure(rollback)
    assert isinstance(wrapped, DbTimeout)
    assert wrapped.__cause__ is rollback


def test_another_os_error_is_db_unavailable():
    assert isinstance(as_db_failure(ConnectionRefusedError("refused")), DbUnavailable)


def test_anything_else_is_unchanged():
    for exc in (asyncpg.LockNotAvailableError("lock"), RuntimeError("bug"), WorkerRoleError("role")):
        assert as_db_failure(exc) is exc
