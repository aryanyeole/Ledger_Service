"""The worker's Postgres connection (D-082, D-109, mirroring D-036 on the Java side).

The worker refuses to run unless it is connected as settlement_worker and that role is not a
superuser: its grants are half of the ledger's defences (D-033), and a superuser would bypass them.
The check runs on every connect, the reconnects after a lost connection included.

Every call on the connection, BEGIN/COMMIT/ROLLBACK included, runs under the command timeout:
asyncpg's transaction context issues them with Connection.execute and no timeout, which falls back
to the connection's command_timeout. Connecting uses the same value as its timeout, so a paused
server can't hold a reconnect for asyncpg's 60 s default. A lost connection is terminate()d, never
close()d: close() waits on the server, and a paused or partitioned server doesn't answer.
"""

import asyncpg
import structlog

from settlement_worker import events

ROLE = "settlement_worker"


class WorkerRoleError(RuntimeError):
    pass


class DbTimeout(Exception):
    """A DB call (or connect) exceeded WORKER_DB_COMMAND_TIMEOUT_MS: connection-class (D-107, D-109)."""


class DbUnavailable(Exception):
    """A socket-level failure from a DB call or connect: connection-class (D-107, D-109)."""


def _chain(exc: BaseException):
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        yield exc
        exc = exc.__cause__ or exc.__context__


def as_db_failure(exc: BaseException) -> BaseException:
    """For an exception raised by a DB call: DbTimeout if its chain holds a TimeoutError (a client
    timeout surfaces as the ROLLBACK's InterfaceError, with the TimeoutError as its context),
    DbUnavailable for any other OSError, else the exception itself. Only DB call sites use this:
    a timeout anywhere else is never a DbTimeout (D-109)."""
    if isinstance(exc, (DbTimeout, DbUnavailable, WorkerRoleError)):
        return exc
    for link in _chain(exc):
        if isinstance(link, TimeoutError):
            wrapped = DbTimeout(f"DB call timed out ({type(exc).__name__}: {exc})")
            wrapped.__cause__ = exc
            return wrapped
    if isinstance(exc, OSError):
        wrapped = DbUnavailable(f"{type(exc).__name__}: {exc}")
        wrapped.__cause__ = exc
        return wrapped
    return exc


async def connect(*, timeout_ms: int | None = None, **params) -> asyncpg.Connection:
    if timeout_ms is not None:
        params = {**params, "timeout": timeout_ms / 1000, "command_timeout": timeout_ms / 1000}
    conn = await asyncpg.connect(**params)
    try:
        await _check_role(conn)
    except BaseException:
        conn.terminate()
        raise
    return conn


async def _check_role(conn: asyncpg.Connection) -> str:
    row = await conn.fetchrow("SELECT current_user AS who, rolsuper FROM pg_roles WHERE rolname = current_user")
    if row is None or row["who"] != ROLE:
        raise WorkerRoleError(f"connected as {row['who'] if row else '?'}, expected {ROLE}")
    if row["rolsuper"]:
        raise WorkerRoleError(f"{ROLE} must not be a superuser")
    return row["who"]


class DbSession:
    """The worker's one connection, reconnected after a loss (D-109). config.db_params() is read on
    every connect, so a test-side Config can change what a reconnect sees (D-129)."""

    def __init__(self, config) -> None:
        self._config = config
        self._conn: asyncpg.Connection | None = None
        self._reconnects = 0

    @property
    def conn(self) -> asyncpg.Connection:
        if self._conn is None:
            raise DbUnavailable("no DB connection")
        return self._conn

    @property
    def broken(self) -> bool:
        return self._conn is None

    def closed(self) -> bool:
        """At the point of a failure: is the connection gone (so an InterfaceError means loss)?"""
        return self._conn is None or self._conn.is_closed()

    async def open(self) -> None:
        """At startup: raw exceptions, so an unreachable DB exits 1 and a role refusal exits 2."""
        self._conn = await connect(timeout_ms=self._config.db_command_timeout_ms, **self._config.db_params())

    def drop(self) -> None:
        """After a connection-class failure: terminate, never wait on the server."""
        if self._conn is not None:
            self._conn.terminate()
            self._conn = None

    async def reconnect(self) -> None:
        """Connects again, the role check included; db_reconnected on success. A refusal raises
        WorkerRoleError; any other failure is raised as a DB failure (as_db_failure)."""
        self._reconnects += 1
        try:
            conn = await connect(timeout_ms=self._config.db_command_timeout_ms, **self._config.db_params())
        except WorkerRoleError:
            raise
        except Exception as ex:  # noqa: BLE001 - classified by the caller
            failure = as_db_failure(ex)
            if failure is ex:
                raise
            raise failure from ex
        self._conn = conn
        events.emit(
            structlog.get_logger(),
            events.DB_RECONNECTED,
            attempt=self._reconnects,
            current_user=ROLE,
            correlation_id=None,
        )
        self._reconnects = 0

    async def close(self) -> None:
        """Bounded: close() with the command timeout, terminate() if it doesn't finish."""
        conn, self._conn = self._conn, None
        if conn is None:
            return
        try:
            await conn.close(timeout=self._config.db_command_timeout_ms / 1000)
        except Exception:  # noqa: BLE001 - a close that fails or times out is aborted
            conn.terminate()
