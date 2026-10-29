"""What a record failure is, by its exception (D-107). A pure function: no I/O, no state.

Transient   the dependency or a lock, not the message: retried indefinitely, never dead-lettered.
            connection=True marks DB connection loss: the connection is terminated and the
            retry reconnects first (D-109). An asyncpg InterfaceError is connection loss only
            when the connection is closed at the point of failure (connection_closed);
            otherwise it is a client-side bug and Unclassified. A raw OSError/TimeoutError is
            transient but not connection loss: DB call sites wrap theirs as DbTimeout or
            DbUnavailable (db.as_db_failure), so anything else never reconnects the DB.
Poison      the message itself can never succeed: dead-lettered on its first failure (T7).
Unclassified anything else: dead-lettered at its WORKER_MAX_ATTEMPTS-th Unclassified failure (T7);
            transient failures of the same record don't count towards it (D-112).
Fatal       the deployment or the ledger is broken: processing_failed, exit 1, nothing committed.

A failed offset commit is not a record failure and never comes here: the effect has already
committed, so the commit path handles it (D-107, D-109). The processor's ProcessorUnavailable and
ProcessorTimeout are transient and never connection loss (D-112): never dead-lettered, never a
decline, and the DB connection is left alone.
"""

import enum
from dataclasses import dataclass

import asyncpg

from settlement_worker.db import DbTimeout, DbUnavailable
from settlement_worker.dlq import DlqUnavailable
from settlement_worker.envelope import InvalidEvent
from settlement_worker.processor import ProcessorTimeout, ProcessorUnavailable
from settlement_worker.settlement import EscrowShortfall, EventMismatch, MissingAccount, UnknownPayment


class ErrorClass(enum.StrEnum):
    TRANSIENT = "Transient"
    POISON = "Poison"
    UNCLASSIFIED = "Unclassified"
    FATAL = "Fatal"


@dataclass(frozen=True)
class Failure:
    error_class: ErrorClass
    sqlstate: str | None = None
    connection: bool = False


# Lock and serialization failures on a live connection.
LOCK_SQLSTATES = frozenset({"55P03", "40P01", "40001"})
_CONNECTION_SQLSTATES = frozenset({"57P01", "57P02", "57P03"})
_CONNECTION_CLASSES = ("08", "53")
_FATAL_SQLSTATES = frozenset({"42501", "42P01", "42703", "42883", "3D000"})
_FATAL_CLASSES = ("28", "23")


def classify(exc: BaseException, connection_closed: bool = False) -> Failure:
    if isinstance(exc, (DbTimeout, DbUnavailable)):
        return Failure(ErrorClass.TRANSIENT, connection=True)
    # The dead letter couldn't be produced (D-111): transient; the DB connection is left alone.
    if isinstance(exc, DlqUnavailable):
        return Failure(ErrorClass.TRANSIENT)
    # The processor didn't answer (D-112): transient, the DB connection left alone.
    if isinstance(exc, (ProcessorUnavailable, ProcessorTimeout)):
        return Failure(ErrorClass.TRANSIENT)
    if isinstance(exc, (InvalidEvent, UnknownPayment, EventMismatch)):
        return Failure(ErrorClass.POISON)
    if isinstance(exc, (EscrowShortfall, MissingAccount)):
        return Failure(ErrorClass.FATAL)
    if isinstance(exc, asyncpg.PostgresError):
        sqlstate = exc.sqlstate
        if sqlstate in LOCK_SQLSTATES:
            return Failure(ErrorClass.TRANSIENT, sqlstate)
        if sqlstate in _CONNECTION_SQLSTATES or sqlstate.startswith(_CONNECTION_CLASSES):
            return Failure(ErrorClass.TRANSIENT, sqlstate, connection=True)
        if sqlstate in _FATAL_SQLSTATES or sqlstate.startswith(_FATAL_CLASSES):
            return Failure(ErrorClass.FATAL, sqlstate)
        return Failure(ErrorClass.UNCLASSIFIED, sqlstate)
    # asyncpg's server-reported connection errors are PostgresErrors of class 08
    # (ConnectionDoesNotExistError is 08003), handled above, as is asyncpg.DataError (22000, raised
    # for an invalid query argument too). A client configuration error is never connection loss.
    if isinstance(exc, asyncpg.InterfaceError) and not isinstance(exc, asyncpg.ClientConfigurationError):
        if connection_closed:
            return Failure(ErrorClass.TRANSIENT, connection=True)
        return Failure(ErrorClass.UNCLASSIFIED)
    if isinstance(exc, OSError):
        return Failure(ErrorClass.TRANSIENT)
    return Failure(ErrorClass.UNCLASSIFIED)
