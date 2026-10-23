"""D-107: every failure class and its boundaries. classify() is pure, so the exceptions are built here."""

import asyncpg
import pytest

from settlement_worker.db import DbTimeout, DbUnavailable
from settlement_worker.envelope import InvalidEvent
from settlement_worker.failures import ErrorClass, Failure, classify
from settlement_worker.settlement import EscrowShortfall, EventMismatch, MissingAccount, SettlementError, UnknownPayment

T, P, U, F = ErrorClass.TRANSIENT, ErrorClass.POISON, ErrorClass.UNCLASSIFIED, ErrorClass.FATAL


@pytest.mark.parametrize(
    "exc, expected",
    [
        # Transient on a live connection: retried in place now (T5a).
        (asyncpg.LockNotAvailableError("lock timeout"), Failure(T, "55P03")),
        (asyncpg.DeadlockDetectedError("deadlock"), Failure(T, "40P01")),
        (asyncpg.SerializationError("serialization"), Failure(T, "40001")),
        # Transient, connection loss (T5b).
        (asyncpg.ConnectionFailureError("gone"), Failure(T, "08006", connection=True)),
        (asyncpg.ConnectionDoesNotExistError("closed"), Failure(T, "08003", connection=True)),
        (asyncpg.AdminShutdownError("shutdown"), Failure(T, "57P01", connection=True)),
        (asyncpg.CrashShutdownError("crash"), Failure(T, "57P02", connection=True)),
        (asyncpg.CannotConnectNowError("starting"), Failure(T, "57P03", connection=True)),
        (asyncpg.TooManyConnectionsError("full"), Failure(T, "53300", connection=True)),
        # DB call sites wrap their timeouts and socket errors (D-109): connection loss.
        (DbTimeout("DB call timed out"), Failure(T, connection=True)),
        (DbUnavailable("ConnectionRefusedError"), Failure(T, connection=True)),
        # A raw OSError/TimeoutError didn't come from a DB call: transient, but the DB is left alone.
        (ConnectionResetError("reset"), Failure(T)),
        (TimeoutError("timeout"), Failure(T)),
        # Fatal: the deployment or the ledger is broken.
        (asyncpg.InsufficientPrivilegeError("grant"), Failure(F, "42501")),
        (asyncpg.UndefinedTableError("table"), Failure(F, "42P01")),
        (asyncpg.UndefinedColumnError("column"), Failure(F, "42703")),
        (asyncpg.UndefinedFunctionError("function"), Failure(F, "42883")),
        (asyncpg.InvalidPasswordError("password"), Failure(F, "28P01")),
        (asyncpg.InvalidAuthorizationSpecificationError("auth"), Failure(F, "28000")),
        (asyncpg.InvalidCatalogNameError("database"), Failure(F, "3D000")),
        (asyncpg.UniqueViolationError("unexpected 23505"), Failure(F, "23505")),
        (asyncpg.CheckViolationError("balance trigger"), Failure(F, "23514")),
        (asyncpg.RestrictViolationError("append-only"), Failure(F, "23001")),
        (asyncpg.NotNullViolationError("not null"), Failure(F, "23502")),
        (EscrowShortfall("escrow"), Failure(F)),
        (MissingAccount("system:escrow account is missing"), Failure(F)),
        # Poison: the message can never succeed.
        (InvalidEvent("bad"), Failure(P)),
        (UnknownPayment("unknown"), Failure(P)),
        (EventMismatch("mismatch"), Failure(P)),
        # Unclassified: anything else, other SQLSTATEs included.
        (RuntimeError("fault RAISE_BEFORE_COMMIT"), Failure(U)),
        (SettlementError("generic"), Failure(U)),
        (asyncpg.NumericValueOutOfRangeError("overflow"), Failure(U, "22003")),
        (asyncpg.InFailedSQLTransactionError("aborted"), Failure(U, "25P02")),
        (asyncpg.PostgresSyntaxError("syntax"), Failure(U, "42601")),
    ],
    ids=lambda v: type(v).__name__ if isinstance(v, BaseException) else "",
)
def test_classify(exc, expected):
    assert classify(exc) == expected


def test_metric_labels_are_the_class_names():
    assert [c.value for c in ErrorClass] == ["Transient", "Poison", "Unclassified", "Fatal"]


# D-109: an asyncpg InterfaceError is connection loss only when the connection is closed at the
# point of failure; on a live connection it is a client-side bug (Unclassified, bounded). DataError
# (a bad query argument) is never connection loss.
@pytest.mark.parametrize(
    "exc, closed, expected",
    [
        (asyncpg.InterfaceError("cannot perform operation: connection is closed"), True, Failure(T, connection=True)),
        (asyncpg.InterfaceError("cannot perform operation: another operation is in progress"), False, Failure(U)),
        # asyncpg raises DataError (a PostgresError, 22000) for an invalid query argument too.
        (asyncpg.DataError("invalid input for query argument $1"), True, Failure(U, "22000")),
        (asyncpg.DataError("invalid input for query argument $1"), False, Failure(U, "22000")),
        (asyncpg.ClientConfigurationError("bad config"), True, Failure(U)),
    ],
    ids=["closed", "live", "data-error-closed", "data-error-live", "client-config"],
)
def test_interface_error_by_connection_state(exc, closed, expected):
    assert classify(exc, connection_closed=closed) == expected
