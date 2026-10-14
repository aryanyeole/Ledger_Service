package dev.aryanyeole.ledgerapi.db;

import java.sql.SQLException;

import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

// Reads Postgres errors by SQLState and constraint name, never by message text (D-058).
public final class DbErrors {

	/** lock_not_available: lock_timeout expired (D-050). */
	public static final String LOCK_NOT_AVAILABLE = "55P03";

	private DbErrors() {
	}

	/** SQLState of the deepest SQLException in the cause chain, or null. */
	public static String sqlState(Throwable thrown) {
		String state = null;
		for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
			if (cause instanceof SQLException sql && sql.getSQLState() != null) {
				state = sql.getSQLState();
			}
		}
		return state;
	}

	public static boolean isLockTimeout(Throwable thrown) {
		return LOCK_NOT_AVAILABLE.equals(sqlState(thrown));
	}

	/** Constraint named in the server error, or null. */
	public static String violatedConstraint(Throwable thrown) {
		for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
			if (cause instanceof PSQLException psql) {
				ServerErrorMessage message = psql.getServerErrorMessage();
				return (message != null) ? message.getConstraint() : null;
			}
		}
		return null;
	}

}
