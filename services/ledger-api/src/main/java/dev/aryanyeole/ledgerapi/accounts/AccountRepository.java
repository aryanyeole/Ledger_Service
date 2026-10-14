package dev.aryanyeole.ledgerapi.accounts;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Optional;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.api.StoredOutcomeException;
import dev.aryanyeole.ledgerapi.db.DbErrors;
import dev.aryanyeole.ledgerapi.db.LockContention;

@Repository
public class AccountRepository {

	// V1's `name TEXT NOT NULL UNIQUE`, under Postgres' default constraint name.
	static final String NAME_UNIQUE_CONSTRAINT = "accounts_name_key";

	private static final String NAME_TAKEN = "An account with this name already exists.";

	private final JdbcClient jdbc;

	AccountRepository(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	record Created(long id, OffsetDateTime createdAt) {
	}

	record Balance(BigDecimal balance, OffsetDateTime asOf) {
	}

	record Account(long id, String name, String type, OffsetDateTime createdAt) {
	}

	// allow_negative is not in the column list: the default (false) applies, and V1's CHECK
	// ties it to type (D-039, D-048). Runs inside WriteTransactions; the controller wraps it.
	// Unkeyed creation (D-054): only a violation of the name constraint is the client's
	// duplicate, an unstored 409; any other 23505 (e.g. the system-type singleton index) is
	// unexpected and stays a 500 (D-058). A lock timeout is contention: 503 (D-050).
	Created insert(String name, String type) {
		try {
			return this.jdbc.sql("INSERT INTO accounts (name, type) VALUES (:name, :type) RETURNING id, created_at")
				.param("name", name)
				.param("type", type)
				.query((rs, rowNum) -> new Created(rs.getLong("id"), rs.getObject("created_at", OffsetDateTime.class)))
				.single();
		}
		catch (DuplicateKeyException ex) {
			if (NAME_UNIQUE_CONSTRAINT.equals(DbErrors.violatedConstraint(ex))) {
				throw new ApiException(HttpStatus.CONFLICT, ProblemCode.ACCOUNT_NAME_TAKEN, NAME_TAKEN);
			}
			throw ex;
		}
		catch (DataAccessException ex) {
			throw DbErrors.isLockTimeout(ex) ? new LockContention() : ex;
		}
	}

	// Keyed creation (D-054): a taken name leaves the transaction usable, so the 409 can be
	// stored against the key. Only the name conflict is absorbed; any other unique violation
	// still raises and stays a 500.
	Created insertIfNameFree(String name, String type) {
		try {
			return this.jdbc
				.sql("INSERT INTO accounts (name, type) VALUES (:name, :type) ON CONFLICT (name) DO NOTHING "
						+ "RETURNING id, created_at")
				.param("name", name)
				.param("type", type)
				.query((rs, rowNum) -> new Created(rs.getLong("id"), rs.getObject("created_at", OffsetDateTime.class)))
				.optional()
				.orElseThrow(() -> new StoredOutcomeException(HttpStatus.CONFLICT, ProblemCode.ACCOUNT_NAME_TAKEN,
						NAME_TAKEN));
		}
		catch (DataAccessException ex) {
			throw DbErrors.isLockTimeout(ex) ? new LockContention() : ex;
		}
	}

	Optional<Account> find(long id) {
		return this.jdbc.sql("SELECT id, name, type, created_at FROM accounts WHERE id = :id")
			.param("id", id)
			.query((rs, rowNum) -> new Account(rs.getLong("id"), rs.getString("name"), rs.getString("type"),
					rs.getObject("created_at", OffsetDateTime.class)))
			.optional();
	}

	// Balance and as_of come from one statement, so as_of is the database's time for the
	// same read (now() is the statement's transaction start), not the JVM's clock.
	Optional<Balance> balance(long id) {
		return this.jdbc.sql("SELECT balance, now() AS as_of FROM accounts WHERE id = :id")
			.param("id", id)
			.query((rs, rowNum) -> new Balance(rs.getBigDecimal("balance"),
					rs.getObject("as_of", OffsetDateTime.class)))
			.optional();
	}

}
