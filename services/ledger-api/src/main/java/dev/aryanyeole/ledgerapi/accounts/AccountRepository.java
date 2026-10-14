package dev.aryanyeole.ledgerapi.accounts;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Optional;

import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

@Repository
public class AccountRepository {

	// V1's `name TEXT NOT NULL UNIQUE`, under Postgres' default constraint name.
	static final String NAME_UNIQUE_CONSTRAINT = "accounts_name_key";

	private final JdbcClient jdbc;

	AccountRepository(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	record Created(long id, OffsetDateTime createdAt) {
	}

	record Balance(BigDecimal balance, OffsetDateTime asOf) {
	}

	// allow_negative is not in the column list: the default (false) applies, and V1's CHECK
	// ties it to type (D-039, D-048). A single statement, so it commits on its own.
	// Only a violation of the name constraint is the client's duplicate; any other 23505
	// (e.g. the system-type singleton index) is unexpected here and stays a 500.
	Created insert(String name, String type) {
		try {
			return this.jdbc.sql("INSERT INTO accounts (name, type) VALUES (:name, :type) RETURNING id, created_at")
				.param("name", name)
				.param("type", type)
				.query((rs, rowNum) -> new Created(rs.getLong("id"), rs.getObject("created_at", OffsetDateTime.class)))
				.single();
		}
		catch (DuplicateKeyException ex) {
			if (NAME_UNIQUE_CONSTRAINT.equals(violatedConstraint(ex))) {
				throw new ApiException(HttpStatus.CONFLICT, ProblemCode.ACCOUNT_NAME_TAKEN,
						"An account with this name already exists.");
			}
			throw ex;
		}
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

	private static String violatedConstraint(DuplicateKeyException ex) {
		for (Throwable cause = ex.getCause(); cause != null; cause = cause.getCause()) {
			if (cause instanceof PSQLException psql) {
				ServerErrorMessage message = psql.getServerErrorMessage();
				return (message != null) ? message.getConstraint() : null;
			}
		}
		return null;
	}

}
