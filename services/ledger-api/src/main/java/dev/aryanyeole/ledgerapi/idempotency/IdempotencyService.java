package dev.aryanyeole.ledgerapi.idempotency;

import java.net.URI;
import java.security.MessageDigest;
import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;

import tools.jackson.databind.json.JsonMapper;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.api.ProblemHandler;
import dev.aryanyeole.ledgerapi.api.StoredOutcomeException;
import dev.aryanyeole.ledgerapi.db.DbErrors;
import dev.aryanyeole.ledgerapi.db.WriteTransactions;

// Idempotent execution (spec §6.1, D-004, D-044, D-045, D-064). In one write transaction:
//  1. Claim the key: INSERT ... ON CONFLICT DO NOTHING. A concurrent duplicate blocks here on
//     the owner's uncommitted unique-index entry until the owner commits or rolls back; past
//     lock_timeout that is 55P03 -> 409 idempotency_request_in_progress, not stored.
//  2. Inserted: this request owns the key. Savepoint, run the operation, store its outcome
//     (the 201, or a StoredOutcomeException after rolling back to the savepoint) and send
//     exactly the text the database returns for it. Key and effect commit together
//     (invariant 7; V6's trigger rejects a commit without a stored response).
//  3. Conflict: read the committed row. A different fingerprint -> 422
//     idempotency_key_reused, not stored. Same fingerprint -> the stored status and text,
//     verbatim; the transaction wrote nothing and is rolled back.
// Owner and replays send the same bytes because both come from response_body::text: JSONB
// normalizes key order and whitespace, so the app's own serialization would differ (D-044).
@Service
public class IdempotencyService {

	private static final MediaType JSON = MediaType.APPLICATION_JSON;

	private static final MediaType PROBLEM_JSON = MediaType.APPLICATION_PROBLEM_JSON;

	private final WriteTransactions transactions;

	private final JdbcClient jdbc;

	private final JsonMapper json;

	IdempotencyService(WriteTransactions transactions, JdbcClient jdbc, JsonMapper json) {
		this.transactions = transactions;
		this.jdbc = jdbc;
		this.json = json;
	}

	/** What an owning request decided: the status and the body to store. */
	public record Outcome(HttpStatus status, Object body) {
	}

	private record Stored(int status, String body) {
	}

	private record KeyRow(byte[] requestHash, int status, String body) {
	}

	public ResponseEntity<String> execute(String clientId, String key, String method, String route, Object request,
			Supplier<Outcome> operation) {
		byte[] requestHash = Fingerprint.of(method, route, request);
		Stored stored = this.transactions
			.execute((status) -> claimOrReplay(status, clientId, key, route, requestHash, operation));
		HttpStatus httpStatus = HttpStatus.valueOf(stored.status());
		return ResponseEntity.status(httpStatus)
			.contentType(httpStatus.is2xxSuccessful() ? JSON : PROBLEM_JSON)
			.body(stored.body());
	}

	private Stored claimOrReplay(TransactionStatus status, String clientId, String key, String route,
			byte[] requestHash, Supplier<Outcome> operation) {
		// One retry: the conflicting row can only vanish once M4 purges expired keys (D-020).
		for (int attempt = 0; attempt < 2; attempt++) {
			if (claim(clientId, key, requestHash)) {
				return runAndStore(status, clientId, key, route, operation);
			}
			Optional<KeyRow> row = this.jdbc
				.sql("SELECT request_hash, status_code, response_body::text FROM idempotency_keys "
						+ "WHERE client_id = :client AND key = :key")
				.param("client", clientId)
				.param("key", key)
				.query((rs, rowNum) -> new KeyRow(rs.getBytes(1), rs.getInt(2), rs.getString(3)))
				.optional();
			if (row.isEmpty()) {
				continue;
			}
			if (!MessageDigest.isEqual(row.get().requestHash(), requestHash)) {
				throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, ProblemCode.IDEMPOTENCY_KEY_REUSED,
						"This Idempotency-Key was already used with a different request.");
			}
			status.setRollbackOnly();
			return new Stored(row.get().status(), row.get().body());
		}
		throw new IllegalStateException("idempotency key conflicted but its row was missing twice");
	}

	private boolean claim(String clientId, String key, byte[] requestHash) {
		try {
			return this.jdbc
				.sql("INSERT INTO idempotency_keys (client_id, key, request_hash) VALUES (:client, :key, :hash) "
						+ "ON CONFLICT DO NOTHING")
				.param("client", clientId)
				.param("key", key)
				.param("hash", requestHash)
				.update() == 1;
		}
		catch (DataAccessException ex) {
			if (DbErrors.isLockTimeout(ex)) {
				throw new ApiException(HttpStatus.CONFLICT, ProblemCode.IDEMPOTENCY_REQUEST_IN_PROGRESS,
						"A request with this Idempotency-Key is still in progress; retry later.");
			}
			throw ex;
		}
	}

	private Stored runAndStore(TransactionStatus status, String clientId, String key, String route,
			Supplier<Outcome> operation) {
		Object savepoint = status.createSavepoint();
		HttpStatus outcomeStatus;
		String body;
		try {
			Outcome outcome = operation.get();
			status.releaseSavepoint(savepoint);
			outcomeStatus = outcome.status();
			body = this.json.writeValueAsString(outcome.body());
		}
		catch (StoredOutcomeException ex) {
			status.rollbackToSavepoint(savepoint);
			ProblemDetail problem = ProblemHandler.problemDetail(ex);
			problem.setInstance(URI.create(route));
			outcomeStatus = ex.status();
			body = this.json.writeValueAsString(problem);
		}
		String text = this.jdbc
			.sql("UPDATE idempotency_keys SET status_code = :status, response_body = :body::jsonb "
					+ "WHERE client_id = :client AND key = :key RETURNING response_body::text")
			.param("status", outcomeStatus.value())
			.param("body", body)
			.param("client", clientId)
			.param("key", key)
			.query(String.class)
			.single();
		return new Stored(outcomeStatus.value(), text);
	}

}
