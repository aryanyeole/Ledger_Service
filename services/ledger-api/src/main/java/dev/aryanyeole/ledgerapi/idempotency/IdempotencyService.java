package dev.aryanyeole.ledgerapi.idempotency;

import java.net.URI;
import java.security.MessageDigest;
import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.beans.factory.ObjectProvider;
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
//
// The Redis fast path (D-139), only when the IdempotencyCache bean exists: a cache read after the
// fingerprint and before the transaction (a hit with a matching fingerprint returns the stored
// status and text, no DB access); after the transaction has ended normally, the owner's and a
// replay's text is cached if the DB says the row outlives ttl + margin. An exception out of the
// transaction caches nothing. Both Redis calls run outside any transaction (invariant 5); the
// gateway refuses otherwise (D-149). Without the bean there is no Redis call at all.
@Service
public class IdempotencyService {

	private static final MediaType JSON = MediaType.APPLICATION_JSON;

	private static final MediaType PROBLEM_JSON = MediaType.APPLICATION_PROBLEM_JSON;

	// The row outlives the fast-path TTL plus its margin, measured by the DB in the statement that
	// returns the text (D-139): durations only, so no clock is compared across hosts.
	private static final String CACHEABLE = "(expires_at - clock_timestamp()) > (:minLifeMs * interval '1 millisecond') AS cacheable";

	private final WriteTransactions transactions;

	private final JdbcClient jdbc;

	private final JsonMapper json;

	private final IdempotencyCache cache;

	private final IdempotencyFaultHook faultHook;

	private final long minLifeMillis;

	IdempotencyService(WriteTransactions transactions, JdbcClient jdbc, JsonMapper json,
			ObjectProvider<IdempotencyCache> cache, IdempotencyFaultHook faultHook,
			LedgerIdempotencyCacheProperties cacheProperties) {
		this.transactions = transactions;
		this.jdbc = jdbc;
		this.json = json;
		this.cache = cache.getIfAvailable();
		this.faultHook = faultHook;
		this.minLifeMillis = cacheProperties.minLifeMillis();
	}

	/** What an owning request decided: the status and the body to store. */
	public record Outcome(HttpStatus status, Object body) {
	}

	// cacheable: the DB found expires_at - clock_timestamp() > ttl + margin in the statement that
	// returned the text (no clock is read here).
	private record Stored(int status, String body, byte[] requestHash, boolean cacheable) {
	}

	private record KeyRow(byte[] requestHash, int status, String body, boolean cacheable) {
	}

	public ResponseEntity<String> execute(String clientId, String key, String method, String route, Object request,
			Supplier<Outcome> operation) {
		byte[] requestHash = Fingerprint.of(method, route, request);
		if (this.cache != null) {
			IdempotencyCache.Lookup lookup = this.cache.read(clientId, key, requestHash);
			if (lookup.result() == IdempotencyCache.ReadResult.HIT) {
				return response(lookup.status(), lookup.text());
			}
		}
		Stored stored = this.transactions
			.execute((status) -> claimOrReplay(status, clientId, key, route, requestHash, operation));
		if (this.cache != null) {
			this.cache.populate(clientId, key, stored.requestHash(), stored.status(), stored.body(),
					stored.cacheable());
		}
		return response(stored.status(), stored.body());
	}

	private static ResponseEntity<String> response(int status, String body) {
		HttpStatus httpStatus = HttpStatus.valueOf(status);
		return ResponseEntity.status(httpStatus)
			.contentType(httpStatus.is2xxSuccessful() ? JSON : PROBLEM_JSON)
			.body(body);
	}

	private Stored claimOrReplay(TransactionStatus status, String clientId, String key, String route,
			byte[] requestHash, Supplier<Outcome> operation) {
		// One retry: the conflicting row can only vanish once M4 purges expired keys (D-020).
		for (int attempt = 0; attempt < 2; attempt++) {
			if (claim(clientId, key, requestHash)) {
				return runAndStore(status, clientId, key, route, requestHash, operation);
			}
			Optional<KeyRow> row = this.jdbc
				.sql("SELECT request_hash, status_code, response_body::text, " + CACHEABLE
						+ " FROM idempotency_keys WHERE client_id = :client AND key = :key")
				.param("client", clientId)
				.param("key", key)
				.param("minLifeMs", this.minLifeMillis)
				.query((rs, rowNum) -> new KeyRow(rs.getBytes(1), rs.getInt(2), rs.getString(3), rs.getBoolean(4)))
				.optional();
			if (row.isEmpty()) {
				continue;
			}
			if (!MessageDigest.isEqual(row.get().requestHash(), requestHash)) {
				throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, ProblemCode.IDEMPOTENCY_KEY_REUSED,
						"This Idempotency-Key was already used with a different request.");
			}
			status.setRollbackOnly();
			return new Stored(row.get().status(), row.get().body(), requestHash, row.get().cacheable());
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
			byte[] requestHash, Supplier<Outcome> operation) {
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
		int storedStatus = outcomeStatus.value();
		Stored stored = this.jdbc
			.sql("UPDATE idempotency_keys SET status_code = :status, response_body = :body::jsonb "
					+ "WHERE client_id = :client AND key = :key RETURNING response_body::text, " + CACHEABLE)
			.param("status", storedStatus)
			.param("body", body)
			.param("client", clientId)
			.param("key", key)
			.param("minLifeMs", this.minLifeMillis)
			.query((rs, rowNum) -> new Stored(storedStatus, rs.getString(1), requestHash, rs.getBoolean(2)))
			.single();
		this.faultHook.afterStoreBeforeCommit(clientId, key);
		return stored;
	}

}
