package dev.aryanyeole.ledgerapi.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;

import dev.aryanyeole.ledgerapi.db.DbErrors;
import dev.aryanyeole.ledgerapi.db.WriteTransactions;

// Deletes idempotency keys past expires_at (D-020, D-119). Until a key is purged the conflict
// path keeps replaying it, whatever its expires_at (IdempotencyService is unchanged); once
// purged, the key is new. Each batch is one WriteTransactions transaction (lock_timeout first,
// D-050) and one statement: the oldest expired keys, at most batch-size of them, FOR UPDATE
// SKIP LOCKED so concurrent purgers on other instances take disjoint batches. now() is the
// transaction's start, the same instant V15's guard compares against, so the guard can't refuse
// a row this select picked; a 23514 idempotency_keys_purge here means the two disagree, which is
// logged as an ERROR before it is rethrown. Never logs a key, request hash or response.
public class IdempotencyKeyPurger {

	private static final Logger log = LoggerFactory.getLogger(IdempotencyKeyPurger.class);

	static final String GUARD = "idempotency_keys_purge";

	private static final String CHECK_VIOLATION = "23514";

	// Package-private so IdempotencyKeyExpiryTest EXPLAINs and runs this exact text (D-128).
	static final String PURGE_BATCH = """
			DELETE FROM idempotency_keys WHERE (client_id, key) IN (
			  SELECT client_id, key FROM idempotency_keys WHERE expires_at < now()
			  ORDER BY expires_at LIMIT :batchSize FOR UPDATE SKIP LOCKED)""";

	private final WriteTransactions transactions;

	private final JdbcClient jdbc;

	private final int batchSize;

	public IdempotencyKeyPurger(WriteTransactions transactions, JdbcClient jdbc,
			IdempotencyKeyPurgeProperties properties) {
		this.transactions = transactions;
		this.jdbc = jdbc;
		this.batchSize = properties.batchSize();
	}

	/** Deletes one batch of expired keys in its own transaction; returns how many were deleted. */
	public int purgeBatch() {
		try {
			Integer deleted = this.transactions
				.execute((status) -> this.jdbc.sql(PURGE_BATCH).param("batchSize", this.batchSize).update());
			return (deleted != null) ? deleted : 0;
		}
		catch (DataAccessException ex) {
			// Rolled back by now: nothing in the batch was deleted.
			if (CHECK_VIOLATION.equals(DbErrors.sqlState(ex)) && GUARD.equals(DbErrors.violatedConstraint(ex))) {
				log.error("Idempotency key purge batch rolled back: the purge select and the {} guard disagree "
						+ "(SQLState {}, constraint {})", GUARD, CHECK_VIOLATION, GUARD);
			}
			throw ex;
		}
	}

	/**
	 * Deletes batches until one comes back short (or the thread is interrupted); returns the total
	 * deleted. One INFO line per run.
	 */
	public long purgeExpired() {
		long started = System.nanoTime();
		long deleted = 0;
		int batches = 0;
		boolean interrupted = false;
		while (true) {
			if (Thread.currentThread().isInterrupted()) {
				interrupted = true;
				break;
			}
			int batch = purgeBatch();
			batches++;
			deleted += batch;
			if (batch < this.batchSize) {
				break;
			}
		}
		log.info("Idempotency key purge: deleted={} batches={} duration_ms={} interrupted={}", deleted, batches,
				(System.nanoTime() - started) / 1_000_000, interrupted);
		return deleted;
	}

}
