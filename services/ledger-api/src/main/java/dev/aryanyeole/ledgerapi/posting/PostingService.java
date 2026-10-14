package dev.aryanyeole.ledgerapi.posting;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.api.StoredOutcomeException;
import dev.aryanyeole.ledgerapi.db.DbErrors;
import dev.aryanyeole.ledgerapi.db.LockContention;

// Ledger postings (spec §6.1 steps 4-6). Callers run inside WriteTransactions: lock first,
// check what they need on the locked rows, then post. Invariants:
//  - Accounts are locked in ascending id order in one statement (invariant 4, D-007), so two
//    postings over the same accounts can never wait on each other in a cycle.
//  - Entries and their transactions row are written in the same transaction, and the
//    deferred trigger checks they sum to 0 at COMMIT (invariant 1). The Java sum check only
//    fails faster; the database is the authority.
//  - Money is BigDecimal throughout (invariant 3).
// A lock timeout on the account lock is contention: 503 lock_contention, not stored (D-050).
// 40P01 and 23514 (no_overdraft, the balance trigger) are never caught or retried: both are
// bugs and surface as 500 (D-007, D-058).
@Service
public class PostingService {

	private final JdbcClient jdbc;

	public PostingService(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	public record LockedAccount(long id, String type, BigDecimal balance, boolean allowNegative) {
	}

	/** Accounts locked FOR UPDATE by this transaction, by id. */
	public record LockedAccounts(Map<Long, LockedAccount> byId) {

		public LockedAccount get(long id) {
			return this.byId.get(id);
		}

	}

	/** One entry: amount leaves (negative) or arrives at (positive) the account (D-003). */
	public record Leg(long accountId, BigDecimal amount) {
	}

	public record Posted(long transactionId, OffsetDateTime createdAt) {
	}

	/** Id of the seeded system:external account (D-048); not locked. */
	public long externalAccountId() {
		return this.jdbc.sql("SELECT id FROM accounts WHERE name = 'system:external'")
			.query(Long.class)
			.optional()
			.orElseThrow(() -> new IllegalStateException("system:external account is missing"));
	}

	/** Locks the distinct ids in ascending order, in one statement. Missing ids -> stored 404. */
	public LockedAccounts lock(Collection<Long> accountIds) {
		Long[] ids = new TreeSet<>(accountIds).toArray(Long[]::new);
		List<LockedAccount> rows;
		try {
			// Positional: a named array parameter would be expanded into a list, not bound as bigint[].
			rows = this.jdbc
				.sql("SELECT id, type, balance, allow_negative FROM accounts WHERE id = ANY(?) ORDER BY id FOR UPDATE")
				.param((Object) ids)
				.query((rs, rowNum) -> new LockedAccount(rs.getLong("id"), rs.getString("type"),
						rs.getBigDecimal("balance"), rs.getBoolean("allow_negative")))
				.list();
		}
		catch (DataAccessException ex) {
			if (DbErrors.isLockTimeout(ex)) {
				throw new LockContention();
			}
			throw ex;
		}
		if (rows.size() != ids.length) {
			throw new StoredOutcomeException(HttpStatus.NOT_FOUND, ProblemCode.ACCOUNT_NOT_FOUND,
					"No account with this id.");
		}
		Map<Long, LockedAccount> byId = new TreeMap<>();
		rows.forEach((row) -> byId.put(row.id(), row));
		return new LockedAccounts(byId);
	}

	/**
	 * Funds check, then the transactions row, entries and balance updates, each in ascending
	 * account id. Every leg's account must be in {@code locked}.
	 */
	public Posted post(LockedAccounts locked, String kind, List<Leg> legs, String reference,
			String correlationId) {
		Map<Long, BigDecimal> deltas = new TreeMap<>();
		for (Leg leg : legs) {
			if (locked.get(leg.accountId()) == null) {
				throw new IllegalArgumentException("account " + leg.accountId() + " is not locked");
			}
			deltas.merge(leg.accountId(), leg.amount(), BigDecimal::add);
		}
		BigDecimal sum = deltas.values().stream().reduce(BigDecimal.ZERO, BigDecimal::add);
		if (legs.size() < 2 || sum.signum() != 0) {
			throw new IllegalArgumentException("legs must be at least two and sum to 0, got " + sum);
		}

		deltas.forEach((accountId, delta) -> {
			LockedAccount account = locked.get(accountId);
			if (!account.allowNegative() && account.balance().add(delta).signum() < 0) {
				throw new StoredOutcomeException(HttpStatus.UNPROCESSABLE_CONTENT, ProblemCode.INSUFFICIENT_FUNDS,
						"Insufficient funds in the source account.");
			}
		});

		Posted posted = this.jdbc
			.sql("INSERT INTO transactions (kind, reference, correlation_id) VALUES (:kind, :reference, :correlation) "
					+ "RETURNING id, created_at")
			.param("kind", kind)
			.param("reference", reference)
			.param("correlation", correlationId)
			.query((rs, rowNum) -> new Posted(rs.getLong("id"), rs.getObject("created_at", OffsetDateTime.class)))
			.single();

		legs.stream().sorted(Comparator.comparingLong(Leg::accountId)).forEach((leg) -> this.jdbc
			.sql("INSERT INTO entries (transaction_id, account_id, amount) VALUES (:transaction, :account, :amount)")
			.param("transaction", posted.transactionId())
			.param("account", leg.accountId())
			.param("amount", leg.amount())
			.update());

		deltas.forEach((accountId, delta) -> this.jdbc
			.sql("UPDATE accounts SET balance = balance + :delta WHERE id = :id")
			.param("delta", delta)
			.param("id", accountId)
			.update());

		return posted;
	}

}
