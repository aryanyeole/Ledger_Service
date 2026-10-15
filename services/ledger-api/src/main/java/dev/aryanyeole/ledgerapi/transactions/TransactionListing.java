package dev.aryanyeole.ledgerapi.transactions;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

// Reads for GET /v1/transactions (D-053, D-069). Two statements per page, no N+1: the page's
// transactions by keyset, then all their entries in one query.
//
// No wrapping transaction (D-069): each statement runs on its own under READ COMMITTED. That
// is consistent because transactions and entries are append-only (D-041, D-042), so a row
// never changes after it is read, and a transaction's entries commit in the same database
// transaction as its transactions row (invariant 1). Every transaction query 1 can see was
// committed with all its entries, so query 2 sees all of them, and nothing else for those ids.
@Repository
class TransactionListing {

	private final JdbcClient jdbc;

	TransactionListing(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	record Row(long id, String kind, String reference, OffsetDateTime createdAt) {
	}

	record EntryRow(long transactionId, long accountId, BigDecimal amount) {
	}

	/** Up to {@code fetch} transactions after {@code from} (or from the top), newest first. */
	List<Row> page(TransactionCursor.Position from, int fetch) {
		String where = (from != null) ? "WHERE (created_at, id) < (:createdAt, :id) " : "";
		JdbcClient.StatementSpec spec = this.jdbc.sql("SELECT id, kind, reference, created_at FROM transactions "
				+ where + "ORDER BY created_at DESC, id DESC LIMIT :limit").param("limit", fetch);
		if (from != null) {
			spec = spec.param("createdAt", from.createdAt()).param("id", from.id());
		}
		return spec.query((rs, rowNum) -> new Row(rs.getLong("id"), rs.getString("kind"), rs.getString("reference"),
				rs.getObject("created_at", OffsetDateTime.class)))
			.list();
	}

	/** Entries of these transactions, by transaction then entry id. */
	Map<Long, List<EntryRow>> entries(List<Long> transactionIds) {
		Map<Long, List<EntryRow>> byTransaction = new LinkedHashMap<>();
		if (transactionIds.isEmpty()) {
			return byTransaction;
		}
		// Positional: a named array parameter would be expanded into a list, not bound as bigint[].
		this.jdbc
			.sql("SELECT transaction_id, account_id, amount FROM entries WHERE transaction_id = ANY(?) "
					+ "ORDER BY transaction_id, id")
			.param((Object) transactionIds.toArray(Long[]::new))
			.query((rs, rowNum) -> new EntryRow(rs.getLong("transaction_id"), rs.getLong("account_id"),
					rs.getBigDecimal("amount")))
			.list()
			.forEach((entry) -> byTransaction.computeIfAbsent(entry.transactionId(), (id) -> new ArrayList<>())
				.add(entry));
		return byTransaction;
	}

}
