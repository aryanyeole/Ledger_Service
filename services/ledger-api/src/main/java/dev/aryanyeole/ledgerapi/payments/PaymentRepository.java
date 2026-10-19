package dev.aryanyeole.ledgerapi.payments;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

// payments rows (V9, D-076). The app inserts only the parties and the money; id, status and both
// timestamps come from the database and are read back with RETURNING.
@Repository
class PaymentRepository {

	private static final String COLUMNS = "id, customer_account_id, merchant_account_id, amount, fee, status, created_at, updated_at";

	private final JdbcClient jdbc;

	PaymentRepository(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	record Payment(UUID id, long customerAccountId, long merchantAccountId, BigDecimal amount, BigDecimal fee,
			String status, OffsetDateTime createdAt, OffsetDateTime updatedAt) {
	}

	// Runs inside the authorize transaction, after the accounts are locked: the FKs take KEY SHARE
	// on the customer and merchant rows, which this transaction already holds FOR UPDATE (D-075).
	Payment insert(long customer, long merchant, BigDecimal amount, BigDecimal fee) {
		return this.jdbc
			.sql("INSERT INTO payments (customer_account_id, merchant_account_id, amount, fee) "
					+ "VALUES (:customer, :merchant, :amount, :fee) RETURNING " + COLUMNS)
			.param("customer", customer)
			.param("merchant", merchant)
			.param("amount", amount)
			.param("fee", fee)
			.query(PaymentRepository::payment)
			.single();
	}

	// A plain read, no transaction: the current status (D-069 reasoning; one statement).
	Optional<Payment> find(UUID id) {
		return this.jdbc.sql("SELECT " + COLUMNS + " FROM payments WHERE id = :id")
			.param("id", id)
			.query(PaymentRepository::payment)
			.optional();
	}

	private static Payment payment(ResultSet rs, int rowNum) throws SQLException {
		return new Payment(rs.getObject("id", UUID.class), rs.getLong("customer_account_id"),
				rs.getLong("merchant_account_id"), rs.getBigDecimal("amount"), rs.getBigDecimal("fee"),
				rs.getString("status"), rs.getObject("created_at", OffsetDateTime.class),
				rs.getObject("updated_at", OffsetDateTime.class));
	}

}
