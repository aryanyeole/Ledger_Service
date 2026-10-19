package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.postgresql.PostgreSQLContainer;

// Proof (D-076): a payment's status moves only AUTHORIZED -> SETTLED or AUTHORIZED -> FAILED,
// nothing else in the row changes with it, updated_at comes from the database clock, and the
// row can't be deleted. The worker makes the valid transitions with only UPDATE (status); every
// other transition fails with check_violation (23514) naming payments_status_transition. The
// owner, which has every privilege, is stopped by the same trigger and by the DELETE/TRUNCATE
// trigger (23001). A conditional transition on a handled payment matches no row and raises
// nothing: the §4 second dedup layer. Fixtures are written through the app role's grants;
// every connection asserts current_user (D-036). Attempts run with autoCommit=false and are
// rolled back, so in the failing-first run (-Dspring.flyway.target=9, D-038) the ones that
// wrongly succeed leave nothing behind.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class PaymentStatusTransitionTest {

	private static final String CHECK_VIOLATION = "23514";

	private static final String RESTRICT_VIOLATION = "23001";

	private static final String TRANSITION = "payments_status_transition";

	enum Role {

		APP("ledger_api_app"), WORKER("settlement_worker"), OWNER("ledger_api_owner");

		final String dbRole;

		Role(String dbRole) {
			this.dbRole = dbRole;
		}

	}

	record Payment(UUID id, long customerAccountId, long merchantAccountId, BigDecimal amount, BigDecimal fee,
			String status, OffsetDateTime createdAt, OffsetDateTime updatedAt) {
	}

	enum OwnerDeletion {

		DELETE_PAYMENT("DELETE FROM payments WHERE id = ?", true),
		DELETE_NO_ROWS("DELETE FROM payments WHERE false", false),
		TRUNCATE_PAYMENTS("TRUNCATE payments", false);

		final String sql;

		final boolean bindsId;

		OwnerDeletion(String sql, boolean bindsId) {
			this.sql = sql;
			this.bindsId = bindsId;
		}

	}

	// Combined with a valid AUTHORIZED -> SETTLED, so only the column check can reject it.
	// %d is another account's id.
	enum ColumnChange {

		ID("id = gen_random_uuid()"),
		CUSTOMER_ACCOUNT_ID("customer_account_id = %d"),
		MERCHANT_ACCOUNT_ID("merchant_account_id = %d"),
		AMOUNT("amount = amount + 1"),
		FEE("fee = fee + 0.01"),
		CREATED_AT("created_at = created_at - interval '1 day'");

		final String assignment;

		ColumnChange(String assignment) {
			this.assignment = assignment;
		}

	}

	// Each violates exactly one constraint, so the one named is the one that fires.
	enum InvalidInsert {

		FEE_ABOVE_AMOUNT("100.00", "100.01", false, "payments_fee_le_amount"),
		SAME_ACCOUNTS("100.00", "2.90", true, "payments_distinct_accounts"),
		ZERO_AMOUNT("0", "0", false, "payments_amount_check"),
		NEGATIVE_FEE("100.00", "-0.01", false, "payments_fee_check");

		final String amount;

		final String fee;

		final boolean sameAccounts;

		final String constraint;

		InvalidInsert(String amount, String fee, boolean sameAccounts, String constraint) {
			this.amount = amount;
			this.fee = fee;
			this.sameAccounts = sameAccounts;
			this.constraint = constraint;
		}

	}

	@Autowired
	DataSource dataSource;

	@Autowired
	PostgreSQLContainer postgres;

	@Test
	void insertTakesIdStatusAndTimestampsFromDefaults() throws SQLException {
		long customer = createAccount("CUSTOMER");
		long merchant = createAccount("MERCHANT");
		UUID id;
		OffsetDateTime insertedAt;
		try (Connection conn = connect(Role.APP)) {
			try {
				insertedAt = dbNow(conn);
				id = insertPayment(conn, customer, merchant, "100.00", "2.90");
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}

		Payment payment = read(id);
		assertThat(payment.id().version()).as("ledger_api_app: id from gen_random_uuid() is a v4 UUID").isEqualTo(4);
		assertThat(payment.status()).as("ledger_api_app: default status").isEqualTo("AUTHORIZED");
		assertThat(payment.createdAt()).as("ledger_api_app: created_at is the inserting transaction's now()")
			.isEqualTo(insertedAt);
		assertThat(payment.updatedAt()).as("ledger_api_app: updated_at is the inserting transaction's now()")
			.isEqualTo(insertedAt);
	}

	@ParameterizedTest(name = "AUTHORIZED -> {0}")
	@ValueSource(strings = { "SETTLED", "FAILED" })
	void workerMovesAuthorizedPaymentToTerminalStatus(String target) throws SQLException {
		Payment before = newPayment();
		OffsetDateTime transitionedAt;
		try (Connection conn = connect(Role.WORKER)) {
			try {
				transitionedAt = dbNow(conn);
				assertThat(update(conn, "UPDATE payments SET status = ? WHERE id = ?", target, before.id()))
					.as("settlement_worker: rows moved to %s", target)
					.isEqualTo(1);
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}

		Payment after = read(before.id());
		assertThat(after.status()).as("settlement_worker: status").isEqualTo(target);
		assertThat(after.updatedAt()).as("settlement_worker: updated_at is the transition's now()")
			.isEqualTo(transitionedAt)
			.isAfter(before.updatedAt());
		assertThat(after).as("settlement_worker: every other column unchanged")
			.usingRecursiveComparison()
			.ignoringFields("status", "updatedAt")
			.isEqualTo(before);
	}

	static Stream<Arguments> invalidTransitions() {
		return Stream.of(Arguments.of("AUTHORIZED", "AUTHORIZED"), Arguments.of("SETTLED", "SETTLED"),
				Arguments.of("SETTLED", "FAILED"), Arguments.of("SETTLED", "AUTHORIZED"),
				Arguments.of("FAILED", "FAILED"), Arguments.of("FAILED", "SETTLED"),
				Arguments.of("FAILED", "AUTHORIZED"));
	}

	@ParameterizedTest(name = "{0} -> {1}")
	@MethodSource("invalidTransitions")
	void workerCannotMakeAnyOtherTransition(String from, String to) throws SQLException {
		Payment before = newPaymentIn(from);

		SQLException e = attempt(Role.WORKER, "UPDATE payments SET status = ? WHERE id = ?", to, before.id());

		assertThat((Throwable) e).as("settlement_worker: %s -> %s must be rejected by the statement", from, to)
			.isNotNull();
		assertTransitionRejected(e, Role.WORKER,
				"payment " + before.id() + ": status " + from + " -> " + to + " is not allowed");
		assertThat(read(before.id())).as("settlement_worker: payment after rejected %s -> %s", from, to)
			.isEqualTo(before);
	}

	// Spec §4: a payment already handled matches no row, so a redelivered settlement does
	// nothing and raises nothing.
	@Test
	void conditionalTransitionOfHandledPaymentMatchesNoRow() throws SQLException {
		Payment before = newPaymentIn("SETTLED");
		try (Connection conn = connect(Role.WORKER)) {
			try {
				try (PreparedStatement ps = conn.prepareStatement("""
						UPDATE payments SET status = 'SETTLED' WHERE id = ? AND status = 'AUTHORIZED'
						RETURNING customer_account_id, merchant_account_id, amount, fee""")) {
					ps.setObject(1, before.id());
					try (ResultSet rs = ps.executeQuery()) {
						assertThat(rs.next()).as("settlement_worker: rows returned for a SETTLED payment").isFalse();
					}
				}
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}
		assertThat(read(before.id())).as("settlement_worker: payment after the no-op transition").isEqualTo(before);
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(OwnerDeletion.class)
	void ownerCannotDeleteOrTruncatePayments(OwnerDeletion deletion) throws SQLException {
		Payment before = newPayment();
		long countBefore = paymentCount();

		SQLException e = deletion.bindsId ? attempt(Role.OWNER, deletion.sql, before.id())
				: attempt(Role.OWNER, deletion.sql);

		assertThat((Throwable) e).as("ledger_api_owner: %s must be rejected by the statement", deletion).isNotNull();
		ServerErrorMessage server = rootError(e, Role.OWNER, RESTRICT_VIOLATION);
		assertThat(server.getMessage()).as("ledger_api_owner: server message").isEqualTo("payments is append-only");
		assertThat(read(before.id())).as("ledger_api_owner: payment after %s", deletion).isEqualTo(before);
		assertThat(paymentCount()).as("ledger_api_owner: payments after %s", deletion).isEqualTo(countBefore);
	}

	@Test
	void ownerCannotChangeAmountWithoutTransition() throws SQLException {
		Payment before = newPayment();

		SQLException e = attempt(Role.OWNER, "UPDATE payments SET amount = amount + 1 WHERE id = ?", before.id());

		assertThat((Throwable) e).as("ledger_api_owner: amount change must be rejected by the statement").isNotNull();
		assertTransitionRejected(e, Role.OWNER,
				"payment " + before.id() + ": status AUTHORIZED -> AUTHORIZED is not allowed");
		assertThat(read(before.id())).as("ledger_api_owner: payment after rejected amount change").isEqualTo(before);
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(ColumnChange.class)
	void ownerCannotChangeAnotherColumnAlongWithTransition(ColumnChange change) throws SQLException {
		Payment before = newPayment();
		long otherAccount = createAccount("CUSTOMER");
		String sql = "UPDATE payments SET status = 'SETTLED', " + change.assignment.formatted(otherAccount)
				+ " WHERE id = ?";

		SQLException e = attempt(Role.OWNER, sql, before.id());

		assertThat((Throwable) e).as("ledger_api_owner: %s with the transition must be rejected", change).isNotNull();
		assertTransitionRejected(e, Role.OWNER, "payment " + before.id() + ": only status may change");
		assertThat(read(before.id())).as("ledger_api_owner: payment after rejected %s", change).isEqualTo(before);
	}

	// The trigger, not the caller, sets updated_at (D-071).
	@Test
	void triggerOverwritesSuppliedUpdatedAt() throws SQLException {
		Payment before = newPayment();
		OffsetDateTime transitionedAt;
		try (Connection conn = connect(Role.OWNER)) {
			try {
				transitionedAt = dbNow(conn);
				assertThat(update(conn,
						"UPDATE payments SET status = 'SETTLED', updated_at = '2000-01-01T00:00:00Z' WHERE id = ?",
						before.id()))
					.as("ledger_api_owner: rows moved to SETTLED")
					.isEqualTo(1);
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}

		Payment after = read(before.id());
		assertThat(after.status()).as("ledger_api_owner: status").isEqualTo("SETTLED");
		assertThat(after.updatedAt()).as("ledger_api_owner: updated_at is the transition's now(), not the supplied value")
			.isEqualTo(transitionedAt);
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(InvalidInsert.class)
	void appInsertViolatingCheckIsRejected(InvalidInsert insert) throws SQLException {
		long customer = createAccount("CUSTOMER");
		long merchant = insert.sameAccounts ? customer : createAccount("MERCHANT");

		SQLException e = attempt(Role.APP,
				"INSERT INTO payments (customer_account_id, merchant_account_id, amount, fee) VALUES (?, ?, ?, ?)",
				customer, merchant, new BigDecimal(insert.amount), new BigDecimal(insert.fee));

		assertThat((Throwable) e).as("ledger_api_app: %s must be rejected", insert).isNotNull();
		ServerErrorMessage server = rootError(e, Role.APP, CHECK_VIOLATION);
		assertThat(server.getConstraint()).as("ledger_api_app: constraint").isEqualTo(insert.constraint);
		assertThat(server.getTable()).as("ledger_api_app: table").isEqualTo("payments");
		assertThat(paymentsOf(customer)).as("ledger_api_app: payments of customer %d", customer).isZero();
	}

	private static void assertTransitionRejected(SQLException thrown, Role role, String message) {
		ServerErrorMessage server = rootError(thrown, role, CHECK_VIOLATION);
		assertThat(server.getConstraint()).as("%s: constraint", role.dbRole).isEqualTo(TRANSITION);
		assertThat(server.getSchema()).as("%s: schema", role.dbRole).isEqualTo("public");
		assertThat(server.getTable()).as("%s: table", role.dbRole).isEqualTo("payments");
		assertThat(server.getMessage()).as("%s: server message", role.dbRole).isEqualTo(message);
	}

	// SQLState and server error of the deepest SQLException in the cause chain.
	private static ServerErrorMessage rootError(SQLException thrown, Role role, String sqlState) {
		SQLException root = thrown;
		for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
			if (cause instanceof SQLException sql) {
				root = sql;
			}
		}
		assertThat(root.getSQLState()).as("%s: SQLState of %s", role.dbRole, root).isEqualTo(sqlState);
		assertThat((Throwable) root).as("%s: driver exception type", role.dbRole).isInstanceOf(PSQLException.class);
		return ((PSQLException) root).getServerErrorMessage();
	}

	// Runs one statement in its own transaction and returns what it threw (or null). The
	// connection is rolled back and closed before returning, releasing its locks.
	private SQLException attempt(Role role, String sql, Object... params) throws SQLException {
		try (Connection conn = connect(role)) {
			try {
				return catchThrowableOfType(SQLException.class, () -> update(conn, sql, params));
			}
			finally {
				conn.rollback();
			}
		}
	}

	private static int update(Connection conn, String sql, Object... params) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				ps.setObject(i + 1, params[i]);
			}
			return ps.executeUpdate();
		}
	}

	// A committed AUTHORIZED payment of 100.00, fee 2.90, between fresh accounts.
	private Payment newPayment() throws SQLException {
		long customer = createAccount("CUSTOMER");
		long merchant = createAccount("MERCHANT");
		try (Connection conn = connect(Role.APP)) {
			try {
				UUID id = insertPayment(conn, customer, merchant, "100.00", "2.90");
				conn.commit();
				return read(id);
			}
			finally {
				conn.rollback();
			}
		}
	}

	// A committed payment in the given status, moved there by the worker if not AUTHORIZED.
	private Payment newPaymentIn(String status) throws SQLException {
		Payment payment = newPayment();
		if (status.equals("AUTHORIZED")) {
			return payment;
		}
		try (Connection conn = connect(Role.WORKER)) {
			try {
				assertThat(update(conn, "UPDATE payments SET status = ? WHERE id = ?", status, payment.id()))
					.isEqualTo(1);
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}
		Payment moved = read(payment.id());
		assertThat(moved.status()).as("fixture status").isEqualTo(status);
		return moved;
	}

	private static UUID insertPayment(Connection conn, long customer, long merchant, String amount, String fee)
			throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO payments (customer_account_id, merchant_account_id, amount, fee) VALUES (?, ?, ?, ?)",
				new String[] { "id" })) {
			ps.setLong(1, customer);
			ps.setLong(2, merchant);
			ps.setBigDecimal(3, new BigDecimal(amount));
			ps.setBigDecimal(4, new BigDecimal(fee));
			assertThat(ps.executeUpdate()).isEqualTo(1);
			try (ResultSet keys = ps.getGeneratedKeys()) {
				assertThat(keys.next()).isTrue();
				return keys.getObject(1, UUID.class);
			}
		}
	}

	private long createAccount(String type) throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try (PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO accounts (name, type, allow_negative) VALUES (?, ?, false)", new String[] { "id" })) {
				ps.setString(1, "m3t1-" + UUID.randomUUID());
				ps.setString(2, type);
				assertThat(ps.executeUpdate()).isEqualTo(1);
				try (ResultSet keys = ps.getGeneratedKeys()) {
					assertThat(keys.next()).isTrue();
					long id = keys.getLong(1);
					conn.commit();
					return id;
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

	// Fresh app connection; read-only, rolled back.
	private Payment read(UUID id) throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try (PreparedStatement ps = conn.prepareStatement("""
					SELECT id, customer_account_id, merchant_account_id, amount, fee, status, created_at, updated_at
					FROM payments WHERE id = ?""")) {
				ps.setObject(1, id);
				try (ResultSet rs = ps.executeQuery()) {
					assertThat(rs.next()).as("payment %s exists", id).isTrue();
					return new Payment(rs.getObject("id", UUID.class), rs.getLong("customer_account_id"),
							rs.getLong("merchant_account_id"), rs.getBigDecimal("amount"), rs.getBigDecimal("fee"),
							rs.getString("status"), rs.getObject("created_at", OffsetDateTime.class),
							rs.getObject("updated_at", OffsetDateTime.class));
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

	private long paymentCount() throws SQLException {
		return queryLong("SELECT count(*) FROM payments");
	}

	private long paymentsOf(long customer) throws SQLException {
		return queryLong("SELECT count(*) FROM payments WHERE customer_account_id = " + customer);
	}

	private long queryLong(String sql) throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getLong(1);
			}
			finally {
				conn.rollback();
			}
		}
	}

	private static OffsetDateTime dbNow(Connection conn) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement("SELECT now()"); ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).isTrue();
			return rs.getObject(1, OffsetDateTime.class);
		}
	}

	// Connection as the given role, autoCommit off; asserts the session really is that role.
	private Connection connect(Role role) throws SQLException {
		Connection conn = switch (role) {
			case APP -> dataSource.getConnection();
			case WORKER -> DriverManager.getConnection(postgres.getJdbcUrl(), role.dbRole,
					TestcontainersConfiguration.WORKER_PASSWORD);
			case OWNER -> DriverManager.getConnection(postgres.getJdbcUrl(), role.dbRole,
					TestcontainersConfiguration.OWNER_PASSWORD);
		};
		try (PreparedStatement ps = conn.prepareStatement("SELECT current_user");
				ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).isTrue();
			assertThat(rs.getString(1)).as("current_user").isEqualTo(role.dbRole);
			conn.setAutoCommit(false);
			return conn;
		}
		catch (SQLException | RuntimeException | Error e) {
			conn.close();
			throw e;
		}
	}

}
