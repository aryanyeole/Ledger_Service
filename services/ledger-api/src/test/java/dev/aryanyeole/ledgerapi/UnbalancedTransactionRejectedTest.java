package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.postgresql.util.PSQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.postgresql.PostgreSQLContainer;

// P1 proof (invariant 1): the database rejects any transaction whose entries do not sum
// to 0, at COMMIT, for both writers. Every case runs as ledger_api_app (app DataSource)
// and as settlement_worker (its own JDBC connection). Postings use raw JDBC with
// autoCommit=false so the deferred triggers fire on an explicit conn.commit(); a
// rolled-back test transaction would never fire them.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class UnbalancedTransactionRejectedTest {

	private static final String CHECK_VIOLATION = "23514";

	enum Role {

		APP("ledger_api_app"), WORKER("settlement_worker");

		final String dbRole;

		Role(String dbRole) {
			this.dbRole = dbRole;
		}

	}

	@Autowired
	DataSource dataSource;

	@Autowired
	PostgreSQLContainer postgres;

	@ParameterizedTest(name = "{0}")
	@EnumSource(Role.class)
	void balancedPostingCommits(Role role) throws SQLException {
		long external = seededExternal();
		long customer = createAccount("CUSTOMER");

		long txId = postBalancedDeposit(role, external, customer);

		try (Connection conn = connect(role)) {
			Map<Long, BigDecimal> entries = entriesByAccount(conn, txId);
			assertThat(entries).containsOnlyKeys(external, customer);
			assertThat(entries.get(external)).isEqualByComparingTo("-100.0000");
			assertThat(entries.get(customer)).isEqualByComparingTo("100.0000");
			assertBalancesMatchEntries(conn, role, external, customer);
		}
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(Role.class)
	void decimalAmountsBalanceExactly(Role role) throws SQLException {
		long external = seededExternal();
		long customerA = createAccount("CUSTOMER");
		long customerB = createAccount("CUSTOMER");
		String reference = newReference();

		long txId;
		try (Connection conn = connect(role)) {
			try {
				txId = insertTransaction(conn, reference);
				insertEntry(conn, txId, external, "-0.3");
				insertEntry(conn, txId, customerA, "0.1");
				insertEntry(conn, txId, customerB, "0.2");
				lockAndApplyBalances(conn, Map.of(external, new BigDecimal("-0.3"), customerA,
						new BigDecimal("0.1"), customerB, new BigDecimal("0.2")));
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}

		try (Connection conn = connect(role)) {
			assertThat(entrySum(conn, txId).compareTo(BigDecimal.ZERO)).isZero();
			assertBalancesMatchEntries(conn, role, external, customerA, customerB);
		}
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(Role.class)
	void unbalancedPairIsRejectedAtCommitNotInsert(Role role) throws SQLException {
		long external = seededExternal();
		long customer = createAccount("CUSTOMER");
		String reference = newReference();

		long txId;
		try (Connection conn = connect(role)) {
			try {
				txId = insertTransaction(conn, reference);
				insertEntry(conn, txId, customer, "100.00");
				insertEntry(conn, txId, external, "-99.99");

				SQLException e = catchThrowableOfType(SQLException.class, conn::commit);
				assertThat((Throwable) e).as("[%s] deferred trigger must reject at commit()", role.dbRole).isNotNull();
				assertCheckViolation(e, role, "unbalanced transaction " + txId + ":");
			}
			finally {
				conn.rollback();
			}
		}

		assertNothingLeftBehind(role, reference, txId);
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(Role.class)
	void singleEntryIsRejected(Role role) throws SQLException {
		long customer = createAccount("CUSTOMER");
		String reference = newReference();

		long txId;
		try (Connection conn = connect(role)) {
			try {
				txId = insertTransaction(conn, reference);
				insertEntry(conn, txId, customer, "100");

				SQLException e = catchThrowableOfType(SQLException.class, conn::commit);
				assertThat((Throwable) e).as("[%s] deferred trigger must reject at commit()", role.dbRole).isNotNull();
				assertCheckViolation(e, role, "unbalanced transaction " + txId + ":");
			}
			finally {
				conn.rollback();
			}
		}

		assertNothingLeftBehind(role, reference, txId);
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(Role.class)
	void transactionWithoutEntriesIsRejected(Role role) throws SQLException {
		String reference = newReference();

		long txId;
		try (Connection conn = connect(role)) {
			try {
				txId = insertTransaction(conn, reference);

				SQLException e = catchThrowableOfType(SQLException.class, conn::commit);
				assertThat((Throwable) e).as("[%s] deferred trigger must reject at commit()", role.dbRole).isNotNull();
				assertCheckViolation(e, role, "transaction " + txId + " has no entries");
			}
			finally {
				conn.rollback();
			}
		}

		assertNothingLeftBehind(role, reference, txId);
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(Role.class)
	void lateUnbalancedAppendIsRejected(Role role) throws SQLException {
		long external = seededExternal();
		long customer = createAccount("CUSTOMER");
		long txId = postBalancedDeposit(role, external, customer);

		try (Connection conn = connect(role)) {
			try {
				insertEntry(conn, txId, customer, "1.0000");

				SQLException e = catchThrowableOfType(SQLException.class, conn::commit);
				assertThat((Throwable) e).as("[%s] deferred trigger must reject at commit()", role.dbRole).isNotNull();
				assertCheckViolation(e, role, "unbalanced transaction " + txId + ":");
			}
			finally {
				conn.rollback();
			}
		}

		// Fresh connection, same role, after the failed one is closed.
		try (Connection conn = connect(role)) {
			assertThat(entryCount(conn, txId)).isEqualTo(2);
			assertThat(entrySum(conn, txId).compareTo(BigDecimal.ZERO)).isZero();
			assertBalancesMatchEntries(conn, role, external, customer);
		}
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(Role.class)
	void fourthDecimalImbalanceIsRejected(Role role) throws SQLException {
		long external = seededExternal();
		long customer = createAccount("CUSTOMER");
		String reference = newReference();

		long txId;
		try (Connection conn = connect(role)) {
			try {
				txId = insertTransaction(conn, reference);
				insertEntry(conn, txId, customer, "100.0000");
				insertEntry(conn, txId, external, "-99.9999");

				SQLException e = catchThrowableOfType(SQLException.class, conn::commit);
				assertThat((Throwable) e).as("[%s] deferred trigger must reject at commit()", role.dbRole).isNotNull();
				assertCheckViolation(e, role, "unbalanced transaction " + txId + ":");
			}
			finally {
				conn.rollback();
			}
		}

		assertNothingLeftBehind(role, reference, txId);
	}

	// EXTERNAL -100.0000 -> CUSTOMER +100.0000 with both balances updated, committed.
	private long postBalancedDeposit(Role role, long external, long customer) throws SQLException {
		try (Connection conn = connect(role)) {
			try {
				long txId = insertTransaction(conn, newReference());
				insertEntry(conn, txId, external, "-100.0000");
				insertEntry(conn, txId, customer, "100.0000");
				lockAndApplyBalances(conn,
						Map.of(external, new BigDecimal("-100.0000"), customer, new BigDecimal("100.0000")));
				conn.commit();
				return txId;
			}
			finally {
				conn.rollback();
			}
		}
	}

	// Connection as the given role, autoCommit off; asserts the session really is that role.
	private Connection connect(Role role) throws SQLException {
		Connection conn = switch (role) {
			case APP -> dataSource.getConnection();
			case WORKER -> DriverManager.getConnection(postgres.getJdbcUrl(), Role.WORKER.dbRole,
					TestcontainersConfiguration.WORKER_PASSWORD);
		};
		try (PreparedStatement ps = conn.prepareStatement("SELECT current_user");
				ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).isTrue();
			assertThat(rs.getString(1)).as("[%s] current_user", role.dbRole).isEqualTo(role.dbRole);
			conn.setAutoCommit(false);
			return conn;
		}
		catch (SQLException | RuntimeException | Error e) {
			conn.close();
			throw e;
		}
	}

	// The one EXTERNAL account, seeded by V4 (D-048): a second EXTERNAL cannot be created.
	// Shared across tests, so assertions on it are per transaction or balance = SUM(entries).
	private long seededExternal() throws SQLException {
		try (Connection conn = dataSource.getConnection();
				PreparedStatement ps = conn.prepareStatement("SELECT id FROM accounts WHERE type = 'EXTERNAL'");
				ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).as("seeded EXTERNAL account").isTrue();
			return rs.getLong(1);
		}
	}

	// Accounts are created as ledger_api_app: settlement_worker has no INSERT on accounts.
	private long createAccount(String type) throws SQLException {
		try (Connection conn = dataSource.getConnection()) {
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO accounts (name, type, allow_negative) VALUES (?, ?, ?)", new String[] { "id" })) {
				ps.setString(1, "t5-" + UUID.randomUUID());
				ps.setString(2, type);
				ps.setBoolean(3, type.equals("EXTERNAL")); // D-039
				assertThat(ps.executeUpdate()).isEqualTo(1);
				long id = generatedId(ps);
				conn.commit();
				return id;
			}
			finally {
				conn.rollback();
			}
		}
	}

	private static long insertTransaction(Connection conn, String reference) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO transactions (kind, reference) VALUES ('DEPOSIT', ?)", new String[] { "id" })) {
			ps.setString(1, reference);
			assertThat(ps.executeUpdate()).isEqualTo(1);
			return generatedId(ps);
		}
	}

	private static void insertEntry(Connection conn, long txId, long accountId, String amount) throws SQLException {
		try (PreparedStatement ps = conn
			.prepareStatement("INSERT INTO entries (transaction_id, account_id, amount) VALUES (?, ?, ?)")) {
			ps.setLong(1, txId);
			ps.setLong(2, accountId);
			ps.setBigDecimal(3, new BigDecimal(amount));
			assertThat(ps.executeUpdate()).isEqualTo(1);
		}
	}

	// Locks the accounts in ascending id order (invariant 4), then applies each delta.
	private static void lockAndApplyBalances(Connection conn, Map<Long, BigDecimal> deltas) throws SQLException {
		SortedMap<Long, BigDecimal> ordered = new TreeMap<>(deltas);
		List<Long> locked = new ArrayList<>();
		try (PreparedStatement ps = conn
			.prepareStatement("SELECT id FROM accounts WHERE id = ANY (?) ORDER BY id FOR UPDATE")) {
			ps.setArray(1, conn.createArrayOf("bigint", ordered.keySet().toArray()));
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					locked.add(rs.getLong(1));
				}
			}
		}
		assertThat(locked).containsExactlyElementsOf(ordered.keySet());

		try (PreparedStatement ps = conn.prepareStatement("UPDATE accounts SET balance = balance + ? WHERE id = ?")) {
			for (Map.Entry<Long, BigDecimal> delta : ordered.entrySet()) {
				ps.setBigDecimal(1, delta.getValue());
				ps.setLong(2, delta.getKey());
				assertThat(ps.executeUpdate()).isEqualTo(1);
			}
		}
	}

	private void assertNothingLeftBehind(Role role, String reference, long txId) throws SQLException {
		try (Connection conn = dataSource.getConnection();
				PreparedStatement ps = conn.prepareStatement(
						"SELECT (SELECT count(*) FROM transactions WHERE reference = ?), (SELECT count(*) FROM entries WHERE transaction_id = ?)")) {
			ps.setString(1, reference);
			ps.setLong(2, txId);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				assertThat(rs.getLong(1)).as("[%s] transactions with reference %s", role.dbRole, reference).isZero();
				assertThat(rs.getLong(2)).as("[%s] entries for transaction %d", role.dbRole, txId).isZero();
			}
		}
	}

	private static void assertBalancesMatchEntries(Connection conn, Role role, long... accountIds) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(
				"SELECT a.balance, (SELECT COALESCE(SUM(e.amount), 0) FROM entries e WHERE e.account_id = a.id) FROM accounts a WHERE a.id = ?")) {
			for (long accountId : accountIds) {
				ps.setLong(1, accountId);
				try (ResultSet rs = ps.executeQuery()) {
					assertThat(rs.next()).isTrue();
					BigDecimal balance = rs.getBigDecimal(1);
					BigDecimal entrySum = rs.getBigDecimal(2);
					assertThat(balance.compareTo(entrySum))
						.as("[%s] account %d balance %s vs entries %s", role.dbRole, accountId, balance, entrySum)
						.isZero();
				}
			}
		}
	}

	private static Map<Long, BigDecimal> entriesByAccount(Connection conn, long txId) throws SQLException {
		Map<Long, BigDecimal> amounts = new LinkedHashMap<>();
		try (PreparedStatement ps = conn
			.prepareStatement("SELECT account_id, amount FROM entries WHERE transaction_id = ? ORDER BY id")) {
			ps.setLong(1, txId);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					amounts.put(rs.getLong(1), rs.getBigDecimal(2));
				}
			}
		}
		return amounts;
	}

	private static long entryCount(Connection conn, long txId) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM entries WHERE transaction_id = ?")) {
			ps.setLong(1, txId);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getLong(1);
			}
		}
	}

	private static BigDecimal entrySum(Connection conn, long txId) throws SQLException {
		try (PreparedStatement ps = conn
			.prepareStatement("SELECT COALESCE(SUM(amount), 0) FROM entries WHERE transaction_id = ?")) {
			ps.setLong(1, txId);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getBigDecimal(1);
			}
		}
	}

	private static long generatedId(PreparedStatement ps) throws SQLException {
		try (ResultSet keys = ps.getGeneratedKeys()) {
			assertThat(keys.next()).isTrue();
			return keys.getLong(1);
		}
	}

	private static String newReference() {
		return "t5-" + UUID.randomUUID();
	}

	// SQLState and server message of the deepest SQLException in the cause chain.
	private static void assertCheckViolation(SQLException thrown, Role role, String messagePrefix) {
		SQLException root = thrown;
		for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
			if (cause instanceof SQLException sql) {
				root = sql;
			}
		}
		assertThat(root.getSQLState()).as("[%s] SQLState of %s", role.dbRole, root).isEqualTo(CHECK_VIOLATION);
		assertThat((Throwable) root).as("[%s] driver exception type", role.dbRole).isInstanceOf(PSQLException.class);
		assertThat(((PSQLException) root).getServerErrorMessage().getMessage()).as("[%s] server message", role.dbRole)
			.startsWith(messagePrefix);
	}

}
