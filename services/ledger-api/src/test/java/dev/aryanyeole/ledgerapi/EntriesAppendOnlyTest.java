package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.util.PSQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.postgresql.PostgreSQLContainer;

// Invariant 2 proof: entries and transactions are append-only (D-041, D-042).
// Runtime roles are stopped by missing grants (42501); the owner, which has the privileges,
// is stopped by the statement-level BEFORE triggers (23001). Either way the statement
// itself fails, not commit(). Attempts run with autoCommit=false and are always rolled
// back, never committed: in the failing-first run (no V3) they succeed, and the rollback
// undoes them. The attempt connection is rolled back and closed before any snapshot is
// taken, because an aborted transaction (and an owner TRUNCATE) keeps its locks until then.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class EntriesAppendOnlyTest {

	private static final String INSUFFICIENT_PRIVILEGE = "42501";

	private static final String RESTRICT_VIOLATION = "23001";

	enum Role {

		APP("ledger_api_app"), WORKER("settlement_worker"), OWNER("ledger_api_owner");

		final String dbRole;

		Role(String dbRole) {
			this.dbRole = dbRole;
		}

	}

	// Which real row id the statement binds, if any.
	enum Target {

		ENTRY, TRANSACTION, NONE

	}

	enum RuntimeAttempt {

		UPDATE_ENTRY("UPDATE entries SET amount = amount WHERE id = ?", Target.ENTRY, "entries"),
		DELETE_ENTRY("DELETE FROM entries WHERE id = ?", Target.ENTRY, "entries"),
		TRUNCATE_ENTRIES("TRUNCATE entries", Target.NONE, "entries"),
		UPDATE_TRANSACTION("UPDATE transactions SET reference = 'x' WHERE id = ?", Target.TRANSACTION, "transactions"),
		DELETE_TRANSACTION("DELETE FROM transactions WHERE id = ?", Target.TRANSACTION, "transactions"),
		TRUNCATE_TRANSACTIONS("TRUNCATE transactions", Target.NONE, "transactions");

		final String sql;

		final Target target;

		final String table;

		RuntimeAttempt(String sql, Target target, String table) {
			this.sql = sql;
			this.target = target;
			this.table = table;
		}

	}

	enum OwnerAttempt {

		UPDATE_ENTRY("UPDATE entries SET amount = amount WHERE id = ?", Target.ENTRY, "entries"),
		UPDATE_NO_ROWS("UPDATE entries SET amount = amount WHERE false", Target.NONE, "entries"),
		DELETE_ENTRY("DELETE FROM entries WHERE id = ?", Target.ENTRY, "entries"),
		TRUNCATE_ENTRIES("TRUNCATE entries", Target.NONE, "entries"),
		TRUNCATE_ACCOUNTS_CASCADE("TRUNCATE accounts CASCADE", Target.NONE, "entries"),
		UPDATE_TRANSACTION("UPDATE transactions SET reference = 'x' WHERE id = ?", Target.TRANSACTION, "transactions"),
		DELETE_TRANSACTION("DELETE FROM transactions WHERE id = ?", Target.TRANSACTION, "transactions"),
		TRUNCATE_TRANSACTIONS_CASCADE("TRUNCATE transactions CASCADE", Target.NONE, "transactions");

		final String sql;

		final Target target;

		// The table whose trigger rejects the statement.
		final String table;

		OwnerAttempt(String sql, Target target, String table) {
			this.sql = sql;
			this.target = target;
			this.table = table;
		}

	}

	record Posting(long transactionId, long entryId, long externalId, long customerId) {
	}

	record Snapshot(BigDecimal entryAmount, long transactionEntries, String reference, long accounts, long entries) {
	}

	@Autowired
	DataSource dataSource;

	@Autowired
	PostgreSQLContainer postgres;

	static Stream<Arguments> runtimeAttempts() {
		return Stream.of(Role.APP, Role.WORKER)
			.flatMap(role -> Arrays.stream(RuntimeAttempt.values()).map(attempt -> Arguments.of(role, attempt)));
	}

	@ParameterizedTest(name = "{0} {1}")
	@MethodSource("runtimeAttempts")
	void runtimeRolesLackMutationPrivileges(Role role, RuntimeAttempt attempt) throws SQLException {
		Posting posting = postDeposit();
		Snapshot before = snapshot(posting);

		SQLException e = attempt(role, attempt.sql, bindId(attempt.target, posting));

		assertThat((Throwable) e).as("%s: %s must be rejected by the statement itself", role.dbRole, attempt)
			.isNotNull();
		assertRootError(e, role, INSUFFICIENT_PRIVILEGE, "permission denied for table " + attempt.table);
		assertUnchanged(role, attempt.name(), before, snapshot(posting));
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(OwnerAttempt.class)
	void ownerIsStoppedByAppendOnlyTriggers(OwnerAttempt attempt) throws SQLException {
		Posting posting = postDeposit();
		Snapshot before = snapshot(posting);

		SQLException e = attempt(Role.OWNER, attempt.sql, bindId(attempt.target, posting));

		assertThat((Throwable) e).as("%s: %s must be rejected by the statement itself", Role.OWNER.dbRole, attempt)
			.isNotNull();
		assertRootError(e, Role.OWNER, RESTRICT_VIOLATION, attempt.table + " is append-only");
		assertUnchanged(Role.OWNER, attempt.name(), before, snapshot(posting));
	}

	// Corrections are new reversing transactions, never edits (invariant 2).
	@ParameterizedTest(name = "{0}")
	@EnumSource(value = Role.class, names = { "APP", "WORKER" })
	void reversingPostingCorrectsWithoutMutation(Role role) throws SQLException {
		BigDecimal[] externalBefore;
		try (Connection conn = connect(Role.APP)) {
			externalBefore = balanceAndEntrySum(conn, seededExternal());
		}
		Posting original = postDeposit();
		Map<Long, BigDecimal> originalEntries;
		try (Connection conn = connect(Role.APP)) {
			originalEntries = entriesById(conn, original.transactionId());
		}

		long reversalId;
		try (Connection conn = connect(role)) {
			try {
				reversalId = insertTransaction(conn, "TRANSFER", "reversal-of:" + original.transactionId());
				insertEntry(conn, reversalId, original.customerId(), "-100.0000");
				insertEntry(conn, reversalId, original.externalId(), "100.0000");
				lockAndApplyBalances(conn, Map.of(original.customerId(), new BigDecimal("-100.0000"),
						original.externalId(), new BigDecimal("100.0000")));
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}

		try (Connection conn = connect(role)) {
			// The customer is fresh, so it returns to 0; the shared EXTERNAL returns to where it was.
			Map<Long, BigDecimal[]> expected = Map.of(original.customerId(),
					new BigDecimal[] { BigDecimal.ZERO, BigDecimal.ZERO }, original.externalId(), externalBefore);
			for (Map.Entry<Long, BigDecimal[]> account : expected.entrySet()) {
				long accountId = account.getKey();
				BigDecimal[] balanceAndSum = balanceAndEntrySum(conn, accountId);
				assertThat(balanceAndSum[0].compareTo(account.getValue()[0]))
					.as("%s: account %d balance", role.dbRole, accountId)
					.isZero();
				assertThat(balanceAndSum[1].compareTo(account.getValue()[1]))
					.as("%s: account %d entry sum", role.dbRole, accountId)
					.isZero();
			}

			Map<Long, BigDecimal> afterEntries = entriesById(conn, original.transactionId());
			assertThat(afterEntries.keySet()).as("%s: original entry ids", role.dbRole)
				.containsExactlyElementsOf(originalEntries.keySet());
			for (Map.Entry<Long, BigDecimal> entry : originalEntries.entrySet()) {
				assertThat(afterEntries.get(entry.getKey()).compareTo(entry.getValue()))
					.as("%s: original entry %d amount", role.dbRole, entry.getKey())
					.isZero();
			}

			try (PreparedStatement ps = conn
				.prepareStatement("SELECT count(*) FROM entries WHERE transaction_id IN (?, ?)")) {
				ps.setLong(1, original.transactionId());
				ps.setLong(2, reversalId);
				try (ResultSet rs = ps.executeQuery()) {
					assertThat(rs.next()).isTrue();
					assertThat(rs.getLong(1)).as("%s: entries across original and reversal", role.dbRole)
						.isEqualTo(4);
				}
			}
		}
	}

	// Runs one mutation in its own transaction and returns what the statement threw (or
	// null). The connection is rolled back and closed before returning, releasing its locks.
	private SQLException attempt(Role role, String sql, Long id) throws SQLException {
		try (Connection conn = connect(role)) {
			try {
				return catchThrowableOfType(SQLException.class, () -> {
					try (PreparedStatement ps = conn.prepareStatement(sql)) {
						if (id != null) {
							ps.setLong(1, id);
						}
						ps.execute();
					}
				});
			}
			finally {
				conn.rollback();
			}
		}
	}

	private static Long bindId(Target target, Posting posting) {
		return switch (target) {
			case ENTRY -> posting.entryId();
			case TRANSACTION -> posting.transactionId();
			case NONE -> null;
		};
	}

	// Committed EXTERNAL -100.0000 -> CUSTOMER +100.0000 via the app role, balances updated.
	// The target entry is the CUSTOMER leg.
	private Posting postDeposit() throws SQLException {
		long external = seededExternal();
		long customer = createAccount("CUSTOMER");
		try (Connection conn = connect(Role.APP)) {
			try {
				long txId = insertTransaction(conn, "DEPOSIT", "t6-" + UUID.randomUUID());
				insertEntry(conn, txId, external, "-100.0000");
				long customerEntry = insertEntry(conn, txId, customer, "100.0000");
				lockAndApplyBalances(conn,
						Map.of(external, new BigDecimal("-100.0000"), customer, new BigDecimal("100.0000")));
				conn.commit();
				return new Posting(txId, customerEntry, external, customer);
			}
			finally {
				conn.rollback();
			}
		}
	}

	// Fresh app connection; lock_timeout fails fast if a lock is still held. SET LOCAL ends
	// with the transaction, so the pooled session keeps its defaults.
	private Snapshot snapshot(Posting posting) throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				try (Statement st = conn.createStatement()) {
					st.execute("SET LOCAL lock_timeout = '5s'");
				}
				try (PreparedStatement ps = conn.prepareStatement("""
						SELECT (SELECT amount FROM entries WHERE id = ?),
						       (SELECT count(*) FROM entries WHERE transaction_id = ?),
						       (SELECT reference FROM transactions WHERE id = ?),
						       (SELECT count(*) FROM accounts),
						       (SELECT count(*) FROM entries)""")) {
					ps.setLong(1, posting.entryId());
					ps.setLong(2, posting.transactionId());
					ps.setLong(3, posting.transactionId());
					try (ResultSet rs = ps.executeQuery()) {
						assertThat(rs.next()).isTrue();
						return new Snapshot(rs.getBigDecimal(1), rs.getLong(2), rs.getString(3), rs.getLong(4),
								rs.getLong(5));
					}
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

	private static void assertUnchanged(Role role, String attempt, Snapshot before, Snapshot after) {
		String who = role.dbRole + ": after " + attempt;
		assertThat(after.entryAmount()).as("%s, target entry amount", who).isNotNull();
		assertThat(after.entryAmount().compareTo(before.entryAmount())).as("%s, target entry amount", who).isZero();
		assertThat(after.transactionEntries()).as("%s, transaction entry count", who)
			.isEqualTo(before.transactionEntries())
			.isEqualTo(2);
		assertThat(after.reference()).as("%s, transaction reference", who).isEqualTo(before.reference());
		assertThat(after.accounts()).as("%s, accounts count", who).isEqualTo(before.accounts());
		assertThat(after.entries()).as("%s, entries count", who).isEqualTo(before.entries());
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

	// The one EXTERNAL account, seeded by V4 (D-048): a second EXTERNAL cannot be created.
	// Shared across tests, so assertions on it are per transaction or relative to its
	// balance before the test.
	private long seededExternal() throws SQLException {
		try (Connection conn = connect(Role.APP);
				PreparedStatement ps = conn.prepareStatement("SELECT id FROM accounts WHERE type = 'EXTERNAL'");
				ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).as("seeded EXTERNAL account").isTrue();
			return rs.getLong(1);
		}
	}

	private long createAccount(String type) throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try (PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO accounts (name, type, allow_negative) VALUES (?, ?, ?)", new String[] { "id" })) {
				ps.setString(1, "t6-" + UUID.randomUUID());
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

	private static long insertTransaction(Connection conn, String kind, String reference) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement("INSERT INTO transactions (kind, reference) VALUES (?, ?)",
				new String[] { "id" })) {
			ps.setString(1, kind);
			ps.setString(2, reference);
			assertThat(ps.executeUpdate()).isEqualTo(1);
			return generatedId(ps);
		}
	}

	private static long insertEntry(Connection conn, long txId, long accountId, String amount) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO entries (transaction_id, account_id, amount) VALUES (?, ?, ?)", new String[] { "id" })) {
			ps.setLong(1, txId);
			ps.setLong(2, accountId);
			ps.setBigDecimal(3, new BigDecimal(amount));
			assertThat(ps.executeUpdate()).isEqualTo(1);
			return generatedId(ps);
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

	private static Map<Long, BigDecimal> entriesById(Connection conn, long txId) throws SQLException {
		Map<Long, BigDecimal> amounts = new LinkedHashMap<>();
		try (PreparedStatement ps = conn
			.prepareStatement("SELECT id, amount FROM entries WHERE transaction_id = ? ORDER BY id")) {
			ps.setLong(1, txId);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					amounts.put(rs.getLong(1), rs.getBigDecimal(2));
				}
			}
		}
		return amounts;
	}

	private static BigDecimal[] balanceAndEntrySum(Connection conn, long accountId) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(
				"SELECT a.balance, (SELECT COALESCE(SUM(e.amount), 0) FROM entries e WHERE e.account_id = a.id) FROM accounts a WHERE a.id = ?")) {
			ps.setLong(1, accountId);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return new BigDecimal[] { rs.getBigDecimal(1), rs.getBigDecimal(2) };
			}
		}
	}

	private static long generatedId(PreparedStatement ps) throws SQLException {
		try (ResultSet keys = ps.getGeneratedKeys()) {
			assertThat(keys.next()).isTrue();
			return keys.getLong(1);
		}
	}

	// SQLState and server message of the deepest SQLException in the cause chain.
	private static void assertRootError(SQLException thrown, Role role, String sqlState, String message) {
		SQLException root = thrown;
		for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
			if (cause instanceof SQLException sql) {
				root = sql;
			}
		}
		assertThat(root.getSQLState()).as("%s: SQLState of %s", role.dbRole, root).isEqualTo(sqlState);
		assertThat((Throwable) root).as("%s: driver exception type", role.dbRole).isInstanceOf(PSQLException.class);
		assertThat(((PSQLException) root).getServerErrorMessage().getMessage()).as("%s: server message", role.dbRole)
			.isEqualTo(message);
	}

}
