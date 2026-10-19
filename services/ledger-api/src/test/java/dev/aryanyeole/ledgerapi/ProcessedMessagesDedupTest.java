package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.postgresql.PostgreSQLContainer;

// Proof (D-078): the consumer dedup row. The worker's statement is
//   INSERT INTO processed_messages (consumer_group, event_id) VALUES (?, ?)
//   ON CONFLICT (consumer_group, event_id) DO NOTHING
// with only SELECT and INSERT on (consumer_group, event_id), and it reads the outcome from the
// row count: 1 = first delivery, 0 = already processed. Dedup is per consumer group. A
// concurrent duplicate blocks on the first delivery's uncommitted key (observed with
// pg_blocking_pids, as in D-052), then does nothing if the first commits, or inserts if it
// rolls back. The owner, which has every privilege, can't update, delete or truncate dedup
// rows (23001, D-041). Every connection asserts current_user (D-036); groups and event ids are
// fresh per test. Failing first (D-038): -Dspring.flyway.target=12.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class ProcessedMessagesDedupTest {

	private static final String DEDUP = "INSERT INTO processed_messages (consumer_group, event_id) VALUES (?, ?) "
			+ "ON CONFLICT (consumer_group, event_id) DO NOTHING";

	private static final String PLAIN_INSERT = "INSERT INTO processed_messages (consumer_group, event_id) VALUES (?, ?)";

	private static final String CHECK_VIOLATION = "23514";

	private static final String UNIQUE_VIOLATION = "23505";

	private static final String RESTRICT_VIOLATION = "23001";

	private static final long BLOCK_TIMEOUT_MILLIS = 5000;

	enum Role {

		WORKER("settlement_worker"), OWNER("ledger_api_owner");

		final String dbRole;

		Role(String dbRole) {
			this.dbRole = dbRole;
		}

	}

	enum OwnerMutation {

		UPDATE_ROW("UPDATE processed_messages SET processed_at = now() WHERE consumer_group = ? AND event_id = ?", true),
		DELETE_ROW("DELETE FROM processed_messages WHERE consumer_group = ? AND event_id = ?", true),
		DELETE_NO_ROWS("DELETE FROM processed_messages WHERE false", false),
		TRUNCATE("TRUNCATE processed_messages", false);

		final String sql;

		final boolean bindsRow;

		OwnerMutation(String sql, boolean bindsRow) {
			this.sql = sql;
			this.bindsRow = bindsRow;
		}

	}

	@Autowired
	PostgreSQLContainer postgres;

	@Test
	void firstDeliveryInsertsAndRedeliveryDoesNothing() throws SQLException {
		String group = newGroup();
		UUID event = UUID.randomUUID();
		try (Connection conn = connect(Role.WORKER)) {
			try {
				assertThat(update(conn, DEDUP, group, event)).as("settlement_worker: first delivery").isEqualTo(1);
				assertThat(update(conn, DEDUP, group, event)).as("settlement_worker: redelivery").isZero();
			}
			finally {
				conn.rollback();
			}
		}
	}

	// The targeted form only absorbs its own conflict; a plain insert still fails loudly.
	@Test
	void plainDuplicateInsertViolatesPrimaryKey() throws SQLException {
		String group = newGroup();
		UUID event = UUID.randomUUID();
		try (Connection conn = connect(Role.WORKER)) {
			try {
				assertThat(update(conn, DEDUP, group, event)).isEqualTo(1);

				SQLException e = catchThrowableOfType(SQLException.class, () -> update(conn, PLAIN_INSERT, group, event));

				assertThat((Throwable) e).as("settlement_worker: plain duplicate insert must be rejected").isNotNull();
				assertThat(rootError(e, Role.WORKER, UNIQUE_VIOLATION).getConstraint())
					.as("settlement_worker: constraint")
					.isEqualTo("processed_messages_pkey");
			}
			finally {
				conn.rollback();
			}
		}
	}

	@Test
	void dedupIsPerConsumerGroup() throws SQLException {
		UUID event = UUID.randomUUID();
		try (Connection conn = connect(Role.WORKER)) {
			try {
				assertThat(update(conn, DEDUP, newGroup(), event)).as("settlement_worker: first group").isEqualTo(1);
				assertThat(update(conn, DEDUP, newGroup(), event)).as("settlement_worker: second group, same event")
					.isEqualTo(1);
			}
			finally {
				conn.rollback();
			}
		}
	}

	// Positive check of the column grant the conflict target needs.
	@Test
	void workerReadsBackDedupColumns() throws SQLException {
		String group = newGroup();
		UUID event = UUID.randomUUID();
		try (Connection conn = connect(Role.WORKER)) {
			try {
				assertThat(update(conn, DEDUP, group, event)).isEqualTo(1);
				try (PreparedStatement ps = conn.prepareStatement(
						"SELECT consumer_group, event_id FROM processed_messages WHERE consumer_group = ? AND event_id = ?")) {
					ps.setString(1, group);
					ps.setObject(2, event);
					try (ResultSet rs = ps.executeQuery()) {
						assertThat(rs.next()).as("settlement_worker: row visible through the column grant").isTrue();
						assertThat(rs.getString(1)).as("settlement_worker: consumer_group").isEqualTo(group);
						assertThat(rs.getObject(2, UUID.class)).as("settlement_worker: event_id").isEqualTo(event);
					}
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

	@Test
	void emptyConsumerGroupIsRejected() throws SQLException {
		try (Connection conn = connect(Role.WORKER)) {
			try {
				SQLException e = catchThrowableOfType(SQLException.class,
						() -> update(conn, DEDUP, "", UUID.randomUUID()));

				assertThat((Throwable) e).as("settlement_worker: empty consumer_group must be rejected").isNotNull();
				ServerErrorMessage server = rootError(e, Role.WORKER, CHECK_VIOLATION);
				assertThat(server.getConstraint()).as("settlement_worker: constraint")
					.isEqualTo("processed_messages_consumer_group_nonempty");
				assertThat(server.getTable()).as("settlement_worker: table").isEqualTo("processed_messages");
			}
			finally {
				conn.rollback();
			}
		}
	}

	// The worker can't read processed_at, so this one row is committed and read by the owner.
	@Test
	void processedAtComesFromTheInsertingTransaction() throws SQLException {
		String group = newGroup();
		UUID event = UUID.randomUUID();
		OffsetDateTime insertedAt;
		try (Connection conn = connect(Role.WORKER)) {
			try {
				insertedAt = dbNow(conn);
				assertThat(update(conn, DEDUP, group, event)).isEqualTo(1);
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}
		try (Connection conn = connect(Role.OWNER);
				PreparedStatement ps = conn.prepareStatement(
						"SELECT processed_at FROM processed_messages WHERE consumer_group = ? AND event_id = ?")) {
			try {
				ps.setString(1, group);
				ps.setObject(2, event);
				try (ResultSet rs = ps.executeQuery()) {
					assertThat(rs.next()).as("ledger_api_owner: committed dedup row").isTrue();
					assertThat(rs.getObject(1, OffsetDateTime.class))
						.as("ledger_api_owner: processed_at is the inserting transaction's now()")
						.isEqualTo(insertedAt);
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

	@Test
	void concurrentDuplicateDoesNothingOnceFirstCommits() throws Exception {
		assertThat(concurrentDuplicate(true)).as("settlement_worker: duplicate after the first commits").isZero();
	}

	@Test
	void concurrentDuplicateInsertsOnceFirstRollsBack() throws Exception {
		assertThat(concurrentDuplicate(false)).as("settlement_worker: duplicate after the first rolls back")
			.isEqualTo(1);
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(OwnerMutation.class)
	void ownerCannotModifyDedupRows(OwnerMutation mutation) throws SQLException {
		String group = newGroup();
		UUID event = UUID.randomUUID();
		try (Connection conn = connect(Role.WORKER)) {
			try {
				assertThat(update(conn, DEDUP, group, event)).isEqualTo(1);
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}

		SQLException e;
		try (Connection conn = connect(Role.OWNER)) {
			try {
				e = catchThrowableOfType(SQLException.class,
						() -> update(conn, mutation.sql, mutation.bindsRow ? new Object[] { group, event } : new Object[0]));
			}
			finally {
				conn.rollback();
			}
		}

		assertThat((Throwable) e).as("ledger_api_owner: %s must be rejected by the statement", mutation).isNotNull();
		assertThat(rootError(e, Role.OWNER, RESTRICT_VIOLATION).getMessage()).as("ledger_api_owner: server message")
			.isEqualTo("processed_messages is append-only");
		assertThat(rowCount(group, event)).as("ledger_api_owner: dedup row after %s", mutation).isEqualTo(1);
	}

	// A holds (group, event) uncommitted; B runs the dedup statement on another thread and must
	// block on A's backend. Then A commits or rolls back; returns B's row count. The final state
	// is exactly one committed row either way.
	private int concurrentDuplicate(boolean holderCommits) throws Exception {
		String group = newGroup();
		UUID event = UUID.randomUUID();
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try (Connection holder = connect(Role.WORKER); Connection waiter = connect(Role.WORKER)) {
			try {
				assertThat(update(holder, DEDUP, group, event)).as("settlement_worker: holder's first delivery")
					.isEqualTo(1);
				int holderPid = backendPid(holder);
				int waiterPid = backendPid(waiter);
				try (Statement st = waiter.createStatement()) {
					st.execute("SET LOCAL lock_timeout = '5s'");
				}

				Future<Integer> duplicate = executor.submit(() -> update(waiter, DEDUP, group, event));
				awaitBlocked(waiterPid, holderPid);
				if (holderCommits) {
					holder.commit();
				}
				else {
					holder.rollback();
				}
				int inserted = duplicate.get(BLOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
				waiter.commit();

				assertThat(rowCount(group, event)).as("committed dedup rows for the event").isEqualTo(1);
				return inserted;
			}
			finally {
				holder.rollback();
				waiter.rollback();
			}
		}
		finally {
			executor.shutdownNow();
		}
	}

	// Polls on its own worker connection, with a fresh snapshot each time, until the waiter's
	// backend is blocked by the holder's.
	private void awaitBlocked(int waiterPid, int holderPid) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BLOCK_TIMEOUT_MILLIS);
		try (Connection poller = connect(Role.WORKER);
				PreparedStatement ps = poller.prepareStatement("SELECT ? = ANY(pg_blocking_pids(?))")) {
			ps.setInt(1, holderPid);
			ps.setInt(2, waiterPid);
			while (System.nanoTime() < deadline) {
				try (ResultSet rs = ps.executeQuery()) {
					assertThat(rs.next()).isTrue();
					boolean blocked = rs.getBoolean(1);
					poller.rollback();
					if (blocked) {
						return;
					}
				}
				Thread.sleep(10);
			}
		}
		fail("waiter (pid " + waiterPid + ") never blocked on the holder (pid " + holderPid + ") within "
				+ BLOCK_TIMEOUT_MILLIS + " ms");
	}

	// Committed rows for (group, event), read by the owner on a fresh connection, independent
	// of the role under test.
	private long rowCount(String group, UUID event) throws SQLException {
		try (Connection conn = connect(Role.OWNER);
				PreparedStatement ps = conn.prepareStatement(
						"SELECT count(*) FROM processed_messages WHERE consumer_group = ? AND event_id = ?")) {
			try {
				ps.setString(1, group);
				ps.setObject(2, event);
				try (ResultSet rs = ps.executeQuery()) {
					assertThat(rs.next()).isTrue();
					return rs.getLong(1);
				}
			}
			finally {
				conn.rollback();
			}
		}
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

	private static int update(Connection conn, String sql, Object... params) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				ps.setObject(i + 1, params[i]);
			}
			return ps.executeUpdate();
		}
	}

	private static int backendPid(Connection conn) throws SQLException {
		try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT pg_backend_pid()")) {
			assertThat(rs.next()).isTrue();
			return rs.getInt(1);
		}
	}

	private static OffsetDateTime dbNow(Connection conn) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement("SELECT now()"); ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).isTrue();
			return rs.getObject(1, OffsetDateTime.class);
		}
	}

	private static String newGroup() {
		return "m3t2-" + UUID.randomUUID();
	}

	// Connection as the given role, autoCommit off; asserts the session really is that role.
	// The app role has no access to processed_messages at all (DbRolePrivilegesTest).
	private Connection connect(Role role) throws SQLException {
		Connection conn = switch (role) {
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
