package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.postgresql.PostgreSQLContainer;

// Proof (D-077): an outbox row is marked published exactly once, published_at comes from the
// database clock at the moment of marking (clock_timestamp(), not the transaction's now()),
// and nothing else in the row ever changes. Violations fail the statement with
// check_violation (23514) naming outbox_publish_once; the owner, which has every privilege,
// is stopped by the same trigger. The worker can insert events but never read them.
// No outbox row is ever committed: the relay (T4) would publish leftovers. Every case runs in
// a transaction that is rolled back, and each test checks the row count is unchanged.
// A rejected statement aborts its transaction, so each one runs after a savepoint and the row
// is read back after rolling back to it. Every connection asserts current_user (D-036).
// Failing first (D-038): -Dspring.flyway.target=12.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class OutboxPublishOnceTest {

	private static final String CHECK_VIOLATION = "23514";

	private static final String UNIQUE_VIOLATION = "23505";

	private static final String INSUFFICIENT_PRIVILEGE = "42501";

	private static final String PUBLISH_ONCE = "outbox_publish_once";

	private static final OffsetDateTime Y2K = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

	enum Role {

		APP("ledger_api_app"), WORKER("settlement_worker"), OWNER("ledger_api_owner");

		final String dbRole;

		Role(String dbRole) {
			this.dbRole = dbRole;
		}

	}

	record Row(long id, UUID eventId, String topic, String messageKey, String eventType, String payload,
			String headers, OffsetDateTime createdAt, OffsetDateTime publishedAt) {
	}

	// The second UPDATE an app relay must never make.
	enum SecondUpdate {

		PUBLISH_AGAIN(true, "published_at = now()"),
		UNPUBLISH(true, "published_at = NULL"),
		NULL_TO_NULL(false, "published_at = NULL");

		final boolean published;

		final String assignment;

		SecondUpdate(boolean published, String assignment) {
			this.published = published;
			this.assignment = assignment;
		}

	}

	// Combined with a valid NULL -> set, so only the column check can reject it. GENERATED
	// ALWAYS id can only be updated to DEFAULT, which assigns a new value.
	enum ColumnChange {

		ID("id = DEFAULT"),
		EVENT_ID("event_id = gen_random_uuid()"),
		TOPIC("topic = topic || '-x'"),
		MESSAGE_KEY("message_key = message_key || '-x'"),
		EVENT_TYPE("event_type = 'payment.settled'"),
		PAYLOAD("payload = payload || '{\"x\": 1}'"),
		HEADERS("headers = headers || '{\"x\": 1}'"),
		CREATED_AT("created_at = created_at - interval '1 day'");

		final String assignment;

		ColumnChange(String assignment) {
			this.assignment = assignment;
		}

	}

	// Each violates exactly one constraint, so the one named is the one that fires.
	enum InvalidInsert {

		UNKNOWN_EVENT_TYPE("ledger.payments.v1", "k", "payment.refunded", "{}", "{}", "outbox_event_type_known"),
		PAYLOAD_ARRAY("ledger.payments.v1", "k", "payment.authorized", "[]", "{}", "outbox_payload_object"),
		HEADERS_STRING("ledger.payments.v1", "k", "payment.authorized", "{}", "\"x\"", "outbox_headers_object"),
		EMPTY_TOPIC("", "k", "payment.authorized", "{}", "{}", "outbox_topic_nonempty"),
		EMPTY_MESSAGE_KEY("ledger.payments.v1", "", "payment.authorized", "{}", "{}", "outbox_message_key_nonempty");

		final String topic;

		final String messageKey;

		final String eventType;

		final String payload;

		final String headers;

		final String constraint;

		InvalidInsert(String topic, String messageKey, String eventType, String payload, String headers,
				String constraint) {
			this.topic = topic;
			this.messageKey = messageKey;
			this.eventType = eventType;
			this.payload = payload;
			this.headers = headers;
			this.constraint = constraint;
		}

	}

	@Autowired
	DataSource dataSource;

	@Autowired
	PostgreSQLContainer postgres;

	private long outboxRowsBefore;

	@BeforeEach
	void countOutboxRows() throws SQLException {
		this.outboxRowsBefore = outboxCount();
	}

	@AfterEach
	void nothingWasCommitted() throws SQLException {
		assertThat(outboxCount()).as("outbox rows after the test (nothing may be committed)")
			.isEqualTo(this.outboxRowsBefore);
	}

	@Test
	void insertTakesEventIdHeadersAndCreatedAtFromDefaults() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				OffsetDateTime insertedAt = dbNow(conn);
				long id = insertEvent(conn);
				Row row = read(conn, id);
				assertThat(row.eventId().version()).as("ledger_api_app: event_id from gen_random_uuid() is a v4 UUID")
					.isEqualTo(4);
				assertThat(row.headers()).as("ledger_api_app: headers default").isEqualTo("{}");
				assertThat(row.publishedAt()).as("ledger_api_app: published_at of a new row").isNull();
				assertThat(row.createdAt()).as("ledger_api_app: created_at is the inserting transaction's now()")
					.isEqualTo(insertedAt);
			}
			finally {
				conn.rollback();
			}
		}
	}

	@Test
	void workerInsertsEventButCannotReadIt() throws SQLException {
		try (Connection conn = connect(Role.WORKER)) {
			try {
				try (PreparedStatement ps = conn.prepareStatement(
						"INSERT INTO outbox (topic, message_key, event_type, payload) VALUES (?, ?, ?, ?::jsonb)")) {
					ps.setString(1, "ledger.payments.v1");
					ps.setString(2, UUID.randomUUID().toString());
					ps.setString(3, "payment.settled");
					ps.setString(4, "{\"payment_id\": \"p\"}");
					assertThat(ps.executeUpdate()).as("settlement_worker: rows inserted").isEqualTo(1);
				}
				SQLException e = catchThrowableOfType(SQLException.class, () -> {
					try (PreparedStatement ps = conn.prepareStatement("SELECT event_id FROM outbox");
							ResultSet rs = ps.executeQuery()) {
						rs.next();
					}
				});
				assertThat((Throwable) e).as("settlement_worker: reading the outbox must be rejected").isNotNull();
				assertThat(rootError(e, Role.WORKER, INSUFFICIENT_PRIVILEGE).getMessage())
					.as("settlement_worker: server message")
					.isEqualTo("permission denied for table outbox");
			}
			finally {
				conn.rollback();
			}
		}
	}

	// now() is the transaction's start; the mark happens 50 ms later and must say so.
	@Test
	void publishedAtIsClockTimeOfMarkingNotSuppliedValue() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				long id = insertEvent(conn);
				OffsetDateTime transactionStart = dbNow(conn);
				try (PreparedStatement ps = conn.prepareStatement("SELECT pg_sleep(0.05)")) {
					ps.execute();
				}
				assertThat(update(conn, "UPDATE outbox SET published_at = '2000-01-01T00:00:00Z' WHERE id = ?", id))
					.as("ledger_api_app: rows marked published")
					.isEqualTo(1);
				OffsetDateTime publishedAt = read(conn, id).publishedAt();
				assertThat(publishedAt).as("ledger_api_app: published_at is not the supplied value").isNotEqualTo(Y2K);
				assertThat(Duration.between(transactionStart, publishedAt))
					.as("ledger_api_app: published_at after the transaction's now() (%s -> %s)", transactionStart,
							publishedAt)
					.isGreaterThanOrEqualTo(Duration.ofMillis(40));
			}
			finally {
				conn.rollback();
			}
		}
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(SecondUpdate.class)
	void appCannotMarkTwiceOrUnmark(SecondUpdate second) throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				long id = insertEvent(conn);
				if (second.published) {
					assertThat(update(conn, "UPDATE outbox SET published_at = now() WHERE id = ?", id)).isEqualTo(1);
				}
				Row before = read(conn, id);

				SQLException e = attempt(conn, "UPDATE outbox SET " + second.assignment + " WHERE id = ?", id);

				assertThat((Throwable) e).as("ledger_api_app: %s must be rejected by the statement", second).isNotNull();
				assertPublishOnceRejected(e, Role.APP, "outbox row " + id + ": published_at may only be set once");
				assertThat(read(conn, id)).as("ledger_api_app: row after rejected %s", second).isEqualTo(before);
			}
			finally {
				conn.rollback();
			}
		}
	}

	// The owner inserts its own row: an uncommitted app row is invisible to its connection.
	@ParameterizedTest(name = "{0}")
	@EnumSource(ColumnChange.class)
	void ownerCannotChangeAnotherColumnWhenMarking(ColumnChange change) throws SQLException {
		try (Connection conn = connect(Role.OWNER)) {
			try {
				long id = insertEvent(conn);
				Row before = read(conn, id);

				SQLException e = attempt(conn,
						"UPDATE outbox SET published_at = now(), " + change.assignment + " WHERE id = ?", id);

				assertThat((Throwable) e).as("ledger_api_owner: %s while marking must be rejected", change).isNotNull();
				assertPublishOnceRejected(e, Role.OWNER, "outbox row " + id + ": only published_at may change");
				assertThat(read(conn, id)).as("ledger_api_owner: row after rejected %s", change).isEqualTo(before);
			}
			finally {
				conn.rollback();
			}
		}
	}

	// published_at stays NULL, so the set-once rule rejects it before the column check.
	@Test
	void ownerCannotChangePayloadOfUnpublishedRow() throws SQLException {
		try (Connection conn = connect(Role.OWNER)) {
			try {
				long id = insertEvent(conn);
				Row before = read(conn, id);

				SQLException e = attempt(conn, "UPDATE outbox SET payload = payload || '{\"x\": 1}' WHERE id = ?", id);

				assertThat((Throwable) e).as("ledger_api_owner: payload change must be rejected").isNotNull();
				assertPublishOnceRejected(e, Role.OWNER, "outbox row " + id + ": published_at may only be set once");
				assertThat(read(conn, id)).as("ledger_api_owner: row after rejected payload change").isEqualTo(before);
			}
			finally {
				conn.rollback();
			}
		}
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(InvalidInsert.class)
	void appInsertViolatingCheckIsRejected(InvalidInsert insert) throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				SQLException e = attempt(conn,
						"INSERT INTO outbox (topic, message_key, event_type, payload, headers) VALUES (?, ?, ?, ?::jsonb, ?::jsonb)",
						insert.topic, insert.messageKey, insert.eventType, insert.payload, insert.headers);

				assertThat((Throwable) e).as("ledger_api_app: %s must be rejected", insert).isNotNull();
				ServerErrorMessage server = rootError(e, Role.APP, CHECK_VIOLATION);
				assertThat(server.getConstraint()).as("ledger_api_app: constraint").isEqualTo(insert.constraint);
				assertThat(server.getTable()).as("ledger_api_app: table").isEqualTo("outbox");
			}
			finally {
				conn.rollback();
			}
		}
	}

	@Test
	void duplicateEventIdIsRejected() throws SQLException {
		UUID eventId = UUID.randomUUID();
		String sql = "INSERT INTO outbox (event_id, topic, message_key, event_type, payload) VALUES (?, 'ledger.payments.v1', 'k', 'payment.authorized', '{}')";
		try (Connection conn = connect(Role.OWNER)) {
			try {
				assertThat(update(conn, sql, eventId)).as("ledger_api_owner: first insert").isEqualTo(1);

				SQLException e = attempt(conn, sql, eventId);

				assertThat((Throwable) e).as("ledger_api_owner: duplicate event_id must be rejected").isNotNull();
				ServerErrorMessage server = rootError(e, Role.OWNER, UNIQUE_VIOLATION);
				assertThat(server.getConstraint()).as("ledger_api_owner: constraint").isEqualTo("outbox_event_id_key");
			}
			finally {
				conn.rollback();
			}
		}
	}

	private static void assertPublishOnceRejected(SQLException thrown, Role role, String message) {
		ServerErrorMessage server = rootError(thrown, role, CHECK_VIOLATION);
		assertThat(server.getConstraint()).as("%s: constraint", role.dbRole).isEqualTo(PUBLISH_ONCE);
		assertThat(server.getSchema()).as("%s: schema", role.dbRole).isEqualTo("public");
		assertThat(server.getTable()).as("%s: table", role.dbRole).isEqualTo("outbox");
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

	// Runs one statement after a savepoint and returns what it threw (or null), rolling back to
	// the savepoint so the transaction stays usable.
	private static SQLException attempt(Connection conn, String sql, Object... params) throws SQLException {
		Savepoint savepoint = conn.setSavepoint();
		try {
			return catchThrowableOfType(SQLException.class, () -> update(conn, sql, params));
		}
		finally {
			conn.rollback(savepoint);
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

	// An unpublished payment.authorized row, headers omitted; uncommitted, on this connection.
	private static long insertEvent(Connection conn) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO outbox (topic, message_key, event_type, payload) VALUES (?, ?, ?, ?::jsonb)",
				new String[] { "id" })) {
			String paymentId = UUID.randomUUID().toString();
			ps.setString(1, "ledger.payments.v1");
			ps.setString(2, paymentId);
			ps.setString(3, "payment.authorized");
			ps.setString(4, "{\"payment_id\": \"" + paymentId + "\", \"amount\": \"100.00\"}");
			assertThat(ps.executeUpdate()).isEqualTo(1);
			try (ResultSet keys = ps.getGeneratedKeys()) {
				assertThat(keys.next()).isTrue();
				return keys.getLong(1);
			}
		}
	}

	private static Row read(Connection conn, long id) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement("""
				SELECT id, event_id, topic, message_key, event_type, payload::text, headers::text, created_at, published_at
				FROM outbox WHERE id = ?""")) {
			ps.setLong(1, id);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).as("outbox row %d exists", id).isTrue();
				return new Row(rs.getLong(1), rs.getObject(2, UUID.class), rs.getString(3), rs.getString(4),
						rs.getString(5), rs.getString(6), rs.getString(7), rs.getObject(8, OffsetDateTime.class),
						rs.getObject(9, OffsetDateTime.class));
			}
		}
	}

	private long outboxCount() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM outbox");
					ResultSet rs = ps.executeQuery()) {
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
