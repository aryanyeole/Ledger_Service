package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.util.PSQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

// V5's own guards on idempotency_keys (D-043): request_hash is exactly 32 bytes, status_code
// is 100-599, and (client_id, key) is unique. Each violation is checked by SQLState and by
// the constraint name in the server error (D-058). Runs as ledger_api_app (D-036).
// No failing-first run: below V5 the table does not exist, so every case would fail with
// 42P01, which says nothing about these constraints.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class IdempotencyKeysSchemaTest {

	private static final String CHECK_VIOLATION = "23514";

	private static final String UNIQUE_VIOLATION = "23505";

	private static final String APP = "ledger_api_app";

	private static final String CLIENT = "t3-schema";

	@Autowired
	DataSource dataSource;

	@ParameterizedTest(name = "{0} bytes")
	@ValueSource(ints = { 0, 31, 33 })
	void requestHashMustBe32Bytes(int length) throws SQLException {
		try (Connection conn = connectApp()) {
			try {
				SQLException e = catchThrowableOfType(SQLException.class,
						() -> insertKey(conn, newKey(), new byte[length]));
				assertThat((Throwable) e).as("[%s] %d-byte request_hash must be rejected", APP, length).isNotNull();
				assertViolation(e, CHECK_VIOLATION, "idempotency_keys_request_hash_sha256");
			}
			finally {
				conn.rollback();
			}
		}
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(ints = { 99, 600, -1 })
	void statusCodeOutOfRangeIsRejected(int statusCode) throws SQLException {
		try (Connection conn = connectApp()) {
			try {
				String key = newKey();
				insertKey(conn, key, new byte[32]);
				SQLException e = catchThrowableOfType(SQLException.class, () -> storeResponse(conn, key, statusCode));
				assertThat((Throwable) e).as("[%s] status_code %d must be rejected", APP, statusCode).isNotNull();
				assertViolation(e, CHECK_VIOLATION, "idempotency_keys_status_code_range");
			}
			finally {
				conn.rollback();
			}
		}
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(ints = { 100, 599 })
	void statusCodeBoundsAreAccepted(int statusCode) throws SQLException {
		try (Connection conn = connectApp()) {
			try {
				String key = newKey();
				insertKey(conn, key, new byte[32]);
				storeResponse(conn, key, statusCode);
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}
	}

	@Test
	void duplicateKeyIsRejected() throws SQLException {
		String key = newKey();
		try (Connection conn = connectApp()) {
			try {
				insertKey(conn, key, new byte[32]);
				storeResponse(conn, key, 201);
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}
		try (Connection conn = connectApp()) {
			try {
				SQLException e = catchThrowableOfType(SQLException.class, () -> insertKey(conn, key, new byte[32]));
				assertThat((Throwable) e).as("[%s] second insert of %s/%s must be rejected", APP, CLIENT, key)
					.isNotNull();
				assertViolation(e, UNIQUE_VIOLATION, "idempotency_keys_pkey");
			}
			finally {
				conn.rollback();
			}
		}
	}

	private static void assertViolation(SQLException thrown, String sqlState, String constraint) {
		Throwable root = thrown;
		while (root.getCause() != null) {
			root = root.getCause();
		}
		assertThat(root).as("[%s] root cause", APP).isInstanceOf(PSQLException.class);
		PSQLException psql = (PSQLException) root;
		assertThat(psql.getSQLState()).as("[%s] SQLState of %s", APP, psql).isEqualTo(sqlState);
		assertThat(psql.getServerErrorMessage().getConstraint()).as("[%s] violated constraint", APP)
			.isEqualTo(constraint);
	}

	private static void insertKey(Connection conn, String key, byte[] requestHash) throws SQLException {
		try (PreparedStatement ps = conn
			.prepareStatement("INSERT INTO idempotency_keys (client_id, key, request_hash) VALUES (?, ?, ?)")) {
			ps.setString(1, CLIENT);
			ps.setString(2, key);
			ps.setBytes(3, requestHash);
			assertThat(ps.executeUpdate()).isEqualTo(1);
		}
	}

	private static void storeResponse(Connection conn, String key, int statusCode) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(
				"UPDATE idempotency_keys SET status_code = ?, response_body = '{}'::jsonb WHERE client_id = ? AND key = ?")) {
			ps.setInt(1, statusCode);
			ps.setString(2, CLIENT);
			ps.setString(3, key);
			assertThat(ps.executeUpdate()).isEqualTo(1);
		}
	}

	private static String newKey() {
		return "t3-" + UUID.randomUUID();
	}

	// App-role connection, autoCommit off; asserts the session really is ledger_api_app (D-036).
	private Connection connectApp() throws SQLException {
		Connection conn = this.dataSource.getConnection();
		try (PreparedStatement ps = conn.prepareStatement("SELECT current_user");
				ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).isTrue();
			assertThat(rs.getString(1)).as("[%s] current_user", APP).isEqualTo(APP);
			conn.setAutoCommit(false);
			return conn;
		}
		catch (SQLException | RuntimeException | Error e) {
			conn.close();
			throw e;
		}
	}

}
