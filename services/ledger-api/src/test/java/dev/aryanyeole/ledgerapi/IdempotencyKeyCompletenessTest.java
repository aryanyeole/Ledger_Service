package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.security.SecureRandom;
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
import org.postgresql.util.ServerErrorMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

// Proof (D-059): an idempotency key commits only with its response. The deferred trigger
// idempotency_keys_complete re-reads the row at COMMIT; a null status_code or response_body
// fails the commit with not_null_violation (23502) naming the constraint, and the
// transaction leaves nothing behind. Runs as ledger_api_app (D-036) with explicit commits,
// asserting the root SQLException. Failing first (D-038): -Dspring.flyway.target=5.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class IdempotencyKeyCompletenessTest {

	private static final String NOT_NULL_VIOLATION = "23502";

	private static final String CONSTRAINT = "idempotency_keys_complete";

	private static final String APP = "ledger_api_app";

	private static final String CLIENT = "t3-client";

	private static final SecureRandom RANDOM = new SecureRandom();

	@Autowired
	DataSource dataSource;

	record StoredKey(Integer statusCode, String responseBody, boolean expiresIn24Hours) {
	}

	@Test
	void keyWithoutResponseIsRejectedAtCommit() throws SQLException {
		String key = newKey();
		try (Connection conn = connectApp()) {
			try {
				assertThat(insertKey(conn, key)).as("[%s] insert is not checked until COMMIT", APP).isEqualTo(1);
				SQLException e = catchThrowableOfType(SQLException.class, conn::commit);
				assertThat((Throwable) e).as("[%s] deferred trigger must reject at commit()", APP).isNotNull();
				assertIncomplete(e, key);
			}
			finally {
				conn.rollback();
			}
		}
		assertThat(stored(key)).as("[%s] key %s after the rejected commit", APP, key).isNull();
	}

	@Test
	void keyWithResponseInSameTransactionCommits() throws SQLException {
		String key = newKey();
		try (Connection conn = connectApp()) {
			try {
				insertKey(conn, key);
				storeResponse(conn, key, 201, "{\"transaction_id\": 7}");
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}
		assertThat(stored(key)).as("[%s] key %s", APP, key)
			.isEqualTo(new StoredKey(201, "{\"transaction_id\": 7}", true));
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = { "response_body", "status_code" })
	void clearingTheResponseIsRejectedAtCommit(String column) throws SQLException {
		String key = newKey();
		try (Connection conn = connectApp()) {
			try {
				insertKey(conn, key);
				storeResponse(conn, key, 201, "{\"transaction_id\": 8}");
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}

		try (Connection conn = connectApp()) {
			try {
				try (PreparedStatement ps = conn.prepareStatement(
						"UPDATE idempotency_keys SET " + column + " = NULL WHERE client_id = ? AND key = ?")) {
					ps.setString(1, CLIENT);
					ps.setString(2, key);
					assertThat(ps.executeUpdate()).as("[%s] update is not checked until COMMIT", APP).isEqualTo(1);
				}
				SQLException e = catchThrowableOfType(SQLException.class, conn::commit);
				assertThat((Throwable) e).as("[%s] deferred trigger must reject at commit()", APP).isNotNull();
				assertIncomplete(e, key);
			}
			finally {
				conn.rollback();
			}
		}
		assertThat(stored(key)).as("[%s] key %s after the rejected commit", APP, key)
			.isEqualTo(new StoredKey(201, "{\"transaction_id\": 8}", true));
	}

	private static void assertIncomplete(SQLException thrown, String key) {
		Throwable root = thrown;
		while (root.getCause() != null) {
			root = root.getCause();
		}
		assertThat(root).as("[%s] root cause", APP).isInstanceOf(PSQLException.class);
		PSQLException psql = (PSQLException) root;
		assertThat(psql.getSQLState()).as("[%s] SQLState of %s", APP, psql).isEqualTo(NOT_NULL_VIOLATION);
		ServerErrorMessage server = psql.getServerErrorMessage();
		assertThat(server.getConstraint()).as("[%s] constraint", APP).isEqualTo(CONSTRAINT);
		assertThat(server.getTable()).as("[%s] table", APP).isEqualTo("idempotency_keys");
		assertThat(server.getMessage()).as("[%s] server message", APP)
			.isEqualTo("idempotency key " + CLIENT + "/" + key + " has no stored response");
	}

	private static int insertKey(Connection conn, String key) throws SQLException {
		try (PreparedStatement ps = conn
			.prepareStatement("INSERT INTO idempotency_keys (client_id, key, request_hash) VALUES (?, ?, ?)")) {
			byte[] hash = new byte[32];
			RANDOM.nextBytes(hash);
			ps.setString(1, CLIENT);
			ps.setString(2, key);
			ps.setBytes(3, hash);
			return ps.executeUpdate();
		}
	}

	private static void storeResponse(Connection conn, String key, int statusCode, String body) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(
				"UPDATE idempotency_keys SET status_code = ?, response_body = ?::jsonb WHERE client_id = ? AND key = ?")) {
			ps.setInt(1, statusCode);
			ps.setString(2, body);
			ps.setString(3, CLIENT);
			ps.setString(4, key);
			assertThat(ps.executeUpdate()).isEqualTo(1);
		}
	}

	// Fresh connection; null if the key does not exist.
	private StoredKey stored(String key) throws SQLException {
		try (Connection conn = connectApp();
				PreparedStatement ps = conn.prepareStatement("""
						SELECT status_code, response_body::text, expires_at - created_at = interval '24 hours'
						FROM idempotency_keys WHERE client_id = ? AND key = ?""")) {
			ps.setString(1, CLIENT);
			ps.setString(2, key);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return null;
				}
				return new StoredKey(rs.getObject(1, Integer.class), rs.getString(2), rs.getBoolean(3));
			}
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
