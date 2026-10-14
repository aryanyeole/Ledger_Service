package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import javax.sql.DataSource;

// Direct database reads for API tests, each on a fresh app-role connection that asserts
// current_user (D-036). Read-only: every connection is rolled back on close.
final class LedgerTestSupport {

	static final String APP = "ledger_api_app";

	private LedgerTestSupport() {
	}

	/** An account's balance and SUM(entries.amount), read in one statement. */
	record Totals(BigDecimal balance, BigDecimal entrySum) {

		Totals minus(Totals before) {
			return new Totals(this.balance.subtract(before.balance), this.entrySum.subtract(before.entrySum));
		}

	}

	record KeyRow(byte[] requestHash, Integer statusCode, String responseBody) {
	}

	static Connection connectApp(DataSource dataSource) throws SQLException {
		Connection conn = dataSource.getConnection();
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

	static Totals totals(DataSource dataSource, long accountId) throws SQLException {
		try (Connection conn = connectApp(dataSource);
				PreparedStatement ps = conn.prepareStatement(
						"SELECT a.balance, (SELECT COALESCE(SUM(e.amount), 0) FROM entries e WHERE e.account_id = a.id) FROM accounts a WHERE a.id = ?")) {
			ps.setLong(1, accountId);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).as("[%s] account %d exists", APP, accountId).isTrue();
				return new Totals(rs.getBigDecimal(1), rs.getBigDecimal(2));
			}
		}
	}

	/** Distinct transactions with an entry on this account. */
	static long transactionsTouching(DataSource dataSource, long accountId) throws SQLException {
		return queryLong(dataSource, "SELECT count(DISTINCT transaction_id) FROM entries WHERE account_id = ?",
				accountId);
	}

	static Optional<KeyRow> keyRow(DataSource dataSource, String clientId, String key) throws SQLException {
		try (Connection conn = connectApp(dataSource);
				PreparedStatement ps = conn.prepareStatement(
						"SELECT request_hash, status_code, response_body::text FROM idempotency_keys WHERE client_id = ? AND key = ?")) {
			ps.setString(1, clientId);
			ps.setString(2, key);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return Optional.empty();
				}
				return Optional.of(new KeyRow(rs.getBytes(1), rs.getObject(2, Integer.class), rs.getString(3)));
			}
		}
	}

	static long keyRowCount(DataSource dataSource, String clientId) throws SQLException {
		try (Connection conn = connectApp(dataSource);
				PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM idempotency_keys WHERE client_id = ?")) {
			ps.setString(1, clientId);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getLong(1);
			}
		}
	}

	static long accountId(DataSource dataSource, String name) throws SQLException {
		try (Connection conn = connectApp(dataSource);
				PreparedStatement ps = conn.prepareStatement("SELECT id FROM accounts WHERE name = ?")) {
			ps.setString(1, name);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).as("[%s] account %s exists", APP, name).isTrue();
				return rs.getLong(1);
			}
		}
	}

	static long queryLong(DataSource dataSource, String sql, long param) throws SQLException {
		try (Connection conn = connectApp(dataSource); PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setLong(1, param);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getLong(1);
			}
		}
	}

}
