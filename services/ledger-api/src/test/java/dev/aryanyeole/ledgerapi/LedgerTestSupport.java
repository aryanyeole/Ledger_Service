package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

// Direct database reads for API tests, each on a fresh app-role connection that asserts
// current_user (D-036). Read-only: every connection is rolled back on close.
public final class LedgerTestSupport {

	public static final String APP = "ledger_api_app";

	private LedgerTestSupport() {
	}

	/** An account's balance and SUM(entries.amount), read in one statement. */
	public record Totals(BigDecimal balance, BigDecimal entrySum) {

		public Totals minus(Totals before) {
			return new Totals(this.balance.subtract(before.balance), this.entrySum.subtract(before.entrySum));
		}

	}

	public record KeyRow(byte[] requestHash, Integer statusCode, String responseBody) {
	}

	public static Connection connectApp(DataSource dataSource) throws SQLException {
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

	public static Totals totals(DataSource dataSource, long accountId) throws SQLException {
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
	public static long transactionsTouching(DataSource dataSource, long accountId) throws SQLException {
		return queryLong(dataSource, "SELECT count(DISTINCT transaction_id) FROM entries WHERE account_id = ?",
				accountId);
	}

	public static Optional<KeyRow> keyRow(DataSource dataSource, String clientId, String key) throws SQLException {
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

	public static long keyRowCount(DataSource dataSource, String clientId) throws SQLException {
		try (Connection conn = connectApp(dataSource);
				PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM idempotency_keys WHERE client_id = ?")) {
			ps.setString(1, clientId);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getLong(1);
			}
		}
	}

	public static long keyRowCount(DataSource dataSource, String clientId, String key) throws SQLException {
		try (Connection conn = connectApp(dataSource);
				PreparedStatement ps = conn
					.prepareStatement("SELECT count(*) FROM idempotency_keys WHERE client_id = ? AND key = ?")) {
			ps.setString(1, clientId);
			ps.setString(2, key);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getLong(1);
			}
		}
	}

	public static long accountId(DataSource dataSource, String name) throws SQLException {
		try (Connection conn = connectApp(dataSource);
				PreparedStatement ps = conn.prepareStatement("SELECT id FROM accounts WHERE name = ?")) {
			ps.setString(1, name);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).as("[%s] account %s exists", APP, name).isTrue();
				return rs.getLong(1);
			}
		}
	}

	/** Asserts the account's balance and entry sum both moved by exactly {@code amount}. */
	public static void assertMoved(DataSource dataSource, long accountId, Totals before, String amount) throws SQLException {
		Totals delta = totals(dataSource, accountId).minus(before);
		assertThat(delta.balance()).as("account %d balance moved", accountId).isEqualByComparingTo(amount);
		assertThat(delta.entrySum()).as("account %d entry sum moved", accountId).isEqualByComparingTo(amount);
	}

	/** Entries of one transaction, account id -> amount, in insert (entry id) order. */
	public static Map<Long, BigDecimal> entries(DataSource dataSource, long transactionId) throws SQLException {
		Map<Long, BigDecimal> entries = new LinkedHashMap<>();
		try (Connection conn = connectApp(dataSource);
				PreparedStatement ps = conn
					.prepareStatement("SELECT account_id, amount FROM entries WHERE transaction_id = ? ORDER BY id")) {
			ps.setLong(1, transactionId);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					entries.put(rs.getLong(1), rs.getBigDecimal(2));
				}
			}
		}
		return entries;
	}

	/** Distinct transactions of this kind with an entry on this account. */
	public static long transactionsTouching(DataSource dataSource, long accountId, String kind) throws SQLException {
		try (Connection conn = connectApp(dataSource);
				PreparedStatement ps = conn.prepareStatement("""
						SELECT count(DISTINCT e.transaction_id) FROM entries e
						JOIN transactions t ON t.id = e.transaction_id
						WHERE e.account_id = ? AND t.kind = ?""")) {
			ps.setLong(1, accountId);
			ps.setString(2, kind);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getLong(1);
			}
		}
	}

	/** Ids of transactions of this kind with an entry on this account, ascending. */
	public static List<Long> transactionIds(DataSource dataSource, long accountId, String kind) throws SQLException {
		List<Long> ids = new ArrayList<>();
		try (Connection conn = connectApp(dataSource);
				PreparedStatement ps = conn.prepareStatement("""
						SELECT DISTINCT e.transaction_id FROM entries e
						JOIN transactions t ON t.id = e.transaction_id
						WHERE e.account_id = ? AND t.kind = ? ORDER BY e.transaction_id""")) {
			ps.setLong(1, accountId);
			ps.setString(2, kind);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					ids.add(rs.getLong(1));
				}
			}
		}
		return ids;
	}

	public static long queryLong(DataSource dataSource, String sql, long param) throws SQLException {
		try (Connection conn = connectApp(dataSource); PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setLong(1, param);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getLong(1);
			}
		}
	}

}
