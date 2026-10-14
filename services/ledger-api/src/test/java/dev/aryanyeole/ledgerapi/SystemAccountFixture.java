package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

// Get-or-create for system:external, the source account of test deposits. From V4 it is
// seeded and a singleton (D-048), so this only reads it. Below V4 (failing-first runs at
// -Dspring.flyway.target=1..3, D-038) it does not exist yet and is inserted here, as
// ledger_api_app, so proof tests fail on their own assertions rather than in setup.
final class SystemAccountFixture {

	static final String EXTERNAL_NAME = "system:external";

	private static final String APP = "ledger_api_app";

	private SystemAccountFixture() {
	}

	static long external(DataSource dataSource) throws SQLException {
		try (Connection conn = dataSource.getConnection()) {
			try (PreparedStatement ps = conn.prepareStatement("SELECT current_user");
					ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				assertThat(rs.getString(1)).as("[%s] current_user", APP).isEqualTo(APP);
			}
			conn.setAutoCommit(false);
			try {
				try (PreparedStatement ps = conn
					.prepareStatement("SELECT id, type, allow_negative FROM accounts WHERE name = ?")) {
					ps.setString(1, EXTERNAL_NAME);
					try (ResultSet rs = ps.executeQuery()) {
						if (rs.next()) {
							assertThat(rs.getString(2)).as("[%s] %s type", APP, EXTERNAL_NAME).isEqualTo("EXTERNAL");
							assertThat(rs.getBoolean(3)).as("[%s] %s allow_negative", APP, EXTERNAL_NAME).isTrue();
							return rs.getLong(1);
						}
					}
				}
				try (PreparedStatement ps = conn.prepareStatement(
						"INSERT INTO accounts (name, type, allow_negative) VALUES (?, 'EXTERNAL', TRUE) RETURNING id")) {
					ps.setString(1, EXTERNAL_NAME);
					try (ResultSet rs = ps.executeQuery()) {
						assertThat(rs.next()).isTrue();
						long id = rs.getLong(1);
						conn.commit();
						return id;
					}
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

}
