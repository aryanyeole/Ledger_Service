package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.util.PSQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

// V4 system accounts (D-048): exactly one EXTERNAL, ESCROW and PLATFORM_FEES account, held
// by the database. Runs as ledger_api_app (D-036), which has INSERT on accounts, so the
// singleton index is what stops a second system account, not a missing grant.
// Failing first (D-038): -Dspring.flyway.target=3.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class SystemAccountsSeedTest {

	private static final String UNIQUE_VIOLATION = "23505";

	private static final String SINGLETON_INDEX = "accounts_one_per_system_type";

	private static final String APP = "ledger_api_app";

	@Autowired
	DataSource dataSource;

	record SystemAccount(String type, String name, boolean allowNegative) {
	}

	@Test
	void eachSystemTypeIsSeededExactlyOnce() throws SQLException {
		List<SystemAccount> seeded = new ArrayList<>();
		try (Connection conn = connectApp();
				PreparedStatement ps = conn.prepareStatement("""
						SELECT type, name, allow_negative FROM accounts
						WHERE type IN ('EXTERNAL', 'ESCROW', 'PLATFORM_FEES') ORDER BY type""");
				ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				seeded.add(new SystemAccount(rs.getString(1), rs.getString(2), rs.getBoolean(3)));
			}
		}
		assertThat(seeded).as("[%s] system accounts", APP)
			.containsExactly(new SystemAccount("ESCROW", "system:escrow", false),
					new SystemAccount("EXTERNAL", "system:external", true),
					new SystemAccount("PLATFORM_FEES", "system:platform_fees", false));
	}

	@Test
	void singletonIndexIsUniqueAndPartial() throws SQLException {
		try (Connection conn = connectApp();
				PreparedStatement ps = conn.prepareStatement("""
						SELECT i.indisunique, pg_get_expr(i.indpred, i.indrelid), pg_get_indexdef(i.indexrelid)
						FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
						WHERE c.relname = ? AND c.relnamespace = 'public'::regnamespace
						  AND i.indrelid = 'public.accounts'::regclass""")) {
			ps.setString(1, SINGLETON_INDEX);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).as("[%s] index %s on accounts", APP, SINGLETON_INDEX).isTrue();
				assertThat(rs.getBoolean(1)).as("[%s] %s is unique", APP, SINGLETON_INDEX).isTrue();
				assertThat(rs.getString(2)).as("[%s] %s predicate", APP, SINGLETON_INDEX)
					.contains("'EXTERNAL'", "'ESCROW'", "'PLATFORM_FEES'")
					.doesNotContain("'CUSTOMER'", "'MERCHANT'");
				assertThat(rs.getString(3)).as("[%s] %s definition", APP, SINGLETON_INDEX).contains("(type)");
			}
		}
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = { "EXTERNAL", "ESCROW", "PLATFORM_FEES" })
	void secondSystemAccountIsRejected(String type) throws SQLException {
		String name = "second-" + type + "-" + UUID.randomUUID();
		try (Connection conn = connectApp()) {
			try (PreparedStatement ps = conn
				.prepareStatement("INSERT INTO accounts (name, type, allow_negative) VALUES (?, ?, ?)")) {
				ps.setString(1, name);
				ps.setString(2, type);
				ps.setBoolean(3, type.equals("EXTERNAL")); // D-039, so only the index can object
				SQLException thrown = catchThrowableOfType(SQLException.class, ps::executeUpdate);
				assertThat((Throwable) thrown).as("[%s] second %s insert must fail", APP, type).isNotNull();
				assertUniqueViolationOn(thrown, type);
			}
			finally {
				conn.rollback();
			}
		}
		try (Connection conn = connectApp();
				PreparedStatement ps = conn.prepareStatement(
						"SELECT count(*) FILTER (WHERE type = ?), count(*) FILTER (WHERE name = ?) FROM accounts")) {
			ps.setString(1, type);
			ps.setString(2, name);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				assertThat(rs.getLong(1)).as("[%s] %s accounts after the rejected insert", APP, type).isEqualTo(1);
				assertThat(rs.getLong(2)).as("[%s] rows named %s", APP, name).isZero();
			}
		}
	}

	// The predicate covers only the system types: user account types stay unrestricted.
	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = { "CUSTOMER", "MERCHANT" })
	void userAccountTypesAreNotSingletons(String type) throws SQLException {
		try (Connection conn = connectApp()) {
			try (PreparedStatement ps = conn
				.prepareStatement("INSERT INTO accounts (name, type) VALUES (?, ?), (?, ?)")) {
				ps.setString(1, type + "-a-" + UUID.randomUUID());
				ps.setString(2, type);
				ps.setString(3, type + "-b-" + UUID.randomUUID());
				ps.setString(4, type);
				assertThat(ps.executeUpdate()).as("[%s] two %s accounts in one commit", APP, type).isEqualTo(2);
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}
	}

	private static void assertUniqueViolationOn(SQLException thrown, String type) {
		Throwable root = thrown;
		while (root.getCause() != null) {
			root = root.getCause();
		}
		assertThat(root).as("[%s] root cause for second %s", APP, type).isInstanceOf(PSQLException.class);
		PSQLException psql = (PSQLException) root;
		assertThat(psql.getSQLState()).as("[%s] SQLState of %s", APP, psql).isEqualTo(UNIQUE_VIOLATION);
		assertThat(psql.getMessage()).as("[%s] message", APP)
			.startsWith("ERROR: duplicate key value violates unique constraint \"" + SINGLETON_INDEX + "\"");
		assertThat(psql.getServerErrorMessage().getConstraint()).as("[%s] violated constraint", APP)
			.isEqualTo(SINGLETON_INDEX);
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
