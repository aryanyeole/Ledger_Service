package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

// Guard: proof tests that write through the app DataSource exercise the runtime role,
// not a superuser that no privilege or trigger bypass check would ever stop.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class DbRoleBootstrapTest {

	@Autowired
	JdbcClient jdbc;

	record Session(String currentUser, boolean superuser) {
	}

	@Test
	void appDataSourceConnectsAsNonSuperuserAppRole() {
		Session session = jdbc
			.sql("SELECT current_user, rolsuper FROM pg_roles WHERE rolname = current_user")
			.query((rs, rowNum) -> new Session(rs.getString(1), rs.getBoolean(2)))
			.single();

		assertThat(session.currentUser()).isEqualTo("ledger_api_app");
		assertThat(session.superuser()).isFalse();
	}

}
