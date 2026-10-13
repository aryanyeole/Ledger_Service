package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

// Smoke test for V1 and its grants, not a proof test (those are T5-T7). Runs through the
// app DataSource as ledger_api_app. Every write commits explicitly via TransactionTemplate;
// rows cannot be deleted, so each test uses its own uniquely named account and leaves
// balances at 0.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class LedgerCoreSchemaSmokeTest {

	private static final String INSUFFICIENT_PRIVILEGE = "42501";

	private static final String CHECK_VIOLATION = "23514";

	@Autowired
	JdbcClient jdbc;

	@Autowired
	TransactionTemplate tx;

	record Account(String name, String type, String currency, BigDecimal balance, boolean allowNegative) {
	}

	@Test
	void appRoleCreatesAndReadsAnAccount() {
		String name = uniqueName();
		long id = createCustomer(name);

		Account account = find(id);
		assertThat(account.name()).isEqualTo(name);
		assertThat(account.type()).isEqualTo("CUSTOMER");
		assertThat(account.currency()).isEqualTo("USD");
		assertThat(account.balance()).isEqualByComparingTo("0");
		assertThat(account.allowNegative()).isFalse();
	}

	@Test
	void appRoleUpdatesBalance() {
		long id = createCustomer(uniqueName());

		Integer updated = tx.execute(status -> jdbc.sql("UPDATE accounts SET balance = 0 WHERE id = ?")
			.param(id)
			.update());

		assertThat(updated).isEqualTo(1);
		assertThat(find(id).balance()).isEqualByComparingTo("0");
	}

	@Test
	void appRoleLocksAccountRowForUpdate() {
		long id = createCustomer(uniqueName());

		Long locked = tx.execute(status -> jdbc.sql("SELECT id FROM accounts WHERE id = ? FOR UPDATE")
			.param(id)
			.query(Long.class)
			.single());

		assertThat(locked).isEqualTo(id);
	}

	@Test
	void appRoleCannotUpdateAccountName() {
		String name = uniqueName();
		long id = createCustomer(name);

		assertThatThrownBy(() -> tx.executeWithoutResult(status -> jdbc.sql("UPDATE accounts SET name = ? WHERE id = ?")
			.params(uniqueName(), id)
			.update()))
			.satisfies(e -> assertThat(rootSqlState(e)).isEqualTo(INSUFFICIENT_PRIVILEGE));

		assertThat(find(id).name()).isEqualTo(name);
	}

	@Test
	void appRoleCannotInsertBalance() {
		String name = uniqueName();

		assertThatThrownBy(() -> tx.executeWithoutResult(
				status -> jdbc.sql("INSERT INTO accounts (name, type, balance) VALUES (?, 'CUSTOMER', 100)")
					.param(name)
					.update()))
			.satisfies(e -> assertThat(rootSqlState(e)).isEqualTo(INSUFFICIENT_PRIVILEGE));

		assertThat(countByName(name)).isZero();
	}

	@Test
	void onlyExternalAccountsMayGoNegative() {
		String name = uniqueName();

		assertThatThrownBy(() -> tx.executeWithoutResult(status -> jdbc
			.sql("INSERT INTO accounts (name, type, allow_negative) VALUES (?, 'CUSTOMER', true)")
			.param(name)
			.update()))
			.satisfies(e -> assertThat(rootSqlState(e)).isEqualTo(CHECK_VIOLATION));

		assertThat(countByName(name)).isZero();
	}

	private long createCustomer(String name) {
		return tx.execute(status -> jdbc.sql("INSERT INTO accounts (name, type) VALUES (?, 'CUSTOMER') RETURNING id")
			.param(name)
			.query(Long.class)
			.single());
	}

	private Account find(long id) {
		return jdbc.sql("SELECT name, type, currency, balance, allow_negative FROM accounts WHERE id = ?")
			.param(id)
			.query((rs, rowNum) -> new Account(rs.getString("name"), rs.getString("type"), rs.getString("currency"),
					rs.getBigDecimal("balance"), rs.getBoolean("allow_negative")))
			.single();
	}

	private long countByName(String name) {
		return jdbc.sql("SELECT count(*) FROM accounts WHERE name = ?").param(name).query(Long.class).single();
	}

	private static String uniqueName() {
		return "smoke-" + UUID.randomUUID();
	}

	// SQLState of the deepest SQLException, beneath Spring's DataAccessException wrappers.
	private static String rootSqlState(Throwable thrown) {
		SQLException root = null;
		for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
			if (cause instanceof SQLException sql) {
				root = sql;
			}
		}
		if (root == null) {
			throw new AssertionError("no SQLException in the cause chain of " + thrown, thrown);
		}
		return root.getSQLState();
	}

}
