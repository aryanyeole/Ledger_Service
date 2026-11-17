package dev.aryanyeole.ledgerapi.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.zaxxer.hikari.HikariDataSource;

import io.micrometer.core.instrument.MeterRegistry;

import dev.aryanyeole.ledgerapi.TestcontainersConfiguration;

// WriteTransactions' post-commit actions (D-147) against real Postgres: they run after the commit, in
// registration order, with no transaction or synchronization active and the connection back in the
// pool; never after an exception or a silent rollback (status.setRollbackOnly(), after which
// TransactionTemplate.execute still returns normally); a throwing action is counted and logged and
// neither stops the next action nor changes the result; registering outside execute is a bug. Same
// annotations as RedisGatewayAbsentTest, so this reuses that cached context (nothing else uses the
// pool here: relay and purge are off).
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class WriteTransactionsPostCommitTest {

	@Autowired
	WriteTransactions transactions;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	DataSource dataSource;

	@Autowired
	MeterRegistry meters;

	private HikariDataSource pool;

	@BeforeEach
	void setUp() throws Exception {
		this.pool = this.dataSource.unwrap(HikariDataSource.class);
		// Hikari starts its pool, and its MXBean, on the first borrow.
		this.jdbc.sql("SELECT 1").query(Integer.class).single();
	}

	record Observed(String name, boolean transactionActive, boolean synchronizationActive, int activeConnections) {
	}

	@Test
	void actionsRunAfterTheCommitInOrderWithNoTransactionAndTheConnectionReturned() {
		int baseline = this.pool.getHikariPoolMXBean().getActiveConnections();
		List<Observed> observed = new ArrayList<>();
		double ok = count("ok");

		String result = this.transactions.execute((status) -> {
			assertThat(this.pool.getHikariPoolMXBean().getActiveConnections()).as("inside: one connection held")
				.isEqualTo(baseline + 1);
			this.jdbc.sql("SELECT 1").query(Integer.class).single();
			this.transactions.afterCommit("first", () -> observed.add(observe("first")));
			this.transactions.afterCommit("second", () -> observed.add(observe("second")));
			assertThat(observed).as("nothing runs before the commit").isEmpty();
			return "committed";
		});

		assertThat(result).isEqualTo("committed");
		assertThat(observed).containsExactly(new Observed("first", false, false, baseline),
				new Observed("second", false, false, baseline));
		assertThat(count("ok") - ok).isEqualTo(2);
	}

	@Test
	void noActionsAfterAnException() {
		List<String> ran = new ArrayList<>();
		double ok = count("ok");
		double failed = count("failed");

		assertThatThrownBy(() -> this.transactions.execute((status) -> {
			this.transactions.afterCommit("never", () -> ran.add("never"));
			throw new IllegalArgumentException("rolled back");
		})).isInstanceOf(IllegalArgumentException.class).hasMessage("rolled back");

		assertThat(ran).isEmpty();
		assertThat(count("ok")).isEqualTo(ok);
		assertThat(count("failed")).isEqualTo(failed);
	}

	@Test
	void noActionsAfterASilentRollback() {
		List<String> ran = new ArrayList<>();
		double ok = count("ok");

		String result = this.transactions.execute((status) -> {
			this.transactions.afterCommit("never", () -> ran.add("never"));
			status.setRollbackOnly();
			return "returned normally";
		});

		assertThat(result).as("execute returns normally after a silent rollback").isEqualTo("returned normally");
		assertThat(ran).isEmpty();
		assertThat(count("ok")).isEqualTo(ok);
	}

	@Test
	void aThrowingActionIsCountedAndNeitherStopsTheNextNorChangesTheResult() {
		List<String> ran = new ArrayList<>();
		double ok = count("ok");
		double failed = count("failed");

		String result = this.transactions.execute((status) -> {
			this.transactions.afterCommit("throws", () -> {
				throw new IllegalStateException("action failed");
			});
			this.transactions.afterCommit("next", () -> ran.add("next"));
			return "committed";
		});

		assertThat(result).isEqualTo("committed");
		assertThat(ran).containsExactly("next");
		assertThat(count("failed") - failed).isEqualTo(1);
		assertThat(count("ok") - ok).isEqualTo(1);
	}

	@Test
	void afterCommitOutsideExecuteIsABug() {
		assertThatIllegalStateException().isThrownBy(() -> this.transactions.afterCommit("outside", () -> {
		})).withMessageContaining("outside WriteTransactions.execute");
	}

	@Test
	void afterCommitFromInsideAnActionIsABugToo() {
		AtomicReference<RuntimeException> thrown = new AtomicReference<>();
		this.transactions.execute((status) -> {
			this.transactions.afterCommit("registers", () -> {
				try {
					this.transactions.afterCommit("late", () -> {
					});
				}
				catch (RuntimeException ex) {
					thrown.set(ex);
				}
			});
			return null;
		});
		assertThat(thrown.get()).isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("outside WriteTransactions.execute");
	}

	private Observed observe(String name) {
		return new Observed(name, TransactionSynchronizationManager.isActualTransactionActive(),
				TransactionSynchronizationManager.isSynchronizationActive(),
				this.pool.getHikariPoolMXBean().getActiveConnections());
	}

	private double count(String result) {
		return this.meters.counter("ledger.db.post_commit_action", "result", result).count();
	}

}
