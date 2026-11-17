package dev.aryanyeole.ledgerapi.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

// The one way to run a write (D-061): an explicit TransactionTemplate at READ COMMITTED,
// not @Transactional, so the boundary is visible and can't be lost to Spring's
// self-invocation rule. READ COMMITTED is load-bearing: a duplicate waiting on the key's
// unique index sees the owner's committed row when it wakes; under REPEATABLE READ it would
// get 40001 instead of a replay. The first statement sets lock_timeout for this
// transaction only (D-050). Runtime exceptions roll back and reach the caller unchanged.
//
// Post-commit actions (D-147): code inside execute registers afterCommit(name, action); the
// actions run after TransactionTemplate.execute has returned (connection back in the pool, no
// transaction or synchronization active), in registration order, and only if the transaction
// committed. Returning isn't proof of a commit: execute also returns normally after a silent
// rollback (status.setRollbackOnly(), the idempotency replay path), so the outcome is recorded by a
// TransactionSynchronization.afterCompletion registered inside the callback (record only, no
// network call there; D-149 would refuse one). No actions after an exception, a silent rollback,
// or a commit whose outcome is unknown (the exception reaches the caller; if the commit landed, a
// balance is stale up to the value TTL: docs/design/cache-invalidation.md §5 row 16). An action's
// exception is logged with its name and exception type only, counted, and never changes the result.
@Component
@EnableConfigurationProperties(LedgerDbProperties.class)
public class WriteTransactions {

	private static final Logger log = LoggerFactory.getLogger(WriteTransactions.class);

	// The write transaction running on this thread, if any. Set and cleared only by execute.
	private static final ThreadLocal<Scope> SCOPE = new ThreadLocal<>();

	private final TransactionTemplate template;

	private final JdbcClient jdbc;

	// SET LOCAL takes no bind parameter; the value is a validated integer (LedgerDbProperties).
	private final String setLockTimeout;

	private final Counter actionsOk;

	private final Counter actionsFailed;

	WriteTransactions(PlatformTransactionManager transactionManager, JdbcClient jdbc, LedgerDbProperties properties,
			MeterRegistry meterRegistry) {
		this.template = new TransactionTemplate(transactionManager);
		this.template.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
		this.template.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
		this.jdbc = jdbc;
		this.setLockTimeout = "SET LOCAL lock_timeout = '" + properties.lockTimeoutMillis() + "ms'";
		this.actionsOk = actionCounter(meterRegistry, "ok");
		this.actionsFailed = actionCounter(meterRegistry, "failed");
	}

	private static Counter actionCounter(MeterRegistry meterRegistry, String result) {
		return Counter.builder("ledger.db.post_commit_action").tag("result", result).register(meterRegistry);
	}

	public <T> T execute(TransactionCallback<T> action) {
		// A write transaction is never nested: SET LOCAL must be its first statement.
		if (TransactionSynchronizationManager.isActualTransactionActive() || SCOPE.get() != null) {
			throw new IllegalStateException("write transactions must not be nested");
		}
		Scope scope = new Scope();
		SCOPE.set(scope);
		T result;
		try {
			result = this.template.execute((status) -> {
				TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {

					@Override
					public void afterCompletion(int completionStatus) {
						scope.completion = completionStatus;
					}

				});
				this.jdbc.sql(this.setLockTimeout).update();
				return action.doInTransaction(status);
			});
		}
		finally {
			SCOPE.remove();
		}
		if (scope.completion == TransactionSynchronization.STATUS_COMMITTED) {
			runActions(scope.actions);
		}
		return result;
	}

	/**
	 * Registers an action to run after the current write transaction commits. The name appears only in
	 * the failure log and the ledger.db.post_commit_action metric. Outside execute this is a bug.
	 */
	public void afterCommit(String name, Runnable action) {
		Objects.requireNonNull(name, "name");
		Objects.requireNonNull(action, "action");
		Scope scope = SCOPE.get();
		if (scope == null) {
			throw new IllegalStateException("post-commit action registered outside WriteTransactions.execute");
		}
		scope.actions.add(new Action(name, action));
	}

	private void runActions(List<Action> actions) {
		for (Action action : actions) {
			try {
				action.runnable().run();
				this.actionsOk.increment();
			}
			catch (RuntimeException ex) {
				this.actionsFailed.increment();
				log.error("Post-commit action {} failed ({})", action.name(), ex.getClass().getSimpleName());
			}
		}
	}

	private record Action(String name, Runnable runnable) {
	}

	private static final class Scope {

		private final List<Action> actions = new ArrayList<>();

		private int completion = TransactionSynchronization.STATUS_UNKNOWN;

	}

}
