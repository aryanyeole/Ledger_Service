package dev.aryanyeole.ledgerapi.db;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

// The one way to run a write (D-061): an explicit TransactionTemplate at READ COMMITTED,
// not @Transactional, so the boundary is visible and can't be lost to Spring's
// self-invocation rule. READ COMMITTED is load-bearing: a duplicate waiting on the key's
// unique index sees the owner's committed row when it wakes; under REPEATABLE READ it would
// get 40001 instead of a replay. The first statement sets lock_timeout for this
// transaction only (D-050). Runtime exceptions roll back and reach the caller unchanged.
@Component
@EnableConfigurationProperties(LedgerDbProperties.class)
public class WriteTransactions {

	private final TransactionTemplate template;

	private final JdbcClient jdbc;

	// SET LOCAL takes no bind parameter; the value is a validated integer (LedgerDbProperties).
	private final String setLockTimeout;

	WriteTransactions(PlatformTransactionManager transactionManager, JdbcClient jdbc,
			LedgerDbProperties properties) {
		this.template = new TransactionTemplate(transactionManager);
		this.template.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
		this.template.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
		this.jdbc = jdbc;
		this.setLockTimeout = "SET LOCAL lock_timeout = '" + properties.lockTimeoutMillis() + "ms'";
	}

	public <T> T execute(TransactionCallback<T> action) {
		// A write transaction is never nested: SET LOCAL must be its first statement.
		if (TransactionSynchronizationManager.isActualTransactionActive()) {
			throw new IllegalStateException("write transactions must not be nested");
		}
		return this.template.execute((status) -> {
			this.jdbc.sql(this.setLockTimeout).update();
			return action.doInTransaction(status);
		});
	}

}
