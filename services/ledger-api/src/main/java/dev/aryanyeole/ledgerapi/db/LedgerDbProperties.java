package dev.aryanyeole.ledgerapi.db;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

// ledger.db.lock-timeout (D-050): how long a write transaction waits for a row lock or a
// uniqueness check before Postgres raises 55P03. Set with SET LOCAL as a whole number of
// milliseconds, so it must be positive, whole milliseconds, and fit Postgres' int limit.
@ConfigurationProperties("ledger.db")
public record LedgerDbProperties(@DefaultValue("5s") Duration lockTimeout) {

	public LedgerDbProperties {
		if (lockTimeout.isNegative() || lockTimeout.isZero() || lockTimeout.toNanos() % 1_000_000 != 0
				|| lockTimeout.toMillis() > Integer.MAX_VALUE) {
			throw new IllegalArgumentException(
					"ledger.db.lock-timeout must be a positive whole number of milliseconds, at most 2147483647ms");
		}
	}

	/** The value for SET LOCAL lock_timeout: an integer of milliseconds, e.g. "5000". */
	String lockTimeoutMillis() {
		return Long.toString(this.lockTimeout.toMillis());
	}

}
