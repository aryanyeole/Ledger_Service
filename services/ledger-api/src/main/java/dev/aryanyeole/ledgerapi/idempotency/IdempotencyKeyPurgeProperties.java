package dev.aryanyeole.ledgerapi.idempotency;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

// ledger.idempotency.purge.* (D-020, D-126, D-127). enabled creates the purger, scheduled its
// scheduler; main config sets both true, src/test/resources/application.properties both false
// (D-096), and migrate-only mode forces both false (D-083). Bound and validated whether or not
// the purge is enabled, so an invalid value fails startup on every instance. A batch size of 0
// would end every run on a "short batch" of 0 without deleting anything; the cap keeps one
// batch's locks and WAL bounded.
@ConfigurationProperties("ledger.idempotency.purge")
public record IdempotencyKeyPurgeProperties(@DefaultValue("false") boolean enabled,
		@DefaultValue("false") boolean scheduled, @DefaultValue("1000") int batchSize,
		@DefaultValue("5m") Duration interval) {

	static final int MAX_BATCH_SIZE = 10_000;

	public IdempotencyKeyPurgeProperties {
		if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
			throw new IllegalArgumentException("ledger.idempotency.purge.batch-size must be from 1 to "
					+ MAX_BATCH_SIZE + ", was " + batchSize);
		}
		if (interval == null || interval.isNegative() || interval.isZero()) {
			throw new IllegalArgumentException(
					"ledger.idempotency.purge.interval must be positive, was " + interval);
		}
	}

}
