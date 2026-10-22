package dev.aryanyeole.ledgerapi.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

// Drives the purge (D-126): fixed delay, so runs never overlap. A failed run is logged and the
// next tick retries; the schedule never stops. Exists only when ledger.idempotency.purge.scheduled
// is true as well as enabled; tests call purgeExpired() directly.
public class IdempotencyKeyPurgeScheduler {

	private static final Logger log = LoggerFactory.getLogger(IdempotencyKeyPurgeScheduler.class);

	private final IdempotencyKeyPurger purger;

	public IdempotencyKeyPurgeScheduler(IdempotencyKeyPurger purger) {
		this.purger = purger;
	}

	@Scheduled(fixedDelayString = "${ledger.idempotency.purge.interval:5m}")
	public void tick() {
		try {
			this.purger.purgeExpired();
		}
		catch (RuntimeException ex) {
			log.error("Idempotency key purge failed; retrying next tick", ex);
		}
	}

}
