package dev.aryanyeole.ledgerapi.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

// Drives the relay (D-079): fixed delay, so a slow batch (Kafka down: up to the producer
// timeouts) never overlaps the next one. A failed batch is logged and retried on the next tick;
// the schedule never stops. Exists only when ledger.outbox.relay.scheduled=true as well as
// enabled; tests drive publishBatch() directly.
public class OutboxRelayScheduler {

	private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);

	private final OutboxRelay relay;

	public OutboxRelayScheduler(OutboxRelay relay) {
		this.relay = relay;
	}

	@Scheduled(fixedDelayString = "${ledger.outbox.relay.poll-interval:200ms}")
	public void tick() {
		try {
			this.relay.publishBatch();
		}
		catch (RuntimeException ex) {
			log.warn("Outbox relay batch failed; retrying next tick", ex);
		}
	}

}
