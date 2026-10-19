package dev.aryanyeole.ledgerapi.outbox;

import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

// ledger.outbox.relay.* (D-079, D-098). enabled and scheduled are read by the relay's conditions;
// main config sets both true, src/test/resources/application.properties sets both false, and
// Kafka test classes turn enabled back on (D-096). bootstrap-servers is the only Kafka address
// in ledger-api: Boot's Kafka auto-configuration is excluded.
// The producer timeouts bound how long the relay can hold its batch's row locks while Kafka is
// slow or down (max.block.ms for metadata, delivery.timeout.ms for each send). The producer
// refuses to start unless delivery.timeout.ms >= linger.ms + request.timeout.ms, so that is
// checked here, at startup, with a readable message.
@ConfigurationProperties("ledger.outbox.relay")
public record OutboxRelayProperties(@DefaultValue("false") boolean enabled, @DefaultValue("false") boolean scheduled,
		@DefaultValue("localhost:9092") List<String> bootstrapServers, @DefaultValue("100") int batchSize,
		@DefaultValue("200ms") Duration pollInterval, @DefaultValue("30s") Duration deliveryTimeout,
		@DefaultValue("10s") Duration requestTimeout, @DefaultValue("10s") Duration maxBlock,
		@DefaultValue("5ms") Duration linger) {

	public OutboxRelayProperties {
		if (bootstrapServers == null || bootstrapServers.isEmpty()
				|| bootstrapServers.stream().anyMatch((server) -> server == null || server.isBlank())) {
			throw new IllegalArgumentException("ledger.outbox.relay.bootstrap-servers must not be empty");
		}
		if (batchSize < 1) {
			throw new IllegalArgumentException("ledger.outbox.relay.batch-size must be at least 1, was " + batchSize);
		}
		requirePositive("poll-interval", pollInterval);
		requirePositive("delivery-timeout", deliveryTimeout);
		requirePositive("request-timeout", requestTimeout);
		requirePositive("max-block", maxBlock);
		if (linger == null || linger.isNegative()) {
			throw new IllegalArgumentException("ledger.outbox.relay.linger must not be negative");
		}
		for (Duration timeout : List.of(deliveryTimeout, requestTimeout, maxBlock, linger)) {
			if (timeout.toMillis() > Integer.MAX_VALUE) {
				throw new IllegalArgumentException("ledger.outbox.relay timeouts must fit in an int of milliseconds");
			}
		}
		if (deliveryTimeout.compareTo(linger.plus(requestTimeout)) < 0) {
			throw new IllegalArgumentException("ledger.outbox.relay.delivery-timeout (" + deliveryTimeout
					+ ") must be >= linger (" + linger + ") + request-timeout (" + requestTimeout + ")");
		}
	}

	private static void requirePositive(String name, Duration value) {
		if (value == null || value.isNegative() || value.isZero()) {
			throw new IllegalArgumentException("ledger.outbox.relay." + name + " must be positive");
		}
	}

}
