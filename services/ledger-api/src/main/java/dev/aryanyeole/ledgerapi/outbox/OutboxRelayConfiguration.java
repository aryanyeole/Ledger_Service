package dev.aryanyeole.ledgerapi.outbox;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import dev.aryanyeole.ledgerapi.db.WriteTransactions;
import dev.aryanyeole.ledgerapi.payments.PaymentsProperties;

// The relay and the only Kafka client in ledger-api (D-079, D-080, D-098). Boot's Kafka
// auto-configuration is excluded, so with ledger.outbox.relay.enabled=false there is no producer,
// template or admin client at all, and nothing can publish outside the outbox (invariant 5).
// The scheduler is a nested configuration that also needs ledger.outbox.relay.scheduled=true;
// @EnableScheduling is SchedulingConfiguration's, active whenever any scheduler is on (D-127).
// Producer (D-079): acks=all with the idempotent producer, so retries within a producer session
// neither duplicate nor reorder records (max.in.flight <= 5 keeps per-partition order). No
// transactional id: Kafka transactions can't include the Postgres commit.
@Configuration(proxyBeanMethods = false)
@ConditionalOnBooleanProperty("ledger.outbox.relay.enabled")
@EnableConfigurationProperties({ OutboxRelayProperties.class, PaymentsProperties.class })
class OutboxRelayConfiguration {

	@Bean
	DefaultKafkaProducerFactory<String, String> outboxRelayProducerFactory(OutboxRelayProperties properties) {
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", properties.bootstrapServers()));
		config.put(ProducerConfig.CLIENT_ID_CONFIG, "ledger-api-outbox-relay");
		config.put(ProducerConfig.ACKS_CONFIG, "all");
		config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
		config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) properties.deliveryTimeout().toMillis());
		config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) properties.requestTimeout().toMillis());
		config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, properties.maxBlock().toMillis());
		config.put(ProducerConfig.LINGER_MS_CONFIG, (int) properties.linger().toMillis());
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return new DefaultKafkaProducerFactory<>(config);
	}

	@Bean
	KafkaTemplate<String, String> outboxRelayKafkaTemplate(
			DefaultKafkaProducerFactory<String, String> outboxRelayProducerFactory) {
		return new KafkaTemplate<>(outboxRelayProducerFactory);
	}

	// A test configuration may replace this with a @Primary hook; no property can arm it.
	@Bean
	@ConditionalOnMissingBean
	RelayFaultHook relayFaultHook() {
		return (ids) -> {
		};
	}

	@Bean
	RelayTopicGuard relayTopicGuard(OutboxRelayProperties relay, PaymentsProperties payments) {
		return new RelayTopicGuard(relay.bootstrapServers(), payments.topic(), payments.partitions(),
				payments.replicationFactor(), (int) relay.requestTimeout().toMillis());
	}

	@Bean
	OutboxRelay outboxRelay(WriteTransactions transactions, JdbcClient jdbc,
			KafkaTemplate<String, String> outboxRelayKafkaTemplate, RelayTopicGuard relayTopicGuard,
			RelayFaultHook relayFaultHook, OutboxRelayProperties properties) {
		return new OutboxRelay(transactions, jdbc, outboxRelayKafkaTemplate, relayTopicGuard, relayFaultHook,
				properties);
	}

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnBooleanProperty("ledger.outbox.relay.scheduled")
	static class Scheduling {

		@Bean
		OutboxRelayScheduler outboxRelayScheduler(OutboxRelay outboxRelay) {
			return new OutboxRelayScheduler(outboxRelay);
		}

	}

}
