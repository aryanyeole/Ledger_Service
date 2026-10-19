package dev.aryanyeole.ledgerapi;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

// Kafka for the outbox relay tests (D-096): apache/kafka pinned per D-027, auto-create off like
// Compose (D-080), so only the relay's topic guard can create a topic. Imported only by Kafka test
// classes, which also set ledger.outbox.relay.enabled=true and scheduled=false and so get their
// own Spring context and Postgres container. No @ServiceConnection: Boot's Kafka auto-configuration
// is excluded (D-098), so the relay reads its bootstrap address from ledger.outbox.relay.*.
@TestConfiguration(proxyBeanMethods = false)
public class KafkaTestcontainersConfiguration {

	@Bean
	KafkaContainer kafkaContainer() {
		return new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"))
			.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
	}

	@Bean
	DynamicPropertyRegistrar kafkaProperties(KafkaContainer kafka) {
		return (registry) -> registry.add("ledger.outbox.relay.bootstrap-servers", kafka::getBootstrapServers);
	}

}
