package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

// Test-side Kafka access for the relay tests: a plain KafkaConsumer (own group, earliest, no
// commits) that reads a topic from the beginning to its current end offsets, and a few Admin
// calls. No sleeps: the relay returns only after every ack, so the end offsets include everything
// it published. Records come back in per-partition offset order.
final class KafkaTopicReader {

	private static final Duration TIMEOUT = Duration.ofSeconds(15);

	private final String bootstrapServers;

	KafkaTopicReader(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	List<ConsumerRecord<String, String>> readAll(String topic) {
		Properties config = new Properties();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, "m3t4-reader-" + UUID.randomUUID());
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
			List<TopicPartition> partitions = consumer.partitionsFor(topic, TIMEOUT)
				.stream()
				.map((info) -> new TopicPartition(topic, info.partition()))
				.toList();
			assertThat(partitions).as("partitions of %s", topic).isNotEmpty();
			consumer.assign(partitions);
			consumer.seekToBeginning(partitions);
			Map<TopicPartition, Long> end = consumer.endOffsets(partitions, TIMEOUT);
			List<ConsumerRecord<String, String>> records = new ArrayList<>();
			long deadline = System.nanoTime() + TIMEOUT.toNanos();
			while (partitions.stream().anyMatch((tp) -> consumer.position(tp, TIMEOUT) < end.get(tp))) {
				if (System.nanoTime() > deadline) {
					fail("did not reach the end offsets " + end + " of " + topic + " within " + TIMEOUT);
				}
				consumer.poll(Duration.ofMillis(200)).forEach(records::add);
			}
			return records;
		}
	}

	int partitionCount(String topic) throws Exception {
		try (Admin admin = admin()) {
			return admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions().size();
		}
	}

	void createTopic(String topic, int partitions) throws Exception {
		try (Admin admin = admin()) {
			admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
		}
	}

	private Admin admin() {
		return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers));
	}

}
