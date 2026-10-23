package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;

// Test-side Kafka access for the relay tests: a plain KafkaConsumer (own group, earliest, no
// commits) that reads a topic from the beginning to its current end offsets, and a few Admin
// calls. Reading never waits for records: the relay returns only after every ack, so the end
// offsets include everything it published. Records come back in per-partition offset order. A
// topic can be briefly missing from the broker's metadata after createTopics returns (BUG-006), so
// createTopic waits until it is visible and readAll waits for its partitions, both polled against
// a deadline.
final class KafkaTopicReader {

	private static final Duration TIMEOUT = Duration.ofSeconds(15);

	private static final long POLL_PAUSE_MILLIS = 50;

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
			List<TopicPartition> partitions = partitionsOf(consumer, topic);
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
		createTopic(topic, partitions, Map.of());
	}

	/** A topic created by the test, with these topic configs (empty = the broker's defaults). */
	void createTopic(String topic, int partitions, Map<String, String> configs) throws Exception {
		try (Admin admin = admin()) {
			admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1).configs(configs))).all().get();
			// createTopics completes before every metadata read is guaranteed to see the topic (BUG-006).
			long deadline = System.nanoTime() + TIMEOUT.toNanos();
			String last = "unknown";
			while (true) {
				try {
					int visible = admin.describeTopics(List.of(topic))
						.allTopicNames()
						.get()
						.get(topic)
						.partitions()
						.size();
					if (visible == partitions) {
						return;
					}
					last = visible + " partitions";
				}
				catch (ExecutionException ex) {
					last = (ex.getCause() instanceof UnknownTopicOrPartitionException) ? "unknown"
							: ex.getCause().toString();
				}
				if (System.nanoTime() > deadline) {
					fail("topic " + topic + " not visible with " + partitions + " partitions within "
							+ TIMEOUT.toSeconds() + " s of creation (last: " + last + ")");
				}
				Thread.sleep(POLL_PAUSE_MILLIS);
			}
		}
	}

	/** A topic config value as the broker reports it (the override, or the broker's default). */
	String config(String topic, String key) throws Exception {
		try (Admin admin = admin()) {
			ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
			return admin.describeConfigs(List.of(resource)).all().get().get(resource).get(key).value();
		}
	}

	short replicationFactor(String topic) throws Exception {
		try (Admin admin = admin()) {
			return (short) admin.describeTopics(List.of(topic))
				.allTopicNames()
				.get()
				.get(topic)
				.partitions()
				.get(0)
				.replicas()
				.size();
		}
	}

	/** Each partition's end offset. */
	Map<Integer, Long> endOffsets(String topic) {
		Properties config = new Properties();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
			Map<Integer, Long> ends = new TreeMap<>();
			consumer.endOffsets(partitionsOf(consumer, topic), TIMEOUT).forEach((tp, end) -> ends.put(tp.partition(), end));
			return ends;
		}
	}

	// partitionsFor returns an empty list, without waiting, for a topic the broker doesn't know yet.
	private static List<TopicPartition> partitionsOf(KafkaConsumer<String, String> consumer, String topic) {
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (true) {
			List<TopicPartition> partitions = consumer.partitionsFor(topic, TIMEOUT)
				.stream()
				.map((info) -> new TopicPartition(topic, info.partition()))
				.toList();
			if (!partitions.isEmpty()) {
				return partitions;
			}
			if (System.nanoTime() > deadline) {
				fail("no partitions for " + topic + " within " + TIMEOUT.toSeconds() + " s");
			}
			try {
				Thread.sleep(POLL_PAUSE_MILLIS);
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("interrupted waiting for partitions of " + topic, ex);
			}
		}
	}

	private Admin admin() {
		return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers));
	}

}
