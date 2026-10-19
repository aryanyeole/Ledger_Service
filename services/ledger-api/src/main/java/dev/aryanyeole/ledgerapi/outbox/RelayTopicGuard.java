package dev.aryanyeole.ledgerapi.outbox;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// The relay owns its topic (D-080). Before the first batch it makes sure the topic exists with the
// expected partition count: absent -> created (partitions, replication factor from config);
// present with that count -> ready; present with another count -> stopped for good, with an ERROR,
// because adding partitions would remap keys and break per-payment ordering, and publishing to a
// wrong topic would put events in the wrong place. Partitions are never added. An unreachable
// broker or a timeout leaves the guard unchecked, so the next tick tries again; ledger-api keeps
// serving while Kafka is down. Runs before the relay opens its DB transaction, so these network
// calls never happen inside one (invariant 5). A short-lived Admin client per check, with bounded
// timeouts; no NewTopic bean (KafkaAdmin could reconcile partition counts on its own).
public class RelayTopicGuard {

	private static final Logger log = LoggerFactory.getLogger(RelayTopicGuard.class);

	enum State {

		UNCHECKED, READY, STOPPED

	}

	private final List<String> bootstrapServers;

	private final String topic;

	private final int partitions;

	private final short replicationFactor;

	private final int timeoutMillis;

	private State state = State.UNCHECKED;

	public RelayTopicGuard(List<String> bootstrapServers, String topic, int partitions, short replicationFactor,
			int timeoutMillis) {
		this.bootstrapServers = List.copyOf(bootstrapServers);
		this.topic = topic;
		this.partitions = partitions;
		this.replicationFactor = replicationFactor;
		this.timeoutMillis = timeoutMillis;
	}

	public String topic() {
		return this.topic;
	}

	synchronized State state() {
		return this.state;
	}

	/** True once the topic is verified; false while unchecked (retry next tick) or stopped (for good). */
	public synchronized boolean ready() {
		if (this.state == State.UNCHECKED) {
			this.state = check();
		}
		return this.state == State.READY;
	}

	private State check() {
		try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
				String.join(",", this.bootstrapServers), AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
				this.timeoutMillis, AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, this.timeoutMillis,
				AdminClientConfig.CLIENT_ID_CONFIG, "ledger-api-outbox-relay-admin"))) {
			int actual = partitionCount(admin);
			if (actual < 0) {
				create(admin);
				actual = partitionCount(admin);
				if (actual < 0) {
					// Created but not yet visible in metadata: not a mismatch, check again next tick.
					log.info("Outbox relay topic {} not visible yet after creation; retrying next tick", this.topic);
					return State.UNCHECKED;
				}
			}
			if (actual != this.partitions) {
				log.error("Outbox relay stopped: topic {} has {} partitions, expected {}. Partitions are never added; "
						+ "fix the topic and restart.", this.topic, actual, this.partitions);
				return State.STOPPED;
			}
			log.info("Outbox relay topic {} verified: {} partitions", this.topic, actual);
			return State.READY;
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			log.warn("Outbox relay topic check for {} interrupted; retrying next tick", this.topic);
			return State.UNCHECKED;
		}
		catch (ExecutionException | TimeoutException | RuntimeException ex) {
			log.warn("Outbox relay topic check for {} failed ({}); retrying next tick", this.topic, ex.toString());
			return State.UNCHECKED;
		}
	}

	// Partition count of the topic, or -1 if it doesn't exist.
	private int partitionCount(Admin admin) throws InterruptedException, ExecutionException, TimeoutException {
		try {
			TopicDescription description = admin.describeTopics(Set.of(this.topic))
				.allTopicNames()
				.get(this.timeoutMillis, TimeUnit.MILLISECONDS)
				.get(this.topic);
			return description.partitions().size();
		}
		catch (ExecutionException ex) {
			if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
				return -1;
			}
			throw ex;
		}
	}

	private void create(Admin admin) throws InterruptedException, ExecutionException, TimeoutException {
		try {
			admin.createTopics(Set.of(new NewTopic(this.topic, this.partitions, this.replicationFactor)))
				.all()
				.get(this.timeoutMillis, TimeUnit.MILLISECONDS);
			log.info("Outbox relay created topic {} with {} partitions, replication factor {}", this.topic,
					this.partitions, this.replicationFactor);
		}
		catch (ExecutionException ex) {
			// Created concurrently (another relay instance): describe it like any existing topic.
			if (!(ex.getCause() instanceof TopicExistsException)) {
				throw ex;
			}
		}
	}

}
