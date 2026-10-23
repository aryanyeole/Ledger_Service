package dev.aryanyeole.ledgerapi.outbox;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ledger-api owns the dead-letter topic too (D-110), though only the worker produces to it. The
// relay calls ensure() after each batch's transaction has committed (never inside it: these are
// network calls) until the topic is settled:
//  - absent -> created with the configured partitions, the shared replication factor and
//    retention.ms=-1 (dead letters must not age out before someone handles them), then verified;
//  - present -> its partition count and retention.ms are verified and it is never altered: a
//    mismatch logs one ERROR naming every difference and is terminal for this process;
//  - an unreachable broker, a timeout or a failed creation -> WARN naming the exception, checked
//    again next tick; a topic not yet visible after creation (BUG-006) is checked again too.
// Its state is its own: unlike the main topic's guard (D-080), nothing here stops or delays the
// relay's publishing of payment events.
public class DlqTopicGuard {

	private static final Logger log = LoggerFactory.getLogger(DlqTopicGuard.class);

	static final String RETENTION = "retention.ms";

	static final String RETAIN_FOREVER = "-1";

	enum State {

		UNCHECKED, READY, MISMATCHED

	}

	private final List<String> bootstrapServers;

	private final String topic;

	private final int partitions;

	private final short replicationFactor;

	private final int timeoutMillis;

	private State state = State.UNCHECKED;

	public DlqTopicGuard(List<String> bootstrapServers, String topic, int partitions, short replicationFactor,
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

	/** Checks (and creates, if absent) the DLQ until it is READY or MISMATCHED; then does nothing. */
	public synchronized void ensure() {
		if (this.state == State.UNCHECKED) {
			this.state = check();
		}
	}

	private State check() {
		try (Admin admin = TopicAdmin.open(this.bootstrapServers, this.timeoutMillis, "ledger-api-dlq-topic-admin")) {
			int actual = TopicAdmin.partitionCount(admin, this.topic, this.timeoutMillis);
			if (actual < 0) {
				NewTopic create = new NewTopic(this.topic, this.partitions, this.replicationFactor)
					.configs(Map.of(RETENTION, RETAIN_FOREVER));
				if (TopicAdmin.create(admin, create, this.timeoutMillis)) {
					log.info("Created DLQ topic {} with {} partitions, replication factor {}, {}={}", this.topic,
							this.partitions, this.replicationFactor, RETENTION, RETAIN_FOREVER);
				}
				actual = TopicAdmin.partitionCount(admin, this.topic, this.timeoutMillis);
				if (actual < 0) {
					log.info("DLQ topic {} not visible yet after creation; checking again next tick", this.topic);
					return State.UNCHECKED;
				}
			}
			String retention = TopicAdmin.config(admin, this.topic, RETENTION, this.timeoutMillis);
			List<String> mismatches = new ArrayList<>();
			if (actual != this.partitions) {
				mismatches.add("has " + actual + " partitions, expected " + this.partitions);
			}
			if (!RETAIN_FOREVER.equals(retention)) {
				mismatches.add(RETENTION + "=" + retention + ", expected " + RETAIN_FOREVER);
			}
			if (!mismatches.isEmpty()) {
				log.error("DLQ topic {} does not match its configuration: {}. It is never altered and payment events "
						+ "keep publishing; fix the topic and restart.", this.topic, String.join("; ", mismatches));
				return State.MISMATCHED;
			}
			log.info("DLQ topic {} verified: {} partitions, {}={}", this.topic, actual, RETENTION, RETAIN_FOREVER);
			return State.READY;
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			log.warn("DLQ topic check for {} interrupted; checking again next tick", this.topic);
			return State.UNCHECKED;
		}
		catch (ExecutionException ex) {
			warnRetry(ex.getCause() != null ? ex.getCause() : ex);
			return State.UNCHECKED;
		}
		catch (TimeoutException | RuntimeException ex) {
			warnRetry(ex);
			return State.UNCHECKED;
		}
	}

	private void warnRetry(Throwable cause) {
		log.warn("DLQ topic check for {} failed ({}: {}); checking again next tick", this.topic,
				cause.getClass().getSimpleName(), cause.getMessage());
	}

}
