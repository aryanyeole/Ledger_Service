package dev.aryanyeole.ledgerapi.outbox;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import dev.aryanyeole.ledgerapi.db.WriteTransactions;
import dev.aryanyeole.ledgerapi.outbox.OutboxEnvelope.Row;

// The outbox relay (spec §6.3, D-008, D-079). One batch per call, in one write transaction:
//   lock up to batch-size unpublished rows of this relay's topic, oldest id first, skipping rows
//   another relay holds -> send every record -> wait for every ack -> mark them published ->
//   commit.
// This is the one sanctioned network call inside a DB transaction (CLAUDE.md invariant 5): the
// row locks are what stop two relays from publishing the same row, so they must be held until the
// row is marked. The producer's timeouts bound how long that can take while Kafka is down.
// At-least-once: if anything fails after a send (a failed ack, the mark, the commit, a crash),
// the whole batch rolls back and is published again next time; consumers dedup by event_id.
// Rows are selected by published_at IS NULL, never by an id high-water mark: identity values
// are assigned at insert, not at commit, so a later-committing row can have a lower id.
public class OutboxRelay {

	// Only rows of the topic this relay's guard verified (D-079, D-080). Rows for any other topic
	// are never published by this relay: they stay unpublished, and M6's per-topic outbox backlog
	// gauge must surface them (open issue). One wrong row can't block every event behind it.
	private static final String SELECT_BATCH = """
			SELECT id, event_id, topic, message_key, event_type, payload::text AS payload,
			       headers->>'correlation_id' AS correlation_id, created_at
			FROM outbox
			WHERE published_at IS NULL AND topic = :topic
			ORDER BY id
			LIMIT :limit
			FOR UPDATE SKIP LOCKED""";

	// The D-077 trigger replaces now() with clock_timestamp(): the time of marking, after the acks.
	private static final String MARK_PUBLISHED = "UPDATE outbox SET published_at = now() WHERE id = ANY(?)";

	private static final Duration ACK_WAIT_MARGIN = Duration.ofSeconds(5);

	private final WriteTransactions transactions;

	private final JdbcClient jdbc;

	private final KafkaTemplate<String, String> kafka;

	private final RelayTopicGuard guard;

	private final RelayFaultHook faultHook;

	private final int batchSize;

	private final Duration ackWait;

	public OutboxRelay(WriteTransactions transactions, JdbcClient jdbc, KafkaTemplate<String, String> kafka,
			RelayTopicGuard guard, RelayFaultHook faultHook, OutboxRelayProperties properties) {
		this.transactions = transactions;
		this.jdbc = jdbc;
		this.kafka = kafka;
		this.guard = guard;
		this.faultHook = faultHook;
		this.batchSize = properties.batchSize();
		this.ackWait = properties.deliveryTimeout().plus(ACK_WAIT_MARGIN);
	}

	/**
	 * Publishes one batch and returns how many rows it marked; 0 if there was nothing to publish or
	 * the topic is not (yet) verified. Any failure rolls the batch back and propagates.
	 */
	public int publishBatch() {
		// Before the transaction: the guard's admin calls are network calls too.
		if (!this.guard.ready()) {
			return 0;
		}
		Integer published = this.transactions.execute((status) -> {
			List<Row> rows = this.jdbc.sql(SELECT_BATCH)
				.param("topic", this.guard.topic())
				.param("limit", this.batchSize)
				.query((rs, rowNum) -> new Row(rs.getLong("id"), rs.getObject("event_id", UUID.class),
						rs.getString("topic"), rs.getString("message_key"), rs.getString("event_type"),
						rs.getString("payload"), rs.getString("correlation_id"),
						rs.getObject("created_at", OffsetDateTime.class)))
				.list();
			if (rows.isEmpty()) {
				return 0;
			}
			List<Long> ids = rows.stream().map(Row::id).toList();
			List<CompletableFuture<SendResult<String, String>>> sends = new ArrayList<>(rows.size());
			for (Row row : rows) {
				sends.add(this.kafka.send(record(row)));
			}
			this.faultHook.afterSendBeforeMark(ids);
			awaitAcks(sends);
			int marked = this.jdbc.sql(MARK_PUBLISHED).param((Object) ids.toArray(Long[]::new)).update();
			if (marked != rows.size()) {
				throw new IllegalStateException("marked " + marked + " outbox rows, expected " + rows.size());
			}
			return marked;
		});
		return published;
	}

	private static ProducerRecord<String, String> record(Row row) {
		ProducerRecord<String, String> record = new ProducerRecord<>(row.topic(), row.messageKey(),
				OutboxEnvelope.build(row));
		if (row.correlationId() != null) {
			record.headers().add("correlation_id", row.correlationId().getBytes(StandardCharsets.UTF_8));
		}
		return record;
	}

	// Every send must be acked (acks=all) before any row is marked; one failure fails the batch.
	private void awaitAcks(List<CompletableFuture<SendResult<String, String>>> sends) {
		try {
			CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
				.get(this.ackWait.toMillis(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new KafkaException("interrupted while waiting for outbox acks", ex);
		}
		catch (ExecutionException ex) {
			throw new KafkaException("outbox send failed: " + ex.getCause(), ex.getCause());
		}
		catch (TimeoutException ex) {
			throw new KafkaException("outbox acks not received within " + this.ackWait, ex);
		}
	}

}
