package dev.aryanyeole.ledgerapi;

import static dev.aryanyeole.ledgerapi.ApiTestClient.keyedPost;
import static dev.aryanyeole.ledgerapi.ApiTestClient.newKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.kafka.KafkaContainer;

import com.jayway.jsonpath.JsonPath;

import dev.aryanyeole.ledgerapi.api.CorrelationIdFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.db.WriteTransactions;
import dev.aryanyeole.ledgerapi.outbox.DlqTopicGuard;
import dev.aryanyeole.ledgerapi.outbox.OutboxRelay;
import dev.aryanyeole.ledgerapi.outbox.OutboxRelayProperties;
import dev.aryanyeole.ledgerapi.outbox.OutboxWriter;
import dev.aryanyeole.ledgerapi.outbox.RelayFaultHook;
import dev.aryanyeole.ledgerapi.outbox.RelayTopicGuard;
import dev.aryanyeole.ledgerapi.payments.PaymentsProperties;

// The outbox relay against real Postgres and Kafka (D-079, D-080, D-097). Ticks are driven
// directly (scheduled=false). This class has its own context, so its own Postgres container
// (D-096); each test first drains whatever earlier tests left unpublished, then asserts only on
// its own message keys and event ids. Kafka is read with a plain consumer from the beginning.
@Import({ TestcontainersConfiguration.class, KafkaTestcontainersConfiguration.class })
@SpringBootTest(properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.outbox.relay.enabled=true",
		"ledger.outbox.relay.scheduled=false" })
@AutoConfigureMockMvc
@ExtendWith(OutputCaptureExtension.class)
class OutboxRelayTest {

	// D-097's format, written out here independently of OutboxEnvelope.
	private static final DateTimeFormatter OCCURRED_AT = DateTimeFormatter
		.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS'Z'");

	@Autowired
	MockMvc mvc;

	@Autowired
	OutboxRelay relay;

	@Autowired
	OutboxWriter outbox;

	@Autowired
	WriteTransactions transactions;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	RelayFaultHook faultHook;

	@Autowired
	OutboxRelayProperties relayProperties;

	@Autowired
	PaymentsProperties paymentsProperties;

	@Autowired
	RelayTopicGuard mainGuard;

	@Autowired
	DlqTopicGuard dlqGuard;

	@Autowired
	KafkaContainer kafka;

	private KafkaTopicReader reader;

	private record OutboxRow(long id, UUID eventId, OffsetDateTime createdAt, OffsetDateTime publishedAt) {
	}

	@BeforeEach
	void drain() {
		this.reader = new KafkaTopicReader(this.kafka.getBootstrapServers());
		while (this.relay.publishBatch() > 0) {
			// earlier tests' rows
		}
	}

	@Test
	void authorizedPaymentIsPublishedOnceWithItsEnvelope() throws Exception {
		long customer = ApiTestClient.createAccount(this.mvc, "CUSTOMER");
		ApiTestClient.fund(this.mvc, customer, "150.00");
		long merchant = ApiTestClient.createAccount(this.mvc, "MERCHANT");
		String correlationId = "t4-" + UUID.randomUUID().toString().replace("-", "");
		MvcResult created = this.mvc
			.perform(keyedPost("/v1/payments", newKey(),
					"{\"customer_account_id\": " + customer + ", \"merchant_account_id\": " + merchant
							+ ", \"amount\": \"100.00\"}")
				.header(CorrelationIdFilter.HEADER, correlationId))
			.andExpect(status().isCreated())
			.andReturn();
		String paymentId = JsonPath.read(created.getResponse().getContentAsString(), "$.payment_id");
		OutboxRow row = rowsWithKey(paymentId).get(0);
		assertThat(row.publishedAt()).as("published_at before the tick").isNull();

		assertThat(this.relay.publishBatch()).as("rows published by the tick").isGreaterThanOrEqualTo(1);

		List<ConsumerRecord<String, String>> records = recordsWithKey(this.paymentsProperties.topic(), paymentId);
		assertThat(records).as("records for payment %s", paymentId).hasSize(1);
		ConsumerRecord<String, String> record = records.get(0);
		// data keeps the stored payload's key order, which is JSONB's: shorter keys first, then bytewise.
		String expected = "{\"event_id\":\"" + row.eventId() + "\",\"event_type\":\"payment.authorized\",\"version\":1,"
				+ "\"occurred_at\":\"" + row.createdAt().withOffsetSameInstant(ZoneOffset.UTC).format(OCCURRED_AT)
				+ "\",\"correlation_id\":\"" + correlationId + "\",\"data\":{\"fee\":\"2.90\",\"amount\":\"100.00\","
				+ "\"payment_id\":\"" + paymentId + "\",\"customer_account_id\":" + customer
				+ ",\"merchant_account_id\":" + merchant + "}}";
		assertThat(record.value()).as("envelope").isEqualTo(expected);
		Header header = record.headers().lastHeader("correlation_id");
		assertThat(header).as("correlation_id header").isNotNull();
		assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo(correlationId);
		assertThat(rowsWithKey(paymentId).get(0).publishedAt()).as("published_at after the tick").isNotNull();

		assertThat(this.relay.publishBatch()).as("next tick").isZero();
		assertThat(recordsWithKey(this.paymentsProperties.topic(), paymentId)).as("records after the next tick")
			.hasSize(1);
	}

	@Test
	void batchesAreLimitedAndEachKeyKeepsIdOrder() {
		String prefix = "m3t4-batch-" + UUID.randomUUID() + "-";
		String topic = this.paymentsProperties.topic();
		this.transactions.execute((status) -> {
			for (int i = 0; i < 250; i++) {
				Map<String, Object> payload = new LinkedHashMap<>();
				payload.put("seq", i);
				this.outbox.append(topic, prefix + (i % 25), "payment.authorized", payload, Map.of());
			}
			return null;
		});

		List<Integer> ticks = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			ticks.add(this.relay.publishBatch());
		}
		assertThat(ticks).as("rows published per tick").containsExactly(100, 100, 50, 0);

		List<OutboxRow> rows = this.jdbc
			.sql("SELECT id, event_id, created_at, published_at FROM outbox WHERE message_key LIKE :prefix ORDER BY id")
			.param("prefix", prefix + "%")
			.query((rs, n) -> new OutboxRow(rs.getLong(1), rs.getObject(2, UUID.class),
					rs.getObject(3, OffsetDateTime.class), rs.getObject(4, OffsetDateTime.class)))
			.list();
		assertThat(rows).hasSize(250);
		assertThat(rows).as("rows still unpublished").filteredOn((r) -> r.publishedAt() == null).isEmpty();

		Map<String, List<String>> expectedByKey = this.jdbc
			.sql("SELECT message_key, event_id::text FROM outbox WHERE message_key LIKE :prefix ORDER BY id")
			.param("prefix", prefix + "%")
			.query((rs, n) -> Map.entry(rs.getString(1), rs.getString(2)))
			.list()
			.stream()
			.collect(Collectors.groupingBy(Map.Entry::getKey, LinkedHashMap::new,
					Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
		Map<String, List<String>> publishedByKey = this.reader.readAll(topic)
			.stream()
			.filter((r) -> r.key().startsWith(prefix))
			.collect(Collectors.groupingBy(ConsumerRecord::key, LinkedHashMap::new,
					Collectors.mapping((r) -> (String) JsonPath.read(r.value(), "$.event_id"), Collectors.toList())));
		assertThat(publishedByKey).as("event ids per key, in consumption order").isEqualTo(expectedByKey);
	}

	@Test
	void relayCreatesItsTopicWithThreePartitions() throws Exception {
		assertThat(this.reader.partitionCount(this.paymentsProperties.topic()))
			.as("partitions of %s after the first tick", this.paymentsProperties.topic())
			.isEqualTo(3);
	}

	// A relay whose topic already exists with the wrong partition count stops for good: it
	// publishes nothing, touches no row, never adds partitions, and says why once.
	@Test
	void relayStopsWhenItsTopicHasTheWrongPartitionCount(CapturedOutput output) throws Exception {
		String topic = "m3t4-one-" + UUID.randomUUID();
		this.reader.createTopic(topic, 1);
		String key = "m3t4-one-key-" + UUID.randomUUID();
		this.transactions.execute((status) -> {
			for (int i = 0; i < 3; i++) {
				this.outbox.append(topic, key, "payment.authorized", Map.of("seq", i), Map.of());
			}
			return null;
		});
		RelayTopicGuard guard = new RelayTopicGuard(List.of(this.kafka.getBootstrapServers()), topic, 3, (short) 1,
				10_000);
		// (d, D-110) with a correct DLQ guard: the main topic's mismatch still stops the relay.
		OutboxRelay wrongTopicRelay = new OutboxRelay(this.transactions, this.jdbc, this.kafkaTemplate, guard,
				this.dlqGuard, this.faultHook, this.relayProperties);

		int firstTick = wrongTopicRelay.publishBatch();
		int secondTick = wrongTopicRelay.publishBatch();
		// The stop first, from its one ERROR line: a guard that is still UNCHECKED (the topic not yet
		// visible in metadata, BUG-006) also makes both ticks return 0, so 0 alone doesn't prove it.
		// RelayTopicGuard's state isn't visible outside its package; ready() can't tell UNCHECKED
		// from STOPPED and would check again.
		String error = "Outbox relay stopped: topic " + topic + " has 1 partitions, expected 3";
		List<String> errorLines = output.getAll().lines().filter((line) -> line.contains(error)).toList();
		assertThat(errorLines).as("ERROR lines naming %s", topic).hasSize(1);
		assertThat(errorLines.get(0)).as("level of the stop line").contains("ERROR");
		assertThat(firstTick).as("first tick").isZero();
		assertThat(secondTick).as("second tick").isZero();
		assertThat(this.relay.publishBatch()).as("the main relay ignores other topics' rows").isZero();

		assertThat(rowsWithKey(key)).as("rows for %s", topic)
			.hasSize(3)
			.allSatisfy((r) -> assertThat(r.publishedAt()).as("published_at").isNull());
		assertThat(this.reader.readAll(topic)).as("records on %s", topic).isEmpty();
		assertThat(this.reader.partitionCount(topic)).as("partitions of %s", topic).isEqualTo(1);
	}

	// --- the DLQ topic (D-110) ---

	// (a) The context's relay ensured the DLQ on its first tick (the drain in @BeforeEach): created
	// with 1 partition, retention.ms=-1 and the shared replication factor. Main-topic events are
	// published as before.
	@Test
	void relayCreatesTheDlqWithOnePartitionAndInfiniteRetention() throws Exception {
		String dlq = this.paymentsProperties.dlq().topic();
		assertThat(this.reader.partitionCount(dlq)).as("partitions of %s", dlq).isEqualTo(1);
		assertThat(this.reader.config(dlq, "retention.ms")).as("retention.ms of %s", dlq).isEqualTo("-1");
		assertThat(this.reader.replicationFactor(dlq)).as("replication factor of %s", dlq)
			.isEqualTo(this.paymentsProperties.replicationFactor());

		String key = "m4t6-main-" + UUID.randomUUID();
		appendMain(key, 2);
		assertThat(this.relay.publishBatch()).as("main-topic rows published").isEqualTo(2);
		assertThat(recordsWithKey(this.paymentsProperties.topic(), key)).hasSize(2);
	}

	// (b) A DLQ with the wrong partition count: one ERROR naming it and the counts across several
	// ticks, and main-topic rows are published on every tick (independent guard states).
	@Test
	void dlqPartitionMismatchLogsOnceAndPublishingContinues(CapturedOutput output) throws Exception {
		String dlq = "m4t6-dlq-partitions-" + UUID.randomUUID();
		this.reader.createTopic(dlq, 3, Map.of("retention.ms", "-1"));
		OutboxRelay relay = relayWithDlq(dlq);

		String key = "m4t6-b-" + UUID.randomUUID();
		for (int tick = 0; tick < 3; tick++) {
			appendMain(key, 2);
			assertThat(relay.publishBatch()).as("tick %d: main-topic rows published", tick).isEqualTo(2);
		}
		List<String> errors = linesContaining(output, "DLQ topic " + dlq + " does not match");
		assertThat(errors).as("ERROR lines naming %s", dlq).hasSize(1);
		assertThat(errors.get(0)).contains("ERROR").contains("has 3 partitions, expected 1");
		assertThat(this.reader.partitionCount(dlq)).as("never altered").isEqualTo(3);
		assertThat(recordsWithKey(this.paymentsProperties.topic(), key)).hasSize(6);
	}

	// (c) A DLQ with the broker's default retention: one ERROR naming retention.ms, publishing
	// continues, and the topic's config is unchanged afterwards (never altered).
	@Test
	void dlqRetentionMismatchLogsOnceAndTheTopicIsNeverAltered(CapturedOutput output) throws Exception {
		String dlq = "m4t6-dlq-retention-" + UUID.randomUUID();
		this.reader.createTopic(dlq, 1);
		String retentionBefore = this.reader.config(dlq, "retention.ms");
		assertThat(retentionBefore).as("broker default retention").isNotEqualTo("-1");
		OutboxRelay relay = relayWithDlq(dlq);

		String key = "m4t6-c-" + UUID.randomUUID();
		for (int tick = 0; tick < 3; tick++) {
			appendMain(key, 1);
			assertThat(relay.publishBatch()).as("tick %d: main-topic rows published", tick).isEqualTo(1);
		}
		List<String> errors = linesContaining(output, "DLQ topic " + dlq + " does not match");
		assertThat(errors).as("ERROR lines naming %s", dlq).hasSize(1);
		assertThat(errors.get(0)).contains("ERROR").contains("retention.ms=" + retentionBefore + ", expected -1");
		assertThat(this.reader.config(dlq, "retention.ms")).as("retention.ms afterwards").isEqualTo(retentionBefore);
	}

	// (e) The relay's producer never sends to the DLQ: after publishing, every DLQ partition is empty.
	@Test
	void relayNeverProducesToTheDlq() throws Exception {
		String key = "m4t6-e-" + UUID.randomUUID();
		appendMain(key, 3);
		assertThat(this.relay.publishBatch()).isEqualTo(3);
		assertThat(this.reader.endOffsets(this.paymentsProperties.dlq().topic())).as("DLQ end offsets")
			.isNotEmpty()
			.allSatisfy((partition, end) -> assertThat(end).as("end offset of partition %d", partition).isZero());
	}

	// (g) Independence, not a retry policy: a DLQ creation the broker rejects (a 250-character name,
	// which configuration refuses, so the guard is built directly here) keeps the DLQ guard
	// UNCHECKED, so every tick tries again with a WARN naming the exception, and it never delays the
	// main topic's batches. A name the broker always rejects is a configuration error (D-110).
	@Test
	void failedDlqCreationIsRetriedWithoutDelayingMainTopicBatches(CapturedOutput output) throws Exception {
		String dlq = "g".repeat(250);
		OutboxRelay relay = relayWithDlq(dlq);

		String key = "m4t6-g-" + UUID.randomUUID();
		for (int tick = 0; tick < 3; tick++) {
			appendMain(key, 1);
			assertThat(relay.publishBatch()).as("tick %d: main-topic rows published", tick).isEqualTo(1);
		}
		List<String> warnings = linesContaining(output, "DLQ topic check for " + dlq + " failed");
		assertThat(warnings).as("one WARN per tick (still UNCHECKED)").hasSize(3);
		assertThat(warnings).allSatisfy((line) -> assertThat(line).contains("WARN").contains("InvalidTopicException"));
		assertThat(linesContaining(output, "DLQ topic " + dlq + " does not match")).as("never MISMATCHED").isEmpty();
		assertThat(recordsWithKey(this.paymentsProperties.topic(), key)).hasSize(3);
	}

	private OutboxRelay relayWithDlq(String dlq) {
		DlqTopicGuard guard = new DlqTopicGuard(List.of(this.kafka.getBootstrapServers()), dlq, 1, (short) 1, 10_000);
		return new OutboxRelay(this.transactions, this.jdbc, this.kafkaTemplate, this.mainGuard, guard,
				this.faultHook, this.relayProperties);
	}

	private void appendMain(String key, int rows) {
		this.transactions.execute((status) -> {
			for (int i = 0; i < rows; i++) {
				this.outbox.append(this.paymentsProperties.topic(), key, "payment.authorized", Map.of("seq", i),
						Map.of());
			}
			return null;
		});
	}

	private static List<String> linesContaining(CapturedOutput output, String text) {
		return output.getAll().lines().filter((line) -> line.contains(text)).toList();
	}

	private List<OutboxRow> rowsWithKey(String key) {
		return this.jdbc.sql("SELECT id, event_id, created_at, published_at FROM outbox WHERE message_key = :key ORDER BY id")
			.param("key", key)
			.query((rs, n) -> new OutboxRow(rs.getLong(1), rs.getObject(2, UUID.class),
					rs.getObject(3, OffsetDateTime.class), rs.getObject(4, OffsetDateTime.class)))
			.list();
	}

	private List<ConsumerRecord<String, String>> recordsWithKey(String topic, String key) {
		return this.reader.readAll(topic).stream().filter((r) -> key.equals(r.key())).toList();
	}

}
