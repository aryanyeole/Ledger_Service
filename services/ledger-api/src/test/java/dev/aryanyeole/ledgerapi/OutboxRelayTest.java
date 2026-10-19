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
import java.util.regex.Pattern;
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
		OutboxRelay wrongTopicRelay = new OutboxRelay(this.transactions, this.jdbc, this.kafkaTemplate, guard,
				this.faultHook, this.relayProperties);

		assertThat(wrongTopicRelay.publishBatch()).as("first tick").isZero();
		assertThat(wrongTopicRelay.publishBatch()).as("second tick").isZero();
		assertThat(this.relay.publishBatch()).as("the main relay ignores other topics' rows").isZero();

		assertThat(rowsWithKey(key)).as("rows for %s", topic)
			.hasSize(3)
			.allSatisfy((r) -> assertThat(r.publishedAt()).as("published_at").isNull());
		assertThat(this.reader.readAll(topic)).as("records on %s", topic).isEmpty();
		assertThat(this.reader.partitionCount(topic)).as("partitions of %s", topic).isEqualTo(1);
		String error = "Outbox relay stopped: topic " + topic + " has 1 partitions, expected 3";
		assertThat(output.getAll().split(Pattern.quote(error), -1).length - 1)
			.as("ERROR lines naming %s", topic)
			.isEqualTo(1);
		assertThat(output.getAll()).contains("ERROR").contains(error);
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
