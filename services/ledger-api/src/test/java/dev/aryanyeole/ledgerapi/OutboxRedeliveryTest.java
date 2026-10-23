package dev.aryanyeole.ledgerapi;

import static dev.aryanyeole.ledgerapi.ApiTestClient.keyedPost;
import static dev.aryanyeole.ledgerapi.ApiTestClient.newKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.mock.MockProducerFactory;
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

// Proof (spec §9 M3, D-008, D-079, D-099): the outbox relay is at-least-once and never loses or
// skips an event.
//  1. A failure after the acks and before the mark republishes the batch: two byte-identical
//     copies per event, same key and partition.
//  2. With the broker paused (producer warmed first), the tick fails within its bounded timeouts
//     and marks nothing; after recovery every event is on the topic at least once. Duplicates are
//     allowed and counted: a paused broker still accepts a request the client already gave up on
//     and appends it later, so pause can show "nothing marked" but never loss.
//  2b. With a MockProducer whose sends fail, the tick throws and leaves every row unpublished for
//     the next tick: the relay's ack contract, deterministically, on real Postgres.
//  3. A row that commits after a higher id is still published (published_at IS NULL, not an id
//     high-water mark).
//  4. Two relays running at once publish every event exactly once; a barrier in the fault hook
//     holds each one's first batch until both have one, so they provably overlap. No per-key
//     order assertion: concurrent relays don't preserve it (D-100).
// Own context and containers: shortened producer timeouts. Ticks are driven directly; every
// test drains first and asserts only on its own keys and event ids.
@Import({ TestcontainersConfiguration.class, KafkaTestcontainersConfiguration.class, OutboxRedeliveryTest.Hooks.class })
@SpringBootTest(properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.outbox.relay.enabled=true",
		"ledger.outbox.relay.scheduled=false", "ledger.outbox.relay.delivery-timeout=3s",
		"ledger.outbox.relay.request-timeout=2s", "ledger.outbox.relay.max-block=2s", "ledger.outbox.relay.linger=5ms" })
@AutoConfigureMockMvc
class OutboxRedeliveryTest {

	private static final DateTimeFormatter OCCURRED_AT = DateTimeFormatter
		.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS'Z'");

	private static final Duration ACK_WAIT_MARGIN = Duration.ofSeconds(5);

	private static final Duration RECOVERY = Duration.ofSeconds(60);

	@Autowired
	MockMvc mvc;

	@Autowired
	DataSource dataSource;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	OutboxRelay relay;

	@Autowired
	OutboxWriter outbox;

	@Autowired
	WriteTransactions transactions;

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	ArmableFaultHook hook;

	@Autowired
	OutboxRelayProperties relayProperties;

	@Autowired
	PaymentsProperties paymentsProperties;

	@Autowired
	DlqTopicGuard dlqGuard;

	@Autowired
	KafkaContainer kafka;

	private KafkaTopicReader reader;

	private record OutboxRow(long id, String key, UUID eventId, OffsetDateTime createdAt, OffsetDateTime publishedAt) {
	}

	// Disarmed by default and after every test. throwOnce fails the next batch after its acks.
	// The barrier makes the first batch of each thread wait (up to 4 s, below lock_timeout) for the
	// first batch of the other; it never throws, it records whether the barrier tripped.
	static class ArmableFaultHook implements RelayFaultHook {

		final AtomicBoolean throwOnce = new AtomicBoolean();

		volatile CyclicBarrier barrier;

		final Set<Thread> waited = ConcurrentHashMap.newKeySet();

		final AtomicBoolean tripped = new AtomicBoolean();

		final AtomicBoolean timedOut = new AtomicBoolean();

		@Override
		public void afterAcksBeforeMark(List<Long> ids) {
			if (this.throwOnce.compareAndSet(true, false)) {
				throw new IllegalStateException("injected failure after the acks, before the mark");
			}
			CyclicBarrier current = this.barrier;
			if (current != null && this.waited.add(Thread.currentThread())) {
				try {
					current.await(4, TimeUnit.SECONDS);
					this.tripped.set(true);
				}
				catch (TimeoutException | BrokenBarrierException ex) {
					this.timedOut.set(true);
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		}

		void disarm() {
			this.throwOnce.set(false);
			this.barrier = null;
			this.waited.clear();
			this.tripped.set(false);
			this.timedOut.set(false);
		}

	}

	// KafkaTemplate closes a non-transactional producer after every send; the real factory's shared
	// producer ignores that, so this one must too, or the second record of a batch would fail.
	static class NonClosingMockProducer extends MockProducer<String, String> {

		NonClosingMockProducer(Cluster cluster) {
			super(cluster, false, null, new StringSerializer(), new StringSerializer());
		}

		@Override
		public void close() {
		}

		@Override
		public void close(Duration timeout) {
		}

	}

	@TestConfiguration(proxyBeanMethods = false)
	static class Hooks {

		@Bean
		@Primary
		ArmableFaultHook armableFaultHook() {
			return new ArmableFaultHook();
		}

	}

	@BeforeEach
	void drain() {
		this.reader = new KafkaTopicReader(this.kafka.getBootstrapServers());
		this.hook.disarm();
		drainTolerantly();
	}

	@AfterEach
	void disarm() {
		this.hook.disarm();
	}

	// 1. Acked but not marked -> rolled back -> published again, byte for byte.
	@Test
	void failureAfterAcksRepublishesByteIdenticalCopies() throws Exception {
		Map<String, String> correlationByPayment = new LinkedHashMap<>();
		for (int i = 0; i < 5; i++) {
			String correlationId = "t5-" + UUID.randomUUID().toString().replace("-", "");
			correlationByPayment.put(authorize(correlationId), correlationId);
		}
		Set<String> payments = correlationByPayment.keySet();
		this.hook.throwOnce.set(true);

		assertThatThrownBy(() -> this.relay.publishBatch()).as("tick 1")
			.hasMessageContaining("injected failure after the acks");
		assertThat(countByKey(payments)).as("records per payment after tick 1").containsOnlyKeys(payments)
			.allSatisfy((key, count) -> assertThat(count).as("records for %s", key).isEqualTo(1L));
		assertThat(rows(payments)).as("rows after tick 1")
			.allSatisfy((row) -> assertThat(row.publishedAt()).as("published_at of %s", row.key()).isNull());

		assertThat(this.relay.publishBatch()).as("tick 2").isEqualTo(5);
		assertThat(rows(payments)).as("rows after tick 2")
			.allSatisfy((row) -> assertThat(row.publishedAt()).as("published_at of %s", row.key()).isNotNull());
		assertThat(this.relay.publishBatch()).as("tick 3").isZero();

		List<ConsumerRecord<String, String>> records = records((r) -> payments.contains(r.key()));
		Map<String, List<ConsumerRecord<String, String>>> byEvent = records.stream()
			.collect(Collectors.groupingBy((r) -> (String) JsonPath.read(r.value(), "$.event_id")));
		assertThat(byEvent).as("events on the topic").hasSize(5);
		byEvent.forEach((eventId, copies) -> {
			assertThat(copies).as("copies of %s", eventId).hasSize(2);
			assertThat(copies.get(1).value()).as("second copy of %s, byte for byte", eventId)
				.isEqualTo(copies.get(0).value());
			assertThat(copies.get(1).key()).as("key of %s", eventId).isEqualTo(copies.get(0).key());
			assertThat(copies.get(1).partition()).as("partition of %s", eventId).isEqualTo(copies.get(0).partition());
		});

		OutboxRow row = rows(payments).get(0);
		ConsumerRecord<String, String> copy = byEvent.get(row.eventId().toString()).get(0);
		String value = copy.value();
		assertThat(copy.key()).isEqualTo(row.key());
		assertThat((String) JsonPath.read(value, "$.event_type")).isEqualTo("payment.authorized");
		assertThat((Integer) JsonPath.read(value, "$.version")).isEqualTo(1);
		assertThat((String) JsonPath.read(value, "$.occurred_at"))
			.isEqualTo(row.createdAt().withOffsetSameInstant(ZoneOffset.UTC).format(OCCURRED_AT));
		String correlationId = correlationByPayment.get(row.key());
		assertThat((String) JsonPath.read(value, "$.correlation_id")).isEqualTo(correlationId);
		Header header = copy.headers().lastHeader("correlation_id");
		assertThat(header).as("correlation_id header").isNotNull();
		assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo(correlationId);
		assertThat((Object) JsonPath.read(value, "$.data.amount")).as("data.amount").isInstanceOf(String.class);
		assertThat((Object) JsonPath.read(value, "$.data.fee")).as("data.fee").isInstanceOf(String.class);
	}

	// 2. Broker paused: authorizes still work, the tick fails within its bounds and marks nothing,
	// and after recovery every event is there at least once. The producer is warmed first by
	// publishing one record: that loads metadata and opens the connection to the partition leader,
	// so the paused tick always reaches the ack wait and the producer expires the batch at its
	// delivery timeout, whatever ran before (partitionsFor alone fetches metadata over the bootstrap
	// connection only; run alone, the paused tick then sat on leader connection setup until the
	// relay's own 8 s ack wait). Pause can show "nothing marked" but never loss: the frozen broker's
	// kernel still accepts the produce request, and the broker appends it after unpause, which is
	// where the duplicates come from.
	@Test
	void brokerOutageMarksNothingAndLosesNothing() throws Exception {
		String warmUpKey = "m3t5-warmup-" + UUID.randomUUID();
		this.transactions.execute((status) -> {
			this.outbox.append(this.paymentsProperties.topic(), warmUpKey, "payment.authorized", Map.of("warm_up", true),
					Map.of());
			return null;
		});
		assertThat(this.relay.publishBatch()).as("warm-up tick").isEqualTo(1);
		String containerId = this.kafka.getContainerId();
		Set<String> payments;
		Throwable pausedFailure;
		long pausedTickMillis;
		List<OutboxRow> rowsDuringPause;
		this.kafka.getDockerClient().pauseContainerCmd(containerId).exec();
		try {
			payments = Set.of(authorize("t5-pause-1"), authorize("t5-pause-2"), authorize("t5-pause-3"));
			long started = System.nanoTime();
			pausedFailure = catchThrowable(() -> this.relay.publishBatch());
			pausedTickMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
			rowsDuringPause = rows(payments);
		}
		finally {
			this.kafka.getDockerClient().unpauseContainerCmd(containerId).exec();
		}
		int recoveryFailures = drainTolerantly();

		List<ConsumerRecord<String, String>> records = records((r) -> payments.contains(r.key()));
		Map<String, List<String>> copiesByEvent = records.stream()
			.collect(Collectors.groupingBy((r) -> (String) JsonPath.read(r.value(), "$.event_id"),
					Collectors.mapping(ConsumerRecord::value, Collectors.toList())));
		List<OutboxRow> rows = rows(payments);
		int duplicates = copiesByEvent.values().stream().mapToInt((copies) -> copies.size() - 1).sum();
		System.out.println("OUTBOX_REDELIVERY broker_pause duplicates=" + duplicates + " pausedTickMs="
				+ pausedTickMillis + " recoveryFailures=" + recoveryFailures + " pausedFailure=" + pausedFailure);

		// (a) the paused tick failed within its bound
		assertThat(pausedFailure).as("the tick while the broker was paused").isNotNull();
		assertThat(Duration.ofMillis(pausedTickMillis)).as("paused tick duration")
			.isLessThan(this.relayProperties.deliveryTimeout().plus(ACK_WAIT_MARGIN));
		// (b) and marked nothing
		assertThat(rowsDuringPause).as("rows right after the paused tick")
			.hasSize(3)
			.allSatisfy((row) -> assertThat(row.publishedAt()).as("published_at of %s", row.key()).isNull());
		// (c) after recovery: every event at least once, extra copies identical, every row marked
		assertThat(copiesByEvent.keySet()).as("events on the topic after recovery (at least once each)")
			.containsExactlyInAnyOrderElementsOf(rows.stream().map((r) -> r.eventId().toString()).toList());
		copiesByEvent.forEach((eventId, copies) -> assertThat(copies).as("copies of %s", eventId)
			.allSatisfy((copy) -> assertThat(copy).isEqualTo(copies.get(0))));
		assertThat(rows).as("rows after recovery")
			.allSatisfy((row) -> assertThat(row.publishedAt()).as("published_at of %s", row.key()).isNotNull());
	}

	// 2b. Ack failure, deterministically: a second relay over a MockProducer whose sends never
	// complete until the test fails them. Its tick must throw and leave every row unpublished; the
	// real relay then publishes them. Real Postgres, real guard against the real topic; only the
	// producer is fake. The rows are written after the drain and nothing else runs in between, so
	// the main relay never sees them before the mock tick.
	@Test
	void ackFailureLeavesEveryRowUnpublished() throws Exception {
		String topic = this.paymentsProperties.topic();
		String prefix = "m3t5-ackfail-" + UUID.randomUUID() + "-";
		Set<String> keys = Set.of(prefix + 0, prefix + 1, prefix + 2);
		NonClosingMockProducer mock = new NonClosingMockProducer(cluster(topic, this.paymentsProperties.partitions()));
		OutboxRelay mockRelay = new OutboxRelay(this.transactions, this.jdbc,
				new KafkaTemplate<>(new MockProducerFactory<>(() -> mock)),
				new RelayTopicGuard(this.relayProperties.bootstrapServers(), topic, this.paymentsProperties.partitions(),
						this.paymentsProperties.replicationFactor(), (int) this.relayProperties.requestTimeout().toMillis()),
				this.dlqGuard, this.hook, this.relayProperties);
		this.transactions.execute((status) -> {
			for (String key : keys) {
				this.outbox.append(topic, key, "payment.authorized", Map.of("key", key), Map.of());
			}
			return null;
		});

		ExecutorService pool = Executors.newSingleThreadExecutor();
		Throwable tickFailure;
		int failedSends = 0;
		try {
			Future<Integer> tick = pool.submit(mockRelay::publishBatch);
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
			while (mock.history().size() < keys.size()) {
				assertThat(System.nanoTime()).as("mock producer received %d sends", keys.size()).isLessThan(deadline);
				Thread.onSpinWait();
			}
			while (mock.errorNext(new org.apache.kafka.common.errors.TimeoutException("injected: no ack"))) {
				failedSends++;
			}
			tickFailure = catchThrowable(() -> tick.get(30, TimeUnit.SECONDS));
		}
		finally {
			pool.shutdownNow();
		}
		List<OutboxRow> afterMockTick = rows(keys);
		System.out.println("OUTBOX_REDELIVERY ack_failure sends=" + mock.history().size() + " failedSends=" + failedSends
				+ " tickFailure=" + tickFailure);

		// No row may be marked when its send failed: under "mark without awaiting acks" this is the loss.
		assertThat(afterMockTick).as("rows after the mock tick, all of whose sends failed")
			.hasSize(3)
			.allSatisfy((row) -> assertThat(row.publishedAt()).as("published_at of %s", row.key()).isNull());
		assertThat(tickFailure).as("the mock tick").isNotNull();

		drainTolerantly();
		Map<String, Long> copies = countByKey(keys);
		assertThat(copies.keySet()).as("keys on the topic after the real relay's tick (at least once each)")
			.containsExactlyInAnyOrderElementsOf(keys);
		assertThat(rows(keys)).as("rows after the real relay's tick")
			.allSatisfy((row) -> assertThat(row.publishedAt()).as("published_at of %s", row.key()).isNotNull());
	}

	// 3. B (higher id) commits while A (lower id) is still open: B is published first, and A is
	// published once it commits.
	@Test
	void lateCommittingLowerIdIsPublished() throws Exception {
		String keyA = "m3t5-late-A-" + UUID.randomUUID();
		String keyB = "m3t5-late-B-" + UUID.randomUUID();
		try (Connection a = LedgerTestSupport.connectApp(this.dataSource)) {
			try {
				long idA = insertRow(a, keyA);
				long idB;
				try (Connection b = LedgerTestSupport.connectApp(this.dataSource)) {
					idB = insertRow(b, keyB);
					b.commit();
				}
				assertThat(idA).as("A's id below B's").isLessThan(idB);

				assertThat(this.relay.publishBatch()).as("tick while A is open").isEqualTo(1);
				assertThat(countByKey(Set.of(keyA, keyB))).as("records after the first tick")
					.isEqualTo(Map.of(keyB, 1L));

				a.commit();
			}
			finally {
				a.rollback();
			}
		}
		assertThat(rows(Set.of(keyA))).as("A after its commit, before the next tick")
			.allSatisfy((row) -> assertThat(row.publishedAt()).isNull());

		assertThat(this.relay.publishBatch()).as("tick after A commits").isEqualTo(1);
		assertThat(countByKey(Set.of(keyA, keyB))).as("records after the second tick")
			.isEqualTo(Map.of(keyA, 1L, keyB, 1L));
		assertThat(rows(Set.of(keyA, keyB))).as("rows after the second tick")
			.hasSize(2)
			.allSatisfy((row) -> assertThat(row.publishedAt()).as("published_at of %s", row.key()).isNotNull());
	}

	// 4. Two relays behind one start gate; the barrier holds each one's first batch (acked, not yet
	// marked) until the other has one too.
	@Test
	void twoConcurrentRelaysPublishEachEventExactlyOnce() throws Exception {
		String prefix = "m3t5-two-" + UUID.randomUUID() + "-";
		String topic = this.paymentsProperties.topic();
		this.transactions.execute((status) -> {
			for (int i = 0; i < 300; i++) {
				Map<String, Object> payload = new LinkedHashMap<>();
				payload.put("seq", i);
				this.outbox.append(topic, prefix + (i % 30), "payment.authorized", payload, Map.of());
			}
			return null;
		});
		OutboxRelay second = new OutboxRelay(this.transactions, this.jdbc, this.kafkaTemplate,
				new RelayTopicGuard(this.relayProperties.bootstrapServers(), topic, this.paymentsProperties.partitions(),
						this.paymentsProperties.replicationFactor(), (int) this.relayProperties.requestTimeout().toMillis()),
				this.dlqGuard, this.hook, this.relayProperties);
		this.hook.barrier = new CyclicBarrier(2);
		ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
		CountDownLatch gate = new CountDownLatch(1);
		ExecutorService pool = Executors.newFixedThreadPool(2);
		List<Integer> published = new ArrayList<>();
		try {
			List<Future<Integer>> futures = new ArrayList<>();
			for (OutboxRelay relay : List.of(this.relay, second)) {
				futures.add(pool.submit(() -> {
					gate.await();
					int total = 0;
					for (int attempt = 0; attempt < 100; attempt++) {
						try {
							int batch = relay.publishBatch();
							if (batch == 0) {
								break;
							}
							total += batch;
						}
						catch (RuntimeException ex) {
							exceptions.add(ex);
						}
					}
					return total;
				}));
			}
			gate.countDown();
			for (Future<Integer> future : futures) {
				published.add(future.get(120, TimeUnit.SECONDS));
			}
		}
		finally {
			pool.shutdownNow();
		}

		Map<String, Long> copiesByEvent = records((r) -> r.key().startsWith(prefix)).stream()
			.collect(Collectors.groupingBy((r) -> (String) JsonPath.read(r.value(), "$.event_id"),
					Collectors.counting()));
		List<OutboxRow> rows = this.jdbc
			.sql("SELECT id, message_key, event_id, created_at, published_at FROM outbox WHERE message_key LIKE :prefix ORDER BY id")
			.param("prefix", prefix + "%")
			.query((rs, n) -> new OutboxRow(rs.getLong(1), rs.getString(2), rs.getObject(3, UUID.class),
					rs.getObject(4, OffsetDateTime.class), rs.getObject(5, OffsetDateTime.class)))
			.list();
		System.out.println("OUTBOX_REDELIVERY two_relays relay1=" + published.get(0) + " relay2=" + published.get(1)
				+ " barrierTripped=" + this.hook.tripped.get() + " barrierTimedOut=" + this.hook.timedOut.get()
				+ " exceptions=" + exceptions.size() + " events=" + copiesByEvent.size() + " records="
				+ copiesByEvent.values().stream().mapToLong(Long::longValue).sum() + " exceptionCauses="
				+ exceptions.stream().map(OutboxRedeliveryTest::rootMessage).distinct().toList());

		// (a) exactly once, (b) all marked, (c) no thread exceptions, (d) the relays overlapped.
		assertThat(copiesByEvent.keySet()).as("events on the topic")
			.containsExactlyInAnyOrderElementsOf(rows.stream().map((r) -> r.eventId().toString()).toList());
		assertThat(copiesByEvent).as("copies per event").allSatisfy((eventId, count) -> assertThat(count)
			.as("copies of %s", eventId).isEqualTo(1));
		assertThat(rows).as("rows").hasSize(300)
			.allSatisfy((row) -> assertThat(row.publishedAt()).as("published_at of row %d", row.id()).isNotNull());
		assertThat(exceptions).as("exceptions in the relay threads").isEmpty();
		assertThat(published).as("rows published by each relay").allSatisfy((n) -> assertThat(n).isPositive());
		assertThat(published.get(0) + published.get(1)).as("rows published in total").isEqualTo(300);
		assertThat(this.hook.tripped.get()).as("barrier tripped: both relays held an acked, unmarked batch at once")
			.isTrue();
	}

	// --- helpers ---

	// publishBatch until 0, tolerating failures while a paused broker recovers. Returns how many
	// ticks failed.
	private int drainTolerantly() {
		long deadline = System.nanoTime() + RECOVERY.toNanos();
		int failures = 0;
		while (true) {
			try {
				if (this.relay.publishBatch() == 0) {
					return failures;
				}
			}
			catch (RuntimeException ex) {
				failures++;
				assertThat(System.nanoTime()).as("relay still failing after %s: %s", RECOVERY, ex).isLessThan(deadline);
			}
		}
	}

	private String authorize(String correlationId) throws Exception {
		long customer = ApiTestClient.createAccount(this.mvc, "CUSTOMER");
		ApiTestClient.fund(this.mvc, customer, "150.00");
		long merchant = ApiTestClient.createAccount(this.mvc, "MERCHANT");
		MvcResult created = this.mvc
			.perform(keyedPost("/v1/payments", newKey(),
					"{\"customer_account_id\": " + customer + ", \"merchant_account_id\": " + merchant
							+ ", \"amount\": \"100.00\"}")
				.header(CorrelationIdFilter.HEADER, correlationId))
			.andExpect(status().isCreated())
			.andReturn();
		return JsonPath.read(created.getResponse().getContentAsString(), "$.payment_id");
	}

	private long insertRow(Connection conn, String key) throws Exception {
		try (PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO outbox (topic, message_key, event_type, payload) VALUES (?, ?, 'payment.authorized', '{}') RETURNING id")) {
			ps.setString(1, this.paymentsProperties.topic());
			ps.setString(2, key);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getLong(1);
			}
		}
	}

	private List<OutboxRow> rows(Set<String> keys) {
		return this.jdbc
			.sql("SELECT id, message_key, event_id, created_at, published_at FROM outbox WHERE message_key IN (:keys) ORDER BY id")
			.param("keys", keys)
			.query((rs, n) -> new OutboxRow(rs.getLong(1), rs.getString(2), rs.getObject(3, UUID.class),
					rs.getObject(4, OffsetDateTime.class), rs.getObject(5, OffsetDateTime.class)))
			.list();
	}

	private static String rootMessage(Throwable thrown) {
		Throwable root = thrown;
		while (root.getCause() != null) {
			root = root.getCause();
		}
		return root.getClass().getSimpleName() + ": " + root.getMessage();
	}

	// The topic's partitions as the MockProducer sees them (it partitions by key over this cluster).
	private static Cluster cluster(String topic, int partitions) {
		Node node = new Node(0, "mock", 9092);
		List<PartitionInfo> infos = IntStream.range(0, partitions)
			.mapToObj((p) -> new PartitionInfo(topic, p, node, new Node[] { node }, new Node[] { node }))
			.toList();
		return new Cluster("mock", List.of(node), infos, Set.of(), Set.of());
	}

	private List<ConsumerRecord<String, String>> records(Predicate<ConsumerRecord<String, String>> filter) {
		return this.reader.readAll(this.paymentsProperties.topic()).stream().filter(filter).toList();
	}

	private Map<String, Long> countByKey(Set<String> keys) {
		return records((r) -> keys.contains(r.key())).stream()
			.collect(Collectors.groupingBy(ConsumerRecord::key, Collectors.counting()));
	}

}
