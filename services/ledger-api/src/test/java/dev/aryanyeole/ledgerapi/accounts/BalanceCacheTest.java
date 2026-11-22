package dev.aryanyeole.ledgerapi.accounts;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.containers.GenericContainer;

import com.jayway.jsonpath.JsonPath;

import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.MeterRegistry;

import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Response;
import dev.aryanyeole.ledgerapi.LedgerTestSupport;
import dev.aryanyeole.ledgerapi.RedisTestSupport;
import dev.aryanyeole.ledgerapi.RedisTestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.TestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.cache.BalanceCacheScripts;
import dev.aryanyeole.ledgerapi.cache.RedisGateway;

// Proof of the balance cache (D-142, D-147, D-151, docs/design/cache-invalidation.md §1.2, §3.5, §3.6,
// §4). Redis may decide only the body of GET /v1/accounts/{id}/balance, never a funds check, never a
// 404, and every committed posting invalidates the accounts it touched before its response returns.
// (a) a miss then a byte-identical hit; (b) read-your-writes after a deposit and a transfer; (c) an
// authorize invalidates the customer and ESCROW; (d) the populate race, deterministic, through
// BalanceCache's own read and populate: an invalidation between MGET and the compare-and-set makes it
// write nothing (§4.2); (e) a planted value never reaches the funds check; (f) a 404 isn't cached; (g)
// sub-cent balances are a 500 on the miss path (and not cached) and on the hit path (D-056); (h) a
// storm of readers and writers ends with every cached value equal to the DB and the sum unchanged; (i)
// the controller notes the token before its DB read (§4.2): a transfer committed during that read (the
// @Primary ArmedAccounts wrapper runs it, for one armed account) makes the populate write nothing.
// Values are planted with a raw Lettuce client on the patient URI (RedisClientSmokeTest's pattern),
// never through the gateway. Its own context and Postgres: (g) commits a sub-cent deposit, which
// leaves EXTERNAL sub-cent too; no case here reads EXTERNAL's balance. The gateway is warmed first
// (BUG-016).
@Import({ TestcontainersConfiguration.class, RedisTestcontainersConfiguration.class, BalanceCacheTest.Wrapper.class })
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
		properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.cache.balance.enabled=true" })
class BalanceCacheTest {

	private static final String PLANTED_AS_OF = "2026-01-01T00:00:00.000000Z";

	private static final int STORM_ACCOUNTS = 4;

	private static final int STORM_READERS = 8;

	private static final int STORM_WRITERS = 2;

	@LocalServerPort
	int port;

	@Autowired
	RedisGateway gateway;

	@Autowired
	GenericContainer<?> redisTestContainer;

	@Autowired
	BalanceCache cache;

	@Autowired
	AccountRepository accounts;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	DataSource dataSource;

	@Autowired
	MeterRegistry meters;

	@Autowired
	ArmedAccounts armedAccounts;

	private ConcurrentRequestHarness harness;

	private final HttpClient client = HttpClient.newHttpClient();

	private RedisClient rawClient;

	private StatefulRedisConnection<String, String> rawConnection;

	@BeforeEach
	void setUp() {
		RedisTestSupport.awaitGatewayReady(this.gateway);
		this.harness = new ConcurrentRequestHarness(this.port);
		this.rawClient = RedisClient.create(RedisTestcontainersConfiguration.patientUri(this.redisTestContainer));
		this.rawClient.setOptions(RedisGateway.clientOptions());
		this.rawConnection = this.rawClient.connect();
	}

	@AfterEach
	void tearDown() {
		this.armedAccounts.disarm();
		this.rawConnection.close();
		this.rawClient.shutdown();
	}

	// (i) §4.2 through the controller: MGET (miss, token T1) -> DB read, during which a transfer commits and
	// invalidates (T2) -> the compare-and-set with T1 writes nothing. (d) drives BalanceCache directly with a token
	// it picks, so only this case sees the controller's order (T10's 2b couldn't either).
	@Test
	void theControllerNotesTheTokenBeforeItsDbRead() throws Exception {
		long a = funded("100.00");
		long b = this.harness.createAccount("CUSTOMER");
		assertThat(rawValue(a)).as("A starts uncached: the GET takes the miss path").isNull();
		this.armedAccounts.arm(a, () -> transfer(a, b, "30.00"));
		double stale = populates("stale_token");
		double written = populates("written");

		HttpResponse<byte[]> during = get(a);

		Response armed = this.armedAccounts.transferResponse();
		assertThat(armed).as("the wrapper fired and ran the transfer during the DB read").isNotNull();
		assertThat(armed.status()).as("the armed transfer: %s", armed.exception()).isEqualTo(201);
		assertThat(xCache(during)).isEqualTo("miss");
		assertThat(balanceOf(during)).as("the pre-transfer balance the wrapper returned").isEqualTo("100.00");

		// Soft: a broken order fails several of these at once, and each failure is reported.
		double staleDelta = populates("stale_token") - stale;
		double writtenDelta = populates("written") - written;
		String value = rawValue(a);
		HttpResponse<byte[]> next = get(a);
		SoftAssertions soft = new SoftAssertions();
		soft.assertThat(staleDelta).as("populate{stale_token}").isEqualTo(1);
		soft.assertThat(writtenDelta).as("populate{written}").isZero();
		soft.assertThat(value).as("no value written").isNull();
		soft.assertThat(xCache(next)).as("the next read's X-Cache").isEqualTo("miss");
		soft.assertThat(balanceOf(next)).as("the next read's balance").isEqualTo("70.00");
		soft.assertThat(balanceOf(next)).as("the next read = DB")
			.isEqualTo(money(this.accounts.balance(a).orElseThrow().balance()));
		soft.assertAll();
	}

	// (a)
	@Test
	void aMissThenAByteIdenticalHit() throws Exception {
		long a = funded("100.00");

		HttpResponse<byte[]> miss = get(a);
		assertThat(miss.statusCode()).isEqualTo(200);
		assertThat(xCache(miss)).isEqualTo("miss");
		HttpResponse<byte[]> hit = get(a);
		assertThat(hit.statusCode()).isEqualTo(200);
		assertThat(xCache(hit)).isEqualTo("hit");
		assertThat(hit.headers().firstValue("Content-Type")).isEqualTo(miss.headers().firstValue("Content-Type"));
		assertThat(hit.body()).as("byte-identical, as_of included: %s", text(miss)).isEqualTo(miss.body());
		assertThat(balanceOf(hit)).isEqualTo("100.00");
	}

	// (b)
	@Test
	void theWritersNextReadShowsItsPostingAndIsAMiss() throws Exception {
		long a = funded("100.00");
		long b = this.harness.createAccount("CUSTOMER");
		get(a);
		assertThat(xCache(get(a))).as("cached").isEqualTo("hit");

		double sent = invalidations("sent");
		this.harness.deposit(a, "5.00");
		assertThat(invalidations("sent") - sent).as("invalidation{sent} for the deposit").isEqualTo(1);
		HttpResponse<byte[]> afterDeposit = get(a);
		assertThat(xCache(afterDeposit)).as("after the deposit").isEqualTo("miss");
		assertThat(balanceOf(afterDeposit)).isEqualTo("105.00");
		assertThat(xCache(get(a))).as("cached again").isEqualTo("hit");

		Response transfer = transfer(a, b, "30.00");
		assertThat(transfer.status()).isEqualTo(201);
		assertThat(invalidations("sent") - sent).as("invalidation{sent} for the transfer").isEqualTo(2);
		HttpResponse<byte[]> afterTransfer = get(a);
		assertThat(xCache(afterTransfer)).as("after the transfer").isEqualTo("miss");
		assertThat(balanceOf(afterTransfer)).isEqualTo("75.00");
	}

	// (c)
	@Test
	void anAuthorizeInvalidatesTheCustomerAndEscrow() throws Exception {
		long customer = funded("100.00");
		long merchant = this.harness.createAccount("MERCHANT");
		long escrow = escrowId();
		for (long id : new long[] { customer, escrow }) {
			get(id);
			assertThat(xCache(get(id))).as("account %d cached", id).isEqualTo("hit");
		}
		BigDecimal escrowBefore = this.accounts.balance(escrow).orElseThrow().balance();

		double sent = invalidations("sent");
		Response authorize = this.harness.send(this.harness.post("/v1/payments", ConcurrentRequestHarness.newKey(),
				("{\"customer_account_id\": " + customer + ", \"merchant_account_id\": " + merchant
						+ ", \"amount\": \"10.00\"}")
					.getBytes(StandardCharsets.UTF_8)));
		assertThat(authorize.status()).as("authorize: %s", new String(authorize.body(), StandardCharsets.UTF_8))
			.isEqualTo(201);
		assertThat(invalidations("sent") - sent).isEqualTo(1);

		HttpResponse<byte[]> customerAfter = get(customer);
		assertThat(xCache(customerAfter)).as("customer after the authorize").isEqualTo("miss");
		assertThat(balanceOf(customerAfter)).isEqualTo("90.00");
		HttpResponse<byte[]> escrowAfter = get(escrow);
		assertThat(xCache(escrowAfter)).as("ESCROW after the authorize").isEqualTo("miss");
		assertThat(balanceOf(escrowAfter)).isEqualTo(money(escrowBefore.add(new BigDecimal("10.00"))));
	}

	// (d) §4.2 step by step: MGET (miss, token T1) -> DB read -> the writer commits and invalidates
	// (token T2) -> the compare-and-set with T1 writes nothing.
	@Test
	void thePopulateRaceWritesNothingAfterAnInvalidation() throws Exception {
		long a = funded("100.00");
		long b = this.harness.createAccount("CUSTOMER");

		BalanceCache.Lookup lookup = this.cache.read(a);
		assertThat(lookup.result()).isEqualTo(BalanceCache.ReadResult.MISS);
		assertThat(lookup.token()).as("the deposit's invalidation set a token").isNotEmpty();
		AccountRepository.Balance old = this.accounts.balance(a).orElseThrow();
		assertThat(old.balance()).isEqualByComparingTo("100.00");

		assertThat(transfer(a, b, "30.00").status()).isEqualTo(201);

		double stale = populates("stale_token");
		double written = populates("written");
		this.cache.populate(a, lookup.token(), old.balance(), old.asOf());
		assertThat(populates("stale_token") - stale).as("populate{stale_token}").isEqualTo(1);
		assertThat(populates("written") - written).as("populate{written}").isZero();
		assertThat(rawValue(a)).as("no value written").isNull();

		HttpResponse<byte[]> next = get(a);
		assertThat(xCache(next)).isEqualTo("miss");
		assertThat(balanceOf(next)).isEqualTo("70.00");
	}

	// (e)
	@Test
	void aPlantedValueNeverReachesTheFundsCheck() throws Exception {
		long sink = this.harness.createAccount("CUSTOMER");

		long a = funded("10.00");
		plant(a, "1000000.0000");
		Response refused = transfer(a, sink, "50.00");
		assertThat(refused.status()).as("the funds check read the locked row, not the planted 1000000").isEqualTo(422);
		assertThat((String) JsonPath.read(new String(refused.body(), StandardCharsets.UTF_8), "$.code"))
			.isEqualTo(ProblemCode.INSUFFICIENT_FUNDS.value());
		HttpResponse<byte[]> served = get(a);
		assertThat(xCache(served))
			.as("the 422 path posted nothing, so nothing invalidated the planted value: the cache served it")
			.isEqualTo("hit");
		assertThat(balanceOf(served)).isEqualTo("1000000.00");

		long b = funded("10.00");
		plant(b, "0.0000");
		assertThat(transfer(b, sink, "5.00").status()).as("the funds check ignored the planted 0").isEqualTo(201);
		HttpResponse<byte[]> after = get(b);
		assertThat(xCache(after)).isEqualTo("miss");
		assertThat(balanceOf(after)).isEqualTo("5.00");
	}

	// (f)
	@Test
	void a404IsNotCached() throws Exception {
		long max = this.jdbc.sql("SELECT coalesce(max(id), 0) FROM accounts").query(Long.class).single();
		long target = max + 3;

		HttpResponse<byte[]> notFound = get(target);
		assertThat(notFound.statusCode()).isEqualTo(404);
		assertThat(notFound.headers().firstValue("X-Cache")).as("no X-Cache on a 404").isEmpty();
		assertThat(rawValue(target)).as("no value for a 404").isNull();

		long created;
		do {
			created = this.harness.createAccount("CUSTOMER");
		}
		while (created < target);
		assertThat(created).as("the created ids reach the 404's id exactly (this class's DB has no rolled-back "
				+ "account inserts, so the identity has no gaps)")
			.isEqualTo(target);

		HttpResponse<byte[]> found = get(target);
		assertThat(found.statusCode()).isEqualTo(200);
		assertThat(xCache(found)).isEqualTo("miss");
		assertThat(balanceOf(found)).isEqualTo("0.00");
	}

	// (g) D-056 on both paths.
	@Test
	void aSubCentBalanceIsA500OnTheMissPathAndIsNotCached() throws Exception {
		long c = this.harness.createAccount("CUSTOMER");
		LedgerTestSupport.directDeposit(this.dataSource, c, "0.0050");
		double misses = reads("miss");
		double written = populates("written");

		HttpResponse<byte[]> response = get(c);
		assertThat(response.statusCode()).isEqualTo(500);
		assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(
				(type) -> assertThat(type).startsWith("application/problem+json"));
		assertThat(text(response)).as("500 body").doesNotContain("0.0050", "0.01", "0.00");
		assertThat(reads("miss") - misses).isEqualTo(1);
		assertThat(populates("written")).as("a sub-cent balance is never cached").isEqualTo(written);
		assertThat(rawValue(c)).isNull();
	}

	@Test
	void aSubCentCachedValueIsA500OnTheHitPath() throws Exception {
		long d = this.harness.createAccount("CUSTOMER");
		plant(d, "1.2345");
		double hits = reads("hit");

		HttpResponse<byte[]> response = get(d);
		assertThat(reads("hit") - hits).as("read{hit}").isEqualTo(1);
		assertThat(response.statusCode()).as("never a rounded number: %s", text(response)).isEqualTo(500);
		assertThat(text(response)).doesNotContain("1.2345", "1.23");
	}

	// (h) Correctness under real interleavings; no timing assertions. stale_token is report-only: it
	// shows the race was exercised.
	@Test
	void aStormEndsWithEveryCachedValueEqualToTheDb() throws Exception {
		int writes = Integer.getInteger("ledger.test.balance.storm.writes", 200);
		List<Long> ids = new ArrayList<>();
		for (int i = 0; i < STORM_ACCOUNTS; i++) {
			ids.add(funded("1000.00"));
		}
		BigDecimal sumBefore = dbSum(ids);
		Map<String, Double> readsBefore = readCounts();
		Map<String, Double> populatesBefore = populateCounts();

		CountDownLatch start = new CountDownLatch(1);
		AtomicBoolean stop = new AtomicBoolean();
		AtomicInteger attempts = new AtomicInteger();
		AtomicLong created = new AtomicLong();
		AtomicLong refused = new AtomicLong();
		AtomicLong readerGets = new AtomicLong();
		List<String> errors = Collections.synchronizedList(new ArrayList<>());
		ExecutorService pool = Executors.newFixedThreadPool(STORM_READERS + STORM_WRITERS);
		long started;
		try {
			List<Future<?>> writers = new ArrayList<>();
			for (int w = 0; w < STORM_WRITERS; w++) {
				writers.add(pool.submit(() -> {
					await(start);
					ThreadLocalRandom random = ThreadLocalRandom.current();
					while (attempts.getAndIncrement() < writes) {
						int from = random.nextInt(STORM_ACCOUNTS);
						int to = (from + 1 + random.nextInt(STORM_ACCOUNTS - 1)) % STORM_ACCOUNTS;
						String amount = BigDecimal.valueOf(100 + random.nextInt(401), 2).toPlainString();
						Response response = transfer(ids.get(from), ids.get(to), amount);
						if (response.status() == 201) {
							created.incrementAndGet();
						}
						else if (response.status() == 422) {
							refused.incrementAndGet();
						}
						else {
							errors.add("transfer " + response.status() + " " + response.exception());
						}
					}
				}));
			}
			List<Future<?>> readers = new ArrayList<>();
			for (int r = 0; r < STORM_READERS; r++) {
				readers.add(pool.submit(() -> {
					await(start);
					while (!stop.get()) {
						for (long id : ids) {
							try {
								HttpResponse<byte[]> response = get(id);
								readerGets.incrementAndGet();
								if (response.statusCode() != 200) {
									errors.add("read " + response.statusCode());
								}
							}
							catch (Exception ex) {
								errors.add("read " + ex.getClass().getSimpleName());
							}
						}
					}
				}));
			}
			started = System.nanoTime();
			start.countDown();
			for (Future<?> writer : writers) {
				writer.get(5, TimeUnit.MINUTES);
			}
			// Stop and join every reader before the final check, so no reader's compare-and-set lands
			// after it.
			stop.set(true);
			for (Future<?> reader : readers) {
				reader.get(1, TimeUnit.MINUTES);
			}
		}
		finally {
			pool.shutdownNow();
		}
		long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
		assertThat(errors).as("writer and reader errors").isEmpty();

		RedisTestSupport.awaitGatewayReady(this.gateway);
		Map<String, Double> populatesBeforeFinal = populateCounts();
		List<String> finalXCache = new ArrayList<>();
		Map<Long, BigDecimal> db = new TreeMap<>();
		for (long id : ids) {
			HttpResponse<byte[]> response = get(id);
			finalXCache.add(xCache(response));
			BigDecimal balance = this.accounts.balance(id).orElseThrow().balance();
			db.put(id, balance);
			assertThat(response.statusCode()).isEqualTo(200);
			assertThat(balanceOf(response)).as("account %d served = DB", id).isEqualTo(money(balance));
		}
		int present = 0;
		for (long id : ids) {
			String value = rawValue(id);
			if (value == null) {
				// The final GET's populate is its own optional() call and can time out (D-135).
				continue;
			}
			present++;
			assertThat(value.split("\\|")[1]).as("account %d cached balance = DB", id)
				.isEqualTo(db.get(id).setScale(BalanceCache.VALUE_SCALE, RoundingMode.UNNECESSARY).toPlainString());
		}
		assertThat(dbSum(ids)).as("sum of balances").isEqualByComparingTo(sumBefore);

		Map<String, Double> readsAfter = readCounts();
		Map<String, Double> populatesAfter = populateCounts();
		System.out.println("STORM balance writes=" + writes + " transfers201=" + created + " transfers422=" + refused
				+ " readerGets=" + readerGets + " elapsedMs=" + elapsedMillis + " processors="
				+ ConcurrentRequestHarness.processors() + " reads=" + delta(readsBefore, readsAfter) + " populates="
				+ delta(populatesBefore, populatesAfter) + " finalXCache=" + finalXCache + " finalPopulates="
				+ delta(populatesBeforeFinal, populatesAfter) + " presentAfter=" + present + "/" + STORM_ACCOUNTS);
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await();
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(ex);
		}
	}

	private long funded(String amount) {
		long account = this.harness.createAccount("CUSTOMER");
		this.harness.deposit(account, amount);
		return account;
	}

	private Response transfer(long from, long to, String amount) {
		return this.harness.send(this.harness.post("/v1/transfers", ConcurrentRequestHarness.newKey(),
				("{\"from_account_id\": " + from + ", \"to_account_id\": " + to + ", \"amount\": \"" + amount + "\"}")
					.getBytes(StandardCharsets.UTF_8)));
	}

	private HttpResponse<byte[]> get(long id) throws Exception {
		HttpRequest request = HttpRequest
			.newBuilder(URI.create("http://localhost:" + this.port + "/v1/accounts/" + id + "/balance"))
			.timeout(Duration.ofSeconds(10))
			.header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY)
			.GET()
			.build();
		return this.client.send(request, HttpResponse.BodyHandlers.ofByteArray());
	}

	private static String xCache(HttpResponse<byte[]> response) {
		return response.headers().firstValue("X-Cache").orElse(null);
	}

	private static String text(HttpResponse<byte[]> response) {
		return new String(response.body(), StandardCharsets.UTF_8);
	}

	private static String balanceOf(HttpResponse<byte[]> response) {
		return JsonPath.read(text(response), "$.balance");
	}

	private static String money(BigDecimal amount) {
		return amount.setScale(2, RoundingMode.UNNECESSARY).toPlainString();
	}

	private long escrowId() {
		return this.jdbc.sql("SELECT id FROM accounts WHERE name = 'system:escrow'").query(Long.class).single();
	}

	private BigDecimal dbSum(List<Long> ids) {
		BigDecimal sum = BigDecimal.ZERO;
		for (long id : ids) {
			sum = sum.add(this.accounts.balance(id).orElseThrow().balance());
		}
		return sum;
	}

	// Raw Redis, never the gateway.
	private void plant(long id, String balance4) {
		RedisCommands<String, String> raw = this.rawConnection.sync();
		assertThat(raw.set(BalanceCacheScripts.valueKey(id), "1|" + balance4 + "|" + PLANTED_AS_OF,
				SetArgs.Builder.px(60_000)))
			.isEqualTo("OK");
	}

	private String rawValue(long id) {
		return this.rawConnection.sync().get(BalanceCacheScripts.valueKey(id));
	}

	private double reads(String result) {
		return this.meters.counter("ledger.balance.cache.read", "result", result).count();
	}

	private double populates(String result) {
		return this.meters.counter("ledger.balance.cache.populate", "result", result).count();
	}

	private double invalidations(String result) {
		return this.meters.counter("ledger.balance.cache.invalidation", "result", result).count();
	}

	private Map<String, Double> readCounts() {
		Map<String, Double> counts = new TreeMap<>();
		for (String result : List.of("hit", "miss", "bypass", "unparseable")) {
			counts.put(result, reads(result));
		}
		return counts;
	}

	private Map<String, Double> populateCounts() {
		Map<String, Double> counts = new TreeMap<>();
		for (String result : List.of("written", "stale_token", "unavailable")) {
			counts.put(result, populates(result));
		}
		return counts;
	}

	private static Map<String, Long> delta(Map<String, Double> before, Map<String, Double> after) {
		Map<String, Long> d = new TreeMap<>();
		after.forEach((name, value) -> d.put(name, Math.round(value - before.get(name))));
		return d;
	}

	// (i)'s wrapper. For the one armed account, once: read the balance, run the armed transfer on another thread and
	// wait for its response (bounded), then return the read from BEFORE the transfer. Every other call, and every
	// call once disarmed, is the real read. The id is matched by value (AtomicLong): an AtomicReference<Long> would
	// compare references, and a Long above 127 boxed separately never matches (T4's ArmableHook slip). No deadlock:
	// the balance read runs outside any transaction, so the transfer's account locks never wait on it. Not final:
	// AccountRepository is a @Repository, so Spring proxies it with a CGLIB subclass (exception translation).
	static class ArmedAccounts extends AccountRepository {

		private static final long DISARMED = -1;

		private static final Duration TRANSFER_BOUND = Duration.ofSeconds(30);

		private final AtomicLong armed = new AtomicLong(DISARMED);

		private final AtomicReference<Supplier<Response>> transfer = new AtomicReference<>();

		private final AtomicReference<Response> response = new AtomicReference<>();

		ArmedAccounts(JdbcClient jdbc) {
			super(jdbc);
		}

		void arm(long id, Supplier<Response> onRead) {
			this.response.set(null);
			this.transfer.set(onRead);
			this.armed.set(id);
		}

		void disarm() {
			this.armed.set(DISARMED);
			this.transfer.set(null);
		}

		/** The armed transfer's response, or null if the wrapper never fired. */
		Response transferResponse() {
			return this.response.get();
		}

		@Override
		Optional<Balance> balance(long id) {
			if (!this.armed.compareAndSet(id, DISARMED)) {
				return super.balance(id);
			}
			Optional<Balance> before = super.balance(id);
			Supplier<Response> run = this.transfer.getAndSet(null);
			ExecutorService other = Executors.newSingleThreadExecutor();
			try {
				this.response.set(other.submit(run::get).get(TRANSFER_BOUND.toMillis(), TimeUnit.MILLISECONDS));
			}
			catch (Exception ex) {
				// Recorded, never thrown: the test asserts the 201, so a failed transfer is a clear assertion.
				this.response.set(new Response(-1, null, null, ex, 0, 0));
				if (ex instanceof InterruptedException) {
					Thread.currentThread().interrupt();
				}
			}
			finally {
				other.shutdownNow();
			}
			return before;
		}

	}

	@TestConfiguration(proxyBeanMethods = false)
	static class Wrapper {

		@Bean
		@Primary
		ArmedAccounts armedAccounts(JdbcClient jdbc) {
			return new ArmedAccounts(jdbc);
		}

	}

}
