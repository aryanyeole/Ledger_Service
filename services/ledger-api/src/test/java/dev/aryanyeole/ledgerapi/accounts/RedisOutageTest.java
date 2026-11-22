package dev.aryanyeole.ledgerapi.accounts;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.jayway.jsonpath.JsonPath;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.MeterRegistry;

import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Response;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Run;
import dev.aryanyeole.ledgerapi.LedgerTestSupport;
import dev.aryanyeole.ledgerapi.RedisTestSupport;
import dev.aryanyeole.ledgerapi.RedisTestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.ServerArrivals;
import dev.aryanyeole.ledgerapi.TestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.cache.BalanceCacheScripts;
import dev.aryanyeole.ledgerapi.cache.RedisGateway;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyFaultHook;

// Redis can make a request faster, never different (D-135, D-137, D-142; docs/design/cache-invalidation.md §1.4,
// §1.5, §5 rows 1, 2, 2a, 2b, 3, 6, 13). With both caches on, three outage modes: stopped (connection refused),
// paused (commands hang to the 100 ms timeout) and flushed (FLUSHALL mid-run). In each, 100 identical transfers on
// one key give one transaction and 100 byte-identical 201s; a mismatched replay is 422 idempotency_key_reused from
// the DB; a stored 422 replays byte-identically; an overdraft is refused; balances equal the DB. Tripwires show
// the outage was real (bypasses and no hits, or a flushed hit turned miss), and recovery is checked: a key cached
// before the outage replays byte-identically, and every present cached balance equals the DB. Two deterministic
// paused cases: 2a, a SET NX delivered late is still the DB's bytes, its TTL counted from execution; 2b, a late
// populate queued before a posting's invalidation on the same connection can't leave a stale value.
// Its own Redis, bound to a fixed loopback port so a stop/start keeps ledger.redis.url valid (RedisDownStartupTest's
// pattern); pause/unpause and stop/start through Testcontainers' Docker client, never a restart (D-129). After a
// stop it comes back empty: D-138's flags (--save "" --appendonly no) keep nothing on disk.
@Import({ TestcontainersConfiguration.class, ServerArrivals.Configuration.class, RedisOutageTest.OutageRedis.class })
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
		properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.cache.idempotency.enabled=true",
				"ledger.cache.balance.enabled=true" })
class RedisOutageTest {

	private static final int REDIS_PORT = freePort();

	private static final String TRANSFERS = "/v1/transfers";

	// Lettuce's default reconnect delay doubles from 1 ms to a 30 s cap (finding, PR body); 45 s covers it.
	private static final Duration STOPPED_RECOVERY_BOUND = Duration.ofSeconds(45);

	private static final Duration PAUSED_REQUEST_BOUND = Duration.ofSeconds(2);

	private static final long IDEMPOTENCY_TTL_MILLIS = Duration.ofMinutes(10).toMillis();

	@DynamicPropertySource
	static void redisUrl(DynamicPropertyRegistry registry) {
		registry.add("ledger.redis.url", () -> "redis://127.0.0.1:" + REDIS_PORT);
	}

	@LocalServerPort
	int port;

	@Autowired
	RedisGateway gateway;

	@Autowired
	GenericContainer<?> outageRedis;

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
	ServerArrivals arrivals;

	@Autowired
	PausingHook hook;

	private ConcurrentRequestHarness harness;

	private final HttpClient client = HttpClient.newHttpClient();

	private final Set<Integer> seenPorts = new HashSet<>();

	@BeforeEach
	void setUp() {
		ensureRunning();
		RedisTestSupport.awaitGatewayReady(this.gateway);
		this.harness = new ConcurrentRequestHarness(this.port);
	}

	// No assertion failure may leave the container paused or stopped for the next case.
	@AfterEach
	void tearDown() {
		this.hook.disarm();
		ensureRunning();
	}

	@Test
	void stopped() throws Exception {
		Prepared before = prepare();
		Map<String, Double> start = counters();
		long downAt = System.nanoTime();
		docker().stopContainerCmd(containerId()).withTimeout(0).exec();
		List<Long> latencies = new ArrayList<>();
		try {
			window(before, latencies, null);
			assertOutageTripwires("stopped", start, counters());
		}
		finally {
			docker().startContainerCmd(containerId()).exec();
		}
		long recoveryMillis = awaitRecovered(STOPPED_RECOVERY_BOUND);
		System.out.printf("REDIS_OUTAGE stopped windowMs=%d timeToRecoverMs=%d dbsizeAfterStart=%d%n",
				(System.nanoTime() - downAt) / 1_000_000, recoveryMillis, raw(RedisCommands::dbsize));
		recover(before, "stopped", false);
	}

	@Test
	void paused() throws Exception {
		Prepared before = prepare();
		Map<String, Double> start = counters();
		docker().pauseContainerCmd(containerId()).exec();
		List<Long> latencies = new ArrayList<>();
		try {
			window(before, latencies, null);
			assertOutageTripwires("paused", start, counters());
		}
		finally {
			docker().unpauseContainerCmd(containerId()).exec();
		}
		List<Long> sorted = latencies.stream().sorted().toList();
		System.out.printf("REDIS_OUTAGE paused requests=%d latencyMs p50=%d p95=%d max=%d%n", sorted.size(),
				sorted.get(sorted.size() / 2), sorted.get((int) Math.ceil(sorted.size() * 0.95) - 1),
				sorted.get(sorted.size() - 1));
		assertThat(sorted.get(sorted.size() - 1)).as("every request under %s while paused", PAUSED_REQUEST_BOUND)
			.isLessThan(PAUSED_REQUEST_BOUND.toMillis());
		RedisTestSupport.awaitGatewayReady(this.gateway);
		recover(before, "paused", true);
	}

	@Test
	void flushed() throws Exception {
		Prepared before = prepare();
		raw(RedisCommands::flushall);
		long keysAfterFlush = raw(RedisCommands::dbsize);
		assertThat(keysAfterFlush).as("DBSIZE right after FLUSHALL").isZero();
		double misses = this.meters.counter("ledger.balance.cache.read", "result", "miss").count();
		HttpResponse<byte[]> afterFlush = get(before.a());
		assertThat(xCache(afterFlush)).as("a balance that was a hit just before the flush").isEqualTo("miss");
		assertThat(this.meters.counter("ledger.balance.cache.read", "result", "miss").count() - misses).isEqualTo(1);

		List<Long> latencies = new ArrayList<>();
		window(before, latencies, () -> raw(RedisCommands::flushall));
		recover(before, "flushed", false);
	}

	// §5 row 2a: the owner's SET NX reaches a paused server, times out client-side and executes on resume. The
	// owner is a keyed account creation, which posts nothing: on a posting route the post-commit balance
	// invalidation goes first, times out on the paused server and opens the bypass, so the populate after it is
	// never sent at all (finding, PR body).
	@Test
	void aLateSetNxIsStillTheDbsBytesWithItsTtlFromExecution() throws Exception {
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = ("{\"name\": \"outage-2a-" + key + "\", \"type\": \"CUSTOMER\"}").getBytes(StandardCharsets.UTF_8);
		// The pause happens inside the owner's transaction, after the fast-path GET (a miss) and the store: a
		// test-side Docker call, not an app Redis call (invariant 5 untouched). Before the request, the GET would
		// time out and open the bypass, and the populate would never be sent.
		this.hook.arm(key, () -> docker().pauseContainerCmd(containerId()).exec());
		Response owner;
		try {
			owner = this.harness.send(this.harness.post("/v1/accounts", key, body));
			assertThat(owner.status()).isEqualTo(201);
			assertThat(this.hook.fired()).as("the hook paused Redis").isTrue();
		}
		finally {
			unpauseIfPaused();
		}
		String entryKey = idempotencyKey(key);
		String value = awaitValue(entryKey, Duration.ofSeconds(10));
		assertThat(value).as("the late SET NX executed on resume").isNotNull();
		String text = this.jdbc
			.sql("SELECT response_body::text FROM idempotency_keys WHERE client_id = :client AND key = :key")
			.param("client", EdgeProbe.CLIENT_ID)
			.param("key", key)
			.query(String.class)
			.single();
		assertThat(value.split("\\|", 4)[3]).as("cached text = response_body::text").isEqualTo(text);
		assertThat(new String(owner.body(), StandardCharsets.UTF_8)).isEqualTo(text);
		long pttl = raw((r) -> r.pttl(entryKey));
		System.out.printf("REDIS_OUTAGE 2a pttlMs=%d ttlMs=%d%n", pttl, IDEMPOTENCY_TTL_MILLIS);
		assertThat(pttl).as("the TTL started when Redis executed the SET, on resume")
			.isGreaterThan(IDEMPOTENCY_TTL_MILLIS - 5_000);
	}

	// §5 row 2b: a populate queued on the paused connection, then the posting's invalidation behind it (always()
	// sends it despite the bypass the populate's timeout opened). On resume both execute in that order.
	@Test
	void aLatePopulateBeforeAnInvalidationOnOneConnectionLeavesNoStaleValue() throws Exception {
		long a = funded("100.00");
		long b = this.harness.createAccount("CUSTOMER");
		BalanceCache.Lookup lookup = this.cache.read(a);
		assertThat(lookup.result()).isEqualTo(BalanceCache.ReadResult.MISS);
		AccountRepository.Balance old = this.accounts.balance(a).orElseThrow();

		docker().pauseContainerCmd(containerId()).exec();
		try {
			this.cache.populate(a, lookup.token(), old.balance(), old.asOf());
			Response posted = send(ConcurrentRequestHarness.newKey(), transfer(a, b, "30.00"));
			assertThat(posted.status()).isEqualTo(201);
		}
		finally {
			docker().unpauseContainerCmd(containerId()).exec();
		}
		// Same connection, same order: this read executes after the queued populate and invalidation.
		RedisTestSupport.awaitGatewayReady(this.gateway);

		HttpResponse<byte[]> next = get(a);
		BigDecimal db = this.accounts.balance(a).orElseThrow().balance();
		assertThat(xCache(next)).as("the late populate's old value didn't survive").isEqualTo("miss");
		assertThat(balanceOf(next)).isEqualTo(money(db));
		String value = raw((r) -> r.get(BalanceCacheScripts.valueKey(a)));
		assertThat(value == null || value.split("\\|")[1].equals(scale4(db)))
			.as("raw value absent or the DB's: %s (DB %s)", value, db)
			.isTrue();
	}

	// --- the outage window, the same for every mode ---

	record Prepared(long a, long b, long c, long d, long poor, String k0, byte[] k0Body, Response k0Owner, String k422,
			byte[] k422Body, Response k422Owner) {

		List<Long> touched() {
			return List.of(this.a, this.b, this.c, this.d, this.poor);
		}

	}

	private Prepared prepare() throws Exception {
		long a = funded("10000.00");
		long b = this.harness.createAccount("CUSTOMER");
		long c = funded("10000.00");
		long d = this.harness.createAccount("CUSTOMER");
		long poor = this.harness.createAccount("CUSTOMER");
		String k0 = ConcurrentRequestHarness.newKey();
		byte[] k0Body = transfer(a, b, "1.00");
		Response k0Owner = send(k0, k0Body);
		assertThat(k0Owner.status()).isEqualTo(201);
		String k422 = ConcurrentRequestHarness.newKey();
		byte[] k422Body = transfer(poor, b, "5.00");
		Response k422Owner = send(k422, k422Body);
		assertThat(k422Owner.status()).isEqualTo(422);
		String k0Entry = raw((r) -> r.get(idempotencyKey(k0)));
		String k422Entry = raw((r) -> r.get(idempotencyKey(k422)));
		assertThat(k0Entry).as("K0 cached before the outage").isNotNull();
		assertThat(k422Entry).as("K422 cached before the outage").isNotNull();
		for (long id : List.of(a, b)) {
			get(id);
			assertThat(xCache(get(id))).as("account %d cached before the outage", id).isEqualTo("hit");
		}
		return new Prepared(a, b, c, d, poor, k0, k0Body, k0Owner, k422, k422Body, k422Owner);
	}

	// midBurst, if given, runs once the burst's owner has committed (the flushed mode's second FLUSHALL).
	private void window(Prepared p, List<Long> latencies, Runnable midBurst) throws Exception {
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = transfer(p.c(), p.d(), "2.00");
		ExecutorService side = Executors.newSingleThreadExecutor();
		Future<Boolean> flushed = null;
		try {
			if (midBurst != null) {
				flushed = side.submit(() -> {
					long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
					while (System.nanoTime() < deadline) {
						if (LedgerTestSupport.transactionsTouching(this.dataSource, p.c(), "TRANSFER") >= 1) {
							midBurst.run();
							return true;
						}
						Thread.sleep(5);
					}
					return false;
				});
			}
			Run run = this.harness.fire(() -> this.harness.post(TRANSFERS, key, body));
			ServerArrivals.Overlap overlap = this.arrivals.overlap(key, ConcurrentRequestHarness.THREADS, this.seenPorts);
			run.responses().forEach((r) -> latencies.add((r.completedNanos() - r.sentNanos()) / 1_000_000));
			if (flushed != null) {
				assertThat(flushed.get(30, TimeUnit.SECONDS)).as("the mid-burst FLUSHALL ran").isTrue();
			}
			System.out.printf("REDIS_OUTAGE burst overlap=%d requests=%d statuses=%s%n",
					overlap.arrivedBeforeFirstCompletion(), overlap.requests(), statuses(run));
			assertThat(run.responses().stream().filter((r) -> r.exception() != null).toList()).as("client exceptions")
				.isEmpty();
			assertThat(statuses(run)).containsExactly(Map.entry(201, (long) ConcurrentRequestHarness.THREADS));
			assertThat(LedgerTestSupport.transactionsTouching(this.dataSource, p.c(), "TRANSFER")).as("one transaction")
				.isEqualTo(1);
			assertThat(run.responses().stream().map((r) -> new String(r.body(), StandardCharsets.UTF_8)).distinct())
				.as("100 byte-identical bodies")
				.hasSize(1);
			assertThat(overlap.arrivedBeforeFirstCompletion()).as("server overlap (requests=%d)", overlap.requests())
				.isGreaterThanOrEqualTo(ConcurrentRequestHarness.UNCALIBRATED_SERVER_OVERLAP_FLOOR);
		}
		finally {
			side.shutdownNow();
		}

		Response mismatch = timed(latencies, () -> send(key, transfer(p.c(), p.d(), "3.00")));
		assertThat(mismatch.status()).isEqualTo(422);
		assertThat(code(mismatch)).isEqualTo(ProblemCode.IDEMPOTENCY_KEY_REUSED.value());

		Response stored = timed(latencies, () -> send(p.k422(), p.k422Body()));
		assertThat(stored.status()).isEqualTo(422);
		assertThat(stored.body()).as("the stored 422 replays byte-identically").isEqualTo(p.k422Owner().body());

		Response overdraft = timed(latencies,
				() -> send(ConcurrentRequestHarness.newKey(), transfer(p.a(), p.b(), "999999.00")));
		assertThat(overdraft.status()).isEqualTo(422);
		assertThat(code(overdraft)).isEqualTo(ProblemCode.INSUFFICIENT_FUNDS.value());

		for (long id : p.touched()) {
			long started = System.nanoTime();
			HttpResponse<byte[]> response = get(id);
			latencies.add((System.nanoTime() - started) / 1_000_000);
			assertThat(response.statusCode()).isEqualTo(200);
			assertThat(balanceOf(response)).as("account %d = DB", id)
				.isEqualTo(money(this.accounts.balance(id).orElseThrow().balance()));
		}
	}

	private void assertOutageTripwires(String mode, Map<String, Double> start, Map<String, Double> end) {
		Map<String, Long> delta = delta(start, end);
		System.out.printf("REDIS_OUTAGE %s window reads=%s%n", mode, delta);
		for (String cache : List.of("idempotency", "balance")) {
			assertThat(delta.get(cache + ".bypass")).as("%s %s read{bypass} in the window", mode, cache).isPositive();
			assertThat(delta.get(cache + ".hit")).as("%s %s read{hit} in the window", mode, cache).isZero();
		}
	}

	// K0 replays byte-identically (a hit if its entry survived, else a miss then a hit); every present cached
	// balance of an account this mode touched equals the DB.
	private void recover(Prepared p, String mode, boolean entrySurvived) {
		double hits = idempotencyReads("hit");
		double misses = idempotencyReads("miss");
		Response first = send(p.k0(), p.k0Body());
		assertThat(first.body()).as("%s: K0 byte-identical after recovery", mode).isEqualTo(p.k0Owner().body());
		if (entrySurvived) {
			assertThat(idempotencyReads("hit") - hits).as("%s: K0 is a hit", mode).isEqualTo(1);
		}
		else {
			assertThat(idempotencyReads("miss") - misses).as("%s: K0 is a miss (Redis lost it)", mode).isEqualTo(1);
			Response second = send(p.k0(), p.k0Body());
			assertThat(idempotencyReads("hit") - hits).as("%s: then a hit", mode).isEqualTo(1);
			assertThat(second.body()).isEqualTo(p.k0Owner().body());
		}
		int present = 0;
		for (long id : p.touched()) {
			String value = raw((r) -> r.get(BalanceCacheScripts.valueKey(id)));
			if (value == null) {
				continue;
			}
			present++;
			assertThat(value.split("\\|")[1]).as("%s: account %d cached = DB", mode, id)
				.isEqualTo(scale4(this.accounts.balance(id).orElseThrow().balance()));
		}
		System.out.printf("REDIS_OUTAGE %s recovery presentBalances=%d/%d%n", mode, present, p.touched().size());
	}

	// --- helpers ---

	private Map<String, Double> counters() {
		Map<String, Double> counts = new TreeMap<>();
		for (String result : List.of("hit", "miss", "bypass")) {
			counts.put("idempotency." + result, idempotencyReads(result));
			counts.put("balance." + result,
					this.meters.counter("ledger.balance.cache.read", "result", result).count());
		}
		return counts;
	}

	private double idempotencyReads(String result) {
		return this.meters.counter("ledger.idempotency.cache.read", "result", result).count();
	}

	private static Map<String, Long> delta(Map<String, Double> start, Map<String, Double> end) {
		Map<String, Long> d = new TreeMap<>();
		end.forEach((name, value) -> d.put(name, Math.round(value - start.get(name))));
		return d;
	}

	private static Map<Integer, Long> statuses(Run run) {
		return run.responses().stream().collect(Collectors.groupingBy(Response::status, TreeMap::new, Collectors.counting()));
	}

	private static Response timed(List<Long> latencies, java.util.function.Supplier<Response> request) {
		Response response = request.get();
		latencies.add((response.completedNanos() - response.sentNanos()) / 1_000_000);
		return response;
	}

	private static String code(Response response) {
		return JsonPath.read(new String(response.body(), StandardCharsets.UTF_8), "$.code");
	}

	private long funded(String amount) {
		long account = this.harness.createAccount("CUSTOMER");
		this.harness.deposit(account, amount);
		return account;
	}

	private Response send(String key, byte[] body) {
		return this.harness.send(this.harness.post(TRANSFERS, key, body));
	}

	private static byte[] transfer(long from, long to, String amount) {
		return ("{\"from_account_id\": " + from + ", \"to_account_id\": " + to + ", \"amount\": \"" + amount + "\"}")
			.getBytes(StandardCharsets.UTF_8);
	}

	private HttpResponse<byte[]> get(long id) throws IOException, InterruptedException {
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

	private static String balanceOf(HttpResponse<byte[]> response) {
		return JsonPath.read(new String(response.body(), StandardCharsets.UTF_8), "$.balance");
	}

	private static String money(BigDecimal amount) {
		return amount.setScale(2, RoundingMode.UNNECESSARY).toPlainString();
	}

	private static String scale4(BigDecimal amount) {
		return amount.setScale(4, RoundingMode.UNNECESSARY).toPlainString();
	}

	// The fast-path key format, pinned by IdempotencyCacheTest (IdempotencyCache.key is package-private there).
	private static String idempotencyKey(String key) {
		return "ledger:v1:idem:" + EdgeProbe.CLIENT_ID.getBytes(StandardCharsets.UTF_8).length + ":"
				+ EdgeProbe.CLIENT_ID + ":" + key;
	}

	// A patient raw client per call (a stop breaks connections), never the gateway (BUG-016).
	private static <T> T raw(Function<RedisCommands<String, String>, T> call) {
		RedisClient client = RedisClient.create(RedisURI.builder()
			.withHost("127.0.0.1")
			.withPort(REDIS_PORT)
			.withDatabase(0)
			.withTimeout(RedisTestcontainersConfiguration.PATIENT_TIMEOUT)
			.build());
		try (StatefulRedisConnection<String, String> connection = client.connect()) {
			return call.apply(connection.sync());
		}
		finally {
			client.shutdown();
		}
	}

	private String awaitValue(String key, Duration bound) throws InterruptedException {
		long deadline = System.nanoTime() + bound.toNanos();
		while (System.nanoTime() < deadline) {
			String value = raw((r) -> r.get(key));
			if (value != null) {
				return value;
			}
			Thread.sleep(50);
		}
		return null;
	}

	// Until the gateway answers again; returns how long that took. Stopped mode waits out Lettuce's reconnect delay.
	private long awaitRecovered(Duration bound) throws InterruptedException {
		long started = System.nanoTime();
		long deadline = started + bound.toNanos();
		while (System.nanoTime() < deadline) {
			if (this.gateway.optional("GET", (r) -> r.get("ledger:test:warmup")).available()) {
				return (System.nanoTime() - started) / 1_000_000;
			}
			Thread.sleep(100);
		}
		throw new AssertionError("the gateway didn't recover within " + bound.toSeconds() + " s");
	}

	private static DockerClient docker() {
		return DockerClientFactory.instance().client();
	}

	private String containerId() {
		return this.outageRedis.getContainerId();
	}

	private void unpauseIfPaused() {
		ContainerState state = docker().inspectContainerCmd(containerId()).exec().getState();
		if (Boolean.TRUE.equals(state.getPaused())) {
			docker().unpauseContainerCmd(containerId()).exec();
		}
	}

	private void ensureRunning() {
		ContainerState state = docker().inspectContainerCmd(containerId()).exec().getState();
		if (Boolean.TRUE.equals(state.getPaused())) {
			docker().unpauseContainerCmd(containerId()).exec();
		}
		else if (!Boolean.TRUE.equals(state.getRunning())) {
			docker().startContainerCmd(containerId()).exec();
		}
		long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
		while (true) {
			try {
				raw(RedisCommands::ping);
				return;
			}
			catch (RuntimeException ex) {
				if (System.nanoTime() > deadline) {
					throw ex;
				}
			}
		}
	}

	private static int freePort() {
		try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			return socket.getLocalPort();
		}
		catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

	// Runs its action once, for the armed key only, inside the owner's transaction (D-141's hook point).
	static final class PausingHook implements IdempotencyFaultHook {

		private final AtomicReference<String> armed = new AtomicReference<>();

		private final AtomicReference<Runnable> action = new AtomicReference<>();

		private volatile boolean fired;

		void arm(String key, Runnable onStore) {
			this.fired = false;
			this.action.set(onStore);
			this.armed.set(key);
		}

		void disarm() {
			this.armed.set(null);
			this.action.set(null);
		}

		boolean fired() {
			return this.fired;
		}

		@Override
		public void afterStoreBeforeCommit(String clientId, String key) {
			String target = this.armed.get();
			if (key.equals(target) && this.armed.compareAndSet(target, null)) {
				Runnable run = this.action.getAndSet(null);
				if (run != null) {
					run.run();
					this.fired = true;
				}
			}
		}

	}

	// redis:7.4.11 with D-138's flags on a fixed loopback port; empty at start (DBSIZE = 0).
	@TestConfiguration(proxyBeanMethods = false)
	static class OutageRedis {

		@Bean(destroyMethod = "stop")
		GenericContainer<?> outageRedis() {
			ExposedPort redisPort = ExposedPort.tcp(RedisTestcontainersConfiguration.PORT);
			GenericContainer<?> container = new GenericContainer<>(
					DockerImageName.parse(RedisTestcontainersConfiguration.IMAGE))
				.withCommand(RedisTestcontainersConfiguration.COMMAND)
				.withCreateContainerCmdModifier((cmd) -> {
					HostConfig hostConfig = (cmd.getHostConfig() != null) ? cmd.getHostConfig() : new HostConfig();
					cmd.withExposedPorts(redisPort)
						.withHostConfig(hostConfig.withPortBindings(
								new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", REDIS_PORT), redisPort)));
				})
				.waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1));
			container.start();
			long keys = raw(RedisCommands::dbsize);
			if (keys != 0) {
				throw new IllegalStateException("test Redis must start empty (D-138), DBSIZE = " + keys);
			}
			return container;
		}

		@Bean
		@Primary
		PausingHook pausingHook() {
			return new PausingHook();
		}

	}

}
