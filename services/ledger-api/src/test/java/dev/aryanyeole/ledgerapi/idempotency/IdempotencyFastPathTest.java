package dev.aryanyeole.ledgerapi.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.math.BigDecimal;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import io.micrometer.core.instrument.MeterRegistry;

import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Response;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Run;
import dev.aryanyeole.ledgerapi.LedgerTestSupport;
import dev.aryanyeole.ledgerapi.RedisTestSupport;
import dev.aryanyeole.ledgerapi.RedisTestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.ServerArrivals;
import dev.aryanyeole.ledgerapi.TestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.cache.RedisGateway;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyCache.ReadResult;
import dev.aryanyeole.ledgerapi.money.Money;

// Proof of the idempotency fast path (D-139, docs/design/cache-invalidation.md §1.1, §3.1-3.4):
// Redis may only replay a completed response's exact bytes, and only for a matching fingerprint.
// (a) the owner's entry is the DB's own text and the next request is a byte-identical hit; (b) two
// waves of 100 identical requests over real HTTP (D-067): one transaction, 200 identical bodies, no
// wave-2 misses once the entry is ensured, and a final sequential hit; (c) stored 422 and keyed 409 outcomes replay from Redis, and a
// keyed account creation's hit carries the owner's Location (D-063); (d) a mismatched fingerprint
// gets 422 from the DB and leaves the entry untouched; (e) an in-flight key is never cached, and the
// response its holder commits is cached by the next replay; (f) an owner rolled back after its store
// (D-141's hook) leaves no entry. Expiry and purge cases are IdempotencyFastPathExpiryTest (they need
// a 1 s TTL). Counters are compared as deltas on the context's registry; the gateway is warmed first
// (BUG-016); server overlap reads wait for every arrival record (BUG-015).
@Import({ TestcontainersConfiguration.class, RedisTestcontainersConfiguration.class, ServerArrivals.Configuration.class,
		IdempotencyFastPathTest.Hooks.class })
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
		properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.cache.idempotency.enabled=true" })
class IdempotencyFastPathTest {

	private static final String TRANSFERS = "/v1/transfers";

	private static final long BLOCK_TIMEOUT_MILLIS = 10_000;

	// Same components as TransfersController.TransferRequest, so the same canonical JSON and fingerprint
	// (IdempotencyInProgressTest's TransferShape, pinned by TransferRequestFingerprintTest).
	record TransferShape(Long fromAccountId, Long toAccountId, Money amount) {
	}

	@LocalServerPort
	int port;

	@Autowired
	RedisGateway gateway;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	DataSource dataSource;

	@Autowired
	MeterRegistry meters;

	@Autowired
	ServerArrivals arrivals;

	@Autowired
	ArmableHook hook;

	private ConcurrentRequestHarness harness;

	private final ExecutorService requests = Executors.newSingleThreadExecutor();

	@BeforeEach
	void setUp() {
		RedisTestSupport.awaitGatewayReady(this.gateway);
		this.harness = new ConcurrentRequestHarness(this.port);
	}

	@AfterEach
	void tearDown() {
		this.requests.shutdownNow();
	}

	// (a)
	@Test
	void ownersEntryIsTheDbTextAndTheNextRequestIsAByteIdenticalHit() throws Exception {
		long a = funded("100.00");
		long b = this.harness.createAccount("CUSTOMER");
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = transfer(a, b, "10.00");

		Response owner = send(key, body);
		assertThat(owner.status()).isEqualTo(201);
		String dbText = storedText(key);
		assertThat(new String(owner.body(), StandardCharsets.UTF_8)).as("owner body = response_body::text").isEqualTo(dbText);
		assertThat(entry(key).text()).as("cached text = response_body::text").isEqualTo(dbText);

		Map<ReadResult, Double> before = reads();
		Response replay = send(key, body);
		assertThat(delta(before, reads()).get(ReadResult.HIT)).as("read{hit}").isEqualTo(1);
		assertThat(replay.status()).isEqualTo(201);
		assertThat(replay.contentType()).isEqualTo("application/json");
		assertThat(replay.body()).as("byte-identical").isEqualTo(owner.body());
	}

	// (b) Per round: wave 1 goes through the DB (the owner populates as soon as its transaction
	// returns, so late arrivals may already hit); a sequential request ensures the entry; wave 2's
	// reads are hits or bypasses, never misses; a final sequential request is a hit. Correctness is
	// asserted; hit counts are printed only (D-067). No retries (D-133).
	@TestFactory
	Stream<DynamicTest> twoWavesOfIdenticalRequests() throws Exception {
		Set<Integer> seenPorts = new HashSet<>();
		long a = funded("1000.00");
		long b = this.harness.createAccount("CUSTOMER");
		String warmKey = ConcurrentRequestHarness.newKey();
		Run warm = this.harness.fire(() -> this.harness.post(TRANSFERS, warmKey, transfer(a, b, "10.00")));
		this.arrivals.overlap(warmKey, ConcurrentRequestHarness.THREADS, seenPorts);
		assertThat(clientExceptions(warm)).as("warm-up client exceptions").isEmpty();
		return IntStream.rangeClosed(1, ConcurrentRequestHarness.rounds())
			.mapToObj((round) -> dynamicTest("round " + round, () -> waves(round, seenPorts)));
	}

	private void waves(int round, Set<Integer> seenPorts) throws Exception {
		long a = funded("1000.00");
		long b = this.harness.createAccount("CUSTOMER");
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = transfer(a, b, "10.00");
		String context = "round " + round;

		Map<ReadResult, Double> beforeWave1 = reads();
		Run wave1 = this.harness.fire(() -> this.harness.post(TRANSFERS, key, body));
		// Both waves share the key: read wave 1's overlap before wave 2 adds records.
		ServerArrivals.Overlap server = this.arrivals.overlap(key, ConcurrentRequestHarness.THREADS, seenPorts);
		Map<ReadResult, Double> afterWave1 = reads();
		Map<ReadResult, Double> d1 = delta(beforeWave1, afterWave1);
		System.out.println("CONCURRENCY fastpath wave=1 round=" + round + " processors="
				+ ConcurrentRequestHarness.processors() + " serverArrivedBeforeFirstCompletion="
				+ server.arrivedBeforeFirstCompletion() + " requests=" + server.requests() + " transactions="
				+ transfersOn(a) + " distinctBodies=" + distinct(wave1).size() + " statuses=" + statuses(wave1)
				+ " readHit=" + d1.get(ReadResult.HIT) + " readMiss=" + d1.get(ReadResult.MISS) + " readBypass="
				+ d1.get(ReadResult.BYPASS));
		assertThat(clientExceptions(wave1)).as("%s wave 1 client exceptions", context).isEmpty();
		assertThat(statuses(wave1)).as("%s wave 1 statuses", context)
			.containsExactly(Map.entry(201, (long) ConcurrentRequestHarness.THREADS));
		assertThat(transfersOn(a)).as("%s wave 1 transactions", context).isEqualTo(1);
		assertThat(distinct(wave1)).as("%s wave 1 distinct bodies", context).hasSize(1);
		assertThat(server.arrivedBeforeFirstCompletion()).as("%s wave 1 server overlap (requests=%d)", context,
				server.requests())
			.isGreaterThanOrEqualTo(ConcurrentRequestHarness.UNCALIBRATED_SERVER_OVERLAP_FLOOR);
		assertThat(d1.get(ReadResult.HIT) + d1.get(ReadResult.MISS) + d1.get(ReadResult.BYPASS))
			.as("%s wave 1 hit + miss + bypass", context)
			.isEqualTo(ConcurrentRequestHarness.THREADS);
		assertThat(List.of(d1.get(ReadResult.MISMATCH), d1.get(ReadResult.UNPARSEABLE)))
			.as("%s wave 1 mismatch, unparseable", context)
			.containsExactly(0.0, 0.0);

		byte[] wave1Body = wave1.responses().get(0).body();

		// Ensure the entry. A timeout during wave 1 bypasses its populates as well as its reads, so
		// the entry may be missing; after this step a wave-2 miss can only be a mutation or a defect.
		RedisTestSupport.awaitGatewayReady(this.gateway);
		Map<ReadResult, Double> beforeEnsure = reads();
		Response ensure = send(key, body);
		Map<ReadResult, Double> beforeWave2 = reads();
		Map<ReadResult, Double> de = delta(beforeEnsure, beforeWave2);
		System.out.println("CONCURRENCY fastpath ensure round=" + round + " readHit=" + de.get(ReadResult.HIT)
				+ " readMiss=" + de.get(ReadResult.MISS) + " readBypass=" + de.get(ReadResult.BYPASS));
		assertThat(ensure.status()).as("%s ensure status", context).isEqualTo(201);
		assertThat(ensure.body()).as("%s ensure body = wave 1's", context).isEqualTo(wave1Body);
		assertThat(entry(key).text()).as("%s ensured entry = response_body::text", context).isEqualTo(storedText(key));

		Run wave2 = this.harness.fire(() -> this.harness.post(TRANSFERS, key, body));
		Map<ReadResult, Double> d2 = delta(beforeWave2, reads());
		Set<String> all = new HashSet<>(distinct(wave1));
		all.addAll(distinct(wave2));
		System.out.println("CONCURRENCY fastpath wave=2 round=" + round + " readHit=" + d2.get(ReadResult.HIT)
				+ " readMiss=" + d2.get(ReadResult.MISS) + " readBypass=" + d2.get(ReadResult.BYPASS)
				+ " readMismatch=" + d2.get(ReadResult.MISMATCH) + " readUnparseable=" + d2.get(ReadResult.UNPARSEABLE)
				+ " transactions=" + transfersOn(a) + " distinctBodiesAll200=" + all.size() + " statuses="
				+ statuses(wave2));
		assertThat(clientExceptions(wave2)).as("%s wave 2 client exceptions", context).isEmpty();
		assertThat(statuses(wave2)).as("%s wave 2 statuses", context)
			.containsExactly(Map.entry(201, (long) ConcurrentRequestHarness.THREADS));
		assertThat(transfersOn(a)).as("%s transactions after wave 2", context).isEqualTo(1);
		assertThat(all).as("%s distinct bodies over both waves", context).hasSize(1);
		// With the entry ensured, a miss can only be a mutation or a defect. A bypass is a timed-out
		// command, a miss by design (D-135), so the hit count is printed, not asserted: "served from
		// Redis" is a performance claim, which comes from the local soak with hardware stated (D-067).
		assertThat(List.of(d2.get(ReadResult.MISS), d2.get(ReadResult.MISMATCH), d2.get(ReadResult.UNPARSEABLE)))
			.as("%s wave 2 miss, mismatch, unparseable", context)
			.containsExactly(0.0, 0.0, 0.0);
		assertThat(d2.get(ReadResult.HIT) + d2.get(ReadResult.BYPASS)).as("%s wave 2 hit + bypass", context)
			.isEqualTo(ConcurrentRequestHarness.THREADS);

		// The proof that the Redis path serves the DB's bytes: a sequential request on a ready gateway
		// is a hit.
		RedisTestSupport.awaitGatewayReady(this.gateway);
		Map<ReadResult, Double> beforeFinal = reads();
		Response last = send(key, body);
		Map<ReadResult, Double> df = delta(beforeFinal, reads());
		System.out.println("CONCURRENCY fastpath final round=" + round + " readHit=" + df.get(ReadResult.HIT)
				+ " readMiss=" + df.get(ReadResult.MISS) + " readBypass=" + df.get(ReadResult.BYPASS));
		assertThat(df.get(ReadResult.HIT)).as("%s final request read{hit}", context).isEqualTo(1);
		assertThat(last.status()).as("%s final status", context).isEqualTo(201);
		assertThat(last.body()).as("%s final body = wave 1's", context).isEqualTo(wave1Body);
	}

	// (c)
	@Test
	void storedOutcomesReplayFromRedis() throws Exception {
		// A stored 422 insufficient_funds.
		long poor = this.harness.createAccount("CUSTOMER");
		long other = this.harness.createAccount("CUSTOMER");
		String key422 = ConcurrentRequestHarness.newKey();
		byte[] overdraw = transfer(poor, other, "10.00");
		Response owner422 = send(key422, overdraw);
		assertThat(owner422.status()).isEqualTo(422);
		assertHitReplays(TRANSFERS, key422, overdraw, owner422, 422, "application/problem+json", "insufficient_funds");

		// A keyed 409 account_name_taken.
		String name = "taken-" + ConcurrentRequestHarness.newKey();
		Response first = this.harness.send(this.harness.post("/v1/accounts", null, account(name)));
		assertThat(first.status()).isEqualTo(201);
		String key409 = ConcurrentRequestHarness.newKey();
		Response owner409 = this.harness.send(this.harness.post("/v1/accounts", key409, account(name)));
		assertThat(owner409.status()).isEqualTo(409);
		assertHitReplays("/v1/accounts", key409, account(name), owner409, 409, "application/problem+json", "account_name_taken");

		// A keyed account creation: the hit carries the owner's Location.
		String keyCreate = ConcurrentRequestHarness.newKey();
		byte[] create = account("fast-path-" + keyCreate);
		HttpClient client = HttpClient.newHttpClient();
		HttpResponse<byte[]> owner = client.send(this.harness.post("/v1/accounts", keyCreate, create),
				HttpResponse.BodyHandlers.ofByteArray());
		assertThat(owner.statusCode()).isEqualTo(201);
		Map<ReadResult, Double> before = reads();
		HttpResponse<byte[]> replay = client.send(this.harness.post("/v1/accounts", keyCreate, create),
				HttpResponse.BodyHandlers.ofByteArray());
		assertThat(delta(before, reads()).get(ReadResult.HIT)).isEqualTo(1);
		assertThat(replay.statusCode()).isEqualTo(201);
		assertThat(replay.body()).isEqualTo(owner.body());
		assertThat(replay.headers().firstValue("Location")).isPresent().isEqualTo(owner.headers().firstValue("Location"));
	}

	private void assertHitReplays(String route, String key, byte[] body, Response owner, int status, String contentType,
			String code) throws Exception {
		assertThat(entry(key).status()).isEqualTo(status);
		Map<ReadResult, Double> before = reads();
		Response replay = send(key, body, route);
		assertThat(delta(before, reads()).get(ReadResult.HIT)).as("read{hit} for the stored %d", status).isEqualTo(1);
		assertThat(replay.status()).isEqualTo(status);
		assertThat(replay.contentType()).isEqualTo(contentType);
		assertThat(replay.body()).isEqualTo(owner.body());
		assertThat(new String(replay.body(), StandardCharsets.UTF_8)).contains(code);
	}

	// (d)
	@Test
	void aMismatchedFingerprintGets422FromTheDbAndLeavesTheEntryUntouched() throws Exception {
		long a = funded("100.00");
		long b = this.harness.createAccount("CUSTOMER");
		String key = ConcurrentRequestHarness.newKey();
		Response owner = send(key, transfer(a, b, "10.00"));
		assertThat(owner.status()).isEqualTo(201);
		String valueBefore = rawEntry(key);
		long pttlBefore = pttl(key);
		String dbBefore = storedText(key) + "|" + storedHash(key);

		Map<ReadResult, Double> before = reads();
		Response mismatched = send(key, transfer(a, b, "11.00"));
		assertThat(mismatched.status()).as("same key, different amount").isEqualTo(422);
		assertThat(mismatched.contentType()).isEqualTo("application/problem+json");
		assertThat(new String(mismatched.body(), StandardCharsets.UTF_8)).contains("idempotency_key_reused");
		assertThat(delta(before, reads()).get(ReadResult.MISMATCH)).as("read{mismatch}").isEqualTo(1);
		assertThat(rawEntry(key)).as("entry value byte-identical").isEqualTo(valueBefore);
		assertThat(pttl(key)).as("PTTL not reset").isLessThanOrEqualTo(pttlBefore);
		assertThat(storedText(key) + "|" + storedHash(key)).as("DB row unchanged (422 not stored)").isEqualTo(dbBefore);
		assertThat(transfersOn(a)).isEqualTo(1);
	}

	// (e) A key held uncommitted by a test transaction (IdempotencyInProgressTest pattern, D-052).
	@Test
	void anInFlightKeyIsNeverCachedAndTheHoldersResponseIsCachedByTheNextReplay() throws Exception {
		long a = funded("100.00");
		long b = this.harness.createAccount("CUSTOMER");
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = transfer(a, b, "10.00");
		byte[] fingerprint = Fingerprint.of("POST", TRANSFERS, new TransferShape(a, b, new Money(new BigDecimal("10.00"))));
		String held = "{\"held_by\": \"test\", \"transaction_id\": 0}";

		Map<ReadResult, Double> before = reads();
		try (Connection holder = LedgerTestSupport.connectApp(this.dataSource)) {
			int holderPid = backendPid(holder);
			insertKey(holder, key, fingerprint);
			Future<Response> waiting = this.requests.submit(() -> send(key, body));
			awaitBlockedOn(holderPid);
			Response inProgress = waiting.get(30, TimeUnit.SECONDS);
			assertThat(inProgress.status()).as("while the key is held").isEqualTo(409);
			assertThat(inProgress.contentType()).isEqualTo("application/problem+json");
			assertThat(new String(inProgress.body(), StandardCharsets.UTF_8)).contains("idempotency_request_in_progress");
			assertThat(rawEntry(key)).as("no entry for an unstored 409").isNull();
			assertThat(delta(before, reads()).get(ReadResult.MISS)).as("read{miss}").isEqualTo(1);

			try (PreparedStatement ps = holder.prepareStatement(
					"UPDATE idempotency_keys SET status_code = 201, response_body = ?::jsonb WHERE client_id = ? AND key = ?")) {
				ps.setString(1, held);
				ps.setString(2, EdgeProbe.CLIENT_ID);
				ps.setString(3, key);
				assertThat(ps.executeUpdate()).isEqualTo(1);
			}
			holder.commit();
		}

		Map<String, Double> populatesBefore = populates();
		Response replay = send(key, body);
		assertThat(replay.status()).as("replay of the holder's response").isEqualTo(201);
		assertThat(new String(replay.body(), StandardCharsets.UTF_8)).isEqualTo(storedText(key));
		assertThat(populates().get("written") - populatesBefore.get("written")).as("populate{written}").isEqualTo(1);

		Map<ReadResult, Double> beforeHit = reads();
		Response hit = send(key, body);
		assertThat(delta(beforeHit, reads()).get(ReadResult.HIT)).isEqualTo(1);
		assertThat(hit.body()).isEqualTo(replay.body());
		assertThat(transfersOn(a)).as("no transfer: the holder's response was a stand-in").isZero();
	}

	// (f) D-141's hook, armed for one key.
	@Test
	void anOwnerRolledBackAfterItsStoreLeavesNoEntryAndTheRetryIsTheOwner() throws Exception {
		long a = funded("100.00");
		long b = this.harness.createAccount("CUSTOMER");
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = transfer(a, b, "10.00");
		this.hook.armed.set(key);

		Response failed = send(key, body);
		assertThat(failed.status()).as("the hook threw after the store").isEqualTo(500);
		assertThat(this.hook.armed.get()).as("the hook fired").isNull();
		assertThat(keyRows(key)).as("key rows after the rollback").isZero();
		assertThat(transfersOn(a)).isZero();
		assertThat(rawEntry(key)).as("no entry for a rolled-back owner").isNull();

		Response retry = send(key, body);
		assertThat(retry.status()).as("the retry is the owner").isEqualTo(201);
		assertThat(transfersOn(a)).isEqualTo(1);
		assertThat(rawEntry(key)).as("entry after the real owner").isNotNull();
	}

	// --- helpers ---

	private Response send(String key, byte[] body) {
		return send(key, body, TRANSFERS);
	}

	private Response send(String key, byte[] body, String route) {
		return this.harness.send(this.harness.post(route, key, body));
	}

	private long funded(String amount) {
		long account = this.harness.createAccount("CUSTOMER");
		this.harness.deposit(account, amount);
		return account;
	}

	private static byte[] transfer(long from, long to, String amount) {
		return ("{\"from_account_id\": " + from + ", \"to_account_id\": " + to + ", \"amount\": \"" + amount + "\"}")
			.getBytes(StandardCharsets.UTF_8);
	}

	private static byte[] account(String name) {
		return ("{\"name\": \"" + name + "\", \"type\": \"CUSTOMER\"}").getBytes(StandardCharsets.UTF_8);
	}

	private int transfersOn(long account) throws SQLException {
		return LedgerTestSupport.transactionIds(this.dataSource, account, "TRANSFER").size();
	}

	private String storedText(String key) {
		return this.jdbc
			.sql("SELECT response_body::text FROM idempotency_keys WHERE client_id = :client AND key = :key")
			.param("client", EdgeProbe.CLIENT_ID)
			.param("key", key)
			.query(String.class)
			.single();
	}

	private String storedHash(String key) {
		return this.jdbc
			.sql("SELECT encode(request_hash, 'hex') FROM idempotency_keys WHERE client_id = :client AND key = :key")
			.param("client", EdgeProbe.CLIENT_ID)
			.param("key", key)
			.query(String.class)
			.single();
	}

	private long keyRows(String key) {
		return this.jdbc.sql("SELECT count(*) FROM idempotency_keys WHERE client_id = :client AND key = :key")
			.param("client", EdgeProbe.CLIENT_ID)
			.param("key", key)
			.query(Long.class)
			.single();
	}

	private String rawEntry(String key) {
		RedisGateway.Reply<String> reply = this.gateway.optional("GET",
				(r) -> r.get(IdempotencyCache.key(EdgeProbe.CLIENT_ID, key)));
		assertThat(reply.available()).as("Redis reachable").isTrue();
		return reply.value();
	}

	private IdempotencyCache.Entry entry(String key) throws IdempotencyCache.Unparseable {
		String value = rawEntry(key);
		assertThat(value).as("cache entry for %s", key).isNotNull();
		return IdempotencyCache.parse(value);
	}

	private long pttl(String key) {
		RedisGateway.Reply<Long> reply = this.gateway.optional("PTTL",
				(r) -> r.pttl(IdempotencyCache.key(EdgeProbe.CLIENT_ID, key)));
		assertThat(reply.available()).isTrue();
		return reply.value();
	}

	private Map<ReadResult, Double> reads() {
		Map<ReadResult, Double> counts = new EnumMap<>(ReadResult.class);
		for (ReadResult result : ReadResult.values()) {
			counts.put(result,
					this.meters.counter("ledger.idempotency.cache.read", "result", result.name().toLowerCase()).count());
		}
		return counts;
	}

	private Map<String, Double> populates() {
		return Stream.of("written", "exists", "skipped_short_life", "unavailable")
			.collect(Collectors.toMap(Function.identity(),
					(result) -> this.meters.counter("ledger.idempotency.cache.populate", "result", result).count()));
	}

	private static Map<ReadResult, Double> delta(Map<ReadResult, Double> before, Map<ReadResult, Double> after) {
		Map<ReadResult, Double> d = new EnumMap<>(ReadResult.class);
		for (ReadResult result : ReadResult.values()) {
			d.put(result, after.get(result) - before.get(result));
		}
		return d;
	}

	private static List<Throwable> clientExceptions(Run run) {
		return run.responses().stream().map(Response::exception).filter((e) -> e != null).toList();
	}

	private static Map<Integer, Long> statuses(Run run) {
		return run.responses().stream().collect(Collectors.groupingBy(Response::status, Collectors.counting()));
	}

	private static Set<String> distinct(Run run) {
		Set<String> bodies = new HashSet<>();
		for (Response response : run.responses()) {
			bodies.add(response.body() == null ? null : new String(response.body(), StandardCharsets.UTF_8));
		}
		return bodies;
	}

	private long awaitBlockedOn(int holderPid) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BLOCK_TIMEOUT_MILLIS);
		try (Connection poller = LedgerTestSupport.connectApp(this.dataSource);
				PreparedStatement ps = poller
					.prepareStatement("SELECT count(*) FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid))")) {
			ps.setInt(1, holderPid);
			while (System.nanoTime() < deadline) {
				try (ResultSet rs = ps.executeQuery()) {
					assertThat(rs.next()).isTrue();
					long blocked = rs.getLong(1);
					poller.rollback();
					if (blocked == 1) {
						return System.nanoTime();
					}
				}
				Thread.sleep(10);
			}
		}
		return fail("request never blocked on the holder (pid " + holderPid + ")");
	}

	private static int backendPid(Connection conn) throws SQLException {
		try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT pg_backend_pid()")) {
			assertThat(rs.next()).isTrue();
			return rs.getInt(1);
		}
	}

	private static void insertKey(Connection conn, String key, byte[] requestHash) throws SQLException {
		try (PreparedStatement ps = conn
			.prepareStatement("INSERT INTO idempotency_keys (client_id, key, request_hash) VALUES (?, ?, ?)")) {
			ps.setString(1, EdgeProbe.CLIENT_ID);
			ps.setString(2, key);
			ps.setBytes(3, requestHash);
			assertThat(ps.executeUpdate()).as("key row inserted").isEqualTo(1);
		}
	}

	// Throws once, for the armed key only (D-141).
	static final class ArmableHook implements IdempotencyFaultHook {

		final AtomicReference<String> armed = new AtomicReference<>();

		@Override
		public void afterStoreBeforeCommit(String clientId, String key) {
			// compareAndSet compares references: match by equals first, then clear that exact reference.
			String target = this.armed.get();
			if (key.equals(target) && this.armed.compareAndSet(target, null)) {
				throw new IllegalStateException("fault injected after the store (test)");
			}
		}

	}

	@TestConfiguration(proxyBeanMethods = false)
	static class Hooks {

		@Bean
		@Primary
		ArmableHook armableHook() {
			return new ArmableHook();
		}

	}

}
