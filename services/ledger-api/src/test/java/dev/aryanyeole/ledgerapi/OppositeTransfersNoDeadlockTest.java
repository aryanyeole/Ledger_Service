package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.Import;

import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Response;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Run;
import dev.aryanyeole.ledgerapi.LedgerTestSupport.Totals;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;

// Proof (D-007, D-068): opposite transfers never deadlock. Behind one start gate, over real
// HTTP after a warm-up round (D-067): 25 A->B and 25 B->A transfers, plus 5 deposits into
// each of A and B, every request with its own Idempotency-Key, in a shuffled order (seed
// logged; -Dledger.test.shuffle.seed replays it). A->B and B->A take the same two row locks;
// PostingService locks them in ascending id order in one statement, so neither can hold one
// while waiting for the other. A deadlock would surface as a 500 (40P01 is never retried,
// D-050). Never asserts on pg_stat_database.deadlocks: it is written lazily per backend.
// Rounds: -Dledger.test.concurrency.rounds (CI 1, local soak 20).
@Import({ TestcontainersConfiguration.class, ServerArrivals.Configuration.class })
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, properties = EdgeProbe.API_KEYS_PROPERTY)
class OppositeTransfersNoDeadlockTest {

	static final String SEED_PROPERTY = "ledger.test.shuffle.seed";

	private static final int TRANSFERS_EACH_WAY = 25;

	private static final int DEPOSITS_EACH = 5;

	private static final int REQUESTS = 2 * TRANSFERS_EACH_WAY + 2 * DEPOSITS_EACH;

	// D-106 tripwire floor = max(2, floor(0.6 x CI min)) over this test's own CI history, 4-processor
	// ubuntu-24.04 runners: n = 53, i.e. 43 observations from 35 retained runs (2026-10-14..20) plus
	// 10 fresh (PR #37: run 39383847328 attempts 1-6, run 39493178184 attempts 1-4). Minimum 3 (run 39380129034 attempt 1, BUG-005), so 2. A miss is logged as a bug and the
	// floor re-derived from the full history.
	private static final int MIN_SERVER_ARRIVED_BEFORE_FIRST_COMPLETION = 2;

	@LocalServerPort
	int port;

	@Autowired
	DataSource dataSource;

	@Autowired
	WebServerApplicationContext webServerContext;

	@Autowired
	ServerArrivals arrivals;

	private record Planned(String key, Supplier<HttpRequest> request) {
	}

	@TestFactory
	Stream<DynamicTest> oppositeTransfersAndDepositsNeverDeadlock() throws Exception {
		long baseSeed = Long.getLong(SEED_PROPERTY, System.nanoTime());
		System.out.println("DEADLOCK config " + ConcurrentRequestHarness.serverLimits(this.dataSource,
				this.webServerContext) + " rounds=" + ConcurrentRequestHarness.rounds() + " baseSeed=" + baseSeed);
		ConcurrentRequestHarness harness = new ConcurrentRequestHarness(this.port);
		Set<Integer> seenPorts = new HashSet<>();
		warmUp(harness, seenPorts, baseSeed);
		return IntStream.rangeClosed(1, ConcurrentRequestHarness.rounds())
			.mapToObj((round) -> dynamicTest("round " + round,
					() -> round(harness, round, baseSeed + round, seenPorts)));
	}

	// One discarded round of the same mix on separate accounts (D-067). Only client exceptions
	// are checked.
	private void warmUp(ConcurrentRequestHarness harness, Set<Integer> seenPorts, long baseSeed) throws Exception {
		long a = funded(harness);
		long b = funded(harness);
		List<Planned> planned = plan(harness, a, b, baseSeed);
		Run run = harness.fire(planned.stream().map(Planned::request).toList());
		ServerArrivals.Overlap server = this.arrivals.overlap(keys(planned), seenPorts);
		System.out.println("DEADLOCK warmup seed=" + baseSeed + " serverArrivedBeforeFirstCompletion="
				+ server.arrivedBeforeFirstCompletion() + " connections=" + server.connections() + " statuses="
				+ statuses(run));
		assertThat(run.responses().stream().filter((r) -> r.exception() != null).map(Response::exception).toList())
			.as("warm-up client exceptions")
			.isEmpty();
	}

	private void round(ConcurrentRequestHarness harness, int round, long seed, Set<Integer> seenPorts)
			throws Exception {
		long a = funded(harness);
		long b = funded(harness);
		long external = SystemAccountFixture.external(this.dataSource);
		Set<Long> transfersBefore = transactionsOn(a, b, "TRANSFER");
		Set<Long> depositsBefore = transactionsOn(a, b, "DEPOSIT");
		Totals externalBefore = LedgerTestSupport.totals(this.dataSource, external);

		List<Planned> planned = plan(harness, a, b, seed);
		Run run = harness.fire(planned.stream().map(Planned::request).toList());

		ServerArrivals.Overlap server = this.arrivals.overlap(keys(planned), seenPorts);
		Set<Long> newTransfers = difference(transactionsOn(a, b, "TRANSFER"), transfersBefore);
		Set<Long> newDeposits = difference(transactionsOn(a, b, "DEPOSIT"), depositsBefore);
		Totals aTotals = LedgerTestSupport.totals(this.dataSource, a);
		Totals bTotals = LedgerTestSupport.totals(this.dataSource, b);
		Map<Integer, Long> statuses = statuses(run);
		System.out.println("DEADLOCK round=" + round + " seed=" + seed + " processors="
				+ ConcurrentRequestHarness.processors() + " serverArrivedBeforeFirstCompletion="
				+ server.arrivedBeforeFirstCompletion() + " arrivalSpreadMs=" + server.spreadMillis()
				+ " reusedConnections=" + server.reusedConnections() + "/" + server.connections()
				+ " clientSentBeforeFirst=" + run.sentBeforeFirstResponse() + " peakInFlight=" + run.peakInFlight()
				+ " statuses=" + statuses + " newTransfers=" + newTransfers.size() + " newDeposits=" + newDeposits.size()
				+ " A=" + aTotals.balance() + "/" + aTotals.entrySum() + " B=" + bTotals.balance() + "/"
				+ bTotals.entrySum());

		String context = "round " + round + " (seed " + seed + ")";
		assertThat(run.responses()).as("%s: responses", context).hasSize(REQUESTS);
		assertThat(run.responses().stream().filter((r) -> r.exception() != null).map(Response::exception).toList())
			.as("%s: client exceptions", context)
			.isEmpty();
		assertThat(statuses).as("%s: status counts (no 409, 503 or 5xx)", context)
			.containsExactly(Map.entry(201, (long) REQUESTS));
		assertThat(newTransfers).as("%s: new TRANSFER transactions on A or B", context)
			.hasSize(2 * TRANSFERS_EACH_WAY);
		assertThat(newDeposits).as("%s: new DEPOSIT transactions on A or B", context).hasSize(2 * DEPOSITS_EACH);
		assertTotals(aTotals, a, "1005.0000", context);
		assertTotals(bTotals, b, "1005.0000", context);
		Totals externalDelta = LedgerTestSupport.totals(this.dataSource, external).minus(externalBefore);
		assertThat(externalDelta.balance()).as("%s: EXTERNAL balance moved", context).isEqualByComparingTo("-10.00");
		assertThat(externalDelta.entrySum()).as("%s: EXTERNAL entry sum moved", context).isEqualByComparingTo("-10.00");
		// D-067 tripwire against a serialized harness; client metrics are report-only.
		assertThat(server.arrivedBeforeFirstCompletion())
			.as("%s: requests that arrived at the server before the first completed (was %d)", context,
					server.arrivedBeforeFirstCompletion())
			.isGreaterThanOrEqualTo(MIN_SERVER_ARRIVED_BEFORE_FIRST_COMPLETION);
	}

	// 25 A->B, 25 B->A, 5 deposits to A, 5 to B; each with its own key; shuffled by seed.
	private List<Planned> plan(ConcurrentRequestHarness harness, long a, long b, long seed) {
		List<Planned> planned = new ArrayList<>();
		for (int i = 0; i < TRANSFERS_EACH_WAY; i++) {
			planned.add(transfer(harness, a, b));
			planned.add(transfer(harness, b, a));
		}
		for (int i = 0; i < DEPOSITS_EACH; i++) {
			planned.add(deposit(harness, a));
			planned.add(deposit(harness, b));
		}
		Collections.shuffle(planned, new Random(seed));
		return planned;
	}

	private static Planned transfer(ConcurrentRequestHarness harness, long from, long to) {
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = ("{\"from_account_id\": " + from + ", \"to_account_id\": " + to + ", \"amount\": \"1.00\"}")
			.getBytes(StandardCharsets.UTF_8);
		return new Planned(key, () -> harness.post("/v1/transfers", key, body));
	}

	private static Planned deposit(ConcurrentRequestHarness harness, long account) {
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = ("{\"account_id\": " + account + ", \"amount\": \"1.00\"}").getBytes(StandardCharsets.UTF_8);
		return new Planned(key, () -> harness.post("/v1/deposits", key, body));
	}

	private static long funded(ConcurrentRequestHarness harness) {
		long id = harness.createAccount("CUSTOMER");
		harness.deposit(id, "1000.00");
		return id;
	}

	private Set<Long> transactionsOn(long a, long b, String kind) throws Exception {
		Set<Long> ids = new TreeSet<>(LedgerTestSupport.transactionIds(this.dataSource, a, kind));
		ids.addAll(LedgerTestSupport.transactionIds(this.dataSource, b, kind));
		return ids;
	}

	private static void assertTotals(Totals totals, long accountId, String expected, String context) {
		assertThat(totals.balance()).as("%s: account %d balance", context, accountId).isEqualByComparingTo(expected);
		assertThat(totals.entrySum()).as("%s: account %d entry sum", context, accountId)
			.isEqualByComparingTo(totals.balance());
	}

	private static Set<Long> difference(Set<Long> after, Set<Long> before) {
		Set<Long> result = new TreeSet<>(after);
		result.removeAll(before);
		return result;
	}

	private static Set<String> keys(List<Planned> planned) {
		return planned.stream().map(Planned::key).collect(Collectors.toSet());
	}

	private static Map<Integer, Long> statuses(Run run) {
		return run.responses().stream().collect(Collectors.groupingBy(Response::status, Collectors.counting()));
	}

}
