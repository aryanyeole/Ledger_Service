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
import java.util.TreeMap;
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

// Proof (D-075, invariant 4): authorizes never deadlock with transfers over the same accounts.
// Behind one start gate, over real HTTP after a warm-up round (D-067): 25 authorizes C->M,
// 25 transfers M->C and 25 transfers C->M, every request with its own key, shuffled (seed logged;
// -Dledger.test.shuffle.seed replays it). M is created before C, so M.id < C.id: a transfer locks
// M then C. An authorize locks {customer, merchant, ESCROW} in one ascending statement before it
// inserts the payment, whose FKs then take KEY SHARE on rows it already holds. Inserting the
// payment first, or leaving the merchant out of the lock, takes a lock outside the ordered pass
// and closes a cycle. Both accounts are funded before the gate, so no ordering can produce a
// 422. A deadlock surfaces as a 500 (40P01 is never retried, D-050).
// Rounds: -Dledger.test.concurrency.rounds (CI 1, local soak 20). ESCROW is checked by delta.
@Import({ TestcontainersConfiguration.class, ServerArrivals.Configuration.class })
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, properties = EdgeProbe.API_KEYS_PROPERTY)
class PaymentAuthorizeLockOrderTest {

	private static final int EACH = 25;

	private static final int REQUESTS = 3 * EACH;

	// D-106: uncalibrated until this test has its own CI distribution.
	private static final int MIN_SERVER_ARRIVED_BEFORE_FIRST_COMPLETION = ConcurrentRequestHarness.UNCALIBRATED_SERVER_OVERLAP_FLOOR;

	@LocalServerPort
	int port;

	@Autowired
	DataSource dataSource;

	@Autowired
	WebServerApplicationContext webServerContext;

	@Autowired
	ServerArrivals arrivals;

	private record Planned(String kind, String key, Supplier<HttpRequest> request) {
	}

	private record Accounts(long merchant, long customer) {
	}

	@TestFactory
	Stream<DynamicTest> authorizesAndOppositeTransfersNeverDeadlock() throws Exception {
		long baseSeed = Long.getLong(OppositeTransfersNoDeadlockTest.SEED_PROPERTY, System.nanoTime());
		System.out.println("PAYLOCK config " + ConcurrentRequestHarness.serverLimits(this.dataSource,
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
		List<Planned> planned = plan(harness, accounts(harness), baseSeed);
		Run run = harness.fire(planned.stream().map(Planned::request).toList());
		ServerArrivals.Overlap server = this.arrivals.overlap(keys(planned), seenPorts);
		System.out.println("PAYLOCK warmup seed=" + baseSeed + " serverArrivedBeforeFirstCompletion="
				+ server.arrivedBeforeFirstCompletion() + " connections=" + server.connections() + " statuses="
				+ statusesByKind(planned, run));
		assertThat(run.responses().stream().filter((r) -> r.exception() != null).map(Response::exception).toList())
			.as("warm-up client exceptions")
			.isEmpty();
	}

	private void round(ConcurrentRequestHarness harness, int round, long seed, Set<Integer> seenPorts)
			throws Exception {
		Accounts accounts = accounts(harness);
		long m = accounts.merchant();
		long c = accounts.customer();
		long escrow = LedgerTestSupport.accountId(this.dataSource, "system:escrow");
		Totals escrowBefore = LedgerTestSupport.totals(this.dataSource, escrow);
		Set<Long> transfersBefore = new HashSet<>(LedgerTestSupport.transactionIds(this.dataSource, c, "TRANSFER"));

		List<Planned> planned = plan(harness, accounts, seed);
		Run run = harness.fire(planned.stream().map(Planned::request).toList());

		ServerArrivals.Overlap server = this.arrivals.overlap(keys(planned), seenPorts);
		Map<String, Map<Integer, Long>> byKind = statusesByKind(planned, run);
		Set<Long> newTransfers = new HashSet<>(LedgerTestSupport.transactionIds(this.dataSource, c, "TRANSFER"));
		newTransfers.removeAll(transfersBefore);
		List<Long> authorizes = LedgerTestSupport.transactionIds(this.dataSource, c, "PAYMENT_AUTHORIZE");
		long payments = LedgerTestSupport.queryLong(this.dataSource,
				"SELECT count(*) FROM payments WHERE customer_account_id = ? AND status = 'AUTHORIZED'", c);
		long outboxRows = LedgerTestSupport.queryLong(this.dataSource,
				"SELECT count(*) FROM outbox WHERE message_key IN (SELECT id::text FROM payments WHERE customer_account_id = ?)",
				c);
		Totals cTotals = LedgerTestSupport.totals(this.dataSource, c);
		Totals mTotals = LedgerTestSupport.totals(this.dataSource, m);
		Totals escrowDelta = LedgerTestSupport.totals(this.dataSource, escrow).minus(escrowBefore);
		System.out.println("PAYLOCK round=" + round + " seed=" + seed + " processors="
				+ ConcurrentRequestHarness.processors() + " serverArrivedBeforeFirstCompletion="
				+ server.arrivedBeforeFirstCompletion() + " arrivalSpreadMs=" + server.spreadMillis()
				+ " clientSentBeforeFirst=" + run.sentBeforeFirstResponse() + " statusesByKind=" + byKind
				+ " authorizes=" + authorizes.size() + " newTransfers=" + newTransfers.size() + " payments=" + payments
				+ " outboxRows=" + outboxRows + " C=" + cTotals.balance() + "/" + cTotals.entrySum() + " M="
				+ mTotals.balance() + "/" + mTotals.entrySum() + " escrowDelta=" + escrowDelta.balance());

		String context = "round " + round + " (seed " + seed + ")";
		assertThat(run.responses()).as("%s: responses", context).hasSize(REQUESTS);
		assertThat(run.responses().stream().filter((r) -> r.exception() != null).map(Response::exception).toList())
			.as("%s: client exceptions", context)
			.isEmpty();
		assertThat(byKind).as("%s: status counts per kind (no 409, 503 or 5xx)", context)
			.isEqualTo(Map.of("authorize C->M", Map.of(201, (long) EACH), "transfer C->M", Map.of(201, (long) EACH),
					"transfer M->C", Map.of(201, (long) EACH)));
		assertThat(authorizes).as("%s: PAYMENT_AUTHORIZE transactions on C", context).hasSize(EACH);
		assertThat(newTransfers).as("%s: new TRANSFER transactions on C", context).hasSize(2 * EACH);
		assertThat(payments).as("%s: AUTHORIZED payments of C", context).isEqualTo(EACH);
		assertThat(outboxRows).as("%s: outbox rows for C's payments", context).isEqualTo(EACH);
		assertTotals(cTotals, c, "975.0000", context);
		assertTotals(mTotals, m, "1000.0000", context);
		assertThat(escrowDelta.balance()).as("%s: ESCROW balance moved", context).isEqualByComparingTo("25.00");
		assertThat(escrowDelta.entrySum()).as("%s: ESCROW entry sum moved", context).isEqualByComparingTo("25.00");
		// D-067 tripwire against a serialized harness; client metrics are report-only.
		assertThat(server.arrivedBeforeFirstCompletion())
			.as("%s: requests that arrived at the server before the first completed (was %d)", context,
					server.arrivedBeforeFirstCompletion())
			.isGreaterThanOrEqualTo(MIN_SERVER_ARRIVED_BEFORE_FIRST_COMPLETION);
	}

	// The merchant first, so its id is lower; both funded with 1000.00 before the gate.
	private static Accounts accounts(ConcurrentRequestHarness harness) {
		long merchant = harness.createAccount("MERCHANT");
		long customer = harness.createAccount("CUSTOMER");
		assertThat(merchant).as("merchant id below customer id").isLessThan(customer);
		harness.deposit(merchant, "1000.00");
		harness.deposit(customer, "1000.00");
		return new Accounts(merchant, customer);
	}

	private List<Planned> plan(ConcurrentRequestHarness harness, Accounts accounts, long seed) {
		long m = accounts.merchant();
		long c = accounts.customer();
		List<Planned> planned = new ArrayList<>();
		for (int i = 0; i < EACH; i++) {
			planned.add(post(harness, "authorize C->M", "/v1/payments",
					"{\"customer_account_id\": " + c + ", \"merchant_account_id\": " + m + ", \"amount\": \"1.00\"}"));
			planned.add(post(harness, "transfer M->C", "/v1/transfers",
					"{\"from_account_id\": " + m + ", \"to_account_id\": " + c + ", \"amount\": \"1.00\"}"));
			planned.add(post(harness, "transfer C->M", "/v1/transfers",
					"{\"from_account_id\": " + c + ", \"to_account_id\": " + m + ", \"amount\": \"1.00\"}"));
		}
		Collections.shuffle(planned, new Random(seed));
		return planned;
	}

	private static Planned post(ConcurrentRequestHarness harness, String kind, String path, String json) {
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = json.getBytes(StandardCharsets.UTF_8);
		return new Planned(kind, key, () -> harness.post(path, key, body));
	}

	private static void assertTotals(Totals totals, long accountId, String expected, String context) {
		assertThat(totals.balance()).as("%s: account %d balance", context, accountId).isEqualByComparingTo(expected);
		assertThat(totals.entrySum()).as("%s: account %d entry sum", context, accountId)
			.isEqualByComparingTo(totals.balance());
	}

	private static Set<String> keys(List<Planned> planned) {
		return planned.stream().map(Planned::key).collect(Collectors.toSet());
	}

	// Responses are in plan order (ConcurrentRequestHarness.fire), so each pairs with its kind.
	private static Map<String, Map<Integer, Long>> statusesByKind(List<Planned> planned, Run run) {
		Map<String, Map<Integer, Long>> byKind = new TreeMap<>();
		for (int i = 0; i < planned.size(); i++) {
			byKind.computeIfAbsent(planned.get(i).kind(), (kind) -> new TreeMap<>())
				.merge(run.responses().get(i).status(), 1L, Long::sum);
		}
		return byKind;
	}

}
