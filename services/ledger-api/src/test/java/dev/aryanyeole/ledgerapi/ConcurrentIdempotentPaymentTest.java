package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import com.jayway.jsonpath.JsonPath;

import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Response;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Run;
import dev.aryanyeole.ledgerapi.LedgerTestSupport.KeyRow;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;

// Invariant 7 for POST /v1/payments: 100 identical authorizes fired at once over real HTTP
// (D-067) create one payment, one PAYMENT_AUTHORIZE posting and one outbox row, and all 100
// clients get the same 201 bytes. Same mechanism and harness as ConcurrentIdempotentTransferTest:
// the first key insert wins, the rest block on its uncommitted index entry and replay the stored
// text. Scoped to the round's fresh customer (BUG-002). Rounds: -Dledger.test.concurrency.rounds.
@Import({ TestcontainersConfiguration.class, ServerArrivals.Configuration.class })
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, properties = EdgeProbe.API_KEYS_PROPERTY)
class ConcurrentIdempotentPaymentTest {

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

	@TestFactory
	Stream<DynamicTest> hundredIdenticalAuthorizesCreateOnePayment() throws Exception {
		System.out.println("CONCURRENCY payments config " + ConcurrentRequestHarness.serverLimits(this.dataSource,
				this.webServerContext) + " rounds=" + ConcurrentRequestHarness.rounds());
		ConcurrentRequestHarness harness = new ConcurrentRequestHarness(this.port);
		Set<Integer> seenPorts = new HashSet<>();
		warmUp(harness, seenPorts);
		return IntStream.rangeClosed(1, ConcurrentRequestHarness.rounds())
			.mapToObj((round) -> dynamicTest("round " + round, () -> round(harness, round, seenPorts)));
	}

	// One discarded round through the same route on separate accounts (D-067, BUG-003).
	private void warmUp(ConcurrentRequestHarness harness, Set<Integer> seenPorts) throws Exception {
		long customer = funded(harness);
		long merchant = harness.createAccount("MERCHANT");
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = body(customer, merchant);
		Run run = harness.fire(() -> harness.post("/v1/payments", key, body));
		ServerArrivals.Overlap server = this.arrivals.overlap(key, seenPorts);
		System.out.println("CONCURRENCY payments warmup serverArrivedBeforeFirstCompletion="
				+ server.arrivedBeforeFirstCompletion() + " connections=" + server.connections());
		assertThat(run.responses().stream().filter((r) -> r.exception() != null).map(Response::exception).toList())
			.as("warm-up client exceptions")
			.isEmpty();
	}

	private void round(ConcurrentRequestHarness harness, int round, Set<Integer> seenPorts) throws Exception {
		long customer = funded(harness);
		long merchant = harness.createAccount("MERCHANT");
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = body(customer, merchant);

		Run run = harness.fire(() -> harness.post("/v1/payments", key, body));

		List<Response> responses = run.responses();
		Set<String> bodies = responses.stream()
			.filter((r) -> r.body() != null)
			.map((r) -> HexFormat.of().formatHex(r.body()))
			.collect(Collectors.toSet());
		Map<Integer, Long> statuses = responses.stream()
			.collect(Collectors.groupingBy(Response::status, Collectors.counting()));
		List<Long> authorizes = LedgerTestSupport.transactionIds(this.dataSource, customer, "PAYMENT_AUTHORIZE");
		long payments = LedgerTestSupport.queryLong(this.dataSource,
				"SELECT count(*) FROM payments WHERE customer_account_id = ?", customer);
		ServerArrivals.Overlap server = this.arrivals.overlap(key, seenPorts);
		System.out.println("CONCURRENCY payments round=" + round + " processors="
				+ ConcurrentRequestHarness.processors() + " serverArrivedBeforeFirstCompletion="
				+ server.arrivedBeforeFirstCompletion() + " arrivalSpreadMs=" + server.spreadMillis()
				+ " clientSentBeforeFirst=" + run.sentBeforeFirstResponse() + " payments=" + payments
				+ " authorizes=" + authorizes.size() + " distinctBodies=" + bodies.size() + " statuses=" + statuses);

		String context = "round " + round;
		assertThat(responses.stream().filter((r) -> r.exception() != null).map(Response::exception).toList())
			.as("%s: client exceptions", context)
			.isEmpty();
		assertThat(statuses).as("%s: status counts", context)
			.containsExactly(Map.entry(201, (long) ConcurrentRequestHarness.THREADS));
		assertThat(bodies).as("%s: distinct raw bodies", context).hasSize(1);
		String onlyBody = new String(responses.get(0).body(), StandardCharsets.UTF_8);
		String paymentId = JsonPath.read(onlyBody, "$.payment_id");

		assertThat(payments).as("%s: payments of the customer", context).isEqualTo(1);
		assertThat(authorizes).as("%s: PAYMENT_AUTHORIZE transactions on the customer", context).hasSize(1);
		assertThat(LedgerTestSupport.queryLong(this.dataSource,
				"SELECT count(*) FROM outbox WHERE payload->>'customer_account_id' = ?::text", customer))
			.as("%s: outbox rows naming the customer", context)
			.isEqualTo(1);
		assertThat(LedgerTestSupport.queryLong(this.dataSource,
				"SELECT count(*) FROM outbox WHERE message_key = (SELECT id::text FROM payments WHERE customer_account_id = ?)",
				customer))
			.as("%s: outbox rows keyed by payment %s", context, paymentId)
			.isEqualTo(1);
		KeyRow keyRow = LedgerTestSupport.keyRow(this.dataSource, EdgeProbe.CLIENT_ID, key).orElseThrow();
		assertThat(keyRow.statusCode()).as("%s: stored status", context).isEqualTo(201);
		assertThat(keyRow.responseBody()).as("%s: stored response_body::text", context).isEqualTo(onlyBody);
		assertThat(server.arrivedBeforeFirstCompletion())
			.as("%s: requests that arrived at the server before the first completed (was %d)", context,
					server.arrivedBeforeFirstCompletion())
			.isGreaterThanOrEqualTo(MIN_SERVER_ARRIVED_BEFORE_FIRST_COMPLETION);
	}

	private static long funded(ConcurrentRequestHarness harness) {
		long customer = harness.createAccount("CUSTOMER");
		harness.deposit(customer, "1000.00");
		return customer;
	}

	private static byte[] body(long customer, long merchant) {
		return ("{\"customer_account_id\": " + customer + ", \"merchant_account_id\": " + merchant
				+ ", \"amount\": \"10.00\"}")
			.getBytes(StandardCharsets.UTF_8);
	}

}
