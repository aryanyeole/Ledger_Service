package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
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
import org.springframework.http.MediaType;

import com.jayway.jsonpath.JsonPath;

import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Response;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Run;
import dev.aryanyeole.ledgerapi.LedgerTestSupport.KeyRow;
import dev.aryanyeole.ledgerapi.LedgerTestSupport.Totals;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;

// Proof (P2, spec §1): 100 identical transfers fired at once over real HTTP create exactly
// one transaction, and all 100 clients get byte-identical 201s. The first request's key
// insert wins the unique index; the others block on its uncommitted entry (or wait for a
// pool connection), then see the committed row and replay its stored text (D-044). Runs
// through Tomcat and the Hikari pool (D-067). Its negative control,
// negativecontrols/CheckThenInsertIdempotencyTest, uses the same harness and goes red (D-017).
// Rounds: -Dledger.test.concurrency.rounds (CI 1, local soak 20); fresh accounts and key each.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, properties = EdgeProbe.API_KEYS_PROPERTY)
class ConcurrentIdempotentTransferTest {

	private static final int MIN_PEAK_IN_FLIGHT = 50;

	@LocalServerPort
	int port;

	@Autowired
	DataSource dataSource;

	@Autowired
	WebServerApplicationContext webServerContext;

	@TestFactory
	Stream<DynamicTest> hundredIdenticalTransfersCreateOneTransaction() throws Exception {
		System.out.println("CONCURRENCY real config " + ConcurrentRequestHarness.serverLimits(this.dataSource,
				this.webServerContext) + " rounds=" + ConcurrentRequestHarness.rounds());
		ConcurrentRequestHarness harness = new ConcurrentRequestHarness(this.port);
		return IntStream.rangeClosed(1, ConcurrentRequestHarness.rounds())
			.mapToObj((round) -> dynamicTest("round " + round, () -> round(harness, round)));
	}

	private void round(ConcurrentRequestHarness harness, int round) throws Exception {
		long a = harness.createAccount("CUSTOMER");
		long b = harness.createAccount("CUSTOMER");
		harness.deposit(a, "1000.00");
		assertThat(LedgerTestSupport.transactionIds(this.dataSource, a, "TRANSFER")).as("baseline on A").isEmpty();
		assertThat(LedgerTestSupport.transactionIds(this.dataSource, b, "TRANSFER")).as("baseline on B").isEmpty();

		String key = ConcurrentRequestHarness.newKey();
		byte[] body = ("{\"from_account_id\": " + a + ", \"to_account_id\": " + b + ", \"amount\": \"10.00\"}")
			.getBytes(StandardCharsets.UTF_8);
		Run run = harness.fire(() -> harness.post("/v1/transfers", key, body));

		List<Response> responses = run.responses();
		Set<String> bodies = responses.stream()
			.filter((r) -> r.body() != null)
			.map((r) -> HexFormat.of().formatHex(r.body()))
			.collect(Collectors.toSet());
		List<Long> transfersA = LedgerTestSupport.transactionIds(this.dataSource, a, "TRANSFER");
		List<Long> transfersB = LedgerTestSupport.transactionIds(this.dataSource, b, "TRANSFER");
		Map<Integer, Long> statuses = responses.stream()
			.collect(Collectors.groupingBy(Response::status, Collectors.counting()));
		System.out.println("CONCURRENCY real round=" + round + " peakInFlight=" + run.peakInFlight() + " transactions="
				+ transfersA.size() + " distinctBodies=" + bodies.size() + " statuses=" + statuses);

		String context = "round " + round;
		assertThat(responses).as("%s: responses", context).hasSize(ConcurrentRequestHarness.THREADS);
		assertThat(responses.stream().filter((r) -> r.exception() != null).map(Response::exception).toList())
			.as("%s: client exceptions", context)
			.isEmpty();
		assertThat(statuses).as("%s: status counts (no 409, no 5xx)", context)
			.containsExactly(Map.entry(201, (long) ConcurrentRequestHarness.THREADS));
		Set<String> contentTypes = responses.stream().map(Response::contentType).collect(Collectors.toSet());
		assertThat(contentTypes).as("%s: Content-Type values, all identical", context).hasSize(1);
		assertThat(MediaType.parseMediaType(contentTypes.iterator().next()).isCompatibleWith(MediaType.APPLICATION_JSON))
			.as("%s: Content-Type %s compatible with application/json", context, contentTypes)
			.isTrue();
		assertThat(bodies).as("%s: distinct raw bodies", context).hasSize(1);

		byte[] onlyBody = responses.get(0).body();
		long transactionId = ((Number) JsonPath.read(new String(onlyBody, StandardCharsets.UTF_8), "$.transaction_id"))
			.longValue();
		assertThat(transfersA).as("%s: TRANSFER transactions on A", context).containsExactly(transactionId);
		assertThat(transfersB).as("%s: TRANSFER transactions on B", context).containsExactly(transactionId);
		assertThat(LedgerTestSupport.entries(this.dataSource, transactionId)).as("%s: entries", context)
			.containsExactlyInAnyOrderEntriesOf(Map.of(a, new BigDecimal("-10.0000"), b, new BigDecimal("10.0000")));

		KeyRow keyRow = LedgerTestSupport.keyRow(this.dataSource, EdgeProbe.CLIENT_ID, key).orElseThrow();
		assertThat(LedgerTestSupport.keyRowCount(this.dataSource, EdgeProbe.CLIENT_ID, key)).as("%s: key rows", context)
			.isEqualTo(1);
		assertThat(keyRow.statusCode()).as("%s: stored status", context).isEqualTo(201);
		assertThat(keyRow.responseBody()).as("%s: stored response_body::text", context)
			.isEqualTo(new String(onlyBody, StandardCharsets.UTF_8));

		assertTotals(a, "990.0000", context);
		assertTotals(b, "10.0000", context);
		assertThat(run.peakInFlight()).as("%s: peak client in-flight requests (was %d)", context, run.peakInFlight())
			.isGreaterThanOrEqualTo(MIN_PEAK_IN_FLIGHT);
	}

	private void assertTotals(long accountId, String expected, String context) throws Exception {
		Totals totals = LedgerTestSupport.totals(this.dataSource, accountId);
		assertThat(totals.balance()).as("%s: account %d balance", context, accountId).isEqualByComparingTo(expected);
		assertThat(totals.entrySum()).as("%s: account %d entry sum", context, accountId)
			.isEqualByComparingTo(totals.balance());
	}

}
