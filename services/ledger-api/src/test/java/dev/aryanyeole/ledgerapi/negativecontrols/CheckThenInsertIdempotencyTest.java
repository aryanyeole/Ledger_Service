package dev.aryanyeole.ledgerapi.negativecontrols;

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

import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Response;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Run;
import dev.aryanyeole.ledgerapi.LedgerTestSupport;
import dev.aryanyeole.ledgerapi.LedgerTestSupport.Totals;
import dev.aryanyeole.ledgerapi.TestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;

// NEGATIVE CONTROL (D-017) for ConcurrentIdempotentTransferTest: the same harness, gate,
// request shape and amounts against a check-then-insert variant (NaiveTransferController)
// must produce duplicate transfers. If it ever produces exactly one, the harness created no
// real overlap and a green proof would mean nothing. Only idempotency is broken here: every
// duplicate is a balanced posting and balances still equal their entry sums, and the unique
// key still allows one key row, which shows the key alone doesn't prevent duplicates when it
// commits outside the effect's transaction.
@Import({ TestcontainersConfiguration.class, NaiveTransferController.class })
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, properties = EdgeProbe.API_KEYS_PROPERTY)
class CheckThenInsertIdempotencyTest {

	@LocalServerPort
	int port;

	@Autowired
	DataSource dataSource;

	@Autowired
	WebServerApplicationContext webServerContext;

	@TestFactory
	Stream<DynamicTest> naiveIdempotencyLetsDuplicatesThrough() throws Exception {
		System.out.println("CONCURRENCY control config " + ConcurrentRequestHarness.serverLimits(this.dataSource,
				this.webServerContext) + " rounds=" + ConcurrentRequestHarness.rounds());
		ConcurrentRequestHarness harness = new ConcurrentRequestHarness(this.port);
		return IntStream.rangeClosed(1, ConcurrentRequestHarness.rounds())
			.mapToObj((round) -> dynamicTest("round " + round, () -> round(harness, round)));
	}

	private void round(ConcurrentRequestHarness harness, int round) throws Exception {
		long a = harness.createAccount("CUSTOMER");
		long b = harness.createAccount("CUSTOMER");
		harness.deposit(a, "1000.00");

		String key = ConcurrentRequestHarness.newKey();
		byte[] body = ("{\"from_account_id\": " + a + ", \"to_account_id\": " + b + ", \"amount\": \"10.00\"}")
			.getBytes(StandardCharsets.UTF_8);
		Run run = harness.fire(() -> harness.post(NaiveTransferController.ROUTE, key, body));

		List<Response> responses = run.responses();
		Set<String> bodies = responses.stream()
			.filter((r) -> r.body() != null)
			.map((r) -> HexFormat.of().formatHex(r.body()))
			.collect(Collectors.toSet());
		List<Long> transfers = LedgerTestSupport.transactionIds(this.dataSource, a, "TRANSFER");
		int n = transfers.size();
		Map<Integer, Long> statuses = responses.stream()
			.collect(Collectors.groupingBy(Response::status, Collectors.counting()));
		System.out.println("CONCURRENCY control round=" + round + " peakInFlight=" + run.peakInFlight()
				+ " transactions=" + n + " distinctBodies=" + bodies.size() + " statuses=" + statuses);

		String context = "round " + round;
		assertThat(statuses).as("%s: status counts", context)
			.containsExactly(Map.entry(201, (long) ConcurrentRequestHarness.THREADS));
		assertThat(n).as("%s: harness produced no overlap (new TRANSFER transactions = %d)", context, n)
			.isGreaterThan(1);
		assertThat(bodies.size()).as("%s: distinct raw bodies (was %d)", context, bodies.size()).isGreaterThan(1);

		assertThat(LedgerTestSupport.transactionIds(this.dataSource, b, "TRANSFER")).as("%s: TRANSFERs on B", context)
			.isEqualTo(transfers);
		for (long transactionId : transfers) {
			Map<Long, BigDecimal> entries = LedgerTestSupport.entries(this.dataSource, transactionId);
			assertThat(entries).as("%s: entries of %d", context, transactionId)
				.containsExactlyInAnyOrderEntriesOf(Map.of(a, new BigDecimal("-10.0000"), b, new BigDecimal("10.0000")));
			assertThat(entries.values().stream().reduce(BigDecimal.ZERO, BigDecimal::add))
				.as("%s: entry sum of %d", context, transactionId)
				.isEqualByComparingTo("0");
		}
		BigDecimal moved = new BigDecimal("10.00").multiply(BigDecimal.valueOf(n));
		assertTotals(a, new BigDecimal("1000.00").subtract(moved), context);
		assertTotals(b, moved, context);
		assertThat(LedgerTestSupport.keyRowCount(this.dataSource, EdgeProbe.CLIENT_ID, key)).as("%s: key rows", context)
			.isEqualTo(1);
	}

	private void assertTotals(long accountId, BigDecimal expected, String context) throws Exception {
		Totals totals = LedgerTestSupport.totals(this.dataSource, accountId);
		assertThat(totals.balance()).as("%s: account %d balance", context, accountId).isEqualByComparingTo(expected);
		assertThat(totals.entrySum()).as("%s: account %d entry sum", context, accountId)
			.isEqualByComparingTo(totals.balance());
	}

}
