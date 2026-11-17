package dev.aryanyeole.ledgerapi.accounts;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.jayway.jsonpath.JsonPath;

import io.micrometer.core.instrument.MeterRegistry;

import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Response;
import dev.aryanyeole.ledgerapi.RedisTestSupport;
import dev.aryanyeole.ledgerapi.RedisTestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.TestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.cache.BalanceCacheScripts;
import dev.aryanyeole.ledgerapi.cache.RedisGateway;

// The balance cache end to end, as a smoke test (T6); the proof is BalanceCacheTest (T7). With the
// balance cache on and its own Redis: a miss then a byte-identical hit (X-Cache miss -> hit); after a
// transfer the next read shows it and is a miss (read-your-writes through the post-commit
// invalidation, D-147); a 404 is never cached, so the account created at that id reads 200; and each
// posting (deposit, transfer, authorize) sends one invalidation. The relay is off in tests and an
// authorize writes only an outbox row, so no Kafka. The gateway is warmed first (BUG-016).
@Import({ TestcontainersConfiguration.class, RedisTestcontainersConfiguration.class })
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
		properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.cache.balance.enabled=true" })
class BalanceCacheSmokeTest {

	@LocalServerPort
	int port;

	@Autowired
	RedisGateway gateway;

	@Autowired
	MeterRegistry meters;

	@Autowired
	PostgreSQLContainer postgres;

	private ConcurrentRequestHarness harness;

	private final HttpClient client = HttpClient.newHttpClient();

	@BeforeEach
	void setUp() {
		RedisTestSupport.awaitGatewayReady(this.gateway);
		this.harness = new ConcurrentRequestHarness(this.port);
	}

	@Test
	void aMissThenAByteIdenticalHit() throws Exception {
		long account = this.harness.createAccount("CUSTOMER");
		this.harness.deposit(account, "100.00");

		HttpResponse<byte[]> miss = balance(account);
		assertThat(miss.statusCode()).isEqualTo(200);
		assertThat(miss.headers().firstValue("X-Cache")).hasValue("miss");
		assertThat(rawValue(account)).as("populated").matches("1\\|100\\.0000\\|\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{6}Z");

		HttpResponse<byte[]> hit = balance(account);
		assertThat(hit.statusCode()).isEqualTo(200);
		assertThat(hit.headers().firstValue("X-Cache")).hasValue("hit");
		assertThat(hit.headers().firstValue("Content-Type")).isEqualTo(miss.headers().firstValue("Content-Type"));
		assertThat(hit.body()).as("byte-identical: %s", text(miss)).isEqualTo(miss.body());
		assertThat((String) JsonPath.read(text(hit), "$.balance")).isEqualTo("100.00");
	}

	@Test
	void theReadAfterATransferShowsItAndIsAMiss() throws Exception {
		long from = this.harness.createAccount("CUSTOMER");
		long to = this.harness.createAccount("CUSTOMER");
		this.harness.deposit(from, "100.00");
		balance(from);
		assertThat(balance(from).headers().firstValue("X-Cache")).hasValue("hit");

		double sent = invalidations("sent");
		Response transfer = this.harness.send(this.harness.post("/v1/transfers", ConcurrentRequestHarness.newKey(),
				("{\"from_account_id\": " + from + ", \"to_account_id\": " + to + ", \"amount\": \"30.00\"}")
					.getBytes(StandardCharsets.UTF_8)));
		assertThat(transfer.status()).isEqualTo(201);
		assertThat(invalidations("sent") - sent).as("one invalidation for the transfer").isEqualTo(1);

		HttpResponse<byte[]> after = balance(from);
		assertThat(after.headers().firstValue("X-Cache")).as("read-your-writes").hasValue("miss");
		assertThat((String) JsonPath.read(text(after), "$.balance")).isEqualTo("70.00");
		assertThat((String) JsonPath.read(text(balance(to)), "$.balance")).isEqualTo("30.00");
	}

	@Test
	void a404IsNotCachedSoTheAccountCreatedAtThatIdReads200() throws Exception {
		long next = nextAccountId();
		double misses = reads("miss");
		double written = populates("written");

		HttpResponse<byte[]> notFound = balance(next);
		assertThat(notFound.statusCode()).isEqualTo(404);
		assertThat(reads("miss") - misses).isEqualTo(1);
		assertThat(populates("written")).as("a 404 populates nothing").isEqualTo(written);
		assertThat(rawValue(next)).isNull();

		long created = this.harness.createAccount("CUSTOMER");
		assertThat(created).as("the account got the id that was just a 404").isEqualTo(next);
		HttpResponse<byte[]> found = balance(created);
		assertThat(found.statusCode()).isEqualTo(200);
		assertThat(found.headers().firstValue("X-Cache")).hasValue("miss");
		assertThat((String) JsonPath.read(text(found), "$.balance")).isEqualTo("0.00");
	}

	@Test
	void everyPostingSendsOneInvalidation() throws Exception {
		long customer = this.harness.createAccount("CUSTOMER");
		long merchant = this.harness.createAccount("MERCHANT");

		double sent = invalidations("sent");
		this.harness.deposit(customer, "100.00");
		assertThat(invalidations("sent") - sent).as("deposit").isEqualTo(1);

		balance(customer);
		assertThat(rawValue(customer)).isNotNull();
		Response authorize = this.harness.send(this.harness.post("/v1/payments", ConcurrentRequestHarness.newKey(),
				("{\"customer_account_id\": " + customer + ", \"merchant_account_id\": " + merchant
						+ ", \"amount\": \"10.00\"}")
					.getBytes(StandardCharsets.UTF_8)));
		assertThat(authorize.status()).as("authorize: %s", new String(authorize.body(), StandardCharsets.UTF_8))
			.isEqualTo(201);
		assertThat(invalidations("sent") - sent).as("deposit + authorize").isEqualTo(2);
		assertThat(rawValue(customer)).as("the authorize deleted the customer's value").isNull();
		assertThat(invalidations("failed")).isZero();
	}

	private HttpResponse<byte[]> balance(long id) throws Exception {
		HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + this.port + "/v1/accounts/" + id + "/balance"))
			.timeout(Duration.ofSeconds(10))
			.header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY)
			.GET()
			.build();
		return this.client.send(request, HttpResponse.BodyHandlers.ofByteArray());
	}

	private static String text(HttpResponse<byte[]> response) {
		return new String(response.body(), StandardCharsets.UTF_8);
	}

	private String rawValue(long id) {
		RedisGateway.Reply<String> reply = this.gateway.optional("GET", (r) -> r.get(BalanceCacheScripts.valueKey(id)));
		assertThat(reply.available()).isTrue();
		return reply.value();
	}

	// The identity's next value, read as the owner (the runtime role can't read the sequence). Nothing
	// else creates accounts in this context while a test runs.
	private long nextAccountId() throws Exception {
		try (Connection owner = DriverManager.getConnection(this.postgres.getJdbcUrl(), "ledger_api_owner",
				TestcontainersConfiguration.OWNER_PASSWORD);
				PreparedStatement ps = owner.prepareStatement(
						"SELECT pg_sequence_last_value(pg_get_serial_sequence('accounts', 'id')::regclass)");
				ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).isTrue();
			long last = rs.getLong(1);
			return rs.wasNull() ? 1 : last + 1;
		}
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

}
