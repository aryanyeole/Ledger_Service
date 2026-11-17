package dev.aryanyeole.ledgerapi.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import io.micrometer.core.instrument.MeterRegistry;

import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Response;
import dev.aryanyeole.ledgerapi.RedisTestSupport;
import dev.aryanyeole.ledgerapi.RedisTestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.TestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.cache.RedisGateway;

// The fast path end to end, as a smoke test (T4); the proof is IdempotencyFastPathTest (T5). With
// the idempotency cache on: the owner's response is cached as the DB's own response_body::text, the
// next request is a hit with byte-identical bytes, a stored 422 replays from Redis as problem+json,
// and a keyed account creation's hit carries the same Location (D-063: the controller derives it
// from the body either way). The gateway is warmed first (BUG-016).
@Import({ TestcontainersConfiguration.class, RedisTestcontainersConfiguration.class })
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
		properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.cache.idempotency.enabled=true" })
class IdempotencyFastPathSmokeTest {

	@LocalServerPort
	int port;

	@Autowired
	RedisGateway gateway;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	MeterRegistry meters;

	private ConcurrentRequestHarness harness;

	@BeforeEach
	void setUp() {
		RedisTestSupport.awaitGatewayReady(this.gateway);
		this.harness = new ConcurrentRequestHarness(this.port);
	}

	@Test
	void theOwnersResponseIsCachedAsTheDbTextAndTheNextRequestIsAByteIdenticalHit() throws Exception {
		System.out.println("FAST_PATH meterRegistry=" + this.meters.getClass().getName());
		long a = this.harness.createAccount("CUSTOMER");
		long b = this.harness.createAccount("CUSTOMER");
		this.harness.deposit(a, "100.00");
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = transfer(a, b, "10.00");

		Response owner = this.harness.send(this.harness.post("/v1/transfers", key, body));
		assertThat(owner.status()).isEqualTo(201);
		String dbText = storedText(key);
		IdempotencyCache.Entry entry = cachedEntry(key);
		assertThat(entry.text()).as("cached text = response_body::text").isEqualTo(dbText);
		assertThat(entry.status()).isEqualTo(201);
		assertThat(new String(owner.body(), StandardCharsets.UTF_8)).isEqualTo(dbText);

		double hitsBefore = reads("hit");
		Response replay = this.harness.send(this.harness.post("/v1/transfers", key, body));
		assertThat(reads("hit")).as("read{hit}").isEqualTo(hitsBefore + 1);
		assertThat(replay.status()).isEqualTo(201);
		assertThat(replay.contentType()).isEqualTo("application/json");
		assertThat(replay.body()).as("byte-identical").isEqualTo(owner.body());
	}

	@Test
	void aStoredInsufficientFundsReplaysFromRedisAsProblemJson() throws Exception {
		long a = this.harness.createAccount("CUSTOMER");
		long b = this.harness.createAccount("CUSTOMER");
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = transfer(a, b, "10.00");

		Response owner = this.harness.send(this.harness.post("/v1/transfers", key, body));
		assertThat(owner.status()).isEqualTo(422);
		assertThat(cachedEntry(key).status()).isEqualTo(422);

		double hitsBefore = reads("hit");
		Response replay = this.harness.send(this.harness.post("/v1/transfers", key, body));
		assertThat(reads("hit")).isEqualTo(hitsBefore + 1);
		assertThat(replay.status()).isEqualTo(422);
		assertThat(replay.contentType()).isEqualTo("application/problem+json");
		assertThat(replay.body()).isEqualTo(owner.body());
		assertThat(new String(replay.body(), StandardCharsets.UTF_8)).contains("insufficient_funds");
	}

	@Test
	void aKeyedAccountCreationHitCarriesTheSameLocation() throws Exception {
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = ("{\"name\": \"fast-path-" + key + "\", \"type\": \"CUSTOMER\"}").getBytes(StandardCharsets.UTF_8);
		HttpClient client = HttpClient.newHttpClient();
		HttpResponse<byte[]> owner = client.send(this.harness.post("/v1/accounts", key, body),
				HttpResponse.BodyHandlers.ofByteArray());
		assertThat(owner.statusCode()).isEqualTo(201);

		double hitsBefore = reads("hit");
		HttpResponse<byte[]> replay = client.send(this.harness.post("/v1/accounts", key, body),
				HttpResponse.BodyHandlers.ofByteArray());
		assertThat(reads("hit")).isEqualTo(hitsBefore + 1);
		assertThat(replay.statusCode()).isEqualTo(201);
		assertThat(replay.body()).isEqualTo(owner.body());
		assertThat(replay.headers().firstValue("Location")).isPresent().isEqualTo(owner.headers().firstValue("Location"));
	}

	private static byte[] transfer(long from, long to, String amount) {
		return ("{\"from_account_id\": " + from + ", \"to_account_id\": " + to + ", \"amount\": \"" + amount + "\"}")
			.getBytes(StandardCharsets.UTF_8);
	}

	private String storedText(String key) {
		return this.jdbc
			.sql("SELECT response_body::text FROM idempotency_keys WHERE client_id = :client AND key = :key")
			.param("client", EdgeProbe.CLIENT_ID)
			.param("key", key)
			.query(String.class)
			.single();
	}

	private IdempotencyCache.Entry cachedEntry(String key) throws IdempotencyCache.Unparseable {
		RedisGateway.Reply<String> reply = this.gateway.optional("GET",
				(r) -> r.get(IdempotencyCache.key(EdgeProbe.CLIENT_ID, key)));
		assertThat(reply.available()).isTrue();
		assertThat(reply.value()).as("cache entry for %s", key).isNotNull();
		return IdempotencyCache.parse(reply.value());
	}

	private double reads(String result) {
		return this.meters.counter("ledger.idempotency.cache.read", "result", result).count();
	}

}
