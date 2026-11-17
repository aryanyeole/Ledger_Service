package dev.aryanyeole.ledgerapi.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

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

import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Response;
import dev.aryanyeole.ledgerapi.LedgerTestSupport;
import dev.aryanyeole.ledgerapi.RedisTestSupport;
import dev.aryanyeole.ledgerapi.RedisTestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.TestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.cache.RedisGateway;

// D-141's hook is wired where it claims to be: inside the owner's transaction, after its response is
// stored. Armed once for one key, it throws there; the client gets 500, nothing of the request
// survives (no key row, no transfer, no cache entry), and the retry becomes the owner. The full
// rollback proof is T5's case (f).
@Import({ TestcontainersConfiguration.class, RedisTestcontainersConfiguration.class,
		IdempotencyFaultHookWiringTest.Hooks.class })
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
		properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.cache.idempotency.enabled=true" })
class IdempotencyFaultHookWiringTest {

	@LocalServerPort
	int port;

	@Autowired
	RedisGateway gateway;

	@Autowired
	ArmableHook hook;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	DataSource dataSource;

	@BeforeEach
	void warm() {
		RedisTestSupport.awaitGatewayReady(this.gateway);
	}

	@Test
	void aFailureAfterTheStoreRollsTheOwnerBackAndTheRetryBecomesTheOwner() throws Exception {
		ConcurrentRequestHarness harness = new ConcurrentRequestHarness(this.port);
		long a = harness.createAccount("CUSTOMER");
		long b = harness.createAccount("CUSTOMER");
		harness.deposit(a, "100.00");
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = ("{\"from_account_id\": " + a + ", \"to_account_id\": " + b + ", \"amount\": \"10.00\"}")
			.getBytes(StandardCharsets.UTF_8);
		this.hook.armed.set(key);

		Response failed = harness.send(harness.post("/v1/transfers", key, body));
		assertThat(failed.status()).as("the hook threw after the store").isEqualTo(500);
		assertThat(this.hook.armed.get()).as("the hook fired").isNull();
		assertThat(keyRows(key)).as("idempotency_keys rows after the rollback").isZero();
		assertThat(LedgerTestSupport.transactionIds(this.dataSource, a, "TRANSFER")).isEmpty();
		assertThat(this.gateway.optional("GET", (r) -> r.get(IdempotencyCache.key(EdgeProbe.CLIENT_ID, key))).value())
			.as("no cache entry")
			.isNull();

		Response retry = harness.send(harness.post("/v1/transfers", key, body));
		assertThat(retry.status()).as("the retry becomes the owner").isEqualTo(201);
		assertThat(LedgerTestSupport.transactionIds(this.dataSource, a, "TRANSFER")).hasSize(1);
		assertThat(keyRows(key)).isEqualTo(1);
	}

	private long keyRows(String key) {
		return this.jdbc.sql("SELECT count(*) FROM idempotency_keys WHERE client_id = :client AND key = :key")
			.param("client", EdgeProbe.CLIENT_ID)
			.param("key", key)
			.query(Long.class)
			.single();
	}

	// Throws once, for the armed key only.
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
