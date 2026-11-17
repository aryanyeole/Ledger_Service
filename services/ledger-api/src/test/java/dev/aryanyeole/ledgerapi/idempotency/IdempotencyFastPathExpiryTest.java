package dev.aryanyeole.ledgerapi.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.postgresql.PostgreSQLContainer;

import io.micrometer.core.instrument.MeterRegistry;

import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness;
import dev.aryanyeole.ledgerapi.ConcurrentRequestHarness.Response;
import dev.aryanyeole.ledgerapi.LedgerTestSupport;
import dev.aryanyeole.ledgerapi.RedisTestSupport;
import dev.aryanyeole.ledgerapi.RedisTestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.TestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.cache.RedisGateway;

// Proof that a purged idempotency key never replays from Redis (D-139 with D-020, D-119). With a
// 1 s TTL and a 2 s margin, an entry is written only while the row has more than 3 s left, measured
// by the DB in the statement that returns the text, so it always expires before the row can be
// purged. (g1) a row backdated to now() + 5 s is cached on replay, its entry expires, the purge
// deletes the row, and the same request becomes a new owner; (g2) a row with 2 s left is replayed
// from the DB and not cached. Its own context: a 1 s TTL would race IdempotencyFastPathTest's hits.
// Rows are backdated only through the ledger_api_owner fixture (D-120); the purge runs on demand
// (enabled, not scheduled, D-126).
@Import({ TestcontainersConfiguration.class, RedisTestcontainersConfiguration.class })
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
		properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.cache.idempotency.enabled=true",
				"ledger.cache.idempotency.ttl=1s", "ledger.cache.idempotency.margin=2s",
				"ledger.idempotency.purge.enabled=true", "ledger.idempotency.purge.scheduled=false" })
class IdempotencyFastPathExpiryTest {

	private static final String TRANSFERS = "/v1/transfers";

	@LocalServerPort
	int port;

	@Autowired
	RedisGateway gateway;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	DataSource dataSource;

	@Autowired
	PostgreSQLContainer postgres;

	@Autowired
	MeterRegistry meters;

	@Autowired
	IdempotencyKeyPurger purger;

	@Autowired
	LedgerIdempotencyCacheProperties cacheProperties;

	private ConcurrentRequestHarness harness;

	@BeforeEach
	void setUp() {
		RedisTestSupport.awaitGatewayReady(this.gateway);
		this.harness = new ConcurrentRequestHarness(this.port);
	}

	// (g1)
	@Test
	void aPurgedKeyIsANewOwnerBecauseItsEntryExpiredFirst() throws Exception {
		long a = funded("100.00");
		long b = this.harness.createAccount("CUSTOMER");
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = transfer(a, b);

		Response owner = send(key, body);
		assertThat(owner.status()).isEqualTo(201);
		awaitEntryGone(key);

		backdate(key, 5);
		double written = populates("written");
		double missesBefore = reads("miss");
		Response replay = send(key, body);
		assertThat(replay.status()).as("replay before expiry").isEqualTo(201);
		assertThat(replay.body()).isEqualTo(owner.body());
		assertThat(reads("miss") - missesBefore).as("read{miss} for the replay").isEqualTo(1);
		assertThat(populates("written") - written).as("populate{written}: 5 s > ttl + margin").isEqualTo(1);

		await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100)).until(() -> expired(key));
		this.purger.purgeExpired();
		assertThat(keyRows(key)).as("row purged").isZero();

		double missesBeforeNew = reads("miss");
		Response newOwner = send(key, body);
		assertThat(newOwner.status()).as("the same key and body after the purge").isEqualTo(201);
		assertThat(reads("miss") - missesBeforeNew).as("read{miss}: the entry expired before the row was purged")
			.isEqualTo(1);
		assertThat(newOwner.body()).as("a new owner's response, not the old one").isNotEqualTo(owner.body());
		assertThat(LedgerTestSupport.transactionIds(this.dataSource, a, "TRANSFER")).as("a second transaction")
			.hasSize(2);
	}

	// (g2)
	@Test
	void aRowWithLessThanTtlPlusMarginLeftIsReplayedButNotCached() throws Exception {
		long a = funded("100.00");
		long b = this.harness.createAccount("CUSTOMER");
		String key = ConcurrentRequestHarness.newKey();
		byte[] body = transfer(a, b);

		Response owner = send(key, body);
		assertThat(owner.status()).isEqualTo(201);
		// Otherwise the replay below would be a hit on the owner's own entry.
		awaitEntryGone(key);

		backdate(key, 2);
		double skipped = populates("skipped_short_life");
		Response replay = send(key, body);
		assertThat(replay.status()).isEqualTo(201);
		assertThat(replay.body()).isEqualTo(owner.body());
		assertThat(populates("skipped_short_life") - skipped).as("populate{skipped_short_life}: 2 s < ttl + margin")
			.isEqualTo(1);
		assertThat(rawEntry(key)).as("no entry").isNull();
	}

	// Bounded at ttl + 5 s, so a run with a longer TTL (the g1 mutation uses 10 s) still gets past it.
	private void awaitEntryGone(String key) {
		await().atMost(this.cacheProperties.ttl().plusSeconds(5))
			.pollInterval(Duration.ofMillis(50))
			.until(() -> pttl(key) == -2);
	}

	// The D-120 fixture: only ledger_api_owner can move expires_at (D-043); V16 leaves it writable.
	private void backdate(String key, int secondsFromNow) throws SQLException {
		try (Connection owner = DriverManager.getConnection(this.postgres.getJdbcUrl(), "ledger_api_owner",
				TestcontainersConfiguration.OWNER_PASSWORD)) {
			try (PreparedStatement who = owner.prepareStatement("SELECT current_user"); ResultSet rs = who.executeQuery()) {
				assertThat(rs.next()).isTrue();
				assertThat(rs.getString(1)).as("current_user").isEqualTo("ledger_api_owner");
			}
			try (PreparedStatement ps = owner.prepareStatement(
					"UPDATE idempotency_keys SET expires_at = now() + make_interval(secs => ?) WHERE client_id = ? AND key = ?")) {
				ps.setInt(1, secondsFromNow);
				ps.setString(2, EdgeProbe.CLIENT_ID);
				ps.setString(3, key);
				assertThat(ps.executeUpdate()).as("rows backdated").isEqualTo(1);
			}
		}
	}

	private boolean expired(String key) {
		return this.jdbc
			.sql("SELECT expires_at < now() FROM idempotency_keys WHERE client_id = :client AND key = :key")
			.param("client", EdgeProbe.CLIENT_ID)
			.param("key", key)
			.query(Boolean.class)
			.single();
	}

	private long keyRows(String key) {
		return this.jdbc.sql("SELECT count(*) FROM idempotency_keys WHERE client_id = :client AND key = :key")
			.param("client", EdgeProbe.CLIENT_ID)
			.param("key", key)
			.query(Long.class)
			.single();
	}

	private Response send(String key, byte[] body) {
		return this.harness.send(this.harness.post(TRANSFERS, key, body));
	}

	private long funded(String amount) {
		long account = this.harness.createAccount("CUSTOMER");
		this.harness.deposit(account, amount);
		return account;
	}

	private static byte[] transfer(long from, long to) {
		return ("{\"from_account_id\": " + from + ", \"to_account_id\": " + to + ", \"amount\": \"10.00\"}")
			.getBytes(StandardCharsets.UTF_8);
	}

	private String rawEntry(String key) {
		RedisGateway.Reply<String> reply = this.gateway.optional("GET",
				(r) -> r.get(IdempotencyCache.key(EdgeProbe.CLIENT_ID, key)));
		assertThat(reply.available()).isTrue();
		return reply.value();
	}

	private long pttl(String key) {
		RedisGateway.Reply<Long> reply = this.gateway.optional("PTTL",
				(r) -> r.pttl(IdempotencyCache.key(EdgeProbe.CLIENT_ID, key)));
		assertThat(reply.available()).isTrue();
		return reply.value();
	}

	private double reads(String result) {
		return this.meters.counter("ledger.idempotency.cache.read", "result", result).count();
	}

	private double populates(String result) {
		return this.meters.counter("ledger.idempotency.cache.populate", "result", result).count();
	}

}
