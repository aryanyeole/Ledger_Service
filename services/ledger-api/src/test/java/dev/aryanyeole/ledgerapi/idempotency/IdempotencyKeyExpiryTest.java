package dev.aryanyeole.ledgerapi.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.Assertions.fail;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.postgresql.util.PSQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.NestedTestConfiguration;
import org.springframework.test.context.NestedTestConfiguration.EnclosingConfiguration;
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.jayway.jsonpath.JsonPath;

import dev.aryanyeole.ledgerapi.LedgerTestSupport;
import dev.aryanyeole.ledgerapi.LedgerTestSupport.KeyRow;
import dev.aryanyeole.ledgerapi.TestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// Proof of the idempotency key TTL contract (D-020, D-119, D-128), over real HTTP on this class's
// own context: purge enabled, scheduled off (tests call purgeExpired() directly), batch size 2.
//   1. expires_at is created_at + 24 h, from the database clock;
//   2. a key past expires_at still replays, byte for byte, until it is purged;
//   3. a purge deletes exactly the expired rows, in batches;
//   4. once purged, a key is new: the same request is a second effect, another payload is no 422;
//   5. a purge in flight: the request's key claim waits on the purge's uncommitted DELETE, then
//      owns the key (purge committed), replays it (purge rolled back), or gets 409 (purge outlasts
//      lock_timeout; PurgeOutlastsLockTimeout, its own context);
//   6. V15's guard refuses to delete a live key;
//   7. the purge statement can use idempotency_keys_expires_at;
//   8. a second purger holding rows with the purger's own SELECT ... FOR UPDATE SKIP LOCKED
//      makes purgeExpired() skip them instead of waiting: the two take disjoint rows.
// Deposits carry no transactions.reference, so "one effect" is counted as DEPOSIT transactions on
// a fresh account per key. Rows are backdated only through a ledger_api_owner fixture (D-120);
// every connection asserts current_user (D-036). Each test that backdates a key deletes its own
// leftovers in finally; the purge cases first assert that no other expired rows exist.
// Red evidence (D-038, D-051): flyway.target=14 -> 6; =13 -> 7; select predicate
// expires_at < now() + interval '1 day' -> 3; conflict path deletes an expired row and re-claims
// -> 2; FOR UPDATE SKIP LOCKED -> FOR UPDATE -> 8.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, properties = { EdgeProbe.API_KEYS_PROPERTY,
		"ledger.idempotency.purge.enabled=true", "ledger.idempotency.purge.scheduled=false",
		"ledger.idempotency.purge.batch-size=" + IdempotencyKeyExpiryTest.BATCH_SIZE })
@ExtendWith(OutputCaptureExtension.class)
class IdempotencyKeyExpiryTest {

	static final int BATCH_SIZE = 2;

	static final String CLIENT = EdgeProbe.CLIENT_ID;

	private static final long BLOCK_TIMEOUT_MILLIS = 5000;

	@LocalServerPort
	int port;

	@Autowired
	DataSource dataSource;

	@Autowired
	PostgreSQLContainer postgres;

	@Autowired
	IdempotencyKeyPurger purger;

	private Fixture fx;

	@BeforeEach
	void openFixture() {
		this.fx = new Fixture(this.port, this.dataSource, this.postgres);
	}

	@AfterEach
	void closeFixture() {
		this.fx.close();
	}

	// 1
	@Test
	void ttlIs24HoursFromTheDbClock() throws Exception {
		String key = newKey();
		assertThat(this.fx.deposit(key, this.fx.createAccount(), "10.00").status()).isEqualTo(201);
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource);
				PreparedStatement ps = conn.prepareStatement(
						"SELECT expires_at - created_at = interval '24 hours', (expires_at - created_at)::text "
								+ "FROM idempotency_keys WHERE client_id = ? AND key = ?")) {
			ps.setString(1, CLIENT);
			ps.setString(2, key);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).as("key row").isTrue();
				assertThat(rs.getBoolean(1)).as("expires_at - created_at = 24 hours (was %s)", rs.getString(2))
					.isTrue();
			}
		}
	}

	// 2
	@Test
	void expiredKeyReplaysUntilPurged() throws Exception {
		String key = newKey();
		long account = this.fx.createAccount();
		try {
			Reply owner = this.fx.deposit(key, account, "10.00");
			assertThat(owner.status()).as("owner").isEqualTo(201);
			this.fx.backdate(key);

			Reply replay = this.fx.deposit(key, account, "10.00");
			assertSameResponse(owner, replay);
			assertThat(this.fx.deposits(account)).as("DEPOSIT transactions after the replay").isEqualTo(1);

			KeyRow before = this.fx.keyRow(key).orElseThrow();
			Reply reused = this.fx.deposit(key, account, "11.00");
			assertThat(reused.status()).as("expired key, different payload").isEqualTo(422);
			assertThat(reused.contentType()).startsWith("application/problem+json");
			assertThat(reused.code()).isEqualTo(ProblemCode.IDEMPOTENCY_KEY_REUSED.value());
			assertSameRow(this.fx.keyRow(key).orElseThrow(), before, "after the 422 (not stored)");
			assertThat(this.fx.deposits(account)).as("DEPOSIT transactions after the 422").isEqualTo(1);
		}
		finally {
			this.fx.deleteOwnExpired(List.of(key));
		}
	}

	// 3
	@Test
	void purgeDeletesExactlyTheExpiredRows(CapturedOutput output) throws Exception {
		this.fx.assertNoExpiredKeys();
		long account = this.fx.createAccount();
		List<String> expired = new ArrayList<>();
		List<String> live = new ArrayList<>();
		try {
			for (int i = 0; i < 2 * BATCH_SIZE + 1; i++) {
				expired.add(this.fx.ownedKey(account));
			}
			for (int i = 0; i < 3; i++) {
				live.add(this.fx.ownedKey(account));
			}
			List<KeyRow> liveBefore = new ArrayList<>();
			for (String key : live) {
				liveBefore.add(this.fx.keyRow(key).orElseThrow());
			}
			for (String key : expired) {
				this.fx.backdate(key);
			}
			assertThat(this.fx.expiredKeys()).as("expired keys before the purge")
				.containsExactlyInAnyOrderElementsOf(expired);

			assertThat(this.purger.purgeExpired()).as("keys purged").isEqualTo(5);
			assertThat(output.getOut()).as("purge summary line").contains("Idempotency key purge: deleted=5 batches=3 ");

			for (String key : expired) {
				assertThat(this.fx.keyRow(key)).as("expired key %s", key).isEmpty();
			}
			for (int i = 0; i < live.size(); i++) {
				assertSameRow(this.fx.keyRow(live.get(i)).orElseThrow(), liveBefore.get(i), "live key " + live.get(i));
			}
			assertThat(this.fx.expiredKeys()).as("expired keys after the purge").isEmpty();
		}
		finally {
			this.fx.deleteOwnExpired(expired);
		}
	}

	// 4
	@Test
	void purgedKeyIsNew() throws Exception {
		this.fx.assertNoExpiredKeys();
		String same = newKey();
		String other = newKey();
		long sameAccount = this.fx.createAccount();
		long otherAccount = this.fx.createAccount();
		try {
			Reply first = this.fx.deposit(same, sameAccount, "10.00");
			assertThat(first.status()).isEqualTo(201);
			assertThat(this.fx.deposit(other, otherAccount, "10.00").status()).isEqualTo(201);
			this.fx.backdate(same);
			this.fx.backdate(other);
			assertThat(this.purger.purgeExpired()).as("keys purged").isEqualTo(2);

			Reply again = this.fx.deposit(same, sameAccount, "10.00");
			assertThat(again.status()).as("purged key, same payload").isEqualTo(201);
			assertThat(again.transactionId()).as("a new transaction").isNotEqualTo(first.transactionId());
			assertThat(this.fx.deposits(sameAccount)).as("DEPOSIT transactions for the same payload").isEqualTo(2);

			Reply changed = this.fx.deposit(other, otherAccount, "11.00");
			assertThat(changed.status()).as("purged key, different payload (not 422)").isEqualTo(201);
			assertThat(this.fx.deposits(otherAccount)).as("DEPOSIT transactions for the other payload").isEqualTo(2);
		}
		finally {
			this.fx.deleteOwnExpired(List.of(same, other));
		}
	}

	// 5a: the claim waits on the purge's uncommitted DELETE; the purge commits, the key is gone, and
	// the request's insert becomes the owner.
	@Test
	void purgeInFlightCommitMakesTheRequestOwner() throws Exception {
		String key = newKey();
		long account = this.fx.createAccount();
		Reply owner = this.fx.deposit(key, account, "10.00");
		assertThat(owner.status()).isEqualTo(201);
		try {
			this.fx.backdate(key);
			Future<Reply> request;
			try (Connection holder = LedgerTestSupport.connectApp(this.dataSource)) {
				Fixture.deleteUncommitted(holder, key);
				request = this.fx.requests.submit(() -> this.fx.deposit(key, account, "10.00"));
				this.fx.awaitBlockedOn(Fixture.backendPid(holder));
				holder.commit();
			}
			Reply reply = request.get(30, TimeUnit.SECONDS);
			assertThat(reply.status()).as("after the purge committed").isEqualTo(201);
			assertThat(reply.transactionId()).as("a new transaction").isNotEqualTo(owner.transactionId());
			assertThat(this.fx.deposits(account)).as("DEPOSIT transactions").isEqualTo(2);
			KeyRow row = this.fx.keyRow(key).orElseThrow();
			assertThat(row.statusCode()).isEqualTo(201);
			assertThat(row.responseBody()).isEqualTo(reply.text());
		}
		finally {
			this.fx.deleteOwnExpired(List.of(key));
		}
	}

	// 5b: the purge rolls back; the claim conflicts with the surviving row and replays it.
	@Test
	void purgeInFlightRollbackReplays() throws Exception {
		String key = newKey();
		long account = this.fx.createAccount();
		Reply owner = this.fx.deposit(key, account, "10.00");
		assertThat(owner.status()).isEqualTo(201);
		try {
			this.fx.backdate(key);
			Future<Reply> request;
			try (Connection holder = LedgerTestSupport.connectApp(this.dataSource)) {
				Fixture.deleteUncommitted(holder, key);
				request = this.fx.requests.submit(() -> this.fx.deposit(key, account, "10.00"));
				this.fx.awaitBlockedOn(Fixture.backendPid(holder));
				holder.rollback();
			}
			assertSameResponse(owner, request.get(30, TimeUnit.SECONDS));
			assertThat(this.fx.deposits(account)).as("DEPOSIT transactions").isEqualTo(1);
		}
		finally {
			this.fx.deleteOwnExpired(List.of(key));
		}
	}

	// 6
	@Test
	void liveKeyDeleteIsRefusedByTheGuard() throws Exception {
		String key = this.fx.ownedKey(this.fx.createAccount());
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource)) {
			SQLException e;
			try {
				e = catchThrowableOfType(SQLException.class, () -> {
					try (PreparedStatement ps = conn
						.prepareStatement("DELETE FROM idempotency_keys WHERE client_id = ? AND key = ?")) {
						ps.setString(1, CLIENT);
						ps.setString(2, key);
						ps.executeUpdate();
					}
				});
			}
			finally {
				conn.rollback();
			}
			assertThat((Throwable) e).as("DELETE of a live key must be rejected").isInstanceOf(PSQLException.class);
			assertThat(e.getSQLState()).isEqualTo("23514");
			assertThat(((PSQLException) e).getServerErrorMessage().getConstraint()).isEqualTo(IdempotencyKeyPurger.GUARD);
		}
		assertThat(this.fx.keyRow(key)).as("live key still present").isPresent();
	}

	// 7: enable_seqscan = off proves the index serves this statement's predicate and ORDER BY; it
	// does not prove the planner picks it at production size (same limit as TransactionsListingTest).
	// The statement is the purger's own text, with the batch size as a literal.
	@Test
	void purgeUsesTheExpiresAtIndex() throws Exception {
		assertThat(IdempotencyKeyPurger.PURGE_BATCH.split(":batchSize", -1)).as("one :batchSize parameter").hasSize(2);
		String statement = IdempotencyKeyPurger.PURGE_BATCH.replace(":batchSize", Integer.toString(BATCH_SIZE));
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource)) {
			try {
				try (Statement st = conn.createStatement()) {
					st.execute("SET LOCAL enable_seqscan = off");
				}
				StringBuilder plan = new StringBuilder();
				try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("EXPLAIN " + statement)) {
					while (rs.next()) {
						plan.append(rs.getString(1)).append('\n');
					}
				}
				System.out.println("PURGE plan:\n" + plan);
				assertThat(plan.toString()).as("purge plan").contains("idempotency_keys_expires_at");
			}
			finally {
				conn.rollback();
			}
		}
	}

	// 8: no seam in main. The holder runs the purger's own SELECT (taken from PURGE_BATCH, so it
	// can't drift) and keeps its transaction open; purgeExpired() must skip the held rows and finish
	// while the holder is still open, instead of waiting on them until lock_timeout (5 s here).
	@Test
	void concurrentPurgersTakeDisjointRows() throws Exception {
		this.fx.assertNoExpiredKeys();
		String select = purgerSelect();
		long account = this.fx.createAccount();
		List<String> seeded = new ArrayList<>();
		try {
			for (int i = 0; i < 5; i++) {
				seeded.add(this.fx.ownedKey(account));
			}
			for (String key : seeded) {
				this.fx.backdate(key);
			}
			Set<String> held = new TreeSet<>();
			try (Connection holder = LedgerTestSupport.connectApp(this.dataSource)) {
				try {
					try (Statement st = holder.createStatement();
							ResultSet rs = st.executeQuery(select.replace(":batchSize", Integer.toString(BATCH_SIZE)))) {
						while (rs.next()) {
							assertThat(rs.getString(1)).as("held client_id").isEqualTo(CLIENT);
							held.add(rs.getString(2));
						}
					}
					assertThat(held).as("rows the holder locked").hasSize(BATCH_SIZE).isSubsetOf(seeded);

					long started = System.nanoTime();
					long purged = this.purger.purgeExpired();
					long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
					System.out.println("DISJOINT PURGERS: held=" + held.size() + " purged=" + purged + " elapsedMs="
							+ elapsedMillis);
					assertThat(purged).as("purged while the holder is open").isEqualTo(seeded.size() - BATCH_SIZE);
					assertThat(elapsedMillis).as("purge time (ms), well under lock_timeout").isLessThan(2500);
					assertThat(this.fx.expiredKeys()).as("expired rows left while the holder is open")
						.containsExactlyInAnyOrderElementsOf(held);

					try (PreparedStatement ps = holder
						.prepareStatement("DELETE FROM idempotency_keys WHERE client_id = ? AND key = ANY(?)")) {
						ps.setString(1, CLIENT);
						ps.setArray(2, holder.createArrayOf("text", held.toArray()));
						assertThat(ps.executeUpdate()).as("rows the holder deleted").isEqualTo(BATCH_SIZE);
					}
					holder.commit();
				}
				finally {
					holder.rollback();
				}
			}
			for (String key : seeded) {
				assertThat(this.fx.keyRow(key)).as("seeded key %s", key).isEmpty();
			}
			assertThat(this.fx.expiredKeys()).as("expired keys at the end").isEmpty();
		}
		finally {
			this.fx.deleteOwnExpired(seeded);
		}
	}

	// 5c, in its own context: lock_timeout 500 ms, and a purge that outlasts it. The request's key
	// claim times out waiting on the purge's DELETE (55P03 on the key insert), which D-050 maps to
	// 409 idempotency_request_in_progress, unstored. This is a characterization of that mapping, not
	// a duplicate in flight: the purge, not another request, holds the row. The holder still owns its
	// delete afterwards, and nothing the request did was stored.
	@Nested
	@NestedTestConfiguration(EnclosingConfiguration.OVERRIDE)
	@Import(TestcontainersConfiguration.class)
	@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, properties = { EdgeProbe.API_KEYS_PROPERTY,
			"ledger.idempotency.purge.enabled=true", "ledger.idempotency.purge.scheduled=false",
			"ledger.idempotency.purge.batch-size=" + BATCH_SIZE, "ledger.db.lock-timeout=500ms" })
	class PurgeOutlastsLockTimeout {

		@LocalServerPort
		int port;

		@Autowired
		DataSource dataSource;

		@Autowired
		PostgreSQLContainer postgres;

		@Test
		void requestGets409AndStoresNothing() throws Exception {
			try (Fixture f = new Fixture(this.port, this.dataSource, this.postgres)) {
				String key = newKey();
				long account = f.createAccount();
				assertThat(f.deposit(key, account, "10.00").status()).isEqualTo(201);
				try {
					f.backdate(key);
					try (Connection holder = LedgerTestSupport.connectApp(this.dataSource)) {
						try {
							Fixture.deleteUncommitted(holder, key);
							long started = System.nanoTime();
							Future<Reply> request = f.requests.submit(() -> f.deposit(key, account, "10.00"));
							f.awaitBlockedOn(Fixture.backendPid(holder));
							Reply reply = request.get(30, TimeUnit.SECONDS);
							long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
							System.out.println("PURGE OUTLASTS LOCK TIMEOUT: status=" + reply.status() + " elapsedMs="
									+ elapsedMillis);
							assertThat(reply.status()).as("while the purge holds the row").isEqualTo(409);
							assertThat(reply.contentType()).startsWith("application/problem+json");
							assertThat(reply.code()).isEqualTo(ProblemCode.IDEMPOTENCY_REQUEST_IN_PROGRESS.value());
							assertThat(elapsedMillis).as("waited lock_timeout (ms)").isGreaterThanOrEqualTo(500);
							holder.commit();
						}
						finally {
							holder.rollback();
						}
					}
					assertThat(f.keyRow(key)).as("deleted by the holder's commit").isEmpty();
					assertThat(f.deposits(account)).as("DEPOSIT transactions").isEqualTo(1);
				}
				finally {
					f.deleteOwnExpired(List.of(key));
				}
			}
		}

	}

	// --- helpers ---

	static String newKey() {
		return "exp-" + UUID.randomUUID();
	}

	// The purger's SELECT: the text between "IN (" and the statement's final ")".
	static String purgerSelect() {
		String statement = IdempotencyKeyPurger.PURGE_BATCH;
		int open = statement.indexOf("IN (");
		assertThat(open).as("PURGE_BATCH has an IN (...) subquery").isPositive();
		assertThat(statement).as("PURGE_BATCH ends with the subquery").endsWith(")");
		String select = statement.substring(open + "IN (".length(), statement.length() - 1).strip();
		assertThat(statement).contains(select);
		assertThat(select).as("purger SELECT")
			.startsWith("SELECT client_id, key FROM idempotency_keys WHERE expires_at < now()")
			.contains("ORDER BY expires_at LIMIT :batchSize");
		return select;
	}

	static void assertSameResponse(Reply first, Reply replay) {
		assertThat(replay.status()).as("replay status").isEqualTo(first.status());
		assertThat(replay.contentType()).as("replay content type").isEqualTo(first.contentType());
		assertThat(replay.body()).as("replay body bytes").isEqualTo(first.body());
	}

	static void assertSameRow(KeyRow actual, KeyRow expected, String what) {
		assertThat(actual.requestHash()).as("%s: request_hash", what).isEqualTo(expected.requestHash());
		assertThat(actual.statusCode()).as("%s: status_code", what).isEqualTo(expected.statusCode());
		assertThat(actual.responseBody()).as("%s: response_body", what).isEqualTo(expected.responseBody());
	}

	record Reply(int status, String contentType, byte[] body) {

		String text() {
			return new String(this.body, StandardCharsets.UTF_8);
		}

		String code() {
			return JsonPath.read(text(), "$.code");
		}

		long transactionId() {
			return ((Number) JsonPath.read(text(), "$.transaction_id")).longValue();
		}

	}

	// HTTP and database access for one context: real Tomcat over JDK HttpClient (D-067), app-role
	// reads through LedgerTestSupport, and the owner fixture for backdating (D-120).
	static final class Fixture implements AutoCloseable {

		final ExecutorService requests = Executors.newSingleThreadExecutor();

		private final HttpClient http = HttpClient.newBuilder()
			.version(HttpClient.Version.HTTP_1_1)
			.connectTimeout(Duration.ofSeconds(10))
			.build();

		private final URI base;

		private final DataSource dataSource;

		private final PostgreSQLContainer postgres;

		Fixture(int port, DataSource dataSource, PostgreSQLContainer postgres) {
			this.base = URI.create("http://127.0.0.1:" + port);
			this.dataSource = dataSource;
			this.postgres = postgres;
		}

		Reply post(String path, String key, String json) throws Exception {
			HttpRequest.Builder request = HttpRequest.newBuilder(this.base.resolve(path))
				.timeout(Duration.ofSeconds(30))
				.header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY)
				.header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofString(json));
			if (key != null) {
				request.header("Idempotency-Key", key);
			}
			HttpResponse<byte[]> response = this.http.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
			return new Reply(response.statusCode(), response.headers().firstValue("Content-Type").orElse(null),
					response.body());
		}

		long createAccount() throws Exception {
			Reply reply = post("/v1/accounts", null,
					"{\"name\": \"exp-" + UUID.randomUUID() + "\", \"type\": \"CUSTOMER\"}");
			assertThat(reply.status()).as("account created").isEqualTo(201);
			return ((Number) JsonPath.read(reply.text(), "$.account_id")).longValue();
		}

		Reply deposit(String key, long account, String amount) throws Exception {
			return post("/v1/deposits", key, "{\"account_id\": " + account + ", \"amount\": \"" + amount + "\"}");
		}

		/** A fresh key that owns a 1.00 deposit into the account. */
		String ownedKey(long account) throws Exception {
			String key = newKey();
			assertThat(deposit(key, account, "1.00").status()).as("deposit with %s", key).isEqualTo(201);
			return key;
		}

		long deposits(long account) throws SQLException {
			return LedgerTestSupport.transactionsTouching(this.dataSource, account, "DEPOSIT");
		}

		Optional<KeyRow> keyRow(String key) throws SQLException {
			return LedgerTestSupport.keyRow(this.dataSource, CLIENT, key);
		}

		Set<String> expiredKeys() throws SQLException {
			Set<String> keys = new HashSet<>();
			try (Connection conn = LedgerTestSupport.connectApp(this.dataSource);
					PreparedStatement ps = conn
						.prepareStatement("SELECT client_id || '/' || key FROM idempotency_keys WHERE expires_at < now()");
					ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					String id = rs.getString(1);
					keys.add(id.startsWith(CLIENT + "/") ? id.substring(CLIENT.length() + 1) : id);
				}
			}
			return keys;
		}

		/** Asserted, never cleaned: the context may hold other tests' rows, but none may be expired. */
		void assertNoExpiredKeys() throws SQLException {
			assertThat(expiredKeys()).as("precondition: no expired keys before this test backdates any").isEmpty();
		}

		// expires_at has no runtime grant (D-043); only the owner can move it into the past (D-120).
		void backdate(String key) throws SQLException {
			try (Connection owner = connectOwner()) {
				try (PreparedStatement ps = owner.prepareStatement(
						"UPDATE idempotency_keys SET expires_at = now() - interval '1 hour' WHERE client_id = ? AND key = ?")) {
					ps.setString(1, CLIENT);
					ps.setString(2, key);
					assertThat(ps.executeUpdate()).as("ledger_api_owner: rows backdated for %s", key).isEqualTo(1);
				}
				owner.commit();
			}
		}

		/** Deletes this test's own keys that are still expired; live ones are left alone. */
		void deleteOwnExpired(Collection<String> keys) throws SQLException {
			try (Connection owner = connectOwner()) {
				try (PreparedStatement ps = owner.prepareStatement(
						"DELETE FROM idempotency_keys WHERE client_id = ? AND key = ANY(?) AND expires_at < now()")) {
					ps.setString(1, CLIENT);
					ps.setArray(2, owner.createArrayOf("text", keys.toArray()));
					ps.executeUpdate();
				}
				owner.commit();
			}
		}

		private Connection connectOwner() throws SQLException {
			Connection conn = DriverManager.getConnection(this.postgres.getJdbcUrl(), "ledger_api_owner",
					TestcontainersConfiguration.OWNER_PASSWORD);
			try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT current_user")) {
				assertThat(rs.next()).isTrue();
				assertThat(rs.getString(1)).as("[ledger_api_owner] current_user").isEqualTo("ledger_api_owner");
				conn.setAutoCommit(false);
				return conn;
			}
			catch (SQLException | RuntimeException | Error e) {
				conn.close();
				throw e;
			}
		}

		/** The purge's DELETE of one expired key, left uncommitted on the holder's connection. */
		static void deleteUncommitted(Connection holder, String key) throws SQLException {
			try (PreparedStatement ps = holder
				.prepareStatement("DELETE FROM idempotency_keys WHERE client_id = ? AND key = ?")) {
				ps.setString(1, CLIENT);
				ps.setString(2, key);
				assertThat(ps.executeUpdate()).as("holder deleted the expired key").isEqualTo(1);
			}
		}

		static int backendPid(Connection conn) throws SQLException {
			try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT pg_backend_pid()")) {
				assertThat(rs.next()).isTrue();
				return rs.getInt(1);
			}
		}

		// Polls pg_stat_activity, on its own app-role connection with a fresh snapshot each time,
		// until a backend is blocked by the holder's backend (D-052).
		void awaitBlockedOn(int holderPid) throws Exception {
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
							return;
						}
					}
					Thread.sleep(10);
				}
			}
			fail("request never blocked on the holder (pid " + holderPid + ") within " + BLOCK_TIMEOUT_MILLIS + " ms");
		}

		@Override
		public void close() {
			this.requests.shutdownNow();
			this.http.close();
		}

	}

}
