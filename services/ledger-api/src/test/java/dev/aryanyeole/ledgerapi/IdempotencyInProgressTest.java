package dev.aryanyeole.ledgerapi;

import static dev.aryanyeole.ledgerapi.ApiTestClient.keyedPost;
import static dev.aryanyeole.ledgerapi.ApiTestClient.newKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.jayway.jsonpath.JsonPath;

import dev.aryanyeole.ledgerapi.LedgerTestSupport.KeyRow;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.idempotency.Fingerprint;
import dev.aryanyeole.ledgerapi.money.Money;

// Proof (D-052): a key held uncommitted by another transaction. The request's key insert
// blocks on the holder's uncommitted unique-index entry; each HTTP case first observes that
// block in pg_stat_activity (pg_blocking_pids lists the holder's backend), then resolves the
// holder: never (-> 409 idempotency_request_in_progress after lock_timeout, nothing stored),
// commit (-> the request replays the holder's stored response), or roll back (-> the request
// becomes the owner, 201). lock_timeout is 1s here. The 503 path is LockContentionTest (T4).
// Holders and the poller are app-role connections asserting current_user (D-036).
// Red evidence (D-051): map the key-insert 55P03 to 503 lock_contention -> case b fails.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.db.lock-timeout=1s" })
@AutoConfigureMockMvc
class IdempotencyInProgressTest {

	private static final String CLIENT = EdgeProbe.CLIENT_ID;

	private static final String ROUTE = "/v1/transfers";

	private static final long BLOCK_TIMEOUT_MILLIS = 5000;

	// Same components as TransfersController.TransferRequest, so the same canonical JSON and
	// fingerprint (pinned by TransferRequestFingerprintTest).
	record TransferShape(Long fromAccountId, Long toAccountId, Money amount) {
	}

	@Autowired
	MockMvc mvc;

	@Autowired
	DataSource dataSource;

	private ExecutorService requests;

	@BeforeEach
	void startRequestThread() {
		this.requests = Executors.newSingleThreadExecutor();
	}

	@AfterEach
	void stopRequestThread() {
		this.requests.shutdownNow();
	}

	// a. Database level: the second insert of an uncommitted key times out with 55P03.
	@Test
	void secondInsertOfAnUncommittedKeyTimesOut() throws Exception {
		String key = newKey();
		try (Connection holder = LedgerTestSupport.connectApp(this.dataSource);
				Connection waiter = LedgerTestSupport.connectApp(this.dataSource)) {
			try {
				insertKey(holder, key, new byte[32]);
				try (Statement st = waiter.createStatement()) {
					st.execute("SET LOCAL lock_timeout = '1s'");
				}
				long started = System.nanoTime();
				SQLException e = catchThrowableOfType(SQLException.class, () -> insertKey(waiter, key, new byte[32]));
				long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
				assertThat((Throwable) e).as("second insert of an uncommitted key").isNotNull();
				Throwable root = e;
				while (root.getCause() != null) {
					root = root.getCause();
				}
				assertThat(root).isInstanceOf(PSQLException.class);
				assertThat(((PSQLException) root).getSQLState()).as("root SQLState").isEqualTo("55P03");
				assertThat(((PSQLException) root).getServerErrorMessage().getMessage()).as("server message")
					.contains("lock timeout");
				assertThat(elapsedMillis).as("waited lock_timeout (ms)").isGreaterThanOrEqualTo(1000);
			}
			finally {
				waiter.rollback();
				holder.rollback();
			}
		}
	}

	// b. The holder never finishes: 409 after lock_timeout, nothing stored, key still usable.
	@Test
	void heldKeyIs409AfterLockTimeout() throws Exception {
		long a = funded("100.00");
		long b = account();
		String key = newKey();
		try (Connection holder = LedgerTestSupport.connectApp(this.dataSource)) {
			int holderPid = backendPid(holder);
			insertKey(holder, key, fingerprint(a, b));

			long started = System.nanoTime();
			Future<Timed> request = this.requests.submit(() -> timedTransfer(key, a, b));
			long blockedAt = awaitBlockedOn(holderPid);
			Timed timed = request.get(30, TimeUnit.SECONDS);
			long elapsedMillis = (timed.completedNanos() - started) / 1_000_000;
			System.out.println("IN-PROGRESS case b: blockedAfterMs=" + (blockedAt - started) / 1_000_000
					+ " elapsedMs=" + elapsedMillis + " status=" + timed.result().getResponse().getStatus());

			assertThat(timed.result().getResponse().getStatus()).as("status while the key is held").isEqualTo(409);
			assertThat(MediaType.parseMediaType(timed.result().getResponse().getContentType())
				.isCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)).as("409 content type is problem+json").isTrue();
			assertThat((String) JsonPath.read(timed.result().getResponse().getContentAsString(), "$.code"))
				.isEqualTo(ProblemCode.IDEMPOTENCY_REQUEST_IN_PROGRESS.value());
			assertThat(timed.completedNanos()).as("409 arrived after the observed block").isGreaterThan(blockedAt);
			assertThat(elapsedMillis).as("waited lock_timeout (ms)").isGreaterThanOrEqualTo(1000);
			assertThat(transfersOn(a)).as("TRANSFER transactions while held").isZero();
			holder.rollback();
		}

		this.mvc.perform(keyedPost(ROUTE, key, body(a, b))).andExpect(result -> assertThat(
				result.getResponse().getStatus()).as("same request after the holder rolled back").isEqualTo(201));
		assertThat(transfersOn(a)).as("TRANSFER transactions after retry").isEqualTo(1);
	}

	// c. The holder commits a complete key: the waiting request replays the holder's response.
	@Test
	void holderCommitIsReplayedToTheWaitingRequest() throws Exception {
		long a = funded("100.00");
		long b = account();
		String key = newKey();
		long started;
		long blockedAt;
		Future<Timed> request;
		try (Connection holder = LedgerTestSupport.connectApp(this.dataSource)) {
			int holderPid = backendPid(holder);
			insertKey(holder, key, fingerprint(a, b));
			try (PreparedStatement ps = holder.prepareStatement(
					"UPDATE idempotency_keys SET status_code = 201, response_body = ?::jsonb WHERE client_id = ? AND key = ?")) {
				ps.setString(1, "{\"held_by\": \"test\", \"transaction_id\": 0}");
				ps.setString(2, CLIENT);
				ps.setString(3, key);
				assertThat(ps.executeUpdate()).isEqualTo(1);
			}

			started = System.nanoTime();
			request = this.requests.submit(() -> timedTransfer(key, a, b));
			blockedAt = awaitBlockedOn(holderPid);
			holder.commit();
		}
		Timed timed = request.get(30, TimeUnit.SECONDS);
		System.out.println("IN-PROGRESS case c: blockedAfterMs=" + (blockedAt - started) / 1_000_000 + " elapsedMs="
				+ (timed.completedNanos() - started) / 1_000_000 + " status=" + timed.result().getResponse().getStatus());

		KeyRow row = LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow();
		assertThat(timed.result().getResponse().getStatus()).as("status after the holder committed").isEqualTo(201);
		assertThat(timed.result().getResponse().getContentAsString()).as("replayed body")
			.isEqualTo(row.responseBody())
			.isEqualTo("{\"held_by\": \"test\", \"transaction_id\": 0}");
		assertThat(transfersOn(a)).as("TRANSFER transactions posted by the request").isZero();
	}

	// d. The holder rolls back: the waiting request claims the key and posts as the owner.
	@Test
	void holderRollbackMakesTheWaitingRequestTheOwner() throws Exception {
		long a = funded("100.00");
		long b = account();
		String key = newKey();
		long started;
		long blockedAt;
		Future<Timed> request;
		try (Connection holder = LedgerTestSupport.connectApp(this.dataSource)) {
			int holderPid = backendPid(holder);
			insertKey(holder, key, fingerprint(a, b));

			started = System.nanoTime();
			request = this.requests.submit(() -> timedTransfer(key, a, b));
			blockedAt = awaitBlockedOn(holderPid);
			holder.rollback();
		}
		Timed timed = request.get(30, TimeUnit.SECONDS);
		System.out.println("IN-PROGRESS case d: blockedAfterMs=" + (blockedAt - started) / 1_000_000 + " elapsedMs="
				+ (timed.completedNanos() - started) / 1_000_000 + " status=" + timed.result().getResponse().getStatus());

		assertThat(timed.result().getResponse().getStatus()).as("status after the holder rolled back").isEqualTo(201);
		assertThat(transfersOn(a)).as("TRANSFER transactions").isEqualTo(1);
		KeyRow row = LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow();
		assertThat(row.statusCode()).as("stored status").isEqualTo(201);
		assertThat(row.responseBody()).as("stored response_body").isEqualTo(timed.result().getResponse().getContentAsString());
	}

	// --- helpers ---

	private record Timed(MvcResult result, long completedNanos) {
	}

	private Timed timedTransfer(String key, long from, long to) throws Exception {
		MvcResult result = this.mvc.perform(keyedPost(ROUTE, key, body(from, to))).andReturn();
		return new Timed(result, System.nanoTime());
	}

	// Polls pg_stat_activity, on its own app-role connection with a fresh snapshot each time,
	// until a backend is blocked by the holder's backend. Returns System.nanoTime() of the
	// first observation.
	private long awaitBlockedOn(int holderPid) throws Exception {
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
						return System.nanoTime();
					}
				}
				Thread.sleep(10);
			}
		}
		return fail("request never blocked on the holder (pid " + holderPid + ") within " + BLOCK_TIMEOUT_MILLIS + " ms");
	}

	private static int backendPid(Connection conn) throws SQLException {
		try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT pg_backend_pid()")) {
			assertThat(rs.next()).isTrue();
			return rs.getInt(1);
		}
	}

	private static void insertKey(Connection conn, String key, byte[] requestHash) throws SQLException {
		try (PreparedStatement ps = conn
			.prepareStatement("INSERT INTO idempotency_keys (client_id, key, request_hash) VALUES (?, ?, ?) ON CONFLICT DO NOTHING")) {
			ps.setString(1, CLIENT);
			ps.setString(2, key);
			ps.setBytes(3, requestHash);
			assertThat(ps.executeUpdate()).as("key row inserted").isEqualTo(1);
		}
	}

	private static byte[] fingerprint(long from, long to) {
		return Fingerprint.of("POST", ROUTE, new TransferShape(from, to, new Money(new BigDecimal("10.00"))));
	}

	private long account() throws Exception {
		return ApiTestClient.createAccount(this.mvc, "CUSTOMER");
	}

	private long funded(String amount) throws Exception {
		long id = account();
		ApiTestClient.fund(this.mvc, id, amount);
		return id;
	}

	private long transfersOn(long accountId) throws Exception {
		return LedgerTestSupport.transactionsTouching(this.dataSource, accountId, "TRANSFER");
	}

	private static String body(long from, long to) {
		return "{\"from_account_id\": " + from + ", \"to_account_id\": " + to + ", \"amount\": \"10.00\"}";
	}

}
