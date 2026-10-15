package dev.aryanyeole.ledgerapi;

import static dev.aryanyeole.ledgerapi.ApiTestClient.authenticated;
import static dev.aryanyeole.ledgerapi.ApiTestClient.keyedPost;
import static dev.aryanyeole.ledgerapi.ApiTestClient.longAt;
import static dev.aryanyeole.ledgerapi.ApiTestClient.newKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.jayway.jsonpath.JsonPath;

import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.transactions.TransactionCursor;

// GET /v1/transactions (D-053, D-069) against real Postgres. Own context (the marker property
// gives it a fresh container), so a full traversal sees only rows this class writes: other
// suites commit rows a listing can't render, e.g. AccountsApiTest's sub-cent entry (D-056).
// Direct reads use app-role connections asserting current_user (D-036); the one owner
// connection (microseconds test) asserts ledger_api_owner.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.test.context=transactions-listing" })
@AutoConfigureMockMvc
class TransactionsListingTest {

	private static final String OWNER = "ledger_api_owner";

	@Autowired
	MockMvc mvc;

	@Autowired
	DataSource dataSource;

	@Autowired
	PostgreSQLContainer postgres;

	private record Page(List<Map<String, Object>> data, String nextCursor) {

		List<Long> ids() {
			return this.data.stream().map((item) -> ((Number) item.get("transaction_id")).longValue()).toList();
		}

	}

	// --- traversal and entries ---

	@Test
	void traversalReturnsEveryTransactionOnceNewestFirstWithItsEntries() throws Exception {
		long a = account();
		long b = account();
		Set<Long> created = new HashSet<>();
		for (int i = 0; i < 3; i++) {
			created.add(deposit(a, "20.00"));
		}
		created.add(deposit(b, "5.00"));
		for (int i = 0; i < 2; i++) {
			created.add(transfer(a, b, "3.00"));
			created.add(transfer(b, a, "1.00"));
		}

		List<Map<String, Object>> seen = new ArrayList<>();
		Set<Long> seenIds = new HashSet<>();
		String cursor = null;
		int pages = 0;
		do {
			Page page = page("3", cursor);
			assertThat(page.data()).as("page %d size", pages).hasSizeLessThanOrEqualTo(3);
			for (Map<String, Object> item : page.data()) {
				long id = ((Number) item.get("transaction_id")).longValue();
				assertThat(seenIds.add(id)).as("transaction %d repeated across pages", id).isTrue();
				seen.add(item);
			}
			cursor = page.nextCursor();
			pages++;
		}
		while (cursor != null);

		List<Map<String, Object>> mine = seen.stream()
			.filter((item) -> created.contains(((Number) item.get("transaction_id")).longValue()))
			.toList();
		assertThat(mine).as("created transactions in the listing").hasSize(created.size());
		assertThat(mine).as("created transactions ordered by (created_at, id) descending")
			.isSortedAccordingTo(Comparator
				.comparing((Map<String, Object> item) -> OffsetDateTime.parse((String) item.get("created_at")))
				.thenComparing((item) -> ((Number) item.get("transaction_id")).longValue())
				.reversed());
		for (Map<String, Object> item : mine) {
			assertEntries(item);
		}
	}

	// --- equal timestamps ---

	@Test
	void equalCreatedAtIsOrderedByIdAcrossAPageBoundary() throws Exception {
		long a = account();
		long b = account();
		deposit(a, "10.00");
		long first;
		long second;
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource)) {
			try {
				first = rawTransfer(conn, null, a, b, "1.00");
				second = rawTransfer(conn, null, a, b, "2.00");
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}
		assertThat(createdAt(first)).as("both postings share created_at").isEqualTo(createdAt(second));

		Page top = page("1", null);
		assertThat(top.ids()).as("newest: the higher id").containsExactly(second);
		Page next = page("1", top.nextCursor());
		assertThat(next.ids()).as("same created_at, next by id descending").containsExactly(first);
	}

	// --- microseconds ---

	// The two postings get explicit created_at values 500 µs apart, inserted as
	// ledger_api_owner with an explicit commit (the balance and append-only triggers still
	// run). The timestamps are set only to make the sub-millisecond boundary deterministic.
	// Runtime roles lose INSERT on created_at in T9b; this test does not rely on them having it.
	@Test
	void subMillisecondNeighboursStraddlingAPageAreNeitherSkippedNorRepeated() throws Exception {
		long a = account();
		long b = account();
		deposit(a, "10.00");
		OffsetDateTime earlier = OffsetDateTime.parse("2026-01-01T00:00:00.000100Z");
		OffsetDateTime later = OffsetDateTime.parse("2026-01-01T00:00:00.000600Z");
		long earlierId;
		long laterId;
		try (Connection owner = connectOwner()) {
			try {
				earlierId = rawTransfer(owner, earlier, a, b, "1.00");
				laterId = rawTransfer(owner, later, a, b, "1.00");
				owner.commit();
			}
			finally {
				owner.rollback();
			}
		}

		String above = TransactionCursor.encode(OffsetDateTime.parse("2026-01-01T00:00:00.001Z"), Long.MAX_VALUE);
		Page first = page("1", above);
		assertThat(first.ids()).as("page 1 below 00:00:00.001").containsExactly(laterId);
		assertThat(first.nextCursor()).as("page 1 has a next cursor").isNotNull();
		TransactionCursor.Position position = TransactionCursor.decode(first.nextCursor());
		assertThat(position.createdAt().toInstant()).as("cursor created_at, to the microsecond")
			.isEqualTo(later.toInstant());
		assertThat(position.id()).isEqualTo(laterId);

		Page second = page("1", first.nextCursor());
		assertThat(second.ids()).as("page 2: the neighbour 500 µs earlier").containsExactly(earlierId);
		assertThat(second.nextCursor()).as("nothing older").isNull();
	}

	// --- validation ---

	@ParameterizedTest(name = "limit=[{0}]")
	@ValueSource(strings = { "0", "201", "abc", "", "+5", "5.0", "-1", "1000" })
	void invalidLimitIs400(String limit) throws Exception {
		this.mvc.perform(authenticated(get("/v1/transactions").param("limit", limit)))
			.andExpect(status().isBadRequest())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_LIMIT.value()));
	}

	@ParameterizedTest(name = "cursor=[{0}]")
	@ValueSource(strings = { "", "not-base64!", "djE6MToy==", "djI6MToy", "djE6eDox", "djE6MTow", "djE6MTotMQ",
			"djE6MToxOjE" })
	void invalidCursorIs400(String cursor) throws Exception {
		this.mvc.perform(authenticated(get("/v1/transactions").param("cursor", cursor)))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_CURSOR.value()));
	}

	@Test
	void limitBoundsAndDefaultAreAccepted() throws Exception {
		for (String limit : new String[] { "1", "200" }) {
			this.mvc.perform(authenticated(get("/v1/transactions").param("limit", limit))).andExpect(status().isOk());
		}
		this.mvc.perform(authenticated(get("/v1/transactions"))).andExpect(status().isOk());
	}

	@Test
	void missingApiKeyIs401() throws Exception {
		this.mvc.perform(get("/v1/transactions"))
			.andExpect(status().isUnauthorized())
			.andExpect(jsonPath("$.code").value(ProblemCode.MISSING_API_KEY.value()));
	}

	// --- index (D-053); failing first at -Dspring.flyway.target=6 (D-038) ---

	// Literal values, not bind parameters: a parameterized EXPLAIN plans generically.
	@Test
	void indexServesFilterAndOrder() throws Exception {
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource)) {
			try {
				try (Statement st = conn.createStatement()) {
					st.execute("SET LOCAL enable_sort = off");
				}
				StringBuilder plan = new StringBuilder();
				try (Statement st = conn.createStatement();
						ResultSet rs = st.executeQuery("""
								EXPLAIN SELECT id, kind, reference, created_at FROM transactions
								WHERE (created_at, id) < ('2026-10-14T00:00:00Z'::timestamptz, 9223372036854775807)
								ORDER BY created_at DESC, id DESC LIMIT 51""")) {
					while (rs.next()) {
						plan.append(rs.getString(1)).append('\n');
					}
				}
				System.out.println("LISTING plan:\n" + plan);
				assertThat(plan.toString()).as("page query plan")
					.contains("transactions_created_at_id_desc")
					.doesNotContain("Sort");
			}
			finally {
				conn.rollback();
			}
		}
	}

	// --- late commit: characterization of a known limitation (D-053) ---

	// created_at is the posting transaction's start time. T_late starts after T_a commits and
	// before T_b starts, but commits after a traversal has passed T_late's position, so that
	// traversal never sees it; a fresh traversal does. Fresh account pairs for all three, and
	// T_late is a transfer (not a deposit), so the open transaction blocks nobody.
	@Test
	void lateCommitBehindTheCursorIsNotSeenByThatTraversal() throws Exception {
		long a1 = fundedAccount();
		long b1 = account();
		long a2 = fundedAccount();
		long b2 = account();
		long a3 = fundedAccount();
		long b3 = account();

		long tA = transfer(a1, b1, "1.00");
		try (Connection late = LedgerTestSupport.connectApp(this.dataSource)) {
			try {
				long tLate = rawTransfer(late, null, a2, b2, "1.00");
				long tB = transfer(a3, b3, "1.00");

				Page first = page("2", null);
				assertThat(first.ids()).as("page 1 while T_late is uncommitted").containsExactly(tB, tA);

				late.commit();
				assertThat(createdAt(tLate)).as("T_late sits between T_a and T_b").isAfter(createdAt(tA))
					.isBefore(createdAt(tB));

				Page second = page("200", first.nextCursor());
				assertThat(second.ids()).as("page 2 from page 1's cursor (known limitation, D-053)")
					.doesNotContain(tLate);

				Page fresh = page("2", null);
				assertThat(fresh.ids()).as("a fresh page 1 shows T_late").containsExactly(tB, tLate);
			}
			finally {
				late.rollback();
			}
		}
	}

	// --- helpers ---

	private Page page(String limit, String cursor) throws Exception {
		MockHttpServletRequestBuilder request = authenticated(get("/v1/transactions")).param("limit", limit);
		if (cursor != null) {
			request.param("cursor", cursor);
		}
		MvcResult result = this.mvc.perform(request)
			.andExpect(status().isOk())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
			.andReturn();
		String json = result.getResponse().getContentAsString();
		assertThat((Object) JsonPath.read(json, "$.next_cursor")).as("next_cursor present").satisfiesAnyOf(
				(value) -> assertThat(value).isNull(), (value) -> assertThat(value).isInstanceOf(String.class));
		return new Page(JsonPath.read(json, "$.data"), JsonPath.read(json, "$.next_cursor"));
	}

	@SuppressWarnings("unchecked")
	private void assertEntries(Map<String, Object> item) throws SQLException {
		long id = ((Number) item.get("transaction_id")).longValue();
		Map<Long, BigDecimal> expected = LedgerTestSupport.entries(this.dataSource, id);
		List<Map<String, Object>> entries = (List<Map<String, Object>>) item.get("entries");
		Map<Long, String> actual = new LinkedHashMap<>();
		entries.forEach((entry) -> actual.put(((Number) entry.get("account_id")).longValue(), (String) entry.get("amount")));
		Map<Long, String> expectedStrings = new LinkedHashMap<>();
		expected.forEach((account, amount) -> expectedStrings.put(account,
				amount.setScale(2, RoundingMode.UNNECESSARY).toPlainString()));
		assertThat(actual).as("entries of %d, in entry-id order, amounts at scale 2", id)
			.containsExactlyEntriesOf(expectedStrings);
	}

	private long account() throws Exception {
		return ApiTestClient.createAccount(this.mvc, "CUSTOMER");
	}

	private long fundedAccount() throws Exception {
		long id = account();
		deposit(id, "10.00");
		return id;
	}

	private long deposit(long accountId, String amount) throws Exception {
		MvcResult result = this.mvc
			.perform(keyedPost("/v1/deposits", newKey(), "{\"account_id\": " + accountId + ", \"amount\": \"" + amount + "\"}"))
			.andExpect(status().isCreated())
			.andReturn();
		return longAt(result, "$.transaction_id");
	}

	private long transfer(long from, long to, String amount) throws Exception {
		MvcResult result = this.mvc
			.perform(keyedPost("/v1/transfers", newKey(),
					"{\"from_account_id\": " + from + ", \"to_account_id\": " + to + ", \"amount\": \"" + amount + "\"}"))
			.andExpect(status().isCreated())
			.andReturn();
		return longAt(result, "$.transaction_id");
	}

	// A balanced TRANSFER posting on this connection: transactions row, accounts locked in
	// ascending id order, entries and balances in ascending id order (invariants 1 and 4).
	// createdAt null -> the column default (the transaction's start time).
	private static long rawTransfer(Connection conn, OffsetDateTime createdAt, long from, long to, String amount)
			throws SQLException {
		long txId;
		String insert = (createdAt == null) ? "INSERT INTO transactions (kind) VALUES ('TRANSFER') RETURNING id"
				: "INSERT INTO transactions (kind, created_at) VALUES ('TRANSFER', ?) RETURNING id";
		try (PreparedStatement ps = conn.prepareStatement(insert)) {
			if (createdAt != null) {
				ps.setObject(1, createdAt);
			}
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				txId = rs.getLong(1);
			}
		}
		try (PreparedStatement ps = conn
			.prepareStatement("SELECT id FROM accounts WHERE id = ANY (?) ORDER BY id FOR UPDATE")) {
			ps.setArray(1, conn.createArrayOf("bigint", new Object[] { from, to }));
			ps.executeQuery().close();
		}
		BigDecimal value = new BigDecimal(amount);
		Map<Long, BigDecimal> legs = new java.util.TreeMap<>(Map.of(from, value.negate(), to, value));
		try (PreparedStatement entry = conn
			.prepareStatement("INSERT INTO entries (transaction_id, account_id, amount) VALUES (?, ?, ?)");
				PreparedStatement balance = conn
					.prepareStatement("UPDATE accounts SET balance = balance + ? WHERE id = ?")) {
			for (Map.Entry<Long, BigDecimal> leg : legs.entrySet()) {
				entry.setLong(1, txId);
				entry.setLong(2, leg.getKey());
				entry.setBigDecimal(3, leg.getValue());
				assertThat(entry.executeUpdate()).isEqualTo(1);
			}
			for (Map.Entry<Long, BigDecimal> leg : legs.entrySet()) {
				balance.setBigDecimal(1, leg.getValue());
				balance.setLong(2, leg.getKey());
				assertThat(balance.executeUpdate()).isEqualTo(1);
			}
		}
		return txId;
	}

	private OffsetDateTime createdAt(long transactionId) throws SQLException {
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource);
				PreparedStatement ps = conn.prepareStatement("SELECT created_at FROM transactions WHERE id = ?")) {
			ps.setLong(1, transactionId);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getObject(1, OffsetDateTime.class);
			}
		}
	}

	// Owner connection, autoCommit off; asserts the session really is ledger_api_owner (D-036).
	private Connection connectOwner() throws SQLException {
		Connection conn = DriverManager.getConnection(this.postgres.getJdbcUrl(), OWNER,
				TestcontainersConfiguration.OWNER_PASSWORD);
		try (PreparedStatement ps = conn.prepareStatement("SELECT current_user");
				ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).isTrue();
			assertThat(rs.getString(1)).as("current_user").isEqualTo(OWNER);
			conn.setAutoCommit(false);
			return conn;
		}
		catch (SQLException | RuntimeException | Error e) {
			conn.close();
			throw e;
		}
	}

}
