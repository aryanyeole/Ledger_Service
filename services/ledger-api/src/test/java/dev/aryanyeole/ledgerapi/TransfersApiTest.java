package dev.aryanyeole.ledgerapi;

import static dev.aryanyeole.ledgerapi.ApiTestClient.assertSameResponse;
import static dev.aryanyeole.ledgerapi.ApiTestClient.authenticated;
import static dev.aryanyeole.ledgerapi.ApiTestClient.keyedPost;
import static dev.aryanyeole.ledgerapi.ApiTestClient.longAt;
import static dev.aryanyeole.ledgerapi.ApiTestClient.newKey;
import static dev.aryanyeole.ledgerapi.LedgerTestSupport.APP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import dev.aryanyeole.ledgerapi.LedgerTestSupport.KeyRow;
import dev.aryanyeole.ledgerapi.LedgerTestSupport.Totals;
import dev.aryanyeole.ledgerapi.api.CorrelationIdFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// POST /v1/transfers against real Postgres (D-044, D-045, D-064, D-066). Accounts are created
// and funded through the public endpoints. The API runs as ledger_api_app; direct checks use
// the same role (D-036). System accounts are shared, so they are checked by deltas (BUG-002).
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = EdgeProbe.API_KEYS_PROPERTY)
@AutoConfigureMockMvc
class TransfersApiTest {

	private static final String CLIENT = EdgeProbe.CLIENT_ID;

	@Autowired
	MockMvc mvc;

	@Autowired
	DataSource dataSource;

	// --- 201 and replay ---

	@Test
	void transferPostsOneBalancedTransaction() throws Exception {
		long from = funded("CUSTOMER", "50.00");
		long to = account("MERCHANT");
		String correlationId = "t5-" + UUID.randomUUID().toString().replace("-", "");

		MvcResult result = this.mvc
			.perform(transfer(newKey(), body(from, to, "20.00")).header(CorrelationIdFilter.HEADER, correlationId))
			.andExpect(status().isCreated())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
			.andExpect(jsonPath("$.kind").value("TRANSFER"))
			.andExpect(jsonPath("$.from_account_id").value(from))
			.andExpect(jsonPath("$.to_account_id").value(to))
			.andExpect(jsonPath("$.amount").value("20.00"))
			.andExpect(jsonPath("$.created_at").isString())
			.andReturn();
		assertThat(result.getResponse().getHeader("Location")).as("Location on a transfer").isNull();
		long transactionId = longAt(result, "$.transaction_id");

		assertThat(transfersOn(from)).as("TRANSFER transactions on from").isEqualTo(1);
		assertThat(transfersOn(to)).as("TRANSFER transactions on to").isEqualTo(1);
		assertThat(kindAndCorrelation(transactionId)).containsExactly("TRANSFER", correlationId);
		assertThat(LedgerTestSupport.entries(this.dataSource, transactionId)).as("[%s] entries", APP)
			.containsExactlyInAnyOrderEntriesOf(Map.of(from, new BigDecimal("-20.0000"), to, new BigDecimal("20.0000")));
		assertBalance(from, "30.00");
		assertBalance(to, "20.00");
	}

	// Locks and entries follow ascending account id, not request order (invariant 4).
	@Test
	void transferFromHigherToLowerIdWritesEntriesInAscendingId() throws Exception {
		long to = account("CUSTOMER");
		long from = funded("CUSTOMER", "9.00");
		assertThat(from).as("from id above to id").isGreaterThan(to);

		MvcResult result = this.mvc.perform(transfer(newKey(), body(from, to, "4.00")))
			.andExpect(status().isCreated())
			.andReturn();
		Map<Long, BigDecimal> entries = LedgerTestSupport.entries(this.dataSource, longAt(result, "$.transaction_id"));
		assertThat(new ArrayList<>(entries.keySet())).as("[%s] entry account ids in insert order", APP)
			.containsExactly(to, from);
		assertThat(entries.get(from)).isEqualByComparingTo("-4.00");
		assertThat(entries.get(to)).isEqualByComparingTo("4.00");
		assertBalance(from, "5.00");
		assertBalance(to, "4.00");
	}

	@Test
	void replayWithSameBodyIsByteIdentical() throws Exception {
		long from = funded("CUSTOMER", "10.00");
		long to = account("CUSTOMER");
		String key = newKey();
		MvcResult first = this.mvc.perform(transfer(key, body(from, to, "3.00"))).andExpect(status().isCreated())
			.andReturn();
		MvcResult replay = this.mvc.perform(transfer(key, body(from, to, "3.00"))).andReturn();

		assertSameResponse(first, replay);
		assertThat(transfersOn(from)).as("TRANSFER transactions on from").isEqualTo(1);
		assertBalance(from, "7.00");
	}

	@Test
	void sameKeyDifferentAmountIs422() throws Exception {
		long from = funded("CUSTOMER", "10.00");
		long to = account("CUSTOMER");
		String key = newKey();
		this.mvc.perform(transfer(key, body(from, to, "3.00"))).andExpect(status().isCreated());
		KeyRow before = LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow();

		this.mvc.perform(transfer(key, body(from, to, "4.00")))
			.andExpect(status().isUnprocessableContent())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.IDEMPOTENCY_KEY_REUSED.value()));

		KeyRow after = LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow();
		assertThat(after.requestHash()).isEqualTo(before.requestHash());
		assertThat(after.responseBody()).isEqualTo(before.responseBody());
		assertThat(transfersOn(from)).isEqualTo(1);
		assertBalance(from, "7.00");
	}

	// --- funds (PostingService's check, D-064) ---

	@Test
	void insufficientFundsIsStored422() throws Exception {
		long from = funded("CUSTOMER", "10.00");
		long to = account("CUSTOMER");
		Totals fromBefore = LedgerTestSupport.totals(this.dataSource, from);
		Totals toBefore = LedgerTestSupport.totals(this.dataSource, to);
		String key = newKey();

		MvcResult first = this.mvc.perform(transfer(key, body(from, to, "10.01")))
			.andExpect(status().isUnprocessableContent())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.INSUFFICIENT_FUNDS.value()))
			.andReturn();
		assertThat(LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow().statusCode())
			.as("stored status").isEqualTo(422);
		assertThat(transfersOn(from)).as("TRANSFER transactions on from").isZero();
		assertThat(transfersOn(to)).as("TRANSFER transactions on to").isZero();
		LedgerTestSupport.assertMoved(this.dataSource, from, fromBefore, "0");
		LedgerTestSupport.assertMoved(this.dataSource, to, toBefore, "0");

		MvcResult replay = this.mvc.perform(transfer(key, body(from, to, "10.01"))).andReturn();
		assertSameResponse(first, replay);
	}

	@Test
	void exactDrainIsAllowed() throws Exception {
		long from = funded("CUSTOMER", "12.34");
		long to = account("MERCHANT");
		this.mvc.perform(transfer(newKey(), body(from, to, "12.34"))).andExpect(status().isCreated());
		this.mvc.perform(authenticated(get("/v1/accounts/{id}/balance", from)))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.balance").value("0.00"));
		assertBalance(to, "12.34");
	}

	// --- 400: decided before the transaction, nothing stored ---

	@Test
	void sameAccountIs400AndStoresNothing() throws Exception {
		long account = funded("CUSTOMER", "5.00");
		long keyRowsBefore = LedgerTestSupport.keyRowCount(this.dataSource, CLIENT);
		this.mvc.perform(transfer(newKey(), body(account, account, "1.00")))
			.andExpect(status().isBadRequest())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.SAME_ACCOUNT.value()));
		assertThat(LedgerTestSupport.keyRowCount(this.dataSource, CLIENT)).isEqualTo(keyRowsBefore);
		assertThat(transfersOn(account)).isZero();
		assertBalance(account, "5.00");
	}

	@Test
	void missingIdempotencyKeyIs400() throws Exception {
		long from = funded("CUSTOMER", "5.00");
		long to = account("CUSTOMER");
		long keyRowsBefore = LedgerTestSupport.keyRowCount(this.dataSource, CLIENT);
		this.mvc
			.perform(authenticated(post("/v1/transfers")).contentType(MediaType.APPLICATION_JSON)
				.content(body(from, to, "1.00")))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.MISSING_IDEMPOTENCY_KEY.value()));
		assertThat(LedgerTestSupport.keyRowCount(this.dataSource, CLIENT)).isEqualTo(keyRowsBefore);
		assertThat(transfersOn(from)).isZero();
	}

	// D-065: each id must be a positive JSON integer; amounts stay on the D-047 codec.
	@ParameterizedTest(name = "{0} = {1}")
	@CsvSource(delimiter = '|', textBlock = """
			from_account_id | "ID"
			from_account_id | ID.5
			from_account_id | 0
			from_account_id | ABSENT
			to_account_id   | "ID"
			to_account_id   | ID.0
			to_account_id   | -1
			to_account_id   | ABSENT
			""")
	void idsMustBePositiveJsonIntegers(String field, String value) throws Exception {
		long from = funded("CUSTOMER", "5.00");
		long to = account("CUSTOMER");
		String fromJson = field.equals("from_account_id") ? value.replace("ID", Long.toString(from)) : Long.toString(from);
		String toJson = field.equals("to_account_id") ? value.replace("ID", Long.toString(to)) : Long.toString(to);
		StringBuilder json = new StringBuilder("{\"amount\": \"1.00\"");
		if (!fromJson.equals("ABSENT")) {
			json.append(", \"from_account_id\": ").append(fromJson);
		}
		if (!toJson.equals("ABSENT")) {
			json.append(", \"to_account_id\": ").append(toJson);
		}
		this.mvc.perform(transfer(newKey(), json.append('}').toString()))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_REQUEST.value()));
		assertThat(transfersOn(from)).isZero();
	}

	@Test
	void numericAmountIsInvalidAmount() throws Exception {
		long from = funded("CUSTOMER", "5.00");
		long to = account("CUSTOMER");
		this.mvc
			.perform(transfer(newKey(), "{\"from_account_id\": " + from + ", \"to_account_id\": " + to + ", \"amount\": 1}"))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_AMOUNT.value()));
	}

	// --- stored 404 and 422 ---

	@ParameterizedTest(name = "unknown {0}")
	@ValueSource(strings = { "from", "to" })
	void unknownAccountIsStored404(String side) throws Exception {
		long known = funded("CUSTOMER", "5.00");
		long unknown = Long.MAX_VALUE;
		String json = side.equals("from") ? body(unknown, known, "1.00") : body(known, unknown, "1.00");
		String key = newKey();
		MvcResult first = this.mvc.perform(transfer(key, json))
			.andExpect(status().isNotFound())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.ACCOUNT_NOT_FOUND.value()))
			.andExpect(jsonPath("$.detail").value("No account with id " + unknown + "."))
			.andReturn();
		assertThat(LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow().statusCode()).isEqualTo(404);
		assertSameResponse(first, this.mvc.perform(transfer(key, json)).andReturn());
		assertBalance(known, "5.00");
	}

	// The detail is stored and replayed, so missing ids are listed in ascending order.
	@Test
	void bothAccountsUnknownIsStored404WithIdsAscending() throws Exception {
		long low = Long.MAX_VALUE - 1;
		long high = Long.MAX_VALUE;
		String key = newKey();
		MvcResult first = this.mvc.perform(transfer(key, body(high, low, "1.00")))
			.andExpect(status().isNotFound())
			.andExpect(jsonPath("$.code").value(ProblemCode.ACCOUNT_NOT_FOUND.value()))
			.andExpect(jsonPath("$.detail").value("No accounts with ids " + low + ", " + high + "."))
			.andReturn();
		assertThat(LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow().statusCode()).isEqualTo(404);
		assertSameResponse(first, this.mvc.perform(transfer(key, body(high, low, "1.00"))).andReturn());
	}

	@ParameterizedTest(name = "{0} as {1}")
	@CsvSource(delimiter = '|', textBlock = """
			system:external      | from
			system:external      | to
			system:escrow        | from
			system:escrow        | to
			system:platform_fees | from
			system:platform_fees | to
			""")
	void systemAccountsAreNotTransferable(String systemName, String side) throws Exception {
		long system = LedgerTestSupport.accountId(this.dataSource, systemName);
		long user = funded("CUSTOMER", "5.00");
		Totals systemBefore = LedgerTestSupport.totals(this.dataSource, system);
		Totals userBefore = LedgerTestSupport.totals(this.dataSource, user);
		String json = side.equals("from") ? body(system, user, "1.00") : body(user, system, "1.00");
		String key = newKey();

		MvcResult first = this.mvc.perform(transfer(key, json))
			.andExpect(status().isUnprocessableContent())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.ACCOUNT_NOT_TRANSFERABLE.value()))
			.andReturn();
		assertThat(LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow().statusCode()).isEqualTo(422);
		assertSameResponse(first, this.mvc.perform(transfer(key, json)).andReturn());
		LedgerTestSupport.assertMoved(this.dataSource, system, systemBefore, "0");
		LedgerTestSupport.assertMoved(this.dataSource, user, userBefore, "0");
	}

	// --- helpers ---

	private long account(String type) throws Exception {
		return ApiTestClient.createAccount(this.mvc, type);
	}

	private long funded(String type, String amount) throws Exception {
		long id = account(type);
		ApiTestClient.fund(this.mvc, id, amount);
		return id;
	}

	private long transfersOn(long accountId) throws Exception {
		return LedgerTestSupport.transactionsTouching(this.dataSource, accountId, "TRANSFER");
	}

	private void assertBalance(long accountId, String expected) throws Exception {
		Totals totals = LedgerTestSupport.totals(this.dataSource, accountId);
		assertThat(totals.balance()).as("account %d balance", accountId).isEqualByComparingTo(expected);
		assertThat(totals.entrySum()).as("account %d entry sum", accountId).isEqualByComparingTo(totals.balance());
	}

	private List<String> kindAndCorrelation(long transactionId) throws Exception {
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource);
				PreparedStatement ps = conn
					.prepareStatement("SELECT kind, correlation_id FROM transactions WHERE id = ?")) {
			ps.setLong(1, transactionId);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).as("[%s] transaction %d", APP, transactionId).isTrue();
				return List.of(rs.getString(1), rs.getString(2));
			}
		}
	}

	private static MockHttpServletRequestBuilder transfer(String key, String json) {
		return keyedPost("/v1/transfers", key, json);
	}

	private static String body(long from, long to, String amount) {
		return "{\"from_account_id\": " + from + ", \"to_account_id\": " + to + ", \"amount\": \"" + amount + "\"}";
	}

}
