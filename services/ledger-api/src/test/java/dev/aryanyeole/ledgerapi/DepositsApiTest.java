package dev.aryanyeole.ledgerapi;

import static dev.aryanyeole.ledgerapi.LedgerTestSupport.APP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
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

import com.jayway.jsonpath.JsonPath;

import dev.aryanyeole.ledgerapi.LedgerTestSupport.KeyRow;
import dev.aryanyeole.ledgerapi.LedgerTestSupport.Totals;
import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.CorrelationIdFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// POST /v1/deposits against real Postgres (D-044, D-045, D-046, D-062, D-065). The API runs
// as ledger_api_app; direct checks use the same role (D-036). system:external is shared by
// every test, so it is checked by deltas (BUG-002).
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = EdgeProbe.API_KEYS_PROPERTY)
@AutoConfigureMockMvc
class DepositsApiTest {

	private static final String CLIENT = EdgeProbe.CLIENT_ID;

	@Autowired
	MockMvc mvc;

	@Autowired
	DataSource dataSource;

	// --- 201 and replay ---

	@Test
	void depositPostsOneBalancedTransaction() throws Exception {
		long target = createAccount("CUSTOMER");
		long external = SystemAccountFixture.external(this.dataSource);
		Totals externalBefore = LedgerTestSupport.totals(this.dataSource, external);
		String correlationId = "t4-" + UUID.randomUUID().toString().replace("-", "");

		MvcResult result = this.mvc
			.perform(deposit(newKey(), body(target, "25.00")).header(CorrelationIdFilter.HEADER, correlationId))
			.andExpect(status().isCreated())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
			.andExpect(jsonPath("$.kind").value("DEPOSIT"))
			.andExpect(jsonPath("$.account_id").value(target))
			.andExpect(jsonPath("$.amount").value("25.00"))
			.andExpect(jsonPath("$.created_at").isString())
			.andReturn();
		assertThat(result.getResponse().getHeader("Location")).as("Location on a deposit").isNull();
		long transactionId = ((Number) JsonPath.read(result.getResponse().getContentAsString(), "$.transaction_id"))
			.longValue();

		assertThat(LedgerTestSupport.transactionsTouching(this.dataSource, target)).as("transactions on target")
			.isEqualTo(1);
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource);
				PreparedStatement ps = conn
					.prepareStatement("SELECT kind, correlation_id FROM transactions WHERE id = ?")) {
			ps.setLong(1, transactionId);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).as("[%s] transaction %d", APP, transactionId).isTrue();
				assertThat(rs.getString(1)).as("kind").isEqualTo("DEPOSIT");
				assertThat(rs.getString(2)).as("transactions.correlation_id")
					.isEqualTo(result.getResponse().getHeader(CorrelationIdFilter.HEADER))
					.isEqualTo(correlationId);
			}
		}
		assertThat(entries(transactionId)).as("[%s] entries of %d", APP, transactionId)
			.containsExactlyInAnyOrderEntriesOf(
					Map.of(external, new BigDecimal("-25.0000"), target, new BigDecimal("25.0000")));
		Totals targetTotals = LedgerTestSupport.totals(this.dataSource, target);
		assertThat(targetTotals.balance()).as("target balance").isEqualByComparingTo("25.00");
		assertThat(targetTotals.entrySum()).as("target entry sum").isEqualByComparingTo(targetTotals.balance());
		assertMoved(external, externalBefore, "-25.00");
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = { "CUSTOMER", "MERCHANT" })
	void depositableTypesAreCredited(String type) throws Exception {
		long target = createAccount(type);
		this.mvc.perform(deposit(newKey(), body(target, "1.00"))).andExpect(status().isCreated());
		assertThat(LedgerTestSupport.totals(this.dataSource, target).balance()).isEqualByComparingTo("1.00");
	}

	@Test
	void replayWithSameBodyIsByteIdentical() throws Exception {
		long target = createAccount("CUSTOMER");
		String key = newKey();
		MvcResult first = this.mvc.perform(deposit(key, body(target, "10.00"))).andExpect(status().isCreated()).andReturn();
		MvcResult replay = this.mvc.perform(deposit(key, body(target, "10.00"))).andReturn();

		assertSameResponse(first, replay);
		assertThat(LedgerTestSupport.transactionsTouching(this.dataSource, target)).as("transactions on target")
			.isEqualTo(1);
		assertThat(LedgerTestSupport.totals(this.dataSource, target).balance()).isEqualByComparingTo("10.00");
	}

	// Same meaning, different bytes: key order, whitespace and "10" vs "10.00" (D-046).
	@Test
	void replayWithEquivalentBodyIsByteIdentical() throws Exception {
		long target = createAccount("CUSTOMER");
		String key = newKey();
		MvcResult first = this.mvc.perform(deposit(key, "{\"account_id\":" + target + ",\"amount\":\"10\"}"))
			.andExpect(status().isCreated())
			.andReturn();
		MvcResult replay = this.mvc
			.perform(deposit(key, "{\n  \"amount\" : \"10.00\",\n\t\"account_id\" :   " + target + "\n}"))
			.andReturn();

		assertSameResponse(first, replay);
		assertThat(LedgerTestSupport.transactionsTouching(this.dataSource, target)).isEqualTo(1);
	}

	@Test
	void sameKeyDifferentAmountIs422AndLeavesKeyUnchanged() throws Exception {
		long target = createAccount("CUSTOMER");
		String key = newKey();
		this.mvc.perform(deposit(key, body(target, "10.00"))).andExpect(status().isCreated());
		KeyRow before = LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow();

		this.mvc.perform(deposit(key, body(target, "11.00")))
			.andExpect(status().isUnprocessableContent())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.IDEMPOTENCY_KEY_REUSED.value()));

		KeyRow after = LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow();
		assertThat(after.requestHash()).as("request_hash").isEqualTo(before.requestHash());
		assertThat(after.statusCode()).as("status_code").isEqualTo(before.statusCode()).isEqualTo(201);
		assertThat(after.responseBody()).as("response_body").isEqualTo(before.responseBody());
		assertThat(LedgerTestSupport.transactionsTouching(this.dataSource, target)).isEqualTo(1);
		assertThat(LedgerTestSupport.totals(this.dataSource, target).balance()).isEqualByComparingTo("10.00");
	}

	// --- 400: decided before the transaction, nothing stored ---

	@ParameterizedTest(name = "[{0}]")
	@CsvSource(delimiter = '|', nullValues = "MISSING", textBlock = """
			MISSING     | missing_idempotency_key
			LONG        | invalid_idempotency_key
			''          | invalid_idempotency_key
			has space   | invalid_idempotency_key
			café   | invalid_idempotency_key
			tab\there   | invalid_idempotency_key
			""")
	void missingOrInvalidKeyIs400(String key, String code) throws Exception {
		long target = createAccount("CUSTOMER");
		String header = "LONG".equals(key) ? "k".repeat(256) : key;
		long keyRowsBefore = LedgerTestSupport.keyRowCount(this.dataSource, CLIENT);
		MockHttpServletRequestBuilder request = authenticated(post("/v1/deposits"))
			.contentType(MediaType.APPLICATION_JSON)
			.content(body(target, "5.00"));
		if (header != null) {
			request.header("Idempotency-Key", header);
		}
		this.mvc.perform(request)
			.andExpect(status().isBadRequest())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(code));
		assertNothingStored(target, keyRowsBefore);
	}

	@Test
	void keyOf255PrintableCharactersIsAccepted() throws Exception {
		long target = createAccount("CUSTOMER");
		String key = "!" + "~".repeat(253) + UUID.randomUUID().toString().charAt(0);
		this.mvc.perform(deposit(key, body(target, "5.00"))).andExpect(status().isCreated());
	}

	@Test
	void unknownFieldIs400InvalidRequest() throws Exception {
		long target = createAccount("CUSTOMER");
		long keyRowsBefore = LedgerTestSupport.keyRowCount(this.dataSource, CLIENT);
		this.mvc
			.perform(deposit(newKey(), "{\"account_id\": " + target + ", \"amount\": \"5.00\", \"memo\": \"x\"}"))
			.andExpect(status().isBadRequest())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_REQUEST.value()));
		assertNothingStored(target, keyRowsBefore);
	}

	// D-065: account_id must be a positive JSON integer; no coercion from strings or floats.
	@ParameterizedTest(name = "account_id {0}")
	@ValueSource(strings = { "\"ID\"", "ID.0", "ID.5", "true", "0", "-1", "null", "99999999999999999999", "[ID]",
			"ABSENT" })
	void accountIdMustBeAPositiveJsonInteger(String accountIdJson) throws Exception {
		long target = createAccount("CUSTOMER");
		long keyRowsBefore = LedgerTestSupport.keyRowCount(this.dataSource, CLIENT);
		String json = accountIdJson.equals("ABSENT") ? "{\"amount\": \"5.00\"}"
				: "{\"account_id\": " + accountIdJson.replace("ID", Long.toString(target)) + ", \"amount\": \"5.00\"}";
		this.mvc.perform(deposit(newKey(), json))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_REQUEST.value()));
		assertNothingStored(target, keyRowsBefore);
	}

	// D-065 leaves amounts on the D-047 codec: a JSON number amount is invalid_amount.
	@ParameterizedTest(name = "amount {0}")
	@ValueSource(strings = { "10", "10.00", "1e3" })
	void numericAmountIsStillInvalidAmount(String amountJson) throws Exception {
		long target = createAccount("CUSTOMER");
		this.mvc.perform(deposit(newKey(), "{\"account_id\": " + target + ", \"amount\": " + amountJson + "}"))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_AMOUNT.value()));
	}

	// --- stored outcomes (D-045) ---

	@Test
	void unknownTargetIsStored404() throws Exception {
		String key = newKey();
		MvcResult first = this.mvc.perform(deposit(key, body(Long.MAX_VALUE, "5.00")))
			.andExpect(status().isNotFound())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.ACCOUNT_NOT_FOUND.value()))
			.andReturn();
		assertThat(LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow().statusCode())
			.as("stored status").isEqualTo(404);

		MvcResult replay = this.mvc.perform(deposit(key, body(Long.MAX_VALUE, "5.00"))).andReturn();
		assertSameResponse(first, replay);
	}

	@Test
	void depositToEscrowIsStored422() throws Exception {
		long escrow = LedgerTestSupport.accountId(this.dataSource, "system:escrow");
		Totals escrowBefore = LedgerTestSupport.totals(this.dataSource, escrow);
		String key = newKey();
		MvcResult first = this.mvc.perform(deposit(key, body(escrow, "5.00")))
			.andExpect(status().isUnprocessableContent())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.ACCOUNT_NOT_DEPOSITABLE.value()))
			.andReturn();
		assertThat(LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow().statusCode())
			.as("stored status").isEqualTo(422);

		MvcResult replay = this.mvc.perform(deposit(key, body(escrow, "5.00"))).andReturn();
		assertSameResponse(first, replay);
		assertMoved(escrow, escrowBefore, "0");
	}

	@Test
	void depositToExternalItselfIsStored422() throws Exception {
		long external = SystemAccountFixture.external(this.dataSource);
		Totals externalBefore = LedgerTestSupport.totals(this.dataSource, external);
		String key = newKey();
		MvcResult first = this.mvc.perform(deposit(key, body(external, "5.00")))
			.andExpect(status().isUnprocessableContent())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.ACCOUNT_NOT_DEPOSITABLE.value()))
			.andReturn();
		assertThat(LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow().statusCode())
			.as("stored status").isEqualTo(422);

		MvcResult replay = this.mvc.perform(deposit(key, body(external, "5.00"))).andReturn();
		assertSameResponse(first, replay);
		assertMoved(external, externalBefore, "0");
	}

	// --- helpers ---

	private void assertSameResponse(MvcResult first, MvcResult replay) throws Exception {
		assertThat(replay.getResponse().getStatus()).as("replay status").isEqualTo(first.getResponse().getStatus());
		assertThat(replay.getResponse().getContentType()).as("replay content type")
			.isEqualTo(first.getResponse().getContentType());
		assertThat(replay.getResponse().getContentAsByteArray()).as("replay body bytes")
			.isEqualTo(first.getResponse().getContentAsByteArray());
	}

	private void assertMoved(long accountId, Totals before, String amount) throws SQLException {
		Totals delta = LedgerTestSupport.totals(this.dataSource, accountId).minus(before);
		assertThat(delta.balance()).as("account %d balance moved", accountId).isEqualByComparingTo(amount);
		assertThat(delta.entrySum()).as("account %d entry sum moved", accountId).isEqualByComparingTo(amount);
	}

	private void assertNothingStored(long target, long keyRowsBefore) throws SQLException {
		assertThat(LedgerTestSupport.keyRowCount(this.dataSource, CLIENT)).as("key rows for %s", CLIENT)
			.isEqualTo(keyRowsBefore);
		assertThat(LedgerTestSupport.transactionsTouching(this.dataSource, target)).as("transactions on target")
			.isZero();
	}

	private Map<Long, BigDecimal> entries(long transactionId) throws SQLException {
		Map<Long, BigDecimal> entries = new LinkedHashMap<>();
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource);
				PreparedStatement ps = conn
					.prepareStatement("SELECT account_id, amount FROM entries WHERE transaction_id = ? ORDER BY id")) {
			ps.setLong(1, transactionId);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					entries.put(rs.getLong(1), rs.getBigDecimal(2));
				}
			}
		}
		return entries;
	}

	private long createAccount(String type) throws Exception {
		MvcResult result = this.mvc
			.perform(authenticated(post("/v1/accounts")).contentType(MediaType.APPLICATION_JSON)
				.content("{\"name\": \"t4-" + UUID.randomUUID() + "\", \"type\": \"" + type + "\"}"))
			.andExpect(status().isCreated())
			.andReturn();
		return ((Number) JsonPath.read(result.getResponse().getContentAsString(), "$.account_id")).longValue();
	}

	private MockHttpServletRequestBuilder deposit(String key, String json) {
		return authenticated(post("/v1/deposits")).header("Idempotency-Key", key)
			.contentType(MediaType.APPLICATION_JSON)
			.content(json);
	}

	private static String body(long accountId, String amount) {
		return "{\"account_id\": " + accountId + ", \"amount\": \"" + amount + "\"}";
	}

	private static MockHttpServletRequestBuilder authenticated(MockHttpServletRequestBuilder request) {
		return request.header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY);
	}

	private static String newKey() {
		return "t4-" + UUID.randomUUID();
	}

}
