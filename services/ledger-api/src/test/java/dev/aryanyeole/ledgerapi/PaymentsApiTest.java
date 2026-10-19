package dev.aryanyeole.ledgerapi;

import static dev.aryanyeole.ledgerapi.ApiTestClient.assertSameResponse;
import static dev.aryanyeole.ledgerapi.ApiTestClient.authenticated;
import static dev.aryanyeole.ledgerapi.ApiTestClient.keyedPost;
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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
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
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.jayway.jsonpath.JsonPath;

import dev.aryanyeole.ledgerapi.LedgerTestSupport.KeyRow;
import dev.aryanyeole.ledgerapi.LedgerTestSupport.Totals;
import dev.aryanyeole.ledgerapi.api.CorrelationIdFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// POST /v1/payments and GET /v1/payments/{id} against real Postgres (D-044, D-045, D-064, D-074,
// D-075, D-088, D-095). Accounts are created and funded through the public endpoints; the API
// runs as ledger_api_app and direct checks use the same role (D-036). The container is shared
// with other test classes and already holds fixture payments and outbox rows, and ESCROW is a
// singleton: every assertion is scoped to rows this test created or is a delta measured inside
// it (BUG-002). Never a global count of payments, outbox rows or ESCROW balance.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = EdgeProbe.API_KEYS_PROPERTY)
@AutoConfigureMockMvc
class PaymentsApiTest {

	private static final String CLIENT = EdgeProbe.CLIENT_ID;

	private static final String ROUTE = "/v1/payments";

	@Autowired
	MockMvc mvc;

	@Autowired
	DataSource dataSource;

	@Autowired
	PostgreSQLContainer postgres;

	record OutboxRow(String topic, String eventType, String payload, String headers, OffsetDateTime publishedAt) {
	}

	// --- 201, replay, key reuse ---

	@Test
	void authorizeWritesPaymentPostingAndOutboxRowInOneCommit() throws Exception {
		long customer = funded("CUSTOMER", "150.00");
		long merchant = account("MERCHANT");
		long escrow = escrow();
		Totals customerBefore = LedgerTestSupport.totals(this.dataSource, customer);
		Totals escrowBefore = LedgerTestSupport.totals(this.dataSource, escrow);
		String correlationId = "t3-" + UUID.randomUUID().toString().replace("-", "");

		MvcResult result = this.mvc
			.perform(payment(newKey(), body(customer, merchant, "100.00")).header(CorrelationIdFilter.HEADER,
					correlationId))
			.andExpect(status().isCreated())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
			.andReturn();

		Map<String, Object> body = JsonPath.read(result.getResponse().getContentAsString(), "$");
		assertThat(body.keySet()).as("response fields").containsExactlyInAnyOrder("payment_id", "status",
				"customer_account_id", "merchant_account_id", "amount", "fee", "created_at", "updated_at");
		String paymentId = (String) body.get("payment_id");
		assertThat(UUID.fromString(paymentId).toString()).as("payment_id is a canonical UUID").isEqualTo(paymentId);
		assertThat(body).containsEntry("status", "AUTHORIZED")
			.containsEntry("customer_account_id", (int) customer)
			.containsEntry("merchant_account_id", (int) merchant)
			.containsEntry("amount", "100.00")
			.containsEntry("fee", "2.90");
		assertThat(body.get("updated_at")).as("updated_at of a new payment").isEqualTo(body.get("created_at"));
		assertThat(result.getResponse().getHeader("Location")).isEqualTo(ROUTE + "/" + paymentId);

		assertThat(paymentsOf(customer)).as("payments of the customer").containsExactly(paymentId);
		assertThat(paymentStatusAndFee(paymentId)).containsExactly("AUTHORIZED", new BigDecimal("2.9000"));

		List<Long> authorizes = LedgerTestSupport.transactionIds(this.dataSource, customer, "PAYMENT_AUTHORIZE");
		assertThat(authorizes).as("PAYMENT_AUTHORIZE transactions on the customer").hasSize(1);
		long transactionId = authorizes.get(0);
		assertThat(kindReferenceCorrelation(transactionId)).containsExactly("PAYMENT_AUTHORIZE", paymentId,
				correlationId);
		Map<Long, BigDecimal> entries = LedgerTestSupport.entries(this.dataSource, transactionId);
		assertThat(new ArrayList<>(entries.keySet())).as("[%s] entry account ids in insert order", APP)
			.containsExactly(Math.min(customer, escrow), Math.max(customer, escrow));
		assertThat(entries).containsExactlyInAnyOrderEntriesOf(
				Map.of(customer, new BigDecimal("-100.0000"), escrow, new BigDecimal("100.0000")));
		LedgerTestSupport.assertMoved(this.dataSource, customer, customerBefore, "-100.00");
		LedgerTestSupport.assertMoved(this.dataSource, escrow, escrowBefore, "100.00");

		List<OutboxRow> outbox = outboxRows(paymentId);
		assertThat(outbox).as("outbox rows for the payment").hasSize(1);
		OutboxRow row = outbox.get(0);
		assertThat(row.topic()).isEqualTo("ledger.payments.v1");
		assertThat(row.eventType()).isEqualTo("payment.authorized");
		assertThat(row.publishedAt()).as("published_at of a new outbox row").isNull();
		assertThat(sameJsonb(row.payload(), "{\"payment_id\": \"" + paymentId + "\", \"customer_account_id\": "
				+ customer + ", \"merchant_account_id\": " + merchant + ", \"amount\": \"100.00\", \"fee\": \"2.90\"}"))
			.as("payload %s", row.payload())
			.isTrue();
		assertThat(sameJsonb(row.headers(), "{\"correlation_id\": \"" + correlationId + "\"}"))
			.as("headers %s", row.headers())
			.isTrue();
	}

	@Test
	void replayIsByteIdenticalWithSameLocationAndWritesNothing() throws Exception {
		long customer = funded("CUSTOMER", "50.00");
		long merchant = account("MERCHANT");
		String key = newKey();
		MvcResult first = this.mvc.perform(payment(key, body(customer, merchant, "10.00")))
			.andExpect(status().isCreated())
			.andReturn();
		String paymentId = JsonPath.read(first.getResponse().getContentAsString(), "$.payment_id");

		MvcResult replay = this.mvc.perform(payment(key, body(customer, merchant, "10.00"))).andReturn();

		assertSameResponse(first, replay);
		assertThat(replay.getResponse().getHeader("Location")).isEqualTo(first.getResponse().getHeader("Location"));
		assertThat(paymentsOf(customer)).containsExactly(paymentId);
		assertThat(LedgerTestSupport.transactionIds(this.dataSource, customer, "PAYMENT_AUTHORIZE")).hasSize(1);
		assertThat(outboxRows(paymentId)).hasSize(1);
		assertBalance(customer, "40.00");
	}

	@Test
	void sameKeyDifferentBodyIs422AndUnstored() throws Exception {
		long customer = funded("CUSTOMER", "50.00");
		long merchant = account("MERCHANT");
		String key = newKey();
		this.mvc.perform(payment(key, body(customer, merchant, "10.00"))).andExpect(status().isCreated());
		KeyRow before = LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow();

		this.mvc.perform(payment(key, body(customer, merchant, "11.00")))
			.andExpect(status().isUnprocessableContent())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.IDEMPOTENCY_KEY_REUSED.value()));

		KeyRow after = LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow();
		assertThat(after.requestHash()).isEqualTo(before.requestHash());
		assertThat(after.responseBody()).isEqualTo(before.responseBody());
		assertThat(paymentsOf(customer)).hasSize(1);
		assertBalance(customer, "40.00");
	}

	// --- stored outcomes (D-045, D-064) ---

	@Test
	void insufficientFundsIsStored422AndReplaysAfterFundsArrive() throws Exception {
		long customer = funded("CUSTOMER", "10.00");
		long merchant = account("MERCHANT");
		Totals customerBefore = LedgerTestSupport.totals(this.dataSource, customer);
		String key = newKey();

		MvcResult first = this.mvc.perform(payment(key, body(customer, merchant, "20.00")))
			.andExpect(status().isUnprocessableContent())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.INSUFFICIENT_FUNDS.value()))
			.andReturn();
		assertThat(LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow().statusCode())
			.as("stored status")
			.isEqualTo(422);
		assertNothingWrittenFor(customer);
		LedgerTestSupport.assertMoved(this.dataSource, customer, customerBefore, "0");

		ApiTestClient.fund(this.mvc, customer, "100.00");
		MvcResult replay = this.mvc.perform(payment(key, body(customer, merchant, "20.00"))).andReturn();

		assertSameResponse(first, replay);
		assertNothingWrittenFor(customer);
		assertBalance(customer, "110.00");
	}

	@ParameterizedTest(name = "unknown {0}")
	@ValueSource(strings = { "customer", "merchant" })
	void unknownAccountIsStored404(String side) throws Exception {
		long unknown = Long.MAX_VALUE;
		long customer = side.equals("customer") ? unknown : funded("CUSTOMER", "50.00");
		long merchant = side.equals("merchant") ? unknown : account("MERCHANT");
		String key = newKey();

		MvcResult first = this.mvc.perform(payment(key, body(customer, merchant, "10.00")))
			.andExpect(status().isNotFound())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.ACCOUNT_NOT_FOUND.value()))
			.andExpect(jsonPath("$.detail").value("No account with id " + unknown + "."))
			.andReturn();

		assertThat(LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow().statusCode()).isEqualTo(404);
		assertSameResponse(first, this.mvc.perform(payment(key, body(customer, merchant, "10.00"))).andReturn());
		if (side.equals("merchant")) {
			assertNothingWrittenFor(customer);
			assertBalance(customer, "50.00");
		}
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = { "customer is a MERCHANT", "merchant is a CUSTOMER", "merchant is system:escrow" })
	void wrongAccountTypesAreStored422NotPayable(String scenario) throws Exception {
		long customer = funded(scenario.equals("customer is a MERCHANT") ? "MERCHANT" : "CUSTOMER", "50.00");
		long merchant = switch (scenario) {
			case "merchant is a CUSTOMER" -> account("CUSTOMER");
			case "merchant is system:escrow" -> escrow();
			default -> account("MERCHANT");
		};
		Totals customerBefore = LedgerTestSupport.totals(this.dataSource, customer);
		String key = newKey();

		MvcResult first = this.mvc.perform(payment(key, body(customer, merchant, "10.00")))
			.andExpect(status().isUnprocessableContent())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.ACCOUNT_NOT_PAYABLE.value()))
			.andReturn();

		assertThat(LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow().statusCode()).isEqualTo(422);
		assertSameResponse(first, this.mvc.perform(payment(key, body(customer, merchant, "10.00"))).andReturn());
		assertNothingWrittenFor(customer);
		LedgerTestSupport.assertMoved(this.dataSource, customer, customerBefore, "0");
	}

	// --- 400 and 401: decided before the key claim, nothing stored ---

	@Test
	void sameAccountIs400AndUnstored() throws Exception {
		long customer = funded("CUSTOMER", "50.00");
		String key = newKey();
		this.mvc.perform(payment(key, body(customer, customer, "10.00")))
			.andExpect(status().isBadRequest())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.SAME_ACCOUNT.value()));
		assertUnstored(key, customer);
	}

	@ParameterizedTest(name = "amount {0}")
	@ValueSource(strings = { "10", "\"10.001\"", "\"0\"", "\"1000000.01\"", "ABSENT" })
	void invalidAmountIs400AndUnstored(String amount) throws Exception {
		long customer = funded("CUSTOMER", "50.00");
		long merchant = account("MERCHANT");
		String json = "{\"customer_account_id\": " + customer + ", \"merchant_account_id\": " + merchant
				+ (amount.equals("ABSENT") ? "" : ", \"amount\": " + amount) + "}";
		String key = newKey();
		this.mvc.perform(payment(key, json))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_AMOUNT.value()));
		assertUnstored(key, customer);
	}

	// D-065: unknown fields and non-integer or non-positive ids.
	@ParameterizedTest(name = "{0}")
	@CsvSource(delimiter = '|', textBlock = """
			unknown field  | "customer_account_id": C, "merchant_account_id": M, "amount": "1.00", "fee": "0.00"
			string id      | "customer_account_id": "C", "merchant_account_id": M, "amount": "1.00"
			5.0 id         | "customer_account_id": C, "merchant_account_id": M.0, "amount": "1.00"
			id 0           | "customer_account_id": 0, "merchant_account_id": M, "amount": "1.00"
			""")
	void invalidRequestIs400AndUnstored(String scenario, String fields) throws Exception {
		long customer = funded("CUSTOMER", "50.00");
		long merchant = account("MERCHANT");
		String json = "{" + fields.replace("C", Long.toString(customer)).replace("M", Long.toString(merchant)) + "}";
		String key = newKey();
		this.mvc.perform(payment(key, json))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_REQUEST.value()));
		assertUnstored(key, customer);
	}

	@Test
	void missingIdempotencyKeyIs400() throws Exception {
		long customer = funded("CUSTOMER", "50.00");
		long merchant = account("MERCHANT");
		this.mvc
			.perform(authenticated(post(ROUTE)).contentType(MediaType.APPLICATION_JSON)
				.content(body(customer, merchant, "10.00")))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.MISSING_IDEMPOTENCY_KEY.value()));
		assertNothingWrittenFor(customer);
	}

	@Test
	void invalidIdempotencyKeyIs400() throws Exception {
		long customer = funded("CUSTOMER", "50.00");
		long merchant = account("MERCHANT");
		String key = "has space";
		this.mvc.perform(payment(key, body(customer, merchant, "10.00")))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_IDEMPOTENCY_KEY.value()));
		assertUnstored(key, customer);
	}

	@Test
	void missingApiKeyIs401() throws Exception {
		this.mvc
			.perform(post(ROUTE).header("Idempotency-Key", newKey())
				.contentType(MediaType.APPLICATION_JSON)
				.content(body(1, 2, "10.00")))
			.andExpect(status().isUnauthorized())
			.andExpect(jsonPath("$.code").value(ProblemCode.MISSING_API_KEY.value()));
		this.mvc.perform(get(ROUTE + "/" + UUID.randomUUID()))
			.andExpect(status().isUnauthorized())
			.andExpect(jsonPath("$.code").value(ProblemCode.MISSING_API_KEY.value()));
	}

	// --- GET: the current state; a replay keeps the original outcome ---

	@Test
	void getReturnsCurrentStatusWhileReplayKeepsAuthorized() throws Exception {
		long customer = funded("CUSTOMER", "50.00");
		long merchant = account("MERCHANT");
		String key = newKey();
		MvcResult created = this.mvc.perform(payment(key, body(customer, merchant, "10.00")))
			.andExpect(status().isCreated())
			.andReturn();
		Map<String, Object> posted = JsonPath.read(created.getResponse().getContentAsString(), "$");
		String paymentId = (String) posted.get("payment_id");

		MvcResult fetched = this.mvc.perform(authenticated(get(ROUTE + "/" + paymentId)))
			.andExpect(status().isOk())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
			.andReturn();
		assertThat((Map<String, Object>) JsonPath.read(fetched.getResponse().getContentAsString(), "$"))
			.as("GET of a new payment")
			.isEqualTo(posted);

		settleAsWorker(paymentId);

		Map<String, Object> settled = JsonPath.read(this.mvc.perform(authenticated(get(ROUTE + "/" + paymentId)))
			.andExpect(status().isOk())
			.andReturn()
			.getResponse()
			.getContentAsString(), "$");
		assertThat(settled.get("status")).isEqualTo("SETTLED");
		assertThat(OffsetDateTime.parse((String) settled.get("updated_at")))
			.as("updated_at after settlement")
			.isAfter(OffsetDateTime.parse((String) posted.get("updated_at")));
		assertThat(settled).as("every other field unchanged")
			.containsAllEntriesOf(Map.of("payment_id", paymentId, "customer_account_id", (int) customer,
					"merchant_account_id", (int) merchant, "amount", "10.00", "fee", "0.29", "created_at",
					posted.get("created_at")));

		MvcResult replay = this.mvc.perform(payment(key, body(customer, merchant, "10.00"))).andReturn();
		assertSameResponse(created, replay);
		assertThat((String) JsonPath.read(replay.getResponse().getContentAsString(), "$.status"))
			.isEqualTo("AUTHORIZED");
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = { "not-a-uuid", "1-1-1-1-1", "0123456789abcdef0123456789abcdef" })
	void malformedPaymentIdIs400(String id) throws Exception {
		this.mvc.perform(authenticated(get(ROUTE + "/" + id)))
			.andExpect(status().isBadRequest())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_REQUEST.value()));
	}

	@Test
	void unknownPaymentIs404() throws Exception {
		this.mvc.perform(authenticated(get(ROUTE + "/" + UUID.randomUUID())))
			.andExpect(status().isNotFound())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.PAYMENT_NOT_FOUND.value()));
	}

	// --- helpers ---

	private void assertUnstored(String key, long customer) throws Exception {
		assertThat(LedgerTestSupport.keyRowCount(this.dataSource, CLIENT, key)).as("key rows for %s", key).isZero();
		assertNothingWrittenFor(customer);
	}

	// No payment, PAYMENT_AUTHORIZE posting or outbox row for this customer.
	private void assertNothingWrittenFor(long customer) throws Exception {
		assertThat(paymentsOf(customer)).as("payments of customer %d", customer).isEmpty();
		assertThat(LedgerTestSupport.transactionIds(this.dataSource, customer, "PAYMENT_AUTHORIZE"))
			.as("PAYMENT_AUTHORIZE transactions on customer %d", customer)
			.isEmpty();
		assertThat(LedgerTestSupport.queryLong(this.dataSource,
				"SELECT count(*) FROM outbox WHERE payload->>'customer_account_id' = ?::text", customer))
			.as("outbox rows naming customer %d", customer)
			.isZero();
	}

	// The worker's grant: UPDATE (status) only; the transition trigger sets updated_at (D-076).
	private void settleAsWorker(String paymentId) throws Exception {
		try (Connection conn = DriverManager.getConnection(this.postgres.getJdbcUrl(), "settlement_worker",
				TestcontainersConfiguration.WORKER_PASSWORD)) {
			try (PreparedStatement ps = conn.prepareStatement("SELECT current_user");
					ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				assertThat(rs.getString(1)).as("current_user").isEqualTo("settlement_worker");
			}
			try (PreparedStatement ps = conn.prepareStatement(
					"UPDATE payments SET status = 'SETTLED' WHERE id = ?::uuid AND status = 'AUTHORIZED'")) {
				ps.setString(1, paymentId);
				assertThat(ps.executeUpdate()).as("settlement_worker: payments settled").isEqualTo(1);
			}
		}
	}

	private List<String> paymentsOf(long customer) throws Exception {
		List<String> ids = new ArrayList<>();
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource);
				PreparedStatement ps = conn
					.prepareStatement("SELECT id::text FROM payments WHERE customer_account_id = ? ORDER BY created_at")) {
			ps.setLong(1, customer);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					ids.add(rs.getString(1));
				}
			}
		}
		return ids;
	}

	private List<Object> paymentStatusAndFee(String paymentId) throws Exception {
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource);
				PreparedStatement ps = conn.prepareStatement("SELECT status, fee FROM payments WHERE id = ?::uuid")) {
			ps.setString(1, paymentId);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).as("[%s] payment %s", APP, paymentId).isTrue();
				return List.of(rs.getString(1), rs.getBigDecimal(2));
			}
		}
	}

	private List<String> kindReferenceCorrelation(long transactionId) throws Exception {
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource);
				PreparedStatement ps = conn
					.prepareStatement("SELECT kind, reference, correlation_id FROM transactions WHERE id = ?")) {
			ps.setLong(1, transactionId);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).as("[%s] transaction %d", APP, transactionId).isTrue();
				return List.of(rs.getString(1), rs.getString(2), rs.getString(3));
			}
		}
	}

	private List<OutboxRow> outboxRows(String messageKey) throws Exception {
		List<OutboxRow> rows = new ArrayList<>();
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource);
				PreparedStatement ps = conn.prepareStatement(
						"SELECT topic, event_type, payload::text, headers::text, published_at FROM outbox WHERE message_key = ?")) {
			ps.setString(1, messageKey);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					rows.add(new OutboxRow(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
							rs.getObject(5, OffsetDateTime.class)));
				}
			}
		}
		return rows;
	}

	// Equality as jsonb: key order and whitespace don't matter, types and values do.
	private boolean sameJsonb(String actual, String expected) throws Exception {
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource);
				PreparedStatement ps = conn.prepareStatement("SELECT ?::jsonb = ?::jsonb")) {
			ps.setString(1, actual);
			ps.setString(2, expected);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getBoolean(1);
			}
		}
	}

	private long escrow() throws Exception {
		return LedgerTestSupport.accountId(this.dataSource, "system:escrow");
	}

	private long account(String type) throws Exception {
		return ApiTestClient.createAccount(this.mvc, type);
	}

	private long funded(String type, String amount) throws Exception {
		long id = account(type);
		ApiTestClient.fund(this.mvc, id, amount);
		return id;
	}

	private void assertBalance(long accountId, String expected) throws Exception {
		Totals totals = LedgerTestSupport.totals(this.dataSource, accountId);
		assertThat(totals.balance()).as("account %d balance", accountId).isEqualByComparingTo(expected);
		assertThat(totals.entrySum()).as("account %d entry sum", accountId).isEqualByComparingTo(totals.balance());
	}

	private static MockHttpServletRequestBuilder payment(String key, String json) {
		return keyedPost(ROUTE, key, json);
	}

	private static String body(long customer, long merchant, String amount) {
		return "{\"customer_account_id\": " + customer + ", \"merchant_account_id\": " + merchant + ", \"amount\": \""
				+ amount + "\"}";
	}

}
