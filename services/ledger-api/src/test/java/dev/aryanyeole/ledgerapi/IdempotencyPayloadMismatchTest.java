package dev.aryanyeole.ledgerapi;

import static dev.aryanyeole.ledgerapi.ApiTestClient.assertSameResponse;
import static dev.aryanyeole.ledgerapi.ApiTestClient.authenticated;
import static dev.aryanyeole.ledgerapi.ApiTestClient.keyedPost;
import static dev.aryanyeole.ledgerapi.ApiTestClient.newKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
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
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// Proof (P2, D-004, D-045, D-046): what a reused Idempotency-Key means. A different payload,
// or the same payload on a different route, is 422 idempotency_key_reused and changes
// nothing; the same meaning in different bytes replays; keys are scoped per client; a
// missing key is 400; a stored business outcome (insufficient funds) replays even after the
// facts change; and a rejected reuse never overwrites the original response.
// Red evidence (D-051): skip the request_hash comparison on conflict -> case 1 gets 201.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = { EdgeProbe.API_KEYS_PROPERTY, EdgeProbe.API_KEYS_PROPERTY_BETA })
@AutoConfigureMockMvc
class IdempotencyPayloadMismatchTest {

	private static final String CLIENT = EdgeProbe.CLIENT_ID;

	@Autowired
	MockMvc mvc;

	@Autowired
	DataSource dataSource;

	// Cases 1 and 7.
	@Test
	void differentAmountIs422AndTheOriginalStillReplays() throws Exception {
		long a = funded("100.00");
		long b = account();
		Totals aBefore = LedgerTestSupport.totals(this.dataSource, a);
		Totals bBefore = LedgerTestSupport.totals(this.dataSource, b);
		String key = newKey();

		MvcResult original = this.mvc.perform(transfer(key, body(a, b, "10.00")))
			.andExpect(status().isCreated())
			.andReturn();
		KeyRow before = LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow();

		// Case 1: same key, different amount.
		this.mvc.perform(transfer(key, body(a, b, "11.00")))
			.andExpect(status().isUnprocessableContent())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.IDEMPOTENCY_KEY_REUSED.value()));
		assertThat(transfersOn(a)).as("case 1: TRANSFER transactions on A").isEqualTo(1);
		LedgerTestSupport.assertMoved(this.dataSource, a, aBefore, "-10.00");
		LedgerTestSupport.assertMoved(this.dataSource, b, bBefore, "10.00");
		assertKeyRowUnchanged(key, before, "case 1");

		// Case 7: the original request after the rejected reuse.
		MvcResult replay = this.mvc.perform(transfer(key, body(a, b, "10.00"))).andReturn();
		assertSameResponse(original, replay);
		assertThat(transfersOn(a)).as("case 7: TRANSFER transactions on A").isEqualTo(1);
		assertKeyRowUnchanged(key, before, "case 7");
	}

	// Case 2: the fingerprint is over the validated request, not its bytes (D-046).
	@Test
	void sameMeaningInDifferentBytesReplays() throws Exception {
		long a = funded("100.00");
		long b = account();
		String key = newKey();
		MvcResult first = this.mvc
			.perform(transfer(key, "{\"from_account_id\":" + a + ",\"to_account_id\":" + b + ",\"amount\":\"10\"}"))
			.andExpect(status().isCreated())
			.andReturn();
		MvcResult replay = this.mvc
			.perform(transfer(key, "{\n  \"amount\" :  \"10.00\",\n\t\"to_account_id\" : " + b
					+ " ,\n  \"from_account_id\":" + a + "\n}"))
			.andReturn();
		assertSameResponse(first, replay);
		assertThat(transfersOn(a)).as("TRANSFER transactions on A").isEqualTo(1);
	}

	// Case 3: the route is part of the fingerprint; the deposit's response is not overwritten.
	@Test
	void sameKeyOnAnotherRouteIs422() throws Exception {
		long a = funded("100.00");
		long b = account();
		String key = newKey();
		this.mvc.perform(keyedPost("/v1/deposits", key, "{\"account_id\": " + a + ", \"amount\": \"10.00\"}"))
			.andExpect(status().isCreated());
		KeyRow before = LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow();

		this.mvc.perform(transfer(key, body(a, b, "10.00")))
			.andExpect(status().isUnprocessableContent())
			.andExpect(jsonPath("$.code").value(ProblemCode.IDEMPOTENCY_KEY_REUSED.value()));
		assertThat(transfersOn(a)).as("TRANSFER transactions on A").isZero();
		assertKeyRowUnchanged(key, before, "case 3");
		assertThat(before.responseBody()).as("stored response is the deposit's").contains("\"DEPOSIT\"");
	}

	// Case 4: keys are scoped per client (spec §7).
	@Test
	void sameKeyFromTwoClientsIsTwoRequests() throws Exception {
		long alphaFrom = funded("100.00");
		long alphaTo = account();
		long betaFrom = funded("100.00");
		long betaTo = account();
		String key = newKey();

		MvcResult alpha = this.mvc.perform(keyedPost("/v1/transfers", key, body(alphaFrom, alphaTo, "10.00")))
			.andExpect(status().isCreated())
			.andReturn();
		MvcResult beta = this.mvc
			.perform(keyedPost("/v1/transfers", key, body(betaFrom, betaTo, "10.00"), EdgeProbe.API_KEY_BETA))
			.andExpect(status().isCreated())
			.andReturn();

		assertThat(transfersOn(alphaFrom) + transfersOn(betaFrom)).as("TRANSFER transactions").isEqualTo(2);
		KeyRow alphaRow = LedgerTestSupport.keyRow(this.dataSource, EdgeProbe.CLIENT_ID, key).orElseThrow();
		KeyRow betaRow = LedgerTestSupport.keyRow(this.dataSource, EdgeProbe.CLIENT_ID_BETA, key).orElseThrow();
		assertThat(alphaRow.statusCode()).isEqualTo(201);
		assertThat(betaRow.statusCode()).isEqualTo(201);
		assertThat(beta.getResponse().getContentAsString()).as("beta was not served alpha's stored response")
			.isNotEqualTo(alpha.getResponse().getContentAsString());
	}

	// Case 5.
	@Test
	void missingKeyIs400AndStoresNothing() throws Exception {
		long a = funded("100.00");
		long b = account();
		long keyRowsBefore = LedgerTestSupport.keyRowCount(this.dataSource, CLIENT);
		this.mvc
			.perform(authenticated(post("/v1/transfers")).contentType(MediaType.APPLICATION_JSON)
				.content(body(a, b, "10.00")))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.MISSING_IDEMPOTENCY_KEY.value()));
		assertThat(transfersOn(a)).isZero();
		assertThat(LedgerTestSupport.keyRowCount(this.dataSource, CLIENT)).isEqualTo(keyRowsBefore);
	}

	// Case 6: a stored business outcome is fixed by the original request (D-045).
	@Test
	void insufficientFundsReplaysAfterFundsArrive() throws Exception {
		long a = funded("5.00");
		long b = account();
		String key = newKey();
		MvcResult first = this.mvc.perform(transfer(key, body(a, b, "10.00")))
			.andExpect(status().isUnprocessableContent())
			.andExpect(jsonPath("$.code").value(ProblemCode.INSUFFICIENT_FUNDS.value()))
			.andReturn();

		ApiTestClient.fund(this.mvc, a, "100.00");
		MvcResult replay = this.mvc.perform(transfer(key, body(a, b, "10.00"))).andReturn();

		assertSameResponse(first, replay);
		assertThat(transfersOn(a)).as("TRANSFER transactions on A").isZero();
		Totals aTotals = LedgerTestSupport.totals(this.dataSource, a);
		assertThat(aTotals.balance()).as("A balance").isEqualByComparingTo("105.00");
		assertThat(aTotals.entrySum()).as("A entry sum").isEqualByComparingTo(aTotals.balance());
	}

	private void assertKeyRowUnchanged(String key, KeyRow before, String context) throws Exception {
		KeyRow after = LedgerTestSupport.keyRow(this.dataSource, CLIENT, key).orElseThrow();
		assertThat(after.requestHash()).as("%s: request_hash", context).isEqualTo(before.requestHash());
		assertThat(after.statusCode()).as("%s: status_code", context).isEqualTo(before.statusCode());
		assertThat(after.responseBody()).as("%s: response_body", context).isEqualTo(before.responseBody());
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

	private static MockHttpServletRequestBuilder transfer(String key, String json) {
		return keyedPost("/v1/transfers", key, json);
	}

	private static String body(long from, long to, String amount) {
		return "{\"from_account_id\": " + from + ", \"to_account_id\": " + to + ", \"amount\": \"" + amount + "\"}";
	}

}
