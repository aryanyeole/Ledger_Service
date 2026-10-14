package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import com.jayway.jsonpath.JsonPath;

import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;

// MockMvc helpers shared by the API tests: authenticated requests with the test-only key,
// keyed POSTs, account creation and funding through the public endpoints, and the
// byte-identical replay check (D-044).
final class ApiTestClient {

	private ApiTestClient() {
	}

	static MockHttpServletRequestBuilder authenticated(MockHttpServletRequestBuilder request) {
		return request.header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY);
	}

	static MockHttpServletRequestBuilder keyedPost(String path, String key, String json) {
		return keyedPost(path, key, json, EdgeProbe.API_KEY);
	}

	/** A keyed POST authenticated with the given API key (e.g. EdgeProbe.API_KEY_BETA). */
	static MockHttpServletRequestBuilder keyedPost(String path, String key, String json, String apiKey) {
		return post(path).header(ApiKeyFilter.HEADER, apiKey)
			.header("Idempotency-Key", key)
			.contentType(MediaType.APPLICATION_JSON)
			.content(json);
	}

	static String newKey() {
		return "k-" + UUID.randomUUID();
	}

	/** Creates an account through POST /v1/accounts (no key) and returns its id. */
	static long createAccount(MockMvc mvc, String type) throws Exception {
		MvcResult result = mvc
			.perform(authenticated(post("/v1/accounts")).contentType(MediaType.APPLICATION_JSON)
				.content("{\"name\": \"acct-" + UUID.randomUUID() + "\", \"type\": \"" + type + "\"}"))
			.andExpect(status().isCreated())
			.andReturn();
		return longAt(result, "$.account_id");
	}

	/** Funds an account through POST /v1/deposits with a fresh key. */
	static void fund(MockMvc mvc, long accountId, String amount) throws Exception {
		mvc.perform(keyedPost("/v1/deposits", newKey(),
				"{\"account_id\": " + accountId + ", \"amount\": \"" + amount + "\"}"))
			.andExpect(status().isCreated());
	}

	static long longAt(MvcResult result, String path) throws Exception {
		return ((Number) JsonPath.read(result.getResponse().getContentAsString(), path)).longValue();
	}

	static void assertSameResponse(MvcResult first, MvcResult replay) throws Exception {
		assertThat(replay.getResponse().getStatus()).as("replay status").isEqualTo(first.getResponse().getStatus());
		assertThat(replay.getResponse().getContentType()).as("replay content type")
			.isEqualTo(first.getResponse().getContentType());
		assertThat(replay.getResponse().getContentAsByteArray()).as("replay body bytes")
			.isEqualTo(first.getResponse().getContentAsByteArray());
	}

}
