package dev.aryanyeole.ledgerapi.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

// Web slice for the API edge (M2 T1): API-key auth (D-049), problem+json errors, and the
// correlation id. No database: these run without Testcontainers. Debug logging is on for
// the app and Spring MVC so the never-log-the-key check sees everything that is logged.
@WebMvcTest(properties = { EdgeProbe.API_KEYS_PROPERTY, "logging.level.dev.aryanyeole=DEBUG",
		"logging.level.org.springframework.web=DEBUG" })
@Import(EdgeProbe.Controller.class)
@ExtendWith(OutputCaptureExtension.class)
class ApiEdgeWebTest {

	private static final String WHOAMI = "/v1/probe/whoami";

	@Autowired
	private MockMvc mvc;

	// --- API key ---

	@Test
	void missingApiKeyIs401Problem() throws Exception {
		this.mvc.perform(get(WHOAMI))
			.andExpect(status().isUnauthorized())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(header().string(HttpHeaders.WWW_AUTHENTICATE, "ApiKey"))
			.andExpect(header().exists(CorrelationIdFilter.HEADER))
			.andExpect(jsonPath("$.status").value(401))
			.andExpect(jsonPath("$.title").value("Unauthorized"))
			.andExpect(jsonPath("$.code").value("missing_api_key"))
			.andExpect(jsonPath("$.detail").isString());
	}

	@Test
	void blankApiKeyIsMissing() throws Exception {
		this.mvc.perform(get(WHOAMI).header(ApiKeyFilter.HEADER, "   "))
			.andExpect(status().isUnauthorized())
			.andExpect(jsonPath("$.code").value("missing_api_key"));
	}

	@Test
	void unknownApiKeyIs401Problem() throws Exception {
		this.mvc.perform(get(WHOAMI).header(ApiKeyFilter.HEADER, "test-only-key-unknown"))
			.andExpect(status().isUnauthorized())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(header().string(HttpHeaders.WWW_AUTHENTICATE, "ApiKey"))
			.andExpect(jsonPath("$.status").value(401))
			.andExpect(jsonPath("$.code").value("invalid_api_key"));
	}

	@Test
	void validApiKeyResolvesClientId() throws Exception {
		this.mvc.perform(authenticated(get(WHOAMI)))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.client_id").value(EdgeProbe.CLIENT_ID));
	}

	// Fail closed: unknown paths need a key too; only the health probe is exempt.
	@Test
	void unknownPathWithoutKeyIs401() throws Exception {
		this.mvc.perform(get("/v1/no-such-endpoint"))
			.andExpect(status().isUnauthorized())
			.andExpect(jsonPath("$.code").value("missing_api_key"));
	}

	@Test
	void healthProbeIsNotBehindApiKey() throws Exception {
		MvcResult result = this.mvc.perform(get("/actuator/health")).andReturn();
		assertThat(result.getResponse().getStatus()).as("/actuator/health without X-Api-Key").isNotEqualTo(401);
	}

	// Only output written during this test counts: the capture spans the class, and MockMvc
	// prints the request headers (key included) of any other test in the class that fails.
	@Test
	void apiKeyIsNeverLogged(CapturedOutput output) throws Exception {
		int start = output.getAll().length();
		this.mvc.perform(authenticated(get(WHOAMI))).andExpect(status().isOk());
		this.mvc.perform(authenticated(get("/v1/probe/unexpected"))).andExpect(status().isInternalServerError());
		this.mvc.perform(get(WHOAMI).header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY + "-wrong"))
			.andExpect(status().isUnauthorized());
		assertThat(output.getAll().substring(start)).as("log output during this test")
			.isNotEmpty()
			.doesNotContain(EdgeProbe.API_KEY);
	}

	// --- problem+json ---

	@Test
	void unknownPathIs404ProblemWithCode() throws Exception {
		this.mvc.perform(authenticated(get("/v1/no-such-endpoint")))
			.andExpect(status().isNotFound())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value("not_found"));
	}

	@Test
	void wrongMethodIs405ProblemWithCode() throws Exception {
		this.mvc.perform(authenticated(post(WHOAMI)))
			.andExpect(status().isMethodNotAllowed())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value("method_not_allowed"));
	}

	@Test
	void malformedJsonIs400Problem() throws Exception {
		MvcResult result = this.mvc
			.perform(authenticated(post("/v1/probe/amount")).contentType(MediaType.APPLICATION_JSON)
				.content("{\"amount\": "))
			.andExpect(status().isBadRequest())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value("malformed_request"))
			.andReturn();
		assertThat(result.getResponse().getContentAsString()).as("400 body").doesNotContain("jackson", "Exception");
	}

	@Test
	void databaseFailureIs500WithoutSqlDetails() throws Exception {
		MvcResult result = this.mvc.perform(authenticated(get("/v1/probe/db-failure")))
			.andExpect(status().isInternalServerError())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.status").value(500))
			.andExpect(jsonPath("$.code").value("internal_error"))
			.andReturn();
		assertThat(result.getResponse().getContentAsString()).as("500 body")
			.doesNotContain("SQL", EdgeProbe.SQL_STATE, EdgeProbe.SQL_TEXT, "no_overdraft", "accounts",
					"Exception", "\tat ", "dev.aryanyeole", "org.springframework");
	}

	@Test
	void unexpectedFailureIs500WithoutExceptionDetails() throws Exception {
		MvcResult result = this.mvc.perform(authenticated(get("/v1/probe/unexpected")))
			.andExpect(status().isInternalServerError())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value("internal_error"))
			.andReturn();
		assertThat(result.getResponse().getContentAsString()).as("500 body")
			.doesNotContain(EdgeProbe.UNEXPECTED_MESSAGE, "IllegalStateException", "\tat ");
	}

	// --- correlation id ---

	@Test
	void suppliedCorrelationIdIsEchoedAndInMdcDuringRequest() throws Exception {
		this.mvc.perform(authenticated(get(WHOAMI)).header(CorrelationIdFilter.HEADER, "req-123.abc_X"))
			.andExpect(status().isOk())
			.andExpect(header().string(CorrelationIdFilter.HEADER, "req-123.abc_X"))
			.andExpect(jsonPath("$.correlation_id").value("req-123.abc_X"));
		assertThat(MDC.get(CorrelationIdFilter.MDC_KEY)).as("MDC after the request").isNull();
	}

	@Test
	void missingCorrelationIdIsGenerated() throws Exception {
		MvcResult result = this.mvc.perform(authenticated(get(WHOAMI))).andExpect(status().isOk()).andReturn();
		String id = result.getResponse().getHeader(CorrelationIdFilter.HEADER);
		assertThat(id).as("generated correlation id").isNotNull();
		assertThat(UUID.fromString(id).toString()).as("generated correlation id is a UUID").isEqualTo(id);
		assertThat(result.getResponse().getContentAsString()).as("MDC during the request").contains(id);
		assertThat(MDC.get(CorrelationIdFilter.MDC_KEY)).as("MDC after the request").isNull();
	}

	@Test
	void unsafeCorrelationIdIsReplaced() throws Exception {
		for (String unsafe : new String[] { "has space", "x".repeat(65), "inject\u0007bell", "a,b" }) {
			MvcResult result = this.mvc.perform(authenticated(get(WHOAMI)).header(CorrelationIdFilter.HEADER, unsafe))
				.andExpect(status().isOk())
				.andReturn();
			String id = result.getResponse().getHeader(CorrelationIdFilter.HEADER);
			assertThat(id).as("correlation id for supplied %s", unsafe).isNotEqualTo(unsafe);
			assertThat(UUID.fromString(id).toString()).as("replacement is a UUID").isEqualTo(id);
		}
	}

	@Test
	void correlationIdIsOnErrorResponses() throws Exception {
		this.mvc.perform(get(WHOAMI).header(CorrelationIdFilter.HEADER, "err-1"))
			.andExpect(status().isUnauthorized())
			.andExpect(header().string(CorrelationIdFilter.HEADER, "err-1"));
		this.mvc.perform(authenticated(get("/v1/probe/unexpected")).header(CorrelationIdFilter.HEADER, "err-2"))
			.andExpect(status().isInternalServerError())
			.andExpect(header().string(CorrelationIdFilter.HEADER, "err-2"));
	}

	private static MockHttpServletRequestBuilder authenticated(MockHttpServletRequestBuilder request) {
		return request.header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY);
	}

}
