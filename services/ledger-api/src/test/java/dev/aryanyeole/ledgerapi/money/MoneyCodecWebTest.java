package dev.aryanyeole.ledgerapi.money;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// The money codec end to end through MVC (D-047): request JSON -> Money -> response JSON.
// Each row is the raw JSON value of "amount". Default max-amount (1000000.00).
@WebMvcTest(controllers = EdgeProbe.Controller.class, properties = EdgeProbe.API_KEYS_PROPERTY)
@Import(EdgeProbe.Controller.class)
class MoneyCodecWebTest {

	@Autowired
	private MockMvc mvc;

	@ParameterizedTest(name = "{0} -> \"{1}\"")
	@CsvSource(delimiter = '|', quoteCharacter = '\'', textBlock = """
			"10"         | 10.00
			"10.00"      | 10.00
			"10.5"       | 10.50
			"0.01"       | 0.01
			"1000000"    | 1000000.00
			"1000000.00" | 1000000.00
			""")
	void acceptedAmountIsNormalizedToScale2(String json, String expected) throws Exception {
		postAmount(json).andExpect(status().isOk())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
			.andExpect(jsonPath("$.amount").value(expected))
			.andExpect(jsonPath("$.amount").isString());
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = {
			// over max, zero, signs, exponents
			"\"1000000.01\"", "\"0\"", "\"0.00\"", "\"-1\"", "\"+1\"", "\"1e3\"", "\"1E3\"",
			// more than 2 dp (rejected, never rounded), leading zeros, malformed decimals
			"\"10.001\"", "\"10.000\"", "\"010\"", "\"00.50\"", "\"10.\"", "\".5\"", "\"1,000.00\"",
			// blanks and whitespace
			"\"\"", "\" \"", "\" 10\"", "\"10 \"", "\"10\\n\"",
			// non-ASCII digits (fullwidth 10)
			"\"１０\"",
			// not a JSON string
			"10", "10.00", "1e3", "true", "{}", "[]", "null" })
	void rejectedAmountIs400InvalidAmount(String json) throws Exception {
		postAmount(json).andExpect(status().isBadRequest())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.status").value(400))
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_AMOUNT.value()));
	}

	@Test
	void absentAmountIs400InvalidAmount() throws Exception {
		this.mvc
			.perform(post("/v1/probe/amount").header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY)
				.contentType(MediaType.APPLICATION_JSON)
				.content("{}"))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_AMOUNT.value()));
	}

	private ResultActions postAmount(String json) throws Exception {
		return this.mvc.perform(post("/v1/probe/amount").header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY)
			.contentType(MediaType.APPLICATION_JSON)
			.content("{\"amount\": " + json + "}"));
	}

}
