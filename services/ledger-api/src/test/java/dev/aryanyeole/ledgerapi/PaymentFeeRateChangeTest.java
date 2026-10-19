package dev.aryanyeole.ledgerapi;

import static dev.aryanyeole.ledgerapi.ApiTestClient.assertSameResponse;
import static dev.aryanyeole.ledgerapi.ApiTestClient.keyedPost;
import static dev.aryanyeole.ledgerapi.ApiTestClient.newKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.payments.FeeCalculator;
import dev.aryanyeole.ledgerapi.payments.PaymentsProperties;

// The fee rate is not part of the fingerprint (D-088): after the rate changes, a replay still
// returns the original fee byte for byte (it is the stored outcome, not a 422), and only a new
// key gets the new fee. A test override of FeeCalculator changes the rate without a restart.
@Import({ TestcontainersConfiguration.class, PaymentFeeRateChangeTest.SettableRate.class })
@SpringBootTest(properties = EdgeProbe.API_KEYS_PROPERTY)
@AutoConfigureMockMvc
class PaymentFeeRateChangeTest {

	@Autowired
	MockMvc mvc;

	@Autowired
	SettableFeeCalculator fees;

	@TestConfiguration(proxyBeanMethods = false)
	static class SettableRate {

		@Bean
		@Primary
		SettableFeeCalculator settableFeeCalculator(PaymentsProperties properties) {
			return new SettableFeeCalculator(properties);
		}

	}

	static class SettableFeeCalculator extends FeeCalculator {

		final AtomicReference<BigDecimal> override = new AtomicReference<>();

		SettableFeeCalculator(PaymentsProperties properties) {
			super(properties);
		}

		@Override
		public BigDecimal rate() {
			BigDecimal rate = this.override.get();
			return (rate != null) ? rate : super.rate();
		}

	}

	@AfterEach
	void restoreRate() {
		this.fees.override.set(null);
	}

	@Test
	void replayKeepsOriginalFeeAfterRateChange() throws Exception {
		long customer = ApiTestClient.createAccount(this.mvc, "CUSTOMER");
		ApiTestClient.fund(this.mvc, customer, "500.00");
		long merchant = ApiTestClient.createAccount(this.mvc, "MERCHANT");
		String key = newKey();
		String body = "{\"customer_account_id\": " + customer + ", \"merchant_account_id\": " + merchant
				+ ", \"amount\": \"100.00\"}";

		MvcResult first = this.mvc.perform(keyedPost("/v1/payments", key, body))
			.andExpect(status().isCreated())
			.andExpect(jsonPath("$.fee").value("2.90"))
			.andReturn();

		this.fees.override.set(new BigDecimal("0.05"));
		MvcResult replay = this.mvc.perform(keyedPost("/v1/payments", key, body)).andReturn();
		assertSameResponse(first, replay);

		this.mvc.perform(keyedPost("/v1/payments", newKey(), body))
			.andExpect(status().isCreated())
			.andExpect(jsonPath("$.fee").value("5.00"));
		assertThat(this.fees.rate()).isEqualByComparingTo("0.05");
	}

}
