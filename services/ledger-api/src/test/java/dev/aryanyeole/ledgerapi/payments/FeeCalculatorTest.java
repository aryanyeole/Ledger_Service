package dev.aryanyeole.ledgerapi.payments;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.aryanyeole.ledgerapi.money.Money;

// The fee rule (spec §4, D-074): amount x rate, rounded once to scale 2 with HALF_EVEN.
// 5.00 x 0.029 = 0.145 is a tie: HALF_EVEN gives 0.14, HALF_UP would give 0.15.
// ledger.payments.fee-rate must be a decimal in [0, 1), or startup fails.
class FeeCalculatorTest {

	private static final PaymentsProperties.Dlq DLQ = new PaymentsProperties.Dlq("ledger.payments.dlq.v1", 1);

	private static final FeeCalculator DEFAULT_RATE = new FeeCalculator(new PaymentsProperties("0.029",
			"ledger.payments.v1", 3, (short) 1, DLQ));

	@ParameterizedTest(name = "{0} -> {1}")
	@CsvSource({ "100.00, 2.90", "5.00, 0.14", "15.00, 0.44", "0.01, 0.00" })
	void feeIsRoundedHalfEvenToCents(String amount, String fee) {
		assertThat(DEFAULT_RATE.feeFor(new Money(new BigDecimal(amount)))).isEqualTo(new Money(new BigDecimal(fee)));
	}

	@Test
	void defaultRateIsExact() {
		assertThat(DEFAULT_RATE.rate()).isEqualTo(new BigDecimal("0.029"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "1", "-0.01", "abc" })
	void invalidRatesFailStartup(String rate) {
		assertThatThrownBy(() -> new PaymentsProperties(rate, "ledger.payments.v1", 3, (short) 1, DLQ))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessageContaining("ledger.payments.fee-rate");
	}

	@ParameterizedTest
	@ValueSource(strings = { "", " " })
	void blankTopicFailsStartup(String topic) {
		assertThatThrownBy(() -> new PaymentsProperties("0.029", topic, 3, (short) 1, DLQ))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessageContaining("ledger.payments.topic");
	}

}
