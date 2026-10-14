package dev.aryanyeole.ledgerapi.money;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

// Money and its configuration without Spring: scale-2 normalization never rounds (D-047),
// and ledger.money.max-amount is validated at startup.
class MoneyTest {

	@Test
	void databaseScaleNormalizesWithoutRounding() {
		assertThat(new Money(new BigDecimal("10.0000"))).hasToString("10.00");
		assertThat(new Money(new BigDecimal("-100.5000"))).hasToString("-100.50");
		assertThat(new Money(BigDecimal.ZERO)).hasToString("0.00");
		assertThat(new Money(new BigDecimal("1E+3"))).as("plain string, never exponent").hasToString("1000.00");
	}

	@Test
	void subCentDigitsAreRejectedNotRounded() {
		assertThatThrownBy(() -> new Money(new BigDecimal("10.0050"))).isInstanceOf(ArithmeticException.class);
	}

	@Test
	void configuredMaxAmountIsEnforced() {
		MoneyDeserializer deserializer = new MoneyDeserializer(new MoneyProperties(new BigDecimal("50.00")));
		assertThat(deserializer.parse("50")).hasToString("50.00");
		assertThatThrownBy(() -> deserializer.parse("50.01")).isInstanceOf(InvalidAmountException.class)
			.hasMessage("amount must not exceed 50.00.");
	}

	@ParameterizedTest
	@ValueSource(strings = { "0", "-1", "10.001", "1000000000000000" })
	void invalidMaxAmountFailsStartup(String maxAmount) {
		assertThatThrownBy(() -> new MoneyProperties(new BigDecimal(maxAmount)))
			.isInstanceOf(IllegalArgumentException.class);
	}

}
