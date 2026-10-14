package dev.aryanyeole.ledgerapi.money;

import java.math.BigDecimal;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

// ledger.money.max-amount: the largest amount a single request may move (D-047).
// Bounded by NUMERIC(19,4): at most 15 integer digits, and at most 2 dp like any amount.
@ConfigurationProperties("ledger.money")
public record MoneyProperties(@DefaultValue("1000000.00") BigDecimal maxAmount) {

	private static final BigDecimal NUMERIC_19_4_LIMIT = new BigDecimal("1000000000000000");

	public MoneyProperties {
		if (maxAmount.signum() <= 0 || maxAmount.compareTo(NUMERIC_19_4_LIMIT) >= 0
				|| maxAmount.stripTrailingZeros().scale() > Money.SCALE) {
			throw new IllegalArgumentException(
					"ledger.money.max-amount must be > 0, below 10^15 and have at most 2 decimal places");
		}
	}

}
