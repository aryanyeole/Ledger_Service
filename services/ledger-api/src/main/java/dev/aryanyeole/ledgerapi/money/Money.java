package dev.aryanyeole.ledgerapi.money;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

// A USD amount at the API's scale of 2 (D-002). Any sign: balances may be zero or, for
// EXTERNAL, negative. Request amounts are additionally validated by MoneyDeserializer.
// Never rounds: a value with non-zero digits beyond 2 dp (e.g. a NUMERIC(19,4) read back
// as 10.0050) throws instead of being silently changed (D-047).
public record Money(BigDecimal amount) {

	public static final int SCALE = 2;

	public Money {
		Objects.requireNonNull(amount, "amount");
		amount = amount.setScale(SCALE, RoundingMode.UNNECESSARY);
	}

	/** Plain decimal string at scale 2, the only form amounts take in JSON (D-011). */
	@Override
	public String toString() {
		return this.amount.toPlainString();
	}

}
