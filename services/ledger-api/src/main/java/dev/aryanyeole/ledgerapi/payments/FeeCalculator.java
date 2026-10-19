package dev.aryanyeole.ledgerapi.payments;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import dev.aryanyeole.ledgerapi.money.Money;

// The one place a payment fee is computed (spec §4, D-074): fee = amount x rate, rounded once
// to scale 2 with HALF_EVEN, at authorize. It is stored in payments.fee; the merchant's share is
// always amount - fee, never computed independently, and the worker never recomputes the fee.
// BigDecimal throughout (invariant 3). rate() is overridable so a test can change the rate
// without a restart.
@Component
@EnableConfigurationProperties(PaymentsProperties.class)
public class FeeCalculator {

	private final BigDecimal rate;

	public FeeCalculator(PaymentsProperties properties) {
		this.rate = properties.rate();
	}

	public BigDecimal rate() {
		return this.rate;
	}

	public Money feeFor(Money amount) {
		return new Money(amount.amount().multiply(rate()).setScale(Money.SCALE, RoundingMode.HALF_EVEN));
	}

}
