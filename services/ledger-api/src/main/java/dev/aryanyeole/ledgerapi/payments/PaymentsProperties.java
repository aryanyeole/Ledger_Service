package dev.aryanyeole.ledgerapi.payments;

import java.math.BigDecimal;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

// ledger.payments.fee-rate (D-074): the platform's share of each payment, parsed from a string
// to BigDecimal and checked to be in [0, 1); anything else fails startup. Quote it in YAML
// (fee-rate: "0.029"): an unquoted 0.029 is read as a YAML double before Spring binds it.
// ledger.payments.topic (D-095): the Kafka topic of payment events, written into each outbox row.
@ConfigurationProperties("ledger.payments")
public record PaymentsProperties(@DefaultValue("0.029") String feeRate, @DefaultValue("ledger.payments.v1") String topic) {

	public PaymentsProperties {
		BigDecimal rate = parse(feeRate);
		if (rate.signum() < 0 || rate.compareTo(BigDecimal.ONE) >= 0) {
			throw new IllegalArgumentException("ledger.payments.fee-rate must be >= 0 and < 1, was " + feeRate);
		}
		if (topic == null || topic.isBlank()) {
			throw new IllegalArgumentException("ledger.payments.topic must not be blank");
		}
	}

	/** The validated fee rate. */
	public BigDecimal rate() {
		return parse(this.feeRate);
	}

	private static BigDecimal parse(String feeRate) {
		if (feeRate == null) {
			throw new IllegalArgumentException("ledger.payments.fee-rate is required");
		}
		try {
			return new BigDecimal(feeRate.strip());
		}
		catch (NumberFormatException ex) {
			throw new IllegalArgumentException("ledger.payments.fee-rate must be a decimal number, was " + feeRate, ex);
		}
	}

}
