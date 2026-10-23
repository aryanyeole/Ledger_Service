package dev.aryanyeole.ledgerapi.payments;

import java.math.BigDecimal;
import java.util.regex.Pattern;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

// ledger.payments.fee-rate (D-074): the platform's share of each payment, parsed from a string
// to BigDecimal and checked to be in [0, 1); anything else fails startup. Quote it in YAML
// (fee-rate: "0.029"): an unquoted 0.029 is read as a YAML double before Spring binds it.
// ledger.payments.topic (D-095): the Kafka topic of payment events, written into each outbox row.
// ledger.payments.partitions / replication-factor (D-080): what the outbox relay creates the topic
// with if it is absent, and the partition count it requires if it exists.
// ledger.payments.dlq.topic / dlq.partitions (D-110): the dead-letter topic ledger-api owns as well;
// the relay creates it with that partition count, the same replication factor and retention.ms=-1.
// Both topic names must satisfy Kafka's naming rule (1-249 characters of [a-zA-Z0-9._-], not "." or
// ".."): a name the broker always rejects is a configuration error, not something to retry.
@ConfigurationProperties("ledger.payments")
public record PaymentsProperties(@DefaultValue("0.029") String feeRate, @DefaultValue("ledger.payments.v1") String topic,
		@DefaultValue("3") int partitions, @DefaultValue("1") short replicationFactor, @DefaultValue Dlq dlq) {

	private static final Pattern TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]{1,249}");

	/** ledger.payments.dlq.*: the dead-letter topic (D-110). */
	public record Dlq(@DefaultValue("ledger.payments.dlq.v1") String topic, @DefaultValue("1") int partitions) {

		public Dlq {
			requireTopicName("ledger.payments.dlq.topic", topic);
			if (partitions < 1) {
				throw new IllegalArgumentException(
						"ledger.payments.dlq.partitions must be at least 1, was " + partitions);
			}
		}

	}

	public PaymentsProperties {
		BigDecimal rate = parse(feeRate);
		if (rate.signum() < 0 || rate.compareTo(BigDecimal.ONE) >= 0) {
			throw new IllegalArgumentException("ledger.payments.fee-rate must be >= 0 and < 1, was " + feeRate);
		}
		requireTopicName("ledger.payments.topic", topic);
		if (dlq == null) {
			throw new IllegalArgumentException("ledger.payments.dlq is required");
		}
		if (dlq.topic().equals(topic)) {
			throw new IllegalArgumentException(
					"ledger.payments.dlq.topic must differ from ledger.payments.topic, both were " + topic);
		}
		if (partitions < 1) {
			throw new IllegalArgumentException("ledger.payments.partitions must be at least 1, was " + partitions);
		}
		if (replicationFactor < 1) {
			throw new IllegalArgumentException(
					"ledger.payments.replication-factor must be at least 1, was " + replicationFactor);
		}
	}

	private static void requireTopicName(String property, String name) {
		if (name == null || name.isBlank()) {
			throw new IllegalArgumentException(property + " must not be blank");
		}
		if (!TOPIC_NAME.matcher(name).matches() || name.equals(".") || name.equals("..")) {
			throw new IllegalArgumentException(property + " must be 1-249 characters of [a-zA-Z0-9._-], not \".\" or "
					+ "\"..\", was \"" + name + "\"");
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
