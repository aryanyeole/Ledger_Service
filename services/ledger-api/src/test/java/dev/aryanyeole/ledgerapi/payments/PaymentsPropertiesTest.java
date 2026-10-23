package dev.aryanyeole.ledgerapi.payments;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

// ledger.payments.* topic settings (D-080, D-110), bound in a context with nothing else in it: the
// DLQ topic and its partition count, and Kafka's naming rule for both topic names (1-249 characters
// of [a-zA-Z0-9._-], not "." or ".."). An invalid value fails the context, naming the property.
class PaymentsPropertiesTest {

	private static final String TOO_LONG = "t".repeat(250);

	private final ApplicationContextRunner runner = new ApplicationContextRunner()
		.withUserConfiguration(PropertiesOnly.class);

	@Test
	void dlqDefaults() {
		this.runner.run((context) -> {
			assertThat(context).hasNotFailed();
			PaymentsProperties properties = context.getBean(PaymentsProperties.class);
			assertThat(properties.dlq().topic()).isEqualTo("ledger.payments.dlq.v1");
			assertThat(properties.dlq().partitions()).isEqualTo(1);
			assertThat(properties.replicationFactor()).isEqualTo((short) 1);
		});
	}

	@Test
	void longestLegalNameBinds() {
		this.runner.withPropertyValues("ledger.payments.dlq.topic=" + "d".repeat(249))
			.run((context) -> assertThat(context.getBean(PaymentsProperties.class).dlq().topic()).hasSize(249));
	}

	static Object[][] invalid() {
		return new Object[][] {
				{ "ledger.payments.dlq.topic=ledger.payments.v1", "ledger.payments.dlq.topic must differ" },
				{ "ledger.payments.dlq.topic=", "ledger.payments.dlq.topic" },
				{ "ledger.payments.dlq.topic=" + TOO_LONG, "ledger.payments.dlq.topic" },
				{ "ledger.payments.dlq.topic=dead letters", "ledger.payments.dlq.topic" },
				{ "ledger.payments.dlq.topic=..", "ledger.payments.dlq.topic" },
				{ "ledger.payments.dlq.partitions=0", "ledger.payments.dlq.partitions" },
				{ "ledger.payments.topic=" + TOO_LONG, "ledger.payments.topic" },
				{ "ledger.payments.topic=payments v1", "ledger.payments.topic" },
				{ "ledger.payments.topic=..", "ledger.payments.topic" },
				{ "ledger.payments.topic=.", "ledger.payments.topic" } };
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("invalid")
	void invalidTopicSettingsFailStartup(String property, String named) {
		this.runner.withPropertyValues(property).run((context) -> {
			assertThat(context).hasFailed();
			assertThat(context.getStartupFailure()).rootCause()
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining(named);
		});
	}

	@ParameterizedTest(name = "{0}")
	@CsvSource({ "ledger.payments.v1", "a", "A-z_0.9" })
	void legalNamesBind(String name) {
		this.runner.withPropertyValues("ledger.payments.topic=" + name)
			.run((context) -> assertThat(context.getBean(PaymentsProperties.class).topic()).isEqualTo(name));
	}

	@Configuration(proxyBeanMethods = false)
	@EnableConfigurationProperties(PaymentsProperties.class)
	static class PropertiesOnly {

	}

}
