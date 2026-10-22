package dev.aryanyeole.ledgerapi.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

// ledger.idempotency.purge.* binding and validation (D-126). Only the properties are bound here,
// in a context with nothing else in it: no purger, no database, nothing standing in for either.
// An invalid value fails the context, and the root cause names the property.
class IdempotencyKeyPurgePropertiesTest {

	private final ApplicationContextRunner runner = new ApplicationContextRunner()
		.withUserConfiguration(PropertiesOnly.class);

	@Test
	void defaults() {
		this.runner.run((context) -> {
			assertThat(context).hasNotFailed();
			IdempotencyKeyPurgeProperties properties = context.getBean(IdempotencyKeyPurgeProperties.class);
			assertThat(properties.enabled()).isFalse();
			assertThat(properties.scheduled()).isFalse();
			assertThat(properties.batchSize()).isEqualTo(1000);
			assertThat(properties.interval()).isEqualTo(Duration.ofMinutes(5));
		});
	}

	@ParameterizedTest(name = "batch-size={0}")
	@CsvSource({ "1", "10000" })
	void batchSizeBoundsBind(int batchSize) {
		this.runner.withPropertyValues("ledger.idempotency.purge.batch-size=" + batchSize)
			.run((context) -> assertThat(context.getBean(IdempotencyKeyPurgeProperties.class).batchSize())
				.isEqualTo(batchSize));
	}

	@ParameterizedTest(name = "{0}={1}")
	@CsvSource({ "batch-size, 0", "batch-size, 10001", "batch-size, -1", "interval, 0s", "interval, -1s" })
	void invalidValuesFailStartup(String property, String value) {
		String name = "ledger.idempotency.purge." + property;
		this.runner.withPropertyValues(name + "=" + value).run((context) -> {
			assertThat(context).hasFailed();
			assertThat(context.getStartupFailure()).rootCause()
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining(name);
		});
	}

	@Configuration(proxyBeanMethods = false)
	@EnableConfigurationProperties(IdempotencyKeyPurgeProperties.class)
	static class PropertiesOnly {

	}

}
