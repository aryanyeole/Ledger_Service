package dev.aryanyeole.ledgerapi.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

// ledger.db.lock-timeout (D-050) becomes the integer in SET LOCAL lock_timeout = '<n>ms'.
class LedgerDbPropertiesTest {

	@Test
	void durationsBecomeWholeMilliseconds() {
		assertThat(new LedgerDbProperties(Duration.ofSeconds(5)).lockTimeoutMillis()).isEqualTo("5000");
		assertThat(new LedgerDbProperties(Duration.ofMillis(500)).lockTimeoutMillis()).isEqualTo("500");
		assertThat(new LedgerDbProperties(Duration.ofMillis(Integer.MAX_VALUE)).lockTimeoutMillis())
			.isEqualTo("2147483647");
	}

	@ParameterizedTest
	@ValueSource(strings = { "PT0S", "PT-1S", "PT0.0015S", "PT0.0000001S", "PT600H" })
	void invalidTimeoutsFailStartup(String duration) {
		assertThatThrownBy(() -> new LedgerDbProperties(Duration.parse(duration)))
			.isInstanceOf(IllegalArgumentException.class);
	}

}
