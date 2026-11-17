package dev.aryanyeole.ledgerapi.accounts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.time.Duration;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import dev.aryanyeole.ledgerapi.cache.BalanceCacheScripts;
import dev.aryanyeole.ledgerapi.cache.RedisGateway;

// The token bound (D-142, D-151, docs/design/cache-invalidation.md §2.3): token PX must exceed
// connectionTimeout + socketTimeout + 2 x command timeout, with equality refused, and socketTimeout is
// resolved the way pgjdbc resolves it (driverconfig < DataSource properties < URL; the real
// Driver.parseURL), failing closed when it's missing, unparseable or 0.
class TokenBoundTest {

	private static final String URL = "jdbc:postgresql://localhost:5433/ledger";

	private static final Duration COMMAND = RedisGateway.COMMAND_TIMEOUT;

	@Test
	void todaysValuesPass() {
		assertThat(BalanceCacheScripts.TOKEN_PX_MILLIS).isEqualTo(300_000);
		assertThat(COMMAND).isEqualTo(Duration.ofMillis(100));
		assertThatNoException().isThrownBy(() -> TokenBound.check(300_000, Duration.ofSeconds(30),
				Duration.ofSeconds(60), COMMAND));
	}

	@Test
	void oneMillisecondUnderPassesAndEqualityIsRefused() {
		// 239 799 + 60 000 + 200 = 299 999 < 300 000.
		assertThatNoException().isThrownBy(() -> TokenBound.check(300_000, Duration.ofMillis(239_799),
				Duration.ofSeconds(60), COMMAND));
		// 239 800 + 60 000 + 200 = 300 000: not strictly greater.
		assertThatIllegalStateException()
			.isThrownBy(() -> TokenBound.check(300_000, Duration.ofMillis(239_800), Duration.ofSeconds(60), COMMAND))
			.withMessageContaining("300000 ms")
			.withMessageContaining("connectionTimeout (239800 ms)")
			.withMessageContaining("socketTimeout (60000 ms)")
			.withMessageContaining("command timeout (100 ms)");
		assertThatIllegalStateException().isThrownBy(
				() -> TokenBound.check(300_000, Duration.ofSeconds(30), Duration.ofSeconds(300), COMMAND));
	}

	@Test
	void socketTimeoutFromTheDataSourceProperties() {
		Properties hikari = new Properties();
		hikari.setProperty("socketTimeout", "60");
		assertThat(TokenBound.socketTimeout(URL, hikari, new Properties())).isEqualTo(Duration.ofSeconds(60));
	}

	@Test
	void theUrlBeatsTheDataSourcePropertiesWhichBeatDriverConfig() {
		Properties defaults = new Properties();
		defaults.setProperty("socketTimeout", "7");
		Properties hikari = new Properties();
		assertThat(TokenBound.socketTimeout(URL, hikari, defaults)).isEqualTo(Duration.ofSeconds(7));
		hikari.setProperty("socketTimeout", "60");
		assertThat(TokenBound.socketTimeout(URL, hikari, defaults)).isEqualTo(Duration.ofSeconds(60));
		assertThat(TokenBound.socketTimeout(URL + "?socketTimeout=90", hikari, defaults))
			.isEqualTo(Duration.ofSeconds(90));
	}

	@Test
	void missingUnparseableOrZeroFailsClosed() {
		assertThatIllegalStateException().isThrownBy(() -> TokenBound.socketTimeout(URL, new Properties(), new Properties()))
			.withMessageContaining("not set");
		Properties zero = new Properties();
		zero.setProperty("socketTimeout", "0");
		assertThatIllegalStateException().isThrownBy(() -> TokenBound.socketTimeout(URL, zero, new Properties()))
			.withMessageContaining("unbounded");
		Properties junk = new Properties();
		junk.setProperty("socketTimeout", "sixty");
		assertThatIllegalStateException().isThrownBy(() -> TokenBound.socketTimeout(URL, junk, new Properties()))
			.withMessageContaining("not an integer");
		assertThatIllegalStateException()
			.isThrownBy(() -> TokenBound.socketTimeout("jdbc:mysql://x/y", new Properties(), new Properties()));
	}

	@Test
	void theDriverConfigOnTheClasspathHasNoSocketTimeout() {
		// D-132 puts socketTimeout on the pool only; driverconfig.properties carries loginTimeout.
		Properties defaults = TokenBound.driverConfigDefaults();
		assertThat(defaults.getProperty("loginTimeout")).isEqualTo("10");
		assertThat(defaults.getProperty("socketTimeout")).isNull();
	}

}
