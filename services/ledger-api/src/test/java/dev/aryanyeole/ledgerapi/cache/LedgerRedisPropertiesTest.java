package dev.aryanyeole.ledgerapi.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

// The Redis URL rule (D-137): redis://host[:port], port 1-65535, no user info, an empty path or /0,
// no query. The worker's test_config.py runs the same cases against REDIS_URL.
class LedgerRedisPropertiesTest {

	@ParameterizedTest(name = "{0}")
	@CsvSource({ "redis://localhost, localhost, 6379", "redis://localhost:6380, localhost, 6380",
			"redis://redis:6379/0, redis, 6379", "redis://127.0.0.1:1, 127.0.0.1, 1",
			"redis://h:65535, h, 65535", "REDIS://Host, Host, 6379" })
	void accepts(String url, String host, int port) {
		LedgerRedisProperties properties = new LedgerRedisProperties(url);
		assertThat(properties.host()).isEqualTo(host);
		assertThat(properties.port()).isEqualTo(port);
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = { "", " ", "localhost:6379", "rediss://localhost", "http://localhost", "redis://",
			"redis://:6379", "redis://localhost:0", "redis://localhost:65536", "redis://localhost:abc",
			"redis://localhost/1", "redis://localhost/0/x", "redis://localhost?db=0", "redis://localhost#x",
			"redis://user@localhost", "redis://:secret@localhost", "redis://user:secret@localhost:6379" })
	void rejects(String url) {
		assertThatIllegalArgumentException().isThrownBy(() -> new LedgerRedisProperties(url))
			.withMessageStartingWith("ledger.redis.url ");
	}

	@Test
	void messageNeverEchoesUserInfo() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> new LedgerRedisProperties("redis://admin:hunter2@localhost:6379"))
			.withMessage("ledger.redis.url must not contain user info")
			.satisfies((ex) -> assertThat(ex.getMessage()).doesNotContain("hunter2").doesNotContain("admin"));
	}

}
