package dev.aryanyeole.ledgerapi.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import dev.aryanyeole.ledgerapi.cache.RedisOps;
import dev.aryanyeole.ledgerapi.cache.TestRedisGateways;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyCache.Entry;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyCache.Lookup;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyCache.ReadResult;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyCache.Unparseable;

// The fast path's Redis side (D-139, docs/design/cache-invalidation.md §2.1) against a fake RedisOps:
// key and value formats, every unparseable shape, hit vs mismatch vs miss vs bypass, and the
// populate outcomes, each with its counter.
class IdempotencyCacheTest {

	private static final byte[] HASH = HexFormat.of().parseHex("ab".repeat(32));

	private static final byte[] OTHER_HASH = HexFormat.of().parseHex("cd".repeat(32));

	private final FakeRedis redis = new FakeRedis();

	private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

	private final IdempotencyCache cache = new IdempotencyCache(
			TestRedisGateways.over(this.redis, new AtomicLong()::get), Duration.ofMinutes(10), this.meters);

	@Test
	void keyFormatIncludesTheClientIdsUtf8ByteLength() {
		assertThat(IdempotencyCache.key("acme", "order-42:retry")).isEqualTo("ledger:v1:idem:4:acme:order-42:retry");
		// "café" is 4 characters and 5 UTF-8 bytes.
		assertThat(IdempotencyCache.key("café", "k")).isEqualTo("ledger:v1:idem:5:café:k");
		assertThat(IdempotencyCache.key("a:b", "c")).isNotEqualTo(IdempotencyCache.key("a", "b:c"));
	}

	@Test
	void valueRoundTripsAndTheTextMayContainPipes() throws Unparseable {
		String text = "{\"note\": \"a|b|c\"}";
		String value = IdempotencyCache.format(HASH, 201, text);
		assertThat(value).isEqualTo("1|" + "ab".repeat(32) + "|201|" + text);
		Entry entry = IdempotencyCache.parse(value);
		assertThat(entry.requestHash()).isEqualTo(HASH);
		assertThat(entry.status()).isEqualTo(201);
		assertThat(entry.text()).isEqualTo(text);
	}

	@ParameterizedTest
	@ValueSource(strings = { "1|aaaa|201", "2|abababababababababababababababababababababababababababababababab|201|{}",
			"1|ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB|201|{}",
			"1|abababababababababababababababababababababababababababababababa|201|{}",
			"1|abababababababababababababababababababababababababababababababab|abc|{}",
			"1|abababababababababababababababababababababababababababababababab|99|{}",
			"1|abababababababababababababababababababababababababababababababab|600|{}" })
	void unparseableShapes(String value) {
		assertThatExceptionOfType(Unparseable.class).isThrownBy(() -> IdempotencyCache.parse(value));
	}

	@Test
	void hitReturnsTheStoredStatusAndText() {
		this.redis.values.put(IdempotencyCache.key("c", "k"), IdempotencyCache.format(HASH, 422, "{\"code\": \"x\"}"));
		Lookup lookup = this.cache.read("c", "k", HASH);
		assertThat(lookup).isEqualTo(new Lookup(ReadResult.HIT, 422, "{\"code\": \"x\"}"));
		assertThat(reads("hit")).isEqualTo(1);
	}

	@Test
	void aDifferentFingerprintIsAMismatchAndTheEntryIsLeftAsIs() {
		String value = IdempotencyCache.format(HASH, 201, "{}");
		this.redis.values.put(IdempotencyCache.key("c", "k"), value);
		assertThat(this.cache.read("c", "k", OTHER_HASH).result()).isEqualTo(ReadResult.MISMATCH);
		assertThat(this.redis.values.get(IdempotencyCache.key("c", "k"))).isEqualTo(value);
		assertThat(reads("mismatch")).isEqualTo(1);
	}

	@Test
	void missBypassAndUnparseableAreCounted() {
		assertThat(this.cache.read("c", "absent", HASH).result()).isEqualTo(ReadResult.MISS);
		this.redis.values.put(IdempotencyCache.key("c", "junk"), "garbage");
		assertThat(this.cache.read("c", "junk", HASH).result()).isEqualTo(ReadResult.UNPARSEABLE);
		this.redis.failing = true;
		assertThat(this.cache.read("c", "k", HASH).result()).isEqualTo(ReadResult.BYPASS);
		assertThat(List.of(reads("miss"), reads("unparseable"), reads("bypass"))).containsExactly(1.0, 1.0, 1.0);
	}

	@Test
	void populateOutcomes() {
		this.cache.populate("c", "k", HASH, 201, "{}", true);
		assertThat(this.redis.values.get(IdempotencyCache.key("c", "k"))).isEqualTo(IdempotencyCache.format(HASH, 201, "{}"));
		assertThat(this.redis.lastTtlMillis).isEqualTo(Duration.ofMinutes(10).toMillis());
		this.cache.populate("c", "k", HASH, 201, "{\"other\": 1}", true);
		assertThat(this.redis.values.get(IdempotencyCache.key("c", "k"))).as("SET NX never overwrites")
			.isEqualTo(IdempotencyCache.format(HASH, 201, "{}"));
		this.cache.populate("c", "short", HASH, 201, "{}", false);
		assertThat(this.redis.values).doesNotContainKey(IdempotencyCache.key("c", "short"));
		this.redis.failing = true;
		this.cache.populate("c", "down", HASH, 201, "{}", true);
		assertThat(List.of(populates("written"), populates("exists"), populates("skipped_short_life"),
				populates("unavailable")))
			.containsExactly(1.0, 1.0, 1.0, 1.0);
	}

	@Test
	void propertiesMustBePositive() {
		LedgerIdempotencyCacheProperties defaults = new LedgerIdempotencyCacheProperties(false, Duration.ofMinutes(10),
				Duration.ofHours(1));
		assertThat(defaults.minLifeMillis()).isEqualTo(Duration.ofMinutes(70).toMillis());
		assertThatIllegalArgumentException()
			.isThrownBy(() -> new LedgerIdempotencyCacheProperties(true, Duration.ZERO, Duration.ofHours(1)))
			.withMessage("ledger.cache.idempotency.ttl must be positive");
		assertThatIllegalArgumentException()
			.isThrownBy(() -> new LedgerIdempotencyCacheProperties(true, Duration.ofMinutes(10), Duration.ofSeconds(-1)))
			.withMessage("ledger.cache.idempotency.margin must be positive");
	}

	private double reads(String result) {
		return this.meters.counter("ledger.idempotency.cache.read", "result", result).count();
	}

	private double populates(String result) {
		return this.meters.counter("ledger.idempotency.cache.populate", "result", result).count();
	}

	private static final class FakeRedis implements RedisOps {

		final Map<String, String> values = new HashMap<>();

		boolean failing;

		long lastTtlMillis;

		private void check() {
			if (this.failing) {
				throw new IllegalStateException("Command timed out");
			}
		}

		@Override
		public String get(String key) {
			check();
			return this.values.get(key);
		}

		@Override
		public boolean setNxPx(String key, String value, long ttlMillis) {
			check();
			this.lastTtlMillis = ttlMillis;
			return this.values.putIfAbsent(key, value) == null;
		}

		@Override
		public List<String> mget(String... keys) {
			check();
			return Arrays.stream(keys).map(this.values::get).toList();
		}

		@Override
		public long pttl(String key) {
			check();
			return this.values.containsKey(key) ? -1 : -2;
		}

		@Override
		public long eval(String script, List<String> keys, List<String> args) {
			check();
			return 0;
		}

	}

}
