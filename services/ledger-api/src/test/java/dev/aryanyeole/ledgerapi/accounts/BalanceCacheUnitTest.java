package dev.aryanyeole.ledgerapi.accounts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import dev.aryanyeole.ledgerapi.accounts.BalanceCache.Entry;
import dev.aryanyeole.ledgerapi.accounts.BalanceCache.Lookup;
import dev.aryanyeole.ledgerapi.accounts.BalanceCache.ReadResult;
import dev.aryanyeole.ledgerapi.accounts.BalanceCache.Unparseable;
import dev.aryanyeole.ledgerapi.cache.BalanceCacheScripts;
import dev.aryanyeole.ledgerapi.cache.RedisOps;
import dev.aryanyeole.ledgerapi.cache.TestRedisGateways;
import dev.aryanyeole.ledgerapi.posting.BalanceCacheInvalidator;

// The balance cache's Redis side (D-142, docs/design/cache-invalidation.md §2.1, §3.5, §4.2) against
// a fake RedisOps that runs both scripts' logic: key and value literals, the parse round trip and
// every unparseable shape, hit vs miss vs bypass vs unparseable, populate written vs stale_token vs
// unavailable, and the invalidation's KEYS and ARGV for a multi-account posting. Each with its counter.
class BalanceCacheUnitTest {

	private static final OffsetDateTime AS_OF = OffsetDateTime.of(2026, 11, 16, 7, 5, 9, 123_456_000, ZoneOffset.UTC);

	private final FakeRedis redis = new FakeRedis();

	private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

	private final LedgerBalanceCacheProperties properties = new LedgerBalanceCacheProperties(true,
			Duration.ofSeconds(60), true);

	private final BalanceCache cache = new BalanceCache(TestRedisGateways.over(this.redis, new AtomicLong()::get),
			this.properties, this.meters);

	private final BalanceCacheInvalidator invalidator = new BalanceCacheInvalidator(
			TestRedisGateways.over(this.redis, new AtomicLong()::get), this.meters);

	@Test
	void keyLiterals() {
		assertThat(BalanceCacheScripts.valueKey(42)).isEqualTo("ledger:v1:bal:42");
		assertThat(BalanceCacheScripts.tokenKey(42)).isEqualTo("ledger:v1:bal-gen:42");
	}

	@Test
	void valueLiteralsAreScale4AndTheD097Format() {
		assertThat(BalanceCache.format(new BigDecimal("100.0000"), AS_OF))
			.isEqualTo("1|100.0000|2026-11-16T07:05:09.123456Z");
		assertThat(BalanceCache.format(new BigDecimal("-250.5000"), AS_OF))
			.isEqualTo("1|-250.5000|2026-11-16T07:05:09.123456Z");
		// Zero fraction still prints six digits; another offset is converted to UTC.
		OffsetDateTime pacific = OffsetDateTime.of(2026, 11, 16, 0, 5, 9, 0, ZoneOffset.ofHours(-7));
		assertThat(BalanceCache.format(new BigDecimal("0.0000"), pacific)).isEqualTo("1|0.0000|2026-11-16T07:05:09.000000Z");
	}

	@Test
	void parseRoundTrips() throws Unparseable {
		Entry entry = BalanceCache.parse(BalanceCache.format(new BigDecimal("-250.5000"), AS_OF));
		assertThat(entry.balance()).isEqualTo(new BigDecimal("-250.5000"));
		assertThat(entry.asOf()).isEqualTo(AS_OF);
		assertThat(entry.asOf().getOffset()).isEqualTo(ZoneOffset.UTC);
	}

	@ParameterizedTest
	@ValueSource(strings = { "2|100.0000|2026-11-16T07:05:09.123456Z", "1|100.0000", "1|100.0000|2026-11-16T07:05:09.123456Z|x",
			"1|100.00|2026-11-16T07:05:09.123456Z", "1|100.00000|2026-11-16T07:05:09.123456Z", "1|1e2|2026-11-16T07:05:09.123456Z",
			"1|100.0000|2026-11-16T07:05:09.123Z", "1|100.0000|2026-11-16T07:05:09.123456+00:00",
			"1|100.0000|2026-11-16T07:05:09.123456", "1|100.0000|2026-13-16T07:05:09.123456Z", "" })
	void unparseableShapes(String value) {
		assertThatExceptionOfType(Unparseable.class).isThrownBy(() -> BalanceCache.parse(value));
	}

	@Test
	void hitReturnsTheEntryAndTheToken() {
		this.redis.values.put("ledger:v1:bal:7", "1|12.3000|2026-11-16T07:05:09.123456Z");
		this.redis.values.put("ledger:v1:bal-gen:7", "token-7");
		Lookup lookup = this.cache.read(7);
		assertThat(lookup.result()).isEqualTo(ReadResult.HIT);
		assertThat(lookup.entry().balance()).isEqualTo(new BigDecimal("12.3000"));
		assertThat(lookup.entry().asOf()).isEqualTo(AS_OF);
		assertThat(this.redis.mgets).containsExactly(List.of("ledger:v1:bal:7", "ledger:v1:bal-gen:7"));
		assertThat(reads("hit")).isEqualTo(1);
	}

	@Test
	void missCarriesTheTokenOrEmptyForAbsent() {
		this.redis.values.put("ledger:v1:bal-gen:7", "token-7");
		assertThat(this.cache.read(7)).isEqualTo(new Lookup(ReadResult.MISS, null, "token-7"));
		assertThat(this.cache.read(8)).isEqualTo(new Lookup(ReadResult.MISS, null, ""));
		assertThat(reads("miss")).isEqualTo(2);
	}

	@Test
	void unparseableIsCountedAndCarriesTheToken() {
		this.redis.values.put("ledger:v1:bal:7", "1|12.30|2026-11-16T07:05:09.123456Z");
		this.redis.values.put("ledger:v1:bal-gen:7", "token-7");
		assertThat(this.cache.read(7)).isEqualTo(new Lookup(ReadResult.UNPARSEABLE, null, "token-7"));
		assertThat(reads("unparseable")).isEqualTo(1);
		assertThat(reads("miss")).isZero();
	}

	@Test
	void unavailableIsABypass() {
		this.redis.down = true;
		assertThat(this.cache.read(7).result()).isEqualTo(ReadResult.BYPASS);
		assertThat(reads("bypass")).isEqualTo(1);
	}

	@Test
	void populateWritesOnlyWhileTheTokenIsUnchanged() {
		// Token absent at MGET and still absent: written, with the value TTL.
		this.cache.populate(7, "", new BigDecimal("12.3000"), AS_OF);
		assertThat(this.redis.values).containsEntry("ledger:v1:bal:7", "1|12.3000|2026-11-16T07:05:09.123456Z");
		assertThat(this.redis.evals.get(0)).isEqualTo(new Eval(BalanceCacheScripts.POPULATE,
				List.of("ledger:v1:bal:7", "ledger:v1:bal-gen:7"), List.of("", "1|12.3000|2026-11-16T07:05:09.123456Z", "60000")));

		// An invalidation ran in between: the token changed, nothing is written.
		this.redis.values.put("ledger:v1:bal-gen:8", "token-after");
		this.cache.populate(8, "token-before", new BigDecimal("1.0000"), AS_OF);
		assertThat(this.redis.values).doesNotContainKey("ledger:v1:bal:8");

		// Absent at MGET, then a token appeared: absent is compared exactly.
		this.cache.populate(8, "", new BigDecimal("1.0000"), AS_OF);
		assertThat(this.redis.values).doesNotContainKey("ledger:v1:bal:8");

		// Same token: written.
		this.cache.populate(8, "token-after", new BigDecimal("1.0000"), AS_OF);
		assertThat(this.redis.values).containsKey("ledger:v1:bal:8");

		this.redis.down = true;
		this.cache.populate(9, "", new BigDecimal("1.0000"), AS_OF);

		assertThat(populates("written")).isEqualTo(2);
		assertThat(populates("stale_token")).isEqualTo(2);
		assertThat(populates("unavailable")).isEqualTo(1);
	}

	@Test
	void invalidationKeysArePairsInAscendingIdOrderWithAFreshUuid() {
		this.redis.values.put("ledger:v1:bal:3", "1|1.0000|2026-11-16T07:05:09.123456Z");
		this.invalidator.invalidate(List.of(30L, 3L, 12L, 3L));
		Eval eval = this.redis.evals.get(0);
		assertThat(eval.script()).isEqualTo(BalanceCacheScripts.INVALIDATE);
		assertThat(eval.keys()).containsExactly("ledger:v1:bal:3", "ledger:v1:bal-gen:3", "ledger:v1:bal:12",
				"ledger:v1:bal-gen:12", "ledger:v1:bal:30", "ledger:v1:bal-gen:30");
		assertThat(eval.args()).hasSize(1);
		assertThat(eval.args().get(0)).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
		assertThat(this.redis.values).doesNotContainKey("ledger:v1:bal:3");
		assertThat(this.redis.values).containsEntry("ledger:v1:bal-gen:30", eval.args().get(0));

		this.invalidator.invalidate(List.of(3L));
		assertThat(this.redis.evals.get(1).args().get(0)).as("a fresh token per call").isNotEqualTo(eval.args().get(0));

		this.redis.down = true;
		this.invalidator.invalidate(List.of(3L));
		assertThat(this.meters.counter("ledger.balance.cache.invalidation", "result", "sent").count()).isEqualTo(2);
		assertThat(this.meters.counter("ledger.balance.cache.invalidation", "result", "failed").count()).isEqualTo(1);
	}

	private double reads(String result) {
		return this.meters.counter("ledger.balance.cache.read", "result", result).count();
	}

	private double populates(String result) {
		return this.meters.counter("ledger.balance.cache.populate", "result", result).count();
	}

	record Eval(String script, List<String> keys, List<String> args) {
	}

	// Runs the two scripts' logic by identity; any other script is a test bug.
	static final class FakeRedis implements RedisOps {

		final Map<String, String> values = new HashMap<>();

		final List<List<String>> mgets = new ArrayList<>();

		final List<Eval> evals = new ArrayList<>();

		boolean down;

		private void check() {
			if (this.down) {
				throw new IllegalStateException("redis down");
			}
		}

		@Override
		public String get(String key) {
			check();
			return this.values.get(key);
		}

		@Override
		public boolean setNxPx(String key, String value, long ttlMillis) {
			throw new UnsupportedOperationException();
		}

		@Override
		public List<String> mget(String... keys) {
			check();
			this.mgets.add(List.of(keys));
			return Arrays.stream(keys).map(this.values::get).toList();
		}

		@Override
		public long pttl(String key) {
			throw new UnsupportedOperationException();
		}

		@Override
		public long eval(String script, List<String> keys, List<String> args) {
			check();
			this.evals.add(new Eval(script, keys, args));
			if (script.equals(BalanceCacheScripts.POPULATE)) {
				String token = this.values.getOrDefault(keys.get(1), "");
				if (!token.equals(args.get(0))) {
					return 0;
				}
				this.values.put(keys.get(0), args.get(1));
				return 1;
			}
			if (script.equals(BalanceCacheScripts.INVALIDATE)) {
				for (int i = 0; i < keys.size(); i += 2) {
					this.values.remove(keys.get(i));
					this.values.put(keys.get(i + 1), args.get(0));
				}
				return keys.size();
			}
			throw new AssertionError("unexpected script");
		}

	}

}
