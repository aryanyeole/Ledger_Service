package dev.aryanyeole.ledgerapi.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
import org.testcontainers.containers.GenericContainer;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import dev.aryanyeole.ledgerapi.RedisTestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.TestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.cache.RedisGateway.Reply;

// Lettuce 7.5.2 against redis:7.4.11 (D-136): every command RedisOps sends, through the gateway
// with its real client options, plus the two §2.1 scripts verbatim. INFO and CLIENT INFO go through
// a raw client built with the same options, since RedisOps has neither. Compatibility is tested, not
// assumed: a failure here is a stop-and-decide, never a server downgrade (D-084).
@Import({ TestcontainersConfiguration.class, RedisTestcontainersConfiguration.class })
@SpringBootTest(webEnvironment = WebEnvironment.NONE,
		properties = { "ledger.cache.idempotency.enabled=true", "ledger.cache.balance.enabled=true" })
class RedisClientSmokeTest {

	@Autowired
	RedisGateway gateway;

	@Autowired
	GenericContainer<?> redisTestContainer;

	private static <T> T raw(GenericContainer<?> redis, Function<RedisCommands<String, String>, T> call) {
		RedisClient client = RedisClient.create(RedisGateway.redisUri(redis.getHost(),
				redis.getMappedPort(RedisTestcontainersConfiguration.PORT)));
		client.setOptions(RedisGateway.clientOptions());
		try (StatefulRedisConnection<String, String> connection = client.connect()) {
			return call.apply(connection.sync());
		}
		finally {
			client.shutdown();
		}
	}

	private <T> T ok(Reply<T> reply) {
		assertThat(reply.available()).as("Redis reply available").isTrue();
		return reply.value();
	}

	private boolean setNxPx(String key, String value, long ttlMillis) {
		Reply<Boolean> reply = this.gateway.optional("SET", (r) -> r.setNxPx(key, value, ttlMillis));
		return ok(reply);
	}

	private String get(String key) {
		Reply<String> reply = this.gateway.optional("GET", (r) -> r.get(key));
		return ok(reply);
	}

	private long pttl(String key) {
		Reply<Long> reply = this.gateway.optional("PTTL", (r) -> r.pttl(key));
		return ok(reply);
	}

	private List<String> mget(String... keys) {
		Reply<List<String>> reply = this.gateway.optional("MGET", (r) -> r.mget(keys));
		return ok(reply);
	}

	private long invalidate(List<String> keys, String token) {
		Reply<Long> reply = this.gateway.always("EVAL", (r) -> r.eval(DesignDocScripts.INVALIDATE, keys, List.of(token)));
		return ok(reply);
	}

	private static String id() {
		return UUID.randomUUID().toString();
	}

	@Test
	void serverIsTheD027PinAndTheDefaultProtocolIsResp3() {
		String server = raw(this.redisTestContainer, (r) -> r.info("server"));
		assertThat(server).contains("redis_version:7.4.11");
		String client = raw(this.redisTestContainer, RedisCommands::clientInfo);
		assertThat(client).as("CLIENT INFO under Lettuce's default protocol").contains("resp=3");
	}

	@Test
	void setNxPxWritesOnceAndPttlIsInRange() {
		String key = "smoke:" + id();
		assertThat(setNxPx(key, "first", 60_000)).isTrue();
		assertThat(setNxPx(key, "second", 60_000)).isFalse();
		assertThat(get(key)).isEqualTo("first");
		assertThat(pttl(key)).isBetween(59_001L, 60_000L);
	}

	@Test
	void mgetReturnsNullForAnAbsentKey() {
		String present = "smoke:" + id();
		String absent = "smoke:" + id();
		setNxPx(present, "v", 60_000);
		List<String> values = mget(present, absent);
		assertThat(values).containsExactly("v", null);
	}

	@Test
	void invalidationScriptForOneAccount() {
		String account = id();
		String value = "ledger:v1:bal:" + account;
		String token = "ledger:v1:bal-gen:" + account;
		String fresh = id();
		setNxPx(value, "1|100.0000|2026-11-16T00:00:00.000000Z", 60_000);
		setNxPx(token, id(), 300_000);

		long reply = invalidate(List.of(value, token), fresh);

		assertThat(reply).isEqualTo(2);
		assertThat(get(value)).isNull();
		assertThat(get(token)).isEqualTo(fresh);
		assertThat(pttl(token)).isBetween(299_001L, 300_000L);
	}

	@Test
	void invalidationScriptForThreeAccounts() {
		List<String> keys = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			String account = id();
			String value = "ledger:v1:bal:" + account;
			String token = "ledger:v1:bal-gen:" + account;
			setNxPx(value, "1|5.0000|2026-11-16T00:00:00.000000Z", 60_000);
			keys.add(value);
			keys.add(token);
		}
		String fresh = id();

		long reply = invalidate(keys, fresh);

		assertThat(reply).isEqualTo(6);
		for (int i = 0; i < keys.size(); i += 2) {
			String value = keys.get(i);
			String token = keys.get(i + 1);
			assertThat(get(value)).isNull();
			assertThat(get(token)).isEqualTo(fresh);
			assertThat(pttl(token)).isBetween(299_001L, 300_000L);
		}
	}

	@Test
	void populateScriptWritesOnlyWhileTheTokenIsUnchanged() {
		String entry = "1|42.0000|2026-11-16T00:00:00.000000Z";

		// The token the MGET saw is still there: written, with the value TTL.
		String a = id();
		String tokenA = id();
		setNxPx("ledger:v1:bal-gen:" + a, tokenA, 300_000);
		assertThat(populate(a, tokenA, entry)).isEqualTo(1);
		assertThat(get("ledger:v1:bal:" + a)).isEqualTo(entry);
		assertThat(pttl("ledger:v1:bal:" + a)).isBetween(59_001L,
				60_000L);

		// An invalidation replaced the token in between: nothing written.
		String b = id();
		setNxPx("ledger:v1:bal-gen:" + b, id(), 300_000);
		assertThat(populate(b, id(), entry)).isZero();
		assertThat(get("ledger:v1:bal:" + b)).isNull();

		// No token then, none now: written.
		String c = id();
		assertThat(populate(c, "", entry)).isEqualTo(1);
		assertThat(get("ledger:v1:bal:" + c)).isEqualTo(entry);

		// No token then, one now (an invalidation ran): nothing written.
		String d = id();
		setNxPx("ledger:v1:bal-gen:" + d, id(), 300_000);
		assertThat(populate(d, "", entry)).isZero();
		assertThat(get("ledger:v1:bal:" + d)).isNull();
	}

	private long populate(String account, String tokenSeen, String entry) {
		Reply<Long> reply = this.gateway.optional("EVAL", (r) -> r.eval(DesignDocScripts.POPULATE,
				List.of("ledger:v1:bal:" + account, "ledger:v1:bal-gen:" + account), List.of(tokenSeen, entry, "60000")));
		return ok(reply);
	}

	// What the invalidation script does when Redis is over maxmemory with noeviction (the config
	// D-138 rejects), on its own container because it changes the server's config. Recorded (the
	// NOEVICTION lines) and asserted as observed, along with a control script whose first command is
	// the SET.
	@Test
	void invalidationScriptUnderNoeviction() {
		try (GenericContainer<?> redis = RedisTestcontainersConfiguration.redisContainer()) {
			redis.start();
			// Printed in finally, so the record survives a failed assertion.
			List<String> lines = new ArrayList<>();
			try {
				raw(redis, (r) -> {
					r.set("ledger:v1:bal:1", "1|100.0000|2026-11-16T00:00:00.000000Z");
					r.set("ledger:v1:bal-gen:1", "old-token-1");
					r.set("ledger:v1:bal:2", "1|200.0000|2026-11-16T00:00:00.000000Z");
					r.set("ledger:v1:bal-gen:2", "old-token-2");
					r.set("ledger:v1:bal-gen:3", "old-token-3");
					r.set("ledger:v1:bal-gen:5", "token-5");
					r.configSet("maxmemory-policy", "noeviction");
					long used = memory(r, "used_memory");
					r.configSet("maxmemory", Long.toString(used + 1024 * 1024));
					lines.add("before fill: used_memory=" + used + " maxmemory=" + memory(r, "maxmemory"));
					String filler = "x".repeat(10_000);
					int filled = 0;
					String oom = null;
					for (; filled < 100_000; filled++) {
						try {
							r.set("filler:" + filled, filler);
						}
						catch (RedisCommandExecutionException ex) {
							oom = ex.getMessage();
							break;
						}
					}
					lines.add("fill: " + filled + " SETs succeeded, then: " + oom);
					lines.add("after fill: used_memory=" + memory(r, "used_memory") + " maxmemory=" + memory(r, "maxmemory"));
					assertThat(oom).as("the fill ended on an OOM refusal").startsWith("OOM command not allowed");
					long maxmemory = memory(r, "maxmemory");

					// Observed: Redis refuses a denyoom command in a script only while the script hasn't
					// written yet. The DEL is a write, so the SET after it is allowed: the whole script runs
					// and Redis ends up past maxmemory.
					String invalidation = eval(r, DesignDocScripts.INVALIDATE, "ledger:v1:bal:1", "ledger:v1:bal-gen:1",
							"new-token-1");
					long afterInvalidation = memory(r, "used_memory");
					lines.add("invalidation script (DEL then SET), account 1: " + invalidation);
					lines.add("  ledger:v1:bal:1 exists=" + r.exists("ledger:v1:bal:1") + "; ledger:v1:bal-gen:1="
							+ r.get("ledger:v1:bal-gen:1") + "; used_memory=" + afterInvalidation);
					assertThat(invalidation).isEqualTo("returned 2");
					assertThat(r.exists("ledger:v1:bal:1")).isZero();
					assertThat(r.get("ledger:v1:bal-gen:1")).isEqualTo("new-token-1");
					assertThat(afterInvalidation).as("used_memory past maxmemory").isGreaterThan(maxmemory);

					// Control: the same commands with the SET first is refused before writing anything.
					String control = eval(r, SET_FIRST, "ledger:v1:bal:2", "ledger:v1:bal-gen:2", "new-token-2");
					lines.add("control script (SET then DEL), account 2: " + control);
					lines.add("  ledger:v1:bal:2 exists=" + r.exists("ledger:v1:bal:2") + "; ledger:v1:bal-gen:2="
							+ r.get("ledger:v1:bal-gen:2") + "; used_memory=" + memory(r, "used_memory"));
					assertThat(control).startsWith("error: OOM command not allowed");
					assertThat(r.exists("ledger:v1:bal:2")).isOne();
					assertThat(r.get("ledger:v1:bal-gen:2")).isEqualTo("old-token-2");

					// (a) The common case: nothing cached for the account, its token present. A DEL that deletes
					// nothing still counts as the script's first write.
					String valueAbsent = eval(r, DesignDocScripts.INVALIDATE, "ledger:v1:bal:3", "ledger:v1:bal-gen:3",
							"new-token-3");
					lines.add("(a) invalidation, value absent, token present: " + valueAbsent);
					lines.add("  ledger:v1:bal:3 exists=" + r.exists("ledger:v1:bal:3") + "; ledger:v1:bal-gen:3="
							+ r.get("ledger:v1:bal-gen:3"));
					assertThat(valueAbsent).isEqualTo("returned 2");
					assertThat(r.get("ledger:v1:bal-gen:3")).isEqualTo("new-token-3");

					// (b) An account never read: neither key present.
					String bothAbsent = eval(r, DesignDocScripts.INVALIDATE, "ledger:v1:bal:4", "ledger:v1:bal-gen:4",
							"new-token-4");
					lines.add("(b) invalidation, neither key present: " + bothAbsent);
					lines.add("  ledger:v1:bal:4 exists=" + r.exists("ledger:v1:bal:4") + "; ledger:v1:bal-gen:4="
							+ r.get("ledger:v1:bal-gen:4"));
					assertThat(bothAbsent).isEqualTo("returned 2");
					assertThat(r.get("ledger:v1:bal-gen:4")).isEqualTo("new-token-4");

					// (c) The populate script: its first write is the SET.
					String populate = eval(r, DesignDocScripts.POPULATE, "ledger:v1:bal:5", "ledger:v1:bal-gen:5", "token-5",
							"1|500.0000|2026-11-16T00:00:00.000000Z", "60000");
					lines.add("(c) populate script, token matches: " + populate);
					lines.add("  ledger:v1:bal:5 exists=" + r.exists("ledger:v1:bal:5"));
					assertThat(populate).startsWith("error: OOM command not allowed");
					assertThat(r.exists("ledger:v1:bal:5")).isZero();

					// (d) The fast path's SET NX PX.
					String setNx;
					try {
						setNx = "returned " + r.set("ledger:v1:idem:4:acme:k", "1|x|201|{}", SetArgs.Builder.nx().px(600_000));
					}
					catch (RedisCommandExecutionException ex) {
						setNx = "error: " + ex.getMessage();
					}
					lines.add("(d) SET NX PX: " + setNx);
					lines.add("  ledger:v1:idem:4:acme:k exists=" + r.exists("ledger:v1:idem:4:acme:k"));
					assertThat(setNx).startsWith("error: OOM command not allowed");
					assertThat(r.exists("ledger:v1:idem:4:acme:k")).isZero();

					long deleted = r.del("filler:0");
					lines.add("plain DEL filler:0 -> " + deleted);
					assertThat(deleted).as("a plain DEL is allowed while over maxmemory").isOne();
				return null;
				});
			}
			finally {
				lines.forEach((line) -> System.out.println("NOEVICTION " + line));
			}
		}
	}

	// The invalidation script's commands in the other order: the control for "a script that hasn't
	// written yet".
	private static final String SET_FIRST = """
			for i = 1, #KEYS, 2 do
			  redis.call('SET', KEYS[i + 1], ARGV[1], 'PX', 300000)
			  redis.call('DEL', KEYS[i])
			end
			return #KEYS
			""";

	private static String eval(RedisCommands<String, String> r, String script, String value, String token,
			String... args) {
		try {
			Long reply = r.eval(script, ScriptOutputType.INTEGER, new String[] { value, token }, args);
			return "returned " + reply;
		}
		catch (RedisCommandExecutionException ex) {
			return "error: " + ex.getMessage();
		}
	}

	private static long memory(RedisCommands<String, String> r, String field) {
		for (String line : r.info("memory").split("\r?\n")) {
			if (line.startsWith(field + ":")) {
				return Long.parseLong(line.substring(field.length() + 1).strip());
			}
		}
		throw new IllegalStateException("INFO memory has no " + field);
	}

}
