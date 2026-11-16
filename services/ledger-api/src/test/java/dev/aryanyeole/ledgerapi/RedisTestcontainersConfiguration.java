package dev.aryanyeole.ledgerapi;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

import dev.aryanyeole.ledgerapi.cache.RedisGateway;

// Redis for test contexts that turn a cache on (D-138): redis:7.4.11 pinned per D-027, with
// Compose's pure-cache flags. One container per Spring context, so every test Redis belongs to
// exactly one Postgres; imported only by test classes that set a ledger.cache.*.enabled switch to
// true. The context refuses to start unless the Redis is empty (DBSIZE = 0), so a reused or shared
// server can't serve one database's keys to another.
@TestConfiguration(proxyBeanMethods = false)
public class RedisTestcontainersConfiguration {

	public static final String IMAGE = "redis:7.4.11";

	public static final String[] COMMAND = { "redis-server", "--save", "", "--appendonly", "no", "--maxmemory",
			"256mb", "--maxmemory-policy", "allkeys-lru" };

	public static final int PORT = 6379;

	/** A not-yet-started container with the D-138 flags. */
	public static GenericContainer<?> redisContainer() {
		return new GenericContainer<>(DockerImageName.parse(IMAGE)).withCommand(COMMAND)
			.withExposedPorts(PORT)
			.waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1));
	}

	@Bean
	GenericContainer<?> redisTestContainer() {
		return redisContainer();
	}

	@Bean
	DynamicPropertyRegistrar redisProperties(GenericContainer<?> redisTestContainer) {
		return (registry) -> registry.add("ledger.redis.url",
				() -> "redis://" + redisTestContainer.getHost() + ":" + redisTestContainer.getMappedPort(PORT));
	}

	@Bean
	EmptyRedis emptyRedis(GenericContainer<?> redisTestContainer) {
		return new EmptyRedis(redisTestContainer);
	}

	/** Fails context startup unless the context's Redis holds no keys (D-138). */
	public static final class EmptyRedis {

		EmptyRedis(GenericContainer<?> redis) {
			redis.start();
			RedisClient client = RedisClient
				.create(RedisGateway.redisUri(redis.getHost(), redis.getMappedPort(PORT)));
			try (StatefulRedisConnection<String, String> connection = client.connect()) {
				long keys = connection.sync().dbsize();
				if (keys != 0) {
					throw new IllegalStateException("test Redis must start empty (D-138), DBSIZE = " + keys);
				}
			}
			finally {
				client.shutdown();
			}
		}

	}

}
