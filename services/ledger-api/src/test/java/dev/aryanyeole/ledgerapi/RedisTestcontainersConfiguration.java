package dev.aryanyeole.ledgerapi;

import java.time.Duration;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

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

	// A test harness's connection to its own Redis: database 0 with a patient 5 s timeout, which also
	// bounds Lettuce's connection initialization (the HELLO handshake). The gateway's 100 ms budget
	// (D-137) is a production choice for request paths that must never wait on Redis, and is
	// deliberately not reused for readiness probes and test-side reads (BUG-016).
	public static final Duration PATIENT_TIMEOUT = Duration.ofSeconds(5);

	/** Database 0 of this container, with PATIENT_TIMEOUT. */
	public static RedisURI patientUri(GenericContainer<?> redis) {
		return RedisURI.builder()
			.withHost(redis.getHost())
			.withPort(redis.getMappedPort(PORT))
			.withDatabase(0)
			.withTimeout(PATIENT_TIMEOUT)
			.build();
	}

	/**
	 * Fails context startup unless the context's Redis holds no keys (D-138). A readiness probe needs
	 * patience: the first connection to a just-started container can be slow under load (BUG-016), so
	 * connect + DBSIZE is retried until READY_DEADLINE, failing with the last exception.
	 */
	public static final class EmptyRedis {

		static final Duration READY_DEADLINE = Duration.ofSeconds(30);

		EmptyRedis(GenericContainer<?> redis) {
			redis.start();
			long keys = dbsize(redis);
			if (keys != 0) {
				throw new IllegalStateException("test Redis must start empty (D-138), DBSIZE = " + keys);
			}
		}

		private static long dbsize(GenericContainer<?> redis) {
			long deadline = System.nanoTime() + READY_DEADLINE.toNanos();
			RuntimeException last = null;
			while (System.nanoTime() - deadline < 0) {
				RedisClient client = RedisClient.create(patientUri(redis));
				try (StatefulRedisConnection<String, String> connection = client.connect()) {
					return connection.sync().dbsize();
				}
				catch (RuntimeException ex) {
					last = ex;
				}
				finally {
					client.shutdown();
				}
				try {
					Thread.sleep(200);
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					break;
				}
			}
			throw new IllegalStateException("test Redis not ready after " + READY_DEADLINE.toSeconds() + " s", last);
		}

	}

}
