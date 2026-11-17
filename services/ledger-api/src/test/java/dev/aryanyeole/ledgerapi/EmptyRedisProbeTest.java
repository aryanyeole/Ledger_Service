package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import com.github.dockerjava.api.DockerClient;

// BUG-016's regression test: the D-138 empty-Redis check must ride out a test Redis that is slow to
// answer its first connection. A paused container still accepts TCP (the kernel answers the
// handshake) but doesn't answer HELLO, which is exactly what "Connection initialization timed out"
// reported; here the pause lasts 500 ms, the check must wait it out, and the elapsed time proves the
// pause really held it.
class EmptyRedisProbeTest {

	private static final Duration PAUSE = Duration.ofMillis(500);

	@Test
	void emptyCheckWaitsOutASlowFirstConnection() throws Exception {
		DockerClient docker = DockerClientFactory.instance().client();
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		try (GenericContainer<?> redis = RedisTestcontainersConfiguration.redisContainer()) {
			redis.start();
			String id = redis.getContainerId();
			docker.pauseContainerCmd(id).exec();
			try {
				scheduler.schedule(() -> docker.unpauseContainerCmd(id).exec(), PAUSE.toMillis(), TimeUnit.MILLISECONDS);
				long started = System.nanoTime();
				new RedisTestcontainersConfiguration.EmptyRedis(redis);
				long elapsedMs = (System.nanoTime() - started) / 1_000_000;
				System.out.println("EMPTY_REDIS_PROBE paused " + PAUSE.toMillis() + " ms, check passed after " + elapsedMs + " ms");
				assertThat(elapsedMs).as("the check waited out the pause").isGreaterThanOrEqualTo(PAUSE.toMillis() - 50);
			}
			finally {
				scheduler.shutdown();
				scheduler.awaitTermination(5, TimeUnit.SECONDS);
				if (Boolean.TRUE.equals(docker.inspectContainerCmd(id).exec().getState().getPaused())) {
					docker.unpauseContainerCmd(id).exec();
				}
			}
		}
	}

}
