package dev.aryanyeole.ledgerapi.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;

import dev.aryanyeole.ledgerapi.RedisTestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.TestcontainersConfiguration;
import dev.aryanyeole.ledgerapi.cache.RedisGateway.Reply;

// ledger-api with no Redis to talk to (D-137, docs/design/cache-invalidation.md §5 row 6). The context
// starts with Redis down, because the connection is made on first use; calls are unavailable, never
// errors; once Redis starts, calls succeed without a restart. Against a server that accepts TCP but
// never answers (a paused Redis during connect), concurrent callers never queue behind the connect:
// one attempt per bypass window, everyone else returns at once. Doesn't import
// RedisTestcontainersConfiguration: its DBSIZE check needs a running Redis at context start.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(webEnvironment = WebEnvironment.NONE,
		properties = { "ledger.cache.idempotency.enabled=true", "ledger.cache.balance.enabled=true" })
class RedisDownStartupTest {

	// Nothing listens here until contextStartsWithRedisDownAndRecoversWhenRedisStarts starts a container on it.
	private static final int DOWN_PORT = freePort();

	private static final int CALLERS = 20;

	// One connect attempt: TCP connect plus the handshake, generously padded for a loaded CI runner.
	private static final Duration ONE_CONNECT_BOUND = RedisGateway.CONNECT_TIMEOUT.plus(RedisGateway.COMMAND_TIMEOUT)
		.plusMillis(700);

	@DynamicPropertySource
	static void redisUrl(DynamicPropertyRegistry registry) {
		registry.add("ledger.redis.url", () -> "redis://127.0.0.1:" + DOWN_PORT);
	}

	@Autowired
	RedisGateway gateway;

	private static int freePort() {
		try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			return socket.getLocalPort();
		}
		catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

	private static RedisClient rawClient(int port) {
		RedisClient client = RedisClient.create(RedisGateway.redisUri("127.0.0.1", port));
		client.setOptions(RedisGateway.clientOptions());
		return client;
	}

	@Test
	void connectThrowsWhileRedisIsDown() {
		RedisClient client = rawClient(DOWN_PORT);
		try {
			// Twice: the first includes the client's one-time Netty start-up.
			for (String attempt : List.of("first", "second")) {
				long started = System.nanoTime();
				assertThatThrownBy(client::connect).isInstanceOf(RedisConnectionException.class);
				System.out.printf("REDIS_DOWN %s connect to a closed port threw after %d ms%n", attempt,
						(System.nanoTime() - started) / 1_000_000);
			}
		}
		finally {
			client.shutdown();
		}
	}

	@Test
	void contextStartsWithRedisDownAndRecoversWhenRedisStarts() throws Exception {
		Reply<String> read = this.gateway.optional("GET", (r) -> r.get("k"));
		Reply<String> invalidation = this.gateway.always("EVAL", (r) -> r.get("k"));
		assertThat(read.available()).isFalse();
		assertThat(invalidation.available()).isFalse();

		try (GenericContainer<?> redis = containerOn(DOWN_PORT)) {
			redis.start();
			long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
			Reply<Boolean> written = Reply.unavailable();
			while (!written.available() && System.nanoTime() < deadline) {
				Thread.sleep(100);
				written = this.gateway.optional("SET", (r) -> r.setNxPx("recovered", "yes", 60_000));
			}
			assertThat(written).as("a call succeeds once Redis is up, without a restart").isEqualTo(Reply.of(true));
			Reply<String> value = this.gateway.optional("GET", (r) -> r.get("recovered"));
			assertThat(value).isEqualTo(Reply.of("yes"));
		}
	}

	// redis:7.4.11 bound to a fixed loopback port, so the gateway's URL was valid before it started.
	private static GenericContainer<?> containerOn(int port) {
		ExposedPort redisPort = ExposedPort.tcp(RedisTestcontainersConfiguration.PORT);
		return new GenericContainer<>(DockerImageName.parse(RedisTestcontainersConfiguration.IMAGE))
			.withCommand(RedisTestcontainersConfiguration.COMMAND)
			.withCreateContainerCmdModifier((cmd) -> {
				HostConfig hostConfig = (cmd.getHostConfig() != null) ? cmd.getHostConfig() : new HostConfig();
				cmd.withExposedPorts(redisPort)
					.withHostConfig(hostConfig.withPortBindings(
							new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", port), redisPort)));
			})
			.waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1));
	}

	@Test
	void concurrentCallersNeverQueueBehindAConnectToASilentServer() throws Exception {
		try (SilentServer silent = new SilentServer()) {
			// What bounds Lettuce's connect against a server that accepts TCP and never answers HELLO.
			RedisClient client = rawClient(silent.port());
			try {
				long started = System.nanoTime();
				Throwable thrown = null;
				try {
					client.connect().close();
				}
				catch (RuntimeException ex) {
					thrown = ex;
				}
				long elapsed = (System.nanoTime() - started) / 1_000_000;
				assertThat(thrown).as("connect to a silent server").isInstanceOf(RedisConnectionException.class);
				System.out.printf("SILENT_SERVER raw connect threw after %d ms: %s%n", elapsed, chain(thrown));
			}
			finally {
				client.shutdown();
			}

			RedisGateway gateway = new RedisGateway(new LedgerRedisProperties("redis://127.0.0.1:" + silent.port()));
			try {
				List<Long> elapsedMillis = burst(gateway);
				Collections.sort(elapsedMillis);
				long slowest = elapsedMillis.get(elapsedMillis.size() - 1);
				System.out.printf("SILENT_SERVER %d concurrent always() calls, elapsed ms sorted: %s; connect attempts %d%n",
						CALLERS, elapsedMillis, gateway.connectAttempts());
				assertThat(slowest).as("the slowest caller waits at most one connect, not %d", CALLERS)
					.isLessThan(ONE_CONNECT_BOUND.toMillis());
				assertThat(gateway.connectAttempts()).as("one connect attempt for the whole burst").isEqualTo(1);

				// The failed attempt opened the bypass: no new attempt starts inside the window, for
				// optional() or always().
				assertThat(gateway.optional("GET", (r) -> r.get("k")).available()).isFalse();
				assertThat(gateway.always("EVAL", (r) -> r.get("k")).available()).isFalse();
				assertThat(gateway.connectAttempts()).as("no attempt inside the bypass window").isEqualTo(1);

				// After the window, exactly one more attempt.
				Thread.sleep(RedisGateway.BYPASS.toMillis() + 100);
				assertThat(burst(gateway)).allMatch((millis) -> millis < ONE_CONNECT_BOUND.toMillis());
				assertThat(gateway.connectAttempts()).as("one attempt in the next window").isEqualTo(2);
			}
			finally {
				gateway.close();
			}
		}
	}

	// CALLERS concurrent always() calls behind a start gate; every one must be unavailable.
	private static List<Long> burst(RedisGateway gateway) throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(CALLERS);
		try {
			CountDownLatch ready = new CountDownLatch(CALLERS);
			CountDownLatch start = new CountDownLatch(1);
			List<Future<Long>> calls = new ArrayList<>();
			for (int i = 0; i < CALLERS; i++) {
				calls.add(pool.submit(() -> {
					ready.countDown();
					start.await();
					long started = System.nanoTime();
					Reply<Long> reply = gateway.always("EVAL",
							(r) -> r.eval(DesignDocScripts.INVALIDATE, List.of("ledger:v1:bal:1", "ledger:v1:bal-gen:1"),
									List.of("token")));
					assertThat(reply.available()).isFalse();
					return (System.nanoTime() - started) / 1_000_000;
				}));
			}
			assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
			start.countDown();
			List<Long> elapsed = new ArrayList<>();
			for (Future<Long> call : calls) {
				elapsed.add(call.get(30, TimeUnit.SECONDS));
			}
			return elapsed;
		}
		finally {
			pool.shutdownNow();
		}
	}

	private static String chain(Throwable thrown) {
		StringBuilder text = new StringBuilder();
		for (Throwable t = thrown; t != null; t = t.getCause()) {
			if (!text.isEmpty()) {
				text.append(" <- ");
			}
			text.append(t.getClass().getName()).append(": ").append(t.getMessage());
		}
		return text.toString();
	}

	// Accepts TCP connections and never writes a byte: what a paused Redis looks like to a client
	// that is connecting.
	private static final class SilentServer implements AutoCloseable {

		private final ServerSocket server;

		private final List<Socket> accepted = Collections.synchronizedList(new ArrayList<>());

		private final Thread acceptor;

		SilentServer() throws IOException {
			this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
			this.acceptor = new Thread(() -> {
				while (!this.server.isClosed()) {
					try {
						this.accepted.add(this.server.accept());
					}
					catch (IOException ex) {
						return;
					}
				}
			}, "silent-redis");
			this.acceptor.setDaemon(true);
			this.acceptor.start();
		}

		int port() {
			return this.server.getLocalPort();
		}

		@Override
		public void close() throws IOException {
			this.server.close();
			synchronized (this.accepted) {
				for (Socket socket : this.accepted) {
					socket.close();
				}
			}
		}

	}

}
