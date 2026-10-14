package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.apache.coyote.AbstractProtocol;
import org.springframework.boot.tomcat.TomcatWebServer;
import org.springframework.boot.web.server.context.WebServerApplicationContext;

import com.jayway.jsonpath.JsonPath;
import com.zaxxer.hikari.HikariDataSource;

import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;

// Fires N identical HTTP requests at real Tomcat at the same moment (D-067). JDK HttpClient
// over HTTP/1.1 (its connection pool is unbounded, so each concurrent request has its own
// connection); 100 worker threads park on a start gate, which opens only once all of them
// are parked, so a low peak can't come from thread startup. Per request it records status,
// Content-Type, raw body bytes and any client exception, and tracks the peak number of
// requests the client had in flight (incremented before send, decremented after the
// response). Used by the real proof and by its D-017 negative control alike.
public final class ConcurrentRequestHarness {

	public static final int THREADS = 100;

	/** -Dledger.test.concurrency.rounds: CI 1, local soak 20. */
	public static final String ROUNDS_PROPERTY = "ledger.test.concurrency.rounds";

	private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(60);

	public record Response(int status, String contentType, byte[] body, Throwable exception) {
	}

	public record Run(List<Response> responses, int peakInFlight) {
	}

	private final HttpClient client = HttpClient.newBuilder()
		.version(HttpClient.Version.HTTP_1_1)
		.connectTimeout(Duration.ofSeconds(10))
		.build();

	private final String baseUri;

	public ConcurrentRequestHarness(int port) {
		this.baseUri = "http://localhost:" + port;
	}

	public static int rounds() {
		int rounds = Integer.parseInt(System.getProperty(ROUNDS_PROPERTY, "1"));
		assertThat(rounds).as(ROUNDS_PROPERTY).isPositive();
		return rounds;
	}

	/** The limits actually in effect: Hikari pool size and Tomcat max worker threads. */
	public static String serverLimits(DataSource dataSource, WebServerApplicationContext context) throws Exception {
		int pool = dataSource.unwrap(HikariDataSource.class).getMaximumPoolSize();
		TomcatWebServer webServer = (TomcatWebServer) context.getWebServer();
		int threads = ((AbstractProtocol<?>) webServer.getTomcat().getConnector().getProtocolHandler()).getMaxThreads();
		return "hikari.maximumPoolSize=" + pool + " tomcat.maxThreads=" + threads;
	}

	public static String newKey() {
		return "c-" + UUID.randomUUID();
	}

	/** An authenticated POST with an Idempotency-Key and a JSON body. */
	public HttpRequest post(String path, String idempotencyKey, byte[] body) {
		HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(this.baseUri + path))
			.timeout(REQUEST_TIMEOUT)
			.header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY)
			.header("Content-Type", "application/json")
			.POST(HttpRequest.BodyPublishers.ofByteArray(body));
		if (idempotencyKey != null) {
			builder.header("Idempotency-Key", idempotencyKey);
		}
		return builder.build();
	}

	public Response send(HttpRequest request) {
		try {
			HttpResponse<byte[]> response = this.client.send(request, HttpResponse.BodyHandlers.ofByteArray());
			return new Response(response.statusCode(), response.headers().firstValue("Content-Type").orElse(null),
					response.body(), null);
		}
		catch (Exception ex) {
			if (ex instanceof InterruptedException) {
				Thread.currentThread().interrupt();
			}
			return new Response(-1, null, null, ex);
		}
	}

	/** Creates an account through POST /v1/accounts (no key) and returns its id. */
	public long createAccount(String type) {
		Response response = send(post("/v1/accounts", null,
				("{\"name\": \"conc-" + UUID.randomUUID() + "\", \"type\": \"" + type + "\"}")
					.getBytes(StandardCharsets.UTF_8)));
		assertThat(response.status()).as("create %s account", type).isEqualTo(201);
		return ((Number) JsonPath.read(new String(response.body(), StandardCharsets.UTF_8), "$.account_id"))
			.longValue();
	}

	public void deposit(long accountId, String amount) {
		Response response = send(post("/v1/deposits", newKey(),
				("{\"account_id\": " + accountId + ", \"amount\": \"" + amount + "\"}").getBytes(StandardCharsets.UTF_8)));
		assertThat(response.status()).as("deposit %s to %d", amount, accountId).isEqualTo(201);
	}

	/** Sends THREADS copies of the request at once and waits for all of them. */
	public Run fire(Supplier<HttpRequest> request) throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(THREADS);
		CountDownLatch ready = new CountDownLatch(THREADS);
		CountDownLatch gate = new CountDownLatch(1);
		AtomicInteger inFlight = new AtomicInteger();
		AtomicInteger peak = new AtomicInteger();
		try {
			List<Future<Response>> futures = new ArrayList<>();
			for (int i = 0; i < THREADS; i++) {
				futures.add(pool.submit(() -> {
					HttpRequest httpRequest = request.get();
					ready.countDown();
					gate.await();
					peak.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
					try {
						return send(httpRequest);
					}
					finally {
						inFlight.decrementAndGet();
					}
				}));
			}
			assertThat(ready.await(60, TimeUnit.SECONDS)).as("all %d threads parked on the gate", THREADS).isTrue();
			gate.countDown();
			List<Response> responses = new ArrayList<>();
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
			for (Future<Response> future : futures) {
				responses.add(future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS));
			}
			return new Run(responses, peak.get());
		}
		finally {
			pool.shutdownNow();
		}
	}

}
