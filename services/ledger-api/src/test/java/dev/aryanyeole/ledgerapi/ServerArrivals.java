package dev.aryanyeole.ledgerapi;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import jakarta.servlet.Filter;
import jakarta.servlet.http.HttpServletRequest;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.awaitility.core.ConditionTimeoutException;
import org.springframework.core.Ordered;

// Server-side arrival evidence for the concurrency harness (D-067, BUG-003). A test-only
// filter, first in the chain, records when each request reaches the application and when it
// completes, with the client's port and its Idempotency-Key. A request counts as concurrent
// only if it arrived before the first one completed: client-side timestamps count threads
// calling send(), not arrivals. Same JVM as the client, so nanoTime values compare.
// A request's record is written in the filter's finally, after the response may already have
// reached the client, so a test that reads right after its last response can find records
// missing (BUG-015). Readers therefore pass the number of requests they sent and wait, bounded,
// until exactly that many records carry their keys; too few or too many fails. The remaining bias
// is the other way: completion is stamped after the response is written, so a request arriving in
// that gap counts as overlapping. Overlap figures could only ever be undercounted by a missing
// record, which the wait now rules out.
public class ServerArrivals {

	// How long a reader waits for the records of requests whose responses it already has.
	static final Duration RECORD_WAIT = Duration.ofSeconds(5);

	public record Arrival(String idempotencyKey, long arrivedNanos, long completedNanos, int remotePort) {
	}

	/**
	 * arrivedBeforeFirstCompletion: requests that reached the server before any completed;
	 * spreadMillis: first to last arrival; reusedConnections: client ports already seen in an
	 * earlier round (or the warm-up) of the same test.
	 */
	public record Overlap(int requests, int arrivedBeforeFirstCompletion, long spreadMillis, int connections,
			int reusedConnections) {
	}

	private final ConcurrentLinkedQueue<Arrival> arrivals = new ConcurrentLinkedQueue<>();

	void record(Arrival arrival) {
		this.arrivals.add(arrival);
	}

	/**
	 * Overlap of the expectedRequests requests carrying this Idempotency-Key; adds their ports to
	 * seenPorts.
	 */
	public Overlap overlap(String idempotencyKey, int expectedRequests, Set<Integer> seenPorts) {
		return overlap(Set.of(idempotencyKey), expectedRequests, seenPorts);
	}

	/**
	 * Overlap of the expectedRequests requests carrying any of these Idempotency-Keys (a round where
	 * every request has its own key); adds their ports to seenPorts. Waits up to RECORD_WAIT for
	 * exactly expectedRequests records (BUG-015).
	 */
	public Overlap overlap(Set<String> idempotencyKeys, int expectedRequests, Set<Integer> seenPorts) {
		List<Arrival> round = awaitRecords(idempotencyKeys, expectedRequests);
		long firstCompletion = round.stream().mapToLong(Arrival::completedNanos).min().orElse(Long.MIN_VALUE);
		int before = (int) round.stream().filter((a) -> a.arrivedNanos() < firstCompletion).count();
		long first = round.stream().mapToLong(Arrival::arrivedNanos).min().orElse(0);
		long last = round.stream().mapToLong(Arrival::arrivedNanos).max().orElse(0);
		Set<Integer> ports = round.stream().map(Arrival::remotePort).collect(Collectors.toSet());
		int reused = (int) ports.stream().filter(seenPorts::contains).count();
		seenPorts.addAll(ports);
		return new Overlap(round.size(), before, (last - first) / 1_000_000, ports.size(), reused);
	}

	private List<Arrival> awaitRecords(Set<String> idempotencyKeys, int expectedRequests) {
		try {
			await().atMost(RECORD_WAIT)
				.pollDelay(Duration.ZERO)
				.pollInterval(Duration.ofMillis(10))
				.until(() -> matching(idempotencyKeys).size() >= expectedRequests);
		}
		catch (ConditionTimeoutException ex) {
			throw new AssertionError("arrival records missing: got " + matching(idempotencyKeys).size() + " of "
					+ expectedRequests, ex);
		}
		List<Arrival> round = matching(idempotencyKeys);
		if (round.size() != expectedRequests) {
			throw new AssertionError("arrival records: got " + round.size() + " of " + expectedRequests
					+ " (more than were sent: a request recorded twice)");
		}
		return round;
	}

	private List<Arrival> matching(Set<String> idempotencyKeys) {
		return this.arrivals.stream()
			.filter((a) -> a.idempotencyKey() != null && idempotencyKeys.contains(a.idempotencyKey()))
			.toList();
	}

	/** Registers the recorder and its filter; import into a RANDOM_PORT harness test. */
	@TestConfiguration(proxyBeanMethods = false)
	public static class Configuration {

		@Bean
		ServerArrivals serverArrivals() {
			return new ServerArrivals();
		}

		@Bean
		FilterRegistrationBean<Filter> serverArrivalFilter(ServerArrivals arrivals) {
			Filter filter = (request, response, chain) -> {
				long arrived = System.nanoTime();
				try {
					chain.doFilter(request, response);
				}
				finally {
					HttpServletRequest http = (HttpServletRequest) request;
					arrivals.record(new Arrival(http.getHeader("Idempotency-Key"), arrived, System.nanoTime(),
							http.getRemotePort()));
				}
			};
			FilterRegistrationBean<Filter> registration = new FilterRegistrationBean<>(filter);
			registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
			return registration;
		}

	}

}
