package dev.aryanyeole.ledgerapi;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import jakarta.servlet.Filter;
import jakarta.servlet.http.HttpServletRequest;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;

// Server-side arrival evidence for the concurrency harness (D-067, BUG-003). A test-only
// filter, first in the chain, records when each request reaches the application and when it
// completes, with the client's port and its Idempotency-Key. A request counts as concurrent
// only if it arrived before the first one completed: client-side timestamps count threads
// calling send(), not arrivals. Same JVM as the client, so nanoTime values compare.
public class ServerArrivals {

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

	/** Overlap of the requests carrying this Idempotency-Key; adds their ports to seenPorts. */
	public Overlap overlap(String idempotencyKey, Set<Integer> seenPorts) {
		return overlap(Set.of(idempotencyKey), seenPorts);
	}

	/**
	 * Overlap of the requests carrying any of these Idempotency-Keys (a round where every
	 * request has its own key); adds their ports to seenPorts.
	 */
	public Overlap overlap(Set<String> idempotencyKeys, Set<Integer> seenPorts) {
		List<Arrival> round = this.arrivals.stream()
			.filter((a) -> a.idempotencyKey() != null && idempotencyKeys.contains(a.idempotencyKey()))
			.toList();
		long firstCompletion = round.stream().mapToLong(Arrival::completedNanos).min().orElse(Long.MIN_VALUE);
		int before = (int) round.stream().filter((a) -> a.arrivedNanos() < firstCompletion).count();
		long first = round.stream().mapToLong(Arrival::arrivedNanos).min().orElse(0);
		long last = round.stream().mapToLong(Arrival::arrivedNanos).max().orElse(0);
		Set<Integer> ports = round.stream().map(Arrival::remotePort).collect(Collectors.toSet());
		int reused = (int) ports.stream().filter(seenPorts::contains).count();
		seenPorts.addAll(ports);
		return new Overlap(round.size(), before, (last - first) / 1_000_000, ports.size(), reused);
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
