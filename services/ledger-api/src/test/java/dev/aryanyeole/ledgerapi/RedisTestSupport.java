package dev.aryanyeole.ledgerapi;

import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.awaitility.core.ConditionTimeoutException;

import dev.aryanyeole.ledgerapi.cache.RedisGateway;

// Test support for contexts that turn a Redis cache on. The gateway connects lazily, its first
// connect has the production 100 ms handshake budget (D-137) and may legitimately fail under load,
// which in production is just a miss, and a failure opens the 1 s bypass. A test that asserts on
// gateway results must warm the gateway first, or one slow first connect fails several tests at once
// (BUG-016).
public final class RedisTestSupport {

	static final Duration GATEWAY_READY = Duration.ofSeconds(10);

	private RedisTestSupport() {
	}

	/** Polls a read until the gateway answers, riding out bypass windows; fails after GATEWAY_READY. */
	public static void awaitGatewayReady(RedisGateway gateway) {
		try {
			await().atMost(GATEWAY_READY)
				.pollDelay(Duration.ZERO)
				.pollInterval(Duration.ofMillis(100))
				.until(() -> gateway.optional("GET", (r) -> r.get("ledger:test:warmup")).available());
		}
		catch (ConditionTimeoutException ex) {
			throw new AssertionError("gateway never connected within " + GATEWAY_READY.toSeconds() + " s", ex);
		}
	}

}
