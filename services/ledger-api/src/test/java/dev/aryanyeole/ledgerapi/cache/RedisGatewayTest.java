package dev.aryanyeole.ledgerapi.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import dev.aryanyeole.ledgerapi.cache.RedisGateway.Reply;

// The gateway's failure handling (D-137) with a fake connector and clock: no Redis, no timing.
// After any failure optional() bypasses for exactly BYPASS; always() still reaches an established
// connection; a success never shortens an open window; nothing escapes; with no connection, at most
// one connect attempt starts per window and every other caller returns unavailable.
class RedisGatewayTest {

	private static final long BYPASS = RedisGateway.BYPASS.toNanos();

	private final AtomicLong clock = new AtomicLong(1_000_000_000L);

	private final FakeOps ops = new FakeOps();

	private final AtomicInteger connects = new AtomicInteger();

	private RedisGateway gateway(Supplier<RedisGateway.Session> connector) {
		return new RedisGateway(connector, () -> {
		}, this.clock::get);
	}

	private RedisGateway connectedGateway() {
		return gateway(() -> {
			this.connects.incrementAndGet();
			return session(this.ops);
		});
	}

	private static RedisGateway.Session session(RedisOps ops) {
		return new RedisGateway.Session() {

			@Override
			public RedisOps ops() {
				return ops;
			}

			@Override
			public void close() {
			}

		};
	}

	private void advance(Duration duration) {
		this.clock.addAndGet(duration.toNanos());
	}

	@Test
	void aSuccessfulCallReturnsTheValueAndAnAbsentKeyIsAvailableNull() {
		RedisGateway gateway = connectedGateway();
		this.ops.values.put("k", "v");

		assertThat(gateway.optional("GET", (r) -> r.get("k"))).isEqualTo(Reply.of("v"));
		assertThat(gateway.optional("GET", (r) -> r.get("missing"))).isEqualTo(Reply.of(null));
		assertThat(this.connects).hasValue(1);
	}

	@Test
	void afterAFailureOptionalBypassesForExactlyTheWindowAndAlwaysStillCallsThrough() {
		RedisGateway gateway = connectedGateway();
		this.ops.values.put("k", "v");
		this.ops.failing = true;

		assertThat(gateway.optional("GET", (r) -> r.get("k"))).isEqualTo(Reply.unavailable());
		this.ops.failing = false;
		int callsAfterFailure = this.ops.calls;

		advance(RedisGateway.BYPASS.minusNanos(1));
		assertThat(gateway.optional("GET", (r) -> r.get("k"))).as("one nanosecond before the window ends")
			.isEqualTo(Reply.unavailable());
		assertThat(this.ops.calls).as("a bypassed call never reaches Redis").isEqualTo(callsAfterFailure);

		assertThat(gateway.always("EVAL", (r) -> r.get("k"))).as("always() during the window")
			.isEqualTo(Reply.of("v"));
		assertThat(this.ops.calls).isEqualTo(callsAfterFailure + 1);

		advance(Duration.ofNanos(1));
		assertThat(gateway.optional("GET", (r) -> r.get("k"))).as("when the window ends").isEqualTo(Reply.of("v"));
	}

	@Test
	void aSuccessDoesNotShortenAnOpenWindowAndALaterFailureExtendsIt() {
		RedisGateway gateway = connectedGateway();
		this.ops.failing = true;
		gateway.optional("GET", (r) -> r.get("k"));
		this.ops.failing = false;

		advance(Duration.ofMillis(500));
		assertThat(gateway.always("EVAL", (r) -> r.get("k")).available()).isTrue();
		advance(Duration.ofMillis(499));
		assertThat(gateway.optional("GET", (r) -> r.get("k")).available()).as("still inside the first window")
			.isFalse();

		this.ops.failing = true;
		assertThat(gateway.always("EVAL", (r) -> r.get("k")).available()).isFalse();
		this.ops.failing = false;
		advance(Duration.ofMillis(2));
		assertThat(gateway.optional("GET", (r) -> r.get("k")).available())
			.as("the failure at 999 ms opened a new window to 1999 ms")
			.isFalse();
		advance(RedisGateway.BYPASS);
		assertThat(gateway.optional("GET", (r) -> r.get("k")).available()).isTrue();
	}

	@Test
	void nothingEscapes() {
		RedisGateway gateway = connectedGateway();
		assertThat(gateway.always("GET", (r) -> {
			throw new IllegalStateException("boom");
		})).isEqualTo(Reply.unavailable());
		assertThat(gateway.always("GET", (r) -> {
			throw new RuntimeException(new Error("wrapped"));
		})).isEqualTo(Reply.unavailable());
	}

	@Test
	void withNoConnectionAFailedConnectOpensTheWindowAndOnlyOneAttemptStartsPerWindow() {
		AtomicInteger attempts = new AtomicInteger();
		RedisGateway gateway = gateway(() -> {
			attempts.incrementAndGet();
			throw new IllegalStateException("connection refused");
		});

		assertThat(gateway.always("EVAL", (r) -> r.get("k"))).isEqualTo(Reply.unavailable());
		assertThat(attempts).hasValue(1);

		advance(Duration.ofMillis(999));
		assertThat(gateway.always("EVAL", (r) -> r.get("k"))).as("an invalidation with no connection, in the window")
			.isEqualTo(Reply.unavailable());
		assertThat(gateway.optional("GET", (r) -> r.get("k"))).isEqualTo(Reply.unavailable());
		assertThat(attempts).as("no second attempt inside the window").hasValue(1);
		assertThat(gateway.connectAttempts()).isEqualTo(1);

		advance(Duration.ofMillis(1));
		gateway.always("EVAL", (r) -> r.get("k"));
		assertThat(attempts).as("one attempt in the next window").hasValue(2);
	}

	@Test
	void aConnectThatSucceedsAfterTheWindowIsUsedFromThenOn() {
		AtomicInteger attempts = new AtomicInteger();
		RedisGateway gateway = gateway(() -> {
			if (attempts.incrementAndGet() == 1) {
				throw new IllegalStateException("connection refused");
			}
			return session(this.ops);
		});
		this.ops.values.put("k", "v");

		assertThat(gateway.optional("GET", (r) -> r.get("k")).available()).isFalse();
		advance(RedisGateway.BYPASS);
		assertThat(gateway.optional("GET", (r) -> r.get("k"))).isEqualTo(Reply.of("v"));
		assertThat(gateway.optional("GET", (r) -> r.get("k"))).isEqualTo(Reply.of("v"));
		assertThat(attempts).hasValue(2);
	}

	@Test
	void whileAnAttemptIsInFlightOtherCallersReturnAtOnce() {
		AtomicInteger attempts = new AtomicInteger();
		RedisGateway[] holder = new RedisGateway[1];
		List<Reply<String>> nested = new ArrayList<>();
		holder[0] = gateway(() -> {
			attempts.incrementAndGet();
			// Re-entrant callers stand in for concurrent ones: the attempt is still in flight.
			nested.add(holder[0].always("EVAL", (r) -> r.get("k")));
			nested.add(holder[0].optional("GET", (r) -> r.get("k")));
			return session(this.ops);
		});

		assertThat(holder[0].always("EVAL", (r) -> "ok")).isEqualTo(Reply.of("ok"));
		assertThat(nested).containsExactly(Reply.unavailable(), Reply.unavailable());
		assertThat(attempts).hasValue(1);
	}

	// D-149: no Redis call while a Spring transaction is active, checked before any Redis work and
	// outside the failure-to-miss handling; afterCommit still counts as inside (Spring clears the flag
	// only in cleanup after completion).
	@Test
	void optionalAndAlwaysRefuseWhileATransactionIsActiveAndMakeNoRedisCall() {
		RedisGateway gateway = connectedGateway();
		TransactionSynchronizationManager.setActualTransactionActive(true);
		try {
			assertThatIllegalStateException().isThrownBy(() -> gateway.optional("GET", (r) -> r.get("k")))
				.withMessage("Redis call inside a DB transaction (invariant 5, D-149)");
			assertThatIllegalStateException().isThrownBy(() -> gateway.always("EVAL", (r) -> r.get("k")))
				.withMessage("Redis call inside a DB transaction (invariant 5, D-149)");
		}
		finally {
			TransactionSynchronizationManager.clear();
		}
		assertThat(this.connects).as("no connect attempt").hasValue(0);
		assertThat(this.ops.calls).as("no Redis call").isZero();
		assertThat(gateway.optional("GET", (r) -> r.get("k")).available()).as("outside a transaction").isTrue();
	}

	@Test
	void aRealTransactionAndItsAfterCommitCallbackAreRefusedAndTheCallAfterExecuteRuns() {
		RedisGateway gateway = connectedGateway();
		TransactionTemplate template = new TransactionTemplate(new StubTransactionManager());
		List<Throwable> refused = new ArrayList<>();
		template.executeWithoutResult((status) -> {
			refused.add(catchThrowable(() -> gateway.optional("GET", (r) -> r.get("k"))));
			TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {

				@Override
				public void afterCommit() {
					refused.add(catchThrowable(() -> gateway.always("EVAL", (r) -> r.get("k"))));
				}

			});
		});
		assertThat(refused).hasSize(2).allSatisfy((thrown) -> assertThat(thrown)
			.isInstanceOf(IllegalStateException.class)
			.hasMessage("Redis call inside a DB transaction (invariant 5, D-149)"));
		assertThat(this.ops.calls).as("no Redis call inside the transaction or its afterCommit").isZero();
		assertThat(gateway.always("EVAL", (r) -> r.get("k")).available()).as("after execute has returned").isTrue();
	}

	// A transaction manager with no resource: enough for TransactionTemplate to run Spring's real
	// begin/commit/synchronization sequence in a unit test.
	private static final class StubTransactionManager extends AbstractPlatformTransactionManager {

		@Override
		protected Object doGetTransaction() {
			return new Object();
		}

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) {
		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) {
		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) {
		}

	}

	private static final class FakeOps implements RedisOps {

		final Map<String, String> values = new HashMap<>();

		volatile boolean failing;

		int calls;

		private void call() {
			this.calls++;
			if (this.failing) {
				throw new IllegalStateException("Command timed out");
			}
		}

		@Override
		public String get(String key) {
			call();
			return this.values.get(key);
		}

		@Override
		public boolean setNxPx(String key, String value, long ttlMillis) {
			call();
			return this.values.putIfAbsent(key, value) == null;
		}

		@Override
		public List<String> mget(String... keys) {
			call();
			return Arrays.stream(keys).map(this.values::get).toList();
		}

		@Override
		public long pttl(String key) {
			call();
			return this.values.containsKey(key) ? -1 : -2;
		}

		@Override
		public long eval(String script, List<String> keys, List<String> args) {
			call();
			return 0;
		}

	}

}
