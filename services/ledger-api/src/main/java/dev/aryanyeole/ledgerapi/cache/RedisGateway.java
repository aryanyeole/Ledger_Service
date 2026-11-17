package dev.aryanyeole.ledgerapi.cache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

// The only class in ledger-api that touches Lettuce (D-135, D-137, docs/design/cache-invalidation.md).
//  - One shared connection, no pool, no MULTI (RedisOps has none), so commands execute in the order
//    they were sent, after a pause too.
//  - Every command is bounded by COMMAND_TIMEOUT and a connect by CONNECT_TIMEOUT; while
//    disconnected Lettuce rejects commands instead of queueing them.
//  - The connection is made on first use, never at startup. With none established, at most one
//    connect attempt is in flight (a flag, never a lock anyone waits on) and at most one starts per
//    bypass window; every other caller, invalidations included, gets Reply.unavailable() at once.
//    Lettuce's auto-reconnect covers only a connection that was once established, so a failed
//    first connect is retried here, by the next caller after the window.
//  - After any failure, optional() returns unavailable for BYPASS without touching Redis: reads,
//    populates and the lease. always() never bypasses on an established connection: invalidations.
//  - Nothing escapes: every exception becomes Reply.unavailable(), logged as a WARN once per entry
//    into the bypass, with the command name and exception type only, never a key or value.
//  - Never inside a DB transaction (invariant 5, D-149): optional() and always() throw
//    IllegalStateException while a Spring transaction is active, before any Redis call and outside
//    the failure-to-miss handling, so a misplaced call fails loudly. Spring keeps the flag set until
//    cleanup after completion, so this also rejects TransactionSynchronization.afterCommit (D-147).
public class RedisGateway implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(RedisGateway.class);

	public static final Duration COMMAND_TIMEOUT = Duration.ofMillis(100);

	public static final Duration CONNECT_TIMEOUT = Duration.ofMillis(200);

	public static final Duration BYPASS = Duration.ofSeconds(1);

	/** A command's outcome: available with a value (null for an absent key), or unavailable. */
	public record Reply<T>(boolean available, T value) {

		public static <T> Reply<T> of(T value) {
			return new Reply<>(true, value);
		}

		public static <T> Reply<T> unavailable() {
			return new Reply<>(false, null);
		}

	}

	/** An established connection, as RedisOps plus a way to close it. */
	interface Session extends AutoCloseable {

		RedisOps ops();

		@Override
		void close();

	}

	private final Supplier<Session> connector;

	private final Runnable shutdown;

	private final LongSupplier nanoClock;

	private final AtomicBoolean connecting = new AtomicBoolean();

	private final AtomicLong connectAttempts = new AtomicLong();

	// nanoClock value before which optional() bypasses and no connect attempt starts. Starts in the
	// past, so the first call may connect.
	private final AtomicLong bypassUntil;

	private volatile Session session;

	public RedisGateway(LedgerRedisProperties properties) {
		this(RedisClient.create(redisUri(properties.host(), properties.port())), System::nanoTime);
	}

	private RedisGateway(RedisClient client, LongSupplier nanoClock) {
		this(lettuceConnector(configured(client)), () -> client.shutdown(0, 2, TimeUnit.SECONDS), nanoClock);
	}

	// Package-private for RedisGatewayTest: a fake connector and clock.
	RedisGateway(Supplier<Session> connector, Runnable shutdown, LongSupplier nanoClock) {
		this.connector = connector;
		this.shutdown = shutdown;
		this.nanoClock = nanoClock;
		this.bypassUntil = new AtomicLong(nanoClock.getAsLong() - 1);
	}

	/** The client options every ledger-api Redis connection uses; tests build raw clients with them. */
	public static ClientOptions clientOptions() {
		return ClientOptions.builder()
			.socketOptions(SocketOptions.builder().connectTimeout(CONNECT_TIMEOUT).build())
			.timeoutOptions(TimeoutOptions.enabled(COMMAND_TIMEOUT))
			.disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
			.autoReconnect(true)
			.build();
	}

	/** Database 0 at host:port, with the command timeout. */
	public static RedisURI redisUri(String host, int port) {
		return RedisURI.builder().withHost(host).withPort(port).withDatabase(0).withTimeout(COMMAND_TIMEOUT).build();
	}

	private static RedisClient configured(RedisClient client) {
		client.setOptions(clientOptions());
		return client;
	}

	private static Supplier<Session> lettuceConnector(RedisClient client) {
		return () -> new LettuceSession(client.connect());
	}

	/** Reads, populates and the lease: unavailable while the bypass is active. */
	public <T> Reply<T> optional(String command, Function<RedisOps, T> call) {
		refuseInsideTransaction();
		if (bypassing(this.nanoClock.getAsLong())) {
			return Reply.unavailable();
		}
		return run(command, call);
	}

	/** Invalidations: never bypassed on an established connection. */
	public <T> Reply<T> always(String command, Function<RedisOps, T> call) {
		refuseInsideTransaction();
		return run(command, call);
	}

	private static void refuseInsideTransaction() {
		if (TransactionSynchronizationManager.isActualTransactionActive()) {
			throw new IllegalStateException("Redis call inside a DB transaction (invariant 5, D-149)");
		}
	}

	private <T> Reply<T> run(String command, Function<RedisOps, T> call) {
		Session current = this.session;
		if (current == null) {
			current = tryConnect();
			if (current == null) {
				return Reply.unavailable();
			}
		}
		try {
			return Reply.of(call.apply(current.ops()));
		}
		catch (RuntimeException ex) {
			failed(command, ex);
			return Reply.unavailable();
		}
	}

	// At most one attempt in flight; at most one per bypass window. Callers that don't get to try
	// return null at once and never wait.
	private Session tryConnect() {
		if (bypassing(this.nanoClock.getAsLong()) || !this.connecting.compareAndSet(false, true)) {
			return null;
		}
		try {
			Session existing = this.session;
			if (existing != null) {
				return existing;
			}
			if (bypassing(this.nanoClock.getAsLong())) {
				return null;
			}
			this.connectAttempts.incrementAndGet();
			try {
				Session connected = this.connector.get();
				this.session = connected;
				return connected;
			}
			catch (RuntimeException ex) {
				failed("CONNECT", ex);
				return null;
			}
		}
		finally {
			this.connecting.set(false);
		}
	}

	private boolean bypassing(long now) {
		return now - this.bypassUntil.get() < 0;
	}

	private void failed(String command, RuntimeException ex) {
		long now = this.nanoClock.getAsLong();
		long until = now + BYPASS.toNanos();
		long previous = this.bypassUntil.getAndAccumulate(until, (current, next) -> (next - current > 0) ? next : current);
		if (now - previous >= 0) {
			log.warn("Redis {} failed ({}); reads, populates and the lease bypass Redis for {} ms", command,
					ex.getClass().getSimpleName(), BYPASS.toMillis());
		}
	}

	/** Connect attempts started so far (RedisDownStartupTest counts them per window). */
	long connectAttempts() {
		return this.connectAttempts.get();
	}

	@Override
	public void close() {
		Session current = this.session;
		this.session = null;
		try {
			if (current != null) {
				current.close();
			}
		}
		finally {
			this.shutdown.run();
		}
	}

	private static final class LettuceSession implements Session {

		private final StatefulRedisConnection<String, String> connection;

		private final RedisOps ops;

		LettuceSession(StatefulRedisConnection<String, String> connection) {
			this.connection = connection;
			this.ops = new LettuceOps(connection.sync());
		}

		@Override
		public RedisOps ops() {
			return this.ops;
		}

		@Override
		public void close() {
			this.connection.close();
		}

	}

	private record LettuceOps(RedisCommands<String, String> commands) implements RedisOps {

		@Override
		public String get(String key) {
			return this.commands.get(key);
		}

		@Override
		public boolean setNxPx(String key, String value, long ttlMillis) {
			return "OK".equals(this.commands.set(key, value, SetArgs.Builder.nx().px(ttlMillis)));
		}

		@Override
		public List<String> mget(String... keys) {
			List<String> values = new ArrayList<>(keys.length);
			for (KeyValue<String, String> entry : this.commands.mget(keys)) {
				values.add(entry.hasValue() ? entry.getValue() : null);
			}
			return values;
		}

		@Override
		public long pttl(String key) {
			return this.commands.pttl(key);
		}

		@Override
		public long eval(String script, List<String> keys, List<String> args) {
			Long reply = this.commands.eval(script, ScriptOutputType.INTEGER, keys.toArray(String[]::new),
					args.toArray(String[]::new));
			return reply;
		}

	}

}
