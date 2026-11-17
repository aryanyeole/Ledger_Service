package dev.aryanyeole.ledgerapi.cache;

import java.util.function.LongSupplier;

// A RedisGateway over a fake RedisOps for unit tests outside this package (the constructor that
// takes a connector and a clock is package-private). The session never fails to connect.
public final class TestRedisGateways {

	private TestRedisGateways() {
	}

	public static RedisGateway over(RedisOps ops, LongSupplier nanoClock) {
		RedisGateway.Session session = new RedisGateway.Session() {

			@Override
			public RedisOps ops() {
				return ops;
			}

			@Override
			public void close() {
			}

		};
		return new RedisGateway(() -> session, () -> {
		}, nanoClock);
	}

}
