package dev.aryanyeole.ledgerapi.posting;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import dev.aryanyeole.ledgerapi.cache.BalanceCacheScripts;
import dev.aryanyeole.ledgerapi.cache.RedisGateway;
import dev.aryanyeole.ledgerapi.cache.RedisGateway.Reply;

// The balance cache's write side (D-142, D-151, docs/design/cache-invalidation.md §2.1, §3.6). One
// EVAL of the invalidation script per committed posting: for every account the posting touched, DEL
// the value and SET the token to a fresh UUID with PX 300000. It goes through the gateway's always(),
// so the bypass never skips it on an established connection (§1.5). PostingService registers it as a
// WriteTransactions post-commit action (D-147), so it runs after the commit, with the connection back
// in the pool. It has no read method: a write path can never read a cached balance. It exists only
// while the balance cache is on and ledger.cache.balance.invalidation is true. A failure is logged
// without ids and counted; the stale value lives at most the value TTL (§5 row 5). Never throws.
public class BalanceCacheInvalidator {

	private static final Logger log = LoggerFactory.getLogger(BalanceCacheInvalidator.class);

	enum Result {

		SENT, FAILED

	}

	private final RedisGateway gateway;

	private final Map<Result, Counter> results = new EnumMap<>(Result.class);

	public BalanceCacheInvalidator(RedisGateway gateway, MeterRegistry meterRegistry) {
		this.gateway = gateway;
		for (Result result : Result.values()) {
			this.results.put(result, Counter.builder("ledger.balance.cache.invalidation")
				.tag("result", result.name().toLowerCase())
				.register(meterRegistry));
		}
	}

	/** KEYS: value key, token key per distinct account, in ascending id order. */
	public static List<String> keys(Collection<Long> accountIds) {
		List<String> keys = new ArrayList<>();
		for (long id : new TreeSet<>(accountIds)) {
			keys.add(BalanceCacheScripts.valueKey(id));
			keys.add(BalanceCacheScripts.tokenKey(id));
		}
		return keys;
	}

	public void invalidate(Collection<Long> accountIds) {
		List<String> keys = keys(accountIds);
		String token = UUID.randomUUID().toString();
		Reply<Long> reply = this.gateway.always("EVAL",
				(redis) -> redis.eval(BalanceCacheScripts.INVALIDATE, keys, List.of(token)));
		if (reply.available()) {
			this.results.get(Result.SENT).increment();
		}
		else {
			this.results.get(Result.FAILED).increment();
			log.warn("Balance cache invalidation failed; affected balances may be stale for up to the value TTL");
		}
	}

}
