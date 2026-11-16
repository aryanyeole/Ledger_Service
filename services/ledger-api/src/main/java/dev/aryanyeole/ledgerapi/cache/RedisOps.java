package dev.aryanyeole.ledgerapi.cache;

import java.util.List;

// The only Redis commands ledger-api sends (docs/design/cache-invalidation.md §2.2). There is no
// MULTI here on purpose: on the shared connection, commands other threads send between MULTI and
// EXEC would join the transaction (D-137), so nothing can open one. Every command is bounded by the
// gateway's command timeout and may throw; RedisGateway catches.
public interface RedisOps {

	/** The value, or null if the key is absent. */
	String get(String key);

	/** SET key value NX PX ttlMillis: true if this call wrote the key, false if it already existed. */
	boolean setNxPx(String key, String value, long ttlMillis);

	/** MGET: one entry per key, in order, null for an absent key. */
	List<String> mget(String... keys);

	/** PTTL: remaining milliseconds, -1 without an expiry, -2 if the key is absent. */
	long pttl(String key);

	/** EVAL of a script that returns an integer. */
	long eval(String script, List<String> keys, List<String> args);

}
