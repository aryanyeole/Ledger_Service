package dev.aryanyeole.ledgerapi.cache;

// The balance cache's two Lua scripts (D-142), exactly as docs/design/cache-invalidation.md §2.1
// prints them between its script markers. BalanceCacheScriptsTest pins both against literals and
// against the doc (D-143); the worker's Python copy of the invalidation script is pinned against the
// same markers (T8). Changing a script changes all three in one PR.
public final class BalanceCacheScripts {

	/** The token PX, a literal inside INVALIDATE: a contract constant, not configuration (D-142). */
	public static final long TOKEN_PX_MILLIS = 300_000;

	/** KEYS: a value-key/token-key pair per account touched by the posting. ARGV[1]: a fresh token. */
	public static final String INVALIDATE = """
			-- ledger:v1 balance invalidation (D-142). KEYS: ledger:v1:bal:{id}, ledger:v1:bal-gen:{id} per account.
			-- ARGV[1]: a fresh token for this call.
			for i = 1, #KEYS, 2 do
			  redis.call('DEL', KEYS[i])
			  redis.call('SET', KEYS[i + 1], ARGV[1], 'PX', 300000)
			end
			return #KEYS
			""";

	/** KEYS: value key, token key. ARGV: token the MGET saw ('' if absent), value, value TTL in ms. */
	public static final String POPULATE = """
			-- ledger:v1 balance populate (D-142). KEYS[1]: ledger:v1:bal:{id}; KEYS[2]: ledger:v1:bal-gen:{id}.
			-- ARGV[1]: token seen by the MGET ('' if absent); ARGV[2]: the value; ARGV[3]: the value TTL in ms.
			local token = redis.call('GET', KEYS[2])
			if token == false then token = '' end
			if token ~= ARGV[1] then return 0 end
			redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
			return 1
			""";

	private BalanceCacheScripts() {
	}

	/** ledger:v1:bal:{id} */
	public static String valueKey(long accountId) {
		return "ledger:v1:bal:" + accountId;
	}

	/** ledger:v1:bal-gen:{id} */
	public static String tokenKey(long accountId) {
		return "ledger:v1:bal-gen:" + accountId;
	}

}
