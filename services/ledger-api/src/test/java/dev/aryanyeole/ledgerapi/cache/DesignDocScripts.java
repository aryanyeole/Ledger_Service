package dev.aryanyeole.ledgerapi.cache;

// The two Lua scripts from docs/design/cache-invalidation.md §2.1, copied verbatim for the smoke
// tests. T6 moves them into main code, where the D-143 literal-string tests pin them.
final class DesignDocScripts {

	static final String INVALIDATE = """
			-- ledger:v1 balance invalidation (D-142). KEYS: ledger:v1:bal:{id}, ledger:v1:bal-gen:{id} per account.
			-- ARGV[1]: a fresh token for this call.
			for i = 1, #KEYS, 2 do
			  redis.call('DEL', KEYS[i])
			  redis.call('SET', KEYS[i + 1], ARGV[1], 'PX', 300000)
			end
			return #KEYS
			""";

	static final String POPULATE = """
			-- ledger:v1 balance populate (D-142). KEYS[1]: ledger:v1:bal:{id}; KEYS[2]: ledger:v1:bal-gen:{id}.
			-- ARGV[1]: token seen by the MGET ('' if absent); ARGV[2]: the value; ARGV[3]: the value TTL in ms.
			local token = redis.call('GET', KEYS[2])
			if token == false then token = '' end
			if token ~= ARGV[1] then return 0 end
			redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
			return 1
			""";

	private DesignDocScripts() {
	}

}
