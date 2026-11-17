package dev.aryanyeole.ledgerapi.accounts;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

// ledger.cache.balance.* (D-137, D-142, D-151). enabled is read by BalanceCacheConfiguration's
// condition; ttl is the PX of a cached balance value; invalidation=false exists only for the
// benchmark's TTL-only mode (D-146) and logs a startup WARNING. Bound and validated whether or not
// the cache is on (D-127's pattern), so a bad value fails startup on every instance.
@ConfigurationProperties("ledger.cache.balance")
public record LedgerBalanceCacheProperties(@DefaultValue("false") boolean enabled, @DefaultValue("60s") Duration ttl,
		@DefaultValue("true") boolean invalidation) {

	public LedgerBalanceCacheProperties {
		if (ttl == null || ttl.isNegative() || ttl.isZero()) {
			throw new IllegalArgumentException("ledger.cache.balance.ttl must be positive");
		}
	}

}
