package dev.aryanyeole.ledgerapi.idempotency;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

// ledger.cache.idempotency.* (D-137, D-139). enabled is read by IdempotencyCacheConfiguration's
// condition; ttl is the PX of a fast-path entry; an entry is written only while the row's remaining
// life, measured by the DB, exceeds ttl + margin. Bound and validated whether or not the cache is on
// (D-127's pattern), so a bad value fails startup on every instance.
@ConfigurationProperties("ledger.cache.idempotency")
public record LedgerIdempotencyCacheProperties(@DefaultValue("false") boolean enabled,
		@DefaultValue("10m") Duration ttl, @DefaultValue("1h") Duration margin) {

	public LedgerIdempotencyCacheProperties {
		requirePositive("ttl", ttl);
		requirePositive("margin", margin);
	}

	/** ttl + margin in milliseconds: the remaining life a row needs before its response is cached. */
	public long minLifeMillis() {
		return this.ttl.plus(this.margin).toMillis();
	}

	private static void requirePositive(String name, Duration value) {
		if (value == null || value.isNegative() || value.isZero()) {
			throw new IllegalArgumentException("ledger.cache.idempotency." + name + " must be positive");
		}
	}

}
