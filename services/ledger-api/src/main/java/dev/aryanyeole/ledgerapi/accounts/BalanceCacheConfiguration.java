package dev.aryanyeole.ledgerapi.accounts;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.MeterRegistry;

import dev.aryanyeole.ledgerapi.cache.RedisGateway;
import dev.aryanyeole.ledgerapi.posting.BalanceCacheInvalidator;

// The balance cache (D-142, D-151). Both beans exist only when ledger.cache.balance.enabled is true
// (the Redis gateway always exists then): BalanceCache, the read side, injected only into
// BalanceController; BalanceCacheInvalidator, the write side, injected only into PostingService and
// present only while ledger.cache.balance.invalidation is true. invalidation=false is the benchmark's
// TTL-only mode (D-146): it logs a startup WARNING. While the cache is on, startup fails unless the
// token PX outlives a reader's MGET -> compare-and-set window (TokenBound). The properties are bound
// here, unconditionally, so an invalid ttl fails startup even where the cache is off (D-127).
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(LedgerBalanceCacheProperties.class)
class BalanceCacheConfiguration {

	private static final Logger log = LoggerFactory.getLogger(BalanceCacheConfiguration.class);

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnBooleanProperty("ledger.cache.balance.enabled")
	static class Caching {

		@Bean
		BalanceCache balanceCache(RedisGateway redisGateway, LedgerBalanceCacheProperties properties,
				DataSource dataSource, MeterRegistry meterRegistry) {
			TokenBound.check(dataSource);
			if (!properties.invalidation()) {
				log.warn("Balance cache invalidation is OFF (ledger.cache.balance.invalidation=false): benchmark "
						+ "TTL-only mode; a balance may be stale for up to ttl={} after every posting", properties.ttl());
			}
			return new BalanceCache(redisGateway, properties, meterRegistry);
		}

		@Bean
		@ConditionalOnBooleanProperty(name = "ledger.cache.balance.invalidation", matchIfMissing = true)
		BalanceCacheInvalidator balanceCacheInvalidator(RedisGateway redisGateway, MeterRegistry meterRegistry) {
			return new BalanceCacheInvalidator(redisGateway, meterRegistry);
		}

	}

}
