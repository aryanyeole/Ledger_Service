package dev.aryanyeole.ledgerapi.idempotency;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.MeterRegistry;

import dev.aryanyeole.ledgerapi.cache.RedisGateway;

// The idempotency fast path (D-139) and the owner's fault hook (D-141). The cache exists only when
// ledger.cache.idempotency.enabled is true (the Redis gateway always exists then); without it,
// IdempotencyService makes no Redis call at all. The properties are bound here, unconditionally, so
// an invalid ttl or margin fails startup even where the cache is off (D-127).
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(LedgerIdempotencyCacheProperties.class)
class IdempotencyCacheConfiguration {

	// A test configuration may replace this with a @Primary hook; no property can arm it.
	@Bean
	@ConditionalOnMissingBean
	IdempotencyFaultHook idempotencyFaultHook() {
		return (clientId, key) -> {
		};
	}

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnBooleanProperty("ledger.cache.idempotency.enabled")
	static class Caching {

		@Bean
		IdempotencyCache idempotencyCache(RedisGateway redisGateway, LedgerIdempotencyCacheProperties properties,
				MeterRegistry meterRegistry) {
			return new IdempotencyCache(redisGateway, properties.ttl(), meterRegistry);
		}

	}

}
