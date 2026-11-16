package dev.aryanyeole.ledgerapi.cache;

import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

// The Redis gateway exists only while ledger.cache.idempotency.enabled or ledger.cache.balance.enabled
// is true (D-137): both true in main config, false in src/test/resources/application.properties
// (D-096), and forced false in migrate-only mode (D-083). With both off there is no Redis bean of
// any kind and ledger.redis.url isn't bound. Creating the gateway never contacts Redis: the
// connection is made on first use, so startup never waits for it.
@Configuration(proxyBeanMethods = false)
@Conditional(RedisConfiguration.AnyCacheSwitch.class)
@EnableConfigurationProperties(LedgerRedisProperties.class)
class RedisConfiguration {

	@Bean(destroyMethod = "close")
	RedisGateway redisGateway(LedgerRedisProperties properties) {
		return new RedisGateway(properties);
	}

	static class AnyCacheSwitch extends AnyNestedCondition {

		AnyCacheSwitch() {
			// Parse phase, as in SchedulingConfiguration: with both switches off the class is skipped
			// whole, @EnableConfigurationProperties included, so ledger.redis.url isn't validated.
			super(ConfigurationPhase.PARSE_CONFIGURATION);
		}

		@ConditionalOnBooleanProperty("ledger.cache.idempotency.enabled")
		static class IdempotencyCache {

		}

		@ConditionalOnBooleanProperty("ledger.cache.balance.enabled")
		static class BalanceCache {

		}

	}

}
