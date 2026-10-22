package dev.aryanyeole.ledgerapi.idempotency;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;

import dev.aryanyeole.ledgerapi.db.WriteTransactions;

// The idempotency key purge (D-126, D-127), with the relay's two switches (D-079): the purger
// exists only when ledger.idempotency.purge.enabled is true, its scheduler only when scheduled is
// true as well. The scheduler is nested inside the purger's configuration, so scheduled=true on
// its own creates nothing. The properties are bound here, unconditionally, so an invalid value
// fails startup even where the purge is off. @EnableScheduling is SchedulingConfiguration's.
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(IdempotencyKeyPurgeProperties.class)
class IdempotencyKeyPurgeConfiguration {

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnBooleanProperty("ledger.idempotency.purge.enabled")
	static class Purging {

		@Bean
		IdempotencyKeyPurger idempotencyKeyPurger(WriteTransactions transactions, JdbcClient jdbc,
				IdempotencyKeyPurgeProperties properties) {
			return new IdempotencyKeyPurger(transactions, jdbc, properties);
		}

		@Configuration(proxyBeanMethods = false)
		@ConditionalOnBooleanProperty("ledger.idempotency.purge.scheduled")
		static class Scheduling {

			@Bean
			IdempotencyKeyPurgeScheduler idempotencyKeyPurgeScheduler(IdempotencyKeyPurger idempotencyKeyPurger) {
				return new IdempotencyKeyPurgeScheduler(idempotencyKeyPurger);
			}

		}

	}

}
