package dev.aryanyeole.ledgerapi.scheduling;

import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

// The application's only @EnableScheduling (D-127). Active when any scheduler is switched on:
// the outbox relay's (ledger.outbox.relay.scheduled) or the idempotency key purge's
// (ledger.idempotency.purge.scheduled). On either scheduler's own configuration it would leave
// the other's @Scheduled method unregistered whenever that configuration is off, silently; made
// unconditional it would start scheduling infrastructure in every test and migrate-only context.
// The scheduler beans themselves are still created only by their own configurations.
// spring.task.scheduling.pool.size is 2 in application.yml, so a long purge run can't hold up
// the relay's ticks.
@Configuration(proxyBeanMethods = false)
@Conditional(SchedulingConfiguration.AnyScheduledSwitch.class)
@EnableScheduling
public class SchedulingConfiguration {

	static class AnyScheduledSwitch extends AnyNestedCondition {

		AnyScheduledSwitch() {
			super(ConfigurationPhase.PARSE_CONFIGURATION);
		}

		@ConditionalOnBooleanProperty("ledger.outbox.relay.scheduled")
		static class OutboxRelayScheduled {

		}

		@ConditionalOnBooleanProperty("ledger.idempotency.purge.scheduled")
		static class IdempotencyPurgeScheduled {

		}

	}

}
