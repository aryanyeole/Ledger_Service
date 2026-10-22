package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.config.ScheduledTaskHolder;

import dev.aryanyeole.ledgerapi.idempotency.IdempotencyKeyPurgeProperties;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyKeyPurgeScheduler;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyKeyPurger;

// enabled=true, scheduled=false (D-126): the purger exists for callers to drive directly, and
// nothing schedules it. The relay is off too, so there is no scheduling infrastructure at all.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = "ledger.idempotency.purge.enabled=true")
class IdempotencyKeyPurgeUnscheduledTest {

	@Autowired
	ApplicationContext context;

	@Test
	void purgerOnlyWithDefaults() {
		assertThat(this.context.getBeanNamesForType(IdempotencyKeyPurger.class)).hasSize(1);
		assertThat(this.context.getBeanNamesForType(IdempotencyKeyPurgeScheduler.class)).isEmpty();
		assertThat(this.context.getBeanNamesForType(ScheduledTaskHolder.class)).isEmpty();
		IdempotencyKeyPurgeProperties properties = this.context.getBean(IdempotencyKeyPurgeProperties.class);
		assertThat(properties.batchSize()).isEqualTo(1000);
		assertThat(properties.interval()).isEqualTo(Duration.ofMinutes(5));
	}

}
