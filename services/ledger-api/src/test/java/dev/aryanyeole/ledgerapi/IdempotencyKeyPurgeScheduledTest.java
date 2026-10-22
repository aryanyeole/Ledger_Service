package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.FixedDelayTask;
import org.springframework.scheduling.config.ScheduledTask;
import org.springframework.scheduling.config.ScheduledTaskHolder;
import org.springframework.scheduling.config.Task;

import dev.aryanyeole.ledgerapi.idempotency.IdempotencyKeyPurgeScheduler;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyKeyPurger;

// enabled=true, scheduled=true with the relay off (D-126, D-127): both beans exist, and the purge
// tick is registered as a fixed-delay task, which needs the shared @EnableScheduling to be active
// without the relay's scheduler. The task's delay comes from ledger.idempotency.purge.interval
// (5m by default, 7s when configured), not from the @Scheduled placeholder's own default, and the
// scheduler has two threads. Each context's first tick runs a real purge against its own empty
// container, which deletes nothing.
class IdempotencyKeyPurgeScheduledTest {

	private static final String TICK = IdempotencyKeyPurgeScheduler.class.getName() + ".tick";

	@Nested
	@Import(TestcontainersConfiguration.class)
	@SpringBootTest(properties = { "ledger.idempotency.purge.enabled=true",
			"ledger.idempotency.purge.scheduled=true" })
	class DefaultInterval {

		@Autowired
		ApplicationContext context;

		@Test
		void purgerAndSchedulerWithAFiveMinuteTickOnTwoThreads() {
			assertThat(this.context.getBeanNamesForType(IdempotencyKeyPurger.class)).hasSize(1);
			assertThat(this.context.getBeanNamesForType(IdempotencyKeyPurgeScheduler.class)).hasSize(1);
			assertThat(purgeTicks(this.context)).singleElement()
				.extracting(FixedDelayTask::getIntervalDuration)
				.isEqualTo(Duration.ofMinutes(5));
			assertThat(this.context.getBean(ThreadPoolTaskScheduler.class).getScheduledThreadPoolExecutor()
				.getCorePoolSize()).as("scheduler threads").isEqualTo(2);
		}

	}

	@Nested
	@Import(TestcontainersConfiguration.class)
	@SpringBootTest(properties = { "ledger.idempotency.purge.enabled=true",
			"ledger.idempotency.purge.scheduled=true", "ledger.idempotency.purge.interval=7s" })
	class ConfiguredInterval {

		@Autowired
		ApplicationContext context;

		@Test
		void tickUsesTheConfiguredInterval() {
			assertThat(purgeTicks(this.context)).singleElement()
				.extracting(FixedDelayTask::getIntervalDuration)
				.isEqualTo(Duration.ofSeconds(7));
		}

	}

	// Fixed-delay tasks whose runnable is IdempotencyKeyPurgeScheduler.tick; a scheduled method's
	// runnable (or the wrapper around it) prints as declaring class + "." + method name.
	private static List<FixedDelayTask> purgeTicks(ApplicationContext context) {
		return context.getBeansOfType(ScheduledTaskHolder.class)
			.values()
			.stream()
			.flatMap((holder) -> holder.getScheduledTasks().stream())
			.map(ScheduledTask::getTask)
			.filter((task) -> TICK.equals(runnableName(task)))
			.map((task) -> {
				assertThat(task).as("purge tick task type").isInstanceOf(FixedDelayTask.class);
				return (FixedDelayTask) task;
			})
			.toList();
	}

	private static String runnableName(Task task) {
		return task.getRunnable().toString();
	}

}
