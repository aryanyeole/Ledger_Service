package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.config.ScheduledTaskHolder;

import dev.aryanyeole.ledgerapi.idempotency.IdempotencyKeyPurgeScheduler;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyKeyPurger;

// No test context purges unless it opts in (D-126): the test application.properties turns both
// purge switches off, and with the relay's off too nothing is scheduled at all (D-127). Same
// annotations as DbRolePrivilegesTest, so this reuses that cached context and container. The main
// config, read directly, enables and schedules the purge and gives the scheduler two threads.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class IdempotencyKeyPurgeAbsentTest {

	@Autowired
	ApplicationContext context;

	@Autowired
	Environment environment;

	@Test
	void noPurgerSchedulerOrScheduledTasksWithoutOptIn() {
		for (Class<?> type : List.of(IdempotencyKeyPurger.class, IdempotencyKeyPurgeScheduler.class,
				ScheduledTaskHolder.class)) {
			assertThat(this.context.getBeanNamesForType(type)).as("beans of type %s", type.getSimpleName()).isEmpty();
		}
		assertThat(this.environment.getProperty("ledger.idempotency.purge.enabled")).isEqualTo("false");
		assertThat(this.environment.getProperty("ledger.idempotency.purge.scheduled")).isEqualTo("false");
	}

	@Test
	void mainConfigEnablesAndSchedulesThePurgeOnItsOwnThread() {
		YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
		yaml.setResources(new ClassPathResource("application.yml"));
		Properties main = yaml.getObject();
		assertThat(main).isNotNull();
		assertThat(String.valueOf(main.get("ledger.idempotency.purge.enabled"))).isEqualTo("true");
		assertThat(String.valueOf(main.get("ledger.idempotency.purge.scheduled"))).isEqualTo("true");
		assertThat(String.valueOf(main.get("spring.task.scheduling.pool.size"))).isEqualTo("2");
	}

}
