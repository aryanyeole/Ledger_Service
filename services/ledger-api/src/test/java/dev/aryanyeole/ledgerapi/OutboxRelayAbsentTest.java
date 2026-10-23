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
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import dev.aryanyeole.ledgerapi.outbox.DlqTopicGuard;
import dev.aryanyeole.ledgerapi.outbox.OutboxRelay;
import dev.aryanyeole.ledgerapi.outbox.OutboxRelayScheduler;
import dev.aryanyeole.ledgerapi.outbox.RelayFaultHook;
import dev.aryanyeole.ledgerapi.outbox.RelayTopicGuard;

// No test context runs the relay or holds a Kafka client unless it opts in (D-096, D-098): the
// test application.properties turns the relay off, and with Boot's Kafka auto-configuration
// excluded there is no producer factory, template or admin either. Same annotations as
// DbRolePrivilegesTest, so this reuses that cached context and container. The main config, read
// directly, enables and schedules the relay and excludes the auto-configuration.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class OutboxRelayAbsentTest {

	@Autowired
	ApplicationContext context;

	@Autowired
	Environment environment;

	@Test
	void noRelayOrKafkaClientBeansWithoutOptIn() {
		for (Class<?> type : List.of(OutboxRelay.class, OutboxRelayScheduler.class, RelayTopicGuard.class,
				DlqTopicGuard.class, RelayFaultHook.class, ProducerFactory.class, KafkaTemplate.class, KafkaAdmin.class)) {
			assertThat(this.context.getBeanNamesForType(type)).as("beans of type %s", type.getSimpleName()).isEmpty();
		}
		assertThat(this.environment.getProperty("ledger.outbox.relay.enabled")).isEqualTo("false");
		assertThat(this.environment.getProperty("ledger.outbox.relay.scheduled")).isEqualTo("false");
	}

	@Test
	void mainConfigEnablesAndSchedulesTheRelay() {
		YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
		yaml.setResources(new ClassPathResource("application.yml"));
		Properties main = yaml.getObject();
		assertThat(main).isNotNull();
		assertThat(String.valueOf(main.get("ledger.outbox.relay.enabled"))).isEqualTo("true");
		assertThat(String.valueOf(main.get("ledger.outbox.relay.scheduled"))).isEqualTo("true");
		assertThat(String.valueOf(main.get("spring.autoconfigure.exclude")))
			.isEqualTo("org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration");
	}

}
