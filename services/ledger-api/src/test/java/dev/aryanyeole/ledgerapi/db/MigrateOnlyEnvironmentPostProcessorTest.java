package dev.aryanyeole.ledgerapi.db;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.core.io.support.SpringFactoriesLoader;

// Migrate-only mode (D-083): ledger.migrate-only=true forces the web server, the outbox relay and
// its scheduler off, above command-line arguments; otherwise nothing changes. The exit codes are
// proven out of process by the worker's test harness (it requires exit 0 from java -jar).
class MigrateOnlyEnvironmentPostProcessorTest {

	private final MigrateOnlyEnvironmentPostProcessor processor = new MigrateOnlyEnvironmentPostProcessor();

	@Test
	void migrateOnlyForcesWebServerRelayAndSchedulerOffOverArguments() {
		StandardEnvironment environment = new StandardEnvironment();
		environment.getPropertySources()
			.addFirst(new SimpleCommandLinePropertySource("--ledger.migrate-only=true",
					"--ledger.outbox.relay.enabled=true", "--ledger.outbox.relay.scheduled=true",
					"--spring.main.web-application-type=servlet"));

		this.processor.postProcessEnvironment(environment, new SpringApplication());

		assertThat(environment.getProperty("spring.main.web-application-type")).isEqualTo("none");
		assertThat(environment.getProperty("ledger.outbox.relay.enabled")).isEqualTo("false");
		assertThat(environment.getProperty("ledger.outbox.relay.scheduled")).isEqualTo("false");
		assertThat(environment.getPropertySources().iterator().next().getName())
			.as("highest-precedence property source")
			.isEqualTo(MigrateOnlyEnvironmentPostProcessor.SOURCE_NAME);
	}

	@ParameterizedTest(name = "ledger.migrate-only={0}")
	@ValueSource(strings = { "false", "ABSENT" })
	void otherwiseNothingChanges(String value) {
		StandardEnvironment environment = new StandardEnvironment();
		Map<String, Object> properties = value.equals("ABSENT") ? Map.of("ledger.outbox.relay.enabled", "true")
				: Map.of("ledger.migrate-only", value, "ledger.outbox.relay.enabled", "true");
		environment.getPropertySources().addFirst(new MapPropertySource("test", properties));

		this.processor.postProcessEnvironment(environment, new SpringApplication());

		assertThat(environment.getPropertySources().contains(MigrateOnlyEnvironmentPostProcessor.SOURCE_NAME))
			.isFalse();
		assertThat(environment.getProperty("ledger.outbox.relay.enabled")).isEqualTo("true");
		assertThat(environment.getProperty("spring.main.web-application-type")).isNull();
	}

	// By name: loading the factories would instantiate Boot's own post-processors too, which need
	// constructor arguments.
	@Test
	void registeredInSpringFactories() throws Exception {
		List<String> registered = new ArrayList<>();
		Enumeration<URL> files = getClass().getClassLoader().getResources(SpringFactoriesLoader.FACTORIES_RESOURCE_LOCATION);
		while (files.hasMoreElements()) {
			Properties factories = PropertiesLoaderUtils.loadProperties(new UrlResource(files.nextElement()));
			String names = factories.getProperty(EnvironmentPostProcessor.class.getName(), "");
			Arrays.stream(names.split(",")).map(String::strip).forEach(registered::add);
		}
		assertThat(registered).contains(MigrateOnlyEnvironmentPostProcessor.class.getName());
	}

}
