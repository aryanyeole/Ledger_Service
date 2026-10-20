package dev.aryanyeole.ledgerapi.db;

import java.util.Map;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

// Migrate-only mode (D-083): with ledger.migrate-only=true the application runs Flyway and nothing
// else, then LedgerApiApplication.main exits. This forces the rest off, at a precedence above
// command-line arguments, so no argument can start a web server, the outbox relay or its
// scheduler during a migration: spring.main.web-application-type is bound after this runs, and
// the relay's @Conditional checks are evaluated later still. Runs last among the post-processors
// (lowest precedence order), so ledger.migrate-only is seen from every config source.
// Registered in META-INF/spring.factories.
public class MigrateOnlyEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

	public static final String PROPERTY = "ledger.migrate-only";

	static final String SOURCE_NAME = "ledgerMigrateOnly";

	static final Map<String, Object> FORCED = Map.of("spring.main.web-application-type", "none",
			"ledger.outbox.relay.enabled", "false", "ledger.outbox.relay.scheduled", "false");

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
		if (isEnabled(environment)) {
			environment.getPropertySources().addFirst(new MapPropertySource(SOURCE_NAME, FORCED));
		}
	}

	public static boolean isEnabled(ConfigurableEnvironment environment) {
		return environment.getProperty(PROPERTY, Boolean.class, false);
	}

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE;
	}

}
