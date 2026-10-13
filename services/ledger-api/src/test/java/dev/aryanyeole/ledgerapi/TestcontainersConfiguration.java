package dev.aryanyeole.ledgerapi;

import java.nio.file.Path;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

// Postgres only until M3 adds the outbox relay; image pinned per D-027.
// No @ServiceConnection: it would connect as the container superuser. The container is
// bootstrapped by the same init script as Compose, and the app connects with the same
// properties as application.yml: ledger_api_app at runtime, ledger_api_owner for Flyway.
@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

	// Relative to the module directory, which is the working directory for Maven tests.
	private static final Path ROLES_SCRIPT = Path.of("../../infra/postgres/init/01-roles.sh");

	// Test-only credentials for throwaway containers. The worker and owner passwords are
	// package-private so proof tests can connect as those roles; outside Flyway, production
	// never connects as ledger_api_owner.
	static final String OWNER_PASSWORD = "test-owner-password";
	private static final String APP_PASSWORD = "test-app-password";
	static final String WORKER_PASSWORD = "test-worker-password";

	@Bean
	PostgreSQLContainer postgresContainer() {
		return new PostgreSQLContainer(DockerImageName.parse("postgres:17.11"))
			.withDatabaseName("ledger")
			.withCopyFileToContainer(MountableFile.forHostPath(ROLES_SCRIPT, 0755),
					"/docker-entrypoint-initdb.d/01-roles.sh")
			.withEnv("LEDGER_API_OWNER_PASSWORD", OWNER_PASSWORD)
			.withEnv("LEDGER_API_APP_PASSWORD", APP_PASSWORD)
			.withEnv("SETTLEMENT_WORKER_PASSWORD", WORKER_PASSWORD);
	}

	@Bean
	DynamicPropertyRegistrar postgresProperties(PostgreSQLContainer postgres) {
		return registry -> {
			registry.add("spring.datasource.url", postgres::getJdbcUrl);
			registry.add("spring.datasource.username", () -> "ledger_api_app");
			registry.add("spring.datasource.password", () -> APP_PASSWORD);
			registry.add("spring.flyway.user", () -> "ledger_api_owner");
			registry.add("spring.flyway.password", () -> OWNER_PASSWORD);
		};
	}

}
