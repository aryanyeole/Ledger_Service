package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.testcontainers.postgresql.PostgreSQLContainer;

import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.outbox.OutboxRelay;
import dev.aryanyeole.ledgerapi.outbox.OutboxRelayScheduler;

// Migrate-only mode in process (D-083): with ledger.migrate-only=true the application context
// starts without a web server, a web filter, the outbox relay or its scheduler, and Flyway has
// applied every migration. Exiting is main()'s job, proven out of process by the worker's test
// harness (java -jar ... --ledger.migrate-only=true must exit 0). Relay switches passed as
// properties don't matter: the post-processor overrides them.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(webEnvironment = WebEnvironment.NONE, properties = { "ledger.migrate-only=true",
		"ledger.outbox.relay.enabled=true", "ledger.outbox.relay.scheduled=true" })
class MigrateOnlyContextTest {

	// Relative to the module directory, the working directory for Maven tests.
	private static final Path MIGRATIONS = Path.of("src/main/resources/db/migration");

	private static final Pattern VERSIONED = Pattern.compile("V(\\d+)__.*\\.sql");

	@Autowired
	ApplicationContext context;

	@Autowired
	Environment environment;

	@Autowired
	PostgreSQLContainer postgres;

	@Test
	void contextHasNoWebServerFilterOrRelay() {
		assertThat(this.context).isNotInstanceOf(WebServerApplicationContext.class);
		assertThat(this.environment.getProperty("ledger.outbox.relay.enabled")).isEqualTo("false");
		assertThat(this.environment.getProperty("ledger.outbox.relay.scheduled")).isEqualTo("false");
		for (Class<?> type : List.of(ApiKeyFilter.class, OutboxRelay.class, OutboxRelayScheduler.class)) {
			assertThat(this.context.getBeanNamesForType(type)).as("beans of type %s", type.getSimpleName()).isEmpty();
		}
	}

	@Test
	void flywayAppliedEveryMigration() throws Exception {
		int highest;
		try (Stream<Path> files = Files.list(MIGRATIONS)) {
			highest = files.map((p) -> VERSIONED.matcher(p.getFileName().toString()))
				.filter(Matcher::matches)
				.mapToInt((m) -> Integer.parseInt(m.group(1)))
				.max()
				.orElseThrow();
		}
		try (Connection conn = DriverManager.getConnection(this.postgres.getJdbcUrl(), "ledger_api_owner",
				TestcontainersConfiguration.OWNER_PASSWORD);
				PreparedStatement ps = conn.prepareStatement(
						"SELECT count(*) FILTER (WHERE NOT success), max(version::int) FROM flyway_schema_history WHERE version IS NOT NULL");
				ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).isTrue();
			assertThat(rs.getInt(1)).as("failed migrations").isZero();
			assertThat(rs.getInt(2)).as("latest migration (highest V file in %s)", MIGRATIONS).isEqualTo(highest);
		}
	}

}
