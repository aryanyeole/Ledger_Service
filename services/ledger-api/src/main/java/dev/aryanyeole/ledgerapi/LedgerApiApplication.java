package dev.aryanyeole.ledgerapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import dev.aryanyeole.ledgerapi.db.MigrateOnlyEnvironmentPostProcessor;

// Exits explicitly rather than when the JVM happens to go idle (D-083): any startup failure,
// including a failed migration, exits 1 (Boot has already logged it); in migrate-only mode the
// context is closed after Flyway and the process exits with SpringApplication.exit's code (0).
@SpringBootApplication
public class LedgerApiApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context;
		try {
			context = SpringApplication.run(LedgerApiApplication.class, args);
		}
		catch (RuntimeException ex) {
			System.exit(1);
			return;
		}
		if (MigrateOnlyEnvironmentPostProcessor.isEnabled(context.getEnvironment())) {
			System.exit(SpringApplication.exit(context));
		}
	}

}
