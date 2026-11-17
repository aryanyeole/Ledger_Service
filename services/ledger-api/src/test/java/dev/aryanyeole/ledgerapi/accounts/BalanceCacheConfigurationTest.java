package dev.aryanyeole.ledgerapi.accounts;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import com.zaxxer.hikari.HikariDataSource;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import dev.aryanyeole.ledgerapi.cache.RedisGateway;
import dev.aryanyeole.ledgerapi.cache.TestRedisGateways;
import dev.aryanyeole.ledgerapi.posting.BalanceCacheInvalidator;

// BalanceCacheConfiguration's startup rules (D-127, D-142, D-151), without a server or a database:
// the properties are validated even with the cache off; with it on, both beans exist, invalidation=false
// removes the invalidator and logs a WARNING, and the token bound fails startup against a pool whose
// timeouts are too long or whose socketTimeout is unset. The pool is a HikariDataSource that is never
// started (unwrap and the getters don't open it); the gateway is over a fake that is never called.
@ExtendWith(OutputCaptureExtension.class)
class BalanceCacheConfigurationTest {

	private final ApplicationContextRunner runner = runner(30_000, "60");

	private static ApplicationContextRunner runner(long connectionTimeoutMillis, String socketTimeoutSeconds) {
		return new ApplicationContextRunner().withUserConfiguration(BalanceCacheConfiguration.class)
			.withBean(RedisGateway.class,
					() -> TestRedisGateways.over(new BalanceCacheUnitTest.FakeRedis(), new AtomicLong()::get))
			.withBean(MeterRegistry.class, SimpleMeterRegistry::new)
			.withBean(DataSource.class, () -> pool(connectionTimeoutMillis, socketTimeoutSeconds));
	}

	private static HikariDataSource pool(long connectionTimeoutMillis, String socketTimeoutSeconds) {
		HikariDataSource pool = new HikariDataSource();
		pool.setJdbcUrl("jdbc:postgresql://localhost:5433/ledger");
		pool.setConnectionTimeout(connectionTimeoutMillis);
		if (socketTimeoutSeconds != null) {
			pool.addDataSourceProperty("socketTimeout", socketTimeoutSeconds);
		}
		return pool;
	}

	@Test
	void offByDefaultAndTheTtlIsStillValidated() {
		this.runner.run((context) -> {
			assertThat(context).hasNotFailed().doesNotHaveBean(BalanceCache.class).doesNotHaveBean(BalanceCacheInvalidator.class);
			assertThat(context.getBean(LedgerBalanceCacheProperties.class).ttl()).hasSeconds(60);
		});
		this.runner.withPropertyValues("ledger.cache.balance.ttl=0s")
			.run((context) -> assertThat(context).hasFailed()
				.getFailure()
				.rootCause()
				.hasMessageContaining("ledger.cache.balance.ttl must be positive"));
	}

	@Test
	void onCreatesTheReadAndWriteSides(CapturedOutput output) {
		this.runner.withPropertyValues("ledger.cache.balance.enabled=true").run((context) -> {
			assertThat(context).hasNotFailed().hasSingleBean(BalanceCache.class).hasSingleBean(BalanceCacheInvalidator.class);
		});
		assertThat(output).doesNotContain("Balance cache invalidation is OFF");
	}

	@Test
	void invalidationOffRemovesTheInvalidatorAndWarns(CapturedOutput output) {
		this.runner.withPropertyValues("ledger.cache.balance.enabled=true", "ledger.cache.balance.invalidation=false",
				"ledger.cache.balance.ttl=1s")
			.run((context) -> assertThat(context).hasNotFailed()
				.hasSingleBean(BalanceCache.class)
				.doesNotHaveBean(BalanceCacheInvalidator.class));
		assertThat(output).contains("WARN")
			.contains("Balance cache invalidation is OFF (ledger.cache.balance.invalidation=false)")
			.contains("ttl=PT1S");
	}

	@Test
	void theTokenBoundFailsStartupWhileTheCacheIsOn() {
		ApplicationContextRunner raised = runner(240_000, "60");
		raised.withPropertyValues("ledger.cache.balance.enabled=true")
			.run((context) -> assertThat(context).hasFailed()
				.getFailure()
				.rootCause()
				.hasMessageContaining("connectionTimeout (240000 ms)"));
		raised.run((context) -> assertThat(context).as("cache off: no check").hasNotFailed());

		runner(30_000, null)
			.withPropertyValues("ledger.cache.balance.enabled=true")
			.run((context) -> assertThat(context).hasFailed()
				.getFailure()
				.rootCause()
				.hasMessageContaining("socketTimeout is not set"));
	}

}
