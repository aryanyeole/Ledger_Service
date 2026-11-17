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

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

import dev.aryanyeole.ledgerapi.cache.LedgerRedisProperties;
import dev.aryanyeole.ledgerapi.cache.RedisGateway;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyCache;

// No test context has a Redis client unless it opts in (D-137, D-138): the test
// application.properties turns both cache switches off, so there is no gateway, no idempotency
// cache (every existing test stays on the DB path, D-139) and ledger.redis.url isn't bound. Same annotations as OutboxRelayAbsentTest, so this reuses that cached context. The
// main config, read directly, turns both caches on and reads the URL from REDIS_URL.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class RedisGatewayAbsentTest {

	@Autowired
	ApplicationContext context;

	@Autowired
	Environment environment;

	@Test
	void noRedisBeansWithoutOptIn() {
		for (Class<?> type : List.of(RedisGateway.class, LedgerRedisProperties.class, RedisClient.class,
				StatefulRedisConnection.class, IdempotencyCache.class)) {
			assertThat(this.context.getBeanNamesForType(type)).as("beans of type %s", type.getSimpleName()).isEmpty();
		}
		assertThat(this.environment.getProperty("ledger.cache.idempotency.enabled")).isEqualTo("false");
		assertThat(this.environment.getProperty("ledger.cache.balance.enabled")).isEqualTo("false");
	}

	@Test
	void mainConfigTurnsBothCachesOn() {
		YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
		yaml.setResources(new ClassPathResource("application.yml"));
		Properties main = yaml.getObject();
		assertThat(main).isNotNull();
		assertThat(String.valueOf(main.get("ledger.cache.idempotency.enabled"))).isEqualTo("true");
		assertThat(String.valueOf(main.get("ledger.cache.balance.enabled"))).isEqualTo("true");
		assertThat(String.valueOf(main.get("ledger.redis.url"))).isEqualTo("${REDIS_URL:redis://localhost:6379}");
	}

}
