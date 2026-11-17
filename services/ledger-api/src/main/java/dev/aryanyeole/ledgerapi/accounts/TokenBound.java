package dev.aryanyeole.ledgerapi.accounts;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Enumeration;
import java.util.Properties;

import javax.sql.DataSource;

import org.postgresql.Driver;

import com.zaxxer.hikari.HikariDataSource;

import dev.aryanyeole.ledgerapi.cache.BalanceCacheScripts;
import dev.aryanyeole.ledgerapi.cache.RedisGateway;

// The balance cache's token bound (D-142, D-151, docs/design/cache-invalidation.md §2.3): a token
// must outlive any reader that saw the token before it, and a reader's MGET -> compare-and-set window
// is bounded by the pool's effective connectionTimeout + socketTimeout + 2 x the Redis command
// timeout. ledger-api refuses to start while the balance cache is on and the bound fails, so raising
// a pool timeout can't silently break it. socketTimeout is resolved as pgjdbc resolves it
// (driverconfig.properties < the DataSource's properties < the JDBC URL); missing, unparseable or 0
// (pgjdbc's "unbounded") fails startup rather than being assumed.
final class TokenBound {

	private static final String SOCKET_TIMEOUT = "socketTimeout";

	private TokenBound() {
	}

	/** Throws IllegalStateException, naming every value, unless tokenPx > the reader's window. */
	static void check(long tokenPxMillis, Duration connectionTimeout, Duration socketTimeout, Duration commandTimeout) {
		Duration window = connectionTimeout.plus(socketTimeout).plus(commandTimeout.multipliedBy(2));
		if (tokenPxMillis <= window.toMillis()) {
			throw new IllegalStateException("Balance cache token PX (" + tokenPxMillis
					+ " ms) must exceed Hikari connectionTimeout (" + connectionTimeout.toMillis()
					+ " ms) + socketTimeout (" + socketTimeout.toMillis() + " ms) + 2 x Redis command timeout ("
					+ commandTimeout.toMillis() + " ms) = " + window.toMillis()
					+ " ms (D-142); lower a pool timeout or turn ledger.cache.balance.enabled off");
		}
	}

	/** The check against the running pool. */
	static void check(DataSource dataSource) {
		HikariDataSource hikari;
		try {
			hikari = dataSource.unwrap(HikariDataSource.class);
		}
		catch (SQLException ex) {
			throw new IllegalStateException("Balance cache token bound: the DataSource is not a HikariDataSource, so "
					+ "its connectionTimeout can't be read (D-142)", ex);
		}
		check(BalanceCacheScripts.TOKEN_PX_MILLIS, Duration.ofMillis(hikari.getConnectionTimeout()),
				socketTimeout(hikari.getJdbcUrl(), hikari.getDataSourceProperties(), driverConfigDefaults()),
				RedisGateway.COMMAND_TIMEOUT);
	}

	/** pgjdbc's precedence: defaults < dataSourceProperties < URL parameters. */
	static Duration socketTimeout(String jdbcUrl, Properties dataSourceProperties, Properties defaults) {
		Properties merged = new Properties();
		merged.putAll(defaults);
		dataSourceProperties.forEach((name, value) -> merged.put(name, String.valueOf(value)));
		Properties parsed = (jdbcUrl == null) ? null : Driver.parseURL(jdbcUrl, merged);
		if (parsed == null) {
			throw new IllegalStateException(
					"Balance cache token bound: the JDBC URL can't be parsed, so socketTimeout is unknown (D-142)");
		}
		String value = parsed.getProperty(SOCKET_TIMEOUT);
		if (value == null) {
			throw new IllegalStateException("Balance cache token bound: socketTimeout is not set (D-132); pgjdbc's "
					+ "default 0 is unbounded, so the bound can't hold (D-142)");
		}
		long seconds;
		try {
			seconds = Long.parseLong(value.trim());
		}
		catch (NumberFormatException ex) {
			throw new IllegalStateException("Balance cache token bound: socketTimeout is not an integer (D-142)", ex);
		}
		if (seconds <= 0) {
			throw new IllegalStateException("Balance cache token bound: socketTimeout is " + seconds
					+ " (unbounded), so the bound can't hold (D-132, D-142)");
		}
		return Duration.ofSeconds(seconds);
	}

	/** Every org/postgresql/driverconfig.properties on the classpath, as pgjdbc loads them. */
	static Properties driverConfigDefaults() {
		Properties defaults = new Properties();
		try {
			ClassLoader loader = TokenBound.class.getClassLoader();
			Enumeration<URL> resources = loader.getResources("org/postgresql/driverconfig.properties");
			while (resources.hasMoreElements()) {
				try (InputStream in = resources.nextElement().openStream()) {
					defaults.load(in);
				}
			}
		}
		catch (IOException ex) {
			throw new IllegalStateException("Balance cache token bound: can't read driverconfig.properties (D-142)",
					ex);
		}
		return defaults;
	}

}
