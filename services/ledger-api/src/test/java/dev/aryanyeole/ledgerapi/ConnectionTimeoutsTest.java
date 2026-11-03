package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.jdbc.PgConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

// ledger-api's Postgres connection timeouts (D-132, BUG-012). The pool's connections read with a
// 60 s socket timeout; Flyway's derived SimpleDriverDataSource has none, so a long migration is
// never cut off. Both give up establishing a connection after loginTimeout (10 s, from
// org/postgresql/driverconfig.properties) against a server that accepts TCP and never answers:
// before D-132 that attempt blocked forever in pgjdbc's doAuthentication.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(webEnvironment = WebEnvironment.NONE)
class ConnectionTimeoutsTest {

	private static final int POOL_SOCKET_TIMEOUT_MILLIS = 60_000;

	private static final Duration LOGIN_TIMEOUT = Duration.ofSeconds(10);

	// pgjdbc's ConnectTask returns at the login timeout; allow for scheduling, not for a second bound.
	private static final Duration LOGIN_MARGIN = Duration.ofSeconds(5);

	@Autowired
	DataSource dataSource;

	@Autowired
	Flyway flyway;

	@Test
	void poolConnectionsHaveTheSocketTimeout() throws Exception {
		try (Connection conn = this.dataSource.getConnection()) {
			assertThat(conn.unwrap(PgConnection.class).getNetworkTimeout()).isEqualTo(POOL_SOCKET_TIMEOUT_MILLIS);
		}
	}

	@Test
	void flywaysConnectionHasNoSocketTimeout() throws Exception {
		DataSource migration = this.flyway.getConfiguration().getDataSource();
		assertThat(migration).isInstanceOf(SimpleDriverDataSource.class);
		try (Connection conn = migration.getConnection()) {
			assertThat(conn.unwrap(PgConnection.class).getNetworkTimeout()).isZero();
		}
	}

	@Test
	@Timeout(value = 60, unit = TimeUnit.SECONDS)
	void flywaysDataSourceGivesUpALoginAfterTheLoginTimeout() throws Exception {
		DataSource migration = this.flyway.getConfiguration().getDataSource();
		try (StalledServer stalled = new StalledServer()) {
			// The same kind of DataSource with the same credentials, pointed at the stalled server.
			DataSource toStalled = DataSourceBuilder.derivedFrom(migration).url(stalled.jdbcUrl()).build();
			assertThat(toStalled).isInstanceOf(SimpleDriverDataSource.class);
			long started = System.nanoTime();
			assertThatThrownBy(toStalled::getConnection).hasMessageContaining("Connection attempt timed out");
			assertLoginBounded(Duration.ofNanos(System.nanoTime() - started));
		}
	}

	@Test
	@Timeout(value = 60, unit = TimeUnit.SECONDS)
	void thePoolGivesUpALoginAfterTheLoginTimeout() throws Exception {
		HikariConfig config = new HikariConfig();
		((HikariDataSource) this.dataSource).copyStateTo(config);
		try (StalledServer stalled = new StalledServer()) {
			config.setJdbcUrl(stalled.jdbcUrl());
			config.setPoolName("connection-timeouts-stalled");
			long started = System.nanoTime();
			// initializationFailTimeout is 1 by default: the pool's first connection is made here.
			assertThatThrownBy(() -> new HikariDataSource(config).close()).rootCause()
				.hasMessageContaining("Connection attempt timed out");
			assertLoginBounded(Duration.ofNanos(System.nanoTime() - started));
		}
	}

	private static void assertLoginBounded(Duration elapsed) {
		assertThat(elapsed).as("the login gave up at loginTimeout, not earlier").isGreaterThanOrEqualTo(LOGIN_TIMEOUT);
		assertThat(elapsed).as("and not much later").isLessThan(LOGIN_TIMEOUT.plus(LOGIN_MARGIN));
	}

	// Accepts TCP connections and never answers: the database that hung BUG-012's startup.
	private static final class StalledServer implements AutoCloseable {

		private final ServerSocket server;

		private final List<Socket> accepted = new CopyOnWriteArrayList<>();

		StalledServer() throws IOException {
			this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
			Thread acceptor = new Thread(() -> {
				while (!this.server.isClosed()) {
					try {
						this.accepted.add(this.server.accept());
					}
					catch (IOException ex) {
						return;
					}
				}
			}, "stalled-postgres");
			acceptor.setDaemon(true);
			acceptor.start();
		}

		String jdbcUrl() {
			return "jdbc:postgresql://127.0.0.1:" + this.server.getLocalPort() + "/ledger";
		}

		@Override
		public void close() throws IOException {
			this.server.close();
			for (Socket socket : this.accepted) {
				socket.close();
			}
		}

	}

}
