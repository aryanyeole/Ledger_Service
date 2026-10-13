package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.util.PSQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.postgresql.PostgreSQLContainer;

// Proof that the database roles match the declared model (D-006, D-033, D-034, D-040):
// role attributes, memberships, ownership, the table/column/sequence privilege matrix,
// schema and database privileges, trigger hardening, and that the runtime roles cannot
// bypass any of it. Catalog reads go through the app connection; pg_catalog and the
// has_*_privilege() functions are readable by every role.
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class DbRolePrivilegesTest {

	private static final String INSUFFICIENT_PRIVILEGE = "42501";

	private static final String SEARCH_PATH = "search_path=pg_catalog, public, pg_temp";

	enum Role {

		APP("ledger_api_app"), WORKER("settlement_worker"), OWNER("ledger_api_owner");

		final String dbRole;

		Role(String dbRole) {
			this.dbRole = dbRole;
		}

	}

	private static final List<Role> RUNTIME_ROLES = List.of(Role.APP, Role.WORKER);

	// MAINTAIN is new in Postgres 17 (VACUUM, ANALYZE, REINDEX, CLUSTER, REFRESH, LOCK TABLE).
	private static final List<String> TABLE_PRIVILEGES = List.of("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE",
			"REFERENCES", "TRIGGER", "MAINTAIN");

	// Table-level privileges per table and runtime role; anything not listed must be false.
	// accounts INSERT/UPDATE are column-level grants (see accountsColumnPrivileges), so they
	// are false at table level. Every table in public must have a row here: M2/M3 tables
	// add theirs, and idempotency_keys will be all false for settlement_worker (D-020).
	private static final Map<String, Map<Role, Set<String>>> TABLE_MATRIX = Map.of(
			"accounts", Map.of(Role.APP, Set.of("SELECT"), Role.WORKER, Set.of("SELECT")),
			"transactions", Map.of(Role.APP, Set.of("SELECT", "INSERT"), Role.WORKER, Set.of("SELECT", "INSERT")),
			"entries", Map.of(Role.APP, Set.of("SELECT", "INSERT"), Role.WORKER, Set.of("SELECT", "INSERT")),
			"flyway_schema_history", Map.of(Role.APP, Set.of(), Role.WORKER, Set.of()));

	private static final List<String> ACCOUNTS_COLUMNS = List.of("id", "name", "type", "currency", "balance",
			"allow_negative", "created_at");

	// Column privileges on accounts, beyond table-level SELECT (which covers every column).
	private static final Map<Role, Map<String, Set<String>>> ACCOUNTS_COLUMN_GRANTS = Map.of(Role.APP,
			Map.of("INSERT", Set.of("name", "type", "allow_negative"), "UPDATE", Set.of("balance")), Role.WORKER,
			Map.of("INSERT", Set.of(), "UPDATE", Set.of("balance")));

	record TriggerSpec(String table, String function, boolean deferred) {
	}

	private static final Map<String, TriggerSpec> TRIGGERS = Map.of(
			"entries_balanced", new TriggerSpec("entries", "assert_transaction_balanced", true),
			"transactions_have_entries", new TriggerSpec("transactions", "assert_transaction_has_entries", true),
			"entries_append_only", new TriggerSpec("entries", "forbid_mutation", false),
			"transactions_append_only", new TriggerSpec("transactions", "forbid_mutation", false));

	enum Bypass {

		REPLICA_MODE("SET session_replication_role = replica",
				"permission denied to set parameter \"session_replication_role\""),
		DISABLE_TRIGGER("ALTER TABLE entries DISABLE TRIGGER entries_balanced", "must be owner of table entries"),
		DROP_TRIGGER("DROP TRIGGER entries_append_only ON entries", "must be owner of relation entries"),
		TEMP_SHADOW("CREATE TEMP TABLE entries (transaction_id bigint, amount numeric)",
				"permission denied to create temporary tables in database \"ledger\""),
		SET_ROLE_OWNER("SET ROLE ledger_api_owner", "permission denied to set role \"ledger_api_owner\""),
		CREATE_TABLE("CREATE TABLE public.t7_probe (id int)", "permission denied for schema public"),
		REPLACE_FUNCTION(
				"CREATE OR REPLACE FUNCTION public.forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$",
				"permission denied for schema public"),
		CREATE_SCHEMA("CREATE SCHEMA t7_shadow", "permission denied for database ledger");

		final String sql;

		final String message;

		Bypass(String sql, String message) {
			this.sql = sql;
			this.message = message;
		}

	}

	record MatrixDiff(Set<String> missingFromMatrix, Set<String> staleInMatrix) {
	}

	@Autowired
	DataSource dataSource;

	@Autowired
	PostgreSQLContainer postgres;

	// Folded in from DbRoleBootstrapTest: proof tests that use the app DataSource exercise
	// the runtime role, not a superuser.
	@Test
	void appDataSourceIsNonSuperuserAppRole() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				assertThat(queryBoolean(conn, "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"))
					.as("ledger_api_app: app DataSource session is superuser")
					.isFalse();
			}
			finally {
				conn.rollback();
			}
		}
	}

	// A
	@Test
	void roleAttributes() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try (PreparedStatement ps = conn.prepareStatement("""
					SELECT rolname, rolcanlogin, rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls
					FROM pg_roles WHERE rolname = ANY (?)""")) {
				ps.setArray(1, conn.createArrayOf("text", allRoleNames()));
				Set<String> seen = new HashSet<>();
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						String role = rs.getString("rolname");
						seen.add(role);
						assertThat(rs.getBoolean("rolcanlogin")).as("%s: rolcanlogin", role).isTrue();
						for (String attribute : List.of("rolsuper", "rolcreaterole", "rolcreatedb", "rolreplication",
								"rolbypassrls")) {
							assertThat(rs.getBoolean(attribute)).as("%s: %s", role, attribute).isFalse();
						}
					}
				}
				assertThat(seen).as("roles present").containsExactlyInAnyOrder(allRoleNames());
			}
			finally {
				conn.rollback();
			}
		}
	}

	// B
	@Test
	void noRoleMemberships() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				try (PreparedStatement ps = conn.prepareStatement("""
						SELECT pg_get_userbyid(m.roleid), pg_get_userbyid(m.member)
						FROM pg_auth_members m
						WHERE m.roleid IN (SELECT oid FROM pg_roles WHERE rolname = ANY (?))
						   OR m.member IN (SELECT oid FROM pg_roles WHERE rolname = ANY (?))""")) {
					Array roles = conn.createArrayOf("text", allRoleNames());
					ps.setArray(1, roles);
					ps.setArray(2, roles);
					List<String> memberships = new ArrayList<>();
					try (ResultSet rs = ps.executeQuery()) {
						while (rs.next()) {
							memberships.add(rs.getString(2) + " member of " + rs.getString(1));
						}
					}
					assertThat(memberships).as("pg_auth_members rows involving %s", Arrays.toString(allRoleNames()))
						.isEmpty();
				}
				for (Role role : RUNTIME_ROLES) {
					assertThat(queryBoolean(conn, "SELECT pg_has_role(?::name, 'ledger_api_owner', 'MEMBER')",
							role.dbRole))
						.as("%s: member of ledger_api_owner", role.dbRole)
						.isFalse();
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

	// C
	@Test
	void ownerOwnsEverythingInPublic() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				Map<String, String> relationOwners = queryPairs(conn, """
						SELECT c.relname || ' (' || c.relkind::text || ')', pg_get_userbyid(c.relowner)
						FROM pg_class c WHERE c.relnamespace = 'public'::regnamespace""");
				assertThat(relationOwners).as("pg_class rows in public").hasSizeGreaterThanOrEqualTo(4);
				assertThat(relationOwners).as("ledger_api_owner: owner of every pg_class row in public")
					.allSatisfy((relation, owner) -> assertThat(owner).as("owner of %s", relation)
						.isEqualTo(Role.OWNER.dbRole));

				Map<String, String> functionOwners = queryPairs(conn, """
						SELECT p.proname, pg_get_userbyid(p.proowner)
						FROM pg_proc p WHERE p.pronamespace = 'public'::regnamespace""");
				assertThat(functionOwners).as("functions in public").hasSizeGreaterThanOrEqualTo(3);
				assertThat(functionOwners).as("ledger_api_owner: owner of every function in public")
					.allSatisfy((function, owner) -> assertThat(owner).as("owner of %s", function)
						.isEqualTo(Role.OWNER.dbRole));

				for (Role role : RUNTIME_ROLES) {
					assertThat(queryLong(conn,
							"SELECT count(*) FROM pg_class WHERE relowner = (SELECT oid FROM pg_roles WHERE rolname = ?)",
							role.dbRole))
						.as("%s: owned pg_class rows", role.dbRole)
						.isZero();
					assertThat(queryLong(conn,
							"SELECT count(*) FROM pg_proc WHERE proowner = (SELECT oid FROM pg_roles WHERE rolname = ?)",
							role.dbRole))
						.as("%s: owned pg_proc rows", role.dbRole)
						.isZero();
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

	// D
	@Test
	void tablePrivilegeMatrix() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				MatrixDiff diff = tableMatrixDiff(conn);
				assertThat(diff.missingFromMatrix()).as("tables in public missing from TABLE_MATRIX").isEmpty();
				assertThat(diff.staleInMatrix()).as("TABLE_MATRIX entries with no table").isEmpty();

				for (Map.Entry<String, Map<Role, Set<String>>> table : TABLE_MATRIX.entrySet()) {
					for (Role role : RUNTIME_ROLES) {
						Set<String> granted = table.getValue().get(role);
						for (String privilege : TABLE_PRIVILEGES) {
							assertThat(queryBoolean(conn, """
									SELECT has_table_privilege(?::name, c.oid, ?::text) FROM pg_class c
									WHERE c.relnamespace = 'public'::regnamespace AND c.relname = ?""", role.dbRole,
									privilege, table.getKey()))
								.as("%s: %s on %s", role.dbRole, privilege, table.getKey())
								.isEqualTo(granted.contains(privilege));
						}
					}
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

	// E1
	@Test
	void accountsColumnPrivileges() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				List<String> columns = queryStrings(conn, """
						SELECT attname FROM pg_attribute
						WHERE attrelid = 'public.accounts'::regclass AND attnum > 0 AND NOT attisdropped""");
				assertThat(columns).as("accounts columns").containsExactlyInAnyOrderElementsOf(ACCOUNTS_COLUMNS);

				for (Role role : RUNTIME_ROLES) {
					for (String column : ACCOUNTS_COLUMNS) {
						for (String privilege : List.of("SELECT", "INSERT", "UPDATE")) {
							boolean expected = privilege.equals("SELECT")
									|| ACCOUNTS_COLUMN_GRANTS.get(role).get(privilege).contains(column);
							assertThat(queryBoolean(conn,
									"SELECT has_column_privilege(?::name, 'public.accounts'::regclass, ?::text, ?::text)",
									role.dbRole, column, privilege))
								.as("%s: %s on accounts.%s", role.dbRole, privilege, column)
								.isEqualTo(expected);
						}
					}
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

	// E2
	@Test
	void columnGrantsExistOnlyOnAccounts() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				List<String> columnsWithAcl = queryStrings(conn, """
						SELECT c.relname || '.' || a.attname
						FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
						WHERE c.relnamespace = 'public'::regnamespace AND a.attnum > 0 AND a.attacl IS NOT NULL""");
				assertThat(columnsWithAcl).as("ledger_api_app/settlement_worker: columns with column-level grants")
					.containsExactlyInAnyOrder("accounts.name", "accounts.type", "accounts.allow_negative",
							"accounts.balance");
			}
			finally {
				conn.rollback();
			}
		}
	}

	// F: identity inserts need no sequence privileges.
	@Test
	void runtimeRolesHaveNoSequencePrivileges() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				List<String> sequences = queryStrings(conn,
						"SELECT relname FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relkind = 'S'");
				assertThat(sequences).as("sequences in public").hasSizeGreaterThanOrEqualTo(3);
				for (String sequence : sequences) {
					for (Role role : RUNTIME_ROLES) {
						for (String privilege : List.of("USAGE", "SELECT", "UPDATE")) {
							assertThat(queryBoolean(conn, """
									SELECT has_sequence_privilege(?::name, c.oid, ?::text) FROM pg_class c
									WHERE c.relnamespace = 'public'::regnamespace AND c.relname = ?""", role.dbRole,
									privilege, sequence))
								.as("%s: %s on sequence %s", role.dbRole, privilege, sequence)
								.isFalse();
						}
					}
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

	// G
	@Test
	void schemaAndDatabasePrivileges() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				for (Role role : RUNTIME_ROLES) {
					assertThat(hasSchemaPrivilege(conn, role, "USAGE")).as("%s: USAGE on schema public", role.dbRole)
						.isTrue();
					assertThat(hasSchemaPrivilege(conn, role, "CREATE")).as("%s: CREATE on schema public", role.dbRole)
						.isFalse();
				}
				assertThat(hasSchemaPrivilege(conn, Role.OWNER, "CREATE"))
					.as("ledger_api_owner: CREATE on schema public")
					.isTrue();

				for (Role role : Role.values()) {
					for (String privilege : List.of("TEMP", "CREATE")) {
						assertThat(queryBoolean(conn,
								"SELECT has_database_privilege(?::name, current_database(), ?::text)", role.dbRole,
								privilege))
							.as("%s: %s on database", role.dbRole, privilege)
							.isFalse();
					}
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

	// H
	@Test
	void triggerFunctionsAndTriggersAreHardened() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				assertTriggerHardening(conn, "initial");
			}
			finally {
				conn.rollback();
			}
		}
	}

	static Stream<Arguments> bypassAttempts() {
		return RUNTIME_ROLES.stream()
			.flatMap(role -> Arrays.stream(Bypass.values()).map(bypass -> Arguments.of(role, bypass)));
	}

	// I
	@ParameterizedTest(name = "{0} {1}")
	@MethodSource("bypassAttempts")
	void runtimeRolesCannotBypass(Role role, Bypass bypass) throws SQLException {
		SQLException e;
		try (Connection conn = connect(role)) {
			try {
				e = catchThrowableOfType(SQLException.class, () -> {
					try (Statement st = conn.createStatement()) {
						st.execute(bypass.sql);
					}
				});
			}
			finally {
				conn.rollback();
			}
		}

		assertThat((Throwable) e).as("%s: %s must be rejected by the statement itself", role.dbRole, bypass)
			.isNotNull();
		assertRootError(e, role, INSUFFICIENT_PRIVILEGE, bypass.message);

		try (Connection conn = connect(Role.APP)) {
			try {
				setLocalLockTimeout(conn);
				assertTriggerHardening(conn, role.dbRole + " after " + bypass);
			}
			finally {
				conn.rollback();
			}
		}
	}

	// J: negative control for D's enumeration. An ungranted table created by the owner
	// (uncommitted, visible on its own connection) must be reported as missing.
	@Test
	void matrixEnumerationCatchesUngrantedTable() throws SQLException {
		String table = "t7_ungranted_" + UUID.randomUUID().toString().replace("-", "");
		try (Connection conn = connect(Role.OWNER)) {
			try {
				try (Statement st = conn.createStatement()) {
					st.execute("CREATE TABLE public." + table + " (id int)");
				}
				MatrixDiff diff = tableMatrixDiff(conn);
				assertThat(diff.missingFromMatrix()).as("ledger_api_owner: tables missing from TABLE_MATRIX")
					.containsExactly(table);
				assertThat(diff.staleInMatrix()).as("ledger_api_owner: TABLE_MATRIX entries with no table").isEmpty();
			}
			finally {
				conn.rollback();
			}
		}
	}

	// Tables (relkind r/p) in public from pg_class: information_schema would hide tables the
	// querying role has no privilege on.
	private static MatrixDiff tableMatrixDiff(Connection conn) throws SQLException {
		Set<String> tables = new TreeSet<>(queryStrings(conn,
				"SELECT relname FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relkind IN ('r', 'p')"));
		Set<String> missing = new TreeSet<>(tables);
		missing.removeAll(TABLE_MATRIX.keySet());
		Set<String> stale = new TreeSet<>(TABLE_MATRIX.keySet());
		stale.removeAll(tables);
		return new MatrixDiff(missing, stale);
	}

	private static void assertTriggerHardening(Connection conn, String context) throws SQLException {
		Map<String, Map<String, Object>> functions = new HashMap<>();
		try (PreparedStatement ps = conn.prepareStatement("""
				SELECT p.proname, p.prosecdef, p.proconfig FROM pg_proc p
				WHERE p.pronamespace = 'public'::regnamespace AND p.proname = ANY (?)""")) {
			ps.setArray(1, conn.createArrayOf("text", new String[] { "assert_transaction_balanced",
					"assert_transaction_has_entries", "forbid_mutation" }));
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					Array config = rs.getArray("proconfig");
					functions.put(rs.getString("proname"), Map.of("prosecdef", rs.getBoolean("prosecdef"), "proconfig",
							config == null ? List.of() : List.of((String[]) config.getArray())));
				}
			}
		}
		assertThat(functions).as("%s: hardened trigger functions", context)
			.containsOnlyKeys("assert_transaction_balanced", "assert_transaction_has_entries", "forbid_mutation");
		functions.forEach((name, attributes) -> {
			assertThat(attributes.get("prosecdef")).as("%s: %s prosecdef", context, name).isEqualTo(false);
			assertThat(attributes.get("proconfig")).as("%s: %s proconfig", context, name)
				.isEqualTo(List.of(SEARCH_PATH));
		});

		Map<String, Map<String, Object>> triggers = new LinkedHashMap<>();
		try (PreparedStatement ps = conn.prepareStatement("""
				SELECT t.tgname, c.relname, t.tgenabled, t.tgdeferrable, t.tginitdeferred, p.proname,
				       p.pronamespace = 'public'::regnamespace AS function_in_public
				FROM pg_trigger t
				JOIN pg_class c ON c.oid = t.tgrelid
				JOIN pg_proc p ON p.oid = t.tgfoid
				WHERE c.relnamespace = 'public'::regnamespace AND NOT t.tgisinternal""");
				ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				triggers.put(rs.getString("tgname"),
						Map.of("table", rs.getString("relname"), "enabled", rs.getString("tgenabled"), "deferrable",
								rs.getBoolean("tgdeferrable"), "initdeferred", rs.getBoolean("tginitdeferred"),
								"function", rs.getString("proname"), "functionInPublic",
								rs.getBoolean("function_in_public")));
			}
		}
		assertThat(triggers).as("%s: triggers in public", context).containsKeys(TRIGGERS.keySet().toArray(String[]::new));
		TRIGGERS.forEach((name, spec) -> {
			Map<String, Object> actual = triggers.get(name);
			assertThat(actual.get("table")).as("%s: %s table", context, name).isEqualTo(spec.table());
			assertThat(actual.get("enabled")).as("%s: %s tgenabled", context, name).isEqualTo("O");
			assertThat(actual.get("function")).as("%s: %s tgfoid", context, name).isEqualTo(spec.function());
			assertThat(actual.get("functionInPublic")).as("%s: %s function schema", context, name).isEqualTo(true);
			if (spec.deferred()) {
				assertThat(actual.get("deferrable")).as("%s: %s tgdeferrable", context, name).isEqualTo(true);
				assertThat(actual.get("initdeferred")).as("%s: %s tginitdeferred", context, name).isEqualTo(true);
			}
		});
	}

	private static boolean hasSchemaPrivilege(Connection conn, Role role, String privilege) throws SQLException {
		return queryBoolean(conn, "SELECT has_schema_privilege(?::name, 'public', ?::text)", role.dbRole, privilege);
	}

	private static void setLocalLockTimeout(Connection conn) throws SQLException {
		try (Statement st = conn.createStatement()) {
			st.execute("SET LOCAL lock_timeout = '5s'");
		}
	}

	private static String[] allRoleNames() {
		return Arrays.stream(Role.values()).map(role -> role.dbRole).toArray(String[]::new);
	}

	// Connection as the given role, autoCommit off; asserts the session really is that role.
	private Connection connect(Role role) throws SQLException {
		Connection conn = switch (role) {
			case APP -> dataSource.getConnection();
			case WORKER -> DriverManager.getConnection(postgres.getJdbcUrl(), role.dbRole,
					TestcontainersConfiguration.WORKER_PASSWORD);
			case OWNER -> DriverManager.getConnection(postgres.getJdbcUrl(), role.dbRole,
					TestcontainersConfiguration.OWNER_PASSWORD);
		};
		try (PreparedStatement ps = conn.prepareStatement("SELECT current_user");
				ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).isTrue();
			assertThat(rs.getString(1)).as("current_user").isEqualTo(role.dbRole);
			conn.setAutoCommit(false);
			return conn;
		}
		catch (SQLException | RuntimeException | Error e) {
			conn.close();
			throw e;
		}
	}

	private static boolean queryBoolean(Connection conn, String sql, String... params) throws SQLException {
		try (PreparedStatement ps = prepare(conn, sql, params); ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).as("row for %s", sql).isTrue();
			return rs.getBoolean(1);
		}
	}

	private static long queryLong(Connection conn, String sql, String... params) throws SQLException {
		try (PreparedStatement ps = prepare(conn, sql, params); ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).as("row for %s", sql).isTrue();
			return rs.getLong(1);
		}
	}

	private static List<String> queryStrings(Connection conn, String sql) throws SQLException {
		List<String> values = new ArrayList<>();
		try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				values.add(rs.getString(1));
			}
		}
		return values;
	}

	private static Map<String, String> queryPairs(Connection conn, String sql) throws SQLException {
		Map<String, String> pairs = new LinkedHashMap<>();
		try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				pairs.put(rs.getString(1), rs.getString(2));
			}
		}
		return pairs;
	}

	private static PreparedStatement prepare(Connection conn, String sql, String... params) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(sql);
		for (int i = 0; i < params.length; i++) {
			ps.setString(i + 1, params[i]);
		}
		return ps;
	}

	// SQLState and server message of the deepest SQLException in the cause chain.
	private static void assertRootError(SQLException thrown, Role role, String sqlState, String message) {
		SQLException root = thrown;
		for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
			if (cause instanceof SQLException sql) {
				root = sql;
			}
		}
		assertThat(root.getSQLState()).as("%s: SQLState of %s", role.dbRole, root).isEqualTo(sqlState);
		assertThat((Throwable) root).as("%s: driver exception type", role.dbRole).isInstanceOf(PSQLException.class);
		assertThat(((PSQLException) root).getServerErrorMessage().getMessage()).as("%s: server message", role.dbRole)
			.isEqualTo(message);
	}

}
