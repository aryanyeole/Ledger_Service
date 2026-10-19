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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
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

// Proof that the database roles match the declared model (D-006, D-033, D-034, D-040,
// D-043, D-060, D-071, D-076, D-077, D-078):
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
	// INSERT/UPDATE on accounts, idempotency_keys, transactions and entries are column-level
	// grants (COLUMN_GRANTS), so they are false at table level. Every table in public must have
	// a row here. Runtime roles can't set ledger created_at (D-071, V8).
	// settlement_worker gets nothing on idempotency_keys (D-020, D-043).
	// payments: INSERT (app) and UPDATE (worker) are column-level too (D-076, V9).
	// outbox: the worker can insert but not read (D-077, V11). processed_messages: the worker's
	// SELECT and INSERT are column-level, the app gets nothing (D-078, V12).
	private static final Map<String, Map<Role, Set<String>>> TABLE_MATRIX = Map.of(
			"accounts", Map.of(Role.APP, Set.of("SELECT"), Role.WORKER, Set.of("SELECT")),
			"transactions", Map.of(Role.APP, Set.of("SELECT"), Role.WORKER, Set.of("SELECT")),
			"entries", Map.of(Role.APP, Set.of("SELECT"), Role.WORKER, Set.of("SELECT")),
			"idempotency_keys", Map.of(Role.APP, Set.of("SELECT"), Role.WORKER, Set.of()),
			"payments", Map.of(Role.APP, Set.of("SELECT"), Role.WORKER, Set.of("SELECT")),
			"outbox", Map.of(Role.APP, Set.of("SELECT"), Role.WORKER, Set.of()),
			"processed_messages", Map.of(Role.APP, Set.of(), Role.WORKER, Set.of()),
			"flyway_schema_history", Map.of(Role.APP, Set.of(), Role.WORKER, Set.of()));

	// Every column of each table that carries column-level grants.
	private static final Map<String, List<String>> TABLE_COLUMNS = Map.of(
			"accounts", List.of("id", "name", "type", "currency", "balance", "allow_negative", "created_at"),
			"idempotency_keys", List.of("client_id", "key", "request_hash", "status_code", "response_body",
					"created_at", "expires_at"),
			"transactions", List.of("id", "kind", "reference", "correlation_id", "created_at"),
			"entries", List.of("id", "transaction_id", "account_id", "amount", "created_at"),
			"payments", List.of("id", "customer_account_id", "merchant_account_id", "amount", "fee", "status",
					"created_at", "updated_at"),
			"outbox", List.of("id", "event_id", "topic", "message_key", "event_type", "payload", "headers",
					"created_at", "published_at"),
			"processed_messages", List.of("consumer_group", "event_id", "processed_at"));

	// Expected column ACLs: table -> role -> privilege -> columns, beyond table-level grants.
	// No other column in public may carry a column-level grant (columnGrantsMatchMap).
	private static final Map<String, Map<Role, Map<String, Set<String>>>> COLUMN_GRANTS = Map.of(
			"accounts", Map.of(
					Role.APP, Map.of("INSERT", Set.of("name", "type", "allow_negative"), "UPDATE", Set.of("balance")),
					Role.WORKER, Map.of("INSERT", Set.of(), "UPDATE", Set.of("balance"))),
			"idempotency_keys", Map.of(
					Role.APP, Map.of("INSERT", Set.of("client_id", "key", "request_hash"), "UPDATE",
							Set.of("status_code", "response_body")),
					Role.WORKER, Map.of("INSERT", Set.of(), "UPDATE", Set.of())),
			"transactions", Map.of(
					Role.APP, Map.of("INSERT", Set.of("kind", "reference", "correlation_id"), "UPDATE", Set.of()),
					Role.WORKER, Map.of("INSERT", Set.of("kind", "reference", "correlation_id"), "UPDATE", Set.of())),
			"entries", Map.of(
					Role.APP, Map.of("INSERT", Set.of("transaction_id", "account_id", "amount"), "UPDATE", Set.of()),
					Role.WORKER, Map.of("INSERT", Set.of("transaction_id", "account_id", "amount"), "UPDATE", Set.of())),
			"payments", Map.of(
					Role.APP, Map.of("INSERT", Set.of("customer_account_id", "merchant_account_id", "amount", "fee"),
							"UPDATE", Set.of()),
					Role.WORKER, Map.of("INSERT", Set.of(), "UPDATE", Set.of("status"))),
			"outbox", Map.of(
					Role.APP, Map.of("INSERT", Set.of("topic", "message_key", "event_type", "payload", "headers"),
							"UPDATE", Set.of("published_at")),
					Role.WORKER, Map.of("INSERT", Set.of("topic", "message_key", "event_type", "payload", "headers"),
							"UPDATE", Set.of())),
			// SELECT is column-level here: ON CONFLICT (consumer_group, event_id) needs it (D-078).
			"processed_messages", Map.of(
					Role.APP, Map.of("SELECT", Set.of(), "INSERT", Set.of(), "UPDATE", Set.of()),
					Role.WORKER, Map.of("SELECT", Set.of("consumer_group", "event_id"), "INSERT",
							Set.of("consumer_group", "event_id"), "UPDATE", Set.of())));

	record TriggerSpec(String table, String function, boolean deferred) {
	}

	// Every non-internal trigger in public, exactly (D-060). Their functions are the exact set
	// of trigger functions allowed in public, and each must be hardened (D-040).
	private static final Map<String, TriggerSpec> TRIGGERS = Map.of(
			"entries_balanced", new TriggerSpec("entries", "assert_transaction_balanced", true),
			"transactions_have_entries", new TriggerSpec("transactions", "assert_transaction_has_entries", true),
			"entries_append_only", new TriggerSpec("entries", "forbid_mutation", false),
			"transactions_append_only", new TriggerSpec("transactions", "forbid_mutation", false),
			"idempotency_keys_complete",
			new TriggerSpec("idempotency_keys", "assert_idempotency_key_complete", true),
			"payments_status_transition", new TriggerSpec("payments", "payments_status_transition", false),
			"payments_no_delete", new TriggerSpec("payments", "forbid_mutation", false),
			"outbox_publish_once", new TriggerSpec("outbox", "outbox_publish_once", false),
			"processed_messages_append_only", new TriggerSpec("processed_messages", "forbid_mutation", false));

	private static final Set<String> TRIGGER_FUNCTIONS = TRIGGERS.values()
		.stream()
		.map(TriggerSpec::function)
		.collect(Collectors.toUnmodifiableSet());

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
		CREATE_SCHEMA("CREATE SCHEMA t7_shadow", "permission denied for database ledger"),

		// idempotency_keys (D-043): role-specific; 42501 is the check, the message only has to
		// name the table. WHERE false still runs the privilege check, so no row is needed.
		WORKER_SELECT_KEYS(Role.WORKER, "SELECT 1 FROM idempotency_keys"),
		WORKER_INSERT_KEY(Role.WORKER,
				"INSERT INTO idempotency_keys (client_id, key, request_hash) VALUES ('t3', 'probe', decode(repeat('00', 32), 'hex'))"),
		APP_INSERT_EXPIRES_AT(Role.APP,
				"INSERT INTO idempotency_keys (client_id, key, request_hash, expires_at) VALUES ('t3', 'probe', decode(repeat('00', 32), 'hex'), now() + interval '30 days')"),
		APP_UPDATE_REQUEST_HASH(Role.APP, "UPDATE idempotency_keys SET request_hash = request_hash WHERE false"),
		APP_UPDATE_EXPIRES_AT(Role.APP, "UPDATE idempotency_keys SET expires_at = expires_at WHERE false"),
		APP_UPDATE_CREATED_AT(Role.APP, "UPDATE idempotency_keys SET created_at = created_at WHERE false"),
		APP_UPDATE_KEY(Role.APP, "UPDATE idempotency_keys SET key = key WHERE false"),
		APP_UPDATE_CLIENT_ID(Role.APP, "UPDATE idempotency_keys SET client_id = client_id WHERE false"),
		APP_DELETE_KEYS(Role.APP, "DELETE FROM idempotency_keys WHERE false"),
		APP_TRUNCATE_KEYS(Role.APP, "TRUNCATE idempotency_keys"),

		// Ledger timestamps (D-071): neither runtime role may set created_at. The entry insert
		// gets a real transaction id from a CTE through the allowed columns, so without V8 it
		// would succeed. The harness rolls back, so nothing (not even an unbalanced row) remains.
		INSERT_TRANSACTION_CREATED_AT(Set.of(Role.APP, Role.WORKER),
				"INSERT INTO transactions (kind, created_at) VALUES ('DEPOSIT', now() - interval '1 day')", "transactions"),
		INSERT_ENTRY_CREATED_AT(Set.of(Role.APP, Role.WORKER),
				"WITH t AS (INSERT INTO transactions (kind) VALUES ('DEPOSIT') RETURNING id) "
						+ "INSERT INTO entries (transaction_id, account_id, amount, created_at) "
						+ "SELECT t.id, (SELECT id FROM accounts WHERE name = 'system:external'), 1, now() - interval '1 day' FROM t",
				"entries"),

		// payments (D-076): the app inserts only the parties and the money and never updates;
		// the worker only updates status; neither deletes. The privilege check runs before the
		// row is built, so the account ids need not exist (WHERE false for UPDATE/DELETE).
		APP_INSERT_PAYMENT_ID(Set.of(Role.APP),
				"INSERT INTO payments (id, customer_account_id, merchant_account_id, amount, fee) VALUES (gen_random_uuid(), 1, 2, 1, 0)",
				"payments"),
		APP_INSERT_PAYMENT_STATUS(Set.of(Role.APP),
				"INSERT INTO payments (customer_account_id, merchant_account_id, amount, fee, status) VALUES (1, 2, 1, 0, 'SETTLED')",
				"payments"),
		APP_INSERT_PAYMENT_CREATED_AT(Set.of(Role.APP),
				"INSERT INTO payments (customer_account_id, merchant_account_id, amount, fee, created_at) VALUES (1, 2, 1, 0, now() - interval '1 day')",
				"payments"),
		APP_INSERT_PAYMENT_UPDATED_AT(Set.of(Role.APP),
				"INSERT INTO payments (customer_account_id, merchant_account_id, amount, fee, updated_at) VALUES (1, 2, 1, 0, now() - interval '1 day')",
				"payments"),
		APP_UPDATE_PAYMENT_STATUS(Set.of(Role.APP), "UPDATE payments SET status = 'SETTLED' WHERE false", "payments"),
		WORKER_INSERT_PAYMENT(Set.of(Role.WORKER),
				"INSERT INTO payments (customer_account_id, merchant_account_id, amount, fee) VALUES (1, 2, 1, 0)",
				"payments"),
		WORKER_UPDATE_PAYMENT_ID(Set.of(Role.WORKER), "UPDATE payments SET id = id WHERE false", "payments"),
		WORKER_UPDATE_PAYMENT_CUSTOMER(Set.of(Role.WORKER),
				"UPDATE payments SET customer_account_id = customer_account_id WHERE false", "payments"),
		WORKER_UPDATE_PAYMENT_MERCHANT(Set.of(Role.WORKER),
				"UPDATE payments SET merchant_account_id = merchant_account_id WHERE false", "payments"),
		WORKER_UPDATE_PAYMENT_AMOUNT(Set.of(Role.WORKER), "UPDATE payments SET amount = amount WHERE false",
				"payments"),
		WORKER_UPDATE_PAYMENT_FEE(Set.of(Role.WORKER), "UPDATE payments SET fee = fee WHERE false", "payments"),
		WORKER_UPDATE_PAYMENT_CREATED_AT(Set.of(Role.WORKER), "UPDATE payments SET created_at = created_at WHERE false",
				"payments"),
		WORKER_UPDATE_PAYMENT_UPDATED_AT(Set.of(Role.WORKER), "UPDATE payments SET updated_at = now() WHERE false",
				"payments"),
		DELETE_PAYMENTS(Set.of(Role.APP, Role.WORKER), "DELETE FROM payments WHERE false", "payments"),
		TRUNCATE_PAYMENTS(Set.of(Role.APP, Role.WORKER), "TRUNCATE payments", "payments"),

		// outbox (D-077): both writers insert only the event's content; only the app reads it and
		// only published_at is updatable. GENERATED ALWAYS id is checked before privileges
		// (428C9), so the id rows use OVERRIDING SYSTEM VALUE / SET id = DEFAULT to reach the
		// privilege check, as a real attempt would.
		INSERT_OUTBOX_ID(Set.of(Role.APP, Role.WORKER),
				"INSERT INTO outbox (id, topic, message_key, event_type, payload) OVERRIDING SYSTEM VALUE VALUES (1, 't', 'k', 'payment.authorized', '{}')",
				"outbox"),
		INSERT_OUTBOX_EVENT_ID(Set.of(Role.APP, Role.WORKER),
				"INSERT INTO outbox (event_id, topic, message_key, event_type, payload) VALUES (gen_random_uuid(), 't', 'k', 'payment.authorized', '{}')",
				"outbox"),
		INSERT_OUTBOX_CREATED_AT(Set.of(Role.APP, Role.WORKER),
				"INSERT INTO outbox (topic, message_key, event_type, payload, created_at) VALUES ('t', 'k', 'payment.authorized', '{}', now() - interval '1 day')",
				"outbox"),
		INSERT_OUTBOX_PUBLISHED_AT(Set.of(Role.APP, Role.WORKER),
				"INSERT INTO outbox (topic, message_key, event_type, payload, published_at) VALUES ('t', 'k', 'payment.authorized', '{}', now())",
				"outbox"),
		APP_UPDATE_OUTBOX_ID(Set.of(Role.APP), "UPDATE outbox SET id = DEFAULT WHERE false", "outbox"),
		APP_UPDATE_OUTBOX_EVENT_ID(Set.of(Role.APP), "UPDATE outbox SET event_id = event_id WHERE false", "outbox"),
		APP_UPDATE_OUTBOX_TOPIC(Set.of(Role.APP), "UPDATE outbox SET topic = topic WHERE false", "outbox"),
		APP_UPDATE_OUTBOX_MESSAGE_KEY(Set.of(Role.APP), "UPDATE outbox SET message_key = message_key WHERE false",
				"outbox"),
		APP_UPDATE_OUTBOX_EVENT_TYPE(Set.of(Role.APP), "UPDATE outbox SET event_type = event_type WHERE false",
				"outbox"),
		APP_UPDATE_OUTBOX_PAYLOAD(Set.of(Role.APP), "UPDATE outbox SET payload = payload WHERE false", "outbox"),
		APP_UPDATE_OUTBOX_HEADERS(Set.of(Role.APP), "UPDATE outbox SET headers = headers WHERE false", "outbox"),
		APP_UPDATE_OUTBOX_CREATED_AT(Set.of(Role.APP), "UPDATE outbox SET created_at = created_at WHERE false",
				"outbox"),
		WORKER_SELECT_OUTBOX(Set.of(Role.WORKER), "SELECT 1 FROM outbox", "outbox"),
		WORKER_UPDATE_OUTBOX_PUBLISHED_AT(Set.of(Role.WORKER), "UPDATE outbox SET published_at = now() WHERE false",
				"outbox"),
		DELETE_OUTBOX(Set.of(Role.APP, Role.WORKER), "DELETE FROM outbox WHERE false", "outbox"),
		TRUNCATE_OUTBOX(Set.of(Role.APP, Role.WORKER), "TRUNCATE outbox", "outbox"),

		// processed_messages (D-078): the worker reads and inserts only (consumer_group, event_id);
		// the app gets nothing.
		APP_SELECT_PROCESSED(Set.of(Role.APP), "SELECT 1 FROM processed_messages", "processed_messages"),
		APP_INSERT_PROCESSED(Set.of(Role.APP),
				"INSERT INTO processed_messages (consumer_group, event_id) VALUES ('g', gen_random_uuid())",
				"processed_messages"),
		WORKER_SELECT_PROCESSED_AT(Set.of(Role.WORKER), "SELECT processed_at FROM processed_messages",
				"processed_messages"),
		WORKER_SELECT_PROCESSED_ALL(Set.of(Role.WORKER), "SELECT * FROM processed_messages", "processed_messages"),
		WORKER_INSERT_PROCESSED_AT(Set.of(Role.WORKER),
				"INSERT INTO processed_messages (consumer_group, event_id, processed_at) VALUES ('g', gen_random_uuid(), now() - interval '1 day')",
				"processed_messages"),
		UPDATE_PROCESSED(Set.of(Role.APP, Role.WORKER), "UPDATE processed_messages SET event_id = event_id WHERE false",
				"processed_messages"),
		DELETE_PROCESSED(Set.of(Role.APP, Role.WORKER), "DELETE FROM processed_messages WHERE false",
				"processed_messages"),
		TRUNCATE_PROCESSED(Set.of(Role.APP, Role.WORKER), "TRUNCATE processed_messages", "processed_messages");

		final Set<Role> roles;

		final String sql;

		final String message;

		// true: the server message must equal `message`; false: it must contain it.
		final boolean exactMessage;

		// Rejected for both runtime roles with exactly this server message.
		Bypass(String sql, String message) {
			this.roles = Set.of(Role.APP, Role.WORKER);
			this.sql = sql;
			this.message = message;
			this.exactMessage = true;
		}

		// Rejected for one role; the server message must name idempotency_keys.
		Bypass(Role role, String sql) {
			this(Set.of(role), sql, "idempotency_keys");
		}

		// Rejected for these roles; 42501 is the check, the server message must name the table.
		Bypass(Set<Role> roles, String sql, String table) {
			this.roles = roles;
			this.sql = sql;
			this.message = table;
			this.exactMessage = false;
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

	// E1: every column of every table in COLUMN_GRANTS. A column privilege holds if the
	// table-level grant (TABLE_MATRIX) or the column grant (COLUMN_GRANTS) gives it.
	@Test
	void columnPrivileges() throws SQLException {
		try (Connection conn = connect(Role.APP)) {
			try {
				for (Map.Entry<String, Map<Role, Map<String, Set<String>>>> table : COLUMN_GRANTS.entrySet()) {
					String name = table.getKey();
					List<String> columns = queryStrings(conn, "SELECT attname FROM pg_attribute WHERE attrelid = 'public."
							+ name + "'::regclass AND attnum > 0 AND NOT attisdropped");
					assertThat(columns).as("%s columns", name)
						.containsExactlyInAnyOrderElementsOf(TABLE_COLUMNS.get(name));

					for (Role role : RUNTIME_ROLES) {
						for (String column : TABLE_COLUMNS.get(name)) {
							for (String privilege : List.of("SELECT", "INSERT", "UPDATE")) {
								boolean expected = TABLE_MATRIX.get(name).get(role).contains(privilege)
										|| table.getValue().get(role).getOrDefault(privilege, Set.of()).contains(column);
								assertThat(queryBoolean(conn,
										"SELECT has_column_privilege(?::name, ?::regclass, ?::text, ?::text)",
										role.dbRole, "public." + name, column, privilege))
									.as("%s: %s on %s.%s", role.dbRole, privilege, name, column)
									.isEqualTo(expected);
							}
						}
					}
				}
			}
			finally {
				conn.rollback();
			}
		}
	}

	// E2: the columns carrying any column-level ACL in public are exactly those in COLUMN_GRANTS.
	@Test
	void columnGrantsMatchMap() throws SQLException {
		Set<String> expected = new TreeSet<>();
		COLUMN_GRANTS.forEach((table, byRole) -> byRole.values()
			.forEach((byPrivilege) -> byPrivilege.values()
				.forEach((columns) -> columns.forEach((column) -> expected.add(table + "." + column)))));
		try (Connection conn = connect(Role.APP)) {
			try {
				List<String> columnsWithAcl = queryStrings(conn, """
						SELECT c.relname || '.' || a.attname
						FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
						WHERE c.relnamespace = 'public'::regnamespace AND a.attnum > 0 AND a.attacl IS NOT NULL""");
				assertThat(columnsWithAcl).as("ledger_api_app/settlement_worker: columns with column-level grants")
					.containsExactlyInAnyOrderElementsOf(expected);
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

	// H2: negative control for H's exact sets (D-060). As the owner, in a transaction that is
	// rolled back, add a trigger function without a pinned search_path and a trigger using it:
	// the check must report both, and the function as unhardened. After the rollback the
	// normal check passes again.
	@Test
	void hardeningCheckCatchesUnlistedUnhardenedTrigger() throws SQLException {
		String suffix = UUID.randomUUID().toString().replace("-", "");
		String function = "t3_unhardened_" + suffix;
		String trigger = "t3_unhardened_trigger_" + suffix;
		try (Connection conn = connect(Role.OWNER)) {
			try {
				try (Statement st = conn.createStatement()) {
					st.execute("CREATE FUNCTION public." + function
							+ "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$");
					st.execute("CREATE TRIGGER " + trigger
							+ " AFTER INSERT ON public.idempotency_keys FOR EACH ROW EXECUTE FUNCTION public." + function
							+ "()");
				}
				HardeningReport report = hardeningReport(conn);
				assertThat(report.unlistedFunctions()).as("ledger_api_owner: unlisted trigger functions")
					.containsExactly(function);
				assertThat(report.unhardenedFunctions()).as("ledger_api_owner: unhardened trigger functions")
					.containsExactly(function);
				assertThat(report.unlistedTriggers()).as("ledger_api_owner: unlisted triggers").containsExactly(trigger);
				assertThat(report.missingFunctions()).as("ledger_api_owner: missing trigger functions").isEmpty();
				assertThat(report.missingTriggers()).as("ledger_api_owner: missing triggers").isEmpty();
				assertThat(report.triggerMismatches()).as("ledger_api_owner: trigger mismatches").isEmpty();
				assertThat(report.clean()).as("ledger_api_owner: report clean").isFalse();
			}
			finally {
				conn.rollback();
			}
		}
		try (Connection conn = connect(Role.APP)) {
			try {
				assertTriggerHardening(conn, "after rolled-back negative control");
			}
			finally {
				conn.rollback();
			}
		}
	}

	static Stream<Arguments> bypassAttempts() {
		return RUNTIME_ROLES.stream()
			.flatMap(role -> Arrays.stream(Bypass.values())
				.filter(bypass -> bypass.roles.contains(role))
				.map(bypass -> Arguments.of(role, bypass)));
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
		if (bypass.exactMessage) {
			assertRootError(e, role, INSUFFICIENT_PRIVILEGE, bypass.message);
		}
		else {
			assertRootErrorContains(e, role, INSUFFICIENT_PRIVILEGE, bypass.message);
		}

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

	// What the exact-set hardening check found (D-060). Empty lists everywhere = hardened.
	record HardeningReport(Set<String> unlistedFunctions, Set<String> missingFunctions,
			Set<String> unhardenedFunctions, Set<String> unlistedTriggers, Set<String> missingTriggers,
			List<String> triggerMismatches) {

		boolean clean() {
			return unlistedFunctions.isEmpty() && missingFunctions.isEmpty() && unhardenedFunctions.isEmpty()
					&& unlistedTriggers.isEmpty() && missingTriggers.isEmpty() && triggerMismatches.isEmpty();
		}

	}

	private static void assertTriggerHardening(Connection conn, String context) throws SQLException {
		HardeningReport report = hardeningReport(conn);
		assertThat(report.unlistedFunctions()).as("%s: trigger functions in public not in TRIGGERS", context).isEmpty();
		assertThat(report.missingFunctions()).as("%s: TRIGGERS functions missing from public", context).isEmpty();
		assertThat(report.unhardenedFunctions()).as("%s: trigger functions not hardened", context).isEmpty();
		assertThat(report.unlistedTriggers()).as("%s: triggers in public not in TRIGGERS", context).isEmpty();
		assertThat(report.missingTriggers()).as("%s: TRIGGERS entries missing from public", context).isEmpty();
		assertThat(report.triggerMismatches()).as("%s: triggers differing from TRIGGERS", context).isEmpty();
	}

	// Exact sets (D-060): every function in public returning trigger, and every non-internal
	// trigger on a table in public, compared with TRIGGERS. Hardening (D-040: not SECURITY
	// DEFINER, search_path pinned) is checked for every trigger function found, listed or not.
	private static HardeningReport hardeningReport(Connection conn) throws SQLException {
		Set<String> unhardened = new TreeSet<>();
		Set<String> functions = new TreeSet<>();
		try (PreparedStatement ps = conn.prepareStatement("""
				SELECT p.proname, p.prosecdef, p.proconfig FROM pg_proc p
				WHERE p.pronamespace = 'public'::regnamespace AND p.prorettype = 'pg_catalog.trigger'::regtype""");
				ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				String name = rs.getString("proname");
				functions.add(name);
				Array config = rs.getArray("proconfig");
				List<String> settings = (config == null) ? List.of() : List.of((String[]) config.getArray());
				if (rs.getBoolean("prosecdef") || !settings.equals(List.of(SEARCH_PATH))) {
					unhardened.add(name);
				}
			}
		}

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

		List<String> mismatches = new ArrayList<>();
		TRIGGERS.forEach((name, spec) -> {
			Map<String, Object> actual = triggers.get(name);
			if (actual == null) {
				return;
			}
			Map<String, Object> expected = new LinkedHashMap<>();
			expected.put("table", spec.table());
			expected.put("enabled", "O");
			expected.put("function", spec.function());
			expected.put("functionInPublic", true);
			if (spec.deferred()) {
				expected.put("deferrable", true);
				expected.put("initdeferred", true);
			}
			expected.forEach((attribute, value) -> {
				if (!value.equals(actual.get(attribute))) {
					mismatches.add(name + "." + attribute + ": expected " + value + ", was " + actual.get(attribute));
				}
			});
		});

		return new HardeningReport(difference(functions, TRIGGER_FUNCTIONS), difference(TRIGGER_FUNCTIONS, functions),
				unhardened, difference(triggers.keySet(), TRIGGERS.keySet()),
				difference(TRIGGERS.keySet(), triggers.keySet()), mismatches);
	}

	private static Set<String> difference(Set<String> left, Set<String> right) {
		Set<String> result = new TreeSet<>(left);
		result.removeAll(right);
		return result;
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
		assertThat(rootServerMessage(thrown, role, sqlState)).as("%s: server message", role.dbRole).isEqualTo(message);
	}

	// SQLState is the check; the server message only has to contain `fragment`.
	private static void assertRootErrorContains(SQLException thrown, Role role, String sqlState, String fragment) {
		assertThat(rootServerMessage(thrown, role, sqlState)).as("%s: server message", role.dbRole).contains(fragment);
	}

	private static String rootServerMessage(SQLException thrown, Role role, String sqlState) {
		SQLException root = thrown;
		for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
			if (cause instanceof SQLException sql) {
				root = sql;
			}
		}
		assertThat(root.getSQLState()).as("%s: SQLState of %s", role.dbRole, root).isEqualTo(sqlState);
		assertThat((Throwable) root).as("%s: driver exception type", role.dbRole).isInstanceOf(PSQLException.class);
		return ((PSQLException) root).getServerErrorMessage().getMessage();
	}

}
