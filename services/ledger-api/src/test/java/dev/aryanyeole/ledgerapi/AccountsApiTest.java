package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import com.jayway.jsonpath.JsonPath;

import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// POST /v1/accounts and GET /v1/accounts/{id}/balance against real Postgres (D-048, D-056).
// The API runs as ledger_api_app; every direct DB check uses the same role and asserts it
// (D-036). Ledger rows written here follow invariants 1 and 4 like any posting.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = EdgeProbe.API_KEYS_PROPERTY)
@AutoConfigureMockMvc
class AccountsApiTest {

	private static final String APP = "ledger_api_app";

	@Autowired
	MockMvc mvc;

	@Autowired
	DataSource dataSource;

	record AccountRow(String type, boolean allowNegative, BigDecimal balance) {
	}

	// --- POST /v1/accounts ---

	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = { "CUSTOMER", "MERCHANT" })
	void userAccountIsCreated(String type) throws Exception {
		String name = uniqueName();
		MvcResult result = createAccount("{\"name\": \"" + name + "\", \"type\": \"" + type + "\"}")
			.andExpect(status().isCreated())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
			.andExpect(jsonPath("$.name").value(name))
			.andExpect(jsonPath("$.type").value(type))
			.andExpect(jsonPath("$.created_at").isString())
			.andReturn();
		long id = ((Number) JsonPath.read(result.getResponse().getContentAsString(), "$.account_id")).longValue();
		assertThat(result.getResponse().getHeader("Location")).as("Location").isEqualTo("/v1/accounts/" + id);
		assertThat(accountRow(id)).as("[%s] row for %s", APP, name)
			.isEqualTo(new AccountRow(type, false, new BigDecimal("0.0000")));
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = { "\"EXTERNAL\"", "\"ESCROW\"", "\"PLATFORM_FEES\"", "\"customer\"", "\"FOO\"", "\"\"",
			"null" })
	void otherTypesAreRejected(String typeJson) throws Exception {
		String name = uniqueName();
		createAccount("{\"name\": \"" + name + "\", \"type\": " + typeJson + "}").andExpect(status().isBadRequest())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_ACCOUNT_TYPE.value()));
		assertThat(countByName(name)).as("[%s] rows named %s", APP, name).isZero();
	}

	@Test
	void missingTypeIsRejected() throws Exception {
		createAccount("{\"name\": \"" + uniqueName() + "\"}").andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_ACCOUNT_TYPE.value()));
	}

	@Test
	void allowNegativeInBodyIsIgnored() throws Exception {
		String name = uniqueName();
		MvcResult result = createAccount(
				"{\"name\": \"" + name + "\", \"type\": \"CUSTOMER\", \"allow_negative\": true, \"allowNegative\": true}")
			.andExpect(status().isCreated())
			.andReturn();
		long id = ((Number) JsonPath.read(result.getResponse().getContentAsString(), "$.account_id")).longValue();
		assertThat(accountRow(id).allowNegative()).as("[%s] allow_negative for %s", APP, name).isFalse();
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = { "\"\"", "\"   \"", "null", "LONG" })
	void invalidNameIsRejected(String nameJson) throws Exception {
		String json = nameJson.equals("LONG") ? "\"" + "n".repeat(101) + "\"" : nameJson;
		createAccount("{\"name\": " + json + ", \"type\": \"CUSTOMER\"}").andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value(ProblemCode.INVALID_ACCOUNT_NAME.value()));
	}

	// Length is counted in characters (code points), not UTF-16 units.
	@Test
	void hundredCharacterNameIsAccepted() throws Exception {
		String ascii = UUID.randomUUID().toString().replace("-", "").repeat(4).substring(0, 100);
		createAccount("{\"name\": \"" + ascii + "\", \"type\": \"CUSTOMER\"}").andExpect(status().isCreated());
		String emoji = "💰".repeat(99) + UUID.randomUUID().toString().charAt(0);
		createAccount("{\"name\": \"" + emoji + "\", \"type\": \"MERCHANT\"}").andExpect(status().isCreated());
	}

	@Test
	void duplicateNameIs409() throws Exception {
		String name = uniqueName();
		createAccount("{\"name\": \"" + name + "\", \"type\": \"CUSTOMER\"}").andExpect(status().isCreated());
		createAccount("{\"name\": \"" + name + "\", \"type\": \"MERCHANT\"}").andExpect(status().isConflict())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.status").value(409))
			.andExpect(jsonPath("$.code").value(ProblemCode.ACCOUNT_NAME_TAKEN.value()));
		assertThat(countByName(name)).as("[%s] rows named %s", APP, name).isEqualTo(1);
	}

	// A system account's name is taken too: the name constraint is checked before the type.
	@Test
	void systemAccountNameIsTaken() throws Exception {
		createAccount("{\"name\": \"system:external\", \"type\": \"CUSTOMER\"}").andExpect(status().isConflict())
			.andExpect(jsonPath("$.code").value(ProblemCode.ACCOUNT_NAME_TAKEN.value()));
	}

	// --- GET /v1/accounts/{id}/balance ---

	@Test
	void newAccountBalanceIsZeroAsOfDatabaseTime() throws Exception {
		long id = createdAccountId("CUSTOMER");
		OffsetDateTime before = databaseNow();
		MvcResult result = this.mvc.perform(authenticated(get("/v1/accounts/{id}/balance", id)))
			.andExpect(status().isOk())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
			.andExpect(jsonPath("$.account_id").value(id))
			.andExpect(jsonPath("$.balance").value("0.00"))
			.andExpect(jsonPath("$.as_of").isString())
			.andReturn();
		OffsetDateTime after = databaseNow();
		OffsetDateTime asOf = OffsetDateTime
			.parse(JsonPath.read(result.getResponse().getContentAsString(), "$.as_of"));
		assertThat(asOf).as("as_of between database now() before and after").isBetween(before, after);
	}

	@Test
	void balanceAfterPostingIsScale2String() throws Exception {
		long id = createdAccountId("CUSTOMER");
		postDeposit(id, "12.3");
		this.mvc.perform(authenticated(get("/v1/accounts/{id}/balance", id)))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.balance").value("12.30"));
	}

	// D-056: a stored balance with non-zero digits past 2 dp is a 500, never rounded.
	@Test
	void subCentBalanceIs500NotRounded() throws Exception {
		long id = createdAccountId("CUSTOMER");
		postDeposit(id, "0.0050");
		MvcResult result = this.mvc.perform(authenticated(get("/v1/accounts/{id}/balance", id)))
			.andExpect(status().isInternalServerError())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.INTERNAL_ERROR.value()))
			.andReturn();
		assertThat(result.getResponse().getContentAsString()).as("500 body")
			.doesNotContain("0.0050", "0.01", "0.00", "Arithmetic", "Rounding");
	}

	@Test
	void unknownAccountIs404() throws Exception {
		this.mvc.perform(authenticated(get("/v1/accounts/{id}/balance", Long.MAX_VALUE)))
			.andExpect(status().isNotFound())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.ACCOUNT_NOT_FOUND.value()));
	}

	// --- helpers ---

	private ResultActions createAccount(String json) throws Exception {
		return this.mvc
			.perform(authenticated(post("/v1/accounts")).contentType(MediaType.APPLICATION_JSON).content(json));
	}

	private long createdAccountId(String type) throws Exception {
		MvcResult result = createAccount("{\"name\": \"" + uniqueName() + "\", \"type\": \"" + type + "\"}")
			.andExpect(status().isCreated())
			.andReturn();
		return ((Number) JsonPath.read(result.getResponse().getContentAsString(), "$.account_id")).longValue();
	}

	private static MockHttpServletRequestBuilder authenticated(MockHttpServletRequestBuilder request) {
		return request.header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY);
	}

	private static String uniqueName() {
		return "t2-" + UUID.randomUUID();
	}

	// EXTERNAL -amount -> account +amount, one transaction, balances updated, accounts locked
	// in ascending id order (invariants 1 and 4), explicit commit.
	private void postDeposit(long accountId, String amount) throws SQLException {
		try (Connection conn = connectApp()) {
			try {
				long external;
				try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM accounts WHERE type = 'EXTERNAL'");
						ResultSet rs = ps.executeQuery()) {
					assertThat(rs.next()).as("seeded EXTERNAL account").isTrue();
					external = rs.getLong(1);
				}
				long txId;
				try (PreparedStatement ps = conn.prepareStatement(
						"INSERT INTO transactions (kind, reference) VALUES ('DEPOSIT', ?) RETURNING id")) {
					ps.setString(1, uniqueName());
					try (ResultSet rs = ps.executeQuery()) {
						assertThat(rs.next()).isTrue();
						txId = rs.getLong(1);
					}
				}
				try (PreparedStatement ps = conn
					.prepareStatement("INSERT INTO entries (transaction_id, account_id, amount) VALUES (?, ?, ?)")) {
					for (Object[] leg : List.of(new Object[] { external, new BigDecimal(amount).negate() },
							new Object[] { accountId, new BigDecimal(amount) })) {
						ps.setLong(1, txId);
						ps.setLong(2, (Long) leg[0]);
						ps.setBigDecimal(3, (BigDecimal) leg[1]);
						assertThat(ps.executeUpdate()).isEqualTo(1);
					}
				}
				try (PreparedStatement ps = conn
					.prepareStatement("SELECT id FROM accounts WHERE id = ANY (?) ORDER BY id FOR UPDATE")) {
					ps.setArray(1, conn.createArrayOf("bigint", new Object[] { external, accountId }));
					ps.executeQuery().close();
				}
				try (PreparedStatement ps = conn
					.prepareStatement("UPDATE accounts SET balance = balance + ? WHERE id = ?")) {
					for (long id : new long[] { Math.min(external, accountId), Math.max(external, accountId) }) {
						BigDecimal delta = new BigDecimal(amount);
						ps.setBigDecimal(1, (id == external) ? delta.negate() : delta);
						ps.setLong(2, id);
						assertThat(ps.executeUpdate()).isEqualTo(1);
					}
				}
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}
	}

	private AccountRow accountRow(long id) throws SQLException {
		try (Connection conn = connectApp();
				PreparedStatement ps = conn
					.prepareStatement("SELECT type, allow_negative, balance FROM accounts WHERE id = ?")) {
			ps.setLong(1, id);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).as("[%s] account %d exists", APP, id).isTrue();
				return new AccountRow(rs.getString(1), rs.getBoolean(2), rs.getBigDecimal(3));
			}
		}
	}

	private long countByName(String name) throws SQLException {
		try (Connection conn = connectApp();
				PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM accounts WHERE name = ?")) {
			ps.setString(1, name);
			try (ResultSet rs = ps.executeQuery()) {
				assertThat(rs.next()).isTrue();
				return rs.getLong(1);
			}
		}
	}

	private OffsetDateTime databaseNow() throws SQLException {
		try (Connection conn = connectApp();
				PreparedStatement ps = conn.prepareStatement("SELECT clock_timestamp()");
				ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).isTrue();
			return rs.getObject(1, OffsetDateTime.class);
		}
	}

	// App-role connection, autoCommit off; asserts the session really is ledger_api_app (D-036).
	private Connection connectApp() throws SQLException {
		Connection conn = this.dataSource.getConnection();
		try (PreparedStatement ps = conn.prepareStatement("SELECT current_user");
				ResultSet rs = ps.executeQuery()) {
			assertThat(rs.next()).isTrue();
			assertThat(rs.getString(1)).as("[%s] current_user", APP).isEqualTo(APP);
			conn.setAutoCommit(false);
			return conn;
		}
		catch (SQLException | RuntimeException | Error e) {
			conn.close();
			throw e;
		}
	}

}
