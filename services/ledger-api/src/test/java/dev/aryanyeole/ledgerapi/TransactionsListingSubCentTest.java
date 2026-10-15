package dev.aryanyeole.ledgerapi;

import static dev.aryanyeole.ledgerapi.ApiTestClient.authenticated;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// D-056 on the listing: a page containing an entry with non-zero digits past 2 dp is a 500,
// never a rounded amount. Its own context (marker property), because the committed sub-cent
// row would make every later full traversal in the same database a 500.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.test.context=transactions-listing-subcent" })
@AutoConfigureMockMvc
class TransactionsListingSubCentTest {

	@Autowired
	MockMvc mvc;

	@Autowired
	DataSource dataSource;

	@Test
	void pageWithASubCentEntryIs500NotRounded() throws Exception {
		long external = SystemAccountFixture.external(this.dataSource);
		long customer = ApiTestClient.createAccount(this.mvc, "CUSTOMER");
		try (Connection conn = LedgerTestSupport.connectApp(this.dataSource)) {
			try {
				long txId;
				try (PreparedStatement ps = conn
					.prepareStatement("INSERT INTO transactions (kind) VALUES ('DEPOSIT') RETURNING id");
						ResultSet rs = ps.executeQuery()) {
					assertThat(rs.next()).isTrue();
					txId = rs.getLong(1);
				}
				try (PreparedStatement ps = conn
					.prepareStatement("SELECT id FROM accounts WHERE id = ANY (?) ORDER BY id FOR UPDATE")) {
					ps.setArray(1, conn.createArrayOf("bigint", new Object[] { external, customer }));
					ps.executeQuery().close();
				}
				try (PreparedStatement entry = conn
					.prepareStatement("INSERT INTO entries (transaction_id, account_id, amount) VALUES (?, ?, ?)");
						PreparedStatement balance = conn
							.prepareStatement("UPDATE accounts SET balance = balance + ? WHERE id = ?")) {
					for (Object[] leg : new Object[][] { { external, new BigDecimal("-0.0050") },
							{ customer, new BigDecimal("0.0050") } }) {
						entry.setLong(1, txId);
						entry.setLong(2, (Long) leg[0]);
						entry.setBigDecimal(3, (BigDecimal) leg[1]);
						entry.executeUpdate();
						balance.setBigDecimal(1, (BigDecimal) leg[1]);
						balance.setLong(2, (Long) leg[0]);
						balance.executeUpdate();
					}
				}
				conn.commit();
			}
			finally {
				conn.rollback();
			}
		}

		MvcResult result = this.mvc.perform(authenticated(get("/v1/transactions")).param("limit", "1"))
			.andExpect(status().isInternalServerError())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
			.andExpect(jsonPath("$.code").value(ProblemCode.INTERNAL_ERROR.value()))
			.andReturn();
		assertThat(result.getResponse().getContentAsString()).as("500 body")
			.doesNotContain("0.0050", "0.01", "Arithmetic", "Rounding");
	}

}
