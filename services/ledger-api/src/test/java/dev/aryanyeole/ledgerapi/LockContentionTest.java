package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import com.jayway.jsonpath.JsonPath;

import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// An account-lock timeout is contention, not a duplicate in flight (D-050): 503
// lock_contention with Retry-After, rolled back and never stored, so the same key works once
// the lock is free. The in-progress 409 on the key itself is T7's IdempotencyInProgressTest.
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = { EdgeProbe.API_KEYS_PROPERTY, "ledger.db.lock-timeout=500ms" })
@AutoConfigureMockMvc
class LockContentionTest {

	@Autowired
	MockMvc mvc;

	@Autowired
	DataSource dataSource;

	@Test
	void heldAccountLockIs503AndStoresNothing() throws Exception {
		long target = createAccount();
		String key = "t4-" + UUID.randomUUID();

		// Another transaction holds the target's row lock; MockMvc runs the request on this
		// thread, which waits lock_timeout (500ms) and gets 55P03.
		try (Connection holder = LedgerTestSupport.connectApp(this.dataSource)) {
			try (PreparedStatement ps = holder.prepareStatement("SELECT id FROM accounts WHERE id = ? FOR UPDATE")) {
				ps.setLong(1, target);
				try (ResultSet rs = ps.executeQuery()) {
					assertThat(rs.next()).as("holder locked account %d", target).isTrue();
				}
			}

			long started = System.nanoTime();
			this.mvc.perform(deposit(key, target))
				.andExpect(status().isServiceUnavailable())
				.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
				.andExpect(header().string("Retry-After", "1"))
				.andExpect(jsonPath("$.code").value(ProblemCode.LOCK_CONTENTION.value()));
			long waitedMillis = (System.nanoTime() - started) / 1_000_000;
			assertThat(waitedMillis).as("waited at least lock_timeout").isGreaterThanOrEqualTo(500);

			assertThat(LedgerTestSupport.keyRow(this.dataSource, EdgeProbe.CLIENT_ID, key)).as("key row after 503")
				.isEmpty();
			assertThat(LedgerTestSupport.transactionsTouching(this.dataSource, target)).isZero();
			holder.rollback();
		}

		this.mvc.perform(deposit(key, target)).andExpect(status().isCreated());
		assertThat(LedgerTestSupport.transactionsTouching(this.dataSource, target)).as("transactions after retry")
			.isEqualTo(1);
	}

	private long createAccount() throws Exception {
		MvcResult result = this.mvc
			.perform(authenticated(post("/v1/accounts")).contentType(MediaType.APPLICATION_JSON)
				.content("{\"name\": \"t4-" + UUID.randomUUID() + "\", \"type\": \"CUSTOMER\"}"))
			.andExpect(status().isCreated())
			.andReturn();
		return ((Number) JsonPath.read(result.getResponse().getContentAsString(), "$.account_id")).longValue();
	}

	private static MockHttpServletRequestBuilder deposit(String key, long target) {
		return authenticated(post("/v1/deposits")).header("Idempotency-Key", key)
			.contentType(MediaType.APPLICATION_JSON)
			.content("{\"account_id\": " + target + ", \"amount\": \"3.00\"}");
	}

	private static MockHttpServletRequestBuilder authenticated(MockHttpServletRequestBuilder request) {
		return request.header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY);
	}

}
