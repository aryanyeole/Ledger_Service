package dev.aryanyeole.ledgerapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import com.jayway.jsonpath.JsonPath;

import dev.aryanyeole.ledgerapi.LedgerTestSupport.Totals;
import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.EdgeProbe;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.posting.PostingService;

// The key and the effect roll back together (invariant 7). A test override of PostingService
// writes the posting for real (transactions row, entries, balances) and then throws, so the
// failure comes after both the key claim and the ledger writes. Afterwards neither the key
// nor any ledger row exists, and the same request with the same key succeeds.
@Import({ TestcontainersConfiguration.class, IdempotencyRollbackTest.FailingPostings.class })
@SpringBootTest(properties = EdgeProbe.API_KEYS_PROPERTY)
@AutoConfigureMockMvc
class IdempotencyRollbackTest {

	@Autowired
	MockMvc mvc;

	@Autowired
	DataSource dataSource;

	@Autowired
	FailingPostingService postings;

	@TestConfiguration(proxyBeanMethods = false)
	static class FailingPostings {

		@Bean
		@Primary
		FailingPostingService failingPostingService(JdbcClient jdbc) {
			return new FailingPostingService(jdbc);
		}

	}

	static class FailingPostingService extends PostingService {

		final AtomicBoolean failNext = new AtomicBoolean();

		FailingPostingService(JdbcClient jdbc) {
			super(jdbc);
		}

		@Override
		public Posted post(LockedAccounts locked, String kind, List<Leg> legs, String reference,
				String correlationId) {
			Posted posted = super.post(locked, kind, legs, reference, correlationId);
			if (this.failNext.getAndSet(false)) {
				throw new IllegalStateException("injected failure after the ledger writes");
			}
			return posted;
		}

	}

	@Test
	void failureAfterTheWritesRollsBackKeyAndEffect() throws Exception {
		long target = createAccount();
		long external = SystemAccountFixture.external(this.dataSource);
		Totals externalBefore = LedgerTestSupport.totals(this.dataSource, external);
		String key = "t4-" + UUID.randomUUID();

		this.postings.failNext.set(true);
		this.mvc.perform(deposit(key, target))
			.andExpect(status().isInternalServerError())
			.andExpect(jsonPath("$.code").value(ProblemCode.INTERNAL_ERROR.value()));

		assertThat(this.postings.failNext.get()).as("injected failure fired").isFalse();
		assertThat(LedgerTestSupport.keyRow(this.dataSource, EdgeProbe.CLIENT_ID, key)).as("key row after rollback")
			.isEmpty();
		assertThat(LedgerTestSupport.transactionsTouching(this.dataSource, target)).as("transactions on target")
			.isZero();
		assertThat(LedgerTestSupport.totals(this.dataSource, target).balance()).as("target balance")
			.isEqualByComparingTo("0");
		Totals externalDelta = LedgerTestSupport.totals(this.dataSource, external).minus(externalBefore);
		assertThat(externalDelta.balance()).as("EXTERNAL balance moved").isEqualByComparingTo("0");
		assertThat(externalDelta.entrySum()).as("EXTERNAL entry sum moved").isEqualByComparingTo("0");

		this.mvc.perform(deposit(key, target)).andExpect(status().isCreated());
		assertThat(LedgerTestSupport.transactionsTouching(this.dataSource, target)).as("transactions after retry")
			.isEqualTo(1);
		assertThat(LedgerTestSupport.keyRow(this.dataSource, EdgeProbe.CLIENT_ID, key).orElseThrow().statusCode())
			.as("stored status after retry")
			.isEqualTo(201);
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
			.content("{\"account_id\": " + target + ", \"amount\": \"7.00\"}");
	}

	private static MockHttpServletRequestBuilder authenticated(MockHttpServletRequestBuilder request) {
		return request.header(ApiKeyFilter.HEADER, EdgeProbe.API_KEY);
	}

}
