package dev.aryanyeole.ledgerapi.negativecontrols;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.MDC;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import tools.jackson.databind.json.JsonMapper;

import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.CorrelationIdFilter;
import dev.aryanyeole.ledgerapi.db.WriteTransactions;
import dev.aryanyeole.ledgerapi.idempotency.Fingerprint;
import dev.aryanyeole.ledgerapi.money.Money;
import dev.aryanyeole.ledgerapi.posting.PostingService;
import dev.aryanyeole.ledgerapi.posting.PostingService.Leg;
import dev.aryanyeole.ledgerapi.posting.PostingService.LockedAccounts;
import dev.aryanyeole.ledgerapi.posting.PostingService.Posted;

// NEGATIVE CONTROL (D-017): check-then-insert idempotency, deliberately violating invariant 7
// (the key commits outside the effect's transaction). Test sources only; registered at
// POST /v1/test/naive-transfers solely by CheckThenInsertIdempotencyTest (@TestComponent
// keeps it out of every component scan); main code never imports it. Invariants 1-5 still
// hold: the posting is the real one, through WriteTransactions and PostingService (ordered
// locks, funds check, deferred balance trigger active). Triggers, schema and roles are
// untouched; tx 3 stores the response with its key, so V6's completeness trigger is satisfied.
@TestComponent
@RestController
public class NaiveTransferController {

	public static final String ROUTE = "/v1/test/naive-transfers";

	private final WriteTransactions transactions;

	private final PostingService postings;

	private final JdbcClient jdbc;

	private final JsonMapper json;

	NaiveTransferController(WriteTransactions transactions, PostingService postings, JdbcClient jdbc,
			JsonMapper json) {
		this.transactions = transactions;
		this.postings = postings;
		this.jdbc = jdbc;
		this.json = json;
	}

	record NaiveTransferRequest(Long fromAccountId, Long toAccountId, Money amount) {
	}

	record NaiveTransferResponse(long transactionId, String kind, long fromAccountId, long toAccountId, Money amount,
			OffsetDateTime createdAt) {
	}

	private record Stored(int status, String body) {
	}

	@PostMapping(ROUTE)
	ResponseEntity<String> transfer(@RequestBody NaiveTransferRequest request,
			@RequestHeader("Idempotency-Key") String key, HttpServletRequest http) {
		String clientId = ApiKeyFilter.clientId(http);
		long from = request.fromAccountId();
		long to = request.toAccountId();

		// tx 1: check. Every request that gets here before some request finishes tx 3 sees
		// "absent" and goes on to post: that window is the bug.
		Optional<Stored> existing = this.transactions.execute((status) -> this.jdbc
			.sql("SELECT status_code, response_body::text FROM idempotency_keys WHERE client_id = :client AND key = :key")
			.param("client", clientId)
			.param("key", key)
			.query((rs, rowNum) -> new Stored(rs.getInt(1), rs.getString(2)))
			.optional());
		if (existing.isPresent()) {
			return ResponseEntity.status(existing.get().status())
				.contentType(MediaType.APPLICATION_JSON)
				.body(existing.get().body());
		}

		// tx 2: the real posting, committed on its own.
		String body = this.transactions.execute((status) -> {
			LockedAccounts locked = this.postings.lock(List.of(from, to));
			Posted posted = this.postings.post(locked, "TRANSFER",
					List.of(new Leg(from, request.amount().amount().negate()), new Leg(to, request.amount().amount())),
					null, MDC.get(CorrelationIdFilter.MDC_KEY));
			return this.json.writeValueAsString(new NaiveTransferResponse(posted.transactionId(), "TRANSFER", from, to,
					request.amount(), posted.createdAt()));
		});

		// tx 3: claim the key afterwards. The unique key stops a second row, not a second effect.
		this.transactions.execute((status) -> {
			int inserted = this.jdbc
				.sql("INSERT INTO idempotency_keys (client_id, key, request_hash) VALUES (:client, :key, :hash) "
						+ "ON CONFLICT DO NOTHING")
				.param("client", clientId)
				.param("key", key)
				.param("hash", Fingerprint.of("POST", ROUTE, request))
				.update();
			if (inserted == 1) {
				this.jdbc
					.sql("UPDATE idempotency_keys SET status_code = 201, response_body = :body::jsonb "
							+ "WHERE client_id = :client AND key = :key")
					.param("body", body)
					.param("client", clientId)
					.param("key", key)
					.update();
			}
			return null;
		});
		return ResponseEntity.status(HttpStatus.CREATED).contentType(MediaType.APPLICATION_JSON).body(body);
	}

}
