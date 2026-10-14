package dev.aryanyeole.ledgerapi.deposits;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyKeyHeader;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyService;
import dev.aryanyeole.ledgerapi.money.Money;

// POST /v1/deposits (spec §7): EXTERNAL -> account, Idempotency-Key required (D-046).
// The body, amount and header are validated before any transaction opens; a 400 never
// touches the database. The response body is the stored text, sent unchanged (D-044).
// No Location header: there is no GET for a single transaction.
@RestController
class DepositsController {

	static final String ROUTE = "/v1/deposits";

	private final IdempotencyService idempotency;

	private final DepositService deposits;

	DepositsController(IdempotencyService idempotency, DepositService deposits) {
		this.idempotency = idempotency;
		this.deposits = deposits;
	}

	// The fingerprint is this record's canonical JSON (D-046): every field counts.
	record DepositRequest(Long accountId, Money amount) {
	}

	@PostMapping(ROUTE)
	ResponseEntity<String> deposit(@RequestBody DepositRequest request,
			@RequestHeader(name = IdempotencyKeyHeader.HEADER, required = false) String idempotencyKey,
			HttpServletRequest http) {
		String key = IdempotencyKeyHeader.require(idempotencyKey);
		if (request.accountId() == null || request.accountId() <= 0) {
			throw new ApiException(HttpStatus.BAD_REQUEST, ProblemCode.INVALID_REQUEST,
					"account_id must be a positive integer.");
		}
		long accountId = request.accountId();
		return this.idempotency.execute(ApiKeyFilter.clientId(http), key, "POST", ROUTE, request,
				() -> this.deposits.deposit(accountId, request.amount()));
	}

}
