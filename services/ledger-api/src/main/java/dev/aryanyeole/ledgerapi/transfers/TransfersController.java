package dev.aryanyeole.ledgerapi.transfers;

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

// POST /v1/transfers (spec §6.1, §7): synchronous double-entry transfer, Idempotency-Key
// required (D-046). Everything below is decided before any transaction opens, so none of it
// touches the database or is stored: the body (strict JSON, D-065; amount codec, D-047), the
// key, positive ids, and from != to (400 same_account, D-066). The response body is the
// stored text, sent unchanged (D-044). No Location: there is no GET for a single transaction.
@RestController
class TransfersController {

	static final String ROUTE = "/v1/transfers";

	private final IdempotencyService idempotency;

	private final TransferService transfers;

	TransfersController(IdempotencyService idempotency, TransferService transfers) {
		this.idempotency = idempotency;
		this.transfers = transfers;
	}

	// The fingerprint is this record's canonical JSON (D-046): every field counts.
	record TransferRequest(Long fromAccountId, Long toAccountId, Money amount) {
	}

	@PostMapping(ROUTE)
	ResponseEntity<String> transfer(@RequestBody TransferRequest request,
			@RequestHeader(name = IdempotencyKeyHeader.HEADER, required = false) String idempotencyKey,
			HttpServletRequest http) {
		String key = IdempotencyKeyHeader.require(idempotencyKey);
		long from = positiveId(request.fromAccountId(), "from_account_id");
		long to = positiveId(request.toAccountId(), "to_account_id");
		if (from == to) {
			throw new ApiException(HttpStatus.BAD_REQUEST, ProblemCode.SAME_ACCOUNT,
					"from_account_id and to_account_id must be different accounts.");
		}
		return this.idempotency.execute(ApiKeyFilter.clientId(http), key, "POST", ROUTE, request,
				() -> this.transfers.transfer(from, to, request.amount()));
	}

	private static long positiveId(Long id, String field) {
		if (id == null || id <= 0) {
			throw new ApiException(HttpStatus.BAD_REQUEST, ProblemCode.INVALID_REQUEST,
					field + " must be a positive integer.");
		}
		return id;
	}

}
