package dev.aryanyeole.ledgerapi.payments;

import java.net.URI;
import java.util.UUID;
import java.util.regex.Pattern;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import tools.jackson.databind.json.JsonMapper;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyKeyHeader;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyService;
import dev.aryanyeole.ledgerapi.money.Money;
import dev.aryanyeole.ledgerapi.payments.PaymentService.PaymentResponse;

// POST /v1/payments and GET /v1/payments/{id} (spec §6.2, §7, D-088). POST is keyed (D-046):
// the body (strict JSON, D-065; amount codec, D-047), the key, positive ids and customer !=
// merchant (400 same_account) are all decided before any transaction opens, so none is stored.
// The response is the stored text, sent unchanged (D-044); every 201, owner or replay, carries
// Location: /v1/payments/{payment_id} taken from the body (D-063). A replay is the original
// AUTHORIZED outcome; GET returns the current status.
@RestController
class PaymentsController {

	static final String ROUTE = "/v1/payments";

	// Canonical 8-4-4-4-12 hex only: UUID.fromString also accepts forms like 1-1-1-1-1.
	private static final Pattern CANONICAL_UUID = Pattern
		.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

	private final IdempotencyService idempotency;

	private final PaymentService payments;

	private final JsonMapper json;

	PaymentsController(IdempotencyService idempotency, PaymentService payments, JsonMapper json) {
		this.idempotency = idempotency;
		this.payments = payments;
		this.json = json;
	}

	// The fingerprint is this record's canonical JSON (D-046): every field counts. The fee rate is
	// not part of the request, so changing it can't turn a replay into a 422.
	record PaymentRequest(Long customerAccountId, Long merchantAccountId, Money amount) {
	}

	@PostMapping(ROUTE)
	ResponseEntity<String> authorize(@RequestBody PaymentRequest request,
			@RequestHeader(name = IdempotencyKeyHeader.HEADER, required = false) String idempotencyKey,
			HttpServletRequest http) {
		String key = IdempotencyKeyHeader.require(idempotencyKey);
		long customer = positiveId(request.customerAccountId(), "customer_account_id");
		long merchant = positiveId(request.merchantAccountId(), "merchant_account_id");
		if (customer == merchant) {
			throw new ApiException(HttpStatus.BAD_REQUEST, ProblemCode.SAME_ACCOUNT,
					"customer_account_id and merchant_account_id must be different accounts.");
		}
		ResponseEntity<String> response = this.idempotency.execute(ApiKeyFilter.clientId(http), key, "POST", ROUTE,
				request, () -> this.payments.authorize(customer, merchant, request.amount()));
		if (response.getStatusCode().value() != HttpStatus.CREATED.value()) {
			return response;
		}
		String paymentId = this.json.readTree(response.getBody()).get("payment_id").asString();
		return ResponseEntity.status(HttpStatus.CREATED)
			.headers(response.getHeaders())
			.location(URI.create(ROUTE + "/" + paymentId))
			.body(response.getBody());
	}

	@GetMapping(ROUTE + "/{id}")
	PaymentResponse get(@PathVariable String id) {
		if (!CANONICAL_UUID.matcher(id).matches()) {
			throw new ApiException(HttpStatus.BAD_REQUEST, ProblemCode.INVALID_REQUEST,
					"id must be a UUID in canonical form.");
		}
		return this.payments.find(UUID.fromString(id))
			.orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, ProblemCode.PAYMENT_NOT_FOUND,
					"No payment with this id."));
	}

	private static long positiveId(Long id, String field) {
		if (id == null || id <= 0) {
			throw new ApiException(HttpStatus.BAD_REQUEST, ProblemCode.INVALID_REQUEST,
					field + " must be a positive integer.");
		}
		return id;
	}

}
