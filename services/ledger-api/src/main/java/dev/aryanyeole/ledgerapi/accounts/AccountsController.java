package dev.aryanyeole.ledgerapi.accounts;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import tools.jackson.databind.json.JsonMapper;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ApiKeyFilter;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.db.WriteTransactions;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyKeyHeader;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyService;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyService.Outcome;
import dev.aryanyeole.ledgerapi.money.Money;

// POST /v1/accounts, GET /v1/accounts/{id} and GET /v1/accounts/{id}/balance (spec §7,
// D-048, D-057). Create and read return the same representation.
@RestController
@RequestMapping("/v1/accounts")
class AccountsController {

	// System types (EXTERNAL, ESCROW, PLATFORM_FEES) are seeded by V4, never created here.
	private static final Set<String> CREATABLE_TYPES = Set.of("CUSTOMER", "MERCHANT");

	private static final int MAX_NAME_LENGTH = 100;

	// Control characters (newline, tab, NUL, DEL, C1, ...) would reach logs and the
	// dashboard (D-057).
	private static final Pattern CONTROL_CHARACTER = Pattern.compile("\\p{Cc}");

	static final String ROUTE = "/v1/accounts";

	private final AccountRepository accounts;

	private final WriteTransactions transactions;

	private final IdempotencyService idempotency;

	private final JsonMapper json;

	AccountsController(AccountRepository accounts, WriteTransactions transactions, IdempotencyService idempotency,
			JsonMapper json) {
		this.accounts = accounts;
		this.transactions = transactions;
		this.idempotency = idempotency;
		this.json = json;
	}

	// Only name and type: allow_negative or any other field in the body is a 400
	// invalid_request (D-046). The fingerprint is this record's canonical JSON.
	record CreateAccountRequest(String name, String type) {
	}

	record AccountResponse(long accountId, String name, String type, OffsetDateTime createdAt) {
	}

	record BalanceResponse(long accountId, Money balance, OffsetDateTime asOf) {
	}

	// Idempotency-Key is optional here (D-054). Without one, the insert runs in its own write
	// transaction and a taken name is an unstored 409. With one, the idempotency service owns
	// the transaction and the 201 or 409 is stored and replayed. Either way every 201 carries
	// Location: /v1/accounts/{account_id}, taken from the body (D-063): stored responses keep
	// only status and body, so a replay rebuilds it the same way.
	@PostMapping
	ResponseEntity<?> create(@RequestBody CreateAccountRequest request,
			@RequestHeader(name = IdempotencyKeyHeader.HEADER, required = false) String idempotencyKey,
			HttpServletRequest http) {
		Optional<String> key = IdempotencyKeyHeader.optional(idempotencyKey);
		String type = request.type();
		if (type == null || !CREATABLE_TYPES.contains(type)) {
			throw new ApiException(HttpStatus.BAD_REQUEST, ProblemCode.INVALID_ACCOUNT_TYPE,
					"type must be CUSTOMER or MERCHANT.");
		}
		String name = request.name();
		if (name == null || name.isBlank() || name.codePointCount(0, name.length()) > MAX_NAME_LENGTH
				|| CONTROL_CHARACTER.matcher(name).find()) {
			throw new ApiException(HttpStatus.BAD_REQUEST, ProblemCode.INVALID_ACCOUNT_NAME,
					"name must be 1 to " + MAX_NAME_LENGTH + " characters, not blank, with no control characters.");
		}
		if (key.isEmpty()) {
			AccountRepository.Created created = this.transactions.execute((status) -> this.accounts.insert(name, type));
			return ResponseEntity.created(location(created.id()))
				.body(new AccountResponse(created.id(), name, type, created.createdAt()));
		}
		ResponseEntity<String> response = this.idempotency.execute(ApiKeyFilter.clientId(http), key.get(), "POST",
				ROUTE, request, () -> {
					AccountRepository.Created created = this.accounts.insertIfNameFree(name, type);
					return new Outcome(HttpStatus.CREATED,
							new AccountResponse(created.id(), name, type, created.createdAt()));
				});
		if (response.getStatusCode().value() != HttpStatus.CREATED.value()) {
			return response;
		}
		long id = this.json.readTree(response.getBody()).get("account_id").asLong();
		return ResponseEntity.status(HttpStatus.CREATED)
			.headers(response.getHeaders())
			.location(location(id))
			.body(response.getBody());
	}

	private static URI location(long id) {
		return URI.create(ROUTE + "/" + id);
	}

	@GetMapping("/{id}")
	AccountResponse get(@PathVariable long id) {
		AccountRepository.Account account = this.accounts.find(id).orElseThrow(AccountsController::notFound);
		return new AccountResponse(account.id(), account.name(), account.type(), account.createdAt());
	}

	// The balance goes through Money: a stored value with non-zero digits past 2 dp throws
	// and becomes a 500, never a rounded number (D-056).
	@GetMapping("/{id}/balance")
	BalanceResponse balance(@PathVariable long id) {
		AccountRepository.Balance balance = this.accounts.balance(id).orElseThrow(AccountsController::notFound);
		return new BalanceResponse(id, new Money(balance.balance()), balance.asOf());
	}

	private static ApiException notFound() {
		return new ApiException(HttpStatus.NOT_FOUND, ProblemCode.ACCOUNT_NOT_FOUND, "No account with this id.");
	}

}
