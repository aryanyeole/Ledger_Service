package dev.aryanyeole.ledgerapi.accounts;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.money.Money;

// POST /v1/accounts, GET /v1/accounts/{id} and GET /v1/accounts/{id}/balance (spec §7,
// D-048, D-057). Create and read return the same representation.
// Idempotency-Key is not read yet; keyed requests come in T4 (D-054).
@RestController
@RequestMapping("/v1/accounts")
class AccountsController {

	// System types (EXTERNAL, ESCROW, PLATFORM_FEES) are seeded by V4, never created here.
	private static final Set<String> CREATABLE_TYPES = Set.of("CUSTOMER", "MERCHANT");

	private static final int MAX_NAME_LENGTH = 100;

	// Control characters (newline, tab, NUL, DEL, C1, ...) would reach logs and the
	// dashboard (D-057).
	private static final Pattern CONTROL_CHARACTER = Pattern.compile("\\p{Cc}");

	private final AccountRepository accounts;

	AccountsController(AccountRepository accounts) {
		this.accounts = accounts;
	}

	// Only name and type are bound: allow_negative or any other field in the body is ignored.
	record CreateAccountRequest(String name, String type) {
	}

	record AccountResponse(long accountId, String name, String type, OffsetDateTime createdAt) {
	}

	record BalanceResponse(long accountId, Money balance, OffsetDateTime asOf) {
	}

	@PostMapping
	ResponseEntity<AccountResponse> create(@RequestBody CreateAccountRequest request) {
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
		AccountRepository.Created created = this.accounts.insert(name, type);
		return ResponseEntity.created(URI.create("/v1/accounts/" + created.id()))
			.body(new AccountResponse(created.id(), name, type, created.createdAt()));
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
