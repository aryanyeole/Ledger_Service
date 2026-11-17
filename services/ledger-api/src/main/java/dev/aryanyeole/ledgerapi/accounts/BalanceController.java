package dev.aryanyeole.ledgerapi.accounts;

import java.time.OffsetDateTime;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.money.Money;

// GET /v1/accounts/{id}/balance (spec §7, D-056), on its own so the balance cache is reachable from
// nothing else: this class holds no WriteTransactions and no write path (D-142, D-151). With the cache
// off it is the plain DB read and sends no X-Cache header. With it on (docs/design/cache-invalidation.md
// §3.5): a hit is rebuilt from the cached balance and as_of through Money and the same serializer as a
// miss, never spliced into the body; a miss (or an unparseable entry) reads the DB, builds the
// response, then populates with the token the MGET saw; a bypass reads the DB and doesn't populate.
// A 404 is never cached. The balance goes through Money on every path: non-zero digits past 2 dp
// throw and become a 500, never a rounded number (D-056).
@RestController
@RequestMapping("/v1/accounts")
class BalanceController {

	static final String X_CACHE = "X-Cache";

	private final AccountRepository accounts;

	private final BalanceCache cache;

	BalanceController(AccountRepository accounts, ObjectProvider<BalanceCache> cache) {
		this.accounts = accounts;
		this.cache = cache.getIfAvailable();
	}

	record BalanceResponse(long accountId, Money balance, OffsetDateTime asOf) {
	}

	@GetMapping("/{id}/balance")
	ResponseEntity<BalanceResponse> balance(@PathVariable long id) {
		if (this.cache == null) {
			return ResponseEntity.ok(fromDatabase(id));
		}
		BalanceCache.Lookup lookup = this.cache.read(id);
		return switch (lookup.result()) {
			case HIT -> cached("hit",
					new BalanceResponse(id, new Money(lookup.entry().balance()), lookup.entry().asOf()));
			case BYPASS -> cached("bypass", fromDatabase(id));
			case MISS, UNPARSEABLE -> {
				AccountRepository.Balance row = this.accounts.balance(id).orElseThrow(BalanceController::notFound);
				BalanceResponse response = new BalanceResponse(id, new Money(row.balance()), row.asOf());
				this.cache.populate(id, lookup.token(), row.balance(), row.asOf());
				yield cached("miss", response);
			}
		};
	}

	private BalanceResponse fromDatabase(long id) {
		AccountRepository.Balance row = this.accounts.balance(id).orElseThrow(BalanceController::notFound);
		return new BalanceResponse(id, new Money(row.balance()), row.asOf());
	}

	private static ResponseEntity<BalanceResponse> cached(String result, BalanceResponse body) {
		return ResponseEntity.ok().header(X_CACHE, result).body(body);
	}

	private static ApiException notFound() {
		return new ApiException(HttpStatus.NOT_FOUND, ProblemCode.ACCOUNT_NOT_FOUND, "No account with this id.");
	}

}
