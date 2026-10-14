package dev.aryanyeole.ledgerapi.deposits;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;

import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import dev.aryanyeole.ledgerapi.api.CorrelationIdFilter;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.api.StoredOutcomeException;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyService.Outcome;
import dev.aryanyeole.ledgerapi.money.Money;
import dev.aryanyeole.ledgerapi.posting.PostingService;
import dev.aryanyeole.ledgerapi.posting.PostingService.Leg;
import dev.aryanyeole.ledgerapi.posting.PostingService.LockedAccounts;
import dev.aryanyeole.ledgerapi.posting.PostingService.Posted;

// A deposit (spec §4): EXTERNAL -X, target +X. Runs inside the idempotency service's
// transaction. system:external and the target are locked together in one ordered statement.
// Only CUSTOMER and MERCHANT accounts can be credited (D-062), so a deposit to EXTERNAL
// itself, ESCROW or PLATFORM_FEES is a stored 422.
@Service
class DepositService {

	private static final Set<String> DEPOSITABLE_TYPES = Set.of("CUSTOMER", "MERCHANT");

	private final PostingService postings;

	DepositService(PostingService postings) {
		this.postings = postings;
	}

	record DepositResponse(long transactionId, String kind, long accountId, Money amount, OffsetDateTime createdAt) {
	}

	Outcome deposit(long accountId, Money amount) {
		long external = this.postings.externalAccountId();
		// A list, not Set.of: the target may be system:external itself (Set.of rejects duplicates);
		// lock() de-duplicates, so that case locks one row and fails the type check below.
		LockedAccounts locked = this.postings.lock(List.of(external, accountId));
		if (!DEPOSITABLE_TYPES.contains(locked.get(accountId).type())) {
			throw new StoredOutcomeException(HttpStatus.UNPROCESSABLE_CONTENT, ProblemCode.ACCOUNT_NOT_DEPOSITABLE,
					"Deposits can only be made to CUSTOMER or MERCHANT accounts.");
		}
		Posted posted = this.postings.post(locked, "DEPOSIT",
				List.of(new Leg(external, amount.amount().negate()), new Leg(accountId, amount.amount())), null,
				MDC.get(CorrelationIdFilter.MDC_KEY));
		return new Outcome(HttpStatus.CREATED,
				new DepositResponse(posted.transactionId(), "DEPOSIT", accountId, amount, posted.createdAt()));
	}

}
