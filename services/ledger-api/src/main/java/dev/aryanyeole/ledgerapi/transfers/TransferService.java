package dev.aryanyeole.ledgerapi.transfers;

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

// A transfer (spec §6.1): from -X, to +X. Runs inside the idempotency service's transaction.
// Both accounts are locked in one ascending statement (invariant 4, D-007), whichever way the
// money moves, so A->B and B->A can't deadlock. Only CUSTOMER and MERCHANT accounts take part
// (D-066). The funds check is PostingService's: from.balance - X >= 0, so draining to exactly
// 0.00 is allowed; short -> stored 422 insufficient_funds, rolled back to the savepoint (D-064).
@Service
class TransferService {

	private static final Set<String> TRANSFERABLE_TYPES = Set.of("CUSTOMER", "MERCHANT");

	private final PostingService postings;

	TransferService(PostingService postings) {
		this.postings = postings;
	}

	record TransferResponse(long transactionId, String kind, long fromAccountId, long toAccountId, Money amount,
			OffsetDateTime createdAt) {
	}

	Outcome transfer(long from, long to, Money amount) {
		LockedAccounts locked = this.postings.lock(List.of(from, to));
		if (!TRANSFERABLE_TYPES.contains(locked.get(from).type())
				|| !TRANSFERABLE_TYPES.contains(locked.get(to).type())) {
			throw new StoredOutcomeException(HttpStatus.UNPROCESSABLE_CONTENT, ProblemCode.ACCOUNT_NOT_TRANSFERABLE,
					"Transfers can only move money between CUSTOMER and MERCHANT accounts.");
		}
		Posted posted = this.postings.post(locked, "TRANSFER",
				List.of(new Leg(from, amount.amount().negate()), new Leg(to, amount.amount())), null,
				MDC.get(CorrelationIdFilter.MDC_KEY));
		return new Outcome(HttpStatus.CREATED,
				new TransferResponse(posted.transactionId(), "TRANSFER", from, to, amount, posted.createdAt()));
	}

}
