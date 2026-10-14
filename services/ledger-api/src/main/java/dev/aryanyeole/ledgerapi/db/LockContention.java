package dev.aryanyeole.ledgerapi.db;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// 503 lock_contention (D-050): lock_timeout expired on an account lock or an account-name
// insert. Contention, not a duplicate in flight, so not 409. Never stored; the transaction
// rolls back and the client retries, so the same Idempotency-Key still works.
public class LockContention extends ApiException {

	public LockContention() {
		super(HttpStatus.SERVICE_UNAVAILABLE, ProblemCode.LOCK_CONTENTION,
				"The ledger is busy with conflicting writes; retry shortly.", retryAfter());
	}

	private static HttpHeaders retryAfter() {
		HttpHeaders headers = new HttpHeaders();
		headers.set(HttpHeaders.RETRY_AFTER, "1");
		return headers;
	}

}
