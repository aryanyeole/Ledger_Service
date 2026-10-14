package dev.aryanyeole.ledgerapi.api;

import org.springframework.http.HttpStatus;

// An outcome decided inside the transaction that owns an idempotency key, so it is stored
// against the key and replayed (D-045): 404 account_not_found, 422 business rules, and 409
// account_name_taken on keyed account creation. The owner rolls back to its savepoint
// before storing it (D-064). Thrown outside a keyed request it is an ordinary ApiException.
// Every other ApiException (400, 409 in progress, 422 key reused, 503) is never stored.
public class StoredOutcomeException extends ApiException {

	public StoredOutcomeException(HttpStatus status, ProblemCode code, String detail) {
		super(status, code, detail);
	}

}
