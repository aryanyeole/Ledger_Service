package dev.aryanyeole.ledgerapi.api;

import java.util.Locale;

// Every machine-readable `code` the API sets itself (D-056). Clients branch on these, so a
// value is never renamed once shipped. Framework errors are not listed: ProblemHandler names
// them after their HTTP status (not_found, method_not_allowed, ...).
public enum ProblemCode {

	MISSING_API_KEY, INVALID_API_KEY, MALFORMED_REQUEST, INVALID_REQUEST, INTERNAL_ERROR, INVALID_AMOUNT,
	INVALID_ACCOUNT_TYPE, INVALID_ACCOUNT_NAME, ACCOUNT_NAME_TAKEN, ACCOUNT_NOT_FOUND, ACCOUNT_NOT_DEPOSITABLE,
	INSUFFICIENT_FUNDS, MISSING_IDEMPOTENCY_KEY, INVALID_IDEMPOTENCY_KEY, IDEMPOTENCY_REQUEST_IN_PROGRESS,
	IDEMPOTENCY_KEY_REUSED, LOCK_CONTENTION, SAME_ACCOUNT, ACCOUNT_NOT_TRANSFERABLE;

	/** The wire form: the constant name in snake_case, e.g. "missing_api_key". */
	public String value() {
		return name().toLowerCase(Locale.ROOT);
	}

}
