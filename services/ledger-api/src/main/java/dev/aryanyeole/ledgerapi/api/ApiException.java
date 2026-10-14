package dev.aryanyeole.ledgerapi.api;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;

// An expected client-facing failure: rendered by ProblemHandler as application/problem+json
// with this status, machine-readable code and detail. The detail is shown to the client, so
// it must never carry SQL, SQLState, key material or stack traces.
public class ApiException extends RuntimeException {

	private final HttpStatus status;

	private final ProblemCode code;

	private final HttpHeaders headers;

	public ApiException(HttpStatus status, ProblemCode code, String detail) {
		this(status, code, detail, HttpHeaders.EMPTY);
	}

	public ApiException(HttpStatus status, ProblemCode code, String detail, HttpHeaders headers) {
		super(detail, null, false, false);
		this.status = status;
		this.code = code;
		this.headers = headers;
	}

	public HttpStatus status() {
		return this.status;
	}

	public ProblemCode code() {
		return this.code;
	}

	public HttpHeaders headers() {
		return this.headers;
	}

}
