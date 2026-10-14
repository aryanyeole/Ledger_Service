package dev.aryanyeole.ledgerapi.api;

import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

// The one error handler: every error response is RFC 9457 application/problem+json with a
// machine-readable `code`. Framework exceptions keep Spring's status and generic detail and
// get a code derived from the status. Anything unexpected is a 500 whose body says nothing
// about the cause; the exception itself goes only to the server log.
@RestControllerAdvice
public class ProblemHandler extends ResponseEntityExceptionHandler {

	public static final String CODE = "code";

	static final String INTERNAL_ERROR = "internal_error";

	static final String MALFORMED_REQUEST = "malformed_request";

	private static final Logger log = LoggerFactory.getLogger(ProblemHandler.class);

	@ExceptionHandler(ApiException.class)
	ResponseEntity<ProblemDetail> handleApiException(ApiException ex) {
		return problem(ex);
	}

	@ExceptionHandler(Exception.class)
	ResponseEntity<ProblemDetail> handleUnexpected(Exception ex) {
		log.error("Unhandled exception", ex);
		ProblemDetail body = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR,
				"An unexpected error occurred.");
		body.setProperty(CODE, INTERNAL_ERROR);
		return ResponseEntity.internalServerError().body(body);
	}

	// A deserializer that rejects a value (e.g. an invalid amount) surfaces here, wrapped by
	// Jackson and the message converter; its own code and detail win over "malformed".
	@Override
	protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException ex,
			HttpHeaders headers, HttpStatusCode status, WebRequest request) {
		for (Throwable cause = ex.getCause(); cause != null; cause = cause.getCause()) {
			if (cause instanceof ApiException apiException) {
				return ResponseEntity.status(apiException.status()).body(problem(apiException).getBody());
			}
		}
		ProblemDetail body = ProblemDetail.forStatusAndDetail(status, "Request body is not readable JSON.");
		body.setProperty(CODE, MALFORMED_REQUEST);
		return handleExceptionInternal(ex, body, headers, status, request);
	}

	@Override
	protected ResponseEntity<Object> handleExceptionInternal(Exception ex, Object body, HttpHeaders headers,
			HttpStatusCode statusCode, WebRequest request) {
		ProblemDetail problem = (body instanceof ProblemDetail pd) ? pd : ProblemDetail.forStatus(statusCode);
		if (problem.getProperties() == null || !problem.getProperties().containsKey(CODE)) {
			problem.setProperty(CODE, codeFor(statusCode));
		}
		return super.handleExceptionInternal(ex, problem, headers, statusCode, request);
	}

	private static ResponseEntity<ProblemDetail> problem(ApiException ex) {
		ProblemDetail body = ProblemDetail.forStatusAndDetail(ex.status(), ex.getMessage());
		body.setProperty(CODE, ex.code());
		return ResponseEntity.status(ex.status()).headers(ex.headers()).body(body);
	}

	// NOT_FOUND -> not_found, METHOD_NOT_ALLOWED -> method_not_allowed, ...
	private static String codeFor(HttpStatusCode statusCode) {
		HttpStatus status = HttpStatus.resolve(statusCode.value());
		if (status == null || status.is5xxServerError()) {
			return INTERNAL_ERROR;
		}
		return status.name().toLowerCase(Locale.ROOT);
	}

}
