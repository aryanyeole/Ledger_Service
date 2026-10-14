package dev.aryanyeole.ledgerapi.api;

import java.sql.SQLException;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.MDC;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import dev.aryanyeole.ledgerapi.money.Money;

// Test-only endpoints for the API edge web-slice tests: there are no real endpoints until
// later M2 tasks. @TestComponent keeps it out of every component scan; slice tests @Import it.
public final class EdgeProbe {

	// Test-only key: never a real credential. Its SHA-256 is the only thing configured.
	public static final String API_KEY = "test-only-key-alpha";

	public static final String CLIENT_ID = "client-alpha";

	public static final String API_KEYS_PROPERTY = "ledger.api-keys.ac0c946c9691de2b912c3724d6aa9f9f842f6b557ed70542fcad05c90ed9f6b7="
			+ CLIENT_ID;

	// A second test-only key and client, for idempotency-key scoping across clients (T7).
	// Configure both properties in the test that needs it.
	public static final String API_KEY_BETA = "test-only-key-beta";

	public static final String CLIENT_ID_BETA = "client-beta";

	public static final String API_KEYS_PROPERTY_BETA = "ledger.api-keys.85c258c40df49a3dd9ed7175709d5fc29a20c2dcad97795829f879d6328b3f89="
			+ CLIENT_ID_BETA;

	// Parts of the failure behind /db-failure that must never reach a response body.
	public static final String SQL_STATE = "23514";

	public static final String SQL_TEXT = "UPDATE accounts SET balance = balance + ? WHERE id = ?";

	public static final String SQL_MESSAGE = "ERROR: new row for relation \"accounts\" violates check constraint \"no_overdraft\"";

	public static final String UNEXPECTED_MESSAGE = "internal detail that must stay server-side";

	private EdgeProbe() {
	}

	public record AmountRequest(Money amount) {
	}

	public record AmountResponse(Money amount) {
	}

	@TestComponent
	@RestController
	@RequestMapping("/v1/probe")
	public static class Controller {

		@GetMapping("/whoami")
		Map<String, String> whoami(HttpServletRequest request) {
			return Map.of("client_id", ApiKeyFilter.clientId(request), "correlation_id",
					MDC.get(CorrelationIdFilter.MDC_KEY));
		}

		@PostMapping("/amount")
		AmountResponse amount(@RequestBody AmountRequest request) {
			return new AmountResponse(request.amount());
		}

		@GetMapping("/db-failure")
		void dbFailure() {
			throw new DataIntegrityViolationException("PreparedStatementCallback; SQL [" + SQL_TEXT + "]; " + SQL_MESSAGE,
					new SQLException(SQL_MESSAGE, SQL_STATE));
		}

		@GetMapping("/unexpected")
		void unexpected() {
			throw new IllegalStateException(UNEXPECTED_MESSAGE);
		}

	}

}
