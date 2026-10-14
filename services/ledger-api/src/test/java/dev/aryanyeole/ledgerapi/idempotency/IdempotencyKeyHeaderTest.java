package dev.aryanyeole.ledgerapi.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// Idempotency-Key limits (D-046): 1-255 characters, each 0x21-0x7E.
class IdempotencyKeyHeaderTest {

	@Test
	void boundsAreAccepted() {
		assertThat(IdempotencyKeyHeader.require("!")).isEqualTo("!");
		assertThat(IdempotencyKeyHeader.require("~".repeat(255))).hasSize(255);
		assertThat(IdempotencyKeyHeader.optional("a-B_9.~")).contains("a-B_9.~");
	}

	@Test
	void missingIsRequiredButOptionalIsEmpty() {
		assertThatThrownBy(() -> IdempotencyKeyHeader.require(null)).isInstanceOfSatisfying(ApiException.class,
				(ex) -> assertThat(ex.code()).isEqualTo(ProblemCode.MISSING_IDEMPOTENCY_KEY));
		assertThat(IdempotencyKeyHeader.optional(null)).isEmpty();
	}

	@ParameterizedTest
	@ValueSource(strings = { "", " ", "a b", "tab\tkey", "del\u007fkey", "café", "nul\u0000", "あ" })
	void outOfRangeCharactersAreInvalid(String key) {
		assertInvalid(key);
	}

	@Test
	void tooLongIsInvalid() {
		assertInvalid("k".repeat(256));
	}

	private static void assertInvalid(String key) {
		assertThatThrownBy(() -> IdempotencyKeyHeader.require(key)).isInstanceOfSatisfying(ApiException.class,
				(ex) -> assertThat(ex.code()).isEqualTo(ProblemCode.INVALID_IDEMPOTENCY_KEY));
		assertThatThrownBy(() -> IdempotencyKeyHeader.optional(key)).isInstanceOfSatisfying(ApiException.class,
				(ex) -> assertThat(ex.code()).isEqualTo(ProblemCode.INVALID_IDEMPOTENCY_KEY));
	}

}
