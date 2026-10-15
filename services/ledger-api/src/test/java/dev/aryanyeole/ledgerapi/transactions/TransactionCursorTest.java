package dev.aryanyeole.ledgerapi.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Base64;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// The keyset cursor format (D-053): base64url without padding of "v1:<epoch µs>:<id>".
class TransactionCursorTest {

	private static String raw(String payload) {
		return Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.US_ASCII));
	}

	@Test
	void encodesEpochMicrosAndId() {
		OffsetDateTime createdAt = OffsetDateTime.parse("2026-01-01T00:00:00.000600Z");
		assertThat(TransactionCursor.encode(createdAt, 42)).isEqualTo(raw("v1:1767225600000600:42"));
	}

	@Test
	void nonZeroMicrosecondsRoundTripExactly() {
		OffsetDateTime createdAt = OffsetDateTime.parse("2026-10-14T21:03:07.123457+02:00");
		TransactionCursor.Position position = TransactionCursor.decode(TransactionCursor.encode(createdAt, 7));
		assertThat(position.createdAt()).isEqualTo(createdAt.toInstant().atOffset(java.time.ZoneOffset.UTC));
		assertThat(position.createdAt().toInstant()).isEqualTo(createdAt.toInstant());
		assertThat(position.id()).isEqualTo(7);
	}

	@Test
	void preEpochMicrosRoundTrip() {
		OffsetDateTime createdAt = OffsetDateTime.parse("1969-12-31T23:59:59.999999Z");
		assertThat(TransactionCursor.decode(TransactionCursor.encode(createdAt, 1)).createdAt().toInstant())
			.isEqualTo(createdAt.toInstant());
	}

	@ParameterizedTest
	@ValueSource(strings = { "", "not base64!", "djE6MTox==", "abc+def", "abc/def" })
	void malformedEncodingIsInvalid(String cursor) {
		assertInvalid(cursor);
	}

	@ParameterizedTest
	@ValueSource(strings = { "v2:1:1", "v1:1", "v1:1:1:1", "v1:x:1", "v1:1:x", "v1:1:0", "v1:1:-1", "v1: 1:1",
			"v1:99999999999999999999:1", "v1:1:99999999999999999999", "V1:1:1", "v1:1:1\n" })
	void malformedPayloadIsInvalid(String payload) {
		assertInvalid(raw(payload));
	}

	// A 7-byte payload leaves 2 unused bits in the last base64 character. Flipping one of them
	// decodes to the same bytes, but it isn't the canonical encoding, so it is rejected: one
	// position has exactly one cursor.
	@Test
	void nonCanonicalEncodingIsInvalid() {
		String canonical = raw("v1:10:5");
		char last = canonical.charAt(canonical.length() - 1);
		String tweaked = canonical.substring(0, canonical.length() - 1) + (char) (last + 1);
		assertThat(Base64.getUrlDecoder().decode(tweaked)).isEqualTo(Base64.getUrlDecoder().decode(canonical));
		assertInvalid(tweaked);
	}

	private static void assertInvalid(String cursor) {
		assertThatThrownBy(() -> TransactionCursor.decode(cursor)).isInstanceOfSatisfying(ApiException.class,
				(ex) -> assertThat(ex.code()).isEqualTo(ProblemCode.INVALID_CURSOR));
	}

}
