package dev.aryanyeole.ledgerapi.transactions;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.http.HttpStatus;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// The keyset cursor for GET /v1/transactions (D-053): base64url, no padding, of
// "v1:<created_at in epoch microseconds>:<id>". Opaque to clients and unsigned: it only
// selects a position in a listing the caller can already read in full. Microseconds match
// Postgres' timestamptz precision, so a cursor never truncates a position and can't skip or
// repeat rows that are less than a millisecond apart.
public final class TransactionCursor {

	private static final Pattern PAYLOAD = Pattern.compile("v1:(-?[0-9]{1,19}):([0-9]{1,19})");

	private static final Pattern BASE64URL = Pattern.compile("[A-Za-z0-9_-]+");

	private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

	private TransactionCursor() {
	}

	/** A listing position: the row's created_at and id. */
	public record Position(OffsetDateTime createdAt, long id) {
	}

	public static String encode(OffsetDateTime createdAt, long id) {
		Instant instant = createdAt.toInstant();
		long micros = Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000);
		return ENCODER.encodeToString(("v1:" + micros + ":" + id).getBytes(StandardCharsets.US_ASCII));
	}

	/** The position, or 400 invalid_cursor. */
	public static Position decode(String cursor) {
		if (!BASE64URL.matcher(cursor).matches()) {
			throw invalid();
		}
		byte[] bytes;
		try {
			bytes = Base64.getUrlDecoder().decode(cursor);
		}
		catch (IllegalArgumentException ex) {
			throw invalid();
		}
		// Only the canonical encoding is accepted, so one position has exactly one cursor.
		if (!ENCODER.encodeToString(bytes).equals(cursor)) {
			throw invalid();
		}
		for (byte b : bytes) {
			if (b < 0x20) {
				throw invalid();
			}
		}
		Matcher matcher = PAYLOAD.matcher(new String(bytes, StandardCharsets.US_ASCII));
		if (!matcher.matches()) {
			throw invalid();
		}
		long micros;
		long id;
		try {
			micros = Long.parseLong(matcher.group(1));
			id = Long.parseLong(matcher.group(2));
		}
		catch (NumberFormatException ex) {
			throw invalid();
		}
		if (id <= 0) {
			throw invalid();
		}
		Instant instant = Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L),
				Math.floorMod(micros, 1_000_000L) * 1_000L);
		return new Position(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC), id);
	}

	private static ApiException invalid() {
		return new ApiException(HttpStatus.BAD_REQUEST, ProblemCode.INVALID_CURSOR,
				"cursor is not a valid transactions cursor.");
	}

}
