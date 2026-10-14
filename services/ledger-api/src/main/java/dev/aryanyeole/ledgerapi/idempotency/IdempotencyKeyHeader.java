package dev.aryanyeole.ledgerapi.idempotency;

import java.util.Optional;

import org.springframework.http.HttpStatus;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

// The Idempotency-Key header (D-046): 1 to 255 characters, each printable ASCII 0x21-0x7E.
// Required on deposits and transfers, optional on POST /v1/accounts. Checked before any
// transaction opens, so a 400 never touches the database. 255 keeps (client_id, key) well
// under the btree entry size limit.
public final class IdempotencyKeyHeader {

	public static final String HEADER = "Idempotency-Key";

	static final int MAX_LENGTH = 255;

	private IdempotencyKeyHeader() {
	}

	/** The validated key; 400 missing_idempotency_key if absent. */
	public static String require(String header) {
		if (header == null) {
			throw new ApiException(HttpStatus.BAD_REQUEST, ProblemCode.MISSING_IDEMPOTENCY_KEY,
					"The Idempotency-Key header is required.");
		}
		return validate(header);
	}

	/** The validated key, or empty if the header is absent. */
	public static Optional<String> optional(String header) {
		return (header == null) ? Optional.empty() : Optional.of(validate(header));
	}

	private static String validate(String header) {
		boolean valid = !header.isEmpty() && header.length() <= MAX_LENGTH
				&& header.chars().allMatch((c) -> c >= 0x21 && c <= 0x7E);
		if (!valid) {
			throw new ApiException(HttpStatus.BAD_REQUEST, ProblemCode.INVALID_IDEMPOTENCY_KEY,
					"The Idempotency-Key header must be 1 to 255 printable ASCII characters with no spaces.");
		}
		return header;
	}

}
