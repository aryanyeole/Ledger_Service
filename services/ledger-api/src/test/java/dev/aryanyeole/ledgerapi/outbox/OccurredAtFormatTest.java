package dev.aryanyeole.ledgerapi.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.UUID;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

import dev.aryanyeole.ledgerapi.outbox.OutboxEnvelope.Row;

// The envelope format (D-097): occurred_at is created_at in UTC with exactly six fractional
// digits (trailing zeros kept, unlike ISO_INSTANT), the keys come in a fixed order, a missing
// correlation id is JSON null, data is the stored payload, and the same row always produces the
// same bytes, so a redelivered copy is identical.
class OccurredAtFormatTest {

	@ParameterizedTest(name = "{0} -> {1}")
	@CsvSource({ "2026-10-19T06:22:05.12Z, 2026-10-19T06:22:05.120000Z",
			"2026-10-19T06:22:05Z, 2026-10-19T06:22:05.000000Z",
			"2026-10-19T06:22:05.000001Z, 2026-10-19T06:22:05.000001Z",
			"2026-10-18T23:22:05.123456-07:00, 2026-10-19T06:22:05.123456Z" })
	void occurredAtIsUtcWithSixFractionalDigits(String createdAt, String occurredAt) {
		assertThat(OutboxEnvelope.occurredAt(OffsetDateTime.parse(createdAt))).isEqualTo(occurredAt);
	}

	@Test
	void sameRowGivesIdenticalBytesInFixedKeyOrder() {
		Row row = new Row(7, UUID.fromString("0f1e2d3c-4b5a-4987-8a6b-5c4d3e2f1a0b"), "ledger.payments.v1", "p-1",
				"payment.authorized", "{\"fee\": \"2.90\", \"amount\": \"100.00\", \"customer_account_id\": 12}", null,
				OffsetDateTime.parse("2026-10-19T06:22:05.12Z"));

		String first = OutboxEnvelope.build(row);
		String second = OutboxEnvelope.build(row);

		assertThat(first).isEqualTo("{\"event_id\":\"0f1e2d3c-4b5a-4987-8a6b-5c4d3e2f1a0b\","
				+ "\"event_type\":\"payment.authorized\",\"version\":1,\"occurred_at\":\"2026-10-19T06:22:05.120000Z\","
				+ "\"correlation_id\":null,\"data\":{\"fee\":\"2.90\",\"amount\":\"100.00\",\"customer_account_id\":12}}");
		assertThat(second.getBytes(StandardCharsets.UTF_8)).isEqualTo(first.getBytes(StandardCharsets.UTF_8));
	}

	@Test
	void correlationIdIsCarriedAsString() {
		Row row = new Row(8, UUID.fromString("0f1e2d3c-4b5a-4987-8a6b-5c4d3e2f1a0b"), "ledger.payments.v1", "p-1",
				"payment.settled", "{}", "abc-123", OffsetDateTime.parse("2026-10-19T06:22:05Z"));

		assertThat(OutboxEnvelope.build(row)).isEqualTo("{\"event_id\":\"0f1e2d3c-4b5a-4987-8a6b-5c4d3e2f1a0b\","
				+ "\"event_type\":\"payment.settled\",\"version\":1,\"occurred_at\":\"2026-10-19T06:22:05.000000Z\","
				+ "\"correlation_id\":\"abc-123\",\"data\":{}}");
	}

}
