package dev.aryanyeole.ledgerapi.outbox;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.cfg.JsonNodeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

// The Kafka record value for an outbox row (D-097):
//   {"event_id", "event_type", "version": 1, "occurred_at", "correlation_id", "data"}
// in that order. Built only from the row, so publishing the same row again (the relay's
// at-least-once seam) produces byte-identical copies. occurred_at is the row's created_at in UTC
// with exactly six fractional digits, a fixed width for the Python parser (ISO_INSTANT would
// drop trailing zeros). data is the stored payload. A plain mapper built here, not the app's, so
// the format can't drift with app JSON config (D-046, D-095); floats (none are expected: money is
// a string) parse as BigDecimal and keep their digits, never pass through a double.
final class OutboxEnvelope {

	static final int VERSION = 1;

	private static final DateTimeFormatter OCCURRED_AT = DateTimeFormatter
		.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS'Z'");

	private static final JsonMapper PLAIN = JsonMapper.builder()
		.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
		.disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
		.build();

	private OutboxEnvelope() {
	}

	/** One outbox row as the relay reads it. payload is payload::text. */
	record Row(long id, UUID eventId, String topic, String messageKey, String eventType, String payload,
			String correlationId, OffsetDateTime createdAt) {
	}

	static String build(Row row) {
		ObjectNode envelope = PLAIN.createObjectNode();
		envelope.put("event_id", row.eventId().toString());
		envelope.put("event_type", row.eventType());
		envelope.put("version", VERSION);
		envelope.put("occurred_at", occurredAt(row.createdAt()));
		if (row.correlationId() != null) {
			envelope.put("correlation_id", row.correlationId());
		}
		else {
			envelope.putNull("correlation_id");
		}
		envelope.set("data", PLAIN.readTree(row.payload()));
		return PLAIN.writeValueAsString(envelope);
	}

	static String occurredAt(OffsetDateTime createdAt) {
		return createdAt.withOffsetSameInstant(ZoneOffset.UTC).format(OCCURRED_AT);
	}

}
