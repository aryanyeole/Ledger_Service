package dev.aryanyeole.ledgerapi.outbox;

import java.util.Map;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

// Appends an event to the transactional outbox (spec §6.2, D-008, D-077). Callers run inside the
// write transaction that makes the state change, so the event exists iff the change commits;
// nothing here talks to Kafka (invariant 5). event_id, created_at and published_at come from the
// database. The relay (T4) publishes the row later.
//
// payload and headers are persisted and read by the Python worker, so their format must not drift
// with the app's JSON configuration (same reasoning as the request fingerprint, D-046). Callers
// pass maps with literal snake_case keys and already-rendered scalar values (UUIDs and amounts as
// strings, ids as longs), and they are written by a plain mapper built here: the app mapper's
// naming strategy and Money serializer never touch them.
@Component
public class OutboxWriter {

	private static final JsonMapper PLAIN = JsonMapper.builder().build();

	private final JdbcClient jdbc;

	OutboxWriter(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	public void append(String topic, String messageKey, String eventType, Map<String, Object> payload,
			Map<String, String> headers) {
		this.jdbc
			.sql("INSERT INTO outbox (topic, message_key, event_type, payload, headers) "
					+ "VALUES (:topic, :key, :type, :payload::jsonb, :headers::jsonb)")
			.param("topic", topic)
			.param("key", messageKey)
			.param("type", eventType)
			.param("payload", PLAIN.writeValueAsString(payload))
			.param("headers", PLAIN.writeValueAsString(headers))
			.update();
	}

}
