package dev.aryanyeole.ledgerapi.idempotency;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.fasterxml.jackson.annotation.JsonInclude;

import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;

import dev.aryanyeole.ledgerapi.money.Money;
import dev.aryanyeole.ledgerapi.money.MoneySerializer;

// Request fingerprint (D-046): SHA-256 over UTF-8 "METHOD\nroute\ncanonical-json". The
// canonical JSON is the validated request record, so it hashes the meaning, not the bytes:
// "10" and "10.00" are the same Money, and key order or whitespace never reach the hash.
//
// The mapper is built here from scratch, with every setting explicit, never derived from
// the app's mapper: request_hash is persisted, so this format must not drift when the app's
// JSON configuration changes. FingerprintTest pins it by exact string and fixed hash.
// Changing the format makes a client's in-flight retry of an earlier request fail with 422
// idempotency_key_reused for up to the 24h key TTL.
public final class Fingerprint {

	private static final JsonMapper CANONICAL = JsonMapper.builder()
		.propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
		.enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
		.disable(MapperFeature.SORT_CREATOR_PROPERTIES_FIRST)
		.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
		.disable(SerializationFeature.INDENT_OUTPUT)
		.changeDefaultPropertyInclusion(
				(inclusion) -> JsonInclude.Value.construct(JsonInclude.Include.ALWAYS, JsonInclude.Include.ALWAYS))
		.addModule(new SimpleModule("canonical-money").addSerializer(Money.class, new MoneySerializer()))
		.build();

	private Fingerprint() {
	}

	/** The canonical JSON of a validated request record. */
	public static String canonicalJson(Object request) {
		return CANONICAL.writeValueAsString(request);
	}

	/** The 32-byte request_hash. */
	public static byte[] of(String method, String route, Object request) {
		String input = method + "\n" + route + "\n" + canonicalJson(request);
		try {
			return MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8));
		}
		catch (NoSuchAlgorithmException ex) {
			throw new IllegalStateException("SHA-256 is required on every Java platform", ex);
		}
	}

}
