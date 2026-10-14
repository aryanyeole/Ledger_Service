package dev.aryanyeole.ledgerapi.api;

import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

// API keys (D-049): ledger.api-keys maps the lowercase hex SHA-256 of each key to its
// client_id. Only hashes are configured, never keys; no keys are committed outside test
// config. With no keys configured every authenticated request is rejected.
@ConfigurationProperties("ledger")
public record ApiKeyProperties(Map<String, String> apiKeys) {

	private static final int SHA256_HEX_LENGTH = 64;

	public ApiKeyProperties {
		Map<String, String> normalized = new LinkedHashMap<>();
		if (apiKeys != null) {
			apiKeys.forEach((hash, clientId) -> {
				String key = hash.toLowerCase(Locale.ROOT);
				if (key.length() != SHA256_HEX_LENGTH || !key.chars().allMatch(HexFormat::isHexDigit)) {
					throw new IllegalArgumentException(
							"ledger.api-keys: every key must be a 64-character hex SHA-256 digest");
				}
				if (clientId == null || clientId.isBlank()) {
					throw new IllegalArgumentException("ledger.api-keys: every hash needs a non-blank client_id");
				}
				normalized.put(key, clientId);
			});
		}
		apiKeys = Map.copyOf(normalized);
	}

}
