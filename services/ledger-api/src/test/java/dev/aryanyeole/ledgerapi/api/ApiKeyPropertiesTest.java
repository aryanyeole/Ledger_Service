package dev.aryanyeole.ledgerapi.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

// ledger.api-keys is validated at startup (D-049): only SHA-256 hex digests, each with a client.
class ApiKeyPropertiesTest {

	private static final String HASH = "AC0C946C9691DE2B912C3724D6AA9F9F842F6B557ED70542FCAD05C90ED9F6B7";

	@Test
	void hashesAreNormalizedToLowercase() {
		assertThat(new ApiKeyProperties(Map.of(HASH, "client-alpha")).apiKeys())
			.containsExactly(Map.entry(HASH.toLowerCase(), "client-alpha"));
	}

	@Test
	void noKeysConfiguredIsEmpty() {
		assertThat(new ApiKeyProperties(null).apiKeys()).isEmpty();
	}

	@Test
	void plaintextKeyInsteadOfHashFailsStartup() {
		assertThatThrownBy(() -> new ApiKeyProperties(Map.of("my-api-key", "client-alpha")))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessageNotContaining("my-api-key");
	}

	@Test
	void blankClientIdFailsStartup() {
		assertThatThrownBy(() -> new ApiKeyProperties(Map.of(HASH, " "))).isInstanceOf(IllegalArgumentException.class);
	}

}
