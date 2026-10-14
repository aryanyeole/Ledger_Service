package dev.aryanyeole.ledgerapi.accounts;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import dev.aryanyeole.ledgerapi.idempotency.Fingerprint;

// Pins the canonical JSON of the account-creation request record (D-046).
class CreateAccountRequestFingerprintTest {

	@Test
	void canonicalJson() {
		assertThat(Fingerprint.canonicalJson(new AccountsController.CreateAccountRequest("Ann", "CUSTOMER")))
			.isEqualTo("{\"name\":\"Ann\",\"type\":\"CUSTOMER\"}");
		assertThat(Fingerprint.canonicalJson(new AccountsController.CreateAccountRequest("Ann", null)))
			.isEqualTo("{\"name\":\"Ann\",\"type\":null}");
	}

}
