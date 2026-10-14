package dev.aryanyeole.ledgerapi.deposits;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;

import dev.aryanyeole.ledgerapi.idempotency.Fingerprint;
import dev.aryanyeole.ledgerapi.money.Money;

// Pins the fingerprint of the real deposit request record (D-046). Adding, renaming or
// retyping a field changes request_hash: earlier keys then answer 422 for up to 24h.
class DepositRequestFingerprintTest {

	@Test
	void canonicalJsonAndHash() {
		DepositsController.DepositRequest request = new DepositsController.DepositRequest(5L,
				new Money(new BigDecimal("10")));
		assertThat(Fingerprint.canonicalJson(request)).isEqualTo("{\"account_id\":5,\"amount\":\"10.00\"}");
		assertThat(HexFormat.of().formatHex(Fingerprint.of("POST", DepositsController.ROUTE, request)))
			.isEqualTo("c19bca52c6be82b9c1e7db2293dfc16d04f5f0b1aa4292bda86eb15babeb1132");
	}

}
