package dev.aryanyeole.ledgerapi.transfers;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;

import dev.aryanyeole.ledgerapi.idempotency.Fingerprint;
import dev.aryanyeole.ledgerapi.money.Money;

// Pins the fingerprint of the real transfer request record (D-046). Adding, renaming or
// retyping a field changes request_hash: earlier keys then answer 422 for up to 24h.
class TransferRequestFingerprintTest {

	@Test
	void canonicalJsonAndHash() {
		TransfersController.TransferRequest request = new TransfersController.TransferRequest(1L, 2L,
				new Money(new BigDecimal("10")));
		assertThat(Fingerprint.canonicalJson(request))
			.isEqualTo("{\"amount\":\"10.00\",\"from_account_id\":1,\"to_account_id\":2}");
		assertThat(HexFormat.of().formatHex(Fingerprint.of("POST", TransfersController.ROUTE, request)))
			.isEqualTo("e84961bc2e77201c457659f1c35177c6c0639b05ca6e3c820fc0e972e3d14feb");
	}

}
