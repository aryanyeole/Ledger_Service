package dev.aryanyeole.ledgerapi.payments;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;

import dev.aryanyeole.ledgerapi.idempotency.Fingerprint;
import dev.aryanyeole.ledgerapi.money.Money;

// Pins the fingerprint of the real payment request record (D-046). Adding, renaming or retyping
// a field changes request_hash: earlier keys then answer 422 for up to 24h. The fee rate is not a
// field, so it never reaches the hash. The hash was cross-checked outside Java:
//   printf 'POST\n/v1/payments\n{"amount":"100.00","customer_account_id":1,"merchant_account_id":2}' | sha256sum
class PaymentRequestFingerprintTest {

	@Test
	void canonicalJsonAndHash() {
		PaymentsController.PaymentRequest request = new PaymentsController.PaymentRequest(1L, 2L,
				new Money(new BigDecimal("100")));
		assertThat(Fingerprint.canonicalJson(request))
			.isEqualTo("{\"amount\":\"100.00\",\"customer_account_id\":1,\"merchant_account_id\":2}");
		assertThat(HexFormat.of().formatHex(Fingerprint.of("POST", PaymentsController.ROUTE, request)))
			.isEqualTo("8ed47965d26a106c309c25f77ea90ede0bc7815e83d1ba6af665e4b7e49b02a1");
	}

}
