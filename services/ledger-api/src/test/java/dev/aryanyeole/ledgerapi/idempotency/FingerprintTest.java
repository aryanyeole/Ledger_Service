package dev.aryanyeole.ledgerapi.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.aryanyeole.ledgerapi.money.Money;

// Pins the canonical JSON format and the fingerprint (D-046). request_hash is persisted:
// if one of these strings or hashes changes, in-flight retries of earlier requests start
// failing with 422 idempotency_key_reused for up to the 24h key TTL. Change deliberately.
class FingerprintTest {

	record Probe(String zeta, Long alphaBeta, Money amount, Map<String, Object> details) {
	}

	@Test
	void canonicalJsonIsSortedSnakeCaseWithNullsAndNoWhitespace() {
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("z", 1);
		details.put("a", null);
		assertThat(Fingerprint.canonicalJson(new Probe(null, 7L, new Money(new BigDecimal("0.1")), details)))
			.isEqualTo("{\"alpha_beta\":7,\"amount\":\"0.10\",\"details\":{\"a\":null,\"z\":1},\"zeta\":null}");
	}

	@Test
	void stringsAreEscapedAndUtf8IsKept() {
		record Name(String name) {
		}
		assertThat(Fingerprint.canonicalJson(new Name("Café \"q\" \\ 東")))
			.isEqualTo("{\"name\":\"Café \\\"q\\\" \\\\ 東\"}");
	}

	@Test
	void equalMoneyGivesEqualFingerprints() {
		record Deposit(Long accountId, Money amount) {
		}
		byte[] ten = Fingerprint.of("POST", "/v1/deposits", new Deposit(5L, new Money(new BigDecimal("10"))));
		byte[] tenPointZeroZero = Fingerprint.of("POST", "/v1/deposits",
				new Deposit(5L, new Money(new BigDecimal("10.00"))));
		assertThat(ten).hasSize(32).isEqualTo(tenPointZeroZero);
	}

	// SHA-256 of UTF-8 "POST\n/v1/deposits\n{\"account_id\":5,\"amount\":\"10.00\"}".
	@Test
	void fingerprintIsSha256OfMethodRouteAndCanonicalJson() {
		record Deposit(Long accountId, Money amount) {
		}
		byte[] hash = Fingerprint.of("POST", "/v1/deposits", new Deposit(5L, new Money(new BigDecimal("10.00"))));
		assertThat(HexFormat.of().formatHex(hash))
			.isEqualTo("c19bca52c6be82b9c1e7db2293dfc16d04f5f0b1aa4292bda86eb15babeb1132");
	}

	@Test
	void methodAndRouteArePartOfTheFingerprint() {
		record Deposit(Long accountId, Money amount) {
		}
		Deposit deposit = new Deposit(5L, new Money(new BigDecimal("10.00")));
		byte[] deposits = Fingerprint.of("POST", "/v1/deposits", deposit);
		assertThat(Fingerprint.of("POST", "/v1/transfers", deposit)).isNotEqualTo(deposits);
		assertThat(Fingerprint.of("PUT", "/v1/deposits", deposit)).isNotEqualTo(deposits);
	}

}
