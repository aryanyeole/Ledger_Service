package dev.aryanyeole.ledgerapi.payments;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import dev.aryanyeole.ledgerapi.api.CorrelationIdFilter;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.api.StoredOutcomeException;
import dev.aryanyeole.ledgerapi.idempotency.IdempotencyService.Outcome;
import dev.aryanyeole.ledgerapi.money.Money;
import dev.aryanyeole.ledgerapi.outbox.OutboxWriter;
import dev.aryanyeole.ledgerapi.payments.PaymentRepository.Payment;
import dev.aryanyeole.ledgerapi.posting.PostingService;
import dev.aryanyeole.ledgerapi.posting.PostingService.Leg;
import dev.aryanyeole.ledgerapi.posting.PostingService.LockedAccount;
import dev.aryanyeole.ledgerapi.posting.PostingService.LockedAccounts;

// Authorize (spec §6.2): runs inside the idempotency service's transaction and savepoint
// (D-061, D-064), so the payment row, the PAYMENT_AUTHORIZE posting and the outbox row commit
// together with the key and its response (invariants 1, 5, 7), or not at all.
// Lock order (D-075, invariant 4): the customer, merchant and ESCROW rows are locked in one
// ascending FOR UPDATE before anything else touches an accounts row. The ESCROW id lookup before
// it is a plain read and takes no lock. The payment insert must come after the lock: its FKs take
// KEY SHARE on the customer and merchant, which outside the ordered pass cycles with settlements
// and transfers.
// Stored outcomes (D-045): 404 account_not_found (from lock), 422 account_not_payable (D-094),
// 422 insufficient_funds. The fee is computed once, here (D-074).
@Service
class PaymentService {

	static final String EVENT_AUTHORIZED = "payment.authorized";

	private final PostingService postings;

	private final PaymentRepository payments;

	private final OutboxWriter outbox;

	private final FeeCalculator fees;

	private final String topic;

	PaymentService(PostingService postings, PaymentRepository payments, OutboxWriter outbox, FeeCalculator fees,
			PaymentsProperties properties) {
		this.postings = postings;
		this.payments = payments;
		this.outbox = outbox;
		this.fees = fees;
		this.topic = properties.topic();
	}

	/** One representation for create and read (D-057, D-088). */
	record PaymentResponse(UUID paymentId, String status, long customerAccountId, long merchantAccountId,
			Money amount, Money fee, OffsetDateTime createdAt, OffsetDateTime updatedAt) {

		static PaymentResponse of(Payment payment) {
			return new PaymentResponse(payment.id(), payment.status(), payment.customerAccountId(),
					payment.merchantAccountId(), new Money(payment.amount()), new Money(payment.fee()),
					payment.createdAt(), payment.updatedAt());
		}

	}

	Outcome authorize(long customer, long merchant, Money amount) {
		long escrow = this.postings.escrowAccountId();
		LockedAccounts locked = this.postings.lock(List.of(customer, merchant, escrow));
		if (!locked.get(customer).type().equals("CUSTOMER") || !locked.get(merchant).type().equals("MERCHANT")) {
			throw new StoredOutcomeException(HttpStatus.UNPROCESSABLE_CONTENT, ProblemCode.ACCOUNT_NOT_PAYABLE,
					"Payments move money from a CUSTOMER account to a MERCHANT account.");
		}
		Money fee = this.fees.feeFor(amount);
		LockedAccount payer = locked.get(customer);
		if (payer.balance().compareTo(amount.amount()) < 0) {
			// Same detail as PostingService's check, so the stored body is the same whichever fires.
			throw new StoredOutcomeException(HttpStatus.UNPROCESSABLE_CONTENT, ProblemCode.INSUFFICIENT_FUNDS,
					"Insufficient funds in the source account.");
		}

		Payment payment = this.payments.insert(customer, merchant, amount.amount(), fee.amount());
		String paymentId = payment.id().toString();
		String correlationId = MDC.get(CorrelationIdFilter.MDC_KEY);
		this.postings.post(locked, "PAYMENT_AUTHORIZE",
				List.of(new Leg(customer, amount.amount().negate()), new Leg(escrow, amount.amount())), paymentId,
				correlationId);
		Map<String, String> headers = new LinkedHashMap<>();
		headers.put("correlation_id", correlationId);
		this.outbox.append(this.topic, paymentId, EVENT_AUTHORIZED, authorizedPayload(payment, amount, fee), headers);
		return new Outcome(HttpStatus.CREATED, PaymentResponse.of(payment));
	}

	Optional<PaymentResponse> find(UUID id) {
		return this.payments.find(id).map(PaymentResponse::of);
	}

	// D-095: literal keys and rendered values, independent of the app mapper (OutboxWriter).
	private static Map<String, Object> authorizedPayload(Payment payment, Money amount, Money fee) {
		Map<String, Object> payload = new LinkedHashMap<>();
		payload.put("payment_id", payment.id().toString());
		payload.put("customer_account_id", payment.customerAccountId());
		payload.put("merchant_account_id", payment.merchantAccountId());
		payload.put("amount", amount.toString());
		payload.put("fee", fee.toString());
		return payload;
	}

}
