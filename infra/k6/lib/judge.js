// The cache benchmark's stale judge (D-146, docs/design/cache-invalidation.md §8.3).
//
// Money is BigInt minor units parsed from the API's own strings: exactly "-?digits.dd", split on ".".
// No parseFloat, no Number(...) and no float arithmetic on money anywhere (invariant 3, D-011); a string
// with more or fewer than two fraction digits is refused, never rounded.
//
// VuLedger is one VU's view of the accounts it alone writes. A customer is judged always, except between
// a declined authorize and the VU seeing that payment FAILED (the release's refund lands at an unknown
// time). A merchant is judged only once every authorize to it has been seen terminal (SETTLED credits it
// amount - fee, from the authorize response's fee, never recomputed). ESCROW and PLATFORM_FEES are never
// judged: every VU writes them.

const MONEY = /^(-?)(\d+)\.(\d{2})$/;

export function parseMinor(text) {
  if (typeof text !== "string") {
    throw new Error(`money must be a string, got ${typeof text}`);
  }
  const match = MONEY.exec(text);
  if (match === null) {
    throw new Error(`not a two-decimal money string: ${JSON.stringify(text)}`);
  }
  const value = BigInt(match[2]) * 100n + BigInt(match[3]);
  return match[1] === "-" ? -value : value;
}

export function formatMinor(value) {
  if (typeof value !== "bigint") {
    throw new Error(`minor units must be a BigInt, got ${typeof value}`);
  }
  const negative = value < 0n;
  const magnitude = negative ? -value : value;
  const units = magnitude / 100n;
  const cents = (magnitude % 100n).toString().padStart(2, "0");
  return `${negative ? "-" : ""}${units}.${cents}`;
}

export function isStale(servedText, expectedMinor) {
  return parseMinor(servedText) !== expectedMinor;
}

export class VuLedger {
  constructor() {
    this.expected = new Map(); // account id -> BigInt minor units
    this.pending = new Map(); // payment id -> { customer, merchant, amount, fee, declined, at }
    this.blockedCustomers = new Map(); // customer -> count of declines not yet seen FAILED
    this.openMerchants = new Map(); // merchant -> count of authorizes not yet seen terminal
  }

  open(account, startingMinor) {
    this.expected.set(account, startingMinor);
  }

  expectedOf(account) {
    if (!this.expected.has(account)) {
      throw new Error(`account ${account} is not owned by this VU`);
    }
    return this.expected.get(account);
  }

  deposited(customer, amountMinor) {
    this.expected.set(customer, this.expectedOf(customer) + amountMinor);
  }

  // The authorize debits the customer at once, approved or declined.
  authorized(paymentId, customer, merchant, amountMinor, feeMinor, declined, at) {
    this.expected.set(customer, this.expectedOf(customer) - amountMinor);
    this.pending.set(paymentId, { customer, merchant, amount: amountMinor, fee: feeMinor, declined, at });
    this.openMerchants.set(merchant, (this.openMerchants.get(merchant) || 0) + 1);
    if (declined) {
      this.blockedCustomers.set(customer, (this.blockedCustomers.get(customer) || 0) + 1);
    }
  }

  // A terminal status seen through GET /v1/payments/{id}. Returns the pending record, or null.
  terminal(paymentId, status) {
    const payment = this.pending.get(paymentId);
    if (payment === undefined) {
      return null;
    }
    if (status === "SETTLED") {
      this.expected.set(payment.merchant, this.expectedOf(payment.merchant) + payment.amount - payment.fee);
    } else if (status === "FAILED") {
      this.expected.set(payment.customer, this.expectedOf(payment.customer) + payment.amount);
      this.blockedCustomers.set(payment.customer, this.blockedCustomers.get(payment.customer) - 1);
    } else {
      throw new Error(`not a terminal status: ${status}`);
    }
    this.openMerchants.set(payment.merchant, this.openMerchants.get(payment.merchant) - 1);
    this.pending.delete(paymentId);
    return payment;
  }

  judgeCustomer(customer) {
    return (this.blockedCustomers.get(customer) || 0) === 0;
  }

  judgeMerchant(merchant) {
    return (this.openMerchants.get(merchant) || 0) === 0;
  }
}
