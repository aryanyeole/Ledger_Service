// The judge's unit test, run by k6 itself in grafana/k6:2.3.0 (no HTTP):
//   docker run --rm -v "${PWD}\infra\k6:/scripts:ro" grafana/k6:2.3.0 run /scripts/lib/judge.test.js
// Exact BigInt cases, refused sub-unit strings and the VuLedger's judging rules. Any failed check fails the run.

import { check } from "k6";
import { formatMinor, isStale, parseMinor, VuLedger } from "./judge.js";

export const options = { vus: 1, iterations: 1, thresholds: { checks: ["rate==1"] } };

function throws(fn) {
  try {
    fn();
    return false;
  } catch (e) {
    return true;
  }
}

export default function () {
  check(null, {
    "0.01 is 1n": () => parseMinor("0.01") === 1n,
    "1000000.00 is 100000000n": () => parseMinor("1000000.00") === 100000000n,
    "0.00 is 0n": () => parseMinor("0.00") === 0n,
    "-250.50 is -25050n": () => parseMinor("-250.50") === -25050n,
    "a large balance stays exact": () => parseMinor("90071992547409.93") === 9007199254740993n,
    "format 1n": () => formatMinor(1n) === "0.01",
    "format 100000000n": () => formatMinor(100000000n) === "1000000.00",
    "format -25050n": () => formatMinor(-25050n) === "-250.50",
    "format 0n": () => formatMinor(0n) === "0.00",
    "round trip": () => formatMinor(parseMinor("12345.67")) === "12345.67",
    "add and subtract exactly": () => parseMinor("0.10") + parseMinor("0.20") === parseMinor("0.30"),
    "a sub-unit string is refused": () => throws(() => parseMinor("1.234")),
    "one decimal is refused": () => throws(() => parseMinor("1.2")),
    "no fraction is refused": () => throws(() => parseMinor("1")),
    "an exponent is refused": () => throws(() => parseMinor("1e2")),
    "an empty string is refused": () => throws(() => parseMinor("")),
    "a Number is refused": () => throws(() => parseMinor(12.3)),
    "formatting a Number is refused": () => throws(() => formatMinor(5)),
    "stale iff served != expected": () => isStale("10.00", 1000n) === false && isStale("10.01", 1000n) === true,
  });

  const ledger = new VuLedger();
  ledger.open(1, parseMinor("100.00"));
  ledger.open(2, 0n);
  ledger.deposited(1, parseMinor("5.00"));
  ledger.authorized("p1", 1, 2, parseMinor("10.00"), parseMinor("0.29"), false, 0);
  const afterApprovedAuthorize = {
    "the authorize debits the customer": () => ledger.expectedOf(1) === parseMinor("95.00"),
    "a customer with only an approved authorize is judged": () => ledger.judgeCustomer(1),
    "a merchant with an open authorize is not judged": () => !ledger.judgeMerchant(2),
  };
  check(null, afterApprovedAuthorize);
  ledger.terminal("p1", "SETTLED");
  check(null, {
    "SETTLED credits the merchant amount - fee": () => ledger.expectedOf(2) === parseMinor("9.71"),
    "the merchant is judged once its authorizes are terminal": () => ledger.judgeMerchant(2),
  });
  ledger.authorized("p2", 1, 2, parseMinor("12.13"), parseMinor("0.35"), true, 0);
  check(null, {
    "a declined authorize blocks the customer": () => !ledger.judgeCustomer(1),
    "and opens the merchant": () => !ledger.judgeMerchant(2),
  });
  ledger.terminal("p2", "FAILED");
  check(null, {
    "FAILED refunds the customer": () => ledger.expectedOf(1) === parseMinor("95.00"),
    "and unblocks it": () => ledger.judgeCustomer(1),
    "a release leaves the merchant unchanged": () => ledger.expectedOf(2) === parseMinor("9.71"),
    "an unknown payment is ignored": () => ledger.terminal("nope", "SETTLED") === null,
    "a non-terminal status is refused": () => {
      ledger.authorized("p3", 1, 2, parseMinor("1.00"), parseMinor("0.03"), false, 0);
      return throws(() => ledger.terminal("p3", "AUTHORIZED"));
    },
    "an account the VU doesn't own is refused": () => throws(() => ledger.expectedOf(99)),
  });
}
