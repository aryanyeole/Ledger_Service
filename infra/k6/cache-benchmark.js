// The M5 cache benchmark's workload and stale judge (D-146, docs/design/cache-invalidation.md §8).
// One script for every mode; scripts/bench-cache.ps1 sets the mode on the services, not here.
//
// Each VU owns CUSTOMERS_PER_VU customers and one merchant (created in setup through the API) and is their
// only writer. Per iteration, after polling its pending payments: a weighted pick of a deposit to an owned
// customer, an authorize from an owned customer to its merchant, a judged read of an owned account, or an
// unjudged read of another VU's account (foreign reads drive populate races; a stale value they plant is
// caught by the owner's next judged read). Deterministic per SEED (mulberry32 per VU); random integers become
// BigInt minor units at once, so money is only ever a BigInt or the API's string (lib/judge.js).
//
// Thresholds: stale_miss and stale_bypass count == 0 in every mode (both read the DB). Mode B's positive
// control (stale_hit > 0) is asserted by the runner, so the modes stay one script.

import http from "k6/http";
import { Counter, Trend } from "k6/metrics";
import { formatMinor, isStale, parseMinor, VuLedger } from "./lib/judge.js";

const BASE_URL = __ENV.BASE_URL || "http://host.docker.internal:8080";
// Read once here: setup() can't read the exported options back (k6 doesn't hand them to the script).
const VUS = parseInt(__ENV.VUS || "2", 10);
const API_KEY = __ENV.API_KEY;
const SEED = parseInt(__ENV.SEED || "42", 10);
const CUSTOMERS_PER_VU = parseInt(__ENV.CUSTOMERS_PER_VU || "3", 10);
const START_BALANCE = __ENV.START_BALANCE || "100000.00";
const DECLINE_PERCENT = parseInt(__ENV.DECLINE_PERCENT || "20", 10);
const POLL_LIMIT = parseInt(__ENV.POLL_LIMIT || "10", 10);
const WEIGHTS = {
  deposit: parseInt(__ENV.W_DEPOSIT || "10", 10),
  authorize: parseInt(__ENV.W_AUTHORIZE || "15", 10),
  ownedRead: parseInt(__ENV.W_OWNED_READ || "50", 10),
  foreignRead: parseInt(__ENV.W_FOREIGN_READ || "25", 10),
};
// Approved amounts never use the fake processor's reserved cents: 13 (declines), 91 and 92 (D-112).
const APPROVED_CENTS = [0n, 5n, 10n, 25n, 50n, 75n, 99n];

export const options = {
  scenarios: {
    mix: {
      executor: "constant-vus",
      vus: VUS,
      duration: __ENV.DURATION || "30s",
    },
  },
  setupTimeout: "120s",
  thresholds: {
    stale_miss: ["count==0"],
    stale_bypass: ["count==0"],
  },
};

const reads = new Counter("reads");
const judgedReads = new Counter("judged_reads");
const staleHit = new Counter("stale_hit");
const staleMiss = new Counter("stale_miss");
const staleBypass = new Counter("stale_bypass");
const judgedIncomplete = new Counter("judged_incomplete");
const writes = new Counter("writes");
const settleWait = new Trend("settle_wait_ms", true);
const releaseWait = new Trend("release_wait_ms", true);

// A unique Idempotency-Key per write. __ITER doesn't exist in setup(), so a per-VU counter instead (__VU is 0 there).
let keyCounter = 0;

function headers(keyed) {
  const h = { "Content-Type": "application/json", "X-Api-Key": API_KEY };
  if (keyed) {
    keyCounter += 1;
    h["Idempotency-Key"] = `bench-${SEED}-${__VU}-${keyCounter}-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
  }
  return { headers: h, tags: { name: "api" } };
}

function post(path, body, keyed) {
  return http.post(`${BASE_URL}${path}`, JSON.stringify(body), headers(keyed));
}

function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function setup() {
  if (!API_KEY) {
    throw new Error("API_KEY is required");
  }
  const vus = VUS;
  const owned = [];
  for (let vu = 1; vu <= vus; vu++) {
    const customers = [];
    for (let i = 0; i < CUSTOMERS_PER_VU; i++) {
      const account = post("/v1/accounts", { name: `bench-${SEED}-${vu}-c${i}-${Date.now()}`, type: "CUSTOMER" }, false);
      if (account.status !== 201) {
        throw new Error(`create customer: ${account.status} ${account.body}`);
      }
      const id = account.json("account_id");
      const deposit = post("/v1/deposits", { account_id: id, amount: START_BALANCE }, true);
      if (deposit.status !== 201) {
        throw new Error(`fund customer: ${deposit.status} ${deposit.body}`);
      }
      customers.push(id);
    }
    const merchant = post("/v1/accounts", { name: `bench-${SEED}-${vu}-m-${Date.now()}`, type: "MERCHANT" }, false);
    if (merchant.status !== 201) {
      throw new Error(`create merchant: ${merchant.status} ${merchant.body}`);
    }
    owned.push({ vu, customers, merchant: merchant.json("account_id") });
  }
  console.log(`BENCH setup seed=${SEED} vus=${vus} customersPerVu=${CUSTOMERS_PER_VU} startBalance=${START_BALANCE} ` +
    `declinePercent=${DECLINE_PERCENT} weights=${JSON.stringify(WEIGHTS)}`);
  return { owned };
}

let state = null;

function init(data) {
  const mine = data.owned.find((o) => o.vu === __VU);
  const ledger = new VuLedger();
  const start = parseMinor(START_BALANCE);
  for (const c of mine.customers) {
    ledger.open(c, start);
  }
  ledger.open(mine.merchant, 0n);
  const foreign = [];
  for (const o of data.owned) {
    if (o.vu !== __VU) {
      foreign.push(...o.customers, o.merchant);
    }
  }
  return { ledger, mine, foreign, rng: mulberry32(SEED * 7919 + __VU) };
}

function pick(rng, list) {
  return list[Math.floor(rng() * list.length)];
}

function pollPending() {
  let polled = 0;
  for (const [paymentId, payment] of state.ledger.pending) {
    if (polled++ >= POLL_LIMIT) {
      break;
    }
    const res = http.get(`${BASE_URL}/v1/payments/${paymentId}`, headers(false));
    if (res.status !== 200) {
      continue;
    }
    const status = res.json("status");
    if (status === "SETTLED" || status === "FAILED") {
      state.ledger.terminal(paymentId, status);
      (status === "SETTLED" ? settleWait : releaseWait).add(Date.now() - payment.at);
    }
  }
}

function deposit() {
  const customer = pick(state.rng, state.mine.customers);
  const amount = BigInt(1 + Math.floor(state.rng() * 100)) * 100n + pick(state.rng, APPROVED_CENTS);
  const res = post("/v1/deposits", { account_id: customer, amount: formatMinor(amount) }, true);
  writes.add(1, { op: "deposit", status: String(res.status) });
  if (res.status === 201) {
    state.ledger.deposited(customer, amount);
  }
}

function authorize() {
  const customer = pick(state.rng, state.mine.customers);
  const declined = Math.floor(state.rng() * 100) < DECLINE_PERCENT;
  const cents = declined ? 13n : pick(state.rng, APPROVED_CENTS);
  const amount = BigInt(1 + Math.floor(state.rng() * 50)) * 100n + cents;
  if (amount > state.ledger.expectedOf(customer)) {
    return deposit();
  }
  const res = post("/v1/payments", {
    customer_account_id: customer, merchant_account_id: state.mine.merchant, amount: formatMinor(amount),
  }, true);
  writes.add(1, { op: "authorize", status: String(res.status) });
  if (res.status === 201) {
    state.ledger.authorized(res.json("payment_id"), customer, state.mine.merchant, amount,
      parseMinor(res.json("fee")), declined, Date.now());
  }
}

function read(account, judged) {
  const res = http.get(`${BASE_URL}/v1/accounts/${account}/balance`, headers(false));
  const xCache = res.headers["X-Cache"] || "none";
  reads.add(1, { x_cache: xCache, judged: String(judged) });
  if (!judged) {
    return;
  }
  if (res.status !== 200) {
    judgedIncomplete.add(1, { status: String(res.status) });
    return;
  }
  judgedReads.add(1, { x_cache: xCache });
  if (isStale(res.json("balance"), state.ledger.expectedOf(account))) {
    const counter = xCache === "hit" ? staleHit : xCache === "miss" ? staleMiss : staleBypass;
    counter.add(1, { x_cache: xCache });
  }
}

function ownedRead() {
  const accounts = [...state.mine.customers, state.mine.merchant];
  const account = pick(state.rng, accounts);
  const judged = account === state.mine.merchant
    ? state.ledger.judgeMerchant(account)
    : state.ledger.judgeCustomer(account);
  read(account, judged);
}

function foreignRead() {
  if (state.foreign.length > 0) {
    read(pick(state.rng, state.foreign), false);
  }
}

export default function (data) {
  if (state === null) {
    state = init(data);
    // Registers the thresholds' metrics, so count==0 is checked even when nothing is ever stale.
    staleMiss.add(0);
    staleBypass.add(0);
    staleHit.add(0);
  }
  pollPending();
  const total = WEIGHTS.deposit + WEIGHTS.authorize + WEIGHTS.ownedRead + WEIGHTS.foreignRead;
  let roll = Math.floor(state.rng() * total);
  if ((roll -= WEIGHTS.deposit) < 0) {
    deposit();
  } else if ((roll -= WEIGHTS.authorize) < 0) {
    authorize();
  } else if ((roll -= WEIGHTS.ownedRead) < 0) {
    ownedRead();
  } else {
    foreignRead();
  }
}
