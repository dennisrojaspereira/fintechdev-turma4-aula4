// Experimento 3 — Idempotência sob concorrência.
// 20 VUs martelam um pool de 25 Idempotency-Keys durante 30 s (a mesma chave chega repetida e
// em paralelo), e 1 em cada 6 requests reusa a chave com outro valor (conflito).
// Invariante: 25 pagamentos, 25 chamadas ao provedor no total, nunca uma a mais.
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { RUN, paymentBody, createPayment, providerCalls, resetProviderJournals, metricCount, waitForBacklogDrain, extraProviderCalls } from './lib.js';

const POOL = 25;
const acceptedNew = new Counter('accepted_new_202');
const replayed = new Counter('replayed_200');
const conflicts = new Counter('conflict_422');

export const options = {
  scenarios: {
    hammer: { executor: 'constant-vus', vus: 20, duration: '30s' },
  },
  thresholds: {
    'checks': ['rate==1'],
    'http_req_duration{name:POST /payments}': ['p(95)<300'],
    'accepted_new_202': [`count==${POOL}`],   // exactly one 202 per logical attempt
    'replayed_200': ['count>100'],             // and plenty of replays to prove it
    'conflict_422': ['count>10'],
    'extra_provider_calls': ['count==0'],
  },
};

export function setup() {
  resetProviderJournals();
  return { outcomeBaseline: metricCount('payments.outcome') };
}

function keyFor(index) {
  return `idem-${RUN}-${index}`;
}

export default function () {
  const index = (__VU * 7 + __ITER) % POOL;
  const method = index % 2 === 0 ? 'PIX' : 'CREDIT_CARD';
  const conflict = __ITER % 6 === 5;
  const amount = conflict ? 999 : 50 + index; // same key, different body → 422
  const res = createPayment(keyFor(index), paymentBody('k6-customer', amount, method), { kind: conflict ? 'conflict' : 'same-body' });

  if (conflict) {
    check(res, { 'same key + different body → 422': (r) => r.status === 422 });
    if (res.status === 422) conflicts.add(1);
    return;
  }
  const ok = check(res, {
    'same key + same body → 202 (first) or 200 (replay)': (r) => r.status === 202 || r.status === 200,
    'replay never creates another id': (r) => r.status !== 200 || !!r.json('id'),
  });
  if (!ok) return;
  if (res.status === 202) acceptedNew.add(1);
  if (res.status === 200) replayed.add(1);
}

export function teardown(data) {
  // Let the worker finish the 25 payments, then count what the fake providers actually saw.
  const drain = waitForBacklogDrain(data.outcomeBaseline, POOL, 90000);
  const calls = providerCalls();
  const total = calls.psp + calls.pix;
  const extra = Math.max(0, total - POOL);
  extraProviderCalls.add(extra);
  check(calls, {
    'provider calls == 25 distinct keys (one financial effect per attempt)': () => total === POOL,
  });
  console.log(`processed ${drain.processed}/${POOL} in ${drain.seconds}s; provider calls PSP=${calls.psp} PIX=${calls.pix} total=${total} extra=${extra}`);
}
