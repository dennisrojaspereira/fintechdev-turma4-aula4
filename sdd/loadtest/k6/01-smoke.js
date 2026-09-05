// Experimento 1 — Smoke: o caminho feliz de ponta a ponta, uma request de cada vez.
// POST → 202 PENDING → Debezium → worker → provedor PIX → APPROVED visível no GET.
import { check } from 'k6';
import { BASE, RUN, paymentBody, createPayment, waitForOutcome, checkAccepted, providerCalls, resetProviderJournals } from './lib.js';

export const options = {
  scenarios: {
    smoke: { executor: 'per-vu-iterations', vus: 1, iterations: 10, maxDuration: '2m' },
  },
  thresholds: {
    'checks': ['rate==1'],
    'http_req_duration{name:POST /payments}': ['p(95)<300'],
    'time_to_outcome': ['p(95)<5000'],
    'extra_provider_calls': ['count==0'],
  },
};

export function setup() {
  resetProviderJournals();
  return { started: Date.now() };
}

export default function () {
  const key = `smoke-${RUN}-${__VU}-${__ITER}`;
  const method = __ITER % 2 === 0 ? 'PIX' : 'CREDIT_CARD';
  const t0 = Date.now();
  const res = createPayment(key, paymentBody('k6-customer', 10 + __ITER, method), { method: method });
  checkAccepted(res);
  if (res.status !== 202) {
    return;
  }
  const id = res.json('id');
  const outcome = waitForOutcome(id, t0, 20000, { method: method });
  check(outcome, {
    'worker recorded APPROVED': (o) => o === 'APPROVED',
  });
}

export function teardown() {
  const calls = providerCalls();
  // 10 payments: 5 PIX + 5 cards → exactly 5 calls on each fake provider.
  check(calls, {
    'PIX provider called exactly 5 times': (c) => c.pix === 5,
    'card PSP called exactly 5 times': (c) => c.psp === 5,
  });
  console.log(`provider calls after 10 payments: PSP=${calls.psp} PIX=${calls.pix} (target ${BASE})`);
}
