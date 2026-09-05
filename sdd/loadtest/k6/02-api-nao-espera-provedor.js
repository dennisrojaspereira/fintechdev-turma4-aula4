// Experimento 2 — "A request não espera o provedor" (INTENT-003).
// Metade dos pagamentos vai para um PSP que demora 10 s (customer-timeout) e metade para um
// provedor normal. A latência do POST tem de ser a mesma nos dois casos; o desfecho lento vira
// UNKNOWN (read timeout do worker) sem nunca ser FAILED, e o rápido vira APPROVED.
import { check } from 'k6';
import { RUN, paymentBody, createPayment, waitForOutcome, checkAccepted, providerCalls, resetProviderJournals } from './lib.js';

export const options = {
  scenarios: {
    fast_provider: {
      executor: 'per-vu-iterations', vus: 3, iterations: 4, maxDuration: '3m',
      exec: 'fast', tags: { provider_speed: 'fast' },
    },
    slow_provider: {
      executor: 'per-vu-iterations', vus: 3, iterations: 2, maxDuration: '3m',
      exec: 'slow', tags: { provider_speed: 'slow' },
    },
  },
  thresholds: {
    'checks': ['rate==1'],
    // The point of the intent: the client never pays for the provider's latency.
    'http_req_duration{name:POST /payments,provider_speed:fast}': ['p(95)<300'],
    'http_req_duration{name:POST /payments,provider_speed:slow}': ['p(95)<300'],
    'time_to_outcome{provider_speed:fast}': ['p(95)<15000'],
    'time_to_outcome{provider_speed:slow}': ['p(95)<60000'],
    'extra_provider_calls': ['count==0'],
  },
};

export function setup() {
  resetProviderJournals();
}

export function fast() {
  const key = `fast-${RUN}-${__VU}-${__ITER}`;
  const t0 = Date.now();
  const res = createPayment(key, paymentBody('k6-customer', 20, 'PIX'), { provider_speed: 'fast' });
  checkAccepted(res);
  if (res.status !== 202) {
    return;
  }
  const outcome = waitForOutcome(res.json('id'), t0, 60000, { provider_speed: 'fast' });
  check(outcome, { 'fast provider → APPROVED': (o) => o === 'APPROVED' });
}

export function slow() {
  const key = `slow-${RUN}-${__VU}-${__ITER}`;
  const t0 = Date.now();
  // customer-timeout: the fake PSP answers APPROVED only after 10 s; the worker gives up at
  // its read timeout and records UNKNOWN. The charge may exist: never FAILED, never retried.
  const res = createPayment(key, paymentBody('customer-timeout', 30, 'CREDIT_CARD'), { provider_speed: 'slow' });
  checkAccepted(res);
  if (res.status !== 202) {
    return;
  }
  const outcome = waitForOutcome(res.json('id'), t0, 90000, { provider_speed: 'slow' });
  check(outcome, {
    'slow provider → UNKNOWN (not FAILED)': (o) => o === 'UNKNOWN',
  });
}

export function teardown() {
  const calls = providerCalls();
  check(calls, {
    'PIX provider: exactly 12 calls (3 VUs × 4)': (c) => c.pix === 12,
    'card PSP: exactly 6 calls, no retry on read timeout (3 VUs × 2)': (c) => c.psp === 6,
  });
  console.log(`provider calls: PSP=${calls.psp} (slow, 1 per payment) PIX=${calls.pix}`);
}
