// Shared helpers for the SPEC-003 load experiments (k6).
// Every scenario talks to the docker compose stack: API :8090, fake PSP :8082, fake PIX :8083.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

export const BASE = __ENV.BASE_URL || 'http://localhost:8090';
export const PSP_ADMIN = __ENV.PSP_ADMIN || 'http://localhost:8082/__admin';
export const PIX_ADMIN = __ENV.PIX_ADMIN || 'http://localhost:8083/__admin';
export const RUN = __ENV.RUN_ID || `${Date.now()}`;

/** POST → outcome (APPROVED/DECLINED/FAILED/UNKNOWN) as seen by polling GET. */
export const timeToOutcome = new Trend('time_to_outcome', true);
/** Provider calls beyond one per logical attempt. Must stay 0: "never a duplicate financial effect". */
export const extraProviderCalls = new Counter('extra_provider_calls');

const JSON_HEADERS = { 'Content-Type': 'application/json' };

export const MERCHANT = 'k6-merchant';

export function paymentBody(customerId, amount, method, merchantId) {
  return JSON.stringify({
    merchantId: merchantId || MERCHANT,
    customerId: customerId,
    amount: amount,
    currency: 'BRL',
    paymentMethod: method,
  });
}

/** POST /api/v1/payments with the Idempotency-Key. Tagged so thresholds can target it. */
export function createPayment(key, body, tags) {
  return http.post(`${BASE}/api/v1/payments`, body, {
    headers: Object.assign({ 'Idempotency-Key': key, 'X-Correlation-Id': `k6-${RUN}-${__VU}-${__ITER}` }, JSON_HEADERS),
    tags: Object.assign({ name: 'POST /payments' }, tags || {}),
  });
}

export function getPayment(id, tags) {
  return http.get(`${BASE}/api/v1/payments/${id}`, {
    tags: Object.assign({ name: 'GET /payments/{id}' }, tags || {}),
  });
}

const RESOLVED = ['APPROVED', 'DECLINED', 'FAILED', 'UNKNOWN'];

/**
 * Polls GET until the worker recorded an outcome (or maxMs). Records time_to_outcome measured
 * from `startedAt` (the POST). Returns the final status, or 'TIMEOUT'.
 */
export function waitForOutcome(id, startedAt, maxMs, tags) {
  const deadline = Date.now() + (maxMs || 30000);
  while (Date.now() < deadline) {
    const res = getPayment(id, tags);
    if (res.status === 200) {
      const status = res.json('status');
      if (RESOLVED.includes(status)) {
        timeToOutcome.add(Date.now() - startedAt, tags || {});
        return status;
      }
    }
    sleep(0.2);
  }
  return 'TIMEOUT';
}

/** Standard checks of the SPEC-003 contract for a brand-new payment. */
export function checkAccepted(res) {
  return check(res, {
    'POST → 202 Accepted': (r) => r.status === 202,
    'POST → Location header': (r) => !!r.headers['Location'],
    'POST → status PENDING (nothing charged yet)': (r) => r.status === 202 && r.json('status') === 'PENDING',
    'POST → no pspTransactionId yet': (r) => r.status === 202 && !r.json('pspTransactionId'),
  });
}

// ------------------------------------------------------------------ fake providers (WireMock)

function countRequests(admin, url) {
  // Only this merchant's calls: the synthetic probe runs concurrently with its own merchantId.
  const body = JSON.stringify({
    method: 'POST', url: url,
    bodyPatterns: [{ matchesJsonPath: `$[?(@.merchantId == '${MERCHANT}')]` }],
  });
  const res = http.post(`${admin}/requests/count`, body, { headers: JSON_HEADERS, tags: { name: 'wiremock count' } });
  return res.status === 200 ? res.json('count') : -1;
}

/** Calls received by the fake card PSP and the fake PIX provider (this merchant) since the last reset. */
export function providerCalls() {
  return {
    psp: countRequests(PSP_ADMIN, '/v1/charges'),
    pix: countRequests(PIX_ADMIN, '/v1/pix/payments'),
  };
}

export function resetProviderJournals() {
  http.del(`${PSP_ADMIN}/requests`, null, { tags: { name: 'wiremock reset' } });
  http.del(`${PIX_ADMIN}/requests`, null, { tags: { name: 'wiremock reset' } });
}

// ------------------------------------------------------------------ app metrics (Actuator)

/** COUNT of a Micrometer counter, optionally filtered by tag ("status:APPROVED"); 0 when absent. */
export function metricCount(name, tag) {
  const url = `${BASE}/actuator/metrics/${name}` + (tag ? `?tag=${encodeURIComponent(tag)}` : '');
  // 404 = the counter has no series for that tag yet (e.g. no DECLINED so far): expected, not an error.
  const res = http.get(url, { tags: { name: 'actuator' }, responseCallback: http.expectedStatuses(200, 404) });
  if (res.status !== 200) {
    return 0;
  }
  const m = res.json('measurements').find((x) => x.statistic === 'COUNT');
  return m ? m.value : 0;
}

/** Waits until the worker recorded `expected` more outcomes than `baseline`. Returns seconds taken. */
export function waitForBacklogDrain(baseline, expected, maxMs) {
  const start = Date.now();
  const deadline = start + maxMs;
  let done = 0;
  while (Date.now() < deadline) {
    done = metricCount('payments.outcome') - baseline;
    if (done >= expected) {
      break;
    }
    sleep(1);
  }
  return { seconds: (Date.now() - start) / 1000, processed: done };
}

export function outcomeBreakdown() {
  const out = {};
  for (const s of RESOLVED) {
    out[s] = metricCount('payments.outcome', `status:${s}`);
  }
  return out;
}
