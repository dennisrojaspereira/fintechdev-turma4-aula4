// Synthetic monitoring probe: one PIX and one card payment, POST → GET until the worker records
// the outcome. Run every SYNTHETIC_INTERVAL seconds by the `synthetic` compose service and
// remote-written to Prometheus (testid=synthetic); Grafana alerts when a probe fails.
// It never resets the fake providers' journals and uses its own merchantId, so the load-test
// scenarios (which count provider calls for merchant k6-merchant) are not disturbed.
import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import { RUN, paymentBody, createPayment, waitForOutcome } from '../k6/lib.js';

const ok = new Rate('synthetic_ok');
const timeToOutcome = new Trend('synthetic_time_to_outcome', true);
const MERCHANT = 'synthetic-probe';

export const options = {
  scenarios: {
    probe: { executor: 'per-vu-iterations', vus: 1, iterations: 2, maxDuration: '45s' },
  },
  thresholds: {
    'synthetic_ok': ['rate==1'],
    'synthetic_time_to_outcome': ['p(95)<15000'],
  },
};

export default function () {
  const probe = __ITER === 0 ? 'pix' : 'card';
  const method = probe === 'pix' ? 'PIX' : 'CREDIT_CARD';
  const key = `synthetic-${RUN}-${probe}`;
  const t0 = Date.now();

  const res = createPayment(key, paymentBody('synthetic-customer', 1.00, method, MERCHANT), { probe: probe });
  const accepted = check(res, { 'probe: POST → 202 PENDING': (r) => r.status === 202 && r.json('status') === 'PENDING' });
  if (!accepted) {
    ok.add(false, { probe: probe });
    return;
  }

  const outcome = waitForOutcome(res.json('id'), t0, 30000, { probe: probe });
  const approved = check(outcome, { 'probe: worker recorded APPROVED': (o) => o === 'APPROVED' });
  ok.add(approved, { probe: probe });
  if (approved) {
    timeToOutcome.add(Date.now() - t0, { probe: probe });
  }
}
