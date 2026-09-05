// Experimento 4 — Throughput: a API aceita mais rápido do que o worker processa.
// Chegada constante de RATE req/s por DURATION com chaves únicas. A API tem de segurar a taxa com
// p95 baixo e 0 erros; depois medimos quanto tempo o worker (concurrency 1) leva para drenar a
// fila. A diferença entre "aceito" e "processado" é a fila do Kafka: visível, durável, sem perda.
import { check } from 'k6';
import { Trend, Gauge } from 'k6/metrics';
import { RUN, paymentBody, createPayment, checkAccepted, metricCount, waitForBacklogDrain, outcomeBreakdown, resetProviderJournals, providerCalls, extraProviderCalls } from './lib.js';

const RATE = Number(__ENV.RATE || 10);
const DURATION = __ENV.DURATION || '20s';

const drainSeconds = new Trend('backlog_drain_seconds');
const backlogAtEnd = new Gauge('backlog_at_end_of_load');

export const options = {
  scenarios: {
    load: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 20, maxVUs: 100,
    },
  },
  thresholds: {
    'checks': ['rate==1'],
    'http_req_failed': ['rate<0.01'],
    'http_req_duration{name:POST /payments}': ['p(95)<300', 'p(99)<800'],
    'dropped_iterations': ['count==0'],  // k6 kept the arrival rate: the API never pushed back
    'extra_provider_calls': ['count==0'],
  },
};

export function setup() {
  resetProviderJournals();
  return {
    acceptedBaseline: metricCount('payments.accepted'),
    outcomeBaseline: metricCount('payments.outcome'),
  };
}

export default function () {
  const key = `load-${RUN}-${__VU}-${__ITER}`;
  const method = __ITER % 3 === 0 ? 'CREDIT_CARD' : 'PIX';
  const res = createPayment(key, paymentBody('k6-customer', 5 + (__ITER % 50), method), { method: method });
  checkAccepted(res);
}

export function teardown(data) {
  const accepted = metricCount('payments.accepted') - data.acceptedBaseline;
  const processedAtEnd = metricCount('payments.outcome') - data.outcomeBaseline;
  backlogAtEnd.add(accepted - processedAtEnd);
  console.log(`load finished: accepted=${accepted} processed=${processedAtEnd} backlog=${accepted - processedAtEnd}`);

  const drain = waitForBacklogDrain(data.outcomeBaseline, accepted, 180000);
  drainSeconds.add(drain.seconds);
  const calls = providerCalls();
  const total = calls.psp + calls.pix;
  extraProviderCalls.add(Math.max(0, total - accepted));
  check(drain, {
    'every accepted payment got an outcome': (d) => d.processed >= accepted,
  });
  check(calls, {
    'provider calls == accepted payments': () => total === accepted,
  });
  console.log(`drained ${drain.processed}/${accepted} in ${drain.seconds}s (~${(drain.processed / Math.max(drain.seconds, 1)).toFixed(1)}/s); provider calls PSP=${calls.psp} PIX=${calls.pix}; outcomes=${JSON.stringify(outcomeBreakdown())}`);
}
