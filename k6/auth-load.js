// p99 authorisation latency at a fixed rate: constant-arrival-rate keeps
// TPS steady regardless of latency, so the percentile is measured at the
// rate that is claimed. Each iteration is one authorisation; 60% are then
// captured and 20% reversed, as background ledger load.
//
//   k6 run -e BASE=http://localhost:8060 -e TPS=1000 -e DURATION=2m k6/auth-load.js
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE || 'http://localhost:8060';
const TPS = parseInt(__ENV.TPS || '1000');
const CARDS = parseInt(__ENV.CARDS || '2000');

export const options = {
  setupTimeout: '5m',
  scenarios: {
    authorizations: {
      executor: 'constant-arrival-rate',
      rate: TPS,
      timeUnit: '1s',
      duration: __ENV.DURATION || '2m',
      preAllocatedVUs: Math.max(50, TPS / 4),
      maxVUs: TPS * 2,
    },
  },
  thresholds: {
    'http_req_duration{op:authorize}': ['p(99)<50'],
    'checks': ['rate>0.999'],
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const json = (key, tag) => ({ headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, tags: { op: tag } });

// setup() runs once and hands its data to every VU: per-VU init code runs at
// different moments, so a run id computed there would differ between VUs.
export function setup() {
  const RUN = `${Date.now()}`;
  for (let i = 0; i < CARDS; i++) {
    const card = { id: `k6-${RUN}-${i}`, credit_limit: 1e9, per_txn_limit: 50000, daily_limit: 1e9, velocity_per_min: 100000 };
    const r = http.post(`${BASE}/v1/cards`, JSON.stringify(card), json(`k6-${RUN}-card-${i}`, 'setup'));
    if (r.status !== 201) throw new Error(`card setup failed: ${r.status} ${r.body}`);
  }
  return { run: RUN };
}

export default function (data) {
  const RUN = data.run;
  const key = `k6-${RUN}-${__VU}-${__ITER}`;
  const card = `k6-${RUN}-${Math.floor(Math.random() * CARDS)}`;
  const amount = 100 + Math.floor(Math.random() * 20000);
  const r = http.post(`${BASE}/v1/authorizations`, JSON.stringify({ card_id: card, merchant: 'k6-shop', amount }), json(key, 'authorize'));
  const ok = check(r, { 'authorized': (x) => x.status === 201 && x.json('status') === 'approved' });
  if (!ok) return;
  const id = r.json('id');
  const x = Math.random();
  if (x < 0.6) {
    http.post(`${BASE}/v1/authorizations/${id}/captures`, JSON.stringify({ amount }), json(key + '-cap', 'capture'));
  } else if (x < 0.8) {
    http.post(`${BASE}/v1/authorizations/${id}/reversals`, '{}', json(key + '-rev', 'reverse'));
  }
}

export function handleSummary(data) {
  const d = data.metrics['http_req_duration{op:authorize}'];
  const out = {
    tps: TPS,
    authorizations: data.metrics['iterations'].values.count,
    p50_ms: d ? d.values.med : null,
    p95_ms: d ? d.values['p(95)'] : null,
    p99_ms: d ? d.values['p(99)'] : null,
    max_ms: d ? d.values.max : null,
    checks_passed: data.metrics['checks'].values.rate,
    dropped_iterations: data.metrics['dropped_iterations'] ? data.metrics['dropped_iterations'].values.count : 0,
  };
  return { stdout: JSON.stringify(out, null, 2) + '\n', [__ENV.OUT || 'bench/results/k6.json']: JSON.stringify(out, null, 2) };
}
