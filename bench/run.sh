#!/usr/bin/env bash
# Reproduces the headline:
#   1. p99 authorisation latency at a fixed rate (k6, constant arrival rate),
#      then the end-of-day proof on the resulting ledger;
#   2. the chaos run: SIGKILLs mid-transaction, duplicate sends, replays,
#      and the end-of-day proof again.
# Needs Go, k6 and a local Postgres (DATABASE_ADMIN_URL).
set -euo pipefail
cd "$(dirname "$0")/.."

TPS=${TPS:-1000}
DURATION=${DURATION:-2m}
CHAOS_FOR=${CHAOS_FOR:-2m}
ADMIN=${DATABASE_ADMIN_URL:-postgres://localhost:5432/postgres?sslmode=disable}
OUT=bench/results
mkdir -p bin "$OUT/logs"
go build -o bin/ ./cmd/ledger ./cmd/chaos

db() { echo "${ADMIN%/*}/$1?${ADMIN#*\?}"; }  # same server and options, another database
fresh() { psql "$ADMIN" -qc "DROP DATABASE IF EXISTS $1 WITH (FORCE)" -c "CREATE DATABASE $1"; }

echo "== latency: $TPS authorisations/s for $DURATION"
fresh card_ledger_bench
bin/ledger -addr :8060 -db "$(db card_ledger_bench)" -max-conns 48 >"$OUT/logs/ledger.log" 2>&1 &
pid=$!
trap 'kill $pid 2>/dev/null || true' EXIT
for _ in $(seq 1 300); do curl -sf localhost:8060/healthz >/dev/null && break; sleep 0.1; done
curl -sf localhost:8060/healthz >/dev/null || { echo "ledger did not start:"; cat "$OUT/logs/ledger.log"; exit 1; }
k6 run -q -e BASE=http://localhost:8060 -e TPS="$TPS" -e DURATION="$DURATION" -e OUT="$OUT/k6.json" k6/auth-load.js >/dev/null || true
curl -s localhost:8060/v1/eod >"$OUT/eod-after-load.json"
kill $pid; wait $pid 2>/dev/null || true

echo "== chaos for $CHAOS_FOR"
fresh card_ledger_chaos
bin/chaos -bin bin/ledger -db "$(db card_ledger_chaos)" -duration "$CHAOS_FOR" -out "$OUT/chaos.json" >/dev/null

k=$OUT/k6.json c=$OUT/chaos.json e=$OUT/eod-after-load.json
{
  echo "**Latency** ($(jq .authorizations $k) authorisations, constant $(jq .tps $k)/s, with captures and reversals on top)"
  echo
  echo "| p50 | p95 | p99 | max | approved | dropped iterations |"
  echo "|---:|---:|---:|---:|---:|---:|"
  echo "| $(jq '.p50_ms*100|floor/100' $k) ms | $(jq '.p95_ms*100|floor/100' $k) ms | **$(jq '.p99_ms*100|floor/100' $k) ms** | $(jq '.max_ms*10|floor/10' $k) ms | $(jq '.checks_passed*1000|floor/10' $k)% | $(jq .dropped_iterations $k) |"
  echo
  echo "End-of-day after the load: $(jq .entries $e) entries, $(jq .postings $e) postings, sum of all postings **$(jq .sum_of_all_postings $e)**, balanced: $(jq .balanced $e)."
  echo
  echo "**Chaos** ($(jq '.duration_seconds|floor' $c) s)"
  echo
  echo "| | |"
  echo "|---|---:|"
  echo "| Service killed with SIGKILL mid-traffic | $(jq .service_kills $c) times |"
  echo "| Client requests (idempotency keys) | $(jq .requests $c) |"
  echo "| HTTP attempts incl. retries and duplicates | $(jq .http_attempts $c) |"
  echo "| Retries while the service was down | $(jq .retries_after_errors $c) |"
  echo "| Requests sent twice at once | $(jq .duplicate_sends $c) |"
  echo "| Old requests replayed at the end | $(jq .replayed_requests $c) |"
  echo "| Responses served from the idempotency store | $(jq .responses_marked_replay $c) |"
  echo "| Authorisations / approved / captures / reversals / refunds | $(jq .authorizations $c) / $(jq .approved $c) / $(jq .captures $c) / $(jq .reversals $c) / $(jq .refunds $c) |"
  echo "| Duplicates or replays that got a different answer | $(( $(jq .duplicate_mismatches $c) + $(jq .replay_mismatches $c) )) |"
  echo "| Authorisations differing from what the client was told | **$(jq .authorization_state_mismatches $c)** |"
  echo "| Ledger changed by the replays | $(jq 'if .ledger_unchanged_by_replay then "no" else "yes" end' -r $c) |"
  echo "| End-of-day: sum of all postings | **$(jq .end_of_day.sum_of_all_postings $c)** |"
  echo "| End-of-day: unbalanced entries, balance mismatches, double postings | $(jq .end_of_day.unbalanced_entries $c), $(jq .end_of_day.account_balance_mismatches $c), $(jq .end_of_day.duplicate_operations $c) |"
} | tee bench/RESULTS.md
jq -e .ok $c >/dev/null && jq -e .balanced $e >/dev/null
