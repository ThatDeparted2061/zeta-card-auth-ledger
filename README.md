**p99 authorisation latency of 57 ms at a constant 1,000 authorisations/s (p50 4 ms), on a double-entry ledger that sums to exactly zero. It still sums to zero after a chaos run that SIGKILLed the service 19 times mid-transaction and sent 30,967 requests twice at once: every authorisation matches what its client was told, with zero double postings.**

# card-auth-ledger

[![CI](https://github.com/ThatDeparted2061/zeta-card-auth-ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/ThatDeparted2061/zeta-card-auth-ledger/actions/workflows/ci.yml)

Card authorisation for an issuer, the kind of system Zeta runs for banks. It covers holds, full and partial captures, reversals and refunds on a double-entry ledger in Postgres. It must be fast, and it must never post money twice, however often the card network retries.

## Results

`make demo` (Go, k6, a local Postgres):

**Latency** (120001 authorisations, constant 1000/s, with captures and reversals on top)

| p50 | p95 | p99 | max | approved | dropped iterations |
|---:|---:|---:|---:|---:|---:|
| 3.98 ms | 25.65 ms | **57.24 ms** | 1744.5 ms | 99.9% | 0 |

End-of-day after the load: 218151 entries, 436302 postings, sum of all postings **0**, balanced: true.

**Chaos** (120 s)

| | |
|---|---:|
| Service killed with SIGKILL mid-traffic | 19 times |
| Client requests (idempotency keys) | 155746 |
| HTTP attempts incl. retries and duplicates | 193364 |
| Retries while the service was down | 4651 |
| Requests sent twice at once | 30967 |
| Old requests replayed at the end | 2000 |
| Responses served from the idempotency store | 33267 |
| Authorisations / approved / captures / reversals / refunds | 67091 / 60984 / 48645 / 27623 / 12087 |
| Duplicates or replays that got a different answer | 0 |
| Authorisations differing from what the client was told | **0** |
| Ledger changed by the replays | no |
| End-of-day: sum of all postings | **0** |
| End-of-day: unbalanced entries, balance mismatches, double postings | 0, 0, 0 |

## How money can't be posted twice

**Every write carries an `Idempotency-Key`.** The key, the ledger entry and the response are committed **in one Postgres transaction**:

| What happens | Result |
|---|---|
| The service dies before `COMMIT` | Nothing was kept. The retry runs the operation for the first time. |
| The service dies after `COMMIT`, before replying | The retry finds the key and returns the stored response, byte for byte (`Idempotent-Replay: true`). |
| The network delivers the same request twice at once | The second insert of the key waits on the first transaction's unique-index lock, then replays its response. |
| The same key is used for a different request | `422 idempotency_key_reused` (the request hash differs). |

Declines and business errors (`exceeds_hold`, `card_not_found`) are stored and replayed like successes, so a retry always gets the same answer. Only `503` ("not applied, retry") is not stored.

**The database refuses an unbalanced entry.** Every movement is one `entries` row with postings that net to zero. A *deferred constraint trigger* re-adds each entry's postings at `COMMIT` and raises an error if they don't sum to 0, so no code path, present or future, can commit an unbalanced entry. `CHECK` constraints keep an authorisation honest too: captured + reversed ≤ amount, and refunded ≤ captured.

```
accounts per card: available (open-to-buy), held (authorised), posted (captured, owed)
open card    credit_line -L   available +L
hold A       available   -A   held      +A
capture C    held        -C   posted    +C        (partial captures: several Cs)
reversal R   held        -R   available +R        (release the rest of the hold)
refund F     posted      -F   available +F
```

**Limits under a lock.** An authorisation locks the card row (`SELECT … FOR UPDATE`). Concurrent authorisations on one card therefore queue, and none can slip past a limit. The checks are, in order: card blocked, per-transaction limit, velocity (authorisations in the last minute), daily spend (holds less reversals since midnight), and available balance. A decline is a `201` with `status: declined` and `decline_reason`.

**End-of-day proof** (`GET /v1/eod`). It checks that all postings sum to 0, that no entry is unbalanced or has no postings, that every stored balance equals the sum of its postings, and that each card's available + held + posted equals its credit limit. Held and posted must also agree with the authorisation records, and no client request may have produced two entries.

## API

```bash
curl -X POST localhost:8060/v1/cards -H 'Idempotency-Key: c-1' \
  -d '{"id":"card-1","credit_limit":100000,"per_txn_limit":50000,"daily_limit":80000,"velocity_per_min":10}'
curl -X POST localhost:8060/v1/authorizations -H 'Idempotency-Key: a-1' -d '{"card_id":"card-1","merchant":"cafe","amount":4200}'
curl -X POST localhost:8060/v1/authorizations/<id>/captures  -H 'Idempotency-Key: a-1-cap' -d '{"amount":3000}'   # partial
curl -X POST localhost:8060/v1/authorizations/<id>/reversals -H 'Idempotency-Key: a-1-rev'                       # release the rest
curl -X POST localhost:8060/v1/authorizations/<id>/refunds   -H 'Idempotency-Key: a-1-ref' -d '{"amount":1000}'
curl localhost:8060/v1/cards/card-1/balance   # {"available":98000,"held":0,"posted":2000}
curl localhost:8060/v1/eod                    # {"sum_of_all_postings":0,...,"balanced":true}
```

Amounts are integers in minor units (paise).

## The chaos run

`cmd/chaos` starts the service as a child process and runs 48 clients doing authorise, capture (full or partial), reverse and refund. Each request is retried with the same key until it gets a definitive answer, as a card network does. Meanwhile it:

- `SIGKILL`s the service every 3-9 s, mid-transaction, and restarts it;
- sends 20% of requests twice at once and requires both copies to get identical answers;
- replays 2,000 old requests at the end and requires identical answers and an unchanged ledger.

It then checks every authorisation in the database against what its client was last told, and runs the end-of-day proof.

## Run it

```bash
make demo         # k6 at 1,000 TPS for 2 min, then 2 min of chaos; writes bench/RESULTS.md
make demo-quick   # 300 TPS, 30 s each
make up           # docker compose: Postgres, the ledger, k6
make test         # flows, declines, 20 concurrent duplicates, the balance trigger (needs Postgres)
```

## Layout

| Path | |
|---|---|
| `internal/ledger/schema.sql` | Tables, `CHECK` constraints, the deferred balance trigger |
| `internal/ledger/ledger.go` | Idempotency protocol, holds, captures, reversals, refunds, limits |
| `internal/ledger/eod.go` | End-of-day proof |
| `internal/api` | HTTP API, latency histograms (`/metrics`) |
| `cmd/chaos`, `k6/auth-load.js` | Chaos runner, constant-arrival-rate load |

## Limits

- One Postgres primary, `synchronous_commit=on`: a ledger should not acknowledge what it could lose. The p99 is dominated by commit fsyncs and lock waits on hot cards. Sharding cards across databases is the scale-out path.
- Velocity and daily limits use the authorisation table with a `(card_id, created_at)` index. At very high per-card rates, a rolling counter row per card would be cheaper.
- There is no authentication on the API. In production it sits behind the network's mTLS.
