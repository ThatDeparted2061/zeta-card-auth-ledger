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
