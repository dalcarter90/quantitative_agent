# A check snapshot taken between two stream batches

One recorded raw log (`.qaraw.zst`, see `quantagent.data.rawlog`) holding
exactly what Coinbase sent. It contains public market data only. No API key
was used to record it and none is stored in it.

| Venue | Instrument | File | Recorded (UTC) | Length |
|-------|------------|------|----------------|--------|
| coinbase | BTC-USD | `coinbase/BTC-USD/20261113T012133447743Z.qaraw.zst` | 2026-11-13 01:21:33 | 60 s |

Recorded with `quantagent.data.recorder` (recorder version 3) for 60 s: one
stream connection, and one probe connection opened 25.6 s in for the check
snapshot. It is a version 2 raw log (ADR 0012): every record has a monotonic
receive stamp, and the log records the measured clock offset (+11.2 ms) and
the clock identity. It was recorded alongside a Kraken log for ADR 0012's
verification; only the Coinbase log is kept here.

## Why it is here

Its one check snapshot is the case ADR 0014 settles. The snapshot's venue time
falls 29.1 ms after one stream batch (A) and 26.8 ms before the next (B). It
differs from the book rebuilt after A on 55 of its 37,746 levels, and every one
of those 55 is among the 93 levels B lists: 50 already show B's value, 5 show a
value the stream never sent. Every other level matches exactly. Under the
Phase 2 rule this was a mismatch; under ADR 0014 it is "explained", and
`python/tests/data/test_check_snapshots.py` checks that the adapter and the
reconstructor both say so, with the same counts and ranks.

The committed fixtures in `../raw/` have check snapshots taken on a batch time,
which are exact.
