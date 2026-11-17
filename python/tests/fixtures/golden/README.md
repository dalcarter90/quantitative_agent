# Golden order book files

The order book rebuilt by the Python reference reconstructor
(`quantagent.data.book`) after every step of each recorded fixture in
`../raw/`, and the points where it goes stale and recovers on damaged copies
of them. Phase 4 checks the C++ order book against these, step by step.

| File | Built from | Log id | Rows (steps) | Of which stream messages |
|------|------------|--------|--------------|--------------------------|
| `kraken-BTC-USD.top20.parquet` | `../raw/kraken/BTC-USD/20261111T030647970222Z.qaraw.zst` | `20261111T030647.970222600Z-164f377e87e8` | 13,785 | 13,782 |
| `coinbase-BTC-USD.top20.parquet` | `../raw/coinbase/BTC-USD/20261111T030647970222Z.qaraw.zst` | `20261111T030647.970222600Z-6fd8ece9ac35` | 2,705 | 2,702 |
| `integrity-points.json` | Damaged copies of both, built by `../../data/integrity.py` | | 8 cases | |

## Format

Parquet, zstd-compressed, one row per step in replay order (`log_id`, then
`seq`; ADR 0013), as `book.trace` yields them (ADR 0020). A step is a stream
message, a stored stream event that makes the book stale, or one that resets
it (a session start, connect or disconnect on the stream). A record's stored
events come before its own message. Independent check snapshots are not part
of the stream and have no row. On the clean fixtures the only steps other
than messages are the three resets of each.

| Column | Type | Meaning |
|--------|------|---------|
| `log_id` | string | The raw log the step came from (ADR 0013) |
| `seq` | uint64 | The raw record's number in its log |
| `recv_ts` | int64 | When that record was received: ns since the Unix epoch, wall clock. Information only, not an ordering key |
| `step` | string | `message`, `stale` or `reset` |
| `reason` | string, nullable | Why a `stale` or `reset` step happened (the C++ `StreamReason` in snake case: `sequence_gap`, `connect`, ...), or why a message made the book stale (`checksum_mismatch`, `sequence_repeat`, `sequence_out_of_order`, `delta_before_snapshot`). Null for a message that did not |
| `stale` | bool | The book is stale after the step |
| `recovered` | bool | The step is the snapshot that ended a stale period |
| `bid_price_00`, `bid_qty_00` | int64, nullable | The best bid after the step: price mantissa and quantity mantissa |
| `ask_price_00`, `ask_qty_00` | int64, nullable | The best ask after the step |
| `bid_price_NN` ... `ask_qty_NN` | int64, nullable | The level of rank `NN` (00 to 19, best first) on each side |

Columns run rank by rank after the first seven: `bid_price_00, bid_qty_00,
ask_price_00, ask_qty_00, bid_price_01, ...`, 87 columns in all. A rank the
book does not have is null on both price and quantity; every level is null
when the book cannot be read (stale, or no snapshot since the start or a
reset). Prices and quantities are int64 mantissas at the instrument's fixed
`price_exp` and `qty_exp` (`docs/data-core-spec.md`, section 2): 8 and 8 for
BTC-USD on both venues. Rank 00 is the top of book.

Kraken books are the subscribed 25 levels, cut to depth after every message;
Coinbase books are the full book. The file holds the top 20 of each.

## Integrity points

`integrity-points.json` (format 1) holds, for each damaged case, where the
reference book replayed in resume mode goes stale, resets and recovers. A
case is a fixture with a stream heartbeat replaced by a snapshot of the true
book (`damage.resnapshot`, so there is something to recover at), then one
book update removed, duplicated or swapped (`damage.py`), as in the Phase 2
gap tests. Each case lists `damage` (applied in order, record numbers of the
undamaged fixture), `stored_events` (false: replayed with no stored stream
events, so only the book's own checks apply), the counts of `steps`,
`messages` and `stale_messages` (not applied), and `points`: every step that
is not an ordinary message (stale and reset steps, a message with a
`reason`, and the `recovered` message), with `seq` in the damaged log's
numbering, `step`, `reason`, `stale` and `recovered`. The C++ book must go
stale and recover at exactly these points, and between them match the
reference's resume-mode book after every message.

## History

- ADR 0013 added the `log_id` column and moved `recv_ts` after `seq`. Every
  row, `seq`, `recv_ts` and level value is unchanged from the files it
  replaced, checked column by column when they were regenerated.
- ADR 0019 fixed each instrument's exponents. Prices were at Kraken's 1 and
  Coinbase's 2 decimals, and are now at 8: every price is the old one times
  10^7 (Kraken) or 10^6 (Coinbase). Every row, `log_id`, `seq`, `recv_ts` and
  quantity is unchanged, and every price is exactly the old one scaled,
  checked column by column when they were regenerated.
- ADR 0020 made the rows steps: the columns `step`, `reason`, `stale` and
  `recovered` after `recv_ts`, and a row for each of the stream's three
  resets. The message rows are exactly the previous files' rows in every
  previous column, checked when they were regenerated. `integrity-points.json`
  was added.

## Regenerating

Load a fixture into a store, then write its golden file:

```
python -m quantagent.data.adapters ../raw/kraken/BTC-USD/<file>.qaraw.zst --store <store>
python -m quantagent.data.book verify --store <store> --venue kraken --instrument BTC-USD
python -m quantagent.data.book golden --store <store> --venue kraken \
    --instrument BTC-USD --out kraken-BTC-USD.top20.parquet
```

`integrity-points.json` is written by `python python\tests\data\integrity.py`
from the repository root.

`python/tests/data/test_book.py` rebuilds both books on every test run and
fails if they differ from these files in any value; `test_book_resume.py` does
the same for the integrity points. Regenerate them only on purpose, when the
fixture or the reconstructor's documented behavior changes, and say why in
the pull request.
