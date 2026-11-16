# Golden order book files

The order book rebuilt by the Python reference reconstructor
(`quantagent.data.book`) after every stream message of each recorded fixture
in `../raw/`. Phase 4 checks the C++ order book against these, message by
message.

| File | Built from | Log id | Rows (stream messages) |
|------|------------|--------|------------------------|
| `kraken-BTC-USD.top20.parquet` | `../raw/kraken/BTC-USD/20261111T030647970222Z.qaraw.zst` | `20261111T030647.970222600Z-164f377e87e8` | 13,782 |
| `coinbase-BTC-USD.top20.parquet` | `../raw/coinbase/BTC-USD/20261111T030647970222Z.qaraw.zst` | `20261111T030647.970222600Z-6fd8ece9ac35` | 2,702 |

## Format

Parquet, zstd-compressed, one row per stream message in replay order
(`log_id`, then `seq`; ADR 0013). Independent check snapshots are not part of
the stream and have no row.

| Column | Type | Meaning |
|--------|------|---------|
| `log_id` | string | The raw log the message came from (ADR 0013) |
| `seq` | uint64 | The raw record's number in its log |
| `recv_ts` | int64 | When that record was received: ns since the Unix epoch, wall clock. Information only, not an ordering key |
| `bid_price_00`, `bid_qty_00` | int64, nullable | The best bid after the message: price mantissa and quantity mantissa |
| `ask_price_00`, `ask_qty_00` | int64, nullable | The best ask after the message |
| `bid_price_NN` ... `ask_qty_NN` | int64, nullable | The level of rank `NN` (00 to 19, best first) on each side |

Columns run rank by rank: `bid_price_00, bid_qty_00, ask_price_00,
ask_qty_00, bid_price_01, ...`, 83 columns in all. A rank the book does not
have is null on both price and quantity. Prices and quantities are int64
mantissas at the instrument's fixed `price_exp` and `qty_exp`
(`docs/data-core-spec.md`, section 2): 8 and 8 for BTC-USD on both venues.
Rank 00 is the top of book.

Kraken books are the subscribed 25 levels, cut to depth after every message;
Coinbase books are the full book. The file holds the top 20 of each.

## History

- ADR 0013 added the `log_id` column and moved `recv_ts` after `seq`. Every
  row, `seq`, `recv_ts` and level value is unchanged from the files it
  replaced, checked column by column when they were regenerated.
- ADR 0019 fixed each instrument's exponents. Prices were at Kraken's 1 and
  Coinbase's 2 decimals, and are now at 8: every price is the old one times
  10^7 (Kraken) or 10^6 (Coinbase). Every row, `log_id`, `seq`, `recv_ts` and
  quantity is unchanged, and every price is exactly the old one scaled,
  checked column by column when they were regenerated.

## Regenerating

Load a fixture into a store, then write its golden file:

```
python -m quantagent.data.adapters ../raw/kraken/BTC-USD/<file>.qaraw.zst --store <store>
python -m quantagent.data.book verify --store <store> --venue kraken --instrument BTC-USD
python -m quantagent.data.book golden --store <store> --venue kraken \
    --instrument BTC-USD --out kraken-BTC-USD.top20.parquet
```

`python/tests/data/test_book.py` rebuilds both books on every test run and
fails if they differ from these files in any value. Regenerate them only on
purpose, when the fixture or the reconstructor's documented behavior changes,
and say why in the pull request.
