# Golden order book files

The order book rebuilt by the Python reference reconstructor
(`quantagent.data.book`) after every stream message of each recorded fixture
in `../raw/`. Phase 4 checks the C++ order book against these, message by
message.

| File | Built from | Rows (stream messages) |
|------|------------|------------------------|
| `kraken-BTC-USD.top20.parquet` | `../raw/kraken/BTC-USD/20261111T030647970222Z.qaraw.zst` | 13,782 |
| `coinbase-BTC-USD.top20.parquet` | `../raw/coinbase/BTC-USD/20261111T030647970222Z.qaraw.zst` | 2,702 |

## Format

Parquet, zstd-compressed, one row per stream message in replay order
(`recv_ts`, then `seq`). Independent check snapshots are not part of the
stream and have no row.

| Column | Type | Meaning |
|--------|------|---------|
| `recv_ts` | int64 | The raw record the message came from: receive time, ns since the Unix epoch |
| `seq` | uint64 | The raw record's number in its log |
| `bid_price_00`, `bid_qty_00` | int64, nullable | The best bid after the message: price mantissa and quantity mantissa |
| `ask_price_00`, `ask_qty_00` | int64, nullable | The best ask after the message |
| `bid_price_NN` ... `ask_qty_NN` | int64, nullable | The level of rank `NN` (00 to 19, best first) on each side |

Columns run rank by rank: `bid_price_00, bid_qty_00, ask_price_00,
ask_qty_00, bid_price_01, ...`, 82 columns in all. A rank the book does not
have is null on both price and quantity. Prices and quantities are int64
mantissas at the instrument's `price_exp` and `qty_exp` (ADR 0005): Kraken
BTC-USD is 1 and 8, Coinbase BTC-USD is 2 and 8. Rank 00 is the top of book.

Kraken books are the subscribed 25 levels, cut to depth after every message;
Coinbase books are the full book. The file holds the top 20 of each.

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
