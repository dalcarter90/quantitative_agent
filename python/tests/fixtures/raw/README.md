# Recorded market data fixtures

Raw logs (`.qaraw.zst`, see `quantagent.data.rawlog`) holding exactly what each
venue sent, recorded with `quantagent.data.recorder`. They contain public
market data only. No API key was used to record them and none is stored in
them.

| Venue | Instrument | File | Recorded (UTC) | Length |
|-------|------------|------|----------------|--------|
| kraken | BTC-USD (venue symbol `BTC/USD`) | `kraken/BTC-USD/20261111T030647970222Z.qaraw.zst` | 2026-11-11 03:06:47 | 150 s |
| coinbase | BTC-USD | `coinbase/BTC-USD/20261111T030647970222Z.qaraw.zst` | 2026-11-11 03:06:47 | 150 s |

Both were recorded at the same time with:

```
python -m quantagent.data.recorder --venue <venue> --instrument BTC-USD \
    --out <dir> --duration 150 --probe-interval 80 --depth 25
```

## Clock

These were recorded before the recorder measured its clock (ADR 0011), on a
machine whose clock ran about 1.4 s ahead. Every `recv_ts` in them is about
1.4 s late, and the quality check reports `clock_not_recorded` for them. Tests
that depend on the lag between venue time and receipt time (for example the
clock skew band test) are written against this. See `docs/data-core.md`,
section 7.

## Raw log version

These are version 1 raw logs, recorded before ADR 0012. Their records have no
monotonic receive stamp (`recv_mono_ns` is None), so they cannot be merged
with other logs, and clock steps during them cannot be detected. They are kept
as recorded and are never rewritten.

## What is inside

Each record has a receive time, a sequence number, the venue, a channel and
the payload bytes.

| Channel | Content |
|---------|---------|
| `recorder` | Control records written by the recorder (JSON): session start, each connect, subscribe request and disconnect, each REST request, stop |
| `ws` | Every message from the main streaming connection |
| `probe` | Messages from one extra connection opened 80 seconds in, to capture an independent order book snapshot |
| `rest:product` | Coinbase only: the REST product metadata response |

**Kraken** (WebSocket v2, `wss://ws.kraken.com/v2`): the `instrument` snapshot
for all pairs, the `book` channel at depth 25 (one snapshot, then about 13,800
updates, each with a CRC32 checksum), the `trade` channel (a snapshot of the
last 50 trades, then updates), heartbeats and status. The probe holds one more
depth-25 book snapshot.

**Coinbase** (Advanced Trade, `wss://advanced-trade-ws.coinbase.com`): the
`level2` channel (one full-book snapshot of about 38,000 levels, then about
2,700 updates), `market_trades`, and `heartbeats`. `sequence_num` runs from 0
to 3309 with no gaps. The probe holds one more full-book snapshot from a
separate connection.

## Replacing a fixture

Record a new one with the command above and copy the sealed file here. Keep it
to a few minutes: each Coinbase snapshot is about 4 MB before compression.
Tests locate the file by directory, so the name may change, but update the
table above; a test checks that this README names the file.
