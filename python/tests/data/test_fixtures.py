"""Adapters against recorded exchange data (python/tests/fixtures/raw).

These run offline. Counts are recomputed here independently from the raw JSON,
so they do not share code with the adapters.
"""

import json
import zlib
from collections import Counter
from pathlib import Path

import pytest
from _builders import INT64_MAX

from quantagent.data.adapters import ingest, normalize
from quantagent.data.rawlog import RawLogWriter, read_raw_log
from quantagent.data.schema import (
    BOOK_EVENTS,
    DATASETS,
    INSTRUMENTS,
    TRADES,
    BookEventKind,
    Side,
    table_from_rows,
)
from quantagent.data.session import PITSession
from quantagent.data.store import AppendOnlyStore

FIXTURES = Path(__file__).parent.parent / "fixtures" / "raw"
VENUES = ["kraken", "coinbase"]
INSTRUMENT = "BTC-USD"


def fixture_path(venue: str) -> Path:
    (path,) = sorted((FIXTURES / venue / INSTRUMENT).glob("*.qaraw.zst"))
    return path


@pytest.fixture(scope="module", params=VENUES)
def capture(request):
    venue = request.param
    reader = read_raw_log(fixture_path(venue))
    records = list(reader)
    assert reader.truncated is False
    return venue, records, normalize(records)


def rows(normalized, dataset, venue):
    return normalized.rows.get((dataset.name, venue, INSTRUMENT), [])


def independent_counts(venue, records):
    """Expected rows per dataset and messages per kind, straight from the JSON."""
    expected = Counter()
    kinds = Counter()
    for record in records:
        if record.channel == "rest:product":
            expected["instruments"] += 1
            continue
        if record.channel == "recorder":
            # Session and connection events (ADR 0010); a REST request is not one.
            expected["stream_events"] += json.loads(record.payload)["event"] != "rest"
            continue
        if record.channel not in ("ws", "probe"):
            continue
        message = json.loads(record.payload)
        if venue == "kraken":
            channel, kind = message.get("channel"), message.get("type")
            expected["stream_events"] += message.get("method") == "subscribe"  # an ack
            expected["stream_events"] += (channel, kind) == ("trade", "snapshot")  # backfill
            if channel == "instrument":
                pairs = [p for p in message["data"]["pairs"] if p["symbol"] == "BTC/USD"]
                expected["instruments"] += len(pairs)
            elif channel == "trade":
                expected["trades"] += len(message["data"])
                kinds[f"trade.{kind}"] += 1
            elif channel == "book" and (record.channel == "ws" or kind == "snapshot"):
                levels = sum(len(d["bids"]) + len(d["asks"]) for d in message["data"])
                expected["book_events"] += levels
                kinds[f"book.{kind}"] += 1 if levels else 0
        else:
            channel = message.get("channel")
            expected["stream_events"] += channel == "subscriptions"  # an ack
            for event in message.get("events", []):
                if channel == "market_trades":
                    expected["trades"] += len(event["trades"])
                    expected["stream_events"] += event["type"] == "snapshot"  # backfill
                elif channel == "l2_data" and (
                    record.channel == "ws" or event["type"] == "snapshot"
                ):
                    expected["book_events"] += len(event["updates"])
    return expected, kinds


# --- the fixtures themselves ---------------------------------------------------------


@pytest.mark.parametrize("venue", VENUES)
def test_fixture_is_small_sealed_and_documented(venue):
    path = fixture_path(venue)
    assert path.stat().st_size < 3_000_000
    readme = (FIXTURES / "README.md").read_text(encoding="utf-8")
    for word in (venue, INSTRUMENT, path.name, "public"):
        assert word in readme, word


def test_fixture_starts_with_a_session_start_and_spans_a_few_minutes(capture):
    venue, records, _ = capture
    start = json.loads(records[0].payload)
    assert records[0].channel == "recorder"
    assert (start["event"], start["venue"], start["instrument"]) == ("start", venue, INSTRUMENT)
    assert json.loads(records[-1].payload) == {"event": "stop"}
    seconds = (records[-1].recv_ts - records[0].recv_ts) / 1e9
    assert 120 < seconds < 600
    assert {r.venue for r in records} == {venue}
    channels = {r.channel for r in records}
    assert {"recorder", "ws", "probe"} <= channels


def test_fixture_contains_no_credentials(capture):
    _, records, _ = capture
    for record in records:
        if record.channel == "recorder":
            text = record.payload.decode().lower()
            for word in ("secret", "jwt", "signature", "passphrase", "api_key", "apikey"):
                assert word not in text


# --- every message parses, and counts match -----------------------------------------


def test_every_fixture_message_parses(capture):
    _, records, normalized = capture
    report = normalized.report
    assert report.rejects == []
    assert report.sequence_anomalies == []
    assert report.records == len(records)
    assert sum(report.outcomes.values()) == len(records)
    assert set(report.outcomes) == {"data", "ignored"}


def test_row_counts_match_message_counts(capture):
    venue, records, normalized = capture
    expected, kinds = independent_counts(venue, records)
    assert expected["trades"] > 100 and expected["book_events"] > 10_000
    assert expected["instruments"] == 1
    assert normalized.report.rows == expected
    assert {name: len(rows(normalized, DATASETS[name], venue)) for name in expected} == expected
    for kind, count in kinds.items():
        assert normalized.report.messages[kind] == count, kind


def test_known_non_data_messages_are_counted(capture):
    venue, records, normalized = capture
    ignored = normalized.report.ignored
    controls = sum(r.channel == "recorder" for r in records)
    assert sum(v for k, v in ignored.items() if k.startswith("recorder.")) == controls
    if venue == "kraken":
        assert ignored["heartbeat"] > 100
        assert ignored["subscribe.ack"] == 4  # three on the stream, one on the probe
        assert ignored["status.update"] == 2
    else:
        assert ignored["heartbeats"] > 100
        assert ignored["subscriptions"] >= 3


def test_fixture_rows_satisfy_the_schema(capture):
    _, _, normalized = capture
    for (dataset_name, _, _), dataset_rows in normalized.rows.items():
        table = table_from_rows(DATASETS[dataset_name], dataset_rows)
        assert table.num_rows == len(dataset_rows)


def test_fixture_has_stream_snapshot_deltas_and_an_independent_check_snapshot(capture):
    venue, _, normalized = capture
    kinds = Counter(r["kind"] for r in rows(normalized, BOOK_EVENTS, venue))
    assert kinds[BookEventKind.SNAPSHOT] > 0
    assert kinds[BookEventKind.DELTA] > 1_000
    assert kinds[BookEventKind.CHECK_SNAPSHOT] > 0
    first = rows(normalized, BOOK_EVENTS, venue)[0]
    assert first["kind"] == BookEventKind.SNAPSHOT  # the stream begins with its snapshot


# --- ADR 0008 on real data ----------------------------------------------------------


def test_event_ts_source_on_real_data(capture):
    venue, _, normalized = capture
    for dataset in (TRADES, BOOK_EVENTS):
        assert {r["event_ts_source"] for r in rows(normalized, dataset, venue)} == {"venue"}
    metadata = rows(normalized, INSTRUMENTS, venue)
    assert {r["event_ts_source"] for r in metadata} == {"receipt"}
    for dataset in (TRADES, BOOK_EVENTS, INSTRUMENTS):
        for row in rows(normalized, dataset, venue):
            if row["event_ts_source"] == "receipt":
                assert row["event_ts"] == row["recv_ts"]
    # Venue timestamps are real: close to, and not identical with, receipt time.
    lags = [r["recv_ts"] - r["event_ts"] for r in rows(normalized, TRADES, venue)][-50:]
    assert all(-5e9 < lag < 60e9 for lag in lags)
    assert any(lag != 0 for lag in lags)


def test_instrument_metadata_from_real_data(capture):
    venue, _, normalized = capture
    (meta,) = rows(normalized, INSTRUMENTS, venue)
    if venue == "kraken":
        assert (meta["price_exp"], meta["qty_exp"], meta["tick_size"], meta["lot_size"]) == (
            1,
            8,
            1,
            1,
        )
    else:
        assert (meta["price_exp"], meta["qty_exp"], meta["tick_size"], meta["lot_size"]) == (
            2,
            8,
            1,
            1,
        )
    first_data = min(r["seq"] for d in (TRADES, BOOK_EVENTS) for r in rows(normalized, d, venue))
    assert meta["seq"] < first_data  # metadata is known before any market data


# --- venue integrity rules, recomputed from the stored integers ------------------------


def by_message(book_rows):
    message, current = [], None
    for row in book_rows:
        if (row["recv_ts"], row["seq"]) != current:
            if message:
                yield message
            message, current = [], (row["recv_ts"], row["seq"])
        message.append(row)
    if message:
        yield message


def apply(book, message, depth=None):
    if message[0]["kind"] != BookEventKind.DELTA:
        book = {Side.BUY: {}, Side.SELL: {}}
    for row in message:
        if row["qty"] == 0:
            book[row["side"]].pop(row["price"], None)
        else:
            book[row["side"]][row["price"]] = row["qty"]
    if depth is not None:  # the venue keeps only this many levels per side
        book[Side.BUY] = dict(sorted(book[Side.BUY].items(), reverse=True)[:depth])
        book[Side.SELL] = dict(sorted(book[Side.SELL].items())[:depth])
    return book


def kraken_checksum(book):
    asks = sorted(book[Side.SELL].items())[:10]
    bids = sorted(book[Side.BUY].items(), reverse=True)[:10]
    text = "".join(f"{price}{qty}" for price, qty in [*asks, *bids])
    return zlib.crc32(text.encode())


def test_kraken_checksum_is_reproduced_from_the_stored_mantissas_for_every_message():
    # This is the venue's own integrity rule. It only works if every price and
    # quantity was converted exactly, the order of levels within a message was
    # kept, and the book is cut to the subscribed depth after each update.
    normalized = normalize(read_raw_log(fixture_path("kraken")))
    book_rows = rows(normalized, BOOK_EVENTS, "kraken")
    stream = [r for r in book_rows if r["kind"] != BookEventKind.CHECK_SNAPSHOT]
    assert {r["depth"] for r in book_rows} == {25}
    assert {r["venue_seq_last"] for r in book_rows} == {None}  # Kraken has no sequence numbers

    book, checked = None, 0
    for message in by_message(stream):
        book = apply(book, message, depth=25)
        assert kraken_checksum(book) == message[0]["checksum"], message[0]["seq"]
        assert len({row["checksum"] for row in message}) == 1
        checked += 1
    assert checked > 10_000

    probes = [r for r in book_rows if r["kind"] == BookEventKind.CHECK_SNAPSHOT]
    for message in by_message(probes):
        assert kraken_checksum(apply(None, message, depth=25)) == message[0]["checksum"]
        assert Counter(row["side"] for row in message) == {Side.BUY: 25, Side.SELL: 25}


def test_kraken_checksum_check_would_notice_a_single_wrong_digit():
    normalized = normalize(read_raw_log(fixture_path("kraken")))
    stream = [
        r for r in rows(normalized, BOOK_EVENTS, "kraken") if r["kind"] == BookEventKind.SNAPSHOT
    ]
    book = apply(None, stream, depth=25)
    assert kraken_checksum(book) == stream[0]["checksum"]
    best_bid = max(book[Side.BUY])
    book[Side.BUY][best_bid] += 1
    assert kraken_checksum(book) != stream[0]["checksum"]


def test_coinbase_sequence_numbers_are_stored_and_continuous_on_the_stream():
    path = fixture_path("coinbase")
    records = list(read_raw_log(path))
    numbers = [json.loads(r.payload)["sequence_num"] for r in records if r.channel == "ws"]
    assert numbers == list(range(len(numbers)))  # one numbering across all channels

    normalized = normalize(records)
    book_rows = rows(normalized, BOOK_EVENTS, "coinbase")
    stream = [r for r in book_rows if r["kind"] != BookEventKind.CHECK_SNAPSHOT]
    stored = sorted({r["venue_seq_last"] for r in stream})
    assert stored[0] >= 0 and stored[-1] <= numbers[-1]
    assert len(stored) < len(numbers)  # heartbeats and trades take numbers too
    by_seq = {r.seq: json.loads(r.payload)["sequence_num"] for r in records if r.channel == "ws"}
    assert all(r["venue_seq_last"] == by_seq[r["seq"]] for r in stream)
    assert {r["checksum"] for r in book_rows} == {None}
    assert {r["depth"] for r in book_rows} == {None}


@pytest.mark.parametrize("venue", VENUES)
def test_independent_snapshot_equals_the_stream_book_at_the_same_venue_time(venue):
    """The check snapshot comes from a second connection, so it shares no
    sequence numbering with the stream. It can still be matched exactly: the
    book rebuilt from the stream, after the last message whose venue timestamp
    is at or before the snapshot's, equals the snapshot on every level."""
    normalized = normalize(read_raw_log(fixture_path(venue)))
    book_rows = rows(normalized, BOOK_EVENTS, venue)
    depth = 25 if venue == "kraken" else None
    stream = list(by_message([r for r in book_rows if r["kind"] != BookEventKind.CHECK_SNAPSHOT]))
    checks = list(by_message([r for r in book_rows if r["kind"] == BookEventKind.CHECK_SNAPSHOT]))
    assert checks

    for check in checks:
        times = {row["event_ts"] for row in check}
        assert len(times) == 1  # one venue timestamp for the whole image
        (taken_at,) = times
        expected = apply(None, check, depth=depth)

        book, applied = None, 0
        for message in stream:
            if max(row["event_ts"] for row in message) > taken_at:
                break
            book = apply(book, message, depth=depth)
            applied += 1
        assert 0 < applied < len(stream)  # strictly inside the recording
        assert book == expected
        assert sum(len(side) for side in book.values()) == len(check)


@pytest.mark.parametrize("venue", VENUES)
def test_reconstructed_book_is_never_crossed_and_trade_sides_agree_with_it(venue):
    """An empirical check of the aggressor side, which the two venues report
    differently: a buyer-initiated trade tends to print above the mid, a
    seller-initiated one below. If Coinbase's maker side were stored without
    being inverted, agreement here would be well under half."""
    normalized = normalize(read_raw_log(fixture_path(venue)))
    book_rows = [r for r in rows(normalized, BOOK_EVENTS, venue)
                 if r["kind"] != BookEventKind.CHECK_SNAPSHOT]  # fmt: skip
    depth = 25 if venue == "kraken" else None
    trades_by_seq = {}
    for trade in rows(normalized, TRADES, venue):
        trades_by_seq.setdefault(trade["seq"], []).append(trade)
    pending = sorted(trades_by_seq)

    book, agree, judged, messages = None, 0, 0, 0
    for message in by_message(book_rows):
        while pending and pending[0] < message[0]["seq"] and book is not None:
            mid2 = max(book[Side.BUY]) + min(book[Side.SELL])  # twice the mid, stays integer
            for trade in trades_by_seq[pending.pop(0)]:
                if 2 * trade["price"] == mid2:
                    continue
                judged += 1
                agree += (2 * trade["price"] > mid2) == (trade["side"] == Side.BUY)
        book = apply(book, message, depth=depth)
        messages += 1
        assert max(book[Side.BUY]) < min(book[Side.SELL]), message[0]["seq"]
    assert messages > 1_000
    assert judged > 50
    # The comparison is rough: by the time a trade message arrives the book has
    # usually already moved. On this data a correct mapping agrees about 80% of
    # the time on both venues (Kraken's side is documented as the taker's, which
    # calibrates the method); an inverted one would agree about 20%.
    assert agree / judged > 0.7, (agree, judged)


# --- end to end: raw log -> adapter -> store -> PITSession ---------------------------------


def canonical(table_rows, drop=("knowledge_ts",)):
    return sorted(
        tuple(sorted((k, v) for k, v in row.items() if k not in drop)) for row in table_rows
    )


@pytest.mark.parametrize("venue", VENUES)
def test_fixture_through_adapter_and_store_reads_back_identically(venue, tmp_path):
    path = fixture_path(venue)
    expected = normalize(read_raw_log(path))
    store = AppendOnlyStore(tmp_path / "store")
    report = ingest(path, store, batch_rows=5_000)  # small batches on purpose

    assert report.rows == expected.report.rows
    assert report.rejects == [] and report.truncated is False
    assert "rejected: none" in report.summary()
    files = list((tmp_path / "store").rglob("*.parquet"))
    assert 3 < len(files) < 60  # batched, not one file per message

    with PITSession(tmp_path / "store", as_of=INT64_MAX) as session:
        read_back = {
            TRADES: session.trades(venue, INSTRUMENT, 0, INT64_MAX),
            BOOK_EVENTS: session.book_events(venue, INSTRUMENT, 0, INT64_MAX),
            INSTRUMENTS: session.instruments(venue, INSTRUMENT),
        }
        for dataset, table in read_back.items():
            want = rows(expected, dataset, venue)
            keys = [tuple(row[k] for k in dataset.key) for row in want]
            assert len(set(keys)) == len(keys), f"{dataset.name}: duplicate key in the fixture"
            assert table.num_rows == len(want)
            assert canonical(table.to_pylist()) == canonical(want), dataset.name
            assert {r["knowledge_ts"] for r in table.to_pylist()} <= {
                int(f.name[5:25]) for f in files
            }
        counted = session.sql(
            "SELECT count(*) AS n, count(DISTINCT event_ts_source) AS sources FROM book_events"
        ).to_pylist()
        assert counted == [{"n": len(rows(expected, BOOK_EVENTS, venue)), "sources": 1}]
        meta = session.instrument(venue, INSTRUMENT, at=INT64_MAX)
        assert meta is not None and meta.qty_exp == 8


def test_normalizing_the_same_raw_log_again_corrects_rather_than_duplicates(tmp_path):
    path = fixture_path("kraken")
    store = AppendOnlyStore(tmp_path / "store")
    first = ingest(path, store)
    files_after_first = len(list((tmp_path / "store").rglob("*.parquet")))
    second = ingest(path, store)
    assert second.rows == first.rows
    assert len(list((tmp_path / "store").rglob("*.parquet"))) == 2 * files_after_first

    with PITSession(tmp_path / "store", as_of=INT64_MAX) as session:
        for name, count in first.rows.items():
            (visible,) = session.sql(f"SELECT count(*) AS n FROM {name}").to_pylist()
            assert visible["n"] == count, name


def test_ingest_bounds_memory_by_rows_not_by_messages(tmp_path, monkeypatch):
    # One Coinbase snapshot message is about 38,000 rows, so a limit counted in
    # messages would not bound memory. The limit is on pending rows.
    import quantagent.data.adapters.base as base

    sizes = []
    real = base.table_from_rows

    def recording(dataset, rows):
        sizes.append(len(rows))
        return real(dataset, rows)

    monkeypatch.setattr(base, "table_from_rows", recording)
    report = ingest(
        fixture_path("coinbase"), AppendOnlyStore(tmp_path / "store"), batch_rows=10_000
    )
    assert sum(sizes) == sum(report.rows.values())
    # A batch is flushed as soon as it reaches the limit, so it can exceed the
    # limit only by the rows of the single message that crossed it.
    assert max(sizes) < 10_000 + 40_000
    assert len(sizes) > 5
    assert inspect_signature_has_no_record_limit()


def inspect_signature_has_no_record_limit():
    import inspect

    return list(inspect.signature(ingest).parameters) == ["path", "store", "batch_rows"]


def test_ingest_reports_a_truncated_log_and_keeps_what_was_complete(tmp_path):
    records = list(read_raw_log(fixture_path("kraken")))[:200]
    path = tmp_path / "cut.qaraw"
    with RawLogWriter(path) as writer:
        for record in records:
            writer.write(record.venue, record.channel, record.payload, recv_ts=record.recv_ts)
    whole = path.read_bytes()
    path.write_bytes(whole[:-7])

    report = ingest(path, AppendOnlyStore(tmp_path / "store"))
    assert report.truncated is True
    assert report.records == 199
    assert report.rejects == []
    assert "partial record" in report.summary()


# --- command line ---------------------------------------------------------------------


@pytest.mark.parametrize("venue", VENUES)
def test_command_line_check_of_a_clean_log(venue, capsys):
    from quantagent.data.adapters.__main__ import main

    assert main([str(fixture_path(venue))]) == 0
    output = capsys.readouterr().out
    assert "rejected: none" in output
    assert "sequence anomalies: 0" in output
    assert "recorded: 150 s, 2026-11-11T03:06:4" in output
    assert "1 stream snapshot" in output and "1 check snapshot" in output


def test_command_line_check_fails_on_a_damaged_log_and_writes_nothing(tmp_path, capsys):
    from quantagent.data.adapters.__main__ import main

    records = list(read_raw_log(fixture_path("kraken")))[:60]
    path = tmp_path / "bad.qaraw"
    with RawLogWriter(path) as writer:
        for record in records:
            writer.write(record.venue, record.channel, record.payload, recv_ts=record.recv_ts)
        writer.write("kraken", "ws", b'{"channel":"book","type":"update","data":"oops"}')
    assert main([str(path)]) == 1
    output = capsys.readouterr().out
    assert "rejected: malformed_message 1" in output
    assert "rejected seq 60 (ws): malformed_message" in output
    assert [p.name for p in tmp_path.iterdir()] == ["bad.qaraw"]


def test_command_line_load_into_a_store(tmp_path, capsys):
    from quantagent.data.adapters.__main__ import main

    assert main([str(fixture_path("kraken")), "--store", str(tmp_path / "store")]) == 0
    output = capsys.readouterr().out
    assert "rows: book_events 18605, instruments 1, stream_events 15, trades 254" in output
    assert "checksum mismatches: 0" in output
    with PITSession(tmp_path / "store", as_of=INT64_MAX) as session:
        assert session.sql("SELECT count(*) AS n FROM trades").to_pylist() == [{"n": 254}]
