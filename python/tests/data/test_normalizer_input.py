"""What the normalizer refuses (ADR 0018, review findings R05 and R11).

- **R05.** A value an adapter accepts but the schema cannot hold (a venue time
  after 2262, a sequence number beyond int64) makes the record a rejected
  message (MESSAGE_REJECTED, reason ``schema_refused``), like any other
  message that cannot be normalized. It used to raise out of ``ingest`` at
  the next flush and stop the load.
- **R11.** A raw log's records are numbered 0, 1, 2, ... by its writer. A
  record whose seq is neither 0 (a new log) nor the previous one plus one
  means records of several logs were fed interleaved (or a log was fed out of
  order). The normalizer cannot tell which log such a record belongs to, so
  it refuses with ``InterleavedLogs`` and refuses everything after it.
"""

import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from test_adapters import (
    COINBASE_CONNECT,
    COINBASE_PRODUCT,
    COINBASE_START,
    KRAKEN_BOOK_ACK,
    KRAKEN_INSTRUMENT,
    KRAKEN_START,
    log,
)
from test_fixtures import INSTRUMENT
from test_review_2026_11 import (
    coinbase_trade,
    kraken_trade,
    records_of,
    write_log,
)

from quantagent.data import quality
from quantagent.data.adapters import InterleavedLogs, Normalizer, ingest, normalize
from quantagent.data.adapters.coinbase import CoinbaseAdapter
from quantagent.data.quality import QualityConfig
from quantagent.data.rawlog import RawRecord, log_id_of
from quantagent.data.schema import (
    BOOK_EVENTS,
    DATASETS,
    INSTRUMENTS,
    STREAM_EVENTS,
    TRADES,
    SchemaError,
    StreamEventKind,
    check_row,
    table_from_rows,
)
from quantagent.data.session import PITSession
from quantagent.data.store import AppendOnlyStore

INT64_MAX = 2**63 - 1
LOG = "20231114T120000.000000000Z-000000000000"
FAR_FUTURE = "9999-12-31T23:59:59.000000Z"


def kraken(*messages):
    return log("kraken", KRAKEN_START, KRAKEN_INSTRUMENT, KRAKEN_BOOK_ACK, *messages)


def coinbase(*messages):
    return log("coinbase", COINBASE_START, COINBASE_PRODUCT, COINBASE_CONNECT, *messages)


def events_at(normalized, venue, seq):
    rows = normalized.rows.get(("stream_events", venue, INSTRUMENT), [])
    return [(r["event_index"], StreamEventKind(r["kind"]).name) for r in rows if r["seq"] == seq]


# --- R05: check_row is the schema, one row at a time ------------------------------------


VALID = {
    "trades": {"event_ts": 1, "event_ts_source": "venue", "recv_ts": 2, "log_id": LOG, "seq": 0,
               "trade_id": "t", "price": 5, "qty": 1, "side": 1},
    "book_events": {"event_ts": 1, "event_ts_source": "venue", "recv_ts": 2, "log_id": LOG,
                    "seq": 0, "kind": 1, "venue_seq_first": None, "venue_seq_last": 3,
                    "checksum": 7, "depth": 10, "level_index": 0, "side": -1, "price": 5,
                    "qty": 0},
    "instruments": {"event_ts": 2, "event_ts_source": "receipt", "recv_ts": 2, "log_id": LOG,
                    "seq": 0, "price_exp": 2, "qty_exp": 8, "price_precision": 2,
                    "qty_precision": 8, "tick_size": 1, "lot_size": 1},
    "stream_events": {"event_ts": 2, "event_ts_source": "receipt", "recv_ts": 2, "log_id": LOG,
                      "seq": 0, "event_index": 0, "kind": 10, "connection": "ws",
                      "expected": 1, "observed": 2, "detail": None},
}  # fmt: skip
ODD_VALUES = [None, -1, 0, 1, 3, 18, 19, 2**31 - 1, 2**31, 2**32 - 1, 2**32, 2**63 - 1, 2**63,
              -(2**63), -(2**63) - 1, 2**64, 2**70, True, False, 1.0, "1", "", "venue", "receipt",
              LOG, "not-a-log", StreamEventKind.SEQUENCE_GAP, 99]  # fmt: skip


def _accepted_by_table(dataset, row):
    try:
        table_from_rows(dataset, [row])
    except SchemaError:
        return False
    return True


def _accepted_by_row(dataset, row):
    try:
        check_row(dataset, row)
    except SchemaError:
        return False
    return True


@settings(max_examples=400, deadline=None)
@given(
    name=st.sampled_from(sorted(DATASETS)),
    changes=st.lists(st.tuples(st.integers(0, 20), st.sampled_from(ODD_VALUES)), max_size=3),
)
def test_check_row_accepts_exactly_the_rows_table_from_rows_accepts(name, changes):
    dataset = DATASETS[name]
    row = dict(VALID[name])
    columns = dataset.schema.names
    for index, value in changes:
        row[columns[index % len(columns)]] = value
    assert _accepted_by_row(dataset, row) == _accepted_by_table(dataset, row), row


@pytest.mark.parametrize("dataset", [TRADES, BOOK_EVENTS, INSTRUMENTS, STREAM_EVENTS],
                         ids=lambda d: d.name)  # fmt: skip
def test_check_row_refuses_a_missing_or_unknown_column(dataset):
    row = dict(VALID[dataset.name])
    with pytest.raises(SchemaError, match="unknown"):
        check_row(dataset, {**row, "extra": 1})
    row.pop("seq")
    with pytest.raises(SchemaError, match="missing"):
        check_row(dataset, row)
    with pytest.raises(SchemaError, match="knowledge_ts"):
        check_row(dataset, {**VALID[dataset.name], "knowledge_ts": 1})


# --- R05: the normalizer rejects a record the schema refuses ------------------------------


def test_a_value_the_schema_refuses_rejects_its_record_and_the_load_goes_on(tmp_path):
    records = kraken(kraken_trade(trade_id=1, timestamp=FAR_FUTURE), kraken_trade(trade_id=2))
    root = tmp_path / "store"
    report = ingest(write_log(tmp_path / "a.qaraw", records), AppendOnlyStore(root))
    (reject,) = report.rejects
    assert (reject.seq, reject.reason) == (3, "schema_refused")
    assert "event_ts" in reject.detail and "int64" in reject.detail
    assert (report.records, report.outcomes["rejected"]) == (5, 1)
    with PITSession(root, available_at=INT64_MAX) as session:
        trades = session.trades("kraken", INSTRUMENT, 0, INT64_MAX).to_pylist()
        result = quality.check_stream_integrity(session, "kraken", INSTRUMENT, 0, INT64_MAX,
                                                QualityConfig())  # fmt: skip
    assert [t["trade_id"] for t in trades] == ["2"]  # the rest of the log was loaded
    (finding,) = result.findings
    assert (finding.what, finding.seq) == ("message_rejected", 3)
    assert "schema_refused" in finding.message


def test_a_coinbase_sequence_number_beyond_int64_is_refused_before_it_becomes_state():
    """Coinbase keeps the highest number seen on a connection. If 2^70 were
    kept, every later message would be out of order against 2^70 + 1, an
    event no row can hold, and each would be rejected in turn."""
    another = ("ws", coinbase_trade(1)[1].replace("1118502354", "1118502355"))  # a new trade
    records = coinbase(coinbase_trade(0), coinbase_trade(2**70), another)
    normalized = normalize(records)
    (reject,) = normalized.report.rejects
    assert (reject.seq, reject.reason) == (4, "malformed_message")
    assert "sequence_num" in reject.detail
    assert events_at(normalized, "coinbase", 4) == [(0, "MESSAGE_REJECTED")]
    assert events_at(normalized, "coinbase", 5) == []  # 1 follows 0: no anomaly
    trades = normalized.rows[("trades", "coinbase", INSTRUMENT)]
    assert [t["seq"] for t in trades] == [3, 5]
    assert normalized.report.sequence_anomalies == []


def test_a_rejected_records_events_the_schema_cannot_hold_are_named_not_stored(monkeypatch):
    """Any rejected record: an event noted before the record failed is kept if
    the store can hold it, and otherwise named in MESSAGE_REJECTED's detail."""
    real = CoinbaseAdapter.handle_venue

    def note_then_fail(self, record):
        if record.seq == 3:
            self.note(StreamEventKind.SEQUENCE_GAP, connection="ws", expected=1, observed=2**70)
            self.note(StreamEventKind.SEQUENCE_GAP, connection="ws", expected=1, observed=5)
            raise ValueError("broken after noting")
        return real(self, record)

    monkeypatch.setattr(CoinbaseAdapter, "handle_venue", note_then_fail)
    normalized = normalize(coinbase(coinbase_trade(0)))
    assert events_at(normalized, "coinbase", 3) == [(0, "SEQUENCE_GAP"), (1, "MESSAGE_REJECTED")]
    rows = normalized.rows[("stream_events", "coinbase", INSTRUMENT)]
    (rejected,) = [r for r in rows if r["kind"] == StreamEventKind.MESSAGE_REJECTED]
    assert "not stored: stream_events.observed" in rejected["detail"]
    for name, rows in normalized.rows.items():
        table_from_rows(DATASETS[name[0]], rows)


def test_events_of_a_refused_record_that_the_schema_can_hold_are_kept():
    clock = {"event": "clock", "offset_ns": 2**70, "limit_ns": 250_000_000,
             "within_limit": False, "override": True, "servers": []}  # fmt: skip
    normalized = normalize(log("kraken", KRAKEN_START, ("recorder", clock)))
    (reject,) = normalized.report.rejects
    assert reject.reason == "schema_refused"
    # CLOCK_OFFSET cannot hold the offset; CLOCK_OVERRIDE can, and the override was used.
    assert events_at(normalized, "kraken", 1) == [(0, "CLOCK_OVERRIDE"), (1, "MESSAGE_REJECTED")]


def test_a_trade_in_a_refused_record_is_not_remembered_as_seen():
    """Otherwise its next, valid observation would be dropped as a repeat of a
    row that was never stored."""
    normalized = normalize(kraken(kraken_trade(timestamp=FAR_FUTURE), kraken_trade()))
    trades = normalized.rows[("trades", "kraken", INSTRUMENT)]
    assert [t["seq"] for t in trades] == [4]
    assert normalized.report.trade_repeats == 0
    assert events_at(normalized, "kraken", 4) == []


def test_the_adapter_is_told_that_a_refused_record_was_lost(monkeypatch):
    lost = []
    monkeypatch.setattr(CoinbaseAdapter, "on_reject", lambda self, record: lost.append(record.seq))
    normalized = normalize(coinbase(coinbase_trade(0, time="2300-01-01T00:00:00Z")))
    assert normalized.report.rejects[0].reason == "schema_refused"
    assert lost == [3]


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_every_row_from_damaged_fixture_messages_fits_the_schema(venue):
    """The review's fuzz found no exception escaping the normalizer, and five
    rows the schema then refused. Damage numbers and timestamps in fixture
    messages: every row that comes out must be storable."""
    rng = random.Random(f"r05-{venue}")
    records = records_of(venue)[:1500]
    damaged = []
    for record in records:
        payload = record.payload
        if record.seq > 0 and rng.random() < 0.3:
            text = payload.decode("utf-8", "replace")
            choice = rng.randrange(3)
            if choice == 0:
                big = 2 ** rng.randrange(60, 80)
                text = text.replace('"sequence_num":', f'"sequence_num":{big}0', 1)
            elif choice == 1:
                text = text.replace('"20', f'"{rng.choice(["99", "23", "22"])}', 1)
            else:
                position = rng.randrange(len(text))
                text = text[:position] + "9" * rng.randrange(15, 30) + text[position:]
            payload = text.encode("utf-8")
        damaged.append(RawRecord(record.recv_ts, record.seq, record.venue, record.channel,
                                 payload, record.recv_mono_ns))  # fmt: skip
    normalized = normalize(damaged)
    assert normalized.report.rejects  # the damage was seen
    for (name, _, _), rows in normalized.rows.items():
        table_from_rows(DATASETS[name], rows)  # raised SchemaError before ADR 0018


# --- R11: records of several logs fed interleaved are refused -----------------------------


def _two_logs():
    one = records_of("kraken")[:50]
    two = [RawRecord(r.recv_ts + 1, r.seq, r.venue, r.channel, r.payload, r.recv_mono_ns)
           for r in one]  # fmt: skip
    assert log_id_of(one[0]) != log_id_of(two[0])
    return one, two


def test_records_of_two_logs_fed_interleaved_are_refused_with_a_clear_error():
    one, two = _two_logs()
    normalizer = Normalizer()
    normalizer.feed(one[0])
    normalizer.feed(two[0])  # a new log begins: indistinguishable from one ending
    normalizer.feed(one[1])  # could be the second log's record 1
    with pytest.raises(InterleavedLogs) as raised:
        normalizer.feed(two[1])
    message = str(raised.value)
    assert "seq 1 after seq 1" in message and "interleaved" in message
    assert "one log at a time" in message


def test_a_normalizer_that_refused_refuses_everything_after():
    one, two = _two_logs()
    normalizer = Normalizer()
    for record in (one[0], two[0], one[1]):
        normalizer.feed(record)
    with pytest.raises(InterleavedLogs):
        normalizer.feed(two[1])
    # Rows it already holds may name the wrong log, so none of them come out.
    with pytest.raises(InterleavedLogs):
        normalizer.take_rows()
    with pytest.raises(InterleavedLogs):
        normalizer.feed(one[2])


def test_a_log_fed_out_of_order_or_with_a_record_missing_is_refused():
    records = kraken(kraken_trade(), kraken_trade(trade_id=7))
    for broken in ([*records[:3], records[4]], [*records[:3], records[4], records[3]]):
        with pytest.raises(InterleavedLogs, match="after seq 2"):
            normalize(broken)


def test_logs_fed_one_after_another_are_still_fine():
    one, two = _two_logs()
    normalized = normalize(one + two + one)
    levels = normalized.rows[("book_events", "kraken", INSTRUMENT)]
    assert {r["log_id"] for r in levels} == {log_id_of(one[0]), log_id_of(two[0])}


def test_a_log_without_its_first_record_is_still_rejected_record_by_record():
    normalized = normalize(records_of("kraken")[1:40])
    assert normalized.report.rejected == {"no_log_start": 39}


def test_ingest_never_sees_interleaved_records(tmp_path):
    """A raw log file is one log in seq order (its reader checks), so ingest
    cannot be fed interleaved records."""
    one, _ = _two_logs()
    report = ingest(write_log(tmp_path / "a.qaraw", one), AppendOnlyStore(tmp_path / "s"))
    assert (report.records, report.rejects) == (50, [])
