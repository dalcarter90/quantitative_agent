"""Instrument metadata: one fixed representation per instrument, and which
version is in effect for a row (the review's R06, ADR 0019).

- Each instrument has one fixed price exponent and one fixed quantity
  exponent, listed in ``adapters.FIXED_EXPONENTS``, independent of its tick
  and lot sizes and generous enough for any it may have. A tick-size change
  never changes how a price is stored.
- The venue's own print precision is kept beside them (``price_precision``,
  ``qty_precision``): Kraken's checksum is computed over the digits Kraken
  prints, so the reconstructor needs it.
- Metadata needing more decimals than the fixed exponent, or for an
  instrument that has none, is refused, never stored at another scale.
- A version is in effect for the rows of its log from its own record on
  (``PITSession.instrument_for``). Looked up by time
  (``PITSession.instrument``), a log's first version is in effect from the
  log's first row, whatever venue time that row carries.
"""

import json
import zlib

import pytest
from _builders import INT64_MAX, LOG
from test_adapters import (
    BASE,
    COINBASE_CONNECT,
    COINBASE_START,
    KRAKEN_BOOK_ACK,
    KRAKEN_START,
    log,
    rows_of,
)
from test_fixtures import INSTRUMENT, fixture_path

from quantagent.data import book
from quantagent.data.adapters import FIXED_EXPONENTS, ingest, normalize
from quantagent.data.rawlog import RawLogWriter, RawRecord, log_id_of, read_raw_log
from quantagent.data.schema import (
    BOOK_EVENTS,
    INSTRUMENTS,
    BookEventKind,
    SchemaError,
    Side,
    check_row,
    table_from_rows,
)
from quantagent.data.session import InstrumentMeta, PITSession
from quantagent.data.store import AppendOnlyStore

NS = 1_000_000_000
ALL = (0, INT64_MAX)


def kraken_pair(*, price_precision=1, price_increment="0.1", qty_precision=8,
                qty_increment="0.00000001", symbol="BTC/USD"):  # fmt: skip
    return (f'{{"symbol":"{symbol}","base":"BTC","quote":"USD","status":"online",'
            f'"qty_precision":{qty_precision},"qty_increment":{qty_increment},'
            f'"price_precision":{price_precision},'
            f'"price_increment":{price_increment}}}')  # fmt: skip


def kraken_instrument(kind="snapshot", **pair):
    return ("ws", f'{{"channel":"instrument","type":"{kind}","data":{{"assets":[],'
                  f'"pairs":[{kraken_pair(**pair)}]}}}}')  # fmt: skip


def kraken_digits(text):
    """Kraken's checksum digits for a number as Kraken prints it."""
    return text.replace(".", "").lstrip("0")


def kraken_checksum(bids, asks):
    """Kraken's checksum of a book as Kraken prints it: computed from the text,
    not from any stored value."""
    best_asks = sorted(asks, key=lambda level: float(level[0]))[:10]
    best_bids = sorted(bids, key=lambda level: -float(level[0]))[:10]
    digits = "".join(kraken_digits(p) + kraken_digits(q) for p, q in (*best_asks, *best_bids))
    return zlib.crc32(digits.encode())


def kraken_book(kind, bids, asks, *, after=None, timestamp="2026-11-11T03:06:47.006069Z"):
    """A book message printed exactly as given. Its checksum is of ``after``,
    the whole book after the message as Kraken would print it (for a snapshot,
    the message itself)."""
    checksum = kraken_checksum(*(after or (bids, asks)))

    def levels(side):
        return ",".join(f'{{"price":{p},"qty":{q}}}' for p, q in side)

    return ("ws", f'{{"channel":"book","type":"{kind}","data":[{{"symbol":"BTC/USD",'
                  f'"bids":[{levels(bids)}],"asks":[{levels(asks)}],"checksum":{checksum},'
                  f'"timestamp":"{timestamp}"}}]}}')  # fmt: skip


def coinbase_product(price_increment="0.01", base_increment="0.00000001", product="BTC-USD"):
    return ("rest:product", json.dumps({"product_id": product, "price_increment": price_increment,
                                        "base_increment": base_increment}))  # fmt: skip


def coinbase_snapshot(price="73469.60", sequence_num=0, product="BTC-USD"):
    level = {"side": "bid", "event_time": "2026-11-11T03:06:47.490859Z", "price_level": price,
             "new_quantity": "0.5"}  # fmt: skip
    return ("ws", json.dumps({"channel": "l2_data", "sequence_num": sequence_num,
                              "timestamp": "2026-11-11T03:06:47.624143697Z",
                              "events": [{"type": "snapshot", "product_id": product,
                                          "updates": [level]}]}))  # fmt: skip


def kraken_session(*entries):
    return normalize(log("kraken", KRAKEN_START, *entries))


def coinbase_session(*entries):
    return normalize(log("coinbase", COINBASE_START, *entries))


def write_log(path, records):
    with RawLogWriter(path) as writer:
        for r in records:
            writer.write(r.venue, r.channel, r.payload, recv_ts=r.recv_ts,
                         recv_mono_ns=r.recv_mono_ns)  # fmt: skip
    return path


# --- one fixed representation per instrument -----------------------------------------------------


def test_btc_usd_has_one_fixed_price_and_quantity_exponent_on_both_venues():
    assert FIXED_EXPONENTS == {("kraken", "BTC-USD"): (8, 8), ("coinbase", "BTC-USD"): (8, 8)}


def test_kraken_metadata_is_stored_at_the_fixed_exponents_with_the_venues_precision():
    normalized = kraken_session(kraken_instrument())
    (row,) = rows_of(normalized, INSTRUMENTS, "kraken")
    assert {
        k: row[k]
        for k in (
            "price_exp",
            "qty_exp",
            "price_precision",
            "qty_precision",
            "tick_size",
            "lot_size",
        )
    } == {  # fmt: skip
        "price_exp": 8,
        "qty_exp": 8,
        "price_precision": 1,  # Kraken prints BTC/USD prices with one decimal
        "qty_precision": 8,
        "tick_size": 10_000_000,  # 0.1 at eight decimal places
        "lot_size": 1,
    }


def test_coinbase_metadata_is_stored_at_the_fixed_exponents_with_the_increments_precision():
    normalized = coinbase_session(coinbase_product("0.050", "1"))
    (row,) = rows_of(normalized, INSTRUMENTS, "coinbase")
    assert (row["price_exp"], row["price_precision"], row["tick_size"]) == (8, 2, 5_000_000)
    assert (row["qty_exp"], row["qty_precision"], row["lot_size"]) == (8, 0, 100_000_000)


@pytest.mark.parametrize("increment", ["0.01", "0.1", "1", "0.05", "0.00000001"])
def test_a_coinbase_tick_size_change_never_changes_how_a_price_is_stored(increment):
    # A price every one of these ticks allows (a tick of 1 is whole dollars).
    normalized = coinbase_session(coinbase_product(increment), COINBASE_CONNECT,
                                  coinbase_snapshot("73470.00"))  # fmt: skip
    (meta,) = rows_of(normalized, INSTRUMENTS, "coinbase")
    (level,) = rows_of(normalized, BOOK_EVENTS, "coinbase")
    assert meta["price_exp"] == 8
    assert level["price"] == 7_347_000_000_000  # 73470.00 at the fixed exponent, whatever the tick


def test_a_price_finer_than_the_venues_precision_is_still_refused():
    """The exponent is generous, but a venue does not quote finer than its own
    precision: a price that does is malformed, as it was when the exponent was
    the precision."""
    normalized = coinbase_session(coinbase_product("1"), COINBASE_CONNECT,
                                  coinbase_snapshot("73469.60"))  # fmt: skip
    assert [r.reason for r in normalized.report.rejects] == ["malformed_message"]


SNAPSHOT = kraken_book("snapshot", [("73468.5", "0.20416909")], [("73468.6", "0.89472056")])
# After Kraken moved BTC/USD to two decimals: the update, and the whole book
# after it as Kraken now prints it, which is what its checksum covers.
AFTER = ([("73468.50", "0.30000000")], [("73468.55", "0.10000000"), ("73468.60", "0.89472056")])
UPDATE = kraken_book("update", [("73468.50", "0.30000000")], [("73468.55", "0.10000000")],
                     after=AFTER, timestamp="2026-11-11T03:06:48.000000Z")  # fmt: skip
PRECISION_CHANGE = kraken_instrument("update", price_precision=2, price_increment="0.01")
# records: 0 start, 1 instrument, 2 ack, 3 snapshot, 4 the change, 5 the update
PRECISION_CHANGE_LOG = (KRAKEN_START, kraken_instrument(), KRAKEN_BOOK_ACK, SNAPSHOT,
                        PRECISION_CHANGE, UPDATE)  # fmt: skip


def test_a_kraken_precision_change_never_changes_how_a_price_is_stored():
    """Kraken moves BTC/USD from one decimal to two mid-session. Prices keep
    their mantissas; the checksum follows the digits Kraken now prints."""
    normalized = normalize(log("kraken", *PRECISION_CHANGE_LOG))
    versions = rows_of(normalized, INSTRUMENTS, "kraken")
    assert [(v["price_exp"], v["price_precision"], v["tick_size"]) for v in versions] == [
        (8, 1, 10_000_000),
        (8, 2, 1_000_000),
    ]
    levels = rows_of(normalized, BOOK_EVENTS, "kraken")
    assert [(r["seq"], r["price"]) for r in levels] == [
        (3, 7_346_850_000_000),
        (3, 7_346_860_000_000),
        (5, 7_346_850_000_000),  # the same price, the same mantissa
        (5, 7_346_855_000_000),
    ]
    assert normalized.report.events["CHECKSUM_MISMATCH"] == 0
    assert normalized.report.rejects == []


def test_the_kraken_checksum_is_still_verified_at_the_venues_precision():
    # The same update with its checksum over the book printed at one decimal,
    # as before the change, is a mismatch once Kraken prints two.
    at_one_decimal = ([("73468.5", "0.30000000")],
                      [("73468.55", "0.10000000"), ("73468.6", "0.89472056")])  # fmt: skip
    wrong = kraken_book("update", [("73468.50", "0.30000000")], [("73468.55", "0.10000000")],
                        after=at_one_decimal)  # fmt: skip
    normalized = normalize(log("kraken", *PRECISION_CHANGE_LOG[:-1], wrong))
    assert normalized.report.events["CHECKSUM_MISMATCH"] == 1
    assert normalize(log("kraken", *PRECISION_CHANGE_LOG)).report.events["CHECKSUM_MISMATCH"] == 0


# --- what is refused ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "entries",
    [
        pytest.param(lambda: ("kraken", [kraken_instrument(price_precision=9,
                                                           price_increment="0.000000001")]),
                     id="kraken price"),
        pytest.param(lambda: ("kraken", [kraken_instrument(qty_precision=10,
                                                           qty_increment="0.0000000001")]),
                     id="kraken quantity"),
        pytest.param(lambda: ("coinbase", [coinbase_product("0.000000001")]), id="coinbase price"),
        pytest.param(lambda: ("coinbase", [coinbase_product("0.01", "0.000000001")]),
                     id="coinbase quantity"),
    ],
)  # fmt: skip
def test_metadata_finer_than_the_fixed_exponent_is_refused_not_rescaled(entries):
    venue, messages = entries()
    start = KRAKEN_START if venue == "kraken" else COINBASE_START
    normalized = normalize(log(venue, start, *messages))
    assert [r.reason for r in normalized.report.rejects] == ["precision_exceeds_exponent"]
    assert ("instruments", venue, INSTRUMENT) not in normalized.rows


def test_metadata_for_an_instrument_without_a_fixed_exponent_is_refused():
    start = ("recorder", {"event": "start", "venue": "coinbase", "instrument": "ETH-USD",
                          "symbol": "ETH-USD", "depth": 25, "recorder_version": 3})  # fmt: skip
    normalized = normalize(log("coinbase", start, coinbase_product(product="ETH-USD"),
                               COINBASE_CONNECT, coinbase_snapshot(product="ETH-USD")))  # fmt: skip
    assert [r.reason for r in normalized.report.rejects] == [
        "no_fixed_exponent",
        "no_instrument_metadata",  # and so is everything that needed it
    ]
    assert "FIXED_EXPONENTS" in normalized.report.rejects[0].detail


def test_the_schema_holds_the_precisions_and_refuses_one_finer_than_the_exponent():
    assert INSTRUMENTS.version == 4
    assert INSTRUMENTS.schema.names[-6:] == ["price_exp", "qty_exp", "price_precision",
                                             "qty_precision", "tick_size", "lot_size"]  # fmt: skip
    row = {"event_ts": 1, "event_ts_source": "receipt", "recv_ts": 1, "log_id": LOG, "seq": 0,
           "price_exp": 8, "qty_exp": 8, "price_precision": 2, "qty_precision": 8,
           "tick_size": 1_000_000, "lot_size": 1}  # fmt: skip
    check_row(INSTRUMENTS, row)
    table_from_rows(INSTRUMENTS, [row])
    for column in ("price_precision", "qty_precision"):
        finer = {**row, column: 9}
        with pytest.raises(SchemaError, match="precision"):
            check_row(INSTRUMENTS, finer)
        with pytest.raises(SchemaError, match="precision"):
            table_from_rows(INSTRUMENTS, [finer])
        with pytest.raises(SchemaError):
            check_row(INSTRUMENTS, {**row, column: -1})


# --- which version is in effect -------------------------------------------------------------


@pytest.fixture(scope="module")
def fixtures_store(tmp_path_factory):
    root = tmp_path_factory.mktemp("instruments") / "store"
    store = AppendOnlyStore(root)
    for venue in ("kraken", "coinbase"):
        ingest(fixture_path(venue), store)
    return root


def market_rows(session, venue):
    levels = session.book_events(venue, INSTRUMENT, *ALL).to_pylist()
    trades = session.trades(venue, INSTRUMENT, *ALL).to_pylist()
    return [*levels, *trades]


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_every_row_has_the_version_of_its_own_log_by_record_and_by_time(fixtures_store, venue):
    with PITSession(fixtures_store, available_at=INT64_MAX) as session:
        (version,) = session.instruments(venue, INSTRUMENT).to_pylist()
        rows = market_rows(session, venue)
        assert len(rows) > 1000
        # By record: every record with market rows, the first and the last
        # among them, and a spread in between (one lookup per record).
        records = sorted({(r["log_id"], r["seq"]) for r in rows})
        for log_id, seq in {records[0], records[-1], *records[:: max(len(records) // 40, 1)]}:
            by_record = session.instrument_for(venue, INSTRUMENT, log_id=log_id, seq=seq)
            assert (by_record.log_id, by_record.seq) == (version["log_id"], version["seq"])
        # By time, too: the fixture's resent trades and Kraken's first snapshot
        # carry venue times before the metadata reached us (review, R06).
        early = sorted({r["event_ts"] for r in rows if r["event_ts"] < version["event_ts"]})
        assert early
        for at in {early[0], early[-1], *early[:: max(len(early) // 20, 1)]}:
            meta = session.instrument(venue, INSTRUMENT, at=at)
            assert (meta.log_id, meta.seq) == (version["log_id"], version["seq"])
        # Before the log's first row nothing was in effect.
        first = min(r["event_ts"] for r in rows)
        assert session.instrument(venue, INSTRUMENT, at=first - 1) is None
        assert session.instrument_for(venue, INSTRUMENT, log_id=version["log_id"],
                                      seq=version["seq"] - 1) is None  # fmt: skip


def test_instrument_meta_names_its_record_and_the_venues_precision(fixtures_store):
    with PITSession(fixtures_store, available_at=INT64_MAX) as session:
        meta = session.instrument("kraken", INSTRUMENT)
    assert isinstance(meta, InstrumentMeta)
    assert (meta.price_exp, meta.qty_exp, meta.price_precision, meta.qty_precision) == (8, 8, 1, 8)
    assert meta.log_id == log_id_of(next(iter(read_raw_log(fixture_path("kraken")))))


def two_sessions(tmp_path):
    """Two Kraken sessions an hour apart. The second resends the first's last
    trade as history: its venue time is before the second session began."""

    def trade(trade_id, timestamp, kind="update"):
        return ("ws", f'{{"channel":"trade","type":"{kind}","data":[{{"symbol":"BTC/USD",'
                      f'"side":"sell","price":73469.7,"qty":0.5,"trade_id":{trade_id},'
                      f'"timestamp":"{timestamp}"}}]}}')  # fmt: skip

    first = log("kraken", KRAKEN_START, kraken_instrument(), trade(1, "2026-11-11T03:06:47.5Z"),
                trade(2, "2026-11-11T03:06:47.9Z"))  # fmt: skip
    hour = 3600 * NS
    second = [RawRecord(r.recv_ts + hour, r.seq, r.venue, r.channel, r.payload) for r in log(
        "kraken", KRAKEN_START, kraken_instrument(price_precision=2, price_increment="0.01"),
        trade(2, "2026-11-11T03:06:47.9Z", "snapshot"), trade(3, "2026-11-11T04:06:48.0Z"),
        trade(4, "2026-11-11T04:06:30.0Z", "snapshot"))]  # fmt: skip
    root = tmp_path / "store"
    store = AppendOnlyStore(root)
    ingest(write_log(tmp_path / "first.qaraw", first), store)
    ingest(write_log(tmp_path / "second.qaraw", second), store)
    return root, log_id_of(first[0]), log_id_of(second[0])


def test_each_session_has_its_own_version_by_time_even_for_history_it_resent(tmp_path):
    root, first, second = two_sessions(tmp_path)
    with PITSession(root, available_at=INT64_MAX) as session:
        trades = {t["trade_id"]: t for t in session.trades("kraken", INSTRUMENT, *ALL).to_pylist()}

        def version_at(trade_id):
            meta = session.instrument("kraken", INSTRUMENT, at=trades[trade_id]["event_ts"])
            return meta.log_id, meta.price_precision

        assert version_at("1") == (first, 1)
        assert version_at("2") == (first, 1)  # seen live in the first session
        assert version_at("3") == (second, 2)
        # Trade 4 is history the second session received, with a venue time 18 s
        # before that session began: the second session's version is in effect.
        versions = session.instruments("kraken", INSTRUMENT).to_pylist()
        assert trades["4"]["log_id"] == second
        assert trades["4"]["event_ts"] < versions[1]["event_ts"]
        assert version_at("4") == (second, 2)


def test_the_time_a_version_takes_effect_comes_from_rows_the_session_can_see(tmp_path):
    root, first, second = two_sessions(tmp_path)
    with PITSession(root, available_at=INT64_MAX) as session:
        trades = session.trades("kraken", INSTRUMENT, *ALL).to_pylist()
    (resent,) = [t for t in trades if t["trade_id"] == "4"]
    # Before the resent trade arrived, nothing in the second log is that early,
    # so its venue time still falls in the first session's version.
    with PITSession(root, available_at=resent["recv_ts"] - 1) as session:
        assert session.instrument("kraken", INSTRUMENT, at=resent["event_ts"]).log_id == first
    with PITSession(root, available_at=resent["recv_ts"]) as session:
        assert session.instrument("kraken", INSTRUMENT, at=resent["event_ts"]).log_id == second


@pytest.fixture
def precision_change(tmp_path):
    records = log("kraken", *PRECISION_CHANGE_LOG)
    root = tmp_path / "store"
    report = ingest(write_log(tmp_path / "log.qaraw", records), AppendOnlyStore(root))
    assert report.rejects == [] and report.events["CHECKSUM_MISMATCH"] == 0
    return root, records


def test_by_record_each_row_has_the_latest_version_of_its_log_at_or_before_it(precision_change):
    root, records = precision_change
    log_id = log_id_of(records[0])
    with PITSession(root, available_at=INT64_MAX) as session:

        def precision(seq):
            meta = session.instrument_for("kraken", INSTRUMENT, log_id=log_id, seq=seq)
            return None if meta is None else meta.price_precision

        assert [precision(seq) for seq in range(6)] == [None, 1, 1, 1, 2, 2]
        assert session.instrument_for("kraken", INSTRUMENT, log_id=LOG, seq=5) is None
    # Only versions the session can see.
    with PITSession(root, available_at=records[3].recv_ts) as session:
        meta = session.instrument_for("kraken", INSTRUMENT, log_id=log_id, seq=5)
        assert meta.price_precision == 1


def test_by_time_a_later_version_of_a_log_takes_effect_at_its_own_time(precision_change):
    root, records = precision_change
    change = records[4].recv_ts  # Kraken's metadata has no venue time: it is the receipt time
    with PITSession(root, available_at=INT64_MAX) as session:
        assert session.instrument("kraken", INSTRUMENT, at=change - 1).price_precision == 1
        assert session.instrument("kraken", INSTRUMENT, at=change).price_precision == 2
        # The log's first version is in effect from its first row: the session start.
        assert session.instrument("kraken", INSTRUMENT, at=BASE).price_precision == 1
        assert session.instrument("kraken", INSTRUMENT, at=BASE - 1) is None


@pytest.mark.parametrize(
    "arguments",
    [
        {"log_id": "not a log", "seq": 0},
        {"log_id": LOG, "seq": -1},
        {"log_id": LOG, "seq": "1"},
        {"log_id": LOG, "seq": True},
        {"log_id": 5, "seq": 0},
    ],
)
def test_instrument_for_validates_its_arguments(fixtures_store, arguments):
    with PITSession(fixtures_store, available_at=INT64_MAX) as session:
        with pytest.raises((TypeError, ValueError)):
            session.instrument_for("kraken", INSTRUMENT, **arguments)


# --- the reconstructor reads Kraken's checksum digits through the metadata ------------------------


def test_the_reconstructor_verifies_kraken_at_the_precision_in_force(precision_change):
    root, _ = precision_change
    with PITSession(root, available_at=INT64_MAX) as session:
        instruments = session.instruments("kraken", INSTRUMENT)
        table = session.book_events("kraken", INSTRUMENT, *ALL)
        messages = list(book.messages_from_table(table, instruments))
        events = session.stream_events("kraken", INSTRUMENT, *ALL).to_pylist()
    assert [(m.price_precision, m.qty_precision) for m in messages] == [(1, 8), (2, 8)]
    states = [state.top(Side.SELL, 2) for _, state in book.replay("kraken", messages, events)]
    assert states[-1] == [(7_346_855_000_000, 10_000_000), (7_346_860_000_000, 89_472_056)]


def kraken_snapshot_messages(instruments=None):
    normalized = normalize(log("kraken", *PRECISION_CHANGE_LOG[:4]))
    table = table_from_rows(BOOK_EVENTS, normalized.rows[("book_events", "kraken", INSTRUMENT)])
    return list(book.messages_from_table(table, instruments))


def test_the_reconstructor_refuses_to_guess_krakens_precision():
    messages = kraken_snapshot_messages()
    assert (messages[0].price_precision, messages[0].qty_precision) == (None, None)
    with pytest.raises(ValueError, match="precision"):
        list(book.replay("kraken", messages))
    # Without verification nothing needs it.
    assert len(list(book.replay("kraken", messages, verify=False))) == 1


def test_messages_carry_no_precision_where_no_version_of_their_log_is_in_force():
    (message,) = kraken_snapshot_messages(table_from_rows(INSTRUMENTS, []))
    assert (message.price_precision, message.qty_precision) == (None, None)
    assert message.kind == BookEventKind.SNAPSHOT
