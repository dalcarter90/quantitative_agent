"""Recorder: public market data into a raw log. These tests use scripted fake
connections; the test against the real venues is marked ``network``."""

import asyncio
import json

import pytest

from quantagent.data import recorder
from quantagent.data.clock import ClockMeasurement, ServerSample
from quantagent.data.rawlog import read_raw_log
from quantagent.data.recorder import VENUES, ClockOffsetError, record

MS = 1_000_000


def clock_at(offset_ns, servers=2):
    """A clock measurement as the recorder would get it from NTP."""
    samples = tuple(ServerSample(f"ntp{i}", offset_ns, 20 * MS, None) for i in range(servers))
    return lambda: ClockMeasurement(offset_ns=offset_ns, samples=samples)


GOOD_CLOCK = clock_at(3 * MS)


class FakeSocket:
    def __init__(self, messages, *, then="hang"):
        self._messages = list(messages)
        self._then = then
        self.sent = []

    async def send(self, text):
        self.sent.append(json.loads(text))

    async def recv(self):
        if self._messages:
            await asyncio.sleep(0)
            return self._messages.pop(0)
        if self._then == "close":
            raise ConnectionError("scripted disconnect")
        if isinstance(self._then, float):
            # A timeout that fires this many seconds after the call.
            await asyncio.sleep(self._then)
            raise TimeoutError
        await asyncio.sleep(3600)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


KRAKEN_INSTRUMENT = '{"channel":"instrument","type":"snapshot","data":{"assets":[],"pairs":[{"symbol":"BTC/USD","qty_precision":8,"qty_increment":0.00000001,"price_precision":1,"price_increment":0.1}]}}'  # noqa: E501
KRAKEN_SNAPSHOT = '{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[],"asks":[],"checksum":0,"timestamp":"2026-01-01T00:00:00.000000Z"}]}'  # noqa: E501
KRAKEN_UPDATE = '{"channel":"book","type":"update","data":[]}'


def run_kraken(
    tmp_path,
    main_sockets,
    *,
    duration=0.5,
    probe_interval=0.12,
    reconnects=False,
    measure_clock=GOOD_CLOCK,
    allow_clock_offset=False,
    **options,
):
    """Run the recorder against scripted sockets.

    The first connection is the main stream. Later connections are probes,
    unless ``reconnects`` is set, in which case they are the next main socket.
    """
    main_sockets = list(main_sockets)
    probes = []

    def connect(url, **options):
        assert url == VENUES["kraken"].url
        if main_sockets and (reconnects or not connect.started):
            connect.started = True
            return main_sockets.pop(0)
        probes.append(FakeSocket([KRAKEN_SNAPSHOT]))
        return probes[-1]

    connect.started = False

    def no_rest(url):
        raise AssertionError("kraken needs no REST request")

    path = asyncio.run(
        record(
            VENUES["kraken"],
            "BTC-USD",
            tmp_path,
            duration=duration,
            probe_interval=probe_interval,
            depth=25,
            connect=connect,
            fetch=no_rest,
            reconnect_delay=0.01,
            measure_clock=measure_clock,
            allow_clock_offset=allow_clock_offset,
            **options,
        )
    )
    return path, list(read_raw_log(path)), probes


def controls(records):
    return [json.loads(r.payload) for r in records if r.channel == "recorder"]


def test_kraken_subscriptions_are_public_and_carry_symbol_and_depth():
    spec = VENUES["kraken"]
    assert spec.url == "wss://ws.kraken.com/v2"
    assert spec.symbol("BTC-USD") == "BTC/USD"
    assert spec.subscriptions("BTC/USD", 25) == [
        {"method": "subscribe", "params": {"channel": "instrument", "snapshot": True}},
        {
            "method": "subscribe",
            "params": {"channel": "book", "symbol": ["BTC/USD"], "depth": 25, "snapshot": True},
        },
        {
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": ["BTC/USD"], "snapshot": True},
        },
    ]
    assert spec.probe_subscriptions("BTC/USD", 25) == [
        {
            "method": "subscribe",
            "params": {"channel": "book", "symbol": ["BTC/USD"], "depth": 25, "snapshot": True},
        },
    ]
    assert spec.rest("BTC/USD") == []


def test_coinbase_subscriptions_are_public_and_fetch_product_metadata():
    spec = VENUES["coinbase"]
    assert spec.url == "wss://advanced-trade-ws.coinbase.com"
    assert spec.symbol("BTC-USD") == "BTC-USD"
    assert spec.subscriptions("BTC-USD", 25) == [
        {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "heartbeats"},
        {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "level2"},
        {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "market_trades"},
    ]
    assert spec.probe_subscriptions("BTC-USD", 25) == [
        {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "level2"},
    ]
    assert spec.rest("BTC-USD") == [
        ("rest:product", "https://api.coinbase.com/api/v3/brokerage/market/products/BTC-USD")
    ]


def test_no_subscription_or_request_carries_credentials():
    for spec in VENUES.values():
        symbol = spec.symbol("BTC-USD")
        text = json.dumps(
            [
                spec.subscriptions(symbol, 25),
                spec.probe_subscriptions(symbol, 25),
                spec.rest(symbol),
            ]
        ).lower()
        for word in ("key", "secret", "jwt", "token", "signature", "passphrase"):
            assert word not in text
    source = open(recorder.__file__, encoding="utf-8").read()
    assert "environ" not in source and "getenv" not in source


def test_recording_writes_control_records_then_every_message_in_order(tmp_path):
    main = FakeSocket([KRAKEN_INSTRUMENT, KRAKEN_SNAPSHOT, KRAKEN_UPDATE, KRAKEN_UPDATE])
    path, records, probes = run_kraken(tmp_path, [main])

    assert path.name.endswith(".qaraw.zst")
    assert path.parent == tmp_path / "kraken" / "BTC-USD"
    assert [r.seq for r in records] == list(range(len(records)))
    assert {r.venue for r in records} == {"kraken"}

    events = controls(records)
    assert events[0]["event"] == "start"
    assert events[0] | {"recorder_version": 0, "clock_identity": None} == {
        "event": "start",
        "venue": "kraken",
        "instrument": "BTC-USD",
        "symbol": "BTC/USD",
        "depth": 25,
        "recorder_version": 0,
        "clock_identity": None,
    }
    assert events[-1] == {"event": "stop"}
    assert events[1]["event"] == "clock"
    assert events[2] == {"event": "connect", "connection": "ws", "url": "wss://ws.kraken.com/v2"}
    subscribed = [
        e["request"] for e in events if e["event"] == "subscribe" and e["connection"] == "ws"
    ]
    assert subscribed == VENUES["kraken"].subscriptions("BTC/USD", 25)
    assert main.sent == subscribed

    stream = [r.payload.decode() for r in records if r.channel == "ws"]
    assert stream == [KRAKEN_INSTRUMENT, KRAKEN_SNAPSHOT, KRAKEN_UPDATE, KRAKEN_UPDATE]
    # Every received message is preceded by the connect record of its connection.
    first_ws = next(i for i, r in enumerate(records) if r.channel == "ws")
    connect_at = next(
        i for i, r in enumerate(records)
        if r.channel == "recorder" and json.loads(r.payload).get("connection") == "ws"
    )  # fmt: skip
    assert connect_at < first_ws


def test_probes_record_fresh_snapshots_on_a_separate_channel(tmp_path):
    main = FakeSocket([KRAKEN_SNAPSHOT])
    _, records, probes = run_kraken(tmp_path, [main], duration=0.6, probe_interval=0.1)

    probe_payloads = [r.payload.decode() for r in records if r.channel == "probe"]
    assert len(probes) >= 2
    assert probe_payloads and set(probe_payloads) == {KRAKEN_SNAPSHOT}
    for probe in probes[: len(probe_payloads)]:
        assert probe.sent == VENUES["kraken"].probe_subscriptions("BTC/USD", 25)
    events = controls(records)
    assert sum(e == {"event": "connect", "connection": "probe", "url": "wss://ws.kraken.com/v2"}
               for e in events) >= len(probe_payloads)  # fmt: skip
    assert any(e["event"] == "disconnect" and e["connection"] == "probe" for e in events)


def test_a_dropped_connection_is_recorded_and_reconnected(tmp_path):
    first = FakeSocket([KRAKEN_INSTRUMENT, KRAKEN_SNAPSHOT, KRAKEN_UPDATE], then="close")
    second = FakeSocket([KRAKEN_INSTRUMENT, KRAKEN_SNAPSHOT])
    _, records, _ = run_kraken(tmp_path, [first, second], probe_interval=10, reconnects=True)

    events = [e for e in controls(records) if e.get("connection") == "ws"]
    kinds = [e["event"] for e in events]
    assert kinds[0] == "connect"
    assert "disconnect" in kinds
    drop = kinds.index("disconnect")
    assert "scripted disconnect" in events[drop]["reason"]
    assert kinds[drop + 1] == "connect"
    assert [r.payload.decode() for r in records if r.channel == "ws"] == [
        KRAKEN_INSTRUMENT,
        KRAKEN_SNAPSHOT,
        KRAKEN_UPDATE,
        KRAKEN_INSTRUMENT,
        KRAKEN_SNAPSHOT,
    ]
    assert second.sent == VENUES["kraken"].subscriptions("BTC/USD", 25)


def test_coinbase_metadata_is_fetched_before_connecting(tmp_path):
    body = b'{"product_id":"BTC-USD","price_increment":"0.01","base_increment":"0.00000001"}'
    fetched = []

    def fetch(url):
        fetched.append(url)
        return body

    main = FakeSocket(['{"channel":"l2_data","events":[{"type":"snapshot"}]}'])
    path = asyncio.run(
        record(
            VENUES["coinbase"],
            "BTC-USD",
            tmp_path,
            duration=0.2,
            probe_interval=10,
            depth=25,
            connect=lambda url, **options: main,
            fetch=fetch,
            reconnect_delay=0.01,
            measure_clock=GOOD_CLOCK,
        )
    )
    records = list(read_raw_log(path))
    assert fetched == ["https://api.coinbase.com/api/v3/brokerage/market/products/BTC-USD"]
    channels = [r.channel for r in records]
    assert channels.index("rest:product") < channels.index("ws")
    assert next(r.payload for r in records if r.channel == "rest:product") == body
    assert controls(records)[2] == {"event": "rest", "channel": "rest:product", "url": fetched[0]}


def test_recv_ts_is_taken_when_each_message_arrives(tmp_path):
    main = FakeSocket([KRAKEN_SNAPSHOT, KRAKEN_UPDATE])
    _, records, _ = run_kraken(tmp_path, [main])
    times = [r.recv_ts for r in records]
    assert times == sorted(times)
    assert times[0] > 1_600_000_000 * 10**9


class ScriptedClock:
    """The recorder's clocks, moved only by the scripted socket below."""

    def __init__(self):
        self.now = 1_800_000_000 * 10**9

    def stamp(self):
        return self.now, self.now


class DelayedSocket(FakeSocket):
    """Delivers each message ``delay`` ns of scripted time after the read for it
    began, and says when each was delivered."""

    def __init__(self, messages, clock, delay):
        super().__init__(messages)
        self.clock, self.delay, self.delivered = clock, delay, []

    async def recv(self):
        if not self._messages:
            await asyncio.sleep(3600)
        await asyncio.sleep(0)
        self.clock.now += self.delay
        self.delivered.append(self.clock.now)
        return self._messages.pop(0)


def test_each_stamp_is_taken_after_its_message_arrives_and_before_the_next_read(
    tmp_path, monkeypatch
):
    """The review's T1 mutation: a recorder that stamps a message before waiting
    for it (so each record carries the previous message's time) passed every
    other test. Here the clocks move only when a message is delivered, so a
    stamp taken anywhere but between a delivery and the next read is wrong."""
    clock = ScriptedClock()
    monkeypatch.setattr(recorder, "stamp", clock.stamp)
    delay = 7_000_000  # 7 ms between asking for a message and getting it
    main = DelayedSocket([KRAKEN_INSTRUMENT, KRAKEN_SNAPSHOT, KRAKEN_UPDATE, KRAKEN_UPDATE],
                         clock, delay)  # fmt: skip
    _, records, _ = run_kraken(tmp_path, [main], probe_interval=10)
    stream = [r for r in records if r.channel == "ws"]
    assert len(stream) == len(main.delivered) == 4
    for written, delivered in zip(stream, main.delivered, strict=True):
        # Not before the message arrived, and not after the next read began
        # (that read moves the clock on by the delay).
        assert delivered <= written.recv_ts < delivered + delay
        assert written.recv_mono_ns == written.recv_ts


# --- instrument metadata before market data (ADR 0019) ------------------------------------------


def test_kraken_subscribes_to_book_and_trades_only_once_the_instrument_snapshot_is_in(tmp_path):
    """Every market data message of a session then follows a metadata version
    in the log, so one is in effect from the session's first row (review, R06)."""
    main = FakeSocket([KRAKEN_SNAPSHOT, KRAKEN_INSTRUMENT, KRAKEN_SNAPSHOT, KRAKEN_UPDATE])
    _, records, _ = run_kraken(tmp_path, [main], probe_interval=10)
    kinds = []
    for written in records:
        if written.channel == "ws":
            kinds.append(json.loads(written.payload).get("channel"))
        elif written.channel == "recorder":
            event = json.loads(written.payload)
            if event["event"] == "subscribe":
                kinds.append("subscribe " + event["request"]["params"]["channel"])
    assert kinds == [
        "subscribe instrument",
        "book",  # whatever arrives first is recorded; it was not asked for yet
        "instrument",
        "subscribe book",
        "subscribe trade",
        "book",
        "book",
    ]
    assert main.sent == VENUES["kraken"].subscriptions("BTC/USD", 25)


def test_kraken_reconnects_if_no_instrument_snapshot_arrives(tmp_path):
    silent = FakeSocket([KRAKEN_SNAPSHOT])  # then nothing
    second = FakeSocket([KRAKEN_INSTRUMENT, KRAKEN_SNAPSHOT])
    _, records, _ = run_kraken(tmp_path, [silent, second], duration=0.6, probe_interval=10,
                               reconnects=True, metadata_timeout=0.1)  # fmt: skip
    reasons = [e["reason"] for e in controls(records)
               if e["event"] == "disconnect" and e["connection"] == "ws"]  # fmt: skip
    assert "instrument metadata" in reasons[0] and reasons[-1] == "session ended"
    assert silent.sent == VENUES["kraken"].subscriptions("BTC/USD", 25)[:1]
    assert second.sent == VENUES["kraken"].subscriptions("BTC/USD", 25)


def test_coinbase_needs_no_wait_because_its_metadata_comes_before_connecting():
    spec = VENUES["coinbase"]
    assert spec.metadata_requests == 0 and spec.rest("BTC-USD")
    kraken = VENUES["kraken"]
    assert kraken.metadata_requests == 1
    assert kraken.is_metadata(KRAKEN_INSTRUMENT) and not kraken.is_metadata(KRAKEN_SNAPSHOT)


@pytest.mark.network
@pytest.mark.parametrize("venue", sorted(VENUES))
def test_live_capture_smoke(tmp_path, venue):
    path = asyncio.run(
        # Uses the real NTP measurement: this refuses to run on a bad clock.
        record(VENUES[venue], "BTC-USD", tmp_path, duration=6, probe_interval=3, depth=10)
    )
    records = list(read_raw_log(path))
    assert sum(r.channel == "ws" for r in records) > 5
    assert any(r.channel == "probe" for r in records)


# --- the recording clock (ADR 0011) -------------------------------------------------------


def test_the_clock_offset_is_recorded_right_after_the_session_start(tmp_path):
    _, records, _ = run_kraken(tmp_path, [FakeSocket([KRAKEN_SNAPSHOT])])
    events = controls(records)
    assert [e["event"] for e in events[:3]] == ["start", "clock", "connect"]
    assert events[1] == {
        "event": "clock",
        "offset_ns": 3 * MS,
        "limit_ns": 250 * MS,
        "within_limit": True,
        "override": False,
        "servers": [
            {"host": "ntp0", "offset_ns": 3 * MS, "round_trip_ns": 20 * MS},
            {"host": "ntp1", "offset_ns": 3 * MS, "round_trip_ns": 20 * MS},
        ],
    }


@pytest.mark.parametrize(
    "measure",
    [clock_at(251 * MS), clock_at(-1400 * MS), clock_at(None, servers=0)],
    ids=["ahead", "behind", "unmeasured"],
)
def test_the_recorder_refuses_to_start_with_a_bad_or_unknown_clock(tmp_path, measure):
    connected = []

    def connect(url, **options):
        connected.append(url)
        return FakeSocket([KRAKEN_SNAPSHOT])

    with pytest.raises(ClockOffsetError, match="--allow-clock-offset"):
        asyncio.run(
            record(
                VENUES["kraken"],
                "BTC-USD",
                tmp_path,
                duration=0.2,
                probe_interval=10,
                depth=25,
                connect=connect,
                fetch=lambda url: b"{}",
                measure_clock=measure,
            )
        )
    assert connected == []  # nothing was contacted
    assert not any(p.is_file() for p in tmp_path.rglob("*"))  # and nothing was written


def test_the_override_records_a_bad_clock_and_says_so(tmp_path):
    _, records, _ = run_kraken(
        tmp_path,
        [FakeSocket([KRAKEN_SNAPSHOT])],
        measure_clock=clock_at(-1400 * MS),
        allow_clock_offset=True,
    )
    clock = controls(records)[1]
    assert (clock["event"], clock["offset_ns"], clock["within_limit"], clock["override"]) == (
        "clock",
        -1400 * MS,
        False,
        True,
    )
    assert any(r.channel == "ws" for r in records)


def test_the_override_is_recorded_even_when_it_was_not_needed(tmp_path):
    _, records, _ = run_kraken(tmp_path, [FakeSocket([KRAKEN_SNAPSHOT])], allow_clock_offset=True)
    clock = controls(records)[1]
    assert (clock["within_limit"], clock["override"]) == (True, True)


def test_the_command_line_refuses_and_exits_2_without_writing(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(recorder, "measure_offset", clock_at(900 * MS))
    arguments = ["--venue", "kraken", "--instrument", "BTC-USD", "--out", str(tmp_path)]
    assert recorder.main(arguments) == 2
    error = capsys.readouterr().err
    assert "900.0 ms" in error and "250 ms" in error and "--allow-clock-offset" in error
    assert list(tmp_path.iterdir()) == []


def test_the_limit_cannot_be_changed_from_the_command_line():
    parser_help = recorder.build_parser().format_help()
    assert "--allow-clock-offset" in parser_help
    assert "offset-ms" not in parser_help and "max-clock" not in parser_help


# --- monotonic receive stamps (ADR 0012) ---------------------------------------------------------


def test_the_session_start_records_this_machines_clock_identity(tmp_path):
    from quantagent.data.clock import clock_identity

    _, records, _ = run_kraken(tmp_path, [FakeSocket([KRAKEN_SNAPSHOT, KRAKEN_UPDATE])])
    start = controls(records)[0]
    assert start["recorder_version"] == recorder.RECORDER_VERSION == 4
    assert start["clock_identity"] == clock_identity().to_json()


def test_every_record_has_a_monotonic_stamp_taken_with_its_receive_time(tmp_path):
    _, records, _ = run_kraken(
        tmp_path, [FakeSocket([KRAKEN_SNAPSHOT, KRAKEN_UPDATE, KRAKEN_UPDATE])], probe_interval=0.1
    )
    assert {r.channel for r in records} >= {"recorder", "ws", "probe"}
    assert all(r.recv_mono_ns is not None for r in records)
    monos = [r.recv_mono_ns for r in records]
    assert monos == sorted(monos)  # file order is arrival order
    offsets = [r.recv_ts - r.recv_mono_ns for r in records]
    assert max(offsets) - min(offsets) < 50_000_000  # the two clocks were read together


def connects(records):
    return [e for e in controls(records) if e["event"] == "connect" and e["connection"] == "ws"]


def test_a_timer_firing_just_before_the_deadline_ends_the_session(tmp_path):
    """The event loop's timer can fire up to 15.6 ms early on Windows. That
    timeout is the session ending, not a reason to open a new connection."""
    early = FakeSocket([KRAKEN_SNAPSHOT], then=0.46)  # 40 ms before the 0.5 s deadline
    _, records, _ = run_kraken(tmp_path, [early], duration=0.5, probe_interval=10)
    assert len(connects(records)) == 1
    stream_ends = [e for e in controls(records)
                   if e["event"] == "disconnect" and e["connection"] == "ws"]  # fmt: skip
    assert [e["reason"] for e in stream_ends] == ["session ended"]


def test_a_timeout_long_before_the_deadline_is_a_failure_and_reconnects(tmp_path):
    first = FakeSocket([KRAKEN_SNAPSHOT], then=0.05)
    second = FakeSocket([KRAKEN_SNAPSHOT])
    _, records, _ = run_kraken(
        tmp_path, [first, second], duration=0.6, probe_interval=10, reconnects=True
    )
    assert len(connects(records)) == 2
    reasons = [e["reason"] for e in controls(records)
               if e["event"] == "disconnect" and e["connection"] == "ws"]  # fmt: skip
    assert reasons[0].startswith("TimeoutError") and reasons[-1] == "session ended"
