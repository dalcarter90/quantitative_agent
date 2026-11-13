"""Replay order across and within raw logs (ADR 0012).

Within one log the order is ``seq``, whatever ``recv_ts`` says. Logs recorded
on the same machine in the same boot are merged by their monotonic stamps.
"""

import json

import pytest
from test_fixtures import fixture_path

from quantagent.data.clock import ClockIdentity
from quantagent.data.logorder import NotComparable, log_clock_identity, merge_logs
from quantagent.data.rawlog import RawLogWriter

IDENTITY = ClockIdentity("win32", "machine-1", "BootId:58", "QueryPerformanceCounter()", 100)


def write_log(path, stamps, *, identity=IDENTITY, venue="kraken"):
    """A log whose start record names ``identity``, then one ws record per
    (recv_ts, recv_mono_ns) pair in ``stamps`` (the first pair is the start's)."""
    start = {"event": "start", "venue": venue, "instrument": "BTC-USD", "symbol": "BTC/USD",
             "depth": 10, "recorder_version": 3,
             "clock_identity": identity.to_json() if identity else None}  # fmt: skip
    with RawLogWriter(path) as writer:
        (wall, mono), *rest = stamps
        writer.write(venue, "recorder", json.dumps(start).encode(), recv_ts=wall,
                     recv_mono_ns=mono)  # fmt: skip
        for index, (wall, mono) in enumerate(rest):
            writer.write(venue, "ws", f"{path.stem}-{index}".encode(), recv_ts=wall,
                         recv_mono_ns=mono)  # fmt: skip
    return path


def names(merged):
    return [(i, r.payload.decode() if r.channel == "ws" else "start") for i, r in merged]


def test_the_identity_is_read_from_the_start_record(tmp_path):
    assert log_clock_identity(write_log(tmp_path / "a.qaraw", [(1, 10)])) == IDENTITY
    assert log_clock_identity(write_log(tmp_path / "b.qaraw", [(1, 10)], identity=None)) is None
    assert log_clock_identity(fixture_path("kraken")) is None  # recorded before ADR 0012


def test_logs_from_the_same_boot_merge_by_monotonic_stamp_not_wall_time(tmp_path):
    # Log b's wall clock was stepped back by a second; the counter was not.
    a = write_log(tmp_path / "a.qaraw", [(1_000, 10), (1_020, 20), (1_040, 40)])
    b = write_log(tmp_path / "b.qaraw", [(15, 15), (30, 30), (50, 50)])
    assert names(merge_logs([a, b])) == [
        (0, "start"),
        (1, "start"),
        (0, "a-0"),
        (1, "b-0"),
        (0, "a-1"),
        (1, "b-1"),
    ]


def test_equal_monotonic_stamps_keep_log_order_then_seq(tmp_path):
    a = write_log(tmp_path / "a.qaraw", [(1, 10), (2, 20), (3, 20)])
    b = write_log(tmp_path / "b.qaraw", [(1, 10), (2, 20)])
    assert names(merge_logs([a, b])) == [(0, "start"), (1, "start"), (0, "a-0"), (0, "a-1"),
                                         (1, "b-0")]  # fmt: skip


def test_within_one_log_the_order_is_seq_even_if_wall_time_goes_backwards(tmp_path):
    one = write_log(tmp_path / "a.qaraw", [(5_000, 10), (6_000, 20), (4_759, 30), (4_800, 40)])
    assert [r.seq for _, r in merge_logs([one])] == [0, 1, 2, 3]
    assert [r.recv_ts for _, r in merge_logs([one])] == [5_000, 6_000, 4_759, 4_800]


def test_a_single_old_log_is_replayed_in_seq_order():
    merged = list(merge_logs([fixture_path("kraken")]))
    assert [r.seq for _, r in merged] == list(range(len(merged)))


def test_logs_from_different_boots_or_machines_are_not_merged(tmp_path):
    a = write_log(tmp_path / "a.qaraw", [(1, 10), (2, 20)])
    rebooted = ClockIdentity("win32", "machine-1", "BootId:59", "QueryPerformanceCounter()", 100)
    b = write_log(tmp_path / "b.qaraw", [(1, 10), (2, 20)], identity=rebooted)
    with pytest.raises(NotComparable, match="boot"):
        list(merge_logs([a, b]))
    other = ClockIdentity("win32", "machine-2", "BootId:58", "QueryPerformanceCounter()", 100)
    c = write_log(tmp_path / "c.qaraw", [(1, 10)], identity=other)
    with pytest.raises(NotComparable, match="machine"):
        list(merge_logs([a, c]))


def test_logs_without_monotonic_stamps_are_not_merged(tmp_path):
    a = write_log(tmp_path / "a.qaraw", [(1, 10), (2, 20)])
    with pytest.raises(NotComparable, match="clock identity"):
        list(merge_logs([a, fixture_path("kraken")]))
    with pytest.raises(NotComparable, match="clock identity"):
        list(merge_logs([fixture_path("kraken"), fixture_path("coinbase")]))


def test_a_record_without_a_monotonic_stamp_stops_the_merge(tmp_path):
    a = write_log(tmp_path / "a.qaraw", [(1, 10), (2, 20)])
    b = tmp_path / "b.qaraw"
    start = {"event": "start", "clock_identity": IDENTITY.to_json()}
    with RawLogWriter(b) as writer:
        writer.write("kraken", "recorder", json.dumps(start).encode(), recv_ts=1, recv_mono_ns=5)
        writer.write("kraken", "ws", b"no stamp", recv_ts=2)
    with pytest.raises(NotComparable, match="seq 1"):
        list(merge_logs([a, b]))


def test_a_log_whose_monotonic_stamps_go_backwards_is_refused(tmp_path):
    a = write_log(tmp_path / "a.qaraw", [(1, 10), (2, 30), (3, 20)])
    b = write_log(tmp_path / "b.qaraw", [(1, 10)])
    with pytest.raises(ValueError, match="monotonic"):
        list(merge_logs([a, b]))


def test_merge_needs_at_least_one_log():
    with pytest.raises(ValueError):
        list(merge_logs([]))
