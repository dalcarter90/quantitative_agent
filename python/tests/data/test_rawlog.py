"""Raw capture log (ADR 0006): exact bytes received, append-only."""

import hashlib

import pytest

from quantagent.data.rawlog import RawLogError, RawLogWriter, RawRecord, read_raw_log

PAYLOADS = [
    b'{"channel":"book","type":"update"}',
    b"",
    bytes(range(256)),
    "café ☃".encode(),
    b"x" * 5_000_000,  # larger than a full Coinbase book snapshot
    b'{"last":true}',
]


def write_log(path, payloads=PAYLOADS, clock=None):
    ticks = iter(range(1_000, 10_000_000, 10))
    with RawLogWriter(path, clock=clock or (lambda: next(ticks))) as writer:
        return [writer.write("kraken", "ws", payload) for payload in payloads]


def test_round_trip_preserves_every_field_and_byte(tmp_path):
    path = tmp_path / "a.qaraw"
    written = write_log(path)

    reader = read_raw_log(path)
    records = list(reader)

    assert records == written
    assert [r.payload for r in records] == PAYLOADS
    assert [r.seq for r in records] == list(range(len(PAYLOADS)))
    assert [r.recv_ts for r in records] == [1_000 + 10 * i for i in range(len(PAYLOADS))]
    assert all(isinstance(r, RawRecord) for r in records)
    assert {(r.venue, r.channel) for r in records} == {("kraken", "ws")}
    assert reader.truncated is False


def test_recv_ts_can_be_given_by_the_caller_and_seq_is_assigned_by_the_writer(tmp_path):
    path = tmp_path / "a.qaraw"
    with RawLogWriter(path, clock=lambda: 7) as writer:
        first = writer.write("coinbase", "ws", b"a", recv_ts=123)
        second = writer.write("coinbase", "rest:product", b"b")
        third = writer.write("coinbase", "recorder", b"c", recv_ts=5)  # clocks can step back
    assert [(r.recv_ts, r.seq, r.channel) for r in (first, second, third)] == [
        (123, 0, "ws"),
        (7, 1, "rest:product"),
        (5, 2, "recorder"),
    ]
    assert list(read_raw_log(path)) == [first, second, third]
    assert "seq" not in RawLogWriter.write.__code__.co_varnames[:6]


def test_an_empty_log_is_valid(tmp_path):
    path = tmp_path / "empty.qaraw"
    RawLogWriter(path).close()
    reader = read_raw_log(path)
    assert list(reader) == []
    assert reader.truncated is False


def test_a_truncated_final_record_is_dropped_and_reported(tmp_path):
    path = tmp_path / "a.qaraw"
    payloads = [b"first record", b"second record", b"the final record, cut short"]
    written = write_log(path, payloads)
    whole = path.read_bytes()
    last_start = whole.rindex(b"the final record") - 60  # inside the second-to-last record

    # Cut at every byte position from inside the last record's header to one
    # byte short of the end.
    boundary = None
    for cut in range(last_start, len(whole)):
        cut_path = tmp_path / f"cut-{cut}.qaraw"
        cut_path.write_bytes(whole[:cut])
        reader = read_raw_log(cut_path)
        records = list(reader)
        if records == written[:1]:
            continue  # the cut landed before the second record ended
        if boundary is None and records == written[:2] and not reader.truncated:
            boundary = cut  # exactly between records two and three
            continue
        assert records == written[:2], cut
        assert reader.truncated is True, cut
    assert boundary is not None


def test_truncation_inside_the_file_header_is_an_error(tmp_path):
    path = tmp_path / "a.qaraw"
    write_log(path, [b"x"])
    short = tmp_path / "short.qaraw"
    short.write_bytes(path.read_bytes()[:3])
    with pytest.raises(RawLogError, match="header"):
        list(read_raw_log(short))


def test_a_file_that_is_not_a_raw_log_is_an_error(tmp_path):
    path = tmp_path / "other.qaraw"
    path.write_bytes(b"PAR1 this is something else entirely")
    with pytest.raises(RawLogError, match="header"):
        list(read_raw_log(path))


def test_corruption_in_a_complete_record_is_an_error_not_a_silent_skip(tmp_path):
    path = tmp_path / "a.qaraw"
    write_log(path, [b"first record", b"second record", b"third record"])
    data = bytearray(path.read_bytes())
    position = data.index(b"second record") + 3
    data[position] ^= 0xFF
    path.write_bytes(bytes(data))

    reader = read_raw_log(path)
    with pytest.raises(RawLogError, match="checksum"):
        list(reader)


def test_a_missing_record_in_the_middle_is_an_error(tmp_path):
    path = tmp_path / "a.qaraw"
    write_log(path, [b"first record", b"second record", b"third record"])
    data = path.read_bytes()
    start = data.index(b"second record") - 30
    # Find the exact record boundaries by re-reading sizes: remove record two.
    one = tmp_path / "one.qaraw"
    write_log(one, [b"first record"])
    two = tmp_path / "two.qaraw"
    write_log(two, [b"first record", b"second record"])
    first_end, second_end = len(one.read_bytes()), len(two.read_bytes())
    assert first_end < start + 30 < second_end
    path.write_bytes(data[:first_end] + data[second_end:])
    with pytest.raises(RawLogError, match="sequence"):
        list(read_raw_log(path))


def test_seal_compresses_removes_the_original_and_reads_back_identically(tmp_path):
    path = tmp_path / "a.qaraw"
    payloads = [b'{"channel":"book","price":73468.5,"qty":0.20416909}' * 20] * 500
    writer = RawLogWriter(path, clock=lambda: 42)
    written = [writer.write("kraken", "ws", p) for p in payloads]
    plain_size = path.stat().st_size
    sealed = writer.seal()

    assert sealed == tmp_path / "a.qaraw.zst"
    assert not path.exists()
    assert sealed.stat().st_size < plain_size / 10
    reader = read_raw_log(sealed)
    assert list(reader) == written
    assert reader.truncated is False


def test_a_truncated_sealed_log_is_an_error(tmp_path):
    path = tmp_path / "a.qaraw"
    writer = RawLogWriter(path)
    for i in range(200):
        writer.write("kraken", "ws", hashlib.sha256(str(i).encode()).digest() * 8)
    sealed = writer.seal()
    data = sealed.read_bytes()
    sealed.write_bytes(data[: len(data) // 2])
    with pytest.raises(RawLogError):
        list(read_raw_log(sealed))


def test_the_writer_never_opens_an_existing_file(tmp_path):
    path = tmp_path / "a.qaraw"
    write_log(path, [b"kept"])
    before = path.read_bytes()
    with pytest.raises(FileExistsError):
        RawLogWriter(path)
    assert path.read_bytes() == before

    sealed_first = tmp_path / "b.qaraw"
    write_log(sealed_first, [b"x"])
    (tmp_path / "b.qaraw.zst").write_bytes(b"already here")
    writer = RawLogWriter(tmp_path / "c.qaraw")
    writer.write("kraken", "ws", b"x")
    (tmp_path / "c.qaraw.zst").write_bytes(b"already here")
    with pytest.raises(FileExistsError):
        writer.seal()
    assert (tmp_path / "c.qaraw.zst").read_bytes() == b"already here"
    assert (tmp_path / "c.qaraw").exists()


def test_the_writer_is_append_only(tmp_path):
    public = {name for name in dir(RawLogWriter) if not name.startswith("_")}
    assert public == {"write", "close", "seal", "path"}
    writer = RawLogWriter(tmp_path / "a.qaraw")
    writer.write("kraken", "ws", b"x")
    writer.close()
    with pytest.raises(ValueError, match="closed"):
        writer.write("kraken", "ws", b"y")
    writer.close()  # closing twice is harmless


@pytest.mark.parametrize(
    ("venue", "channel", "payload", "error"),
    [
        ("", "ws", b"x", ValueError),
        ("kraken", "", b"x", ValueError),
        ("k" * 256, "ws", b"x", ValueError),
        ("kraken", "c" * 256, b"x", ValueError),
        ("kraken", "ws", "text", TypeError),
        ("kraken", "ws", None, TypeError),
        (b"kraken", "ws", b"x", TypeError),
    ],
)
def test_write_validates_its_arguments(tmp_path, venue, channel, payload, error):
    with RawLogWriter(tmp_path / "a.qaraw") as writer:
        with pytest.raises(error):
            writer.write(venue, channel, payload)
        ok = writer.write("kraken", "ws", b"fine")
    assert ok.seq == 0  # a rejected write does not consume a sequence number
    assert list(read_raw_log(tmp_path / "a.qaraw")) == [ok]


@pytest.mark.parametrize("bad", [-1, 2**63, 1.5, "1", True])
def test_recv_ts_must_be_int64_nanoseconds(tmp_path, bad):
    with RawLogWriter(tmp_path / "a.qaraw") as writer:
        with pytest.raises((TypeError, ValueError)):
            writer.write("kraken", "ws", b"x", recv_ts=bad)


def test_file_names_must_use_the_raw_log_suffix(tmp_path):
    with pytest.raises(ValueError, match="qaraw"):
        RawLogWriter(tmp_path / "a.bin")
    (tmp_path / "a.bin").write_bytes(b"")
    with pytest.raises(ValueError, match="qaraw"):
        read_raw_log(tmp_path / "a.bin")


def test_records_are_readable_while_the_log_is_still_open(tmp_path):
    path = tmp_path / "a.qaraw"
    with RawLogWriter(path) as writer:
        first = writer.write("kraken", "ws", b"one")
        assert list(read_raw_log(path)) == [first]
        second = writer.write("kraken", "ws", b"two")
        assert list(read_raw_log(path)) == [first, second]


# --- version 2: monotonic receive stamps (ADR 0012) -----------------------------------------


def test_records_carry_a_monotonic_stamp_taken_with_the_wall_time(tmp_path):
    path = tmp_path / "a.qaraw"
    walls, monos = iter(range(100, 10**6, 10)), iter(range(5_000, 10**7, 3))
    with RawLogWriter(path, clock=lambda: next(walls), monotonic=lambda: next(monos)) as writer:
        stamped = writer.write("kraken", "ws", b"x")
        given = writer.write("kraken", "ws", b"y", recv_ts=999, recv_mono_ns=777)
        copied = writer.write("kraken", "ws", b"z", recv_ts=555)  # from a log without stamps
    assert (stamped.recv_ts, stamped.recv_mono_ns) == (100, 5_000)
    assert (given.recv_ts, given.recv_mono_ns) == (999, 777)
    assert (copied.recv_ts, copied.recv_mono_ns) == (555, None)
    reader = read_raw_log(path)
    assert list(reader) == [stamped, given, copied]
    assert reader.version == 2


def test_the_writer_always_writes_version_2(tmp_path):
    path = tmp_path / "a.qaraw"
    RawLogWriter(path).close()
    assert path.read_bytes() == b"QARAW\x02\n"


def test_by_default_both_stamps_come_from_the_system_clocks(tmp_path):
    import time

    with RawLogWriter(tmp_path / "a.qaraw") as writer:
        wall, mono = time.time_ns(), time.perf_counter_ns()
        record = writer.write("kraken", "ws", b"x")
    assert 0 <= record.recv_ts - wall < 1_000_000_000
    assert 0 <= record.recv_mono_ns - mono < 1_000_000_000


@pytest.mark.parametrize(
    ("arguments", "error"),
    [
        ({"recv_mono_ns": 5}, ValueError),  # a monotonic stamp without its wall time
        ({"recv_ts": 5, "recv_mono_ns": -1}, ValueError),
        ({"recv_ts": 5, "recv_mono_ns": 2**63}, ValueError),
        ({"recv_ts": 5, "recv_mono_ns": 1.5}, TypeError),
        ({"recv_ts": 5, "recv_mono_ns": True}, TypeError),
    ],
)
def test_monotonic_stamps_are_validated(tmp_path, arguments, error):
    with RawLogWriter(tmp_path / "a.qaraw") as writer:
        with pytest.raises(error):
            writer.write("kraken", "ws", b"x", **arguments)


def test_version_1_logs_are_still_read_and_have_no_monotonic_stamps():
    from test_fixtures import fixture_path

    for venue in ("kraken", "coinbase"):
        reader = read_raw_log(fixture_path(venue))
        records = list(reader)
        assert reader.version == 1
        assert records and {r.recv_mono_ns for r in records} == {None}
