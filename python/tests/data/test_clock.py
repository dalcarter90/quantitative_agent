"""Clock offset measurement for the recorder (ADR 0011).

The SNTP client is tested against a local fake NTP server on 127.0.0.1, so
no test here needs the network.
"""

import socket
import struct
import threading
import time

import pytest

from quantagent.data.clock import (
    DEFAULT_SERVERS,
    MAX_OFFSET_NS,
    ClockMeasurement,
    ServerSample,
    measure_offset,
    sntp_query,
)

NTP_EPOCH = 2_208_988_800


def ntp_bytes(seconds: float) -> bytes:
    whole = int(seconds)
    return struct.pack("!II", whole + NTP_EPOCH, int((seconds - whole) * 2**32))


class FakeNtpServer:
    """Answers SNTP requests as a server whose clock is ``skew`` seconds
    ahead of this machine's. ``reply`` can replace the answer to test bad ones."""

    def __init__(self, skew=0.0, *, reply=None, delay=0.0):
        self.skew, self.reply, self.delay = skew, reply, delay
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.settimeout(0.2)
        self.port = self.sock.getsockname()[1]
        self.requests = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)

    def _serve(self):
        while not self._stop.is_set():
            try:
                data, address = self.sock.recvfrom(512)
            except TimeoutError:
                continue
            except OSError:
                return
            self.requests.append(data)
            received = time.time() + self.skew
            time.sleep(self.delay)
            if self.reply is not None:
                answer = self.reply(data)
            else:
                answer = (
                    bytes([0x24, 2, 0, 0])  # leap 0, version 4, mode 4 (server); stratum 2
                    + bytes(20)
                    + data[40:48]  # originate: the client's transmit timestamp
                    + ntp_bytes(received)
                    + ntp_bytes(time.time() + self.skew)
                )
            if answer:
                self.sock.sendto(answer, address)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        self._stop.set()
        self._thread.join()
        self.sock.close()


# --- the SNTP client ---------------------------------------------------------------


@pytest.mark.parametrize("skew", [0.0, 1.4, -0.75])
def test_sntp_measures_how_far_this_clock_is_from_the_server(skew):
    with FakeNtpServer(skew) as server:
        offset_ns, round_trip_ns = sntp_query("127.0.0.1", port=server.port, timeout=2)
    # Positive offset means this machine's clock is ahead of the server's.
    assert abs(offset_ns - (-skew * 1e9)) < 20_000_000
    assert 0 <= round_trip_ns < 200_000_000
    assert type(offset_ns) is int and type(round_trip_ns) is int


def test_sntp_request_is_a_client_packet_with_a_transmit_timestamp():
    with FakeNtpServer() as server:
        sntp_query("127.0.0.1", port=server.port, timeout=2)
    (request,) = server.requests
    assert len(request) == 48
    assert request[0] & 0x07 == 3  # mode 3: client
    assert (request[0] >> 3) & 0x07 == 4  # version 4
    assert request[40:48] != bytes(8)  # so the reply can be matched to it


def test_server_processing_time_is_not_counted_as_round_trip():
    with FakeNtpServer(delay=0.3) as server:
        offset_ns, round_trip_ns = sntp_query("127.0.0.1", port=server.port, timeout=2)
    assert round_trip_ns < 100_000_000
    assert abs(offset_ns) < 50_000_000


def reply_with(first, stratum, *, originate=True, times=True):
    """A server reply with the given first byte and stratum."""

    def reply(request):
        echoed = request[40:48] if originate else bytes(8)
        stamps = ntp_bytes(time.time()) * 2 if times else bytes(16)
        return bytes([first, stratum]) + bytes(22) + echoed + stamps

    return reply


@pytest.mark.parametrize(
    ("name", "reply"),
    [
        ("too short", lambda request: b"\x24" * 20),
        ("client mode", reply_with(0x23, 2)),
        ("kiss of death", reply_with(0x24, 0)),
        ("wrong originate", reply_with(0x24, 2, originate=False)),
        ("no transmit time", reply_with(0x24, 2, times=False)),
        ("unsynchronized", reply_with(0xE4, 2)),
    ],
)
def test_bad_replies_are_refused(name, reply):
    with FakeNtpServer(reply=reply) as server:
        with pytest.raises(ValueError):
            sntp_query("127.0.0.1", port=server.port, timeout=1)


def test_no_reply_is_a_timeout():
    with FakeNtpServer(reply=lambda request: b"") as server:
        with pytest.raises(OSError):
            sntp_query("127.0.0.1", port=server.port, timeout=0.3)


# --- combining servers -----------------------------------------------------------------


def scripted(results):
    """A query function returning scripted (offset, round trip) values per host."""
    calls = []

    def query(host, *, timeout):
        calls.append(host)
        outcome = results[host].pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    query.calls = calls
    return query


MS = 1_000_000


def test_each_server_uses_its_fastest_sample_and_the_result_is_the_median():
    query = scripted(
        {
            "a": [(30 * MS, 80 * MS), (10 * MS, 20 * MS), (50 * MS, 90 * MS)],
            "b": [(12 * MS, 30 * MS), (14 * MS, 25 * MS), (11 * MS, 40 * MS)],
            "c": [(400 * MS, 60 * MS), OSError("timed out"), (390 * MS, 70 * MS)],
        }
    )
    measured = measure_offset(("a", "b", "c"), samples=3, query=query)
    assert isinstance(measured, ClockMeasurement)
    assert [(s.host, s.offset_ns, s.round_trip_ns) for s in measured.samples] == [
        ("a", 10 * MS, 20 * MS),
        ("b", 14 * MS, 25 * MS),
        ("c", 400 * MS, 60 * MS),
    ]
    assert measured.offset_ns == 14 * MS  # one wild server cannot move the median far
    assert query.calls == ["a"] * 3 + ["b"] * 3 + ["c"] * 3


def test_a_server_that_never_answers_is_recorded_with_its_error():
    query = scripted({"a": [OSError("timed out")] * 2, "b": [(5 * MS, 9 * MS)] * 2,
                      "c": [(7 * MS, 9 * MS)] * 2})  # fmt: skip
    measured = measure_offset(("a", "b", "c"), samples=2, query=query)
    assert measured.samples[0] == ServerSample("a", None, None, "OSError: timed out")
    assert measured.offset_ns == 5 * MS  # median of two is the lower one
    assert measured.servers_answered == 2


def test_fewer_than_two_answering_servers_is_no_measurement():
    query = scripted({"a": [OSError("down")], "b": [ValueError("kiss of death")],
                      "c": [(5 * MS, 9 * MS)]})  # fmt: skip
    measured = measure_offset(("a", "b", "c"), samples=1, query=query)
    assert measured.offset_ns is None
    assert measured.servers_answered == 1
    assert measured.within(MAX_OFFSET_NS) is False


@pytest.mark.parametrize(
    ("offset", "within"),
    [(0, True), (250 * MS, True), (-250 * MS, True), (250 * MS + 1, False), (-1400 * MS, False)],
)
def test_the_limit_applies_to_the_size_of_the_offset(offset, within):
    measured = ClockMeasurement(offset_ns=offset, samples=())
    assert measured.within(MAX_OFFSET_NS) is within


def test_the_measurement_serializes_for_the_raw_log():
    query = scripted({"a": [(3 * MS, 9 * MS)], "b": [OSError("down")], "c": [(5 * MS, 11 * MS)]})
    measured = measure_offset(("a", "b", "c"), samples=1, query=query)
    assert measured.to_json() == {
        "offset_ns": 3 * MS,
        "servers": [
            {"host": "a", "offset_ns": 3 * MS, "round_trip_ns": 9 * MS},
            {"host": "b", "error": "OSError: down"},
            {"host": "c", "offset_ns": 5 * MS, "round_trip_ns": 11 * MS},
        ],
    }


def test_defaults():
    assert MAX_OFFSET_NS == 250_000_000
    assert len(DEFAULT_SERVERS) >= 3
    assert len({host.split(".")[-2] for host in DEFAULT_SERVERS}) >= 3  # independent operators


@pytest.mark.network
def test_this_machine_against_real_ntp_servers():
    measured = measure_offset()
    assert measured.servers_answered >= 2
    assert measured.offset_ns is not None


# --- clock identity and stamps (ADR 0012) -------------------------------------------------------

import sys  # noqa: E402

from quantagent.data import clock as clock_module  # noqa: E402
from quantagent.data.clock import ClockIdentity, clock_identity, stamp  # noqa: E402


def test_clock_identity_names_this_machine_boot_and_counter():
    identity = clock_identity()
    assert isinstance(identity, ClockIdentity)
    assert identity.platform == sys.platform
    assert identity.machine_id and identity.boot_id
    assert identity.counter == time.get_clock_info("perf_counter").implementation
    assert 0 < identity.counter_resolution_ns <= 1_000_000
    assert identity == clock_identity()  # the same for the whole boot
    assert identity.comparable_with(clock_identity())
    assert ClockIdentity.from_json(identity.to_json()) == identity


def test_identities_are_comparable_only_for_the_same_machine_boot_and_counter():
    base = ClockIdentity("win32", "m1", "BootId:58", "QueryPerformanceCounter()", 100)
    assert base.comparable_with(ClockIdentity("win32", "m1", "BootId:58",
                                              "QueryPerformanceCounter()", 200))  # fmt: skip
    for other in (
        ClockIdentity("win32", "m2", "BootId:58", "QueryPerformanceCounter()", 100),
        ClockIdentity("win32", "m1", "BootId:59", "QueryPerformanceCounter()", 100),
        ClockIdentity("win32", "m1", "BootId:58", "clock_gettime(CLOCK_MONOTONIC)", 100),
        ClockIdentity("linux", "m1", "BootId:58", "QueryPerformanceCounter()", 100),
    ):
        assert not base.comparable_with(other)


def test_an_unknown_machine_or_boot_is_never_comparable(monkeypatch):
    monkeypatch.setattr(clock_module, "_boot_id", lambda: None)
    identity = clock_identity()
    assert identity.boot_id is None
    assert not identity.comparable_with(identity)


def test_stamp_reads_the_wall_clock_and_the_counter_together():
    wall_before, mono_before = time.time_ns(), time.perf_counter_ns()
    wall, mono = stamp()
    assert 0 <= wall - wall_before < 1_000_000_000
    assert 0 <= mono - mono_before < 1_000_000_000
    assert stamp()[1] >= mono  # the counter never goes back
