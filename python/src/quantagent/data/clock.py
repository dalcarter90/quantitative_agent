"""How far this machine's clock is from true time (ADR 0011).

Every ``recv_ts`` and ``knowledge_ts`` comes from the wall clock, so the
recorder measures the clock against several NTP servers before it starts and
refuses to record if the offset is above ``MAX_OFFSET_NS``.

``offset_ns`` is this machine's clock minus true time: positive means the
clock is ahead, so timestamps taken here are too late by that much.

The SNTP client is a single request and reply per sample (RFC 4330). Each
server is asked a few times and its fastest reply is used, because the
uncertainty of a sample is half its round trip. The result is the median of
the servers' offsets, so one wrong server cannot move it far.
"""

import socket
import statistics
import struct
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass

MAX_OFFSET_NS = 250_000_000
# Four independent operators.
DEFAULT_SERVERS = ("time.cloudflare.com", "time.google.com", "time.windows.com", "pool.ntp.org")
MIN_SERVERS = 2

_NTP_EPOCH_OFFSET = 2_208_988_800  # seconds from 1900-01-01 to 1970-01-01
_NS = 1_000_000_000
_PACKET = 48
_CLIENT_FIRST_BYTE = 0x23  # leap indicator 0, version 4, mode 3 (client)
_SERVER_MODE = 4
_UNSYNCHRONIZED = 3  # leap indicator value meaning "clock not synchronized"


def _to_ntp(nanoseconds: int) -> bytes:
    seconds, remainder = divmod(nanoseconds, _NS)
    return struct.pack("!II", seconds + _NTP_EPOCH_OFFSET, (remainder << 32) // _NS)


def _from_ntp(data: bytes) -> int:
    seconds, fraction = struct.unpack("!II", data)
    return (seconds - _NTP_EPOCH_OFFSET) * _NS + ((fraction * _NS) >> 32)


def sntp_query(host: str, *, port: int = 123, timeout: float = 2.0) -> tuple[int, int]:
    """One SNTP exchange with ``host``. Returns ``(offset_ns, round_trip_ns)``.

    Raises OSError if there is no reply and ValueError if the reply cannot be
    trusted (wrong mode, unsynchronized server, kiss-of-death, or a reply
    that does not answer this request).
    """
    sent = time.time_ns()
    request = bytes([_CLIENT_FIRST_BYTE]) + bytes(39) + _to_ntp(sent)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(timeout)
        sock.sendto(request, (host, port))
        reply, _ = sock.recvfrom(1024)
    arrived = time.time_ns()

    if len(reply) < _PACKET:
        raise ValueError(f"NTP reply is {len(reply)} bytes, expected at least {_PACKET}")
    if reply[0] & 0x07 != _SERVER_MODE:
        raise ValueError(f"NTP reply has mode {reply[0] & 0x07}, not a server reply")
    if reply[0] >> 6 == _UNSYNCHRONIZED:
        raise ValueError("NTP server says its own clock is not synchronized")
    if not 1 <= reply[1] <= 15:
        raise ValueError(f"NTP server stratum {reply[1]} (0 is a kiss-of-death refusal)")
    if reply[24:32] != request[40:48]:
        raise ValueError("NTP reply does not answer this request")
    if reply[32:40] == bytes(8) or reply[40:48] == bytes(8):
        raise ValueError("NTP reply has no receive or transmit time")

    server_received, server_sent = _from_ntp(reply[32:40]), _from_ntp(reply[40:48])
    server_minus_local = ((server_received - sent) + (server_sent - arrived)) // 2
    round_trip = (arrived - sent) - (server_sent - server_received)
    return -server_minus_local, max(round_trip, 0)


@dataclass(frozen=True, slots=True)
class ServerSample:
    host: str
    offset_ns: int | None  # None if the server never gave a usable reply
    round_trip_ns: int | None
    error: str | None


@dataclass(frozen=True)
class ClockMeasurement:
    offset_ns: int | None  # None if too few servers answered
    samples: tuple[ServerSample, ...]

    @property
    def servers_answered(self) -> int:
        return sum(sample.offset_ns is not None for sample in self.samples)

    def within(self, limit_ns: int) -> bool:
        return self.offset_ns is not None and abs(self.offset_ns) <= limit_ns

    def to_json(self) -> dict:
        servers = []
        for sample in self.samples:
            if sample.offset_ns is None:
                servers.append({"host": sample.host, "error": sample.error})
            else:
                servers.append(
                    {
                        "host": sample.host,
                        "offset_ns": sample.offset_ns,
                        "round_trip_ns": sample.round_trip_ns,
                    }
                )
        return {"offset_ns": self.offset_ns, "servers": servers}


Query = Callable[..., tuple[int, int]]


def measure_offset(
    servers: Sequence[str] = DEFAULT_SERVERS,
    *,
    samples: int = 3,
    query: Query = sntp_query,
    timeout: float = 2.0,
) -> ClockMeasurement:
    """Measure this clock against ``servers``; see the module docstring."""
    results = []
    for host in servers:
        best: tuple[int, int] | None = None
        error = None
        for _ in range(samples):
            try:
                offset, round_trip = query(host, timeout=timeout)
            except (OSError, ValueError) as failure:
                error = f"{type(failure).__name__}: {failure}"
                continue
            if best is None or round_trip < best[1]:
                best = (offset, round_trip)
        if best is None:
            results.append(ServerSample(host, None, None, error))
        else:
            results.append(ServerSample(host, best[0], best[1], None))

    answered = [sample.offset_ns for sample in results if sample.offset_ns is not None]
    offset = statistics.median_low(answered) if len(answered) >= MIN_SERVERS else None
    return ClockMeasurement(offset_ns=offset, samples=tuple(results))
