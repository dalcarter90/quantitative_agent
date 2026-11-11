"""Raw capture log (ADR 0006).

The raw log holds the exact bytes received from a venue, in the order they
were received. It is the source of truth: the normalized store can always be
rebuilt from it, and the replay engine reads it directly.

A log is one segment file. While it is being written it is ``<name>.qaraw``;
sealing it produces ``<name>.qaraw.zst`` (the same bytes, zstd-compressed) and
removes the uncompressed file. A segment is never reopened for writing.

Layout, all integers little-endian::

    file    := MAGIC record*
    record  := length:u32 body crc:u32
    body    := recv_ts:i64 seq:u64 venue_len:u8 venue channel_len:u8 channel payload

``length`` is the size of ``body`` and ``crc`` is the CRC32 of ``body``.
``seq`` starts at 0 and increases by one per record within a segment.
"""

import os
import struct
import time
import zlib
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import pyarrow as pa

MAGIC = b"QARAW\x01\n"
SUFFIX = ".qaraw"
SEALED_SUFFIX = ".qaraw.zst"

_INT64_MAX = 2**63 - 1
_LENGTH = struct.Struct("<I")
_FIXED = struct.Struct("<qQ")
_MAX_BODY = 1 << 30  # a length beyond this means the file is not a raw log
_COMPRESSION_LEVEL = 9


class RawLogError(ValueError):
    """The file is not a valid raw log, or it is damaged."""


@dataclass(frozen=True, slots=True)
class RawRecord:
    recv_ts: int  # when our process received the bytes, ns since the Unix epoch
    seq: int  # position in the segment; (recv_ts, seq) orders replay
    venue: str
    channel: str  # which connection or request the bytes came from
    payload: bytes  # exactly what was received


def _encode_name(kind: str, value: str) -> bytes:
    if not isinstance(value, str):
        raise TypeError(f"{kind} must be str, got {type(value).__name__}")
    encoded = value.encode("utf-8")
    if not 1 <= len(encoded) <= 255:
        raise ValueError(f"{kind} must be 1 to 255 bytes, got {len(encoded)}")
    return encoded


class RawLogWriter:
    """Appends records to a new raw log segment. There is no other operation."""

    def __init__(self, path: str | os.PathLike[str], *, clock: Callable[[], int] = time.time_ns):
        self._path = Path(path)
        if not self._path.name.endswith(SUFFIX):
            raise ValueError(f"raw log file names must end with {SUFFIX}: {self._path.name}")
        self._clock = clock
        self._next_seq = 0
        # "x": fail rather than open a file that already exists.
        self._file: BinaryIO | None = open(self._path, "xb")
        self._file.write(MAGIC)
        self._file.flush()

    @property
    def path(self) -> Path:
        return self._path

    def write(
        self, venue: str, channel: str, payload: bytes, *, recv_ts: int | None = None
    ) -> RawRecord:
        """Append one record. ``recv_ts`` defaults to the clock; pass the time
        the bytes arrived when that was measured earlier."""
        if self._file is None:
            raise ValueError("raw log is closed")
        venue_bytes = _encode_name("venue", venue)
        channel_bytes = _encode_name("channel", channel)
        if not isinstance(payload, bytes):
            raise TypeError(f"payload must be bytes, got {type(payload).__name__}")
        if recv_ts is None:
            recv_ts = self._clock()
        if type(recv_ts) is not int:
            raise TypeError(f"recv_ts must be int nanoseconds, got {type(recv_ts).__name__}")
        if not 0 <= recv_ts <= _INT64_MAX:
            raise ValueError(f"recv_ts={recv_ts} is outside the int64 nanosecond range")

        body = b"".join(
            (
                _FIXED.pack(recv_ts, self._next_seq),
                bytes([len(venue_bytes)]),
                venue_bytes,
                bytes([len(channel_bytes)]),
                channel_bytes,
                payload,
            )
        )
        if len(body) > _MAX_BODY:
            raise ValueError(f"record of {len(body)} bytes is too large")
        self._file.write(_LENGTH.pack(len(body)) + body + _LENGTH.pack(zlib.crc32(body)))
        # Hand each record to the operating system as it is written, so a
        # crash of this process loses at most the record in progress.
        self._file.flush()
        record = RawRecord(recv_ts, self._next_seq, venue, channel, payload)
        self._next_seq += 1
        return record

    def close(self) -> None:
        if self._file is not None:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
            self._file = None

    def seal(self) -> Path:
        """Close the segment, compress it, verify the copy and remove the original."""
        self.close()
        sealed = self._path.with_name(self._path.name[: -len(SUFFIX)] + SEALED_SUFFIX)
        codec = pa.Codec("zstd", compression_level=_COMPRESSION_LEVEL)
        with open(sealed, "xb") as out:
            # One zstd frame for the whole segment. Segments are bounded by a
            # recording session, so they fit in memory.
            out.write(codec.compress(self._path.read_bytes(), asbytes=True))
            out.flush()
            os.fsync(out.fileno())
        count = sum(1 for _ in read_raw_log(sealed))
        if count != self._next_seq:
            raise RawLogError(f"sealed copy has {count} records, expected {self._next_seq}")
        self._path.unlink()
        return sealed

    def __enter__(self) -> "RawLogWriter":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class RawLogReader:
    """Iterates the records of a segment in order.

    An unsealed segment may end in a partial record if the writer was
    interrupted. That record is dropped and ``truncated`` is set once
    iteration finishes. Any other damage raises RawLogError.
    """

    def __init__(self, path: Path):
        self._path = path
        self._sealed = path.name.endswith(SEALED_SUFFIX)
        self.truncated = False

    def __iter__(self) -> Iterator[RawRecord]:
        self.truncated = False
        with open(self._path, "rb") as file:
            if not self._sealed:
                yield from self._records(file)
                return
            try:
                yield from self._records(pa.CompressedInputStream(file, "zstd"))
            except (pa.ArrowException, OSError) as error:
                raise RawLogError(f"damaged sealed raw log: {error}") from None

    def _read(self, stream, size: int) -> bytes:
        parts = []
        remaining = size
        while remaining:
            chunk = stream.read(remaining)
            if not chunk:
                break
            parts.append(chunk)
            remaining -= len(chunk)
        return b"".join(parts)

    def _partial(self) -> None:
        if self._sealed:
            raise RawLogError("sealed raw log ends in the middle of a record")
        self.truncated = True

    def _records(self, stream) -> Iterator[RawRecord]:
        if self._read(stream, len(MAGIC)) != MAGIC:
            raise RawLogError("not a raw log: bad or incomplete file header")
        expected_seq = 0
        while True:
            prefix = self._read(stream, _LENGTH.size)
            if not prefix:
                return
            if len(prefix) < _LENGTH.size:
                return self._partial()
            (length,) = _LENGTH.unpack(prefix)
            if length > _MAX_BODY or length < _FIXED.size + 2:
                raise RawLogError(f"impossible record length {length}")
            rest = self._read(stream, length + _LENGTH.size)
            if len(rest) < length + _LENGTH.size:
                return self._partial()
            body = rest[:length]
            if zlib.crc32(body) != _LENGTH.unpack(rest[length:])[0]:
                raise RawLogError(f"record {expected_seq}: checksum mismatch")

            recv_ts, seq = _FIXED.unpack_from(body)
            if seq != expected_seq:
                raise RawLogError(f"record sequence broken: expected {expected_seq}, found {seq}")
            position = _FIXED.size
            names = []
            for _ in range(2):
                size = body[position]
                names.append(body[position + 1 : position + 1 + size].decode("utf-8"))
                position += 1 + size
            if position > length:
                raise RawLogError(f"record {seq}: malformed names")
            yield RawRecord(recv_ts, seq, names[0], names[1], body[position:])
            expected_seq += 1


def read_raw_log(path: str | os.PathLike[str]) -> RawLogReader:
    """Open a sealed or unsealed segment for reading."""
    path = Path(path)
    if not path.name.endswith((SUFFIX, SEALED_SUFFIX)):
        raise ValueError(f"raw log file names must end with {SUFFIX} or {SEALED_SUFFIX}")
    return RawLogReader(path)
