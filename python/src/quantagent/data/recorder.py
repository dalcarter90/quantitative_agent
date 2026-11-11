"""Record public market data from a venue into a raw log (ADR 0006).

    python -m quantagent.data.recorder --venue kraken --instrument BTC-USD \
        --out data/raw --duration 3600

Only public market data is requested. The recorder sends no credentials and
reads none.

What goes into the log, by channel:

- ``recorder``: control records written by this program, as JSON. They mark
  the start of the session, every connect, subscribe request and disconnect,
  and each REST request. Adapters use them to know where one connection ends
  and the next begins.
- ``ws``: every message received on the main streaming connection, verbatim.
- ``probe``: messages from short-lived extra connections opened every
  ``probe_interval`` seconds to fetch a fresh order book snapshot. They are an
  independent image of the book for verifying a reconstruction.
- ``rest:<name>``: the body of a REST response, verbatim.
"""

import argparse
import asyncio
import json
import time
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quantagent.data.rawlog import RawLogWriter

RECORDER_VERSION = 1
CONTROL = "recorder"
STREAM = "ws"
PROBE = "probe"


@dataclass(frozen=True)
class VenueSpec:
    name: str
    url: str
    symbol: Callable[[str], str]  # our instrument name -> the venue's
    subscriptions: Callable[[str, int], list[dict]]
    probe_subscriptions: Callable[[str, int], list[dict]]
    rest: Callable[[str], list[tuple[str, str]]]  # (channel, url) fetched before connecting
    is_book_snapshot: Callable[[str], bool]


def _kraken_book(symbol: str, depth: int) -> dict:
    return {
        "method": "subscribe",
        "params": {"channel": "book", "symbol": [symbol], "depth": depth, "snapshot": True},
    }


def _coinbase(symbol: str, channel: str) -> dict:
    return {"type": "subscribe", "product_ids": [symbol], "channel": channel}


VENUES: dict[str, VenueSpec] = {
    # https://docs.kraken.com/api/docs/websocket-v2/book
    "kraken": VenueSpec(
        name="kraken",
        url="wss://ws.kraken.com/v2",
        symbol=lambda instrument: instrument.replace("-", "/"),
        subscriptions=lambda symbol, depth: [
            {"method": "subscribe", "params": {"channel": "instrument", "snapshot": True}},
            _kraken_book(symbol, depth),
            {
                "method": "subscribe",
                "params": {"channel": "trade", "symbol": [symbol], "snapshot": True},
            },
        ],
        probe_subscriptions=lambda symbol, depth: [_kraken_book(symbol, depth)],
        rest=lambda symbol: [],
        is_book_snapshot=lambda text: '"channel":"book"' in text and '"type":"snapshot"' in text,
    ),
    # https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels
    # level2, market_trades and heartbeats are public; no JWT is required.
    "coinbase": VenueSpec(
        name="coinbase",
        url="wss://advanced-trade-ws.coinbase.com",
        symbol=lambda instrument: instrument,
        subscriptions=lambda symbol, depth: [
            _coinbase(symbol, "heartbeats"),
            _coinbase(symbol, "level2"),
            _coinbase(symbol, "market_trades"),
        ],
        probe_subscriptions=lambda symbol, depth: [_coinbase(symbol, "level2")],
        rest=lambda symbol: [
            ("rest:product", f"https://api.coinbase.com/api/v3/brokerage/market/products/{symbol}")
        ],
        is_book_snapshot=lambda text: (
            '"channel":"l2_data"' in text[:200] and '"type":"snapshot"' in text[:400]
        ),
    ),
}


def _http_get(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "quantagent-recorder"})
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - fixed https URLs
        return response.read()


def _default_connect(url: str, **options: Any):
    import websockets  # imported here so the rest of the package does not need it

    return websockets.connect(url, **options)


async def record(
    spec: VenueSpec,
    instrument: str,
    out_dir: str | Path,
    *,
    duration: float,
    probe_interval: float,
    depth: int,
    connect: Callable[..., Any] = _default_connect,
    fetch: Callable[[str], bytes] = _http_get,
    reconnect_delay: float = 2.0,
) -> Path:
    """Record for ``duration`` seconds and return the path of the sealed log."""
    symbol = spec.symbol(instrument)
    directory = Path(out_dir) / spec.name / instrument
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    writer = RawLogWriter(directory / f"{stamp}.qaraw")
    deadline = time.monotonic() + duration

    def control(**fields: Any) -> None:
        writer.write(spec.name, CONTROL, json.dumps(fields, separators=(",", ":")).encode())

    def remaining() -> float:
        return deadline - time.monotonic()

    async def open_and_subscribe(socket: Any, connection: str, requests: list[dict]) -> None:
        control(event="connect", connection=connection, url=spec.url)
        for request in requests:
            await socket.send(json.dumps(request))
            control(event="subscribe", connection=connection, request=request)

    async def stream() -> None:
        while remaining() > 0:
            reason = "session ended"
            try:
                async with connect(spec.url, max_size=None) as socket:
                    await open_and_subscribe(socket, STREAM, spec.subscriptions(symbol, depth))
                    while remaining() > 0:
                        try:
                            message = await asyncio.wait_for(socket.recv(), timeout=remaining())
                        except TimeoutError:
                            break
                        received = time.time_ns()
                        payload = message.encode() if isinstance(message, str) else bytes(message)
                        writer.write(spec.name, STREAM, payload, recv_ts=received)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - any failure means reconnect
                reason = f"{type(error).__name__}: {error}"
            control(event="disconnect", connection=STREAM, reason=reason)
            if remaining() > 0:
                await asyncio.sleep(min(reconnect_delay, max(remaining(), 0)))

    async def probe_once() -> None:
        reason = "snapshot received"
        try:
            async with connect(spec.url, max_size=None) as socket:
                await open_and_subscribe(socket, PROBE, spec.probe_subscriptions(symbol, depth))
                give_up = time.monotonic() + 20
                while time.monotonic() < give_up:
                    message = await asyncio.wait_for(socket.recv(), timeout=20)
                    received = time.time_ns()
                    text = message if isinstance(message, str) else bytes(message).decode()
                    writer.write(spec.name, PROBE, text.encode(), recv_ts=received)
                    if spec.is_book_snapshot(text):
                        break
                else:
                    reason = "no snapshot within 20 seconds"
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            reason = f"{type(error).__name__}: {error}"
        control(event="disconnect", connection=PROBE, reason=reason)

    async def probes() -> None:
        while remaining() > probe_interval:
            await asyncio.sleep(probe_interval)
            if remaining() > 0:
                await probe_once()

    try:
        control(
            event="start",
            venue=spec.name,
            instrument=instrument,
            symbol=symbol,
            depth=depth,
            recorder_version=RECORDER_VERSION,
        )
        # Instrument metadata first, so it is known before any market data.
        for channel, url in spec.rest(symbol):
            control(event="rest", channel=channel, url=url)
            body = await asyncio.to_thread(fetch, url)
            writer.write(spec.name, channel, body)

        prober = asyncio.create_task(probes())
        try:
            await stream()
        finally:
            prober.cancel()
            await asyncio.gather(prober, return_exceptions=True)
        control(event="stop")
    finally:
        sealed = writer.seal()
    return sealed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Record public market data into a raw log.")
    parser.add_argument("--venue", required=True, choices=sorted(VENUES))
    parser.add_argument("--instrument", required=True, help="for example BTC-USD")
    parser.add_argument("--out", required=True, help="directory for raw logs, for example data/raw")
    parser.add_argument("--duration", type=float, default=3600, help="seconds to record")
    parser.add_argument("--probe-interval", type=float, default=600, help="seconds between probes")
    parser.add_argument("--depth", type=int, default=25, help="book depth where the venue has one")
    args = parser.parse_args(argv)

    path = asyncio.run(
        record(
            VENUES[args.venue],
            args.instrument,
            args.out,
            duration=args.duration,
            probe_interval=args.probe_interval,
            depth=args.depth,
        )
    )
    print(f"wrote {path} ({path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
