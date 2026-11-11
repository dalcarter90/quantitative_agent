"""Venue adapters: raw log records to rows for the normalized store.

from quantagent.data.adapters import ingest
report = ingest("data/raw/kraken/BTC-USD/20261111T030647Z.qaraw.zst", store)
print(report.summary())
"""

from quantagent.data.adapters.base import (
    Normalized,
    Normalizer,
    Reject,
    Report,
    SequenceAnomaly,
    VenueAdapter,
    ingest,
    normalize,
    parse_rfc3339_ns,
)
from quantagent.data.adapters.coinbase import CoinbaseAdapter
from quantagent.data.adapters.kraken import KrakenAdapter

ADAPTERS: dict[str, type[VenueAdapter]] = {
    KrakenAdapter.venue: KrakenAdapter,
    CoinbaseAdapter.venue: CoinbaseAdapter,
}

__all__ = [
    "ADAPTERS",
    "CoinbaseAdapter",
    "KrakenAdapter",
    "Normalized",
    "Normalizer",
    "Reject",
    "Report",
    "SequenceAnomaly",
    "ingest",
    "normalize",
    "parse_rfc3339_ns",
]
