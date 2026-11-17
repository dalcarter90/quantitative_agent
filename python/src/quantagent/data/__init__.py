"""Data core: capture, an append-only bitemporal store, and its one read path.

``recorder`` and ``rawlog`` capture raw logs; ``adapters`` normalize them and
load them into ``store.AppendOnlyStore``, the only writer; research reads only
through ``session.PITSession`` (and the backtest harness through
``session.ReplayCursor``); ``quality`` checks the data and ``book`` is the
reference order book.

docs/data-core-spec.md is the consolidated description; docs/data-core.md is
the guide to its commands.
"""
