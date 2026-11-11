"""Data core: bitemporal, append-only market data storage.

See docs/architecture.md sections 3.2, 6 and 7, ADR 0005 (representation) and
ADR 0006 (storage format).

This package currently holds the write side only. The point-in-time read
path, PITSession, is the only supported way to read the store and arrives in
roadmap Phase 1b.
"""
