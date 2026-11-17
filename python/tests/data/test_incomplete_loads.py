"""A log read from an incomplete load is reported (ADR 0019, owner decision 2026-11-16).

ADR 0018: a log with no complete load is read from its latest incomplete load,
as far as its committed batches go. Such a log may continue beyond what a
session reads, and nothing said so. The quality check ``loads`` now reports
every log whose rows come from an incomplete load, as a WARNING naming the
log, the load, how many batches were committed and the last receipt time read,
whether the load is still being written or failed (an ``InterleavedLogs``, a
crash) before its last batch.

``ingest`` writes a load as one batch, so a failed ingest commits nothing and
leaves nothing to read. Incomplete loads come from a loader that commits a log
in chunks, as a live loader will; ``ChunkedLoader`` below is one.
"""

import os

import pytest
from _builders import INT64_MAX
from test_fixtures import INSTRUMENT, fixture_path

from quantagent.data import quality
from quantagent.data.adapters import InterleavedLogs, Normalizer, ingest
from quantagent.data.quality import QualityConfig, Severity, check_loads
from quantagent.data.rawlog import RawRecord, log_id_of, read_raw_log
from quantagent.data.schema import DATASETS, table_from_rows
from quantagent.data.session import LoadRead, PITSession
from quantagent.data.store import AppendOnlyStore

ALL = (0, INT64_MAX)
CHUNK = 2000  # records per batch


@pytest.fixture(scope="module")
def records():
    return list(read_raw_log(fixture_path("kraken")))


class ChunkedLoader:
    """Loads a raw log one batch per chunk of records, as a live loader would."""

    def __init__(self, store, records):
        self.records, self.normalizer = records, Normalizer()
        self.load = store.load(log_id_of(records[0]))
        self.fed = 0

    def write(self, batch, records):
        for record in records:
            self.normalizer.feed(record)
        for (name, venue, instrument), rows in sorted(self.normalizer.take_rows().items()):
            dataset = DATASETS[name]
            batch.append(dataset, venue, instrument, table_from_rows(dataset, rows))

    def chunk(self, records=None):
        """Commit the next chunk of the log (or ``records``) as one batch."""
        if records is None:
            records = self.records[self.fed : self.fed + CHUNK]
            self.fed += len(records)
        with self.load.batch() as batch:
            self.write(batch, records)


def loads_check(root, window=ALL, *, available_at=INT64_MAX, version=None):
    with PITSession(root, available_at=available_at, version=version) as session:
        return check_loads(session, "kraken", INSTRUMENT, *window, QualityConfig())


def last_read(root, available_at=INT64_MAX):
    """The last record of the log that has a row the session reads: (seq, recv_ts)."""
    union = " UNION ALL ".join(f"SELECT seq, recv_ts FROM {view}"
                               for view in ("trades", "book_events", "stream_events"))  # fmt: skip
    with PITSession(root, available_at=available_at) as session:
        (row,) = session.sql(
            f"SELECT seq, recv_ts FROM ({union}) x ORDER BY seq DESC LIMIT 1"
        ).to_pylist()
        return row["seq"], row["recv_ts"]


def assert_reported(finding, load, batches, root, available_at=INT64_MAX):
    seq, recv_ts = last_read(root, available_at)
    assert (finding.check, finding.what, finding.severity) == (
        "loads", "incomplete_load", Severity.WARNING)  # fmt: skip
    assert finding.log_id == load.log_id and f"load {load.load_id}" in finding.message
    noun = "batch" if batches == 1 else "batches"
    assert f"{batches} {noun} committed" in finding.message
    assert str(recv_ts) in finding.message  # the last receipt time read
    assert (finding.seq, finding.recv_ts) == (seq, recv_ts)  # it points at that record


# --- a load still being written -----------------------------------------------------------


def test_a_load_still_being_written_is_a_warning(tmp_path, records):
    root = tmp_path / "store"
    loader = ChunkedLoader(AppendOnlyStore(root), records)
    loader.chunk()
    loader.chunk()  # the load stays open: its writer goes on

    result = loads_check(root)
    (finding,) = result.findings
    assert_reported(finding, loader.load, 2, root)
    assert CHUNK <= finding.seq < 2 * CHUNK
    assert result.stats == {"logs": 1, "incomplete": 1}

    loader.chunk([])  # a batch with no rows publishes nothing
    loader.chunk()
    (finding,) = loads_check(root).findings
    assert_reported(finding, loader.load, 3, root)
    assert finding.seq >= 2 * CHUNK

    loader.load.finish()
    result = loads_check(root)
    assert result.findings == [] and result.stats == {"logs": 1, "incomplete": 0}


def test_the_session_says_which_load_it_reads_each_log_from(tmp_path, records):
    root = tmp_path / "store"
    loader = ChunkedLoader(AppendOnlyStore(root), records)
    loader.chunk()
    loader.chunk()
    load = loader.load
    with PITSession(root, available_at=INT64_MAX) as session:
        assert session.loads() == (
            LoadRead(log_id=load.log_id, load_id=load.load_id, complete=False, batches=2),
        )
    load.finish()
    with PITSession(root, available_at=INT64_MAX) as session:
        assert session.loads() == (LoadRead(load.log_id, load.load_id, True, 3),)
    # At a version before it completed, the load was incomplete, and still is.
    with PITSession(root, available_at=INT64_MAX, version=load.load_id) as session:
        assert session.loads() == (LoadRead(load.log_id, load.load_id, False, 1),)
    assert len(loads_check(root, version=load.load_id).findings) == 1


# --- a load that failed ----------------------------------------------------------------------


def test_a_load_that_failed_on_interleaved_logs_is_a_warning(tmp_path, records):
    root = tmp_path / "store"
    loader = ChunkedLoader(AppendOnlyStore(root), records)
    loader.chunk()
    # The next chunk interleaves another log's records: the normalizer raises,
    # that batch is abandoned, and the load never completes.
    other = [RawRecord(r.recv_ts + 1, r.seq, r.venue, r.channel, r.payload, r.recv_mono_ns)
             for r in records[:CHUNK]]  # fmt: skip
    mixed = [r for pair in zip(records[CHUNK : 2 * CHUNK], other, strict=True) for r in pair]
    with pytest.raises(InterleavedLogs):
        loader.chunk(mixed)

    (finding,) = loads_check(root).findings
    assert_reported(finding, loader.load, 1, root)
    assert finding.seq < CHUNK


def test_a_load_whose_writer_died_is_a_warning(tmp_path, records):
    root = tmp_path / "store"
    loader = ChunkedLoader(AppendOnlyStore(root), records)
    loader.chunk()
    # The second batch's files are written and the process dies before its
    # marker: they are not part of the store.
    dying = loader.load.batch()
    loader.write(dying, records[CHUNK : 2 * CHUNK])
    del loader, dying

    (finding,) = loads_check(root).findings
    assert "1 batch committed" in finding.message
    assert finding.seq < CHUNK  # nothing of the dead batch was read


def test_a_failed_ingest_commits_nothing_so_there_is_nothing_to_report(tmp_path, monkeypatch):
    """ingest writes a load as one batch: if it fails, nothing of it is readable."""
    root = tmp_path / "store"
    store = AppendOnlyStore(root)

    def no_marker(source, target):
        raise OSError("disk full")

    monkeypatch.setattr(os, "link", no_marker)
    with pytest.raises(OSError, match="disk full"):
        ingest(fixture_path("kraken"), store)
    monkeypatch.undo()
    assert loads_check(root).findings == []
    with PITSession(root, available_at=INT64_MAX) as session:
        assert session.loads() == ()


# --- what is not reported, and where -----------------------------------------------------------


def test_a_complete_load_is_not_reported(tmp_path):
    root = tmp_path / "store"
    ingest(fixture_path("kraken"), AppendOnlyStore(root))
    result = loads_check(root)
    assert result.findings == [] and result.stats == {"logs": 1, "incomplete": 0}


def test_a_reload_in_progress_is_not_reported_while_the_complete_load_is_read(tmp_path, records):
    root = tmp_path / "store"
    store = AppendOnlyStore(root)
    ingest(fixture_path("kraken"), store)
    ChunkedLoader(store, records).chunk()  # a reload, not finished
    with PITSession(root, available_at=INT64_MAX) as session:
        (read,) = session.loads()
    assert read.complete is True and read.batches == 1
    assert loads_check(root).findings == []


def test_the_finding_is_in_every_range_the_log_has_rows_in(tmp_path, records):
    root = tmp_path / "store"
    loader = ChunkedLoader(AppendOnlyStore(root), records)
    loader.chunk()
    loader.chunk()
    _, last = last_read(root)
    middle = records[CHUNK].recv_ts
    assert len(loads_check(root, window=(0, middle)).findings) == 1
    assert len(loads_check(root, window=(middle, last + 1)).findings) == 1
    assert loads_check(root, window=(last + 1, INT64_MAX)).findings == []
    # A session at an earlier time reads less, and the finding says so.
    (finding,) = loads_check(root, available_at=middle).findings
    assert_reported(finding, loader.load, 2, root, available_at=middle)
    assert finding.recv_ts <= middle < last


def test_the_report_shows_the_warning_and_fails_only_when_asked(tmp_path, records, capsys):
    root = tmp_path / "store"
    loader = ChunkedLoader(AppendOnlyStore(root), records)
    loader.chunk()
    loader.chunk()
    day = ["--store", str(root), "--start", "2026-11-11", "--end", "2026-11-11"]
    assert quality.main(day) == 0  # a WARNING, below the default --fail-on error
    out = capsys.readouterr().out
    assert "loads: 1 finding: 1 WARNING" in out and "incomplete_load" in out
    assert quality.main([*day, "--fail-on", "warning"]) == 1
