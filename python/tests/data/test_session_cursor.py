"""ReplayCursor: one session whose availability time a backtest advances (ADR 0017).

A backtest used to need a new session per decision time. A cursor opens the
store once and hands out one session; the harness keeps the cursor and moves
simulated time forward with ``advance``. The strategy gets only the session,
which has no way to move time.

See conftest.py for the ``history`` store. Its Kraken trades a, b and c were
received at RECV_A, RECV_B and RECV_C.
"""

import copy
import pickle

import duckdb
import pytest
from _builders import INT64_MAX, LAG, NS_PER_DAY, NS_PER_SECOND, T0, trade_table

from quantagent.data.schema import TRADES
from quantagent.data.session import PITSession, ReplayCursor

ALL_TIME = (0, INT64_MAX)
KRK = ("kraken", "ETH-USD")
RECV_A = T0 + LAG
RECV_B = T0 + NS_PER_SECOND + LAG
RECV_C = T0 + NS_PER_DAY + 2 * NS_PER_SECOND + LAG
END = T0 + 3 * NS_PER_DAY


def trade_ids(session):
    return [r["trade_id"] for r in session.trades(*KRK, *ALL_TIME).to_pylist()]


def everything(session):
    """Every read path of a session, as plain values."""
    return {
        "trades": session.trades(*KRK, *ALL_TIME).to_pylist(),
        "window": session.trades(*KRK, RECV_A, RECV_B + 1).to_pylist(),
        "book_events": session.book_events(*KRK, *ALL_TIME).to_pylist(),
        "stream_events": session.stream_events(*KRK, *ALL_TIME).to_pylist(),
        "instruments": session.instruments(*KRK).to_pylist(),
        "instrument": session.instrument(*KRK),
        "sql": session.sql(
            "SELECT venue, trade_id, price, recv_ts, knowledge_ts FROM trades ORDER BY ALL"
        ).to_pylist(),
        "counts": session.sql(
            "SELECT (SELECT count(*) FROM book_events) AS levels, "
            "(SELECT count(*) FROM instruments) AS versions"
        ).to_pylist(),
    }


# --- construction ------------------------------------------------------------------------


def test_a_cursor_opens_one_session_at_its_start_time(history):
    with ReplayCursor(history.root, start=RECV_A, end=END) as cursor:
        session = cursor.session
        assert isinstance(session, PITSession)
        assert cursor.session is session  # one session for the cursor's whole life
        assert (cursor.available_at, session.available_at) == (RECV_A, RECV_A)
        assert session.version == 9000  # the latest committed batch, as for any session
        assert trade_ids(session) == ["a"]


def test_start_end_and_version_are_checked(history):
    for bad in (None, 1.5, "1", True):
        with pytest.raises(TypeError):
            ReplayCursor(history.root, start=bad, end=END)
        with pytest.raises(TypeError):
            ReplayCursor(history.root, start=RECV_A, end=bad)
    for bad in (-1, 2**63):
        with pytest.raises(ValueError):
            ReplayCursor(history.root, start=bad, end=END)
    with pytest.raises(ValueError, match="end"):
        ReplayCursor(history.root, start=RECV_B, end=RECV_A)  # the end is before the start
    with pytest.raises(TypeError):
        ReplayCursor(history.root, RECV_A, END)  # keywords only, like a session's cutoffs
    with pytest.raises(TypeError):
        ReplayCursor(history.root, end=END)  # no default start
    with pytest.raises(ValueError, match="latest committed"):
        ReplayCursor(history.root, start=RECV_A, end=END, version=9001)
    with ReplayCursor(history.root, start=RECV_A, end=END, version=3999) as cursor:
        assert cursor.session.version == 3999


# --- advancing ---------------------------------------------------------------------------


def test_advancing_makes_rows_visible_as_they_were_received(history):
    with ReplayCursor(history.root, start=RECV_A - 1, end=END) as cursor:
        session = cursor.session
        assert trade_ids(session) == []
        seen = []
        for now in (RECV_A, RECV_B - 1, RECV_B, RECV_C - 1, RECV_C, END):
            cursor.advance(now)
            assert (cursor.available_at, session.available_at) == (now, now)
            seen.append(trade_ids(session))
        assert seen == [["a"], ["a"], ["a", "b"], ["a", "b"], ["a", "b", "c"], ["a", "b", "c"]]


@pytest.mark.parametrize("version", [None, 3999, 6000])
def test_at_every_step_the_session_reads_what_a_fresh_session_at_that_time_reads(history, version):
    steps = [RECV_A - NS_PER_DAY, RECV_A - 1, RECV_A, RECV_B, RECV_B + 1, RECV_C, END]
    with ReplayCursor(history.root, start=steps[0], end=END, version=version) as cursor:
        for now in steps:
            cursor.advance(now)
            with PITSession(history.root, available_at=now, version=version) as fresh:
                assert cursor.session.version == fresh.version
                assert everything(cursor.session) == everything(fresh), now


def test_time_only_moves_forward_and_never_past_the_end(history):
    with ReplayCursor(history.root, start=RECV_B, end=RECV_C) as cursor:
        cursor.advance(RECV_B)  # staying put is allowed
        with pytest.raises(ValueError, match="backwards"):
            cursor.advance(RECV_B - 1)
        with pytest.raises(ValueError, match="end"):
            cursor.advance(RECV_C + 1)
        for bad in (None, 1.5, "1", True):
            with pytest.raises(TypeError):
                cursor.advance(bad)
        # A refused move changes nothing.
        assert cursor.available_at == RECV_B
        assert trade_ids(cursor.session) == ["a", "b"]
        cursor.advance(RECV_C)
        assert trade_ids(cursor.session) == ["a", "b", "c"]


# --- the session cannot move time ---------------------------------------------------------


def test_the_session_a_cursor_hands_out_has_no_way_to_move_time(history):
    with ReplayCursor(history.root, start=RECV_A, end=END) as cursor:
        session = cursor.session
        # The same public surface as any session: reads, the two cutoffs, close.
        public = {name for name in dir(session) if not name.startswith("_")}
        assert public == {name for name in dir(PITSession) if not name.startswith("_")}
        assert not {"advance", "cursor", "seek", "set"} & public
        for name in ("available_at", "version", *type(session).__slots__):
            with pytest.raises(AttributeError):
                setattr(session, name, END)
        # It holds no reference to the cursor, which is what can move time.
        for name in type(session).__slots__:
            assert getattr(session, name) is not cursor
        assert trade_ids(session) == ["a"]


def test_forcing_the_sessions_private_time_does_not_reveal_later_rows(history):
    with ReplayCursor(history.root, start=RECV_A, end=END) as cursor:
        session = cursor.session
        for name in type(session).__slots__:
            if isinstance(getattr(session, name, None), int):
                object.__setattr__(session, name, INT64_MAX)
        assert trade_ids(session) == ["a"]


def test_a_cursor_is_fixed_too(history):
    with ReplayCursor(history.root, start=RECV_A, end=END) as cursor:
        for name in ("available_at", "session", "end", *type(cursor).__slots__):
            with pytest.raises(AttributeError):
                setattr(cursor, name, END)
        with pytest.raises(TypeError):
            pickle.dumps(cursor)
        with pytest.raises(TypeError):
            copy.copy(cursor)
    with pytest.raises(TypeError):

        class Wider(ReplayCursor):
            pass


# --- a cursor is a snapshot of the committed store ---------------------------------------


def test_batches_committed_after_the_cursor_opened_never_appear(history):
    with ReplayCursor(history.root, start=RECV_A, end=END) as cursor:
        history.store._append(TRADES, *KRK, trade_table(("z", T0, 1), seq=9))
        cursor.advance(END)
        assert cursor.session.version == 9000
        assert trade_ids(cursor.session) == ["a", "b", "c"]


def test_an_uncommitted_batch_never_appears(history):
    history.store._batch().append(TRADES, *KRK, trade_table(("u", T0, 1), seq=9))
    with ReplayCursor(history.root, start=RECV_A, end=END) as cursor:
        cursor.advance(END)
        assert trade_ids(cursor.session) == ["a", "b", "c"]


# --- the engine's own limit ---------------------------------------------------------------


def _connection(session) -> duckdb.DuckDBPyConnection:
    (found,) = [
        getattr(session, name)
        for name in type(session).__slots__
        if isinstance(getattr(session, name, None), duckdb.DuckDBPyConnection)
    ]
    return found


def test_the_engine_cannot_open_files_received_after_the_cursors_end(history):
    """Deliberately reaching into the private connection, as test_session_sql
    does: even there, files of a receipt day after ``end`` cannot be opened."""
    later_day = history.file(8000)  # trade c, received the day after a and b
    same_day = history.file(2000)
    with ReplayCursor(history.root, start=RECV_A, end=RECV_B) as cursor:
        con = _connection(cursor.session)
        with pytest.raises(duckdb.PermissionException):
            con.sql(f"SELECT * FROM read_parquet('{later_day}')").fetchall()
        assert con.sql(f"SELECT count(*) FROM read_parquet('{same_day}')").fetchall() == [(2,)]
        cursor.advance(RECV_B)
        with pytest.raises(duckdb.PermissionException):
            con.sql(f"SELECT * FROM read_parquet('{later_day}')").fetchall()
        with pytest.raises(duckdb.Error):
            con.execute("SET lock_configuration = false")


# --- closing -----------------------------------------------------------------------------


def test_closing_the_cursor_closes_its_session(history):
    cursor = ReplayCursor(history.root, start=RECV_A, end=END)
    session = cursor.session
    cursor.close()
    with pytest.raises(RuntimeError, match="closed"):
        session.trades(*KRK, *ALL_TIME)
    with pytest.raises(RuntimeError, match="closed"):
        cursor.advance(RECV_B)
    cursor.close()  # closing twice is harmless


def test_a_session_closed_by_its_holder_cannot_be_advanced(history):
    with ReplayCursor(history.root, start=RECV_A, end=END) as cursor:
        cursor.session.close()
        with pytest.raises(RuntimeError, match="closed"):
            cursor.advance(RECV_B)
