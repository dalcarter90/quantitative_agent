# 0011: The recording clock must be synchronized, and is measured

Status: Accepted (owner decision)
Date: 2026-11-12
Amends: 0005, 0010

## Context

`recv_ts` and `knowledge_ts` come from the recording machine's wall clock
(ADR 0005, architecture section 6.2). The design assumed that clock was
right, but nothing said so or checked it.

It was not right. The machine that made the Phase 1c recordings had its
Windows Time service stopped:

- During the recordings (2026-11-11 03:06 to 04:10 UTC) its clock ran about
  1.4 s ahead. No Coinbase message arrived sooner than 1,398 ms after
  Coinbase's own send timestamp, over a route with 20 to 50 ms round trips.
- When the time service was started, at 2026-11-12 00:13:43 UTC, the system
  event log shows the clock stepped back by 1.241 s.
- Most of the ~1.48 s median lag in the Phase 1d quality report is this
  offset.

A fast clock causes no look-ahead (data appears known later than it was), but
it makes every latency and lag figure wrong by an amount that drifts, it
makes `recv_ts` from different machines or days incomparable, and later it
would misplace our own orders against market data. The Phase 1d clock skew
check cannot see it, because a steady offset moves every lag the same way and
stays inside the allowed band.

## Decision

- **Assumption, stated:** every component that stamps wall-clock time
  (recorder now; feed and journal threads later) runs on a clock kept
  synchronized to NTP. Architecture section 6.2 says so.
- **Measured at every session start.** Before it opens any connection or
  writes anything, the recorder measures its offset against four NTP servers
  from independent operators (`quantagent.data.clock`): three SNTP samples
  per server, the fastest reply per server, the median across servers. At
  least two servers must answer.
- **Limit 250 ms.** If the offset is above it, or cannot be measured, the
  recorder refuses to start. The limit is a constant, not a command-line
  option.
- **Override, recorded.** `--allow-clock-offset` records anyway. Using it is
  always written into the log, whether or not it was needed.
- **Stored as stream events** (amending ADR 0010). The recorder writes a
  `clock` control record right after `start`. The adapter turns it into:
  - `CLOCK_OFFSET`: `observed` is the offset in ns (positive: the clock was
    ahead) or null if unmeasured, `expected` the limit, `detail` the
    per-server results;
  - `CLOCK_OVERRIDE`, if the override was used; `detail` says whether the
    offset was within the limit, above it, or not measured.
- **Checked.** A quality check, `recording_clock`, reports each session:
  offset above the limit is an ERROR, unmeasured a WARNING, a session with no
  measurement at all (recorded before this ADR) INFO, and any use of the
  override INFO.
- **Old data is documented, not altered.** The Phase 1c recordings and the
  committed fixtures keep their timestamps. `docs/data-core.md` section 7 and
  the fixture README record their ~1.4 s offset. They report
  `clock_not_recorded` in the quality check.

## Alternatives considered

- **Correct `recv_ts` by the measured offset when recording.** Tempting, but
  the raw log would no longer hold what the clock said, a single measurement
  cannot capture drift during an hour, and a bad measurement would corrupt
  every timestamp. Storing the measurement keeps both facts.
- **Correct the old recordings in the store.** The append-only store would
  allow corrected rows with a later knowledge time, but the offset is known
  only roughly (1.24 to 1.4 s, drifting), and a wrong correction is worse
  than a documented error.
- **Measure only, never refuse.** The check would report it after the fact,
  but by then the hour of data is recorded. Refusing at start costs nothing
  when the clock is right.
- **Rely on the operating system's sync status** (`w32tm /query /status`,
  `timedatectl`). Platform-specific, and a running service is not proof of a
  small offset: it reports what it last did, not how far off the clock is
  now. Measuring directly works the same everywhere.
- **PTP or a GPS clock.** Sub-microsecond accuracy, needed for colocated
  trading. Unnecessary for public data over the internet, where network
  delay is tens of milliseconds.
- **A tighter limit (10 to 50 ms).** Achievable with a running time service,
  but a home connection's NTP round trips are 20 to 50 ms, so the
  measurement's own uncertainty is up to 25 ms. 250 ms is far above that
  noise and far below the 1.4 s problem that prompted this.
- **Measure again during and at the end of a session.** Would catch drift
  within a session. Not required by this decision; easy to add later as more
  `CLOCK_OFFSET` events.

## Consequences

- A recorder on a machine without working NTP, or without network access to
  NTP servers (UDP port 123), refuses to start unless overridden.
- The limit bounds the error of `recv_ts` and `knowledge_ts` at session start
  only. A clock that drifts within a session is not caught.
- The offset measurement adds a few seconds to starting a recording.
- The quality report on old data now shows one INFO finding per session
  (`clock_not_recorded`); the default `--fail-on error` still passes it.
- When feed handlers start stamping time (Phase 6 onward), they need the
  same assumption and the same kind of check. The roadmap notes it.
