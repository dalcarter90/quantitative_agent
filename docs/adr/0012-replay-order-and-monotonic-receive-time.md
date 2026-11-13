# 0012: Replay order is `seq`; receive time gains a monotonic stamp

Status: Accepted (owner decision), amended by 0013, which settles the "Open" point below
Date: 2026-11-12
Amends: 0005, 0006, 0008

## Context

ADR 0005 and the architecture defined the total order for replay as
`(recv_ts, seq)`. `recv_ts` is the wall clock, and a time service can step the
wall clock. On the machine that made the Phase 1c recordings, Windows Time
stepped it back by 1.241 s when it started (ADR 0011). A step backwards in the
middle of a recording would put later records before earlier ones in
`(recv_ts, seq)` order. No existing log has one (every log was checked), but
nothing prevented it.

Two more facts made `recv_ts` a poor ordering key:

- On Windows, Python 3.12's wall clock ticks only every 0.3 to 0.5 ms
  (nominally up to 15.6 ms). About 70% of the records in the one-hour Kraken
  log share their `recv_ts` with the record before.
- Logs recorded at the same time (one per venue) can only be interleaved by a
  common clock, and the wall clock is the one that can jump.

## Decision

- **Within one raw log, replay order is `seq` alone.** `seq` is the order the
  recorder wrote records, which is the order they arrived. `recv_ts` is a
  measurement of when, not an ordering key.
- **Every raw record gains `recv_mono_ns`**, the system's monotonic
  high-resolution counter (Python's `time.perf_counter_ns`:
  `QueryPerformanceCounter` on Windows, `CLOCK_MONOTONIC` on Linux), read in
  the same call as `recv_ts`. No time service can step it. `recv_ts` stays the
  absolute time.
- **The raw log format goes to version 2**, with `recv_mono` after `recv_ts`
  in each record (-1 when a record has none). The reader reads both versions;
  the writer writes only version 2.
- **The recorder records its clock identity** in every session's start
  record: platform, machine id (Windows `MachineGuid`, Linux
  `/etc/machine-id`), boot id (Windows `BootId`, incremented every boot; Linux
  `/proc/sys/kernel/random/boot_id`), the counter's implementation, and its
  resolution. Two logs' monotonic stamps are comparable only if machine, boot
  and counter all match; an unknown machine or boot is never comparable.
- **Logs from the same machine and boot are merged by `recv_mono_ns`**
  (`quantagent.data.logorder.merge_logs`), then log, then `seq`. Logs that are
  not comparable, or have no monotonic stamps, are refused, not merged by wall
  time.
- **Clock steps are recorded and checked.** The adapter compares
  `recv_ts - recv_mono_ns` between consecutive records of a session. A change
  larger than 25 ms is a `CLOCK_STEP` stream event whose `observed` is the
  signed size (negative: the wall clock went back). The `recording_clock`
  quality check reports any step beyond `max_clock_step_ns` (default 50 ms) as
  an ERROR, with its size and direction. A session that has an NTP measurement
  but no monotonic stamps is reported as INFO.
- **Existing logs are kept as they are.** The Phase 1c recordings and the
  committed fixtures are version 1: they are read with `recv_mono_ns` None,
  never rewritten, and documented as lacking the field. Their sessions report
  `clock_not_recorded` (INFO), which now also says steps cannot be detected.

## Alternatives considered

- **Keep `(recv_ts, seq)` and rely on ADR 0011's clock check.** The NTP check
  bounds the offset at session start, but a time service can still step the
  clock during a session; that is exactly what it is for.
- **Order by `recv_mono_ns` within a log too.** Equivalent to `seq` when the
  log is intact, but `seq` is already the writer's order and is checked by the
  reader. The monotonic stamp is needed only between logs.
- **Correct `recv_ts` when a step is detected.** It would hide what the clock
  said, and the right correction is not knowable from the stream alone (a step
  forward may be the clock becoming right). Recording the step keeps both
  facts.
- **Use `time.monotonic_ns`.** On Windows it is `GetTickCount64`, with
  15.6 ms resolution. `perf_counter_ns` has 100 ns.
- **Infer the boot from uptime (`wall time - counter`).** That difference
  moves whenever the wall clock is stepped, which is the event in question. A
  boot counter from the operating system does not.
- **Merge non-comparable logs by `recv_ts`.** Exactly the failure this ADR
  removes. Refusing is honest; a caller that accepts the risk can still read
  each log alone.

## Consequences

- Replay (the Phase 7 engine) reads raw logs in `seq` order and merges
  several logs with `merge_logs`.
- A clock step forward does not reorder anything but leaves a jump in
  `recv_ts`; a step back means that within the session, `recv_ts` is not in
  arrival order. Both are reported.
- On the first 60-second version 2 recordings, `recv_ts - recv_mono_ns`
  moved at most 1.16 ms between consecutive records, so the 25 ms floor is
  about 20 times the observed noise. No record shared its monotonic stamp with
  another.
- Slow drift (a time service slewing rather than stepping) is not a step and
  produces no event. It was under 0.3 ms per minute on those recordings.
- The reference book reconstructor refuses to replay a session with a stored
  backward `CLOCK_STEP` (`ReplayOrderUnreliable`), because of the open point
  below.

## Open: the normalized store does not know which log a row came from

The decision is fully implemented for raw logs. It is not yet possible for
data in the normalized store, because no stored row says which raw log it came
from, and `seq` restarts at 0 in every log. `PITSession` returns rows ordered
by `(recv_ts, seq)`, and the reconstructor and the clock skew check use that
order. That is arrival order for every log recorded so far, and the
reconstructor now refuses the one case where it is not (a backward step).

The same gap affects keys. Book events and stream events are keyed by
`(recv_ts, seq, ...)` (ADRs 0009, 0010). Two logs recorded at the same time
for the same venue and instrument could produce the same key, and one row
would then be taken as a correction of the other.

Settling both needs a schema change, for example a `log_id` (and
`recv_mono_ns`) column on every dataset, with `log_id` added to the keys. That
is the owner's decision; until it is made, run at most one recorder per venue
and instrument at a time.
