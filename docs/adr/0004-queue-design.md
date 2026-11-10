# 0004: In-house bounded SPSC ring buffer

Status: Accepted
Date: 2026-11-10

## Context

The threading model (architecture section 5) has a single core thread that
owns all trading state, fed by and feeding I/O threads. Every edge between
threads has exactly one producer and one consumer. The queue on those edges
must not allocate, lock or make system calls, and its behavior when full must
be an explicit policy, not an accident.

## Decision

Implement one queue type in `cpp/`:

- Bounded single-producer single-consumer ring buffer; capacity is a power of
  two fixed at construction, storage allocated once.
- Elements are fixed-size, trivially copyable message structs. No pointers to
  heap data cross a thread boundary.
- Head and tail indices are `std::atomic<size_t>` on separate cache lines
  (`alignas` to the destructive interference size), with each side keeping a
  cached copy of the other's index to avoid touching the shared line on most
  operations.
- `try_push` and `try_pop` only. They never block and never fail spuriously.
  Waiting (spin, yield, sleep) is the caller's configurable strategy.
- Acquire/release memory ordering, no `seq_cst`.

Full-queue policy is decided per edge by the producer:

| Edge | When full |
|------|-----------|
| Feed → core (market data) | Mark the instrument's book stale and resync. Never drop silently |
| Core → execution (orders) | Reject the order, journal the reject |
| Execution → core (acks/fills) | Execution thread retries; fills are never dropped |
| Core → journal | Trip the kill switch. No trading without an audit trail |
| Control → core (targets, pause) | Caller gets an error; a pause is retried until accepted |

Multi-reader data (snapshots for Python) does not use a queue; it uses a
seqlock over a fixed-size struct.

## Alternatives considered

- **`rigtorp::SPSCQueue`.** The design above is essentially this one. It is
  about a hundred lines; owning it means no dependency, our own message
  constraints enforced with `static_assert`, and our own tests and benchmark.
  If the in-house version shows a defect, switching to this is trivial.
- **`boost::lockfree::spsc_queue`.** Correct and well tested, but pulls in
  Boost for one container and does not pad/cached-index as aggressively.
- **`moodycamel::ReaderWriterQueue`.** Fast, but grows by allocating blocks
  unless carefully configured; unbounded growth hides backpressure.
- **LMAX Disruptor-style ring with multiple consumers.** Lets the journal and
  core read the same sequence without a second queue. More machinery than
  needed while each edge has one consumer.
- **MPSC queue into the core.** One queue instead of several, but it loses the
  deterministic merge order across inputs and costs more per operation.
- **Mutex + condition variable.** Simple, but blocking and syscalls on the
  hot path defeat the purpose.

## Consequences

- One queue per edge, so the core polls several queues. The poll order is
  fixed and part of the determinism contract.
- Concurrency bugs in lock-free code are subtle. The queue phase must include
  a two-thread stress test and, once Linux builds exist, a ThreadSanitizer run
  (TSan is not available with MSVC).
- Message structs must stay trivially copyable; this constrains event design
  (fixed-size fields, no `std::string`).
