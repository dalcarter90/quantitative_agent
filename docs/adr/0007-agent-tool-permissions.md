# 0007: Allow-listed tool server, structurally enforced

Status: Accepted
Date: 2026-11-10

## Context

Layer 5 is an LLM agent that researches and monitors. An LLM can be wrong, can
be manipulated by text it reads (market news, log lines, data fields), and
cannot be made reliable by instructions alone. The owner's hard rules are:

1. No LLM calls in the tick loop.
2. The agent never places orders directly and never modifies risk limits.
3. Every agent action goes through layer 4 and is logged with its reasoning.

## Decision

**The agent's entire capability is an explicit allow-list of tools served by a
tool server in layer 4.** The list is in architecture section 8. Anything not
on the list does not exist for the agent.

Enforcement does not depend on the prompt:

- **Separate process.** The agent runs in its own OS process. It has no
  import path to the order gateway, the engine control bindings, or the store
  writers. It talks only to the tool server.
- **No credentials.** Exchange and broker keys are never loaded into the agent
  process. It gets the LLM API key and nothing else.
- **No such tools.** There is no tool to place, cancel or modify orders,
  submit targets, change limits, change the cost model or validation
  protocol, resume from a pause, promote a strategy, or enable live trading.
  The tool server has no code for these.
- **Risk-reducing only.** The two tools with side effects on trading are
  `request_pause` and `raise_flag`. A pause is always honored and can only be
  lifted by a human.
- **Immutable limits.** Risk limits are read from a file at process start and
  are not writable through any runtime interface.
- **Declarative research.** `propose_hypothesis` takes a spec that selects
  from registered features and model families. The agent does not submit
  code. New feature code arrives by human-reviewed pull request.
- **Pre-registration and budget.** A hypothesis must be registered before
  `run_backtest` will run it. Every run increments a trial counter used to
  deflate reported performance. The final holdout period is not queryable by
  the agent.
- **Audit before effect.** Each call requires a `reasoning` argument. The
  server appends `{ts, model, tool, args, reasoning}` to the audit log, then
  executes, then appends the result. If the log write fails, the call fails.
- **Not in the tick loop.** The agent process has no handle to the core
  thread's queues. Its pause request reaches the core through layer 4's
  control queue like any other operator command.

A permission test suite (written with the tool server, before any LLM is
connected) asserts that the forbidden operations are unreachable.

## Alternatives considered

- **Give the agent broad tools and constrain it by system prompt.** Fast to
  build. One prompt injection or one bad inference from an order. Rejected.
- **Let the agent trade within a small sandbox budget.** Gives a real feedback
  loop. Contradicts hard rule 2, and "small" limits tend to grow.
- **Human approval for every tool call.** Safest, but makes research loops
  useless and trains the human to click approve. Kept only for promotion and
  resume.
- **Let the agent tighten risk limits.** Consistent with the risk-reducing
  rule, but any runtime write path to limits is a path that could one day
  loosen them. Pause covers the need.
- **Let the agent write and run arbitrary research code in a sandbox.** Much
  more expressive. Needs real sandboxing (filesystem, network, resource
  limits) and makes look-ahead bias harder to rule out. Deferred; would need
  its own ADR.
- **In-process agent with a restricted Python API.** Python cannot enforce
  that restriction; anything importable is callable.

## Consequences

- The agent's research is limited to what the feature library and harness can
  express. That is intended at first.
- A false-positive pause costs opportunity and needs a human to resume.
- The audit log contains model reasoning text and may grow large; it is
  rotated, never truncated.
- Adding a tool is a deliberate change: update architecture section 8, the
  permission tests and this ADR (or a superseding one) in the same PR.
