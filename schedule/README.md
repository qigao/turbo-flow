# TurboFlow Schedule

`TurboFlow::Schedule` is a source-only adapter module for generic time events.
It supports fixed intervals, one-shot delays, bounded interval repetition, and
five-field cron expressions. It does not issue HTTP, RPC, S3, email, or other
protocol requests.

The host registers typed configuration and the `.flow` file references the
normal adapter binding:

```flow
source tick adapter schedule.minute
stage process

stage main {
  tick -> process
}
```

```c
turbo_flow_schedule_config_t config = {
    .mode = TURBO_FLOW_SCHEDULE_CRON,
    .cron_expression = "*/5 * * * *",
    .catch_up_limit = 2,
};
turbo_flow_schedule_register_adapter(flow, "schedule.minute", &config, NULL);
```

Interval and one-shot `delay_ms` values must be non-zero. Runtime delays use
the shared `Salts::Core` native timer. Delays larger than a platform timer
period are advanced in bounded slices without narrowing the public delay
range. `repeat_limit == 0` means an unbounded interval; any positive value
stops that source after the specified number of successful publications.
One-shot and cron reject `repeat_limit`. Payloads are copied at registration
and limited to 64 KiB.

## Cron Semantics

Cron expressions are parsed once during registration by
`salts_cron_parse_ex()`. Runtime scheduling uses `salts_cron_next()`; TurboFlow
does not implement or repair cron text. Invalid expressions fail registration
with `SALTS_EINVAL` before the flow starts.

Cron follows Salts local wall-clock semantics and has no per-adapter UTC
or timezone setting. A local minute skipped by a DST transition does not fire.
Repeated wall-clock minutes during a DST rollback are distinct epoch minutes
and may both match. Repeated advancement within the same epoch minute is
suppressed.

At most `1 + catch_up_limit` due ticks are emitted in chronological order per
wake or manual advance. One additional matching time is checked to detect
truncation; the remaining range is skipped without an unbounded scan. Each such
advance increments `catch_up_truncations`. The limit is at most 1024.

`manual_clock` is cron-only and intended for deterministic hosts and tests.
After the flow starts, `turbo_flow_schedule_advance()` advances the borrowed
schedule handle to a supplied `time_t`. It returns the number of ticks emitted,
or a Salts error code. Calls before start, after stop, or on a non-manual
schedule return `SALTS_EINVAL`.

## Lifecycle

The adapter owns a `Salts::Core` native timer, copied payload, and cron
cursor. A short-lived bootstrap thread waits until the Flow has committed its
`STARTED` state before arming the timer; it does not perform delay scheduling.
Timer callbacks are coalesced so one Schedule never publishes concurrently
with itself. Automatic ticks use the Flow-owned bounded async ingress, so the
native timer callback only constructs and submits an event; graph processing
then follows the stage's configured inline, thread, or coroutine executor.
Ingress exhaustion fails the Schedule instead of blocking the timer thread.
Manual cron advancement remains synchronous so its return value is
deterministic.

Stop clears the started flag, joins a pending bootstrap, and destroys the
timer. Timer destruction waits for an active callback, and the Flow then drains
all accepted async publications before releasing runtime data planes and
executors. No future tick can be published after `turbo_flow_stop()` returns.
Snapshot counters are cumulative for the adapter lifetime, while a bounded
interval's repeat count resets on each Flow start.
