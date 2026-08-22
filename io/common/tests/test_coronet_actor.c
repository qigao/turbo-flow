#include "flow_coronet_actor.h"

#include "platform.h"
#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

typedef struct flow_actor_test_command_s {
  uint32_t value;
} flow_actor_test_command_t;

typedef struct flow_actor_test_state_s {
  coro_context_t *expected_context;
  uint32_t calls;
  uint32_t last_value;
  uint64_t last_command_id;
  uint32_t delay_ms;
} flow_actor_test_state_t;

typedef struct flow_execution_coro_state_s {
  coro_context_t *expected_context;
  int entered;
  int resumed;
} flow_execution_coro_state_t;

static int flow_actor_test_handler(void *ctx, uint64_t command_id, const void *bytes, size_t size) {
  flow_actor_test_state_t *state = (flow_actor_test_state_t *)ctx;
  const flow_actor_test_command_t *command = (const flow_actor_test_command_t *)bytes;
  if (!state || !command || size != sizeof(*command) ||
      coro_context_current() != state->expected_context) {
    return TURBO_EINVAL;
  }
  state->calls += 1u;
  state->last_value = command->value;
  state->last_command_id = command_id;
  if (state->delay_ms != 0u) turbo_sleep_ms(state->delay_ms);
  return (int)command->value;
}

static turbo_flow_coronet_execution_binding_t flow_actor_borrowed_binding(coro_context_t *ctx) {
  turbo_flow_coronet_execution_binding_t binding = {0};
  binding.size = sizeof(binding);
  binding.kind = TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
  binding.context = ctx;
  return binding;
}

static turbo_flow_coronet_execution_binding_t flow_actor_private_binding(void) {
  turbo_flow_coronet_execution_binding_t binding = {0};
  binding.size = sizeof(binding);
  binding.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  return binding;
}

static int flow_actor_execution_ping(void *ctx) {
  return coro_context_current() == (coro_context_t *)ctx ? TURBO_OK : TURBO_EPROTO;
}

static int flow_execution_yielding_call(void *ctx) {
  flow_execution_coro_state_t *state =
      (flow_execution_coro_state_t *)ctx;
  if (!state || coro_context_current() != state->expected_context ||
      !coro_running())
    return TURBO_EPROTO;
  state->entered = 1;
  coro_sleep(state->expected_context, 2u);
  state->resumed = 1;
  return 47;
}

spec("flow_coronet_actor") {
  it("bounds copied commands and expires queued work before owner mutation") {
    coro_context_t *context = coro_context_create(NULL);
    turbo_flow_coronet_execution_binding_t binding = flow_actor_borrowed_binding(context);
    tf_coronet_execution_t execution;
    tf_coronet_actor_t actor;
    tf_coronet_actor_reply_t *reply = NULL;
    tf_coronet_actor_reply_t *rejected = NULL;
    flow_actor_test_state_t state = {0};
    flow_actor_test_command_t command = {17u};
    int status = TURBO_EALREADY;

    check_true(context != NULL);
    state.expected_context = context;
    check_equal(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    check_equal(tf_coronet_actor_init(&actor, &execution, flow_actor_test_handler, &state, 1u,
                                       sizeof(command)),
                 TURBO_OK);
    check_equal(tf_coronet_actor_submit(&actor, &command, sizeof(command), UINT64_MAX, &reply),
                 TURBO_OK);
    check_true(reply != NULL);
    check_equal(tf_coronet_actor_reply_command_id(reply), 1u);
    command.value = 99u;
    check_equal(tf_coronet_actor_reply_poll(reply, &status), TURBO_EBUSY);
    check_equal(tf_coronet_actor_submit(&actor, &command, sizeof(command), UINT64_MAX, &rejected),
                 TURBO_ENOSPC);
    check_true(rejected == NULL);
    check_equal(tf_coronet_actor_drain(&actor, 0u), TURBO_ETIMEDOUT);

    check_equal(coro_context_run(context, TURBO_RUN_NOWAIT), TURBO_OK);
    check_equal(tf_coronet_actor_reply_wait(reply, 0u, &status), TURBO_OK);
    check_equal(status, 17);
    check_equal(state.calls, 1u);
    check_equal(state.last_value, 17u);
    check_equal(state.last_command_id, 1u);
    tf_coronet_actor_reply_destroy(reply);

    command.value = 23u;
    check_equal(tf_coronet_actor_submit(&actor, &command, sizeof(command),
                                         turbo_hrtime() + UINT64_C(1000000), &reply),
                 TURBO_OK);
    turbo_sleep_ms(5u);
    check_equal(coro_context_run(context, TURBO_RUN_NOWAIT), TURBO_OK);
    check_equal(tf_coronet_actor_reply_wait(reply, 0u, &status), TURBO_OK);
    check_equal(status, TURBO_ETIMEDOUT);
    check_equal(state.calls, 1u);
    tf_coronet_actor_reply_destroy(reply);

    check_equal(tf_coronet_actor_destroy(&actor), TURBO_EBUSY);
    tf_coronet_actor_close(&actor);
    check_equal(tf_coronet_actor_submit(&actor, &command, sizeof(command), UINT64_MAX, &rejected),
                 TURBO_ESHUTDOWN);
    check_equal(tf_coronet_actor_drain(&actor, 0u), TURBO_OK);
    check_equal(tf_coronet_actor_destroy(&actor), TURBO_OK);
    tf_coronet_execution_destroy(&execution);
    coro_context_destroy(context);
  }

  it("returns delayed replies without invoking a cross-thread callback") {
    turbo_flow_coronet_execution_binding_t binding = flow_actor_private_binding();
    tf_coronet_execution_t execution;
    tf_coronet_actor_t actor;
    tf_coronet_actor_reply_t *reply = NULL;
    flow_actor_test_state_t state = {0};
    flow_actor_test_command_t command = {31u};
    int status = TURBO_EALREADY;

    check_equal(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    state.expected_context = execution.context;
    state.delay_ms = 25u;
    check_equal(tf_coronet_actor_init(&actor, &execution, flow_actor_test_handler, &state, 4u,
                                       sizeof(command)),
                 TURBO_OK);
    check_equal(tf_coronet_execution_start(&execution), TURBO_OK);
    check_equal(tf_coronet_actor_submit(&actor, &command, sizeof(command), UINT64_MAX, &reply),
                 TURBO_OK);
    check_equal(tf_coronet_actor_reply_wait(reply, UINT64_C(1000000), &status), TURBO_ETIMEDOUT);
    check_equal(tf_coronet_actor_reply_wait(reply, UINT64_C(500000000), &status), TURBO_OK);
    check_equal(status, 31);
    check_equal(state.calls, 1u);
    tf_coronet_actor_reply_destroy(reply);

    tf_coronet_actor_close(&actor);
    check_equal(tf_coronet_actor_drain(&actor, UINT64_C(500000000)), TURBO_OK);
    check_equal(tf_coronet_actor_destroy(&actor), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
  }

  it("preserves synchronous status after an owner-lane command starts") {
    turbo_flow_coronet_execution_binding_t binding = flow_actor_private_binding();
    tf_coronet_execution_t execution;
    tf_coronet_actor_t actor;
    flow_actor_test_state_t state = {0};
    flow_actor_test_command_t command = {41u};

    check_equal(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    state.expected_context = execution.context;
    state.delay_ms = 10u;
    check_equal(tf_coronet_actor_init(&actor, &execution, flow_actor_test_handler, &state, 2u,
                                       sizeof(command)),
                 TURBO_OK);
    check_equal(tf_coronet_execution_start(&execution), TURBO_OK);
    check_equal(tf_coronet_actor_call(&actor, &command, sizeof(command), UINT64_C(500000000)), 41);
    check_equal(state.calls, 1u);

    tf_coronet_actor_close(&actor);
    check_equal(tf_coronet_actor_drain(&actor, 0u), TURBO_OK);
    check_equal(tf_coronet_actor_destroy(&actor), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
  }

  it("returns only after a coroutine call resumes from I/O-style yielding") {
    turbo_flow_coronet_execution_binding_t binding =
        flow_actor_private_binding();
    tf_coronet_execution_t execution;
    flow_execution_coro_state_t state = {0};

    check_equal(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    state.expected_context = execution.context;
    check_equal(tf_coronet_execution_start(&execution), TURBO_OK);
    check_equal(tf_coronet_execution_call_coro(
                     &execution, flow_execution_yielding_call, &state,
                     UINT64_C(500000000)),
                 47);
    check_true(state.entered);
    check_true(state.resumed);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
  }

  it("explicitly stops and restarts a drained private execution context") {
    enum {
      EXECUTION_RESTART_CYCLES = 64u,
      EXECUTION_STOP_MAX_MS = 1000u,
    };
    turbo_flow_coronet_execution_binding_t binding = flow_actor_private_binding();
    tf_coronet_execution_t execution;

    check_equal(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    for (size_t cycle = 0u; cycle < EXECUTION_RESTART_CYCLES; ++cycle) {
      uint64_t started_at;
      uint64_t elapsed;

      check_equal(tf_coronet_execution_start(&execution), TURBO_OK);
      check_equal(tf_coronet_execution_call(&execution, flow_actor_execution_ping,
                                             execution.context, UINT64_C(500000000)),
                   TURBO_OK);
      started_at = turbo_monotonic_ms();
      tf_coronet_execution_stop(&execution);
      elapsed = turbo_monotonic_ms() - started_at;
      info("cycle=%zu stop_elapsed_ms=%llu", cycle, (unsigned long long)elapsed);
      check_less_equal((size_t)elapsed, (size_t)EXECUTION_STOP_MAX_MS);
    }
    tf_coronet_execution_destroy(&execution);
  }
}
