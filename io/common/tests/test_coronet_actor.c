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
    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    check_int_eq(tf_coronet_actor_init(&actor, &execution, flow_actor_test_handler, &state, 1u,
                                       sizeof(command)),
                 TURBO_OK);
    check_int_eq(tf_coronet_actor_submit(&actor, &command, sizeof(command), UINT64_MAX, &reply),
                 TURBO_OK);
    check_true(reply != NULL);
    check_uint_eq(tf_coronet_actor_reply_command_id(reply), 1u);
    command.value = 99u;
    check_int_eq(tf_coronet_actor_reply_poll(reply, &status), TURBO_EBUSY);
    check_int_eq(tf_coronet_actor_submit(&actor, &command, sizeof(command), UINT64_MAX, &rejected),
                 TURBO_ENOSPC);
    check_true(rejected == NULL);
    check_int_eq(tf_coronet_actor_drain(&actor, 0u), TURBO_ETIMEDOUT);

    check_int_eq(coro_context_run(context, TURBO_RUN_NOWAIT), TURBO_OK);
    check_int_eq(tf_coronet_actor_reply_wait(reply, 0u, &status), TURBO_OK);
    check_int_eq(status, 17);
    check_uint_eq(state.calls, 1u);
    check_uint_eq(state.last_value, 17u);
    check_uint_eq(state.last_command_id, 1u);
    tf_coronet_actor_reply_destroy(reply);

    command.value = 23u;
    check_int_eq(tf_coronet_actor_submit(&actor, &command, sizeof(command),
                                         turbo_hrtime() + UINT64_C(1000000), &reply),
                 TURBO_OK);
    turbo_sleep_ms(5u);
    check_int_eq(coro_context_run(context, TURBO_RUN_NOWAIT), TURBO_OK);
    check_int_eq(tf_coronet_actor_reply_wait(reply, 0u, &status), TURBO_OK);
    check_int_eq(status, TURBO_ETIMEDOUT);
    check_uint_eq(state.calls, 1u);
    tf_coronet_actor_reply_destroy(reply);

    check_int_eq(tf_coronet_actor_destroy(&actor), TURBO_EBUSY);
    tf_coronet_actor_close(&actor);
    check_int_eq(tf_coronet_actor_submit(&actor, &command, sizeof(command), UINT64_MAX, &rejected),
                 TURBO_ESHUTDOWN);
    check_int_eq(tf_coronet_actor_drain(&actor, 0u), TURBO_OK);
    check_int_eq(tf_coronet_actor_destroy(&actor), TURBO_OK);
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

    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    state.expected_context = execution.context;
    state.delay_ms = 25u;
    check_int_eq(tf_coronet_actor_init(&actor, &execution, flow_actor_test_handler, &state, 4u,
                                       sizeof(command)),
                 TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution), TURBO_OK);
    check_int_eq(tf_coronet_actor_submit(&actor, &command, sizeof(command), UINT64_MAX, &reply),
                 TURBO_OK);
    check_int_eq(tf_coronet_actor_reply_wait(reply, UINT64_C(1000000), &status), TURBO_ETIMEDOUT);
    check_int_eq(tf_coronet_actor_reply_wait(reply, UINT64_C(500000000), &status), TURBO_OK);
    check_int_eq(status, 31);
    check_uint_eq(state.calls, 1u);
    tf_coronet_actor_reply_destroy(reply);

    tf_coronet_actor_close(&actor);
    check_int_eq(tf_coronet_actor_drain(&actor, UINT64_C(500000000)), TURBO_OK);
    check_int_eq(tf_coronet_actor_destroy(&actor), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
  }

  it("preserves synchronous status after an owner-lane command starts") {
    turbo_flow_coronet_execution_binding_t binding = flow_actor_private_binding();
    tf_coronet_execution_t execution;
    tf_coronet_actor_t actor;
    flow_actor_test_state_t state = {0};
    flow_actor_test_command_t command = {41u};

    check_int_eq(tf_coronet_execution_init(&execution, &binding), TURBO_OK);
    state.expected_context = execution.context;
    state.delay_ms = 10u;
    check_int_eq(tf_coronet_actor_init(&actor, &execution, flow_actor_test_handler, &state, 2u,
                                       sizeof(command)),
                 TURBO_OK);
    check_int_eq(tf_coronet_execution_start(&execution), TURBO_OK);
    check_int_eq(tf_coronet_actor_call(&actor, &command, sizeof(command), UINT64_C(500000000)), 41);
    check_uint_eq(state.calls, 1u);

    tf_coronet_actor_close(&actor);
    check_int_eq(tf_coronet_actor_drain(&actor, 0u), TURBO_OK);
    check_int_eq(tf_coronet_actor_destroy(&actor), TURBO_OK);
    tf_coronet_execution_stop(&execution);
    tf_coronet_execution_destroy(&execution);
  }
}
