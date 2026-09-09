#include "flow_internal.h"
#include "tinytest.h"
#include <stdio.h>
#include <string.h>

/* Allocation failure is confined to the actual dispatch implementation in this TU. */
static int fail_reserve;
static int fail_resize;
static stl_status command_fault_reserve(vec_t *vec, size_t capacity) {
  if (fail_reserve) {
    fail_reserve = 0;
    return STL_OUT_OF_MEMORY;
  }
  return vec_reserve(vec, capacity);
}
static stl_status command_fault_resize(vec_t *vec, size_t count) {
  if (fail_resize) {
    fail_resize = 0;
    return STL_OUT_OF_MEMORY;
  }
  return vec_resize(vec, count);
}
#define vec_reserve command_fault_reserve
#define vec_resize command_fault_resize
#include "../src/flow_resource_command.c"
#undef vec_reserve
#undef vec_resize

/* The native implementation reads the same diagnostic through its public accessor. */
static int command_fault_error_code(const turbo_flow_t *flow) {
  const turbo_flow_error_t *error = turbo_flow_last_error(flow);
  return error ? error->code : SALTS_EINVAL;
}
#define flow_error_code command_fault_error_code
#include "../src/flow_native_resource.c"
#undef flow_error_code

typedef struct command_owner_s {
  uint64_t generation;
  int calls;
  int metadata_calls;
  int nested_status;
  int nested_scope_status;
  int reenter;
  int status;
} command_owner_t;

static int command_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  command_owner_t *owner = ctx;
  owner->metadata_calls++;
  *out = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
  out->kind = TURBO_FLOW_RESOURCE_CONNECTION;
  out->domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  memcpy(out->uid, "connection:fault", sizeof("connection:fault"));
  memcpy(out->owner_name, "fault", sizeof("fault"));
  out->generation = out->observed_generation = owner->generation;
  return SALTS_OK;
}

static int command_apply(void *ctx, turbo_flow_t *flow,
                         const turbo_flow_resource_command_t *command) {
  command_owner_t *owner = ctx;
  owner->calls++;
  if (owner->status != SALTS_OK) return owner->status;
  if (owner->reenter) {
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    flow_resource_command_scope_t nested_scope = {0};
    owner->reenter = 0;
    owner->nested_status = turbo_flow_resource_command(flow, command, &result);
    owner->nested_scope_status = flow_resource_command_scope_begin(flow, 1u, &nested_scope);
  }
  owner->generation++;
  return SALTS_OK;
}

static turbo_flow_t *command_flow(command_owner_t *owner) {
  turbo_flow_resource_provider_ops_t ops = TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  ops.metadata = command_metadata;
  ops.command = command_apply;
  if (turbo_flow_register_resource_provider(flow, "fault", &ops, owner) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  owner->metadata_calls = 0;
  return flow;
}

spec("resource command storage claims") {
  it("distinguishes replayed provider protocol errors from conflicting payloads") {
    command_owner_t owner = {1u, 0};
    turbo_flow_t *flow = command_flow(&owner);
    flow_resource_command_scope_t scope = {0};
    turbo_flow_resource_command_t command;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    check_not_null(flow);
    check_equal(flow_resource_command_scope_begin(flow, 1u, &scope), SALTS_OK);
    check_equal(flow_resource_command_init(&command, TURBO_FLOW_RESOURCE_COMMAND_QUIESCE,
                                           "connection:fault", 1u),
                SALTS_OK);
    owner.status = SALTS_EPROTO;
    check_equal(flow_resource_command_scope_execute(&scope, &command, &result), SALTS_EPROTO);
    check_false(result.replayed);
    check_equal(flow_resource_command_scope_execute(&scope, &command, &result), SALTS_EPROTO);
    check_true(result.replayed);
    check_equal(scope.remaining, 0u);
    check_equal(owner.calls, 1);
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
    check_equal(flow_resource_command_scope_execute(&scope, &command, &result), SALTS_EPROTO);
    check_false(result.replayed);
    check_equal(owner.calls, 1);
    flow_resource_command_scope_end(&scope);
    turbo_flow_destroy(flow);
  }

  it("replays a recorded missing target error without consuming another scope record") {
    command_owner_t owner = {1u, 0};
    turbo_flow_t *flow = command_flow(&owner);
    flow_resource_command_scope_t scope = {0};
    turbo_flow_resource_command_t command;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    check_not_null(flow);
    check_equal(flow_resource_command_scope_begin(flow, 1u, &scope), SALTS_OK);
    check_equal(flow_resource_command_init(&command, TURBO_FLOW_RESOURCE_COMMAND_QUIESCE,
                                           "connection:missing", 1u),
                SALTS_OK);
    check_equal(flow_resource_command_scope_execute(&scope, &command, &result), SALTS_ENOENT);
    check_equal(scope.remaining, 0u);
    check_equal(flow_resource_command_scope_execute(&scope, &command, &result), SALTS_ENOENT);
    check_true(result.replayed);
    check_equal(owner.calls, 0);
    flow_resource_command_scope_end(&scope);
    turbo_flow_destroy(flow);
  }

  it("rejects an ordinary claim allocation failure before metadata or owner effects and retries") {
    command_owner_t owner = {1u, 0};
    turbo_flow_t *flow = command_flow(&owner);
    turbo_flow_resource_command_t command;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    check_not_null(flow);
    check_equal(flow_resource_command_init(&command, TURBO_FLOW_RESOURCE_COMMAND_QUIESCE,
                                           "connection:fault", 1u),
                SALTS_OK);
    fail_resize = 1;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ENOMEM);
    check_equal(owner.calls, 0);
    check_equal(owner.metadata_calls, 0);
    check_equal(owner.generation, 1u);
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
    check_equal(owner.calls, 1);
    check_equal(owner.generation, 2u);
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
    check_true(result.replayed);
    check_equal(owner.calls, 1);
    turbo_flow_destroy(flow);
  }

  it("releases a failed scope reserve and protects pending records from callback reentry") {
    command_owner_t owner = {1u, 0};
    turbo_flow_t *flow = command_flow(&owner);
    flow_resource_command_scope_t scope = {0}, other = {0};
    turbo_flow_resource_command_t command;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    check_not_null(flow);
    check_equal(flow_resource_command_scope_begin(flow, 0u, &scope), SALTS_EINVAL);
    fail_reserve = 1;
    check_equal(flow_resource_command_scope_begin(flow, 2u, &scope), SALTS_ENOMEM);
    check_null(scope.flow);
    check_equal(owner.calls, 0);
    check_equal(owner.metadata_calls, 0);
    check_equal(owner.generation, 1u);
    check_equal(flow_resource_command_scope_begin(flow, 2u, &scope), SALTS_OK);
    check_equal(flow_resource_command_scope_begin(flow, 1u, &other), SALTS_EBUSY);
    check_equal(flow_resource_command_init(&command, TURBO_FLOW_RESOURCE_COMMAND_QUIESCE,
                                           "connection:fault", 1u),
                SALTS_OK);
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_EBUSY);
    owner.reenter = 1;
    check_equal(flow_resource_command_scope_execute(&scope, &command, &result), SALTS_OK);
    check_equal(owner.nested_status, SALTS_EBUSY);
    check_equal(owner.nested_scope_status, SALTS_EBUSY);
    check_equal(scope.remaining, 1u);
    check_equal(flow_resource_command_scope_execute(&scope, &command, &result), SALTS_OK);
    check_true(result.replayed);
    check_equal(scope.remaining, 1u);
    check_equal(flow_resource_command_init(&command, TURBO_FLOW_RESOURCE_COMMAND_RESUME,
                                           "connection:fault", 2u),
                SALTS_OK);
    check_equal(flow_resource_command_scope_execute(&scope, &command, &result), SALTS_OK);
    check_equal(scope.remaining, 0u);
    check_equal(flow_resource_command_scope_execute(&scope, &command, &result), SALTS_OK);
    check_true(result.replayed);
    check_equal(flow_resource_command_init(&command, TURBO_FLOW_RESOURCE_COMMAND_QUIESCE,
                                           "connection:fault", 3u),
                SALTS_OK);
    check_equal(flow_resource_command_scope_execute(&scope, &command, &result), SALTS_ENOSPC);
    flow_resource_command_scope_end(&scope);
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
    check_equal(owner.calls, 3);
    turbo_flow_destroy(flow);
  }
}
