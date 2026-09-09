#include "flow_internal.h"
#include "flow_projection_owner_internal.h"
#include "tinytest.h"
#include <salts/thread.h>
#include <stdlib.h>
#include <string.h>

/* Compile the actual implementations with allocation failure confined to this TU. */
static int fail_allocation;
static void *observed_wrapper;
static turbo_flow_projection_owner_t *observed_owner;
static size_t outstanding_at_wrapper_free;
static void *projection_fault_calloc(size_t count, size_t size) {
  if (fail_allocation) {
    fail_allocation = 0;
    return NULL;
  }
  return calloc(count, size);
}
static void projection_fault_free(void *ptr) {
  if (ptr == observed_wrapper && ptr) {
    turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    if (turbo_flow_projection_owner_snapshot(observed_owner, &state) == SALTS_OK)
      outstanding_at_wrapper_free = state.outstanding;
  }
  free(ptr);
}
#define calloc projection_fault_calloc
#define free projection_fault_free
#include "../src/flow_projection_owner.c"
#include "../src/flow_message.c"
#undef calloc
#undef free

enum { FAULT_VALUE = 37, FAULT_CAPACITY = 2 };
typedef struct fault_context_s { int clones; int destroys; int releases; } fault_context_t;
static const turbo_flow_data_schema_t fault_schema = {
  sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
  "fault", "Integer", "test.int", 1u, 1u, NULL};
static int fault_clone(const void *value, void *ctx, void **out) {
  fault_context_t *counts = (fault_context_t *)ctx;
  ++counts->clones;
  *out = malloc(sizeof(int));
  if (!*out) return SALTS_ENOMEM;
  *(int *)*out = *(const int *)value;
  return SALTS_OK;
}
static void fault_destroy(void *value, void *ctx) {
  ++((fault_context_t *)ctx)->destroys;
  free(value);
}
static int fault_release(void *ctx) {
  ++((fault_context_t *)ctx)->releases;
  return SALTS_OK;
}
spec("retained projection allocation rollback") {
  it("preserves bind ownership and skips clone callback on wrapper OOM") {
    fault_context_t counts = {0};
    turbo_flow_projection_owner_config_t config = TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT;
    turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_msg_t src, dst;
    int *value = (int *)malloc(sizeof(int));
    check_not_null(value);
    *value = FAULT_VALUE;
    config.flags = TURBO_FLOW_PROJECTION_IMMUTABLE | TURBO_FLOW_PROJECTION_CROSS_THREAD |
                   TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT;
    config.capacity = FAULT_CAPACITY;
    config.max_result_bytes = sizeof(int);
    config.max_retained_bytes = FAULT_CAPACITY * sizeof(int);
    config.schema = &fault_schema;
    config.clone = fault_clone;
    config.destroy = fault_destroy;
    config.ctx = &counts;
    config.release_context = fault_release;
    fail_allocation = 1;
    check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_ENOMEM);
    check_null(owner);
    check_equal(counts.releases, 0);
    check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
    turbo_flow_msg_init(&src);
    src.id = FAULT_VALUE;
    fail_allocation = 1;
    check_equal(turbo_flow_msg_bind_retained_projection(&src, owner, value), SALTS_ENOMEM);
    check_null(src._content_handle);
    check_equal(src.id, (uint64_t)FAULT_VALUE);
    check_equal(*value, FAULT_VALUE);
    check_equal(counts.destroys, 0);
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)0);
    check_equal(state.retained_bytes, (size_t)0);
    check_equal(turbo_flow_msg_bind_retained_projection(&src, owner, value), SALTS_OK);
    fail_allocation = 1;
    check_equal(turbo_flow_msg_clone(&dst, &src), SALTS_ENOMEM);
    check_null(dst._content_handle);
    check_equal(counts.clones, 0);
    check_equal(counts.destroys, 0);
    check_equal(*(const int *)turbo_flow_msg_projection(&src, NULL), FAULT_VALUE);
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)1);
    check_equal(state.retained_bytes, sizeof(int));
    check_equal(turbo_flow_msg_clone(&dst, &src), SALTS_OK);
    check_equal(counts.clones, 1);
    check_equal(*(const int *)turbo_flow_msg_projection(&dst, NULL), FAULT_VALUE);
    observed_owner = owner;
    observed_wrapper = src._content_handle;
    turbo_flow_msg_cleanup(&src);
    check_equal(outstanding_at_wrapper_free, (size_t)2);
    observed_wrapper = NULL;
    observed_owner = NULL;
    turbo_flow_msg_cleanup(&dst);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(counts.destroys, 2);
    check_equal(counts.releases, 1);
  }
}
