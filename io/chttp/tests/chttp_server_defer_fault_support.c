/* Compile the real server owner in isolation so tests can replace native
 * admission/completion calls without adding production fault switches. */
#include <stdatomic.h>
#include <stdlib.h>

static atomic_size_t chttp_test_calloc_calls;
static atomic_size_t chttp_test_calloc_failure_call;

static void *chttp_test_server_calloc(size_t count, size_t size) {
  const size_t call =
      atomic_fetch_add_explicit(&chttp_test_calloc_calls, 1u, memory_order_relaxed) + 1u;
  if (call == atomic_load_explicit(&chttp_test_calloc_failure_call, memory_order_relaxed))
    return NULL;
  return calloc(count, size);
}

#define calloc chttp_test_server_calloc
#include "../src/turbo_flow_chttp_server.c"
#undef calloc

void chttp_test_server_fail_calloc_call(size_t call) {
  atomic_store_explicit(&chttp_test_calloc_calls, 0u, memory_order_relaxed);
  atomic_store_explicit(&chttp_test_calloc_failure_call, call, memory_order_relaxed);
}

int chttp_test_server_retry_first(turbo_flow_chttp_server_t *server) {
  if (!server || server->slot_count == 0u) return SALTS_EINVAL;
  chttp_server_adapter_execute_reply(&server->slots[0]);
  return SALTS_OK;
}

int chttp_test_server_set_managed_generation(turbo_flow_chttp_server_t *server,
                                             uint64_t generation) {
  if (!server || generation == 0u) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  server->managed_generation = generation;
  salts_mutex_unlock(&server->mutex);
  return SALTS_OK;
}

int chttp_test_server_direct_command(turbo_flow_chttp_server_t *server,
                                     const turbo_flow_resource_command_t *command) {
  return server ? chttp_server_adapter_resource_command(server, server->flow, command)
                : SALTS_EINVAL;
}

int chttp_test_server_set_snapshot_state(turbo_flow_chttp_server_t *server, int occupied,
                                         int managed_admitted, size_t active_requests,
                                         uint64_t accepted, uint64_t completed,
                                         uint64_t rejected) {
  if (!server || server->slot_count != 1u) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  server->slots[0].occupied = occupied != 0;
  server->slots[0].managed_admitted = managed_admitted != 0;
  server->active_requests = active_requests;
  server->managed_accepted = accepted;
  server->managed_completed = completed;
  server->managed_rejected = rejected;
  salts_mutex_unlock(&server->mutex);
  return SALTS_OK;
}

int chttp_test_server_saturate_managed_counters(turbo_flow_chttp_server_t *server) {
  if (!server) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  server->managed_accepted = UINT64_MAX;
  server->managed_completed = UINT64_MAX;
  server->managed_rejected = UINT64_MAX;
  chttp_server_adapter_counter_increment(&server->managed_accepted);
  chttp_server_adapter_counter_increment(&server->managed_completed);
  chttp_server_adapter_counter_increment(&server->managed_rejected);
  salts_mutex_unlock(&server->mutex);
  return SALTS_OK;
}
