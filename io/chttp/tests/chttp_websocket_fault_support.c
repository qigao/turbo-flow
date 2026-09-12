/* Compile the real adapter into a test-only translation unit so tests can
 * replace only native close, message allocation, and publish-return windows. */
#include <stdatomic.h>
#include <stdlib.h>

static atomic_size_t chttp_test_websocket_calloc_calls;
static atomic_size_t chttp_test_websocket_calloc_failure_call;

static void *chttp_test_websocket_calloc(size_t count, size_t size) {
  const size_t call =
      atomic_fetch_add_explicit(&chttp_test_websocket_calloc_calls, 1u, memory_order_relaxed) + 1u;
  if (call ==
      atomic_load_explicit(&chttp_test_websocket_calloc_failure_call, memory_order_relaxed))
    return NULL;
  return calloc(count, size);
}

#define calloc chttp_test_websocket_calloc
#include "../src/turbo_flow_chttp_websocket_server.c"
#undef calloc

void chttp_test_websocket_fail_calloc_call(size_t call) {
  atomic_store_explicit(&chttp_test_websocket_calloc_calls, 0u, memory_order_relaxed);
  atomic_store_explicit(&chttp_test_websocket_calloc_failure_call, call, memory_order_relaxed);
}

int chttp_test_delayed_completion_close(turbo_flow_chttp_websocket_server_t *server) {
  return websocket_close_drained(server, 0u, false);
}

int chttp_test_websocket_set_managed_generation(
    turbo_flow_chttp_websocket_server_t *server, uint64_t generation) {
  if (!server || generation == 0u) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  server->managed_generation = generation;
  salts_mutex_unlock(&server->mutex);
  return SALTS_OK;
}

int chttp_test_websocket_set_control_state(turbo_flow_chttp_websocket_server_t *server,
                                           turbo_flow_chttp_websocket_server_state_t state) {
  if (!server) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  server->state = state;
  salts_mutex_unlock(&server->mutex);
  return SALTS_OK;
}

int chttp_test_websocket_direct_command(turbo_flow_chttp_websocket_server_t *server,
                                        const turbo_flow_resource_command_t *command) {
  return server ? websocket_resource_command(server, server->flow, command) : SALTS_EINVAL;
}

int chttp_test_websocket_set_snapshot_state(turbo_flow_chttp_websocket_server_t *server,
                                            size_t active_sessions, size_t in_flight_frames,
                                            size_t session_frames, int frame_occupied,
                                            size_t pending_publications, uint64_t accepted,
                                            uint64_t completed) {
  if (!server || server->session_capacity == 0u || server->frame_capacity == 0u)
    return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  server->active_sessions = active_sessions;
  server->in_flight_frames = in_flight_frames;
  server->sessions[0].active = active_sessions != 0u;
  server->sessions[0].in_flight_frames = session_frames;
  server->frames[0].occupied = frame_occupied != 0;
  server->pending_publications = pending_publications;
  server->managed_accepted = accepted;
  server->managed_completed = completed;
  salts_mutex_unlock(&server->mutex);
  return SALTS_OK;
}

int chttp_test_websocket_saturate_managed_counters(
    turbo_flow_chttp_websocket_server_t *server) {
  if (!server) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  server->managed_accepted = UINT64_MAX;
  server->managed_completed = UINT64_MAX;
  server->managed_rejected = UINT64_MAX;
  websocket_counter_increment(&server->managed_accepted);
  websocket_counter_increment(&server->managed_completed);
  websocket_counter_increment(&server->managed_rejected);
  salts_mutex_unlock(&server->mutex);
  return SALTS_OK;
}
