/* Compile the real server owner in isolation so tests can replace native
 * admission/completion calls without adding production fault switches. */
#include "../src/turbo_flow_chttp_server.c"

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
