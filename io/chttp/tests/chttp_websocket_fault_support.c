/* Compile the real adapter into a test-only translation unit so a delayed
 * completion's final close attempt can be scheduled without timing sleeps. */
#include "../src/turbo_flow_chttp_websocket_server.c"

int chttp_test_delayed_completion_close(turbo_flow_chttp_websocket_server_t *server) {
  return websocket_close_drained(server, 0u, false);
}
