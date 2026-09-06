#include "turbo_flow_chttp.h"

extern "C" int chttp_server_header_cpp_probe(void) {
  turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
  turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
  turbo_flow_chttp_server_t *server = nullptr;
  const turbo_flow_chttp_server_request_context_t *request = nullptr;
  (void)config;
  (void)snapshot;
  (void)server;
  (void)request;
  return TURBO_FLOW_CHTTP_SERVER_API_VERSION == 1u;
}
