#include "turbo_flow_chttp.h"

extern "C" int chttp_server_header_cpp_probe(void) {
  turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
  turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
  turbo_flow_chttp_server_t *server = nullptr;
  const turbo_flow_chttp_server_request_context_t *request = nullptr;
  turbo_flow_chttp_websocket_server_config_t websocket_config =
      TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
  turbo_flow_chttp_websocket_server_snapshot_t websocket_snapshot =
      TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
  turbo_flow_chttp_websocket_server_t *websocket_server = nullptr;
  const turbo_flow_chttp_websocket_event_context_t *websocket_event = nullptr;
  (void)config;
  (void)snapshot;
  (void)server;
  (void)request;
  (void)websocket_config;
  (void)websocket_snapshot;
  (void)websocket_server;
  (void)websocket_event;
  if (turbo_flow_chttp_server_quiesce(nullptr) != SALTS_EINVAL ||
      turbo_flow_chttp_server_resume(nullptr) != SALTS_EINVAL ||
      turbo_flow_chttp_websocket_server_quiesce(nullptr) != SALTS_EINVAL ||
      turbo_flow_chttp_websocket_server_resume(nullptr) != SALTS_EINVAL)
    return 0;
  return TURBO_FLOW_CHTTP_SERVER_API_VERSION == 1u &&
         TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_API_VERSION == 1u;
}
