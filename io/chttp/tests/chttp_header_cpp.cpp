#include "turbo_flow_chttp.h"

extern "C" int chttp_header_cpp_probe(void) {
  turbo_flow_chttp_client_config_t config = TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
  turbo_flow_chttp_client_snapshot_t snapshot = TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
  return config.version == TURBO_FLOW_CHTTP_CLIENT_API_VERSION &&
                 snapshot.version == TURBO_FLOW_CHTTP_CLIENT_API_VERSION
             ? 0
             : 1;
}
