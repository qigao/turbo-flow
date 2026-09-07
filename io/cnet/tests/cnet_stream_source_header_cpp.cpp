#include "turbo_flow_cnet.h"

extern "C" int cnet_stream_source_header_cpp_probe(void) {
  turbo_flow_cnet_stream_source_config_t config = TURBO_FLOW_CNET_STREAM_SOURCE_CONFIG_INIT;
  turbo_flow_cnet_stream_source_snapshot_t snapshot = TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_INIT;
  return config.version == TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION &&
                 snapshot.version == TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION
             ? 0
             : 1;
}

extern "C" int cnet_listener_source_header_cpp_probe(void) {
  turbo_flow_cnet_listener_source_config_t config = TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_INIT;
  turbo_flow_cnet_listener_source_snapshot_t snapshot =
      TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
  return config.version == TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION &&
                 snapshot.version == TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION
             ? 0
             : 1;
}
