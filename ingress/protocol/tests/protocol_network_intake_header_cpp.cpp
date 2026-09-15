#include "turbo_flow_protocol_network_intake.h"

extern "C" int protocol_network_intake_header_cpp_probe(void) {
  turbo_flow_protocol_network_intake_config_t config =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT;
  turbo_flow_protocol_network_intake_snapshot_t snapshot =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
  return config.size == sizeof(config) &&
                 config.version == TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION &&
                 snapshot.size == sizeof(snapshot) &&
                 snapshot.version == TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION &&
                 snapshot.state == TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED
             ? 0
             : 1;
}
