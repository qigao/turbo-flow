#include <turbo_flow_turbodb.h>

extern "C" int turbodb_adapter_header_cpp_probe(void) {
  turbo_flow_turbodb_outbox_source_config_t config =
      turbo_flow_turbodb_outbox_source_config_default();
  turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
      TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
  return TURBO_FLOW_TURBODB_API_VERSION == 1u &&
                 config.version == TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION &&
                 snapshot.version == TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION
             ? 0
             : 1;
}
