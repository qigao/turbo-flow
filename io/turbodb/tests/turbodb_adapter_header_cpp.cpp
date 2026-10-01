#include <turbo_flow_turbodb.h>
#include <turbo_flow_turbodb_resource.h>

extern "C" int turbodb_adapter_header_cpp_probe(void) {
  turbo_flow_turbodb_inbox_config_t inbox_config = turbo_flow_turbodb_inbox_config_default();
  turbo_flow_inbox_snapshot_t inbox_snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  turbo_flow_turbodb_outbox_source_config_t config =
      turbo_flow_turbodb_outbox_source_config_default();
  turbo_flow_turbodb_outbox_source_snapshot_t snapshot =
      TURBO_FLOW_TURBODB_OUTBOX_SOURCE_SNAPSHOT_INIT;
  turbo_flow_turbodb_database_view_t database_view =
      TURBO_FLOW_TURBODB_DATABASE_VIEW_INIT;
  return TURBO_FLOW_TURBODB_API_VERSION == 1u &&
                 inbox_config.version == TURBO_FLOW_TURBODB_INBOX_API_VERSION &&
                 inbox_snapshot.version == TURBO_FLOW_INBOX_API_VERSION &&
                 config.version == TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION &&
                 snapshot.version == TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION &&
                 database_view.size == sizeof(turbo_flow_turbodb_database_view_t) &&
                 TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONTRACT_VERSION == 1u
             ? 0
             : 1;
}
