#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <turbo_flow.h>
#include <turbo_flow_chttp.h>
#include <turbo_flow_cnet.h>
#include <turbo_flow_plugin.h>
#include <turbo_flow_plugin_generation.h>
#include <turbo_flow_plugin_operation.h>
#include <turbo_flow_plugin_protocol.h>
#if defined(TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER)
  #include <turbo_flow_turbodb.h>
#endif

#if !defined(CNET_STOP_DRAIN_CONTRACT_VERSION) || CNET_STOP_DRAIN_CONTRACT_VERSION < 1u
  #error "TurboFlow install consumers require CNet stop-drain contract v1"
#endif

static native_io_backend_kind install_consumer_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#else
  return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static int install_operation_binding_config(void) {
  static const char yaml[] =
      "version: 1\noperation_bindings:\n  - operation: installed.evaluate\n"
      "    plugin: installed.typed\n    version: 1\n    input_schema: installed.Input\n"
      "    input_schema_version: 1\n    output_schema: installed.Output\n"
      "    output_schema_version: 1\n    permissions: [installed.read]\n"
      "    execution: inline\n    threading: owner\n    cancellation: none\n"
      "    max_inflight: 1\n    max_input_bytes: 64\n    max_result_bytes: 32\n"
      "    max_retained_bytes: 64\n    max_steps: 10\n    deadline_ms: 0\nadapters: {}\n";
  turbo_flow_resolved_config_t *config = NULL;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_resolved_operation_binding_view_t view =
      TURBO_FLOW_RESOLVED_OPERATION_BINDING_VIEW_INIT;
  const char *permission = NULL;
  size_t count = 0u;
  int rc = turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error);
  if (rc == SALTS_OK) rc = turbo_flow_resolved_config_operation_binding_count(config, &count);
  if (rc == SALTS_OK && count == 1u)
    rc = turbo_flow_resolved_config_operation_binding_at(config, 0u, &view);
  if (rc == SALTS_OK)
    rc = turbo_flow_resolved_config_operation_binding_permission_at(config, 0u, 0u, &permission);
  if (rc == SALTS_OK &&
      (strcmp(view.operation, "installed.evaluate") != 0 || view.resource != NULL ||
       strcmp(view.input_schema, "installed.Input") != 0 || view.input_schema_version != 1u ||
       strcmp(view.output_schema, "installed.Output") != 0 || view.output_schema_version != 1u ||
       strcmp(permission, "installed.read") != 0))
    rc = SALTS_EPROTO;
  turbo_flow_resolved_config_destroy(config);
  return rc;
}

static void install_projection_destroy(void *value, void *ctx) {
  (void)ctx;
  free(value);
}
static int install_projection_release(void *ctx) {
  free(ctx);
  return SALTS_OK;
}
static int install_projection_owner(void) {
  static const turbo_flow_data_schema_t schema = {sizeof(turbo_flow_data_schema_t),
                                                  TURBO_FLOW_DOMAIN_DATA,
                                                  TURBO_FLOW_DATA_ENCODING_OPAQUE,
                                                  "installed.projection",
                                                  "Integer",
                                                  "installed.int",
                                                  1u,
                                                  1u,
                                                  NULL};
  turbo_flow_plugin_host_config_t host_config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_projection_owner_config_t config = TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT;
  turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
  turbo_flow_plugin_host_t *host = NULL;
  turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
  turbo_flow_projection_owner_t *owner = NULL;
  turbo_flow_msg_t message;
  int *value = NULL;
  int rc = SALTS_EIO;
  int cleanup_rc;
  turbo_flow_msg_init(&message);
  if (turbo_flow_plugin_projection_owner_create(NULL, &config, &owner) != SALTS_EINVAL || owner)
    return SALTS_EPROTO;
  rc = turbo_flow_plugin_host_create(&host_config, &host, &error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &error);
  if (rc != SALTS_OK) goto cleanup;
  config.flags = TURBO_FLOW_PROJECTION_IMMUTABLE | TURBO_FLOW_PROJECTION_CROSS_THREAD |
                 TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT;
  config.capacity = 1u;
  config.max_result_bytes = sizeof(int);
  config.max_retained_bytes = sizeof(int);
  config.schema = &schema;
  config.destroy = install_projection_destroy;
  config.release_context = install_projection_release;
  config.ctx = malloc(sizeof(int));
  if (!config.ctx) {
    rc = SALTS_ENOMEM;
    goto cleanup;
  }
  rc = turbo_flow_plugin_projection_owner_create(snapshot, &config, &owner);
  if (rc != SALTS_OK) goto cleanup;
  config.ctx = NULL;
  turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
  snapshot = NULL;
  value = (int *)malloc(sizeof(*value));
  if (!value) {
    rc = SALTS_ENOMEM;
    goto cleanup;
  }
  *value = 1;
  rc = turbo_flow_msg_bind_retained_projection(&message, owner, value);
  if (rc != SALTS_OK) goto cleanup;
  value = NULL;
  rc = turbo_flow_projection_owner_snapshot(owner, &state);
  if (rc == SALTS_OK &&
      (state.outstanding != 1u || *(const int *)turbo_flow_msg_projection(&message, NULL) != 1 ||
       turbo_flow_plugin_host_destroy(host, 0u, &error) != SALTS_EBUSY))
    rc = SALTS_EPROTO;
cleanup:
  free(value);
  free(config.ctx);
  turbo_flow_msg_cleanup(&message);
  if (owner) {
    cleanup_rc = turbo_flow_projection_owner_stop(owner);
    if (cleanup_rc == SALTS_OK) cleanup_rc = turbo_flow_projection_owner_destroy(owner);
    if (cleanup_rc != SALTS_OK) return cleanup_rc;
  }
  turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
  cleanup_rc = turbo_flow_plugin_host_destroy(host, 0u, &error);
  return rc == SALTS_OK ? cleanup_rc : rc;
}

int main(void) {
  if (install_operation_binding_config() != SALTS_OK) return 1;
  if (install_projection_owner() != SALTS_OK) return 1;
#if defined(TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER)
  turbo_flow_turbodb_source_config_t turbodb_config = turbo_flow_turbodb_source_config_default();
  turbo_flow_turbodb_outbox_source_config_t outbox_config =
      turbo_flow_turbodb_outbox_source_config_default();
  if (turbodb_config.version != TURBO_FLOW_TURBODB_API_VERSION ||
      outbox_config.version != TURBO_FLOW_TURBODB_OUTBOX_SOURCE_API_VERSION)
    return 1;
#endif
  turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
  turbo_flow_run_result_t run_result = TURBO_FLOW_RUN_RESULT_INIT;
  turbo_flow_managed_boundary_descriptor_t boundary_descriptor =
      TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  turbo_flow_managed_boundary_snapshot_t boundary_snapshot =
      TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  turbo_flow_managed_boundary_provider_ops_t boundary_ops =
      TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
  turbo_flow_managed_async_terminal_registration_t managed_terminal_registration =
      TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_INIT;
  turbo_flow_managed_source_registration_t managed_source_registration =
      TURBO_FLOW_MANAGED_SOURCE_REGISTRATION_INIT;
  turbo_flow_plugin_host_config_t plugin_host_config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_protocol_catalog_v1_t plugin_protocol_catalog =
      TURBO_FLOW_PLUGIN_PROTOCOL_CATALOG_V1_INIT;
  turbo_flow_plugin_product_owner_v1_t plugin_product_owner =
      TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  turbo_flow_plugin_transactional_adapter_provider_v1_t transactional_adapter =
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_ADAPTER_PROVIDER_V1_INIT;
  turbo_flow_plugin_transactional_resource_provider_v1_t transactional_resource =
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_RESOURCE_PROVIDER_V1_INIT;
  turbo_flow_plugin_transactional_product_catalog_v1_t transactional_catalog =
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
  turbo_flow_plugin_generation_config_t generation_config =
      TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  turbo_flow_plugin_host_t *plugin_host = NULL;
  turbo_flow_async_terminal_claim_t terminal_claim = TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  turbo_flow_async_terminal_adapter_ops_t terminal_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
  turbo_flow_async_emit_claim_t emit_claim = TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT;
  turbo_flow_async_emit_adapter_ops_t emit_ops = TURBO_FLOW_ASYNC_EMIT_ADAPTER_OPS_INIT;
  turbo_flow_chttp_client_config_t chttp_config = TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
  turbo_flow_chttp_client_snapshot_t chttp_snapshot = TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
  turbo_flow_chttp_server_config_t chttp_server_config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
  turbo_flow_chttp_server_snapshot_t chttp_server_snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
  turbo_flow_cnet_stream_source_config_t cnet_config = TURBO_FLOW_CNET_STREAM_SOURCE_CONFIG_INIT;
  turbo_flow_cnet_listener_source_config_t listener_config =
      TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_INIT;
  turbo_flow_cnet_listener_source_snapshot_t listener_snapshot =
      TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
  turbo_flow_cnet_packet_source_config_t packet_config = TURBO_FLOW_CNET_PACKET_SOURCE_CONFIG_INIT;
  turbo_flow_cnet_packet_source_snapshot_t packet_snapshot =
      TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
  turbo_flow_cnet_stream_sink_config_t stream_sink_config = TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
  turbo_flow_cnet_stream_sink_snapshot_t stream_sink_snapshot =
      TURBO_FLOW_CNET_STREAM_SINK_SNAPSHOT_INIT;
  turbo_flow_cnet_datagram_sink_config_t datagram_sink_config =
      TURBO_FLOW_CNET_DATAGRAM_SINK_CONFIG_INIT;
  turbo_flow_cnet_datagram_sink_snapshot_t datagram_sink_snapshot =
      TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
  cnet_datagram_config datagram_native = CNET_DATAGRAM_CONFIG_INIT;
  turbo_flow_cnet_datagram_sink_t *datagram_sink = NULL;
  turbo_flow_cnet_packet_sink_config_t packet_sink_config = TURBO_FLOW_CNET_PACKET_SINK_CONFIG_INIT;
  turbo_flow_cnet_packet_sink_snapshot_t packet_sink_snapshot =
      TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
  int (*terminal_move)(turbo_flow_async_terminal_claim_t *, turbo_flow_async_terminal_claim_t *) =
      turbo_flow_async_terminal_claim_move;
  int (*terminal_complete)(turbo_flow_async_terminal_claim_t *, int,
                           const turbo_flow_settlement_result_t *) =
      turbo_flow_async_terminal_complete;
  int (*managed_terminal_register)(turbo_flow_t *,
                                   const turbo_flow_managed_async_terminal_registration_t *) =
      turbo_flow_register_managed_async_terminal_adapter;
  int (*managed_source_register)(turbo_flow_t *, const turbo_flow_managed_source_registration_t *) =
      turbo_flow_register_managed_source_adapter;
  int (*managed_source_run_open)(turbo_flow_t *, const turbo_flow_stage_plan_t *, cflow_publisher *,
                                 const turbo_flow_run_config_t *, turbo_flow_run_t **) =
      turbo_flow_managed_source_run_open;
  int (*plugin_host_create)(const turbo_flow_plugin_host_config_t *, turbo_flow_plugin_host_t **,
                            turbo_flow_plugin_error_t *) = turbo_flow_plugin_host_create;
  int (*plugin_host_load)(turbo_flow_plugin_host_t *, const char *, turbo_flow_plugin_error_t *) =
      turbo_flow_plugin_host_load;
  int (*plugin_host_destroy)(turbo_flow_plugin_host_t *, uint64_t, turbo_flow_plugin_error_t *) =
      turbo_flow_plugin_host_destroy;
  int (*plugin_protocol_catalog_read)(const turbo_flow_plugin_catalog_snapshot_t *,
                                      turbo_flow_plugin_protocol_catalog_v1_t *) =
      turbo_flow_plugin_catalog_snapshot_protocol_catalog;
  int (*plugin_transactional_catalog_read)(const turbo_flow_plugin_catalog_snapshot_t *,
                                           turbo_flow_plugin_transactional_product_catalog_v1_t *) =
      turbo_flow_plugin_catalog_snapshot_transactional_product_catalog;
  int (*plugin_generation_create)(
      turbo_flow_plugin_catalog_snapshot_t *, const turbo_flow_resolved_config_t *, turbo_flow_t **,
      const turbo_flow_plugin_generation_config_t *, turbo_flow_plugin_generation_t **,
      turbo_flow_config_error_t *) = turbo_flow_plugin_generation_create;
  turbo_flow_t *(*plugin_generation_flow)(turbo_flow_plugin_generation_t *) =
      turbo_flow_plugin_generation_flow;
  turbo_flow_plugin_generation_state_t (*plugin_generation_state)(
      const turbo_flow_plugin_generation_t *) = turbo_flow_plugin_generation_state;
  size_t (*plugin_generation_owner_count)(const turbo_flow_plugin_generation_t *) =
      turbo_flow_plugin_generation_owner_count;
  int (*plugin_generation_poll)(turbo_flow_plugin_generation_t *, uint32_t,
                                turbo_flow_config_error_t *) = turbo_flow_plugin_generation_poll;
  int (*plugin_generation_lease_acquire)(turbo_flow_plugin_generation_t *) =
      turbo_flow_plugin_generation_lease_acquire;
  int (*plugin_generation_lease_release)(turbo_flow_plugin_generation_t *) =
      turbo_flow_plugin_generation_lease_release;
  int (*plugin_generation_destroy)(turbo_flow_plugin_generation_t *, uint64_t,
                                   turbo_flow_config_error_t *) =
      turbo_flow_plugin_generation_destroy;
  int (*plugin_protocol_registry_create)(turbo_flow_plugin_catalog_snapshot_t *,
                                         turbo_flow_protocol_registry_t **) =
      turbo_flow_protocol_registry_create;
  int (*plugin_business_registry_create)(turbo_flow_plugin_catalog_snapshot_t *,
                                         turbo_flow_protocol_business_registry_t **) =
      turbo_flow_protocol_business_registry_create;
  int (*listener_open)(const turbo_flow_cnet_listener_source_config_t *,
                       turbo_flow_cnet_listener_source_t **) = turbo_flow_cnet_listener_source_open;
  int (*listener_request)(turbo_flow_cnet_listener_source_t *, size_t) =
      turbo_flow_cnet_listener_source_request;
  int (*listener_poll)(turbo_flow_cnet_listener_source_t *, uint32_t,
                       turbo_flow_cnet_listener_source_snapshot_t *) =
      turbo_flow_cnet_listener_source_poll;
  int (*listener_snapshot_copy)(const turbo_flow_cnet_listener_source_t *,
                                turbo_flow_cnet_listener_source_snapshot_t *) =
      turbo_flow_cnet_listener_source_snapshot;
  int (*listener_stop)(turbo_flow_cnet_listener_source_t *, uint32_t) =
      turbo_flow_cnet_listener_source_stop;
  int (*listener_destroy)(turbo_flow_cnet_listener_source_t *) =
      turbo_flow_cnet_listener_source_destroy;
  int (*packet_open)(const turbo_flow_cnet_packet_source_config_t *,
                     turbo_flow_cnet_packet_source_t **) = turbo_flow_cnet_packet_source_open;
  int (*packet_request)(turbo_flow_cnet_packet_source_t *, size_t) =
      turbo_flow_cnet_packet_source_request;
  int (*packet_poll)(turbo_flow_cnet_packet_source_t *, uint32_t,
                     turbo_flow_cnet_packet_source_snapshot_t *) =
      turbo_flow_cnet_packet_source_poll;
  int (*packet_snapshot_copy)(const turbo_flow_cnet_packet_source_t *,
                              turbo_flow_cnet_packet_source_snapshot_t *) =
      turbo_flow_cnet_packet_source_snapshot;
  int (*packet_session_open)(turbo_flow_cnet_packet_source_t *, const cnet_datagram_peer *,
                             uint32_t, cnet_packet_session *) =
      turbo_flow_cnet_packet_source_session_open;
  int (*packet_session_info)(const turbo_flow_cnet_packet_source_t *, cnet_packet_session,
                             cnet_packet_session_info *) =
      turbo_flow_cnet_packet_source_session_get_info;
  int (*packet_session_close)(turbo_flow_cnet_packet_source_t *, cnet_packet_session) =
      turbo_flow_cnet_packet_source_session_close;
  int (*packet_send)(turbo_flow_cnet_packet_source_t *, cnet_packet_session, const void *, size_t) =
      turbo_flow_cnet_packet_source_send;
  const turbo_flow_cnet_packet_message_context_t *(*packet_message_context)(
      const turbo_flow_msg_t *) = turbo_flow_cnet_packet_message_context;
  int (*packet_stop)(turbo_flow_cnet_packet_source_t *, uint32_t) =
      turbo_flow_cnet_packet_source_stop;
  int (*packet_destroy)(turbo_flow_cnet_packet_source_t *) = turbo_flow_cnet_packet_source_destroy;
  int (*stream_sink_register)(const turbo_flow_cnet_stream_sink_config_t *,
                              turbo_flow_cnet_stream_sink_t **) =
      turbo_flow_cnet_stream_sink_register;
  int (*stream_sink_poll)(turbo_flow_cnet_stream_sink_t *, uint32_t,
                          turbo_flow_cnet_stream_sink_snapshot_t *) =
      turbo_flow_cnet_stream_sink_poll;
  int (*stream_sink_snapshot_copy)(const turbo_flow_cnet_stream_sink_t *,
                                   turbo_flow_cnet_stream_sink_snapshot_t *) =
      turbo_flow_cnet_stream_sink_snapshot;
  int (*stream_sink_destroy)(turbo_flow_cnet_stream_sink_t *) = turbo_flow_cnet_stream_sink_destroy;
  int (*datagram_sink_register)(const turbo_flow_cnet_datagram_sink_config_t *,
                                turbo_flow_cnet_datagram_sink_t **) =
      turbo_flow_cnet_datagram_sink_register;
  int (*datagram_sink_poll)(turbo_flow_cnet_datagram_sink_t *, uint32_t,
                            turbo_flow_cnet_datagram_sink_snapshot_t *) =
      turbo_flow_cnet_datagram_sink_poll;
  int (*datagram_sink_snapshot_copy)(const turbo_flow_cnet_datagram_sink_t *,
                                     turbo_flow_cnet_datagram_sink_snapshot_t *) =
      turbo_flow_cnet_datagram_sink_snapshot;
  int (*datagram_sink_destroy)(turbo_flow_cnet_datagram_sink_t *) =
      turbo_flow_cnet_datagram_sink_destroy;
  int (*packet_sink_register)(const turbo_flow_cnet_packet_sink_config_t *,
                              turbo_flow_cnet_packet_sink_t **) =
      turbo_flow_cnet_packet_sink_register;
  int (*packet_sink_poll)(turbo_flow_cnet_packet_sink_t *, uint32_t,
                          turbo_flow_cnet_packet_sink_snapshot_t *) =
      turbo_flow_cnet_packet_sink_poll;
  int (*packet_sink_snapshot_copy)(const turbo_flow_cnet_packet_sink_t *,
                                   turbo_flow_cnet_packet_sink_snapshot_t *) =
      turbo_flow_cnet_packet_sink_snapshot;
  int (*packet_sink_destroy)(turbo_flow_cnet_packet_sink_t *) = turbo_flow_cnet_packet_sink_destroy;
  int (*chttp_server_register)(const turbo_flow_chttp_server_config_t *,
                               turbo_flow_chttp_server_t **) = turbo_flow_chttp_server_register;
  int (*chttp_server_snapshot_copy)(const turbo_flow_chttp_server_t *,
                                    turbo_flow_chttp_server_snapshot_t *) =
      turbo_flow_chttp_server_snapshot;
  int (*chttp_server_destroy)(turbo_flow_chttp_server_t *) = turbo_flow_chttp_server_destroy;
  int (*chttp_server_quiesce)(turbo_flow_chttp_server_t *) = turbo_flow_chttp_server_quiesce;
  int (*chttp_server_resume)(turbo_flow_chttp_server_t *) = turbo_flow_chttp_server_resume;
  int (*ws_quiesce)(turbo_flow_chttp_websocket_server_t *) =
      turbo_flow_chttp_websocket_server_quiesce;
  int (*ws_resume)(turbo_flow_chttp_websocket_server_t *) =
      turbo_flow_chttp_websocket_server_resume;
  const turbo_flow_chttp_server_request_context_t *(*chttp_server_request_context)(
      const turbo_flow_msg_t *) = turbo_flow_chttp_server_request_context;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || run_config.version != TURBO_FLOW_RUN_API_VERSION ||
      run_result.version != TURBO_FLOW_RUN_API_VERSION ||
      boundary_descriptor.version != TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION ||
      boundary_snapshot.version != TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION ||
      boundary_ops.version != TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION ||
      managed_terminal_registration.version !=
          TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_API_VERSION ||
      managed_source_registration.version != TURBO_FLOW_MANAGED_SOURCE_REGISTRATION_API_VERSION ||
      plugin_host_config.size != sizeof(turbo_flow_plugin_host_config_t) ||
      plugin_host_config.abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      plugin_error.size != sizeof(turbo_flow_plugin_error_t) ||
      plugin_error.abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      plugin_protocol_catalog.size != sizeof(turbo_flow_plugin_protocol_catalog_v1_t) ||
      plugin_protocol_catalog.abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      plugin_product_owner.size != sizeof(turbo_flow_plugin_product_owner_v1_t) ||
      TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_0_SIZE !=
          offsetof(turbo_flow_plugin_product_owner_v1_t, reserved_v1_1) ||
      plugin_product_owner.reserved_v1_1 != 0u ||
      transactional_adapter.size != sizeof(turbo_flow_plugin_transactional_adapter_provider_v1_t) ||
      transactional_resource.size !=
          sizeof(turbo_flow_plugin_transactional_resource_provider_v1_t) ||
      transactional_catalog.size != sizeof(turbo_flow_plugin_transactional_product_catalog_v1_t) ||
      generation_config.size != sizeof(turbo_flow_plugin_generation_config_t) ||
      TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR != 1u || TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR != 4u ||
      plugin_host_config.protocol_provider_capacity == 0u ||
      plugin_host_config.business_provider_capacity == 0u ||
      plugin_host_config.transactional_adapter_provider_capacity == 0u ||
      plugin_host_config.transactional_resource_provider_capacity == 0u ||
      plugin_host_config.schema_capacity == 0u || generation_config.owner_capacity == 0u ||
      !plugin_host_create || !plugin_host_load || !plugin_host_destroy ||
      !plugin_protocol_catalog_read || !plugin_transactional_catalog_read ||
      !plugin_generation_create || !plugin_generation_flow || !plugin_generation_state ||
      !plugin_generation_owner_count || !plugin_generation_poll ||
      plugin_generation_poll(NULL, 0u, &config_error) != SALTS_EINVAL ||
      !plugin_generation_lease_acquire || !plugin_generation_lease_release ||
      !plugin_generation_destroy || !plugin_protocol_registry_create ||
      !plugin_business_registry_create || !managed_terminal_register ||
      managed_terminal_register(flow, &managed_terminal_registration) != SALTS_EINVAL ||
      !managed_source_register ||
      managed_source_register(flow, &managed_source_registration) != SALTS_EINVAL ||
      !managed_source_run_open ||
      managed_source_run_open(flow, NULL, NULL, NULL, NULL) != SALTS_EINVAL ||
      turbo_flow_register_managed_boundary_provider(flow, "invalid", &boundary_ops, NULL) !=
          SALTS_EINVAL ||
      turbo_flow_managed_boundary_count(flow) != 0u ||
      turbo_flow_managed_boundary_descriptor_at(flow, 0u, &boundary_descriptor) != SALTS_ENOENT ||
      turbo_flow_managed_boundary_snapshot_at(flow, 0u, &boundary_snapshot) != SALTS_ENOENT ||
      terminal_claim.version != TURBO_FLOW_ASYNC_TERMINAL_API_VERSION ||
      terminal_ops.version != TURBO_FLOW_ASYNC_TERMINAL_API_VERSION || !terminal_move ||
      !terminal_complete || emit_claim.version != TURBO_FLOW_ASYNC_EMIT_API_VERSION ||
      emit_ops.version != TURBO_FLOW_ASYNC_EMIT_API_VERSION ||
      chttp_config.version != TURBO_FLOW_CHTTP_CLIENT_API_VERSION ||
      chttp_snapshot.version != TURBO_FLOW_CHTTP_CLIENT_API_VERSION ||
      chttp_server_config.version != TURBO_FLOW_CHTTP_SERVER_API_VERSION ||
      chttp_server_snapshot.version != TURBO_FLOW_CHTTP_SERVER_API_VERSION ||
      cnet_config.version != TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION ||
      listener_config.version != TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION ||
      listener_snapshot.version != TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION ||
      packet_config.version != TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION ||
      packet_snapshot.version != TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION ||
      stream_sink_config.version != TURBO_FLOW_CNET_STREAM_SINK_API_VERSION ||
      stream_sink_snapshot.version != TURBO_FLOW_CNET_STREAM_SINK_API_VERSION ||
      datagram_sink_config.version != TURBO_FLOW_CNET_DATAGRAM_SINK_API_VERSION ||
      datagram_sink_snapshot.version != TURBO_FLOW_CNET_DATAGRAM_SINK_API_VERSION ||
      packet_sink_config.version != TURBO_FLOW_CNET_PACKET_SINK_API_VERSION ||
      packet_sink_snapshot.version != TURBO_FLOW_CNET_PACKET_SINK_API_VERSION || !listener_open ||
      !listener_request || !listener_poll || !listener_snapshot_copy || !listener_stop ||
      !listener_destroy || !packet_open || !packet_request || !packet_poll ||
      !packet_snapshot_copy || !packet_session_open || !packet_session_info ||
      !packet_session_close || !packet_send || !packet_message_context || !packet_stop ||
      !packet_destroy || !stream_sink_register || !stream_sink_poll || !stream_sink_snapshot_copy ||
      !stream_sink_destroy || !datagram_sink_register || !datagram_sink_poll ||
      !datagram_sink_snapshot_copy || !datagram_sink_destroy || !packet_sink_register ||
      !packet_sink_poll || !packet_sink_snapshot_copy || !packet_sink_destroy ||
      !chttp_server_register || !chttp_server_snapshot_copy || !chttp_server_destroy ||
      !chttp_server_quiesce || !chttp_server_resume || !ws_quiesce || !ws_resume ||
      !chttp_server_request_context || !turbo_flow_message_type())
    return 1;

  plugin_host_config.module_capacity = 0u;
  if (plugin_host_create(&plugin_host_config, &plugin_host, &plugin_error) != SALTS_EINVAL ||
      plugin_host != NULL || plugin_error.stage != TURBO_FLOW_PLUGIN_STAGE_ARGUMENT)
    return 1;

  datagram_native.backend = install_consumer_backend();
  datagram_native.host = "127.0.0.1";
  datagram_native.port = 0u;
  datagram_native.send_capacity = 2u;
  datagram_native.request_capacity = 3u;
  datagram_native.completion_batch_capacity = 3u;
  datagram_native.max_datagram_bytes = 256u;
  datagram_native.receive_buffer_bytes = 256u;
  datagram_sink_config.flow = flow;
  datagram_sink_config.adapter_name = "installed.cnet.udp.out";
  datagram_sink_config.datagram = &datagram_native;
  datagram_sink_config.peer.family = CNET_DATAGRAM_ADDRESS_IPV4;
  datagram_sink_config.peer.port = 9u;
  datagram_sink_config.peer.address[0] = 127u;
  datagram_sink_config.peer.address[3] = 1u;
  datagram_sink_config.max_message_bytes = 256u;
  memset(&boundary_descriptor, 0, sizeof(boundary_descriptor));
  boundary_descriptor.size = sizeof(boundary_descriptor);
  boundary_descriptor.version = TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION;
  memset(&boundary_snapshot, 0, sizeof(boundary_snapshot));
  boundary_snapshot.size = sizeof(boundary_snapshot);
  boundary_snapshot.version = TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION;
  if (datagram_sink_register(&datagram_sink_config, &datagram_sink) != SALTS_OK ||
      turbo_flow_managed_boundary_count(flow) != 1u ||
      turbo_flow_managed_boundary_descriptor_at(flow, 0u, &boundary_descriptor) != SALTS_OK ||
      strcmp(boundary_descriptor.uid, "cnet-datagram-sink:installed.cnet.udp.out") != 0 ||
      strcmp(boundary_descriptor.input.schema_name, "CNetDatagram") != 0 ||
      turbo_flow_managed_boundary_snapshot_at(flow, 0u, &boundary_snapshot) != SALTS_OK ||
      boundary_snapshot.queue_capacity != 2u) {
    turbo_flow_destroy(flow);
    if (datagram_sink) (void)datagram_sink_destroy(datagram_sink);
    return 1;
  }
  turbo_flow_destroy(flow);
  return datagram_sink_destroy(datagram_sink) == SALTS_OK ? 0 : 1;
}
