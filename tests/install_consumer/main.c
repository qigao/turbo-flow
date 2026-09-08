#include <turbo_flow.h>
#include <turbo_flow_cnet.h>
#include <turbo_flow_chttp.h>
#include <string.h>
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

int main(void) {
#if defined(TURBO_FLOW_TEST_HAS_TURBODB_ADAPTER)
  turbo_flow_turbodb_source_config_t turbodb_config =
      turbo_flow_turbodb_source_config_default();
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
  turbo_flow_cnet_stream_sink_config_t stream_sink_config =
      TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
  turbo_flow_cnet_stream_sink_snapshot_t stream_sink_snapshot =
      TURBO_FLOW_CNET_STREAM_SINK_SNAPSHOT_INIT;
  turbo_flow_cnet_datagram_sink_config_t datagram_sink_config =
      TURBO_FLOW_CNET_DATAGRAM_SINK_CONFIG_INIT;
  turbo_flow_cnet_datagram_sink_snapshot_t datagram_sink_snapshot =
      TURBO_FLOW_CNET_DATAGRAM_SINK_SNAPSHOT_INIT;
  cnet_datagram_config datagram_native = CNET_DATAGRAM_CONFIG_INIT;
  turbo_flow_cnet_datagram_sink_t *datagram_sink = NULL;
  turbo_flow_cnet_packet_sink_config_t packet_sink_config =
      TURBO_FLOW_CNET_PACKET_SINK_CONFIG_INIT;
  turbo_flow_cnet_packet_sink_snapshot_t packet_sink_snapshot =
      TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
  int (*terminal_move)(turbo_flow_async_terminal_claim_t *, turbo_flow_async_terminal_claim_t *) =
      turbo_flow_async_terminal_claim_move;
  int (*terminal_complete)(turbo_flow_async_terminal_claim_t *, int,
                           const turbo_flow_settlement_result_t *) =
      turbo_flow_async_terminal_complete;
  int (*managed_terminal_register)(
      turbo_flow_t *, const turbo_flow_managed_async_terminal_registration_t *) =
      turbo_flow_register_managed_async_terminal_adapter;
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
  int (*stream_sink_destroy)(turbo_flow_cnet_stream_sink_t *) =
      turbo_flow_cnet_stream_sink_destroy;
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
  int (*packet_sink_destroy)(turbo_flow_cnet_packet_sink_t *) =
      turbo_flow_cnet_packet_sink_destroy;
  int (*chttp_server_register)(const turbo_flow_chttp_server_config_t *,
                               turbo_flow_chttp_server_t **) = turbo_flow_chttp_server_register;
  int (*chttp_server_snapshot_copy)(const turbo_flow_chttp_server_t *,
                                    turbo_flow_chttp_server_snapshot_t *) =
      turbo_flow_chttp_server_snapshot;
  int (*chttp_server_destroy)(turbo_flow_chttp_server_t *) = turbo_flow_chttp_server_destroy;
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
      !managed_terminal_register ||
      managed_terminal_register(flow, &managed_terminal_registration) != SALTS_EINVAL ||
      turbo_flow_register_managed_boundary_provider(flow, "invalid", &boundary_ops, NULL) !=
          SALTS_EINVAL ||
      turbo_flow_managed_boundary_count(flow) != 0u ||
      turbo_flow_managed_boundary_descriptor_at(flow, 0u, &boundary_descriptor) != SALTS_ENOENT ||
      turbo_flow_managed_boundary_snapshot_at(flow, 0u, &boundary_snapshot) != SALTS_ENOENT ||
      terminal_claim.version != TURBO_FLOW_ASYNC_TERMINAL_API_VERSION ||
      terminal_ops.version != TURBO_FLOW_ASYNC_TERMINAL_API_VERSION || !terminal_move ||
      !terminal_complete ||
      emit_claim.version != TURBO_FLOW_ASYNC_EMIT_API_VERSION ||
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
      packet_sink_snapshot.version != TURBO_FLOW_CNET_PACKET_SINK_API_VERSION ||
      !listener_open ||
      !listener_request || !listener_poll || !listener_snapshot_copy || !listener_stop ||
      !listener_destroy || !packet_open || !packet_request || !packet_poll ||
      !packet_snapshot_copy || !packet_session_open || !packet_session_info ||
      !packet_session_close || !packet_send || !packet_message_context || !packet_stop ||
      !packet_destroy || !stream_sink_register || !stream_sink_poll ||
      !stream_sink_snapshot_copy || !stream_sink_destroy || !datagram_sink_register ||
      !datagram_sink_poll || !datagram_sink_snapshot_copy || !datagram_sink_destroy ||
      !packet_sink_register || !packet_sink_poll || !packet_sink_snapshot_copy ||
      !packet_sink_destroy ||
      !chttp_server_register || !chttp_server_snapshot_copy || !chttp_server_destroy ||
      !chttp_server_request_context ||
      !turbo_flow_message_type())
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
