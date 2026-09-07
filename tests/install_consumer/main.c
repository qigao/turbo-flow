#include <turbo_flow.h>
#include <turbo_flow_cnet.h>
#include <turbo_flow_chttp.h>

int main(void) {
  turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
  turbo_flow_run_result_t run_result = TURBO_FLOW_RUN_RESULT_INIT;
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
  int (*terminal_move)(turbo_flow_async_terminal_claim_t *, turbo_flow_async_terminal_claim_t *) =
      turbo_flow_async_terminal_claim_move;
  int (*terminal_complete)(turbo_flow_async_terminal_claim_t *, int,
                           const turbo_flow_settlement_result_t *) =
      turbo_flow_async_terminal_complete;
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
      !listener_open ||
      !listener_request || !listener_poll || !listener_snapshot_copy || !listener_stop ||
      !listener_destroy || !packet_open || !packet_request || !packet_poll ||
      !packet_snapshot_copy || !packet_session_open || !packet_session_info ||
      !packet_session_close || !packet_send || !packet_message_context || !packet_stop ||
      !packet_destroy || !stream_sink_register || !stream_sink_poll ||
      !stream_sink_snapshot_copy || !stream_sink_destroy || !datagram_sink_register ||
      !datagram_sink_poll || !datagram_sink_snapshot_copy || !datagram_sink_destroy ||
      !chttp_server_register || !chttp_server_snapshot_copy || !chttp_server_destroy ||
      !chttp_server_request_context ||
      !turbo_flow_message_type())
    return 1;
  turbo_flow_destroy(flow);
  return 0;
}
