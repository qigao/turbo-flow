#include <turbo_flow.h>
#include <turbo_flow_cnet.h>

int main(void) {
  turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
  turbo_flow_run_result_t run_result = TURBO_FLOW_RUN_RESULT_INIT;
  turbo_flow_cnet_stream_source_config_t cnet_config = TURBO_FLOW_CNET_STREAM_SOURCE_CONFIG_INIT;
  turbo_flow_cnet_listener_source_config_t listener_config =
      TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_INIT;
  turbo_flow_cnet_listener_source_snapshot_t listener_snapshot =
      TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
  turbo_flow_cnet_packet_source_config_t packet_config = TURBO_FLOW_CNET_PACKET_SOURCE_CONFIG_INIT;
  turbo_flow_cnet_packet_source_snapshot_t packet_snapshot =
      TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
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
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || run_config.version != TURBO_FLOW_RUN_API_VERSION ||
      run_result.version != TURBO_FLOW_RUN_API_VERSION ||
      cnet_config.version != TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION ||
      listener_config.version != TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION ||
      listener_snapshot.version != TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION ||
      packet_config.version != TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION ||
      packet_snapshot.version != TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION || !listener_open ||
      !listener_request || !listener_poll || !listener_snapshot_copy || !listener_stop ||
      !listener_destroy || !packet_open || !packet_request || !packet_poll ||
      !packet_snapshot_copy || !packet_session_open || !packet_session_info ||
      !packet_session_close || !packet_send || !packet_message_context || !packet_stop ||
      !packet_destroy || !turbo_flow_message_type())
    return 1;
  turbo_flow_destroy(flow);
  return 0;
}
