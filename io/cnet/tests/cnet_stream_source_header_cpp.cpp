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
  turbo_flow_cnet_listener_message_context_t context = {0};
  return config.version == TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION &&
                 snapshot.version == TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION &&
                 context.size == 0u
             ? 0
             : 1;
}

extern "C" int cnet_packet_source_header_cpp_probe(void) {
  turbo_flow_cnet_packet_source_config_t config = TURBO_FLOW_CNET_PACKET_SOURCE_CONFIG_INIT;
  turbo_flow_cnet_packet_source_snapshot_t snapshot = TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
  return config.version == TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION &&
                 snapshot.version == TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION
             ? 0
             : 1;
}

extern "C" int cnet_terminal_sink_header_cpp_probe(void) {
  turbo_flow_cnet_stream_sink_config_t stream = TURBO_FLOW_CNET_STREAM_SINK_CONFIG_INIT;
  turbo_flow_cnet_datagram_sink_config_t datagram = TURBO_FLOW_CNET_DATAGRAM_SINK_CONFIG_INIT;
  turbo_flow_cnet_packet_sink_config_t packet = TURBO_FLOW_CNET_PACKET_SINK_CONFIG_INIT;
  turbo_flow_cnet_packet_sink_snapshot_t packet_snapshot =
      TURBO_FLOW_CNET_PACKET_SINK_SNAPSHOT_INIT;
  return stream.version == TURBO_FLOW_CNET_STREAM_SINK_API_VERSION &&
                 datagram.version == TURBO_FLOW_CNET_DATAGRAM_SINK_API_VERSION &&
                 packet.version == TURBO_FLOW_CNET_PACKET_SINK_API_VERSION &&
                 packet_snapshot.version == TURBO_FLOW_CNET_PACKET_SINK_API_VERSION
             ? 0
             : 1;
}
