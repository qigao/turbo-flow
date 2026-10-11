#ifndef PROTOCOL_NETWORK_GRAPHS_H
#define PROTOCOL_NETWORK_GRAPHS_H
#if defined(_WIN32)
  #define FLOW_E2E_BACKEND "0"
#elif defined(__APPLE__)
  #define FLOW_E2E_BACKEND "2"
#else
  #define FLOW_E2E_BACKEND "1"
#endif
#define FLOW_E2E_DECODER_FIELDS(protocol, version, source, claims, bytes)                          \
  "  schema_version 1\n"                                                                           \
  "  max_sessions 1\n"                                                                             \
  "  max_frame_size 1024\n"                                                                        \
  "  max_pending_claims " claims "\n"                                                              \
  "  max_pending_bytes " bytes "\n"                                                                \
  "  protocol_provider \"" protocol "\"\n"                                                         \
  "  protocol_kind \"" protocol "\"\n"                                                             \
  "  protocol_version \"" version "\"\n"                                                           \
  "  source_id \"" source "\"\n"
#define FLOW_E2E_JTT_DECODER_FIELDS                                                                \
  FLOW_E2E_DECODER_FIELDS("jtt808", "2019-A1", "fleet.primary", "64", "65536")
#define FLOW_E2E_COAP_DECODER_FIELDS                                                               \
  FLOW_E2E_DECODER_FIELDS("coap", "RFC7252", "coap.primary", "64", "65536")
#define FLOW_E2E_LISTENER_GRAPH(resource_name, decoder_fields)                                     \
  "source wire adapter cnet.listener_source {\n"                                                   \
  "  resource " resource_name "\n"                                                                 \
  "  schema_version 2\n"                                                                           \
  "  backend " FLOW_E2E_BACKEND "\n"                                                               \
  "  tls_enabled 0\n"                                                                              \
  "  connection_capacity 1\n"                                                                      \
  "  command_capacity 8\n"                                                                         \
  "  request_capacity 8\n"                                                                         \
  "  completion_batch_capacity 4\n"                                                                \
  "  event_capacity 8\n"                                                                           \
  "  max_send_bytes 1024\n"                                                                        \
  "  receive_buffer_bytes 1024\n"                                                                  \
  "  connect_timeout_ms 0\n"                                                                       \
  "  read_timeout_ms 0\n"                                                                          \
  "  write_timeout_ms 0\n"                                                                         \
  "  tls_io_buffer_bytes 0\n"                                                                      \
  "  tls_handshake_timeout_ms 0\n"                                                                 \
  "  command_buffer_bytes 0\n"                                                                     \
  "  event_buffer_bytes 0\n"                                                                       \
  "  socket_receive_buffer_bytes 0\n"                                                              \
  "  socket_send_buffer_bytes 0\n"                                                                 \
  "  keepalive 0\n"                                                                                \
  "  keepalive_idle_ms 0\n"                                                                        \
  "  keepalive_interval_ms 0\n"                                                                    \
  "  keepalive_count 0\n"                                                                          \
  "  linger 0\n"                                                                                   \
  "  linger_ms 0\n"                                                                                \
  "  backlog 4\n"                                                                                  \
  "  reuse_port 0\n"                                                                               \
  "  tls_client_auth 0\n"                                                                          \
  "  max_connections 1\n"                                                                          \
  "  max_message_bytes 1024\n"                                                                     \
  "  scheduler_capacity 8\n"                                                                       \
  "  scheduler_max_steps_per_poll 32\n"                                                            \
  "  first_message_id 1\n"                                                                         \
  "  initial_demand 9\n"                                                                           \
  "  stop_timeout_ms 1000\n"                                                                       \
  "  content_encoding 5\n"                                                                         \
  "  content_schema_version 1\n"                                                                   \
  "  content_media_type \"application/octet-stream\"\n"                                            \
  "  content_schema \"Wire\"\n"                                                                    \
  "  content_type \"Frame\"\n"                                                                     \
  "}\n"                                                                                            \
  "stage decode adapter protocol.decode {\n" decoder_fields "}\n"                                  \
  "stage main {\n"                                                                                 \
  "  wire -> decode\n"                                                                             \
  "}\n"
static const char protocol_network_listener_graph[] =
    FLOW_E2E_LISTENER_GRAPH("wire_net", FLOW_E2E_JTT_DECODER_FIELDS);
#define FLOW_E2E_PACKET_GRAPH(resource_name, decoder_fields)                                       \
  "source wire adapter cnet.packet_source {\n"                                                     \
  "  resource " resource_name "\n"                                                                 \
  "  schema_version 2\n"                                                                           \
  "  backend " FLOW_E2E_BACKEND "\n"                                                               \
  "  packet_mode 0\n"                                                                              \
  "  datagram_send_capacity 8\n"                                                                   \
  "  request_capacity 16\n"                                                                        \
  "  completion_batch_capacity 4\n"                                                                \
  "  max_datagram_bytes 1024\n"                                                                    \
  "  receive_buffer_bytes 1024\n"                                                                  \
  "  reuse_port 0\n"                                                                               \
  "  session_capacity 1\n"                                                                         \
  "  kcp_mtu 0\n"                                                                                  \
  "  kcp_send_window 0\n"                                                                          \
  "  kcp_receive_window 0\n"                                                                       \
  "  kcp_interval_ms 0\n"                                                                          \
  "  kcp_fast_resend 0\n"                                                                          \
  "  kcp_no_congestion_window 0\n"                                                                 \
  "  kcp_stream_mode 0\n"                                                                          \
  "  kcp_send_segment_capacity 0\n"                                                                \
  "  kcp_max_message_bytes 0\n"                                                                    \
  "  security_mode 0\n"                                                                            \
  "  handshake_retry_ms 0\n"                                                                       \
  "  fec_backend 0\n"                                                                              \
  "  fec_data_shards 0\n"                                                                          \
  "  fec_parity_shards 0\n"                                                                        \
  "  fec_max_payload_bytes 0\n"                                                                    \
  "  fec_receive_group_count 0\n"                                                                  \
  "  queue_capacity 8\n"                                                                           \
  "  max_message_bytes 1024\n"                                                                     \
  "  scheduler_capacity 8\n"                                                                       \
  "  scheduler_max_steps_per_poll 32\n"                                                            \
  "  first_message_id 1\n"                                                                         \
  "  initial_demand 9\n"                                                                           \
  "  stop_timeout_ms 1000\n"                                                                       \
  "  content_encoding 5\n"                                                                         \
  "  content_schema_version 1\n"                                                                   \
  "  content_media_type \"application/octet-stream\"\n"                                            \
  "  content_schema \"Wire\"\n"                                                                    \
  "  content_type \"Frame\"\n"                                                                     \
  "}\n"                                                                                            \
  "stage decode adapter protocol.decode {\n" decoder_fields "}\n"                                  \
  "stage main {\n"                                                                                 \
  "  wire -> decode\n"                                                                             \
  "}\n"
static const char protocol_network_packet_graph[] =
    FLOW_E2E_PACKET_GRAPH("wire_net", FLOW_E2E_COAP_DECODER_FIELDS);
#define FLOW_E2E_MEMORY_STAGE                                                                      \
  "buffer intake provider flow.durable.memory {\n"                                                 \
  "  resource intake_store\n"                                                                      \
  "  schema_version 2\n"                                                                           \
  "  max_message_bytes 4096\n"                                                                     \
  "  max_records 8\n"                                                                              \
  "  max_total_bytes 32768\n"                                                                      \
  "  max_record_bytes 4096\n"                                                                      \
  "  max_claims 1\n"                                                                               \
  "  identity_mode 1\n"                                                                            \
  "}\n"
#define FLOW_E2E_OUTPUT_STAGE                                                                      \
  "stage output adapter cnet.datagram_sink {\n"                                                    \
  "  resource output_net\n"                                                                        \
  "  backend " FLOW_E2E_BACKEND "\n"                                                               \
  "  schema_version 2\n"                                                                           \
  "  datagram_send_capacity 4\n"                                                                   \
  "  request_capacity 8\n"                                                                         \
  "  completion_batch_capacity 4\n"                                                                \
  "  max_datagram_bytes 4096\n"                                                                    \
  "  receive_buffer_bytes 4096\n"                                                                  \
  "  reuse_port 0\n"                                                                               \
  "  max_message_bytes 4096\n"                                                                     \
  "  actor_command_capacity 8\n"                                                                   \
  "  actor_max_steps_per_poll 32\n"                                                                \
  "  stop_timeout_ms 1000\n"                                                                       \
  "}\n"
static const char protocol_network_memory_business_graph[] =
    "source decoded\n" FLOW_E2E_MEMORY_STAGE FLOW_E2E_OUTPUT_STAGE "stage main {\n"
    "  decoded -> intake -> output\n"
    "}\n";
static const char protocol_network_database_business_graph[] =
    "source decoded\n"
    "buffer intake provider flow.durable.turbodb {\n"
    "  resource intake_store\n"
    "  schema_version 2\n"
    "  max_message_bytes 4096\n"
    "  max_records 8\n"
    "  max_total_bytes 32768\n"
    "  max_record_bytes 4096\n"
    "  max_claims 1\n"
    "  identity_mode 1\n"
    "  connection_count 4\n"
    "  expected_generation 0\n"
    "  open_mode 0\n"
    "}\n" FLOW_E2E_OUTPUT_STAGE "stage main {\n"
    "  decoded -> intake -> output\n"
    "}\n";

#endif
