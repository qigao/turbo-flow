#include "tinytest.h"

#include "turbo_flow_plugin_generation.h"

#include <string.h>

#ifndef TURBO_FLOW_CNET_PLUGIN_PATH
  #error TURBO_FLOW_CNET_PLUGIN_PATH is required
#endif

#if defined(_WIN32)
  #define CNET_SNAPSHOT_TEST_BACKEND "iocp"
#elif defined(__APPLE__)
  #define CNET_SNAPSHOT_TEST_BACKEND "kqueue"
#else
  #define CNET_SNAPSHOT_TEST_BACKEND "epoll"
#endif

#define CNET_SNAPSHOT_CLIENT_YAML                                                                 \
  "      backend: " CNET_SNAPSHOT_TEST_BACKEND "\n"                                             \
  "      connection_capacity: 4\n"                                                           \
  "      command_capacity: 8\n"                                                              \
  "      request_capacity: 8\n"                                                              \
  "      completion_batch_capacity: 8\n"                                                     \
  "      event_capacity: 8\n"                                                                \
  "      max_send_bytes: 1024\n"                                                             \
  "      receive_buffer_bytes: 1024\n"                                                       \
  "      connect_timeout_ms: 0\n"                                                            \
  "      read_timeout_ms: 0\n"                                                               \
  "      write_timeout_ms: 0\n"                                                              \
  "      tls_io_buffer_bytes: 0\n"                                                           \
  "      tls_handshake_timeout_ms: 0\n"                                                      \
  "      command_buffer_bytes: 0\n"                                                          \
  "      event_buffer_bytes: 0\n"

#define CNET_SNAPSHOT_SOCKET_YAML                                                                 \
  "      socket_receive_buffer_bytes: 0\n"                                                   \
  "      socket_send_buffer_bytes: 0\n"                                                      \
  "      keepalive: false\n"                                                                 \
  "      keepalive_idle_ms: 0\n"                                                             \
  "      keepalive_interval_ms: 0\n"                                                         \
  "      keepalive_count: 0\n"                                                               \
  "      linger: false\n"                                                                    \
  "      linger_ms: 0\n"

#define CNET_SNAPSHOT_DATAGRAM_YAML                                                               \
  "      backend: " CNET_SNAPSHOT_TEST_BACKEND "\n"                                             \
  "      bind_host: \"127.0.0.1\"\n"                                                         \
  "      bind_port: 0\n"                                                                     \
  "      datagram_send_capacity: 4\n"                                                        \
  "      request_capacity: 8\n"                                                              \
  "      completion_batch_capacity: 8\n"                                                     \
  "      max_datagram_bytes: 1024\n"                                                         \
  "      receive_buffer_bytes: 1024\n"                                                       \
  "      reuse_port: false\n"

#define CNET_SNAPSHOT_SOURCE_TAIL_YAML                                                            \
  "      max_message_bytes: 1024\n"                                                          \
  "      scheduler_capacity: 8\n"                                                            \
  "      scheduler_max_steps_per_poll: 32\n"                                                 \
  "      first_message_id: 1\n"                                                              \
  "      initial_demand: 8\n"                                                                \
  "      stop_timeout_ms: 1000\n"

#define CNET_SNAPSHOT_SINK_TAIL_YAML                                                              \
  "      max_message_bytes: 1024\n"                                                          \
  "      actor_command_capacity: 8\n"                                                        \
  "      actor_max_steps_per_poll: 32\n"                                                     \
  "      stop_timeout_ms: 1000\n"

static const char cnet_snapshot_yaml[] =
    "version: 1\n"
    "adapters:\n"
    "  listener.source:\n"
    "    kind: cnet.listener_source\n"
    "    config:\n"
    "      schema_version: 1\n" CNET_SNAPSHOT_CLIENT_YAML
    "      bind_host: \"127.0.0.1\"\n"
    "      bind_port: 0\n"
    "      backlog: 4\n"
    "      reuse_port: false\n" CNET_SNAPSHOT_SOCKET_YAML
    "      tls_enabled: false\n"
    "      tls_ca_file: \"\"\n"
    "      tls_ca_path: \"\"\n"
    "      tls_cert_file: \"\"\n"
    "      tls_key_file: \"\"\n"
    "      tls_key_password: \"\"\n"
    "      tls_client_auth: none\n"
    "      tls_alpn: []\n"
    "      max_connections: 4\n" CNET_SNAPSHOT_SOURCE_TAIL_YAML
    "  packet.source:\n"
    "    kind: cnet.packet_source\n"
    "    config:\n"
    "      schema_version: 1\n" CNET_SNAPSHOT_DATAGRAM_YAML
    "      packet_mode: udp\n"
    "      session_capacity: 4\n"
    "      kcp_mtu: 0\n"
    "      kcp_send_window: 0\n"
    "      kcp_receive_window: 0\n"
    "      kcp_interval_ms: 0\n"
    "      kcp_fast_resend: 0\n"
    "      kcp_no_congestion_window: false\n"
    "      kcp_stream_mode: false\n"
    "      kcp_send_segment_capacity: 0\n"
    "      kcp_max_message_bytes: 0\n"
    "      security_mode: none\n"
    "      psk_hex: \"\"\n"
    "      handshake_retry_ms: 0\n"
    "      fec_backend: none\n"
    "      fec_data_shards: 0\n"
    "      fec_parity_shards: 0\n"
    "      fec_max_payload_bytes: 0\n"
    "      fec_receive_group_count: 0\n"
    "      queue_capacity: 8\n" CNET_SNAPSHOT_SOURCE_TAIL_YAML
    "  datagram.sink:\n"
    "    kind: cnet.datagram_sink\n"
    "    config:\n"
    "      schema_version: 1\n" CNET_SNAPSHOT_DATAGRAM_YAML
    "      peer_host: \"127.0.0.1\"\n"
    "      peer_port: 9\n"
    "      peer_scope_id: 0\n" CNET_SNAPSHOT_SINK_TAIL_YAML;

static const char cnet_snapshot_graph[] =
    "source listener_input adapter listener.source\n"
    "source packet_input adapter packet.source\n"
    "stage output adapter datagram.sink\n"
    "stage main {\n"
    "  listener_input -> output\n"
    "  packet_input -> output\n"
    "}\n";

static turbo_flow_plugin_host_t *cnet_snapshot_host(void) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_plugin_host_t *host = NULL;

  config.module_capacity = 1u;
  config.adapter_provider_capacity = 0u;
  config.resource_provider_capacity = 0u;
  config.protocol_provider_capacity = 0u;
  config.business_provider_capacity = 0u;
  config.transactional_adapter_provider_capacity = 6u;
  config.transactional_resource_provider_capacity = 0u;
  check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
  check_equal(turbo_flow_plugin_host_load(host, TURBO_FLOW_CNET_PLUGIN_PATH, &error), SALTS_OK);
  return host;
}

spec("configured CNet Source connection snapshots") {
  it("reports real listener and UDP packet bound endpoints through the generic adapter API") {
    turbo_flow_plugin_host_t *host = cnet_snapshot_host();
    turbo_flow_plugin_catalog_snapshot_t *catalog = NULL;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_plugin_generation_t *cleanup_generation = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    int listener_seen = 0;
    int packet_seen = 0;

    check_not_null(flow);
    check_equal(turbo_flow_config_resolve_yaml(cnet_snapshot_yaml, sizeof(cnet_snapshot_yaml) - 1u,
                                               &resolved, &error),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, cnet_snapshot_graph,
                                        sizeof(cnet_snapshot_graph) - 1u),
                SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &catalog, &plugin_error), SALTS_OK);
    generation_config.owner_capacity = 3u;
    check_equal(turbo_flow_plugin_generation_create(catalog, resolved, &flow, &generation_config,
                                                    NULL, &generation, &cleanup_generation, &error),
                SALTS_OK);
    check_null(flow);
    check_not_null(generation);
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);

    for (size_t i = 0u; i < turbo_flow_adapter_count(turbo_flow_plugin_generation_flow(generation));
         ++i) {
      turbo_flow_connection_snapshot_t connection;
      memset(&connection, 0, sizeof(connection));
      if (turbo_flow_adapter_connection_snapshot_at(turbo_flow_plugin_generation_flow(generation),
                                                    i, &connection) != SALTS_OK)
        continue;
      if (connection.adapter_name && strcmp(connection.adapter_name, "listener.source") == 0) {
        check_true(strncmp(connection.endpoint, "tcp://127.0.0.1:", 16u) == 0);
        check_true(strcmp(connection.endpoint, "tcp://127.0.0.1:0") != 0);
        check_equal(connection.connection_limit, (uint64_t)4u);
        listener_seen = 1;
      }
      if (connection.adapter_name && strcmp(connection.adapter_name, "packet.source") == 0) {
        check_true(strncmp(connection.endpoint, "udp://127.0.0.1:", 16u) == 0);
        check_true(strcmp(connection.endpoint, "udp://127.0.0.1:0") != 0);
        check_equal(connection.connection_limit, (uint64_t)4u);
        packet_seen = 1;
      }
    }

    check_true(listener_seen);
    check_true(packet_seen);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &error), SALTS_OK);
    if (cleanup_generation) {
      check_equal(turbo_flow_plugin_generation_destroy(cleanup_generation, 1000u, &error),
                  SALTS_OK);
    }
    turbo_flow_plugin_catalog_snapshot_destroy(catalog);
    turbo_flow_resolved_config_destroy(resolved);
    plugin_error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &plugin_error), SALTS_OK);
  }
}
