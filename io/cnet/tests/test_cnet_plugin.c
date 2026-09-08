#include "tinytest.h"

#include "turbo_flow_plugin_generation.h"

#include <stdio.h>
#include <string.h>

#if defined(_WIN32)
  #include <winsock2.h>
typedef SOCKET cnet_plugin_test_socket_t;
typedef int cnet_plugin_test_socket_length_t;
  #define CNET_PLUGIN_TEST_INVALID_SOCKET INVALID_SOCKET
  #define cnet_plugin_test_close_socket closesocket
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
typedef int cnet_plugin_test_socket_t;
typedef socklen_t cnet_plugin_test_socket_length_t;
  #define CNET_PLUGIN_TEST_INVALID_SOCKET (-1)
  #define cnet_plugin_test_close_socket close
#endif

#ifndef TURBO_FLOW_CNET_PLUGIN_PATH
  #error TURBO_FLOW_CNET_PLUGIN_PATH is required
#endif

#if defined(_WIN32)
  #define CNET_TEST_BACKEND "iocp"
#elif defined(__APPLE__)
  #define CNET_TEST_BACKEND "kqueue"
#else
  #define CNET_TEST_BACKEND "epoll"
#endif

#define CNET_CLIENT_YAML                                                                           \
  "      backend: " CNET_TEST_BACKEND "\n"                                                         \
  "      connection_capacity: 4\n"                                                                 \
  "      command_capacity: 8\n"                                                                    \
  "      request_capacity: 8\n"                                                                    \
  "      completion_batch_capacity: 8\n"                                                           \
  "      event_capacity: 8\n"                                                                      \
  "      max_send_bytes: 1024\n"                                                                   \
  "      receive_buffer_bytes: 1024\n"                                                             \
  "      connect_timeout_ms: 0\n"                                                                  \
  "      read_timeout_ms: 0\n"                                                                     \
  "      write_timeout_ms: 0\n"                                                                    \
  "      tls_io_buffer_bytes: 0\n"                                                                 \
  "      tls_handshake_timeout_ms: 0\n"                                                            \
  "      command_buffer_bytes: 0\n"                                                                \
  "      event_buffer_bytes: 0\n"

#define CNET_SOCKET_YAML                                                                           \
  "      socket_receive_buffer_bytes: 0\n"                                                         \
  "      socket_send_buffer_bytes: 0\n"                                                            \
  "      keepalive: false\n"                                                                       \
  "      keepalive_idle_ms: 0\n"                                                                   \
  "      keepalive_interval_ms: 0\n"                                                               \
  "      keepalive_count: 0\n"                                                                     \
  "      linger: false\n"                                                                          \
  "      linger_ms: 0\n"

#define CNET_TLS_CLIENT_YAML                                                                       \
  "      tls_ca_file: \"\"\n"                                                                      \
  "      tls_ca_path: \"\"\n"                                                                      \
  "      tls_cert_file: \"\"\n"                                                                    \
  "      tls_key_file: \"\"\n"                                                                     \
  "      tls_key_password: \"\"\n"                                                                 \
  "      tls_server_name: \"\"\n"                                                                  \
  "      tls_alpn: []\n"

#define CNET_DATAGRAM_YAML                                                                         \
  "      backend: " CNET_TEST_BACKEND "\n"                                                         \
  "      bind_host: \"127.0.0.1\"\n"                                                               \
  "      bind_port: 0\n"                                                                           \
  "      datagram_send_capacity: 4\n"                                                              \
  "      request_capacity: 8\n"                                                                    \
  "      completion_batch_capacity: 8\n"                                                           \
  "      max_datagram_bytes: 1024\n"                                                               \
  "      receive_buffer_bytes: 1024\n"                                                             \
  "      reuse_port: false\n"

#define CNET_PACKET_YAML                                                                           \
  CNET_DATAGRAM_YAML                                                                               \
  "      packet_mode: udp\n"                                                                       \
  "      session_capacity: 4\n"                                                                    \
  "      kcp_mtu: 0\n"                                                                             \
  "      kcp_send_window: 0\n"                                                                     \
  "      kcp_receive_window: 0\n"                                                                  \
  "      kcp_interval_ms: 0\n"                                                                     \
  "      kcp_fast_resend: 0\n"                                                                     \
  "      kcp_no_congestion_window: false\n"                                                        \
  "      kcp_stream_mode: false\n"                                                                 \
  "      kcp_send_segment_capacity: 0\n"                                                           \
  "      kcp_max_message_bytes: 0\n"                                                               \
  "      security_mode: none\n"                                                                    \
  "      psk_hex: \"\"\n"                                                                          \
  "      handshake_retry_ms: 0\n"                                                                  \
  "      fec_backend: none\n"                                                                      \
  "      fec_data_shards: 0\n"                                                                     \
  "      fec_parity_shards: 0\n"                                                                   \
  "      fec_max_payload_bytes: 0\n"                                                               \
  "      fec_receive_group_count: 0\n"

#define CNET_SOURCE_TAIL_YAML                                                                      \
  "      max_message_bytes: 1024\n"                                                                \
  "      scheduler_capacity: 8\n"                                                                  \
  "      scheduler_max_steps_per_poll: 32\n"                                                       \
  "      first_message_id: 1\n"                                                                    \
  "      initial_demand: 8\n"                                                                      \
  "      stop_timeout_ms: 1000\n"

#define CNET_SINK_TAIL_YAML                                                                        \
  "      max_message_bytes: 1024\n"                                                                \
  "      actor_command_capacity: 8\n"                                                              \
  "      actor_max_steps_per_poll: 32\n"                                                           \
  "      stop_timeout_ms: 1000\n"

static const char cnet_six_yaml[] =
    "version: 1\n"
    "adapters:\n"
    "  stream.source:\n"
    "    kind: cnet.stream_source\n"
    "    config:\n"
    "      schema_version: 1\n"
    "      uri: \"tcp://127.0.0.1:9\"\n" CNET_CLIENT_YAML CNET_SOCKET_YAML
        CNET_TLS_CLIENT_YAML CNET_SOURCE_TAIL_YAML "  listener.source:\n"
    "    kind: cnet.listener_source\n"
    "    config:\n"
    "      schema_version: 1\n" CNET_CLIENT_YAML "      bind_host: \"127.0.0.1\"\n"
    "      bind_port: 0\n"
    "      backlog: 4\n"
    "      reuse_port: false\n" CNET_SOCKET_YAML "      tls_enabled: false\n"
    "      tls_ca_file: \"\"\n"
    "      tls_ca_path: \"\"\n"
    "      tls_cert_file: \"\"\n"
    "      tls_key_file: \"\"\n"
    "      tls_key_password: \"\"\n"
    "      tls_client_auth: none\n"
    "      tls_alpn: []\n"
    "      max_connections: 4\n" CNET_SOURCE_TAIL_YAML "  packet.source:\n"
    "    kind: cnet.packet_source\n"
    "    config:\n"
    "      schema_version: 1\n" CNET_PACKET_YAML "      queue_capacity: 8\n" CNET_SOURCE_TAIL_YAML
    "  stream.sink:\n"
    "    kind: cnet.stream_sink\n"
    "    config:\n"
    "      schema_version: 1\n"
    "      uri: \"tcp://127.0.0.1:9\"\n" CNET_CLIENT_YAML CNET_SOCKET_YAML
        CNET_TLS_CLIENT_YAML CNET_SINK_TAIL_YAML "  datagram.sink:\n"
    "    kind: cnet.datagram_sink\n"
    "    config:\n"
    "      schema_version: 1\n" CNET_DATAGRAM_YAML "      peer_host: \"127.0.0.1\"\n"
    "      peer_port: 9\n"
    "      peer_scope_id: 0\n" CNET_SINK_TAIL_YAML "  packet.sink:\n"
    "    kind: cnet.packet_sink\n"
    "    config:\n"
    "      schema_version: 1\n" CNET_PACKET_YAML "      peer_host: \"127.0.0.1\"\n"
    "      peer_port: 9\n"
    "      peer_scope_id: 0\n"
    "      conversation: 0\n"
    "      adapter_send_capacity: 4\n" CNET_SINK_TAIL_YAML;

static const char cnet_six_graph[] = "source stream_input adapter stream.source\n"
                                     "stage stream_output adapter stream.sink\n"
                                     "source listener_input adapter listener.source\n"
                                     "stage datagram_output adapter datagram.sink\n"
                                     "source packet_input adapter packet.source\n"
                                     "stage packet_output adapter packet.sink\n"
                                     "stage main {\n"
                                     "  stream_input -> stream_output\n"
                                     "  listener_input -> datagram_output\n"
                                     "  packet_input -> packet_output\n"
                                     "}\n";

static const char cnet_pipe_yaml[] =
    "version: 1\n"
    "adapters:\n"
    "  stream.source:\n"
    "    kind: cnet.stream_source\n"
    "    config:\n"
    "      schema_version: 1\n"
    "      uri: \"pipe://turbo-flow-cnet-test\"\n" CNET_CLIENT_YAML CNET_SOCKET_YAML
        CNET_TLS_CLIENT_YAML CNET_SOURCE_TAIL_YAML;

static const char cnet_tls_yaml[] =
    "version: 1\n"
    "adapters:\n"
    "  stream.source:\n"
    "    kind: cnet.stream_source\n"
    "    config:\n"
    "      schema_version: 1\n"
    "      uri: \"tls://127.0.0.1:443\"\n"
    "      backend: " CNET_TEST_BACKEND "\n"
    "      connection_capacity: 4\n"
    "      command_capacity: 8\n"
    "      request_capacity: 8\n"
    "      completion_batch_capacity: 8\n"
    "      event_capacity: 8\n"
    "      max_send_bytes: 1024\n"
    "      receive_buffer_bytes: 1024\n"
    "      connect_timeout_ms: 1000\n"
    "      read_timeout_ms: 1000\n"
    "      write_timeout_ms: 1000\n"
    "      tls_io_buffer_bytes: 17408\n"
    "      tls_handshake_timeout_ms: 1000\n"
    "      command_buffer_bytes: 2048\n"
    "      event_buffer_bytes: 1024\n" CNET_SOCKET_YAML "      tls_ca_file: \"ca.pem\"\n"
    "      tls_ca_path: \"\"\n"
    "      tls_cert_file: \"\"\n"
    "      tls_key_file: \"\"\n"
    "      tls_key_password: \"\"\n"
    "      tls_server_name: \"localhost\"\n"
    "      tls_alpn: [\"h2\"]\n" CNET_SOURCE_TAIL_YAML;

#define CNET_KCP_TRANSPORT_YAML                                                                    \
  CNET_DATAGRAM_YAML                                                                               \
  "      packet_mode: kcp\n"                                                                       \
  "      session_capacity: 4\n"                                                                    \
  "      kcp_mtu: 900\n"                                                                           \
  "      kcp_send_window: 32\n"                                                                    \
  "      kcp_receive_window: 32\n"                                                                 \
  "      kcp_interval_ms: 10\n"                                                                    \
  "      kcp_fast_resend: 2\n"                                                                     \
  "      kcp_no_congestion_window: false\n"                                                        \
  "      kcp_stream_mode: false\n"                                                                 \
  "      kcp_send_segment_capacity: 16\n"                                                          \
  "      kcp_max_message_bytes: 1024\n"

static const char cnet_kcp_plain_yaml[] =
    "version: 1\n"
    "adapters:\n"
    "  packet.source:\n"
    "    kind: cnet.packet_source\n"
    "    config:\n"
    "      schema_version: 1\n" CNET_KCP_TRANSPORT_YAML "      security_mode: none\n"
    "      psk_hex: \"\"\n"
    "      handshake_retry_ms: 0\n"
    "      fec_backend: none\n"
    "      fec_data_shards: 0\n"
    "      fec_parity_shards: 0\n"
    "      fec_max_payload_bytes: 0\n"
    "      fec_receive_group_count: 0\n"
    "      queue_capacity: 8\n" CNET_SOURCE_TAIL_YAML;

static const char cnet_kcp_secure_yaml[] =
    "version: 1\n"
    "adapters:\n"
    "  packet.source:\n"
    "    kind: cnet.packet_source\n"
    "    config:\n"
    "      schema_version: 1\n" CNET_KCP_TRANSPORT_YAML "      security_mode: psk_v1\n"
    "      psk_hex: \"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\"\n"
    "      handshake_retry_ms: 200\n"
    "      fec_backend: reed_solomon\n"
    "      fec_data_shards: 8\n"
    "      fec_parity_shards: 2\n"
    "      fec_max_payload_bytes: 948\n"
    "      fec_receive_group_count: 16\n"
    "      queue_capacity: 8\n" CNET_SOURCE_TAIL_YAML;

static turbo_flow_plugin_host_t *cnet_plugin_test_host_with_capacity(size_t provider_capacity,
                                                                     int load_status) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_plugin_host_t *host = NULL;
  config.module_capacity = 1u;
  config.adapter_provider_capacity = 0u;
  config.resource_provider_capacity = 0u;
  config.protocol_provider_capacity = 0u;
  config.business_provider_capacity = 0u;
  config.transactional_adapter_provider_capacity = provider_capacity;
  config.transactional_resource_provider_capacity = 0u;
  check_equal(turbo_flow_plugin_host_create(&config, &host, &error), SALTS_OK);
  check_equal(turbo_flow_plugin_host_load(host, TURBO_FLOW_CNET_PLUGIN_PATH, &error), load_status);
  return host;
}

static turbo_flow_plugin_host_t *cnet_plugin_test_host(void) {
  return cnet_plugin_test_host_with_capacity(6u, SALTS_OK);
}

static int
cnet_plugin_test_preflight(const turbo_flow_plugin_transactional_product_catalog_v1_t *catalog,
                           size_t provider_index, const char *yaml, const char *adapter_name,
                           turbo_flow_config_error_t *error) {
  turbo_flow_resolved_config_t *resolved = NULL;
  int status;
  if (!catalog || provider_index >= catalog->adapter_provider_count || !yaml || !adapter_name ||
      !error)
    return SALTS_EINVAL;
  status = turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, error);
  if (status == SALTS_OK)
    status = catalog->adapter_providers[provider_index].preflight(
        catalog->adapter_providers[provider_index].ctx, resolved, adapter_name, error);
  turbo_flow_resolved_config_destroy(resolved);
  return status;
}

static int cnet_plugin_test_replace_once(const char *input, const char *needle,
                                         const char *replacement, char *output,
                                         size_t output_capacity) {
  const char *match;
  size_t prefix_size;
  size_t suffix_size;
  size_t replacement_size;
  if (!input || !needle || !needle[0] || !replacement || !output || output_capacity == 0u)
    return SALTS_EINVAL;
  match = strstr(input, needle);
  if (!match) return SALTS_ENOENT;
  prefix_size = (size_t)(match - input);
  suffix_size = strlen(match + strlen(needle));
  replacement_size = strlen(replacement);
  if (prefix_size > SIZE_MAX - replacement_size ||
      prefix_size + replacement_size > SIZE_MAX - suffix_size ||
      prefix_size + replacement_size + suffix_size >= output_capacity)
    return SALTS_ENOSPC;
  memcpy(output, input, prefix_size);
  memcpy(output + prefix_size, replacement, replacement_size);
  memcpy(output + prefix_size + replacement_size, match + strlen(needle), suffix_size + 1u);
  return SALTS_OK;
}

static int cnet_plugin_test_refused_port(cnet_plugin_test_socket_t *socket_out,
                                         uint16_t *port_out) {
  struct sockaddr_in address;
  cnet_plugin_test_socket_length_t length = (cnet_plugin_test_socket_length_t)sizeof(address);
  cnet_plugin_test_socket_t value;
  if (!socket_out || !port_out) return SALTS_EINVAL;
  *socket_out = CNET_PLUGIN_TEST_INVALID_SOCKET;
  *port_out = 0u;
#if defined(_WIN32)
  {
    WSADATA data;
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return SALTS_EIO;
  }
#endif
  value = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (value == CNET_PLUGIN_TEST_INVALID_SOCKET) return SALTS_EIO;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (bind(value, (const struct sockaddr *)&address, sizeof(address)) != 0 ||
      getsockname(value, (struct sockaddr *)&address, &length) != 0) {
    cnet_plugin_test_close_socket(value);
    return SALTS_EIO;
  }
  *socket_out = value;
  *port_out = ntohs(address.sin_port);
  return SALTS_OK;
}

static void cnet_plugin_test_refused_port_close(cnet_plugin_test_socket_t value) {
  if (value != CNET_PLUGIN_TEST_INVALID_SOCKET) cnet_plugin_test_close_socket(value);
#if defined(_WIN32)
  (void)WSACleanup();
#endif
}

spec("cnet_plugin") {
  it("loads one canonical DLL and atomically registers all six transactional adapters") {
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_host_t *host = cnet_plugin_test_host();
    check_equal(turbo_flow_plugin_host_module_count(host), 1u);
    check_equal(turbo_flow_plugin_host_transactional_adapter_provider_count(host), 6u);
    check_equal(turbo_flow_plugin_host_transactional_resource_provider_count(host), 0u);
    check_equal(turbo_flow_plugin_host_adapter_provider_count(host), 0u);
    check_equal(turbo_flow_plugin_host_resource_provider_count(host), 0u);

    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);

    host = cnet_plugin_test_host_with_capacity(7u, SALTS_OK);
    check_equal(turbo_flow_plugin_host_module_count(host), 1u);
    check_equal(turbo_flow_plugin_host_transactional_adapter_provider_count(host), 6u);
    error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
  }

  it("rolls back all six providers at zero one and N-minus-one catalog capacity") {
    static const size_t capacities[] = {0u, 1u, 5u};
    for (size_t i = 0u; i < sizeof(capacities) / sizeof(capacities[0]); ++i) {
      turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_host_t *host =
          cnet_plugin_test_host_with_capacity(capacities[i], SALTS_ENOSPC);
      check_equal(turbo_flow_plugin_host_module_count(host), 0u);
      check_equal(turbo_flow_plugin_host_transactional_adapter_provider_count(host), 0u);
      check_equal(turbo_flow_plugin_host_transactional_resource_provider_count(host), 0u);
      check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &error), SALTS_OK);
    }
  }

  it("strictly preflights and transactionally materializes all six CNet kinds") {
    turbo_flow_plugin_host_t *host = cnet_plugin_test_host();
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_not_null(flow);
    check_equal(turbo_flow_config_resolve_yaml(cnet_six_yaml, sizeof(cnet_six_yaml) - 1u, &resolved,
                                               &error),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, cnet_six_graph, sizeof(cnet_six_graph) - 1u),
                SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &plugin_error),
                SALTS_OK);
    check_equal(
        turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
        SALTS_OK);
    check_equal(catalog.adapter_provider_count, 6u);
    for (size_t i = 0u; i < catalog.adapter_provider_count; ++i) {
      const turbo_flow_plugin_transactional_adapter_provider_v1_t *provider =
          &catalog.adapter_providers[i];
      check_not_null(provider->preflight);
      error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      check_equal(provider->preflight(provider->ctx, resolved,
                                      i == 0u   ? "stream.source"
                                      : i == 1u ? "listener.source"
                                      : i == 2u ? "packet.source"
                                      : i == 3u ? "stream.sink"
                                      : i == 4u ? "datagram.sink"
                                                : "packet.sink",
                                      &error),
                  SALTS_OK);
    }
    generation_config.owner_capacity = 6u;
    check_equal(turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &generation_config,
                                                    &generation, &error),
                SALTS_OK);
    check_null(flow);
    check_not_null(generation);
    check_equal(turbo_flow_plugin_generation_owner_count(generation), 6u);
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_count(turbo_flow_plugin_generation_flow(generation)),
                6u);
    size_t source_boundary_count = 0u;
    size_t sink_boundary_count = 0u;
    bool packet_sink_found = false;
    for (size_t i = 0u;
         i < turbo_flow_managed_boundary_count(turbo_flow_plugin_generation_flow(generation));
         ++i) {
      turbo_flow_managed_boundary_descriptor_t descriptor =
          TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
      turbo_flow_managed_boundary_snapshot_t boundary = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
      check_equal(turbo_flow_managed_boundary_descriptor_at(
                      turbo_flow_plugin_generation_flow(generation), i, &descriptor),
                  SALTS_OK);
      if ((descriptor.role_flags & TURBO_FLOW_MANAGED_BOUNDARY_SINK) != 0u) {
        ++sink_boundary_count;
        if (strcmp(descriptor.input.schema_name, "CNetPacket") == 0) {
          packet_sink_found = true;
          check_equal(descriptor.capability_flags,
                      (uint32_t)TURBO_FLOW_MANAGED_BOUNDARY_DURABLE_SETTLEMENT);
          check_equal(turbo_flow_managed_boundary_snapshot_at(
                          turbo_flow_plugin_generation_flow(generation), i, &boundary),
                      SALTS_OK);
          check_equal(boundary.state, TURBO_FLOW_MANAGED_BOUNDARY_RUNNING);
          check_true(boundary.queue_capacity > 0u);
          check_equal(boundary.accepted, (uint64_t)0u);
          check_equal(boundary.completed, (uint64_t)0u);
          check_equal(boundary.rejected, (uint64_t)0u);
        }
      }
      if ((descriptor.role_flags & TURBO_FLOW_MANAGED_BOUNDARY_SOURCE) == 0u) continue;
      check_equal(turbo_flow_managed_boundary_snapshot_at(
                      turbo_flow_plugin_generation_flow(generation), i, &boundary),
                  SALTS_OK);
      check_equal(boundary.state, TURBO_FLOW_MANAGED_BOUNDARY_RUNNING);
      check_equal(boundary.demand, 0u);
      check_equal(boundary.accepted, 0u);
      ++source_boundary_count;
    }
    check_equal(source_boundary_count, 3u);
    check_equal(sink_boundary_count, 3u);
    check_true(packet_sink_found);
    plugin_error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &plugin_error), SALTS_EBUSY);
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &error), SALTS_OK);
    generation = NULL;
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    snapshot = NULL;
    turbo_flow_resolved_config_destroy(resolved);
    plugin_error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &plugin_error), SALTS_OK);
  }

  it("strictly preflights Pipe TLS plain KCP and secure KCP endpoint policies") {
    turbo_flow_plugin_host_t *host = cnet_plugin_test_host();
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &plugin_error),
                SALTS_OK);
    check_equal(
        turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
        SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, cnet_pipe_yaml, "stream.source", &error),
                SALTS_OK);
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, cnet_tls_yaml, "stream.source", &error),
                SALTS_OK);
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(
        cnet_plugin_test_preflight(&catalog, 2u, cnet_kcp_plain_yaml, "packet.source", &error),
        SALTS_OK);
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(
        cnet_plugin_test_preflight(&catalog, 2u, cnet_kcp_secure_yaml, "packet.source", &error),
        SALTS_OK);

    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &plugin_error), SALTS_OK);
  }

  it("rejects secure KCP bounds during preflight instead of endpoint creation") {
    turbo_flow_plugin_host_t *host = cnet_plugin_test_host();
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    char invalid_yaml[8192];

    check_equal(cnet_plugin_test_replace_once(
                    cnet_kcp_secure_yaml, "      fec_max_payload_bytes: 948\n",
                    "      fec_max_payload_bytes: 900\n", invalid_yaml, sizeof(invalid_yaml)),
                SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &plugin_error),
                SALTS_OK);
    check_equal(
        turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
        SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 2u, invalid_yaml, "packet.source", &error),
                SALTS_EINVAL);
    check_contains(error.path, "packet.source");

    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &plugin_error), SALTS_OK);
  }

  it("rejects every mirrored CNet URI TLS and secure KCP invariant during preflight") {
    static const char zero_psk[] =
        "      psk_hex: \"0000000000000000000000000000000000000000000000000000000000000000\"\n";
    turbo_flow_plugin_host_t *host = cnet_plugin_test_host();
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    char first[32768];
    char second[32768];
    char third[32768];
    char replacement[512];
    size_t prefix_size;

    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &plugin_error),
                SALTS_OK);
    check_equal(
        turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
        SALTS_OK);

    check_equal(
        cnet_plugin_test_replace_once(
            cnet_kcp_secure_yaml,
            "      psk_hex: \"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\"\n",
            zero_psk, first, sizeof(first)),
        SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 2u, first, "packet.source", &error),
                SALTS_EINVAL);
    check_contains(error.path, "psk_hex");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(cnet_plugin_test_replace_once(cnet_six_yaml, "      tls_enabled: false\n",
                                              "      tls_enabled: true\n", first, sizeof(first)),
                SALTS_OK);
    check_equal(cnet_plugin_test_replace_once(first,
                                              "      tls_io_buffer_bytes: 0\n"
                                              "      tls_handshake_timeout_ms: 0\n"
                                              "      command_buffer_bytes: 0\n"
                                              "      event_buffer_bytes: 0\n"
                                              "      bind_host: \"127.0.0.1\"\n",
                                              "      tls_io_buffer_bytes: 17408\n"
                                              "      tls_handshake_timeout_ms: 1000\n"
                                              "      command_buffer_bytes: 2048\n"
                                              "      event_buffer_bytes: 1024\n"
                                              "      bind_host: \"127.0.0.1\"\n",
                                              second, sizeof(second)),
                SALTS_OK);
    check_equal(cnet_plugin_test_replace_once(second,
                                              "      tls_ca_file: \"\"\n"
                                              "      tls_ca_path: \"\"\n"
                                              "      tls_cert_file: \"\"\n"
                                              "      tls_key_file: \"\"\n"
                                              "      tls_key_password: \"\"\n"
                                              "      tls_client_auth: none\n",
                                              "      tls_ca_file: \"ca.pem\"\n"
                                              "      tls_ca_path: \"\"\n"
                                              "      tls_cert_file: \"server.pem\"\n"
                                              "      tls_key_file: \"server-key.pem\"\n"
                                              "      tls_key_password: \"\"\n"
                                              "      tls_client_auth: none\n",
                                              third, sizeof(third)),
                SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 1u, third, "listener.source", &error),
                SALTS_EINVAL);
    check_contains(error.path, "tls_client_auth");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    prefix_size = strlen("      uri: \"tcp://");
    memcpy(replacement, "      uri: \"tcp://", prefix_size);
    memset(replacement + prefix_size, 'a', 254u);
    memcpy(replacement + prefix_size + 254u, ":9\"\n", sizeof(":9\"\n"));
    check_equal(cnet_plugin_test_replace_once(cnet_six_yaml, "      uri: \"tcp://127.0.0.1:9\"\n",
                                              replacement, first, sizeof(first)),
                SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, first, "stream.source", &error),
                SALTS_ERANGE);
    check_contains(error.path, "uri");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    prefix_size = strlen("      tls_server_name: \"");
    memcpy(replacement, "      tls_server_name: \"", prefix_size);
    memset(replacement + prefix_size, 's', 254u);
    memcpy(replacement + prefix_size + 254u, "\"\n", sizeof("\"\n"));
    check_equal(cnet_plugin_test_replace_once(cnet_tls_yaml,
                                              "      tls_server_name: \"localhost\"\n", replacement,
                                              first, sizeof(first)),
                SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, first, "stream.source", &error),
                SALTS_ERANGE);
    check_contains(error.path, "tls_server_name");

    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &plugin_error), SALTS_OK);
  }

  it("reports a runtime CNet source failure through the managed boundary snapshot") {
    turbo_flow_plugin_host_t *host = cnet_plugin_test_host();
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_managed_boundary_snapshot_t boundary = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    cnet_plugin_test_socket_t reservation = CNET_PLUGIN_TEST_INVALID_SOCKET;
    uint16_t refused_port = 0u;
    char replacement[128];
    char yaml[32768];
    char timed_yaml[32768];
    int poll_status = SALTS_OK;
    int found = 0;

    check_not_null(flow);
    check_equal(cnet_plugin_test_refused_port(&reservation, &refused_port), SALTS_OK);
    check_true(snprintf(replacement, sizeof(replacement), "      uri: \"tcp://127.0.0.1:%u\"\n",
                        (unsigned)refused_port) > 0);
    check_equal(cnet_plugin_test_replace_once(cnet_six_yaml, "      uri: \"tcp://127.0.0.1:9\"\n",
                                              replacement, yaml, sizeof(yaml)),
                SALTS_OK);
    check_equal(cnet_plugin_test_replace_once(yaml, "      connect_timeout_ms: 0\n",
                                              "      connect_timeout_ms: 50\n", timed_yaml,
                                              sizeof(timed_yaml)),
                SALTS_OK);
    check_equal(turbo_flow_config_resolve_yaml(timed_yaml, strlen(timed_yaml), &resolved, &error),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, cnet_six_graph, sizeof(cnet_six_graph) - 1u),
                SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &plugin_error),
                SALTS_OK);
    generation_config.owner_capacity = 6u;
    check_equal(turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &generation_config,
                                                    &generation, &error),
                SALTS_OK);
    check_null(flow);
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);
    for (size_t attempt = 0u; attempt < 100u && poll_status == SALTS_OK; ++attempt) {
      error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      poll_status = turbo_flow_plugin_generation_poll(generation, 10u, &error);
    }
    check_not_equal(poll_status, SALTS_OK);
    for (size_t i = 0u;
         i < turbo_flow_managed_boundary_count(turbo_flow_plugin_generation_flow(generation));
         ++i) {
      turbo_flow_managed_boundary_descriptor_t descriptor =
          TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
      check_equal(turbo_flow_managed_boundary_descriptor_at(
                      turbo_flow_plugin_generation_flow(generation), i, &descriptor),
                  SALTS_OK);
      if (strcmp(descriptor.uid, "cnet:stream.source") != 0) continue;
      check_equal(turbo_flow_managed_boundary_snapshot_at(
                      turbo_flow_plugin_generation_flow(generation), i, &boundary),
                  SALTS_OK);
      found = 1;
      break;
    }
    check_true(found);
    check_equal(boundary.state, TURBO_FLOW_MANAGED_BOUNDARY_FAILED);
    check_equal(boundary.last_status, poll_status);

    check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &error), SALTS_OK);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    cnet_plugin_test_refused_port_close(reservation);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &plugin_error), SALTS_OK);
  }

  it("rejects an unknown field during preflight before materialization") {
    static const char invalid_yaml[] =
        "version: 1\n"
        "adapters:\n"
        "  datagram.sink:\n"
        "    kind: cnet.datagram_sink\n"
        "    config:\n"
        "      schema_version: 1\n" CNET_DATAGRAM_YAML "      peer_host: \"127.0.0.1\"\n"
        "      peer_port: 9\n"
        "      peer_scope_id: 0\n" CNET_SINK_TAIL_YAML "      unexpected: 1\n";
    turbo_flow_plugin_host_t *host = cnet_plugin_test_host();
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(
        turbo_flow_config_resolve_yaml(invalid_yaml, sizeof(invalid_yaml) - 1u, &resolved, &error),
        SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &plugin_error),
                SALTS_OK);
    check_equal(
        turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
        SALTS_OK);
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(catalog.adapter_providers[4].preflight(catalog.adapter_providers[4].ctx, resolved,
                                                       "datagram.sink", &error),
                SALTS_EINVAL);
    check_contains(error.path, "unexpected");
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    turbo_flow_resolved_config_destroy(resolved);
    plugin_error = (turbo_flow_plugin_error_t)TURBO_FLOW_PLUGIN_ERROR_INIT;
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &plugin_error), SALTS_OK);
  }

  it("rejects missing wrong-type unsupported-version and out-of-range fields in preflight") {
    turbo_flow_plugin_host_t *host = cnet_plugin_test_host();
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_transactional_product_catalog_v1_t catalog =
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    char invalid_yaml[32768];
    char overflow_yaml[32768];

    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &plugin_error),
                SALTS_OK);
    check_equal(
        turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(snapshot, &catalog),
        SALTS_OK);

    check_equal(cnet_plugin_test_replace_once(cnet_six_yaml, "      stop_timeout_ms: 1000\n", "",
                                              invalid_yaml, sizeof(invalid_yaml)),
                SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, invalid_yaml, "stream.source", &error),
                SALTS_EINVAL);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(cnet_plugin_test_replace_once(cnet_six_yaml, "      uri: \"tcp://127.0.0.1:9\"\n",
                                              "      uri: \"tcp://\"\n", invalid_yaml,
                                              sizeof(invalid_yaml)),
                SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, invalid_yaml, "stream.source", &error),
                SALTS_EINVAL);
    check_contains(error.path, "uri");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(cnet_plugin_test_replace_once(cnet_six_yaml, "      bind_host: \"127.0.0.1\"\n",
                                              "      bind_host: \"localhost\"\n", invalid_yaml,
                                              sizeof(invalid_yaml)),
                SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 1u, invalid_yaml, "listener.source", &error),
                SALTS_EINVAL);
    check_contains(error.path, "bind_host");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(
        cnet_plugin_test_replace_once(cnet_six_yaml, "      backend: " CNET_TEST_BACKEND "\n",
                                      "      backend: 7\n", invalid_yaml, sizeof(invalid_yaml)),
        SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, invalid_yaml, "stream.source", &error),
                SALTS_EINVAL);
    check_contains(error.path, "backend");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(cnet_plugin_test_replace_once(cnet_six_yaml, "      schema_version: 1\n",
                                              "      schema_version: 2\n", invalid_yaml,
                                              sizeof(invalid_yaml)),
                SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, invalid_yaml, "stream.source", &error),
                SALTS_ENOTSUP);
    check_contains(error.path, "schema_version");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(cnet_plugin_test_replace_once(cnet_six_yaml, "      command_capacity: 8\n",
                                              "      command_capacity: 7\n", invalid_yaml,
                                              sizeof(invalid_yaml)),
                SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, invalid_yaml, "stream.source", &error),
                SALTS_EINVAL);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(cnet_plugin_test_replace_once(cnet_six_yaml, "      command_capacity: 8\n",
                                              "      command_capacity: 4294967296\n", invalid_yaml,
                                              sizeof(invalid_yaml)),
                SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, invalid_yaml, "stream.source", &error),
                SALTS_ERANGE);
    check_contains(error.path, "command_capacity");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(cnet_plugin_test_replace_once(cnet_six_yaml, "      command_capacity: 8\n",
                                              "      command_capacity: 2147483648\n", overflow_yaml,
                                              sizeof(overflow_yaml)),
                SALTS_OK);
    check_equal(cnet_plugin_test_replace_once(overflow_yaml, "      max_send_bytes: 1024\n",
                                              "      max_send_bytes: 17179869184\n", invalid_yaml,
                                              sizeof(invalid_yaml)),
                SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, invalid_yaml, "stream.source", &error),
                SALTS_ERANGE);
    check_contains(error.path, "command_capacity");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(cnet_plugin_test_replace_once(cnet_six_yaml, "      event_capacity: 8\n",
                                              "      event_capacity: 4503599627370496\n",
                                              overflow_yaml, sizeof(overflow_yaml)),
                SALTS_OK);
    check_equal(cnet_plugin_test_replace_once(overflow_yaml, "      receive_buffer_bytes: 1024\n",
                                              "      receive_buffer_bytes: 4096\n", invalid_yaml,
                                              sizeof(invalid_yaml)),
                SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, invalid_yaml, "stream.source", &error),
                SALTS_ERANGE);
    check_contains(error.path, "event_capacity");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(cnet_plugin_test_replace_once(cnet_six_yaml, "      command_buffer_bytes: 0\n",
                                              "      command_buffer_bytes: 1024\n", invalid_yaml,
                                              sizeof(invalid_yaml)),
                SALTS_OK);
    check_equal(cnet_plugin_test_preflight(&catalog, 0u, invalid_yaml, "stream.source", &error),
                SALTS_ERANGE);
    check_contains(error.path, "command_buffer_bytes");

    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &plugin_error), SALTS_OK);
  }

  it("rejects a generation owner budget one below the six-adapter graph") {
    turbo_flow_plugin_host_t *host = cnet_plugin_test_host();
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_not_null(flow);
    check_equal(turbo_flow_config_resolve_yaml(cnet_six_yaml, sizeof(cnet_six_yaml) - 1u, &resolved,
                                               &error),
                SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, cnet_six_graph, sizeof(cnet_six_graph) - 1u),
                SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &plugin_error),
                SALTS_OK);
    generation_config.owner_capacity = 5u;
    check_equal(turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &generation_config,
                                                    &generation, &error),
                SALTS_ENOSPC);
    check_null(generation);
    check_not_null(flow);
    check_equal(turbo_flow_state(flow), TURBO_FLOW_STATE_PARSED);
    check_equal(turbo_flow_adapter_count(flow), 0u);

    turbo_flow_destroy(flow);
    turbo_flow_resolved_config_destroy(resolved);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    check_equal(turbo_flow_plugin_host_destroy(host, 1000u, &plugin_error), SALTS_OK);
  }
}
