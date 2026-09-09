#include "turbo_flow_plugin_generation.h"

#include <stdio.h>
#include <string.h>

#if defined(_WIN32)
  #include <winsock2.h>
  #include <ws2tcpip.h>
typedef SOCKET test_socket_t;
typedef int test_socket_length_t;
  #define TEST_INVALID_SOCKET INVALID_SOCKET
  #define TEST_BACKEND "iocp"
  #define test_close_socket closesocket
#else
  #include <arpa/inet.h>
  #include <errno.h>
  #include <fcntl.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
typedef int test_socket_t;
typedef socklen_t test_socket_length_t;
  #define TEST_INVALID_SOCKET (-1)
  #define test_close_socket close
  #if defined(__APPLE__)
    #define TEST_BACKEND "kqueue"
  #else
    #define TEST_BACKEND "epoll"
  #endif
#endif

enum { TEST_CONFIG_CAPACITY = 32768, TEST_POLL_LIMIT = 200 };

static int test_socket_start(void) {
#if defined(_WIN32)
  WSADATA data;
  return WSAStartup(MAKEWORD(2, 2), &data) == 0 ? 0 : 1;
#else
  return 0;
#endif
}

static void test_socket_finish(void) {
#if defined(_WIN32)
  (void)WSACleanup();
#endif
}

static int test_udp_bind(test_socket_t *socket_out, uint16_t *port_out) {
  struct sockaddr_in address;
  test_socket_length_t length = (test_socket_length_t)sizeof(address);
  test_socket_t value;
  if (!socket_out || !port_out) return 1;
  *socket_out = TEST_INVALID_SOCKET;
  *port_out = 0u;
  value = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
  if (value == TEST_INVALID_SOCKET) return 1;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (bind(value, (const struct sockaddr *)&address, sizeof(address)) != 0 ||
      getsockname(value, (struct sockaddr *)&address, &length) != 0) {
    test_close_socket(value);
    return 1;
  }
  *socket_out = value;
  *port_out = ntohs(address.sin_port);
  return 0;
}

static int test_socket_nonblocking(test_socket_t value) {
#if defined(_WIN32)
  u_long enabled = 1u;
  return ioctlsocket(value, FIONBIO, &enabled) == 0 ? 0 : 1;
#else
  int flags = fcntl(value, F_GETFL, 0);
  return flags >= 0 && fcntl(value, F_SETFL, flags | O_NONBLOCK) == 0 ? 0 : 1;
#endif
}

static int test_tcp_listen(test_socket_t *socket_out, uint16_t *port_out) {
  struct sockaddr_in address;
  test_socket_length_t length = (test_socket_length_t)sizeof(address);
  test_socket_t value;
  int reuse = 1;
  if (!socket_out || !port_out) return 1;
  *socket_out = TEST_INVALID_SOCKET;
  *port_out = 0u;
  value = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (value == TEST_INVALID_SOCKET) return 1;
  (void)setsockopt(value, SOL_SOCKET, SO_REUSEADDR, (const char *)&reuse, sizeof(reuse));
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (bind(value, (const struct sockaddr *)&address, sizeof(address)) != 0 ||
      getsockname(value, (struct sockaddr *)&address, &length) != 0 || listen(value, 4) != 0 ||
      test_socket_nonblocking(value) != 0) {
    test_close_socket(value);
    return 1;
  }
  *socket_out = value;
  *port_out = ntohs(address.sin_port);
  return 0;
}

static int test_socket_would_block(void) {
#if defined(_WIN32)
  return WSAGetLastError() == WSAEWOULDBLOCK;
#else
  return errno == EAGAIN || errno == EWOULDBLOCK;
#endif
}

static int test_config(char *output, size_t capacity, uint16_t tcp_input_port,
                       uint16_t udp_receiver_port, uint16_t tcp_receiver_port) {
  int written =
      snprintf(output, capacity,
               "version: 1\n"
               "adapters:\n"
               "  tcp.input:\n"
               "    kind: cnet.stream_source\n"
               "    config:\n"
               "      schema_version: 1\n"
               "      uri: \"tcp://127.0.0.1:%u\"\n"
               "      backend: " TEST_BACKEND "\n"
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
               "      tls_io_buffer_bytes: 0\n"
               "      tls_handshake_timeout_ms: 0\n"
               "      command_buffer_bytes: 0\n"
               "      event_buffer_bytes: 0\n"
               "      socket_receive_buffer_bytes: 0\n"
               "      socket_send_buffer_bytes: 0\n"
               "      keepalive: false\n"
               "      keepalive_idle_ms: 0\n"
               "      keepalive_interval_ms: 0\n"
               "      keepalive_count: 0\n"
               "      linger: false\n"
               "      linger_ms: 0\n"
               "      tls_ca_file: \"\"\n"
               "      tls_ca_path: \"\"\n"
               "      tls_cert_file: \"\"\n"
               "      tls_key_file: \"\"\n"
               "      tls_key_password: \"\"\n"
               "      tls_server_name: \"\"\n"
               "      tls_alpn: []\n"
               "      max_message_bytes: 1024\n"
               "      scheduler_capacity: 8\n"
               "      scheduler_max_steps_per_poll: 32\n"
               "      first_message_id: 1\n"
               "      initial_demand: 8\n"
               "      stop_timeout_ms: 1000\n"
               "  udp.output:\n"
               "    kind: cnet.datagram_sink\n"
               "    config:\n"
               "      schema_version: 1\n"
               "      backend: " TEST_BACKEND "\n"
               "      bind_host: \"127.0.0.1\"\n"
               "      bind_port: 0\n"
               "      datagram_send_capacity: 4\n"
               "      request_capacity: 8\n"
               "      completion_batch_capacity: 8\n"
               "      max_datagram_bytes: 1024\n"
               "      receive_buffer_bytes: 1024\n"
               "      reuse_port: false\n"
               "      peer_host: \"127.0.0.1\"\n"
               "      peer_port: %u\n"
               "      peer_scope_id: 0\n"
               "      max_message_bytes: 1024\n"
               "      actor_command_capacity: 8\n"
               "      actor_max_steps_per_poll: 32\n"
               "      stop_timeout_ms: 1000\n"
               "  tcp.output:\n"
               "    kind: cnet.stream_sink\n"
               "    config:\n"
               "      schema_version: 1\n"
               "      uri: \"tcp://127.0.0.1:%u\"\n"
               "      backend: " TEST_BACKEND "\n"
               "      connection_capacity: 4\n"
               "      command_capacity: 8\n"
               "      request_capacity: 8\n"
               "      completion_batch_capacity: 8\n"
               "      event_capacity: 8\n"
               "      max_send_bytes: 1024\n"
               "      receive_buffer_bytes: 1024\n"
               "      connect_timeout_ms: 0\n"
               "      read_timeout_ms: 0\n"
               "      write_timeout_ms: 0\n"
               "      tls_io_buffer_bytes: 0\n"
               "      tls_handshake_timeout_ms: 0\n"
               "      command_buffer_bytes: 0\n"
               "      event_buffer_bytes: 0\n"
               "      socket_receive_buffer_bytes: 0\n"
               "      socket_send_buffer_bytes: 0\n"
               "      keepalive: false\n"
               "      keepalive_idle_ms: 0\n"
               "      keepalive_interval_ms: 0\n"
               "      keepalive_count: 0\n"
               "      linger: false\n"
               "      linger_ms: 0\n"
               "      tls_ca_file: \"\"\n"
               "      tls_ca_path: \"\"\n"
               "      tls_cert_file: \"\"\n"
               "      tls_key_file: \"\"\n"
               "      tls_key_password: \"\"\n"
               "      tls_server_name: \"\"\n"
               "      tls_alpn: []\n"
               "      max_message_bytes: 1024\n"
               "      actor_command_capacity: 8\n"
               "      actor_max_steps_per_poll: 32\n"
               "      stop_timeout_ms: 1000\n",
               (unsigned)tcp_input_port, (unsigned)udp_receiver_port, (unsigned)tcp_receiver_port);
  return written > 0 && (size_t)written < capacity ? 0 : 1;
}

int main(int argc, char **argv) {
  static const char graph_text[] = "source input adapter tcp.input\n"
                                   "stage output adapter udp.output\n"
                                   "stage tcp_output adapter tcp.output\n"
                                   "stage main {\n"
                                   "  input -> output\n"
                                   "  input -> tcp_output\n"
                                   "}\n";
  static const char payload[] = "gateway-cnet-plugin-flow";
  turbo_flow_plugin_host_config_t host_config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_generation_config_t generation_config =
      TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_plugin_error_t destroy_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_config_error_t destroy_config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_host_t *host = NULL;
  turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
  turbo_flow_plugin_generation_t *generation = NULL;
  turbo_flow_plugin_generation_t *cleanup_generation = NULL;
  turbo_flow_resolved_config_t *resolved = NULL;
  turbo_flow_t *flow = NULL;
  test_socket_t receiver = TEST_INVALID_SOCKET;
  test_socket_t tcp_input_listener = TEST_INVALID_SOCKET;
  test_socket_t tcp_input = TEST_INVALID_SOCKET;
  test_socket_t tcp_receiver = TEST_INVALID_SOCKET;
  test_socket_t tcp_output = TEST_INVALID_SOCKET;
  uint16_t receiver_port = 0u;
  uint16_t tcp_input_port = 0u;
  uint16_t tcp_receiver_port = 0u;
  char yaml[TEST_CONFIG_CAPACITY];
  char received[64];
  char tcp_received_data[64];
  const char *failure_stage = "arguments";
  int failure_status = SALTS_OK;
  int udp_received = 0;
  int tcp_received = 0;
  size_t tcp_received_bytes = 0u;
  int rc = 1;

  if (argc != 2 || !argv[1][0]) {
    failure_status = SALTS_EINVAL;
    goto cleanup;
  }
  if (test_socket_start() != 0) {
    failure_stage = "socket startup";
    failure_status = SALTS_EIO;
    goto cleanup;
  }
  failure_stage = "socket setup";
  if (test_udp_bind(&receiver, &receiver_port) != 0 || test_socket_nonblocking(receiver) != 0 ||
      test_tcp_listen(&tcp_input_listener, &tcp_input_port) != 0 ||
      test_tcp_listen(&tcp_receiver, &tcp_receiver_port) != 0) {
    failure_status = SALTS_EIO;
    goto cleanup;
  }
  failure_stage = "config format";
  if (test_config(yaml, sizeof(yaml), tcp_input_port, receiver_port, tcp_receiver_port) != 0) {
    failure_status = SALTS_ERANGE;
    goto cleanup;
  }

  host_config.module_capacity = 1u;
  host_config.adapter_provider_capacity = 0u;
  host_config.resource_provider_capacity = 0u;
  host_config.protocol_provider_capacity = 0u;
  host_config.business_provider_capacity = 0u;
  host_config.transactional_adapter_provider_capacity = 6u;
  host_config.transactional_resource_provider_capacity = 0u;
  generation_config.owner_capacity = 3u;
  failure_stage = "plugin host create";
  failure_status = turbo_flow_plugin_host_create(&host_config, &host, &plugin_error);
  if (failure_status != SALTS_OK) goto cleanup;
  failure_stage = "plugin DLL load";
  failure_status = turbo_flow_plugin_host_load(host, argv[1], &plugin_error);
  if (failure_status != SALTS_OK) goto cleanup;
  failure_stage = "plugin catalog snapshot";
  failure_status = turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &plugin_error);
  if (failure_status != SALTS_OK) goto cleanup;
  failure_stage = "resolved config";
  failure_status = turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &config_error);
  if (failure_status != SALTS_OK) goto cleanup;
  flow = turbo_flow_create();
  failure_stage = "graph create";
  if (!flow) {
    failure_status = SALTS_ENOMEM;
    goto cleanup;
  }
  failure_stage = "graph parse";
  failure_status = turbo_flow_parse_string(flow, graph_text, sizeof(graph_text) - 1u);
  if (failure_status != SALTS_OK) goto cleanup;
  failure_stage = "graph generation";
  failure_status =
      turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &generation_config, NULL,
                                          &generation, &cleanup_generation, &config_error);
  if (failure_status != SALTS_OK) goto cleanup;
  failure_stage = "graph start";
  failure_status = turbo_flow_start(turbo_flow_plugin_generation_flow(generation));
  if (failure_status != SALTS_OK) goto cleanup;
  failure_stage = "TCP input accept";
  for (int i = 0; i < TEST_POLL_LIMIT && tcp_input == TEST_INVALID_SOCKET; ++i) {
    config_error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    failure_status = turbo_flow_plugin_generation_poll(generation, 10u, &config_error);
    if (failure_status != SALTS_OK) goto cleanup;
    tcp_input = accept(tcp_input_listener, NULL, NULL);
    if (tcp_input == TEST_INVALID_SOCKET && !test_socket_would_block()) {
      failure_status = SALTS_EIO;
      goto cleanup;
    }
  }
  if (tcp_input == TEST_INVALID_SOCKET) {
    failure_status = SALTS_ETIMEDOUT;
    goto cleanup;
  }
  failure_stage = "TCP input send";
  if (send(tcp_input, payload, (int)(sizeof(payload) - 1u), 0) != (int)(sizeof(payload) - 1u)) {
    failure_status = SALTS_EIO;
    goto cleanup;
  }

  failure_stage = "TCP/UDP loopback poll";
  for (int i = 0; i < TEST_POLL_LIMIT; ++i) {
    int received_size;
    config_error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    failure_status = turbo_flow_plugin_generation_poll(generation, 10u, &config_error);
    if (failure_status != SALTS_OK) goto cleanup;
    if (!udp_received) {
      received_size = recv(receiver, received, (int)sizeof(received), 0);
      if (received_size >= 0) {
        if ((size_t)received_size != sizeof(payload) - 1u ||
            memcmp(received, payload, sizeof(payload) - 1u) != 0) {
          failure_status = SALTS_EPROTO;
          goto cleanup;
        }
        udp_received = 1;
      } else if (!test_socket_would_block()) {
        failure_status = SALTS_EIO;
        goto cleanup;
      }
    }
    if (tcp_output == TEST_INVALID_SOCKET) {
      tcp_output = accept(tcp_receiver, NULL, NULL);
      if (tcp_output != TEST_INVALID_SOCKET && test_socket_nonblocking(tcp_output) != 0) {
        failure_status = SALTS_EIO;
        goto cleanup;
      } else if (tcp_output == TEST_INVALID_SOCKET && !test_socket_would_block()) {
        failure_status = SALTS_EIO;
        goto cleanup;
      }
    }
    if (tcp_output != TEST_INVALID_SOCKET && !tcp_received) {
      received_size = recv(tcp_output, tcp_received_data + tcp_received_bytes,
                           (int)(sizeof(tcp_received_data) - tcp_received_bytes), 0);
      if (received_size == 0) {
        failure_status = SALTS_ESHUTDOWN;
        goto cleanup;
      }
      if (received_size > 0) {
        tcp_received_bytes += (size_t)received_size;
        if (tcp_received_bytes > sizeof(payload) - 1u ||
            memcmp(tcp_received_data, payload, tcp_received_bytes) != 0) {
          failure_status = SALTS_EPROTO;
          goto cleanup;
        }
        tcp_received = tcp_received_bytes == sizeof(payload) - 1u;
      } else if (received_size < 0 && !test_socket_would_block()) {
        failure_status = SALTS_EIO;
        goto cleanup;
      }
    }
    if (udp_received && tcp_received) {
      rc = 0;
      break;
    }
  }
  if (rc != 0) failure_status = SALTS_ETIMEDOUT;

cleanup:
  if (cleanup_generation) {
    int cleanup_rc =
        turbo_flow_plugin_generation_destroy(cleanup_generation, 1000u, &destroy_config_error);
    if (cleanup_rc == SALTS_OK) cleanup_generation = NULL;
    else {
      failure_status = cleanup_rc;
      rc = 1;
    }
  }
  if (generation) {
    destroy_config_error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    if (turbo_flow_plugin_generation_destroy(generation, 1000u, &destroy_config_error) !=
        SALTS_OK) {
      if (failure_status == SALTS_OK) {
        failure_status = destroy_config_error.status;
        failure_stage = "plugin generation destroy";
        config_error = destroy_config_error;
      }
      rc = 1;
    }
  }
  turbo_flow_destroy(flow);
  turbo_flow_resolved_config_destroy(resolved);
  turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
  if (host) {
    if (turbo_flow_plugin_host_destroy(host, 1000u, &destroy_error) != SALTS_OK) {
      if (failure_status == SALTS_OK) {
        failure_status = destroy_error.status;
        failure_stage = "plugin host destroy";
        plugin_error = destroy_error;
      }
      rc = 1;
    }
  }
  if (receiver != TEST_INVALID_SOCKET) test_close_socket(receiver);
  if (tcp_input != TEST_INVALID_SOCKET) test_close_socket(tcp_input);
  if (tcp_input_listener != TEST_INVALID_SOCKET) test_close_socket(tcp_input_listener);
  if (tcp_output != TEST_INVALID_SOCKET) test_close_socket(tcp_output);
  if (tcp_receiver != TEST_INVALID_SOCKET) test_close_socket(tcp_receiver);
  test_socket_finish();
  if (rc != 0) {
    fprintf(stderr, "CNet plugin Gateway consumer failed at %s (status=%d)", failure_stage,
            failure_status);
    if (plugin_error.message[0]) fprintf(stderr, ": %s", plugin_error.message);
    if (config_error.message[0]) fprintf(stderr, ": %s", config_error.message);
    fputc('\n', stderr);
  }
  return rc;
}
