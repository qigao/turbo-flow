#include "turbo_flow_cnet_typed_config_internal.h"

#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <uri_parser.h>

#if defined(_WIN32)
  #include <winsock2.h>
  #include <ws2tcpip.h>
#else
  #include <arpa/inet.h>
  #include <netinet/tcp.h>
#endif

enum {
  CNET_TYPED_KCP_MIN_MTU = 50,
  CNET_TYPED_SECURE_KCP_MIN_MTU = 576,
  CNET_TYPED_KCP_MIN_INTERVAL_MS = 10,
  CNET_TYPED_KCP_MAX_INTERVAL_MS = 5000,
  CNET_TYPED_FEC_MAX_RECEIVE_GROUPS = 64,
  CNET_TYPED_FEC_MAX_TOTAL_SHARDS = 255,
  CNET_TYPED_FEC_MAX_STATE_BYTES = 64 * 1024 * 1024,
  CNET_TYPED_SECURE_FEC_WIRE_OVERHEAD_BYTES = 46,
  CNET_TYPED_URI_HOST_CAPACITY = 254,
  CNET_TYPED_TLS_SERVER_NAME_MAX_BYTES = 253,
  CNET_TYPED_CLIENT_COMMAND_PAYLOAD_MIN_BYTES = 2048,
  CNET_TYPED_CLIENT_QUEUE_ENTRY_ACCOUNTING_BYTES = 2048
};

static int typed_fail(
    turbo_flow_config_error_t *error, int status, const char *instance_name,
    const char *field, const char *message) {
  if (error && error->size == sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(
        error->path, sizeof(error->path), "$.stages.%s%s%s",
        instance_name ? instance_name : "",
        field && field[0] ? "." : "", field ? field : "");
    (void)snprintf(
        error->message, sizeof(error->message), "%s",
        message ? message : "CNet typed provider policy rejected");
  }
  return status;
}

static int text_present(const char *value) {
  return value && value[0] != '\0';
}

static const char *optional_text(const char *value) {
  return text_present(value) ? value : NULL;
}

static int deployment_tls_empty(
    const turbo_flow_cnet_deployment_view_t *deployment) {
  return deployment &&
         !text_present(deployment->tls_ca_file) &&
         !text_present(deployment->tls_ca_path) &&
         !text_present(deployment->tls_cert_file) &&
         !text_present(deployment->tls_key_file) &&
         !text_present(deployment->tls_key_password) &&
         !text_present(deployment->tls_server_name) &&
         deployment->alpn_protocol_count == 0u;
}

static int deployment_psk_empty(
    const turbo_flow_cnet_deployment_view_t *deployment) {
  return deployment && deployment->psk_size == 0u && deployment->psk == NULL;
}

static int power_of_two(size_t value) {
  return value && (value & (value - 1u)) == 0u;
}

static int backend_kind(uint32_t value, native_io_backend_kind *backend) {
  if (!backend) return SALTS_EINVAL;
  switch (value) {
    case 0u: *backend = NATIVE_IO_BACKEND_IOCP; break;
    case 1u: *backend = NATIVE_IO_BACKEND_EPOLL; break;
    case 2u: *backend = NATIVE_IO_BACKEND_KQUEUE; break;
    case 3u: *backend = NATIVE_IO_BACKEND_IO_URING; break;
    default: return SALTS_EINVAL;
  }
  return native_io_backend_kind_supported(*backend)
             ? SALTS_OK
             : SALTS_ENOTSUP;
}

static int source_encoding(
    uint32_t value, turbo_flow_data_encoding_t *encoding) {
  if (!encoding) return SALTS_EINVAL;
  switch (value) {
    case 0u: *encoding = TURBO_FLOW_DATA_ENCODING_TBE; break;
    case 1u: *encoding = TURBO_FLOW_DATA_ENCODING_JSON; break;
    case 2u: *encoding = TURBO_FLOW_DATA_ENCODING_CSV; break;
    case 3u: *encoding = TURBO_FLOW_DATA_ENCODING_XML; break;
    case 4u: *encoding = TURBO_FLOW_DATA_ENCODING_UTF8; break;
    case 5u: *encoding = TURBO_FLOW_DATA_ENCODING_OPAQUE; break;
    default: return SALTS_EINVAL;
  }
  return SALTS_OK;
}

static int build_content(
    uint32_t encoding_value, const char *media_type,
    const char *schema_name, const char *type_name,
    uint32_t schema_version, turbo_flow_content_descriptor_t *out) {
  turbo_flow_data_encoding_t encoding;
  int rc;
  if (!media_type || !schema_name || !type_name || !out)
    return SALTS_EINVAL;
  rc = source_encoding(encoding_value, &encoding);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_content_descriptor_init(
      out, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
      encoding, media_type, "cnet.business");
  if (rc == SALTS_OK)
    rc = turbo_flow_content_descriptor_declare_schema(
        out, schema_name, type_name, schema_version);
  return rc;
}


static int build_sink_content(turbo_flow_content_descriptor_t *out) {
  int rc;
  if (!out) return SALTS_EINVAL;
  rc = turbo_flow_content_descriptor_init(
      out, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
      TURBO_FLOW_CONTENT_PROFILE_GENERIC, TURBO_FLOW_DATA_ENCODING_OPAQUE,
      "application/octet-stream", "cnet.payload");
  if (rc == SALTS_OK)
    rc = turbo_flow_content_descriptor_declare_schema(
        out, "CNetPayload", "Bytes", 1u);
  return rc;
}

#define CNET_TYPED_FILL_CLIENT(OUT, TYPED)                                  \
  do {                                                                       \
    (OUT)->client.connection_capacity = (TYPED)->connection_capacity;        \
    (OUT)->client.command_capacity = (TYPED)->command_capacity;              \
    (OUT)->client.request_capacity = (TYPED)->request_capacity;              \
    (OUT)->client.completion_batch_capacity =                                \
        (TYPED)->completion_batch_capacity;                                  \
    (OUT)->client.event_capacity = (TYPED)->event_capacity;                  \
    (OUT)->client.max_send_bytes = (TYPED)->max_send_bytes;                  \
    (OUT)->client.receive_buffer_bytes = (TYPED)->receive_buffer_bytes;      \
    (OUT)->client.connect_timeout_ms = (TYPED)->connect_timeout_ms;          \
    (OUT)->client.read_timeout_ms = (TYPED)->read_timeout_ms;                \
    (OUT)->client.write_timeout_ms = (TYPED)->write_timeout_ms;              \
    (OUT)->client.tls_io_buffer_bytes = (TYPED)->tls_io_buffer_bytes;        \
    (OUT)->client.tls_handshake_timeout_ms =                                 \
        (TYPED)->tls_handshake_timeout_ms;                                   \
    (OUT)->client.command_buffer_bytes = (TYPED)->command_buffer_bytes;      \
    (OUT)->client.event_buffer_bytes = (TYPED)->event_buffer_bytes;          \
  } while (0)

#define CNET_TYPED_FILL_SOCKET(OUT, TYPED)                                   \
  do {                                                                       \
    (OUT)->socket_options =                                                   \
        (cnet_stream_socket_options)CNET_STREAM_SOCKET_OPTIONS_INIT;         \
    (OUT)->socket_options.receive_buffer_bytes =                              \
        (TYPED)->socket_receive_buffer_bytes;                                 \
    (OUT)->socket_options.send_buffer_bytes =                                 \
        (TYPED)->socket_send_buffer_bytes;                                    \
    (OUT)->socket_options.keepalive = (TYPED)->keepalive != 0u;              \
    (OUT)->socket_options.keepalive_idle_ms =                                 \
        (TYPED)->keepalive_idle_ms;                                           \
    (OUT)->socket_options.keepalive_interval_ms =                             \
        (TYPED)->keepalive_interval_ms;                                       \
    (OUT)->socket_options.keepalive_count = (TYPED)->keepalive_count;        \
    (OUT)->socket_options.linger = (TYPED)->linger != 0u;                    \
    (OUT)->socket_options.linger_ms = (TYPED)->linger_ms;                    \
  } while (0)

#define CNET_TYPED_FILL_DATAGRAM(OUT, TYPED)                                 \
  do {                                                                       \
    (OUT)->datagram = (cnet_datagram_config)CNET_DATAGRAM_CONFIG_INIT;       \
    (OUT)->datagram.send_capacity = (TYPED)->datagram_send_capacity;         \
    (OUT)->datagram.request_capacity = (TYPED)->request_capacity;            \
    (OUT)->datagram.completion_batch_capacity =                              \
        (TYPED)->completion_batch_capacity;                                  \
    (OUT)->datagram.max_datagram_bytes = (TYPED)->max_datagram_bytes;        \
    (OUT)->datagram.receive_buffer_bytes = (TYPED)->receive_buffer_bytes;    \
    (OUT)->datagram.reuse_port = (TYPED)->reuse_port != 0u;                  \
  } while (0)

static int client_validate(cnet_client_config *client) {
  size_t max_command_payload_bytes;
  size_t max_event_payload_bytes;
  if (!client) return SALTS_EINVAL;
  if (!native_io_backend_kind_supported(client->backend) ||
      !power_of_two(client->command_capacity) ||
      !power_of_two(client->event_capacity) ||
      client->event_capacity < 2u ||
      client->completion_batch_capacity > client->request_capacity ||
      client->connection_capacity > UINT32_MAX / 2u ||
      client->request_capacity > UINT32_MAX ||
      (client->command_buffer_bytes &&
       client->command_buffer_bytes < client->max_send_bytes) ||
      (client->event_buffer_bytes &&
       client->event_buffer_bytes < client->receive_buffer_bytes) ||
      ((client->tls_io_buffer_bytes == 0u) !=
       (client->tls_handshake_timeout_ms == 0u)) ||
      (client->tls_io_buffer_bytes &&
       (client->tls_io_buffer_bytes < CNET_TLS_MIN_IO_BUFFER_BYTES ||
        client->tls_io_buffer_bytes > INT_MAX)))
    return SALTS_EINVAL;

  max_command_payload_bytes =
      client->max_send_bytes > CNET_TYPED_CLIENT_COMMAND_PAYLOAD_MIN_BYTES
          ? client->max_send_bytes
          : CNET_TYPED_CLIENT_COMMAND_PAYLOAD_MIN_BYTES;
  max_event_payload_bytes =
      client->receive_buffer_bytes > CNET_TLS_ALPN_NAME_MAX_BYTES
          ? client->receive_buffer_bytes
          : CNET_TLS_ALPN_NAME_MAX_BYTES;
  if (client->command_capacity >
          SIZE_MAX / CNET_TYPED_CLIENT_QUEUE_ENTRY_ACCOUNTING_BYTES ||
      (client->command_buffer_bytes == 0u &&
       client->command_capacity > SIZE_MAX / max_command_payload_bytes) ||
      client->event_capacity >
          SIZE_MAX / CNET_TYPED_CLIENT_QUEUE_ENTRY_ACCOUNTING_BYTES ||
      (client->event_buffer_bytes == 0u &&
       client->event_capacity > SIZE_MAX / max_event_payload_bytes))
    return SALTS_ERANGE;
  if (client->command_buffer_bytes &&
      client->command_buffer_bytes < CNET_TYPED_CLIENT_COMMAND_PAYLOAD_MIN_BYTES)
    return SALTS_ERANGE;
  if (client->event_buffer_bytes && client->tls_io_buffer_bytes &&
      client->event_buffer_bytes < CNET_TLS_ALPN_NAME_MAX_BYTES)
    return SALTS_ERANGE;
  return SALTS_OK;
}

static int socket_validate(cnet_stream_socket_options *socket) {
  int rc;
  if (!socket) return SALTS_EINVAL;
  rc = cnet_stream_socket_options_validate(socket);
  if (rc != SALTS_OK) return rc;
#if defined(_WIN32)
  if (socket->keepalive_count != 0u) return SALTS_ENOTSUP;
#else
  #if !defined(TCP_KEEPIDLE) && !defined(TCP_KEEPALIVE)
  if (socket->keepalive_idle_ms != 0u) return SALTS_ENOTSUP;
  #endif
  #if !defined(TCP_KEEPINTVL)
  if (socket->keepalive_interval_ms != 0u) return SALTS_ENOTSUP;
  #endif
  #if !defined(TCP_KEEPCNT)
  if (socket->keepalive_count != 0u) return SALTS_ENOTSUP;
  #endif
#endif
  return SALTS_OK;
}

static int validate_stream_uri(const char *uri) {
  uri_t parsed;
  const char *expected_scheme;
  if (!uri) return SALTS_EINVAL;
  if (strncmp(uri, "pipe://", 7u) == 0)
    return uri[7] && !strpbrk(uri + 7u, "/?#") ? SALTS_OK : SALTS_EINVAL;
  if (strncmp(uri, "tcp://", 6u) == 0) expected_scheme = "tcp";
  else if (strncmp(uri, "tls://", 6u) == 0) expected_scheme = "tls";
  else return SALTS_ENOTSUP;
  memset(&parsed, 0, sizeof(parsed));
  if (!uri_parse(uri, &parsed) || !parsed.valid) return SALTS_EINVAL;
  if (strcmp(parsed.scheme, expected_scheme) != 0 || !parsed.host[0] ||
      (parsed.component_flags & URI_COMPONENT_USERINFO) || parsed.path[0] ||
      (parsed.component_flags & URI_COMPONENT_QUERY) ||
      (parsed.component_flags & URI_COMPONENT_FRAGMENT))
    return SALTS_EINVAL;
  if (strlen(parsed.host) >= CNET_TYPED_URI_HOST_CAPACITY) return SALTS_ERANGE;
  if (!(parsed.component_flags & URI_COMPONENT_PORT) ||
      (parsed.overflow_flags & URI_OVERFLOW_PORT) ||
      parsed.port <= 0 || parsed.port > UINT16_MAX)
    return SALTS_ERANGE;
  return SALTS_OK;
}

static int stream_tls(
    cnet_typed_runtime_config_t *out,
    const turbo_flow_cnet_deployment_view_t *deployment) {
  int tls_uri;
  if (!out || !deployment || !turbo_flow_cnet_deployment_view_valid(deployment) ||
      !text_present(deployment->uri))
    return SALTS_EPROTO;
  tls_uri = strncmp(deployment->uri, "tls://", 6u) == 0;
  if (validate_stream_uri(deployment->uri) != SALTS_OK)
    return SALTS_EINVAL;
  if (!tls_uri) {
    if (out->client.tls_io_buffer_bytes ||
        out->client.tls_handshake_timeout_ms ||
        !deployment_tls_empty(deployment))
      return SALTS_EINVAL;
    out->tls_enabled = 0;
    out->uri = deployment->uri;
    return SALTS_OK;
  }
  if (out->client.tls_io_buffer_bytes == 0u ||
      out->client.tls_handshake_timeout_ms == 0u)
    return SALTS_EINVAL;
  if (text_present(deployment->tls_server_name) &&
      strlen(deployment->tls_server_name) >
          CNET_TYPED_TLS_SERVER_NAME_MAX_BYTES)
    return SALTS_ERANGE;
  if (!!text_present(deployment->tls_cert_file) !=
          !!text_present(deployment->tls_key_file) ||
      (text_present(deployment->tls_key_password) &&
       !text_present(deployment->tls_key_file)))
    return SALTS_EINVAL;

  out->tls_enabled = 1;
  out->uri = deployment->uri;
  out->tls_client.size = sizeof(out->tls_client);
  out->tls_client.ca_file = optional_text(deployment->tls_ca_file);
  out->tls_client.ca_path = optional_text(deployment->tls_ca_path);
  out->tls_client.cert_file = optional_text(deployment->tls_cert_file);
  out->tls_client.key_file = optional_text(deployment->tls_key_file);
  out->tls_client.key_password = optional_text(deployment->tls_key_password);
  out->tls_client.server_name = optional_text(deployment->tls_server_name);
  out->tls_client.alpn_protocols = deployment->alpn_protocols;
  out->tls_client.alpn_protocol_count = deployment->alpn_protocol_count;
  return SALTS_OK;
}

static int validate_numeric_host(const char *host) {
  unsigned char bytes[16] = {0};
  if (!host || !host[0]) return SALTS_EINVAL;
#if defined(_WIN32)
  return InetPtonA(AF_INET, host, bytes) == 1 ||
                 InetPtonA(AF_INET6, host, bytes) == 1
             ? SALTS_OK
             : SALTS_EINVAL;
#else
  return inet_pton(AF_INET, host, bytes) == 1 ||
                 inet_pton(AF_INET6, host, bytes) == 1
             ? SALTS_OK
             : SALTS_EINVAL;
#endif
}

static int parse_peer(
    const turbo_flow_cnet_deployment_view_t *deployment,
    cnet_datagram_peer *peer) {
  unsigned char bytes[16] = {0};
  if (!deployment || !peer || !text_present(deployment->peer_host) ||
      deployment->peer_port == 0u)
    return SALTS_EINVAL;
#if defined(_WIN32)
  if (InetPtonA(AF_INET, deployment->peer_host, bytes) == 1) {
#else
  if (inet_pton(AF_INET, deployment->peer_host, bytes) == 1) {
#endif
    peer->family = CNET_DATAGRAM_ADDRESS_IPV4;
    memcpy(peer->address, bytes, 4u);
  } else {
#if defined(_WIN32)
    if (InetPtonA(AF_INET6, deployment->peer_host, bytes) != 1)
      return SALTS_EINVAL;
#else
    if (inet_pton(AF_INET6, deployment->peer_host, bytes) != 1)
      return SALTS_EINVAL;
#endif
    peer->family = CNET_DATAGRAM_ADDRESS_IPV6;
    memcpy(peer->address, bytes, 16u);
  }
  peer->port = deployment->peer_port;
  peer->scope_id = deployment->peer_scope_id;
  return SALTS_OK;
}

static int datagram_validate(
    cnet_typed_runtime_config_t *out,
    const turbo_flow_cnet_deployment_view_t *deployment) {
  cnet_datagram_config *datagram;
  if (!out || !deployment ||
      !turbo_flow_cnet_deployment_view_valid(deployment) ||
      !text_present(deployment->bind_host))
    return SALTS_EPROTO;
  if (validate_numeric_host(deployment->bind_host) != SALTS_OK)
    return SALTS_EINVAL;
  datagram = &out->datagram;
  datagram->host = deployment->bind_host;
  datagram->port = deployment->bind_port;
  if (datagram->request_capacity <= datagram->send_capacity ||
      datagram->completion_batch_capacity > datagram->request_capacity ||
      datagram->send_capacity > UINT32_MAX ||
      datagram->request_capacity > UINT32_MAX ||
      datagram->max_datagram_bytes > CNET_DATAGRAM_MAX_PAYLOAD_BYTES ||
      datagram->receive_buffer_bytes < datagram->max_datagram_bytes ||
      datagram->receive_buffer_bytes > CNET_DATAGRAM_MAX_PAYLOAD_BYTES ||
      datagram->send_capacity > SIZE_MAX / datagram->max_datagram_bytes)
    return SALTS_EINVAL;
#if !defined(SO_REUSEPORT)
  if (datagram->reuse_port) return SALTS_ENOTSUP;
#endif
  out->bind_host = deployment->bind_host;
  return SALTS_OK;
}

static int packet_validate(
    cnet_typed_runtime_config_t *out,
    const turbo_flow_cnet_deployment_view_t *deployment) {
  cnet_packet_endpoint_config *endpoint;
  cnet_kcp_config *kcp;
  cnet_kcp_fec_config *fec;
  size_t total_shards;
  size_t shard_size;
  size_t group_bytes;
  size_t fec_datagram_bytes;

  if (!out || !deployment) return SALTS_EINVAL;
  endpoint = &out->endpoint;
  kcp = &endpoint->kcp;
  fec = &endpoint->security.fec;
  if (endpoint->session_capacity > UINT32_MAX) return SALTS_ERANGE;

  if (endpoint->protocol == CNET_PACKET_UDP) {
    if (kcp->mtu || kcp->send_window || kcp->receive_window ||
        kcp->interval_ms || kcp->fast_resend ||
        kcp->no_congestion_window || kcp->stream_mode ||
        kcp->send_segment_capacity || kcp->max_message_bytes ||
        endpoint->security.mode != CNET_KCP_SECURITY_NONE ||
        endpoint->security.handshake_retry_ms ||
        fec->backend != CNET_KCP_FEC_NONE || fec->data_shards ||
        fec->parity_shards || fec->max_payload_bytes ||
        fec->receive_group_count || !deployment_psk_empty(deployment))
      return SALTS_EINVAL;
    return SALTS_OK;
  }

  if (endpoint->protocol != CNET_PACKET_KCP ||
      kcp->mtu == 0u || kcp->send_window == 0u ||
      kcp->receive_window == 0u || kcp->interval_ms == 0u ||
      kcp->send_segment_capacity == 0u ||
      kcp->max_message_bytes == 0u || kcp->stream_mode)
    return SALTS_EINVAL;

  if (endpoint->security.mode == CNET_KCP_SECURITY_NONE) {
    if (kcp->mtu < CNET_TYPED_KCP_MIN_MTU ||
        kcp->mtu > endpoint->datagram.max_datagram_bytes ||
        kcp->send_window > (uint32_t)INT_MAX ||
        kcp->receive_window > (uint32_t)INT_MAX ||
        kcp->interval_ms < CNET_TYPED_KCP_MIN_INTERVAL_MS ||
        kcp->interval_ms > CNET_TYPED_KCP_MAX_INTERVAL_MS ||
        kcp->fast_resend > (uint32_t)INT_MAX ||
        kcp->send_segment_capacity > (size_t)INT_MAX ||
        kcp->max_message_bytes > (size_t)INT_MAX ||
        endpoint->security.handshake_retry_ms != 0u ||
        fec->backend != CNET_KCP_FEC_NONE || fec->data_shards ||
        fec->parity_shards || fec->max_payload_bytes ||
        fec->receive_group_count || !deployment_psk_empty(deployment))
      return SALTS_EINVAL;
    return SALTS_OK;
  }

  if (endpoint->security.mode != CNET_KCP_SECURITY_PSK_V1 ||
      kcp->mtu < CNET_TYPED_SECURE_KCP_MIN_MTU ||
      kcp->mtu > CNET_DATAGRAM_MAX_PAYLOAD_BYTES ||
      kcp->send_window > (uint32_t)INT_MAX ||
      kcp->receive_window > (uint32_t)INT_MAX ||
      kcp->interval_ms < CNET_TYPED_KCP_MIN_INTERVAL_MS ||
      kcp->interval_ms > CNET_TYPED_KCP_MAX_INTERVAL_MS ||
      kcp->fast_resend > (uint32_t)INT_MAX ||
      kcp->send_segment_capacity > (size_t)INT_MAX ||
      kcp->max_message_bytes > (size_t)INT_MAX ||
      deployment->psk_size != CNET_KCP_PSK_BYTES || !deployment->psk ||
      endpoint->security.handshake_retry_ms == 0u ||
      fec->backend != CNET_KCP_FEC_REED_SOLOMON ||
      fec->data_shards == 0u || fec->parity_shards == 0u)
    return SALTS_EINVAL;

  {
    uint8_t combined = 0u;
    for (size_t i = 0u; i < CNET_KCP_PSK_BYTES; ++i)
      combined |= deployment->psk[i];
    if (combined == 0u) return SALTS_EINVAL;
    memcpy(
        endpoint->security.pre_shared_key,
        deployment->psk, CNET_KCP_PSK_BYTES);
  }

  total_shards = (size_t)fec->data_shards + fec->parity_shards;
  shard_size = (size_t)fec->max_payload_bytes + 2u;
  if (total_shards > CNET_TYPED_FEC_MAX_TOTAL_SHARDS ||
      fec->receive_group_count == 0u ||
      fec->receive_group_count > CNET_TYPED_FEC_MAX_RECEIVE_GROUPS ||
      fec->max_payload_bytes <
          kcp->mtu + CNET_KCP_SECURE_RECORD_OVERHEAD ||
      total_shards > SIZE_MAX / shard_size)
    return SALTS_EINVAL;

  group_bytes = total_shards * shard_size;
  if ((size_t)fec->receive_group_count > SIZE_MAX / group_bytes ||
      group_bytes * fec->receive_group_count >
          CNET_TYPED_FEC_MAX_STATE_BYTES)
    return SALTS_ERANGE;

  fec_datagram_bytes =
      (size_t)fec->max_payload_bytes +
      CNET_TYPED_SECURE_FEC_WIRE_OVERHEAD_BYTES;
  if (fec_datagram_bytes > endpoint->datagram.max_datagram_bytes ||
      fec_datagram_bytes > endpoint->datagram.receive_buffer_bytes)
    return SALTS_EINVAL;
  return SALTS_OK;
}

#define CNET_TYPED_SOURCE_TAIL(OUT, TYPED)                                   \
  do {                                                                       \
    (OUT)->max_message_bytes = (TYPED)->max_message_bytes;                   \
    (OUT)->scheduler_capacity = (TYPED)->scheduler_capacity;                 \
    (OUT)->scheduler_max_steps_per_poll =                                    \
        (TYPED)->scheduler_max_steps_per_poll;                               \
    (OUT)->first_message_id = (TYPED)->first_message_id;                     \
    (OUT)->initial_demand = (TYPED)->initial_demand;                         \
    (OUT)->stop_timeout_ms = (TYPED)->stop_timeout_ms;                       \
  } while (0)

#define CNET_TYPED_SINK_TAIL(OUT, TYPED)                                     \
  do {                                                                       \
    (OUT)->max_message_bytes = (TYPED)->max_message_bytes;                   \
    (OUT)->actor_command_capacity = (TYPED)->actor_command_capacity;         \
    (OUT)->actor_max_steps_per_poll =                                        \
        (TYPED)->actor_max_steps_per_poll;                                   \
    (OUT)->stop_timeout_ms = (TYPED)->stop_timeout_ms;                       \
  } while (0)

#define CNET_TYPED_PACKET_COMMON(OUT, TYPED)                                 \
  do {                                                                       \
    CNET_TYPED_FILL_DATAGRAM((OUT), (TYPED));                                \
    (OUT)->endpoint =                                                        \
        (cnet_packet_endpoint_config)CNET_PACKET_ENDPOINT_CONFIG_INIT;       \
    (OUT)->endpoint.datagram = (OUT)->datagram;                              \
    (OUT)->endpoint.protocol =                                               \
        (TYPED)->packet_mode == 0u ? CNET_PACKET_UDP : CNET_PACKET_KCP;      \
    (OUT)->endpoint.session_capacity = (TYPED)->session_capacity;            \
    (OUT)->endpoint.kcp = (cnet_kcp_config)CNET_KCP_CONFIG_INIT;             \
    (OUT)->endpoint.kcp.conversation = 0u;                                   \
    (OUT)->endpoint.kcp.mtu = (TYPED)->kcp_mtu;                              \
    (OUT)->endpoint.kcp.send_window = (TYPED)->kcp_send_window;              \
    (OUT)->endpoint.kcp.receive_window = (TYPED)->kcp_receive_window;        \
    (OUT)->endpoint.kcp.interval_ms = (TYPED)->kcp_interval_ms;              \
    (OUT)->endpoint.kcp.fast_resend = (TYPED)->kcp_fast_resend;              \
    (OUT)->endpoint.kcp.no_congestion_window =                               \
        (TYPED)->kcp_no_congestion_window != 0u;                             \
    (OUT)->endpoint.kcp.stream_mode = (TYPED)->kcp_stream_mode != 0u;        \
    (OUT)->endpoint.kcp.send_segment_capacity =                              \
        (TYPED)->kcp_send_segment_capacity;                                  \
    (OUT)->endpoint.kcp.max_message_bytes =                                  \
        (TYPED)->kcp_max_message_bytes;                                      \
    (OUT)->endpoint.security =                                               \
        (cnet_kcp_security_config)CNET_KCP_SECURITY_CONFIG_INIT;             \
    (OUT)->endpoint.security.mode =                                          \
        (TYPED)->security_mode == 0u                                         \
            ? CNET_KCP_SECURITY_NONE                                         \
            : CNET_KCP_SECURITY_PSK_V1;                                      \
    (OUT)->endpoint.security.handshake_retry_ms =                            \
        (TYPED)->handshake_retry_ms;                                         \
    (OUT)->endpoint.security.fec.backend =                                   \
        (TYPED)->fec_backend == 0u                                           \
            ? CNET_KCP_FEC_NONE                                              \
            : CNET_KCP_FEC_REED_SOLOMON;                                    \
    (OUT)->endpoint.security.fec.data_shards =                               \
        (uint16_t)(TYPED)->fec_data_shards;                                  \
    (OUT)->endpoint.security.fec.parity_shards =                             \
        (uint16_t)(TYPED)->fec_parity_shards;                                \
    (OUT)->endpoint.security.fec.max_payload_bytes =                         \
        (uint16_t)(TYPED)->fec_max_payload_bytes;                            \
    (OUT)->endpoint.security.fec.receive_group_count =                       \
        (uint16_t)(TYPED)->fec_receive_group_count;                          \
  } while (0)

static int packet_u16_fields_valid(
    uint32_t data_shards, uint32_t parity_shards,
    uint32_t max_payload_bytes, uint32_t receive_group_count) {
  return data_shards <= UINT16_MAX &&
         parity_shards <= UINT16_MAX &&
         max_payload_bytes <= UINT16_MAX &&
         receive_group_count <= UINT16_MAX;
}

int cnet_typed_stream_source_config(
    const CNetStreamSourceConfig_t *typed,
    const turbo_flow_cnet_deployment_view_t *deployment,
    const char *instance_name, cnet_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error) {
  int rc;
  if (!typed || !deployment || !instance_name || !instance_name[0] || !out)
    return typed_fail(error, SALTS_EINVAL, instance_name, NULL,
                      "invalid CNet stream source arguments");
  if (typed->schema_version != 2u)
    return typed_fail(error, SALTS_ENOTSUP, instance_name, "schema_version",
                      "unsupported CNet stream source schema");
  memset(out, 0, sizeof(*out));
  out->kind = CNET_TYPED_STREAM_SOURCE;
  rc = backend_kind(typed->backend, &out->client.backend);
  if (rc == SALTS_OK) {
    CNET_TYPED_FILL_CLIENT(out, typed);
    rc = client_validate(&out->client);
  }
  if (rc == SALTS_OK) {
    CNET_TYPED_FILL_SOCKET(out, typed);
    rc = socket_validate(&out->socket_options);
  }
  if (rc == SALTS_OK) rc = stream_tls(out, deployment);
  CNET_TYPED_SOURCE_TAIL(out, typed);
  if (rc == SALTS_OK &&
      out->max_message_bytes > out->client.receive_buffer_bytes)
    rc = SALTS_ERANGE;
  if (rc == SALTS_OK)
    rc = build_content(
        typed->content_encoding, typed->content_media_type,
        typed->content_schema, typed->content_type,
        typed->content_schema_version, &out->content);
  return rc == SALTS_OK
             ? SALTS_OK
             : typed_fail(error, rc, instance_name, "policy",
                          "inconsistent CNet stream source policy/resource");
}

int cnet_typed_stream_sink_config(
    const CNetStreamSinkConfig_t *typed,
    const turbo_flow_cnet_deployment_view_t *deployment,
    const char *instance_name, cnet_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error) {
  int rc;
  if (!typed || !deployment || !instance_name || !instance_name[0] || !out)
    return typed_fail(error, SALTS_EINVAL, instance_name, NULL,
                      "invalid CNet stream sink arguments");
  if (typed->schema_version != 2u)
    return typed_fail(error, SALTS_ENOTSUP, instance_name, "schema_version",
                      "unsupported CNet stream sink schema");
  memset(out, 0, sizeof(*out));
  out->kind = CNET_TYPED_STREAM_SINK;
  rc = backend_kind(typed->backend, &out->client.backend);
  if (rc == SALTS_OK) {
    CNET_TYPED_FILL_CLIENT(out, typed);
    rc = client_validate(&out->client);
  }
  if (rc == SALTS_OK) {
    CNET_TYPED_FILL_SOCKET(out, typed);
    rc = socket_validate(&out->socket_options);
  }
  if (rc == SALTS_OK) rc = stream_tls(out, deployment);
  CNET_TYPED_SINK_TAIL(out, typed);
  if (rc == SALTS_OK) rc = build_sink_content(&out->content);
  if (rc == SALTS_OK &&
      out->max_message_bytes > out->client.max_send_bytes)
    rc = SALTS_ERANGE;
  return rc == SALTS_OK
             ? SALTS_OK
             : typed_fail(error, rc, instance_name, "policy",
                          "inconsistent CNet stream sink policy/resource");
}

int cnet_typed_listener_source_config(
    const CNetListenerSourceConfig_t *typed,
    const turbo_flow_cnet_deployment_view_t *deployment,
    const char *instance_name, cnet_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error) {
  int rc;
  if (!typed || !deployment || !instance_name || !instance_name[0] || !out)
    return typed_fail(error, SALTS_EINVAL, instance_name, NULL,
                      "invalid CNet listener source arguments");
  if (typed->schema_version != 2u)
    return typed_fail(error, SALTS_ENOTSUP, instance_name, "schema_version",
                      "unsupported CNet listener source schema");
  if (!turbo_flow_cnet_deployment_view_valid(deployment) ||
      !text_present(deployment->bind_host))
    return typed_fail(error, SALTS_EPROTO, instance_name, "resource",
                      "listener source requires a bind deployment resource");

  memset(out, 0, sizeof(*out));
  out->kind = CNET_TYPED_LISTENER_SOURCE;
  out->tls_enabled = typed->tls_enabled != 0u;
  rc = backend_kind(typed->backend, &out->client.backend);
  if (rc == SALTS_OK) {
    CNET_TYPED_FILL_CLIENT(out, typed);
    rc = client_validate(&out->client);
  }
  if (rc == SALTS_OK) {
    CNET_TYPED_FILL_SOCKET(out, typed);
    rc = socket_validate(&out->socket_options);
  }
  if (rc == SALTS_OK &&
      validate_numeric_host(deployment->bind_host) != SALTS_OK)
    rc = SALTS_EINVAL;

  out->listener.backend = out->client.backend;
  out->listener.host = deployment->bind_host;
  out->listener.port = deployment->bind_port;
  out->listener.backlog = typed->backlog;
  out->listener_options =
      (cnet_listener_options)CNET_LISTENER_OPTIONS_INIT;
  out->listener_options.reuse_port = typed->reuse_port != 0u;
  out->bind_host = deployment->bind_host;
  out->max_connections = typed->max_connections;
  CNET_TYPED_SOURCE_TAIL(out, typed);

#if !defined(SO_REUSEPORT)
  if (rc == SALTS_OK && out->listener_options.reuse_port)
    rc = SALTS_ENOTSUP;
#endif
  if (rc == SALTS_OK &&
      (out->listener.backlog > (size_t)INT_MAX ||
       out->max_connections > out->client.connection_capacity ||
       out->max_message_bytes > out->client.receive_buffer_bytes))
    rc = SALTS_ERANGE;

  if (rc == SALTS_OK) {
    if (typed->tls_client_auth == 0u)
      out->tls_server.client_auth = CNET_TLS_CLIENT_AUTH_NONE;
    else if (typed->tls_client_auth == 1u)
      out->tls_server.client_auth = CNET_TLS_CLIENT_AUTH_REQUIRED;
    else
      rc = SALTS_EINVAL;
  }

  if (rc == SALTS_OK && !out->tls_enabled) {
    if (out->client.tls_io_buffer_bytes ||
        out->client.tls_handshake_timeout_ms ||
        out->tls_server.client_auth != CNET_TLS_CLIENT_AUTH_NONE ||
        !deployment_tls_empty(deployment))
      rc = SALTS_EINVAL;
  } else if (rc == SALTS_OK) {
    if (!text_present(deployment->tls_cert_file) ||
        !text_present(deployment->tls_key_file) ||
        out->client.tls_io_buffer_bytes == 0u ||
        out->client.tls_handshake_timeout_ms == 0u ||
        (out->tls_server.client_auth == CNET_TLS_CLIENT_AUTH_REQUIRED &&
         !text_present(deployment->tls_ca_file) &&
         !text_present(deployment->tls_ca_path)) ||
        (out->tls_server.client_auth == CNET_TLS_CLIENT_AUTH_NONE &&
         (text_present(deployment->tls_ca_file) ||
          text_present(deployment->tls_ca_path))))
      rc = SALTS_EINVAL;
    else {
      out->tls_server.size = sizeof(out->tls_server);
      out->tls_server.ca_file = optional_text(deployment->tls_ca_file);
      out->tls_server.ca_path = optional_text(deployment->tls_ca_path);
      out->tls_server.cert_file = optional_text(deployment->tls_cert_file);
      out->tls_server.key_file = optional_text(deployment->tls_key_file);
      out->tls_server.key_password =
          optional_text(deployment->tls_key_password);
      out->tls_server.alpn_protocols = deployment->alpn_protocols;
      out->tls_server.alpn_protocol_count =
          deployment->alpn_protocol_count;
    }
  }

  if (rc == SALTS_OK)
    rc = build_content(
        typed->content_encoding, typed->content_media_type,
        typed->content_schema, typed->content_type,
        typed->content_schema_version, &out->content);
  return rc == SALTS_OK
             ? SALTS_OK
             : typed_fail(error, rc, instance_name, "policy",
                          "inconsistent CNet listener source policy/resource");
}

static int packet_common(
    cnet_typed_runtime_config_t *out,
    const turbo_flow_cnet_deployment_view_t *deployment,
    uint32_t backend, uint32_t packet_mode,
    uint32_t datagram_send_capacity, uint32_t request_capacity,
    uint32_t completion_batch_capacity, uint32_t max_datagram_bytes,
    uint32_t receive_buffer_bytes, uint32_t reuse_port,
    uint32_t session_capacity, uint32_t kcp_mtu,
    uint32_t kcp_send_window, uint32_t kcp_receive_window,
    uint32_t kcp_interval_ms, uint32_t kcp_fast_resend,
    uint32_t kcp_no_congestion_window, uint32_t kcp_stream_mode,
    uint32_t kcp_send_segment_capacity, uint32_t kcp_max_message_bytes,
    uint32_t security_mode, uint32_t handshake_retry_ms,
    uint32_t fec_backend, uint32_t fec_data_shards,
    uint32_t fec_parity_shards, uint32_t fec_max_payload_bytes,
    uint32_t fec_receive_group_count) {
  int rc;
  if (!out || !deployment || packet_mode > 1u || security_mode > 1u ||
      fec_backend > 1u || reuse_port > 1u ||
      !packet_u16_fields_valid(
          fec_data_shards, fec_parity_shards,
          fec_max_payload_bytes, fec_receive_group_count))
    return SALTS_EINVAL;

  rc = backend_kind(backend, &out->datagram.backend);
  if (rc != SALTS_OK) return rc;
  out->datagram =
      (cnet_datagram_config)CNET_DATAGRAM_CONFIG_INIT;
  out->datagram.backend =
      backend == 0u ? NATIVE_IO_BACKEND_IOCP :
      backend == 1u ? NATIVE_IO_BACKEND_EPOLL :
      backend == 2u ? NATIVE_IO_BACKEND_KQUEUE :
                      NATIVE_IO_BACKEND_IO_URING;
  out->datagram.send_capacity = datagram_send_capacity;
  out->datagram.request_capacity = request_capacity;
  out->datagram.completion_batch_capacity = completion_batch_capacity;
  out->datagram.max_datagram_bytes = max_datagram_bytes;
  out->datagram.receive_buffer_bytes = receive_buffer_bytes;
  out->datagram.reuse_port = reuse_port != 0u;
  rc = datagram_validate(out, deployment);
  if (rc != SALTS_OK) return rc;

  out->endpoint =
      (cnet_packet_endpoint_config)CNET_PACKET_ENDPOINT_CONFIG_INIT;
  out->endpoint.datagram = out->datagram;
  out->endpoint.protocol =
      packet_mode == 0u ? CNET_PACKET_UDP : CNET_PACKET_KCP;
  out->endpoint.session_capacity = session_capacity;
  out->endpoint.kcp =
      (cnet_kcp_config)CNET_KCP_CONFIG_INIT;
  out->endpoint.kcp.conversation = 0u;
  out->endpoint.kcp.mtu = kcp_mtu;
  out->endpoint.kcp.send_window = kcp_send_window;
  out->endpoint.kcp.receive_window = kcp_receive_window;
  out->endpoint.kcp.interval_ms = kcp_interval_ms;
  out->endpoint.kcp.fast_resend = kcp_fast_resend;
  out->endpoint.kcp.no_congestion_window =
      kcp_no_congestion_window != 0u;
  out->endpoint.kcp.stream_mode = kcp_stream_mode != 0u;
  out->endpoint.kcp.send_segment_capacity =
      kcp_send_segment_capacity;
  out->endpoint.kcp.max_message_bytes = kcp_max_message_bytes;
  out->endpoint.security =
      (cnet_kcp_security_config)CNET_KCP_SECURITY_CONFIG_INIT;
  out->endpoint.security.mode =
      security_mode == 0u ? CNET_KCP_SECURITY_NONE
                          : CNET_KCP_SECURITY_PSK_V1;
  out->endpoint.security.handshake_retry_ms = handshake_retry_ms;
  out->endpoint.security.fec.backend =
      fec_backend == 0u ? CNET_KCP_FEC_NONE
                        : CNET_KCP_FEC_REED_SOLOMON;
  out->endpoint.security.fec.data_shards =
      (uint16_t)fec_data_shards;
  out->endpoint.security.fec.parity_shards =
      (uint16_t)fec_parity_shards;
  out->endpoint.security.fec.max_payload_bytes =
      (uint16_t)fec_max_payload_bytes;
  out->endpoint.security.fec.receive_group_count =
      (uint16_t)fec_receive_group_count;
  return packet_validate(out, deployment);
}

int cnet_typed_packet_source_config(
    const CNetPacketSourceConfig_t *typed,
    const turbo_flow_cnet_deployment_view_t *deployment,
    const char *instance_name, cnet_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error) {
  int rc;
  if (!typed || !deployment || !instance_name || !instance_name[0] || !out)
    return typed_fail(error, SALTS_EINVAL, instance_name, NULL,
                      "invalid CNet packet source arguments");
  if (typed->schema_version != 2u)
    return typed_fail(error, SALTS_ENOTSUP, instance_name, "schema_version",
                      "unsupported CNet packet source schema");
  memset(out, 0, sizeof(*out));
  out->kind = CNET_TYPED_PACKET_SOURCE;
  rc = packet_common(
      out, deployment, typed->backend, typed->packet_mode,
      typed->datagram_send_capacity, typed->request_capacity,
      typed->completion_batch_capacity, typed->max_datagram_bytes,
      typed->receive_buffer_bytes, typed->reuse_port,
      typed->session_capacity, typed->kcp_mtu,
      typed->kcp_send_window, typed->kcp_receive_window,
      typed->kcp_interval_ms, typed->kcp_fast_resend,
      typed->kcp_no_congestion_window, typed->kcp_stream_mode,
      typed->kcp_send_segment_capacity, typed->kcp_max_message_bytes,
      typed->security_mode, typed->handshake_retry_ms,
      typed->fec_backend, typed->fec_data_shards,
      typed->fec_parity_shards, typed->fec_max_payload_bytes,
      typed->fec_receive_group_count);
  out->queue_capacity = typed->queue_capacity;
  CNET_TYPED_SOURCE_TAIL(out, typed);
  if (rc == SALTS_OK &&
      out->max_message_bytes >
          (out->endpoint.protocol == CNET_PACKET_UDP
               ? out->endpoint.datagram.max_datagram_bytes
               : out->endpoint.kcp.max_message_bytes))
    rc = SALTS_ERANGE;
  if (rc == SALTS_OK)
    rc = build_content(
        typed->content_encoding, typed->content_media_type,
        typed->content_schema, typed->content_type,
        typed->content_schema_version, &out->content);
  return rc == SALTS_OK
             ? SALTS_OK
             : typed_fail(error, rc, instance_name, "policy",
                          "inconsistent CNet packet source policy/resource");
}

int cnet_typed_datagram_sink_config(
    const CNetDatagramSinkConfig_t *typed,
    const turbo_flow_cnet_deployment_view_t *deployment,
    const char *instance_name, cnet_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error) {
  int rc;
  if (!typed || !deployment || !instance_name || !instance_name[0] || !out)
    return typed_fail(error, SALTS_EINVAL, instance_name, NULL,
                      "invalid CNet datagram sink arguments");
  if (typed->schema_version != 2u)
    return typed_fail(error, SALTS_ENOTSUP, instance_name, "schema_version",
                      "unsupported CNet datagram sink schema");
  memset(out, 0, sizeof(*out));
  out->kind = CNET_TYPED_DATAGRAM_SINK;
  rc = backend_kind(typed->backend, &out->datagram.backend);
  if (rc == SALTS_OK) {
    CNET_TYPED_FILL_DATAGRAM(out, typed);
    rc = backend_kind(typed->backend, &out->datagram.backend);
  }
  if (rc == SALTS_OK) rc = datagram_validate(out, deployment);
  if (rc == SALTS_OK) rc = parse_peer(deployment, &out->peer);
  out->peer_host = deployment->peer_host;
  CNET_TYPED_SINK_TAIL(out, typed);
  if (rc == SALTS_OK) rc = build_sink_content(&out->content);
  if (rc == SALTS_OK &&
      out->max_message_bytes > out->datagram.max_datagram_bytes)
    rc = SALTS_ERANGE;
  return rc == SALTS_OK
             ? SALTS_OK
             : typed_fail(error, rc, instance_name, "policy",
                          "inconsistent CNet datagram sink policy/resource");
}

int cnet_typed_packet_sink_config(
    const CNetPacketSinkConfig_t *typed,
    const turbo_flow_cnet_deployment_view_t *deployment,
    const char *instance_name, cnet_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error) {
  int rc;
  if (!typed || !deployment || !instance_name || !instance_name[0] || !out)
    return typed_fail(error, SALTS_EINVAL, instance_name, NULL,
                      "invalid CNet packet sink arguments");
  if (typed->schema_version != 2u)
    return typed_fail(error, SALTS_ENOTSUP, instance_name, "schema_version",
                      "unsupported CNet packet sink schema");
  memset(out, 0, sizeof(*out));
  out->kind = CNET_TYPED_PACKET_SINK;
  rc = packet_common(
      out, deployment, typed->backend, typed->packet_mode,
      typed->datagram_send_capacity, typed->request_capacity,
      typed->completion_batch_capacity, typed->max_datagram_bytes,
      typed->receive_buffer_bytes, typed->reuse_port,
      typed->session_capacity, typed->kcp_mtu,
      typed->kcp_send_window, typed->kcp_receive_window,
      typed->kcp_interval_ms, typed->kcp_fast_resend,
      typed->kcp_no_congestion_window, typed->kcp_stream_mode,
      typed->kcp_send_segment_capacity, typed->kcp_max_message_bytes,
      typed->security_mode, typed->handshake_retry_ms,
      typed->fec_backend, typed->fec_data_shards,
      typed->fec_parity_shards, typed->fec_max_payload_bytes,
      typed->fec_receive_group_count);
  if (rc == SALTS_OK) rc = parse_peer(deployment, &out->peer);
  out->peer_host = deployment->peer_host;
  out->conversation = typed->conversation;
  out->adapter_send_capacity = typed->adapter_send_capacity;
  CNET_TYPED_SINK_TAIL(out, typed);
  if (rc == SALTS_OK) rc = build_sink_content(&out->content);
  if (rc == SALTS_OK && out->endpoint.protocol == CNET_PACKET_UDP) {
    if (out->conversation != 0u ||
        out->adapter_send_capacity > out->endpoint.datagram.send_capacity)
      rc = SALTS_EINVAL;
  } else if (rc == SALTS_OK) {
    if ((out->endpoint.security.mode == CNET_KCP_SECURITY_NONE &&
         out->conversation == 0u) ||
        (out->endpoint.security.mode != CNET_KCP_SECURITY_NONE &&
         out->conversation != 0u))
      rc = SALTS_EINVAL;
  }
  if (rc == SALTS_OK &&
      out->max_message_bytes >
          (out->endpoint.protocol == CNET_PACKET_UDP
               ? out->endpoint.datagram.max_datagram_bytes
               : out->endpoint.kcp.max_message_bytes))
    rc = SALTS_ERANGE;
  return rc == SALTS_OK
             ? SALTS_OK
             : typed_fail(error, rc, instance_name, "policy",
                          "inconsistent CNet packet sink policy/resource");
}

#undef CNET_TYPED_PACKET_COMMON
#undef CNET_TYPED_SINK_TAIL
#undef CNET_TYPED_SOURCE_TAIL
#undef CNET_TYPED_FILL_DATAGRAM
#undef CNET_TYPED_FILL_SOCKET
#undef CNET_TYPED_FILL_CLIENT
