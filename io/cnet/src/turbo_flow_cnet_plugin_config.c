#include "turbo_flow_cnet_plugin_internal.h"

#include <uri_parser.h>

#include <limits.h>
#include <stdio.h>
#include <string.h>

#if defined(_WIN32)
  #include <winsock2.h>
  #include <ws2tcpip.h>
#else
  #include <arpa/inet.h>
  #include <netinet/tcp.h>
#endif

#define ARRAY_COUNT(values) (sizeof(values) / sizeof((values)[0]))

enum {
  CNET_PLUGIN_KCP_MIN_MTU = 50,
  CNET_PLUGIN_SECURE_KCP_MIN_MTU = 576,
  CNET_PLUGIN_KCP_MIN_INTERVAL_MS = 10,
  CNET_PLUGIN_KCP_MAX_INTERVAL_MS = 5000,
  CNET_PLUGIN_FEC_MAX_RECEIVE_GROUPS = 64,
  CNET_PLUGIN_FEC_MAX_TOTAL_SHARDS = 255,
  CNET_PLUGIN_FEC_MAX_STATE_BYTES = 64 * 1024 * 1024,
  CNET_PLUGIN_SECURE_FEC_WIRE_OVERHEAD_BYTES = 46,
  CNET_PLUGIN_URI_HOST_CAPACITY = 254,
  CNET_PLUGIN_TLS_SERVER_NAME_MAX_BYTES = 253,
  CNET_PLUGIN_CLIENT_COMMAND_PAYLOAD_MIN_BYTES = 2048,
  CNET_PLUGIN_CLIENT_QUEUE_ENTRY_ACCOUNTING_BYTES = 2048
};

static const char *const stream_source_fields[] = {"schema_version",
                                                   "backend",
                                                   "uri",
                                                   "connection_capacity",
                                                   "command_capacity",
                                                   "request_capacity",
                                                   "completion_batch_capacity",
                                                   "event_capacity",
                                                   "max_send_bytes",
                                                   "receive_buffer_bytes",
                                                   "connect_timeout_ms",
                                                   "read_timeout_ms",
                                                   "write_timeout_ms",
                                                   "tls_io_buffer_bytes",
                                                   "tls_handshake_timeout_ms",
                                                   "command_buffer_bytes",
                                                   "event_buffer_bytes",
                                                   "socket_receive_buffer_bytes",
                                                   "socket_send_buffer_bytes",
                                                   "keepalive",
                                                   "keepalive_idle_ms",
                                                   "keepalive_interval_ms",
                                                   "keepalive_count",
                                                   "linger",
                                                   "linger_ms",
                                                   "tls_ca_file",
                                                   "tls_ca_path",
                                                   "tls_cert_file",
                                                   "tls_key_file",
                                                   "tls_key_password",
                                                   "tls_server_name",
                                                   "tls_alpn",
                                                   "max_message_bytes",
                                                   "scheduler_capacity",
                                                   "scheduler_max_steps_per_poll",
                                                   "first_message_id",
                                                   "initial_demand",
                                                   "stop_timeout_ms"};

static const char *const stream_sink_fields[] = {"schema_version",
                                                 "backend",
                                                 "uri",
                                                 "connection_capacity",
                                                 "command_capacity",
                                                 "request_capacity",
                                                 "completion_batch_capacity",
                                                 "event_capacity",
                                                 "max_send_bytes",
                                                 "receive_buffer_bytes",
                                                 "connect_timeout_ms",
                                                 "read_timeout_ms",
                                                 "write_timeout_ms",
                                                 "tls_io_buffer_bytes",
                                                 "tls_handshake_timeout_ms",
                                                 "command_buffer_bytes",
                                                 "event_buffer_bytes",
                                                 "socket_receive_buffer_bytes",
                                                 "socket_send_buffer_bytes",
                                                 "keepalive",
                                                 "keepalive_idle_ms",
                                                 "keepalive_interval_ms",
                                                 "keepalive_count",
                                                 "linger",
                                                 "linger_ms",
                                                 "tls_ca_file",
                                                 "tls_ca_path",
                                                 "tls_cert_file",
                                                 "tls_key_file",
                                                 "tls_key_password",
                                                 "tls_server_name",
                                                 "tls_alpn",
                                                 "max_message_bytes",
                                                 "actor_command_capacity",
                                                 "actor_max_steps_per_poll",
                                                 "stop_timeout_ms"};

static const char *const listener_source_fields[] = {"schema_version",
                                                     "backend",
                                                     "bind_host",
                                                     "bind_port",
                                                     "backlog",
                                                     "reuse_port",
                                                     "connection_capacity",
                                                     "command_capacity",
                                                     "request_capacity",
                                                     "completion_batch_capacity",
                                                     "event_capacity",
                                                     "max_send_bytes",
                                                     "receive_buffer_bytes",
                                                     "connect_timeout_ms",
                                                     "read_timeout_ms",
                                                     "write_timeout_ms",
                                                     "tls_io_buffer_bytes",
                                                     "tls_handshake_timeout_ms",
                                                     "command_buffer_bytes",
                                                     "event_buffer_bytes",
                                                     "socket_receive_buffer_bytes",
                                                     "socket_send_buffer_bytes",
                                                     "keepalive",
                                                     "keepalive_idle_ms",
                                                     "keepalive_interval_ms",
                                                     "keepalive_count",
                                                     "linger",
                                                     "linger_ms",
                                                     "tls_enabled",
                                                     "tls_ca_file",
                                                     "tls_ca_path",
                                                     "tls_cert_file",
                                                     "tls_key_file",
                                                     "tls_key_password",
                                                     "tls_client_auth",
                                                     "tls_alpn",
                                                     "max_connections",
                                                     "max_message_bytes",
                                                     "scheduler_capacity",
                                                     "scheduler_max_steps_per_poll",
                                                     "first_message_id",
                                                     "initial_demand",
                                                     "stop_timeout_ms"};

static const char *const datagram_sink_fields[] = {"schema_version",
                                                   "backend",
                                                   "bind_host",
                                                   "bind_port",
                                                   "datagram_send_capacity",
                                                   "request_capacity",
                                                   "completion_batch_capacity",
                                                   "max_datagram_bytes",
                                                   "receive_buffer_bytes",
                                                   "reuse_port",
                                                   "peer_host",
                                                   "peer_port",
                                                   "peer_scope_id",
                                                   "max_message_bytes",
                                                   "actor_command_capacity",
                                                   "actor_max_steps_per_poll",
                                                   "stop_timeout_ms"};

static const char *const packet_common_fields[] = {"schema_version",
                                                   "backend",
                                                   "packet_mode",
                                                   "bind_host",
                                                   "bind_port",
                                                   "datagram_send_capacity",
                                                   "request_capacity",
                                                   "completion_batch_capacity",
                                                   "max_datagram_bytes",
                                                   "receive_buffer_bytes",
                                                   "reuse_port",
                                                   "session_capacity",
                                                   "kcp_mtu",
                                                   "kcp_send_window",
                                                   "kcp_receive_window",
                                                   "kcp_interval_ms",
                                                   "kcp_fast_resend",
                                                   "kcp_no_congestion_window",
                                                   "kcp_stream_mode",
                                                   "kcp_send_segment_capacity",
                                                   "kcp_max_message_bytes",
                                                   "security_mode",
                                                   "psk_hex",
                                                   "handshake_retry_ms",
                                                   "fec_backend",
                                                   "fec_data_shards",
                                                   "fec_parity_shards",
                                                   "fec_max_payload_bytes",
                                                   "fec_receive_group_count"};

static const char *const packet_source_tail[] = {
    "queue_capacity",   "max_message_bytes", "scheduler_capacity", "scheduler_max_steps_per_poll",
    "first_message_id", "initial_demand",    "stop_timeout_ms"};

static const char *const source_content_fields[] = {
    "content_encoding", "content_media_type", "content_schema",
    "content_type", "content_schema_version"};

static const char *const packet_sink_tail[] = {"peer_host",
                                               "peer_port",
                                               "peer_scope_id",
                                               "conversation",
                                               "adapter_send_capacity",
                                               "max_message_bytes",
                                               "actor_command_capacity",
                                               "actor_max_steps_per_poll",
                                               "stop_timeout_ms"};

const char *turbo_flow_cnet_plugin_kind_name(turbo_flow_cnet_plugin_kind_t kind) {
  static const char *const names[] = {"cnet.stream_source", "cnet.listener_source",
                                      "cnet.packet_source", "cnet.stream_sink",
                                      "cnet.datagram_sink", "cnet.packet_sink"};
  return kind < TURBO_FLOW_CNET_PLUGIN_KIND_COUNT ? names[kind] : NULL;
}

static int config_error(turbo_flow_config_error_t *error, int status, const char *name,
                        const char *field, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.config%s%s", name,
                   field ? "." : "", field ? field : "");
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int field_in(const char *field, const char *const *fields, size_t count) {
  for (size_t i = 0u; i < count; ++i)
    if (strcmp(field, fields[i]) == 0) return 1;
  return 0;
}

static int field_present(const turbo_flow_resolved_adapter_view_t *view,
                         const char *field) {
  const size_t actual = turbo_flow_resolved_adapter_field_count(view);
  for (size_t i = 0u; i < actual; ++i) {
    const char *candidate = turbo_flow_resolved_adapter_field_name(view, i);
    if (candidate && strcmp(candidate, field) == 0) return 1;
  }
  return 0;
}

static int exact_fields_optional(
    const turbo_flow_resolved_adapter_view_t *view, const char *name,
    const char *const *head, size_t head_count, const char *const *tail,
    size_t tail_count, const char *const *optional, size_t optional_count,
    turbo_flow_config_error_t *error) {
  const size_t actual = turbo_flow_resolved_adapter_field_count(view);
  size_t optional_seen = 0u;
  for (size_t i = 0u; i < actual; ++i) {
    const char *field = turbo_flow_resolved_adapter_field_name(view, i);
    if (!field)
      return config_error(error, SALTS_EINVAL, name, NULL,
                          "CNet adapter config contains an invalid field");
    if (field_in(field, optional, optional_count)) {
      ++optional_seen;
      continue;
    }
    if (!field_in(field, head, head_count) && !field_in(field, tail, tail_count))
      return config_error(error, SALTS_EINVAL, name, field, "unknown CNet adapter config field");
  }
  for (size_t i = 0u; i < head_count; ++i)
    if (!field_present(view, head[i]))
      return config_error(error, SALTS_EINVAL, name, head[i],
                          "CNet adapter config has a missing field");
  for (size_t i = 0u; i < tail_count; ++i)
    if (!field_present(view, tail[i]))
      return config_error(error, SALTS_EINVAL, name, tail[i],
                          "CNet adapter config has a missing field");
  if (optional_seen != 0u && optional_seen != optional_count)
    return config_error(error, SALTS_EINVAL, name, NULL,
                        "CNet canonical content fields must be supplied as one complete group");
  if (actual != head_count + tail_count + optional_seen)
    return config_error(error, SALTS_EINVAL, name, NULL,
                        "CNet adapter config has duplicate or missing fields");
  return SALTS_OK;
}

static int exact_fields(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                        const char *const *head, size_t head_count, const char *const *tail,
                        size_t tail_count, turbo_flow_config_error_t *error) {
  return exact_fields_optional(view, name, head, head_count, tail, tail_count,
                               NULL, 0u, error);
}

static int get_u64(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                   const char *field, uint64_t max_value, int allow_zero, uint64_t *value,
                   turbo_flow_config_error_t *error) {
  int rc = turbo_flow_resolved_adapter_get_u64(view, field, value);
  if (rc != SALTS_OK || (!allow_zero && *value == 0u) || *value > max_value)
    return config_error(error, rc == SALTS_OK ? SALTS_ERANGE : rc, name, field,
                        "expected an in-range unsigned integer");
  return SALTS_OK;
}

static int get_size(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                    const char *field, int allow_zero, size_t *value,
                    turbo_flow_config_error_t *error) {
  uint64_t raw = 0u;
  int rc = get_u64(view, name, field, (uint64_t)SIZE_MAX, allow_zero, &raw, error);
  if (rc == SALTS_OK) *value = (size_t)raw;
  return rc;
}

static int get_u32(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                   const char *field, int allow_zero, uint32_t *value,
                   turbo_flow_config_error_t *error) {
  uint64_t raw = 0u;
  int rc = get_u64(view, name, field, UINT32_MAX, allow_zero, &raw, error);
  if (rc == SALTS_OK) *value = (uint32_t)raw;
  return rc;
}

static int get_u16(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                   const char *field, int allow_zero, uint16_t *value,
                   turbo_flow_config_error_t *error) {
  uint64_t raw = 0u;
  int rc = get_u64(view, name, field, UINT16_MAX, allow_zero, &raw, error);
  if (rc == SALTS_OK) *value = (uint16_t)raw;
  return rc;
}

static int get_bool(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                    const char *field, int *value, turbo_flow_config_error_t *error) {
  int rc = turbo_flow_resolved_adapter_get_bool(view, field, value);
  return rc == SALTS_OK ? SALTS_OK : config_error(error, rc, name, field, "expected a boolean");
}

static int get_text(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                    const char *field, char *out, size_t capacity, int allow_empty,
                    turbo_flow_config_error_t *error) {
  const char *value = NULL;
  int rc = turbo_flow_resolved_adapter_get_string(view, field, &value);
  if (rc != SALTS_OK || !value || (!allow_empty && !value[0]) || strlen(value) >= capacity)
    return config_error(error, rc == SALTS_OK ? SALTS_ERANGE : rc, name, field,
                        "expected a bounded string");
  memcpy(out, value, strlen(value) + 1u);
  return SALTS_OK;
}

static const char *optional_text(char *text) { return text && text[0] ? text : NULL; }

static int source_content_encoding(const char *value,
                                   turbo_flow_data_encoding_t *encoding) {
  if (!value || !encoding) return SALTS_EINVAL;
  if (strcmp(value, "tbe") == 0) *encoding = TURBO_FLOW_DATA_ENCODING_TBE;
  else if (strcmp(value, "json") == 0) *encoding = TURBO_FLOW_DATA_ENCODING_JSON;
  else if (strcmp(value, "csv") == 0) *encoding = TURBO_FLOW_DATA_ENCODING_CSV;
  else if (strcmp(value, "xml") == 0) *encoding = TURBO_FLOW_DATA_ENCODING_XML;
  else if (strcmp(value, "utf8") == 0) *encoding = TURBO_FLOW_DATA_ENCODING_UTF8;
  else if (strcmp(value, "opaque") == 0) *encoding = TURBO_FLOW_DATA_ENCODING_OPAQUE;
  else return SALTS_EINVAL;
  return SALTS_OK;
}

static int build_content_descriptor(
    const turbo_flow_resolved_adapter_view_t *view, const char *name, int source,
    turbo_flow_cnet_plugin_config_t *config, turbo_flow_config_error_t *error) {
  char encoding_name[16];
  char media_type[TURBO_FLOW_CONTENT_MEDIA_TYPE_MAX + 1u];
  char schema_name[TURBO_FLOW_CONTENT_SCHEMA_NAME_MAX + 1u];
  char type_name[TURBO_FLOW_CONTENT_TYPE_NAME_MAX + 1u];
  turbo_flow_data_encoding_t encoding = TURBO_FLOW_DATA_ENCODING_OPAQUE;
  uint32_t schema_version = 0u;
  int rc;

  if (!source || !field_present(view, "content_encoding")) {
    rc = turbo_flow_content_descriptor_init(
        &config->content, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
        TURBO_FLOW_CONTENT_PROFILE_GENERIC, TURBO_FLOW_DATA_ENCODING_OPAQUE,
        "application/octet-stream", "cnet.payload");
    if (rc == SALTS_OK)
      rc = turbo_flow_content_descriptor_declare_schema(
          &config->content, "CNetPayload", "Bytes", 1u);
    return rc == SALTS_OK
               ? SALTS_OK
               : config_error(error, rc, name, NULL,
                              "failed to build CNet transport content descriptor");
  }

  rc = get_text(view, name, "content_encoding", encoding_name,
                sizeof(encoding_name), 0, error);
  if (rc == SALTS_OK) rc = source_content_encoding(encoding_name, &encoding);
  if (rc != SALTS_OK)
    return config_error(error, rc, name, "content_encoding",
                        "content encoding must be tbe, json, csv, xml, utf8, or opaque");
  rc = get_text(view, name, "content_media_type", media_type,
                sizeof(media_type), 0, error);
  if (rc == SALTS_OK)
    rc = get_text(view, name, "content_schema", schema_name,
                  sizeof(schema_name), 0, error);
  if (rc == SALTS_OK)
    rc = get_text(view, name, "content_type", type_name,
                  sizeof(type_name), 0, error);
  if (rc == SALTS_OK)
    rc = get_u32(view, name, "content_schema_version", 0, &schema_version, error);
  if (rc != SALTS_OK) return rc;

  rc = turbo_flow_content_descriptor_init(
      &config->content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
      encoding, media_type, "cnet.business");
  if (rc == SALTS_OK)
    rc = turbo_flow_content_descriptor_declare_schema(
        &config->content, schema_name, type_name, schema_version);
  return rc == SALTS_OK
             ? SALTS_OK
             : config_error(error, rc, name, NULL,
                            "failed to build canonical CNet business content descriptor");
}

static int get_backend(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                       native_io_backend_kind *backend, turbo_flow_config_error_t *error) {
  char value[32];
  int rc = get_text(view, name, "backend", value, sizeof(value), 0, error);
  if (rc != SALTS_OK) return rc;
  if (strcmp(value, "iocp") == 0) *backend = NATIVE_IO_BACKEND_IOCP;
  else if (strcmp(value, "epoll") == 0) *backend = NATIVE_IO_BACKEND_EPOLL;
  else if (strcmp(value, "io_uring") == 0) *backend = NATIVE_IO_BACKEND_IO_URING;
  else if (strcmp(value, "kqueue") == 0) *backend = NATIVE_IO_BACKEND_KQUEUE;
  else
    return config_error(error, SALTS_EINVAL, name, "backend",
                        "backend must be iocp, epoll, io_uring, or kqueue");
  if (!native_io_backend_kind_supported(*backend))
    return config_error(error, SALTS_ENOTSUP, name, "backend",
                        "configured CNet backend is unavailable on this platform");
  return SALTS_OK;
}

static int get_alpn(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                    turbo_flow_cnet_plugin_config_t *config, size_t *count,
                    turbo_flow_config_error_t *error) {
  int rc = turbo_flow_resolved_adapter_array_size(view, "tls_alpn", count);
  if (rc != SALTS_OK || *count > TURBO_FLOW_CNET_PLUGIN_ALPN_CAPACITY)
    return config_error(error, rc == SALTS_OK ? SALTS_ENOSPC : rc, name, "tls_alpn",
                        "expected a bounded string array");
  for (size_t i = 0u; i < *count; ++i) {
    const char *value = NULL;
    rc = turbo_flow_resolved_adapter_array_string_at(view, "tls_alpn", i, &value);
    if (rc != SALTS_OK || !value || !value[0] || strlen(value) >= sizeof(config->alpn_storage[i]))
      return config_error(error, rc == SALTS_OK ? SALTS_ERANGE : rc, name, "tls_alpn",
                          "ALPN names must be non-empty bounded strings");
    memcpy(config->alpn_storage[i], value, strlen(value) + 1u);
    config->alpn[i] = config->alpn_storage[i];
  }
  return SALTS_OK;
}

static int power_of_two(size_t value) { return value && (value & (value - 1u)) == 0u; }

static int read_client(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                       turbo_flow_cnet_plugin_config_t *config, turbo_flow_config_error_t *error) {
  cnet_client_config *client = &config->client;
  size_t max_command_payload_bytes;
  size_t max_event_payload_bytes;
  int rc = get_backend(view, name, &client->backend, error);
#define READ_SIZE(field, zero)                                                                     \
  if (rc == SALTS_OK) rc = get_size(view, name, #field, zero, &client->field, error)
#define READ_U32(field, zero)                                                                      \
  if (rc == SALTS_OK) rc = get_u32(view, name, #field, zero, &client->field, error)
  READ_SIZE(connection_capacity, 0);
  READ_SIZE(command_capacity, 0);
  READ_SIZE(request_capacity, 0);
  READ_SIZE(completion_batch_capacity, 0);
  READ_SIZE(event_capacity, 0);
  READ_SIZE(max_send_bytes, 0);
  READ_SIZE(receive_buffer_bytes, 0);
  READ_U32(connect_timeout_ms, 1);
  READ_U32(read_timeout_ms, 1);
  READ_U32(write_timeout_ms, 1);
  READ_SIZE(tls_io_buffer_bytes, 1);
  READ_U32(tls_handshake_timeout_ms, 1);
  READ_SIZE(command_buffer_bytes, 1);
  READ_SIZE(event_buffer_bytes, 1);
#undef READ_U32
#undef READ_SIZE
  if (rc != SALTS_OK) return rc;
  if (!power_of_two(client->command_capacity) || !power_of_two(client->event_capacity) ||
      client->event_capacity < 2u || client->completion_batch_capacity > client->request_capacity ||
      client->connection_capacity > UINT32_MAX / 2u || client->request_capacity > UINT32_MAX ||
      (client->command_buffer_bytes && client->command_buffer_bytes < client->max_send_bytes) ||
      (client->event_buffer_bytes && client->event_buffer_bytes < client->receive_buffer_bytes) ||
      ((client->tls_io_buffer_bytes == 0u) != (client->tls_handshake_timeout_ms == 0u)) ||
      (client->tls_io_buffer_bytes && (client->tls_io_buffer_bytes < CNET_TLS_MIN_IO_BUFFER_BYTES ||
                                       client->tls_io_buffer_bytes > INT_MAX)))
    return config_error(error, SALTS_EINVAL, name, NULL, "invalid bounded CNet client policy");
  if (client->command_capacity > UINT32_MAX)
    return config_error(error, SALTS_ERANGE, name, "command_capacity",
                        "command queue capacity exceeds the CNet index bound");
  max_command_payload_bytes = client->max_send_bytes > CNET_PLUGIN_CLIENT_COMMAND_PAYLOAD_MIN_BYTES
                                  ? client->max_send_bytes
                                  : CNET_PLUGIN_CLIENT_COMMAND_PAYLOAD_MIN_BYTES;
  max_event_payload_bytes = client->receive_buffer_bytes > CNET_TLS_ALPN_NAME_MAX_BYTES
                                ? client->receive_buffer_bytes
                                : CNET_TLS_ALPN_NAME_MAX_BYTES;
  if (client->command_capacity > SIZE_MAX / CNET_PLUGIN_CLIENT_QUEUE_ENTRY_ACCOUNTING_BYTES ||
      (client->command_buffer_bytes == 0u &&
       client->command_capacity > SIZE_MAX / max_command_payload_bytes))
    return config_error(error, SALTS_ERANGE, name, "command_capacity",
                        "command queue storage exceeds the addressable bound");
  if (client->event_capacity > SIZE_MAX / CNET_PLUGIN_CLIENT_QUEUE_ENTRY_ACCOUNTING_BYTES ||
      (client->event_buffer_bytes == 0u &&
       client->event_capacity > SIZE_MAX / max_event_payload_bytes))
    return config_error(error, SALTS_ERANGE, name, "event_capacity",
                        "event queue storage exceeds the addressable bound");
  if (client->command_buffer_bytes &&
      client->command_buffer_bytes < CNET_PLUGIN_CLIENT_COMMAND_PAYLOAD_MIN_BYTES)
    return config_error(error, SALTS_ERANGE, name, "command_buffer_bytes",
                        "explicit command buffer cannot hold a CNet connect command");
  if (client->event_buffer_bytes && client->tls_io_buffer_bytes &&
      client->event_buffer_bytes < CNET_TLS_ALPN_NAME_MAX_BYTES)
    return config_error(error, SALTS_ERANGE, name, "event_buffer_bytes",
                        "explicit event buffer cannot hold CNet TLS state");
  return SALTS_OK;
}

static int read_socket_options(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                               turbo_flow_cnet_plugin_config_t *config,
                               turbo_flow_config_error_t *error) {
  cnet_stream_socket_options *socket = &config->socket_options;
  int rc;
  *socket = (cnet_stream_socket_options)CNET_STREAM_SOCKET_OPTIONS_INIT;
  rc = get_size(view, name, "socket_receive_buffer_bytes", 1, &socket->receive_buffer_bytes, error);
  if (rc == SALTS_OK)
    rc = get_size(view, name, "socket_send_buffer_bytes", 1, &socket->send_buffer_bytes, error);
  if (rc == SALTS_OK) rc = get_bool(view, name, "keepalive", &socket->keepalive, error);
  if (rc == SALTS_OK)
    rc = get_u32(view, name, "keepalive_idle_ms", 1, &socket->keepalive_idle_ms, error);
  if (rc == SALTS_OK)
    rc = get_u32(view, name, "keepalive_interval_ms", 1, &socket->keepalive_interval_ms, error);
  if (rc == SALTS_OK)
    rc = get_u32(view, name, "keepalive_count", 1, &socket->keepalive_count, error);
  if (rc == SALTS_OK) rc = get_bool(view, name, "linger", &socket->linger, error);
  if (rc == SALTS_OK) rc = get_u32(view, name, "linger_ms", 1, &socket->linger_ms, error);
  if (rc == SALTS_OK) rc = cnet_stream_socket_options_validate(socket);
  if (rc != SALTS_OK) return config_error(error, rc, name, NULL, "invalid stream socket policy");
#if defined(_WIN32)
  if (socket->keepalive_count != 0u)
    return config_error(error, SALTS_ENOTSUP, name, "keepalive_count",
                        "keepalive count is unavailable on this platform");
#else
  #if !defined(TCP_KEEPIDLE) && !defined(TCP_KEEPALIVE)
  if (socket->keepalive_idle_ms != 0u)
    return config_error(error, SALTS_ENOTSUP, name, "keepalive_idle_ms",
                        "keepalive idle is unavailable on this platform");
  #endif
  #if !defined(TCP_KEEPINTVL)
  if (socket->keepalive_interval_ms != 0u)
    return config_error(error, SALTS_ENOTSUP, name, "keepalive_interval_ms",
                        "keepalive interval is unavailable on this platform");
  #endif
  #if !defined(TCP_KEEPCNT)
  if (socket->keepalive_count != 0u)
    return config_error(error, SALTS_ENOTSUP, name, "keepalive_count",
                        "keepalive count is unavailable on this platform");
  #endif
#endif
  return SALTS_OK;
}

static int read_tls_strings(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                            turbo_flow_cnet_plugin_config_t *config, size_t *alpn_count,
                            turbo_flow_config_error_t *error) {
  int rc = get_text(view, name, "tls_ca_file", config->tls_ca_file, sizeof(config->tls_ca_file), 1,
                    error);
#define READ_TLS(field, member)                                                                    \
  if (rc == SALTS_OK)                                                                              \
  rc = get_text(view, name, field, config->member, sizeof(config->member), 1, error)
  READ_TLS("tls_ca_path", tls_ca_path);
  READ_TLS("tls_cert_file", tls_cert_file);
  READ_TLS("tls_key_file", tls_key_file);
  READ_TLS("tls_key_password", tls_key_password);
#undef READ_TLS
  if (rc == SALTS_OK) rc = get_alpn(view, name, config, alpn_count, error);
  if (rc != SALTS_OK) return rc;
  if ((config->tls_cert_file[0] == '\0') != (config->tls_key_file[0] == '\0'))
    return config_error(error, SALTS_EINVAL, name, "tls_cert_file",
                        "TLS certificate and key must be configured together");
  if (config->tls_key_password[0] && !config->tls_key_file[0])
    return config_error(error, SALTS_EINVAL, name, "tls_key_password",
                        "TLS key password requires certificate and key material");
  return SALTS_OK;
}

static int read_stream_tls(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                           turbo_flow_cnet_plugin_config_t *config,
                           turbo_flow_config_error_t *error) {
  size_t alpn_count = 0u;
  const int tls_uri = strncmp(config->uri, "tls://", 6u) == 0;
  int rc = read_tls_strings(view, name, config, &alpn_count, error);
  if (rc == SALTS_OK)
    rc = get_text(view, name, "tls_server_name", config->tls_server_name,
                  sizeof(config->tls_server_name), 1, error);
  if (rc != SALTS_OK) return rc;
  if (!tls_uri && (config->client.tls_io_buffer_bytes || config->tls_ca_file[0] ||
                   config->tls_ca_path[0] || config->tls_cert_file[0] || config->tls_key_file[0] ||
                   config->tls_key_password[0] || config->tls_server_name[0] || alpn_count))
    return config_error(error, SALTS_EINVAL, name, "uri",
                        "TLS fields are forbidden for a non-TLS URI");
  if (tls_uri && config->client.tls_io_buffer_bytes == 0u)
    return config_error(error, SALTS_EINVAL, name, "tls_io_buffer_bytes",
                        "TLS URI requires explicit TLS storage and handshake timeout");
  if (config->tls_server_name[0] &&
      strlen(config->tls_server_name) > CNET_PLUGIN_TLS_SERVER_NAME_MAX_BYTES)
    return config_error(error, SALTS_ERANGE, name, "tls_server_name",
                        "TLS server name exceeds the CNet bound");
  config->tls_client.size = sizeof(config->tls_client);
  config->tls_client.ca_file = optional_text(config->tls_ca_file);
  config->tls_client.ca_path = optional_text(config->tls_ca_path);
  config->tls_client.cert_file = optional_text(config->tls_cert_file);
  config->tls_client.key_file = optional_text(config->tls_key_file);
  config->tls_client.key_password = optional_text(config->tls_key_password);
  config->tls_client.server_name = optional_text(config->tls_server_name);
  config->tls_client.alpn_protocols = alpn_count ? config->alpn : NULL;
  config->tls_client.alpn_protocol_count = alpn_count;
  config->tls_enabled = tls_uri;
  return SALTS_OK;
}

static int parse_peer(const char *host, uint16_t port, uint32_t scope_id,
                      cnet_datagram_peer *peer) {
  unsigned char bytes[16] = {0};
  if (!host || !host[0] || !peer || port == 0u) return SALTS_EINVAL;
#if defined(_WIN32)
  if (InetPtonA(AF_INET, host, bytes) == 1) {
#else
  if (inet_pton(AF_INET, host, bytes) == 1) {
#endif
    peer->family = CNET_DATAGRAM_ADDRESS_IPV4;
    memcpy(peer->address, bytes, 4u);
  } else {
#if defined(_WIN32)
    if (InetPtonA(AF_INET6, host, bytes) != 1) return SALTS_EINVAL;
#else
    if (inet_pton(AF_INET6, host, bytes) != 1) return SALTS_EINVAL;
#endif
    peer->family = CNET_DATAGRAM_ADDRESS_IPV6;
    memcpy(peer->address, bytes, 16u);
  }
  peer->port = port;
  peer->scope_id = scope_id;
  return SALTS_OK;
}

static int validate_bind_host(const char *host) {
  unsigned char bytes[16] = {0};
  if (!host || !host[0]) return SALTS_EINVAL;
#if defined(_WIN32)
  return InetPtonA(AF_INET, host, bytes) == 1 || InetPtonA(AF_INET6, host, bytes) == 1
             ? SALTS_OK
             : SALTS_EINVAL;
#else
  return inet_pton(AF_INET, host, bytes) == 1 || inet_pton(AF_INET6, host, bytes) == 1
             ? SALTS_OK
             : SALTS_EINVAL;
#endif
}

static int read_datagram(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                         turbo_flow_cnet_plugin_config_t *config,
                         turbo_flow_config_error_t *error) {
  cnet_datagram_config *datagram = &config->datagram;
  int reuse_port = 0;
  int rc;
  *datagram = (cnet_datagram_config)CNET_DATAGRAM_CONFIG_INIT;
  rc = get_backend(view, name, &datagram->backend, error);
  if (rc == SALTS_OK)
    rc = get_text(view, name, "bind_host", config->bind_host, sizeof(config->bind_host), 0, error);
  if (rc == SALTS_OK) rc = get_u16(view, name, "bind_port", 1, &datagram->port, error);
  if (rc == SALTS_OK)
    rc = get_size(view, name, "datagram_send_capacity", 0, &datagram->send_capacity, error);
  if (rc == SALTS_OK)
    rc = get_size(view, name, "request_capacity", 0, &datagram->request_capacity, error);
  if (rc == SALTS_OK)
    rc = get_size(view, name, "completion_batch_capacity", 0, &datagram->completion_batch_capacity,
                  error);
  if (rc == SALTS_OK)
    rc = get_size(view, name, "max_datagram_bytes", 0, &datagram->max_datagram_bytes, error);
  if (rc == SALTS_OK)
    rc = get_size(view, name, "receive_buffer_bytes", 0, &datagram->receive_buffer_bytes, error);
  if (rc == SALTS_OK) rc = get_bool(view, name, "reuse_port", &reuse_port, error);
  if (rc != SALTS_OK) return rc;
  if (validate_bind_host(config->bind_host) != SALTS_OK)
    return config_error(error, SALTS_EINVAL, name, "bind_host",
                        "bind host must be a numeric IP address");
  datagram->host = config->bind_host;
  datagram->reuse_port = reuse_port;
  if (datagram->request_capacity <= datagram->send_capacity ||
      datagram->completion_batch_capacity > datagram->request_capacity ||
      datagram->send_capacity > UINT32_MAX || datagram->request_capacity > UINT32_MAX ||
      datagram->max_datagram_bytes > CNET_DATAGRAM_MAX_PAYLOAD_BYTES ||
      datagram->receive_buffer_bytes < datagram->max_datagram_bytes ||
      datagram->receive_buffer_bytes > CNET_DATAGRAM_MAX_PAYLOAD_BYTES ||
      datagram->send_capacity > SIZE_MAX / datagram->max_datagram_bytes)
    return config_error(error, SALTS_EINVAL, name, NULL, "invalid bounded datagram policy");
#if !defined(SO_REUSEPORT)
  if (reuse_port)
    return config_error(error, SALTS_ENOTSUP, name, "reuse_port",
                        "SO_REUSEPORT is unavailable on this platform");
#endif
  return SALTS_OK;
}

static int read_common_source_tail(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                                   turbo_flow_cnet_plugin_config_t *config,
                                   turbo_flow_config_error_t *error) {
  int rc = get_size(view, name, "max_message_bytes", 0, &config->max_message_bytes, error);
  if (rc == SALTS_OK)
    rc = get_size(view, name, "scheduler_capacity", 0, &config->scheduler_capacity, error);
  if (rc == SALTS_OK)
    rc = get_size(view, name, "scheduler_max_steps_per_poll", 0,
                  &config->scheduler_max_steps_per_poll, error);
  if (rc == SALTS_OK)
    rc = get_u64(view, name, "first_message_id", UINT64_MAX, 0, &config->first_message_id, error);
  if (rc == SALTS_OK)
    rc = get_size(view, name, "initial_demand", 0, &config->initial_demand, error);
  if (rc == SALTS_OK)
    rc = get_u32(view, name, "stop_timeout_ms", 0, &config->stop_timeout_ms, error);
  return rc;
}

static int read_common_sink_tail(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                                 turbo_flow_cnet_plugin_config_t *config,
                                 turbo_flow_config_error_t *error) {
  int rc = get_size(view, name, "max_message_bytes", 0, &config->max_message_bytes, error);
  if (rc == SALTS_OK)
    rc = get_size(view, name, "actor_command_capacity", 0, &config->actor_command_capacity, error);
  if (rc == SALTS_OK)
    rc = get_size(view, name, "actor_max_steps_per_poll", 0, &config->actor_max_steps_per_poll,
                  error);
  if (rc == SALTS_OK)
    rc = get_u32(view, name, "stop_timeout_ms", 0, &config->stop_timeout_ms, error);
  return rc;
}

static int validate_pipe_name(const char *name) {
  if (!name || !name[0]) return SALTS_EINVAL;
  for (size_t i = 0u; name[i]; ++i) {
    const unsigned char value = (unsigned char)name[i];
    if (value <= 0x20u || value == 0x7fu || value == '?' || value == '#' || value == '@')
      return SALTS_EINVAL;
  }
  return SALTS_OK;
}

static int validate_stream_uri(const char *uri) {
  uri_t parsed;
  const char *expected_scheme;
  if (!uri) return SALTS_EINVAL;
  if (strncmp(uri, "pipe://", 7u) == 0) return validate_pipe_name(uri + 7u);
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
  if (strlen(parsed.host) >= CNET_PLUGIN_URI_HOST_CAPACITY) return SALTS_ERANGE;
  if (!(parsed.component_flags & URI_COMPONENT_PORT) ||
      (parsed.overflow_flags & URI_OVERFLOW_PORT) || parsed.port <= 0 || parsed.port > UINT16_MAX)
    return SALTS_ERANGE;
  return SALTS_OK;
}

static int read_stream(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                       turbo_flow_cnet_plugin_config_t *config, int source,
                       turbo_flow_config_error_t *error) {
  const char *const *fields = source ? stream_source_fields : stream_sink_fields;
  const size_t count = source ? ARRAY_COUNT(stream_source_fields) : ARRAY_COUNT(stream_sink_fields);
  int rc = source
               ? exact_fields_optional(view, name, fields, count, NULL, 0u,
                                       source_content_fields,
                                       ARRAY_COUNT(source_content_fields), error)
               : exact_fields(view, name, fields, count, NULL, 0u, error);
  if (rc == SALTS_OK) rc = read_client(view, name, config, error);
  if (rc == SALTS_OK) rc = get_text(view, name, "uri", config->uri, sizeof(config->uri), 0, error);
  if (rc == SALTS_OK) {
    const int uri_status = validate_stream_uri(config->uri);
    if (uri_status != SALTS_OK)
      rc = config_error(error, uri_status, name, "uri", "invalid CNet stream URI");
  }
  if (rc == SALTS_OK) rc = read_socket_options(view, name, config, error);
  if (rc == SALTS_OK) rc = read_stream_tls(view, name, config, error);
  if (rc == SALTS_OK)
    rc = source ? read_common_source_tail(view, name, config, error)
                : read_common_sink_tail(view, name, config, error);
  if (rc == SALTS_OK && source && config->max_message_bytes > config->client.receive_buffer_bytes)
    rc = config_error(error, SALTS_ERANGE, name, "max_message_bytes",
                      "source message bound exceeds receive buffer");
  if (rc == SALTS_OK && !source && config->max_message_bytes > config->client.max_send_bytes)
    rc = config_error(error, SALTS_ERANGE, name, "max_message_bytes",
                      "sink message bound exceeds send bound");
  return rc;
}

static int read_listener(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                         turbo_flow_cnet_plugin_config_t *config,
                         turbo_flow_config_error_t *error) {
  size_t alpn_count = 0u;
  char auth[32];
  int rc = exact_fields_optional(
      view, name, listener_source_fields, ARRAY_COUNT(listener_source_fields),
      NULL, 0u, source_content_fields, ARRAY_COUNT(source_content_fields), error);
  if (rc == SALTS_OK) rc = read_client(view, name, config, error);
  if (rc == SALTS_OK)
    rc = get_text(view, name, "bind_host", config->bind_host, sizeof(config->bind_host), 0, error);
  config->listener.backend = config->client.backend;
  config->listener.host = config->bind_host;
  if (rc == SALTS_OK) rc = get_u16(view, name, "bind_port", 1, &config->listener.port, error);
  if (rc == SALTS_OK) rc = get_size(view, name, "backlog", 0, &config->listener.backlog, error);
  config->listener_options = (cnet_listener_options)CNET_LISTENER_OPTIONS_INIT;
  if (rc == SALTS_OK)
    rc = get_bool(view, name, "reuse_port", &config->listener_options.reuse_port, error);
  if (rc == SALTS_OK) rc = read_socket_options(view, name, config, error);
  if (rc == SALTS_OK) rc = get_bool(view, name, "tls_enabled", &config->tls_enabled, error);
  if (rc == SALTS_OK) rc = read_tls_strings(view, name, config, &alpn_count, error);
  if (rc == SALTS_OK) rc = get_text(view, name, "tls_client_auth", auth, sizeof(auth), 0, error);
  if (rc == SALTS_OK && strcmp(auth, "none") == 0)
    config->tls_server.client_auth = CNET_TLS_CLIENT_AUTH_NONE;
  else if (rc == SALTS_OK && strcmp(auth, "required") == 0)
    config->tls_server.client_auth = CNET_TLS_CLIENT_AUTH_REQUIRED;
  else if (rc == SALTS_OK)
    rc = config_error(error, SALTS_EINVAL, name, "tls_client_auth",
                      "TLS client auth must be none or required");
  if (rc == SALTS_OK)
    rc = get_size(view, name, "max_connections", 0, &config->max_connections, error);
  if (rc == SALTS_OK) rc = read_common_source_tail(view, name, config, error);
  if (rc != SALTS_OK) return rc;
  if (validate_bind_host(config->bind_host) != SALTS_OK)
    return config_error(error, SALTS_EINVAL, name, "bind_host",
                        "bind host must be a numeric IP address");
  if (config->listener.backlog > (size_t)INT_MAX ||
      config->max_connections > config->client.connection_capacity ||
      config->max_message_bytes > config->client.receive_buffer_bytes)
    return config_error(error, SALTS_ERANGE, name, NULL,
                        "listener bounds exceed the configured client bounds");
  if (!config->tls_enabled &&
      (config->client.tls_io_buffer_bytes || config->tls_ca_file[0] || config->tls_ca_path[0] ||
       config->tls_cert_file[0] || config->tls_key_file[0] || config->tls_key_password[0] ||
       alpn_count || config->tls_server.client_auth != CNET_TLS_CLIENT_AUTH_NONE))
    return config_error(error, SALTS_EINVAL, name, "tls_enabled",
                        "disabled TLS requires empty TLS material and no client auth");
  if (config->tls_enabled && (!config->tls_cert_file[0] || !config->tls_key_file[0] ||
                              config->client.tls_io_buffer_bytes == 0u ||
                              (config->tls_server.client_auth == CNET_TLS_CLIENT_AUTH_REQUIRED &&
                               !config->tls_ca_file[0] && !config->tls_ca_path[0])))
    return config_error(error, SALTS_EINVAL, name, "tls_enabled",
                        "enabled listener TLS requires explicit storage and key material");
  if (config->tls_enabled && config->tls_server.client_auth == CNET_TLS_CLIENT_AUTH_NONE &&
      (config->tls_ca_file[0] || config->tls_ca_path[0]))
    return config_error(error, SALTS_EINVAL, name, "tls_client_auth",
                        "listener CA material requires required client authentication");
  config->tls_server.size = sizeof(config->tls_server);
  config->tls_server.ca_file = optional_text(config->tls_ca_file);
  config->tls_server.ca_path = optional_text(config->tls_ca_path);
  config->tls_server.cert_file = optional_text(config->tls_cert_file);
  config->tls_server.key_file = optional_text(config->tls_key_file);
  config->tls_server.key_password = optional_text(config->tls_key_password);
  config->tls_server.alpn_protocols = alpn_count ? config->alpn : NULL;
  config->tls_server.alpn_protocol_count = alpn_count;
  return SALTS_OK;
}

static int hex_nibble(char value) {
  if (value >= '0' && value <= '9') return value - '0';
  if (value >= 'a' && value <= 'f') return value - 'a' + 10;
  if (value >= 'A' && value <= 'F') return value - 'A' + 10;
  return -1;
}

static int parse_psk(const char *text, uint8_t output[CNET_KCP_PSK_BYTES]) {
  uint8_t combined = 0u;
  if (!text || strlen(text) != CNET_KCP_PSK_BYTES * 2u) return SALTS_EINVAL;
  for (size_t i = 0u; i < CNET_KCP_PSK_BYTES; ++i) {
    int high = hex_nibble(text[i * 2u]);
    int low = hex_nibble(text[i * 2u + 1u]);
    if (high < 0 || low < 0) return SALTS_EINVAL;
    output[i] = (uint8_t)((high << 4) | low);
    combined |= output[i];
  }
  return combined != 0u ? SALTS_OK : SALTS_EINVAL;
}

static int validate_plain_kcp(const char *name, const turbo_flow_cnet_plugin_config_t *config,
                              const char *psk, turbo_flow_config_error_t *error) {
  const cnet_kcp_config *kcp = &config->endpoint.kcp;
  const cnet_kcp_fec_config *fec = &config->endpoint.security.fec;
  if (kcp->mtu < CNET_PLUGIN_KCP_MIN_MTU ||
      kcp->mtu > config->endpoint.datagram.max_datagram_bytes ||
      kcp->send_window > (uint32_t)INT_MAX || kcp->receive_window > (uint32_t)INT_MAX ||
      kcp->interval_ms < CNET_PLUGIN_KCP_MIN_INTERVAL_MS ||
      kcp->interval_ms > CNET_PLUGIN_KCP_MAX_INTERVAL_MS || kcp->fast_resend > (uint32_t)INT_MAX ||
      kcp->send_segment_capacity > (size_t)INT_MAX || kcp->max_message_bytes > (size_t)INT_MAX)
    return config_error(error, SALTS_EINVAL, name, NULL, "plain KCP bounds are invalid");
  if (psk[0] || config->endpoint.security.handshake_retry_ms != 0u ||
      fec->backend != CNET_KCP_FEC_NONE || fec->data_shards != 0u || fec->parity_shards != 0u ||
      fec->max_payload_bytes != 0u || fec->receive_group_count != 0u)
    return config_error(error, SALTS_EINVAL, name, "security_mode",
                        "plain KCP requires an explicit empty security policy");
  return SALTS_OK;
}

static int validate_secure_kcp(const char *name, turbo_flow_cnet_plugin_config_t *config,
                               const char *psk, turbo_flow_config_error_t *error) {
  const cnet_kcp_config *kcp = &config->endpoint.kcp;
  const cnet_kcp_fec_config *fec = &config->endpoint.security.fec;
  const size_t total_shards = (size_t)fec->data_shards + fec->parity_shards;
  const size_t shard_size = (size_t)fec->max_payload_bytes + 2u;
  size_t group_bytes;
  size_t fec_datagram_bytes;
  if (kcp->mtu < CNET_PLUGIN_SECURE_KCP_MIN_MTU || kcp->mtu > CNET_DATAGRAM_MAX_PAYLOAD_BYTES ||
      kcp->send_window > (uint32_t)INT_MAX || kcp->receive_window > (uint32_t)INT_MAX ||
      kcp->interval_ms < CNET_PLUGIN_KCP_MIN_INTERVAL_MS ||
      kcp->interval_ms > CNET_PLUGIN_KCP_MAX_INTERVAL_MS || kcp->fast_resend > (uint32_t)INT_MAX ||
      kcp->send_segment_capacity > (size_t)INT_MAX || kcp->max_message_bytes > (size_t)INT_MAX ||
      parse_psk(psk, config->endpoint.security.pre_shared_key) != SALTS_OK ||
      config->endpoint.security.handshake_retry_ms == 0u ||
      fec->backend != CNET_KCP_FEC_REED_SOLOMON || fec->data_shards == 0u ||
      fec->parity_shards == 0u || total_shards > CNET_PLUGIN_FEC_MAX_TOTAL_SHARDS ||
      fec->receive_group_count == 0u ||
      fec->receive_group_count > CNET_PLUGIN_FEC_MAX_RECEIVE_GROUPS ||
      fec->max_payload_bytes < kcp->mtu + CNET_KCP_SECURE_RECORD_OVERHEAD)
    return config_error(error, SALTS_EINVAL, name, "psk_hex", "secure KCP bounds are invalid");
  if (total_shards > SIZE_MAX / shard_size)
    return config_error(error, SALTS_ERANGE, name, "fec_data_shards",
                        "secure KCP FEC storage overflows");
  group_bytes = total_shards * shard_size;
  if ((size_t)fec->receive_group_count > SIZE_MAX / group_bytes ||
      group_bytes * fec->receive_group_count > CNET_PLUGIN_FEC_MAX_STATE_BYTES)
    return config_error(error, SALTS_ERANGE, name, "fec_receive_group_count",
                        "secure KCP FEC storage exceeds its bound");
  fec_datagram_bytes = (size_t)fec->max_payload_bytes + CNET_PLUGIN_SECURE_FEC_WIRE_OVERHEAD_BYTES;
  if (fec_datagram_bytes > config->endpoint.datagram.max_datagram_bytes ||
      fec_datagram_bytes > config->endpoint.datagram.receive_buffer_bytes)
    return config_error(error, SALTS_EINVAL, name, "fec_max_payload_bytes",
                        "secure KCP FEC frame exceeds the datagram bounds");
  return SALTS_OK;
}

static int read_packet(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                       turbo_flow_cnet_plugin_config_t *config, int source,
                       turbo_flow_config_error_t *error) {
  const char *const *tail = source ? packet_source_tail : packet_sink_tail;
  const size_t tail_count =
      source ? ARRAY_COUNT(packet_source_tail) : ARRAY_COUNT(packet_sink_tail);
  char mode[32];
  char security[32];
  char fec[32];
  char psk[CNET_KCP_PSK_BYTES * 2u + 1u];
  uint64_t raw = 0u;
  int boolean_value = 0;
  int rc = source
               ? exact_fields_optional(
                     view, name, packet_common_fields, ARRAY_COUNT(packet_common_fields),
                     tail, tail_count, source_content_fields,
                     ARRAY_COUNT(source_content_fields), error)
               : exact_fields(view, name, packet_common_fields,
                              ARRAY_COUNT(packet_common_fields), tail, tail_count, error);
  if (rc == SALTS_OK) rc = read_datagram(view, name, config, error);
  config->endpoint = (cnet_packet_endpoint_config)CNET_PACKET_ENDPOINT_CONFIG_INIT;
  config->endpoint.datagram = config->datagram;
  if (rc == SALTS_OK) rc = get_text(view, name, "packet_mode", mode, sizeof(mode), 0, error);
  if (rc == SALTS_OK && strcmp(mode, "udp") == 0) config->endpoint.protocol = CNET_PACKET_UDP;
  else if (rc == SALTS_OK && strcmp(mode, "kcp") == 0) config->endpoint.protocol = CNET_PACKET_KCP;
  else if (rc == SALTS_OK)
    rc = config_error(error, SALTS_EINVAL, name, "packet_mode", "packet mode must be udp or kcp");
  if (rc == SALTS_OK)
    rc = get_size(view, name, "session_capacity", 0, &config->endpoint.session_capacity, error);
  config->endpoint.kcp = (cnet_kcp_config)CNET_KCP_CONFIG_INIT;
  config->endpoint.kcp.conversation = 0u;
#define READ_KCP_U32(field, member, zero)                                                          \
  if (rc == SALTS_OK) rc = get_u32(view, name, field, zero, &config->endpoint.kcp.member, error)
  READ_KCP_U32("kcp_mtu", mtu, 1);
  READ_KCP_U32("kcp_send_window", send_window, 1);
  READ_KCP_U32("kcp_receive_window", receive_window, 1);
  READ_KCP_U32("kcp_interval_ms", interval_ms, 1);
  READ_KCP_U32("kcp_fast_resend", fast_resend, 1);
#undef READ_KCP_U32
  if (rc == SALTS_OK) rc = get_bool(view, name, "kcp_no_congestion_window", &boolean_value, error);
  if (rc == SALTS_OK) config->endpoint.kcp.no_congestion_window = boolean_value != 0;
  if (rc == SALTS_OK) rc = get_bool(view, name, "kcp_stream_mode", &boolean_value, error);
  if (rc == SALTS_OK) config->endpoint.kcp.stream_mode = boolean_value != 0;
  if (rc == SALTS_OK)
    rc = get_size(view, name, "kcp_send_segment_capacity", 1,
                  &config->endpoint.kcp.send_segment_capacity, error);
  if (rc == SALTS_OK)
    rc = get_size(view, name, "kcp_max_message_bytes", 1, &config->endpoint.kcp.max_message_bytes,
                  error);
  if (rc == SALTS_OK)
    rc = get_text(view, name, "security_mode", security, sizeof(security), 0, error);
  config->endpoint.security = (cnet_kcp_security_config)CNET_KCP_SECURITY_CONFIG_INIT;
  if (rc == SALTS_OK && strcmp(security, "none") == 0)
    config->endpoint.security.mode = CNET_KCP_SECURITY_NONE;
  else if (rc == SALTS_OK && strcmp(security, "psk_v1") == 0)
    config->endpoint.security.mode = CNET_KCP_SECURITY_PSK_V1;
  else if (rc == SALTS_OK)
    rc = config_error(error, SALTS_EINVAL, name, "security_mode",
                      "security mode must be none or psk_v1");
  if (rc == SALTS_OK) rc = get_text(view, name, "psk_hex", psk, sizeof(psk), 1, error);
  if (rc == SALTS_OK)
    rc = get_u32(view, name, "handshake_retry_ms", 1, &config->endpoint.security.handshake_retry_ms,
                 error);
  if (rc == SALTS_OK) rc = get_text(view, name, "fec_backend", fec, sizeof(fec), 0, error);
  if (rc == SALTS_OK && strcmp(fec, "none") == 0)
    config->endpoint.security.fec.backend = CNET_KCP_FEC_NONE;
  else if (rc == SALTS_OK && strcmp(fec, "reed_solomon") == 0)
    config->endpoint.security.fec.backend = CNET_KCP_FEC_REED_SOLOMON;
  else if (rc == SALTS_OK)
    rc = config_error(error, SALTS_EINVAL, name, "fec_backend",
                      "FEC backend must be none or reed_solomon");
#define READ_FEC(field, member)                                                                    \
  if (rc == SALTS_OK) {                                                                            \
    rc = get_u64(view, name, field, UINT16_MAX, 1, &raw, error);                                   \
    if (rc == SALTS_OK) config->endpoint.security.fec.member = (uint16_t)raw;                      \
  }
  READ_FEC("fec_data_shards", data_shards);
  READ_FEC("fec_parity_shards", parity_shards);
  READ_FEC("fec_max_payload_bytes", max_payload_bytes);
  READ_FEC("fec_receive_group_count", receive_group_count);
#undef READ_FEC
  if (rc != SALTS_OK) return rc;
  if (config->endpoint.session_capacity > UINT32_MAX)
    return config_error(error, SALTS_ERANGE, name, "session_capacity",
                        "packet session capacity exceeds the handle space");
  if (config->endpoint.protocol == CNET_PACKET_UDP) {
    if (config->endpoint.kcp.mtu != 0u || config->endpoint.kcp.send_window != 0u ||
        config->endpoint.kcp.receive_window != 0u || config->endpoint.kcp.interval_ms != 0u ||
        config->endpoint.kcp.fast_resend != 0u || config->endpoint.kcp.no_congestion_window ||
        config->endpoint.kcp.stream_mode || config->endpoint.kcp.send_segment_capacity != 0u ||
        config->endpoint.kcp.max_message_bytes != 0u ||
        config->endpoint.security.mode != CNET_KCP_SECURITY_NONE || psk[0] ||
        config->endpoint.security.handshake_retry_ms != 0u ||
        config->endpoint.security.fec.backend != CNET_KCP_FEC_NONE ||
        config->endpoint.security.fec.data_shards != 0u ||
        config->endpoint.security.fec.parity_shards != 0u ||
        config->endpoint.security.fec.max_payload_bytes != 0u ||
        config->endpoint.security.fec.receive_group_count != 0u)
      return config_error(error, SALTS_EINVAL, name, "packet_mode",
                          "UDP mode requires an explicit empty KCP policy");
  } else {
    if (config->endpoint.kcp.mtu == 0u || config->endpoint.kcp.send_window == 0u ||
        config->endpoint.kcp.receive_window == 0u || config->endpoint.kcp.interval_ms == 0u ||
        config->endpoint.kcp.send_segment_capacity == 0u ||
        config->endpoint.kcp.max_message_bytes == 0u || config->endpoint.kcp.stream_mode)
      return config_error(error, SALTS_EINVAL, name, NULL, "invalid bounded KCP policy");
    rc = config->endpoint.security.mode == CNET_KCP_SECURITY_PSK_V1
             ? validate_secure_kcp(name, config, psk, error)
             : validate_plain_kcp(name, config, psk, error);
    if (rc != SALTS_OK) return rc;
  }
  if (source) {
    rc = get_size(view, name, "queue_capacity", 0, &config->queue_capacity, error);
    if (rc == SALTS_OK) rc = read_common_source_tail(view, name, config, error);
  } else {
    uint16_t peer_port = 0u;
    uint32_t scope_id = 0u;
    rc = get_text(view, name, "peer_host", config->peer_host, sizeof(config->peer_host), 0, error);
    if (rc == SALTS_OK) rc = get_u16(view, name, "peer_port", 0, &peer_port, error);
    if (rc == SALTS_OK) rc = get_u32(view, name, "peer_scope_id", 1, &scope_id, error);
    if (rc == SALTS_OK &&
        parse_peer(config->peer_host, peer_port, scope_id, &config->peer) != SALTS_OK)
      rc =
          config_error(error, SALTS_EINVAL, name, "peer_host", "peer must be a numeric IP address");
    if (rc == SALTS_OK) rc = get_u32(view, name, "conversation", 1, &config->conversation, error);
    if (rc == SALTS_OK)
      rc = get_size(view, name, "adapter_send_capacity", 0, &config->adapter_send_capacity, error);
    if (rc == SALTS_OK) rc = read_common_sink_tail(view, name, config, error);
    if (rc == SALTS_OK && config->adapter_send_capacity > UINT32_MAX)
      rc = config_error(error, SALTS_ERANGE, name, "adapter_send_capacity",
                        "packet send capacity exceeds the terminal handle space");
    if (rc == SALTS_OK && config->endpoint.protocol == CNET_PACKET_UDP &&
        config->adapter_send_capacity > config->endpoint.datagram.send_capacity)
      rc = config_error(error, SALTS_ERANGE, name, "adapter_send_capacity",
                        "UDP adapter send capacity exceeds datagram send capacity");
    if (rc == SALTS_OK && config->endpoint.protocol == CNET_PACKET_UDP &&
        config->conversation != 0u)
      rc = config_error(error, SALTS_EINVAL, name, "conversation",
                        "UDP packet sink conversation must be zero");
    if (rc == SALTS_OK && config->endpoint.protocol == CNET_PACKET_KCP &&
        ((config->endpoint.security.mode == CNET_KCP_SECURITY_NONE && config->conversation == 0u) ||
         (config->endpoint.security.mode != CNET_KCP_SECURITY_NONE && config->conversation != 0u)))
      rc = config_error(error, SALTS_EINVAL, name, "conversation",
                        "plain KCP requires a conversation; secure KCP derives it");
  }
  if (rc == SALTS_OK &&
      config->max_message_bytes > (config->endpoint.protocol == CNET_PACKET_UDP
                                       ? config->endpoint.datagram.max_datagram_bytes
                                       : config->endpoint.kcp.max_message_bytes))
    rc = config_error(error, SALTS_ERANGE, name, "max_message_bytes",
                      "message bound exceeds the selected packet mode bound");
  return rc;
}

static int read_datagram_sink(const turbo_flow_resolved_adapter_view_t *view, const char *name,
                              turbo_flow_cnet_plugin_config_t *config,
                              turbo_flow_config_error_t *error) {
  uint16_t peer_port = 0u;
  uint32_t scope_id = 0u;
  int rc = exact_fields(view, name, datagram_sink_fields, ARRAY_COUNT(datagram_sink_fields), NULL,
                        0u, error);
  if (rc == SALTS_OK) rc = read_datagram(view, name, config, error);
  if (rc == SALTS_OK)
    rc = get_text(view, name, "peer_host", config->peer_host, sizeof(config->peer_host), 0, error);
  if (rc == SALTS_OK) rc = get_u16(view, name, "peer_port", 0, &peer_port, error);
  if (rc == SALTS_OK) rc = get_u32(view, name, "peer_scope_id", 1, &scope_id, error);
  if (rc == SALTS_OK &&
      parse_peer(config->peer_host, peer_port, scope_id, &config->peer) != SALTS_OK)
    rc = config_error(error, SALTS_EINVAL, name, "peer_host", "peer must be a numeric IP address");
  if (rc == SALTS_OK) rc = read_common_sink_tail(view, name, config, error);
  if (rc == SALTS_OK && config->max_message_bytes > config->datagram.max_datagram_bytes)
    rc = config_error(error, SALTS_ERANGE, name, "max_message_bytes",
                      "message bound exceeds datagram bound");
  return rc;
}

int turbo_flow_cnet_plugin_config_read(const turbo_flow_resolved_config_t *resolved,
                                       const char *name, turbo_flow_cnet_plugin_kind_t kind,
                                       turbo_flow_cnet_plugin_config_t *config,
                                       turbo_flow_config_error_t *error) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  uint64_t version = 0u;
  int rc;
  if (!resolved || !name || !name[0] || kind >= TURBO_FLOW_CNET_PLUGIN_KIND_COUNT || !config ||
      !error || error->size < sizeof(*error))
    return config_error(error, SALTS_EINVAL, name ? name : "unknown", NULL,
                        "invalid CNet provider arguments");
  memset(config, 0, sizeof(*config));
  config->kind = kind;
  rc = turbo_flow_resolved_config_adapter(resolved, name, &view);
  if (rc != SALTS_OK) return config_error(error, rc, name, NULL, "adapter config is missing");
  if (!view.kind || strcmp(view.kind, turbo_flow_cnet_plugin_kind_name(kind)) != 0)
    return config_error(error, SALTS_EPROTO, name, NULL, "adapter kind does not match provider");
  rc = get_u64(&view, name, "schema_version", UINT32_MAX, 0, &version, error);
  if (rc != SALTS_OK) return rc;
  if (version != 1u)
    return config_error(error, SALTS_ENOTSUP, name, "schema_version",
                        "unsupported CNet adapter schema version");
  switch (kind) {
  case TURBO_FLOW_CNET_PLUGIN_STREAM_SOURCE:
    rc = read_stream(&view, name, config, 1, error);
    break;
  case TURBO_FLOW_CNET_PLUGIN_LISTENER_SOURCE:
    rc = read_listener(&view, name, config, error);
    break;
  case TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE:
    rc = read_packet(&view, name, config, 1, error);
    break;
  case TURBO_FLOW_CNET_PLUGIN_STREAM_SINK:
    rc = read_stream(&view, name, config, 0, error);
    break;
  case TURBO_FLOW_CNET_PLUGIN_DATAGRAM_SINK:
    rc = read_datagram_sink(&view, name, config, error);
    break;
  case TURBO_FLOW_CNET_PLUGIN_PACKET_SINK:
    rc = read_packet(&view, name, config, 0, error);
    break;
  default:
    rc = SALTS_EINVAL;
    break;
  }
  if (rc == SALTS_OK)
    rc = build_content_descriptor(
        &view, name, kind <= TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE, config, error);
  return rc;
}
