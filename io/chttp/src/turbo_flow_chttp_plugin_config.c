#include "turbo_flow_chttp_plugin_internal.h"
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <uri_parser.h>
#if defined(_WIN32)
  #include <ws2tcpip.h>
#else
  #include <arpa/inet.h>
  #include <strings.h>
#endif

enum { FIELD_SZ, FIELD_U32, FIELD_U64, FIELD_UINT, FIELD_U16, FIELD_BOOL, FIELD_TEXT };
typedef struct field_s {
  unsigned kinds;
  const char *name;
  size_t offset;
  int type;
  uint64_t minimum;
} field_t;
#define FIELD(k, n, m, t, min) {k, n, offsetof(chttp_plugin_config_t, m), t, min}
static const field_t fields[] = {
    FIELD(7, "schema_version", schema_version, FIELD_U32, 1),
    FIELD(7, "poll_budget_ms", poll_budget_ms, FIELD_U32, 1),
    FIELD(7, "tls_enabled", tls_enabled, FIELD_BOOL, 0),
    FIELD(1, "idempotent", client_adapter.idempotent, FIELD_BOOL, 0),
    FIELD(6, "bind_port", server.port, FIELD_U16, 0),
    FIELD(7, "network_connection_capacity", network.connection_capacity, FIELD_SZ, 1),
    FIELD(7, "network_command_capacity", network.command_capacity, FIELD_SZ, 1),
    FIELD(7, "network_request_capacity", network.request_capacity, FIELD_SZ, 1),
    FIELD(7, "network_completion_batch_capacity", network.completion_batch_capacity, FIELD_SZ, 1),
    FIELD(7, "network_event_capacity", network.event_capacity, FIELD_SZ, 1),
    FIELD(7, "network_max_send_bytes", network.max_send_bytes, FIELD_SZ, 1),
    FIELD(7, "network_receive_buffer_bytes", network.receive_buffer_bytes, FIELD_SZ, 1),
    FIELD(7, "network_command_buffer_bytes", network.command_buffer_bytes, FIELD_SZ, 1),
    FIELD(7, "network_event_buffer_bytes", network.event_buffer_bytes, FIELD_SZ, 1),
    FIELD(7, "network_connect_timeout_ms", network.connect_timeout_ms, FIELD_U32, 0),
    FIELD(7, "network_read_timeout_ms", network.read_timeout_ms, FIELD_U32, 0),
    FIELD(7, "network_write_timeout_ms", network.write_timeout_ms, FIELD_U32, 0),
    FIELD(7, "network_tls_handshake_timeout_ms", network.tls_handshake_timeout_ms, FIELD_U32, 0),
    FIELD(7, "network_tls_io_buffer_bytes", network.tls_io_buffer_bytes, FIELD_SZ, 0),
    FIELD(1, "request_capacity", client.request_capacity, FIELD_SZ, 1),
    FIELD(1, "max_start_line_bytes", client.max_start_line_bytes, FIELD_SZ, 1),
    FIELD(1, "max_header_count", client.max_header_count, FIELD_SZ, 1),
    FIELD(1, "max_header_bytes", client.max_header_bytes, FIELD_SZ, 1),
    FIELD(1, "max_request_body_bytes", client.max_request_body_bytes, FIELD_SZ, 1),
    FIELD(1, "max_response_body_bytes", client.max_response_body_bytes, FIELD_SZ, 1),
    FIELD(1, "max_informational_responses", client.max_informational_responses, FIELD_SZ, 1),
    FIELD(1, "stream_chunk_bytes", client.stream_chunk_bytes, FIELD_SZ, 1),
    FIELD(1, "h2_input_buffer_bytes", client.h2_input_buffer_bytes, FIELD_SZ, 1),
    FIELD(1, "h2_hpack_dynamic_table_bytes", client.h2_hpack_dynamic_table_bytes, FIELD_SZ, 1),
    FIELD(1, "h2_max_settings_count", client.h2_max_settings_count, FIELD_SZ, 1),
    FIELD(6, "backlog", server.backlog, FIELD_SZ, 1),
    FIELD(6, "route_capacity", server.route_capacity, FIELD_SZ, 1),
    FIELD(6, "max_target_bytes", server.max_target_bytes, FIELD_SZ, 1),
    FIELD(6, "max_header_count", server.max_header_count, FIELD_SZ, 1),
    FIELD(6, "max_header_bytes", server.max_header_bytes, FIELD_SZ, 1),
    FIELD(6, "max_request_body_bytes", server.max_request_body_bytes, FIELD_SZ, 1),
    FIELD(6, "max_response_header_count", server.max_response_header_count, FIELD_SZ, 1),
    FIELD(6, "max_response_header_bytes", server.max_response_header_bytes, FIELD_SZ, 1),
    FIELD(6, "max_response_body_bytes", server.max_response_body_bytes, FIELD_SZ, 1),
    FIELD(6, "stream_chunk_bytes", server.stream_chunk_bytes, FIELD_SZ, 1),
    FIELD(6, "max_buffered_response_body_bytes", server.max_buffered_response_body_bytes, FIELD_SZ,
          1),
    FIELD(6, "buffer_capacity_bytes", server.buffer_capacity_bytes, FIELD_SZ, 1),
    FIELD(6, "middleware_capacity", server.middleware_capacity, FIELD_SZ, 0),
    FIELD(6, "max_route_middleware_count", server.max_route_middleware_count, FIELD_SZ, 0),
    FIELD(6, "max_route_param_count", server.max_route_param_count, FIELD_SZ, 0),
    FIELD(6, "max_route_param_bytes", server.max_route_param_bytes, FIELD_SZ, 0),
    FIELD(6, "h2_stream_capacity", server.h2_stream_capacity, FIELD_SZ, 0),
    FIELD(6, "h2_input_buffer_bytes", server.h2_input_buffer_bytes, FIELD_SZ, 0),
    FIELD(6, "h2_output_buffer_bytes", server.h2_output_buffer_bytes, FIELD_SZ, 0),
    FIELD(6, "h2_hpack_dynamic_table_bytes", server.h2_hpack_dynamic_table_bytes, FIELD_SZ, 0),
    FIELD(6, "h2_max_settings_count", server.h2_max_settings_count, FIELD_SZ, 0),
    FIELD(6, "poll_slice_ms", server.poll_slice_ms, FIELD_U32, 1),
    FIELD(1, "overall_timeout_ms", client_adapter.overall_timeout_ms, FIELD_U32, 0),
    FIELD(1, "retry_delay_ms", client_adapter.retry_delay_ms, FIELD_U32, 0),
    FIELD(1, "max_attempts", client_adapter.max_attempts, FIELD_U32, 1),
    FIELD(1, "stop_timeout_ms", client_adapter.stop_timeout_ms, FIELD_U32, 1),
    FIELD(2, "max_request_message_bytes", server_adapter.max_request_message_bytes, FIELD_SZ, 1),
    FIELD(2, "success_status", server_adapter.success_status, FIELD_UINT, 1),
    FIELD(2, "overload_status", server_adapter.overload_status, FIELD_UINT, 1),
    FIELD(2, "unavailable_status", server_adapter.unavailable_status, FIELD_UINT, 1),
    FIELD(2, "graph_error_status", server_adapter.graph_error_status, FIELD_UINT, 1),
    FIELD(2, "first_message_id", server_adapter.first_message_id, FIELD_U64, 1),
    FIELD(2, "stop_timeout_ms", server_adapter.stop_timeout_ms, FIELD_U32, 1),
    FIELD(4, "session_capacity", websocket_adapter.session_capacity, FIELD_SZ, 1),
    FIELD(4, "frame_capacity", websocket_adapter.frame_capacity, FIELD_SZ, 1),
    FIELD(4, "max_frame_bytes", websocket_adapter.max_frame_bytes, FIELD_SZ, 1),
    FIELD(4, "max_message_bytes", websocket_adapter.max_message_bytes, FIELD_SZ, 1),
    FIELD(4, "max_buffered_input_bytes", websocket_adapter.max_buffered_input_bytes, FIELD_SZ, 1),
    FIELD(4, "first_message_id", websocket_adapter.first_message_id, FIELD_U64, 1),
    FIELD(4, "stop_timeout_ms", websocket_adapter.stop_timeout_ms, FIELD_U32, 1),
    FIELD(7, "backend", backend, FIELD_TEXT, 1),
    FIELD(7, "protocol", protocol, FIELD_TEXT, 1),
    FIELD(7, "tls_ca_file", tls_ca_file, FIELD_TEXT, 0),
    FIELD(7, "tls_ca_path", tls_ca_path, FIELD_TEXT, 0),
    FIELD(7, "tls_cert_file", tls_cert_file, FIELD_TEXT, 0),
    FIELD(7, "tls_key_file", tls_key_file, FIELD_TEXT, 0),
    FIELD(7, "tls_key_password", tls_key_password, FIELD_TEXT, 0),
    FIELD(1, "connection_uri", connection_uri, FIELD_TEXT, 1),
    FIELD(1, "authority", authority, FIELD_TEXT, 1),
    FIELD(1, "target", target, FIELD_TEXT, 1),
    FIELD(1, "method", method, FIELD_TEXT, 1),
    FIELD(1, "tls_server_name", tls_server_name, FIELD_TEXT, 0),
    FIELD(6, "bind_host", bind_host, FIELD_TEXT, 1),
    FIELD(6, "path", path, FIELD_TEXT, 1),
    FIELD(6, "tls_client_auth", tls_client_auth, FIELD_TEXT, 1),
    FIELD(2, "method", method, FIELD_TEXT, 1),
    FIELD(2, "response_content_type", response_content_type, FIELD_TEXT, 1),
    FIELD(2, "error_content_type", error_content_type, FIELD_TEXT, 1),
    FIELD(2, "graph_error_body", graph_error_body, FIELD_TEXT, 0),
    FIELD(4, "subprotocol", subprotocol, FIELD_TEXT, 0),
};
#undef FIELD
#define COUNT(a) (sizeof(a) / sizeof((a)[0]))
enum {
  H2_INPUT_MIN = 16393,
  H2_OUTPUT_MIN = 16468,
  H1_RESPONSE_RESERVE = 281,
  CHUNK_RESERVE = 32,
  HPACK_FIELD_OVERHEAD = 15,
  HPACK_UPDATE_RESERVE = 12,
  HPACK_GENERATED_RESERVE = 58,
  HTTP_STATUS_MIN = 200,
  HTTP_STATUS_MAX = 599,
  HTTP_NO_CONTENT = 204,
  HTTP_RESET_CONTENT = 205,
  HTTP_NOT_MODIFIED = 304
};

const char *chttp_plugin_kind_name(unsigned kind) {
  switch (kind) {
  case CHTTP_PLUGIN_CLIENT:
    return "chttp.client";
  case CHTTP_PLUGIN_SERVER:
    return "chttp.server";
  case CHTTP_PLUGIN_WEBSOCKET:
    return "chttp.websocket_server";
  default:
    return NULL;
  }
}
static int fail(turbo_flow_config_error_t *e, int rc, const char *name, const char *field,
                const char *message) {
  if (e && e->size >= sizeof(*e)) {
    *e = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    e->status = rc;
    (void)snprintf(e->path, sizeof(e->path), "$.adapters.%s.config.%s", name, field);
    (void)snprintf(e->message, sizeof(e->message), "%s", message);
  }
  return rc;
}
static int read_field(const turbo_flow_resolved_adapter_view_t *view, const field_t *f,
                      chttp_plugin_config_t *c) {
  unsigned char *dst = (unsigned char *)c + f->offset;
  uint64_t value = 0u;
  int boolean = 0;
  int rc;
  if (f->type == FIELD_TEXT) {
    const char *text = NULL;
    rc = turbo_flow_resolved_adapter_get_string(view, f->name, &text);
    if (rc != SALTS_OK) return rc;
    if (!text || strlen(text) < f->minimum || strlen(text) >= CHTTP_PLUGIN_TEXT_BYTES)
      return SALTS_ERANGE;
    memcpy(dst, text, strlen(text) + 1u);
    return SALTS_OK;
  }
  if (f->type == FIELD_BOOL) {
    rc = turbo_flow_resolved_adapter_get_bool(view, f->name, &boolean);
    if (rc == SALTS_OK) *(int *)dst = boolean;
    return rc;
  }
  turbo_flow_config_value_type_t actual;
  rc = turbo_flow_resolved_adapter_field_type(view, f->name, &actual);
  if (rc != SALTS_OK) return rc;
  if (actual != TURBO_FLOW_CONFIG_NUMBER) return SALTS_EINVAL;
  rc = turbo_flow_resolved_adapter_get_u64(view, f->name, &value);
  if (rc != SALTS_OK) return rc;
  if (value < f->minimum || (f->type != FIELD_U64 && value > UINT32_MAX) ||
      (f->type == FIELD_U16 && value > UINT16_MAX))
    return SALTS_ERANGE;
  switch (f->type) {
  case FIELD_SZ:
    *(size_t *)dst = (size_t)value;
    break;
  case FIELD_U32:
    *(uint32_t *)dst = (uint32_t)value;
    break;
  case FIELD_U64:
    *(uint64_t *)dst = value;
    break;
  case FIELD_UINT:
    *(unsigned int *)dst = (unsigned int)value;
    break;
  case FIELD_U16:
    *(uint16_t *)dst = (uint16_t)value;
    break;
  default:
    return SALTS_EINVAL;
  }
  return SALTS_OK;
}
static int read_array(const turbo_flow_resolved_adapter_view_t *view, const char *field,
                      char (*storage)[CHTTP_PLUGIN_TEXT_BYTES], size_t capacity, size_t *count,
                      int allow_empty) {
  int rc = turbo_flow_resolved_adapter_array_size(view, field, count);
  if (rc != SALTS_OK) return rc;
  if (*count > capacity) return SALTS_ENOSPC;
  for (size_t i = 0u; i < *count; ++i) {
    const char *value = NULL;
    rc = turbo_flow_resolved_adapter_array_string_at(view, field, i, &value);
    if (rc != SALTS_OK) return rc;
    if (!value || (!allow_empty && !value[0]) || strlen(value) >= CHTTP_PLUGIN_TEXT_BYTES)
      return SALTS_ERANGE;
    memcpy(storage[i], value, strlen(value) + 1u);
  }
  return SALTS_OK;
}
static int clean_text(const char *text) {
  for (const unsigned char *p = (const unsigned char *)text; *p; ++p)
    if (*p <= ' ' || *p == 127u) return 0;
  return 1;
}
static const char *optional(const char *value) { return value[0] ? value : NULL; }
static int header_name_equal(const char *left, const char *right) {
#if defined(_WIN32)
  return _stricmp(left, right) == 0;
#else
  return strcasecmp(left, right) == 0;
#endif
}
static int header_valid(const char *name, const char *value, int h2) {
  static const char token[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!#$%&'*+-.^_`|~";
  if (!name[0] || strspn(name, token) != strlen(name) || header_name_equal(name, "host") ||
      header_name_equal(name, "content-length") || header_name_equal(name, "transfer-encoding") ||
      header_name_equal(name, "connection") ||
      (h2 && (header_name_equal(name, "keep-alive") || header_name_equal(name, "upgrade") ||
              header_name_equal(name, "proxy-connection"))))
    return 0;
  for (const unsigned char *p = (const unsigned char *)value; *p; ++p)
    if ((*p < ' ' && *p != '\t') || *p == 127u) return 0;
  return 1;
}
static int pow2(size_t n) { return n && !(n & (n - 1u)); }
static int body_status(unsigned int status) {
  return status >= HTTP_STATUS_MIN && status <= HTTP_STATUS_MAX && status != HTTP_NO_CONTENT &&
         status != HTTP_RESET_CONTENT && status != HTTP_NOT_MODIFIED;
}
static int validate_network(chttp_plugin_config_t *c) {
  cnet_client_config *n = &c->network;
  if (!strcmp(c->backend, "iocp")) n->backend = NATIVE_IO_BACKEND_IOCP;
  else if (!strcmp(c->backend, "epoll")) n->backend = NATIVE_IO_BACKEND_EPOLL;
  else if (!strcmp(c->backend, "kqueue")) n->backend = NATIVE_IO_BACKEND_KQUEUE;
  else if (!strcmp(c->backend, "io_uring")) n->backend = NATIVE_IO_BACKEND_IO_URING;
  else return SALTS_EINVAL;
  if (!native_io_backend_kind_supported(n->backend)) return SALTS_ENOTSUP;
  if (!pow2(n->command_capacity) || !pow2(n->event_capacity) || n->event_capacity < 2u ||
      n->connection_capacity > UINT32_MAX / 2u ||
      n->completion_batch_capacity > n->request_capacity ||
      n->command_buffer_bytes < n->max_send_bytes ||
      n->event_buffer_bytes < n->receive_buffer_bytes)
    return SALTS_ERANGE;
  if (c->tls_enabled) {
    if (n->tls_io_buffer_bytes < CNET_TLS_MIN_IO_BUFFER_BYTES || n->tls_io_buffer_bytes > INT_MAX ||
        n->tls_handshake_timeout_ms == 0u || c->alpn_count == 0u)
      return SALTS_EINVAL;
  } else if (n->tls_io_buffer_bytes || n->tls_handshake_timeout_ms || c->alpn_count ||
             c->tls_ca_file[0] || c->tls_ca_path[0] || c->tls_cert_file[0] || c->tls_key_file[0] ||
             c->tls_key_password[0] || c->tls_server_name[0])
    return SALTS_EINVAL;
  if ((!c->tls_cert_file[0]) != (!c->tls_key_file[0]) ||
      (c->tls_key_password[0] && !c->tls_key_file[0]))
    return SALTS_EINVAL;
  for (size_t i = 0u; i < c->alpn_count; ++i)
    c->alpn[i] = c->alpn_storage[i];
  return SALTS_OK;
}
static int read_method(const char *text, chttp_method *method) {
  static const char *const names[] = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"};
  static const chttp_method values[] = {CHTTP_METHOD_GET,    CHTTP_METHOD_POST,  CHTTP_METHOD_PUT,
                                        CHTTP_METHOD_DELETE, CHTTP_METHOD_PATCH, CHTTP_METHOD_HEAD,
                                        CHTTP_METHOD_OPTIONS};
  for (size_t i = 0u; i < COUNT(names); ++i)
    if (!strcmp(text, names[i])) {
      *method = values[i];
      return SALTS_OK;
    }
  return SALTS_EINVAL;
}
static int validate_client(chttp_plugin_config_t *c) {
  uri_t uri = {0};
  chttp_client_config *n = &c->client;
  turbo_flow_chttp_client_config_t *a = &c->client_adapter;
  const char *alpn;
  if (!strcmp(c->protocol, "h1")) {
    a->protocol = CHTTP_HTTP_1_1;
    alpn = "http/1.1";
  } else if (!strcmp(c->protocol, "h2")) {
    a->protocol = CHTTP_HTTP_2;
    alpn = "h2";
  } else return SALTS_ENOTSUP;
  if (!uri_parse(c->connection_uri, &uri) || !uri.valid || uri.overflow_flags || !uri.host[0] ||
      strlen(uri.host) >= 256u || uri.path[0] || !(uri.component_flags & URI_COMPONENT_PORT) ||
      uri.port <= 0 || uri.port > UINT16_MAX ||
      (uri.component_flags &
       (URI_COMPONENT_USERINFO | URI_COMPONENT_QUERY | URI_COMPONENT_FRAGMENT)) ||
      strcmp(uri.scheme, c->tls_enabled ? "tls" : "tcp") || !clean_text(c->authority) ||
      strpbrk(c->authority, "/?#@") || c->target[0] != '/' || !clean_text(c->target))
    return SALTS_EINVAL;
  if (c->tls_enabled && (c->alpn_count != 1u || strcmp(c->alpn[0], alpn))) return SALTS_ENOTSUP;
  if (a->protocol == CHTTP_HTTP_2 && c->network.max_send_bytes < H2_OUTPUT_MIN) return SALTS_ERANGE;
  if (n->max_start_line_bytes <= 15u || n->max_header_count < 3u ||
      n->h2_input_buffer_bytes < H2_INPUT_MIN ||
      n->stream_chunk_bytes + CHUNK_RESERVE > c->network.max_send_bytes ||
      strlen(c->target) + strlen(c->method) + 12u > n->max_start_line_bytes ||
      strlen(c->authority) + 8u > n->max_header_bytes)
    return SALTS_ERANGE;
  size_t header_bytes = strlen(c->authority) + 8u;
  for (size_t i = 0u; i < a->header_count; ++i) {
    if (!header_valid(c->header_names[i], c->header_values[i], a->protocol == CHTTP_HTTP_2))
      return SALTS_EINVAL;
    header_bytes += strlen(c->header_names[i]) + strlen(c->header_values[i]) + 4u;
    c->headers[i].name = c->header_names[i];
    c->headers[i].value = c->header_values[i];
  }
  if (a->header_count + 3u > n->max_header_count || header_bytes > n->max_header_bytes ||
      n->max_start_line_bytes + n->max_header_bytes + n->max_request_body_bytes + CHUNK_RESERVE >
          c->network.max_send_bytes)
    return SALTS_ERANGE;
  n->network = c->network;
  a->client = n;
  a->connection_uri = c->connection_uri;
  a->authority = c->authority;
  a->target = c->target;
  a->headers = c->headers;
  c->tls_client.size = sizeof(c->tls_client);
  c->tls_client.ca_file = optional(c->tls_ca_file);
  c->tls_client.ca_path = optional(c->tls_ca_path);
  c->tls_client.cert_file = optional(c->tls_cert_file);
  c->tls_client.key_file = optional(c->tls_key_file);
  c->tls_client.key_password = optional(c->tls_key_password);
  c->tls_client.server_name = optional(c->tls_server_name);
  c->tls_client.alpn_protocols = c->alpn;
  c->tls_client.alpn_protocol_count = c->alpn_count;
  return read_method(c->method, &a->method);
}
static int validate_route(const char *path, const chttp_server_config *n) {
  static const char parameter_chars[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
  size_t parameters = 0u, bytes = 0u;
  if (path[0] != '/' || strpbrk(path, "?#")) return SALTS_EINVAL;
  for (const char *cursor = path; *cursor;) {
    const char *segment = cursor + 1;
    const char *end = strchr(segment, '/');
    if (!end) end = segment + strlen(segment);
    if (*segment == ':') {
      size_t length = (size_t)(end - segment);
      if (length <= 1u || strspn(segment + 1, parameter_chars) < length - 1u) return SALTS_EINVAL;
      if (++parameters > n->max_route_param_count || length > n->max_route_param_bytes - bytes)
        return SALTS_ENOBUFS;
      bytes += length;
    } else if (memchr(segment, ':', (size_t)(end - segment))) return SALTS_EINVAL;
    cursor = end;
  }
  return SALTS_OK;
}
static int validate_server(chttp_plugin_config_t *c) {
  chttp_server_config *n = &c->server;
  struct in6_addr address;
  if (inet_pton(AF_INET, c->bind_host, &address) != 1 &&
      inet_pton(AF_INET6, c->bind_host, &address) != 1)
    return SALTS_EINVAL;
  int route_status = validate_route(c->path, n);
  if (route_status != SALTS_OK) return route_status;
  if (!strcmp(c->protocol, "h1")) n->enable_http2 = 0;
  else if (!strcmp(c->protocol, "h1_h2")) n->enable_http2 = 1;
  else return SALTS_ENOTSUP;
  if (!clean_text(c->bind_host) || c->path[0] != '/' || !clean_text(c->path) ||
      strlen(c->path) > n->max_target_bytes || n->backlog > INT_MAX ||
      n->stream_chunk_bytes + CHUNK_RESERVE > c->network.max_send_bytes ||
      n->max_buffered_response_body_bytes > n->max_response_body_bytes ||
      n->max_buffered_response_body_bytes + n->max_response_header_bytes + H1_RESPONSE_RESERVE >
          c->network.max_send_bytes ||
      n->buffer_capacity_bytes < c->network.max_send_bytes ||
      (n->max_route_param_count && !n->max_route_param_bytes))
    return SALTS_ERANGE;
  if (!n->enable_http2) {
    if (n->h2_stream_capacity || n->h2_input_buffer_bytes || n->h2_output_buffer_bytes ||
        n->h2_hpack_dynamic_table_bytes || n->h2_max_settings_count)
      return SALTS_EINVAL;
  } else {
    size_t header_block = (n->max_response_header_count + 2u) * HPACK_FIELD_OVERHEAD +
                          n->max_response_header_bytes + HPACK_GENERATED_RESERVE;
    size_t request_block =
        n->max_header_count * HPACK_FIELD_OVERHEAD + n->max_header_bytes + HPACK_UPDATE_RESERVE;
    if (request_block > header_block) header_block = request_block;
    if (!n->h2_stream_capacity || !n->h2_hpack_dynamic_table_bytes || !n->h2_max_settings_count ||
        n->h2_input_buffer_bytes < H2_INPUT_MIN || n->h2_output_buffer_bytes < H2_OUTPUT_MIN ||
        n->h2_output_buffer_bytes < header_block + 9u ||
        n->h2_output_buffer_bytes > c->network.max_send_bytes)
      return SALTS_ERANGE;
  }
  if (!strcmp(c->tls_client_auth, "none")) c->tls_server.client_auth = CNET_TLS_CLIENT_AUTH_NONE;
  else if (!strcmp(c->tls_client_auth, "required"))
    c->tls_server.client_auth = CNET_TLS_CLIENT_AUTH_REQUIRED;
  else return SALTS_EINVAL;
  if (!c->tls_enabled && c->tls_server.client_auth != CNET_TLS_CLIENT_AUTH_NONE)
    return SALTS_EINVAL;
  if (c->tls_enabled) {
    if (!c->tls_cert_file[0] || !c->tls_key_file[0] ||
        (c->tls_server.client_auth == CNET_TLS_CLIENT_AUTH_REQUIRED && !c->tls_ca_file[0] &&
         !c->tls_ca_path[0]) ||
        (c->tls_server.client_auth == CNET_TLS_CLIENT_AUTH_NONE &&
         (c->tls_ca_file[0] || c->tls_ca_path[0])))
      return SALTS_EINVAL;
    if ((!n->enable_http2 && (c->alpn_count != 1u || strcmp(c->alpn[0], "http/1.1"))) ||
        (n->enable_http2 &&
         (c->alpn_count != 2u || strcmp(c->alpn[0], "h2") || strcmp(c->alpn[1], "http/1.1"))))
      return SALTS_ENOTSUP;
  }
  c->tls_server.size = sizeof(c->tls_server);
  c->tls_server.ca_file = optional(c->tls_ca_file);
  c->tls_server.ca_path = optional(c->tls_ca_path);
  c->tls_server.cert_file = optional(c->tls_cert_file);
  c->tls_server.key_file = optional(c->tls_key_file);
  c->tls_server.key_password = optional(c->tls_key_password);
  c->tls_server.alpn_protocols = c->alpn;
  c->tls_server.alpn_protocol_count = c->alpn_count;
  n->network = c->network;
  n->host = c->bind_host;
  n->tls = c->tls_enabled ? &c->tls_server : NULL;
  if (c->kind == CHTTP_PLUGIN_SERVER) {
    turbo_flow_chttp_server_config_t *a = &c->server_adapter;
    if (read_method(c->method, &a->method) != SALTS_OK || a->success_status < HTTP_STATUS_MIN ||
        a->success_status > HTTP_STATUS_MAX || !body_status(a->overload_status) ||
        !body_status(a->unavailable_status) || !body_status(a->graph_error_status) ||
        strpbrk(c->response_content_type, "\r\n") || strpbrk(c->error_content_type, "\r\n") ||
        n->max_buffered_response_body_bytes < sizeof("request too large") - 1u ||
        strlen(c->graph_error_body) > n->max_buffered_response_body_bytes ||
        strlen(c->response_content_type) + 12u > n->max_response_header_bytes ||
        strlen(c->error_content_type) + 12u > n->max_response_header_bytes ||
        a->max_request_message_bytes < n->max_request_body_bytes)
      return SALTS_ERANGE;
    a->server = n;
    a->path = c->path;
    a->response_content_type = c->response_content_type;
    a->error_content_type = c->error_content_type;
    a->graph_error_body = c->graph_error_body;
    a->graph_error_body_size = strlen(c->graph_error_body);
  } else {
    turbo_flow_chttp_websocket_server_config_t *a = &c->websocket_adapter;
    size_t wire = a->max_frame_bytes + TURBO_FLOW_CHTTP_WEBSOCKET_MAX_WIRE_HEADER_BYTES;
    size_t sessions =
        c->network.connection_capacity * (n->enable_http2 ? n->h2_stream_capacity : 1u);
    if (a->max_frame_bytes < TURBO_FLOW_CHTTP_WEBSOCKET_MIN_FRAME_BYTES ||
        a->max_message_bytes < a->max_frame_bytes || wire > c->network.max_send_bytes ||
        a->max_buffered_input_bytes < wire || a->session_capacity > sessions ||
        (c->subprotocol[0] && (!clean_text(c->subprotocol) || strchr(c->subprotocol, ','))))
      return SALTS_ERANGE;
    a->server = n;
    a->path = c->path;
    a->subprotocol = optional(c->subprotocol);
  }
  return SALTS_OK;
}
int chttp_plugin_config_read(const turbo_flow_resolved_config_t *resolved, const char *name,
                             unsigned kind, chttp_plugin_config_t *c,
                             turbo_flow_config_error_t *error) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  int rc;
  if (!resolved || !name || !c || !chttp_plugin_kind_name(kind)) return SALTS_EINVAL;
  memset(c, 0, sizeof(*c));
  c->kind = kind;
  c->client_adapter = (turbo_flow_chttp_client_config_t)TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
  c->server_adapter = (turbo_flow_chttp_server_config_t)TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
  c->websocket_adapter =
      (turbo_flow_chttp_websocket_server_config_t)TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
  rc = turbo_flow_resolved_config_adapter(resolved, name, &view);
  if (rc != SALTS_OK) return fail(error, rc, name, "kind", "missing adapter configuration");
  if (!view.kind || strcmp(view.kind, chttp_plugin_kind_name(kind)))
    return fail(error, SALTS_EPROTO, name, "kind", "provider kind mismatch");
  for (size_t i = 0u; i < turbo_flow_resolved_adapter_field_count(&view); ++i) {
    const char *key = turbo_flow_resolved_adapter_field_name(&view, i);
    int known = key && (!strcmp(key, "tls_alpn") ||
                        (kind == CHTTP_PLUGIN_CLIENT &&
                         (!strcmp(key, "header_names") || !strcmp(key, "header_values"))));
    for (size_t j = 0u; !known && j < COUNT(fields); ++j)
      known = key && (fields[j].kinds & kind) && !strcmp(key, fields[j].name);
    if (!known) return fail(error, SALTS_EINVAL, name, key ? key : "", "unknown config field");
  }
  for (size_t i = 0u; i < COUNT(fields); ++i) {
    if (!(fields[i].kinds & kind)) continue;
    rc = read_field(&view, &fields[i], c);
    if (rc != SALTS_OK)
      return fail(error, rc, name, fields[i].name,
                  "missing, mistyped or out-of-range required field");
  }
  if (c->schema_version != 1u)
    return fail(error, SALTS_ENOTSUP, name, "schema_version", "unsupported schema");
  rc =
      read_array(&view, "tls_alpn", c->alpn_storage, CHTTP_PLUGIN_ALPN_CAPACITY, &c->alpn_count, 0);
  if (rc != SALTS_OK) return fail(error, rc, name, "tls_alpn", "invalid ALPN array");
  if (kind == CHTTP_PLUGIN_CLIENT) {
    size_t values = 0u;
    rc = read_array(&view, "header_names", c->header_names, CHTTP_PLUGIN_HEADER_CAPACITY,
                    &c->client_adapter.header_count, 0);
    if (rc == SALTS_OK)
      rc = read_array(&view, "header_values", c->header_values, CHTTP_PLUGIN_HEADER_CAPACITY,
                      &values, 1);
    if (rc != SALTS_OK || values != c->client_adapter.header_count)
      return fail(error, rc == SALTS_OK ? SALTS_EINVAL : rc, name, "header_values",
                  "header arrays must match");
  }
  rc = validate_network(c);
  if (rc == SALTS_OK) rc = kind == CHTTP_PLUGIN_CLIENT ? validate_client(c) : validate_server(c);
  return rc == SALTS_OK ? SALTS_OK
                        : fail(error, rc, name, "policy",
                               "inconsistent endpoint, protocol, TLS or capacity policy");
}
