#include "turbo_flow_chttp_typed_config_internal.h"

#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <uri_parser.h>

#if defined(_WIN32)
  #include <ws2tcpip.h>
#else
  #include <arpa/inet.h>
  #include <strings.h>
#endif

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
        message ? message : "CHTTP typed provider policy rejected");
  }
  return status;
}

static int size_add(size_t *value, size_t addend) {
  if (!value || addend > SIZE_MAX - *value) return SALTS_ERANGE;
  *value += addend;
  return SALTS_OK;
}

static int size_mul(size_t left, size_t right, size_t *out) {
  if (!out) return SALTS_EINVAL;
  if (left && right > SIZE_MAX / left) return SALTS_ERANGE;
  *out = left * right;
  return SALTS_OK;
}

static int pow2(size_t value) {
  return value != 0u && (value & (value - 1u)) == 0u;
}

static int text_present(const char *value) {
  return value && value[0] != '\0';
}

static const char *optional_text(const char *value) {
  return text_present(value) ? value : NULL;
}

static int clean_text(const char *text) {
  const unsigned char *cursor;
  if (!text) return 0;
  for (cursor = (const unsigned char *)text; *cursor; ++cursor)
    if (*cursor <= ' ' || *cursor == 127u) return 0;
  return 1;
}

static int header_name_equal(const char *left, const char *right) {
  if (!left || !right) return 0;
#if defined(_WIN32)
  return _stricmp(left, right) == 0;
#else
  return strcasecmp(left, right) == 0;
#endif
}

static int http_token_valid(const char *value) {
  static const char token[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!#$%&'*+-.^_\x60|~";
  return value && value[0] &&
         strspn(value, token) == strlen(value);
}

static int header_value_valid(const char *value) {
  const unsigned char *cursor;
  if (!value) return 0;
  for (cursor = (const unsigned char *)value; *cursor; ++cursor)
    if ((*cursor < ' ' && *cursor != '\t') || *cursor == 127u) return 0;
  return 1;
}

static int header_valid(const char *name, const char *value, int h2) {
  if (!http_token_valid(name) ||
      header_name_equal(name, "host") ||
      header_name_equal(name, "content-length") ||
      header_name_equal(name, "transfer-encoding") ||
      header_name_equal(name, "connection") ||
      (h2 && (header_name_equal(name, "keep-alive") ||
              header_name_equal(name, "upgrade") ||
              header_name_equal(name, "proxy-connection"))))
    return 0;
  return header_value_valid(value);
}

static int body_status(unsigned int status) {
  return status >= HTTP_STATUS_MIN && status <= HTTP_STATUS_MAX &&
         status != HTTP_NO_CONTENT && status != HTTP_RESET_CONTENT &&
         status != HTTP_NOT_MODIFIED;
}

static int deployment_tls_empty(
    const turbo_flow_chttp_deployment_view_t *deployment) {
  return deployment &&
         !text_present(deployment->tls_ca_file) &&
         !text_present(deployment->tls_ca_path) &&
         !text_present(deployment->tls_cert_file) &&
         !text_present(deployment->tls_key_file) &&
         !text_present(deployment->tls_key_password) &&
         !text_present(deployment->tls_server_name) &&
         deployment->alpn_protocol_count == 0u;
}

static int backend_kind(
    CHttpBackend_t value, native_io_backend_kind *out) {
  if (!out) return SALTS_EINVAL;
  switch (value) {
    case CHttpBackend_Iocp:
      *out = NATIVE_IO_BACKEND_IOCP;
      break;
    case CHttpBackend_Epoll:
      *out = NATIVE_IO_BACKEND_EPOLL;
      break;
    case CHttpBackend_Kqueue:
      *out = NATIVE_IO_BACKEND_KQUEUE;
      break;
    case CHttpBackend_IoUring:
      *out = NATIVE_IO_BACKEND_IO_URING;
      break;
    default:
      return SALTS_EINVAL;
  }
  return native_io_backend_kind_supported(*out) ? SALTS_OK : SALTS_ENOTSUP;
}

static int method_kind(CHttpMethod_t value, chttp_method *out) {
  if (!out) return SALTS_EINVAL;
  switch (value) {
    case CHttpMethod_Get: *out = CHTTP_METHOD_GET; return SALTS_OK;
    case CHttpMethod_Post: *out = CHTTP_METHOD_POST; return SALTS_OK;
    case CHttpMethod_Put: *out = CHTTP_METHOD_PUT; return SALTS_OK;
    case CHttpMethod_Delete: *out = CHTTP_METHOD_DELETE; return SALTS_OK;
    case CHttpMethod_Patch: *out = CHTTP_METHOD_PATCH; return SALTS_OK;
    case CHttpMethod_Head: *out = CHTTP_METHOD_HEAD; return SALTS_OK;
    case CHttpMethod_Options: *out = CHTTP_METHOD_OPTIONS; return SALTS_OK;
    default: return SALTS_EINVAL;
  }
}

static size_t method_token_size(CHttpMethod_t value) {
  switch (value) {
    case CHttpMethod_Get: return 3u;
    case CHttpMethod_Post: return 4u;
    case CHttpMethod_Put: return 3u;
    case CHttpMethod_Delete: return 6u;
    case CHttpMethod_Patch: return 5u;
    case CHttpMethod_Head: return 4u;
    case CHttpMethod_Options: return 7u;
    default: return 0u;
  }
}

static int client_protocol(
    CHttpClientProtocol_t value, chttp_protocol *out,
    const char **alpn) {
  if (!out || !alpn) return SALTS_EINVAL;
  switch (value) {
    case CHttpClientProtocol_H1:
      *out = CHTTP_HTTP_1_1;
      *alpn = "http/1.1";
      return SALTS_OK;
    case CHttpClientProtocol_H2:
      *out = CHTTP_HTTP_2;
      *alpn = "h2";
      return SALTS_OK;
    default:
      return SALTS_EINVAL;
  }
}

static int server_protocol(
    CHttpServerProtocol_t value, int *enable_http2) {
  if (!enable_http2) return SALTS_EINVAL;
  switch (value) {
    case CHttpServerProtocol_H1:
      *enable_http2 = 0;
      return SALTS_OK;
    case CHttpServerProtocol_H1H2:
      *enable_http2 = 1;
      return SALTS_OK;
    default:
      return SALTS_EINVAL;
  }
}

static int client_auth(
    CHttpTlsClientAuth_t value, cnet_tls_client_auth *out) {
  if (!out) return SALTS_EINVAL;
  switch (value) {
    case CHttpTlsClientAuth_None:
      *out = CNET_TLS_CLIENT_AUTH_NONE;
      return SALTS_OK;
    case CHttpTlsClientAuth_Required:
      *out = CNET_TLS_CLIENT_AUTH_REQUIRED;
      return SALTS_OK;
    default:
      return SALTS_EINVAL;
  }
}

static int network_validate(
    cnet_client_config *network, int tls_enabled,
    const turbo_flow_chttp_deployment_view_t *deployment) {
  if (!network || !deployment) return SALTS_EINVAL;
  if (!native_io_backend_kind_supported(network->backend))
    return SALTS_ENOTSUP;
  if (!pow2(network->command_capacity) ||
      !pow2(network->event_capacity) ||
      network->event_capacity < 2u ||
      network->connection_capacity > UINT32_MAX / 2u ||
      network->completion_batch_capacity > network->request_capacity ||
      network->command_buffer_bytes < network->max_send_bytes ||
      network->event_buffer_bytes < network->receive_buffer_bytes)
    return SALTS_ERANGE;

  if (tls_enabled) {
    if (network->tls_io_buffer_bytes < CNET_TLS_MIN_IO_BUFFER_BYTES ||
        network->tls_io_buffer_bytes > INT_MAX ||
        network->tls_handshake_timeout_ms == 0u ||
        deployment->alpn_protocol_count == 0u)
      return SALTS_EINVAL;
  } else if (network->tls_io_buffer_bytes ||
             network->tls_handshake_timeout_ms ||
             !deployment_tls_empty(deployment)) {
    return SALTS_EINVAL;
  }

  if (!!text_present(deployment->tls_cert_file) !=
          !!text_present(deployment->tls_key_file) ||
      (text_present(deployment->tls_key_password) &&
       !text_present(deployment->tls_key_file)))
    return SALTS_EINVAL;
  return SALTS_OK;
}

#define CHTTP_TYPED_FILL_NETWORK(OUT, TYPED)                                      \
  do {                                                                             \
    (OUT)->network.connection_capacity = (TYPED)->network_connection_capacity;     \
    (OUT)->network.command_capacity = (TYPED)->network_command_capacity;           \
    (OUT)->network.request_capacity = (TYPED)->network_request_capacity;           \
    (OUT)->network.completion_batch_capacity =                                     \
        (TYPED)->network_completion_batch_capacity;                                \
    (OUT)->network.event_capacity = (TYPED)->network_event_capacity;               \
    (OUT)->network.max_send_bytes = (TYPED)->network_max_send_bytes;               \
    (OUT)->network.receive_buffer_bytes =                                           \
        (TYPED)->network_receive_buffer_bytes;                                      \
    (OUT)->network.command_buffer_bytes =                                           \
        (TYPED)->network_command_buffer_bytes;                                      \
    (OUT)->network.event_buffer_bytes =                                             \
        (TYPED)->network_event_buffer_bytes;                                        \
    (OUT)->network.connect_timeout_ms =                                             \
        (TYPED)->network_connect_timeout_ms;                                        \
    (OUT)->network.read_timeout_ms = (TYPED)->network_read_timeout_ms;              \
    (OUT)->network.write_timeout_ms = (TYPED)->network_write_timeout_ms;            \
    (OUT)->network.tls_handshake_timeout_ms =                                       \
        (TYPED)->network_tls_handshake_timeout_ms;                                  \
    (OUT)->network.tls_io_buffer_bytes =                                            \
        (TYPED)->network_tls_io_buffer_bytes;                                       \
  } while (0)

#define CHTTP_TYPED_FILL_SERVER(OUT, TYPED)                                        \
  do {                                                                             \
    (OUT)->server.backlog = (TYPED)->backlog;                                      \
    (OUT)->server.route_capacity = (TYPED)->route_capacity;                        \
    (OUT)->server.middleware_capacity = (TYPED)->middleware_capacity;              \
    (OUT)->server.max_route_middleware_count =                                     \
        (TYPED)->max_route_middleware_count;                                       \
    (OUT)->server.max_route_param_count = (TYPED)->max_route_param_count;           \
    (OUT)->server.max_route_param_bytes = (TYPED)->max_route_param_bytes;           \
    (OUT)->server.max_target_bytes = (TYPED)->max_target_bytes;                    \
    (OUT)->server.max_header_count = (TYPED)->max_header_count;                    \
    (OUT)->server.max_header_bytes = (TYPED)->max_header_bytes;                    \
    (OUT)->server.max_request_body_bytes = (TYPED)->max_request_body_bytes;        \
    (OUT)->server.max_response_header_count =                                      \
        (TYPED)->max_response_header_count;                                        \
    (OUT)->server.max_response_header_bytes =                                      \
        (TYPED)->max_response_header_bytes;                                        \
    (OUT)->server.max_response_body_bytes =                                        \
        (TYPED)->max_response_body_bytes;                                          \
    (OUT)->server.poll_slice_ms = (TYPED)->poll_slice_ms;                          \
    (OUT)->server.h2_stream_capacity = (TYPED)->h2_stream_capacity;                \
    (OUT)->server.h2_input_buffer_bytes = (TYPED)->h2_input_buffer_bytes;           \
    (OUT)->server.h2_output_buffer_bytes = (TYPED)->h2_output_buffer_bytes;         \
    (OUT)->server.h2_hpack_dynamic_table_bytes =                                   \
        (TYPED)->h2_hpack_dynamic_table_bytes;                                     \
    (OUT)->server.h2_max_settings_count = (TYPED)->h2_max_settings_count;           \
    (OUT)->server.stream_chunk_bytes = (TYPED)->stream_chunk_bytes;                \
    (OUT)->server.max_buffered_response_body_bytes =                               \
        (TYPED)->max_buffered_response_body_bytes;                                 \
    (OUT)->server.buffer_capacity_bytes = (TYPED)->buffer_capacity_bytes;           \
  } while (0)

static int validate_route(const char *path, const chttp_server_config *server) {
  static const char parameter_chars[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
  size_t parameters = 0u;
  size_t bytes = 0u;

  if (!path || !server || path[0] != '/' || strpbrk(path, "?#"))
    return SALTS_EINVAL;

  for (const char *cursor = path; *cursor;) {
    const char *segment = cursor + 1;
    const char *end = strchr(segment, '/');
    size_t length;
    if (!end) end = segment + strlen(segment);
    length = (size_t)(end - segment);
    if (*segment == ':') {
      if (length <= 1u ||
          strspn(segment + 1, parameter_chars) < length - 1u)
        return SALTS_EINVAL;
      ++parameters;
      if (parameters > server->max_route_param_count ||
          bytes > server->max_route_param_bytes ||
          length > server->max_route_param_bytes - bytes)
        return SALTS_ENOBUFS;
      bytes += length;
    } else if (memchr(segment, ':', length)) {
      return SALTS_EINVAL;
    }
    cursor = end;
  }
  return SALTS_OK;
}

static int server_base_validate(
    chttp_typed_runtime_config_t *out,
    CHttpServerProtocol_t protocol,
    CHttpTlsClientAuth_t auth,
    const char *path,
    const turbo_flow_chttp_deployment_view_t *deployment) {
  chttp_server_config *server;
  struct in6_addr address;
  size_t header_block;
  size_t request_block;
  int rc;

  if (!out || !deployment || !path) return SALTS_EINVAL;
  server = &out->server;
  if (!turbo_flow_chttp_deployment_view_valid(deployment) ||
      deployment->kind != TURBO_FLOW_CHTTP_DEPLOYMENT_SERVER ||
      text_present(deployment->tls_server_name))
    return SALTS_EPROTO;

  if (inet_pton(AF_INET, deployment->bind_host, &address) != 1 &&
      inet_pton(AF_INET6, deployment->bind_host, &address) != 1)
    return SALTS_EINVAL;

  rc = validate_route(path, server);
  if (rc != SALTS_OK) return rc;
  rc = server_protocol(protocol, &server->enable_http2);
  if (rc != SALTS_OK) return rc;
  rc = client_auth(auth, &out->tls_server.client_auth);
  if (rc != SALTS_OK) return rc;

  if (!clean_text(deployment->bind_host) ||
      !clean_text(path) ||
      strlen(path) > server->max_target_bytes ||
      server->backlog > INT_MAX ||
      server->stream_chunk_bytes >
          out->network.max_send_bytes - CHUNK_RESERVE ||
      server->max_buffered_response_body_bytes >
          server->max_response_body_bytes ||
      server->buffer_capacity_bytes < out->network.max_send_bytes ||
      (server->max_route_param_count && !server->max_route_param_bytes))
    return SALTS_ERANGE;

  header_block = server->max_buffered_response_body_bytes;
  if (size_add(&header_block, server->max_response_header_bytes) != SALTS_OK ||
      size_add(&header_block, H1_RESPONSE_RESERVE) != SALTS_OK ||
      header_block > out->network.max_send_bytes)
    return SALTS_ERANGE;

  if (!server->enable_http2) {
    if (server->h2_stream_capacity ||
        server->h2_input_buffer_bytes ||
        server->h2_output_buffer_bytes ||
        server->h2_hpack_dynamic_table_bytes ||
        server->h2_max_settings_count)
      return SALTS_EINVAL;
  } else {
    if (size_mul(
            server->max_response_header_count + 2u,
            HPACK_FIELD_OVERHEAD, &header_block) != SALTS_OK ||
        size_add(&header_block, server->max_response_header_bytes) != SALTS_OK ||
        size_add(&header_block, HPACK_GENERATED_RESERVE) != SALTS_OK ||
        size_mul(
            server->max_header_count, HPACK_FIELD_OVERHEAD,
            &request_block) != SALTS_OK ||
        size_add(&request_block, server->max_header_bytes) != SALTS_OK ||
        size_add(&request_block, HPACK_UPDATE_RESERVE) != SALTS_OK)
      return SALTS_ERANGE;
    if (request_block > header_block) header_block = request_block;
    if (!server->h2_stream_capacity ||
        !server->h2_hpack_dynamic_table_bytes ||
        !server->h2_max_settings_count ||
        server->h2_input_buffer_bytes < H2_INPUT_MIN ||
        server->h2_output_buffer_bytes < H2_OUTPUT_MIN ||
        header_block > SIZE_MAX - 9u ||
        server->h2_output_buffer_bytes < header_block + 9u ||
        server->h2_output_buffer_bytes > out->network.max_send_bytes)
      return SALTS_ERANGE;
  }

  if (!out->tls_enabled &&
      out->tls_server.client_auth != CNET_TLS_CLIENT_AUTH_NONE)
    return SALTS_EINVAL;

  if (out->tls_enabled) {
    if (!text_present(deployment->tls_cert_file) ||
        !text_present(deployment->tls_key_file) ||
        (out->tls_server.client_auth == CNET_TLS_CLIENT_AUTH_REQUIRED &&
         !text_present(deployment->tls_ca_file) &&
         !text_present(deployment->tls_ca_path)) ||
        (out->tls_server.client_auth == CNET_TLS_CLIENT_AUTH_NONE &&
         (text_present(deployment->tls_ca_file) ||
          text_present(deployment->tls_ca_path))))
      return SALTS_EINVAL;

    if ((!server->enable_http2 &&
         (deployment->alpn_protocol_count != 1u ||
          strcmp(deployment->alpn_protocols[0], "http/1.1") != 0)) ||
        (server->enable_http2 &&
         (deployment->alpn_protocol_count != 2u ||
          strcmp(deployment->alpn_protocols[0], "h2") != 0 ||
          strcmp(deployment->alpn_protocols[1], "http/1.1") != 0)))
      return SALTS_ENOTSUP;
  }

  out->tls_server.size = sizeof(out->tls_server);
  out->tls_server.ca_file = optional_text(deployment->tls_ca_file);
  out->tls_server.ca_path = optional_text(deployment->tls_ca_path);
  out->tls_server.cert_file = optional_text(deployment->tls_cert_file);
  out->tls_server.key_file = optional_text(deployment->tls_key_file);
  out->tls_server.key_password =
      optional_text(deployment->tls_key_password);
  out->tls_server.alpn_protocols = deployment->alpn_protocols;
  out->tls_server.alpn_protocol_count = deployment->alpn_protocol_count;

  server->network = out->network;
  server->host = deployment->bind_host;
  server->port = deployment->bind_port;
  server->tls = out->tls_enabled ? &out->tls_server : NULL;
  return SALTS_OK;
}

static int runtime_init(
    chttp_typed_runtime_config_t *out, unsigned kind,
    uint32_t poll_budget_ms, uint32_t tls_enabled) {
  if (!out || tls_enabled > 1u) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  out->kind = kind;
  out->poll_budget_ms = poll_budget_ms;
  out->tls_enabled = tls_enabled != 0u;
  out->client_adapter =
      (turbo_flow_chttp_client_config_t)TURBO_FLOW_CHTTP_CLIENT_CONFIG_INIT;
  out->server_adapter =
      (turbo_flow_chttp_server_config_t)TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
  out->websocket_adapter =
      (turbo_flow_chttp_websocket_server_config_t)
          TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
  return SALTS_OK;
}

int chttp_typed_client_config(
    const CHttpClientConfig_t *typed,
    const turbo_flow_chttp_deployment_view_t *deployment,
    const char *instance_name,
    chttp_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error) {
  chttp_client_config *client;
  turbo_flow_chttp_client_config_t *adapter;
  const char *expected_alpn = NULL;
  uri_t uri = {0};
  size_t header_count;
  size_t header_bytes;
  size_t serialized;
  size_t start_line;
  int rc;

  if (!typed || !deployment || !instance_name || !instance_name[0] || !out)
    return typed_fail(
        error, SALTS_EINVAL, instance_name, NULL,
        "invalid CHTTP client typed config arguments");
  if (typed->schema_version != 2u)
    return typed_fail(
        error, SALTS_ENOTSUP, instance_name, "schema_version",
        "unsupported CHTTP client typed config version");
  rc = runtime_init(
      out, CHTTP_TYPED_CLIENT, typed->poll_budget_ms, typed->tls_enabled);
  if (rc != SALTS_OK)
    return typed_fail(error, rc, instance_name, "tls_enabled",
                      "invalid CHTTP client TLS policy");

  rc = backend_kind(typed->backend, &out->network.backend);
  if (rc != SALTS_OK)
    return typed_fail(
        error, rc, instance_name, "backend",
        "requested NativeIO backend is unavailable");
  CHTTP_TYPED_FILL_NETWORK(out, typed);
  rc = network_validate(&out->network, out->tls_enabled, deployment);
  if (rc != SALTS_OK)
    return typed_fail(
        error, rc, instance_name, "network",
        "inconsistent CNet capacity or TLS policy");

  if (!turbo_flow_chttp_deployment_view_valid(deployment) ||
      deployment->kind != TURBO_FLOW_CHTTP_DEPLOYMENT_CLIENT)
    return typed_fail(
        error, SALTS_EPROTO, instance_name, "resource",
        "CHTTP client requires an exact client deployment resource");

  client = &out->client;
  client->network = out->network;
  client->request_capacity = typed->request_capacity;
  client->max_start_line_bytes = typed->max_start_line_bytes;
  client->max_header_count = typed->max_header_count;
  client->max_header_bytes = typed->max_header_bytes;
  client->max_request_body_bytes = typed->max_request_body_bytes;
  client->max_response_body_bytes = typed->max_response_body_bytes;
  client->max_informational_responses = typed->max_informational_responses;
  client->stream_chunk_bytes = typed->stream_chunk_bytes;
  client->h2_input_buffer_bytes = typed->h2_input_buffer_bytes;
  client->h2_hpack_dynamic_table_bytes = typed->h2_hpack_dynamic_table_bytes;
  client->h2_max_settings_count = typed->h2_max_settings_count;

  adapter = &out->client_adapter;
  rc = client_protocol(typed->protocol, &adapter->protocol, &expected_alpn);
  if (rc != SALTS_OK)
    return typed_fail(
        error, rc, instance_name, "protocol",
        "unsupported CHTTP client protocol");
  rc = method_kind(typed->method, &adapter->method);
  if (rc != SALTS_OK)
    return typed_fail(
        error, rc, instance_name, "method",
        "unsupported HTTP method");

  if (!uri_parse(deployment->connection_uri, &uri) ||
      !uri.valid || uri.overflow_flags || !uri.host[0] ||
      strlen(uri.host) >= 256u || uri.path[0] ||
      !(uri.component_flags & URI_COMPONENT_PORT) ||
      uri.port <= 0 || uri.port > UINT16_MAX ||
      (uri.component_flags &
       (URI_COMPONENT_USERINFO | URI_COMPONENT_QUERY |
        URI_COMPONENT_FRAGMENT)) ||
      strcmp(uri.scheme, out->tls_enabled ? "tls" : "tcp") != 0 ||
      !clean_text(typed->authority) ||
      strpbrk(typed->authority, "/?#@") ||
      !typed->target || typed->target[0] != '/' ||
      !clean_text(typed->target))
    return typed_fail(
        error, SALTS_EINVAL, instance_name, "endpoint",
        "invalid CHTTP client endpoint/authority/target policy");

  if (out->tls_enabled &&
      (deployment->alpn_protocol_count != 1u ||
       strcmp(deployment->alpn_protocols[0], expected_alpn) != 0))
    return typed_fail(
        error, SALTS_ENOTSUP, instance_name, "resource",
        "deployment ALPN does not match the CHTTP client protocol");

  if (adapter->protocol == CHTTP_HTTP_2 &&
      out->network.max_send_bytes < H2_OUTPUT_MIN)
    return typed_fail(
        error, SALTS_ERANGE, instance_name, "network_max_send_bytes",
        "HTTP/2 requires a larger CNet send bound");

  if (client->max_start_line_bytes <= 15u ||
      client->max_header_count < 3u ||
      client->h2_input_buffer_bytes < H2_INPUT_MIN ||
      client->stream_chunk_bytes >
          out->network.max_send_bytes - CHUNK_RESERVE)
    return typed_fail(
        error, SALTS_ERANGE, instance_name, "policy",
        "CHTTP client parser/stream bounds are inconsistent");

  start_line = strlen(typed->target);
  if (size_add(&start_line, method_token_size(typed->method)) != SALTS_OK ||
      size_add(&start_line, 12u) != SALTS_OK ||
      start_line > client->max_start_line_bytes)
    return typed_fail(
        error, SALTS_ERANGE, instance_name, "max_start_line_bytes",
        "HTTP request start line exceeds its bound");

  header_bytes = strlen(typed->authority);
  if (size_add(&header_bytes, 8u) != SALTS_OK ||
      header_bytes > client->max_header_bytes)
    return typed_fail(
        error, SALTS_ERANGE, instance_name, "max_header_bytes",
        "HTTP authority exceeds the header bound");

  header_count = CHttpClientConfig_headers_vec_t_size(&typed->headers);
  if (header_count > CHTTP_TYPED_HEADER_CAPACITY)
    return typed_fail(
        error, SALTS_ENOSPC, instance_name, "headers",
        "static HTTP header count exceeds the provider bound");

  for (size_t i = 0u; i < header_count; ++i) {
    const CHttpHeader_t *header =
        CHttpClientConfig_headers_vec_t_at_const(&typed->headers, i);
    size_t bytes;
    if (!header ||
        !header_valid(
            header->name, header->value,
            adapter->protocol == CHTTP_HTTP_2))
      return typed_fail(
          error, SALTS_EINVAL, instance_name, "headers",
          "static HTTP header violates protocol policy");
    bytes = strlen(header->name);
    if (size_add(&bytes, strlen(header->value)) != SALTS_OK ||
        size_add(&bytes, 4u) != SALTS_OK ||
        size_add(&header_bytes, bytes) != SALTS_OK)
      return typed_fail(
          error, SALTS_ERANGE, instance_name, "headers",
          "static HTTP header bytes overflow the provider bound");
    out->headers[i].name = header->name;
    out->headers[i].value = header->value;
  }

  if (header_count + 3u > client->max_header_count ||
      header_bytes > client->max_header_bytes)
    return typed_fail(
        error, SALTS_ERANGE, instance_name, "headers",
        "static HTTP headers exceed configured parser bounds");

  serialized = client->max_start_line_bytes;
  if (size_add(&serialized, client->max_header_bytes) != SALTS_OK ||
      size_add(&serialized, client->max_request_body_bytes) != SALTS_OK ||
      size_add(&serialized, CHUNK_RESERVE) != SALTS_OK ||
      serialized > out->network.max_send_bytes)
    return typed_fail(
        error, SALTS_ERANGE, instance_name, "network_max_send_bytes",
        "serialized HTTP request exceeds the CNet send bound");

  out->tls_client.size = sizeof(out->tls_client);
  out->tls_client.ca_file = optional_text(deployment->tls_ca_file);
  out->tls_client.ca_path = optional_text(deployment->tls_ca_path);
  out->tls_client.cert_file = optional_text(deployment->tls_cert_file);
  out->tls_client.key_file = optional_text(deployment->tls_key_file);
  out->tls_client.key_password =
      optional_text(deployment->tls_key_password);
  out->tls_client.server_name =
      optional_text(deployment->tls_server_name);
  out->tls_client.alpn_protocols = deployment->alpn_protocols;
  out->tls_client.alpn_protocol_count = deployment->alpn_protocol_count;

  adapter->client = client;
  adapter->connection_uri = deployment->connection_uri;
  adapter->authority = typed->authority;
  adapter->target = typed->target;
  adapter->headers = out->headers;
  adapter->header_count = header_count;
  adapter->overall_timeout_ms = typed->overall_timeout_ms;
  adapter->max_attempts = typed->max_attempts;
  adapter->retry_delay_ms = typed->retry_delay_ms;
  adapter->stop_timeout_ms = typed->stop_timeout_ms;
  adapter->idempotent = typed->idempotent != 0u;
  return SALTS_OK;
}

int chttp_typed_server_config(
    const CHttpServerConfig_t *typed,
    const turbo_flow_chttp_deployment_view_t *deployment,
    const char *instance_name,
    chttp_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error) {
  turbo_flow_chttp_server_config_t *adapter;
  size_t text_size;
  int rc;

  if (!typed || !deployment || !instance_name || !instance_name[0] || !out)
    return typed_fail(
        error, SALTS_EINVAL, instance_name, NULL,
        "invalid CHTTP server typed config arguments");
  if (typed->schema_version != 2u)
    return typed_fail(
        error, SALTS_ENOTSUP, instance_name, "schema_version",
        "unsupported CHTTP server typed config version");
  rc = runtime_init(
      out, CHTTP_TYPED_SERVER, typed->poll_budget_ms, typed->tls_enabled);
  if (rc != SALTS_OK)
    return typed_fail(error, rc, instance_name, "tls_enabled",
                      "invalid CHTTP server TLS policy");

  rc = backend_kind(typed->backend, &out->network.backend);
  if (rc != SALTS_OK)
    return typed_fail(
        error, rc, instance_name, "backend",
        "requested NativeIO backend is unavailable");
  CHTTP_TYPED_FILL_NETWORK(out, typed);
  rc = network_validate(&out->network, out->tls_enabled, deployment);
  if (rc != SALTS_OK)
    return typed_fail(
        error, rc, instance_name, "network",
        "inconsistent CNet capacity or TLS policy");

  CHTTP_TYPED_FILL_SERVER(out, typed);
  rc = server_base_validate(
      out, typed->protocol, typed->tls_client_auth,
      typed->path, deployment);
  if (rc != SALTS_OK)
    return typed_fail(
        error, rc, instance_name, "policy",
        "inconsistent CHTTP server endpoint/TLS/capacity policy");

  adapter = &out->server_adapter;
  rc = method_kind(typed->method, &adapter->method);
  if (rc != SALTS_OK)
    return typed_fail(
        error, rc, instance_name, "method", "unsupported HTTP method");

  adapter->max_request_message_bytes = typed->max_request_message_bytes;
  adapter->success_status = typed->success_status;
  adapter->overload_status = typed->overload_status;
  adapter->unavailable_status = typed->unavailable_status;
  adapter->graph_error_status = typed->graph_error_status;
  adapter->first_message_id = typed->first_message_id;
  adapter->stop_timeout_ms = typed->stop_timeout_ms;

  if (adapter->success_status < HTTP_STATUS_MIN ||
      adapter->success_status > HTTP_STATUS_MAX ||
      !body_status(adapter->overload_status) ||
      !body_status(adapter->unavailable_status) ||
      !body_status(adapter->graph_error_status) ||
      !header_value_valid(typed->response_content_type) ||
      !header_value_valid(typed->error_content_type) ||
      out->server.max_buffered_response_body_bytes <
          sizeof("request too large") - 1u ||
      strlen(typed->graph_error_body) >
          out->server.max_buffered_response_body_bytes ||
      adapter->max_request_message_bytes <
          out->server.max_request_body_bytes)
    return typed_fail(
        error, SALTS_ERANGE, instance_name, "policy",
        "CHTTP server response/admission bounds are inconsistent");

  text_size = strlen(typed->response_content_type);
  if (size_add(&text_size, 12u) != SALTS_OK ||
      text_size > out->server.max_response_header_bytes)
    return typed_fail(
        error, SALTS_ERANGE, instance_name, "response_content_type",
        "response content type exceeds response header bound");
  text_size = strlen(typed->error_content_type);
  if (size_add(&text_size, 12u) != SALTS_OK ||
      text_size > out->server.max_response_header_bytes)
    return typed_fail(
        error, SALTS_ERANGE, instance_name, "error_content_type",
        "error content type exceeds response header bound");

  adapter->server = &out->server;
  adapter->path = typed->path;
  adapter->response_content_type = typed->response_content_type;
  adapter->error_content_type = typed->error_content_type;
  adapter->graph_error_body = typed->graph_error_body;
  adapter->graph_error_body_size = strlen(typed->graph_error_body);
  return SALTS_OK;
}

int chttp_typed_websocket_config(
    const CHttpWebSocketServerConfig_t *typed,
    const turbo_flow_chttp_deployment_view_t *deployment,
    const char *instance_name,
    chttp_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error) {
  turbo_flow_chttp_websocket_server_config_t *adapter;
  size_t wire;
  size_t sessions;
  int rc;

  if (!typed || !deployment || !instance_name || !instance_name[0] || !out)
    return typed_fail(
        error, SALTS_EINVAL, instance_name, NULL,
        "invalid CHTTP WebSocket typed config arguments");
  if (typed->schema_version != 2u)
    return typed_fail(
        error, SALTS_ENOTSUP, instance_name, "schema_version",
        "unsupported CHTTP WebSocket typed config version");
  rc = runtime_init(
      out, CHTTP_TYPED_WEBSOCKET, typed->poll_budget_ms,
      typed->tls_enabled);
  if (rc != SALTS_OK)
    return typed_fail(error, rc, instance_name, "tls_enabled",
                      "invalid CHTTP WebSocket TLS policy");

  rc = backend_kind(typed->backend, &out->network.backend);
  if (rc != SALTS_OK)
    return typed_fail(
        error, rc, instance_name, "backend",
        "requested NativeIO backend is unavailable");
  CHTTP_TYPED_FILL_NETWORK(out, typed);
  rc = network_validate(&out->network, out->tls_enabled, deployment);
  if (rc != SALTS_OK)
    return typed_fail(
        error, rc, instance_name, "network",
        "inconsistent CNet capacity or TLS policy");

  CHTTP_TYPED_FILL_SERVER(out, typed);
  rc = server_base_validate(
      out, typed->protocol, typed->tls_client_auth,
      typed->path, deployment);
  if (rc != SALTS_OK)
    return typed_fail(
        error, rc, instance_name, "policy",
        "inconsistent WebSocket server endpoint/TLS/capacity policy");

  adapter = &out->websocket_adapter;
  adapter->session_capacity = typed->session_capacity;
  adapter->frame_capacity = typed->frame_capacity;
  adapter->max_frame_bytes = typed->max_frame_bytes;
  adapter->max_message_bytes = typed->max_message_bytes;
  adapter->max_buffered_input_bytes = typed->max_buffered_input_bytes;
  adapter->first_message_id = typed->first_message_id;
  adapter->stop_timeout_ms = typed->stop_timeout_ms;

  wire = adapter->max_frame_bytes;
  if (size_add(
          &wire, TURBO_FLOW_CHTTP_WEBSOCKET_MAX_WIRE_HEADER_BYTES) != SALTS_OK ||
      size_mul(
          out->network.connection_capacity,
          out->server.enable_http2 ? out->server.h2_stream_capacity : 1u,
          &sessions) != SALTS_OK ||
      adapter->max_frame_bytes < TURBO_FLOW_CHTTP_WEBSOCKET_MIN_FRAME_BYTES ||
      adapter->max_message_bytes < adapter->max_frame_bytes ||
      wire > out->network.max_send_bytes ||
      adapter->max_buffered_input_bytes < wire ||
      adapter->session_capacity > sessions ||
      (text_present(typed->subprotocol) &&
       !http_token_valid(typed->subprotocol)))
    return typed_fail(
        error, SALTS_ERANGE, instance_name, "policy",
        "WebSocket frame/session bounds are inconsistent");

  adapter->server = &out->server;
  adapter->path = typed->path;
  adapter->subprotocol = optional_text(typed->subprotocol);
  return SALTS_OK;
}

#undef CHTTP_TYPED_FILL_SERVER
#undef CHTTP_TYPED_FILL_NETWORK
