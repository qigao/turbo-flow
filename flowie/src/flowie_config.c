#include "flowie.h"

#include "turbo_error.h"
#include "turbo_flow_coronet_execution.h"

#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

enum {
  FLOWIE_CONFIG_IPV6_TEXT_CAPACITY = 46,
  FLOWIE_CONFIG_PROXY_CIDR_CAPACITY = FLOWIE_CONFIG_IPV6_TEXT_CAPACITY + 4
};

static int flowie_config_proxy_cidrs_split(
    const char *text,
    char storage[FLOWIE_ENDPOINT_PROXY_MAX_TRUSTED_PEERS][FLOWIE_CONFIG_PROXY_CIDR_CAPACITY],
    const char *items[FLOWIE_ENDPOINT_PROXY_MAX_TRUSTED_PEERS], size_t *out_count) {
  const char *cursor;
  size_t count = 0u;

  if (out_count) *out_count = 0u;
  if (!text || !text[0] || !storage || !items || !out_count) return TURBO_EINVAL;
  cursor = text;
  for (;;) {
    const char *begin;
    const char *end;
    size_t length;
    while (*cursor == ' ' || *cursor == '\t')
      ++cursor;
    begin = cursor;
    while (*cursor != '\0' && *cursor != ',')
      ++cursor;
    end = cursor;
    while (end > begin && (end[-1] == ' ' || end[-1] == '\t'))
      --end;
    length = (size_t)(end - begin);
    if (length == 0u || length >= FLOWIE_CONFIG_PROXY_CIDR_CAPACITY ||
        count >= FLOWIE_ENDPOINT_PROXY_MAX_TRUSTED_PEERS) {
      return TURBO_EINVAL;
    }
    memcpy(storage[count], begin, length);
    storage[count][length] = '\0';
    items[count] = storage[count];
    ++count;
    if (*cursor == '\0') break;
    ++cursor;
    if (*cursor == '\0') return TURBO_EINVAL;
  }
  *out_count = count;
  return TURBO_OK;
}

typedef enum flowie_config_field_type_e {
  FLOWIE_CONFIG_STRING = 1,
  FLOWIE_CONFIG_BOOL,
  FLOWIE_CONFIG_U16,
  FLOWIE_CONFIG_U32,
  FLOWIE_CONFIG_INT,
  FLOWIE_CONFIG_SIZE,
  FLOWIE_CONFIG_U64,
  FLOWIE_CONFIG_TRANSPORT,
  FLOWIE_CONFIG_SETTLEMENT,
  FLOWIE_CONFIG_SLOW_SUBSCRIBER_POLICY
} flowie_config_field_type_t;

typedef struct flowie_config_field_s {
  const char *name;
  flowie_config_field_type_t type;
  size_t offset;
  uint64_t maximum;
} flowie_config_field_t;

#define FLOWIE_CONFIG_FIELD(member, field_type, max_value)                                         \
  {#member, field_type, offsetof(flowie_endpoint_config_t, member), max_value}

#define FLOWIE_CONFIG_SETTLEMENT_FIELD(name, member)                                               \
  {name, FLOWIE_CONFIG_SETTLEMENT,                                                                 \
   offsetof(flowie_endpoint_config_t, settlement) +                                                \
       offsetof(turbo_flow_protocol_settlement_policy_t, member),                                  \
   0u}

static const flowie_config_field_t FLOWIE_CONFIG_FIELDS[] = {
    FLOWIE_CONFIG_FIELD(transport, FLOWIE_CONFIG_TRANSPORT, 0u),
    FLOWIE_CONFIG_SETTLEMENT_FIELD("settlement_qos1", qos1),
    FLOWIE_CONFIG_SETTLEMENT_FIELD("settlement_qos2", qos2),
    FLOWIE_CONFIG_FIELD(host, FLOWIE_CONFIG_STRING, 0u),
    FLOWIE_CONFIG_FIELD(port, FLOWIE_CONFIG_INT, 65535u),
    FLOWIE_CONFIG_FIELD(path, FLOWIE_CONFIG_STRING, 0u),
    FLOWIE_CONFIG_FIELD(max_packet_size, FLOWIE_CONFIG_SIZE, FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE),
    FLOWIE_CONFIG_FIELD(max_connections, FLOWIE_CONFIG_U32, FLOWIE_MAX_CONNECTIONS_LIMIT),
    FLOWIE_CONFIG_FIELD(coroutine_stack_size, FLOWIE_CONFIG_SIZE, FLOWIE_MAX_COROUTINE_STACK_SIZE),
    FLOWIE_CONFIG_FIELD(stream_recv_buffer_bytes, FLOWIE_CONFIG_SIZE, FLOWIE_MAX_RECV_BUFFER_SIZE),
    FLOWIE_CONFIG_FIELD(socket_recv_buffer_bytes, FLOWIE_CONFIG_SIZE, INT_MAX),
    FLOWIE_CONFIG_FIELD(socket_send_buffer_bytes, FLOWIE_CONFIG_SIZE, INT_MAX),
    FLOWIE_CONFIG_FIELD(tls_client_ca_file, FLOWIE_CONFIG_STRING, 0u),
    FLOWIE_CONFIG_FIELD(timeout_ms, FLOWIE_CONFIG_U64, UINT64_MAX),
    FLOWIE_CONFIG_FIELD(recv_timeout_ms, FLOWIE_CONFIG_U64, UINT64_MAX),
    FLOWIE_CONFIG_FIELD(reuse_port, FLOWIE_CONFIG_BOOL, 0u),
    FLOWIE_CONFIG_FIELD(tcp_keepalive, FLOWIE_CONFIG_BOOL, 0u),
    FLOWIE_CONFIG_FIELD(tcp_keepalive_idle_ms, FLOWIE_CONFIG_U64, UINT64_MAX),
    FLOWIE_CONFIG_FIELD(tcp_keepalive_interval_ms, FLOWIE_CONFIG_U64, UINT64_MAX),
    FLOWIE_CONFIG_FIELD(tcp_keepalive_count, FLOWIE_CONFIG_U32, UINT32_MAX),
    FLOWIE_CONFIG_FIELD(linger, FLOWIE_CONFIG_BOOL, 0u),
    FLOWIE_CONFIG_FIELD(linger_ms, FLOWIE_CONFIG_U64, UINT64_MAX),
    FLOWIE_CONFIG_FIELD(send_hwm_bytes, FLOWIE_CONFIG_SIZE, SIZE_MAX),
    FLOWIE_CONFIG_FIELD(manage_sessions, FLOWIE_CONFIG_BOOL, 0u),
    FLOWIE_CONFIG_FIELD(max_sessions, FLOWIE_CONFIG_SIZE, SIZE_MAX),
    FLOWIE_CONFIG_FIELD(max_subscriptions_per_session, FLOWIE_CONFIG_SIZE, UINT16_MAX),
    FLOWIE_CONFIG_FIELD(max_inflight_per_session, FLOWIE_CONFIG_SIZE, UINT16_MAX),
    FLOWIE_CONFIG_FIELD(max_retained_messages, FLOWIE_CONFIG_SIZE, SIZE_MAX),
    FLOWIE_CONFIG_FIELD(topic_alias_maximum, FLOWIE_CONFIG_U16, UINT16_MAX),
    FLOWIE_CONFIG_FIELD(slow_subscriber_policy, FLOWIE_CONFIG_SLOW_SUBSCRIBER_POLICY, 0u)};

static int flowie_config_error(turbo_flow_config_error_t *error, int status, const char *name,
                               const char *field, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field) {
      (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.config.%s", name, field);
    } else {
      (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s", name ? name : "?");
    }
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static const flowie_config_field_t *flowie_config_field_find(const char *name) {
  for (size_t i = 0u; i < sizeof(FLOWIE_CONFIG_FIELDS) / sizeof(FLOWIE_CONFIG_FIELDS[0]); ++i) {
    if (strcmp(FLOWIE_CONFIG_FIELDS[i].name, name) == 0) return &FLOWIE_CONFIG_FIELDS[i];
  }
  return NULL;
}

static int flowie_config_assign(flowie_endpoint_config_t *config,
                                const turbo_flow_resolved_adapter_view_t *view,
                                const flowie_config_field_t *field) {
  unsigned char *target;
  uint64_t number;
  int rc;
  if (!config || !view || !field) return TURBO_EINVAL;
  target = (unsigned char *)config + field->offset;
  if (field->type == FLOWIE_CONFIG_STRING) {
    return turbo_flow_resolved_adapter_get_string(view, field->name, (const char **)target);
  }
  if (field->type == FLOWIE_CONFIG_BOOL) {
    return turbo_flow_resolved_adapter_get_bool(view, field->name, (int *)target);
  }
  if (field->type == FLOWIE_CONFIG_TRANSPORT) {
    const char *text;
    rc = turbo_flow_resolved_adapter_get_string(view, field->name, &text);
    if (rc != TURBO_OK) return rc;
    if (strcmp(text, "tcp") == 0) config->transport = FLOWIE_TRANSPORT_TCP;
    else if (strcmp(text, "tls") == 0) config->transport = FLOWIE_TRANSPORT_TLS;
    else if (strcmp(text, "ws") == 0) config->transport = FLOWIE_TRANSPORT_WS;
    else if (strcmp(text, "wss") == 0) config->transport = FLOWIE_TRANSPORT_WSS;
    else if (strcmp(text, "pipe") == 0) config->transport = FLOWIE_TRANSPORT_PIPE;
    else return TURBO_EINVAL;
    return TURBO_OK;
  }
  if (field->type == FLOWIE_CONFIG_SETTLEMENT) {
    const char *text;
    rc = turbo_flow_resolved_adapter_get_string(view, field->name, &text);
    if (rc != TURBO_OK) return rc;
    if (strcmp(text, "received") == 0)
      *(turbo_flow_protocol_settlement_point_t *)target = TURBO_FLOW_PROTOCOL_SETTLE_RECEIVED;
    else if (strcmp(text, "accepted") == 0)
      *(turbo_flow_protocol_settlement_point_t *)target = TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED;
    else if (strcmp(text, "processed") == 0)
      *(turbo_flow_protocol_settlement_point_t *)target = TURBO_FLOW_PROTOCOL_SETTLE_PROCESSED;
    else if (strcmp(text, "durable") == 0)
      *(turbo_flow_protocol_settlement_point_t *)target = TURBO_FLOW_PROTOCOL_SETTLE_DURABLE;
    else return TURBO_ENOTSUP;
    return TURBO_OK;
  }
  if (field->type == FLOWIE_CONFIG_SLOW_SUBSCRIBER_POLICY) {
    const char *text;
    rc = turbo_flow_resolved_adapter_get_string(view, field->name, &text);
    if (rc != TURBO_OK) return rc;
    if (strcmp(text, "disconnect") != 0) return TURBO_ENOTSUP;
    *(flowie_slow_subscriber_policy_t *)target = FLOWIE_SLOW_SUBSCRIBER_DISCONNECT;
    return TURBO_OK;
  }
  rc = turbo_flow_resolved_adapter_get_u64(view, field->name, &number);
  if (rc != TURBO_OK) return rc;
  if (number > field->maximum) return TURBO_ERANGE;
  if (strcmp(field->name, "coroutine_stack_size") == 0 && number < FLOWIE_MIN_COROUTINE_STACK_SIZE)
    return TURBO_ERANGE;
  if (strcmp(field->name, "stream_recv_buffer_bytes") == 0 && number < FLOWIE_MIN_RECV_BUFFER_SIZE)
    return TURBO_ERANGE;
  if (number == 0u &&
      (strcmp(field->name, "max_packet_size") == 0 || strcmp(field->name, "max_connections") == 0 ||
       strcmp(field->name, "max_sessions") == 0 ||
       strcmp(field->name, "max_subscriptions_per_session") == 0 ||
       strcmp(field->name, "max_inflight_per_session") == 0 ||
       strcmp(field->name, "max_retained_messages") == 0)) {
    return TURBO_ERANGE;
  }
  switch (field->type) {
  case FLOWIE_CONFIG_U16:
    *(uint16_t *)target = (uint16_t)number;
    return TURBO_OK;
  case FLOWIE_CONFIG_U32:
    *(uint32_t *)target = (uint32_t)number;
    return TURBO_OK;
  case FLOWIE_CONFIG_INT:
    if (number > INT_MAX) return TURBO_ERANGE;
    *(int *)target = (int)number;
    return TURBO_OK;
  case FLOWIE_CONFIG_SIZE:
    *(size_t *)target = (size_t)number;
    return TURBO_OK;
  case FLOWIE_CONFIG_U64:
    *(uint64_t *)target = number;
    return TURBO_OK;
  default:
    return TURBO_EINVAL;
  }
}

static int flowie_register_resolved_endpoint_internal(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_coronet_execution_binding_t *execution,
    const flowie_endpoint_security_binding_t *security,
    const flowie_endpoint_persistence_binding_t *persistence,
    const flowie_endpoint_proxy_binding_t *injected_proxy,
    const flowie_endpoint_cluster_binding_t *cluster, turbo_flow_config_error_t *error) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  flowie_endpoint_proxy_binding_t resolved_proxy = FLOWIE_ENDPOINT_PROXY_BINDING_INIT;
  char proxy_cidr_storage[FLOWIE_ENDPOINT_PROXY_MAX_TRUSTED_PEERS]
                         [FLOWIE_CONFIG_PROXY_CIDR_CAPACITY];
  const char *proxy_cidrs[FLOWIE_ENDPOINT_PROXY_MAX_TRUSTED_PEERS];
  const char *proxy_cidrs_text = NULL;
  uint64_t proxy_header_max_bytes = 0u;
  uint64_t proxy_header_timeout_ms = 0u;
  int proxy_cidrs_seen = 0;
  int proxy_max_seen = 0;
  int proxy_timeout_seen = 0;
  int security_realm_seen = 0;
  int auth_method_seen = 0;
  int protocol_store_seen = 0;
  int legacy_session_store_seen = 0;
  int rc;
  if (!flow || !name || !name[0] || !resolved || !execution || !error ||
      error->size < sizeof(*error)) {
    return TURBO_EINVAL;
  }
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  if (security && (security->size < sizeof(*security) || !security->realm_channel ||
                   !security->realm_channel[0] || !security->auth_method ||
                   !security->auth_method[0] || !security->auth_provider || !security->realm)) {
    return flowie_config_error(error, TURBO_EINVAL, name, NULL,
                               "security binding is incomplete or has an invalid ABI size");
  }
  if (persistence && (persistence->size < sizeof(*persistence) || !persistence->store_channel ||
                      !persistence->store_channel[0] || !persistence->store)) {
    return flowie_config_error(error, TURBO_EINVAL, name, NULL,
                               "persistence binding is incomplete or has an invalid ABI size");
  }
  if (injected_proxy && injected_proxy->size != sizeof(*injected_proxy)) {
    return flowie_config_error(error, TURBO_EINVAL, name, NULL,
                               "proxy binding has an invalid ABI size");
  }
  if (cluster &&
      (cluster->size < sizeof(*cluster) ||
       cluster->abi_version != FLOWIE_ENDPOINT_CLUSTER_BINDING_ABI_CURRENT || !cluster->ctx ||
       cluster->request_timeout_ms == 0u || !cluster->connect || !cluster->command ||
       !cluster->settle || !cluster->connection_lost || !cluster->detach)) {
    return flowie_config_error(error, TURBO_EINVAL, name, NULL,
                               "cluster binding is incomplete or has an invalid ABI");
  }
  rc = turbo_flow_resolved_config_adapter(resolved, name, &view);
  if (rc != TURBO_OK) {
    return flowie_config_error(error, rc, name, NULL,
                               rc == TURBO_ENOENT ? "adapter is not resolved"
                                                  : "resolved adapter is invalid");
  }
  if (strcmp(view.kind, "flowie_endpoint") != 0) {
    return flowie_config_error(error, TURBO_EINVAL, name, NULL,
                               "adapter kind must be flowie_endpoint");
  }
  for (size_t i = 0u; i < turbo_flow_resolved_adapter_field_count(&view); ++i) {
    const char *field_name = turbo_flow_resolved_adapter_field_name(&view, i);
    const flowie_config_field_t *field = field_name ? flowie_config_field_find(field_name) : NULL;
    if (field_name &&
        (strcmp(field_name, "security_realm") == 0 || strcmp(field_name, "auth_method") == 0)) {
      const char *value = NULL;
      const char *expected = NULL;
      if (!security) {
        return flowie_config_error(error, TURBO_EINVAL, name, field_name,
                                   "secure field requires an explicit security binding");
      }
      rc = turbo_flow_resolved_adapter_get_string(&view, field_name, &value);
      expected = strcmp(field_name, "security_realm") == 0 ? security->realm_channel
                                                           : security->auth_method;
      if (rc != TURBO_OK || !expected || strcmp(value, expected) != 0) {
        return flowie_config_error(error, rc == TURBO_OK ? TURBO_EINVAL : rc, name, field_name,
                                   "secure field does not match the injected binding");
      }
      if (strcmp(field_name, "security_realm") == 0) security_realm_seen = 1;
      else auth_method_seen = 1;
      continue;
    }
    if (field_name &&
        (strcmp(field_name, "protocol_store") == 0 || strcmp(field_name, "session_store") == 0)) {
      const char *value = NULL;
      if (!persistence) {
        return flowie_config_error(error, TURBO_EINVAL, name, field_name,
                                   "protocol store requires an explicit persistence binding");
      }
      rc = turbo_flow_resolved_adapter_get_string(&view, field_name, &value);
      if (rc != TURBO_OK || strcmp(value, persistence->store_channel) != 0) {
        return flowie_config_error(error, rc == TURBO_OK ? TURBO_EINVAL : rc, name, field_name,
                                   "protocol store does not match the injected binding");
      }
      if (strcmp(field_name, "protocol_store") == 0) protocol_store_seen = 1;
      else legacy_session_store_seen = 1;
      if (protocol_store_seen && legacy_session_store_seen) {
        return flowie_config_error(
            error, TURBO_EINVAL, name, field_name,
            "protocol_store and legacy session_store are mutually exclusive");
      }
      continue;
    }
    if (field_name && strcmp(field_name, "trusted_proxy_cidrs") == 0) {
      rc = turbo_flow_resolved_adapter_get_string(&view, field_name, &proxy_cidrs_text);
      if (rc != TURBO_OK || !proxy_cidrs_text || !proxy_cidrs_text[0])
        return flowie_config_error(error, rc == TURBO_OK ? TURBO_EINVAL : rc, name, field_name,
                                   "trusted proxy CIDRs are invalid");
      proxy_cidrs_seen = 1;
      continue;
    }
    if (field_name && strcmp(field_name, "proxy_header_max_bytes") == 0) {
      rc = turbo_flow_resolved_adapter_get_u64(&view, field_name, &proxy_header_max_bytes);
      if (rc != TURBO_OK || proxy_header_max_bytes > SIZE_MAX)
        return flowie_config_error(error, rc == TURBO_OK ? TURBO_ERANGE : rc, name, field_name,
                                   "proxy header limit is invalid");
      proxy_max_seen = 1;
      continue;
    }
    if (field_name && strcmp(field_name, "proxy_header_timeout_ms") == 0) {
      rc = turbo_flow_resolved_adapter_get_u64(&view, field_name, &proxy_header_timeout_ms);
      if (rc != TURBO_OK || proxy_header_timeout_ms == 0u)
        return flowie_config_error(error, rc == TURBO_OK ? TURBO_ERANGE : rc, name, field_name,
                                   "proxy header timeout is invalid");
      proxy_timeout_seen = 1;
      continue;
    }
    if (!field) {
      return flowie_config_error(error, TURBO_EINVAL, name, field_name,
                                 "unknown Flowie endpoint field");
    }
    rc = flowie_config_assign(&config, &view, field);
    if (rc != TURBO_OK) {
      return flowie_config_error(error, rc, name, field_name,
                                 "invalid Flowie endpoint field value");
    }
  }
  if (security && (!security_realm_seen || !auth_method_seen)) {
    return flowie_config_error(error, TURBO_EINVAL, name,
                               !security_realm_seen ? "security_realm" : "auth_method",
                               "secure endpoint requires both security fields");
  }
  if (persistence && !protocol_store_seen && !legacy_session_store_seen &&
      strcmp(persistence->store_channel, FLOWIE_IMPLICIT_PROTOCOL_STORE_CHANNEL) != 0) {
    return flowie_config_error(error, TURBO_EINVAL, name, "protocol_store",
                               "persistent endpoint requires protocol_store");
  }
  if (proxy_cidrs_seen || proxy_max_seen || proxy_timeout_seen) {
    if (!proxy_cidrs_seen || !proxy_max_seen || !proxy_timeout_seen || injected_proxy) {
      return flowie_config_error(
          error, TURBO_EINVAL, name, "trusted_proxy_cidrs",
          injected_proxy ? "resolved and injected proxy bindings cannot be combined"
                         : "trusted proxy configuration requires CIDRs, limit, and timeout");
    }
    rc = flowie_config_proxy_cidrs_split(proxy_cidrs_text, proxy_cidr_storage, proxy_cidrs,
                                         &resolved_proxy.trusted_peer_count);
    if (rc != TURBO_OK)
      return flowie_config_error(error, rc, name, "trusted_proxy_cidrs",
                                 "trusted proxy CIDR list is invalid");
    resolved_proxy.trusted_peer_cidrs = proxy_cidrs;
    resolved_proxy.max_header_bytes = (size_t)proxy_header_max_bytes;
    resolved_proxy.header_timeout_ms = proxy_header_timeout_ms;
    injected_proxy = &resolved_proxy;
  }
  if (security || persistence || injected_proxy || cluster) {
    flowie_endpoint_bindings_t bindings = FLOWIE_ENDPOINT_BINDINGS_INIT;
    bindings.security = security;
    bindings.persistence = persistence;
    bindings.proxy = injected_proxy;
    bindings.cluster = cluster;
    rc = flowie_register_bound_endpoint_ex(flow, name, &config, execution, &bindings);
  } else {
    rc = flowie_register_endpoint_ex(flow, name, &config, execution);
  }
  if (rc != TURBO_OK) {
    rc = flowie_config_error(error, rc, name, NULL,
                             "Flowie endpoint configuration validation failed");
  }
  return rc;
}

int flowie_register_resolved_endpoint_ex(turbo_flow_t *flow, const char *name,
                                         const turbo_flow_resolved_config_t *resolved,
                                         const turbo_flow_coronet_execution_binding_t *execution,
                                         turbo_flow_config_error_t *error) {
  return flowie_register_resolved_endpoint_internal(flow, name, resolved, execution, NULL, NULL,
                                                    NULL, NULL, error);
}

int flowie_register_resolved_secure_endpoint_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_coronet_execution_binding_t *execution,
    const flowie_endpoint_security_binding_t *security, turbo_flow_config_error_t *error) {
  return flowie_register_resolved_endpoint_internal(flow, name, resolved, execution, security, NULL,
                                                    NULL, NULL, error);
}

int flowie_register_resolved_bound_endpoint_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_resolved_config_t *resolved,
    const turbo_flow_coronet_execution_binding_t *execution,
    const flowie_endpoint_bindings_t *bindings, turbo_flow_config_error_t *error) {
  const flowie_endpoint_proxy_binding_t *proxy;
  const flowie_endpoint_cluster_binding_t *cluster;
  if (!bindings || bindings->size < FLOWIE_ENDPOINT_BINDINGS_V1_SIZE) return TURBO_EINVAL;
  proxy = bindings->size >= FLOWIE_ENDPOINT_BINDINGS_V2_SIZE ? bindings->proxy : NULL;
  cluster = bindings->size >= FLOWIE_ENDPOINT_BINDINGS_V3_SIZE ? bindings->cluster : NULL;
  if (!bindings->security && !bindings->persistence && !proxy && !cluster) return TURBO_EINVAL;
  return flowie_register_resolved_endpoint_internal(flow, name, resolved, execution,
                                                    bindings->security, bindings->persistence,
                                                    proxy, cluster, error);
}

int flowie_register_resolved_endpoint(turbo_flow_t *flow, const char *name,
                                      const turbo_flow_resolved_config_t *resolved,
                                      turbo_flow_config_error_t *error) {
  turbo_flow_coronet_execution_binding_t execution;
  memset(&execution, 0, sizeof(execution));
  execution.size = sizeof(execution);
  execution.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  return flowie_register_resolved_endpoint_ex(flow, name, resolved, &execution, error);
}

int flowie_register_resolved_secure_endpoint(turbo_flow_t *flow, const char *name,
                                             const turbo_flow_resolved_config_t *resolved,
                                             const flowie_endpoint_security_binding_t *security,
                                             turbo_flow_config_error_t *error) {
  turbo_flow_coronet_execution_binding_t execution;
  memset(&execution, 0, sizeof(execution));
  execution.size = sizeof(execution);
  execution.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  return flowie_register_resolved_secure_endpoint_ex(flow, name, resolved, &execution, security,
                                                     error);
}

int flowie_register_resolved_bound_endpoint(turbo_flow_t *flow, const char *name,
                                            const turbo_flow_resolved_config_t *resolved,
                                            const flowie_endpoint_bindings_t *bindings,
                                            turbo_flow_config_error_t *error) {
  turbo_flow_coronet_execution_binding_t execution;
  memset(&execution, 0, sizeof(execution));
  execution.size = sizeof(execution);
  execution.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  return flowie_register_resolved_bound_endpoint_ex(flow, name, resolved, &execution, bindings,
                                                    error);
}
