#include "socket.h"

#include "CoroNet/turbo_coro_socket.h"
#include "flow_connection.h"
#include "flow_coronet_execution.h"
#include "flow_coronet_runtime.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *const FLOW_SOCKET_ROLE_VALUES[] = {"source", "sink"};
static const turbo_flow_option_field_t FLOW_SOCKET_OPTION_FIELDS[] = {
    {"role", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0, FLOW_SOCKET_ROLE_VALUES, 2},
    {"transport", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0,
     TF_CORONET_TRANSPORT_VALUES, TF_CORONET_TRANSPORT_VALUE_COUNT},
    {"kcp_pre_shared_key", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"kcp_mtu", TURBO_FLOW_OPTION_U32, TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 576,
     UINT16_MAX, NULL, 0},
    {"kcp_send_window", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, UINT16_MAX, NULL, 0},
    {"kcp_receive_window", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, UINT16_MAX, NULL, 0},
    {"kcp_interval_ms", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 100, NULL, 0},
    {"kcp_handshake_retry_ms", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, UINT16_MAX, NULL, 0},
    {"kcp_fast_resend", TURBO_FLOW_OPTION_U32, TURBO_FLOW_OPTION_HAS_MAX, 0, UINT8_MAX, NULL, 0},
    {"kcp_congestion_control", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"kcp_fec_data_shards", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 255, NULL, 0},
    {"kcp_fec_parity_shards", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 255, NULL, 0},
    {"kcp_fec_max_payload_size", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, UINT16_MAX, NULL, 0},
    {"kcp_fec_receive_groups", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 64, NULL, 0},
    {"host", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"port", TURBO_FLOW_OPTION_U32, TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 65535,
     NULL, 0},
    {"path", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {TF_CORONET_OPTION_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {TF_CORONET_OPTION_CONNECT_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {TF_CORONET_OPTION_SEND_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {TF_CORONET_OPTION_RECV_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {TF_CORONET_OPTION_HANDSHAKE_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"reuse_port", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"tcp_keepalive", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"tcp_keepalive_idle_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"tcp_keepalive_interval_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"tcp_keepalive_count", TURBO_FLOW_OPTION_U32, 0, 0, 0, NULL, 0},
    {"linger", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"linger_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"send_hwm_bytes", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"udp_multicast_group", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"udp_multicast_interface", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"udp_option_flags", TURBO_FLOW_OPTION_U32, 0, 0, 0, NULL, 0},
    {"udp_multicast_loop", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"udp_multicast_ttl", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 0, 255, NULL, 0},
    {"udp_broadcast", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"max_pump_iterations", TURBO_FLOW_OPTION_U32, 0, 0, 0, NULL, 0},
    {"context", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL, 0},
    {"take_context_ownership", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0}};

#define FLOW_CORONET_DEFAULT_TIMEOUT_MS 1000u
#define FLOW_CORONET_DEFAULT_PUMP_ITERATIONS 10000u

static uint64_t flow_coronet_timeout_ns(uint64_t timeout_ms) {
  return timeout_ms > UINT64_MAX / UINT64_C(1000000) ? UINT64_MAX : timeout_ms * UINT64_C(1000000);
}

static void flow_coronet_timeout_config_resolve(tf_coronet_socket_timeout_config_t *timeouts,
                                                const turbo_flow_coronet_socket_config_t *config) {
  if (!timeouts || !config) return;
  timeouts->timeout_ms = config->timeout_ms;
  timeouts->connect_timeout_ms = config->connect_timeout_ms;
  timeouts->send_timeout_ms = config->send_timeout_ms;
  timeouts->recv_timeout_ms = config->recv_timeout_ms;
  timeouts->handshake_timeout_ms = config->handshake_timeout_ms;
  if (timeouts->timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_DEFAULT;
  if (timeouts->connect_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_CONNECT;
  if (timeouts->send_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_SEND;
  if (timeouts->recv_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_RECV;
  if (timeouts->handshake_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_HANDSHAKE;
  if (timeouts->timeout_ms == TURBO_FLOW_CORONET_SOCKET_TIMEOUT_DISABLED) {
    timeouts->timeout_ms = 0;
    timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_DEFAULT;
  }
  if (timeouts->connect_timeout_ms == TURBO_FLOW_CORONET_SOCKET_TIMEOUT_DISABLED) {
    timeouts->connect_timeout_ms = 0;
    timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_CONNECT;
  }
  if (timeouts->send_timeout_ms == TURBO_FLOW_CORONET_SOCKET_TIMEOUT_DISABLED) {
    timeouts->send_timeout_ms = 0;
    timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_SEND;
  }
  if (timeouts->recv_timeout_ms == TURBO_FLOW_CORONET_SOCKET_TIMEOUT_DISABLED) {
    timeouts->recv_timeout_ms = 0;
    timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_RECV;
  }
  if (timeouts->handshake_timeout_ms == TURBO_FLOW_CORONET_SOCKET_TIMEOUT_DISABLED) {
    timeouts->handshake_timeout_ms = 0;
    timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_HANDSHAKE;
  }
  tf_coronet_socket_timeouts_resolve(timeouts, FLOW_CORONET_DEFAULT_TIMEOUT_MS);
}

typedef struct flow_coronet_socket_adapter_s {
  turbo_flow_t *flow;
  tf_coronet_execution_t execution;
  coro_context_t *ctx;
  tstr host;
  tstr path;
  tstr source_name;
  tstr udp_multicast_group;
  tstr udp_multicast_interface;
  coro_socket_t *server;
  int port;
  turbo_flow_coronet_socket_role_t role;
  turbo_flow_coronet_transport_t transport;
  tf_coronet_socket_timeout_config_t timeouts;
  tf_coronet_socket_options_t socket_options;
  tf_coronet_udp_options_t udp_options;
  turbo_kcp_config_t kcp_config;
  int kcp_configured;
  int reuse_port;
  uint32_t max_pump_iterations;
  atomic_int started;
  atomic_int quiesced;
  tf_connection_state_t connection;
  turbo_mutex_t request_mutex;
  turbo_cond_t request_cond;
  struct flow_coronet_send_task_s *active_requests;
  size_t active_request_count;
  size_t active_request_bytes;
  int request_sync_initialized;
} flow_coronet_socket_adapter_t;

typedef struct flow_coronet_send_task_s {
  flow_coronet_socket_adapter_t *adapter;
  struct flow_coronet_send_task_s *next;
  tstr payload;
  coro_socket_t *socket;
  atomic_int refs;
  turbo_mutex_t mutex;
  turbo_cond_t cond;
  int done;
  int status;
} flow_coronet_send_task_t;

static void flow_coronet_send_task_release(flow_coronet_send_task_t *task) {
  if (!task || atomic_fetch_sub_explicit(&task->refs, 1, memory_order_acq_rel) != 1) return;
  turbo_cond_destroy(&task->cond);
  turbo_mutex_destroy(&task->mutex);
  tstr_freep(&task->payload);
  free(task);
}

static void flow_coronet_send_task_complete(flow_coronet_send_task_t *task, int status) {
  flow_coronet_socket_adapter_t *adapter;
  flow_coronet_send_task_t **link;
  if (!task || !(adapter = task->adapter)) return;

  turbo_mutex_lock(&adapter->request_mutex);
  link = &adapter->active_requests;
  while (*link && *link != task)
    link = &(*link)->next;
  if (*link == task) {
    *link = task->next;
    adapter->active_request_count -= 1u;
    adapter->active_request_bytes -= tstr_len(task->payload);
  }
  task->socket = NULL;
  tf_connection_set_usage(&adapter->connection, 0u, adapter->active_request_count,
                          adapter->active_request_bytes);
  atomic_store_explicit(&adapter->connection.last_status, status, memory_order_relaxed);
  turbo_cond_broadcast(&adapter->request_cond);
  turbo_mutex_unlock(&adapter->request_mutex);
  turbo_mutex_lock(&task->mutex);
  task->status = status;
  task->done = 1;
  turbo_cond_signal(&task->cond);
  turbo_mutex_unlock(&task->mutex);
}

static void flow_coronet_send_task_detach_socket(flow_coronet_send_task_t *task) {
  flow_coronet_socket_adapter_t *adapter;
  if (!task || !(adapter = task->adapter)) return;
  turbo_mutex_lock(&adapter->request_mutex);
  task->socket = NULL;
  turbo_mutex_unlock(&adapter->request_mutex);
}

static tf_coronet_transport_t flow_coronet_transport(turbo_flow_coronet_transport_t transport) {
  return (tf_coronet_transport_t)transport;
}

static int flow_coronet_kcp_config_resolve(tf_coronet_transport_t transport,
                                           const turbo_flow_coronet_socket_config_t *config,
                                           turbo_kcp_config_t *out, int *configured) {
  tf_coronet_kcp_options_t options;
  turbo_kcp_config_t defaults;
  if (!config || !out || !configured) return TURBO_EINVAL;
  memset(&options, 0, sizeof(options));
  if (transport != TF_CORONET_TRANSPORT_KCP) {
    if (config->kcp_pre_shared_key || config->kcp_mtu || config->kcp_send_window ||
        config->kcp_receive_window || config->kcp_interval_ms || config->kcp_handshake_retry_ms ||
        config->kcp_fast_resend || config->kcp_congestion_control || config->kcp_fec_data_shards ||
        config->kcp_fec_parity_shards || config->kcp_fec_max_payload_size ||
        config->kcp_fec_receive_groups)
      return TURBO_EINVAL;
    return tf_coronet_kcp_options_resolve(transport, &options, out, configured);
  }
  if (tf_coronet_kcp_pre_shared_key_parse(config->kcp_pre_shared_key, options.pre_shared_key) !=
      TURBO_OK)
    return TURBO_EINVAL;
  turbo_kcp_config_default(&defaults);
  options.mtu = config->kcp_mtu ? config->kcp_mtu : defaults.mtu;
  options.send_window = config->kcp_send_window ? config->kcp_send_window : defaults.send_window;
  options.receive_window =
      config->kcp_receive_window ? config->kcp_receive_window : defaults.receive_window;
  options.interval_ms = config->kcp_interval_ms ? config->kcp_interval_ms : defaults.interval_ms;
  options.handshake_retry_ms =
      config->kcp_handshake_retry_ms ? config->kcp_handshake_retry_ms : defaults.handshake_retry_ms;
  options.fast_resend = config->kcp_fast_resend ? config->kcp_fast_resend : defaults.fast_resend;
  options.no_congestion_window = config->kcp_congestion_control ? 0 : 1;
  options.data_shards =
      config->kcp_fec_data_shards ? config->kcp_fec_data_shards : defaults.fec.data_shards;
  options.parity_shards =
      config->kcp_fec_parity_shards ? config->kcp_fec_parity_shards : defaults.fec.parity_shards;
  options.max_payload_size = config->kcp_fec_max_payload_size ? config->kcp_fec_max_payload_size
                                                              : defaults.fec.max_payload_size;
  options.receive_group_count = config->kcp_fec_receive_groups ? config->kcp_fec_receive_groups
                                                               : defaults.fec.receive_group_count;
  return tf_coronet_kcp_options_resolve(transport, &options, out, configured);
}

int turbo_flow_coronet_socket_config_validate(const turbo_flow_coronet_socket_config_t *config) {
  tf_coronet_transport_t transport;
  tf_coronet_socket_timeout_config_t timeouts;
  turbo_kcp_config_t kcp_config;
  int kcp_configured = 0;
  int rc;

  if (!config || (config->role != TURBO_FLOW_CORONET_SOCKET_SOURCE &&
                  config->role != TURBO_FLOW_CORONET_SOCKET_SINK)) {
    return TURBO_EINVAL;
  }

  transport = flow_coronet_transport((turbo_flow_coronet_transport_t)config->transport);
  if (!tf_coronet_transport_valid(transport)) return TURBO_EINVAL;

  rc = tf_coronet_endpoint_config_validate(transport, config->host, config->port, config->path);
  if (rc != TURBO_OK) return rc;
  rc = tf_coronet_reuse_port_validate(transport, config->reuse_port,
                                      config->role == TURBO_FLOW_CORONET_SOCKET_SOURCE);
  if (rc != TURBO_OK) return rc;

  memset(&timeouts, 0, sizeof(timeouts));
  flow_coronet_timeout_config_resolve(&timeouts, config);
  {
    uint64_t connection_timeout_ms;
    rc = tf_coronet_connection_timeout_resolve(transport, &timeouts, &connection_timeout_ms);
    if (rc != TURBO_OK) return rc;
    if (config->role == TURBO_FLOW_CORONET_SOCKET_SOURCE &&
        (transport == TF_CORONET_TRANSPORT_TLS || transport == TF_CORONET_TRANSPORT_WS ||
         transport == TF_CORONET_TRANSPORT_WSS) &&
        (timeouts.explicit_flags & TF_CORONET_TIMEOUT_SET_HANDSHAKE) != 0) {
      return TURBO_ENOTSUP;
    }
  }

  rc = flow_coronet_kcp_config_resolve(transport, config, &kcp_config, &kcp_configured);
  if (rc != TURBO_OK) return rc;
  turbo_kcp_config_wipe(&kcp_config);

  {
    tf_coronet_socket_options_t socket_options;
    memset(&socket_options, 0, sizeof(socket_options));
    socket_options.tcp_keepalive = config->tcp_keepalive;
    socket_options.tcp_keepalive_idle_ms = config->tcp_keepalive_idle_ms;
    socket_options.tcp_keepalive_interval_ms = config->tcp_keepalive_interval_ms;
    socket_options.tcp_keepalive_count = config->tcp_keepalive_count;
    socket_options.linger = config->linger;
    socket_options.linger_ms = config->linger_ms;
    socket_options.send_hwm_bytes = config->send_hwm_bytes;
    rc = tf_coronet_socket_options_validate(transport, &socket_options);
    if (rc != TURBO_OK) return rc;
  }
  {
    tf_coronet_udp_options_t udp_options;
    memset(&udp_options, 0, sizeof(udp_options));
    udp_options.multicast_group = config->udp_multicast_group;
    udp_options.multicast_interface = config->udp_multicast_interface;
    udp_options.option_flags = config->udp_option_flags;
    udp_options.multicast_loop = config->udp_multicast_loop;
    udp_options.multicast_ttl = config->udp_multicast_ttl;
    udp_options.broadcast = config->udp_broadcast;
    return tf_coronet_udp_options_validate(transport, &udp_options,
                                           config->role == TURBO_FLOW_CORONET_SOCKET_SOURCE);
  }
}

typedef enum flow_socket_resolved_field_kind_e {
  FLOW_SOCKET_RESOLVED_STRING = 0,
  FLOW_SOCKET_RESOLVED_BOOL,
  FLOW_SOCKET_RESOLVED_U32,
  FLOW_SOCKET_RESOLVED_U64,
  FLOW_SOCKET_RESOLVED_SIZE,
  FLOW_SOCKET_RESOLVED_PORT
} flow_socket_resolved_field_kind_t;

typedef struct flow_socket_resolved_field_s {
  const char *name;
  flow_socket_resolved_field_kind_t kind;
  size_t offset;
  uint64_t maximum;
  uint32_t udp_presence_flag;
} flow_socket_resolved_field_t;

#define FLOW_SOCKET_FIELD(member, field_kind, maximum_value)                                       \
  {#member, field_kind, offsetof(turbo_flow_coronet_socket_config_t, member), maximum_value, 0u}
#define FLOW_SOCKET_UDP_FIELD(member, field_kind, maximum_value, presence_flag)                    \
  {#member, field_kind, offsetof(turbo_flow_coronet_socket_config_t, member), maximum_value,       \
   presence_flag}

static const flow_socket_resolved_field_t FLOW_SOCKET_RESOLVED_FIELDS[] = {
    FLOW_SOCKET_FIELD(host, FLOW_SOCKET_RESOLVED_STRING, 0u),
    FLOW_SOCKET_FIELD(port, FLOW_SOCKET_RESOLVED_PORT, 65535u),
    FLOW_SOCKET_FIELD(path, FLOW_SOCKET_RESOLVED_STRING, 0u),
    FLOW_SOCKET_FIELD(timeout_ms, FLOW_SOCKET_RESOLVED_U64, UINT64_MAX),
    FLOW_SOCKET_FIELD(connect_timeout_ms, FLOW_SOCKET_RESOLVED_U64, UINT64_MAX),
    FLOW_SOCKET_FIELD(send_timeout_ms, FLOW_SOCKET_RESOLVED_U64, UINT64_MAX),
    FLOW_SOCKET_FIELD(recv_timeout_ms, FLOW_SOCKET_RESOLVED_U64, UINT64_MAX),
    FLOW_SOCKET_FIELD(handshake_timeout_ms, FLOW_SOCKET_RESOLVED_U64, UINT64_MAX),
    FLOW_SOCKET_FIELD(kcp_pre_shared_key, FLOW_SOCKET_RESOLVED_STRING, 0u),
    FLOW_SOCKET_FIELD(kcp_mtu, FLOW_SOCKET_RESOLVED_U32, UINT16_MAX),
    FLOW_SOCKET_FIELD(kcp_send_window, FLOW_SOCKET_RESOLVED_U32, UINT16_MAX),
    FLOW_SOCKET_FIELD(kcp_receive_window, FLOW_SOCKET_RESOLVED_U32, UINT16_MAX),
    FLOW_SOCKET_FIELD(kcp_interval_ms, FLOW_SOCKET_RESOLVED_U32, 100u),
    FLOW_SOCKET_FIELD(kcp_handshake_retry_ms, FLOW_SOCKET_RESOLVED_U32, UINT16_MAX),
    FLOW_SOCKET_FIELD(kcp_fast_resend, FLOW_SOCKET_RESOLVED_U32, UINT8_MAX),
    FLOW_SOCKET_FIELD(kcp_congestion_control, FLOW_SOCKET_RESOLVED_BOOL, 1u),
    FLOW_SOCKET_FIELD(kcp_fec_data_shards, FLOW_SOCKET_RESOLVED_U32, UINT32_MAX),
    FLOW_SOCKET_FIELD(kcp_fec_parity_shards, FLOW_SOCKET_RESOLVED_U32, UINT32_MAX),
    FLOW_SOCKET_FIELD(kcp_fec_max_payload_size, FLOW_SOCKET_RESOLVED_U32, UINT32_MAX),
    FLOW_SOCKET_FIELD(kcp_fec_receive_groups, FLOW_SOCKET_RESOLVED_U32, UINT16_MAX),
    FLOW_SOCKET_FIELD(reuse_port, FLOW_SOCKET_RESOLVED_BOOL, 1u),
    FLOW_SOCKET_FIELD(tcp_keepalive, FLOW_SOCKET_RESOLVED_BOOL, 1u),
    FLOW_SOCKET_FIELD(tcp_keepalive_idle_ms, FLOW_SOCKET_RESOLVED_U64, UINT64_MAX),
    FLOW_SOCKET_FIELD(tcp_keepalive_interval_ms, FLOW_SOCKET_RESOLVED_U64, UINT64_MAX),
    FLOW_SOCKET_FIELD(tcp_keepalive_count, FLOW_SOCKET_RESOLVED_U32, UINT32_MAX),
    FLOW_SOCKET_FIELD(linger, FLOW_SOCKET_RESOLVED_BOOL, 1u),
    FLOW_SOCKET_FIELD(linger_ms, FLOW_SOCKET_RESOLVED_U64, UINT64_MAX),
    FLOW_SOCKET_FIELD(send_hwm_bytes, FLOW_SOCKET_RESOLVED_SIZE, SIZE_MAX),
    FLOW_SOCKET_FIELD(udp_multicast_group, FLOW_SOCKET_RESOLVED_STRING, 0u),
    FLOW_SOCKET_FIELD(udp_multicast_interface, FLOW_SOCKET_RESOLVED_STRING, 0u),
    FLOW_SOCKET_UDP_FIELD(udp_multicast_loop, FLOW_SOCKET_RESOLVED_BOOL, 1u,
                          TURBO_FLOW_CORONET_UDP_OPTION_MULTICAST_LOOP),
    FLOW_SOCKET_UDP_FIELD(udp_multicast_ttl, FLOW_SOCKET_RESOLVED_U32, UINT8_MAX,
                          TURBO_FLOW_CORONET_UDP_OPTION_MULTICAST_TTL),
    FLOW_SOCKET_UDP_FIELD(udp_broadcast, FLOW_SOCKET_RESOLVED_BOOL, 1u,
                          TURBO_FLOW_CORONET_UDP_OPTION_BROADCAST)};

static int flow_socket_resolved_enum(const turbo_flow_resolved_adapter_view_t *view,
                                     const char *field, const char *const *values,
                                     size_t value_count, int *value) {
  const char *text = NULL;
  int rc = turbo_flow_resolved_adapter_get_string(view, field, &text);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < value_count; ++i) {
    if (strcmp(text, values[i]) == 0) {
      *value = (int)i;
      return TURBO_OK;
    }
  }
  return TURBO_EINVAL;
}

static int flow_socket_resolved_assign(const turbo_flow_resolved_adapter_view_t *view,
                                       const flow_socket_resolved_field_t *field,
                                       turbo_flow_coronet_socket_config_t *config) {
  unsigned char *destination = (unsigned char *)config + field->offset;
  uint64_t number = 0u;
  int boolean = 0;
  int rc;
  switch (field->kind) {
  case FLOW_SOCKET_RESOLVED_STRING:
    return turbo_flow_resolved_adapter_get_string(view, field->name, (const char **)destination);
  case FLOW_SOCKET_RESOLVED_BOOL:
    rc = turbo_flow_resolved_adapter_get_bool(view, field->name, &boolean);
    if (rc == TURBO_OK) *(int *)destination = boolean;
    break;
  case FLOW_SOCKET_RESOLVED_U32:
    rc = turbo_flow_resolved_adapter_get_u64(view, field->name, &number);
    if (rc == TURBO_OK && number > field->maximum) rc = TURBO_ERANGE;
    if (rc == TURBO_OK) *(uint32_t *)destination = (uint32_t)number;
    break;
  case FLOW_SOCKET_RESOLVED_U64:
    rc = turbo_flow_resolved_adapter_get_u64(view, field->name, &number);
    if (rc == TURBO_OK) *(uint64_t *)destination = number;
    break;
  case FLOW_SOCKET_RESOLVED_SIZE:
    rc = turbo_flow_resolved_adapter_get_u64(view, field->name, &number);
    if (rc == TURBO_OK && number > field->maximum) rc = TURBO_ERANGE;
    if (rc == TURBO_OK) *(size_t *)destination = (size_t)number;
    break;
  case FLOW_SOCKET_RESOLVED_PORT:
    rc = turbo_flow_resolved_adapter_get_u64(view, field->name, &number);
    if (rc == TURBO_OK && number > field->maximum) rc = TURBO_ERANGE;
    if (rc == TURBO_OK) *(int *)destination = (int)number;
    break;
  default:
    rc = TURBO_EPROTO;
    break;
  }
  if (rc == TURBO_OK) config->udp_option_flags |= field->udp_presence_flag;
  return rc;
}

static int flow_socket_config_from_resolved(const turbo_flow_resolved_config_t *resolved,
                                            const char *adapter_name,
                                            turbo_flow_coronet_socket_config_t *config) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  int have_role = 0;
  int have_transport = 0;
  int rc;
  if (!config) return TURBO_EINVAL;
  memset(config, 0, sizeof(*config));
  rc = turbo_flow_resolved_config_adapter(resolved, adapter_name, &view);
  if (rc != TURBO_OK) return rc;
  if (strcmp(view.kind, "socket") != 0) return TURBO_EINVAL;
  for (size_t field_index = 0u; field_index < turbo_flow_resolved_adapter_field_count(&view);
       ++field_index) {
    const char *name = turbo_flow_resolved_adapter_field_name(&view, field_index);
    int matched = 0;
    if (!name) return TURBO_EPROTO;
    if (strcmp(name, "role") == 0) {
      int role_index = 0;
      rc = flow_socket_resolved_enum(
          &view, name, FLOW_SOCKET_ROLE_VALUES,
          sizeof(FLOW_SOCKET_ROLE_VALUES) / sizeof(FLOW_SOCKET_ROLE_VALUES[0]), &role_index);
      if (rc == TURBO_OK) config->role = role_index + TURBO_FLOW_CORONET_SOCKET_SOURCE;
      have_role = rc == TURBO_OK;
      matched = 1;
    } else if (strcmp(name, "transport") == 0) {
      rc = flow_socket_resolved_enum(&view, name, TF_CORONET_TRANSPORT_VALUES,
                                     TF_CORONET_TRANSPORT_VALUE_COUNT, &config->transport);
      have_transport = rc == TURBO_OK;
      matched = 1;
    } else {
      rc = TURBO_EINVAL;
      for (size_t i = 0u;
           i < sizeof(FLOW_SOCKET_RESOLVED_FIELDS) / sizeof(FLOW_SOCKET_RESOLVED_FIELDS[0]); ++i) {
        if (strcmp(name, FLOW_SOCKET_RESOLVED_FIELDS[i].name) == 0) {
          rc = flow_socket_resolved_assign(&view, &FLOW_SOCKET_RESOLVED_FIELDS[i], config);
          matched = 1;
          break;
        }
      }
    }
    if (!matched || rc != TURBO_OK) return rc;
  }
  if (!have_role || !have_transport) return TURBO_EINVAL;
  return turbo_flow_coronet_socket_config_validate(config);
}

static int flow_coronet_is_sink_configured(const flow_coronet_socket_adapter_t *adapter) {
  if (!adapter || adapter->role != TURBO_FLOW_CORONET_SOCKET_SINK) return 0;
  if (tf_coronet_transport_is_pipe(flow_coronet_transport(adapter->transport))) {
    return (adapter->path && adapter->path[0] != '\0') ||
           (adapter->host && adapter->host[0] != '\0');
  }
  return adapter->host && adapter->host[0] != '\0' && adapter->port > 0;
}

static int flow_coronet_is_source_configured(const flow_coronet_socket_adapter_t *adapter) {
  if (!adapter || adapter->role != TURBO_FLOW_CORONET_SOCKET_SOURCE ||
      !tf_coronet_transport_valid(flow_coronet_transport(adapter->transport)) || !adapter->flow ||
      !adapter->ctx || !adapter->source_name || adapter->source_name[0] == '\0') {
    return 0;
  }
  if (tf_coronet_transport_is_pipe(flow_coronet_transport(adapter->transport))) {
    return (adapter->path && adapter->path[0] != '\0') ||
           (adapter->host && adapter->host[0] != '\0');
  }
  return adapter->host && adapter->host[0] != '\0' && adapter->port > 0;
}

static void flow_coronet_sink_send_task(coro_t *co, void *arg) {
  flow_coronet_send_task_t *task = (flow_coronet_send_task_t *)arg;
  flow_coronet_socket_adapter_t *adapter = task ? task->adapter : NULL;
  coro_socket_t *socket;
  int rc;

  (void)co;
  if (!task || !adapter || !adapter->ctx) return;
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    flow_coronet_send_task_complete(task, TURBO_ESHUTDOWN);
    flow_coronet_send_task_release(task);
    return;
  }

  socket =
      tf_coronet_create_client_socket(adapter->ctx, flow_coronet_transport(adapter->transport));
  if (!socket) {
    flow_coronet_send_task_complete(task, TURBO_ENOMEM);
    flow_coronet_send_task_release(task);
    return;
  }

  turbo_mutex_lock(&adapter->request_mutex);
  task->socket = socket;
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    task->socket = NULL;
    turbo_mutex_unlock(&adapter->request_mutex);
    coro_socket_destroy(socket);
    flow_coronet_send_task_complete(task, TURBO_ESHUTDOWN);
    flow_coronet_send_task_release(task);
    return;
  }
  turbo_mutex_unlock(&adapter->request_mutex);

  rc = tf_coronet_apply_kcp_config(socket, flow_coronet_transport(adapter->transport),
                                   &adapter->kcp_config, adapter->kcp_configured);
  if (rc != TURBO_OK) {
    flow_coronet_send_task_detach_socket(task);
    coro_socket_destroy(socket);
    flow_coronet_send_task_complete(task, rc);
    flow_coronet_send_task_release(task);
    return;
  }
  rc = tf_coronet_apply_socket_options(socket, flow_coronet_transport(adapter->transport),
                                       &adapter->socket_options);
  if (rc != TURBO_OK) {
    flow_coronet_send_task_detach_socket(task);
    coro_socket_destroy(socket);
    flow_coronet_send_task_complete(task, rc);
    flow_coronet_send_task_release(task);
    return;
  }
  {
    uint64_t connection_timeout_ms = 0;
    rc = tf_coronet_connection_timeout_resolve(flow_coronet_transport(adapter->transport),
                                               &adapter->timeouts, &connection_timeout_ms);
    if (rc == TURBO_OK) coro_socket_set_timeout(socket, connection_timeout_ms);
  }

  if (rc == TURBO_OK) {
    rc = tf_coronet_connect_socket(socket, flow_coronet_transport(adapter->transport),
                                   adapter->host, adapter->port, adapter->path);
  }
  if (rc == TURBO_OK) {
    rc = tf_coronet_apply_udp_options(socket, flow_coronet_transport(adapter->transport),
                                      &adapter->udp_options);
  }
  if (rc == TURBO_OK) {
    (void)tf_coronet_apply_socket_timeout(socket, &adapter->timeouts, TF_CORONET_TIMEOUT_SEND);
    rc = tf_coronet_send_socket(socket, flow_coronet_transport(adapter->transport), task->payload,
                                tstr_len(task->payload));
  }

  flow_coronet_send_task_detach_socket(task);
  coro_socket_destroy(socket);
  flow_coronet_send_task_complete(task, rc);
  flow_coronet_send_task_release(task);
}

static void flow_coronet_source_recv_release(void *data, void *user_data) {
  (void)user_data;
  coro_socket_free_recv(data);
}

static int flow_coronet_source_publish_received(flow_coronet_socket_adapter_t *adapter, char *data,
                                                size_t len) {
  turbo_flow_msg_t msg;
  int rc;

  if (!adapter || !adapter->flow || !adapter->source_name || !data || len == 0u) {
    coro_socket_free_recv(data);
    return TURBO_EINVAL;
  }

  turbo_flow_msg_init(&msg);
  msg.buffer = mem_wrap_external(data, len, flow_coronet_source_recv_release, NULL);
  if (!msg.buffer) {
    coro_socket_free_recv(data);
    return TURBO_ENOMEM;
  }
  msg.payload = vstr_from_buf(data, len);

  rc = turbo_flow_publish(adapter->flow, adapter->source_name, &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static void flow_coronet_tcp_source_handler(coro_socket_t *client, void *arg) {
  flow_coronet_socket_adapter_t *adapter = (flow_coronet_socket_adapter_t *)arg;

  if (!client || !adapter) return;
  (void)tf_coronet_apply_socket_timeout(client, &adapter->timeouts, TF_CORONET_TIMEOUT_RECV);

  while (atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    char *data = NULL;
    size_t len = 0;
    int rc = coro_socket_recv(client, &data, &len);
    if (rc != TURBO_OK) break;
    if (data && len > 0) {
      rc = flow_coronet_source_publish_received(adapter, data, len);
      if (rc != TURBO_OK) break;
    } else if (data) {
      coro_socket_free_recv(data);
    } else {
      break;
    }
  }
}

static int flow_coronet_socket_start_source(flow_coronet_socket_adapter_t *adapter,
                                            turbo_flow_t *flow,
                                            const turbo_flow_stage_plan_t *stage) {
  int rc;

  if (!stage->is_source) return TURBO_EINVAL;
  if (adapter->server) return TURBO_EALREADY;

  tstr_freep(&adapter->source_name);
  adapter->source_name = tstr_dup(stage->name);
  if (!adapter->source_name) return TURBO_ENOMEM;
  adapter->flow = flow;

  if (!flow_coronet_is_source_configured(adapter)) return TURBO_ENOTSUP;

  adapter->server =
      tf_coronet_create_server_socket(adapter->ctx, flow_coronet_transport(adapter->transport));
  if (!adapter->server) return TURBO_ENOTSUP;
  rc = tf_coronet_apply_kcp_config(adapter->server, flow_coronet_transport(adapter->transport),
                                   &adapter->kcp_config, adapter->kcp_configured);
  if (rc != TURBO_OK) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
    return rc;
  }
  if (adapter->reuse_port) coro_socket_set_reuse_port(adapter->server, 1);
  rc = tf_coronet_apply_socket_options(adapter->server, flow_coronet_transport(adapter->transport),
                                       &adapter->socket_options);
  if (rc != TURBO_OK) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
    return rc;
  }
  (void)tf_coronet_apply_socket_timeout(adapter->server, &adapter->timeouts,
                                        TF_CORONET_TIMEOUT_RECV);

  rc = tf_coronet_listen_socket(adapter->server, flow_coronet_transport(adapter->transport),
                                adapter->host, adapter->port, adapter->path,
                                flow_coronet_tcp_source_handler, adapter);
  if (rc != TURBO_OK) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
    return rc;
  }
  rc = tf_coronet_apply_udp_options(adapter->server, flow_coronet_transport(adapter->transport),
                                    &adapter->udp_options);
  if (rc != TURBO_OK) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
    return rc;
  }
  rc = tf_coronet_join_multicast(adapter->server, flow_coronet_transport(adapter->transport),
                                 &adapter->udp_options);
  if (rc != TURBO_OK) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
    return rc;
  }

  return TURBO_OK;
}

typedef struct flow_coronet_start_source_args_s {
  flow_coronet_socket_adapter_t *adapter;
  turbo_flow_t *flow;
  const turbo_flow_stage_plan_t *stage;
} flow_coronet_start_source_args_t;

static int flow_coronet_socket_start_source_call(void *arg) {
  flow_coronet_start_source_args_t *start = (flow_coronet_start_source_args_t *)arg;
  return flow_coronet_socket_start_source(start->adapter, start->flow, start->stage);
}

static int flow_coronet_execution_ready_call(void *arg) { return arg ? TURBO_OK : TURBO_EINVAL; }

static int flow_coronet_socket_start(void *ctx, turbo_flow_t *flow,
                                     const turbo_flow_stage_plan_t *stage) {
  flow_coronet_socket_adapter_t *adapter = (flow_coronet_socket_adapter_t *)ctx;

  if (!adapter || !adapter->ctx || !stage) return TURBO_EINVAL;
  atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_OK);
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  {
    int rc = tf_coronet_execution_start(&adapter->execution);
    if (rc != TURBO_OK) {
      atomic_store_explicit(&adapter->started, 0, memory_order_release);
      tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, rc);
      return rc;
    }
  }
  if (stage->is_source && adapter->role == TURBO_FLOW_CORONET_SOCKET_SOURCE) {
    flow_coronet_start_source_args_t args = {adapter, flow, stage};
    int rc =
        tf_coronet_execution_call(&adapter->execution, flow_coronet_socket_start_source_call, &args,
                                  flow_coronet_timeout_ns(adapter->timeouts.timeout_ms));
    if (rc != TURBO_OK) {
      tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, rc);
      atomic_store_explicit(&adapter->started, 0, memory_order_release);
      return rc;
    }
    tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
    return TURBO_OK;
  }
  {
    int rc =
        tf_coronet_execution_call(&adapter->execution, flow_coronet_execution_ready_call, adapter,
                                  flow_coronet_timeout_ns(adapter->timeouts.timeout_ms));
    if (rc != TURBO_OK) {
      atomic_store_explicit(&adapter->started, 0, memory_order_release);
      tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, rc);
      return rc;
    }
  }
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
  return TURBO_OK;
}

static void flow_coronet_socket_send_post(void *arg1, void *arg2) {
  flow_coronet_socket_adapter_t *adapter = (flow_coronet_socket_adapter_t *)arg1;
  flow_coronet_send_task_t *task = (flow_coronet_send_task_t *)arg2;
  int rc = coro_context_spawn(adapter->ctx, flow_coronet_sink_send_task, task);
  if (rc == TURBO_OK) return;
  flow_coronet_send_task_complete(task, rc);
  flow_coronet_send_task_release(task);
}

static int flow_coronet_socket_consume(void *ctx, turbo_flow_t *flow,
                                       const turbo_flow_stage_plan_t *stage,
                                       turbo_flow_msg_t *msg) {
  flow_coronet_socket_adapter_t *adapter = (flow_coronet_socket_adapter_t *)ctx;
  flow_coronet_send_task_t *task;
  int rc;
  (void)flow;
  (void)stage;

  if (!adapter || !adapter->ctx) return TURBO_EINVAL;
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
  if (atomic_load_explicit(&adapter->quiesced, memory_order_acquire)) return TURBO_EBUSY;
  if (!flow_coronet_is_sink_configured(adapter)) return TURBO_ENOTSUP;
  if (!msg || (msg->payload.len > 0 && !msg->payload.data)) return TURBO_EINVAL;

  task = (flow_coronet_send_task_t *)calloc(1, sizeof(*task));
  if (!task) return TURBO_ENOMEM;
  task->adapter = adapter;
  task->payload = tstr_new_len(msg->payload.data ? msg->payload.data : "", msg->payload.len);
  if (!task->payload) {
    free(task);
    return TURBO_ENOMEM;
  }
  task->status = TURBO_EALREADY;
  atomic_init(&task->refs, 2);
  turbo_mutex_init(&task->mutex);
  turbo_cond_init(&task->cond);

  turbo_mutex_lock(&adapter->request_mutex);
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    turbo_mutex_unlock(&adapter->request_mutex);
    flow_coronet_send_task_release(task);
    flow_coronet_send_task_release(task);
    return TURBO_ESHUTDOWN;
  }
  task->next = adapter->active_requests;
  adapter->active_requests = task;
  adapter->active_request_count += 1u;
  adapter->active_request_bytes += msg->payload.len;
  tf_connection_set_usage(&adapter->connection, 0u, adapter->active_request_count,
                          adapter->active_request_bytes);
  turbo_mutex_unlock(&adapter->request_mutex);

  rc = tf_coronet_execution_post(&adapter->execution, flow_coronet_socket_send_post, adapter, task);
  if (rc != TURBO_OK) {
    flow_coronet_send_task_complete(task, rc);
    flow_coronet_send_task_release(task);
    flow_coronet_send_task_release(task);
    return rc;
  }

  turbo_mutex_lock(&task->mutex);
  while (!task->done) {
    if (turbo_cond_timedwait(&task->cond, &task->mutex,
                             flow_coronet_timeout_ns(adapter->timeouts.send_timeout_ms)) !=
        TURBO_OK) {
      break;
    }
  }
  rc = task->done ? task->status : TURBO_ETIMEDOUT;
  turbo_mutex_unlock(&task->mutex);
  flow_coronet_send_task_release(task);
  return rc;
}

static int flow_coronet_socket_begin_close_resources_call(void *arg) {
  flow_coronet_socket_adapter_t *adapter = (flow_coronet_socket_adapter_t *)arg;
  flow_coronet_send_task_t *task;
  int rc = TURBO_OK;
  if (!adapter) return TURBO_EINVAL;

  turbo_mutex_lock(&adapter->request_mutex);
  for (task = adapter->active_requests; task; task = task->next) {
    if (task->socket) (void)coro_socket_interrupt_wait(task->socket, TURBO_ESHUTDOWN);
  }
  turbo_mutex_unlock(&adapter->request_mutex);
  if (adapter->server) {
    (void)tf_coronet_leave_multicast(adapter->server, flow_coronet_transport(adapter->transport),
                                     &adapter->udp_options);
    rc = coro_socket_server_stop(adapter->server);
  }
  return rc;
}

static int flow_coronet_socket_server_stopped_call(void *arg) {
  flow_coronet_socket_adapter_t *adapter = (flow_coronet_socket_adapter_t *)arg;
  if (!adapter) return TURBO_EINVAL;
  return !adapter->server || coro_socket_server_is_stopped(adapter->server) ? TURBO_OK
                                                                            : TURBO_EBUSY;
}

static int flow_coronet_socket_destroy_server_call(void *arg) {
  flow_coronet_socket_adapter_t *adapter = (flow_coronet_socket_adapter_t *)arg;
  if (!adapter) return TURBO_EINVAL;
  if (!adapter->server) return TURBO_OK;
  if (!coro_socket_server_is_stopped(adapter->server)) return TURBO_EBUSY;
  coro_socket_destroy(adapter->server);
  adapter->server = NULL;
  return TURBO_OK;
}

static int flow_coronet_socket_close_resources(flow_coronet_socket_adapter_t *adapter) {
  uint64_t call_timeout_ns;
  int rc;
  if (!adapter) return TURBO_EINVAL;

  call_timeout_ns = flow_coronet_timeout_ns(adapter->timeouts.timeout_ms);
  rc =
      tf_coronet_execution_call(&adapter->execution, flow_coronet_socket_begin_close_resources_call,
                                adapter, call_timeout_ns);
  if (rc != TURBO_OK || !adapter->server) return rc;

  for (uint32_t i = 0u; i < adapter->max_pump_iterations; ++i) {
    rc = tf_coronet_execution_call(&adapter->execution, flow_coronet_socket_server_stopped_call,
                                   adapter, call_timeout_ns);
    if (rc == TURBO_OK) {
      return tf_coronet_execution_call(&adapter->execution, flow_coronet_socket_destroy_server_call,
                                       adapter, call_timeout_ns);
    }
    if (rc != TURBO_EBUSY) return rc;
    turbo_sleep_ms(1);
  }
  return TURBO_ETIMEDOUT;
}

static void flow_coronet_socket_drain_requests(flow_coronet_socket_adapter_t *adapter) {
  if (!adapter || !adapter->request_sync_initialized) return;
  turbo_mutex_lock(&adapter->request_mutex);
  while (adapter->active_request_count != 0u) {
    turbo_cond_wait(&adapter->request_cond, &adapter->request_mutex);
  }
  turbo_mutex_unlock(&adapter->request_mutex);
}

static void flow_coronet_socket_stop(void *ctx, turbo_flow_t *flow,
                                     const turbo_flow_stage_plan_t *stage) {
  flow_coronet_socket_adapter_t *adapter = (flow_coronet_socket_adapter_t *)ctx;
  int close_status;
  (void)flow;
  (void)stage;

  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CLOSING, TURBO_OK);
  close_status = flow_coronet_socket_close_resources(adapter);
  flow_coronet_socket_drain_requests(adapter);
  tf_coronet_execution_stop(&adapter->execution);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_STOPPED,
                           close_status == TURBO_OK ? TURBO_ESHUTDOWN : close_status);
}

static int flow_coronet_socket_connection_snapshot(void *ctx,
                                                   turbo_flow_connection_snapshot_t *out) {
  flow_coronet_socket_adapter_t *adapter = (flow_coronet_socket_adapter_t *)ctx;
  return adapter ? tf_connection_snapshot(&adapter->connection, out) : TURBO_EINVAL;
}

static int flow_coronet_socket_command(void *ctx, turbo_flow_t *flow,
                                       const turbo_flow_adapter_command_t *command) {
  flow_coronet_socket_adapter_t *adapter = (flow_coronet_socket_adapter_t *)ctx;
  (void)flow;
  if (!adapter || !command || !atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    return TURBO_EINVAL;
  }
  if (adapter->role != TURBO_FLOW_CORONET_SOCKET_SINK) return TURBO_ENOTSUP;
  switch (command->kind) {
  case TURBO_FLOW_ADAPTER_QUIESCE:
    atomic_store_explicit(&adapter->quiesced, 1, memory_order_release);
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_RESUME:
    atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT:
    return TURBO_ENOTSUP;
  default:
    return TURBO_EINVAL;
  }
}

static void flow_coronet_socket_shutdown(void *ctx) {
  flow_coronet_socket_adapter_t *adapter = (flow_coronet_socket_adapter_t *)ctx;

  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  if (adapter->server || adapter->active_request_count != 0u) {
    (void)flow_coronet_socket_close_resources(adapter);
  }
  flow_coronet_socket_drain_requests(adapter);
  tf_coronet_execution_destroy(&adapter->execution);
  tstr_freep(&adapter->host);
  tstr_freep(&adapter->path);
  tstr_freep(&adapter->source_name);
  tstr_freep(&adapter->udp_multicast_group);
  tstr_freep(&adapter->udp_multicast_interface);
  if (adapter->request_sync_initialized) {
    turbo_cond_destroy(&adapter->request_cond);
    turbo_mutex_destroy(&adapter->request_mutex);
  }
  turbo_kcp_config_wipe(&adapter->kcp_config);
  adapter->ctx = NULL;
  free(adapter);
}

static int flow_socket_register_contract(turbo_flow_t *flow) {
  static const char *const primitive_types[] = {TURBO_FLOW_SOCKET_PRIMITIVE_TYPE};
  static const char *const operation_names[] = {TURBO_FLOW_SOCKET_RECEIVE_OPERATION,
                                                TURBO_FLOW_SOCKET_SEND_OPERATION};
  turbo_flow_operation_descriptor_t operations[2];
  turbo_flow_module_descriptor_t module;

  memset(operations, 0, sizeof(operations));
  memset(&module, 0, sizeof(module));
  for (size_t i = 0u; i < 2u; ++i) {
    operations[i].size = sizeof(operations[i]);
    operations[i].name = operation_names[i];
    operations[i].version = TURBO_FLOW_SOCKET_MODULE_VERSION;
    operations[i].domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
    operations[i].resource_domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
    operations[i].resource_type = TURBO_FLOW_SOCKET_PRIMITIVE_TYPE;
    operations[i].resource_min_version = TURBO_FLOW_SOCKET_MODULE_VERSION;
    operations[i].resource_max_version = TURBO_FLOW_SOCKET_MODULE_VERSION;
    operations[i].scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
    operations[i].scope.state = TURBO_FLOW_STATE_SCOPE_RESOURCE_OWNER;
    operations[i].scope.lifetime =
        i == 0u ? TURBO_FLOW_LIFETIME_DISPATCH : TURBO_FLOW_LIFETIME_CALL;
    operations[i].scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
    operations[i].scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
    operations[i].flags = (i == 0u ? TURBO_FLOW_OPERATION_SOURCE : TURBO_FLOW_OPERATION_STAGE) |
                          TURBO_FLOW_OPERATION_BRIDGE;
    operations[i].execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  }
  operations[0].output_domain = TURBO_FLOW_DOMAIN_DATA;
  operations[0].output_type = "Message";
  operations[1].input_domain = TURBO_FLOW_DOMAIN_DATA;
  operations[1].input_type = "Message";
  module.size = sizeof(module);
  module.name = TURBO_FLOW_SOCKET_MODULE;
  module.version = TURBO_FLOW_SOCKET_MODULE_VERSION;
  module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                            TURBO_FLOW_MODULE_MANAGED_RESOURCES | TURBO_FLOW_MODULE_NATIVE_API;
  module.primitive_types = primitive_types;
  module.primitive_type_count = 1u;
  module.operation_names = operation_names;
  module.operation_count = 2u;
  return turbo_flow_register_module_contract(flow, &module, operations, 2u);
}

int turbo_flow_coronet_register_socket_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_coronet_socket_config_t *config,
    const turbo_flow_coronet_execution_binding_t *execution) {
  flow_coronet_socket_adapter_t *adapter;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_adapter_schema_t schema;
  turbo_flow_module_adapter_registration_t registration =
      TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT;
  turbo_flow_primitive_descriptor_t primitive;
  const char *operation_names[2];
  const char *operation_resources[2];
  size_t operation_count;
  int rc;

  if (!flow || !name || name[0] == '\0' || !execution) return TURBO_EINVAL;
  rc = turbo_flow_coronet_execution_binding_validate(execution);
  if (rc != TURBO_OK) return rc;
  if (config) {
    if (config->context || config->take_context_ownership) return TURBO_EINVAL;
    rc = turbo_flow_coronet_socket_config_validate(config);
    if (rc != TURBO_OK) return rc;
  }
  rc = flow_socket_register_contract(flow);
  if (rc != TURBO_OK) return rc;

  adapter = (flow_coronet_socket_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  if (tf_connection_init(&adapter->connection, "", 1u) != TURBO_OK) {
    free(adapter);
    return TURBO_ENOMEM;
  }
  turbo_mutex_init(&adapter->request_mutex);
  turbo_cond_init(&adapter->request_cond);
  adapter->request_sync_initialized = 1;
  atomic_init(&adapter->started, 0);
  atomic_init(&adapter->quiesced, 0);

  rc = tf_coronet_execution_init(&adapter->execution, execution);
  adapter->ctx = adapter->execution.context;
  if (rc != TURBO_OK || !adapter->ctx) {
    flow_coronet_socket_shutdown(adapter);
    return rc != TURBO_OK ? rc : TURBO_ENOMEM;
  }

  if (config) {
    adapter->role = (turbo_flow_coronet_socket_role_t)config->role;
    adapter->transport = (turbo_flow_coronet_transport_t)config->transport;
    adapter->port = config->port;
    flow_coronet_timeout_config_resolve(&adapter->timeouts, config);
    adapter->max_pump_iterations = config->max_pump_iterations
                                       ? config->max_pump_iterations
                                       : FLOW_CORONET_DEFAULT_PUMP_ITERATIONS;
    rc = flow_coronet_kcp_config_resolve(flow_coronet_transport(adapter->transport), config,
                                         &adapter->kcp_config, &adapter->kcp_configured);
    if (rc != TURBO_OK) {
      flow_coronet_socket_shutdown(adapter);
      return rc;
    }
    adapter->reuse_port = config->reuse_port ? 1 : 0;
    adapter->socket_options.tcp_keepalive = config->tcp_keepalive;
    adapter->socket_options.tcp_keepalive_idle_ms = config->tcp_keepalive_idle_ms;
    adapter->socket_options.tcp_keepalive_interval_ms = config->tcp_keepalive_interval_ms;
    adapter->socket_options.tcp_keepalive_count = config->tcp_keepalive_count;
    adapter->socket_options.linger = config->linger;
    adapter->socket_options.linger_ms = config->linger_ms;
    adapter->socket_options.send_hwm_bytes = config->send_hwm_bytes;
    adapter->udp_options.option_flags = config->udp_option_flags;
    adapter->udp_options.multicast_loop = config->udp_multicast_loop;
    adapter->udp_options.multicast_ttl = config->udp_multicast_ttl;
    adapter->udp_options.broadcast = config->udp_broadcast;
    if (config->host) {
      adapter->host = tstr_dup(config->host);
      if (!adapter->host) {
        flow_coronet_socket_shutdown(adapter);
        return TURBO_ENOMEM;
      }
    }
    if (config->path) {
      adapter->path = tstr_dup(config->path);
      if (!adapter->path) {
        flow_coronet_socket_shutdown(adapter);
        return TURBO_ENOMEM;
      }
    }
    if (config->udp_multicast_group) {
      adapter->udp_multicast_group = tstr_dup(config->udp_multicast_group);
      if (!adapter->udp_multicast_group) {
        flow_coronet_socket_shutdown(adapter);
        return TURBO_ENOMEM;
      }
      adapter->udp_options.multicast_group = adapter->udp_multicast_group;
    }
    if (config->udp_multicast_interface) {
      adapter->udp_multicast_interface = tstr_dup(config->udp_multicast_interface);
      if (!adapter->udp_multicast_interface) {
        flow_coronet_socket_shutdown(adapter);
        return TURBO_ENOMEM;
      }
      adapter->udp_options.multicast_interface = adapter->udp_multicast_interface;
    }
  } else {
    adapter->role = TURBO_FLOW_CORONET_SOCKET_UNCONFIGURED;
    adapter->transport = TURBO_FLOW_CORONET_TRANSPORT_TCP;
    adapter->timeouts.timeout_ms = FLOW_CORONET_DEFAULT_TIMEOUT_MS;
    adapter->max_pump_iterations = FLOW_CORONET_DEFAULT_PUMP_ITERATIONS;
    tf_coronet_socket_timeouts_resolve(&adapter->timeouts, FLOW_CORONET_DEFAULT_TIMEOUT_MS);
  }

  {
    char endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
    const char *scheme = TF_CORONET_TRANSPORT_VALUES[adapter->transport];
    int written;
    if (tf_coronet_transport_is_pipe(flow_coronet_transport(adapter->transport))) {
      written = snprintf(endpoint, sizeof(endpoint), "%s", adapter->path ? adapter->path : "");
    } else {
      written = snprintf(endpoint, sizeof(endpoint), "%s://%s:%d", scheme,
                         adapter->host ? adapter->host : "", adapter->port);
    }
    if (written < 0 || (size_t)written >= sizeof(endpoint) ||
        tf_connection_set_endpoint(&adapter->connection, endpoint) != TURBO_OK) {
      flow_coronet_socket_shutdown(adapter);
      return TURBO_ENOSPC;
    }
  }

  memset(&ops, 0, sizeof(ops));
  ops.start = flow_coronet_socket_start;
  ops.consume = flow_coronet_socket_consume;
  ops.stop = flow_coronet_socket_stop;
  ops.shutdown = flow_coronet_socket_shutdown;
  ops.connection_snapshot = flow_coronet_socket_connection_snapshot;
  ops.command = flow_coronet_socket_command;

  memset(&schema, 0, sizeof(schema));
  schema.kind = TURBO_FLOW_ADAPTER_KIND_SOCKET;
  schema.roles = TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
  if (config && config->role == TURBO_FLOW_CORONET_SOCKET_SOURCE) {
    schema.roles = TURBO_FLOW_ADAPTER_SOURCE;
    schema.direction = TURBO_FLOW_ADAPTER_INPUT;
  } else if (config && config->role == TURBO_FLOW_CORONET_SOCKET_SINK) {
    schema.roles = TURBO_FLOW_ADAPTER_SINK;
    schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
  }
  schema.fields = FLOW_SOCKET_OPTION_FIELDS;
  schema.field_count = sizeof(FLOW_SOCKET_OPTION_FIELDS) / sizeof(FLOW_SOCKET_OPTION_FIELDS[0]);

  if (adapter->role == TURBO_FLOW_CORONET_SOCKET_SOURCE) {
    operation_names[0] = TURBO_FLOW_SOCKET_RECEIVE_OPERATION;
    operation_count = 1u;
  } else if (adapter->role == TURBO_FLOW_CORONET_SOCKET_SINK) {
    operation_names[0] = TURBO_FLOW_SOCKET_SEND_OPERATION;
    operation_count = 1u;
  } else {
    operation_names[0] = TURBO_FLOW_SOCKET_RECEIVE_OPERATION;
    operation_names[1] = TURBO_FLOW_SOCKET_SEND_OPERATION;
    operation_count = 2u;
  }
  for (size_t i = 0u; i < operation_count; ++i)
    operation_resources[i] = name;
  memset(&primitive, 0, sizeof(primitive));
  primitive.size = sizeof(primitive);
  primitive.name = name;
  primitive.type_name = TURBO_FLOW_SOCKET_PRIMITIVE_TYPE;
  primitive.version = TURBO_FLOW_SOCKET_MODULE_VERSION;
  primitive.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  primitive.kind = TURBO_FLOW_PRIMITIVE_RESOURCE;
  registration.module_name = TURBO_FLOW_SOCKET_MODULE;
  registration.adapter_name = name;
  registration.ops = &ops;
  registration.ctx = adapter;
  registration.schema = &schema;
  registration.operation_names = operation_names;
  registration.operation_count = operation_count;
  registration.operation_resource_names = operation_resources;
  registration.primitives = &primitive;
  registration.primitive_count = 1u;
  rc = turbo_flow_register_module_adapter(flow, &registration);
  if (rc != TURBO_OK) {
    flow_coronet_socket_shutdown(adapter);
    return rc;
  }

  return TURBO_OK;
}

int turbo_flow_coronet_register_socket_adapter(turbo_flow_t *flow, const char *name,
                                               const turbo_flow_coronet_socket_config_t *config) {
  turbo_flow_coronet_execution_binding_t execution;
  turbo_flow_coronet_socket_config_t resolved;
  const turbo_flow_coronet_socket_config_t *resolved_config = config;
  memset(&execution, 0, sizeof(execution));
  execution.size = sizeof(execution);
  execution.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  if (config) {
    resolved = *config;
    if (config->context) {
      execution.kind = config->take_context_ownership
                           ? TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT
                           : TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT;
      execution.context = config->context;
    } else if (config->take_context_ownership) {
      return TURBO_EINVAL;
    }
    resolved.context = NULL;
    resolved.take_context_ownership = 0;
    resolved_config = &resolved;
  }
  return turbo_flow_coronet_register_socket_adapter_ex(flow, name, resolved_config, &execution);
}

int turbo_flow_coronet_register_socket_resolved_adapter_ex(
    turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *adapter_name,
    const turbo_flow_coronet_execution_binding_t *execution) {
  turbo_flow_coronet_socket_config_t config;
  int rc;
  if (!flow || !execution) return TURBO_EINVAL;
  rc = flow_socket_config_from_resolved(resolved, adapter_name, &config);
  if (rc != TURBO_OK) return rc;
  return turbo_flow_coronet_register_socket_adapter_ex(flow, adapter_name, &config, execution);
}

int turbo_flow_coronet_register_socket_resolved_adapter(
    turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *adapter_name) {
  turbo_flow_coronet_execution_binding_t execution;
  memset(&execution, 0, sizeof(execution));
  execution.size = sizeof(execution);
  execution.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  return turbo_flow_coronet_register_socket_resolved_adapter_ex(flow, resolved, adapter_name,
                                                                &execution);
}
