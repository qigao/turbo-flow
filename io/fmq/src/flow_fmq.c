#include "turbo_flow_fmq.h"

#include "fmq_delivery.h"
#include "fmq_protocol.h"

#include "CoroNet/turbo_coro_socket.h"
#include "flow_coronet_actor.h"
#include "flow_coronet_execution.h"
#include "flow_coronet_runtime.h"
#include "flow_io_policy.h"
#include "flow_timer.h"
#include "fmt.h"
#include "turbo_byte_buffer.h"
#include "turbo_deque.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"
#include "turbo_vec.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_FMQ_DEFAULT_TIMEOUT_MS 1000u
#define FLOW_FMQ_RECONNECT_INITIAL_MS 1000u
#define FLOW_FMQ_RECONNECT_MAX_MS 30000u
#define FLOW_FMQ_MESSAGE_CONTEXT_MAGIC UINT32_C(0x464d5143)
#define FLOW_FMQ_JITTER_GAMMA UINT64_C(0x9e3779b97f4a7c15)
#define FLOW_FMQ_JITTER_MIX_A UINT64_C(0xbf58476d1ce4e5b9)
#define FLOW_FMQ_JITTER_MIX_B UINT64_C(0x94d049bb133111eb)

static atomic_uint_fast64_t flow_fmq_next_adapter_instance_id = 0u;

static const char FLOW_FMQ_CONNECTION_SCHEMA_TEXT[] =
    "schema TurboFlowFmqResource [id(1), version(1)];\n"
    "message ConnectionStatus {\n"
    "  uint32 state;\n"
    "  int32 last_status;\n"
    "  string connections_current;\n"
    "  string connection_limit;\n"
    "  string in_flight_messages;\n"
    "  string in_flight_bytes;\n"
    "}\n";
static const char FLOW_FMQ_QUEUE_SCHEMA_TEXT[] =
    "schema TurboFlowFmqResource [id(2), version(1)];\n"
    "message FmqQueueStatus {\n"
    "  string load;\n"
    "  string capacity;\n"
    "  string messages;\n"
    "  string bytes;\n"
    "  bool saturated;\n"
    "  bool accepting;\n"
    "}\n";
static const char FLOW_FMQ_PROTOCOL_SCHEMA_TEXT[] =
    "schema TurboFlowFmqResource [id(3), version(1)];\n"
    "message FmqProtocolStatus {\n"
    "  uint32 pattern;\n"
    "  uint32 mode;\n"
    "  uint32 transport;\n"
    "  uint32 pattern_state;\n"
    "  string sessions;\n"
    "  bool started;\n"
    "  bool quiesced;\n"
    "}\n";

static const turbo_flow_resource_schema_t FLOW_FMQ_CONNECTION_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_IO_TRANSPORT,
    TURBO_FLOW_RESOURCE_CONNECTION,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowFmqResource",
    "ConnectionStatus",
    1u,
    1u,
    FLOW_FMQ_CONNECTION_SCHEMA_TEXT};
static const turbo_flow_resource_schema_t FLOW_FMQ_QUEUE_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
    TURBO_FLOW_RESOURCE_QUEUE_BUFFER,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowFmqResource",
    "FmqQueueStatus",
    2u,
    1u,
    FLOW_FMQ_QUEUE_SCHEMA_TEXT};
static const turbo_flow_resource_schema_t FLOW_FMQ_PROTOCOL_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
    TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowFmqResource",
    "FmqProtocolStatus",
    3u,
    1u,
    FLOW_FMQ_PROTOCOL_SCHEMA_TEXT};

typedef turbo_flow_pattern_exchange_state_t flow_fmq_pattern_state_t;
#define FLOW_FMQ_PATTERN_READY TURBO_FLOW_PATTERN_EXCHANGE_READY
#define FLOW_FMQ_PATTERN_WAIT_REPLY TURBO_FLOW_PATTERN_EXCHANGE_WAIT_REPLY
#define FLOW_FMQ_PATTERN_PROCESSING_REQUEST TURBO_FLOW_PATTERN_EXCHANGE_PROCESSING_REQUEST
#define FLOW_FMQ_PATTERN_RESETTING TURBO_FLOW_PATTERN_EXCHANGE_RESETTING

static flow_fmq_pattern_state_t
flow_fmq_exchange_state(const turbo_flow_pattern_exchange_t *exchange) {
  turbo_flow_pattern_exchange_state_t state = TURBO_FLOW_PATTERN_EXCHANGE_RESETTING;
  uint64_t correlation_id = 0u;
  if (turbo_flow_pattern_exchange_snapshot(exchange, &state, &correlation_id) != TURBO_OK)
    return TURBO_FLOW_PATTERN_EXCHANGE_RESETTING;
  return state;
}

static void flow_fmq_timeout_config_resolve(tf_coronet_socket_timeout_config_t *timeouts,
                                            const turbo_flow_fmq_config_t *config) {
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
  if (timeouts->timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) timeouts->timeout_ms = 0;
  if (timeouts->connect_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED)
    timeouts->connect_timeout_ms = 0;
  if (timeouts->send_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) timeouts->send_timeout_ms = 0;
  if (timeouts->recv_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) timeouts->recv_timeout_ms = 0;
  if (timeouts->handshake_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED)
    timeouts->handshake_timeout_ms = 0;
  tf_coronet_socket_timeouts_resolve(timeouts, FLOW_FMQ_DEFAULT_TIMEOUT_MS);
}

static const char *const FLOW_FMQ_PATTERN_VALUES[] = {
    "pub", "sub", "push", "pull", "router", "dealer", "pair", "req", "rep", "xpub", "xsub"};
static const char *const FLOW_FMQ_MODE_VALUES[] = {"bind", "connect"};
static const char *const FLOW_FMQ_TRANSPORT_VALUES[] = {"tcp",  "tls", "udp", "kcp",
                                                        "pipe", "ws",  "wss"};
static const char *const FLOW_FMQ_METADATA_POLICY_VALUES[] = {"static", "inherit", "content"};
static const char *const FLOW_FMQ_FRAME_ADMISSION_POLICY_VALUES[] = {"fail", "block",
                                                                     "drop_oldest"};
static const char *const FLOW_FMQ_SLOW_PEER_POLICY_VALUES[] = {"fail", "drop_oldest", "disconnect"};
static const turbo_flow_option_field_t FLOW_FMQ_OPTION_FIELDS[] = {
    {"pattern", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0, FLOW_FMQ_PATTERN_VALUES,
     11},
    {"mode", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0, FLOW_FMQ_MODE_VALUES, 2},
    {"transport", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0,
     FLOW_FMQ_TRANSPORT_VALUES, 7},
    {"kcp_fec", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"kcp_fec_backend", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, TF_CORONET_KCP_FEC_BACKEND_VALUES, 2},
    {"kcp_fec_data_shards", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 256, NULL, 0},
    {"kcp_fec_parity_shards", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 256, NULL, 0},
    {"kcp_fec_max_payload_size", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, UINT16_MAX, NULL, 0},
    {"host", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"port", TURBO_FLOW_OPTION_U32, TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 65535,
     NULL, 0},
    {"path", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"topic", TURBO_FLOW_OPTION_STRING, 0, 0, TURBO_FLOW_FMQ_MAX_TOPIC_SIZE, NULL, 0},
    {"content_type", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"content_binding", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0,
     NULL, 0},
    {"identity", TURBO_FLOW_OPTION_STRING, 0, 0, TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE, NULL, 0},
    {"topic_policy", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_FMQ_METADATA_POLICY_VALUES, 3},
    {"identity_policy", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_FMQ_METADATA_POLICY_VALUES, 3},
    {"max_frame_size", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, UINT32_MAX, NULL, 0},
    {"max_connections", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, TURBO_FLOW_FMQ_MAX_CONNECTIONS_LIMIT,
     NULL, 0},
    {TF_CORONET_OPTION_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {TF_CORONET_OPTION_CONNECT_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {TF_CORONET_OPTION_SEND_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {TF_CORONET_OPTION_RECV_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {TF_CORONET_OPTION_HANDSHAKE_TIMEOUT_MS, TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"reconnect_initial_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"reconnect_max_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"heartbeat_interval_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"heartbeat_timeout_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"event_callback", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0,
     NULL, 0},
    {"event_ctx", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL, 0},
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
    {"frame_hwm_messages", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"frame_hwm_bytes", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"frame_admission_policy", TURBO_FLOW_OPTION_ENUM, 0, 0, 0,
     FLOW_FMQ_FRAME_ADMISSION_POLICY_VALUES, 3},
    {"frame_admission_timeout_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"frame_linger_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"peer_hwm_messages", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"peer_hwm_bytes", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"slow_peer_policy", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_FMQ_SLOW_PEER_POLICY_VALUES, 3},
    {"context", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL, 0},
    {"take_context_ownership", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0}};

typedef struct flow_fmq_adapter_s flow_fmq_adapter_t;

typedef enum flow_fmq_reactor_command_kind_e {
  FLOW_FMQ_REACTOR_START_BIND = 1,
  FLOW_FMQ_REACTOR_SCHEDULE_CONNECT,
  FLOW_FMQ_REACTOR_CLOSE_RESOURCES,
  FLOW_FMQ_REACTOR_CLEAR_PEERS
} flow_fmq_reactor_command_kind_t;

typedef struct flow_fmq_reactor_command_s {
  uint32_t kind;
  uint32_t reserved;
} flow_fmq_reactor_command_t;

/* Public lifecycle and resource-command entry points are host-serialized. */
#define FLOW_FMQ_REACTOR_COMMAND_CAPACITY 1u

typedef struct flow_fmq_resource_context_s {
  flow_fmq_adapter_t *adapter;
  turbo_flow_resource_kind_t kind;
} flow_fmq_resource_context_t;

typedef struct flow_fmq_reader_s {
  turbo_byte_buffer_t buffer;
} flow_fmq_reader_t;

typedef struct flow_fmq_subscription_s {
  tstr_t topic;
  size_t refs;
} flow_fmq_subscription_t;

typedef struct flow_fmq_send_request_s flow_fmq_send_request_t;
TURBO_DEQUE_DEFINE(flow_fmq_peer_send_requests, flow_fmq_send_request_t *)

typedef struct flow_fmq_peer_s {
  flow_fmq_adapter_t *adapter;
  coro_socket_t *socket;
  flow_fmq_route_token_t route;
  tstr_t identity;
  tstr_t topic;
  turbo_vec_t subscriptions;
  int subscriptions_initialized;
  flow_fmq_reader_t reader;
  flow_fmq_peer_send_requests send_queue;
  tf_io_budget_t send_budget;
  int send_queue_initialized;
  int send_drain_active;
  int closing;
  atomic_int disconnect_event_sent;
  turbo_flow_pattern_exchange_t exchange;
} flow_fmq_peer_t;

typedef struct flow_fmq_message_context_s {
  uint32_t magic;
  flow_fmq_adapter_t *adapter;
  flow_fmq_peer_t *peer;
  tstr_v identity;
  tstr_v topic;
  uint64_t correlation_id;
  int subscription;
  turbo_flow_content_descriptor_t content_descriptor;
} flow_fmq_message_context_t;

struct flow_fmq_send_request_s {
  flow_fmq_adapter_t *adapter;
  flow_fmq_route_token_t route;
  tstr_t frame;
  turbo_mutex_t mutex;
  turbo_cond_t cond;
  coro_wait_t *completion_wait;
  atomic_int refs;
  int done;
  int completion_wait_armed;
  int status;
  int cancel_requested;
  flow_fmq_delivery_stage_t delivery_stage;
  size_t budget_bytes;
  int budget_reserved;
  int drain_allowed;
  size_t fanout_pending;
  size_t fanout_delivered;
  size_t fanout_dropped;
  int fanout_active;
  int fanout_first_error;
  struct flow_fmq_send_request_s *drop_next;
};

TURBO_DEQUE_DEFINE(flow_fmq_send_requests, flow_fmq_send_request_t *)

struct flow_fmq_adapter_s {
  turbo_flow_t *flow;
  tf_coronet_execution_t execution;
  tf_coronet_actor_t reactor_actor;
  int reactor_actor_initialized;
  coro_context_t *ctx;
  coro_socket_t *server;
  coro_socket_t *client;
  turbo_vec_t peers;
  turbo_vec_t subscriptions;
  int subscriptions_initialized;
  tstr_t host;
  tstr_t path;
  tstr_t topic;
  tstr_t identity;
  tstr_t source_name;
  tstr_t resource_owner;
  tstr_t connection_uid;
  tstr_t queue_uid;
  tstr_t protocol_uid;
  tstr_t udp_multicast_group;
  tstr_t udp_multicast_interface;
  turbo_flow_fmq_pattern_t pattern;
  turbo_flow_fmq_endpoint_mode_t mode;
  turbo_flow_fmq_transport_t transport;
  turbo_flow_fmq_metadata_policy_t topic_policy;
  turbo_flow_fmq_metadata_policy_t identity_policy;
  turbo_kcp_fec_config_t kcp_fec;
  int port;
  size_t max_frame_size;
  uint32_t max_connections;
  tf_coronet_socket_timeout_config_t timeouts;
  tf_coronet_socket_options_t socket_options;
  tf_coronet_udp_options_t udp_options;
  size_t frame_hwm_messages;
  size_t frame_hwm_bytes;
  turbo_flow_fmq_frame_admission_policy_t frame_admission_policy;
  turbo_flow_fmq_fanout_config_t fanout;
  int fanout_enabled;
  uint64_t frame_linger_ms;
  uint64_t reconnect_initial_ms;
  uint64_t reconnect_max_ms;
  uint64_t reconnect_jitter_state;
  uint64_t heartbeat_interval_ms;
  uint64_t heartbeat_timeout_ms;
  turbo_flow_fmq_event_fn event_callback;
  void *event_ctx;
  turbo_flow_pattern_selector_t selector;
  int kcp_fec_configured;
  int initial_subscription_configured;
  int reuse_port;
  int start_refs;
  atomic_int started;
  atomic_int connect_status;
  atomic_int connection_state;
  atomic_int reconnect_failure_reported;
  atomic_int quiesced;
  atomic_size_t connections_current;
  turbo_flow_pattern_exchange_t exchange;
  tf_io_budget_t send_budget;
  flow_fmq_send_requests send_queue;
  turbo_mutex_t send_queue_mutex;
  turbo_mutex_t send_admission_mutex;
  int send_queue_initialized;
  int send_drain_active;
  uint64_t next_session_id;
  uint64_t instance_id;
  atomic_uint_fast64_t generation;
  flow_fmq_resource_context_t resource_contexts[3];
  atomic_uint_fast64_t next_message_id;
  coro_wait_t *reconnect_wait;
  tf_timer_t connect_wait;
  tf_timer_t retry_wait;
  int connect_wait_initialized;
  int retry_wait_initialized;
  turbo_mutex_t lane_task_mutex;
  turbo_cond_t lane_task_cond;
  size_t lane_task_count;
  int lane_task_sync_initialized;
  turbo_flow_content_descriptor_t data_descriptor;
  turbo_flow_content_descriptor_t control_descriptor;
  int has_data_descriptor;
};

static void flow_fmq_server_handler(coro_socket_t *client, void *arg);
static int flow_fmq_reserve_drop_oldest(flow_fmq_adapter_t *adapter, size_t frame_bytes);
static void flow_fmq_fail_peer_send_queue(flow_fmq_peer_t *peer, int status);
static void flow_fmq_request_release(flow_fmq_send_request_t *request);

static int flow_fmq_allocate_adapter_instance_id(uint64_t *out) {
  uint64_t current;
  if (!out) return TURBO_EINVAL;
  current = atomic_load_explicit(&flow_fmq_next_adapter_instance_id, memory_order_relaxed);
  for (;;) {
    if (current == UINT64_MAX) return TURBO_ERANGE;
    if (atomic_compare_exchange_weak_explicit(&flow_fmq_next_adapter_instance_id, &current,
                                              current + 1u, memory_order_relaxed,
                                              memory_order_relaxed)) {
      *out = current + 1u;
      return TURBO_OK;
    }
  }
}

static void flow_fmq_lane_task_begin(flow_fmq_adapter_t *adapter) {
  if (!adapter || !adapter->lane_task_sync_initialized) return;
  turbo_mutex_lock(&adapter->lane_task_mutex);
  adapter->lane_task_count += 1u;
  turbo_mutex_unlock(&adapter->lane_task_mutex);
}

static void flow_fmq_lane_task_end(flow_fmq_adapter_t *adapter) {
  if (!adapter || !adapter->lane_task_sync_initialized) return;
  turbo_mutex_lock(&adapter->lane_task_mutex);
  if (adapter->lane_task_count > 0u) adapter->lane_task_count -= 1u;
  if (adapter->lane_task_count == 0u) turbo_cond_broadcast(&adapter->lane_task_cond);
  turbo_mutex_unlock(&adapter->lane_task_mutex);
}

static void flow_fmq_wait_lane_tasks(flow_fmq_adapter_t *adapter) {
  if (!adapter || !adapter->lane_task_sync_initialized) return;
  turbo_mutex_lock(&adapter->lane_task_mutex);
  while (adapter->lane_task_count != 0u) {
    turbo_cond_wait(&adapter->lane_task_cond, &adapter->lane_task_mutex);
  }
  turbo_mutex_unlock(&adapter->lane_task_mutex);
}

static void flow_fmq_emit_event(flow_fmq_adapter_t *adapter, turbo_flow_fmq_event_kind_t kind,
                                int status, const flow_fmq_peer_t *peer, tstr_v peer_identity,
                                tstr_v peer_topic, uint64_t reconnect_delay_ms) {
  turbo_flow_fmq_event_t event = TURBO_FLOW_FMQ_EVENT_INIT;
  if (!adapter || !adapter->event_callback) return;
  event.kind = kind;
  event.status = status;
  event.pattern = adapter->pattern;
  event.mode = adapter->mode;
  event.transport = adapter->transport;
  if (peer) {
    if (peer->identity) event.peer_identity = tstr_to_v(peer->identity);
    if (peer->topic) event.peer_topic = tstr_to_v(peer->topic);
  }
  if (peer_identity.data || peer_identity.len != 0) event.peer_identity = peer_identity;
  if (peer_topic.data || peer_topic.len != 0) event.peer_topic = peer_topic;
  event.reconnect_delay_ms = reconnect_delay_ms;
  adapter->event_callback(adapter->event_ctx, &event);
}

static void flow_fmq_emit_peer_disconnected(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer,
                                            int status) {
  if (!adapter || !peer) return;
  if (atomic_exchange_explicit(&peer->disconnect_event_sent, 1, memory_order_acq_rel) != 0) {
    return;
  }
  flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_PEER_DISCONNECTED, status, peer, (tstr_v){0},
                      (tstr_v){0}, 0);
}

static void flow_fmq_emit_frame_event(flow_fmq_adapter_t *adapter, turbo_flow_fmq_event_kind_t kind,
                                      int status, size_t frame_bytes) {
  turbo_flow_fmq_event_t event = TURBO_FLOW_FMQ_EVENT_INIT;
  if (!adapter || !adapter->event_callback) return;
  event.kind = kind;
  event.status = status;
  event.pattern = adapter->pattern;
  event.mode = adapter->mode;
  event.transport = adapter->transport;
  event.frame_bytes = (uint64_t)frame_bytes;
  adapter->event_callback(adapter->event_ctx, &event);
}

static void flow_fmq_emit_peer_frame_event(flow_fmq_adapter_t *adapter,
                                           turbo_flow_fmq_event_kind_t kind, int status,
                                           const flow_fmq_peer_t *peer, size_t frame_bytes) {
  turbo_flow_fmq_event_t event = TURBO_FLOW_FMQ_EVENT_INIT;
  if (!adapter || !adapter->event_callback) return;
  event.kind = kind;
  event.status = status;
  event.pattern = adapter->pattern;
  event.mode = adapter->mode;
  event.transport = adapter->transport;
  if (peer) {
    if (peer->identity) event.peer_identity = tstr_to_v(peer->identity);
    if (peer->topic) event.peer_topic = tstr_to_v(peer->topic);
  }
  event.frame_bytes = (uint64_t)frame_bytes;
  adapter->event_callback(adapter->event_ctx, &event);
}

static void flow_fmq_emit_reconnect_failed_once(flow_fmq_adapter_t *adapter, int status) {
  if (!adapter || status == TURBO_OK || status == TURBO_ESHUTDOWN) return;
  if (atomic_exchange_explicit(&adapter->reconnect_failure_reported, 1, memory_order_acq_rel)) {
    return;
  }
  flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_RECONNECT_FAILED, status, NULL, (tstr_v){0},
                      (tstr_v){0}, 0);
}

static void flow_fmq_transition(flow_fmq_adapter_t *adapter, turbo_flow_connection_state_t state,
                                int status, size_t connections_current) {
  if (!adapter) return;
  atomic_store_explicit(&adapter->connections_current, connections_current, memory_order_relaxed);
  atomic_store_explicit(&adapter->connect_status, status, memory_order_relaxed);
  atomic_store_explicit(&adapter->connection_state, state, memory_order_release);
}

static tf_coronet_transport_t flow_fmq_coronet_transport(turbo_flow_fmq_transport_t transport) {
  switch (transport) {
  case TURBO_FLOW_FMQ_TCP:
    return TF_CORONET_TRANSPORT_TCP;
  case TURBO_FLOW_FMQ_TLS:
    return TF_CORONET_TRANSPORT_TLS;
  case TURBO_FLOW_FMQ_UDP:
    return TF_CORONET_TRANSPORT_UDP;
  case TURBO_FLOW_FMQ_KCP:
    return TF_CORONET_TRANSPORT_KCP;
  case TURBO_FLOW_FMQ_PIPE:
    return TF_CORONET_TRANSPORT_PIPE;
  case TURBO_FLOW_FMQ_WS:
    return TF_CORONET_TRANSPORT_WS;
  case TURBO_FLOW_FMQ_WSS:
    return TF_CORONET_TRANSPORT_WSS;
  default:
    return TF_CORONET_TRANSPORT_COUNT;
  }
}

static uint64_t flow_fmq_send_timeout_ns(const flow_fmq_adapter_t *adapter) {
  if (!adapter || adapter->timeouts.send_timeout_ms == 0) return 0;
  if (adapter->timeouts.send_timeout_ms > (UINT64_MAX / UINT64_C(1000000))) return UINT64_MAX;
  return adapter->timeouts.send_timeout_ms * UINT64_C(1000000);
}

static void flow_fmq_reader_destroy(flow_fmq_reader_t *reader) {
  if (!reader) return;
  turbo_byte_buffer_destroy(&reader->buffer);
}

static int flow_fmq_reader_consume(flow_fmq_reader_t *reader, size_t count) {
  if (!reader) return TURBO_EINVAL;
  return turbo_byte_buffer_consume(&reader->buffer, count);
}

static int flow_fmq_reader_next(coro_socket_t *socket, flow_fmq_reader_t *reader,
                                size_t max_frame_size, uint64_t receive_deadline_ns,
                                flow_fmq_frame_t *frame, size_t *consumed) {
  int rc;
  size_t encoded_limit;
  if (!socket || !reader || !frame || !consumed) return TURBO_EINVAL;
  rc = flow_fmq_encoded_size_limit(max_frame_size, &encoded_limit);
  if (rc != TURBO_OK) return rc;
  if (reader->buffer.max_bytes == 0u) {
    rc = turbo_byte_buffer_init(&reader->buffer, encoded_limit);
    if (rc != TURBO_OK) return rc;
  }
  for (;;) {
    turbo_byte_buffer_view_t view;
    rc = turbo_byte_buffer_view(&reader->buffer, &view);
    if (rc != TURBO_OK) return rc;
    rc = flow_fmq_decode_frame((const char *)view.data, view.size, max_frame_size, frame, consumed);
    if (rc != FLOW_FMQ_INCOMPLETE) return rc;
    {
      char *chunk = NULL;
      size_t chunk_len = 0;
      if (receive_deadline_ns != 0u) {
        uint64_t now_ns = turbo_hrtime();
        uint64_t remaining_ns;
        uint64_t timeout_ms;
        if (receive_deadline_ns <= now_ns) return TURBO_ETIMEDOUT;
        remaining_ns = receive_deadline_ns - now_ns;
        timeout_ms = remaining_ns > UINT64_MAX - UINT64_C(999999)
                         ? UINT64_MAX / UINT64_C(1000000)
                         : (remaining_ns + UINT64_C(999999)) / UINT64_C(1000000);
        if (timeout_ms == 0u) timeout_ms = 1u;
        coro_socket_set_timeout(socket, timeout_ms);
      }
      rc = coro_socket_recv(socket, &chunk, &chunk_len);
      if (rc != TURBO_OK) return rc;
      if (!chunk || chunk_len == 0) {
        if (chunk) coro_socket_free_recv(chunk);
        return TURBO_EOF;
      }
      if (chunk_len > turbo_byte_buffer_available(&reader->buffer)) {
        coro_socket_free_recv(chunk);
        return TURBO_EMSGSIZE;
      }
      rc = turbo_byte_buffer_append(&reader->buffer, chunk, chunk_len);
      coro_socket_free_recv(chunk);
      if (rc != TURBO_OK) return rc;
    }
  }
}

static int flow_fmq_socket_send(flow_fmq_adapter_t *adapter, coro_socket_t *socket,
                                const char *data, size_t len) {
  if (!adapter || !socket || !data || len == 0) return TURBO_EINVAL;
  (void)tf_coronet_apply_socket_timeout(socket, &adapter->timeouts, TF_CORONET_TIMEOUT_SEND);
  return tf_coronet_send_socket(socket, flow_fmq_coronet_transport(adapter->transport), data, len);
}

static int flow_fmq_send_hello(flow_fmq_adapter_t *adapter, coro_socket_t *socket) {
  flow_fmq_frame_t hello;
  tstr_t encoded = NULL;
  int rc;
  memset(&hello, 0, sizeof(hello));
  hello.kind = FLOW_FMQ_FRAME_HELLO;
  hello.pattern = adapter->pattern;
  if (adapter->identity) hello.identity = tstr_to_v(adapter->identity);
  if (adapter->topic) hello.topic = tstr_to_v(adapter->topic);
  rc = flow_fmq_encode_frame(&hello, adapter->max_frame_size, &encoded);
  if (rc == TURBO_OK) rc = flow_fmq_socket_send(adapter, socket, encoded, tstr_len(encoded));
  tstr_freep(&encoded);
  return rc;
}

static int flow_fmq_send_control(flow_fmq_adapter_t *adapter, coro_socket_t *socket,
                                 flow_fmq_frame_kind_t kind) {
  flow_fmq_frame_t frame;
  tstr_t encoded = NULL;
  int rc;
  if (!adapter || !socket || (kind != FLOW_FMQ_FRAME_PING && kind != FLOW_FMQ_FRAME_PONG)) {
    return TURBO_EINVAL;
  }
  memset(&frame, 0, sizeof(frame));
  frame.kind = kind;
  frame.pattern = adapter->pattern;
  rc = flow_fmq_encode_frame(&frame, adapter->max_frame_size, &encoded);
  if (rc == TURBO_OK) rc = flow_fmq_socket_send(adapter, socket, encoded, tstr_len(encoded));
  tstr_freep(&encoded);
  return rc;
}

static int flow_fmq_send_subscription(flow_fmq_adapter_t *adapter, coro_socket_t *socket,
                                      flow_fmq_frame_kind_t kind, tstr_v topic) {
  flow_fmq_frame_t frame;
  tstr_t encoded = NULL;
  int rc;
  if (!adapter || !socket ||
      (adapter->pattern != TURBO_FLOW_FMQ_SUB && adapter->pattern != TURBO_FLOW_FMQ_XSUB) ||
      (kind != FLOW_FMQ_FRAME_SUBSCRIBE && kind != FLOW_FMQ_FRAME_UNSUBSCRIBE)) {
    return TURBO_EINVAL;
  }
  memset(&frame, 0, sizeof(frame));
  frame.kind = kind;
  frame.pattern = adapter->pattern;
  frame.topic = topic;
  rc = flow_fmq_encode_frame(&frame, adapter->max_frame_size, &encoded);
  if (rc == TURBO_OK) rc = flow_fmq_socket_send(adapter, socket, encoded, tstr_len(encoded));
  tstr_freep(&encoded);
  return rc;
}

static int flow_fmq_read_hello(flow_fmq_adapter_t *adapter, coro_socket_t *socket,
                               flow_fmq_reader_t *reader, flow_fmq_frame_t *hello,
                               size_t *consumed) {
  int rc = flow_fmq_reader_next(socket, reader, adapter->max_frame_size, 0u, hello, consumed);
  if (rc != TURBO_OK) return rc;
  if (hello->kind != FLOW_FMQ_FRAME_HELLO || hello->payload.len != 0 ||
      !turbo_flow_fmq_patterns_compatible(adapter->pattern, hello->pattern)) {
    flow_fmq_frame_cleanup(hello);
    return TURBO_EPROTO;
  }
  if (hello->pattern == TURBO_FLOW_FMQ_DEALER && hello->identity.len == 0) {
    flow_fmq_frame_cleanup(hello);
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flow_fmq_add_peer(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer) {
  int rc;
  if (turbo_vec_size(&adapter->peers) >= adapter->max_connections) return TURBO_ENOBUFS;
  if (adapter->pattern == TURBO_FLOW_FMQ_PAIR && !turbo_vec_empty(&adapter->peers)) {
    return TURBO_EBUSY;
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_ROUTER || adapter->fanout_enabled) {
    for (size_t i = 0; i < turbo_vec_size(&adapter->peers); ++i) {
      flow_fmq_peer_t *const *existing =
          (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, i);
      if (existing && *existing && tstr_cmp((*existing)->identity, peer->identity) == 0) {
        return TURBO_EALREADY;
      }
    }
  }
  rc = turbo_vec_push(&adapter->peers, &peer);
  if (rc == TURBO_OK)
    atomic_fetch_add_explicit(&adapter->connections_current, 1u, memory_order_release);
  return rc;
}

static flow_fmq_peer_t *flow_fmq_find_peer(flow_fmq_adapter_t *adapter,
                                           flow_fmq_route_token_t route) {
  if (!adapter) return NULL;
  for (size_t i = 0; i < turbo_vec_size(&adapter->peers); ++i) {
    flow_fmq_peer_t *const *peer = (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, i);
    if (peer && *peer &&
        flow_fmq_route_matches(atomic_load_explicit(&adapter->generation, memory_order_acquire),
                               route, (*peer)->route)) {
      return *peer;
    }
  }
  return NULL;
}

static void flow_fmq_remove_peer(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer) {
  size_t count;
  if (!adapter || !peer) return;
  count = turbo_vec_size(&adapter->peers);
  for (size_t i = 0; i < count; ++i) {
    flow_fmq_peer_t **slot = (flow_fmq_peer_t **)turbo_vec_at(&adapter->peers, i);
    if (slot && *slot == peer) {
      if (i + 1u < count) {
        flow_fmq_peer_t *const *last =
            (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, count - 1u);
        *slot = last ? *last : NULL;
      }
      (void)turbo_vec_resize(&adapter->peers, count - 1u);
      atomic_fetch_sub_explicit(&adapter->connections_current, 1u, memory_order_release);
      return;
    }
  }
}

static void flow_fmq_interrupt_peers(flow_fmq_adapter_t *adapter) {
  if (!adapter) return;
  for (size_t i = 0; i < turbo_vec_size(&adapter->peers); ++i) {
    flow_fmq_peer_t *const *peer = (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, i);
    if (peer && *peer && (*peer)->socket) {
      flow_fmq_emit_peer_disconnected(adapter, *peer, TURBO_ESHUTDOWN);
      (void)coro_socket_interrupt_wait((*peer)->socket, TURBO_ESHUTDOWN);
    }
  }
}

static void flow_fmq_subscriptions_clear(turbo_vec_t *subscriptions) {
  if (!subscriptions) return;
  for (size_t i = 0; i < turbo_vec_size(subscriptions); ++i) {
    flow_fmq_subscription_t *subscription =
        (flow_fmq_subscription_t *)turbo_vec_at(subscriptions, i);
    if (subscription) tstr_freep(&subscription->topic);
  }
  turbo_vec_clear(subscriptions);
}

static void flow_fmq_peer_subscriptions_destroy(flow_fmq_peer_t *peer) {
  if (!peer || !peer->subscriptions_initialized) return;
  flow_fmq_subscriptions_clear(&peer->subscriptions);
  turbo_vec_destroy(&peer->subscriptions);
  peer->subscriptions_initialized = 0;
}

static int flow_fmq_peer_send_queue_init(flow_fmq_peer_t *peer) {
  tf_io_budget_config_t budget_config;
  if (!peer || !peer->adapter) return TURBO_EINVAL;
  if (!peer->adapter->fanout_enabled) return TURBO_OK;
  if (flow_fmq_peer_send_requests_init(&peer->send_queue) != TURBO_OK) return TURBO_ENOMEM;
  memset(&budget_config, 0, sizeof(budget_config));
  budget_config.max_messages = peer->adapter->fanout.peer_hwm_messages;
  budget_config.max_bytes = peer->adapter->fanout.peer_hwm_bytes;
  budget_config.admission = TF_IO_ADMISSION_FAIL;
  if (tf_io_budget_init(&peer->send_budget, &budget_config) != TURBO_OK) {
    flow_fmq_peer_send_requests_destroy(&peer->send_queue);
    return TURBO_ENOMEM;
  }
  if (tf_io_budget_open(&peer->send_budget) != TURBO_OK) {
    tf_io_budget_destroy(&peer->send_budget);
    flow_fmq_peer_send_requests_destroy(&peer->send_queue);
    return TURBO_EIO;
  }
  peer->send_queue_initialized = 1;
  return TURBO_OK;
}

static void flow_fmq_peer_send_queue_destroy(flow_fmq_peer_t *peer) {
  if (!peer || !peer->send_queue_initialized) return;
  flow_fmq_peer_send_requests_destroy(&peer->send_queue);
  tf_io_budget_destroy(&peer->send_budget);
  peer->send_queue_initialized = 0;
}

static int flow_fmq_subscription_update(turbo_vec_t *subscriptions, int subscribe, tstr_v topic,
                                        int *changed) {
  if (!subscriptions || !changed || (topic.len > 0 && !topic.data)) {
    return TURBO_EINVAL;
  }
  *changed = 0;
  for (size_t i = 0; i < turbo_vec_size(subscriptions); ++i) {
    flow_fmq_subscription_t *existing = (flow_fmq_subscription_t *)turbo_vec_at(subscriptions, i);
    size_t existing_len = existing && existing->topic ? tstr_len(existing->topic) : 0u;
    if (existing_len != topic.len ||
        (topic.len > 0 && memcmp(existing->topic, topic.data, topic.len) != 0)) {
      continue;
    }
    if (subscribe) {
      if (existing->refs == SIZE_MAX) return TURBO_ERANGE;
      existing->refs += 1u;
      *changed = 1;
    } else if (existing->refs > 1u) {
      existing->refs -= 1u;
      *changed = 1;
    } else {
      flow_fmq_subscription_t removed;
      int rc = turbo_vec_swap_remove(subscriptions, i, &removed);
      if (rc != TURBO_OK) return rc;
      tstr_freep(&removed.topic);
      *changed = 1;
    }
    return TURBO_OK;
  }
  if (!subscribe) return TURBO_OK;
  {
    flow_fmq_subscription_t subscription;
    int rc;
    memset(&subscription, 0, sizeof(subscription));
    subscription.topic = tstr_new_len(topic.data, topic.len);
    subscription.refs = 1u;
    if (!subscription.topic) return TURBO_ENOMEM;
    rc = turbo_vec_push(subscriptions, &subscription);
    if (rc != TURBO_OK) {
      tstr_free(subscription.topic);
      return rc;
    }
  }
  *changed = 1;
  return TURBO_OK;
}

static int flow_fmq_peer_subscription_update(flow_fmq_peer_t *peer, int subscribe, tstr_v topic,
                                             int *changed) {
  if (!peer || !peer->subscriptions_initialized) return TURBO_EINVAL;
  return flow_fmq_subscription_update(&peer->subscriptions, subscribe, topic, changed);
}

static int flow_fmq_replay_subscriptions(flow_fmq_adapter_t *adapter, coro_socket_t *socket) {
  if (!adapter || !socket || adapter->pattern != TURBO_FLOW_FMQ_XSUB ||
      !adapter->subscriptions_initialized) {
    return TURBO_EINVAL;
  }
  for (size_t i = 0; i < turbo_vec_size(&adapter->subscriptions); ++i) {
    const flow_fmq_subscription_t *subscription =
        (const flow_fmq_subscription_t *)turbo_vec_at_const(&adapter->subscriptions, i);
    if (!subscription) continue;
    for (size_t ref = 0; ref < subscription->refs; ++ref) {
      int rc = flow_fmq_send_subscription(adapter, socket, FLOW_FMQ_FRAME_SUBSCRIBE,
                                          tstr_to_v(subscription->topic));
      if (rc != TURBO_OK) return rc;
    }
  }
  return TURBO_OK;
}

static int flow_fmq_publish(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer,
                            const flow_fmq_frame_t *frame) {
  flow_fmq_message_context_t message_context;
  turbo_flow_msg_t msg;
  int rc;
  if (!adapter || !adapter->flow || !adapter->source_name || !frame) return TURBO_EINVAL;
  memset(&message_context, 0, sizeof(message_context));
  message_context.magic = FLOW_FMQ_MESSAGE_CONTEXT_MAGIC;
  message_context.adapter = adapter;
  message_context.peer = peer;
  message_context.topic = frame->topic;
  message_context.correlation_id = frame->message_id;
  if (frame->kind == FLOW_FMQ_FRAME_SUBSCRIBE) message_context.subscription = 1;
  if (frame->kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) message_context.subscription = -1;
  if (peer && peer->identity) message_context.identity = tstr_to_v(peer->identity);
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_new_len(frame->payload.data, frame->payload.len);
  if (!msg.owned_payload) return TURBO_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  msg.transport_context = &message_context;
  if (frame->kind == FLOW_FMQ_FRAME_DATA && adapter->has_data_descriptor) {
    message_context.content_descriptor = adapter->data_descriptor;
    if (frame->topic.len > TURBO_FLOW_CONTENT_IDENTITY_MAX) {
      turbo_flow_msg_cleanup(&msg);
      return TURBO_ENOSPC;
    }
    if (frame->topic.len > 0u) {
      memcpy(message_context.content_descriptor.identity, frame->topic.data, frame->topic.len);
    }
    message_context.content_descriptor.identity[frame->topic.len] = '\0';
    rc = turbo_flow_msg_set_content_descriptor(&msg, &message_context.content_descriptor);
  } else if (frame->kind == FLOW_FMQ_FRAME_SUBSCRIBE || frame->kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) {
    message_context.content_descriptor = adapter->control_descriptor;
    if (frame->topic.len > TURBO_FLOW_CONTENT_IDENTITY_MAX) {
      turbo_flow_msg_cleanup(&msg);
      return TURBO_ENOSPC;
    }
    if (frame->topic.len > 0u) {
      memcpy(message_context.content_descriptor.identity, frame->topic.data, frame->topic.len);
    }
    message_context.content_descriptor.identity[frame->topic.len] = '\0';
    rc = turbo_flow_msg_set_content_descriptor(&msg, &message_context.content_descriptor);
  } else {
    rc = TURBO_OK;
  }
  if (rc != TURBO_OK) {
    turbo_flow_msg_cleanup(&msg);
    return rc;
  }
  rc = turbo_flow_publish(adapter->flow, adapter->source_name, &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static int flow_fmq_receive_subscription(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer,
                                         const flow_fmq_frame_t *frame) {
  int changed;
  int rc;
  if (!adapter || adapter->pattern != TURBO_FLOW_FMQ_XPUB || !peer || !frame ||
      (frame->kind != FLOW_FMQ_FRAME_SUBSCRIBE && frame->kind != FLOW_FMQ_FRAME_UNSUBSCRIBE)) {
    return TURBO_EPROTO;
  }
  rc = flow_fmq_peer_subscription_update(peer, frame->kind == FLOW_FMQ_FRAME_SUBSCRIBE,
                                         frame->topic, &changed);
  if (rc != TURBO_OK || !changed) return rc;
  if (!adapter->source_name) return TURBO_OK;
  return flow_fmq_publish(adapter, peer, frame);
}

static void flow_fmq_publish_peer_unsubscriptions(flow_fmq_adapter_t *adapter,
                                                  flow_fmq_peer_t *peer) {
  if (!adapter || adapter->pattern != TURBO_FLOW_FMQ_XPUB || !peer ||
      !peer->subscriptions_initialized || !adapter->source_name) {
    return;
  }
  for (size_t i = 0; i < turbo_vec_size(&peer->subscriptions); ++i) {
    const flow_fmq_subscription_t *subscription =
        (const flow_fmq_subscription_t *)turbo_vec_at_const(&peer->subscriptions, i);
    flow_fmq_frame_t frame;
    if (!subscription) continue;
    memset(&frame, 0, sizeof(frame));
    frame.kind = FLOW_FMQ_FRAME_UNSUBSCRIBE;
    frame.pattern = TURBO_FLOW_FMQ_XSUB;
    frame.topic = tstr_to_v(subscription->topic);
    for (size_t ref = 0; ref < subscription->refs; ++ref)
      (void)flow_fmq_publish(adapter, peer, &frame);
  }
}

static int flow_fmq_receive_data(flow_fmq_adapter_t *adapter, flow_fmq_peer_t *peer,
                                 flow_fmq_reader_t *reader, coro_socket_t *socket) {
  flow_fmq_heartbeat_deadlines_t heartbeat;
  uint64_t base_recv_timeout_ms;
  int heartbeat_enabled;
  if (!adapter || !reader || !socket) return TURBO_EINVAL;
  heartbeat_enabled = adapter->heartbeat_interval_ms != 0 && adapter->heartbeat_timeout_ms != 0;
  base_recv_timeout_ms = adapter->timeouts.recv_timeout_ms;
  flow_fmq_heartbeat_deadlines_init(&heartbeat, turbo_hrtime(), adapter->heartbeat_interval_ms,
                                    adapter->heartbeat_timeout_ms, base_recv_timeout_ms);
  while (atomic_load_explicit(&adapter->started, memory_order_acquire) && adapter->flow &&
         turbo_flow_state(adapter->flow) != TURBO_FLOW_STATE_STARTED) {
    coro_sleep(adapter->ctx, 1);
  }
  while (atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    flow_fmq_frame_t frame;
    size_t consumed = 0;
    int rc;
    uint64_t receive_deadline_ns = 0u;
    if (heartbeat_enabled) {
      for (;;) {
        uint64_t now_ns = turbo_hrtime();
        flow_fmq_heartbeat_action_t action =
            flow_fmq_heartbeat_deadlines_next(&heartbeat, now_ns, &receive_deadline_ns);
        if (action == FLOW_FMQ_HEARTBEAT_EXPIRED) {
          flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_HEARTBEAT_TIMEOUT, TURBO_ETIMEDOUT,
                              peer, (tstr_v){0}, (tstr_v){0}, 0);
          return TURBO_ETIMEDOUT;
        }
        if (action == FLOW_FMQ_HEARTBEAT_RECV_EXPIRED) return TURBO_ETIMEDOUT;
        if (action == FLOW_FMQ_HEARTBEAT_SEND_PING) {
          rc = flow_fmq_send_control(adapter, socket, FLOW_FMQ_FRAME_PING);
          if (rc != TURBO_OK) return rc;
          flow_fmq_heartbeat_deadlines_on_ping(&heartbeat, now_ns);
          continue;
        }
        break;
      }
    }
    rc = flow_fmq_reader_next(socket, reader, adapter->max_frame_size, receive_deadline_ns, &frame,
                              &consumed);
    if (rc == TURBO_ETIMEDOUT && heartbeat_enabled) continue;
    if (rc != TURBO_OK) return rc;
    if (heartbeat_enabled) {
      flow_fmq_heartbeat_deadlines_on_receive(&heartbeat, turbo_hrtime());
    }
    if (!turbo_flow_fmq_patterns_compatible(adapter->pattern, frame.pattern)) {
      flow_fmq_frame_cleanup(&frame);
      return TURBO_EPROTO;
    }
    if (frame.kind == FLOW_FMQ_FRAME_PING) {
      rc = flow_fmq_send_control(adapter, socket, FLOW_FMQ_FRAME_PONG);
      flow_fmq_reader_consume(reader, consumed);
      flow_fmq_frame_cleanup(&frame);
      if (rc != TURBO_OK) return rc;
      continue;
    }
    if (frame.kind == FLOW_FMQ_FRAME_PONG) {
      flow_fmq_reader_consume(reader, consumed);
      flow_fmq_frame_cleanup(&frame);
      continue;
    }
    if (frame.kind == FLOW_FMQ_FRAME_SUBSCRIBE || frame.kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) {
      rc = flow_fmq_receive_subscription(adapter, peer, &frame);
      flow_fmq_reader_consume(reader, consumed);
      flow_fmq_frame_cleanup(&frame);
      if (rc != TURBO_OK) return rc;
      continue;
    }
    if (frame.kind != FLOW_FMQ_FRAME_DATA) {
      flow_fmq_frame_cleanup(&frame);
      return TURBO_EPROTO;
    }
    if (adapter->pattern == TURBO_FLOW_FMQ_PUB || adapter->pattern == TURBO_FLOW_FMQ_PUSH ||
        adapter->pattern == TURBO_FLOW_FMQ_ROUTER || adapter->pattern == TURBO_FLOW_FMQ_XPUB) {
      if (adapter->pattern != TURBO_FLOW_FMQ_ROUTER) {
        flow_fmq_frame_cleanup(&frame);
        return TURBO_EPROTO;
      }
    }
    if (adapter->pattern == TURBO_FLOW_FMQ_REQ) {
      if (turbo_flow_pattern_exchange_match(&adapter->exchange, FLOW_FMQ_PATTERN_WAIT_REPLY,
                                            frame.message_id) != TURBO_OK) {
        flow_fmq_frame_cleanup(&frame);
        return TURBO_EPROTO;
      }
    } else if (adapter->pattern == TURBO_FLOW_FMQ_REP) {
      if (!peer || turbo_flow_pattern_exchange_begin(&peer->exchange,
                                                      FLOW_FMQ_PATTERN_PROCESSING_REQUEST,
                                                      frame.message_id) != TURBO_OK) {
        flow_fmq_frame_cleanup(&frame);
        return TURBO_EPROTO;
      }
    }
    rc = flow_fmq_publish(adapter, peer, &frame);
    if (adapter->pattern == TURBO_FLOW_FMQ_REQ) {
      int finish_rc = turbo_flow_pattern_exchange_finish(
          &adapter->exchange, FLOW_FMQ_PATTERN_WAIT_REPLY, frame.message_id,
          rc == TURBO_OK ? FLOW_FMQ_PATTERN_READY : FLOW_FMQ_PATTERN_RESETTING);
      if (finish_rc != TURBO_OK && rc == TURBO_OK) rc = finish_rc;
    } else if (adapter->pattern == TURBO_FLOW_FMQ_REP) {
      if (rc == TURBO_OK && flow_fmq_exchange_state(&peer->exchange) != FLOW_FMQ_PATTERN_READY) {
        rc = TURBO_EPROTO;
      }
      if (rc != TURBO_OK) (void)turbo_flow_pattern_exchange_mark_resetting(&peer->exchange);
    }
    flow_fmq_reader_consume(reader, consumed);
    flow_fmq_frame_cleanup(&frame);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_ESHUTDOWN;
}

static void flow_fmq_server_handler(coro_socket_t *client, void *arg) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)arg;
  flow_fmq_peer_t *peer = NULL;
  flow_fmq_frame_t hello;
  size_t consumed = 0;
  int peer_added = 0;
  int rc;
  if (!adapter) return;
  flow_fmq_lane_task_begin(adapter);
  if (!client || !atomic_load_explicit(&adapter->started, memory_order_acquire)) goto finished;
  peer = (flow_fmq_peer_t *)calloc(1, sizeof(*peer));
  if (!peer) goto finished;
  rc = turbo_vec_init(&peer->subscriptions, sizeof(flow_fmq_subscription_t));
  if (rc != TURBO_OK) goto done;
  peer->subscriptions_initialized = 1;
  peer->adapter = adapter;
  peer->socket = client;
  adapter->next_session_id += 1u;
  if (adapter->next_session_id == 0u) adapter->next_session_id += 1u;
  peer->route.session_id = adapter->next_session_id;
  peer->route.generation = atomic_load_explicit(&adapter->generation, memory_order_acquire);
  rc = turbo_flow_pattern_exchange_init(&peer->exchange);
  if (rc != TURBO_OK) goto done;
  (void)tf_coronet_apply_socket_timeout(client, &adapter->timeouts, TF_CORONET_TIMEOUT_RECV);
  rc = flow_fmq_read_hello(adapter, client, &peer->reader, &hello, &consumed);
  if (rc != TURBO_OK) goto done;
  if (adapter->fanout_enabled && hello.identity.len == 0u) {
    rc = TURBO_EPROTO;
    goto done;
  }
  peer->identity = tstr_from_v(hello.identity);
  peer->topic = tstr_from_v(hello.topic);
  if ((hello.identity.len > 0 && !peer->identity) || (hello.topic.len > 0 && !peer->topic)) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = flow_fmq_peer_send_queue_init(peer);
  if (rc != TURBO_OK) goto done;
  flow_fmq_reader_consume(&peer->reader, consumed);
  rc = flow_fmq_send_hello(adapter, client);
  if (rc != TURBO_OK) goto done;
  rc = flow_fmq_add_peer(adapter, peer);
  if (rc != TURBO_OK) goto done;
  peer_added = 1;
  flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_PEER_CONNECTED, TURBO_OK, peer, (tstr_v){0},
                      (tstr_v){0}, 0);
  rc = flow_fmq_receive_data(adapter, peer, &peer->reader, client);
  peer->closing = 1;
  flow_fmq_publish_peer_unsubscriptions(adapter, peer);
  flow_fmq_emit_peer_disconnected(adapter, peer, rc);
  flow_fmq_remove_peer(adapter, peer);
  peer_added = 0;
done:
  (void)rc;
  if (peer) {
    peer->closing = 1;
    if (peer_added) flow_fmq_remove_peer(adapter, peer);
    if (peer->socket) {
      (void)coro_socket_interrupt_wait(peer->socket, rc == TURBO_OK ? TURBO_ENOTCONN : rc);
    }
    flow_fmq_fail_peer_send_queue(peer, rc == TURBO_OK ? TURBO_ENOTCONN : rc);
    while (peer->send_drain_active)
      coro_sleep(adapter->ctx, 1);
    flow_fmq_peer_send_queue_destroy(peer);
    flow_fmq_peer_subscriptions_destroy(peer);
    flow_fmq_reader_destroy(&peer->reader);
    tstr_freep(&peer->identity);
    tstr_freep(&peer->topic);
    free(peer);
  }
finished:
  flow_fmq_lane_task_end(adapter);
}

static uint64_t flow_fmq_next_reconnect_delay_ms(const flow_fmq_adapter_t *adapter,
                                                 uint64_t current_delay_ms) {
  if (!adapter || current_delay_ms == 0) {
    return adapter ? adapter->reconnect_initial_ms : FLOW_FMQ_RECONNECT_INITIAL_MS;
  }
  if (!adapter->reconnect_max_ms) {
    return current_delay_ms > UINT64_MAX / 2u ? UINT64_MAX : current_delay_ms * 2u;
  }
  if (current_delay_ms >= adapter->reconnect_max_ms) return adapter->reconnect_max_ms;
  if (current_delay_ms > adapter->reconnect_max_ms / 2u) return adapter->reconnect_max_ms;
  return current_delay_ms * 2u;
}

static uint64_t flow_fmq_jitter_reconnect_delay_ms(flow_fmq_adapter_t *adapter,
                                                   uint64_t base_delay_ms) {
  uint64_t value;
  uint64_t minimum_delay_ms;
  uint64_t span_ms;
  if (!adapter || base_delay_ms <= 1u) return base_delay_ms;

  /* SplitMix64 keeps jitter state local to the single-owner connect task. */
  adapter->reconnect_jitter_state += FLOW_FMQ_JITTER_GAMMA;
  value = adapter->reconnect_jitter_state;
  value = (value ^ (value >> 30u)) * FLOW_FMQ_JITTER_MIX_A;
  value = (value ^ (value >> 27u)) * FLOW_FMQ_JITTER_MIX_B;
  value ^= value >> 31u;

  minimum_delay_ms = base_delay_ms / 2u + base_delay_ms % 2u;
  span_ms = base_delay_ms - minimum_delay_ms;
  return minimum_delay_ms + value % span_ms;
}

static int flow_fmq_reconnect_sleep(flow_fmq_adapter_t *adapter, uint64_t delay_ms) {
  if (!adapter || !adapter->reconnect_wait) return TURBO_EINVAL;
  return coro_wait_for(adapter->reconnect_wait, delay_ms);
}

static uint64_t flow_fmq_connect_timeout_ns(const flow_fmq_adapter_t *adapter) {
  uint64_t timeout_ms;
  if (!adapter) return 0;
  timeout_ms = adapter->timeouts.connect_timeout_ms ? adapter->timeouts.connect_timeout_ms
                                                    : adapter->timeouts.timeout_ms;
  if (timeout_ms == 0) timeout_ms = FLOW_FMQ_DEFAULT_TIMEOUT_MS;
  if (timeout_ms > (UINT64_MAX / UINT64_C(1000000))) return UINT64_MAX;
  return timeout_ms * UINT64_C(1000000);
}

static void flow_fmq_connect_task(coro_t *co, void *arg) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)arg;
  uint64_t reconnect_backoff_ms;
  flow_fmq_reader_t reader;
  flow_fmq_frame_t hello;
  size_t consumed = 0;
  coro_socket_t *socket;
  int rc;
  uint64_t wait_ms;

  (void)co;
  reconnect_backoff_ms = 0u;
  memset(&reader, 0, sizeof(reader));
  for (;;) {
    if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) {
      rc = TURBO_ESHUTDOWN;
      break;
    }
    memset(&reader, 0, sizeof(reader));
    flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_EALREADY, 0u);
    consumed = 0;
    memset(&hello, 0, sizeof(hello));
    socket = tf_coronet_create_client_socket(adapter->ctx,
                                             flow_fmq_coronet_transport(adapter->transport));
    if (!socket) {
      rc = TURBO_ENOMEM;
      flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_FAILED, rc, 0u);
      tf_timer_signal(&adapter->connect_wait);
      break;
    }
    rc = tf_coronet_apply_kcp_fec(socket, flow_fmq_coronet_transport(adapter->transport),
                                  &adapter->kcp_fec, adapter->kcp_fec_configured);
    if (rc != TURBO_OK) {
      coro_socket_destroy(socket);
      flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_FAILED, rc, 0u);
      tf_timer_signal(&adapter->connect_wait);
      break;
    }
    rc = tf_coronet_apply_socket_options(socket, flow_fmq_coronet_transport(adapter->transport),
                                         &adapter->socket_options);
    if (rc != TURBO_OK) {
      coro_socket_destroy(socket);
      flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_FAILED, rc, 0u);
      tf_timer_signal(&adapter->connect_wait);
      break;
    }
    {
      uint64_t connection_timeout_ms = 0;
      rc = tf_coronet_connection_timeout_resolve(flow_fmq_coronet_transport(adapter->transport),
                                                 &adapter->timeouts, &connection_timeout_ms);
      if (rc == TURBO_OK) coro_socket_set_timeout(socket, connection_timeout_ms);
    }
    adapter->client = socket;
    if (rc == TURBO_OK) {
      rc = tf_coronet_connect_socket(socket, flow_fmq_coronet_transport(adapter->transport),
                                     adapter->host, adapter->port, adapter->path);
    }
    if (rc == TURBO_OK) {
      rc = tf_coronet_apply_udp_options(socket, flow_fmq_coronet_transport(adapter->transport),
                                        &adapter->udp_options);
    }
    (void)tf_coronet_apply_socket_timeout(socket, &adapter->timeouts, TF_CORONET_TIMEOUT_RECV);
    if (rc == TURBO_OK) rc = flow_fmq_send_hello(adapter, socket);
    if (rc == TURBO_OK) rc = flow_fmq_read_hello(adapter, socket, &reader, &hello, &consumed);
    if (rc == TURBO_OK) {
      flow_fmq_reader_consume(&reader, consumed);
      if ((adapter->pattern == TURBO_FLOW_FMQ_SUB ||
           (adapter->pattern == TURBO_FLOW_FMQ_XSUB && adapter->initial_subscription_configured)) &&
          hello.pattern == TURBO_FLOW_FMQ_XPUB) {
        rc = flow_fmq_send_subscription(adapter, socket, FLOW_FMQ_FRAME_SUBSCRIBE,
                                        tstr_to_v(adapter->topic));
      }
      if (rc == TURBO_OK && adapter->pattern == TURBO_FLOW_FMQ_XSUB &&
          hello.pattern == TURBO_FLOW_FMQ_XPUB) {
        rc = flow_fmq_replay_subscriptions(adapter, socket);
      }
    }
    if (rc == TURBO_OK) {
      reconnect_backoff_ms = 0u;
      if (adapter->pattern == TURBO_FLOW_FMQ_REQ) {
        rc = turbo_flow_pattern_exchange_reset(&adapter->exchange);
      }
      flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_READY, TURBO_OK, 1u);
      flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_RECONNECT_SUCCEEDED, TURBO_OK, NULL,
                          hello.identity, hello.topic, 0);
      tf_timer_signal(&adapter->connect_wait);
      rc = flow_fmq_receive_data(adapter, NULL, &reader, socket);
      if (rc != TURBO_OK) {
        if (adapter->pattern == TURBO_FLOW_FMQ_REQ) {
          (void)turbo_flow_pattern_exchange_mark_resetting(&adapter->exchange);
        }
        flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_FAILED, rc, 0u);
        tf_timer_signal(&adapter->connect_wait);
      }
    }

    adapter->client = NULL;
    if (socket) {
      coro_socket_destroy(socket);
      socket = NULL;
    }
    flow_fmq_reader_destroy(&reader);
    if (!atomic_load_explicit(&adapter->started, memory_order_acquire) || rc == TURBO_OK) {
      break;
    }

    if (adapter->reconnect_initial_ms == 0) {
      break;
    }
    flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_BACKOFF, TURBO_EALREADY, 0u);
    tf_timer_signal(&adapter->connect_wait);
    reconnect_backoff_ms = flow_fmq_next_reconnect_delay_ms(adapter, reconnect_backoff_ms);
    wait_ms = flow_fmq_jitter_reconnect_delay_ms(adapter, reconnect_backoff_ms);
    flow_fmq_emit_event(adapter, TURBO_FLOW_FMQ_EVENT_RECONNECT_SCHEDULED, rc, NULL, (tstr_v){0},
                        (tstr_v){0}, wait_ms);
    if (wait_ms > 0) {
      int wait_rc = flow_fmq_reconnect_sleep(adapter, wait_ms);
      if (wait_rc != TURBO_OK) {
        rc = wait_rc;
        break;
      }
    }
  }
  flow_fmq_emit_reconnect_failed_once(adapter, rc);
  if (rc != TURBO_ESHUTDOWN) {
    flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_FAILED, rc, 0u);
  }
  tf_timer_signal(&adapter->connect_wait);
  flow_fmq_lane_task_end(adapter);
}

static int flow_fmq_start_bind(flow_fmq_adapter_t *adapter) {
  int rc;
  if (adapter->server) return TURBO_OK;
  adapter->server =
      tf_coronet_create_server_socket(adapter->ctx, flow_fmq_coronet_transport(adapter->transport));
  if (!adapter->server) return TURBO_ENOMEM;
  rc = tf_coronet_apply_kcp_fec(adapter->server, flow_fmq_coronet_transport(adapter->transport),
                                &adapter->kcp_fec, adapter->kcp_fec_configured);
  if (rc != TURBO_OK) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
    return rc;
  }
  if (adapter->reuse_port) coro_socket_set_reuse_port(adapter->server, 1);
  rc = tf_coronet_apply_socket_options(
      adapter->server, flow_fmq_coronet_transport(adapter->transport), &adapter->socket_options);
  if (rc != TURBO_OK) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
    return rc;
  }
  (void)tf_coronet_apply_socket_timeout(adapter->server, &adapter->timeouts,
                                        TF_CORONET_TIMEOUT_RECV);
  rc = tf_coronet_listen_socket(adapter->server, flow_fmq_coronet_transport(adapter->transport),
                                adapter->host, adapter->port, adapter->path,
                                flow_fmq_server_handler, adapter);
  if (rc != TURBO_OK) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
    return rc;
  }
  rc = tf_coronet_apply_udp_options(adapter->server, flow_fmq_coronet_transport(adapter->transport),
                                    &adapter->udp_options);
  if (rc != TURBO_OK) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
    return rc;
  }
  rc = tf_coronet_join_multicast(adapter->server, flow_fmq_coronet_transport(adapter->transport),
                                 &adapter->udp_options);
  if (rc != TURBO_OK) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
  }
  return rc;
}

static int flow_fmq_schedule_connect(flow_fmq_adapter_t *adapter) {
  int rc;
  if (adapter->client ||
      atomic_load_explicit(&adapter->connect_status, memory_order_acquire) == TURBO_EALREADY) {
    return TURBO_OK;
  }
  flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_EALREADY, 0u);
  flow_fmq_lane_task_begin(adapter);
  rc = coro_context_spawn(adapter->ctx, flow_fmq_connect_task, adapter);
  if (rc != TURBO_OK) {
    flow_fmq_lane_task_end(adapter);
    flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_FAILED, rc, 0u);
    tf_timer_signal(&adapter->connect_wait);
    return rc;
  }
  return TURBO_OK;
}

static int flow_fmq_start_bind_call(void *arg) {
  return flow_fmq_start_bind((flow_fmq_adapter_t *)arg);
}

static int flow_fmq_schedule_connect_call(void *arg) {
  return flow_fmq_schedule_connect((flow_fmq_adapter_t *)arg);
}

static int flow_fmq_close_resources_call(void *arg) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)arg;
  if (!adapter) return TURBO_EINVAL;
  if (adapter->server) {
    (void)tf_coronet_leave_multicast(
        adapter->server, flow_fmq_coronet_transport(adapter->transport), &adapter->udp_options);
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
  }
  if (adapter->client) (void)coro_socket_interrupt_wait(adapter->client, TURBO_ESHUTDOWN);
  flow_fmq_interrupt_peers(adapter);
  return TURBO_OK;
}

static int flow_fmq_clear_peers_call(void *arg) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)arg;
  if (!adapter) return TURBO_EINVAL;
  turbo_vec_clear(&adapter->peers);
  return TURBO_OK;
}

static int flow_fmq_reactor_command(void *ctx, uint64_t command_id, const void *bytes,
                                    size_t size) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  const flow_fmq_reactor_command_t *command = (const flow_fmq_reactor_command_t *)bytes;
  (void)command_id;
  if (!adapter || !command || size != sizeof(*command) || command->reserved != 0u) {
    return TURBO_EINVAL;
  }
  switch ((flow_fmq_reactor_command_kind_t)command->kind) {
  case FLOW_FMQ_REACTOR_START_BIND:
    return flow_fmq_start_bind_call(adapter);
  case FLOW_FMQ_REACTOR_SCHEDULE_CONNECT:
    return flow_fmq_schedule_connect_call(adapter);
  case FLOW_FMQ_REACTOR_CLOSE_RESOURCES:
    return flow_fmq_close_resources_call(adapter);
  case FLOW_FMQ_REACTOR_CLEAR_PEERS:
    return flow_fmq_clear_peers_call(adapter);
  default:
    return TURBO_EINVAL;
  }
}

static int flow_fmq_reactor_call(flow_fmq_adapter_t *adapter, flow_fmq_reactor_command_kind_t kind,
                                 uint64_t timeout_ns) {
  const flow_fmq_reactor_command_t command = {(uint32_t)kind, 0u};
  if (!adapter || !adapter->reactor_actor_initialized) return TURBO_EINVAL;
  return tf_coronet_actor_call(&adapter->reactor_actor, &command, sizeof(command), timeout_ns);
}

static int flow_fmq_wait_connect(flow_fmq_adapter_t *adapter) {
  uint64_t deadline_ns;
  uint64_t timeout_ns = flow_fmq_connect_timeout_ns(adapter);
  if (!adapter) return TURBO_EINVAL;
  if (timeout_ns == 0) return TURBO_EINVAL;
  deadline_ns = turbo_hrtime();
  if (deadline_ns > UINT64_MAX - timeout_ns) {
    deadline_ns = UINT64_MAX;
  } else {
    deadline_ns += timeout_ns;
  }
  for (;;) {
    int status = atomic_load_explicit(&adapter->connect_status, memory_order_acquire);
    if (status != TURBO_EALREADY) return status;
    if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
    if (turbo_hrtime() >= deadline_ns) {
      flow_fmq_emit_reconnect_failed_once(adapter, TURBO_ETIMEDOUT);
      return TURBO_ETIMEDOUT;
    }
    int wait_rc = tf_timer_wait_until_ns(&adapter->connect_wait, deadline_ns);
    if (wait_rc != TURBO_OK) {
      flow_fmq_emit_reconnect_failed_once(adapter, wait_rc);
      return wait_rc;
    }
  }
}

static int flow_fmq_start_resources(flow_fmq_adapter_t *adapter) {
  int rc;
  uint64_t timeout_ns;
  if (!adapter || !adapter->flow || adapter->start_refs <= 0) return TURBO_EINVAL;
  if (atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_OK;
  rc = tf_io_budget_open(&adapter->send_budget);
  if (rc != TURBO_OK) return rc;
  if (atomic_fetch_add_explicit(&adapter->generation, 1u, memory_order_acq_rel) == UINT64_MAX) {
    atomic_store_explicit(&adapter->generation, 1u, memory_order_release);
  }
  atomic_store_explicit(&adapter->reconnect_failure_reported, 0, memory_order_release);
  if (adapter->retry_wait_initialized) tf_timer_reset(&adapter->retry_wait);
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_ENOTCONN, 0u);
  rc = tf_coronet_execution_start(&adapter->execution);
  timeout_ns = flow_fmq_connect_timeout_ns(adapter);
  if (rc == TURBO_OK && adapter->mode == TURBO_FLOW_FMQ_BIND) {
    rc = flow_fmq_reactor_call(adapter, FLOW_FMQ_REACTOR_START_BIND, timeout_ns);
  } else if (rc == TURBO_OK) {
    rc = flow_fmq_reactor_call(adapter, FLOW_FMQ_REACTOR_SCHEDULE_CONNECT, timeout_ns);
  }
  if (rc == TURBO_OK && adapter->mode == TURBO_FLOW_FMQ_BIND) {
    flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_READY, TURBO_OK, 0u);
  }
  if (rc == TURBO_OK && adapter->mode == TURBO_FLOW_FMQ_CONNECT) {
    rc = flow_fmq_wait_connect(adapter);
  }
  if (rc != TURBO_OK) {
    atomic_store_explicit(&adapter->started, 0, memory_order_release);
    tf_io_budget_close(&adapter->send_budget);
    flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_FAILED, rc, 0u);
    (void)flow_fmq_reactor_call(adapter, FLOW_FMQ_REACTOR_CLOSE_RESOURCES, timeout_ns);
    flow_fmq_wait_lane_tasks(adapter);
    (void)flow_fmq_reactor_call(adapter, FLOW_FMQ_REACTOR_CLEAR_PEERS, timeout_ns);
    tf_coronet_execution_stop(&adapter->execution);
  }
  return rc;
}

static int flow_fmq_start(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  int rc;
  if (!adapter || !flow || !stage) return TURBO_EINVAL;
  if (stage->is_source) {
    tstr_t source_name = tstr_dup(stage->name);
    if (!source_name) return TURBO_ENOMEM;
    if (adapter->source_name && tstr_cmp(adapter->source_name, source_name) != 0) {
      tstr_free(source_name);
      return TURBO_EALREADY;
    }
    tstr_freep(&adapter->source_name);
    adapter->source_name = source_name;
  }
  adapter->start_refs += 1;
  if (adapter->start_refs > 1) return TURBO_OK;
  adapter->flow = flow;
  atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
  rc = flow_fmq_start_resources(adapter);
  if (rc != TURBO_OK) {
    adapter->start_refs -= 1;
    if (adapter->start_refs == 0) adapter->flow = NULL;
  }
  return rc;
}

static int flow_fmq_connection_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  tf_io_budget_snapshot_t budget;
  const char *scheme;
  int status;
  if (!adapter || !out) return TURBO_EINVAL;
  out->state = (turbo_flow_connection_state_t)atomic_load_explicit(&adapter->connection_state,
                                                                   memory_order_acquire);
  status = atomic_load_explicit(&adapter->connect_status, memory_order_relaxed);
  out->connections_current =
      atomic_load_explicit(&adapter->connections_current, memory_order_relaxed);
  out->connection_limit = adapter->max_connections;
  if (tf_io_budget_snapshot(&adapter->send_budget, &budget) != TURBO_OK) return TURBO_EINVAL;
  out->in_flight_messages = budget.messages;
  out->in_flight_bytes = budget.bytes;
  out->last_status = status;
  if (adapter->transport == TURBO_FLOW_FMQ_PIPE) {
    if (strncmp(adapter->path, "pipe://", sizeof("pipe://") - 1u) == 0) {
      (void)snprintf(out->endpoint, sizeof(out->endpoint), "%s", adapter->path);
    } else {
      (void)snprintf(out->endpoint, sizeof(out->endpoint), "pipe://%s", adapter->path);
    }
  } else {
    switch (adapter->transport) {
    case TURBO_FLOW_FMQ_TCP:
      scheme = "tcp";
      break;
    case TURBO_FLOW_FMQ_TLS:
      scheme = "tls";
      break;
    case TURBO_FLOW_FMQ_UDP:
      scheme = "udp";
      break;
    case TURBO_FLOW_FMQ_KCP:
      scheme = "kcp";
      break;
    case TURBO_FLOW_FMQ_WS:
      scheme = "ws";
      break;
    case TURBO_FLOW_FMQ_WSS:
      scheme = "wss";
      break;
    default:
      return TURBO_EINVAL;
    }
    (void)snprintf(out->endpoint, sizeof(out->endpoint), "%s://%s:%d", scheme, adapter->host,
                   adapter->port);
  }
  return TURBO_OK;
}

static int flow_fmq_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  flow_fmq_resource_context_t *resource = (flow_fmq_resource_context_t *)ctx;
  flow_fmq_adapter_t *adapter;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  const char *uid;
  int written;
  if (!resource || !resource->adapter || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  adapter = resource->adapter;
  metadata.kind = resource->kind;
  switch (resource->kind) {
  case TURBO_FLOW_RESOURCE_CONNECTION:
    metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
    uid = adapter->connection_uid;
    break;
  case TURBO_FLOW_RESOURCE_QUEUE_BUFFER:
    metadata.domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
    uid = adapter->queue_uid;
    break;
  case TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE:
    metadata.domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
    uid = adapter->protocol_uid;
    break;
  default:
    return TURBO_EINVAL;
  }
  metadata.generation = atomic_load_explicit(&adapter->generation, memory_order_acquire);
  if (metadata.generation == 0u) metadata.generation = 1u;
  metadata.observed_generation = metadata.generation;
  written = snprintf(metadata.uid, sizeof(metadata.uid), "%s", uid);
  if (written < 0 || (size_t)written >= sizeof(metadata.uid)) return TURBO_ENAMETOOLONG;
  written =
      snprintf(metadata.owner_name, sizeof(metadata.owner_name), "%s", adapter->resource_owner);
  if (written < 0 || (size_t)written >= sizeof(metadata.owner_name)) return TURBO_ENAMETOOLONG;
  *out = metadata;
  return TURBO_OK;
}

static int flow_fmq_resource_snapshot(void *ctx, turbo_flow_resource_snapshot_t *out) {
  flow_fmq_resource_context_t *resource = (flow_fmq_resource_context_t *)ctx;
  flow_fmq_adapter_t *adapter;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  tf_io_budget_snapshot_t budget;
  int rc;
  if (!resource || !resource->adapter || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  adapter = resource->adapter;
  rc = flow_fmq_resource_metadata(resource, &metadata);
  if (rc != TURBO_OK) return rc;
  if (tf_io_budget_snapshot(&adapter->send_budget, &budget) != TURBO_OK) return TURBO_EINVAL;
  *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  out->domain = metadata.domain;
  out->kind = metadata.kind;
  memcpy(out->uid, metadata.uid, sizeof(out->uid));
  memcpy(out->owner_name, metadata.owner_name, sizeof(out->owner_name));
  out->generation = metadata.generation;
  out->observed_generation = metadata.observed_generation;
  if (resource->kind == TURBO_FLOW_RESOURCE_QUEUE_BUFFER) {
    if (adapter->frame_hwm_messages != 0) {
      out->load = budget.messages;
      out->capacity = adapter->frame_hwm_messages;
    } else {
      out->load = budget.bytes;
      out->capacity = adapter->frame_hwm_bytes;
    }
  } else {
    out->load = atomic_load_explicit(&adapter->connections_current, memory_order_relaxed);
    out->capacity = adapter->max_connections;
  }
  out->saturated = out->capacity != 0 && out->load >= out->capacity;
  out->last_status = atomic_load_explicit(&adapter->connect_status, memory_order_relaxed);
  return TURBO_OK;
}

static int flow_fmq_resource_document(void *ctx, turbo_flow_resource_document_kind_t document_kind,
                                      turbo_flow_resource_document_t *out) {
  flow_fmq_resource_context_t *resource = (flow_fmq_resource_context_t *)ctx;
  flow_fmq_adapter_t *adapter;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_connection_snapshot_t connection;
  tf_io_budget_snapshot_t budget;
  const turbo_flow_resource_schema_t *schema;
  tstr_t payload = NULL;
  int rc;
  if (!resource || !resource->adapter || !out || out->size < sizeof(*out) || out->payload) {
    return TURBO_EINVAL;
  }
  if (document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return TURBO_ENOTSUP;
  adapter = resource->adapter;
  rc = flow_fmq_resource_metadata(resource, &metadata);
  if (rc != TURBO_OK) return rc;
  if (resource->kind == TURBO_FLOW_RESOURCE_CONNECTION) {
    memset(&connection, 0, sizeof(connection));
    rc = flow_fmq_connection_snapshot(adapter, &connection);
    if (rc != TURBO_OK) return rc;
    schema = &FLOW_FMQ_CONNECTION_SCHEMA;
    payload = tstr_format("{\"state\":{},\"last_status\":{},\"connections_current\":\"{}\","
                          "\"connection_limit\":\"{}\",\"in_flight_messages\":\"{}\","
                          "\"in_flight_bytes\":\"{}\"}",
                          (unsigned)connection.state, connection.last_status,
                          connection.connections_current, connection.connection_limit,
                          connection.in_flight_messages, connection.in_flight_bytes);
  } else if (resource->kind == TURBO_FLOW_RESOURCE_QUEUE_BUFFER) {
    uint64_t load;
    uint64_t capacity;
    if (tf_io_budget_snapshot(&adapter->send_budget, &budget) != TURBO_OK) return TURBO_EINVAL;
    load = adapter->frame_hwm_messages ? budget.messages : budget.bytes;
    capacity = adapter->frame_hwm_messages ? adapter->frame_hwm_messages : adapter->frame_hwm_bytes;
    schema = &FLOW_FMQ_QUEUE_SCHEMA;
    payload = tstr_format("{\"load\":\"{}\",\"capacity\":\"{}\",\"messages\":\"{}\","
                          "\"bytes\":\"{}\",\"saturated\":{},\"accepting\":{}}",
                          load, capacity, budget.messages, budget.bytes,
                          capacity != 0u && load >= capacity,
                          atomic_load_explicit(&adapter->started, memory_order_acquire) &&
                              !atomic_load_explicit(&adapter->quiesced, memory_order_acquire));
  } else if (resource->kind == TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE) {
    flow_fmq_pattern_state_t pattern_state = flow_fmq_exchange_state(&adapter->exchange);
    schema = &FLOW_FMQ_PROTOCOL_SCHEMA;
    payload = tstr_format(
        "{\"pattern\":{},\"mode\":{},\"transport\":{},\"pattern_state\":{},"
        "\"sessions\":\"{}\",\"started\":{},\"quiesced\":{}}",
        (unsigned)adapter->pattern, (unsigned)adapter->mode, (unsigned)adapter->transport,
        (unsigned)pattern_state,
        atomic_load_explicit(&adapter->connections_current, memory_order_relaxed),
        atomic_load_explicit(&adapter->started, memory_order_acquire),
        atomic_load_explicit(&adapter->quiesced, memory_order_acquire));
  } else {
    return TURBO_EINVAL;
  }
  if (!payload) return TURBO_ENOMEM;
  rc = turbo_flow_resource_document_set_payload_copy(out, &metadata, schema, payload,
                                                     tstr_len(payload));
  tstr_free(payload);
  return rc;
}

static int flow_fmq_topic_matches(const flow_fmq_adapter_t *adapter, const flow_fmq_peer_t *peer,
                                  tstr_v topic) {
  if (!adapter || !peer || peer->closing) return 0;
  if (adapter->pattern == TURBO_FLOW_FMQ_XPUB) {
    for (size_t i = 0; i < turbo_vec_size(&peer->subscriptions); ++i) {
      const flow_fmq_subscription_t *subscription =
          (const flow_fmq_subscription_t *)turbo_vec_at_const(&peer->subscriptions, i);
      size_t prefix_len = subscription && subscription->topic ? tstr_len(subscription->topic) : 0u;
      if (prefix_len == 0u ||
          (topic.len >= prefix_len && memcmp(topic.data, subscription->topic, prefix_len) == 0)) {
        return 1;
      }
    }
    return 0;
  }
  {
    size_t prefix_len = peer->topic ? tstr_len(peer->topic) : 0u;
    return prefix_len == 0u ||
           (topic.len >= prefix_len && memcmp(topic.data, peer->topic, prefix_len) == 0);
  }
}

static void flow_fmq_release_send_budget(flow_fmq_adapter_t *adapter, size_t frame_bytes) {
  if (!adapter) return;
  (void)tf_io_budget_release(&adapter->send_budget, frame_bytes);
}

static int flow_fmq_reserve_send_budget(flow_fmq_adapter_t *adapter, size_t frame_bytes) {
  int rc;
  if (!adapter) return TURBO_EINVAL;
  if (adapter->frame_admission_policy == TURBO_FLOW_FMQ_FRAME_ADMISSION_DROP_OLDEST) {
    return flow_fmq_reserve_drop_oldest(adapter, frame_bytes);
  }
  rc = tf_io_budget_acquire(&adapter->send_budget, frame_bytes);
  if (rc != TURBO_OK) {
    if (rc == TURBO_ENOSPC || rc == TURBO_ETIMEDOUT) {
      flow_fmq_emit_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_HWM_REACHED, rc, frame_bytes);
    }
    return rc;
  }
  return TURBO_OK;
}

static void flow_fmq_wait_send_drain(flow_fmq_adapter_t *adapter) {
  if (!adapter || adapter->frame_linger_ms == 0) return;
  (void)tf_io_budget_drain(&adapter->send_budget, adapter->frame_linger_ms);
}

static int flow_fmq_request_begin_socket_send(flow_fmq_send_request_t *request) {
  int rc = TURBO_OK;
  turbo_mutex_lock(&request->mutex);
  if (request->cancel_requested) {
    rc = atomic_load_explicit(&request->adapter->started, memory_order_acquire) ? TURBO_ECANCELED
                                                                                : TURBO_ESHUTDOWN;
  } else if (request->delivery_stage == FLOW_FMQ_DELIVERY_NOT_SUBMITTED) {
    request->delivery_stage = FLOW_FMQ_DELIVERY_WRITE_STARTED;
  }
  turbo_mutex_unlock(&request->mutex);
  return rc;
}

static void flow_fmq_request_mark_delivery(flow_fmq_send_request_t *request,
                                           flow_fmq_delivery_stage_t stage) {
  turbo_mutex_lock(&request->mutex);
  if (request->delivery_stage < stage) request->delivery_stage = stage;
  turbo_mutex_unlock(&request->mutex);
}

static int flow_fmq_request_socket_send(flow_fmq_send_request_t *request, coro_socket_t *socket) {
  int rc = flow_fmq_request_begin_socket_send(request);
  if (rc != TURBO_OK) return rc;
  return flow_fmq_socket_send(request->adapter, socket, request->frame, tstr_len(request->frame));
}

static int flow_fmq_record_xsub_control(flow_fmq_adapter_t *adapter, const char *encoded,
                                        size_t encoded_len) {
  flow_fmq_frame_t frame;
  size_t consumed = 0u;
  int changed;
  int rc;
  if (!adapter || adapter->pattern != TURBO_FLOW_FMQ_XSUB || !adapter->subscriptions_initialized) {
    return TURBO_EINVAL;
  }
  rc = flow_fmq_decode_frame(encoded, encoded_len, adapter->max_frame_size, &frame, &consumed);
  if (rc != TURBO_OK) return rc;
  if (consumed != encoded_len ||
      (frame.kind != FLOW_FMQ_FRAME_SUBSCRIBE && frame.kind != FLOW_FMQ_FRAME_UNSUBSCRIBE)) {
    flow_fmq_frame_cleanup(&frame);
    return TURBO_EPROTO;
  }
  rc = flow_fmq_subscription_update(&adapter->subscriptions, frame.kind == FLOW_FMQ_FRAME_SUBSCRIBE,
                                    frame.topic, &changed);
  flow_fmq_frame_cleanup(&frame);
  return rc;
}

static void flow_fmq_complete_fanout_peer(flow_fmq_peer_t *peer, flow_fmq_send_request_t *request,
                                          int status, turbo_flow_fmq_event_kind_t event_kind) {
  int final = 0;
  if (!peer || !request) return;
  if (peer->send_queue_initialized) {
    (void)tf_io_budget_release(&peer->send_budget, request->budget_bytes);
  }
  turbo_mutex_lock(&request->mutex);
  if (status == TURBO_OK) {
    request->fanout_delivered += 1u;
    if (request->delivery_stage < FLOW_FMQ_DELIVERY_PARTIAL) {
      request->delivery_stage = FLOW_FMQ_DELIVERY_PARTIAL;
    }
  } else {
    request->fanout_dropped += 1u;
    if (request->fanout_first_error == TURBO_OK) request->fanout_first_error = status;
  }
  if (request->fanout_pending > 0u) request->fanout_pending -= 1u;
  if (request->fanout_pending == 0u && request->fanout_active) {
    if (request->fanout_dropped == 0u) {
      request->delivery_stage = FLOW_FMQ_DELIVERY_DELIVERED;
    }
    final = 1;
  }
  turbo_mutex_unlock(&request->mutex);
  flow_fmq_emit_peer_frame_event(request->adapter, event_kind, status, peer, request->budget_bytes);
  if (final) {
    if (request->budget_reserved) {
      flow_fmq_release_send_budget(request->adapter, request->budget_bytes);
      request->budget_reserved = 0;
    }
    flow_fmq_request_release(request);
  }
}

static void flow_fmq_drop_peer_send_queue(flow_fmq_peer_t *peer, int status,
                                          turbo_flow_fmq_event_kind_t event_kind) {
  flow_fmq_send_request_t *request = NULL;
  if (!peer || !peer->send_queue_initialized) return;
  (void)tf_io_budget_close(&peer->send_budget);
  while (flow_fmq_peer_send_requests_pop_front(&peer->send_queue, &request)) {
    flow_fmq_complete_fanout_peer(peer, request, status, event_kind);
  }
}

static void flow_fmq_fail_peer_send_queue(flow_fmq_peer_t *peer, int status) {
  flow_fmq_drop_peer_send_queue(peer, status, TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED);
}

static void flow_fmq_peer_send_drain_task(coro_t *co, void *arg) {
  flow_fmq_peer_t *peer = (flow_fmq_peer_t *)arg;
  flow_fmq_adapter_t *adapter;
  (void)co;
  if (!peer || !(adapter = peer->adapter)) return;
  for (;;) {
    flow_fmq_send_request_t *request = NULL;
    int rc;
    if (peer->closing || !flow_fmq_peer_send_requests_pop_front(&peer->send_queue, &request)) {
      if (peer->closing) flow_fmq_fail_peer_send_queue(peer, TURBO_ENOTCONN);
      peer->send_drain_active = 0;
      flow_fmq_lane_task_end(adapter);
      return;
    }
    rc = flow_fmq_request_socket_send(request, peer->socket);
    flow_fmq_complete_fanout_peer(peer, request, rc,
                                  rc == TURBO_OK ? TURBO_FLOW_FMQ_EVENT_FRAME_SENT
                                                 : TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED);
    if (rc != TURBO_OK) {
      peer->closing = 1;
      (void)coro_socket_interrupt_wait(peer->socket, rc);
      flow_fmq_fail_peer_send_queue(peer, rc);
    }
  }
}

static int flow_fmq_schedule_peer_send_drain(flow_fmq_peer_t *peer) {
  flow_fmq_adapter_t *adapter;
  int rc;
  if (!peer || !(adapter = peer->adapter) || !peer->send_queue_initialized) {
    return TURBO_EINVAL;
  }
  if (peer->send_drain_active) return TURBO_OK;
  peer->send_drain_active = 1;
  flow_fmq_lane_task_begin(adapter);
  rc = coro_context_spawn(adapter->ctx, flow_fmq_peer_send_drain_task, peer);
  if (rc == TURBO_OK) return TURBO_OK;
  peer->send_drain_active = 0;
  flow_fmq_lane_task_end(adapter);
  return rc;
}

static int flow_fmq_reserve_peer_queue_slot(flow_fmq_peer_t *peer) {
  size_t size;
  if (!peer || !peer->send_queue_initialized) return TURBO_EINVAL;
  size = flow_fmq_peer_send_requests_size(&peer->send_queue);
  if (size == SIZE_MAX) return TURBO_ERANGE;
  return flow_fmq_peer_send_requests_reserve(&peer->send_queue, size + 1u);
}

static int flow_fmq_acquire_peer_drop_oldest(flow_fmq_peer_t *peer,
                                             flow_fmq_send_request_t *incoming) {
  flow_fmq_send_request_t *dropped = NULL;
  int rc;
  if (!peer || !incoming || !peer->send_queue_initialized) return TURBO_EINVAL;
  if (peer->adapter->fanout.peer_hwm_bytes != 0u &&
      incoming->budget_bytes > peer->adapter->fanout.peer_hwm_bytes) {
    flow_fmq_emit_peer_frame_event(peer->adapter, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_HWM, TURBO_ENOSPC,
                                   peer, incoming->budget_bytes);
    return TURBO_ENOSPC;
  }
  rc = tf_io_budget_acquire(&peer->send_budget, incoming->budget_bytes);
  if (rc != TURBO_ENOSPC) return rc;
  flow_fmq_emit_peer_frame_event(peer->adapter, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_HWM, rc, peer,
                                 incoming->budget_bytes);
  while (rc == TURBO_ENOSPC && flow_fmq_peer_send_requests_pop_front(&peer->send_queue, &dropped)) {
    flow_fmq_complete_fanout_peer(peer, dropped, TURBO_ECANCELED,
                                  TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DROPPED);
    rc = tf_io_budget_acquire(&peer->send_budget, incoming->budget_bytes);
  }
  return rc;
}

static void flow_fmq_release_peer_reservations(flow_fmq_adapter_t *adapter, tstr_v topic,
                                               size_t end_index, size_t frame_bytes) {
  size_t count;
  if (!adapter) return;
  count = turbo_vec_size(&adapter->peers);
  if (end_index > count) end_index = count;
  for (size_t i = 0; i < end_index; ++i) {
    flow_fmq_peer_t *const *slot = (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, i);
    if (!slot || !*slot || !flow_fmq_topic_matches(adapter, *slot, topic)) continue;
    (void)tf_io_budget_release(&(*slot)->send_budget, frame_bytes);
  }
}

static void flow_fmq_rollback_fanout_enqueue(flow_fmq_adapter_t *adapter,
                                             flow_fmq_send_request_t *request, tstr_v topic) {
  if (!adapter || !request) return;
  for (size_t i = 0; i < turbo_vec_size(&adapter->peers); ++i) {
    flow_fmq_peer_t *const *slot = (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, i);
    flow_fmq_send_request_t **back;
    flow_fmq_send_request_t *removed = NULL;
    if (!slot || !*slot || !flow_fmq_topic_matches(adapter, *slot, topic)) continue;
    back = flow_fmq_peer_send_requests_back(&(*slot)->send_queue);
    if (back && *back == request) {
      (void)flow_fmq_peer_send_requests_pop_back(&(*slot)->send_queue, &removed);
    }
    (void)tf_io_budget_release(&(*slot)->send_budget, request->budget_bytes);
  }
  request->fanout_active = 0;
  request->fanout_pending = 0u;
  request->fanout_delivered = 0u;
  request->fanout_dropped = 0u;
  request->fanout_first_error = TURBO_OK;
  flow_fmq_request_release(request);
}

static void flow_fmq_signal_fanout_admission(flow_fmq_send_request_t *request) {
  if (!request) return;
  turbo_mutex_lock(&request->mutex);
  request->status = TURBO_OK;
  request->done = 1;
  turbo_cond_signal(&request->cond);
  turbo_mutex_unlock(&request->mutex);
}

static int flow_fmq_peer_selection_begin(
    flow_fmq_adapter_t *adapter, turbo_flow_pattern_selection_t selection,
    turbo_flow_pattern_selection_iterator_t *iterator) {
  if (!adapter || !iterator) return TURBO_EINVAL;
  *iterator = (turbo_flow_pattern_selection_iterator_t)
      TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
  return turbo_flow_pattern_selection_begin(&adapter->selector, selection,
                                            turbo_vec_size(&adapter->peers), iterator);
}

static int flow_fmq_enqueue_fanout(flow_fmq_send_request_t *request, tstr_v topic) {
  flow_fmq_adapter_t *adapter;
  turbo_flow_pattern_selection_iterator_t selection =
      TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
  size_t peer_index;
  size_t matched = 0u;
  size_t accepted = 0u;
  int rc;
  if (!request || !(adapter = request->adapter) || !adapter->fanout_enabled) {
    return TURBO_EINVAL;
  }
  rc = flow_fmq_peer_selection_begin(adapter, TURBO_FLOW_PATTERN_SELECT_FAN_OUT, &selection);
  if (rc == TURBO_ENOENT) return TURBO_ENOTCONN;
  if (rc != TURBO_OK) return rc;
  while ((rc = turbo_flow_pattern_selection_next(&selection, &peer_index)) == TURBO_OK) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    if (!slot || !*slot || !flow_fmq_topic_matches(adapter, *slot, topic)) continue;
    matched += 1u;
    rc = flow_fmq_reserve_peer_queue_slot(*slot);
    if (rc != TURBO_OK) return rc;
  }
  if (rc != TURBO_ENOENT) return rc;
  if (matched == 0u) return TURBO_ENOTCONN;

  rc = flow_fmq_peer_selection_begin(adapter, TURBO_FLOW_PATTERN_SELECT_FAN_OUT, &selection);
  if (rc != TURBO_OK) return rc;
  while ((rc = turbo_flow_pattern_selection_next(&selection, &peer_index)) == TURBO_OK) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    flow_fmq_peer_t *peer;
    if (!slot || !(peer = *slot) || !flow_fmq_topic_matches(adapter, peer, topic)) continue;
    rc = adapter->fanout.slow_peer_policy == TURBO_FLOW_FMQ_SLOW_PEER_DROP_OLDEST
             ? flow_fmq_acquire_peer_drop_oldest(peer, request)
             : tf_io_budget_acquire(&peer->send_budget, request->budget_bytes);
    if (rc == TURBO_OK) {
      accepted += 1u;
      continue;
    }
    if (adapter->fanout.slow_peer_policy == TURBO_FLOW_FMQ_SLOW_PEER_DISCONNECT &&
        rc == TURBO_ENOSPC) {
      flow_fmq_emit_peer_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_HWM, rc, peer,
                                     request->budget_bytes);
      flow_fmq_emit_peer_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DROPPED, rc, peer,
                                     request->budget_bytes);
      peer->closing = 1;
      flow_fmq_emit_peer_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DISCONNECTED, rc, peer,
                                     request->budget_bytes);
      (void)coro_socket_interrupt_wait(peer->socket, TURBO_ENOSPC);
      flow_fmq_drop_peer_send_queue(peer, TURBO_ENOSPC, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_DROPPED);
      continue;
    }
    if (rc == TURBO_ENOSPC) {
      flow_fmq_emit_peer_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_SLOW_PEER_HWM, rc, peer,
                                     request->budget_bytes);
    }
    flow_fmq_release_peer_reservations(adapter, topic, peer_index, request->budget_bytes);
    return rc;
  }
  if (rc != TURBO_ENOENT) return rc;
  if (accepted == 0u) return TURBO_ENOTCONN;

  request->fanout_pending = accepted;
  request->fanout_active = 1;
  request->fanout_first_error = TURBO_OK;
  atomic_fetch_add_explicit(&request->refs, 1, memory_order_relaxed);
  rc = flow_fmq_peer_selection_begin(adapter, TURBO_FLOW_PATTERN_SELECT_FAN_OUT, &selection);
  if (rc != TURBO_OK) {
    flow_fmq_rollback_fanout_enqueue(adapter, request, topic);
    return rc;
  }
  while ((rc = turbo_flow_pattern_selection_next(&selection, &peer_index)) == TURBO_OK) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    if (!slot || !*slot || !flow_fmq_topic_matches(adapter, *slot, topic)) continue;
    rc = flow_fmq_peer_send_requests_push_back(&(*slot)->send_queue, request);
    if (rc != TURBO_OK) {
      flow_fmq_rollback_fanout_enqueue(adapter, request, topic);
      return rc;
    }
  }
  if (rc != TURBO_ENOENT) {
    flow_fmq_rollback_fanout_enqueue(adapter, request, topic);
    return rc;
  }
  rc = flow_fmq_peer_selection_begin(adapter, TURBO_FLOW_PATTERN_SELECT_FAN_OUT, &selection);
  if (rc != TURBO_OK) {
    flow_fmq_rollback_fanout_enqueue(adapter, request, topic);
    return rc;
  }
  while ((rc = turbo_flow_pattern_selection_next(&selection, &peer_index)) == TURBO_OK) {
    flow_fmq_peer_t *const *slot =
        (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
    if (!slot || !*slot || !flow_fmq_topic_matches(adapter, *slot, topic)) continue;
    rc = flow_fmq_schedule_peer_send_drain(*slot);
    if (rc != TURBO_OK) {
      flow_fmq_rollback_fanout_enqueue(adapter, request, topic);
      return rc;
    }
  }
  if (rc != TURBO_ENOENT) {
    flow_fmq_rollback_fanout_enqueue(adapter, request, topic);
    return rc;
  }
  flow_fmq_signal_fanout_admission(request);
  return TURBO_OK;
}

static int flow_fmq_send_frame_now(flow_fmq_send_request_t *request) {
  flow_fmq_adapter_t *adapter = request->adapter;
  int rc;
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire) && !request->drain_allowed)
    return TURBO_ESHUTDOWN;
  if (request->route.session_id != 0u) {
    flow_fmq_peer_t *peer = flow_fmq_find_peer(adapter, request->route);
    if (!peer) return TURBO_ENOTCONN;
    rc = flow_fmq_request_socket_send(request, peer->socket);
    if (rc == TURBO_OK) flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
    return rc;
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_PUB || adapter->pattern == TURBO_FLOW_FMQ_XPUB) {
    int sent = 0;
    int first_error = TURBO_OK;
    tstr_v topic;
    turbo_flow_pattern_selection_iterator_t selection =
        TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
    size_t peer_index;
    if (flow_fmq_encoded_topic(request->frame, tstr_len(request->frame), adapter->max_frame_size,
                               &topic) != TURBO_OK) {
      return TURBO_EPROTO;
    }
    if (adapter->fanout_enabled) return flow_fmq_enqueue_fanout(request, topic);
    rc = turbo_flow_pattern_selection_begin(&adapter->selector,
                                            TURBO_FLOW_PATTERN_SELECT_FAN_OUT,
                                            turbo_vec_size(&adapter->peers), &selection);
    if (rc == TURBO_ENOENT) return TURBO_ENOTCONN;
    if (rc != TURBO_OK) return rc;
    while ((rc = turbo_flow_pattern_selection_next(&selection, &peer_index)) == TURBO_OK) {
      flow_fmq_peer_t *const *peer =
          (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, peer_index);
      int peer_rc;
      if (!peer || !*peer || !flow_fmq_topic_matches(adapter, *peer, topic)) continue;
      peer_rc = flow_fmq_request_socket_send(request, (*peer)->socket);
      if (peer_rc == TURBO_OK) {
        sent += 1;
        flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_PARTIAL);
      } else if (first_error == TURBO_OK) {
        first_error = peer_rc;
      }
      if (peer_rc == TURBO_ECANCELED) break;
    }
    if (rc != TURBO_ENOENT && rc != TURBO_OK) return rc;
    if (first_error != TURBO_OK) return first_error;
    if (sent == 0) return TURBO_ENOTCONN;
    flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
    return TURBO_OK;
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_PUSH) {
    size_t count = turbo_vec_size(&adapter->peers);
    size_t index;
    flow_fmq_peer_t *const *peer;
    turbo_flow_pattern_selection_iterator_t selection =
        TURBO_FLOW_PATTERN_SELECTION_ITERATOR_INIT;
    if (count == 0) return TURBO_ENOTCONN;
    rc = turbo_flow_pattern_selection_begin(&adapter->selector,
                                            TURBO_FLOW_PATTERN_SELECT_ROUND_ROBIN, count,
                                            &selection);
    if (rc != TURBO_OK) return rc;
    rc = turbo_flow_pattern_selection_next(&selection, &index);
    if (rc != TURBO_OK) return rc;
    peer = (flow_fmq_peer_t *const *)turbo_vec_at_const(&adapter->peers, index);
    if (!peer || !*peer) return TURBO_ENOTCONN;
    rc = flow_fmq_request_socket_send(request, (*peer)->socket);
    if (rc == TURBO_OK) flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
    return rc;
  }
  if (!adapter->client) return TURBO_ENOTCONN;
  if (adapter->pattern == TURBO_FLOW_FMQ_XSUB) {
    rc = flow_fmq_request_begin_socket_send(request);
    if (rc != TURBO_OK) return rc;
    rc = flow_fmq_record_xsub_control(adapter, request->frame, tstr_len(request->frame));
    if (rc != TURBO_OK) return rc;
    rc = flow_fmq_socket_send(adapter, adapter->client, request->frame, tstr_len(request->frame));
    if (rc == TURBO_OK) flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
    return rc;
  }
  rc = flow_fmq_request_socket_send(request, adapter->client);
  if (rc == TURBO_OK) flow_fmq_request_mark_delivery(request, FLOW_FMQ_DELIVERY_DELIVERED);
  return rc;
}

static void flow_fmq_request_release(flow_fmq_send_request_t *request) {
  if (atomic_fetch_sub_explicit(&request->refs, 1, memory_order_acq_rel) != 1) return;
  tstr_freep(&request->frame);
  if (request->completion_wait) {
    (void)coro_wait_destroy(request->completion_wait);
    request->completion_wait = NULL;
  }
  turbo_cond_destroy(&request->cond);
  turbo_mutex_destroy(&request->mutex);
  free(request);
}

static void flow_fmq_retry_send_wait_interrupt(void *arg1, void *arg2) {
  flow_fmq_send_request_t *request = (flow_fmq_send_request_t *)arg1;
  coro_wait_t *wait = NULL;
  (void)arg2;
  if (!request) return;
  turbo_mutex_lock(&request->mutex);
  if (request->completion_wait_armed) wait = request->completion_wait;
  turbo_mutex_unlock(&request->mutex);
  if (wait) (void)coro_wait_interrupt(wait, TURBO_EINTR);
  flow_fmq_request_release(request);
}

static void flow_fmq_complete_send_request(flow_fmq_send_request_t *request, int status) {
  coro_wait_t *wait = NULL;
  int wait_rc;
  if (!request) return;
  turbo_mutex_lock(&request->mutex);
  request->status = status;
  request->done = 1;
  if (request->completion_wait_armed) wait = request->completion_wait;
  turbo_cond_signal(&request->cond);
  turbo_mutex_unlock(&request->mutex);
  if (wait) {
    wait_rc = coro_wait_interrupt(wait, TURBO_EINTR);
    if (wait_rc == TURBO_EALREADY) {
      /* A foreign thread can complete between arming and coro_wait_for() becoming active.
       * Retry on the owner lane so that boundary cannot lose the completion wake. */
      atomic_fetch_add_explicit(&request->refs, 1, memory_order_relaxed);
      if (coro_post(request->adapter->ctx, flow_fmq_retry_send_wait_interrupt, request, NULL) !=
          TURBO_OK) {
        flow_fmq_request_release(request);
      }
    }
  }
  /* The send result is final before observers run; budget ownership remains until observers
   * return so HWM and linger snapshots still include this completion boundary. */
  flow_fmq_emit_frame_event(request->adapter,
                            status == TURBO_OK ? TURBO_FLOW_FMQ_EVENT_FRAME_SENT
                                               : TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED,
                            status, request->budget_bytes);
  if (request->budget_reserved) {
    flow_fmq_release_send_budget(request->adapter, request->budget_bytes);
    request->budget_reserved = 0;
  }
  flow_fmq_request_release(request);
}

static void flow_fmq_append_request(flow_fmq_send_request_t **head, flow_fmq_send_request_t **tail,
                                    flow_fmq_send_request_t *request) {
  request->drop_next = NULL;
  if (*tail) {
    (*tail)->drop_next = request;
  } else {
    *head = request;
  }
  *tail = request;
}

static void flow_fmq_complete_request_chain(flow_fmq_send_request_t *head, int status) {
  while (head) {
    flow_fmq_send_request_t *request = head;
    head = head->drop_next;
    request->drop_next = NULL;
    flow_fmq_complete_send_request(request, status);
  }
}

static void flow_fmq_fail_send_queue(flow_fmq_adapter_t *adapter, int status) {
  flow_fmq_send_request_t *head = NULL;
  flow_fmq_send_request_t *tail = NULL;
  flow_fmq_send_request_t *request = NULL;
  if (!adapter || !adapter->send_queue_initialized) return;
  turbo_mutex_lock(&adapter->send_queue_mutex);
  while (flow_fmq_send_requests_pop_front(&adapter->send_queue, &request)) {
    flow_fmq_append_request(&head, &tail, request);
  }
  adapter->send_drain_active = 0;
  turbo_mutex_unlock(&adapter->send_queue_mutex);
  flow_fmq_complete_request_chain(head, status);
}

static int flow_fmq_reserve_drop_oldest(flow_fmq_adapter_t *adapter, size_t frame_bytes) {
  flow_fmq_send_request_t *head = NULL;
  flow_fmq_send_request_t *tail = NULL;
  flow_fmq_send_request_t *dropped = NULL;
  int rc;
  int reached_hwm = 0;
  if (!adapter || !adapter->send_queue_initialized) return TURBO_EINVAL;
  if (adapter->frame_hwm_bytes != 0 && frame_bytes > adapter->frame_hwm_bytes) {
    flow_fmq_emit_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_HWM_REACHED, TURBO_ENOSPC, frame_bytes);
    return TURBO_ENOSPC;
  }
  turbo_mutex_lock(&adapter->send_queue_mutex);
  rc = tf_io_budget_acquire(&adapter->send_budget, frame_bytes);
  while (rc == TURBO_ENOSPC) {
    reached_hwm = 1;
    if (!flow_fmq_send_requests_pop_front(&adapter->send_queue, &dropped)) break;
    if (dropped->budget_reserved) {
      flow_fmq_release_send_budget(adapter, dropped->budget_bytes);
      dropped->budget_reserved = 0;
    }
    flow_fmq_append_request(&head, &tail, dropped);
    rc = tf_io_budget_acquire(&adapter->send_budget, frame_bytes);
  }
  turbo_mutex_unlock(&adapter->send_queue_mutex);
  if (reached_hwm) {
    flow_fmq_emit_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_HWM_REACHED, TURBO_ENOSPC, frame_bytes);
  }
  flow_fmq_complete_request_chain(head, TURBO_ECANCELED);
  return rc;
}

static void flow_fmq_send_drain_task(coro_t *co, void *arg) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)arg;
  (void)co;
  for (;;) {
    flow_fmq_send_request_t *request = NULL;
    turbo_mutex_lock(&adapter->send_queue_mutex);
    if (!flow_fmq_send_requests_pop_front(&adapter->send_queue, &request)) {
      adapter->send_drain_active = 0;
      turbo_mutex_unlock(&adapter->send_queue_mutex);
      flow_fmq_lane_task_end(adapter);
      return;
    }
    turbo_mutex_unlock(&adapter->send_queue_mutex);
    {
      int rc = flow_fmq_send_frame_now(request);
      if (request->fanout_active) {
        flow_fmq_request_release(request);
      } else {
        flow_fmq_complete_send_request(request, rc);
      }
    }
  }
}

static void flow_fmq_send_drain_post(void *arg1, void *arg2) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)arg1;
  int rc;
  (void)arg2;
  rc = coro_context_spawn(adapter->ctx, flow_fmq_send_drain_task, adapter);
  if (rc == TURBO_OK) return;
  flow_fmq_fail_send_queue(adapter, rc);
  flow_fmq_lane_task_end(adapter);
}

static int flow_fmq_enqueue_send(flow_fmq_adapter_t *adapter, flow_fmq_send_request_t *request) {
  int schedule_drain = 0;
  int rc;
  turbo_mutex_lock(&adapter->send_queue_mutex);
  /* Linger preserves requests that reserved budget before stop closed admission. */
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire) && !request->drain_allowed) {
    turbo_mutex_unlock(&adapter->send_queue_mutex);
    return TURBO_ESHUTDOWN;
  }
  rc = flow_fmq_send_requests_push_back(&adapter->send_queue, request);
  if (rc == TURBO_OK && !adapter->send_drain_active) {
    adapter->send_drain_active = 1;
    /* Lock order is send_queue_mutex -> lane_task_mutex; stop never takes the reverse order. */
    flow_fmq_lane_task_begin(adapter);
    schedule_drain = 1;
  }
  turbo_mutex_unlock(&adapter->send_queue_mutex);
  if (rc != TURBO_OK || !schedule_drain) return rc;
  rc = coro_post(adapter->ctx, flow_fmq_send_drain_post, adapter, NULL);
  if (rc == TURBO_OK) return TURBO_OK;
  flow_fmq_fail_send_queue(adapter, rc);
  flow_fmq_lane_task_end(adapter);
  return rc;
}

static uint64_t flow_fmq_send_deadline_ns(const flow_fmq_adapter_t *adapter) {
  uint64_t timeout_ns = flow_fmq_send_timeout_ns(adapter);
  uint64_t now = turbo_hrtime();
  if (timeout_ns > UINT64_MAX - now) return UINT64_MAX;
  return now + timeout_ns;
}

static int flow_fmq_wait_send_request(flow_fmq_adapter_t *adapter, flow_fmq_send_request_t *request,
                                      flow_fmq_delivery_stage_t *delivery_stage) {
  uint64_t deadline_ns = flow_fmq_send_deadline_ns(adapter);
  int rc;
  if (coro_context_current() == adapter->ctx) {
    uint64_t now = turbo_hrtime();
    uint64_t remaining_ns;
    uint64_t remaining_ms;
    int wait_rc;
    if (!request->completion_wait) return TURBO_ENOMEM;
    turbo_mutex_lock(&request->mutex);
    if (request->done) {
      rc = request->status;
      if (delivery_stage) *delivery_stage = request->delivery_stage;
      turbo_mutex_unlock(&request->mutex);
      return rc;
    }
    if (deadline_ns != UINT64_MAX && now >= deadline_ns) {
      request->cancel_requested = 1;
      if (delivery_stage) *delivery_stage = request->delivery_stage;
      rc = atomic_load_explicit(&adapter->started, memory_order_acquire) ? TURBO_ETIMEDOUT
                                                                         : TURBO_ESHUTDOWN;
      turbo_mutex_unlock(&request->mutex);
      return rc;
    }
    remaining_ns = deadline_ns - now;
    remaining_ms = remaining_ns > UINT64_MAX - UINT64_C(999999)
                       ? UINT64_MAX / UINT64_C(1000000)
                       : (remaining_ns + UINT64_C(999999)) / UINT64_C(1000000);
    if (remaining_ms == 0u) remaining_ms = 1u;
    request->completion_wait_armed = 1;
    turbo_mutex_unlock(&request->mutex);

    /* Completion interrupts this wait; the timer is only the send deadline. This keeps the
     * owner lane runnable without quantizing every reply through a 1 ms polling timer. */
    wait_rc = coro_wait_for(request->completion_wait, remaining_ms);

    turbo_mutex_lock(&request->mutex);
    request->completion_wait_armed = 0;
    if (!request->done) request->cancel_requested = 1;
    if (delivery_stage) *delivery_stage = request->delivery_stage;
    if (request->done) {
      rc = request->status;
    } else if (wait_rc != TURBO_OK && wait_rc != TURBO_EINTR) {
      rc = wait_rc;
    } else {
      rc = atomic_load_explicit(&adapter->started, memory_order_acquire) ? TURBO_ETIMEDOUT
                                                                         : TURBO_ESHUTDOWN;
    }
    turbo_mutex_unlock(&request->mutex);
    return rc;
  }
  turbo_mutex_lock(&request->mutex);
  while (!request->done) {
    uint64_t now = turbo_hrtime();
    if (deadline_ns != UINT64_MAX && now >= deadline_ns) break;
    if (deadline_ns == UINT64_MAX) {
      turbo_cond_wait(&request->cond, &request->mutex);
    } else if (turbo_cond_timedwait(&request->cond, &request->mutex, deadline_ns - now) != 0) {
      break;
    }
  }
  if (!request->done) request->cancel_requested = 1;
  if (delivery_stage) *delivery_stage = request->delivery_stage;
  rc = request->done
           ? request->status
           : (atomic_load_explicit(&adapter->started, memory_order_acquire) ? TURBO_ETIMEDOUT
                                                                            : TURBO_ESHUTDOWN);
  turbo_mutex_unlock(&request->mutex);
  return rc;
}

static int flow_fmq_submit_send(flow_fmq_adapter_t *adapter, flow_fmq_route_token_t route,
                                tstr_t frame, flow_fmq_send_request_t **out_request) {
  flow_fmq_send_request_t *request;
  int rc;
  if (!out_request) return TURBO_EINVAL;
  *out_request = NULL;
  request = (flow_fmq_send_request_t *)calloc(1, sizeof(*request));
  if (!request) {
    flow_fmq_emit_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED, TURBO_ENOMEM,
                              tstr_len(frame));
    flow_fmq_release_send_budget(adapter, tstr_len(frame));
    tstr_free(frame);
    return TURBO_ENOMEM;
  }
  request->adapter = adapter;
  request->route = route;
  request->frame = frame;
  request->status = TURBO_EALREADY;
  request->delivery_stage = FLOW_FMQ_DELIVERY_NOT_SUBMITTED;
  request->budget_bytes = tstr_len(frame);
  request->budget_reserved = 1;
  request->drain_allowed = adapter->frame_linger_ms != 0;
  atomic_init(&request->refs, 2);
  turbo_mutex_init(&request->mutex);
  turbo_cond_init(&request->cond);
  if (coro_context_current() == adapter->ctx) {
    request->completion_wait = coro_wait_create(adapter->ctx);
    if (!request->completion_wait) {
      flow_fmq_emit_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED, TURBO_ENOMEM,
                                request->budget_bytes);
      flow_fmq_release_send_budget(adapter, request->budget_bytes);
      request->budget_reserved = 0;
      flow_fmq_request_release(request);
      flow_fmq_request_release(request);
      return TURBO_ENOMEM;
    }
  }
  rc = flow_fmq_enqueue_send(adapter, request);
  if (rc != TURBO_OK) {
    int completed;
    turbo_mutex_lock(&request->mutex);
    completed = request->done;
    if (completed) rc = request->status;
    turbo_mutex_unlock(&request->mutex);
    if (!completed) {
      flow_fmq_emit_frame_event(adapter, TURBO_FLOW_FMQ_EVENT_FRAME_DROPPED, rc,
                                request->budget_bytes);
      flow_fmq_release_send_budget(adapter, request->budget_bytes);
      request->budget_reserved = 0;
      flow_fmq_request_release(request);
    }
    flow_fmq_request_release(request);
    return rc;
  }
  *out_request = request;
  return TURBO_OK;
}

static int flow_fmq_select_metadata(flow_fmq_adapter_t *adapter, const turbo_flow_msg_t *msg,
                                    turbo_flow_fmq_metadata_policy_t policy, int identity,
                                    tstr_v *out) {
  flow_fmq_message_context_t *message_context;
  if (!adapter || !out) return TURBO_EINVAL;
  if (policy == TURBO_FLOW_FMQ_METADATA_STATIC) {
    *out = identity ? tstr_to_v(adapter->identity) : tstr_to_v(adapter->topic);
    return TURBO_OK;
  }
  if (policy == TURBO_FLOW_FMQ_METADATA_CONTENT) {
    const turbo_flow_content_descriptor_t *descriptor;
    if (!msg || !(descriptor = turbo_flow_msg_content_descriptor(msg))) return TURBO_ENOENT;
    *out = tstr_v_from_buf(descriptor->identity, strlen(descriptor->identity));
    return TURBO_OK;
  }
  if (policy != TURBO_FLOW_FMQ_METADATA_INHERIT || !msg) return TURBO_EINVAL;
  message_context = (flow_fmq_message_context_t *)msg->transport_context;
  if (!message_context || message_context->magic != FLOW_FMQ_MESSAGE_CONTEXT_MAGIC) {
    return TURBO_ENOENT;
  }
  *out = identity ? message_context->identity : message_context->topic;
  return TURBO_OK;
}

static int flow_fmq_consume_with_delivery(void *ctx, turbo_flow_t *flow,
                                          const turbo_flow_stage_plan_t *stage,
                                          turbo_flow_msg_t *msg,
                                          flow_fmq_delivery_stage_t *delivery_stage) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  flow_fmq_message_context_t *message_context = NULL;
  flow_fmq_frame_t frame;
  flow_fmq_send_request_t *request = NULL;
  tstr_t encoded = NULL;
  size_t encoded_size;
  uint64_t correlation_id = 0u;
  flow_fmq_route_token_t send_route = {0u, 0u};
  flow_fmq_delivery_stage_t current_delivery_stage = FLOW_FMQ_DELIVERY_NOT_SUBMITTED;
  int req_state_acquired = 0;
  int rc;
  (void)flow;
  (void)stage;
  if (delivery_stage) *delivery_stage = FLOW_FMQ_DELIVERY_NOT_SUBMITTED;
  if (!adapter || !msg || (msg->payload.len > 0 && !msg->payload.data)) {
    return TURBO_EINVAL;
  }
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
  if (adapter->pattern == TURBO_FLOW_FMQ_SUB || adapter->pattern == TURBO_FLOW_FMQ_PULL) {
    return TURBO_ENOTSUP;
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_XSUB) {
    message_context = (flow_fmq_message_context_t *)msg->transport_context;
    if (!message_context || message_context->magic != FLOW_FMQ_MESSAGE_CONTEXT_MAGIC ||
        message_context->subscription == 0) {
      return TURBO_EINVAL;
    }
  }
  memset(&frame, 0, sizeof(frame));
  frame.kind = FLOW_FMQ_FRAME_DATA;
  frame.pattern = adapter->pattern;
  frame.message_id =
      atomic_fetch_add_explicit(&adapter->next_message_id, 1u, memory_order_relaxed) + 1u;
  if (frame.message_id == 0u) {
    frame.message_id =
        atomic_fetch_add_explicit(&adapter->next_message_id, 1u, memory_order_relaxed) + 1u;
  }
  correlation_id = frame.message_id;
  if (adapter->pattern == TURBO_FLOW_FMQ_REQ) {
    rc = turbo_flow_pattern_exchange_begin(&adapter->exchange, FLOW_FMQ_PATTERN_WAIT_REPLY,
                                           correlation_id);
    if (rc != TURBO_OK) return rc;
    req_state_acquired = 1;
  }
  frame.payload = msg->payload;
  if (adapter->pattern == TURBO_FLOW_FMQ_XSUB) {
    frame.kind =
        message_context->subscription > 0 ? FLOW_FMQ_FRAME_SUBSCRIBE : FLOW_FMQ_FRAME_UNSUBSCRIBE;
    frame.message_id = 0u;
    correlation_id = 0u;
    frame.payload = (tstr_v){0};
    frame.topic = message_context->topic;
    frame.identity = (tstr_v){0};
    goto frame_ready;
  }
  rc = flow_fmq_select_metadata(adapter, msg, adapter->topic_policy, 0, &frame.topic);
  if (rc != TURBO_OK) goto local_failure;
  rc = flow_fmq_select_metadata(adapter, msg, adapter->identity_policy, 1, &frame.identity);
  if (rc != TURBO_OK) goto local_failure;
  if (adapter->pattern == TURBO_FLOW_FMQ_ROUTER || adapter->pattern == TURBO_FLOW_FMQ_REP ||
      (adapter->pattern == TURBO_FLOW_FMQ_PAIR && adapter->mode == TURBO_FLOW_FMQ_BIND)) {
    message_context = (flow_fmq_message_context_t *)msg->transport_context;
    if (adapter->pattern == TURBO_FLOW_FMQ_ROUTER && !message_context) {
      const turbo_flow_protocol_route_t *route = turbo_flow_msg_protocol_route(msg);
      if (!route || route->protocol != TURBO_FLOW_PROTOCOL_FMQ ||
          route->owner_instance_id != adapter->instance_id) {
        rc = TURBO_EINVAL;
        goto local_failure;
      }
      if (route->session_generation !=
          atomic_load_explicit(&adapter->generation, memory_order_acquire)) {
        rc = TURBO_ENOTCONN;
        goto local_failure;
      }
      send_route.session_id = route->session_id;
      send_route.generation = route->session_generation;
    } else {
      if (!message_context || message_context->magic != FLOW_FMQ_MESSAGE_CONTEXT_MAGIC ||
          message_context->adapter != adapter || !message_context->peer) {
        rc = adapter->pattern == TURBO_FLOW_FMQ_REP ? TURBO_EBUSY : TURBO_EINVAL;
        goto local_failure;
      }
      send_route = message_context->peer->route;
    }
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_REP) {
    if (message_context->correlation_id == 0u ||
        turbo_flow_pattern_exchange_match(&message_context->peer->exchange,
                                          FLOW_FMQ_PATTERN_PROCESSING_REQUEST,
                                          message_context->correlation_id) != TURBO_OK) {
      rc = TURBO_EBUSY;
      goto local_failure;
    }
    frame.message_id = message_context->correlation_id;
    correlation_id = frame.message_id;
  }
frame_ready:
  rc = flow_fmq_encoded_size(&frame, adapter->max_frame_size, &encoded_size);
  if (rc != TURBO_OK) goto local_failure;
  turbo_mutex_lock(&adapter->send_admission_mutex);
  rc = flow_fmq_reserve_send_budget(adapter, encoded_size);
  if (rc != TURBO_OK) {
    turbo_mutex_unlock(&adapter->send_admission_mutex);
    goto local_failure;
  }
  rc = flow_fmq_encode_frame(&frame, adapter->max_frame_size, &encoded);
  if (rc != TURBO_OK) {
    flow_fmq_release_send_budget(adapter, encoded_size);
    turbo_mutex_unlock(&adapter->send_admission_mutex);
    goto local_failure;
  }
  rc = flow_fmq_submit_send(adapter, send_route, encoded, &request);
  turbo_mutex_unlock(&adapter->send_admission_mutex);
  if (rc != TURBO_OK) goto transport_failure;
  rc = flow_fmq_wait_send_request(adapter, request, &current_delivery_stage);
  flow_fmq_request_release(request);
  if (delivery_stage) *delivery_stage = current_delivery_stage;
  if (rc != TURBO_OK) goto transport_failure;
  if (adapter->pattern == TURBO_FLOW_FMQ_REP) {
    rc = turbo_flow_pattern_exchange_finish(&message_context->peer->exchange,
                                            FLOW_FMQ_PATTERN_PROCESSING_REQUEST,
                                            message_context->correlation_id,
                                            FLOW_FMQ_PATTERN_READY);
  }
  return rc;

local_failure:
  if (req_state_acquired) {
    int finish_rc = turbo_flow_pattern_exchange_finish(
        &adapter->exchange, FLOW_FMQ_PATTERN_WAIT_REPLY, correlation_id, FLOW_FMQ_PATTERN_READY);
    if (finish_rc != TURBO_OK) return finish_rc;
  }
  return rc;

transport_failure:
  if (req_state_acquired) {
    flow_fmq_pattern_state_t terminal =
        flow_fmq_delivery_requires_session_reset(current_delivery_stage)
            ? FLOW_FMQ_PATTERN_RESETTING
            : FLOW_FMQ_PATTERN_READY;
    int finish_rc = turbo_flow_pattern_exchange_finish(
        &adapter->exchange, FLOW_FMQ_PATTERN_WAIT_REPLY, correlation_id, terminal);
    if (terminal == FLOW_FMQ_PATTERN_RESETTING) {
      if (adapter->client) (void)coro_socket_interrupt_wait(adapter->client, rc);
    }
    if (finish_rc != TURBO_OK) return finish_rc;
  }
  if (adapter->pattern == TURBO_FLOW_FMQ_REP && message_context && message_context->peer) {
    if (flow_fmq_delivery_requires_session_reset(current_delivery_stage)) {
      (void)turbo_flow_pattern_exchange_mark_resetting(&message_context->peer->exchange);
      (void)coro_socket_interrupt_wait(message_context->peer->socket, rc);
    }
  }
  return rc;
}

static int flow_fmq_consume(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                            turbo_flow_msg_t *msg) {
  return flow_fmq_consume_with_delivery(ctx, flow, stage, msg, NULL);
}

typedef struct flow_fmq_retry_context_s {
  flow_fmq_adapter_t *adapter;
  flow_fmq_delivery_stage_t delivery_stage;
} flow_fmq_retry_context_t;

static int flow_fmq_retry_attempt(void *ctx, turbo_flow_msg_t *msg, uint32_t attempt) {
  flow_fmq_retry_context_t *retry = (flow_fmq_retry_context_t *)ctx;
  (void)attempt;
  retry->delivery_stage = FLOW_FMQ_DELIVERY_NOT_SUBMITTED;
  return flow_fmq_consume_with_delivery(retry->adapter, NULL, NULL, msg, &retry->delivery_stage);
}

static int flow_fmq_retryable(void *ctx, int status) {
  flow_fmq_retry_context_t *retry = (flow_fmq_retry_context_t *)ctx;
  return retry && flow_fmq_delivery_retryable(retry->delivery_stage, status);
}

static int flow_fmq_retry_wait(void *ctx, uint32_t delay_ms) {
  flow_fmq_retry_context_t *retry = (flow_fmq_retry_context_t *)ctx;
  flow_fmq_adapter_t *adapter = retry ? retry->adapter : NULL;
  int rc;
  if (!adapter || !adapter->retry_wait_initialized) return TURBO_EINVAL;
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
  rc = tf_timer_wait_for_ms(&adapter->retry_wait, delay_ms);
  if (rc == TURBO_ETIMEDOUT) return TURBO_OK;
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
  return rc;
}

static int flow_fmq_consume_retry(void *ctx, turbo_flow_t *flow,
                                  const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg,
                                  const turbo_flow_retry_policy_t *policy) {
  turbo_flow_retry_ops_t ops;
  flow_fmq_retry_context_t retry;
  (void)flow;
  (void)stage;
  memset(&ops, 0, sizeof(ops));
  memset(&retry, 0, sizeof(retry));
  retry.adapter = (flow_fmq_adapter_t *)ctx;
  ops.size = sizeof(ops);
  ops.attempt = flow_fmq_retry_attempt;
  ops.retryable = flow_fmq_retryable;
  ops.wait = flow_fmq_retry_wait;
  return turbo_flow_retry_execute(policy, msg, &ops, &retry);
}

static void flow_fmq_stop_resources(flow_fmq_adapter_t *adapter) {
  uint64_t timeout_ns;
  if (!adapter || !atomic_load_explicit(&adapter->started, memory_order_acquire)) return;
  flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_CLOSING, TURBO_ESHUTDOWN,
                      atomic_load_explicit(&adapter->connections_current, memory_order_relaxed));
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  tf_io_budget_close(&adapter->send_budget);
  if (adapter->retry_wait_initialized) tf_timer_stop(&adapter->retry_wait);
  if (adapter->reconnect_wait) {
    (void)coro_wait_interrupt(adapter->reconnect_wait, TURBO_ESHUTDOWN);
  }
  /* Close admission first so a BLOCK waiter wakes, then serialize against the
   * reserve-to-enqueue critical section before draining accepted requests. */
  turbo_mutex_lock(&adapter->send_admission_mutex);
  if (adapter->frame_linger_ms == 0) flow_fmq_fail_send_queue(adapter, TURBO_ESHUTDOWN);
  flow_fmq_wait_send_drain(adapter);
  turbo_mutex_unlock(&adapter->send_admission_mutex);
  tf_timer_signal(&adapter->connect_wait);
  timeout_ns = flow_fmq_connect_timeout_ns(adapter);
  (void)flow_fmq_reactor_call(adapter, FLOW_FMQ_REACTOR_CLOSE_RESOURCES, timeout_ns);
  flow_fmq_wait_lane_tasks(adapter);
  (void)flow_fmq_reactor_call(adapter, FLOW_FMQ_REACTOR_CLEAR_PEERS, timeout_ns);
  tf_coronet_execution_stop(&adapter->execution);
  flow_fmq_transition(adapter, TURBO_FLOW_CONNECTION_STOPPED, TURBO_ESHUTDOWN, 0u);
}

static void flow_fmq_stop(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  (void)flow;
  (void)stage;
  if (!adapter || adapter->start_refs <= 0) return;
  adapter->start_refs -= 1;
  if (adapter->start_refs > 0) return;
  atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
  flow_fmq_stop_resources(adapter);
  if (adapter->subscriptions_initialized) flow_fmq_subscriptions_clear(&adapter->subscriptions);
}

static int flow_fmq_endpoint_prepare(flow_fmq_adapter_t *adapter,
                                     const turbo_flow_adapter_endpoint_t *endpoint, tstr_t *host,
                                     tstr_t *path) {
  if (!adapter || !endpoint || !host || !path || *host || *path) return TURBO_EINVAL;
  if (adapter->transport == TURBO_FLOW_FMQ_PIPE) {
    if (!endpoint->path || endpoint->path[0] == '\0') return TURBO_EINVAL;
    *host = tstr_new();
    *path = tstr_dup(endpoint->path);
  } else {
    if (!endpoint->host || endpoint->host[0] == '\0' || endpoint->port < 1 ||
        endpoint->port > 65535) {
      return TURBO_EINVAL;
    }
    *host = tstr_dup(endpoint->host);
    *path = endpoint->path ? tstr_dup(endpoint->path) : tstr_new();
  }
  if (!*host || !*path) {
    tstr_freep(host);
    tstr_freep(path);
    return TURBO_ENOMEM;
  }
  return TURBO_OK;
}

static int flow_fmq_replace_endpoint(flow_fmq_adapter_t *adapter,
                                     const turbo_flow_adapter_endpoint_t *endpoint) {
  tstr_t next_host = NULL;
  tstr_t next_path = NULL;
  tstr_t old_host;
  tstr_t old_path;
  int old_port;
  int was_started;
  int rc;
  int rollback_rc;

  rc = flow_fmq_endpoint_prepare(adapter, endpoint, &next_host, &next_path);
  if (rc != TURBO_OK) return rc;
  old_host = adapter->host;
  old_path = adapter->path;
  old_port = adapter->port;
  was_started = atomic_load_explicit(&adapter->started, memory_order_acquire);
  if (was_started) flow_fmq_stop_resources(adapter);
  adapter->host = next_host;
  adapter->path = next_path;
  adapter->port = adapter->transport == TURBO_FLOW_FMQ_PIPE ? 0 : endpoint->port;
  if (!was_started) {
    tstr_free(old_host);
    tstr_free(old_path);
    return TURBO_OK;
  }

  rc = flow_fmq_start_resources(adapter);
  if (rc == TURBO_OK) {
    tstr_free(old_host);
    tstr_free(old_path);
    return TURBO_OK;
  }
  flow_fmq_stop_resources(adapter);
  next_host = adapter->host;
  next_path = adapter->path;
  adapter->host = old_host;
  adapter->path = old_path;
  adapter->port = old_port;
  rollback_rc = flow_fmq_start_resources(adapter);
  tstr_free(next_host);
  tstr_free(next_path);
  return rollback_rc == TURBO_OK ? rc : rollback_rc;
}

static int flow_fmq_command(void *ctx, turbo_flow_t *flow,
                            const turbo_flow_adapter_command_t *command) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  (void)flow;
  if (!adapter || !command || adapter->start_refs <= 0) return TURBO_EINVAL;
  switch (command->kind) {
  case TURBO_FLOW_ADAPTER_QUIESCE:
    if (atomic_exchange_explicit(&adapter->quiesced, 1, memory_order_acq_rel) == 0) {
      flow_fmq_stop_resources(adapter);
    }
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_RESUME:
    if (!atomic_load_explicit(&adapter->quiesced, memory_order_acquire)) return TURBO_OK;
    {
      int rc = flow_fmq_start_resources(adapter);
      if (rc != TURBO_OK) return rc;
    }
    atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT:
    return flow_fmq_replace_endpoint(adapter, &command->endpoint);
  default:
    return TURBO_EINVAL;
  }
}

static int flow_fmq_resource_command(void *ctx, turbo_flow_t *flow,
                                     const turbo_flow_resource_command_t *command) {
  flow_fmq_resource_context_t *resource = (flow_fmq_resource_context_t *)ctx;
  turbo_flow_adapter_command_t owner_command;
  if (!resource || !resource->adapter || !command ||
      resource->kind != TURBO_FLOW_RESOURCE_CONNECTION) {
    return resource && command ? TURBO_ENOTSUP : TURBO_EINVAL;
  }
  memset(&owner_command, 0, sizeof(owner_command));
  owner_command.size = sizeof(owner_command);
  switch (command->kind) {
  case TURBO_FLOW_RESOURCE_COMMAND_QUIESCE:
    owner_command.kind = TURBO_FLOW_ADAPTER_QUIESCE;
    break;
  case TURBO_FLOW_RESOURCE_COMMAND_RESUME:
    owner_command.kind = TURBO_FLOW_ADAPTER_RESUME;
    break;
  case TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT:
    owner_command.kind = TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT;
    owner_command.endpoint.host = command->endpoint_host;
    owner_command.endpoint.path = command->endpoint_path;
    owner_command.endpoint.port = command->endpoint_port;
    break;
  default:
    return TURBO_ENOTSUP;
  }
  return flow_fmq_command(resource->adapter, flow, &owner_command);
}

static void flow_fmq_shutdown(void *ctx) {
  flow_fmq_adapter_t *adapter = (flow_fmq_adapter_t *)ctx;
  if (!adapter) return;
  if (adapter->start_refs > 0) {
    adapter->start_refs = 1;
    flow_fmq_stop(adapter, NULL, NULL);
  }
  if (adapter->send_queue_initialized) {
    flow_fmq_fail_send_queue(adapter, TURBO_ESHUTDOWN);
    flow_fmq_send_requests_destroy(&adapter->send_queue);
    turbo_mutex_destroy(&adapter->send_queue_mutex);
    turbo_mutex_destroy(&adapter->send_admission_mutex);
    adapter->send_queue_initialized = 0;
  }
  tf_io_budget_destroy(&adapter->send_budget);
  if (adapter->retry_wait_initialized) {
    tf_timer_destroy(&adapter->retry_wait);
    adapter->retry_wait_initialized = 0;
  }
  if (adapter->connect_wait_initialized) {
    tf_timer_destroy(&adapter->connect_wait);
    adapter->connect_wait_initialized = 0;
  }
  if (adapter->reconnect_wait) {
    (void)coro_wait_destroy(adapter->reconnect_wait);
    adapter->reconnect_wait = NULL;
  }
  if (adapter->lane_task_sync_initialized) {
    turbo_cond_destroy(&adapter->lane_task_cond);
    turbo_mutex_destroy(&adapter->lane_task_mutex);
    adapter->lane_task_sync_initialized = 0;
  }
  if (adapter->reactor_actor_initialized) {
    tf_coronet_actor_close(&adapter->reactor_actor);
    (void)tf_coronet_actor_drain(&adapter->reactor_actor, UINT64_MAX);
    (void)tf_coronet_actor_destroy(&adapter->reactor_actor);
    adapter->reactor_actor_initialized = 0;
  }
  tf_coronet_execution_destroy(&adapter->execution);
  turbo_vec_destroy(&adapter->peers);
  if (adapter->subscriptions_initialized) {
    flow_fmq_subscriptions_clear(&adapter->subscriptions);
    turbo_vec_destroy(&adapter->subscriptions);
    adapter->subscriptions_initialized = 0;
  }
  tstr_freep(&adapter->host);
  tstr_freep(&adapter->path);
  tstr_freep(&adapter->topic);
  tstr_freep(&adapter->identity);
  tstr_freep(&adapter->source_name);
  tstr_freep(&adapter->resource_owner);
  tstr_freep(&adapter->connection_uid);
  tstr_freep(&adapter->queue_uid);
  tstr_freep(&adapter->protocol_uid);
  tstr_freep(&adapter->udp_multicast_group);
  tstr_freep(&adapter->udp_multicast_interface);
  free(adapter);
}

static uint32_t flow_fmq_roles(turbo_flow_fmq_pattern_t pattern) {
  switch (pattern) {
  case TURBO_FLOW_FMQ_PUB:
  case TURBO_FLOW_FMQ_PUSH:
    return TURBO_FLOW_ADAPTER_SINK;
  case TURBO_FLOW_FMQ_SUB:
  case TURBO_FLOW_FMQ_PULL:
    return TURBO_FLOW_ADAPTER_SOURCE;
  case TURBO_FLOW_FMQ_ROUTER:
  case TURBO_FLOW_FMQ_DEALER:
  case TURBO_FLOW_FMQ_PAIR:
  case TURBO_FLOW_FMQ_REQ:
  case TURBO_FLOW_FMQ_REP:
  case TURBO_FLOW_FMQ_XPUB:
  case TURBO_FLOW_FMQ_XSUB:
    return TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK;
  default:
    return 0;
  }
}

static int flow_fmq_fanout_config_validate(const turbo_flow_fmq_config_t *config,
                                           const turbo_flow_fmq_fanout_config_t *fanout) {
  if (!config || !fanout || fanout->size < sizeof(*fanout) ||
      fanout->version != TURBO_FLOW_FMQ_API_VERSION ||
      (fanout->peer_hwm_messages == 0u && fanout->peer_hwm_bytes == 0u) ||
      fanout->slow_peer_policy < TURBO_FLOW_FMQ_SLOW_PEER_FAIL ||
      fanout->slow_peer_policy > TURBO_FLOW_FMQ_SLOW_PEER_DISCONNECT) {
    return TURBO_EINVAL;
  }
  if ((config->pattern != TURBO_FLOW_FMQ_PUB && config->pattern != TURBO_FLOW_FMQ_XPUB) ||
      config->mode != TURBO_FLOW_FMQ_BIND) {
    return TURBO_ENOTSUP;
  }
  if (config->transport == TURBO_FLOW_FMQ_UDP || config->transport == TURBO_FLOW_FMQ_KCP) {
    return TURBO_ENOTSUP;
  }
  return TURBO_OK;
}

static int
flow_fmq_register_adapter_internal(turbo_flow_t *flow, const char *name,
                                   const turbo_flow_fmq_config_t *config,
                                   const turbo_flow_fmq_fanout_config_t *fanout,
                                   const turbo_flow_coronet_execution_binding_t *execution) {
  flow_fmq_adapter_t *adapter;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_adapter_schema_t schema;
  turbo_flow_resource_provider_registration_t resources[3];
  uint32_t roles;
  int rc;
  if (!flow || !name || name[0] == '\0') return TURBO_EINVAL;
  if (!config || config->context || config->take_context_ownership) return TURBO_EINVAL;
  rc = turbo_flow_coronet_execution_binding_validate(execution);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_config_validate(config);
  if (rc != TURBO_OK) return rc;
  if (fanout) {
    rc = flow_fmq_fanout_config_validate(config, fanout);
    if (rc != TURBO_OK) return rc;
  }
  roles = flow_fmq_roles(config->pattern);
  if (roles == 0) return TURBO_EINVAL;
  adapter = (flow_fmq_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  rc = flow_fmq_allocate_adapter_instance_id(&adapter->instance_id);
  if (rc != TURBO_OK) {
    free(adapter);
    return rc;
  }
  if (turbo_vec_init(&adapter->peers, sizeof(flow_fmq_peer_t *)) != TURBO_OK) {
    free(adapter);
    return TURBO_ENOMEM;
  }
  if (turbo_vec_init(&adapter->subscriptions, sizeof(flow_fmq_subscription_t)) != TURBO_OK) {
    turbo_vec_destroy(&adapter->peers);
    free(adapter);
    return TURBO_ENOMEM;
  }
  adapter->subscriptions_initialized = 1;
  rc = tf_coronet_execution_init(&adapter->execution, execution);
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&adapter->subscriptions);
    turbo_vec_destroy(&adapter->peers);
    free(adapter);
    return rc;
  }
  rc = tf_coronet_actor_init(&adapter->reactor_actor, &adapter->execution, flow_fmq_reactor_command,
                             adapter, FLOW_FMQ_REACTOR_COMMAND_CAPACITY,
                             sizeof(flow_fmq_reactor_command_t));
  if (rc != TURBO_OK) {
    tf_coronet_execution_destroy(&adapter->execution);
    turbo_vec_destroy(&adapter->subscriptions);
    turbo_vec_destroy(&adapter->peers);
    free(adapter);
    return rc;
  }
  adapter->reactor_actor_initialized = 1;
  turbo_mutex_init(&adapter->lane_task_mutex);
  turbo_cond_init(&adapter->lane_task_cond);
  adapter->lane_task_sync_initialized = 1;
  adapter->ctx = adapter->execution.context;
  if (adapter->ctx) adapter->reconnect_wait = coro_wait_create(adapter->ctx);
  adapter->host = config->host ? tstr_dup(config->host) : tstr_new();
  adapter->path = config->path ? tstr_dup(config->path) : tstr_new();
  adapter->topic = config->topic ? tstr_dup(config->topic) : tstr_new();
  adapter->identity = config->identity ? tstr_dup(config->identity) : tstr_new();
  adapter->resource_owner = tstr_dup(name);
  adapter->connection_uid = tstr_format("fmq:{}:connection", name);
  adapter->queue_uid = tstr_format("fmq:{}:queue", name);
  adapter->protocol_uid = tstr_format("fmq:{}:protocol", name);
  adapter->pattern = config->pattern;
  adapter->initial_subscription_configured = config->topic != NULL;
  adapter->mode = config->mode;
  adapter->transport = config->transport;
  rc = turbo_flow_content_descriptor_init(
      &adapter->control_descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
      TURBO_FLOW_CONTENT_PROFILE_FMQ_CONTROL, TURBO_FLOW_DATA_ENCODING_OPAQUE,
      "application/octet-stream", config->topic);
  if (rc != TURBO_OK) {
    flow_fmq_shutdown(adapter);
    return rc;
  }
  adapter->control_descriptor.flags |= TURBO_FLOW_CONTENT_PROTOCOL_CONTROL;
  if (config->content_type) {
    rc = turbo_flow_content_descriptor_from_media(
        &adapter->data_descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
        TURBO_FLOW_CONTENT_PROFILE_FMQ_DATA, config->content_type, config->topic,
        config->content_binding);
    if (rc == TURBO_OK) {
      adapter->has_data_descriptor = 1;
    } else if (rc != TURBO_ENOENT ||
               (config->content_binding && config->content_binding->schema.schema_version != 0u)) {
      flow_fmq_shutdown(adapter);
      return rc == TURBO_ENOENT ? TURBO_EPROTO : rc;
    }
  }
  adapter->topic_policy =
      config->topic_policy ? config->topic_policy : TURBO_FLOW_FMQ_METADATA_STATIC;
  adapter->identity_policy =
      config->identity_policy ? config->identity_policy : TURBO_FLOW_FMQ_METADATA_STATIC;
  adapter->port = config->port;
  adapter->max_frame_size =
      config->max_frame_size ? config->max_frame_size : TURBO_FLOW_FMQ_DEFAULT_MAX_FRAME_SIZE;
  adapter->max_connections =
      config->max_connections ? config->max_connections : TURBO_FLOW_FMQ_DEFAULT_MAX_CONNECTIONS;
  flow_fmq_timeout_config_resolve(&adapter->timeouts, config);
  adapter->reconnect_initial_ms =
      config->reconnect_initial_ms == TURBO_FLOW_FMQ_RECONNECT_DISABLED
          ? 0
          : (config->reconnect_initial_ms ? config->reconnect_initial_ms
                                          : FLOW_FMQ_RECONNECT_INITIAL_MS);
  adapter->reconnect_max_ms =
      config->reconnect_max_ms == TURBO_FLOW_FMQ_RECONNECT_MAX_UNBOUNDED
          ? 0
          : (config->reconnect_max_ms ? config->reconnect_max_ms : FLOW_FMQ_RECONNECT_MAX_MS);
  adapter->reconnect_jitter_state = turbo_hrtime() ^ (uint64_t)(uintptr_t)adapter;
  adapter->heartbeat_interval_ms = config->heartbeat_interval_ms;
  adapter->heartbeat_timeout_ms = config->heartbeat_timeout_ms;
  adapter->event_callback = config->event_callback;
  adapter->event_ctx = config->event_ctx;
  {
    tf_coronet_kcp_fec_options_t fec_options;
    memset(&fec_options, 0, sizeof(fec_options));
    fec_options.enabled = config->kcp_fec;
    fec_options.backend = config->kcp_fec_backend;
    fec_options.data_shards = config->kcp_fec_data_shards;
    fec_options.parity_shards = config->kcp_fec_parity_shards;
    fec_options.max_payload_size = config->kcp_fec_max_payload_size;
    rc = tf_coronet_kcp_fec_options_resolve(flow_fmq_coronet_transport(adapter->transport),
                                            &fec_options, &adapter->kcp_fec,
                                            &adapter->kcp_fec_configured);
    if (rc != TURBO_OK) {
      flow_fmq_shutdown(adapter);
      return rc;
    }
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
  if (config->udp_multicast_group) {
    adapter->udp_multicast_group = tstr_dup(config->udp_multicast_group);
    if (!adapter->udp_multicast_group) {
      flow_fmq_shutdown(adapter);
      return TURBO_ENOMEM;
    }
    adapter->udp_options.multicast_group = adapter->udp_multicast_group;
  }
  if (config->udp_multicast_interface) {
    adapter->udp_multicast_interface = tstr_dup(config->udp_multicast_interface);
    if (!adapter->udp_multicast_interface) {
      flow_fmq_shutdown(adapter);
      return TURBO_ENOMEM;
    }
    adapter->udp_options.multicast_interface = adapter->udp_multicast_interface;
  }
  adapter->frame_hwm_messages = config->frame_hwm_messages;
  adapter->frame_hwm_bytes = config->frame_hwm_bytes;
  adapter->frame_admission_policy = config->frame_admission_policy;
  adapter->frame_linger_ms = config->frame_linger_ms;
  if (fanout) {
    adapter->fanout = *fanout;
    adapter->fanout.size = sizeof(adapter->fanout);
    adapter->fanout_enabled = 1;
  }
  atomic_init(&adapter->started, 0);
  atomic_init(&adapter->connect_status, TURBO_ENOTCONN);
  atomic_init(&adapter->connection_state, TURBO_FLOW_CONNECTION_STOPPED);
  atomic_init(&adapter->reconnect_failure_reported, 0);
  atomic_init(&adapter->quiesced, 0);
  atomic_init(&adapter->connections_current, 0);
  rc = turbo_flow_pattern_exchange_init(&adapter->exchange);
  if (rc != TURBO_OK) {
    flow_fmq_shutdown(adapter);
    return rc;
  }
  atomic_init(&adapter->generation, 1u);
  rc = turbo_flow_pattern_selector_init(&adapter->selector);
  if (rc != TURBO_OK) {
    flow_fmq_shutdown(adapter);
    return rc;
  }
  atomic_init(&adapter->next_message_id, 0u);
  if (flow_fmq_send_requests_init(&adapter->send_queue) != TURBO_OK) {
    flow_fmq_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  turbo_mutex_init(&adapter->send_queue_mutex);
  turbo_mutex_init(&adapter->send_admission_mutex);
  adapter->send_queue_initialized = 1;
  {
    const tf_io_budget_config_t budget_config = {
        adapter->frame_hwm_messages, adapter->frame_hwm_bytes,
        config->frame_admission_policy == TURBO_FLOW_FMQ_FRAME_ADMISSION_BLOCK
            ? TF_IO_ADMISSION_BLOCK
            : TF_IO_ADMISSION_FAIL,
        config->frame_admission_timeout_ms};
    if (tf_io_budget_init(&adapter->send_budget, &budget_config) != TURBO_OK) {
      flow_fmq_shutdown(adapter);
      return TURBO_ENOMEM;
    }
  }
  if (tf_timer_init(&adapter->connect_wait) != TURBO_OK) {
    flow_fmq_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  adapter->connect_wait_initialized = 1;
  if (tf_timer_init(&adapter->retry_wait) != TURBO_OK) {
    flow_fmq_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  adapter->retry_wait_initialized = 1;
  if (!adapter->ctx || !adapter->reconnect_wait || !adapter->host || !adapter->path ||
      !adapter->topic || !adapter->identity || !adapter->resource_owner ||
      !adapter->connection_uid || !adapter->queue_uid || !adapter->protocol_uid ||
      tstr_len(adapter->resource_owner) > TURBO_FLOW_RESOURCE_OWNER_MAX ||
      tstr_len(adapter->connection_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      tstr_len(adapter->queue_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      tstr_len(adapter->protocol_uid) > TURBO_FLOW_RESOURCE_UID_MAX) {
    int identity_missing = !adapter->resource_owner || !adapter->connection_uid ||
                           !adapter->queue_uid || !adapter->protocol_uid;
    flow_fmq_shutdown(adapter);
    return identity_missing ? TURBO_ENOMEM : TURBO_ENAMETOOLONG;
  }
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_fmq_start;
  ops.consume = flow_fmq_consume;
  ops.consume_retry = flow_fmq_consume_retry;
  ops.stop = flow_fmq_stop;
  ops.shutdown = flow_fmq_shutdown;
  ops.connection_snapshot = flow_fmq_connection_snapshot;
  ops.command = flow_fmq_command;
  memset(&schema, 0, sizeof(schema));
  schema.kind = TURBO_FLOW_ADAPTER_KIND_FMQ;
  schema.roles = roles;
  schema.direction = roles == TURBO_FLOW_ADAPTER_SOURCE ? TURBO_FLOW_ADAPTER_INPUT
                     : roles == TURBO_FLOW_ADAPTER_SINK ? TURBO_FLOW_ADAPTER_OUTPUT
                                                        : TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
  schema.fields = FLOW_FMQ_OPTION_FIELDS;
  schema.field_count = sizeof(FLOW_FMQ_OPTION_FIELDS) / sizeof(FLOW_FMQ_OPTION_FIELDS[0]);
  for (size_t i = 0; i < 3u; ++i) {
    resources[i] =
        (turbo_flow_resource_provider_registration_t)TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
    resources[i].owner_name = adapter->resource_owner;
    resources[i].ops.metadata = flow_fmq_resource_metadata;
    resources[i].ops.snapshot = flow_fmq_resource_snapshot;
    resources[i].ops.document = flow_fmq_resource_document;
    resources[i].ctx = &adapter->resource_contexts[i];
    adapter->resource_contexts[i].adapter = adapter;
  }
  adapter->resource_contexts[0].kind = TURBO_FLOW_RESOURCE_CONNECTION;
  adapter->resource_contexts[1].kind = TURBO_FLOW_RESOURCE_QUEUE_BUFFER;
  adapter->resource_contexts[2].kind = TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE;
  resources[0].ops.command = flow_fmq_resource_command;
  rc =
      turbo_flow_register_adapter_with_resources(flow, name, &ops, adapter, &schema, resources, 3u);
  return rc;
}

int turbo_flow_fmq_register_adapter_ex(turbo_flow_t *flow, const char *name,
                                       const turbo_flow_fmq_config_t *config,
                                       const turbo_flow_coronet_execution_binding_t *execution) {
  return flow_fmq_register_adapter_internal(flow, name, config, NULL, execution);
}

int turbo_flow_fmq_register_fanout_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_fmq_config_t *config,
    const turbo_flow_fmq_fanout_config_t *fanout,
    const turbo_flow_coronet_execution_binding_t *execution) {
  return flow_fmq_register_adapter_internal(flow, name, config, fanout, execution);
}

int turbo_flow_fmq_register_adapter(turbo_flow_t *flow, const char *name,
                                    const turbo_flow_fmq_config_t *config) {
  turbo_flow_coronet_execution_binding_t execution;
  turbo_flow_fmq_config_t normalized;
  int rc;
  rc = flow_fmq_config_validate(config);
  if (rc != TURBO_OK) return rc;
  memset(&execution, 0, sizeof(execution));
  execution.size = sizeof(execution);
  if (config->context) {
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT;
    execution.context = config->context;
  } else {
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  }
  normalized = *config;
  normalized.context = NULL;
  normalized.take_context_ownership = 0;
  return turbo_flow_fmq_register_adapter_ex(flow, name, &normalized, &execution);
}

int turbo_flow_fmq_register_fanout_adapter(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_fmq_config_t *config,
                                           const turbo_flow_fmq_fanout_config_t *fanout) {
  turbo_flow_coronet_execution_binding_t execution;
  turbo_flow_fmq_config_t normalized;
  int rc = flow_fmq_config_validate(config);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_fanout_config_validate(config, fanout);
  if (rc != TURBO_OK) return rc;
  memset(&execution, 0, sizeof(execution));
  execution.size = sizeof(execution);
  if (config->context) {
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_OWNED_CONTEXT;
    execution.context = config->context;
  } else {
    execution.kind = TURBO_FLOW_CORONET_EXECUTION_PRIVATE;
  }
  normalized = *config;
  normalized.context = NULL;
  normalized.take_context_ownership = 0;
  return turbo_flow_fmq_register_fanout_adapter_ex(flow, name, &normalized, fanout, &execution);
}

static int flow_fmq_message_context(const turbo_flow_msg_t *msg, flow_fmq_message_context_t **out) {
  flow_fmq_message_context_t *context;
  if (!msg || !out) return TURBO_EINVAL;
  context = (flow_fmq_message_context_t *)msg->transport_context;
  if (!context || context->magic != FLOW_FMQ_MESSAGE_CONTEXT_MAGIC) return TURBO_ENOENT;
  *out = context;
  return TURBO_OK;
}

int turbo_flow_fmq_message_topic(const turbo_flow_msg_t *msg, tstr_v *topic) {
  flow_fmq_message_context_t *context;
  int rc;
  if (!topic) return TURBO_EINVAL;
  rc = flow_fmq_message_context(msg, &context);
  if (rc != TURBO_OK) return rc;
  *topic = context->topic;
  return TURBO_OK;
}

int turbo_flow_fmq_message_identity(const turbo_flow_msg_t *msg, tstr_v *identity) {
  flow_fmq_message_context_t *context;
  int rc;
  if (!identity) return TURBO_EINVAL;
  rc = flow_fmq_message_context(msg, &context);
  if (rc != TURBO_OK) return rc;
  *identity = context->identity;
  return TURBO_OK;
}

int turbo_flow_fmq_message_correlation_id(const turbo_flow_msg_t *msg, uint64_t *correlation_id) {
  flow_fmq_message_context_t *context;
  int rc;
  if (!correlation_id) return TURBO_EINVAL;
  rc = flow_fmq_message_context(msg, &context);
  if (rc != TURBO_OK) return rc;
  *correlation_id = context->correlation_id;
  return TURBO_OK;
}

int turbo_flow_fmq_message_detach_router_route(turbo_flow_msg_t *msg) {
  flow_fmq_message_context_t *context;
  turbo_flow_protocol_route_t route = TURBO_FLOW_PROTOCOL_ROUTE_INIT;
  int rc;
  if (!msg) return TURBO_EINVAL;
  rc = flow_fmq_message_context(msg, &context);
  if (rc != TURBO_OK) return rc;
  if (!context->adapter || context->adapter->pattern != TURBO_FLOW_FMQ_ROUTER || !context->peer ||
      context->peer->route.session_id == 0u || context->peer->route.generation == 0u) {
    return TURBO_ENOTSUP;
  }
  route.protocol = TURBO_FLOW_PROTOCOL_FMQ;
  route.owner_instance_id = context->adapter->instance_id;
  route.session_id = context->peer->route.session_id;
  route.session_generation = context->peer->route.generation;
  rc = turbo_flow_msg_set_protocol_route(msg, &route);
  if (rc != TURBO_OK) return rc;
  if (turbo_flow_msg_content_descriptor(msg)) {
    rc = turbo_flow_msg_copy_content_descriptor(msg, turbo_flow_msg_content_descriptor(msg));
    if (rc != TURBO_OK) {
      turbo_flow_msg_clear_protocol_route(msg);
      return rc;
    }
  }
  msg->transport_context = NULL;
  return TURBO_OK;
}

int turbo_flow_fmq_message_subscription(const turbo_flow_msg_t *msg, int *subscribe,
                                        tstr_v *topic) {
  flow_fmq_message_context_t *context;
  int rc;
  if (!subscribe || !topic) return TURBO_EINVAL;
  rc = flow_fmq_message_context(msg, &context);
  if (rc != TURBO_OK) return rc;
  if (context->subscription == 0) return TURBO_ENOENT;
  *subscribe = context->subscription > 0;
  *topic = context->topic;
  return TURBO_OK;
}
