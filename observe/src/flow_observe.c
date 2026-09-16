#include "turbo_flow_observe.h"

#include "turbo_flow_stl_error_internal.h"
#include "salts_error.h"
#include "tstr.h"
#include "salts_thread.h"

#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static uint64_t flow_observe_add_saturated(uint64_t left, uint64_t right) {
  return left > UINT64_MAX - right ? UINT64_MAX : left + right;
}

static const turbo_flow_option_field_t FLOW_OBSERVE_LOG_FIELDS[] = {
    {"max_preview_bytes", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 0, TURBO_FLOW_OBSERVE_MAX_PREVIEW_BYTES,
     NULL, 0},
    {"include_payload_preview", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"write", TURBO_FLOW_OPTION_HOST_OBJECT,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL, 0},
    {"write_ctx", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL,
     0}};

typedef struct flow_observe_stage_entry_s {
  tstr name;
  uint64_t calls;
  uint64_t errors;
  uint64_t latency_ns_total;
  uint64_t latency_ns_max;
} flow_observe_stage_entry_t;

typedef struct flow_observe_log_sink_s {
  size_t max_preview_bytes;
  int include_payload_preview;
  turbo_flow_observe_log_fn write;
  void *write_ctx;
} flow_observe_log_sink_t;

struct turbo_flow_observe_s {
  turbo_flow_t *flow;
  vec_t stages;
  salts_mutex_t stage_mutex;
  salts_mutex_t export_mutex;
  deque_t events;
  size_t max_stages;
  size_t max_events;
  uint64_t next_event_sequence;
  uint64_t dropped_events;
  int has_command_result;
  char command_target_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  turbo_flow_resource_command_result_t command_result;
  atomic_uint_fast64_t messages;
  atomic_uint_fast64_t payload_bytes;
  atomic_uint_fast64_t message_errors;
  atomic_uint_fast64_t message_latency_ns_total;
  atomic_uint_fast64_t message_latency_ns_max;
  atomic_uint_fast64_t stage_calls;
  atomic_uint_fast64_t stage_errors;
  atomic_uint_fast64_t stage_latency_ns_total;
  atomic_uint_fast64_t stage_latency_ns_max;
  atomic_uint_fast64_t adapter_starts;
  atomic_uint_fast64_t adapter_stops;
  atomic_uint_fast64_t adapter_errors;
  atomic_uint_fast64_t dropped_stage_series;
  atomic_uint_fast64_t control_peer_connected;
  atomic_uint_fast64_t control_peer_disconnected;
  atomic_uint_fast64_t control_reconnect_scheduled;
  atomic_uint_fast64_t control_reconnect_succeeded;
  atomic_uint_fast64_t control_reconnect_failed;
  atomic_uint_fast64_t control_heartbeat_timeout;
  atomic_uint_fast64_t control_frame_sent;
  atomic_uint_fast64_t control_hwm_reached;
  atomic_uint_fast64_t control_frame_dropped;
  atomic_uint_fast64_t control_errors;
  atomic_int control_last_status;
  atomic_uint_fast64_t control_last_value;
};

static void flow_observe_atomic_max(atomic_uint_fast64_t *target, uint64_t value) {
  uint64_t current = atomic_load_explicit(target, memory_order_relaxed);
  while (current < value &&
         !atomic_compare_exchange_weak_explicit(target, &current, value, memory_order_relaxed,
                                                memory_order_relaxed)) {
  }
}

static void flow_observe_message_complete(void *ctx, const char *source_name,
                                          const turbo_flow_msg_t *msg, uint64_t duration_ns,
                                          int status) {
  turbo_flow_observe_t *observe = (turbo_flow_observe_t *)ctx;
  (void)source_name;
  (void)msg;
  if (!observe) return;
  atomic_fetch_add_explicit(&observe->messages, 1, memory_order_relaxed);
  atomic_fetch_add_explicit(&observe->payload_bytes, msg ? msg->payload.len : 0u,
                            memory_order_relaxed);
  if (status != SALTS_OK) {
    atomic_fetch_add_explicit(&observe->message_errors, 1, memory_order_relaxed);
  }
  atomic_fetch_add_explicit(&observe->message_latency_ns_total, duration_ns, memory_order_relaxed);
  flow_observe_atomic_max(&observe->message_latency_ns_max, duration_ns);
}

static flow_observe_stage_entry_t *flow_observe_find_stage(turbo_flow_observe_t *observe,
                                                           const char *stage_name) {
  /* Bounded O(max_stages); default 256 keeps opt-in update cost predictable. */
  for (size_t i = 0; i < vec_size(&observe->stages); ++i) {
    flow_observe_stage_entry_t *entry =
        (flow_observe_stage_entry_t *)vec_at(&observe->stages, i);
    if (entry && strcmp(entry->name, stage_name) == 0) return entry;
  }
  return NULL;
}

static void flow_observe_stage_complete(void *ctx, const char *stage_name, const char *adapter_name,
                                        const turbo_flow_msg_t *msg, uint64_t duration_ns,
                                        int status) {
  turbo_flow_observe_t *observe = (turbo_flow_observe_t *)ctx;
  flow_observe_stage_entry_t *entry;
  (void)msg;
  if (!observe || !stage_name) return;
  atomic_fetch_add_explicit(&observe->stage_calls, 1, memory_order_relaxed);
  if (status != SALTS_OK) {
    atomic_fetch_add_explicit(&observe->stage_errors, 1, memory_order_relaxed);
    if (adapter_name) {
      atomic_fetch_add_explicit(&observe->adapter_errors, 1, memory_order_relaxed);
    }
  }
  atomic_fetch_add_explicit(&observe->stage_latency_ns_total, duration_ns, memory_order_relaxed);
  flow_observe_atomic_max(&observe->stage_latency_ns_max, duration_ns);

  salts_mutex_lock(&observe->stage_mutex);
  entry = flow_observe_find_stage(observe, stage_name);
  if (!entry && vec_size(&observe->stages) < observe->max_stages) {
    flow_observe_stage_entry_t added;
    memset(&added, 0, sizeof(added));
    added.name = tstr_dup(stage_name);
    if (added.name && turbo_flow_stl_error(vec_push(&observe->stages, &added)) == SALTS_OK) {
      entry = (flow_observe_stage_entry_t *)vec_at(&observe->stages,
                                                         vec_size(&observe->stages) - 1u);
    } else {
      tstr_freep(&added.name);
    }
  }
  if (entry) {
    entry->calls += 1u;
    if (status != SALTS_OK) entry->errors += 1u;
    entry->latency_ns_total += duration_ns;
    if (duration_ns > entry->latency_ns_max) entry->latency_ns_max = duration_ns;
  } else {
    atomic_fetch_add_explicit(&observe->dropped_stage_series, 1, memory_order_relaxed);
  }
  salts_mutex_unlock(&observe->stage_mutex);
}

static void flow_observe_adapter_event(void *ctx, const char *stage_name, const char *adapter_name,
                                       turbo_flow_adapter_event_t event, int status) {
  turbo_flow_observe_t *observe = (turbo_flow_observe_t *)ctx;
  (void)stage_name;
  (void)adapter_name;
  if (!observe) return;
  if (event == TURBO_FLOW_ADAPTER_EVENT_START) {
    atomic_fetch_add_explicit(&observe->adapter_starts, 1, memory_order_relaxed);
    if (status != SALTS_OK) {
      atomic_fetch_add_explicit(&observe->adapter_errors, 1, memory_order_relaxed);
    }
  } else if (event == TURBO_FLOW_ADAPTER_EVENT_STOP) {
    atomic_fetch_add_explicit(&observe->adapter_stops, 1, memory_order_relaxed);
  }
}

static void flow_observe_flow_destroyed(void *ctx) {
  turbo_flow_observe_t *observe = (turbo_flow_observe_t *)ctx;
  if (observe) observe->flow = NULL;
}

turbo_flow_observe_t *turbo_flow_observe_create(const turbo_flow_observe_config_t *config) {
  turbo_flow_observe_t *observe;
  size_t max_stages =
      config && config->max_stages ? config->max_stages : TURBO_FLOW_OBSERVE_DEFAULT_MAX_STAGES;
  size_t max_events =
      config && config->max_events ? config->max_events : TURBO_FLOW_OBSERVE_DEFAULT_MAX_EVENTS;
  if (max_stages == 0 || max_stages > TURBO_FLOW_OBSERVE_MAX_STAGES || max_events == 0u ||
      max_events > TURBO_FLOW_OBSERVE_MAX_EVENTS)
    return NULL;
  observe = (turbo_flow_observe_t *)calloc(1, sizeof(*observe));
  if (!observe) return NULL;
  if (turbo_flow_stl_error(vec_init_bytes(&observe->stages, sizeof(flow_observe_stage_entry_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK) {
    free(observe);
    return NULL;
  }
  if (turbo_flow_stl_error(deque_init_bytes(&observe->events, sizeof(turbo_flow_observe_event_record_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != SALTS_OK ||
      turbo_flow_stl_error(deque_reserve(&observe->events, max_events)) != SALTS_OK) {
    deque_destroy(&observe->events);
    vec_destroy(&observe->stages);
    free(observe);
    return NULL;
  }
  salts_mutex_init(&observe->stage_mutex);
  salts_mutex_init(&observe->export_mutex);
  observe->max_stages = max_stages;
  observe->max_events = max_events;
  observe->command_result =
      (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
  atomic_init(&observe->messages, 0);
  atomic_init(&observe->payload_bytes, 0);
  atomic_init(&observe->message_errors, 0);
  atomic_init(&observe->message_latency_ns_total, 0);
  atomic_init(&observe->message_latency_ns_max, 0);
  atomic_init(&observe->stage_calls, 0);
  atomic_init(&observe->stage_errors, 0);
  atomic_init(&observe->stage_latency_ns_total, 0);
  atomic_init(&observe->stage_latency_ns_max, 0);
  atomic_init(&observe->adapter_starts, 0);
  atomic_init(&observe->adapter_stops, 0);
  atomic_init(&observe->adapter_errors, 0);
  atomic_init(&observe->dropped_stage_series, 0);
  atomic_init(&observe->control_peer_connected, 0);
  atomic_init(&observe->control_peer_disconnected, 0);
  atomic_init(&observe->control_reconnect_scheduled, 0);
  atomic_init(&observe->control_reconnect_succeeded, 0);
  atomic_init(&observe->control_reconnect_failed, 0);
  atomic_init(&observe->control_heartbeat_timeout, 0);
  atomic_init(&observe->control_frame_sent, 0);
  atomic_init(&observe->control_hwm_reached, 0);
  atomic_init(&observe->control_frame_dropped, 0);
  atomic_init(&observe->control_errors, 0);
  atomic_init(&observe->control_last_status, SALTS_OK);
  atomic_init(&observe->control_last_value, 0);
  return observe;
}

int turbo_flow_observe_destroy(turbo_flow_observe_t *observe) {
  if (!observe) return SALTS_EINVAL;
  if (observe->flow) return SALTS_EBUSY;
  for (size_t i = 0; i < vec_size(&observe->stages); ++i) {
    flow_observe_stage_entry_t *entry =
        (flow_observe_stage_entry_t *)vec_at(&observe->stages, i);
    if (entry) tstr_freep(&entry->name);
  }
  vec_destroy(&observe->stages);
  salts_mutex_destroy(&observe->stage_mutex);
  deque_destroy(&observe->events);
  salts_mutex_destroy(&observe->export_mutex);
  free(observe);
  return SALTS_OK;
}

int turbo_flow_observe_attach(turbo_flow_observe_t *observe, turbo_flow_t *flow) {
  turbo_flow_observer_ops_t ops;
  int rc;
  if (!observe || !flow || observe->flow) return SALTS_EINVAL;
  memset(&ops, 0, sizeof(ops));
  ops.size = sizeof(ops);
  ops.message_complete = flow_observe_message_complete;
  ops.stage_complete = flow_observe_stage_complete;
  ops.adapter_event = flow_observe_adapter_event;
  ops.flow_destroyed = flow_observe_flow_destroyed;
  rc = turbo_flow_set_observer(flow, &ops, observe);
  if (rc == SALTS_OK) observe->flow = flow;
  return rc;
}

int turbo_flow_observe_detach(turbo_flow_observe_t *observe) {
  int rc;
  if (!observe || !observe->flow) return SALTS_EINVAL;
  rc = turbo_flow_set_observer(observe->flow, NULL, NULL);
  if (rc == SALTS_OK) observe->flow = NULL;
  return rc;
}

int turbo_flow_observe_snapshot(const turbo_flow_observe_t *observe,
                                turbo_flow_observe_snapshot_t *out) {
  if (!observe || !out) return SALTS_EINVAL;
  out->messages = atomic_load_explicit(&observe->messages, memory_order_relaxed);
  out->payload_bytes = atomic_load_explicit(&observe->payload_bytes, memory_order_relaxed);
  out->message_errors = atomic_load_explicit(&observe->message_errors, memory_order_relaxed);
  out->message_latency_ns_total =
      atomic_load_explicit(&observe->message_latency_ns_total, memory_order_relaxed);
  out->message_latency_ns_max =
      atomic_load_explicit(&observe->message_latency_ns_max, memory_order_relaxed);
  out->stage_calls = atomic_load_explicit(&observe->stage_calls, memory_order_relaxed);
  out->stage_errors = atomic_load_explicit(&observe->stage_errors, memory_order_relaxed);
  out->stage_latency_ns_total =
      atomic_load_explicit(&observe->stage_latency_ns_total, memory_order_relaxed);
  out->stage_latency_ns_max =
      atomic_load_explicit(&observe->stage_latency_ns_max, memory_order_relaxed);
  out->adapter_starts = atomic_load_explicit(&observe->adapter_starts, memory_order_relaxed);
  out->adapter_stops = atomic_load_explicit(&observe->adapter_stops, memory_order_relaxed);
  out->adapter_errors = atomic_load_explicit(&observe->adapter_errors, memory_order_relaxed);
  out->dropped_stage_series =
      atomic_load_explicit(&observe->dropped_stage_series, memory_order_relaxed);
  out->control_peer_connected =
      atomic_load_explicit(&observe->control_peer_connected, memory_order_relaxed);
  out->control_peer_disconnected =
      atomic_load_explicit(&observe->control_peer_disconnected, memory_order_relaxed);
  out->control_reconnect_scheduled =
      atomic_load_explicit(&observe->control_reconnect_scheduled, memory_order_relaxed);
  out->control_reconnect_succeeded =
      atomic_load_explicit(&observe->control_reconnect_succeeded, memory_order_relaxed);
  out->control_reconnect_failed =
      atomic_load_explicit(&observe->control_reconnect_failed, memory_order_relaxed);
  out->control_heartbeat_timeout =
      atomic_load_explicit(&observe->control_heartbeat_timeout, memory_order_relaxed);
  out->control_frame_sent =
      atomic_load_explicit(&observe->control_frame_sent, memory_order_relaxed);
  out->control_hwm_reached =
      atomic_load_explicit(&observe->control_hwm_reached, memory_order_relaxed);
  out->control_frame_dropped =
      atomic_load_explicit(&observe->control_frame_dropped, memory_order_relaxed);
  out->control_errors = atomic_load_explicit(&observe->control_errors, memory_order_relaxed);
  out->control_last_status =
      atomic_load_explicit(&observe->control_last_status, memory_order_relaxed);
  out->control_last_value =
      atomic_load_explicit(&observe->control_last_value, memory_order_relaxed);
  return SALTS_OK;
}

int turbo_flow_observe_graph_snapshot(const turbo_flow_observe_t *observe,
                                      turbo_flow_observe_graph_snapshot_t *out) {
  salts_platform_cpu_info_t cpu;
  salts_platform_memory_info_t memory;
  salts_platform_load_average_t load;
  size_t pool_count;
  size_t resource_count;
  int rc;

  if (!observe || !out || !observe->flow) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  rc = turbo_flow_runtime_snapshot(observe->flow, &out->runtime);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_observe_snapshot(observe, &out->traffic);
  if (rc != SALTS_OK) return rc;

  out->connections_total = out->traffic.control_peer_connected;
  out->disconnections_total = out->traffic.control_peer_disconnected;
  out->connections_current = out->connections_total > out->disconnections_total
                                 ? out->connections_total - out->disconnections_total
                                 : 0u;

  for (size_t i = 0; i < out->runtime.adapter_count; ++i) {
    turbo_flow_connection_snapshot_t connection;
    if (turbo_flow_adapter_connection_snapshot_at(observe->flow, i, &connection) == SALTS_OK) {
      if (out->connection_providers == 0u) out->connections_current = 0u;
      ++out->connection_providers;
      out->connections_current += connection.connections_current;
    }
  }

  pool_count = turbo_flow_pool_count(observe->flow);
  for (size_t i = 0; i < pool_count; ++i) {
    turbo_flow_pool_snapshot_t pool;
    if (turbo_flow_pool_snapshot_at(observe->flow, i, &pool) == SALTS_OK &&
        turbo_flow_pool_saturated(&pool)) {
      ++out->saturated_pools;
    }
  }

  resource_count = turbo_flow_resource_count(observe->flow);
  for (size_t i = 0; i < resource_count; ++i) {
    turbo_flow_resource_snapshot_t resource = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    if (turbo_flow_resource_snapshot_at(observe->flow, i, &resource) != SALTS_OK) continue;
    ++out->resource_providers;
    if (resource.kind != TURBO_FLOW_RESOURCE_SEGMENT) {
      out->resource_load = flow_observe_add_saturated(out->resource_load, resource.load);
      out->resource_capacity =
          flow_observe_add_saturated(out->resource_capacity, resource.capacity);
    }
    if (resource.saturated) ++out->saturated_resources;
    switch (resource.kind) {
    case TURBO_FLOW_RESOURCE_CONNECTION:
      ++out->connection_resources;
      break;
    case TURBO_FLOW_RESOURCE_QUEUE_BUFFER:
      ++out->queue_buffer_resources;
      break;
    case TURBO_FLOW_RESOURCE_POOL:
      ++out->pool_resources;
      break;
    case TURBO_FLOW_RESOURCE_RUNTIME:
      ++out->runtime_resources;
      break;
    case TURBO_FLOW_RESOURCE_SEGMENT:
      ++out->segment_resources;
      break;
    case TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE:
      ++out->protocol_resources;
      break;
    case TURBO_FLOW_RESOURCE_STORAGE:
      ++out->storage_resources;
      break;
    case TURBO_FLOW_RESOURCE_RULE_SET:
      ++out->rule_set_resources;
      break;
    case TURBO_FLOW_RESOURCE_SECURITY_REALM:
      ++out->security_realm_resources;
      break;
    default:
      break;
    }
  }
  if (salts_platform_cpu_info(&cpu) == SALTS_OK) {
    out->system.cpu_cores = cpu.core_count;
    out->system.cpu_speed_mhz = cpu.speed_mhz;
  }
  if (salts_platform_memory_info(&memory) == SALTS_OK) {
    out->system.total_memory_bytes = memory.total_memory;
    out->system.available_memory_bytes = memory.available_memory;
  }
  if (salts_platform_load_average(&load) == SALTS_OK) {
    out->system.load_1m = load.one_minute;
    out->system.load_5m = load.five_minutes;
    out->system.load_15m = load.fifteen_minutes;
  }
  return SALTS_OK;
}

typedef enum flow_observe_control_fact_id_e {
  FLOW_OBSERVE_FACT_TRAFFIC_MESSAGES = 1,
  FLOW_OBSERVE_FACT_TRAFFIC_MESSAGE_ERRORS,
  FLOW_OBSERVE_FACT_TRAFFIC_MESSAGE_LATENCY_NS_TOTAL,
  FLOW_OBSERVE_FACT_TRAFFIC_MESSAGE_LATENCY_NS_MAX,
  FLOW_OBSERVE_FACT_TRAFFIC_STAGE_CALLS,
  FLOW_OBSERVE_FACT_TRAFFIC_STAGE_ERRORS,
  FLOW_OBSERVE_FACT_TRAFFIC_STAGE_LATENCY_NS_TOTAL,
  FLOW_OBSERVE_FACT_TRAFFIC_STAGE_LATENCY_NS_MAX,
  FLOW_OBSERVE_FACT_TRAFFIC_ADAPTER_STARTS,
  FLOW_OBSERVE_FACT_TRAFFIC_ADAPTER_STOPS,
  FLOW_OBSERVE_FACT_TRAFFIC_ADAPTER_ERRORS,
  FLOW_OBSERVE_FACT_TRAFFIC_DROPPED_STAGE_SERIES,
  FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_PEER_CONNECTED,
  FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_PEER_DISCONNECTED,
  FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_RECONNECT_SCHEDULED,
  FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_RECONNECT_SUCCEEDED,
  FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_RECONNECT_FAILED,
  FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_HEARTBEAT_TIMEOUT,
  FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_FRAME_SENT,
  FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_HWM_REACHED,
  FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_FRAME_DROPPED,
  FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_ERRORS,
  FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_LAST_STATUS,
  FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_LAST_VALUE,
  FLOW_OBSERVE_FACT_TRAFFIC_PAYLOAD_BYTES,
  FLOW_OBSERVE_FACT_SYSTEM_CPU_CORES,
  FLOW_OBSERVE_FACT_SYSTEM_CPU_SPEED_MHZ,
  FLOW_OBSERVE_FACT_SYSTEM_TOTAL_MEMORY_BYTES,
  FLOW_OBSERVE_FACT_SYSTEM_AVAILABLE_MEMORY_BYTES,
  FLOW_OBSERVE_FACT_SYSTEM_LOAD_1M,
  FLOW_OBSERVE_FACT_SYSTEM_LOAD_5M,
  FLOW_OBSERVE_FACT_SYSTEM_LOAD_15M,
  FLOW_OBSERVE_FACT_GRAPH_CONNECTIONS_CURRENT,
  FLOW_OBSERVE_FACT_GRAPH_CONNECTIONS_TOTAL,
  FLOW_OBSERVE_FACT_GRAPH_DISCONNECTIONS_TOTAL,
  FLOW_OBSERVE_FACT_GRAPH_SATURATED_POOLS,
  FLOW_OBSERVE_FACT_GRAPH_CONNECTION_PROVIDERS,
  FLOW_OBSERVE_FACT_GRAPH_RESOURCE_PROVIDERS,
  FLOW_OBSERVE_FACT_GRAPH_CONNECTION_RESOURCES,
  FLOW_OBSERVE_FACT_GRAPH_QUEUE_BUFFER_RESOURCES,
  FLOW_OBSERVE_FACT_GRAPH_POOL_RESOURCES,
  FLOW_OBSERVE_FACT_GRAPH_RUNTIME_RESOURCES,
  FLOW_OBSERVE_FACT_GRAPH_SEGMENT_RESOURCES,
  FLOW_OBSERVE_FACT_GRAPH_PROTOCOL_RESOURCES,
  FLOW_OBSERVE_FACT_GRAPH_STORAGE_RESOURCES,
  FLOW_OBSERVE_FACT_GRAPH_RULE_SET_RESOURCES,
  FLOW_OBSERVE_FACT_GRAPH_SATURATED_RESOURCES,
  FLOW_OBSERVE_FACT_GRAPH_RESOURCE_LOAD,
  FLOW_OBSERVE_FACT_GRAPH_RESOURCE_CAPACITY
} flow_observe_control_fact_id_t;

#define OBSERVE_I64(path, id) {path, TURBO_FLOW_EXPR_TYPE_I64, id}
#define OBSERVE_F64(path, id) {path, TURBO_FLOW_EXPR_TYPE_F64, id}
static const turbo_flow_expr_schema_field_t FLOW_OBSERVE_CONTROL_FIELDS[] = {
    OBSERVE_I64("traffic.messages", FLOW_OBSERVE_FACT_TRAFFIC_MESSAGES),
    OBSERVE_I64("traffic.message_errors", FLOW_OBSERVE_FACT_TRAFFIC_MESSAGE_ERRORS),
    OBSERVE_I64("traffic.message_latency_ns_total",
                FLOW_OBSERVE_FACT_TRAFFIC_MESSAGE_LATENCY_NS_TOTAL),
    OBSERVE_I64("traffic.message_latency_ns_max", FLOW_OBSERVE_FACT_TRAFFIC_MESSAGE_LATENCY_NS_MAX),
    OBSERVE_I64("traffic.stage_calls", FLOW_OBSERVE_FACT_TRAFFIC_STAGE_CALLS),
    OBSERVE_I64("traffic.stage_errors", FLOW_OBSERVE_FACT_TRAFFIC_STAGE_ERRORS),
    OBSERVE_I64("traffic.stage_latency_ns_total", FLOW_OBSERVE_FACT_TRAFFIC_STAGE_LATENCY_NS_TOTAL),
    OBSERVE_I64("traffic.stage_latency_ns_max", FLOW_OBSERVE_FACT_TRAFFIC_STAGE_LATENCY_NS_MAX),
    OBSERVE_I64("traffic.adapter_starts", FLOW_OBSERVE_FACT_TRAFFIC_ADAPTER_STARTS),
    OBSERVE_I64("traffic.adapter_stops", FLOW_OBSERVE_FACT_TRAFFIC_ADAPTER_STOPS),
    OBSERVE_I64("traffic.adapter_errors", FLOW_OBSERVE_FACT_TRAFFIC_ADAPTER_ERRORS),
    OBSERVE_I64("traffic.dropped_stage_series", FLOW_OBSERVE_FACT_TRAFFIC_DROPPED_STAGE_SERIES),
    OBSERVE_I64("traffic.control_peer_connected", FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_PEER_CONNECTED),
    OBSERVE_I64("traffic.control_peer_disconnected",
                FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_PEER_DISCONNECTED),
    OBSERVE_I64("traffic.control_reconnect_scheduled",
                FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_RECONNECT_SCHEDULED),
    OBSERVE_I64("traffic.control_reconnect_succeeded",
                FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_RECONNECT_SUCCEEDED),
    OBSERVE_I64("traffic.control_reconnect_failed",
                FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_RECONNECT_FAILED),
    OBSERVE_I64("traffic.control_heartbeat_timeout",
                FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_HEARTBEAT_TIMEOUT),
    OBSERVE_I64("traffic.control_frame_sent", FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_FRAME_SENT),
    OBSERVE_I64("traffic.control_hwm_reached", FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_HWM_REACHED),
    OBSERVE_I64("traffic.control_frame_dropped", FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_FRAME_DROPPED),
    OBSERVE_I64("traffic.control_errors", FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_ERRORS),
    OBSERVE_I64("traffic.control_last_status", FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_LAST_STATUS),
    OBSERVE_I64("traffic.control_last_value", FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_LAST_VALUE),
    OBSERVE_I64("traffic.payload_bytes", FLOW_OBSERVE_FACT_TRAFFIC_PAYLOAD_BYTES),
    OBSERVE_I64("system.cpu.cores", FLOW_OBSERVE_FACT_SYSTEM_CPU_CORES),
    OBSERVE_F64("system.cpu.speed_mhz", FLOW_OBSERVE_FACT_SYSTEM_CPU_SPEED_MHZ),
    OBSERVE_I64("system.memory.total_bytes", FLOW_OBSERVE_FACT_SYSTEM_TOTAL_MEMORY_BYTES),
    OBSERVE_I64("system.memory.available_bytes", FLOW_OBSERVE_FACT_SYSTEM_AVAILABLE_MEMORY_BYTES),
    OBSERVE_F64("system.load.one_minute", FLOW_OBSERVE_FACT_SYSTEM_LOAD_1M),
    OBSERVE_F64("system.load.five_minutes", FLOW_OBSERVE_FACT_SYSTEM_LOAD_5M),
    OBSERVE_F64("system.load.fifteen_minutes", FLOW_OBSERVE_FACT_SYSTEM_LOAD_15M),
    OBSERVE_I64("graph.connections_current", FLOW_OBSERVE_FACT_GRAPH_CONNECTIONS_CURRENT),
    OBSERVE_I64("graph.connections_total", FLOW_OBSERVE_FACT_GRAPH_CONNECTIONS_TOTAL),
    OBSERVE_I64("graph.disconnections_total", FLOW_OBSERVE_FACT_GRAPH_DISCONNECTIONS_TOTAL),
    OBSERVE_I64("graph.saturated_pools", FLOW_OBSERVE_FACT_GRAPH_SATURATED_POOLS),
    OBSERVE_I64("graph.connection_providers", FLOW_OBSERVE_FACT_GRAPH_CONNECTION_PROVIDERS),
    OBSERVE_I64("graph.resource_providers", FLOW_OBSERVE_FACT_GRAPH_RESOURCE_PROVIDERS),
    OBSERVE_I64("graph.connection_resources", FLOW_OBSERVE_FACT_GRAPH_CONNECTION_RESOURCES),
    OBSERVE_I64("graph.queue_buffer_resources", FLOW_OBSERVE_FACT_GRAPH_QUEUE_BUFFER_RESOURCES),
    OBSERVE_I64("graph.pool_resources", FLOW_OBSERVE_FACT_GRAPH_POOL_RESOURCES),
    OBSERVE_I64("graph.runtime_resources", FLOW_OBSERVE_FACT_GRAPH_RUNTIME_RESOURCES),
    OBSERVE_I64("graph.segment_resources", FLOW_OBSERVE_FACT_GRAPH_SEGMENT_RESOURCES),
    OBSERVE_I64("graph.protocol_resources", FLOW_OBSERVE_FACT_GRAPH_PROTOCOL_RESOURCES),
    OBSERVE_I64("graph.storage_resources", FLOW_OBSERVE_FACT_GRAPH_STORAGE_RESOURCES),
    OBSERVE_I64("graph.rule_set_resources", FLOW_OBSERVE_FACT_GRAPH_RULE_SET_RESOURCES),
    OBSERVE_I64("graph.saturated_resources", FLOW_OBSERVE_FACT_GRAPH_SATURATED_RESOURCES),
    OBSERVE_I64("graph.resource_load", FLOW_OBSERVE_FACT_GRAPH_RESOURCE_LOAD),
    OBSERVE_I64("graph.resource_capacity", FLOW_OBSERVE_FACT_GRAPH_RESOURCE_CAPACITY)};
#undef OBSERVE_I64
#undef OBSERVE_F64

static int flow_observe_fact_u64(turbo_flow_expr_value_t *out, uint64_t value) {
  if (value > INT64_MAX) return SALTS_ERANGE;
  out->type = TURBO_FLOW_EXPR_TYPE_I64;
  out->as.i64 = (int64_t)value;
  return SALTS_OK;
}

static int flow_observe_control_read(void *ctx, uint32_t field_id, turbo_flow_expr_value_t *out) {
  const turbo_flow_observe_control_facts_snapshot_t *snapshot =
      (const turbo_flow_observe_control_facts_snapshot_t *)ctx;
  const turbo_flow_observe_snapshot_t *traffic;
  const turbo_flow_observe_system_snapshot_t *system;
  if (!snapshot || !out) return SALTS_EINVAL;
  traffic = &snapshot->graph.traffic;
  system = &snapshot->graph.system;
  memset(out, 0, sizeof(*out));
#define U64_CASE(id, value)                                                                        \
  case id:                                                                                         \
    return flow_observe_fact_u64(out, value)
  switch (field_id) {
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_MESSAGES, traffic->messages);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_MESSAGE_ERRORS, traffic->message_errors);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_MESSAGE_LATENCY_NS_TOTAL, traffic->message_latency_ns_total);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_MESSAGE_LATENCY_NS_MAX, traffic->message_latency_ns_max);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_STAGE_CALLS, traffic->stage_calls);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_STAGE_ERRORS, traffic->stage_errors);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_STAGE_LATENCY_NS_TOTAL, traffic->stage_latency_ns_total);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_STAGE_LATENCY_NS_MAX, traffic->stage_latency_ns_max);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_ADAPTER_STARTS, traffic->adapter_starts);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_ADAPTER_STOPS, traffic->adapter_stops);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_ADAPTER_ERRORS, traffic->adapter_errors);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_DROPPED_STAGE_SERIES, traffic->dropped_stage_series);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_PEER_CONNECTED, traffic->control_peer_connected);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_PEER_DISCONNECTED,
             traffic->control_peer_disconnected);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_RECONNECT_SCHEDULED,
             traffic->control_reconnect_scheduled);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_RECONNECT_SUCCEEDED,
             traffic->control_reconnect_succeeded);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_RECONNECT_FAILED, traffic->control_reconnect_failed);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_HEARTBEAT_TIMEOUT,
             traffic->control_heartbeat_timeout);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_FRAME_SENT, traffic->control_frame_sent);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_HWM_REACHED, traffic->control_hwm_reached);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_FRAME_DROPPED, traffic->control_frame_dropped);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_ERRORS, traffic->control_errors);
  case FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_LAST_STATUS:
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = traffic->control_last_status;
    return SALTS_OK;
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_CONTROL_LAST_VALUE, traffic->control_last_value);
    U64_CASE(FLOW_OBSERVE_FACT_TRAFFIC_PAYLOAD_BYTES, traffic->payload_bytes);
  case FLOW_OBSERVE_FACT_SYSTEM_CPU_CORES:
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = system->cpu_cores;
    return SALTS_OK;
  case FLOW_OBSERVE_FACT_SYSTEM_CPU_SPEED_MHZ:
    out->type = TURBO_FLOW_EXPR_TYPE_F64;
    out->as.f64 = system->cpu_speed_mhz;
    return SALTS_OK;
    U64_CASE(FLOW_OBSERVE_FACT_SYSTEM_TOTAL_MEMORY_BYTES, system->total_memory_bytes);
    U64_CASE(FLOW_OBSERVE_FACT_SYSTEM_AVAILABLE_MEMORY_BYTES, system->available_memory_bytes);
  case FLOW_OBSERVE_FACT_SYSTEM_LOAD_1M:
    out->type = TURBO_FLOW_EXPR_TYPE_F64;
    out->as.f64 = system->load_1m;
    return SALTS_OK;
  case FLOW_OBSERVE_FACT_SYSTEM_LOAD_5M:
    out->type = TURBO_FLOW_EXPR_TYPE_F64;
    out->as.f64 = system->load_5m;
    return SALTS_OK;
  case FLOW_OBSERVE_FACT_SYSTEM_LOAD_15M:
    out->type = TURBO_FLOW_EXPR_TYPE_F64;
    out->as.f64 = system->load_15m;
    return SALTS_OK;
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_CONNECTIONS_CURRENT, snapshot->graph.connections_current);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_CONNECTIONS_TOTAL, snapshot->graph.connections_total);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_DISCONNECTIONS_TOTAL, snapshot->graph.disconnections_total);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_SATURATED_POOLS, snapshot->graph.saturated_pools);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_CONNECTION_PROVIDERS, snapshot->graph.connection_providers);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_RESOURCE_PROVIDERS, snapshot->graph.resource_providers);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_CONNECTION_RESOURCES, snapshot->graph.connection_resources);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_QUEUE_BUFFER_RESOURCES,
             snapshot->graph.queue_buffer_resources);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_POOL_RESOURCES, snapshot->graph.pool_resources);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_RUNTIME_RESOURCES, snapshot->graph.runtime_resources);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_SEGMENT_RESOURCES, snapshot->graph.segment_resources);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_PROTOCOL_RESOURCES, snapshot->graph.protocol_resources);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_STORAGE_RESOURCES, snapshot->graph.storage_resources);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_RULE_SET_RESOURCES, snapshot->graph.rule_set_resources);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_SATURATED_RESOURCES, snapshot->graph.saturated_resources);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_RESOURCE_LOAD, snapshot->graph.resource_load);
    U64_CASE(FLOW_OBSERVE_FACT_GRAPH_RESOURCE_CAPACITY, snapshot->graph.resource_capacity);
  default:
    return SALTS_ENOENT;
  }
#undef U64_CASE
}

int turbo_flow_observe_control_facts(const turbo_flow_observe_t *observe,
                                     turbo_flow_observe_control_facts_snapshot_t *snapshot,
                                     turbo_flow_control_facts_t *facts) {
  static const turbo_flow_expr_schema_t schema = {FLOW_OBSERVE_CONTROL_FIELDS,
                                                  sizeof(FLOW_OBSERVE_CONTROL_FIELDS) /
                                                      sizeof(FLOW_OBSERVE_CONTROL_FIELDS[0])};
  int rc;
  if (!snapshot || !facts) return SALTS_EINVAL;
  memset(snapshot, 0, sizeof(*snapshot));
  memset(facts, 0, sizeof(*facts));
  rc = turbo_flow_observe_graph_snapshot(observe, &snapshot->graph);
  if (rc != SALTS_OK) return rc;
  facts->size = sizeof(*facts);
  facts->schema = &schema;
  facts->read_field = flow_observe_control_read;
  facts->ctx = snapshot;
  return SALTS_OK;
}

static uint32_t flow_observe_ratio_bps(uint64_t value, uint64_t capacity) {
  double bps;
  if (capacity == 0u) return 0u;
  if (value >= capacity) return TURBO_FLOW_OBSERVE_UTILIZATION_BPS;
  bps = (double)value * (double)TURBO_FLOW_OBSERVE_UTILIZATION_BPS / (double)capacity;
  return bps >= (double)TURBO_FLOW_OBSERVE_UTILIZATION_BPS ? TURBO_FLOW_OBSERVE_UTILIZATION_BPS
                                                           : (uint32_t)bps;
}

static int flow_observe_find_pool(const turbo_flow_observe_t *observe,
                                  const turbo_flow_observe_pool_policy_t *policy,
                                  turbo_flow_pool_resource_status_t *out) {
  size_t count = turbo_flow_pool_count(observe->flow);
  for (size_t i = 0; i < count; ++i) {
    turbo_flow_pool_resource_status_t status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
    if (turbo_flow_pool_resource_status_at(observe->flow, i, &status) == SALTS_OK &&
        status.snapshot.kind == policy->kind &&
        strcmp(status.owner_name, policy->stage_name) == 0) {
      *out = status;
      return SALTS_OK;
    }
  }
  return SALTS_ENOENT;
}

int turbo_flow_observe_reconcile_pool(turbo_flow_observe_t *observe,
                                      const turbo_flow_observe_pool_policy_t *policy,
                                      turbo_flow_observe_reconcile_state_t *state, uint64_t now_ns,
                                      turbo_flow_observe_reconcile_result_t *out) {
  turbo_flow_observe_graph_snapshot_t graph;
  turbo_flow_pool_resource_status_t pool_status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
  turbo_flow_pool_snapshot_t *pool;
  turbo_flow_resource_reconcile_request_t request = TURBO_FLOW_RESOURCE_RECONCILE_REQUEST_INIT;
  turbo_flow_resource_reconcile_result_t reconcile = TURBO_FLOW_RESOURCE_RECONCILE_RESULT_INIT;
  uint64_t capacity;
  uint64_t load;
  uint64_t cooldown_ns;
  uint64_t elapsed_ns;
  uint32_t desired;
  int upscale = 0;
  int rc;
  int written;

  if (!observe || !observe->flow || !policy || !state || !out || policy->size < sizeof(*policy) ||
      !policy->stage_name || policy->min_parallelism == 0u ||
      policy->max_parallelism < policy->min_parallelism || policy->scale_up_step == 0u ||
      policy->scale_down_step == 0u ||
      policy->low_utilization_bps >= policy->high_utilization_bps ||
      policy->high_utilization_bps > TURBO_FLOW_OBSERVE_UTILIZATION_BPS ||
      policy->high_observations == 0u || policy->low_observations == 0u) {
    return SALTS_EINVAL;
  }
  memset(out, 0, sizeof(*out));
  rc = turbo_flow_observe_graph_snapshot(observe, &graph);
  if (rc != SALTS_OK) return rc;
  if (graph.runtime.state != TURBO_FLOW_STATE_STARTED) return SALTS_EINVAL;
  rc = flow_observe_find_pool(observe, policy, &pool_status);
  if (rc != SALTS_OK) return rc;
  pool = &pool_status.snapshot;

  capacity = pool->resource_capacity;
  if (capacity == 0u) {
    capacity = pool->queue_capacity > UINT64_MAX - pool->parallelism
                   ? UINT64_MAX
                   : pool->queue_capacity + pool->parallelism;
  }
  load = pool->active > UINT64_MAX - pool->queued ? UINT64_MAX : pool->active + pool->queued;
  out->utilization_bps = flow_observe_ratio_bps(load, capacity);
  if (graph.system.cpu_cores > 0 && graph.system.load_1m > 0.0) {
    double normalized = graph.system.load_1m / (double)graph.system.cpu_cores *
                        (double)TURBO_FLOW_OBSERVE_UTILIZATION_BPS;
    out->system_load_per_core_bps =
        normalized >= (double)UINT32_MAX ? UINT32_MAX : (uint32_t)normalized;
  }
  out->previous_parallelism = pool->parallelism;
  out->desired_parallelism = pool->parallelism;
  out->command_status = SALTS_OK;

  desired = pool->parallelism;
  if (desired < policy->min_parallelism) {
    desired = policy->min_parallelism;
    upscale = 1;
  } else if (desired > policy->max_parallelism) {
    desired = policy->max_parallelism;
  } else if (out->utilization_bps >= policy->high_utilization_bps) {
    state->low_observations = 0u;
    if (state->high_observations < UINT32_MAX) ++state->high_observations;
    if (state->high_observations < policy->high_observations) {
      out->action = TURBO_FLOW_OBSERVE_RECONCILE_HYSTERESIS;
      return SALTS_OK;
    }
    desired = policy->scale_up_step > policy->max_parallelism - pool->parallelism
                  ? policy->max_parallelism
                  : pool->parallelism + policy->scale_up_step;
    upscale = desired > pool->parallelism;
  } else if (out->utilization_bps <= policy->low_utilization_bps) {
    state->high_observations = 0u;
    if (state->low_observations < UINT32_MAX) ++state->low_observations;
    if (state->low_observations < policy->low_observations) {
      out->action = TURBO_FLOW_OBSERVE_RECONCILE_HYSTERESIS;
      return SALTS_OK;
    }
    desired = pool->parallelism > policy->scale_down_step
                  ? pool->parallelism - policy->scale_down_step
                  : policy->min_parallelism;
    if (desired < policy->min_parallelism) desired = policy->min_parallelism;
  } else {
    state->high_observations = 0u;
    state->low_observations = 0u;
    out->action = TURBO_FLOW_OBSERVE_RECONCILE_STABLE;
    return SALTS_OK;
  }

  out->desired_parallelism = desired;
  if (desired == pool->parallelism) {
    state->high_observations = 0u;
    state->low_observations = 0u;
    out->action = TURBO_FLOW_OBSERVE_RECONCILE_STABLE;
    return SALTS_OK;
  }
  cooldown_ns = policy->cooldown_ms > UINT64_MAX / UINT64_C(1000000)
                    ? UINT64_MAX
                    : policy->cooldown_ms * UINT64_C(1000000);
  elapsed_ns = now_ns >= state->last_resize_ns ? now_ns - state->last_resize_ns : 0u;
  if (state->last_resize_ns != 0u && elapsed_ns < cooldown_ns) {
    out->action = TURBO_FLOW_OBSERVE_RECONCILE_COOLDOWN;
    return SALTS_OK;
  }
  if (upscale && policy->max_system_load_per_core_bps > 0u &&
      out->system_load_per_core_bps >= policy->max_system_load_per_core_bps) {
    out->action = TURBO_FLOW_OBSERVE_RECONCILE_SYSTEM_LIMIT;
    return SALTS_OK;
  }

  request.metadata.domain = TURBO_FLOW_DOMAIN_EXECUTION;
  request.metadata.kind = TURBO_FLOW_RESOURCE_POOL;
  request.metadata.generation = pool_status.generation;
  request.metadata.observed_generation = pool_status.observed_generation;
  written = snprintf(request.metadata.uid, sizeof(request.metadata.uid), "%s", pool_status.uid);
  if (written < 0 || (size_t)written >= sizeof(request.metadata.uid)) return SALTS_ENAMETOOLONG;
  written = snprintf(request.metadata.owner_name, sizeof(request.metadata.owner_name), "%s",
                     pool_status.owner_name);
  if (written < 0 || (size_t)written >= sizeof(request.metadata.owner_name))
    return SALTS_ENAMETOOLONG;
  request.observed_value = pool->parallelism;
  request.desired_value = desired;
  request.command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL;
  written = snprintf(request.command.target_uid, sizeof(request.command.target_uid), "%s",
                     pool_status.uid);
  if (written < 0 || (size_t)written >= sizeof(request.command.target_uid))
    return SALTS_ENAMETOOLONG;
  written = snprintf(request.command.idempotency_key, sizeof(request.command.idempotency_key),
                     "reconcile:%s:%llu:%u:%llu", pool_status.uid,
                     (unsigned long long)pool_status.observed_generation, (unsigned)desired,
                     (unsigned long long)now_ns);
  if (written < 0 || (size_t)written >= sizeof(request.command.idempotency_key))
    return SALTS_ENAMETOOLONG;
  request.command.parallelism = desired;
  request.command.drain_timeout_ms = policy->drain_timeout_ms;
  request.command.deadline_ns = UINT64_MAX;
  rc = turbo_flow_resource_reconcile_tick(observe->flow, &request, &reconcile);
  out->command_status = rc;
  if (rc != SALTS_OK) return rc;
  if (reconcile.action != TURBO_FLOW_RESOURCE_RECONCILE_COMMAND_APPLIED) return SALTS_EPROTO;
  state->last_resize_ns = now_ns ? now_ns : 1u;
  state->high_observations = 0u;
  state->low_observations = 0u;
  out->action = TURBO_FLOW_OBSERVE_RECONCILE_RESIZED;
  return SALTS_OK;
}

int turbo_flow_observe_record_control_event(turbo_flow_observe_t *observe,
                                            const turbo_flow_observe_control_event_t *event) {
  atomic_uint_fast64_t *counter = NULL;
  if (!observe || !event) return SALTS_EINVAL;
  switch (event->kind) {
  case TURBO_FLOW_OBSERVE_CONTROL_PEER_CONNECTED:
    counter = &observe->control_peer_connected;
    break;
  case TURBO_FLOW_OBSERVE_CONTROL_PEER_DISCONNECTED:
    counter = &observe->control_peer_disconnected;
    break;
  case TURBO_FLOW_OBSERVE_CONTROL_RECONNECT_SCHEDULED:
    counter = &observe->control_reconnect_scheduled;
    break;
  case TURBO_FLOW_OBSERVE_CONTROL_RECONNECT_SUCCEEDED:
    counter = &observe->control_reconnect_succeeded;
    break;
  case TURBO_FLOW_OBSERVE_CONTROL_RECONNECT_FAILED:
    counter = &observe->control_reconnect_failed;
    break;
  case TURBO_FLOW_OBSERVE_CONTROL_HEARTBEAT_TIMEOUT:
    counter = &observe->control_heartbeat_timeout;
    break;
  case TURBO_FLOW_OBSERVE_CONTROL_FRAME_SENT:
    counter = &observe->control_frame_sent;
    break;
  case TURBO_FLOW_OBSERVE_CONTROL_HWM_REACHED:
    counter = &observe->control_hwm_reached;
    break;
  case TURBO_FLOW_OBSERVE_CONTROL_FRAME_DROPPED:
    counter = &observe->control_frame_dropped;
    break;
  default:
    return SALTS_EINVAL;
  }
  atomic_fetch_add_explicit(counter, 1, memory_order_relaxed);
  if (event->status != SALTS_OK) {
    atomic_fetch_add_explicit(&observe->control_errors, 1, memory_order_relaxed);
  }
  atomic_store_explicit(&observe->control_last_status, event->status, memory_order_relaxed);
  atomic_store_explicit(&observe->control_last_value, event->value, memory_order_relaxed);
  {
    turbo_flow_observe_event_record_t record;
    memset(&record, 0, sizeof(record));
    record.timestamp_ns = salts_hrtime();
    record.event = *event;
    salts_mutex_lock(&observe->export_mutex);
    record.sequence = ++observe->next_event_sequence;
    if (deque_size(&observe->events) == observe->max_events) {
      turbo_flow_observe_event_record_t discarded;
      if (turbo_flow_stl_error(deque_pop_front(&observe->events, &discarded)) == SALTS_OK) {
        ++observe->dropped_events;
      }
    }
    if (turbo_flow_stl_error(deque_push_back(&observe->events, &record)) != SALTS_OK) {
      salts_mutex_unlock(&observe->export_mutex);
      return SALTS_ENOMEM;
    }
    salts_mutex_unlock(&observe->export_mutex);
  }
  return SALTS_OK;
}

size_t turbo_flow_observe_event_count(const turbo_flow_observe_t *observe) {
  size_t count;
  if (!observe) return 0u;
  salts_mutex_lock((salts_mutex_t *)&observe->export_mutex);
  count = deque_size(&observe->events);
  salts_mutex_unlock((salts_mutex_t *)&observe->export_mutex);
  return count;
}

uint64_t turbo_flow_observe_dropped_event_count(const turbo_flow_observe_t *observe) {
  uint64_t count;
  if (!observe) return 0u;
  salts_mutex_lock((salts_mutex_t *)&observe->export_mutex);
  count = observe->dropped_events;
  salts_mutex_unlock((salts_mutex_t *)&observe->export_mutex);
  return count;
}

int turbo_flow_observe_event_at(const turbo_flow_observe_t *observe, size_t index,
                                turbo_flow_observe_event_record_t *out) {
  const turbo_flow_observe_event_record_t *event;
  if (!observe || !out) return SALTS_EINVAL;
  salts_mutex_lock((salts_mutex_t *)&observe->export_mutex);
  event = (const turbo_flow_observe_event_record_t *)deque_at_const(&observe->events, index);
  if (!event) {
    salts_mutex_unlock((salts_mutex_t *)&observe->export_mutex);
    return SALTS_ENOENT;
  }
  *out = *event;
  salts_mutex_unlock((salts_mutex_t *)&observe->export_mutex);
  return SALTS_OK;
}

int turbo_flow_observe_record_command_result(turbo_flow_observe_t *observe, const char *target_uid,
                                             const turbo_flow_resource_command_result_t *result) {
  size_t length;
  if (!observe || !target_uid || !*target_uid || !result || result->size < sizeof(*result))
    return SALTS_EINVAL;
  length = strlen(target_uid);
  if (length > TURBO_FLOW_RESOURCE_UID_MAX) return SALTS_ENAMETOOLONG;
  salts_mutex_lock(&observe->export_mutex);
  memcpy(observe->command_target_uid, target_uid, length + 1u);
  observe->command_result = *result;
  observe->command_result.size = sizeof(observe->command_result);
  observe->has_command_result = 1;
  salts_mutex_unlock(&observe->export_mutex);
  return SALTS_OK;
}

int turbo_flow_observe_last_command_result(const turbo_flow_observe_t *observe,
                                           char target_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u],
                                           turbo_flow_resource_command_result_t *out) {
  if (!observe || !target_uid || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  salts_mutex_lock((salts_mutex_t *)&observe->export_mutex);
  if (!observe->has_command_result) {
    salts_mutex_unlock((salts_mutex_t *)&observe->export_mutex);
    return SALTS_ENOENT;
  }
  memcpy(target_uid, observe->command_target_uid, sizeof(observe->command_target_uid));
  *out = observe->command_result;
  salts_mutex_unlock((salts_mutex_t *)&observe->export_mutex);
  return SALTS_OK;
}

size_t turbo_flow_observe_resource_count(const turbo_flow_observe_t *observe) {
  return observe && observe->flow ? turbo_flow_resource_count(observe->flow) : 0u;
}

int turbo_flow_observe_resource_at(const turbo_flow_observe_t *observe, size_t index,
                                   turbo_flow_observe_resource_view_t *out) {
  int accepting;
  if (!observe || !observe->flow || !out) return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  out->snapshot = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  if (turbo_flow_resource_snapshot_at(observe->flow, index, &out->snapshot) != SALTS_OK)
    return SALTS_ENOENT;
  accepting = !out->snapshot.saturated;
  if (out->snapshot.kind == TURBO_FLOW_RESOURCE_RUNTIME) {
    turbo_flow_runtime_snapshot_t runtime;
    int rc = turbo_flow_runtime_snapshot(observe->flow, &runtime);
    if (rc != SALTS_OK) return rc;
    accepting = runtime.accepting_publishes;
  }
  out->condition_count = TURBO_FLOW_RESOURCE_CONDITION_MAX;
  out->conditions[0] = (turbo_flow_resource_condition_t){
      TURBO_FLOW_RESOURCE_CONDITION_READY,
      out->snapshot.last_status == SALTS_OK ? TURBO_FLOW_CONDITION_TRUE
                                            : TURBO_FLOW_CONDITION_FALSE,
      out->snapshot.last_status == SALTS_OK ? TURBO_FLOW_RESOURCE_REASON_RUNNING
                                            : TURBO_FLOW_RESOURCE_REASON_NOT_RUNNING};
  out->conditions[1] = (turbo_flow_resource_condition_t){
      TURBO_FLOW_RESOURCE_CONDITION_ACCEPTING,
      accepting ? TURBO_FLOW_CONDITION_TRUE : TURBO_FLOW_CONDITION_FALSE,
      accepting ? TURBO_FLOW_RESOURCE_REASON_ACCEPTING : TURBO_FLOW_RESOURCE_REASON_NOT_ACCEPTING};
  out->conditions[2] = (turbo_flow_resource_condition_t){
      TURBO_FLOW_RESOURCE_CONDITION_DRAINED,
      out->snapshot.load == 0u ? TURBO_FLOW_CONDITION_TRUE : TURBO_FLOW_CONDITION_FALSE,
      out->snapshot.load == 0u ? TURBO_FLOW_RESOURCE_REASON_DRAINED
                               : TURBO_FLOW_RESOURCE_REASON_WORK_PENDING};
  out->conditions[3] = (turbo_flow_resource_condition_t){
      TURBO_FLOW_RESOURCE_CONDITION_SATURATED,
      out->snapshot.saturated ? TURBO_FLOW_CONDITION_TRUE : TURBO_FLOW_CONDITION_FALSE,
      out->snapshot.saturated ? TURBO_FLOW_RESOURCE_REASON_CAPACITY_EXHAUSTED
                              : TURBO_FLOW_RESOURCE_REASON_CAPACITY_AVAILABLE};
  return SALTS_OK;
}

size_t turbo_flow_observe_stage_count(const turbo_flow_observe_t *observe) {
  size_t count;
  if (!observe) return 0;
  salts_mutex_lock((salts_mutex_t *)&observe->stage_mutex);
  count = vec_size(&observe->stages);
  salts_mutex_unlock((salts_mutex_t *)&observe->stage_mutex);
  return count;
}

int turbo_flow_observe_stage_snapshot_at(const turbo_flow_observe_t *observe, size_t index,
                                         turbo_flow_observe_stage_snapshot_t *out) {
  const flow_observe_stage_entry_t *entry;
  size_t name_len;
  if (!observe || !out) return SALTS_EINVAL;
  salts_mutex_lock((salts_mutex_t *)&observe->stage_mutex);
  entry = (const flow_observe_stage_entry_t *)vec_at_const(&observe->stages, index);
  if (!entry) {
    salts_mutex_unlock((salts_mutex_t *)&observe->stage_mutex);
    return SALTS_ENOENT;
  }
  memset(out, 0, sizeof(*out));
  name_len = tstr_len(entry->name);
  if (name_len > TURBO_FLOW_OBSERVE_MAX_STAGE_NAME) {
    name_len = TURBO_FLOW_OBSERVE_MAX_STAGE_NAME;
  }
  memcpy(out->name, entry->name, name_len);
  out->calls = entry->calls;
  out->errors = entry->errors;
  out->latency_ns_total = entry->latency_ns_total;
  out->latency_ns_max = entry->latency_ns_max;
  salts_mutex_unlock((salts_mutex_t *)&observe->stage_mutex);
  return SALTS_OK;
}

static int flow_observe_log_consume(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  static const char HEX[] = "0123456789abcdef";
  flow_observe_log_sink_t *sink = (flow_observe_log_sink_t *)ctx;
  turbo_flow_observe_log_summary_t summary;
  char preview[TURBO_FLOW_OBSERVE_MAX_PREVIEW_BYTES * 2u + 1u];
  size_t preview_bytes = 0;
  (void)flow;
  if (!sink || !stage || !msg || !sink->write || (msg->payload.len > 0 && !msg->payload.data))
    return SALTS_EINVAL;
  memset(&summary, 0, sizeof(summary));
  preview[0] = '\0';
  if (sink->include_payload_preview) {
    preview_bytes =
        msg->payload.len < sink->max_preview_bytes ? msg->payload.len : sink->max_preview_bytes;
    for (size_t i = 0; i < preview_bytes; ++i) {
      unsigned char byte = (unsigned char)msg->payload.data[i];
      preview[i * 2u] = HEX[byte >> 4];
      preview[i * 2u + 1u] = HEX[byte & 0x0fu];
    }
    preview[preview_bytes * 2u] = '\0';
  }
  summary.stage_name = stage->name;
  summary.message_id = msg->id;
  summary.status = msg->status;
  summary.payload_size = msg->payload.len;
  summary.payload_preview = preview;
  summary.preview_bytes = preview_bytes;
  summary.payload_redacted = sink->include_payload_preview ? 0 : 1;
  sink->write(sink->write_ctx, &summary);
  return SALTS_OK;
}

static void flow_observe_log_shutdown(void *ctx) { free(ctx); }

int turbo_flow_observe_register_log_sink(turbo_flow_t *flow, const char *name,
                                         const turbo_flow_observe_log_config_t *config) {
  flow_observe_log_sink_t *sink;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_adapter_schema_t schema;
  int rc;
  if (!flow || !name || name[0] == '\0' || !config || !config->write ||
      config->max_preview_bytes > TURBO_FLOW_OBSERVE_MAX_PREVIEW_BYTES) {
    return SALTS_EINVAL;
  }
  sink = (flow_observe_log_sink_t *)calloc(1, sizeof(*sink));
  if (!sink) return SALTS_ENOMEM;
  sink->max_preview_bytes = config->max_preview_bytes;
  sink->include_payload_preview = config->include_payload_preview ? 1 : 0;
  sink->write = config->write;
  sink->write_ctx = config->write_ctx;
  memset(&ops, 0, sizeof(ops));
  ops.consume = flow_observe_log_consume;
  ops.shutdown = flow_observe_log_shutdown;
  memset(&schema, 0, sizeof(schema));
  schema.kind = TURBO_FLOW_ADAPTER_KIND_OBSERVE;
  schema.roles = TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
  schema.fields = FLOW_OBSERVE_LOG_FIELDS;
  schema.field_count = sizeof(FLOW_OBSERVE_LOG_FIELDS) / sizeof(FLOW_OBSERVE_LOG_FIELDS[0]);
  rc = turbo_flow_register_adapter_ex(flow, name, &ops, sink, &schema);
  if (rc != SALTS_OK) free(sink);
  return rc;
}
