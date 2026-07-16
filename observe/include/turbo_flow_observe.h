#ifndef TURBO_FLOW_OBSERVE_H
#define TURBO_FLOW_OBSERVE_H

#include "turbo_flow.h"
#include "turbo_flow_control.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_OBSERVE_DEFAULT_MAX_STAGES 256u
#define TURBO_FLOW_OBSERVE_MAX_STAGES 4096u
#define TURBO_FLOW_OBSERVE_MAX_STAGE_NAME 127u
#define TURBO_FLOW_OBSERVE_MAX_PREVIEW_BYTES 256u
#define TURBO_FLOW_OBSERVE_DEFAULT_MAX_EVENTS 256u
#define TURBO_FLOW_OBSERVE_MAX_EVENTS 4096u

typedef struct turbo_flow_observe_s turbo_flow_observe_t;

typedef struct turbo_flow_observe_config_s {
  size_t max_stages;
  size_t max_events;
} turbo_flow_observe_config_t;

typedef struct turbo_flow_observe_snapshot_s {
  uint64_t messages;
  uint64_t message_errors;
  uint64_t message_latency_ns_total;
  uint64_t message_latency_ns_max;
  uint64_t stage_calls;
  uint64_t stage_errors;
  uint64_t stage_latency_ns_total;
  uint64_t stage_latency_ns_max;
  uint64_t adapter_starts;
  uint64_t adapter_stops;
  uint64_t adapter_errors;
  uint64_t dropped_stage_series;
  uint64_t control_peer_connected;
  uint64_t control_peer_disconnected;
  uint64_t control_reconnect_scheduled;
  uint64_t control_reconnect_succeeded;
  uint64_t control_reconnect_failed;
  uint64_t control_heartbeat_timeout;
  uint64_t control_frame_sent;
  uint64_t control_hwm_reached;
  uint64_t control_frame_dropped;
  uint64_t control_errors;
  int control_last_status;
  uint64_t control_last_value;
  uint64_t payload_bytes;
} turbo_flow_observe_snapshot_t;

typedef struct turbo_flow_observe_system_snapshot_s {
  int cpu_cores;
  double cpu_speed_mhz;
  uint64_t total_memory_bytes;
  uint64_t available_memory_bytes;
  double load_1m;
  double load_5m;
  double load_15m;
} turbo_flow_observe_system_snapshot_t;

typedef struct turbo_flow_observe_graph_snapshot_s {
  turbo_flow_runtime_snapshot_t runtime;
  turbo_flow_observe_snapshot_t traffic;
  turbo_flow_observe_system_snapshot_t system;
  uint64_t connections_current;
  uint64_t connections_total;
  uint64_t disconnections_total;
  uint64_t saturated_pools;
  uint64_t connection_providers;
  uint64_t resource_providers;
  uint64_t connection_resources;
  uint64_t queue_buffer_resources;
  uint64_t pool_resources;
  uint64_t runtime_resources;
  uint64_t segment_resources;
  uint64_t protocol_resources;
  uint64_t storage_resources;
  uint64_t rule_set_resources;
  uint64_t security_realm_resources;
  uint64_t saturated_resources;
  uint64_t resource_load;
  uint64_t resource_capacity;
} turbo_flow_observe_graph_snapshot_t;

/** Caller-owned backing storage for one immutable control-fact evaluation. */
typedef struct turbo_flow_observe_control_facts_snapshot_s {
  turbo_flow_observe_graph_snapshot_t graph;
} turbo_flow_observe_control_facts_snapshot_t;

#define TURBO_FLOW_OBSERVE_UTILIZATION_BPS 10000u

typedef struct turbo_flow_observe_pool_policy_s {
  size_t size;
  const char *stage_name;
  turbo_flow_pool_kind_t kind;
  uint32_t min_parallelism;
  uint32_t max_parallelism;
  uint32_t scale_up_step;
  uint32_t scale_down_step;
  uint32_t high_utilization_bps;
  uint32_t low_utilization_bps;
  uint32_t high_observations;
  uint32_t low_observations;
  uint64_t cooldown_ms;
  uint64_t drain_timeout_ms;
  /** 10000 means a 1m load of 1.0 per CPU core; zero disables the guard. */
  uint32_t max_system_load_per_core_bps;
} turbo_flow_observe_pool_policy_t;

typedef struct turbo_flow_observe_reconcile_state_s {
  uint64_t last_resize_ns;
  uint32_t high_observations;
  uint32_t low_observations;
} turbo_flow_observe_reconcile_state_t;

typedef enum turbo_flow_observe_reconcile_action_e {
  TURBO_FLOW_OBSERVE_RECONCILE_STABLE = 0,
  TURBO_FLOW_OBSERVE_RECONCILE_HYSTERESIS,
  TURBO_FLOW_OBSERVE_RECONCILE_COOLDOWN,
  TURBO_FLOW_OBSERVE_RECONCILE_SYSTEM_LIMIT,
  TURBO_FLOW_OBSERVE_RECONCILE_RESIZED
} turbo_flow_observe_reconcile_action_t;

typedef struct turbo_flow_observe_reconcile_result_s {
  turbo_flow_observe_reconcile_action_t action;
  uint32_t previous_parallelism;
  uint32_t desired_parallelism;
  uint32_t utilization_bps;
  uint32_t system_load_per_core_bps;
  int command_status;
} turbo_flow_observe_reconcile_result_t;

typedef enum turbo_flow_observe_control_event_kind_e {
  TURBO_FLOW_OBSERVE_CONTROL_PEER_CONNECTED = 1,
  TURBO_FLOW_OBSERVE_CONTROL_PEER_DISCONNECTED,
  TURBO_FLOW_OBSERVE_CONTROL_RECONNECT_SCHEDULED,
  TURBO_FLOW_OBSERVE_CONTROL_RECONNECT_SUCCEEDED,
  TURBO_FLOW_OBSERVE_CONTROL_RECONNECT_FAILED,
  TURBO_FLOW_OBSERVE_CONTROL_HEARTBEAT_TIMEOUT,
  TURBO_FLOW_OBSERVE_CONTROL_FRAME_SENT,
  TURBO_FLOW_OBSERVE_CONTROL_HWM_REACHED,
  TURBO_FLOW_OBSERVE_CONTROL_FRAME_DROPPED
} turbo_flow_observe_control_event_kind_t;

typedef struct turbo_flow_observe_control_event_s {
  turbo_flow_observe_control_event_kind_t kind;
  int status;
  uint64_t value;
} turbo_flow_observe_control_event_t;

typedef struct turbo_flow_observe_event_record_s {
  uint64_t sequence;
  uint64_t timestamp_ns;
  turbo_flow_observe_control_event_t event;
} turbo_flow_observe_event_record_t;

typedef struct turbo_flow_observe_resource_view_s {
  turbo_flow_resource_snapshot_t snapshot;
  turbo_flow_resource_condition_t conditions[TURBO_FLOW_RESOURCE_CONDITION_MAX];
  uint32_t condition_count;
} turbo_flow_observe_resource_view_t;

typedef int (*turbo_flow_observe_write_fn)(void *ctx, const char *data, size_t len);

typedef struct turbo_flow_observe_metric_s {
  const char *name;
  double value;
  const char *resource_uid;
  const char *owner_name;
  turbo_flow_resource_kind_t resource_kind;
} turbo_flow_observe_metric_t;

typedef int (*turbo_flow_observe_metric_fn)(void *ctx, const turbo_flow_observe_metric_t *metric);

typedef struct turbo_flow_observe_stage_snapshot_s {
  char name[TURBO_FLOW_OBSERVE_MAX_STAGE_NAME + 1u];
  uint64_t calls;
  uint64_t errors;
  uint64_t latency_ns_total;
  uint64_t latency_ns_max;
} turbo_flow_observe_stage_snapshot_t;

typedef struct turbo_flow_observe_log_summary_s {
  const char *stage_name;
  uint64_t message_id;
  int status;
  size_t payload_size;
  /** Lowercase hexadecimal; empty when payload preview is disabled. */
  const char *payload_preview;
  size_t preview_bytes;
  int payload_redacted;
} turbo_flow_observe_log_summary_t;

typedef void (*turbo_flow_observe_log_fn)(void *ctx,
                                          const turbo_flow_observe_log_summary_t *summary);

typedef struct turbo_flow_observe_log_config_s {
  size_t max_preview_bytes;
  int include_payload_preview;
  turbo_flow_observe_log_fn write;
  void *write_ctx;
} turbo_flow_observe_log_config_t;

CXX_C_API turbo_flow_observe_t *
turbo_flow_observe_create(const turbo_flow_observe_config_t *config);

/** Returns TURBO_EBUSY while still attached to a flow. */
CXX_C_API int turbo_flow_observe_destroy(turbo_flow_observe_t *observe);

/** Attach before start. One Observe object can be attached to one flow. */
CXX_C_API int turbo_flow_observe_attach(turbo_flow_observe_t *observe, turbo_flow_t *flow);
CXX_C_API int turbo_flow_observe_detach(turbo_flow_observe_t *observe);

CXX_C_API int turbo_flow_observe_snapshot(const turbo_flow_observe_t *observe,
                                          turbo_flow_observe_snapshot_t *out);
/**
 * Pull one control-plane view. The host must serialize this call with flow
 * lifecycle/configuration operations; message counters may change during the pull.
 * Pool details remain available via core pool snapshots.
 */
CXX_C_API int turbo_flow_observe_graph_snapshot(const turbo_flow_observe_t *observe,
                                                turbo_flow_observe_graph_snapshot_t *out);
/**
 * Capture traffic, system, and aggregate graph facts for the control DSL.
 * The snapshot must outlive evaluation through facts; both are caller-owned.
 */
CXX_C_API int
turbo_flow_observe_control_facts(const turbo_flow_observe_t *observe,
                                 turbo_flow_observe_control_facts_snapshot_t *snapshot,
                                 turbo_flow_control_facts_t *facts);
/**
 * Run one host-owned reconcile tick. The caller owns scheduling and state and
 * serializes this call with flow lifecycle/configuration operations.
 */
CXX_C_API int turbo_flow_observe_reconcile_pool(turbo_flow_observe_t *observe,
                                                const turbo_flow_observe_pool_policy_t *policy,
                                                turbo_flow_observe_reconcile_state_t *state,
                                                uint64_t now_ns,
                                                turbo_flow_observe_reconcile_result_t *out);
CXX_C_API int
turbo_flow_observe_record_control_event(turbo_flow_observe_t *observe,
                                        const turbo_flow_observe_control_event_t *event);
CXX_C_API size_t turbo_flow_observe_event_count(const turbo_flow_observe_t *observe);
CXX_C_API uint64_t turbo_flow_observe_dropped_event_count(const turbo_flow_observe_t *observe);
CXX_C_API int turbo_flow_observe_event_at(const turbo_flow_observe_t *observe, size_t index,
                                          turbo_flow_observe_event_record_t *out);
CXX_C_API int
turbo_flow_observe_record_command_result(turbo_flow_observe_t *observe, const char *target_uid,
                                         const turbo_flow_resource_command_result_t *result);
CXX_C_API int
turbo_flow_observe_last_command_result(const turbo_flow_observe_t *observe,
                                       char target_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u],
                                       turbo_flow_resource_command_result_t *out);
CXX_C_API size_t turbo_flow_observe_resource_count(const turbo_flow_observe_t *observe);
CXX_C_API int turbo_flow_observe_resource_at(const turbo_flow_observe_t *observe, size_t index,
                                             turbo_flow_observe_resource_view_t *out);
/** Prometheus text exposition; resource identity is label-escaped and payload is never queried. */
CXX_C_API int turbo_flow_observe_export_prometheus(const turbo_flow_observe_t *observe,
                                                   turbo_flow_observe_write_fn write, void *ctx);
/** Exporter-neutral OpenTelemetry bridge; the callback maps samples to its selected SDK. */
CXX_C_API int turbo_flow_observe_export_opentelemetry(const turbo_flow_observe_t *observe,
                                                      turbo_flow_observe_metric_fn emit, void *ctx);
CXX_C_API size_t turbo_flow_observe_stage_count(const turbo_flow_observe_t *observe);
CXX_C_API int turbo_flow_observe_stage_snapshot_at(const turbo_flow_observe_t *observe,
                                                   size_t index,
                                                   turbo_flow_observe_stage_snapshot_t *out);

/** Register an explicit terminal summary sink. The host callback owns logging policy. */
CXX_C_API int turbo_flow_observe_register_log_sink(turbo_flow_t *flow, const char *name,
                                                   const turbo_flow_observe_log_config_t *config);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_OBSERVE_H */
