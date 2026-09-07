#ifndef TURBO_FLOW_INTERNAL_H
#define TURBO_FLOW_INTERNAL_H

#include "turbo_flow.h"
#include "turbo_flow_expr.h"

#include "disruptor.h"
#include "salts_coro.h"
#include "salts_coro_pool.h"
#include "turbo_flow_stl_error_internal.h"
#include "salts_thread.h"

#include <cflow/cflow.h>

#include <stdbool.h>
#include <stdint.h>

#define TURBO_FLOW_LINE_START 1u
#define TURBO_FLOW_COLUMN_START 1u
#define FLOW_WORKER_POOL_DEFAULT_CAPACITY 1024u
#define FLOW_WORKER_POOL_MAX_CAPACITY 1048576u

typedef struct flow_stage_plan_impl_s {
  tstr name;
  uint32_t line;
  uint32_t column;
  int is_source;
  int is_port;
  int is_port_output;
  tstr adapter_name;
  tstr operation_name;
  tstr resource_name;
  turbo_flow_operation_descriptor_t resolved_operation;
  int operation_resolved;
  turbo_flow_data_strategy_t data_strategy;
  uint32_t data_worker_count;
  uint32_t data_pool_capacity;
  turbo_flow_exec_config_t exec;
  turbo_flow_stage_mutability_t mutability;
  uint32_t effects;
  turbo_flow_retry_policy_t retry;
  turbo_flow_reorder_config_t reorder;
  turbo_flow_stage_fn fn;
  turbo_flow_emitting_stage_fn emit_fn;
  turbo_flow_key_selector_fn key_selector;
  void *key_ctx;
  turbo_flow_keyed_stage_fn keyed_fn;
  turbo_flow_keyed_emitting_stage_fn keyed_emit_fn;
  turbo_flow_event_time_window_stage_fn window_fn;
  turbo_flow_event_time_window_close_fn window_close_fn;
  turbo_flow_keyed_state_store_t *keyed_store;
  uint32_t max_outputs;
  void *ctx;
} flow_stage_plan_impl_t;

typedef struct flow_stage_registration_s {
  tstr name;
  turbo_flow_stage_fn fn;
  void *ctx;
  turbo_flow_stage_options_t options;
} flow_stage_registration_t;

typedef struct flow_operation_provider_registration_s {
  tstr operation_name;
  tstr resource_name;
  tstr module_name;
  turbo_flow_stage_fn fn;
  turbo_flow_emitting_stage_fn emit_fn;
  turbo_flow_key_selector_fn key_selector;
  void *key_ctx;
  turbo_flow_keyed_stage_fn keyed_fn;
  turbo_flow_keyed_emitting_stage_fn keyed_emit_fn;
  turbo_flow_event_time_window_stage_fn window_fn;
  turbo_flow_event_time_window_close_fn window_close_fn;
  turbo_flow_keyed_state_store_t *keyed_store;
  uint32_t max_outputs;
  void *ctx;
  turbo_flow_stage_options_t options;
} flow_operation_provider_registration_t;

typedef struct flow_adapter_operation_binding_s {
  tstr operation_name;
  tstr module_name;
  tstr resource_name;
} flow_adapter_operation_binding_t;

typedef struct flow_adapter_registration_s {
  tstr name;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_adapter_consume_batch_fn consume_batch;
  void *ctx;
  turbo_flow_settlement_owner_ops_t settlement_ops;
  void *settlement_ctx;
  turbo_flow_adapter_schema_t schema;
  turbo_flow_option_field_t *schema_fields;
  vec_t operation_bindings;
} flow_adapter_registration_t;

typedef struct flow_resource_registration_s {
  tstr owner_name;
  turbo_flow_resource_provider_ops_t ops;
  void *ctx;
} flow_resource_registration_t;

typedef struct flow_primitive_registration_s {
  turbo_flow_primitive_descriptor_t descriptor;
  tstr name;
  tstr type_name;
} flow_primitive_registration_t;

typedef struct flow_operation_registration_s {
  turbo_flow_operation_descriptor_t descriptor;
  tstr name;
  tstr input_type;
  tstr output_type;
  tstr resource_type;
} flow_operation_registration_t;

typedef struct flow_module_registration_s {
  turbo_flow_module_descriptor_t descriptor;
  tstr name;
  vec_t primitive_types;
  vec_t operation_names;
  vec_t requirements;
} flow_module_registration_t;

typedef struct flow_active_adapter_s {
  uint32_t stage_index;
  size_t adapter_index;
} flow_active_adapter_t;

typedef struct flow_edge_plan_impl_s {
  tstr from_name;
  tstr to_name;
  uint32_t from_stage;
  uint32_t to_stage;
  uint32_t line;
  uint32_t column;
  int is_stage_internal;
  turbo_flow_edge_kind_t kind;
  tstr condition;
  tstr name;
  turbo_flow_expr_t *predicate;
} flow_edge_plan_impl_t;

typedef struct flow_expr_projection_field_s {
  tstr path;
  turbo_flow_expr_value_type_t type;
  uint32_t field_id;
} flow_expr_projection_field_t;

typedef struct flow_expr_projection_registration_s {
  turbo_flow_data_schema_t schema;
  tstr schema_name;
  tstr type_name;
  tstr projection_type;
  flow_expr_projection_field_t *fields;
  size_t field_count;
  turbo_flow_expr_projection_field_fn read_field;
  void *ctx;
} flow_expr_projection_registration_t;

typedef struct flow_expr_projection_eval_binding_s {
  const flow_expr_projection_registration_t *registration;
  const void *projection;
} flow_expr_projection_eval_binding_t;

typedef enum flow_runtime_node_flags_e {
  FLOW_RUNTIME_NODE_SOURCE = 1u << 0,
  FLOW_RUNTIME_NODE_PORT = 1u << 1,
  FLOW_RUNTIME_NODE_WORKER_POOL = 1u << 2,
  FLOW_RUNTIME_NODE_FANOUT = 1u << 3,
  FLOW_RUNTIME_NODE_FANIN = 1u << 4
} flow_runtime_node_flags_t;

typedef struct flow_runtime_node_plan_s {
  uint32_t stage_index;
  uint32_t incoming_count;
  uint32_t outgoing_begin;
  uint32_t outgoing_count;
  uint32_t flags;
} flow_runtime_node_plan_t;

typedef struct flow_runtime_edge_plan_s {
  uint32_t from_stage;
  uint32_t to_stage;
  uint32_t line;
  uint32_t column;
  int is_stage_internal;
  turbo_flow_edge_kind_t kind;
  const char *name;
  const turbo_flow_expr_t *predicate;
} flow_runtime_edge_plan_t;

typedef enum flow_data_segment_kind_e {
  FLOW_DATA_SEGMENT_DIRECT = TURBO_FLOW_SEGMENT_DIRECT,
  FLOW_DATA_SEGMENT_BROADCAST_FANOUT = TURBO_FLOW_SEGMENT_BROADCAST_FANOUT,
  FLOW_DATA_SEGMENT_WORKER_POOL = TURBO_FLOW_SEGMENT_WORKER_POOL,
  FLOW_DATA_SEGMENT_FANIN_GATE = TURBO_FLOW_SEGMENT_FANIN_GATE
} flow_data_segment_kind_t;

typedef struct flow_data_segment_plan_s {
  flow_data_segment_kind_t kind;
  uint32_t stage_index;
  uint32_t edge_index;
  uint32_t width;
  uint32_t capacity;
  turbo_flow_operation_runtime_contract_t operation;
} flow_data_segment_plan_t;

typedef struct flow_executor_plan_s {
  uint32_t stage_index;
  turbo_flow_exec_config_t exec;
  turbo_flow_stage_fn fn;
  turbo_flow_emitting_stage_fn emit_fn;
  turbo_flow_key_selector_fn key_selector;
  void *key_ctx;
  turbo_flow_keyed_stage_fn keyed_fn;
  turbo_flow_keyed_emitting_stage_fn keyed_emit_fn;
  turbo_flow_event_time_window_stage_fn window_fn;
  turbo_flow_event_time_window_close_fn window_close_fn;
  turbo_flow_keyed_state_store_t *keyed_store;
  uint32_t max_outputs;
  void *ctx;
} flow_executor_plan_t;

#define FLOW_PLAN_INDEX_NONE UINT32_MAX

typedef enum flow_plan_backend_requirement_e {
  FLOW_PLAN_BACKEND_NATIVE = 0,
  FLOW_PLAN_BACKEND_CFLOW
} flow_plan_backend_requirement_t;

typedef enum flow_lowering_barrier_e {
  FLOW_LOWERING_BARRIER_NONE = 0,
  FLOW_LOWERING_BARRIER_UNTYPED_CALLABLE = 1u << 0,
  FLOW_LOWERING_BARRIER_STATEFUL = 1u << 1,
  FLOW_LOWERING_BARRIER_ASYNC = 1u << 2,
  FLOW_LOWERING_BARRIER_RETRY = 1u << 3,
  FLOW_LOWERING_BARRIER_SETTLEMENT = 1u << 4,
  FLOW_LOWERING_BARRIER_WINDOW = 1u << 5,
  FLOW_LOWERING_BARRIER_DYNAMIC_ROUTE = 1u << 6,
  FLOW_LOWERING_BARRIER_EXTERNAL_IO = 1u << 7,
  FLOW_LOWERING_BARRIER_ORDERING = 1u << 8,
  FLOW_LOWERING_BARRIER_RELATION = 1u << 9,
  FLOW_LOWERING_BARRIER_MESSAGE_MUTATION = 1u << 10
} flow_lowering_barrier_t;

typedef struct flow_semantic_type_plan_s {
  tstr stable_id;
  cmeta_type_identity identity;
  cmeta_type_desc descriptor;
} flow_semantic_type_plan_t;

typedef struct flow_stage_semantic_plan_s {
  uint32_t input_type_index;
  uint32_t output_type_index;
  uint32_t candidate_region;
  cmeta_effects effects;
  uint32_t barriers;
  cflow_op cflow_operator;
  int typed;
  int lowering_candidate;
} flow_stage_semantic_plan_t;

/**
 * Single-owner compiled graph plan. The compiler is the only writer; runtime
 * code may read the contained storage only after sealed becomes nonzero.
 */
typedef struct flow_compiled_plan_s {
  vec_t nodes;
  vec_t edges;
  vec_t data_segments;
  vec_t executors;
  vec_t executor_by_stage;
  /** Adapter registry entry for a stage, or FLOW_PLAN_INDEX_NONE. */
  vec_t adapter_by_stage;
  /** Primary bounded data-plane segment for a stage, or FLOW_PLAN_INDEX_NONE. */
  vec_t data_segment_by_stage;
  vec_t semantic_types;
  vec_t stage_semantics;
  const cmeta_type_desc *message_type;
  const cmeta_type_desc *operation_type;
  uint32_t candidate_region_count;
  int sealed;
} flow_compiled_plan_t;

/** Mutable runtime capacity derived from, but never written back into, a sealed plan. */
typedef struct flow_runtime_stage_config_s {
  uint32_t data_workers;
  uint32_t thread_workers;
  uint32_t coro_lanes;
} flow_runtime_stage_config_t;

struct turbo_flow_emitter_s {
  vec_t outputs;
  uint32_t max_outputs;
  int active;
  int status;
};

typedef struct flow_stage_completion_s flow_stage_completion_t;

typedef enum flow_entry_ownership_e {
  FLOW_ENTRY_OWNERSHIP_BORROWED = 0,
  FLOW_ENTRY_OWNERSHIP_OWNED_MESSAGE
} flow_entry_ownership_t;

typedef struct flow_entry_header_s {
  size_t size;
  uint64_t runtime_generation;
  uint32_t stage_index;
  uint32_t edge_index;
  flow_data_segment_kind_t segment_kind;
  uint32_t worker_lane;
  flow_entry_ownership_t ownership;
  uint64_t ordering_key;
  uint64_t sequence;
  uint64_t message_id;
  uint64_t deadline_at_ns;
  atomic_int *cancel_handle;
  flow_stage_completion_t *completion_handle;
} flow_entry_header_t;

#define FLOW_ENTRY_HEADER_INIT                                                                     \
  {sizeof(flow_entry_header_t),                                                                    \
   0u,                                                                                             \
   UINT32_MAX,                                                                                     \
   UINT32_MAX,                                                                                     \
   FLOW_DATA_SEGMENT_DIRECT,                                                                       \
   UINT32_MAX,                                                                                     \
   FLOW_ENTRY_OWNERSHIP_BORROWED,                                                                  \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL}

struct flow_stage_completion_s {
  flow_entry_header_t entry;
  int status;
  int terminal;
  int settlement_reported;
  int settlement_duplicate;
  turbo_flow_settlement_result_t settlement;
};

typedef enum flow_execution_backend_e {
  FLOW_EXECUTION_THREAD = 1,
  FLOW_EXECUTION_CORO,
  FLOW_EXECUTION_DISRUPTOR
} flow_execution_backend_t;

typedef enum flow_execution_state_e {
  FLOW_EXECUTION_NEW = 0,
  FLOW_EXECUTION_ACCEPTED,
  FLOW_EXECUTION_RUNNING,
  FLOW_EXECUTION_COMPLETED,
  FLOW_EXECUTION_CANCELED
} flow_execution_state_t;

typedef struct flow_execution_task_s {
  flow_execution_backend_t backend;
  turbo_flow_stage_fn fn;
  void *ctx;
  turbo_flow_msg_t msg;
  flow_stage_completion_t completion;
  salts_mutex_t mutex;
  salts_cond_t cond;
  atomic_int state;
  atomic_int cancel_requested;
  atomic_int deadline_expired;
  atomic_int accounting_done;
  uint64_t deadline_ms;
  atomic_uint_fast64_t deadline_at_ns;
  int status;
  int sync_initialized;
} flow_execution_task_t;

typedef struct flow_threadpool_adapter_s {
  uint32_t stage_index;
  uint32_t workers;
  size_t pool_record_index;
  salts_threadpool_t *pool;
} flow_threadpool_adapter_t;

typedef struct flow_coro_adapter_s {
  uint32_t stage_index;
  uint32_t lanes;
  size_t pool_record_index;
  atomic_uint next_lane;
  coro_scheduler_t **schedulers;
  salts_coro_pool_t **pools;
  salts_mutex_t *lane_mutexes;
} flow_coro_adapter_t;

typedef struct flow_broadcast_consumer_s {
  uint32_t stage_index;
  disruptor_consumer_t consumer;
  disruptor_stage_t topology_stage;
  uint64_t next_sequence;
} flow_broadcast_consumer_t;

typedef struct flow_worker_pool_adapter_s {
  turbo_flow_t *flow;
  uint32_t stage_index;
  uint32_t width;
  uint32_t capacity;
  size_t pool_record_index;
  flow_stage_plan_impl_t *stage;
  const flow_executor_plan_t *executor;
  disruptor_t *ring;
  salts_thread_t *workers;
  void *worker_contexts;
  atomic_int accepting;
  atomic_int running;
  atomic_uint pending;
  atomic_uint submitters;
} flow_worker_pool_adapter_t;

typedef struct flow_pool_record_s {
  turbo_flow_pool_kind_t kind;
  uint32_t stage_index;
  uint64_t generation;
  uint32_t parallelism;
  uint64_t queue_capacity;
  uint64_t resource_capacity;
  atomic_int state;
  atomic_uint_fast64_t submitted;
  atomic_uint_fast64_t started;
  atomic_uint_fast64_t completed;
  atomic_uint_fast64_t failed;
  atomic_uint_fast64_t canceled;
  atomic_uint_fast64_t rejected;
  atomic_uint_fast64_t queued;
  atomic_uint_fast64_t active;
} flow_pool_record_t;

typedef struct flow_resource_command_record_s {
  turbo_flow_resource_command_t command;
  turbo_flow_resource_command_result_t result;
} flow_resource_command_record_t;

typedef enum flow_admission_state_e {
  FLOW_ADMISSION_CLOSED = 0,
  FLOW_ADMISSION_OPEN,
  FLOW_ADMISSION_PAUSED,
  FLOW_ADMISSION_RESIZING,
  FLOW_ADMISSION_STOPPING
} flow_admission_state_t;

typedef struct flow_reorder_state_s {
  uint32_t stage_index;
  uint32_t capacity;
  uint32_t timeout_ms;
  uint32_t waiting;
  uint64_t issued_sequence;
  uint64_t next_sequence;
  hash_set_t canceled_sequences;
  int canceled_sequences_initialized;
  int active;
  int stopping;
  salts_mutex_t mutex;
  salts_cond_t cond;
} flow_reorder_state_t;

typedef struct flow_event_observer_registration_s {
  tstr name;
  turbo_flow_event_observer_ops_t ops;
  void *ctx;
} flow_event_observer_registration_t;

typedef int (*flow_pool_rebuild_fault_fn)(void *ctx, size_t attempt);

/** Private deterministic resource-construction seam used by internal runtime tests. */
typedef struct flow_pool_rebuild_fault_s {
  flow_pool_rebuild_fault_fn before_create;
  void *ctx;
  size_t attempts;
} flow_pool_rebuild_fault_t;

struct turbo_flow_s {
  turbo_flow_state_t state;
  vec_t stages;
  vec_t edges;
  flow_compiled_plan_t compiled_plan;
  flow_plan_backend_requirement_t required_backend;
  vec_t runtime_stage_configs;
  vec_t threadpool_adapters;
  vec_t threadpool_adapter_by_stage;
  vec_t coro_adapters;
  vec_t coro_adapter_by_stage;
  vec_t broadcast_consumers;
  vec_t worker_pool_adapters;
  vec_t worker_pool_adapter_by_stage;
  vec_t reorder_states;
  disruptor_t *broadcast_ring;
  disruptor_topology_t *broadcast_topology;
  vec_t registrations;
  vec_t operation_providers;
  vec_t primitives;
  vec_t operations;
  vec_t modules;
  vec_t adapters;
  vec_t resources;
  vec_t expr_projection_registrations;
  vec_t active_adapters;
  vec_t pool_records;
  flow_pool_rebuild_fault_t pool_rebuild_fault;
  vec_t resource_command_history;
  vec_t event_observers;
  uint64_t runtime_generation;
  atomic_uint_fast64_t next_sequence;
  atomic_uint_fast64_t observer_failures;
  salts_mutex_t runtime_mutex;
  salts_cond_t runtime_cond;
  salts_mutex_t broadcast_mutex;
  salts_mutex_t async_ingress_mutex;
  salts_threadpool_t *async_ingress_pool;
  turbo_flow_async_ingress_config_t async_ingress_config;
  size_t async_ingress_inflight_bytes;
  uint32_t active_publishes;
  flow_admission_state_t admission_state;
  int runtime_sync_initialized;
  turbo_flow_observer_ops_t observer_ops;
  void *observer_ctx;
  turbo_flow_error_t last_error;
};

int flow_set_error(turbo_flow_t *flow, int code, uint32_t line, uint32_t column,
                   const char *message);
int flow_set_error_keep_state(turbo_flow_t *flow, int code, uint32_t line, uint32_t column,
                              const char *message);
void flow_clear_error(turbo_flow_t *flow);
void flow_publish_error_context_begin(turbo_flow_t *flow);
void flow_publish_error_context_end(turbo_flow_t *flow);
int flow_error_code(const turbo_flow_t *flow);
int flow_observer_event_enabled(const turbo_flow_t *flow,
                                turbo_flow_observe_event_kind_t kind);
int flow_observer_has_handlers(const turbo_flow_t *flow);
void flow_observer_emit(turbo_flow_t *flow, const turbo_flow_observe_event_t *event);
void flow_observer_clear(turbo_flow_t *flow);
int flow_entry_header_init(const turbo_flow_t *flow, flow_entry_header_t *header,
                           uint32_t stage_index, flow_data_segment_kind_t segment_kind,
                           uint64_t ordering_key, uint64_t sequence, uint64_t message_id,
                           flow_entry_ownership_t ownership,
                           flow_stage_completion_t *completion_handle);
int flow_entry_header_validate(const turbo_flow_t *flow, const flow_entry_header_t *header,
                               const turbo_flow_msg_t *message);

int flow_view_eq_cstr(vstr view, const char *text);
int flow_find_stage_view(const turbo_flow_t *flow, vstr name);
int flow_find_registration(const turbo_flow_t *flow, const char *name);
int flow_find_operation_provider(const turbo_flow_t *flow, const char *operation_name,
                                 const char *resource_name);
int flow_find_adapter(const turbo_flow_t *flow, const char *name);
int flow_resource_metadata_valid(const turbo_flow_resource_metadata_t *metadata);
int flow_resource_command_valid(const turbo_flow_resource_command_t *command);
size_t flow_native_resource_count(const turbo_flow_t *flow);
int flow_native_resource_metadata_at(const turbo_flow_t *flow, size_t index,
                                     turbo_flow_resource_metadata_t *out);
int flow_native_resource_snapshot_at(const turbo_flow_t *flow, size_t index,
                                     turbo_flow_resource_snapshot_t *out);
int flow_native_resource_document_at(const turbo_flow_t *flow, size_t index,
                                     turbo_flow_resource_document_kind_t document_kind,
                                     turbo_flow_resource_document_t *out);
int flow_native_resource_command(turbo_flow_t *flow, size_t index,
                                 const turbo_flow_resource_command_t *command);
void flow_make_stage_view(const flow_stage_plan_impl_t *stage, turbo_flow_stage_plan_t *view);
int flow_find_primitive_index(const turbo_flow_t *flow, const char *name);
int flow_find_operation_index(const turbo_flow_t *flow, const char *name);
int flow_find_module_index(const turbo_flow_t *flow, const char *name);
int flow_find_operation_export_module(const turbo_flow_t *flow, const char *operation_name);
void flow_expr_projection_clear(turbo_flow_t *flow);
int flow_expr_projection_compile(turbo_flow_t *flow, const char *text, size_t len,
                                 turbo_flow_expr_t **out, turbo_flow_error_t *error);
void flow_expr_projection_bind_eval(
    const turbo_flow_t *flow, const turbo_flow_msg_t *msg,
    flow_expr_projection_eval_binding_t *binding, turbo_flow_expr_eval_context_t *context);
const flow_adapter_operation_binding_t *
flow_find_adapter_operation_binding(const flow_adapter_registration_t *adapter,
                                    const char *operation_name);

void flow_stage_impl_destroy(flow_stage_plan_impl_t *stage);
void flow_registration_destroy(flow_stage_registration_t *reg);
void flow_operation_provider_registration_destroy(flow_operation_provider_registration_t *provider);
void flow_resource_registration_destroy(flow_resource_registration_t *resource);
void flow_adapter_registration_destroy(flow_adapter_registration_t *adapter);
void flow_primitive_registration_destroy(flow_primitive_registration_t *primitive);
void flow_operation_registration_destroy(flow_operation_registration_t *operation);
void flow_module_registration_destroy(flow_module_registration_t *module);
void flow_edge_impl_destroy(flow_edge_plan_impl_t *edge);
int flow_msg_set_failure(turbo_flow_msg_t *msg, const char *stage_name, const char *adapter_name,
                         const char *route_name, int code, uint32_t attempt);
int flow_msg_payload_validate(const turbo_flow_msg_t *msg);
int flow_msg_transport_context_is_borrowed(const turbo_flow_msg_t *msg);
void flow_clear_runtime_plan(turbo_flow_t *flow);
int flow_compiled_plan_init(flow_compiled_plan_t *plan);
void flow_compiled_plan_destroy(flow_compiled_plan_t *plan);
int flow_plan_build_semantics(const turbo_flow_t *flow, flow_compiled_plan_t *plan);
int flow_runtime_stage_index_reset(vec_t *index_by_stage, size_t stage_count);
const flow_runtime_stage_config_t *flow_runtime_stage_config_for_stage(const turbo_flow_t *flow,
                                                                       uint32_t stage_index);
flow_runtime_stage_config_t *flow_runtime_stage_config_for_stage_mut(turbo_flow_t *flow,
                                                                     uint32_t stage_index);
void flow_clear_plan(turbo_flow_t *flow);
void flow_clear_registry(turbo_flow_t *flow);
int flow_build_runtime_plan(turbo_flow_t *flow);
void flow_close_publish_admission(turbo_flow_t *flow);
void flow_wait_for_publishes(turbo_flow_t *flow);
int flow_pool_record_add(turbo_flow_t *flow, turbo_flow_pool_kind_t kind, uint32_t stage_index,
                         uint32_t parallelism, uint64_t queue_capacity, uint64_t resource_capacity,
                         size_t *index);
flow_pool_record_t *flow_pool_record_at(turbo_flow_t *flow, size_t index);
void flow_pool_record_set_state(flow_pool_record_t *record, turbo_flow_pool_state_t state);
void flow_pool_record_submitted(flow_pool_record_t *record);
void flow_pool_record_attempted(flow_pool_record_t *record);
void flow_pool_record_queued(flow_pool_record_t *record);
void flow_pool_record_started(flow_pool_record_t *record);
void flow_pool_record_finished(flow_pool_record_t *record, int status);
void flow_pool_record_rejected(flow_pool_record_t *record);
void flow_pool_record_rejected_unqueued(flow_pool_record_t *record);
void flow_pool_record_canceled_unqueued(flow_pool_record_t *record);
int flow_runtime_generation_can_advance(const turbo_flow_t *flow);
void flow_runtime_generation_commit(turbo_flow_t *flow);
int flow_start_data_planes(turbo_flow_t *flow);
void flow_stop_data_planes(turbo_flow_t *flow);
int flow_publish_broadcast_data_plane(turbo_flow_t *flow, uint32_t source_index,
                                      turbo_flow_msg_t *msg, uint64_t sequence,
                                      turbo_flow_publish_result_t *result);
int flow_start_executor_adapters(turbo_flow_t *flow);
void flow_stop_runtime_executor_adapters(turbo_flow_t *flow);
void flow_stop_executor_adapters(turbo_flow_t *flow);
int flow_start_adapters(turbo_flow_t *flow);
void flow_stop_adapters(turbo_flow_t *flow);
const flow_adapter_registration_t *flow_adapter_for_stage(const turbo_flow_t *flow,
                                                          const flow_stage_plan_impl_t *stage);
TURBO_FLOW_C_API const flow_adapter_registration_t *
flow_adapter_for_compiled_stage(const turbo_flow_t *flow, uint32_t stage_index);
int flow_adapter_consume_stage(turbo_flow_t *flow, const flow_stage_plan_impl_t *stage,
                               const flow_adapter_registration_t *adapter, turbo_flow_msg_t *msg);
int flow_adapter_apply_settlement(turbo_flow_t *flow, const flow_stage_plan_impl_t *stage,
                                  uint32_t stage_index, turbo_flow_msg_t *msg,
                                  flow_stage_completion_t *completion, int callback_status);
flow_stage_completion_t *flow_settlement_scope_enter(flow_stage_completion_t *completion);
void flow_settlement_scope_leave(flow_stage_completion_t *previous);
TURBO_FLOW_C_API const flow_executor_plan_t *flow_executor_plan_for_stage(const turbo_flow_t *flow,
                                                                          uint32_t stage_index);
TURBO_FLOW_C_API const flow_data_segment_plan_t *
flow_worker_pool_segment_for_stage(const turbo_flow_t *flow, uint32_t stage_index);
TURBO_FLOW_C_API const flow_threadpool_adapter_t *
flow_threadpool_adapter_for_stage(const turbo_flow_t *flow, uint32_t stage_index);
TURBO_FLOW_C_API flow_coro_adapter_t *flow_coro_adapter_for_stage(turbo_flow_t *flow,
                                                                  uint32_t stage_index);
TURBO_FLOW_C_API flow_worker_pool_adapter_t *
flow_worker_pool_adapter_for_stage(turbo_flow_t *flow, uint32_t stage_index);
TURBO_FLOW_C_API int flow_worker_pool_submit(flow_worker_pool_adapter_t *adapter,
                                             turbo_flow_msg_t *msg,
                                             flow_stage_completion_t *completion);
TURBO_FLOW_C_API int
flow_execution_task_init(flow_execution_task_t *task, flow_execution_backend_t backend,
                         turbo_flow_stage_fn fn, void *ctx, turbo_flow_msg_t *msg,
                         const flow_stage_completion_t *completion, uint64_t deadline_ms);
TURBO_FLOW_C_API void flow_execution_task_run(flow_execution_task_t *task);
TURBO_FLOW_C_API void flow_execution_task_fail(flow_execution_task_t *task, int status);
TURBO_FLOW_C_API int flow_execution_task_wait(flow_execution_task_t *task, turbo_flow_msg_t *msg,
                                              flow_stage_completion_t *completion);
TURBO_FLOW_C_API void flow_execution_task_mark_accounting_done(flow_execution_task_t *task);
TURBO_FLOW_C_API void flow_execution_task_wait_accounting(flow_execution_task_t *task);
TURBO_FLOW_C_API int flow_execution_task_abort(flow_execution_task_t *task);
TURBO_FLOW_C_API void flow_execution_task_discard(flow_execution_task_t *task);
TURBO_FLOW_C_API int flow_execution_yield(void);
TURBO_FLOW_C_API int flow_execution_suspend_for_io(void);
TURBO_FLOW_C_API int flow_execution_cancel_requested(void);
TURBO_FLOW_C_API flow_execution_state_t flow_execution_task_state(const flow_execution_task_t *task);
TURBO_FLOW_C_API void flow_execution_task_cleanup(flow_execution_task_t *task);
const turbo_flow_operation_runtime_contract_t *
flow_stage_operation_runtime(const turbo_flow_t *flow, const flow_stage_plan_impl_t *stage);
const turbo_flow_operation_descriptor_t *
flow_stage_operation_descriptor(const flow_stage_plan_impl_t *stage);
int flow_dispatch_call_executor(turbo_flow_t *flow, flow_stage_plan_impl_t *stage,
                                const flow_executor_plan_t *executor, uint32_t stage_index,
                                turbo_flow_msg_t *msg, flow_stage_completion_t *completion);
int flow_execute_threadpool_stage(turbo_flow_t *flow, flow_stage_plan_impl_t *stage,
                                  const flow_executor_plan_t *executor, uint32_t stage_index,
                                  turbo_flow_msg_t *msg, flow_stage_completion_t *completion);
int flow_execute_coro_stage(turbo_flow_t *flow, flow_stage_plan_impl_t *stage,
                            const flow_executor_plan_t *executor, uint32_t stage_index,
                            turbo_flow_msg_t *msg, flow_stage_completion_t *completion);
TURBO_FLOW_C_API int flow_mark_reachable_from_stage(const turbo_flow_t *flow, uint8_t *reachable,
                                                    uint32_t *worklist, size_t worklist_cap,
                                                    uint32_t stage_index);
int flow_dispatch_validate_stage(turbo_flow_t *flow, uint32_t stage_index);
int flow_dispatch_stage(turbo_flow_t *flow, uint32_t stage_index, turbo_flow_msg_t *msg,
                        uint64_t sequence, uint64_t msg_id, flow_stage_completion_t *completion,
                        turbo_flow_emitter_t *emitter);
int flow_run_message_from_stage(turbo_flow_t *flow, uint32_t origin_stage,
                                turbo_flow_msg_t *message);
int flow_publish_enter(turbo_flow_t *flow);
void flow_publish_leave(turbo_flow_t *flow);
void flow_stop_async_ingress(turbo_flow_t *flow);
int flow_publish_local(turbo_flow_t *flow, const char *source_name, uint32_t source_index,
                       turbo_flow_msg_t *local, uint64_t observe_start,
                       turbo_flow_publish_result_t *result);
int flow_emitter_init(turbo_flow_emitter_t *emitter, uint32_t max_outputs);
void flow_emitter_close(turbo_flow_emitter_t *emitter);
void flow_emitter_cleanup(turbo_flow_emitter_t *emitter);
int flow_keyed_state_execute(turbo_flow_keyed_state_store_t *store,
                             turbo_flow_key_selector_fn key_selector, void *key_ctx,
                             turbo_flow_keyed_stage_fn fn, void *ctx, turbo_flow_msg_t *message);
int flow_keyed_state_execute_emitting(turbo_flow_keyed_state_store_t *store,
                                      turbo_flow_key_selector_fn key_selector, void *key_ctx,
                                      turbo_flow_keyed_emitting_stage_fn fn, void *ctx,
                                      const turbo_flow_msg_t *message,
                                      turbo_flow_emitter_t *emitter);
int flow_event_time_window_execute(turbo_flow_event_time_window_store_t *store,
                                   turbo_flow_key_selector_fn key_selector, void *key_ctx,
                                   turbo_flow_event_time_window_stage_fn fn, void *ctx,
                                   const turbo_flow_msg_t *message);
int flow_event_time_window_advance(turbo_flow_t *flow, uint32_t stage_index,
                                   turbo_flow_event_time_window_store_t *store,
                                   turbo_flow_event_time_window_close_fn close_fn, void *ctx,
                                   uint32_t max_outputs, uint64_t watermark_ns,
                                   size_t *closed_windows);
int flow_keyed_state_store_bind(turbo_flow_keyed_state_store_t *store);
int flow_keyed_state_store_is_event_time(const turbo_flow_keyed_state_store_t *store);
void flow_keyed_state_store_unbind(turbo_flow_keyed_state_store_t *store);
void flow_keyed_state_store_reset(turbo_flow_keyed_state_store_t *store);
int flow_apply_completion(turbo_flow_t *flow, const flow_stage_completion_t *completion,
                          turbo_flow_msg_t *msg, uint8_t *done, const uint8_t *reachable,
                          uint32_t *remaining, uint32_t *activated, uint32_t *queue,
                          size_t queue_cap, size_t *tail, uint32_t *skipped_queue,
                          size_t skipped_queue_cap);
TURBO_FLOW_C_API int flow_start_reorder_states(turbo_flow_t *flow);
TURBO_FLOW_C_API void flow_stop_reorder_states(turbo_flow_t *flow);
TURBO_FLOW_C_API void flow_clear_reorder_states(turbo_flow_t *flow);
TURBO_FLOW_C_API int flow_reorder_reserve(turbo_flow_t *flow, uint32_t stage_index, uint64_t *sequence);
TURBO_FLOW_C_API int flow_reorder_cancel(turbo_flow_t *flow, uint32_t stage_index, uint64_t sequence);
TURBO_FLOW_C_API int flow_reorder_enter(turbo_flow_t *flow, uint32_t stage_index, uint64_t sequence);
TURBO_FLOW_C_API void flow_reorder_leave(turbo_flow_t *flow, uint32_t stage_index, uint64_t sequence);

#endif /* TURBO_FLOW_INTERNAL_H */
