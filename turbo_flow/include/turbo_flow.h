#ifndef TURBO_FLOW_H
#define TURBO_FLOW_H

#include "turbo_flow_export.h"
#include "platform.h"
#include <salts/clock.h>
#include "salts_buffer.h"
#include "salts_error.h"
#include "turbo_flow_domain.h"
#include "turbo_flow_config_limits.h"
#include "tstr.h"
#include "vstr.h"

#include <cflow/reactive.h>

#include <stddef.h>
#include <stdint.h>

#ifndef TURBO_FLOW_DEPRECATED
  #if defined(_MSC_VER)
    #define TURBO_FLOW_DEPRECATED(message) __declspec(deprecated(message))
  #elif defined(__GNUC__) || defined(__clang__)
    #define TURBO_FLOW_DEPRECATED(message) __attribute__((deprecated(message)))
  #else
    #define TURBO_FLOW_DEPRECATED(message)
  #endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct turbo_flow_s turbo_flow_t;
typedef struct turbo_flow_run_s turbo_flow_run_t;
typedef struct turbo_flow_stage_plan_s turbo_flow_stage_plan_t;
typedef struct turbo_flow_schema_registry_s turbo_flow_schema_registry_t;
typedef struct turbo_flow_emitter_s turbo_flow_emitter_t;
typedef struct turbo_flow_keyed_state_store_s turbo_flow_keyed_state_store_t;
typedef struct turbo_flow_keyed_state_s turbo_flow_keyed_state_t;
typedef struct turbo_flow_keyed_state_store_s turbo_flow_event_time_window_store_t;

typedef enum turbo_flow_segment_kind_e {
  TURBO_FLOW_SEGMENT_DIRECT = 0,
  TURBO_FLOW_SEGMENT_BROADCAST_FANOUT,
  TURBO_FLOW_SEGMENT_WORKER_POOL,
  TURBO_FLOW_SEGMENT_FANIN_GATE
} turbo_flow_segment_kind_t;

typedef struct turbo_flow_segment_plan_s {
  turbo_flow_segment_kind_t kind;
  uint32_t stage_index;
  uint32_t edge_index;
  uint32_t width;
  uint32_t capacity;
  turbo_flow_operation_runtime_contract_t operation;
} turbo_flow_segment_plan_t;

typedef enum turbo_flow_pool_kind_e {
  TURBO_FLOW_POOL_THREAD = 0,
  TURBO_FLOW_POOL_CORO,
  TURBO_FLOW_POOL_DISRUPTOR
} turbo_flow_pool_kind_t;

typedef enum turbo_flow_pool_state_e {
  TURBO_FLOW_POOL_STOPPED = 0,
  TURBO_FLOW_POOL_STARTING,
  TURBO_FLOW_POOL_RUNNING,
  TURBO_FLOW_POOL_DRAINING,
  TURBO_FLOW_POOL_FAILED
} turbo_flow_pool_state_t;

typedef enum turbo_flow_resource_kind_e {
  TURBO_FLOW_RESOURCE_CONNECTION = 0,
  TURBO_FLOW_RESOURCE_QUEUE_BUFFER,
  TURBO_FLOW_RESOURCE_POOL,
  TURBO_FLOW_RESOURCE_RUNTIME,
  TURBO_FLOW_RESOURCE_SEGMENT,
  TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE,
  TURBO_FLOW_RESOURCE_STORAGE,
  TURBO_FLOW_RESOURCE_RULE_SET,
  TURBO_FLOW_RESOURCE_SECURITY_REALM
} turbo_flow_resource_kind_t;

typedef struct turbo_flow_pool_snapshot_s {
  turbo_flow_pool_kind_t kind;
  turbo_flow_pool_state_t state;
  uint32_t stage_index;
  const char *stage_name;
  uint32_t parallelism;
  uint64_t queue_capacity;
  uint64_t resource_capacity;
  uint64_t submitted;
  uint64_t started;
  uint64_t completed;
  uint64_t failed;
  uint64_t canceled;
  uint64_t rejected;
  uint64_t queued;
  uint64_t active;
} turbo_flow_pool_snapshot_t;

typedef struct turbo_flow_pool_resize_command_s {
  /** Set to sizeof(turbo_flow_pool_resize_command_t). */
  size_t size;
  const char *stage_name;
  turbo_flow_pool_kind_t kind;
  uint32_t parallelism;
  /** Zero checks immediately; UINT64_MAX waits without a deadline. */
  uint64_t drain_timeout_ms;
  /** Must match the current pool generation. */
  uint64_t expected_generation;
} turbo_flow_pool_resize_command_t;

#define TURBO_FLOW_RESOURCE_UID_MAX 511u
#define TURBO_FLOW_RESOURCE_OWNER_MAX 255u
#define TURBO_FLOW_RESOURCE_CONDITION_MAX 4u
#define TURBO_FLOW_RESOURCE_COMMAND_KEY_MAX 127u
#define TURBO_FLOW_RESOURCE_COMMAND_HISTORY_MAX 256u
#define TURBO_FLOW_RESOURCE_DOCUMENT_MAX_BYTES 1048576u

/** Stable management identity; unlike resource_snapshot.identity this survives enumeration. */
typedef struct turbo_flow_resource_metadata_s {
  size_t size;
  turbo_flow_domain_t domain;
  turbo_flow_resource_kind_t kind;
  char uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  char owner_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  uint64_t generation;
  uint64_t observed_generation;
} turbo_flow_resource_metadata_t;

#define TURBO_FLOW_RESOURCE_METADATA_INIT                                                          \
  {sizeof(turbo_flow_resource_metadata_t), TURBO_FLOW_DOMAIN_NONE, TURBO_FLOW_RESOURCE_CONNECTION}

typedef enum turbo_flow_resource_document_kind_e {
  TURBO_FLOW_RESOURCE_DOCUMENT_SPEC = 0,
  TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
  TURBO_FLOW_RESOURCE_DOCUMENT_CONDITIONS,
  TURBO_FLOW_RESOURCE_DOCUMENT_COMMAND,
  TURBO_FLOW_RESOURCE_DOCUMENT_EVENT
} turbo_flow_resource_document_kind_t;

typedef enum turbo_flow_resource_document_encoding_e {
  TURBO_FLOW_RESOURCE_DOCUMENT_JSON = 0,
  TURBO_FLOW_RESOURCE_DOCUMENT_TBE
} turbo_flow_resource_document_encoding_t;

typedef enum turbo_flow_data_encoding_e {
  TURBO_FLOW_DATA_ENCODING_TBE = 0,
  TURBO_FLOW_DATA_ENCODING_JSON,
  TURBO_FLOW_DATA_ENCODING_CSV,
  TURBO_FLOW_DATA_ENCODING_XML,
  TURBO_FLOW_DATA_ENCODING_UTF8,
  TURBO_FLOW_DATA_ENCODING_OPAQUE
} turbo_flow_data_encoding_t;

#define TURBO_FLOW_CONTENT_MEDIA_TYPE_MAX 127u
#define TURBO_FLOW_CONTENT_SCHEMA_NAME_MAX 127u
#define TURBO_FLOW_CONTENT_TYPE_NAME_MAX 127u
#define TURBO_FLOW_CONTENT_IDENTITY_MAX 255u

typedef enum turbo_flow_content_profile_e {
  TURBO_FLOW_CONTENT_PROFILE_GENERIC = 0,
  TURBO_FLOW_CONTENT_PROFILE_HTTP_REQUEST_BODY,
  TURBO_FLOW_CONTENT_PROFILE_HTTP_RESPONSE_BODY,
  TURBO_FLOW_CONTENT_PROFILE_S3_OBJECT,
  TURBO_FLOW_CONTENT_PROFILE_DATABASE_PARAMETERS,
  TURBO_FLOW_CONTENT_PROFILE_DATABASE_ROWSET,
  TURBO_FLOW_CONTENT_PROFILE_DATABASE_COMMAND_RESULT,
  TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA,
  TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_CONTROL,
  TURBO_FLOW_CONTENT_PROFILE_MQTT_APPLICATION,
  TURBO_FLOW_CONTENT_PROFILE_MQTT_CONTROL
} turbo_flow_content_profile_t;

typedef enum turbo_flow_content_flags_e {
  TURBO_FLOW_CONTENT_SCHEMA_DECLARED = 1u << 0,
  TURBO_FLOW_CONTENT_BATCH = 1u << 1,
  TURBO_FLOW_CONTENT_PROTOCOL_CONTROL = 1u << 2
} turbo_flow_content_flags_t;

/**
 * Immutable content identity owned by a domain adapter or registry-facing host.
 *
 * It describes the original payload bytes and never owns a parsed value. Empty
 * schema fields mean that content is opaque or awaits registry resolution.
 */
typedef struct turbo_flow_content_descriptor_s {
  size_t size;
  turbo_flow_domain_t domain;
  turbo_flow_content_profile_t profile;
  turbo_flow_data_encoding_t encoding;
  uint32_t flags;
  uint32_t schema_version;
  char media_type[TURBO_FLOW_CONTENT_MEDIA_TYPE_MAX + 1u];
  char schema_name[TURBO_FLOW_CONTENT_SCHEMA_NAME_MAX + 1u];
  char type_name[TURBO_FLOW_CONTENT_TYPE_NAME_MAX + 1u];
  /** Non-secret domain identity such as an S3 object key or statement name. */
  char identity[TURBO_FLOW_CONTENT_IDENTITY_MAX + 1u];
} turbo_flow_content_descriptor_t;

typedef struct turbo_flow_content_schema_ref_s {
  size_t size;
  const char *schema_name;
  const char *type_name;
  uint32_t schema_version;
} turbo_flow_content_schema_ref_t;

#define TURBO_FLOW_CONTENT_SCHEMA_REF_INIT {sizeof(turbo_flow_content_schema_ref_t)}

typedef struct turbo_flow_content_binding_s {
  size_t size;
  const turbo_flow_schema_registry_t *registry;
  turbo_flow_content_schema_ref_t schema;
} turbo_flow_content_binding_t;

#define TURBO_FLOW_CONTENT_BINDING_INIT                                                            \
  {sizeof(turbo_flow_content_binding_t), NULL, TURBO_FLOW_CONTENT_SCHEMA_REF_INIT}

#define TURBO_FLOW_CONTENT_DESCRIPTOR_INIT                                                         \
  {sizeof(turbo_flow_content_descriptor_t), TURBO_FLOW_DOMAIN_DATA,                                \
   TURBO_FLOW_CONTENT_PROFILE_GENERIC, TURBO_FLOW_DATA_ENCODING_OPAQUE}

/** Initialize and checked-copy one domain content identity. */
TURBO_FLOW_C_API int turbo_flow_content_descriptor_init(turbo_flow_content_descriptor_t *descriptor,
                                                 turbo_flow_domain_t domain,
                                                 turbo_flow_content_profile_t profile,
                                                 turbo_flow_data_encoding_t encoding,
                                                 const char *media_type, const char *identity);

/** Validate descriptor structure, profile/domain scope, flags, and schema declaration coherence. */
TURBO_FLOW_C_API int
turbo_flow_content_descriptor_check(const turbo_flow_content_descriptor_t *descriptor);

/** Declare a trusted registry key on an initialized descriptor. */
TURBO_FLOW_C_API int
turbo_flow_content_descriptor_declare_schema(turbo_flow_content_descriptor_t *descriptor,
                                             const char *schema_name, const char *type_name,
                                             uint32_t schema_version);

/** Normalize a recognized media type to one stable registry key. */
TURBO_FLOW_C_API int turbo_flow_content_media_type_normalize(const char *media_type,
                                                      turbo_flow_data_encoding_t *encoding_out,
                                                      const char **normalized_media_type_out);

/** Resolve and declare a trusted schema while the owner may still mutate the descriptor. */
TURBO_FLOW_C_API int
turbo_flow_content_descriptor_resolve(turbo_flow_content_descriptor_t *descriptor,
                                      const turbo_flow_schema_registry_t *registry,
                                      const turbo_flow_content_schema_ref_t *selector);

/** Normalize domain metadata, initialize a descriptor, and apply one host binding. */
TURBO_FLOW_C_API int turbo_flow_content_descriptor_from_media(turbo_flow_content_descriptor_t *descriptor,
                                                       turbo_flow_domain_t domain,
                                                       turbo_flow_content_profile_t profile,
                                                       const char *media_type, const char *identity,
                                                       const turbo_flow_content_binding_t *binding);

/** Validate exact registry-key and declared-schema compatibility. */
TURBO_FLOW_C_API int
turbo_flow_content_descriptor_validate(const turbo_flow_content_descriptor_t *expected,
                                       const turbo_flow_content_descriptor_t *actual);

/** Validate payload media/encoding and any schema declared by `expected`, across domains. */
TURBO_FLOW_C_API int
turbo_flow_content_descriptor_validate_payload(const turbo_flow_content_descriptor_t *expected,
                                               const turbo_flow_content_descriptor_t *actual);

/**
 * Trusted, immutable identity for an optional schema-bound data projection.
 *
 * The provider owns this descriptor and every pointed-to string. The descriptor
 * must outlive each message projection that refers to it. `schema_text` may be
 * NULL when the trusted schema is resolved by name/id through a registry.
 */
typedef struct turbo_flow_data_schema_s {
  size_t size;
  turbo_flow_domain_t domain;
  turbo_flow_data_encoding_t encoding;
  const char *schema_name;
  const char *type_name;
  /** Stable provider value type, for example `Salts.DataBindValue`. */
  const char *projection_type;
  uint32_t schema_id;
  uint32_t schema_version;
  const char *schema_text;
} turbo_flow_data_schema_t;

#define TURBO_FLOW_DATA_SCHEMA_INIT                                                                \
  {sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_NONE, TURBO_FLOW_DATA_ENCODING_TBE}

/**
 * Trusted, immutable schema identity owned by a provider or registry.
 *
 * `schema_text` must remain valid for the descriptor lifetime. Consumers may
 * compile/cache it with DataBind using `type_name`; untrusted runtime input must
 * never be accepted as schema text.
 */
typedef struct turbo_flow_resource_schema_s {
  size_t size;
  turbo_flow_domain_t domain;
  turbo_flow_resource_kind_t resource_kind;
  turbo_flow_resource_document_kind_t document_kind;
  turbo_flow_resource_document_encoding_t encoding;
  const char *schema_name;
  const char *type_name;
  uint32_t schema_id;
  uint32_t schema_version;
  const char *schema_text;
} turbo_flow_resource_schema_t;

/**
 * Caller-owned immutable resource document. The schema pointer is borrowed;
 * payload is owned by this document until turbo_flow_resource_document_cleanup().
 * Initialize with TURBO_FLOW_RESOURCE_DOCUMENT_INIT before the first query.
 */
typedef struct turbo_flow_resource_document_s {
  size_t size;
  turbo_flow_domain_t domain;
  turbo_flow_resource_kind_t resource_kind;
  turbo_flow_resource_document_kind_t document_kind;
  char uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  char owner_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  uint64_t generation;
  uint64_t observed_generation;
  const turbo_flow_resource_schema_t *schema;
  mem_buffer_t *payload;
} turbo_flow_resource_document_t;

#define TURBO_FLOW_RESOURCE_DOCUMENT_INIT                                                          \
  {sizeof(turbo_flow_resource_document_t), TURBO_FLOW_DOMAIN_NONE, TURBO_FLOW_RESOURCE_CONNECTION, \
   TURBO_FLOW_RESOURCE_DOCUMENT_STATUS}

/** Build an owned immutable resource document by copying one bounded payload. */
TURBO_FLOW_C_API int turbo_flow_resource_document_set_payload_copy(
    turbo_flow_resource_document_t *document, const turbo_flow_resource_metadata_t *metadata,
    const turbo_flow_resource_schema_t *schema, const void *payload, size_t payload_size);

/** Validate one document against an exact trusted schema identity without parsing its payload. */
TURBO_FLOW_C_API int
turbo_flow_resource_document_validate(const turbo_flow_resource_document_t *document,
                                      const turbo_flow_resource_schema_t *expected_schema);

typedef enum turbo_flow_resource_condition_kind_e {
  TURBO_FLOW_RESOURCE_CONDITION_READY = 0,
  TURBO_FLOW_RESOURCE_CONDITION_ACCEPTING,
  TURBO_FLOW_RESOURCE_CONDITION_DRAINED,
  TURBO_FLOW_RESOURCE_CONDITION_SATURATED
} turbo_flow_resource_condition_kind_t;

typedef enum turbo_flow_condition_status_e {
  TURBO_FLOW_CONDITION_UNKNOWN = 0,
  TURBO_FLOW_CONDITION_FALSE,
  TURBO_FLOW_CONDITION_TRUE
} turbo_flow_condition_status_t;

typedef enum turbo_flow_resource_condition_reason_e {
  TURBO_FLOW_RESOURCE_REASON_NONE = 0,
  TURBO_FLOW_RESOURCE_REASON_RUNNING,
  TURBO_FLOW_RESOURCE_REASON_NOT_RUNNING,
  TURBO_FLOW_RESOURCE_REASON_ACCEPTING,
  TURBO_FLOW_RESOURCE_REASON_NOT_ACCEPTING,
  TURBO_FLOW_RESOURCE_REASON_DRAINED,
  TURBO_FLOW_RESOURCE_REASON_WORK_PENDING,
  TURBO_FLOW_RESOURCE_REASON_CAPACITY_AVAILABLE,
  TURBO_FLOW_RESOURCE_REASON_CAPACITY_EXHAUSTED,
  TURBO_FLOW_RESOURCE_REASON_OBSERVATION_CURRENT,
  TURBO_FLOW_RESOURCE_REASON_OBSERVATION_LAGGING,
  TURBO_FLOW_RESOURCE_REASON_OWNER_ERROR
} turbo_flow_resource_condition_reason_t;

typedef struct turbo_flow_resource_condition_s {
  turbo_flow_resource_condition_kind_t kind;
  turbo_flow_condition_status_t status;
  turbo_flow_resource_condition_reason_t reason;
} turbo_flow_resource_condition_t;

/** Caller-owned, generation-aware status for one runtime-owned pool resource. */
typedef struct turbo_flow_pool_resource_status_s {
  /** Caller sets this to sizeof(turbo_flow_pool_resource_status_t). */
  size_t size;
  turbo_flow_resource_kind_t resource_kind;
  char uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  char owner_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  uint64_t generation;
  uint64_t observed_generation;
  turbo_flow_pool_snapshot_t snapshot;
  uint32_t condition_count;
  turbo_flow_resource_condition_t conditions[TURBO_FLOW_RESOURCE_CONDITION_MAX];
} turbo_flow_pool_resource_status_t;

#define TURBO_FLOW_POOL_RESOURCE_STATUS_INIT                                                       \
  {sizeof(turbo_flow_pool_resource_status_t), TURBO_FLOW_RESOURCE_POOL}

typedef void (*turbo_flow_destroy_fn)(void *ptr, void *ctx);
/** On success, store one independently owned projection in `out`; leave it NULL on failure. */
typedef int (*turbo_flow_projection_clone_fn)(const void *value, void *ctx, void **out);

typedef enum turbo_flow_content_state_e {
  /** Payload bytes are available, but core has no schema-bound projection. */
  TURBO_FLOW_CONTENT_OPAQUE = 0,
  /** Payload bytes remain the source of truth and have an attached projection. */
  TURBO_FLOW_CONTENT_SCHEMA_BOUND
} turbo_flow_content_state_t;

typedef struct turbo_flow_failure_s {
  /** Owned stage name; NULL when the message has no captured failure. */
  tstr stage_name;
  /** Owned adapter name; NULL for callback-only stages. */
  tstr adapter_name;
  /** Owned name of the reject route that handled the failure. */
  tstr route_name;
  int code;
  /** First execution attempt is 1; retry policies increment this value. */
  uint32_t attempt;
} turbo_flow_failure_t;

#define TURBO_FLOW_DATA_DECISION_KEY_MAX 127u

typedef enum turbo_flow_data_evaluation_status_e {
  TURBO_FLOW_DATA_NOT_EVALUATED = 0,
  TURBO_FLOW_DATA_NOT_MATCHED,
  TURBO_FLOW_DATA_MATCHED,
  TURBO_FLOW_DATA_EVALUATION_ERROR
} turbo_flow_data_evaluation_status_t;

/** Runtime-owned decision sidecar produced by a typed data-rule stage. */
typedef struct turbo_flow_data_decision_s {
  size_t size;
  uint32_t stage_index;
  turbo_flow_data_evaluation_status_t evaluation_status;
  uint32_t match_count;
  int evaluation_error;
  int dropped;
  int dead_letter;
  int dead_letter_status;
  char route[TURBO_FLOW_DATA_DECISION_KEY_MAX + 1u];
  char batch_key[TURBO_FLOW_DATA_DECISION_KEY_MAX + 1u];
  char retry_class[TURBO_FLOW_DATA_DECISION_KEY_MAX + 1u];
} turbo_flow_data_decision_t;

#define TURBO_FLOW_DATA_DECISION_INIT                                                              \
  {sizeof(turbo_flow_data_decision_t), UINT32_MAX, TURBO_FLOW_DATA_NOT_EVALUATED, 0u, SALTS_OK,    \
   0, 0, SALTS_OK, {0}, {0}, {0}}

typedef struct turbo_flow_msg_s {
  uint64_t id;
  uint64_t ts_ns;
  uint32_t type;
  uint32_t flags;
  mem_buffer_t *buffer;
  vstr payload;
  tstr owned_payload;
  /**
   * Adapter context. An address inside `buffer` is message-owned and follows
   * that buffer across clone/move and asynchronous graph boundaries. Any other
   * address is borrowed, propagated inline, and never destroyed by the flow.
   */
  void *transport_context;
  /** Core-private message sidecar. Callers must use content/projection/route APIs. */
  void *_content_handle;
  int status;
  /** Current stage attempt, set by retry-aware adapters; zero outside execution. */
  uint32_t execution_attempt;
  turbo_flow_failure_t failure;
  turbo_flow_data_decision_t data_decision;
} turbo_flow_msg_t;

#define TURBO_FLOW_RETRY_MAX_ATTEMPTS 64u
#define TURBO_FLOW_RETRY_MAX_DELAY_MS 3600000u
#define TURBO_FLOW_REORDER_MAX_CAPACITY 1048576u
#define TURBO_FLOW_REORDER_MAX_TIMEOUT_MS 3600000u

typedef struct turbo_flow_retry_policy_s {
  uint32_t max_attempts;
  uint32_t delay_ms;
} turbo_flow_retry_policy_t;

typedef struct turbo_flow_reorder_config_s {
  /** Maximum number of out-of-order executions waiting at this boundary. */
  uint32_t capacity;
  /** Maximum wait for a missing sequence before failing with SALTS_ETIMEDOUT. */
  uint32_t timeout_ms;
} turbo_flow_reorder_config_t;

typedef int (*turbo_flow_retry_attempt_fn)(void *ctx, turbo_flow_msg_t *attempt_msg,
                                           uint32_t attempt);
typedef int (*turbo_flow_retryable_fn)(void *ctx, int status);
/** Return SALTS_OK after the delay or SALTS_ESHUTDOWN/SALTS_ECANCELED when stopping. */
typedef int (*turbo_flow_retry_wait_fn)(void *ctx, uint32_t delay_ms);

typedef struct turbo_flow_retry_ops_s {
  size_t size;
  turbo_flow_retry_attempt_fn attempt;
  turbo_flow_retryable_fn retryable;
  turbo_flow_retry_wait_fn wait;
} turbo_flow_retry_ops_t;

typedef int (*turbo_flow_stage_fn)(turbo_flow_msg_t *msg, void *ctx);
typedef int (*turbo_flow_const_stage_fn)(const turbo_flow_msg_t *msg, void *ctx);

/**
 * Produce zero or more owned output messages from one immutable input.
 *
 * Outputs remain private to the runtime until the callback returns SALTS_OK.
 * The emitter is valid only for the callback duration and enforces the bound
 * declared by its provider registration.
 */
typedef int (*turbo_flow_emitting_stage_fn)(const turbo_flow_msg_t *input,
                                            turbo_flow_emitter_t *emitter, void *ctx);

/**
 * Clone one self-contained output into the current bounded emission batch.
 * Returns SALTS_OK, SALTS_ENOSPC at the declared bound, SALTS_ENOTSUP for a
 * process-local capability, SALTS_EINVAL for invalid backing, or SALTS_ENOMEM.
 * The first failure is sticky and makes the whole callback batch fail.
 */
TURBO_FLOW_C_API int turbo_flow_emitter_emit_clone(turbo_flow_emitter_t *emitter,
                                            const turbo_flow_msg_t *output);

/**
 * Transfer one self-contained output into the current bounded emission batch.
 * `output` is reset only after the batch accepts it. Returns the same errors as
 * turbo_flow_emitter_emit_clone().
 */
TURBO_FLOW_C_API int turbo_flow_emitter_emit_move(turbo_flow_emitter_t *emitter, turbo_flow_msg_t *output);

/** Return the number of outputs accepted so far, or zero outside an active callback. */
TURBO_FLOW_C_API size_t turbo_flow_emitter_count(const turbo_flow_emitter_t *emitter);

#define TURBO_FLOW_KEYED_STATE_MAX_ENTRIES 1048576u
#define TURBO_FLOW_KEYED_STATE_MAX_KEY_SIZE 1048576u
#define TURBO_FLOW_KEYED_STATE_MAX_VALUE_SIZE 16777216u

typedef struct turbo_flow_keyed_state_store_config_s {
  size_t size;
  /** Maximum distinct key slots retained during one runtime generation. */
  size_t max_entries;
  size_t max_key_size;
  size_t max_value_size;
  /** Maximum aggregate bytes owned by retained keys and present values. */
  size_t max_total_bytes;
} turbo_flow_keyed_state_store_config_t;

#define TURBO_FLOW_KEYED_STATE_STORE_CONFIG_INIT                                                   \
  {sizeof(turbo_flow_keyed_state_store_config_t), 1024u, 256u, 65536u, 67108864u}

/**
 * Create one bounded in-memory keyed-state owner.
 *
 * The returned store is thread-safe for registered processors. It must outlive
 * every flow provider that borrows it and must not be destroyed while a flow is
 * started. Deleted key slots retain their key and revision until the next runtime
 * generation so commits require no allocation under the store lock and
 * insert-delete ABA conflicts remain visible.
 * Returns NULL for invalid bounds or allocation failure.
 */
TURBO_FLOW_C_API turbo_flow_keyed_state_store_t *
turbo_flow_keyed_state_store_create(const turbo_flow_keyed_state_store_config_t *config);

/** Destroy a store after all borrowing flows have stopped and been reset/destroyed. */
TURBO_FLOW_C_API void turbo_flow_keyed_state_store_destroy(turbo_flow_keyed_state_store_t *store);

/** Return the current number of present keys; serialize with store destruction. */
TURBO_FLOW_C_API size_t turbo_flow_keyed_state_store_size(const turbo_flow_keyed_state_store_t *store);

/** Return the immutable key selected for the current callback, or an empty view. */
TURBO_FLOW_C_API vstr turbo_flow_keyed_state_key(const turbo_flow_keyed_state_t *state);

/**
 * Read the callback-local value snapshot or pending PUT value.
 *
 * The returned view is borrowed until the next PUT/DELETE or callback return.
 * `revision` may be NULL. Returns SALTS_ENOENT when the key is absent/deleted,
 * SALTS_EBUSY outside an active callback, or SALTS_EINVAL for invalid output.
 */
TURBO_FLOW_C_API int turbo_flow_keyed_state_get(const turbo_flow_keyed_state_t *state, vstr *value,
                                         uint64_t *revision);

/**
 * Stage a copied value for atomic commit after callback success.
 * Returns SALTS_OK, SALTS_ENOSPC for configured bounds, SALTS_ENOMEM, or
 * SALTS_EBUSY outside an active callback. The first PUT failure is sticky.
 */
TURBO_FLOW_C_API int turbo_flow_keyed_state_put(turbo_flow_keyed_state_t *state, vstr value);

/** Stage deletion of an existing key; returns SALTS_ENOENT when absent. */
TURBO_FLOW_C_API int turbo_flow_keyed_state_delete(turbo_flow_keyed_state_t *state);

/** One bounded event-time tumbling-window store; keys and values are copied. */
typedef struct turbo_flow_event_time_window_store_config_s {
  size_t size;
  size_t max_windows;
  /** Maximum application key size; the runtime owns its internal window prefix. */
  size_t max_key_size;
  size_t max_value_size;
  /** Includes application keys, values, and the runtime's per-window timestamp prefix. */
  size_t max_total_bytes;
  uint64_t window_size_ns;
  uint64_t allowed_lateness_ns;
} turbo_flow_event_time_window_store_config_t;

#define TURBO_FLOW_EVENT_TIME_WINDOW_STORE_CONFIG_INIT                                             \
  {sizeof(turbo_flow_event_time_window_store_config_t),                                            \
   1024u,                                                                                          \
   256u,                                                                                           \
   65536u,                                                                                         \
   67108864u,                                                                                      \
   UINT64_C(60000000000),                                                                          \
   0u}

/** Borrowed immutable view valid only for the current event/close callback. */
typedef struct turbo_flow_event_time_window_s {
  size_t size;
  vstr key;
  uint64_t start_ns;
  uint64_t end_ns;
  /** Current aggregate before the event, or the final aggregate during close. */
  vstr aggregate;
} turbo_flow_event_time_window_t;

#define TURBO_FLOW_EVENT_TIME_WINDOW_INIT                                                          \
  {sizeof(turbo_flow_event_time_window_t), {NULL, 0u}, 0u, 0u, {NULL, 0u}}

/** Create a runtime-generation event-time store with fixed tumbling-window bounds. */
TURBO_FLOW_C_API turbo_flow_event_time_window_store_t *turbo_flow_event_time_window_store_create(
    const turbo_flow_event_time_window_store_config_t *config);

/** Destroy an unbound, stopped event-time store. */
TURBO_FLOW_C_API void
turbo_flow_event_time_window_store_destroy(turbo_flow_event_time_window_store_t *store);

/** Return the committed watermark, or zero and initialized=0 before its first advance. */
TURBO_FLOW_C_API uint64_t turbo_flow_event_time_window_watermark(
    const turbo_flow_event_time_window_store_t *store, int *initialized);

typedef int (*turbo_flow_key_selector_fn)(const turbo_flow_msg_t *message, vstr *key, void *ctx);
typedef int (*turbo_flow_keyed_stage_fn)(turbo_flow_msg_t *message, turbo_flow_keyed_state_t *state,
                                         void *ctx);
typedef int (*turbo_flow_keyed_emitting_stage_fn)(const turbo_flow_msg_t *input,
                                                  turbo_flow_keyed_state_t *state,
                                                  turbo_flow_emitter_t *emitter, void *ctx);
typedef int (*turbo_flow_event_time_window_stage_fn)(const turbo_flow_msg_t *input,
                                                     const turbo_flow_event_time_window_t *window,
                                                     turbo_flow_keyed_state_t *state, void *ctx);
typedef int (*turbo_flow_event_time_window_close_fn)(const turbo_flow_event_time_window_t *window,
                                                     turbo_flow_emitter_t *emitter, void *ctx);

typedef enum turbo_flow_settlement_action_e {
  TURBO_FLOW_SETTLEMENT_ACTION_COMPLETE = 1,
  TURBO_FLOW_SETTLEMENT_ACTION_RETRYABLE_FAILURE,
  TURBO_FLOW_SETTLEMENT_ACTION_REQUEUE,
  TURBO_FLOW_SETTLEMENT_ACTION_DEAD_LETTER,
  TURBO_FLOW_SETTLEMENT_ACTION_CANCELED,
  TURBO_FLOW_SETTLEMENT_ACTION_ACKNOWLEDGE
} turbo_flow_settlement_action_t;

/** One immutable data-attempt decision delivered synchronously to its state owner. */
typedef struct turbo_flow_settlement_result_s {
  size_t size;
  turbo_flow_settlement_action_t action;
  /** SALTS_OK for COMPLETE/ACKNOWLEDGE; the classified cause for terminal actions. */
  int status;
  uint32_t attempt;
  uint64_t message_id;
  uint64_t sequence;
} turbo_flow_settlement_result_t;

#define TURBO_FLOW_SETTLEMENT_RESULT_INIT                                                          \
  {sizeof(turbo_flow_settlement_result_t), 0, SALTS_OK, 0u, 0u, 0u}

/**
 * Report exactly one settlement decision from the current stage callback.
 *
 * The runtime validates the action against the registered operation contract
 * after the callback returns. The report is copied and owns no pointers.
 */
TURBO_FLOW_C_API int turbo_flow_settlement_report(const turbo_flow_settlement_result_t *result);

/**
 * Cooperatively yield the current stage task.
 *
 * Thread/disruptor tasks yield their OS time slice. Coroutine tasks suspend to
 * their scheduler. Returns SALTS_EINVAL outside an executor-owned stage task,
 * SALTS_ECANCELED when cancellation was requested, or SALTS_ETIMEDOUT when the
 * bound operation deadline expires.
 */
TURBO_FLOW_C_API int turbo_flow_execution_yield(void);

/** Request cooperative cancellation of the current stage task. */
TURBO_FLOW_C_API int turbo_flow_execution_abort(void);

/** Return nonzero when the current stage task has a cooperative cancel request. */
TURBO_FLOW_C_API int turbo_flow_execution_cancel_requested(void);

typedef enum turbo_flow_adapter_kind_e {
  TURBO_FLOW_ADAPTER_KIND_CUSTOM = 0,
  TURBO_FLOW_ADAPTER_KIND_SOCKET,
  TURBO_FLOW_ADAPTER_KIND_HTTP,
  TURBO_FLOW_ADAPTER_KIND_EMAIL,
  TURBO_FLOW_ADAPTER_KIND_FILE,
  TURBO_FLOW_ADAPTER_KIND_CODEC,
  TURBO_FLOW_ADAPTER_KIND_DATABIND,
  TURBO_FLOW_ADAPTER_KIND_RPC,
  TURBO_FLOW_ADAPTER_KIND_S3,
  TURBO_FLOW_ADAPTER_KIND_MESSAGE_BUS,
  TURBO_FLOW_ADAPTER_KIND_OBSERVE,
  TURBO_FLOW_ADAPTER_KIND_SCHEDULE,
  TURBO_FLOW_ADAPTER_KIND_QUEUE,
  TURBO_FLOW_ADAPTER_KIND_REDIS,
  TURBO_FLOW_ADAPTER_KIND_POSTGRESQL,
  TURBO_FLOW_ADAPTER_KIND_SQLITE
} turbo_flow_adapter_kind_t;

typedef enum turbo_flow_adapter_direction_e {
  TURBO_FLOW_ADAPTER_INPUT = 0,
  TURBO_FLOW_ADAPTER_OUTPUT,
  TURBO_FLOW_ADAPTER_BIDIRECTIONAL
} turbo_flow_adapter_direction_t;

#define TURBO_FLOW_ENDPOINT_MAX 255u

typedef enum turbo_flow_connection_state_e {
  TURBO_FLOW_CONNECTION_STOPPED = 0,
  TURBO_FLOW_CONNECTION_CONNECTING,
  TURBO_FLOW_CONNECTION_READY,
  TURBO_FLOW_CONNECTION_BACKOFF,
  TURBO_FLOW_CONNECTION_CLOSING,
  TURBO_FLOW_CONNECTION_FAILED
} turbo_flow_connection_state_t;

typedef struct turbo_flow_connection_snapshot_s {
  /** Registry-owned and valid until reset without keep_registry or destroy. */
  const char *adapter_name;
  turbo_flow_adapter_kind_t adapter_kind;
  turbo_flow_adapter_direction_t direction;
  turbo_flow_connection_state_t state;
  /** Caller-owned, NUL-terminated endpoint copied by the provider. */
  char endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
  uint64_t connections_current;
  uint64_t connection_limit;
  uint64_t in_flight_messages;
  uint64_t in_flight_bytes;
  int last_status;
} turbo_flow_connection_snapshot_t;

typedef int (*turbo_flow_adapter_connection_snapshot_fn)(void *ctx,
                                                         turbo_flow_connection_snapshot_t *out);

/** Pointer-free, generation-aware read-only projection for load aggregation. */
typedef struct turbo_flow_resource_snapshot_s {
  size_t size;
  turbo_flow_domain_t domain;
  turbo_flow_resource_kind_t kind;
  char uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  char owner_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  uint64_t generation;
  uint64_t observed_generation;
  uint64_t load;
  uint64_t capacity;
  int saturated;
  int last_status;
} turbo_flow_resource_snapshot_t;

#define TURBO_FLOW_RESOURCE_SNAPSHOT_INIT                                                          \
  {sizeof(turbo_flow_resource_snapshot_t), TURBO_FLOW_DOMAIN_NONE, TURBO_FLOW_RESOURCE_CONNECTION}

typedef int (*turbo_flow_resource_snapshot_fn)(void *ctx, turbo_flow_resource_snapshot_t *out);
typedef int (*turbo_flow_resource_metadata_fn)(void *ctx, turbo_flow_resource_metadata_t *out);
typedef int (*turbo_flow_resource_document_fn)(void *ctx,
                                               turbo_flow_resource_document_kind_t document_kind,
                                               turbo_flow_resource_document_t *out);

#define TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION 1u

typedef enum turbo_flow_managed_boundary_role_flags_e {
  TURBO_FLOW_MANAGED_BOUNDARY_SOURCE = 1u << 0,
  TURBO_FLOW_MANAGED_BOUNDARY_SINK = 1u << 1
} turbo_flow_managed_boundary_role_flags_t;

typedef enum turbo_flow_managed_boundary_capability_flags_e {
  TURBO_FLOW_MANAGED_BOUNDARY_DEMAND_AWARE = 1u << 0,
  TURBO_FLOW_MANAGED_BOUNDARY_REPLAYABLE = 1u << 1,
  TURBO_FLOW_MANAGED_BOUNDARY_DURABLE_SETTLEMENT = 1u << 2,
  TURBO_FLOW_MANAGED_BOUNDARY_MANUAL_REVIEW = 1u << 3
} turbo_flow_managed_boundary_capability_flags_t;

typedef enum turbo_flow_managed_boundary_command_flags_e {
  TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_QUIESCE = 1u << 0,
  TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_RESUME = 1u << 1,
  TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_REPLACE_ENDPOINT = 1u << 2
} turbo_flow_managed_boundary_command_flags_t;

typedef enum turbo_flow_managed_boundary_state_e {
  TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED = 0,
  TURBO_FLOW_MANAGED_BOUNDARY_STARTING,
  TURBO_FLOW_MANAGED_BOUNDARY_RUNNING,
  TURBO_FLOW_MANAGED_BOUNDARY_QUIESCING,
  TURBO_FLOW_MANAGED_BOUNDARY_QUIESCENT,
  TURBO_FLOW_MANAGED_BOUNDARY_DRAINING,
  TURBO_FLOW_MANAGED_BOUNDARY_STOPPING,
  TURBO_FLOW_MANAGED_BOUNDARY_STOPPED,
  TURBO_FLOW_MANAGED_BOUNDARY_FAILED
} turbo_flow_managed_boundary_state_t;

/** Immutable, pointer-free Source/Sink capability contract copied at registration. */
typedef struct turbo_flow_managed_boundary_descriptor_s {
  size_t size;
  uint32_t version;
  turbo_flow_domain_t domain;
  turbo_flow_resource_kind_t kind;
  char uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  char owner_name[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  /** Bitwise turbo_flow_managed_boundary_role_flags_t values. */
  uint32_t role_flags;
  /** Bitwise turbo_flow_managed_boundary_capability_flags_t values. */
  uint32_t capability_flags;
  /** Bitwise command flags; zero declares a read-only boundary. */
  uint32_t command_flags;
  /** Required when the SINK role is present. */
  turbo_flow_content_descriptor_t input;
  /** Required when the SOURCE role is present. */
  turbo_flow_content_descriptor_t output;
} turbo_flow_managed_boundary_descriptor_t;

#define TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT                                                \
  {sizeof(turbo_flow_managed_boundary_descriptor_t),                                               \
   TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION,                                                        \
   TURBO_FLOW_DOMAIN_NONE,                                                                         \
   TURBO_FLOW_RESOURCE_CONNECTION,                                                                 \
   {0},                                                                                            \
   {0},                                                                                            \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   TURBO_FLOW_CONTENT_DESCRIPTOR_INIT,                                                             \
   TURBO_FLOW_CONTENT_DESCRIPTOR_INIT}

/** Owner-synchronized, pointer-free operational projection for one managed boundary. */
typedef struct turbo_flow_managed_boundary_snapshot_s {
  size_t size;
  uint32_t version;
  char uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  uint64_t generation;
  uint64_t observed_generation;
  turbo_flow_managed_boundary_state_t state;
  uint64_t demand;
  uint64_t queue_depth;
  uint64_t queue_capacity;
  uint64_t in_flight;
  uint64_t lag;
  /** Items admitted to the boundary data path. */
  uint64_t accepted;
  /** Admitted items that reached an owner-defined terminal outcome. */
  uint64_t completed;
  /** Items rejected before admission; independent of accepted/completed. */
  uint64_t rejected;
  int backpressured;
  int last_status;
} turbo_flow_managed_boundary_snapshot_t;

#define TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT                                                  \
  {sizeof(turbo_flow_managed_boundary_snapshot_t), TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION}

typedef int (*turbo_flow_managed_boundary_descriptor_fn)(
    void *ctx, turbo_flow_managed_boundary_descriptor_t *out);
typedef int (*turbo_flow_managed_boundary_snapshot_fn)(
    void *ctx, turbo_flow_managed_boundary_snapshot_t *out);

/** Legacy adapter-scoped command kinds retained for source and ABI compatibility. */
typedef enum turbo_flow_adapter_command_kind_e {
  TURBO_FLOW_ADAPTER_QUIESCE = 1,
  TURBO_FLOW_ADAPTER_RESUME,
  TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT
} turbo_flow_adapter_command_kind_t;

typedef struct turbo_flow_adapter_endpoint_s {
  /** Required for network transports and ignored by path-only transports. */
  const char *host;
  int port;
  /** Required for path-only transports; optional protocol path otherwise. */
  const char *path;
} turbo_flow_adapter_endpoint_t;

/**
 * Legacy adapter-scoped command payload.
 *
 * New controllable adapters expose a stable resource provider and accept
 * turbo_flow_resource_command_t through its command callback.
 */
typedef struct turbo_flow_adapter_command_s {
  /** Set to sizeof(turbo_flow_adapter_command_t). */
  size_t size;
  turbo_flow_adapter_command_kind_t kind;
  turbo_flow_adapter_endpoint_t endpoint;
} turbo_flow_adapter_command_t;

typedef enum turbo_flow_resource_command_kind_e {
  TURBO_FLOW_RESOURCE_COMMAND_QUIESCE = 1,
  TURBO_FLOW_RESOURCE_COMMAND_RESUME,
  TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT,
  TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL
} turbo_flow_resource_command_kind_t;

/** Versioned, pointer-free command envelope checked by the host dispatcher. */
typedef struct turbo_flow_resource_command_s {
  size_t size;
  turbo_flow_resource_command_kind_t kind;
  char target_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  char idempotency_key[TURBO_FLOW_RESOURCE_COMMAND_KEY_MAX + 1u];
  uint64_t expected_generation;
  /** Absolute salts_hrtime() deadline; UINT64_MAX means no deadline. */
  uint64_t deadline_ns;
  uint32_t parallelism;
  /** Resize-only owner drain budget; UINT64_MAX waits without a drain deadline. */
  uint64_t drain_timeout_ms;
  char endpoint_host[TURBO_FLOW_ENDPOINT_MAX + 1u];
  char endpoint_path[TURBO_FLOW_ENDPOINT_MAX + 1u];
  int endpoint_port;
} turbo_flow_resource_command_t;

#define TURBO_FLOW_RESOURCE_COMMAND_INIT                                                           \
  {sizeof(turbo_flow_resource_command_t), 0, {0}, {0}, 0u, UINT64_MAX, 0u, UINT64_MAX, {0}, {0}, 0}

typedef struct turbo_flow_resource_command_result_s {
  size_t size;
  int status;
  int replayed;
  uint64_t generation_before;
  uint64_t generation_after;
  uint64_t observed_generation;
} turbo_flow_resource_command_result_t;

#define TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT                                                    \
  {sizeof(turbo_flow_resource_command_result_t), SALTS_OK, 0, 0u, 0u, 0u}

typedef enum turbo_flow_resource_reconcile_action_e {
  TURBO_FLOW_RESOURCE_RECONCILE_CONVERGED = 0,
  TURBO_FLOW_RESOURCE_RECONCILE_OBSERVING,
  TURBO_FLOW_RESOURCE_RECONCILE_COMMAND_APPLIED,
  TURBO_FLOW_RESOURCE_RECONCILE_COMMAND_FAILED
} turbo_flow_resource_reconcile_action_t;

/**
 * One immutable observed resource value and its desired state.
 *
 * `observed_value` is owner-defined typed scalar state, such as pool parallelism. Conditions are
 * matched by kind. The command is copied and must already contain a unique idempotency key and the
 * same target UID as `metadata`; reconcile supplies the checked observed generation.
 */
typedef struct turbo_flow_resource_reconcile_request_s {
  size_t size;
  turbo_flow_resource_metadata_t metadata;
  uint64_t observed_value;
  uint64_t desired_value;
  uint32_t condition_count;
  turbo_flow_resource_condition_t conditions[TURBO_FLOW_RESOURCE_CONDITION_MAX];
  uint32_t desired_condition_count;
  turbo_flow_resource_condition_t desired_conditions[TURBO_FLOW_RESOURCE_CONDITION_MAX];
  turbo_flow_resource_command_t command;
} turbo_flow_resource_reconcile_request_t;

#define TURBO_FLOW_RESOURCE_RECONCILE_REQUEST_INIT                                                 \
  {sizeof(turbo_flow_resource_reconcile_request_t),                                                \
   TURBO_FLOW_RESOURCE_METADATA_INIT,                                                              \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   {{0}},                                                                                          \
   0u,                                                                                             \
   {{0}},                                                                                          \
   TURBO_FLOW_RESOURCE_COMMAND_INIT}

typedef struct turbo_flow_resource_reconcile_result_s {
  size_t size;
  turbo_flow_resource_reconcile_action_t action;
  int status;
  int value_matches;
  uint32_t matched_condition_count;
  turbo_flow_resource_command_result_t command_result;
} turbo_flow_resource_reconcile_result_t;

#define TURBO_FLOW_RESOURCE_RECONCILE_RESULT_INIT                                                  \
  {sizeof(turbo_flow_resource_reconcile_result_t),                                                 \
   TURBO_FLOW_RESOURCE_RECONCILE_CONVERGED,                                                        \
   SALTS_OK,                                                                                       \
   0,                                                                                              \
   0u,                                                                                             \
   TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT}

#define TURBO_FLOW_RESIZE_WORKFLOW_ID_MAX 95u

typedef enum turbo_flow_resize_workflow_phase_e {
  TURBO_FLOW_RESIZE_WORKFLOW_QUIESCE_INGRESS = 0,
  TURBO_FLOW_RESIZE_WORKFLOW_DRAIN_GRAPH,
  TURBO_FLOW_RESIZE_WORKFLOW_RESIZE_POOL,
  TURBO_FLOW_RESIZE_WORKFLOW_RESUME_GRAPH,
  TURBO_FLOW_RESIZE_WORKFLOW_RESUME_INGRESS,
  TURBO_FLOW_RESIZE_WORKFLOW_DONE,
  TURBO_FLOW_RESIZE_WORKFLOW_FAILED
} turbo_flow_resize_workflow_phase_t;

/** Fixed, pointer-free workflow specification. The caller resolves and supplies both generations.
 */
typedef struct turbo_flow_resize_workflow_spec_s {
  size_t size;
  char workflow_id[TURBO_FLOW_RESIZE_WORKFLOW_ID_MAX + 1u];
  char ingress_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  char pool_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  uint64_t ingress_generation;
  uint64_t pool_generation;
  uint32_t parallelism;
  uint64_t drain_timeout_ms;
  /** Absolute salts_hrtime() deadline; UINT64_MAX means no deadline. */
  uint64_t deadline_ns;
} turbo_flow_resize_workflow_spec_t;

#define TURBO_FLOW_RESIZE_WORKFLOW_SPEC_INIT                                                       \
  {sizeof(turbo_flow_resize_workflow_spec_t), {0}, {0}, {0}, 0u, 0u, 0u, UINT64_MAX, UINT64_MAX}

/** Caller-owned workflow state. It contains only control metadata and never stores payload data. */
typedef struct turbo_flow_resize_workflow_state_s {
  size_t size;
  turbo_flow_resize_workflow_spec_t spec;
  turbo_flow_resize_workflow_phase_t phase;
  turbo_flow_resize_workflow_phase_t failed_phase;
  uint32_t attempt;
  uint32_t commands_applied;
  uint64_t ingress_generation;
  uint64_t pool_generation;
  int last_status;
} turbo_flow_resize_workflow_state_t;

#define TURBO_FLOW_RESIZE_WORKFLOW_STATE_INIT                                                      \
  {sizeof(turbo_flow_resize_workflow_state_t),                                                     \
   TURBO_FLOW_RESIZE_WORKFLOW_SPEC_INIT,                                                           \
   TURBO_FLOW_RESIZE_WORKFLOW_QUIESCE_INGRESS,                                                     \
   TURBO_FLOW_RESIZE_WORKFLOW_QUIESCE_INGRESS,                                                     \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   0u,                                                                                             \
   SALTS_OK}

typedef enum turbo_flow_resize_workflow_action_e {
  TURBO_FLOW_RESIZE_WORKFLOW_NOOP = 0,
  TURBO_FLOW_RESIZE_WORKFLOW_COMMAND_APPLIED,
  TURBO_FLOW_RESIZE_WORKFLOW_GRAPH_DRAINED,
  TURBO_FLOW_RESIZE_WORKFLOW_GRAPH_RESUMED,
  TURBO_FLOW_RESIZE_WORKFLOW_STEP_FAILED
} turbo_flow_resize_workflow_action_t;

typedef struct turbo_flow_resize_workflow_result_s {
  size_t size;
  turbo_flow_resize_workflow_action_t action;
  turbo_flow_resize_workflow_phase_t phase_before;
  turbo_flow_resize_workflow_phase_t phase_after;
  int status;
  turbo_flow_resource_command_result_t command_result;
} turbo_flow_resize_workflow_result_t;

#define TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT                                                     \
  {sizeof(turbo_flow_resize_workflow_result_t),                                                    \
   TURBO_FLOW_RESIZE_WORKFLOW_NOOP,                                                                \
   TURBO_FLOW_RESIZE_WORKFLOW_QUIESCE_INGRESS,                                                     \
   TURBO_FLOW_RESIZE_WORKFLOW_QUIESCE_INGRESS,                                                     \
   SALTS_OK,                                                                                       \
   TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT}

typedef int (*turbo_flow_resource_command_apply_fn)(void *ctx, turbo_flow_t *flow,
                                                    const turbo_flow_resource_command_t *command);

/**
 * One owner-native resource provider. A flow may register multiple providers
 * for the same owner. All callbacks are read-only except `command` and the
 * provider context remains owned by the registering module.
 */
typedef struct turbo_flow_resource_provider_ops_s {
  size_t size;
  turbo_flow_resource_metadata_fn metadata;
  turbo_flow_resource_snapshot_fn snapshot;
  turbo_flow_resource_document_fn document;
  turbo_flow_resource_command_apply_fn command;
} turbo_flow_resource_provider_ops_t;

#define TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT                                                      \
  {sizeof(turbo_flow_resource_provider_ops_t), NULL, NULL, NULL, NULL}

/** Additive provider contract that registers into the canonical resource registry. */
typedef struct turbo_flow_managed_boundary_provider_ops_s {
  size_t size;
  uint32_t version;
  turbo_flow_resource_provider_ops_t resource;
  turbo_flow_managed_boundary_descriptor_fn descriptor;
  turbo_flow_managed_boundary_snapshot_fn snapshot;
} turbo_flow_managed_boundary_provider_ops_t;

#define TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT                                              \
  {sizeof(turbo_flow_managed_boundary_provider_ops_t),                                             \
   TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION,                                                        \
   TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT,                                                          \
   NULL,                                                                                           \
   NULL}

typedef struct turbo_flow_resource_provider_registration_s {
  size_t size;
  const char *owner_name;
  turbo_flow_resource_provider_ops_t ops;
  void *ctx;
} turbo_flow_resource_provider_registration_t;

#define TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT                                             \
  {sizeof(turbo_flow_resource_provider_registration_t), NULL,                                      \
   TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT, NULL}

typedef int (*turbo_flow_adapter_command_fn)(void *ctx, turbo_flow_t *flow,
                                             const turbo_flow_adapter_command_t *command);

typedef int (*turbo_flow_settlement_apply_fn)(void *ctx, turbo_flow_t *flow,
                                              const turbo_flow_stage_plan_t *stage,
                                              const turbo_flow_msg_t *msg,
                                              const turbo_flow_settlement_result_t *result);

typedef struct turbo_flow_settlement_owner_ops_s {
  size_t size;
  /** Apply one owner-local decision synchronously; core never mutates owner state directly. */
  turbo_flow_settlement_apply_fn apply;
} turbo_flow_settlement_owner_ops_t;

#define TURBO_FLOW_SETTLEMENT_OWNER_OPS_INIT {sizeof(turbo_flow_settlement_owner_ops_t), NULL}

/**
 * Thin binding for an already-claimed storage record.
 *
 * The binding owns neither `ctx` nor a payload. Calls must follow the bound owner's threading
 * contract. A successful callback is terminal for that claim token; a failure leaves it active.
 */
typedef int (*turbo_flow_claim_settle_fn)(void *ctx, uint64_t token);

typedef enum turbo_flow_claim_commit_action_e {
  TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY = 0,
  TURBO_FLOW_CLAIM_COMMIT_ACK,
  TURBO_FLOW_CLAIM_COMMIT_REQUEUE,
  TURBO_FLOW_CLAIM_COMMIT_DROP
} turbo_flow_claim_commit_action_t;

/**
 * Load one owner-local durable state snapshot.
 *
 * SALTS_ENOENT means the key does not exist. SALTS_ENOSPC reports the required
 * size through `out_size`. The returned bytes are opaque to the claim owner.
 */
typedef int (*turbo_flow_claim_state_load_fn)(void *ctx, const char *key, uint8_t *out,
                                              size_t capacity, size_t *out_size);

/**
 * Atomically commit one claim disposition and its new owner snapshot.
 *
 * `token` must name an active claim for ACK/REQUEUE/DROP and must be zero for
 * STATE_ONLY. Success is the durable commit point. An uncertain result must be
 * retryable with the same arguments; providers therefore make an exact retry
 * idempotent. The callback never takes ownership of key or state bytes.
 */
typedef int (*turbo_flow_claim_state_commit_fn)(void *ctx, uint64_t token,
                                                turbo_flow_claim_commit_action_t action,
                                                const char *key, const uint8_t *state,
                                                size_t state_size);

typedef struct turbo_flow_claim_settler_s {
  size_t size;
  void *ctx;
  turbo_flow_claim_settle_fn ack;
  turbo_flow_claim_settle_fn requeue;
  turbo_flow_claim_settle_fn drop;
  /** Zero when this settler provides only volatile claim settlement. */
  size_t max_state_size;
  turbo_flow_claim_state_load_fn load_state;
  turbo_flow_claim_state_commit_fn commit_state;
} turbo_flow_claim_settler_t;

#define TURBO_FLOW_CLAIM_SETTLER_INIT                                                              \
  {sizeof(turbo_flow_claim_settler_t), NULL, NULL, NULL, NULL, 0u, NULL, NULL}

typedef struct turbo_flow_adapter_ops_s {
  /**
   * Start one source/sink adapter binding.
   *
   * The flow data planes and executors are already created, but external
   * ingress should not publish messages until turbo_flow_start() returns.
   */
  int (*start)(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage);
  /**
   * Consume one message delivered to an adapter-backed stage.
   *
   * The MVP adapter path is synchronous: return only after the adapter no
   * longer depends on the message envelope owned by the current flow dispatch.
   */
  int (*consume)(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                 turbo_flow_msg_t *msg);
  /** Adapter-owned bounded retry path, required only when stage retry is configured. */
  int (*consume_retry)(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                       turbo_flow_msg_t *msg, const turbo_flow_retry_policy_t *policy);
  /** Stop one active source/sink adapter binding during turbo_flow_stop(). */
  void (*stop)(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage);
  /** Release adapter-wide resources when the adapter registry is cleared. */
  void (*shutdown)(void *ctx);
  /** Optional lock-free or internally synchronized connection/resource snapshot. */
  turbo_flow_adapter_connection_snapshot_fn connection_snapshot;
  /**
   * @deprecated Compatibility callback for adapters without a stable resource provider.
   * New controllable adapters implement turbo_flow_resource_provider_ops_t::command.
   */
  turbo_flow_adapter_command_fn command;
} turbo_flow_adapter_ops_t;

/**
 * Report a non-success status from the currently executing adapter stop callback.
 *
 * This function must be called synchronously on the callback's own thread. The
 * first reported status is returned by `turbo_flow_stop()`. Before reporting, the
 * adapter must settle every Flow claim it owns. A failed adapter remains active for
 * a later `turbo_flow_stop()` retry; adapters that stopped successfully are not
 * invoked again during that retry.
 *
 * @return `SALTS_OK`, or `SALTS_EINVAL` outside a stop callback or for `SALTS_OK`.
 */
TURBO_FLOW_C_API int turbo_flow_adapter_report_stop_status(turbo_flow_t *flow, int status);

#define TURBO_FLOW_ASYNC_TERMINAL_API_VERSION 1u

/**
 * Move-only ownership of one deferred terminal stage attempt.
 *
 * The core creates a claim for an asynchronous publication. A successful
 * adapter submit callback moves it to an explicit owner. Completing the moved
 * claim releases its retained message and invalidates the claim value.
 */
typedef struct turbo_flow_async_terminal_claim_s {
  size_t size;
  uint32_t version;
  void *_impl;
} turbo_flow_async_terminal_claim_t;

#define TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT                                                       \
  {sizeof(turbo_flow_async_terminal_claim_t), TURBO_FLOW_ASYNC_TERMINAL_API_VERSION, NULL}

typedef int (*turbo_flow_async_terminal_submit_fn)(void *ctx, turbo_flow_t *flow,
                                                   const turbo_flow_stage_plan_t *stage,
                                                   const turbo_flow_msg_t *message,
                                                   turbo_flow_async_terminal_claim_t *claim);

typedef struct turbo_flow_async_terminal_adapter_ops_s {
  size_t size;
  uint32_t version;
  turbo_flow_async_terminal_submit_fn submit;
} turbo_flow_async_terminal_adapter_ops_t;

#define TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT                                                 \
  {sizeof(turbo_flow_async_terminal_adapter_ops_t), TURBO_FLOW_ASYNC_TERMINAL_API_VERSION, NULL}

/** Move a live claim; rejection leaves both claim values unchanged. */
TURBO_FLOW_C_API int
turbo_flow_async_terminal_claim_move(turbo_flow_async_terminal_claim_t *destination,
                                     turbo_flow_async_terminal_claim_t *source);

/** Borrow the retained message while the claim remains live. */
TURBO_FLOW_C_API const turbo_flow_msg_t *
turbo_flow_async_terminal_claim_message(const turbo_flow_async_terminal_claim_t *claim);

/**
 * Consume one live claim with its authoritative terminal result.
 *
 * `settlement` is copied before return and may be NULL when the stage does not
 * report an explicit settlement action.
 */
TURBO_FLOW_C_API int
turbo_flow_async_terminal_complete(turbo_flow_async_terminal_claim_t *claim, int status,
                                   const turbo_flow_settlement_result_t *settlement);

#define TURBO_FLOW_ASYNC_EMIT_API_VERSION 1u

/**
 * Move-only ownership of one deferred 0..1-output stage attempt.
 *
 * A successful adapter submission must move the claim into exactly one owner.
 * Completion with a message moves one self-contained output into the graph and
 * resumes downstream from the deferred stage. Completion with `SALTS_OK` and a
 * NULL output represents an empty flat-map result. Any non-OK status is a
 * terminal publication error and requires a NULL output.
 */
typedef struct turbo_flow_async_emit_claim_s {
  size_t size;
  uint32_t version;
  void *_impl;
} turbo_flow_async_emit_claim_t;

#define TURBO_FLOW_ASYNC_EMIT_CLAIM_INIT                                                           \
  {sizeof(turbo_flow_async_emit_claim_t), TURBO_FLOW_ASYNC_EMIT_API_VERSION, NULL}

typedef int (*turbo_flow_async_emit_submit_fn)(void *ctx, turbo_flow_t *flow,
                                               const turbo_flow_stage_plan_t *stage,
                                               const turbo_flow_msg_t *message,
                                               turbo_flow_async_emit_claim_t *claim);

typedef struct turbo_flow_async_emit_adapter_ops_s {
  size_t size;
  uint32_t version;
  turbo_flow_async_emit_submit_fn submit;
} turbo_flow_async_emit_adapter_ops_t;

#define TURBO_FLOW_ASYNC_EMIT_ADAPTER_OPS_INIT                                                     \
  {sizeof(turbo_flow_async_emit_adapter_ops_t), TURBO_FLOW_ASYNC_EMIT_API_VERSION, NULL}

/** Move a live deferred-output claim; rejection leaves both values unchanged. */
TURBO_FLOW_C_API int
turbo_flow_async_emit_claim_move(turbo_flow_async_emit_claim_t *destination,
                                 turbo_flow_async_emit_claim_t *source);

/** Borrow the retained input message while the claim remains live. */
TURBO_FLOW_C_API const turbo_flow_msg_t *
turbo_flow_async_emit_claim_message(const turbo_flow_async_emit_claim_t *claim);

/**
 * Consume one live claim exactly once.
 *
 * On successful validation a non-NULL output is moved and reset before return.
 * The publication's terminal status is delivered through its asynchronous
 * completion callback after all resumed downstream work settles.
 */
TURBO_FLOW_C_API int turbo_flow_async_emit_complete(turbo_flow_async_emit_claim_t *claim,
                                                    int status, turbo_flow_msg_t *output);

/**
 * Core-owned iterator for one native adapter batch.
 *
 * The adapter calls next exactly once for each index in ascending order. On
 * SALTS_OK, message owns an independent clone/retained view and must be cleaned
 * with turbo_flow_msg_cleanup() before requesting the next item. On failure,
 * message is initialized but empty and the batch must stop at that index.
 */
typedef int (*turbo_flow_adapter_batch_next_fn)(void *ctx, size_t index,
                                                turbo_flow_msg_t *message);

typedef struct turbo_flow_adapter_batch_s {
  size_t size;
  size_t message_count;
  turbo_flow_adapter_batch_next_fn next;
  void *ctx;
} turbo_flow_adapter_batch_t;

#define TURBO_FLOW_ADAPTER_BATCH_INIT {sizeof(turbo_flow_adapter_batch_t), 0u, NULL, NULL}

/**
 * Optional native batch consumer for a direct terminal adapter stage.
 *
 * Core invokes this callback only when one source is connected directly to one
 * inline terminal adapter stage without observers, retry, reorder, deadline, or
 * settlement behavior. The callback processes messages in ascending order and
 * stops at the first prepare or consume failure. consumed reports only
 * successful items before that failure.
 */
typedef int (*turbo_flow_adapter_consume_batch_fn)(
    void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
    const turbo_flow_adapter_batch_t *batch, size_t *consumed);

typedef enum turbo_flow_adapter_event_e {
  TURBO_FLOW_ADAPTER_EVENT_START = 0,
  TURBO_FLOW_ADAPTER_EVENT_STOP
} turbo_flow_adapter_event_t;

/**
 * Optional read-only instrumentation callbacks.
 *
 * Concurrent publications may invoke callbacks concurrently. Implementations
 * must synchronize mutable observer state and must not re-enter the same flow.
 */
typedef struct turbo_flow_observer_ops_s {
  size_t size;
  void (*message_complete)(void *ctx, const char *source_name, const turbo_flow_msg_t *msg,
                           uint64_t duration_ns, int status);
  void (*stage_complete)(void *ctx, const char *stage_name, const char *adapter_name,
                         const turbo_flow_msg_t *msg, uint64_t duration_ns, int status);
  void (*adapter_event)(void *ctx, const char *stage_name, const char *adapter_name,
                        turbo_flow_adapter_event_t event, int status);
  /** Called after adapter shutdown and before the flow storage is released. */
  void (*flow_destroyed)(void *ctx);
} turbo_flow_observer_ops_t;

typedef enum turbo_flow_observe_event_kind_e {
  TURBO_FLOW_OBSERVE_SOURCE_RECEIVED = 0,
  TURBO_FLOW_OBSERVE_STAGE_BEGIN,
  TURBO_FLOW_OBSERVE_STAGE_END,
  TURBO_FLOW_OBSERVE_ROUTE_EVALUATED,
  TURBO_FLOW_OBSERVE_SINK_COMPLETE,
  TURBO_FLOW_OBSERVE_ADAPTER_START,
  TURBO_FLOW_OBSERVE_ADAPTER_STOP,
  TURBO_FLOW_OBSERVE_FLOW_COMPLETE,
  TURBO_FLOW_OBSERVE_EVENT_COUNT
} turbo_flow_observe_event_kind_t;

#define TURBO_FLOW_OBSERVE_EVENT_MASK(kind) (UINT64_C(1) << (uint32_t)(kind))
#define TURBO_FLOW_OBSERVE_ALL_EVENTS                                                        \
  ((UINT64_C(1) << (uint32_t)TURBO_FLOW_OBSERVE_EVENT_COUNT) - UINT64_C(1))
#define TURBO_FLOW_MAX_EVENT_OBSERVERS 64u

/**
 * One immutable runtime observation borrowed for the duration of on_event().
 *
 * selected is -1 outside route events, otherwise zero or one. edge_kind is -1
 * outside route events. All strings and msg are flow-owned borrowed views.
 */
typedef struct turbo_flow_observe_event_s {
  size_t size;
  turbo_flow_observe_event_kind_t kind;
  const char *source_name;
  const char *stage_name;
  const char *adapter_name;
  const char *operation_name;
  const char *from_name;
  const char *to_name;
  const char *route_name;
  const turbo_flow_msg_t *msg;
  int status;
  int selected;
  int edge_kind;
  uint32_t attempt;
  uint64_t timestamp_ns;
  uint64_t duration_ns;
} turbo_flow_observe_event_t;

typedef int (*turbo_flow_observer_on_event_fn)(void *ctx,
                                               const turbo_flow_observe_event_t *event);

/**
 * One named structured Observer.
 *
 * on_event() is synchronous and may run concurrently for concurrent
 * publications. It must not mutate or re-enter the flow. A non-OK result is
 * counted but never changes Graph execution. If destroy is present, a
 * successful registration transfers ctx lifecycle to the flow.
 */
typedef struct turbo_flow_event_observer_ops_s {
  size_t size;
  uint64_t event_mask;
  turbo_flow_observer_on_event_fn on_event;
  void (*destroy)(void *ctx);
} turbo_flow_event_observer_ops_t;

#define TURBO_FLOW_EVENT_OBSERVER_OPS_INIT \
  {sizeof(turbo_flow_event_observer_ops_t), TURBO_FLOW_OBSERVE_ALL_EVENTS, NULL, NULL}

typedef enum turbo_flow_option_type_e {
  TURBO_FLOW_OPTION_BOOL = 0,
  TURBO_FLOW_OPTION_U32,
  TURBO_FLOW_OPTION_U64,
  TURBO_FLOW_OPTION_SIZE,
  TURBO_FLOW_OPTION_STRING,
  TURBO_FLOW_OPTION_STRING_LIST,
  TURBO_FLOW_OPTION_STRING_MAP,
  TURBO_FLOW_OPTION_ENUM,
  TURBO_FLOW_OPTION_ENUM_SET,
  TURBO_FLOW_OPTION_DURATION_MS,
  TURBO_FLOW_OPTION_PATH,
  TURBO_FLOW_OPTION_SECRET,
  TURBO_FLOW_OPTION_SCHEMA_TEXT,
  TURBO_FLOW_OPTION_SCHEMA_PATH,
  /** Host-owned pointer or handle; never accepted from serialized configuration. */
  TURBO_FLOW_OPTION_HOST_OBJECT
} turbo_flow_option_type_t;

typedef enum turbo_flow_option_flags_e {
  TURBO_FLOW_OPTION_REQUIRED = 1u << 0,
  TURBO_FLOW_OPTION_SECRET_VALUE = 1u << 1,
  TURBO_FLOW_OPTION_NOT_SERIALIZABLE = 1u << 2,
  TURBO_FLOW_OPTION_HAS_MIN = 1u << 3,
  TURBO_FLOW_OPTION_HAS_MAX = 1u << 4
} turbo_flow_option_flags_t;

typedef struct turbo_flow_option_field_s {
  const char *name;
  turbo_flow_option_type_t type;
  uint32_t flags;
  uint64_t min_value;
  uint64_t max_value;
  const char *const *enum_values;
  size_t enum_value_count;
} turbo_flow_option_field_t;

typedef enum turbo_flow_adapter_role_e {
  TURBO_FLOW_ADAPTER_SOURCE = 1u << 0,
  TURBO_FLOW_ADAPTER_SINK = 1u << 1,
  TURBO_FLOW_ADAPTER_TRANSFORM = 1u << 2
} turbo_flow_adapter_role_t;

typedef struct turbo_flow_adapter_schema_s {
  /** Populated by the registry; callers pass NULL when registering a schema. */
  const char *binding_name;
  turbo_flow_adapter_kind_t kind;
  uint32_t roles;
  turbo_flow_adapter_direction_t direction;
  const turbo_flow_option_field_t *fields;
  size_t field_count;
} turbo_flow_adapter_schema_t;

#define TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_API_VERSION 1u

/**
 * One explicit owner contract for an async-terminal adapter and its managed
 * Sink boundary. The registry copies names, schemas, and the boundary
 * descriptor. The input structure and nested tables are borrowed only for the
 * call; callback code and `ctx` must remain valid until registry teardown after
 * a successful registration. `ctx` remains caller-owned on failure.
 */
typedef struct turbo_flow_managed_async_terminal_registration_s {
  size_t size;
  uint32_t version;
  const char *adapter_name;
  const turbo_flow_adapter_ops_t *adapter_ops;
  const turbo_flow_async_terminal_adapter_ops_t *async_ops;
  const turbo_flow_adapter_schema_t *schema;
  const char *owner_name;
  const turbo_flow_managed_boundary_provider_ops_t *boundary_ops;
  void *ctx;
} turbo_flow_managed_async_terminal_registration_t;

#define TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_INIT                                       \
  {sizeof(turbo_flow_managed_async_terminal_registration_t),                                      \
   TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_API_VERSION,                                    \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

/**
 * Atomically register one native adapter and bind its executable operations to
 * their module owner. The registry copies all names and schemas; callback and
 * resource contexts remain owned by the registering module.
 *
 * Every operation must already be exported by `module_name` and match the
 * adapter role/callback used to execute it. Adapter-owned operations leave the
 * parallel resource entry NULL. Resource/protocol-owner operations name a
 * compatible primitive exported by the module; missing primitive instances may
 * be supplied through `primitives`. A failed registration leaves no adapter,
 * resource provider, primitive, or operation binding behind. `ctx` remains
 * caller-owned on failure and transfers to registry shutdown ownership only on
 * success.
 */
typedef struct turbo_flow_module_adapter_registration_s {
  size_t size;
  const char *module_name;
  const char *adapter_name;
  const turbo_flow_adapter_ops_t *ops;
  void *ctx;
  const turbo_flow_adapter_schema_t *schema;
  const char *const *operation_names;
  size_t operation_count;
  const turbo_flow_resource_provider_registration_t *resources;
  size_t resource_count;
  /** Optional parallel array; NULL entries mean the operation is adapter-owned. */
  const char *const *operation_resource_names;
  /** Primitive instances to register atomically before adapter ownership transfers. */
  const turbo_flow_primitive_descriptor_t *primitives;
  size_t primitive_count;
  /** Optional native direct-terminal batch consumer. */
  turbo_flow_adapter_consume_batch_fn consume_batch;
} turbo_flow_module_adapter_registration_t;

#define TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT                                                \
  {sizeof(turbo_flow_module_adapter_registration_t),                                               \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL}

#define TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_V1_SIZE                                             \
  offsetof(turbo_flow_module_adapter_registration_t, operation_resource_names)
#define TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_V2_SIZE                                             \
  offsetof(turbo_flow_module_adapter_registration_t, consume_batch)

typedef enum turbo_flow_state_e {
  TURBO_FLOW_STATE_NEW = 0,
  TURBO_FLOW_STATE_PARSED,
  TURBO_FLOW_STATE_COMPILED,
  TURBO_FLOW_STATE_STARTED,
  TURBO_FLOW_STATE_STOPPED,
  TURBO_FLOW_STATE_FAILED
} turbo_flow_state_t;

typedef struct turbo_flow_runtime_snapshot_s {
  turbo_flow_state_t state;
  int accepting_publishes;
  uint32_t active_publishes;
  size_t stage_count;
  size_t edge_count;
  size_t adapter_count;
  size_t pool_count;
} turbo_flow_runtime_snapshot_t;

typedef enum turbo_flow_data_strategy_e {
  TURBO_FLOW_DATA_BROADCAST = 0,
  TURBO_FLOW_DATA_WORKER_POOL
} turbo_flow_data_strategy_t;

typedef enum turbo_flow_exec_kind_e {
  /** Execute on the caller's current context. TurboFlow Policy evaluation uses this path. */
  TURBO_FLOW_EXEC_INLINE = 0,
  /** Submit to a runtime-owned OS thread pool. */
  TURBO_FLOW_EXEC_THREAD_POOL,
  /** Submit to a runtime-owned Salts coroutine pool. */
  TURBO_FLOW_EXEC_CORO_POOL
} turbo_flow_exec_kind_t;

typedef enum turbo_flow_stage_mutability_e {
  TURBO_FLOW_STAGE_READONLY = 0,
  TURBO_FLOW_STAGE_MUTATES_PRIVATE,
  TURBO_FLOW_STAGE_MUTATES_IN_PLACE
} turbo_flow_stage_mutability_t;

typedef enum turbo_flow_stage_effect_e {
  TURBO_FLOW_STAGE_EFFECT_NONE = 0,
  /** Stage may select a named route or terminate normal downstream release. */
  TURBO_FLOW_STAGE_EFFECT_DYNAMIC_DECISION = 1u << 0,
  /** Stage replaces one input with a bounded sequence of zero or more outputs. */
  TURBO_FLOW_STAGE_EFFECT_EMITS = 1u << 1
} turbo_flow_stage_effect_t;

typedef struct turbo_flow_exec_config_s {
  turbo_flow_exec_kind_t kind;
  uint32_t workers;
  uint32_t lanes;
  uint32_t pool_capacity;
} turbo_flow_exec_config_t;

typedef struct turbo_flow_stage_options_s {
  turbo_flow_stage_mutability_t mutability;
  uint32_t effects;
} turbo_flow_stage_options_t;

/**
 * Executable implementation for one registered operation contract.
 *
 * The operation descriptor remains metadata-only. This binding supplies the
 * runtime callback and its optional resource binding. The callback context is
 * borrowed by the flow and remains owned by the registering module.
 */
typedef struct turbo_flow_operation_provider_registration_s {
  size_t size;
  const char *operation_name;
  /** NULL for a stateless provider; otherwise must match DSL `resource`. */
  const char *resource_name;
  turbo_flow_stage_fn fn;
  void *ctx;
  turbo_flow_stage_options_t options;
} turbo_flow_operation_provider_registration_t;

#define TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT                                            \
  {                                                                                                \
    sizeof(turbo_flow_operation_provider_registration_t), NULL, NULL, NULL, NULL, {                \
      TURBO_FLOW_STAGE_READONLY, TURBO_FLOW_STAGE_EFFECT_NONE                                      \
    }                                                                                              \
  }

#define TURBO_FLOW_EMITTER_MAX_OUTPUTS 1048576u

/**
 * Executable bounded 0..N implementation for one registered data operation.
 *
 * The operation must use direct inline execution, declare no settlement, and
 * have PURE or DATA_MUTATION authority. Emitted messages must be self-contained
 * and may not carry transport capabilities. Callback
 * failure commits no outputs. Once a successful batch enters downstream,
 * later downstream failures do not roll back earlier external side effects.
 *
 * Example callback:
 * @code
 * static int split(const turbo_flow_msg_t *input, turbo_flow_emitter_t *out, void *ctx) {
 *   turbo_flow_msg_t child;
 *   int rc;
 *   (void)ctx;
 *   turbo_flow_msg_init(&child);
 *   child.id = input->id;
 *   rc = turbo_flow_emitter_emit_move(out, &child);
 *   turbo_flow_msg_cleanup(&child);
 *   return rc;
 * }
 * @endcode
 */
typedef struct turbo_flow_emitting_operation_provider_registration_s {
  size_t size;
  const char *operation_name;
  /** NULL for a stateless provider; otherwise must match DSL `resource`. */
  const char *resource_name;
  turbo_flow_emitting_stage_fn fn;
  void *ctx;
  turbo_flow_stage_options_t options;
  /** Per-input output bound in the inclusive range [1, TURBO_FLOW_EMITTER_MAX_OUTPUTS]. */
  uint32_t max_outputs;
} turbo_flow_emitting_operation_provider_registration_t;

#define TURBO_FLOW_EMITTING_OPERATION_PROVIDER_REGISTRATION_INIT                                   \
  {sizeof(turbo_flow_emitting_operation_provider_registration_t), NULL, NULL, NULL, NULL,          \
   {TURBO_FLOW_STAGE_READONLY, TURBO_FLOW_STAGE_EFFECT_NONE},     1u}

/**
 * One 1:1 processor with node-local keyed state.
 *
 * The operation contract must declare NODE state, RUNTIME_GENERATION lifetime,
 * DATA_MUTATION authority, direct inline execution, and no settlement/deadline.
 * One store may bind only one provider/node. Callback PUT/DELETE changes commit
 * only after callback success and per-key revision validation. A concurrent
 * conflicting commit returns SALTS_EBUSY; the runtime does not retry callbacks
 * because their external side effects cannot be assumed idempotent.
 */
typedef struct turbo_flow_keyed_operation_provider_registration_s {
  size_t size;
  const char *operation_name;
  const char *resource_name;
  turbo_flow_key_selector_fn key_selector;
  void *key_ctx;
  turbo_flow_keyed_stage_fn fn;
  void *ctx;
  turbo_flow_keyed_state_store_t *store;
  turbo_flow_stage_options_t options;
} turbo_flow_keyed_operation_provider_registration_t;

#define TURBO_FLOW_KEYED_OPERATION_PROVIDER_REGISTRATION_INIT                                      \
  {                                                                                                \
    sizeof(turbo_flow_keyed_operation_provider_registration_t), NULL, NULL, NULL, NULL, NULL,      \
        NULL, NULL, {                                                                              \
      TURBO_FLOW_STAGE_MUTATES_IN_PLACE, TURBO_FLOW_STAGE_EFFECT_NONE                              \
    }                                                                                              \
  }

/**
 * One stateful 0..N processor with node-local keyed state.
 *
 * State changes and the bounded output batch become visible only when the
 * callback, emitter validation, and per-key state commit all succeed. A later
 * downstream failure does not roll the committed state back. The operation
 * contract and store ownership rules match the 1:1 keyed provider.
 */
typedef struct turbo_flow_keyed_emitting_operation_provider_registration_s {
  size_t size;
  const char *operation_name;
  const char *resource_name;
  turbo_flow_key_selector_fn key_selector;
  void *key_ctx;
  turbo_flow_keyed_emitting_stage_fn fn;
  void *ctx;
  turbo_flow_keyed_state_store_t *store;
  uint32_t max_outputs;
  turbo_flow_stage_options_t options;
} turbo_flow_keyed_emitting_operation_provider_registration_t;

#define TURBO_FLOW_KEYED_EMITTING_OPERATION_PROVIDER_REGISTRATION_INIT                             \
  {                                                                                                \
    sizeof(turbo_flow_keyed_emitting_operation_provider_registration_t), NULL, NULL, NULL, NULL,   \
        NULL, NULL, NULL, 0u, {                                                                    \
      TURBO_FLOW_STAGE_READONLY, TURBO_FLOW_STAGE_EFFECT_NONE                                      \
    }                                                                                              \
  }

/**
 * One keyed event-time tumbling-window processor.
 *
 * `on_event` updates copied aggregate state but emits nothing. A monotonic explicit
 * watermark closes windows whose `end_ns + allowed_lateness_ns` is reached and invokes
 * `on_close` with a final immutable aggregate. A successful close deletes state before
 * outputs enter downstream processing; downstream failure does not restore that state.
 */
typedef struct turbo_flow_event_time_window_provider_registration_s {
  size_t size;
  const char *operation_name;
  const char *resource_name;
  turbo_flow_key_selector_fn key_selector;
  void *key_ctx;
  turbo_flow_event_time_window_stage_fn on_event;
  turbo_flow_event_time_window_close_fn on_close;
  void *ctx;
  turbo_flow_event_time_window_store_t *store;
  uint32_t max_outputs;
  turbo_flow_stage_options_t options;
} turbo_flow_event_time_window_provider_registration_t;

#define TURBO_FLOW_EVENT_TIME_WINDOW_PROVIDER_REGISTRATION_INIT                                    \
  {                                                                                                \
    sizeof(turbo_flow_event_time_window_provider_registration_t), NULL, NULL, NULL, NULL, NULL,    \
        NULL, NULL, NULL, 0u, {                                                                    \
      TURBO_FLOW_STAGE_READONLY, TURBO_FLOW_STAGE_EFFECT_NONE                                      \
    }                                                                                              \
  }

typedef struct turbo_flow_stage_plan_s {
  const char *name;
  int is_source;
  const char *adapter_name;
  turbo_flow_data_strategy_t data_strategy;
  uint32_t data_worker_count;
  turbo_flow_exec_config_t exec;
  turbo_flow_stage_mutability_t mutability;
  uint32_t effects;
  turbo_flow_retry_policy_t retry;
  turbo_flow_reorder_config_t reorder;
  /** Explicit DSL binding or compiler-resolved core operation name. */
  const char *operation_name;
  /** Optional resource primitive binding selected by DSL `resource`. */
  const char *resource_name;
} turbo_flow_stage_plan_t;

typedef enum turbo_flow_edge_kind_e {
  TURBO_FLOW_EDGE_UNCONDITIONAL = 0,
  TURBO_FLOW_EDGE_CONDITIONAL = 1,
  TURBO_FLOW_EDGE_REJECT = 2
} turbo_flow_edge_kind_t;

typedef struct turbo_flow_edge_plan_s {
  uint32_t from_stage;
  uint32_t to_stage;
  uint32_t line;
  uint32_t column;
  turbo_flow_edge_kind_t kind;
  /** Borrowed condition text for conditional edges; NULL otherwise. */
  const char *condition;
  /** Borrowed name for reject edges; NULL otherwise. */
  const char *name;
} turbo_flow_edge_plan_t;

typedef struct turbo_flow_error_s {
  int code;
  uint32_t line;
  uint32_t column;
  char message[160];
} turbo_flow_error_t;

#define TURBO_FLOW_CONTROL_NAME_MAX 255u
#define TURBO_FLOW_CONTROL_EXPR_MAX 4096u

typedef enum turbo_flow_control_kind_e {
  TURBO_FLOW_CONTROL_PAUSE = 1,
  TURBO_FLOW_CONTROL_RESUME,
  TURBO_FLOW_CONTROL_DRAIN,
  TURBO_FLOW_CONTROL_RESIZE_POOL,
  TURBO_FLOW_CONTROL_ADAPTER
} turbo_flow_control_kind_t;

typedef struct turbo_flow_control_command_s {
  /** Set to sizeof(turbo_flow_control_command_t). */
  size_t size;
  turbo_flow_control_kind_t kind;
  uint64_t timeout_ms;
  char target[TURBO_FLOW_CONTROL_NAME_MAX + 1u];
  turbo_flow_pool_kind_t pool_kind;
  uint32_t parallelism;
  turbo_flow_adapter_command_t adapter;
  char endpoint_host[TURBO_FLOW_ENDPOINT_MAX + 1u];
  char endpoint_path[TURBO_FLOW_ENDPOINT_MAX + 1u];
  /** Empty for an unconditional command; otherwise an owned BOOL expression. */
  char condition[TURBO_FLOW_CONTROL_EXPR_MAX + 1u];
} turbo_flow_control_command_t;

#define TURBO_FLOW_CONTROL_COMMAND_INIT {sizeof(turbo_flow_control_command_t), 0}

TURBO_FLOW_C_API turbo_flow_t *turbo_flow_create(void);
TURBO_FLOW_C_API void turbo_flow_destroy(turbo_flow_t *flow);

/**
 * Clear parsed/compiled stage plan state.
 *
 * Reset is rejected while the flow is STARTED or while a failed adapter stop is
 * awaiting an explicit `turbo_flow_stop()` retry. When `keep_registry` is
 * non-zero, registered stage callbacks and adapters remain available for the
 * next parse/compile cycle. When zero, the flow returns to an empty NEW state.
 */
TURBO_FLOW_C_API int turbo_flow_reset(turbo_flow_t *flow, int keep_registry);

/**
 * Parse a graph definition. Parsing is rejected while a failed adapter stop is
 * awaiting an explicit `turbo_flow_stop()` retry.
 */
TURBO_FLOW_C_API int turbo_flow_parse_string(turbo_flow_t *flow, const char *text, size_t len);

/**
 * Validate the parsed stage plan and build runtime plans. Topology remains
 * immutable after compile; pool parallelism may change only through the
 * serialized resize command.
 *
 * A flow may be compiled from PARSED or recompiled from STOPPED. Compile errors
 * move the flow to FAILED; call turbo_flow_reset() before parsing again.
 */
TURBO_FLOW_C_API int turbo_flow_compile(turbo_flow_t *flow);

/**
 * Start runtime data planes and executor adapters.
 *
 * Valid from COMPILED and STOPPED. Starting a STOPPED flow reuses the compiled
 * stage plan and recreates runtime rings/adapters. If start rollback cannot stop
 * an adapter, the flow enters FAILED and requires an explicit `turbo_flow_stop()`
 * retry before reset, parse, or another start.
 */
TURBO_FLOW_C_API int turbo_flow_start(turbo_flow_t *flow);

/** Idempotently stop accepting new publishes while keeping runtime resources started. */
TURBO_FLOW_C_API int turbo_flow_pause(turbo_flow_t *flow);

/** Idempotently reopen publish admission for a paused STARTED flow. */
TURBO_FLOW_C_API int turbo_flow_resume(turbo_flow_t *flow);

/**
 * Pause admission and wait for accepted publishes to complete. A zero timeout
 * performs a non-blocking check; UINT64_MAX waits without a deadline. The flow
 * remains paused after success or timeout.
 */
TURBO_FLOW_C_API int turbo_flow_drain(turbo_flow_t *flow, uint64_t timeout_ms);

/**
 * Drain admission and atomically rebuild runtime-owned pool resources.
 *
 * The host must serialize this command with lifecycle and other configuration
 * commands. Pool records start a new runtime generation after a successful
 * resize. A rebuild failure restores the previous configuration and resources;
 * a drain timeout leaves the flow paused without changing pool configuration.
 */
TURBO_FLOW_C_API int turbo_flow_resize_pool(turbo_flow_t *flow,
                                     const turbo_flow_pool_resize_command_t *command);

/** Parse exactly one side-effect-free control DSL command into caller-owned storage. */
TURBO_FLOW_C_API int turbo_flow_control_parse(const char *text, size_t len,
                                       turbo_flow_control_command_t *out,
                                       turbo_flow_error_t *error);
/** Execute a previously parsed control command on a STARTED flow. */
TURBO_FLOW_C_API int turbo_flow_control_execute(turbo_flow_t *flow,
                                         const turbo_flow_control_command_t *command);
/** Convenience parse-and-execute entry point for exactly one command. */
TURBO_FLOW_C_API int turbo_flow_control(turbo_flow_t *flow, const char *text, size_t len);

/**
 * Stop a STARTED flow and release runtime-only rings/adapters.
 *
 * The compiled stage plan and registries remain available, so the flow can be
 * started again or reset for another parse/compile cycle. If an adapter reports
 * a stop error, the Flow enters FAILED and retains only failed adapter bindings;
 * call this function again to retry their stop before restarting.
 */
TURBO_FLOW_C_API int turbo_flow_stop(turbo_flow_t *flow);

/**
 * Publish one message through a STARTED flow source.
 *
 * Borrowed payload views must be backed by `buffer`; unique `owned_payload`
 * messages are cloned before entering the runtime. Schema projections without
 * a clone hook are rejected.
 */
TURBO_FLOW_C_API int turbo_flow_publish(turbo_flow_t *flow, const char *source_name,
                                 const turbo_flow_msg_t *msg);

/**
 * Prepare one transient message for an ordered synchronous batch.
 *
 * Core initializes `message` before the callback and always cleans it after the
 * callback/publish attempt. On SALTS_OK, ownership of all fields transfers to
 * core for that attempt. On failure, core still cleans any fields already set.
 * The callback must not retain `message` or synchronously stop, drain, reset, or
 * destroy the same flow.
 */
typedef int (*turbo_flow_publish_batch_prepare_fn)(void *ctx, size_t index,
                                                   turbo_flow_msg_t *message);

typedef struct turbo_flow_publish_batch_config_s {
  size_t size;
  size_t message_count;
  turbo_flow_publish_batch_prepare_fn prepare;
  void *ctx;
} turbo_flow_publish_batch_config_t;

#define TURBO_FLOW_PUBLISH_BATCH_CONFIG_INIT                                                       \
  {sizeof(turbo_flow_publish_batch_config_t), 0u, NULL, NULL}

/**
 * Prepare and publish an ordered batch synchronously through one STARTED source.
 *
 * The copied config drives one fixed lifecycle: source validation, prepare,
 * publish, and cleanup. Processing stops at the first prepare or publish failure.
 * `published`, when non-NULL, receives the number of successful prefix messages
 * and is zeroed before input or lifecycle validation. Per-message ownership and
 * clone/retain rules match `turbo_flow_publish()`. A concurrent stop waits for
 * the accepted batch to return.
 *
 * Returns SALTS_EINVAL for invalid ABI/source/config input or an unknown/non-source
 * stage, SALTS_ESHUTDOWN when publication admission is closed, or the first prepare,
 * graph, primitive, or owner callback failure.
 *
 * Example (the flow must already be compiled and started):
 * @code
 * typedef struct publish_ids_s {
 *   uint64_t first_id;
 * } publish_ids_t;
 *
 * static int prepare_id(void *ctx, size_t index, turbo_flow_msg_t *message) {
 *   const publish_ids_t *ids = (const publish_ids_t *)ctx;
 *   message->id = ids->first_id + index;
 *   return SALTS_OK;
 * }
 *
 * publish_ids_t ids = {1000u};
 * turbo_flow_publish_batch_config_t config = TURBO_FLOW_PUBLISH_BATCH_CONFIG_INIT;
 * size_t published = 0u;
 * config.message_count = 32u;
 * config.prepare = prepare_id;
 * config.ctx = &ids;
 * int rc = turbo_flow_publish_batch(flow, "input", &config, &published);
 * @endcode
 *
 * The callback and `ctx` are borrowed for the duration of this synchronous call.
 * The config fields are snapshotted before the first callback.
 */
TURBO_FLOW_C_API int turbo_flow_publish_batch(turbo_flow_t *flow, const char *source_name,
                                       const turbo_flow_publish_batch_config_t *config,
                                       size_t *published);

typedef struct turbo_flow_publish_result_s {
  size_t size;
  int status;
} turbo_flow_publish_result_t;

#define TURBO_FLOW_PUBLISH_RESULT_INIT {sizeof(turbo_flow_publish_result_t), SALTS_OK}

#define TURBO_FLOW_RUN_API_VERSION 1u

typedef enum turbo_flow_run_state_e {
  TURBO_FLOW_RUN_OPEN = 0,
  TURBO_FLOW_RUN_ACTIVE,
  TURBO_FLOW_RUN_COMPLETED,
  TURBO_FLOW_RUN_CANCELED,
  TURBO_FLOW_RUN_FAILED
} turbo_flow_run_state_t;

/** Versioned options copied by `turbo_flow_run_open()`. */
typedef struct turbo_flow_run_config_s {
  size_t size;
  uint32_t version;
  /** Borrowed through terminal settlement; NULL selects the Flow-owned bounded Scheduler. */
  cflow_scheduler *scheduler;
  /** Relative monotonic deadline from open; zero disables the deadline. */
  uint64_t deadline_ms;
} turbo_flow_run_config_t;

#define TURBO_FLOW_RUN_CONFIG_V1_SIZE sizeof(turbo_flow_run_config_t)
#define TURBO_FLOW_RUN_CONFIG_INIT                                                                \
  {TURBO_FLOW_RUN_CONFIG_V1_SIZE, TURBO_FLOW_RUN_API_VERSION, NULL, 0u}

/** Caller-owned snapshot; error text is copied and remains valid with the snapshot. */
typedef struct turbo_flow_run_result_s {
  size_t size;
  uint32_t version;
  turbo_flow_run_state_t state;
  int status;
  size_t values;
  size_t outstanding_demand;
  turbo_flow_error_t error;
} turbo_flow_run_result_t;

#define TURBO_FLOW_RUN_RESULT_V1_SIZE sizeof(turbo_flow_run_result_t)
#define TURBO_FLOW_RUN_RESULT_INIT                                                               \
  {TURBO_FLOW_RUN_RESULT_V1_SIZE, TURBO_FLOW_RUN_API_VERSION, TURBO_FLOW_RUN_OPEN, SALTS_OK}

/** Managed CMeta descriptor required by Publishers passed to `turbo_flow_run_open()`. */
TURBO_FLOW_C_API const cmeta_type_desc *turbo_flow_message_type(void);

/**
 * Open one independently demand-driven Reactive run.
 *
 * On success CFlow takes `publisher`, clears it, and `*run_out` owns the returned handle.
 * Any failure before subscribe leaves Publisher ownership with the caller. Active runs are
 * bounded by the configured asynchronous-ingress capacity. A full bound returns SALTS_ENOSPC;
 * closed Flow admission or Scheduler admission returns SALTS_ESHUTDOWN. A nonzero deadline
 * requires delayed Scheduler capability and otherwise returns SALTS_ENOTSUP.
 */
TURBO_FLOW_C_API int turbo_flow_run_open(turbo_flow_t *flow, const char *source_name,
                                         cflow_publisher *publisher,
                                         const turbo_flow_run_config_t *config,
                                         turbo_flow_run_t **run_out);

/** Add downstream-value demand without blocking. A full Scheduler retains the demand. */
TURBO_FLOW_C_API int turbo_flow_run_request(turbo_flow_run_t *run, size_t demand);

/** Copy current state without advancing demand or execution. */
TURBO_FLOW_C_API int turbo_flow_run_snapshot(const turbo_flow_run_t *run,
                                             turbo_flow_run_result_t *result);

/**
 * Wait for a terminal result. Zero polls and UINT64_MAX waits without a caller deadline.
 * SALTS_ETIMEDOUT from this function does not change or cancel the run.
 */
TURBO_FLOW_C_API int turbo_flow_run_wait(turbo_flow_run_t *run, uint64_t timeout_ms,
                                         turbo_flow_run_result_t *result);

/** Request cancellation and synchronously unregister any active WAIT waker. */
TURBO_FLOW_C_API int turbo_flow_run_cancel(turbo_flow_run_t *run);

/** Cancel if necessary and release the caller's handle. Concurrent handle close is unsupported. */
TURBO_FLOW_C_API void turbo_flow_run_close(turbo_flow_run_t *run);

#define TURBO_FLOW_ASYNC_INGRESS_DEFAULT_WORKERS TURBO_FLOW_CONFIG_INGRESS_DEFAULT_WORKERS
#define TURBO_FLOW_ASYNC_INGRESS_DEFAULT_CAPACITY TURBO_FLOW_CONFIG_INGRESS_DEFAULT_CAPACITY
#define TURBO_FLOW_ASYNC_INGRESS_MAX_WORKERS TURBO_FLOW_CONFIG_INGRESS_MAX_WORKERS
#define TURBO_FLOW_ASYNC_INGRESS_MAX_CAPACITY TURBO_FLOW_CONFIG_INGRESS_MAX_CAPACITY
#define TURBO_FLOW_ASYNC_INGRESS_DEFAULT_MAX_MESSAGE_BYTES                                         \
  TURBO_FLOW_CONFIG_INGRESS_DEFAULT_MAX_MESSAGE_BYTES
#define TURBO_FLOW_ASYNC_INGRESS_DEFAULT_MAX_INFLIGHT_BYTES                                        \
  TURBO_FLOW_CONFIG_INGRESS_DEFAULT_MAX_INFLIGHT_BYTES
#define TURBO_FLOW_ASYNC_INGRESS_MAX_MESSAGE_BYTES TURBO_FLOW_CONFIG_INGRESS_MAX_MESSAGE_BYTES
#define TURBO_FLOW_ASYNC_INGRESS_MAX_INFLIGHT_BYTES TURBO_FLOW_CONFIG_INGRESS_MAX_INFLIGHT_BYTES

/** Flow-owned bounded source ingress used by non-blocking producers. */
typedef struct turbo_flow_async_ingress_config_s {
  size_t size;
  uint32_t workers;
  size_t queue_capacity;
  /** Maximum bytes retained by one accepted task across all payload backing stores. */
  size_t max_message_bytes;
  /** Maximum aggregate retained payload bytes across accepted async tasks. */
  size_t max_inflight_bytes;
} turbo_flow_async_ingress_config_t;

#define TURBO_FLOW_ASYNC_INGRESS_CONFIG_V1_SIZE                                                    \
  offsetof(turbo_flow_async_ingress_config_t, max_message_bytes)
#define TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT                                                       \
  {sizeof(turbo_flow_async_ingress_config_t), TURBO_FLOW_ASYNC_INGRESS_DEFAULT_WORKERS,            \
   TURBO_FLOW_ASYNC_INGRESS_DEFAULT_CAPACITY, TURBO_FLOW_ASYNC_INGRESS_DEFAULT_MAX_MESSAGE_BYTES,  \
   TURBO_FLOW_ASYNC_INGRESS_DEFAULT_MAX_INFLIGHT_BYTES}

/**
 * Completion invoked on a Flow ingress worker after graph execution.
 *
 * `result` is borrowed for the callback duration. The callback is part of the
 * active publication and must not synchronously stop, drain, reset, or destroy
 * the same flow.
 */
typedef void (*turbo_flow_publish_completion_fn)(void *ctx,
                                                 const turbo_flow_publish_result_t *result);

/** External source-to-Graph handoff policy. */
typedef enum turbo_flow_source_handoff_mode_e {
  /** Execute graph admission and stages on the producer thread before returning. */
  TURBO_FLOW_SOURCE_HANDOFF_INLINE = 0,
  /** Retain ownership and try-admit through the configured bounded async ingress. */
  TURBO_FLOW_SOURCE_HANDOFF_ASYNC_BOUNDED
} turbo_flow_source_handoff_mode_t;

/**
 * Configure the lazily-created bounded async source ingress.
 *
 * The configuration is copied and survives stop/start. Call only while the
 * flow is not STARTED. Queue, per-message byte, or aggregate in-flight byte
 * exhaustion is reported by publish_async as SALTS_ENOSPC; submissions never
 * block waiting for capacity. A V1-sized configuration remains accepted and
 * receives the current byte-budget defaults.
 */
TURBO_FLOW_C_API int turbo_flow_configure_async_ingress(turbo_flow_t *flow,
                                                 const turbo_flow_async_ingress_config_t *config);

/**
 * Clone/retain one message and enqueue it for source publication.
 *
 * SALTS_OK means the Flow owns the accepted message; graph completion is
 * reported later through `completion`. Invalid input/lifecycle errors are
 * returned directly, and a full bounded ingress returns SALTS_ENOSPC. The
 * producer thread never executes graph stages.
 */
TURBO_FLOW_C_API int turbo_flow_publish_async(turbo_flow_t *flow, const char *source_name,
                                       const turbo_flow_msg_t *msg,
                                       turbo_flow_publish_completion_fn completion, void *ctx);

/**
 * Publish synchronously and report graph execution status.
 *
 * `result` is caller-owned and must use TURBO_FLOW_PUBLISH_RESULT_INIT. On
 * return, `result->status` equals the function status.
 * Returns SALTS_EINVAL for invalid ABI/source/message input, lifecycle or
 * stage errors from the graph, or the primitive/owner callback status.
 */
TURBO_FLOW_C_API int turbo_flow_publish_ex(turbo_flow_t *flow, const char *source_name,
                                    const turbo_flow_msg_t *msg,
                                    turbo_flow_publish_result_t *result);

TURBO_FLOW_C_API int turbo_flow_register_stage_ex(turbo_flow_t *flow, const char *name,
                                           turbo_flow_stage_fn fn, void *ctx,
                                           const turbo_flow_stage_options_t *options);

/**
 * Register an executable implementation for a domain operation.
 *
 * Registration is selected by the pair `(operation_name, resource_name)`;
 * `resource_name` is NULL only for a stateless operation. The descriptor must
 * be registered separately with turbo_flow_register_operation().
 */
TURBO_FLOW_C_API int turbo_flow_register_operation_provider(
    turbo_flow_t *flow, const turbo_flow_operation_provider_registration_t *registration);

/**
 * Register one bounded 0..N implementation for a domain data operation.
 *
 * The flow copies registration metadata and borrows ctx until reset/destroy.
 * Returns SALTS_OK, SALTS_EINVAL for an invalid ABI/options/bound,
 * SALTS_EALREADY for a duplicate operation/resource pair, SALTS_EBUSY after
 * compile/start, or SALTS_ENOMEM. Compile may return SALTS_ENOTSUP when the
 * operation contract, executor, retry policy, or downstream topology cannot
 * provide the documented emission semantics.
 */
TURBO_FLOW_C_API int turbo_flow_register_emitting_operation_provider(
    turbo_flow_t *flow, const turbo_flow_emitting_operation_provider_registration_t *registration);

/**
 * Register one keyed-state processor. Metadata is copied; callback contexts and
 * store are borrowed. Returns standard provider registration errors and
 * SALTS_EALREADY when the store already has an owner binding.
 */
TURBO_FLOW_C_API int turbo_flow_register_keyed_operation_provider(
    turbo_flow_t *flow, const turbo_flow_keyed_operation_provider_registration_t *registration);

/** Register one bounded stateful 0..N processor; callback contexts and store are borrowed. */
TURBO_FLOW_C_API int turbo_flow_register_keyed_emitting_operation_provider(
    turbo_flow_t *flow,
    const turbo_flow_keyed_emitting_operation_provider_registration_t *registration);

/** Register one bounded event-time tumbling-window provider. */
TURBO_FLOW_C_API int turbo_flow_register_event_time_window_provider(
    turbo_flow_t *flow, const turbo_flow_event_time_window_provider_registration_t *registration);

/**
 * Monotonically advance one provider watermark and synchronously close eligible windows.
 *
 * The caller owns multi-source watermark merging. Equal watermarks are accepted so a failed
 * close can be retried. `closed_windows` may be NULL and counts state deletions completed by
 * this call. Events for already-closed time ranges fail with SALTS_ETIMEDOUT. One advance uses
 * O(W + C log C) time and O(W) temporary references for W active and C closable windows.
 */
TURBO_FLOW_C_API int turbo_flow_advance_event_time_watermark(turbo_flow_t *flow,
                                                      turbo_flow_event_time_window_store_t *store,
                                                      uint64_t watermark_ns,
                                                      size_t *closed_windows);

/** Atomically register one stage callback and zero or more independently addressable resources. */
TURBO_FLOW_C_API int turbo_flow_register_stage_with_resources(
    turbo_flow_t *flow, const char *name, turbo_flow_stage_fn fn, void *ctx,
    const turbo_flow_stage_options_t *options,
    const turbo_flow_resource_provider_registration_t *resources, size_t resource_count);

/**
 * Register a source/sink adapter name referenced by DSL `adapter` metadata.
 *
 * `ops` may be NULL for hosts that only validate topology or bind adapter
 * behavior outside turbo_flow. Registration must happen before compile/start.
 */
TURBO_FLOW_C_API int turbo_flow_register_adapter(turbo_flow_t *flow, const char *name,
                                          const turbo_flow_adapter_ops_t *ops, void *ctx);

/** Attach an asynchronous terminal-stage submit contract before compile. */
TURBO_FLOW_C_API int
turbo_flow_register_adapter_async_terminal(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_async_terminal_adapter_ops_t *ops);

/** Attach an asynchronous 0..1-output stage submit contract before compile. */
TURBO_FLOW_C_API int
turbo_flow_register_adapter_async_emit(turbo_flow_t *flow, const char *name,
                                       const turbo_flow_async_emit_adapter_ops_t *ops);

/** Attach an explicit settlement owner to an existing adapter before compile. */
TURBO_FLOW_C_API int turbo_flow_register_adapter_settlement(turbo_flow_t *flow, const char *name,
                                                     const turbo_flow_settlement_owner_ops_t *ops,
                                                     void *ctx);

/**
 * Register one stable resource independently of adapter cardinality.
 *
 * `owner_name` and the UID returned by `ops->metadata` must be stable and
 * unique within the flow. Registration is rejected after the flow starts.
 */
TURBO_FLOW_C_API int turbo_flow_register_resource_provider(turbo_flow_t *flow, const char *owner_name,
                                                    const turbo_flow_resource_provider_ops_t *ops,
                                                    void *ctx);
/**
 * Register one explicitly managed Source/Sink in the canonical resource registry.
 * @param flow Mutable flow before compile/start.
 * @param owner_name Stable owner name, equal to metadata and descriptor owner names.
 * @param ops Versioned resource, descriptor, snapshot, and optional command provider contract.
 * @param ctx Owner context retained by the caller through reset/destroy.
 * @return SALTS_OK, an owner error, SALTS_EINVAL, SALTS_EPROTO, SALTS_EALREADY, or SALTS_ENOMEM.
 */
TURBO_FLOW_C_API int turbo_flow_register_managed_boundary_provider(
    turbo_flow_t *flow, const char *owner_name,
    const turbo_flow_managed_boundary_provider_ops_t *ops, void *ctx);

/**
 * Register an adapter and immutable option metadata.
 *
 * The registry copies the schema, field names, and enum values. A source
 * declaration requires the SOURCE role. A stage requires SINK or TRANSFORM;
 * sink-only stages must be terminal. Concrete adapter config structs remain
 * the runtime value source and are validated by their owning adapter module.
 */
TURBO_FLOW_C_API int turbo_flow_register_adapter_ex(turbo_flow_t *flow, const char *name,
                                             const turbo_flow_adapter_ops_t *ops, void *ctx,
                                             const turbo_flow_adapter_schema_t *schema);

/** Atomically register an adapter together with its asynchronous terminal contract. */
TURBO_FLOW_C_API int turbo_flow_register_async_terminal_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_adapter_ops_t *adapter_ops,
    const turbo_flow_async_terminal_adapter_ops_t *async_ops, void *ctx,
    const turbo_flow_adapter_schema_t *schema);

/**
 * Atomically register an asynchronous terminal adapter and exactly one
 * explicitly managed Sink boundary for the same owner context.
 *
 * A failed call leaves both registries unchanged and retains caller ownership
 * of `registration->ctx`. No boundary is inferred from adapter metadata.
 * @param flow Mutable flow before compile/start.
 * @param registration Size/versioned adapter, boundary, and owner contract.
 * @return SALTS_OK, an owner error, SALTS_EINVAL, SALTS_EPROTO, SALTS_EALREADY,
 * SALTS_EBUSY, or SALTS_ENOMEM.
 */
TURBO_FLOW_C_API int turbo_flow_register_managed_async_terminal_adapter(
    turbo_flow_t *flow,
    const turbo_flow_managed_async_terminal_registration_t *registration);

/** Atomically register a transform adapter with an asynchronous 0..1-output contract. */
TURBO_FLOW_C_API int turbo_flow_register_async_emit_adapter_ex(
    turbo_flow_t *flow, const char *name, const turbo_flow_adapter_ops_t *adapter_ops,
    const turbo_flow_async_emit_adapter_ops_t *async_ops, void *ctx,
    const turbo_flow_adapter_schema_t *schema);

/** Atomically register one adapter and zero or more independently addressable resources. */
TURBO_FLOW_C_API int turbo_flow_register_adapter_with_resources(
    turbo_flow_t *flow, const char *name, const turbo_flow_adapter_ops_t *ops, void *ctx,
    const turbo_flow_adapter_schema_t *schema,
    const turbo_flow_resource_provider_registration_t *resources, size_t resource_count);

/** Atomically register a module-owned native adapter and its typed operations. */
TURBO_FLOW_C_API int
turbo_flow_register_module_adapter(turbo_flow_t *flow,
                                   const turbo_flow_module_adapter_registration_t *registration);

/**
 * Return the module owning an adapter's executable operation association.
 * The borrowed string remains valid until registry-clearing reset or destroy.
 */
TURBO_FLOW_C_API const char *turbo_flow_adapter_operation_module(const turbo_flow_t *flow,
                                                          const char *adapter_name,
                                                          const char *operation_name);

/** Return the primitive binding for one typed adapter operation, or NULL when adapter-owned. */
TURBO_FLOW_C_API const char *turbo_flow_adapter_operation_resource(const turbo_flow_t *flow,
                                                            const char *adapter_name,
                                                            const char *operation_name);

/** Attach or clear one host-owned observer. Rejected while the flow is started. */
TURBO_FLOW_C_API int turbo_flow_set_observer(turbo_flow_t *flow, const turbo_flow_observer_ops_t *ops,
                                      void *ctx);

/**
 * Register one named structured Observer.
 *
 * Registration is rejected while started, on duplicate names, or after the
 * bounded observer limit is reached. The registration remains across reset.
 */
TURBO_FLOW_C_API int turbo_flow_register_observer(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_event_observer_ops_t *ops, void *ctx);
/** Unregister and destroy one Observer. Rejected while the flow is started. */
TURBO_FLOW_C_API int turbo_flow_unregister_observer(turbo_flow_t *flow, const char *name);
TURBO_FLOW_C_API size_t turbo_flow_observer_count(const turbo_flow_t *flow);
/** Return the aggregate count of non-OK structured Observer callback results. */
TURBO_FLOW_C_API uint64_t turbo_flow_observer_failure_count(const turbo_flow_t *flow);

TURBO_FLOW_C_API size_t turbo_flow_adapter_count(const turbo_flow_t *flow);
/**
 * Query one registered adapter. Returns SALTS_ENOTSUP when it has no provider.
 * Serialize with registry reset/destroy; the provider synchronizes live fields.
 */
TURBO_FLOW_C_API int turbo_flow_adapter_connection_snapshot_at(const turbo_flow_t *flow, size_t index,
                                                        turbo_flow_connection_snapshot_t *out);
/** Enumerate registered resource providers with snapshots followed by runtime pools. */
TURBO_FLOW_C_API size_t turbo_flow_resource_count(const turbo_flow_t *flow);
TURBO_FLOW_C_API int turbo_flow_resource_snapshot_at(const turbo_flow_t *flow, size_t index,
                                              turbo_flow_resource_snapshot_t *out);
/**
 * Enumerate only registered resources with an explicit managed Source/Sink contract.
 * @param flow Flow registry; NULL returns zero.
 * @return Boundary count. This read does not invoke owner callbacks.
 */
TURBO_FLOW_C_API size_t turbo_flow_managed_boundary_count(const turbo_flow_t *flow);
/**
 * Copy the immutable descriptor captured when its resource provider was registered.
 * @param flow Flow registry serialized against reset/destroy.
 * @param index Zero-based managed-boundary index.
 * @param out Caller-owned initialized output.
 * @return SALTS_OK, an owner error, SALTS_EINVAL, SALTS_ENOENT, or SALTS_EPROTO on identity drift.
 */
TURBO_FLOW_C_API int turbo_flow_managed_boundary_descriptor_at(
    const turbo_flow_t *flow, size_t index, turbo_flow_managed_boundary_descriptor_t *out);
/**
 * Query and validate one owner-synchronized managed Source/Sink snapshot.
 * @param flow Flow registry serialized against reset/destroy.
 * @param index Zero-based managed-boundary index.
 * @param out Caller-owned initialized output.
 * @return SALTS_OK, an owner error, SALTS_EINVAL, SALTS_ENOENT, SALTS_EBUSY on concurrent
 *         generation change, or SALTS_EPROTO on stable contract drift.
 */
TURBO_FLOW_C_API int turbo_flow_managed_boundary_snapshot_at(
    const turbo_flow_t *flow, size_t index, turbo_flow_managed_boundary_snapshot_t *out);
/** Enumerate explicitly stable adapter resources followed by runtime-owned pools. */
TURBO_FLOW_C_API size_t turbo_flow_resource_metadata_count(const turbo_flow_t *flow);
TURBO_FLOW_C_API int turbo_flow_resource_metadata_at(const turbo_flow_t *flow, size_t index,
                                              turbo_flow_resource_metadata_t *out);
/** Query one stable resource through its owner-specific schema-backed document provider. */
TURBO_FLOW_C_API int turbo_flow_resource_document_at(const turbo_flow_t *flow, size_t index,
                                              turbo_flow_resource_document_kind_t document_kind,
                                              turbo_flow_resource_document_t *out);

/**
 * Return the immutable common-governance schema for one canonical domain/resource pair.
 *
 * Spec, Conditions, and Event are available. Status remains owner-native and Command is an
 * operation rather than a synthesized document. NULL means the pair or document kind is not
 * part of the common governance contract.
 */
TURBO_FLOW_C_API const turbo_flow_resource_schema_t *
turbo_flow_resource_governance_schema(turbo_flow_domain_t domain,
                                      turbo_flow_resource_kind_t resource_kind,
                                      turbo_flow_resource_document_kind_t document_kind);
/** Execute one generation-checked idempotent resource command. Hosts serialize lifecycle calls. */
TURBO_FLOW_C_API int turbo_flow_resource_command(turbo_flow_t *flow,
                                          const turbo_flow_resource_command_t *command,
                                          turbo_flow_resource_command_result_t *result);
/** Compare one immutable observation and issue at most one generation-checked owner command. */
TURBO_FLOW_C_API int
turbo_flow_resource_reconcile_tick(turbo_flow_t *flow,
                                   const turbo_flow_resource_reconcile_request_t *request,
                                   turbo_flow_resource_reconcile_result_t *result);
/** Initialize caller-owned state for a quiesce -> drain -> resize -> resume workflow. */
TURBO_FLOW_C_API int turbo_flow_resize_workflow_init(const turbo_flow_resize_workflow_spec_t *spec,
                                              turbo_flow_resize_workflow_state_t *state);
/** Advance at most one owner operation. No controller thread or hidden scheduling is created. */
TURBO_FLOW_C_API int turbo_flow_resize_workflow_tick(turbo_flow_t *flow,
                                              turbo_flow_resize_workflow_state_t *state,
                                              turbo_flow_resize_workflow_result_t *result);
/** Retry the explicitly failed step with a new idempotency attempt. */
TURBO_FLOW_C_API int turbo_flow_resize_workflow_retry(turbo_flow_resize_workflow_state_t *state);
/**
 * Execute one adapter-owned command by registry binding name on a STARTED flow.
 *
 * @deprecated Register a command-capable stable resource with
 * turbo_flow_register_adapter_with_resources() and dispatch a
 * turbo_flow_resource_command_t through turbo_flow_resource_command(). This
 * entry point remains available for source and ABI compatibility.
 */
TURBO_FLOW_C_API TURBO_FLOW_DEPRECATED(
    "use turbo_flow_resource_command() with a stable resource "
    "provider") int turbo_flow_adapter_command(turbo_flow_t *flow, const char *adapter_name,
                                               const turbo_flow_adapter_command_t *command);

/** Return registry-owned metadata valid until reset without keep_registry or destroy. */
TURBO_FLOW_C_API const turbo_flow_adapter_schema_t *turbo_flow_adapter_schema_at(const turbo_flow_t *flow,
                                                                          size_t index);

TURBO_FLOW_C_API const turbo_flow_adapter_schema_t *
turbo_flow_find_adapter_schema(const turbo_flow_t *flow, const char *name);

TURBO_FLOW_C_API turbo_flow_state_t turbo_flow_state(const turbo_flow_t *flow);
/**
 * Copy runtime admission and topology counters into caller-owned storage.
 * The host must serialize this call with parse/compile/start/stop/reset/destroy.
 * Concurrent publish admission and completion are synchronized internally.
 */
TURBO_FLOW_C_API int turbo_flow_runtime_snapshot(const turbo_flow_t *flow,
                                          turbo_flow_runtime_snapshot_t *out);
TURBO_FLOW_C_API const turbo_flow_error_t *turbo_flow_last_error(const turbo_flow_t *flow);

/** Runtime pool records remain queryable after stop and reset on the next start. */
TURBO_FLOW_C_API size_t turbo_flow_pool_count(const turbo_flow_t *flow);
TURBO_FLOW_C_API int turbo_flow_pool_snapshot_at(const turbo_flow_t *flow, size_t index,
                                          turbo_flow_pool_snapshot_t *out);
TURBO_FLOW_C_API int turbo_flow_pool_resource_status_at(const turbo_flow_t *flow, size_t index,
                                                 turbo_flow_pool_resource_status_t *out);
/**
 * Return the process-lifetime schema descriptor for pool Status documents.
 *
 * The returned pointer is borrowed, immutable, and never NULL.
 */
TURBO_FLOW_C_API const turbo_flow_resource_schema_t *turbo_flow_pool_status_schema(void);
/**
 * Capture one runtime pool as an owned, schema-backed Status document.
 *
 * @param flow Flow whose lifecycle and pool vectors are serialized by the host.
 * @param index Pool index in the current runtime generation.
 * @param out Caller-owned output initialized with TURBO_FLOW_RESOURCE_DOCUMENT_INIT.
 * @return SALTS_OK on success; SALTS_EINVAL for invalid/uninitialized or still-owned output,
 *         SALTS_ENOENT when the pool does not exist, SALTS_ENOMEM on allocation failure, or a
 *         snapshot/identity error from the pool owner.
 *
 * On success `out->payload` is immutable and remains valid independently of later pool changes.
 * Call turbo_flow_resource_document_cleanup() before reusing or discarding `out`.
 */
TURBO_FLOW_C_API int turbo_flow_pool_status_document_at(const turbo_flow_t *flow, size_t index,
                                                 turbo_flow_resource_document_t *out);
/**
 * Release a document payload and restore TURBO_FLOW_RESOURCE_DOCUMENT_INIT state.
 *
 * @param document Document to release; NULL and repeated cleanup are accepted.
 */
TURBO_FLOW_C_API void turbo_flow_resource_document_cleanup(turbo_flow_resource_document_t *document);
TURBO_FLOW_C_API int turbo_flow_pool_accepting(const turbo_flow_pool_snapshot_t *snapshot);
TURBO_FLOW_C_API int turbo_flow_pool_drained(const turbo_flow_pool_snapshot_t *snapshot);
TURBO_FLOW_C_API int turbo_flow_pool_saturated(const turbo_flow_pool_snapshot_t *snapshot);

TURBO_FLOW_C_API size_t turbo_flow_stage_count(const turbo_flow_t *flow);
TURBO_FLOW_C_API const turbo_flow_stage_plan_t *turbo_flow_stage_at(const turbo_flow_t *flow,
                                                             size_t index);
/** Return the complete resolved operation contract after compile, or NULL before resolution. */
TURBO_FLOW_C_API const turbo_flow_operation_descriptor_t *
turbo_flow_stage_operation_at(const turbo_flow_t *flow, size_t index);
TURBO_FLOW_C_API int turbo_flow_find_stage(const turbo_flow_t *flow, const char *name);

TURBO_FLOW_C_API size_t turbo_flow_edge_count(const turbo_flow_t *flow);
TURBO_FLOW_C_API const turbo_flow_edge_plan_t *turbo_flow_edge_at(const turbo_flow_t *flow, size_t index);

/** Compiled, read-only runtime segment metadata. Available after successful compile. */
TURBO_FLOW_C_API size_t turbo_flow_segment_count(const turbo_flow_t *flow);
TURBO_FLOW_C_API int turbo_flow_segment_plan_at(const turbo_flow_t *flow, size_t index,
                                         turbo_flow_segment_plan_t *out);

TURBO_FLOW_C_API void turbo_flow_msg_init(turbo_flow_msg_t *msg);
TURBO_FLOW_C_API void turbo_flow_msg_cleanup(turbo_flow_msg_t *msg);
TURBO_FLOW_C_API int turbo_flow_msg_retain_view(turbo_flow_msg_t *dst, const turbo_flow_msg_t *src);
TURBO_FLOW_C_API int turbo_flow_msg_clone(turbo_flow_msg_t *dst, const turbo_flow_msg_t *src);
TURBO_FLOW_C_API int turbo_flow_msg_move(turbo_flow_msg_t *dst, turbo_flow_msg_t *src);

/** Return whether the message has an attached schema-bound projection. */
TURBO_FLOW_C_API turbo_flow_content_state_t turbo_flow_msg_content_state(const turbo_flow_msg_t *msg);

/**
 * Attach an owned derived projection without changing the original payload.
 *
 * `destroy` is required and receives `ctx`. `clone` is optional; message clone,
 * retry, and fan-out return SALTS_ENOTSUP when it is absent. Existing parsed
 * data must be cleared by its owner before binding a projection. When the
 * message descriptor declares a schema, projection encoding and schema
 * name/type/version must match it exactly.
 */
TURBO_FLOW_C_API int turbo_flow_msg_bind_projection(turbo_flow_msg_t *msg,
                                             const turbo_flow_data_schema_t *schema,
                                             void *projection, turbo_flow_projection_clone_fn clone,
                                             turbo_flow_destroy_fn destroy, void *ctx);

/** Return the attached schema projection, or NULL for opaque content. */
TURBO_FLOW_C_API const void *turbo_flow_msg_projection(const turbo_flow_msg_t *msg,
                                                const turbo_flow_data_schema_t **schema_out);

/** Remove and destroy only a schema-bound projection; an attached descriptor is preserved. */
TURBO_FLOW_C_API void turbo_flow_msg_clear_projection(turbo_flow_msg_t *msg);
/** Remove the descriptor and destroy any schema-bound projection. Payload bytes are preserved. */
TURBO_FLOW_C_API void turbo_flow_msg_clear_content(turbo_flow_msg_t *msg);

/**
 * Borrow an immutable descriptor without changing payload bytes.
 * The descriptor owner must outlive the message and all of its clones.
 * A descriptor declaring a schema must match any existing projection.
 */
TURBO_FLOW_C_API int
turbo_flow_msg_set_content_descriptor(turbo_flow_msg_t *msg,
                                      const turbo_flow_content_descriptor_t *descriptor);

/** Copy a descriptor into message-owned storage for asynchronous propagation. */
TURBO_FLOW_C_API int
turbo_flow_msg_copy_content_descriptor(turbo_flow_msg_t *msg,
                                       const turbo_flow_content_descriptor_t *descriptor);

/** Return the immutable borrowed or message-owned descriptor, or NULL. */
TURBO_FLOW_C_API const turbo_flow_content_descriptor_t *
turbo_flow_msg_content_descriptor(const turbo_flow_msg_t *msg);

/** Return non-zero only when the descriptor storage belongs to the message. */
TURBO_FLOW_C_API int turbo_flow_msg_content_descriptor_owned(const turbo_flow_msg_t *msg);

/** Explicit host-owned trusted schema registry; no process-global registry is created. */
TURBO_FLOW_C_API turbo_flow_schema_registry_t *turbo_flow_schema_registry_create(void);
TURBO_FLOW_C_API void turbo_flow_schema_registry_destroy(turbo_flow_schema_registry_t *registry);

/**
 * Register one exact content match and a copied trusted schema descriptor.
 * `match.identity` is diagnostic and is not part of the registry key.
 * Runtime schema text is rejected; providers must compile/load schemas outside
 * this identity registry and register a descriptor with `schema_text == NULL`.
 */
TURBO_FLOW_C_API int turbo_flow_schema_registry_register(turbo_flow_schema_registry_t *registry,
                                                  const turbo_flow_content_descriptor_t *match,
                                                  const turbo_flow_data_schema_t *schema);

/**
 * Return a registry-owned stable schema pointer, or SALTS_ENOENT.
 * The pointer remains valid until the host destroys the registry; destruction
 * must be serialized after all adapters and callers stop using it.
 */
TURBO_FLOW_C_API int turbo_flow_schema_registry_resolve(const turbo_flow_schema_registry_t *registry,
                                                 const turbo_flow_content_descriptor_t *descriptor,
                                                 const turbo_flow_data_schema_t **schema_out);

/** Resolve the current message descriptor through a trusted registry. */
TURBO_FLOW_C_API int turbo_flow_msg_resolve_schema(const turbo_flow_msg_t *msg,
                                            const turbo_flow_schema_registry_t *registry,
                                            const turbo_flow_data_schema_t **schema_out);

/** Execute an explicit bounded adapter retry policy with isolated per-attempt messages. */
TURBO_FLOW_C_API int turbo_flow_retry_execute(const turbo_flow_retry_policy_t *policy,
                                       turbo_flow_msg_t *msg, const turbo_flow_retry_ops_t *ops,
                                       void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_H */
