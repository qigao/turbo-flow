#ifndef TURBO_FLOW_DOMAIN_H
#define TURBO_FLOW_DOMAIN_H

#include "platform.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct turbo_flow_s turbo_flow_t;

typedef enum turbo_flow_domain_e {
  TURBO_FLOW_DOMAIN_NONE = 0,
  TURBO_FLOW_DOMAIN_DATA,
  TURBO_FLOW_DOMAIN_EXECUTION,
  TURBO_FLOW_DOMAIN_IO_TRANSPORT,
  TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
  TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
  TURBO_FLOW_DOMAIN_RULES,
  TURBO_FLOW_DOMAIN_MANAGEMENT
} turbo_flow_domain_t;

typedef enum turbo_flow_primitive_kind_e {
  TURBO_FLOW_PRIMITIVE_VALUE = 0,
  TURBO_FLOW_PRIMITIVE_RESOURCE
} turbo_flow_primitive_kind_t;

/**
 * One registered domain primitive binding.
 *
 * `name` identifies the binding referenced by DSL `resource`; `type_name`
 * identifies the stable domain type required by an operation. The registry
 * copies both strings.
 */
typedef struct turbo_flow_primitive_descriptor_s {
  size_t size;
  const char *name;
  const char *type_name;
  uint32_t version;
  turbo_flow_domain_t domain;
  turbo_flow_primitive_kind_t kind;
} turbo_flow_primitive_descriptor_t;

typedef enum turbo_flow_data_scope_e {
  TURBO_FLOW_DATA_SCOPE_NONE = 0,
  TURBO_FLOW_DATA_SCOPE_MESSAGE,
  TURBO_FLOW_DATA_SCOPE_BATCH,
  TURBO_FLOW_DATA_SCOPE_STREAM_CHUNK,
  TURBO_FLOW_DATA_SCOPE_SNAPSHOT
} turbo_flow_data_scope_t;

typedef enum turbo_flow_state_scope_e {
  TURBO_FLOW_STATE_SCOPE_NONE = 0,
  TURBO_FLOW_STATE_SCOPE_PRIVATE,
  TURBO_FLOW_STATE_SCOPE_NODE,
  TURBO_FLOW_STATE_SCOPE_GRAPH,
  TURBO_FLOW_STATE_SCOPE_RESOURCE_OWNER,
  TURBO_FLOW_STATE_SCOPE_PROTOCOL_SESSION
} turbo_flow_state_scope_t;

typedef enum turbo_flow_lifetime_scope_e {
  TURBO_FLOW_LIFETIME_CALL = 0,
  TURBO_FLOW_LIFETIME_DISPATCH,
  TURBO_FLOW_LIFETIME_TASK,
  TURBO_FLOW_LIFETIME_SESSION_GENERATION,
  TURBO_FLOW_LIFETIME_RUNTIME_GENERATION,
  TURBO_FLOW_LIFETIME_PERSISTENT
} turbo_flow_lifetime_scope_t;

typedef enum turbo_flow_concurrency_scope_e {
  TURBO_FLOW_CONCURRENCY_INLINE_LANE = 0,
  TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT,
  TURBO_FLOW_CONCURRENCY_POOL,
  TURBO_FLOW_CONCURRENCY_LOCK_FREE_SNAPSHOT
} turbo_flow_concurrency_scope_t;

typedef enum turbo_flow_authority_scope_e {
  TURBO_FLOW_AUTHORITY_PURE = 0,
  TURBO_FLOW_AUTHORITY_OBSERVE_ONLY,
  TURBO_FLOW_AUTHORITY_DATA_MUTATION,
  TURBO_FLOW_AUTHORITY_OWNER_LOCAL,
  TURBO_FLOW_AUTHORITY_OWNER_COMMAND
} turbo_flow_authority_scope_t;

typedef struct turbo_flow_operation_scope_s {
  turbo_flow_data_scope_t data;
  turbo_flow_state_scope_t state;
  turbo_flow_lifetime_scope_t lifetime;
  turbo_flow_concurrency_scope_t concurrency;
  turbo_flow_authority_scope_t authority;
} turbo_flow_operation_scope_t;

typedef enum turbo_flow_operation_flags_e {
  TURBO_FLOW_OPERATION_SOURCE = 1u << 0,
  TURBO_FLOW_OPERATION_STAGE = 1u << 1,
  /** The operation explicitly converts between different input/output domains. */
  TURBO_FLOW_OPERATION_BRIDGE = 1u << 2
} turbo_flow_operation_flags_t;

typedef enum turbo_flow_operation_exec_mask_e {
  TURBO_FLOW_OPERATION_EXEC_INLINE = 1u << 0,
  TURBO_FLOW_OPERATION_EXEC_THREAD = 1u << 1,
  TURBO_FLOW_OPERATION_EXEC_CORO = 1u << 2
} turbo_flow_operation_exec_mask_t;

typedef enum turbo_flow_handoff_kind_e {
  /** No queue/ring is required at this operation boundary. */
  TURBO_FLOW_HANDOFF_DIRECT = 0,
  /** A bounded runtime segment transfers ownership to this operation. */
  TURBO_FLOW_HANDOFF_BOUNDED
} turbo_flow_handoff_kind_t;

typedef enum turbo_flow_ordering_kind_e {
  TURBO_FLOW_ORDERING_UNORDERED = 0,
  /** Output completion must preserve the operation input sequence. */
  TURBO_FLOW_ORDERING_PRESERVE_INPUT
} turbo_flow_ordering_kind_t;

typedef enum turbo_flow_backpressure_kind_e {
  TURBO_FLOW_BACKPRESSURE_NONE = 0,
  TURBO_FLOW_BACKPRESSURE_BLOCK,
  TURBO_FLOW_BACKPRESSURE_FAIL,
  TURBO_FLOW_BACKPRESSURE_DROP_NEWEST,
  TURBO_FLOW_BACKPRESSURE_DROP_OLDEST
} turbo_flow_backpressure_kind_t;

typedef enum turbo_flow_cancellation_kind_e {
  TURBO_FLOW_CANCELLATION_NONE = 0,
  TURBO_FLOW_CANCELLATION_COOPERATIVE
} turbo_flow_cancellation_kind_t;

typedef enum turbo_flow_error_mode_e {
  TURBO_FLOW_ERROR_PROPAGATE = 0,
  TURBO_FLOW_ERROR_REJECT,
  TURBO_FLOW_ERROR_RETRY,
  TURBO_FLOW_ERROR_SETTLE
} turbo_flow_error_mode_t;

typedef enum turbo_flow_settlement_flags_e {
  TURBO_FLOW_SETTLEMENT_COMPLETE = 1u << 0,
  TURBO_FLOW_SETTLEMENT_RETRY = 1u << 1,
  TURBO_FLOW_SETTLEMENT_REQUEUE = 1u << 2,
  TURBO_FLOW_SETTLEMENT_DEAD_LETTER = 1u << 3,
  TURBO_FLOW_SETTLEMENT_PROTOCOL_ACK = 1u << 4,
  TURBO_FLOW_SETTLEMENT_CANCELED = 1u << 5
} turbo_flow_settlement_flags_t;

/** Static runtime boundary required by one operation. */
typedef struct turbo_flow_operation_runtime_contract_s {
  turbo_flow_handoff_kind_t handoff;
  turbo_flow_ordering_kind_t ordering;
  turbo_flow_backpressure_kind_t backpressure;
  turbo_flow_cancellation_kind_t cancellation;
  turbo_flow_error_mode_t error_mode;
  /** Required bounded segment capacity; zero for direct handoff. */
  uint32_t capacity;
  /** Zero means no operation-level deadline. */
  uint64_t deadline_ms;
  /** Bitwise turbo_flow_settlement_flags_t values; zero means no settlement action. */
  uint32_t settlement;
} turbo_flow_operation_runtime_contract_t;

#define TURBO_FLOW_OPERATION_MAX_SEGMENT_CAPACITY 1048576u
#define TURBO_FLOW_OPERATION_MAX_DEADLINE_MS UINT64_C(3600000)

/**
 * Immutable operation contract copied into the flow registry.
 *
 * Type names are domain-local stable identifiers. A NULL input/output type
 * means the operation has no graph input/output on that side. Resource fields
 * are both NULL for a stateless operation, or both non-NULL/non-NONE when a
 * resource primitive binding is required. `size` must cover this structure.
 */
typedef struct turbo_flow_operation_descriptor_s {
  size_t size;
  const char *name;
  uint32_t version;
  turbo_flow_domain_t domain;
  turbo_flow_domain_t input_domain;
  const char *input_type;
  turbo_flow_domain_t output_domain;
  const char *output_type;
  turbo_flow_domain_t resource_domain;
  const char *resource_type;
  turbo_flow_operation_scope_t scope;
  uint32_t flags;
  uint32_t execution_mask;
  turbo_flow_operation_runtime_contract_t runtime;
} turbo_flow_operation_descriptor_t;

/**
 * Register one immutable primitive contract before compile.
 *
 * Returns TURBO_OK, TURBO_EINVAL for an invalid descriptor, TURBO_EALREADY for
 * a duplicate name, TURBO_EBUSY after compile/start, or TURBO_ENOMEM.
 */
CXX_C_API int turbo_flow_register_primitive(turbo_flow_t *flow,
                                            const turbo_flow_primitive_descriptor_t *descriptor);

/**
 * Register one immutable operation contract before compile.
 *
 * Cross-domain input/output requires TURBO_FLOW_OPERATION_BRIDGE. Owner or
 * protocol-session state scope requires a resource contract. Returns the same
 * registration errors as turbo_flow_register_primitive().
 */
CXX_C_API int turbo_flow_register_operation(turbo_flow_t *flow,
                                            const turbo_flow_operation_descriptor_t *descriptor);

CXX_C_API size_t turbo_flow_primitive_count(const turbo_flow_t *flow);
CXX_C_API size_t turbo_flow_operation_count(const turbo_flow_t *flow);
CXX_C_API const turbo_flow_primitive_descriptor_t *turbo_flow_primitive_at(const turbo_flow_t *flow,
                                                                           size_t index);
CXX_C_API const turbo_flow_operation_descriptor_t *turbo_flow_operation_at(const turbo_flow_t *flow,
                                                                           size_t index);
CXX_C_API const turbo_flow_primitive_descriptor_t *
turbo_flow_find_primitive(const turbo_flow_t *flow, const char *name);
CXX_C_API const turbo_flow_operation_descriptor_t *
turbo_flow_find_operation(const turbo_flow_t *flow, const char *name);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_DOMAIN_H */
