#ifndef TURBO_FLOW_DOMAIN_H
#define TURBO_FLOW_DOMAIN_H

#include "turbo_flow_export.h"
#include "platform.h"

#include <cmeta/data.h>
#include <cmeta/function.h>

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
  TURBO_FLOW_STATE_SCOPE_PROTOCOL_SESSION,
  /** Mutable state remains owned by the native adapter instance. */
  TURBO_FLOW_STATE_SCOPE_ADAPTER_OWNER
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
  TURBO_FLOW_SETTLEMENT_ACKNOWLEDGE = 1u << 4,
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
 * resource primitive binding is required. `size` must equal
 * `sizeof(turbo_flow_operation_descriptor_t)`.
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
  /** Inclusive primitive version lower bound; zero means any current primitive version. */
  uint32_t resource_min_version;
  /** Inclusive upper bound; zero means unbounded when resource_min_version is nonzero. */
  uint32_t resource_max_version;
} turbo_flow_operation_descriptor_t;

/*
 * Reflected-operation port mapping.
 *
 * Native parameter/result type, direction, effects and properties remain
 * authoritative in cmeta_function_desc. TurboFlow adds only graph-port and
 * domain/lifecycle binding. `data` is canonical CMeta data identity for the
 * graph payload and its storage_type must equal the mapped FunctionDesc type.
 */
typedef enum turbo_flow_operation_port_direction_e {
  TURBO_FLOW_OPERATION_PORT_INPUT = 1,
  TURBO_FLOW_OPERATION_PORT_OUTPUT
} turbo_flow_operation_port_direction_t;

typedef enum turbo_flow_operation_value_kind_e {
  TURBO_FLOW_OPERATION_VALUE_PARAMETER = 1,
  TURBO_FLOW_OPERATION_VALUE_RETURN
} turbo_flow_operation_value_kind_t;

typedef struct turbo_flow_operation_port_binding_s {
  size_t size;
  uint32_t port_index;
  turbo_flow_operation_port_direction_t direction;
  turbo_flow_operation_value_kind_t value_kind;
  /* SIZE_MAX for RETURN; exact FunctionDesc parameter index otherwise. */
  size_t parameter_index;
  const cmeta_data_desc *data;
} turbo_flow_operation_port_binding_t;

#define TURBO_FLOW_OPERATION_PORT_BINDING_INIT \
  {sizeof(turbo_flow_operation_port_binding_t), 0u, TURBO_FLOW_OPERATION_PORT_INPUT, \
   TURBO_FLOW_OPERATION_VALUE_PARAMETER, SIZE_MAX, NULL}

typedef enum turbo_flow_reflected_lowering_e {
  /* Canonical semantics only; Graph execution through this operation is rejected. */
  TURBO_FLOW_REFLECTED_LOWERING_NONE = 0,
  /* Explicit semantic intent: one IN parameter -> return value CFlow MAP. */
  TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP
} turbo_flow_reflected_lowering_t;

/*
 * Canonical reflected operation registration.
 *
 * `operation` owns only TurboFlow domain/lifecycle policy. To avoid a second
 * native type registry its input/output type fields and domains must be empty;
 * TurboFlow derives its internal graph type keys from `ports[].data`.
 *
 * FunctionDesc/FunctionAbi and adapter are borrowed for the Flow registry
 * lifetime. A module/Plugin provider must retain their code/descriptors for at
 * least that lifetime. The compiled plan copies already-admitted concrete
 * CMeta types/effects/callable state and performs no runtime reflection lookup.
 */
typedef struct turbo_flow_reflected_operation_registration_s {
  size_t size;
  const turbo_flow_operation_descriptor_t *operation;
  const cmeta_function_desc *function;
  const cmeta_function_abi_desc *abi;
  cmeta_callable adapter;
  const turbo_flow_operation_port_binding_t *ports;
  size_t port_count;
  turbo_flow_reflected_lowering_t lowering;
} turbo_flow_reflected_operation_registration_t;

#define TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT \
  {sizeof(turbo_flow_reflected_operation_registration_t), NULL, NULL, NULL, {0}, NULL, 0u, \
   TURBO_FLOW_REFLECTED_LOWERING_NONE}

typedef struct turbo_flow_reflected_operation_view_s {
  size_t size;
  const cmeta_function_desc *function;
  const cmeta_function_abi_desc *abi;
  const turbo_flow_operation_port_binding_t *ports;
  size_t port_count;
  turbo_flow_reflected_lowering_t lowering;
} turbo_flow_reflected_operation_view_t;

#define TURBO_FLOW_REFLECTED_OPERATION_VIEW_INIT \
  {sizeof(turbo_flow_reflected_operation_view_t), NULL, NULL, NULL, 0u, \
   TURBO_FLOW_REFLECTED_LOWERING_NONE}

/** Stable module boundary advertised to the graph compiler and management code. */
typedef enum turbo_flow_module_capability_flags_e {
  /** The module exports typed operations that may be referenced by Graph DSL nodes. */
  TURBO_FLOW_MODULE_GRAPH_OPERATIONS = 1u << 0,
  /** The module owns resources whose lifecycle/status is managed outside the data graph. */
  TURBO_FLOW_MODULE_MANAGED_RESOURCES = 1u << 1,
  /** The module retains a domain-native API/runtime below its graph adapter boundary. */
  TURBO_FLOW_MODULE_NATIVE_API = 1u << 2
} turbo_flow_module_capability_flags_t;

/** One already-registered module dependency. Version bounds are inclusive. */
typedef struct turbo_flow_module_requirement_s {
  size_t size;
  const char *module_name;
  uint32_t min_version;
  /** Zero means that the dependency has no upper version bound. */
  uint32_t max_version;
  /** Required turbo_flow_module_capability_flags_t bits. */
  uint32_t capability_flags;
} turbo_flow_module_requirement_t;

/**
 * Immutable module catalog entry copied into a flow.
 *
 * Primitive exports are stable `type_name` contracts, not configured resource
 * binding names. Operation exports must already exist in the operation registry.
 * Requirements must already exist, so callers register modules in dependency
 * order. The catalog describes boundaries; it does not construct resources or
 * replace a module's native transport/runtime.
 */
typedef struct turbo_flow_module_descriptor_s {
  size_t size;
  const char *name;
  uint32_t version;
  /** Bitwise turbo_flow_module_capability_flags_t values. */
  uint32_t capability_flags;
  const char *const *primitive_types;
  size_t primitive_type_count;
  const char *const *operation_names;
  size_t operation_count;
  const turbo_flow_module_requirement_t *requirements;
  size_t requirement_count;
} turbo_flow_module_descriptor_t;

/**
 * Register one immutable primitive contract before compile.
 *
 * Returns SALTS_OK, SALTS_EINVAL for an invalid descriptor, SALTS_EALREADY for
 * a duplicate name, SALTS_EBUSY after compile/start, or SALTS_ENOMEM.
 */
TURBO_FLOW_C_API int turbo_flow_register_primitive(turbo_flow_t *flow,
                                            const turbo_flow_primitive_descriptor_t *descriptor);

/**
 * Register one immutable operation contract before compile.
 *
 * Cross-domain input/output requires TURBO_FLOW_OPERATION_BRIDGE. Resource-owner
 * or protocol-session state scope requires a resource contract; adapter-owner
 * state requires a typed module-adapter binding when compiled. Returns the same
 * registration errors as turbo_flow_register_primitive().
 */
TURBO_FLOW_C_API int turbo_flow_register_operation(turbo_flow_t *flow,
                                            const turbo_flow_operation_descriptor_t *descriptor);

/*
 * Register canonical native function semantics for one TurboFlow operation.
 *
 * This is control-plane only. A CFLOW_MAP lowering is admitted immediately
 * through CFlow's reflected-function admission. NONE keeps valid canonical
 * semantics/port mapping but is not executable as a graph stage.
 */
TURBO_FLOW_C_API int turbo_flow_register_reflected_operation(
    turbo_flow_t *flow,
    const turbo_flow_reflected_operation_registration_t *registration);

/* Borrow canonical reflected semantics from the immutable Flow registry. */
TURBO_FLOW_C_API int turbo_flow_reflected_operation(
    const turbo_flow_t *flow, const char *operation_name,
    turbo_flow_reflected_operation_view_t *out);

/**
 * Register one immutable module catalog entry before compile.
 *
 * Exported operation names and dependencies are validated against registrations
 * already present in `flow`. One operation has one module owner. Returns
 * SALTS_ENOENT for a missing export/dependency and SALTS_EPROTO for an
 * incompatible dependency contract, in addition to the normal registration
 * errors.
 */
TURBO_FLOW_C_API int turbo_flow_register_module(turbo_flow_t *flow,
                                         const turbo_flow_module_descriptor_t *descriptor);

/**
 * Idempotently register one complete module contract and its operation descriptors.
 * Existing entries must be exactly compatible. Newly appended operations are
 * rolled back if module registration fails.
 */
TURBO_FLOW_C_API int turbo_flow_register_module_contract(
    turbo_flow_t *flow, const turbo_flow_module_descriptor_t *module,
    const turbo_flow_operation_descriptor_t *operations, size_t operation_count);

/**
 * Associate an existing typed operation provider with its exporting module.
 *
 * A resource-bound provider must reference a primitive whose `type_name` is
 * exported by the module. The association is immutable and borrowed query
 * results remain valid until registry-clearing reset or flow destruction.
 */
TURBO_FLOW_C_API int turbo_flow_bind_operation_provider_module(turbo_flow_t *flow,
                                                        const char *module_name,
                                                        const char *operation_name,
                                                        const char *resource_name);

TURBO_FLOW_C_API size_t turbo_flow_primitive_count(const turbo_flow_t *flow);
TURBO_FLOW_C_API size_t turbo_flow_operation_count(const turbo_flow_t *flow);
TURBO_FLOW_C_API size_t turbo_flow_module_count(const turbo_flow_t *flow);
TURBO_FLOW_C_API const turbo_flow_primitive_descriptor_t *turbo_flow_primitive_at(const turbo_flow_t *flow,
                                                                           size_t index);
TURBO_FLOW_C_API const turbo_flow_operation_descriptor_t *turbo_flow_operation_at(const turbo_flow_t *flow,
                                                                           size_t index);
TURBO_FLOW_C_API const turbo_flow_module_descriptor_t *turbo_flow_module_at(const turbo_flow_t *flow,
                                                                     size_t index);
TURBO_FLOW_C_API const turbo_flow_primitive_descriptor_t *
turbo_flow_find_primitive(const turbo_flow_t *flow, const char *name);
TURBO_FLOW_C_API const turbo_flow_operation_descriptor_t *
turbo_flow_find_operation(const turbo_flow_t *flow, const char *name);
TURBO_FLOW_C_API const turbo_flow_module_descriptor_t *turbo_flow_find_module(const turbo_flow_t *flow,
                                                                       const char *name);
TURBO_FLOW_C_API const char *turbo_flow_operation_provider_module(const turbo_flow_t *flow,
                                                           const char *operation_name,
                                                           const char *resource_name);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_DOMAIN_H */
