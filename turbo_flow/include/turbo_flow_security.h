#ifndef TURBO_FLOW_SECURITY_H
#define TURBO_FLOW_SECURITY_H

#include "turbo_flow_config.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_SECURITY_ABI_V3 3u
#define TURBO_FLOW_SECURITY_ID_MAX 255u
#define TURBO_FLOW_SECURITY_TYPE_MAX 63u
#define TURBO_FLOW_SECURITY_PATTERN_MAX 511u
#define TURBO_FLOW_SECURITY_MAX_ROLES 8u
#define TURBO_FLOW_SECURITY_MAX_GROUPS 16u
#define TURBO_FLOW_SECURITY_MAX_RULES 4096u
#define TURBO_FLOW_SECURITY_SECRET_REF_MAX 255u
#define TURBO_FLOW_SECURITY_RULE_LINE_MAX 2047u

typedef struct turbo_flow_security_realm_s turbo_flow_security_realm_t;
typedef struct turbo_flow_security_policy_provider_owner_s
    turbo_flow_security_policy_provider_owner_t;

typedef enum turbo_flow_security_scope_e {
  TURBO_FLOW_SECURITY_SCOPE_SELF = 1,
  TURBO_FLOW_SECURITY_SCOPE_GROUP,
  TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP,
  TURBO_FLOW_SECURITY_SCOPE_SYSTEM
} turbo_flow_security_scope_t;

/** Authentication output copied by value and safe to pass into authorization. */
typedef struct turbo_flow_security_principal_s {
  size_t size;
  uint32_t abi_version;
  char principal_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char principal_type[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  char root_group_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char auth_method[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  turbo_flow_security_scope_t scope;
  uint32_t role_count;
  char roles[TURBO_FLOW_SECURITY_MAX_ROLES][TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  uint32_t group_count;
  /** Direct groups and their ancestors; non-SYSTEM principals must include root_group_id. */
  char groups[TURBO_FLOW_SECURITY_MAX_GROUPS][TURBO_FLOW_SECURITY_ID_MAX + 1u];
  /** Unix epoch seconds; zero means the provider did not assign an expiry. */
  uint64_t expires_at;
  /** Required realm generation; authorization rejects zero and stale generations. */
  uint64_t policy_version;
} turbo_flow_security_principal_t;

#define TURBO_FLOW_SECURITY_PRINCIPAL_INIT                                                         \
  {sizeof(turbo_flow_security_principal_t), TURBO_FLOW_SECURITY_ABI_V3}

/** Borrowed credentials visible only for the duration of authenticate(). */
typedef struct turbo_flow_security_auth_request_s {
  size_t size;
  const char *identity;
  const char *method;
  const uint8_t *secret;
  size_t secret_size;
  const char *remote_address;
  const char *protocol;
} turbo_flow_security_auth_request_t;

#define TURBO_FLOW_SECURITY_AUTH_REQUEST_INIT {sizeof(turbo_flow_security_auth_request_t)}

typedef int (*turbo_flow_security_authenticate_fn)(
    void *ctx, const turbo_flow_security_auth_request_t *request,
    turbo_flow_security_principal_t *principal_out);

typedef struct turbo_flow_security_auth_provider_s {
  size_t size;
  void *ctx;
  turbo_flow_security_authenticate_fn authenticate;
} turbo_flow_security_auth_provider_t;

#define TURBO_FLOW_SECURITY_AUTH_PROVIDER_INIT                                                     \
  {sizeof(turbo_flow_security_auth_provider_t), NULL, NULL}

typedef enum turbo_flow_security_enhanced_auth_status_e {
  TURBO_FLOW_SECURITY_ENHANCED_AUTH_CONTINUE = 1,
  TURBO_FLOW_SECURITY_ENHANCED_AUTH_SUCCESS = 2
} turbo_flow_security_enhanced_auth_status_t;

/** Borrowed MQTT-style enhanced authentication input for one exchange step. */
typedef struct turbo_flow_security_enhanced_auth_request_s {
  size_t size;
  const char *identity;
  const char *method;
  const uint8_t *data;
  size_t data_size;
  const char *remote_address;
  const char *protocol;
} turbo_flow_security_enhanced_auth_request_t;

#define TURBO_FLOW_SECURITY_ENHANCED_AUTH_REQUEST_INIT                                             \
  {sizeof(turbo_flow_security_enhanced_auth_request_t), NULL, NULL, NULL, 0u, NULL, NULL}

/** Provider-owned output spans remain valid until the next provider callback or cancel(). */
typedef struct turbo_flow_security_enhanced_auth_result_s {
  size_t size;
  turbo_flow_security_enhanced_auth_status_t status;
  const uint8_t *data;
  size_t data_size;
  turbo_flow_security_principal_t principal;
} turbo_flow_security_enhanced_auth_result_t;

#define TURBO_FLOW_SECURITY_ENHANCED_AUTH_RESULT_INIT                                              \
  {sizeof(turbo_flow_security_enhanced_auth_result_t), 0, NULL, 0u,                                \
   TURBO_FLOW_SECURITY_PRINCIPAL_INIT}

typedef int (*turbo_flow_security_enhanced_auth_begin_fn)(
    void *ctx, const turbo_flow_security_enhanced_auth_request_t *request, void **exchange_out,
    turbo_flow_security_enhanced_auth_result_t *result_out);
typedef int (*turbo_flow_security_enhanced_auth_continue_fn)(
    void *ctx, void *exchange, const turbo_flow_security_enhanced_auth_request_t *request,
    turbo_flow_security_enhanced_auth_result_t *result_out);
typedef void (*turbo_flow_security_enhanced_auth_cancel_fn)(void *ctx, void *exchange);

typedef struct turbo_flow_security_enhanced_auth_provider_s {
  size_t size;
  void *ctx;
  turbo_flow_security_enhanced_auth_begin_fn begin;
  turbo_flow_security_enhanced_auth_continue_fn continue_exchange;
  turbo_flow_security_enhanced_auth_cancel_fn cancel;
} turbo_flow_security_enhanced_auth_provider_t;

#define TURBO_FLOW_SECURITY_ENHANCED_AUTH_PROVIDER_INIT                                            \
  {sizeof(turbo_flow_security_enhanced_auth_provider_t), NULL, NULL, NULL, NULL}

CXX_C_API int turbo_flow_security_enhanced_auth_begin(
    const turbo_flow_security_enhanced_auth_provider_t *provider,
    const turbo_flow_security_enhanced_auth_request_t *request, void **exchange_out,
    turbo_flow_security_enhanced_auth_result_t *result_out);
CXX_C_API int turbo_flow_security_enhanced_auth_continue(
    const turbo_flow_security_enhanced_auth_provider_t *provider, void *exchange,
    const turbo_flow_security_enhanced_auth_request_t *request,
    turbo_flow_security_enhanced_auth_result_t *result_out);
CXX_C_API void turbo_flow_security_enhanced_auth_cancel(
    const turbo_flow_security_enhanced_auth_provider_t *provider, void *exchange);

typedef enum turbo_flow_security_action_e {
  TURBO_FLOW_SECURITY_ACTION_CONNECT = 1u << 0,
  TURBO_FLOW_SECURITY_ACTION_PUBLISH = 1u << 1,
  TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE = 1u << 2,
  TURBO_FLOW_SECURITY_ACTION_READ = 1u << 3,
  TURBO_FLOW_SECURITY_ACTION_WRITE = 1u << 4,
  TURBO_FLOW_SECURITY_ACTION_EXECUTE = 1u << 5,
  TURBO_FLOW_SECURITY_ACTION_ADMIN = 1u << 6
} turbo_flow_security_action_t;

#define TURBO_FLOW_SECURITY_ACTION_ALL UINT32_C(0x7f)

typedef enum turbo_flow_security_resource_type_e {
  TURBO_FLOW_SECURITY_RESOURCE_GENERIC = 1,
  TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC,
  TURBO_FLOW_SECURITY_RESOURCE_FLOW_RESOURCE,
  TURBO_FLOW_SECURITY_RESOURCE_SQL_OBJECT,
  TURBO_FLOW_SECURITY_RESOURCE_SECRET
} turbo_flow_security_resource_type_t;

typedef enum turbo_flow_security_effect_e {
  TURBO_FLOW_SECURITY_DENY = 0,
  TURBO_FLOW_SECURITY_ALLOW = 1
} turbo_flow_security_effect_t;

typedef enum turbo_flow_security_subject_kind_e {
  TURBO_FLOW_SECURITY_SUBJECT_ANY = 0,
  TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL,
  TURBO_FLOW_SECURITY_SUBJECT_ROLE,
  TURBO_FLOW_SECURITY_SUBJECT_GROUP
} turbo_flow_security_subject_kind_t;

typedef enum turbo_flow_security_match_kind_e {
  TURBO_FLOW_SECURITY_MATCH_EXACT = 0,
  TURBO_FLOW_SECURITY_MATCH_PREFIX,
  /** Delegates protocol-specific semantics, such as MQTT topic filters, to matcher. */
  TURBO_FLOW_SECURITY_MATCH_ADAPTER
} turbo_flow_security_match_kind_t;

/** Pointer-free rule copied into one immutable realm generation. */
typedef struct turbo_flow_security_rule_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_security_effect_t effect;
  turbo_flow_security_subject_kind_t subject_kind;
  char subject[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  /** Immutable security-tree root. Every rule belongs to exactly one root group. */
  char root_group_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  uint32_t action_mask;
  turbo_flow_security_resource_type_t resource_type;
  turbo_flow_security_match_kind_t match_kind;
  char pattern[TURBO_FLOW_SECURITY_PATTERN_MAX + 1u];
} turbo_flow_security_rule_t;

#define TURBO_FLOW_SECURITY_RULE_INIT                                                              \
  {sizeof(turbo_flow_security_rule_t), TURBO_FLOW_SECURITY_ABI_V3, TURBO_FLOW_SECURITY_DENY}

/**
 * Parse one canonical ACL rule line:
 * `effect|subject_kind|subject|root_group|actions|resource_type|match_kind|pattern`.
 * `subject` must be `*` for `any`; actions are comma-separated. `\\`, `\|`, and
 * `\xHH` escapes are accepted in subject, root_group, and pattern fields.
 *
 * The parser copies into `rule_out`, performs no allocation, and returns TURBO_OK,
 * TURBO_EINVAL for invalid pointers/capacities, or TURBO_EPROTO for invalid syntax.
 */
CXX_C_API int turbo_flow_security_rule_parse_line(const char *line, size_t line_size,
                                                  turbo_flow_security_rule_t *rule_out);
/**
 * Serialize one validated rule to the canonical line representation.
 * `line_out` is not NUL-terminated by contract; `line_size_out` receives its byte count.
 * Returns TURBO_OK, TURBO_EINVAL for invalid input, or TURBO_ENOSPC for insufficient capacity.
 */
CXX_C_API int turbo_flow_security_rule_format_line(const turbo_flow_security_rule_t *rule,
                                                   char *line_out, size_t line_capacity,
                                                   size_t *line_size_out);

/** Immutable provider-owned policy view. Rules remain borrowed until release(). */
typedef struct turbo_flow_security_policy_bundle_s {
  size_t size;
  uint32_t abi_version;
  uint64_t policy_version;
  /** Unix epoch seconds; zero means the bundle does not expire. */
  uint64_t expires_at;
  const turbo_flow_security_rule_t *rules;
  size_t rule_count;
  void *provider_bundle;
} turbo_flow_security_policy_bundle_t;

#define TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT                                                     \
  {sizeof(turbo_flow_security_policy_bundle_t), TURBO_FLOW_SECURITY_ABI_V3, 0u, 0u, NULL, 0u, NULL}

/**
 * Load the requested immutable policy generation.
 *
 * required_version is positive for authorization refreshes and zero for the provider's current
 * generation. Implementations may yield when called from a coroutine, but must not retain realm
 * or request pointers. A successful load must be paired with release().
 */
typedef int (*turbo_flow_security_policy_load_fn)(void *ctx, uint64_t required_version,
                                                  turbo_flow_security_policy_bundle_t *bundle_out);
typedef void (*turbo_flow_security_policy_release_fn)(void *ctx,
                                                      turbo_flow_security_policy_bundle_t *bundle);

typedef struct turbo_flow_security_policy_provider_s {
  size_t size;
  void *ctx;
  turbo_flow_security_policy_load_fn load;
  turbo_flow_security_policy_release_fn release;
} turbo_flow_security_policy_provider_t;

#define TURBO_FLOW_SECURITY_POLICY_PROVIDER_INIT                                                   \
  {sizeof(turbo_flow_security_policy_provider_t), NULL, NULL, NULL}

typedef struct turbo_flow_security_request_s {
  size_t size;
  const turbo_flow_security_principal_t *principal;
  const char *root_group_id;
  uint32_t action;
  turbo_flow_security_resource_type_t resource_type;
  const char *resource;
  /** Protocol-owner facts available to an injected matcher; Core never dereferences it. */
  const void *protocol_context;
} turbo_flow_security_request_t;

#define TURBO_FLOW_SECURITY_REQUEST_INIT {sizeof(turbo_flow_security_request_t)}

/**
 * Immutable adapter leaf input borrowed only for compile_leaf(). Candidate indices are ordered
 * positions in rules and remain owned by Core. A compiled leaf may borrow rule strings until its
 * destroy_leaf() call; it must not retain candidate_rule_indices.
 */
typedef struct turbo_flow_security_matcher_leaf_s {
  size_t size;
  const turbo_flow_security_rule_t *rules;
  size_t rule_count;
  const size_t *candidate_rule_indices;
  size_t candidate_count;
} turbo_flow_security_matcher_leaf_t;

#define TURBO_FLOW_SECURITY_MATCHER_LEAF_INIT                                                      \
  {sizeof(turbo_flow_security_matcher_leaf_t), NULL, 0u, NULL, 0u}

/** Emit one leaf-local candidate position. The adapter must stop if this returns an error. */
typedef int (*turbo_flow_security_match_emit_fn)(void *ctx, size_t candidate_position);

/** Build one immutable, concurrently readable adapter index for a subject leaf. */
typedef int (*turbo_flow_security_match_compile_leaf_fn)(
    void *ctx, const turbo_flow_security_matcher_leaf_t *leaf, void **compiled_leaf_out);

/**
 * Evaluate one compiled leaf without mutating it. Each match is emitted at most once and must be
 * in [0, candidate_count). Core validates emitted positions and retains all authorization policy.
 */
typedef int (*turbo_flow_security_match_evaluate_leaf_fn)(
    void *ctx, const void *compiled_leaf, const turbo_flow_security_request_t *request,
    turbo_flow_security_match_emit_fn emit, void *emit_ctx);

typedef void (*turbo_flow_security_match_destroy_leaf_fn)(void *ctx, void *compiled_leaf);

typedef struct turbo_flow_security_matcher_s {
  size_t size;
  uint32_t abi_version;
  void *ctx;
  turbo_flow_security_match_compile_leaf_fn compile_leaf;
  turbo_flow_security_match_evaluate_leaf_fn evaluate_leaf;
  turbo_flow_security_match_destroy_leaf_fn destroy_leaf;
} turbo_flow_security_matcher_t;

#define TURBO_FLOW_SECURITY_MATCHER_INIT                                                           \
  {sizeof(turbo_flow_security_matcher_t), TURBO_FLOW_SECURITY_ABI_V3, NULL, NULL, NULL, NULL}

typedef enum turbo_flow_security_decision_reason_e {
  TURBO_FLOW_SECURITY_REASON_DEFAULT_DENY = 1,
  TURBO_FLOW_SECURITY_REASON_ALLOW_RULE,
  TURBO_FLOW_SECURITY_REASON_DENY_RULE,
  TURBO_FLOW_SECURITY_REASON_ROOT_GROUP_MISMATCH,
  TURBO_FLOW_SECURITY_REASON_PRINCIPAL_EXPIRED,
  TURBO_FLOW_SECURITY_REASON_POLICY_VERSION_MISMATCH
} turbo_flow_security_decision_reason_t;

typedef struct turbo_flow_security_decision_s {
  size_t size;
  turbo_flow_security_effect_t effect;
  turbo_flow_security_decision_reason_t reason;
  size_t matched_rule;
  uint64_t policy_version;
} turbo_flow_security_decision_t;

#define TURBO_FLOW_SECURITY_DECISION_INIT                                                          \
  {sizeof(turbo_flow_security_decision_t), TURBO_FLOW_SECURITY_DENY,                               \
   TURBO_FLOW_SECURITY_REASON_DEFAULT_DENY, SIZE_MAX, 0u}

typedef struct turbo_flow_security_realm_config_s {
  size_t size;
  uint32_t abi_version;
  const char *resource_uid;
  const char *owner_name;
  uint64_t policy_version;
  const turbo_flow_security_rule_t *rules;
  size_t rule_count;
  turbo_flow_security_matcher_t matcher;
  /** Required when no initial rules are supplied; copied and resolved by the product root. */
  const char *policy_source;
} turbo_flow_security_realm_config_t;

#define TURBO_FLOW_SECURITY_REALM_CONFIG_INIT                                                      \
  {sizeof(turbo_flow_security_realm_config_t),                                                     \
   TURBO_FLOW_SECURITY_ABI_V3,                                                                     \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u,                                                                                             \
   TURBO_FLOW_SECURITY_MATCHER_INIT,                                                               \
   NULL}

/** Validate provider output and authenticate without retaining credential bytes. */
CXX_C_API int turbo_flow_security_authenticate(const turbo_flow_security_auth_provider_t *provider,
                                               const turbo_flow_security_auth_request_t *request,
                                               turbo_flow_security_principal_t *principal_out);

CXX_C_API int turbo_flow_security_realm_create(const turbo_flow_security_realm_config_t *config,
                                               turbo_flow_security_realm_t **out);
CXX_C_API void turbo_flow_security_realm_destroy(turbo_flow_security_realm_t *realm);

/** Borrowed configured source channel name, or NULL for a programmatic static realm. */
CXX_C_API const char *
turbo_flow_security_realm_policy_source(const turbo_flow_security_realm_t *realm);

/** Bind one borrowed provider before the realm becomes reachable by protocol adapters. */
CXX_C_API int turbo_flow_security_realm_bind_policy_provider(
    turbo_flow_security_realm_t *realm, const turbo_flow_security_policy_provider_t *provider);

/** Fetch, validate, copy, and atomically install one exact generation. */
CXX_C_API int turbo_flow_security_realm_refresh(turbo_flow_security_realm_t *realm,
                                                uint64_t required_version,
                                                uint64_t now_epoch_seconds);

/** Complete one deterministic decision. Deny is a valid result and returns TURBO_OK. */
CXX_C_API int turbo_flow_security_realm_evaluate(turbo_flow_security_realm_t *realm,
                                                 const turbo_flow_security_request_t *request,
                                                 uint64_t now_epoch_seconds,
                                                 turbo_flow_security_decision_t *decision);

/** Evaluate and map a deny decision to TURBO_EPERM for protocol-owner boundaries. */
CXX_C_API int turbo_flow_security_realm_authorize(turbo_flow_security_realm_t *realm,
                                                  const turbo_flow_security_request_t *request,
                                                  uint64_t now_epoch_seconds,
                                                  turbo_flow_security_decision_t *decision);

/** Register the realm as a borrowed, independently addressable SECURITY_REALM resource. */
CXX_C_API int turbo_flow_security_realm_register(turbo_flow_t *flow,
                                                 turbo_flow_security_realm_t *realm);

/**
 * Create a fail-closed realm from strict YAML metadata and a policy_source reference.
 * ACL rule bodies and policy versions are rejected in YAML.
 */
CXX_C_API int turbo_flow_security_realm_create_resolved(
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_matcher_t *matcher, turbo_flow_security_realm_t **out,
    turbo_flow_config_error_t *error);

/** Opaque provider-owned secret lease; it never belongs in a graph message or resource document. */
typedef struct turbo_flow_security_secret_lease_s {
  size_t size;
  const uint8_t *bytes;
  size_t byte_count;
  uint64_t version;
  uint64_t expires_at;
  void *provider_lease;
} turbo_flow_security_secret_lease_t;

#define TURBO_FLOW_SECURITY_SECRET_LEASE_INIT                                                      \
  {sizeof(turbo_flow_security_secret_lease_t), NULL, 0u, 0u, 0u, NULL}

typedef int (*turbo_flow_security_secret_acquire_fn)(void *ctx, const char *reference,
                                                     turbo_flow_security_secret_lease_t *lease_out);
typedef void (*turbo_flow_security_secret_release_fn)(void *ctx,
                                                      turbo_flow_security_secret_lease_t *lease);

typedef struct turbo_flow_security_key_provider_s {
  size_t size;
  void *ctx;
  turbo_flow_security_secret_acquire_fn acquire;
  turbo_flow_security_secret_release_fn release;
} turbo_flow_security_key_provider_t;

#define TURBO_FLOW_SECURITY_KEY_PROVIDER_INIT                                                      \
  {sizeof(turbo_flow_security_key_provider_t), NULL, NULL, NULL}

typedef void (*turbo_flow_security_auth_provider_owner_destroy_fn)(void *owner);

/** Provider-owned lifecycle envelope around the borrowed authentication ABI. */
typedef struct turbo_flow_security_auth_provider_owner_s {
  size_t size;
  uint32_t abi_version;
  const char *backend;
  const char *method;
  const turbo_flow_security_auth_provider_t *provider;
  const turbo_flow_security_enhanced_auth_provider_t *enhanced_provider;
  void *owner;
  turbo_flow_security_auth_provider_owner_destroy_fn destroy;
} turbo_flow_security_auth_provider_owner_t;

#define TURBO_FLOW_SECURITY_AUTH_PROVIDER_OWNER_INIT                                               \
  {sizeof(turbo_flow_security_auth_provider_owner_t),                                              \
   TURBO_FLOW_SECURITY_ABI_V3,                                                                     \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

typedef int (*turbo_flow_security_auth_provider_create_resolved_fn)(
    void *ctx, const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider,
    turbo_flow_security_auth_provider_owner_t *owner_out, turbo_flow_config_error_t *error);

/** One statically or dynamically registered provider backend factory. */
typedef struct turbo_flow_security_auth_provider_factory_s {
  size_t size;
  uint32_t abi_version;
  const char *backend;
  turbo_flow_security_auth_provider_create_resolved_fn create_resolved;
  void *ctx;
} turbo_flow_security_auth_provider_factory_t;

#define TURBO_FLOW_SECURITY_AUTH_PROVIDER_FACTORY_INIT                                             \
  {sizeof(turbo_flow_security_auth_provider_factory_t), TURBO_FLOW_SECURITY_ABI_V3, NULL, NULL,    \
   NULL}

CXX_C_API int turbo_flow_security_secret_acquire(const turbo_flow_security_key_provider_t *provider,
                                                 const char *reference,
                                                 turbo_flow_security_secret_lease_t *lease_out);
CXX_C_API void
turbo_flow_security_secret_release(const turbo_flow_security_key_provider_t *provider,
                                   turbo_flow_security_secret_lease_t *lease);

/** Invoke one exact backend factory and validate its lifecycle envelope. */
CXX_C_API int turbo_flow_security_auth_provider_owner_create_resolved(
    const turbo_flow_security_auth_provider_factory_t *factory,
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider,
    turbo_flow_security_auth_provider_owner_t *owner_out, turbo_flow_config_error_t *error);

/** Select exactly one registered backend from an `auth_provider` channel. */
CXX_C_API int turbo_flow_security_auth_provider_owner_create_registered(
    const turbo_flow_security_auth_provider_factory_t *const *factories, size_t factory_count,
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider,
    turbo_flow_security_auth_provider_owner_t *owner_out, turbo_flow_config_error_t *error);

CXX_C_API void
turbo_flow_security_auth_provider_owner_destroy(turbo_flow_security_auth_provider_owner_t *owner);

typedef void (*turbo_flow_security_policy_provider_owner_destroy_fn)(void *owner);

/** Provider-owned lifecycle envelope around the borrowed policy-source ABI. */
struct turbo_flow_security_policy_provider_owner_s {
  size_t size;
  uint32_t abi_version;
  const char *backend;
  const turbo_flow_security_policy_provider_t *provider;
  void *owner;
  turbo_flow_security_policy_provider_owner_destroy_fn destroy;
};

#define TURBO_FLOW_SECURITY_POLICY_PROVIDER_OWNER_INIT                                             \
  {sizeof(turbo_flow_security_policy_provider_owner_t),                                            \
   TURBO_FLOW_SECURITY_ABI_V3,                                                                     \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

typedef int (*turbo_flow_security_policy_provider_create_resolved_fn)(
    void *ctx, const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider,
    turbo_flow_security_policy_provider_owner_t *owner_out, turbo_flow_config_error_t *error);

typedef struct turbo_flow_security_policy_provider_factory_s {
  size_t size;
  uint32_t abi_version;
  const char *backend;
  turbo_flow_security_policy_provider_create_resolved_fn create_resolved;
  void *ctx;
} turbo_flow_security_policy_provider_factory_t;

#define TURBO_FLOW_SECURITY_POLICY_PROVIDER_FACTORY_INIT                                           \
  {sizeof(turbo_flow_security_policy_provider_factory_t), TURBO_FLOW_SECURITY_ABI_V3, NULL, NULL,  \
   NULL}

CXX_C_API int turbo_flow_security_policy_provider_owner_create_resolved(
    const turbo_flow_security_policy_provider_factory_t *factory,
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider,
    turbo_flow_security_policy_provider_owner_t *owner_out, turbo_flow_config_error_t *error);

/** Select exactly one registered backend from an `acl_provider` channel. */
CXX_C_API int turbo_flow_security_policy_provider_owner_create_registered(
    const turbo_flow_security_policy_provider_factory_t *const *factories, size_t factory_count,
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider,
    turbo_flow_security_policy_provider_owner_t *owner_out, turbo_flow_config_error_t *error);

CXX_C_API void turbo_flow_security_policy_provider_owner_destroy(
    turbo_flow_security_policy_provider_owner_t *owner);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_SECURITY_H */
