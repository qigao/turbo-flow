#ifndef TURBO_FLOW_SECURITY_H
#define TURBO_FLOW_SECURITY_H

#include "turbo_flow_config.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_SECURITY_ABI_V1 1u
#define TURBO_FLOW_SECURITY_ID_MAX 255u
#define TURBO_FLOW_SECURITY_TYPE_MAX 63u
#define TURBO_FLOW_SECURITY_PATTERN_MAX 511u
#define TURBO_FLOW_SECURITY_MAX_ROLES 8u
#define TURBO_FLOW_SECURITY_MAX_GROUPS 16u
#define TURBO_FLOW_SECURITY_MAX_RULES 4096u
#define TURBO_FLOW_SECURITY_SECRET_REF_MAX 255u

typedef struct turbo_flow_security_realm_s turbo_flow_security_realm_t;

typedef enum turbo_flow_security_scope_e {
  TURBO_FLOW_SECURITY_SCOPE_SELF = 1,
  TURBO_FLOW_SECURITY_SCOPE_GROUP,
  TURBO_FLOW_SECURITY_SCOPE_TENANT,
  TURBO_FLOW_SECURITY_SCOPE_SYSTEM
} turbo_flow_security_scope_t;

/** Authentication output copied by value and safe to pass into authorization. */
typedef struct turbo_flow_security_principal_s {
  size_t size;
  uint32_t abi_version;
  char principal_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char principal_type[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  char tenant_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char auth_method[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  turbo_flow_security_scope_t scope;
  uint32_t role_count;
  char roles[TURBO_FLOW_SECURITY_MAX_ROLES][TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  uint32_t group_count;
  char groups[TURBO_FLOW_SECURITY_MAX_GROUPS][TURBO_FLOW_SECURITY_ID_MAX + 1u];
  /** Unix epoch seconds; zero means the provider did not assign an expiry. */
  uint64_t expires_at;
  /** Required realm generation; authorization rejects zero and stale generations. */
  uint64_t policy_version;
} turbo_flow_security_principal_t;

#define TURBO_FLOW_SECURITY_PRINCIPAL_INIT                                                         \
  {sizeof(turbo_flow_security_principal_t), TURBO_FLOW_SECURITY_ABI_V1}

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
  /** Empty means any tenant, but cross-tenant access still requires SYSTEM scope. */
  char tenant_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  uint32_t action_mask;
  turbo_flow_security_resource_type_t resource_type;
  turbo_flow_security_match_kind_t match_kind;
  char pattern[TURBO_FLOW_SECURITY_PATTERN_MAX + 1u];
} turbo_flow_security_rule_t;

#define TURBO_FLOW_SECURITY_RULE_INIT                                                              \
  {sizeof(turbo_flow_security_rule_t), TURBO_FLOW_SECURITY_ABI_V1, TURBO_FLOW_SECURITY_DENY}

typedef struct turbo_flow_security_request_s {
  size_t size;
  const turbo_flow_security_principal_t *principal;
  const char *tenant_id;
  uint32_t action;
  turbo_flow_security_resource_type_t resource_type;
  const char *resource;
  /** Protocol-owner facts available to an injected matcher; Core never dereferences it. */
  const void *protocol_context;
} turbo_flow_security_request_t;

#define TURBO_FLOW_SECURITY_REQUEST_INIT {sizeof(turbo_flow_security_request_t)}

typedef int (*turbo_flow_security_match_fn)(void *ctx, const turbo_flow_security_rule_t *rule,
                                            const turbo_flow_security_request_t *request,
                                            int *matched_out);

typedef struct turbo_flow_security_matcher_s {
  size_t size;
  void *ctx;
  turbo_flow_security_match_fn match;
} turbo_flow_security_matcher_t;

#define TURBO_FLOW_SECURITY_MATCHER_INIT {sizeof(turbo_flow_security_matcher_t), NULL, NULL}

typedef enum turbo_flow_security_decision_reason_e {
  TURBO_FLOW_SECURITY_REASON_DEFAULT_DENY = 1,
  TURBO_FLOW_SECURITY_REASON_ALLOW_RULE,
  TURBO_FLOW_SECURITY_REASON_DENY_RULE,
  TURBO_FLOW_SECURITY_REASON_TENANT_MISMATCH,
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
} turbo_flow_security_realm_config_t;

#define TURBO_FLOW_SECURITY_REALM_CONFIG_INIT                                                      \
  {sizeof(turbo_flow_security_realm_config_t),                                                     \
   TURBO_FLOW_SECURITY_ABI_V1,                                                                     \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   NULL,                                                                                           \
   0u,                                                                                             \
   TURBO_FLOW_SECURITY_MATCHER_INIT}

/** Validate provider output and authenticate without retaining credential bytes. */
CXX_C_API int turbo_flow_security_authenticate(const turbo_flow_security_auth_provider_t *provider,
                                               const turbo_flow_security_auth_request_t *request,
                                               turbo_flow_security_principal_t *principal_out);

CXX_C_API int turbo_flow_security_realm_create(const turbo_flow_security_realm_config_t *config,
                                               turbo_flow_security_realm_t **out);
CXX_C_API void turbo_flow_security_realm_destroy(turbo_flow_security_realm_t *realm);

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

/** Create a realm from strict `channels.<name>.kind: security_realm` YAML projection. */
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

CXX_C_API int turbo_flow_security_secret_acquire(const turbo_flow_security_key_provider_t *provider,
                                                 const char *reference,
                                                 turbo_flow_security_secret_lease_t *lease_out);
CXX_C_API void
turbo_flow_security_secret_release(const turbo_flow_security_key_provider_t *provider,
                                   turbo_flow_security_secret_lease_t *lease);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_SECURITY_H */
