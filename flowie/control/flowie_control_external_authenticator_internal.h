#ifndef FLOWIE_CONTROL_EXTERNAL_AUTHENTICATOR_INTERNAL_H
#define FLOWIE_CONTROL_EXTERNAL_AUTHENTICATOR_INTERNAL_H

#include "flowie_control_security_limits_internal.h"
#include "flowie_control_store_internal.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_EXTERNAL_AUTHENTICATOR_VERSION 1u
#define FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAPPER_VERSION 1u
#define FLOWIE_CONTROL_EXTERNAL_ISSUER_MAX TURBO_FLOW_SECURITY_ID_MAX

typedef enum flowie_control_external_authenticator_capability_e {
  FLOWIE_CONTROL_EXTERNAL_AUTH_STABLE_SUBJECT = 1u << 0,
  FLOWIE_CONTROL_EXTERNAL_AUTH_ACCOUNT_STATE = 1u << 1,
  FLOWIE_CONTROL_EXTERNAL_AUTH_EXPIRING_ASSERTION = 1u << 2,
  FLOWIE_CONTROL_EXTERNAL_AUTH_GROUP_CLAIMS = 1u << 3
} flowie_control_external_authenticator_capability_t;

#define FLOWIE_CONTROL_EXTERNAL_AUTH_REQUIRED_CAPABILITIES                                         \
  (FLOWIE_CONTROL_EXTERNAL_AUTH_STABLE_SUBJECT | FLOWIE_CONTROL_EXTERNAL_AUTH_ACCOUNT_STATE |      \
   FLOWIE_CONTROL_EXTERNAL_AUTH_EXPIRING_ASSERTION)

typedef enum flowie_control_external_assurance_level_e {
  FLOWIE_CONTROL_EXTERNAL_ASSURANCE_SINGLE_FACTOR = 1u,
  FLOWIE_CONTROL_EXTERNAL_ASSURANCE_MULTI_FACTOR = 2u,
  FLOWIE_CONTROL_EXTERNAL_ASSURANCE_HARDWARE_BOUND = 3u
} flowie_control_external_assurance_level_t;

/**
 * Request-local third-party authentication input.
 *
 * All pointers are borrowed only for the duration of verify(). The secret is the presented
 * password/token, never a stored verifier. A network-backed callback may yield in the current
 * coroutine; a blocking SDK must be isolated behind a bounded executor by its adapter.
 */
typedef struct flowie_control_external_auth_request_s {
  size_t size;
  const char *root_group_id;
  const char *presented_identity;
  const char *method;
  const uint8_t *secret;
  size_t secret_size;
  const char *protocol;
  const char *remote_address;
  const char *peer_certificate_sha256;
} flowie_control_external_auth_request_t;

#define FLOWIE_CONTROL_EXTERNAL_AUTH_REQUEST_INIT                                                  \
  {sizeof(flowie_control_external_auth_request_t), NULL, NULL, NULL, NULL, 0u, NULL, NULL, NULL}

/**
 * Allowlisted authentication facts returned by a trusted provider adapter.
 *
 * Provider-specific raw claims remain inside the adapter. External groups are mapping input only;
 * they are never copied directly into the local authorization principal.
 */
typedef struct flowie_control_external_auth_assertion_s {
  size_t size;
  char issuer[FLOWIE_CONTROL_EXTERNAL_ISSUER_MAX + 1u];
  char subject[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char subject_type[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  char auth_method[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  uint64_t issued_at;
  uint64_t expires_at;
  uint64_t revision;
  uint32_t assurance_level;
  int account_enabled;
  uint32_t external_group_count;
  char external_groups[TURBO_FLOW_SECURITY_MAX_GROUPS][TURBO_FLOW_SECURITY_ID_MAX + 1u];
} flowie_control_external_auth_assertion_t;

#define FLOWIE_CONTROL_EXTERNAL_AUTH_ASSERTION_INIT                                                \
  {sizeof(flowie_control_external_auth_assertion_t), "", "", "", "", 0u, 0u, 0u, 0u, 0, 0u}

typedef int (*flowie_control_external_auth_verify_fn)(
    void *ctx, const flowie_control_external_auth_request_t *request,
    flowie_control_external_auth_assertion_t *assertion_out);

typedef struct flowie_control_external_authenticator_s {
  size_t size;
  uint32_t version;
  uint32_t capabilities;
  void *ctx;
  const char *method;
  flowie_control_external_auth_verify_fn verify;
} flowie_control_external_authenticator_t;

#define FLOWIE_CONTROL_EXTERNAL_AUTHENTICATOR_INIT                                                 \
  {sizeof(flowie_control_external_authenticator_t),                                                \
   FLOWIE_CONTROL_EXTERNAL_AUTHENTICATOR_VERSION,                                                  \
   0u,                                                                                             \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL}

typedef struct flowie_control_external_identity_map_request_s {
  size_t size;
  const char *root_group_id;
  const char *presented_identity;
  const flowie_control_external_auth_assertion_t *assertion;
} flowie_control_external_identity_map_request_t;

#define FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAP_REQUEST_INIT                                          \
  {sizeof(flowie_control_external_identity_map_request_t), NULL, NULL, NULL}

typedef struct flowie_control_external_identity_map_result_s {
  size_t size;
  char principal_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
} flowie_control_external_identity_map_result_t;

#define FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAP_RESULT_INIT                                           \
  {sizeof(flowie_control_external_identity_map_result_t), ""}

typedef int (*flowie_control_external_identity_map_fn)(
    void *ctx, const flowie_control_external_identity_map_request_t *request,
    flowie_control_external_identity_map_result_t *result_out);

typedef struct flowie_control_external_identity_mapper_s {
  size_t size;
  uint32_t version;
  void *ctx;
  flowie_control_external_identity_map_fn map;
} flowie_control_external_identity_mapper_t;

#define FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAPPER_INIT                                               \
  {sizeof(flowie_control_external_identity_mapper_t),                                              \
   FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAPPER_VERSION, NULL, NULL}

typedef struct flowie_control_external_subject_mapper_s flowie_control_external_subject_mapper_t;

typedef struct flowie_control_external_subject_mapper_config_s {
  size_t size;
  const char *trusted_issuer;
  const char *subject_type;
} flowie_control_external_subject_mapper_config_t;

#define FLOWIE_CONTROL_EXTERNAL_SUBJECT_MAPPER_CONFIG_INIT                                         \
  {sizeof(flowie_control_external_subject_mapper_config_t), NULL, NULL}

int flowie_control_external_authenticator_validate(
    const flowie_control_external_authenticator_t *authenticator);
int flowie_control_external_identity_mapper_validate(
    const flowie_control_external_identity_mapper_t *mapper);
int flowie_control_external_auth_assertion_validate(
    const flowie_control_external_auth_assertion_t *assertion, const char *expected_method,
    uint64_t now);
int flowie_control_external_identity_map_result_validate(
    const flowie_control_external_identity_map_result_t *result);

/**
 * Map a trusted assertion subject directly to one existing local principal id.
 *
 * The mapper first requires exact issuer and subject-type matches. Authorization remains local:
 * the repository subsequently checks the principal in the caller-bound Root Group and loads only
 * local roles/groups.
 */
int flowie_control_external_subject_mapper_create(
    const flowie_control_external_subject_mapper_config_t *config,
    flowie_control_external_subject_mapper_t **out);
void flowie_control_external_subject_mapper_destroy(
    flowie_control_external_subject_mapper_t *mapper);
const flowie_control_external_identity_mapper_t *flowie_control_external_subject_mapper_interface(
    const flowie_control_external_subject_mapper_t *mapper);

#ifdef __cplusplus
}
#endif

#endif
