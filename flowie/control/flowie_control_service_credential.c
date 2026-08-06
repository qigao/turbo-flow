#include "flowie_control_service_credential_internal.h"

#include "monocypher.h"
#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

struct flowie_control_service_credential_resolver_s {
  flowie_control_repository_t repository;
  char listener_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
};

static int flowie_control_service_credential_text_valid(const char *value, size_t maximum) {
  size_t size;
  if (!value || maximum == 0u) return 0;
  size = strnlen(value, maximum + 1u);
  if (size == 0u || size > maximum) return 0;
  for (size_t index = 0u; index < size; ++index) {
    unsigned char byte = (unsigned char)value[index];
    if (byte < 0x20u || byte == 0x7fu) return 0;
  }
  return 1;
}

static uint32_t flowie_control_service_permissions(
    const flowie_control_principal_snapshot_t *snapshot) {
  uint32_t permissions = 0u;
  if (!snapshot || strcmp(snapshot->principal_type, "service") != 0) return 0u;
  for (uint32_t index = 0u; index < snapshot->effective_roles.role_count; ++index) {
    const char *role = snapshot->effective_roles.roles[index];
    if (strcmp(role, FLOWIE_CONTROL_SERVICE_ROLE_AUTH_CLIENT) == 0)
      permissions |= FLOWIE_CONTROL_SERVICE_AUTHENTICATE;
    else if (strcmp(role, FLOWIE_CONTROL_SERVICE_ROLE_ACL_CLIENT) == 0)
      permissions |= FLOWIE_CONTROL_SERVICE_ACL_CHECK;
  }
  return permissions;
}

int flowie_control_service_credential_resolver_create(
    const flowie_control_service_credential_config_t *config,
    flowie_control_service_credential_resolver_t **out) {
  flowie_control_service_credential_resolver_t *resolver;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) ||
      !flowie_control_service_credential_text_valid(config->listener_id,
                                                    TURBO_FLOW_SECURITY_ID_MAX) ||
      flowie_control_repository_validate(config->repository) != TURBO_OK || !out)
    return TURBO_EINVAL;
  resolver = (flowie_control_service_credential_resolver_t *)calloc(1u, sizeof(*resolver));
  if (!resolver) return TURBO_ENOMEM;
  resolver->repository = *config->repository;
  memcpy(resolver->listener_id, config->listener_id, strlen(config->listener_id) + 1u);
  *out = resolver;
  return TURBO_OK;
}

void flowie_control_service_credential_resolver_destroy(
    flowie_control_service_credential_resolver_t *resolver) {
  if (!resolver) return;
  crypto_wipe(resolver, sizeof(*resolver));
  free(resolver);
}

int flowie_control_service_credential_resolve(
    flowie_control_service_credential_resolver_t *resolver, const char *service_domain,
    const char *service_id, const uint8_t *token, size_t token_size,
    uint32_t required_permission, flowie_control_verified_caller_t *caller_out) {
  flowie_control_credential_verify_result_t verified = FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  flowie_control_principal_snapshot_t snapshot = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;
  uint32_t permissions;
  int rc;
  if (caller_out && caller_out->size >= sizeof(*caller_out)) *caller_out = caller;
  if (!resolver ||
      !flowie_control_service_credential_text_valid(service_domain,
                                                    TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_service_credential_text_valid(service_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !token || token_size == 0u || token_size > FLOWIE_CONTROL_SERVICE_CREDENTIAL_TOKEN_MAX ||
      memchr(token, '\0', token_size) || memchr(token, '\r', token_size) ||
      memchr(token, '\n', token_size) || required_permission == 0u || !caller_out ||
      caller_out->size < sizeof(*caller_out))
    return TURBO_EINVAL;
  rc = resolver->repository.auth->credential_verify(
      resolver->repository.ctx, service_domain, service_id, token, token_size, &verified);
  if (rc != TURBO_OK) return rc;
  rc = resolver->repository.auth->principal_snapshot(
      resolver->repository.ctx, service_domain, service_id, &verified, &snapshot);
  if (rc != TURBO_OK) return rc;
  permissions = flowie_control_service_permissions(&snapshot);
  if ((permissions & required_permission) != required_permission) return TURBO_EPERM;
  memcpy(caller_out->resolved_listener_id, resolver->listener_id,
         strlen(resolver->listener_id) + 1u);
  memcpy(caller_out->resolved_service_id, service_id, strlen(service_id) + 1u);
  memcpy(caller_out->resolved_domain_id, service_domain, strlen(service_domain) + 1u);
  caller_out->listener_id = caller_out->resolved_listener_id;
  caller_out->service_id = caller_out->resolved_service_id;
  caller_out->domain_id = caller_out->resolved_domain_id;
  caller_out->permissions = permissions;
  caller_out->authenticated = 1;
  return TURBO_OK;
}
