#include "flowie_control_auth_service_internal.h"

#include "turbo_error.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct flowie_control_auth_binding_record_s {
  char listener_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char peer_certificate_sha256[FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE + 1u];
  char root_group_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
} flowie_control_auth_binding_record_t;

struct flowie_control_auth_service_s {
  flowie_control_store_t *store;
  flowie_control_auth_cache_t *credential_cache;
  flowie_control_principal_cache_t *principal_cache;
  flowie_control_auth_rate_limiter_t *rate_limiter;
  flowie_control_auth_policy_version_provider_t policy_version;
  flowie_control_auth_clock_fn clock_seconds;
  void *clock_ctx;
  uint64_t principal_ttl_seconds;
  size_t binding_count;
  char method[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  flowie_control_auth_binding_record_t bindings[];
};

static int flowie_control_auth_text_valid(const char *value, size_t limit) {
  size_t length;
  if (!value || limit == 0u) return 0;
  length = strnlen(value, limit + 1u);
  if (length == 0u || length > limit) return 0;
  for (size_t index = 0u; index < length; ++index) {
    unsigned char byte = (unsigned char)value[index];
    if (byte < 0x20u || byte == 0x7fu) return 0;
  }
  return 1;
}

static int flowie_control_auth_fingerprint_valid(const char *fingerprint) {
  static const char prefix[] = "sha256:";
  if (!fingerprint ||
      strnlen(fingerprint, FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE + 1u) !=
          FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE ||
      memcmp(fingerprint, prefix, sizeof(prefix) - 1u) != 0)
    return 0;
  for (size_t index = sizeof(prefix) - 1u; index < FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE;
       ++index) {
    char byte = fingerprint[index];
    if (!((byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f'))) return 0;
  }
  return 1;
}

static int flowie_control_auth_binding_compare(const void *left, const void *right) {
  const flowie_control_auth_binding_record_t *a =
      (const flowie_control_auth_binding_record_t *)left;
  const flowie_control_auth_binding_record_t *b =
      (const flowie_control_auth_binding_record_t *)right;
  int result = strcmp(a->listener_id, b->listener_id);
  return result != 0 ? result : strcmp(a->peer_certificate_sha256, b->peer_certificate_sha256);
}

static uint64_t flowie_control_auth_default_clock(void *ctx) {
  time_t now;
  (void)ctx;
  now = time(NULL);
  return now > 0 ? (uint64_t)now : 0u;
}

static const char *
flowie_control_auth_resolve_root(const flowie_control_auth_service_t *service,
                                 const flowie_control_verified_caller_t *caller) {
  flowie_control_auth_binding_record_t key = {0};
  const flowie_control_auth_binding_record_t *found;
  size_t listener_size = strlen(caller->listener_id);
  memcpy(key.listener_id, caller->listener_id, listener_size + 1u);
  memcpy(key.peer_certificate_sha256, caller->peer_certificate_sha256,
         FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE + 1u);
  found = (const flowie_control_auth_binding_record_t *)bsearch(
      &key, service->bindings, service->binding_count, sizeof(service->bindings[0]),
      flowie_control_auth_binding_compare);
  return found ? found->root_group_id : NULL;
}

int flowie_control_auth_service_create(const flowie_control_auth_service_config_t *config,
                                       flowie_control_auth_service_t **out) {
  flowie_control_auth_service_t *service;
  size_t allocation_size;
  size_t method_size;
  int rc;

  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !out || !config->store || !config->bindings ||
      config->binding_count == 0u || config->binding_count > FLOWIE_CONTROL_AUTH_MAX_BINDINGS ||
      !flowie_control_auth_text_valid(config->method, TURBO_FLOW_SECURITY_TYPE_MAX) ||
      config->principal_ttl_seconds == 0u ||
      config->principal_ttl_seconds > FLOWIE_CONTROL_AUTH_MAX_PRINCIPAL_TTL_SECONDS ||
      config->credential_cache.size < sizeof(config->credential_cache) ||
      config->principal_cache.size < sizeof(config->principal_cache) ||
      config->rate_limiter.size < sizeof(config->rate_limiter) ||
      config->policy_version.size < sizeof(config->policy_version) ||
      !config->policy_version.current ||
      config->binding_count > (SIZE_MAX - sizeof(*service)) / sizeof(service->bindings[0]))
    return TURBO_EINVAL;

  allocation_size = sizeof(*service) + config->binding_count * sizeof(service->bindings[0]);
  service = (flowie_control_auth_service_t *)calloc(1u, allocation_size);
  if (!service) return TURBO_ENOMEM;
  service->store = config->store;
  service->policy_version = config->policy_version;
  service->clock_seconds =
      config->clock_seconds ? config->clock_seconds : flowie_control_auth_default_clock;
  service->clock_ctx = config->clock_ctx;
  service->principal_ttl_seconds = config->principal_ttl_seconds;
  service->binding_count = config->binding_count;
  method_size = strlen(config->method);
  memcpy(service->method, config->method, method_size + 1u);

  for (size_t index = 0u; index < config->binding_count; ++index) {
    const flowie_control_auth_root_binding_t *source = &config->bindings[index];
    flowie_control_auth_binding_record_t *destination = &service->bindings[index];
    size_t listener_size;
    size_t root_size;
    if (source->size < sizeof(*source) ||
        !flowie_control_auth_text_valid(source->listener_id, TURBO_FLOW_SECURITY_ID_MAX) ||
        !flowie_control_auth_fingerprint_valid(source->peer_certificate_sha256) ||
        !flowie_control_auth_text_valid(source->root_group_id, TURBO_FLOW_SECURITY_ID_MAX)) {
      rc = TURBO_EINVAL;
      goto fail;
    }
    listener_size = strlen(source->listener_id);
    root_size = strlen(source->root_group_id);
    memcpy(destination->listener_id, source->listener_id, listener_size + 1u);
    memcpy(destination->peer_certificate_sha256, source->peer_certificate_sha256,
           FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE + 1u);
    memcpy(destination->root_group_id, source->root_group_id, root_size + 1u);
  }
  qsort(service->bindings, service->binding_count, sizeof(service->bindings[0]),
        flowie_control_auth_binding_compare);
  for (size_t index = 1u; index < service->binding_count; ++index) {
    if (flowie_control_auth_binding_compare(&service->bindings[index - 1u],
                                            &service->bindings[index]) == 0) {
      rc = TURBO_EINVAL;
      goto fail;
    }
  }
  rc = flowie_control_auth_cache_create(&config->credential_cache, &service->credential_cache);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_control_principal_cache_create(&config->principal_cache, &service->principal_cache);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_control_auth_rate_limiter_create(&config->rate_limiter, &service->rate_limiter);
  if (rc != TURBO_OK) goto fail;
  *out = service;
  return TURBO_OK;

fail:
  flowie_control_auth_service_destroy(service);
  return rc;
}

void flowie_control_auth_service_destroy(flowie_control_auth_service_t *service) {
  if (!service) return;
  flowie_control_auth_cache_destroy(service->credential_cache);
  flowie_control_principal_cache_destroy(service->principal_cache);
  flowie_control_auth_rate_limiter_destroy(service->rate_limiter);
  memset(service, 0, sizeof(*service) + service->binding_count * sizeof(service->bindings[0]));
  free(service);
}

int flowie_control_auth_service_authenticate(flowie_control_auth_service_t *service,
                                             const flowie_control_authenticate_request_t *request,
                                             turbo_flow_security_principal_t *principal_out,
                                             int *credential_cache_hit_out) {
  flowie_control_credential_verify_result_t verified = FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  flowie_control_principal_snapshot_t snapshot = FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  const char *root_group_id;
  uint64_t policy_version = 0u;
  uint64_t store_revision = 0u;
  uint64_t now;
  int cache_hit = 0;
  int principal_cache_hit = 0;
  int rc;

  if (credential_cache_hit_out) *credential_cache_hit_out = 0;
  if (principal_out && principal_out->size >= sizeof(*principal_out)) *principal_out = principal;
  if (!service || !request || request->size < sizeof(*request) || !request->caller ||
      request->caller->size < sizeof(*request->caller) ||
      !flowie_control_auth_text_valid(request->identity, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_auth_text_valid(request->method, TURBO_FLOW_SECURITY_TYPE_MAX) ||
      !request->secret || request->secret_size == 0u ||
      request->secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX || !principal_out ||
      principal_out->size < sizeof(*principal_out))
    return TURBO_EINVAL;
  if (request->caller->certificate_verified != 1 ||
      !flowie_control_auth_text_valid(request->caller->listener_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_auth_fingerprint_valid(request->caller->peer_certificate_sha256) ||
      strcmp(request->method, service->method) != 0)
    return TURBO_EPERM;

  root_group_id = flowie_control_auth_resolve_root(service, request->caller);
  if (!root_group_id) return TURBO_EPERM;
  rc = flowie_control_auth_rate_limiter_acquire(
      service->rate_limiter, request->caller->peer_certificate_sha256, root_group_id,
      request->identity);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_auth_cache_verify(service->credential_cache, service->store, root_group_id,
                                        request->identity, request->secret, request->secret_size,
                                        &verified, &cache_hit);
  if (rc != TURBO_OK) goto done;
  flowie_control_auth_rate_limiter_record_success(
      service->rate_limiter, request->caller->peer_certificate_sha256, root_group_id,
      request->identity);
  rc = service->policy_version.current(service->policy_version.ctx, root_group_id, &policy_version);
  if (rc != TURBO_OK) goto done;
  if (policy_version == 0u) {
    rc = TURBO_EPROTO;
    goto done;
  }
  rc = flowie_control_store_current_revision(service->store, &store_revision);
  if (rc != TURBO_OK || store_revision == 0u) {
    if (rc == TURBO_OK) rc = TURBO_EPROTO;
    goto done;
  }
  rc = flowie_control_principal_cache_get(
      service->principal_cache, root_group_id, request->identity, verified.user_revision,
      verified.credential_revision, store_revision, policy_version, &snapshot,
      &principal_cache_hit);
  if (rc == TURBO_ENOENT) {
    rc = flowie_control_store_principal_snapshot(service->store, root_group_id, request->identity,
                                                 &verified, &snapshot);
    if (rc != TURBO_OK) goto done;
    rc = flowie_control_principal_cache_put(service->principal_cache, &snapshot, store_revision,
                                            policy_version);
    if (rc != TURBO_OK) goto done;
  } else if (rc != TURBO_OK) {
    goto done;
  }
  now = service->clock_seconds(service->clock_ctx);
  if (now == 0u || now > UINT64_MAX - service->principal_ttl_seconds) {
    rc = TURBO_EIO;
    goto done;
  }

  memcpy(principal.principal_id, snapshot.principal_id, strlen(snapshot.principal_id) + 1u);
  memcpy(principal.principal_type, snapshot.principal_type, strlen(snapshot.principal_type) + 1u);
  memcpy(principal.root_group_id, snapshot.root_group_id, strlen(snapshot.root_group_id) + 1u);
  memcpy(principal.auth_method, service->method, strlen(service->method) + 1u);
  principal.scope = TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP;
  principal.role_count = snapshot.effective_roles.role_count;
  principal.group_count = snapshot.effective_groups.group_count;
  memcpy(principal.roles, snapshot.effective_roles.roles, sizeof(principal.roles));
  memcpy(principal.groups, snapshot.effective_groups.groups, sizeof(principal.groups));
  principal.expires_at = now + service->principal_ttl_seconds;
  principal.policy_version = policy_version;
  *principal_out = principal;
  if (credential_cache_hit_out) *credential_cache_hit_out = cache_hit;
  rc = TURBO_OK;

done:
  if (rc != TURBO_OK)
    *principal_out = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  return rc;
}
