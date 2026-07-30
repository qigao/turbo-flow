#include "flowie_control_service_credential_internal.h"

#include "monocypher.h"
#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

enum { FLOWIE_CONTROL_SERVICE_CREDENTIAL_DIGEST_SIZE = 32 };

typedef struct flowie_control_service_credential_record_s {
  char service_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char token_ref[FLOWIE_CONTROL_SERVICE_CREDENTIAL_REFERENCE_MAX + 1u];
  char root_group_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char peer_certificate_sha256[FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE + 1u];
} flowie_control_service_credential_record_t;

struct flowie_control_service_credential_resolver_s {
  turbo_flow_security_key_provider_t key_provider;
  char listener_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  size_t binding_count;
  flowie_control_service_credential_record_t bindings[];
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

static int flowie_control_service_credential_fingerprint_valid(const char *value) {
  static const char prefix[] = "sha256:";
  if (!value || strlen(value) != FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE ||
      memcmp(value, prefix, sizeof(prefix) - 1u) != 0)
    return 0;
  for (size_t index = sizeof(prefix) - 1u; index < FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE;
       ++index) {
    char byte = value[index];
    if (!((byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f'))) return 0;
  }
  return 1;
}

static int flowie_control_service_credential_secret_valid(
    const turbo_flow_security_secret_lease_t *lease) {
  return lease && lease->bytes && lease->byte_count > 0u &&
         lease->byte_count <= FLOWIE_CONTROL_SERVICE_CREDENTIAL_TOKEN_MAX &&
         !memchr(lease->bytes, '\0', lease->byte_count) &&
         !memchr(lease->bytes, '\r', lease->byte_count) &&
         !memchr(lease->bytes, '\n', lease->byte_count);
}

int flowie_control_service_credential_resolver_create(
    const flowie_control_service_credential_config_t *config,
    flowie_control_service_credential_resolver_t **out) {
  flowie_control_service_credential_resolver_t *resolver;
  uint8_t *digests = NULL;
  size_t allocation_size;
  int rc = TURBO_OK;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !out ||
      !flowie_control_service_credential_text_valid(config->listener_id,
                                                    TURBO_FLOW_SECURITY_ID_MAX) ||
      !config->bindings || config->binding_count == 0u ||
      config->binding_count > FLOWIE_CONTROL_AUTH_MAX_SERVICE_BINDINGS ||
      config->key_provider.size < sizeof(config->key_provider) ||
      !config->key_provider.acquire || !config->key_provider.release ||
      config->binding_count >
          (SIZE_MAX - sizeof(*resolver)) / sizeof(flowie_control_service_credential_record_t))
    return TURBO_EINVAL;
  allocation_size =
      sizeof(*resolver) + config->binding_count * sizeof(flowie_control_service_credential_record_t);
  resolver = (flowie_control_service_credential_resolver_t *)calloc(1u, allocation_size);
  if (!resolver) return TURBO_ENOMEM;
  digests = (uint8_t *)calloc(config->binding_count,
                             FLOWIE_CONTROL_SERVICE_CREDENTIAL_DIGEST_SIZE);
  if (!digests) {
    free(resolver);
    return TURBO_ENOMEM;
  }
  resolver->key_provider = config->key_provider;
  resolver->binding_count = config->binding_count;
  memcpy(resolver->listener_id, config->listener_id, strlen(config->listener_id) + 1u);
  for (size_t index = 0u; index < config->binding_count; ++index) {
    const flowie_control_service_credential_binding_t *source = &config->bindings[index];
    flowie_control_service_credential_record_t *destination = &resolver->bindings[index];
    turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
    if (source->size < sizeof(*source) ||
        !flowie_control_service_credential_text_valid(source->service_id,
                                                      TURBO_FLOW_SECURITY_ID_MAX) ||
        !flowie_control_service_credential_text_valid(source->token_ref,
                                                      FLOWIE_CONTROL_SERVICE_CREDENTIAL_REFERENCE_MAX) ||
        !flowie_control_service_credential_text_valid(source->root_group_id,
                                                      TURBO_FLOW_SECURITY_ID_MAX) ||
        (source->peer_certificate_sha256 &&
         !flowie_control_service_credential_fingerprint_valid(
             source->peer_certificate_sha256))) {
      rc = TURBO_EINVAL;
      break;
    }
    for (size_t prior = 0u; prior < index; ++prior) {
      if (strcmp(source->service_id, resolver->bindings[prior].service_id) == 0 ||
          strcmp(source->token_ref, resolver->bindings[prior].token_ref) == 0) {
        rc = TURBO_EALREADY;
        break;
      }
    }
    if (rc != TURBO_OK) break;
    rc = turbo_flow_security_secret_acquire(&resolver->key_provider, source->token_ref, &lease);
    if (rc == TURBO_OK && !flowie_control_service_credential_secret_valid(&lease))
      rc = TURBO_EINVAL;
    if (rc == TURBO_OK)
      crypto_blake2b(digests + index * FLOWIE_CONTROL_SERVICE_CREDENTIAL_DIGEST_SIZE,
                     FLOWIE_CONTROL_SERVICE_CREDENTIAL_DIGEST_SIZE, lease.bytes,
                     lease.byte_count);
    turbo_flow_security_secret_release(&resolver->key_provider, &lease);
    if (rc != TURBO_OK) break;
    for (size_t prior = 0u; prior < index; ++prior) {
      if (crypto_verify32(
              digests + prior * FLOWIE_CONTROL_SERVICE_CREDENTIAL_DIGEST_SIZE,
              digests + index * FLOWIE_CONTROL_SERVICE_CREDENTIAL_DIGEST_SIZE) == 0) {
        rc = TURBO_EALREADY;
        break;
      }
    }
    if (rc != TURBO_OK) break;
    memcpy(destination->service_id, source->service_id, strlen(source->service_id) + 1u);
    memcpy(destination->token_ref, source->token_ref, strlen(source->token_ref) + 1u);
    memcpy(destination->root_group_id, source->root_group_id,
           strlen(source->root_group_id) + 1u);
    if (source->peer_certificate_sha256)
      memcpy(destination->peer_certificate_sha256, source->peer_certificate_sha256,
             FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE + 1u);
  }
  crypto_wipe(digests,
              config->binding_count * FLOWIE_CONTROL_SERVICE_CREDENTIAL_DIGEST_SIZE);
  free(digests);
  if (rc != TURBO_OK) {
    crypto_wipe(resolver, allocation_size);
    free(resolver);
    return rc;
  }
  *out = resolver;
  return TURBO_OK;
}

void flowie_control_service_credential_resolver_destroy(
    flowie_control_service_credential_resolver_t *resolver) {
  size_t allocation_size;
  if (!resolver) return;
  allocation_size =
      sizeof(*resolver) + resolver->binding_count * sizeof(flowie_control_service_credential_record_t);
  crypto_wipe(resolver, allocation_size);
  free(resolver);
}

int flowie_control_service_credential_resolve(
    flowie_control_service_credential_resolver_t *resolver, const uint8_t *token,
    size_t token_size, const char *verified_peer_certificate_sha256,
    flowie_control_verified_caller_t *caller_out) {
  flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;
  uint8_t actual[FLOWIE_CONTROL_SERVICE_CREDENTIAL_DIGEST_SIZE] = {0};
  size_t matched = SIZE_MAX;
  size_t match_count = 0u;
  int provider_error = TURBO_OK;
  if (caller_out && caller_out->size >= sizeof(*caller_out)) *caller_out = caller;
  if (!resolver || !token || token_size == 0u ||
      token_size > FLOWIE_CONTROL_SERVICE_CREDENTIAL_TOKEN_MAX || !caller_out ||
      caller_out->size < sizeof(*caller_out) || memchr(token, '\0', token_size) ||
      memchr(token, '\r', token_size) || memchr(token, '\n', token_size) ||
      (verified_peer_certificate_sha256 &&
       !flowie_control_service_credential_fingerprint_valid(
           verified_peer_certificate_sha256)))
    return TURBO_EINVAL;
  crypto_blake2b(actual, sizeof(actual), token, token_size);
  for (size_t index = 0u; index < resolver->binding_count; ++index) {
    turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
    uint8_t expected[FLOWIE_CONTROL_SERVICE_CREDENTIAL_DIGEST_SIZE] = {0};
    int rc = turbo_flow_security_secret_acquire(
        &resolver->key_provider, resolver->bindings[index].token_ref, &lease);
    if (rc == TURBO_OK && !flowie_control_service_credential_secret_valid(&lease))
      rc = TURBO_EINVAL;
    if (rc == TURBO_OK) {
      crypto_blake2b(expected, sizeof(expected), lease.bytes, lease.byte_count);
      if (lease.byte_count == token_size && crypto_verify32(expected, actual) == 0 &&
          (resolver->bindings[index].peer_certificate_sha256[0] == '\0' ||
           (verified_peer_certificate_sha256 &&
            strcmp(resolver->bindings[index].peer_certificate_sha256,
                   verified_peer_certificate_sha256) == 0))) {
        matched = index;
        ++match_count;
      }
    } else if (provider_error == TURBO_OK) {
      provider_error = rc;
    }
    crypto_wipe(expected, sizeof(expected));
    turbo_flow_security_secret_release(&resolver->key_provider, &lease);
  }
  crypto_wipe(actual, sizeof(actual));
  if (provider_error != TURBO_OK) return provider_error;
  if (match_count != 1u) return TURBO_EPERM;
  caller.listener_id = resolver->listener_id;
  caller.service_id = resolver->bindings[matched].service_id;
  caller.root_group_id = resolver->bindings[matched].root_group_id;
  caller.peer_certificate_sha256 =
      resolver->bindings[matched].peer_certificate_sha256[0]
          ? resolver->bindings[matched].peer_certificate_sha256
          : NULL;
  caller.authenticated = 1;
  *caller_out = caller;
  return TURBO_OK;
}
