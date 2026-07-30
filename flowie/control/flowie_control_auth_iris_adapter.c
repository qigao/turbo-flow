#include "flowie_control_auth_iris_adapter_internal.h"

#include "monocypher.h"
#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

struct flowie_control_auth_iris_adapter_s {
  flowie_control_auth_service_t *service;
};

int flowie_control_auth_iris_adapter_create(const flowie_control_auth_iris_adapter_config_t *config,
                                            flowie_control_auth_iris_adapter_t **out) {
  flowie_control_auth_iris_adapter_t *adapter;

  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !config->service || !out)
    return TURBO_EINVAL;

  adapter = (flowie_control_auth_iris_adapter_t *)calloc(1u, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  adapter->service = config->service;
  *out = adapter;
  return TURBO_OK;
}

void flowie_control_auth_iris_adapter_destroy(flowie_control_auth_iris_adapter_t *adapter) {
  if (!adapter) return;
  memset(adapter, 0, sizeof(*adapter));
  free(adapter);
}

int flowie_control_auth_iris_adapter_optional_verified_peer_certificate(
    const Req *http_request, char peer_certificate_sha256[CORO_TLS_PEER_CERT_SHA256_CAPACITY]) {
  int rc;
  if (!peer_certificate_sha256) return TURBO_EINVAL;
  memset(peer_certificate_sha256, 0, CORO_TLS_PEER_CERT_SHA256_CAPACITY);
  if (!http_request) return TURBO_EINVAL;
  rc = req_get_verified_tls_peer_certificate_sha256(http_request, peer_certificate_sha256);
  if (rc == TURBO_OK) return TURBO_OK;
  crypto_wipe(peer_certificate_sha256, CORO_TLS_PEER_CERT_SHA256_CAPACITY);
  return TURBO_OK;
}

int flowie_control_auth_iris_adapter_authenticate_verified(
    flowie_control_auth_iris_adapter_t *adapter, const flowie_control_verified_caller_t *caller,
    const char *identity, const char *method, const uint8_t *secret, size_t secret_size,
    const char *protocol, const char *remote_address, const char *client_peer_certificate_sha256,
    turbo_flow_security_principal_t *principal_out, int *credential_cache_hit_out) {
  flowie_control_authenticate_request_t request = FLOWIE_CONTROL_AUTHENTICATE_REQUEST_INIT;

  if (credential_cache_hit_out) *credential_cache_hit_out = 0;
  if (!adapter || !caller || caller->size < sizeof(*caller) || !identity || !method || !secret ||
      secret_size == 0u || !protocol || !remote_address || !principal_out ||
      principal_out->size < sizeof(*principal_out))
    return TURBO_EINVAL;

  request.caller = caller;
  request.identity = identity;
  request.method = method;
  request.secret = secret;
  request.secret_size = secret_size;
  request.protocol = protocol;
  request.remote_address = remote_address;
  request.peer_certificate_sha256 = client_peer_certificate_sha256;
  return flowie_control_auth_service_authenticate(adapter->service, &request, principal_out,
                                                  credential_cache_hit_out);
}
