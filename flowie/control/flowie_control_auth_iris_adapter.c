#include "flowie_control_auth_iris_adapter_internal.h"

#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

struct flowie_control_auth_iris_adapter_s {
  flowie_control_auth_service_t *service;
  char listener_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
};

static int flowie_control_auth_iris_listener_valid(const char *listener_id) {
  size_t length;
  if (!listener_id) return 0;
  length = strnlen(listener_id, TURBO_FLOW_SECURITY_ID_MAX + 1u);
  if (length == 0u || length > TURBO_FLOW_SECURITY_ID_MAX) return 0;
  for (size_t index = 0u; index < length; ++index) {
    unsigned char byte = (unsigned char)listener_id[index];
    if (byte < 0x20u || byte == 0x7fu) return 0;
  }
  return 1;
}

int flowie_control_auth_iris_adapter_create(
    const flowie_control_auth_iris_adapter_config_t *config,
    flowie_control_auth_iris_adapter_t **out) {
  flowie_control_auth_iris_adapter_t *adapter;
  size_t listener_size;

  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !config->service || !out ||
      !flowie_control_auth_iris_listener_valid(config->listener_id))
    return TURBO_EINVAL;

  adapter = (flowie_control_auth_iris_adapter_t *)calloc(1u, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  adapter->service = config->service;
  listener_size = strlen(config->listener_id);
  memcpy(adapter->listener_id, config->listener_id, listener_size + 1u);
  *out = adapter;
  return TURBO_OK;
}

void flowie_control_auth_iris_adapter_destroy(flowie_control_auth_iris_adapter_t *adapter) {
  if (!adapter) return;
  memset(adapter, 0, sizeof(*adapter));
  free(adapter);
}

int flowie_control_auth_iris_adapter_authenticate(
    flowie_control_auth_iris_adapter_t *adapter, const Req *http_request,
    const char *identity, const char *method, const uint8_t *secret,
    size_t secret_size, turbo_flow_security_principal_t *principal_out,
    int *credential_cache_hit_out) {
  flowie_control_verified_caller_t caller = FLOWIE_CONTROL_VERIFIED_CALLER_INIT;
  flowie_control_authenticate_request_t request = FLOWIE_CONTROL_AUTHENTICATE_REQUEST_INIT;
  char fingerprint[CORO_TLS_PEER_CERT_SHA256_CAPACITY];
  int rc;

  if (credential_cache_hit_out) *credential_cache_hit_out = 0;
  if (!adapter || !http_request || !identity || !method || !secret || secret_size == 0u ||
      !principal_out || principal_out->size < sizeof(*principal_out))
    return TURBO_EINVAL;

  rc = req_get_verified_tls_peer_certificate_sha256(http_request, fingerprint);
  if (rc != TURBO_OK) {
    *principal_out = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    memset(fingerprint, 0, sizeof(fingerprint));
    return TURBO_EPERM;
  }

  caller.listener_id = adapter->listener_id;
  caller.peer_certificate_sha256 = fingerprint;
  caller.certificate_verified = 1;
  request.caller = &caller;
  request.identity = identity;
  request.method = method;
  request.secret = secret;
  request.secret_size = secret_size;
  rc = flowie_control_auth_service_authenticate(adapter->service, &request, principal_out,
                                                credential_cache_hit_out);
  memset(fingerprint, 0, sizeof(fingerprint));
  return rc;
}
