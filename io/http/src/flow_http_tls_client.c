#include "flow_http_tls_client.h"

#include "turbo_crypto.h"
#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

static int flow_http_tls_string_valid(const char *value, size_t limit) {
  size_t length;
  if (!value) return 1;
  length = strlen(value);
  return length > 0u && length <= limit;
}

static int flow_http_tls_config_valid(const turbo_flow_http_tls_client_config_t *config) {
  int has_cert;
  int has_key;
  if (!config) return 1;
  has_cert = config->client_cert_file != NULL;
  has_key = config->client_key_file != NULL;
  return has_cert == has_key && (!config->client_key_password_ref || has_key) &&
         flow_http_tls_string_valid(config->ca_file, TURBO_FLOW_HTTP_TLS_PATH_LIMIT) &&
         flow_http_tls_string_valid(config->client_cert_file, TURBO_FLOW_HTTP_TLS_PATH_LIMIT) &&
         flow_http_tls_string_valid(config->client_key_file, TURBO_FLOW_HTTP_TLS_PATH_LIMIT) &&
         flow_http_tls_string_valid(config->client_key_password_ref,
                                    TURBO_FLOW_HTTP_TLS_PATH_LIMIT);
}

static int flow_http_tls_secret_valid(const turbo_flow_security_secret_lease_t *lease) {
  return lease && lease->bytes && lease->byte_count > 0u &&
         lease->byte_count <= TURBO_FLOW_HTTP_TLS_SECRET_LIMIT &&
         !memchr(lease->bytes, '\0', lease->byte_count);
}

int flow_http_tls_client_init(flow_http_tls_client_t *tls,
                              const turbo_flow_http_tls_client_config_t *config) {
  flow_http_tls_client_t next = {0};
  if (!tls || !flow_http_tls_config_valid(config)) return TURBO_EINVAL;
  if (!config) {
    *tls = next;
    return TURBO_OK;
  }
  next.ca_file = config->ca_file ? tstr_dup(config->ca_file) : NULL;
  next.client_cert_file =
      config->client_cert_file ? tstr_dup(config->client_cert_file) : NULL;
  next.client_key_file = config->client_key_file ? tstr_dup(config->client_key_file) : NULL;
  next.client_key_password_ref =
      config->client_key_password_ref ? tstr_dup(config->client_key_password_ref) : NULL;
  if ((config->ca_file && !next.ca_file) ||
      (config->client_cert_file && !next.client_cert_file) ||
      (config->client_key_file && !next.client_key_file) ||
      (config->client_key_password_ref && !next.client_key_password_ref)) {
    flow_http_tls_client_cleanup(&next);
    return TURBO_ENOMEM;
  }
  *tls = next;
  return TURBO_OK;
}

int flow_http_tls_client_probe_secret(const flow_http_tls_client_t *tls,
                                      const turbo_flow_security_key_provider_t *key_provider) {
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  int rc;
  if (!tls || !key_provider) return TURBO_EINVAL;
  if (!tls->client_key_password_ref) return TURBO_OK;
  rc = turbo_flow_security_secret_acquire(key_provider, tls->client_key_password_ref, &lease);
  if (rc == TURBO_OK && !flow_http_tls_secret_valid(&lease)) rc = TURBO_EPERM;
  turbo_flow_security_secret_release(key_provider, &lease);
  return rc;
}

int flow_http_tls_client_apply(const flow_http_tls_client_t *tls,
                               const turbo_flow_security_key_provider_t *key_provider,
                               http_client_t *client) {
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  turbo_tls_client_config_t config = {0};
  char *password = NULL;
  int rc = TURBO_OK;
  if (!tls || !key_provider || !client) return TURBO_EINVAL;
  if (!tls->ca_file && !tls->client_cert_file) return TURBO_OK;
  if (tls->client_key_password_ref) {
    rc = turbo_flow_security_secret_acquire(key_provider, tls->client_key_password_ref, &lease);
    if (rc != TURBO_OK) goto done;
    if (!flow_http_tls_secret_valid(&lease)) {
      rc = TURBO_EPERM;
      goto done;
    }
    password = (char *)malloc(lease.byte_count + 1u);
    if (!password) {
      rc = TURBO_ENOMEM;
      goto done;
    }
    memcpy(password, lease.bytes, lease.byte_count);
    password[lease.byte_count] = '\0';
  }
  config.ca_file = tls->ca_file;
  config.cert_file = tls->client_cert_file;
  config.key_file = tls->client_key_file;
  config.key_password = password;
  config.verify_peer = 1;
  rc = http_client_set_tls_client_config(client, &config);

done:
  if (password) {
    turbo_crypto_wipe(password, lease.byte_count);
    free(password);
  }
  turbo_flow_security_secret_release(key_provider, &lease);
  return rc;
}

int flow_http_tls_client_parse_json(const json_value_t *value,
                                    turbo_flow_http_tls_client_config_t *config,
                                    const char **detail) {
  static const char *const allowed[] = {"ca_file", "client_cert_file", "client_key_file",
                                        "client_key_password_ref"};
  if (detail) *detail = NULL;
  if (!config || !detail) return TURBO_EINVAL;
  *config = (turbo_flow_http_tls_client_config_t)TURBO_FLOW_HTTP_TLS_CLIENT_CONFIG_INIT;
  if (!value) return TURBO_OK;
  if (turbo_json_type(value) != TURBO_JSON_OBJECT) {
    *detail = "tls must be a mapping";
    return TURBO_EINVAL;
  }
  for (size_t i = 0u; i < turbo_json_object_size(value); ++i) {
    const char *field = turbo_json_object_key(value, i);
    json_value_t *field_value = turbo_json_object_get(value, field);
    int known = 0;
    for (size_t j = 0u; j < sizeof(allowed) / sizeof(allowed[0]); ++j)
      if (field && strcmp(field, allowed[j]) == 0) known = 1;
    if (!known) {
      *detail = "unknown TLS client field";
      return TURBO_EINVAL;
    }
    if (!field_value || turbo_json_type(field_value) != TURBO_JSON_STRING) {
      *detail = "TLS client fields must be strings";
      return TURBO_EINVAL;
    }
  }
  config->ca_file = turbo_json_get_string(value, "ca_file");
  config->client_cert_file = turbo_json_get_string(value, "client_cert_file");
  config->client_key_file = turbo_json_get_string(value, "client_key_file");
  config->client_key_password_ref = turbo_json_get_string(value, "client_key_password_ref");
  if (!flow_http_tls_config_valid(config)) {
    *detail = "TLS paths must be non-empty and client certificate/key must be configured together";
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

void flow_http_tls_client_cleanup(flow_http_tls_client_t *tls) {
  if (!tls) return;
  tstr_freep(&tls->ca_file);
  tstr_freep(&tls->client_cert_file);
  tstr_freep(&tls->client_key_file);
  tstr_freep(&tls->client_key_password_ref);
}
