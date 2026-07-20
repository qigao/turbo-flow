#ifndef FLOW_HTTP_TLS_CLIENT_H
#define FLOW_HTTP_TLS_CLIENT_H

#include "http_client.h"
#include "turbo_flow_http_types.h"
#include "turbo_flow_security.h"
#include "turbo_parser.h"
#include "turbo_str.h"

typedef struct flow_http_tls_client_s {
  tstr_t ca_file;
  tstr_t client_cert_file;
  tstr_t client_key_file;
  tstr_t client_key_password_ref;
} flow_http_tls_client_t;

int flow_http_tls_client_init(flow_http_tls_client_t *tls,
                              const turbo_flow_http_tls_client_config_t *config);
int flow_http_tls_client_probe_secret(const flow_http_tls_client_t *tls,
                                      const turbo_flow_security_key_provider_t *key_provider);
int flow_http_tls_client_apply(const flow_http_tls_client_t *tls,
                               const turbo_flow_security_key_provider_t *key_provider,
                               http_client_t *client);
int flow_http_tls_client_parse_json(const json_value_t *value,
                                    turbo_flow_http_tls_client_config_t *config,
                                    const char **detail);
void flow_http_tls_client_cleanup(flow_http_tls_client_t *tls);

#endif
