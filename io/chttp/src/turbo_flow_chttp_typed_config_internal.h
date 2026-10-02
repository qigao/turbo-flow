#ifndef TURBO_FLOW_CHTTP_TYPED_CONFIG_INTERNAL_H
#define TURBO_FLOW_CHTTP_TYPED_CONFIG_INTERNAL_H

#include "chttp_provider_config_native.h"
#include "turbo_flow_chttp.h"
#include "turbo_flow_chttp_resource.h"
#include "turbo_flow_provider.h"

enum {
  CHTTP_TYPED_CLIENT = 1,
  CHTTP_TYPED_SERVER = 2,
  CHTTP_TYPED_WEBSOCKET = 4,
  CHTTP_TYPED_HEADER_CAPACITY = 64
};

/*
 * Borrowed policy/resource projection used by the canonical provider.
 *
 * Generated strings/list elements remain owned by the compiled provider
 * instance. Deployment strings remain owned by the resource lease. The provider
 * runtime owner must not outlive either binding.
 */
typedef struct chttp_typed_runtime_config_s {
  unsigned kind;
  uint32_t poll_budget_ms;
  int tls_enabled;

  cnet_client_config network;
  chttp_client_config client;
  chttp_server_config server;
  cnet_tls_client_config tls_client;
  cnet_tls_server_config tls_server;

  turbo_flow_chttp_client_config_t client_adapter;
  turbo_flow_chttp_server_config_t server_adapter;
  turbo_flow_chttp_websocket_server_config_t websocket_adapter;

  chttp_header headers[CHTTP_TYPED_HEADER_CAPACITY];
} chttp_typed_runtime_config_t;

int chttp_typed_client_config(
    const CHttpClientConfig_t *typed,
    const turbo_flow_chttp_deployment_view_t *deployment,
    const char *instance_name,
    chttp_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error);

int chttp_typed_server_config(
    const CHttpServerConfig_t *typed,
    const turbo_flow_chttp_deployment_view_t *deployment,
    const char *instance_name,
    chttp_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error);

int chttp_typed_websocket_config(
    const CHttpWebSocketServerConfig_t *typed,
    const turbo_flow_chttp_deployment_view_t *deployment,
    const char *instance_name,
    chttp_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error);

#endif
