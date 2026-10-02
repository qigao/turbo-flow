#ifndef TURBO_FLOW_CNET_RESOURCE_H
#define TURBO_FLOW_CNET_RESOURCE_H

#include <cmeta/interface.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_CNET_DEPLOYMENT_RESOURCE_CONTRACT_ID \
  "turbo_flow.cnet.deployment"

enum {
  TURBO_FLOW_CNET_RESOURCE_CONTRACT_VERSION = 1u,
  TURBO_FLOW_CNET_RESOURCE_CLIENT_ENDPOINT = UINT64_C(1) << 0,
  TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT = UINT64_C(1) << 1,
  TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT = UINT64_C(1) << 2,
  TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL = UINT64_C(1) << 3,
  TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL = UINT64_C(1) << 4,
  TURBO_FLOW_CNET_RESOURCE_SHAREABLE_SNAPSHOT = UINT64_C(1) << 5,
  TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND = UINT64_C(1) << 6
};

/**
 * Borrowed deployment endpoint/secret view.
 *
 * All pointers remain resource-plugin owned under the retained Salts Plugin
 * lease. TurboFlow Core never copies these values into generic config,
 * diagnostics, snapshots, or durable metadata. A provider may project/copy
 * secret material only into its own runtime owner when the native CNet ABI
 * requires owned bytes.
 */
typedef struct turbo_flow_cnet_deployment_view_s {
  size_t size;

  /* Stream client endpoint. */
  const char *uri;

  /* Local listener/datagram/packet bind endpoint. */
  const char *bind_host;
  uint16_t bind_port;

  /* Fixed datagram/packet peer endpoint. */
  const char *peer_host;
  uint16_t peer_port;
  uint32_t peer_scope_id;

  /* TLS material. Empty/NULL means absent. */
  const char *tls_ca_file;
  const char *tls_ca_path;
  const char *tls_cert_file;
  const char *tls_key_file;
  const char *tls_key_password;
  const char *tls_server_name;
  const char *const *alpn_protocols;
  size_t alpn_protocol_count;

  /* KCP PSK bytes. Empty means plain/no-secret mode. */
  const uint8_t *psk;
  size_t psk_size;
} turbo_flow_cnet_deployment_view_t;

#define TURBO_FLOW_CNET_DEPLOYMENT_VIEW_INIT \
  {sizeof(turbo_flow_cnet_deployment_view_t), \
   NULL, NULL, 0u, NULL, 0u, 0u, \
   NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0u, NULL, 0u}

#define TURBO_FLOW_CNET_DEPLOYMENT_RESOURCE_METHODS(X, I) \
  X(I, R1, int, snapshot, turbo_flow_cnet_deployment_view_t *, view_out)

CMETA_INTERFACE(
    turbo_flow_cnet_deployment_resource,
    TURBO_FLOW_CNET_DEPLOYMENT_RESOURCE_METHODS);

static inline int turbo_flow_cnet_deployment_view_valid(
    const turbo_flow_cnet_deployment_view_t *view) {
  if (!view || view->size != sizeof(*view)) return 0;
  if (view->alpn_protocol_count != 0u && !view->alpn_protocols) return 0;
  if (view->psk_size != 0u && !view->psk) return 0;
  if (view->uri && !view->uri[0]) return 0;
  if (view->bind_host && !view->bind_host[0]) return 0;
  if (view->peer_host && !view->peer_host[0]) return 0;
  return 1;
}

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_CNET_RESOURCE_H */
