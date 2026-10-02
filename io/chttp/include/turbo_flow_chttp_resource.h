#ifndef TURBO_FLOW_CHTTP_RESOURCE_H
#define TURBO_FLOW_CHTTP_RESOURCE_H

#include <cmeta/interface.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_CHTTP_CLIENT_RESOURCE_CONTRACT_ID \
  "turbo_flow.chttp.client_deployment"
#define TURBO_FLOW_CHTTP_SERVER_RESOURCE_CONTRACT_ID \
  "turbo_flow.chttp.server_deployment"

enum {
  TURBO_FLOW_CHTTP_RESOURCE_CONTRACT_VERSION = 1u,
  TURBO_FLOW_CHTTP_RESOURCE_ENDPOINT = UINT64_C(1) << 0,
  /*
   * snapshot() returns immutable borrowed deployment facts that remain stable
   * for every concurrent Salts Plugin lease. Providers may require this bit
   * only when the endpoint semantics themselves permit independent owners to
   * share one resolved resource identity. CHTTP client deployments admit this;
   * server/WebSocket bind deployments intentionally do not.
   */
  TURBO_FLOW_CHTTP_RESOURCE_SHAREABLE_SNAPSHOT = UINT64_C(1) << 1,
  /*
   * One resolved server/WebSocket deployment identity admits exactly one
   * native bind owner in a generation. Source + terminal stages may share that
   * owner, but a second independent server owner must resolve a different
   * deployment resource identity.
   */
  TURBO_FLOW_CHTTP_RESOURCE_EXCLUSIVE_BIND = UINT64_C(1) << 2
};

typedef enum turbo_flow_chttp_deployment_kind_e {
  TURBO_FLOW_CHTTP_DEPLOYMENT_CLIENT = 1,
  TURBO_FLOW_CHTTP_DEPLOYMENT_SERVER = 2
} turbo_flow_chttp_deployment_kind_t;

/**
 * Borrowed deployment endpoint/TLS view.
 *
 * Secrets and environment-specific endpoint identity stay owned by the
 * resource plugin under its Salts Plugin lease. The provider may borrow these
 * pointers only while that lease remains alive. An export that advertises
 * TURBO_FLOW_CHTTP_RESOURCE_SHAREABLE_SNAPSHOT guarantees that these borrowed
 * facts are immutable/stable across concurrent leases; native CHTTP owners are
 * still materialized independently per stage instance.
 */
typedef struct turbo_flow_chttp_deployment_view_s {
  size_t size;
  turbo_flow_chttp_deployment_kind_t kind;

  /* Client endpoint; NULL for server resources. */
  const char *connection_uri;

  /* Server bind identity; NULL/zero for client resources. */
  const char *bind_host;
  uint16_t bind_port;

  /* Deployment-owned TLS identity/material. Empty/NULL means absent. */
  const char *tls_ca_file;
  const char *tls_ca_path;
  const char *tls_cert_file;
  const char *tls_key_file;
  const char *tls_key_password;
  const char *tls_server_name;

  /* Ordered negotiated protocol policy owned by the deployment resource. */
  const char *const *alpn_protocols;
  size_t alpn_protocol_count;
} turbo_flow_chttp_deployment_view_t;

#define TURBO_FLOW_CHTTP_DEPLOYMENT_VIEW_INIT \
  {sizeof(turbo_flow_chttp_deployment_view_t), \
   (turbo_flow_chttp_deployment_kind_t)0, NULL, NULL, 0u, \
   NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0u}

#define TURBO_FLOW_CHTTP_DEPLOYMENT_RESOURCE_METHODS(X, I) \
  X(I, R1, int, snapshot, turbo_flow_chttp_deployment_view_t *, view_out)

CMETA_INTERFACE(
    turbo_flow_chttp_deployment_resource,
    TURBO_FLOW_CHTTP_DEPLOYMENT_RESOURCE_METHODS);

static inline int turbo_flow_chttp_deployment_view_valid(
    const turbo_flow_chttp_deployment_view_t *view) {
  if (view == NULL || view->size != sizeof(*view)) return 0;
  if (view->alpn_protocol_count != 0u && view->alpn_protocols == NULL) return 0;
  if (view->kind == TURBO_FLOW_CHTTP_DEPLOYMENT_CLIENT)
    return view->connection_uri != NULL && view->connection_uri[0] != '\0' &&
           view->bind_host == NULL && view->bind_port == 0u;
  if (view->kind == TURBO_FLOW_CHTTP_DEPLOYMENT_SERVER)
    return view->connection_uri == NULL &&
           view->bind_host != NULL && view->bind_host[0] != '\0';
  return 0;
}

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_CHTTP_RESOURCE_H */
