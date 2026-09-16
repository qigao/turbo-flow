#include "turbo_flow_chttp_plugin_internal.h"
#include <cstl/vec.h>
#include <string.h>

typedef struct plugin_root_s plugin_root_t;
typedef struct provider_s {
  plugin_root_t *root;
  unsigned kind;
} provider_t;
struct plugin_root_s {
  const turbo_flow_plugin_host_v1_t *host;
  vec_t owners;
  provider_t providers[CHTTP_PLUGIN_KIND_COUNT];
  int quiesced;
};
typedef struct owner_s {
  plugin_root_t *root;
  chttp_plugin_config_t config;
  chttp_tls_profile tls;
  union {
    turbo_flow_chttp_client_t *client;
    turbo_flow_chttp_server_t *server;
    turbo_flow_chttp_websocket_server_t *websocket;
    void *any;
  } handle;
} owner_t;

static int owner_idle(owner_t *o) {
  int rc;
  if (!o || !o->handle.any) return SALTS_EINVAL;
  switch (o->config.kind) {
  case CHTTP_PLUGIN_CLIENT: {
    turbo_flow_chttp_client_snapshot_t s = TURBO_FLOW_CHTTP_CLIENT_SNAPSHOT_INIT;
    rc = turbo_flow_chttp_client_snapshot(o->handle.client, &s);
    if (rc != SALTS_OK) return rc;
    return (s.state == TURBO_FLOW_CHTTP_CLIENT_REGISTERED ||
            s.state == TURBO_FLOW_CHTTP_CLIENT_STOPPED ||
            s.state == TURBO_FLOW_CHTTP_CLIENT_DETACHED) &&
                   !s.active_requests && !s.queued_requests
               ? SALTS_OK
               : SALTS_EBUSY;
  }
  case CHTTP_PLUGIN_SERVER: {
    turbo_flow_chttp_server_snapshot_t s = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    rc = turbo_flow_chttp_server_snapshot(o->handle.server, &s);
    if (rc != SALTS_OK) return rc;
    return (s.state == TURBO_FLOW_CHTTP_SERVER_REGISTERED ||
            s.state == TURBO_FLOW_CHTTP_SERVER_STOPPED ||
            s.state == TURBO_FLOW_CHTTP_SERVER_DETACHED) &&
                   !s.active_requests
               ? SALTS_OK
               : SALTS_EBUSY;
  }
  default: {
    turbo_flow_chttp_websocket_server_snapshot_t s =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    rc = turbo_flow_chttp_websocket_server_snapshot(o->handle.websocket, &s);
    if (rc != SALTS_OK) return rc;
    return (s.state == TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_REGISTERED ||
            s.state == TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STOPPED ||
            s.state == TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_DETACHED) &&
                   !s.active_sessions && !s.in_flight_frames
               ? SALTS_OK
               : SALTS_EBUSY;
  }
  }
}
static int owner_quiesce(void *ctx, uint64_t timeout_ms) {
  owner_t *o = (owner_t *)ctx;
  (void)timeout_ms;
  if (!o) return SALTS_EINVAL;
  if (owner_idle(o) == SALTS_OK) return SALTS_OK;
  switch (o->config.kind) {
  case CHTTP_PLUGIN_CLIENT:
    return turbo_flow_chttp_client_quiesce(o->handle.client);
  case CHTTP_PLUGIN_SERVER:
    return turbo_flow_chttp_server_quiesce(o->handle.server);
  default:
    return turbo_flow_chttp_websocket_server_quiesce(o->handle.websocket);
  }
}
static int owner_drain(void *ctx, uint64_t timeout_ms) {
  (void)timeout_ms;
  return owner_idle((owner_t *)ctx);
}
/* Generation shutdown precedes Graph detach. Native storage is released by destroy. */
static int owner_shutdown(void *ctx) { return owner_idle((owner_t *)ctx); }
static int owner_poll(void *ctx, uint32_t timeout_ms) {
  owner_t *o = (owner_t *)ctx;
  if (!o || o->config.kind != CHTTP_PLUGIN_CLIENT) return SALTS_EINVAL;
  if (timeout_ms > o->config.poll_budget_ms) timeout_ms = o->config.poll_budget_ms;
  return turbo_flow_chttp_client_poll(o->handle.client, timeout_ms, NULL);
}
static void owner_destroy(void *ctx) {
  owner_t *o = (owner_t *)ctx;
  plugin_root_t *root;
  int rc = SALTS_OK;
  if (!o) return;
  root = o->root;
  if (o->handle.any) {
    switch (o->config.kind) {
    case CHTTP_PLUGIN_CLIENT:
      rc = turbo_flow_chttp_client_destroy(o->handle.client);
      break;
    case CHTTP_PLUGIN_SERVER:
      rc = turbo_flow_chttp_server_destroy(o->handle.server);
      break;
    default:
      rc = turbo_flow_chttp_websocket_server_destroy(o->handle.websocket);
      break;
    }
  }
  /* A violated detach contract must retain its root/module rather than free borrowed storage. */
  if (rc != SALTS_OK) return;
  if (chttp_tls_profile_destroy(&o->tls) != SALTS_OK) return;
  for (size_t i = 0u; i < vec_size(&root->owners); ++i) {
    owner_t *const *entry = (owner_t *const *)vec_at_const(&root->owners, i);
    if (*entry == o) {
      (void)vec_erase(&root->owners, i, NULL);
      break;
    }
  }
  memset(o, 0, sizeof(*o));
  root->host->deallocate(root->host->ctx, o);
}
static int references(turbo_flow_t *flow, const char *name, unsigned kind, const char **source) {
  size_t sources = 0u, stages = 0u;
  *source = NULL;
  for (size_t i = 0u; i < turbo_flow_stage_count(flow); ++i) {
    const turbo_flow_stage_plan_t *s = turbo_flow_stage_at(flow, i);
    if (!s || !s->adapter_name || strcmp(s->adapter_name, name)) continue;
    if (s->is_source) {
      ++sources;
      *source = s->name;
    } else {
      ++stages;
      /* Parsed edge indices resolve during compile; the registered native
       * async terminal schema enforces the terminal topology there. */
    }
  }
  return stages == 1u && sources == (kind == CHTTP_PLUGIN_CLIENT ? 0u : 1u) ? SALTS_OK
                                                                            : SALTS_EINVAL;
}
static int preflight(void *ctx, const turbo_flow_resolved_config_t *resolved, const char *name,
                     turbo_flow_config_error_t *error) {
  provider_t *p = (provider_t *)ctx;
  chttp_plugin_config_t config;
  if (!p || p->root->quiesced) return SALTS_ESHUTDOWN;
  return chttp_plugin_config_read(resolved, name, p->kind, &config, error);
}
static int materialize(void *ctx, turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved,
                       const char *name, turbo_flow_plugin_product_owner_v1_t *out,
                       turbo_flow_config_error_t *error) {
  provider_t *p = (provider_t *)ctx;
  const char *source;
  owner_t *o;
  int rc;
  turbo_flow_plugin_product_owner_v1_t descriptor = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  if (!p || !flow || !out || out->size < sizeof(*out) || p->root->quiesced) return SALTS_EINVAL;
  rc = references(flow, name, p->kind, &source);
  if (rc != SALTS_OK) return rc;
  if (vec_size(&p->root->owners) >= CHTTP_PLUGIN_MAX_OWNERS) return SALTS_ENOSPC;
  o = (owner_t *)p->root->host->allocate(p->root->host->ctx, sizeof(*o));
  if (!o) return SALTS_ENOMEM;
  memset(o, 0, sizeof(*o));
  o->root = p->root;
  rc = chttp_plugin_config_read(resolved, name, p->kind, &o->config, error);
  if (rc != SALTS_OK) goto fail;
  if (p->kind == CHTTP_PLUGIN_CLIENT && o->config.tls_enabled) {
    rc = chttp_tls_profile_init(&o->tls, &o->config.tls_client);
    if (rc != SALTS_OK) goto fail;
  }
  if (vec_push(&p->root->owners, &o) != STL_OK) {
    rc = SALTS_ENOMEM;
    goto fail;
  }
  descriptor.flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD;
  if (p->kind == CHTTP_PLUGIN_CLIENT) {
    descriptor.flags |= TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
    descriptor.poll = owner_poll;
  }
  descriptor.ctx = o;
  descriptor.quiesce = owner_quiesce;
  descriptor.drain = owner_drain;
  descriptor.shutdown = owner_shutdown;
  descriptor.destroy = owner_destroy;
  rc = turbo_flow_plugin_product_owner_publish(out, &descriptor);
  if (rc != SALTS_OK) {
    owner_destroy(o);
    return rc;
  }
  switch (p->kind) {
  case CHTTP_PLUGIN_CLIENT:
    o->config.client_adapter.flow = flow;
    o->config.client_adapter.adapter_name = name;
    o->config.client_adapter.tls = o->config.tls_enabled ? &o->tls : NULL;
    rc = turbo_flow_chttp_client_register(&o->config.client_adapter, &o->handle.client);
    break;
  case CHTTP_PLUGIN_SERVER:
    o->config.server_adapter.flow = flow;
    o->config.server_adapter.adapter_name = name;
    o->config.server_adapter.source_name = source;
    rc = turbo_flow_chttp_server_register(&o->config.server_adapter, &o->handle.server);
    break;
  default:
    o->config.websocket_adapter.flow = flow;
    o->config.websocket_adapter.adapter_name = name;
    o->config.websocket_adapter.source_name = source;
    rc = turbo_flow_chttp_websocket_server_register(&o->config.websocket_adapter,
                                                    &o->handle.websocket);
    break;
  }
  if (rc == SALTS_OK) return rc;
  *out = (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  owner_destroy(o);
  return rc;
fail:
  (void)chttp_tls_profile_destroy(&o->tls);
  memset(o, 0, sizeof(*o));
  p->root->host->deallocate(p->root->host->ctx, o);
  return rc;
}
static int plugin_load(const turbo_flow_plugin_host_v1_t *host, void **out) {
  plugin_root_t *root;
  stl_status rc;
  if (out) *out = NULL;
  if (!out || !host || host->size != sizeof(*host)) return SALTS_EINVAL;
  if (host->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      host->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR) return SALTS_EINVAL;
  if (!host->allocate || !host->deallocate) return SALTS_EINVAL;
  root = (plugin_root_t *)host->allocate(host->ctx, sizeof(*root));
  if (!root) return SALTS_ENOMEM;
  memset(root, 0, sizeof(*root));
  root->host = host;
  rc = vec_init_bytes(&root->owners, sizeof(owner_t *), _Alignof(owner_t *),
                      CHTTP_PLUGIN_MAX_OWNERS);
  if (rc == STL_OK) rc = vec_reserve(&root->owners, CHTTP_PLUGIN_MAX_OWNERS);
  if (rc != STL_OK) {
    vec_destroy(&root->owners);
    host->deallocate(host->ctx, root);
    return SALTS_ENOMEM;
  }
  for (size_t i = 0u; i < CHTTP_PLUGIN_KIND_COUNT; ++i) {
    root->providers[i].root = root;
    root->providers[i].kind = 1u << i;
  }
  *out = root;
  return SALTS_OK;
}
static int plugin_register(void *ctx, const turbo_flow_plugin_registration_v1_t *registration) {
  plugin_root_t *root = (plugin_root_t *)ctx;
  if (!root || !registration || registration->size != sizeof(*registration) ||
      registration->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !registration->add_transactional_adapter_provider)
    return SALTS_EINVAL;
  for (size_t i = 0u; i < CHTTP_PLUGIN_KIND_COUNT; ++i) {
    turbo_flow_plugin_transactional_adapter_provider_v1_t p =
        TURBO_FLOW_PLUGIN_TRANSACTIONAL_ADAPTER_PROVIDER_V1_INIT;
    p.kind = chttp_plugin_kind_name(root->providers[i].kind);
    p.ctx = &root->providers[i];
    p.preflight = preflight;
    p.materialize = materialize;
    int rc = registration->add_transactional_adapter_provider(registration->ctx, &p);
    if (rc != SALTS_OK) return rc;
  }
  return SALTS_OK;
}
static int plugin_quiesce(void *ctx, uint64_t timeout_ms) {
  plugin_root_t *root = (plugin_root_t *)ctx;
  (void)timeout_ms;
  if (!root) return SALTS_EINVAL;
  if (vec_size(&root->owners)) return SALTS_EBUSY;
  root->quiesced = 1;
  return SALTS_OK;
}
static int plugin_shutdown(void *ctx) {
  plugin_root_t *root = (plugin_root_t *)ctx;
  return root && root->quiesced && !vec_size(&root->owners) ? SALTS_OK : SALTS_EBUSY;
}
static void plugin_destroy(void *ctx) {
  plugin_root_t *root = (plugin_root_t *)ctx;
  if (!root || vec_size(&root->owners)) return;
  const turbo_flow_plugin_host_v1_t *host = root->host;
  vec_destroy(&root->owners);
  memset(root, 0, sizeof(*root));
  host->deallocate(host->ctx, root);
}
static const turbo_flow_plugin_api_v1_t api = {sizeof(turbo_flow_plugin_api_v1_t),
                                               TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
                                               TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
                                               "turbo-flow.chttp",
                                               "1.0.0",
                                               TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_ADAPTER |
                                                   TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL,
                                               plugin_load,
                                               plugin_register,
                                               plugin_quiesce,
                                               plugin_shutdown,
                                               plugin_destroy};
TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &api;
}
