#include "turbo_flow_chttp_resource.h"

#include <salts/error_codes.h>
#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdbool.h>

typedef struct fixture_root_s fixture_root_t;

typedef struct fixture_resource_s {
  fixture_root_t *root;
  turbo_flow_chttp_deployment_view_t view;
} fixture_resource_t;

struct fixture_root_s {
  bool started;
  bool stopping;
  fixture_resource_t client;
  fixture_resource_t server;
};

static fixture_root_t fixture_root;

static int resource_snapshot(
    void *self, turbo_flow_chttp_deployment_view_t *view_out) {
  fixture_resource_t *resource = (fixture_resource_t *)self;
  if (!resource || !resource->root || !resource->root->started ||
      resource->root->stopping || !view_out ||
      view_out->size != sizeof(*view_out))
    return SALTS_EINVAL;
  *view_out = resource->view;
  return SALTS_OK;
}

CMETA_IMPLEMENTS(
    turbo_flow_chttp_deployment_resource, fixture_client_resource_impl,
    TURBO_FLOW_CHTTP_RESOURCE_ENDPOINT |
        TURBO_FLOW_CHTTP_RESOURCE_SHAREABLE_SNAPSHOT,
    .snapshot = resource_snapshot);

CMETA_IMPLEMENTS(
    turbo_flow_chttp_deployment_resource, fixture_server_resource_impl,
    TURBO_FLOW_CHTTP_RESOURCE_ENDPOINT |
        TURBO_FLOW_CHTTP_RESOURCE_EXCLUSIVE_BIND,
    .snapshot = resource_snapshot);

static turbo_flow_chttp_deployment_resource client_handle;
static turbo_flow_chttp_deployment_resource server_handle;
static cmeta_plugin_export fixture_exports[2];
static cmeta_plugin_manifest fixture_manifest = {
    .struct_size = CMETA_PLUGIN_MANIFEST_SIZE,
    .abi_version = CMETA_PLUGIN_ABI_VERSION,
    .plugin_id = "test.turboflow.chttp.deployment",
    .version = {1u, 0u, 0u},
    .self = &fixture_root,
};
static cmeta_once_t fixture_once = SALTS_ONCE_INIT;

static void fixture_init(void) {
  fixture_root.client.root = &fixture_root;
  fixture_root.client.view =
      (turbo_flow_chttp_deployment_view_t)
          TURBO_FLOW_CHTTP_DEPLOYMENT_VIEW_INIT;
  fixture_root.client.view.kind = TURBO_FLOW_CHTTP_DEPLOYMENT_CLIENT;
  fixture_root.client.view.connection_uri = "tcp://127.0.0.1:1";

  fixture_root.server.root = &fixture_root;
  fixture_root.server.view =
      (turbo_flow_chttp_deployment_view_t)
          TURBO_FLOW_CHTTP_DEPLOYMENT_VIEW_INIT;
  fixture_root.server.view.kind = TURBO_FLOW_CHTTP_DEPLOYMENT_SERVER;
  fixture_root.server.view.bind_host = "127.0.0.1";
  fixture_root.server.view.bind_port = 0u;

  client_handle =
      fixture_client_resource_impl_as_turbo_flow_chttp_deployment_resource(
          &fixture_root.client);
  server_handle =
      fixture_server_resource_impl_as_turbo_flow_chttp_deployment_resource(
          &fixture_root.server);

  fixture_exports[0] = (cmeta_plugin_export){
      .struct_size = CMETA_PLUGIN_EXPORT_SIZE,
      .kind = CMETA_PLUGIN_EXPORT_INTERFACE,
      .contract_version = TURBO_FLOW_CHTTP_RESOURCE_CONTRACT_VERSION,
      .capabilities =
          TURBO_FLOW_CHTTP_RESOURCE_ENDPOINT |
          TURBO_FLOW_CHTTP_RESOURCE_SHAREABLE_SNAPSHOT,
      .export_id = "fixture.chttp.client",
      .contract_id = TURBO_FLOW_CHTTP_CLIENT_RESOURCE_CONTRACT_ID,
      .value.interface = {
          .desc = turbo_flow_chttp_deployment_resource_interface(),
          .value = &client_handle,
      },
  };
  fixture_exports[1] = (cmeta_plugin_export){
      .struct_size = CMETA_PLUGIN_EXPORT_SIZE,
      .kind = CMETA_PLUGIN_EXPORT_INTERFACE,
      .contract_version = TURBO_FLOW_CHTTP_RESOURCE_CONTRACT_VERSION,
      .capabilities =
          TURBO_FLOW_CHTTP_RESOURCE_ENDPOINT |
          TURBO_FLOW_CHTTP_RESOURCE_EXCLUSIVE_BIND,
      .export_id = "fixture.chttp.server",
      .contract_id = TURBO_FLOW_CHTTP_SERVER_RESOURCE_CONTRACT_ID,
      .value.interface = {
          .desc = turbo_flow_chttp_deployment_resource_interface(),
          .value = &server_handle,
      },
  };

  fixture_manifest.exports = fixture_exports;
  fixture_manifest.export_count = 2u;
}

static cmeta_plugin_status CMETA_PLUGIN_CALL fixture_start(void *self) {
  fixture_root_t *root = (fixture_root_t *)self;
  if (!root) return CMETA_PLUGIN_INVALID_ARGUMENT;
  root->stopping = false;
  root->started = true;
  return CMETA_PLUGIN_OK;
}

static cmeta_plugin_status CMETA_PLUGIN_CALL fixture_request_stop(void *self) {
  fixture_root_t *root = (fixture_root_t *)self;
  if (!root) return CMETA_PLUGIN_INVALID_ARGUMENT;
  root->stopping = true;
  root->started = false;
  return CMETA_PLUGIN_OK;
}

static bool CMETA_PLUGIN_CALL fixture_is_quiescent(const void *self) {
  const fixture_root_t *root = (const fixture_root_t *)self;
  return root && root->stopping;
}

static void CMETA_PLUGIN_CALL fixture_destroy(void *self) {
  fixture_root_t *root = (fixture_root_t *)self;
  if (!root) return;
  root->started = false;
  root->stopping = true;
}

CMETA_PLUGIN_QUERY_EXPORT
const cmeta_plugin_manifest *CMETA_PLUGIN_CALL
cmeta_plugin_query(uint32_t host_abi) {
  if (host_abi != CMETA_PLUGIN_ABI_VERSION) return NULL;
  salts_once(&fixture_once, fixture_init);
  fixture_manifest.start = fixture_start;
  fixture_manifest.request_stop = fixture_request_stop;
  fixture_manifest.is_quiescent = fixture_is_quiescent;
  fixture_manifest.destroy = fixture_destroy;
  return &fixture_manifest;
}
