#include "turbo_flow_cnet_resource.h"

#include <salts/error_codes.h>
#include <salts/plugin.h>
#include <salts/thread.h>

#include <stdbool.h>

typedef struct fixture_root_s fixture_root_t;

typedef struct fixture_resource_s {
  fixture_root_t *root;
  turbo_flow_cnet_deployment_view_t view;
} fixture_resource_t;

struct fixture_root_s {
  bool started;
  bool stopping;
  fixture_resource_t stream;
  fixture_resource_t listener;
  fixture_resource_t datagram;
  fixture_resource_t packet_source;
  fixture_resource_t packet_sink;
};

static fixture_root_t fixture_root;

static int resource_snapshot(
    void *self, turbo_flow_cnet_deployment_view_t *view_out) {
  fixture_resource_t *resource = (fixture_resource_t *)self;
  if (!resource || !resource->root || !resource->root->started ||
      resource->root->stopping || !view_out ||
      view_out->size != sizeof(*view_out))
    return SALTS_EINVAL;
  *view_out = resource->view;
  return SALTS_OK;
}

CMETA_IMPLEMENTS(
    turbo_flow_cnet_deployment_resource, fixture_stream_impl,
    TURBO_FLOW_CNET_RESOURCE_CLIENT_ENDPOINT |
        TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
        TURBO_FLOW_CNET_RESOURCE_SHAREABLE_SNAPSHOT,
    .snapshot = resource_snapshot);

CMETA_IMPLEMENTS(
    turbo_flow_cnet_deployment_resource, fixture_listener_impl,
    TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
        TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
        TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
    .snapshot = resource_snapshot);

CMETA_IMPLEMENTS(
    turbo_flow_cnet_deployment_resource, fixture_datagram_impl,
    TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
        TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT |
        TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
    .snapshot = resource_snapshot);

CMETA_IMPLEMENTS(
    turbo_flow_cnet_deployment_resource, fixture_packet_source_impl,
    TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
        TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL |
        TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
    .snapshot = resource_snapshot);

CMETA_IMPLEMENTS(
    turbo_flow_cnet_deployment_resource, fixture_packet_sink_impl,
    TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
        TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT |
        TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL |
        TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
    .snapshot = resource_snapshot);

static turbo_flow_cnet_deployment_resource handles[5];
static salts_plugin_export exports[5];
static salts_plugin_manifest manifest = {
    .struct_size = SALTS_PLUGIN_MANIFEST_SIZE,
    .abi_version = SALTS_PLUGIN_ABI_VERSION,
    .plugin_id = "test.turboflow.cnet.deployment",
    .version = {1u, 0u, 0u},
    .self = &fixture_root,
};
static salts_once_t fixture_once = SALTS_ONCE_INIT;

static void fixture_export(
    size_t index, const char *export_id, uint64_t capabilities,
    turbo_flow_cnet_deployment_resource handle) {
  handles[index] = handle;
  exports[index] = (salts_plugin_export){
      .struct_size = SALTS_PLUGIN_EXPORT_SIZE,
      .kind = SALTS_PLUGIN_EXPORT_INTERFACE,
      .contract_version = TURBO_FLOW_CNET_RESOURCE_CONTRACT_VERSION,
      .capabilities = capabilities,
      .export_id = export_id,
      .contract_id = TURBO_FLOW_CNET_DEPLOYMENT_RESOURCE_CONTRACT_ID,
      .value.interface = {
          .desc = turbo_flow_cnet_deployment_resource_interface(),
          .value = &handles[index],
      },
  };
}

static void fixture_init(void) {
  fixture_root.stream.root = &fixture_root;
  fixture_root.stream.view =
      (turbo_flow_cnet_deployment_view_t)
          TURBO_FLOW_CNET_DEPLOYMENT_VIEW_INIT;
  fixture_root.stream.view.uri = "tcp://127.0.0.1:9";

  fixture_root.listener.root = &fixture_root;
  fixture_root.listener.view =
      (turbo_flow_cnet_deployment_view_t)
          TURBO_FLOW_CNET_DEPLOYMENT_VIEW_INIT;
  fixture_root.listener.view.bind_host = "127.0.0.1";
  fixture_root.listener.view.bind_port = 0u;

  fixture_root.datagram.root = &fixture_root;
  fixture_root.datagram.view =
      (turbo_flow_cnet_deployment_view_t)
          TURBO_FLOW_CNET_DEPLOYMENT_VIEW_INIT;
  fixture_root.datagram.view.bind_host = "127.0.0.1";
  fixture_root.datagram.view.bind_port = 0u;
  fixture_root.datagram.view.peer_host = "127.0.0.1";
  fixture_root.datagram.view.peer_port = 9u;

  fixture_root.packet_source.root = &fixture_root;
  fixture_root.packet_source.view =
      (turbo_flow_cnet_deployment_view_t)
          TURBO_FLOW_CNET_DEPLOYMENT_VIEW_INIT;
  fixture_root.packet_source.view.bind_host = "127.0.0.1";
  fixture_root.packet_source.view.bind_port = 0u;

  fixture_root.packet_sink.root = &fixture_root;
  fixture_root.packet_sink.view =
      (turbo_flow_cnet_deployment_view_t)
          TURBO_FLOW_CNET_DEPLOYMENT_VIEW_INIT;
  fixture_root.packet_sink.view.bind_host = "127.0.0.1";
  fixture_root.packet_sink.view.bind_port = 0u;
  fixture_root.packet_sink.view.peer_host = "127.0.0.1";
  fixture_root.packet_sink.view.peer_port = 9u;

  fixture_export(
      0u, "fixture.cnet.stream",
      TURBO_FLOW_CNET_RESOURCE_CLIENT_ENDPOINT |
          TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
          TURBO_FLOW_CNET_RESOURCE_SHAREABLE_SNAPSHOT,
      fixture_stream_impl_as_turbo_flow_cnet_deployment_resource(
          &fixture_root.stream));
  fixture_export(
      1u, "fixture.cnet.listener",
      TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
          TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
          TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
      fixture_listener_impl_as_turbo_flow_cnet_deployment_resource(
          &fixture_root.listener));
  fixture_export(
      2u, "fixture.cnet.datagram",
      TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
          TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT |
          TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
      fixture_datagram_impl_as_turbo_flow_cnet_deployment_resource(
          &fixture_root.datagram));
  fixture_export(
      3u, "fixture.cnet.packet_source",
      TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
          TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL |
          TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
      fixture_packet_source_impl_as_turbo_flow_cnet_deployment_resource(
          &fixture_root.packet_source));
  fixture_export(
      4u, "fixture.cnet.packet_sink",
      TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
          TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT |
          TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL |
          TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
      fixture_packet_sink_impl_as_turbo_flow_cnet_deployment_resource(
          &fixture_root.packet_sink));

  manifest.exports = exports;
  manifest.export_count = 5u;
}

static salts_plugin_status SALTS_PLUGIN_CALL fixture_start(void *self) {
  fixture_root_t *root = (fixture_root_t *)self;
  if (!root) return SALTS_PLUGIN_INVALID_ARGUMENT;
  root->stopping = false;
  root->started = true;
  return SALTS_PLUGIN_OK;
}

static salts_plugin_status SALTS_PLUGIN_CALL fixture_request_stop(void *self) {
  fixture_root_t *root = (fixture_root_t *)self;
  if (!root) return SALTS_PLUGIN_INVALID_ARGUMENT;
  root->stopping = true;
  root->started = false;
  return SALTS_PLUGIN_OK;
}

static bool SALTS_PLUGIN_CALL fixture_is_quiescent(const void *self) {
  const fixture_root_t *root = (const fixture_root_t *)self;
  return root && root->stopping;
}

static void SALTS_PLUGIN_CALL fixture_destroy(void *self) {
  fixture_root_t *root = (fixture_root_t *)self;
  if (!root) return;
  root->started = false;
  root->stopping = true;
}

SALTS_PLUGIN_QUERY_EXPORT
const salts_plugin_manifest *SALTS_PLUGIN_CALL
salts_plugin_query(uint32_t host_abi) {
  if (host_abi != SALTS_PLUGIN_ABI_VERSION) return NULL;
  salts_once(&fixture_once, fixture_init);
  manifest.start = fixture_start;
  manifest.request_stop = fixture_request_stop;
  manifest.is_quiescent = fixture_is_quiescent;
  manifest.destroy = fixture_destroy;
  return &manifest;
}
