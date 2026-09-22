#include "turbo_flow_plugin_protocol.h"
#include "turbo_flow_protocol_plugin.h"

#include <string.h>

typedef struct semantic_coap_fixture_s {
  const turbo_flow_plugin_host_v1_t *host;
} semantic_coap_fixture_t;

static int fixture_inspect(void *ctx, const char *configured_version,
                           const turbo_flow_protocol_frame_view_t *frame,
                           turbo_flow_protocol_metadata_t *metadata) {
  (void)ctx;
  (void)configured_version;
  if (!frame || !frame->data || frame->data_size < 4u || !metadata) return SALTS_EINVAL;
  if (frame->data[0] != 0x40u) return SALTS_EPROTO;
  metadata->message_type = frame->data[1];
  metadata->sequence = ((uint64_t)frame->data[2] << 8u) | frame->data[3];
  memcpy(metadata->operation, "post", sizeof("post"));
  return SALTS_OK;
}

static int fixture_decode_semantic(
    void *ctx, const char *configured_version,
    const turbo_flow_protocol_frame_view_t *frame,
    turbo_flow_protocol_metadata_t *metadata,
    turbo_flow_protocol_semantic_output_t *output) {
  static const uint8_t canonical[] = "{\"age\":21}";
  (void)ctx;
  (void)configured_version;
  if (!frame || !frame->data || frame->data_size < 4u || !metadata || !output ||
      output->size != sizeof(*output) ||
      output->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      !output->data || output->capacity < sizeof(canonical) - 1u)
    return SALTS_EINVAL;
  if (frame->data[0] != 0x40u) return SALTS_EPROTO;
  metadata->message_type = frame->data[1];
  metadata->sequence = ((uint64_t)frame->data[2] << 8u) | frame->data[3];
  memcpy(metadata->operation, "post", sizeof("post"));
  memcpy(output->data, canonical, sizeof(canonical) - 1u);
  output->data_size = sizeof(canonical) - 1u;
  output->semantic_type = 50u;
  memcpy(output->media_type, "application/json", sizeof("application/json"));
  return SALTS_OK;
}

static int fixture_protocol_open(void *ctx,
                                 const turbo_flow_protocol_open_request_t *request,
                                 turbo_flow_protocol_service_t *service) {
  turbo_flow_protocol_codec_ops_t ops = TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT;
  turbo_flow_protocol_t *protocol = NULL;
  int rc;
  if (!ctx || !request || !service || request->protocol != TURBO_FLOW_PROTOCOL_COAP)
    return SALTS_EINVAL;
  ops.inspect = fixture_inspect;
  ops.decode_semantic = fixture_decode_semantic;
  rc = turbo_flow_protocol_create(
      request, "coap-semantic-fixture", "RFC7252",
      TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
          TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE |
          TURBO_FLOW_PROTOCOL_CAP_SEMANTIC_DECODE,
      &ops, ctx, &protocol);
  if (rc != SALTS_OK) return rc;
  service->protocol = TURBO_FLOW_PROTOCOL_COAP;
  service->instance = protocol;
  service->owner = protocol;
  return SALTS_OK;
}

static void fixture_protocol_close(void *ctx, turbo_flow_protocol_service_t *service) {
  (void)ctx;
  if (!service) return;
  turbo_flow_protocol_destroy((turbo_flow_protocol_t *)service->owner);
  service->instance = NULL;
  service->owner = NULL;
}

static int fixture_load(const turbo_flow_plugin_host_v1_t *host, void **out) {
  semantic_coap_fixture_t *fixture;
  if (out) *out = NULL;
  if (!host || host->size != sizeof(*host) ||
      host->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      host->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !host->allocate || !host->deallocate || !out)
    return SALTS_EINVAL;
  fixture = (semantic_coap_fixture_t *)host->allocate(host->ctx, sizeof(*fixture));
  if (!fixture) return SALTS_ENOMEM;
  fixture->host = host;
  *out = fixture;
  return SALTS_OK;
}

static int fixture_register(void *plugin,
                            const turbo_flow_plugin_registration_v1_t *registration) {
  turbo_flow_plugin_protocol_provider_v1_t provider =
      TURBO_FLOW_PLUGIN_PROTOCOL_PROVIDER_V1_INIT;
  if (!plugin || !registration || registration->size != sizeof(*registration) ||
      registration->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !registration->add_protocol_provider)
    return SALTS_EINVAL;
  provider.provider.size = sizeof(provider.provider);
  provider.provider.version_major = TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MAJOR;
  provider.provider.version_minor = TURBO_FLOW_PROTOCOL_PLUGIN_API_VERSION_MINOR;
  provider.provider.name = "coap-semantic-fixture";
  provider.provider.protocol = TURBO_FLOW_PROTOCOL_COAP;
  provider.provider.capabilities =
      TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
      TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE | TURBO_FLOW_PROTOCOL_CAP_SEMANTIC_DECODE;
  provider.provider.ctx = plugin;
  provider.provider.open = fixture_protocol_open;
  provider.provider.close = fixture_protocol_close;
  return registration->add_protocol_provider(registration->ctx, &provider);
}

static int fixture_quiesce(void *plugin, uint64_t timeout_ms) {
  (void)timeout_ms;
  return plugin ? SALTS_OK : SALTS_EINVAL;
}
static int fixture_shutdown(void *plugin) { return plugin ? SALTS_OK : SALTS_EINVAL; }
static void fixture_destroy(void *value) {
  semantic_coap_fixture_t *fixture = (semantic_coap_fixture_t *)value;
  const turbo_flow_plugin_host_v1_t *host;
  if (!fixture) return;
  host = fixture->host;
  memset(fixture, 0, sizeof(*fixture));
  host->deallocate(host->ctx, fixture);
}

static const turbo_flow_plugin_api_v1_t fixture_api = {
    sizeof(turbo_flow_plugin_api_v1_t),
    TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
    TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
    "fixture.semantic-coap",
    "1.0.0",
    TURBO_FLOW_PLUGIN_CAP_PROTOCOL,
    fixture_load,
    fixture_register,
    fixture_quiesce,
    fixture_shutdown,
    fixture_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &fixture_api;
}
