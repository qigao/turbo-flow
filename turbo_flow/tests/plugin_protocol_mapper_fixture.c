#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_protocol_mapper.h"

#include <stdio.h>
#include <string.h>

#ifndef FLOW_PROTOCOL_MAPPER_FIXTURE_ID
#define FLOW_PROTOCOL_MAPPER_FIXTURE_ID "fixture.protocol-mapper.good"
#endif
#ifndef FLOW_PROTOCOL_MAPPER_NAME
#define FLOW_PROTOCOL_MAPPER_NAME "fixture.mapper"
#endif
#ifndef FLOW_PROTOCOL_MAPPER_PROFILE
#define FLOW_PROTOCOL_MAPPER_PROFILE "applicant-json"
#endif
#ifndef FLOW_PROTOCOL_MAPPER_MODE
#define FLOW_PROTOCOL_MAPPER_MODE 0
#endif

enum {
  FLOW_PROTOCOL_MAPPER_GOOD = 0,
  FLOW_PROTOCOL_MAPPER_DUPLICATE = 1,
  FLOW_PROTOCOL_MAPPER_MISSING_MAP = 2
};

typedef struct protocol_mapper_fixture_s {
  const turbo_flow_plugin_host_v1_t *host;
} protocol_mapper_fixture_t;

static int fixture_preflight(
    void *ctx, const turbo_flow_protocol_mapper_preflight_request_t *request,
    turbo_flow_protocol_mapper_contract_t *contract) {
  turbo_flow_content_descriptor_t content = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
  (void)ctx;
  if (!request || request->size != sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION ||
      !request->profile || !request->semantic_media_type ||
      !request->message_type || !request->semantic_type ||
      !request->max_semantic_bytes || !request->max_output_bytes ||
      !contract || contract->size != sizeof(*contract) ||
      contract->abi_version != TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION)
    return SALTS_EINVAL;
  if (turbo_flow_content_descriptor_init(
          &content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
          TURBO_FLOW_DATA_ENCODING_JSON, "application/json", "fixture.mapper") != SALTS_OK)
    return SALTS_EPROTO;
  if (turbo_flow_content_descriptor_declare_schema(
          &content, "rulesforge.Applicant.data", "Applicant", 1u) != SALTS_OK)
    return SALTS_EPROTO;
  *contract = (turbo_flow_protocol_mapper_contract_t)
      TURBO_FLOW_PROTOCOL_MAPPER_CONTRACT_INIT;
  contract->protocol = request->protocol;
  contract->message_type = request->message_type;
  contract->semantic_type = request->semantic_type;
  (void)snprintf(contract->profile, sizeof(contract->profile), "%s", request->profile);
  contract->max_semantic_bytes = request->max_semantic_bytes;
  contract->max_output_bytes = request->max_output_bytes;
  contract->content = content;
  return SALTS_OK;
}

static int fixture_map(
    void *ctx, const turbo_flow_protocol_mapper_request_t *request,
    turbo_flow_protocol_mapper_output_t *output) {
  (void)ctx;
  if (!request || request->size != sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION ||
      !request->semantic_data || !request->semantic_size ||
      !output || output->size != sizeof(*output) ||
      output->abi_version != TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION ||
      !output->payload || request->semantic_size > output->payload_capacity)
    return SALTS_EINVAL;
  memcpy(output->payload, request->semantic_data, request->semantic_size);
  output->payload_size = request->semantic_size;
  return SALTS_OK;
}

static int fixture_load(const turbo_flow_plugin_host_v1_t *host, void **out) {
  protocol_mapper_fixture_t *fixture;
  if (!host || host->size != sizeof(*host) ||
      host->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      host->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !host->allocate || !host->deallocate || !out)
    return SALTS_EINVAL;
  *out = NULL;
  fixture = (protocol_mapper_fixture_t *)host->allocate(host->ctx, sizeof(*fixture));
  if (!fixture) return SALTS_ENOMEM;
  fixture->host = host;
  *out = fixture;
  return SALTS_OK;
}

static int fixture_register(
    void *plugin, const turbo_flow_plugin_registration_v1_t *registration) {
  turbo_flow_protocol_mapper_v1_t mapper = TURBO_FLOW_PROTOCOL_MAPPER_V1_INIT;
  int rc;
  if (!plugin || !registration || registration->size != sizeof(*registration) ||
      registration->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !registration->add_protocol_mapper)
    return SALTS_EINVAL;
  mapper.name = FLOW_PROTOCOL_MAPPER_NAME;
  mapper.protocol = TURBO_FLOW_PROTOCOL_COAP;
  mapper.profile = FLOW_PROTOCOL_MAPPER_PROFILE;
  mapper.max_semantic_bytes = 1024u;
  mapper.max_output_bytes = 1024u;
  mapper.ctx = plugin;
  mapper.preflight = fixture_preflight;
  mapper.map = FLOW_PROTOCOL_MAPPER_MODE == FLOW_PROTOCOL_MAPPER_MISSING_MAP
                   ? NULL
                   : fixture_map;
  rc = registration->add_protocol_mapper(registration->ctx, &mapper);
  if (FLOW_PROTOCOL_MAPPER_MODE == FLOW_PROTOCOL_MAPPER_DUPLICATE && rc == SALTS_OK)
    rc = registration->add_protocol_mapper(registration->ctx, &mapper);
  return rc;
}

static int fixture_quiesce(void *plugin, uint64_t timeout_ms) {
  (void)timeout_ms;
  return plugin ? SALTS_OK : SALTS_EINVAL;
}
static int fixture_shutdown(void *plugin) { return plugin ? SALTS_OK : SALTS_EINVAL; }
static void fixture_destroy(void *plugin) {
  protocol_mapper_fixture_t *fixture = (protocol_mapper_fixture_t *)plugin;
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
    FLOW_PROTOCOL_MAPPER_FIXTURE_ID,
    "1.0.0",
    TURBO_FLOW_PLUGIN_CAP_PROTOCOL_MAPPER,
    fixture_load,
    fixture_register,
    fixture_quiesce,
    fixture_shutdown,
    fixture_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *
turbo_flow_plugin_get_api(void) {
  return &fixture_api;
}
