#include "turbo_flow_applicant_mapper_plugin.h"

#include "json_parser.h"
#include "salts_error.h"
#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_protocol_mapper.h"

#include <limits.h>
#include <stdint.h>
#include <string.h>

typedef struct applicant_mapper_plugin_s {
  const turbo_flow_plugin_host_v1_t *host;
} applicant_mapper_plugin_t;

static int applicant_mapper_content(turbo_flow_content_descriptor_t *content) {
  int rc;
  if (!content) return SALTS_EINVAL;
  *content = (turbo_flow_content_descriptor_t)TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
  rc = turbo_flow_content_descriptor_init(
      content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
      TURBO_FLOW_DATA_ENCODING_JSON, "application/json", "applicant-json");
  if (rc != SALTS_OK) return rc;
  return turbo_flow_content_descriptor_declare_schema(
      content, TURBO_FLOW_APPLICANT_SCHEMA_ID, TURBO_FLOW_APPLICANT_TYPE_NAME,
      TURBO_FLOW_APPLICANT_SCHEMA_VERSION);
}

static int applicant_mapper_tuple_valid(
    turbo_flow_protocol_kind_t protocol, uint32_t message_type,
    uint32_t semantic_type, const char *media_type) {
  if (!media_type || strcmp(media_type, "application/json") != 0) return 0;
  if (protocol == TURBO_FLOW_PROTOCOL_COAP)
    return message_type == TURBO_FLOW_APPLICANT_COAP_MESSAGE_TYPE &&
           semantic_type == TURBO_FLOW_APPLICANT_COAP_SEMANTIC_TYPE;
  if (protocol == TURBO_FLOW_PROTOCOL_JTT_808)
    return message_type == TURBO_FLOW_APPLICANT_JTT808_MESSAGE_TYPE &&
           semantic_type == TURBO_FLOW_APPLICANT_JTT808_SEMANTIC_TYPE;
  return 0;
}

static int applicant_age_token_valid(const char *token, size_t size) {
  uint64_t value = 0u;
  if (!token || size == 0u) return 0;
  for (size_t i = 0u; i < size; ++i) {
    const unsigned char ch = (unsigned char)token[i];
    if (ch < (unsigned char)'0' || ch > (unsigned char)'9') return 0;
    if (value > (uint64_t)INT_MAX / 10u) return 0;
    value = value * 10u + (uint64_t)(ch - (unsigned char)'0');
    if (value > (uint64_t)INT_MAX) return 0;
  }
  return 1;
}

static int applicant_json_valid(const uint8_t *data, size_t size) {
  json_value_t *root;
  json_value_t *age;
  const char *number;
  size_t number_size = 0u;
  int valid;
  if (!data || size == 0u) return 0;
  root = json_parse((const char *)data, size);
  if (!root) return 0;
  valid = json_type(root) == JSON_OBJECT && json_object_size(root) == 1u;
  age = valid ? json_object_get(root, "age") : NULL;
  if (!age || json_type(age) != JSON_NUMBER) valid = 0;
  number = valid ? json_number_text(age, &number_size) : NULL;
  if (!applicant_age_token_valid(number, number_size)) valid = 0;
  json_free(root);
  return valid;
}

static int applicant_mapper_preflight(
    void *ctx, const turbo_flow_protocol_mapper_preflight_request_t *request,
    turbo_flow_protocol_mapper_contract_t *contract) {
  int rc;
  (void)ctx;
  if (!request || request->size != sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION ||
      !request->profile ||
      strcmp(request->profile, TURBO_FLOW_APPLICANT_MAPPER_PROFILE) != 0 ||
      !request->semantic_media_type ||
      request->max_semantic_bytes == 0u || request->max_output_bytes == 0u ||
      request->max_semantic_bytes > TURBO_FLOW_APPLICANT_MAPPER_MAX_BYTES ||
      request->max_output_bytes > TURBO_FLOW_APPLICANT_MAPPER_MAX_BYTES ||
      !contract || contract->size != sizeof(*contract) ||
      contract->abi_version != TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION)
    return SALTS_EINVAL;
  if (!applicant_mapper_tuple_valid(
          request->protocol, request->message_type, request->semantic_type,
          request->semantic_media_type))
    return SALTS_ENOTSUP;

  *contract =
      (turbo_flow_protocol_mapper_contract_t)TURBO_FLOW_PROTOCOL_MAPPER_CONTRACT_INIT;
  contract->protocol = request->protocol;
  contract->message_type = request->message_type;
  contract->semantic_type = request->semantic_type;
  memcpy(contract->profile, TURBO_FLOW_APPLICANT_MAPPER_PROFILE,
         sizeof(TURBO_FLOW_APPLICANT_MAPPER_PROFILE));
  contract->max_semantic_bytes = request->max_semantic_bytes;
  contract->max_output_bytes = request->max_output_bytes;
  rc = applicant_mapper_content(&contract->content);
  return rc;
}

static int applicant_mapper_map(
    void *ctx, const turbo_flow_protocol_mapper_request_t *request,
    turbo_flow_protocol_mapper_output_t *output) {
  int rc;
  (void)ctx;
  if (!request || request->size != sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION ||
      !request->profile ||
      strcmp(request->profile, TURBO_FLOW_APPLICANT_MAPPER_PROFILE) != 0 ||
      !applicant_mapper_tuple_valid(
          request->protocol, request->metadata.message_type,
          request->semantic_type, request->semantic_media_type) ||
      !request->semantic_data || request->semantic_size == 0u ||
      request->semantic_size > TURBO_FLOW_APPLICANT_MAPPER_MAX_BYTES ||
      !output || output->size != sizeof(*output) ||
      output->abi_version != TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION ||
      !output->payload)
    return SALTS_EINVAL;
  if (request->semantic_size > output->payload_capacity)
    return SALTS_EMSGSIZE;
  if (!applicant_json_valid(request->semantic_data, request->semantic_size))
    return SALTS_EPROTO;

  memmove(output->payload, request->semantic_data, request->semantic_size);
  output->payload_size = request->semantic_size;
  rc = applicant_mapper_content(&output->content);
  if (rc != SALTS_OK) output->payload_size = 0u;
  return rc;
}

static int applicant_mapper_load(const turbo_flow_plugin_host_v1_t *host,
                                 void **plugin_out) {
  applicant_mapper_plugin_t *plugin;
  if (!plugin_out) return SALTS_EINVAL;
  *plugin_out = NULL;
  if (!host || host->size != sizeof(*host) ||
      host->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      host->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !host->allocate || !host->deallocate)
    return SALTS_EINVAL;
  plugin = (applicant_mapper_plugin_t *)host->allocate(host->ctx, sizeof(*plugin));
  if (!plugin) return SALTS_ENOMEM;
  plugin->host = host;
  *plugin_out = plugin;
  return SALTS_OK;
}

static int applicant_mapper_register(
    void *plugin_ptr, const turbo_flow_plugin_registration_v1_t *registration) {
  turbo_flow_protocol_mapper_v1_t mapper = TURBO_FLOW_PROTOCOL_MAPPER_V1_INIT;
  int rc;
  if (!plugin_ptr || !registration || registration->size != sizeof(*registration) ||
      registration->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !registration->add_protocol_mapper)
    return SALTS_EINVAL;

  mapper.name = TURBO_FLOW_APPLICANT_MAPPER_NAME;
  mapper.profile = TURBO_FLOW_APPLICANT_MAPPER_PROFILE;
  mapper.max_semantic_bytes = TURBO_FLOW_APPLICANT_MAPPER_MAX_BYTES;
  mapper.max_output_bytes = TURBO_FLOW_APPLICANT_MAPPER_MAX_BYTES;
  mapper.ctx = plugin_ptr;
  mapper.preflight = applicant_mapper_preflight;
  mapper.map = applicant_mapper_map;

  mapper.protocol = TURBO_FLOW_PROTOCOL_COAP;
  rc = registration->add_protocol_mapper(registration->ctx, &mapper);
  if (rc != SALTS_OK) return rc;

  mapper.protocol = TURBO_FLOW_PROTOCOL_JTT_808;
  return registration->add_protocol_mapper(registration->ctx, &mapper);
}

static int applicant_mapper_quiesce(void *plugin, uint64_t timeout_ms) {
  (void)timeout_ms;
  return plugin ? SALTS_OK : SALTS_EINVAL;
}

static int applicant_mapper_shutdown(void *plugin) {
  return plugin ? SALTS_OK : SALTS_EINVAL;
}

static void applicant_mapper_destroy(void *value) {
  applicant_mapper_plugin_t *plugin = (applicant_mapper_plugin_t *)value;
  const turbo_flow_plugin_host_v1_t *host;
  if (!plugin) return;
  host = plugin->host;
  plugin->host = NULL;
  host->deallocate(host->ctx, plugin);
}

static const turbo_flow_plugin_api_v1_t APPLICANT_MAPPER_API = {
    sizeof(turbo_flow_plugin_api_v1_t),
    TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
    TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
    TURBO_FLOW_APPLICANT_MAPPER_PLUGIN_ID,
    TURBO_FLOW_APPLICANT_MAPPER_PLUGIN_VERSION,
    TURBO_FLOW_PLUGIN_CAP_PROTOCOL_MAPPER,
    applicant_mapper_load,
    applicant_mapper_register,
    applicant_mapper_quiesce,
    applicant_mapper_shutdown,
    applicant_mapper_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &APPLICANT_MAPPER_API;
}
