#include "flowie_control_external_authenticator_internal.h"

#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

struct flowie_control_external_subject_mapper_s {
  flowie_control_external_identity_mapper_t interface;
  char trusted_issuer[FLOWIE_CONTROL_EXTERNAL_ISSUER_MAX + 1u];
  char subject_type[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
};

static int external_auth_text_valid(const char *value, size_t limit) {
  size_t length;
  if (!value || limit == 0u) return 0;
  length = strnlen(value, limit + 1u);
  if (length == 0u || length > limit) return 0;
  for (size_t index = 0u; index < length; ++index) {
    unsigned char byte = (unsigned char)value[index];
    if (byte < 0x20u || byte == 0x7fu) return 0;
  }
  return 1;
}

int flowie_control_external_authenticator_validate(
    const flowie_control_external_authenticator_t *authenticator) {
  if (!authenticator || authenticator->size < sizeof(*authenticator) ||
      authenticator->version != FLOWIE_CONTROL_EXTERNAL_AUTHENTICATOR_VERSION ||
      (authenticator->capabilities & FLOWIE_CONTROL_EXTERNAL_AUTH_REQUIRED_CAPABILITIES) !=
          FLOWIE_CONTROL_EXTERNAL_AUTH_REQUIRED_CAPABILITIES ||
      !external_auth_text_valid(authenticator->method, TURBO_FLOW_SECURITY_TYPE_MAX) ||
      !authenticator->verify)
    return TURBO_EINVAL;
  return TURBO_OK;
}

int flowie_control_external_identity_mapper_validate(
    const flowie_control_external_identity_mapper_t *mapper) {
  if (!mapper || mapper->size < sizeof(*mapper) ||
      mapper->version != FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAPPER_VERSION || !mapper->map)
    return TURBO_EINVAL;
  return TURBO_OK;
}

int flowie_control_external_auth_assertion_validate(
    const flowie_control_external_auth_assertion_t *assertion, const char *expected_method,
    uint64_t now) {
  if (!assertion || assertion->size < sizeof(*assertion) ||
      !external_auth_text_valid(expected_method, TURBO_FLOW_SECURITY_TYPE_MAX) ||
      !external_auth_text_valid(assertion->issuer, FLOWIE_CONTROL_EXTERNAL_ISSUER_MAX) ||
      !external_auth_text_valid(assertion->subject, TURBO_FLOW_SECURITY_ID_MAX) ||
      !external_auth_text_valid(assertion->subject_type, TURBO_FLOW_SECURITY_TYPE_MAX) ||
      !external_auth_text_valid(assertion->auth_method, TURBO_FLOW_SECURITY_TYPE_MAX) ||
      strcmp(assertion->auth_method, expected_method) != 0 || assertion->issued_at == 0u ||
      assertion->issued_at > now || assertion->expires_at <= assertion->issued_at ||
      assertion->expires_at <= now || assertion->revision == 0u ||
      assertion->account_enabled != 1 ||
      assertion->assurance_level < FLOWIE_CONTROL_EXTERNAL_ASSURANCE_SINGLE_FACTOR ||
      assertion->assurance_level > FLOWIE_CONTROL_EXTERNAL_ASSURANCE_HARDWARE_BOUND ||
      assertion->external_group_count > TURBO_FLOW_SECURITY_MAX_GROUPS)
    return TURBO_EINVAL;

  for (uint32_t index = 0u; index < assertion->external_group_count; ++index) {
    if (!external_auth_text_valid(assertion->external_groups[index], TURBO_FLOW_SECURITY_ID_MAX))
      return TURBO_EINVAL;
    for (uint32_t previous = 0u; previous < index; ++previous)
      if (strcmp(assertion->external_groups[previous], assertion->external_groups[index]) == 0)
        return TURBO_EINVAL;
  }
  return TURBO_OK;
}

int flowie_control_external_identity_map_result_validate(
    const flowie_control_external_identity_map_result_t *result) {
  if (!result || result->size < sizeof(*result) ||
      !external_auth_text_valid(result->principal_id, TURBO_FLOW_SECURITY_ID_MAX))
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int external_subject_map(void *ctx,
                                const flowie_control_external_identity_map_request_t *request,
                                flowie_control_external_identity_map_result_t *result_out) {
  flowie_control_external_subject_mapper_t *mapper =
      (flowie_control_external_subject_mapper_t *)ctx;
  flowie_control_external_identity_map_result_t result =
      FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAP_RESULT_INIT;
  size_t subject_size;
  if (result_out && result_out->size >= sizeof(*result_out)) *result_out = result;
  if (!mapper || !request || request->size < sizeof(*request) ||
      !external_auth_text_valid(request->domain_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !external_auth_text_valid(request->presented_identity, TURBO_FLOW_SECURITY_ID_MAX) ||
      !request->assertion || request->assertion->size < sizeof(*request->assertion) ||
      !result_out || result_out->size < sizeof(*result_out))
    return TURBO_EINVAL;
  if (!external_auth_text_valid(request->assertion->issuer, FLOWIE_CONTROL_EXTERNAL_ISSUER_MAX) ||
      !external_auth_text_valid(request->assertion->subject_type, TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EPROTO;
  if (strcmp(request->assertion->issuer, mapper->trusted_issuer) != 0 ||
      strcmp(request->assertion->subject_type, mapper->subject_type) != 0)
    return TURBO_EPERM;
  if (!external_auth_text_valid(request->assertion->subject, TURBO_FLOW_SECURITY_ID_MAX))
    return TURBO_EPROTO;
  subject_size = strlen(request->assertion->subject);
  memcpy(result.principal_id, request->assertion->subject, subject_size + 1u);
  *result_out = result;
  return TURBO_OK;
}

int flowie_control_external_subject_mapper_create(
    const flowie_control_external_subject_mapper_config_t *config,
    flowie_control_external_subject_mapper_t **out) {
  flowie_control_external_subject_mapper_t *mapper;
  size_t issuer_size;
  size_t type_size;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !out ||
      !external_auth_text_valid(config->trusted_issuer, FLOWIE_CONTROL_EXTERNAL_ISSUER_MAX) ||
      !external_auth_text_valid(config->subject_type, TURBO_FLOW_SECURITY_TYPE_MAX))
    return TURBO_EINVAL;
  mapper = (flowie_control_external_subject_mapper_t *)calloc(1u, sizeof(*mapper));
  if (!mapper) return TURBO_ENOMEM;
  issuer_size = strlen(config->trusted_issuer);
  type_size = strlen(config->subject_type);
  memcpy(mapper->trusted_issuer, config->trusted_issuer, issuer_size + 1u);
  memcpy(mapper->subject_type, config->subject_type, type_size + 1u);
  mapper->interface =
      (flowie_control_external_identity_mapper_t)FLOWIE_CONTROL_EXTERNAL_IDENTITY_MAPPER_INIT;
  mapper->interface.ctx = mapper;
  mapper->interface.map = external_subject_map;
  *out = mapper;
  return TURBO_OK;
}

void flowie_control_external_subject_mapper_destroy(
    flowie_control_external_subject_mapper_t *mapper) {
  if (!mapper) return;
  memset(mapper, 0, sizeof(*mapper));
  free(mapper);
}

const flowie_control_external_identity_mapper_t *flowie_control_external_subject_mapper_interface(
    const flowie_control_external_subject_mapper_t *mapper) {
  return mapper ? &mapper->interface : NULL;
}
