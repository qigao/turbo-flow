#include "turbo_flow_security.h"

#include "turbo_error.h"
#include "turbo_parser.h"
#include "turbo_str.h"
#include "turbo_vec.h"

#include <math.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_security_realm_s {
  tstr_t resource_uid;
  tstr_t owner_name;
  uint64_t policy_version;
  turbo_vec_t rules;
  turbo_flow_security_matcher_t matcher;
  atomic_uint_fast64_t evaluations;
  atomic_uint_fast64_t allowed;
  atomic_uint_fast64_t denied;
  atomic_uint_fast64_t failures;
  atomic_int last_status;
};

static const char FLOW_SECURITY_STATUS_SCHEMA_TEXT[] =
    "schema TurboFlowSecurityResource [id(1101), version(1)];\n"
    "message SecurityRealmStatus {\n"
    "  string policy_version;\n"
    "  string rule_count;\n"
    "  string evaluations;\n"
    "  string allowed;\n"
    "  string denied;\n"
    "  string failures;\n"
    "  int32 last_status;\n"
    "}\n";

static const turbo_flow_resource_schema_t FLOW_SECURITY_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_RULES,
    TURBO_FLOW_RESOURCE_SECURITY_REALM,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowSecurityResource",
    "SecurityRealmStatus",
    1101u,
    1u,
    FLOW_SECURITY_STATUS_SCHEMA_TEXT};

static int flow_security_cstr_valid(const char *value, size_t capacity, int required) {
  const char *end;
  if (!value || capacity == 0u) return 0;
  end = (const char *)memchr(value, '\0', capacity);
  return end && (!required || end != value);
}

static int flow_security_principal_valid(const turbo_flow_security_principal_t *principal) {
  if (!principal || principal->size < sizeof(*principal) ||
      principal->abi_version != TURBO_FLOW_SECURITY_ABI_V1 ||
      !flow_security_cstr_valid(principal->principal_id, sizeof(principal->principal_id), 1) ||
      !flow_security_cstr_valid(principal->principal_type, sizeof(principal->principal_type), 1) ||
      !flow_security_cstr_valid(principal->tenant_id, sizeof(principal->tenant_id), 0) ||
      !flow_security_cstr_valid(principal->auth_method, sizeof(principal->auth_method), 1) ||
      principal->scope < TURBO_FLOW_SECURITY_SCOPE_SELF ||
      principal->scope > TURBO_FLOW_SECURITY_SCOPE_SYSTEM ||
      principal->role_count > TURBO_FLOW_SECURITY_MAX_ROLES ||
      principal->group_count > TURBO_FLOW_SECURITY_MAX_GROUPS || principal->policy_version == 0u ||
      (principal->scope == TURBO_FLOW_SECURITY_SCOPE_GROUP && principal->group_count == 0u)) {
    return 0;
  }
  if (principal->scope != TURBO_FLOW_SECURITY_SCOPE_SYSTEM && principal->tenant_id[0] == '\0') {
    return 0;
  }
  for (uint32_t i = 0u; i < principal->role_count; ++i) {
    if (!flow_security_cstr_valid(principal->roles[i], sizeof(principal->roles[i]), 1)) return 0;
  }
  for (uint32_t i = 0u; i < principal->group_count; ++i) {
    if (!flow_security_cstr_valid(principal->groups[i], sizeof(principal->groups[i]), 1)) return 0;
  }
  return 1;
}

int turbo_flow_security_authenticate(const turbo_flow_security_auth_provider_t *provider,
                                     const turbo_flow_security_auth_request_t *request,
                                     turbo_flow_security_principal_t *principal_out) {
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  int rc;
  if (!provider || provider->size < sizeof(*provider) || !provider->authenticate || !request ||
      request->size < sizeof(*request) || !request->identity || !request->identity[0] ||
      !request->method || !request->method[0] || (request->secret_size > 0u && !request->secret) ||
      !principal_out || principal_out->size < sizeof(*principal_out)) {
    return TURBO_EINVAL;
  }
  rc = provider->authenticate(provider->ctx, request, &principal);
  if (rc != TURBO_OK) return rc;
  if (!flow_security_principal_valid(&principal)) return TURBO_EPROTO;
  *principal_out = principal;
  return TURBO_OK;
}

static int flow_security_rule_valid(const turbo_flow_security_rule_t *rule,
                                    const turbo_flow_security_matcher_t *matcher) {
  if (!rule || rule->size < sizeof(*rule) || rule->abi_version != TURBO_FLOW_SECURITY_ABI_V1 ||
      rule->effect < TURBO_FLOW_SECURITY_DENY || rule->effect > TURBO_FLOW_SECURITY_ALLOW ||
      rule->subject_kind < TURBO_FLOW_SECURITY_SUBJECT_ANY ||
      rule->subject_kind > TURBO_FLOW_SECURITY_SUBJECT_GROUP ||
      !flow_security_cstr_valid(rule->subject, sizeof(rule->subject), 0) ||
      !flow_security_cstr_valid(rule->tenant_id, sizeof(rule->tenant_id), 0) ||
      rule->action_mask == 0u || (rule->action_mask & ~TURBO_FLOW_SECURITY_ACTION_ALL) != 0u ||
      rule->resource_type < TURBO_FLOW_SECURITY_RESOURCE_GENERIC ||
      rule->resource_type > TURBO_FLOW_SECURITY_RESOURCE_SECRET ||
      rule->match_kind < TURBO_FLOW_SECURITY_MATCH_EXACT ||
      rule->match_kind > TURBO_FLOW_SECURITY_MATCH_ADAPTER ||
      !flow_security_cstr_valid(rule->pattern, sizeof(rule->pattern), 1)) {
    return 0;
  }
  if (rule->subject_kind == TURBO_FLOW_SECURITY_SUBJECT_ANY) {
    if (rule->subject[0] != '\0') return 0;
  } else if (rule->subject[0] == '\0') {
    return 0;
  }
  return rule->match_kind != TURBO_FLOW_SECURITY_MATCH_ADAPTER ||
         (matcher && matcher->size >= sizeof(*matcher) && matcher->match);
}

static int flow_security_realm_config_valid(const turbo_flow_security_realm_config_t *config) {
  if (!config || config->size < sizeof(*config) ||
      config->abi_version != TURBO_FLOW_SECURITY_ABI_V1 || !config->resource_uid ||
      !config->resource_uid[0] || strlen(config->resource_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      !config->owner_name || !config->owner_name[0] ||
      strlen(config->owner_name) > TURBO_FLOW_RESOURCE_OWNER_MAX || config->policy_version == 0u ||
      !config->rules || config->rule_count == 0u ||
      config->rule_count > TURBO_FLOW_SECURITY_MAX_RULES) {
    return 0;
  }
  if (config->matcher.size != 0u && config->matcher.size < sizeof(config->matcher)) return 0;
  for (size_t i = 0u; i < config->rule_count; ++i) {
    if (!flow_security_rule_valid(&config->rules[i], &config->matcher)) return 0;
  }
  return 1;
}

int turbo_flow_security_realm_create(const turbo_flow_security_realm_config_t *config,
                                     turbo_flow_security_realm_t **out) {
  turbo_flow_security_realm_t *realm;
  int rc;
  if (out) *out = NULL;
  if (!out || !flow_security_realm_config_valid(config)) return TURBO_EINVAL;
  realm = (turbo_flow_security_realm_t *)calloc(1u, sizeof(*realm));
  if (!realm) return TURBO_ENOMEM;
  rc = turbo_vec_init(&realm->rules, sizeof(turbo_flow_security_rule_t));
  if (rc != TURBO_OK) {
    free(realm);
    return rc;
  }
  realm->resource_uid = tstr_dup(config->resource_uid);
  realm->owner_name = tstr_dup(config->owner_name);
  realm->policy_version = config->policy_version;
  realm->matcher = config->matcher;
  if (!realm->resource_uid || !realm->owner_name) {
    turbo_flow_security_realm_destroy(realm);
    return TURBO_ENOMEM;
  }
  rc = turbo_vec_reserve(&realm->rules, config->rule_count);
  for (size_t i = 0u; rc == TURBO_OK && i < config->rule_count; ++i) {
    rc = turbo_vec_push(&realm->rules, &config->rules[i]);
  }
  if (rc != TURBO_OK) {
    turbo_flow_security_realm_destroy(realm);
    return rc;
  }
  atomic_init(&realm->evaluations, 0u);
  atomic_init(&realm->allowed, 0u);
  atomic_init(&realm->denied, 0u);
  atomic_init(&realm->failures, 0u);
  atomic_init(&realm->last_status, TURBO_OK);
  *out = realm;
  return TURBO_OK;
}

void turbo_flow_security_realm_destroy(turbo_flow_security_realm_t *realm) {
  if (!realm) return;
  turbo_vec_destroy(&realm->rules);
  tstr_freep(&realm->resource_uid);
  tstr_freep(&realm->owner_name);
  free(realm);
}

static int flow_security_subject_matches(const turbo_flow_security_rule_t *rule,
                                         const turbo_flow_security_principal_t *principal) {
  if (rule->subject_kind == TURBO_FLOW_SECURITY_SUBJECT_ANY) return 1;
  if (rule->subject_kind == TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL)
    return strcmp(rule->subject, principal->principal_id) == 0;
  if (rule->subject_kind == TURBO_FLOW_SECURITY_SUBJECT_ROLE) {
    for (uint32_t i = 0u; i < principal->role_count; ++i) {
      if (strcmp(rule->subject, principal->roles[i]) == 0) return 1;
    }
    return 0;
  }
  for (uint32_t i = 0u; i < principal->group_count; ++i) {
    if (strcmp(rule->subject, principal->groups[i]) == 0) return 1;
  }
  return 0;
}

static int flow_security_resource_matches(turbo_flow_security_realm_t *realm,
                                          const turbo_flow_security_rule_t *rule,
                                          const turbo_flow_security_request_t *request,
                                          int *matched) {
  size_t pattern_size;
  *matched = 0;
  if (rule->match_kind == TURBO_FLOW_SECURITY_MATCH_EXACT) {
    *matched = strcmp(rule->pattern, request->resource) == 0;
    return TURBO_OK;
  }
  if (rule->match_kind == TURBO_FLOW_SECURITY_MATCH_PREFIX) {
    pattern_size = strlen(rule->pattern);
    *matched = strncmp(rule->pattern, request->resource, pattern_size) == 0;
    return TURBO_OK;
  }
  if (!realm->matcher.match) return TURBO_ENOTSUP;
  {
    int rc = realm->matcher.match(realm->matcher.ctx, rule, request, matched);
    if (rc != TURBO_OK) return rc;
    return (*matched == 0 || *matched == 1) ? TURBO_OK : TURBO_EPROTO;
  }
}

static void flow_security_record(turbo_flow_security_realm_t *realm, int status,
                                 const turbo_flow_security_decision_t *decision) {
  atomic_fetch_add_explicit(&realm->evaluations, 1u, memory_order_relaxed);
  if (status != TURBO_OK) {
    atomic_fetch_add_explicit(&realm->failures, 1u, memory_order_relaxed);
  } else if (decision->effect == TURBO_FLOW_SECURITY_ALLOW) {
    atomic_fetch_add_explicit(&realm->allowed, 1u, memory_order_relaxed);
  } else {
    atomic_fetch_add_explicit(&realm->denied, 1u, memory_order_relaxed);
  }
  atomic_store_explicit(&realm->last_status, status, memory_order_release);
}

int turbo_flow_security_realm_evaluate(turbo_flow_security_realm_t *realm,
                                       const turbo_flow_security_request_t *request,
                                       uint64_t now_epoch_seconds,
                                       turbo_flow_security_decision_t *decision) {
  turbo_flow_security_decision_t result = TURBO_FLOW_SECURITY_DECISION_INIT;
  size_t allow_rule = SIZE_MAX;
  int rc = TURBO_OK;
  if (!realm || !request || request->size < sizeof(*request) ||
      !flow_security_principal_valid(request->principal) || !request->tenant_id ||
      !request->resource || !request->resource[0] || request->action == 0u ||
      (request->action & (request->action - 1u)) != 0u ||
      (request->action & ~TURBO_FLOW_SECURITY_ACTION_ALL) != 0u ||
      request->resource_type < TURBO_FLOW_SECURITY_RESOURCE_GENERIC ||
      request->resource_type > TURBO_FLOW_SECURITY_RESOURCE_SECRET || !decision ||
      decision->size < sizeof(*decision)) {
    return TURBO_EINVAL;
  }
  result.policy_version = realm->policy_version;
  if (request->principal->scope != TURBO_FLOW_SECURITY_SCOPE_SYSTEM &&
      (request->tenant_id[0] == '\0' ||
       strcmp(request->principal->tenant_id, request->tenant_id) != 0)) {
    result.reason = TURBO_FLOW_SECURITY_REASON_TENANT_MISMATCH;
    goto complete;
  }
  if (request->principal->expires_at != 0u) {
    if (now_epoch_seconds == 0u) {
      rc = TURBO_EINVAL;
      goto complete;
    }
    if (now_epoch_seconds >= request->principal->expires_at) {
      result.reason = TURBO_FLOW_SECURITY_REASON_PRINCIPAL_EXPIRED;
      goto complete;
    }
  }
  if (request->principal->policy_version != realm->policy_version) {
    result.reason = TURBO_FLOW_SECURITY_REASON_POLICY_VERSION_MISMATCH;
    goto complete;
  }
  for (size_t i = 0u; i < turbo_vec_size(&realm->rules); ++i) {
    const turbo_flow_security_rule_t *rule =
        (const turbo_flow_security_rule_t *)turbo_vec_at(&realm->rules, i);
    int matched = 0;
    if (!rule || (rule->action_mask & request->action) == 0u ||
        rule->resource_type != request->resource_type ||
        (rule->tenant_id[0] && strcmp(rule->tenant_id, request->tenant_id) != 0) ||
        !flow_security_subject_matches(rule, request->principal)) {
      continue;
    }
    rc = flow_security_resource_matches(realm, rule, request, &matched);
    if (rc != TURBO_OK) goto complete;
    if (!matched) continue;
    if (rule->effect == TURBO_FLOW_SECURITY_DENY) {
      result.reason = TURBO_FLOW_SECURITY_REASON_DENY_RULE;
      result.matched_rule = i;
      goto complete;
    }
    if (allow_rule == SIZE_MAX) allow_rule = i;
  }
  if (allow_rule != SIZE_MAX) {
    result.effect = TURBO_FLOW_SECURITY_ALLOW;
    result.reason = TURBO_FLOW_SECURITY_REASON_ALLOW_RULE;
    result.matched_rule = allow_rule;
  }

complete:
  flow_security_record(realm, rc, &result);
  *decision = result;
  return rc;
}

int turbo_flow_security_realm_authorize(turbo_flow_security_realm_t *realm,
                                        const turbo_flow_security_request_t *request,
                                        uint64_t now_epoch_seconds,
                                        turbo_flow_security_decision_t *decision) {
  int rc = turbo_flow_security_realm_evaluate(realm, request, now_epoch_seconds, decision);
  return rc == TURBO_OK && decision->effect == TURBO_FLOW_SECURITY_DENY ? TURBO_EPERM : rc;
}

static int flow_security_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  const turbo_flow_security_realm_t *realm = (const turbo_flow_security_realm_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int written;
  if (!realm || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  metadata.domain = TURBO_FLOW_DOMAIN_RULES;
  metadata.kind = TURBO_FLOW_RESOURCE_SECURITY_REALM;
  metadata.generation = realm->policy_version;
  metadata.observed_generation = realm->policy_version;
  written = snprintf(metadata.uid, sizeof(metadata.uid), "%s", realm->resource_uid);
  if (written < 0 || (size_t)written >= sizeof(metadata.uid)) return TURBO_ENAMETOOLONG;
  written = snprintf(metadata.owner_name, sizeof(metadata.owner_name), "%s", realm->owner_name);
  if (written < 0 || (size_t)written >= sizeof(metadata.owner_name)) return TURBO_ENAMETOOLONG;
  *out = metadata;
  return TURBO_OK;
}

static int flow_security_resource_snapshot(void *ctx, turbo_flow_resource_snapshot_t *out) {
  turbo_flow_security_realm_t *realm = (turbo_flow_security_realm_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int rc;
  if (!out || out->size < sizeof(*out)) return TURBO_EINVAL;
  rc = flow_security_resource_metadata(ctx, &metadata);
  if (rc != TURBO_OK) return rc;
  *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  out->domain = metadata.domain;
  out->kind = metadata.kind;
  memcpy(out->uid, metadata.uid, sizeof(out->uid));
  memcpy(out->owner_name, metadata.owner_name, sizeof(out->owner_name));
  out->generation = metadata.generation;
  out->observed_generation = metadata.observed_generation;
  out->last_status = atomic_load_explicit(&realm->last_status, memory_order_acquire);
  return TURBO_OK;
}

static int flow_security_resource_document(void *ctx,
                                           turbo_flow_resource_document_kind_t document_kind,
                                           turbo_flow_resource_document_t *out) {
  turbo_flow_security_realm_t *realm = (turbo_flow_security_realm_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  char payload[512];
  int written;
  int rc;
  if (document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return TURBO_ENOTSUP;
  if (!realm || !out) return TURBO_EINVAL;
  rc = flow_security_resource_metadata(realm, &metadata);
  if (rc != TURBO_OK) return rc;
  written = snprintf(
      payload, sizeof(payload),
      "{\"policy_version\":\"%llu\",\"rule_count\":\"%llu\","
      "\"evaluations\":\"%llu\",\"allowed\":\"%llu\",\"denied\":\"%llu\","
      "\"failures\":\"%llu\",\"last_status\":%d}",
      (unsigned long long)realm->policy_version, (unsigned long long)turbo_vec_size(&realm->rules),
      (unsigned long long)atomic_load_explicit(&realm->evaluations, memory_order_relaxed),
      (unsigned long long)atomic_load_explicit(&realm->allowed, memory_order_relaxed),
      (unsigned long long)atomic_load_explicit(&realm->denied, memory_order_relaxed),
      (unsigned long long)atomic_load_explicit(&realm->failures, memory_order_relaxed),
      atomic_load_explicit(&realm->last_status, memory_order_acquire));
  if (written < 0 || (size_t)written >= sizeof(payload)) return TURBO_ERANGE;
  return turbo_flow_resource_document_set_payload_copy(out, &metadata, &FLOW_SECURITY_STATUS_SCHEMA,
                                                       payload, (size_t)written);
}

int turbo_flow_security_realm_register(turbo_flow_t *flow, turbo_flow_security_realm_t *realm) {
  turbo_flow_resource_provider_ops_t ops = TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;
  if (!flow || !realm) return TURBO_EINVAL;
  ops.metadata = flow_security_resource_metadata;
  ops.snapshot = flow_security_resource_snapshot;
  ops.document = flow_security_resource_document;
  return turbo_flow_register_resource_provider(flow, realm->owner_name, &ops, realm);
}

static int flow_security_config_error(turbo_flow_config_error_t *error, int status,
                                      const char *channel, const char *field, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field) {
      (void)snprintf(error->path, sizeof(error->path), "$.channels.%s.config.%s", channel, field);
    } else {
      (void)snprintf(error->path, sizeof(error->path), "$.channels.%s", channel ? channel : "?");
    }
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int flow_security_json_fields(const json_value_t *object, const char *const *allowed,
                                     size_t allowed_count, const char *channel, const char *scope,
                                     turbo_flow_config_error_t *error) {
  if (!object || turbo_json_type(object) != TURBO_JSON_OBJECT)
    return flow_security_config_error(error, TURBO_EINVAL, channel, scope, "expected mapping");
  for (size_t i = 0u; i < turbo_json_object_size(object); ++i) {
    const char *field = turbo_json_object_key(object, i);
    int known = 0;
    for (size_t j = 0u; j < allowed_count; ++j) {
      if (field && strcmp(field, allowed[j]) == 0) {
        known = 1;
        break;
      }
    }
    if (!known)
      return flow_security_config_error(error, TURBO_EINVAL, channel, field,
                                        "unknown security realm field");
  }
  return TURBO_OK;
}

static const char *flow_security_json_string(const json_value_t *object, const char *field) {
  json_value_t *value = turbo_json_object_get(object, field);
  return value && turbo_json_type(value) == TURBO_JSON_STRING ? turbo_json_string(value) : NULL;
}

static int flow_security_copy(char *out, size_t capacity, const char *value, int required) {
  size_t size;
  if (!out || capacity == 0u || !value || (required && !value[0])) return TURBO_EINVAL;
  size = strlen(value);
  if (size >= capacity) return TURBO_ENAMETOOLONG;
  memcpy(out, value, size + 1u);
  return TURBO_OK;
}

static int flow_security_parse_effect(const char *text, turbo_flow_security_effect_t *out) {
  if (!text || !out) return TURBO_EINVAL;
  if (strcmp(text, "allow") == 0) *out = TURBO_FLOW_SECURITY_ALLOW;
  else if (strcmp(text, "deny") == 0) *out = TURBO_FLOW_SECURITY_DENY;
  else return TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_security_parse_subject(const char *text, turbo_flow_security_subject_kind_t *out) {
  if (!text || !out) return TURBO_EINVAL;
  if (strcmp(text, "any") == 0) *out = TURBO_FLOW_SECURITY_SUBJECT_ANY;
  else if (strcmp(text, "principal") == 0) *out = TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL;
  else if (strcmp(text, "role") == 0) *out = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
  else if (strcmp(text, "group") == 0) *out = TURBO_FLOW_SECURITY_SUBJECT_GROUP;
  else return TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_security_parse_resource_type(const char *text,
                                             turbo_flow_security_resource_type_t *out) {
  if (!text || !out) return TURBO_EINVAL;
  if (strcmp(text, "generic") == 0) *out = TURBO_FLOW_SECURITY_RESOURCE_GENERIC;
  else if (strcmp(text, "mqtt_topic") == 0) *out = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
  else if (strcmp(text, "flow_resource") == 0) *out = TURBO_FLOW_SECURITY_RESOURCE_FLOW_RESOURCE;
  else if (strcmp(text, "sql_object") == 0) *out = TURBO_FLOW_SECURITY_RESOURCE_SQL_OBJECT;
  else if (strcmp(text, "secret") == 0) *out = TURBO_FLOW_SECURITY_RESOURCE_SECRET;
  else return TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_security_parse_match(const char *text, turbo_flow_security_match_kind_t *out) {
  if (!text || !out) return TURBO_EINVAL;
  if (strcmp(text, "exact") == 0) *out = TURBO_FLOW_SECURITY_MATCH_EXACT;
  else if (strcmp(text, "prefix") == 0) *out = TURBO_FLOW_SECURITY_MATCH_PREFIX;
  else if (strcmp(text, "adapter") == 0) *out = TURBO_FLOW_SECURITY_MATCH_ADAPTER;
  else return TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_security_action_bit(const char *text, uint32_t *out) {
  if (!text || !out) return TURBO_EINVAL;
  if (strcmp(text, "connect") == 0) *out = TURBO_FLOW_SECURITY_ACTION_CONNECT;
  else if (strcmp(text, "publish") == 0) *out = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
  else if (strcmp(text, "subscribe") == 0) *out = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
  else if (strcmp(text, "read") == 0) *out = TURBO_FLOW_SECURITY_ACTION_READ;
  else if (strcmp(text, "write") == 0) *out = TURBO_FLOW_SECURITY_ACTION_WRITE;
  else if (strcmp(text, "execute") == 0) *out = TURBO_FLOW_SECURITY_ACTION_EXECUTE;
  else if (strcmp(text, "admin") == 0) *out = TURBO_FLOW_SECURITY_ACTION_ADMIN;
  else return TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_security_parse_rule(const json_value_t *value, const char *channel, size_t index,
                                    turbo_flow_security_rule_t *rule,
                                    turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {"effect",  "subject_kind",  "subject", "tenant_id",
                                        "actions", "resource_type", "match",   "pattern"};
  json_value_t *actions;
  const char *text;
  char scope[64];
  int rc;
  (void)snprintf(scope, sizeof(scope), "rules[%llu]", (unsigned long long)index);
  rc = flow_security_json_fields(value, allowed, sizeof(allowed) / sizeof(allowed[0]), channel,
                                 scope, error);
  if (rc != TURBO_OK) return rc;
  *rule = (turbo_flow_security_rule_t)TURBO_FLOW_SECURITY_RULE_INIT;
  text = flow_security_json_string(value, "effect");
  if ((rc = flow_security_parse_effect(text, &rule->effect)) != TURBO_OK) goto invalid;
  text = flow_security_json_string(value, "subject_kind");
  if ((rc = flow_security_parse_subject(text, &rule->subject_kind)) != TURBO_OK) goto invalid;
  text = flow_security_json_string(value, "subject");
  if (rule->subject_kind == TURBO_FLOW_SECURITY_SUBJECT_ANY) {
    if (text && text[0]) goto invalid;
  } else if ((rc = flow_security_copy(rule->subject, sizeof(rule->subject), text, 1)) != TURBO_OK) {
    goto invalid;
  }
  text = flow_security_json_string(value, "tenant_id");
  if (text &&
      (rc = flow_security_copy(rule->tenant_id, sizeof(rule->tenant_id), text, 0)) != TURBO_OK)
    goto invalid;
  text = flow_security_json_string(value, "resource_type");
  if ((rc = flow_security_parse_resource_type(text, &rule->resource_type)) != TURBO_OK)
    goto invalid;
  text = flow_security_json_string(value, "match");
  if ((rc = flow_security_parse_match(text, &rule->match_kind)) != TURBO_OK) goto invalid;
  text = flow_security_json_string(value, "pattern");
  if ((rc = flow_security_copy(rule->pattern, sizeof(rule->pattern), text, 1)) != TURBO_OK)
    goto invalid;
  actions = turbo_json_object_get(value, "actions");
  if (!actions || turbo_json_type(actions) != TURBO_JSON_ARRAY ||
      turbo_json_array_size(actions) == 0u)
    goto invalid;
  for (size_t i = 0u; i < turbo_json_array_size(actions); ++i) {
    json_value_t *action = turbo_json_array_get(actions, i);
    uint32_t bit = 0u;
    if (!action || turbo_json_type(action) != TURBO_JSON_STRING ||
        flow_security_action_bit(turbo_json_string(action), &bit) != TURBO_OK ||
        (rule->action_mask & bit) != 0u) {
      goto invalid;
    }
    rule->action_mask |= bit;
  }
  return TURBO_OK;

invalid:
  return flow_security_config_error(error, rc == TURBO_ENAMETOOLONG ? rc : TURBO_EINVAL, channel,
                                    scope, "invalid security rule");
}

int turbo_flow_security_realm_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                              const char *channel_name,
                                              const turbo_flow_security_matcher_t *matcher,
                                              turbo_flow_security_realm_t **out,
                                              turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {"resource_uid", "owner_name", "policy_version", "rules"};
  turbo_json_doc_t *document = NULL;
  turbo_vec_t rules;
  turbo_flow_security_realm_config_t config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  json_value_t *rules_value;
  const char *json;
  size_t json_size = 0u;
  int rules_initialized = 0;
  int rc;
  if (out) *out = NULL;
  if (!resolved || !channel_name || !channel_name[0] || !out || !error ||
      error->size < sizeof(*error)) {
    return TURBO_EINVAL;
  }
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  json = turbo_flow_resolved_config_json(resolved, &json_size);
  if (!json || turbo_parse_json((const uint8_t *)json, json_size, &document) != TURBO_OK ||
      !document) {
    return flow_security_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                      "invalid resolved configuration snapshot");
  }
  channels = turbo_json_object_get(document, "channels");
  channel = channels ? turbo_json_object_get(channels, channel_name) : NULL;
  kind = channel ? turbo_json_object_get(channel, "kind") : NULL;
  fields = channel ? turbo_json_object_get(channel, "config") : NULL;
  if (!channel || turbo_json_type(channel) != TURBO_JSON_OBJECT) {
    rc = flow_security_config_error(error, TURBO_ENOENT, channel_name, NULL,
                                    "security realm channel is not resolved");
    goto done;
  }
  if (!kind || turbo_json_type(kind) != TURBO_JSON_STRING ||
      strcmp(turbo_json_string(kind), "security_realm") != 0) {
    rc = flow_security_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                    "channel kind must be security_realm");
    goto done;
  }
  rc = flow_security_json_fields(fields, allowed, sizeof(allowed) / sizeof(allowed[0]),
                                 channel_name, NULL, error);
  if (rc != TURBO_OK) goto done;
  config.resource_uid = flow_security_json_string(fields, "resource_uid");
  config.owner_name = flow_security_json_string(fields, "owner_name");
  {
    json_value_t *version = turbo_json_object_get(fields, "policy_version");
    double number =
        version && turbo_json_type(version) == TURBO_JSON_NUMBER ? turbo_json_number(version) : 0.0;
    if (!isfinite(number) || number < 1.0 || number > 9007199254740991.0 ||
        (double)(uint64_t)number != number) {
      rc = flow_security_config_error(error, TURBO_EINVAL, channel_name, "policy_version",
                                      "policy_version must be a positive safe integer");
      goto done;
    }
    config.policy_version = (uint64_t)number;
  }
  rules_value = turbo_json_object_get(fields, "rules");
  if (!rules_value || turbo_json_type(rules_value) != TURBO_JSON_ARRAY ||
      turbo_json_array_size(rules_value) == 0u ||
      turbo_json_array_size(rules_value) > TURBO_FLOW_SECURITY_MAX_RULES) {
    rc = flow_security_config_error(error, TURBO_EINVAL, channel_name, "rules",
                                    "rules must be a non-empty bounded array");
    goto done;
  }
  rc = turbo_vec_init(&rules, sizeof(turbo_flow_security_rule_t));
  if (rc != TURBO_OK) goto done;
  rules_initialized = 1;
  rc = turbo_vec_reserve(&rules, turbo_json_array_size(rules_value));
  for (size_t i = 0u; rc == TURBO_OK && i < turbo_json_array_size(rules_value); ++i) {
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    rc = flow_security_parse_rule(turbo_json_array_get(rules_value, i), channel_name, i, &rule,
                                  error);
    if (rc == TURBO_OK) rc = turbo_vec_push(&rules, &rule);
  }
  if (rc != TURBO_OK) goto done;
  config.rules = (const turbo_flow_security_rule_t *)rules.data;
  config.rule_count = turbo_vec_size(&rules);
  if (matcher) config.matcher = *matcher;
  rc = turbo_flow_security_realm_create(&config, out);
  if (rc != TURBO_OK)
    rc =
        flow_security_config_error(error, rc, channel_name, NULL, "security realm creation failed");

done:
  if (rules_initialized) turbo_vec_destroy(&rules);
  turbo_free_json(&document);
  return rc;
}

int turbo_flow_security_secret_acquire(const turbo_flow_security_key_provider_t *provider,
                                       const char *reference,
                                       turbo_flow_security_secret_lease_t *lease_out) {
  turbo_flow_security_secret_lease_t lease = TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
  int rc;
  if (!provider || provider->size < sizeof(*provider) || !provider->acquire || !provider->release ||
      !reference || !reference[0] || strlen(reference) > TURBO_FLOW_SECURITY_SECRET_REF_MAX ||
      !lease_out || lease_out->size < sizeof(*lease_out)) {
    return TURBO_EINVAL;
  }
  rc = provider->acquire(provider->ctx, reference, &lease);
  if (rc != TURBO_OK) {
    if (lease.provider_lease) provider->release(provider->ctx, &lease);
    return rc;
  }
  if (lease.size < sizeof(lease) || !lease.bytes || lease.byte_count == 0u ||
      !lease.provider_lease) {
    provider->release(provider->ctx, &lease);
    return TURBO_EPROTO;
  }
  *lease_out = lease;
  return TURBO_OK;
}

void turbo_flow_security_secret_release(const turbo_flow_security_key_provider_t *provider,
                                        turbo_flow_security_secret_lease_t *lease) {
  if (!lease) return;
  if (provider && provider->size >= sizeof(*provider) && provider->release && lease->provider_lease)
    provider->release(provider->ctx, lease);
  *lease = (turbo_flow_security_secret_lease_t)TURBO_FLOW_SECURITY_SECRET_LEASE_INIT;
}
