#include "turbo_flow_security.h"

#include "turbo_error.h"
#include "turbo_hash.h"
#include "turbo_parser.h"
#include "turbo_str.h"
#include "turbo_thread.h"
#include "turbo_vec.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct flow_security_adapter_leaf_s {
  turbo_vec_t entries;
  void *compiled;
  void *matcher_ctx;
  turbo_flow_security_match_destroy_leaf_fn destroy;
} flow_security_adapter_leaf_t;

typedef struct flow_security_rule_bucket_s {
  flow_security_adapter_leaf_t any_adapter;
  turbo_vec_t subjects;
  turbo_vec_t exact_patterns;
  turbo_vec_t prefix_patterns;
  turbo_hash_map_t subject_index;
  turbo_hash_map_t exact_index;
  turbo_hash_map_t prefix_index;
  uint32_t candidate_subject_mask;
  uint32_t exact_subject_mask;
  uint32_t prefix_subject_mask;
  size_t max_prefix_size;
  int initialized;
} flow_security_rule_bucket_t;

typedef struct flow_security_subject_key_s {
  turbo_flow_security_subject_kind_t kind;
  tstr_v subject;
} flow_security_subject_key_t;

typedef struct flow_security_subject_index_s {
  flow_security_subject_key_t key;
  flow_security_adapter_leaf_t adapter;
} flow_security_subject_index_t;

typedef struct flow_security_pattern_key_s {
  turbo_flow_security_subject_kind_t subject_kind;
  tstr_v subject;
  tstr_v pattern;
} flow_security_pattern_key_t;

typedef struct flow_security_pattern_index_s {
  flow_security_pattern_key_t key;
  turbo_vec_t entries;
} flow_security_pattern_index_t;

typedef struct flow_security_root_index_s {
  char domain_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  flow_security_rule_bucket_t buckets[7u][5u];
} flow_security_root_index_t;

typedef struct flow_security_policy_snapshot_s {
  atomic_uint_fast64_t references;
  uint64_t policy_version;
  uint64_t expires_at;
  turbo_vec_t rules;
  turbo_vec_t roots;
  turbo_hash_map_t root_index;
} flow_security_policy_snapshot_t;

struct turbo_flow_security_realm_s {
  tstr_t resource_uid;
  tstr_t owner_name;
  tstr_t policy_source;
  turbo_flow_security_matcher_t matcher;
  turbo_mutex_t snapshot_lock;
  flow_security_policy_snapshot_t *active;
  const turbo_flow_security_policy_provider_t *policy_provider;
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
      principal->abi_version != TURBO_FLOW_SECURITY_ABI_V3 ||
      !flow_security_cstr_valid(principal->principal_id, sizeof(principal->principal_id), 1) ||
      !flow_security_cstr_valid(principal->principal_type, sizeof(principal->principal_type), 1) ||
      !flow_security_cstr_valid(principal->domain_id, sizeof(principal->domain_id), 0) ||
      !flow_security_cstr_valid(principal->auth_method, sizeof(principal->auth_method), 1) ||
      principal->scope < TURBO_FLOW_SECURITY_SCOPE_SELF ||
      principal->scope > TURBO_FLOW_SECURITY_SCOPE_SYSTEM ||
      principal->role_count > TURBO_FLOW_SECURITY_MAX_ROLES ||
      principal->group_count > TURBO_FLOW_SECURITY_MAX_GROUPS || principal->policy_version == 0u ||
      (principal->scope == TURBO_FLOW_SECURITY_SCOPE_GROUP && principal->group_count == 0u)) {
    return 0;
  }
  if (principal->scope != TURBO_FLOW_SECURITY_SCOPE_SYSTEM && principal->domain_id[0] == '\0') {
    return 0;
  }
  for (uint32_t i = 0u; i < principal->role_count; ++i) {
    if (!flow_security_cstr_valid(principal->roles[i], sizeof(principal->roles[i]), 1)) return 0;
  }
  for (uint32_t i = 0u; i < principal->group_count; ++i) {
    if (!flow_security_cstr_valid(principal->groups[i], sizeof(principal->groups[i]), 1)) return 0;
    for (uint32_t j = 0u; j < i; ++j)
      if (strcmp(principal->groups[i], principal->groups[j]) == 0) return 0;
  }
  return 1;
}

int turbo_flow_security_authenticate(const turbo_flow_security_auth_provider_t *provider,
                                     const turbo_flow_security_auth_request_t *request,
                                     turbo_flow_security_principal_t *principal_out) {
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  int rc;
  if (!provider || provider->size < sizeof(*provider) || !provider->authenticate || !request ||
      request->size < TURBO_FLOW_SECURITY_AUTH_REQUEST_BASE_SIZE || !request->identity ||
      !request->identity[0] || !request->method || !request->method[0] ||
      (request->secret_size > 0u && !request->secret) || !principal_out ||
      principal_out->size < sizeof(*principal_out)) {
    return TURBO_EINVAL;
  }
  rc = provider->authenticate(provider->ctx, request, &principal);
  if (rc != TURBO_OK) return rc;
  if (!flow_security_principal_valid(&principal)) return TURBO_EPROTO;
  *principal_out = principal;
  return TURBO_OK;
}

static int
flow_security_enhanced_result_validate(const turbo_flow_security_enhanced_auth_result_t *result,
                                       void *exchange) {
  if (!result || result->size < sizeof(*result) || (result->data_size != 0u && !result->data) ||
      (result->status != TURBO_FLOW_SECURITY_ENHANCED_AUTH_CONTINUE &&
       result->status != TURBO_FLOW_SECURITY_ENHANCED_AUTH_SUCCESS))
    return TURBO_EPROTO;
  if (result->status == TURBO_FLOW_SECURITY_ENHANCED_AUTH_CONTINUE)
    return exchange ? TURBO_OK : TURBO_EPROTO;
  return flow_security_principal_valid(&result->principal) ? TURBO_OK : TURBO_EPROTO;
}

static int
flow_security_enhanced_request_valid(const turbo_flow_security_enhanced_auth_request_t *request) {
  return request && request->size >= TURBO_FLOW_SECURITY_ENHANCED_AUTH_REQUEST_BASE_SIZE &&
         request->method && request->method[0] && (!request->data_size || request->data);
}

int turbo_flow_security_enhanced_auth_begin(
    const turbo_flow_security_enhanced_auth_provider_t *provider,
    const turbo_flow_security_enhanced_auth_request_t *request, void **exchange_out,
    turbo_flow_security_enhanced_auth_result_t *result_out) {
  turbo_flow_security_enhanced_auth_result_t result = TURBO_FLOW_SECURITY_ENHANCED_AUTH_RESULT_INIT;
  void *exchange = NULL;
  int rc;
  if (!provider || provider->size < sizeof(*provider) || !provider->begin ||
      !provider->continue_exchange || !provider->cancel ||
      !flow_security_enhanced_request_valid(request) || !exchange_out || !result_out ||
      result_out->size < sizeof(*result_out))
    return TURBO_EINVAL;
  *exchange_out = NULL;
  rc = provider->begin(provider->ctx, request, &exchange, &result);
  if (rc != TURBO_OK) {
    if (exchange) provider->cancel(provider->ctx, exchange);
    return rc;
  }
  rc = flow_security_enhanced_result_validate(&result, exchange);
  if (rc != TURBO_OK) {
    if (exchange) provider->cancel(provider->ctx, exchange);
    return rc;
  }
  if (result.status == TURBO_FLOW_SECURITY_ENHANCED_AUTH_SUCCESS && exchange) {
    provider->cancel(provider->ctx, exchange);
    return TURBO_EPROTO;
  }
  *exchange_out = exchange;
  *result_out = result;
  return TURBO_OK;
}

int turbo_flow_security_enhanced_auth_continue(
    const turbo_flow_security_enhanced_auth_provider_t *provider, void *exchange,
    const turbo_flow_security_enhanced_auth_request_t *request,
    turbo_flow_security_enhanced_auth_result_t *result_out) {
  turbo_flow_security_enhanced_auth_result_t result = TURBO_FLOW_SECURITY_ENHANCED_AUTH_RESULT_INIT;
  int rc;
  if (!provider || provider->size < sizeof(*provider) || !provider->continue_exchange ||
      !provider->cancel || !exchange || !flow_security_enhanced_request_valid(request) ||
      !result_out || result_out->size < sizeof(*result_out))
    return TURBO_EINVAL;
  rc = provider->continue_exchange(provider->ctx, exchange, request, &result);
  if (rc != TURBO_OK) return rc;
  rc = flow_security_enhanced_result_validate(&result, exchange);
  if (rc != TURBO_OK) return rc;
  *result_out = result;
  return TURBO_OK;
}

void turbo_flow_security_enhanced_auth_cancel(
    const turbo_flow_security_enhanced_auth_provider_t *provider, void *exchange) {
  if (!provider || provider->size < sizeof(*provider) || !provider->cancel || !exchange) return;
  provider->cancel(provider->ctx, exchange);
}

static int flow_security_rule_valid(const turbo_flow_security_rule_t *rule,
                                    const turbo_flow_security_matcher_t *matcher) {
  int allow_empty_pattern;
  if (!rule) return 0;
  allow_empty_pattern = rule->action_mask == TURBO_FLOW_SECURITY_ACTION_CONNECT &&
                        rule->resource_type == TURBO_FLOW_SECURITY_RESOURCE_GENERIC &&
                        rule->match_kind == TURBO_FLOW_SECURITY_MATCH_PREFIX;
  if (rule->size < sizeof(*rule) || rule->abi_version != TURBO_FLOW_SECURITY_ABI_V3 ||
      rule->effect < TURBO_FLOW_SECURITY_DENY || rule->effect > TURBO_FLOW_SECURITY_ALLOW ||
      rule->subject_kind < TURBO_FLOW_SECURITY_SUBJECT_ANY ||
      rule->subject_kind > TURBO_FLOW_SECURITY_SUBJECT_GROUP ||
      !flow_security_cstr_valid(rule->subject, sizeof(rule->subject), 0) ||
      !flow_security_cstr_valid(rule->domain_id, sizeof(rule->domain_id), 1) ||
      rule->action_mask == 0u || (rule->action_mask & ~TURBO_FLOW_SECURITY_ACTION_ALL) != 0u ||
      rule->resource_type < TURBO_FLOW_SECURITY_RESOURCE_GENERIC ||
      rule->resource_type > TURBO_FLOW_SECURITY_RESOURCE_SECRET ||
      rule->match_kind < TURBO_FLOW_SECURITY_MATCH_EXACT ||
      rule->match_kind > TURBO_FLOW_SECURITY_MATCH_ADAPTER ||
      !flow_security_cstr_valid(rule->pattern, sizeof(rule->pattern), !allow_empty_pattern)) {
    return 0;
  }
  if (rule->subject_kind == TURBO_FLOW_SECURITY_SUBJECT_ANY) {
    if (rule->subject[0] != '\0') return 0;
  } else if (rule->subject[0] == '\0') {
    return 0;
  }
  return rule->match_kind != TURBO_FLOW_SECURITY_MATCH_ADAPTER ||
         (matcher && matcher->size >= sizeof(*matcher) &&
          matcher->abi_version == TURBO_FLOW_SECURITY_ABI_V3 && matcher->compile_leaf &&
          matcher->evaluate_leaf && matcher->destroy_leaf);
}

static int flow_security_realm_config_valid(const turbo_flow_security_realm_config_t *config) {
  const char *policy_source;
  int has_initial;
  int has_source;
  if (!config || config->size < offsetof(turbo_flow_security_realm_config_t, policy_source) ||
      config->abi_version != TURBO_FLOW_SECURITY_ABI_V3 || !config->resource_uid ||
      !config->resource_uid[0] || strlen(config->resource_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      !config->owner_name || !config->owner_name[0] ||
      strlen(config->owner_name) > TURBO_FLOW_RESOURCE_OWNER_MAX) {
    return 0;
  }
  policy_source = config->size >= sizeof(*config) ? config->policy_source : NULL;
  has_initial = config->policy_version != 0u || config->rules || config->rule_count != 0u;
  has_source = policy_source && policy_source[0];
  if (has_initial == has_source ||
      (has_source && strlen(policy_source) > TURBO_FLOW_RESOURCE_OWNER_MAX) ||
      (has_initial && (config->policy_version == 0u || !config->rules || config->rule_count == 0u ||
                       config->rule_count > TURBO_FLOW_SECURITY_MAX_RULES)))
    return 0;
  if (config->matcher.size < sizeof(config->matcher) ||
      config->matcher.abi_version != TURBO_FLOW_SECURITY_ABI_V3 ||
      ((!config->matcher.compile_leaf || !config->matcher.evaluate_leaf ||
        !config->matcher.destroy_leaf) &&
       (config->matcher.compile_leaf || config->matcher.evaluate_leaf ||
        config->matcher.destroy_leaf)))
    return 0;
  for (size_t i = 0u; i < config->rule_count; ++i) {
    if (!flow_security_rule_valid(&config->rules[i], &config->matcher)) return 0;
  }
  return 1;
}

static size_t flow_security_root_hash(const void *key, size_t key_size, void *ctx) {
  const tstr_v *value = (const tstr_v *)key;
  (void)key_size;
  (void)ctx;
  return turbo_hash_bytes(value->data, value->len, NULL);
}

static bool flow_security_root_equal(const void *left, const void *right, size_t key_size,
                                     void *ctx) {
  const tstr_v *a = (const tstr_v *)left;
  const tstr_v *b = (const tstr_v *)right;
  (void)key_size;
  (void)ctx;
  return a->len == b->len && (a->len == 0u || memcmp(a->data, b->data, a->len) == 0);
}

static size_t flow_security_subject_hash(const void *key, size_t key_size, void *ctx) {
  const flow_security_subject_key_t *value = (const flow_security_subject_key_t *)key;
  size_t hash = turbo_hash_bytes(value->subject.data, value->subject.len, NULL);
  (void)key_size;
  (void)ctx;
  return hash ^ ((size_t)value->kind * (size_t)UINT32_C(0x9e3779b1));
}

static bool flow_security_subject_equal(const void *left, const void *right, size_t key_size,
                                        void *ctx) {
  const flow_security_subject_key_t *a = (const flow_security_subject_key_t *)left;
  const flow_security_subject_key_t *b = (const flow_security_subject_key_t *)right;
  (void)key_size;
  (void)ctx;
  return a->kind == b->kind && a->subject.len == b->subject.len &&
         (a->subject.len == 0u || memcmp(a->subject.data, b->subject.data, a->subject.len) == 0);
}

static size_t flow_security_exact_hash(const void *key, size_t key_size, void *ctx) {
  const flow_security_pattern_key_t *value = (const flow_security_pattern_key_t *)key;
  size_t subject_hash = turbo_hash_bytes(value->subject.data, value->subject.len, NULL);
  size_t pattern_hash = turbo_hash_bytes(value->pattern.data, value->pattern.len, NULL);
  (void)key_size;
  (void)ctx;
  return pattern_hash ^ (subject_hash * (size_t)UINT32_C(0x9e3779b1)) ^
         ((size_t)value->subject_kind * (size_t)UINT32_C(0x85ebca6b));
}

static bool flow_security_exact_equal(const void *left, const void *right, size_t key_size,
                                      void *ctx) {
  const flow_security_pattern_key_t *a = (const flow_security_pattern_key_t *)left;
  const flow_security_pattern_key_t *b = (const flow_security_pattern_key_t *)right;
  (void)key_size;
  (void)ctx;
  return a->subject_kind == b->subject_kind && a->subject.len == b->subject.len &&
         a->pattern.len == b->pattern.len &&
         (a->subject.len == 0u || memcmp(a->subject.data, b->subject.data, a->subject.len) == 0) &&
         (a->pattern.len == 0u || memcmp(a->pattern.data, b->pattern.data, a->pattern.len) == 0);
}

static int flow_security_adapter_leaf_init(flow_security_adapter_leaf_t *leaf) {
  if (!leaf) return TURBO_EINVAL;
  memset(leaf, 0, sizeof(*leaf));
  return turbo_vec_init(&leaf->entries, sizeof(size_t));
}

static void flow_security_adapter_leaf_destroy(flow_security_adapter_leaf_t *leaf) {
  if (!leaf) return;
  if (leaf->compiled && leaf->destroy) leaf->destroy(leaf->matcher_ctx, leaf->compiled);
  turbo_vec_destroy(&leaf->entries);
  memset(leaf, 0, sizeof(*leaf));
}

static void flow_security_rule_bucket_destroy(flow_security_rule_bucket_t *bucket) {
  if (!bucket || !bucket->initialized) return;
  turbo_hash_map_destroy(&bucket->prefix_index);
  for (size_t i = 0u; i < turbo_vec_size(&bucket->prefix_patterns); ++i) {
    flow_security_pattern_index_t **pattern =
        (flow_security_pattern_index_t **)turbo_vec_at(&bucket->prefix_patterns, i);
    if (pattern && *pattern) {
      turbo_vec_destroy(&(*pattern)->entries);
      free(*pattern);
    }
  }
  turbo_hash_map_destroy(&bucket->exact_index);
  for (size_t i = 0u; i < turbo_vec_size(&bucket->exact_patterns); ++i) {
    flow_security_pattern_index_t **exact =
        (flow_security_pattern_index_t **)turbo_vec_at(&bucket->exact_patterns, i);
    if (exact && *exact) {
      turbo_vec_destroy(&(*exact)->entries);
      free(*exact);
    }
  }
  turbo_hash_map_destroy(&bucket->subject_index);
  for (size_t i = 0u; i < turbo_vec_size(&bucket->subjects); ++i) {
    flow_security_subject_index_t **subject =
        (flow_security_subject_index_t **)turbo_vec_at(&bucket->subjects, i);
    if (subject && *subject) {
      flow_security_adapter_leaf_destroy(&(*subject)->adapter);
      free(*subject);
    }
  }
  turbo_vec_destroy(&bucket->prefix_patterns);
  turbo_vec_destroy(&bucket->exact_patterns);
  turbo_vec_destroy(&bucket->subjects);
  flow_security_adapter_leaf_destroy(&bucket->any_adapter);
  memset(bucket, 0, sizeof(*bucket));
}

static int flow_security_rule_bucket_init(flow_security_rule_bucket_t *bucket) {
  int rc;
  if (!bucket) return TURBO_EINVAL;
  if (bucket->initialized) return TURBO_OK;
  rc = flow_security_adapter_leaf_init(&bucket->any_adapter);
  if (rc != TURBO_OK) return rc;
  rc = turbo_vec_init(&bucket->subjects, sizeof(flow_security_subject_index_t *));
  if (rc != TURBO_OK) {
    flow_security_adapter_leaf_destroy(&bucket->any_adapter);
    return rc;
  }
  rc = turbo_vec_init(&bucket->exact_patterns, sizeof(flow_security_pattern_index_t *));
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&bucket->subjects);
    flow_security_adapter_leaf_destroy(&bucket->any_adapter);
    return rc;
  }
  rc = turbo_vec_init(&bucket->prefix_patterns, sizeof(flow_security_pattern_index_t *));
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&bucket->exact_patterns);
    turbo_vec_destroy(&bucket->subjects);
    flow_security_adapter_leaf_destroy(&bucket->any_adapter);
    return rc;
  }
  rc = turbo_hash_map_init(&bucket->subject_index, sizeof(flow_security_subject_key_t),
                           sizeof(flow_security_subject_index_t *), flow_security_subject_hash,
                           flow_security_subject_equal, NULL);
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&bucket->prefix_patterns);
    turbo_vec_destroy(&bucket->exact_patterns);
    turbo_vec_destroy(&bucket->subjects);
    flow_security_adapter_leaf_destroy(&bucket->any_adapter);
    return rc;
  }
  rc = turbo_hash_map_init(&bucket->exact_index, sizeof(flow_security_pattern_key_t),
                           sizeof(flow_security_pattern_index_t *), flow_security_exact_hash,
                           flow_security_exact_equal, NULL);
  if (rc != TURBO_OK) {
    turbo_hash_map_destroy(&bucket->subject_index);
    turbo_vec_destroy(&bucket->prefix_patterns);
    turbo_vec_destroy(&bucket->exact_patterns);
    turbo_vec_destroy(&bucket->subjects);
    flow_security_adapter_leaf_destroy(&bucket->any_adapter);
    return rc;
  }
  rc = turbo_hash_map_init(&bucket->prefix_index, sizeof(flow_security_pattern_key_t),
                           sizeof(flow_security_pattern_index_t *), flow_security_exact_hash,
                           flow_security_exact_equal, NULL);
  if (rc != TURBO_OK) {
    turbo_hash_map_destroy(&bucket->exact_index);
    turbo_hash_map_destroy(&bucket->subject_index);
    turbo_vec_destroy(&bucket->prefix_patterns);
    turbo_vec_destroy(&bucket->exact_patterns);
    turbo_vec_destroy(&bucket->subjects);
    flow_security_adapter_leaf_destroy(&bucket->any_adapter);
    return rc;
  }
  bucket->initialized = 1;
  return TURBO_OK;
}

static int flow_security_rule_bucket_insert_pattern(turbo_hash_map_t *index, turbo_vec_t *patterns,
                                                    uint32_t *subject_mask,
                                                    const turbo_flow_security_rule_t *rule,
                                                    size_t rule_index) {
  flow_security_pattern_key_t key;
  flow_security_pattern_index_t **found;
  flow_security_pattern_index_t *pattern;
  int rc;
  if (!index || !patterns || !subject_mask || !rule) return TURBO_EINVAL;
  key.subject_kind = rule->subject_kind;
  key.subject = tstr_v_from_buf(rule->subject, strlen(rule->subject));
  key.pattern = tstr_v_from_buf(rule->pattern, strlen(rule->pattern));
  found = (flow_security_pattern_index_t **)turbo_hash_map_get(index, &key);
  if (found && *found) return turbo_vec_push(&(*found)->entries, &rule_index);
  pattern = (flow_security_pattern_index_t *)calloc(1u, sizeof(*pattern));
  if (!pattern) return TURBO_ENOMEM;
  pattern->key = key;
  rc = turbo_vec_init(&pattern->entries, sizeof(size_t));
  if (rc == TURBO_OK) rc = turbo_vec_push(&pattern->entries, &rule_index);
  if (rc == TURBO_OK) rc = turbo_vec_push(patterns, &pattern);
  if (rc == TURBO_OK) rc = turbo_hash_map_put(index, &pattern->key, &pattern);
  if (rc == TURBO_OK) *subject_mask |= UINT32_C(1) << rule->subject_kind;
  if (rc != TURBO_OK) {
    if (turbo_vec_size(patterns) > 0u) {
      flow_security_pattern_index_t **last =
          (flow_security_pattern_index_t **)turbo_vec_at(patterns, turbo_vec_size(patterns) - 1u);
      if (last && *last == pattern) (void)turbo_vec_pop(patterns, NULL);
    }
    turbo_vec_destroy(&pattern->entries);
    free(pattern);
  }
  return rc;
}

static int flow_security_rule_bucket_insert(flow_security_rule_bucket_t *bucket,
                                            const turbo_flow_security_rule_t *rule,
                                            size_t rule_index) {
  flow_security_subject_key_t key;
  flow_security_subject_index_t **found;
  flow_security_subject_index_t *subject;
  int rc = flow_security_rule_bucket_init(bucket);
  if (rc != TURBO_OK) return rc;
  if (rule->match_kind == TURBO_FLOW_SECURITY_MATCH_EXACT)
    return flow_security_rule_bucket_insert_pattern(&bucket->exact_index, &bucket->exact_patterns,
                                                    &bucket->exact_subject_mask, rule, rule_index);
  if (rule->match_kind == TURBO_FLOW_SECURITY_MATCH_PREFIX) {
    size_t pattern_size = strlen(rule->pattern);
    rc = flow_security_rule_bucket_insert_pattern(&bucket->prefix_index, &bucket->prefix_patterns,
                                                  &bucket->prefix_subject_mask, rule, rule_index);
    if (rc == TURBO_OK && pattern_size > bucket->max_prefix_size)
      bucket->max_prefix_size = pattern_size;
    return rc;
  }
  bucket->candidate_subject_mask |= UINT32_C(1) << rule->subject_kind;
  if (rule->subject_kind == TURBO_FLOW_SECURITY_SUBJECT_ANY)
    return turbo_vec_push(&bucket->any_adapter.entries, &rule_index);
  key.kind = rule->subject_kind;
  key.subject = tstr_v_from_buf(rule->subject, strlen(rule->subject));
  found = (flow_security_subject_index_t **)turbo_hash_map_get(&bucket->subject_index, &key);
  if (found && *found) return turbo_vec_push(&(*found)->adapter.entries, &rule_index);
  subject = (flow_security_subject_index_t *)calloc(1u, sizeof(*subject));
  if (!subject) return TURBO_ENOMEM;
  subject->key = key;
  rc = flow_security_adapter_leaf_init(&subject->adapter);
  if (rc == TURBO_OK) rc = turbo_vec_push(&subject->adapter.entries, &rule_index);
  if (rc == TURBO_OK) rc = turbo_vec_push(&bucket->subjects, &subject);
  if (rc == TURBO_OK) rc = turbo_hash_map_put(&bucket->subject_index, &subject->key, &subject);
  if (rc != TURBO_OK) {
    if (turbo_vec_size(&bucket->subjects) > 0u) {
      flow_security_subject_index_t **last = (flow_security_subject_index_t **)turbo_vec_at(
          &bucket->subjects, turbo_vec_size(&bucket->subjects) - 1u);
      if (last && *last == subject) (void)turbo_vec_pop(&bucket->subjects, NULL);
    }
    flow_security_adapter_leaf_destroy(&subject->adapter);
    free(subject);
  }
  return rc;
}

static flow_security_adapter_leaf_t *
flow_security_rule_bucket_subject(flow_security_rule_bucket_t *bucket,
                                  turbo_flow_security_subject_kind_t kind, const char *subject) {
  flow_security_subject_key_t key;
  flow_security_subject_index_t **found;
  if (!bucket || !bucket->initialized || !subject || !subject[0]) return NULL;
  if ((bucket->candidate_subject_mask & (UINT32_C(1) << kind)) == 0u) return NULL;
  key.kind = kind;
  key.subject = tstr_v_from_buf(subject, strlen(subject));
  found = (flow_security_subject_index_t **)turbo_hash_map_get(&bucket->subject_index, &key);
  return found && *found ? &(*found)->adapter : NULL;
}

static const turbo_vec_t *
flow_security_rule_bucket_pattern(flow_security_rule_bucket_t *bucket, turbo_hash_map_t *index,
                                  uint32_t subject_mask,
                                  turbo_flow_security_subject_kind_t subject_kind,
                                  const char *subject, const char *pattern, size_t pattern_size) {
  flow_security_pattern_key_t key;
  flow_security_pattern_index_t **found;
  if (!bucket || !bucket->initialized || !index || !subject || !pattern)
    return NULL;
  if ((subject_mask & (UINT32_C(1) << subject_kind)) == 0u) return NULL;
  key.subject_kind = subject_kind;
  key.subject = tstr_v_from_buf(subject, strlen(subject));
  key.pattern = tstr_v_from_buf(pattern, pattern_size);
  found = (flow_security_pattern_index_t **)turbo_hash_map_get(index, &key);
  return found && *found ? &(*found)->entries : NULL;
}

static int flow_security_adapter_leaf_compile(flow_security_adapter_leaf_t *leaf,
                                              const turbo_vec_t *rules,
                                              const turbo_flow_security_matcher_t *matcher) {
  turbo_flow_security_matcher_leaf_t input = TURBO_FLOW_SECURITY_MATCHER_LEAF_INIT;
  void *compiled = NULL;
  int rc;
  if (!leaf || !rules) return TURBO_EINVAL;
  if (turbo_vec_empty(&leaf->entries)) return TURBO_OK;
  if (!matcher || !matcher->compile_leaf || !matcher->evaluate_leaf || !matcher->destroy_leaf)
    return TURBO_EINVAL;
  if (leaf->compiled) return TURBO_EALREADY;
  input.rules = (const turbo_flow_security_rule_t *)turbo_vec_data_const(rules);
  input.rule_count = turbo_vec_size(rules);
  input.candidate_rule_indices = (const size_t *)turbo_vec_data_const(&leaf->entries);
  input.candidate_count = turbo_vec_size(&leaf->entries);
  if (!input.rules || !input.candidate_rule_indices) return TURBO_EPROTO;
  rc = matcher->compile_leaf(matcher->ctx, &input, &compiled);
  if (rc != TURBO_OK || !compiled) {
    if (compiled) matcher->destroy_leaf(matcher->ctx, compiled);
    return rc != TURBO_OK ? rc : TURBO_EPROTO;
  }
  leaf->compiled = compiled;
  leaf->matcher_ctx = matcher->ctx;
  leaf->destroy = matcher->destroy_leaf;
  return TURBO_OK;
}

static int flow_security_rule_bucket_compile(flow_security_rule_bucket_t *bucket,
                                             const turbo_vec_t *rules,
                                             const turbo_flow_security_matcher_t *matcher) {
  int rc;
  if (!bucket || !bucket->initialized) return TURBO_OK;
  rc = flow_security_adapter_leaf_compile(&bucket->any_adapter, rules, matcher);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < turbo_vec_size(&bucket->subjects); ++i) {
    flow_security_subject_index_t **subject =
        (flow_security_subject_index_t **)turbo_vec_at(&bucket->subjects, i);
    if (!subject || !*subject) return TURBO_EPROTO;
    rc = flow_security_adapter_leaf_compile(&(*subject)->adapter, rules, matcher);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static void flow_security_root_index_destroy(flow_security_root_index_t *root) {
  if (!root) return;
  for (size_t action = 0u; action < 7u; ++action)
    for (size_t resource = 0u; resource < 5u; ++resource)
      flow_security_rule_bucket_destroy(&root->buckets[action][resource]);
  free(root);
}

static int flow_security_snapshot_root(flow_security_policy_snapshot_t *snapshot,
                                       const char *domain_id,
                                       flow_security_root_index_t **root_out) {
  tstr_v key;
  flow_security_root_index_t **found;
  flow_security_root_index_t *root;
  if (!snapshot || !domain_id || !root_out) return TURBO_EINVAL;
  *root_out = NULL;
  key = tstr_v_from_buf(domain_id, strlen(domain_id));
  found = (flow_security_root_index_t **)turbo_hash_map_get(&snapshot->root_index, &key);
  if (found && *found) {
    *root_out = *found;
    return TURBO_OK;
  }
  root = (flow_security_root_index_t *)calloc(1u, sizeof(*root));
  if (!root) return TURBO_ENOMEM;
  memcpy(root->domain_id, domain_id, strlen(domain_id) + 1u);
  if (turbo_vec_push(&snapshot->roots, &root) != TURBO_OK) {
    flow_security_root_index_destroy(root);
    return TURBO_ENOMEM;
  }
  key = tstr_v_from_buf(root->domain_id, strlen(root->domain_id));
  if (turbo_hash_map_put(&snapshot->root_index, &key, &root) != TURBO_OK) {
    size_t last = turbo_vec_size(&snapshot->roots) - 1u;
    (void)turbo_vec_swap_remove(&snapshot->roots, last, NULL);
    flow_security_root_index_destroy(root);
    return TURBO_ENOMEM;
  }
  *root_out = root;
  return TURBO_OK;
}

static void flow_security_policy_snapshot_release(flow_security_policy_snapshot_t *snapshot) {
  if (!snapshot) return;
  if (atomic_fetch_sub_explicit(&snapshot->references, 1u, memory_order_acq_rel) == 1u) {
    turbo_hash_map_destroy(&snapshot->root_index);
    for (size_t i = 0u; i < turbo_vec_size(&snapshot->roots); ++i) {
      flow_security_root_index_t **root =
          (flow_security_root_index_t **)turbo_vec_at(&snapshot->roots, i);
      if (root) flow_security_root_index_destroy(*root);
    }
    turbo_vec_destroy(&snapshot->roots);
    turbo_vec_destroy(&snapshot->rules);
    free(snapshot);
  }
}

static int flow_security_policy_snapshot_create(uint64_t policy_version, uint64_t expires_at,
                                                const turbo_flow_security_rule_t *rules,
                                                size_t rule_count,
                                                const turbo_flow_security_matcher_t *matcher,
                                                flow_security_policy_snapshot_t **out) {
  flow_security_policy_snapshot_t *snapshot;
  int rc;
  if (out) *out = NULL;
  if (!out || policy_version == 0u || !rules || rule_count == 0u ||
      rule_count > TURBO_FLOW_SECURITY_MAX_RULES)
    return TURBO_EINVAL;
  for (size_t i = 0u; i < rule_count; ++i)
    if (!flow_security_rule_valid(&rules[i], matcher)) return TURBO_EINVAL;
  snapshot = (flow_security_policy_snapshot_t *)calloc(1u, sizeof(*snapshot));
  if (!snapshot) return TURBO_ENOMEM;
  atomic_init(&snapshot->references, 1u);
  rc = turbo_vec_init(&snapshot->rules, sizeof(turbo_flow_security_rule_t));
  if (rc != TURBO_OK) {
    free(snapshot);
    return rc;
  }
  rc = turbo_vec_init(&snapshot->roots, sizeof(flow_security_root_index_t *));
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&snapshot->rules);
    free(snapshot);
    return rc;
  }
  rc = turbo_hash_map_init(&snapshot->root_index, sizeof(tstr_v),
                           sizeof(flow_security_root_index_t *), flow_security_root_hash,
                           flow_security_root_equal, NULL);
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&snapshot->roots);
    turbo_vec_destroy(&snapshot->rules);
    free(snapshot);
    return rc;
  }
  rc = turbo_vec_reserve(&snapshot->rules, rule_count);
  for (size_t i = 0u; rc == TURBO_OK && i < rule_count; ++i) {
    const turbo_flow_security_rule_t *compiled_rule;
    flow_security_root_index_t *root = NULL;
    rc = turbo_vec_push(&snapshot->rules, &rules[i]);
    if (rc != TURBO_OK) break;
    compiled_rule = (const turbo_flow_security_rule_t *)turbo_vec_at_const(&snapshot->rules, i);
    if (!compiled_rule) {
      rc = TURBO_EPROTO;
      break;
    }
    rc = flow_security_snapshot_root(snapshot, compiled_rule->domain_id, &root);
    if (rc != TURBO_OK) break;
    for (size_t action = 0u; action < 7u; ++action) {
      if ((compiled_rule->action_mask & (UINT32_C(1) << action)) == 0u) continue;
      rc = flow_security_rule_bucket_insert(
          &root->buckets[action][compiled_rule->resource_type - 1u], compiled_rule, i);
      if (rc != TURBO_OK) break;
    }
  }
  for (size_t root_index = 0u; rc == TURBO_OK && root_index < turbo_vec_size(&snapshot->roots);
       ++root_index) {
    flow_security_root_index_t **root =
        (flow_security_root_index_t **)turbo_vec_at(&snapshot->roots, root_index);
    if (!root || !*root) {
      rc = TURBO_EPROTO;
      break;
    }
    for (size_t action = 0u; rc == TURBO_OK && action < 7u; ++action) {
      for (size_t resource = 0u; rc == TURBO_OK && resource < 5u; ++resource)
        rc = flow_security_rule_bucket_compile(&(*root)->buckets[action][resource],
                                               &snapshot->rules, matcher);
    }
  }
  if (rc != TURBO_OK) {
    flow_security_policy_snapshot_release(snapshot);
    return rc;
  }
  snapshot->policy_version = policy_version;
  snapshot->expires_at = expires_at;
  *out = snapshot;
  return TURBO_OK;
}

static flow_security_policy_snapshot_t *
flow_security_policy_snapshot_acquire(turbo_flow_security_realm_t *realm) {
  flow_security_policy_snapshot_t *snapshot;
  turbo_mutex_lock(&realm->snapshot_lock);
  snapshot = realm->active;
  if (snapshot) (void)atomic_fetch_add_explicit(&snapshot->references, 1u, memory_order_relaxed);
  turbo_mutex_unlock(&realm->snapshot_lock);
  return snapshot;
}

int turbo_flow_security_realm_create(const turbo_flow_security_realm_config_t *config,
                                     turbo_flow_security_realm_t **out) {
  turbo_flow_security_realm_t *realm;
  const char *policy_source;
  int rc;
  if (out) *out = NULL;
  if (!out || !flow_security_realm_config_valid(config)) return TURBO_EINVAL;
  realm = (turbo_flow_security_realm_t *)calloc(1u, sizeof(*realm));
  if (!realm) return TURBO_ENOMEM;
  turbo_mutex_init(&realm->snapshot_lock);
  policy_source = config->size >= sizeof(*config) ? config->policy_source : NULL;
  realm->resource_uid = tstr_dup(config->resource_uid);
  realm->owner_name = tstr_dup(config->owner_name);
  realm->policy_source = policy_source ? tstr_dup(policy_source) : NULL;
  realm->matcher = config->matcher;
  if (!realm->resource_uid || !realm->owner_name || (policy_source && !realm->policy_source)) {
    turbo_flow_security_realm_destroy(realm);
    return TURBO_ENOMEM;
  }
  if (config->policy_version != 0u) {
    rc = flow_security_policy_snapshot_create(config->policy_version, 0u, config->rules,
                                              config->rule_count, &realm->matcher, &realm->active);
    if (rc != TURBO_OK) {
      turbo_flow_security_realm_destroy(realm);
      return rc;
    }
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
  flow_security_policy_snapshot_t *active;
  if (!realm) return;
  turbo_mutex_lock(&realm->snapshot_lock);
  active = realm->active;
  realm->active = NULL;
  realm->policy_provider = NULL;
  turbo_mutex_unlock(&realm->snapshot_lock);
  flow_security_policy_snapshot_release(active);
  turbo_mutex_destroy(&realm->snapshot_lock);
  tstr_freep(&realm->resource_uid);
  tstr_freep(&realm->owner_name);
  tstr_freep(&realm->policy_source);
  free(realm);
}

const char *turbo_flow_security_realm_policy_source(const turbo_flow_security_realm_t *realm) {
  return realm && realm->policy_source ? realm->policy_source : NULL;
}

int turbo_flow_security_realm_bind_policy_provider(
    turbo_flow_security_realm_t *realm, const turbo_flow_security_policy_provider_t *provider) {
  int rc = TURBO_OK;
  if (!realm || !realm->policy_source || !provider || provider->size < sizeof(*provider) ||
      !provider->load || !provider->release)
    return TURBO_EINVAL;
  turbo_mutex_lock(&realm->snapshot_lock);
  if (realm->policy_provider)
    rc = realm->policy_provider == provider ? TURBO_EALREADY : TURBO_EBUSY;
  else realm->policy_provider = provider;
  turbo_mutex_unlock(&realm->snapshot_lock);
  return rc;
}

int turbo_flow_security_realm_refresh(turbo_flow_security_realm_t *realm, uint64_t required_version,
                                      uint64_t now_epoch_seconds) {
  turbo_flow_security_policy_bundle_t bundle = TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  const turbo_flow_security_policy_provider_t *provider;
  flow_security_policy_snapshot_t *replacement = NULL;
  flow_security_policy_snapshot_t *previous = NULL;
  uint64_t active_version = 0u;
  int bundle_loaded = 0;
  int rc;
  if (!realm || !realm->policy_source) return TURBO_EINVAL;
  turbo_mutex_lock(&realm->snapshot_lock);
  provider = realm->policy_provider;
  if (realm->active) active_version = realm->active->policy_version;
  turbo_mutex_unlock(&realm->snapshot_lock);
  if (!provider) return TURBO_ENOTSUP;
  rc = provider->load(provider->ctx, required_version, &bundle);
  if (rc != TURBO_OK) goto done;
  bundle_loaded = 1;
  if (bundle.size < sizeof(bundle) || bundle.abi_version != TURBO_FLOW_SECURITY_ABI_V3 ||
      bundle.policy_version == 0u ||
      (required_version != 0u && bundle.policy_version != required_version) ||
      bundle.policy_version < active_version || !bundle.rules || bundle.rule_count == 0u ||
      bundle.rule_count > TURBO_FLOW_SECURITY_MAX_RULES ||
      (bundle.expires_at != 0u &&
       (now_epoch_seconds == 0u || now_epoch_seconds >= bundle.expires_at))) {
    rc = TURBO_EPROTO;
    goto done;
  }
  rc = flow_security_policy_snapshot_create(bundle.policy_version, bundle.expires_at, bundle.rules,
                                            bundle.rule_count, &realm->matcher, &replacement);
  if (rc != TURBO_OK) goto done;
  turbo_mutex_lock(&realm->snapshot_lock);
  if (realm->active && realm->active->policy_version > replacement->policy_version) {
    rc = TURBO_EBUSY;
  } else if (realm->active && realm->active->policy_version == replacement->policy_version) {
    rc = realm->active->expires_at != 0u &&
                 (now_epoch_seconds == 0u || now_epoch_seconds >= realm->active->expires_at)
             ? TURBO_EPROTO
             : TURBO_OK;
  } else {
    previous = realm->active;
    realm->active = replacement;
    replacement = NULL;
  }
  turbo_mutex_unlock(&realm->snapshot_lock);

done:
  flow_security_policy_snapshot_release(previous);
  flow_security_policy_snapshot_release(replacement);
  if (provider && provider->release && (bundle_loaded || bundle.provider_bundle))
    provider->release(provider->ctx, &bundle);
  atomic_store_explicit(&realm->last_status, rc, memory_order_release);
  if (rc != TURBO_OK) atomic_fetch_add_explicit(&realm->failures, 1u, memory_order_relaxed);
  return rc;
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

static int flow_security_resource_matches(const turbo_flow_security_rule_t *rule,
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
  return TURBO_ENOTSUP;
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

static size_t flow_security_action_index(uint32_t action) {
  size_t index = 0u;
  while ((UINT32_C(1) << index) != action)
    ++index;
  return index;
}

/**
 * Evaluate one pre-indexed subject list. Snapshot compilation is O(rules * action bits);
 * authorization uses average O(1) structural/exact lookup, O(resource length) prefix probes, and
 * one protocol-compiled adapter query per applicable subject leaf, with no Core allocation.
 */
static int flow_security_evaluate_entries(flow_security_policy_snapshot_t *snapshot,
                                          const turbo_flow_security_request_t *request,
                                          const turbo_vec_t *entries, size_t *deny_rule,
                                          size_t *allow_rule) {
  if (!entries) return TURBO_OK;
  for (size_t position = 0u; position < turbo_vec_size(entries); ++position) {
    const size_t *rule_index = (const size_t *)turbo_vec_at_const(entries, position);
    const turbo_flow_security_rule_t *rule =
        rule_index
            ? (const turbo_flow_security_rule_t *)turbo_vec_at_const(&snapshot->rules, *rule_index)
            : NULL;
    int matched = 0;
    int rc;
    if (!rule || !flow_security_subject_matches(rule, request->principal)) continue;
    rc = flow_security_resource_matches(rule, request, &matched);
    if (rc != TURBO_OK) return rc;
    if (!matched) continue;
    if (rule->effect == TURBO_FLOW_SECURITY_DENY) {
      if (*deny_rule == SIZE_MAX || *rule_index < *deny_rule) *deny_rule = *rule_index;
    } else if (*allow_rule == SIZE_MAX || *rule_index < *allow_rule) {
      *allow_rule = *rule_index;
    }
  }
  return TURBO_OK;
}

typedef struct flow_security_adapter_emit_context_s {
  flow_security_policy_snapshot_t *snapshot;
  const flow_security_adapter_leaf_t *leaf;
  size_t *deny_rule;
  size_t *allow_rule;
  int status;
} flow_security_adapter_emit_context_t;

static int flow_security_adapter_emit(void *ctx, size_t candidate_position) {
  flow_security_adapter_emit_context_t *emit = (flow_security_adapter_emit_context_t *)ctx;
  const size_t *rule_index;
  const turbo_flow_security_rule_t *rule;
  if (!emit || !emit->snapshot || !emit->leaf || !emit->deny_rule || !emit->allow_rule ||
      emit->status != TURBO_OK || candidate_position >= turbo_vec_size(&emit->leaf->entries)) {
    if (emit) emit->status = TURBO_EPROTO;
    return TURBO_EPROTO;
  }
  rule_index = (const size_t *)turbo_vec_at_const(&emit->leaf->entries, candidate_position);
  rule = rule_index ? (const turbo_flow_security_rule_t *)turbo_vec_at_const(&emit->snapshot->rules,
                                                                             *rule_index)
                    : NULL;
  if (!rule || rule->match_kind != TURBO_FLOW_SECURITY_MATCH_ADAPTER) {
    emit->status = TURBO_EPROTO;
    return TURBO_EPROTO;
  }
  if (rule->effect == TURBO_FLOW_SECURITY_DENY) {
    if (*emit->deny_rule == SIZE_MAX || *rule_index < *emit->deny_rule)
      *emit->deny_rule = *rule_index;
  } else if (*emit->allow_rule == SIZE_MAX || *rule_index < *emit->allow_rule) {
    *emit->allow_rule = *rule_index;
  }
  return TURBO_OK;
}

static int flow_security_evaluate_adapter_leaf(const turbo_flow_security_matcher_t *matcher,
                                               flow_security_policy_snapshot_t *snapshot,
                                               const turbo_flow_security_request_t *request,
                                               const flow_security_adapter_leaf_t *leaf,
                                               size_t *deny_rule, size_t *allow_rule) {
  flow_security_adapter_emit_context_t emit;
  int rc;
  if (!leaf || turbo_vec_empty(&leaf->entries)) return TURBO_OK;
  if (!matcher || !matcher->evaluate_leaf || !leaf->compiled) return TURBO_EPROTO;
  emit.snapshot = snapshot;
  emit.leaf = leaf;
  emit.deny_rule = deny_rule;
  emit.allow_rule = allow_rule;
  emit.status = TURBO_OK;
  rc = matcher->evaluate_leaf(matcher->ctx, leaf->compiled, request, flow_security_adapter_emit,
                              &emit);
  return rc != TURBO_OK ? rc : emit.status;
}

static int flow_security_evaluate_subject(turbo_flow_security_realm_t *realm,
                                          flow_security_policy_snapshot_t *snapshot,
                                          const turbo_flow_security_request_t *request,
                                          flow_security_rule_bucket_t *bucket,
                                          turbo_flow_security_subject_kind_t subject_kind,
                                          const char *subject, size_t resource_size,
                                          size_t *deny_rule, size_t *allow_rule) {
  const turbo_vec_t *candidates;
  flow_security_adapter_leaf_t *adapter;
  int rc;
  candidates =
      flow_security_rule_bucket_pattern(bucket, &bucket->exact_index, bucket->exact_subject_mask,
                                        subject_kind, subject, request->resource, resource_size);
  rc = flow_security_evaluate_entries(snapshot, request, candidates, deny_rule, allow_rule);
  if (rc != TURBO_OK) return rc;
  if ((bucket->prefix_subject_mask & (UINT32_C(1) << subject_kind)) != 0u) {
    candidates = flow_security_rule_bucket_pattern(bucket, &bucket->prefix_index,
                                                   bucket->prefix_subject_mask, subject_kind,
                                                   subject, request->resource, 0u);
    rc = flow_security_evaluate_entries(snapshot, request, candidates, deny_rule, allow_rule);
    if (rc != TURBO_OK) return rc;
    size_t prefix_limit =
        resource_size < bucket->max_prefix_size ? resource_size : bucket->max_prefix_size;
    for (size_t prefix_size = 1u; prefix_size <= prefix_limit; ++prefix_size) {
      candidates = flow_security_rule_bucket_pattern(bucket, &bucket->prefix_index,
                                                     bucket->prefix_subject_mask, subject_kind,
                                                     subject, request->resource, prefix_size);
      rc = flow_security_evaluate_entries(snapshot, request, candidates, deny_rule, allow_rule);
      if (rc != TURBO_OK) return rc;
    }
  }
  adapter = subject_kind == TURBO_FLOW_SECURITY_SUBJECT_ANY
                ? &bucket->any_adapter
                : flow_security_rule_bucket_subject(bucket, subject_kind, subject);
  return flow_security_evaluate_adapter_leaf(&realm->matcher, snapshot, request, adapter, deny_rule,
                                             allow_rule);
}

static int flow_security_request_valid(const turbo_flow_security_realm_t *realm,
                                       const turbo_flow_security_request_t *request,
                                       const turbo_flow_security_decision_t *decision) {
  return realm && request && request->size >= sizeof(*request) &&
         flow_security_principal_valid(request->principal) && request->domain_id &&
         request->domain_id[0] && request->resource && request->resource[0] &&
         request->action != 0u && (request->action & (request->action - 1u)) == 0u &&
         (request->action & ~TURBO_FLOW_SECURITY_ACTION_ALL) == 0u &&
         request->resource_type >= TURBO_FLOW_SECURITY_RESOURCE_GENERIC &&
         request->resource_type <= TURBO_FLOW_SECURITY_RESOURCE_SECRET && decision &&
         decision->size >= sizeof(*decision);
}

static int flow_security_realm_evaluate_validated(turbo_flow_security_realm_t *realm,
                                                  const turbo_flow_security_request_t *request,
                                                  uint64_t now_epoch_seconds,
                                                  turbo_flow_security_decision_t *decision) {
  turbo_flow_security_decision_t result = TURBO_FLOW_SECURITY_DECISION_INIT;
  flow_security_policy_snapshot_t *snapshot = NULL;
  size_t resource_size;
  size_t deny_rule = SIZE_MAX;
  size_t allow_rule = SIZE_MAX;
  int rc = TURBO_OK;
  resource_size = strlen(request->resource);
  snapshot = flow_security_policy_snapshot_acquire(realm);
  result.policy_version = snapshot ? snapshot->policy_version : 0u;
  if (request->principal->scope != TURBO_FLOW_SECURITY_SCOPE_SYSTEM &&
      (request->domain_id[0] == '\0' ||
       strcmp(request->principal->domain_id, request->domain_id) != 0)) {
    result.reason = TURBO_FLOW_SECURITY_REASON_DOMAIN_MISMATCH;
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
  if (!snapshot || request->principal->policy_version != snapshot->policy_version ||
      (snapshot->expires_at != 0u &&
       (now_epoch_seconds == 0u || now_epoch_seconds >= snapshot->expires_at))) {
    result.reason = TURBO_FLOW_SECURITY_REASON_POLICY_VERSION_MISMATCH;
    goto complete;
  }
  {
    tstr_v root_key = tstr_v_from_buf(request->domain_id, strlen(request->domain_id));
    flow_security_root_index_t **root =
        (flow_security_root_index_t **)turbo_hash_map_get(&snapshot->root_index, &root_key);
    flow_security_rule_bucket_t *bucket =
        root && *root ? &(*root)->buckets[flow_security_action_index(request->action)]
                                         [request->resource_type - 1u]
                      : NULL;
    if (bucket && bucket->initialized) {
      rc = flow_security_evaluate_subject(realm, snapshot, request, bucket,
                                          TURBO_FLOW_SECURITY_SUBJECT_ANY, "", resource_size,
                                          &deny_rule, &allow_rule);
      if (rc != TURBO_OK) goto complete;
      rc = flow_security_evaluate_subject(
          realm, snapshot, request, bucket, TURBO_FLOW_SECURITY_SUBJECT_PRINCIPAL,
          request->principal->principal_id, resource_size, &deny_rule, &allow_rule);
      if (rc != TURBO_OK) goto complete;
      for (uint32_t i = 0u; i < request->principal->role_count; ++i) {
        rc = flow_security_evaluate_subject(
            realm, snapshot, request, bucket, TURBO_FLOW_SECURITY_SUBJECT_ROLE,
            request->principal->roles[i], resource_size, &deny_rule, &allow_rule);
        if (rc != TURBO_OK) goto complete;
      }
      for (uint32_t i = 0u; i < request->principal->group_count; ++i) {
        rc = flow_security_evaluate_subject(
            realm, snapshot, request, bucket, TURBO_FLOW_SECURITY_SUBJECT_GROUP,
            request->principal->groups[i], resource_size, &deny_rule, &allow_rule);
        if (rc != TURBO_OK) goto complete;
      }
    }
  }
  if (deny_rule != SIZE_MAX) {
    result.reason = TURBO_FLOW_SECURITY_REASON_DENY_RULE;
    result.matched_rule = deny_rule;
    goto complete;
  }
  if (allow_rule != SIZE_MAX) {
    result.effect = TURBO_FLOW_SECURITY_ALLOW;
    result.reason = TURBO_FLOW_SECURITY_REASON_ALLOW_RULE;
    result.matched_rule = allow_rule;
  }

complete:
  flow_security_policy_snapshot_release(snapshot);
  flow_security_record(realm, rc, &result);
  *decision = result;
  return rc;
}

int turbo_flow_security_realm_evaluate(turbo_flow_security_realm_t *realm,
                                       const turbo_flow_security_request_t *request,
                                       uint64_t now_epoch_seconds,
                                       turbo_flow_security_decision_t *decision) {
  if (!flow_security_request_valid(realm, request, decision)) return TURBO_EINVAL;
  return flow_security_realm_evaluate_validated(realm, request, now_epoch_seconds, decision);
}

int turbo_flow_security_realm_authorize(turbo_flow_security_realm_t *realm,
                                        const turbo_flow_security_request_t *request,
                                        uint64_t now_epoch_seconds,
                                        turbo_flow_security_decision_t *decision) {
  flow_security_policy_snapshot_t *snapshot;
  int needs_refresh = 0;
  int rc;
  if (!flow_security_request_valid(realm, request, decision)) return TURBO_EINVAL;
  if (realm->policy_source) {
    snapshot = flow_security_policy_snapshot_acquire(realm);
    needs_refresh = !snapshot || snapshot->policy_version != request->principal->policy_version ||
                    (snapshot->expires_at != 0u &&
                     (now_epoch_seconds == 0u || now_epoch_seconds >= snapshot->expires_at));
    flow_security_policy_snapshot_release(snapshot);
  }
  if (needs_refresh) {
    rc = turbo_flow_security_realm_refresh(realm, request->principal->policy_version,
                                           now_epoch_seconds);
    if (rc != TURBO_OK) {
      if (decision && decision->size >= sizeof(*decision))
        *decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
      return rc;
    }
  }
  rc = flow_security_realm_evaluate_validated(realm, request, now_epoch_seconds, decision);
  return rc == TURBO_OK && decision->effect == TURBO_FLOW_SECURITY_DENY ? TURBO_EPERM : rc;
}

static int flow_security_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  const turbo_flow_security_realm_t *realm = (const turbo_flow_security_realm_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  flow_security_policy_snapshot_t *snapshot;
  int written;
  if (!realm || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  metadata.domain = TURBO_FLOW_DOMAIN_RULES;
  metadata.kind = TURBO_FLOW_RESOURCE_SECURITY_REALM;
  snapshot = flow_security_policy_snapshot_acquire((turbo_flow_security_realm_t *)realm);
  metadata.generation = snapshot ? snapshot->policy_version : 1u;
  metadata.observed_generation = snapshot ? snapshot->policy_version : 0u;
  flow_security_policy_snapshot_release(snapshot);
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
  flow_security_policy_snapshot_t *snapshot;
  char payload[512];
  int written;
  int rc;
  if (document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return TURBO_ENOTSUP;
  if (!realm || !out) return TURBO_EINVAL;
  rc = flow_security_resource_metadata(realm, &metadata);
  if (rc != TURBO_OK) return rc;
  snapshot = flow_security_policy_snapshot_acquire(realm);
  written =
      snprintf(payload, sizeof(payload),
               "{\"policy_version\":\"%llu\",\"rule_count\":\"%llu\","
               "\"evaluations\":\"%llu\",\"allowed\":\"%llu\",\"denied\":\"%llu\","
               "\"failures\":\"%llu\",\"last_status\":%d}",
               (unsigned long long)(snapshot ? snapshot->policy_version : 0u),
               (unsigned long long)(snapshot ? turbo_vec_size(&snapshot->rules) : 0u),
               (unsigned long long)atomic_load_explicit(&realm->evaluations, memory_order_relaxed),
               (unsigned long long)atomic_load_explicit(&realm->allowed, memory_order_relaxed),
               (unsigned long long)atomic_load_explicit(&realm->denied, memory_order_relaxed),
               (unsigned long long)atomic_load_explicit(&realm->failures, memory_order_relaxed),
               atomic_load_explicit(&realm->last_status, memory_order_acquire));
  flow_security_policy_snapshot_release(snapshot);
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

int turbo_flow_security_realm_create_resolved(const turbo_flow_resolved_config_t *resolved,
                                              const char *channel_name,
                                              const turbo_flow_security_matcher_t *matcher,
                                              turbo_flow_security_realm_t **out,
                                              turbo_flow_config_error_t *error) {
  static const char *const allowed[] = {"resource_uid", "owner_name", "policy_source"};
  turbo_json_doc_t *document = NULL;
  turbo_flow_security_realm_config_t config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
  json_value_t *channels;
  json_value_t *channel;
  json_value_t *kind;
  json_value_t *fields;
  const char *json;
  size_t json_size = 0u;
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
  config.policy_source = flow_security_json_string(fields, "policy_source");
  if (!config.resource_uid || !config.resource_uid[0] || !config.owner_name ||
      !config.owner_name[0] || !config.policy_source || !config.policy_source[0]) {
    rc = flow_security_config_error(error, TURBO_EINVAL, channel_name, NULL,
                                    "resource_uid, owner_name, and policy_source are required");
    goto done;
  }
  if (matcher) config.matcher = *matcher;
  rc = turbo_flow_security_realm_create(&config, out);
  if (rc != TURBO_OK)
    rc =
        flow_security_config_error(error, rc, channel_name, NULL, "security realm creation failed");

done:
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

int turbo_flow_security_auth_provider_owner_create_resolved(
    const turbo_flow_security_auth_provider_factory_t *factory,
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider,
    turbo_flow_security_auth_provider_owner_t *owner_out, turbo_flow_config_error_t *error) {
  turbo_flow_security_auth_provider_owner_t owner = TURBO_FLOW_SECURITY_AUTH_PROVIDER_OWNER_INIT;
  int rc;
  if (owner_out && owner_out->size >= sizeof(*owner_out))
    *owner_out =
        (turbo_flow_security_auth_provider_owner_t)TURBO_FLOW_SECURITY_AUTH_PROVIDER_OWNER_INIT;
  if (!factory || factory->size < sizeof(*factory) ||
      factory->abi_version != TURBO_FLOW_SECURITY_ABI_V3 || !factory->backend ||
      !factory->backend[0] || !factory->create_resolved || !resolved || !channel_name ||
      !channel_name[0] || !owner_out || owner_out->size < sizeof(*owner_out) || !error ||
      error->size < sizeof(*error))
    return TURBO_EINVAL;
  rc = factory->create_resolved(factory->ctx, resolved, channel_name, key_provider, &owner, error);
  if (rc != TURBO_OK) {
    if (owner.owner && owner.destroy) owner.destroy(owner.owner);
    return rc;
  }
  if (owner.size < sizeof(owner) || owner.abi_version != TURBO_FLOW_SECURITY_ABI_V3 ||
      !owner.backend || strcmp(owner.backend, factory->backend) != 0 || !owner.method ||
      !owner.method[0] || !owner.provider || owner.provider->size < sizeof(*owner.provider) ||
      !owner.provider->authenticate ||
      (owner.enhanced_provider &&
       (owner.enhanced_provider->size < sizeof(*owner.enhanced_provider) ||
        !owner.enhanced_provider->begin || !owner.enhanced_provider->continue_exchange ||
        !owner.enhanced_provider->cancel)) ||
      !owner.owner || !owner.destroy) {
    if (owner.owner && owner.destroy) owner.destroy(owner.owner);
    return TURBO_EPROTO;
  }
  *owner_out = owner;
  return TURBO_OK;
}

void turbo_flow_security_auth_provider_owner_destroy(
    turbo_flow_security_auth_provider_owner_t *owner) {
  if (!owner) return;
  if (owner->size >= sizeof(*owner) && owner->owner && owner->destroy) owner->destroy(owner->owner);
  *owner = (turbo_flow_security_auth_provider_owner_t)TURBO_FLOW_SECURITY_AUTH_PROVIDER_OWNER_INIT;
}

static int flow_security_provider_backend(const turbo_flow_resolved_config_t *resolved,
                                          const char *channel_name, const char *expected_kind,
                                          const char **backend, turbo_flow_config_error_t *error) {
  turbo_flow_resolved_channel_view_t view = TURBO_FLOW_RESOLVED_CHANNEL_VIEW_INIT;
  int rc;
  if (backend) *backend = NULL;
  if (!resolved || !channel_name || !channel_name[0] || !expected_kind || !backend || !error ||
      error->size < sizeof(*error))
    return TURBO_EINVAL;
  rc = turbo_flow_resolved_config_channel(resolved, channel_name, &view);
  if (rc == TURBO_OK && strcmp(view.kind, expected_kind) != 0) rc = TURBO_EINVAL;
  if (rc == TURBO_OK) rc = turbo_flow_resolved_channel_get_string(&view, "backend", backend);
  if (rc == TURBO_OK && (!*backend || !(*backend)[0])) rc = TURBO_EINVAL;
  if (rc != TURBO_OK)
    return flow_security_config_error(error, rc, channel_name, "backend",
                                      "provider channel kind or backend is invalid");
  return TURBO_OK;
}

int turbo_flow_security_auth_provider_owner_create_registered(
    const turbo_flow_security_auth_provider_factory_t *const *factories, size_t factory_count,
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider,
    turbo_flow_security_auth_provider_owner_t *owner_out, turbo_flow_config_error_t *error) {
  const turbo_flow_security_auth_provider_factory_t *selected = NULL;
  const char *backend;
  int rc;
  if (owner_out && owner_out->size >= sizeof(*owner_out))
    *owner_out =
        (turbo_flow_security_auth_provider_owner_t)TURBO_FLOW_SECURITY_AUTH_PROVIDER_OWNER_INIT;
  if (!factories || factory_count == 0u || !owner_out || owner_out->size < sizeof(*owner_out))
    return TURBO_EINVAL;
  rc = flow_security_provider_backend(resolved, channel_name, "auth_provider", &backend, error);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < factory_count; ++i) {
    const turbo_flow_security_auth_provider_factory_t *factory = factories[i];
    if (!factory || factory->size < sizeof(*factory) ||
        factory->abi_version != TURBO_FLOW_SECURITY_ABI_V3 || !factory->backend ||
        !factory->backend[0] || !factory->create_resolved) {
      return flow_security_config_error(error, TURBO_EINVAL, channel_name, "backend",
                                        "authentication provider registry is invalid");
    }
    for (size_t j = 0u; j < i; ++j)
      if (strcmp(factories[j]->backend, factory->backend) == 0)
        return flow_security_config_error(error, TURBO_EALREADY, channel_name, "backend",
                                          "authentication provider backend is ambiguous");
    if (strcmp(factory->backend, backend) == 0) selected = factory;
  }
  if (!selected)
    return flow_security_config_error(error, TURBO_ENOTSUP, channel_name, "backend",
                                      "authentication provider backend is not registered");
  return turbo_flow_security_auth_provider_owner_create_resolved(selected, resolved, channel_name,
                                                                 key_provider, owner_out, error);
}

int turbo_flow_security_policy_provider_owner_create_resolved(
    const turbo_flow_security_policy_provider_factory_t *factory,
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider,
    turbo_flow_security_policy_provider_owner_t *owner_out, turbo_flow_config_error_t *error) {
  turbo_flow_security_policy_provider_owner_t owner =
      TURBO_FLOW_SECURITY_POLICY_PROVIDER_OWNER_INIT;
  int rc;
  if (owner_out && owner_out->size >= sizeof(*owner_out))
    *owner_out =
        (turbo_flow_security_policy_provider_owner_t)TURBO_FLOW_SECURITY_POLICY_PROVIDER_OWNER_INIT;
  if (!factory || factory->size < sizeof(*factory) ||
      factory->abi_version != TURBO_FLOW_SECURITY_ABI_V3 || !factory->backend ||
      !factory->backend[0] || !factory->create_resolved || !resolved || !channel_name ||
      !channel_name[0] || !owner_out || owner_out->size < sizeof(*owner_out) || !error ||
      error->size < sizeof(*error))
    return TURBO_EINVAL;
  rc = factory->create_resolved(factory->ctx, resolved, channel_name, key_provider, &owner, error);
  if (rc != TURBO_OK) {
    if (owner.owner && owner.destroy) owner.destroy(owner.owner);
    return rc;
  }
  if (owner.size < sizeof(owner) || owner.abi_version != TURBO_FLOW_SECURITY_ABI_V3 ||
      !owner.backend || strcmp(owner.backend, factory->backend) != 0 || !owner.provider ||
      owner.provider->size < sizeof(*owner.provider) || !owner.provider->load ||
      !owner.provider->release || !owner.owner || !owner.destroy) {
    if (owner.owner && owner.destroy) owner.destroy(owner.owner);
    return TURBO_EPROTO;
  }
  *owner_out = owner;
  return TURBO_OK;
}

int turbo_flow_security_policy_provider_owner_create_registered(
    const turbo_flow_security_policy_provider_factory_t *const *factories, size_t factory_count,
    const turbo_flow_resolved_config_t *resolved, const char *channel_name,
    const turbo_flow_security_key_provider_t *key_provider,
    turbo_flow_security_policy_provider_owner_t *owner_out, turbo_flow_config_error_t *error) {
  const turbo_flow_security_policy_provider_factory_t *selected = NULL;
  const char *backend;
  int rc;
  if (owner_out && owner_out->size >= sizeof(*owner_out))
    *owner_out =
        (turbo_flow_security_policy_provider_owner_t)TURBO_FLOW_SECURITY_POLICY_PROVIDER_OWNER_INIT;
  if (!factories || factory_count == 0u || !owner_out || owner_out->size < sizeof(*owner_out))
    return TURBO_EINVAL;
  rc = flow_security_provider_backend(resolved, channel_name, "acl_provider", &backend, error);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < factory_count; ++i) {
    const turbo_flow_security_policy_provider_factory_t *factory = factories[i];
    if (!factory || factory->size < sizeof(*factory) ||
        factory->abi_version != TURBO_FLOW_SECURITY_ABI_V3 || !factory->backend ||
        !factory->backend[0] || !factory->create_resolved) {
      return flow_security_config_error(error, TURBO_EINVAL, channel_name, "backend",
                                        "ACL provider registry is invalid");
    }
    for (size_t j = 0u; j < i; ++j)
      if (strcmp(factories[j]->backend, factory->backend) == 0)
        return flow_security_config_error(error, TURBO_EALREADY, channel_name, "backend",
                                          "ACL provider backend is ambiguous");
    if (strcmp(factory->backend, backend) == 0) selected = factory;
  }
  if (!selected)
    return flow_security_config_error(error, TURBO_ENOTSUP, channel_name, "backend",
                                      "ACL provider backend is not registered");
  return turbo_flow_security_policy_provider_owner_create_resolved(selected, resolved, channel_name,
                                                                   key_provider, owner_out, error);
}

void turbo_flow_security_policy_provider_owner_destroy(
    turbo_flow_security_policy_provider_owner_t *owner) {
  if (!owner) return;
  if (owner->size >= sizeof(*owner) && owner->owner && owner->destroy) owner->destroy(owner->owner);
  *owner =
      (turbo_flow_security_policy_provider_owner_t)TURBO_FLOW_SECURITY_POLICY_PROVIDER_OWNER_INIT;
}
