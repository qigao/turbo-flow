#include "flow_internal.h"

#include "fmt.h"

#include <stdio.h>
#include <string.h>

#define FLOW_STRINGIFY_IMPL(value) #value
#define FLOW_STRINGIFY(value) FLOW_STRINGIFY_IMPL(value)
#define FLOW_JOIN_IMPL(left, right) left##right
#define FLOW_JOIN(left, right) FLOW_JOIN_IMPL(left, right)

#define FLOW_GOVERNANCE_SCHEMA_TEXT(schema_name, schema_id, type_prefix)                           \
  "schema " FLOW_STRINGIFY(schema_name) " [id(" FLOW_STRINGIFY(                                    \
      schema_id) "), version(1)];\n"                                                               \
                 "message " FLOW_STRINGIFY(FLOW_JOIN(                                              \
                     type_prefix,                                                                  \
                     Spec)) " {\n"                                                                 \
                            "  uint32 domain;\n"                                                   \
                            "  uint32 kind;\n"                                                     \
                            "  string capacity;\n"                                                 \
                            "  bool capacity_bounded;\n"                                           \
                            "}\n"                                                                  \
                            "message " FLOW_STRINGIFY(FLOW_JOIN(                                   \
                                type_prefix,                                                       \
                                Conditions)) " {\n"                                                \
                                             "  uint32 ready_status;\n"                            \
                                             "  uint32 ready_reason;\n"                            \
                                             "  uint32 accepting_status;\n"                        \
                                             "  uint32 accepting_reason;\n"                        \
                                             "  uint32 drained_status;\n"                          \
                                             "  uint32 drained_reason;\n"                          \
                                             "  uint32 saturated_status;\n"                        \
                                             "  uint32 saturated_reason;\n"                        \
                                             "}\n"                                                 \
                                             "message " FLOW_STRINGIFY(FLOW_JOIN(                  \
                                                 type_prefix,                                      \
                                                 Event)) " {\n"                                    \
                                                         "  string sequence;\n"                    \
                                                         "  string generation;\n"                  \
                                                         "  string observed_generation;\n"         \
                                                         "  bool gap;\n"                           \
                                                         "  int32 last_status;\n"                  \
                                                         "  uint32 reason;\n"                      \
                                                         "}\n"

#define FLOW_DEFINE_GOVERNANCE_SCHEMA(symbol, domain_value, kind_value, schema_name, schema_id,    \
                                      type_prefix)                                                 \
  static const char symbol##_TEXT[] =                                                              \
      FLOW_GOVERNANCE_SCHEMA_TEXT(schema_name, schema_id, type_prefix);                            \
  static const turbo_flow_resource_schema_t symbol##_SPEC = {                                      \
      sizeof(turbo_flow_resource_schema_t),                                                        \
      domain_value,                                                                                \
      kind_value,                                                                                  \
      TURBO_FLOW_RESOURCE_DOCUMENT_SPEC,                                                           \
      TURBO_FLOW_RESOURCE_DOCUMENT_JSON,                                                           \
      FLOW_STRINGIFY(schema_name),                                                                 \
      FLOW_STRINGIFY(FLOW_JOIN(type_prefix, Spec)),                                                \
      schema_id,                                                                                   \
      1u,                                                                                          \
      symbol##_TEXT};                                                                              \
  static const turbo_flow_resource_schema_t symbol##_CONDITIONS = {                                \
      sizeof(turbo_flow_resource_schema_t),                                                        \
      domain_value,                                                                                \
      kind_value,                                                                                  \
      TURBO_FLOW_RESOURCE_DOCUMENT_CONDITIONS,                                                     \
      TURBO_FLOW_RESOURCE_DOCUMENT_JSON,                                                           \
      FLOW_STRINGIFY(schema_name),                                                                 \
      FLOW_STRINGIFY(FLOW_JOIN(type_prefix, Conditions)),                                          \
      schema_id,                                                                                   \
      1u,                                                                                          \
      symbol##_TEXT};                                                                              \
  static const turbo_flow_resource_schema_t symbol##_EVENT = {                                     \
      sizeof(turbo_flow_resource_schema_t),                                                        \
      domain_value,                                                                                \
      kind_value,                                                                                  \
      TURBO_FLOW_RESOURCE_DOCUMENT_EVENT,                                                          \
      TURBO_FLOW_RESOURCE_DOCUMENT_JSON,                                                           \
      FLOW_STRINGIFY(schema_name),                                                                 \
      FLOW_STRINGIFY(FLOW_JOIN(type_prefix, Event)),                                               \
      schema_id,                                                                                   \
      1u,                                                                                          \
      symbol##_TEXT}

FLOW_DEFINE_GOVERNANCE_SCHEMA(FLOW_CONNECTION_GOVERNANCE, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                              TURBO_FLOW_RESOURCE_CONNECTION, TurboFlowConnectionGovernance, 1001,
                              Connection);
FLOW_DEFINE_GOVERNANCE_SCHEMA(FLOW_QUEUE_GOVERNANCE, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
                              TURBO_FLOW_RESOURCE_QUEUE_BUFFER, TurboFlowQueueGovernance, 1002,
                              Queue);
FLOW_DEFINE_GOVERNANCE_SCHEMA(FLOW_POOL_GOVERNANCE, TURBO_FLOW_DOMAIN_EXECUTION,
                              TURBO_FLOW_RESOURCE_POOL, TurboFlowPoolGovernance, 1003, Pool);
FLOW_DEFINE_GOVERNANCE_SCHEMA(FLOW_RUNTIME_GOVERNANCE, TURBO_FLOW_DOMAIN_MANAGEMENT,
                              TURBO_FLOW_RESOURCE_RUNTIME, TurboFlowRuntimeGovernance, 1004,
                              Runtime);
FLOW_DEFINE_GOVERNANCE_SCHEMA(FLOW_SEGMENT_GOVERNANCE, TURBO_FLOW_DOMAIN_EXECUTION,
                              TURBO_FLOW_RESOURCE_SEGMENT, TurboFlowSegmentGovernance, 1005,
                              Segment);
FLOW_DEFINE_GOVERNANCE_SCHEMA(FLOW_PROTOCOL_GOVERNANCE, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                              TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE, TurboFlowProtocolGovernance,
                              1006, Protocol);
FLOW_DEFINE_GOVERNANCE_SCHEMA(FLOW_STORAGE_GOVERNANCE, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
                              TURBO_FLOW_RESOURCE_STORAGE, TurboFlowStorageGovernance, 1007,
                              Storage);
FLOW_DEFINE_GOVERNANCE_SCHEMA(FLOW_RULE_SET_GOVERNANCE, TURBO_FLOW_DOMAIN_RULES,
                              TURBO_FLOW_RESOURCE_RULE_SET, TurboFlowRuleSetGovernance, 1008,
                              RuleSet);
FLOW_DEFINE_GOVERNANCE_SCHEMA(FLOW_SECURITY_REALM_GOVERNANCE, TURBO_FLOW_DOMAIN_RULES,
                              TURBO_FLOW_RESOURCE_SECURITY_REALM, TurboFlowSecurityRealmGovernance,
                              1009, SecurityRealm);

#undef FLOW_DEFINE_GOVERNANCE_SCHEMA
#undef FLOW_GOVERNANCE_SCHEMA_TEXT
#undef FLOW_JOIN
#undef FLOW_JOIN_IMPL
#undef FLOW_STRINGIFY
#undef FLOW_STRINGIFY_IMPL

static void flow_resource_document_payload_free(void *data, void *ctx) {
  (void)ctx;
  tstr_free((tstr)data);
}

static int flow_resource_document_kind_valid(turbo_flow_resource_document_kind_t kind) {
  return kind >= TURBO_FLOW_RESOURCE_DOCUMENT_SPEC && kind <= TURBO_FLOW_RESOURCE_DOCUMENT_EVENT;
}

static int flow_resource_schema_valid(const turbo_flow_resource_schema_t *schema) {
  return schema && schema->size >= sizeof(*schema) && schema->domain > TURBO_FLOW_DOMAIN_NONE &&
         schema->domain <= TURBO_FLOW_DOMAIN_MANAGEMENT &&
         schema->resource_kind >= TURBO_FLOW_RESOURCE_CONNECTION &&
         schema->resource_kind <= TURBO_FLOW_RESOURCE_SECURITY_REALM &&
         flow_resource_document_kind_valid(schema->document_kind) &&
         schema->encoding >= TURBO_FLOW_RESOURCE_DOCUMENT_JSON &&
         schema->encoding <= TURBO_FLOW_RESOURCE_DOCUMENT_TBE && schema->schema_name &&
         schema->schema_name[0] != '\0' && schema->type_name && schema->type_name[0] != '\0' &&
         schema->schema_id != 0u && schema->schema_version != 0u && schema->schema_text &&
         schema->schema_text[0] != '\0';
}

static const turbo_flow_resource_schema_t *
flow_governance_schema_for_kind(turbo_flow_resource_kind_t resource_kind,
                                turbo_flow_resource_document_kind_t document_kind) {
#define FLOW_SELECT_GOVERNANCE_SCHEMA(symbol)                                                      \
  do {                                                                                             \
    if (document_kind == TURBO_FLOW_RESOURCE_DOCUMENT_SPEC) return &symbol##_SPEC;                 \
    if (document_kind == TURBO_FLOW_RESOURCE_DOCUMENT_CONDITIONS) return &symbol##_CONDITIONS;     \
    if (document_kind == TURBO_FLOW_RESOURCE_DOCUMENT_EVENT) return &symbol##_EVENT;               \
    return NULL;                                                                                   \
  } while (0)
  switch (resource_kind) {
  case TURBO_FLOW_RESOURCE_CONNECTION:
    FLOW_SELECT_GOVERNANCE_SCHEMA(FLOW_CONNECTION_GOVERNANCE);
  case TURBO_FLOW_RESOURCE_QUEUE_BUFFER:
    FLOW_SELECT_GOVERNANCE_SCHEMA(FLOW_QUEUE_GOVERNANCE);
  case TURBO_FLOW_RESOURCE_POOL:
    FLOW_SELECT_GOVERNANCE_SCHEMA(FLOW_POOL_GOVERNANCE);
  case TURBO_FLOW_RESOURCE_RUNTIME:
    FLOW_SELECT_GOVERNANCE_SCHEMA(FLOW_RUNTIME_GOVERNANCE);
  case TURBO_FLOW_RESOURCE_SEGMENT:
    FLOW_SELECT_GOVERNANCE_SCHEMA(FLOW_SEGMENT_GOVERNANCE);
  case TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE:
    FLOW_SELECT_GOVERNANCE_SCHEMA(FLOW_PROTOCOL_GOVERNANCE);
  case TURBO_FLOW_RESOURCE_STORAGE:
    FLOW_SELECT_GOVERNANCE_SCHEMA(FLOW_STORAGE_GOVERNANCE);
  case TURBO_FLOW_RESOURCE_RULE_SET:
    FLOW_SELECT_GOVERNANCE_SCHEMA(FLOW_RULE_SET_GOVERNANCE);
  case TURBO_FLOW_RESOURCE_SECURITY_REALM:
    FLOW_SELECT_GOVERNANCE_SCHEMA(FLOW_SECURITY_REALM_GOVERNANCE);
  default:
    return NULL;
  }
#undef FLOW_SELECT_GOVERNANCE_SCHEMA
}

const turbo_flow_resource_schema_t *
turbo_flow_resource_governance_schema(turbo_flow_domain_t domain,
                                      turbo_flow_resource_kind_t resource_kind,
                                      turbo_flow_resource_document_kind_t document_kind) {
  const turbo_flow_resource_schema_t *schema =
      flow_governance_schema_for_kind(resource_kind, document_kind);
  return schema && schema->domain == domain ? schema : NULL;
}

static int flow_resource_snapshot_matches(const turbo_flow_resource_snapshot_t *snapshot,
                                          const turbo_flow_resource_metadata_t *metadata) {
  return snapshot && snapshot->size >= sizeof(*snapshot) && snapshot->domain == metadata->domain &&
         snapshot->kind == metadata->kind && strcmp(snapshot->uid, metadata->uid) == 0 &&
         strcmp(snapshot->owner_name, metadata->owner_name) == 0 &&
         snapshot->generation == metadata->generation &&
         snapshot->observed_generation == metadata->observed_generation &&
         snapshot->observed_generation <= snapshot->generation;
}

static turbo_flow_resource_condition_reason_t
flow_resource_event_reason(const turbo_flow_resource_snapshot_t *snapshot) {
  if (snapshot->last_status != TURBO_OK) return TURBO_FLOW_RESOURCE_REASON_OWNER_ERROR;
  if (snapshot->observed_generation < snapshot->generation) {
    return TURBO_FLOW_RESOURCE_REASON_OBSERVATION_LAGGING;
  }
  return TURBO_FLOW_RESOURCE_REASON_OBSERVATION_CURRENT;
}

static int flow_resource_governance_document(const turbo_flow_resource_metadata_t *metadata,
                                             const turbo_flow_resource_snapshot_t *snapshot,
                                             turbo_flow_resource_document_kind_t document_kind,
                                             turbo_flow_resource_document_t *out) {
  const turbo_flow_resource_schema_t *schema;
  tstr payload = NULL;
  int ready;
  int accepting;
  int drained;
  int saturated;
  int rc;
  if (!flow_resource_metadata_valid(metadata) ||
      !flow_resource_snapshot_matches(snapshot, metadata)) {
    return TURBO_EPROTO;
  }
  schema = turbo_flow_resource_governance_schema(metadata->domain, metadata->kind, document_kind);
  if (!schema) {
    return flow_governance_schema_for_kind(metadata->kind, document_kind) ? TURBO_EPROTO
                                                                          : TURBO_ENOTSUP;
  }
  ready = snapshot->last_status == TURBO_OK;
  saturated = snapshot->saturated != 0;
  accepting = ready && !saturated;
  drained = snapshot->load == 0u;
  if (document_kind == TURBO_FLOW_RESOURCE_DOCUMENT_SPEC) {
    payload = tstr_format("{\"domain\":{},\"kind\":{},\"capacity\":\"{}\","
                          "\"capacity_bounded\":{}}",
                          (unsigned)metadata->domain, (unsigned)metadata->kind, snapshot->capacity,
                          snapshot->capacity == 0u ? "false" : "true");
  } else if (document_kind == TURBO_FLOW_RESOURCE_DOCUMENT_CONDITIONS) {
    payload = tstr_format(
        "{\"ready_status\":{},\"ready_reason\":{},"
        "\"accepting_status\":{},\"accepting_reason\":{},"
        "\"drained_status\":{},\"drained_reason\":{},"
        "\"saturated_status\":{},\"saturated_reason\":{}}",
        ready ? TURBO_FLOW_CONDITION_TRUE : TURBO_FLOW_CONDITION_FALSE,
        ready ? TURBO_FLOW_RESOURCE_REASON_RUNNING : TURBO_FLOW_RESOURCE_REASON_NOT_RUNNING,
        accepting ? TURBO_FLOW_CONDITION_TRUE : TURBO_FLOW_CONDITION_FALSE,
        accepting ? TURBO_FLOW_RESOURCE_REASON_ACCEPTING : TURBO_FLOW_RESOURCE_REASON_NOT_ACCEPTING,
        drained ? TURBO_FLOW_CONDITION_TRUE : TURBO_FLOW_CONDITION_FALSE,
        drained ? TURBO_FLOW_RESOURCE_REASON_DRAINED : TURBO_FLOW_RESOURCE_REASON_WORK_PENDING,
        saturated ? TURBO_FLOW_CONDITION_TRUE : TURBO_FLOW_CONDITION_FALSE,
        saturated ? TURBO_FLOW_RESOURCE_REASON_CAPACITY_EXHAUSTED
                  : TURBO_FLOW_RESOURCE_REASON_CAPACITY_AVAILABLE);
  } else if (document_kind == TURBO_FLOW_RESOURCE_DOCUMENT_EVENT) {
    payload = tstr_format("{\"sequence\":\"{}\",\"generation\":\"{}\","
                          "\"observed_generation\":\"{}\",\"gap\":{},"
                          "\"last_status\":{},\"reason\":{}}",
                          snapshot->observed_generation, snapshot->generation,
                          snapshot->observed_generation,
                          snapshot->observed_generation < snapshot->generation ? "true" : "false",
                          snapshot->last_status, (unsigned)flow_resource_event_reason(snapshot));
  }
  if (!payload) return TURBO_ENOMEM;
  rc = turbo_flow_resource_document_set_payload_copy(out, metadata, schema, payload,
                                                     tstr_len(payload));
  tstr_free(payload);
  return rc;
}

void turbo_flow_resource_document_cleanup(turbo_flow_resource_document_t *document) {
  if (!document) return;
  mem_buffer_release(document->payload);
  *document = (turbo_flow_resource_document_t)TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
}

int turbo_flow_resource_document_set_payload_copy(turbo_flow_resource_document_t *document,
                                                  const turbo_flow_resource_metadata_t *metadata,
                                                  const turbo_flow_resource_schema_t *schema,
                                                  const void *payload, size_t payload_size) {
  turbo_flow_resource_document_t result = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
  tstr copy;
  int written;
  if (!document || document->size < sizeof(*document) || document->payload ||
      !flow_resource_metadata_valid(metadata) || !flow_resource_schema_valid(schema) ||
      schema->domain != metadata->domain || schema->resource_kind != metadata->kind ||
      payload_size > TURBO_FLOW_RESOURCE_DOCUMENT_MAX_BYTES || (payload_size > 0u && !payload)) {
    return TURBO_EINVAL;
  }
  copy = tstr_new_len(payload ? payload : "", payload_size);
  if (!copy) return TURBO_ENOMEM;
  result.payload = mem_wrap_external(copy, payload_size, flow_resource_document_payload_free, NULL);
  if (!result.payload) {
    tstr_free(copy);
    return TURBO_ENOMEM;
  }
  result.domain = metadata->domain;
  result.resource_kind = metadata->kind;
  result.document_kind = schema->document_kind;
  result.generation = metadata->generation;
  result.observed_generation = metadata->observed_generation;
  result.schema = schema;
  written = snprintf(result.uid, sizeof(result.uid), "%s", metadata->uid);
  if (written < 0 || (size_t)written >= sizeof(result.uid)) {
    turbo_flow_resource_document_cleanup(&result);
    return TURBO_ENAMETOOLONG;
  }
  written = snprintf(result.owner_name, sizeof(result.owner_name), "%s", metadata->owner_name);
  if (written < 0 || (size_t)written >= sizeof(result.owner_name)) {
    turbo_flow_resource_document_cleanup(&result);
    return TURBO_ENAMETOOLONG;
  }
  memcpy(document, &result, sizeof(result));
  return TURBO_OK;
}

int turbo_flow_resource_document_validate(const turbo_flow_resource_document_t *document,
                                          const turbo_flow_resource_schema_t *expected_schema) {
  const turbo_flow_resource_schema_t *actual;
  if (!document || document->size < sizeof(*document) || !document->payload ||
      mem_buffer_used(document->payload) > TURBO_FLOW_RESOURCE_DOCUMENT_MAX_BYTES ||
      !flow_resource_schema_valid(expected_schema)) {
    return TURBO_EINVAL;
  }
  actual = document->schema;
  if (!flow_resource_schema_valid(actual) || document->domain != expected_schema->domain ||
      document->resource_kind != expected_schema->resource_kind ||
      document->document_kind != expected_schema->document_kind ||
      actual->domain != expected_schema->domain ||
      actual->resource_kind != expected_schema->resource_kind ||
      actual->document_kind != expected_schema->document_kind ||
      actual->encoding != expected_schema->encoding ||
      actual->schema_id != expected_schema->schema_id ||
      actual->schema_version != expected_schema->schema_version ||
      strcmp(actual->schema_name, expected_schema->schema_name) != 0 ||
      strcmp(actual->type_name, expected_schema->type_name) != 0) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flow_resource_document_matches(const turbo_flow_resource_document_t *document,
                                          const turbo_flow_resource_metadata_t *metadata,
                                          turbo_flow_resource_document_kind_t kind) {
  return document && document->size >= sizeof(*document) && document->payload &&
         mem_buffer_used(document->payload) <= TURBO_FLOW_RESOURCE_DOCUMENT_MAX_BYTES &&
         document->domain == metadata->domain && document->resource_kind == metadata->kind &&
         document->document_kind == kind && strcmp(document->uid, metadata->uid) == 0 &&
         strcmp(document->owner_name, metadata->owner_name) == 0 &&
         document->generation == metadata->generation &&
         document->observed_generation == metadata->observed_generation &&
         document->observed_generation <= document->generation &&
         flow_resource_schema_valid(document->schema) &&
         document->schema->domain == document->domain &&
         document->schema->resource_kind == document->resource_kind &&
         document->schema->document_kind == document->document_kind;
}

int turbo_flow_resource_document_at(const turbo_flow_t *flow, size_t index,
                                    turbo_flow_resource_document_kind_t document_kind,
                                    turbo_flow_resource_document_t *out) {
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  if (!flow || !out || out->size < sizeof(*out) || out->payload ||
      !flow_resource_document_kind_valid(document_kind)) {
    return TURBO_EINVAL;
  }
  for (size_t i = 0; i < turbo_vec_size(&flow->resources); ++i) {
    const flow_resource_registration_t *resource =
        (const flow_resource_registration_t *)turbo_vec_at_const(&flow->resources, i);
    int rc;
    if (index != 0u) {
      --index;
      continue;
    }
    if (!resource) return TURBO_EPROTO;
    rc = resource->ops.metadata(resource->ctx, &metadata);
    if (rc != TURBO_OK) return rc;
    if (!flow_resource_metadata_valid(&metadata) ||
        strcmp(metadata.owner_name, resource->owner_name) != 0) {
      return TURBO_EPROTO;
    }
    if (resource->ops.document) {
      rc = resource->ops.document(resource->ctx, document_kind, out);
      if (rc == TURBO_OK) {
        if (flow_resource_document_matches(out, &metadata, document_kind)) return TURBO_OK;
        turbo_flow_resource_document_cleanup(out);
        return TURBO_EPROTO;
      }
      turbo_flow_resource_document_cleanup(out);
      if (rc != TURBO_ENOTSUP) return rc;
    }
    if (!resource->ops.snapshot) return TURBO_ENOTSUP;
    {
      turbo_flow_resource_snapshot_t snapshot = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
      rc = resource->ops.snapshot(resource->ctx, &snapshot);
      if (rc != TURBO_OK) return rc;
      return flow_resource_governance_document(&metadata, &snapshot, document_kind, out);
    }
  }
  if (index < flow_native_resource_count(flow)) {
    turbo_flow_resource_snapshot_t snapshot = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    int rc = flow_native_resource_document_at(flow, index, document_kind, out);
    if (rc != TURBO_ENOTSUP) return rc;
    rc = flow_native_resource_metadata_at(flow, index, &metadata);
    if (rc != TURBO_OK) return rc;
    rc = flow_native_resource_snapshot_at(flow, index, &snapshot);
    if (rc != TURBO_OK) return rc;
    return flow_resource_governance_document(&metadata, &snapshot, document_kind, out);
  }
  index -= flow_native_resource_count(flow);
  if (document_kind == TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) {
    return turbo_flow_pool_status_document_at(flow, index, out);
  }
  {
    turbo_flow_resource_snapshot_t snapshot = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
    size_t snapshot_index = flow_native_resource_count(flow) + index;
    int uid_written;
    int owner_written;
    int rc;
    for (size_t i = 0; i < turbo_vec_size(&flow->resources); ++i) {
      const flow_resource_registration_t *resource =
          (const flow_resource_registration_t *)turbo_vec_at_const(&flow->resources, i);
      if (resource && resource->ops.snapshot) ++snapshot_index;
    }
    rc = turbo_flow_resource_snapshot_at(flow, snapshot_index, &snapshot);
    if (rc != TURBO_OK) return rc;
    metadata.domain = snapshot.domain;
    metadata.kind = snapshot.kind;
    metadata.generation = snapshot.generation;
    metadata.observed_generation = snapshot.observed_generation;
    uid_written = snprintf(metadata.uid, sizeof(metadata.uid), "%s", snapshot.uid);
    owner_written =
        snprintf(metadata.owner_name, sizeof(metadata.owner_name), "%s", snapshot.owner_name);
    if (uid_written < 0 || (size_t)uid_written >= sizeof(metadata.uid) || owner_written < 0 ||
        (size_t)owner_written >= sizeof(metadata.owner_name))
      return TURBO_EPROTO;
    return flow_resource_governance_document(&metadata, &snapshot, document_kind, out);
  }
}
