#include "data_bind.h"
#include "tinytest.h"
#include "turbo_flow.h"

#include <stdio.h>
#include <string.h>

static int resource_document_noop_stage(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return SALTS_OK;
}

typedef struct resource_metadata_fixture_s {
  turbo_flow_resource_metadata_t metadata;
  int status;
  int calls;
  const turbo_flow_resource_schema_t *document_schema;
  const char *document_payload;
  size_t document_payload_size;
  int document_status;
  int document_calls;
  int corrupt_document_uid;
  uint64_t load;
  uint64_t capacity;
  int saturated;
  int last_status;
  int command_status;
  int command_calls;
  turbo_flow_resource_command_kind_t command_fail_kind;
  turbo_flow_resource_command_kind_t command_kinds[8];
} resource_metadata_fixture_t;

static const char RESOURCE_FIXTURE_SCHEMA_TEXT[] =
    "schema TurboFlowTestResource [id(99), version(1)];\n"
    "message QueueStatus { bool ready; }\n";

static const turbo_flow_resource_schema_t RESOURCE_FIXTURE_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
    TURBO_FLOW_RESOURCE_QUEUE_BUFFER,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowTestResource",
    "QueueStatus",
    99u,
    1u,
    RESOURCE_FIXTURE_SCHEMA_TEXT};

static int resource_metadata_query(void *ctx, turbo_flow_resource_metadata_t *out) {
  resource_metadata_fixture_t *fixture = (resource_metadata_fixture_t *)ctx;
  fixture->calls += 1;
  if (fixture->status != SALTS_OK) return fixture->status;
  *out = fixture->metadata;
  return SALTS_OK;
}

static int resource_document_query(void *ctx, turbo_flow_resource_document_kind_t kind,
                                   turbo_flow_resource_document_t *out) {
  resource_metadata_fixture_t *fixture = (resource_metadata_fixture_t *)ctx;
  int rc;
  fixture->document_calls += 1;
  if (fixture->document_status != SALTS_OK) return fixture->document_status;
  if (kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return SALTS_ENOTSUP;
  rc = turbo_flow_resource_document_set_payload_copy(
      out, &fixture->metadata,
      fixture->document_schema ? fixture->document_schema : &RESOURCE_FIXTURE_SCHEMA,
      fixture->document_payload ? fixture->document_payload : "{\"ready\":true}",
      fixture->document_payload_size ? fixture->document_payload_size
                                     : sizeof("{\"ready\":true}") - 1u);
  if (rc == SALTS_OK && fixture->corrupt_document_uid) out->uid[0] = 'x';
  return rc;
}

static int resource_snapshot_query(void *ctx, turbo_flow_resource_snapshot_t *out) {
  resource_metadata_fixture_t *fixture = (resource_metadata_fixture_t *)ctx;
  int uid_written;
  int owner_written;
  if (!out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  out->domain = fixture->metadata.domain;
  out->kind = fixture->metadata.kind;
  out->generation = fixture->metadata.generation;
  out->observed_generation = fixture->metadata.observed_generation;
  out->load = fixture->load;
  out->capacity = fixture->capacity;
  out->saturated = fixture->saturated;
  out->last_status = fixture->last_status;
  uid_written = snprintf(out->uid, sizeof(out->uid), "%s", fixture->metadata.uid);
  owner_written =
      snprintf(out->owner_name, sizeof(out->owner_name), "%s", fixture->metadata.owner_name);
  if (uid_written < 0 || (size_t)uid_written >= sizeof(out->uid) || owner_written < 0 ||
      (size_t)owner_written >= sizeof(out->owner_name)) {
    return SALTS_ENAMETOOLONG;
  }
  return SALTS_OK;
}

static int resource_command_apply(void *ctx, turbo_flow_t *flow,
                                  const turbo_flow_resource_command_t *command) {
  resource_metadata_fixture_t *fixture = (resource_metadata_fixture_t *)ctx;
  (void)flow;
  if (fixture->command_calls <
      (int)(sizeof(fixture->command_kinds) / sizeof(fixture->command_kinds[0]))) {
    fixture->command_kinds[fixture->command_calls] = command->kind;
  }
  fixture->command_calls += 1;
  if (fixture->command_status != SALTS_OK &&
      (fixture->command_fail_kind == 0 || fixture->command_fail_kind == command->kind)) {
    return fixture->command_status;
  }
  fixture->metadata.generation += 1u;
  fixture->metadata.observed_generation = fixture->metadata.generation;
  return SALTS_OK;
}

static int resource_fixture_register(turbo_flow_t *flow, resource_metadata_fixture_t *fixture,
                                     int with_document, int with_command) {
  turbo_flow_resource_provider_ops_t ops = TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;
  ops.metadata = resource_metadata_query;
  ops.snapshot = resource_snapshot_query;
  ops.document = with_document ? resource_document_query : NULL;
  ops.command = with_command ? resource_command_apply : NULL;
  return turbo_flow_register_resource_provider(flow, fixture->metadata.owner_name, &ops, fixture);
}

static int resource_metadata_find(const turbo_flow_t *flow, turbo_flow_resource_kind_t kind,
                                  const char *owner_name, turbo_flow_resource_metadata_t *out) {
  size_t count;
  if (!flow || !owner_name || !out) return SALTS_EINVAL;
  count = turbo_flow_resource_metadata_count(flow);
  for (size_t i = 0; i < count; ++i) {
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    int rc = turbo_flow_resource_metadata_at(flow, i, &metadata);
    if (rc != SALTS_OK) return rc;
    if (metadata.kind == kind && strcmp(metadata.owner_name, owner_name) == 0) {
      *out = metadata;
      return SALTS_OK;
    }
  }
  return SALTS_ENOENT;
}

spec("Turbo Flow resource document") {
  it("queries schema-backed adapter documents without mutating owner state") {
    turbo_flow_t *flow = turbo_flow_create();
    resource_metadata_fixture_t fixture;
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    uint64_t generation;

    memset(&fixture, 0, sizeof(fixture));
    fixture.metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    fixture.metadata.domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
    fixture.metadata.kind = TURBO_FLOW_RESOURCE_QUEUE_BUFFER;
    memcpy(fixture.metadata.uid, "queue:orders", sizeof("queue:orders"));
    memcpy(fixture.metadata.owner_name, "orders", sizeof("orders"));
    fixture.metadata.generation = 7u;
    fixture.metadata.observed_generation = 7u;
    generation = fixture.metadata.generation;
    check_not_null(flow);
    check_equal(resource_fixture_register(flow, &fixture, 1, 0), SALTS_OK);
    check_equal(
        turbo_flow_resource_document_at(flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
        SALTS_OK);
    check_equal(document.uid, "queue:orders");
    check_equal(document.schema->type_name, "QueueStatus");
    check_equal(fixture.metadata.generation, generation);
    check_equal(fixture.calls, 2);
    check_equal(fixture.document_calls, 1);
    turbo_flow_resource_document_cleanup(&document);
    check_equal(
        turbo_flow_resource_document_at(flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_SPEC, &document),
        SALTS_OK);
    check_equal(document.schema->type_name, "QueueSpec");
    check_equal(fixture.metadata.generation, generation);
    turbo_flow_resource_document_cleanup(&document);
    turbo_flow_destroy(flow);
  }

  it("projects typed governance documents for every canonical resource kind") {
    static const struct {
      turbo_flow_domain_t domain;
      turbo_flow_resource_kind_t kind;
      const char *name;
      uint32_t schema_id;
    } cases[] = {
        {TURBO_FLOW_DOMAIN_IO_TRANSPORT, TURBO_FLOW_RESOURCE_CONNECTION, "connection", 1001u},
        {TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, TURBO_FLOW_RESOURCE_QUEUE_BUFFER, "queue", 1002u},
        {TURBO_FLOW_DOMAIN_EXECUTION, TURBO_FLOW_RESOURCE_POOL, "pool", 1003u},
        {TURBO_FLOW_DOMAIN_MANAGEMENT, TURBO_FLOW_RESOURCE_RUNTIME, "runtime", 1004u},
        {TURBO_FLOW_DOMAIN_EXECUTION, TURBO_FLOW_RESOURCE_SEGMENT, "segment", 1005u},
        {TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN, TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE, "protocol",
         1006u},
        {TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, TURBO_FLOW_RESOURCE_STORAGE, "storage", 1007u},
        {TURBO_FLOW_DOMAIN_RULES, TURBO_FLOW_RESOURCE_RULE_SET, "rules", 1008u},
        {TURBO_FLOW_DOMAIN_RULES, TURBO_FLOW_RESOURCE_SECURITY_REALM, "security", 1009u},
    };
    static const turbo_flow_resource_document_kind_t document_kinds[] = {
        TURBO_FLOW_RESOURCE_DOCUMENT_SPEC, TURBO_FLOW_RESOURCE_DOCUMENT_CONDITIONS,
        TURBO_FLOW_RESOURCE_DOCUMENT_EVENT};
    resource_metadata_fixture_t fixtures[sizeof(cases) / sizeof(cases[0])];
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    memset(fixtures, 0, sizeof(fixtures));
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      int uid_written;
      fixtures[i].metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
      fixtures[i].metadata.domain = cases[i].domain;
      fixtures[i].metadata.kind = cases[i].kind;
      fixtures[i].metadata.generation = 10u + i;
      fixtures[i].metadata.observed_generation =
          i == 5u ? fixtures[i].metadata.generation - 1u : fixtures[i].metadata.generation;
      fixtures[i].capacity = i == 3u ? 0u : 64u;
      fixtures[i].load = i == 1u ? 4u : 0u;
      fixtures[i].saturated = i == 1u;
      fixtures[i].last_status = i == 7u ? SALTS_EIO : SALTS_OK;
      uid_written = snprintf(fixtures[i].metadata.uid, sizeof(fixtures[i].metadata.uid), "%s:%u",
                             cases[i].name, (unsigned)i);
      check(uid_written > 0 && (size_t)uid_written < sizeof(fixtures[i].metadata.uid));
      uid_written = snprintf(fixtures[i].metadata.owner_name,
                             sizeof(fixtures[i].metadata.owner_name), "%s-owner", cases[i].name);
      check(uid_written > 0 && (size_t)uid_written < sizeof(fixtures[i].metadata.owner_name));
      check_equal(resource_fixture_register(flow, &fixtures[i], 0, 0), SALTS_OK);
    }

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      for (size_t j = 0; j < sizeof(document_kinds) / sizeof(document_kinds[0]); ++j) {
        turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
        const turbo_flow_resource_schema_t *schema = turbo_flow_resource_governance_schema(
            cases[i].domain, cases[i].kind, document_kinds[j]);
        DataBindError error = DATA_BIND_ERROR_INIT;
        DataBindValue *value = NULL;
        DataBind *codec = NULL;
        int32_t number = 0;

        check_not_null(schema);
        check_equal(schema->schema_id, cases[i].schema_id);
        check_equal(turbo_flow_resource_document_at(flow, i, document_kinds[j], &document),
                     SALTS_OK);
        check(document.schema == schema);
        check_equal(turbo_flow_resource_document_validate(&document, schema), SALTS_OK);
        check_equal(data_bind_create_from_text(schema->schema_text, strlen(schema->schema_text),
                                                &codec, &error),
                     DATA_BIND_OK);
        check_equal(data_bind_parse_json(codec, schema->type_name,
                                          mem_buffer_const_data(document.payload),
                                          mem_buffer_used(document.payload), &value, &error),
                     DATA_BIND_OK);
        check_not_null(value);
        if (i == 1u && document_kinds[j] == TURBO_FLOW_RESOURCE_DOCUMENT_CONDITIONS) {
          check_equal(
              data_bind_value_get_int32(data_bind_value_get(value, "ready_status"), &number),
              DATA_BIND_OK);
          check_equal(number, TURBO_FLOW_CONDITION_TRUE);
          check_equal(
              data_bind_value_get_int32(data_bind_value_get(value, "accepting_reason"), &number),
              DATA_BIND_OK);
          check_equal(number, TURBO_FLOW_RESOURCE_REASON_NOT_ACCEPTING);
          check_equal(
              data_bind_value_get_int32(data_bind_value_get(value, "saturated_reason"), &number),
              DATA_BIND_OK);
          check_equal(number, TURBO_FLOW_RESOURCE_REASON_CAPACITY_EXHAUSTED);
        }
        if (document_kinds[j] == TURBO_FLOW_RESOURCE_DOCUMENT_EVENT) {
          int gap = 0;
          check_equal(data_bind_value_get_bool(data_bind_value_get(value, "gap"), &gap),
                       DATA_BIND_OK);
          check_equal(gap, i == 5u);
          check_equal(data_bind_value_get_int32(data_bind_value_get(value, "reason"), &number),
                       DATA_BIND_OK);
          check_equal(number, i == 7u   ? TURBO_FLOW_RESOURCE_REASON_OWNER_ERROR
                               : i == 5u ? TURBO_FLOW_RESOURCE_REASON_OBSERVATION_LAGGING
                                         : TURBO_FLOW_RESOURCE_REASON_OBSERVATION_CURRENT);
        }
        data_bind_value_free(value);
        data_bind_free(codec);
        turbo_flow_resource_document_cleanup(&document);
      }
    }

    check_null(turbo_flow_resource_governance_schema(TURBO_FLOW_DOMAIN_RULES,
                                                     TURBO_FLOW_RESOURCE_CONNECTION,
                                                     TURBO_FLOW_RESOURCE_DOCUMENT_SPEC));
    check_null(turbo_flow_resource_governance_schema(TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                                                     TURBO_FLOW_RESOURCE_CONNECTION,
                                                     TURBO_FLOW_RESOURCE_DOCUMENT_STATUS));
    {
      turbo_flow_resource_document_t first = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
      turbo_flow_resource_document_t second = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
      check_equal(
          turbo_flow_resource_document_at(flow, 5u, TURBO_FLOW_RESOURCE_DOCUMENT_EVENT, &first),
          SALTS_OK);
      check_equal(
          turbo_flow_resource_document_at(flow, 5u, TURBO_FLOW_RESOURCE_DOCUMENT_EVENT, &second),
          SALTS_OK);
      check_equal(mem_buffer_used(first.payload), mem_buffer_used(second.payload));
      check(memcmp(mem_buffer_const_data(first.payload), mem_buffer_const_data(second.payload),
                   mem_buffer_used(first.payload)) == 0);
      check_equal(first.generation, second.generation);
      check_equal(first.observed_generation, second.observed_generation);
      turbo_flow_resource_document_cleanup(&first);
      turbo_flow_resource_document_cleanup(&second);
    }
    turbo_flow_destroy(flow);
  }

  it("rejects oversized or owner-inconsistent adapter documents") {
    turbo_flow_t *flow = turbo_flow_create();
    resource_metadata_fixture_t fixture;
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    char byte = 'x';

    memset(&fixture, 0, sizeof(fixture));
    fixture.metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    fixture.metadata.domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
    fixture.metadata.kind = TURBO_FLOW_RESOURCE_QUEUE_BUFFER;
    memcpy(fixture.metadata.uid, "queue:orders", sizeof("queue:orders"));
    memcpy(fixture.metadata.owner_name, "orders", sizeof("orders"));
    fixture.metadata.generation = 1u;
    fixture.metadata.observed_generation = 1u;
    check_not_null(flow);
    check_equal(resource_fixture_register(flow, &fixture, 1, 0), SALTS_OK);
    fixture.document_payload = &byte;
    fixture.document_payload_size = TURBO_FLOW_RESOURCE_DOCUMENT_MAX_BYTES + 1u;
    check_equal(
        turbo_flow_resource_document_at(flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
        SALTS_EINVAL);
    fixture.document_payload = NULL;
    fixture.document_payload_size = 0u;
    fixture.corrupt_document_uid = 1;
    check_equal(
        turbo_flow_resource_document_at(flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
        SALTS_EPROTO);
    check_null(document.payload);
    turbo_flow_destroy(flow);
  }

  it("validates exact resource schema identity") {
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    turbo_flow_resource_schema_t expected = RESOURCE_FIXTURE_SCHEMA;

    metadata.domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
    metadata.kind = TURBO_FLOW_RESOURCE_QUEUE_BUFFER;
    memcpy(metadata.uid, "queue:orders", sizeof("queue:orders"));
    memcpy(metadata.owner_name, "orders", sizeof("orders"));
    metadata.generation = 1u;
    metadata.observed_generation = 1u;
    check_equal(turbo_flow_resource_document_set_payload_copy(
                     &document, &metadata, &RESOURCE_FIXTURE_SCHEMA, "{\"ready\":true}",
                     sizeof("{\"ready\":true}") - 1u),
                 SALTS_OK);
    check_equal(turbo_flow_resource_document_validate(&document, &expected), SALTS_OK);
    expected.schema_version = 2u;
    check_equal(turbo_flow_resource_document_validate(&document, &expected), SALTS_EPROTO);
    expected = RESOURCE_FIXTURE_SCHEMA;
    expected.type_name = "OtherStatus";
    check_equal(turbo_flow_resource_document_validate(&document, &expected), SALTS_EPROTO);
    expected = RESOURCE_FIXTURE_SCHEMA;
    expected.schema_id = 0u;
    check_equal(turbo_flow_resource_document_validate(&document, &expected), SALTS_EINVAL);
    turbo_flow_resource_document_cleanup(&document);
  }

  it("binds pool status dynamically through its DataBind schema") {
    static const char *src = "source input\n"
                             "stage transform worker 2 capacity 16\n"
                             "stage main {\n"
                             "  input -> transform\n"
                             "}\n";
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    turbo_flow_pool_resource_status_t pool_status = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBindValue *value = NULL;
    DataBind *codec = NULL;
    const DataBindValue *field;
    const char *text;
    size_t text_len = 0u;
    int32_t parallelism = 0;
    int accepting = 0;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "transform", resource_document_noop_stage, NULL, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    document.size = sizeof(document) - 1u;
    check_equal(turbo_flow_pool_status_document_at(flow, 0, &document), SALTS_EINVAL);
    document = (turbo_flow_resource_document_t)TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    check_equal(turbo_flow_pool_status_document_at(flow, 0, &document), SALTS_OK);
    check_equal(document.domain, TURBO_FLOW_DOMAIN_EXECUTION);
    check_equal(document.resource_kind, TURBO_FLOW_RESOURCE_POOL);
    check_equal(document.document_kind, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS);
    check_equal(document.uid, "pool:2:transform");
    check_equal(document.owner_name, "transform");
    check_equal(turbo_flow_pool_resource_status_at(flow, 0u, &pool_status), SALTS_OK);
    check_equal(document.generation, pool_status.generation);
    check_equal(document.observed_generation, document.generation);
    check_not_null(document.schema);
    check_equal(document.schema->schema_name, "TurboFlowResource");
    check_equal(document.schema->type_name, "PoolStatus");
    check_equal(document.schema->schema_id, 1u);
    check_equal(document.schema->schema_version, 1u);
    check_equal(document.schema->encoding, TURBO_FLOW_RESOURCE_DOCUMENT_JSON);
    check_not_null(document.payload);

    check_equal(data_bind_create_from_text(document.schema->schema_text,
                                            strlen(document.schema->schema_text), &codec, &error),
                 DATA_BIND_OK);
    check_not_null(codec);
    check_equal(data_bind_schema_name(codec), document.schema->schema_name);
    check_equal(data_bind_schema_attribute_get(codec, "id"), "1");
    check_equal(data_bind_schema_attribute_get(codec, "version"), "1");
    check_equal(data_bind_parse_json(codec, document.schema->type_name,
                                      mem_buffer_const_data(document.payload),
                                      mem_buffer_used(document.payload), &value, &error),
                 DATA_BIND_OK);
    check_not_null(value);
    check_equal(data_bind_value_get_int32(data_bind_value_get(value, "parallelism"), &parallelism),
                 DATA_BIND_OK);
    check_equal(parallelism, 2);
    field = data_bind_value_get(value, "queue_capacity");
    check_equal(data_bind_value_get_string(field, &text, &text_len), DATA_BIND_OK);
    check_equal(text_len, 2u);
    check(strncmp(text, "16", text_len) == 0);
    check_equal(data_bind_value_get_bool(data_bind_value_get(value, "accepting"), &accepting),
                 DATA_BIND_OK);
    check_equal(accepting, 1);

    data_bind_value_free(value);
    data_bind_free(codec);
    check_equal(turbo_flow_pool_status_document_at(flow, 0, &document), SALTS_EINVAL);
    turbo_flow_resource_document_cleanup(&document);
    check_null(document.payload);
    check_equal(turbo_flow_pool_status_document_at(flow, 0, &document), SALTS_OK);
    turbo_flow_resource_document_cleanup(&document);
    check_equal(
        turbo_flow_resource_document_at(flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_SPEC, &document),
        SALTS_OK);
    check_equal(document.schema->type_name, "RuntimeSpec");
    turbo_flow_resource_document_cleanup(&document);
    check_equal(
        turbo_flow_resource_document_at(flow, 1u, TURBO_FLOW_RESOURCE_DOCUMENT_EVENT, &document),
        SALTS_OK);
    check_equal(document.schema->type_name, "SegmentEvent");
    turbo_flow_resource_document_cleanup(&document);
    check_equal(
        turbo_flow_resource_document_at(flow, turbo_flow_resource_metadata_count(flow) - 1u,
                                        TURBO_FLOW_RESOURCE_DOCUMENT_CONDITIONS, &document),
        SALTS_OK);
    check_equal(document.schema->type_name, "PoolConditions");
    turbo_flow_resource_document_cleanup(&document);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("enumerates provider, runtime, segment, and pool resources by stable identity") {
    static const char *src = "source input\n"
                             "stage transform worker 1 capacity 8\n"
                             "stage main {\n"
                             "  input -> transform\n"
                             "}\n";
    resource_metadata_fixture_t fixture;
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_pool_resource_status_t pool = TURBO_FLOW_POOL_RESOURCE_STATUS_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&fixture, 0, sizeof(fixture));
    fixture.metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    fixture.metadata.domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
    fixture.metadata.kind = TURBO_FLOW_RESOURCE_QUEUE_BUFFER;
    memcpy(fixture.metadata.uid, "queue:orders", sizeof("queue:orders"));
    memcpy(fixture.metadata.owner_name, "orders", sizeof("orders"));
    fixture.metadata.generation = 7u;
    fixture.metadata.observed_generation = 7u;
    check_not_null(flow);
    check_equal(turbo_flow_register_adapter(flow, "plain", NULL, NULL), SALTS_OK);
    check_equal(turbo_flow_resource_metadata_count(flow), 0u);
    check_equal(resource_fixture_register(flow, &fixture, 0, 0), SALTS_OK);
    check_equal(turbo_flow_resource_metadata_count(flow), 1u);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "transform", resource_document_noop_stage, NULL, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_resource_metadata_count(flow), 5u);
    check_equal(turbo_flow_resource_metadata_at(flow, 0u, &metadata), SALTS_OK);
    check_equal(metadata.uid, "queue:orders");
    check_equal(metadata.owner_name, "orders");
    check_equal(metadata.generation, 7u);
    check_true(fixture.calls >= 2);

    metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    check_equal(resource_metadata_find(flow, TURBO_FLOW_RESOURCE_RUNTIME, "graph", &metadata),
                 SALTS_OK);
    check_equal(metadata.uid, "runtime:graph");

    metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    check_equal(resource_metadata_find(flow, TURBO_FLOW_RESOURCE_SEGMENT, "transform", &metadata),
                 SALTS_OK);
    check_true(strncmp(metadata.uid, "segment:", sizeof("segment:") - 1u) == 0);

    metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    check_equal(resource_metadata_find(flow, TURBO_FLOW_RESOURCE_POOL, "transform", &metadata),
                 SALTS_OK);
    check_equal(metadata.domain, TURBO_FLOW_DOMAIN_EXECUTION);
    check_equal(metadata.kind, TURBO_FLOW_RESOURCE_POOL);
    check_equal(turbo_flow_pool_resource_status_at(flow, 0u, &pool), SALTS_OK);
    check_equal(metadata.uid, pool.uid);
    check_equal(metadata.owner_name, pool.owner_name);
    check_equal(metadata.generation, pool.generation);
    check_equal(metadata.observed_generation, pool.observed_generation);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects malformed or future-observed stable metadata") {
    turbo_flow_t *flow = turbo_flow_create();
    resource_metadata_fixture_t fixture;

    memset(&fixture, 0, sizeof(fixture));
    fixture.metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    fixture.metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
    fixture.metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
    memcpy(fixture.metadata.uid, "connection:test", sizeof("connection:test"));
    memcpy(fixture.metadata.owner_name, "test", sizeof("test"));
    fixture.metadata.generation = 2u;
    fixture.metadata.observed_generation = 3u;
    check_not_null(flow);
    check_equal(resource_fixture_register(flow, &fixture, 0, 0), SALTS_EPROTO);
    fixture.metadata.observed_generation = 2u;
    fixture.metadata.uid[0] = '\0';
    check_equal(resource_fixture_register(flow, &fixture, 0, 0), SALTS_EPROTO);
    memset(fixture.metadata.uid, 'x', sizeof(fixture.metadata.uid));
    check_equal(resource_fixture_register(flow, &fixture, 0, 0), SALTS_EPROTO);
    turbo_flow_destroy(flow);
  }

  it("dispatches generation-checked idempotent adapter commands") {
    turbo_flow_t *flow = turbo_flow_create();
    resource_metadata_fixture_t fixture;
    turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;

    memset(&fixture, 0, sizeof(fixture));
    fixture.metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    fixture.metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
    fixture.metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
    memcpy(fixture.metadata.uid, "connection:test", sizeof("connection:test"));
    memcpy(fixture.metadata.owner_name, "test", sizeof("test"));
    fixture.metadata.generation = 4u;
    fixture.metadata.observed_generation = 4u;
    check_not_null(flow);
    check_equal(resource_fixture_register(flow, &fixture, 0, 1), SALTS_OK);

    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    memcpy(command.target_uid, fixture.metadata.uid, sizeof("connection:test"));
    memcpy(command.idempotency_key, "quiesce-1", sizeof("quiesce-1"));
    command.expected_generation = 4u;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
    check_equal(result.replayed, 0);
    check_equal(result.generation_before, 4u);
    check_equal(result.generation_after, 5u);
    check_equal(fixture.command_calls, 1);

    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
    check_equal(result.replayed, 1);
    check_equal(fixture.command_calls, 1);
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_EPROTO);
    check_equal(fixture.command_calls, 1);

    command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
    memcpy(command.target_uid, fixture.metadata.uid, sizeof("connection:test"));
    memcpy(command.idempotency_key, "resume-stale", sizeof("resume-stale"));
    command.expected_generation = 4u;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_EBUSY);
    check_equal(fixture.command_calls, 1);

    command.expected_generation = 5u;
    command.deadline_ns = salts_hrtime();
    memcpy(command.idempotency_key, "resume-expired", sizeof("resume-expired"));
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ETIMEDOUT);
    check_equal(fixture.command_calls, 1);

    command.deadline_ns = UINT64_MAX;
    memcpy(command.idempotency_key, "resume-fails", sizeof("resume-fails"));
    fixture.command_status = SALTS_EIO;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_EIO);
    check_equal(fixture.command_calls, 2);
    check_equal(fixture.metadata.generation, 5u);
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_EIO);
    check_equal(result.replayed, 1);
    check_equal(fixture.command_calls, 2);

    command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    memset(command.target_uid, 'x', sizeof(command.target_uid));
    memcpy(command.idempotency_key, "unterminated-target", sizeof("unterminated-target"));
    command.expected_generation = 5u;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("dispatches pool resize through the same stable resource command") {
    static const char *src = "source input\n"
                             "stage transform worker 1 capacity 8\n"
                             "stage main {\n"
                             "  input -> transform\n"
                             "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    turbo_flow_pool_snapshot_t snapshot;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "transform", resource_document_noop_stage, NULL, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(resource_metadata_find(flow, TURBO_FLOW_RESOURCE_POOL, "transform", &metadata),
                 SALTS_OK);
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESIZE_POOL;
    memcpy(command.target_uid, metadata.uid, strlen(metadata.uid) + 1u);
    memcpy(command.idempotency_key, "resize-1", sizeof("resize-1"));
    command.expected_generation = metadata.generation;
    command.deadline_ns = salts_hrtime() + UINT64_C(1000000000);
    command.parallelism = 2u;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
    check_equal(result.generation_before, metadata.generation);
    check_equal(result.generation_after, metadata.generation + 1u);
    check_equal(turbo_flow_pool_snapshot_at(flow, 0u, &snapshot), SALTS_OK);
    check_equal(snapshot.parallelism, 2u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("observes and controls native runtime resources without mutating queries") {
    static const char *src = "source input\n"
                             "stage transform\n"
                             "stage main {\n"
                             "  input -> transform\n"
                             "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_resource_metadata_t runtime = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_metadata_t segment = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_metadata_t queried = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    turbo_flow_runtime_snapshot_t runtime_snapshot;
    size_t runtime_index = SIZE_MAX;
    size_t segment_index = SIZE_MAX;

    check_not_null(flow);
    check_equal(turbo_flow_resource_metadata_count(flow), 0u);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "transform", resource_document_noop_stage, NULL, NULL),
        SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    for (size_t i = 0; i < turbo_flow_resource_metadata_count(flow); ++i) {
      turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
      check_equal(turbo_flow_resource_metadata_at(flow, i, &metadata), SALTS_OK);
      if (metadata.kind == TURBO_FLOW_RESOURCE_RUNTIME) {
        runtime = metadata;
        runtime_index = i;
      } else if (metadata.kind == TURBO_FLOW_RESOURCE_SEGMENT) {
        segment = metadata;
        segment_index = i;
      }
    }
    check_not_equal(runtime_index, SIZE_MAX);
    check_not_equal(segment_index, SIZE_MAX);
    check_equal(runtime.uid, "runtime:graph");
    check_true(strncmp(segment.uid, "segment:", sizeof("segment:") - 1u) == 0);
    check_equal(turbo_flow_resource_document_at(flow, runtime_index,
                                                 TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
                 SALTS_OK);
    check_equal(document.schema->type_name, "RuntimeStatus");
    turbo_flow_resource_document_cleanup(&document);
    check_equal(turbo_flow_resource_document_at(flow, segment_index,
                                                 TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
                 SALTS_OK);
    check_equal(document.schema->type_name, "SegmentStatus");
    turbo_flow_resource_document_cleanup(&document);
    check_equal(turbo_flow_resource_metadata_at(flow, runtime_index, &queried), SALTS_OK);
    check_equal(queried.generation, runtime.generation);

    check_equal(turbo_flow_start(flow), SALTS_OK);
    runtime = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    check_equal(resource_metadata_find(flow, TURBO_FLOW_RESOURCE_RUNTIME, "graph", &runtime),
                 SALTS_OK);
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    memcpy(command.target_uid, runtime.uid, strlen(runtime.uid) + 1u);
    memcpy(command.idempotency_key, "runtime-quiesce", sizeof("runtime-quiesce"));
    command.expected_generation = runtime.generation;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
    check_equal(result.generation_after, runtime.generation + 1u);
    check_equal(turbo_flow_runtime_snapshot(flow, &runtime_snapshot), SALTS_OK);
    check_false(runtime_snapshot.accepting_publishes);

    command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
    memcpy(command.target_uid, runtime.uid, strlen(runtime.uid) + 1u);
    memcpy(command.idempotency_key, "runtime-resume-stale", sizeof("runtime-resume-stale"));
    command.expected_generation = runtime.generation;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_EBUSY);
    command.expected_generation = runtime.generation + 1u;
    memcpy(command.idempotency_key, "runtime-resume", sizeof("runtime-resume"));
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
    check_equal(turbo_flow_runtime_snapshot(flow, &runtime_snapshot), SALTS_OK);
    check_true(runtime_snapshot.accepting_publishes);

    command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    memcpy(command.target_uid, segment.uid, strlen(segment.uid) + 1u);
    memcpy(command.idempotency_key, "segment-quiesce", sizeof("segment-quiesce"));
    command.expected_generation = runtime.generation + 2u;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ENOTSUP);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("reconciles immutable values and conditions with at most one owner command") {
    turbo_flow_t *flow = turbo_flow_create();
    resource_metadata_fixture_t fixture;
    turbo_flow_resource_reconcile_request_t request = TURBO_FLOW_RESOURCE_RECONCILE_REQUEST_INIT;
    turbo_flow_resource_reconcile_result_t result = TURBO_FLOW_RESOURCE_RECONCILE_RESULT_INIT;

    memset(&fixture, 0, sizeof(fixture));
    fixture.metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    fixture.metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
    fixture.metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
    memcpy(fixture.metadata.uid, "connection:reconcile", sizeof("connection:reconcile"));
    memcpy(fixture.metadata.owner_name, "reconcile", sizeof("reconcile"));
    fixture.metadata.generation = 3u;
    fixture.metadata.observed_generation = 3u;
    check_not_null(flow);
    check_equal(resource_fixture_register(flow, &fixture, 0, 1), SALTS_OK);

    request.metadata = fixture.metadata;
    request.observed_value = 7u;
    request.desired_value = 7u;
    request.condition_count = 1u;
    request.conditions[0] = (turbo_flow_resource_condition_t){TURBO_FLOW_RESOURCE_CONDITION_READY,
                                                              TURBO_FLOW_CONDITION_TRUE,
                                                              TURBO_FLOW_RESOURCE_REASON_RUNNING};
    request.desired_condition_count = 1u;
    request.desired_conditions[0] = request.conditions[0];
    request.command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    memcpy(request.command.target_uid, fixture.metadata.uid, strlen(fixture.metadata.uid) + 1u);
    memcpy(request.command.idempotency_key, "reconcile-ready", sizeof("reconcile-ready"));
    check_equal(turbo_flow_resource_reconcile_tick(flow, &request, &result), SALTS_OK);
    check_equal(result.action, TURBO_FLOW_RESOURCE_RECONCILE_CONVERGED);
    check_true(result.value_matches);
    check_equal(result.matched_condition_count, 1u);
    check_equal(fixture.command_calls, 0);

    request.metadata.observed_generation = 2u;
    request.desired_value = 8u;
    result = (turbo_flow_resource_reconcile_result_t)TURBO_FLOW_RESOURCE_RECONCILE_RESULT_INIT;
    check_equal(turbo_flow_resource_reconcile_tick(flow, &request, &result), SALTS_OK);
    check_equal(result.action, TURBO_FLOW_RESOURCE_RECONCILE_OBSERVING);
    check_equal(fixture.command_calls, 0);

    request.metadata.observed_generation = 3u;
    result = (turbo_flow_resource_reconcile_result_t)TURBO_FLOW_RESOURCE_RECONCILE_RESULT_INIT;
    check_equal(turbo_flow_resource_reconcile_tick(flow, &request, &result), SALTS_OK);
    check_equal(result.action, TURBO_FLOW_RESOURCE_RECONCILE_COMMAND_APPLIED);
    check_equal(result.command_result.generation_before, 3u);
    check_equal(result.command_result.generation_after, 4u);
    check_equal(fixture.command_calls, 1);

    turbo_flow_destroy(flow);
  }

  it("advances the explicit multi-owner resize workflow one operation per tick") {
    static const char *src = "source input\n"
                             "stage transform worker 2 capacity 8\n"
                             "stage main {\n"
                             "  input -> transform\n"
                             "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    resource_metadata_fixture_t fixture;
    turbo_flow_resource_metadata_t pool_metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resize_workflow_spec_t workflow = TURBO_FLOW_RESIZE_WORKFLOW_SPEC_INIT;
    turbo_flow_resize_workflow_state_t state = TURBO_FLOW_RESIZE_WORKFLOW_STATE_INIT;
    turbo_flow_resize_workflow_result_t result = TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT;
    turbo_flow_runtime_snapshot_t runtime;
    turbo_flow_pool_snapshot_t pool;

    memset(&fixture, 0, sizeof(fixture));
    fixture.metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    fixture.metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
    fixture.metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
    memcpy(fixture.metadata.uid, "connection:ingress", sizeof("connection:ingress"));
    memcpy(fixture.metadata.owner_name, "ingress", sizeof("ingress"));
    fixture.metadata.generation = 1u;
    fixture.metadata.observed_generation = 1u;
    check_not_null(flow);
    check_equal(resource_fixture_register(flow, &fixture, 0, 1), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "transform", resource_document_noop_stage, NULL, NULL),
        SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(
        resource_metadata_find(flow, TURBO_FLOW_RESOURCE_POOL, "transform", &pool_metadata),
        SALTS_OK);

    memcpy(workflow.workflow_id, "scale-transform", sizeof("scale-transform"));
    memcpy(workflow.ingress_uid, fixture.metadata.uid, strlen(fixture.metadata.uid) + 1u);
    memcpy(workflow.pool_uid, pool_metadata.uid, strlen(pool_metadata.uid) + 1u);
    workflow.ingress_generation = fixture.metadata.generation;
    workflow.pool_generation = pool_metadata.generation;
    workflow.parallelism = 3u;
    check_equal(turbo_flow_resize_workflow_init(&workflow, &state), SALTS_OK);

    check_equal(turbo_flow_resize_workflow_tick(flow, &state, &result), SALTS_OK);
    check_equal(state.phase, TURBO_FLOW_RESIZE_WORKFLOW_DRAIN_GRAPH);
    check_equal(fixture.command_calls, 1);
    check_equal(fixture.command_kinds[0], TURBO_FLOW_RESOURCE_COMMAND_QUIESCE);
    result = (turbo_flow_resize_workflow_result_t)TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT;
    check_equal(turbo_flow_resize_workflow_tick(flow, &state, &result), SALTS_OK);
    check_equal(result.action, TURBO_FLOW_RESIZE_WORKFLOW_GRAPH_DRAINED);
    check_equal(turbo_flow_runtime_snapshot(flow, &runtime), SALTS_OK);
    check_false(runtime.accepting_publishes);
    check_equal(fixture.command_calls, 1);
    result = (turbo_flow_resize_workflow_result_t)TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT;
    check_equal(turbo_flow_resize_workflow_tick(flow, &state, &result), SALTS_OK);
    check_equal(state.phase, TURBO_FLOW_RESIZE_WORKFLOW_RESUME_GRAPH);
    check_equal(state.commands_applied, 2u);
    check_equal(turbo_flow_pool_snapshot_at(flow, 0u, &pool), SALTS_OK);
    check_equal(pool.parallelism, 3u);
    result = (turbo_flow_resize_workflow_result_t)TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT;
    check_equal(turbo_flow_resize_workflow_tick(flow, &state, &result), SALTS_OK);
    check_equal(result.action, TURBO_FLOW_RESIZE_WORKFLOW_GRAPH_RESUMED);
    check_equal(turbo_flow_runtime_snapshot(flow, &runtime), SALTS_OK);
    check_true(runtime.accepting_publishes);
    check_equal(fixture.command_calls, 1);
    result = (turbo_flow_resize_workflow_result_t)TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT;
    check_equal(turbo_flow_resize_workflow_tick(flow, &state, &result), SALTS_OK);
    check_equal(state.phase, TURBO_FLOW_RESIZE_WORKFLOW_DONE);
    check_equal(state.commands_applied, 3u);
    check_equal(fixture.command_calls, 2);
    check_equal(fixture.command_kinds[1], TURBO_FLOW_RESOURCE_COMMAND_RESUME);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("stops a failed workflow at its owner boundary and retries explicitly") {
    static const char *src = "source input\n"
                             "stage transform worker 1 capacity 8\n"
                             "stage main {\n"
                             "  input -> transform\n"
                             "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    resource_metadata_fixture_t fixture;
    turbo_flow_resource_metadata_t pool_metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resize_workflow_spec_t workflow = TURBO_FLOW_RESIZE_WORKFLOW_SPEC_INIT;
    turbo_flow_resize_workflow_state_t state = TURBO_FLOW_RESIZE_WORKFLOW_STATE_INIT;
    turbo_flow_resize_workflow_result_t result = TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT;

    memset(&fixture, 0, sizeof(fixture));
    fixture.metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    fixture.metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
    fixture.metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
    memcpy(fixture.metadata.uid, "connection:retry", sizeof("connection:retry"));
    memcpy(fixture.metadata.owner_name, "retry", sizeof("retry"));
    fixture.metadata.generation = 1u;
    fixture.metadata.observed_generation = 1u;
    fixture.command_status = SALTS_EIO;
    fixture.command_fail_kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
    check_not_null(flow);
    check_equal(resource_fixture_register(flow, &fixture, 0, 1), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "transform", resource_document_noop_stage, NULL, NULL),
        SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(
        resource_metadata_find(flow, TURBO_FLOW_RESOURCE_POOL, "transform", &pool_metadata),
        SALTS_OK);
    memcpy(workflow.workflow_id, "retry-resume", sizeof("retry-resume"));
    memcpy(workflow.ingress_uid, fixture.metadata.uid, strlen(fixture.metadata.uid) + 1u);
    memcpy(workflow.pool_uid, pool_metadata.uid, strlen(pool_metadata.uid) + 1u);
    workflow.ingress_generation = 1u;
    workflow.pool_generation = pool_metadata.generation;
    workflow.parallelism = 2u;
    check_equal(turbo_flow_resize_workflow_init(&workflow, &state), SALTS_OK);
    for (int i = 0; i < 4; ++i) {
      result = (turbo_flow_resize_workflow_result_t)TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT;
      check_equal(turbo_flow_resize_workflow_tick(flow, &state, &result), SALTS_OK);
    }
    result = (turbo_flow_resize_workflow_result_t)TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT;
    check_equal(turbo_flow_resize_workflow_tick(flow, &state, &result), SALTS_EIO);
    check_equal(state.phase, TURBO_FLOW_RESIZE_WORKFLOW_FAILED);
    check_equal(state.failed_phase, TURBO_FLOW_RESIZE_WORKFLOW_RESUME_INGRESS);
    check_equal(state.commands_applied, 2u);
    fixture.command_status = SALTS_OK;
    check_equal(turbo_flow_resize_workflow_retry(&state), SALTS_OK);
    check_equal(state.attempt, 1u);
    result = (turbo_flow_resize_workflow_result_t)TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT;
    check_equal(turbo_flow_resize_workflow_tick(flow, &state, &result), SALTS_OK);
    check_equal(state.phase, TURBO_FLOW_RESIZE_WORKFLOW_DONE);
    check_equal(state.commands_applied, 3u);
    check_equal(fixture.command_calls, 3);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("does not adopt an unobserved generation after a stale workflow command") {
    turbo_flow_t *flow = turbo_flow_create();
    resource_metadata_fixture_t fixture;
    turbo_flow_resize_workflow_spec_t workflow = TURBO_FLOW_RESIZE_WORKFLOW_SPEC_INIT;
    turbo_flow_resize_workflow_state_t state = TURBO_FLOW_RESIZE_WORKFLOW_STATE_INIT;
    turbo_flow_resize_workflow_result_t result = TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT;

    memset(&fixture, 0, sizeof(fixture));
    fixture.metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    fixture.metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
    fixture.metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
    memcpy(fixture.metadata.uid, "connection:stale-workflow", sizeof("connection:stale-workflow"));
    memcpy(fixture.metadata.owner_name, "stale-workflow", sizeof("stale-workflow"));
    fixture.metadata.generation = 2u;
    fixture.metadata.observed_generation = 2u;
    check_not_null(flow);
    check_equal(resource_fixture_register(flow, &fixture, 0, 1), SALTS_OK);
    memcpy(workflow.workflow_id, "stale-workflow", sizeof("stale-workflow"));
    memcpy(workflow.ingress_uid, fixture.metadata.uid, strlen(fixture.metadata.uid) + 1u);
    memcpy(workflow.pool_uid, "pool:unused", sizeof("pool:unused"));
    workflow.ingress_generation = 1u;
    workflow.pool_generation = 1u;
    workflow.parallelism = 2u;
    check_equal(turbo_flow_resize_workflow_init(&workflow, &state), SALTS_OK);
    check_equal(turbo_flow_resize_workflow_tick(flow, &state, &result), SALTS_EBUSY);
    check_equal(state.phase, TURBO_FLOW_RESIZE_WORKFLOW_FAILED);
    check_equal(state.ingress_generation, 1u);
    check_equal(fixture.command_calls, 0);
    check_equal(turbo_flow_resize_workflow_retry(&state), SALTS_OK);
    result = (turbo_flow_resize_workflow_result_t)TURBO_FLOW_RESIZE_WORKFLOW_RESULT_INIT;
    check_equal(turbo_flow_resize_workflow_tick(flow, &state, &result), SALTS_EBUSY);
    check_equal(state.ingress_generation, 1u);
    check_equal(fixture.command_calls, 0);
    turbo_flow_destroy(flow);
  }

  it("rejects malformed or single-owner resize workflows") {
    turbo_flow_resize_workflow_spec_t workflow = TURBO_FLOW_RESIZE_WORKFLOW_SPEC_INIT;
    turbo_flow_resize_workflow_state_t state = TURBO_FLOW_RESIZE_WORKFLOW_STATE_INIT;

    memcpy(workflow.workflow_id, "invalid-workflow", sizeof("invalid-workflow"));
    memcpy(workflow.ingress_uid, "resource:same", sizeof("resource:same"));
    memcpy(workflow.pool_uid, "resource:same", sizeof("resource:same"));
    workflow.ingress_generation = 1u;
    workflow.pool_generation = 1u;
    workflow.parallelism = 2u;
    check_equal(turbo_flow_resize_workflow_init(&workflow, &state), SALTS_EINVAL);

    memcpy(workflow.pool_uid, "pool:other", sizeof("pool:other"));
    memset(workflow.workflow_id, 'x', sizeof(workflow.workflow_id));
    check_equal(turbo_flow_resize_workflow_init(&workflow, &state), SALTS_EINVAL);
  }
}
