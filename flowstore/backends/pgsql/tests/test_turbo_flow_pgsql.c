#include "data_bind.h"
#include "flow_pgsql_internal.h"
#include "libpq-fe.h"
#include "tinytest.h"
#include "turbo_flow_pgsql.h"
#include "pgsql_storage_test_helpers.h"
#include "turbo_str.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static PGresult *pgsql_make_result(const char *const *names, const Oid *oids, const int *formats,
                                   size_t columns, const char *const *cells, size_t rows) {
  PGresult *result = PQmakeEmptyPGresult(NULL, PGRES_TUPLES_OK);
  PGresAttDesc *attributes;
  if (!result || columns == 0u || columns > INT32_MAX || rows > INT32_MAX) return result;
  attributes = (PGresAttDesc *)calloc(columns, sizeof(*attributes));
  if (!attributes) {
    PQclear(result);
    return NULL;
  }
  for (size_t column = 0; column < columns; ++column) {
    attributes[column].name = (char *)names[column];
    attributes[column].typid = oids[column];
    attributes[column].format = formats[column];
    attributes[column].typlen = -1;
  }
  if (!PQsetResultAttrs(result, (int)columns, attributes)) {
    free(attributes);
    PQclear(result);
    return NULL;
  }
  free(attributes);
  for (size_t row = 0; row < rows; ++row) {
    for (size_t column = 0; column < columns; ++column) {
      const char *cell = cells[row * columns + column];
      int length = cell ? (int)strlen(cell) : -1;
      if (!PQsetvalue(result, (int)row, (int)column, (char *)cell, length)) {
        PQclear(result);
        return NULL;
      }
    }
  }
  return result;
}

static int pgsql_result_to_message(PGresult *result, const char *statement_name, size_t max_rows,
                                   size_t max_result_bytes, turbo_flow_pgsql_result_format_t format,
                                   turbo_flow_msg_t *msg) {
  static turbo_flow_content_descriptor_t rowset;
  static turbo_flow_content_descriptor_t command;
  const char *rowset_media = format == TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV
                                 ? "text/csv"
                                 : "application/vnd.turboflow.pg-rowset+json";
  int rc = turbo_flow_content_descriptor_from_media(&rowset, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
                                                    TURBO_FLOW_CONTENT_PROFILE_DATABASE_ROWSET,
                                                    rowset_media, statement_name, NULL);
  if (rc != TURBO_OK) return rc;
  rowset.flags |= TURBO_FLOW_CONTENT_BATCH;
  rc = turbo_flow_content_descriptor_from_media(&command, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
                                                TURBO_FLOW_CONTENT_PROFILE_DATABASE_COMMAND_RESULT,
                                                "application/vnd.turboflow.pg-command+json",
                                                statement_name, NULL);
  if (rc != TURBO_OK) return rc;
  return flow_pgsql_result_to_message(result, statement_name, max_rows, max_result_bytes, format,
                                      &rowset, &command, msg);
}

spec("turbo_flow_pgsql") {
  it("registers bounded PostgreSQL outbox source and sink without opening the database") {
    static const char *dsl = "source durable adapter pg.outbox.source\n"
                             "stage persist adapter pg.outbox.sink\n"
                             "stage main {\n"
                             "  durable -> persist\n"
                             "}\n";
    turbo_flow_pgsql_outbox_config_t config = TURBO_FLOW_PGSQL_OUTBOX_CONFIG_INIT;
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    const turbo_flow_adapter_schema_t *schema;
    tstr_t payload;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    config.conninfo = "host=127.0.0.1 port=1 password=outbox-secret connect_timeout=1";
    config.outbox_name = "orders";
    config.capacity = 1024u;
    config.max_payload_size = 1048576u;
    config.poll_interval_ms = 50u;
    config.claim_scan_limit = 64u;
    config.create_table = 1;
    check_int_eq(turbo_flow_pgsql_register_outbox_adapter(flow, "pg.outbox.sink", &config),
                 TURBO_OK);
    config.role = TURBO_FLOW_PGSQL_OUTBOX_SOURCE;
    check_int_eq(turbo_flow_pgsql_register_outbox_adapter(flow, "pg.outbox.source", &config),
                 TURBO_OK);
    check_size_eq(turbo_flow_resource_metadata_count(flow), 2u);
    schema = turbo_flow_find_adapter_schema(flow, "pg.outbox.sink");
    check_not_null(schema);
    check_int_eq(schema->kind, TURBO_FLOW_ADAPTER_KIND_POSTGRESQL);
    check_int_eq(schema->roles, TURBO_FLOW_ADAPTER_SINK);
    schema = turbo_flow_find_adapter_schema(flow, "pg.outbox.source");
    check_not_null(schema);
    check_int_eq(schema->roles, TURBO_FLOW_ADAPTER_SOURCE);
    check_int_eq(
        turbo_flow_resource_document_at(flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
        TURBO_OK);
    check_uint_eq(document.schema->schema_id, 303u);
    check_str_eq(document.schema->type_name, "PostgreSqlOutboxStatus");
    check_int_eq(turbo_flow_resource_document_validate(&document, document.schema), TURBO_OK);
    payload =
        tstr_new_len(mem_buffer_const_data(document.payload), mem_buffer_used(document.payload));
    check_not_null(payload);
    check_str_contains(payload, "\"started\":false");
    check_str_contains(payload, "\"accepted\":\"0\"");
    check_null(strstr(payload, "outbox-secret"));
    check_null(strstr(payload, "orders"));
    tstr_freep(&payload);
    turbo_flow_resource_document_cleanup(&document);
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("registers PostgreSQL outbox adapters from strict shared-channel YAML") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  orders.outbox:\n"
                               "    kind: outbox\n"
                               "    config:\n"
                               "      backend: postgresql\n"
                               "      conninfo: 'host=127.0.0.1 port=5432 dbname=flow'\n"
                               "      outbox_name: orders\n"
                               "      capacity: 1024\n"
                               "      max_payload_size: 1048576\n"
                               "      poll_interval_ms: 50\n"
                               "      claim_scan_limit: 64\n"
                               "      create_table: true\n"
                               "      completion: archive\n"
                               "      max_delivery_attempts: 3\n"
                               "      retry_delay_ms: 100\n"
                               "      archive_ttl_ms: 60000\n"
                               "adapters:\n"
                               "  pg.orders.sink:\n"
                               "    kind: pgsql_outbox\n"
                               "    config:\n"
                               "      channel: orders.outbox\n"
                               "      role: sink\n"
                               "  pg.orders.source:\n"
                               "    kind: pgsql_outbox\n"
                               "    config:\n"
                               "      channel: orders.outbox\n"
                               "      role: source\n";
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(
        turbo_flow_pgsql_register_resolved_outbox_adapter(flow, "pg.orders.sink", resolved, &error),
        TURBO_OK);
    check_int_eq(turbo_flow_pgsql_register_resolved_outbox_adapter(flow, "pg.orders.source",
                                                                   resolved, &error),
                 TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_resolved_config_destroy(resolved);
  }

  it("keeps the installed PostgreSQL outbox YAML example resolvable") {
    char example_path[1024];
    char *yaml;
    size_t yaml_size = 0u;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    const char *adapter_name = NULL;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    (void)snprintf(example_path, sizeof(example_path), "%s/examples/pgsql.yml",
                   TURBO_FLOW_PGSQL_SOURCE_DIR);
    yaml = tt_read_file(example_path, &yaml_size);
    check_not_null(yaml);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, yaml_size, &resolved, &error), TURBO_OK);
    check_int_eq(
        turbo_flow_resolved_config_profile_adapter(resolved, "orders", "enqueue", &adapter_name),
        TURBO_OK);
    check_str_eq(adapter_name, "pg.orders.sink");
    check_int_eq(
        turbo_flow_pgsql_register_resolved_outbox_adapter(flow, "pg.orders.sink", resolved, &error),
        TURBO_OK);
    check_int_eq(turbo_flow_pgsql_register_resolved_outbox_adapter(flow, "pg.orders.source",
                                                                   resolved, &error),
                 TURBO_OK);
    turbo_flow_destroy(flow);
    turbo_flow_resolved_config_destroy(resolved);
    free(yaml);
  }

  it("rejects invalid PostgreSQL outbox ABI and YAML contracts before I/O") {
    static const char yaml_template[] =
        "version: 1\nchannels:\n  out:\n    kind: %s\n    config:\n"
        "      backend: %s\n      conninfo: 'host=127.0.0.1'\n"
        "      outbox_name: orders\n      capacity: %u\n      max_payload_size: 64\n"
        "      poll_interval_ms: 10\n      claim_scan_limit: 4\n      create_table: true\n"
        "      %s\n"
        "adapters:\n  pg.out:\n    kind: pgsql_outbox\n    config:\n"
        "      channel: out\n      role: %s\n";
    struct invalid_yaml_case {
      const char *kind;
      const char *backend;
      unsigned capacity;
      const char *extra;
      const char *role;
      int expected;
      const char *path;
    } cases[] = {
        {"queue", "postgresql", 4u, "", "sink", TURBO_EINVAL, "$.channels.out"},
        {"outbox", "sqlite", 4u, "", "sink", TURBO_ENOTSUP, "$.channels.out.config.backend"},
        {"outbox", "postgresql", 0u, "", "sink", TURBO_ERANGE, "$.channels.out.config.capacity"},
        {"outbox", "postgresql", 4u, "unknown: true", "sink", TURBO_EINVAL,
         "$.channels.out.config.unknown"},
        {"outbox", "postgresql", 4u, "", "dealer", TURBO_EINVAL, "$.adapters.pg.out.config.role"}};
    turbo_flow_pgsql_outbox_config_t direct = TURBO_FLOW_PGSQL_OUTBOX_CONFIG_INIT;
    char yaml[1024];

    direct.conninfo = "host=127.0.0.1";
    direct.outbox_name = "orders";
    direct.capacity = 4u;
    direct.max_payload_size = 64u;
    direct.poll_interval_ms = 10u;
    direct.claim_scan_limit = 4u;
    direct.create_table = 1;
    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      turbo_flow_resolved_config_t *resolved = NULL;
      turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_t *flow = turbo_flow_create();
      check_not_null(flow);
      (void)snprintf(yaml, sizeof(yaml), yaml_template, cases[i].kind, cases[i].backend,
                     cases[i].capacity, cases[i].extra, cases[i].role);
      check_int_eq(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error), TURBO_OK);
      check_int_eq(
          turbo_flow_pgsql_register_resolved_outbox_adapter(flow, "pg.out", resolved, &error),
          cases[i].expected);
      check_str_eq(error.path, cases[i].path);
      turbo_flow_destroy(flow);
      turbo_flow_resolved_config_destroy(resolved);
    }
    {
      turbo_flow_t *flow = turbo_flow_create();
      check_not_null(flow);
      direct.version++;
      check_int_eq(turbo_flow_pgsql_register_outbox_adapter(flow, "pg.invalid", &direct),
                   TURBO_EINVAL);
      turbo_flow_destroy(flow);
    }
    {
      turbo_flow_t *flow = turbo_flow_create();
      check_not_null(flow);
      direct.version = TURBO_FLOW_PGSQL_OUTBOX_API_VERSION;
      direct.max_delivery_attempts = 3u;
      direct.retry_delay_ms = 0u;
      check_int_eq(turbo_flow_pgsql_register_outbox_adapter(flow, "pg.invalid", &direct),
                   TURBO_EINVAL);
      turbo_flow_destroy(flow);
    }
  }

  it("exposes distinct typed credential-free sink and query status documents") {
    turbo_flow_pgsql_sink_config_t sink_config = {
        "host=127.0.0.1 port=1 password=db-secret connect_timeout=1",
        "insert into private_events(payload) values($1)", 0};
    turbo_flow_pgsql_query_config_t query_config;
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBind *codec = NULL;
    DataBindValue *value = NULL;
    tstr_t payload = NULL;
    int32_t mode = -1;
    int started = -1;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    memset(&query_config, 0, sizeof(query_config));
    query_config.conninfo = sink_config.conninfo;
    query_config.statement = "select secret_column from private_events";
    query_config.statement_name = "private.events.read";
    query_config.result_format = TURBO_FLOW_PGSQL_RESULT_ROWSET_JSON;
    check_int_eq(turbo_flow_pgsql_register_sink_adapter(flow, "pg.sink", &sink_config), TURBO_OK);
    check_int_eq(turbo_flow_pgsql_register_query_adapter(flow, "pg.query", &query_config),
                 TURBO_OK);
    check_size_eq(turbo_flow_resource_metadata_count(flow), 2u);

    check_int_eq(turbo_flow_resource_metadata_at(flow, 0u, &metadata), TURBO_OK);
    check_str_eq(metadata.uid, "postgresql:pg.sink");
    check_int_eq(metadata.kind, TURBO_FLOW_RESOURCE_STORAGE);
    check_int_eq(
        turbo_flow_resource_document_at(flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
        TURBO_OK);
    check_str_eq(document.schema->schema_name, "TurboFlowDatabaseResource");
    check_str_eq(document.schema->type_name, "PostgreSqlSinkStatus");
    check_uint_eq(document.schema->schema_id, 301u);
    check_int_eq(turbo_flow_resource_document_validate(&document, document.schema), TURBO_OK);
    {
      turbo_flow_resource_schema_t unknown = *document.schema;
      unknown.type_name = "UnknownStatus";
      check_int_eq(turbo_flow_resource_document_validate(&document, &unknown), TURBO_EPROTO);
    }
    check_int_eq(data_bind_create_from_text(document.schema->schema_text,
                                            strlen(document.schema->schema_text), &codec, &error),
                 DATA_BIND_OK);
    check_int_eq(data_bind_parse_json(codec, document.schema->type_name,
                                      mem_buffer_const_data(document.payload),
                                      mem_buffer_used(document.payload), &value, &error),
                 DATA_BIND_OK);
    check_int_eq(data_bind_validate_json(codec, document.schema->type_name, "{\"started\":false}",
                                         sizeof("{\"started\":false}") - 1u, &error),
                 DATA_BIND_ERR_TYPE_MISMATCH);
    check_int_eq(data_bind_validate_json(codec, document.schema->type_name,
                                         "{\"started\":{},\"mode\":0}",
                                         sizeof("{\"started\":{},\"mode\":0}") - 1u, &error),
                 DATA_BIND_ERR_TYPE_MISMATCH);
    check_int_eq(data_bind_value_get_bool(data_bind_value_get(value, "started"), &started),
                 DATA_BIND_OK);
    check_int_eq(data_bind_value_get_int32(data_bind_value_get(value, "mode"), &mode),
                 DATA_BIND_OK);
    check_int_eq(started, 0);
    check_int_eq(mode, 0);
    payload =
        tstr_new_len(mem_buffer_const_data(document.payload), mem_buffer_used(document.payload));
    check_not_null(payload);
    check_null(strstr(payload, "db-secret"));
    check_null(strstr(payload, "private_events"));
    tstr_freep(&payload);
    data_bind_value_free(value);
    data_bind_free(codec);
    value = NULL;
    codec = NULL;
    turbo_flow_resource_document_cleanup(&document);

    metadata = (turbo_flow_resource_metadata_t)TURBO_FLOW_RESOURCE_METADATA_INIT;
    check_int_eq(turbo_flow_resource_metadata_at(flow, 1u, &metadata), TURBO_OK);
    check_str_eq(metadata.uid, "postgresql:pg.query");
    check_int_eq(
        turbo_flow_resource_document_at(flow, 1u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
        TURBO_OK);
    check_str_eq(document.schema->type_name, "PostgreSqlQueryStatus");
    check_uint_eq(document.schema->schema_id, 302u);
    check_int_eq(data_bind_create_from_text(document.schema->schema_text,
                                            strlen(document.schema->schema_text), &codec, &error),
                 DATA_BIND_OK);
    check_int_eq(data_bind_parse_json(codec, document.schema->type_name,
                                      mem_buffer_const_data(document.payload),
                                      mem_buffer_used(document.payload), &value, &error),
                 DATA_BIND_OK);
    check_int_eq(data_bind_value_get_bool(data_bind_value_get(value, "started"), &started),
                 DATA_BIND_OK);
    check_int_eq(data_bind_value_get_int32(data_bind_value_get(value, "mode"), &mode),
                 DATA_BIND_OK);
    check_int_eq(started, 0);
    check_int_eq(mode, 1);
    payload =
        tstr_new_len(mem_buffer_const_data(document.payload), mem_buffer_used(document.payload));
    check_not_null(payload);
    check_null(strstr(payload, "db-secret"));
    check_null(strstr(payload, query_config.statement));
    check_null(strstr(payload, query_config.statement_name));

    tstr_freep(&payload);
    data_bind_value_free(value);
    data_bind_free(codec);
    turbo_flow_resource_document_cleanup(&document);
    turbo_flow_destroy(flow);
  }

  it("registers a parameterized PostgreSQL sink") {
    static const char *dsl = "source input\n"
                             "stage persist adapter pg.out\n"
                             "stage main {\n"
                             "  input -> persist\n"
                             "}\n";
    turbo_flow_pgsql_sink_config_t config = {"host=127.0.0.1 port=1 connect_timeout=1",
                                             "insert into events(payload) values($1)", 1};
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_adapter_schema_t *schema;
    check_not_null(flow);
    check_int_eq(turbo_flow_pgsql_register_sink_adapter(flow, "pg.out", &config), TURBO_OK);
    schema = turbo_flow_find_adapter_schema(flow, "pg.out");
    check_not_null(schema);
    check_int_eq(schema->kind, TURBO_FLOW_ADAPTER_KIND_POSTGRESQL);
    check_int_eq(schema->roles, TURBO_FLOW_ADAPTER_SINK);
    check_int_eq(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("requires a payload parameter") {
    turbo_flow_pgsql_sink_config_t config = {"host=127.0.0.1", "select 1", 0};
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_pgsql_register_sink_adapter(flow, "pg.out", &config), TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("normalizes PostgreSQL parameter content bindings at registration") {
    turbo_flow_pgsql_sink_config_t config = {"host=127.0.0.1",
                                             "insert into events(payload) values($1)", 0};
    turbo_flow_content_descriptor_t match;
    turbo_flow_data_schema_t schema = {sizeof(turbo_flow_data_schema_t),
                                       TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
                                       TURBO_FLOW_DATA_ENCODING_JSON,
                                       "database.parameters",
                                       "EventParameter",
                                       "test.parameter",
                                       21u,
                                       1u,
                                       NULL};
    turbo_flow_content_binding_t binding = TURBO_FLOW_CONTENT_BINDING_INIT;
    turbo_flow_schema_registry_t *registry = turbo_flow_schema_registry_create();
    turbo_flow_t *flow;

    check_not_null(registry);
    config.parameter_content_type = "application/x-postgresql-opaque";
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_pgsql_register_sink_adapter(flow, "pg.opaque", &config), TURBO_OK);
    turbo_flow_destroy(flow);

    binding.registry = registry;
    config.parameter_content_binding = &binding;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_pgsql_register_sink_adapter(flow, "pg.registry-only", &config),
                 TURBO_OK);
    turbo_flow_destroy(flow);

    config.parameter_content_type = "application/json";
    binding.schema.schema_name = schema.schema_name;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_pgsql_register_sink_adapter(flow, "pg.incomplete", &config),
                 TURBO_EINVAL);
    turbo_flow_destroy(flow);

    check_int_eq(turbo_flow_content_descriptor_init(&match, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
                                                    TURBO_FLOW_CONTENT_PROFILE_DATABASE_PARAMETERS,
                                                    TURBO_FLOW_DATA_ENCODING_JSON,
                                                    "application/json", NULL),
                 TURBO_OK);
    check_int_eq(turbo_flow_schema_registry_register(registry, &match, &schema), TURBO_OK);
    binding.registry = registry;
    binding.schema.type_name = schema.type_name;
    binding.schema.schema_version = schema.schema_version;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_pgsql_register_sink_adapter(flow, "pg.typed", &config), TURBO_OK);
    turbo_flow_destroy(flow);

    binding.schema.schema_version = 2u;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_int_eq(turbo_flow_pgsql_register_sink_adapter(flow, "pg.mismatch", &config),
                 TURBO_EPROTO);
    turbo_flow_destroy(flow);
    turbo_flow_schema_registry_destroy(registry);
  }

  it("serializes tuple results as strict CSV with a database descriptor") {
    static const char *const names[] = {"id", "symbol"};
    static const Oid oids[] = {23u, 25u};
    static const int formats[] = {0, 0};
    static const char *const cells[] = {"11", "A,B", "12", "A\"B"};
    turbo_flow_msg_t msg;
    const turbo_flow_content_descriptor_t *descriptor;
    PGresult *result = pgsql_make_result(names, oids, formats, 2u, cells, 2u);
    check_not_null(result);
    turbo_flow_msg_init(&msg);
    check_int_eq(pgsql_result_to_message(result, "orders.list", 10u, 1024u,
                                         TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV, &msg),
                 TURBO_OK);
    check_str_eq(msg.owned_payload, "id,symbol\n11,\"A,B\"\n12,\"A\"\"B\"\n");
    descriptor = turbo_flow_msg_content_descriptor(&msg);
    check_not_null(descriptor);
    check_int_eq(descriptor->profile, TURBO_FLOW_CONTENT_PROFILE_DATABASE_ROWSET);
    check_int_eq(descriptor->encoding, TURBO_FLOW_DATA_ENCODING_CSV);
    check_bits(descriptor->flags, TURBO_FLOW_CONTENT_BATCH);
    check_str_eq(descriptor->media_type, "text/csv");
    check_str_eq(descriptor->identity, "orders.list");
    turbo_flow_msg_cleanup(&msg);
    PQclear(result);
  }

  it("binds PostgreSQL CSV rows to a DataBind list of objects") {
    static const char *const names[] = {"id", "symbol"};
    static const Oid oids[] = {23u, 25u};
    static const int formats[] = {0, 0};
    static const char *const cells[] = {"11", "ABCD", "12", "EFGH"};
    static const char schema[] = "message Order { uint32 id; string symbol; }\n";
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBind *databind = NULL;
    DataBindValue *batch = NULL;
    turbo_flow_msg_t msg;
    PGresult *result = pgsql_make_result(names, oids, formats, 2u, cells, 2u);
    check_not_null(result);
    turbo_flow_msg_init(&msg);
    check_int_eq(pgsql_result_to_message(result, "orders.list", 10u, 1024u,
                                         TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV, &msg),
                 TURBO_OK);
    check_int_eq(data_bind_create_from_text(schema, strlen(schema), &databind, &error),
                 DATA_BIND_OK);
    check_int_eq(data_bind_parse_csv_all(databind, "Order", msg.payload.data, msg.payload.len,
                                         &batch, &error),
                 DATA_BIND_OK);
    check_not_null(batch);
    check_int_eq(data_bind_value_kind(batch), DATA_BIND_VALUE_LIST);
    check_size_eq(data_bind_value_count(batch), 2u);
    check_int_eq(data_bind_value_as_int(data_bind_value_get(data_bind_value_at(batch, 0u), "id")),
                 11);
    check_str_eq(
        data_bind_value_as_string(data_bind_value_get(data_bind_value_at(batch, 1u), "symbol")),
        "EFGH");
    data_bind_value_free(batch);
    data_bind_free(databind);
    turbo_flow_msg_cleanup(&msg);
    PQclear(result);
  }

  it("preserves duplicate columns and SQL NULL in canonical JSON rowsets") {
    static const char *const names[] = {"value", "value"};
    static const Oid oids[] = {23u, 25u};
    static const int formats[] = {0, 0};
    static const char *const cells[] = {"11", NULL};
    turbo_flow_msg_t msg;
    const turbo_flow_content_descriptor_t *descriptor;
    PGresult *result = pgsql_make_result(names, oids, formats, 2u, cells, 1u);
    check_not_null(result);
    turbo_flow_msg_init(&msg);
    check_int_eq(pgsql_result_to_message(result, "orders.raw", 10u, 2048u,
                                         TURBO_FLOW_PGSQL_RESULT_ROWSET_JSON, &msg),
                 TURBO_OK);
    check_str_contains(msg.owned_payload,
                       "\"columns\":[{\"name\":\"value\",\"oid\":\"23\",\"format\":0}");
    check_str_contains(msg.owned_payload, "{\"name\":\"value\",\"oid\":\"25\",\"format\":0}");
    check_str_contains(msg.owned_payload, "\"rows\":[[\"11\",null]]");
    descriptor = turbo_flow_msg_content_descriptor(&msg);
    check_not_null(descriptor);
    check_int_eq(descriptor->profile, TURBO_FLOW_CONTENT_PROFILE_DATABASE_ROWSET);
    check_int_eq(descriptor->encoding, TURBO_FLOW_DATA_ENCODING_JSON);
    check_bits(descriptor->flags, TURBO_FLOW_CONTENT_BATCH);
    check_str_eq(descriptor->media_type, "application/vnd.turboflow.pg-rowset+json");
    turbo_flow_msg_cleanup(&msg);
    PQclear(result);
  }

  it("rejects CSV rows that cannot preserve PostgreSQL semantics") {
    static const Oid oids[] = {23u, 25u};
    static const int text_formats[] = {0, 0};
    static const int binary_formats[] = {0, 1};
    static const char *const cells_with_null[] = {"11", NULL};
    static const char *const cells[] = {"11", "ABCD"};
    static const char *const duplicate_names[] = {"id", "id"};
    static const char *const names[] = {"id", "symbol"};
    turbo_flow_msg_t msg;
    PGresult *result;

    turbo_flow_msg_init(&msg);
    result = pgsql_make_result(names, oids, text_formats, 2u, cells_with_null, 1u);
    check_not_null(result);
    check_int_eq(pgsql_result_to_message(result, "orders.list", 10u, 1024u,
                                         TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV, &msg),
                 TURBO_ENOTSUP);
    PQclear(result);

    result = pgsql_make_result(duplicate_names, oids, text_formats, 2u, cells, 1u);
    check_not_null(result);
    check_int_eq(pgsql_result_to_message(result, "orders.list", 10u, 1024u,
                                         TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV, &msg),
                 TURBO_ENOTSUP);
    PQclear(result);

    result = pgsql_make_result(names, oids, binary_formats, 2u, cells, 1u);
    check_not_null(result);
    check_int_eq(pgsql_result_to_message(result, "orders.list", 10u, 1024u,
                                         TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV, &msg),
                 TURBO_ENOTSUP);
    PQclear(result);
    turbo_flow_msg_cleanup(&msg);
  }

  it("enforces row and byte limits before publishing PostgreSQL CSV") {
    static const char *const names[] = {"id"};
    static const Oid oids[] = {23u};
    static const int formats[] = {0};
    static const char *const cells[] = {"12345", "67890"};
    turbo_flow_msg_t msg;
    PGresult *result = pgsql_make_result(names, oids, formats, 1u, cells, 2u);
    check_not_null(result);
    turbo_flow_msg_init(&msg);
    check_int_eq(pgsql_result_to_message(result, "orders.list", 1u, 1024u,
                                         TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV, &msg),
                 TURBO_ENOSPC);
    check_int_eq(pgsql_result_to_message(result, "orders.list", 10u, 8u,
                                         TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV, &msg),
                 TURBO_EFBIG);
    turbo_flow_msg_cleanup(&msg);
    PQclear(result);
  }

  it("validates PostgreSQL record-store ABI and rejects foreign backends") {
    turbo_flow_pgsql_record_store_config_t config = TURBO_FLOW_PGSQL_RECORD_STORE_CONFIG_INIT;
    turbo_flow_record_store_t store = TURBO_FLOW_RECORD_STORE_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    static const char yaml[] =
        "version: 1\nchannels:\n  mqtt.sessions:\n    kind: record_store\n"
        "    config:\n      backend: sqlite\n      database_path: ':memory:'\n"
        "      namespace_name: mqtt.sessions\nadapters: {}\n";

    config.version = 99u;
    config.conninfo = "host=127.0.0.1 port=1";
    config.namespace_name = "mqtt.sessions";
    config.max_records = 1u;
    pgsql_test_storage_t storage = {0};
    check_int_eq(pgsql_test_record_store_open(&config, NULL, NULL, &store, &storage, &error),
                 TURBO_EINVAL);
    check_null(store.ctx);
    check_int_eq(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error),
                 TURBO_OK);
    check_int_eq(pgsql_test_record_store_open(NULL, resolved, "mqtt.sessions", &store, &storage,
                                              &error),
                 TURBO_ENOTSUP);
    check_str_contains(error.path, "backend");
    turbo_flow_resolved_config_destroy(resolved);
  }
}
