#include "../../../tests/flow_operation_fixture.h"
#include "flow_provider_instance_internal.h"
#include "tinytest.h"
#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resource.h"
#include "turbo_flow_turbodb_resource.h"

#include <salts/plugin.h>

#include <stdio.h>
#include <string.h>

#ifndef FLOW_TURBODB_PROVIDER_FIXTURE
#error "FLOW_TURBODB_PROVIDER_FIXTURE is required"
#endif
#ifndef FLOW_TURBODB_RESOURCE_FIXTURE
#error "FLOW_TURBODB_RESOURCE_FIXTURE is required"
#endif
#ifndef FLOW_TURBODB_RESOURCE_FIXTURE_DB
#error "FLOW_TURBODB_RESOURCE_FIXTURE_DB is required"
#endif
#ifndef FLOW_TURBODB_SQLITE_DRIVER
#error "FLOW_TURBODB_SQLITE_DRIVER is required"
#endif

static const char INBOX_META_DDL[] =
    "CREATE TABLE orders_inbox_meta_v3 ("
    "singleton_id integer primary key not null, schema_magic text not null, "
    "schema_version integer not null, generation bigint not null, "
    "owner_state integer not null, next_record_id bigint not null, "
    "next_claim_token bigint not null, max_records bigint not null, "
    "max_total_bytes bigint not null, max_record_bytes bigint not null, "
    "max_claims bigint not null, records bigint not null, "
    "history_records bigint not null, pending_records bigint not null, "
    "failed_records bigint not null, in_flight_claims bigint not null, "
    "retained_bytes bigint not null, admitted bigint not null, "
    "completed bigint not null, failed bigint not null, retried bigint not null, "
    "discarded bigint not null)";

static const char INBOX_RECORDS_DDL[] =
    "CREATE TABLE orders_inbox_records_v3 ("
    "record_id bigint primary key not null, phase integer not null, "
    "claim_generation bigint not null, claim_token bigint not null, "
    "failure_status integer not null, failure_kind integer not null, "
    "terminal_kind integer not null, envelope_schema text not null, "
    "envelope_schema_version integer not null, source_id bytea not null, "
    "partition_key bytea not null, admission_id bytea not null, "
    "source_sequence_be bytea not null, timestamp_ns_be bytea not null, "
    "message_type bigint not null, message_flags bigint not null, "
    "content_domain integer not null, content_profile integer not null, "
    "content_encoding integer not null, content_flags bigint not null, "
    "content_schema_version bigint not null, content_media_type text not null, "
    "content_schema_name text not null, content_type_name text not null, "
    "content_identity text not null, correlation bytea not null, "
    "payload bytea not null, retained_bytes bigint not null)";

static const char INBOX_DEDUPE_INDEX_DDL[] =
    "CREATE UNIQUE INDEX orders_inbox_records_v3_admission ON "
    "orders_inbox_records_v3(source_id, admission_id)";

static const char INBOX_PHASE_INDEX_DDL[] =
    "CREATE INDEX orders_inbox_records_v3_phase ON "
    "orders_inbox_records_v3(phase, record_id)";

static const char INBOX_META_V3_ROW[] =
    "INSERT INTO orders_inbox_meta_v3 VALUES "
    "(1, 'turbo-flow.turbodb.inbox', 3, 0, 0, 1, 1, 4, 256, 128, 2, "
    "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)";

typedef struct resolver_fixture_s {
  salts_plugin_registry *registry;
  salts_plugin_ref plugin;
  unsigned calls;
} resolver_fixture_t;

static void sql(orm_connection_t *connection, const char *text) {
  orm_error_t error;
  orm_query_t *query = NULL;
  orm_result_t *result = NULL;
  orm_error_init(&error);
  check_equal(orm_raw(connection, orm_view(text), &query, &error), ORM_STATUS_OK);
  check_equal(orm_query_execute(query, &result, &error), ORM_STATUS_OK);
  orm_result_destroy(result);
  orm_query_destroy(query);
}

static orm_runtime_t *sqlite_runtime(orm_error_t *error) {
  orm_runtime_config_t runtime_config;
  orm_driver_load_config_t load = {0};
  orm_runtime_t *runtime = NULL;

  orm_runtime_config_init(&runtime_config);
  check_equal(
      orm_runtime_create(&runtime_config, &runtime, error),
      ORM_STATUS_OK);
  check_not_null(runtime);

  load.struct_size = (uint32_t)sizeof(load);
  load.abi_version = ORM_RUNTIME_ABI_VERSION;
  load.module_path = orm_view(FLOW_TURBODB_SQLITE_DRIVER);
  load.expected_driver_id = orm_view("sqlite");
  check_equal(
      orm_runtime_load_driver(runtime, &load, error),
      ORM_STATUS_OK);
  return runtime;
}

static void provision_database(void) {
  orm_config_t database;
  orm_option_t filename;
  orm_runtime_t *runtime;
  orm_connection_t *connection = NULL;
  orm_error_t error;

  (void)remove(FLOW_TURBODB_RESOURCE_FIXTURE_DB);
  orm_error_init(&error);
  runtime = sqlite_runtime(&error);

  orm_config(&database);
  filename.keyword = orm_view("filename");
  filename.value = orm_view(FLOW_TURBODB_RESOURCE_FIXTURE_DB);
  database.driver = orm_view("sqlite");
  database.options = &filename;
  database.option_count = 1u;

  check_equal(
      orm_runtime_connect(runtime, &database, &connection, &error),
      ORM_STATUS_OK);
  check_not_null(connection);
  sql(connection, INBOX_META_DDL);
  sql(connection, INBOX_RECORDS_DDL);
  sql(connection, INBOX_DEDUPE_INDEX_DDL);
  sql(connection, INBOX_PHASE_INDEX_DDL);
  sql(connection, INBOX_META_V3_ROW);
  orm_disconnect(connection);
  check_equal(orm_runtime_close(runtime, &error), ORM_STATUS_OK);
  orm_runtime_release(runtime);
}

static int output(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  (void)ctx;
  return SALTS_OK;
}

static int resolve_provider(
    void *ctx, const char *provider_identity,
    turbo_flow_provider_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  resolver_fixture_t *fixture = (resolver_fixture_t *)ctx;
  if (!fixture || !out || out->size != sizeof(*out)) return SALTS_EINVAL;
  ++fixture->calls;
  if (!provider_identity ||
      strcmp(provider_identity, "flow.durable.turbodb") != 0) {
    if (error && error->size == sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_ENOENT;
    }
    return SALTS_ENOENT;
  }
  out->module_identity = "turbo-flow.durable.turbodb";
  out->registry = fixture->registry;
  out->plugin = fixture->plugin;
  return SALTS_OK;
}

static int resolve_resource(
    void *ctx, const char *resource_name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_resource_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  resolver_fixture_t *fixture = (resolver_fixture_t *)ctx;
  if (!fixture || !requirement || !out || out->size != sizeof(*out))
    return SALTS_EINVAL;
  ++fixture->calls;
  if (!resource_name || strcmp(resource_name, "telemetry_db") != 0 ||
      !requirement->contract_id ||
      strcmp(requirement->contract_id,
             TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONTRACT_ID) != 0) {
    if (error && error->size == sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_ENOENT;
    }
    return SALTS_ENOENT;
  }
  out->identity = "deployment.telemetry.primary";
  out->registry = fixture->registry;
  out->plugin = fixture->plugin;
  out->export_id = "fixture.turbodb.database";
  return SALTS_OK;
}

typedef struct provider_acceptance_fixture_s {
  turbo_flow_t *flow;
  salts_plugin_registry registry;
  int registry_initialized;
  salts_plugin_ref provider_ref;
  salts_plugin_ref resource_ref;
  int provider_loaded;
  int resource_loaded;
  int provider_started;
  int resource_started;
  flow_compiled_provider_instance_t *compiled;
  turbo_flow_runtime_owner *owner;
  int database_created;
} provider_acceptance_fixture_t;

static provider_acceptance_fixture_t acceptance;

static void acceptance_cleanup(void) {
  turbo_flow_runtime_owner *owner = acceptance.owner;

  if (acceptance.compiled) {
    if (!owner) {
      turbo_flow_runtime_owner *borrowed = NULL;
      if (flow_compiled_provider_instance_owner(
              acceptance.compiled, &borrowed) == SALTS_OK)
        owner = borrowed;
    }
    if (owner && turbo_flow_runtime_owner_contract_valid(owner)) {
      (void)turbo_flow_runtime_owner_quiesce(owner, 10u);
      (void)turbo_flow_runtime_owner_drain(owner, 10u);
      if (turbo_flow_runtime_owner_shutdown(owner) == SALTS_OK)
        (void)flow_compiled_provider_instance_owner_destroy(
            acceptance.compiled);
    }
    acceptance.owner = NULL;
    (void)flow_compiled_provider_instance_release(&acceptance.compiled);
  }

  if (acceptance.registry_initialized) {
    bool quiescent = false;
    if (acceptance.provider_started) {
      (void)salts_plugin_registry_request_stop(
          &acceptance.registry, acceptance.provider_ref);
      acceptance.provider_started = 0;
    }
    if (acceptance.resource_started) {
      (void)salts_plugin_registry_request_stop(
          &acceptance.registry, acceptance.resource_ref);
      acceptance.resource_started = 0;
    }
    if (acceptance.provider_loaded &&
        salts_plugin_registry_poll_quiescent(
            &acceptance.registry, acceptance.provider_ref,
            &quiescent) == SALTS_PLUGIN_OK &&
        quiescent) {
      if (salts_plugin_registry_unload(
              &acceptance.registry,
              acceptance.provider_ref) == SALTS_PLUGIN_OK)
        acceptance.provider_loaded = 0;
    }
    quiescent = false;
    if (acceptance.resource_loaded &&
        salts_plugin_registry_poll_quiescent(
            &acceptance.registry, acceptance.resource_ref,
            &quiescent) == SALTS_PLUGIN_OK &&
        quiescent) {
      if (salts_plugin_registry_unload(
              &acceptance.registry,
              acceptance.resource_ref) == SALTS_PLUGIN_OK)
        acceptance.resource_loaded = 0;
    }
    if (!acceptance.provider_loaded && !acceptance.resource_loaded) {
      (void)salts_plugin_registry_destroy(&acceptance.registry);
      acceptance.registry_initialized = 0;
    }
  }

  if (acceptance.flow) {
    turbo_flow_destroy(acceptance.flow);
    acceptance.flow = NULL;
  }
  if (acceptance.database_created) {
    (void)remove(FLOW_TURBODB_RESOURCE_FIXTURE_DB);
    acceptance.database_created = 0;
  }
}

spec("TurboDB canonical Salts provider") {
  before_each() {
    memset(&acceptance, 0, sizeof(acceptance));
    acceptance.flow = turbo_flow_create();
  }

  after_each() { acceptance_cleanup(); }

  it("materializes typed policy with an exact leased database resource") {
    static const char *src =
        "source input\n"
        "buffer durable_buffer provider flow.durable.turbodb {\n"
        "  resource telemetry_db\n"
        "  schema_version 2\n"
        "  max_message_bytes 128\n"
        "  max_records 4\n"
        "  max_total_bytes 256\n"
        "  max_record_bytes 128\n"
        "  max_claims 2\n"
        "  connection_count 2\n"
        "  identity_mode 1\n"
        "  expected_generation 0\n"
        "  open_mode 0\n"
        "}\n"
        "stage output operation test.output\n"
        "stage main {\n"
        "  input -> durable_buffer -> output\n"
        "}\n";
    salts_plugin_registry_config registry_config = {2u};
    resolver_fixture_t provider_fixture = {0};
    resolver_fixture_t resource_fixture = {0};
    turbo_flow_provider_resolver_v1_t provider_resolver =
        TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
    turbo_flow_resource_resolver_v1_t resource_resolver =
        TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
    const turbo_flow_provider_instance_v1_t *view = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    flow_test_operation_t operation =
        flow_test_operation_init("test.output", output, NULL);
    int stage_index;
    int rc;
    bool quiescent = true;

    acceptance.database_created = 1;
    provision_database();
    check_not_null(acceptance.flow);
    check_equal(
        flow_test_operation_register(acceptance.flow, &operation),
        SALTS_OK);
    check_equal(
        turbo_flow_parse_string(acceptance.flow, src, strlen(src)),
        SALTS_OK);
    stage_index =
        turbo_flow_find_stage(acceptance.flow, "durable_buffer");
    check_true(stage_index >= 0);

    rc = salts_plugin_registry_init(
        &acceptance.registry, &registry_config);
    if (rc == SALTS_PLUGIN_OK) acceptance.registry_initialized = 1;
    check_equal(rc, SALTS_PLUGIN_OK);

    rc = salts_plugin_registry_load(
        &acceptance.registry, FLOW_TURBODB_PROVIDER_FIXTURE,
        &acceptance.provider_ref);
    if (rc == SALTS_PLUGIN_OK) acceptance.provider_loaded = 1;
    check_equal(rc, SALTS_PLUGIN_OK);

    rc = salts_plugin_registry_load(
        &acceptance.registry, FLOW_TURBODB_RESOURCE_FIXTURE,
        &acceptance.resource_ref);
    if (rc == SALTS_PLUGIN_OK) acceptance.resource_loaded = 1;
    check_equal(rc, SALTS_PLUGIN_OK);

    rc = salts_plugin_registry_start(
        &acceptance.registry, acceptance.provider_ref);
    if (rc == SALTS_PLUGIN_OK) acceptance.provider_started = 1;
    check_equal(rc, SALTS_PLUGIN_OK);

    rc = salts_plugin_registry_start(
        &acceptance.registry, acceptance.resource_ref);
    if (rc == SALTS_PLUGIN_OK) acceptance.resource_started = 1;
    check_equal(rc, SALTS_PLUGIN_OK);

    provider_fixture.registry = &acceptance.registry;
    provider_fixture.plugin = acceptance.provider_ref;
    provider_resolver.ctx = &provider_fixture;
    provider_resolver.resolve = resolve_provider;
    resource_fixture.registry = &acceptance.registry;
    resource_fixture.plugin = acceptance.resource_ref;
    resource_resolver.ctx = &resource_fixture;
    resource_resolver.resolve = resolve_resource;

    rc = flow_compiled_provider_instance_prepare(
        acceptance.flow, (size_t)stage_index,
        &provider_resolver, &resource_resolver,
        &acceptance.compiled, &error);
    info("compiled TurboDB provider prepare status=%d provider_calls=%u "
         "resource_calls=%u path=%s reason=%s",
         rc, provider_fixture.calls, resource_fixture.calls,
         error.path, error.message);
    check_equal(rc, SALTS_OK);
    check_not_null(acceptance.compiled);
    check_equal(provider_fixture.calls, 1u);
    check_equal(resource_fixture.calls, 1u);
    check_equal(
        flow_compiled_provider_instance_view(
            acceptance.compiled, &view),
        SALTS_OK);
    check_not_null(view);
    check_equal(view->instance_name, "durable_buffer");
    check_equal(view->config.type_name, "DurableTurboDbConfig");
    check_not_null(view->resource);
    check_equal(view->resource->reference_name, "telemetry_db");
    check_equal(
        view->resource->identity, "deployment.telemetry.primary");

    check_equal(
        flow_compiled_provider_instance_materialize(
            acceptance.compiled, acceptance.flow, &error),
        SALTS_OK);
    check_equal(turbo_flow_compile(acceptance.flow), SALTS_OK);
    check_equal(
        flow_compiled_provider_instance_owner(
            acceptance.compiled, &acceptance.owner),
        SALTS_OK);
    check_not_null(acceptance.owner);
    check_true(
        turbo_flow_runtime_owner_contract_valid(acceptance.owner));

    rc = salts_plugin_registry_request_stop(
        &acceptance.registry, acceptance.provider_ref);
    if (rc == SALTS_PLUGIN_OK) acceptance.provider_started = 0;
    check_equal(rc, SALTS_PLUGIN_OK);
    rc = salts_plugin_registry_request_stop(
        &acceptance.registry, acceptance.resource_ref);
    if (rc == SALTS_PLUGIN_OK) acceptance.resource_started = 0;
    check_equal(rc, SALTS_PLUGIN_OK);

    check_equal(
        salts_plugin_registry_poll_quiescent(
            &acceptance.registry, acceptance.provider_ref, &quiescent),
        SALTS_PLUGIN_OK);
    check_false(quiescent);
    check_equal(
        salts_plugin_registry_poll_quiescent(
            &acceptance.registry, acceptance.resource_ref, &quiescent),
        SALTS_PLUGIN_OK);
    check_false(quiescent);

    check_equal(
        turbo_flow_runtime_owner_quiesce(acceptance.owner, 10u),
        SALTS_OK);
    check_equal(
        turbo_flow_runtime_owner_drain(acceptance.owner, 10u),
        SALTS_OK);
    check_equal(
        turbo_flow_runtime_owner_shutdown(acceptance.owner),
        SALTS_OK);
    check_equal(
        flow_compiled_provider_instance_owner_destroy(
            acceptance.compiled),
        SALTS_OK);
    acceptance.owner = NULL;
    check_equal(
        flow_compiled_provider_instance_release(
            &acceptance.compiled),
        SALTS_OK);
    check_null(acceptance.compiled);

    check_equal(
        salts_plugin_registry_poll_quiescent(
            &acceptance.registry, acceptance.provider_ref, &quiescent),
        SALTS_PLUGIN_OK);
    check_true(quiescent);
    rc = salts_plugin_registry_unload(
        &acceptance.registry, acceptance.provider_ref);
    if (rc == SALTS_PLUGIN_OK) acceptance.provider_loaded = 0;
    check_equal(rc, SALTS_PLUGIN_OK);

    check_equal(
        salts_plugin_registry_poll_quiescent(
            &acceptance.registry, acceptance.resource_ref, &quiescent),
        SALTS_PLUGIN_OK);
    check_true(quiescent);
    rc = salts_plugin_registry_unload(
        &acceptance.registry, acceptance.resource_ref);
    if (rc == SALTS_PLUGIN_OK) acceptance.resource_loaded = 0;
    check_equal(rc, SALTS_PLUGIN_OK);

    rc = salts_plugin_registry_destroy(&acceptance.registry);
    if (rc == SALTS_PLUGIN_OK) acceptance.registry_initialized = 0;
    check_equal(rc, SALTS_PLUGIN_OK);

    turbo_flow_destroy(acceptance.flow);
    acceptance.flow = NULL;
    check_equal(remove(FLOW_TURBODB_RESOURCE_FIXTURE_DB), 0);
    acceptance.database_created = 0;
  }
}
