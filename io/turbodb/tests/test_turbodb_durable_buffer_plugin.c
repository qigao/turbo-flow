#include "../../../tests/flow_operation_fixture.h"
#include "../../durable/tests/durable_provider_conformance.h"
#include "tinytest.h"
#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_turbodb.h"
#include <salts/clock.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { YAML_BYTES = 4096, TEST_TIMEOUT_MS = 1000 };
static const char yaml_format[] =
    "version: 1\nchannels:\n  intake.store:\n    kind: flow.durable.turbodb\n    config:\n"
    "      schema_version: 2\n      identity_mode: stable_required\n"
    "      filename: '%s'\n      namespace: orders\n"
    "      max_message_bytes: 1048576\n      max_records: 2\n"
    "      max_total_bytes: 67108864\n      max_record_bytes: 1048576\n"
    "      max_claims: 1\n      connection_count: 4\n      open_mode: exclusive\n"
    "      expected_generation: 0\n";
static const char takeover_yaml_format[] =
    "version: 1\nchannels:\n  intake.store:\n    kind: flow.durable.turbodb\n    config:\n"
    "      schema_version: 2\n      identity_mode: stable_required\n"
    "      filename: '%s'\n      namespace: orders\n"
    "      max_message_bytes: 1048576\n      max_records: 2\n"
    "      max_total_bytes: 67108864\n      max_record_bytes: 1048576\n"
    "      max_claims: 1\n      connection_count: 4\n      open_mode: takeover\n"
    "      expected_generation: 1\n";
static const char graph_text[] =
    "source input\nbuffer intake resource intake.store\n"
    "stage output operation test.output\nstage main {\n input -> intake -> output\n}\n";
static const char INBOX_META_DDL[] =
    "CREATE TABLE orders_inbox_meta_v2 ("
    "singleton_id integer primary key not null, schema_magic text not null, "
    "schema_version integer not null, generation bigint not null, owner_state integer not null, "
    "next_record_id bigint not null, next_claim_token bigint not null, max_records bigint not "
    "null, max_total_bytes bigint not null, max_record_bytes bigint not null, max_claims bigint "
    "not null, records bigint not null, history_records bigint not null, pending_records bigint "
    "not null, failed_records bigint not null, in_flight_claims bigint not null, retained_bytes "
    "bigint not null, admitted bigint "
    "not null, completed bigint not null, failed bigint not null, retried bigint not null, "
    "discarded bigint not null)";

static const char INBOX_RECORDS_DDL[] =
    "CREATE TABLE orders_inbox_records_v2 ("
    "record_id bigint primary key not null, phase integer not null, claim_generation bigint not "
    "null, "
    "claim_token bigint not null, failure_status integer not null, failure_kind integer not null, "
    "terminal_kind integer not null, envelope_schema text not null, envelope_schema_version "
    "integer not null, source_id bytea not null, admission_id bytea not null, source_sequence_be "
    "bytea not null, timestamp_ns_be bytea not null, message_type bigint not null, message_flags "
    "bigint not null, content_domain integer not null, content_profile integer not null, "
    "content_encoding integer not null, content_flags bigint not null, content_schema_version "
    "bigint not null, content_media_type text not null, content_schema_name text not null, "
    "content_type_name text not null, content_identity text not null, correlation bytea not null, "
    "payload bytea not null, retained_bytes bigint not null)";

static const char INBOX_DEDUPE_INDEX_DDL[] =
    "CREATE UNIQUE INDEX orders_inbox_records_v2_admission ON "
    "orders_inbox_records_v2(source_id, admission_id)";

static const char INBOX_PHASE_INDEX_DDL[] =
    "CREATE INDEX orders_inbox_records_v2_phase ON orders_inbox_records_v2(phase, record_id)";

static const char INBOX_META_V2_ROW[] =
    "INSERT INTO orders_inbox_meta_v2 VALUES "
    "(1, 'turbo-flow.turbodb.inbox', 2, 0, 0, 1, 1, 2, 67108864, 1048576, 1, "
    "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)";

static const char INBOX_META_OLD_ROW[] =
    "INSERT INTO orders_inbox_meta_v2 VALUES "
    "(1, 'turbo-flow.turbodb.inbox', 1, 0, 0, 1, 1, 2, 67108864, 1048576, 1, "
    "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)";


typedef struct fixture_s {
  char *path;
  char yaml[YAML_BYTES];
  orm_option_t filename;
  orm_config_t database;
  orm_connection_t *db;
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  turbo_flow_resolved_config_t *resolved;
  turbo_flow_t *flow;
  turbo_flow_t *execution_flow;
  turbo_flow_plugin_generation_t *generation;
  turbo_flow_plugin_generation_t *cleanup;
  atomic_size_t delivered;
  atomic_size_t delivered_after_stop;
} fixture_t;
static void sql(fixture_t *f, const char *text) {
  orm_error_t e; orm_query_t *q = NULL; orm_result_t *r = NULL;
  orm_error_init(&e);
  check_equal(orm_raw(f->db, orm_view(text), &q, &e), ORM_STATUS_OK);
  check_equal(orm_query_execute(q, &r, &e), ORM_STATUS_OK);
  orm_result_destroy(r); orm_query_destroy(q);
}
static int64_t scalar(fixture_t *f, const char *text) {
  orm_error_t e; orm_query_t *q = NULL; orm_result_t *r = NULL;
  int64_t value = -1;
  orm_error_init(&e);
  check_equal(orm_raw(f->db, orm_view(text), &q, &e), ORM_STATUS_OK);
  check_equal(orm_query_execute(q, &r, &e), ORM_STATUS_OK);
  check_equal(orm_result_get_int64(r, 0u, 0u, &value, &e), ORM_STATUS_OK);
  orm_result_destroy(r); orm_query_destroy(q);
  return value;
}
static int output(turbo_flow_msg_t *msg, void *ctx) {
  fixture_t *f = ctx;
  check_equal(msg->payload.len, (size_t)7u);
  check_equal(memcmp(msg->payload.data, "payload", 7u), 0);
  check_null(msg->transport_context);
  if (turbo_flow_state(f->execution_flow) != TURBO_FLOW_STATE_STARTED) ++f->delivered_after_stop;
  ++f->delivered;
  return SALTS_OK;
}
static void resolve(fixture_t *f, const char *yaml) {
  turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_resolved_config_destroy(f->resolved); f->resolved = NULL;
  check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &f->resolved, &e), SALTS_OK);
}
static void open_fixture(fixture_t *f, const char *graph, int schema) {
  turbo_flow_plugin_host_config_t hc = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t e = TURBO_FLOW_PLUGIN_ERROR_INIT;
  orm_error_t oe;
  const char *path = getenv("TURBO_FLOW_TURBODB_PLUGIN_PATH");
  memset(f, 0, sizeof(*f));
  atomic_init(&f->delivered, 0u); atomic_init(&f->delivered_after_stop, 0u);
  f->path = tt_make_temp_file("turbo-flow-durable-plugin", ".sqlite3");
  check_not_null(f->path);
  orm_config(&f->database);
  f->filename.keyword = orm_view("filename"); f->filename.value = orm_view(f->path);
  f->database.driver = orm_view("sqlite"); f->database.options = &f->filename;
  f->database.option_count = 1u;
  orm_error_init(&oe);
  check_equal(orm_connect(&f->database, &f->db, &oe), ORM_STATUS_OK);
  check_not_null(f->db);
  /* DELETE journaling permits a held reader to reject the writer's COMMIT. */
  sql(f, "PRAGMA journal_mode=DELETE");
  if (schema) {
    sql(f, INBOX_META_DDL); sql(f, INBOX_RECORDS_DDL);
    sql(f, INBOX_DEDUPE_INDEX_DDL); sql(f, INBOX_PHASE_INDEX_DDL);
    sql(f, schema == 1 ? INBOX_META_V2_ROW : INBOX_META_OLD_ROW);
  }
  check_not_null(path);
  check_equal(turbo_flow_plugin_host_create(&hc, &f->host, &e), SALTS_OK);
  check_equal(turbo_flow_plugin_host_load(f->host, path, &e), SALTS_OK);
  check_equal(turbo_flow_plugin_catalog_snapshot_create(f->host, &f->snapshot, &e), SALTS_OK);
  (void)snprintf(f->yaml, sizeof(f->yaml), yaml_format, f->path);
  resolve(f, f->yaml);
  f->flow = turbo_flow_create(); check_not_null(f->flow); f->execution_flow = f->flow;
  flow_test_operation_t op = flow_test_operation_init("test.output", output, f);
  check_equal(flow_test_operation_register(f->flow, &op), SALTS_OK);
  check_equal(turbo_flow_parse_string(f->flow, graph, strlen(graph)), SALTS_OK);
}
static void close_fixture(fixture_t *f) {
  turbo_flow_config_error_t ce = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  if (f->generation) check_equal(turbo_flow_plugin_generation_destroy(f->generation, TEST_TIMEOUT_MS, &ce), SALTS_OK);
  if (f->cleanup) check_equal(turbo_flow_plugin_generation_destroy(f->cleanup, TEST_TIMEOUT_MS, &ce), SALTS_OK);
  if (f->flow) turbo_flow_destroy(f->flow);
  turbo_flow_resolved_config_destroy(f->resolved);
  turbo_flow_plugin_catalog_snapshot_destroy(f->snapshot);
  check_equal(turbo_flow_plugin_host_destroy(f->host, TEST_TIMEOUT_MS, &pe), SALTS_OK);
  orm_disconnect(f->db);
  check_equal(tt_remove_file(f->path), 0); free(f->path);
}
static const turbo_flow_plugin_transactional_resource_provider_v1_t *provider(fixture_t *f) {
  turbo_flow_plugin_transactional_product_catalog_v1_t c = TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
  check_equal(turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(f->snapshot, &c), SALTS_OK);
  for (size_t i=0u; i<c.resource_provider_count; ++i)
    if (!strcmp(c.resource_providers[i].kind, "flow.durable.turbodb")) return &c.resource_providers[i];
  return NULL;
}
static int create_generation(fixture_t *f) {
  turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_generation_config_t c = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  int rc = turbo_flow_plugin_generation_create(f->snapshot, f->resolved, &f->flow, &c,
                                               NULL, &f->generation, &f->cleanup, &e);
  info("generation status=%d path=%s reason=%s", rc, e.path, e.message);
  return rc;
}
static void message_init(turbo_flow_msg_t *msg, const char *key) {
  turbo_flow_durable_identity_t identity = TURBO_FLOW_DURABLE_IDENTITY_INIT;
  turbo_flow_msg_init(msg);
  msg->owned_payload = tstr_dup("payload"); msg->payload = tstr_to_v(msg->owned_payload);
  if (!key) return;
  identity.source_id = vstr_from_buf("source", 6u);
  identity.admission_id = vstr_from_buf(key, strlen(key));
  check_equal(turbo_flow_msg_set_durable_identity(msg, &identity), SALTS_OK);
}
static int publish(turbo_flow_t *flow, const char *key) {
  turbo_flow_msg_t msg; message_init(&msg, key);
  int rc = turbo_flow_publish(flow, "input", &msg);
  turbo_flow_msg_cleanup(&msg); return rc;
}
static int conformance_publish_stable(void *ctx, const char *admission_id) {
  fixture_t *f = ctx;
  return publish(turbo_flow_plugin_generation_flow(f->generation), admission_id);
}
static int conformance_progress(void *ctx) {
  fixture_t *f = ctx;
  turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
  return turbo_flow_plugin_generation_poll(f->generation, 0u, &e);
}
static size_t conformance_delivered(void *ctx) {
  fixture_t *f = ctx;
  return atomic_load(&f->delivered);
}
static void replace_field(fixture_t *f, const char *before, const char *after, char *out) {
  const char *at = strstr(f->yaml, before); check_not_null(at);
  size_t prefix = (size_t)(at - f->yaml);
  check(prefix + strlen(after) + strlen(at + strlen(before)) < YAML_BYTES);
  memcpy(out, f->yaml, prefix); strcpy(out + prefix, after); strcat(out, at + strlen(before));
}
static void retire(fixture_t *f) {
  turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
  int rc = turbo_flow_plugin_generation_destroy(f->generation, TEST_TIMEOUT_MS, &e);
  check_equal(rc, SALTS_OK);
  if (rc == SALTS_OK) f->generation = NULL;
}
spec("configured file-backed TurboDB durable resource") {
  it("matches the shared provider-neutral durable conformance contract") {
    fixture_t f; open_fixture(&f, graph_text, 1);
    int rc = create_generation(&f); check_equal(rc, SALTS_OK);
    if (rc == SALTS_OK) {
      turbo_flow_t *flow = turbo_flow_plugin_generation_flow(f.generation);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      turbo_flow_durable_provider_conformance_v1_t contract = {
        &f, flow, conformance_publish_stable, conformance_progress, conformance_delivered
      };
      turbo_flow_durable_provider_conformance_capacity_and_replay(&contract);
    }
    close_fixture(&f);
  }
  it("registers the kind and commits the same Graph cut before downstream retirement") {
    fixture_t f; open_fixture(&f, graph_text, 1);
    check_not_null(provider(&f));
    int rc = create_generation(&f); check_equal(rc, SALTS_OK);
    if (rc == SALTS_OK) {
      check_null(f.flow);
      check_equal(turbo_flow_plugin_generation_owner_count(f.generation), (size_t)1u);
      turbo_flow_t *flow = turbo_flow_plugin_generation_flow(f.generation);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(publish(flow, NULL), SALTS_EINVAL);
      check_equal(publish(flow, "one"), SALTS_OK);
      check_equal(publish(flow, "one"), SALTS_OK);
      check_equal(publish(flow, "two"), SALTS_OK);
      check_equal(publish(flow, "three"), SALTS_ENOSPC);
      check_equal(atomic_load(&f.delivered), (size_t)0u);
      check_equal(scalar(&f, "SELECT records FROM orders_inbox_meta_v2"), (int64_t)2);
      check_equal(scalar(&f, "SELECT COUNT(*) FROM orders_inbox_records_v2 WHERE payload=X'7061796C6F6164'"), (int64_t)2);
      retire(&f);
      check_equal(atomic_load(&f.delivered), (size_t)2u);
      check_equal(atomic_load(&f.delivered_after_stop), (size_t)0u);
      check_equal(scalar(&f, "SELECT owner_state FROM orders_inbox_meta_v2"), (int64_t)0);
      check_equal(scalar(&f, "SELECT completed FROM orders_inbox_meta_v2"), (int64_t)2);
    }
    close_fixture(&f);
  }
  it("keeps preflight config-only and rejects invalid serialized fields without consuming Graph") {
    static const struct { const char *before; const char *after; } cases[] = {
      {"schema_version: 2", "schema_version: 1"},
      {"schema_version: 2", "schema_version: '2'"},
      {"schema_version: 2", "schema_version: 2\n      unknown: 1"},
      {"identity_mode: stable_required", "identity_mode: automatic"},
      {"namespace: orders", "namespace: bad-name"},
      {"connection_count: 4", "connection_count: 0"},
      {"connection_count: 4", "connection_count: '4'"},
      {"open_mode: exclusive", "open_mode: automatic"},
      {"open_mode: exclusive", "open_mode: takeover"},
      {"expected_generation: 0", "expected_generation: 1"},
      {"expected_generation: 0", "expected_generation: '0'"},
      {"max_records: 2", "max_records: 0"},
      {"max_records: 2", "max_records: -1"},
      {"max_claims: 1", "max_claims: 3"},
      {"max_total_bytes: 67108864", "max_total_bytes: 1"},
      {"max_record_bytes: 1048576", "max_record_bytes: 0"},
      {"max_message_bytes: 1048576", "max_message_bytes: 0"}
    };
    fixture_t f; open_fixture(&f, graph_text, 0);
    const turbo_flow_plugin_transactional_resource_provider_v1_t *p = provider(&f);
    check_not_null(p);
    if (p) {
      turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
      check_equal(p->preflight(p->ctx, f.resolved, "intake.store", &e), SALTS_OK);
      check_equal(scalar(&f, "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"), (int64_t)0);
      char generated_yaml[YAML_BYTES];
      replace_field(&f, "identity_mode: stable_required", "identity_mode: generated",
                    generated_yaml);
      resolve(&f, generated_yaml);
      check_equal(p->preflight(p->ctx, f.resolved, "intake.store", &e), SALTS_OK);
      resolve(&f, f.yaml);
      {
        char takeover_yaml[YAML_BYTES];
        char *open_mode;
        char *expected_generation;
        memcpy(takeover_yaml, f.yaml, strlen(f.yaml) + 1u);
        open_mode = strstr(takeover_yaml, "open_mode: exclusive");
        expected_generation = strstr(takeover_yaml, "expected_generation: 0");
        check_not_null(open_mode);
        check_not_null(expected_generation);
        if (open_mode && expected_generation) {
          const size_t tail = strlen(open_mode + strlen("open_mode: exclusive"));
          memmove(open_mode + strlen("open_mode: takeover"),
                  open_mode + strlen("open_mode: exclusive"), tail + 1u);
          memcpy(open_mode, "open_mode: takeover", strlen("open_mode: takeover"));
          expected_generation = strstr(takeover_yaml, "expected_generation: 0");
          check_not_null(expected_generation);
          if (expected_generation)
            memcpy(expected_generation, "expected_generation: 1",
                   strlen("expected_generation: 1"));
          resolve(&f, takeover_yaml);
          check_equal(p->preflight(p->ctx, f.resolved, "intake.store", &e), SALTS_OK);
          resolve(&f, f.yaml);
        }
      }
      for (size_t i=0u; i<sizeof(cases)/sizeof(cases[0]); ++i) {
        char yaml[YAML_BYTES]; replace_field(&f, cases[i].before, cases[i].after, yaml); resolve(&f, yaml);
        check_not_equal(p->preflight(p->ctx, f.resolved, "intake.store", &e), SALTS_OK);
        turbo_flow_t *original = f.flow;
        check_not_equal(create_generation(&f), SALTS_OK);
        check(f.flow == original); check_null(f.generation); check_null(f.cleanup);
      }
      char yaml[YAML_BYTES]; replace_field(&f, f.path, ":memory:", yaml); resolve(&f, yaml);
      check_not_equal(p->preflight(p->ctx, f.resolved, "intake.store", &e), SALTS_OK);
      check_not_equal(create_generation(&f), SALTS_OK); check_not_null(f.flow);
      check_equal(scalar(&f, "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"), (int64_t)0);
    }
    close_fixture(&f);
  }
  it("propagates missing old and wrong schema without migration or memory fallback") {
    for (int schema=0; schema<3; ++schema) {
      fixture_t f; open_fixture(&f, graph_text, schema);
      if (schema == 1) sql(&f, "DROP INDEX orders_inbox_records_v2_admission");
      const turbo_flow_plugin_transactional_resource_provider_v1_t *p = provider(&f);
      check_not_null(p);
      if (p) {
        turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
        check_equal(p->preflight(p->ctx, f.resolved, "intake.store", &e), SALTS_OK);
        check_not_equal(create_generation(&f), SALTS_OK);
        check_null(f.generation); check_null(f.cleanup);
        if (!schema) check_equal(scalar(&f, "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"), (int64_t)0);
        else {
          check_equal(scalar(&f, "SELECT owner_state FROM orders_inbox_meta_v2"), (int64_t)0);
          check_equal(scalar(&f, "SELECT schema_version FROM orders_inbox_meta_v2"), (int64_t)(schema == 2 ? 1 : 2));
        }
      }
      close_fixture(&f);
    }
  }
  it("checks exactly one buffer reference before creating the SQLite owner") {
    static const char *const graphs[] = {
      "source input\nstage output operation test.output\nstage main {\n input -> output\n}\n",
      "source input\nstage output operation test.output resource intake.store\nstage main {\n input -> output\n}\n",
      "source input\nbuffer a resource intake.store\nbuffer b resource intake.store\nstage output operation test.output\nstage main {\n input -> a -> b -> output\n}\n"
    };
    for (size_t i=0u; i<sizeof(graphs)/sizeof(graphs[0]); ++i) {
      fixture_t f; open_fixture(&f, graphs[i], 1);
      const turbo_flow_plugin_transactional_resource_provider_v1_t *p = provider(&f); check_not_null(p);
      if (p) {
        turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
        turbo_flow_plugin_product_owner_v1_t owner = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
        check_equal(p->preflight(p->ctx, f.resolved, "intake.store", &e), SALTS_OK);
        check_equal(p->materialize(p->ctx, f.flow, f.resolved, "intake.store", &owner, &e), SALTS_EINVAL);
        check_null(owner.ctx);
        check_equal(scalar(&f, "SELECT generation FROM orders_inbox_meta_v2"), (int64_t)0);
      }
      close_fixture(&f);
    }
  }
  it("rolls back owner creation when SQLite rejects its commit") {
    fixture_t f; open_fixture(&f, graph_text, 1);
    const turbo_flow_plugin_transactional_resource_provider_v1_t *p = provider(&f); check_not_null(p);
    if (p) {
      turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_plugin_product_owner_v1_t owner = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
      sql(&f, "BEGIN");
      check_equal(scalar(&f, "SELECT owner_state FROM orders_inbox_meta_v2"), (int64_t)0);
      check_not_equal(p->materialize(p->ctx, f.flow, f.resolved, "intake.store", &owner, &e), SALTS_OK);
      check_null(owner.ctx);
      sql(&f, "ROLLBACK");
      check_equal(scalar(&f, "SELECT generation FROM orders_inbox_meta_v2"), (int64_t)0);
      int rc = create_generation(&f); check_equal(rc, SALTS_OK);
      if (rc == SALTS_OK) retire(&f);
    }
    close_fixture(&f);
  }
  it("propagates admission commit failure and retains no uncommitted record or memory copy") {
    fixture_t f; open_fixture(&f, graph_text, 1);
    int rc = create_generation(&f); check_equal(rc, SALTS_OK);
    if (rc == SALTS_OK) {
      turbo_flow_t *flow = turbo_flow_plugin_generation_flow(f.generation);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      sql(&f, "BEGIN");
      check_equal(scalar(&f, "SELECT records FROM orders_inbox_meta_v2"), (int64_t)0);
      check_not_equal(publish(flow, "one"), SALTS_OK);
      sql(&f, "ROLLBACK");
      check_equal(scalar(&f, "SELECT admitted FROM orders_inbox_meta_v2"), (int64_t)0);
      check_equal(scalar(&f, "SELECT COUNT(*) FROM orders_inbox_records_v2"), (int64_t)0);
      check_equal(atomic_load(&f.delivered), (size_t)0u);
      check_equal(publish(flow, "one"), SALTS_OK);
      retire(&f); check_equal(atomic_load(&f.delivered), (size_t)1u);
    }
    close_fixture(&f);
  }
  it("retains generation Graph and SQLite ownership when pre-retire claim fails") {
    fixture_t f; open_fixture(&f, graph_text, 1);
    int rc = create_generation(&f); check_equal(rc, SALTS_OK);
    if (rc == SALTS_OK) {
      turbo_flow_t *flow = turbo_flow_plugin_generation_flow(f.generation);
      check_equal(turbo_flow_start(flow), SALTS_OK); check_equal(publish(flow, "one"), SALTS_OK);
      sql(&f, "CREATE TRIGGER block_claim BEFORE UPDATE OF phase ON orders_inbox_records_v2 WHEN NEW.phase=1 BEGIN SELECT RAISE(ABORT,'claim blocked'); END");
      turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
      check_not_equal(turbo_flow_plugin_generation_destroy(f.generation, TEST_TIMEOUT_MS, &e), SALTS_OK);
      check(turbo_flow_plugin_generation_flow(f.generation) == flow);
      check_equal(turbo_flow_state(flow), TURBO_FLOW_STATE_STARTED);
      check_equal(turbo_flow_plugin_generation_owner_count(f.generation), (size_t)1u);
      check_equal(scalar(&f, "SELECT owner_state FROM orders_inbox_meta_v2"), (int64_t)1);
      check_equal(scalar(&f, "SELECT pending_records FROM orders_inbox_meta_v2"), (int64_t)1);
      check_equal(atomic_load(&f.delivered), (size_t)0u);
      sql(&f, "DROP TRIGGER block_claim");
      retire(&f);
      check_equal(atomic_load(&f.delivered), (size_t)1u);
      check_equal(atomic_load(&f.delivered_after_stop), (size_t)0u);
    }
    close_fixture(&f);
  }
  it("recovers pending and owner-lost work only through explicit takeover and retry") {
    fixture_t f;
    turbo_flow_turbodb_inbox_config_t seed_config;
    turbo_flow_inbox_t seed_inbox = TURBO_FLOW_INBOX_INIT;
    turbo_flow_durable_buffer_binding_config_t binding_config =
        TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
    turbo_flow_durable_buffer_binding_t *binding = NULL;
    turbo_flow_inbox_claim_t old_claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_failed_entry_t failed = TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
    orm_error_t oe;
    flow_test_operation_t op;
    char takeover_yaml[YAML_BYTES];
    uint64_t owner_lost_record_id;
    size_t count = 0u;
    int rc;

    open_fixture(&f, graph_text, 1);
    seed_config = turbo_flow_turbodb_inbox_config_default();
    seed_config.database = &f.database;
    seed_config.namespace_name = "orders";
    seed_config.max_records = 2u;
    seed_config.max_total_bytes = 67108864u;
    seed_config.max_record_bytes = 1048576u;
    seed_config.max_claims = 1u;
    seed_config.connection_count = 4u;
    orm_error_init(&oe);
    check_equal(turbo_flow_turbodb_inbox_create(&seed_config, &seed_inbox, &oe), SALTS_OK);

    binding_config.resource_name = "intake.store";
    binding_config.inbox = &seed_inbox;
    binding_config.identity_mode = TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED;
    binding_config.max_message_bytes = 1048576u;
    check_equal(turbo_flow_durable_buffer_bind(f.flow, &binding_config, &binding), SALTS_OK);
    check_equal(turbo_flow_compile(f.flow), SALTS_OK);
    check_equal(turbo_flow_start(f.flow), SALTS_OK);
    check_equal(publish(f.flow, "one"), SALTS_OK);
    check_equal(publish(f.flow, "two"), SALTS_OK);
    check_equal(atomic_load(&f.delivered), (size_t)0u);
    check_equal(scalar(&f, "SELECT pending_records FROM orders_inbox_meta_v2"), (int64_t)2);
    check_equal(turbo_flow_inbox_claim(&seed_inbox, &old_claim), SALTS_OK);
    owner_lost_record_id = old_claim.record_id;
    check_equal(scalar(&f, "SELECT pending_records FROM orders_inbox_meta_v2"), (int64_t)1);
    check_equal(scalar(&f, "SELECT in_flight_claims FROM orders_inbox_meta_v2"), (int64_t)1);

    check_equal(turbo_flow_stop(f.flow), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_unbind(binding), SALTS_OK);
    binding = NULL;
    turbo_flow_destroy(f.flow);
    f.flow = NULL;

    check_true((size_t)snprintf(takeover_yaml, sizeof(takeover_yaml), takeover_yaml_format, f.path) <
               sizeof(takeover_yaml));
    resolve(&f, takeover_yaml);
    f.flow = turbo_flow_create();
    check_not_null(f.flow);
    f.execution_flow = f.flow;
    op = flow_test_operation_init("test.output", output, &f);
    check_equal(flow_test_operation_register(f.flow, &op), SALTS_OK);
    check_equal(turbo_flow_parse_string(f.flow, graph_text, strlen(graph_text)), SALTS_OK);
    rc = create_generation(&f);
    check_equal(rc, SALTS_OK);
    if (rc == SALTS_OK) {
      turbo_flow_t *flow = turbo_flow_plugin_generation_flow(f.generation);
      check_equal(scalar(&f, "SELECT generation FROM orders_inbox_meta_v2"), (int64_t)2);
      check_equal(scalar(&f, "SELECT pending_records FROM orders_inbox_meta_v2"), (int64_t)1);
      check_equal(scalar(&f, "SELECT failed_records FROM orders_inbox_meta_v2"), (int64_t)1);
      check_equal(scalar(&f, "SELECT in_flight_claims FROM orders_inbox_meta_v2"), (int64_t)0);

      check_equal(turbo_flow_durable_buffer_scan_failed(
                      flow, "intake.store", 0u, &failed, 1u, &count),
                  SALTS_OK);
      check_equal(count, (size_t)1u);
      check_equal(failed.record_id, owner_lost_record_id);
      check_equal(failed.kind, TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN);

      check_equal(turbo_flow_inbox_complete(&seed_inbox, &old_claim), SALTS_ECANCELED);
      check_equal(old_claim.record_id, (uint64_t)0u);
      check_equal(turbo_flow_inbox_destroy(&seed_inbox), SALTS_OK);

      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(atomic_load(&f.delivered), (size_t)0u);
      for (size_t attempt = 0u; attempt < 8u && atomic_load(&f.delivered) < 1u; ++attempt)
        for (size_t i = 0u; i < 1000u && atomic_load(&f.delivered) < 1u; ++i)
        check_equal(turbo_flow_plugin_generation_poll(f.generation, 0u, &e), SALTS_OK);
      check_equal(atomic_load(&f.delivered), (size_t)1u);

      failed = (turbo_flow_inbox_failed_entry_t)TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
      count = 0u;
      check_equal(turbo_flow_durable_buffer_scan_failed(
                      flow, "intake.store", 0u, &failed, 1u, &count),
                  SALTS_OK);
      check_equal(count, (size_t)1u);
      check_equal(failed.record_id, owner_lost_record_id);
      check_equal(failed.kind, TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN);

      check_equal(turbo_flow_durable_buffer_retry_failed(
                      flow, "intake.store", owner_lost_record_id),
                  SALTS_OK);
      check_equal(atomic_load(&f.delivered), (size_t)1u);
      for (size_t attempt = 0u; attempt < 8u && atomic_load(&f.delivered) < 2u; ++attempt)
        check_equal(turbo_flow_plugin_generation_poll(f.generation, 0u, &e), SALTS_OK);
      check_equal(atomic_load(&f.delivered), (size_t)2u);
      check_equal(scalar(&f, "SELECT pending_records FROM orders_inbox_meta_v2"), (int64_t)0);
      check_equal(scalar(&f, "SELECT failed_records FROM orders_inbox_meta_v2"), (int64_t)0);
      check_equal(scalar(&f, "SELECT retried FROM orders_inbox_meta_v2"), (int64_t)1);
      retire(&f);
      check_equal(scalar(&f, "SELECT owner_state FROM orders_inbox_meta_v2"), (int64_t)0);
      check_equal(scalar(&f, "SELECT completed FROM orders_inbox_meta_v2"), (int64_t)2);
    } else {
      (void)turbo_flow_inbox_complete(&seed_inbox, &old_claim);
      (void)turbo_flow_inbox_destroy(&seed_inbox);
    }
    close_fixture(&f);
  }

  it("rejects runtime-only transport capabilities before any SQL admission") {
    fixture_t f; open_fixture(&f, graph_text, 1);
    int rc = create_generation(&f); check_equal(rc, SALTS_OK);
    if (rc == SALTS_OK) {
      turbo_flow_t *flow = turbo_flow_plugin_generation_flow(f.generation);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      turbo_flow_msg_t msg; message_init(&msg, "one"); msg.transport_context = &f;
      check_equal(turbo_flow_publish(flow, "input", &msg), SALTS_ENOTSUP);
      check(msg.transport_context == &f); check_equal(memcmp(msg.payload.data, "payload", 7u), 0);
      check_equal(scalar(&f, "SELECT admitted FROM orders_inbox_meta_v2"), (int64_t)0);
      msg.transport_context = NULL; turbo_flow_msg_cleanup(&msg); retire(&f);
    }
    close_fixture(&f);
  }
}
