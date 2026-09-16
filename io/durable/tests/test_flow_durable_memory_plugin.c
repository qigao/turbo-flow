#include "../../../tests/flow_operation_fixture.h"
#include "tinytest.h"
#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_plugin_generation.h"
#include <salts/clock.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { YAML_BYTES = 2048, TEST_TIMEOUT_MS = 1000, POLL_ATTEMPTS = 1000 };
static const char valid_yaml[] =
    "version: 1\nchannels:\n  intake.store:\n    kind: flow.durable.memory\n    config:\n"
    "      schema_version: 1\n      identity_mode: generated\n"
    "      max_message_bytes: 1048576\n      max_records: 2\n"
    "      max_total_bytes: 67108864\n      max_record_bytes: 1048576\n      max_claims: 1\n";
static const char graph_text[] =
    "source input\nbuffer intake resource intake.store\n"
    "stage output operation test.output\nstage main {\n input -> intake -> output\n}\n";
typedef struct fixture_s {
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
static int output(turbo_flow_msg_t *msg, void *ctx) {
  fixture_t *f = ctx;
  check_equal(msg->payload.len, (size_t)7u);
  check_equal(memcmp(msg->payload.data, "payload", 7u), 0);
  if (turbo_flow_state(f->execution_flow) != TURBO_FLOW_STATE_STARTED) ++f->delivered_after_stop;
  ++f->delivered;
  return SALTS_OK;
}
static void resolve(fixture_t *f, const char *yaml) {
  turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_resolved_config_destroy(f->resolved); f->resolved = NULL;
  check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &f->resolved, &e), SALTS_OK);
}
static void open_fixture(fixture_t *f, const char *graph) {
  turbo_flow_plugin_host_config_t hc = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t e = TURBO_FLOW_PLUGIN_ERROR_INIT;
  const char *path = getenv("TURBO_FLOW_DURABLE_MEMORY_PLUGIN_PATH");
  memset(f, 0, sizeof(*f));
  atomic_init(&f->delivered, 0u); atomic_init(&f->delivered_after_stop, 0u);
  check_not_null(path);
  check_equal(turbo_flow_plugin_host_create(&hc, &f->host, &e), SALTS_OK);
  check_equal(turbo_flow_plugin_host_load(f->host, path, &e), SALTS_OK);
  check_equal(turbo_flow_plugin_catalog_snapshot_create(f->host, &f->snapshot, &e), SALTS_OK);
  resolve(f, valid_yaml);
  f->flow = turbo_flow_create(); check_not_null(f->flow);
  f->execution_flow = f->flow;
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
}
static const turbo_flow_plugin_transactional_resource_provider_v1_t *provider(fixture_t *f) {
  turbo_flow_plugin_transactional_product_catalog_v1_t c = TURBO_FLOW_PLUGIN_TRANSACTIONAL_PRODUCT_CATALOG_V1_INIT;
  check_equal(turbo_flow_plugin_catalog_snapshot_transactional_product_catalog(f->snapshot, &c), SALTS_OK);
  check_equal(c.adapter_provider_count, (size_t)0u);
  for (size_t i=0u; i<c.resource_provider_count; ++i)
    if (!strcmp(c.resource_providers[i].kind, "flow.durable.memory")) return &c.resource_providers[i];
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
static int publish_message(turbo_flow_t *flow, const turbo_flow_msg_t *msg) {
  int rc = turbo_flow_publish(flow, "input", msg);
  const turbo_flow_error_t *error = turbo_flow_last_error(flow);
  info("publish status=%d state=%d payload=%zu owned=%zu descriptor=%d error=%d at %u:%u %s",
       rc, (int)turbo_flow_state(flow), msg->payload.len,
       msg->owned_payload ? tstr_len(msg->owned_payload) : 0u,
       turbo_flow_msg_content_descriptor(msg) != NULL,
       error ? error->code : 0, error ? error->line : 0u, error ? error->column : 0u,
       error ? error->message : "");
  return rc;
}
static int publish(turbo_flow_t *flow) {
  turbo_flow_msg_t msg;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_dup("payload"); msg.payload = tstr_to_v(msg.owned_payload);
  int rc = publish_message(flow, &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}
static void replace_field(const char *before, const char *after, char *out) {
  const char *at = strstr(valid_yaml, before);
  check_not_null(at);
  size_t prefix = (size_t)(at - valid_yaml);
  check(prefix + strlen(after) + strlen(at + strlen(before)) < YAML_BYTES);
  memcpy(out, valid_yaml, prefix);
  strcpy(out + prefix, after); strcat(out, at + strlen(before));
}
spec("configured bounded memory durable resource") {
  it("creates a PluginHost generation and retires accepted backlog before Graph stop") {
    fixture_t f; open_fixture(&f, graph_text);
    int rc = create_generation(&f);
    check_equal(rc, SALTS_OK);
    if (rc == SALTS_OK) {
      check_null(f.flow);
      check_equal(turbo_flow_plugin_generation_owner_count(f.generation), (size_t)1u);
      turbo_flow_t *flow = turbo_flow_plugin_generation_flow(f.generation);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(publish(flow), SALTS_OK); check_equal(publish(flow), SALTS_OK);
      check_equal(publish(flow), SALTS_ENOSPC);
      check_equal(atomic_load(&f.delivered), (size_t)0u);
      turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
      check_equal(turbo_flow_plugin_generation_destroy(f.generation, TEST_TIMEOUT_MS, &e), SALTS_OK);
      f.generation = NULL;
      check_equal(atomic_load(&f.delivered), (size_t)2u);
      check_equal(atomic_load(&f.delivered_after_stop), (size_t)0u);
    }
    close_fixture(&f);
  }
  it("maps stable_required identity and deduplicates an exact stable replay") {
    fixture_t f; open_fixture(&f, graph_text);
    char yaml[YAML_BYTES];
    replace_field("identity_mode: generated", "identity_mode: stable_required", yaml);
    resolve(&f, yaml);
    int rc = create_generation(&f); check_equal(rc, SALTS_OK);
    if (rc == SALTS_OK) {
      turbo_flow_t *flow = turbo_flow_plugin_generation_flow(f.generation);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(publish(flow), SALTS_EINVAL);
      turbo_flow_msg_t msg;
      turbo_flow_durable_identity_t identity = TURBO_FLOW_DURABLE_IDENTITY_INIT;
      turbo_flow_msg_init(&msg);
      msg.owned_payload = tstr_dup("payload"); msg.payload = tstr_to_v(msg.owned_payload);
      identity.source_id = vstr_from_buf("source", 6u);
      identity.admission_id = vstr_from_buf("record", 6u);
      check_equal(turbo_flow_msg_set_durable_identity(&msg, &identity), SALTS_OK);
      check_equal(publish_message(flow, &msg), SALTS_OK);
      check_equal(publish_message(flow, &msg), SALTS_OK);
      turbo_flow_msg_cleanup(&msg);
      turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
      for (size_t i=0u; i<POLL_ATTEMPTS && atomic_load(&f.delivered)==0u; ++i) {
        check_equal(turbo_flow_plugin_generation_poll(f.generation, 0u, &e), SALTS_OK);
        salts_sleep_ms(1u);
      }
      check_equal(atomic_load(&f.delivered), (size_t)1u);
      check_equal(turbo_flow_plugin_generation_destroy(f.generation, TEST_TIMEOUT_MS, &e), SALTS_OK);
      f.generation = NULL;
      check_equal(atomic_load(&f.delivered), (size_t)1u);
    }
    close_fixture(&f);
  }
  it("rolls back an earlier memory owner on later materialization or compile failure") {
    static const char *const graphs[] = {
      "source input\nbuffer intake resource intake.store\n"
      "stage output operation test.output resource bad.store\nstage main {\n input -> intake -> output\n}\n",
      "source input\nbuffer intake resource intake.store\n"
      "stage output operation test.missing\nstage main {\n input -> intake -> output\n}\n"
    };
    for (size_t i=0u; i<sizeof(graphs)/sizeof(graphs[0]); ++i) {
      fixture_t f; open_fixture(&f, graphs[i]);
      if (i == 0u) {
        char yaml[YAML_BYTES];
        (void)snprintf(yaml, sizeof(yaml), "%s  bad.store:\n%s", valid_yaml, strstr(valid_yaml, "    kind:"));
        resolve(&f, yaml);
      }
      check_not_equal(create_generation(&f), SALTS_OK);
      check_null(f.flow); check_null(f.generation); check_null(f.cleanup);
      close_fixture(&f);
    }
  }
  it("rejects exact schema type and finite bound violations without consuming the flow") {
    static const struct { const char *before; const char *after; } cases[] = {
      {"schema_version: 1", "schema_version: 2"},
      {"schema_version: 1", "schema_version: \"1\""},
      {"schema_version: 1", "unknown: 1"},
      {"schema_version: 1", "schema_version: 1\n      unknown: 1"},
      {"identity_mode: generated", "identity_mode: automatic"},
      {"identity_mode: generated", "identity_mode: false"},
      {"max_records: 2", "max_records: 0"},
      {"max_records: 2", "max_records: -1"},
      {"max_records: 2", "max_records: 1048577"},
      {"max_records: 2", "max_records: 1.5"},
      {"max_records: 2", "max_records: \"2\""},
      {"max_claims: 1", "max_claims: 0"},
      {"max_claims: 1", "max_claims: 3"},
      {"max_total_bytes: 67108864", "max_total_bytes: 0"},
      {"max_total_bytes: 67108864", "max_total_bytes: 1048575"},
      {"max_record_bytes: 1048576", "max_record_bytes: 0"},
      {"max_message_bytes: 1048576", "max_message_bytes: 0"}
    };
    fixture_t f; open_fixture(&f, graph_text);
    const turbo_flow_plugin_transactional_resource_provider_v1_t *p = provider(&f);
    check_not_null(p);
    if (p) for (size_t i=0u; i<sizeof(cases)/sizeof(cases[0]); ++i) {
      char yaml[YAML_BYTES]; turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
      replace_field(cases[i].before, cases[i].after, yaml); resolve(&f, yaml);
      check_not_equal(p->preflight(p->ctx, f.resolved, "intake.store", &e), SALTS_OK);
      turbo_flow_t *original = f.flow;
      check_not_equal(create_generation(&f), SALTS_OK);
      check(f.flow == original); check_null(f.generation); check_null(f.cleanup);
    }
    close_fixture(&f);
  }
  it("rejects a numeric value beyond uint64 in the canonical resolver") {
    char yaml[YAML_BYTES];
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
    replace_field("max_records: 2", "max_records: 18446744073709551616", yaml);
    check_not_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &e), SALTS_OK);
    check_null(resolved);
    turbo_flow_resolved_config_destroy(resolved);
  }
  it("requires exactly one buffer reference before allocating or binding") {
    static const char *const graphs[] = {
      "source input\nstage output operation test.output\nstage main {\n input -> output\n}\n",
      "source input\nstage output operation test.output resource intake.store\nstage main {\n input -> output\n}\n",
      "source input\nbuffer a resource intake.store\nbuffer b resource intake.store\nstage output operation test.output\nstage main {\n input -> a -> b -> output\n}\n"
    };
    for (size_t i=0u; i<sizeof(graphs)/sizeof(graphs[0]); ++i) {
      fixture_t f; open_fixture(&f, graphs[i]);
      const turbo_flow_plugin_transactional_resource_provider_v1_t *p = provider(&f);
      check_not_null(p);
      if (p) {
        turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
        turbo_flow_plugin_product_owner_v1_t owner = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
        check_equal(p->preflight(p->ctx, f.resolved, "intake.store", &e), SALTS_OK);
        check_equal(p->materialize(p->ctx, f.flow, f.resolved, "intake.store", &owner, &e), SALTS_EINVAL);
        check_null(owner.ctx);
      }
      close_fixture(&f);
    }
  }
  it("publishes a control thread external poll owner and verifies late lifecycle without new work") {
    fixture_t f; open_fixture(&f, graph_text);
    const turbo_flow_plugin_transactional_resource_provider_v1_t *p = provider(&f);
    check_not_null(p);
    if (p) {
      turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_plugin_product_owner_v1_t o = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
      check_equal(p->preflight(p->ctx, f.resolved, "intake.store", &e), SALTS_OK);
      turbo_flow_plugin_product_owner_v1_t invalid = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
      invalid.abi_minor++;
      check_equal(p->materialize(p->ctx, f.flow, f.resolved, "intake.store", &invalid, &e), SALTS_EINVAL);
      check_null(invalid.ctx);
      check_equal(p->materialize(p->ctx, f.flow, f.resolved, "intake.store", &o, &e), SALTS_OK);
      turbo_flow_plugin_product_owner_v1_t duplicate = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
      check_equal(p->materialize(p->ctx, f.flow, f.resolved, "intake.store", &duplicate, &e), SALTS_EALREADY);
      check_null(duplicate.ctx);
      check_equal(o.flags, (turbo_flow_plugin_product_owner_flags_t)(TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD | TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL));
      check_equal(turbo_flow_compile(f.flow), SALTS_OK); check_equal(turbo_flow_start(f.flow), SALTS_OK);
      check_equal(publish(f.flow), SALTS_OK);
      check_equal(o.quiesce(o.ctx, 0u), SALTS_EBUSY);
      check_equal(turbo_flow_stop(f.flow), SALTS_OK);
      check_equal(o.drain(o.ctx, 0u), SALTS_EBUSY);
      check_equal(atomic_load(&f.delivered), (size_t)0u);
      check_equal(turbo_flow_start(f.flow), SALTS_OK);
      int idle = SALTS_EBUSY;
      for (size_t i=0u; i<POLL_ATTEMPTS && idle == SALTS_EBUSY; ++i) {
        check_equal(o.poll(o.ctx, 0u), SALTS_OK);
        idle = o.quiesce(o.ctx, 0u); salts_sleep_ms(1u);
      }
      check_equal(idle, SALTS_OK);
      check_equal(atomic_load(&f.delivered), (size_t)1u);
      check_equal(turbo_flow_stop(f.flow), SALTS_OK);
      check_equal(o.poll(o.ctx, 0u), SALTS_ESHUTDOWN);
      check_equal(o.quiesce(o.ctx, 0u), SALTS_OK);
      check_equal(o.quiesce(o.ctx, 0u), SALTS_OK);
      check_equal(o.drain(o.ctx, 0u), SALTS_OK);
      check_equal(o.shutdown(o.ctx), SALTS_OK); check_equal(o.shutdown(o.ctx), SALTS_OK);
      o.destroy(o.ctx);
    }
    close_fixture(&f);
  }
}
