#include "../../../tests/flow_operation_fixture.h"
#include "durable_provider_conformance.h"
#include "tinytest.h"
#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_durable_memory_resource.h"
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resource.h"

#include <salts/clock.h>
#include <salts/plugin.h>

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  TEST_TIMEOUT_MS = 1000,
  POLL_ATTEMPTS = 1000,
  GRAPH_BYTES = 4096
};

static const char resolved_yaml[] =
    "version: 1\n"
    "adapters: {}\n";

static const char graph_generated[] =
    "source input\n"
    "buffer intake provider flow.durable.memory {\n"
    "  resource intake_store\n"
    "  schema_version 2\n"
    "  max_message_bytes 1048576\n"
    "  max_records 2\n"
    "  max_total_bytes 67108864\n"
    "  max_record_bytes 1048576\n"
    "  max_claims 1\n"
    "  identity_mode 0\n"
    "}\n"
    "stage output operation test.output\n"
    "stage main {\n"
    "  input -> intake -> output\n"
    "}\n";

typedef struct resolver_state_s {
  salts_plugin_registry *registry;
  salts_plugin_ref plugin;
  unsigned provider_calls;
  unsigned resource_calls;
} resolver_state_t;

typedef struct fixture_s {
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  turbo_flow_resolved_config_t *resolved;
  turbo_flow_t *flow;
  turbo_flow_t *execution_flow;
  turbo_flow_plugin_generation_t *generation;
  turbo_flow_plugin_generation_t *cleanup;

  salts_plugin_registry registry;
  salts_plugin_ref plugin_ref;
  int registry_initialized;
  int plugin_loaded;
  int plugin_started;

  resolver_state_t resolver;
  turbo_flow_provider_resolver_v1_t provider_resolver;
  turbo_flow_resource_resolver_v1_t resource_resolver;

  atomic_size_t delivered;
  atomic_size_t delivered_after_stop;
} fixture_t;

static int output(turbo_flow_msg_t *msg, void *ctx) {
  fixture_t *f = (fixture_t *)ctx;
  check_equal(msg->payload.len, (size_t)7u);
  check_equal(memcmp(msg->payload.data, "payload", 7u), 0);
  if (turbo_flow_state(f->execution_flow) != TURBO_FLOW_STATE_STARTED)
    ++f->delivered_after_stop;
  ++f->delivered;
  return SALTS_OK;
}

static int resolve_provider(
    void *ctx, const char *provider_identity,
    turbo_flow_provider_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  resolver_state_t *state = (resolver_state_t *)ctx;
  if (!state || !provider_identity || !out ||
      out->size != sizeof(*out))
    return SALTS_EINVAL;
  ++state->provider_calls;
  if (strcmp(provider_identity, "flow.durable.memory") != 0) {
    if (error && error->size == sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_ENOENT;
    }
    return SALTS_ENOENT;
  }
  out->module_identity = "turbo-flow.durable.memory";
  out->registry = state->registry;
  out->plugin = state->plugin;
  return SALTS_OK;
}

static int resolve_resource(
    void *ctx, const char *resource_name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_resource_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  resolver_state_t *state = (resolver_state_t *)ctx;
  if (!state || !resource_name || !requirement || !out ||
      out->size != sizeof(*out))
    return SALTS_EINVAL;
  ++state->resource_calls;
  if (!requirement->contract_id ||
      strcmp(
          requirement->contract_id,
          TURBO_FLOW_DURABLE_MEMORY_RESOURCE_CONTRACT_ID) != 0 ||
      requirement->contract_version !=
          TURBO_FLOW_DURABLE_MEMORY_RESOURCE_CONTRACT_VERSION ||
      (requirement->required_capabilities &
       TURBO_FLOW_DURABLE_MEMORY_RESOURCE_LIMITS) == 0u ||
      !cmeta_interface_desc_equal(
          requirement->expected_interface,
          turbo_flow_durable_memory_resource_interface())) {
    if (error && error->size == sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_EPROTO;
    }
    return SALTS_EPROTO;
  }
  out->identity = "deployment.memory";
  out->registry = state->registry;
  out->plugin = state->plugin;
  out->export_id = "flow.durable.memory.resource";
  return SALTS_OK;
}

static void stop_unload(fixture_t *f) {
  bool quiescent = false;
  if (!f || !f->registry_initialized) return;
  if (f->plugin_started) {
    check_equal(
        salts_plugin_registry_request_stop(
            &f->registry, f->plugin_ref),
        SALTS_PLUGIN_OK);
    f->plugin_started = 0;
  }
  if (f->plugin_loaded) {
    check_equal(
        salts_plugin_registry_poll_quiescent(
            &f->registry, f->plugin_ref, &quiescent),
        SALTS_PLUGIN_OK);
    check_true(quiescent);
    check_equal(
        salts_plugin_registry_unload(
            &f->registry, f->plugin_ref),
        SALTS_PLUGIN_OK);
    f->plugin_loaded = 0;
  }
  check_equal(
      salts_plugin_registry_destroy(&f->registry),
      SALTS_PLUGIN_OK);
  f->registry_initialized = 0;
}

static int open_fixture(
    fixture_t *f, const char *graph,
    turbo_flow_config_error_t *config_error) {
  turbo_flow_plugin_host_config_t host_config =
      TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t plugin_error =
      TURBO_FLOW_PLUGIN_ERROR_INIT;
  salts_plugin_registry_config registry_config = {2u};
  flow_test_operation_t operation;
  const char *path =
      getenv("TURBO_FLOW_DURABLE_MEMORY_PLUGIN_PATH");
  int rc;

  memset(f, 0, sizeof(*f));
  atomic_init(&f->delivered, 0u);
  atomic_init(&f->delivered_after_stop, 0u);
  if (!path || !path[0]) return SALTS_ENOENT;

  rc = turbo_flow_plugin_host_create(
      &host_config, &f->host, &plugin_error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_catalog_snapshot_create(
      f->host, &f->snapshot, &plugin_error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_config_resolve_yaml(
      resolved_yaml, sizeof(resolved_yaml) - 1u,
      &f->resolved, config_error);
  if (rc != SALTS_OK) return rc;

  f->flow = turbo_flow_create();
  if (!f->flow) return SALTS_ENOMEM;
  f->execution_flow = f->flow;
  operation = flow_test_operation_init("test.output", output, f);
  rc = flow_test_operation_register(f->flow, &operation);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_parse_string(f->flow, graph, strlen(graph));
  if (rc != SALTS_OK) return rc;

  if (salts_plugin_registry_init(
          &f->registry, &registry_config) != SALTS_PLUGIN_OK)
    return SALTS_EIO;
  f->registry_initialized = 1;
  if (salts_plugin_registry_load(
          &f->registry, path, &f->plugin_ref) != SALTS_PLUGIN_OK)
    return SALTS_EIO;
  f->plugin_loaded = 1;
  if (salts_plugin_registry_start(
          &f->registry, f->plugin_ref) != SALTS_PLUGIN_OK)
    return SALTS_EIO;
  f->plugin_started = 1;

  f->resolver.registry = &f->registry;
  f->resolver.plugin = f->plugin_ref;
  f->provider_resolver =
      (turbo_flow_provider_resolver_v1_t)
          TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
  f->provider_resolver.ctx = &f->resolver;
  f->provider_resolver.resolve = resolve_provider;
  f->resource_resolver =
      (turbo_flow_resource_resolver_v1_t)
          TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
  f->resource_resolver.ctx = &f->resolver;
  f->resource_resolver.resolve = resolve_resource;
  return SALTS_OK;
}

static void close_fixture(fixture_t *f) {
  turbo_flow_config_error_t config_error =
      TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_error_t plugin_error =
      TURBO_FLOW_PLUGIN_ERROR_INIT;
  if (!f) return;

  if (f->generation) {
    check_equal(
        turbo_flow_plugin_generation_destroy(
            f->generation, TEST_TIMEOUT_MS, &config_error),
        SALTS_OK);
    f->generation = NULL;
  }
  if (f->cleanup) {
    check_equal(
        turbo_flow_plugin_generation_destroy(
            f->cleanup, TEST_TIMEOUT_MS, &config_error),
        SALTS_OK);
    f->cleanup = NULL;
  }
  if (f->flow) {
    turbo_flow_destroy(f->flow);
    f->flow = NULL;
  }
  if (f->resolved) {
    turbo_flow_resolved_config_destroy(f->resolved);
    f->resolved = NULL;
  }
  if (f->snapshot) {
    turbo_flow_plugin_catalog_snapshot_destroy(f->snapshot);
    f->snapshot = NULL;
  }
  if (f->host) {
    check_equal(
        turbo_flow_plugin_host_destroy(
            f->host, TEST_TIMEOUT_MS, &plugin_error),
        SALTS_OK);
    f->host = NULL;
  }
  stop_unload(f);
}

static int create_generation(fixture_t *f) {
  turbo_flow_config_error_t error =
      TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_generation_config_t config =
      TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  int rc;

  config.provider_resolver = &f->provider_resolver;
  config.resource_resolver = &f->resource_resolver;
  rc = turbo_flow_plugin_generation_create(
      f->snapshot, f->resolved, &f->flow, &config,
      NULL, &f->generation, &f->cleanup, &error);
  info(
      "generation status=%d path=%s reason=%s",
      rc, error.path, error.message);
  return rc;
}

static int publish_message(
    turbo_flow_t *flow, const turbo_flow_msg_t *msg) {
  int rc = turbo_flow_publish(flow, "input", msg);
  const turbo_flow_error_t *error = turbo_flow_last_error(flow);
  info(
      "publish status=%d state=%d payload=%zu error=%d %s",
      rc, (int)turbo_flow_state(flow), msg->payload.len,
      error ? error->code : 0,
      error ? error->message : "");
  return rc;
}

static int publish(turbo_flow_t *flow) {
  turbo_flow_msg_t msg;
  int rc;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_dup("payload");
  msg.payload = tstr_to_v(msg.owned_payload);
  rc = publish_message(flow, &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static int conformance_publish_stable(
    void *ctx, const char *admission_id) {
  fixture_t *f = (fixture_t *)ctx;
  turbo_flow_msg_t msg;
  turbo_flow_durable_identity_t identity =
      TURBO_FLOW_DURABLE_IDENTITY_INIT;
  int rc;

  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_dup("payload");
  msg.payload = tstr_to_v(msg.owned_payload);
  identity.source_id = vstr_from_buf("source", 6u);
  identity.admission_id =
      vstr_from_buf(admission_id, strlen(admission_id));
  check_equal(
      turbo_flow_msg_set_durable_identity(&msg, &identity),
      SALTS_OK);
  rc = publish_message(
      turbo_flow_plugin_generation_flow(f->generation), &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static int conformance_progress(void *ctx) {
  fixture_t *f = (fixture_t *)ctx;
  turbo_flow_config_error_t error =
      TURBO_FLOW_CONFIG_ERROR_INIT;
  return turbo_flow_plugin_generation_poll(
      f->generation, 0u, &error);
}

static size_t conformance_delivered(void *ctx) {
  fixture_t *f = (fixture_t *)ctx;
  return atomic_load(&f->delivered);
}

static void graph_replace(
    const char *before, const char *after, char *out) {
  const char *at = strstr(graph_generated, before);
  size_t prefix;
  check_not_null(at);
  if (!at) {
    out[0] = '\0';
    return;
  }
  prefix = (size_t)(at - graph_generated);
  check(
      prefix + strlen(after) +
          strlen(at + strlen(before)) + 1u <
      GRAPH_BYTES);
  memcpy(out, graph_generated, prefix);
  strcpy(out + prefix, after);
  strcat(out, at + strlen(before));
}

spec("canonical bounded memory durable provider") {
  it("matches the shared provider-neutral durable conformance contract") {
    fixture_t f;
    turbo_flow_config_error_t error =
        TURBO_FLOW_CONFIG_ERROR_INIT;
    char graph[GRAPH_BYTES];
    graph_replace("identity_mode 0", "identity_mode 1", graph);

    check_equal(open_fixture(&f, graph, &error), SALTS_OK);
    check_equal(create_generation(&f), SALTS_OK);
    if (f.generation) {
      turbo_flow_t *flow =
          turbo_flow_plugin_generation_flow(f.generation);
      turbo_flow_durable_provider_conformance_v1_t contract = {
          &f, flow, conformance_publish_stable,
          conformance_progress, conformance_delivered};
      check_equal(turbo_flow_start(flow), SALTS_OK);
      turbo_flow_durable_provider_conformance_capacity_and_replay(
          &contract);
    }
    close_fixture(&f);
  }

  it("retires accepted generated-identity backlog before Graph stop") {
    fixture_t f;
    turbo_flow_config_error_t error =
        TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(
        open_fixture(&f, graph_generated, &error), SALTS_OK);
    check_equal(create_generation(&f), SALTS_OK);
    if (f.generation) {
      turbo_flow_t *flow =
          turbo_flow_plugin_generation_flow(f.generation);
      check_null(f.flow);
      check_equal(
          turbo_flow_plugin_generation_owner_count(f.generation),
          (size_t)1u);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(publish(flow), SALTS_OK);
      check_equal(publish(flow), SALTS_OK);
      check_equal(publish(flow), SALTS_ENOSPC);
      check_equal(atomic_load(&f.delivered), (size_t)0u);
      check_equal(
          turbo_flow_plugin_generation_destroy(
              f.generation, TEST_TIMEOUT_MS, &error),
          SALTS_OK);
      f.generation = NULL;
      check_equal(atomic_load(&f.delivered), (size_t)2u);
      check_equal(
          atomic_load(&f.delivered_after_stop), (size_t)0u);
    }
    close_fixture(&f);
  }

  it("maps stable-required identity and deduplicates exact replay") {
    fixture_t f;
    turbo_flow_config_error_t error =
        TURBO_FLOW_CONFIG_ERROR_INIT;
    char graph[GRAPH_BYTES];
    graph_replace("identity_mode 0", "identity_mode 1", graph);

    check_equal(open_fixture(&f, graph, &error), SALTS_OK);
    check_equal(create_generation(&f), SALTS_OK);
    if (f.generation) {
      turbo_flow_t *flow =
          turbo_flow_plugin_generation_flow(f.generation);
      turbo_flow_msg_t msg;
      turbo_flow_durable_identity_t identity =
          TURBO_FLOW_DURABLE_IDENTITY_INIT;

      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(publish(flow), SALTS_EINVAL);

      turbo_flow_msg_init(&msg);
      msg.owned_payload = tstr_dup("payload");
      msg.payload = tstr_to_v(msg.owned_payload);
      identity.source_id = vstr_from_buf("source", 6u);
      identity.admission_id = vstr_from_buf("record", 6u);
      check_equal(
          turbo_flow_msg_set_durable_identity(&msg, &identity),
          SALTS_OK);
      check_equal(publish_message(flow, &msg), SALTS_OK);
      check_equal(publish_message(flow, &msg), SALTS_OK);
      turbo_flow_msg_cleanup(&msg);

      for (size_t i = 0u;
           i < POLL_ATTEMPTS &&
           atomic_load(&f.delivered) == 0u;
           ++i) {
        check_equal(
            turbo_flow_plugin_generation_poll(
                f.generation, 0u, &error),
            SALTS_OK);
        salts_sleep_ms(1u);
      }
      check_equal(atomic_load(&f.delivered), (size_t)1u);
    }
    close_fixture(&f);
  }

  it("releases terminal history only through explicit forget") {
    fixture_t f;
    turbo_flow_config_error_t error =
        TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_inbox_history_entry_t history =
        TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
    char graph[GRAPH_BYTES];
    size_t count = 0u;

    graph_replace("max_records 2", "max_records 1", graph);
    check_equal(open_fixture(&f, graph, &error), SALTS_OK);
    check_equal(create_generation(&f), SALTS_OK);
    if (f.generation) {
      turbo_flow_t *flow =
          turbo_flow_plugin_generation_flow(f.generation);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(publish(flow), SALTS_OK);
      for (size_t i = 0u; i < POLL_ATTEMPTS && count == 0u; ++i) {
        check_equal(
            turbo_flow_plugin_generation_poll(
                f.generation, 0u, &error),
            SALTS_OK);
        history = (turbo_flow_inbox_history_entry_t)
            TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
        check_equal(
            turbo_flow_durable_buffer_scan_history(
                flow, "intake_store", 0u,
                &history, 1u, &count),
            SALTS_OK);
        if (count == 0u) salts_sleep_ms(1u);
      }
      check_equal(atomic_load(&f.delivered), (size_t)1u);
      check_equal(publish(flow), SALTS_ENOSPC);
      check_equal(count, (size_t)1u);
      check_equal(
          history.kind, TURBO_FLOW_INBOX_TERMINAL_COMPLETED);
      check_equal(
          turbo_flow_durable_buffer_forget(
              flow, "intake_store", history.record_id),
          SALTS_OK);
      check_equal(publish(flow), SALTS_OK);
    }
    close_fixture(&f);
  }

  it("releases prepared memory leases when operation preflight fails") {
    static const char graph[] =
        "source input\n"
        "buffer intake provider flow.durable.memory {\n"
        "  resource intake_store\n"
        "  schema_version 2\n"
        "  max_message_bytes 1048576\n"
        "  max_records 2\n"
        "  max_total_bytes 67108864\n"
        "  max_record_bytes 1048576\n"
        "  max_claims 1\n"
        "  identity_mode 0\n"
        "}\n"
        "stage output operation test.missing\n"
        "stage main {\n"
        "  input -> intake -> output\n"
        "}\n";
    fixture_t f;
    turbo_flow_config_error_t error =
        TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(open_fixture(&f, graph, &error), SALTS_OK);
    {
      turbo_flow_t *original = f.flow;
      check_not_equal(create_generation(&f), SALTS_OK);
      check_true(f.flow == original);
      check_null(f.generation);
      check_null(f.cleanup);
    }
    close_fixture(&f);
  }

  it("rejects typed policy violations before consuming the Graph") {
    static const struct {
      const char *before;
      const char *after;
    } cases[] = {
        {"schema_version 2", "schema_version 1"},
        {"max_records 2", "max_records 0"},
        {"max_claims 1", "max_claims 3"},
        {"max_total_bytes 67108864", "max_total_bytes 1048575"},
        {"max_record_bytes 1048576", "max_record_bytes 67108865"},
        {"identity_mode 0", "identity_mode 2"},
        {"schema_version 2", "schema_version 2\n  unknown 1"}};

    for (size_t i = 0u; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      fixture_t f;
      turbo_flow_config_error_t error =
          TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_t *original;
      char graph[GRAPH_BYTES];

      graph_replace(cases[i].before, cases[i].after, graph);
      check_equal(open_fixture(&f, graph, &error), SALTS_OK);
      original = f.flow;
      check_not_equal(create_generation(&f), SALTS_OK);
      check_true(f.flow == original);
      check_null(f.generation);
      check_null(f.cleanup);
      close_fixture(&f);
    }
  }

  it("rejects a resource reference shared by multiple buffers") {
    static const char graph[] =
        "source input\n"
        "buffer first provider flow.durable.memory {\n"
        "  resource intake_store\n"
        "  schema_version 2\n"
        "  max_message_bytes 128\n"
        "  max_records 2\n"
        "  max_total_bytes 256\n"
        "  max_record_bytes 128\n"
        "  max_claims 1\n"
        "  identity_mode 0\n"
        "}\n"
        "buffer second provider flow.durable.memory {\n"
        "  resource intake_store\n"
        "  schema_version 2\n"
        "  max_message_bytes 128\n"
        "  max_records 2\n"
        "  max_total_bytes 256\n"
        "  max_record_bytes 128\n"
        "  max_claims 1\n"
        "  identity_mode 0\n"
        "}\n"
        "stage output operation test.output\n"
        "stage main {\n"
        "  input -> first -> second -> output\n"
        "}\n";
    fixture_t f;
    turbo_flow_config_error_t error =
        TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(open_fixture(&f, graph, &error), SALTS_OK);
    check_not_equal(create_generation(&f), SALTS_OK);
    close_fixture(&f);
  }
}
