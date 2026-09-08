#include "tinytest.h"
#include "turbo_flow_plugin_generation.h"

#include <string.h>

#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_CAPACITY
  #error FLOW_PLUGIN_GENERATION_FIXTURE_CAPACITY is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_PREFLIGHT_FAIL
  #error FLOW_PLUGIN_GENERATION_FIXTURE_PREFLIGHT_FAIL is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_SUCCESS
  #error FLOW_PLUGIN_GENERATION_FIXTURE_SUCCESS is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_MATERIALIZE_FIRST_FAIL
  #error FLOW_PLUGIN_GENERATION_FIXTURE_MATERIALIZE_FIRST_FAIL is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_MATERIALIZE_SECOND_FAIL
  #error FLOW_PLUGIN_GENERATION_FIXTURE_MATERIALIZE_SECOND_FAIL is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_COMPILE_FAIL
  #error FLOW_PLUGIN_GENERATION_FIXTURE_COMPILE_FAIL is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_LIFECYCLE
  #error FLOW_PLUGIN_GENERATION_FIXTURE_LIFECYCLE is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_QUIESCE_ONCE
  #error FLOW_PLUGIN_GENERATION_FIXTURE_QUIESCE_ONCE is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_DRAIN_ONCE
  #error FLOW_PLUGIN_GENERATION_FIXTURE_DRAIN_ONCE is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_SHUTDOWN_ONCE
  #error FLOW_PLUGIN_GENERATION_FIXTURE_SHUTDOWN_ONCE is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_QUIESCE_TIMEOUT
  #error FLOW_PLUGIN_GENERATION_FIXTURE_QUIESCE_TIMEOUT is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_DRAIN_TIMEOUT
  #error FLOW_PLUGIN_GENERATION_FIXTURE_DRAIN_TIMEOUT is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_RESOURCE_ORDER
  #error FLOW_PLUGIN_GENERATION_FIXTURE_RESOURCE_ORDER is required
#endif

static const char flow_plugin_generation_yaml[] = "version: 1\n"
                                                  "adapters:\n"
                                                  "  input.adapter:\n"
                                                  "    kind: fixture.transactional.adapter\n"
                                                  "    config: {}\n"
                                                  "  output.adapter:\n"
                                                  "    kind: fixture.transactional.adapter\n"
                                                  "    config: {}\n";

static const char flow_plugin_generation_graph[] = "source input adapter input.adapter\n"
                                                   "stage output adapter output.adapter\n"
                                                   "stage main {\n"
                                                   "  input -> output\n"
                                                   "}\n";

static const char flow_plugin_generation_resource_yaml[] =
    "version: 1\n"
    "channels:\n"
    "  routing:\n"
    "    kind: fixture.transactional.resource\n"
    "    config: {}\n"
    "adapters:\n"
    "  input.adapter:\n"
    "    kind: fixture.transactional.adapter\n"
    "    config: {}\n";

static const char flow_plugin_generation_resource_graph[] =
    "source input adapter input.adapter\n"
    "stage decide operation rules.apply resource routing\n"
    "stage main {\n"
    "  input -> decide\n"
    "}\n";

typedef struct flow_plugin_generation_test_context_s {
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  turbo_flow_resolved_config_t *resolved;
  turbo_flow_t *flow;
} flow_plugin_generation_test_context_t;

static int flow_plugin_generation_test_open(flow_plugin_generation_test_context_t *context,
                                            const char *plugin_path,
                                            turbo_flow_plugin_error_t *plugin_error,
                                            turbo_flow_config_error_t *config_error) {
  turbo_flow_plugin_host_config_t host_config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  int rc;
  if (!context || !plugin_path || !plugin_error || !config_error) return SALTS_EINVAL;
  memset(context, 0, sizeof(*context));
  host_config.module_capacity = 1u;
  host_config.adapter_provider_capacity = 0u;
  host_config.resource_provider_capacity = 0u;
  host_config.protocol_provider_capacity = 0u;
  host_config.business_provider_capacity = 0u;
  host_config.transactional_adapter_provider_capacity = 1u;
  host_config.transactional_resource_provider_capacity = 1u;
  rc = turbo_flow_plugin_host_create(&host_config, &context->host, plugin_error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_host_load(context->host, plugin_path, plugin_error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_catalog_snapshot_create(context->host, &context->snapshot, plugin_error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_config_resolve_yaml(flow_plugin_generation_yaml,
                                      sizeof(flow_plugin_generation_yaml) - 1u, &context->resolved,
                                      config_error);
  if (rc != SALTS_OK) return rc;
  context->flow = turbo_flow_create();
  if (!context->flow) return SALTS_ENOMEM;
  return turbo_flow_parse_string(context->flow, flow_plugin_generation_graph,
                                 sizeof(flow_plugin_generation_graph) - 1u);
}

static int flow_plugin_generation_test_close(flow_plugin_generation_test_context_t *context,
                                             turbo_flow_plugin_error_t *error) {
  int rc = SALTS_OK;
  if (!context) return SALTS_EINVAL;
  turbo_flow_destroy(context->flow);
  context->flow = NULL;
  turbo_flow_resolved_config_destroy(context->resolved);
  context->resolved = NULL;
  turbo_flow_plugin_catalog_snapshot_destroy(context->snapshot);
  context->snapshot = NULL;
  if (context->host) rc = turbo_flow_plugin_host_destroy(context->host, 1000u, error);
  context->host = NULL;
  return rc;
}

static int
flow_plugin_generation_test_replace_documents(flow_plugin_generation_test_context_t *context,
                                              const char *yaml, size_t yaml_size, const char *graph,
                                              size_t graph_size, turbo_flow_config_error_t *error) {
  int rc;
  if (!context || !yaml || !graph || !error) return SALTS_EINVAL;
  turbo_flow_destroy(context->flow);
  context->flow = NULL;
  turbo_flow_resolved_config_destroy(context->resolved);
  context->resolved = NULL;
  rc = turbo_flow_config_resolve_yaml(yaml, yaml_size, &context->resolved, error);
  if (rc != SALTS_OK) return rc;
  context->flow = turbo_flow_create();
  if (!context->flow) return SALTS_ENOMEM;
  return turbo_flow_parse_string(context->flow, graph, graph_size);
}

spec("transactional plugin Graph generation") {
  it("rejects owner capacity before preflight or materialization and preserves the Graph") {
    for (size_t capacity = 0u; capacity <= 1u; ++capacity) {
      flow_plugin_generation_test_context_t context;
      turbo_flow_plugin_generation_config_t generation_config =
          TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
      turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_plugin_generation_t *generation = NULL;
      generation_config.owner_capacity = capacity;

      check_equal(flow_plugin_generation_test_open(&context,
                                                   FLOW_PLUGIN_GENERATION_FIXTURE_CAPACITY,
                                                   &plugin_error, &config_error),
                  SALTS_OK);
      check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                      &context.flow, &generation_config,
                                                      &generation, &config_error),
                  SALTS_ENOSPC);
      check_null(generation);
      check_not_null(context.flow);
      check_equal(turbo_flow_state(context.flow), TURBO_FLOW_STATE_PARSED);
      check_equal(turbo_flow_adapter_count(context.flow), 0u);
      check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
    }
  }

  it("runs all preflights before consuming the Graph or invoking materialization") {
    flow_plugin_generation_test_context_t context;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    generation_config.owner_capacity = 2u;

    check_equal(flow_plugin_generation_test_open(&context,
                                                 FLOW_PLUGIN_GENERATION_FIXTURE_PREFLIGHT_FAIL,
                                                 &plugin_error, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                    &context.flow, &generation_config, &generation,
                                                    &config_error),
                SALTS_EIO);
    check_equal(config_error.path, "$.fixture.preflight");
    check_null(generation);
    check_not_null(context.flow);
    check_equal(turbo_flow_state(context.flow), TURBO_FLOW_STATE_PARSED);
    check_equal(turbo_flow_adapter_count(context.flow), 0u);
    check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
  }

  it("moves, materializes, and compiles one bounded Graph generation") {
    flow_plugin_generation_test_context_t context;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_t *compiled_flow;
    generation_config.owner_capacity = 2u;

    check_equal(flow_plugin_generation_test_open(&context, FLOW_PLUGIN_GENERATION_FIXTURE_SUCCESS,
                                                 &plugin_error, &config_error),
                SALTS_OK);
    {
      const int rc =
          turbo_flow_plugin_generation_create(context.snapshot, context.resolved, &context.flow,
                                              &generation_config, &generation, &config_error);
      info("generation status=%d path=%s message=%s", rc, config_error.path, config_error.message);
      check_equal(rc, SALTS_OK);
    }
    check_null(context.flow);
    check_not_null(generation);
    check_equal(turbo_flow_plugin_generation_state(generation),
                TURBO_FLOW_PLUGIN_GENERATION_COMPILED);
    check_equal(turbo_flow_plugin_generation_owner_count(generation), 2u);
    compiled_flow = turbo_flow_plugin_generation_flow(generation);
    check_not_null(compiled_flow);
    check_equal(turbo_flow_state(compiled_flow), TURBO_FLOW_STATE_COMPILED);
    check_equal(turbo_flow_adapter_count(compiled_flow), 2u);
    check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &config_error), SALTS_OK);
    check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
  }

  it("rolls back transferred owners and preserves a materialize error") {
    const char *fixtures[] = {FLOW_PLUGIN_GENERATION_FIXTURE_MATERIALIZE_FIRST_FAIL,
                              FLOW_PLUGIN_GENERATION_FIXTURE_MATERIALIZE_SECOND_FAIL};
    for (size_t i = 0u; i < 2u; ++i) {
      flow_plugin_generation_test_context_t context;
      turbo_flow_plugin_generation_config_t generation_config =
          TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
      turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_plugin_generation_t *generation = NULL;
      generation_config.owner_capacity = 2u;

      check_equal(
          flow_plugin_generation_test_open(&context, fixtures[i], &plugin_error, &config_error),
          SALTS_OK);
      check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                      &context.flow, &generation_config,
                                                      &generation, &config_error),
                  SALTS_EIO);
      check_equal(config_error.path, "$.fixture.materialize");
      check_equal(config_error.message, "fixture materialize failure");
      check_null(generation);
      check_null(context.flow);
      check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
    }
  }

  it("rolls back every transferred owner when Graph compilation fails") {
    flow_plugin_generation_test_context_t context;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    generation_config.owner_capacity = 2u;

    check_equal(flow_plugin_generation_test_open(&context,
                                                 FLOW_PLUGIN_GENERATION_FIXTURE_COMPILE_FAIL,
                                                 &plugin_error, &config_error),
                SALTS_OK);
    {
      const int rc =
          turbo_flow_plugin_generation_create(context.snapshot, context.resolved, &context.flow,
                                              &generation_config, &generation, &config_error);
      info("compile rollback generation status=%d path=%s message=%s", rc, config_error.path,
           config_error.message);
      check_equal(rc, SALTS_EINVAL);
    }
    check_equal(config_error.path, "$.graph");
    check_null(generation);
    check_null(context.flow);
    check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
  }

  it("preflights and materializes resources before adapters regardless of stage order") {
    flow_plugin_generation_test_context_t context;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    generation_config.owner_capacity = 2u;

    check_equal(flow_plugin_generation_test_open(&context,
                                                 FLOW_PLUGIN_GENERATION_FIXTURE_RESOURCE_ORDER,
                                                 &plugin_error, &config_error),
                SALTS_OK);
    check_equal(flow_plugin_generation_test_replace_documents(
                    &context, flow_plugin_generation_resource_yaml,
                    sizeof(flow_plugin_generation_resource_yaml) - 1u,
                    flow_plugin_generation_resource_graph,
                    sizeof(flow_plugin_generation_resource_graph) - 1u, &config_error),
                SALTS_OK);
    {
      turbo_flow_resolved_channel_view_t channel = TURBO_FLOW_RESOLVED_CHANNEL_VIEW_INIT;
      check_equal(turbo_flow_resolved_config_channel(context.resolved, "routing", &channel),
                  SALTS_OK);
      check_equal(channel.kind, "fixture.transactional.resource");
      check_equal(turbo_flow_stage_count(context.flow), 2u);
      check_null(turbo_flow_stage_at(context.flow, 0u)->resource_name);
      check_equal(turbo_flow_stage_at(context.flow, 0u)->adapter_name, "input.adapter");
      check_equal(turbo_flow_stage_at(context.flow, 1u)->resource_name, "routing");
      check_null(turbo_flow_stage_at(context.flow, 1u)->adapter_name);
    }
    {
      const int rc =
          turbo_flow_plugin_generation_create(context.snapshot, context.resolved, &context.flow,
                                              &generation_config, &generation, &config_error);
      info("resource order generation status=%d path=%s message=%s", rc, config_error.path,
           config_error.message);
      check_equal(rc, SALTS_EINVAL);
    }
    check_null(generation);
    check_null(context.flow);
    check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
  }

  it("blocks retirement with leases and keeps DLLs retained through owner destruction") {
    flow_plugin_generation_test_context_t context;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    generation_config.owner_capacity = 2u;

    check_equal(flow_plugin_generation_test_open(&context, FLOW_PLUGIN_GENERATION_FIXTURE_LIFECYCLE,
                                                 &plugin_error, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                    &context.flow, &generation_config, &generation,
                                                    &config_error),
                SALTS_OK);
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_lease_acquire(generation), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &config_error),
                SALTS_EBUSY);
    check_equal(config_error.path, "$.generation.leases");
    check_equal(turbo_flow_plugin_host_destroy(context.host, 1000u, &plugin_error), SALTS_EBUSY);
    check_equal(turbo_flow_plugin_generation_lease_release(generation), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_lease_release(generation), SALTS_EINVAL);
    check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &config_error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_destroy(context.host, 1000u, &plugin_error), SALTS_EBUSY);
    check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
  }

  it("resumes failed owner lifecycle phases without repeating completed callbacks") {
    const char *fixtures[] = {FLOW_PLUGIN_GENERATION_FIXTURE_QUIESCE_ONCE,
                              FLOW_PLUGIN_GENERATION_FIXTURE_DRAIN_ONCE,
                              FLOW_PLUGIN_GENERATION_FIXTURE_SHUTDOWN_ONCE,
                              FLOW_PLUGIN_GENERATION_FIXTURE_QUIESCE_TIMEOUT,
                              FLOW_PLUGIN_GENERATION_FIXTURE_DRAIN_TIMEOUT};
    const char *paths[] = {
        "$.adapters.output.adapter.owner.quiesce", "$.adapters.output.adapter.owner.drain",
        "$.adapters.output.adapter.owner.shutdown", "$.adapters.output.adapter.owner.quiesce",
        "$.adapters.output.adapter.owner.drain"};
    const int statuses[] = {SALTS_EIO, SALTS_EIO, SALTS_EIO, SALTS_ETIMEDOUT, SALTS_ETIMEDOUT};
    const turbo_flow_plugin_generation_state_t states[] = {
        TURBO_FLOW_PLUGIN_GENERATION_ACTIVE, TURBO_FLOW_PLUGIN_GENERATION_STOPPED,
        TURBO_FLOW_PLUGIN_GENERATION_DRAINED, TURBO_FLOW_PLUGIN_GENERATION_ACTIVE,
        TURBO_FLOW_PLUGIN_GENERATION_STOPPED};
    for (size_t i = 0u; i < 5u; ++i) {
      flow_plugin_generation_test_context_t context;
      turbo_flow_plugin_generation_config_t generation_config =
          TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
      turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_plugin_generation_t *generation = NULL;
      generation_config.owner_capacity = 2u;

      check_equal(
          flow_plugin_generation_test_open(&context, fixtures[i], &plugin_error, &config_error),
          SALTS_OK);
      check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                      &context.flow, &generation_config,
                                                      &generation, &config_error),
                  SALTS_OK);
      check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);
      check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &config_error),
                  statuses[i]);
      check_equal(config_error.path, paths[i]);
      check_equal(turbo_flow_plugin_generation_state(generation), states[i]);
      check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &config_error), SALTS_OK);
      check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
    }
  }
}
