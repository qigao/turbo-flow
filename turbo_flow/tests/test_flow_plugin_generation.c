#include "tinytest.h"
#include "turbo_flow_plugin_generation.h"

#include <stdlib.h>
#include <string.h>

#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_LEGACY
  #error FLOW_PLUGIN_GENERATION_FIXTURE_LEGACY is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_CAPACITY
  #error FLOW_PLUGIN_GENERATION_FIXTURE_CAPACITY is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_PREFLIGHT_FAIL
  #error FLOW_PLUGIN_GENERATION_FIXTURE_PREFLIGHT_FAIL is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_SUCCESS
  #error FLOW_PLUGIN_GENERATION_FIXTURE_SUCCESS is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_DUPLICATE_REFERENCE
  #error FLOW_PLUGIN_GENERATION_FIXTURE_DUPLICATE_REFERENCE is required
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
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_EXTERNAL_POLL
  #error FLOW_PLUGIN_GENERATION_FIXTURE_EXTERNAL_POLL is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_MIXED_POLL
  #error FLOW_PLUGIN_GENERATION_FIXTURE_MIXED_POLL is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_LEGACY_OWNER_PREFIX
  #error FLOW_PLUGIN_GENERATION_FIXTURE_LEGACY_OWNER_PREFIX is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_POLL_ERROR
  #error FLOW_PLUGIN_GENERATION_FIXTURE_POLL_ERROR is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_POLL_CLOSES
  #error FLOW_PLUGIN_GENERATION_FIXTURE_POLL_CLOSES is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_POLL_FLAG_WITHOUT_CALLBACK
  #error FLOW_PLUGIN_GENERATION_FIXTURE_POLL_FLAG_WITHOUT_CALLBACK is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_POLL_CALLBACK_WITHOUT_FLAG
  #error FLOW_PLUGIN_GENERATION_FIXTURE_POLL_CALLBACK_WITHOUT_FLAG is required
#endif
#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_POLL_SMALL_DESCRIPTOR
  #error FLOW_PLUGIN_GENERATION_FIXTURE_POLL_SMALL_DESCRIPTOR is required
#endif

static const char flow_plugin_generation_yaml[] = "version: 1\n"
                                                  "adapters:\n"
                                                  "  input.adapter:\n"
                                                  "    kind: fixture.transactional.adapter\n"
                                                  "    config: {}\n"
                                                  "  output.adapter:\n"
                                                  "    kind: fixture.transactional.adapter\n"
                                                  "    config: {}\n";

static const char flow_plugin_generation_legacy_yaml[] = "version: 1\n"
                                                         "adapters:\n"
                                                         "  input.adapter:\n"
                                                         "    kind: fixture.adapter.one\n"
                                                         "    config: {}\n"
                                                         "  output.adapter:\n"
                                                         "    kind: fixture.adapter.one\n"
                                                         "    config: {}\n";

static const char flow_plugin_generation_operation_binding_yaml[] =
    "version: 1\n"
    "operation_bindings:\n"
    "  - operation: decision.evaluate\n"
    "    plugin: fixture.typed\n"
    "    version: 1\n"
    "    input_schema: example.Input\n"
    "    input_schema_version: 1\n"
    "    output_schema: example.Decision\n"
    "    output_schema_version: 1\n"
    "    permissions: []\n"
    "    execution: inline\n"
    "    threading: owner\n"
    "    cancellation: none\n"
    "    max_inflight: 1\n"
    "    max_input_bytes: 4096\n"
    "    max_result_bytes: 1024\n"
    "    max_retained_bytes: 8192\n"
    "    max_steps: 10000\n"
    "    deadline_ms: 0\n"
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

static const char flow_plugin_generation_three_yaml[] = "version: 1\n"
                                                        "adapters:\n"
                                                        "  input.adapter:\n"
                                                        "    kind: fixture.transactional.adapter\n"
                                                        "    config: {}\n"
                                                        "  middle.adapter:\n"
                                                        "    kind: fixture.transactional.adapter\n"
                                                        "    config: {}\n"
                                                        "  output.adapter:\n"
                                                        "    kind: fixture.transactional.adapter\n"
                                                        "    config: {}\n";

static const char flow_plugin_generation_three_graph[] = "source input adapter input.adapter\n"
                                                         "stage middle adapter middle.adapter\n"
                                                         "stage output adapter output.adapter\n"
                                                         "stage main {\n"
                                                         "  input -> middle\n"
                                                         "  middle -> output\n"
                                                         "}\n";

static const char flow_plugin_generation_duplicate_reference_graph[] =
    "source input adapter input.adapter\n"
    "stage output adapter input.adapter\n"
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
  turbo_flow_plugin_generation_t *cleanup;
} flow_plugin_generation_test_context_t;

static int flow_plugin_generation_test_poll(void *ctx, uint32_t timeout_ms) {
  (void)ctx;
  (void)timeout_ms;
  return SALTS_OK;
}

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
  if (context->cleanup) {
    turbo_flow_config_error_t cleanup_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    rc = turbo_flow_plugin_generation_destroy(context->cleanup, 1000u, &cleanup_error);
    if (rc != SALTS_OK) return rc;
    context->cleanup = NULL;
  }
  turbo_flow_destroy(context->flow);
  context->flow = NULL;
  turbo_flow_resolved_config_destroy(context->resolved);
  context->resolved = NULL;
  turbo_flow_plugin_catalog_snapshot_destroy(context->snapshot);
  context->snapshot = NULL;
  if (context->host) rc = turbo_flow_plugin_host_destroy(context->host, 1000u, error);
  if (rc == SALTS_OK) context->host = NULL;
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
  it("preserves Graph stop failure and retries without repeating completed Product phases") {
    flow_plugin_generation_test_context_t context;
    turbo_flow_plugin_generation_config_t config = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t ce = TURBO_FLOW_CONFIG_ERROR_INIT,
                              observed = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    check_equal(flow_plugin_generation_test_open(
                    &context, FLOW_PLUGIN_GENERATION_FIXTURE_GRAPH_STOP, &pe, &ce),
                SALTS_OK);
    check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                    &context.flow, &config, NULL, &generation,
                                                    &context.cleanup, &ce),
                SALTS_OK);
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_destroy(generation, 0, &ce), SALTS_EIO);
    check_equal(ce.path, "$.generation.graph.stop");
    check_equal(turbo_flow_plugin_generation_cleanup_error(generation, &observed), SALTS_OK);
    check_equal(observed.status, SALTS_EIO);
    check_equal(turbo_flow_plugin_host_destroy(context.host, 0, &pe), SALTS_EBUSY);
    check_equal(turbo_flow_plugin_generation_destroy(generation, 0, &ce), SALTS_OK);
    check_equal(flow_plugin_generation_test_close(&context, &pe), SALTS_OK);
  }
  it("rejects operation bindings without a result domain before consuming the Graph") {
    flow_plugin_generation_test_context_t context;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_t *original_flow;
    generation_config.owner_capacity = 2u;

    check_equal(flow_plugin_generation_test_open(&context, FLOW_PLUGIN_GENERATION_FIXTURE_CAPACITY,
                                                 &plugin_error, &config_error),
                SALTS_OK);
    check_equal(flow_plugin_generation_test_replace_documents(
                    &context, flow_plugin_generation_operation_binding_yaml,
                    sizeof(flow_plugin_generation_operation_binding_yaml) - 1u,
                    flow_plugin_generation_graph, sizeof(flow_plugin_generation_graph) - 1u,
                    &config_error),
                SALTS_OK);
    original_flow = context.flow;
    check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                    &context.flow, &generation_config, NULL,
                                                    &generation, &context.cleanup, &config_error),
                SALTS_EINVAL);
    check_equal(config_error.status, SALTS_EINVAL);
    check_equal(config_error.path, "$.operation_bindings[0].result_domain");
    check_null(generation);
    check(context.flow == original_flow);
    check_equal(turbo_flow_state(context.flow), TURBO_FLOW_STATE_PARSED);
    check_equal(turbo_flow_adapter_count(context.flow), 0u);
    check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
  }

  it("publishes Product owners within the caller-provided ABI capacity") {
    union {
      turbo_flow_plugin_product_owner_v1_t alignment;
      unsigned char bytes[offsetof(turbo_flow_plugin_product_owner_v1_t, poll) + sizeof(uint64_t)];
    } storage;
    const uint64_t canary = UINT64_C(0x6B3A1D5E92C7408F);
    uint64_t observed_canary = 0u;
    turbo_flow_plugin_product_owner_v1_t desired = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
    turbo_flow_plugin_product_owner_v1_t *legacy_out =
        (turbo_flow_plugin_product_owner_v1_t *)(void *)storage.bytes;

    memset(&storage, 0, sizeof(storage));
    legacy_out->size = offsetof(turbo_flow_plugin_product_owner_v1_t, poll);
    memcpy(storage.bytes + offsetof(turbo_flow_plugin_product_owner_v1_t, poll), &canary, sizeof(canary));
    desired.flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD |
                    TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
    desired.poll = flow_plugin_generation_test_poll;
    check_equal(turbo_flow_plugin_product_owner_publish(legacy_out, &desired), SALTS_EINVAL);
    memcpy(&observed_canary, storage.bytes + offsetof(turbo_flow_plugin_product_owner_v1_t, poll),
           sizeof(observed_canary));
    check_equal(observed_canary, canary);

    {
      const size_t sizes[] = {0u,
                              offsetof(turbo_flow_plugin_product_owner_v1_t, abi_minor) +
                                  sizeof(uint32_t) - 1u,
                              sizeof(desired) - 1u, sizeof(desired) + 1u};
      for (size_t i = 0u; i < sizeof(sizes) / sizeof(sizes[0]); ++i) {
        turbo_flow_plugin_product_owner_v1_t out = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
        desired = (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
        desired.size = sizes[i];
        check_equal(turbo_flow_plugin_product_owner_publish(&out, &desired), SALTS_EINVAL);
        check_equal(out.ctx, NULL);
      }
      desired = (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
      desired.abi_minor = 1u;
      {
        turbo_flow_plugin_product_owner_v1_t out = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
        check_equal(turbo_flow_plugin_product_owner_publish(&out, &desired), SALTS_EINVAL);
        check_equal(out.ctx, NULL);
      }
    }

    desired = (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
    desired.flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD;
    desired.poll = NULL;
    check_equal(turbo_flow_plugin_product_owner_publish(legacy_out, &desired), SALTS_EINVAL);
    check_equal(legacy_out->size, offsetof(turbo_flow_plugin_product_owner_v1_t, poll));
    memcpy(&observed_canary, storage.bytes + offsetof(turbo_flow_plugin_product_owner_v1_t, poll),
           sizeof(observed_canary));
    check_equal(observed_canary, canary);
  }

  it("rejects incompatible owner publication destinations without modifying any output bytes") {
    const uint32_t versions[][2] = {{2u, 0u}, {3u, 1u}, {4u, 0u}};
    const size_t sizes[] = {0u, sizeof(size_t), sizeof(turbo_flow_plugin_product_owner_v1_t) - 1u,
                            sizeof(turbo_flow_plugin_product_owner_v1_t) + 1u};
    turbo_flow_plugin_product_owner_v1_t desired = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
    size_t *prefix = malloc(sizeof(*prefix));
    check_not_null(prefix);
    *prefix = sizeof(*prefix);
    int prefix_rc = turbo_flow_plugin_product_owner_publish(
        (turbo_flow_plugin_product_owner_v1_t *)(void *)prefix, &desired);
    size_t prefix_after = *prefix;
    free(prefix);
    check_equal(prefix_rc, SALTS_EINVAL);
    check_equal(prefix_after, sizeof(size_t));
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); ++i) {
      turbo_flow_plugin_product_owner_v1_t out = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
      unsigned char before[sizeof(out)];
      out.abi_major = versions[i][0];
      out.abi_minor = versions[i][1];
      out.ctx = &desired;
      memcpy(before, &out, sizeof(out));
      int rc = turbo_flow_plugin_product_owner_publish(&out, &desired);
      check_equal(rc, SALTS_EINVAL);
      check_equal(memcmp(before, &out, sizeof(out)), 0);
    }
    for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); ++i) {
      turbo_flow_plugin_product_owner_v1_t out = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
      unsigned char before[sizeof(out)];
      out.size = sizes[i];
      out.ctx = &desired;
      memcpy(before, &out, sizeof(out));
      check_equal(turbo_flow_plugin_product_owner_publish(&out, &desired), SALTS_EINVAL);
      check_equal(memcmp(before, &out, sizeof(out)), 0);
    }
  }

  it("rejects a legacy-only Product catalog without fallback") {
    flow_plugin_generation_test_context_t context = {0};
    turbo_flow_plugin_host_config_t host_config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    host_config.module_capacity = 1u;
    host_config.adapter_provider_capacity = 1u;
    host_config.resource_provider_capacity = 1u;
    host_config.protocol_provider_capacity = 0u;
    host_config.business_provider_capacity = 0u;
    host_config.transactional_adapter_provider_capacity = 0u;
    host_config.transactional_resource_provider_capacity = 0u;
    generation_config.owner_capacity = 2u;

    check_equal(turbo_flow_plugin_host_create(&host_config, &context.host, &plugin_error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(context.host, FLOW_PLUGIN_GENERATION_FIXTURE_LEGACY,
                                            &plugin_error),
                SALTS_OK);
    check_equal(
        turbo_flow_plugin_catalog_snapshot_create(context.host, &context.snapshot, &plugin_error),
        SALTS_OK);
    check_equal(turbo_flow_config_resolve_yaml(flow_plugin_generation_legacy_yaml,
                                               sizeof(flow_plugin_generation_legacy_yaml) - 1u,
                                               &context.resolved, &config_error),
                SALTS_OK);
    context.flow = turbo_flow_create();
    check_not_null(context.flow);
    check_equal(turbo_flow_parse_string(context.flow, flow_plugin_generation_graph,
                                        sizeof(flow_plugin_generation_graph) - 1u),
                SALTS_OK);
    check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                    &context.flow, &generation_config, NULL,
                                                    &generation, &context.cleanup, &config_error),
                SALTS_ENOTSUP);
    check_equal(config_error.path, "$.adapters.input.adapter");
    check_null(generation);
    check_not_null(context.flow);
    check_equal(turbo_flow_state(context.flow), TURBO_FLOW_STATE_PARSED);
    check_equal(turbo_flow_adapter_count(context.flow), 0u);
    check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
  }

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
                                                      &context.flow, &generation_config, NULL,
                                                      &generation, &context.cleanup, &config_error),
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
                                                    &context.flow, &generation_config, NULL,
                                                    &generation, &context.cleanup, &config_error),
                SALTS_EIO);
    check_equal(config_error.path, "$.fixture.preflight");
    check_null(generation);
    check_not_null(context.flow);
    check_equal(turbo_flow_state(context.flow), TURBO_FLOW_STATE_PARSED);
    check_equal(turbo_flow_adapter_count(context.flow), 0u);
    check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
  }

  it("moves and compiles with exact and one-spare owner capacity") {
    for (size_t capacity = 2u; capacity <= 3u; ++capacity) {
      flow_plugin_generation_test_context_t context;
      turbo_flow_plugin_generation_config_t generation_config =
          TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
      turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
      turbo_flow_plugin_generation_t *generation = NULL;
      turbo_flow_t *compiled_flow;
      generation_config.owner_capacity = capacity;

      check_equal(flow_plugin_generation_test_open(&context, FLOW_PLUGIN_GENERATION_FIXTURE_SUCCESS,
                                                   &plugin_error, &config_error),
                  SALTS_OK);
      {
        const int rc = turbo_flow_plugin_generation_create(
            context.snapshot, context.resolved, &context.flow, &generation_config, NULL,
            &generation, &context.cleanup, &config_error);
        info("generation status=%d path=%s message=%s", rc, config_error.path,
             config_error.message);
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
  }

  it("polls external owners once per round with a rotating total timeout") {
    flow_plugin_generation_test_context_t context;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    generation_config.owner_capacity = 3u;

    check_equal(flow_plugin_generation_test_open(&context,
                                                 FLOW_PLUGIN_GENERATION_FIXTURE_EXTERNAL_POLL,
                                                 &plugin_error, &config_error),
                SALTS_OK);
    check_equal(flow_plugin_generation_test_replace_documents(
                    &context, flow_plugin_generation_three_yaml,
                    sizeof(flow_plugin_generation_three_yaml) - 1u,
                    flow_plugin_generation_three_graph,
                    sizeof(flow_plugin_generation_three_graph) - 1u, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                    &context.flow, &generation_config, NULL,
                                                    &generation, &context.cleanup, &config_error),
                SALTS_OK);
    turbo_flow_plugin_catalog_snapshot_destroy(context.snapshot);
    context.snapshot = NULL;
    check_equal(turbo_flow_plugin_generation_poll(generation, 7u, &config_error), SALTS_EBUSY);
    check_equal(config_error.path, "$.generation.poll");
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_poll(generation, 7u, &config_error), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_poll(generation, 11u, &config_error), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &config_error), SALTS_OK);
    check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
  }

  it("polls one eligible owner among lifecycle-only owners including zero timeout") {
    flow_plugin_generation_test_context_t context;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    generation_config.owner_capacity = 2u;

    check_equal(flow_plugin_generation_test_open(&context,
                                                 FLOW_PLUGIN_GENERATION_FIXTURE_MIXED_POLL,
                                                 &plugin_error, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                    &context.flow, &generation_config, NULL,
                                                    &generation, &context.cleanup, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_poll(generation, 13u, &config_error), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_poll(generation, 0u, &config_error), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &config_error), SALTS_OK);
    check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
  }

  it("rejects inconsistent external poll descriptors transactionally") {
    const char *fixtures[] = {FLOW_PLUGIN_GENERATION_FIXTURE_POLL_FLAG_WITHOUT_CALLBACK,
                              FLOW_PLUGIN_GENERATION_FIXTURE_POLL_CALLBACK_WITHOUT_FLAG};
    for (size_t i = 0u; i < sizeof(fixtures) / sizeof(fixtures[0]); ++i) {
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
                                                      &context.flow, &generation_config, NULL,
                                                      &generation, &context.cleanup, &config_error),
                  SALTS_EPROTO);
      check_equal(config_error.path, "$.adapters.input.adapter");
      check_null(context.flow);
      check_null(generation);
      check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
    }
  }

  it("returns the exact external owner poll error path") {
    flow_plugin_generation_test_context_t context;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    generation_config.owner_capacity = 2u;

    check_equal(flow_plugin_generation_test_open(&context,
                                                 FLOW_PLUGIN_GENERATION_FIXTURE_POLL_ERROR,
                                                 &plugin_error, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                    &context.flow, &generation_config, NULL,
                                                    &generation, &context.cleanup, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_poll(generation, 5u, &config_error), SALTS_EIO);
    check_equal(config_error.path, "$.adapters.output.adapter.owner.poll");
    check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &config_error), SALTS_OK);
    check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
  }

  it("keeps progress closed after retirement begins") {
    flow_plugin_generation_test_context_t context;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    generation_config.owner_capacity = 2u;

    check_equal(flow_plugin_generation_test_open(&context,
                                                 FLOW_PLUGIN_GENERATION_FIXTURE_POLL_CLOSES,
                                                 &plugin_error, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                    &context.flow, &generation_config, NULL,
                                                    &generation, &context.cleanup, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_destroy(generation, 1u, &config_error),
                SALTS_ETIMEDOUT);
    check_equal(turbo_flow_plugin_generation_poll(generation, 0u, &config_error), SALTS_EBUSY);
    check_equal(config_error.path, "$.generation.poll");
    check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &config_error), SALTS_OK);
    check_equal(flow_plugin_generation_test_close(&context, &plugin_error), SALTS_OK);
  }

  it("materializes one owner for a repeated named adapter reference") {
    flow_plugin_generation_test_context_t context;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    generation_config.owner_capacity = 1u;

    check_equal(flow_plugin_generation_test_open(&context,
                                                 FLOW_PLUGIN_GENERATION_FIXTURE_DUPLICATE_REFERENCE,
                                                 &plugin_error, &config_error),
                SALTS_OK);
    check_equal(flow_plugin_generation_test_replace_documents(
                    &context, flow_plugin_generation_yaml, sizeof(flow_plugin_generation_yaml) - 1u,
                    flow_plugin_generation_duplicate_reference_graph,
                    sizeof(flow_plugin_generation_duplicate_reference_graph) - 1u, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_plugin_generation_create(context.snapshot, context.resolved,
                                                    &context.flow, &generation_config, NULL,
                                                    &generation, &context.cleanup, &config_error),
                SALTS_OK);
    check_null(context.flow);
    check_not_null(generation);
    check_equal(turbo_flow_plugin_generation_owner_count(generation), 1u);
    check_equal(turbo_flow_adapter_count(turbo_flow_plugin_generation_flow(generation)), 1u);
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
                                                      &context.flow, &generation_config, NULL,
                                                      &generation, &context.cleanup, &config_error),
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
      const int rc = turbo_flow_plugin_generation_create(
          context.snapshot, context.resolved, &context.flow, &generation_config, NULL, &generation,
          &context.cleanup, &config_error);
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
      const int rc = turbo_flow_plugin_generation_create(
          context.snapshot, context.resolved, &context.flow, &generation_config, NULL, &generation,
          &context.cleanup, &config_error);
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
                                                    &context.flow, &generation_config, NULL,
                                                    &generation, &context.cleanup, &config_error),
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
                                                      &context.flow, &generation_config, NULL,
                                                      &generation, &context.cleanup, &config_error),
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
