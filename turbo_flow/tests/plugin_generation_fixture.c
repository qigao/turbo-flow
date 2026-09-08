#include "turbo_flow_plugin_generation.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#ifndef FLOW_PLUGIN_GENERATION_FIXTURE_ID
  #define FLOW_PLUGIN_GENERATION_FIXTURE_ID "fixture.transactional"
#endif
#ifndef FLOW_PLUGIN_GENERATION_ADAPTER_KIND
  #define FLOW_PLUGIN_GENERATION_ADAPTER_KIND "fixture.transactional.adapter"
#endif
#ifndef FLOW_PLUGIN_GENERATION_RESOURCE_KIND
  #define FLOW_PLUGIN_GENERATION_RESOURCE_KIND "fixture.transactional.resource"
#endif
#ifndef FLOW_PLUGIN_GENERATION_FAIL_PREFLIGHT_CALL
  #define FLOW_PLUGIN_GENERATION_FAIL_PREFLIGHT_CALL 0u
#endif
#ifndef FLOW_PLUGIN_GENERATION_FAIL_MATERIALIZE_CALL
  #define FLOW_PLUGIN_GENERATION_FAIL_MATERIALIZE_CALL 0u
#endif
#ifndef FLOW_PLUGIN_GENERATION_SKIP_ADAPTER_REGISTRATION_CALL
  #define FLOW_PLUGIN_GENERATION_SKIP_ADAPTER_REGISTRATION_CALL 0u
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS
  #define FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS SIZE_MAX
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS
  #define FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS SIZE_MAX
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS
  #define FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS SIZE_MAX
#endif
#ifndef FLOW_PLUGIN_GENERATION_FAIL_QUIESCE_CALL
  #define FLOW_PLUGIN_GENERATION_FAIL_QUIESCE_CALL 0u
#endif
#ifndef FLOW_PLUGIN_GENERATION_FAIL_DRAIN_CALL
  #define FLOW_PLUGIN_GENERATION_FAIL_DRAIN_CALL 0u
#endif
#ifndef FLOW_PLUGIN_GENERATION_FAIL_SHUTDOWN_CALL
  #define FLOW_PLUGIN_GENERATION_FAIL_SHUTDOWN_CALL 0u
#endif
#ifndef FLOW_PLUGIN_GENERATION_LIFECYCLE_FAIL_STATUS
  #define FLOW_PLUGIN_GENERATION_LIFECYCLE_FAIL_STATUS SALTS_EIO
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE
  #define FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE NULL
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_ASSEMBLY
  #define FLOW_PLUGIN_GENERATION_EXPECT_ASSEMBLY NULL
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_STOPPED_ON_DRAIN
  #define FLOW_PLUGIN_GENERATION_EXPECT_STOPPED_ON_DRAIN 0
#endif

#define FLOW_PLUGIN_GENERATION_LIFECYCLE_MAX 64u

typedef struct flow_plugin_generation_fixture_s {
  const turbo_flow_plugin_host_v1_t *host;
  size_t preflight_calls;
  size_t materialize_calls;
  size_t owner_destroys;
  size_t quiesce_calls;
  size_t drain_calls;
  size_t shutdown_calls;
  size_t lifecycle_size;
  char lifecycle[FLOW_PLUGIN_GENERATION_LIFECYCLE_MAX + 1u];
  size_t assembly_size;
  char assembly[FLOW_PLUGIN_GENERATION_LIFECYCLE_MAX + 1u];
} flow_plugin_generation_fixture_t;

typedef struct flow_plugin_generation_owner_s {
  const turbo_flow_plugin_host_v1_t *host;
  flow_plugin_generation_fixture_t *fixture;
  turbo_flow_t *flow;
  size_t ordinal;
} flow_plugin_generation_owner_t;

static int flow_plugin_generation_error(turbo_flow_config_error_t *error, int status,
                                        const char *path, const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    (void)snprintf(error->path, sizeof(error->path), "%s", path);
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static void flow_plugin_generation_record_assembly(flow_plugin_generation_fixture_t *fixture,
                                                   char phase, char capability) {
  if (!fixture || fixture->assembly_size + 2u > FLOW_PLUGIN_GENERATION_LIFECYCLE_MAX) return;
  fixture->assembly[fixture->assembly_size++] = phase;
  fixture->assembly[fixture->assembly_size++] = capability;
  fixture->assembly[fixture->assembly_size] = '\0';
}

static int flow_plugin_generation_preflight(void *ctx, const turbo_flow_resolved_config_t *resolved,
                                            const char *name, turbo_flow_config_error_t *error) {
  flow_plugin_generation_fixture_t *fixture = (flow_plugin_generation_fixture_t *)ctx;
  if (!fixture || !resolved || !name || !name[0]) return SALTS_EINVAL;
  flow_plugin_generation_record_assembly(fixture, 'p', strcmp(name, "routing") == 0 ? 'r' : 'a');
  fixture->preflight_calls++;
  if (fixture->preflight_calls == FLOW_PLUGIN_GENERATION_FAIL_PREFLIGHT_CALL)
    return flow_plugin_generation_error(error, SALTS_EIO, "$.fixture.preflight",
                                        "fixture preflight failure");
  return SALTS_OK;
}

static void flow_plugin_generation_record(flow_plugin_generation_owner_t *owner, char event) {
  flow_plugin_generation_fixture_t *fixture;
  if (!owner || !owner->fixture) return;
  fixture = owner->fixture;
  if (fixture->lifecycle_size + 2u > FLOW_PLUGIN_GENERATION_LIFECYCLE_MAX) return;
  fixture->lifecycle[fixture->lifecycle_size++] = event;
  fixture->lifecycle[fixture->lifecycle_size++] = (char)('0' + owner->ordinal);
  fixture->lifecycle[fixture->lifecycle_size] = '\0';
}

static int flow_plugin_generation_owner_quiesce(void *ctx, uint64_t timeout_ms) {
  flow_plugin_generation_owner_t *owner = (flow_plugin_generation_owner_t *)ctx;
  (void)timeout_ms;
  if (!owner) return SALTS_EINVAL;
  flow_plugin_generation_record(owner, 'q');
  owner->fixture->quiesce_calls++;
  return owner->fixture->quiesce_calls == FLOW_PLUGIN_GENERATION_FAIL_QUIESCE_CALL
             ? FLOW_PLUGIN_GENERATION_LIFECYCLE_FAIL_STATUS
             : SALTS_OK;
}

static int flow_plugin_generation_owner_drain(void *ctx, uint64_t timeout_ms) {
  flow_plugin_generation_owner_t *owner = (flow_plugin_generation_owner_t *)ctx;
  (void)timeout_ms;
  if (!owner) return SALTS_EINVAL;
  flow_plugin_generation_record(owner, 'd');
  owner->fixture->drain_calls++;
#if FLOW_PLUGIN_GENERATION_EXPECT_STOPPED_ON_DRAIN
  if (turbo_flow_state(owner->flow) != TURBO_FLOW_STATE_STOPPED) return SALTS_EPROTO;
#endif
  return owner->fixture->drain_calls == FLOW_PLUGIN_GENERATION_FAIL_DRAIN_CALL
             ? FLOW_PLUGIN_GENERATION_LIFECYCLE_FAIL_STATUS
             : SALTS_OK;
}

static int flow_plugin_generation_owner_shutdown(void *ctx) {
  flow_plugin_generation_owner_t *owner = (flow_plugin_generation_owner_t *)ctx;
  if (!owner) return SALTS_EINVAL;
  flow_plugin_generation_record(owner, 's');
  owner->fixture->shutdown_calls++;
  return owner->fixture->shutdown_calls == FLOW_PLUGIN_GENERATION_FAIL_SHUTDOWN_CALL
             ? FLOW_PLUGIN_GENERATION_LIFECYCLE_FAIL_STATUS
             : SALTS_OK;
}

static int flow_plugin_generation_adapter_consume(void *ctx, turbo_flow_t *flow,
                                                  const turbo_flow_stage_plan_t *stage,
                                                  turbo_flow_msg_t *message) {
  return ctx && flow && stage && message ? SALTS_OK : SALTS_EINVAL;
}

static void flow_plugin_generation_owner_destroy(void *ctx) {
  flow_plugin_generation_owner_t *owner = (flow_plugin_generation_owner_t *)ctx;
  const turbo_flow_plugin_host_v1_t *host;
  if (!owner) return;
  host = owner->host;
  flow_plugin_generation_record(owner, 'x');
  owner->fixture->owner_destroys++;
  memset(owner, 0, sizeof(*owner));
  host->deallocate(host->ctx, owner);
}

static int flow_plugin_generation_materialize_adapter(
    void *ctx, turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *name,
    turbo_flow_plugin_product_owner_v1_t *owner_out, turbo_flow_config_error_t *error) {
  flow_plugin_generation_fixture_t *fixture = (flow_plugin_generation_fixture_t *)ctx;
  flow_plugin_generation_owner_t *owner;
  turbo_flow_adapter_ops_t ops;
  int rc;
  (void)resolved;
  if (!fixture || !flow || !name || !name[0] || !owner_out) return SALTS_EINVAL;
  flow_plugin_generation_record_assembly(fixture, 'm', 'a');
  fixture->materialize_calls++;
  if (fixture->materialize_calls == FLOW_PLUGIN_GENERATION_FAIL_MATERIALIZE_CALL)
    return flow_plugin_generation_error(error, SALTS_EIO, "$.fixture.materialize",
                                        "fixture materialize failure");
  owner =
      (flow_plugin_generation_owner_t *)fixture->host->allocate(fixture->host->ctx, sizeof(*owner));
  if (!owner) return SALTS_ENOMEM;
  owner->host = fixture->host;
  owner->fixture = fixture;
  owner->flow = flow;
  owner->ordinal = fixture->materialize_calls;
  memset(&ops, 0, sizeof(ops));
  ops.consume = flow_plugin_generation_adapter_consume;
  rc = fixture->materialize_calls == FLOW_PLUGIN_GENERATION_SKIP_ADAPTER_REGISTRATION_CALL
           ? SALTS_OK
           : turbo_flow_register_adapter(flow, name, &ops, owner);
  if (rc != SALTS_OK) {
    fixture->host->deallocate(fixture->host->ctx, owner);
    return rc;
  }
  *owner_out = (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  owner_out->flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD;
  owner_out->ctx = owner;
  owner_out->quiesce = flow_plugin_generation_owner_quiesce;
  owner_out->drain = flow_plugin_generation_owner_drain;
  owner_out->shutdown = flow_plugin_generation_owner_shutdown;
  owner_out->destroy = flow_plugin_generation_owner_destroy;
  return SALTS_OK;
}

static int flow_plugin_generation_materialize_resource(
    void *ctx, turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *name,
    turbo_flow_plugin_product_owner_v1_t *owner_out, turbo_flow_config_error_t *error) {
  flow_plugin_generation_fixture_t *fixture = (flow_plugin_generation_fixture_t *)ctx;
  flow_plugin_generation_owner_t *owner;
  (void)resolved;
  if (!fixture || !name || !name[0] || !owner_out) return SALTS_EINVAL;
  flow_plugin_generation_record_assembly(fixture, 'm', 'r');
  fixture->materialize_calls++;
  if (fixture->materialize_calls == FLOW_PLUGIN_GENERATION_FAIL_MATERIALIZE_CALL)
    return flow_plugin_generation_error(error, SALTS_EIO, "$.fixture.materialize",
                                        "fixture materialize failure");
  owner =
      (flow_plugin_generation_owner_t *)fixture->host->allocate(fixture->host->ctx, sizeof(*owner));
  if (!owner) return SALTS_ENOMEM;
  owner->host = fixture->host;
  owner->fixture = fixture;
  owner->flow = flow;
  owner->ordinal = fixture->materialize_calls;
  *owner_out = (turbo_flow_plugin_product_owner_v1_t)TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
  owner_out->flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD;
  owner_out->ctx = owner;
  owner_out->quiesce = flow_plugin_generation_owner_quiesce;
  owner_out->drain = flow_plugin_generation_owner_drain;
  owner_out->shutdown = flow_plugin_generation_owner_shutdown;
  owner_out->destroy = flow_plugin_generation_owner_destroy;
  return SALTS_OK;
}

static int flow_plugin_generation_fixture_load(const turbo_flow_plugin_host_v1_t *host,
                                               void **plugin_out) {
  flow_plugin_generation_fixture_t *fixture;
  if (!host || !plugin_out || !host->allocate || !host->deallocate) return SALTS_EINVAL;
  *plugin_out = NULL;
  fixture = (flow_plugin_generation_fixture_t *)host->allocate(host->ctx, sizeof(*fixture));
  if (!fixture) return SALTS_ENOMEM;
  memset(fixture, 0, sizeof(*fixture));
  fixture->host = host;
  *plugin_out = fixture;
  return SALTS_OK;
}

static int
flow_plugin_generation_fixture_register(void *plugin,
                                        const turbo_flow_plugin_registration_v1_t *registration) {
  turbo_flow_plugin_transactional_adapter_provider_v1_t adapter =
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_ADAPTER_PROVIDER_V1_INIT;
  turbo_flow_plugin_transactional_resource_provider_v1_t resource =
      TURBO_FLOW_PLUGIN_TRANSACTIONAL_RESOURCE_PROVIDER_V1_INIT;
  int rc;
  if (!plugin || !registration || !registration->add_transactional_adapter_provider ||
      !registration->add_transactional_resource_provider)
    return SALTS_EINVAL;
  adapter.kind = FLOW_PLUGIN_GENERATION_ADAPTER_KIND;
  adapter.ctx = plugin;
  adapter.preflight = flow_plugin_generation_preflight;
  adapter.materialize = flow_plugin_generation_materialize_adapter;
  resource.kind = FLOW_PLUGIN_GENERATION_RESOURCE_KIND;
  resource.ctx = plugin;
  resource.preflight = flow_plugin_generation_preflight;
  resource.materialize = flow_plugin_generation_materialize_resource;
  rc = registration->add_transactional_adapter_provider(registration->ctx, &adapter);
  if (rc != SALTS_OK) return rc;
  return registration->add_transactional_resource_provider(registration->ctx, &resource);
}

static int flow_plugin_generation_fixture_quiesce(void *plugin, uint64_t timeout_ms) {
  (void)timeout_ms;
  return plugin ? SALTS_OK : SALTS_EINVAL;
}

static int flow_plugin_generation_expected_size(size_t actual, size_t expected) {
  return expected == SIZE_MAX || actual == expected;
}

static int flow_plugin_generation_expected_text(const char *actual, const char *expected) {
  return !expected || strcmp(actual, expected) == 0;
}

static int flow_plugin_generation_fixture_shutdown(void *plugin) {
  const flow_plugin_generation_fixture_t *fixture =
      (const flow_plugin_generation_fixture_t *)plugin;
  if (!fixture) return SALTS_EINVAL;
  if (!flow_plugin_generation_expected_size(fixture->preflight_calls,
                                            FLOW_PLUGIN_GENERATION_EXPECT_PREFLIGHT_CALLS) ||
      !flow_plugin_generation_expected_size(fixture->materialize_calls,
                                            FLOW_PLUGIN_GENERATION_EXPECT_MATERIALIZE_CALLS) ||
      !flow_plugin_generation_expected_size(fixture->owner_destroys,
                                            FLOW_PLUGIN_GENERATION_EXPECT_OWNER_DESTROYS) ||
      !flow_plugin_generation_expected_text(fixture->assembly,
                                            FLOW_PLUGIN_GENERATION_EXPECT_ASSEMBLY) ||
      !flow_plugin_generation_expected_text(fixture->lifecycle,
                                            FLOW_PLUGIN_GENERATION_EXPECT_LIFECYCLE))
    return SALTS_EPROTO;
  return SALTS_OK;
}

static void flow_plugin_generation_fixture_destroy(void *plugin) {
  flow_plugin_generation_fixture_t *fixture = (flow_plugin_generation_fixture_t *)plugin;
  const turbo_flow_plugin_host_v1_t *host;
  if (!fixture) return;
  host = fixture->host;
  memset(fixture, 0, sizeof(*fixture));
  host->deallocate(host->ctx, fixture);
}

static const turbo_flow_plugin_api_v1_t flow_plugin_generation_fixture_api = {
    sizeof(turbo_flow_plugin_api_v1_t),
    TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
    TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
    FLOW_PLUGIN_GENERATION_FIXTURE_ID,
    "1.0.0",
    TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_ADAPTER | TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_RESOURCE,
    flow_plugin_generation_fixture_load,
    flow_plugin_generation_fixture_register,
    flow_plugin_generation_fixture_quiesce,
    flow_plugin_generation_fixture_shutdown,
    flow_plugin_generation_fixture_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &flow_plugin_generation_fixture_api;
}
