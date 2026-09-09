#include "plugin_generation_owner_fixture.h"
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
#ifndef FLOW_PLUGIN_GENERATION_FAIL_GRAPH_STOP
  #define FLOW_PLUGIN_GENERATION_FAIL_GRAPH_STOP 0
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
#ifndef FLOW_PLUGIN_GENERATION_ENABLE_POLL
  #define FLOW_PLUGIN_GENERATION_ENABLE_POLL 0
#endif
#ifndef FLOW_PLUGIN_GENERATION_POLL_OWNER_ORDINAL
  #define FLOW_PLUGIN_GENERATION_POLL_OWNER_ORDINAL 0u
#endif
#ifndef FLOW_PLUGIN_GENERATION_LEGACY_OWNER_PREFIX
  #define FLOW_PLUGIN_GENERATION_LEGACY_OWNER_PREFIX 0
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_POLL_CALLS
  #define FLOW_PLUGIN_GENERATION_EXPECT_POLL_CALLS SIZE_MAX
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_BLOCKING_POLL_CALLS
  #define FLOW_PLUGIN_GENERATION_EXPECT_BLOCKING_POLL_CALLS SIZE_MAX
#endif
#ifndef FLOW_PLUGIN_GENERATION_FORCE_COMPILE_ROLE_MISMATCH
  #define FLOW_PLUGIN_GENERATION_FORCE_COMPILE_ROLE_MISMATCH 0
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_GRAPH_SHUTDOWN_BEFORE_OWNER_DESTROY
  #define FLOW_PLUGIN_GENERATION_EXPECT_GRAPH_SHUTDOWN_BEFORE_OWNER_DESTROY 0
#endif
#ifndef FLOW_PLUGIN_GENERATION_POLL_DESCRIPTOR_MODE
  #define FLOW_PLUGIN_GENERATION_POLL_DESCRIPTOR_MODE 0
#endif
#ifndef FLOW_PLUGIN_GENERATION_FAIL_POLL_CALL
  #define FLOW_PLUGIN_GENERATION_FAIL_POLL_CALL 0u
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_1
  #define FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_1 UINT32_MAX
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_2
  #define FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_2 UINT32_MAX
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_3
  #define FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_3 UINT32_MAX
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_4
  #define FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_4 UINT32_MAX
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_5
  #define FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_5 UINT32_MAX
#endif
#ifndef FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_6
  #define FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_6 UINT32_MAX
#endif

#define FLOW_PLUGIN_GENERATION_LIFECYCLE_MAX 64u

#if FLOW_PLUGIN_GENERATION_ENABLE_POLL || FLOW_PLUGIN_GENERATION_POLL_DESCRIPTOR_MODE == 1 ||      \
    FLOW_PLUGIN_GENERATION_POLL_DESCRIPTOR_MODE == 3
  #define FLOW_PLUGIN_GENERATION_ROOT_POLL_CAPABILITY TURBO_FLOW_PLUGIN_CAP_EXTERNAL_POLL
#else
  #define FLOW_PLUGIN_GENERATION_ROOT_POLL_CAPABILITY 0u
#endif

typedef struct flow_plugin_generation_fixture_s {
  generation_owner_observer_t observer;
  const turbo_flow_plugin_host_v1_t *host;
  size_t preflight_calls;
  size_t materialize_calls;
  size_t owner_destroys;
  size_t quiesce_calls;
  size_t drain_calls;
  size_t shutdown_calls;
  size_t graph_stop_calls;
  size_t poll_calls;
  size_t blocking_poll_calls;
  size_t owner_destroy_before_graph_shutdown;
  size_t lifecycle_size;
  char lifecycle[FLOW_PLUGIN_GENERATION_LIFECYCLE_MAX + 1u];
  size_t assembly_size;
  char assembly[FLOW_PLUGIN_GENERATION_LIFECYCLE_MAX + 1u];
  struct flow_plugin_generation_owner_s *retained_owners[FLOW_PLUGIN_GENERATION_LIFECYCLE_MAX];
  size_t retained_owner_count;
} flow_plugin_generation_fixture_t;

typedef struct flow_plugin_generation_owner_s {
  const turbo_flow_plugin_host_v1_t *host;
  flow_plugin_generation_fixture_t *fixture;
  turbo_flow_t *flow;
  size_t ordinal;
  int graph_shutdown;
} flow_plugin_generation_owner_t;

static void flow_plugin_generation_record(flow_plugin_generation_owner_t *owner, char event);

static int flow_plugin_generation_owner_poll(void *ctx, uint32_t timeout_ms) {
  static const uint32_t expected_timeouts[] = {
      FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_1, FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_2,
      FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_3, FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_4,
      FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_5, FLOW_PLUGIN_GENERATION_EXPECT_POLL_TIMEOUT_6};
  flow_plugin_generation_owner_t *owner = (flow_plugin_generation_owner_t *)ctx;
  size_t call_index;
  if (!owner) return SALTS_EINVAL;
  flow_plugin_generation_record(owner, 'p');
  call_index = owner->fixture->poll_calls;
  owner->fixture->poll_calls++;
  if (timeout_ms != 0u) owner->fixture->blocking_poll_calls++;
  if (call_index < sizeof(expected_timeouts) / sizeof(expected_timeouts[0]) &&
      expected_timeouts[call_index] != UINT32_MAX && expected_timeouts[call_index] != timeout_ms)
    return SALTS_EPROTO;
  return owner->fixture->poll_calls == FLOW_PLUGIN_GENERATION_FAIL_POLL_CALL ? SALTS_EIO : SALTS_OK;
}

#if FLOW_PLUGIN_GENERATION_ENABLE_POLL
static int flow_plugin_generation_poll_enabled(size_t ordinal) {
  #if FLOW_PLUGIN_GENERATION_POLL_OWNER_ORDINAL == 0u
  (void)ordinal;
  return 1;
  #else
  return ordinal == FLOW_PLUGIN_GENERATION_POLL_OWNER_ORDINAL;
  #endif
}
#endif

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

static int
flow_plugin_generation_publish_fixture_owner(turbo_flow_plugin_product_owner_v1_t *owner_out,
                                             const turbo_flow_plugin_product_owner_v1_t *owner) {
  const flow_plugin_generation_owner_t *instance = owner->ctx;
  turbo_flow_plugin_product_owner_v1_t fault = *owner;
  switch (instance->fixture->observer.mode) {
  case OWNER_FIXTURE_NO_QUIESCE:
    fault.quiesce = NULL;
    break;
  case OWNER_FIXTURE_NO_DRAIN:
    fault.drain = NULL;
    break;
  case OWNER_FIXTURE_NO_SHUTDOWN:
    fault.shutdown = NULL;
    break;
  case OWNER_FIXTURE_SHORT:
    --fault.size;
    break;
  case OWNER_FIXTURE_TINY:
    fault.size = sizeof(size_t);
    break;
  case OWNER_FIXTURE_LONG:
    ++fault.size;
    break;
  case OWNER_FIXTURE_OLD_MAJOR:
    --fault.abi_major;
    break;
  case OWNER_FIXTURE_FUTURE_MAJOR:
    ++fault.abi_major;
    break;
  case OWNER_FIXTURE_FUTURE_MINOR:
    ++fault.abi_minor;
    break;
  case OWNER_FIXTURE_NO_DESTROY:
    fault.destroy = NULL;
    break;
  case OWNER_FIXTURE_NO_CONTEXT:
    fault.ctx = NULL;
    break;
  default:
    break;
  }
  if (instance->fixture->observer.mode != OWNER_FIXTURE_NORMAL) {
    /* Deliberately violate publication for true DLL boundary rejection tests. */
    *owner_out = fault;
    return SALTS_OK;
  }
#if FLOW_PLUGIN_GENERATION_LEGACY_OWNER_PREFIX
  if (!owner_out || !owner || owner_out->size < offsetof(turbo_flow_plugin_product_owner_v1_t, poll))
    return SALTS_EINVAL;
  memcpy(owner_out, owner, offsetof(turbo_flow_plugin_product_owner_v1_t, poll));
  owner_out->size = offsetof(turbo_flow_plugin_product_owner_v1_t, poll);
  return SALTS_OK;
#elif FLOW_PLUGIN_GENERATION_POLL_DESCRIPTOR_MODE != 0
  size_t copy_size;
  if (!owner_out || !owner || owner_out->size < offsetof(turbo_flow_plugin_product_owner_v1_t, poll))
    return SALTS_EINVAL;
  copy_size = owner_out->size < sizeof(*owner) ? owner_out->size : sizeof(*owner);
  memcpy(owner_out, owner, copy_size);
  return SALTS_OK;
#else
  return turbo_flow_plugin_product_owner_publish(owner_out, owner);
#endif
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
  if (event == 'x') ++fixture->observer.destroys;
  else ++fixture->observer.lifecycle_calls;
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

static void flow_plugin_generation_adapter_shutdown(void *ctx) {
  flow_plugin_generation_owner_t *owner = (flow_plugin_generation_owner_t *)ctx;
  if (owner) {
    owner->graph_shutdown = 1;
    ++owner->fixture->observer.graph_shutdowns;
  }
}
#if FLOW_PLUGIN_GENERATION_FAIL_GRAPH_STOP
static void flow_plugin_generation_adapter_stop(void *ctx, turbo_flow_t *flow,
                                                const turbo_flow_stage_plan_t *stage) {
  flow_plugin_generation_owner_t *owner = ctx;
  (void)stage;
  flow_plugin_generation_record(owner, 'g');
  if (++owner->fixture->graph_stop_calls == 1u)
    (void)turbo_flow_adapter_report_stop_status(flow, SALTS_EIO);
}
#endif

static void flow_plugin_generation_owner_destroy(void *ctx) {
  flow_plugin_generation_owner_t *owner = (flow_plugin_generation_owner_t *)ctx;
  const turbo_flow_plugin_host_v1_t *host;
  if (!owner) return;
  host = owner->host;
  flow_plugin_generation_record(owner, 'x');
  owner->fixture->owner_destroys++;
  if (!owner->graph_shutdown) ++owner->fixture->observer.destroys_before_graph;
#if FLOW_PLUGIN_GENERATION_EXPECT_GRAPH_SHUTDOWN_BEFORE_OWNER_DESTROY
  if (!owner->graph_shutdown) owner->fixture->owner_destroy_before_graph_shutdown++;
#else
  memset(owner, 0, sizeof(*owner));
  host->deallocate(host->ctx, owner);
#endif
}

static int flow_plugin_generation_materialize_adapter(
    void *ctx, turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *name,
    turbo_flow_plugin_product_owner_v1_t *owner_out, turbo_flow_config_error_t *error) {
  flow_plugin_generation_fixture_t *fixture = (flow_plugin_generation_fixture_t *)ctx;
  flow_plugin_generation_owner_t *owner;
  turbo_flow_plugin_product_owner_v1_t descriptor = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
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
  memset(owner, 0, sizeof(*owner));
  owner->host = fixture->host;
  owner->fixture = fixture;
  owner->flow = flow;
  owner->ordinal = fixture->materialize_calls;
  if (fixture->retained_owner_count < FLOW_PLUGIN_GENERATION_LIFECYCLE_MAX)
    fixture->retained_owners[fixture->retained_owner_count++] = owner;
  memset(&ops, 0, sizeof(ops));
  ops.consume = flow_plugin_generation_adapter_consume;
#if FLOW_PLUGIN_GENERATION_FAIL_GRAPH_STOP
  ops.stop = flow_plugin_generation_adapter_stop;
#endif
#if FLOW_PLUGIN_GENERATION_EXPECT_GRAPH_SHUTDOWN_BEFORE_OWNER_DESTROY
  ops.shutdown = flow_plugin_generation_adapter_shutdown;
#endif
#if FLOW_PLUGIN_GENERATION_FORCE_COMPILE_ROLE_MISMATCH
  {
    turbo_flow_adapter_schema_t schema = {0};
    schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
    schema.roles = TURBO_FLOW_ADAPTER_SOURCE;
    schema.direction = TURBO_FLOW_ADAPTER_INPUT;
    rc = turbo_flow_register_adapter_ex(flow, name, &ops, owner, &schema);
  }
#else
  rc = fixture->materialize_calls == FLOW_PLUGIN_GENERATION_SKIP_ADAPTER_REGISTRATION_CALL
           ? SALTS_OK
           : turbo_flow_register_adapter(flow, name, &ops, owner);
#endif
  if (rc != SALTS_OK) {
    fixture->retained_owner_count--;
    fixture->host->deallocate(fixture->host->ctx, owner);
    return rc;
  }
  descriptor.flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD;
  descriptor.ctx = owner;
  descriptor.quiesce = flow_plugin_generation_owner_quiesce;
  descriptor.drain = flow_plugin_generation_owner_drain;
  descriptor.shutdown = flow_plugin_generation_owner_shutdown;
  descriptor.destroy = flow_plugin_generation_owner_destroy;
#if FLOW_PLUGIN_GENERATION_ENABLE_POLL
  if (flow_plugin_generation_poll_enabled(owner->ordinal)) {
    descriptor.flags |= TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
    descriptor.poll = flow_plugin_generation_owner_poll;
  }
#endif
#if FLOW_PLUGIN_GENERATION_POLL_DESCRIPTOR_MODE == 1
  descriptor.flags |= TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
#elif FLOW_PLUGIN_GENERATION_POLL_DESCRIPTOR_MODE == 2
  descriptor.poll = flow_plugin_generation_owner_poll;
#elif FLOW_PLUGIN_GENERATION_POLL_DESCRIPTOR_MODE == 3
  descriptor.flags |= TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
  descriptor.poll = flow_plugin_generation_owner_poll;
  descriptor.size = offsetof(turbo_flow_plugin_product_owner_v1_t, poll);
#endif
#if FLOW_PLUGIN_GENERATION_LEGACY_OWNER_PREFIX
  descriptor.size = offsetof(turbo_flow_plugin_product_owner_v1_t, poll);
#endif
  return flow_plugin_generation_publish_fixture_owner(owner_out, &descriptor);
}

static int flow_plugin_generation_materialize_resource(
    void *ctx, turbo_flow_t *flow, const turbo_flow_resolved_config_t *resolved, const char *name,
    turbo_flow_plugin_product_owner_v1_t *owner_out, turbo_flow_config_error_t *error) {
  flow_plugin_generation_fixture_t *fixture = (flow_plugin_generation_fixture_t *)ctx;
  flow_plugin_generation_owner_t *owner;
  turbo_flow_plugin_product_owner_v1_t descriptor = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_V1_INIT;
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
  memset(owner, 0, sizeof(*owner));
  owner->host = fixture->host;
  owner->fixture = fixture;
  owner->flow = flow;
  owner->ordinal = fixture->materialize_calls;
  descriptor.flags = TURBO_FLOW_PLUGIN_PRODUCT_OWNER_CONTROL_THREAD;
  descriptor.ctx = owner;
  descriptor.quiesce = flow_plugin_generation_owner_quiesce;
  descriptor.drain = flow_plugin_generation_owner_drain;
  descriptor.shutdown = flow_plugin_generation_owner_shutdown;
  descriptor.destroy = flow_plugin_generation_owner_destroy;
#if FLOW_PLUGIN_GENERATION_ENABLE_POLL
  if (flow_plugin_generation_poll_enabled(owner->ordinal)) {
    descriptor.flags |= TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
    descriptor.poll = flow_plugin_generation_owner_poll;
  }
#endif
#if FLOW_PLUGIN_GENERATION_POLL_DESCRIPTOR_MODE == 1
  descriptor.flags |= TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
#elif FLOW_PLUGIN_GENERATION_POLL_DESCRIPTOR_MODE == 2
  descriptor.poll = flow_plugin_generation_owner_poll;
#elif FLOW_PLUGIN_GENERATION_POLL_DESCRIPTOR_MODE == 3
  descriptor.flags |= TURBO_FLOW_PLUGIN_PRODUCT_OWNER_EXTERNAL_POLL;
  descriptor.poll = flow_plugin_generation_owner_poll;
  descriptor.size = offsetof(turbo_flow_plugin_product_owner_v1_t, poll);
#endif
#if FLOW_PLUGIN_GENERATION_LEGACY_OWNER_PREFIX
  descriptor.size = offsetof(turbo_flow_plugin_product_owner_v1_t, poll);
#endif
  return flow_plugin_generation_publish_fixture_owner(owner_out, &descriptor);
}

static int flow_plugin_generation_fixture_load(const turbo_flow_plugin_host_v1_t *host,
                                               void **plugin_out) {
  flow_plugin_generation_fixture_t *fixture;
  if (!plugin_out) return SALTS_EINVAL;
  *plugin_out = NULL;
  if (!host || host->size != sizeof(*host)) return SALTS_EINVAL;
  if (host->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      host->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR) return SALTS_EINVAL;
  if (!host->allocate || !host->deallocate) return SALTS_EINVAL;
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
  if (!plugin || !registration || registration->size != sizeof(*registration) ||
      registration->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      registration->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||
      !registration->add_transactional_adapter_provider ||
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
      !flow_plugin_generation_expected_size(fixture->poll_calls,
                                            FLOW_PLUGIN_GENERATION_EXPECT_POLL_CALLS) ||
      !flow_plugin_generation_expected_size(fixture->blocking_poll_calls,
                                            FLOW_PLUGIN_GENERATION_EXPECT_BLOCKING_POLL_CALLS) ||
      (FLOW_PLUGIN_GENERATION_EXPECT_GRAPH_SHUTDOWN_BEFORE_OWNER_DESTROY &&
       fixture->owner_destroy_before_graph_shutdown != 0u) ||
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
#if FLOW_PLUGIN_GENERATION_EXPECT_GRAPH_SHUTDOWN_BEFORE_OWNER_DESTROY
  for (size_t i = 0u; i < fixture->retained_owner_count; ++i) {
    flow_plugin_generation_owner_t *owner = fixture->retained_owners[i];
    if (!owner) continue;
    memset(owner, 0, sizeof(*owner));
    host->deallocate(host->ctx, owner);
  }
#endif
  memset(fixture, 0, sizeof(*fixture));
  host->deallocate(host->ctx, fixture);
}

static const turbo_flow_plugin_api_v1_t flow_plugin_generation_fixture_api = {
    sizeof(turbo_flow_plugin_api_v1_t),
    TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,
    TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,
    FLOW_PLUGIN_GENERATION_FIXTURE_ID,
    "1.0.0",
    TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_ADAPTER | TURBO_FLOW_PLUGIN_CAP_TRANSACTIONAL_RESOURCE |
        FLOW_PLUGIN_GENERATION_ROOT_POLL_CAPABILITY,
    flow_plugin_generation_fixture_load,
    flow_plugin_generation_fixture_register,
    flow_plugin_generation_fixture_quiesce,
    flow_plugin_generation_fixture_shutdown,
    flow_plugin_generation_fixture_destroy};

TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {
  return &flow_plugin_generation_fixture_api;
}
