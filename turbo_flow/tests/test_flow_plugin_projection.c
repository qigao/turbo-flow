#include "plugin_projection_fixture.h"
#include "component_scope_fixture.h"
#include "salts_resource_fixture.h"
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resource.h"
#include <tinytest.h>
#include <stdlib.h>
#include <string.h>
int flow_plugin_projection_cpp_probe(void);
static const turbo_flow_data_schema_t graph_result_schema = {
  sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
  "cmeta.int.data", "Integer", "int", 91u, 1u, NULL};
static int graph_result_clone(const void *value, void *ctx, void **out) {
  (void)ctx; *out = malloc(sizeof(int));
  if (!*out) return SALTS_ENOMEM;
  *(int *)*out = *(const int *)value; return SALTS_OK;
}
static void graph_result_destroy(void *value, void *ctx) { (void)ctx; free(value); }
static int graph_result_release(void *ctx) { (void)ctx; return SALTS_OK; }

static void lifecycle(void *ctx, turbo_flow_plugin_lifecycle_event_t event,
                      const char *plugin_id, int status) {
  projection_observer_t *observer = (projection_observer_t *)ctx;
  (void)status;
  if (strcmp(plugin_id, "fixture.projection") ||
      (event != TURBO_FLOW_PLUGIN_LIFECYCLE_DESTROY && event != TURBO_FLOW_PLUGIN_LIFECYCLE_UNLOAD)) return;
  cmeta_mutex_lock(&observer->mutex);
  if (observer->count < PROJECTION_TEST_EVENTS) observer->events[observer->count++] = event;
  cmeta_mutex_unlock(&observer->mutex);
}
typedef struct projection_test_s {
  projection_observer_t observer;
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  turbo_flow_plugin_generation_t *cleanup;
  turbo_flow_projection_owner_config_t config;
  void *value;
} projection_test_t;
static int test_open(projection_test_t *test) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_product_provider_registry_t registry = TURBO_FLOW_PRODUCT_PROVIDER_REGISTRY_INIT;
  projection_fixture_protocol_t *protocol;
  int rc;
  memset(test, 0, sizeof(*test));
  cmeta_mutex_init(&test->observer.mutex);
  cmeta_cond_init(&test->observer.cond);
  config.module_capacity = 2u;
  config.lifecycle_observer = lifecycle;
  config.lifecycle_observer_ctx = &test->observer;
  rc = turbo_flow_plugin_host_create(&config, &test->host, &error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_host_load(test->host, FLOW_PROJECTION_FIXTURE, &error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_catalog_snapshot_create(test->host, &test->snapshot, &error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_catalog_snapshot_product_registry(test->snapshot, &registry);
  if (rc != SALTS_OK) return rc;
  if (registry.resource_provider_count != 1u) return SALTS_EPROTO;
  protocol = (projection_fixture_protocol_t *)registry.resource_providers[0].ctx;
  return protocol->create(&test->observer, &test->config, &test->value);
}
static int host_destroy(projection_test_t *test) {
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  if (test->cleanup) {
    turbo_flow_config_error_t cleanup_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    int rc = turbo_flow_plugin_generation_destroy(test->cleanup, 0, &cleanup_error);
    if (rc != SALTS_OK) return rc;
    test->cleanup = NULL;
  }
  return turbo_flow_plugin_host_destroy(test->host, 0u, &error);
}
static void observer_destroy(projection_test_t *test) {
  cmeta_cond_destroy(&test->observer.cond);
  cmeta_mutex_destroy(&test->observer.mutex);
}
static int create_stack_owner(projection_test_t *test, turbo_flow_projection_owner_t **owner) {
  turbo_flow_projection_owner_config_t config = test->config;
  turbo_flow_data_schema_t schema = *config.schema;
  config.schema = &schema;
  return turbo_flow_plugin_projection_owner_create(test->snapshot, &config, owner);
}

typedef struct projection_resources_s {
  cmeta_plugin_registry registry;
  cmeta_plugin_ref provider;
  cmeta_plugin_ref resource;
  component_fixture_t component;
} projection_resources_t;

static int projection_resolve_provider(void *ctx, const char *identity,
    turbo_flow_provider_candidate_v2_t *out, turbo_flow_config_error_t *error) {
  (void)ctx;
  (void)error;
  if (!identity || strcmp(identity, "fixture.provider") != 0) return SALTS_ENOENT;
  out->module_identity = "test.turboflow.provider";
  out->component_identity = "TurboFlowFixtureProvider";
  return SALTS_OK;
}

static int projection_resolve_resource(void *ctx, const char *name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_resource_candidate_v1_t *out, turbo_flow_config_error_t *error) {
  projection_resources_t *resources = ctx;
  (void)error;
  if (!name || strcmp(name, FLOW_TEST_RESOURCE_IDENTITY) != 0 ||
      !requirement || strcmp(requirement->contract_id, FLOW_TEST_RESOURCE_CONTRACT_ID) != 0)
    return SALTS_ENOENT;
  out->identity = "deployment.db_main";
  out->registry = &resources->registry;
  out->plugin = resources->resource;
  out->export_id = "fixture.resource";
  return SALTS_OK;
}

static void projection_resources_open(projection_resources_t *resources) {
  const cmeta_plugin_registry_config config = {2u};
  check_equal(cmeta_plugin_registry_init(&resources->registry, &config), CMETA_PLUGIN_OK);
  check_equal(cmeta_plugin_registry_load(&resources->registry, FLOW_SALTS_PROVIDER_FIXTURE,
                                         &resources->provider), CMETA_PLUGIN_OK);
  check_equal(cmeta_plugin_registry_load(&resources->registry, FLOW_SALTS_RESOURCE_FIXTURE,
                                         &resources->resource), CMETA_PLUGIN_OK);
  check_equal(cmeta_plugin_registry_start(&resources->registry, resources->provider), CMETA_PLUGIN_OK);
  check_equal(cmeta_plugin_registry_start(&resources->registry, resources->resource), CMETA_PLUGIN_OK);
  check_equal(component_fixture_open(&resources->component, &resources->registry,
                                     resources->provider), SALTS_OK);
}

static void projection_resources_close(projection_resources_t *resources) {
  const cmeta_plugin_ref refs[] = {resources->provider, resources->resource};
  check_equal(component_fixture_close(&resources->component), SALTS_OK);
  for (size_t i = 0u; i < sizeof(refs) / sizeof(refs[0]); ++i) {
    bool quiescent = false;
    check_equal(cmeta_plugin_registry_request_stop(&resources->registry, refs[i]), CMETA_PLUGIN_OK);
    check_equal(cmeta_plugin_registry_poll_quiescent(&resources->registry, refs[i], &quiescent),
                CMETA_PLUGIN_OK);
    check_true(quiescent);
    check_equal(cmeta_plugin_registry_unload(&resources->registry, refs[i]), CMETA_PLUGIN_OK);
  }
  check_equal(cmeta_plugin_registry_destroy(&resources->registry), CMETA_PLUGIN_OK);
}
typedef struct projection_worker_s {
  turbo_flow_msg_t *source;
  int destroying;
  int result;
  int observed;
} projection_worker_t;
static void projection_worker(void *ctx) {
  projection_worker_t *worker = (projection_worker_t *)ctx;
  turbo_flow_msg_t clone;
  if (worker->destroying) {
    turbo_flow_msg_cleanup(worker->source);
    worker->result = SALTS_OK;
    return;
  }
  worker->result = turbo_flow_msg_clone(&clone, worker->source);
  if (worker->result == SALTS_OK) {
    worker->observed = *(const int *)turbo_flow_msg_projection(&clone, NULL);
    turbo_flow_msg_cleanup(&clone);
  }
}
spec("PluginHost retained projection DLL leases") {
  it("restores ordinary retain behavior after a real independent result is cleared") {
    turbo_flow_projection_owner_config_t config = TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT;
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_result_claim_t *claim = NULL;
    turbo_flow_msg_t source, view;
    int *result = malloc(sizeof(int));
    *result = 17;
    config.flags = TURBO_FLOW_PROJECTION_IMMUTABLE | TURBO_FLOW_PROJECTION_CROSS_THREAD |
                   TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT;
    config.capacity = 1; config.max_result_bytes = sizeof(int);
    config.max_retained_bytes = sizeof(int); config.schema = &graph_result_schema;
    config.clone = graph_result_clone; config.destroy = graph_result_destroy;
    config.release_context = graph_result_release;
    check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
    turbo_flow_msg_init(&source); turbo_flow_msg_init(&view); view.id = 81;
    check_equal(turbo_flow_msg_result_claim(&source, owner, &cmeta_data_int, &claim), SALTS_OK);
    check_equal(turbo_flow_msg_result_commit(&claim, (void **)&result), SALTS_OK);
    check_equal(turbo_flow_msg_retain_view(&view, &source), SALTS_EINVAL);
    check_equal(view.id, (uint64_t)81);
    turbo_flow_msg_clear_result(&source);
    check_equal(turbo_flow_msg_retain_view(&view, &source), SALTS_OK);
    turbo_flow_msg_cleanup(&view); turbo_flow_msg_cleanup(&source);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
  }
  it("calls the exported factory through its C++ public header") {
    check_equal(flow_plugin_projection_cpp_probe(), SALTS_EINVAL);
  }
  it("keeps clone payload and DLL schema alive after caller snapshot and source release") {
    projection_test_t test;
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_msg_t source, clone;
    const turbo_flow_data_schema_t *schema = NULL;
    check_equal(test_open(&test), SALTS_OK);
    check_equal(create_stack_owner(&test, &owner), SALTS_OK);
    memset(&test.config, 0, sizeof(test.config));
    turbo_flow_msg_init(&source);
    check_equal(turbo_flow_msg_bind_retained_projection(&source, owner, test.value), SALTS_OK);
    check_equal(turbo_flow_msg_clone(&clone, &source), SALTS_OK);
    turbo_flow_plugin_catalog_snapshot_destroy(test.snapshot);
    turbo_flow_msg_cleanup(&source);
    check_equal(host_destroy(&test), SALTS_EBUSY);
    check_equal(*(const int *)turbo_flow_msg_projection(&clone, &schema), PROJECTION_TEST_VALUE);
    check_equal(schema->schema_name, "fixture.projection");
    check_equal(schema->projection_type, "fixture.int");
    turbo_flow_msg_cleanup(&clone);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(host_destroy(&test), SALTS_OK);
    check_equal(test.observer.count, (size_t)5);
    check_equal(test.observer.events[0], PROJECTION_PAYLOAD_DESTROY);
    check_equal(test.observer.events[1], PROJECTION_PAYLOAD_DESTROY);
    check_equal(test.observer.events[2], PROJECTION_CONTEXT_RELEASE);
    check_equal(test.observer.events[3], TURBO_FLOW_PLUGIN_LIFECYCLE_DESTROY);
    check_equal(test.observer.events[4], TURBO_FLOW_PLUGIN_LIFECYCLE_UNLOAD);
    check_equal(test.observer.release_attempts, 1);
    observer_destroy(&test);
  }

  it("balances failed factory leases without consuming the original DLL context") {
    enum { BAD_DESTROY, BAD_RELEASE, BAD_FLAGS, BAD_CAPACITY, BAD_ABI, BAD_SCHEMA, BAD_SIZE, BAD_COUNT };
    for (int mode = BAD_DESTROY; mode < BAD_COUNT; ++mode) {
      projection_test_t test;
      turbo_flow_projection_owner_t *owner = NULL;
      turbo_flow_projection_owner_config_t config;
      check_equal(test_open(&test), SALTS_OK);
      config = test.config;
      switch (mode) {
        case BAD_DESTROY: config.destroy = NULL; break;
        case BAD_RELEASE: config.release_context = NULL; break;
        case BAD_FLAGS: config.flags = 0u; break;
        case BAD_CAPACITY: config.capacity = 0u; break;
        case BAD_ABI: ++config.abi_major; break;
        case BAD_SCHEMA: config.schema = NULL; break;
        case BAD_SIZE: config.size = 0u; break;
      }
      check_equal(turbo_flow_plugin_projection_owner_create(test.snapshot, &config, &owner),
                  mode == BAD_FLAGS ? SALTS_ENOTSUP : SALTS_EINVAL);
      check_null(owner);
      check_equal(test.observer.release_attempts, 0);
      check_equal(*(int *)test.value, PROJECTION_TEST_VALUE);
      test.config.destroy(test.value, test.config.ctx);
      check_equal(test.config.release_context(test.config.ctx), SALTS_OK);
      turbo_flow_plugin_catalog_snapshot_destroy(test.snapshot);
      check_equal(host_destroy(&test), SALTS_OK);
      observer_destroy(&test);
    }
  }

  it("preserves no-clone semantics and pins the DLL across retryable context release") {
    projection_test_t test;
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    turbo_flow_msg_t source, clone;
    check_equal(test_open(&test), SALTS_OK);
    test.config.clone = NULL;
    check_equal(create_stack_owner(&test, &owner), SALTS_OK);
    turbo_flow_msg_init(&source);
    check_equal(turbo_flow_msg_bind_retained_projection(&source, owner, test.value), SALTS_OK);
    check_equal(turbo_flow_msg_clone(&clone, &source), SALTS_ENOTSUP);
    turbo_flow_msg_cleanup(&source);
    turbo_flow_msg_cleanup(&clone);
    turbo_flow_plugin_catalog_snapshot_destroy(test.snapshot);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    cmeta_mutex_lock(&test.observer.mutex);
    test.observer.release_busy = 1;
    cmeta_mutex_unlock(&test.observer.mutex);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_EBUSY);
    check_equal(host_destroy(&test), SALTS_EBUSY);
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)0);
    check_equal(state.retained_bytes, (size_t)0);
    check_equal(state.accepting, 0);
    cmeta_mutex_lock(&test.observer.mutex);
    test.observer.release_busy = 0;
    cmeta_mutex_unlock(&test.observer.mutex);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(host_destroy(&test), SALTS_OK);
    check_equal(test.observer.release_attempts, 2);
    check_equal(test.observer.count, (size_t)4);
    check_equal(test.observer.events[1], PROJECTION_CONTEXT_RELEASE);
    observer_destroy(&test);
  }

  it("moves multiple clones and returns quota exactly once through both clear operations") {
    projection_test_t test;
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    turbo_flow_content_descriptor_t descriptor;
    turbo_flow_msg_t source, first, second, moved, view;
    check_equal(test_open(&test), SALTS_OK);
    check_equal(create_stack_owner(&test, &owner), SALTS_OK);
    turbo_flow_msg_init(&source);
    check_equal(turbo_flow_content_descriptor_init(&descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
        TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_JSON,
        "application/json", "fixture.projection"), SALTS_OK);
    check_equal(turbo_flow_msg_copy_content_descriptor(&source, &descriptor), SALTS_OK);
    check_equal(turbo_flow_msg_bind_retained_projection(&source, owner, test.value), SALTS_OK);
    check_equal(turbo_flow_msg_clone(&first, &source), SALTS_OK);
    check_equal(turbo_flow_msg_clone(&second, &source), SALTS_OK);
    check_equal(turbo_flow_msg_move(&moved, &first), SALTS_OK);
    check_null(turbo_flow_msg_projection(&first, NULL));
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)3);
    check_equal(state.retained_bytes, 3u * sizeof(int));
    turbo_flow_plugin_catalog_snapshot_destroy(test.snapshot);
    turbo_flow_msg_cleanup(&source);
    turbo_flow_msg_clear_projection(&moved);
    check_not_null(turbo_flow_msg_content_descriptor(&moved));
    check_equal(turbo_flow_msg_retain_view(&view, &moved), SALTS_OK);
    turbo_flow_msg_clear_content(&second);
    check_null(turbo_flow_msg_content_descriptor(&second));
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)0);
    check_equal(state.retained_bytes, (size_t)0);
    check_equal(state.peak_outstanding, (size_t)3);
    check_equal(host_destroy(&test), SALTS_EBUSY);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(host_destroy(&test), SALTS_OK);
    check_equal(turbo_flow_msg_content_descriptor(&view)->identity, "fixture.projection");
    turbo_flow_msg_cleanup(&view);
    turbo_flow_msg_cleanup(&moved);
    turbo_flow_msg_cleanup(&first);
    turbo_flow_msg_cleanup(&second);
    check_equal(test.observer.count, (size_t)6);
    check_equal(test.observer.events[3], PROJECTION_CONTEXT_RELEASE);
    observer_destroy(&test);
  }

  it("destroys callback failure temporaries in their DLL before returning reservation quota") {
    projection_test_t test;
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    turbo_flow_msg_t source, clone;
    check_equal(test_open(&test), SALTS_OK);
    check_equal(create_stack_owner(&test, &owner), SALTS_OK);
    turbo_flow_msg_init(&source);
    check_equal(turbo_flow_msg_bind_retained_projection(&source, owner, test.value), SALTS_OK);
    cmeta_mutex_lock(&test.observer.mutex);
    test.observer.clone_error = 1;
    cmeta_mutex_unlock(&test.observer.mutex);
    check_equal(turbo_flow_msg_clone(&clone, &source), SALTS_EIO);
    check_null(turbo_flow_msg_projection(&clone, NULL));
    check_equal(test.observer.count, (size_t)1);
    check_equal(test.observer.events[0], PROJECTION_PAYLOAD_DESTROY);
    check_equal(*(const int *)turbo_flow_msg_projection(&source, NULL), PROJECTION_TEST_VALUE);
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)1);
    check_equal(state.retained_bytes, sizeof(int));
    cmeta_mutex_lock(&test.observer.mutex);
    test.observer.clone_error = 0;
    cmeta_mutex_unlock(&test.observer.mutex);
    check_equal(turbo_flow_msg_clone(&clone, &source), SALTS_OK);
    turbo_flow_msg_cleanup(&clone);
    turbo_flow_msg_cleanup(&source);
    turbo_flow_plugin_catalog_snapshot_destroy(test.snapshot);
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)0);
    check_equal(state.retained_bytes, (size_t)0);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(host_destroy(&test), SALTS_OK);
    observer_destroy(&test);
  }

  it("pins DLL inside clone and destroy barriers until workers return and control destroys owner") {
    for (int destroying = 0; destroying <= 1; ++destroying) {
      projection_test_t test;
      turbo_flow_projection_owner_t *owner = NULL;
      turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
      turbo_flow_msg_t source, rejected;
      cmeta_thread_t thread;
      projection_worker_t worker = {&source, destroying, SALTS_EIO, 0};
      check_equal(test_open(&test), SALTS_OK);
      check_equal(create_stack_owner(&test, &owner), SALTS_OK);
      turbo_flow_msg_init(&source);
      check_equal(turbo_flow_msg_bind_retained_projection(&source, owner, test.value), SALTS_OK);
      cmeta_mutex_lock(&test.observer.mutex);
      test.observer.block_clone = !destroying;
      test.observer.block_destroy = destroying;
      cmeta_mutex_unlock(&test.observer.mutex);
      turbo_flow_plugin_catalog_snapshot_destroy(test.snapshot);
      check_equal(cmeta_thread_create(&thread, projection_worker, &worker), 0);
      cmeta_mutex_lock(&test.observer.mutex);
      while (!test.observer.entered) cmeta_cond_wait(&test.observer.cond, &test.observer.mutex);
      cmeta_mutex_unlock(&test.observer.mutex);
      check_equal(host_destroy(&test), SALTS_EBUSY);
      check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
      check_equal(state.outstanding, destroying ? (size_t)1 : (size_t)2);
      check_equal(state.retained_bytes, (destroying ? 1u : 2u) * sizeof(int));
      check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
      if (!destroying) check_equal(turbo_flow_msg_clone(&rejected, &source), SALTS_ECANCELED);
      cmeta_mutex_lock(&test.observer.mutex);
      test.observer.proceed = 1;
      cmeta_cond_broadcast(&test.observer.cond);
      cmeta_mutex_unlock(&test.observer.mutex);
      check_equal(cmeta_thread_join(&thread), 0);
      check_equal(worker.result, SALTS_OK);
      if (!destroying) {
        check_equal(worker.observed, PROJECTION_TEST_VALUE);
        turbo_flow_msg_cleanup(&source);
        turbo_flow_msg_cleanup(&rejected);
      }
      check_equal(host_destroy(&test), SALTS_EBUSY);
      check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
      check_equal(state.outstanding, (size_t)0);
      check_equal(state.retained_bytes, (size_t)0);
      check_equal(test.observer.release_attempts, 0);
      check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
      check_equal(host_destroy(&test), SALTS_OK);
      check_equal(test.observer.release_attempts, 1);
      observer_destroy(&test);
    }
  }

  it("keeps independent results usable after a canonical provider generation is destroyed") {
    static const char graph[] = "source input\n"
        "stage first adapter fixture.provider resource " FLOW_TEST_RESOURCE_IDENTITY " {\n"
        " batch 7\n concurrency 2\n}\n"
        "stage second adapter fixture.provider resource " FLOW_TEST_RESOURCE_IDENTITY " {\n"
        " batch 7\n concurrency 2\n}\nstage main {\n input -> [first, second]\n}\n";
    projection_resources_t resources = {0};
    turbo_flow_provider_resolver_v2_t provider_resolver = {
        sizeof(provider_resolver), &resources, projection_resolve_provider};
    turbo_flow_resource_resolver_v1_t resource_resolver = {
        sizeof(resource_resolver), &resources, projection_resolve_resource};
    projection_test_t test;
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_plugin_generation_config_t config = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_t *flow;
    turbo_flow_msg_t source, clone;
    const turbo_flow_data_schema_t *schema = NULL;
    check_equal(test_open(&test), SALTS_OK);
    projection_resources_open(&resources);
    config.component_runtime = &resources.component.runtime;
    config.provider_resolver = &provider_resolver;
    config.resource_resolver = &resource_resolver;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    int rc = turbo_flow_plugin_generation_create(test.snapshot, &flow, &config, NULL,
                                                 &generation, &test.cleanup, &error);
    info("generation status=%d path=%s reason=%s", rc, error.path, error.message);
    check_equal(rc, SALTS_OK);
    check_null(flow);
    check_equal(turbo_flow_plugin_generation_owner_count(generation), (size_t)2);
    check_equal(create_stack_owner(&test, &owner), SALTS_OK);
    turbo_flow_msg_init(&source);
    check_equal(turbo_flow_msg_bind_retained_projection(&source, owner, test.value), SALTS_OK);
    turbo_flow_plugin_catalog_snapshot_destroy(test.snapshot);
    check_equal(turbo_flow_plugin_generation_destroy(generation, 1000u, &error), SALTS_OK);
    projection_resources_close(&resources);
    check_equal(host_destroy(&test), SALTS_EBUSY);
    check_equal(turbo_flow_msg_clone(&clone, &source), SALTS_OK);
    turbo_flow_msg_cleanup(&source);
    check_equal(*(const int *)turbo_flow_msg_projection(&clone, &schema), PROJECTION_TEST_VALUE);
    check_equal(schema->schema_name, "fixture.projection");
    turbo_flow_msg_cleanup(&clone);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(host_destroy(&test), SALTS_OK);
    check_equal(test.observer.release_attempts, 1);
    observer_destroy(&test);
  }
}
