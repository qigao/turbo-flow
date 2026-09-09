#include "plugin_projection_fixture.h"
#include "turbo_flow_plugin_generation.h"
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
  salts_mutex_lock(&observer->mutex);
  if (observer->count < PROJECTION_TEST_EVENTS) observer->events[observer->count++] = event;
  salts_mutex_unlock(&observer->mutex);
}
typedef struct projection_test_s {
  projection_observer_t observer;
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  turbo_flow_plugin_generation_t *cleanup;
  turbo_flow_projection_owner_config_t config;
  void *value;
} projection_test_t;
static int test_open(projection_test_t *test, int generation) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_product_provider_registry_t registry = TURBO_FLOW_PRODUCT_PROVIDER_REGISTRY_INIT;
  projection_fixture_protocol_t *protocol;
  int rc;
  memset(test, 0, sizeof(*test));
  salts_mutex_init(&test->observer.mutex);
  salts_cond_init(&test->observer.cond);
  config.module_capacity = 2u;
  config.lifecycle_observer = lifecycle;
  config.lifecycle_observer_ctx = &test->observer;
  rc = turbo_flow_plugin_host_create(&config, &test->host, &error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_host_load(test->host, FLOW_PROJECTION_FIXTURE, &error);
  if (rc != SALTS_OK) return rc;
  if (generation) {
    rc = turbo_flow_plugin_host_load(test->host, FLOW_PROJECTION_GENERATION_FIXTURE, &error);
    if (rc != SALTS_OK) return rc;
  }
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
  salts_cond_destroy(&test->observer.cond);
  salts_mutex_destroy(&test->observer.mutex);
}
static int create_stack_owner(projection_test_t *test, turbo_flow_projection_owner_t **owner) {
  turbo_flow_projection_owner_config_t config = test->config;
  turbo_flow_data_schema_t schema = *config.schema;
  config.schema = &schema;
  return turbo_flow_plugin_projection_owner_create(test->snapshot, &config, owner);
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
    check_equal(test_open(&test, 0), SALTS_OK);
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
      check_equal(test_open(&test, 0), SALTS_OK);
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
    check_equal(test_open(&test, 0), SALTS_OK);
    test.config.clone = NULL;
    check_equal(create_stack_owner(&test, &owner), SALTS_OK);
    turbo_flow_msg_init(&source);
    check_equal(turbo_flow_msg_bind_retained_projection(&source, owner, test.value), SALTS_OK);
    check_equal(turbo_flow_msg_clone(&clone, &source), SALTS_ENOTSUP);
    turbo_flow_msg_cleanup(&source);
    turbo_flow_msg_cleanup(&clone);
    turbo_flow_plugin_catalog_snapshot_destroy(test.snapshot);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    salts_mutex_lock(&test.observer.mutex);
    test.observer.release_busy = 1;
    salts_mutex_unlock(&test.observer.mutex);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_EBUSY);
    check_equal(host_destroy(&test), SALTS_EBUSY);
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)0);
    check_equal(state.retained_bytes, (size_t)0);
    check_equal(state.accepting, 0);
    salts_mutex_lock(&test.observer.mutex);
    test.observer.release_busy = 0;
    salts_mutex_unlock(&test.observer.mutex);
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
    check_equal(test_open(&test, 0), SALTS_OK);
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
    check_equal(test_open(&test, 0), SALTS_OK);
    check_equal(create_stack_owner(&test, &owner), SALTS_OK);
    turbo_flow_msg_init(&source);
    check_equal(turbo_flow_msg_bind_retained_projection(&source, owner, test.value), SALTS_OK);
    salts_mutex_lock(&test.observer.mutex);
    test.observer.clone_error = 1;
    salts_mutex_unlock(&test.observer.mutex);
    check_equal(turbo_flow_msg_clone(&clone, &source), SALTS_EIO);
    check_null(turbo_flow_msg_projection(&clone, NULL));
    check_equal(test.observer.count, (size_t)1);
    check_equal(test.observer.events[0], PROJECTION_PAYLOAD_DESTROY);
    check_equal(*(const int *)turbo_flow_msg_projection(&source, NULL), PROJECTION_TEST_VALUE);
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)1);
    check_equal(state.retained_bytes, sizeof(int));
    salts_mutex_lock(&test.observer.mutex);
    test.observer.clone_error = 0;
    salts_mutex_unlock(&test.observer.mutex);
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
      salts_thread_t thread;
      projection_worker_t worker = {&source, destroying, SALTS_EIO, 0};
      check_equal(test_open(&test, 0), SALTS_OK);
      check_equal(create_stack_owner(&test, &owner), SALTS_OK);
      turbo_flow_msg_init(&source);
      check_equal(turbo_flow_msg_bind_retained_projection(&source, owner, test.value), SALTS_OK);
      salts_mutex_lock(&test.observer.mutex);
      test.observer.block_clone = !destroying;
      test.observer.block_destroy = destroying;
      salts_mutex_unlock(&test.observer.mutex);
      turbo_flow_plugin_catalog_snapshot_destroy(test.snapshot);
      check_equal(salts_thread_create(&thread, projection_worker, &worker), 0);
      salts_mutex_lock(&test.observer.mutex);
      while (!test.observer.entered) salts_cond_wait(&test.observer.cond, &test.observer.mutex);
      salts_mutex_unlock(&test.observer.mutex);
      check_equal(host_destroy(&test), SALTS_EBUSY);
      check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
      check_equal(state.outstanding, destroying ? (size_t)1 : (size_t)2);
      check_equal(state.retained_bytes, (destroying ? 1u : 2u) * sizeof(int));
      check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
      if (!destroying) check_equal(turbo_flow_msg_clone(&rejected, &source), SALTS_ECANCELED);
      salts_mutex_lock(&test.observer.mutex);
      test.observer.proceed = 1;
      salts_cond_broadcast(&test.observer.cond);
      salts_mutex_unlock(&test.observer.mutex);
      check_equal(salts_thread_join(&thread), 0);
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

  it("keeps independent results usable after a real transactional generation is destroyed") {
    static const char yaml[] = "version: 1\nadapters:\n"
        "  input.adapter:\n    kind: fixture.transactional.adapter\n    config: {}\n"
        "  output.adapter:\n    kind: fixture.transactional.adapter\n    config: {}\n";
    static const char graph[] = "source input adapter input.adapter\n"
        "stage output adapter output.adapter\nstage main {\n  input -> output\n}\n";
    projection_test_t test;
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_plugin_generation_config_t config = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow;
    turbo_flow_msg_t source, clone;
    const turbo_flow_data_schema_t *schema = NULL;
    check_equal(test_open(&test, 1), SALTS_OK);
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &resolved, &error), SALTS_OK);
    flow = turbo_flow_create();
    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_create(test.snapshot, resolved, &flow, &config, NULL,
                                                    &generation, &test.cleanup, &error),
                SALTS_OK);
    check_null(flow);
    check_equal(turbo_flow_plugin_generation_owner_count(generation), (size_t)2);
    check_equal(create_stack_owner(&test, &owner), SALTS_OK);
    turbo_flow_msg_init(&source);
    check_equal(turbo_flow_msg_bind_retained_projection(&source, owner, test.value), SALTS_OK);
    turbo_flow_plugin_catalog_snapshot_destroy(test.snapshot);
    turbo_flow_resolved_config_destroy(resolved);
    check_equal(turbo_flow_plugin_generation_destroy(generation, 0u, &error), SALTS_OK);
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
