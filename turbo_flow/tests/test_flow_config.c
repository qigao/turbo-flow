#include "tinytest.h"
#include "turbo_flow_product.h"
#include "turbo_flow_resolved_config.h"

#include <string.h>
#include <stdlib.h>

#ifdef _WIN32
  #define FLOW_CONFIG_TEST_PLUGIN_ROOT "C:/plugins/"
#else
  #define FLOW_CONFIG_TEST_PLUGIN_ROOT "/opt/plugins/"
#endif

typedef struct flow_product_provider_probe_s {
  int adapter_calls;
  int resource_calls;
  int fail_adapter;
  char order[8];
  size_t order_len;
} flow_product_provider_probe_t;

static void flow_product_probe_record(flow_product_provider_probe_t *probe, char event) {
  if (probe->order_len + 1u < sizeof(probe->order)) {
    probe->order[probe->order_len++] = event;
    probe->order[probe->order_len] = '\0';
  }
}

static int flow_product_test_adapter_provider(void *ctx, turbo_flow_t *flow,
                                              const turbo_flow_resolved_config_t *resolved,
                                              const char *adapter_name,
                                              turbo_flow_config_error_t *error) {
  flow_product_provider_probe_t *probe = (flow_product_provider_probe_t *)ctx;
  (void)resolved;
  (void)error;
  ++probe->adapter_calls;
  flow_product_probe_record(probe, 'A');
  if (probe->fail_adapter) return SALTS_EIO;
  return turbo_flow_register_adapter(flow, adapter_name, NULL, NULL);
}

static int flow_product_test_resource_provider(void *ctx, turbo_flow_t *flow,
                                               const turbo_flow_resolved_config_t *resolved,
                                               const char *resource_name,
                                               turbo_flow_config_error_t *error) {
  flow_product_provider_probe_t *probe = (flow_product_provider_probe_t *)ctx;
  (void)flow;
  (void)resolved;
  (void)resource_name;
  (void)error;
  ++probe->resource_calls;
  flow_product_probe_record(probe, 'R');
  return SALTS_OK;
}

spec("flow_config") {
  it("resolves an ordered explicit plugin DLL manifest") {
    static const char yaml[] =
        "version: 1\n"
        "plugins:\n"
        "  - id: turbo-flow.cnet\n"
        "    version: 1.0.0\n"
        "    path: " FLOW_CONFIG_TEST_PLUGIN_ROOT "turbo_flow_cnet_plugin.dll\n"
        "  - id: turbo-flow.chttp\n"
        "    version: 1.0.0\n"
        "    path: " FLOW_CONFIG_TEST_PLUGIN_ROOT "turbo_flow_chttp_plugin.dll\n"
        "adapters: {}\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_plugin_view_t plugin = TURBO_FLOW_RESOLVED_PLUGIN_VIEW_INIT;
    size_t count = 0u;

    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                SALTS_OK);
    check_equal(turbo_flow_resolved_config_plugin_count(config, &count), SALTS_OK);
    check_equal(count, 2u);
    check_equal(turbo_flow_resolved_config_plugin_at(config, 0u, &plugin), SALTS_OK);
    check_equal(plugin.id, "turbo-flow.cnet");
    check_equal(plugin.version, "1.0.0");
    check_equal(plugin.path, FLOW_CONFIG_TEST_PLUGIN_ROOT "turbo_flow_cnet_plugin.dll");
    plugin = (turbo_flow_resolved_plugin_view_t)TURBO_FLOW_RESOLVED_PLUGIN_VIEW_INIT;
    check_equal(turbo_flow_resolved_config_plugin_at(config, 1u, &plugin), SALTS_OK);
    check_equal(plugin.id, "turbo-flow.chttp");
    check_equal(turbo_flow_resolved_config_plugin_at(config, 2u, &plugin), SALTS_ENOENT);
    {
      turbo_flow_resolved_plugin_view_t invalid = TURBO_FLOW_RESOLVED_PLUGIN_VIEW_INIT;
      invalid.size = sizeof(invalid) - 1u;
      invalid.id = "unchanged";
      check_equal(turbo_flow_resolved_config_plugin_at(config, 0u, &invalid), SALTS_EINVAL);
      check_equal(invalid.id, "unchanged");
    }
    turbo_flow_resolved_config_destroy(config);
  }

  it("rejects ambiguous or incomplete plugin DLL manifests") {
    const char *documents[] = {
        "version: 1\nplugins: [{id: fixture.one, version: 1.0.0}]\nadapters: {}\n",
        "version: 1\nplugins: [{id: fixture.one, version: 1.0.0, path: one.dll, "
        "fallback: two.dll}]\nadapters: {}\n",
        "version: 1\nplugins:\n"
        "  - {id: fixture.one, version: 1.0.0, path: '"
        FLOW_CONFIG_TEST_PLUGIN_ROOT "one.dll'}\n"
        "  - {id: fixture.one, version: 2.0.0, path: '"
        FLOW_CONFIG_TEST_PLUGIN_ROOT "two.dll'}\n"
        "adapters: {}\n",
        "version: 1\nplugins: [{id: fixture.one, version: 1.0.0, path: relative.dll}]\n"
        "adapters: {}\n"};
    const char *paths[] = {"$.plugins[0].path", "$.plugins[0].fallback", "$.plugins[1].id",
                           "$.plugins[0].path"};
    const int statuses[] = {SALTS_EINVAL, SALTS_EINVAL, SALTS_EALREADY, SALTS_EINVAL};

    for (size_t i = 0u; i < sizeof(documents) / sizeof(documents[0]); ++i) {
      turbo_flow_resolved_config_t *config = NULL;
      turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
      check_equal(turbo_flow_config_resolve_yaml(documents[i], strlen(documents[i]), &config,
                                                 &error),
                  statuses[i]);
      check_equal(error.path, paths[i]);
      check_null(config);
    }
  }

  it("resolves process async ingress defaults and explicit bounds") {
    static const char defaults_yaml[] = "version: 1\n";
    static const char explicit_yaml[] = "version: 1\n"
                                        "runtime:\n"
                                        "  ingress:\n"
                                        "    workers: 3\n"
                                        "    capacity: 17\n"
                                        "    max_message_bytes: 4096\n"
                                        "    max_inflight_bytes: 8192\n"
                                        "adapters: {}\n";
    static const char partial_yaml[] = "version: 1\n"
                                       "runtime:\n"
                                       "  ingress:\n"
                                       "    workers: 2\n"
                                       "adapters: {}\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
    const char *json;

    check_equal(
        turbo_flow_config_resolve_yaml(defaults_yaml, sizeof(defaults_yaml) - 1u, &config, &error),
        SALTS_OK);
    check_equal(turbo_flow_resolved_config_runtime_ingress(config, &ingress), SALTS_OK);
    check_equal(ingress.workers, TURBO_FLOW_ASYNC_INGRESS_DEFAULT_WORKERS);
    check_equal(ingress.queue_capacity, TURBO_FLOW_ASYNC_INGRESS_DEFAULT_CAPACITY);
    check_equal(ingress.max_message_bytes,
                TURBO_FLOW_ASYNC_INGRESS_DEFAULT_MAX_MESSAGE_BYTES);
    check_equal(ingress.max_inflight_bytes,
                TURBO_FLOW_ASYNC_INGRESS_DEFAULT_MAX_INFLIGHT_BYTES);
    json = turbo_flow_resolved_config_json(config, NULL);
    check_contains(json,
                   "\"runtime\":{\"ingress\":{\"workers\":1,\"capacity\":1024,"
                   "\"max_message_bytes\":16777216,\"max_inflight_bytes\":67108864}}");
    turbo_flow_resolved_config_destroy(config);

    config = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    ingress = (turbo_flow_async_ingress_config_t)TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
    check_equal(
        turbo_flow_config_resolve_yaml(explicit_yaml, sizeof(explicit_yaml) - 1u, &config, &error),
        SALTS_OK);
    check_equal(turbo_flow_resolved_config_runtime_ingress(config, &ingress), SALTS_OK);
    check_equal(ingress.workers, 3u);
    check_equal(ingress.queue_capacity, 17u);
    check_equal(ingress.max_message_bytes, 4096u);
    check_equal(ingress.max_inflight_bytes, 8192u);
    turbo_flow_resolved_config_destroy(config);

    config = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    ingress = (turbo_flow_async_ingress_config_t)TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
    check_equal(
        turbo_flow_config_resolve_yaml(partial_yaml, sizeof(partial_yaml) - 1u, &config, &error),
        SALTS_OK);
    check_equal(turbo_flow_resolved_config_runtime_ingress(config, &ingress), SALTS_OK);
    check_equal(ingress.workers, 2u);
    check_equal(ingress.queue_capacity, TURBO_FLOW_ASYNC_INGRESS_DEFAULT_CAPACITY);
    check_equal(ingress.max_message_bytes,
                TURBO_FLOW_ASYNC_INGRESS_DEFAULT_MAX_MESSAGE_BYTES);
    check_equal(ingress.max_inflight_bytes,
                TURBO_FLOW_ASYNC_INGRESS_DEFAULT_MAX_INFLIGHT_BYTES);
    turbo_flow_resolved_config_destroy(config);
  }

  it("rejects non-exact ingress output layouts without changing caller storage") {
    typedef struct legacy_async_ingress_config_s {
      size_t size;
      uint32_t workers;
      size_t queue_capacity;
    } legacy_async_ingress_config_t;
    static const char explicit_yaml[] = "version: 1\n"
                                        "runtime:\n"
                                        "  ingress:\n"
                                        "    workers: 3\n"
                                        "    capacity: 17\n"
                                        "    max_message_bytes: 4096\n"
                                        "    max_inflight_bytes: 8192\n"
                                        "adapters: {}\n";
    const size_t invalid_sizes[] = {0u, sizeof(size_t),
                                    offsetof(turbo_flow_async_ingress_config_t,
                                             max_message_bytes),
                                    sizeof(turbo_flow_async_ingress_config_t) - 1u,
                                    sizeof(turbo_flow_async_ingress_config_t) + 1u, SIZE_MAX};
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    unsigned char *short_size = (unsigned char *)malloc(sizeof(size_t));
    legacy_async_ingress_config_t legacy = {sizeof(legacy), 9u, 23u};

    check_not_null(short_size);
    check_equal(
        turbo_flow_config_resolve_yaml(explicit_yaml, sizeof(explicit_yaml) - 1u, &config, &error),
        SALTS_OK);
    for (size_t i = 0u; i < sizeof(invalid_sizes) / sizeof(invalid_sizes[0]); ++i) {
      turbo_flow_async_ingress_config_t invalid;
      turbo_flow_async_ingress_config_t before;
      memset(&invalid, 0xa5, sizeof(invalid));
      invalid.size = invalid_sizes[i];
      before = invalid;
      check_equal(turbo_flow_resolved_config_runtime_ingress(config, &invalid), SALTS_EINVAL);
      check_equal(memcmp(&invalid, &before, sizeof(invalid)), 0);
    }
    memset(short_size, 0xa5, sizeof(size_t));
    *(size_t *)short_size = sizeof(size_t);
    unsigned char short_before[sizeof(size_t)];
    memcpy(short_before, short_size, sizeof(short_before));
    check_equal(turbo_flow_resolved_config_runtime_ingress(
                    config, (turbo_flow_async_ingress_config_t *)short_size),
                SALTS_EINVAL);
    check_equal(memcmp(short_size, short_before, sizeof(short_before)), 0);
    legacy_async_ingress_config_t legacy_before = legacy;
    check_equal(turbo_flow_resolved_config_runtime_ingress(
                    config, (turbo_flow_async_ingress_config_t *)&legacy),
                SALTS_EINVAL);
    check_equal(memcmp(&legacy, &legacy_before, sizeof(legacy)), 0);
    turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
    check_equal(turbo_flow_resolved_config_runtime_ingress(config, &ingress), SALTS_OK);
    check_equal(ingress.workers, 3u);
    check_equal(ingress.queue_capacity, (size_t)17);
    check_equal(ingress.max_message_bytes, (size_t)4096);
    check_equal(ingress.max_inflight_bytes, (size_t)8192);
    free(short_size);
    turbo_flow_resolved_config_destroy(config);
  }

  it("rejects malformed or unbounded process async ingress configuration") {
    static const char unknown[] =
        "version: 1\nruntime:\n  ingress:\n    blocking: true\nadapters: {}\n";
    static const char wrong_type[] =
        "version: 1\nruntime:\n  ingress:\n    workers: many\nadapters: {}\n";
    static const char fractional[] =
        "version: 1\nruntime:\n  ingress:\n    capacity: 1.5\nadapters: {}\n";
    static const char too_many_workers[] =
        "version: 1\nruntime:\n  ingress:\n    workers: 257\nadapters: {}\n";
    static const char inconsistent_bytes[] =
        "version: 1\nruntime:\n  ingress:\n    max_message_bytes: 65\n"
        "    max_inflight_bytes: 64\nadapters: {}\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(turbo_flow_config_resolve_yaml(unknown, sizeof(unknown) - 1u, &config, &error),
                 SALTS_EINVAL);
    check_equal(error.path, "$.runtime.ingress.blocking");
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(
        turbo_flow_config_resolve_yaml(wrong_type, sizeof(wrong_type) - 1u, &config, &error),
        SALTS_EINVAL);
    check_equal(error.path, "$.runtime.ingress.workers");
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(
        turbo_flow_config_resolve_yaml(fractional, sizeof(fractional) - 1u, &config, &error),
        SALTS_EINVAL);
    check_equal(error.path, "$.runtime.ingress.capacity");
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_config_resolve_yaml(too_many_workers, sizeof(too_many_workers) - 1u,
                                                &config, &error),
                 SALTS_ERANGE);
    check_equal(error.path, "$.runtime.ingress.workers");
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_config_resolve_yaml(inconsistent_bytes,
                                                sizeof(inconsistent_bytes) - 1u, &config, &error),
                SALTS_ERANGE);
    check_equal(error.path, "$.runtime.ingress.max_message_bytes");
    check_null(config);
  }

  it("resolves YAML fragments and records each final field source") {
    static const char yaml[] = "version: 1\n"
                               "profiles:\n"
                               "  ingress:\n"
                               "    endpoint: stream.in\n"
                               "    policy: routing\n"
                               "channels:\n"
                               "  routing:\n"
                               "    kind: rule_set\n"
                               "    config:\n"
                               "      mode: first_match\n"
                               "fragments:\n"
                               "  connection:\n"
                               "    local:\n"
                               "      transport: tcp\n"
                               "      host: 127.0.0.1\n"
                               "      port: 7001\n"
                               "adapters:\n"
                               "  stream.in:\n"
                               "    kind: stream\n"
                               "    fragments:\n"
                               "      connection: local\n"
                               "    config:\n"
                               "      pattern: sub\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    const char *json;
    const char *adapter_name = NULL;
    const char *channel_name = NULL;
    size_t len = 0u;
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                 SALTS_OK);
    check_not_null(config);
    json = turbo_flow_resolved_config_json(config, &len);
    check_not_null(json);
    check_equal(strlen(json), len);
    check_contains(json, "\"host\":\"127.0.0.1\"");
    check_contains(json, "\"host\":\"fragment.connection.local\"");
    check_contains(json, "\"pattern\":\"adapter.config\"");
    check_null(strstr(json, "fragments"));
    check_equal(
        turbo_flow_resolved_config_profile_adapter(config, "ingress", "endpoint", &adapter_name),
        SALTS_OK);
    check_equal(adapter_name, "stream.in");
    adapter_name = NULL;
    check_equal(turbo_flow_resolved_config_profile_adapter_optional(config, "ingress", "endpoint",
                                                                     &adapter_name),
                 SALTS_OK);
    check_equal(adapter_name, "stream.in");
    adapter_name = (const char *)1;
    check_equal(turbo_flow_resolved_config_profile_adapter_optional(
                     config, "ingress", "optional_sink", &adapter_name),
                 SALTS_OK);
    check_null(adapter_name);
    check_equal(turbo_flow_resolved_config_profile_adapter_optional(config, "ingress", "policy",
                                                                     &adapter_name),
                 SALTS_EINVAL);
    check_null(adapter_name);
    check_equal(turbo_flow_resolved_config_profile_adapter_optional(config, "missing", "endpoint",
                                                                     &adapter_name),
                 SALTS_ENOENT);
    check_equal(
        turbo_flow_resolved_config_profile_channel(config, "ingress", "policy", &channel_name),
        SALTS_OK);
    check_equal(channel_name, "routing");
    channel_name = NULL;
    check_equal(turbo_flow_resolved_config_profile_channel_optional(config, "ingress", "policy",
                                                                     &channel_name),
                 SALTS_OK);
    check_equal(channel_name, "routing");
    channel_name = (const char *)1;
    check_equal(turbo_flow_resolved_config_profile_channel_optional(
                     config, "ingress", "optional_policy", &channel_name),
                 SALTS_OK);
    check_null(channel_name);
    check_equal(turbo_flow_resolved_config_profile_channel_optional(config, "ingress", "endpoint",
                                                                     &channel_name),
                 SALTS_EINVAL);
    check_null(channel_name);
    check_equal(turbo_flow_resolved_config_profile_channel_optional(config, "missing", "policy",
                                                                     &channel_name),
                 SALTS_ENOENT);
    check_equal(
        turbo_flow_resolved_config_profile_adapter(config, "ingress", "policy", &adapter_name),
        SALTS_ENOENT);
    check_equal(
        turbo_flow_resolved_config_profile_adapter(config, "missing", "endpoint", &adapter_name),
        SALTS_ENOENT);
    turbo_flow_resolved_config_destroy(config);
  }

  it("reads full range 64 bit adapter integers from decimal strings") {
    static const char yaml[] =
        "version: 1\n"
        "adapters:\n"
        "  limits:\n"
        "    kind: test\n"
        "    config:\n"
        "      max_u64: \"18446744073709551615\"\n"
        "      min_i64: \"-9223372036854775808\"\n"
        "      safe_u64: 9007199254740991\n"
        "      invalid_u64: \"12x\"\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
    uint64_t u64 = 0u;
    int64_t i64 = 0;

    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                 SALTS_OK);
    check_equal(turbo_flow_resolved_config_adapter(config, "limits", &view), SALTS_OK);
    check_equal(turbo_flow_resolved_adapter_get_u64(&view, "max_u64", &u64), SALTS_OK);
    check_equal(u64, UINT64_MAX);
    check_equal(turbo_flow_resolved_adapter_get_i64(&view, "min_i64", &i64), SALTS_OK);
    check_true(i64 == INT64_MIN);
    check_equal(turbo_flow_resolved_adapter_get_u64(&view, "safe_u64", &u64), SALTS_OK);
    check_true(u64 == UINT64_C(9007199254740991));
    check_equal(turbo_flow_resolved_adapter_get_u64(&view, "invalid_u64", &u64),
                 SALTS_EINVAL);

    turbo_flow_resolved_config_destroy(config);
  }

  it("fails fast for unknown fields unresolved references and conflicting sources") {
    static const char unknown[] = "version: 1\nadapters: {}\nextra: true\n";
    static const char unresolved[] =
        "version: 1\nprofiles:\n  p:\n    output: missing\nadapters: {}\n";
    static const char conflict[] =
        "version: 1\nfragments:\n  timer:\n    fast:\n      timeout_ms: 10\n"
        "adapters:\n  rpc.client:\n    kind: rpc\n    fragments:\n      timer: fast\n"
        "    config:\n      timeout_ms: 20\n";
    static const char malformed_fragment[] =
        "version: 1\nfragments:\n  connection:\n    unused: bad\nadapters: {}\n";
    static const char ambiguous[] =
        "version: 1\nprofiles:\n  p:\n    output: same\n"
        "channels:\n  same:\n    kind: queue\n    config: {}\n"
        "adapters:\n  same:\n    kind: socket\n    config:\n      role: sink\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_config_resolve_yaml(unknown, sizeof(unknown) - 1u, &config, &error),
                 SALTS_EINVAL);
    check_equal(error.path, "$.extra");
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(
        turbo_flow_config_resolve_yaml(unresolved, sizeof(unresolved) - 1u, &config, &error),
        SALTS_ENOENT);
    check_contains(error.path, "profiles.p.output");
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_config_resolve_yaml(conflict, sizeof(conflict) - 1u, &config, &error),
                 SALTS_EALREADY);
    check_contains(error.path, "timeout_ms");
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_config_resolve_yaml(malformed_fragment, sizeof(malformed_fragment) - 1u,
                                                &config, &error),
                 SALTS_EINVAL);
    check_contains(error.path, "fragments.connection.unused");
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_config_resolve_yaml(ambiguous, sizeof(ambiguous) - 1u, &config, &error),
                 SALTS_EALREADY);
    check_equal(error.path, "$.profiles.p.output");
    check_null(config);
  }

  it("preserves validated channel resources in the immutable snapshot") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  orders:\n"
                               "    kind: queue\n"
                               "    config:\n"
                               "      backend: memory\n"
                               "      pattern: push_pull\n"
                               "adapters: {}\n";
    static const char malformed[] =
        "version: 1\nchannels:\n  orders:\n    kind: queue\n    config: bad\nadapters: {}\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    const char *json;
    size_t len = 0u;
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                 SALTS_OK);
    json = turbo_flow_resolved_config_json(config, &len);
    check_not_null(json);
    check_contains(json, "\"channels\":{\"orders\"");
    check_contains(json, "\"pattern\":\"push_pull\"");
    turbo_flow_resolved_config_destroy(config);

    config = NULL;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_config_resolve_yaml(malformed, sizeof(malformed) - 1u, &config, &error),
                 SALTS_EINVAL);
    check_contains(error.path, "channels.orders");
    check_null(config);
  }

  it("projects typed adapter fields from the immutable resolved snapshot") {
    static const char yaml[] = "version: 1\n"
                               "fragments:\n"
                               "  connection:\n"
                               "    local:\n"
                               "      host: 127.0.0.1\n"
                               "      port: 7001\n"
                               "adapters:\n"
                               "  socket.in:\n"
                               "    kind: socket\n"
                               "    fragments:\n"
                               "      connection: local\n"
                               "    config:\n"
                               "      enabled: true\n"
                               "      offset: -2\n"
                               "      topics: [orders, invoices]\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
    turbo_flow_config_value_type_t type = TURBO_FLOW_CONFIG_NULL;
    const char *value = NULL;
    uint64_t port = 0u;
    int64_t offset = 0;
    int enabled = 0;
    size_t count = 0u;

    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                 SALTS_OK);
    check_equal(turbo_flow_resolved_config_adapter(config, "socket.in", &view), SALTS_OK);
    check_equal(view.name, "socket.in");
    check_equal(view.kind, "socket");
    check_equal(turbo_flow_resolved_adapter_field_count(&view), 5u);
    check_not_null(turbo_flow_resolved_adapter_field_name(&view, 0u));
    check_null(turbo_flow_resolved_adapter_field_name(&view, 5u));
    check_equal(turbo_flow_resolved_adapter_field_type(&view, "topics", &type), SALTS_OK);
    check_equal(type, TURBO_FLOW_CONFIG_ARRAY);
    check_equal(turbo_flow_resolved_adapter_get_string(&view, "host", &value), SALTS_OK);
    check_equal(value, "127.0.0.1");
    check_equal(turbo_flow_resolved_adapter_get_u64(&view, "port", &port), SALTS_OK);
    check_equal(port, 7001u);
    check_equal(turbo_flow_resolved_adapter_get_i64(&view, "offset", &offset), SALTS_OK);
    check_equal(offset, -2);
    check_equal(turbo_flow_resolved_adapter_get_bool(&view, "enabled", &enabled), SALTS_OK);
    check_true(enabled);
    check_equal(turbo_flow_resolved_adapter_array_size(&view, "topics", &count), SALTS_OK);
    check_equal(count, 2u);
    check_equal(turbo_flow_resolved_adapter_array_string_at(&view, "topics", 1u, &value),
                 SALTS_OK);
    check_equal(value, "invoices");
    check_equal(turbo_flow_resolved_adapter_get_string(&view, "missing", &value), SALTS_ENOENT);
    check_equal(turbo_flow_resolved_adapter_get_string(&view, "port", &value), SALTS_EINVAL);
    check_equal(turbo_flow_resolved_adapter_array_string_at(&view, "topics", 2u, &value),
                 SALTS_ENOENT);
    check_equal(turbo_flow_resolved_config_adapter(config, "missing", &view), SALTS_ENOENT);
    turbo_flow_resolved_config_destroy(config);
  }

  it("projects config-driven channel backends without exposing resolver JSON") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  mqtt.auth:\n"
                               "    kind: auth_provider\n"
                               "    config:\n"
                               "      backend: https\n"
                               "      url: https://auth.internal/v4/authenticate\n"
                               "adapters: {}\n";
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_channel_view_t view = TURBO_FLOW_RESOLVED_CHANNEL_VIEW_INIT;
    const char *value = NULL;

    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                 SALTS_OK);
    check_equal(turbo_flow_resolved_config_channel(config, "mqtt.auth", &view), SALTS_OK);
    check_equal(view.name, "mqtt.auth");
    check_equal(view.kind, "auth_provider");
    check_equal(turbo_flow_resolved_channel_get_string(&view, "backend", &value), SALTS_OK);
    check_equal(value, "https");
    check_equal(turbo_flow_resolved_channel_get_string(&view, "missing", &value), SALTS_ENOENT);
    check_equal(turbo_flow_resolved_config_channel(config, "missing", &view), SALTS_ENOENT);
    turbo_flow_resolved_config_destroy(config);
  }

  it("preflights enabled adapter kinds without projecting disabled config") {
    static const char yaml[] = "version: 1\n"
                               "adapters:\n"
                               "  socket.in:\n"
                               "    kind: socket\n"
                               "    config:\n"
                               "      role: source\n"
                               "  rpc.out:\n"
                               "    kind: rpc\n"
                               "    config:\n"
                               "      host_only_object: {opaque: true}\n";
    static const char *const socket_only[] = {"socket"};
    static const char *const both[] = {"socket", "rpc"};
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                 SALTS_OK);
    check_equal(turbo_flow_resolved_config_preflight_adapter_kinds(
                     config, socket_only, sizeof(socket_only) / sizeof(socket_only[0]), &error),
                 SALTS_ENOTSUP);
    check_equal(error.path, "$.adapters.rpc.out.kind");
    check_contains(error.message, "disabled");
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_resolved_config_preflight_adapter_kinds(
                     config, both, sizeof(both) / sizeof(both[0]), &error),
                 SALTS_OK);
    turbo_flow_resolved_config_destroy(config);
  }

  it("assembles each Graph resource and adapter once with resources first") {
    static const char yaml[] = "version: 1\n"
                               "channels:\n"
                               "  routing:\n"
                               "    kind: rule_set\n"
                               "    config: {}\n"
                               "adapters:\n"
                               "  socket.in:\n"
                               "    kind: socket\n"
                               "    config: {role: source}\n"
                               "  socket.out:\n"
                               "    kind: socket\n"
                               "    config: {role: sink}\n";
    static const char graph[] = "source input adapter socket.in\n"
                                "stage decide operation rules.apply resource routing\n"
                                "stage output adapter socket.out\n"
                                "stage audit adapter socket.out\n"
                                "stage main {\n"
                                "  input -> decide -> [output, audit]\n"
                                "}\n";
    flow_product_provider_probe_t probe = {0};
    turbo_flow_product_adapter_provider_t adapter = TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT;
    turbo_flow_product_resource_provider_t resource = TURBO_FLOW_PRODUCT_RESOURCE_PROVIDER_INIT;
    turbo_flow_product_provider_registry_t registry = TURBO_FLOW_PRODUCT_PROVIDER_REGISTRY_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_t *flow = turbo_flow_create();

    adapter.kind = "socket";
    adapter.register_adapter = flow_product_test_adapter_provider;
    adapter.ctx = &probe;
    resource.kind = "rule_set";
    resource.register_resource = flow_product_test_resource_provider;
    resource.ctx = &probe;
    registry.adapter_providers = &adapter;
    registry.adapter_provider_count = 1u;
    registry.resource_providers = &resource;
    registry.resource_provider_count = 1u;

    check_not_null(flow);
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                 SALTS_OK);
    check_equal(turbo_flow_product_preflight(config, &registry, &error), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    {
      int rc = turbo_flow_product_assemble_graph(flow, config, &registry, &error);
      info("assembly status=%d state=%d path=%s message=%s", rc, (int)turbo_flow_state(flow),
           error.path, error.message);
      check_equal(rc, SALTS_OK);
    }
    check_equal(probe.resource_calls, 1);
    check_equal(probe.adapter_calls, 2);
    check_equal(probe.order, "RAA");
    check_equal(turbo_flow_adapter_count(flow), 2u);

    turbo_flow_destroy(flow);
    turbo_flow_resolved_config_destroy(config);
  }

  it("rejects missing and duplicate providers before invoking callbacks") {
    static const char yaml[] = "version: 1\n"
                               "adapters:\n"
                               "  rpc.out:\n"
                               "    kind: rpc\n"
                               "    config: {}\n";
    flow_product_provider_probe_t probe = {0};
    turbo_flow_product_adapter_provider_t providers[2] = {TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT,
                                                          TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT};
    turbo_flow_product_provider_registry_t registry = TURBO_FLOW_PRODUCT_PROVIDER_REGISTRY_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *config = NULL;

    providers[0].kind = "socket";
    providers[0].register_adapter = flow_product_test_adapter_provider;
    providers[0].ctx = &probe;
    registry.adapter_providers = providers;
    registry.adapter_provider_count = 1u;
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                 SALTS_OK);
    check_equal(turbo_flow_product_preflight(config, &registry, &error), SALTS_ENOTSUP);
    check_equal(error.path, "$.adapters.rpc.out.kind");
    check_equal(probe.adapter_calls, 0);

    providers[1] = providers[0];
    registry.adapter_provider_count = 2u;
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_product_preflight(config, &registry, &error), SALTS_EALREADY);
    check_equal(error.path, "$.providers.adapters");
    check_equal(probe.adapter_calls, 0);
    turbo_flow_resolved_config_destroy(config);
  }

  it("reports provider failure at the owning adapter path") {
    static const char yaml[] = "version: 1\n"
                               "adapters:\n"
                               "  socket.out:\n"
                               "    kind: socket\n"
                               "    config: {role: sink}\n";
    static const char graph[] = "source input\n"
                                "stage output adapter socket.out\n"
                                "stage main {\n"
                                "  input -> output\n"
                                "}\n";
    flow_product_provider_probe_t probe = {0};
    turbo_flow_product_adapter_provider_t adapter = TURBO_FLOW_PRODUCT_ADAPTER_PROVIDER_INIT;
    turbo_flow_product_provider_registry_t registry = TURBO_FLOW_PRODUCT_PROVIDER_REGISTRY_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *config = NULL;
    turbo_flow_t *flow = turbo_flow_create();

    probe.fail_adapter = 1;
    adapter.kind = "socket";
    adapter.register_adapter = flow_product_test_adapter_provider;
    adapter.ctx = &probe;
    registry.adapter_providers = &adapter;
    registry.adapter_provider_count = 1u;
    check_not_null(flow);
    check_equal(turbo_flow_config_resolve_yaml(yaml, sizeof(yaml) - 1u, &config, &error),
                 SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    {
      int rc = turbo_flow_product_assemble_graph(flow, config, &registry, &error);
      info("assembly status=%d state=%d path=%s message=%s", rc, (int)turbo_flow_state(flow),
           error.path, error.message);
      check_equal(rc, SALTS_EIO);
    }
    check_equal(error.path, "$.adapters.socket.out");
    check_equal(probe.adapter_calls, 1);

    turbo_flow_destroy(flow);
    turbo_flow_resolved_config_destroy(config);
  }
}
