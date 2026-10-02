#include "../../../tests/flow_operation_fixture.h"
#include "chttp_provider_config_native.h"
#include "tinytest.h"
#include "turbo_flow_chttp.h"
#include "turbo_flow_chttp_resource.h"
#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resource.h"

#include <salts/plugin.h>

#include <string.h>

#ifndef FLOW_CHTTP_PROVIDER_FIXTURE
#error "FLOW_CHTTP_PROVIDER_FIXTURE is required"
#endif
#ifndef FLOW_CHTTP_RESOURCE_FIXTURE
#error "FLOW_CHTTP_RESOURCE_FIXTURE is required"
#endif

typedef struct resolver_fixture_s {
  salts_plugin_registry *registry;
  salts_plugin_ref provider;
  salts_plugin_ref resource;
  unsigned provider_calls;
  unsigned resource_calls;
} resolver_fixture_t;

typedef struct provider_acceptance_s {
  turbo_flow_t *flow;
  salts_plugin_registry registry;
  int registry_initialized;
  salts_plugin_ref provider_ref;
  salts_plugin_ref resource_ref;
  int provider_loaded;
  int resource_loaded;
  int provider_started;
  int resource_started;
  turbo_flow_provider_binding_t *provider_binding;
  turbo_flow_resource_binding_t *resource_binding;
  turbo_flow_runtime_owner owner;
  int owner_live;
  unsigned typed_kind;
  union {
    CHttpClientConfig_t client;
    CHttpServerConfig_t server;
  } typed;
  resolver_fixture_t resolver;
} provider_acceptance_t;

static provider_acceptance_t acceptance;

enum {
  ACCEPTANCE_CLIENT = 1u,
  ACCEPTANCE_SERVER = 2u
};

static CHttpBackend_t supported_backend(void) {
  if (native_io_backend_kind_supported(NATIVE_IO_BACKEND_IOCP))
    return CHttpBackend_Iocp;
  if (native_io_backend_kind_supported(NATIVE_IO_BACKEND_EPOLL))
    return CHttpBackend_Epoll;
  if (native_io_backend_kind_supported(NATIVE_IO_BACKEND_KQUEUE))
    return CHttpBackend_Kqueue;
  return CHttpBackend_IoUring;
}

#define FILL_NETWORK(CONFIG)                                                       \
  do {                                                                             \
    (CONFIG)->backend = supported_backend();                                       \
    (CONFIG)->network_connection_capacity = 4u;                                    \
    (CONFIG)->network_command_capacity = 8u;                                       \
    (CONFIG)->network_request_capacity = 8u;                                       \
    (CONFIG)->network_completion_batch_capacity = 8u;                              \
    (CONFIG)->network_event_capacity = 8u;                                         \
    (CONFIG)->network_max_send_bytes = 65536u;                                     \
    (CONFIG)->network_receive_buffer_bytes = 16384u;                               \
    (CONFIG)->network_command_buffer_bytes = 131072u;                              \
    (CONFIG)->network_event_buffer_bytes = 32768u;                                 \
    (CONFIG)->network_connect_timeout_ms = 1000u;                                  \
    (CONFIG)->network_read_timeout_ms = 1000u;                                     \
    (CONFIG)->network_write_timeout_ms = 1000u;                                    \
    (CONFIG)->network_tls_handshake_timeout_ms = 0u;                               \
    (CONFIG)->network_tls_io_buffer_bytes = 0u;                                    \
  } while (0)

static void fill_client(CHttpClientConfig_t *config) {
  CHttpClientConfig_init(config);
  config->schema_version = 2u;
  config->poll_budget_ms = 10u;
  config->tls_enabled = 0u;
  config->idempotent = 0u;
  FILL_NETWORK(config);
  config->protocol = CHttpClientProtocol_H1;
  config->request_capacity = 4u;
  config->max_start_line_bytes = 1024u;
  config->max_header_count = 16u;
  config->max_header_bytes = 4096u;
  config->max_request_body_bytes = 8192u;
  config->max_response_body_bytes = 8192u;
  config->max_informational_responses = 4u;
  config->stream_chunk_bytes = 4096u;
  config->h2_input_buffer_bytes = 32768u;
  config->h2_hpack_dynamic_table_bytes = 4096u;
  config->h2_max_settings_count = 16u;
  config->overall_timeout_ms = 5000u;
  config->retry_delay_ms = 10u;
  config->max_attempts = 1u;
  config->stop_timeout_ms = 1000u;
  config->authority = tstr_dup("example.test:80");
  config->target = tstr_dup("/v1/items");
  config->method = CHttpMethod_Get;
}

static void fill_server(CHttpServerConfig_t *config) {
  CHttpServerConfig_init(config);
  config->schema_version = 2u;
  config->poll_budget_ms = 10u;
  config->tls_enabled = 0u;
  FILL_NETWORK(config);
  config->protocol = CHttpServerProtocol_H1;
  config->tls_client_auth = CHttpTlsClientAuth_None;
  config->backlog = 16u;
  config->route_capacity = 8u;
  config->max_target_bytes = 1024u;
  config->max_header_count = 16u;
  config->max_header_bytes = 4096u;
  config->max_request_body_bytes = 8192u;
  config->max_response_header_count = 16u;
  config->max_response_header_bytes = 4096u;
  config->max_response_body_bytes = 16384u;
  config->stream_chunk_bytes = 4096u;
  config->max_buffered_response_body_bytes = 8192u;
  config->buffer_capacity_bytes = 131072u;
  config->middleware_capacity = 0u;
  config->max_route_middleware_count = 0u;
  config->max_route_param_count = 2u;
  config->max_route_param_bytes = 64u;
  config->h2_stream_capacity = 0u;
  config->h2_input_buffer_bytes = 0u;
  config->h2_output_buffer_bytes = 0u;
  config->h2_hpack_dynamic_table_bytes = 0u;
  config->h2_max_settings_count = 0u;
  config->poll_slice_ms = 10u;
  config->path = tstr_dup("/items/:id");
  config->method = CHttpMethod_Post;
  config->max_request_message_bytes = 8192u;
  config->success_status = 200u;
  config->overload_status = 429u;
  config->unavailable_status = 503u;
  config->graph_error_status = 500u;
  config->first_message_id = 1u;
  config->stop_timeout_ms = 1000u;
  config->response_content_type = tstr_dup("application/json");
  config->error_content_type = tstr_dup("application/json");
}

static int output(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  (void)ctx;
  return SALTS_OK;
}

static int resolve_provider(
    void *ctx, const char *provider_identity,
    turbo_flow_provider_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  resolver_fixture_t *fixture = (resolver_fixture_t *)ctx;
  if (!fixture || !out || out->size != sizeof(*out))
    return SALTS_EINVAL;
  ++fixture->provider_calls;
  if (!provider_identity ||
      (strcmp(provider_identity, "chttp.client") != 0 &&
       strcmp(provider_identity, "chttp.server") != 0 &&
       strcmp(provider_identity, "chttp.websocket_server") != 0)) {
    if (error && error->size == sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_ENOENT;
    }
    return SALTS_ENOENT;
  }
  out->module_identity = "turbo-flow.chttp";
  out->registry = fixture->registry;
  out->plugin = fixture->provider;
  return SALTS_OK;
}

static int resolve_resource(
    void *ctx, const char *resource_name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_resource_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  resolver_fixture_t *fixture = (resolver_fixture_t *)ctx;
  const char *expected_contract;
  const char *export_id;
  const char *identity;

  if (!fixture || !requirement || !out || out->size != sizeof(*out))
    return SALTS_EINVAL;
  ++fixture->resource_calls;

  if (resource_name && strcmp(resource_name, "client_net") == 0) {
    expected_contract = TURBO_FLOW_CHTTP_CLIENT_RESOURCE_CONTRACT_ID;
    export_id = "fixture.chttp.client";
    identity = "deployment.chttp.client.primary";
  } else if (resource_name && strcmp(resource_name, "server_net") == 0) {
    expected_contract = TURBO_FLOW_CHTTP_SERVER_RESOURCE_CONTRACT_ID;
    export_id = "fixture.chttp.server";
    identity = "deployment.chttp.server.primary";
  } else {
    expected_contract = NULL;
    export_id = NULL;
    identity = NULL;
  }

  if (!expected_contract || !requirement->contract_id ||
      strcmp(requirement->contract_id, expected_contract) != 0) {
    if (error && error->size == sizeof(*error)) {
      *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
      error->status = SALTS_ENOENT;
    }
    return SALTS_ENOENT;
  }

  out->identity = identity;
  out->registry = fixture->registry;
  out->plugin = fixture->resource;
  out->export_id = export_id;
  return SALTS_OK;
}

static int bind_config_view(
    const DataBindMessageNativeArtifact *artifact,
    const void *value, size_t value_bytes,
    turbo_flow_provider_config_view_v1_t *view) {
  DataBindNativeTypeBinding binding =
      DATA_BIND_NATIVE_TYPE_BINDING_INIT(NULL, NULL);
  DataBindError error = DATA_BIND_ERROR_INIT;
  if (!artifact || !value || !view) return SALTS_EINVAL;
  if (artifact->native_binding(&binding, &error) != DATA_BIND_OK)
    return SALTS_EPROTO;
  *view =
      (turbo_flow_provider_config_view_v1_t)
          TURBO_FLOW_PROVIDER_CONFIG_VIEW_V1_INIT;
  view->type_name = artifact->type_name;
  view->data = binding.data;
  view->value = value;
  view->value_bytes = value_bytes;
  return SALTS_OK;
}

static void acceptance_cleanup(void) {
  if (acceptance.owner_live &&
      turbo_flow_runtime_owner_contract_valid(&acceptance.owner)) {
    (void)turbo_flow_runtime_owner_quiesce(&acceptance.owner, 10u);
    (void)turbo_flow_runtime_owner_drain(&acceptance.owner, 10u);
    (void)turbo_flow_runtime_owner_shutdown(&acceptance.owner);
  }

  if (acceptance.flow) {
    turbo_flow_destroy(acceptance.flow);
    acceptance.flow = NULL;
  }

  if (acceptance.owner_live &&
      turbo_flow_runtime_owner_contract_valid(&acceptance.owner)) {
    turbo_flow_runtime_owner_destroy(&acceptance.owner);
    memset(&acceptance.owner, 0, sizeof(acceptance.owner));
    acceptance.owner_live = 0;
  }

  if (acceptance.resource_binding)
    (void)turbo_flow_resource_binding_release(
        &acceptance.resource_binding);
  if (acceptance.provider_binding)
    (void)turbo_flow_provider_binding_release(
        &acceptance.provider_binding);

  if (acceptance.typed_kind == ACCEPTANCE_CLIENT) {
    CHttpClientConfig_clear(&acceptance.typed.client);
  } else if (acceptance.typed_kind == ACCEPTANCE_SERVER) {
    CHttpServerConfig_clear(&acceptance.typed.server);
  }
  acceptance.typed_kind = 0u;

  if (acceptance.registry_initialized) {
    bool quiescent = false;
    if (acceptance.provider_started) {
      (void)salts_plugin_registry_request_stop(
          &acceptance.registry, acceptance.provider_ref);
      acceptance.provider_started = 0;
    }
    if (acceptance.resource_started) {
      (void)salts_plugin_registry_request_stop(
          &acceptance.registry, acceptance.resource_ref);
      acceptance.resource_started = 0;
    }

    if (acceptance.provider_loaded &&
        salts_plugin_registry_poll_quiescent(
            &acceptance.registry, acceptance.provider_ref,
            &quiescent) == SALTS_PLUGIN_OK &&
        quiescent) {
      if (salts_plugin_registry_unload(
              &acceptance.registry,
              acceptance.provider_ref) == SALTS_PLUGIN_OK)
        acceptance.provider_loaded = 0;
    }

    quiescent = false;
    if (acceptance.resource_loaded &&
        salts_plugin_registry_poll_quiescent(
            &acceptance.registry, acceptance.resource_ref,
            &quiescent) == SALTS_PLUGIN_OK &&
        quiescent) {
      if (salts_plugin_registry_unload(
              &acceptance.registry,
              acceptance.resource_ref) == SALTS_PLUGIN_OK)
        acceptance.resource_loaded = 0;
    }

    if (!acceptance.provider_loaded && !acceptance.resource_loaded) {
      (void)salts_plugin_registry_destroy(&acceptance.registry);
      acceptance.registry_initialized = 0;
    }
  }
}

static int setup_registry(void) {
  salts_plugin_registry_config config = {2u};
  int rc;

  rc = salts_plugin_registry_init(&acceptance.registry, &config);
  if (rc != SALTS_PLUGIN_OK) return rc;
  acceptance.registry_initialized = 1;

  rc = salts_plugin_registry_load(
      &acceptance.registry, FLOW_CHTTP_PROVIDER_FIXTURE,
      &acceptance.provider_ref);
  if (rc != SALTS_PLUGIN_OK) return rc;
  acceptance.provider_loaded = 1;

  rc = salts_plugin_registry_load(
      &acceptance.registry, FLOW_CHTTP_RESOURCE_FIXTURE,
      &acceptance.resource_ref);
  if (rc != SALTS_PLUGIN_OK) return rc;
  acceptance.resource_loaded = 1;

  rc = salts_plugin_registry_start(
      &acceptance.registry, acceptance.provider_ref);
  if (rc != SALTS_PLUGIN_OK) return rc;
  acceptance.provider_started = 1;

  rc = salts_plugin_registry_start(
      &acceptance.registry, acceptance.resource_ref);
  if (rc != SALTS_PLUGIN_OK) return rc;
  acceptance.resource_started = 1;

  acceptance.resolver.registry = &acceptance.registry;
  acceptance.resolver.provider = acceptance.provider_ref;
  acceptance.resolver.resource = acceptance.resource_ref;
  return SALTS_OK;
}

static int acquire_instance_bindings(
    const char *provider_identity, const char *resource_name,
    turbo_flow_provider_contract_v1_t *contract,
    turbo_flow_provider_resource_view_v1_t *resource_view,
    turbo_flow_config_error_t *error) {
  turbo_flow_provider_resolver_v1_t provider_resolver =
      TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
  turbo_flow_resource_resolver_v1_t resource_resolver =
      TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
  int rc;

  provider_resolver.ctx = &acceptance.resolver;
  provider_resolver.resolve = resolve_provider;
  resource_resolver.ctx = &acceptance.resolver;
  resource_resolver.resolve = resolve_resource;

  rc = turbo_flow_provider_binding_acquire(
      &provider_resolver, provider_identity,
      &acceptance.provider_binding, error);
  if (rc != SALTS_OK) return rc;

  rc = turbo_flow_provider_binding_contract(
      acceptance.provider_binding, contract);
  if (rc != SALTS_OK) return rc;

  rc = turbo_flow_resource_binding_acquire(
      &resource_resolver, resource_name, &contract->resource,
      &acceptance.resource_binding, error);
  if (rc != SALTS_OK) return rc;

  return turbo_flow_resource_binding_view(
      acceptance.resource_binding, resource_view);
}

spec("CHTTP canonical Salts provider") {
  before_each() { memset(&acceptance, 0, sizeof(acceptance)); }
  after_each() { acceptance_cleanup(); }

  it("materializes a client stage from typed policy and leased deployment resource") {
    static const char *src =
        "source input\n"
        "stage request adapter chttp.client {\n"
        "  resource client_net\n"
        "}\n"
        "stage output operation test.output\n"
        "stage main {\n"
        "  input -> request -> output\n"
        "}\n";
    turbo_flow_provider_contract_v1_t contract =
        TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
    turbo_flow_provider_resource_view_v1_t resource_view =
        TURBO_FLOW_PROVIDER_RESOURCE_VIEW_V1_INIT;
    turbo_flow_provider_instance_v1_t instance =
        TURBO_FLOW_PROVIDER_INSTANCE_V1_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    flow_test_operation_t operation =
        flow_test_operation_init("test.output", output, NULL);
    int rc;

    check_equal(setup_registry(), SALTS_PLUGIN_OK);
    acceptance.flow = turbo_flow_create();
    check_not_null(acceptance.flow);
    check_equal(
        flow_test_operation_register(acceptance.flow, &operation),
        SALTS_OK);
    check_equal(
        turbo_flow_parse_string(
            acceptance.flow, src, strlen(src)),
        SALTS_OK);

    acceptance.typed_kind = ACCEPTANCE_CLIENT;
    fill_client(&acceptance.typed.client);
    check_not_null(acceptance.typed.client.authority);
    check_not_null(acceptance.typed.client.target);

    check_equal(
        acquire_instance_bindings(
            "chttp.client", "client_net",
            &contract, &resource_view, &error),
        SALTS_OK);
    check_equal(
        contract.config.message_artifact,
        CHttpClientConfig_native_artifact());
    check_equal(
        contract.resource.contract_id,
        TURBO_FLOW_CHTTP_CLIENT_RESOURCE_CONTRACT_ID);
    check_equal(
        contract.resource.required_capabilities,
        (uint64_t)(TURBO_FLOW_CHTTP_RESOURCE_ENDPOINT |
                   TURBO_FLOW_CHTTP_RESOURCE_SHAREABLE_SNAPSHOT));
    check_equal(resource_view.reference_name, "client_net");
    check_equal(
        resource_view.identity,
        "deployment.chttp.client.primary");

    instance.instance_name = "request";
    check_equal(
        bind_config_view(
            CHttpClientConfig_native_artifact(),
            &acceptance.typed.client,
            sizeof(acceptance.typed.client),
            &instance.config),
        SALTS_OK);
    instance.resource = &resource_view;

    check_equal(
        turbo_flow_provider_binding_preflight(
            acceptance.provider_binding, &instance, &error),
        SALTS_OK);
    rc = turbo_flow_provider_binding_materialize(
        acceptance.provider_binding, acceptance.flow,
        &instance, &acceptance.owner, &error);
    info("CHTTP client materialize status=%d path=%s reason=%s",
         rc, error.path, error.message);
    check_equal(rc, SALTS_OK);
    acceptance.owner_live = rc == SALTS_OK;
    check_true(
        turbo_flow_runtime_owner_contract_valid(&acceptance.owner));
    check_equal(turbo_flow_adapter_count(acceptance.flow), (size_t)1u);
    check_equal(turbo_flow_compile(acceptance.flow), SALTS_OK);
  }

  it("materializes one server owner for an exact source and terminal pair") {
    static const char *src =
        "source ingress adapter chttp.server {\n"
        "  resource server_net\n"
        "}\n"
        "stage response adapter chttp.server {\n"
        "  resource server_net\n"
        "}\n"
        "stage main {\n"
        "  ingress -> response\n"
        "}\n";
    turbo_flow_provider_contract_v1_t contract =
        TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
    turbo_flow_provider_resource_view_v1_t resource_view =
        TURBO_FLOW_PROVIDER_RESOURCE_VIEW_V1_INIT;
    turbo_flow_provider_instance_v1_t instance =
        TURBO_FLOW_PROVIDER_INSTANCE_V1_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    int rc;

    check_equal(setup_registry(), SALTS_PLUGIN_OK);
    acceptance.flow = turbo_flow_create();
    check_not_null(acceptance.flow);
    check_equal(
        turbo_flow_parse_string(
            acceptance.flow, src, strlen(src)),
        SALTS_OK);

    acceptance.typed_kind = ACCEPTANCE_SERVER;
    fill_server(&acceptance.typed.server);
    check_not_null(acceptance.typed.server.path);
    check_not_null(acceptance.typed.server.response_content_type);
    check_not_null(acceptance.typed.server.error_content_type);

    check_equal(
        acquire_instance_bindings(
            "chttp.server", "server_net",
            &contract, &resource_view, &error),
        SALTS_OK);
    check_equal(
        contract.config.message_artifact,
        CHttpServerConfig_native_artifact());
    check_equal(
        contract.resource.contract_id,
        TURBO_FLOW_CHTTP_SERVER_RESOURCE_CONTRACT_ID);
    check_equal(
        contract.resource.required_capabilities,
        (uint64_t)(TURBO_FLOW_CHTTP_RESOURCE_ENDPOINT |
                   TURBO_FLOW_CHTTP_RESOURCE_EXCLUSIVE_BIND));
    check_equal(resource_view.reference_name, "server_net");
    check_equal(
        resource_view.identity,
        "deployment.chttp.server.primary");

    instance.instance_name = "response";
    check_equal(
        bind_config_view(
            CHttpServerConfig_native_artifact(),
            &acceptance.typed.server,
            sizeof(acceptance.typed.server),
            &instance.config),
        SALTS_OK);
    instance.resource = &resource_view;

    check_equal(
        turbo_flow_provider_binding_preflight(
            acceptance.provider_binding, &instance, &error),
        SALTS_OK);
    rc = turbo_flow_provider_binding_materialize(
        acceptance.provider_binding, acceptance.flow,
        &instance, &acceptance.owner, &error);
    info("CHTTP server materialize status=%d path=%s reason=%s",
         rc, error.path, error.message);
    check_equal(rc, SALTS_OK);
    acceptance.owner_live = rc == SALTS_OK;
    check_true(
        turbo_flow_runtime_owner_contract_valid(&acceptance.owner));
    check_equal(turbo_flow_adapter_count(acceptance.flow), (size_t)1u);
    check_equal(turbo_flow_managed_boundary_count(acceptance.flow), (size_t)1u);
    check_equal(turbo_flow_compile(acceptance.flow), SALTS_OK);
  }
}

#undef FILL_NETWORK
