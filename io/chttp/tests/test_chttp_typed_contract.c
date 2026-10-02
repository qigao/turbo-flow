#include "chttp_provider_config_native.h"
#include "tinytest.h"
#include "turbo_flow_chttp_resource.h"
#include "turbo_flow_chttp_typed_config_internal.h"

#include <cmeta/data.h>
#include <cmeta/interface.h>
#include <data_bind_message_plan.h>
#include <data_bind_native_binding.h>

#include <string.h>

static const cmeta_data_field_desc *find_field(
    const cmeta_data_desc *data, const char *name) {
  const cmeta_data_struct_shape *shape;
  size_t i;
  if (!data || data->kind != CMETA_DATA_STRUCT || !data->shape || !name)
    return NULL;
  shape = (const cmeta_data_struct_shape *)data->shape;
  for (i = 0u; i < shape->field_count; ++i)
    if (shape->fields[i].name && strcmp(shape->fields[i].name, name) == 0)
      return &shape->fields[i];
  return NULL;
}

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

#define FILL_SERVER_BASE(CONFIG)                                                   \
  do {                                                                             \
    FILL_NETWORK(CONFIG);                                                          \
    (CONFIG)->schema_version = 2u;                                                 \
    (CONFIG)->poll_budget_ms = 10u;                                                \
    (CONFIG)->tls_enabled = 0u;                                                    \
    (CONFIG)->protocol = CHttpServerProtocol_H1;                                   \
    (CONFIG)->tls_client_auth = CHttpTlsClientAuth_None;                           \
    (CONFIG)->backlog = 16u;                                                       \
    (CONFIG)->route_capacity = 8u;                                                 \
    (CONFIG)->max_target_bytes = 1024u;                                            \
    (CONFIG)->max_header_count = 16u;                                              \
    (CONFIG)->max_header_bytes = 4096u;                                            \
    (CONFIG)->max_request_body_bytes = 8192u;                                      \
    (CONFIG)->max_response_header_count = 16u;                                     \
    (CONFIG)->max_response_header_bytes = 4096u;                                   \
    (CONFIG)->max_response_body_bytes = 16384u;                                    \
    (CONFIG)->stream_chunk_bytes = 4096u;                                          \
    (CONFIG)->max_buffered_response_body_bytes = 8192u;                            \
    (CONFIG)->buffer_capacity_bytes = 131072u;                                     \
    (CONFIG)->middleware_capacity = 0u;                                            \
    (CONFIG)->max_route_middleware_count = 0u;                                     \
    (CONFIG)->max_route_param_count = 2u;                                          \
    (CONFIG)->max_route_param_bytes = 64u;                                         \
    (CONFIG)->h2_stream_capacity = 0u;                                             \
    (CONFIG)->h2_input_buffer_bytes = 0u;                                          \
    (CONFIG)->h2_output_buffer_bytes = 0u;                                         \
    (CONFIG)->h2_hpack_dynamic_table_bytes = 0u;                                   \
    (CONFIG)->h2_max_settings_count = 0u;                                          \
    (CONFIG)->poll_slice_ms = 10u;                                                 \
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
  check_not_null(config->authority);
  check_not_null(config->target);
}

static void fill_server(CHttpServerConfig_t *config) {
  CHttpServerConfig_init(config);
  FILL_SERVER_BASE(config);
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
  check_not_null(config->path);
  check_not_null(config->response_content_type);
  check_not_null(config->error_content_type);
}

static void fill_websocket(CHttpWebSocketServerConfig_t *config) {
  CHttpWebSocketServerConfig_init(config);
  FILL_SERVER_BASE(config);
  config->path = tstr_dup("/ws");
  config->session_capacity = 4u;
  config->frame_capacity = 8u;
  config->max_frame_bytes = 1024u;
  config->max_message_bytes = 4096u;
  config->max_buffered_input_bytes = 2048u;
  config->first_message_id = 1u;
  config->stop_timeout_ms = 1000u;
  check_not_null(config->path);
}

static turbo_flow_chttp_deployment_view_t client_deployment(void) {
  turbo_flow_chttp_deployment_view_t deployment =
      TURBO_FLOW_CHTTP_DEPLOYMENT_VIEW_INIT;
  deployment.kind = TURBO_FLOW_CHTTP_DEPLOYMENT_CLIENT;
  deployment.connection_uri = "tcp://127.0.0.1:8080";
  return deployment;
}

static turbo_flow_chttp_deployment_view_t server_deployment(void) {
  turbo_flow_chttp_deployment_view_t deployment =
      TURBO_FLOW_CHTTP_DEPLOYMENT_VIEW_INIT;
  deployment.kind = TURBO_FLOW_CHTTP_DEPLOYMENT_SERVER;
  deployment.bind_host = "127.0.0.1";
  deployment.bind_port = 8080u;
  return deployment;
}

static void compile_message_artifact(
    DataBind *codec, const char *type_name,
    const DataBindMessageNativeArtifact *artifact) {
  DataBindNativeTypeBinding binding =
      DATA_BIND_NATIVE_TYPE_BINDING_INIT(NULL, NULL);
  DataBindMessagePlan *plan = NULL;
  DataBindMessagePlanDiagnostic diagnostic =
      DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
  DataBindError error = DATA_BIND_ERROR_INIT;

  check_true(data_bind_message_native_artifact_valid(artifact));
  check_equal(artifact->native_binding(&binding, &error), DATA_BIND_OK);
  check_equal(binding.idl_type_name, type_name);
  check_not_null(binding.data);
  check_equal(
      data_bind_message_plan_compile(
          codec, type_name, &binding, &plan, &diagnostic),
      DATA_BIND_OK);
  check_not_null(plan);
  data_bind_message_plan_free(plan);
}

spec("CHTTP canonical typed provider contracts") {
  it("publishes exact deployment resource Interface contracts") {
    const cmeta_interface_desc *meta =
        turbo_flow_chttp_deployment_resource_interface();
    turbo_flow_chttp_deployment_view_t client =
        TURBO_FLOW_CHTTP_DEPLOYMENT_VIEW_INIT;
    turbo_flow_chttp_deployment_view_t server =
        TURBO_FLOW_CHTTP_DEPLOYMENT_VIEW_INIT;
    const char *alpn[] = {"h2", "http/1.1"};

    check_true(cmeta_interface_desc_valid(meta));
    check_equal(meta->name, "turbo_flow_chttp_deployment_resource");

    client.kind = TURBO_FLOW_CHTTP_DEPLOYMENT_CLIENT;
    client.connection_uri = "tls://example.test:443";
    client.alpn_protocols = alpn;
    client.alpn_protocol_count = 1u;
    check_true(turbo_flow_chttp_deployment_view_valid(&client));

    server.kind = TURBO_FLOW_CHTTP_DEPLOYMENT_SERVER;
    server.bind_host = "127.0.0.1";
    server.bind_port = 8443u;
    server.alpn_protocols = alpn;
    server.alpn_protocol_count = 2u;
    check_true(turbo_flow_chttp_deployment_view_valid(&server));

    client.bind_host = "127.0.0.1";
    check_false(turbo_flow_chttp_deployment_view_valid(&client));
  }

  it("compiles all three provider Messages through generated native artifacts") {
    DataBind *codec = NULL;
    DataBindError error = DATA_BIND_ERROR_INIT;

    check_equal(
        TurboFlowCHttpProviderConfig_codec_create(&codec, &error),
        DATA_BIND_OK);
    check_not_null(codec);
    if (!codec) return;

    compile_message_artifact(
        codec, "CHttpClientConfig",
        CHttpClientConfig_native_artifact());
    compile_message_artifact(
        codec, "CHttpServerConfig",
        CHttpServerConfig_native_artifact());
    compile_message_artifact(
        codec, "CHttpWebSocketServerConfig",
        CHttpWebSocketServerConfig_native_artifact());

    data_bind_free(codec);
  }

  it("publishes client headers as an ordered generated sequence") {
    const DataBindMessageNativeArtifact *artifact =
        CHttpClientConfig_native_artifact();
    DataBindNativeTypeBinding binding =
        DATA_BIND_NATIVE_TYPE_BINDING_INIT(NULL, NULL);
    DataBindError error = DATA_BIND_ERROR_INIT;
    const cmeta_data_field_desc *field;
    const cmeta_data_desc *element;

    check_equal(artifact->native_binding(&binding, &error), DATA_BIND_OK);
    field = find_field(binding.data, "headers");
    check_not_null(field);
    if (!field || !field->value) return;

    check_equal(field->value->kind, CMETA_DATA_SEQUENCE);
    check_not_null(cmeta_data_collection_ops_of(field->value));
    check_not_null(cmeta_data_construct_ops_of(field->value));
    element = cmeta_data_collection_element_data(field->value);
    check_not_null(element);
    if (element) {
      check_equal(element->kind, CMETA_DATA_STRUCT);
      check_not_null(element->storage_type);
      check_equal(
          cmeta_type_require_traits(
              element->storage_type,
              CMETA_TRAIT_COPY | CMETA_TRAIT_MOVE | CMETA_TRAIT_DESTROY),
          CMETA_OK);
    }
  }

  it("projects client policy with duplicate ordered headers") {
    CHttpClientConfig_t typed;
    chttp_typed_runtime_config_t runtime;
    turbo_flow_chttp_deployment_view_t deployment = client_deployment();
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    CHttpHeader_t *header;

    fill_client(&typed);
    check_equal(
        CHttpClientConfig_headers_vec_t_resize(&typed.headers, 2u),
        STL_OK);
    header = CHttpClientConfig_headers_vec_t_at(&typed.headers, 0u);
    check_not_null(header);
    if (header) {
      header->name = tstr_dup("x-trace");
      header->value = tstr_dup("a");
    }
    header = CHttpClientConfig_headers_vec_t_at(&typed.headers, 1u);
    check_not_null(header);
    if (header) {
      header->name = tstr_dup("x-trace");
      header->value = tstr_dup("b");
    }

    check_equal(
        chttp_typed_client_config(
            &typed, &deployment, "client_a", &runtime, &error),
        SALTS_OK);
    check_equal(runtime.kind, CHTTP_TYPED_CLIENT);
    check_equal(runtime.client_adapter.header_count, (size_t)2u);
    check_equal(runtime.client_adapter.headers[0].name, "x-trace");
    check_equal(runtime.client_adapter.headers[0].value, "a");
    check_equal(runtime.client_adapter.headers[1].name, "x-trace");
    check_equal(runtime.client_adapter.headers[1].value, "b");
    check_equal(runtime.client_adapter.connection_uri, deployment.connection_uri);
    check_equal(runtime.client_adapter.protocol, CHTTP_HTTP_1_1);
    check_equal(runtime.client_adapter.method, CHTTP_METHOD_GET);

    CHttpClientConfig_clear(&typed);
  }

  it("borrows client TLS material only from the deployment resource") {
    static const char *alpn[] = {"h2"};
    CHttpClientConfig_t typed;
    chttp_typed_runtime_config_t runtime;
    turbo_flow_chttp_deployment_view_t deployment = client_deployment();
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    fill_client(&typed);
    typed.tls_enabled = 1u;
    typed.protocol = CHttpClientProtocol_H2;
    typed.network_tls_handshake_timeout_ms = 1000u;
    typed.network_tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
    tstr_freep(&typed.authority);
    typed.authority = tstr_dup("example.test:443");
    deployment.connection_uri = "tls://example.test:443";
    deployment.tls_ca_file = "/deployment/ca.pem";
    deployment.tls_server_name = "example.test";
    deployment.alpn_protocols = alpn;
    deployment.alpn_protocol_count = 1u;

    check_equal(
        chttp_typed_client_config(
            &typed, &deployment, "client_tls", &runtime, &error),
        SALTS_OK);
    check_true(runtime.tls_enabled);
    check_true(runtime.tls_client.ca_file == deployment.tls_ca_file);
    check_true(
        runtime.tls_client.server_name == deployment.tls_server_name);
    check_true(
        runtime.tls_client.alpn_protocols == deployment.alpn_protocols);
    check_equal(runtime.client_adapter.protocol, CHTTP_HTTP_2);
    check_null(runtime.client_adapter.tls);

    deployment.alpn_protocols = NULL;
    deployment.alpn_protocol_count = 0u;
    check_equal(
        chttp_typed_client_config(
            &typed, &deployment, "client_tls", &runtime, &error),
        SALTS_EINVAL);

    CHttpClientConfig_clear(&typed);
  }

  it("projects server policy from a separate bind resource") {
    CHttpServerConfig_t typed;
    chttp_typed_runtime_config_t runtime;
    turbo_flow_chttp_deployment_view_t deployment = server_deployment();
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    fill_server(&typed);
    check_equal(
        chttp_typed_server_config(
            &typed, &deployment, "server_a", &runtime, &error),
        SALTS_OK);
    check_equal(runtime.kind, CHTTP_TYPED_SERVER);
    check_true(runtime.server.host == deployment.bind_host);
    check_equal(runtime.server.port, deployment.bind_port);
    check_equal(runtime.server_adapter.path, "/items/:id");
    check_equal(runtime.server_adapter.method, CHTTP_METHOD_POST);
    check_equal(runtime.server_adapter.success_status, 200u);
    check_null(runtime.server.tls);

    deployment.kind = TURBO_FLOW_CHTTP_DEPLOYMENT_CLIENT;
    deployment.connection_uri = "tcp://127.0.0.1:8080";
    deployment.bind_host = NULL;
    deployment.bind_port = 0u;
    check_equal(
        chttp_typed_server_config(
            &typed, &deployment, "server_a", &runtime, &error),
        SALTS_EPROTO);

    CHttpServerConfig_clear(&typed);
  }

  it("projects websocket bounds and rejects impossible session capacity") {
    CHttpWebSocketServerConfig_t typed;
    chttp_typed_runtime_config_t runtime;
    turbo_flow_chttp_deployment_view_t deployment = server_deployment();
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    fill_websocket(&typed);
    check_equal(
        chttp_typed_websocket_config(
            &typed, &deployment, "ws_a", &runtime, &error),
        SALTS_OK);
    check_equal(runtime.kind, CHTTP_TYPED_WEBSOCKET);
    check_equal(runtime.websocket_adapter.path, "/ws");
    check_equal(runtime.websocket_adapter.session_capacity, (size_t)4u);

    typed.session_capacity = 5u;
    check_equal(
        chttp_typed_websocket_config(
            &typed, &deployment, "ws_a", &runtime, &error),
        SALTS_ERANGE);

    CHttpWebSocketServerConfig_clear(&typed);
  }

  it("rejects cross-field policy that DataBind cannot decide alone") {
    CHttpClientConfig_t typed;
    chttp_typed_runtime_config_t runtime;
    turbo_flow_chttp_deployment_view_t deployment = client_deployment();
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    CHttpHeader_t *header;

    fill_client(&typed);
    typed.network_command_capacity = 7u;
    check_equal(
        chttp_typed_client_config(
            &typed, &deployment, "client_bad", &runtime, &error),
        SALTS_ERANGE);

    typed.network_command_capacity = 8u;
    check_equal(
        CHttpClientConfig_headers_vec_t_resize(&typed.headers, 1u),
        STL_OK);
    header = CHttpClientConfig_headers_vec_t_at(&typed.headers, 0u);
    check_not_null(header);
    if (header) {
      header->name = tstr_dup("Host");
      header->value = tstr_dup("forbidden.example");
    }
    check_equal(
        chttp_typed_client_config(
            &typed, &deployment, "client_bad", &runtime, &error),
        SALTS_EINVAL);

    CHttpClientConfig_clear(&typed);
  }

  it("keeps deployment endpoint and TLS material out of typed policy") {
    const char *schema = TurboFlowCHttpProviderConfig_schema_text();
    static const char *forbidden[] = {
        "connection_uri",
        "bind_host",
        "bind_port",
        "tls_ca_file",
        "tls_ca_path",
        "tls_cert_file",
        "tls_key_file",
        "tls_key_password",
        "tls_server_name",
        "tls_alpn"};

    check_not_null(schema);
    if (!schema) return;
    for (size_t i = 0u; i < sizeof(forbidden) / sizeof(forbidden[0]); ++i)
      check_null(strstr(schema, forbidden[i]));
  }
}


#undef FILL_SERVER_BASE
#undef FILL_NETWORK
