#include "chttp_provider_config_native.h"
#include "tinytest.h"
#include "turbo_flow_chttp_resource.h"

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
