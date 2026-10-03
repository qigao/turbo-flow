#include "cnet_provider_config_native.h"
#include "tinytest.h"
#include "turbo_flow_cnet_resource.h"

#include <cnet/cnet.h>
#include <cmeta/data.h>
#include <data_bind_message_plan.h>
#include <data_bind_native_binding.h>

#include <string.h>

static void compile_artifact(
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

spec("CNet canonical typed provider contracts") {
  it("compiles all six provider Messages") {
    DataBind *codec = NULL;
    DataBindError error = DATA_BIND_ERROR_INIT;

    check_equal(
        TurboFlowCNetProviderConfig_codec_create(&codec, &error),
        DATA_BIND_OK);
    check_not_null(codec);
    if (!codec) return;

    compile_artifact(
        codec, "CNetStreamSourceConfig",
        CNetStreamSourceConfig_native_artifact());
    compile_artifact(
        codec, "CNetListenerSourceConfig",
        CNetListenerSourceConfig_native_artifact());
    compile_artifact(
        codec, "CNetPacketSourceConfig",
        CNetPacketSourceConfig_native_artifact());
    compile_artifact(
        codec, "CNetStreamSinkConfig",
        CNetStreamSinkConfig_native_artifact());
    compile_artifact(
        codec, "CNetDatagramSinkConfig",
        CNetDatagramSinkConfig_native_artifact());
    compile_artifact(
        codec, "CNetPacketSinkConfig",
        CNetPacketSinkConfig_native_artifact());

    data_bind_free(codec);
  }

  it("keeps endpoint TLS and PSK deployment facts outside typed policy") {
    const char *schema = TurboFlowCNetProviderConfig_schema_text();
    static const char *forbidden[] = {
        " string uri;",
        "bind_host",
        "bind_port",
        "peer_host",
        "peer_port",
        "peer_scope_id",
        "tls_ca_file",
        "tls_ca_path",
        "tls_cert_file",
        "tls_key_file",
        "tls_key_password",
        "tls_server_name",
        "tls_alpn",
        "psk_hex"};

    check_not_null(schema);
    if (!schema) return;
    for (size_t i = 0u; i < sizeof(forbidden) / sizeof(forbidden[0]); ++i)
      check_null(strstr(schema, forbidden[i]));
  }

  it("publishes one borrowed deployment Interface for all endpoint classes") {
    const char *alpn[] = {"h2", "http/1.1"};
    const uint8_t psk[CNET_KCP_PSK_BYTES] = {1u};
    turbo_flow_cnet_deployment_view_t view =
        TURBO_FLOW_CNET_DEPLOYMENT_VIEW_INIT;
    const cmeta_interface_desc *meta =
        turbo_flow_cnet_deployment_resource_interface();

    check_true(cmeta_interface_desc_valid(meta));
    check_equal(meta->name, "turbo_flow_cnet_deployment_resource");

    view.uri = "tls://127.0.0.1:443";
    view.bind_host = "127.0.0.1";
    view.bind_port = 0u;
    view.peer_host = "::1";
    view.peer_port = 9000u;
    view.tls_server_name = "example.test";
    view.alpn_protocols = alpn;
    view.alpn_protocol_count = 2u;
    view.psk = psk;
    view.psk_size = sizeof(psk);
    check_true(turbo_flow_cnet_deployment_view_valid(&view));

    view.psk = NULL;
    check_false(turbo_flow_cnet_deployment_view_valid(&view));
  }
}
