#include "flow_provider_config_internal.h"
#include "tinytest.h"
#include "turbo_flow.h"

#include "provider_config_native.h"

#include <string.h>

static const flow_stage_plan_impl_t *find_stage(
    const turbo_flow_t *flow, const char *name) {
  const int index = turbo_flow_find_stage(flow, name);
  return index >= 0
             ? (const flow_stage_plan_impl_t *)vec_at_const(
                   &flow->stages, (size_t)index)
             : NULL;
}

spec("flow typed provider config") {
  it("binds unrelated provider configs through one DataBind MessagePlan path") {
    static const char *src =
        "stage http adapter fixture.http {\n"
        "  port 443\n"
        "  tls true\n"
        "  mode \"client\"\n"
        "}\n"
        "stage batch adapter fixture.batch {\n"
        "  batch 32\n"
        "  concurrency 4\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    flow_provider_typed_config_t http = {0};
    flow_provider_typed_config_t batch = {0};
    flow_provider_typed_config_view_t view = {0};
    DataBindMessagePlanDiagnostic diagnostic =
        DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
    const HttpConfig_t *http_value;
    const BatchConfig_t *batch_value;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);

    check_equal(
        flow_provider_typed_config_bind(
            find_stage(flow, "http"), ProviderConfig_codec_create,
            HttpConfig_native_artifact(), &http, &diagnostic),
        SALTS_OK);
    check_equal(flow_provider_typed_config_view(&http, &view), SALTS_OK);
    check_equal(view.type_name, "HttpConfig");
    check_true(view.data == http.native.data);
    check_equal(view.value_bytes, sizeof(HttpConfig_t));
    http_value = (const HttpConfig_t *)view.value;
    check_not_null(http_value);
    check_equal(http_value->port, 443u);
    check_true(http_value->tls);
    check_not_null(http_value->mode);
    check_equal(tstr_len(http_value->mode), (size_t)6u);
    check_true(memcmp(http_value->mode, "client", 6u) == 0);
    check_not_null(http_value->label);
    check_equal(tstr_len(http_value->label), (size_t)7u);
    check_true(memcmp(http_value->label, "default", 7u) == 0);

    diagnostic = (DataBindMessagePlanDiagnostic)
        DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
    check_equal(
        flow_provider_typed_config_bind(
            find_stage(flow, "batch"), ProviderConfig_codec_create,
            BatchConfig_native_artifact(), &batch, &diagnostic),
        SALTS_OK);
    check_equal(flow_provider_typed_config_view(&batch, &view), SALTS_OK);
    check_equal(view.type_name, "BatchConfig");
    check_equal(view.value_bytes, sizeof(BatchConfig_t));
    batch_value = (const BatchConfig_t *)view.value;
    check_not_null(batch_value);
    check_equal(batch_value->batch, 32u);
    check_equal(batch_value->concurrency, 4u);

    check_equal(flow_provider_typed_config_destroy(&batch), SALTS_OK);
    check_equal(flow_provider_typed_config_destroy(&http), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("fails canonical validation before provider materialization") {
    static const char *src =
        "stage http adapter fixture.http {\n"
        "  port 0\n"
        "  tls true\n"
        "  mode \"CLIENT\"\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    flow_provider_typed_config_t config = {0};
    DataBindMessagePlanDiagnostic diagnostic =
        DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(
        flow_provider_typed_config_bind(
            find_stage(flow, "http"), ProviderConfig_codec_create,
            HttpConfig_native_artifact(), &config, &diagnostic),
        SALTS_EPROTO);
    check_true(diagnostic.status != DATA_BIND_OK);
    check_true(diagnostic.schema_field[0] != '\0');
    check_equal(flow_provider_typed_config_destroy(&config), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects unknown provider fields through the canonical Message contract") {
    static const char *src =
        "stage batch adapter fixture.batch {\n"
        "  batch 32\n"
        "  concurrency 4\n"
        "  unexpected 1\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    flow_provider_typed_config_t config = {0};
    DataBindMessagePlanDiagnostic diagnostic =
        DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(
        flow_provider_typed_config_bind(
            find_stage(flow, "batch"), ProviderConfig_codec_create,
            BatchConfig_native_artifact(), &config, &diagnostic),
        SALTS_EPROTO);
    check_true(diagnostic.status != DATA_BIND_OK);
    check_equal(flow_provider_typed_config_destroy(&config), SALTS_OK);
    turbo_flow_destroy(flow);
  }
}
