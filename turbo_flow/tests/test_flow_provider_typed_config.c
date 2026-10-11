#include "flow_provider_config_internal.h"
#include "tinytest.h"
#include "turbo_flow.h"

#include "provider_config_native.h"

size_t flow_provider_config_cpp_policy_size(void);
size_t flow_provider_config_cpp_headers_offset(void);
size_t flow_provider_config_cpp_header_size(void);

#include <string.h>

static const flow_stage_plan_impl_t *find_stage(
    const turbo_flow_t *flow, const char *name) {
  const int index = turbo_flow_find_stage(flow, name);
  return index >= 0
             ? (const flow_stage_plan_impl_t *)vec_at_const(
                   &flow->stages, (size_t)index)
             : NULL;
}

spec("structured provider MessagePlan admission") {
  static turbo_flow_t *flow;
  static flow_provider_typed_config_t config;
  static DataBindMessagePlanDiagnostic diagnostic;

  before_each() {
    flow = turbo_flow_create();
    check_not_null(flow);
    memset(&config, 0, sizeof(config));
    diagnostic = (DataBindMessagePlanDiagnostic)DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
  }

  after_each() {
    const int cleanup_status = flow_provider_typed_config_destroy(&config);
    turbo_flow_destroy(flow);
    flow = NULL;
    check_equal(cleanup_status, SALTS_OK);
  }

  it("keeps generated collection and record layout identical for C and C++") {
    check_equal(flow_provider_config_cpp_policy_size(), sizeof(HeaderPolicy_t));
    check_equal(flow_provider_config_cpp_headers_offset(), offsetof(HeaderPolicy_t, headers));
    check_equal(flow_provider_config_cpp_header_size(), sizeof(StaticHeader_t));
  }

  it("decodes ordered duplicate header names from real source into generated records") {
    static const char src[] =
        "stage request adapter fixture.headers {\n"
        " headers [{ name \"x-trace\", value \"a\" }, { name \"x-trace\", value \"b\" }]\n"
        "}\n";
    flow_provider_typed_config_view_t view = {0};
    const HeaderPolicy_t *policy;
    check_equal(turbo_flow_parse_string(flow, src, sizeof(src) - 1u), SALTS_OK);
    check_equal(flow_provider_typed_config_bind(
                    find_stage(flow, "request"), ProviderConfig_codec_create,
                    HeaderPolicy_native_artifact(), &config, &diagnostic), SALTS_OK);
    check_equal(flow_provider_typed_config_view(&config, &view), SALTS_OK);
    policy = (const HeaderPolicy_t *)view.value;
    check_not_null(policy);
    check_equal(HeaderPolicy_headers_vec_t_size(&policy->headers), (size_t)2u);
    for (size_t i = 0u; i < 2u; ++i) {
      const StaticHeader_t *header = HeaderPolicy_headers_vec_t_at_const(&policy->headers, i);
      check_not_null(header);
      check_equal(header->name, "x-trace");
      check_equal(header->value, i ? "b" : "a");
    }
    /* The typed configuration owns its values after source storage is retired. */
    check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
    check_equal(HeaderPolicy_headers_vec_t_at_const(&policy->headers, 0u)->name, "x-trace");
    check_equal(HeaderPolicy_headers_vec_t_at_const(&policy->headers, 1u)->value, "b");
  }

  it("decodes an empty generated record collection") {
    static const char src[] =
        "stage request adapter fixture.headers {\n headers []\n}\n";
    flow_provider_typed_config_view_t view = {0};
    check_equal(turbo_flow_parse_string(flow, src, sizeof(src) - 1u), SALTS_OK);
    check_equal(flow_provider_typed_config_bind(
                    find_stage(flow, "request"), ProviderConfig_codec_create,
                    HeaderPolicy_native_artifact(), &config, &diagnostic), SALTS_OK);
    check_equal(flow_provider_typed_config_view(&config, &view), SALTS_OK);
    check_equal(HeaderPolicy_headers_vec_t_size(&((const HeaderPolicy_t *)view.value)->headers),
                (size_t)0u);
  }

  it("preserves empty string values for the provider schema to validate") {
    static const char src[] =
        "stage request adapter fixture.headers {\n"
        " headers [{ name \"x-trace\", value \"\" }]\n}\n";
    flow_provider_typed_config_view_t view = {0};
    check_equal(turbo_flow_parse_string(flow, src, sizeof(src) - 1u), SALTS_OK);
    check_equal(flow_provider_typed_config_bind(
                    find_stage(flow, "request"), ProviderConfig_codec_create,
                    HeaderPolicy_native_artifact(), &config, &diagnostic), SALTS_OK);
    check_equal(flow_provider_typed_config_view(&config, &view), SALTS_OK);
    const StaticHeader_t *header = HeaderPolicy_headers_vec_t_at_const(
        &((const HeaderPolicy_t *)view.value)->headers, 0u);
    check_not_null(header);
    check_true(tstr_empty(header->value));
  }

  it("propagates nested record constraints through MessagePlan diagnostics") {
    static const char src[] =
        "stage request adapter fixture.headers {\n"
        " headers [{ name \"name-longer-than-sixteen\", value \"a\" }]\n}\n";
    check_equal(turbo_flow_parse_string(flow, src, sizeof(src) - 1u), SALTS_OK);
    check_equal(flow_provider_typed_config_bind(
                    find_stage(flow, "request"), ProviderConfig_codec_create,
                    HeaderPolicy_native_artifact(), &config, &diagnostic), SALTS_EPROTO);
    check_not_equal(diagnostic.status, DATA_BIND_OK);
    check_true(diagnostic.schema_field[0] != '\0');
    check_true(diagnostic.message[0] != '\0');
  }
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

    const int bind_status = flow_provider_typed_config_bind(
        find_stage(flow, "http"), ProviderConfig_codec_create,
        HttpConfig_native_artifact(), &http, &diagnostic);
    info("typed bind status=%d diagnostic=%d field=%s reason=%s",
        bind_status, diagnostic.status, diagnostic.schema_field, diagnostic.message);
    check_equal(bind_status, SALTS_OK);
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
