#include "flow_internal.h"
#include "flow_provider_config_internal.h"
#include "tinytest.h"
#include "turbo_flow.h"

#include <cserde/reader.h>

#include <string.h>

static void check_slice(const cserde_slice *slice, const char *text) {
  check_not_null(slice);
  check_not_null(text);
  check_equal(slice->size, strlen(text));
  check_true(slice->data != NULL);
  check_true(memcmp(slice->data, text, slice->size) == 0);
  check_equal(slice->lifetime, CSERDE_VIEW_STABLE);
}

static void expect_token(cserde_reader *reader, cserde_token_kind kind,
                         const char *text, uint64_t number, int boolean) {
  cserde_token token = {0};
  check_equal(cserde_reader_next(reader, &token), CSERDE_OK);
  check_equal(token.kind, kind);
  if (kind == CSERDE_STRING) check_slice(&token.value.slice, text);
  if (kind == CSERDE_UINT) check_equal(token.value.uint, number);
  if (kind == CSERDE_BOOL) check_equal((int)token.value.boolean, boolean);
}

spec("flow provider config literals") {
  it("emits node provider literals as one bounded CSerde map") {
    static const char *src =
        "source input adapter cnet.stream {\n"
        "  endpoint \"127.0.0.1\"\n"
        "  port 9000\n"
        "  tls true\n"
        "  mode client\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    const flow_stage_plan_impl_t *stage;
    flow_provider_config_reader_t config_reader;
    cserde_token token = {0};

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    stage = (const flow_stage_plan_impl_t *)vec_at_const(
        &flow->stages, (size_t)turbo_flow_find_stage(flow, "input"));
    check_not_null(stage);
    check_equal(vec_size(&stage->provider_config_literals), (size_t)4u);
    check_equal(flow_provider_config_reader_init(&config_reader, stage), SALTS_OK);

    expect_token(&config_reader.reader, CSERDE_MAP_BEGIN, NULL, 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "endpoint", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "127.0.0.1", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "port", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_UINT, NULL, 9000u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "tls", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_BOOL, NULL, 0u, 1);
    expect_token(&config_reader.reader, CSERDE_STRING, "mode", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "client", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_MAP_END, NULL, 0u, 0);
    check_equal(cserde_reader_next(&config_reader.reader, &token), CSERDE_DONE);

    turbo_flow_destroy(flow);
  }

  it("copies provider literals exactly through reusable use expansion") {
    static const char *src =
        "stage cleanse {\n"
        "  in raw\n"
        "  out clean\n"
        "  step trim operation Text.trim {\n"
        "    locale \"en-US\"\n"
        "    strict true\n"
        "  }\n"
        "  raw -> trim -> clean\n"
        "}\n"
        "stage main {\n"
        "  source input\n"
        "  use c = cleanse\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    const flow_stage_plan_impl_t *declared;
    const flow_stage_plan_impl_t *instance;
    const flow_provider_config_literal_t *literal;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);

    declared = (const flow_stage_plan_impl_t *)vec_at_const(
        &flow->stages, (size_t)turbo_flow_find_stage(flow, "cleanse.trim"));
    instance = (const flow_stage_plan_impl_t *)vec_at_const(
        &flow->stages, (size_t)turbo_flow_find_stage(flow, "c.trim"));
    check_not_null(declared);
    check_not_null(instance);
    check_equal(vec_size(&declared->provider_config_literals), (size_t)2u);
    check_equal(vec_size(&instance->provider_config_literals), (size_t)2u);

    literal = (const flow_provider_config_literal_t *)vec_at_const(
        &instance->provider_config_literals, 0u);
    check_not_null(literal);
    check_equal(literal->name, "locale");
    check_equal(literal->text, "en-US");

    literal = (const flow_provider_config_literal_t *)vec_at_const(
        &instance->provider_config_literals, 1u);
    check_not_null(literal);
    check_equal(literal->name, "strict");
    check_equal(literal->kind, FLOW_PROVIDER_CONFIG_LITERAL_BOOL);
    check_equal(literal->bool_value, 1);

    turbo_flow_destroy(flow);
  }

  it("emits ordered duplicate-preserving array-of-record tokens") {
    static const char *src =
        "stage request adapter chttp.client {\n"
        "  headers [\n"
        "    { name \"x-trace\", value \"a\" },\n"
        "    { name \"x-trace\", value \"b\" }\n"
        "  ]\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    const flow_stage_plan_impl_t *stage;
    const flow_provider_config_literal_t *literal;
    flow_provider_config_reader_t config_reader;
    cserde_token token = {0};

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    stage = (const flow_stage_plan_impl_t *)vec_at_const(
        &flow->stages, (size_t)turbo_flow_find_stage(flow, "request"));
    check_not_null(stage);
    check_equal(vec_size(&stage->provider_config_literals), (size_t)1u);
    literal = (const flow_provider_config_literal_t *)vec_at_const(
        &stage->provider_config_literals, 0u);
    check_not_null(literal);
    check_equal(literal->name, "headers");
    check_equal(literal->kind, FLOW_PROVIDER_CONFIG_LITERAL_STRUCTURED);
    check_not_null(literal->text);

    check_equal(flow_provider_config_reader_init(&config_reader, stage), SALTS_OK);
    expect_token(&config_reader.reader, CSERDE_MAP_BEGIN, NULL, 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "headers", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_ARRAY_BEGIN, NULL, 0u, 0);

    expect_token(&config_reader.reader, CSERDE_MAP_BEGIN, NULL, 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "name", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "x-trace", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "value", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "a", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_MAP_END, NULL, 0u, 0);

    expect_token(&config_reader.reader, CSERDE_MAP_BEGIN, NULL, 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "name", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "x-trace", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "value", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "b", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_MAP_END, NULL, 0u, 0);

    expect_token(&config_reader.reader, CSERDE_ARRAY_END, NULL, 0u, 0);
    expect_token(&config_reader.reader, CSERDE_MAP_END, NULL, 0u, 0);
    check_equal(cserde_reader_next(&config_reader.reader, &token), CSERDE_DONE);
    turbo_flow_destroy(flow);
  }

  it("emits empty structured arrays without a provider-specific fallback") {
    static const char *src =
        "stage request adapter chttp.client {\n"
        "  headers []\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    const flow_stage_plan_impl_t *stage;
    flow_provider_config_reader_t config_reader;
    cserde_token token = {0};

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    stage = (const flow_stage_plan_impl_t *)vec_at_const(
        &flow->stages, (size_t)turbo_flow_find_stage(flow, "request"));
    check_not_null(stage);
    check_equal(flow_provider_config_reader_init(&config_reader, stage), SALTS_OK);
    expect_token(&config_reader.reader, CSERDE_MAP_BEGIN, NULL, 0u, 0);
    expect_token(&config_reader.reader, CSERDE_STRING, "headers", 0u, 0);
    expect_token(&config_reader.reader, CSERDE_ARRAY_BEGIN, NULL, 0u, 0);
    expect_token(&config_reader.reader, CSERDE_ARRAY_END, NULL, 0u, 0);
    expect_token(&config_reader.reader, CSERDE_MAP_END, NULL, 0u, 0);
    check_equal(cserde_reader_next(&config_reader.reader, &token), CSERDE_DONE);
    turbo_flow_destroy(flow);
  }

  it("copies structured provider literals exactly through reusable use expansion") {
    static const char *src =
        "stage template {\n"
        "  in raw\n"
        "  out clean\n"
        "  step request operation Http.send {\n"
        "    headers [{ name \"x-trace\", value \"a\" }]\n"
        "  }\n"
        "  raw -> request -> clean\n"
        "}\n"
        "stage main {\n"
        "  source input\n"
        "  use copy = template\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    const flow_stage_plan_impl_t *declared;
    const flow_stage_plan_impl_t *instance;
    const flow_provider_config_literal_t *left;
    const flow_provider_config_literal_t *right;

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    declared = (const flow_stage_plan_impl_t *)vec_at_const(
        &flow->stages, (size_t)turbo_flow_find_stage(flow, "template.request"));
    instance = (const flow_stage_plan_impl_t *)vec_at_const(
        &flow->stages, (size_t)turbo_flow_find_stage(flow, "copy.request"));
    check_not_null(declared);
    check_not_null(instance);
    left = (const flow_provider_config_literal_t *)vec_at_const(
        &declared->provider_config_literals, 0u);
    right = (const flow_provider_config_literal_t *)vec_at_const(
        &instance->provider_config_literals, 0u);
    check_not_null(left);
    check_not_null(right);
    check_equal(left->kind, FLOW_PROVIDER_CONFIG_LITERAL_STRUCTURED);
    check_equal(right->kind, FLOW_PROVIDER_CONFIG_LITERAL_STRUCTURED);
    check_equal(left->text, right->text);
    check_true(left->text != right->text);
    turbo_flow_destroy(flow);
  }

  it("enforces structured literal depth before typed provider execution") {
    char src[4096];
    size_t used;
    turbo_flow_t *flow = turbo_flow_create();
    const flow_stage_plan_impl_t *stage;
    flow_provider_config_reader_t config_reader;
    cserde_token token = {0};
    cserde_status status = CSERDE_OK;

    used = (size_t)snprintf(
        src, sizeof(src), "stage deep adapter fixture.deep {\n  value ");
    for (size_t i = 0u;
         i < FLOW_PROVIDER_CONFIG_MAX_STRUCTURED_DEPTH + 1u &&
         used + 2u < sizeof(src);
         ++i)
      src[used++] = '[';
    src[used++] = '1';
    for (size_t i = 0u;
         i < FLOW_PROVIDER_CONFIG_MAX_STRUCTURED_DEPTH + 1u &&
         used + 2u < sizeof(src);
         ++i)
      src[used++] = ']';
    memcpy(src + used, "\n}\n", 4u);
    used += 4u;
    src[used] = '\0';

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, used), SALTS_OK);
    stage = (const flow_stage_plan_impl_t *)vec_at_const(
        &flow->stages, (size_t)turbo_flow_find_stage(flow, "deep"));
    check_not_null(stage);
    check_equal(flow_provider_config_reader_init(&config_reader, stage), SALTS_OK);
    while ((status = cserde_reader_next(&config_reader.reader, &token)) == CSERDE_OK) {
    }
    check_equal(status, CSERDE_LIMIT_EXCEEDED);
    turbo_flow_destroy(flow);
  }

  it("keeps invalid legacy resource identifiers rejected by the lexer") {
    static const char *src =
        "stage request adapter fixture.http resource db-main\n";
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_EINVAL);
    check_contains(turbo_flow_last_error(flow)->message, "unexpected character");
    turbo_flow_destroy(flow);
  }

  it("rejects duplicate provider fields before provider lookup") {
    static const char *src =
        "stage classify operation Vehicle.classify {\n"
        "  threshold 10\n"
        "  threshold 20\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_EALREADY);
    check_equal(turbo_flow_last_error(flow)->line, 3u);
    check_contains(turbo_flow_last_error(flow)->message, "duplicate provider config field");

    turbo_flow_destroy(flow);
  }
}
