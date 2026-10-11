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

static void append_source(tstr *source, const char *suffix) {
  tstr next = tstr_cat(*source, suffix);
  check_not_null(next);
  *source = next;
}

spec("structured provider source events") {
  static turbo_flow_t *flow;
  static flow_provider_config_reader_t reader;
  static const flow_stage_plan_impl_t *stage;
  static tstr source_text;
  static tstr repeated;

  before_each() {
    flow = turbo_flow_create();
    check_not_null(flow);
    stage = NULL;
    source_text = NULL;
    repeated = NULL;
  }

  after_each() {
    turbo_flow_destroy(flow);
    flow = NULL;
    tstr_freep(&source_text);
    tstr_freep(&repeated);
  }

  it("preserves ordered records and duplicate values as canonical CSerde events") {
    static const char src[] =
        "stage request adapter fixture.http {\n"
        "  headers [{ name \"x-trace\", value \"a\" }, { name \"x-trace\", value \"b\" }]\n"
        "  flags [true, 7, [false], {}]\n"
        "}\n";
    cserde_token token = {0};
    check_equal(turbo_flow_parse_string(flow, src, sizeof(src) - 1u), SALTS_OK);
    stage = (const flow_stage_plan_impl_t *)vec_at_const(
        &flow->stages, (size_t)turbo_flow_find_stage(flow, "request"));
    check_equal(flow_provider_config_reader_init(&reader, stage), SALTS_OK);
    expect_token(&reader.reader, CSERDE_MAP_BEGIN, NULL, 0u, 0);
    expect_token(&reader.reader, CSERDE_STRING, "headers", 0u, 0);
    expect_token(&reader.reader, CSERDE_ARRAY_BEGIN, NULL, 0u, 0);
    for (int i = 0; i < 2; ++i) {
      expect_token(&reader.reader, CSERDE_MAP_BEGIN, NULL, 0u, 0);
      expect_token(&reader.reader, CSERDE_STRING, "name", 0u, 0);
      expect_token(&reader.reader, CSERDE_STRING, "x-trace", 0u, 0);
      expect_token(&reader.reader, CSERDE_STRING, "value", 0u, 0);
      expect_token(&reader.reader, CSERDE_STRING, i ? "b" : "a", 0u, 0);
      expect_token(&reader.reader, CSERDE_MAP_END, NULL, 0u, 0);
    }
    expect_token(&reader.reader, CSERDE_ARRAY_END, NULL, 0u, 0);
    expect_token(&reader.reader, CSERDE_STRING, "flags", 0u, 0);
    expect_token(&reader.reader, CSERDE_ARRAY_BEGIN, NULL, 0u, 0);
    expect_token(&reader.reader, CSERDE_BOOL, NULL, 0u, 1);
    expect_token(&reader.reader, CSERDE_UINT, NULL, 7u, 0);
    expect_token(&reader.reader, CSERDE_ARRAY_BEGIN, NULL, 0u, 0);
    expect_token(&reader.reader, CSERDE_BOOL, NULL, 0u, 0);
    expect_token(&reader.reader, CSERDE_ARRAY_END, NULL, 0u, 0);
    expect_token(&reader.reader, CSERDE_MAP_BEGIN, NULL, 0u, 0);
    expect_token(&reader.reader, CSERDE_MAP_END, NULL, 0u, 0);
    expect_token(&reader.reader, CSERDE_ARRAY_END, NULL, 0u, 0);
    expect_token(&reader.reader, CSERDE_MAP_END, NULL, 0u, 0);
    check_equal(cserde_reader_next(&reader.reader, &token), CSERDE_DONE);
    check_equal(cserde_reader_next(&reader.reader, &token), CSERDE_DONE);
  }

  it("accepts the exact source token bound and rejects the next item") {
    /* Outer map, key and array delimiters consume five tokens. */
    for (size_t extra = 0u; extra < 2u; ++extra) {
      const size_t items = FLOW_PROVIDER_CONFIG_MAX_SOURCE_TOKENS - 5u + extra;
      tstr_freep(&source_text);
      tstr_freep(&repeated);
      source_text = tstr_dup("stage s adapter fixture.test {\n v [");
      repeated = tstr_repeat("0,", items - 1u);
      check_not_null(source_text);
      check_not_null(repeated);
      append_source(&source_text, repeated);
      append_source(&source_text, "0]\n}\n");
      check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, source_text, tstr_len(source_text)),
                  extra ? SALTS_ENOSPC : SALTS_OK);
      if (!extra) {
        cserde_token token = {0};
        size_t tokens = 0u;
        stage = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, 0u);
        check_equal(flow_provider_config_reader_init(&reader, stage), SALTS_OK);
        while (cserde_reader_next(&reader.reader, &token) == CSERDE_OK) ++tokens;
        check_equal(tokens, (size_t)FLOW_PROVIDER_CONFIG_MAX_SOURCE_TOKENS);
      } else {
        check_contains(turbo_flow_last_error(flow)->message, "capacity");
      }
    }
  }

  it("bounds container depth including the reader's outer map") {
    for (size_t extra = 0u; extra < 2u; ++extra) {
      const size_t depth = FLOW_PROVIDER_CONFIG_MAX_SOURCE_DEPTH - 1u + extra;
      tstr_freep(&source_text);
      tstr_freep(&repeated);
      source_text = tstr_dup("stage s adapter fixture.test {\n v ");
      repeated = tstr_repeat("[", depth);
      check_not_null(source_text);
      check_not_null(repeated);
      append_source(&source_text, repeated);
      tstr_freep(&repeated);
      repeated = tstr_repeat("]", depth);
      check_not_null(repeated);
      append_source(&source_text, repeated);
      append_source(&source_text, "\n}\n");
      check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, source_text, tstr_len(source_text)),
                  extra ? SALTS_ENOSPC : SALTS_OK);
      if (extra) check_contains(turbo_flow_last_error(flow)->message, "depth");
    }
  }

  it("retains the top-level field bound independently of the event budget") {
    for (size_t extra = 0u; extra < 2u; ++extra) {
      tstr_freep(&source_text);
      source_text = tstr_dup("stage s adapter fixture.test {\n");
      check_not_null(source_text);
      for (size_t i = 0u; i < FLOW_PROVIDER_CONFIG_MAX_FIELDS + extra; ++i) {
        tstr_freep(&repeated);
        repeated = tstr_from_ll((long long)i);
        check_not_null(repeated);
        append_source(&source_text, " f");
        append_source(&source_text, repeated);
        append_source(&source_text, " 0\n");
      }
      append_source(&source_text, "}\n");
      check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, source_text, tstr_len(source_text)),
                  extra ? SALTS_ENOSPC : SALTS_OK);
      if (extra) check_contains(turbo_flow_last_error(flow)->message, "field capacity");
    }
  }

  it("emits empty scalar text and rejects overflowing nested integers") {
    static const char valid[] =
        "stage s adapter fixture.test {\n v \"\"\n}\n";
    static const char overflow[] =
        "stage s adapter fixture.test {\n v [18446744073709551616]\n}\n";
    check_equal(turbo_flow_parse_string(flow, valid, sizeof(valid) - 1u), SALTS_OK);
    stage = (const flow_stage_plan_impl_t *)vec_at_const(&flow->stages, 0u);
    check_equal(flow_provider_config_reader_init(&reader, stage), SALTS_OK);
    expect_token(&reader.reader, CSERDE_MAP_BEGIN, NULL, 0u, 0);
    expect_token(&reader.reader, CSERDE_STRING, "v", 0u, 0);
    expect_token(&reader.reader, CSERDE_STRING, "", 0u, 0);
    expect_token(&reader.reader, CSERDE_MAP_END, NULL, 0u, 0);
    check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, overflow, sizeof(overflow) - 1u), SALTS_ERANGE);
  }

  it("bounds owned text including field names and string terminators") {
    for (size_t extra = 0u; extra < 2u; ++extra) {
      tstr_freep(&source_text);
      tstr_freep(&repeated);
      source_text = tstr_dup("stage s adapter fixture.test {\n v [\"");
      repeated = tstr_repeat("x", FLOW_PROVIDER_CONFIG_MAX_SOURCE_BYTES - 3u + extra);
      check_not_null(source_text);
      check_not_null(repeated);
      append_source(&source_text, repeated);
      append_source(&source_text, "\"]\n}\n");
      check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, source_text, tstr_len(source_text)),
                  extra ? SALTS_ENOSPC : SALTS_OK);
      if (extra) check_contains(turbo_flow_last_error(flow)->message, "capacity");
    }
  }

  it("rejects malformed containers before provider lookup and recovers on reset") {
    static const char *values[] = {
        "[1,]", "[", "{name}", "{name 1,}", "[true false]",
        "[{name \"x\"]}", "[\n1\n]"};
    for (size_t i = 0u; i < sizeof(values) / sizeof(values[0]); ++i) {
      tstr_freep(&source_text);
      source_text = tstr_dup("stage s adapter fixture.test {\n v ");
      check_not_null(source_text);
      append_source(&source_text, values[i]);
      append_source(&source_text, "\n}\n");
      check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
      check_not_equal(turbo_flow_parse_string(flow, source_text, tstr_len(source_text)),
                      SALTS_OK);
    }
    check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
    static const char valid[] = "stage s adapter fixture.test {\n v []\n}\n";
    check_equal(turbo_flow_parse_string(flow, valid, sizeof(valid) - 1u), SALTS_OK);
  }

  it("preserves resource binding grammar and duplicate top-level rejection") {
    static const char duplicate[] =
        "stage s adapter fixture.test {\n v []\n v {}\n}\n";
    static const char dotted[] =
        "stage s adapter fixture.test {\n resource db.main\n v []\n}\n";
    static const char hyphenated[] =
        "stage s adapter fixture.test {\n resource db-main\n v []\n}\n";
    check_equal(turbo_flow_parse_string(flow, duplicate, sizeof(duplicate) - 1u),
                SALTS_EALREADY);
    check_equal(turbo_flow_last_error(flow)->line, 3u);
    check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dotted, sizeof(dotted) - 1u), SALTS_OK);
    check_equal(turbo_flow_reset(flow, 0), SALTS_OK);
    check_not_equal(turbo_flow_parse_string(flow, hyphenated, sizeof(hyphenated) - 1u),
                    SALTS_OK);
  }

  it("deep-copies structured literals through use and releases them on reset") {
    static const char src[] =
        "stage reusable {\n in raw\n out clean\n"
        " step call operation fixture.call {\n"
        "  headers [{ name \"x-trace\", value \"a\" }]\n"
        " }\n raw -> call -> clean\n}\n"
        "stage main {\n source input\n use c = reusable\n}\n";
    const flow_stage_plan_impl_t *copy;
    source_text = tstr_dup(src);
    check_not_null(source_text);
    check_equal(turbo_flow_parse_string(flow, source_text, tstr_len(source_text)), SALTS_OK);
    tstr_freep(&source_text);
    stage = (const flow_stage_plan_impl_t *)vec_at_const(
        &flow->stages, (size_t)turbo_flow_find_stage(flow, "reusable.call"));
    copy = (const flow_stage_plan_impl_t *)vec_at_const(
        &flow->stages, (size_t)turbo_flow_find_stage(flow, "c.call"));
    check_not_null(stage);
    check_not_null(copy);
    check_equal(vec_size(&copy->provider_config_literals),
                vec_size(&stage->provider_config_literals));
    for (size_t i = 0u; i < vec_size(&copy->provider_config_literals); ++i) {
      const flow_provider_config_literal_t *a =
          (const flow_provider_config_literal_t *)vec_at_const(&stage->provider_config_literals, i);
      const flow_provider_config_literal_t *b =
          (const flow_provider_config_literal_t *)vec_at_const(&copy->provider_config_literals, i);
      check_equal(a->kind, b->kind);
      if (a->name) {
        check_equal(a->name, b->name);
        check_true(a->name != b->name);
      } else check_null(b->name);
      if (a->text) {
        check_equal(a->text, b->text);
        check_true(a->text != b->text);
      } else check_null(b->text);
    }
    check_equal(turbo_flow_reset(flow, 1), SALTS_OK);
    check_equal(vec_size(&flow->stages), (size_t)0u);
    check_equal(turbo_flow_parse_string(flow, src, sizeof(src) - 1u), SALTS_OK);
  }
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
