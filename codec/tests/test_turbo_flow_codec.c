#include "turbo_flow_codec.h"

#include "tinytest.h"
#include "salts_str.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static const turbo_flow_data_schema_t CODEC_OTHER_PROJECTION_SCHEMA = {
    sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DATA_ENCODING_JSON,
    "other.orders", "Order", "OtherProvider.Value", 0u, 1u, NULL};

static void codec_free_projection(void *ptr, void *ctx) {
  (void)ctx;
  free(ptr);
}

typedef struct codec_capture_ctx_s {
  atomic_int count;
  tstr payload;
} codec_capture_ctx_t;

typedef struct codec_databind_capture_ctx_s {
  atomic_int count;
  int id;
  tstr symbol;
  tstr payload;
  tstr schema_name;
  tstr type_name;
  int content_state;
  int encoding;
} codec_databind_capture_ctx_t;

typedef struct codec_csv_split_capture_s {
  atomic_int count;
  int ids[8];
  tstr symbols[8];
} codec_csv_split_capture_t;

typedef struct codec_clone_capture_s {
  atomic_int count;
  const void *source_projection;
  turbo_flow_msg_t clone;
} codec_clone_capture_t;

typedef struct codec_retry_capture_s {
  uint32_t attempts;
  uint32_t independent_clones;
  const void *source_projection;
} codec_retry_capture_t;

static int codec_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  codec_capture_ctx_t *capture = (codec_capture_ctx_t *)ctx;

  if (!capture || !msg) return SALTS_EINVAL;
  tstr_freep(&capture->payload);
  capture->payload = tstr_new_len(msg->payload.data ? msg->payload.data : "", msg->payload.len);
  if (!capture->payload) return SALTS_ENOMEM;
  atomic_fetch_add_explicit(&capture->count, 1, memory_order_release);
  return SALTS_OK;
}

static int codec_databind_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  codec_databind_capture_ctx_t *capture = (codec_databind_capture_ctx_t *)ctx;
  const DataBindValue *value;
  const DataBindValue *symbol;
  const turbo_flow_data_schema_t *schema = NULL;
  const char *symbol_data = NULL;
  size_t symbol_len = 0;

  if (!capture || !msg) return SALTS_EINVAL;
  value = turbo_flow_codec_msg_databind_value(msg);
  if (!value) return SALTS_EINVAL;
  if (value != turbo_flow_msg_projection(msg, &schema) || !schema) return SALTS_EINVAL;

  capture->id = data_bind_value_as_int(data_bind_value_get(value, "id"));
  symbol = data_bind_value_get(value, "symbol");
  if (data_bind_value_get_string(symbol, &symbol_data, &symbol_len) != DATA_BIND_OK) {
    return SALTS_EPROTO;
  }
  tstr_freep(&capture->symbol);
  capture->symbol = tstr_new_len(symbol_data ? symbol_data : "", symbol_len);
  if (!capture->symbol) return SALTS_ENOMEM;
  tstr_freep(&capture->payload);
  tstr_freep(&capture->schema_name);
  tstr_freep(&capture->type_name);
  capture->payload = tstr_new_len(msg->payload.data ? msg->payload.data : "", msg->payload.len);
  capture->schema_name = tstr_dup(schema->schema_name);
  capture->type_name = tstr_dup(schema->type_name);
  if (!capture->payload || !capture->schema_name || !capture->type_name) return SALTS_ENOMEM;
  capture->content_state = turbo_flow_msg_content_state(msg);
  capture->encoding = schema->encoding;
  atomic_fetch_add_explicit(&capture->count, 1, memory_order_release);
  return SALTS_OK;
}

static void codec_databind_capture_cleanup(codec_databind_capture_ctx_t *capture) {
  if (!capture) return;
  tstr_freep(&capture->symbol);
  tstr_freep(&capture->payload);
  tstr_freep(&capture->schema_name);
  tstr_freep(&capture->type_name);
}

static int codec_clone_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  codec_clone_capture_t *capture = (codec_clone_capture_t *)ctx;

  if (!capture || !msg) return SALTS_EINVAL;
  capture->source_projection = turbo_flow_codec_msg_databind_value(msg);
  if (!capture->source_projection) return SALTS_EINVAL;
  if (turbo_flow_msg_clone(&capture->clone, msg) != SALTS_OK) return SALTS_EPROTO;
  atomic_fetch_add_explicit(&capture->count, 1, memory_order_release);
  return SALTS_OK;
}

static int codec_retry_attempt(void *ctx, turbo_flow_msg_t *msg, uint32_t attempt) {
  codec_retry_capture_t *capture = (codec_retry_capture_t *)ctx;
  const DataBindValue *value = turbo_flow_codec_msg_databind_value(msg);

  if (!capture || !value || attempt == 0u || attempt > 2u) return SALTS_EINVAL;
  if (value == capture->source_projection) return SALTS_EPROTO;
  capture->independent_clones += 1u;
  capture->attempts = attempt;
  if (data_bind_value_as_int(data_bind_value_get(value, "id")) != 23) return SALTS_EPROTO;
  return attempt == 1u ? SALTS_EIO : SALTS_OK;
}

static int codec_retryable(void *ctx, int status) {
  (void)ctx;
  return status == SALTS_EIO;
}

static int codec_retry_consume(void *ctx, turbo_flow_t *flow,
                               const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  (void)ctx;
  (void)flow;
  (void)stage;
  (void)msg;
  return SALTS_EIO;
}

static int codec_retry_consume_retry(void *ctx, turbo_flow_t *flow,
                                     const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg,
                                     const turbo_flow_retry_policy_t *policy) {
  turbo_flow_retry_ops_t ops;

  (void)flow;
  if (!stage || !policy || policy->max_attempts != 2u) return SALTS_EINVAL;
  ((codec_retry_capture_t *)ctx)->source_projection = turbo_flow_codec_msg_databind_value(msg);
  if (!((codec_retry_capture_t *)ctx)->source_projection) return SALTS_EINVAL;
  memset(&ops, 0, sizeof(ops));
  ops.size = sizeof(ops);
  ops.attempt = codec_retry_attempt;
  ops.retryable = codec_retryable;
  return turbo_flow_retry_execute(policy, msg, &ops, ctx);
}

static int codec_csv_split_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  codec_csv_split_capture_t *capture = (codec_csv_split_capture_t *)ctx;
  const DataBindValue *value = turbo_flow_codec_msg_databind_value(msg);
  const DataBindValue *symbol;
  const char *data = NULL;
  size_t len = 0;
  int index;
  if (!capture || !value) return SALTS_EINVAL;
  index = atomic_load_explicit(&capture->count, memory_order_acquire);
  if (index < 0 || index >= 8) return SALTS_ENOSPC;
  capture->ids[index] = data_bind_value_as_int(data_bind_value_get(value, "id"));
  symbol = data_bind_value_get(value, "symbol");
  if (data_bind_value_get_string(symbol, &data, &len) != DATA_BIND_OK) return SALTS_EPROTO;
  capture->symbols[index] = tstr_new_len(data ? data : "", len);
  if (!capture->symbols[index]) return SALTS_ENOMEM;
  atomic_fetch_add_explicit(&capture->count, 1, memory_order_release);
  return SALTS_OK;
}

static void codec_write_le32(unsigned char *out, uint32_t value) {
  out[0] = (unsigned char)(value & 0xffu);
  out[1] = (unsigned char)((value >> 8u) & 0xffu);
  out[2] = (unsigned char)((value >> 16u) & 0xffu);
  out[3] = (unsigned char)((value >> 24u) & 0xffu);
}

static void codec_publish_payload(turbo_flow_t *flow, const char *source_name, const char *data,
                                  size_t len, int expected_rc) {
  turbo_flow_msg_t msg;
  mem_buffer_t *buffer = mem_wrap_external((void *)data, len, NULL, NULL);

  check_not_null(buffer);
  turbo_flow_msg_init(&msg);
  msg.buffer = buffer;
  msg.payload = vstr_from_buf(data, len);
  check_equal(turbo_flow_publish(flow, source_name, &msg), expected_rc);
  turbo_flow_msg_cleanup(&msg);
}

spec("turbo_flow_codec") {
  it("does not reinterpret another provider projection as DataBindValue") {
    int *value = (int *)malloc(sizeof(*value));
    turbo_flow_msg_t msg;

    check_not_null(value);
    *value = 7;
    turbo_flow_msg_init(&msg);
    check_equal(turbo_flow_msg_bind_projection(&msg, &CODEC_OTHER_PROJECTION_SCHEMA, value, NULL,
                                                codec_free_projection, NULL),
                 SALTS_OK);
    check_null(turbo_flow_codec_msg_databind_value(&msg));
    check_equal((const void *)turbo_flow_msg_projection(&msg, NULL), (const void *)value);
    turbo_flow_msg_cleanup(&msg);
  }

  it("extracts one line-delimited frame") {
    static const char *src = "source input\n"
                             "stage lines adapter \"codec.lines\"\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> lines -> capture\n"
                             "}\n";
    turbo_flow_codec_line_config_t config;
    codec_capture_ctx_t capture;
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_adapter_schema_t *adapter_schema;

    memset(&config, 0, sizeof(config));
    config.delimiter = TURBO_FLOW_CODEC_LINE_LF;
    config.strip_delimiter = 1;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.count, 0);

    check_not_null(flow);
    check_equal(turbo_flow_codec_register_line_adapter(flow, "codec.lines", &config), SALTS_OK);
    adapter_schema = turbo_flow_find_adapter_schema(flow, "codec.lines");
    check_not_null(adapter_schema);
    check_equal(adapter_schema->kind, TURBO_FLOW_ADAPTER_KIND_CODEC);
    check_equal(adapter_schema->roles, TURBO_FLOW_ADAPTER_TRANSFORM);
    check_equal(turbo_flow_register_stage_ex(flow, "capture", codec_capture_stage, &capture, NULL),
                 SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", "hello\n", 6u, SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);

    check_equal(atomic_load_explicit(&capture.count, memory_order_acquire), 1);
    check_not_null(capture.payload);
    check_equal(capture.payload, "hello", 5u);
    check_equal(tstr_len(capture.payload), 5u);

    tstr_freep(&capture.payload);
    turbo_flow_destroy(flow);
  }

  it("rejects partial and multiple line frames explicitly") {
    static const char *src = "source input\n"
                             "stage lines adapter \"codec.lines\"\n"
                             "stage main {\n"
                             "  input -> lines\n"
                             "}\n";
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_equal(turbo_flow_codec_register_line_adapter(flow, "codec.lines", NULL), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", "partial", 7u, SALTS_EPROTO);
    codec_publish_payload(flow, "input", "one\ntwo\n", 8u, SALTS_ENOTSUP);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("extracts a length-prefixed binary frame") {
    static const char *src = "source input\n"
                             "stage frame adapter \"codec.length\"\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> frame -> capture\n"
                             "}\n";
    unsigned char payload[7];
    const char expected[] = {'A', '\0', 'B'};
    turbo_flow_codec_length_config_t config;
    codec_capture_ctx_t capture;
    turbo_flow_t *flow = turbo_flow_create();

    codec_write_le32(payload, 3u);
    memcpy(payload + 4u, expected, sizeof(expected));
    memset(&config, 0, sizeof(config));
    config.max_frame_size = sizeof(expected);
    config.prefix = TURBO_FLOW_CODEC_LENGTH_PREFIX_LE32;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.count, 0);

    check_not_null(flow);
    check_equal(turbo_flow_codec_register_length_adapter(flow, "codec.length", &config), SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "capture", codec_capture_stage, &capture, NULL),
                 SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", (const char *)payload, sizeof(payload), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);

    check_equal(atomic_load_explicit(&capture.count, memory_order_acquire), 1);
    check_equal(tstr_len(capture.payload), sizeof(expected));
    check_equal(capture.payload, expected, sizeof(expected));

    tstr_freep(&capture.payload);
    turbo_flow_destroy(flow);
  }

  it("rejects a partial length-prefixed frame") {
    static const char *src = "source input\n"
                             "stage frame adapter \"codec.length\"\n"
                             "stage main {\n"
                             "  input -> frame\n"
                             "}\n";
    unsigned char payload[6];
    turbo_flow_t *flow = turbo_flow_create();

    codec_write_le32(payload, 4u);
    payload[4] = 'A';
    payload[5] = 'B';

    check_not_null(flow);
    check_equal(turbo_flow_codec_register_length_adapter(flow, "codec.length", NULL), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", (const char *)payload, sizeof(payload), SALTS_EPROTO);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("binds JSON payloads to DataBind parsed values") {
    static const char *src = "source input\n"
                             "stage bind adapter \"codec.databind\"\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> bind -> capture\n"
                             "}\n";
    static const char *schema = "message Order { uint32 id; string symbol; }\n";
    static const char *json = "{\"id\":7,\"symbol\":\"ABCD\"}";
    turbo_flow_codec_databind_config_t config;
    codec_databind_capture_ctx_t capture;
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_adapter_schema_t *adapter_schema;

    memset(&config, 0, sizeof(config));
    config.schema_text = schema;
    config.type_name = "Order";
    config.input_format = TURBO_FLOW_CODEC_DATABIND_JSON;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.count, 0);

    check_not_null(flow);
    check_equal(turbo_flow_codec_register_databind_adapter(flow, "codec.databind", &config),
                 SALTS_OK);
    adapter_schema = turbo_flow_find_adapter_schema(flow, "codec.databind");
    check_not_null(adapter_schema);
    check_equal(adapter_schema->kind, TURBO_FLOW_ADAPTER_KIND_DATABIND);
    check_equal(adapter_schema->roles, TURBO_FLOW_ADAPTER_TRANSFORM);
    check_equal(
        turbo_flow_register_stage_ex(flow, "capture", codec_databind_capture_stage, &capture, NULL),
        SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", json, strlen(json), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);

    check_equal(atomic_load_explicit(&capture.count, memory_order_acquire), 1);
    check_equal(capture.id, 7);
    check_equal(capture.symbol, "ABCD");
    check_equal(capture.payload, json);
    check_equal(capture.content_state, TURBO_FLOW_CONTENT_SCHEMA_BOUND);
    check_equal(capture.schema_name, "Order");
    check_equal(capture.type_name, "Order");
    check_equal(capture.encoding, TURBO_FLOW_DATA_ENCODING_JSON);

    codec_databind_capture_cleanup(&capture);
    turbo_flow_destroy(flow);
  }

  it("deep clones DataBind projections across a message lifetime boundary") {
    static const char *src = "source input\n"
                             "stage bind adapter \"codec.databind\"\n"
                             "stage clone_capture\n"
                             "stage main {\n"
                             "  input -> bind -> clone_capture\n"
                             "}\n";
    static const char *schema = "message Order { uint32 id; string symbol; }\n";
    static const char *json = "{\"id\":17,\"symbol\":\"CLONE\"}";
    turbo_flow_codec_databind_config_t config;
    codec_clone_capture_t capture;
    turbo_flow_t *flow = turbo_flow_create();
    const DataBindValue *copy;
    const DataBindValue *symbol;
    const char *symbol_data = NULL;
    size_t symbol_len = 0;

    memset(&config, 0, sizeof(config));
    config.schema_text = schema;
    config.type_name = "Order";
    config.input_format = TURBO_FLOW_CODEC_DATABIND_JSON;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.count, 0);
    turbo_flow_msg_init(&capture.clone);

    check_not_null(flow);
    check_equal(turbo_flow_codec_register_databind_adapter(flow, "codec.databind", &config),
                 SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "clone_capture", codec_clone_capture_stage,
                                              &capture, NULL),
                 SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", json, strlen(json), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);

    check_equal(atomic_load_explicit(&capture.count, memory_order_acquire), 1);
    copy = turbo_flow_codec_msg_databind_value(&capture.clone);
    check_not_null(copy);
    check_not_equal((const void *)copy, (const void *)capture.source_projection);
    check_equal(data_bind_value_as_int(data_bind_value_get(copy, "id")), 17);
    symbol = data_bind_value_get(copy, "symbol");
    check_equal(data_bind_value_get_string(symbol, &symbol_data, &symbol_len), DATA_BIND_OK);
    check_equal(symbol_len, 5u);
    check_equal(symbol_data, "CLONE", 5u);

    turbo_flow_msg_cleanup(&capture.clone);
    turbo_flow_destroy(flow);
  }

  it("deep clones DataBind projections for each graph retry attempt") {
    static const char *src = "source input\n"
                             "stage bind adapter \"codec.databind\"\n"
                             "stage remote adapter retryable retry attempts 2\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> bind -> remote -> capture\n"
                             "}\n";
    static const char *schema = "message Order { uint32 id; string symbol; }\n";
    static const char *json = "{\"id\":23,\"symbol\":\"RETRY\"}";
    turbo_flow_codec_databind_config_t config;
    turbo_flow_adapter_ops_t ops;
    codec_retry_capture_t retry;
    codec_databind_capture_ctx_t capture;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&config, 0, sizeof(config));
    config.schema_text = schema;
    config.type_name = "Order";
    config.input_format = TURBO_FLOW_CODEC_DATABIND_JSON;
    memset(&ops, 0, sizeof(ops));
    ops.consume = codec_retry_consume;
    ops.consume_retry = codec_retry_consume_retry;
    memset(&retry, 0, sizeof(retry));
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.count, 0);

    check_not_null(flow);
    check_equal(turbo_flow_codec_register_databind_adapter(flow, "codec.databind", &config),
                 SALTS_OK);
    check_equal(turbo_flow_register_adapter(flow, "retryable", &ops, &retry), SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "capture", codec_databind_capture_stage, &capture, NULL),
        SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", json, strlen(json), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);

    check_equal(retry.attempts, 2u);
    check_equal(retry.independent_clones, 2u);
    check_equal(atomic_load_explicit(&capture.count, memory_order_acquire), 1);
    check_equal(capture.id, 23);
    check_equal(capture.symbol, "RETRY");

    codec_databind_capture_cleanup(&capture);
    turbo_flow_destroy(flow);
  }

  it("binds bin payloads through the same DataBind adapter") {
    static const char *src = "source input\n"
                             "stage bind adapter \"codec.databind\"\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> bind -> capture\n"
                             "}\n";
    static const char *schema = "message Order { uint32 id; string symbol; }\n";
    unsigned char payload[12];
    turbo_flow_codec_databind_config_t config;
    codec_databind_capture_ctx_t capture;
    turbo_flow_t *flow = turbo_flow_create();

    codec_write_le32(payload, 9u);
    codec_write_le32(payload + 4u, 4u);
    memcpy(payload + 8u, "WXYZ", 4u);

    memset(&config, 0, sizeof(config));
    config.schema_text = schema;
    config.type_name = "Order";
    config.input_format = TURBO_FLOW_CODEC_DATABIND_BIN;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.count, 0);

    check_not_null(flow);
    check_equal(turbo_flow_codec_register_databind_adapter(flow, "codec.databind", &config),
                 SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "capture", codec_databind_capture_stage, &capture, NULL),
        SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", (const char *)payload, sizeof(payload), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);

    check_equal(atomic_load_explicit(&capture.count, memory_order_acquire), 1);
    check_equal(capture.id, 9);
    check_equal(capture.symbol, "WXYZ");

    codec_databind_capture_cleanup(&capture);
    turbo_flow_destroy(flow);
  }

  it("binds CSV payloads and validates XML payloads through DataBind") {
    static const char *bind_src = "source input\n"
                                  "stage bind adapter \"codec.databind\"\n"
                                  "stage capture\n"
                                  "stage main {\n"
                                  "  input -> bind -> capture\n"
                                  "}\n";
    static const char *validate_src = "source input\n"
                                      "stage validate adapter \"codec.databind\"\n"
                                      "stage main {\n"
                                      "  input -> validate\n"
                                      "}\n";
    static const char *schema = "message Order { uint32 id; string symbol; }\n";
    static const char *csv = "id,symbol\n11,EFGH\n";
    static const char *xml = "<order><id>12</id><symbol>IJKL</symbol></order>";
    turbo_flow_codec_databind_config_t config;
    codec_databind_capture_ctx_t capture;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&config, 0, sizeof(config));
    config.schema_text = schema;
    config.type_name = "Order";
    config.input_format = TURBO_FLOW_CODEC_DATABIND_CSV;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.count, 0);

    check_not_null(flow);
    check_equal(turbo_flow_codec_register_databind_adapter(flow, "codec.databind", &config),
                 SALTS_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "capture", codec_databind_capture_stage, &capture, NULL),
        SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, bind_src, strlen(bind_src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", csv, strlen(csv), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(capture.id, 11);
    check_equal(capture.symbol, "EFGH");
    codec_databind_capture_cleanup(&capture);
    turbo_flow_destroy(flow);

    flow = turbo_flow_create();
    memset(&config, 0, sizeof(config));
    config.schema_text = schema;
    config.type_name = "Order";
    config.input_format = TURBO_FLOW_CODEC_DATABIND_XML;
    config.validate_only = 1;

    check_not_null(flow);
    check_equal(turbo_flow_codec_register_databind_adapter(flow, "codec.databind", &config),
                 SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, validate_src, strlen(validate_src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", xml, strlen(xml), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects invalid schema-bound JSON during validation") {
    static const char *src = "source input\n"
                             "stage validate adapter \"codec.databind\"\n"
                             "stage main {\n"
                             "  input -> validate\n"
                             "}\n";
    static const char *schema = "message Order { uint32 id; string symbol; }\n";
    static const char *json = "{\"id\":\"bad\",\"symbol\":\"ABCD\"}";
    turbo_flow_codec_databind_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&config, 0, sizeof(config));
    config.schema_text = schema;
    config.type_name = "Order";
    config.input_format = TURBO_FLOW_CODEC_DATABIND_JSON;
    config.validate_only = 1;

    check_not_null(flow);
    check_equal(turbo_flow_codec_register_databind_adapter(flow, "codec.databind", &config),
                 SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", json, strlen(json), SALTS_EPROTO);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects XPath for unsupported single-value XML binding at registration") {
    static const char *schema = "message Order { uint32 id; string symbol; }\n";
    turbo_flow_codec_databind_config_t config;
    turbo_flow_t *flow = turbo_flow_create();

    memset(&config, 0, sizeof(config));
    config.schema_text = schema;
    config.type_name = "Order";
    config.input_format = TURBO_FLOW_CODEC_DATABIND_XML;
    config.xml_xpath = "/orders/order";

    check_not_null(flow);
    check_equal(turbo_flow_codec_register_databind_adapter(flow, "codec.databind", &config),
                 SALTS_ENOTSUP);
    turbo_flow_destroy(flow);
  }

  it("splits CSV rows into ordered source messages for DataBind") {
    static const char *dsl = "source input\n"
                             "source rows\n"
                             "stage split adapter codec.csv.split\n"
                             "stage bind adapter codec.databind\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> split\n"
                             "  rows -> bind -> capture\n"
                             "}\n";
    static const char *schema = "message Order { uint32 id; string symbol; }\n";
    static const char *csv = "id,symbol\n11,\"A,B\"\n12,EFGH\n";
    turbo_flow_codec_csv_split_config_t split_config;
    turbo_flow_codec_databind_config_t bind_config;
    codec_csv_split_capture_t capture;
    turbo_flow_t *flow = turbo_flow_create();
    memset(&split_config, 0, sizeof(split_config));
    split_config.output_source = "rows";
    memset(&bind_config, 0, sizeof(bind_config));
    bind_config.schema_text = schema;
    bind_config.type_name = "Order";
    bind_config.input_format = TURBO_FLOW_CODEC_DATABIND_CSV;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.count, 0);
    check_not_null(flow);
    check_equal(
        turbo_flow_codec_register_csv_splitter_adapter(flow, "codec.csv.split", &split_config),
        SALTS_OK);
    check_equal(turbo_flow_codec_register_databind_adapter(flow, "codec.databind", &bind_config),
                 SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "capture", codec_csv_split_capture_stage,
                                              &capture, NULL),
                 SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", csv, strlen(csv), SALTS_OK);
    check_equal(atomic_load_explicit(&capture.count, memory_order_acquire), 2);
    check_equal(capture.ids[0], 11);
    check_equal(capture.ids[1], 12);
    check_equal(capture.symbols[0], "A,B");
    check_equal(capture.symbols[1], "EFGH");
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    tstr_freep(&capture.symbols[0]);
    tstr_freep(&capture.symbols[1]);
    turbo_flow_destroy(flow);
  }

  it("rejects CSV row overflow before emitting any message") {
    static const char *dsl = "source input\n"
                             "source rows\n"
                             "stage split adapter codec.csv.split\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> split\n"
                             "  rows -> capture\n"
                             "}\n";
    static const char *csv = "id,symbol\n1,A\n2,B\n";
    turbo_flow_codec_csv_split_config_t config;
    codec_capture_ctx_t capture;
    turbo_flow_t *flow = turbo_flow_create();
    memset(&config, 0, sizeof(config));
    config.output_source = "rows";
    config.max_rows = 1;
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.count, 0);
    check_not_null(flow);
    check_equal(turbo_flow_codec_register_csv_splitter_adapter(flow, "codec.csv.split", &config),
                 SALTS_OK);
    check_equal(turbo_flow_register_stage_ex(flow, "capture", codec_capture_stage, &capture, NULL),
                 SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    codec_publish_payload(flow, "input", csv, strlen(csv), SALTS_ENOSPC);
    check_equal(atomic_load_explicit(&capture.count, memory_order_acquire), 0);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects recursive or unknown CSV splitter output sources at start") {
    static const char *recursive = "source rows\n"
                                   "stage split adapter codec.csv.split\n"
                                   "stage main {\n"
                                   "  rows -> split\n"
                                   "}\n";
    static const char *unknown = "source input\n"
                                 "stage split adapter codec.csv.split\n"
                                 "stage main {\n"
                                 "  input -> split\n"
                                 "}\n";
    turbo_flow_codec_csv_split_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    memset(&config, 0, sizeof(config));
    config.output_source = "rows";
    check_not_null(flow);
    check_equal(turbo_flow_codec_register_csv_splitter_adapter(flow, "codec.csv.split", &config),
                 SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, recursive, strlen(recursive)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_ELOOP);
    turbo_flow_destroy(flow);

    flow = turbo_flow_create();
    check_not_null(flow);
    check_equal(turbo_flow_codec_register_csv_splitter_adapter(flow, "codec.csv.split", &config),
                 SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, unknown, strlen(unknown)), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_ENOENT);
    turbo_flow_destroy(flow);
  }
}
