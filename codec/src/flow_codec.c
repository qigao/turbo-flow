#include "turbo_flow_codec.h"

#include "turbo_parser.h"
#include "turbo_flow_stl_adapter.h"

#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static const char *const FLOW_CODEC_DELIMITER_VALUES[] = {"lf", "crlf"};
static const char *const FLOW_CODEC_PREFIX_VALUES[] = {"le32", "le64", "be32", "be64"};
static const char *const FLOW_DATABIND_INPUT_VALUES[] = {"bin", "json", "csv", "xml"};
static const char FLOW_DATABIND_PROJECTION_TYPE[] = "TurboUtils.DataBindValue";

static const turbo_flow_option_field_t FLOW_CODEC_LINE_FIELDS[] = {
    {"max_frame_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"delimiter", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_CODEC_DELIMITER_VALUES, 2},
    {"strip_delimiter", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0}};

static const turbo_flow_option_field_t FLOW_CODEC_LENGTH_FIELDS[] = {
    {"max_frame_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"prefix", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_CODEC_PREFIX_VALUES, 4}};

static const turbo_flow_option_field_t FLOW_DATABIND_FIELDS[] = {
    {"schema_path", TURBO_FLOW_OPTION_SCHEMA_PATH, 0, 0, 0, NULL, 0},
    {"schema_text", TURBO_FLOW_OPTION_SCHEMA_TEXT, 0, 0, 0, NULL, 0},
    {"schema_text_len", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"type_name", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"input_format", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0,
     FLOW_DATABIND_INPUT_VALUES, 4},
    {"validate_only", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"bind_all", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"csv_row", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"xml_xpath", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"max_payload_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0}};

static const turbo_flow_adapter_schema_t FLOW_CODEC_LINE_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_CODEC,
    TURBO_FLOW_ADAPTER_TRANSFORM,
    TURBO_FLOW_ADAPTER_BIDIRECTIONAL,
    FLOW_CODEC_LINE_FIELDS,
    sizeof(FLOW_CODEC_LINE_FIELDS) / sizeof(FLOW_CODEC_LINE_FIELDS[0])};
static const turbo_flow_adapter_schema_t FLOW_CODEC_LENGTH_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_CODEC,
    TURBO_FLOW_ADAPTER_TRANSFORM,
    TURBO_FLOW_ADAPTER_BIDIRECTIONAL,
    FLOW_CODEC_LENGTH_FIELDS,
    sizeof(FLOW_CODEC_LENGTH_FIELDS) / sizeof(FLOW_CODEC_LENGTH_FIELDS[0])};
static const turbo_flow_adapter_schema_t FLOW_DATABIND_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_DATABIND,
    TURBO_FLOW_ADAPTER_TRANSFORM,
    TURBO_FLOW_ADAPTER_BIDIRECTIONAL,
    FLOW_DATABIND_FIELDS,
    sizeof(FLOW_DATABIND_FIELDS) / sizeof(FLOW_DATABIND_FIELDS[0])};
static const turbo_flow_option_field_t FLOW_CSV_SPLIT_FIELDS[] = {
    {"output_source", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"max_rows", TURBO_FLOW_OPTION_SIZE, TURBO_FLOW_OPTION_HAS_MIN, 1, 0, NULL, 0},
    {"max_payload_size", TURBO_FLOW_OPTION_SIZE, TURBO_FLOW_OPTION_HAS_MIN, 1, 0, NULL, 0},
    {"max_row_size", TURBO_FLOW_OPTION_SIZE, TURBO_FLOW_OPTION_HAS_MIN, 1, 0, NULL, 0}};
static const turbo_flow_adapter_schema_t FLOW_CSV_SPLIT_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_CODEC,
    TURBO_FLOW_ADAPTER_SINK,
    TURBO_FLOW_ADAPTER_OUTPUT,
    FLOW_CSV_SPLIT_FIELDS,
    sizeof(FLOW_CSV_SPLIT_FIELDS) / sizeof(FLOW_CSV_SPLIT_FIELDS[0])};

#define FLOW_CODEC_CSV_DEFAULT_MAX_ROWS 10000u
#define FLOW_CODEC_CSV_DEFAULT_MAX_PAYLOAD (16u * 1024u * 1024u)
#define FLOW_CODEC_CSV_DEFAULT_MAX_ROW (1024u * 1024u)

typedef enum flow_codec_adapter_kind_e {
  FLOW_CODEC_LINE = 0,
  FLOW_CODEC_LENGTH,
  FLOW_CODEC_DATABIND,
  FLOW_CODEC_CSV_SPLIT
} flow_codec_adapter_kind_t;

typedef struct flow_codec_adapter_s {
  flow_codec_adapter_kind_t kind;
  size_t max_payload_size;
  int delimiter;
  int strip_delimiter;
  int prefix;
  int input_format;
  int validate_only;
  int bind_all;
  size_t csv_row;
  size_t max_rows;
  size_t max_row_size;
  char csv_delimiter;
  char csv_quote;
  tstr output_source;
  tstr schema_path;
  tstr schema_text;
  tstr schema_name;
  tstr type_name;
  tstr xml_xpath;
  turbo_flow_data_schema_t data_schema;
  DataBind *databind;
  turbo_mutex_t databind_lock;
  int databind_lock_initialized;
  atomic_int started;
} flow_codec_adapter_t;

static void flow_codec_databind_value_destroy(void *ptr, void *ctx) {
  (void)ctx;
  data_bind_value_free((DataBindValue *)ptr);
}

const DataBindValue *turbo_flow_codec_msg_databind_value(const turbo_flow_msg_t *msg) {
  const turbo_flow_data_schema_t *schema = NULL;
  const void *projection = turbo_flow_msg_projection(msg, &schema);

  if (!projection || !schema || !schema->projection_type ||
      strcmp(schema->projection_type, FLOW_DATABIND_PROJECTION_TYPE) != 0) {
    return NULL;
  }
  return (const DataBindValue *)projection;
}

static int flow_codec_dup_opt(tstr *dst, const char *src, size_t len) {
  if (!dst) return TURBO_EINVAL;
  if (!src) return TURBO_OK;
  if (len == 0) len = strlen(src);
  *dst = tstr_new_len(src, len);
  return *dst ? TURBO_OK : TURBO_ENOMEM;
}

static int flow_codec_valid_line_delimiter(int delimiter) {
  return delimiter == TURBO_FLOW_CODEC_LINE_LF || delimiter == TURBO_FLOW_CODEC_LINE_CRLF;
}

static int flow_codec_valid_length_prefix(int prefix) {
  return prefix == TURBO_FLOW_CODEC_LENGTH_PREFIX_LE32 ||
         prefix == TURBO_FLOW_CODEC_LENGTH_PREFIX_LE64 ||
         prefix == TURBO_FLOW_CODEC_LENGTH_PREFIX_BE32 ||
         prefix == TURBO_FLOW_CODEC_LENGTH_PREFIX_BE64;
}

static int flow_codec_valid_databind_input(int input_format) {
  return input_format == TURBO_FLOW_CODEC_DATABIND_BIN ||
         input_format == TURBO_FLOW_CODEC_DATABIND_JSON ||
         input_format == TURBO_FLOW_CODEC_DATABIND_CSV ||
         input_format == TURBO_FLOW_CODEC_DATABIND_XML;
}

static turbo_flow_data_encoding_t flow_codec_databind_encoding(int input_format) {
  switch (input_format) {
  case TURBO_FLOW_CODEC_DATABIND_JSON:
    return TURBO_FLOW_DATA_ENCODING_JSON;
  case TURBO_FLOW_CODEC_DATABIND_CSV:
    return TURBO_FLOW_DATA_ENCODING_CSV;
  case TURBO_FLOW_CODEC_DATABIND_XML:
    return TURBO_FLOW_DATA_ENCODING_XML;
  case TURBO_FLOW_CODEC_DATABIND_BIN:
  default:
    return TURBO_FLOW_DATA_ENCODING_TBE;
  }
}

static int flow_codec_check_payload(const flow_codec_adapter_t *adapter,
                                    const turbo_flow_msg_t *msg) {
  if (!adapter || !msg || (msg->payload.len > 0 && !msg->payload.data)) return TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_codec_replace_payload(turbo_flow_msg_t *msg, const char *data, size_t len) {
  tstr owned;

  if (!msg || (len > 0 && !data)) return TURBO_EINVAL;
  owned = tstr_new_len(data ? data : "", len);
  if (!owned) return TURBO_ENOMEM;

  turbo_flow_msg_clear_content(msg);
  tstr_freep(&msg->owned_payload);
  mem_buffer_release(msg->buffer);
  msg->buffer = NULL;
  msg->owned_payload = owned;
  msg->payload = tstr_to_v(msg->owned_payload);
  return TURBO_OK;
}

static int flow_codec_transform_line(flow_codec_adapter_t *adapter, turbo_flow_msg_t *msg) {
  const char *data;
  size_t len;
  size_t frame_len = 0;
  size_t delim_len = 0;
  int found = 0;

  if (!adapter || !msg) return TURBO_EINVAL;
  data = msg->payload.data;
  len = msg->payload.len;

  if (adapter->delimiter == TURBO_FLOW_CODEC_LINE_CRLF) {
    delim_len = 2u;
    for (size_t i = 0; i + 1u < len; ++i) {
      if (data[i] == '\r' && data[i + 1u] == '\n') {
        frame_len = i;
        found = 1;
        break;
      }
    }
  } else {
    delim_len = 1u;
    for (size_t i = 0; i < len; ++i) {
      if (data[i] == '\n') {
        frame_len = i;
        found = 1;
        break;
      }
    }
  }

  if (!found) return TURBO_EPROTO;
  if (adapter->strip_delimiter == 0) frame_len += delim_len;
  if (frame_len > len) return TURBO_EINVAL;
  if (adapter->max_payload_size > 0 && frame_len > adapter->max_payload_size) return TURBO_EFBIG;
  if (len > frame_len + (adapter->strip_delimiter ? delim_len : 0u)) return TURBO_ENOTSUP;
  return flow_codec_replace_payload(msg, data, frame_len);
}

static uint64_t flow_codec_load_le(const unsigned char *data, size_t n) {
  uint64_t value = 0;
  for (size_t i = 0; i < n; ++i)
    value |= ((uint64_t)data[i]) << (i * 8u);
  return value;
}

static uint64_t flow_codec_load_be(const unsigned char *data, size_t n) {
  uint64_t value = 0;
  for (size_t i = 0; i < n; ++i)
    value = (value << 8u) | (uint64_t)data[i];
  return value;
}

static int flow_codec_transform_length(flow_codec_adapter_t *adapter, turbo_flow_msg_t *msg) {
  const unsigned char *data;
  size_t prefix_size;
  uint64_t frame_len64;
  size_t frame_len;

  if (!adapter || !msg) return TURBO_EINVAL;
  data = (const unsigned char *)msg->payload.data;
  prefix_size = (adapter->prefix == TURBO_FLOW_CODEC_LENGTH_PREFIX_LE64 ||
                 adapter->prefix == TURBO_FLOW_CODEC_LENGTH_PREFIX_BE64)
                    ? 8u
                    : 4u;
  if (msg->payload.len < prefix_size) return TURBO_EPROTO;

  frame_len64 = (adapter->prefix == TURBO_FLOW_CODEC_LENGTH_PREFIX_BE32 ||
                 adapter->prefix == TURBO_FLOW_CODEC_LENGTH_PREFIX_BE64)
                    ? flow_codec_load_be(data, prefix_size)
                    : flow_codec_load_le(data, prefix_size);
  if (frame_len64 > (uint64_t)((size_t)-1)) return TURBO_ERANGE;
  frame_len = (size_t)frame_len64;
  if (adapter->max_payload_size > 0 && frame_len > adapter->max_payload_size) return TURBO_EFBIG;
  if (msg->payload.len < prefix_size + frame_len) return TURBO_EPROTO;
  if (msg->payload.len > prefix_size + frame_len) return TURBO_ENOTSUP;
  return flow_codec_replace_payload(msg, (const char *)data + prefix_size, frame_len);
}

static int flow_codec_databind_status_to_turbo(DataBindStatus status) {
  switch (status) {
  case DATA_BIND_OK:
    return TURBO_OK;
  case DATA_BIND_ERR_INVALID_ARG:
    return TURBO_EINVAL;
  case DATA_BIND_ERR_IO:
    return TURBO_EIO;
  case DATA_BIND_ERR_TYPE_NOT_FOUND:
    return TURBO_ENOENT;
  case DATA_BIND_ERR_OOM:
    return TURBO_ENOMEM;
  case DATA_BIND_ERR_PARSE:
  case DATA_BIND_ERR_SCHEMA:
  case DATA_BIND_ERR_TYPE_MISMATCH:
  case DATA_BIND_ERR_RUNTIME:
  default:
    return TURBO_EPROTO;
  }
}

static int flow_codec_databind_value_clone(const void *value, void *ctx, void **out) {
  DataBindValue *copy = NULL;
  DataBindStatus status;

  (void)ctx;
  if (!value || !out) return TURBO_EINVAL;
  *out = NULL;
  status = data_bind_value_clone((const DataBindValue *)value, &copy);
  if (status != DATA_BIND_OK) return flow_codec_databind_status_to_turbo(status);
  if (!copy) return TURBO_EPROTO;
  *out = copy;
  return TURBO_OK;
}

static int flow_codec_databind_validate(flow_codec_adapter_t *adapter, turbo_flow_msg_t *msg) {
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindStatus status;

  if (!adapter || !adapter->databind || !adapter->type_name || !msg) return TURBO_EINVAL;
  switch (adapter->input_format) {
  case TURBO_FLOW_CODEC_DATABIND_JSON:
    status = data_bind_validate_json(adapter->databind, adapter->type_name, msg->payload.data,
                                     msg->payload.len, &error);
    break;
  case TURBO_FLOW_CODEC_DATABIND_CSV:
    status = data_bind_validate_csv(adapter->databind, adapter->type_name, msg->payload.data,
                                    msg->payload.len, &error);
    break;
  case TURBO_FLOW_CODEC_DATABIND_XML:
    status = data_bind_validate_xml_path(adapter->databind, adapter->type_name, msg->payload.data,
                                         msg->payload.len, adapter->xml_xpath, &error);
    break;
  case TURBO_FLOW_CODEC_DATABIND_BIN: {
    DataBindValue *value = NULL;
    status = data_bind_parse(adapter->databind, adapter->type_name,
                             (const uint8_t *)msg->payload.data, msg->payload.len, &value, &error);
    data_bind_value_free(value);
    break;
  }
  default:
    status = DATA_BIND_ERR_INVALID_ARG;
    break;
  }
  return flow_codec_databind_status_to_turbo(status);
}

static int flow_codec_databind_parse(flow_codec_adapter_t *adapter, turbo_flow_msg_t *msg) {
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindValue *value = NULL;
  DataBindStatus status;
  int rc;

  if (!adapter || !adapter->databind || !adapter->type_name || !msg) return TURBO_EINVAL;
  switch (adapter->input_format) {
  case TURBO_FLOW_CODEC_DATABIND_BIN:
    status = data_bind_parse(adapter->databind, adapter->type_name,
                             (const uint8_t *)msg->payload.data, msg->payload.len, &value, &error);
    break;
  case TURBO_FLOW_CODEC_DATABIND_JSON:
    status = adapter->bind_all
                 ? data_bind_parse_json_all(adapter->databind, adapter->type_name,
                                            msg->payload.data, msg->payload.len, &value, &error)
                 : data_bind_parse_json(adapter->databind, adapter->type_name, msg->payload.data,
                                        msg->payload.len, &value, &error);
    break;
  case TURBO_FLOW_CODEC_DATABIND_CSV:
    status = adapter->bind_all
                 ? data_bind_parse_csv_all(adapter->databind, adapter->type_name, msg->payload.data,
                                           msg->payload.len, &value, &error)
                 : data_bind_parse_csv(adapter->databind, adapter->type_name, msg->payload.data,
                                       msg->payload.len, adapter->csv_row, &value, &error);
    break;
  case TURBO_FLOW_CODEC_DATABIND_XML:
    if (adapter->bind_all) {
      status = data_bind_parse_xml_path_all(adapter->databind, adapter->type_name,
                                            msg->payload.data, msg->payload.len,
                                            adapter->xml_xpath, &value, &error);
    } else {
      status = data_bind_parse_xml(adapter->databind, adapter->type_name, msg->payload.data,
                                   msg->payload.len, &value, &error);
    }
    break;
  default:
    status = DATA_BIND_ERR_INVALID_ARG;
    break;
  }

  rc = flow_codec_databind_status_to_turbo(status);
  if (rc != TURBO_OK) {
    data_bind_value_free(value);
    return rc;
  }
  if (!value) return TURBO_EPROTO;
  turbo_flow_msg_clear_content(msg);
  rc = turbo_flow_msg_bind_projection(msg, &adapter->data_schema, value,
                                      flow_codec_databind_value_clone,
                                      flow_codec_databind_value_destroy, NULL);
  if (rc != TURBO_OK) data_bind_value_free(value);
  return rc;
}

static int flow_codec_transform_databind(flow_codec_adapter_t *adapter, turbo_flow_msg_t *msg) {
  int rc;

  if (!adapter || !msg) return TURBO_EINVAL;
  if (!adapter->databind || !adapter->type_name) return TURBO_ENOTSUP;
  if (adapter->max_payload_size > 0 && msg->payload.len > adapter->max_payload_size) {
    return TURBO_EFBIG;
  }

  turbo_mutex_lock(&adapter->databind_lock);
  rc = adapter->validate_only ? flow_codec_databind_validate(adapter, msg)
                              : flow_codec_databind_parse(adapter, msg);
  turbo_mutex_unlock(&adapter->databind_lock);
  return rc;
}

static int flow_codec_csv_append_cell(tstr *row, const char *value, char delimiter, char quote) {
  int quoted = 0;
  if (!row || !value) return TURBO_EINVAL;
  for (const char *p = value; *p; ++p) {
    if (*p == delimiter || *p == quote || *p == '\r' || *p == '\n') {
      quoted = 1;
      break;
    }
  }
  if (quoted && !(*row = tstr_cat_len(*row, &quote, 1))) return TURBO_ENOMEM;
  for (const char *p = value; *p; ++p) {
    if (*p == quote && !(*row = tstr_cat_len(*row, &quote, 1))) return TURBO_ENOMEM;
    if (!(*row = tstr_cat_len(*row, p, 1))) return TURBO_ENOMEM;
  }
  if (quoted && !(*row = tstr_cat_len(*row, &quote, 1))) return TURBO_ENOMEM;
  return TURBO_OK;
}

static int flow_codec_csv_append_record(tstr *payload, const turbo_csv_doc_t *doc,
                                        size_t row_index, char delimiter, char quote) {
  size_t columns = turbo_csv_column_count(doc);
  for (size_t column = 0; column < columns; ++column) {
    const char *value = turbo_csv_get(doc, row_index, column);
    int rc;
    if (column > 0 && !(*payload = tstr_cat_len(*payload, &delimiter, 1))) return TURBO_ENOMEM;
    rc = flow_codec_csv_append_cell(payload, value ? value : "", delimiter, quote);
    if (rc != TURBO_OK) return rc;
  }
  if (!(*payload = tstr_cat_len(*payload, "\n", 1))) return TURBO_ENOMEM;
  return TURBO_OK;
}

static int flow_codec_csv_emit_row(flow_codec_adapter_t *adapter, turbo_flow_t *flow,
                                   const turbo_flow_msg_t *input, const turbo_csv_doc_t *doc,
                                   size_t row_index) {
  turbo_flow_msg_t output;
  int rc;
  turbo_flow_msg_init(&output);
  output.id = input->id;
  output.ts_ns = input->ts_ns;
  output.type = input->type;
  output.flags = input->flags;
  output.status = input->status;
  output.owned_payload = tstr_new_len(NULL, 0);
  if (!output.owned_payload) return TURBO_ENOMEM;
  rc = flow_codec_csv_append_record(&output.owned_payload, doc, 0, adapter->csv_delimiter,
                                    adapter->csv_quote);
  if (rc == TURBO_OK)
    rc = flow_codec_csv_append_record(&output.owned_payload, doc, row_index, adapter->csv_delimiter,
                                      adapter->csv_quote);
  if (rc == TURBO_OK && tstr_len(output.owned_payload) > adapter->max_row_size) rc = TURBO_EFBIG;
  if (rc == TURBO_OK) {
    output.payload = tstr_to_v(output.owned_payload);
    rc = turbo_flow_publish(flow, adapter->output_source, &output);
  }
  turbo_flow_msg_cleanup(&output);
  return rc;
}

static int flow_codec_transform_csv_split(flow_codec_adapter_t *adapter, turbo_flow_t *flow,
                                          turbo_flow_msg_t *msg) {
  turbo_csv_options_t options = {false, adapter->csv_delimiter, adapter->csv_quote, true};
  turbo_csv_doc_t *doc = NULL;
  size_t rows;
  int rc;
  if (msg->payload.len > adapter->max_payload_size) return TURBO_EFBIG;
  if (turbo_parse_csv_opts((const uint8_t *)msg->payload.data, msg->payload.len, &options, &doc) !=
      0)
    return TURBO_EPROTO;
  rows = turbo_csv_row_count(doc);
  if (rows > 0) --rows;
  if (rows > adapter->max_rows) {
    turbo_free_csv(&doc);
    return TURBO_ENOSPC;
  }
  rc = TURBO_OK;
  for (size_t row = 1; row <= rows && rc == TURBO_OK; ++row)
    rc = flow_codec_csv_emit_row(adapter, flow, msg, doc, row);
  turbo_free_csv(&doc);
  return rc;
}

static int flow_codec_csv_source_reaches_stage(turbo_flow_t *flow, uint32_t source_index,
                                               uint32_t target_index) {
  turbo_vec_t reachable;
  int changed = 1;
  if (turbo_vec_init(&reachable, sizeof(uint8_t)) != TURBO_OK) return -1;
  if (turbo_vec_resize(&reachable, turbo_flow_stage_count(flow)) != TURBO_OK) {
    turbo_vec_destroy(&reachable);
    return -1;
  }
  memset(turbo_vec_data(&reachable), 0, turbo_vec_size(&reachable));
  *(uint8_t *)turbo_vec_at(&reachable, source_index) = 1;
  while (changed) {
    changed = 0;
    for (size_t i = 0; i < turbo_flow_edge_count(flow); ++i) {
      const turbo_flow_edge_plan_t *edge = turbo_flow_edge_at(flow, i);
      uint8_t *from = (uint8_t *)turbo_vec_at(&reachable, edge->from_stage);
      uint8_t *to = (uint8_t *)turbo_vec_at(&reachable, edge->to_stage);
      if (*from && !*to) {
        *to = 1;
        changed = 1;
      }
    }
  }
  changed = *(uint8_t *)turbo_vec_at(&reachable, target_index) != 0;
  turbo_vec_destroy(&reachable);
  return changed;
}

static int flow_codec_start(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_codec_adapter_t *adapter = (flow_codec_adapter_t *)ctx;

  (void)flow;
  if (!adapter || !stage) return TURBO_EINVAL;
  if (stage->is_source) return TURBO_EINVAL;

  switch (adapter->kind) {
  case FLOW_CODEC_LINE:
    if (!flow_codec_valid_line_delimiter(adapter->delimiter)) return TURBO_EINVAL;
    break;
  case FLOW_CODEC_LENGTH:
    if (!flow_codec_valid_length_prefix(adapter->prefix)) return TURBO_EINVAL;
    break;
  case FLOW_CODEC_DATABIND:
    if (!flow_codec_valid_databind_input(adapter->input_format)) return TURBO_EINVAL;
    if (!adapter->databind || !adapter->type_name || adapter->type_name[0] == '\0') {
      return TURBO_ENOTSUP;
    }
    if (!data_bind_schema_find_type(adapter->databind, adapter->type_name,
                                    &(DataBindSchemaType)DATA_BIND_SCHEMA_TYPE_INIT)) {
      return TURBO_ENOENT;
    }
    break;
  case FLOW_CODEC_CSV_SPLIT: {
    int source_index = turbo_flow_find_stage(flow, adapter->output_source);
    int stage_index = turbo_flow_find_stage(flow, stage->name);
    const turbo_flow_stage_plan_t *source;
    int reaches;
    if (source_index < 0 || stage_index < 0) return TURBO_ENOENT;
    source = turbo_flow_stage_at(flow, (size_t)source_index);
    if (!source || !source->is_source) return TURBO_EINVAL;
    reaches =
        flow_codec_csv_source_reaches_stage(flow, (uint32_t)source_index, (uint32_t)stage_index);
    if (reaches < 0) return TURBO_ENOMEM;
    if (reaches) return TURBO_ELOOP;
    break;
  }
  default:
    return TURBO_EINVAL;
  }

  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  return TURBO_OK;
}

static int flow_codec_consume(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                              turbo_flow_msg_t *msg) {
  flow_codec_adapter_t *adapter = (flow_codec_adapter_t *)ctx;
  int rc;

  (void)flow;
  (void)stage;
  if (!adapter) return TURBO_EINVAL;
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_EINVAL;
  rc = flow_codec_check_payload(adapter, msg);
  if (rc != TURBO_OK) return rc;

  switch (adapter->kind) {
  case FLOW_CODEC_LINE:
    return flow_codec_transform_line(adapter, msg);
  case FLOW_CODEC_LENGTH:
    return flow_codec_transform_length(adapter, msg);
  case FLOW_CODEC_DATABIND:
    return flow_codec_transform_databind(adapter, msg);
  case FLOW_CODEC_CSV_SPLIT:
    return flow_codec_transform_csv_split(adapter, flow, msg);
  default:
    return TURBO_EINVAL;
  }
}

static void flow_codec_stop(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_codec_adapter_t *adapter = (flow_codec_adapter_t *)ctx;

  (void)flow;
  (void)stage;
  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
}

static void flow_codec_shutdown(void *ctx) {
  flow_codec_adapter_t *adapter = (flow_codec_adapter_t *)ctx;

  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  data_bind_free(adapter->databind);
  if (adapter->databind_lock_initialized) turbo_mutex_destroy(&adapter->databind_lock);
  tstr_freep(&adapter->schema_path);
  tstr_freep(&adapter->schema_text);
  tstr_freep(&adapter->schema_name);
  tstr_freep(&adapter->type_name);
  tstr_freep(&adapter->xml_xpath);
  tstr_freep(&adapter->output_source);
  free(adapter);
}

static int flow_codec_register_adapter(turbo_flow_t *flow, const char *name,
                                       flow_codec_adapter_t *adapter,
                                       const turbo_flow_adapter_schema_t *schema) {
  turbo_flow_adapter_ops_t ops;
  int rc;

  if (!flow || !name || name[0] == '\0' || !adapter) return TURBO_EINVAL;
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_codec_start;
  ops.consume = flow_codec_consume;
  ops.stop = flow_codec_stop;
  ops.shutdown = flow_codec_shutdown;

  rc = turbo_flow_register_adapter_ex(flow, name, &ops, adapter, schema);
  if (rc != TURBO_OK) {
    flow_codec_shutdown(adapter);
    return rc;
  }
  return TURBO_OK;
}

int turbo_flow_codec_register_line_adapter(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_codec_line_config_t *config) {
  flow_codec_adapter_t *adapter;

  if (!flow || !name || name[0] == '\0') return TURBO_EINVAL;
  adapter = (flow_codec_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  atomic_init(&adapter->started, 0);
  adapter->kind = FLOW_CODEC_LINE;
  adapter->delimiter = TURBO_FLOW_CODEC_LINE_LF;
  adapter->strip_delimiter = 1;
  if (config) {
    adapter->max_payload_size = config->max_frame_size;
    adapter->delimiter = config->delimiter;
    adapter->strip_delimiter = config->strip_delimiter ? 1 : 0;
  }
  return flow_codec_register_adapter(flow, name, adapter, &FLOW_CODEC_LINE_SCHEMA);
}

int turbo_flow_codec_register_length_adapter(turbo_flow_t *flow, const char *name,
                                             const turbo_flow_codec_length_config_t *config) {
  flow_codec_adapter_t *adapter;

  if (!flow || !name || name[0] == '\0') return TURBO_EINVAL;
  adapter = (flow_codec_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  atomic_init(&adapter->started, 0);
  adapter->kind = FLOW_CODEC_LENGTH;
  adapter->prefix = TURBO_FLOW_CODEC_LENGTH_PREFIX_LE32;
  if (config) {
    adapter->max_payload_size = config->max_frame_size;
    adapter->prefix = config->prefix;
  }
  return flow_codec_register_adapter(flow, name, adapter, &FLOW_CODEC_LENGTH_SCHEMA);
}

int turbo_flow_codec_register_databind_adapter(turbo_flow_t *flow, const char *name,
                                               const turbo_flow_codec_databind_config_t *config) {
  flow_codec_adapter_t *adapter;
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindStatus status;
  int rc;

  if (!flow || !name || name[0] == '\0') return TURBO_EINVAL;
  adapter = (flow_codec_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  atomic_init(&adapter->started, 0);
  adapter->kind = FLOW_CODEC_DATABIND;
  adapter->input_format = TURBO_FLOW_CODEC_DATABIND_BIN;
  turbo_mutex_init(&adapter->databind_lock);
  adapter->databind_lock_initialized = 1;

  if (config) {
    int schema_path_set = config->schema_path && config->schema_path[0] != '\0';
    int schema_text_set = config->schema_text && config->schema_text[0] != '\0';
    if (schema_path_set == schema_text_set) {
      flow_codec_shutdown(adapter);
      return TURBO_EINVAL;
    }
    adapter->max_payload_size = config->max_payload_size;
    adapter->input_format = config->input_format;
    adapter->validate_only = config->validate_only ? 1 : 0;
    adapter->bind_all = config->bind_all ? 1 : 0;
    adapter->csv_row = config->csv_row;
    if (adapter->input_format == TURBO_FLOW_CODEC_DATABIND_XML && !adapter->validate_only &&
        !adapter->bind_all && config->xml_xpath && config->xml_xpath[0] != '\0') {
      flow_codec_shutdown(adapter);
      return TURBO_ENOTSUP;
    }
    rc = flow_codec_dup_opt(&adapter->schema_path, config->schema_path, 0);
    if (rc == TURBO_OK) {
      rc = flow_codec_dup_opt(&adapter->schema_text, config->schema_text, config->schema_text_len);
    }
    if (rc == TURBO_OK) rc = flow_codec_dup_opt(&adapter->type_name, config->type_name, 0);
    if (rc == TURBO_OK) rc = flow_codec_dup_opt(&adapter->xml_xpath, config->xml_xpath, 0);
    if (rc != TURBO_OK) {
      flow_codec_shutdown(adapter);
      return rc;
    }
    if (schema_path_set) {
      status = data_bind_create(adapter->schema_path, &adapter->databind, &error);
    } else {
      status = data_bind_create_from_text(adapter->schema_text, tstr_len(adapter->schema_text),
                                          &adapter->databind, &error);
    }
    rc = flow_codec_databind_status_to_turbo(status);
    if (rc != TURBO_OK) {
      flow_codec_shutdown(adapter);
      return rc;
    }
    {
      const char *schema_name = data_bind_schema_name(adapter->databind);
      if (!schema_name || schema_name[0] == '\0') {
        schema_name = schema_path_set ? adapter->schema_path : adapter->type_name;
      }
      rc = flow_codec_dup_opt(&adapter->schema_name, schema_name, 0);
      if (rc != TURBO_OK) {
        flow_codec_shutdown(adapter);
        return rc;
      }
      adapter->data_schema.size = sizeof(adapter->data_schema);
      adapter->data_schema.domain = TURBO_FLOW_DOMAIN_DATA;
      adapter->data_schema.encoding = flow_codec_databind_encoding(adapter->input_format);
      adapter->data_schema.schema_name = adapter->schema_name;
      adapter->data_schema.type_name = adapter->type_name;
      adapter->data_schema.projection_type = FLOW_DATABIND_PROJECTION_TYPE;
      adapter->data_schema.schema_version = 1u;
      adapter->data_schema.schema_text = adapter->schema_text;
    }
  }

  return flow_codec_register_adapter(flow, name, adapter, &FLOW_DATABIND_SCHEMA);
}

int turbo_flow_codec_register_csv_splitter_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_codec_csv_split_config_t *config) {
  flow_codec_adapter_t *adapter;
  int rc;
  if (!flow || !name || !*name || !config || !config->output_source || !*config->output_source)
    return TURBO_EINVAL;
  adapter = (flow_codec_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  atomic_init(&adapter->started, 0);
  adapter->kind = FLOW_CODEC_CSV_SPLIT;
  adapter->csv_delimiter = ',';
  adapter->csv_quote = '"';
  adapter->max_rows = config->max_rows ? config->max_rows : FLOW_CODEC_CSV_DEFAULT_MAX_ROWS;
  adapter->max_payload_size =
      config->max_payload_size ? config->max_payload_size : FLOW_CODEC_CSV_DEFAULT_MAX_PAYLOAD;
  adapter->max_row_size =
      config->max_row_size ? config->max_row_size : FLOW_CODEC_CSV_DEFAULT_MAX_ROW;
  rc = flow_codec_dup_opt(&adapter->output_source, config->output_source, 0);
  if (rc != TURBO_OK) {
    flow_codec_shutdown(adapter);
    return rc;
  }
  return flow_codec_register_adapter(flow, name, adapter, &FLOW_CSV_SPLIT_SCHEMA);
}
