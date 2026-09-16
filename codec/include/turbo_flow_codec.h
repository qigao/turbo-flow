#ifndef TURBO_FLOW_CODEC_H
#define TURBO_FLOW_CODEC_H

#include "data_bind.h"
#include "turbo_flow.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum turbo_flow_codec_line_delimiter_e {
  TURBO_FLOW_CODEC_LINE_LF = 0,
  TURBO_FLOW_CODEC_LINE_CRLF
} turbo_flow_codec_line_delimiter_t;

typedef enum turbo_flow_codec_length_prefix_e {
  TURBO_FLOW_CODEC_LENGTH_PREFIX_LE32 = 0,
  TURBO_FLOW_CODEC_LENGTH_PREFIX_LE64,
  TURBO_FLOW_CODEC_LENGTH_PREFIX_BE32,
  TURBO_FLOW_CODEC_LENGTH_PREFIX_BE64
} turbo_flow_codec_length_prefix_t;

typedef enum turbo_flow_codec_databind_input_e {
  TURBO_FLOW_CODEC_DATABIND_BIN = 0,
  TURBO_FLOW_CODEC_DATABIND_JSON,
  TURBO_FLOW_CODEC_DATABIND_CSV,
  TURBO_FLOW_CODEC_DATABIND_XML
} turbo_flow_codec_databind_input_t;

typedef struct turbo_flow_codec_line_config_s {
  /** Maximum accepted frame payload size in bytes; 0 leaves it unbounded. */
  size_t max_frame_size;
  /** TURBO_FLOW_CODEC_LINE_LF or TURBO_FLOW_CODEC_LINE_CRLF. */
  int delimiter;
  /** Non-zero removes the delimiter from the message payload. */
  int strip_delimiter;
} turbo_flow_codec_line_config_t;

typedef struct turbo_flow_codec_length_config_s {
  /** Maximum accepted frame payload size in bytes; 0 leaves it unbounded. */
  size_t max_frame_size;
  /** TURBO_FLOW_CODEC_LENGTH_PREFIX_* prefix format. */
  int prefix;
} turbo_flow_codec_length_config_t;

typedef struct turbo_flow_codec_databind_config_s {
  /** Trusted TBE schema path. Exactly one of schema_path or schema_text is required. */
  const char *schema_path;
  /** Trusted TBE schema text. Exactly one of schema_path or schema_text is required. */
  const char *schema_text;
  /** Schema text length. If 0 and schema_text is set, strlen(schema_text) is used. */
  size_t schema_text_len;
  /** TBE message/composite/group type name to bind. Required. */
  const char *type_name;
  /** TURBO_FLOW_CODEC_DATABIND_* input format: bin, json, csv, or xml. */
  int input_format;
  /** Non-zero validates and leaves message content metadata untouched. */
  int validate_only;
  /** Non-zero binds all records/items when DataBind exposes a *_all API. */
  int bind_all;
  /** CSV data row index used when bind_all is zero. */
  size_t csv_row;
  /** Optional XPath for XML validation and bind_all; single-value bind rejects it. */
  const char *xml_xpath;
  /** Maximum accepted input payload size in bytes; 0 leaves it unbounded. */
  size_t max_payload_size;
} turbo_flow_codec_databind_config_t;

typedef struct turbo_flow_codec_csv_split_config_s {
  /** Existing source stage that receives one canonical header+row CSV message per data row. */
  const char *output_source;
  /** Maximum data rows emitted from one input; zero uses 10000. */
  size_t max_rows;
  /** Maximum input payload bytes; zero uses 16 MiB. */
  size_t max_payload_size;
  /** Maximum canonical header+row payload bytes; zero uses 1 MiB. */
  size_t max_row_size;
} turbo_flow_codec_csv_split_config_t;

/**
 * Register a line framing adapter for DSL `stage <name> adapter "<adapter>"`.
 *
 * The current turbo_flow stage contract is one input message to one downstream
 * message, so payloads containing more than one complete frame return
 * SALTS_ENOTSUP instead of silently dropping trailing records.
 */
TURBO_FLOW_C_API int turbo_flow_codec_register_line_adapter(turbo_flow_t *flow, const char *name,
                                                     const turbo_flow_codec_line_config_t *config);

/**
 * Register a length-prefixed framing adapter for DSL `stage <name> adapter "<adapter>"`.
 *
 * The adapter strips a single configured length prefix and replaces the payload
 * with that one frame body. Partial frames return SALTS_EPROTO.
 */
TURBO_FLOW_C_API int
turbo_flow_codec_register_length_adapter(turbo_flow_t *flow, const char *name,
                                         const turbo_flow_codec_length_config_t *config);

/**
 * Register a schema-driven DataBind adapter for DSL `stage <name> adapter "<adapter>"`.
 *
 * When validate_only is zero, the original payload remains unchanged and the
 * bound DataBindValue is attached as a schema-bound projection owned by the
 * message envelope. Downstream stages can use
 * turbo_flow_codec_msg_databind_value() to retrieve it. Message clone, retry,
 * and other ownership boundaries use DataBind's deep-clone API, so each cloned
 * envelope owns an independent value tree.
 */
TURBO_FLOW_C_API int
turbo_flow_codec_register_databind_adapter(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_codec_databind_config_t *config);

/**
 * Register a terminal CSV splitter sink.
 *
 * Every parsed data row is normalized to a two-line CSV payload containing the
 * original header and that row, then published through `output_source`. The
 * current Salts DataBind CSV DOM accepts comma delimiters and double-quote escaping.
 * Emission is ordered but non-atomic: when publication fails, the already
 * published prefix remains committed and the input operation returns that error.
 */
TURBO_FLOW_C_API int
turbo_flow_codec_register_csv_splitter_adapter(turbo_flow_t *flow, const char *name,
                                               const turbo_flow_codec_csv_split_config_t *config);

/** Return the DataBindValue projection attached by this module, or NULL for opaque content. */
TURBO_FLOW_C_API const DataBindValue *turbo_flow_codec_msg_databind_value(const turbo_flow_msg_t *msg);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_CODEC_H */
