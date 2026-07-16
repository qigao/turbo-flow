#ifndef FLOWIE_MQTT_INTERNAL_H
#define FLOWIE_MQTT_INTERNAL_H

#include "flowie_mqtt_protocol.h"

typedef enum flowie_mqtt_lexer_state_e {
  FLOWIE_MQTT_LEX_FIXED_HEADER = 0,
  FLOWIE_MQTT_LEX_REMAINING_LENGTH,
  FLOWIE_MQTT_LEX_BODY,
  FLOWIE_MQTT_LEX_DONE
} flowie_mqtt_lexer_state_t;

enum { FLOWIE_MQTT_LEXER_NEED_MORE = -100 };

typedef struct flowie_mqtt_token_s {
  uint32_t integer;
  flowie_mqtt_span_t span;
  size_t offset;
} flowie_mqtt_token_t;

typedef struct flowie_mqtt_lexer_s {
  const uint8_t *input;
  const uint8_t *cursor;
  const uint8_t *limit;
  flowie_mqtt_lexer_state_t state;
  uint32_t remaining_length;
  size_t remaining_length_bytes;
} flowie_mqtt_lexer_t;

typedef struct flowie_mqtt_parse_ctx_s {
  const uint8_t *input;
  flowie_mqtt_version_t version;
  flowie_mqtt_packet_view_t packet;
  int accepted;
  flowie_mqtt_parse_result_t code;
  size_t error_offset;
  const char *message;
} flowie_mqtt_parse_ctx_t;

void flowie_mqtt_lexer_init(flowie_mqtt_lexer_t *lexer, const uint8_t *bytes, size_t byte_count);
int flowie_mqtt_lexer_next(flowie_mqtt_lexer_t *lexer, flowie_mqtt_token_t *token,
                           flowie_mqtt_version_t version);
void flowie_mqtt_parser_accept(flowie_mqtt_parse_ctx_t *ctx, flowie_mqtt_token_t header,
                               flowie_mqtt_token_t remaining, flowie_mqtt_token_t body);
void flowie_mqtt_parser_fail(flowie_mqtt_parse_ctx_t *ctx, flowie_mqtt_token_t token);

#endif /* FLOWIE_MQTT_INTERNAL_H */
