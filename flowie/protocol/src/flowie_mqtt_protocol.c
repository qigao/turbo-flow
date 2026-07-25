#include "flowie_mqtt_grammar_gen.h"
#include "flowie_mqtt_internal.h"

#include <stdlib.h>
#include <string.h>

void *FlowieMqttParseAlloc(void *(*malloc_proc)(size_t));
void FlowieMqttParse(void *parser, int token_id, flowie_mqtt_token_t token,
                     flowie_mqtt_parse_ctx_t *ctx);
void FlowieMqttParseFree(void *parser, void (*free_proc)(void *));

static void flowie_mqtt_error(flowie_mqtt_parse_error_t *error, flowie_mqtt_parse_result_t code,
                              size_t offset, const char *message) {
  if (!error || error->size < sizeof(*error)) return;
  error->code = code;
  error->offset = offset;
  error->message = message;
}

void flowie_mqtt_parser_accept(flowie_mqtt_parse_ctx_t *ctx, flowie_mqtt_token_t header,
                               flowie_mqtt_token_t remaining, flowie_mqtt_token_t body) {
  size_t fixed_header_size;
  uint8_t packet_type;
  if (!ctx || ctx->code != FLOWIE_MQTT_PARSE_OK) return;
  fixed_header_size = 1u + remaining.span.size;
  if ((remaining.integer == 0u && body.span.size != 0u) || (remaining.integer != body.span.size)) {
    ctx->code = FLOWIE_MQTT_PARSE_MALFORMED;
    ctx->error_offset = body.offset;
    ctx->message = "packet body length does not match Remaining Length";
    return;
  }
  packet_type = (uint8_t)(header.integer >> 4u);
  if ((packet_type == FLOWIE_MQTT_PACKET_PINGREQ ||
       packet_type == FLOWIE_MQTT_PACKET_PINGRESP) && remaining.integer != 0u) {
    ctx->code = FLOWIE_MQTT_PARSE_PROTOCOL_ERROR;
    ctx->error_offset = remaining.offset;
    ctx->message = "PING packet must have zero Remaining Length";
    return;
  }
  ctx->packet = (flowie_mqtt_packet_view_t)FLOWIE_MQTT_PACKET_VIEW_INIT;
  ctx->packet.version = ctx->version;
  ctx->packet.type = (flowie_mqtt_packet_type_t)(header.integer >> 4u);
  ctx->packet.flags = (uint8_t)(header.integer & 0x0fu);
  ctx->packet.remaining_length = remaining.integer;
  ctx->packet.fixed_header_size = fixed_header_size;
  ctx->packet.packet.data = ctx->input;
  ctx->packet.packet.size = fixed_header_size + body.span.size;
  ctx->packet.body = body.span;
  if (remaining.integer == 0u) ctx->packet.body.data = ctx->input + fixed_header_size;
  ctx->accepted = 1;
}

void flowie_mqtt_parser_fail(flowie_mqtt_parse_ctx_t *ctx, flowie_mqtt_token_t token) {
  if (!ctx || ctx->code != FLOWIE_MQTT_PARSE_OK) return;
  ctx->code = FLOWIE_MQTT_PARSE_MALFORMED;
  ctx->error_offset = token.offset;
  ctx->message = "invalid MQTT packet token sequence";
}

int flowie_mqtt_packet_parse(const uint8_t *bytes, size_t byte_count,
                             const flowie_mqtt_parse_options_t *options,
                             flowie_mqtt_packet_view_t *out, size_t *consumed,
                             flowie_mqtt_parse_error_t *error) {
  flowie_mqtt_parse_options_t defaults = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_parse_ctx_t ctx;
  flowie_mqtt_lexer_t lexer;
  flowie_mqtt_token_t token;
  void *parser;
  size_t max_packet_size;
  int token_id;
  if (consumed) *consumed = 0u;
  if (error && error->size >= sizeof(*error)) {
    error->code = FLOWIE_MQTT_PARSE_OK;
    error->offset = 0u;
    error->message = NULL;
  }
  if ((!bytes && byte_count != 0u) || !out || out->size < sizeof(*out) ||
      (options &&
       (options->size < sizeof(*options) || options->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1))) {
    flowie_mqtt_error(error, FLOWIE_MQTT_PARSE_INVALID_ARGUMENT, 0u, "invalid parser ABI");
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  }
  if (!options) options = &defaults;
  if (options->version != FLOWIE_MQTT_VERSION_UNSPECIFIED &&
      !flowie_mqtt_version_is_supported(options->version)) {
    flowie_mqtt_error(error, FLOWIE_MQTT_PARSE_INVALID_ARGUMENT, 0u, "unsupported MQTT version");
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  }
  max_packet_size =
      options->max_packet_size ? options->max_packet_size : FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE;
  if (max_packet_size > FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE) {
    flowie_mqtt_error(error, FLOWIE_MQTT_PARSE_INVALID_ARGUMENT, 0u,
                      "packet limit exceeds MQTT maximum");
    return FLOWIE_MQTT_PARSE_INVALID_ARGUMENT;
  }
  memset(&ctx, 0, sizeof(ctx));
  ctx.input = bytes;
  ctx.version = options->version;
  ctx.code = FLOWIE_MQTT_PARSE_OK;
  parser = FlowieMqttParseAlloc(malloc);
  if (!parser) {
    flowie_mqtt_error(error, FLOWIE_MQTT_PARSE_NO_MEMORY, 0u, "parser allocation failed");
    return FLOWIE_MQTT_PARSE_NO_MEMORY;
  }
  flowie_mqtt_lexer_init(&lexer, bytes, byte_count);
  for (;;) {
    token_id = flowie_mqtt_lexer_next(&lexer, &token, options->version);
    if (token_id == FLOWIE_MQTT_LEXER_NEED_MORE) {
      FlowieMqttParseFree(parser, free);
      flowie_mqtt_error(error, FLOWIE_MQTT_PARSE_NEED_MORE, byte_count, "incomplete MQTT packet");
      return FLOWIE_MQTT_PARSE_NEED_MORE;
    }
    if (token_id < 0) {
      FlowieMqttParseFree(parser, free);
      flowie_mqtt_error(error, (flowie_mqtt_parse_result_t)token_id, token.offset,
                        token_id == FLOWIE_MQTT_PARSE_PROTOCOL_ERROR
                            ? "invalid MQTT fixed header"
                            : "malformed MQTT Remaining Length");
      return token_id;
    }
    if (token_id == 0) break;
    if (token_id == FLOWIE_MQTT_TOKEN_REMAINING_LENGTH &&
        1u + token.span.size + (size_t)token.integer > max_packet_size) {
      FlowieMqttParseFree(parser, free);
      flowie_mqtt_error(error, FLOWIE_MQTT_PARSE_TOO_LARGE, token.offset,
                        "MQTT packet exceeds configured limit");
      return FLOWIE_MQTT_PARSE_TOO_LARGE;
    }
    FlowieMqttParse(parser, token_id, token, &ctx);
    if (ctx.code != FLOWIE_MQTT_PARSE_OK) break;
  }
  memset(&token, 0, sizeof(token));
  if (ctx.code == FLOWIE_MQTT_PARSE_OK) FlowieMqttParse(parser, 0, token, &ctx);
  FlowieMqttParseFree(parser, free);
  if (ctx.code != FLOWIE_MQTT_PARSE_OK || !ctx.accepted) {
    flowie_mqtt_parse_result_t code =
        ctx.code != FLOWIE_MQTT_PARSE_OK ? ctx.code : FLOWIE_MQTT_PARSE_MALFORMED;
    flowie_mqtt_error(error, code, ctx.error_offset,
                      ctx.message ? ctx.message : "MQTT packet was not accepted");
    return code;
  }
  *out = ctx.packet;
  if (consumed) *consumed = ctx.packet.packet.size;
  return FLOWIE_MQTT_PARSE_OK;
}
