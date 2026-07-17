#include "turbo_flow_fmq_control.h"

#include "turbo_error.h"

#include <limits.h>
#include <string.h>

static const uint8_t FLOW_FMQ_CONTROL_REQUEST_MAGIC[4] = {'T', 'F', 'C', 'Q'};
static const uint8_t FLOW_FMQ_CONTROL_REPLY_MAGIC[4] = {'T', 'F', 'C', 'P'};

static void flow_control_write_u16(uint8_t *out, uint16_t value) {
  out[0] = (uint8_t)(value >> 8u);
  out[1] = (uint8_t)value;
}

static void flow_control_write_u32(uint8_t *out, uint32_t value) {
  out[0] = (uint8_t)(value >> 24u);
  out[1] = (uint8_t)(value >> 16u);
  out[2] = (uint8_t)(value >> 8u);
  out[3] = (uint8_t)value;
}

static void flow_control_write_u64(uint8_t *out, uint64_t value) {
  for (size_t i = 0u; i < 8u; ++i)
    out[i] = (uint8_t)(value >> (56u - 8u * i));
}

static uint16_t flow_control_read_u16(const uint8_t *data) {
  return (uint16_t)(((uint16_t)data[0] << 8u) | data[1]);
}

static uint32_t flow_control_read_u32(const uint8_t *data) {
  return ((uint32_t)data[0] << 24u) | ((uint32_t)data[1] << 16u) | ((uint32_t)data[2] << 8u) |
         data[3];
}

static uint64_t flow_control_read_u64(const uint8_t *data) {
  uint64_t value = 0u;
  for (size_t i = 0u; i < 8u; ++i)
    value = (value << 8u) | data[i];
  return value;
}

static int flow_control_text_length(const char *text, size_t capacity, int required,
                                    size_t *length) {
  const char *end;
  if (!text || !length) return TURBO_EINVAL;
  end = (const char *)memchr(text, '\0', capacity);
  if (!end) return TURBO_EINVAL;
  *length = (size_t)(end - text);
  return required && *length == 0u ? TURBO_EINVAL : TURBO_OK;
}

static int flow_control_command_lengths(const turbo_flow_fmq_control_command_t *command,
                                        size_t lengths[4]) {
  if (!command || command->size < sizeof(*command) || command->kind < TURBO_FLOW_CONTROL_PAUSE ||
      command->kind > TURBO_FLOW_CONTROL_ADAPTER ||
      flow_control_text_length(command->target, sizeof(command->target),
                               command->kind == TURBO_FLOW_CONTROL_RESIZE_POOL ||
                                   command->kind == TURBO_FLOW_CONTROL_ADAPTER,
                               &lengths[0]) != TURBO_OK ||
      flow_control_text_length(command->condition, sizeof(command->condition), 0, &lengths[1]) !=
          TURBO_OK ||
      flow_control_text_length(command->endpoint_host, sizeof(command->endpoint_host), 0,
                               &lengths[2]) != TURBO_OK ||
      flow_control_text_length(command->endpoint_path, sizeof(command->endpoint_path), 0,
                               &lengths[3]) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  if (command->kind == TURBO_FLOW_CONTROL_RESIZE_POOL &&
      (command->parallelism == 0u || command->pool_kind < TURBO_FLOW_POOL_THREAD ||
       command->pool_kind > TURBO_FLOW_POOL_DISRUPTOR || command->adapter_kind != 0 ||
       command->endpoint_port != 0 || lengths[2] != 0u || lengths[3] != 0u)) {
    return TURBO_EINVAL;
  }
  if (command->kind == TURBO_FLOW_CONTROL_ADAPTER &&
      (command->adapter_kind < TURBO_FLOW_ADAPTER_QUIESCE ||
       command->adapter_kind > TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT)) {
    return TURBO_EINVAL;
  }
  if (command->kind == TURBO_FLOW_CONTROL_ADAPTER &&
      command->adapter_kind == TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT &&
      (lengths[2] == 0u || command->endpoint_port <= 0 || command->endpoint_port > 65535 ||
       command->pool_kind != 0 || command->parallelism != 0u || command->timeout_ms != 0u)) {
    return TURBO_EINVAL;
  }
  if (command->kind == TURBO_FLOW_CONTROL_ADAPTER &&
      command->adapter_kind != TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT &&
      (command->pool_kind != 0 || command->parallelism != 0u || command->timeout_ms != 0u ||
       command->endpoint_port != 0 || lengths[2] != 0u || lengths[3] != 0u))
    return TURBO_EINVAL;
  if ((command->kind == TURBO_FLOW_CONTROL_PAUSE || command->kind == TURBO_FLOW_CONTROL_RESUME ||
       command->kind == TURBO_FLOW_CONTROL_DRAIN) &&
      (lengths[0] != 0u || command->pool_kind != 0 || command->parallelism != 0u ||
       command->adapter_kind != 0 || command->endpoint_port != 0 || lengths[2] != 0u ||
       lengths[3] != 0u ||
       (command->kind != TURBO_FLOW_CONTROL_DRAIN && command->timeout_ms != 0u)))
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_control_request_lengths(const turbo_flow_fmq_control_request_t *request,
                                        size_t lengths[6]) {
  if (!request || request->size < sizeof(*request) ||
      request->version != TURBO_FLOW_FMQ_CONTROL_PROTOCOL_VERSION || request->request_id == 0u ||
      request->operation < TURBO_FLOW_FMQ_CONTROL_STATUS ||
      request->operation > TURBO_FLOW_FMQ_CONTROL_EXECUTE ||
      flow_control_text_length(request->target, sizeof(request->target), 1, &lengths[0]) !=
          TURBO_OK ||
      flow_control_text_length(request->idempotency_key, sizeof(request->idempotency_key),
                               request->operation == TURBO_FLOW_FMQ_CONTROL_EXECUTE,
                               &lengths[1]) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  memset(lengths + 2, 0, 4u * sizeof(*lengths));
  if (request->operation == TURBO_FLOW_FMQ_CONTROL_STATUS) {
    return request->idempotency_key[0] == '\0' && request->command.kind == 0 &&
                   request->command.timeout_ms == 0u && request->command.target[0] == '\0' &&
                   request->command.pool_kind == 0 && request->command.parallelism == 0u &&
                   request->command.adapter_kind == 0 && request->command.endpoint_port == 0 &&
                   request->command.condition[0] == '\0' &&
                   request->command.endpoint_host[0] == '\0' &&
                   request->command.endpoint_path[0] == '\0'
               ? TURBO_OK
               : TURBO_EINVAL;
  }
  return flow_control_command_lengths(&request->command, lengths + 2);
}

int turbo_flow_fmq_control_request_encode(const turbo_flow_fmq_control_request_t *request,
                                          uint8_t *out, size_t capacity, size_t *out_len) {
  size_t lengths[6];
  size_t required = TURBO_FLOW_FMQ_CONTROL_REQUEST_HEADER_SIZE;
  size_t cursor;
  if (!out_len) return TURBO_EINVAL;
  *out_len = 0u;
  if (flow_control_request_lengths(request, lengths) != TURBO_OK) return TURBO_EINVAL;
  for (size_t i = 0u; i < 6u; ++i)
    required += lengths[i];
  *out_len = required;
  if (!out || capacity < required) return TURBO_ENOSPC;
  memcpy(out, FLOW_FMQ_CONTROL_REQUEST_MAGIC, sizeof(FLOW_FMQ_CONTROL_REQUEST_MAGIC));
  flow_control_write_u16(out + 4u, TURBO_FLOW_FMQ_CONTROL_PROTOCOL_VERSION);
  flow_control_write_u16(out + 6u, TURBO_FLOW_FMQ_CONTROL_REQUEST_HEADER_SIZE);
  flow_control_write_u16(out + 8u, (uint16_t)request->operation);
  flow_control_write_u16(out + 10u, request->operation == TURBO_FLOW_FMQ_CONTROL_EXECUTE
                                        ? (uint16_t)request->command.kind
                                        : 0u);
  flow_control_write_u16(out + 12u, request->operation == TURBO_FLOW_FMQ_CONTROL_EXECUTE
                                        ? (uint16_t)request->command.pool_kind
                                        : 0u);
  flow_control_write_u16(out + 14u, request->operation == TURBO_FLOW_FMQ_CONTROL_EXECUTE &&
                                            request->command.kind == TURBO_FLOW_CONTROL_ADAPTER
                                        ? (uint16_t)request->command.adapter_kind
                                        : 0u);
  flow_control_write_u32(out + 16u, request->operation == TURBO_FLOW_FMQ_CONTROL_EXECUTE
                                        ? request->command.parallelism
                                        : 0u);
  flow_control_write_u32(out + 20u, request->operation == TURBO_FLOW_FMQ_CONTROL_EXECUTE &&
                                            request->command.kind == TURBO_FLOW_CONTROL_ADAPTER
                                        ? (uint32_t)request->command.endpoint_port
                                        : 0u);
  flow_control_write_u64(out + 24u, request->request_id);
  flow_control_write_u64(out + 32u, request->operation == TURBO_FLOW_FMQ_CONTROL_EXECUTE
                                        ? request->command.timeout_ms
                                        : 0u);
  for (size_t i = 0u; i < 6u; ++i)
    flow_control_write_u16(out + 40u + 2u * i, (uint16_t)lengths[i]);
  memset(out + 52u, 0, 12u);
  cursor = TURBO_FLOW_FMQ_CONTROL_REQUEST_HEADER_SIZE;
#define COPY_FIELD(field, index)                                                                   \
  do {                                                                                             \
    if (lengths[index] > 0u) memcpy(out + cursor, field, lengths[index]);                          \
    cursor += lengths[index];                                                                      \
  } while (0)
  COPY_FIELD(request->target, 0u);
  COPY_FIELD(request->idempotency_key, 1u);
  COPY_FIELD(request->command.target, 2u);
  COPY_FIELD(request->command.condition, 3u);
  COPY_FIELD(request->command.endpoint_host, 4u);
  COPY_FIELD(request->command.endpoint_path, 5u);
#undef COPY_FIELD
  return TURBO_OK;
}

int turbo_flow_fmq_control_request_decode(const uint8_t *data, size_t data_len,
                                          turbo_flow_fmq_control_request_t *out) {
  turbo_flow_fmq_control_request_t decoded = TURBO_FLOW_FMQ_CONTROL_REQUEST_INIT;
  size_t lengths[6];
  size_t expected = TURBO_FLOW_FMQ_CONTROL_REQUEST_HEADER_SIZE;
  size_t cursor;
  if (!data || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  if (data_len < TURBO_FLOW_FMQ_CONTROL_REQUEST_HEADER_SIZE ||
      memcmp(data, FLOW_FMQ_CONTROL_REQUEST_MAGIC, sizeof(FLOW_FMQ_CONTROL_REQUEST_MAGIC)) != 0 ||
      flow_control_read_u16(data + 4u) != TURBO_FLOW_FMQ_CONTROL_PROTOCOL_VERSION ||
      flow_control_read_u16(data + 6u) != TURBO_FLOW_FMQ_CONTROL_REQUEST_HEADER_SIZE) {
    return TURBO_EPROTO;
  }
  for (size_t i = 0u; i < 6u; ++i) {
    lengths[i] = flow_control_read_u16(data + 40u + 2u * i);
    if (SIZE_MAX - expected < lengths[i]) return TURBO_EPROTO;
    expected += lengths[i];
  }
  if (data_len != expected || memcmp(data + 52u, "\0\0\0\0\0\0\0\0\0\0\0\0", 12u) != 0 ||
      lengths[0] == 0u || lengths[0] > TURBO_FLOW_FMQ_CONTROL_TARGET_MAX ||
      lengths[1] > TURBO_FLOW_RESOURCE_COMMAND_KEY_MAX ||
      lengths[2] > TURBO_FLOW_CONTROL_NAME_MAX || lengths[3] > TURBO_FLOW_CONTROL_EXPR_MAX ||
      lengths[4] > TURBO_FLOW_ENDPOINT_MAX || lengths[5] > TURBO_FLOW_ENDPOINT_MAX) {
    return TURBO_EPROTO;
  }
  decoded.operation = (turbo_flow_fmq_control_operation_t)flow_control_read_u16(data + 8u);
  decoded.request_id = flow_control_read_u64(data + 24u);
  decoded.command.size = sizeof(decoded.command);
  decoded.command.kind = (turbo_flow_control_kind_t)flow_control_read_u16(data + 10u);
  decoded.command.pool_kind = (turbo_flow_pool_kind_t)flow_control_read_u16(data + 12u);
  decoded.command.adapter_kind =
      (turbo_flow_adapter_command_kind_t)flow_control_read_u16(data + 14u);
  decoded.command.parallelism = flow_control_read_u32(data + 16u);
  decoded.command.endpoint_port = (int)flow_control_read_u32(data + 20u);
  decoded.command.timeout_ms = flow_control_read_u64(data + 32u);
  cursor = TURBO_FLOW_FMQ_CONTROL_REQUEST_HEADER_SIZE;
#define READ_FIELD(field, index)                                                                   \
  do {                                                                                             \
    if (lengths[index] > 0u && memchr(data + cursor, '\0', lengths[index])) return TURBO_EPROTO;   \
    if (lengths[index] > 0u) memcpy(field, data + cursor, lengths[index]);                         \
    field[lengths[index]] = '\0';                                                                  \
    cursor += lengths[index];                                                                      \
  } while (0)
  READ_FIELD(decoded.target, 0u);
  READ_FIELD(decoded.idempotency_key, 1u);
  READ_FIELD(decoded.command.target, 2u);
  READ_FIELD(decoded.command.condition, 3u);
  READ_FIELD(decoded.command.endpoint_host, 4u);
  READ_FIELD(decoded.command.endpoint_path, 5u);
#undef READ_FIELD
  if (flow_control_request_lengths(&decoded, lengths) != TURBO_OK) return TURBO_EPROTO;
  *out = decoded;
  return TURBO_OK;
}

int turbo_flow_fmq_control_reply_encode(const turbo_flow_fmq_control_reply_t *reply, uint8_t *out,
                                        size_t capacity, size_t *out_len) {
  size_t message_len;
  size_t required;
  uint32_t flags;
  if (!out_len) return TURBO_EINVAL;
  *out_len = 0u;
  if (!reply || reply->size < sizeof(*reply) ||
      reply->version != TURBO_FLOW_FMQ_CONTROL_PROTOCOL_VERSION ||
      (reply->request_id == 0u && reply->status != TURBO_EPROTO &&
       reply->status != TURBO_EMSGSIZE) ||
      !memchr(reply->error.message, '\0', sizeof(reply->error.message))) {
    return TURBO_EINVAL;
  }
  message_len = strlen(reply->error.message);
  if (message_len > TURBO_FLOW_FMQ_CONTROL_REPLY_MESSAGE_MAX) return TURBO_EINVAL;
  required = TURBO_FLOW_FMQ_CONTROL_REPLY_HEADER_SIZE + message_len;
  *out_len = required;
  if (!out || capacity < required) return TURBO_ENOSPC;
  flags = reply->replayed ? 1u : 0u;
  memcpy(out, FLOW_FMQ_CONTROL_REPLY_MAGIC, sizeof(FLOW_FMQ_CONTROL_REPLY_MAGIC));
  flow_control_write_u16(out + 4u, TURBO_FLOW_FMQ_CONTROL_PROTOCOL_VERSION);
  flow_control_write_u16(out + 6u, TURBO_FLOW_FMQ_CONTROL_REPLY_HEADER_SIZE);
  flow_control_write_u64(out + 8u, reply->request_id);
  flow_control_write_u32(out + 16u, (uint32_t)(int32_t)reply->status);
  flow_control_write_u32(out + 20u, flags);
  flow_control_write_u32(out + 24u, (uint32_t)reply->runtime.state);
  flow_control_write_u32(out + 28u, reply->runtime.accepting_publishes ? 1u : 0u);
  flow_control_write_u32(out + 32u, reply->runtime.active_publishes);
  flow_control_write_u32(out + 36u, 0u);
  flow_control_write_u64(out + 40u, (uint64_t)reply->runtime.stage_count);
  flow_control_write_u64(out + 48u, (uint64_t)reply->runtime.edge_count);
  flow_control_write_u64(out + 56u, (uint64_t)reply->runtime.adapter_count);
  flow_control_write_u64(out + 64u, (uint64_t)reply->runtime.pool_count);
  flow_control_write_u32(out + 72u, reply->error.line);
  flow_control_write_u32(out + 76u, reply->error.column);
  flow_control_write_u16(out + 80u, (uint16_t)message_len);
  flow_control_write_u16(out + 82u, 0u);
  flow_control_write_u32(out + 84u, 0u);
  if (message_len > 0u)
    memcpy(out + TURBO_FLOW_FMQ_CONTROL_REPLY_HEADER_SIZE, reply->error.message, message_len);
  return TURBO_OK;
}

int turbo_flow_fmq_control_reply_decode(const uint8_t *data, size_t data_len,
                                        turbo_flow_fmq_control_reply_t *out) {
  turbo_flow_fmq_control_reply_t decoded = TURBO_FLOW_FMQ_CONTROL_REPLY_INIT;
  size_t message_len;
  if (!data || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  if (data_len < TURBO_FLOW_FMQ_CONTROL_REPLY_HEADER_SIZE ||
      memcmp(data, FLOW_FMQ_CONTROL_REPLY_MAGIC, sizeof(FLOW_FMQ_CONTROL_REPLY_MAGIC)) != 0 ||
      flow_control_read_u16(data + 4u) != TURBO_FLOW_FMQ_CONTROL_PROTOCOL_VERSION ||
      flow_control_read_u16(data + 6u) != TURBO_FLOW_FMQ_CONTROL_REPLY_HEADER_SIZE ||
      flow_control_read_u32(data + 36u) != 0u || flow_control_read_u16(data + 82u) != 0u ||
      flow_control_read_u32(data + 84u) != 0u || (flow_control_read_u32(data + 20u) & ~1u) != 0u ||
      flow_control_read_u32(data + 28u) > 1u) {
    return TURBO_EPROTO;
  }
  message_len = flow_control_read_u16(data + 80u);
  if (message_len > TURBO_FLOW_FMQ_CONTROL_REPLY_MESSAGE_MAX ||
      data_len != TURBO_FLOW_FMQ_CONTROL_REPLY_HEADER_SIZE + message_len ||
      memchr(data + TURBO_FLOW_FMQ_CONTROL_REPLY_HEADER_SIZE, '\0', message_len)) {
    return TURBO_EPROTO;
  }
  decoded.request_id = flow_control_read_u64(data + 8u);
  decoded.status = (int)(int32_t)flow_control_read_u32(data + 16u);
  if (decoded.request_id == 0u && decoded.status != TURBO_EPROTO &&
      decoded.status != TURBO_EMSGSIZE)
    return TURBO_EPROTO;
  decoded.replayed = (flow_control_read_u32(data + 20u) & 1u) != 0u;
  decoded.runtime.state = (turbo_flow_state_t)flow_control_read_u32(data + 24u);
  if (decoded.runtime.state < TURBO_FLOW_STATE_NEW ||
      decoded.runtime.state > TURBO_FLOW_STATE_FAILED)
    return TURBO_EPROTO;
  decoded.runtime.accepting_publishes = (int)flow_control_read_u32(data + 28u);
  decoded.runtime.active_publishes = flow_control_read_u32(data + 32u);
  if (flow_control_read_u64(data + 40u) > SIZE_MAX ||
      flow_control_read_u64(data + 48u) > SIZE_MAX ||
      flow_control_read_u64(data + 56u) > SIZE_MAX || flow_control_read_u64(data + 64u) > SIZE_MAX)
    return TURBO_EPROTO;
  decoded.runtime.stage_count = (size_t)flow_control_read_u64(data + 40u);
  decoded.runtime.edge_count = (size_t)flow_control_read_u64(data + 48u);
  decoded.runtime.adapter_count = (size_t)flow_control_read_u64(data + 56u);
  decoded.runtime.pool_count = (size_t)flow_control_read_u64(data + 64u);
  decoded.error.code = decoded.status;
  decoded.error.line = flow_control_read_u32(data + 72u);
  decoded.error.column = flow_control_read_u32(data + 76u);
  if (message_len > 0u)
    memcpy(decoded.error.message, data + TURBO_FLOW_FMQ_CONTROL_REPLY_HEADER_SIZE, message_len);
  decoded.error.message[message_len] = '\0';
  *out = decoded;
  return TURBO_OK;
}
