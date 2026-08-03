#include "flowie_cluster_peer_internal.h"
#include "flowie_cluster_peer_wire_internal.h"

#include <limits.h>
#include <string.h>

static const uint8_t FLOWIE_CLUSTER_PEER_MAGIC[4] = {'T', 'F', 'C', 'L'};

enum {
  FLOWIE_CLUSTER_PEER_OFFSET_VERSION = 4,
  FLOWIE_CLUSTER_PEER_OFFSET_HEADER_SIZE = 6,
  FLOWIE_CLUSTER_PEER_OFFSET_KIND = 8,
  FLOWIE_CLUSTER_PEER_OFFSET_OPERATION = 10,
  FLOWIE_CLUSTER_PEER_OFFSET_FLAGS = 12,
  FLOWIE_CLUSTER_PEER_OFFSET_TOTAL_SIZE = 16,
  FLOWIE_CLUSTER_PEER_OFFSET_PAYLOAD_SIZE = 20,
  FLOWIE_CLUSTER_PEER_OFFSET_CLUSTER_SIZE = 24,
  FLOWIE_CLUSTER_PEER_OFFSET_LISTENER_SIZE = 26,
  FLOWIE_CLUSTER_PEER_OFFSET_SOURCE_NODE_SIZE = 28,
  FLOWIE_CLUSTER_PEER_OFFSET_TARGET_NODE_SIZE = 30,
  FLOWIE_CLUSTER_PEER_OFFSET_SOURCE_BOOT_ID = 32,
  FLOWIE_CLUSTER_PEER_OFFSET_TARGET_BOOT_ID = 48,
  FLOWIE_CLUSTER_PEER_OFFSET_SHARD_ID = 64,
  FLOWIE_CLUSTER_PEER_OFFSET_STATUS = 68,
  FLOWIE_CLUSTER_PEER_OFFSET_OWNER_EPOCH = 72,
  FLOWIE_CLUSTER_PEER_OFFSET_CORRELATION_ID = 80,
  FLOWIE_CLUSTER_PEER_OFFSET_CONNECTION_ID = 96,
  FLOWIE_CLUSTER_PEER_OFFSET_CONNECTION_GENERATION = 104
};

static int flowie_cluster_peer_nonzero(const uint8_t *data, size_t size) {
  size_t index;
  for (index = 0u; index < size; ++index)
    if (data[index] != 0u) return 1;
  return 0;
}

static int flowie_cluster_peer_text_validate(tstr_v value, size_t maximum, int required) {
  if ((value.len != 0u && !value.data) || (required && value.len == 0u)) return TURBO_EINVAL;
  if (value.len > maximum) return TURBO_EMSGSIZE;
  return value.len != 0u && memchr(value.data, '\0', value.len) != NULL ? TURBO_EPROTO : TURBO_OK;
}

static int flowie_cluster_peer_is_state_frame(flowie_cluster_peer_frame_kind_t kind) {
  return kind == FLOWIE_CLUSTER_PEER_FRAME_COMMAND || kind == FLOWIE_CLUSTER_PEER_FRAME_REPLY ||
         kind == FLOWIE_CLUSTER_PEER_FRAME_EVENT;
}

static int flowie_cluster_peer_operation_validate(flowie_cluster_peer_frame_kind_t kind,
                                                  flowie_cluster_peer_operation_t operation) {
  switch (kind) {
  case FLOWIE_CLUSTER_PEER_FRAME_COMMAND:
    return (operation >= FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND &&
            operation <= FLOWIE_CLUSTER_PEER_OPERATION_TAKEOVER_CLOSE) ||
           operation == FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION ||
           operation == FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH_SETTLE;
  case FLOWIE_CLUSTER_PEER_FRAME_REPLY:
    return operation == FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY ||
           operation == FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION_ACK;
  case FLOWIE_CLUSTER_PEER_FRAME_EVENT:
    return operation == FLOWIE_CLUSTER_PEER_OPERATION_EVENT_DELIVER ||
           operation == FLOWIE_CLUSTER_PEER_OPERATION_EVENT_ACK;
  default:
    return operation == FLOWIE_CLUSTER_PEER_OPERATION_NONE;
  }
}

static int
flowie_cluster_peer_operation_requires_connection(flowie_cluster_peer_operation_t operation) {
  return (operation >= FLOWIE_CLUSTER_PEER_OPERATION_CONNECT_BIND &&
         operation <= FLOWIE_CLUSTER_PEER_OPERATION_MQTT_REPLY) ||
         operation == FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION ||
         operation == FLOWIE_CLUSTER_PEER_OPERATION_EDGE_ACTION_ACK ||
         operation == FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH_SETTLE;
}

static int flowie_cluster_peer_frame_validate(const flowie_cluster_peer_frame_t *frame,
                                              size_t max_payload_size) {
  int state_frame;
  int rc;
  if (!frame || frame->size != sizeof(*frame) || max_payload_size == 0u) return TURBO_EINVAL;
  if (frame->kind < FLOWIE_CLUSTER_PEER_FRAME_HELLO ||
      frame->kind > FLOWIE_CLUSTER_PEER_FRAME_GOAWAY || frame->flags != 0u) {
    return TURBO_EPROTO;
  }
  if (frame->operation < FLOWIE_CLUSTER_PEER_OPERATION_NONE ||
      frame->operation > FLOWIE_CLUSTER_PEER_OPERATION_MQTT_PUBLISH_SETTLE ||
      !flowie_cluster_peer_operation_validate(frame->kind, frame->operation)) {
    return TURBO_EPROTO;
  }
  if ((frame->payload.len != 0u && !frame->payload.data) || frame->payload.len > max_payload_size ||
      frame->payload.len > UINT32_MAX) {
    return frame->payload.len > max_payload_size || frame->payload.len > UINT32_MAX ? TURBO_EMSGSIZE
                                                                                    : TURBO_EINVAL;
  }
  rc = flowie_cluster_peer_text_validate(frame->cluster_id, FLOWIE_CLUSTER_ID_MAX, 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_text_validate(frame->source_node_id, FLOWIE_CLUSTER_NODE_ID_MAX, 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_text_validate(frame->target_node_id, FLOWIE_CLUSTER_NODE_ID_MAX, 1);
  if (rc != TURBO_OK) return rc;
  if (!flowie_cluster_peer_nonzero(frame->source_boot_id, sizeof(frame->source_boot_id)) ||
      !flowie_cluster_peer_nonzero(frame->target_boot_id, sizeof(frame->target_boot_id))) {
    return TURBO_EPROTO;
  }

  state_frame = flowie_cluster_peer_is_state_frame(frame->kind);
  rc = flowie_cluster_peer_text_validate(frame->listener_id, FLOWIE_CLUSTER_LISTENER_ID_MAX,
                                         state_frame);
  if (rc != TURBO_OK) return rc;
  if (state_frame) {
    int requires_connection = flowie_cluster_peer_operation_requires_connection(frame->operation);
    if (frame->owner_epoch == 0u ||
        !flowie_cluster_peer_nonzero(frame->correlation_id, sizeof(frame->correlation_id)) ||
        (requires_connection &&
         (frame->connection_id == 0u || frame->connection_generation == 0u)) ||
        (!requires_connection &&
         (frame->connection_id != 0u || frame->connection_generation != 0u))) {
      return TURBO_EPROTO;
    }
    if (frame->kind != FLOWIE_CLUSTER_PEER_FRAME_REPLY && frame->status != 0) return TURBO_EPROTO;
  } else if (frame->operation != FLOWIE_CLUSTER_PEER_OPERATION_NONE ||
             frame->listener_id.len != 0u || frame->shard_id != 0u || frame->status != 0 ||
             frame->owner_epoch != 0u ||
             flowie_cluster_peer_nonzero(frame->correlation_id, sizeof(frame->correlation_id)) ||
             frame->connection_id != 0u || frame->connection_generation != 0u) {
    return TURBO_EPROTO;
  }
  if ((frame->kind == FLOWIE_CLUSTER_PEER_FRAME_PING ||
       frame->kind == FLOWIE_CLUSTER_PEER_FRAME_PONG) &&
      frame->payload.len != 0u) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

int flowie_cluster_peer_frame_encoded_size(const flowie_cluster_peer_frame_t *frame,
                                           size_t max_payload_size, size_t *out_size) {
  size_t body_size;
  int rc;
  if (!out_size) return TURBO_EINVAL;
  *out_size = 0u;
  rc = flowie_cluster_peer_frame_validate(frame, max_payload_size);
  if (rc != TURBO_OK) return rc;
  body_size = frame->cluster_id.len + frame->listener_id.len + frame->source_node_id.len +
              frame->target_node_id.len;
  if (body_size > SIZE_MAX - frame->payload.len ||
      FLOWIE_CLUSTER_PEER_HEADER_SIZE > SIZE_MAX - body_size - frame->payload.len) {
    return TURBO_ERANGE;
  }
  *out_size = FLOWIE_CLUSTER_PEER_HEADER_SIZE + body_size + frame->payload.len;
  return *out_size > UINT32_MAX ? TURBO_ERANGE : TURBO_OK;
}

static void flowie_cluster_peer_frame_encode_header(const flowie_cluster_peer_frame_t *frame,
                                                    uint8_t *out, size_t total_size) {
  memcpy(out, FLOWIE_CLUSTER_PEER_MAGIC, sizeof(FLOWIE_CLUSTER_PEER_MAGIC));
  flowie_cluster_peer_wire_write_u16(out + FLOWIE_CLUSTER_PEER_OFFSET_VERSION,
                                     FLOWIE_CLUSTER_PEER_WIRE_VERSION);
  flowie_cluster_peer_wire_write_u16(out + FLOWIE_CLUSTER_PEER_OFFSET_HEADER_SIZE,
                                     FLOWIE_CLUSTER_PEER_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u16(out + FLOWIE_CLUSTER_PEER_OFFSET_KIND, (uint16_t)frame->kind);
  flowie_cluster_peer_wire_write_u16(out + FLOWIE_CLUSTER_PEER_OFFSET_OPERATION,
                                     (uint16_t)frame->operation);
  flowie_cluster_peer_wire_write_u32(out + FLOWIE_CLUSTER_PEER_OFFSET_FLAGS, frame->flags);
  flowie_cluster_peer_wire_write_u32(out + FLOWIE_CLUSTER_PEER_OFFSET_TOTAL_SIZE,
                                     (uint32_t)total_size);
  flowie_cluster_peer_wire_write_u32(out + FLOWIE_CLUSTER_PEER_OFFSET_PAYLOAD_SIZE,
                                     (uint32_t)frame->payload.len);
  flowie_cluster_peer_wire_write_u16(out + FLOWIE_CLUSTER_PEER_OFFSET_CLUSTER_SIZE,
                                     (uint16_t)frame->cluster_id.len);
  flowie_cluster_peer_wire_write_u16(out + FLOWIE_CLUSTER_PEER_OFFSET_LISTENER_SIZE,
                                     (uint16_t)frame->listener_id.len);
  flowie_cluster_peer_wire_write_u16(out + FLOWIE_CLUSTER_PEER_OFFSET_SOURCE_NODE_SIZE,
                                     (uint16_t)frame->source_node_id.len);
  flowie_cluster_peer_wire_write_u16(out + FLOWIE_CLUSTER_PEER_OFFSET_TARGET_NODE_SIZE,
                                     (uint16_t)frame->target_node_id.len);
  memcpy(out + FLOWIE_CLUSTER_PEER_OFFSET_SOURCE_BOOT_ID, frame->source_boot_id,
         sizeof(frame->source_boot_id));
  memcpy(out + FLOWIE_CLUSTER_PEER_OFFSET_TARGET_BOOT_ID, frame->target_boot_id,
         sizeof(frame->target_boot_id));
  flowie_cluster_peer_wire_write_u32(out + FLOWIE_CLUSTER_PEER_OFFSET_SHARD_ID, frame->shard_id);
  flowie_cluster_peer_wire_write_u32(out + FLOWIE_CLUSTER_PEER_OFFSET_STATUS,
                                     (uint32_t)frame->status);
  flowie_cluster_peer_wire_write_u64(out + FLOWIE_CLUSTER_PEER_OFFSET_OWNER_EPOCH,
                                     frame->owner_epoch);
  memcpy(out + FLOWIE_CLUSTER_PEER_OFFSET_CORRELATION_ID, frame->correlation_id,
         sizeof(frame->correlation_id));
  flowie_cluster_peer_wire_write_u64(out + FLOWIE_CLUSTER_PEER_OFFSET_CONNECTION_ID,
                                     frame->connection_id);
  flowie_cluster_peer_wire_write_u64(out + FLOWIE_CLUSTER_PEER_OFFSET_CONNECTION_GENERATION,
                                     frame->connection_generation);
}

int flowie_cluster_peer_frame_encode(const flowie_cluster_peer_frame_t *frame,
                                     size_t max_payload_size, tstr_t *out) {
  size_t total_size;
  size_t offset = FLOWIE_CLUSTER_PEER_HEADER_SIZE;
  int rc;
  if (!out || *out) return TURBO_EINVAL;
  rc = flowie_cluster_peer_frame_encoded_size(frame, max_payload_size, &total_size);
  if (rc != TURBO_OK) return rc;
  *out = tstr_new_len(NULL, total_size);
  if (!*out) return TURBO_ENOMEM;
  flowie_cluster_peer_frame_encode_header(frame, (uint8_t *)*out, total_size);
#define FLOWIE_CLUSTER_PEER_COPY_VIEW(view)                                                        \
  do {                                                                                             \
    if ((view).len != 0u) {                                                                        \
      memcpy(*out + offset, (view).data, (view).len);                                              \
      offset += (view).len;                                                                        \
    }                                                                                              \
  } while (0)
  FLOWIE_CLUSTER_PEER_COPY_VIEW(frame->cluster_id);
  FLOWIE_CLUSTER_PEER_COPY_VIEW(frame->listener_id);
  FLOWIE_CLUSTER_PEER_COPY_VIEW(frame->source_node_id);
  FLOWIE_CLUSTER_PEER_COPY_VIEW(frame->target_node_id);
  FLOWIE_CLUSTER_PEER_COPY_VIEW(frame->payload);
#undef FLOWIE_CLUSTER_PEER_COPY_VIEW
  return TURBO_OK;
}

static int flowie_cluster_peer_header_decode(const uint8_t *data, size_t max_payload_size,
                                             flowie_cluster_peer_frame_t *frame,
                                             size_t *total_size) {
  size_t identity_size;
  uint32_t wire_total_size;
  uint32_t payload_size;
  if (memcmp(data, FLOWIE_CLUSTER_PEER_MAGIC, sizeof(FLOWIE_CLUSTER_PEER_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(data + FLOWIE_CLUSTER_PEER_OFFSET_VERSION) !=
          FLOWIE_CLUSTER_PEER_WIRE_VERSION ||
      flowie_cluster_peer_wire_read_u16(data + FLOWIE_CLUSTER_PEER_OFFSET_HEADER_SIZE) !=
          FLOWIE_CLUSTER_PEER_HEADER_SIZE) {
    return TURBO_EPROTO;
  }
  wire_total_size = flowie_cluster_peer_wire_read_u32(data + FLOWIE_CLUSTER_PEER_OFFSET_TOTAL_SIZE);
  payload_size = flowie_cluster_peer_wire_read_u32(data + FLOWIE_CLUSTER_PEER_OFFSET_PAYLOAD_SIZE);
  frame->kind = (flowie_cluster_peer_frame_kind_t)flowie_cluster_peer_wire_read_u16(
      data + FLOWIE_CLUSTER_PEER_OFFSET_KIND);
  frame->operation = (flowie_cluster_peer_operation_t)flowie_cluster_peer_wire_read_u16(
      data + FLOWIE_CLUSTER_PEER_OFFSET_OPERATION);
  frame->flags = flowie_cluster_peer_wire_read_u32(data + FLOWIE_CLUSTER_PEER_OFFSET_FLAGS);
  frame->cluster_id.len =
      flowie_cluster_peer_wire_read_u16(data + FLOWIE_CLUSTER_PEER_OFFSET_CLUSTER_SIZE);
  frame->listener_id.len =
      flowie_cluster_peer_wire_read_u16(data + FLOWIE_CLUSTER_PEER_OFFSET_LISTENER_SIZE);
  frame->source_node_id.len =
      flowie_cluster_peer_wire_read_u16(data + FLOWIE_CLUSTER_PEER_OFFSET_SOURCE_NODE_SIZE);
  frame->target_node_id.len =
      flowie_cluster_peer_wire_read_u16(data + FLOWIE_CLUSTER_PEER_OFFSET_TARGET_NODE_SIZE);
  frame->payload.len = payload_size;
  if (frame->cluster_id.len > FLOWIE_CLUSTER_ID_MAX ||
      frame->listener_id.len > FLOWIE_CLUSTER_LISTENER_ID_MAX ||
      frame->source_node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX ||
      frame->target_node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX || payload_size > max_payload_size) {
    return TURBO_EMSGSIZE;
  }
  identity_size = frame->cluster_id.len + frame->listener_id.len + frame->source_node_id.len +
                  frame->target_node_id.len;
  if (identity_size > SIZE_MAX - payload_size ||
      FLOWIE_CLUSTER_PEER_HEADER_SIZE > SIZE_MAX - identity_size - payload_size ||
      FLOWIE_CLUSTER_PEER_HEADER_SIZE + identity_size + payload_size != wire_total_size)
    return TURBO_EPROTO;
  memcpy(frame->source_boot_id, data + FLOWIE_CLUSTER_PEER_OFFSET_SOURCE_BOOT_ID,
         sizeof(frame->source_boot_id));
  memcpy(frame->target_boot_id, data + FLOWIE_CLUSTER_PEER_OFFSET_TARGET_BOOT_ID,
         sizeof(frame->target_boot_id));
  frame->shard_id = flowie_cluster_peer_wire_read_u32(data + FLOWIE_CLUSTER_PEER_OFFSET_SHARD_ID);
  {
    uint32_t wire_status =
        flowie_cluster_peer_wire_read_u32(data + FLOWIE_CLUSTER_PEER_OFFSET_STATUS);
    memcpy(&frame->status, &wire_status, sizeof(frame->status));
  }
  frame->owner_epoch =
      flowie_cluster_peer_wire_read_u64(data + FLOWIE_CLUSTER_PEER_OFFSET_OWNER_EPOCH);
  memcpy(frame->correlation_id, data + FLOWIE_CLUSTER_PEER_OFFSET_CORRELATION_ID,
         sizeof(frame->correlation_id));
  frame->connection_id =
      flowie_cluster_peer_wire_read_u64(data + FLOWIE_CLUSTER_PEER_OFFSET_CONNECTION_ID);
  frame->connection_generation =
      flowie_cluster_peer_wire_read_u64(data + FLOWIE_CLUSTER_PEER_OFFSET_CONNECTION_GENERATION);
  *total_size = wire_total_size;
  return TURBO_OK;
}

int flowie_cluster_peer_frame_decode(const void *data, size_t data_size, size_t max_payload_size,
                                     flowie_cluster_peer_frame_t *out, size_t *consumed) {
  flowie_cluster_peer_frame_t parsed = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  const uint8_t *bytes = (const uint8_t *)data;
  size_t total_size;
  size_t offset;
  int rc;
  if (!out || out->size != sizeof(*out) || out->storage || !consumed || max_payload_size == 0u ||
      (data_size != 0u && !data)) {
    return TURBO_EINVAL;
  }
  *consumed = 0u;
  if (data_size < FLOWIE_CLUSTER_PEER_HEADER_SIZE) return FLOWIE_CLUSTER_PEER_INCOMPLETE;
  rc = flowie_cluster_peer_header_decode(bytes, max_payload_size, &parsed, &total_size);
  if (rc != TURBO_OK) return rc;
  if (data_size < total_size) return FLOWIE_CLUSTER_PEER_INCOMPLETE;
  parsed.storage = tstr_new_len(data, total_size);
  if (!parsed.storage) return TURBO_ENOMEM;
  bytes = (const uint8_t *)parsed.storage;
  offset = FLOWIE_CLUSTER_PEER_HEADER_SIZE;
#define FLOWIE_CLUSTER_PEER_ASSIGN_VIEW(view)                                                      \
  do {                                                                                             \
    (view).data = (const char *)bytes + offset;                                                    \
    offset += (view).len;                                                                          \
  } while (0)
  FLOWIE_CLUSTER_PEER_ASSIGN_VIEW(parsed.cluster_id);
  FLOWIE_CLUSTER_PEER_ASSIGN_VIEW(parsed.listener_id);
  FLOWIE_CLUSTER_PEER_ASSIGN_VIEW(parsed.source_node_id);
  FLOWIE_CLUSTER_PEER_ASSIGN_VIEW(parsed.target_node_id);
  FLOWIE_CLUSTER_PEER_ASSIGN_VIEW(parsed.payload);
#undef FLOWIE_CLUSTER_PEER_ASSIGN_VIEW
  rc = flowie_cluster_peer_frame_validate(&parsed, max_payload_size);
  if (rc != TURBO_OK) {
    tstr_free(parsed.storage);
    return rc == TURBO_EINVAL ? TURBO_EPROTO : rc;
  }
  *out = parsed;
  *consumed = total_size;
  return TURBO_OK;
}

int flowie_cluster_peer_frame_require_target(
    const flowie_cluster_peer_frame_t *frame, tstr_v cluster_id, tstr_v target_node_id,
    const uint8_t target_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  if (!frame || frame->size != sizeof(*frame) || !target_boot_id || cluster_id.len == 0u ||
      !cluster_id.data || target_node_id.len == 0u || !target_node_id.data ||
      (frame->cluster_id.len != 0u && !frame->cluster_id.data) ||
      (frame->target_node_id.len != 0u && !frame->target_node_id.data)) {
    return TURBO_EINVAL;
  }
  if (frame->cluster_id.len != cluster_id.len ||
      memcmp(frame->cluster_id.data, cluster_id.data, cluster_id.len) != 0 ||
      frame->target_node_id.len != target_node_id.len ||
      memcmp(frame->target_node_id.data, target_node_id.data, target_node_id.len) != 0 ||
      memcmp(frame->target_boot_id, target_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

void flowie_cluster_peer_frame_cleanup(flowie_cluster_peer_frame_t *frame) {
  flowie_cluster_peer_frame_t reset = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  if (!frame) return;
  tstr_free(frame->storage);
  *frame = reset;
}
