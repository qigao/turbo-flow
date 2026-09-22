#include "turbo_flow_protocol_source.h"

#include "salts_error.h"

#include <stdlib.h>
#include <string.h>

typedef enum flow_protocol_source_session_state_e {
  FLOW_PROTOCOL_SOURCE_SESSION_OPEN = 1,
  FLOW_PROTOCOL_SOURCE_SESSION_FAILED
} flow_protocol_source_session_state_t;

typedef struct flow_protocol_source_session_s {
  int in_use;
  flow_protocol_source_session_state_t state;
  int failure_status;
  int admit_blocked;
  uint64_t session_id;
  uint64_t generation;
  char device_id[TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX + 1u];
  char protocol_version[TURBO_FLOW_PROTOCOL_VERSION_MAX + 1u];
  uint8_t *buffer;
  /* Unread bytes occupy [buffer_offset, buffer_offset + buffered). */
  size_t buffer_offset;
  size_t buffered;
} flow_protocol_source_session_t;

struct turbo_flow_protocol_source_s {
  turbo_flow_protocol_t *protocol;
  turbo_flow_protocol_info_t protocol_info;
  turbo_flow_protocol_source_config_t config;
  turbo_flow_protocol_source_ops_t ops;
  void *callback_ctx;
  flow_protocol_source_session_t *sessions;
  uint8_t *storage;
  uint8_t *scratch_payload;
  uint8_t *scratch_semantic;
  uint64_t next_delivery_id;
  int accepting;
};

static int flow_protocol_source_text_copy(char *out, size_t capacity, const char *text,
                                          int optional) {
  size_t length;
  if (!out || capacity == 0u || (!optional && (!text || !text[0]))) return SALTS_EINVAL;
  if (!text || !text[0]) {
    out[0] = '\0';
    return SALTS_OK;
  }
  for (length = 0u; length < capacity && text[length] != '\0'; ++length) {
  }
  if (length >= capacity) return SALTS_EMSGSIZE;
  memcpy(out, text, length + 1u);
  return SALTS_OK;
}

static int flow_protocol_source_is_message_protocol(turbo_flow_protocol_kind_t protocol) {
  return protocol == TURBO_FLOW_PROTOCOL_MQTT_SN || protocol == TURBO_FLOW_PROTOCOL_COAP ||
         protocol == TURBO_FLOW_PROTOCOL_LWM2M || protocol == TURBO_FLOW_PROTOCOL_OCPP;
}

static int flow_protocol_source_is_capacity_error(int status) {
  return status == SALTS_EBUSY || status == SALTS_ENOSPC || status == SALTS_ENOBUFS;
}

static flow_protocol_source_session_t *
flow_protocol_source_session_find(turbo_flow_protocol_source_t *source, uint64_t session_id) {
  if (!source || session_id == 0u) return NULL;
  for (size_t i = 0u; i < source->config.max_sessions; ++i) {
    if (source->sessions[i].in_use && source->sessions[i].session_id == session_id)
      return &source->sessions[i];
  }
  return NULL;
}

static flow_protocol_source_session_t *
flow_protocol_source_session_free(turbo_flow_protocol_source_t *source) {
  if (!source) return NULL;
  for (size_t i = 0u; i < source->config.max_sessions; ++i) {
    if (!source->sessions[i].in_use) return &source->sessions[i];
  }
  return NULL;
}

static void flow_protocol_source_session_release(turbo_flow_protocol_source_t *source,
                                                 flow_protocol_source_session_t *session,
                                                 int status) {
  uint8_t *buffer;
  if (!source || !session || !session->in_use) return;
  if (source->ops.session_closed)
    source->ops.session_closed(source->callback_ctx, session->session_id, session->generation,
                               status);
  buffer = session->buffer;
  memset(session, 0, sizeof(*session));
  session->buffer = buffer;
}

static int flow_protocol_source_frame_size(const turbo_flow_protocol_source_t *source,
                                           const flow_protocol_source_session_t *session,
                                           size_t *out) {
  const uint8_t *data;
  size_t size;
  if (!source || !session || !out) return SALTS_EINVAL;
  *out = 0u;
  data = session->buffer + session->buffer_offset;
  size = session->buffered;
  if (flow_protocol_source_is_message_protocol(source->protocol_info.protocol)) {
    *out = size;
    return SALTS_OK;
  }
  switch (source->protocol_info.protocol) {
  case TURBO_FLOW_PROTOCOL_GBT_32960: {
    size_t body_size;
    size_t frame_size;
    if (size == 0u) return SALTS_OK;
    if (data[0] != 0x23u || (size >= 2u && data[1] != 0x23u)) return SALTS_EPROTO;
    if (size < 24u) return SALTS_OK;
    body_size = ((size_t)data[22] << 8u) | data[23];
    if (body_size > SIZE_MAX - 25u) return SALTS_EMSGSIZE;
    frame_size = body_size + 25u;
    if (frame_size > source->config.max_frame_size) return SALTS_EMSGSIZE;
    if (size >= frame_size) *out = frame_size;
    return SALTS_OK;
  }
  case TURBO_FLOW_PROTOCOL_JTT_808:
    if (size == 0u) return SALTS_OK;
    if (data[0] != 0x7eu) return SALTS_EPROTO;
    for (size_t i = 1u; i < size; ++i) {
      if (data[i] == 0x7eu) {
        *out = i + 1u;
        return SALTS_OK;
      }
    }
    return size == source->config.max_frame_size ? SALTS_EMSGSIZE : SALTS_OK;
  default:
    return SALTS_ENOTSUP;
  }
}

static int flow_protocol_source_next_delivery(turbo_flow_protocol_source_t *source, uint64_t *out) {
  if (!source || !out) return SALTS_EINVAL;
  if (source->next_delivery_id == UINT64_MAX) return SALTS_ERANGE;
  *out = source->next_delivery_id + 1u;
  if (*out == 0u) return SALTS_ERANGE;
  return SALTS_OK;
}

static int flow_protocol_source_admit(turbo_flow_protocol_source_t *source,
                                      flow_protocol_source_session_t *session, const uint8_t *data,
                                      size_t size, turbo_flow_protocol_source_feed_result_t *result,
                                      int *provider_called) {
  turbo_flow_protocol_frame_view_t frame = TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
  turbo_flow_protocol_message_output_t message = TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
  turbo_flow_protocol_semantic_output_t semantic = TURBO_FLOW_PROTOCOL_SEMANTIC_OUTPUT_INIT;
  turbo_flow_protocol_source_admit_request_t request =
      TURBO_FLOW_PROTOCOL_SOURCE_ADMIT_REQUEST_INIT;
  uint64_t delivery_id;
  int rc;
  if (!provider_called) return SALTS_EINVAL;
  *provider_called = 0;
  rc = flow_protocol_source_next_delivery(source, &delivery_id);
  if (rc != SALTS_OK) return rc;
  frame.data = data;
  frame.data_size = size;
  frame.device_id = session->device_id[0] != '\0' ? session->device_id : NULL;
  frame.protocol_version = session->protocol_version;
  message.payload = source->scratch_payload;
  message.payload_capacity = source->config.max_frame_size;
  if (source->config.decode_mode == TURBO_FLOW_PROTOCOL_SOURCE_DECODE_SEMANTIC) {
    semantic.data = source->scratch_semantic;
    semantic.capacity = source->config.max_semantic_bytes;
    rc = turbo_flow_protocol_decode_semantic(source->protocol, &frame, &message, &semantic);
  } else {
    rc = turbo_flow_protocol_decode(source->protocol, &frame, &message);
  }
  if (rc != SALTS_OK) return rc;
  request.delivery_id = delivery_id;
  request.session_id = session->session_id;
  request.session_generation = session->generation;
  request.message = &message;
  request.semantic =
      source->config.decode_mode == TURBO_FLOW_PROTOCOL_SOURCE_DECODE_SEMANTIC ? &semantic : NULL;
  *provider_called = 1;
  rc = source->ops.admit(source->callback_ctx, &request);
  if (rc != SALTS_OK) return rc;
  source->next_delivery_id = delivery_id;
  result->frames_admitted++;
  return SALTS_OK;
}

static int flow_protocol_source_pump(turbo_flow_protocol_source_t *source,
                                     flow_protocol_source_session_t *session,
                                     turbo_flow_protocol_source_feed_result_t *result) {
  while (session->state == FLOW_PROTOCOL_SOURCE_SESSION_OPEN && session->buffered > 0u) {
    size_t frame_size = 0u;
    int provider_called = 0;
    int rc = flow_protocol_source_frame_size(source, session, &frame_size);
    if (rc != SALTS_OK) {
      session->state = FLOW_PROTOCOL_SOURCE_SESSION_FAILED;
      session->failure_status = rc;
      return rc;
    }
    if (frame_size == 0u) return SALTS_OK;
    rc = flow_protocol_source_admit(source, session, session->buffer + session->buffer_offset,
                                    frame_size, result, &provider_called);
    if (provider_called && flow_protocol_source_is_capacity_error(rc)) {
      session->admit_blocked = 1;
      result->backpressured = 1u;
      return SALTS_OK;
    }
    if (rc != SALTS_OK) {
      session->state = FLOW_PROTOCOL_SOURCE_SESSION_FAILED;
      session->failure_status = rc;
      return rc;
    }
    session->admit_blocked = 0;
    session->buffer_offset += frame_size;
    session->buffered -= frame_size;
    if (session->buffered == 0u) session->buffer_offset = 0u;
  }
  return SALTS_OK;
}

int turbo_flow_protocol_source_create(turbo_flow_protocol_t *protocol,
                                      const turbo_flow_protocol_source_config_t *config,
                                      const turbo_flow_protocol_source_ops_t *ops, void *ctx,
                                      turbo_flow_protocol_source_t **out) {
  turbo_flow_protocol_source_t *source;
  turbo_flow_protocol_info_t info = TURBO_FLOW_PROTOCOL_INFO_INIT;
  size_t buffer_count;
  size_t frame_buffer_bytes;
  size_t buffer_bytes;
  int rc;
  if (out) *out = NULL;
  if (!protocol || !config || config->size != sizeof(*config) ||
      config->abi_version != TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION || config->max_sessions == 0u ||
      config->max_frame_size == 0u ||
      (config->decode_mode != TURBO_FLOW_PROTOCOL_SOURCE_DECODE_RAW &&
       config->decode_mode != TURBO_FLOW_PROTOCOL_SOURCE_DECODE_SEMANTIC) ||
      (config->decode_mode == TURBO_FLOW_PROTOCOL_SOURCE_DECODE_RAW &&
       config->max_semantic_bytes != 0u) ||
      (config->decode_mode == TURBO_FLOW_PROTOCOL_SOURCE_DECODE_SEMANTIC &&
       config->max_semantic_bytes == 0u) ||
      !ops || ops->size != sizeof(*ops) ||
      ops->abi_version != TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION || !ops->admit || !out)
    return SALTS_EINVAL;
  rc = turbo_flow_protocol_get_info(protocol, &info);
  if (rc != SALTS_OK) return rc;
  if (config->max_frame_size > info.max_frame_size) return SALTS_ERANGE;
  if (config->decode_mode == TURBO_FLOW_PROTOCOL_SOURCE_DECODE_SEMANTIC &&
      (info.capabilities & TURBO_FLOW_PROTOCOL_CAP_SEMANTIC_DECODE) == 0u)
    return SALTS_ENOTSUP;
  if (config->max_sessions == SIZE_MAX) return SALTS_EMSGSIZE;
  buffer_count = config->max_sessions + 1u;
  if (config->max_frame_size > SIZE_MAX / buffer_count) return SALTS_EMSGSIZE;
  frame_buffer_bytes = buffer_count * config->max_frame_size;
  if (config->max_semantic_bytes > SIZE_MAX - frame_buffer_bytes) return SALTS_EMSGSIZE;
  buffer_bytes = frame_buffer_bytes + config->max_semantic_bytes;
  if (buffer_bytes > config->max_buffered_bytes) return SALTS_ENOSPC;
  source = (turbo_flow_protocol_source_t *)calloc(1u, sizeof(*source));
  if (!source) return SALTS_ENOMEM;
  source->sessions =
      (flow_protocol_source_session_t *)calloc(config->max_sessions, sizeof(*source->sessions));
  source->storage = (uint8_t *)calloc(1u, buffer_bytes);
  if (!source->sessions || !source->storage) {
    free(source->storage);
    free(source->sessions);
    free(source);
    return SALTS_ENOMEM;
  }
  for (size_t i = 0u; i < config->max_sessions; ++i)
    source->sessions[i].buffer = source->storage + i * config->max_frame_size;
  source->scratch_payload = source->storage + config->max_sessions * config->max_frame_size;
  source->scratch_semantic =
      config->decode_mode == TURBO_FLOW_PROTOCOL_SOURCE_DECODE_SEMANTIC
          ? source->scratch_payload + config->max_frame_size
          : NULL;
  source->protocol = protocol;
  source->protocol_info = info;
  source->config = *config;
  source->ops = *ops;
  source->callback_ctx = ctx;
  source->accepting = 1;
  *out = source;
  return SALTS_OK;
}

int turbo_flow_protocol_source_destroy(turbo_flow_protocol_source_t *source) {
  if (!source) return SALTS_OK;
  if (source->accepting) return SALTS_EBUSY;
  for (size_t i = 0u; i < source->config.max_sessions; ++i) {
    if (source->sessions[i].in_use) return SALTS_EBUSY;
  }
  free(source->storage);
  free(source->sessions);
  free(source);
  return SALTS_OK;
}

int turbo_flow_protocol_source_session_open(
    turbo_flow_protocol_source_t *source,
    const turbo_flow_protocol_source_session_open_request_t *request) {
  flow_protocol_source_session_t *session;
  const char *version;
  int rc;
  if (!source || !request || request->size != sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION || request->session_id == 0u ||
      request->generation == 0u)
    return SALTS_EINVAL;
  if (!source->accepting) return SALTS_ESHUTDOWN;
  session = flow_protocol_source_session_find(source, request->session_id);
  if (session) return session->generation == request->generation ? SALTS_EALREADY : SALTS_EBUSY;
  session = flow_protocol_source_session_free(source);
  if (!session) return SALTS_ENOSPC;
  version = request->protocol_version && request->protocol_version[0] != '\0'
                ? request->protocol_version
                : source->protocol_info.protocol_version;
  rc = flow_protocol_source_text_copy(session->protocol_version, sizeof(session->protocol_version),
                                      version, 0);
  if (rc != SALTS_OK) return rc;
  if (strcmp(session->protocol_version, source->protocol_info.protocol_version) != 0) {
    session->protocol_version[0] = '\0';
    return SALTS_ENOTSUP;
  }
  rc = flow_protocol_source_text_copy(
      session->device_id, sizeof(session->device_id), request->device_id,
      source->protocol_info.protocol == TURBO_FLOW_PROTOCOL_GBT_32960 ||
          source->protocol_info.protocol == TURBO_FLOW_PROTOCOL_JTT_808);
  if (rc != SALTS_OK) {
    session->protocol_version[0] = '\0';
    return rc;
  }
  session->in_use = 1;
  session->state = FLOW_PROTOCOL_SOURCE_SESSION_OPEN;
  session->session_id = request->session_id;
  session->generation = request->generation;
  session->failure_status = SALTS_OK;
  return SALTS_OK;
}

static int flow_protocol_source_result_validate(turbo_flow_protocol_source_feed_result_t *result) {
  if (!result || result->size != sizeof(*result) ||
      result->abi_version != TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION)
    return SALTS_EINVAL;
  *result = (turbo_flow_protocol_source_feed_result_t)TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
  return SALTS_OK;
}

int turbo_flow_protocol_source_session_feed(turbo_flow_protocol_source_t *source,
                                            uint64_t session_id, uint64_t generation,
                                            const uint8_t *data, size_t size,
                                            turbo_flow_protocol_source_feed_result_t *result) {
  flow_protocol_source_session_t *session;
  int message_protocol;
  int rc = flow_protocol_source_result_validate(result);
  if (rc != SALTS_OK || !source || session_id == 0u || generation == 0u || (!data && size != 0u))
    return SALTS_EINVAL;
  if (!source->accepting) return SALTS_ESHUTDOWN;
  session = flow_protocol_source_session_find(source, session_id);
  if (!session) return SALTS_ENOENT;
  if (session->generation != generation) return SALTS_EPROTO;
  if (session->state == FLOW_PROTOCOL_SOURCE_SESSION_FAILED) return session->failure_status;
  if (session->admit_blocked) {
    if (size != 0u) {
      result->backpressured = 1u;
      return SALTS_EBUSY;
    }
    session->admit_blocked = 0;
    return flow_protocol_source_pump(source, session, result);
  }
  message_protocol = flow_protocol_source_is_message_protocol(source->protocol_info.protocol);
  if (message_protocol) {
    if (size == 0u) return SALTS_EINVAL;
    if (size > source->config.max_frame_size) return SALTS_EMSGSIZE;
    if (session->buffered != 0u || session->buffer_offset != 0u) return SALTS_EPROTO;
    memcpy(session->buffer, data, size);
    session->buffered = size;
    result->accepted_size = size;
    return flow_protocol_source_pump(source, session, result);
  }
  if (session->buffer_offset > source->config.max_frame_size ||
      session->buffered > source->config.max_frame_size - session->buffer_offset)
    return SALTS_EPROTO;
  if (size > source->config.max_frame_size - session->buffered) return SALTS_ENOBUFS;
  if (size > 0u) {
    size_t tail = session->buffer_offset + session->buffered;
    /* Compaction is O(buffered), but happens at most once per feed instead of once per frame. */
    if (size > source->config.max_frame_size - tail) {
      if (session->buffered > 0u)
        memmove(session->buffer, session->buffer + session->buffer_offset, session->buffered);
      session->buffer_offset = 0u;
      tail = session->buffered;
    }
    memcpy(session->buffer + tail, data, size);
    session->buffered += size;
  }
  result->accepted_size = size;
  return flow_protocol_source_pump(source, session, result);
}

int turbo_flow_protocol_source_session_close(turbo_flow_protocol_source_t *source,
                                             uint64_t session_id, uint64_t generation, int status) {
  flow_protocol_source_session_t *session;
  if (!source || session_id == 0u || generation == 0u) return SALTS_EINVAL;
  session = flow_protocol_source_session_find(source, session_id);
  if (!session) return SALTS_ENOENT;
  if (session->generation != generation) return SALTS_EPROTO;
  flow_protocol_source_session_release(source, session, status);
  return SALTS_OK;
}

int turbo_flow_protocol_source_begin_shutdown(turbo_flow_protocol_source_t *source) {
  if (!source) return SALTS_EINVAL;
  source->accepting = 0;
  for (size_t i = 0u; i < source->config.max_sessions; ++i) {
    flow_protocol_source_session_t *session = &source->sessions[i];
    if (session->in_use) flow_protocol_source_session_release(source, session, SALTS_ESHUTDOWN);
  }
  return SALTS_OK;
}

int turbo_flow_protocol_source_force_shutdown(turbo_flow_protocol_source_t *source, int status) {
  if (!source || status == SALTS_OK) return SALTS_EINVAL;
  source->accepting = 0;
  for (size_t i = 0u; i < source->config.max_sessions; ++i) {
    flow_protocol_source_session_t *session = &source->sessions[i];
    if (session->in_use) flow_protocol_source_session_release(source, session, status);
  }
  return SALTS_OK;
}

int turbo_flow_protocol_source_snapshot(const turbo_flow_protocol_source_t *source,
                                        turbo_flow_protocol_source_snapshot_t *out) {
  turbo_flow_protocol_source_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_SOURCE_SNAPSHOT_INIT;
  if (!source || !out || out->size != sizeof(*out) ||
      out->abi_version != TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION)
    return SALTS_EINVAL;
  snapshot.accepting = source->accepting ? 1u : 0u;
  for (size_t i = 0u; i < source->config.max_sessions; ++i) {
    const flow_protocol_source_session_t *session = &source->sessions[i];
    if (!session->in_use) continue;
    snapshot.active_sessions++;
    snapshot.buffered_bytes += session->buffered;
  }
  *out = snapshot;
  return SALTS_OK;
}
