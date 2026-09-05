#include "turbo_flow_protocol_runtime.h"

#include "salts_error.h"

#include <stdlib.h>
#include <string.h>

typedef struct flow_protocol_session_s {
  int in_use;
  turbo_flow_protocol_session_state_t state;
  int failure_status;
  uint64_t session_id;
  uint64_t generation;
  char device_id[TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX + 1u];
  char protocol_version[TURBO_FLOW_PROTOCOL_VERSION_MAX + 1u];
  uint8_t *buffer;
  /* Unread bytes occupy [buffer_offset, buffer_offset + buffered). */
  size_t buffer_offset;
  size_t buffered;
  uint64_t pending_delivery_id;
  size_t pending_frame_size;
  turbo_flow_protocol_metadata_t pending_metadata;
} flow_protocol_session_t;

struct turbo_flow_protocol_runtime_s {
  turbo_flow_protocol_t *protocol;
  turbo_flow_protocol_info_t protocol_info;
  turbo_flow_protocol_runtime_config_t config;
  turbo_flow_protocol_runtime_ops_t ops;
  void *callback_ctx;
  flow_protocol_session_t *sessions;
  uint8_t *storage;
  uint8_t *scratch_payload;
  uint64_t next_delivery_id;
  int accepting;
};

static int flow_protocol_runtime_text_copy(char *out, size_t capacity, const char *text,
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

static int flow_protocol_runtime_is_message_protocol(turbo_flow_protocol_kind_t protocol) {
  return protocol == TURBO_FLOW_PROTOCOL_MQTT_SN || protocol == TURBO_FLOW_PROTOCOL_COAP ||
         protocol == TURBO_FLOW_PROTOCOL_LWM2M || protocol == TURBO_FLOW_PROTOCOL_OCPP;
}

static flow_protocol_session_t *
flow_protocol_runtime_session_find(turbo_flow_protocol_runtime_t *runtime, uint64_t session_id) {
  if (!runtime || session_id == 0u) return NULL;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    if (runtime->sessions[i].in_use && runtime->sessions[i].session_id == session_id)
      return &runtime->sessions[i];
  }
  return NULL;
}

static flow_protocol_session_t *
flow_protocol_runtime_session_free(turbo_flow_protocol_runtime_t *runtime) {
  if (!runtime) return NULL;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    if (!runtime->sessions[i].in_use) return &runtime->sessions[i];
  }
  return NULL;
}

static void flow_protocol_runtime_session_release(turbo_flow_protocol_runtime_t *runtime,
                                                  flow_protocol_session_t *session, int status) {
  uint8_t *buffer;
  if (!runtime || !session || !session->in_use) return;
  if (runtime->ops.session_closed)
    runtime->ops.session_closed(runtime->callback_ctx, session->session_id, session->generation,
                                status);
  buffer = session->buffer;
  memset(session, 0, sizeof(*session));
  session->buffer = buffer;
}

static int flow_protocol_runtime_frame_size(const turbo_flow_protocol_runtime_t *runtime,
                                            const flow_protocol_session_t *session, size_t *out) {
  const uint8_t *data;
  size_t size;
  if (!runtime || !session || !out) return SALTS_EINVAL;
  *out = 0u;
  data = session->buffer + session->buffer_offset;
  size = session->buffered;
  switch (runtime->protocol_info.protocol) {
  case TURBO_FLOW_PROTOCOL_GBT_32960: {
    size_t body_size;
    size_t frame_size;
    if (size == 0u) return SALTS_OK;
    if (data[0] != 0x23u || (size >= 2u && data[1] != 0x23u)) return SALTS_EPROTO;
    if (size < 24u) return SALTS_OK;
    body_size = ((size_t)data[22] << 8u) | data[23];
    if (body_size > SIZE_MAX - 25u) return SALTS_EMSGSIZE;
    frame_size = body_size + 25u;
    if (frame_size > runtime->config.max_frame_size) return SALTS_EMSGSIZE;
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
    return size == runtime->config.max_frame_size ? SALTS_EMSGSIZE : SALTS_OK;
  default:
    return SALTS_ENOTSUP;
  }
}

static int flow_protocol_runtime_next_delivery(turbo_flow_protocol_runtime_t *runtime,
                                               uint64_t *out) {
  if (!runtime || !out) return SALTS_EINVAL;
  if (runtime->next_delivery_id == UINT64_MAX) return SALTS_ERANGE;
  *out = runtime->next_delivery_id + 1u;
  if (*out == 0u) return SALTS_ERANGE;
  return SALTS_OK;
}

static int flow_protocol_runtime_dispatch(turbo_flow_protocol_runtime_t *runtime,
                                          flow_protocol_session_t *session, const uint8_t *data,
                                          size_t size, turbo_flow_protocol_feed_result_t *result,
                                          int *admitted) {
  turbo_flow_protocol_frame_view_t frame = TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
  turbo_flow_protocol_message_output_t message = TURBO_FLOW_PROTOCOL_MESSAGE_OUTPUT_INIT;
  turbo_flow_protocol_publish_request_t request = TURBO_FLOW_PROTOCOL_PUBLISH_REQUEST_INIT;
  turbo_flow_protocol_publish_disposition_t disposition =
      (turbo_flow_protocol_publish_disposition_t)0;
  uint64_t delivery_id;
  int rc;
  *admitted = 0;
  rc = flow_protocol_runtime_next_delivery(runtime, &delivery_id);
  if (rc != SALTS_OK) return rc;
  frame.data = data;
  frame.data_size = size;
  frame.device_id = session->device_id[0] != '\0' ? session->device_id : NULL;
  frame.protocol_version = session->protocol_version;
  message.payload = runtime->scratch_payload;
  message.payload_capacity = runtime->config.max_frame_size;
  rc = turbo_flow_protocol_decode(runtime->protocol, &frame, &message);
  if (rc != SALTS_OK) return rc;
  request.delivery_id = delivery_id;
  request.session_id = session->session_id;
  request.session_generation = session->generation;
  request.message = &message;
  rc = runtime->ops.publish(runtime->callback_ctx, &request, &disposition);
  if (rc != SALTS_OK) return rc;
  if (disposition != TURBO_FLOW_PROTOCOL_PUBLISH_SETTLED &&
      disposition != TURBO_FLOW_PROTOCOL_PUBLISH_PENDING)
    return SALTS_EPROTO;
  runtime->next_delivery_id = delivery_id;
  result->frames_dispatched++;
  *admitted = 1;
  if (disposition == TURBO_FLOW_PROTOCOL_PUBLISH_PENDING) {
    if (size > runtime->config.max_frame_size) return SALTS_EMSGSIZE;
    if (flow_protocol_runtime_is_message_protocol(runtime->protocol_info.protocol)) {
      if (data != session->buffer) memcpy(session->buffer, data, size);
      session->buffer_offset = 0u;
      session->buffered = size;
    }
    session->state = TURBO_FLOW_PROTOCOL_SESSION_WAIT_SETTLEMENT;
    session->pending_delivery_id = delivery_id;
    session->pending_frame_size = size;
    session->pending_metadata = message.metadata;
    result->pending_delivery_id = delivery_id;
  } else {
    turbo_flow_protocol_frame_view_t reply_request = TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
    turbo_flow_protocol_frame_output_t reply = TURBO_FLOW_PROTOCOL_FRAME_OUTPUT_INIT;
    if (runtime->ops.reply &&
        (runtime->protocol_info.capabilities & TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY) != 0u) {
      reply_request.data = data;
      reply_request.data_size = size;
      reply_request.device_id = session->device_id[0] != '\0' ? session->device_id : NULL;
      reply_request.protocol_version = session->protocol_version;
      reply.data = runtime->scratch_payload;
      reply.capacity = runtime->config.max_frame_size;
      rc = turbo_flow_protocol_reply(runtime->protocol, &reply_request, SALTS_OK, &reply);
      if (rc != SALTS_OK) return rc;
      if (reply.data_size > 0u) {
        rc = runtime->ops.reply(runtime->callback_ctx, session->session_id, session->generation,
                                delivery_id, &reply);
        if (rc != SALTS_OK) return rc;
      }
    }
    if (runtime->ops.settled)
      runtime->ops.settled(runtime->callback_ctx, session->session_id, session->generation,
                           delivery_id, &message.metadata, SALTS_OK);
  }
  return SALTS_OK;
}

static int flow_protocol_runtime_pump(turbo_flow_protocol_runtime_t *runtime,
                                      flow_protocol_session_t *session,
                                      turbo_flow_protocol_feed_result_t *result) {
  while (session->state == TURBO_FLOW_PROTOCOL_SESSION_OPEN && session->buffered > 0u) {
    size_t frame_size = 0u;
    int admitted = 0;
    int rc = flow_protocol_runtime_frame_size(runtime, session, &frame_size);
    if (rc != SALTS_OK) {
      session->state = TURBO_FLOW_PROTOCOL_SESSION_FAILED;
      session->failure_status = rc;
      return rc;
    }
    if (frame_size == 0u) return SALTS_OK;
    rc = flow_protocol_runtime_dispatch(runtime, session, session->buffer + session->buffer_offset,
                                        frame_size, result, &admitted);
    if (rc == SALTS_EBUSY || rc == SALTS_ENOSPC || rc == SALTS_ENOBUFS) {
      result->backpressured = 1u;
      return SALTS_OK;
    }
    if (rc != SALTS_OK) {
      session->state = TURBO_FLOW_PROTOCOL_SESSION_FAILED;
      session->failure_status = rc;
      return rc;
    }
    if (!admitted) return SALTS_EPROTO;
    if (session->state == TURBO_FLOW_PROTOCOL_SESSION_WAIT_SETTLEMENT) {
      if (session->buffered > 0u) result->backpressured = 1u;
      return SALTS_OK;
    }
    session->buffer_offset += frame_size;
    session->buffered -= frame_size;
    if (session->buffered == 0u) session->buffer_offset = 0u;
  }
  return SALTS_OK;
}

int turbo_flow_protocol_runtime_create(turbo_flow_protocol_t *protocol,
                                       const turbo_flow_protocol_runtime_config_t *config,
                                       const turbo_flow_protocol_runtime_ops_t *ops, void *ctx,
                                       turbo_flow_protocol_runtime_t **out) {
  turbo_flow_protocol_runtime_t *runtime;
  turbo_flow_protocol_info_t info = TURBO_FLOW_PROTOCOL_INFO_INIT;
  size_t buffer_count;
  size_t buffer_bytes;
  int rc;
  if (out) *out = NULL;
  if (!protocol || !config || config->size < sizeof(*config) ||
      config->abi_version != TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION ||
      config->max_sessions == 0u || config->max_frame_size == 0u || !ops ||
      ops->size < offsetof(turbo_flow_protocol_runtime_ops_t, reply) ||
      ops->abi_version != TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION || !ops->publish || !out)
    return SALTS_EINVAL;
  rc = turbo_flow_protocol_get_info(protocol, &info);
  if (rc != SALTS_OK) return rc;
  if (config->max_frame_size > info.max_frame_size) return SALTS_ERANGE;
  if (config->max_sessions == SIZE_MAX) return SALTS_EMSGSIZE;
  buffer_count = config->max_sessions + 1u;
  if (config->max_frame_size > SIZE_MAX / buffer_count) return SALTS_EMSGSIZE;
  buffer_bytes = buffer_count * config->max_frame_size;
  if (buffer_bytes > config->max_buffered_bytes) return SALTS_ENOSPC;
  runtime = (turbo_flow_protocol_runtime_t *)calloc(1u, sizeof(*runtime));
  if (!runtime) return SALTS_ENOMEM;
  runtime->sessions =
      (flow_protocol_session_t *)calloc(config->max_sessions, sizeof(*runtime->sessions));
  runtime->storage = (uint8_t *)calloc(1u, buffer_bytes);
  if (!runtime->sessions || !runtime->storage) {
    free(runtime->storage);
    free(runtime->sessions);
    free(runtime);
    return SALTS_ENOMEM;
  }
  for (size_t i = 0u; i < config->max_sessions; ++i)
    runtime->sessions[i].buffer = runtime->storage + i * config->max_frame_size;
  runtime->scratch_payload = runtime->storage + config->max_sessions * config->max_frame_size;
  runtime->protocol = protocol;
  runtime->protocol_info = info;
  runtime->config = *config;
  memset(&runtime->ops, 0, sizeof(runtime->ops));
  memcpy(&runtime->ops, ops, ops->size < sizeof(runtime->ops) ? ops->size : sizeof(runtime->ops));
  runtime->callback_ctx = ctx;
  runtime->accepting = 1;
  *out = runtime;
  return SALTS_OK;
}

int turbo_flow_protocol_runtime_destroy(turbo_flow_protocol_runtime_t *runtime) {
  if (!runtime) return SALTS_OK;
  if (runtime->accepting) return SALTS_EBUSY;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    if (runtime->sessions[i].in_use) return SALTS_EBUSY;
  }
  free(runtime->storage);
  free(runtime->sessions);
  free(runtime);
  return SALTS_OK;
}

int turbo_flow_protocol_runtime_session_open(
    turbo_flow_protocol_runtime_t *runtime,
    const turbo_flow_protocol_session_open_request_t *request) {
  flow_protocol_session_t *session;
  const char *version;
  int rc;
  if (!runtime || !request || request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION ||
      request->session_id == 0u || request->generation == 0u)
    return SALTS_EINVAL;
  if (!runtime->accepting) return SALTS_ESHUTDOWN;
  session = flow_protocol_runtime_session_find(runtime, request->session_id);
  if (session) return session->generation == request->generation ? SALTS_EALREADY : SALTS_EBUSY;
  session = flow_protocol_runtime_session_free(runtime);
  if (!session) return SALTS_ENOSPC;
  version = request->protocol_version && request->protocol_version[0] != '\0'
                ? request->protocol_version
                : runtime->protocol_info.protocol_version;
  if (strcmp(version, runtime->protocol_info.protocol_version) != 0) return SALTS_ENOTSUP;
  rc = flow_protocol_runtime_text_copy(
      session->device_id, sizeof(session->device_id), request->device_id,
      runtime->protocol_info.protocol == TURBO_FLOW_PROTOCOL_GBT_32960 ||
          runtime->protocol_info.protocol == TURBO_FLOW_PROTOCOL_JTT_808);
  if (rc != SALTS_OK) return rc;
  rc = flow_protocol_runtime_text_copy(session->protocol_version, sizeof(session->protocol_version),
                                       version, 0);
  if (rc != SALTS_OK) {
    session->device_id[0] = '\0';
    return rc;
  }
  session->in_use = 1;
  session->state = TURBO_FLOW_PROTOCOL_SESSION_OPEN;
  session->session_id = request->session_id;
  session->generation = request->generation;
  session->failure_status = SALTS_OK;
  return SALTS_OK;
}

static int flow_protocol_runtime_result_validate(turbo_flow_protocol_feed_result_t *result) {
  if (!result || result->size < sizeof(*result) ||
      result->abi_version != TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION)
    return SALTS_EINVAL;
  *result = (turbo_flow_protocol_feed_result_t)TURBO_FLOW_PROTOCOL_FEED_RESULT_INIT;
  return SALTS_OK;
}

int turbo_flow_protocol_runtime_session_feed(turbo_flow_protocol_runtime_t *runtime,
                                             uint64_t session_id, uint64_t generation,
                                             const uint8_t *data, size_t size,
                                             turbo_flow_protocol_feed_result_t *result) {
  flow_protocol_session_t *session;
  int message_protocol;
  int admitted = 0;
  int rc = flow_protocol_runtime_result_validate(result);
  if (rc != SALTS_OK || !runtime || session_id == 0u || generation == 0u || (!data && size != 0u))
    return SALTS_EINVAL;
  if (!runtime->accepting) return SALTS_ESHUTDOWN;
  session = flow_protocol_runtime_session_find(runtime, session_id);
  if (!session) return SALTS_ENOENT;
  if (session->generation != generation) return SALTS_EPROTO;
  if (session->state == TURBO_FLOW_PROTOCOL_SESSION_FAILED) return session->failure_status;
  if (session->state == TURBO_FLOW_PROTOCOL_SESSION_DRAINING) return SALTS_ESHUTDOWN;
  message_protocol = flow_protocol_runtime_is_message_protocol(runtime->protocol_info.protocol);
  if (message_protocol) {
    if (size == 0u) return SALTS_EINVAL;
    if (size > runtime->config.max_frame_size) return SALTS_EMSGSIZE;
    if (session->state == TURBO_FLOW_PROTOCOL_SESSION_WAIT_SETTLEMENT) {
      result->backpressured = 1u;
      return SALTS_EBUSY;
    }
    rc = flow_protocol_runtime_dispatch(runtime, session, data, size, result, &admitted);
    if (rc == SALTS_EBUSY || rc == SALTS_ENOSPC || rc == SALTS_ENOBUFS) {
      result->backpressured = 1u;
      return SALTS_EBUSY;
    }
    if (rc != SALTS_OK) {
      session->state = TURBO_FLOW_PROTOCOL_SESSION_FAILED;
      session->failure_status = rc;
      return rc;
    }
    if (!admitted) return SALTS_EPROTO;
    result->accepted_size = size;
    return SALTS_OK;
  }
  if (session->buffer_offset > runtime->config.max_frame_size ||
      session->buffered > runtime->config.max_frame_size - session->buffer_offset)
    return SALTS_EPROTO;
  if (size > runtime->config.max_frame_size - session->buffered) return SALTS_ENOBUFS;
  if (size > 0u) {
    size_t tail = session->buffer_offset + session->buffered;
    /* Compaction is O(buffered), but happens at most once per feed instead of once per frame. */
    if (size > runtime->config.max_frame_size - tail) {
      if (session->buffered > 0u)
        memmove(session->buffer, session->buffer + session->buffer_offset, session->buffered);
      session->buffer_offset = 0u;
      tail = session->buffered;
    }
    memcpy(session->buffer + tail, data, size);
    session->buffered += size;
  }
  result->accepted_size = size;
  if (session->state == TURBO_FLOW_PROTOCOL_SESSION_WAIT_SETTLEMENT) {
    result->backpressured = 1u;
    result->pending_delivery_id = session->pending_delivery_id;
    return SALTS_OK;
  }
  return flow_protocol_runtime_pump(runtime, session, result);
}

int turbo_flow_protocol_runtime_settle(turbo_flow_protocol_runtime_t *runtime, uint64_t delivery_id,
                                       int status, turbo_flow_protocol_feed_result_t *result) {
  flow_protocol_session_t *session = NULL;
  int rc = flow_protocol_runtime_result_validate(result);
  if (rc != SALTS_OK || !runtime || delivery_id == 0u) return SALTS_EINVAL;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    if (runtime->sessions[i].in_use && runtime->sessions[i].pending_delivery_id == delivery_id) {
      session = &runtime->sessions[i];
      break;
    }
  }
  if (!session) return SALTS_ENOENT;
  if (session->pending_frame_size == 0u || session->pending_frame_size > session->buffered)
    return SALTS_EPROTO;
  if (runtime->ops.reply &&
      (runtime->protocol_info.capabilities & TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY) != 0u) {
    turbo_flow_protocol_frame_view_t request = TURBO_FLOW_PROTOCOL_FRAME_VIEW_INIT;
    turbo_flow_protocol_frame_output_t reply = TURBO_FLOW_PROTOCOL_FRAME_OUTPUT_INIT;
    request.data = session->buffer + session->buffer_offset;
    request.data_size = session->pending_frame_size;
    request.device_id = session->device_id[0] != '\0' ? session->device_id : NULL;
    request.protocol_version = session->protocol_version;
    reply.data = runtime->scratch_payload;
    reply.capacity = runtime->config.max_frame_size;
    rc = turbo_flow_protocol_reply(runtime->protocol, &request, status, &reply);
    if (rc != SALTS_OK) {
      flow_protocol_runtime_session_release(runtime, session, rc);
      return rc;
    }
    if (reply.data_size > 0u) {
      rc = runtime->ops.reply(runtime->callback_ctx, session->session_id, session->generation,
                              delivery_id, &reply);
      if (rc != SALTS_OK) {
        flow_protocol_runtime_session_release(runtime, session, rc);
        return rc;
      }
    }
  }
  if (runtime->ops.settled)
    runtime->ops.settled(runtime->callback_ctx, session->session_id, session->generation,
                         delivery_id, &session->pending_metadata, status);
  session->pending_delivery_id = 0u;
  session->buffer_offset += session->pending_frame_size;
  session->buffered -= session->pending_frame_size;
  if (session->buffered == 0u) session->buffer_offset = 0u;
  session->pending_frame_size = 0u;
  session->pending_metadata = (turbo_flow_protocol_metadata_t)TURBO_FLOW_PROTOCOL_METADATA_INIT;
  if (session->state == TURBO_FLOW_PROTOCOL_SESSION_DRAINING) {
    flow_protocol_runtime_session_release(runtime, session,
                                          status == SALTS_OK ? SALTS_ESHUTDOWN : status);
    return SALTS_OK;
  }
  if (status != SALTS_OK) {
    flow_protocol_runtime_session_release(runtime, session, status);
    return SALTS_OK;
  }
  session->state = TURBO_FLOW_PROTOCOL_SESSION_OPEN;
  return flow_protocol_runtime_pump(runtime, session, result);
}

int turbo_flow_protocol_runtime_session_close(turbo_flow_protocol_runtime_t *runtime,
                                              uint64_t session_id, uint64_t generation,
                                              int status) {
  flow_protocol_session_t *session;
  if (!runtime || session_id == 0u || generation == 0u) return SALTS_EINVAL;
  session = flow_protocol_runtime_session_find(runtime, session_id);
  if (!session) return SALTS_ENOENT;
  if (session->generation != generation) return SALTS_EPROTO;
  if (session->pending_delivery_id != 0u && runtime->ops.settled)
    runtime->ops.settled(runtime->callback_ctx, session->session_id, session->generation,
                         session->pending_delivery_id, &session->pending_metadata, status);
  flow_protocol_runtime_session_release(runtime, session, status);
  return SALTS_OK;
}

int turbo_flow_protocol_runtime_begin_shutdown(turbo_flow_protocol_runtime_t *runtime) {
  size_t pending = 0u;
  if (!runtime) return SALTS_EINVAL;
  runtime->accepting = 0;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    flow_protocol_session_t *session = &runtime->sessions[i];
    if (!session->in_use) continue;
    if (session->pending_delivery_id != 0u) {
      session->buffered = session->pending_frame_size;
      session->state = TURBO_FLOW_PROTOCOL_SESSION_DRAINING;
      pending++;
    } else {
      session->buffered = 0u;
      flow_protocol_runtime_session_release(runtime, session, SALTS_ESHUTDOWN);
    }
  }
  return pending == 0u ? SALTS_OK : SALTS_EBUSY;
}

int turbo_flow_protocol_runtime_force_shutdown(turbo_flow_protocol_runtime_t *runtime, int status) {
  if (!runtime || status == SALTS_OK) return SALTS_EINVAL;
  runtime->accepting = 0;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    flow_protocol_session_t *session = &runtime->sessions[i];
    if (!session->in_use) continue;
    if (session->pending_delivery_id != 0u && runtime->ops.settled)
      runtime->ops.settled(runtime->callback_ctx, session->session_id, session->generation,
                           session->pending_delivery_id, &session->pending_metadata, status);
    flow_protocol_runtime_session_release(runtime, session, status);
  }
  return SALTS_OK;
}

int turbo_flow_protocol_runtime_snapshot(const turbo_flow_protocol_runtime_t *runtime,
                                         turbo_flow_protocol_runtime_snapshot_t *out) {
  turbo_flow_protocol_runtime_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_RUNTIME_SNAPSHOT_INIT;
  if (!runtime || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_PROTOCOL_RUNTIME_ABI_VERSION)
    return SALTS_EINVAL;
  snapshot.accepting = runtime->accepting ? 1u : 0u;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    const flow_protocol_session_t *session = &runtime->sessions[i];
    if (!session->in_use) continue;
    snapshot.active_sessions++;
    snapshot.buffered_bytes += session->buffered;
    if (session->pending_delivery_id != 0u) snapshot.pending_settlements++;
  }
  *out = snapshot;
  return SALTS_OK;
}
