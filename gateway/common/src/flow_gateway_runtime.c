#include "turbo_flow_gateway_runtime.h"

#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

#define FLOW_GATEWAY_RUNTIME_TOPIC_CAPACITY 384u

typedef struct flow_gateway_session_s {
  int in_use;
  turbo_flow_gateway_session_state_t state;
  int failure_status;
  uint64_t session_id;
  uint64_t generation;
  char device_id[TURBO_FLOW_GATEWAY_DEVICE_ID_MAX + 1u];
  char protocol_version[TURBO_FLOW_GATEWAY_VERSION_MAX + 1u];
  uint8_t *buffer;
  size_t buffered;
  uint64_t pending_delivery_id;
  size_t pending_frame_size;
  turbo_flow_gateway_metadata_t pending_metadata;
} flow_gateway_session_t;

struct turbo_flow_gateway_runtime_s {
  turbo_flow_gateway_t *gateway;
  turbo_flow_gateway_info_t gateway_info;
  turbo_flow_gateway_runtime_config_t config;
  turbo_flow_gateway_runtime_ops_t ops;
  void *callback_ctx;
  flow_gateway_session_t *sessions;
  uint8_t *storage;
  uint8_t *scratch_payload;
  char scratch_topic[FLOW_GATEWAY_RUNTIME_TOPIC_CAPACITY];
  uint64_t next_delivery_id;
  int accepting;
};

static int flow_gateway_runtime_text_copy(char *out, size_t capacity,
                                          const char *text, int optional) {
  size_t length;
  if (!out || capacity == 0u || (!optional && (!text || !text[0])))
    return TURBO_EINVAL;
  if (!text || !text[0]) {
    out[0] = '\0';
    return TURBO_OK;
  }
  for (length = 0u; length < capacity && text[length] != '\0'; ++length) {
  }
  if (length >= capacity) return TURBO_EMSGSIZE;
  memcpy(out, text, length + 1u);
  return TURBO_OK;
}

static int flow_gateway_runtime_is_message_protocol(
    turbo_flow_gateway_protocol_t protocol) {
  return protocol == TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN ||
         protocol == TURBO_FLOW_GATEWAY_PROTOCOL_COAP ||
         protocol == TURBO_FLOW_GATEWAY_PROTOCOL_LWM2M ||
         protocol == TURBO_FLOW_GATEWAY_PROTOCOL_OCPP;
}

static flow_gateway_session_t *flow_gateway_runtime_session_find(
    turbo_flow_gateway_runtime_t *runtime, uint64_t session_id) {
  if (!runtime || session_id == 0u) return NULL;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    if (runtime->sessions[i].in_use &&
        runtime->sessions[i].session_id == session_id)
      return &runtime->sessions[i];
  }
  return NULL;
}

static flow_gateway_session_t *flow_gateway_runtime_session_free(
    turbo_flow_gateway_runtime_t *runtime) {
  if (!runtime) return NULL;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    if (!runtime->sessions[i].in_use) return &runtime->sessions[i];
  }
  return NULL;
}

static void flow_gateway_runtime_session_release(
    turbo_flow_gateway_runtime_t *runtime, flow_gateway_session_t *session,
    int status) {
  uint8_t *buffer;
  if (!runtime || !session || !session->in_use) return;
  if (runtime->ops.session_closed)
    runtime->ops.session_closed(runtime->callback_ctx, session->session_id,
                                session->generation, status);
  buffer = session->buffer;
  memset(session, 0, sizeof(*session));
  session->buffer = buffer;
}

static int flow_gateway_runtime_frame_size(
    const turbo_flow_gateway_runtime_t *runtime,
    const flow_gateway_session_t *session, size_t *out) {
  const uint8_t *data;
  size_t size;
  if (!runtime || !session || !out) return TURBO_EINVAL;
  *out = 0u;
  data = session->buffer;
  size = session->buffered;
  switch (runtime->gateway_info.protocol) {
  case TURBO_FLOW_GATEWAY_PROTOCOL_GBT_32960: {
    size_t body_size;
    size_t frame_size;
    if (size == 0u) return TURBO_OK;
    if (data[0] != 0x23u || (size >= 2u && data[1] != 0x23u))
      return TURBO_EPROTO;
    if (size < 24u) return TURBO_OK;
    body_size = ((size_t)data[22] << 8u) | data[23];
    if (body_size > SIZE_MAX - 25u) return TURBO_EMSGSIZE;
    frame_size = body_size + 25u;
    if (frame_size > runtime->config.max_frame_size)
      return TURBO_EMSGSIZE;
    if (size >= frame_size) *out = frame_size;
    return TURBO_OK;
  }
  case TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808:
    if (size == 0u) return TURBO_OK;
    if (data[0] != 0x7eu) return TURBO_EPROTO;
    for (size_t i = 1u; i < size; ++i) {
      if (data[i] == 0x7eu) {
        *out = i + 1u;
        return TURBO_OK;
      }
    }
    return size == runtime->config.max_frame_size ? TURBO_EMSGSIZE
                                                   : TURBO_OK;
  default:
    return TURBO_ENOTSUP;
  }
}

static int flow_gateway_runtime_next_delivery(
    turbo_flow_gateway_runtime_t *runtime, uint64_t *out) {
  if (!runtime || !out) return TURBO_EINVAL;
  if (runtime->next_delivery_id == UINT64_MAX) return TURBO_ERANGE;
  *out = runtime->next_delivery_id + 1u;
  if (*out == 0u) return TURBO_ERANGE;
  return TURBO_OK;
}

static int flow_gateway_runtime_dispatch(
    turbo_flow_gateway_runtime_t *runtime, flow_gateway_session_t *session,
    const uint8_t *data, size_t size,
    turbo_flow_gateway_feed_result_t *result, int *admitted) {
  turbo_flow_gateway_frame_view_t frame =
      TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
  turbo_flow_gateway_mqtt_output_t message =
      TURBO_FLOW_GATEWAY_MQTT_OUTPUT_INIT;
  turbo_flow_gateway_publish_request_t request =
      TURBO_FLOW_GATEWAY_PUBLISH_REQUEST_INIT;
  turbo_flow_gateway_publish_disposition_t disposition =
      (turbo_flow_gateway_publish_disposition_t)0;
  uint64_t delivery_id;
  int rc;
  *admitted = 0;
  rc = flow_gateway_runtime_next_delivery(runtime, &delivery_id);
  if (rc != TURBO_OK) return rc;
  frame.data = data;
  frame.data_size = size;
  frame.device_id =
      session->device_id[0] != '\0' ? session->device_id : NULL;
  frame.protocol_version = session->protocol_version;
  message.topic = runtime->scratch_topic;
  message.topic_capacity = sizeof(runtime->scratch_topic);
  message.payload = runtime->scratch_payload;
  message.payload_capacity = runtime->config.max_frame_size;
  rc = turbo_flow_gateway_ingress(runtime->gateway, &frame, &message);
  if (rc != TURBO_OK) return rc;
  request.delivery_id = delivery_id;
  request.session_id = session->session_id;
  request.session_generation = session->generation;
  request.message = &message;
  rc = runtime->ops.publish(runtime->callback_ctx, &request, &disposition);
  if (rc != TURBO_OK) return rc;
  if (disposition != TURBO_FLOW_GATEWAY_PUBLISH_SETTLED &&
      disposition != TURBO_FLOW_GATEWAY_PUBLISH_PENDING)
    return TURBO_EPROTO;
  runtime->next_delivery_id = delivery_id;
  result->frames_dispatched++;
  *admitted = 1;
  if (disposition == TURBO_FLOW_GATEWAY_PUBLISH_PENDING) {
    if (size > runtime->config.max_frame_size) return TURBO_EMSGSIZE;
    if (data != session->buffer)
      memcpy(session->buffer, data, size);
    if (flow_gateway_runtime_is_message_protocol(
            runtime->gateway_info.protocol))
      session->buffered = size;
    session->state = TURBO_FLOW_GATEWAY_SESSION_WAIT_SETTLEMENT;
    session->pending_delivery_id = delivery_id;
    session->pending_frame_size = size;
    session->pending_metadata = message.metadata;
    result->pending_delivery_id = delivery_id;
  } else {
    turbo_flow_gateway_frame_view_t reply_request =
        TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
    turbo_flow_gateway_frame_output_t reply =
        TURBO_FLOW_GATEWAY_FRAME_OUTPUT_INIT;
    if (runtime->ops.reply &&
        (runtime->gateway_info.capabilities &
         TURBO_FLOW_GATEWAY_CAP_PROTOCOL_REPLY) != 0u) {
      reply_request.data = data;
      reply_request.data_size = size;
      reply_request.device_id =
          session->device_id[0] != '\0' ? session->device_id : NULL;
      reply_request.protocol_version = session->protocol_version;
      reply.data = runtime->scratch_payload;
      reply.capacity = runtime->config.max_frame_size;
      rc = turbo_flow_gateway_reply(runtime->gateway, &reply_request,
                                    TURBO_OK, &reply);
      if (rc != TURBO_OK) return rc;
      if (reply.data_size > 0u) {
        rc = runtime->ops.reply(runtime->callback_ctx, session->session_id,
                                session->generation, delivery_id, &reply);
        if (rc != TURBO_OK) return rc;
      }
    }
    if (runtime->ops.settled)
      runtime->ops.settled(runtime->callback_ctx, session->session_id,
                           session->generation, delivery_id,
                           &message.metadata, TURBO_OK);
  }
  return TURBO_OK;
}

static int flow_gateway_runtime_pump(
    turbo_flow_gateway_runtime_t *runtime, flow_gateway_session_t *session,
    turbo_flow_gateway_feed_result_t *result) {
  while (session->state == TURBO_FLOW_GATEWAY_SESSION_OPEN &&
         session->buffered > 0u) {
    size_t frame_size = 0u;
    int admitted = 0;
    int rc = flow_gateway_runtime_frame_size(runtime, session, &frame_size);
    if (rc != TURBO_OK) {
      session->state = TURBO_FLOW_GATEWAY_SESSION_FAILED;
      session->failure_status = rc;
      return rc;
    }
    if (frame_size == 0u) return TURBO_OK;
    rc = flow_gateway_runtime_dispatch(runtime, session, session->buffer,
                                       frame_size, result, &admitted);
    if (rc == TURBO_EBUSY || rc == TURBO_ENOSPC ||
        rc == TURBO_ENOBUFS) {
      result->backpressured = 1u;
      return TURBO_OK;
    }
    if (rc != TURBO_OK) {
      session->state = TURBO_FLOW_GATEWAY_SESSION_FAILED;
      session->failure_status = rc;
      return rc;
    }
    if (!admitted) return TURBO_EPROTO;
    if (session->state ==
        TURBO_FLOW_GATEWAY_SESSION_WAIT_SETTLEMENT) {
      if (session->buffered > 0u) result->backpressured = 1u;
      return TURBO_OK;
    }
    session->buffered -= frame_size;
    if (session->buffered > 0u)
      memmove(session->buffer, session->buffer + frame_size,
              session->buffered);
  }
  return TURBO_OK;
}

int turbo_flow_gateway_runtime_create(
    turbo_flow_gateway_t *gateway,
    const turbo_flow_gateway_runtime_config_t *config,
    const turbo_flow_gateway_runtime_ops_t *ops, void *ctx,
    turbo_flow_gateway_runtime_t **out) {
  turbo_flow_gateway_runtime_t *runtime;
  turbo_flow_gateway_info_t info = TURBO_FLOW_GATEWAY_INFO_INIT;
  size_t buffer_count;
  size_t buffer_bytes;
  int rc;
  if (out) *out = NULL;
  if (!gateway || !config || config->size < sizeof(*config) ||
      config->abi_version != TURBO_FLOW_GATEWAY_RUNTIME_ABI_VERSION ||
      config->max_sessions == 0u || config->max_frame_size == 0u ||
      !ops || ops->size < offsetof(turbo_flow_gateway_runtime_ops_t, reply) ||
      ops->abi_version != TURBO_FLOW_GATEWAY_RUNTIME_ABI_VERSION ||
      !ops->publish || !out)
    return TURBO_EINVAL;
  rc = turbo_flow_gateway_get_info(gateway, &info);
  if (rc != TURBO_OK) return rc;
  if (config->max_frame_size > info.max_frame_size)
    return TURBO_ERANGE;
  if (config->max_sessions == SIZE_MAX)
    return TURBO_EMSGSIZE;
  buffer_count = config->max_sessions + 1u;
  if (config->max_frame_size > SIZE_MAX / buffer_count)
    return TURBO_EMSGSIZE;
  buffer_bytes = buffer_count * config->max_frame_size;
  if (buffer_bytes > config->max_buffered_bytes)
    return TURBO_ENOSPC;
  runtime =
      (turbo_flow_gateway_runtime_t *)calloc(1u, sizeof(*runtime));
  if (!runtime) return TURBO_ENOMEM;
  runtime->sessions = (flow_gateway_session_t *)calloc(
      config->max_sessions, sizeof(*runtime->sessions));
  runtime->storage = (uint8_t *)calloc(1u, buffer_bytes);
  if (!runtime->sessions || !runtime->storage) {
    free(runtime->storage);
    free(runtime->sessions);
    free(runtime);
    return TURBO_ENOMEM;
  }
  for (size_t i = 0u; i < config->max_sessions; ++i)
    runtime->sessions[i].buffer =
        runtime->storage + i * config->max_frame_size;
  runtime->scratch_payload =
      runtime->storage + config->max_sessions * config->max_frame_size;
  runtime->gateway = gateway;
  runtime->gateway_info = info;
  runtime->config = *config;
  memset(&runtime->ops, 0, sizeof(runtime->ops));
  memcpy(&runtime->ops, ops,
         ops->size < sizeof(runtime->ops) ? ops->size
                                         : sizeof(runtime->ops));
  runtime->callback_ctx = ctx;
  runtime->accepting = 1;
  *out = runtime;
  return TURBO_OK;
}

int turbo_flow_gateway_runtime_destroy(
    turbo_flow_gateway_runtime_t *runtime) {
  if (!runtime) return TURBO_OK;
  if (runtime->accepting) return TURBO_EBUSY;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    if (runtime->sessions[i].in_use) return TURBO_EBUSY;
  }
  free(runtime->storage);
  free(runtime->sessions);
  free(runtime);
  return TURBO_OK;
}

int turbo_flow_gateway_runtime_session_open(
    turbo_flow_gateway_runtime_t *runtime,
    const turbo_flow_gateway_session_open_request_t *request) {
  flow_gateway_session_t *session;
  const char *version;
  int rc;
  if (!runtime || !request || request->size < sizeof(*request) ||
      request->abi_version != TURBO_FLOW_GATEWAY_RUNTIME_ABI_VERSION ||
      request->session_id == 0u || request->generation == 0u)
    return TURBO_EINVAL;
  if (!runtime->accepting) return TURBO_ESHUTDOWN;
  session =
      flow_gateway_runtime_session_find(runtime, request->session_id);
  if (session)
    return session->generation == request->generation ? TURBO_EALREADY
                                                      : TURBO_EBUSY;
  session = flow_gateway_runtime_session_free(runtime);
  if (!session) return TURBO_ENOSPC;
  version =
      request->protocol_version && request->protocol_version[0] != '\0'
          ? request->protocol_version
          : runtime->gateway_info.protocol_version;
  if (strcmp(version, runtime->gateway_info.protocol_version) != 0)
    return TURBO_ENOTSUP;
  rc = flow_gateway_runtime_text_copy(
      session->device_id, sizeof(session->device_id), request->device_id,
      runtime->gateway_info.protocol ==
              TURBO_FLOW_GATEWAY_PROTOCOL_GBT_32960 ||
          runtime->gateway_info.protocol ==
              TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808);
  if (rc != TURBO_OK) return rc;
  rc = flow_gateway_runtime_text_copy(
      session->protocol_version, sizeof(session->protocol_version), version,
      0);
  if (rc != TURBO_OK) {
    session->device_id[0] = '\0';
    return rc;
  }
  session->in_use = 1;
  session->state = TURBO_FLOW_GATEWAY_SESSION_OPEN;
  session->session_id = request->session_id;
  session->generation = request->generation;
  session->failure_status = TURBO_OK;
  return TURBO_OK;
}

static int flow_gateway_runtime_result_validate(
    turbo_flow_gateway_feed_result_t *result) {
  if (!result || result->size < sizeof(*result) ||
      result->abi_version != TURBO_FLOW_GATEWAY_RUNTIME_ABI_VERSION)
    return TURBO_EINVAL;
  *result = (turbo_flow_gateway_feed_result_t)
      TURBO_FLOW_GATEWAY_FEED_RESULT_INIT;
  return TURBO_OK;
}

int turbo_flow_gateway_runtime_session_feed(
    turbo_flow_gateway_runtime_t *runtime, uint64_t session_id,
    uint64_t generation, const uint8_t *data, size_t size,
    turbo_flow_gateway_feed_result_t *result) {
  flow_gateway_session_t *session;
  int message_protocol;
  int admitted = 0;
  int rc = flow_gateway_runtime_result_validate(result);
  if (rc != TURBO_OK || !runtime || session_id == 0u || generation == 0u ||
      (!data && size != 0u))
    return TURBO_EINVAL;
  if (!runtime->accepting) return TURBO_ESHUTDOWN;
  session = flow_gateway_runtime_session_find(runtime, session_id);
  if (!session) return TURBO_ENOENT;
  if (session->generation != generation) return TURBO_EPROTO;
  if (session->state == TURBO_FLOW_GATEWAY_SESSION_FAILED)
    return session->failure_status;
  if (session->state == TURBO_FLOW_GATEWAY_SESSION_DRAINING)
    return TURBO_ESHUTDOWN;
  message_protocol =
      flow_gateway_runtime_is_message_protocol(runtime->gateway_info.protocol);
  if (message_protocol) {
    if (size == 0u) return TURBO_EINVAL;
    if (size > runtime->config.max_frame_size) return TURBO_EMSGSIZE;
    if (session->state ==
        TURBO_FLOW_GATEWAY_SESSION_WAIT_SETTLEMENT) {
      result->backpressured = 1u;
      return TURBO_EBUSY;
    }
    rc = flow_gateway_runtime_dispatch(runtime, session, data, size, result,
                                       &admitted);
    if (rc == TURBO_EBUSY || rc == TURBO_ENOSPC ||
        rc == TURBO_ENOBUFS) {
      result->backpressured = 1u;
      return TURBO_EBUSY;
    }
    if (rc != TURBO_OK) {
      session->state = TURBO_FLOW_GATEWAY_SESSION_FAILED;
      session->failure_status = rc;
      return rc;
    }
    if (!admitted) return TURBO_EPROTO;
    result->accepted_size = size;
    return TURBO_OK;
  }
  if (size > runtime->config.max_frame_size - session->buffered)
    return TURBO_ENOBUFS;
  if (size > 0u) {
    memcpy(session->buffer + session->buffered, data, size);
    session->buffered += size;
    result->accepted_size = size;
  }
  if (session->state ==
      TURBO_FLOW_GATEWAY_SESSION_WAIT_SETTLEMENT) {
    result->backpressured = 1u;
    result->pending_delivery_id = session->pending_delivery_id;
    return TURBO_OK;
  }
  return flow_gateway_runtime_pump(runtime, session, result);
}

int turbo_flow_gateway_runtime_settle(
    turbo_flow_gateway_runtime_t *runtime, uint64_t delivery_id, int status,
    turbo_flow_gateway_feed_result_t *result) {
  flow_gateway_session_t *session = NULL;
  int rc = flow_gateway_runtime_result_validate(result);
  if (rc != TURBO_OK || !runtime || delivery_id == 0u)
    return TURBO_EINVAL;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    if (runtime->sessions[i].in_use &&
        runtime->sessions[i].pending_delivery_id == delivery_id) {
      session = &runtime->sessions[i];
      break;
    }
  }
  if (!session) return TURBO_ENOENT;
  if (session->pending_frame_size == 0u ||
      session->pending_frame_size > session->buffered)
    return TURBO_EPROTO;
  if (runtime->ops.reply &&
      (runtime->gateway_info.capabilities &
       TURBO_FLOW_GATEWAY_CAP_PROTOCOL_REPLY) != 0u) {
    turbo_flow_gateway_frame_view_t request =
        TURBO_FLOW_GATEWAY_FRAME_VIEW_INIT;
    turbo_flow_gateway_frame_output_t reply =
        TURBO_FLOW_GATEWAY_FRAME_OUTPUT_INIT;
    request.data = session->buffer;
    request.data_size = session->pending_frame_size;
    request.device_id =
        session->device_id[0] != '\0' ? session->device_id : NULL;
    request.protocol_version = session->protocol_version;
    reply.data = runtime->scratch_payload;
    reply.capacity = runtime->config.max_frame_size;
    rc = turbo_flow_gateway_reply(runtime->gateway, &request, status, &reply);
    if (rc != TURBO_OK) {
      flow_gateway_runtime_session_release(runtime, session, rc);
      return rc;
    }
    if (reply.data_size > 0u) {
      rc = runtime->ops.reply(
          runtime->callback_ctx, session->session_id, session->generation,
          delivery_id, &reply);
      if (rc != TURBO_OK) {
        flow_gateway_runtime_session_release(runtime, session, rc);
        return rc;
      }
    }
  }
  if (runtime->ops.settled)
    runtime->ops.settled(
        runtime->callback_ctx, session->session_id, session->generation,
        delivery_id, &session->pending_metadata, status);
  session->pending_delivery_id = 0u;
  session->buffered -= session->pending_frame_size;
  if (session->buffered > 0u)
    memmove(session->buffer,
            session->buffer + session->pending_frame_size,
            session->buffered);
  session->pending_frame_size = 0u;
  session->pending_metadata =
      (turbo_flow_gateway_metadata_t)TURBO_FLOW_GATEWAY_METADATA_INIT;
  if (session->state == TURBO_FLOW_GATEWAY_SESSION_DRAINING) {
    flow_gateway_runtime_session_release(
        runtime, session, status == TURBO_OK ? TURBO_ESHUTDOWN : status);
    return TURBO_OK;
  }
  if (status != TURBO_OK) {
    flow_gateway_runtime_session_release(runtime, session, status);
    return TURBO_OK;
  }
  session->state = TURBO_FLOW_GATEWAY_SESSION_OPEN;
  return flow_gateway_runtime_pump(runtime, session, result);
}

int turbo_flow_gateway_runtime_session_close(
    turbo_flow_gateway_runtime_t *runtime, uint64_t session_id,
    uint64_t generation, int status) {
  flow_gateway_session_t *session;
  if (!runtime || session_id == 0u || generation == 0u)
    return TURBO_EINVAL;
  session = flow_gateway_runtime_session_find(runtime, session_id);
  if (!session) return TURBO_ENOENT;
  if (session->generation != generation) return TURBO_EPROTO;
  if (session->pending_delivery_id != 0u && runtime->ops.settled)
    runtime->ops.settled(
        runtime->callback_ctx, session->session_id, session->generation,
        session->pending_delivery_id, &session->pending_metadata, status);
  flow_gateway_runtime_session_release(runtime, session, status);
  return TURBO_OK;
}

int turbo_flow_gateway_runtime_begin_shutdown(
    turbo_flow_gateway_runtime_t *runtime) {
  size_t pending = 0u;
  if (!runtime) return TURBO_EINVAL;
  runtime->accepting = 0;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    flow_gateway_session_t *session = &runtime->sessions[i];
    if (!session->in_use) continue;
    if (session->pending_delivery_id != 0u) {
      session->buffered = session->pending_frame_size;
      session->state = TURBO_FLOW_GATEWAY_SESSION_DRAINING;
      pending++;
    } else {
      session->buffered = 0u;
      flow_gateway_runtime_session_release(runtime, session,
                                           TURBO_ESHUTDOWN);
    }
  }
  return pending == 0u ? TURBO_OK : TURBO_EBUSY;
}

int turbo_flow_gateway_runtime_force_shutdown(
    turbo_flow_gateway_runtime_t *runtime, int status) {
  if (!runtime || status == TURBO_OK) return TURBO_EINVAL;
  runtime->accepting = 0;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    flow_gateway_session_t *session = &runtime->sessions[i];
    if (!session->in_use) continue;
    if (session->pending_delivery_id != 0u && runtime->ops.settled)
      runtime->ops.settled(
          runtime->callback_ctx, session->session_id, session->generation,
          session->pending_delivery_id, &session->pending_metadata, status);
    flow_gateway_runtime_session_release(runtime, session, status);
  }
  return TURBO_OK;
}

int turbo_flow_gateway_runtime_snapshot(
    const turbo_flow_gateway_runtime_t *runtime,
    turbo_flow_gateway_runtime_snapshot_t *out) {
  turbo_flow_gateway_runtime_snapshot_t snapshot =
      TURBO_FLOW_GATEWAY_RUNTIME_SNAPSHOT_INIT;
  if (!runtime || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_GATEWAY_RUNTIME_ABI_VERSION)
    return TURBO_EINVAL;
  snapshot.accepting = runtime->accepting ? 1u : 0u;
  for (size_t i = 0u; i < runtime->config.max_sessions; ++i) {
    const flow_gateway_session_t *session = &runtime->sessions[i];
    if (!session->in_use) continue;
    snapshot.active_sessions++;
    snapshot.buffered_bytes += session->buffered;
    if (session->pending_delivery_id != 0u)
      snapshot.pending_settlements++;
  }
  *out = snapshot;
  return TURBO_OK;
}
