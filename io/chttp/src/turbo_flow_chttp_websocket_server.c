#include "turbo_flow_chttp.h"

#include <salts/thread.h>

#define XXH_INLINE_ALL
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

#define CHTTP_WEBSOCKET_EVENT_MAGIC UINT64_C(0x5446434857535631)
enum {
  WEBSOCKET_QUIESCE_CLOSE_CODE = 1013u,
  WEBSOCKET_RESOURCE_GENERATION_INITIAL = 1u
};

static const char WEBSOCKET_RESOURCE_UID_PREFIX[] = "chttp-websocket:";
static const char WEBSOCKET_MEDIA_TYPE[] = "application/octet-stream";
static const char WEBSOCKET_COMMAND_SCHEMA[] = "CHTTPWebSocketCommand";
static const char WEBSOCKET_EVENT_SCHEMA[] = "CHTTPWebSocketEvent";
static const char WEBSOCKET_FRAME_TYPE[] = "Frame";

typedef struct websocket_session_slot_s {
  chttp_server_websocket_session session;
  uint64_t generation;
  size_t in_flight_frames;
  bool active;
  bool closing;
  bool peer_closed;
  bool close_command_submitted;
  bool close_on_drain;
  bool close_retry_required;
} websocket_session_slot_t;

typedef struct websocket_frame_slot_s {
  struct turbo_flow_chttp_websocket_server_s *owner;
  size_t index;
  size_t session_index;
  uint64_t generation;
  uint64_t session_generation;
  turbo_flow_chttp_websocket_frame_type_t event_type;
  bool occupied;
} websocket_frame_slot_t;

typedef struct websocket_event_storage_s {
  turbo_flow_chttp_websocket_event_context_t public_context;
  uint64_t magic;
  struct turbo_flow_chttp_websocket_server_s *owner;
  size_t session_index;
  uint64_t session_generation;
} websocket_event_storage_t;

struct turbo_flow_chttp_websocket_server_s {
  turbo_flow_t *flow;
  tstr adapter_name;
  tstr source_name;
  tstr route_path;
  tstr subprotocol;
  tstr host;
  tstr session_cookie_name;
  char managed_owner[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  char managed_uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
  chttp_server_config config;
  chttp_server_socket_options socket_options;
  bool has_socket_options;
  size_t session_capacity;
  size_t frame_capacity;
  size_t max_frame_bytes;
  size_t max_message_bytes;
  size_t max_buffered_input_bytes;
  uint32_t stop_timeout_ms;
  chttp_server http;
  bool http_initialized;
  websocket_session_slot_t *sessions;
  websocket_frame_slot_t *frames;
  salts_mutex_t mutex;
  turbo_flow_chttp_websocket_server_state_t state;
  uint16_t bound_port;
  uint64_t next_message_id;
  size_t active_sessions;
  size_t in_flight_frames;
  uint64_t sessions_opened;
  uint64_t sessions_rejected;
  uint64_t sessions_closed;
  uint64_t frames_admitted;
  uint64_t frames_rejected;
  uint64_t frames_completed;
  uint64_t bytes_admitted;
  uint64_t commands_admitted;
  uint64_t bytes_sent;
  uint64_t managed_generation;
  uint64_t managed_accepted;
  uint64_t managed_completed;
  uint64_t managed_rejected;
  size_t pending_publications;
  int last_status;
};

static void websocket_counter_increment(uint64_t *counter) {
  if (counter && *counter != UINT64_MAX) ++*counter;
}

static int websocket_managed_identity_init(turbo_flow_chttp_websocket_server_t *server,
                                           const char *adapter_name) {
  size_t length;
  int written;
  if (!server || !adapter_name) return SALTS_EINVAL;
  length = strlen(adapter_name);
  if (length <= TURBO_FLOW_RESOURCE_OWNER_MAX) {
    memcpy(server->managed_owner, adapter_name, length + 1u);
  } else {
    const XXH128_hash_t hash = XXH3_128bits(adapter_name, length);
    written = snprintf(server->managed_owner, sizeof(server->managed_owner),
                       "xxh3-128:%016" PRIx64 "%016" PRIx64, hash.high64, hash.low64);
    if (written < 0 || (size_t)written >= sizeof(server->managed_owner)) return SALTS_ERANGE;
  }
  written = snprintf(server->managed_uid, sizeof(server->managed_uid), "%s%s",
                     WEBSOCKET_RESOURCE_UID_PREFIX, server->managed_owner);
  if (written < 0 || (size_t)written >= sizeof(server->managed_uid)) return SALTS_ERANGE;
  return SALTS_OK;
}

static bool websocket_session_equal(chttp_server_websocket_session left,
                                    chttp_server_websocket_session right) {
  return left.impl == right.impl && left.connection_slot == right.connection_slot &&
         left.connection_generation == right.connection_generation &&
         left.stream_id == right.stream_id;
}

static turbo_flow_chttp_websocket_frame_type_t
websocket_event_type(const chttp_websocket_event *event) {
  if (!event) return 0;
  if (event->kind == CHTTP_WEBSOCKET_EVENT_PING) return TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_PING;
  if (event->kind == CHTTP_WEBSOCKET_EVENT_PONG) return TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_PONG;
  if (event->kind == CHTTP_WEBSOCKET_EVENT_CLOSE) return TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_CLOSE;
  if (event->kind != CHTTP_WEBSOCKET_EVENT_MESSAGE) return 0;
  return event->message_type == CHTTP_WEBSOCKET_MESSAGE_TEXT ? TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_TEXT
         : event->message_type == CHTTP_WEBSOCKET_MESSAGE_BINARY
             ? TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_BINARY
             : 0;
}

static const websocket_event_storage_t *websocket_event_storage(const turbo_flow_msg_t *message) {
  const websocket_event_storage_t *storage;
  if (!message || !message->buffer || !message->transport_context ||
      message->transport_context != mem_buffer_const_data(message->buffer) ||
      mem_buffer_used(message->buffer) < sizeof(*storage)) {
    return NULL;
  }
  storage = (const websocket_event_storage_t *)message->transport_context;
  if (storage->magic != CHTTP_WEBSOCKET_EVENT_MAGIC ||
      storage->public_context.size < sizeof(storage->public_context) ||
      storage->public_context.version != TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_API_VERSION) {
    return NULL;
  }
  return storage;
}

const turbo_flow_chttp_websocket_event_context_t *
turbo_flow_chttp_websocket_event_context(const turbo_flow_msg_t *message) {
  const websocket_event_storage_t *storage = websocket_event_storage(message);
  return storage ? &storage->public_context : NULL;
}

static websocket_session_slot_t *
websocket_find_session_locked(turbo_flow_chttp_websocket_server_t *server,
                              chttp_server_websocket_session session, size_t *out_index) {
  size_t index;
  if (out_index) *out_index = SIZE_MAX;
  for (index = 0u; index < server->session_capacity; ++index) {
    websocket_session_slot_t *slot = &server->sessions[index];
    if (slot->active && websocket_session_equal(slot->session, session)) {
      if (out_index) *out_index = index;
      return slot;
    }
  }
  return NULL;
}

static void websocket_retire_session_locked(turbo_flow_chttp_websocket_server_t *server,
                                            websocket_session_slot_t *slot) {
  if (!slot || !slot->active || !slot->peer_closed || slot->in_flight_frames != 0u) return;
  slot->active = false;
  slot->close_command_submitted = false;
  slot->session = (chttp_server_websocket_session){0};
  if (server->active_sessions != 0u) --server->active_sessions;
  websocket_counter_increment(&server->sessions_closed);
}

static int websocket_open(void *user, chttp_websocket *websocket,
                          const chttp_server_request_view *request,
                          chttp_server_response *response) {
  static const char overload[] = "flow websocket capacity exhausted";
  turbo_flow_chttp_websocket_server_t *server = (turbo_flow_chttp_websocket_server_t *)user;
  chttp_server_websocket_session captured = {0};
  websocket_session_slot_t *selected = NULL;
  size_t index;
  int status;
  if (!server || !websocket || !request || !response) return SALTS_EINVAL;
  if (server->subprotocol) {
    status =
        chttp_server_response_select_websocket_subprotocol(response, request, server->subprotocol);
    if (status != SALTS_OK) return status;
  }
  status = chttp_server_websocket_session_capture(websocket, &captured);
  if (status != SALTS_OK) return status;
  salts_mutex_lock(&server->mutex);
  if (server->state == TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_RUNNING) {
    for (index = 0u; index < server->session_capacity; ++index) {
      websocket_session_slot_t *slot = &server->sessions[index];
      if (slot->active || slot->generation == UINT64_MAX) continue;
      ++slot->generation;
      slot->session = captured;
      slot->active = true;
      slot->closing = false;
      slot->peer_closed = false;
      slot->close_command_submitted = false;
      slot->close_on_drain = false;
      slot->close_retry_required = false;
      slot->in_flight_frames = 0u;
      selected = slot;
      ++server->active_sessions;
      websocket_counter_increment(&server->sessions_opened);
      server->last_status = SALTS_OK;
      break;
    }
  } else {
    server->last_status = SALTS_ESHUTDOWN;
  }
  if (!selected) websocket_counter_increment(&server->sessions_rejected);
  salts_mutex_unlock(&server->mutex);
  if (selected) return SALTS_OK;
  return chttp_server_reply(response, 503u, "text/plain", overload, sizeof(overload) - 1u);
}

static int websocket_reserve_frame(turbo_flow_chttp_websocket_server_t *server,
                                   chttp_server_websocket_session session,
                                   turbo_flow_chttp_websocket_frame_type_t event_type,
                                   websocket_frame_slot_t **out_frame,
                                   websocket_session_slot_t **out_session,
                                   uint64_t *out_message_id) {
  websocket_session_slot_t *session_slot;
  websocket_frame_slot_t *frame = NULL;
  size_t session_index;
  size_t index;
  int status = SALTS_ENOSPC;
  *out_frame = NULL;
  *out_session = NULL;
  *out_message_id = 0u;
  salts_mutex_lock(&server->mutex);
  session_slot = websocket_find_session_locked(server, session, &session_index);
  if (!session_slot) {
    status = SALTS_ENOENT;
  } else if (server->state != TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_RUNNING ||
             session_slot->close_on_drain) {
    status = SALTS_ESHUTDOWN;
  } else if (server->next_message_id == 0u) {
    status = SALTS_ERANGE;
  } else {
    for (index = 0u; index < server->frame_capacity; ++index) {
      websocket_frame_slot_t *candidate = &server->frames[index];
      if (candidate->occupied || candidate->generation == UINT64_MAX) continue;
      ++candidate->generation;
      candidate->session_index = session_index;
      candidate->session_generation = session_slot->generation;
      candidate->event_type = event_type;
      candidate->occupied = true;
      frame = candidate;
      ++session_slot->in_flight_frames;
      ++server->in_flight_frames;
      ++server->pending_publications;
      *out_message_id = server->next_message_id;
      server->next_message_id =
          server->next_message_id == UINT64_MAX ? 0u : server->next_message_id + 1u;
      status = SALTS_OK;
      break;
    }
  }
  if (status != SALTS_OK) {
    websocket_counter_increment(&server->frames_rejected);
    websocket_counter_increment(&server->managed_rejected);
    server->last_status = status;
  }
  if (event_type == TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_CLOSE && session_slot) {
    session_slot->closing = true;
    session_slot->peer_closed = true;
    if (status != SALTS_OK) websocket_retire_session_locked(server, session_slot);
  }
  salts_mutex_unlock(&server->mutex);
  *out_frame = frame;
  *out_session = session_slot;
  return status;
}

/* Claim close under the owner lock; invoke CHTTP outside it. Failed admission
 * leaves the same generation pending for an explicit quiesce retry. */
static int websocket_close_drained(turbo_flow_chttp_websocket_server_t *server, size_t index,
                                   bool explicit_retry) {
  chttp_server_websocket_session captured = {0};
  uint64_t generation;
  websocket_session_slot_t *slot;
  int status;
  salts_mutex_lock(&server->mutex);
  slot = &server->sessions[index];
  if (!slot->active || !slot->close_on_drain || slot->in_flight_frames != 0u ||
      slot->close_command_submitted || slot->peer_closed ||
      (slot->close_retry_required && !explicit_retry)) {
    salts_mutex_unlock(&server->mutex);
    return SALTS_OK;
  }
  captured = slot->session;
  generation = slot->generation;
  slot->close_command_submitted = true;
  salts_mutex_unlock(&server->mutex);
  status = chttp_server_websocket_close(&captured, WEBSOCKET_QUIESCE_CLOSE_CODE, NULL, 0u);
  if (status != SALTS_OK) {
    salts_mutex_lock(&server->mutex);
    if (slot->active && slot->generation == generation) {
      slot->close_command_submitted = false;
      slot->close_retry_required = true;
    }
    server->last_status = status;
    salts_mutex_unlock(&server->mutex);
  }
  return status;
}

static void websocket_release_frame(turbo_flow_chttp_websocket_server_t *server,
                                    websocket_frame_slot_t *frame, int completion_status,
                                    bool publication_terminal, bool publication_rejected,
                                    chttp_server_websocket_session *close_session) {
  websocket_session_slot_t *session = NULL;
  if (close_session) *close_session = (chttp_server_websocket_session){0};
  salts_mutex_lock(&server->mutex);
  if (frame->occupied && frame->session_index < server->session_capacity) {
    session = &server->sessions[frame->session_index];
    if (session->active && session->generation == frame->session_generation) {
      if (session->in_flight_frames != 0u) --session->in_flight_frames;
      if (completion_status != SALTS_OK) {
        session->closing = true;
        if (close_session) *close_session = session->session;
      }
      websocket_retire_session_locked(server, session);
    }
    frame->occupied = false;
    if (server->in_flight_frames != 0u) --server->in_flight_frames;
    if (completion_status == SALTS_OK) websocket_counter_increment(&server->frames_completed);
    else {
      websocket_counter_increment(&server->frames_rejected);
      server->last_status = completion_status;
    }
    if (publication_terminal) websocket_counter_increment(&server->managed_completed);
  }
  if (publication_rejected) {
    websocket_counter_increment(&server->managed_rejected);
    if (server->pending_publications != 0u) --server->pending_publications;
  }
  salts_mutex_unlock(&server->mutex);
}

static int websocket_make_message(turbo_flow_chttp_websocket_server_t *server,
                                  websocket_frame_slot_t *frame, websocket_session_slot_t *session,
                                  const chttp_websocket_event *event, uint64_t message_id,
                                  turbo_flow_msg_t *message) {
  websocket_event_storage_t *storage;
  mem_buffer_t *buffer;
  size_t total;
  if (event->size > SIZE_MAX - sizeof(*storage)) return SALTS_ERANGE;
  total = sizeof(*storage) + event->size;
  buffer = mem_get_buffer(mem_global(), total);
  if (!buffer) return SALTS_ENOMEM;
  storage = (websocket_event_storage_t *)mem_buffer_data(buffer);
  memset(storage, 0, sizeof(*storage));
  storage->public_context.size = sizeof(storage->public_context);
  storage->public_context.version = TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_API_VERSION;
  storage->public_context.session = session->session;
  storage->public_context.event_type = frame->event_type;
  storage->public_context.message_type = event->message_type;
  storage->public_context.close_code = event->close_code;
  storage->magic = CHTTP_WEBSOCKET_EVENT_MAGIC;
  storage->owner = server;
  storage->session_index = frame->session_index;
  storage->session_generation = frame->session_generation;
  if (event->size != 0u)
    memcpy((unsigned char *)storage + sizeof(*storage), event->data, event->size);
  mem_set_used(buffer, total);
  turbo_flow_msg_init(message);
  message->id = message_id;
  message->type = (uint32_t)frame->event_type;
  message->status = frame->event_type == TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_CLOSE
                        ? (int)event->close_code
                        : SALTS_OK;
  message->buffer = buffer;
  message->payload = vstr_from_buf((const char *)storage + sizeof(*storage), event->size);
  message->transport_context = storage;
  return SALTS_OK;
}

static void websocket_publication_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  websocket_frame_slot_t *frame = (websocket_frame_slot_t *)ctx;
  turbo_flow_chttp_websocket_server_t *server = frame ? frame->owner : NULL;
  chttp_server_websocket_session close_session = {0};
  int status = result ? result->status : SALTS_EINVAL;
  if (!server) return;
  const size_t session_index = frame->session_index;
  websocket_release_frame(server, frame, status, true, false, &close_session);
  if (close_session.impl) (void)chttp_server_websocket_close(&close_session, 1011u, NULL, 0u);
  else (void)websocket_close_drained(server, session_index, false);
}

static void websocket_event(void *user, chttp_websocket *websocket,
                            const chttp_websocket_event *event) {
  turbo_flow_chttp_websocket_server_t *server = (turbo_flow_chttp_websocket_server_t *)user;
  chttp_server_websocket_session captured = {0};
  websocket_session_slot_t *session = NULL;
  websocket_frame_slot_t *frame = NULL;
  turbo_flow_chttp_websocket_frame_type_t event_type;
  turbo_flow_msg_t message;
  uint64_t message_id = 0u;
  bool reservation_attempted = false;
  int status;
  if (!server || !websocket || !event) return;
  event_type = websocket_event_type(event);
  status = event_type ? chttp_server_websocket_session_capture(websocket, &captured) : SALTS_EPROTO;
  if (status == SALTS_OK && event->size > server->max_message_bytes) status = SALTS_EMSGSIZE;
  if (status == SALTS_OK) {
    reservation_attempted = true;
    status = websocket_reserve_frame(server, captured, event_type, &frame, &session, &message_id);
  }
  if (status == SALTS_OK) {
    turbo_flow_msg_init(&message);
    status = websocket_make_message(server, frame, session, event, message_id, &message);
    if (status == SALTS_OK)
      status = turbo_flow_publish_async(server->flow, server->source_name, &message,
                                        websocket_publication_complete, frame);
    turbo_flow_msg_cleanup(&message);
    if (status == SALTS_OK) {
      salts_mutex_lock(&server->mutex);
      websocket_counter_increment(&server->managed_accepted);
      if (server->pending_publications != 0u) --server->pending_publications;
      websocket_counter_increment(&server->frames_admitted);
      if (server->bytes_admitted <= UINT64_MAX - (uint64_t)event->size)
        server->bytes_admitted += (uint64_t)event->size;
      else server->last_status = SALTS_ERANGE;
      salts_mutex_unlock(&server->mutex);
    } else {
      websocket_release_frame(server, frame, status, false, true, NULL);
    }
  }
  if (status != SALTS_OK) {
    bool draining = false;
    salts_mutex_lock(&server->mutex);
    if (!reservation_attempted) websocket_counter_increment(&server->managed_rejected);
    server->last_status = status;
    session = websocket_find_session_locked(server, captured, NULL);
    draining = status == SALTS_ESHUTDOWN && session && session->close_on_drain;
    salts_mutex_unlock(&server->mutex);
    if (draining) {
      /* Rejected input must not retry a failed close admission. */
      return;
    }
    (void)chttp_websocket_close(websocket, status == SALTS_ENOSPC ? 1013u : 1011u, NULL, 0u);
  }
}

static int websocket_send_command(const turbo_flow_msg_t *message,
                                  chttp_server_websocket_session session) {
  switch ((turbo_flow_chttp_websocket_frame_type_t)message->type) {
  case TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_TEXT:
    return chttp_server_websocket_send_text(&session, message->payload.data, message->payload.len);
  case TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_BINARY:
    return chttp_server_websocket_send_binary(&session, message->payload.data,
                                              message->payload.len);
  case TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_PING:
    return chttp_server_websocket_send_ping(&session, message->payload.data, message->payload.len);
  case TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_PONG:
    return chttp_server_websocket_send_pong(&session, message->payload.data, message->payload.len);
  case TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_CLOSE:
    return chttp_server_websocket_close(&session, (uint16_t)message->status, message->payload.data,
                                        message->payload.len);
  default:
    return SALTS_EINVAL;
  }
}

static int websocket_terminal_submit(void *ctx, turbo_flow_t *flow,
                                     const turbo_flow_stage_plan_t *stage,
                                     const turbo_flow_msg_t *message,
                                     turbo_flow_async_terminal_claim_t *claim) {
  turbo_flow_chttp_websocket_server_t *server = (turbo_flow_chttp_websocket_server_t *)ctx;
  const websocket_event_storage_t *storage = websocket_event_storage(message);
  turbo_flow_async_terminal_claim_t owned_claim = TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  websocket_session_slot_t *session;
  chttp_server_websocket_session captured = {0};
  bool close_reserved = false;
  int status;
  (void)stage;
  if (!server || flow != server->flow || !storage || storage->owner != server || !claim ||
      storage->session_index >= server->session_capacity ||
      message->type < TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_TEXT ||
      message->type > TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_CLOSE ||
      message->payload.len > server->max_frame_bytes ||
      ((message->type == TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_PING ||
        message->type == TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_PONG) &&
       message->payload.len > 125u) ||
      (message->type == TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_CLOSE &&
       (message->payload.len > 123u || message->status < 0 || message->status > UINT16_MAX))) {
    return SALTS_EINVAL;
  }
  salts_mutex_lock(&server->mutex);
  session = &server->sessions[storage->session_index];
  if (!session->active || session->generation != storage->session_generation ||
      !websocket_session_equal(session->session, storage->public_context.session)) {
    status = SALTS_ENOENT;
  } else if (message->type == TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_CLOSE &&
             session->close_command_submitted) {
    status = SALTS_EALREADY;
  } else {
    captured = session->session;
    status = SALTS_OK;
    if (message->type == TURBO_FLOW_CHTTP_WEBSOCKET_FRAME_CLOSE) {
      session->close_command_submitted = true;
      session->closing = true;
      close_reserved = true;
    }
  }
  salts_mutex_unlock(&server->mutex);
  if (status != SALTS_OK) return status;
  status = turbo_flow_async_terminal_claim_move(&owned_claim, claim);
  if (status != SALTS_OK) return status;
  status = websocket_send_command(message, captured);
  salts_mutex_lock(&server->mutex);
  session = &server->sessions[storage->session_index];
  if (status == SALTS_OK) {
    websocket_counter_increment(&server->commands_admitted);
    if (server->bytes_sent <= UINT64_MAX - (uint64_t)message->payload.len)
      server->bytes_sent += (uint64_t)message->payload.len;
    else server->last_status = SALTS_ERANGE;
  } else {
    server->last_status = status;
    if (close_reserved && session->active && session->generation == storage->session_generation) {
      session->close_command_submitted = false;
    }
  }
  salts_mutex_unlock(&server->mutex);
  return turbo_flow_async_terminal_complete(&owned_claim, status, NULL);
}

static turbo_flow_managed_boundary_state_t
websocket_managed_state(turbo_flow_chttp_websocket_server_state_t state,
                        size_t active_sessions, size_t in_flight_frames) {
  switch (state) {
  case TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_REGISTERED:
    return TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED;
  case TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STARTING:
    return TURBO_FLOW_MANAGED_BOUNDARY_STARTING;
  case TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_RUNNING:
    return TURBO_FLOW_MANAGED_BOUNDARY_RUNNING;
  case TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_QUIESCED:
    return active_sessions != 0u || in_flight_frames != 0u
               ? TURBO_FLOW_MANAGED_BOUNDARY_DRAINING
               : TURBO_FLOW_MANAGED_BOUNDARY_QUIESCENT;
  case TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STOPPING:
    return TURBO_FLOW_MANAGED_BOUNDARY_STOPPING;
  case TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STOPPED:
  case TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_DETACHED:
    return TURBO_FLOW_MANAGED_BOUNDARY_STOPPED;
  case TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_FAILED:
  default:
    return TURBO_FLOW_MANAGED_BOUNDARY_FAILED;
  }
}

static int websocket_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  turbo_flow_chttp_websocket_server_t *server = (turbo_flow_chttp_websocket_server_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  if (!server || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(metadata.uid, server->managed_uid, strlen(server->managed_uid) + 1u);
  memcpy(metadata.owner_name, server->managed_owner, strlen(server->managed_owner) + 1u);
  metadata.generation = server->managed_generation;
  metadata.observed_generation = server->managed_generation;
  salts_mutex_unlock(&server->mutex);
  *out = metadata;
  return SALTS_OK;
}

static int websocket_managed_descriptor(void *ctx,
                                        turbo_flow_managed_boundary_descriptor_t *out) {
  turbo_flow_chttp_websocket_server_t *server = (turbo_flow_chttp_websocket_server_t *)ctx;
  turbo_flow_managed_boundary_descriptor_t descriptor =
      TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  int status;
  if (!server || !out || out->size < sizeof(*out) ||
      out->version != TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION) {
    return SALTS_EINVAL;
  }
  salts_mutex_lock(&server->mutex);
  descriptor.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  descriptor.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  memcpy(descriptor.uid, server->managed_uid, strlen(server->managed_uid) + 1u);
  memcpy(descriptor.owner_name, server->managed_owner, strlen(server->managed_owner) + 1u);
  salts_mutex_unlock(&server->mutex);
  descriptor.role_flags =
      TURBO_FLOW_MANAGED_BOUNDARY_SOURCE | TURBO_FLOW_MANAGED_BOUNDARY_SINK;
  descriptor.capability_flags = 0u;
  descriptor.command_flags = TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_QUIESCE |
                             TURBO_FLOW_MANAGED_BOUNDARY_COMMAND_RESUME;
  status = turbo_flow_content_descriptor_init(
      &descriptor.input, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
      TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
      WEBSOCKET_MEDIA_TYPE, descriptor.owner_name);
  if (status != SALTS_OK) return status;
  status = turbo_flow_content_descriptor_declare_schema(
      &descriptor.input, WEBSOCKET_COMMAND_SCHEMA, WEBSOCKET_FRAME_TYPE, 1u);
  if (status != SALTS_OK) return status;
  status = turbo_flow_content_descriptor_init(
      &descriptor.output, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
      TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
      WEBSOCKET_MEDIA_TYPE, descriptor.owner_name);
  if (status != SALTS_OK) return status;
  status = turbo_flow_content_descriptor_declare_schema(
      &descriptor.output, WEBSOCKET_EVENT_SCHEMA, WEBSOCKET_FRAME_TYPE, 1u);
  if (status != SALTS_OK) return status;
  *out = descriptor;
  return SALTS_OK;
}

static int websocket_managed_snapshot(void *ctx,
                                      turbo_flow_managed_boundary_snapshot_t *out) {
  turbo_flow_chttp_websocket_server_t *server = (turbo_flow_chttp_websocket_server_t *)ctx;
  turbo_flow_managed_boundary_snapshot_t snapshot = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  size_t occupied_frames = 0u;
  size_t session_frames = 0u;
  size_t active_sessions = 0u;
  size_t index;
  if (!server || !out || out->size < sizeof(*out) ||
      out->version != TURBO_FLOW_MANAGED_BOUNDARY_API_VERSION) {
    return SALTS_EINVAL;
  }
  salts_mutex_lock(&server->mutex);
  if (server->pending_publications != 0u) {
    const int status = server->pending_publications > server->frame_capacity ? SALTS_EPROTO
                                                                             : SALTS_EBUSY;
    salts_mutex_unlock(&server->mutex);
    return status;
  }
  for (index = 0u; index < server->frame_capacity; ++index)
    if (server->frames[index].occupied) ++occupied_frames;
  for (index = 0u; index < server->session_capacity; ++index) {
    const websocket_session_slot_t *slot = &server->sessions[index];
    if (slot->active) {
      ++active_sessions;
      if (slot->in_flight_frames > SIZE_MAX - session_frames) {
        salts_mutex_unlock(&server->mutex);
        return SALTS_EPROTO;
      }
      session_frames += slot->in_flight_frames;
    } else if (slot->in_flight_frames != 0u) {
      salts_mutex_unlock(&server->mutex);
      return SALTS_EPROTO;
    }
  }
  if (occupied_frames != server->in_flight_frames || session_frames != occupied_frames ||
      active_sessions != server->active_sessions ||
      server->managed_completed > server->managed_accepted) {
    salts_mutex_unlock(&server->mutex);
    return SALTS_EPROTO;
  }
  memcpy(snapshot.uid, server->managed_uid, strlen(server->managed_uid) + 1u);
  snapshot.generation = server->managed_generation;
  snapshot.observed_generation = server->managed_generation;
  snapshot.state = websocket_managed_state(server->state, active_sessions, occupied_frames);
  snapshot.queue_depth = 0u;
  snapshot.queue_capacity = (uint64_t)server->frame_capacity;
  snapshot.in_flight = (uint64_t)occupied_frames;
  snapshot.accepted = server->managed_accepted;
  snapshot.completed = server->managed_completed;
  snapshot.rejected = server->managed_rejected;
  snapshot.backpressured = occupied_frames == server->frame_capacity;
  snapshot.last_status = server->last_status;
  salts_mutex_unlock(&server->mutex);
  *out = snapshot;
  return SALTS_OK;
}

static int websocket_set_admission(turbo_flow_chttp_websocket_server_t *server,
                                   turbo_flow_chttp_websocket_server_state_t desired,
                                   bool check_generation, uint64_t expected_generation) {
  int status = SALTS_OK;
  size_t index;
  if (!server) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  if (check_generation && expected_generation != server->managed_generation) {
    status = SALTS_EBUSY;
  } else if (server->state != TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_RUNNING &&
             server->state != TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_QUIESCED) {
    status = SALTS_ESHUTDOWN;
  } else if (server->state != desired && server->managed_generation == UINT64_MAX) {
    status = SALTS_ERANGE;
  } else {
    if (server->state != desired) {
      server->state = desired;
      ++server->managed_generation;
    }
    if (desired == TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_QUIESCED) {
      for (index = 0u; index < server->session_capacity; ++index) {
        if (server->sessions[index].active) {
          server->sessions[index].closing = true;
          server->sessions[index].close_on_drain = true;
        }
      }
    }
  }
  salts_mutex_unlock(&server->mutex);
  if (status != SALTS_OK || desired != TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_QUIESCED) return status;
  for (index = 0u; index < server->session_capacity; ++index) {
    status = websocket_close_drained(server, index, true);
    if (status != SALTS_OK) return status;
  }
  return SALTS_OK;
}

static int websocket_resource_command(void *ctx, turbo_flow_t *flow,
                                      const turbo_flow_resource_command_t *command) {
  turbo_flow_chttp_websocket_server_t *server = (turbo_flow_chttp_websocket_server_t *)ctx;
  turbo_flow_chttp_websocket_server_state_t desired;
  if (!server || !flow || flow != server->flow || !command ||
      command->size < sizeof(*command)) {
    return SALTS_EINVAL;
  }
  if (command->kind == TURBO_FLOW_RESOURCE_COMMAND_QUIESCE)
    desired = TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_QUIESCED;
  else if (command->kind == TURBO_FLOW_RESOURCE_COMMAND_RESUME)
    desired = TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_RUNNING;
  else
    return SALTS_ENOTSUP;
  return websocket_set_admission(server, desired, true, command->expected_generation);
}

static int websocket_start_native(turbo_flow_chttp_websocket_server_t *server, uint16_t *out_port) {
  chttp_server_websocket_options options = {.size = sizeof(options),
                                            .path = server->route_path,
                                            .max_frame_bytes = server->max_frame_bytes,
                                            .max_message_bytes = server->max_message_bytes,
                                            .max_buffered_input_bytes =
                                                server->max_buffered_input_bytes,
                                            .on_open = websocket_open,
                                            .on_event = websocket_event,
                                            .user = server};
  int status = chttp_server_init(&server->http, &server->config);
  *out_port = 0u;
  if (status != SALTS_OK) return status;
  if (server->has_socket_options) {
    status = chttp_server_set_socket_options(&server->http, &server->socket_options);
    if (status != SALTS_OK) goto fail;
  }
  status = chttp_server_websocket_with(&server->http, &options);
  if (status != SALTS_OK) goto fail;
  status = chttp_server_start(&server->http);
  if (status != SALTS_OK) goto fail;
  status = chttp_server_port(&server->http, out_port);
  if (status == SALTS_OK) return SALTS_OK;
  {
    const int cleanup_status = chttp_server_stop(&server->http, server->stop_timeout_ms);
    if (cleanup_status != SALTS_OK) return cleanup_status;
  }
fail: {
  const int cleanup_status = chttp_server_destroy(&server->http);
  if (cleanup_status != SALTS_OK) return cleanup_status;
}
  return status;
}

static int websocket_adapter_start(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage) {
  turbo_flow_chttp_websocket_server_t *server = (turbo_flow_chttp_websocket_server_t *)ctx;
  uint16_t port = 0u;
  int status;
  if (!server || flow != server->flow || !stage) return SALTS_EINVAL;
  if (!stage->is_source) return SALTS_OK;
  if (strcmp(stage->name, server->source_name) != 0) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  if (server->state != TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_REGISTERED &&
      server->state != TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STOPPED) {
    salts_mutex_unlock(&server->mutex);
    return SALTS_EALREADY;
  }
  server->state = TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STARTING;
  server->last_status = SALTS_OK;
  salts_mutex_unlock(&server->mutex);
  status = websocket_start_native(server, &port);
  salts_mutex_lock(&server->mutex);
  /* Native ownership survives a failed cleanup, including a failed start. */
  server->http_initialized = server->http.impl != NULL;
  server->bound_port = status == SALTS_OK ? port : 0u;
  server->state = status == SALTS_OK         ? TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_RUNNING
                  : server->http_initialized ? TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_FAILED
                                             : TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STOPPED;
  server->last_status = status;
  salts_mutex_unlock(&server->mutex);
  return status;
}

int turbo_flow_chttp_websocket_server_quiesce(turbo_flow_chttp_websocket_server_t *server) {
  return websocket_set_admission(server, TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_QUIESCED, false, 0u);
}

int turbo_flow_chttp_websocket_server_resume(turbo_flow_chttp_websocket_server_t *server) {
  return websocket_set_admission(server, TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_RUNNING, false, 0u);
}

static int websocket_stop_native(turbo_flow_chttp_websocket_server_t *server, uint32_t timeout_ms) {
  int status = chttp_server_stop(&server->http, timeout_ms);
  if (status != SALTS_OK) return status;
  return chttp_server_destroy(&server->http);
}

static void websocket_clear_sessions_locked(turbo_flow_chttp_websocket_server_t *server) {
  size_t index;
  for (index = 0u; index < server->session_capacity; ++index) {
    websocket_session_slot_t *slot = &server->sessions[index];
    if (slot->active) websocket_counter_increment(&server->sessions_closed);
    slot->session = (chttp_server_websocket_session){0};
    slot->in_flight_frames = 0u;
    slot->active = false;
    slot->closing = false;
    slot->peer_closed = false;
    slot->close_command_submitted = false;
    slot->close_on_drain = false;
    slot->close_retry_required = false;
  }
  for (index = 0u; index < server->frame_capacity; ++index)
    server->frames[index].occupied = false;
  server->active_sessions = 0u;
  server->in_flight_frames = 0u;
}

static void websocket_adapter_stop(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage) {
  turbo_flow_chttp_websocket_server_t *server = (turbo_flow_chttp_websocket_server_t *)ctx;
  int status;
  if (!server || !stage || !stage->is_source) return;
  salts_mutex_lock(&server->mutex);
  if (!server->http_initialized) {
    if (server->state != TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_DETACHED)
      server->state = TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STOPPED;
    salts_mutex_unlock(&server->mutex);
    return;
  }
  server->state = TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STOPPING;
  salts_mutex_unlock(&server->mutex);
  status = websocket_stop_native(server, server->stop_timeout_ms);
  if (status != SALTS_OK) {
    const int report_status = turbo_flow_adapter_report_stop_status(flow, status);
    if (report_status != SALTS_OK) status = report_status;
  }
  salts_mutex_lock(&server->mutex);
  if (status == SALTS_OK) {
    server->http_initialized = false;
    server->bound_port = 0u;
    websocket_clear_sessions_locked(server);
  }
  server->state = status == SALTS_OK ? TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STOPPED
                                     : TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_FAILED;
  server->last_status = status;
  salts_mutex_unlock(&server->mutex);
}

static void websocket_adapter_shutdown(void *ctx) {
  turbo_flow_chttp_websocket_server_t *server = (turbo_flow_chttp_websocket_server_t *)ctx;
  bool initialized;
  int status;
  if (!server) return;
  salts_mutex_lock(&server->mutex);
  initialized = server->http_initialized;
  if (initialized) server->state = TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_STOPPING;
  salts_mutex_unlock(&server->mutex);
  status = initialized ? websocket_stop_native(server, 0u) : SALTS_OK;
  salts_mutex_lock(&server->mutex);
  server->last_status = status;
  if (status == SALTS_OK) {
    websocket_clear_sessions_locked(server);
    server->http_initialized = false;
    server->bound_port = 0u;
    server->flow = NULL;
    server->state = TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_DETACHED;
  } else {
    server->state = TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_FAILED;
  }
  salts_mutex_unlock(&server->mutex);
}

static int websocket_config_valid(const turbo_flow_chttp_websocket_server_config_t *config) {
  size_t maximum_sessions;
  size_t output_bytes;
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_API_VERSION || !config->flow ||
      !config->adapter_name || config->adapter_name[0] == '\0' || !config->source_name ||
      config->source_name[0] == '\0' || !config->server || !config->server->host || !config->path ||
      config->path[0] != '/' || config->session_capacity == 0u || config->frame_capacity == 0u ||
      config->max_frame_bytes < TURBO_FLOW_CHTTP_WEBSOCKET_MIN_FRAME_BYTES ||
      config->max_message_bytes < config->max_frame_bytes ||
      config->max_frame_bytes > SIZE_MAX - TURBO_FLOW_CHTTP_WEBSOCKET_MAX_WIRE_HEADER_BYTES ||
      config->first_message_id == 0u || config->stop_timeout_ms == 0u ||
      config->server->network.connection_capacity == 0u ||
      config->server->network.command_capacity == 0u ||
      config->server->network.max_send_bytes == 0u ||
      config->session_capacity > SIZE_MAX / sizeof(websocket_session_slot_t) ||
      config->frame_capacity > SIZE_MAX / sizeof(websocket_frame_slot_t) ||
      (config->socket_options && config->socket_options->size != sizeof(*config->socket_options)) ||
      (config->subprotocol && config->subprotocol[0] == '\0')) {
    return 0;
  }
  output_bytes = config->max_frame_bytes + TURBO_FLOW_CHTTP_WEBSOCKET_MAX_WIRE_HEADER_BYTES;
  if (output_bytes > config->server->network.max_send_bytes ||
      config->max_buffered_input_bytes < output_bytes ||
      config->max_buffered_input_bytes > SIZE_MAX - config->max_message_bytes ||
      config->max_buffered_input_bytes + config->max_message_bytes > SIZE_MAX - output_bytes) {
    return 0;
  }
  maximum_sessions = config->server->network.connection_capacity;
  if (config->server->enable_http2) {
    if (config->server->h2_stream_capacity == 0u ||
        maximum_sessions > SIZE_MAX / config->server->h2_stream_capacity)
      return 0;
    maximum_sessions *= config->server->h2_stream_capacity;
  }
  if (config->session_capacity > maximum_sessions) return 0;
  return turbo_flow_state(config->flow) != TURBO_FLOW_STATE_COMPILED &&
         turbo_flow_state(config->flow) != TURBO_FLOW_STATE_STARTED;
}

static void websocket_cleanup(turbo_flow_chttp_websocket_server_t *server) {
  if (!server) return;
  free(server->sessions);
  free(server->frames);
  tstr_free(server->adapter_name);
  tstr_free(server->source_name);
  tstr_free(server->route_path);
  tstr_free(server->subprotocol);
  tstr_free(server->host);
  tstr_free(server->session_cookie_name);
}

int turbo_flow_chttp_websocket_server_register(
    const turbo_flow_chttp_websocket_server_config_t *config,
    turbo_flow_chttp_websocket_server_t **out_server) {
  turbo_flow_chttp_websocket_server_t *server;
  turbo_flow_adapter_ops_t adapter_ops = {0};
  turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
  turbo_flow_managed_boundary_provider_ops_t boundary_ops =
      TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
  turbo_flow_managed_async_terminal_registration_t registration =
      TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_INIT;
  turbo_flow_adapter_schema_t schema = {0};
  size_t index;
  int status;
  if (out_server) *out_server = NULL;
  if (!out_server || !websocket_config_valid(config)) return SALTS_EINVAL;
  server = (turbo_flow_chttp_websocket_server_t *)calloc(1u, sizeof(*server));
  if (!server) return SALTS_ENOMEM;
  salts_mutex_init(&server->mutex);
  server->flow = config->flow;
  server->config = *config->server;
  server->session_capacity = config->session_capacity;
  server->frame_capacity = config->frame_capacity;
  server->max_frame_bytes = config->max_frame_bytes;
  server->max_message_bytes = config->max_message_bytes;
  server->max_buffered_input_bytes = config->max_buffered_input_bytes;
  server->next_message_id = config->first_message_id;
  server->stop_timeout_ms = config->stop_timeout_ms;
  server->state = TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_REGISTERED;
  server->managed_generation = WEBSOCKET_RESOURCE_GENERATION_INITIAL;
  server->last_status = SALTS_OK;
  server->adapter_name = tstr_dup(config->adapter_name);
  server->source_name = tstr_dup(config->source_name);
  server->route_path = tstr_dup(config->path);
  if (config->subprotocol) server->subprotocol = tstr_dup(config->subprotocol);
  server->host = tstr_dup(config->server->host);
  if (config->server->session_cookie_name)
    server->session_cookie_name = tstr_dup(config->server->session_cookie_name);
  server->sessions =
      (websocket_session_slot_t *)calloc(server->session_capacity, sizeof(*server->sessions));
  server->frames =
      (websocket_frame_slot_t *)calloc(server->frame_capacity, sizeof(*server->frames));
  if (!server->adapter_name || !server->source_name || !server->route_path ||
      (config->subprotocol && !server->subprotocol) || !server->host ||
      (config->server->session_cookie_name && !server->session_cookie_name) || !server->sessions ||
      !server->frames) {
    websocket_cleanup(server);
    salts_mutex_destroy(&server->mutex);
    free(server);
    return SALTS_ENOMEM;
  }
  status = websocket_managed_identity_init(server, server->adapter_name);
  if (status != SALTS_OK) {
    websocket_cleanup(server);
    salts_mutex_destroy(&server->mutex);
    free(server);
    return status;
  }
  server->config.host = server->host;
  server->config.session_cookie_name = server->session_cookie_name;
  if (config->socket_options) {
    server->socket_options = *config->socket_options;
    server->has_socket_options = true;
  }
  for (index = 0u; index < server->frame_capacity; ++index) {
    server->frames[index].owner = server;
    server->frames[index].index = index;
  }
  adapter_ops.start = websocket_adapter_start;
  adapter_ops.stop = websocket_adapter_stop;
  adapter_ops.shutdown = websocket_adapter_shutdown;
  async_ops.submit = websocket_terminal_submit;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_HTTP;
  schema.roles = TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
  boundary_ops.resource.metadata = websocket_resource_metadata;
  boundary_ops.resource.command = websocket_resource_command;
  boundary_ops.descriptor = websocket_managed_descriptor;
  boundary_ops.snapshot = websocket_managed_snapshot;
  registration.adapter_name = server->adapter_name;
  registration.adapter_ops = &adapter_ops;
  registration.async_ops = &async_ops;
  registration.schema = &schema;
  registration.owner_name = server->managed_owner;
  registration.boundary_ops = &boundary_ops;
  registration.ctx = server;
  status = turbo_flow_register_managed_async_terminal_adapter(config->flow, &registration);
  if (status != SALTS_OK) {
    websocket_cleanup(server);
    salts_mutex_destroy(&server->mutex);
    free(server);
    return status;
  }
  *out_server = server;
  return SALTS_OK;
}

int turbo_flow_chttp_websocket_server_snapshot(
    const turbo_flow_chttp_websocket_server_t *server,
    turbo_flow_chttp_websocket_server_snapshot_t *out_snapshot) {
  turbo_flow_chttp_websocket_server_snapshot_t current =
      TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
  if (!server || !out_snapshot || out_snapshot->size < sizeof(*out_snapshot) ||
      out_snapshot->version != TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_API_VERSION)
    return SALTS_EINVAL;
  salts_mutex_lock((salts_mutex_t *)&server->mutex);
  current.state = server->state;
  current.bound_port = server->bound_port;
  current.active_sessions = server->active_sessions;
  current.session_capacity = server->session_capacity;
  current.in_flight_frames = server->in_flight_frames;
  current.frame_capacity = server->frame_capacity;
  current.sessions_opened = server->sessions_opened;
  current.sessions_rejected = server->sessions_rejected;
  current.sessions_closed = server->sessions_closed;
  current.frames_admitted = server->frames_admitted;
  current.frames_rejected = server->frames_rejected;
  current.frames_completed = server->frames_completed;
  current.bytes_admitted = server->bytes_admitted;
  current.commands_admitted = server->commands_admitted;
  current.bytes_sent = server->bytes_sent;
  current.last_status = server->last_status;
  salts_mutex_unlock((salts_mutex_t *)&server->mutex);
  *out_snapshot = current;
  return SALTS_OK;
}

int turbo_flow_chttp_websocket_server_destroy(turbo_flow_chttp_websocket_server_t *server) {
  if (!server) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  if (server->state != TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_DETACHED ||
      server->active_sessions != 0u || server->in_flight_frames != 0u || server->http_initialized) {
    salts_mutex_unlock(&server->mutex);
    return SALTS_EBUSY;
  }
  salts_mutex_unlock(&server->mutex);
  websocket_cleanup(server);
  salts_mutex_destroy(&server->mutex);
  free(server);
  return SALTS_OK;
}
