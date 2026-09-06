#include "turbo_flow_chttp.h"

#include <salts/thread.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define CHTTP_SERVER_REQUEST_MAGIC UINT64_C(0x5446434853525631)

typedef struct chttp_server_message_field_s {
  size_t name_offset;
  size_t name_size;
  size_t value_offset;
  size_t value_size;
} chttp_server_message_field_t;

typedef struct chttp_server_message_storage_s {
  turbo_flow_chttp_server_request_context_t public_context;
  uint64_t magic;
  struct turbo_flow_chttp_server_s *owner;
  size_t slot_index;
  uint64_t slot_generation;
  size_t target_offset;
  size_t target_size;
  size_t path_offset;
  size_t path_size;
  size_t headers_offset;
  size_t params_offset;
  size_t peer_certificate_offset;
  size_t peer_certificate_size;
} chttp_server_message_storage_t;

typedef struct turbo_flow_chttp_server_slot_s {
  struct turbo_flow_chttp_server_s *owner;
  size_t index;
  uint64_t generation;
  uint64_t message_id;
  bool occupied;
  bool deferred_attached;
  bool defer_failed;
  bool publication_done;
  bool response_ready;
  bool reply_in_progress;
  int publication_status;
  chttp_server_deferred deferred;
  turbo_flow_msg_t response;
} turbo_flow_chttp_server_slot_t;

typedef struct chttp_server_reply_work_s {
  turbo_flow_chttp_server_slot_t *slot;
  uint64_t generation;
  chttp_server_deferred deferred;
  turbo_flow_msg_t response;
  unsigned int status_code;
  const char *content_type;
  const void *body;
  size_t body_size;
} chttp_server_reply_work_t;

struct turbo_flow_chttp_server_s {
  turbo_flow_t *flow;
  tstr adapter_name;
  tstr source_name;
  tstr route_path;
  tstr host;
  tstr session_cookie_name;
  tstr response_content_type;
  tstr error_content_type;
  tstr graph_error_body;
  chttp_server_config config;
  chttp_server_socket_options socket_options;
  bool has_socket_options;
  chttp_method method;
  size_t max_request_message_bytes;
  unsigned int success_status;
  unsigned int overload_status;
  unsigned int unavailable_status;
  unsigned int graph_error_status;
  uint32_t stop_timeout_ms;
  chttp_server http;
  bool http_initialized;
  turbo_flow_chttp_server_slot_t *slots;
  size_t slot_count;
  salts_mutex_t mutex;
  turbo_flow_chttp_server_state_t state;
  uint16_t bound_port;
  uint64_t next_message_id;
  size_t active_requests;
  uint64_t admitted_requests;
  uint64_t rejected_requests;
  uint64_t completed_requests;
  uint64_t response_bytes;
  int last_status;
};

static int chttp_server_adapter_add_size(size_t *total, size_t value) {
  if (!total || value > SIZE_MAX - *total) return SALTS_ERANGE;
  *total += value;
  return SALTS_OK;
}

static int chttp_server_adapter_status_valid(unsigned int status) {
  return status >= 200u && status <= 599u;
}

static int chttp_server_adapter_status_allows_body(unsigned int status) {
  return status != 204u && status != 205u && status != 304u;
}

static int chttp_server_adapter_content_type_fits(const chttp_server_config *config,
                                                  const char *content_type) {
  static const size_t content_type_wire_overhead = sizeof("Content-Type: \r\n") - 1u;
  const unsigned char *cursor;
  size_t value_size;
  if (!config || !content_type) return 0;
  for (cursor = (const unsigned char *)content_type; *cursor != 0u; ++cursor) {
    if ((*cursor < 32u && *cursor != (unsigned char)'\t') || *cursor == 127u) return 0;
  }
  value_size = strlen(content_type);
  return value_size <= SIZE_MAX - content_type_wire_overhead &&
         value_size + content_type_wire_overhead <= config->max_response_header_bytes;
}

static void chttp_server_adapter_counter_increment(uint64_t *counter) {
  if (counter && *counter != UINT64_MAX) ++*counter;
}

static const chttp_server_message_storage_t *
chttp_server_adapter_storage(const turbo_flow_msg_t *message) {
  const chttp_server_message_storage_t *storage;
  if (!message || !message->buffer || !message->transport_context ||
      message->transport_context != mem_buffer_const_data(message->buffer) ||
      mem_buffer_used(message->buffer) < sizeof(*storage)) {
    return NULL;
  }
  storage = (const chttp_server_message_storage_t *)message->transport_context;
  if (storage->magic != CHTTP_SERVER_REQUEST_MAGIC ||
      storage->public_context.size < sizeof(storage->public_context) ||
      storage->public_context.version != TURBO_FLOW_CHTTP_SERVER_API_VERSION) {
    return NULL;
  }
  return storage;
}

static int chttp_server_adapter_view(const turbo_flow_msg_t *message, size_t offset, size_t size,
                                     vstr *out) {
  size_t used;
  if (!message || !message->buffer || !out) return SALTS_EINVAL;
  used = mem_buffer_used(message->buffer);
  if (offset > used || size > used - offset) return SALTS_EPROTO;
  *out = vstr_from_buf((const char *)mem_buffer_const_data(message->buffer) + offset, size);
  return SALTS_OK;
}

static int chttp_server_adapter_field_at(const turbo_flow_msg_t *message, size_t records_offset,
                                         size_t count, size_t index, vstr *out_name,
                                         vstr *out_value) {
  const chttp_server_message_field_t *records;
  size_t used;
  size_t records_size;
  if (!message || !message->buffer || !out_name || !out_value || index >= count ||
      count > SIZE_MAX / sizeof(*records)) {
    return SALTS_EINVAL;
  }
  used = mem_buffer_used(message->buffer);
  records_size = count * sizeof(*records);
  if (records_offset > used || records_size > used - records_offset) return SALTS_EPROTO;
  records = (const chttp_server_message_field_t *)((const unsigned char *)mem_buffer_const_data(
                                                       message->buffer) +
                                                   records_offset);
  if (chttp_server_adapter_view(message, records[index].name_offset, records[index].name_size,
                                out_name) != SALTS_OK ||
      chttp_server_adapter_view(message, records[index].value_offset, records[index].value_size,
                                out_value) != SALTS_OK) {
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

const turbo_flow_chttp_server_request_context_t *
turbo_flow_chttp_server_request_context(const turbo_flow_msg_t *message) {
  const chttp_server_message_storage_t *storage = chttp_server_adapter_storage(message);
  return storage ? &storage->public_context : NULL;
}

int turbo_flow_chttp_server_request_target(const turbo_flow_msg_t *message, vstr *out_target) {
  const chttp_server_message_storage_t *storage = chttp_server_adapter_storage(message);
  if (!storage || !out_target) return SALTS_EINVAL;
  return chttp_server_adapter_view(message, storage->target_offset, storage->target_size,
                                   out_target);
}

int turbo_flow_chttp_server_request_path(const turbo_flow_msg_t *message, vstr *out_path) {
  const chttp_server_message_storage_t *storage = chttp_server_adapter_storage(message);
  if (!storage || !out_path) return SALTS_EINVAL;
  return chttp_server_adapter_view(message, storage->path_offset, storage->path_size, out_path);
}

int turbo_flow_chttp_server_request_header_at(const turbo_flow_msg_t *message, size_t index,
                                              vstr *out_name, vstr *out_value) {
  const chttp_server_message_storage_t *storage = chttp_server_adapter_storage(message);
  if (!storage) return SALTS_EINVAL;
  return chttp_server_adapter_field_at(message, storage->headers_offset,
                                       storage->public_context.header_count, index, out_name,
                                       out_value);
}

int turbo_flow_chttp_server_request_param_at(const turbo_flow_msg_t *message, size_t index,
                                             vstr *out_name, vstr *out_value) {
  const chttp_server_message_storage_t *storage = chttp_server_adapter_storage(message);
  if (!storage) return SALTS_EINVAL;
  return chttp_server_adapter_field_at(message, storage->params_offset,
                                       storage->public_context.param_count, index, out_name,
                                       out_value);
}

int turbo_flow_chttp_server_request_peer_certificate_sha256(const turbo_flow_msg_t *message,
                                                            vstr *out_digest) {
  const chttp_server_message_storage_t *storage = chttp_server_adapter_storage(message);
  if (!storage || !out_digest) return SALTS_EINVAL;
  return chttp_server_adapter_view(message, storage->peer_certificate_offset,
                                   storage->peer_certificate_size, out_digest);
}

static void chttp_server_adapter_slot_reset(turbo_flow_chttp_server_slot_t *slot,
                                            turbo_flow_msg_t *released_response) {
  if (!slot) return;
  if (released_response) {
    turbo_flow_msg_init(released_response);
    (void)turbo_flow_msg_move(released_response, &slot->response);
  } else {
    turbo_flow_msg_cleanup(&slot->response);
  }
  slot->message_id = 0u;
  slot->occupied = false;
  slot->deferred_attached = false;
  slot->defer_failed = false;
  slot->publication_done = false;
  slot->response_ready = false;
  slot->reply_in_progress = false;
  slot->publication_status = SALTS_OK;
  slot->deferred = (chttp_server_deferred)CHTTP_SERVER_DEFERRED_INIT;
}

static int chttp_server_adapter_reserve_slot(turbo_flow_chttp_server_t *server,
                                             turbo_flow_chttp_server_slot_t **out_slot,
                                             uint64_t *message_id) {
  turbo_flow_chttp_server_slot_t *selected = NULL;
  size_t index;
  int status = SALTS_ENOSPC;
  if (!server || !out_slot || !message_id) return SALTS_EINVAL;
  *out_slot = NULL;
  salts_mutex_lock(&server->mutex);
  if (server->next_message_id != 0u) {
    for (index = 0u; index < server->slot_count; ++index) {
      turbo_flow_chttp_server_slot_t *slot = &server->slots[index];
      if (slot->occupied) continue;
      if (slot->generation == UINT64_MAX) {
        status = SALTS_ERANGE;
        continue;
      }
      ++slot->generation;
      slot->occupied = true;
      slot->message_id = server->next_message_id;
      slot->publication_status = SALTS_OK;
      *message_id = server->next_message_id;
      server->next_message_id =
          server->next_message_id == UINT64_MAX ? 0u : server->next_message_id + 1u;
      ++server->active_requests;
      selected = slot;
      status = SALTS_OK;
      break;
    }
  } else {
    status = SALTS_ERANGE;
  }
  salts_mutex_unlock(&server->mutex);
  *out_slot = selected;
  return status;
}

static void chttp_server_adapter_release_unadmitted(turbo_flow_chttp_server_slot_t *slot,
                                                    int status) {
  turbo_flow_chttp_server_t *server;
  turbo_flow_msg_t released;
  if (!slot || !(server = slot->owner)) return;
  turbo_flow_msg_init(&released);
  salts_mutex_lock(&server->mutex);
  if (slot->occupied) {
    chttp_server_adapter_slot_reset(slot, &released);
    if (server->active_requests != 0u) --server->active_requests;
    chttp_server_adapter_counter_increment(&server->rejected_requests);
    server->last_status = status;
  }
  salts_mutex_unlock(&server->mutex);
  turbo_flow_msg_cleanup(&released);
}

static int chttp_server_adapter_request_size(const chttp_server_request_view *request,
                                             size_t *out_size) {
  size_t total = sizeof(chttp_server_message_storage_t);
  size_t index;
  if (!request || !out_size || !request->target || !request->path ||
      (request->header_count != 0u && !request->headers) ||
      (request->param_count != 0u && !request->params) ||
      (request->body_size != 0u && !request->body) ||
      request->header_count > SIZE_MAX / sizeof(chttp_server_message_field_t) ||
      request->param_count > SIZE_MAX / sizeof(chttp_server_message_field_t)) {
    return SALTS_EINVAL;
  }
  if (chttp_server_adapter_add_size(&total, request->header_count *
                                                sizeof(chttp_server_message_field_t)) != SALTS_OK ||
      chttp_server_adapter_add_size(&total, request->param_count *
                                                sizeof(chttp_server_message_field_t)) != SALTS_OK ||
      chttp_server_adapter_add_size(&total, strlen(request->target)) != SALTS_OK ||
      chttp_server_adapter_add_size(&total, strlen(request->path)) != SALTS_OK) {
    return SALTS_ERANGE;
  }
  for (index = 0u; index < request->header_count; ++index) {
    if (!request->headers[index].name || !request->headers[index].value) return SALTS_EINVAL;
    if (chttp_server_adapter_add_size(&total, strlen(request->headers[index].name)) != SALTS_OK ||
        chttp_server_adapter_add_size(&total, strlen(request->headers[index].value)) != SALTS_OK) {
      return SALTS_ERANGE;
    }
  }
  for (index = 0u; index < request->param_count; ++index) {
    if (!request->params[index].name || !request->params[index].value) return SALTS_EINVAL;
    if (chttp_server_adapter_add_size(&total, strlen(request->params[index].name)) != SALTS_OK ||
        chttp_server_adapter_add_size(&total, strlen(request->params[index].value)) != SALTS_OK) {
      return SALTS_ERANGE;
    }
  }
  if (request->peer_certificate_sha256 &&
      chttp_server_adapter_add_size(&total, strlen(request->peer_certificate_sha256)) != SALTS_OK) {
    return SALTS_ERANGE;
  }
  if (chttp_server_adapter_add_size(&total, request->body_size) != SALTS_OK) return SALTS_ERANGE;
  *out_size = total;
  return SALTS_OK;
}

static void chttp_server_adapter_copy_field(unsigned char *base, size_t *cursor,
                                            chttp_server_message_field_t *field, const char *name,
                                            const char *value) {
  field->name_offset = *cursor;
  field->name_size = strlen(name);
  memcpy(base + *cursor, name, field->name_size);
  *cursor += field->name_size;
  field->value_offset = *cursor;
  field->value_size = strlen(value);
  memcpy(base + *cursor, value, field->value_size);
  *cursor += field->value_size;
}

static int chttp_server_adapter_make_message(turbo_flow_chttp_server_slot_t *slot,
                                             const chttp_server_request_view *request,
                                             turbo_flow_msg_t *message) {
  turbo_flow_chttp_server_t *server;
  chttp_server_message_storage_t *storage;
  chttp_server_message_field_t *headers;
  chttp_server_message_field_t *params;
  mem_buffer_t *buffer;
  unsigned char *base;
  size_t total;
  size_t cursor;
  size_t index;
  int rc;
  if (!slot || !(server = slot->owner) || !request || !message) return SALTS_EINVAL;
  rc = chttp_server_adapter_request_size(request, &total);
  if (rc != SALTS_OK) return rc;
  if (total > server->max_request_message_bytes) return SALTS_EMSGSIZE;
  buffer = mem_get_buffer(mem_global(), total);
  if (!buffer) return SALTS_ENOMEM;
  base = (unsigned char *)mem_buffer_data(buffer);
  memset(base, 0, sizeof(*storage));
  storage = (chttp_server_message_storage_t *)base;
  storage->public_context.size = sizeof(storage->public_context);
  storage->public_context.version = TURBO_FLOW_CHTTP_SERVER_API_VERSION;
  storage->public_context.http_major = request->http_major;
  storage->public_context.http_minor = request->http_minor;
  storage->public_context.method = request->method;
  storage->public_context.header_count = request->header_count;
  storage->public_context.param_count = request->param_count;
  storage->public_context.body_streamed = request->body_streamed;
  storage->public_context.protocol_keep_alive = request->protocol_keep_alive;
  storage->public_context.has_peer = request->peer != NULL;
  if (request->peer) storage->public_context.peer = *request->peer;
  storage->magic = CHTTP_SERVER_REQUEST_MAGIC;
  storage->owner = server;
  storage->slot_index = slot->index;
  storage->slot_generation = slot->generation;
  cursor = sizeof(*storage);
  storage->headers_offset = cursor;
  headers = (chttp_server_message_field_t *)(base + cursor);
  cursor += request->header_count * sizeof(*headers);
  storage->params_offset = cursor;
  params = (chttp_server_message_field_t *)(base + cursor);
  cursor += request->param_count * sizeof(*params);
  storage->target_offset = cursor;
  storage->target_size = strlen(request->target);
  memcpy(base + cursor, request->target, storage->target_size);
  cursor += storage->target_size;
  storage->path_offset = cursor;
  storage->path_size = strlen(request->path);
  memcpy(base + cursor, request->path, storage->path_size);
  cursor += storage->path_size;
  for (index = 0u; index < request->header_count; ++index)
    chttp_server_adapter_copy_field(base, &cursor, &headers[index], request->headers[index].name,
                                    request->headers[index].value);
  for (index = 0u; index < request->param_count; ++index)
    chttp_server_adapter_copy_field(base, &cursor, &params[index], request->params[index].name,
                                    request->params[index].value);
  storage->peer_certificate_offset = cursor;
  storage->peer_certificate_size =
      request->peer_certificate_sha256 ? strlen(request->peer_certificate_sha256) : 0u;
  if (storage->peer_certificate_size != 0u) {
    memcpy(base + cursor, request->peer_certificate_sha256, storage->peer_certificate_size);
    cursor += storage->peer_certificate_size;
  }
  if (request->body_size != 0u) memcpy(base + cursor, request->body, request->body_size);
  mem_set_used(buffer, total);
  turbo_flow_msg_init(message);
  message->id = slot->message_id;
  message->buffer = buffer;
  message->payload = vstr_from_buf((const char *)base + cursor, request->body_size);
  message->transport_context = storage;
  return SALTS_OK;
}

static int chttp_server_adapter_immediate(chttp_server_response *response, unsigned int status,
                                          const char *content_type, const char *body) {
  return chttp_server_reply(response, status, content_type, body, strlen(body));
}

static bool chttp_server_adapter_prepare_reply(turbo_flow_chttp_server_slot_t *slot,
                                               chttp_server_reply_work_t *work) {
  turbo_flow_chttp_server_t *server;
  if (!slot || !work || !(server = slot->owner)) return false;
  memset(work, 0, sizeof(*work));
  turbo_flow_msg_init(&work->response);
  salts_mutex_lock(&server->mutex);
  if (!slot->occupied || !slot->publication_done || !slot->deferred_attached ||
      slot->reply_in_progress) {
    salts_mutex_unlock(&server->mutex);
    return false;
  }
  slot->reply_in_progress = true;
  work->slot = slot;
  work->generation = slot->generation;
  work->deferred = slot->deferred;
  if (slot->publication_status == SALTS_OK && slot->response_ready) {
    (void)turbo_flow_msg_move(&work->response, &slot->response);
    slot->response_ready = false;
    work->status_code = chttp_server_adapter_status_valid((unsigned int)work->response.status)
                            ? (unsigned int)work->response.status
                            : server->success_status;
    work->content_type = server->response_content_type;
    work->body = work->response.payload.data;
    work->body_size = work->response.payload.len;
  } else {
    work->status_code = server->graph_error_status;
    work->content_type = server->error_content_type;
    work->body = server->graph_error_body;
    work->body_size = tstr_len(server->graph_error_body);
  }
  salts_mutex_unlock(&server->mutex);
  return true;
}

static void chttp_server_adapter_finish_reply(chttp_server_reply_work_t *work, int reply_status,
                                              int cancel_status) {
  turbo_flow_chttp_server_slot_t *slot;
  turbo_flow_chttp_server_t *server;
  turbo_flow_msg_t released;
  if (!work || !(slot = work->slot) || !(server = slot->owner)) return;
  turbo_flow_msg_init(&released);
  salts_mutex_lock(&server->mutex);
  if (slot->occupied && slot->generation == work->generation) {
    const bool reply_terminal = reply_status == SALTS_OK || reply_status == SALTS_ENOENT ||
                                reply_status == SALTS_EALREADY;
    const bool cancel_terminal = !reply_terminal &&
                                 (cancel_status == SALTS_OK || cancel_status == SALTS_ENOENT);
    server->last_status = reply_status;
    if (reply_terminal || cancel_terminal) {
      if (reply_status == SALTS_OK) {
        chttp_server_adapter_counter_increment(&server->completed_requests);
        if (work->body_size <= UINT64_MAX - server->response_bytes)
          server->response_bytes += (uint64_t)work->body_size;
        else server->last_status = SALTS_ERANGE;
      }
      chttp_server_adapter_slot_reset(slot, &released);
      if (server->active_requests != 0u) --server->active_requests;
      if (cancel_terminal) server->state = TURBO_FLOW_CHTTP_SERVER_FAILED;
    } else {
      slot->reply_in_progress = false;
      server->last_status = cancel_status;
      server->state = TURBO_FLOW_CHTTP_SERVER_FAILED;
    }
  }
  salts_mutex_unlock(&server->mutex);
  turbo_flow_msg_cleanup(&released);
  turbo_flow_msg_cleanup(&work->response);
}

static void chttp_server_adapter_execute_reply(turbo_flow_chttp_server_slot_t *slot) {
  chttp_server_reply_work_t work;
  chttp_server_deferred_response response;
  int reply_status;
  int cancel_status = SALTS_OK;
  if (!chttp_server_adapter_prepare_reply(slot, &work)) return;
  response = (chttp_server_deferred_response){.size = sizeof(response),
                                              .status_code = work.status_code,
                                              .content_type = work.content_type,
                                              .body = work.body,
                                              .body_size = work.body_size};
  reply_status = chttp_server_deferred_reply(&work.deferred, &response);
  if (reply_status != SALTS_OK && reply_status != SALTS_ENOENT &&
      reply_status != SALTS_EALREADY) {
    cancel_status = chttp_server_deferred_cancel(&work.deferred);
  }
  chttp_server_adapter_finish_reply(&work, reply_status, cancel_status);
}

static void chttp_server_adapter_publication_complete(void *ctx,
                                                      const turbo_flow_publish_result_t *result) {
  turbo_flow_chttp_server_slot_t *slot = (turbo_flow_chttp_server_slot_t *)ctx;
  turbo_flow_chttp_server_t *server = slot ? slot->owner : NULL;
  turbo_flow_msg_t released;
  bool retire_without_deferred = false;
  if (!slot || !server) return;
  turbo_flow_msg_init(&released);
  salts_mutex_lock(&server->mutex);
  if (slot->occupied) {
    slot->publication_status = result ? result->status : SALTS_EINVAL;
    slot->publication_done = true;
    server->last_status = slot->publication_status;
    if (slot->defer_failed) {
      chttp_server_adapter_slot_reset(slot, &released);
      if (server->active_requests != 0u) --server->active_requests;
      chttp_server_adapter_counter_increment(&server->rejected_requests);
      retire_without_deferred = true;
    }
  }
  salts_mutex_unlock(&server->mutex);
  turbo_flow_msg_cleanup(&released);
  if (!retire_without_deferred) chttp_server_adapter_execute_reply(slot);
}

static int chttp_server_adapter_handler(void *user, const chttp_server_request_view *request,
                                        chttp_server_response *response) {
  turbo_flow_chttp_server_t *server = (turbo_flow_chttp_server_t *)user;
  turbo_flow_chttp_server_slot_t *slot;
  turbo_flow_msg_t message;
  chttp_server_deferred deferred = CHTTP_SERVER_DEFERRED_INIT;
  uint64_t message_id = 0u;
  unsigned int rejection_status;
  bool release_completed_publication = false;
  int status;
  if (!server || !request || !response) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  status = server->state == TURBO_FLOW_CHTTP_SERVER_RUNNING ? SALTS_OK : SALTS_ESHUTDOWN;
  salts_mutex_unlock(&server->mutex);
  if (status != SALTS_OK) {
    return chttp_server_adapter_immediate(response, server->unavailable_status,
                                          server->error_content_type, "flow unavailable");
  }
  status = chttp_server_adapter_reserve_slot(server, &slot, &message_id);
  if (status != SALTS_OK) {
    salts_mutex_lock(&server->mutex);
    chttp_server_adapter_counter_increment(&server->rejected_requests);
    server->last_status = status;
    salts_mutex_unlock(&server->mutex);
    return chttp_server_adapter_immediate(
        response, status == SALTS_ENOSPC ? server->overload_status : server->unavailable_status,
        server->error_content_type,
        status == SALTS_ENOSPC ? "flow overloaded" : "flow unavailable");
  }
  (void)message_id;
  turbo_flow_msg_init(&message);
  status = chttp_server_adapter_make_message(slot, request, &message);
  if (status != SALTS_OK) {
    chttp_server_adapter_release_unadmitted(slot, status);
    rejection_status = status == SALTS_EMSGSIZE ? 413u : server->unavailable_status;
    return chttp_server_adapter_immediate(response, rejection_status, server->error_content_type,
                                          status == SALTS_EMSGSIZE ? "request too large"
                                                                   : "flow unavailable");
  }
  status = turbo_flow_publish_async(server->flow, server->source_name, &message,
                                    chttp_server_adapter_publication_complete, slot);
  turbo_flow_msg_cleanup(&message);
  if (status != SALTS_OK) {
    chttp_server_adapter_release_unadmitted(slot, status);
    rejection_status =
        status == SALTS_ENOSPC ? server->overload_status : server->unavailable_status;
    return chttp_server_adapter_immediate(response, rejection_status, server->error_content_type,
                                          status == SALTS_ENOSPC ? "flow overloaded"
                                                                 : "flow unavailable");
  }
  status = chttp_server_response_defer(response, &deferred);
  salts_mutex_lock(&server->mutex);
  if (slot->occupied) {
    if (status == SALTS_OK) {
      slot->deferred = deferred;
      slot->deferred_attached = true;
      chttp_server_adapter_counter_increment(&server->admitted_requests);
    } else {
      slot->defer_failed = true;
      server->last_status = status;
      release_completed_publication = slot->publication_done;
    }
  }
  salts_mutex_unlock(&server->mutex);
  if (status == SALTS_OK) chttp_server_adapter_execute_reply(slot);
  else if (release_completed_publication) chttp_server_adapter_release_unadmitted(slot, status);
  return status;
}

static int chttp_server_adapter_terminal_submit(void *ctx, turbo_flow_t *flow,
                                                const turbo_flow_stage_plan_t *stage,
                                                const turbo_flow_msg_t *message,
                                                turbo_flow_async_terminal_claim_t *claim) {
  turbo_flow_chttp_server_t *server = (turbo_flow_chttp_server_t *)ctx;
  const chttp_server_message_storage_t *storage = chttp_server_adapter_storage(message);
  turbo_flow_chttp_server_slot_t *slot;
  turbo_flow_async_terminal_claim_t owned_claim = TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  turbo_flow_msg_t response;
  unsigned int response_status;
  int status;
  (void)stage;
  if (!server || flow != server->flow || !storage || storage->owner != server || !claim ||
      storage->slot_index >= server->slot_count) {
    return SALTS_EINVAL;
  }
  if (message->payload.len > server->config.max_buffered_response_body_bytes) return SALTS_EMSGSIZE;
  response_status = chttp_server_adapter_status_valid((unsigned int)message->status)
                        ? (unsigned int)message->status
                        : server->success_status;
  if (message->payload.len != 0u && !chttp_server_adapter_status_allows_body(response_status))
    return SALTS_EINVAL;
  turbo_flow_msg_init(&response);
  status = turbo_flow_msg_clone(&response, message);
  if (status != SALTS_OK) return status;
  slot = &server->slots[storage->slot_index];
  salts_mutex_lock(&server->mutex);
  if (!slot->occupied || slot->generation != storage->slot_generation) {
    status = SALTS_ENOENT;
  } else if (slot->response_ready) {
    status = SALTS_EALREADY;
  } else {
    status = turbo_flow_async_terminal_claim_move(&owned_claim, claim);
    if (status == SALTS_OK) {
      (void)turbo_flow_msg_move(&slot->response, &response);
      slot->response_ready = true;
    }
  }
  salts_mutex_unlock(&server->mutex);
  turbo_flow_msg_cleanup(&response);
  if (status != SALTS_OK) return status;
  status = turbo_flow_async_terminal_complete(&owned_claim, SALTS_OK, NULL);
  if (status != SALTS_OK) {
    salts_mutex_lock(&server->mutex);
    server->last_status = status;
    salts_mutex_unlock(&server->mutex);
  }
  return status;
}

static int chttp_server_adapter_start_native(turbo_flow_chttp_server_t *server,
                                             uint16_t *out_bound_port) {
  uint16_t bound_port = 0u;
  int status;
  if (!out_bound_port) return SALTS_EINVAL;
  *out_bound_port = 0u;
  if (server->config.enable_http2) return SALTS_ENOTSUP;
  status = chttp_server_init(&server->http, &server->config);
  if (status != SALTS_OK) return status;
  if (server->has_socket_options) {
    status = chttp_server_set_socket_options(&server->http, &server->socket_options);
    if (status != SALTS_OK) goto fail;
  }
  status = chttp_server_route(&server->http, server->method, server->route_path,
                              chttp_server_adapter_handler, server);
  if (status != SALTS_OK) goto fail;
  status = chttp_server_start(&server->http);
  if (status != SALTS_OK) goto fail;
  status = chttp_server_port(&server->http, &bound_port);
  if (status == SALTS_OK) {
    *out_bound_port = bound_port;
    return SALTS_OK;
  }
  (void)chttp_server_stop(&server->http, 0u);

fail:
  (void)chttp_server_destroy(&server->http);
  return status;
}

static int chttp_server_adapter_start(void *ctx, turbo_flow_t *flow,
                                      const turbo_flow_stage_plan_t *stage) {
  turbo_flow_chttp_server_t *server = (turbo_flow_chttp_server_t *)ctx;
  uint16_t bound_port = 0u;
  int status;
  if (!server || flow != server->flow || !stage) return SALTS_EINVAL;
  if (!stage->is_source) return SALTS_OK;
  if (strcmp(stage->name, server->source_name) != 0) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  if (server->state != TURBO_FLOW_CHTTP_SERVER_REGISTERED &&
      server->state != TURBO_FLOW_CHTTP_SERVER_STOPPED) {
    salts_mutex_unlock(&server->mutex);
    return SALTS_EALREADY;
  }
  server->state = TURBO_FLOW_CHTTP_SERVER_STARTING;
  server->last_status = SALTS_OK;
  salts_mutex_unlock(&server->mutex);
  status = chttp_server_adapter_start_native(server, &bound_port);
  salts_mutex_lock(&server->mutex);
  server->http_initialized = status == SALTS_OK;
  server->bound_port = status == SALTS_OK ? bound_port : 0u;
  server->state =
      status == SALTS_OK ? TURBO_FLOW_CHTTP_SERVER_RUNNING : TURBO_FLOW_CHTTP_SERVER_FAILED;
  server->last_status = status;
  salts_mutex_unlock(&server->mutex);
  return status;
}

static int chttp_server_adapter_stop_native(turbo_flow_chttp_server_t *server,
                                            uint32_t timeout_ms) {
  int status;
  int destroy_status;
  if (!server) return SALTS_EINVAL;
  status = chttp_server_stop(&server->http, timeout_ms);
  if (status != SALTS_OK) return status;
  destroy_status = chttp_server_destroy(&server->http);
  if (destroy_status != SALTS_OK) return destroy_status;
  return SALTS_OK;
}

static void chttp_server_adapter_stop(void *ctx, turbo_flow_t *flow,
                                      const turbo_flow_stage_plan_t *stage) {
  turbo_flow_chttp_server_t *server = (turbo_flow_chttp_server_t *)ctx;
  int status;
  if (!server || !stage || !stage->is_source) return;
  salts_mutex_lock(&server->mutex);
  if (!server->http_initialized) {
    if (server->state != TURBO_FLOW_CHTTP_SERVER_DETACHED)
      server->state = TURBO_FLOW_CHTTP_SERVER_STOPPED;
    salts_mutex_unlock(&server->mutex);
    return;
  }
  server->state = TURBO_FLOW_CHTTP_SERVER_STOPPING;
  salts_mutex_unlock(&server->mutex);
  status = chttp_server_adapter_stop_native(server, server->stop_timeout_ms);
  if (status != SALTS_OK) {
    int report_status = turbo_flow_adapter_report_stop_status(flow, status);
    if (report_status != SALTS_OK) status = report_status;
  }
  salts_mutex_lock(&server->mutex);
  if (status == SALTS_OK) {
    server->http_initialized = false;
    server->bound_port = 0u;
  }
  server->state =
      status == SALTS_OK ? TURBO_FLOW_CHTTP_SERVER_STOPPED : TURBO_FLOW_CHTTP_SERVER_FAILED;
  server->last_status = status;
  salts_mutex_unlock(&server->mutex);
}

static void chttp_server_adapter_shutdown(void *ctx) {
  turbo_flow_chttp_server_t *server = (turbo_flow_chttp_server_t *)ctx;
  bool http_initialized;
  int status;
  if (!server) return;
  salts_mutex_lock(&server->mutex);
  http_initialized = server->http_initialized;
  if (http_initialized) server->state = TURBO_FLOW_CHTTP_SERVER_STOPPING;
  salts_mutex_unlock(&server->mutex);
  status = http_initialized ? chttp_server_adapter_stop_native(server, 0u) : SALTS_OK;
  salts_mutex_lock(&server->mutex);
  server->last_status = status;
  if (status == SALTS_OK) {
    server->http_initialized = false;
    server->bound_port = 0u;
    server->flow = NULL;
    server->state = TURBO_FLOW_CHTTP_SERVER_DETACHED;
  } else {
    server->state = TURBO_FLOW_CHTTP_SERVER_FAILED;
  }
  salts_mutex_unlock(&server->mutex);
}

static int chttp_server_adapter_config_valid(const turbo_flow_chttp_server_config_t *config) {
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_CHTTP_SERVER_API_VERSION || !config->flow ||
      !config->adapter_name || config->adapter_name[0] == '\0' || !config->source_name ||
      config->source_name[0] == '\0' || !config->server || !config->server->host || !config->path ||
      config->path[0] != '/' || config->method < CHTTP_METHOD_GET ||
      config->method > CHTTP_METHOD_OPTIONS || config->server->network.connection_capacity == 0u ||
      config->server->network.connection_capacity >
          SIZE_MAX / sizeof(turbo_flow_chttp_server_slot_t) ||
      config->server->session_capacity != 0u || config->max_request_message_bytes == 0u ||
      config->server->max_buffered_response_body_bytes == 0u ||
      !chttp_server_adapter_status_valid(config->success_status) ||
      !chttp_server_adapter_status_valid(config->overload_status) ||
      !chttp_server_adapter_status_valid(config->unavailable_status) ||
      !chttp_server_adapter_status_valid(config->graph_error_status) ||
      !chttp_server_adapter_status_allows_body(config->overload_status) ||
      !chttp_server_adapter_status_allows_body(config->unavailable_status) ||
      !config->response_content_type || config->response_content_type[0] == '\0' ||
      !config->error_content_type || config->error_content_type[0] == '\0' ||
      (config->graph_error_body_size != 0u && !config->graph_error_body) ||
      (config->graph_error_body_size != 0u &&
       !chttp_server_adapter_status_allows_body(config->graph_error_status)) ||
      config->server->max_buffered_response_body_bytes < sizeof("request too large") - 1u ||
      config->graph_error_body_size > config->server->max_buffered_response_body_bytes ||
      !chttp_server_adapter_content_type_fits(config->server, config->response_content_type) ||
      !chttp_server_adapter_content_type_fits(config->server, config->error_content_type) ||
      config->first_message_id == 0u || config->stop_timeout_ms == 0u) {
    return 0;
  }
  return 1;
}

static void chttp_server_adapter_cleanup(turbo_flow_chttp_server_t *server) {
  size_t index;
  if (!server) return;
  for (index = 0u; index < server->slot_count; ++index)
    turbo_flow_msg_cleanup(&server->slots[index].response);
  free(server->slots);
  tstr_free(server->adapter_name);
  tstr_free(server->source_name);
  tstr_free(server->route_path);
  tstr_free(server->host);
  tstr_free(server->session_cookie_name);
  tstr_free(server->response_content_type);
  tstr_free(server->error_content_type);
  tstr_free(server->graph_error_body);
}

int turbo_flow_chttp_server_register(const turbo_flow_chttp_server_config_t *config,
                                     turbo_flow_chttp_server_t **out_server) {
  turbo_flow_chttp_server_t *server;
  turbo_flow_adapter_ops_t adapter_ops = {0};
  turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
  turbo_flow_adapter_schema_t schema = {0};
  size_t index;
  int status;
  if (out_server) *out_server = NULL;
  if (!out_server || !chttp_server_adapter_config_valid(config)) return SALTS_EINVAL;
  server = (turbo_flow_chttp_server_t *)calloc(1u, sizeof(*server));
  if (!server) return SALTS_ENOMEM;
  salts_mutex_init(&server->mutex);
  server->flow = config->flow;
  server->config = *config->server;
  server->method = config->method;
  server->max_request_message_bytes = config->max_request_message_bytes;
  server->success_status = config->success_status;
  server->overload_status = config->overload_status;
  server->unavailable_status = config->unavailable_status;
  server->graph_error_status = config->graph_error_status;
  server->stop_timeout_ms = config->stop_timeout_ms;
  server->next_message_id = config->first_message_id;
  server->slot_count = config->server->network.connection_capacity;
  server->state = TURBO_FLOW_CHTTP_SERVER_REGISTERED;
  server->last_status = SALTS_OK;
  server->adapter_name = tstr_dup(config->adapter_name);
  server->source_name = tstr_dup(config->source_name);
  server->route_path = tstr_dup(config->path);
  server->host = tstr_dup(config->server->host);
  if (config->server->session_cookie_name)
    server->session_cookie_name = tstr_dup(config->server->session_cookie_name);
  server->response_content_type = tstr_dup(config->response_content_type);
  server->error_content_type = tstr_dup(config->error_content_type);
  server->graph_error_body =
      tstr_new_len(config->graph_error_body_size != 0u ? config->graph_error_body : "",
                   config->graph_error_body_size);
  server->slots =
      (turbo_flow_chttp_server_slot_t *)calloc(server->slot_count, sizeof(*server->slots));
  if (!server->adapter_name || !server->source_name || !server->route_path || !server->host ||
      (config->server->session_cookie_name && !server->session_cookie_name) ||
      !server->response_content_type || !server->error_content_type || !server->graph_error_body ||
      !server->slots) {
    chttp_server_adapter_cleanup(server);
    salts_mutex_destroy(&server->mutex);
    free(server);
    return SALTS_ENOMEM;
  }
  server->config.host = server->host;
  server->config.session_cookie_name = server->session_cookie_name;
  if (config->socket_options) {
    server->socket_options = *config->socket_options;
    server->has_socket_options = true;
  }
  for (index = 0u; index < server->slot_count; ++index) {
    server->slots[index].owner = server;
    server->slots[index].index = index;
    turbo_flow_msg_init(&server->slots[index].response);
  }
  adapter_ops.start = chttp_server_adapter_start;
  adapter_ops.stop = chttp_server_adapter_stop;
  adapter_ops.shutdown = chttp_server_adapter_shutdown;
  async_ops.submit = chttp_server_adapter_terminal_submit;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_HTTP;
  schema.roles = TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_BIDIRECTIONAL;
  status = turbo_flow_register_async_terminal_adapter_ex(config->flow, config->adapter_name,
                                                         &adapter_ops, &async_ops, server, &schema);
  if (status != SALTS_OK) {
    chttp_server_adapter_cleanup(server);
    salts_mutex_destroy(&server->mutex);
    free(server);
    return status;
  }
  *out_server = server;
  return SALTS_OK;
}

int turbo_flow_chttp_server_snapshot(const turbo_flow_chttp_server_t *server,
                                     turbo_flow_chttp_server_snapshot_t *out_snapshot) {
  turbo_flow_chttp_server_snapshot_t current = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
  if (!server || !out_snapshot || out_snapshot->size < sizeof(*out_snapshot) ||
      out_snapshot->version != TURBO_FLOW_CHTTP_SERVER_API_VERSION) {
    return SALTS_EINVAL;
  }
  salts_mutex_lock((salts_mutex_t *)&server->mutex);
  current.state = server->state;
  current.bound_port = server->bound_port;
  current.active_requests = server->active_requests;
  current.request_capacity = server->slot_count;
  current.admitted_requests = server->admitted_requests;
  current.rejected_requests = server->rejected_requests;
  current.completed_requests = server->completed_requests;
  current.response_bytes = server->response_bytes;
  current.last_status = server->last_status;
  salts_mutex_unlock((salts_mutex_t *)&server->mutex);
  *out_snapshot = current;
  return SALTS_OK;
}

int turbo_flow_chttp_server_destroy(turbo_flow_chttp_server_t *server) {
  if (!server) return SALTS_EINVAL;
  salts_mutex_lock(&server->mutex);
  if (server->state != TURBO_FLOW_CHTTP_SERVER_DETACHED || server->active_requests != 0u ||
      server->http_initialized) {
    salts_mutex_unlock(&server->mutex);
    return SALTS_EBUSY;
  }
  salts_mutex_unlock(&server->mutex);
  chttp_server_adapter_cleanup(server);
  salts_mutex_destroy(&server->mutex);
  free(server);
  return SALTS_OK;
}
