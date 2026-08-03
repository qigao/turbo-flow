#include "flowie_cluster_peer_internal.h"

#include "CoroNet.h"
#include "turbo_buffer.h"
#include "turbo_deque.h"
#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

typedef struct flowie_cluster_peer_send_s {
  tstr_t bytes;
  flowie_cluster_peer_send_complete_fn complete;
  void *complete_user_data;
} flowie_cluster_peer_send_t;

struct flowie_cluster_peer_link_s {
  flowie_cluster_peer_role_t role;
  size_t max_payload_size;
  size_t max_frame_size;
  size_t queue_entries_limit;
  size_t queue_bytes_limit;
  size_t pending_entries;
  size_t pending_bytes;
  turbo_mutex_t mutex;
  turbo_deque_t queue;
  flowie_cluster_peer_link_state_t state;
  coro_socket_t *socket;
  tstr_t cluster_id;
  tstr_t local_node_id;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  tstr_t remote_node_id;
  uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_peer_authorize_fn authorize;
  flowie_cluster_peer_active_fn active;
  flowie_cluster_peer_receive_fn receive;
  void *user_data;
  mem_buffer_t *receive_buffer;
  char *receive_chunk;
  size_t receive_chunk_size;
  size_t receive_chunk_offset;
};

static int flowie_cluster_peer_link_config_text(tstr_v value, size_t maximum, int required) {
  if ((value.len != 0u && !value.data) || (required && value.len == 0u)) return TURBO_EINVAL;
  if (value.len > maximum) return TURBO_EMSGSIZE;
  return value.len != 0u && memchr(value.data, '\0', value.len) != NULL ? TURBO_EPROTO : TURBO_OK;
}

static int flowie_cluster_peer_link_nonzero(const uint8_t *value, size_t size) {
  size_t index;
  for (index = 0u; index < size; ++index)
    if (value[index] != 0u) return 1;
  return 0;
}

static int flowie_cluster_peer_link_config_validate(const flowie_cluster_peer_link_config_t *config,
                                                    size_t *max_frame_size) {
  size_t identity_overhead =
      FLOWIE_CLUSTER_ID_MAX + FLOWIE_CLUSTER_LISTENER_ID_MAX + FLOWIE_CLUSTER_NODE_ID_MAX * 2u;
  int rc;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_PEER_TRANSPORT_ABI_V1 || !max_frame_size ||
      (config->role != FLOWIE_CLUSTER_PEER_ROLE_INITIATOR &&
       config->role != FLOWIE_CLUSTER_PEER_ROLE_RESPONDER) ||
      config->max_payload_size == 0u || config->max_payload_size > UINT32_MAX ||
      config->queue_entries == 0u || config->queue_bytes == 0u || !config->authorize ||
      !config->receive) {
    return TURBO_EINVAL;
  }
  if (config->max_payload_size > SIZE_MAX - identity_overhead - FLOWIE_CLUSTER_PEER_HEADER_SIZE)
    return TURBO_ERANGE;
  *max_frame_size = FLOWIE_CLUSTER_PEER_HEADER_SIZE + identity_overhead + config->max_payload_size;
  if (*max_frame_size > UINT32_MAX) return TURBO_ERANGE;
  if (config->queue_entries > SIZE_MAX / sizeof(flowie_cluster_peer_send_t) ||
      config->queue_bytes < *max_frame_size) {
    return TURBO_EINVAL;
  }
  rc = flowie_cluster_peer_link_config_text(config->cluster_id, FLOWIE_CLUSTER_ID_MAX, 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_link_config_text(config->local_node_id, FLOWIE_CLUSTER_NODE_ID_MAX, 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_link_config_text(config->remote_node_id, FLOWIE_CLUSTER_NODE_ID_MAX,
                                              config->role == FLOWIE_CLUSTER_PEER_ROLE_INITIATOR);
  if (rc != TURBO_OK) return rc;
  if (!flowie_cluster_peer_link_nonzero(config->local_boot_id, sizeof(config->local_boot_id)) ||
      (config->role == FLOWIE_CLUSTER_PEER_ROLE_INITIATOR &&
       !flowie_cluster_peer_link_nonzero(config->remote_boot_id, sizeof(config->remote_boot_id)))) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static void flowie_cluster_peer_send_complete(flowie_cluster_peer_send_t *send, int status) {
  flowie_cluster_peer_send_complete_fn complete = send->complete;
  void *user_data = send->complete_user_data;
  tstr_free(send->bytes);
  memset(send, 0, sizeof(*send));
  if (complete) complete(user_data, status);
}

static int flowie_cluster_peer_link_pop(flowie_cluster_peer_link_t *link,
                                        flowie_cluster_peer_send_t *out) {
  int rc;
  turbo_mutex_lock(&link->mutex);
  rc = turbo_deque_pop_front(&link->queue, out);
  turbo_mutex_unlock(&link->mutex);
  return rc;
}

static void flowie_cluster_peer_link_complete_account(flowie_cluster_peer_link_t *link,
                                                      size_t bytes) {
  turbo_mutex_lock(&link->mutex);
  if (link->pending_entries != 0u) --link->pending_entries;
  if (bytes <= link->pending_bytes) link->pending_bytes -= bytes;
  turbo_mutex_unlock(&link->mutex);
}

static void flowie_cluster_peer_link_fail_pending(flowie_cluster_peer_link_t *link, int status) {
  flowie_cluster_peer_send_t send;
  while (flowie_cluster_peer_link_pop(link, &send) == TURBO_OK) {
    size_t bytes = tstr_len(send.bytes);
    flowie_cluster_peer_link_complete_account(link, bytes);
    flowie_cluster_peer_send_complete(&send, status);
  }
}

int flowie_cluster_peer_link_create(const flowie_cluster_peer_link_config_t *config,
                                    flowie_cluster_peer_link_t **out) {
  flowie_cluster_peer_link_t *link;
  size_t max_frame_size = 0u;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  rc = flowie_cluster_peer_link_config_validate(config, &max_frame_size);
  if (rc != TURBO_OK) return rc;
  link = (flowie_cluster_peer_link_t *)calloc(1u, sizeof(*link));
  if (!link) return TURBO_ENOMEM;
  link->role = config->role;
  link->max_payload_size = config->max_payload_size;
  link->max_frame_size = max_frame_size;
  link->queue_entries_limit = config->queue_entries;
  link->queue_bytes_limit = config->queue_bytes;
  link->state = FLOWIE_CLUSTER_PEER_LINK_CREATED;
  link->authorize = config->authorize;
  link->active = config->active;
  link->receive = config->receive;
  link->user_data = config->user_data;
  memcpy(link->local_boot_id, config->local_boot_id, sizeof(link->local_boot_id));
  memcpy(link->remote_boot_id, config->remote_boot_id, sizeof(link->remote_boot_id));
  link->cluster_id = tstr_from_v(config->cluster_id);
  link->local_node_id = tstr_from_v(config->local_node_id);
  if (config->remote_node_id.len != 0u) link->remote_node_id = tstr_from_v(config->remote_node_id);
  if (!link->cluster_id || !link->local_node_id ||
      (config->remote_node_id.len != 0u && !link->remote_node_id)) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  turbo_mutex_init(&link->mutex);
  rc = turbo_deque_init(&link->queue, sizeof(flowie_cluster_peer_send_t));
  if (rc != TURBO_OK) goto fail_mutex;
  rc = turbo_deque_reserve(&link->queue, link->queue_entries_limit);
  if (rc != TURBO_OK) goto fail_queue;
  link->receive_buffer = mem_get_buffer(mem_global(), link->max_frame_size);
  if (!link->receive_buffer) {
    rc = TURBO_ENOMEM;
    goto fail_queue;
  }
  *out = link;
  return TURBO_OK;

fail_queue:
  turbo_deque_destroy(&link->queue);
fail_mutex:
  turbo_mutex_destroy(&link->mutex);
fail:
  tstr_free(link->cluster_id);
  tstr_free(link->local_node_id);
  tstr_free(link->remote_node_id);
  free(link);
  return rc;
}

int flowie_cluster_peer_link_destroy(flowie_cluster_peer_link_t *link) {
  if (!link) return TURBO_OK;
  turbo_mutex_lock(&link->mutex);
  if (link->state == FLOWIE_CLUSTER_PEER_LINK_HANDSHAKING ||
      link->state == FLOWIE_CLUSTER_PEER_LINK_ACTIVE ||
      link->state == FLOWIE_CLUSTER_PEER_LINK_CLOSING) {
    turbo_mutex_unlock(&link->mutex);
    return TURBO_EBUSY;
  }
  turbo_mutex_unlock(&link->mutex);
  flowie_cluster_peer_link_fail_pending(link, TURBO_ECANCELED);
  if (link->receive_chunk) coro_socket_free_recv(link->receive_chunk);
  mem_buffer_release(link->receive_buffer);
  turbo_deque_destroy(&link->queue);
  turbo_mutex_destroy(&link->mutex);
  tstr_free(link->cluster_id);
  tstr_free(link->local_node_id);
  tstr_free(link->remote_node_id);
  free(link);
  return TURBO_OK;
}

static int flowie_cluster_peer_link_route_validate(flowie_cluster_peer_link_t *link,
                                                   const flowie_cluster_peer_frame_t *frame) {
  if (frame->cluster_id.len != tstr_len(link->cluster_id) ||
      memcmp(frame->cluster_id.data, link->cluster_id, frame->cluster_id.len) != 0 ||
      frame->source_node_id.len != tstr_len(link->local_node_id) ||
      memcmp(frame->source_node_id.data, link->local_node_id, frame->source_node_id.len) != 0 ||
      memcmp(frame->source_boot_id, link->local_boot_id, sizeof(link->local_boot_id)) != 0 ||
      !link->remote_node_id || frame->target_node_id.len != tstr_len(link->remote_node_id) ||
      memcmp(frame->target_node_id.data, link->remote_node_id, frame->target_node_id.len) != 0 ||
      memcmp(frame->target_boot_id, link->remote_boot_id, sizeof(link->remote_boot_id)) != 0) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

int flowie_cluster_peer_link_send(flowie_cluster_peer_link_t *link,
                                  const flowie_cluster_peer_frame_t *frame,
                                  flowie_cluster_peer_send_complete_fn complete,
                                  void *complete_user_data) {
  flowie_cluster_peer_send_t send;
  size_t bytes;
  int rc;
  if (!link || !frame) return TURBO_EINVAL;
  memset(&send, 0, sizeof(send));
  rc = flowie_cluster_peer_frame_encode(frame, link->max_payload_size, &send.bytes);
  if (rc != TURBO_OK) return rc;
  bytes = tstr_len(send.bytes);
  send.complete = complete;
  send.complete_user_data = complete_user_data;
  turbo_mutex_lock(&link->mutex);
  if (link->state != FLOWIE_CLUSTER_PEER_LINK_ACTIVE) {
    rc = link->state == FLOWIE_CLUSTER_PEER_LINK_CLOSING ||
                 link->state == FLOWIE_CLUSTER_PEER_LINK_CLOSED
             ? TURBO_ECANCELED
             : TURBO_EBUSY;
  } else if (flowie_cluster_peer_link_route_validate(link, frame) != TURBO_OK) {
    rc = TURBO_EPROTO;
  } else if (link->pending_entries >= link->queue_entries_limit ||
             bytes > link->queue_bytes_limit - link->pending_bytes) {
    rc = TURBO_ENOSPC;
  } else {
    rc = turbo_deque_push_back(&link->queue, &send);
    if (rc == TURBO_OK) {
      ++link->pending_entries;
      link->pending_bytes += bytes;
      rc = coro_socket_interrupt_wait(link->socket, TURBO_OK);
      if (rc != TURBO_OK) {
        flowie_cluster_peer_send_t rollback;
        (void)turbo_deque_pop_back(&link->queue, &rollback);
        --link->pending_entries;
        link->pending_bytes -= bytes;
      }
    }
  }
  turbo_mutex_unlock(&link->mutex);
  if (rc != TURBO_OK) tstr_free(send.bytes);
  return rc;
}

int flowie_cluster_peer_link_close(flowie_cluster_peer_link_t *link) {
  int rc = TURBO_OK;
  if (!link) return TURBO_EINVAL;
  turbo_mutex_lock(&link->mutex);
  if (link->state == FLOWIE_CLUSTER_PEER_LINK_CLOSED) {
    rc = TURBO_EALREADY;
  } else if (link->state == FLOWIE_CLUSTER_PEER_LINK_CREATED) {
    link->state = FLOWIE_CLUSTER_PEER_LINK_CLOSED;
  } else {
    link->state = FLOWIE_CLUSTER_PEER_LINK_CLOSING;
    rc = coro_socket_interrupt_wait(link->socket, TURBO_OK);
  }
  turbo_mutex_unlock(&link->mutex);
  return rc;
}

flowie_cluster_peer_link_state_t flowie_cluster_peer_link_state(flowie_cluster_peer_link_t *link) {
  flowie_cluster_peer_link_state_t state;
  if (!link) return FLOWIE_CLUSTER_PEER_LINK_CLOSED;
  turbo_mutex_lock(&link->mutex);
  state = link->state;
  turbo_mutex_unlock(&link->mutex);
  return state;
}

int flowie_cluster_peer_link_pending(flowie_cluster_peer_link_t *link, size_t *entries,
                                     size_t *bytes) {
  if (!link || !entries || !bytes) return TURBO_EINVAL;
  turbo_mutex_lock(&link->mutex);
  *entries = link->pending_entries;
  *bytes = link->pending_bytes;
  turbo_mutex_unlock(&link->mutex);
  return TURBO_OK;
}

static int flowie_cluster_peer_link_decode_buffer(flowie_cluster_peer_link_t *link,
                                                  flowie_cluster_peer_frame_t *out) {
  size_t consumed = 0u;
  size_t remaining;
  int rc = flowie_cluster_peer_frame_decode(mem_buffer_const_data(link->receive_buffer),
                                            mem_buffer_used(link->receive_buffer),
                                            link->max_payload_size, out, &consumed);
  if (rc != TURBO_OK) return rc;
  remaining = mem_buffer_used(link->receive_buffer) - consumed;
  if (remaining != 0u)
    memmove(mem_buffer_data(link->receive_buffer),
            mem_buffer_const_data(link->receive_buffer) + consumed, remaining);
  mem_set_used(link->receive_buffer, remaining);
  return TURBO_OK;
}

static int flowie_cluster_peer_link_receive_next(flowie_cluster_peer_link_t *link,
                                                 flowie_cluster_peer_frame_t *out) {
  for (;;) {
    int rc = flowie_cluster_peer_link_decode_buffer(link, out);
    if (rc != FLOWIE_CLUSTER_PEER_INCOMPLETE) return rc;
    if (link->receive_chunk) {
      size_t available = mem_remaining(link->receive_buffer);
      size_t remaining = link->receive_chunk_size - link->receive_chunk_offset;
      size_t copy_size = remaining < available ? remaining : available;
      if (copy_size != 0u) {
        memcpy(mem_write_ptr(link->receive_buffer),
               link->receive_chunk + link->receive_chunk_offset, copy_size);
        mem_set_used(link->receive_buffer, mem_buffer_used(link->receive_buffer) + copy_size);
        link->receive_chunk_offset += copy_size;
      }
      if (link->receive_chunk_offset == link->receive_chunk_size) {
        coro_socket_free_recv(link->receive_chunk);
        link->receive_chunk = NULL;
        link->receive_chunk_size = 0u;
        link->receive_chunk_offset = 0u;
      }
      if (copy_size == 0u) return TURBO_EMSGSIZE;
      continue;
    }
    rc = coro_socket_recv(link->socket, &link->receive_chunk, &link->receive_chunk_size);
    if (rc != TURBO_OK) return rc;
    link->receive_chunk_offset = 0u;
    if (!link->receive_chunk && link->receive_chunk_size == 0u) return TURBO_EINTR;
    if (!link->receive_chunk || link->receive_chunk_size == 0u) return TURBO_EPROTO;
  }
}

static void flowie_cluster_peer_link_control_frame(flowie_cluster_peer_link_t *link,
                                                   flowie_cluster_peer_frame_kind_t kind,
                                                   tstr_v target_node_id,
                                                   const uint8_t *target_boot_id, tstr_v payload,
                                                   flowie_cluster_peer_frame_t *frame) {
  *frame = (flowie_cluster_peer_frame_t)FLOWIE_CLUSTER_PEER_FRAME_INIT;
  frame->kind = kind;
  frame->cluster_id = tstr_to_v(link->cluster_id);
  frame->source_node_id = tstr_to_v(link->local_node_id);
  frame->target_node_id = target_node_id;
  frame->payload = payload;
  memcpy(frame->source_boot_id, link->local_boot_id, sizeof(frame->source_boot_id));
  memcpy(frame->target_boot_id, target_boot_id, sizeof(frame->target_boot_id));
}

static int flowie_cluster_peer_link_send_direct(flowie_cluster_peer_link_t *link,
                                                const flowie_cluster_peer_frame_t *frame) {
  tstr_t encoded = NULL;
  int rc = flowie_cluster_peer_frame_encode(frame, link->max_payload_size, &encoded);
  if (rc == TURBO_OK) rc = coro_socket_send(link->socket, encoded, tstr_len(encoded));
  tstr_free(encoded);
  return rc;
}

static int flowie_cluster_peer_link_authorize(flowie_cluster_peer_link_t *link,
                                              const flowie_cluster_peer_frame_t *frame,
                                              const char *certificate_sha256) {
  return link->authorize(link->user_data, frame->source_node_id, frame->source_boot_id,
                         certificate_sha256);
}

static int flowie_cluster_peer_link_handshake_initiator(flowie_cluster_peer_link_t *link,
                                                        tstr_v channel_binding,
                                                        const char *certificate_sha256) {
  flowie_cluster_peer_frame_t hello;
  flowie_cluster_peer_frame_t reply = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  int rc;
  flowie_cluster_peer_link_control_frame(link, FLOWIE_CLUSTER_PEER_FRAME_HELLO,
                                         tstr_to_v(link->remote_node_id), link->remote_boot_id,
                                         channel_binding, &hello);
  rc = flowie_cluster_peer_link_send_direct(link, &hello);
  do {
    if (rc == TURBO_OK) rc = flowie_cluster_peer_link_receive_next(link, &reply);
    if (rc == TURBO_EINTR &&
        flowie_cluster_peer_link_state(link) == FLOWIE_CLUSTER_PEER_LINK_CLOSING)
      rc = TURBO_ECANCELED;
  } while (rc == TURBO_EINTR);
  if (rc == TURBO_OK && reply.kind != FLOWIE_CLUSTER_PEER_FRAME_HELLO_ACK) rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_frame_require_target(
        &reply, tstr_to_v(link->cluster_id), tstr_to_v(link->local_node_id), link->local_boot_id);
  if (rc == TURBO_OK &&
      (reply.source_node_id.len != tstr_len(link->remote_node_id) ||
       memcmp(reply.source_node_id.data, link->remote_node_id, reply.source_node_id.len) != 0 ||
       memcmp(reply.source_boot_id, link->remote_boot_id, sizeof(link->remote_boot_id)) != 0 ||
       !tstr_v_eq(reply.payload, channel_binding))) {
    rc = TURBO_EPROTO;
  }
  if (rc == TURBO_OK) rc = flowie_cluster_peer_link_authorize(link, &reply, certificate_sha256);
  flowie_cluster_peer_frame_cleanup(&reply);
  return rc;
}

static int flowie_cluster_peer_link_handshake_responder(flowie_cluster_peer_link_t *link,
                                                        tstr_v channel_binding,
                                                        const char *certificate_sha256) {
  flowie_cluster_peer_frame_t hello = FLOWIE_CLUSTER_PEER_FRAME_INIT;
  flowie_cluster_peer_frame_t reply;
  int rc;
  do {
    rc = flowie_cluster_peer_link_receive_next(link, &hello);
    if (rc == TURBO_EINTR &&
        flowie_cluster_peer_link_state(link) == FLOWIE_CLUSTER_PEER_LINK_CLOSING)
      rc = TURBO_ECANCELED;
  } while (rc == TURBO_EINTR);
  if (rc == TURBO_OK && hello.kind != FLOWIE_CLUSTER_PEER_FRAME_HELLO) rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_frame_require_target(
        &hello, tstr_to_v(link->cluster_id), tstr_to_v(link->local_node_id), link->local_boot_id);
  if (rc == TURBO_OK && !tstr_v_eq(hello.payload, channel_binding)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flowie_cluster_peer_link_authorize(link, &hello, certificate_sha256);
  if (rc == TURBO_OK) {
    link->remote_node_id = tstr_from_v(hello.source_node_id);
    if (!link->remote_node_id) {
      rc = TURBO_ENOMEM;
    } else {
      memcpy(link->remote_boot_id, hello.source_boot_id, sizeof(link->remote_boot_id));
    }
  }
  if (rc == TURBO_OK) {
    flowie_cluster_peer_link_control_frame(link, FLOWIE_CLUSTER_PEER_FRAME_HELLO_ACK,
                                           tstr_to_v(link->remote_node_id), link->remote_boot_id,
                                           channel_binding, &reply);
    rc = flowie_cluster_peer_link_send_direct(link, &reply);
  }
  flowie_cluster_peer_frame_cleanup(&hello);
  return rc;
}

static int flowie_cluster_peer_link_drain_one(flowie_cluster_peer_link_t *link) {
  flowie_cluster_peer_send_t send;
  size_t bytes;
  int rc = flowie_cluster_peer_link_pop(link, &send);
  if (rc != TURBO_OK) return rc;
  bytes = tstr_len(send.bytes);
  rc = coro_socket_send(link->socket, send.bytes, bytes);
  flowie_cluster_peer_link_complete_account(link, bytes);
  flowie_cluster_peer_send_complete(&send, rc);
  return rc;
}

int flowie_cluster_peer_link_run(flowie_cluster_peer_link_t *link,
                                 coro_socket_t *connected_tls_socket) {
  uint8_t channel_binding[CORO_TLS_CHANNEL_BINDING_SIZE];
  char certificate_sha256[CORO_TLS_PEER_CERT_SHA256_CAPACITY];
  int rc;
  if (!link || !connected_tls_socket) return TURBO_EINVAL;
  turbo_mutex_lock(&link->mutex);
  if (link->state != FLOWIE_CLUSTER_PEER_LINK_CREATED) {
    turbo_mutex_unlock(&link->mutex);
    return TURBO_EBUSY;
  }
  link->state = FLOWIE_CLUSTER_PEER_LINK_HANDSHAKING;
  link->socket = connected_tls_socket;
  turbo_mutex_unlock(&link->mutex);
  rc = coro_socket_tls_get_verified_peer_certificate_sha256(connected_tls_socket,
                                                            certificate_sha256);
  if (rc == TURBO_OK)
    rc = coro_socket_tls_export_channel_binding(connected_tls_socket, channel_binding);
  if (rc == TURBO_OK) {
    tstr_v binding = tstr_v_from_buf((const char *)channel_binding, sizeof(channel_binding));
    rc = link->role == FLOWIE_CLUSTER_PEER_ROLE_INITIATOR
             ? flowie_cluster_peer_link_handshake_initiator(link, binding, certificate_sha256)
             : flowie_cluster_peer_link_handshake_responder(link, binding, certificate_sha256);
  }
  turbo_mutex_lock(&link->mutex);
  if (rc == TURBO_OK && link->state == FLOWIE_CLUSTER_PEER_LINK_HANDSHAKING)
    link->state = FLOWIE_CLUSTER_PEER_LINK_ACTIVE;
  turbo_mutex_unlock(&link->mutex);

  if (rc == TURBO_OK && link->active)
    rc = link->active(link->user_data, link, tstr_to_v(link->remote_node_id), link->remote_boot_id);

  while (rc == TURBO_OK) {
    flowie_cluster_peer_frame_t frame = FLOWIE_CLUSTER_PEER_FRAME_INIT;
    flowie_cluster_peer_link_state_t state;
    size_t pending;
    turbo_mutex_lock(&link->mutex);
    state = link->state;
    pending = link->pending_entries;
    turbo_mutex_unlock(&link->mutex);
    if (pending != 0u) {
      rc = flowie_cluster_peer_link_drain_one(link);
      continue;
    }
    if (state == FLOWIE_CLUSTER_PEER_LINK_CLOSING) break;
    rc = flowie_cluster_peer_link_receive_next(link, &frame);
    if (rc == TURBO_EINTR) {
      rc = TURBO_OK;
      continue;
    }
    if (rc == TURBO_OK)
      rc = flowie_cluster_peer_frame_require_target(
          &frame, tstr_to_v(link->cluster_id), tstr_to_v(link->local_node_id), link->local_boot_id);
    if (rc == TURBO_OK &&
        (frame.source_node_id.len != tstr_len(link->remote_node_id) ||
         memcmp(frame.source_node_id.data, link->remote_node_id, frame.source_node_id.len) != 0 ||
         memcmp(frame.source_boot_id, link->remote_boot_id, sizeof(link->remote_boot_id)) != 0)) {
      rc = TURBO_EPROTO;
    }
    if (rc == TURBO_OK) rc = link->receive(link->user_data, &frame);
    flowie_cluster_peer_frame_cleanup(&frame);
  }

  if (rc != TURBO_OK) {
    turbo_mutex_lock(&link->mutex);
    link->state = FLOWIE_CLUSTER_PEER_LINK_CLOSING;
    turbo_mutex_unlock(&link->mutex);
    flowie_cluster_peer_link_fail_pending(link, rc);
  }
  turbo_mutex_lock(&link->mutex);
  link->socket = NULL;
  link->state = FLOWIE_CLUSTER_PEER_LINK_CLOSED;
  turbo_mutex_unlock(&link->mutex);
  return rc;
}

static int flowie_cluster_peer_tls_paths_validate(coro_socket_t *socket, const char *ca_file,
                                                  const char *cert_file, const char *key_file) {
  return !socket || !ca_file || !*ca_file || !cert_file || !*cert_file || !key_file || !*key_file
             ? TURBO_EINVAL
             : TURBO_OK;
}

int flowie_cluster_peer_tls_client_configure(coro_socket_t *socket, const char *ca_file,
                                             const char *cert_file, const char *key_file,
                                             const char *key_password) {
  turbo_tls_client_config_t config;
  int rc = flowie_cluster_peer_tls_paths_validate(socket, ca_file, cert_file, key_file);
  if (rc != TURBO_OK) return rc;
  memset(&config, 0, sizeof(config));
  config.ca_file = ca_file;
  config.cert_file = cert_file;
  config.key_file = key_file;
  config.key_password = key_password;
  config.verify_peer = 1;
  return coro_socket_set_tls_client_config(socket, &config);
}

int flowie_cluster_peer_tls_server_configure(coro_socket_t *socket, const char *ca_file,
                                             const char *cert_file, const char *key_file,
                                             const char *key_password) {
  turbo_tls_server_config_t config;
  int rc = flowie_cluster_peer_tls_paths_validate(socket, ca_file, cert_file, key_file);
  if (rc != TURBO_OK) return rc;
  memset(&config, 0, sizeof(config));
  config.size = sizeof(config);
  config.ca_file = ca_file;
  config.cert_file = cert_file;
  config.key_file = key_file;
  config.key_password = key_password;
  config.client_auth = TURBO_TLS_CLIENT_AUTH_REQUIRED;
  return coro_socket_set_tls_server_config(socket, &config);
}
