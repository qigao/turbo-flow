#include "turbo_flow_cnet.h"

#include <cflow/scheduler.h>
#include <salts_error.h>
#include <salts_str.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct listener_source_slot_s listener_source_slot_t;

struct listener_source_slot_s {
  turbo_flow_cnet_listener_source_t *owner;
  cnet_connection connection;
  bool occupied;
  bool connected;
  bool closing;
  bool receive_pending;
};

struct turbo_flow_cnet_listener_source_s {
  turbo_flow_cnet_listener_source_state_t state;
  int status;
  int native_status;
  char error_stage[TURBO_FLOW_CNET_STREAM_SOURCE_ERROR_STAGE_CAPACITY];
  int last_connection_status;
  int last_connection_native_status;
  char last_connection_error_stage[TURBO_FLOW_CNET_STREAM_SOURCE_ERROR_STAGE_CAPACITY];
  char publisher_error[192];
  tstr source_name;
  cnet_listener listener;
  cnet_client client;
  cnet_tls_server tls_server;
  cflow_scheduler scheduler;
  turbo_flow_run_t *run;
  listener_source_slot_t *slots;
  turbo_flow_msg_t ready_message;
  cflow_waker value_waker;
  cflow_waker terminal_waker;
  turbo_flow_content_descriptor_t content;
  size_t slot_capacity;
  size_t max_message_bytes;
  size_t active_connections;
  size_t receive_cursor;
  size_t scheduler_max_steps_per_poll;
  size_t pending_slot;
  uint16_t bound_port;
  uint64_t next_message_id;
  uint64_t connections_accepted;
  uint64_t connections_closed;
  uint64_t connections_failed;
  uint64_t messages_received;
  uint64_t bytes_received;
  bool listener_initialized;
  bool listener_closed;
  bool client_initialized;
  bool tls_server_initialized;
  bool tls_enabled;
  bool scheduler_initialized;
  bool ready;
  bool receive_pending;
  bool publisher_destroyed;
  bool publisher_cancelled;
  bool has_content;
  bool message_id_exhausted;
};

static bool listener_source_connection_equal(cnet_connection lhs, cnet_connection rhs) {
  return lhs.slot == rhs.slot && lhs.generation == rhs.generation;
}

static void listener_source_wake(cflow_waker *waker) {
  cflow_waker pending;
  if (!waker || !waker->wake) return;
  pending = *waker;
  *waker = (cflow_waker){0};
  pending.wake(pending.user);
}

static void listener_source_copy_stage(char *destination, size_t capacity, const char *stage) {
  if (!destination || capacity == 0u) return;
  (void)snprintf(destination, capacity, "%s", stage ? stage : "cnet-listener");
}

static void listener_source_close_connections(turbo_flow_cnet_listener_source_t *source) {
  size_t index;
  if (!source || !source->client_initialized) return;
  for (index = 0u; index < source->slot_capacity; ++index) {
    listener_source_slot_t *slot = &source->slots[index];
    if (!slot->occupied || slot->closing) continue;
    slot->closing = true;
    (void)cnet_close(&source->client, slot->connection);
  }
}

static void listener_source_fail(turbo_flow_cnet_listener_source_t *source, int status,
                                 int native_status, const char *stage) {
  if (!source || source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_FAILED ||
      source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPED)
    return;
  source->state = TURBO_FLOW_CNET_LISTENER_SOURCE_FAILED;
  source->status = status == SALTS_OK ? SALTS_EIO : status;
  source->native_status = native_status;
  listener_source_copy_stage(source->error_stage, sizeof(source->error_stage), stage);
  (void)snprintf(source->publisher_error, sizeof(source->publisher_error),
                 "CNet listener source failed at %s: %s (%d, native=%d)", source->error_stage,
                 salts_strerror(source->status), source->status, source->native_status);
  if (source->listener_initialized && !source->listener_closed) {
    (void)cnet_listener_close(&source->listener);
    source->listener_closed = true;
  }
  listener_source_close_connections(source);
  listener_source_wake(&source->value_waker);
  listener_source_wake(&source->terminal_waker);
}

static bool listener_source_waitable_arm(void *state, cflow_waker waker) {
  turbo_flow_cnet_listener_source_t *source = (turbo_flow_cnet_listener_source_t *)state;
  if (!source || source->publisher_destroyed) return false;
  source->value_waker = waker;
  return true;
}

static void listener_source_waitable_cancel(void *state) {
  turbo_flow_cnet_listener_source_t *source = (turbo_flow_cnet_listener_source_t *)state;
  if (source) source->value_waker = (cflow_waker){0};
}

CMETA_IMPLEMENTS(cflow_waitable, listener_source_waitable, 0, .arm = listener_source_waitable_arm,
                 .cancel = listener_source_waitable_cancel);

static const char *listener_source_publisher_name(void *state) {
  turbo_flow_cnet_listener_source_t *source = (turbo_flow_cnet_listener_source_t *)state;
  return source && source->source_name ? source->source_name : "cnet-listener-source";
}

static const cmeta_type_desc *listener_source_publisher_type(void *state) {
  (void)state;
  return turbo_flow_message_type();
}

static listener_source_slot_t *
listener_source_select_receive_slot(turbo_flow_cnet_listener_source_t *source, size_t *slot_index) {
  size_t offset;
  if (!source || source->slot_capacity == 0u) return NULL;
  for (offset = 0u; offset < source->slot_capacity; ++offset) {
    const size_t index = (source->receive_cursor + offset) % source->slot_capacity;
    listener_source_slot_t *slot = &source->slots[index];
    if (slot->occupied && slot->connected && !slot->closing && !slot->receive_pending) {
      source->receive_cursor = (index + 1u) % source->slot_capacity;
      if (slot_index) *slot_index = index;
      return slot;
    }
  }
  return NULL;
}

static cflow_step listener_source_publisher_resume(void *state, cflow_publish_context *context,
                                                   void *out_value) {
  turbo_flow_cnet_listener_source_t *source = (turbo_flow_cnet_listener_source_t *)state;
  listener_source_slot_t *slot;
  size_t slot_index = 0u;
  int status;
  (void)context;
  if (!source || !out_value)
    return (cflow_step){CFLOW_STEP_ERROR, {0}, "invalid CNet listener source"};
  if (source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_FAILED)
    return (cflow_step){CFLOW_STEP_ERROR, {0}, source->publisher_error};
  if (source->publisher_cancelled || source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPED)
    return (cflow_step){CFLOW_STEP_DONE, {0}, NULL};
  if (source->ready) {
    status = turbo_flow_msg_move((turbo_flow_msg_t *)out_value, &source->ready_message);
    if (status != SALTS_OK) {
      listener_source_fail(source, status, 0, "message_move");
      return (cflow_step){CFLOW_STEP_ERROR, {0}, source->publisher_error};
    }
    source->ready = false;
    return (cflow_step){CFLOW_STEP_VALUE, {0}, NULL};
  }
  if (source->message_id_exhausted) {
    listener_source_fail(source, SALTS_ERANGE, 0, "message_id");
    return (cflow_step){CFLOW_STEP_ERROR, {0}, source->publisher_error};
  }
  if (!source->receive_pending) {
    slot = listener_source_select_receive_slot(source, &slot_index);
    if (slot) {
      status = cnet_receive(&source->client, slot->connection, 1u);
      if (status != SALTS_OK) {
        listener_source_fail(source, status, 0, "receive_admission");
        return (cflow_step){CFLOW_STEP_ERROR, {0}, source->publisher_error};
      }
      slot->receive_pending = true;
      source->receive_pending = true;
      source->pending_slot = slot_index;
    }
  }
  return (cflow_step){CFLOW_STEP_WAIT, listener_source_waitable_as_cflow_waitable(source), NULL};
}

static void listener_source_publisher_cancel(void *state) {
  turbo_flow_cnet_listener_source_t *source = (turbo_flow_cnet_listener_source_t *)state;
  if (!source) return;
  source->publisher_cancelled = true;
  source->value_waker = (cflow_waker){0};
  source->terminal_waker = (cflow_waker){0};
}

static void listener_source_publisher_destroy(void *state) {
  turbo_flow_cnet_listener_source_t *source = (turbo_flow_cnet_listener_source_t *)state;
  if (!source) return;
  source->publisher_destroyed = true;
  source->value_waker = (cflow_waker){0};
  source->terminal_waker = (cflow_waker){0};
}

static void listener_source_publisher_bind_terminal(void *state, cflow_waker waker) {
  turbo_flow_cnet_listener_source_t *source = (turbo_flow_cnet_listener_source_t *)state;
  if (source && !source->publisher_destroyed) source->terminal_waker = waker;
}

static cflow_publisher_terminal listener_source_publisher_poll_terminal(void *state,
                                                                        const char **error) {
  turbo_flow_cnet_listener_source_t *source = (turbo_flow_cnet_listener_source_t *)state;
  if (error) *error = NULL;
  if (!source) {
    if (error) *error = "invalid CNet listener source";
    return CFLOW_PUBLISHER_ERROR;
  }
  if (source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_FAILED) {
    if (error) *error = source->publisher_error;
    return CFLOW_PUBLISHER_ERROR;
  }
  if (source->publisher_cancelled || source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPED)
    return CFLOW_PUBLISHER_DONE;
  return CFLOW_PUBLISHER_OPEN;
}

CMETA_IMPLEMENTS(cflow_publisher, listener_source_publisher, CFLOW_PUBLISHER_CAP_CONSTRUCTS_VALUES,
                 .name = listener_source_publisher_name,
                 .output_type = listener_source_publisher_type,
                 .resume = listener_source_publisher_resume,
                 .cancel = listener_source_publisher_cancel,
                 .destroy = listener_source_publisher_destroy,
                 .bind_terminal_waker = listener_source_publisher_bind_terminal,
                 .poll_terminal = listener_source_publisher_poll_terminal);

static void listener_source_clear_pending(turbo_flow_cnet_listener_source_t *source,
                                          listener_source_slot_t *slot) {
  if (!source || !slot) return;
  slot->receive_pending = false;
  if (source->receive_pending && source->pending_slot == (size_t)(slot - source->slots))
    source->receive_pending = false;
}

static void listener_source_retire_slot(turbo_flow_cnet_listener_source_t *source,
                                        listener_source_slot_t *slot) {
  if (!source || !slot || !slot->occupied) return;
  listener_source_clear_pending(source, slot);
  memset(&slot->connection, 0, sizeof(slot->connection));
  slot->occupied = false;
  slot->connected = false;
  slot->closing = false;
  if (source->active_connections > 0u) --source->active_connections;
  listener_source_wake(&source->value_waker);
}

static void listener_source_on_state(void *user, cnet_connection connection,
                                     cnet_connection_state state, const cnet_error *error) {
  listener_source_slot_t *slot = (listener_source_slot_t *)user;
  turbo_flow_cnet_listener_source_t *source = slot ? slot->owner : NULL;
  if (!source || !slot->occupied || !listener_source_connection_equal(connection, slot->connection))
    return;
  switch (state) {
  case CNET_CONNECTION_CONNECTED:
    slot->connected = true;
    listener_source_wake(&source->value_waker);
    break;
  case CNET_CONNECTION_CLOSING:
    slot->closing = true;
    break;
  case CNET_CONNECTION_CLOSED:
    if (source->connections_closed == UINT64_MAX)
      listener_source_fail(source, SALTS_ERANGE, 0, "connection_counters");
    else ++source->connections_closed;
    listener_source_retire_slot(source, slot);
    break;
  case CNET_CONNECTION_FAILED:
    if (source->connections_failed == UINT64_MAX) {
      listener_source_fail(source, SALTS_ERANGE, 0, "connection_counters");
    } else {
      ++source->connections_failed;
      if (source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_LISTENING) {
        source->last_connection_status = error ? error->status : SALTS_EIO;
        source->last_connection_native_status = error ? error->native_status : 0;
        listener_source_copy_stage(source->last_connection_error_stage,
                                   sizeof(source->last_connection_error_stage),
                                   error ? error->stage : "connection");
      }
    }
    listener_source_retire_slot(source, slot);
    break;
  case CNET_CONNECTION_CONNECTING:
    break;
  }
}

static void listener_source_on_receive(void *user, cnet_connection connection,
                                       const cnet_receive_view *view) {
  listener_source_slot_t *slot = (listener_source_slot_t *)user;
  turbo_flow_cnet_listener_source_t *source = slot ? slot->owner : NULL;
  mem_buffer_t *buffer;
  int status;
  if (!source || !slot->occupied || !listener_source_connection_equal(connection, slot->connection))
    return;
  listener_source_clear_pending(source, slot);
  if (!view || view->kind != CNET_MESSAGE_BYTES || !view->data || view->size == 0u) {
    listener_source_fail(source, SALTS_EPROTO, 0, "receive_view");
    return;
  }
  if (source->ready) {
    listener_source_fail(source, SALTS_EBUSY, 0, "receive_ownership");
    return;
  }
  if (view->size > source->max_message_bytes) {
    listener_source_fail(source, SALTS_EMSGSIZE, 0, "receive_size");
    return;
  }
  if (source->bytes_received > UINT64_MAX - (uint64_t)view->size ||
      source->messages_received == UINT64_MAX) {
    listener_source_fail(source, SALTS_ERANGE, 0, "receive_counters");
    return;
  }
  buffer = mem_get_buffer(mem_global(), view->size);
  if (!buffer) {
    listener_source_fail(source, SALTS_ENOMEM, 0, "receive_copy");
    return;
  }
  memcpy(mem_buffer_data(buffer), view->data, view->size);
  mem_set_used(buffer, view->size);
  turbo_flow_msg_init(&source->ready_message);
  source->ready_message.id = source->next_message_id;
  source->ready_message.buffer = buffer;
  source->ready_message.payload = vstr_from_buf(mem_buffer_const_data(buffer), view->size);
  if (source->has_content) {
    status = turbo_flow_msg_copy_content_descriptor(&source->ready_message, &source->content);
    if (status != SALTS_OK) {
      turbo_flow_msg_cleanup(&source->ready_message);
      listener_source_fail(source, status, 0, "content_descriptor");
      return;
    }
  }
  source->message_id_exhausted = source->next_message_id == UINT64_MAX;
  if (!source->message_id_exhausted) ++source->next_message_id;
  ++source->messages_received;
  source->bytes_received += (uint64_t)view->size;
  source->ready = true;
  listener_source_wake(&source->value_waker);
}

static int listener_source_config_validate(const turbo_flow_cnet_listener_source_config_t *config) {
  int status;
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION || !config->flow ||
      !config->source_name || config->source_name[0] == '\0' || !config->listener ||
      !config->client || config->max_connections == 0u || config->max_connections > UINT32_MAX ||
      config->max_connections > SIZE_MAX / sizeof(listener_source_slot_t) ||
      config->max_connections > config->client->connection_capacity ||
      config->max_message_bytes == 0u ||
      config->max_message_bytes > config->client->receive_buffer_bytes ||
      config->scheduler_capacity == 0u || config->scheduler_max_steps_per_poll == 0u ||
      config->first_message_id == 0u || config->listener->backend != config->client->backend)
    return SALTS_EINVAL;
  if (turbo_flow_state(config->flow) != TURBO_FLOW_STATE_STARTED) return SALTS_EBUSY;
  if (config->listener_options) {
    status = cnet_listener_options_validate(config->listener_options);
    if (status != SALTS_OK) return status;
  }
  if (config->socket_options) {
    status = cnet_stream_socket_options_validate(config->socket_options);
    if (status != SALTS_OK) return status;
  }
  if (config->content) {
    status = turbo_flow_content_descriptor_check(config->content);
    if (status != SALTS_OK) return status;
  }
  if (config->tls && (config->client->tls_io_buffer_bytes < CNET_TLS_MIN_IO_BUFFER_BYTES ||
                      config->client->tls_handshake_timeout_ms == 0u))
    return SALTS_ENOTSUP;
  return SALTS_OK;
}

static void listener_source_open_cleanup(turbo_flow_cnet_listener_source_t *source,
                                         cflow_publisher *publisher) {
  if (!source) return;
  if (source->run) {
    turbo_flow_run_close(source->run);
    source->run = NULL;
  } else if (publisher && cflow_publisher_valid(publisher)) {
    cflow_publisher_destroy(publisher);
  }
  if (source->listener_initialized) {
    if (!source->listener_closed) (void)cnet_listener_close(&source->listener);
    (void)cnet_listener_destroy(&source->listener);
  }
  if (source->client_initialized) {
    (void)cnet_client_stop(&source->client, 0u);
    (void)cnet_client_destroy(&source->client);
  }
  if (source->tls_server_initialized) (void)cnet_tls_server_destroy(&source->tls_server);
  if (source->scheduler_initialized) cflow_scheduler_destroy(&source->scheduler);
  turbo_flow_msg_cleanup(&source->ready_message);
  free(source->slots);
  tstr_free(source->source_name);
  free(source);
}

int turbo_flow_cnet_listener_source_open(const turbo_flow_cnet_listener_source_config_t *config,
                                         turbo_flow_cnet_listener_source_t **source_out) {
  turbo_flow_cnet_listener_source_t *source;
  turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
  cflow_publisher publisher = {0};
  size_t index;
  int status;
  if (!source_out) return SALTS_EINVAL;
  *source_out = NULL;
  status = listener_source_config_validate(config);
  if (status != SALTS_OK) return status;
  source = (turbo_flow_cnet_listener_source_t *)calloc(1u, sizeof(*source));
  if (!source) return SALTS_ENOMEM;
  source->state = TURBO_FLOW_CNET_LISTENER_SOURCE_NEW;
  source->status = SALTS_OK;
  source->source_name = tstr_dup(config->source_name);
  source->slots = (listener_source_slot_t *)calloc(config->max_connections, sizeof(*source->slots));
  source->slot_capacity = config->max_connections;
  source->max_message_bytes = config->max_message_bytes;
  source->scheduler_max_steps_per_poll = config->scheduler_max_steps_per_poll;
  source->next_message_id = config->first_message_id;
  source->tls_enabled = config->tls != NULL;
  turbo_flow_msg_init(&source->ready_message);
  if (!source->source_name || !source->slots) {
    listener_source_open_cleanup(source, &publisher);
    return SALTS_ENOMEM;
  }
  for (index = 0u; index < source->slot_capacity; ++index)
    source->slots[index].owner = source;
  if (config->content) {
    source->content = *config->content;
    source->has_content = true;
  }
  if (!cflow_scheduler_manual_init_with_capacity(&source->scheduler, config->scheduler_capacity)) {
    listener_source_open_cleanup(source, &publisher);
    return SALTS_ENOMEM;
  }
  source->scheduler_initialized = true;
  status = cnet_client_init(&source->client, config->client);
  if (status != SALTS_OK) {
    listener_source_open_cleanup(source, &publisher);
    return status;
  }
  source->client_initialized = true;
  if (config->socket_options) {
    status = cnet_client_set_stream_socket_options(&source->client, config->socket_options);
    if (status != SALTS_OK) {
      listener_source_open_cleanup(source, &publisher);
      return status;
    }
  }
  if (config->tls) {
    status = cnet_tls_server_init(&source->tls_server, config->tls);
    if (status != SALTS_OK) {
      listener_source_open_cleanup(source, &publisher);
      return status;
    }
    source->tls_server_initialized = true;
  }
  status = config->listener_options ? cnet_listener_init_ex(&source->listener, config->listener,
                                                            config->listener_options)
                                    : cnet_listener_init(&source->listener, config->listener);
  if (status != SALTS_OK) {
    listener_source_open_cleanup(source, &publisher);
    return status;
  }
  source->listener_initialized = true;
  status = cnet_listener_port(&source->listener, &source->bound_port);
  if (status != SALTS_OK) {
    listener_source_open_cleanup(source, &publisher);
    return status;
  }
  publisher = listener_source_publisher_as_cflow_publisher(source);
  run_config.scheduler = &source->scheduler;
  status =
      turbo_flow_run_open(config->flow, config->source_name, &publisher, &run_config, &source->run);
  if (status != SALTS_OK) {
    listener_source_open_cleanup(source, &publisher);
    return status;
  }
  source->state = TURBO_FLOW_CNET_LISTENER_SOURCE_LISTENING;
  *source_out = source;
  return SALTS_OK;
}

static listener_source_slot_t *
listener_source_find_free_slot(turbo_flow_cnet_listener_source_t *source) {
  size_t index;
  if (!source) return NULL;
  for (index = 0u; index < source->slot_capacity; ++index)
    if (!source->slots[index].occupied) return &source->slots[index];
  return NULL;
}

static int listener_source_accept_available(turbo_flow_cnet_listener_source_t *source,
                                            uint32_t initial_timeout_ms) {
  uint32_t timeout_ms = initial_timeout_ms;
  while (source->active_connections < source->slot_capacity) {
    listener_source_slot_t *slot;
    cnet_observer observer;
    cnet_connection connection = {0};
    int ready = 0;
    int status = cnet_listener_wait(&source->listener, timeout_ms, &ready);
    timeout_ms = 0u;
    if (status != SALTS_OK) return status;
    if (!ready) return SALTS_OK;
    slot = listener_source_find_free_slot(source);
    if (!slot) return SALTS_EPROTO;
    observer = (cnet_observer){.on_state = listener_source_on_state,
                               .on_receive = listener_source_on_receive,
                               .user = slot};
    status = source->tls_enabled
                 ? cnet_listener_accept_tls(&source->listener, &source->client, &source->tls_server,
                                            &observer, &connection)
                 : cnet_listener_accept(&source->listener, &source->client, &observer, &connection);
    if (status == SALTS_ETIMEDOUT) return SALTS_OK;
    if (status != SALTS_OK) return status;
    slot->connection = connection;
    slot->occupied = true;
    slot->connected = false;
    slot->closing = false;
    slot->receive_pending = false;
    ++source->active_connections;
    if (source->connections_accepted == UINT64_MAX) return SALTS_ERANGE;
    ++source->connections_accepted;
  }
  return SALTS_OK;
}

static void listener_source_refresh_run(turbo_flow_cnet_listener_source_t *source) {
  turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
  if (!source || !source->run || source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPED)
    return;
  if (turbo_flow_run_snapshot(source->run, &result) != SALTS_OK) return;
  if (result.state == TURBO_FLOW_RUN_FAILED || result.state == TURBO_FLOW_RUN_CANCELED)
    listener_source_fail(source, result.status, 0, "graph_run");
}

int turbo_flow_cnet_listener_source_request(turbo_flow_cnet_listener_source_t *source,
                                            size_t demand) {
  int status;
  if (!source || demand == 0u) return SALTS_EINVAL;
  if (source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_FAILED) return source->status;
  if (source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPED || !source->run)
    return SALTS_ESHUTDOWN;
  status = turbo_flow_run_request(source->run, demand);
  listener_source_refresh_run(source);
  return source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_FAILED ? source->status : status;
}

int turbo_flow_cnet_listener_source_snapshot(const turbo_flow_cnet_listener_source_t *source,
                                             turbo_flow_cnet_listener_source_snapshot_t *snapshot) {
  turbo_flow_cnet_listener_source_snapshot_t current =
      TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
  turbo_flow_run_result_t run_result = TURBO_FLOW_RUN_RESULT_INIT;
  if (!source || !snapshot || snapshot->size < sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION)
    return SALTS_EINVAL;
  current.state = source->state;
  current.status = source->status;
  current.native_status = source->native_status;
  current.bound_port = source->bound_port;
  current.connections_accepted = source->connections_accepted;
  current.active_connections = source->active_connections;
  current.connections_closed = source->connections_closed;
  current.connections_failed = source->connections_failed;
  current.messages_received = source->messages_received;
  current.bytes_received = source->bytes_received;
  current.receive_pending = source->receive_pending ? 1 : 0;
  current.last_connection_status = source->last_connection_status;
  current.last_connection_native_status = source->last_connection_native_status;
  listener_source_copy_stage(
      current.last_connection_error_stage, sizeof(current.last_connection_error_stage),
      source->last_connection_error_stage[0] ? source->last_connection_error_stage : "");
  listener_source_copy_stage(current.error_stage, sizeof(current.error_stage),
                             source->error_stage[0] ? source->error_stage : "");
  if (source->run && turbo_flow_run_snapshot(source->run, &run_result) == SALTS_OK)
    current.outstanding_demand = run_result.outstanding_demand;
  *snapshot = current;
  return SALTS_OK;
}

int turbo_flow_cnet_listener_source_poll(turbo_flow_cnet_listener_source_t *source,
                                         uint32_t timeout_ms,
                                         turbo_flow_cnet_listener_source_snapshot_t *snapshot) {
  size_t events = 0u;
  bool waited_on_listener;
  int status = SALTS_OK;
  if (!source || (snapshot && (snapshot->size < sizeof(*snapshot) ||
                               snapshot->version != TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION)))
    return SALTS_EINVAL;
  if (source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPED)
    return SALTS_ESHUTDOWN;
  if (source->scheduler_initialized) {
    (void)cflow_scheduler_run_until_idle(&source->scheduler, source->scheduler_max_steps_per_poll);
    listener_source_refresh_run(source);
  }
  waited_on_listener = source->active_connections == 0u;
  if (source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_LISTENING) {
    status = listener_source_accept_available(source, waited_on_listener ? timeout_ms : 0u);
    if (status != SALTS_OK) listener_source_fail(source, status, 0, "listener_accept");
  }
  if (source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_LISTENING) {
    status = cnet_client_poll(&source->client, waited_on_listener ? 0u : timeout_ms, &events);
    if (status != SALTS_OK) listener_source_fail(source, status, 0, "client_poll");
  }
  if (source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_LISTENING) {
    status = listener_source_accept_available(source, 0u);
    if (status != SALTS_OK) listener_source_fail(source, status, 0, "listener_accept");
  }
  if (source->scheduler_initialized) {
    (void)cflow_scheduler_run_until_idle(&source->scheduler, source->scheduler_max_steps_per_poll);
    listener_source_refresh_run(source);
  }
  if (snapshot) (void)turbo_flow_cnet_listener_source_snapshot(source, snapshot);
  return source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_FAILED ? source->status : status;
}

int turbo_flow_cnet_listener_source_stop(turbo_flow_cnet_listener_source_t *source,
                                         uint32_t timeout_ms) {
  int stop_status = SALTS_OK;
  int destroy_status;
  if (!source) return SALTS_EINVAL;
  if (source->state == TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPED) return SALTS_EALREADY;
  source->state = TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPING;
  if (source->listener_initialized && !source->listener_closed) {
    stop_status = cnet_listener_close(&source->listener);
    if (stop_status == SALTS_OK || stop_status == SALTS_EALREADY) {
      source->listener_closed = true;
      stop_status = SALTS_OK;
    }
  }
  if (source->run) {
    turbo_flow_run_close(source->run);
    source->run = NULL;
  }
  listener_source_close_connections(source);
  if (source->client_initialized) {
    int client_stop_status = cnet_client_stop(&source->client, timeout_ms);
    if (client_stop_status == SALTS_ETIMEDOUT) return client_stop_status;
    if (client_stop_status == SALTS_EALREADY) client_stop_status = SALTS_OK;
    if (stop_status == SALTS_OK) stop_status = client_stop_status;
    destroy_status = cnet_client_destroy(&source->client);
    if (destroy_status != SALTS_OK) return stop_status == SALTS_OK ? destroy_status : stop_status;
    source->client_initialized = false;
  }
  if (source->listener_initialized) {
    destroy_status = cnet_listener_destroy(&source->listener);
    if (destroy_status != SALTS_OK) return stop_status == SALTS_OK ? destroy_status : stop_status;
    source->listener_initialized = false;
  }
  if (source->tls_server_initialized) {
    destroy_status = cnet_tls_server_destroy(&source->tls_server);
    if (destroy_status != SALTS_OK) return stop_status == SALTS_OK ? destroy_status : stop_status;
    source->tls_server_initialized = false;
  }
  if (source->scheduler_initialized) {
    (void)cflow_scheduler_run_until_idle(&source->scheduler, 0u);
    (void)cflow_scheduler_shutdown(&source->scheduler);
    cflow_scheduler_destroy(&source->scheduler);
    source->scheduler_initialized = false;
  }
  if (source->ready) {
    turbo_flow_msg_cleanup(&source->ready_message);
    source->ready = false;
  }
  source->receive_pending = false;
  source->active_connections = 0u;
  source->state = TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPED;
  return stop_status;
}

int turbo_flow_cnet_listener_source_destroy(turbo_flow_cnet_listener_source_t *source) {
  if (!source) return SALTS_EINVAL;
  if (source->state != TURBO_FLOW_CNET_LISTENER_SOURCE_STOPPED) return SALTS_EBUSY;
  turbo_flow_msg_cleanup(&source->ready_message);
  free(source->slots);
  tstr_free(source->source_name);
  free(source);
  return SALTS_OK;
}
