#include "turbo_flow_cnet.h"

#include <cflow/scheduler.h>
#include <cstl/queue.h>
#include <salts_error.h>
#include <tstr.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { PACKET_SOURCE_OPEN_CLEANUP_TIMEOUT_MS = 1000 };

struct turbo_flow_cnet_packet_source_s {
  turbo_flow_cnet_packet_source_state_t state;
  int status;
  char error_stage[TURBO_FLOW_CNET_PACKET_SOURCE_ERROR_STAGE_CAPACITY];
  char publisher_error[192];
  tstr source_name;
  cnet_packet_endpoint endpoint;
  cflow_scheduler scheduler;
  queue_t messages;
  turbo_flow_run_t *run;
  cflow_waker value_waker;
  cflow_waker terminal_waker;
  turbo_flow_content_descriptor_t content;
  cnet_packet_protocol protocol;
  cnet_packet_session last_error_session;
  size_t queue_capacity;
  size_t max_message_bytes;
  size_t scheduler_max_steps_per_poll;
  uint16_t bound_port;
  uint64_t next_message_id;
  uint64_t sessions_admitted;
  uint64_t sessions_opened;
  uint64_t sessions_closed;
  uint64_t messages_received;
  uint64_t bytes_received;
  bool endpoint_initialized;
  bool scheduler_initialized;
  bool queue_initialized;
  bool publisher_destroyed;
  bool managed_run;
  bool publisher_cancelled;
  bool has_content;
  bool message_id_exhausted;
};

static int packet_source_stl_status(stl_status status) {
  if (status == STL_OK) return SALTS_OK;
  if (status == STL_OUT_OF_MEMORY) return SALTS_ENOMEM;
  if (status == STL_CAPACITY_EXCEEDED) return SALTS_ENOBUFS;
  if (status == STL_EMPTY) return SALTS_ENOENT;
  return SALTS_EINVAL;
}

static void packet_source_wake(cflow_waker *waker) {
  cflow_waker pending;
  if (!waker || !waker->wake) return;
  pending = *waker;
  *waker = (cflow_waker){0};
  pending.wake(pending.user);
}

static void packet_source_copy_stage(char *destination, size_t capacity, const char *stage) {
  if (!destination || capacity == 0u) return;
  (void)snprintf(destination, capacity, "%s", stage ? stage : "cnet-packet");
}

static void packet_source_fail(turbo_flow_cnet_packet_source_t *source, int status,
                               cnet_packet_session session, const char *stage) {
  if (!source || source->state == TURBO_FLOW_CNET_PACKET_SOURCE_FAILED ||
      source->state == TURBO_FLOW_CNET_PACKET_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_PACKET_SOURCE_STOPPED)
    return;
  source->state = TURBO_FLOW_CNET_PACKET_SOURCE_FAILED;
  source->status = status == SALTS_OK ? SALTS_EIO : status;
  source->last_error_session = session;
  packet_source_copy_stage(source->error_stage, sizeof(source->error_stage), stage);
  (void)snprintf(source->publisher_error, sizeof(source->publisher_error),
                 "CNet packet source failed at %s: %s (%d)", source->error_stage,
                 salts_strerror(source->status), source->status);
  packet_source_wake(&source->value_waker);
  packet_source_wake(&source->terminal_waker);
}

static bool packet_source_waitable_arm(void *state, cflow_waker waker) {
  turbo_flow_cnet_packet_source_t *source = (turbo_flow_cnet_packet_source_t *)state;
  if (!source || source->publisher_destroyed) return false;
  source->value_waker = waker;
  return true;
}

static void packet_source_waitable_cancel(void *state) {
  turbo_flow_cnet_packet_source_t *source = (turbo_flow_cnet_packet_source_t *)state;
  if (source) source->value_waker = (cflow_waker){0};
}

CMETA_IMPLEMENTS(cflow_waitable, packet_source_waitable, 0, .arm = packet_source_waitable_arm,
                 .cancel = packet_source_waitable_cancel);

static const char *packet_source_publisher_name(void *state) {
  turbo_flow_cnet_packet_source_t *source = (turbo_flow_cnet_packet_source_t *)state;
  return source && source->source_name ? source->source_name : "cnet-packet-source";
}

static const cmeta_type_desc *packet_source_publisher_type(void *state) {
  (void)state;
  return turbo_flow_message_type();
}

static cflow_step packet_source_publisher_resume(void *state, cflow_publish_context *context,
                                                 void *out_value) {
  turbo_flow_cnet_packet_source_t *source = (turbo_flow_cnet_packet_source_t *)state;
  turbo_flow_msg_t message;
  int status;
  (void)context;
  if (!source || !out_value)
    return (cflow_step){CFLOW_STEP_ERROR, {0}, "invalid CNet packet source"};
  if (source->state == TURBO_FLOW_CNET_PACKET_SOURCE_FAILED)
    return (cflow_step){CFLOW_STEP_ERROR, {0}, source->publisher_error};
  if (source->publisher_cancelled || source->state == TURBO_FLOW_CNET_PACKET_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_PACKET_SOURCE_STOPPED)
    return (cflow_step){CFLOW_STEP_DONE, {0}, NULL};
  if (!queue_empty(&source->messages)) {
    turbo_flow_msg_init(&message);
    status = packet_source_stl_status(queue_pop(&source->messages, &message));
    if (status != SALTS_OK) {
      packet_source_fail(source, status, (cnet_packet_session){0}, "queue_pop");
      return (cflow_step){CFLOW_STEP_ERROR, {0}, source->publisher_error};
    }
    status = turbo_flow_msg_move((turbo_flow_msg_t *)out_value, &message);
    if (status != SALTS_OK) {
      turbo_flow_msg_cleanup(&message);
      packet_source_fail(source, status, (cnet_packet_session){0}, "message_move");
      return (cflow_step){CFLOW_STEP_ERROR, {0}, source->publisher_error};
    }
    return (cflow_step){CFLOW_STEP_VALUE, {0}, NULL};
  }
  if (source->message_id_exhausted) {
    packet_source_fail(source, SALTS_ERANGE, (cnet_packet_session){0}, "message_id");
    return (cflow_step){CFLOW_STEP_ERROR, {0}, source->publisher_error};
  }
  return (cflow_step){CFLOW_STEP_WAIT, packet_source_waitable_as_cflow_waitable(source), NULL};
}

static void packet_source_publisher_cancel(void *state) {
  turbo_flow_cnet_packet_source_t *source = (turbo_flow_cnet_packet_source_t *)state;
  if (!source) return;
  source->publisher_cancelled = true;
  source->value_waker = (cflow_waker){0};
  source->terminal_waker = (cflow_waker){0};
}

static void packet_source_publisher_destroy(void *state) {
  turbo_flow_cnet_packet_source_t *source = (turbo_flow_cnet_packet_source_t *)state;
  if (!source) return;
  source->publisher_destroyed = true;
  source->value_waker = (cflow_waker){0};
  source->terminal_waker = (cflow_waker){0};
}

static void packet_source_publisher_bind_terminal(void *state, cflow_waker waker) {
  turbo_flow_cnet_packet_source_t *source = (turbo_flow_cnet_packet_source_t *)state;
  if (source && !source->publisher_destroyed) source->terminal_waker = waker;
}

static cflow_publisher_terminal packet_source_publisher_poll_terminal(void *state,
                                                                      const char **error) {
  turbo_flow_cnet_packet_source_t *source = (turbo_flow_cnet_packet_source_t *)state;
  if (error) *error = NULL;
  if (!source) {
    if (error) *error = "invalid CNet packet source";
    return CFLOW_PUBLISHER_ERROR;
  }
  if (source->state == TURBO_FLOW_CNET_PACKET_SOURCE_FAILED) {
    if (error) *error = source->publisher_error;
    return CFLOW_PUBLISHER_ERROR;
  }
  if (source->publisher_cancelled || source->state == TURBO_FLOW_CNET_PACKET_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_PACKET_SOURCE_STOPPED)
    return CFLOW_PUBLISHER_DONE;
  return CFLOW_PUBLISHER_OPEN;
}

CMETA_IMPLEMENTS(cflow_publisher, packet_source_publisher, CFLOW_PUBLISHER_CAP_CONSTRUCTS_VALUES,
                 .name = packet_source_publisher_name, .output_type = packet_source_publisher_type,
                 .resume = packet_source_publisher_resume, .cancel = packet_source_publisher_cancel,
                 .destroy = packet_source_publisher_destroy,
                 .bind_terminal_waker = packet_source_publisher_bind_terminal,
                 .poll_terminal = packet_source_publisher_poll_terminal);

static int packet_source_on_admit(void *user, cnet_packet_endpoint *endpoint,
                                  cnet_packet_protocol protocol, const cnet_datagram_peer *peer,
                                  uint32_t conversation) {
  turbo_flow_cnet_packet_source_t *source = (turbo_flow_cnet_packet_source_t *)user;
  (void)endpoint;
  (void)protocol;
  (void)peer;
  (void)conversation;
  if (!source || source->state != TURBO_FLOW_CNET_PACKET_SOURCE_RUNNING) return SALTS_ESHUTDOWN;
  if (source->sessions_admitted == UINT64_MAX) {
    packet_source_fail(source, SALTS_ERANGE, (cnet_packet_session){0}, "session_counters");
    return SALTS_ERANGE;
  }
  ++source->sessions_admitted;
  return SALTS_OK;
}

static void packet_source_on_state(void *user, cnet_packet_endpoint *endpoint,
                                   cnet_packet_session session, cnet_packet_session_state state,
                                   const cnet_datagram_peer *peer, uint32_t conversation) {
  turbo_flow_cnet_packet_source_t *source = (turbo_flow_cnet_packet_source_t *)user;
  uint64_t *counter = NULL;
  (void)endpoint;
  (void)peer;
  (void)conversation;
  if (!source) return;
  if (state == CNET_PACKET_SESSION_OPEN) counter = &source->sessions_opened;
  else if (state == CNET_PACKET_SESSION_CLOSED) counter = &source->sessions_closed;
  if (!counter) return;
  if (*counter == UINT64_MAX) {
    packet_source_fail(source, SALTS_ERANGE, session, "session_counters");
    return;
  }
  ++*counter;
}

static void packet_source_on_receive(void *user, cnet_packet_endpoint *endpoint,
                                     cnet_packet_session session, const cnet_receive_view *view) {
  turbo_flow_cnet_packet_source_t *source = (turbo_flow_cnet_packet_source_t *)user;
  turbo_flow_cnet_packet_message_context_t *packet_context;
  turbo_flow_msg_t message;
  mem_buffer_t *buffer;
  size_t buffer_size;
  int status;
  if (!source || source->state != TURBO_FLOW_CNET_PACKET_SOURCE_RUNNING) return;
  if (!view || !view->data || view->size == 0u ||
      (source->protocol == CNET_PACKET_UDP && view->kind != CNET_MESSAGE_DATAGRAM) ||
      (source->protocol == CNET_PACKET_KCP && view->kind != CNET_MESSAGE_BYTES)) {
    packet_source_fail(source, SALTS_EPROTO, session, "receive_view");
    return;
  }
  if (queue_size(&source->messages) >= source->queue_capacity) {
    packet_source_fail(source, SALTS_ENOBUFS, session, "message_queue");
    return;
  }
  if (view->size > source->max_message_bytes) {
    packet_source_fail(source, SALTS_EMSGSIZE, session, "receive_size");
    return;
  }
  if (source->message_id_exhausted) {
    packet_source_fail(source, SALTS_ERANGE, session, "message_id");
    return;
  }
  if (source->messages_received == UINT64_MAX ||
      source->bytes_received > UINT64_MAX - (uint64_t)view->size) {
    packet_source_fail(source, SALTS_ERANGE, session, "receive_counters");
    return;
  }
  if (view->size > SIZE_MAX - sizeof(*packet_context)) {
    packet_source_fail(source, SALTS_ERANGE, session, "receive_size");
    return;
  }
  buffer_size = sizeof(*packet_context) + view->size;
  buffer = mem_get_buffer(mem_global(), buffer_size);
  if (!buffer) {
    packet_source_fail(source, SALTS_ENOMEM, session, "receive_copy");
    return;
  }
  packet_context = (turbo_flow_cnet_packet_message_context_t *)mem_buffer_data(buffer);
  memset(packet_context, 0, sizeof(*packet_context));
  packet_context->size = TURBO_FLOW_CNET_PACKET_MESSAGE_CONTEXT_V1_SIZE;
  packet_context->version = TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION;
  packet_context->session = session;
  status = cnet_packet_session_get_info(endpoint, session, &packet_context->info);
  if (status != SALTS_OK) {
    mem_buffer_release(buffer);
    packet_source_fail(source, status, session, "session_info");
    return;
  }
  memcpy(mem_buffer_data(buffer) + sizeof(*packet_context), view->data, view->size);
  mem_set_used(buffer, buffer_size);
  turbo_flow_msg_init(&message);
  message.id = source->next_message_id;
  message.buffer = buffer;
  message.payload =
      vstr_from_buf(mem_buffer_const_data(buffer) + sizeof(*packet_context), view->size);
  message.transport_context = packet_context;
  if (source->has_content) {
    status = turbo_flow_msg_copy_content_descriptor(&message, &source->content);
    if (status != SALTS_OK) {
      turbo_flow_msg_cleanup(&message);
      packet_source_fail(source, status, session, "content_descriptor");
      return;
    }
  }
  status = packet_source_stl_status(queue_push(&source->messages, &message));
  if (status != SALTS_OK) {
    turbo_flow_msg_cleanup(&message);
    packet_source_fail(source, status, session, "message_queue");
    return;
  }
  turbo_flow_msg_init(&message);
  source->message_id_exhausted = source->next_message_id == UINT64_MAX;
  if (!source->message_id_exhausted) ++source->next_message_id;
  ++source->messages_received;
  source->bytes_received += (uint64_t)view->size;
  packet_source_wake(&source->value_waker);
}

static void packet_source_on_error(void *user, cnet_packet_endpoint *endpoint,
                                   cnet_packet_session session, int status) {
  turbo_flow_cnet_packet_source_t *source = (turbo_flow_cnet_packet_source_t *)user;
  (void)endpoint;
  packet_source_fail(source, status, session, "packet_endpoint");
}

static bool packet_source_observer_empty(const cnet_packet_observer *observer) {
  return observer && !observer->on_admit && !observer->on_state && !observer->on_receive &&
         !observer->on_error && !observer->user;
}

static int packet_source_config_validate(const turbo_flow_cnet_packet_source_config_t *config,
                                         bool managed) {
  const cnet_packet_endpoint_config *endpoint;
  int status;
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION || !config->flow ||
      !config->source_name || config->source_name[0] == '\0' || !config->endpoint ||
      config->queue_capacity == 0u ||
      config->queue_capacity > SIZE_MAX / sizeof(turbo_flow_msg_t) ||
      config->max_message_bytes == 0u ||
      config->max_message_bytes > SIZE_MAX - sizeof(turbo_flow_cnet_packet_message_context_t) ||
      config->scheduler_capacity == 0u || config->scheduler_max_steps_per_poll == 0u ||
      config->first_message_id == 0u)
    return SALTS_EINVAL;
  if ((!managed && turbo_flow_state(config->flow) != TURBO_FLOW_STATE_STARTED) ||
      (managed && turbo_flow_state(config->flow) != TURBO_FLOW_STATE_COMPILED))
    return SALTS_EBUSY;
  endpoint = config->endpoint;
  if (endpoint->size != sizeof(*endpoint) || endpoint->session_capacity == 0u ||
      endpoint->session_capacity > UINT32_MAX ||
      !packet_source_observer_empty(&endpoint->observer) ||
      (endpoint->protocol != CNET_PACKET_UDP && endpoint->protocol != CNET_PACKET_KCP))
    return SALTS_EINVAL;
  if (endpoint->protocol == CNET_PACKET_UDP &&
      (config->max_message_bytes > endpoint->datagram.max_datagram_bytes ||
       config->max_message_bytes > endpoint->datagram.receive_buffer_bytes))
    return SALTS_EINVAL;
  if (endpoint->protocol == CNET_PACKET_KCP &&
      config->max_message_bytes > endpoint->kcp.max_message_bytes)
    return SALTS_EINVAL;
  if (config->content) {
    status = turbo_flow_content_descriptor_check(config->content);
    if (status != SALTS_OK) return status;
  }
  return SALTS_OK;
}

static void packet_source_clear_queue(turbo_flow_cnet_packet_source_t *source) {
  turbo_flow_msg_t message;
  if (!source || !source->queue_initialized) return;
  while (!queue_empty(&source->messages)) {
    turbo_flow_msg_init(&message);
    if (queue_pop(&source->messages, &message) != STL_OK) break;
    turbo_flow_msg_cleanup(&message);
  }
}

static void packet_source_open_cleanup(turbo_flow_cnet_packet_source_t *source,
                                       cflow_publisher *publisher) {
  if (!source) return;
  if (source->run) {
    if (!source->managed_run) turbo_flow_run_close(source->run);
    source->run = NULL;
  } else if (publisher && cflow_publisher_valid(publisher)) {
    cflow_publisher_destroy(publisher);
  }
  if (source->endpoint_initialized) {
    (void)cnet_packet_endpoint_stop(&source->endpoint, PACKET_SOURCE_OPEN_CLEANUP_TIMEOUT_MS);
    (void)cnet_packet_endpoint_destroy(&source->endpoint);
  }
  if (source->scheduler_initialized) cflow_scheduler_destroy(&source->scheduler);
  packet_source_clear_queue(source);
  if (source->queue_initialized) queue_destroy(&source->messages);
  tstr_free(source->source_name);
  free(source);
}

static int packet_source_open_impl(const turbo_flow_cnet_packet_source_config_t *config,
                                   const turbo_flow_stage_plan_t *managed_stage,
                                   turbo_flow_cnet_packet_source_t **source_out) {
  turbo_flow_cnet_packet_source_t *source;
  turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
  cnet_packet_endpoint_config endpoint_config;
  cflow_publisher publisher = {0};
  int status;
  if (!source_out) return SALTS_EINVAL;
  *source_out = NULL;
  status = packet_source_config_validate(config, managed_stage != NULL);
  if (status != SALTS_OK) return status;
  source = (turbo_flow_cnet_packet_source_t *)calloc(1u, sizeof(*source));
  if (!source) return SALTS_ENOMEM;
  source->state = TURBO_FLOW_CNET_PACKET_SOURCE_NEW;
  source->status = SALTS_OK;
  source->source_name = tstr_dup(config->source_name);
  source->protocol = config->endpoint->protocol;
  source->queue_capacity = config->queue_capacity;
  source->max_message_bytes = config->max_message_bytes;
  source->scheduler_max_steps_per_poll = config->scheduler_max_steps_per_poll;
  source->next_message_id = config->first_message_id;
  source->managed_run = managed_stage != NULL;
  if (!source->source_name) {
    packet_source_open_cleanup(source, &publisher);
    return SALTS_ENOMEM;
  }
  if (config->content) {
    source->content = *config->content;
    source->has_content = true;
  }
  status = packet_source_stl_status(queue_init_bytes(&source->messages, sizeof(turbo_flow_msg_t),
                                                     _Alignof(turbo_flow_msg_t),
                                                     config->queue_capacity));
  if (status != SALTS_OK) {
    packet_source_open_cleanup(source, &publisher);
    return status;
  }
  source->queue_initialized = true;
  status = packet_source_stl_status(queue_reserve(&source->messages, config->queue_capacity));
  if (status != SALTS_OK) {
    packet_source_open_cleanup(source, &publisher);
    return status;
  }
  if (!cflow_scheduler_manual_init_with_capacity(&source->scheduler, config->scheduler_capacity)) {
    packet_source_open_cleanup(source, &publisher);
    return SALTS_ENOMEM;
  }
  source->scheduler_initialized = true;
  endpoint_config = *config->endpoint;
  endpoint_config.observer = (cnet_packet_observer){.on_admit = packet_source_on_admit,
                                                    .on_state = packet_source_on_state,
                                                    .on_receive = packet_source_on_receive,
                                                    .on_error = packet_source_on_error,
                                                    .user = source};
  status = cnet_packet_endpoint_init(&source->endpoint, &endpoint_config);
  if (status != SALTS_OK) {
    packet_source_open_cleanup(source, &publisher);
    return status;
  }
  source->endpoint_initialized = true;
  status = cnet_packet_endpoint_port(&source->endpoint, &source->bound_port);
  if (status != SALTS_OK) {
    packet_source_open_cleanup(source, &publisher);
    return status;
  }
  publisher = packet_source_publisher_as_cflow_publisher(source);
  run_config.scheduler = &source->scheduler;
  status = managed_stage ? turbo_flow_managed_source_run_open(config->flow, managed_stage,
                                                              &publisher, &run_config, &source->run)
                         : turbo_flow_run_open(config->flow, config->source_name, &publisher,
                                               &run_config, &source->run);
  if (status != SALTS_OK) {
    packet_source_open_cleanup(source, &publisher);
    return status;
  }
  source->state = TURBO_FLOW_CNET_PACKET_SOURCE_RUNNING;
  *source_out = source;
  return SALTS_OK;
}

int turbo_flow_cnet_packet_source_open(const turbo_flow_cnet_packet_source_config_t *config,
                                       turbo_flow_cnet_packet_source_t **source_out) {
  return packet_source_open_impl(config, NULL, source_out);
}

int turbo_flow_cnet_packet_source_open_managed(const turbo_flow_cnet_packet_source_config_t *config,
                                               const turbo_flow_stage_plan_t *stage,
                                               turbo_flow_cnet_packet_source_t **source_out) {
  if (!stage || !config || !config->source_name || !stage->name ||
      strcmp(config->source_name, stage->name) != 0)
    return SALTS_EINVAL;
  return packet_source_open_impl(config, stage, source_out);
}

static void packet_source_refresh_run(turbo_flow_cnet_packet_source_t *source) {
  turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
  if (!source || !source->run || source->state == TURBO_FLOW_CNET_PACKET_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_PACKET_SOURCE_STOPPED)
    return;
  if (turbo_flow_run_snapshot(source->run, &result) != SALTS_OK) return;
  if (result.state == TURBO_FLOW_RUN_FAILED || result.state == TURBO_FLOW_RUN_CANCELED)
    packet_source_fail(source, result.status, (cnet_packet_session){0}, "graph_run");
}

static int packet_source_live_status(const turbo_flow_cnet_packet_source_t *source) {
  if (!source) return SALTS_EINVAL;
  if (source->state == TURBO_FLOW_CNET_PACKET_SOURCE_FAILED) return source->status;
  if (source->state != TURBO_FLOW_CNET_PACKET_SOURCE_RUNNING) return SALTS_ESHUTDOWN;
  return SALTS_OK;
}

int turbo_flow_cnet_packet_source_request(turbo_flow_cnet_packet_source_t *source, size_t demand) {
  int status;
  if (!source || demand == 0u) return SALTS_EINVAL;
  status = packet_source_live_status(source);
  if (status != SALTS_OK) return status;
  status = turbo_flow_run_request(source->run, demand);
  packet_source_refresh_run(source);
  return source->state == TURBO_FLOW_CNET_PACKET_SOURCE_FAILED ? source->status : status;
}

int turbo_flow_cnet_packet_source_snapshot(const turbo_flow_cnet_packet_source_t *source,
                                           turbo_flow_cnet_packet_source_snapshot_t *snapshot) {
  turbo_flow_cnet_packet_source_snapshot_t current = TURBO_FLOW_CNET_PACKET_SOURCE_SNAPSHOT_INIT;
  turbo_flow_run_result_t run_result = TURBO_FLOW_RUN_RESULT_INIT;
  if (!source || !snapshot || snapshot->size < sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION)
    return SALTS_EINVAL;
  current.state = source->state;
  current.status = source->status;
  current.protocol = source->protocol;
  current.bound_port = source->bound_port;
  current.queue_depth = source->queue_initialized ? queue_size(&source->messages) : 0u;
  current.queue_capacity = source->queue_capacity;
  current.sessions_admitted = source->sessions_admitted;
  current.sessions_opened = source->sessions_opened;
  current.sessions_closed = source->sessions_closed;
  current.messages_received = source->messages_received;
  current.bytes_received = source->bytes_received;
  current.last_error_session = source->last_error_session;
  packet_source_copy_stage(current.error_stage, sizeof(current.error_stage),
                           source->error_stage[0] ? source->error_stage : "");
  if (source->run && turbo_flow_run_snapshot(source->run, &run_result) == SALTS_OK)
    current.outstanding_demand = run_result.outstanding_demand;
  *snapshot = current;
  return SALTS_OK;
}

int turbo_flow_cnet_packet_source_poll(turbo_flow_cnet_packet_source_t *source, uint32_t timeout_ms,
                                       turbo_flow_cnet_packet_source_snapshot_t *snapshot) {
  size_t events = 0u;
  int status;
  if (!source || (snapshot && (snapshot->size < sizeof(*snapshot) ||
                               snapshot->version != TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION)))
    return SALTS_EINVAL;
  status = packet_source_live_status(source);
  if (status != SALTS_OK) {
    if (snapshot) (void)turbo_flow_cnet_packet_source_snapshot(source, snapshot);
    return status;
  }
  (void)cflow_scheduler_run_until_idle(&source->scheduler, source->scheduler_max_steps_per_poll);
  packet_source_refresh_run(source);
  if (source->state == TURBO_FLOW_CNET_PACKET_SOURCE_RUNNING) {
    status = cnet_packet_poll(&source->endpoint, timeout_ms, &events);
    if (status != SALTS_OK)
      packet_source_fail(source, status, source->last_error_session, "packet_poll");
  }
  (void)cflow_scheduler_run_until_idle(&source->scheduler, source->scheduler_max_steps_per_poll);
  packet_source_refresh_run(source);
  if (snapshot) (void)turbo_flow_cnet_packet_source_snapshot(source, snapshot);
  return source->state == TURBO_FLOW_CNET_PACKET_SOURCE_FAILED ? source->status : SALTS_OK;
}

int turbo_flow_cnet_packet_source_session_open(turbo_flow_cnet_packet_source_t *source,
                                               const cnet_datagram_peer *peer,
                                               uint32_t conversation,
                                               cnet_packet_session *session_out) {
  int status = packet_source_live_status(source);
  if (session_out) *session_out = (cnet_packet_session){0};
  if (status != SALTS_OK) return status;
  return cnet_packet_session_open(&source->endpoint, peer, conversation, session_out);
}

int turbo_flow_cnet_packet_source_session_get_info(const turbo_flow_cnet_packet_source_t *source,
                                                   cnet_packet_session session,
                                                   cnet_packet_session_info *info_out) {
  int status = packet_source_live_status(source);
  if (info_out) memset(info_out, 0, sizeof(*info_out));
  if (status != SALTS_OK) return status;
  return cnet_packet_session_get_info(&source->endpoint, session, info_out);
}

int turbo_flow_cnet_packet_source_session_close(turbo_flow_cnet_packet_source_t *source,
                                                cnet_packet_session session) {
  int status = packet_source_live_status(source);
  if (status != SALTS_OK) return status;
  return cnet_packet_session_close(&source->endpoint, session);
}

int turbo_flow_cnet_packet_source_send(turbo_flow_cnet_packet_source_t *source,
                                       cnet_packet_session session, const void *data, size_t size) {
  int status = packet_source_live_status(source);
  if (status != SALTS_OK) return status;
  return cnet_packet_send(&source->endpoint, session, data, size);
}

const turbo_flow_cnet_packet_message_context_t *
turbo_flow_cnet_packet_message_context(const turbo_flow_msg_t *message) {
  const turbo_flow_cnet_packet_message_context_t *context;
  const char *base;
  uintptr_t base_address;
  uintptr_t context_address;
  size_t used;
  size_t offset;
  if (!message || !message->buffer || !message->transport_context) return NULL;
  base = mem_buffer_const_data(message->buffer);
  used = mem_buffer_used(message->buffer);
  base_address = (uintptr_t)base;
  context_address = (uintptr_t)message->transport_context;
  if (!base || context_address < base_address) return NULL;
  offset = (size_t)(context_address - base_address);
  if (offset > used || used - offset < sizeof(*context)) return NULL;
  context = (const turbo_flow_cnet_packet_message_context_t *)message->transport_context;
  if (context->size < sizeof(*context) || context->size > used - offset ||
      context->version != TURBO_FLOW_CNET_PACKET_SOURCE_API_VERSION ||
      !cnet_packet_session_valid(context->session))
    return NULL;
  if (message->payload.data != base + offset + context->size ||
      message->payload.len > used - offset - context->size)
    return NULL;
  return context;
}

int turbo_flow_cnet_packet_source_stop(turbo_flow_cnet_packet_source_t *source,
                                       uint32_t timeout_ms) {
  int status;
  if (!source) return SALTS_EINVAL;
  if (source->state == TURBO_FLOW_CNET_PACKET_SOURCE_STOPPED) return SALTS_EALREADY;
  source->state = TURBO_FLOW_CNET_PACKET_SOURCE_STOPPING;
  if (source->run) {
    if (!source->managed_run) turbo_flow_run_close(source->run);
    source->run = NULL;
  }
  packet_source_clear_queue(source);
  if (source->endpoint_initialized) {
    status = cnet_packet_endpoint_stop(&source->endpoint, timeout_ms);
    if (status != SALTS_OK) return status;
    status = cnet_packet_endpoint_destroy(&source->endpoint);
    if (status != SALTS_OK) return status;
    source->endpoint_initialized = false;
  }
  if (source->scheduler_initialized) {
    (void)cflow_scheduler_run_until_idle(&source->scheduler, 0u);
    (void)cflow_scheduler_shutdown(&source->scheduler);
    cflow_scheduler_destroy(&source->scheduler);
    source->scheduler_initialized = false;
  }
  source->state = TURBO_FLOW_CNET_PACKET_SOURCE_STOPPED;
  return SALTS_OK;
}

int turbo_flow_cnet_packet_source_destroy(turbo_flow_cnet_packet_source_t *source) {
  if (!source) return SALTS_EINVAL;
  if (source->state != TURBO_FLOW_CNET_PACKET_SOURCE_STOPPED) return SALTS_EBUSY;
  packet_source_clear_queue(source);
  if (source->queue_initialized) queue_destroy(&source->messages);
  tstr_free(source->source_name);
  free(source);
  return SALTS_OK;
}
