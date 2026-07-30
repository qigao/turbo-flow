#include "flowie.h"

#include "flowie_mqtt_client.h"
#include "flowie_mqtt_protocol.h"
#include "flowie_rule_internal.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_CLIENT_SOURCE_RECONNECT_MIN_MS 1u
#define FLOWIE_CLIENT_SOURCE_POLL_MS 10u

typedef enum flowie_client_source_state_e {
  FLOWIE_CLIENT_SOURCE_DISCONNECTED = 0,
  FLOWIE_CLIENT_SOURCE_CONNECTING,
  FLOWIE_CLIENT_SOURCE_CONNECTED,
  FLOWIE_CLIENT_SOURCE_SUBSCRIBING,
  FLOWIE_CLIENT_SOURCE_READY,
  FLOWIE_CLIENT_SOURCE_FAILED
} flowie_client_source_state_t;

typedef struct flowie_client_source_s {
  tstr_t host;
  tstr_t path;
  tstr_t client_id;
  tstr_t topic_filter;
  tstr_t ca_file;
  flowie_mqtt_client_transport_t transport;
  int port;
  uint8_t qos;
  uint8_t clean_start;
  uint16_t keep_alive;
  uint64_t timeout_ms;
  size_t max_packet_size;
  size_t stream_recv_buffer_bytes;
  size_t socket_recv_buffer_bytes;
  size_t socket_send_buffer_bytes;
  uint32_t reconnect_initial_ms;
  uint32_t reconnect_max_ms;
  flowie_mqtt_client_t *client;
  turbo_thread_t supervisor;
  int supervisor_started;
  turbo_flow_t *flow;
  tstr_t source_name;
  atomic_int started;
  atomic_int state;
  atomic_int last_status;
  atomic_uint callback_depth;
} flowie_client_source_t;

static const turbo_flow_option_field_t FLOWIE_CLIENT_SOURCE_FIELDS[] = {
    {"transport", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0u, 0u, NULL, 0u},
    {"host", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0u, 0u, NULL, 0u},
    {"port", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1u,
     65535u, NULL, 0u},
    {"path", TURBO_FLOW_OPTION_STRING, 0u, 0u, 0u, NULL, 0u},
    {"client_id", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0u, 0u, NULL, 0u},
    {"topic_filter", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0u, 0u, NULL, 0u},
    {"qos", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MAX, 0u, 2u, NULL, 0u},
    {"clean_start", TURBO_FLOW_OPTION_BOOL, TURBO_FLOW_OPTION_REQUIRED, 0u, 0u, NULL, 0u},
    {"keep_alive", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MAX, 0u, UINT16_MAX, NULL, 0u},
    {"timeout_ms", TURBO_FLOW_OPTION_U64,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN, 1u, 0u, NULL, 0u},
    {"max_packet_size", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 2u,
     FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE, NULL, 0u},
    {"reconnect_initial_ms", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN, FLOWIE_CLIENT_SOURCE_RECONNECT_MIN_MS,
     0u, NULL, 0u},
    {"reconnect_max_ms", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN, FLOWIE_CLIENT_SOURCE_RECONNECT_MIN_MS,
     0u, NULL, 0u},
    {"ca_file", TURBO_FLOW_OPTION_STRING, 0u, 0u, 0u, NULL, 0u},
    {"stream_recv_buffer_bytes", TURBO_FLOW_OPTION_SIZE, 0u, 0u, 0u, NULL, 0u},
    {"socket_recv_buffer_bytes", TURBO_FLOW_OPTION_SIZE, TURBO_FLOW_OPTION_HAS_MAX, 0u, INT_MAX,
     NULL, 0u},
    {"socket_send_buffer_bytes", TURBO_FLOW_OPTION_SIZE, TURBO_FLOW_OPTION_HAS_MAX, 0u, INT_MAX,
     NULL, 0u}};

static const turbo_flow_adapter_schema_t FLOWIE_CLIENT_SOURCE_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_SOCKET,
    TURBO_FLOW_ADAPTER_SOURCE,
    TURBO_FLOW_ADAPTER_INPUT,
    FLOWIE_CLIENT_SOURCE_FIELDS,
    sizeof(FLOWIE_CLIENT_SOURCE_FIELDS) / sizeof(FLOWIE_CLIENT_SOURCE_FIELDS[0])};

static int flowie_client_source_error(turbo_flow_config_error_t *error, int status,
                                      const char *name, const char *field,
                                      const char *message) {
  if (error && error->size >= sizeof(*error)) {
    *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    error->status = status;
    if (field)
      (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s.config.%s", name, field);
    else
      (void)snprintf(error->path, sizeof(error->path), "$.adapters.%s", name ? name : "?");
    (void)snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static int flowie_client_source_parse_status(int status) {
  switch (status) {
  case FLOWIE_MQTT_PARSE_OK:
    return TURBO_OK;
  case FLOWIE_MQTT_PARSE_NO_MEMORY:
    return TURBO_ENOMEM;
  case FLOWIE_MQTT_PARSE_TOO_LARGE:
    return TURBO_EMSGSIZE;
  case FLOWIE_MQTT_PARSE_INVALID_ARGUMENT:
    return TURBO_EINVAL;
  case FLOWIE_MQTT_PARSE_MALFORMED:
  case FLOWIE_MQTT_PARSE_PROTOCOL_ERROR:
  default:
    return TURBO_EPROTO;
  }
}

static size_t flowie_client_source_wire_capacity(const flowie_mqtt_publish_view_t *publish) {
  size_t capacity;
  if (!publish || publish->topic.size > SIZE_MAX - publish->properties.values.size ||
      publish->topic.size + publish->properties.values.size > SIZE_MAX - publish->payload.size ||
      publish->topic.size + publish->properties.values.size + publish->payload.size >
          SIZE_MAX - 16u)
    return 0u;
  capacity =
      publish->topic.size + publish->properties.values.size + publish->payload.size + 16u;
  return capacity;
}

static int flowie_client_source_message(flowie_mqtt_client_t *client,
                                        const flowie_mqtt_publish_view_t *publish,
                                        void *user_data) {
  flowie_client_source_t *source = (flowie_client_source_t *)user_data;
  flowie_mqtt_publish_packet_t packet = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  turbo_flow_msg_t message;
  size_t capacity;
  size_t written = 0u;
  uint8_t fixed_flags;
  int rc;
  (void)client;
  if (!source || !publish ||
      !atomic_load_explicit(&source->started, memory_order_acquire) || !source->flow ||
      !source->source_name)
    return TURBO_ESHUTDOWN;
  (void)atomic_fetch_add_explicit(&source->callback_depth, 1u, memory_order_acq_rel);
  capacity = flowie_client_source_wire_capacity(publish);
  if (capacity == 0u)
    rc = TURBO_ERANGE;
  else if (capacity > source->max_packet_size)
    rc = TURBO_EMSGSIZE;
  else
    rc = TURBO_OK;
  turbo_flow_msg_init(&message);
  if (rc == TURBO_OK) {
    message.buffer = mem_get_buffer(mem_global(), capacity);
    if (!message.buffer) rc = TURBO_ENOMEM;
  }
  if (rc == TURBO_OK) {
    packet.version = FLOWIE_MQTT_VERSION_5;
    packet.qos = publish->qos;
    packet.retain = publish->retain;
    packet.duplicate = publish->duplicate;
    packet.packet_id = publish->packet_id;
    packet.topic = publish->topic;
    packet.properties = publish->properties.values;
    packet.payload = publish->payload;
    rc = flowie_client_source_parse_status(flowie_mqtt_publish_packet_encode(
        &packet, (uint8_t *)mem_buffer_data(message.buffer), capacity, &written));
  }
  if (rc == TURBO_OK) {
    mem_set_used(message.buffer, written);
    message.type = FLOWIE_MQTT_PACKET_PUBLISH;
    fixed_flags = (uint8_t)((publish->duplicate ? 0x08u : 0u) |
                            ((publish->qos & 0x03u) << 1u) |
                            (publish->retain ? 0x01u : 0u));
    rc = flowie_mqtt_message_flags_encode(FLOWIE_MQTT_VERSION_5, fixed_flags, &message.flags);
  }
  if (rc == TURBO_OK) {
    message.payload = tstr_v_from_buf(mem_buffer_data(message.buffer), written);
    rc = flowie_mqtt_rule_bind_projection(&message, NULL);
  }
  if (rc == TURBO_OK) rc = turbo_flow_publish(source->flow, source->source_name, &message);
  turbo_flow_msg_cleanup(&message);
  (void)atomic_fetch_sub_explicit(&source->callback_depth, 1u, memory_order_acq_rel);
  return rc;
}

static void flowie_client_source_connect_complete(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  flowie_client_source_t *source = (flowie_client_source_t *)user_data;
  (void)client;
  (void)response;
  if (!source) return;
  (void)atomic_fetch_add_explicit(&source->callback_depth, 1u, memory_order_acq_rel);
  atomic_store_explicit(&source->last_status, status, memory_order_release);
  atomic_store_explicit(&source->state,
                        status == TURBO_OK ? FLOWIE_CLIENT_SOURCE_CONNECTED
                                           : FLOWIE_CLIENT_SOURCE_FAILED,
                        memory_order_release);
  (void)atomic_fetch_sub_explicit(&source->callback_depth, 1u, memory_order_acq_rel);
}

static void flowie_client_source_subscribe_complete(
    flowie_mqtt_client_t *client, int status,
    const flowie_mqtt_control_packet_view_t *response, void *user_data) {
  flowie_client_source_t *source = (flowie_client_source_t *)user_data;
  (void)client;
  (void)response;
  if (!source) return;
  (void)atomic_fetch_add_explicit(&source->callback_depth, 1u, memory_order_acq_rel);
  atomic_store_explicit(&source->last_status, status, memory_order_release);
  atomic_store_explicit(&source->state,
                        status == TURBO_OK ? FLOWIE_CLIENT_SOURCE_READY
                                           : FLOWIE_CLIENT_SOURCE_FAILED,
                        memory_order_release);
  (void)atomic_fetch_sub_explicit(&source->callback_depth, 1u, memory_order_acq_rel);
}

static void flowie_client_source_background_error(flowie_mqtt_client_t *client, int status,
                                                  void *user_data) {
  flowie_client_source_t *source = (flowie_client_source_t *)user_data;
  (void)client;
  if (!source) return;
  (void)atomic_fetch_add_explicit(&source->callback_depth, 1u, memory_order_acq_rel);
  atomic_store_explicit(&source->last_status, status, memory_order_release);
  atomic_store_explicit(&source->state, FLOWIE_CLIENT_SOURCE_FAILED, memory_order_release);
  (void)atomic_fetch_sub_explicit(&source->callback_depth, 1u, memory_order_acq_rel);
}

static int flowie_client_source_client_create(flowie_client_source_t *source) {
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_client_topic_handler_t handler;
  if (!source || source->client) return TURBO_EINVAL;
  memset(&handler, 0, sizeof(handler));
  handler.filter = (flowie_mqtt_span_t){(const uint8_t *)source->topic_filter,
                                        tstr_len(source->topic_filter)};
  handler.on_message = flowie_client_source_message;
  config.transport = source->transport;
  config.host = source->host;
  config.port = source->port;
  config.path = source->path;
  config.timeout_ms = source->timeout_ms;
  config.max_packet_size = source->max_packet_size;
  config.topic_handlers.data = &handler;
  config.topic_handlers.count = 1u;
  config.on_connect = flowie_client_source_connect_complete;
  config.on_subscribe = flowie_client_source_subscribe_complete;
  config.on_error = flowie_client_source_background_error;
  config.user_data = source;
  config.tls.ca_file = source->ca_file;
  config.stream_recv_buffer_bytes = source->stream_recv_buffer_bytes;
  config.socket_recv_buffer_bytes = source->socket_recv_buffer_bytes;
  config.socket_send_buffer_bytes = source->socket_send_buffer_bytes;
  return flowie_mqtt_client_create(&config, &source->client);
}

static int flowie_client_source_connect(flowie_client_source_t *source) {
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  if (!source || !source->client) return TURBO_EINVAL;
  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = source->clean_start;
  connect.keep_alive = source->keep_alive;
  connect.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)source->client_id, tstr_len(source->client_id)};
  atomic_store_explicit(&source->state, FLOWIE_CLIENT_SOURCE_CONNECTING, memory_order_release);
  return flowie_mqtt_client_connect(source->client, &connect);
}

static int flowie_client_source_subscribe(flowie_client_source_t *source) {
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  flowie_mqtt_subscription_t subscription;
  if (!source || !source->client) return TURBO_EINVAL;
  memset(&subscription, 0, sizeof(subscription));
  subscription.filter = (flowie_mqtt_span_t){(const uint8_t *)source->topic_filter,
                                              tstr_len(source->topic_filter)};
  subscription.qos = source->qos;
  subscribe.version = FLOWIE_MQTT_VERSION_5;
  subscribe.subscriptions = &subscription;
  subscribe.subscription_count = 1u;
  atomic_store_explicit(&source->state, FLOWIE_CLIENT_SOURCE_SUBSCRIBING,
                        memory_order_release);
  return flowie_mqtt_client_subscribe(source->client, &subscribe);
}

static int flowie_client_source_wait(flowie_client_source_t *source, uint32_t delay_ms) {
  uint32_t elapsed = 0u;
  while (elapsed < delay_ms) {
    uint32_t step = delay_ms - elapsed;
    if (!atomic_load_explicit(&source->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
    if (step > FLOWIE_CLIENT_SOURCE_POLL_MS) step = FLOWIE_CLIENT_SOURCE_POLL_MS;
    turbo_sleep_ms(step);
    elapsed += step;
  }
  return TURBO_OK;
}

static void flowie_client_source_wait_callbacks(flowie_client_source_t *source) {
  while (atomic_load_explicit(&source->callback_depth, memory_order_acquire) != 0u)
    turbo_thread_yield();
}

static void flowie_client_source_supervise(void *ctx) {
  flowie_client_source_t *source = (flowie_client_source_t *)ctx;
  uint32_t reconnect_delay = source->reconnect_initial_ms;
  while (atomic_load_explicit(&source->started, memory_order_acquire)) {
    flowie_client_source_state_t state =
        (flowie_client_source_state_t)atomic_load_explicit(&source->state, memory_order_acquire);
    int rc = TURBO_OK;
    if (state == FLOWIE_CLIENT_SOURCE_CONNECTED) {
      rc = flowie_client_source_subscribe(source);
      if (rc != TURBO_OK) {
        atomic_store_explicit(&source->last_status, rc, memory_order_release);
        atomic_store_explicit(&source->state, FLOWIE_CLIENT_SOURCE_FAILED, memory_order_release);
      }
      continue;
    }
    if (state == FLOWIE_CLIENT_SOURCE_READY) {
      reconnect_delay = source->reconnect_initial_ms;
      (void)flowie_client_source_wait(source, FLOWIE_CLIENT_SOURCE_POLL_MS);
      continue;
    }
    if (state != FLOWIE_CLIENT_SOURCE_FAILED) {
      (void)flowie_client_source_wait(source, FLOWIE_CLIENT_SOURCE_POLL_MS);
      continue;
    }
    if (flowie_client_source_wait(source, reconnect_delay) != TURBO_OK) break;
    flowie_client_source_wait_callbacks(source);
    flowie_mqtt_client_destroy(source->client);
    source->client = NULL;
    rc = flowie_client_source_client_create(source);
    if (rc == TURBO_OK) rc = flowie_client_source_connect(source);
    if (rc != TURBO_OK) {
      atomic_store_explicit(&source->last_status, rc, memory_order_release);
      atomic_store_explicit(&source->state, FLOWIE_CLIENT_SOURCE_FAILED, memory_order_release);
    }
    if (reconnect_delay < source->reconnect_max_ms) {
      uint64_t next = (uint64_t)reconnect_delay * 2u;
      reconnect_delay =
          next > source->reconnect_max_ms ? source->reconnect_max_ms : (uint32_t)next;
    }
  }
  flowie_client_source_wait_callbacks(source);
  flowie_mqtt_client_destroy(source->client);
  source->client = NULL;
}

static int flowie_client_source_start(void *ctx, turbo_flow_t *flow,
                                      const turbo_flow_stage_plan_t *stage) {
  flowie_client_source_t *source = (flowie_client_source_t *)ctx;
  int rc;
  if (!source || !flow || !stage || !stage->is_source ||
      atomic_load_explicit(&source->started, memory_order_acquire))
    return TURBO_EINVAL;
  source->source_name = tstr_dup(stage->name);
  if (!source->source_name) return TURBO_ENOMEM;
  source->flow = flow;
  rc = flowie_client_source_client_create(source);
  if (rc != TURBO_OK) goto fail;
  atomic_store_explicit(&source->started, 1, memory_order_release);
  rc = flowie_client_source_connect(source);
  if (rc != TURBO_OK) goto fail_started;
  rc = turbo_thread_create(&source->supervisor, flowie_client_source_supervise, source);
  if (rc != TURBO_OK) goto fail_started;
  source->supervisor_started = 1;
  return TURBO_OK;

fail_started:
  atomic_store_explicit(&source->started, 0, memory_order_release);
  flowie_client_source_wait_callbacks(source);
  flowie_mqtt_client_destroy(source->client);
  source->client = NULL;
fail:
  source->flow = NULL;
  tstr_freep(&source->source_name);
  return rc;
}

static void flowie_client_source_stop(void *ctx, turbo_flow_t *flow,
                                      const turbo_flow_stage_plan_t *stage) {
  flowie_client_source_t *source = (flowie_client_source_t *)ctx;
  (void)flow;
  (void)stage;
  if (!source) return;
  atomic_store_explicit(&source->started, 0, memory_order_release);
  if (source->supervisor_started) {
    (void)turbo_thread_join(&source->supervisor);
    source->supervisor_started = 0;
  } else if (source->client) {
    flowie_client_source_wait_callbacks(source);
    flowie_mqtt_client_destroy(source->client);
    source->client = NULL;
  }
  source->flow = NULL;
  tstr_freep(&source->source_name);
}

static void flowie_client_source_shutdown(void *ctx) {
  flowie_client_source_t *source = (flowie_client_source_t *)ctx;
  if (!source) return;
  flowie_client_source_stop(source, NULL, NULL);
  tstr_freep(&source->host);
  tstr_freep(&source->path);
  tstr_freep(&source->client_id);
  tstr_freep(&source->topic_filter);
  tstr_freep(&source->ca_file);
  free(source);
}

static int flowie_client_source_transport(const char *text,
                                          flowie_mqtt_client_transport_t *out) {
  if (!text || !out) return TURBO_EINVAL;
  if (strcmp(text, "tcp") == 0) *out = FLOWIE_MQTT_CLIENT_TRANSPORT_TCP;
  else if (strcmp(text, "tls") == 0) *out = FLOWIE_MQTT_CLIENT_TRANSPORT_TLS;
  else if (strcmp(text, "ws") == 0) *out = FLOWIE_MQTT_CLIENT_TRANSPORT_WS;
  else if (strcmp(text, "wss") == 0) *out = FLOWIE_MQTT_CLIENT_TRANSPORT_WSS;
  else return TURBO_ENOTSUP;
  return TURBO_OK;
}

int flowie_register_resolved_client_source(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_resolved_config_t *resolved,
                                           turbo_flow_config_error_t *error) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  flowie_client_source_t *source = NULL;
  turbo_flow_adapter_ops_t ops;
  const char *host = NULL;
  const char *path = NULL;
  const char *client_id = NULL;
  const char *topic_filter = NULL;
  const char *transport = NULL;
  const char *ca_file = NULL;
  uint64_t value;
  int clean_start;
  int rc;
  if (!flow || !name || !name[0] || !resolved || !error || error->size < sizeof(*error))
    return TURBO_EINVAL;
  *error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  rc = turbo_flow_resolved_config_adapter(resolved, name, &view);
  if (rc != TURBO_OK)
    return flowie_client_source_error(error, rc, name, NULL, "adapter is not resolved");
  if (strcmp(view.kind, "flowie_client") != 0)
    return flowie_client_source_error(error, TURBO_EINVAL, name, NULL,
                                      "adapter kind must be flowie_client");
  for (size_t i = 0u; i < turbo_flow_resolved_adapter_field_count(&view); ++i) {
    const char *field = turbo_flow_resolved_adapter_field_name(&view, i);
    int known = 0;
    for (size_t j = 0u;
         j < sizeof(FLOWIE_CLIENT_SOURCE_FIELDS) / sizeof(FLOWIE_CLIENT_SOURCE_FIELDS[0]); ++j) {
      if (field && strcmp(field, FLOWIE_CLIENT_SOURCE_FIELDS[j].name) == 0) {
        known = 1;
        break;
      }
    }
    if (!known)
      return flowie_client_source_error(error, TURBO_EINVAL, name, field,
                                        "unknown Flowie client field");
  }
  if (turbo_flow_resolved_adapter_get_string(&view, "host", &host) != TURBO_OK || !host[0] ||
      turbo_flow_resolved_adapter_get_string(&view, "transport", &transport) != TURBO_OK ||
      turbo_flow_resolved_adapter_get_string(&view, "client_id", &client_id) != TURBO_OK ||
      !client_id[0] ||
      turbo_flow_resolved_adapter_get_string(&view, "topic_filter", &topic_filter) != TURBO_OK ||
      !topic_filter[0] || strlen(client_id) > UINT16_MAX || strlen(topic_filter) > UINT16_MAX)
    return flowie_client_source_error(error, TURBO_EINVAL, name, NULL,
                                      "required Flowie client string is invalid");
  source = (flowie_client_source_t *)calloc(1u, sizeof(*source));
  if (!source) return TURBO_ENOMEM;
  atomic_init(&source->started, 0);
  atomic_init(&source->state, FLOWIE_CLIENT_SOURCE_DISCONNECTED);
  atomic_init(&source->last_status, TURBO_OK);
  atomic_init(&source->callback_depth, 0u);
  source->host = tstr_dup(host);
  source->client_id = tstr_dup(client_id);
  source->topic_filter = tstr_dup(topic_filter);
  rc = flowie_client_source_transport(transport, &source->transport);
  if (rc != TURBO_OK) goto invalid;
  rc = turbo_flow_resolved_adapter_get_string(&view, "path", &path);
  if (rc == TURBO_OK)
    source->path = tstr_dup(path);
  else if (rc != TURBO_ENOENT)
    goto invalid;
  rc = turbo_flow_resolved_adapter_get_string(&view, "ca_file", &ca_file);
  if (rc == TURBO_OK)
    source->ca_file = tstr_dup(ca_file);
  else if (rc != TURBO_ENOENT)
    goto invalid;
  if (!source->host || !source->client_id || !source->topic_filter ||
      (path && !source->path) || (ca_file && !source->ca_file)) {
    rc = TURBO_ENOMEM;
    goto invalid;
  }
#define FLOWIE_CLIENT_GET_U64(field_name, target, maximum)                                         \
  do {                                                                                             \
    rc = turbo_flow_resolved_adapter_get_u64(&view, field_name, &value);                           \
    if (rc != TURBO_OK || value > (uint64_t)(maximum)) goto invalid;                               \
    target = value;                                                                                \
  } while (0)
  FLOWIE_CLIENT_GET_U64("port", source->port, 65535u);
  FLOWIE_CLIENT_GET_U64("qos", source->qos, 2u);
  FLOWIE_CLIENT_GET_U64("keep_alive", source->keep_alive, UINT16_MAX);
  FLOWIE_CLIENT_GET_U64("timeout_ms", source->timeout_ms, UINT64_MAX);
  FLOWIE_CLIENT_GET_U64("max_packet_size", source->max_packet_size,
                        FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE);
  FLOWIE_CLIENT_GET_U64("reconnect_initial_ms", source->reconnect_initial_ms, UINT32_MAX);
  FLOWIE_CLIENT_GET_U64("reconnect_max_ms", source->reconnect_max_ms, UINT32_MAX);
#undef FLOWIE_CLIENT_GET_U64
  rc = turbo_flow_resolved_adapter_get_bool(&view, "clean_start", &clean_start);
  if (rc != TURBO_OK) goto invalid;
  source->clean_start = clean_start ? 1u : 0u;
  rc = turbo_flow_resolved_adapter_get_u64(&view, "stream_recv_buffer_bytes", &value);
  if (rc == TURBO_OK) {
    if (value > SIZE_MAX) goto invalid_range;
    source->stream_recv_buffer_bytes = (size_t)value;
  } else if (rc != TURBO_ENOENT)
    goto invalid;
  rc = turbo_flow_resolved_adapter_get_u64(&view, "socket_recv_buffer_bytes", &value);
  if (rc == TURBO_OK) {
    if (value > INT_MAX) goto invalid_range;
    source->socket_recv_buffer_bytes = (size_t)value;
  } else if (rc != TURBO_ENOENT)
    goto invalid;
  rc = turbo_flow_resolved_adapter_get_u64(&view, "socket_send_buffer_bytes", &value);
  if (rc == TURBO_OK) {
    if (value > INT_MAX) goto invalid_range;
    source->socket_send_buffer_bytes = (size_t)value;
  } else if (rc != TURBO_ENOENT)
    goto invalid;
  if (source->port == 0 || source->timeout_ms == 0u || source->max_packet_size < 2u ||
      source->reconnect_initial_ms < FLOWIE_CLIENT_SOURCE_RECONNECT_MIN_MS ||
      source->reconnect_max_ms < source->reconnect_initial_ms ||
      ((source->transport == FLOWIE_MQTT_CLIENT_TRANSPORT_WS ||
        source->transport == FLOWIE_MQTT_CLIENT_TRANSPORT_WSS) &&
       (!source->path || source->path[0] != '/')) ||
      ((source->transport == FLOWIE_MQTT_CLIENT_TRANSPORT_TCP ||
        source->transport == FLOWIE_MQTT_CLIENT_TRANSPORT_TLS) &&
       source->path)) {
    rc = TURBO_EINVAL;
    goto invalid;
  }
  memset(&ops, 0, sizeof(ops));
  ops.start = flowie_client_source_start;
  ops.stop = flowie_client_source_stop;
  ops.shutdown = flowie_client_source_shutdown;
  rc = turbo_flow_register_adapter_with_resources(flow, name, &ops, source,
                                                  &FLOWIE_CLIENT_SOURCE_SCHEMA, NULL, 0u);
  if (rc == TURBO_OK) return TURBO_OK;

invalid:
  flowie_client_source_shutdown(source);
  return flowie_client_source_error(error, rc, name, NULL,
                                    "Flowie client configuration validation failed");
invalid_range:
  rc = TURBO_ERANGE;
  goto invalid;
}
