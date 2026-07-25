#include "flowie_mqtt_client.h"

#include "CoroNet/turbo_coro_context.h"
#include "CoroNet/turbo_coro_socket.h"
#include "flowie_mqtt_protocol.h"
#include "monocypher.h"
#include "turbo_byte_buffer.h"
#include "turbo_deque.h"
#include "turbo_error.h"
#include "turbo_set.h"
#include "turbo_str.h"
#include "turbo_thread.h"
#include "turbo_vec.h"

#include <limits.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

TURBO_SET_DEFINE(flowie_mqtt_packet_id_set_t, uint16_t)

#define FLOWIE_MQTT_CLIENT_TLS_STRING_LIMIT 4096u

typedef enum flowie_mqtt_client_state_e {
  FLOWIE_MQTT_CLIENT_DISCONNECTED = 0,
  FLOWIE_MQTT_CLIENT_TRANSPORT_CONNECTED,
  FLOWIE_MQTT_CLIENT_CONNECTED
} flowie_mqtt_client_state_t;

typedef enum flowie_mqtt_client_command_type_e {
  FLOWIE_MQTT_CLIENT_COMMAND_CONNECT = 1,
  FLOWIE_MQTT_CLIENT_COMMAND_PUBLISH,
  FLOWIE_MQTT_CLIENT_COMMAND_SUBSCRIBE,
  FLOWIE_MQTT_CLIENT_COMMAND_UNSUBSCRIBE,
  FLOWIE_MQTT_CLIENT_COMMAND_PING,
  FLOWIE_MQTT_CLIENT_COMMAND_AUTH,
  FLOWIE_MQTT_CLIENT_COMMAND_DISCONNECT
} flowie_mqtt_client_command_type_t;

typedef struct flowie_mqtt_client_command_s {
  flowie_mqtt_client_command_type_t type;
  flowie_mqtt_client_completion_fn completion;
  void *user_data;
  uint8_t *owned_bytes;
  size_t owned_size;
  union {
    flowie_mqtt_connect_packet_t connect;
    flowie_mqtt_publish_packet_t publish;
    flowie_mqtt_subscribe_packet_t subscribe;
    flowie_mqtt_unsubscribe_packet_t unsubscribe;
    struct {
      uint8_t reason_code;
      flowie_mqtt_span_t properties;
    } control;
  } packet;
} flowie_mqtt_client_command_t;

TURBO_DEQUE_DEFINE(flowie_mqtt_client_command_queue_t, flowie_mqtt_client_command_t *)
TURBO_VEC_DEFINE(flowie_mqtt_client_command_vec_t, flowie_mqtt_client_command_t *)

struct flowie_mqtt_client_s {
  coro_context_t *context;
  coro_socket_t *socket;
  flowie_mqtt_client_transport_t transport;
  flowie_mqtt_client_state_t state;
  flowie_mqtt_version_t version;
  tstr_t host;
  tstr_t path;
  tstr_t tls_ca_file;
  tstr_t tls_cert_file;
  tstr_t tls_key_file;
  tstr_t tls_key_password;
  tstr_t auth_method;
  int tls_configured;
  int port;
  uint64_t timeout_ms;
  size_t max_packet_size;
  size_t outbound_max_packet_size;
  size_t max_inbound_qos2;
  size_t stream_recv_buffer_bytes;
  size_t socket_recv_buffer_bytes;
  size_t socket_send_buffer_bytes;
  uint16_t server_receive_maximum;
  uint16_t server_topic_alias_maximum;
  uint16_t server_keep_alive;
  uint8_t server_maximum_qos;
  uint8_t server_retain_available;
  flowie_mqtt_client_topic_handler_t *topic_handlers;
  size_t topic_handler_count;
  flowie_mqtt_client_completion_fn on_connect;
  flowie_mqtt_client_completion_fn on_publish;
  flowie_mqtt_client_completion_fn on_subscribe;
  flowie_mqtt_client_completion_fn on_unsubscribe;
  flowie_mqtt_client_completion_fn on_ping;
  flowie_mqtt_client_auth_challenge_fn on_auth_challenge;
  flowie_mqtt_client_completion_fn on_auth;
  flowie_mqtt_client_completion_fn on_disconnect;
  flowie_mqtt_client_error_fn on_error;
  void *user_data;
  tstr_t send_buffer;
  turbo_byte_buffer_t framing;
  char *recv_data;
  size_t recv_size;
  size_t recv_offset;
  size_t pending_packet_size;
  uint16_t next_packet_id;
  flowie_mqtt_packet_id_set_t inbound_qos2;
  int framing_initialized;
  int qos2_initialized;
  int busy;
  int callback_active;
  int command_queue_initialized;
  int sync_initialized;
  int worker_started;
  int pump_active;
  int idle_receive;
  int interrupt_pending;
  int stopping;
  size_t command_queue_capacity;
  size_t command_queue_max_bytes;
  size_t command_queue_bytes;
  flowie_mqtt_client_command_queue_t commands;
  turbo_mutex_t command_mutex;
  turbo_thread_t worker;
  atomic_int public_connected;
};

static int flowie_mqtt_client_connect_operation(flowie_mqtt_client_t *client,
                                                const flowie_mqtt_connect_packet_t *packet,
                                                flowie_mqtt_control_packet_view_t *connack);
static int flowie_mqtt_client_publish_operation(flowie_mqtt_client_t *client,
                                                const flowie_mqtt_publish_packet_t *packet,
                                                flowie_mqtt_control_packet_view_t *ack);
static int flowie_mqtt_client_subscribe_operation(flowie_mqtt_client_t *client,
                                                  const flowie_mqtt_subscribe_packet_t *packet,
                                                  flowie_mqtt_control_packet_view_t *suback);
static int flowie_mqtt_client_unsubscribe_operation(flowie_mqtt_client_t *client,
                                                    const flowie_mqtt_unsubscribe_packet_t *packet,
                                                    flowie_mqtt_control_packet_view_t *unsuback);
static int flowie_mqtt_client_ping_operation(flowie_mqtt_client_t *client);
static int flowie_mqtt_client_auth_operation(flowie_mqtt_client_t *client,
                                             flowie_mqtt_span_t properties,
                                             flowie_mqtt_control_packet_view_t *auth);
static int flowie_mqtt_client_disconnect_operation(flowie_mqtt_client_t *client,
                                                   uint8_t reason_code,
                                                   flowie_mqtt_span_t properties);

static int flowie_mqtt_client_parse_status(int rc) {
  switch (rc) {
  case FLOWIE_MQTT_PARSE_OK:
    return TURBO_OK;
  case FLOWIE_MQTT_PARSE_NO_MEMORY:
    return TURBO_ENOMEM;
  case FLOWIE_MQTT_PARSE_TOO_LARGE:
    return TURBO_EMSGSIZE;
  case FLOWIE_MQTT_PARSE_INVALID_ARGUMENT:
    return TURBO_EINVAL;
  default:
    return TURBO_EPROTO;
  }
}

static int flowie_mqtt_client_transport_valid(flowie_mqtt_client_transport_t transport) {
  return transport >= FLOWIE_MQTT_CLIENT_TRANSPORT_TCP &&
         transport <= FLOWIE_MQTT_CLIENT_TRANSPORT_WSS;
}

static const flowie_mqtt_client_tls_config_t *
flowie_mqtt_client_tls_config(const flowie_mqtt_client_config_t *config) {
  return config ? &config->tls : NULL;
}

static int flowie_mqtt_client_tls_string_valid(const char *value) {
  size_t length;
  if (!value) return 1;
  length = strlen(value);
  return length > 0u && length <= FLOWIE_MQTT_CLIENT_TLS_STRING_LIMIT;
}

static int flowie_mqtt_client_config_validate(const flowie_mqtt_client_config_t *config) {
  const flowie_mqtt_client_tls_config_t *tls;
  size_t max_packet_size;
  if (!config || config->size != sizeof(*config) ||
      !config->host || config->host[0] == '\0' || config->port < 1 || config->port > 65535 ||
      !flowie_mqtt_client_transport_valid(config->transport))
    return TURBO_EINVAL;
  if ((config->transport == FLOWIE_MQTT_CLIENT_TRANSPORT_WS ||
       config->transport == FLOWIE_MQTT_CLIENT_TRANSPORT_WSS) &&
      config->path && config->path[0] != '/')
    return TURBO_EINVAL;
  max_packet_size = config->max_packet_size ? config->max_packet_size
                                            : FLOWIE_MQTT_CLIENT_DEFAULT_MAX_PACKET_SIZE;
  if (max_packet_size < 2u || max_packet_size > FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE ||
      config->max_inbound_qos2 == 0u)
    return TURBO_EINVAL;
  if (config->command_queue_capacity == SIZE_MAX || config->command_queue_max_bytes == SIZE_MAX)
    return TURBO_EINVAL;
  tls = flowie_mqtt_client_tls_config(config);
  if (tls) {
    int has_cert = tls->cert_file != NULL;
    int has_key = tls->key_file != NULL;
    int has_tls_config = tls->ca_file || tls->cert_file || tls->key_file || tls->key_password;
    if ((has_tls_config && config->transport != FLOWIE_MQTT_CLIENT_TRANSPORT_TLS &&
         config->transport != FLOWIE_MQTT_CLIENT_TRANSPORT_WSS) ||
        has_cert != has_key || (tls->key_password && !has_key) ||
        !flowie_mqtt_client_tls_string_valid(tls->ca_file) ||
        !flowie_mqtt_client_tls_string_valid(tls->cert_file) ||
        !flowie_mqtt_client_tls_string_valid(tls->key_file) ||
        !flowie_mqtt_client_tls_string_valid(tls->key_password))
      return TURBO_EINVAL;
  }
  if (config->topic_handlers.count != 0u && !config->topic_handlers.data) return TURBO_EINVAL;
  if (config->socket_recv_buffer_bytes > (size_t)INT_MAX ||
      config->socket_send_buffer_bytes > (size_t)INT_MAX)
    return TURBO_ERANGE;
  if ((config->stream_recv_buffer_bytes != 0u &&
       config->stream_recv_buffer_bytes < FLOWIE_MQTT_CLIENT_MIN_STREAM_RECV_BUFFER_SIZE) ||
      config->stream_recv_buffer_bytes > FLOWIE_MQTT_CLIENT_MAX_STREAM_RECV_BUFFER_SIZE)
    return TURBO_ERANGE;
  for (size_t i = 0u; i < config->topic_handlers.count; ++i) {
    const flowie_mqtt_client_topic_handler_t *handler = &config->topic_handlers.data[i];
    if (!handler->on_message || !handler->filter.data || handler->filter.size == 0u ||
        !flowie_mqtt_topic_filter_validate(handler->filter))
      return TURBO_EINVAL;
    for (size_t j = 0u; j < i; ++j) {
      flowie_mqtt_span_t previous = config->topic_handlers.data[j].filter;
      if (previous.size == handler->filter.size &&
          memcmp(previous.data, handler->filter.data, previous.size) == 0)
        return TURBO_EINVAL;
    }
  }
  return TURBO_OK;
}

static int flowie_mqtt_client_auth_method_get(const flowie_mqtt_property_block_view_t *properties,
                                              flowie_mqtt_span_t *method) {
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  int found = 0;
  int rc;
  if (!properties || !method) return TURBO_EINVAL;
  *method = (flowie_mqtt_span_t){0};
  if (properties->values.size == 0u) return TURBO_OK;
  rc = flowie_mqtt_property_iterator_init(properties, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return flowie_mqtt_client_parse_status(rc);
  while ((rc = flowie_mqtt_property_iterator_next(&iterator, &property)) == FLOWIE_MQTT_PARSE_OK) {
    if (property.identifier != FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD) continue;
    if (found || property.value.size == 0u) return TURBO_EPROTO;
    *method = property.value;
    found = 1;
  }
  return rc == FLOWIE_MQTT_PARSE_NEED_MORE ? TURBO_OK : flowie_mqtt_client_parse_status(rc);
}

static int
flowie_mqtt_client_auth_method_matches(const flowie_mqtt_client_t *client,
                                       const flowie_mqtt_property_block_view_t *properties,
                                       int required) {
  flowie_mqtt_span_t method = {0};
  int rc;
  if (!client || !properties) return TURBO_EINVAL;
  rc = flowie_mqtt_client_auth_method_get(properties, &method);
  if (rc != TURBO_OK) return rc;
  if (method.size == 0u) return required ? TURBO_EPROTO : TURBO_OK;
  if (!client->auth_method || method.size != tstr_len(client->auth_method) ||
      memcmp(method.data, client->auth_method, method.size) != 0)
    return TURBO_EPROTO;
  return TURBO_OK;
}

static int flowie_mqtt_client_auth_method_select(flowie_mqtt_client_t *client,
                                                 flowie_mqtt_span_t properties) {
  flowie_mqtt_property_block_view_t block = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  flowie_mqtt_span_t method = {0};
  tstr_t selected = NULL;
  int rc;
  if (!client) return TURBO_EINVAL;
  block.values = properties;
  rc = flowie_mqtt_client_auth_method_get(&block, &method);
  if (rc != TURBO_OK) return rc;
  if (method.size != 0u) {
    selected = tstr_new_len(method.data, method.size);
    if (!selected) return TURBO_ENOMEM;
  }
  tstr_freep(&client->auth_method);
  client->auth_method = selected;
  return TURBO_OK;
}

static void flowie_mqtt_client_recv_release(flowie_mqtt_client_t *client) {
  if (!client || !client->recv_data) return;
  coro_socket_free_recv(client->recv_data);
  client->recv_data = NULL;
  client->recv_size = 0u;
  client->recv_offset = 0u;
}

static void flowie_mqtt_client_transport_close(flowie_mqtt_client_t *client, int reset_framing) {
  if (!client) return;
  flowie_mqtt_client_recv_release(client);
  if (client->socket) {
    coro_socket_destroy(client->socket);
    client->socket = NULL;
  }
  client->state = FLOWIE_MQTT_CLIENT_DISCONNECTED;
  atomic_store_explicit(&client->public_connected, 0, memory_order_release);
  client->version = FLOWIE_MQTT_VERSION_UNSPECIFIED;
  tstr_freep(&client->auth_method);
  client->outbound_max_packet_size = client->max_packet_size;
  client->server_receive_maximum = UINT16_MAX;
  client->server_topic_alias_maximum = 0u;
  client->server_keep_alive = 0u;
  client->server_maximum_qos = 2u;
  client->server_retain_available = 1u;
  client->pending_packet_size = 0u;
  if (reset_framing && client->framing_initialized) turbo_byte_buffer_reset(&client->framing);
  if (client->qos2_initialized) flowie_mqtt_packet_id_set_t_clear(&client->inbound_qos2);
}

static int flowie_mqtt_client_begin(flowie_mqtt_client_t *client, int require_connected) {
  if (!client) return TURBO_EINVAL;
  if (client->busy || client->callback_active) return TURBO_EBUSY;
  if (coro_context_current() != client->context) return TURBO_EBUSY;
  if (require_connected && client->state != FLOWIE_MQTT_CLIENT_CONNECTED) return TURBO_ENOTCONN;
  client->busy = 1;
  return TURBO_OK;
}

static void flowie_mqtt_client_end(flowie_mqtt_client_t *client) {
  if (client) client->busy = 0;
}

static int flowie_mqtt_client_ack_output_valid(const flowie_mqtt_control_packet_view_t *out) {
  return !out || (out->size >= sizeof(*out) && out->abi_version == FLOWIE_MQTT_PROTOCOL_ABI_V1);
}

static int flowie_mqtt_client_span_valid(flowie_mqtt_span_t span) {
  return span.size == 0u || span.data != NULL;
}

static int flowie_mqtt_client_size_add(size_t *total, size_t value) {
  if (!total || value > SIZE_MAX - *total) return TURBO_EMSGSIZE;
  *total += value;
  return TURBO_OK;
}

static int flowie_mqtt_client_size_array(size_t *total, size_t count, size_t elem_size) {
  if (count != 0u && elem_size > SIZE_MAX / count) return TURBO_EMSGSIZE;
  return flowie_mqtt_client_size_add(total, count * elem_size);
}

static void flowie_mqtt_client_copy_span(flowie_mqtt_span_t source, uint8_t **cursor,
                                         flowie_mqtt_span_t *out) {
  *out = source;
  if (source.size == 0u) {
    out->data = NULL;
    return;
  }
  memcpy(*cursor, source.data, source.size);
  out->data = *cursor;
  *cursor += source.size;
}

static int
flowie_mqtt_client_clone_topic_handlers(flowie_mqtt_client_t *client,
                                        const flowie_mqtt_client_topic_handler_map_t *map) {
  flowie_mqtt_client_topic_handler_t *handlers;
  size_t array_size = 0u;
  size_t total = 0u;
  uint8_t *cursor;
  int rc;
  if (!client || !map) return TURBO_EINVAL;
  if (map->count == 0u) return TURBO_OK;
  rc =
      flowie_mqtt_client_size_array(&total, map->count, sizeof(flowie_mqtt_client_topic_handler_t));
  if (rc != TURBO_OK) return rc;
  array_size = total;
  for (size_t i = 0u; i < map->count; ++i) {
    rc = flowie_mqtt_client_size_add(&total, map->data[i].filter.size);
    if (rc != TURBO_OK) return rc;
  }
  handlers = (flowie_mqtt_client_topic_handler_t *)malloc(total);
  if (!handlers) return TURBO_ENOMEM;
  memcpy(handlers, map->data, array_size);
  cursor = (uint8_t *)handlers + array_size;
  for (size_t i = 0u; i < map->count; ++i)
    flowie_mqtt_client_copy_span(map->data[i].filter, &cursor, &handlers[i].filter);
  client->topic_handlers = handlers;
  client->topic_handler_count = map->count;
  return TURBO_OK;
}

static flowie_mqtt_client_command_t *
flowie_mqtt_client_command_new(flowie_mqtt_client_command_type_t type,
                               flowie_mqtt_client_completion_fn completion, void *user_data) {
  flowie_mqtt_client_command_t *command;
  if (!completion) return NULL;
  command = (flowie_mqtt_client_command_t *)calloc(1, sizeof(*command));
  if (!command) return NULL;
  command->type = type;
  command->completion = completion;
  command->user_data = user_data;
  return command;
}

static void flowie_mqtt_client_command_destroy(flowie_mqtt_client_command_t *command) {
  if (!command) return;
  free(command->owned_bytes);
  free(command);
}

static int flowie_mqtt_client_command_allocate(flowie_mqtt_client_command_t *command, size_t size,
                                               uint8_t **cursor) {
  if (!command || !cursor) return TURBO_EINVAL;
  *cursor = NULL;
  if (size == 0u) return TURBO_OK;
  command->owned_bytes = (uint8_t *)malloc(size);
  if (!command->owned_bytes) return TURBO_ENOMEM;
  command->owned_size = size;
  *cursor = command->owned_bytes;
  return TURBO_OK;
}

static int flowie_mqtt_client_clone_connect(flowie_mqtt_client_command_t *command,
                                            const flowie_mqtt_connect_packet_t *packet) {
  flowie_mqtt_span_t *spans;
  const flowie_mqtt_span_t source[] = {
      packet->properties,   packet->client_id, packet->will_properties, packet->will_topic,
      packet->will_payload, packet->username,  packet->password};
  size_t total = 0u;
  uint8_t *cursor;
  int rc;
  if (packet->size < sizeof(*packet) || packet->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1)
    return TURBO_EINVAL;
  for (size_t i = 0u; i < sizeof(source) / sizeof(source[0]); ++i) {
    if (!flowie_mqtt_client_span_valid(source[i])) return TURBO_EINVAL;
    rc = flowie_mqtt_client_size_add(&total, source[i].size);
    if (rc != TURBO_OK) return rc;
  }
  rc = flowie_mqtt_client_command_allocate(command, total, &cursor);
  if (rc != TURBO_OK) return rc;
  command->packet.connect = *packet;
  spans = &command->packet.connect.properties;
  for (size_t i = 0u; i < sizeof(source) / sizeof(source[0]); ++i)
    flowie_mqtt_client_copy_span(source[i], &cursor, &spans[i]);
  return TURBO_OK;
}

static int flowie_mqtt_client_clone_publish(flowie_mqtt_client_command_t *command,
                                            const flowie_mqtt_publish_packet_t *packet) {
  const flowie_mqtt_span_t source[] = {packet->topic, packet->properties, packet->payload};
  flowie_mqtt_span_t *spans;
  size_t total = 0u;
  uint8_t *cursor;
  int rc;
  if (packet->size < sizeof(*packet) || packet->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      packet->packet_id != 0u)
    return TURBO_EINVAL;
  for (size_t i = 0u; i < sizeof(source) / sizeof(source[0]); ++i) {
    if (!flowie_mqtt_client_span_valid(source[i])) return TURBO_EINVAL;
    rc = flowie_mqtt_client_size_add(&total, source[i].size);
    if (rc != TURBO_OK) return rc;
  }
  rc = flowie_mqtt_client_command_allocate(command, total, &cursor);
  if (rc != TURBO_OK) return rc;
  command->packet.publish = *packet;
  spans = &command->packet.publish.topic;
  for (size_t i = 0u; i < sizeof(source) / sizeof(source[0]); ++i)
    flowie_mqtt_client_copy_span(source[i], &cursor, &spans[i]);
  return TURBO_OK;
}

static int flowie_mqtt_client_clone_publish_topic(flowie_mqtt_client_command_t *command,
                                                  flowie_mqtt_version_t version,
                                                  const flowie_mqtt_client_publish_topic_t *topic) {
  flowie_mqtt_publish_packet_t packet = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  if (!topic) return TURBO_EINVAL;
  packet.version = version;
  packet.qos = topic->qos;
  packet.retain = topic->retain;
  packet.duplicate = topic->duplicate;
  packet.topic = topic->topic;
  packet.properties = topic->properties;
  packet.payload = topic->payload;
  return flowie_mqtt_client_clone_publish(command, &packet);
}

static int flowie_mqtt_client_clone_subscribe(flowie_mqtt_client_command_t *command,
                                              const flowie_mqtt_subscribe_packet_t *packet) {
  flowie_mqtt_subscription_t *subscriptions;
  size_t array_size;
  size_t total = 0u;
  uint8_t *cursor;
  int rc;
  if (packet->size < sizeof(*packet) || packet->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      packet->packet_id != 0u || !packet->subscriptions || packet->subscription_count == 0u ||
      !flowie_mqtt_client_span_valid(packet->properties))
    return TURBO_EINVAL;
  rc = flowie_mqtt_client_size_array(&total, packet->subscription_count,
                                     sizeof(flowie_mqtt_subscription_t));
  if (rc != TURBO_OK) return rc;
  array_size = total;
  rc = flowie_mqtt_client_size_add(&total, packet->properties.size);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < packet->subscription_count; ++i) {
    if (!flowie_mqtt_client_span_valid(packet->subscriptions[i].filter)) return TURBO_EINVAL;
    rc = flowie_mqtt_client_size_add(&total, packet->subscriptions[i].filter.size);
    if (rc != TURBO_OK) return rc;
  }
  rc = flowie_mqtt_client_command_allocate(command, total, &cursor);
  if (rc != TURBO_OK) return rc;
  subscriptions = (flowie_mqtt_subscription_t *)cursor;
  memcpy(subscriptions, packet->subscriptions, array_size);
  cursor += array_size;
  command->packet.subscribe = *packet;
  command->packet.subscribe.subscriptions = subscriptions;
  flowie_mqtt_client_copy_span(packet->properties, &cursor, &command->packet.subscribe.properties);
  for (size_t i = 0u; i < packet->subscription_count; ++i)
    flowie_mqtt_client_copy_span(packet->subscriptions[i].filter, &cursor,
                                 &subscriptions[i].filter);
  return TURBO_OK;
}

static int flowie_mqtt_client_clone_unsubscribe(flowie_mqtt_client_command_t *command,
                                                const flowie_mqtt_unsubscribe_packet_t *packet) {
  flowie_mqtt_span_t *filters;
  size_t array_size;
  size_t total = 0u;
  uint8_t *cursor;
  int rc;
  if (packet->size < sizeof(*packet) || packet->abi_version != FLOWIE_MQTT_PROTOCOL_ABI_V1 ||
      packet->packet_id != 0u || !packet->filters || packet->filter_count == 0u ||
      !flowie_mqtt_client_span_valid(packet->properties))
    return TURBO_EINVAL;
  rc = flowie_mqtt_client_size_array(&total, packet->filter_count, sizeof(flowie_mqtt_span_t));
  if (rc != TURBO_OK) return rc;
  array_size = total;
  rc = flowie_mqtt_client_size_add(&total, packet->properties.size);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < packet->filter_count; ++i) {
    if (!flowie_mqtt_client_span_valid(packet->filters[i])) return TURBO_EINVAL;
    rc = flowie_mqtt_client_size_add(&total, packet->filters[i].size);
    if (rc != TURBO_OK) return rc;
  }
  rc = flowie_mqtt_client_command_allocate(command, total, &cursor);
  if (rc != TURBO_OK) return rc;
  filters = (flowie_mqtt_span_t *)cursor;
  memcpy(filters, packet->filters, array_size);
  cursor += array_size;
  command->packet.unsubscribe = *packet;
  command->packet.unsubscribe.filters = filters;
  flowie_mqtt_client_copy_span(packet->properties, &cursor,
                               &command->packet.unsubscribe.properties);
  for (size_t i = 0u; i < packet->filter_count; ++i)
    flowie_mqtt_client_copy_span(packet->filters[i], &cursor, &filters[i]);
  return TURBO_OK;
}

static uint16_t flowie_mqtt_client_packet_id(flowie_mqtt_client_t *client) {
  uint16_t packet_id = client->next_packet_id;
  ++client->next_packet_id;
  if (client->next_packet_id == 0u) client->next_packet_id = 1u;
  return packet_id;
}

static int flowie_mqtt_client_send(flowie_mqtt_client_t *client, size_t written) {
  if (!client || !client->socket || written == 0u) return TURBO_EINVAL;
  if (written > client->outbound_max_packet_size) return TURBO_EMSGSIZE;
  return coro_socket_send(client->socket, client->send_buffer, written);
}

static int flowie_mqtt_client_negotiate_connack(flowie_mqtt_client_t *client,
                                                const flowie_mqtt_control_packet_view_t *connack) {
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  int rc;
  if (!client || !connack) return TURBO_EINVAL;
  client->outbound_max_packet_size = client->max_packet_size;
  client->server_receive_maximum = UINT16_MAX;
  client->server_topic_alias_maximum = 0u;
  client->server_keep_alive = 0u;
  client->server_maximum_qos = 2u;
  client->server_retain_available = 1u;
  if (client->version != FLOWIE_MQTT_VERSION_5 || connack->properties.values.size == 0u)
    return TURBO_OK;
  rc = flowie_mqtt_client_auth_method_matches(client, &connack->properties, 0);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_property_iterator_init(&connack->properties, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  while ((rc = flowie_mqtt_property_iterator_next(&iterator, &property)) == FLOWIE_MQTT_PARSE_OK) {
    switch (property.identifier) {
    case FLOWIE_MQTT_PROPERTY_RECEIVE_MAXIMUM:
      client->server_receive_maximum = (uint16_t)property.integer;
      break;
    case FLOWIE_MQTT_PROPERTY_MAXIMUM_PACKET_SIZE:
      if ((size_t)property.integer < client->outbound_max_packet_size)
        client->outbound_max_packet_size = property.integer;
      break;
    case FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS_MAXIMUM:
      client->server_topic_alias_maximum = (uint16_t)property.integer;
      break;
    case FLOWIE_MQTT_PROPERTY_SERVER_KEEP_ALIVE:
      client->server_keep_alive = (uint16_t)property.integer;
      break;
    case FLOWIE_MQTT_PROPERTY_MAXIMUM_QOS:
      client->server_maximum_qos = (uint8_t)property.integer;
      break;
    case FLOWIE_MQTT_PROPERTY_RETAIN_AVAILABLE:
      client->server_retain_available = (uint8_t)property.integer;
      break;
    default:
      break;
    }
  }
  return rc == FLOWIE_MQTT_PARSE_NEED_MORE ? TURBO_OK : TURBO_EPROTO;
}

static int
flowie_mqtt_client_publish_capabilities_validate(const flowie_mqtt_client_t *client,
                                                 const flowie_mqtt_publish_packet_t *packet) {
  flowie_mqtt_property_block_view_t block = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  flowie_mqtt_property_iterator_t iterator = FLOWIE_MQTT_PROPERTY_ITERATOR_INIT;
  flowie_mqtt_property_view_t property = FLOWIE_MQTT_PROPERTY_VIEW_INIT;
  int rc;
  if (!client || !packet) return TURBO_EINVAL;
  if (client->version != FLOWIE_MQTT_VERSION_5) return TURBO_OK;
  if (packet->qos > client->server_maximum_qos ||
      (packet->retain && !client->server_retain_available))
    return TURBO_ENOTSUP;
  if (packet->properties.size == 0u) return TURBO_OK;
  block.values = packet->properties;
  rc = flowie_mqtt_property_iterator_init(&block, &iterator);
  if (rc != FLOWIE_MQTT_PARSE_OK) return TURBO_EPROTO;
  while ((rc = flowie_mqtt_property_iterator_next(&iterator, &property)) == FLOWIE_MQTT_PARSE_OK) {
    if (property.identifier == FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS &&
        (property.integer == 0u || property.integer > client->server_topic_alias_maximum))
      return TURBO_ENOTSUP;
  }
  return rc == FLOWIE_MQTT_PARSE_NEED_MORE ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_mqtt_client_send_control(flowie_mqtt_client_t *client,
                                           flowie_mqtt_packet_type_t type, uint16_t packet_id,
                                           uint8_t reason_code) {
  flowie_mqtt_control_packet_t packet = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  size_t written = 0u;
  int rc;
  packet.version = client->version;
  packet.type = type;
  packet.packet_id = packet_id;
  packet.reason_code = reason_code;
  rc = flowie_mqtt_control_packet_encode(&packet, (uint8_t *)client->send_buffer,
                                         client->outbound_max_packet_size, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) return flowie_mqtt_client_parse_status(rc);
  return flowie_mqtt_client_send(client, written);
}

static int flowie_mqtt_client_send_auth(flowie_mqtt_client_t *client, uint8_t reason_code,
                                        flowie_mqtt_span_t properties) {
  flowie_mqtt_control_packet_t packet = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  size_t written = 0u;
  int rc;
  if (!client || client->version != FLOWIE_MQTT_VERSION_5 ||
      !flowie_mqtt_client_span_valid(properties))
    return TURBO_EINVAL;
  packet.version = FLOWIE_MQTT_VERSION_5;
  packet.type = FLOWIE_MQTT_PACKET_AUTH;
  packet.reason_code = reason_code;
  packet.properties = properties;
  rc = flowie_mqtt_control_packet_encode(&packet, (uint8_t *)client->send_buffer,
                                         client->outbound_max_packet_size, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) return flowie_mqtt_client_parse_status(rc);
  return flowie_mqtt_client_send(client, written);
}

static int flowie_mqtt_client_receive_packet(flowie_mqtt_client_t *client,
                                             flowie_mqtt_packet_view_t *out) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  if (!client || !out) return TURBO_EINVAL;
  if (client->pending_packet_size != 0u) {
    int rc = turbo_byte_buffer_consume(&client->framing, client->pending_packet_size);
    if (rc != TURBO_OK) return rc;
    client->pending_packet_size = 0u;
  }
  options.version = client->version;
  options.max_packet_size = client->max_packet_size;
  for (;;) {
    turbo_byte_buffer_view_t bytes;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    size_t consumed = 0u;
    int rc = turbo_byte_buffer_view(&client->framing, &bytes);
    if (rc != TURBO_OK) return rc;
    if (bytes.size != 0u) {
      rc = flowie_mqtt_packet_parse(bytes.data, bytes.size, &options, &packet, &consumed, NULL);
      if (rc == FLOWIE_MQTT_PARSE_OK) {
        if (consumed == 0u || consumed > bytes.size) return TURBO_EPROTO;
        client->pending_packet_size = consumed;
        *out = packet;
        return TURBO_OK;
      }
      if (rc != FLOWIE_MQTT_PARSE_NEED_MORE) return flowie_mqtt_client_parse_status(rc);
      if (bytes.size == client->max_packet_size) return TURBO_EMSGSIZE;
    }
    if (client->recv_data) {
      size_t remaining = client->recv_size - client->recv_offset;
      size_t available = turbo_byte_buffer_available(&client->framing);
      size_t chunk = remaining < available ? remaining : available;
      if (chunk == 0u) return TURBO_EMSGSIZE;
      rc = turbo_byte_buffer_append(&client->framing, client->recv_data + client->recv_offset,
                                    chunk);
      if (rc != TURBO_OK) return rc;
      client->recv_offset += chunk;
      if (client->recv_offset == client->recv_size) flowie_mqtt_client_recv_release(client);
      continue;
    }
    rc = coro_socket_recv(client->socket, &client->recv_data, &client->recv_size);
    if (rc != TURBO_OK) {
      if (rc == TURBO_EINTR && client->interrupt_pending) {
        client->interrupt_pending = 0;
        /* Command submission uses EINTR only to leave the idle receive. If the
         * wake raced with normal input, its retained interrupt can reach the
         * command that consumed that input; keep waiting for that command's
         * protocol response instead of failing it. */
        if (client->busy) continue;
      }
      return rc;
    }
    client->recv_offset = 0u;
    if (!client->recv_data && client->recv_size == 0u) {
      if (client->interrupt_pending) {
        client->interrupt_pending = 0;
        if (client->busy) continue;
        return TURBO_EINTR;
      }
      return TURBO_ECONNRESET;
    }
    if (!client->recv_data || client->recv_size == 0u) {
      flowie_mqtt_client_recv_release(client);
      return TURBO_EPROTO;
    }
  }
}

static int flowie_mqtt_client_handle_publish(flowie_mqtt_client_t *client,
                                             const flowie_mqtt_packet_view_t *packet) {
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  size_t match_count = 0u;
  int rc = flowie_mqtt_publish_parse(packet, &publish);
  if (rc != FLOWIE_MQTT_PARSE_OK) return flowie_mqtt_client_parse_status(rc);
  if (publish.qos == 2u &&
      flowie_mqtt_packet_id_set_t_contains(&client->inbound_qos2, publish.packet_id)) {
    if (!publish.duplicate) return TURBO_EPROTO;
    return flowie_mqtt_client_send_control(client, FLOWIE_MQTT_PACKET_PUBREC, publish.packet_id,
                                           0u);
  }
  if (publish.qos == 2u) {
    if (flowie_mqtt_packet_id_set_t_size(&client->inbound_qos2) >= client->max_inbound_qos2)
      return TURBO_ENOSPC;
    rc = flowie_mqtt_packet_id_set_t_add(&client->inbound_qos2, publish.packet_id);
    if (rc != TURBO_OK) return rc;
  }
  client->callback_active = 1;
  for (size_t i = 0u; i < client->topic_handler_count; ++i) {
    int matched = 0;
    rc = flowie_mqtt_topic_matches(client->topic_handlers[i].filter, publish.topic, &matched);
    if (rc != FLOWIE_MQTT_PARSE_OK) {
      rc = flowie_mqtt_client_parse_status(rc);
      break;
    }
    if (!matched) continue;
    ++match_count;
    rc = client->topic_handlers[i].on_message(client, &publish, client->user_data);
    if (rc != TURBO_OK) break;
  }
  client->callback_active = 0;
  if (rc == TURBO_OK && match_count == 0u) rc = TURBO_ENOTSUP;
  if (rc != TURBO_OK) {
    if (publish.qos == 2u)
      (void)flowie_mqtt_packet_id_set_t_remove(&client->inbound_qos2, publish.packet_id);
    return rc;
  }
  if (publish.qos == 1u)
    return flowie_mqtt_client_send_control(client, FLOWIE_MQTT_PACKET_PUBACK, publish.packet_id,
                                           0u);
  if (publish.qos == 2u)
    return flowie_mqtt_client_send_control(client, FLOWIE_MQTT_PACKET_PUBREC, publish.packet_id,
                                           0u);
  return TURBO_OK;
}

static int flowie_mqtt_client_handle_pubrel(flowie_mqtt_client_t *client,
                                            const flowie_mqtt_packet_view_t *packet) {
  flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  uint8_t reason_code = 0u;
  int rc = flowie_mqtt_control_packet_parse(packet, &control);
  if (rc != FLOWIE_MQTT_PARSE_OK) return flowie_mqtt_client_parse_status(rc);
  if (!flowie_mqtt_packet_id_set_t_remove(&client->inbound_qos2, control.packet_id) &&
      client->version == FLOWIE_MQTT_VERSION_5)
    reason_code = 0x92u;
  return flowie_mqtt_client_send_control(client, FLOWIE_MQTT_PACKET_PUBCOMP, control.packet_id,
                                         reason_code);
}

static int
flowie_mqtt_client_handle_auth_challenge(flowie_mqtt_client_t *client,
                                         const flowie_mqtt_packet_view_t *packet,
                                         flowie_mqtt_control_packet_view_t *challenge_out) {
  flowie_mqtt_control_packet_view_t challenge = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  flowie_mqtt_client_auth_response_t response = FLOWIE_MQTT_CLIENT_AUTH_RESPONSE_INIT;
  int rc;
  if (!client || !packet || packet->type != FLOWIE_MQTT_PACKET_AUTH ||
      client->version != FLOWIE_MQTT_VERSION_5)
    return TURBO_EINVAL;
  rc = flowie_mqtt_control_packet_parse(packet, &challenge);
  if (rc != FLOWIE_MQTT_PARSE_OK) return flowie_mqtt_client_parse_status(rc);
  if (challenge.reason_code != 0x18u) return TURBO_EPROTO;
  rc = flowie_mqtt_client_auth_method_matches(client, &challenge.properties, 1);
  if (rc != TURBO_OK) return rc;
  if (!client->on_auth_challenge) return TURBO_ENOTSUP;
  client->callback_active = 1;
  rc = client->on_auth_challenge(client, &challenge, &response, client->user_data);
  client->callback_active = 0;
  if (rc != TURBO_OK) return rc;
  if (response.size != sizeof(response) || response.reason_code != 0x18u ||
      !flowie_mqtt_client_span_valid(response.properties))
    return TURBO_EPROTO;
  {
    flowie_mqtt_property_block_view_t properties = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    properties.values = response.properties;
    rc = flowie_mqtt_client_auth_method_matches(client, &properties, 1);
    if (rc != TURBO_OK) return rc;
  }
  rc = flowie_mqtt_client_send_auth(client, response.reason_code, response.properties);
  if (rc == TURBO_OK && challenge_out) *challenge_out = challenge;
  return rc;
}

static int flowie_mqtt_client_handle_unsolicited(flowie_mqtt_client_t *client,
                                                 const flowie_mqtt_packet_view_t *packet,
                                                 int *handled) {
  if (!handled) return TURBO_EINVAL;
  *handled = 1;
  switch (packet->type) {
  case FLOWIE_MQTT_PACKET_PUBLISH:
    return flowie_mqtt_client_handle_publish(client, packet);
  case FLOWIE_MQTT_PACKET_PUBREL:
    return flowie_mqtt_client_handle_pubrel(client, packet);
  case FLOWIE_MQTT_PACKET_DISCONNECT:
    return TURBO_ECONNRESET;
  case FLOWIE_MQTT_PACKET_AUTH:
    return TURBO_EPROTO;
  default:
    *handled = 0;
    return TURBO_OK;
  }
}

static int flowie_mqtt_client_wait_control(flowie_mqtt_client_t *client,
                                           flowie_mqtt_packet_type_t expected, uint16_t packet_id,
                                           flowie_mqtt_control_packet_view_t *out) {
  for (;;) {
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
    int handled = 0;
    int rc = flowie_mqtt_client_receive_packet(client, &packet);
    if (rc != TURBO_OK) return rc;
    if (expected == FLOWIE_MQTT_PACKET_CONNACK && packet.type == FLOWIE_MQTT_PACKET_AUTH) {
      rc = flowie_mqtt_client_handle_auth_challenge(client, &packet, NULL);
      if (rc != TURBO_OK) return rc;
      continue;
    }
    rc = flowie_mqtt_client_handle_unsolicited(client, &packet, &handled);
    if (rc != TURBO_OK) return rc;
    if (handled) continue;
    if (packet.type != expected) return TURBO_EPROTO;
    rc = flowie_mqtt_control_packet_parse(&packet, &control);
    if (rc != FLOWIE_MQTT_PARSE_OK) return flowie_mqtt_client_parse_status(rc);
    if (control.packet_id != packet_id) return TURBO_EPROTO;
    if (out) *out = control;
    return TURBO_OK;
  }
}

static void flowie_mqtt_client_complete(flowie_mqtt_client_t *client,
                                        flowie_mqtt_client_command_t *command, int status,
                                        const flowie_mqtt_control_packet_view_t *response) {
  client->callback_active = 1;
  command->completion(client, status, response, command->user_data);
  client->callback_active = 0;
}

static void flowie_mqtt_client_report_error(flowie_mqtt_client_t *client, int status) {
  if (!client->on_error) return;
  client->callback_active = 1;
  client->on_error(client, status, client->user_data);
  client->callback_active = 0;
}

static int flowie_mqtt_client_command_pop(flowie_mqtt_client_t *client,
                                          flowie_mqtt_client_command_t **out) {
  int stopping;
  turbo_mutex_lock(&client->command_mutex);
  stopping = client->stopping;
  if (!stopping) {
    if (!flowie_mqtt_client_command_queue_t_pop_front(&client->commands, out)) {
      *out = NULL;
    } else {
      client->command_queue_bytes -= sizeof(**out) + (*out)->owned_size;
    }
  }
  turbo_mutex_unlock(&client->command_mutex);
  return stopping;
}

static void flowie_mqtt_client_cancel_commands(flowie_mqtt_client_t *client, int status) {
  for (;;) {
    flowie_mqtt_client_command_t *command = NULL;
    turbo_mutex_lock(&client->command_mutex);
    (void)flowie_mqtt_client_command_queue_t_pop_front(&client->commands, &command);
    if (command) client->command_queue_bytes -= sizeof(*command) + command->owned_size;
    turbo_mutex_unlock(&client->command_mutex);
    if (!command) return;
    flowie_mqtt_client_complete(client, command, status, NULL);
    flowie_mqtt_client_command_destroy(command);
  }
}

static int flowie_mqtt_client_is_stopping(flowie_mqtt_client_t *client) {
  int stopping;
  turbo_mutex_lock(&client->command_mutex);
  stopping = client->stopping;
  turbo_mutex_unlock(&client->command_mutex);
  return stopping;
}

static void flowie_mqtt_client_run_command(flowie_mqtt_client_t *client,
                                           flowie_mqtt_client_command_t *command) {
  flowie_mqtt_control_packet_view_t response = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  const flowie_mqtt_control_packet_view_t *response_ptr = NULL;
  int rc;
  switch (command->type) {
  case FLOWIE_MQTT_CLIENT_COMMAND_CONNECT:
    rc = flowie_mqtt_client_connect_operation(client, &command->packet.connect, &response);
    if (rc == TURBO_OK) response_ptr = &response;
    break;
  case FLOWIE_MQTT_CLIENT_COMMAND_PUBLISH:
    rc = flowie_mqtt_client_publish_operation(client, &command->packet.publish, &response);
    if (rc == TURBO_OK && command->packet.publish.qos != 0u) response_ptr = &response;
    break;
  case FLOWIE_MQTT_CLIENT_COMMAND_SUBSCRIBE:
    rc = flowie_mqtt_client_subscribe_operation(client, &command->packet.subscribe, &response);
    if (rc == TURBO_OK) response_ptr = &response;
    break;
  case FLOWIE_MQTT_CLIENT_COMMAND_UNSUBSCRIBE:
    rc = flowie_mqtt_client_unsubscribe_operation(client, &command->packet.unsubscribe, &response);
    if (rc == TURBO_OK) response_ptr = &response;
    break;
  case FLOWIE_MQTT_CLIENT_COMMAND_PING:
    rc = flowie_mqtt_client_ping_operation(client);
    break;
  case FLOWIE_MQTT_CLIENT_COMMAND_AUTH:
    rc = flowie_mqtt_client_auth_operation(client, command->packet.control.properties, &response);
    if (rc == TURBO_OK) response_ptr = &response;
    break;
  case FLOWIE_MQTT_CLIENT_COMMAND_DISCONNECT:
    rc = flowie_mqtt_client_disconnect_operation(client, command->packet.control.reason_code,
                                                 command->packet.control.properties);
    if (rc == TURBO_EOF || rc == TURBO_ECONNRESET) rc = TURBO_OK;
    break;
  default:
    rc = TURBO_EINVAL;
    break;
  }
  if (rc != TURBO_OK && flowie_mqtt_client_is_stopping(client)) {
    rc = TURBO_ESHUTDOWN;
    response_ptr = NULL;
  }
  flowie_mqtt_client_complete(client, command, rc, response_ptr);
}

static void flowie_mqtt_client_worker_pump(coro_t *co, void *arg) {
  flowie_mqtt_client_t *client = (flowie_mqtt_client_t *)arg;
  (void)co;
  for (;;) {
    flowie_mqtt_client_command_t *command = NULL;
    if (flowie_mqtt_client_command_pop(client, &command)) {
      flowie_mqtt_client_transport_close(client, 1);
      flowie_mqtt_client_cancel_commands(client, TURBO_ESHUTDOWN);
      client->pump_active = 0;
      coro_context_set_persistent(client->context, 0);
      coro_context_stop(client->context);
      return;
    }
    if (command) {
      flowie_mqtt_client_run_command(client, command);
      flowie_mqtt_client_command_destroy(command);
      continue;
    }
    if (client->state == FLOWIE_MQTT_CLIENT_CONNECTED) {
      flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
      int handled = 0;
      int rc;
      client->idle_receive = 1;
      rc = flowie_mqtt_client_receive_packet(client, &packet);
      client->idle_receive = 0;
      if (rc == TURBO_EINTR) continue;
      if (rc == TURBO_OK) {
        rc = flowie_mqtt_client_handle_unsolicited(client, &packet, &handled);
        if (rc == TURBO_OK && !handled && packet.type != FLOWIE_MQTT_PACKET_PINGRESP)
          rc = TURBO_EPROTO;
      }
      if (rc != TURBO_OK) {
        flowie_mqtt_client_transport_close(client, 1);
        flowie_mqtt_client_report_error(client, rc);
      }
      continue;
    }
    turbo_mutex_lock(&client->command_mutex);
    if (!client->stopping && flowie_mqtt_client_command_queue_t_empty(&client->commands)) {
      client->pump_active = 0;
      turbo_mutex_unlock(&client->command_mutex);
      return;
    }
    turbo_mutex_unlock(&client->command_mutex);
  }
}

static void flowie_mqtt_client_worker_wake(void *arg1, void *arg2) {
  flowie_mqtt_client_t *client = (flowie_mqtt_client_t *)arg1;
  int stopping;
  int rc;
  (void)arg2;
  turbo_mutex_lock(&client->command_mutex);
  stopping = client->stopping;
  turbo_mutex_unlock(&client->command_mutex);
  if (!client->pump_active) {
    client->pump_active = 1;
    rc = coro_context_spawn(client->context, flowie_mqtt_client_worker_pump, client);
    if (rc != TURBO_OK) {
      turbo_mutex_lock(&client->command_mutex);
      client->stopping = 1;
      turbo_mutex_unlock(&client->command_mutex);
      client->pump_active = 0;
      flowie_mqtt_client_cancel_commands(client, rc);
      coro_context_set_persistent(client->context, 0);
      coro_context_stop(client->context);
    }
    return;
  }
  if (stopping && client->socket) {
    /* Destruction owns the transport now. Closing on the loop thread wakes an
     * active command without relying on a second, nested context post. */
    flowie_mqtt_client_transport_close(client, 1);
  } else if (client->idle_receive && client->socket) {
    client->interrupt_pending = 1;
    rc = coro_socket_interrupt_wait(client->socket, TURBO_EINTR);
    if (rc != TURBO_OK) client->interrupt_pending = 0;
  }
}

static void flowie_mqtt_client_worker(void *arg) {
  flowie_mqtt_client_t *client = (flowie_mqtt_client_t *)arg;
  (void)coro_context_run(client->context, TURBO_RUN_DEFAULT);
}

static int flowie_mqtt_client_submit(flowie_mqtt_client_t *client,
                                     flowie_mqtt_client_command_t *command) {
  size_t charge;
  size_t queue_size;
  int rc;
  if (!client || !command) return TURBO_EINVAL;
  if (command->owned_size > SIZE_MAX - sizeof(*command)) return TURBO_EMSGSIZE;
  charge = sizeof(*command) + command->owned_size;
  turbo_mutex_lock(&client->command_mutex);
  queue_size = flowie_mqtt_client_command_queue_t_size(&client->commands);
  if (client->stopping) {
    rc = TURBO_ESHUTDOWN;
  } else if (queue_size >= client->command_queue_capacity) {
    rc = TURBO_ENOSPC;
  } else if (charge > client->command_queue_max_bytes - client->command_queue_bytes) {
    rc = TURBO_ENOSPC;
  } else {
    rc = flowie_mqtt_client_command_queue_t_push_back(&client->commands, command);
    if (rc == TURBO_OK) {
      client->command_queue_bytes += charge;
      rc = coro_post(client->context, flowie_mqtt_client_worker_wake, client, NULL);
      if (rc != TURBO_OK) {
        flowie_mqtt_client_command_t *rolled_back = NULL;
        (void)flowie_mqtt_client_command_queue_t_pop_back(&client->commands, &rolled_back);
        client->command_queue_bytes -= charge;
      }
    }
  }
  turbo_mutex_unlock(&client->command_mutex);
  return rc;
}

static int flowie_mqtt_client_submit_many(flowie_mqtt_client_t *client,
                                          flowie_mqtt_client_command_t *const *commands,
                                          size_t command_count) {
  size_t charge = 0u;
  size_t inserted = 0u;
  size_t queue_size;
  int rc = TURBO_OK;
  if (!client || !commands || command_count == 0u) return TURBO_EINVAL;
  for (size_t i = 0u; i < command_count; ++i) {
    size_t command_charge;
    if (!commands[i] || commands[i]->owned_size > SIZE_MAX - sizeof(*commands[i]))
      return TURBO_EMSGSIZE;
    command_charge = sizeof(*commands[i]) + commands[i]->owned_size;
    if (command_charge > SIZE_MAX - charge) return TURBO_EMSGSIZE;
    charge += command_charge;
  }

  turbo_mutex_lock(&client->command_mutex);
  queue_size = flowie_mqtt_client_command_queue_t_size(&client->commands);
  if (client->stopping) {
    rc = TURBO_ESHUTDOWN;
  } else if (queue_size > client->command_queue_capacity ||
             command_count > client->command_queue_capacity - queue_size) {
    rc = TURBO_ENOSPC;
  } else if (client->command_queue_bytes > client->command_queue_max_bytes ||
             charge > client->command_queue_max_bytes - client->command_queue_bytes) {
    rc = TURBO_ENOSPC;
  } else {
    for (; inserted < command_count; ++inserted) {
      rc = flowie_mqtt_client_command_queue_t_push_back(&client->commands, commands[inserted]);
      if (rc != TURBO_OK) break;
    }
    if (rc == TURBO_OK) {
      client->command_queue_bytes += charge;
      rc = coro_post(client->context, flowie_mqtt_client_worker_wake, client, NULL);
      if (rc == TURBO_OK) {
        turbo_mutex_unlock(&client->command_mutex);
        return TURBO_OK;
      }
      client->command_queue_bytes -= charge;
    }
    while (inserted != 0u) {
      flowie_mqtt_client_command_t *rolled_back = NULL;
      --inserted;
      (void)flowie_mqtt_client_command_queue_t_pop_back(&client->commands, &rolled_back);
    }
  }
  turbo_mutex_unlock(&client->command_mutex);
  return rc;
}

int flowie_mqtt_client_create(const flowie_mqtt_client_config_t *config,
                              flowie_mqtt_client_t **out) {
  const flowie_mqtt_client_tls_config_t *tls;
  flowie_mqtt_client_t *client;
  size_t max_packet_size;
  int rc;
  if (out) *out = NULL;
  if (!out) return TURBO_EINVAL;
  rc = flowie_mqtt_client_config_validate(config);
  if (rc != TURBO_OK) return rc;
  max_packet_size = config->max_packet_size ? config->max_packet_size
                                            : FLOWIE_MQTT_CLIENT_DEFAULT_MAX_PACKET_SIZE;
  client = (flowie_mqtt_client_t *)calloc(1, sizeof(*client));
  if (!client) return TURBO_ENOMEM;
  atomic_init(&client->public_connected, 0);
  client->transport = config->transport;
  client->port = config->port;
  client->timeout_ms =
      config->timeout_ms ? config->timeout_ms : FLOWIE_MQTT_CLIENT_DEFAULT_TIMEOUT_MS;
  client->max_packet_size = max_packet_size;
  client->outbound_max_packet_size = max_packet_size;
  client->max_inbound_qos2 = config->max_inbound_qos2;
  client->stream_recv_buffer_bytes = config->stream_recv_buffer_bytes;
  client->socket_recv_buffer_bytes = config->socket_recv_buffer_bytes;
  client->socket_send_buffer_bytes = config->socket_send_buffer_bytes;
  client->server_receive_maximum = UINT16_MAX;
  client->server_maximum_qos = 2u;
  client->server_retain_available = 1u;
  rc = flowie_mqtt_client_clone_topic_handlers(client, &config->topic_handlers);
  if (rc != TURBO_OK) goto fail;
  client->on_connect = config->on_connect;
  client->on_publish = config->on_publish;
  client->on_subscribe = config->on_subscribe;
  client->on_unsubscribe = config->on_unsubscribe;
  client->on_ping = config->on_ping;
  client->on_auth_challenge = config->on_auth_challenge;
  client->on_auth = config->on_auth;
  client->on_disconnect = config->on_disconnect;
  client->on_error = config->on_error;
  client->user_data = config->user_data;
  client->command_queue_capacity = config->command_queue_capacity
                                       ? config->command_queue_capacity
                                       : FLOWIE_MQTT_CLIENT_DEFAULT_COMMAND_QUEUE_CAPACITY;
  client->command_queue_max_bytes = config->command_queue_max_bytes
                                        ? config->command_queue_max_bytes
                                        : FLOWIE_MQTT_CLIENT_DEFAULT_COMMAND_QUEUE_BYTES;
  client->next_packet_id = 1u;
  client->host = tstr_dup(config->host);
  client->path = tstr_dup(config->path ? config->path : "/mqtt");
  tls = flowie_mqtt_client_tls_config(config);
  if (tls) {
    client->tls_ca_file = tls->ca_file ? tstr_dup(tls->ca_file) : NULL;
    client->tls_cert_file = tls->cert_file ? tstr_dup(tls->cert_file) : NULL;
    client->tls_key_file = tls->key_file ? tstr_dup(tls->key_file) : NULL;
    client->tls_key_password = tls->key_password ? tstr_dup(tls->key_password) : NULL;
    client->tls_configured = tls->ca_file || tls->cert_file || tls->key_file || tls->key_password;
  }
  client->send_buffer = tstr_new_len(NULL, max_packet_size);
  if (!client->host || !client->path || !client->send_buffer ||
      (tls &&
       ((tls->ca_file && !client->tls_ca_file) || (tls->cert_file && !client->tls_cert_file) ||
        (tls->key_file && !client->tls_key_file) ||
        (tls->key_password && !client->tls_key_password)))) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  rc = turbo_byte_buffer_init(&client->framing, max_packet_size);
  if (rc != TURBO_OK) goto fail;
  client->framing_initialized = 1;
  rc = flowie_mqtt_packet_id_set_t_init(&client->inbound_qos2);
  if (rc != TURBO_OK) goto fail;
  client->qos2_initialized = 1;
  rc = flowie_mqtt_packet_id_set_t_reserve(&client->inbound_qos2, client->max_inbound_qos2);
  if (rc != TURBO_OK) goto fail;
  turbo_mutex_init(&client->command_mutex);
  client->sync_initialized = 1;
  rc = flowie_mqtt_client_command_queue_t_init(&client->commands);
  if (rc != TURBO_OK) goto fail;
  client->command_queue_initialized = 1;
  rc =
      flowie_mqtt_client_command_queue_t_reserve(&client->commands, client->command_queue_capacity);
  if (rc != TURBO_OK) goto fail;
  client->context = coro_context_create(NULL);
  if (!client->context) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  if (client->stream_recv_buffer_bytes != 0u) {
    rc = coro_context_set_stream_recv_buffer_size(client->context,
                                                  client->stream_recv_buffer_bytes);
    if (rc != TURBO_OK) goto fail;
  }
  coro_context_set_persistent(client->context, 1);
  rc = turbo_thread_create(&client->worker, flowie_mqtt_client_worker, client);
  if (rc != TURBO_OK) goto fail;
  client->worker_started = 1;
  *out = client;
  return TURBO_OK;

fail:
  flowie_mqtt_client_destroy(client);
  return rc;
}

void flowie_mqtt_client_destroy(flowie_mqtt_client_t *client) {
  flowie_mqtt_client_command_t *command = NULL;
  if (!client) return;
  if (client->worker_started) {
    int rc;
    if (coro_context_current() == client->context) {
      turbo_mutex_lock(&client->command_mutex);
      client->stopping = 1;
      turbo_mutex_unlock(&client->command_mutex);
      (void)coro_post(client->context, flowie_mqtt_client_worker_wake, client, NULL);
      return;
    }
    turbo_mutex_lock(&client->command_mutex);
    client->stopping = 1;
    turbo_mutex_unlock(&client->command_mutex);
    do {
      rc = coro_post(client->context, flowie_mqtt_client_worker_wake, client, NULL);
      if (rc == TURBO_ENOMEM) turbo_thread_yield();
    } while (rc == TURBO_ENOMEM);
    if (rc != TURBO_OK) coro_context_stop(client->context);
    if (turbo_thread_join(&client->worker) != TURBO_OK) return;
    turbo_thread_destroy(&client->worker);
    client->worker_started = 0;
  }
  flowie_mqtt_client_transport_close(client, 1);
  if (client->command_queue_initialized) {
    while (flowie_mqtt_client_command_queue_t_pop_front(&client->commands, &command))
      flowie_mqtt_client_command_destroy(command);
    flowie_mqtt_client_command_queue_t_destroy(&client->commands);
  }
  if (client->sync_initialized) turbo_mutex_destroy(&client->command_mutex);
  if (client->context) coro_context_destroy(client->context);
  if (client->qos2_initialized) flowie_mqtt_packet_id_set_t_destroy(&client->inbound_qos2);
  if (client->framing_initialized) turbo_byte_buffer_destroy(&client->framing);
  tstr_freep(&client->send_buffer);
  tstr_freep(&client->path);
  tstr_freep(&client->host);
  tstr_freep(&client->tls_ca_file);
  tstr_freep(&client->tls_cert_file);
  tstr_freep(&client->tls_key_file);
  if (client->tls_key_password) {
    crypto_wipe(client->tls_key_password, tstr_len(client->tls_key_password));
    tstr_freep(&client->tls_key_password);
  }
  free(client->topic_handlers);
  free(client);
}

static int flowie_mqtt_client_connect_operation(flowie_mqtt_client_t *client,
                                                const flowie_mqtt_connect_packet_t *packet,
                                                flowie_mqtt_control_packet_view_t *connack) {
  size_t written = 0u;
  int rc;
  if (!client || !packet || !connack || !flowie_mqtt_client_ack_output_valid(connack) ||
      !flowie_mqtt_version_is_supported(packet->version))
    return TURBO_EINVAL;
  rc = flowie_mqtt_client_begin(client, 0);
  if (rc != TURBO_OK) return rc;
  if (client->state != FLOWIE_MQTT_CLIENT_DISCONNECTED) {
    rc = TURBO_EALREADY;
    goto done;
  }
  rc = flowie_mqtt_connect_packet_encode(packet, (uint8_t *)client->send_buffer,
                                         client->max_packet_size, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    rc = flowie_mqtt_client_parse_status(rc);
    goto done;
  }
  rc = flowie_mqtt_client_auth_method_select(client, packet->version == FLOWIE_MQTT_VERSION_5
                                                         ? packet->properties
                                                         : (flowie_mqtt_span_t){0});
  if (rc != TURBO_OK) goto done;
  turbo_byte_buffer_reset(&client->framing);
  flowie_mqtt_packet_id_set_t_clear(&client->inbound_qos2);
  client->version = packet->version;
  client->socket = coro_socket_create(client->context,
                                      client->transport == FLOWIE_MQTT_CLIENT_TRANSPORT_TLS ||
                                              client->transport == FLOWIE_MQTT_CLIENT_TRANSPORT_WSS
                                          ? CORO_SOCKET_TLS
                                          : CORO_SOCKET_TCP_V4);
  if (!client->socket) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  if (client->socket_recv_buffer_bytes != 0u) {
    rc = coro_socket_set_recv_buffer_size(client->socket, client->socket_recv_buffer_bytes);
    if (rc != TURBO_OK) goto fail;
  }
  if (client->socket_send_buffer_bytes != 0u) {
    rc = coro_socket_set_send_buffer_size(client->socket, client->socket_send_buffer_bytes);
    if (rc != TURBO_OK) goto fail;
  }
  if (client->tls_configured) {
    turbo_tls_client_config_t tls_config = {0};
    tls_config.ca_file = client->tls_ca_file;
    tls_config.cert_file = client->tls_cert_file;
    tls_config.key_file = client->tls_key_file;
    tls_config.key_password = client->tls_key_password;
    tls_config.verify_peer = 1;
    rc = coro_socket_set_tls_client_config(client->socket, &tls_config);
    if (rc != TURBO_OK) goto fail;
  }
  coro_socket_set_timeout(client->socket, client->timeout_ms);
  if (client->transport == FLOWIE_MQTT_CLIENT_TRANSPORT_WS ||
      client->transport == FLOWIE_MQTT_CLIENT_TRANSPORT_WSS) {
    rc = coro_socket_connect_ws_ex(client->socket, client->host, client->port, client->path,
                                   client->transport == FLOWIE_MQTT_CLIENT_TRANSPORT_WSS, "mqtt");
  } else {
    rc = coro_socket_connect(client->socket, client->host, client->port);
  }
  if (rc != TURBO_OK) goto fail;
  client->state = FLOWIE_MQTT_CLIENT_TRANSPORT_CONNECTED;
  rc = flowie_mqtt_client_send(client, written);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_mqtt_client_wait_control(client, FLOWIE_MQTT_PACKET_CONNACK, 0u, connack);
  if (rc != TURBO_OK) goto fail;
  if (connack->reason_code != 0u) {
    flowie_mqtt_client_transport_close(client, 0);
    rc = TURBO_OK;
    goto done;
  }
  rc = flowie_mqtt_client_negotiate_connack(client, connack);
  if (rc != TURBO_OK) goto fail;
  client->state = FLOWIE_MQTT_CLIENT_CONNECTED;
  atomic_store_explicit(&client->public_connected, 1, memory_order_release);
  rc = TURBO_OK;
  goto done;

fail:
  flowie_mqtt_client_transport_close(client, 1);
done:
  flowie_mqtt_client_end(client);
  return rc;
}

static int flowie_mqtt_client_publish_operation(flowie_mqtt_client_t *client,
                                                const flowie_mqtt_publish_packet_t *packet,
                                                flowie_mqtt_control_packet_view_t *ack) {
  flowie_mqtt_publish_packet_t encoded;
  flowie_mqtt_control_packet_view_t received = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  size_t written = 0u;
  uint16_t packet_id = 0u;
  int rc;
  if (!client || !packet || !flowie_mqtt_client_ack_output_valid(ack) || packet->packet_id != 0u)
    return TURBO_EINVAL;
  rc = flowie_mqtt_client_begin(client, 1);
  if (rc != TURBO_OK) return rc;
  encoded = *packet;
  if (encoded.version != client->version) {
    rc = TURBO_EPROTO;
    goto done;
  }
  rc = flowie_mqtt_client_publish_capabilities_validate(client, &encoded);
  if (rc != TURBO_OK) goto done;
  if (encoded.qos != 0u) {
    packet_id = flowie_mqtt_client_packet_id(client);
    encoded.packet_id = packet_id;
  }
  rc = flowie_mqtt_publish_packet_encode(&encoded, (uint8_t *)client->send_buffer,
                                         client->outbound_max_packet_size, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    rc = flowie_mqtt_client_parse_status(rc);
    goto done;
  }
  rc = flowie_mqtt_client_send(client, written);
  if (rc != TURBO_OK) goto fail;
  if (encoded.qos == 0u) {
    if (ack) *ack = received;
    goto done;
  }
  rc = flowie_mqtt_client_wait_control(
      client, encoded.qos == 1u ? FLOWIE_MQTT_PACKET_PUBACK : FLOWIE_MQTT_PACKET_PUBREC, packet_id,
      &received);
  if (rc != TURBO_OK) goto fail;
  if (encoded.qos == 1u || received.reason_code >= 0x80u) {
    if (ack) *ack = received;
    goto done;
  }
  rc = flowie_mqtt_client_send_control(client, FLOWIE_MQTT_PACKET_PUBREL, packet_id, 0u);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_mqtt_client_wait_control(client, FLOWIE_MQTT_PACKET_PUBCOMP, packet_id, &received);
  if (rc != TURBO_OK) goto fail;
  if (ack) *ack = received;
  goto done;

fail:
  flowie_mqtt_client_transport_close(client, 1);
done:
  flowie_mqtt_client_end(client);
  return rc;
}

static int flowie_mqtt_client_subscribe_operation(flowie_mqtt_client_t *client,
                                                  const flowie_mqtt_subscribe_packet_t *packet,
                                                  flowie_mqtt_control_packet_view_t *suback) {
  flowie_mqtt_subscribe_packet_t encoded;
  flowie_mqtt_control_packet_view_t received = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  size_t written = 0u;
  uint16_t packet_id;
  int rc;
  if (!client || !packet || !suback || !flowie_mqtt_client_ack_output_valid(suback) ||
      packet->packet_id != 0u)
    return TURBO_EINVAL;
  rc = flowie_mqtt_client_begin(client, 1);
  if (rc != TURBO_OK) return rc;
  encoded = *packet;
  if (encoded.version != client->version) {
    rc = TURBO_EPROTO;
    goto done;
  }
  packet_id = flowie_mqtt_client_packet_id(client);
  encoded.packet_id = packet_id;
  rc = flowie_mqtt_subscribe_packet_encode(&encoded, (uint8_t *)client->send_buffer,
                                           client->outbound_max_packet_size, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    rc = flowie_mqtt_client_parse_status(rc);
    goto done;
  }
  rc = flowie_mqtt_client_send(client, written);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_mqtt_client_wait_control(client, FLOWIE_MQTT_PACKET_SUBACK, packet_id, &received);
  if (rc != TURBO_OK) goto fail;
  if (received.reason_codes.size != encoded.subscription_count) {
    rc = TURBO_EPROTO;
    goto fail;
  }
  *suback = received;
  goto done;

fail:
  flowie_mqtt_client_transport_close(client, 1);
done:
  flowie_mqtt_client_end(client);
  return rc;
}

static int flowie_mqtt_client_unsubscribe_operation(flowie_mqtt_client_t *client,
                                                    const flowie_mqtt_unsubscribe_packet_t *packet,
                                                    flowie_mqtt_control_packet_view_t *unsuback) {
  flowie_mqtt_unsubscribe_packet_t encoded;
  flowie_mqtt_control_packet_view_t received = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  size_t written = 0u;
  uint16_t packet_id;
  int rc;
  if (!client || !packet || !unsuback || !flowie_mqtt_client_ack_output_valid(unsuback) ||
      packet->packet_id != 0u)
    return TURBO_EINVAL;
  rc = flowie_mqtt_client_begin(client, 1);
  if (rc != TURBO_OK) return rc;
  encoded = *packet;
  if (encoded.version != client->version) {
    rc = TURBO_EPROTO;
    goto done;
  }
  packet_id = flowie_mqtt_client_packet_id(client);
  encoded.packet_id = packet_id;
  rc = flowie_mqtt_unsubscribe_packet_encode(&encoded, (uint8_t *)client->send_buffer,
                                             client->outbound_max_packet_size, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    rc = flowie_mqtt_client_parse_status(rc);
    goto done;
  }
  rc = flowie_mqtt_client_send(client, written);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_mqtt_client_wait_control(client, FLOWIE_MQTT_PACKET_UNSUBACK, packet_id, &received);
  if (rc != TURBO_OK) goto fail;
  if (client->version == FLOWIE_MQTT_VERSION_5 &&
      received.reason_codes.size != encoded.filter_count) {
    rc = TURBO_EPROTO;
    goto fail;
  }
  *unsuback = received;
  goto done;

fail:
  flowie_mqtt_client_transport_close(client, 1);
done:
  flowie_mqtt_client_end(client);
  return rc;
}

static int flowie_mqtt_client_ping_operation(flowie_mqtt_client_t *client) {
  size_t written = 0u;
  int rc = flowie_mqtt_client_begin(client, 1);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_pingreq_encode(client->version, (uint8_t *)client->send_buffer,
                                  client->outbound_max_packet_size, &written);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    rc = flowie_mqtt_client_parse_status(rc);
    goto done;
  }
  rc = flowie_mqtt_client_send(client, written);
  if (rc == TURBO_OK)
    rc = flowie_mqtt_client_wait_control(client, FLOWIE_MQTT_PACKET_PINGRESP, 0u, NULL);
  if (rc != TURBO_OK) flowie_mqtt_client_transport_close(client, 1);
done:
  flowie_mqtt_client_end(client);
  return rc;
}

static int flowie_mqtt_client_auth_operation(flowie_mqtt_client_t *client,
                                             flowie_mqtt_span_t properties,
                                             flowie_mqtt_control_packet_view_t *auth) {
  flowie_mqtt_property_block_view_t auth_properties = FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
  int rc;
  if (!client || !auth || !flowie_mqtt_client_ack_output_valid(auth)) return TURBO_EINVAL;
  rc = flowie_mqtt_client_begin(client, 1);
  if (rc != TURBO_OK) return rc;
  if (client->version != FLOWIE_MQTT_VERSION_5) {
    rc = TURBO_ENOTSUP;
    goto done;
  }
  auth_properties.values = properties;
  rc = flowie_mqtt_client_auth_method_matches(client, &auth_properties, 1);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_mqtt_client_send_auth(client, 0x19u, properties);
  if (rc != TURBO_OK) goto fail;
  for (;;) {
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    int handled = 0;
    rc = flowie_mqtt_client_receive_packet(client, &packet);
    if (rc != TURBO_OK) goto fail;
    if (packet.type == FLOWIE_MQTT_PACKET_AUTH) {
      flowie_mqtt_control_packet_view_t response = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
      rc = flowie_mqtt_control_packet_parse(&packet, &response);
      if (rc != FLOWIE_MQTT_PARSE_OK) {
        rc = flowie_mqtt_client_parse_status(rc);
        goto fail;
      }
      if (response.reason_code == 0x18u) {
        rc = flowie_mqtt_client_handle_auth_challenge(client, &packet, NULL);
        if (rc != TURBO_OK) goto fail;
        continue;
      }
      if (response.reason_code != 0u) {
        rc = TURBO_EPROTO;
        goto fail;
      }
      rc = flowie_mqtt_client_auth_method_matches(client, &response.properties, 0);
      if (rc != TURBO_OK) goto fail;
      *auth = response;
      rc = TURBO_OK;
      goto done;
    }
    rc = flowie_mqtt_client_handle_unsolicited(client, &packet, &handled);
    if (rc != TURBO_OK || !handled) {
      if (rc == TURBO_OK) rc = TURBO_EPROTO;
      goto fail;
    }
  }

fail:
  flowie_mqtt_client_transport_close(client, 1);
done:
  flowie_mqtt_client_end(client);
  return rc;
}

static int flowie_mqtt_client_disconnect_operation(flowie_mqtt_client_t *client,
                                                   uint8_t reason_code,
                                                   flowie_mqtt_span_t properties) {
  flowie_mqtt_control_packet_t packet = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  size_t written = 0u;
  int rc = flowie_mqtt_client_begin(client, 1);
  if (rc != TURBO_OK) return rc;
  packet.version = client->version;
  packet.type = FLOWIE_MQTT_PACKET_DISCONNECT;
  packet.reason_code = reason_code;
  packet.properties = properties;
  rc = flowie_mqtt_control_packet_encode(&packet, (uint8_t *)client->send_buffer,
                                         client->outbound_max_packet_size, &written);
  if (rc == FLOWIE_MQTT_PARSE_OK) rc = flowie_mqtt_client_send(client, written);
  else rc = flowie_mqtt_client_parse_status(rc);
  flowie_mqtt_client_transport_close(client, 1);
  flowie_mqtt_client_end(client);
  return rc;
}

int flowie_mqtt_client_is_connected(const flowie_mqtt_client_t *client) {
  return client && atomic_load_explicit(&client->public_connected, memory_order_acquire);
}

static int flowie_mqtt_client_submit_owned(flowie_mqtt_client_t *client,
                                           flowie_mqtt_client_command_t *command, int clone_rc) {
  int rc = clone_rc;
  if (rc == TURBO_OK) rc = flowie_mqtt_client_submit(client, command);
  if (rc != TURBO_OK) flowie_mqtt_client_command_destroy(command);
  return rc;
}

int flowie_mqtt_client_connect(flowie_mqtt_client_t *client,
                               const flowie_mqtt_connect_packet_t *packet) {
  flowie_mqtt_client_command_t *command;
  if (!client || !packet) return TURBO_EINVAL;
  if (!client->on_connect) return TURBO_ENOTSUP;
  command = flowie_mqtt_client_command_new(FLOWIE_MQTT_CLIENT_COMMAND_CONNECT, client->on_connect,
                                           client->user_data);
  if (!command) return TURBO_ENOMEM;
  return flowie_mqtt_client_submit_owned(client, command,
                                         flowie_mqtt_client_clone_connect(command, packet));
}

int flowie_mqtt_client_publish(flowie_mqtt_client_t *client,
                               const flowie_mqtt_client_publish_topic_vec_t *topics) {
  flowie_mqtt_client_command_vec_t commands;
  int rc;
  if (!client || !topics || topics->size != sizeof(*topics) || !topics->data ||
      topics->count == 0u)
    return TURBO_EINVAL;
  if (!client->on_publish) return TURBO_ENOTSUP;
  if (topics->count > client->command_queue_capacity) return TURBO_ENOSPC;
  rc = flowie_mqtt_client_command_vec_t_init(&commands);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_client_command_vec_t_reserve(&commands, topics->count);
  for (size_t i = 0u; rc == TURBO_OK && i < topics->count; ++i) {
    flowie_mqtt_client_command_t *command = flowie_mqtt_client_command_new(
        FLOWIE_MQTT_CLIENT_COMMAND_PUBLISH, client->on_publish, client->user_data);
    if (!command) {
      rc = TURBO_ENOMEM;
      break;
    }
    rc = flowie_mqtt_client_clone_publish_topic(command, topics->version, &topics->data[i]);
    if (rc == TURBO_OK) rc = flowie_mqtt_client_command_vec_t_push(&commands, command);
    if (rc != TURBO_OK) flowie_mqtt_client_command_destroy(command);
  }
  if (rc == TURBO_OK)
    rc = flowie_mqtt_client_submit_many(client, flowie_mqtt_client_command_vec_t_data(&commands),
                                        flowie_mqtt_client_command_vec_t_size(&commands));
  if (rc != TURBO_OK) {
    for (size_t i = 0u; i < flowie_mqtt_client_command_vec_t_size(&commands); ++i)
      flowie_mqtt_client_command_destroy(*flowie_mqtt_client_command_vec_t_at(&commands, i));
  }
  flowie_mqtt_client_command_vec_t_destroy(&commands);
  return rc;
}

int flowie_mqtt_client_subscribe(flowie_mqtt_client_t *client,
                                 const flowie_mqtt_subscribe_packet_t *packet) {
  flowie_mqtt_client_command_t *command;
  if (!client || !packet) return TURBO_EINVAL;
  if (!client->on_subscribe) return TURBO_ENOTSUP;
  command = flowie_mqtt_client_command_new(FLOWIE_MQTT_CLIENT_COMMAND_SUBSCRIBE,
                                           client->on_subscribe, client->user_data);
  if (!command) return TURBO_ENOMEM;
  return flowie_mqtt_client_submit_owned(client, command,
                                         flowie_mqtt_client_clone_subscribe(command, packet));
}

int flowie_mqtt_client_unsubscribe(flowie_mqtt_client_t *client,
                                   const flowie_mqtt_unsubscribe_packet_t *packet) {
  flowie_mqtt_client_command_t *command;
  if (!client || !packet) return TURBO_EINVAL;
  if (!client->on_unsubscribe) return TURBO_ENOTSUP;
  command = flowie_mqtt_client_command_new(FLOWIE_MQTT_CLIENT_COMMAND_UNSUBSCRIBE,
                                           client->on_unsubscribe, client->user_data);
  if (!command) return TURBO_ENOMEM;
  return flowie_mqtt_client_submit_owned(client, command,
                                         flowie_mqtt_client_clone_unsubscribe(command, packet));
}

int flowie_mqtt_client_ping(flowie_mqtt_client_t *client) {
  flowie_mqtt_client_command_t *command;
  int rc;
  if (!client) return TURBO_EINVAL;
  if (!client->on_ping) return TURBO_ENOTSUP;
  command = flowie_mqtt_client_command_new(FLOWIE_MQTT_CLIENT_COMMAND_PING, client->on_ping,
                                           client->user_data);
  if (!command) return TURBO_ENOMEM;
  rc = flowie_mqtt_client_submit(client, command);
  if (rc != TURBO_OK) flowie_mqtt_client_command_destroy(command);
  return rc;
}

int flowie_mqtt_client_authenticate(flowie_mqtt_client_t *client, flowie_mqtt_span_t properties) {
  flowie_mqtt_client_command_t *command;
  uint8_t *cursor;
  int rc;
  if (!client || !flowie_mqtt_client_span_valid(properties)) return TURBO_EINVAL;
  if (!client->on_auth) return TURBO_ENOTSUP;
  command = flowie_mqtt_client_command_new(FLOWIE_MQTT_CLIENT_COMMAND_AUTH, client->on_auth,
                                           client->user_data);
  if (!command) return TURBO_ENOMEM;
  rc = flowie_mqtt_client_command_allocate(command, properties.size, &cursor);
  if (rc == TURBO_OK) {
    command->packet.control.reason_code = 0x19u;
    flowie_mqtt_client_copy_span(properties, &cursor, &command->packet.control.properties);
    rc = flowie_mqtt_client_submit(client, command);
  }
  if (rc != TURBO_OK) flowie_mqtt_client_command_destroy(command);
  return rc;
}

int flowie_mqtt_client_disconnect(flowie_mqtt_client_t *client, uint8_t reason_code,
                                  flowie_mqtt_span_t properties) {
  flowie_mqtt_client_command_t *command;
  uint8_t *cursor;
  int rc;
  if (!client || !flowie_mqtt_client_span_valid(properties)) return TURBO_EINVAL;
  if (!client->on_disconnect) return TURBO_ENOTSUP;
  command = flowie_mqtt_client_command_new(FLOWIE_MQTT_CLIENT_COMMAND_DISCONNECT,
                                           client->on_disconnect, client->user_data);
  if (!command) return TURBO_ENOMEM;
  rc = flowie_mqtt_client_command_allocate(command, properties.size, &cursor);
  if (rc == TURBO_OK) {
    command->packet.control.reason_code = reason_code;
    flowie_mqtt_client_copy_span(properties, &cursor, &command->packet.control.properties);
    rc = flowie_mqtt_client_submit(client, command);
  }
  if (rc != TURBO_OK) flowie_mqtt_client_command_destroy(command);
  return rc;
}
