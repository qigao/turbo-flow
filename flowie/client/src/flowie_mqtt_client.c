#include "flowie_mqtt_client.h"

#include "CoroNet/turbo_coro_context.h"
#include "CoroNet/turbo_coro_socket.h"
#include "flowie_mqtt_protocol.h"
#include "turbo_byte_buffer.h"
#include "turbo_deque.h"
#include "turbo_error.h"
#include "turbo_set.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

TURBO_SET_DEFINE(flowie_mqtt_packet_id_set_t, uint16_t)

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
    } disconnect;
  } packet;
} flowie_mqtt_client_command_t;

TURBO_DEQUE_DEFINE(flowie_mqtt_client_command_queue_t, flowie_mqtt_client_command_t *)

struct flowie_mqtt_client_s {
  coro_context_t *context;
  coro_socket_t *socket;
  flowie_mqtt_client_transport_t transport;
  flowie_mqtt_client_state_t state;
  flowie_mqtt_version_t version;
  tstr_t host;
  tstr_t path;
  int port;
  uint64_t timeout_ms;
  size_t max_packet_size;
  size_t max_inbound_qos2;
  flowie_mqtt_client_publish_fn on_publish;
  flowie_mqtt_client_disconnect_fn on_disconnect;
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
  int managed;
  int owns_context;
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

static int flowie_mqtt_client_config_validate(const flowie_mqtt_client_config_t *config) {
  const size_t v1_size = offsetof(flowie_mqtt_client_config_t, command_queue_capacity);
  size_t max_packet_size;
  if (!config ||
      !((config->abi_version == FLOWIE_MQTT_CLIENT_ABI_V1 && config->size >= v1_size &&
         config->context) ||
        (config->abi_version == FLOWIE_MQTT_CLIENT_ABI_V2 && config->size >= sizeof(*config))) ||
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
  if (config->abi_version == FLOWIE_MQTT_CLIENT_ABI_V2 && !config->context &&
      (config->command_queue_capacity == SIZE_MAX || config->command_queue_max_bytes == SIZE_MAX))
    return TURBO_EINVAL;
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
  if (!client || !client->socket || written == 0u || written > client->max_packet_size)
    return TURBO_EINVAL;
  return coro_socket_send(client->socket, client->send_buffer, written);
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
                                         client->max_packet_size, &written);
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
      if (rc == TURBO_EINTR) client->interrupt_pending = 0;
      return rc;
    }
    client->recv_offset = 0u;
    if (!client->recv_data && client->recv_size == 0u) {
      if (client->interrupt_pending) {
        client->interrupt_pending = 0;
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
  int rc = flowie_mqtt_publish_parse(packet, &publish);
  if (rc != FLOWIE_MQTT_PARSE_OK) return flowie_mqtt_client_parse_status(rc);
  if (!client->on_publish) return TURBO_ENOTSUP;
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
  rc = client->on_publish(client, &publish, client->user_data);
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
    return TURBO_ENOTSUP;
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

static void flowie_mqtt_client_run_command(flowie_mqtt_client_t *client,
                                           flowie_mqtt_client_command_t *command) {
  flowie_mqtt_control_packet_view_t response = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  const flowie_mqtt_control_packet_view_t *response_ptr = NULL;
  int rc;
  switch (command->type) {
  case FLOWIE_MQTT_CLIENT_COMMAND_CONNECT:
    rc = flowie_mqtt_client_connect(client, &command->packet.connect, &response);
    if (rc == TURBO_OK) response_ptr = &response;
    break;
  case FLOWIE_MQTT_CLIENT_COMMAND_PUBLISH:
    rc = flowie_mqtt_client_publish(client, &command->packet.publish, &response);
    if (rc == TURBO_OK && command->packet.publish.qos != 0u) response_ptr = &response;
    break;
  case FLOWIE_MQTT_CLIENT_COMMAND_SUBSCRIBE:
    rc = flowie_mqtt_client_subscribe(client, &command->packet.subscribe, &response);
    if (rc == TURBO_OK) response_ptr = &response;
    break;
  case FLOWIE_MQTT_CLIENT_COMMAND_UNSUBSCRIBE:
    rc = flowie_mqtt_client_unsubscribe(client, &command->packet.unsubscribe, &response);
    if (rc == TURBO_OK) response_ptr = &response;
    break;
  case FLOWIE_MQTT_CLIENT_COMMAND_PING:
    rc = flowie_mqtt_client_ping(client);
    break;
  case FLOWIE_MQTT_CLIENT_COMMAND_DISCONNECT:
    rc = flowie_mqtt_client_disconnect(client, command->packet.disconnect.reason_code,
                                       command->packet.disconnect.properties);
    if (rc == TURBO_EOF || rc == TURBO_ECONNRESET) rc = TURBO_OK;
    break;
  default:
    rc = TURBO_EINVAL;
    break;
  }
  flowie_mqtt_client_complete(client, command, rc, response_ptr);
}

static void flowie_mqtt_client_managed_pump(coro_t *co, void *arg) {
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
        if (client->on_disconnect) {
          client->callback_active = 1;
          client->on_disconnect(client, rc, client->user_data);
          client->callback_active = 0;
        }
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

static void flowie_mqtt_client_managed_wake(void *arg1, void *arg2) {
  flowie_mqtt_client_t *client = (flowie_mqtt_client_t *)arg1;
  int stopping;
  int rc;
  (void)arg2;
  turbo_mutex_lock(&client->command_mutex);
  stopping = client->stopping;
  turbo_mutex_unlock(&client->command_mutex);
  if (!client->pump_active) {
    client->pump_active = 1;
    rc = coro_context_spawn(client->context, flowie_mqtt_client_managed_pump, client);
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
  if ((stopping || client->idle_receive) && client->socket) {
    if (!stopping) client->interrupt_pending = 1;
    rc = coro_socket_interrupt_wait(client->socket, stopping ? TURBO_ESHUTDOWN : TURBO_EINTR);
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
  int rc;
  if (!client || !command) return TURBO_EINVAL;
  if (!client->managed) return TURBO_ENOTSUP;
  if (command->owned_size > SIZE_MAX - sizeof(*command)) return TURBO_EMSGSIZE;
  charge = sizeof(*command) + command->owned_size;
  turbo_mutex_lock(&client->command_mutex);
  if (client->stopping) {
    rc = TURBO_ESHUTDOWN;
  } else if (flowie_mqtt_client_command_queue_t_size(&client->commands) >=
             client->command_queue_capacity) {
    rc = TURBO_ENOSPC;
  } else if (charge > client->command_queue_max_bytes - client->command_queue_bytes) {
    rc = TURBO_ENOSPC;
  } else {
    rc = flowie_mqtt_client_command_queue_t_push_back(&client->commands, command);
    if (rc == TURBO_OK) {
      client->command_queue_bytes += charge;
      rc = coro_post(client->context, flowie_mqtt_client_managed_wake, client, NULL);
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

int flowie_mqtt_client_create(const flowie_mqtt_client_config_t *config,
                              flowie_mqtt_client_t **out) {
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
  client->managed = config->abi_version == FLOWIE_MQTT_CLIENT_ABI_V2 && !config->context;
  client->context = config->context;
  client->transport = config->transport;
  client->port = config->port;
  client->timeout_ms =
      config->timeout_ms ? config->timeout_ms : FLOWIE_MQTT_CLIENT_DEFAULT_TIMEOUT_MS;
  client->max_packet_size = max_packet_size;
  client->max_inbound_qos2 = config->max_inbound_qos2;
  client->on_publish = config->on_publish;
  client->on_disconnect =
      config->abi_version == FLOWIE_MQTT_CLIENT_ABI_V2 ? config->on_disconnect : NULL;
  client->user_data = config->user_data;
  client->command_queue_capacity =
      config->abi_version == FLOWIE_MQTT_CLIENT_ABI_V2 && config->command_queue_capacity
          ? config->command_queue_capacity
          : FLOWIE_MQTT_CLIENT_DEFAULT_COMMAND_QUEUE_CAPACITY;
  client->command_queue_max_bytes =
      config->abi_version == FLOWIE_MQTT_CLIENT_ABI_V2 && config->command_queue_max_bytes
          ? config->command_queue_max_bytes
          : FLOWIE_MQTT_CLIENT_DEFAULT_COMMAND_QUEUE_BYTES;
  client->next_packet_id = 1u;
  client->host = tstr_dup(config->host);
  client->path = tstr_dup(config->path ? config->path : "/mqtt");
  client->send_buffer = tstr_new_len(NULL, max_packet_size);
  if (!client->host || !client->path || !client->send_buffer) {
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
  if (client->managed) {
    turbo_mutex_init(&client->command_mutex);
    client->sync_initialized = 1;
    rc = flowie_mqtt_client_command_queue_t_init(&client->commands);
    if (rc != TURBO_OK) goto fail;
    client->command_queue_initialized = 1;
    rc = flowie_mqtt_client_command_queue_t_reserve(&client->commands,
                                                    client->command_queue_capacity);
    if (rc != TURBO_OK) goto fail;
    client->context = coro_context_create(NULL);
    if (!client->context) {
      rc = TURBO_ENOMEM;
      goto fail;
    }
    client->owns_context = 1;
    coro_context_set_persistent(client->context, 1);
    rc = turbo_thread_create(&client->worker, flowie_mqtt_client_worker, client);
    if (rc != TURBO_OK) goto fail;
    client->worker_started = 1;
  }
  *out = client;
  return TURBO_OK;

fail:
  flowie_mqtt_client_destroy(client);
  return rc;
}

void flowie_mqtt_client_destroy(flowie_mqtt_client_t *client) {
  flowie_mqtt_client_command_t *command = NULL;
  if (!client) return;
  if (client->managed && client->worker_started) {
    int rc;
    if (coro_context_current() == client->context) {
      turbo_mutex_lock(&client->command_mutex);
      client->stopping = 1;
      turbo_mutex_unlock(&client->command_mutex);
      (void)coro_post(client->context, flowie_mqtt_client_managed_wake, client, NULL);
      return;
    }
    turbo_mutex_lock(&client->command_mutex);
    client->stopping = 1;
    turbo_mutex_unlock(&client->command_mutex);
    do {
      rc = coro_post(client->context, flowie_mqtt_client_managed_wake, client, NULL);
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
  if (client->owns_context) coro_context_destroy(client->context);
  if (client->qos2_initialized) flowie_mqtt_packet_id_set_t_destroy(&client->inbound_qos2);
  if (client->framing_initialized) turbo_byte_buffer_destroy(&client->framing);
  tstr_freep(&client->send_buffer);
  tstr_freep(&client->path);
  tstr_freep(&client->host);
  free(client);
}

int flowie_mqtt_client_connect(flowie_mqtt_client_t *client,
                               const flowie_mqtt_connect_packet_t *packet,
                               flowie_mqtt_control_packet_view_t *connack) {
  size_t written = 0u;
  int rc;
  if (!client || !packet || !connack || !flowie_mqtt_client_ack_output_valid(connack) ||
      (packet->version != FLOWIE_MQTT_VERSION_3_1_1 && packet->version != FLOWIE_MQTT_VERSION_5))
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

int flowie_mqtt_client_publish(flowie_mqtt_client_t *client,
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
  if (encoded.qos != 0u) {
    packet_id = flowie_mqtt_client_packet_id(client);
    encoded.packet_id = packet_id;
  }
  rc = flowie_mqtt_publish_packet_encode(&encoded, (uint8_t *)client->send_buffer,
                                         client->max_packet_size, &written);
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

int flowie_mqtt_client_subscribe(flowie_mqtt_client_t *client,
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
                                           client->max_packet_size, &written);
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

int flowie_mqtt_client_unsubscribe(flowie_mqtt_client_t *client,
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
                                             client->max_packet_size, &written);
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

int flowie_mqtt_client_ping(flowie_mqtt_client_t *client) {
  size_t written = 0u;
  int rc = flowie_mqtt_client_begin(client, 1);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_pingreq_encode(client->version, (uint8_t *)client->send_buffer,
                                  client->max_packet_size, &written);
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

int flowie_mqtt_client_poll(flowie_mqtt_client_t *client,
                            flowie_mqtt_packet_type_t *received_type) {
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  int handled = 0;
  int rc = flowie_mqtt_client_begin(client, 1);
  if (rc != TURBO_OK) return rc;
  rc = flowie_mqtt_client_receive_packet(client, &packet);
  if (rc == TURBO_OK) {
    if (received_type) *received_type = packet.type;
    rc = flowie_mqtt_client_handle_unsolicited(client, &packet, &handled);
    if (rc == TURBO_OK && !handled && packet.type != FLOWIE_MQTT_PACKET_PINGRESP) rc = TURBO_EPROTO;
  }
  if (rc != TURBO_OK) flowie_mqtt_client_transport_close(client, 1);
  flowie_mqtt_client_end(client);
  return rc;
}

int flowie_mqtt_client_disconnect(flowie_mqtt_client_t *client, uint8_t reason_code,
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
                                         client->max_packet_size, &written);
  if (rc == FLOWIE_MQTT_PARSE_OK) rc = flowie_mqtt_client_send(client, written);
  else rc = flowie_mqtt_client_parse_status(rc);
  flowie_mqtt_client_transport_close(client, 1);
  flowie_mqtt_client_end(client);
  return rc;
}

int flowie_mqtt_client_is_connected(const flowie_mqtt_client_t *client) {
  return client && atomic_load_explicit(&client->public_connected, memory_order_acquire);
}

int flowie_mqtt_client_is_managed(const flowie_mqtt_client_t *client) {
  return client && client->managed;
}

static int flowie_mqtt_client_submit_owned(flowie_mqtt_client_t *client,
                                           flowie_mqtt_client_command_t *command, int clone_rc) {
  int rc = clone_rc;
  if (rc == TURBO_OK) rc = flowie_mqtt_client_submit(client, command);
  if (rc != TURBO_OK) flowie_mqtt_client_command_destroy(command);
  return rc;
}

int flowie_mqtt_client_connect_async(flowie_mqtt_client_t *client,
                                     const flowie_mqtt_connect_packet_t *packet,
                                     flowie_mqtt_client_completion_fn completion, void *user_data) {
  flowie_mqtt_client_command_t *command;
  if (!client || !packet || !completion) return TURBO_EINVAL;
  if (!client->managed) return TURBO_ENOTSUP;
  command =
      flowie_mqtt_client_command_new(FLOWIE_MQTT_CLIENT_COMMAND_CONNECT, completion, user_data);
  if (!command) return TURBO_ENOMEM;
  return flowie_mqtt_client_submit_owned(client, command,
                                         flowie_mqtt_client_clone_connect(command, packet));
}

int flowie_mqtt_client_publish_async(flowie_mqtt_client_t *client,
                                     const flowie_mqtt_publish_packet_t *packet,
                                     flowie_mqtt_client_completion_fn completion, void *user_data) {
  flowie_mqtt_client_command_t *command;
  if (!client || !packet || !completion) return TURBO_EINVAL;
  if (!client->managed) return TURBO_ENOTSUP;
  command =
      flowie_mqtt_client_command_new(FLOWIE_MQTT_CLIENT_COMMAND_PUBLISH, completion, user_data);
  if (!command) return TURBO_ENOMEM;
  return flowie_mqtt_client_submit_owned(client, command,
                                         flowie_mqtt_client_clone_publish(command, packet));
}

int flowie_mqtt_client_subscribe_async(flowie_mqtt_client_t *client,
                                       const flowie_mqtt_subscribe_packet_t *packet,
                                       flowie_mqtt_client_completion_fn completion,
                                       void *user_data) {
  flowie_mqtt_client_command_t *command;
  if (!client || !packet || !completion) return TURBO_EINVAL;
  if (!client->managed) return TURBO_ENOTSUP;
  command =
      flowie_mqtt_client_command_new(FLOWIE_MQTT_CLIENT_COMMAND_SUBSCRIBE, completion, user_data);
  if (!command) return TURBO_ENOMEM;
  return flowie_mqtt_client_submit_owned(client, command,
                                         flowie_mqtt_client_clone_subscribe(command, packet));
}

int flowie_mqtt_client_unsubscribe_async(flowie_mqtt_client_t *client,
                                         const flowie_mqtt_unsubscribe_packet_t *packet,
                                         flowie_mqtt_client_completion_fn completion,
                                         void *user_data) {
  flowie_mqtt_client_command_t *command;
  if (!client || !packet || !completion) return TURBO_EINVAL;
  if (!client->managed) return TURBO_ENOTSUP;
  command =
      flowie_mqtt_client_command_new(FLOWIE_MQTT_CLIENT_COMMAND_UNSUBSCRIBE, completion, user_data);
  if (!command) return TURBO_ENOMEM;
  return flowie_mqtt_client_submit_owned(client, command,
                                         flowie_mqtt_client_clone_unsubscribe(command, packet));
}

int flowie_mqtt_client_ping_async(flowie_mqtt_client_t *client,
                                  flowie_mqtt_client_completion_fn completion, void *user_data) {
  flowie_mqtt_client_command_t *command;
  int rc;
  if (!client || !completion) return TURBO_EINVAL;
  if (!client->managed) return TURBO_ENOTSUP;
  command = flowie_mqtt_client_command_new(FLOWIE_MQTT_CLIENT_COMMAND_PING, completion, user_data);
  if (!command) return TURBO_ENOMEM;
  rc = flowie_mqtt_client_submit(client, command);
  if (rc != TURBO_OK) flowie_mqtt_client_command_destroy(command);
  return rc;
}

int flowie_mqtt_client_disconnect_async(flowie_mqtt_client_t *client, uint8_t reason_code,
                                        flowie_mqtt_span_t properties,
                                        flowie_mqtt_client_completion_fn completion,
                                        void *user_data) {
  flowie_mqtt_client_command_t *command;
  uint8_t *cursor;
  int rc;
  if (!client || !completion || !flowie_mqtt_client_span_valid(properties)) return TURBO_EINVAL;
  if (!client->managed) return TURBO_ENOTSUP;
  command =
      flowie_mqtt_client_command_new(FLOWIE_MQTT_CLIENT_COMMAND_DISCONNECT, completion, user_data);
  if (!command) return TURBO_ENOMEM;
  rc = flowie_mqtt_client_command_allocate(command, properties.size, &cursor);
  if (rc == TURBO_OK) {
    command->packet.disconnect.reason_code = reason_code;
    flowie_mqtt_client_copy_span(properties, &cursor, &command->packet.disconnect.properties);
    rc = flowie_mqtt_client_submit(client, command);
  }
  if (rc != TURBO_OK) flowie_mqtt_client_command_destroy(command);
  return rc;
}
