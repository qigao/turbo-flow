#include "flowie_ingress_internal.h"
#include "flowie_rule_internal.h"

#include "turbo_byte_buffer.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <stdlib.h>
#include <string.h>

struct flowie_ingress_s {
  turbo_flow_t *flow;
  tstr_t publish_source;
  flowie_mqtt_parse_options_t parse_options;
  turbo_byte_buffer_t framing;
  turbo_flow_protocol_route_t route;
  flowie_ingress_prepare_fn prepare;
  flowie_ingress_publish_complete_fn publish_complete;
  void *prepare_ctx;
  turbo_flow_protocol_settlement_envelope_t protocol_settlement;
  tstr_t publish_packet_override;
  int has_protocol_settlement;
  int has_route;
  int terminal_error;
};

static int flowie_ingress_parse_error(int parse_rc) {
  switch (parse_rc) {
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

static int flowie_ingress_message_create(const flowie_ingress_t *ingress,
                                         const flowie_mqtt_packet_view_t *packet,
                                         const uint8_t *bytes, size_t packet_size,
                                         turbo_flow_msg_t *msg) {
  int rc;
  turbo_flow_msg_init(msg);
  msg->type = (uint32_t)packet->type;
  rc = flowie_mqtt_message_flags_encode(ingress->parse_options.version, packet->flags,
                                        &msg->flags);
  if (rc != TURBO_OK) return rc;
  msg->owned_payload = tstr_new_len(bytes, packet_size);
  if (!msg->owned_payload) return TURBO_ENOMEM;
  msg->payload = tstr_to_v(msg->owned_payload);
  if (ingress->has_route) {
    rc = turbo_flow_msg_set_protocol_route(msg, &ingress->route);
    if (rc != TURBO_OK) {
      turbo_flow_msg_cleanup(msg);
      return rc;
    }
  }
  if (ingress->has_protocol_settlement) {
    rc = turbo_flow_msg_set_protocol_settlement(msg, &ingress->protocol_settlement);
    if (rc != TURBO_OK) {
      turbo_flow_msg_cleanup(msg);
      return rc;
    }
  }
  return TURBO_OK;
}

static int flowie_ingress_pump(flowie_ingress_t *ingress, size_t *published) {
  int rc;
  for (;;) {
    turbo_byte_buffer_view_t bytes;
    flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
    flowie_mqtt_parse_error_t error = FLOWIE_MQTT_PARSE_ERROR_INIT;
    turbo_flow_msg_t msg;
    size_t consumed = 0u;
    rc = turbo_byte_buffer_view(&ingress->framing, &bytes);
    if (rc != TURBO_OK) return rc;
    if (bytes.size == 0u) break;
    rc = flowie_mqtt_packet_parse(bytes.data, bytes.size, &ingress->parse_options, &packet,
                                  &consumed, &error);
    if (rc == FLOWIE_MQTT_PARSE_NEED_MORE) break;
    if (rc != FLOWIE_MQTT_PARSE_OK) return flowie_ingress_parse_error(rc);
    if (consumed == 0u || consumed > bytes.size) return TURBO_EPROTO;
    if (ingress->parse_options.version == FLOWIE_MQTT_VERSION_UNSPECIFIED) {
      flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
      if (packet.type != FLOWIE_MQTT_PACKET_CONNECT ||
          flowie_mqtt_connect_parse(&packet, &connect) != FLOWIE_MQTT_PARSE_OK) {
        return TURBO_EPROTO;
      }
      ingress->parse_options.version = connect.version;
    } else if (packet.type == FLOWIE_MQTT_PACKET_CONNECT) {
      return TURBO_EPROTO;
    }

    if (ingress->prepare) {
      int publish_packet = 1;
      int stop_pump = 0;
      ingress->has_protocol_settlement = 0;
      rc = ingress->prepare(ingress->prepare_ctx, ingress, &packet, &publish_packet, &stop_pump);
      if (rc != TURBO_OK) {
        tstr_freep(&ingress->publish_packet_override);
        return rc;
      }
      if (!publish_packet) {
        tstr_freep(&ingress->publish_packet_override);
        rc = turbo_byte_buffer_consume(&ingress->framing, consumed);
        if (rc != TURBO_OK) return rc;
        if (stop_pump) break;
        continue;
      }
    }

    /* Ownership is transferred before the borrowed framing view is invalidated. */
    rc = flowie_ingress_message_create(
        ingress, &packet,
        ingress->publish_packet_override ? (const uint8_t *)ingress->publish_packet_override
                                         : bytes.data,
        ingress->publish_packet_override ? tstr_len(ingress->publish_packet_override) : consumed,
        &msg);
    tstr_freep(&ingress->publish_packet_override);
    ingress->has_protocol_settlement = 0;
    if (rc != TURBO_OK) return rc;
    rc = turbo_byte_buffer_consume(&ingress->framing, consumed);
    {
      turbo_flow_publish_result_t result = TURBO_FLOW_PUBLISH_RESULT_INIT;
      if (rc == TURBO_OK)
        rc = turbo_flow_publish_ex(ingress->flow, ingress->publish_source, &msg, &result);
      else result.status = rc;
      if (ingress->publish_complete) {
        int completion_rc = ingress->publish_complete(ingress->prepare_ctx, ingress, &msg, &result);
        if (rc == TURBO_OK) rc = completion_rc;
      }
    }
    turbo_flow_msg_cleanup(&msg);
    if (rc != TURBO_OK) return rc;
    ++*published;
  }
  return TURBO_OK;
}

static int flowie_ingress_terminal(flowie_ingress_t *ingress, int rc) {
  if (rc != TURBO_OK && rc != FLOWIE_MQTT_PARSE_NEED_MORE) ingress->terminal_error = rc;
  return rc;
}

flowie_ingress_t *flowie_ingress_create(const flowie_ingress_config_t *config) {
  flowie_ingress_t *ingress;
  size_t max_packet_size;
  int rc;
  if (!config || config->size < sizeof(*config) ||
      config->abi_version != FLOWIE_INGRESS_INTERNAL_ABI_V2 || !config->flow ||
      !config->publish_source || config->publish_source[0] == '\0' ||
      (config->version != FLOWIE_MQTT_VERSION_UNSPECIFIED &&
       !flowie_mqtt_version_is_supported(config->version)))
    return NULL;
  max_packet_size =
      config->max_packet_size ? config->max_packet_size : FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE;
  if (max_packet_size > FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE) return NULL;
  ingress = (flowie_ingress_t *)calloc(1, sizeof(*ingress));
  if (!ingress) return NULL;
  ingress->flow = config->flow;
  ingress->publish_source = tstr_dup(config->publish_source);
  if (!ingress->publish_source) goto fail;
  ingress->parse_options = (flowie_mqtt_parse_options_t)FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  ingress->parse_options.version = config->version;
  ingress->parse_options.max_packet_size = max_packet_size;
  ingress->prepare = config->prepare;
  ingress->publish_complete = config->publish_complete;
  ingress->prepare_ctx = config->prepare_ctx;
  if (config->route.protocol != 0) {
    turbo_flow_msg_t route_probe;
    turbo_flow_msg_init(&route_probe);
    rc = turbo_flow_msg_set_protocol_route(&route_probe, &config->route);
    turbo_flow_msg_cleanup(&route_probe);
    if (rc != TURBO_OK) goto fail;
    ingress->route = config->route;
    ingress->route.size = sizeof(ingress->route);
    ingress->has_route = 1;
  }
  rc = turbo_byte_buffer_init(&ingress->framing, max_packet_size);
  if (rc != TURBO_OK) goto fail;
  return ingress;

fail:
  flowie_ingress_destroy(ingress);
  return NULL;
}

void flowie_ingress_destroy(flowie_ingress_t *ingress) {
  if (!ingress) return;
  turbo_byte_buffer_destroy(&ingress->framing);
  tstr_freep(&ingress->publish_packet_override);
  tstr_freep(&ingress->publish_source);
  free(ingress);
}

int flowie_ingress_feed(flowie_ingress_t *ingress, const void *data, size_t size,
                        size_t *published) {
  const uint8_t *cursor = (const uint8_t *)data;
  size_t remaining = size;
  int rc;
  if (!ingress || (!data && size != 0u) || !published) return TURBO_EINVAL;
  *published = 0u;
  if (ingress->terminal_error != TURBO_OK) return ingress->terminal_error;
  while (remaining != 0u) {
    size_t writable = turbo_byte_buffer_available(&ingress->framing);
    size_t chunk;
    if (writable == 0u) {
      rc = flowie_ingress_pump(ingress, published);
      if (rc != TURBO_OK) return flowie_ingress_terminal(ingress, rc);
      writable = turbo_byte_buffer_available(&ingress->framing);
      if (writable == 0u) return flowie_ingress_terminal(ingress, TURBO_EMSGSIZE);
    }
    chunk = remaining < writable ? remaining : writable;
    rc = turbo_byte_buffer_append(&ingress->framing, cursor, chunk);
    if (rc != TURBO_OK) return flowie_ingress_terminal(ingress, rc);
    cursor += chunk;
    remaining -= chunk;
    rc = flowie_ingress_pump(ingress, published);
    if (rc != TURBO_OK) return flowie_ingress_terminal(ingress, rc);
  }
  return TURBO_OK;
}

size_t flowie_ingress_buffered_bytes(const flowie_ingress_t *ingress) {
  return ingress ? turbo_byte_buffer_size(&ingress->framing) : 0u;
}

flowie_mqtt_version_t flowie_ingress_version(const flowie_ingress_t *ingress) {
  return ingress ? ingress->parse_options.version : FLOWIE_MQTT_VERSION_UNSPECIFIED;
}

int flowie_ingress_set_route(flowie_ingress_t *ingress, const turbo_flow_protocol_route_t *route) {
  turbo_flow_msg_t probe;
  int rc;
  if (!ingress || !route) return TURBO_EINVAL;
  turbo_flow_msg_init(&probe);
  rc = turbo_flow_msg_set_protocol_route(&probe, route);
  turbo_flow_msg_cleanup(&probe);
  if (rc != TURBO_OK) return rc;
  ingress->route = *route;
  ingress->route.size = sizeof(ingress->route);
  ingress->has_route = 1;
  return TURBO_OK;
}

int flowie_ingress_set_protocol_settlement(
    flowie_ingress_t *ingress, const turbo_flow_protocol_settlement_envelope_t *settlement) {
  if (!ingress || !settlement || settlement->size < sizeof(*settlement) ||
      settlement->contract_version != TURBO_FLOW_PROTOCOL_CONTRACT_VERSION ||
      turbo_flow_protocol_message_validate(&settlement->message) != TURBO_OK ||
      settlement->settled_point != 0)
    return TURBO_EINVAL;
  ingress->protocol_settlement = *settlement;
  ingress->protocol_settlement.size = sizeof(ingress->protocol_settlement);
  ingress->has_protocol_settlement = 1;
  return TURBO_OK;
}

int flowie_ingress_set_publish_packet(flowie_ingress_t *ingress, const void *packet,
                                      size_t packet_size) {
  tstr_t replacement;
  if (!ingress || !packet || packet_size == 0u || ingress->publish_packet_override)
    return TURBO_EINVAL;
  replacement = tstr_new_len(packet, packet_size);
  if (!replacement) return TURBO_ENOMEM;
  ingress->publish_packet_override = replacement;
  return TURBO_OK;
}
