#ifndef FLOWIE_MQTT_TYPES_H
#define FLOWIE_MQTT_TYPES_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_MQTT_PROTOCOL_ABI_V1 1u

typedef enum flowie_mqtt_version_e {
  FLOWIE_MQTT_VERSION_UNSPECIFIED = 0,
  FLOWIE_MQTT_VERSION_3_1 = 3,
  FLOWIE_MQTT_VERSION_3_1_1 = 4,
  FLOWIE_MQTT_VERSION_5 = 5
} flowie_mqtt_version_t;

static inline int flowie_mqtt_version_is_supported(flowie_mqtt_version_t version) {
  return version == FLOWIE_MQTT_VERSION_3_1 || version == FLOWIE_MQTT_VERSION_3_1_1 ||
         version == FLOWIE_MQTT_VERSION_5;
}

static inline int flowie_mqtt_version_is_3x(flowie_mqtt_version_t version) {
  return version == FLOWIE_MQTT_VERSION_3_1 || version == FLOWIE_MQTT_VERSION_3_1_1;
}

typedef enum flowie_mqtt_packet_type_e {
  FLOWIE_MQTT_PACKET_CONNECT = 1,
  FLOWIE_MQTT_PACKET_CONNACK,
  FLOWIE_MQTT_PACKET_PUBLISH,
  FLOWIE_MQTT_PACKET_PUBACK,
  FLOWIE_MQTT_PACKET_PUBREC,
  FLOWIE_MQTT_PACKET_PUBREL,
  FLOWIE_MQTT_PACKET_PUBCOMP,
  FLOWIE_MQTT_PACKET_SUBSCRIBE,
  FLOWIE_MQTT_PACKET_SUBACK,
  FLOWIE_MQTT_PACKET_UNSUBSCRIBE,
  FLOWIE_MQTT_PACKET_UNSUBACK,
  FLOWIE_MQTT_PACKET_PINGREQ,
  FLOWIE_MQTT_PACKET_PINGRESP,
  FLOWIE_MQTT_PACKET_DISCONNECT,
  FLOWIE_MQTT_PACKET_AUTH
} flowie_mqtt_packet_type_t;

typedef struct flowie_mqtt_span_s {
  const uint8_t *data;
  size_t size;
} flowie_mqtt_span_t;

typedef struct flowie_mqtt_property_block_view_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_span_t encoded;
  flowie_mqtt_span_t values;
} flowie_mqtt_property_block_view_t;

#define FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT                                                       \
  {sizeof(flowie_mqtt_property_block_view_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

typedef struct flowie_mqtt_publish_view_s {
  size_t size;
  uint32_t abi_version;
  uint8_t qos;
  uint8_t retain;
  uint8_t duplicate;
  uint16_t packet_id;
  flowie_mqtt_span_t topic;
  flowie_mqtt_property_block_view_t properties;
  flowie_mqtt_span_t payload;
} flowie_mqtt_publish_view_t;

#define FLOWIE_MQTT_PUBLISH_VIEW_INIT                                                              \
  {sizeof(flowie_mqtt_publish_view_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

typedef struct flowie_mqtt_subscription_view_s {
  flowie_mqtt_span_t filter;
  uint8_t qos;
  uint8_t no_local;
  uint8_t retain_as_published;
  uint8_t retain_handling;
} flowie_mqtt_subscription_view_t;

/** Same borrowed layout when used as a client-side subscription description. */
typedef flowie_mqtt_subscription_view_t flowie_mqtt_subscription_t;

/**
 * Caller-owned CONNECT description. All spans are borrowed for the duration of
 * the operation. MQTT 5 property spans omit their variable-byte length prefix
 * and must be empty for MQTT 3.1.1. Presence flags distinguish an absent
 * optional field from a present empty one.
 */
typedef struct flowie_mqtt_connect_packet_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_version_t version;
  uint8_t clean_start;
  uint8_t has_will;
  uint8_t will_qos;
  uint8_t will_retain;
  uint8_t has_username;
  uint8_t has_password;
  uint16_t keep_alive;
  flowie_mqtt_span_t properties;
  flowie_mqtt_span_t client_id;
  flowie_mqtt_span_t will_properties;
  flowie_mqtt_span_t will_topic;
  flowie_mqtt_span_t will_payload;
  flowie_mqtt_span_t username;
  flowie_mqtt_span_t password;
} flowie_mqtt_connect_packet_t;

#define FLOWIE_MQTT_CONNECT_PACKET_INIT                                                            \
  {sizeof(flowie_mqtt_connect_packet_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

/** Caller-owned PUBLISH description; payload may contain arbitrary bytes. */
typedef struct flowie_mqtt_publish_packet_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_version_t version;
  uint8_t qos;
  uint8_t retain;
  uint8_t duplicate;
  uint16_t packet_id;
  flowie_mqtt_span_t topic;
  flowie_mqtt_span_t properties;
  flowie_mqtt_span_t payload;
} flowie_mqtt_publish_packet_t;

#define FLOWIE_MQTT_PUBLISH_PACKET_INIT                                                            \
  {sizeof(flowie_mqtt_publish_packet_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

/** Caller-owned SUBSCRIBE description; subscriptions is a borrowed array. */
typedef struct flowie_mqtt_subscribe_packet_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_version_t version;
  uint16_t packet_id;
  flowie_mqtt_span_t properties;
  const flowie_mqtt_subscription_t *subscriptions;
  size_t subscription_count;
} flowie_mqtt_subscribe_packet_t;

#define FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT                                                          \
  {sizeof(flowie_mqtt_subscribe_packet_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

/** Caller-owned UNSUBSCRIBE description; filters is a borrowed span array. */
typedef struct flowie_mqtt_unsubscribe_packet_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_version_t version;
  uint16_t packet_id;
  flowie_mqtt_span_t properties;
  const flowie_mqtt_span_t *filters;
  size_t filter_count;
} flowie_mqtt_unsubscribe_packet_t;

#define FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT                                                        \
  {sizeof(flowie_mqtt_unsubscribe_packet_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

/** Borrowed typed view of one decoded server/client ACK control packet. */
typedef struct flowie_mqtt_control_packet_view_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_version_t version;
  flowie_mqtt_packet_type_t type;
  uint8_t session_present;
  uint16_t packet_id;
  uint8_t reason_code;
  flowie_mqtt_property_block_view_t properties;
  flowie_mqtt_span_t reason_codes;
} flowie_mqtt_control_packet_view_t;

#define FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT                                                       \
  {sizeof(flowie_mqtt_control_packet_view_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_MQTT_TYPES_H */
