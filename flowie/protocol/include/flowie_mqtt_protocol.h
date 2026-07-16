#ifndef FLOWIE_MQTT_PROTOCOL_H
#define FLOWIE_MQTT_PROTOCOL_H

#include "platform.h"
#include "flowie_mqtt_types.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_MQTT_MAX_REMAINING_LENGTH UINT32_C(268435455)
#define FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE ((size_t)FLOWIE_MQTT_MAX_REMAINING_LENGTH + 5u)
#define FLOWIE_MQTT_MAX_UTF8_SIZE 65535u

typedef enum flowie_mqtt_parse_result_e {
  FLOWIE_MQTT_PARSE_OK = 0,
  FLOWIE_MQTT_PARSE_NEED_MORE = 1,
  FLOWIE_MQTT_PARSE_INVALID_ARGUMENT = -1,
  FLOWIE_MQTT_PARSE_MALFORMED = -2,
  FLOWIE_MQTT_PARSE_PROTOCOL_ERROR = -3,
  FLOWIE_MQTT_PARSE_TOO_LARGE = -4,
  FLOWIE_MQTT_PARSE_NO_MEMORY = -5
} flowie_mqtt_parse_result_t;

/**
 * Borrowed zero-copy packet envelope. All spans point into the input buffer and
 * remain valid only while that buffer is alive and unchanged.
 */
typedef struct flowie_mqtt_packet_view_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_version_t version;
  flowie_mqtt_packet_type_t type;
  uint8_t flags;
  uint32_t remaining_length;
  size_t fixed_header_size;
  flowie_mqtt_span_t packet;
  flowie_mqtt_span_t body;
} flowie_mqtt_packet_view_t;

#define FLOWIE_MQTT_PACKET_VIEW_INIT                                                               \
  {sizeof(flowie_mqtt_packet_view_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

typedef struct flowie_mqtt_parse_options_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_version_t version;
  /** Maximum complete wire packet size; zero selects the MQTT protocol maximum. */
  size_t max_packet_size;
} flowie_mqtt_parse_options_t;

#define FLOWIE_MQTT_PARSE_OPTIONS_INIT                                                             \
  {sizeof(flowie_mqtt_parse_options_t), FLOWIE_MQTT_PROTOCOL_ABI_V1,                               \
   FLOWIE_MQTT_VERSION_UNSPECIFIED, FLOWIE_MQTT_MAX_WIRE_PACKET_SIZE}

typedef struct flowie_mqtt_parse_error_s {
  size_t size;
  flowie_mqtt_parse_result_t code;
  size_t offset;
  const char *message;
} flowie_mqtt_parse_error_t;

#define FLOWIE_MQTT_PARSE_ERROR_INIT                                                               \
  {sizeof(flowie_mqtt_parse_error_t), FLOWIE_MQTT_PARSE_OK, 0u, NULL}

/**
 * Parse exactly the first MQTT packet from bytes. Trailing bytes are allowed
 * and reported through consumed. Output is modified only on success.
 */
CXX_C_API int flowie_mqtt_packet_parse(const uint8_t *bytes, size_t byte_count,
                                       const flowie_mqtt_parse_options_t *options,
                                       flowie_mqtt_packet_view_t *out, size_t *consumed,
                                       flowie_mqtt_parse_error_t *error);

/** Validate MQTT UTF-8 rules without requiring a trailing NUL. */
CXX_C_API int flowie_mqtt_utf8_validate(flowie_mqtt_span_t value);

/** Validate a Topic Name (wildcards are forbidden). */
CXX_C_API int flowie_mqtt_topic_name_validate(flowie_mqtt_span_t topic);

/** Validate a normal or MQTT 5 shared Topic Filter. */
CXX_C_API int flowie_mqtt_topic_filter_validate(flowie_mqtt_span_t filter);

/** Validate and match a Topic Name against a normal or shared Topic Filter. */
CXX_C_API int flowie_mqtt_topic_matches(flowie_mqtt_span_t filter, flowie_mqtt_span_t topic,
                                        int *matched_out);

typedef enum flowie_mqtt_acl_effect_e {
  FLOWIE_MQTT_ACL_DENY = 0,
  FLOWIE_MQTT_ACL_ALLOW = 1
} flowie_mqtt_acl_effect_t;

typedef enum flowie_mqtt_acl_permission_e {
  FLOWIE_MQTT_ACL_READ = 1,
  FLOWIE_MQTT_ACL_WRITE = 2,
  FLOWIE_MQTT_ACL_READ_WRITE = 3
} flowie_mqtt_acl_permission_t;

/** Empty role/scope/username/client_id spans mean wildcard `*`. */
typedef struct flowie_mqtt_acl_rule_view_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_acl_effect_t effect;
  flowie_mqtt_acl_permission_t permission;
  flowie_mqtt_span_t role;
  flowie_mqtt_span_t scope;
  flowie_mqtt_span_t username;
  flowie_mqtt_span_t client_id;
  flowie_mqtt_span_t topic_filter;
} flowie_mqtt_acl_rule_view_t;

#define FLOWIE_MQTT_ACL_RULE_VIEW_INIT                                                             \
  {sizeof(flowie_mqtt_acl_rule_view_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

typedef enum flowie_mqtt_acl_parse_result_e {
  FLOWIE_MQTT_ACL_PARSE_OK = 0,
  FLOWIE_MQTT_ACL_PARSE_SKIP = 1,
  FLOWIE_MQTT_ACL_PARSE_INVALID_ARGUMENT = -1,
  FLOWIE_MQTT_ACL_PARSE_INVALID_FORMAT = -2,
  FLOWIE_MQTT_ACL_PARSE_INVALID_ACTION = -3,
  FLOWIE_MQTT_ACL_PARSE_INVALID_PERMISSION = -4,
  FLOWIE_MQTT_ACL_PARSE_INVALID_SUBJECT = -5,
  FLOWIE_MQTT_ACL_PARSE_INVALID_TOPIC = -6
} flowie_mqtt_acl_parse_result_t;

/**
 * Parse `action:permission:role:scope:username:client_id:topic_filter`.
 * The result borrows the line buffer and performs no allocation or I/O.
 */
CXX_C_API int flowie_mqtt_acl_parse_line(const char *line, size_t line_size,
                                         flowie_mqtt_acl_rule_view_t *out);

typedef enum flowie_mqtt_property_id_e {
  FLOWIE_MQTT_PROPERTY_PAYLOAD_FORMAT_INDICATOR = 0x01,
  FLOWIE_MQTT_PROPERTY_MESSAGE_EXPIRY_INTERVAL = 0x02,
  FLOWIE_MQTT_PROPERTY_CONTENT_TYPE = 0x03,
  FLOWIE_MQTT_PROPERTY_RESPONSE_TOPIC = 0x08,
  FLOWIE_MQTT_PROPERTY_CORRELATION_DATA = 0x09,
  FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER = 0x0b,
  FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL = 0x11,
  FLOWIE_MQTT_PROPERTY_ASSIGNED_CLIENT_IDENTIFIER = 0x12,
  FLOWIE_MQTT_PROPERTY_SERVER_KEEP_ALIVE = 0x13,
  FLOWIE_MQTT_PROPERTY_AUTHENTICATION_METHOD = 0x15,
  FLOWIE_MQTT_PROPERTY_AUTHENTICATION_DATA = 0x16,
  FLOWIE_MQTT_PROPERTY_REQUEST_PROBLEM_INFORMATION = 0x17,
  FLOWIE_MQTT_PROPERTY_WILL_DELAY_INTERVAL = 0x18,
  FLOWIE_MQTT_PROPERTY_REQUEST_RESPONSE_INFORMATION = 0x19,
  FLOWIE_MQTT_PROPERTY_RESPONSE_INFORMATION = 0x1a,
  FLOWIE_MQTT_PROPERTY_SERVER_REFERENCE = 0x1c,
  FLOWIE_MQTT_PROPERTY_REASON_STRING = 0x1f,
  FLOWIE_MQTT_PROPERTY_RECEIVE_MAXIMUM = 0x21,
  FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS_MAXIMUM = 0x22,
  FLOWIE_MQTT_PROPERTY_TOPIC_ALIAS = 0x23,
  FLOWIE_MQTT_PROPERTY_MAXIMUM_QOS = 0x24,
  FLOWIE_MQTT_PROPERTY_RETAIN_AVAILABLE = 0x25,
  FLOWIE_MQTT_PROPERTY_USER_PROPERTY = 0x26,
  FLOWIE_MQTT_PROPERTY_MAXIMUM_PACKET_SIZE = 0x27,
  FLOWIE_MQTT_PROPERTY_WILDCARD_SUBSCRIPTION_AVAILABLE = 0x28,
  FLOWIE_MQTT_PROPERTY_SUBSCRIPTION_IDENTIFIER_AVAILABLE = 0x29,
  FLOWIE_MQTT_PROPERTY_SHARED_SUBSCRIPTION_AVAILABLE = 0x2a
} flowie_mqtt_property_id_t;

typedef enum flowie_mqtt_property_type_e {
  FLOWIE_MQTT_PROPERTY_BYTE = 1,
  FLOWIE_MQTT_PROPERTY_UINT16,
  FLOWIE_MQTT_PROPERTY_UINT32,
  FLOWIE_MQTT_PROPERTY_VARIABLE_INTEGER,
  FLOWIE_MQTT_PROPERTY_UTF8,
  FLOWIE_MQTT_PROPERTY_BINARY,
  FLOWIE_MQTT_PROPERTY_UTF8_PAIR
} flowie_mqtt_property_type_t;

typedef struct flowie_mqtt_property_view_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_property_id_t identifier;
  flowie_mqtt_property_type_t type;
  uint32_t integer;
  flowie_mqtt_span_t value;
  flowie_mqtt_span_t pair_value;
} flowie_mqtt_property_view_t;

#define FLOWIE_MQTT_PROPERTY_VIEW_INIT                                                             \
  {sizeof(flowie_mqtt_property_view_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

typedef struct flowie_mqtt_property_iterator_s {
  size_t size;
  uint32_t abi_version;
  const uint8_t *cursor;
  const uint8_t *limit;
} flowie_mqtt_property_iterator_t;

#define FLOWIE_MQTT_PROPERTY_ITERATOR_INIT                                                         \
  {sizeof(flowie_mqtt_property_iterator_t), FLOWIE_MQTT_PROTOCOL_ABI_V1, NULL, NULL}

CXX_C_API int flowie_mqtt_property_block_parse(flowie_mqtt_span_t bytes,
                                               flowie_mqtt_property_block_view_t *out,
                                               size_t *consumed);
CXX_C_API int flowie_mqtt_property_iterator_init(const flowie_mqtt_property_block_view_t *block,
                                                 flowie_mqtt_property_iterator_t *iterator);
/** Return OK for one property and NEED_MORE when the iterator is exhausted. */
CXX_C_API int flowie_mqtt_property_iterator_next(flowie_mqtt_property_iterator_t *iterator,
                                                 flowie_mqtt_property_view_t *out);

typedef struct flowie_mqtt_connect_view_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_version_t version;
  uint8_t clean_start;
  uint8_t will_qos;
  uint8_t will_retain;
  uint16_t keep_alive;
  flowie_mqtt_property_block_view_t properties;
  flowie_mqtt_span_t client_id;
  flowie_mqtt_property_block_view_t will_properties;
  flowie_mqtt_span_t will_topic;
  flowie_mqtt_span_t will_payload;
  flowie_mqtt_span_t username;
  flowie_mqtt_span_t password;
} flowie_mqtt_connect_view_t;

#define FLOWIE_MQTT_CONNECT_VIEW_INIT                                                              \
  {sizeof(flowie_mqtt_connect_view_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

typedef struct flowie_mqtt_subscribe_view_s {
  size_t size;
  uint32_t abi_version;
  uint16_t packet_id;
  flowie_mqtt_property_block_view_t properties;
  flowie_mqtt_span_t entries;
  size_t entry_count;
} flowie_mqtt_subscribe_view_t;

#define FLOWIE_MQTT_SUBSCRIBE_VIEW_INIT                                                            \
  {sizeof(flowie_mqtt_subscribe_view_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

typedef struct flowie_mqtt_subscription_iterator_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_version_t version;
  const uint8_t *cursor;
  const uint8_t *limit;
} flowie_mqtt_subscription_iterator_t;

#define FLOWIE_MQTT_SUBSCRIPTION_ITERATOR_INIT                                                     \
  {sizeof(flowie_mqtt_subscription_iterator_t), FLOWIE_MQTT_PROTOCOL_ABI_V1,                       \
   FLOWIE_MQTT_VERSION_UNSPECIFIED, NULL, NULL}

typedef struct flowie_mqtt_unsubscribe_view_s {
  size_t size;
  uint32_t abi_version;
  uint16_t packet_id;
  flowie_mqtt_property_block_view_t properties;
  flowie_mqtt_span_t filters;
  size_t filter_count;
} flowie_mqtt_unsubscribe_view_t;

#define FLOWIE_MQTT_UNSUBSCRIBE_VIEW_INIT                                                          \
  {sizeof(flowie_mqtt_unsubscribe_view_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

typedef struct flowie_mqtt_topic_filter_iterator_s {
  size_t size;
  uint32_t abi_version;
  const uint8_t *cursor;
  const uint8_t *limit;
} flowie_mqtt_topic_filter_iterator_t;

#define FLOWIE_MQTT_TOPIC_FILTER_ITERATOR_INIT                                                     \
  {sizeof(flowie_mqtt_topic_filter_iterator_t), FLOWIE_MQTT_PROTOCOL_ABI_V1, NULL, NULL}

CXX_C_API int flowie_mqtt_connect_parse(const flowie_mqtt_packet_view_t *packet,
                                        flowie_mqtt_connect_view_t *out);
CXX_C_API int flowie_mqtt_publish_parse(const flowie_mqtt_packet_view_t *packet,
                                        flowie_mqtt_publish_view_t *out);
CXX_C_API int flowie_mqtt_subscribe_parse(const flowie_mqtt_packet_view_t *packet,
                                          flowie_mqtt_subscribe_view_t *out);
CXX_C_API int flowie_mqtt_subscription_iterator_init(const flowie_mqtt_packet_view_t *packet,
                                                     const flowie_mqtt_subscribe_view_t *subscribe,
                                                     flowie_mqtt_subscription_iterator_t *iterator);
/** Return OK for one entry and NEED_MORE when the iterator is exhausted. */
CXX_C_API int flowie_mqtt_subscription_iterator_next(flowie_mqtt_subscription_iterator_t *iterator,
                                                     flowie_mqtt_subscription_view_t *out);
CXX_C_API int flowie_mqtt_unsubscribe_parse(const flowie_mqtt_packet_view_t *packet,
                                            flowie_mqtt_unsubscribe_view_t *out);
CXX_C_API int
flowie_mqtt_topic_filter_iterator_init(const flowie_mqtt_unsubscribe_view_t *unsubscribe,
                                       flowie_mqtt_topic_filter_iterator_t *iterator);
/** Return OK for one Topic Filter and NEED_MORE when the iterator is exhausted. */
CXX_C_API int flowie_mqtt_topic_filter_iterator_next(flowie_mqtt_topic_filter_iterator_t *iterator,
                                                     flowie_mqtt_span_t *out);

/**
 * Bounded client encoders. Output is modified only on success. `written` is
 * reset to zero before validation and receives the complete wire packet size.
 */
CXX_C_API int flowie_mqtt_connect_packet_encode(const flowie_mqtt_connect_packet_t *packet,
                                                uint8_t *output, size_t output_capacity,
                                                size_t *written);
CXX_C_API int flowie_mqtt_publish_packet_encode(const flowie_mqtt_publish_packet_t *packet,
                                                uint8_t *output, size_t output_capacity,
                                                size_t *written);
CXX_C_API int flowie_mqtt_subscribe_packet_encode(const flowie_mqtt_subscribe_packet_t *packet,
                                                  uint8_t *output, size_t output_capacity,
                                                  size_t *written);
CXX_C_API int flowie_mqtt_unsubscribe_packet_encode(const flowie_mqtt_unsubscribe_packet_t *packet,
                                                    uint8_t *output, size_t output_capacity,
                                                    size_t *written);
CXX_C_API int flowie_mqtt_pingreq_encode(flowie_mqtt_version_t version, uint8_t *output,
                                         size_t output_capacity, size_t *written);

/**
 * Caller-owned description for MQTT server control packets. `properties`
 * contains property bytes without the MQTT variable-byte length prefix;
 * reason_codes is used only by SUBACK and MQTT 5 UNSUBACK.
 */
typedef struct flowie_mqtt_control_packet_s {
  size_t size;
  uint32_t abi_version;
  flowie_mqtt_version_t version;
  flowie_mqtt_packet_type_t type;
  uint8_t session_present;
  uint16_t packet_id;
  uint8_t reason_code;
  flowie_mqtt_span_t properties;
  flowie_mqtt_span_t reason_codes;
} flowie_mqtt_control_packet_t;

#define FLOWIE_MQTT_CONTROL_PACKET_INIT                                                            \
  {sizeof(flowie_mqtt_control_packet_t), FLOWIE_MQTT_PROTOCOL_ABI_V1}

/**
 * Bounded encoder for CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP,
 * SUBACK, UNSUBACK, PINGRESP, DISCONNECT, and AUTH. Output is
 * modified only on success.
 */
CXX_C_API int flowie_mqtt_control_packet_encode(const flowie_mqtt_control_packet_t *packet,
                                                uint8_t *output, size_t output_capacity,
                                                size_t *written);

/** Decode every packet supported by flowie_mqtt_control_packet_encode(). */
CXX_C_API int flowie_mqtt_control_packet_parse(const flowie_mqtt_packet_view_t *packet,
                                               flowie_mqtt_control_packet_view_t *out);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_MQTT_PROTOCOL_H */
