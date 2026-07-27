#include "flowie.h"
#include "flowie_test_socket.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_ENDURANCE_SCENARIO_ID UINT32_C(0x454e4431)
#define FLOWIE_ENDURANCE_DEFAULT_SEED UINT64_C(0x464c4f574945)
#define FLOWIE_ENDURANCE_DEFAULT_MESSAGES 32u
#define FLOWIE_ENDURANCE_MAX_MESSAGES 10000u
#define FLOWIE_ENDURANCE_DEFAULT_TAKEOVERS 16u
#define FLOWIE_ENDURANCE_MAX_TAKEOVERS 1000u
#define FLOWIE_ENDURANCE_OFFLINE_MESSAGES 4u
#define FLOWIE_ENDURANCE_HEALTHY_AFTER_ISOLATION 32u
#define FLOWIE_ENDURANCE_ROUTING_ROUNDS 16u
#define FLOWIE_ENDURANCE_HISTORY_CAPACITY 256u
#define FLOWIE_ENDURANCE_PACKET_CAPACITY 1024u
#define FLOWIE_ENDURANCE_PAYLOAD_HEADER_SIZE 28u
#define FLOWIE_ENDURANCE_PAYLOAD_BODY_SIZE 37u
#define FLOWIE_ENDURANCE_PAYLOAD_SIZE                                                              \
  (FLOWIE_ENDURANCE_PAYLOAD_HEADER_SIZE + FLOWIE_ENDURANCE_PAYLOAD_BODY_SIZE)
#define FLOWIE_ENDURANCE_SESSION_EXPIRY_SECONDS 60u
#define FLOWIE_ENDURANCE_WILL_DELAY_SECONDS 1u
#define FLOWIE_ENDURANCE_WILL_EXPIRY_DELAY_SECONDS 5u
#define FLOWIE_ENDURANCE_WILL_OBSERVATION_MS 1200u
#define FLOWIE_ENDURANCE_WAIT_STEPS 3000u
#define FLOWIE_ENDURANCE_STOP_MAX_MS 3000u
#define FLOWIE_ENDURANCE_KEEP_ALIVE_SECONDS 60u
#define FLOWIE_ENDURANCE_RECV_TIMEOUT_MS 5000u
#define FLOWIE_ENDURANCE_SEND_HWM_BYTES (64u * 1024u)
#define FLOWIE_ENDURANCE_MAX_CONNECTIONS 8u
#define FLOWIE_ENDURANCE_MAX_SESSIONS 8u
#define FLOWIE_ENDURANCE_MAX_SUBSCRIPTIONS_PER_SESSION 8u
#define FLOWIE_ENDURANCE_MAX_INFLIGHT_PER_SESSION 8u

typedef enum flowie_endurance_segment_e {
  FLOWIE_ENDURANCE_SEGMENT_BOOTSTRAP = 1,
  FLOWIE_ENDURANCE_SEGMENT_STEADY,
  FLOWIE_ENDURANCE_SEGMENT_SESSION_CHURN,
  FLOWIE_ENDURANCE_SEGMENT_RECOVERY,
  FLOWIE_ENDURANCE_SEGMENT_DRAIN
} flowie_endurance_segment_t;

typedef enum flowie_endurance_role_e {
  FLOWIE_ENDURANCE_ROLE_PUBLISHER = 1,
  FLOWIE_ENDURANCE_ROLE_SUBSCRIBER
} flowie_endurance_role_t;

typedef enum flowie_endurance_state_e {
  FLOWIE_ENDURANCE_STATE_ABSENT = 0,
  FLOWIE_ENDURANCE_STATE_CONNECTING,
  FLOWIE_ENDURANCE_STATE_ONLINE,
  FLOWIE_ENDURANCE_STATE_SUBSCRIBED,
  FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT,
  FLOWIE_ENDURANCE_STATE_RESUMED,
  FLOWIE_ENDURANCE_STATE_TAKEN_OVER,
  FLOWIE_ENDURANCE_STATE_REMOVED
} flowie_endurance_state_t;

typedef enum flowie_endurance_read_policy_e {
  FLOWIE_ENDURANCE_READ_NORMAL = 0,
  FLOWIE_ENDURANCE_READ_PAUSED
} flowie_endurance_read_policy_t;

typedef struct flowie_endurance_client_profile_s {
  uint32_t id;
  const char *name;
  const char *client_id;
  flowie_mqtt_version_t version;
  flowie_endurance_role_t role;
  uint8_t clean_start;
  uint32_t session_expiry_seconds;
  uint16_t receive_maximum;
  flowie_endurance_read_policy_t read_policy;
} flowie_endurance_client_profile_t;

typedef struct flowie_endurance_client_s {
  const flowie_endurance_client_profile_t *profile;
  flowie_test_socket_t socket;
  flowie_endurance_state_t state;
  uint32_t next_sequence;
} flowie_endurance_client_t;

typedef struct flowie_endurance_operation_s {
  uint64_t operation_id;
  uint64_t producer_seed;
  uint32_t actor_id;
  uint32_t producer_sequence;
  flowie_endurance_segment_t segment;
  flowie_endurance_state_t state_before;
  flowie_endurance_state_t state_after;
  size_t payload_size;
} flowie_endurance_operation_t;

typedef struct flowie_endurance_history_s {
  flowie_endurance_operation_t records[FLOWIE_ENDURANCE_HISTORY_CAPACITY];
  size_t count;
  size_t next;
} flowie_endurance_history_t;

typedef struct flowie_endurance_control_s {
  flowie_mqtt_packet_type_t type;
  uint8_t session_present;
  uint8_t reason_code;
  uint16_t packet_id;
  uint8_t first_reason_code;
  size_t reason_code_count;
} flowie_endurance_control_t;

typedef struct flowie_endurance_message_key_s {
  uint64_t operation_id;
  uint64_t producer_seed;
  uint32_t actor_id;
  uint32_t producer_sequence;
} flowie_endurance_message_key_t;

typedef struct flowie_endurance_final_snapshot_s {
  turbo_flow_connection_snapshot_t connection;
  turbo_flow_resource_snapshot_t queue;
  turbo_flow_resource_snapshot_t sessions;
  uint64_t stop_elapsed_ms;
} flowie_endurance_final_snapshot_t;

typedef struct flowie_endurance_will_s {
  const uint8_t *topic;
  size_t topic_size;
  const uint8_t *payload;
  size_t payload_size;
  uint32_t delay_seconds;
  uint8_t qos;
} flowie_endurance_will_t;

static const flowie_endurance_client_profile_t FLOWIE_ENDURANCE_PROFILES[] = {
    {1u, "MQTT 3.1.1 stable publisher", "end-pub-v311", FLOWIE_MQTT_VERSION_3_1_1,
     FLOWIE_ENDURANCE_ROLE_PUBLISHER, 1u, 0u, 0u, FLOWIE_ENDURANCE_READ_NORMAL},
    {2u, "MQTT 5 stable publisher", "end-pub-v5", FLOWIE_MQTT_VERSION_5,
     FLOWIE_ENDURANCE_ROLE_PUBLISHER, 1u, 0u, 16u, FLOWIE_ENDURANCE_READ_NORMAL},
    {3u, "MQTT 3.1.1 wildcard subscriber", "end-sub-v311", FLOWIE_MQTT_VERSION_3_1_1,
     FLOWIE_ENDURANCE_ROLE_SUBSCRIBER, 1u, 0u, 0u, FLOWIE_ENDURANCE_READ_NORMAL},
    {4u, "MQTT 5 persistent subscriber", "end-sub-v5", FLOWIE_MQTT_VERSION_5,
     FLOWIE_ENDURANCE_ROLE_SUBSCRIBER, 0u, FLOWIE_ENDURANCE_SESSION_EXPIRY_SECONDS, 8u,
     FLOWIE_ENDURANCE_READ_NORMAL},
};

#define FLOWIE_ENDURANCE_PROFILE_COUNT                                                             \
  (sizeof(FLOWIE_ENDURANCE_PROFILES) / sizeof(FLOWIE_ENDURANCE_PROFILES[0]))

static const char FLOWIE_ENDURANCE_TOPIC[] = "endurance/data/value";
static const char FLOWIE_ENDURANCE_FILTER[] = "endurance/data/#";

static void flowie_endurance_store_u32(uint8_t *output, uint32_t value) {
  output[0] = (uint8_t)(value >> 24u);
  output[1] = (uint8_t)(value >> 16u);
  output[2] = (uint8_t)(value >> 8u);
  output[3] = (uint8_t)value;
}

static void flowie_endurance_store_u64(uint8_t *output, uint64_t value) {
  flowie_endurance_store_u32(output, (uint32_t)(value >> 32u));
  flowie_endurance_store_u32(output + 4u, (uint32_t)value);
}

static uint32_t flowie_endurance_load_u32(const uint8_t *input) {
  return ((uint32_t)input[0] << 24u) | ((uint32_t)input[1] << 16u) | ((uint32_t)input[2] << 8u) |
         (uint32_t)input[3];
}

static uint64_t flowie_endurance_load_u64(const uint8_t *input) {
  return ((uint64_t)flowie_endurance_load_u32(input) << 32u) |
         (uint64_t)flowie_endurance_load_u32(input + 4u);
}

static uint64_t flowie_endurance_client_seed(uint64_t root_seed, uint32_t client_id) {
  return root_seed ^ (UINT64_C(0x9e3779b97f4a7c15) * (uint64_t)(client_id + 1u));
}

static int flowie_endurance_env_size(const char *name, size_t default_value, size_t max_value,
                                     size_t *result) {
  const char *value;
  char *end = NULL;
  unsigned long long parsed;
  if (!name || default_value == 0u || max_value < default_value || !result) return TURBO_EINVAL;
  value = getenv(name);
  if (!value) {
    *result = default_value;
    return TURBO_OK;
  }
  if (value[0] < '1' || value[0] > '9') return TURBO_EINVAL;
  errno = 0;
  parsed = strtoull(value, &end, 10);
  if (errno == ERANGE || !end || *end != '\0' || parsed == 0u || parsed > max_value)
    return TURBO_EINVAL;
  *result = (size_t)parsed;
  return TURBO_OK;
}

static int flowie_endurance_message_count(size_t *result) {
  return flowie_endurance_env_size("FLOWIE_MQTT_ENDURANCE_MESSAGES",
                                   FLOWIE_ENDURANCE_DEFAULT_MESSAGES, FLOWIE_ENDURANCE_MAX_MESSAGES,
                                   result);
}

static int flowie_endurance_takeover_count(size_t *result) {
  return flowie_endurance_env_size("FLOWIE_MQTT_ENDURANCE_TAKEOVERS",
                                   FLOWIE_ENDURANCE_DEFAULT_TAKEOVERS,
                                   FLOWIE_ENDURANCE_MAX_TAKEOVERS, result);
}

static int flowie_endurance_seed(uint64_t *result) {
  const char *value = getenv("FLOWIE_MQTT_ENDURANCE_SEED");
  char *end = NULL;
  unsigned long long parsed;
  if (!result) return TURBO_EINVAL;
  if (!value) {
    *result = FLOWIE_ENDURANCE_DEFAULT_SEED;
    return TURBO_OK;
  }
  if (value[0] < '0' || value[0] > '9') return TURBO_EINVAL;
  errno = 0;
  parsed = strtoull(value, &end, 0);
  if (errno == ERANGE || !end || *end != '\0' || parsed == 0u) return TURBO_EINVAL;
  *result = (uint64_t)parsed;
  return TURBO_OK;
}

static int flowie_endurance_state_transition_allowed(flowie_endurance_state_t before,
                                                     flowie_endurance_state_t after) {
  switch (before) {
  case FLOWIE_ENDURANCE_STATE_ABSENT:
    return after == FLOWIE_ENDURANCE_STATE_CONNECTING;
  case FLOWIE_ENDURANCE_STATE_CONNECTING:
    return after == FLOWIE_ENDURANCE_STATE_ONLINE || after == FLOWIE_ENDURANCE_STATE_RESUMED;
  case FLOWIE_ENDURANCE_STATE_ONLINE:
    return after == FLOWIE_ENDURANCE_STATE_SUBSCRIBED ||
           after == FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT ||
           after == FLOWIE_ENDURANCE_STATE_REMOVED;
  case FLOWIE_ENDURANCE_STATE_SUBSCRIBED:
    return after == FLOWIE_ENDURANCE_STATE_ONLINE ||
           after == FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT ||
           after == FLOWIE_ENDURANCE_STATE_TAKEN_OVER || after == FLOWIE_ENDURANCE_STATE_REMOVED;
  case FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT:
    return after == FLOWIE_ENDURANCE_STATE_CONNECTING;
  case FLOWIE_ENDURANCE_STATE_RESUMED:
    return after == FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT ||
           after == FLOWIE_ENDURANCE_STATE_TAKEN_OVER || after == FLOWIE_ENDURANCE_STATE_REMOVED;
  case FLOWIE_ENDURANCE_STATE_TAKEN_OVER:
    return after == FLOWIE_ENDURANCE_STATE_REMOVED;
  case FLOWIE_ENDURANCE_STATE_REMOVED:
    return after == FLOWIE_ENDURANCE_STATE_CONNECTING;
  default:
    return 0;
  }
}

static int flowie_endurance_transition(flowie_endurance_client_t *client,
                                       flowie_endurance_state_t after) {
  if (!client || !flowie_endurance_state_transition_allowed(client->state, after))
    return TURBO_EPROTO;
  client->state = after;
  return TURBO_OK;
}

static void flowie_endurance_history_append(flowie_endurance_history_t *history,
                                            const flowie_endurance_operation_t *operation) {
  history->records[history->next] = *operation;
  history->next = (history->next + 1u) % FLOWIE_ENDURANCE_HISTORY_CAPACITY;
  if (history->count < FLOWIE_ENDURANCE_HISTORY_CAPACITY) ++history->count;
}

static const flowie_endurance_operation_t *
flowie_endurance_history_oldest(const flowie_endurance_history_t *history) {
  size_t index;
  if (!history || history->count == 0u) return NULL;
  index = history->count == FLOWIE_ENDURANCE_HISTORY_CAPACITY ? history->next : 0u;
  return &history->records[index];
}

static void flowie_endurance_payload_build(const flowie_endurance_message_key_t *key,
                                           uint8_t payload[FLOWIE_ENDURANCE_PAYLOAD_SIZE]) {
  flowie_endurance_store_u32(payload, FLOWIE_ENDURANCE_SCENARIO_ID);
  flowie_endurance_store_u32(payload + 4u, key->actor_id);
  flowie_endurance_store_u32(payload + 8u, key->producer_sequence);
  flowie_endurance_store_u64(payload + 12u, key->operation_id);
  flowie_endurance_store_u32(payload + 20u, FLOWIE_ENDURANCE_PAYLOAD_BODY_SIZE);
  flowie_endurance_store_u32(payload + 24u, (uint32_t)key->producer_seed);
  for (size_t i = 0u; i < FLOWIE_ENDURANCE_PAYLOAD_BODY_SIZE; ++i) {
    const unsigned int shift = (unsigned int)((i % 8u) * 8u);
    payload[FLOWIE_ENDURANCE_PAYLOAD_HEADER_SIZE + i] =
        (uint8_t)((key->producer_seed >> shift) ^ key->operation_id ^ i);
  }
}

static int flowie_endurance_payload_validate(const uint8_t *payload, size_t payload_size,
                                             const flowie_endurance_message_key_t *expected) {
  uint8_t rebuilt[FLOWIE_ENDURANCE_PAYLOAD_SIZE];
  if (!payload || !expected || payload_size != sizeof(rebuilt) ||
      flowie_endurance_load_u32(payload) != FLOWIE_ENDURANCE_SCENARIO_ID ||
      flowie_endurance_load_u32(payload + 4u) != expected->actor_id ||
      flowie_endurance_load_u32(payload + 8u) != expected->producer_sequence ||
      flowie_endurance_load_u64(payload + 12u) != expected->operation_id ||
      flowie_endurance_load_u32(payload + 20u) != FLOWIE_ENDURANCE_PAYLOAD_BODY_SIZE ||
      flowie_endurance_load_u32(payload + 24u) != (uint32_t)expected->producer_seed)
    return TURBO_EPROTO;
  flowie_endurance_payload_build(expected, rebuilt);
  return memcmp(payload, rebuilt, sizeof(rebuilt)) == 0 ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_endurance_recv_packet(flowie_test_socket_t socket, uint8_t *wire, size_t capacity,
                                        size_t *wire_size) {
  uint32_t remaining = 0u;
  uint32_t multiplier = 1u;
  size_t fixed_size = 1u;
  int rc;
  if (!wire || capacity < 2u || !wire_size) return TURBO_EINVAL;
  *wire_size = 0u;
  rc = flowie_test_recv_exact(socket, wire, 1u);
  if (rc != TURBO_OK) return rc;
  for (;;) {
    uint8_t byte;
    if (fixed_size >= 5u) return TURBO_EPROTO;
    rc = flowie_test_recv_exact(socket, &byte, 1u);
    if (rc != TURBO_OK) return rc;
    wire[fixed_size++] = byte;
    remaining += (uint32_t)(byte & UINT8_C(0x7f)) * multiplier;
    if ((byte & UINT8_C(0x80)) == 0u) break;
    if (multiplier > UINT32_MAX / 128u) return TURBO_EPROTO;
    multiplier *= 128u;
  }
  if ((size_t)remaining > capacity - fixed_size) return TURBO_EMSGSIZE;
  rc = flowie_test_recv_exact(socket, wire + fixed_size, remaining);
  if (rc != TURBO_OK) return rc;
  *wire_size = fixed_size + (size_t)remaining;
  return TURBO_OK;
}

static int flowie_endurance_recv_control(flowie_test_socket_t socket, flowie_mqtt_version_t version,
                                         flowie_endurance_control_t *result) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_control_packet_view_t control = FLOWIE_MQTT_CONTROL_PACKET_VIEW_INIT;
  uint8_t wire[FLOWIE_ENDURANCE_PACKET_CAPACITY];
  size_t wire_size = 0u;
  size_t consumed = 0u;
  int rc;
  if (!result) return TURBO_EINVAL;
  memset(result, 0, sizeof(*result));
  rc = flowie_endurance_recv_packet(socket, wire, sizeof(wire), &wire_size);
  if (rc != TURBO_OK) return rc;
  options.version = version;
  options.max_packet_size = sizeof(wire);
  if (flowie_mqtt_packet_parse(wire, wire_size, &options, &packet, &consumed, NULL) !=
          FLOWIE_MQTT_PARSE_OK ||
      consumed != wire_size ||
      flowie_mqtt_control_packet_parse(&packet, &control) != FLOWIE_MQTT_PARSE_OK)
    return TURBO_EPROTO;
  result->type = control.type;
  result->session_present = control.session_present;
  result->reason_code = control.reason_code;
  result->packet_id = control.packet_id;
  result->reason_code_count = control.reason_codes.size;
  if (control.reason_codes.size != 0u) result->first_reason_code = control.reason_codes.data[0];
  return TURBO_OK;
}

static int flowie_endurance_send_control(flowie_test_socket_t socket, flowie_mqtt_version_t version,
                                         flowie_mqtt_packet_type_t type, uint16_t packet_id) {
  flowie_mqtt_control_packet_t control = FLOWIE_MQTT_CONTROL_PACKET_INIT;
  uint8_t wire[16];
  size_t wire_size = 0u;
  control.version = version;
  control.type = type;
  control.packet_id = packet_id;
  if (flowie_mqtt_control_packet_encode(&control, wire, sizeof(wire), &wire_size) !=
      FLOWIE_MQTT_PARSE_OK)
    return TURBO_EPROTO;
  return flowie_test_send(socket, wire, wire_size);
}

static int
flowie_endurance_recv_publish_stage_on_topic(flowie_endurance_client_t *client,
                                             const flowie_endurance_message_key_t *expected,
                                             const char *expected_topic, uint8_t expected_qos,
                                             uint8_t expected_duplicate, uint16_t *packet_id) {
  flowie_mqtt_parse_options_t options = FLOWIE_MQTT_PARSE_OPTIONS_INIT;
  flowie_mqtt_packet_view_t packet = FLOWIE_MQTT_PACKET_VIEW_INIT;
  flowie_mqtt_publish_view_t publish = FLOWIE_MQTT_PUBLISH_VIEW_INIT;
  uint8_t wire[FLOWIE_ENDURANCE_PACKET_CAPACITY];
  size_t wire_size = 0u;
  size_t consumed = 0u;
  int rc;
  size_t expected_topic_size;
  if (!client || client->socket == FLOWIE_TEST_INVALID_SOCKET || !expected || !expected_topic)
    return TURBO_EINVAL;
  expected_topic_size = strlen(expected_topic);
  rc = flowie_endurance_recv_packet(client->socket, wire, sizeof(wire), &wire_size);
  if (rc != TURBO_OK) return rc;
  options.version = client->profile->version;
  options.max_packet_size = sizeof(wire);
  if (flowie_mqtt_packet_parse(wire, wire_size, &options, &packet, &consumed, NULL) !=
          FLOWIE_MQTT_PARSE_OK ||
      consumed != wire_size ||
      flowie_mqtt_publish_parse(&packet, &publish) != FLOWIE_MQTT_PARSE_OK ||
      publish.qos != expected_qos || publish.duplicate != expected_duplicate ||
      publish.topic.size != expected_topic_size ||
      memcmp(publish.topic.data, expected_topic, expected_topic_size) != 0)
    return TURBO_EPROTO;
  rc = flowie_endurance_payload_validate(publish.payload.data, publish.payload.size, expected);
  if (rc != TURBO_OK) return rc;
  if (packet_id) *packet_id = publish.packet_id;
  return TURBO_OK;
}

static int flowie_endurance_recv_publish_stage(flowie_endurance_client_t *client,
                                               const flowie_endurance_message_key_t *expected,
                                               uint8_t expected_qos, uint8_t expected_duplicate,
                                               uint16_t *packet_id) {
  return flowie_endurance_recv_publish_stage_on_topic(client, expected, FLOWIE_ENDURANCE_TOPIC,
                                                      expected_qos, expected_duplicate, packet_id);
}

static int flowie_endurance_recv_publish_on_topic(flowie_endurance_client_t *client,
                                                  const flowie_endurance_message_key_t *expected,
                                                  const char *expected_topic) {
  uint16_t packet_id = 0u;
  int rc = flowie_endurance_recv_publish_stage_on_topic(client, expected, expected_topic, 1u, 0u,
                                                        &packet_id);
  return rc == TURBO_OK ? flowie_endurance_send_control(client->socket, client->profile->version,
                                                        FLOWIE_MQTT_PACKET_PUBACK, packet_id)
                        : rc;
}

static int flowie_endurance_recv_publish(flowie_endurance_client_t *client,
                                         const flowie_endurance_message_key_t *expected) {
  return flowie_endurance_recv_publish_on_topic(client, expected, FLOWIE_ENDURANCE_TOPIC);
}

static int flowie_endurance_recv_publish_unsettled(flowie_endurance_client_t *client,
                                                   const flowie_endurance_message_key_t *expected) {
  return flowie_endurance_recv_publish_stage(client, expected, 1u, 0u, NULL);
}

static int flowie_endurance_recv_shared_publish(flowie_endurance_client_t *first,
                                                flowie_endurance_client_t *second,
                                                const flowie_endurance_message_key_t *expected,
                                                const char *expected_topic, size_t *winner_index) {
  flowie_endurance_client_t *winner;
  flowie_endurance_client_t *other;
  uint16_t packet_id = 0u;
  int first_ready;
  int second_ready;
  int rc;
  if (!first || !second || !expected || !expected_topic || !winner_index ||
      first->socket == FLOWIE_TEST_INVALID_SOCKET || second->socket == FLOWIE_TEST_INVALID_SOCKET)
    return TURBO_EINVAL;
  for (size_t step = 0u; step < FLOWIE_ENDURANCE_WAIT_STEPS; ++step) {
    first_ready = flowie_test_socket_readable(first->socket, 0u);
    second_ready = flowie_test_socket_readable(second->socket, 0u);
    if (first_ready && second_ready) return TURBO_EPROTO;
    if (first_ready || second_ready) {
      *winner_index = first_ready ? 0u : 1u;
      winner = first_ready ? first : second;
      other = first_ready ? second : first;
      rc = flowie_endurance_recv_publish_stage_on_topic(winner, expected, expected_topic, 1u, 0u,
                                                        &packet_id);
      if (rc != TURBO_OK) return rc;
      rc = flowie_endurance_send_control(winner->socket, winner->profile->version,
                                         FLOWIE_MQTT_PACKET_PUBACK, packet_id);
      if (rc != TURBO_OK) return rc;
      return flowie_test_socket_readable(other->socket, 50u) ? TURBO_EPROTO : TURBO_OK;
    }
    turbo_sleep_ms(1u);
  }
  return TURBO_ETIMEDOUT;
}

static int flowie_endurance_connect_with_will(flowie_endurance_client_t *client,
                                              unsigned short port, uint8_t expected_session_present,
                                              const flowie_endurance_will_t *will) {
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_endurance_control_t connack = {0};
  uint8_t properties[8];
  uint8_t will_properties[5];
  uint8_t wire[FLOWIE_ENDURANCE_PACKET_CAPACITY];
  size_t properties_size = 0u;
  size_t wire_size = 0u;
  int rc;
  if (!client || !client->profile || port == 0u ||
      (will && (client->profile->version != FLOWIE_MQTT_VERSION_5 || !will->topic ||
                will->topic_size == 0u || will->qos > 2u)))
    return TURBO_EINVAL;
  rc = flowie_endurance_transition(client, FLOWIE_ENDURANCE_STATE_CONNECTING);
  if (rc != TURBO_OK) return rc;
  connect.version = client->profile->version;
  connect.clean_start = client->profile->clean_start;
  connect.keep_alive = FLOWIE_ENDURANCE_KEEP_ALIVE_SECONDS;
  connect.client_id = (flowie_mqtt_span_t){(const uint8_t *)client->profile->client_id,
                                           strlen(client->profile->client_id)};
  if (client->profile->version == FLOWIE_MQTT_VERSION_5) {
    if (client->profile->session_expiry_seconds != 0u) {
      properties[properties_size++] = FLOWIE_MQTT_PROPERTY_SESSION_EXPIRY_INTERVAL;
      flowie_endurance_store_u32(properties + properties_size,
                                 client->profile->session_expiry_seconds);
      properties_size += 4u;
    }
    if (client->profile->receive_maximum != 0u) {
      properties[properties_size++] = FLOWIE_MQTT_PROPERTY_RECEIVE_MAXIMUM;
      properties[properties_size++] = (uint8_t)(client->profile->receive_maximum >> 8u);
      properties[properties_size++] = (uint8_t)client->profile->receive_maximum;
    }
    connect.properties = (flowie_mqtt_span_t){properties, properties_size};
  }
  if (will) {
    connect.has_will = 1u;
    connect.will_qos = will->qos;
    connect.will_topic = (flowie_mqtt_span_t){will->topic, will->topic_size};
    connect.will_payload = (flowie_mqtt_span_t){will->payload, will->payload_size};
    if (will->delay_seconds != 0u) {
      will_properties[0] = FLOWIE_MQTT_PROPERTY_WILL_DELAY_INTERVAL;
      flowie_endurance_store_u32(will_properties + 1u, will->delay_seconds);
      connect.will_properties = (flowie_mqtt_span_t){will_properties, sizeof(will_properties)};
    }
  }
  if (flowie_mqtt_connect_packet_encode(&connect, wire, sizeof(wire), &wire_size) !=
      FLOWIE_MQTT_PARSE_OK)
    return TURBO_EPROTO;
  client->socket = flowie_test_connect(port);
  if (client->socket == FLOWIE_TEST_INVALID_SOCKET) return TURBO_EIO;
  rc = flowie_test_send(client->socket, wire, wire_size);
  if (rc == TURBO_OK)
    rc = flowie_endurance_recv_control(client->socket, client->profile->version, &connack);
  if (rc != TURBO_OK || connack.type != FLOWIE_MQTT_PACKET_CONNACK || connack.reason_code != 0u ||
      connack.session_present != expected_session_present)
    return TURBO_EPROTO;
  return flowie_endurance_transition(client, expected_session_present
                                                 ? FLOWIE_ENDURANCE_STATE_RESUMED
                                                 : FLOWIE_ENDURANCE_STATE_ONLINE);
}

static int flowie_endurance_connect_at(flowie_endurance_client_t *client, unsigned short port,
                                       uint8_t expected_session_present) {
  return flowie_endurance_connect_with_will(client, port, expected_session_present, NULL);
}

static int flowie_endurance_subscribe_filter_qos(flowie_endurance_client_t *client,
                                                 const char *filter, uint8_t qos) {
  flowie_mqtt_subscription_t subscription = {0};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  flowie_endurance_control_t suback = {0};
  uint8_t wire[FLOWIE_ENDURANCE_PACKET_CAPACITY];
  size_t wire_size = 0u;
  int rc;
  if (!client || !filter || !filter[0] ||
      client->profile->role != FLOWIE_ENDURANCE_ROLE_SUBSCRIBER ||
      client->state != FLOWIE_ENDURANCE_STATE_ONLINE || qos > 2u)
    return TURBO_EINVAL;
  subscription.filter = (flowie_mqtt_span_t){(const uint8_t *)filter, strlen(filter)};
  subscription.qos = qos;
  subscribe.version = client->profile->version;
  subscribe.packet_id = 1u;
  subscribe.subscriptions = &subscription;
  subscribe.subscription_count = 1u;
  if (flowie_mqtt_subscribe_packet_encode(&subscribe, wire, sizeof(wire), &wire_size) !=
      FLOWIE_MQTT_PARSE_OK)
    return TURBO_EPROTO;
  rc = flowie_test_send(client->socket, wire, wire_size);
  if (rc == TURBO_OK)
    rc = flowie_endurance_recv_control(client->socket, client->profile->version, &suback);
  if (rc != TURBO_OK || suback.type != FLOWIE_MQTT_PACKET_SUBACK ||
      suback.packet_id != subscribe.packet_id || suback.reason_code_count != 1u ||
      suback.first_reason_code != qos)
    return TURBO_EPROTO;
  return flowie_endurance_transition(client, FLOWIE_ENDURANCE_STATE_SUBSCRIBED);
}

static int flowie_endurance_subscribe_qos(flowie_endurance_client_t *client, uint8_t qos) {
  return flowie_endurance_subscribe_filter_qos(client, FLOWIE_ENDURANCE_FILTER, qos);
}

static int flowie_endurance_subscribe(flowie_endurance_client_t *client) {
  return flowie_endurance_subscribe_qos(client, 1u);
}

static int flowie_endurance_unsubscribe_filter(flowie_endurance_client_t *client,
                                               const char *filter) {
  flowie_mqtt_unsubscribe_packet_t unsubscribe = FLOWIE_MQTT_UNSUBSCRIBE_PACKET_INIT;
  flowie_mqtt_span_t filter_span;
  flowie_endurance_control_t unsuback = {0};
  uint8_t wire[FLOWIE_ENDURANCE_PACKET_CAPACITY];
  size_t wire_size = 0u;
  int rc;
  if (!client || !filter || !filter[0] || client->state != FLOWIE_ENDURANCE_STATE_SUBSCRIBED)
    return TURBO_EINVAL;
  filter_span = (flowie_mqtt_span_t){(const uint8_t *)filter, strlen(filter)};
  unsubscribe.version = client->profile->version;
  unsubscribe.packet_id = 2u;
  unsubscribe.filters = &filter_span;
  unsubscribe.filter_count = 1u;
  if (flowie_mqtt_unsubscribe_packet_encode(&unsubscribe, wire, sizeof(wire), &wire_size) !=
      FLOWIE_MQTT_PARSE_OK)
    return TURBO_EPROTO;
  rc = flowie_test_send(client->socket, wire, wire_size);
  if (rc == TURBO_OK)
    rc = flowie_endurance_recv_control(client->socket, client->profile->version, &unsuback);
  if (rc != TURBO_OK || unsuback.type != FLOWIE_MQTT_PACKET_UNSUBACK ||
      unsuback.packet_id != unsubscribe.packet_id ||
      (client->profile->version == FLOWIE_MQTT_VERSION_5 &&
       (unsuback.reason_code_count != 1u || unsuback.first_reason_code >= UINT8_C(0x80))))
    return TURBO_EPROTO;
  return flowie_endurance_transition(client, FLOWIE_ENDURANCE_STATE_ONLINE);
}

static int flowie_endurance_publish_qos_on_topic(flowie_endurance_client_t *publisher,
                                                 const flowie_endurance_message_key_t *key,
                                                 const char *topic, uint8_t qos) {
  flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  flowie_endurance_control_t control = {0};
  uint8_t payload[FLOWIE_ENDURANCE_PAYLOAD_SIZE];
  uint8_t wire[FLOWIE_ENDURANCE_PACKET_CAPACITY];
  size_t wire_size = 0u;
  uint16_t packet_id;
  int rc;
  if (!publisher || !key || !topic || !topic[0] || (qos != 1u && qos != 2u) ||
      publisher->profile->role != FLOWIE_ENDURANCE_ROLE_PUBLISHER ||
      publisher->state != FLOWIE_ENDURANCE_STATE_ONLINE)
    return TURBO_EINVAL;
  packet_id = (uint16_t)((key->operation_id % UINT16_MAX) + 1u);
  flowie_endurance_payload_build(key, payload);
  publish.version = publisher->profile->version;
  publish.qos = qos;
  publish.packet_id = packet_id;
  publish.topic = (flowie_mqtt_span_t){(const uint8_t *)topic, strlen(topic)};
  publish.payload = (flowie_mqtt_span_t){payload, sizeof(payload)};
  if (flowie_mqtt_publish_packet_encode(&publish, wire, sizeof(wire), &wire_size) !=
      FLOWIE_MQTT_PARSE_OK)
    return TURBO_EPROTO;
  rc = flowie_test_send(publisher->socket, wire, wire_size);
  if (rc == TURBO_OK)
    rc = flowie_endurance_recv_control(publisher->socket, publisher->profile->version, &control);
  if (rc != TURBO_OK ||
      control.type != (qos == 1u ? FLOWIE_MQTT_PACKET_PUBACK : FLOWIE_MQTT_PACKET_PUBREC) ||
      control.packet_id != packet_id || control.reason_code >= UINT8_C(0x80))
    return TURBO_EPROTO;
  if (qos == 2u) {
    rc = flowie_endurance_send_control(publisher->socket, publisher->profile->version,
                                       FLOWIE_MQTT_PACKET_PUBREL, packet_id);
    if (rc == TURBO_OK)
      rc = flowie_endurance_recv_control(publisher->socket, publisher->profile->version, &control);
    if (rc != TURBO_OK || control.type != FLOWIE_MQTT_PACKET_PUBCOMP ||
        control.packet_id != packet_id || control.reason_code >= UINT8_C(0x80))
      return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flowie_endurance_publish_qos(flowie_endurance_client_t *publisher,
                                        const flowie_endurance_message_key_t *key, uint8_t qos) {
  return flowie_endurance_publish_qos_on_topic(publisher, key, FLOWIE_ENDURANCE_TOPIC, qos);
}

static int flowie_endurance_publish(flowie_endurance_client_t *publisher,
                                    const flowie_endurance_message_key_t *key) {
  return flowie_endurance_publish_qos(publisher, key, 1u);
}

static int flowie_endurance_disconnect(flowie_endurance_client_t *client) {
  static const uint8_t disconnect[] = {0xe0u, 0x00u};
  int rc;
  if (!client || client->socket == FLOWIE_TEST_INVALID_SOCKET) return TURBO_EINVAL;
  rc = flowie_test_send(client->socket, disconnect, sizeof(disconnect));
  flowie_test_socket_close(client->socket);
  client->socket = FLOWIE_TEST_INVALID_SOCKET;
  if (rc != TURBO_OK) return rc;
  return flowie_endurance_transition(client, FLOWIE_ENDURANCE_STATE_REMOVED);
}

static int flowie_endurance_expect_takeover(flowie_endurance_client_t *client) {
  flowie_endurance_control_t disconnect = {0};
  int rc;
  if (!client || !client->profile || client->profile->version != FLOWIE_MQTT_VERSION_5 ||
      client->socket == FLOWIE_TEST_INVALID_SOCKET)
    return TURBO_EINVAL;
  rc = flowie_endurance_recv_control(client->socket, client->profile->version, &disconnect);
  if (rc != TURBO_OK || disconnect.type != FLOWIE_MQTT_PACKET_DISCONNECT ||
      disconnect.reason_code != UINT8_C(0x8e))
    return TURBO_EPROTO;
  rc = flowie_endurance_transition(client, FLOWIE_ENDURANCE_STATE_TAKEN_OVER);
  flowie_test_socket_close(client->socket);
  client->socket = FLOWIE_TEST_INVALID_SOCKET;
  if (rc != TURBO_OK) return rc;
  return flowie_endurance_transition(client, FLOWIE_ENDURANCE_STATE_REMOVED);
}

static int flowie_endurance_wait_connections(turbo_flow_t *flow, size_t expected) {
  turbo_flow_connection_snapshot_t snapshot = {0};
  for (size_t i = 0u; i < FLOWIE_ENDURANCE_WAIT_STEPS; ++i) {
    int rc = turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot);
    if (rc != TURBO_OK) return rc;
    if (snapshot.connections_current == expected) return TURBO_OK;
    turbo_sleep_ms(1u);
  }
  return TURBO_ETIMEDOUT;
}

static turbo_flow_t *flowie_endurance_flow_with_limits(unsigned short port, size_t send_hwm_bytes,
                                                       size_t max_inflight_per_session) {
  static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                              "stage mqtt_fanout adapter flowie.endpoint\n"
                              "stage main {\n"
                              "  mqtt_in -> mqtt_fanout\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  turbo_flow_t *flow;
  if (port == 0u || send_hwm_bytes == 0u || max_inflight_per_session == 0u) return NULL;
  flow = turbo_flow_create();
  if (!flow) return NULL;
  config.host = "127.0.0.1";
  config.port = (int)port;
  config.max_packet_size = FLOWIE_ENDURANCE_PACKET_CAPACITY;
  config.max_connections = FLOWIE_ENDURANCE_MAX_CONNECTIONS;
  config.coroutine_stack_size = FLOWIE_MIN_COROUTINE_STACK_SIZE;
  config.recv_timeout_ms = FLOWIE_ENDURANCE_RECV_TIMEOUT_MS;
  config.send_hwm_bytes = send_hwm_bytes;
  config.manage_sessions = 1;
  config.max_sessions = FLOWIE_ENDURANCE_MAX_SESSIONS;
  config.max_subscriptions_per_session = FLOWIE_ENDURANCE_MAX_SUBSCRIPTIONS_PER_SESSION;
  config.max_inflight_per_session = max_inflight_per_session;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *flowie_endurance_flow(unsigned short port) {
  return flowie_endurance_flow_with_limits(port, FLOWIE_ENDURANCE_SEND_HWM_BYTES,
                                           FLOWIE_ENDURANCE_MAX_INFLIGHT_PER_SESSION);
}

static int flowie_endurance_stop_drained(turbo_flow_t *flow, size_t expected_sessions,
                                         flowie_endurance_final_snapshot_t *result) {
  int rc;
  uint64_t stopped_at;
  if (!flow || !result) return TURBO_EINVAL;
  memset(result, 0, sizeof(*result));
  result->queue = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  result->sessions = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  stopped_at = turbo_monotonic_ms();
  rc = turbo_flow_stop(flow);
  result->stop_elapsed_ms = turbo_monotonic_ms() - stopped_at;
  if (rc != TURBO_OK) return rc;
  if (result->stop_elapsed_ms > FLOWIE_ENDURANCE_STOP_MAX_MS) return TURBO_ETIMEDOUT;
  rc = turbo_flow_adapter_connection_snapshot_at(flow, 0u, &result->connection);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_resource_snapshot_at(flow, 1u, &result->queue);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_resource_snapshot_at(flow, 2u, &result->sessions);
  if (rc != TURBO_OK) return rc;
  if (result->connection.state != TURBO_FLOW_CONNECTION_STOPPED ||
      result->connection.connections_current != 0u || result->connection.in_flight_messages != 0u ||
      result->connection.in_flight_bytes != 0u || result->queue.load != 0u ||
      result->sessions.load != expected_sessions)
    return TURBO_EPROTO;
  return TURBO_OK;
}

static flowie_endurance_message_key_t
flowie_endurance_next_key(flowie_endurance_client_t *publisher, uint64_t root_seed,
                          uint64_t operation_id) {
  flowie_endurance_message_key_t key;
  key.operation_id = operation_id;
  key.producer_seed = flowie_endurance_client_seed(root_seed, publisher->profile->id);
  key.actor_id = publisher->profile->id;
  key.producer_sequence = publisher->next_sequence++;
  return key;
}

spec("Flowie MQTT persistent-instance endurance") {
  it("keeps client profiles protocol-valid and identity-unique") {
    check_size_eq(FLOWIE_ENDURANCE_PROFILE_COUNT, 4u);
    for (size_t i = 0u; i < FLOWIE_ENDURANCE_PROFILE_COUNT; ++i) {
      check_not_null(FLOWIE_ENDURANCE_PROFILES[i].name);
      check_not_null(FLOWIE_ENDURANCE_PROFILES[i].client_id);
      check_true(FLOWIE_ENDURANCE_PROFILES[i].version == FLOWIE_MQTT_VERSION_3_1_1 ||
                 FLOWIE_ENDURANCE_PROFILES[i].version == FLOWIE_MQTT_VERSION_5);
      check_true(FLOWIE_ENDURANCE_PROFILES[i].read_policy == FLOWIE_ENDURANCE_READ_NORMAL ||
                 FLOWIE_ENDURANCE_PROFILES[i].read_policy == FLOWIE_ENDURANCE_READ_PAUSED);
      if (FLOWIE_ENDURANCE_PROFILES[i].version != FLOWIE_MQTT_VERSION_5)
        check_uint_eq(FLOWIE_ENDURANCE_PROFILES[i].session_expiry_seconds, 0u);
      for (size_t j = i + 1u; j < FLOWIE_ENDURANCE_PROFILE_COUNT; ++j) {
        check_uint_ne(FLOWIE_ENDURANCE_PROFILES[i].id, FLOWIE_ENDURANCE_PROFILES[j].id);
        check_str_ne(FLOWIE_ENDURANCE_PROFILES[i].client_id,
                     FLOWIE_ENDURANCE_PROFILES[j].client_id);
      }
    }
  }

  it("rejects illegal client-state transitions") {
    flowie_endurance_client_t client = {&FLOWIE_ENDURANCE_PROFILES[3], FLOWIE_TEST_INVALID_SOCKET,
                                        FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    check_int_eq(flowie_endurance_transition(&client, FLOWIE_ENDURANCE_STATE_SUBSCRIBED),
                 TURBO_EPROTO);
    check_int_eq(client.state, FLOWIE_ENDURANCE_STATE_ABSENT);
    check_int_eq(flowie_endurance_transition(&client, FLOWIE_ENDURANCE_STATE_CONNECTING), TURBO_OK);
    check_int_eq(flowie_endurance_transition(&client, FLOWIE_ENDURANCE_STATE_ONLINE), TURBO_OK);
    check_int_eq(flowie_endurance_transition(&client, FLOWIE_ENDURANCE_STATE_SUBSCRIBED), TURBO_OK);
    check_int_eq(flowie_endurance_transition(&client, FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT),
                 TURBO_OK);
    check_int_eq(flowie_endurance_transition(&client, FLOWIE_ENDURANCE_STATE_CONNECTING), TURBO_OK);
    check_int_eq(flowie_endurance_transition(&client, FLOWIE_ENDURANCE_STATE_RESUMED), TURBO_OK);
  }

  it("bounds replay history while preserving the newest operation metadata") {
    flowie_endurance_history_t history = {0};
    flowie_endurance_operation_t operation = {0};
    const size_t appended = FLOWIE_ENDURANCE_HISTORY_CAPACITY + 3u;
    for (size_t i = 0u; i < appended; ++i) {
      operation.operation_id = (uint64_t)i;
      operation.actor_id = 2u;
      operation.payload_size = FLOWIE_ENDURANCE_PAYLOAD_SIZE;
      flowie_endurance_history_append(&history, &operation);
    }
    check_size_eq(history.count, FLOWIE_ENDURANCE_HISTORY_CAPACITY);
    check_not_null(flowie_endurance_history_oldest(&history));
    check_uint_eq((unsigned int)flowie_endurance_history_oldest(&history)->operation_id, 3u);
    check_uint_eq((unsigned int)history
                      .records[(history.next + FLOWIE_ENDURANCE_HISTORY_CAPACITY - 1u) %
                               FLOWIE_ENDURANCE_HISTORY_CAPACITY]
                      .operation_id,
                  (unsigned int)(appended - 1u));
  }

  it("ENDURANCE-001 preserves fan-out and persistent replay on one broker instance") {
    flowie_endurance_client_t clients[] = {
        {&FLOWIE_ENDURANCE_PROFILES[0], FLOWIE_TEST_INVALID_SOCKET, FLOWIE_ENDURANCE_STATE_ABSENT,
         0u},
        {&FLOWIE_ENDURANCE_PROFILES[1], FLOWIE_TEST_INVALID_SOCKET, FLOWIE_ENDURANCE_STATE_ABSENT,
         0u},
        {&FLOWIE_ENDURANCE_PROFILES[2], FLOWIE_TEST_INVALID_SOCKET, FLOWIE_ENDURANCE_STATE_ABSENT,
         0u},
        {&FLOWIE_ENDURANCE_PROFILES[3], FLOWIE_TEST_INVALID_SOCKET, FLOWIE_ENDURANCE_STATE_ABSENT,
         0u},
    };
    flowie_endurance_client_t resumed = {&FLOWIE_ENDURANCE_PROFILES[3], FLOWIE_TEST_INVALID_SOCKET,
                                         FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT, 0u};
    flowie_endurance_history_t history = {0};
    flowie_endurance_message_key_t offline[FLOWIE_ENDURANCE_OFFLINE_MESSAGES];
    flowie_endurance_final_snapshot_t final = {0};
    uint64_t root_seed = 0u;
    size_t message_count = 0u;
    uint64_t operation_id = 1u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_endurance_flow(port);
    uint64_t started_at = turbo_hrtime();

    check_int_eq(flowie_endurance_seed(&root_seed), TURBO_OK);
    check_int_eq(flowie_endurance_message_count(&message_count), TURBO_OK);
    info("seed=%llu configured_messages=%zu", (unsigned long long)root_seed, message_count);
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);

    for (size_t i = 0u; i < FLOWIE_ENDURANCE_PROFILE_COUNT; ++i)
      check_int_eq(flowie_endurance_connect_at(&clients[i], port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe(&clients[2]), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe(&clients[3]), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 4u), TURBO_OK);

    for (size_t i = 0u; i < message_count; ++i) {
      flowie_endurance_client_t *publisher = &clients[i % 2u];
      flowie_endurance_message_key_t key =
          flowie_endurance_next_key(publisher, root_seed, operation_id++);
      flowie_endurance_operation_t operation = {
          key.operation_id,
          key.producer_seed,
          key.actor_id,
          key.producer_sequence,
          FLOWIE_ENDURANCE_SEGMENT_STEADY,
          publisher->state,
          publisher->state,
          FLOWIE_ENDURANCE_PAYLOAD_SIZE,
      };
      info("segment=steady operation=%llu actor=%u sequence=%u",
           (unsigned long long)key.operation_id, key.actor_id, key.producer_sequence);
      check_int_eq(flowie_endurance_publish(publisher, &key), TURBO_OK);
      check_int_eq(flowie_endurance_recv_publish(&clients[2], &key), TURBO_OK);
      check_int_eq(flowie_endurance_recv_publish(&clients[3], &key), TURBO_OK);
      flowie_endurance_history_append(&history, &operation);
    }

    flowie_test_socket_close(clients[3].socket);
    clients[3].socket = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(
        flowie_endurance_transition(&clients[3], FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT),
        TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 3u), TURBO_OK);

    for (size_t i = 0u; i < FLOWIE_ENDURANCE_OFFLINE_MESSAGES; ++i) {
      flowie_endurance_client_t *publisher = &clients[i % 2u];
      flowie_endurance_operation_t operation;
      offline[i] = flowie_endurance_next_key(publisher, root_seed, operation_id++);
      operation = (flowie_endurance_operation_t){
          offline[i].operation_id,
          offline[i].producer_seed,
          offline[i].actor_id,
          offline[i].producer_sequence,
          FLOWIE_ENDURANCE_SEGMENT_SESSION_CHURN,
          publisher->state,
          publisher->state,
          FLOWIE_ENDURANCE_PAYLOAD_SIZE,
      };
      info("segment=session-churn operation=%llu actor=%u sequence=%u",
           (unsigned long long)offline[i].operation_id, offline[i].actor_id,
           offline[i].producer_sequence);
      check_int_eq(flowie_endurance_publish(publisher, &offline[i]), TURBO_OK);
      check_int_eq(flowie_endurance_recv_publish(&clients[2], &offline[i]), TURBO_OK);
      flowie_endurance_history_append(&history, &operation);
    }

    check_int_eq(flowie_endurance_connect_at(&resumed, port, 1u), TURBO_OK);
    for (size_t i = 0u; i < FLOWIE_ENDURANCE_OFFLINE_MESSAGES; ++i) {
      info("segment=recovery operation=%llu actor=%u sequence=%u",
           (unsigned long long)offline[i].operation_id, offline[i].actor_id,
           offline[i].producer_sequence);
      check_int_eq(flowie_endurance_recv_publish(&resumed, &offline[i]), TURBO_OK);
    }
    check_int_eq(flowie_endurance_wait_connections(flow, 4u), TURBO_OK);

    check_int_eq(flowie_endurance_disconnect(&clients[0]), TURBO_OK);
    check_int_eq(flowie_endurance_disconnect(&clients[1]), TURBO_OK);
    check_int_eq(flowie_endurance_disconnect(&clients[2]), TURBO_OK);
    check_int_eq(flowie_endurance_disconnect(&resumed), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 0u), TURBO_OK);

    check_int_eq(flowie_endurance_stop_drained(flow, 1u, &final), TURBO_OK);
    check_size_eq(history.count, message_count + FLOWIE_ENDURANCE_OFFLINE_MESSAGES >
                                         FLOWIE_ENDURANCE_HISTORY_CAPACITY
                                     ? FLOWIE_ENDURANCE_HISTORY_CAPACITY
                                     : message_count + FLOWIE_ENDURANCE_OFFLINE_MESSAGES);
    printf("ENDURANCE_RESULT id=MQTT-ENDURANCE-001 seed=%" PRIu64
           " clients=%zu publications=%zu deliveries=%zu duration_ns=%" PRIu64
           " history_capacity=%u history_count=%zu connections_final=%" PRIu64
           " inflight_final=%" PRIu64 " queue_final=%" PRIu64 " sessions_final=%" PRIu64
           " stop_ms=%" PRIu64 "\n",
           root_seed, (size_t)FLOWIE_ENDURANCE_PROFILE_COUNT,
           message_count + FLOWIE_ENDURANCE_OFFLINE_MESSAGES,
           message_count * 2u + FLOWIE_ENDURANCE_OFFLINE_MESSAGES * 2u, turbo_hrtime() - started_at,
           FLOWIE_ENDURANCE_HISTORY_CAPACITY, history.count, final.connection.connections_current,
           final.connection.in_flight_messages, final.queue.load, final.sessions.load,
           final.stop_elapsed_ms);
    turbo_flow_destroy(flow);
  }

  it("ENDURANCE-002 preserves one persistent session through repeated client-ID takeover") {
    flowie_endurance_client_t publisher = {&FLOWIE_ENDURANCE_PROFILES[1],
                                           FLOWIE_TEST_INVALID_SOCKET,
                                           FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t subscribers[] = {
        {&FLOWIE_ENDURANCE_PROFILES[3], FLOWIE_TEST_INVALID_SOCKET, FLOWIE_ENDURANCE_STATE_ABSENT,
         0u},
        {&FLOWIE_ENDURANCE_PROFILES[3], FLOWIE_TEST_INVALID_SOCKET, FLOWIE_ENDURANCE_STATE_ABSENT,
         0u},
    };
    flowie_endurance_history_t history = {0};
    flowie_endurance_final_snapshot_t final = {0};
    flowie_endurance_client_t *current = &subscribers[0];
    uint64_t root_seed = 0u;
    uint64_t operation_id = 1u;
    size_t takeover_count = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_endurance_flow(port);
    uint64_t started_at = turbo_hrtime();

    check_int_eq(flowie_endurance_seed(&root_seed), TURBO_OK);
    check_int_eq(flowie_endurance_takeover_count(&takeover_count), TURBO_OK);
    info("seed=%llu configured_takeovers=%zu", (unsigned long long)root_seed, takeover_count);
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&publisher, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(current, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe(current), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 2u), TURBO_OK);

    for (size_t i = 0u; i < takeover_count; ++i) {
      flowie_endurance_client_t *replacement = &subscribers[(i + 1u) % 2u];
      flowie_endurance_message_key_t before =
          flowie_endurance_next_key(&publisher, root_seed, operation_id++);
      flowie_endurance_message_key_t after;
      flowie_endurance_operation_t operation = {
          before.operation_id,
          before.producer_seed,
          before.actor_id,
          before.producer_sequence,
          FLOWIE_ENDURANCE_SEGMENT_SESSION_CHURN,
          current->state,
          current->state,
          FLOWIE_ENDURANCE_PAYLOAD_SIZE,
      };

      check_int_eq(flowie_endurance_publish(&publisher, &before), TURBO_OK);
      check_int_eq(flowie_endurance_recv_publish(current, &before), TURBO_OK);
      flowie_endurance_history_append(&history, &operation);

      check_int_eq(flowie_endurance_connect_at(replacement, port, 1u), TURBO_OK);
      check_int_eq(flowie_endurance_expect_takeover(current), TURBO_OK);
      current = replacement;

      after = flowie_endurance_next_key(&publisher, root_seed, operation_id++);
      operation = (flowie_endurance_operation_t){
          after.operation_id,
          after.producer_seed,
          after.actor_id,
          after.producer_sequence,
          FLOWIE_ENDURANCE_SEGMENT_RECOVERY,
          current->state,
          current->state,
          FLOWIE_ENDURANCE_PAYLOAD_SIZE,
      };
      check_int_eq(flowie_endurance_publish(&publisher, &after), TURBO_OK);
      check_int_eq(flowie_endurance_recv_publish(current, &after), TURBO_OK);
      flowie_endurance_history_append(&history, &operation);
    }

    check_int_eq(flowie_endurance_wait_connections(flow, 2u), TURBO_OK);
    check_int_eq(flowie_endurance_disconnect(&publisher), TURBO_OK);
    check_int_eq(flowie_endurance_disconnect(current), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_stop_drained(flow, 1u, &final), TURBO_OK);
    check_size_eq(history.count, takeover_count * 2u > FLOWIE_ENDURANCE_HISTORY_CAPACITY
                                     ? FLOWIE_ENDURANCE_HISTORY_CAPACITY
                                     : takeover_count * 2u);
    printf("ENDURANCE_RESULT id=MQTT-ENDURANCE-002 seed=%" PRIu64
           " clients=3 takeovers=%zu publications=%zu deliveries=%zu duration_ns=%" PRIu64
           " history_capacity=%u history_count=%zu connections_final=%" PRIu64
           " inflight_final=%" PRIu64 " queue_final=%" PRIu64 " sessions_final=%" PRIu64
           " stop_ms=%" PRIu64 "\n",
           root_seed, takeover_count, takeover_count * 2u, takeover_count * 2u,
           turbo_hrtime() - started_at, FLOWIE_ENDURANCE_HISTORY_CAPACITY, history.count,
           final.connection.connections_current, final.connection.in_flight_messages,
           final.queue.load, final.sessions.load, final.stop_elapsed_ms);
    turbo_flow_destroy(flow);
  }

  it("ENDURANCE-003 isolates an inflight-stalled subscriber while a healthy peer advances") {
    static const flowie_endurance_client_profile_t slow_profile = {
        5u,
        "MQTT 5 inflight-stalled persistent subscriber",
        "end-sub-v5-slow",
        FLOWIE_MQTT_VERSION_5,
        FLOWIE_ENDURANCE_ROLE_SUBSCRIBER,
        0u,
        FLOWIE_ENDURANCE_SESSION_EXPIRY_SECONDS,
        1u,
        FLOWIE_ENDURANCE_READ_PAUSED,
    };
    flowie_endurance_client_t publisher = {&FLOWIE_ENDURANCE_PROFILES[1],
                                           FLOWIE_TEST_INVALID_SOCKET,
                                           FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t slow = {&slow_profile, FLOWIE_TEST_INVALID_SOCKET,
                                      FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t healthy = {&FLOWIE_ENDURANCE_PROFILES[2], FLOWIE_TEST_INVALID_SOCKET,
                                         FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_final_snapshot_t final = {0};
    uint64_t root_seed = 0u;
    uint64_t operation_id = 1u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow =
        flowie_endurance_flow_with_limits(port, FLOWIE_ENDURANCE_SEND_HWM_BYTES, 1u);
    uint64_t started_at = turbo_hrtime();
    flowie_endurance_message_key_t unsettled;
    flowie_endurance_message_key_t isolation_trigger;

    check_int_eq(flowie_endurance_seed(&root_seed), TURBO_OK);
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&publisher, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&slow, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&healthy, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe(&slow), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe(&healthy), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 3u), TURBO_OK);

    unsettled = flowie_endurance_next_key(&publisher, root_seed, operation_id++);
    check_int_eq(flowie_endurance_publish(&publisher, &unsettled), TURBO_OK);
    check_int_eq(flowie_endurance_recv_publish_unsettled(&slow, &unsettled), TURBO_OK);
    check_int_eq(flowie_endurance_recv_publish(&healthy, &unsettled), TURBO_OK);

    isolation_trigger = flowie_endurance_next_key(&publisher, root_seed, operation_id++);
    check_int_eq(flowie_endurance_publish(&publisher, &isolation_trigger), TURBO_OK);
    check_int_eq(flowie_endurance_recv_publish(&healthy, &isolation_trigger), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 2u), TURBO_OK);
    flowie_test_socket_close(slow.socket);
    slow.socket = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(flowie_endurance_transition(&slow, FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT),
                 TURBO_OK);

    for (size_t i = 0u; i < FLOWIE_ENDURANCE_HEALTHY_AFTER_ISOLATION; ++i) {
      flowie_endurance_message_key_t key =
          flowie_endurance_next_key(&publisher, root_seed, operation_id++);
      check_int_eq(flowie_endurance_publish(&publisher, &key), TURBO_OK);
      check_int_eq(flowie_endurance_recv_publish(&healthy, &key), TURBO_OK);
    }

    check_int_eq(flowie_endurance_disconnect(&publisher), TURBO_OK);
    check_int_eq(flowie_endurance_disconnect(&healthy), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_stop_drained(flow, 1u, &final), TURBO_OK);
    printf("ENDURANCE_RESULT id=MQTT-ENDURANCE-003 seed=%" PRIu64
           " clients=3 isolated=1 publications=%u healthy_deliveries=%u duration_ns=%" PRIu64
           " connections_final=%" PRIu64 " inflight_final=%" PRIu64 " queue_final=%" PRIu64
           " sessions_final=%" PRIu64 " stop_ms=%" PRIu64 "\n",
           root_seed, FLOWIE_ENDURANCE_HEALTHY_AFTER_ISOLATION + 2u,
           FLOWIE_ENDURANCE_HEALTHY_AFTER_ISOLATION + 2u, turbo_hrtime() - started_at,
           final.connection.connections_current, final.connection.in_flight_messages,
           final.queue.load, final.sessions.load, final.stop_elapsed_ms);
    turbo_flow_destroy(flow);
  }

  it("ENDURANCE-004 resumes both broker-owned QoS 2 acknowledgement stages") {
    static const flowie_endurance_client_profile_t subscriber_profile = {
        6u,
        "MQTT 5 persistent QoS 2 subscriber",
        "end-sub-v5-qos2",
        FLOWIE_MQTT_VERSION_5,
        FLOWIE_ENDURANCE_ROLE_SUBSCRIBER,
        0u,
        FLOWIE_ENDURANCE_SESSION_EXPIRY_SECONDS,
        8u,
        FLOWIE_ENDURANCE_READ_NORMAL,
    };
    flowie_endurance_client_t publisher = {&FLOWIE_ENDURANCE_PROFILES[1],
                                           FLOWIE_TEST_INVALID_SOCKET,
                                           FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t subscriber = {&subscriber_profile, FLOWIE_TEST_INVALID_SOCKET,
                                            FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_final_snapshot_t final = {0};
    flowie_endurance_control_t control = {0};
    flowie_endurance_message_key_t key;
    uint16_t delivery_packet_id = 0u;
    uint16_t replay_packet_id = 0u;
    uint64_t root_seed = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_endurance_flow(port);
    uint64_t started_at = turbo_hrtime();

    check_int_eq(flowie_endurance_seed(&root_seed), TURBO_OK);
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&publisher, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&subscriber, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe_qos(&subscriber, 2u), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 2u), TURBO_OK);

    key = flowie_endurance_next_key(&publisher, root_seed, 1u);
    check_int_eq(flowie_endurance_publish_qos(&publisher, &key, 2u), TURBO_OK);
    check_int_eq(
        flowie_endurance_recv_publish_stage(&subscriber, &key, 2u, 0u, &delivery_packet_id),
        TURBO_OK);
    check_uint_ne(delivery_packet_id, 0u);

    flowie_test_socket_close(subscriber.socket);
    subscriber.socket = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(
        flowie_endurance_transition(&subscriber, FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT),
        TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 1u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&subscriber, port, 1u), TURBO_OK);
    check_int_eq(flowie_endurance_recv_publish_stage(&subscriber, &key, 2u, 1u, &replay_packet_id),
                 TURBO_OK);
    check_uint_eq(replay_packet_id, delivery_packet_id);
    check_int_eq(flowie_endurance_send_control(subscriber.socket, subscriber.profile->version,
                                               FLOWIE_MQTT_PACKET_PUBREC, replay_packet_id),
                 TURBO_OK);
    check_int_eq(
        flowie_endurance_recv_control(subscriber.socket, subscriber.profile->version, &control),
        TURBO_OK);
    check_int_eq(control.type, FLOWIE_MQTT_PACKET_PUBREL);
    check_uint_eq(control.packet_id, delivery_packet_id);

    flowie_test_socket_close(subscriber.socket);
    subscriber.socket = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(
        flowie_endurance_transition(&subscriber, FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT),
        TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 1u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&subscriber, port, 1u), TURBO_OK);
    check_int_eq(
        flowie_endurance_recv_control(subscriber.socket, subscriber.profile->version, &control),
        TURBO_OK);
    check_int_eq(control.type, FLOWIE_MQTT_PACKET_PUBREL);
    check_uint_eq(control.packet_id, delivery_packet_id);
    check_int_eq(flowie_endurance_send_control(subscriber.socket, subscriber.profile->version,
                                               FLOWIE_MQTT_PACKET_PUBREC, delivery_packet_id),
                 TURBO_OK);
    check_int_eq(
        flowie_endurance_recv_control(subscriber.socket, subscriber.profile->version, &control),
        TURBO_OK);
    check_int_eq(control.type, FLOWIE_MQTT_PACKET_PUBREL);
    check_uint_eq(control.packet_id, delivery_packet_id);
    check_int_eq(flowie_endurance_send_control(subscriber.socket, subscriber.profile->version,
                                               FLOWIE_MQTT_PACKET_PUBCOMP, delivery_packet_id),
                 TURBO_OK);
    check_false(flowie_test_socket_readable(subscriber.socket, 50u));

    check_int_eq(flowie_endurance_disconnect(&publisher), TURBO_OK);
    check_int_eq(flowie_endurance_disconnect(&subscriber), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_stop_drained(flow, 1u, &final), TURBO_OK);
    printf("ENDURANCE_RESULT id=MQTT-ENDURANCE-004 seed=%" PRIu64
           " clients=2 qos=2 reconnects=2 publications=1 deliveries=2 duration_ns=%" PRIu64
           " connections_final=%" PRIu64 " inflight_final=%" PRIu64 " queue_final=%" PRIu64
           " sessions_final=%" PRIu64 " stop_ms=%" PRIu64 "\n",
           root_seed, turbo_hrtime() - started_at, final.connection.connections_current,
           final.connection.in_flight_messages, final.queue.load, final.sessions.load,
           final.stop_elapsed_ms);
    turbo_flow_destroy(flow);
  }

  it("ENDURANCE-005 resolves delayed Will reconnect and session-expiry ordering") {
    static const flowie_endurance_client_profile_t delayed_profile = {
        7u,
        "MQTT 5 delayed Will publisher",
        "end-pub-v5-will-delay",
        FLOWIE_MQTT_VERSION_5,
        FLOWIE_ENDURANCE_ROLE_PUBLISHER,
        0u,
        FLOWIE_ENDURANCE_SESSION_EXPIRY_SECONDS,
        0u,
        FLOWIE_ENDURANCE_READ_NORMAL,
    };
    static const flowie_endurance_client_profile_t expiry_profile = {
        8u,
        "MQTT 5 expiry-forced Will publisher",
        "end-pub-v5-will-expiry",
        FLOWIE_MQTT_VERSION_5,
        FLOWIE_ENDURANCE_ROLE_PUBLISHER,
        1u,
        0u,
        0u,
        FLOWIE_ENDURANCE_READ_NORMAL,
    };
    flowie_endurance_client_t subscriber = {&FLOWIE_ENDURANCE_PROFILES[2],
                                            FLOWIE_TEST_INVALID_SOCKET,
                                            FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t delayed = {&delayed_profile, FLOWIE_TEST_INVALID_SOCKET,
                                         FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t expiry = {&expiry_profile, FLOWIE_TEST_INVALID_SOCKET,
                                        FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_final_snapshot_t final = {0};
    flowie_endurance_message_key_t canceled_key;
    flowie_endurance_message_key_t expiry_key;
    uint8_t canceled_payload[FLOWIE_ENDURANCE_PAYLOAD_SIZE];
    uint8_t expiry_payload[FLOWIE_ENDURANCE_PAYLOAD_SIZE];
    flowie_endurance_will_t canceled_will;
    flowie_endurance_will_t expiry_will;
    uint64_t root_seed = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_endurance_flow(port);
    uint64_t started_at = turbo_hrtime();

    check_int_eq(flowie_endurance_seed(&root_seed), TURBO_OK);
    canceled_key = flowie_endurance_next_key(&delayed, root_seed, 1u);
    expiry_key = flowie_endurance_next_key(&expiry, root_seed, 2u);
    flowie_endurance_payload_build(&canceled_key, canceled_payload);
    flowie_endurance_payload_build(&expiry_key, expiry_payload);
    canceled_will = (flowie_endurance_will_t){
        (const uint8_t *)FLOWIE_ENDURANCE_TOPIC,
        sizeof(FLOWIE_ENDURANCE_TOPIC) - 1u,
        canceled_payload,
        sizeof(canceled_payload),
        FLOWIE_ENDURANCE_WILL_DELAY_SECONDS,
        1u,
    };
    expiry_will = (flowie_endurance_will_t){
        (const uint8_t *)FLOWIE_ENDURANCE_TOPIC,
        sizeof(FLOWIE_ENDURANCE_TOPIC) - 1u,
        expiry_payload,
        sizeof(expiry_payload),
        FLOWIE_ENDURANCE_WILL_EXPIRY_DELAY_SECONDS,
        1u,
    };

    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&subscriber, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe(&subscriber), TURBO_OK);

    check_int_eq(flowie_endurance_connect_with_will(&delayed, port, 0u, &canceled_will), TURBO_OK);
    flowie_test_socket_close(delayed.socket);
    delayed.socket = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(flowie_endurance_transition(&delayed, FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT),
                 TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 1u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&delayed, port, 1u), TURBO_OK);
    turbo_sleep_ms(FLOWIE_ENDURANCE_WILL_OBSERVATION_MS);
    check_false(flowie_test_socket_readable(subscriber.socket, 50u));
    check_int_eq(flowie_endurance_disconnect(&delayed), TURBO_OK);

    check_int_eq(flowie_endurance_connect_with_will(&expiry, port, 0u, &expiry_will), TURBO_OK);
    flowie_test_socket_close(expiry.socket);
    expiry.socket = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(flowie_endurance_transition(&expiry, FLOWIE_ENDURANCE_STATE_REMOVED), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 1u), TURBO_OK);
    check_int_eq(flowie_endurance_recv_publish(&subscriber, &expiry_key), TURBO_OK);
    check_false(flowie_test_socket_readable(subscriber.socket, 50u));

    check_int_eq(flowie_endurance_disconnect(&subscriber), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_stop_drained(flow, 1u, &final), TURBO_OK);
    printf("ENDURANCE_RESULT id=MQTT-ENDURANCE-005 seed=%" PRIu64
           " clients=3 canceled_wills=1 expiry_forced_wills=1 duration_ns=%" PRIu64
           " connections_final=%" PRIu64 " inflight_final=%" PRIu64 " queue_final=%" PRIu64
           " sessions_final=%" PRIu64 " stop_ms=%" PRIu64 "\n",
           root_seed, turbo_hrtime() - started_at, final.connection.connections_current,
           final.connection.in_flight_messages, final.queue.load, final.sessions.load,
           final.stop_elapsed_ms);
    turbo_flow_destroy(flow);
  }

  it("ENDURANCE-006 preserves routing truth through subscription churn") {
    static const char topic_a[] = "endurance/routes/a/temp";
    static const char topic_b[] = "endurance/routes/b/temp";
    static const char exact_filter[] = "endurance/routes/a/temp";
    static const char plus_filter[] = "endurance/routes/+/temp";
    static const char hash_filter[] = "endurance/routes/#";
    static const char shared_filter[] = "$share/endurance/endurance/routes/+/temp";
    static const flowie_endurance_client_profile_t exact_profile = {
        9u,
        "MQTT 5 exact subscriber",
        "end-route-exact",
        FLOWIE_MQTT_VERSION_5,
        FLOWIE_ENDURANCE_ROLE_SUBSCRIBER,
        1u,
        0u,
        8u,
        FLOWIE_ENDURANCE_READ_NORMAL};
    static const flowie_endurance_client_profile_t plus_profile = {10u,
                                                                   "MQTT 5 plus subscriber",
                                                                   "end-route-plus",
                                                                   FLOWIE_MQTT_VERSION_5,
                                                                   FLOWIE_ENDURANCE_ROLE_SUBSCRIBER,
                                                                   1u,
                                                                   0u,
                                                                   8u,
                                                                   FLOWIE_ENDURANCE_READ_NORMAL};
    static const flowie_endurance_client_profile_t hash_profile = {11u,
                                                                   "MQTT 5 hash subscriber",
                                                                   "end-route-hash",
                                                                   FLOWIE_MQTT_VERSION_5,
                                                                   FLOWIE_ENDURANCE_ROLE_SUBSCRIBER,
                                                                   1u,
                                                                   0u,
                                                                   8u,
                                                                   FLOWIE_ENDURANCE_READ_NORMAL};
    static const flowie_endurance_client_profile_t shared_a_profile = {
        12u,
        "MQTT 5 shared subscriber A",
        "end-route-share-a",
        FLOWIE_MQTT_VERSION_5,
        FLOWIE_ENDURANCE_ROLE_SUBSCRIBER,
        1u,
        0u,
        8u,
        FLOWIE_ENDURANCE_READ_NORMAL};
    static const flowie_endurance_client_profile_t shared_b_profile = {
        13u,
        "MQTT 5 shared subscriber B",
        "end-route-share-b",
        FLOWIE_MQTT_VERSION_5,
        FLOWIE_ENDURANCE_ROLE_SUBSCRIBER,
        1u,
        0u,
        8u,
        FLOWIE_ENDURANCE_READ_NORMAL};
    flowie_endurance_client_t publisher = {&FLOWIE_ENDURANCE_PROFILES[1],
                                           FLOWIE_TEST_INVALID_SOCKET,
                                           FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t exact = {&exact_profile, FLOWIE_TEST_INVALID_SOCKET,
                                       FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t plus = {&plus_profile, FLOWIE_TEST_INVALID_SOCKET,
                                      FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t hash = {&hash_profile, FLOWIE_TEST_INVALID_SOCKET,
                                      FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t shared_a = {&shared_a_profile, FLOWIE_TEST_INVALID_SOCKET,
                                          FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t shared_b = {&shared_b_profile, FLOWIE_TEST_INVALID_SOCKET,
                                          FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_final_snapshot_t final = {0};
    uint64_t root_seed = 0u;
    size_t exact_deliveries = 0u;
    size_t plus_deliveries = 0u;
    size_t hash_deliveries = 0u;
    size_t shared_deliveries[2] = {0u, 0u};
    int exact_active = 1;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_endurance_flow(port);
    uint64_t started_at = turbo_hrtime();

    check_int_eq(flowie_endurance_seed(&root_seed), TURBO_OK);
    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&publisher, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&exact, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&plus, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&hash, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&shared_a, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&shared_b, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe_filter_qos(&exact, exact_filter, 1u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe_filter_qos(&plus, plus_filter, 1u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe_filter_qos(&hash, hash_filter, 1u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe_filter_qos(&shared_a, shared_filter, 1u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe_filter_qos(&shared_b, shared_filter, 1u), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 6u), TURBO_OK);

    for (size_t round = 0u; round < FLOWIE_ENDURANCE_ROUTING_ROUNDS; ++round) {
      const char *topic = (round & 1u) == 0u ? topic_a : topic_b;
      flowie_endurance_message_key_t key;
      size_t shared_winner = 0u;
      int exact_expected;
      if (round == 4u || round == 12u) {
        check_int_eq(flowie_endurance_unsubscribe_filter(&exact, exact_filter), TURBO_OK);
        exact_active = 0;
      } else if (round == 8u) {
        check_int_eq(flowie_endurance_subscribe_filter_qos(&exact, exact_filter, 1u), TURBO_OK);
        exact_active = 1;
      }
      key = flowie_endurance_next_key(&publisher, root_seed, round + 1u);
      exact_expected = exact_active && (round & 1u) == 0u;
      check_int_eq(flowie_endurance_publish_qos_on_topic(&publisher, &key, topic, 1u), TURBO_OK);
      if (exact_expected) {
        check_int_eq(flowie_endurance_recv_publish_on_topic(&exact, &key, topic), TURBO_OK);
        ++exact_deliveries;
      }
      check_int_eq(flowie_endurance_recv_publish_on_topic(&plus, &key, topic), TURBO_OK);
      ++plus_deliveries;
      check_int_eq(flowie_endurance_recv_publish_on_topic(&hash, &key, topic), TURBO_OK);
      ++hash_deliveries;
      check_int_eq(
          flowie_endurance_recv_shared_publish(&shared_a, &shared_b, &key, topic, &shared_winner),
          TURBO_OK);
      ++shared_deliveries[shared_winner];
      if (!exact_expected) check_false(flowie_test_socket_readable(exact.socket, 20u));
    }

    check_size_eq(exact_deliveries, 4u);
    check_size_eq(plus_deliveries, FLOWIE_ENDURANCE_ROUTING_ROUNDS);
    check_size_eq(hash_deliveries, FLOWIE_ENDURANCE_ROUTING_ROUNDS);
    check_size_eq(shared_deliveries[0] + shared_deliveries[1], FLOWIE_ENDURANCE_ROUTING_ROUNDS);
    check_int_eq(flowie_endurance_disconnect(&shared_b), TURBO_OK);
    check_int_eq(flowie_endurance_disconnect(&shared_a), TURBO_OK);
    check_int_eq(flowie_endurance_disconnect(&hash), TURBO_OK);
    check_int_eq(flowie_endurance_disconnect(&plus), TURBO_OK);
    check_int_eq(flowie_endurance_disconnect(&exact), TURBO_OK);
    check_int_eq(flowie_endurance_disconnect(&publisher), TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_stop_drained(flow, 0u, &final), TURBO_OK);
    printf("ENDURANCE_RESULT id=MQTT-ENDURANCE-006 seed=%" PRIu64
           " rounds=%u exact=%zu plus=%zu hash=%zu shared_a=%zu shared_b=%zu"
           " duration_ns=%" PRIu64 " connections_final=%" PRIu64 " inflight_final=%" PRIu64
           " queue_final=%" PRIu64 " sessions_final=%" PRIu64 " stop_ms=%" PRIu64 "\n",
           root_seed, FLOWIE_ENDURANCE_ROUTING_ROUNDS, exact_deliveries, plus_deliveries,
           hash_deliveries, shared_deliveries[0], shared_deliveries[1], turbo_hrtime() - started_at,
           final.connection.connections_current, final.connection.in_flight_messages,
           final.queue.load, final.sessions.load, final.stop_elapsed_ms);
    turbo_flow_destroy(flow);
  }

  it("ENDURANCE-007 converges compound admitted state during shutdown") {
    static const char qos2_topic[] = "endurance/shutdown/qos2";
    static const char offline_topic[] = "endurance/shutdown/offline";
    static const char slow_topic[] = "endurance/shutdown/slow";
    static const char will_topic[] = "endurance/shutdown/will";
    static const flowie_endurance_client_profile_t qos2_profile = {
        20u,
        "MQTT 5 shutdown QoS 2 subscriber",
        "end-stop-qos2",
        FLOWIE_MQTT_VERSION_5,
        FLOWIE_ENDURANCE_ROLE_SUBSCRIBER,
        0u,
        FLOWIE_ENDURANCE_SESSION_EXPIRY_SECONDS,
        8u,
        FLOWIE_ENDURANCE_READ_NORMAL};
    static const flowie_endurance_client_profile_t offline_profile = {
        21u,
        "MQTT 5 shutdown offline subscriber",
        "end-stop-offline",
        FLOWIE_MQTT_VERSION_5,
        FLOWIE_ENDURANCE_ROLE_SUBSCRIBER,
        0u,
        FLOWIE_ENDURANCE_SESSION_EXPIRY_SECONDS,
        8u,
        FLOWIE_ENDURANCE_READ_NORMAL};
    static const flowie_endurance_client_profile_t slow_profile = {
        22u,
        "MQTT 5 shutdown inflight subscriber",
        "end-stop-slow",
        FLOWIE_MQTT_VERSION_5,
        FLOWIE_ENDURANCE_ROLE_SUBSCRIBER,
        0u,
        FLOWIE_ENDURANCE_SESSION_EXPIRY_SECONDS,
        1u,
        FLOWIE_ENDURANCE_READ_PAUSED};
    static const flowie_endurance_client_profile_t will_profile = {
        23u,
        "MQTT 5 shutdown delayed Will publisher",
        "end-stop-will",
        FLOWIE_MQTT_VERSION_5,
        FLOWIE_ENDURANCE_ROLE_PUBLISHER,
        0u,
        FLOWIE_ENDURANCE_SESSION_EXPIRY_SECONDS,
        8u,
        FLOWIE_ENDURANCE_READ_NORMAL};
    flowie_endurance_client_t publisher = {&FLOWIE_ENDURANCE_PROFILES[1],
                                           FLOWIE_TEST_INVALID_SOCKET,
                                           FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t qos2 = {&qos2_profile, FLOWIE_TEST_INVALID_SOCKET,
                                      FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t offline = {&offline_profile, FLOWIE_TEST_INVALID_SOCKET,
                                         FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t slow = {&slow_profile, FLOWIE_TEST_INVALID_SOCKET,
                                      FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_client_t will = {&will_profile, FLOWIE_TEST_INVALID_SOCKET,
                                      FLOWIE_ENDURANCE_STATE_ABSENT, 0u};
    flowie_endurance_message_key_t qos2_key;
    flowie_endurance_message_key_t offline_key;
    flowie_endurance_message_key_t slow_key;
    flowie_endurance_message_key_t will_key;
    uint8_t will_payload[FLOWIE_ENDURANCE_PAYLOAD_SIZE];
    flowie_endurance_will_t pending_will;
    flowie_endurance_final_snapshot_t final = {0};
    uint16_t qos2_packet_id = 0u;
    uint16_t slow_packet_id = 0u;
    uint64_t root_seed = 0u;
    unsigned short port = flowie_test_port();
    turbo_flow_t *flow = flowie_endurance_flow(port);
    uint64_t started_at = turbo_hrtime();

    check_int_eq(flowie_endurance_seed(&root_seed), TURBO_OK);
    qos2_key = flowie_endurance_next_key(&publisher, root_seed, 1u);
    offline_key = flowie_endurance_next_key(&publisher, root_seed, 2u);
    slow_key = flowie_endurance_next_key(&publisher, root_seed, 3u);
    will_key = flowie_endurance_next_key(&will, root_seed, 4u);
    flowie_endurance_payload_build(&will_key, will_payload);
    pending_will = (flowie_endurance_will_t){(const uint8_t *)will_topic,
                                             sizeof(will_topic) - 1u,
                                             will_payload,
                                             sizeof(will_payload),
                                             FLOWIE_ENDURANCE_WILL_DELAY_SECONDS,
                                             1u};

    check_int_gt(port, 0);
    check_not_null(flow);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&publisher, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&qos2, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe_filter_qos(&qos2, qos2_topic, 2u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&offline, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe_filter_qos(&offline, offline_topic, 1u), TURBO_OK);
    check_int_eq(flowie_endurance_connect_at(&slow, port, 0u), TURBO_OK);
    check_int_eq(flowie_endurance_subscribe_filter_qos(&slow, slow_topic, 1u), TURBO_OK);

    flowie_test_socket_close(offline.socket);
    offline.socket = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(flowie_endurance_transition(&offline, FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT),
                 TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 3u), TURBO_OK);

    check_int_eq(flowie_endurance_publish_qos_on_topic(&publisher, &qos2_key, qos2_topic, 2u),
                 TURBO_OK);
    check_int_eq(flowie_endurance_recv_publish_stage_on_topic(&qos2, &qos2_key, qos2_topic, 2u, 0u,
                                                              &qos2_packet_id),
                 TURBO_OK);
    check_uint_ne(qos2_packet_id, 0u);
    check_int_eq(flowie_endurance_publish_qos_on_topic(&publisher, &offline_key, offline_topic, 1u),
                 TURBO_OK);
    check_int_eq(flowie_endurance_publish_qos_on_topic(&publisher, &slow_key, slow_topic, 1u),
                 TURBO_OK);
    check_int_eq(flowie_endurance_recv_publish_stage_on_topic(&slow, &slow_key, slow_topic, 1u, 0u,
                                                              &slow_packet_id),
                 TURBO_OK);
    check_uint_ne(slow_packet_id, 0u);

    check_int_eq(flowie_endurance_connect_with_will(&will, port, 0u, &pending_will), TURBO_OK);
    flowie_test_socket_close(will.socket);
    will.socket = FLOWIE_TEST_INVALID_SOCKET;
    check_int_eq(flowie_endurance_transition(&will, FLOWIE_ENDURANCE_STATE_OFFLINE_PERSISTENT),
                 TURBO_OK);
    check_int_eq(flowie_endurance_wait_connections(flow, 3u), TURBO_OK);

    check_int_eq(flowie_endurance_stop_drained(flow, 4u, &final), TURBO_OK);
    flowie_test_socket_close(slow.socket);
    slow.socket = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_close(qos2.socket);
    qos2.socket = FLOWIE_TEST_INVALID_SOCKET;
    flowie_test_socket_close(publisher.socket);
    publisher.socket = FLOWIE_TEST_INVALID_SOCKET;
    printf("ENDURANCE_RESULT id=MQTT-ENDURANCE-007 seed=%" PRIu64
           " pending_qos2=1 pending_offline=1 pending_inflight=1 pending_will=1"
           " duration_ns=%" PRIu64 " connections_final=%" PRIu64 " inflight_final=%" PRIu64
           " queue_final=%" PRIu64 " sessions_final=%" PRIu64 " stop_ms=%" PRIu64 "\n",
           root_seed, turbo_hrtime() - started_at, final.connection.connections_current,
           final.connection.in_flight_messages, final.queue.load, final.sessions.load,
           final.stop_elapsed_ms);
    turbo_flow_destroy(flow);
  }
}
