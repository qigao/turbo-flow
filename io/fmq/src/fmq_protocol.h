#ifndef TURBO_FLOW_FMQ_PROTOCOL_H
#define TURBO_FLOW_FMQ_PROTOCOL_H

#include "turbo_flow_fmq.h"

#include <stddef.h>
#include <stdint.h>

#define FLOW_FMQ_PROTOCOL_VERSION 2u
#define FLOW_FMQ_HEADER_SIZE 32u
#define FLOW_FMQ_PACKET_PAYLOAD_SIZE (64u * 1024u)
#define FLOW_FMQ_PACKET_FIRST 0x01u
#define FLOW_FMQ_PACKET_LAST 0x02u
#define FLOW_FMQ_INCOMPLETE 1

typedef enum flow_fmq_frame_kind_e {
  FLOW_FMQ_FRAME_HELLO = 1,
  FLOW_FMQ_FRAME_DATA = 2,
  FLOW_FMQ_FRAME_PING = 3,
  FLOW_FMQ_FRAME_PONG = 4,
  FLOW_FMQ_FRAME_SUBSCRIBE = 5,
  FLOW_FMQ_FRAME_UNSUBSCRIBE = 6
} flow_fmq_frame_kind_t;

typedef struct flow_fmq_frame_s {
  flow_fmq_frame_kind_t kind;
  turbo_flow_fmq_pattern_t pattern;
  uint64_t message_id;
  tstr_v identity;
  tstr_v topic;
  tstr_v payload;
  tstr_t owned_payload;
} flow_fmq_frame_t;

typedef enum flow_fmq_heartbeat_action_e {
  FLOW_FMQ_HEARTBEAT_WAIT = 0,
  FLOW_FMQ_HEARTBEAT_SEND_PING,
  FLOW_FMQ_HEARTBEAT_EXPIRED,
  FLOW_FMQ_HEARTBEAT_RECV_EXPIRED
} flow_fmq_heartbeat_action_t;

typedef struct flow_fmq_heartbeat_deadlines_s {
  uint64_t interval_ns;
  uint64_t timeout_ns;
  uint64_t recv_timeout_ns;
  uint64_t next_ping_ns;
  uint64_t heartbeat_deadline_ns;
  uint64_t recv_deadline_ns;
} flow_fmq_heartbeat_deadlines_t;

void flow_fmq_heartbeat_deadlines_init(flow_fmq_heartbeat_deadlines_t *state, uint64_t now_ns,
                                       uint64_t interval_ms, uint64_t timeout_ms,
                                       uint64_t recv_timeout_ms);
void flow_fmq_heartbeat_deadlines_on_receive(flow_fmq_heartbeat_deadlines_t *state,
                                             uint64_t now_ns);
void flow_fmq_heartbeat_deadlines_on_ping(flow_fmq_heartbeat_deadlines_t *state, uint64_t now_ns);
flow_fmq_heartbeat_action_t
flow_fmq_heartbeat_deadlines_next(const flow_fmq_heartbeat_deadlines_t *state, uint64_t now_ns,
                                  uint64_t *wait_deadline_ns);

int flow_fmq_encode_frame(const flow_fmq_frame_t *frame, size_t max_frame_size, tstr_t *out);
int flow_fmq_decode_frame(const char *data, size_t data_len, size_t max_frame_size,
                          flow_fmq_frame_t *out, size_t *consumed);
int flow_fmq_encoded_topic(const char *data, size_t data_len, size_t max_frame_size, tstr_v *topic);
void flow_fmq_frame_cleanup(flow_fmq_frame_t *frame);
int flow_fmq_encoded_size_limit(size_t max_frame_size, size_t *limit);
int flow_fmq_encoded_size(const flow_fmq_frame_t *frame, size_t max_frame_size, size_t *size);
int flow_fmq_config_validate(const turbo_flow_fmq_config_t *config);

#endif /* TURBO_FLOW_FMQ_PROTOCOL_H */
