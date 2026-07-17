#ifndef FLOWMQ_PROTOCOL_H
#define FLOWMQ_PROTOCOL_H

#include "platform.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWMQ_PROTOCOL_API_VERSION 1u
#define FLOWMQ_PROTOCOL_WIRE_VERSION 2u
#define FLOWMQ_PROTOCOL_HEADER_SIZE 32u
#define FLOWMQ_PROTOCOL_PACKET_PAYLOAD_SIZE (64u * 1024u)
#define FLOWMQ_PROTOCOL_MAX_IDENTITY_SIZE 255u
#define FLOWMQ_PROTOCOL_MAX_TOPIC_SIZE 1024u
#define FLOWMQ_PROTOCOL_PACKET_FIRST 0x01u
#define FLOWMQ_PROTOCOL_PACKET_LAST 0x02u
#define FLOWMQ_PROTOCOL_INCOMPLETE 1

typedef uint8_t flowmq_protocol_pattern_t;

typedef enum flowmq_protocol_pattern_value_e {
  FLOWMQ_PROTOCOL_PUB = 1,
  FLOWMQ_PROTOCOL_SUB,
  FLOWMQ_PROTOCOL_PUSH,
  FLOWMQ_PROTOCOL_PULL,
  FLOWMQ_PROTOCOL_ROUTER,
  FLOWMQ_PROTOCOL_DEALER,
  FLOWMQ_PROTOCOL_PAIR,
  FLOWMQ_PROTOCOL_REQ,
  FLOWMQ_PROTOCOL_REP,
  FLOWMQ_PROTOCOL_XPUB,
  FLOWMQ_PROTOCOL_XSUB
} flowmq_protocol_pattern_value_t;

typedef enum flowmq_protocol_frame_kind_e {
  FLOWMQ_PROTOCOL_FRAME_HELLO = 1,
  FLOWMQ_PROTOCOL_FRAME_DATA,
  FLOWMQ_PROTOCOL_FRAME_PING,
  FLOWMQ_PROTOCOL_FRAME_PONG,
  FLOWMQ_PROTOCOL_FRAME_SUBSCRIBE,
  FLOWMQ_PROTOCOL_FRAME_UNSUBSCRIBE
} flowmq_protocol_frame_kind_t;

/**
 * Decoded FMQ v2 frame. Identity, topic, and single-packet payload are borrowed
 * from the input buffer. Multi-packet payload is owned by this value and must
 * be released with flowmq_protocol_frame_cleanup().
 */
typedef struct flowmq_protocol_frame_s {
  flowmq_protocol_frame_kind_t kind;
  flowmq_protocol_pattern_t pattern;
  uint64_t message_id;
  tstr_v identity;
  tstr_v topic;
  tstr_v payload;
  tstr_t owned_payload;
} flowmq_protocol_frame_t;

typedef enum flowmq_protocol_heartbeat_action_e {
  FLOWMQ_PROTOCOL_HEARTBEAT_WAIT = 0,
  FLOWMQ_PROTOCOL_HEARTBEAT_SEND_PING,
  FLOWMQ_PROTOCOL_HEARTBEAT_EXPIRED,
  FLOWMQ_PROTOCOL_HEARTBEAT_RECV_EXPIRED
} flowmq_protocol_heartbeat_action_t;

typedef struct flowmq_protocol_heartbeat_deadlines_s {
  uint64_t interval_ns;
  uint64_t timeout_ns;
  uint64_t recv_timeout_ns;
  uint64_t next_ping_ns;
  uint64_t heartbeat_deadline_ns;
  uint64_t recv_deadline_ns;
} flowmq_protocol_heartbeat_deadlines_t;

/**
 * Initialize heartbeat and receive deadlines from one monotonic timestamp.
 * Zero receive timeout disables only the receive deadline.
 */
CXX_C_API void flowmq_protocol_heartbeat_deadlines_init(
    flowmq_protocol_heartbeat_deadlines_t *state, uint64_t now_ns, uint64_t interval_ms,
    uint64_t timeout_ms, uint64_t recv_timeout_ms);
/** Advance all deadlines after receiving any valid FMQ frame. */
CXX_C_API void flowmq_protocol_heartbeat_deadlines_on_receive(
    flowmq_protocol_heartbeat_deadlines_t *state, uint64_t now_ns);
/** Advance only the next-ping deadline after sending a ping. */
CXX_C_API void flowmq_protocol_heartbeat_deadlines_on_ping(
    flowmq_protocol_heartbeat_deadlines_t *state, uint64_t now_ns);
/**
 * Select the next heartbeat action.
 * Writes the next absolute wait deadline only when returning
 * FLOWMQ_PROTOCOL_HEARTBEAT_WAIT. Invalid state is treated as receive expiry.
 */
CXX_C_API flowmq_protocol_heartbeat_action_t flowmq_protocol_heartbeat_deadlines_next(
    const flowmq_protocol_heartbeat_deadlines_t *state, uint64_t now_ns,
    uint64_t *wait_deadline_ns);

/**
 * Encode one complete frame. On success, caller owns *out and releases it with
 * tstr_free(). Returns TURBO_OK, TURBO_EINVAL, TURBO_EPROTO,
 * TURBO_EMSGSIZE, TURBO_ERANGE, or TURBO_ENOMEM.
 */
CXX_C_API int flowmq_protocol_encode_frame(const flowmq_protocol_frame_t *frame,
                                           size_t max_frame_size, tstr_t *out);

/**
 * Decode the first complete frame. Returns FLOWMQ_PROTOCOL_INCOMPLETE without
 * consuming input when more bytes are required. On success, consumed reports
 * the first frame length and out must be cleaned with
 * flowmq_protocol_frame_cleanup(). Other failures use the same validation,
 * size, and allocation errors as encode.
 */
CXX_C_API int flowmq_protocol_decode_frame(const char *data, size_t data_len,
                                           size_t max_frame_size,
                                           flowmq_protocol_frame_t *out, size_t *consumed);

/**
 * Borrow the first packet topic directly from encoded bytes. The view remains
 * valid while data is alive and unchanged. Returns TURBO_OK,
 * FLOWMQ_PROTOCOL_INCOMPLETE, or a concrete validation/size error.
 */
CXX_C_API int flowmq_protocol_encoded_topic(const char *data, size_t data_len,
                                            size_t max_frame_size, tstr_v *topic);
/** Release any reassembled payload and reset the frame. NULL is accepted. */
CXX_C_API void flowmq_protocol_frame_cleanup(flowmq_protocol_frame_t *frame);

/**
 * Return the maximum encoded byte count for max_frame_size. Returns TURBO_OK,
 * TURBO_EINVAL, or TURBO_ERANGE.
 */
CXX_C_API int flowmq_protocol_encoded_size_limit(size_t max_frame_size, size_t *limit);

/**
 * Validate frame and return its exact encoded byte count. Returns the same
 * errors as flowmq_protocol_encode_frame(), except allocation cannot fail.
 */
CXX_C_API int flowmq_protocol_encoded_size(const flowmq_protocol_frame_t *frame,
                                           size_t max_frame_size, size_t *size);

#ifdef __cplusplus
}
#endif

#endif /* FLOWMQ_PROTOCOL_H */
