#include "fmq_protocol.h"

#include "CoroNet/turbo_kcp.h"
#include "flow_coronet_runtime.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <limits.h>
#include <string.h>

static const unsigned char FLOW_FMQ_MAGIC[4] = {'T', 'F', 'M', 'Q'};
#define FLOW_FMQ_DEFAULT_TIMEOUT_MS 1000u

static tf_coronet_transport_t flow_fmq_coronet_transport(turbo_flow_fmq_transport_t transport) {
  switch (transport) {
  case TURBO_FLOW_FMQ_TCP:
    return TF_CORONET_TRANSPORT_TCP;
  case TURBO_FLOW_FMQ_TLS:
    return TF_CORONET_TRANSPORT_TLS;
  case TURBO_FLOW_FMQ_UDP:
    return TF_CORONET_TRANSPORT_UDP;
  case TURBO_FLOW_FMQ_KCP:
    return TF_CORONET_TRANSPORT_KCP;
  case TURBO_FLOW_FMQ_PIPE:
    return TF_CORONET_TRANSPORT_PIPE;
  case TURBO_FLOW_FMQ_WS:
    return TF_CORONET_TRANSPORT_WS;
  case TURBO_FLOW_FMQ_WSS:
    return TF_CORONET_TRANSPORT_WSS;
  default:
    return TF_CORONET_TRANSPORT_COUNT;
  }
}

static void flow_fmq_timeout_config_resolve(tf_coronet_socket_timeout_config_t *timeouts,
                                            const turbo_flow_fmq_config_t *config) {
  if (!timeouts || !config) return;
  timeouts->timeout_ms = config->timeout_ms;
  timeouts->connect_timeout_ms = config->connect_timeout_ms;
  timeouts->send_timeout_ms = config->send_timeout_ms;
  timeouts->recv_timeout_ms = config->recv_timeout_ms;
  timeouts->handshake_timeout_ms = config->handshake_timeout_ms;
  if (timeouts->timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_DEFAULT;
  if (timeouts->connect_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_CONNECT;
  if (timeouts->send_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_SEND;
  if (timeouts->recv_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_RECV;
  if (timeouts->handshake_timeout_ms != 0) timeouts->set_flags |= TF_CORONET_TIMEOUT_SET_HANDSHAKE;
  if (timeouts->timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) timeouts->timeout_ms = 0;
  if (timeouts->connect_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED)
    timeouts->connect_timeout_ms = 0;
  if (timeouts->send_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) timeouts->send_timeout_ms = 0;
  if (timeouts->recv_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) timeouts->recv_timeout_ms = 0;
  if (timeouts->handshake_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED)
    timeouts->handshake_timeout_ms = 0;
  tf_coronet_socket_timeouts_resolve(timeouts, FLOW_FMQ_DEFAULT_TIMEOUT_MS);
}

static int flow_fmq_coronet_options_validate(const turbo_flow_fmq_config_t *config) {
  tf_coronet_transport_t transport;
  tf_coronet_kcp_fec_options_t fec_options;
  turbo_kcp_fec_config_t fec_config;
  tf_coronet_socket_options_t socket_options;
  tf_coronet_udp_options_t udp_options;
  int fec_configured;
  int rc;
  if (!config) return TURBO_EINVAL;
  transport = flow_fmq_coronet_transport(config->transport);

  rc = tf_coronet_endpoint_config_validate(transport, config->host, config->port, config->path);
  if (rc != TURBO_OK) return rc;
  rc = tf_coronet_reuse_port_validate(transport, config->reuse_port,
                                      config->mode == TURBO_FLOW_FMQ_BIND);
  if (rc != TURBO_OK) return rc;

  memset(&fec_options, 0, sizeof(fec_options));
  fec_options.enabled = config->kcp_fec;
  fec_options.backend = config->kcp_fec_backend;
  fec_options.data_shards = config->kcp_fec_data_shards;
  fec_options.parity_shards = config->kcp_fec_parity_shards;
  fec_options.max_payload_size = config->kcp_fec_max_payload_size;
  rc = tf_coronet_kcp_fec_options_resolve(transport, &fec_options, &fec_config, &fec_configured);
  if (rc != TURBO_OK) return rc;

  memset(&socket_options, 0, sizeof(socket_options));
  socket_options.tcp_keepalive = config->tcp_keepalive;
  socket_options.tcp_keepalive_idle_ms = config->tcp_keepalive_idle_ms;
  socket_options.tcp_keepalive_interval_ms = config->tcp_keepalive_interval_ms;
  socket_options.tcp_keepalive_count = config->tcp_keepalive_count;
  socket_options.linger = config->linger;
  socket_options.linger_ms = config->linger_ms;
  socket_options.send_hwm_bytes = config->send_hwm_bytes;
  rc = tf_coronet_socket_options_validate(transport, &socket_options);
  if (rc != TURBO_OK) return rc;

  memset(&udp_options, 0, sizeof(udp_options));
  udp_options.multicast_group = config->udp_multicast_group;
  udp_options.multicast_interface = config->udp_multicast_interface;
  udp_options.option_flags = config->udp_option_flags;
  udp_options.multicast_loop = config->udp_multicast_loop;
  udp_options.multicast_ttl = config->udp_multicast_ttl;
  udp_options.broadcast = config->udp_broadcast;
  return tf_coronet_udp_options_validate(transport, &udp_options,
                                         config->mode == TURBO_FLOW_FMQ_BIND);
}

static int flow_fmq_frame_hwm_validate(const turbo_flow_fmq_config_t *config) {
  if (!config) return TURBO_EINVAL;
  if (config->frame_hwm_bytes != 0 && config->frame_hwm_bytes < FLOW_FMQ_HEADER_SIZE) {
    return TURBO_ERANGE;
  }
  if (config->frame_admission_policy < TURBO_FLOW_FMQ_FRAME_ADMISSION_FAIL ||
      config->frame_admission_policy > TURBO_FLOW_FMQ_FRAME_ADMISSION_DROP_OLDEST) {
    return TURBO_EINVAL;
  }
  if (config->frame_admission_policy != TURBO_FLOW_FMQ_FRAME_ADMISSION_BLOCK &&
      config->frame_admission_timeout_ms != 0) {
    return TURBO_EINVAL;
  }
  if (config->frame_admission_policy != TURBO_FLOW_FMQ_FRAME_ADMISSION_FAIL &&
      config->frame_hwm_messages == 0 && config->frame_hwm_bytes == 0) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static int flow_fmq_heartbeat_config_validate(const turbo_flow_fmq_config_t *config) {
  if (!config) return TURBO_EINVAL;
  if (config->heartbeat_interval_ms == 0 && config->heartbeat_timeout_ms == 0) {
    return TURBO_OK;
  }
  if (config->heartbeat_interval_ms == 0 || config->heartbeat_timeout_ms == 0) {
    return TURBO_EINVAL;
  }
  if (config->heartbeat_timeout_ms < config->heartbeat_interval_ms) {
    return TURBO_ERANGE;
  }
  return TURBO_OK;
}

static void flow_fmq_write_u16(unsigned char *out, uint16_t value) {
  out[0] = (unsigned char)(value >> 8);
  out[1] = (unsigned char)value;
}

static void flow_fmq_write_u32(unsigned char *out, uint32_t value) {
  out[0] = (unsigned char)(value >> 24);
  out[1] = (unsigned char)(value >> 16);
  out[2] = (unsigned char)(value >> 8);
  out[3] = (unsigned char)value;
}

static uint16_t flow_fmq_read_u16(const unsigned char *data) {
  return (uint16_t)(((uint16_t)data[0] << 8) | data[1]);
}

static uint32_t flow_fmq_read_u32(const unsigned char *data) {
  return ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) | ((uint32_t)data[2] << 8) | data[3];
}

static uint64_t flow_fmq_deadline_duration_ns(uint64_t duration_ms) {
  return duration_ms > UINT64_MAX / UINT64_C(1000000) ? UINT64_MAX
                                                      : duration_ms * UINT64_C(1000000);
}

static uint64_t flow_fmq_deadline_add(uint64_t now_ns, uint64_t duration_ns) {
  return duration_ns == UINT64_MAX || now_ns > UINT64_MAX - duration_ns ? UINT64_MAX
                                                                        : now_ns + duration_ns;
}

void flow_fmq_heartbeat_deadlines_init(flow_fmq_heartbeat_deadlines_t *state, uint64_t now_ns,
                                       uint64_t interval_ms, uint64_t timeout_ms,
                                       uint64_t recv_timeout_ms) {
  if (!state) return;
  memset(state, 0, sizeof(*state));
  state->interval_ns = flow_fmq_deadline_duration_ns(interval_ms);
  state->timeout_ns = flow_fmq_deadline_duration_ns(timeout_ms);
  state->recv_timeout_ns =
      recv_timeout_ms == 0u ? UINT64_MAX : flow_fmq_deadline_duration_ns(recv_timeout_ms);
  flow_fmq_heartbeat_deadlines_on_receive(state, now_ns);
}

void flow_fmq_heartbeat_deadlines_on_receive(flow_fmq_heartbeat_deadlines_t *state,
                                             uint64_t now_ns) {
  if (!state) return;
  state->next_ping_ns = flow_fmq_deadline_add(now_ns, state->interval_ns);
  state->heartbeat_deadline_ns = flow_fmq_deadline_add(now_ns, state->timeout_ns);
  state->recv_deadline_ns = flow_fmq_deadline_add(now_ns, state->recv_timeout_ns);
}

void flow_fmq_heartbeat_deadlines_on_ping(flow_fmq_heartbeat_deadlines_t *state, uint64_t now_ns) {
  if (!state) return;
  state->next_ping_ns = flow_fmq_deadline_add(now_ns, state->interval_ns);
}

flow_fmq_heartbeat_action_t
flow_fmq_heartbeat_deadlines_next(const flow_fmq_heartbeat_deadlines_t *state, uint64_t now_ns,
                                  uint64_t *wait_deadline_ns) {
  uint64_t deadline;
  if (!state || !wait_deadline_ns || state->interval_ns == 0u || state->timeout_ns == 0u) {
    return FLOW_FMQ_HEARTBEAT_RECV_EXPIRED;
  }
  *wait_deadline_ns = 0u;
  if (state->heartbeat_deadline_ns <= now_ns) return FLOW_FMQ_HEARTBEAT_EXPIRED;
  if (state->recv_deadline_ns <= now_ns) return FLOW_FMQ_HEARTBEAT_RECV_EXPIRED;
  if (state->next_ping_ns <= now_ns) return FLOW_FMQ_HEARTBEAT_SEND_PING;
  deadline = state->next_ping_ns;
  if (state->heartbeat_deadline_ns < deadline) deadline = state->heartbeat_deadline_ns;
  if (state->recv_deadline_ns < deadline) deadline = state->recv_deadline_ns;
  *wait_deadline_ns = deadline;
  return FLOW_FMQ_HEARTBEAT_WAIT;
}

static int flow_fmq_frame_lengths(const flow_fmq_frame_t *frame, size_t max_frame_size,
                                  size_t *total) {
  size_t body;
  size_t packet_count;
  if (!frame || !total || frame->kind < FLOW_FMQ_FRAME_HELLO ||
      frame->kind > FLOW_FMQ_FRAME_UNSUBSCRIBE || frame->pattern < TURBO_FLOW_FMQ_PUB ||
      frame->pattern > TURBO_FLOW_FMQ_XSUB) {
    return TURBO_EINVAL;
  }
  if ((frame->identity.len > 0 && !frame->identity.data) ||
      (frame->topic.len > 0 && !frame->topic.data) ||
      (frame->payload.len > 0 && !frame->payload.data)) {
    return TURBO_EINVAL;
  }
  if (frame->identity.len > TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE ||
      frame->topic.len > TURBO_FLOW_FMQ_MAX_TOPIC_SIZE || frame->payload.len > UINT32_MAX) {
    return TURBO_EMSGSIZE;
  }
  if (frame->identity.len > SIZE_MAX - frame->topic.len ||
      frame->identity.len + frame->topic.len > SIZE_MAX - frame->payload.len) {
    return TURBO_ERANGE;
  }
  if ((frame->kind == FLOW_FMQ_FRAME_PING || frame->kind == FLOW_FMQ_FRAME_PONG) &&
      (frame->identity.len != 0 || frame->topic.len != 0 || frame->payload.len != 0)) {
    return TURBO_EPROTO;
  }
  if ((frame->kind == FLOW_FMQ_FRAME_SUBSCRIBE || frame->kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) &&
      (frame->identity.len != 0 || frame->payload.len != 0)) {
    return TURBO_EPROTO;
  }
  if ((frame->kind == FLOW_FMQ_FRAME_SUBSCRIBE || frame->kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) &&
      frame->pattern != TURBO_FLOW_FMQ_SUB && frame->pattern != TURBO_FLOW_FMQ_XSUB) {
    return TURBO_EPROTO;
  }
  if (frame->kind == FLOW_FMQ_FRAME_HELLO && frame->payload.len != 0u) return TURBO_EPROTO;
  if (frame->kind == FLOW_FMQ_FRAME_DATA && frame->message_id == 0u) return TURBO_EPROTO;
  if (frame->kind != FLOW_FMQ_FRAME_DATA && frame->message_id != 0u) return TURBO_EPROTO;
  body = frame->identity.len + frame->topic.len + frame->payload.len;
  if (body > max_frame_size) return TURBO_EMSGSIZE;
  packet_count = frame->kind == FLOW_FMQ_FRAME_DATA && frame->payload.len > 0
                     ? (frame->payload.len - 1u) / FLOW_FMQ_PACKET_PAYLOAD_SIZE + 1u
                     : 1u;
  if (packet_count > (SIZE_MAX - body) / FLOW_FMQ_HEADER_SIZE) return TURBO_ERANGE;
  *total = body + packet_count * FLOW_FMQ_HEADER_SIZE;
  return TURBO_OK;
}

int flow_fmq_encoded_size(const flow_fmq_frame_t *frame, size_t max_frame_size, size_t *size) {
  return flow_fmq_frame_lengths(frame, max_frame_size, size);
}

static void flow_fmq_write_u64(unsigned char *dst, uint64_t value) {
  for (unsigned int i = 0; i < 8u; ++i)
    dst[i] = (unsigned char)(value >> (56u - i * 8u));
}

static uint64_t flow_fmq_read_u64(const unsigned char *src) {
  uint64_t value = 0u;
  for (unsigned int i = 0; i < 8u; ++i)
    value = (value << 8u) | src[i];
  return value;
}

int flow_fmq_encoded_size_limit(size_t max_frame_size, size_t *limit) {
  size_t packet_count;
  if (!limit || max_frame_size == 0u) return TURBO_EINVAL;
  packet_count = (max_frame_size - 1u) / FLOW_FMQ_PACKET_PAYLOAD_SIZE + 1u;
  if (packet_count > (SIZE_MAX - max_frame_size) / FLOW_FMQ_HEADER_SIZE) return TURBO_ERANGE;
  *limit = max_frame_size + packet_count * FLOW_FMQ_HEADER_SIZE;
  return TURBO_OK;
}

int flow_fmq_encode_frame(const flow_fmq_frame_t *frame, size_t max_frame_size, tstr_t *out) {
  size_t total;
  size_t encoded_offset = 0u;
  size_t payload_offset = 0u;
  int rc;
  if (!out || *out) return TURBO_EINVAL;
  rc = flow_fmq_frame_lengths(frame, max_frame_size, &total);
  if (rc != TURBO_OK) return rc;
  *out = tstr_new_len(NULL, total);
  if (!*out) return TURBO_ENOMEM;
  do {
    unsigned char *header = (unsigned char *)*out + encoded_offset;
    size_t chunk_len = frame->payload.len - payload_offset;
    uint8_t flags = payload_offset == 0u ? FLOW_FMQ_PACKET_FIRST : 0u;
    uint16_t identity_len = payload_offset == 0u ? (uint16_t)frame->identity.len : 0u;
    uint16_t topic_len = payload_offset == 0u ? (uint16_t)frame->topic.len : 0u;
    if (chunk_len > FLOW_FMQ_PACKET_PAYLOAD_SIZE) chunk_len = FLOW_FMQ_PACKET_PAYLOAD_SIZE;
    if (payload_offset + chunk_len == frame->payload.len) flags |= FLOW_FMQ_PACKET_LAST;
    memcpy(header, FLOW_FMQ_MAGIC, sizeof(FLOW_FMQ_MAGIC));
    header[4] = FLOW_FMQ_PROTOCOL_VERSION;
    header[5] = (unsigned char)frame->kind;
    header[6] = (unsigned char)frame->pattern;
    header[7] = flags;
    flow_fmq_write_u16(header + 8, identity_len);
    flow_fmq_write_u16(header + 10, topic_len);
    flow_fmq_write_u32(header + 12, (uint32_t)chunk_len);
    flow_fmq_write_u64(header + 16, frame->message_id);
    flow_fmq_write_u32(header + 24, (uint32_t)frame->payload.len);
    flow_fmq_write_u32(header + 28, (uint32_t)payload_offset);
    encoded_offset += FLOW_FMQ_HEADER_SIZE;
    if (identity_len > 0u) {
      memcpy(*out + encoded_offset, frame->identity.data, identity_len);
      encoded_offset += identity_len;
    }
    if (topic_len > 0u) {
      memcpy(*out + encoded_offset, frame->topic.data, topic_len);
      encoded_offset += topic_len;
    }
    if (chunk_len > 0u) {
      memcpy(*out + encoded_offset, frame->payload.data + payload_offset, chunk_len);
      encoded_offset += chunk_len;
      payload_offset += chunk_len;
    }
  } while (payload_offset < frame->payload.len);
  return TURBO_OK;
}

typedef struct flow_fmq_packet_s {
  flow_fmq_frame_kind_t kind;
  turbo_flow_fmq_pattern_t pattern;
  uint8_t flags;
  uint16_t identity_len;
  uint16_t topic_len;
  uint32_t chunk_len;
  uint64_t message_id;
  uint32_t payload_len;
  uint32_t payload_offset;
  size_t record_len;
} flow_fmq_packet_t;

static int flow_fmq_decode_packet(const char *data, size_t data_len, flow_fmq_packet_t *packet) {
  const unsigned char *header = (const unsigned char *)data;
  size_t body_len;
  if (!packet || (data_len > 0u && !data)) return TURBO_EINVAL;
  if (data_len >= sizeof(FLOW_FMQ_MAGIC) &&
      memcmp(header, FLOW_FMQ_MAGIC, sizeof(FLOW_FMQ_MAGIC)) != 0)
    return TURBO_EPROTO;
  if (data_len >= 5u && header[4] != FLOW_FMQ_PROTOCOL_VERSION) return TURBO_EPROTO;
  if (data_len < FLOW_FMQ_HEADER_SIZE) return FLOW_FMQ_INCOMPLETE;
  if ((header[7] & ~(FLOW_FMQ_PACKET_FIRST | FLOW_FMQ_PACKET_LAST)) != 0u) return TURBO_EPROTO;
  if (header[5] < FLOW_FMQ_FRAME_HELLO || header[5] > FLOW_FMQ_FRAME_UNSUBSCRIBE ||
      header[6] < TURBO_FLOW_FMQ_PUB || header[6] > TURBO_FLOW_FMQ_XSUB)
    return TURBO_EPROTO;
  memset(packet, 0, sizeof(*packet));
  packet->kind = (flow_fmq_frame_kind_t)header[5];
  packet->pattern = (turbo_flow_fmq_pattern_t)header[6];
  packet->flags = header[7];
  packet->identity_len = flow_fmq_read_u16(header + 8);
  packet->topic_len = flow_fmq_read_u16(header + 10);
  packet->chunk_len = flow_fmq_read_u32(header + 12);
  packet->message_id = flow_fmq_read_u64(header + 16);
  packet->payload_len = flow_fmq_read_u32(header + 24);
  packet->payload_offset = flow_fmq_read_u32(header + 28);
  if (packet->identity_len > TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE ||
      packet->topic_len > TURBO_FLOW_FMQ_MAX_TOPIC_SIZE ||
      packet->chunk_len > FLOW_FMQ_PACKET_PAYLOAD_SIZE)
    return TURBO_EMSGSIZE;
  body_len = (size_t)packet->identity_len + packet->topic_len + packet->chunk_len;
  if (body_len > SIZE_MAX - FLOW_FMQ_HEADER_SIZE) return TURBO_ERANGE;
  packet->record_len = FLOW_FMQ_HEADER_SIZE + body_len;
  return data_len < packet->record_len ? FLOW_FMQ_INCOMPLETE : TURBO_OK;
}

int flow_fmq_encoded_topic(const char *data, size_t data_len, size_t max_frame_size,
                           tstr_v *topic) {
  flow_fmq_packet_t packet;
  int rc;
  if (!topic) return TURBO_EINVAL;
  *topic = (tstr_v){0};
  rc = flow_fmq_decode_packet(data, data_len, &packet);
  if (rc != TURBO_OK) return rc;
  if ((packet.flags & FLOW_FMQ_PACKET_FIRST) == 0u || packet.payload_offset != 0u ||
      (size_t)packet.identity_len + packet.topic_len + packet.payload_len > max_frame_size) {
    return TURBO_EPROTO;
  }
  if (packet.kind == FLOW_FMQ_FRAME_DATA) {
    if (packet.message_id == 0u) return TURBO_EPROTO;
  } else if (packet.message_id != 0u) {
    return TURBO_EPROTO;
  }
  *topic = tstr_v_from_buf(data + FLOW_FMQ_HEADER_SIZE + packet.identity_len, packet.topic_len);
  return TURBO_OK;
}

int flow_fmq_decode_frame(const char *data, size_t data_len, size_t max_frame_size,
                          flow_fmq_frame_t *out, size_t *consumed) {
  flow_fmq_packet_t first;
  size_t cursor = 0u;
  size_t payload_copied = 0u;
  size_t packet_count = 0u;
  int rc;
  if (!out || !consumed || (data_len > 0 && !data)) return TURBO_EINVAL;
  *consumed = 0;
  memset(out, 0, sizeof(*out));
  rc = flow_fmq_decode_packet(data, data_len, &first);
  if (rc != TURBO_OK) return rc;
  if ((first.flags & FLOW_FMQ_PACKET_FIRST) == 0u || first.payload_offset != 0u)
    return TURBO_EPROTO;
  if ((size_t)first.identity_len + first.topic_len + first.payload_len > max_frame_size)
    return TURBO_EMSGSIZE;
  if (first.kind == FLOW_FMQ_FRAME_DATA) {
    if (first.message_id == 0u) return TURBO_EPROTO;
  } else if (first.message_id != 0u || first.payload_len != first.chunk_len ||
             (first.flags & FLOW_FMQ_PACKET_LAST) == 0u) {
    return TURBO_EPROTO;
  }
  for (;;) {
    flow_fmq_packet_t packet;
    rc = flow_fmq_decode_packet(data + cursor, data_len - cursor, &packet);
    if (rc != TURBO_OK) return rc;
    if (packet.kind != first.kind || packet.pattern != first.pattern ||
        packet.message_id != first.message_id || packet.payload_len != first.payload_len ||
        packet.payload_offset != payload_copied)
      return TURBO_EPROTO;
    if (packet_count == 0u) {
      if ((packet.flags & FLOW_FMQ_PACKET_FIRST) == 0u) return TURBO_EPROTO;
    } else if ((packet.flags & FLOW_FMQ_PACKET_FIRST) != 0u || packet.identity_len != 0u ||
               packet.topic_len != 0u)
      return TURBO_EPROTO;
    if ((size_t)packet.chunk_len > first.payload_len - payload_copied) return TURBO_EPROTO;
    payload_copied += packet.chunk_len;
    cursor += packet.record_len;
    packet_count += 1u;
    if ((packet.flags & FLOW_FMQ_PACKET_LAST) != 0u) break;
    if (packet.chunk_len == 0u || payload_copied == first.payload_len) return TURBO_EPROTO;
  }
  if (payload_copied != first.payload_len) return TURBO_EPROTO;
  out->kind = first.kind;
  out->pattern = first.pattern;
  out->message_id = first.message_id;
  out->identity = tstr_v_from_buf(data + FLOW_FMQ_HEADER_SIZE, first.identity_len);
  out->topic = tstr_v_from_buf(data + FLOW_FMQ_HEADER_SIZE + first.identity_len, first.topic_len);
  if (packet_count == 1u) {
    out->payload = tstr_v_from_buf(
        data + FLOW_FMQ_HEADER_SIZE + first.identity_len + first.topic_len, first.payload_len);
  } else {
    size_t read_cursor = 0u;
    size_t write_offset = 0u;
    out->owned_payload = tstr_new_len(NULL, first.payload_len);
    if (!out->owned_payload) return TURBO_ENOMEM;
    while (read_cursor < cursor) {
      flow_fmq_packet_t packet;
      rc = flow_fmq_decode_packet(data + read_cursor, cursor - read_cursor, &packet);
      if (rc != TURBO_OK) {
        flow_fmq_frame_cleanup(out);
        return rc;
      }
      if (packet.chunk_len > 0u) {
        memcpy(out->owned_payload + write_offset,
               data + read_cursor + FLOW_FMQ_HEADER_SIZE + packet.identity_len + packet.topic_len,
               packet.chunk_len);
        write_offset += packet.chunk_len;
      }
      read_cursor += packet.record_len;
    }
    out->payload = tstr_to_v(out->owned_payload);
  }
  if ((out->kind == FLOW_FMQ_FRAME_PING || out->kind == FLOW_FMQ_FRAME_PONG) &&
      (out->identity.len != 0 || out->topic.len != 0 || out->payload.len != 0)) {
    flow_fmq_frame_cleanup(out);
    return TURBO_EPROTO;
  }
  if ((out->kind == FLOW_FMQ_FRAME_SUBSCRIBE || out->kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) &&
      (out->identity.len != 0 || out->payload.len != 0)) {
    flow_fmq_frame_cleanup(out);
    return TURBO_EPROTO;
  }
  if ((out->kind == FLOW_FMQ_FRAME_SUBSCRIBE || out->kind == FLOW_FMQ_FRAME_UNSUBSCRIBE) &&
      out->pattern != TURBO_FLOW_FMQ_SUB && out->pattern != TURBO_FLOW_FMQ_XSUB) {
    flow_fmq_frame_cleanup(out);
    return TURBO_EPROTO;
  }
  *consumed = cursor;
  return TURBO_OK;
}

void flow_fmq_frame_cleanup(flow_fmq_frame_t *frame) {
  if (!frame) return;
  tstr_freep(&frame->owned_payload);
  memset(frame, 0, sizeof(*frame));
}

int flow_fmq_config_validate(const turbo_flow_fmq_config_t *config) {
  tf_coronet_socket_timeout_config_t timeouts;
  uint64_t connection_timeout_ms;
  size_t max_frame_size;
  uint32_t max_connections;
  if (!config || config->size < sizeof(*config) || config->version != TURBO_FLOW_FMQ_API_VERSION ||
      turbo_flow_fmq_pattern_validate(config->pattern) != TURBO_OK ||
      config->mode < TURBO_FLOW_FMQ_BIND || config->mode > TURBO_FLOW_FMQ_CONNECT ||
      config->transport < TURBO_FLOW_FMQ_TCP || config->transport > TURBO_FLOW_FMQ_WSS) {
    return TURBO_EINVAL;
  }
  if (config->timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED ||
      config->connect_timeout_ms == TURBO_FLOW_FMQ_TIMEOUT_DISABLED) {
    return TURBO_EINVAL;
  }
  memset(&timeouts, 0, sizeof(timeouts));
  flow_fmq_timeout_config_resolve(&timeouts, config);
  if (tf_coronet_connection_timeout_resolve(flow_fmq_coronet_transport(config->transport),
                                            &timeouts, &connection_timeout_ms) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  if (config->mode == TURBO_FLOW_FMQ_BIND &&
      (config->transport == TURBO_FLOW_FMQ_TLS || config->transport == TURBO_FLOW_FMQ_WS ||
       config->transport == TURBO_FLOW_FMQ_WSS) &&
      (timeouts.explicit_flags & TF_CORONET_TIMEOUT_SET_HANDSHAKE) != 0) {
    return TURBO_ENOTSUP;
  }
  {
    int coronet_options_rc = flow_fmq_coronet_options_validate(config);
    if (coronet_options_rc != TURBO_OK) return coronet_options_rc;
  }
  {
    int frame_hwm_rc = flow_fmq_frame_hwm_validate(config);
    if (frame_hwm_rc != TURBO_OK) return frame_hwm_rc;
  }
  {
    int heartbeat_rc = flow_fmq_heartbeat_config_validate(config);
    if (heartbeat_rc != TURBO_OK) return heartbeat_rc;
  }
  if ((config->pattern == TURBO_FLOW_FMQ_PUB || config->pattern == TURBO_FLOW_FMQ_PUSH ||
       config->pattern == TURBO_FLOW_FMQ_ROUTER || config->pattern == TURBO_FLOW_FMQ_REP ||
       config->pattern == TURBO_FLOW_FMQ_XPUB) &&
      config->mode != TURBO_FLOW_FMQ_BIND) {
    return TURBO_EINVAL;
  }
  if ((config->pattern == TURBO_FLOW_FMQ_SUB || config->pattern == TURBO_FLOW_FMQ_PULL ||
       config->pattern == TURBO_FLOW_FMQ_DEALER || config->pattern == TURBO_FLOW_FMQ_REQ ||
       config->pattern == TURBO_FLOW_FMQ_XSUB) &&
      config->mode != TURBO_FLOW_FMQ_CONNECT) {
    return TURBO_EINVAL;
  }
  if (config->pattern == TURBO_FLOW_FMQ_DEALER &&
      (!config->identity || config->identity[0] == '\0')) {
    return TURBO_EINVAL;
  }
  if (config->identity && strlen(config->identity) > TURBO_FLOW_FMQ_MAX_IDENTITY_SIZE) {
    return TURBO_EMSGSIZE;
  }
  if (config->topic && strlen(config->topic) > TURBO_FLOW_FMQ_MAX_TOPIC_SIZE) {
    return TURBO_EMSGSIZE;
  }
  if (config->topic_policy != 0 && (config->topic_policy < TURBO_FLOW_FMQ_METADATA_STATIC ||
                                    config->topic_policy > TURBO_FLOW_FMQ_METADATA_CONTENT)) {
    return TURBO_EINVAL;
  }
  if (config->identity_policy != 0 && (config->identity_policy < TURBO_FLOW_FMQ_METADATA_STATIC ||
                                       config->identity_policy > TURBO_FLOW_FMQ_METADATA_CONTENT)) {
    return TURBO_EINVAL;
  }
  max_frame_size =
      config->max_frame_size ? config->max_frame_size : TURBO_FLOW_FMQ_DEFAULT_MAX_FRAME_SIZE;
  max_connections =
      config->max_connections ? config->max_connections : TURBO_FLOW_FMQ_DEFAULT_MAX_CONNECTIONS;
  if (max_frame_size == 0 || max_frame_size > UINT32_MAX || max_connections == 0 ||
      max_connections > TURBO_FLOW_FMQ_MAX_CONNECTIONS_LIMIT) {
    return TURBO_ERANGE;
  }
  if (config->mode == TURBO_FLOW_FMQ_CONNECT && config->max_connections != 0) {
    return TURBO_EINVAL;
  }
  if (config->reconnect_initial_ms > 0 && config->reconnect_initial_ms > config->reconnect_max_ms &&
      config->reconnect_max_ms != 0) {
    return TURBO_ERANGE;
  }
  if ((config->context && !config->take_context_ownership) ||
      (!config->context && config->take_context_ownership))
    return TURBO_ENOTSUP;
  return TURBO_OK;
}
