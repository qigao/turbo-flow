#include "fmq_protocol.h"

#include "CoroNet/turbo_kcp.h"
#include "flowmq_coronet_transport.h"
#include "flow_coronet_runtime.h"
#include "turbo_error.h"

#include <limits.h>
#include <string.h>

#define FLOW_FMQ_DEFAULT_TIMEOUT_MS 1000u

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
  transport = flowmq_coronet_transport_coronet((flowmq_coronet_transport_t)config->transport);

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
  rc = flowmq_coronet_transport_kcp_fec_resolve(
      (flowmq_coronet_transport_t)config->transport, &fec_options, &fec_config,
      &fec_configured);
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
  if (config->heartbeat_interval_ms == 0 && config->heartbeat_timeout_ms == 0) return TURBO_OK;
  if (config->heartbeat_interval_ms == 0 || config->heartbeat_timeout_ms == 0) {
    return TURBO_EINVAL;
  }
  return config->heartbeat_timeout_ms < config->heartbeat_interval_ms ? TURBO_ERANGE : TURBO_OK;
}

int flow_fmq_config_validate(const turbo_flow_fmq_config_t *config) {
  tf_coronet_socket_timeout_config_t timeouts;
  uint64_t connection_timeout_ms;
  size_t max_frame_size;
  uint32_t max_connections;
  int rc;

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
  if (tf_coronet_connection_timeout_resolve(
          flowmq_coronet_transport_coronet((flowmq_coronet_transport_t)config->transport),
          &timeouts, &connection_timeout_ms) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  if (config->mode == TURBO_FLOW_FMQ_BIND &&
      (config->transport == TURBO_FLOW_FMQ_TLS || config->transport == TURBO_FLOW_FMQ_WS ||
       config->transport == TURBO_FLOW_FMQ_WSS) &&
      (timeouts.explicit_flags & TF_CORONET_TIMEOUT_SET_HANDSHAKE) != 0) {
    return TURBO_ENOTSUP;
  }
  rc = flow_fmq_coronet_options_validate(config);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_frame_hwm_validate(config);
  if (rc != TURBO_OK) return rc;
  rc = flow_fmq_heartbeat_config_validate(config);
  if (rc != TURBO_OK) return rc;
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
  if (config->mode == TURBO_FLOW_FMQ_CONNECT && config->max_connections != 0) return TURBO_EINVAL;
  if (config->reconnect_initial_ms > 0 && config->reconnect_initial_ms > config->reconnect_max_ms &&
      config->reconnect_max_ms != 0) {
    return TURBO_ERANGE;
  }
  if ((config->context && !config->take_context_ownership) ||
      (!config->context && config->take_context_ownership)) {
    return TURBO_ENOTSUP;
  }
  return TURBO_OK;
}
