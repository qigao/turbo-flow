#ifndef TURBO_FLOW_CNET_TYPED_CONFIG_INTERNAL_H
#define TURBO_FLOW_CNET_TYPED_CONFIG_INTERNAL_H

#include "cnet_provider_config_native.h"
#include "turbo_flow_cnet.h"
#include "turbo_flow_cnet_resource.h"
#include "turbo_flow_provider.h"

enum {
  CNET_TYPED_STREAM_SOURCE = 0,
  CNET_TYPED_LISTENER_SOURCE,
  CNET_TYPED_PACKET_SOURCE,
  CNET_TYPED_STREAM_SINK,
  CNET_TYPED_DATAGRAM_SINK,
  CNET_TYPED_PACKET_SINK,
  CNET_TYPED_KIND_COUNT
};

#include "turbo_flow_cnet_source_contract_internal.h"

typedef struct cnet_typed_runtime_config_s {
  unsigned kind;

  cnet_client_config client;
  cnet_stream_socket_options socket_options;
  cnet_listener_config listener;
  cnet_listener_options listener_options;
  cnet_datagram_config datagram;
  cnet_packet_endpoint_config endpoint;
  cnet_tls_client_config tls_client;
  cnet_tls_server_config tls_server;
  cnet_datagram_peer peer;
  turbo_flow_content_descriptor_t content;

  size_t max_connections;
  size_t queue_capacity;
  size_t max_message_bytes;
  size_t scheduler_capacity;
  size_t scheduler_max_steps_per_poll;
  size_t actor_command_capacity;
  size_t actor_max_steps_per_poll;
  size_t adapter_send_capacity;
  uint64_t first_message_id;
  size_t initial_demand;
  uint32_t stop_timeout_ms;
  uint32_t conversation;
  int tls_enabled;

  /* Borrowed from the retained deployment resource lease. */
  const char *uri;
  const char *bind_host;
  const char *peer_host;
} cnet_typed_runtime_config_t;

int cnet_typed_stream_source_config(
    const CNetStreamSourceConfig_t *typed,
    const turbo_flow_cnet_deployment_view_t *deployment,
    const char *instance_name,
    cnet_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error);

int cnet_typed_listener_source_config(
    const CNetListenerSourceConfig_t *typed,
    const turbo_flow_cnet_deployment_view_t *deployment,
    const char *instance_name,
    cnet_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error);

int cnet_typed_packet_source_config(
    const CNetPacketSourceConfig_t *typed,
    const turbo_flow_cnet_deployment_view_t *deployment,
    const char *instance_name,
    cnet_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error);

int cnet_typed_stream_sink_config(
    const CNetStreamSinkConfig_t *typed,
    const turbo_flow_cnet_deployment_view_t *deployment,
    const char *instance_name,
    cnet_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error);

int cnet_typed_datagram_sink_config(
    const CNetDatagramSinkConfig_t *typed,
    const turbo_flow_cnet_deployment_view_t *deployment,
    const char *instance_name,
    cnet_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error);

int cnet_typed_packet_sink_config(
    const CNetPacketSinkConfig_t *typed,
    const turbo_flow_cnet_deployment_view_t *deployment,
    const char *instance_name,
    cnet_typed_runtime_config_t *out,
    turbo_flow_config_error_t *error);

#endif /* TURBO_FLOW_CNET_TYPED_CONFIG_INTERNAL_H */
