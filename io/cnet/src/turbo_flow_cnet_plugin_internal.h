#ifndef TURBO_FLOW_CNET_PLUGIN_INTERNAL_H
#define TURBO_FLOW_CNET_PLUGIN_INTERNAL_H

#include "turbo_flow_cnet.h"
#include "turbo_flow_plugin_generation.h"

enum {
  TURBO_FLOW_CNET_PLUGIN_NAME_CAPACITY = 256,
  TURBO_FLOW_CNET_PLUGIN_URI_CAPACITY = 512,
  TURBO_FLOW_CNET_PLUGIN_HOST_CAPACITY = 256,
  TURBO_FLOW_CNET_PLUGIN_TLS_PATH_CAPACITY = 512,
  TURBO_FLOW_CNET_PLUGIN_ALPN_CAPACITY = 8,
  TURBO_FLOW_CNET_PLUGIN_ALPN_NAME_CAPACITY = CNET_TLS_ALPN_NAME_MAX_BYTES + 1,
  TURBO_FLOW_CNET_PLUGIN_MAX_OWNERS = 256
};

typedef enum turbo_flow_cnet_plugin_kind_e {
  TURBO_FLOW_CNET_PLUGIN_STREAM_SOURCE = 0,
  TURBO_FLOW_CNET_PLUGIN_LISTENER_SOURCE,
  TURBO_FLOW_CNET_PLUGIN_PACKET_SOURCE,
  TURBO_FLOW_CNET_PLUGIN_STREAM_SINK,
  TURBO_FLOW_CNET_PLUGIN_DATAGRAM_SINK,
  TURBO_FLOW_CNET_PLUGIN_PACKET_SINK,
  TURBO_FLOW_CNET_PLUGIN_KIND_COUNT
} turbo_flow_cnet_plugin_kind_t;

typedef struct turbo_flow_cnet_plugin_config_s {
  turbo_flow_cnet_plugin_kind_t kind;
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
  char uri[TURBO_FLOW_CNET_PLUGIN_URI_CAPACITY];
  char bind_host[TURBO_FLOW_CNET_PLUGIN_HOST_CAPACITY];
  char peer_host[TURBO_FLOW_CNET_PLUGIN_HOST_CAPACITY];
  char tls_ca_file[TURBO_FLOW_CNET_PLUGIN_TLS_PATH_CAPACITY];
  char tls_ca_path[TURBO_FLOW_CNET_PLUGIN_TLS_PATH_CAPACITY];
  char tls_cert_file[TURBO_FLOW_CNET_PLUGIN_TLS_PATH_CAPACITY];
  char tls_key_file[TURBO_FLOW_CNET_PLUGIN_TLS_PATH_CAPACITY];
  char tls_key_password[TURBO_FLOW_CNET_PLUGIN_TLS_PATH_CAPACITY];
  char tls_server_name[TURBO_FLOW_CNET_PLUGIN_HOST_CAPACITY];
  const char *alpn[TURBO_FLOW_CNET_PLUGIN_ALPN_CAPACITY];
  char alpn_storage[TURBO_FLOW_CNET_PLUGIN_ALPN_CAPACITY]
                   [TURBO_FLOW_CNET_PLUGIN_ALPN_NAME_CAPACITY];
} turbo_flow_cnet_plugin_config_t;

const char *turbo_flow_cnet_plugin_kind_name(turbo_flow_cnet_plugin_kind_t kind);
int turbo_flow_cnet_plugin_config_read(const turbo_flow_resolved_config_t *resolved,
                                       const char *name, turbo_flow_cnet_plugin_kind_t kind,
                                       turbo_flow_cnet_plugin_config_t *config,
                                       turbo_flow_config_error_t *error);

#endif
