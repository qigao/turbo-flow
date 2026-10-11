#ifndef PROTOCOL_NETWORK_E2E_FIXTURE_H
#define PROTOCOL_NETWORK_E2E_FIXTURE_H

#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_protocol_network_intake.h"

#include <cnet/cnet.h>

#if defined(FLOW_TURBODB_PLUGIN_MODULE)
  #include "turbo_flow_turbodb.h"
#endif

#include <stddef.h>
#include <stdint.h>

typedef enum protocol_network_e2e_storage_kind_e {
  PROTOCOL_NETWORK_E2E_STORAGE_MEMORY = 1,
  PROTOCOL_NETWORK_E2E_STORAGE_TURBODB = 2
} protocol_network_e2e_storage_kind_t;

typedef struct protocol_network_e2e_tcp_probe_s {
  size_t connected;
  size_t sent;
  size_t received;
  size_t bytes;
  size_t terminal;
  int failed;
  uint8_t last_payload[4096];
  size_t last_payload_size;
} protocol_network_e2e_tcp_probe_t;

typedef struct protocol_network_e2e_udp_probe_s {
  size_t received;
  size_t bytes;
  size_t sent;
  int last_send_status;
  uint8_t last_payload[4096];
  size_t last_payload_size;
} protocol_network_e2e_udp_probe_t;

typedef struct protocol_network_e2e_business_probe_s {
  size_t stage_completions;
  size_t failed_completions;
} protocol_network_e2e_business_probe_t;

typedef struct protocol_network_e2e_fixture_s {
  struct protocol_network_deployment_s *deployment;
  salts_component_plugin_runtime *component_runtime;
  turbo_flow_provider_resolver_v2_t provider_resolver;
  turbo_flow_resource_resolver_v1_t resource_resolver;
  unsigned deployment_slot;
  int packet_source;
  protocol_network_e2e_storage_kind_t storage;
  char *turbodb_path;
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *catalog;
  turbo_flow_protocol_network_intake_t *intake;
  turbo_flow_plugin_generation_t *business_generation;
  turbo_flow_plugin_generation_t *business_cleanup;
  cnet_datagram receiver;
  int receiver_initialized;
  uint16_t receiver_port;
  uint64_t next_udp_tag;
  cnet_client client;
  int client_initialized;
  cnet_connection connection;
  protocol_network_e2e_tcp_probe_t tcp;
  protocol_network_e2e_udp_probe_t udp;
  protocol_network_e2e_business_probe_t business;
  int reply_enabled;
} protocol_network_e2e_fixture_t;

int protocol_network_e2e_storage_prepare(protocol_network_e2e_fixture_t *fixture,
                                         protocol_network_e2e_storage_kind_t storage,
                                         char *turbodb_path);
void protocol_network_e2e_storage_cleanup(protocol_network_e2e_fixture_t *fixture);

int protocol_network_e2e_jtt808_init(protocol_network_e2e_fixture_t *fixture,
                                     const char *cnet_module, const char *jtt808_module,
                                     const char *durable_module,
                                     protocol_network_e2e_storage_kind_t storage,
                                     char *turbodb_path);
int protocol_network_e2e_jtt808_init_reply(protocol_network_e2e_fixture_t *fixture,
                                           const char *cnet_module, const char *jtt808_module,
                                           const char *durable_module,
                                           protocol_network_e2e_storage_kind_t storage,
                                           char *turbodb_path);
int protocol_network_e2e_coap_init(protocol_network_e2e_fixture_t *fixture, const char *cnet_module,
                                   const char *coap_module, const char *durable_module,
                                   protocol_network_e2e_storage_kind_t storage, char *turbodb_path);
int protocol_network_e2e_coap_init_reply(protocol_network_e2e_fixture_t *fixture,
                                         const char *cnet_module, const char *coap_module,
                                         const char *durable_module,
                                         protocol_network_e2e_storage_kind_t storage,
                                         char *turbodb_path);
int protocol_network_e2e_business_open(protocol_network_e2e_fixture_t *fixture);
int protocol_network_e2e_coap_init_peer(protocol_network_e2e_fixture_t *fixture,
                                        protocol_network_e2e_fixture_t *peer,
                                        const char *cnet_module, const char *coap_module,
                                        const char *durable_module,
                                        protocol_network_e2e_storage_kind_t storage, char *path);
int protocol_network_e2e_start(protocol_network_e2e_fixture_t *fixture,
                               turbo_flow_protocol_network_intake_snapshot_t *snapshot);
int protocol_network_e2e_tcp_connect(protocol_network_e2e_fixture_t *fixture, const char *endpoint,
                                     uint32_t timeout_ms);
int protocol_network_e2e_tcp_send(protocol_network_e2e_fixture_t *fixture, const void *data,
                                  size_t size);
int protocol_network_e2e_tcp_close(protocol_network_e2e_fixture_t *fixture, uint32_t timeout_ms);
int protocol_network_e2e_udp_send_frame(protocol_network_e2e_fixture_t *fixture,
                                        const char *endpoint, const void *data, size_t size);
int protocol_network_e2e_poll(protocol_network_e2e_fixture_t *fixture, uint32_t timeout_ms,
                              turbo_flow_protocol_network_intake_snapshot_t *snapshot);
int protocol_network_e2e_business_request_and_drive(protocol_network_e2e_fixture_t *fixture,
                                                    uint32_t timeout_ms);
int protocol_network_e2e_destroy(protocol_network_e2e_fixture_t *fixture);

size_t protocol_network_e2e_jtt808_frame(uint8_t *out, size_t capacity);

#endif /* PROTOCOL_NETWORK_E2E_FIXTURE_H */
