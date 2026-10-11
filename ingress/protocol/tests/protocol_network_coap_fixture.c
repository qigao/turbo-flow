#include "protocol_network_deployment.h"
#include "protocol_network_e2e_fixture.h"
#include "protocol_network_graphs.h"

#include <stdio.h>
#include <string.h>

static native_io_backend_kind protocol_network_coap_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__APPLE__)
  return NATIVE_IO_BACKEND_KQUEUE;
#else
  return NATIVE_IO_BACKEND_EPOLL;
#endif
}

static void protocol_network_coap_receive(void *user, cnet_datagram *datagram,
                                          const cnet_datagram_peer *peer,
                                          const cnet_receive_view *view) {
  protocol_network_e2e_udp_probe_t *probe = (protocol_network_e2e_udp_probe_t *)user;
  (void)datagram;
  (void)peer;
  if (!probe || !view || !view->data || view->size == 0u ||
      view->size > sizeof(probe->last_payload))
    return;
  memcpy(probe->last_payload, view->data, view->size);
  probe->last_payload_size = view->size;
  ++probe->received;
  probe->bytes += view->size;
}

static void protocol_network_coap_sent(void *user, cnet_datagram *datagram,
                                       const cnet_datagram_peer *peer, size_t size, int status,
                                       uint64_t tag) {
  protocol_network_e2e_udp_probe_t *probe = (protocol_network_e2e_udp_probe_t *)user;
  (void)datagram;
  (void)peer;
  (void)size;
  (void)tag;
  if (!probe) return;
  ++probe->sent;
  probe->last_send_status = status;
}

static int protocol_network_coap_receiver_open(protocol_network_e2e_fixture_t *fixture) {
  cnet_datagram_config config = CNET_DATAGRAM_CONFIG_INIT;
  int rc;
  config.backend = protocol_network_coap_backend();
  config.host = "127.0.0.1";
  config.port = 0u;
  config.send_capacity = 2u;
  config.request_capacity = 8u;
  config.completion_batch_capacity = 4u;
  config.max_datagram_bytes = sizeof(fixture->udp.last_payload);
  config.receive_buffer_bytes = sizeof(fixture->udp.last_payload);
  config.observer.on_receive = protocol_network_coap_receive;
  config.observer.on_send = protocol_network_coap_sent;
  config.observer.user = &fixture->udp;
  rc = cnet_datagram_init(&fixture->receiver, &config);
  if (rc != SALTS_OK) return rc;
  fixture->receiver_initialized = 1;
  rc = cnet_datagram_port(&fixture->receiver, &fixture->receiver_port);
  if (rc != SALTS_OK || fixture->receiver_port == 0u) return rc != SALTS_OK ? rc : SALTS_EPROTO;
  fixture->next_udp_tag = 1u;
  return cnet_datagram_receive(&fixture->receiver, 8u);
}

static int protocol_network_coap_host_open(protocol_network_e2e_fixture_t *fixture,
                                           const char *coap_module) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  int rc;
  config.module_capacity = 1u;
  config.adapter_provider_capacity = 0u;
  config.resource_provider_capacity = 0u;
  config.protocol_provider_capacity = 1u;
  config.business_provider_capacity = 0u;
  config.transactional_adapter_provider_capacity = 0u;
  config.transactional_resource_provider_capacity = 0u;
  config.schema_capacity = 0u;
  config.operation_capacity = 0u;
  rc = turbo_flow_plugin_host_create(&config, &fixture->host, &error);
  if (rc == SALTS_OK) rc = turbo_flow_plugin_host_load(fixture->host, coap_module, &error);
  if (rc == SALTS_OK)
    rc = turbo_flow_plugin_catalog_snapshot_create(fixture->host, &fixture->catalog, &error);
  return rc;
}

static int protocol_network_coap_yaml(protocol_network_e2e_fixture_t *fixture,
                                      protocol_network_e2e_storage_kind_t storage, char *out,
                                      size_t capacity) {
  static const char yaml[] =
      "version: 1\nadapters:\n  protocol.decode:\n    kind: protocol.decode\n    config:\n"
      "      schema_version: 2\n      protocol_provider: coap\n      protocol_kind: coap\n"
      "      protocol_version: RFC7252\n      source_id: coap.primary\n      max_sessions: 1\n"
      "      max_frame_size: 1024\n      max_pending_claims: 64\n      max_pending_bytes: 65536\n";
  (void)fixture;
  (void)storage;
  if (capacity < sizeof(yaml)) return SALTS_ENOSPC;
  memcpy(out, yaml, sizeof(yaml));
  return SALTS_OK;
}

static int protocol_network_coap_intake_open(protocol_network_e2e_fixture_t *fixture) {
  const char *graph = protocol_network_packet_graph;
  turbo_flow_protocol_network_intake_config_t config =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT;
  turbo_flow_protocol_network_reply_policy_t reply = TURBO_FLOW_PROTOCOL_NETWORK_REPLY_POLICY_INIT;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  int rc;
  if (!flow) return SALTS_ENOMEM;
  rc = turbo_flow_parse_string(flow, graph, strlen(graph));
  if (rc != SALTS_OK) {
    turbo_flow_destroy(flow);
    return rc;
  }
  config.catalog = fixture->catalog;
  config.resolved = fixture->resolved;
  config.downstream_flow = turbo_flow_plugin_generation_flow(fixture->business_generation);
  config.source_stage_name = "wire";
  config.component_runtime = fixture->component_runtime;
  config.provider_resolver = &fixture->provider_resolver;
  config.resource_resolver = &fixture->resource_resolver;
  config.decoder_adapter_name = "protocol.decode";
  config.decoded_source_name = "decoded";
  if (fixture->reply_enabled) {
    reply.point = TURBO_FLOW_PROTOCOL_NETWORK_REPLY_DURABLE_ADMISSION;
    reply.max_encoded_bytes = 1024u;
    config.reply_policy = &reply;
  }
  rc = turbo_flow_protocol_network_intake_create(&config, &flow, &fixture->intake, &error);
  if (rc != SALTS_OK)
    (void)fprintf(stderr, "coap e2e intake create rc=%d path=%s reason=%s\n", rc, error.path,
                  error.message);
  if (flow) turbo_flow_destroy(flow);
  return rc;
}

static int protocol_network_e2e_coap_init_mode(protocol_network_e2e_fixture_t *fixture,
                                               const char *cnet_module, const char *coap_module,
                                               const char *durable_module,
                                               protocol_network_e2e_storage_kind_t storage,
                                               char *turbodb_path, int reply_enabled,
                                               protocol_network_e2e_fixture_t *peer) {
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  char yaml[16384];
  int rc;
  if (!fixture || !cnet_module || !cnet_module[0] || !coap_module || !coap_module[0] ||
      !durable_module || !durable_module[0])
    return SALTS_EINVAL;
  memset(fixture, 0, sizeof(*fixture));
  fixture->reply_enabled = reply_enabled ? 1 : 0;
  rc = protocol_network_e2e_storage_prepare(fixture, storage, turbodb_path);
  if (rc == SALTS_OK) rc = protocol_network_coap_receiver_open(fixture);
  if (rc == SALTS_OK) rc = protocol_network_coap_host_open(fixture, coap_module);
  if (rc == SALTS_OK) {
    if (peer) fixture->deployment = peer->deployment;
    rc = protocol_network_deployment_open(fixture, cnet_module, durable_module, 1);
  }
  if (rc == SALTS_OK) rc = protocol_network_coap_yaml(fixture, storage, yaml, sizeof(yaml));
  if (rc == SALTS_OK)
    rc = turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &fixture->resolved, &error);
  if (rc == SALTS_OK) rc = protocol_network_e2e_business_open(fixture);
  if (rc == SALTS_OK) rc = protocol_network_coap_intake_open(fixture);
  if (rc != SALTS_OK) {
    const int cleanup = protocol_network_e2e_destroy(fixture);
    if (cleanup != SALTS_OK) return cleanup;
  }
  return rc;
}

int protocol_network_e2e_coap_init(protocol_network_e2e_fixture_t *fixture, const char *cnet_module,
                                   const char *coap_module, const char *durable_module,
                                   protocol_network_e2e_storage_kind_t storage,
                                   char *turbodb_path) {
  return protocol_network_e2e_coap_init_mode(fixture, cnet_module, coap_module, durable_module,
                                             storage, turbodb_path, 0, NULL);
}

int protocol_network_e2e_coap_init_reply(protocol_network_e2e_fixture_t *fixture,
                                         const char *cnet_module, const char *coap_module,
                                         const char *durable_module,
                                         protocol_network_e2e_storage_kind_t storage,
                                         char *turbodb_path) {
  return protocol_network_e2e_coap_init_mode(fixture, cnet_module, coap_module, durable_module,
                                             storage, turbodb_path, 1, NULL);
}

int protocol_network_e2e_udp_send_frame(protocol_network_e2e_fixture_t *fixture,
                                        const char *endpoint, const void *data, size_t size) {
  cnet_datagram_peer peer;
  unsigned port = 0u;
  uint64_t tag;
  if (!fixture || !fixture->receiver_initialized || !endpoint || !data || size == 0u)
    return SALTS_EINVAL;
  if (sscanf(endpoint, "udp://127.0.0.1:%u", &port) != 1 || port == 0u || port > UINT16_MAX)
    return SALTS_EINVAL;
  if (fixture->next_udp_tag == 0u || fixture->next_udp_tag == UINT64_MAX) return SALTS_ERANGE;
  memset(&peer, 0, sizeof(peer));
  peer.family = CNET_DATAGRAM_ADDRESS_IPV4;
  peer.address[0] = 127u;
  peer.address[3] = 1u;
  peer.port = (uint16_t)port;
  tag = fixture->next_udp_tag++;
  return cnet_datagram_send(&fixture->receiver, &peer, data, size, tag);
}

int protocol_network_e2e_coap_init_peer(protocol_network_e2e_fixture_t *fixture,
                                        protocol_network_e2e_fixture_t *peer,
                                        const char *cnet_module, const char *coap_module,
                                        const char *durable_module,
                                        protocol_network_e2e_storage_kind_t storage, char *path) {
  return protocol_network_e2e_coap_init_mode(fixture, cnet_module, coap_module, durable_module,
                                             storage, path, 0, peer);
}
