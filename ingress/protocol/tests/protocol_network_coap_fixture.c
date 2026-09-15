#include "protocol_network_e2e_fixture.h"

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

static const char *protocol_network_coap_backend_name(void) {
#if defined(_WIN32)
  return "iocp";
#elif defined(__APPLE__)
  return "kqueue";
#else
  return "epoll";
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

static void protocol_network_coap_business_stage(void *ctx, const char *stage_name,
                                                 const char *adapter_name,
                                                 const turbo_flow_msg_t *message,
                                                 uint64_t duration_ns, int status) {
  protocol_network_e2e_business_probe_t *probe =
      (protocol_network_e2e_business_probe_t *)ctx;
  (void)stage_name;
  (void)adapter_name;
  (void)message;
  (void)duration_ns;
  if (!probe) return;
  ++probe->stage_completions;
  if (status != SALTS_OK) ++probe->failed_completions;
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
                                           const char *cnet_module, const char *coap_module) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  int rc;
  config.module_capacity = 2u;
  config.adapter_provider_capacity = 0u;
  config.resource_provider_capacity = 0u;
  config.protocol_provider_capacity = 1u;
  config.business_provider_capacity = 0u;
  config.transactional_adapter_provider_capacity = 6u;
  config.transactional_resource_provider_capacity = 0u;
  config.schema_capacity = 0u;
  config.operation_capacity = 0u;
  rc = turbo_flow_plugin_host_create(&config, &fixture->host, &error);
  if (rc == SALTS_OK) rc = turbo_flow_plugin_host_load(fixture->host, cnet_module, &error);
  if (rc == SALTS_OK) rc = turbo_flow_plugin_host_load(fixture->host, coap_module, &error);
  if (rc == SALTS_OK)
    rc = turbo_flow_plugin_catalog_snapshot_create(fixture->host, &fixture->catalog, &error);
  return rc;
}

static int protocol_network_coap_yaml(protocol_network_e2e_fixture_t *fixture, char *out,
                                      size_t capacity) {
  const char *backend = protocol_network_coap_backend_name();
  int written;
  if (!fixture || !out || capacity == 0u || fixture->receiver_port == 0u) return SALTS_EINVAL;
  written = snprintf(
      out, capacity,
      "version: 1\n"
      "adapters:\n"
      "  udp.input:\n"
      "    kind: cnet.packet_source\n"
      "    config:\n"
      "      schema_version: 1\n"
      "      backend: %s\n"
      "      packet_mode: udp\n"
      "      bind_host: \"127.0.0.1\"\n"
      "      bind_port: 0\n"
      "      datagram_send_capacity: 4\n"
      "      request_capacity: 8\n"
      "      completion_batch_capacity: 4\n"
      "      max_datagram_bytes: 1024\n"
      "      receive_buffer_bytes: 1024\n"
      "      reuse_port: false\n"
      "      session_capacity: 1\n"
      "      kcp_mtu: 0\n"
      "      kcp_send_window: 0\n"
      "      kcp_receive_window: 0\n"
      "      kcp_interval_ms: 0\n"
      "      kcp_fast_resend: 0\n"
      "      kcp_no_congestion_window: false\n"
      "      kcp_stream_mode: false\n"
      "      kcp_send_segment_capacity: 0\n"
      "      kcp_max_message_bytes: 0\n"
      "      security_mode: none\n"
      "      psk_hex: \"\"\n"
      "      handshake_retry_ms: 0\n"
      "      fec_backend: none\n"
      "      fec_data_shards: 0\n"
      "      fec_parity_shards: 0\n"
      "      fec_max_payload_bytes: 0\n"
      "      fec_receive_group_count: 0\n"
      "      queue_capacity: 8\n"
      "      max_message_bytes: 1024\n"
      "      scheduler_capacity: 8\n"
      "      scheduler_max_steps_per_poll: 32\n"
      "      first_message_id: 1\n"
      "      initial_demand: 8\n"
      "      stop_timeout_ms: 1000\n"
      "  protocol.store:\n"
      "    kind: protocol.intake\n"
      "    config:\n"
      "      schema_version: 1\n"
      "      protocol_provider: coap\n"
      "      protocol_kind: coap\n"
      "      protocol_version: RFC7252\n"
      "      source_id: coap.primary\n"
      "      max_sessions: 1\n"
      "      max_frame_size: 1024\n"
      "      max_pending_claims: 64\n"
      "      max_pending_bytes: 65536\n"
      "  udp.output:\n"
      "    kind: cnet.datagram_sink\n"
      "    config:\n"
      "      schema_version: 1\n"
      "      backend: %s\n"
      "      bind_host: \"127.0.0.1\"\n"
      "      bind_port: 0\n"
      "      datagram_send_capacity: 4\n"
      "      request_capacity: 8\n"
      "      completion_batch_capacity: 4\n"
      "      max_datagram_bytes: 4096\n"
      "      receive_buffer_bytes: 4096\n"
      "      reuse_port: false\n"
      "      peer_host: \"127.0.0.1\"\n"
      "      peer_port: %u\n"
      "      peer_scope_id: 0\n"
      "      max_message_bytes: 4096\n"
      "      actor_command_capacity: 8\n"
      "      actor_max_steps_per_poll: 32\n"
      "      stop_timeout_ms: 1000\n",
      backend, backend, (unsigned)fixture->receiver_port);
  return written < 0 || (size_t)written >= capacity ? SALTS_ENOSPC : SALTS_OK;
}

static int protocol_network_coap_inbox_open(protocol_network_e2e_fixture_t *fixture) {
  turbo_flow_inbox_memory_config_t config = turbo_flow_inbox_memory_config_default();
  fixture->inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
  config.max_records = 8u;
  config.max_total_bytes = 32768u;
  config.max_record_bytes = 4096u;
  config.max_claims = 8u;
  return turbo_flow_inbox_memory_create(&config, &fixture->inbox);
}

static int protocol_network_coap_intake_open(protocol_network_e2e_fixture_t *fixture) {
  static const char graph[] = "source wire adapter udp.input\n"
                              "stage durable adapter protocol.store\n"
                              "stage main {\n"
                              "  wire -> durable\n"
                              "}\n";
  turbo_flow_protocol_network_intake_config_t config =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  int rc;
  if (!flow) return SALTS_ENOMEM;
  rc = turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u);
  if (rc != SALTS_OK) {
    turbo_flow_destroy(flow);
    return rc;
  }
  config.catalog = fixture->catalog;
  config.resolved = fixture->resolved;
  config.inbox = &fixture->inbox;
  config.source_adapter_name = "udp.input";
  config.intake_adapter_name = "protocol.store";
  rc = turbo_flow_protocol_network_intake_create(&config, &flow, &fixture->intake, &error);
  if (flow) turbo_flow_destroy(flow);
  return rc;
}

static int protocol_network_coap_business_open(protocol_network_e2e_fixture_t *fixture) {
  static const char graph[] = "source inbox\n"
                              "stage output adapter udp.output\n"
                              "stage main {\n"
                              "  inbox -> output\n"
                              "}\n";
  turbo_flow_plugin_generation_config_t generation_config =
      TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_observer_ops_t observer;
  turbo_flow_inbox_source_config_t source_config = TURBO_FLOW_INBOX_SOURCE_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  int rc;
  if (!flow) return SALTS_ENOMEM;
  rc = turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u);
  if (rc != SALTS_OK) goto fail;
  memset(&observer, 0, sizeof(observer));
  observer.size = sizeof(observer);
  observer.stage_complete = protocol_network_coap_business_stage;
  rc = turbo_flow_set_observer(flow, &observer, &fixture->business);
  if (rc != SALTS_OK) goto fail;
  generation_config.owner_capacity = 1u;
  rc = turbo_flow_plugin_generation_create(fixture->catalog, fixture->resolved, &flow,
                                           &generation_config, NULL,
                                           &fixture->business_generation,
                                           &fixture->business_cleanup, &error);
  if (rc != SALTS_OK) goto fail;
  rc = turbo_flow_start(turbo_flow_plugin_generation_flow(fixture->business_generation));
  if (rc != SALTS_OK) return rc;
  source_config.inbox = &fixture->inbox;
  source_config.flow = turbo_flow_plugin_generation_flow(fixture->business_generation);
  source_config.graph_source_name = "inbox";
  source_config.max_message_bytes = 4096u;
  return turbo_flow_inbox_source_create(&source_config, &fixture->inbox_source);

fail:
  if (flow) turbo_flow_destroy(flow);
  return rc;
}

int protocol_network_e2e_coap_init(protocol_network_e2e_fixture_t *fixture,
                                  const char *cnet_module, const char *coap_module) {
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  char yaml[16384];
  int rc;
  if (!fixture || !cnet_module || !cnet_module[0] || !coap_module || !coap_module[0])
    return SALTS_EINVAL;
  memset(fixture, 0, sizeof(*fixture));
  fixture->inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
  rc = protocol_network_coap_receiver_open(fixture);
  if (rc == SALTS_OK) rc = protocol_network_coap_host_open(fixture, cnet_module, coap_module);
  if (rc == SALTS_OK) rc = protocol_network_coap_yaml(fixture, yaml, sizeof(yaml));
  if (rc == SALTS_OK)
    rc = turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &fixture->resolved, &error);
  if (rc == SALTS_OK) rc = protocol_network_coap_inbox_open(fixture);
  if (rc == SALTS_OK) rc = protocol_network_coap_intake_open(fixture);
  if (rc == SALTS_OK) rc = protocol_network_coap_business_open(fixture);
  if (rc != SALTS_OK) protocol_network_e2e_destroy(fixture);
  return rc;
}

int protocol_network_e2e_udp_send(protocol_network_e2e_fixture_t *fixture, const char *endpoint,
                                  const void *data, size_t size) {
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
