#include "protocol_network_e2e_fixture.h"
#include "protocol_network_deployment.h"
#include "protocol_network_graphs.h"

#include <salts/clock.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int protocol_network_e2e_storage_prepare(protocol_network_e2e_fixture_t *fixture,
                                         protocol_network_e2e_storage_kind_t storage, char *path) {
  if (!fixture) return SALTS_EINVAL;
  fixture->storage = storage;
  fixture->turbodb_path = path;
  if (storage == PROTOCOL_NETWORK_E2E_STORAGE_MEMORY) return path ? SALTS_EINVAL : SALTS_OK;
  if (storage == PROTOCOL_NETWORK_E2E_STORAGE_TURBODB) return path ? SALTS_OK : SALTS_ENOMEM;
  return SALTS_EINVAL;
}
void protocol_network_e2e_storage_cleanup(protocol_network_e2e_fixture_t *fixture) {
  if (fixture->turbodb_path) {
    (void)remove(fixture->turbodb_path);
    free(fixture->turbodb_path);
    fixture->turbodb_path = NULL;
  }
}

static native_io_backend_kind protocol_network_e2e_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__APPLE__)
  return NATIVE_IO_BACKEND_KQUEUE;
#else
  return NATIVE_IO_BACKEND_EPOLL;
#endif
}

static void protocol_network_e2e_udp_receive(void *user, cnet_datagram *datagram,
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

static void protocol_network_e2e_udp_send_terminal(void *user, cnet_datagram *datagram,
                                                   const cnet_datagram_peer *peer, size_t size,
                                                   int status, uint64_t tag) {
  (void)user;
  (void)datagram;
  (void)peer;
  (void)size;
  (void)status;
  (void)tag;
}

static void protocol_network_e2e_tcp_state(void *user, cnet_connection connection,
                                           cnet_connection_state state, const cnet_error *error) {
  protocol_network_e2e_tcp_probe_t *probe = (protocol_network_e2e_tcp_probe_t *)user;
  (void)connection;
  if (!probe) return;
  if (state == CNET_CONNECTION_CONNECTED) ++probe->connected;
  if (state == CNET_CONNECTION_CLOSED || state == CNET_CONNECTION_FAILED) ++probe->terminal;
  if (state == CNET_CONNECTION_FAILED || error) probe->failed = 1;
}

static void protocol_network_e2e_tcp_receive(void *user, cnet_connection connection,
                                             const cnet_receive_view *view) {
  protocol_network_e2e_tcp_probe_t *probe = (protocol_network_e2e_tcp_probe_t *)user;
  (void)connection;
  if (!probe || !view || !view->data || view->size == 0u ||
      view->size > sizeof(probe->last_payload))
    return;
  memcpy(probe->last_payload, view->data, view->size);
  probe->last_payload_size = view->size;
  probe->bytes += view->size;
  ++probe->received;
}

static void protocol_network_e2e_tcp_sent(void *user, cnet_connection connection, size_t size) {
  protocol_network_e2e_tcp_probe_t *probe = (protocol_network_e2e_tcp_probe_t *)user;
  (void)connection;
  if (!probe) return;
  if (size == 0u) probe->failed = 1;
  ++probe->sent;
}

static void protocol_network_e2e_business_stage(void *ctx, const char *stage_name,
                                                const char *adapter_name,
                                                const turbo_flow_msg_t *message,
                                                uint64_t duration_ns, int status) {
  protocol_network_e2e_business_probe_t *probe = (protocol_network_e2e_business_probe_t *)ctx;
  (void)adapter_name;
  (void)message;
  (void)duration_ns;
  if (!probe || !stage_name || strcmp(stage_name, "output") != 0) return;
  ++probe->stage_completions;
  if (status != SALTS_OK) ++probe->failed_completions;
}

static int protocol_network_e2e_receiver_open(protocol_network_e2e_fixture_t *fixture) {
  cnet_datagram_config config = CNET_DATAGRAM_CONFIG_INIT;
  int rc;
  config.backend = protocol_network_e2e_backend();
  config.host = "127.0.0.1";
  config.port = 0u;
  config.send_capacity = 1u;
  config.request_capacity = 8u;
  config.completion_batch_capacity = 4u;
  config.max_datagram_bytes = sizeof(fixture->udp.last_payload);
  config.receive_buffer_bytes = sizeof(fixture->udp.last_payload);
  config.observer.on_receive = protocol_network_e2e_udp_receive;
  config.observer.on_send = protocol_network_e2e_udp_send_terminal;
  config.observer.user = &fixture->udp;
  rc = cnet_datagram_init(&fixture->receiver, &config);
  if (rc != SALTS_OK) return rc;
  fixture->receiver_initialized = 1;
  rc = cnet_datagram_port(&fixture->receiver, &fixture->receiver_port);
  if (rc != SALTS_OK || fixture->receiver_port == 0u) return rc != SALTS_OK ? rc : SALTS_EPROTO;
  return cnet_datagram_receive(&fixture->receiver, 8u);
}

static int protocol_network_e2e_client_open(protocol_network_e2e_fixture_t *fixture) {
  cnet_client_config config;
  memset(&config, 0, sizeof(config));
  config.backend = protocol_network_e2e_backend();
  config.connection_capacity = 2u;
  config.command_capacity = 8u;
  config.request_capacity = 8u;
  config.completion_batch_capacity = 4u;
  config.event_capacity = 8u;
  config.max_send_bytes = 4096u;
  config.receive_buffer_bytes = 1024u;
  if (cnet_client_init(&fixture->client, &config) != SALTS_OK) return SALTS_EIO;
  fixture->client_initialized = 1;
  return SALTS_OK;
}

static int protocol_network_e2e_host_open(protocol_network_e2e_fixture_t *fixture,
                                          const char *jtt808_module) {
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
  if (rc == SALTS_OK) rc = turbo_flow_plugin_host_load(fixture->host, jtt808_module, &error);
  if (rc == SALTS_OK)
    rc = turbo_flow_plugin_catalog_snapshot_create(fixture->host, &fixture->catalog, &error);
  return rc;
}

static int protocol_network_e2e_intake_open(protocol_network_e2e_fixture_t *fixture) {
  const char *graph = protocol_network_listener_graph;
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
  config.downstream_flow = turbo_flow_plugin_generation_flow(fixture->business_generation);
  config.source_stage_name = "wire";
  config.component_runtime = fixture->component_runtime;
  config.provider_resolver = &fixture->provider_resolver;
  config.resource_resolver = &fixture->resource_resolver;
  config.decoder_stage_name = "decode";
  config.decoded_source_name = "decoded";
  if (fixture->reply_enabled) {
    reply.point = TURBO_FLOW_PROTOCOL_NETWORK_REPLY_DURABLE_ADMISSION;
    reply.max_encoded_bytes = 1024u;
    config.reply_policy = &reply;
  }
  rc = turbo_flow_protocol_network_intake_create(&config, &flow, &fixture->intake, &error);
  if (rc != SALTS_OK)
    (void)fprintf(stderr, "jtt808 e2e intake create rc=%d path=%s reason=%s\n", rc, error.path,
                  error.message);
  if (flow) turbo_flow_destroy(flow);
  return rc;
}

int protocol_network_e2e_business_open(protocol_network_e2e_fixture_t *fixture) {
  const char *graph = fixture->storage == PROTOCOL_NETWORK_E2E_STORAGE_TURBODB
                          ? protocol_network_database_business_graph
                          : protocol_network_memory_business_graph;
  turbo_flow_plugin_generation_config_t generation_config =
      TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_observer_ops_t observer;
  turbo_flow_t *flow = turbo_flow_create();
  int rc;
  if (!flow) return SALTS_ENOMEM;
  rc = turbo_flow_parse_string(flow, graph, strlen(graph));
  if (rc != SALTS_OK) goto fail;
  memset(&observer, 0, sizeof(observer));
  observer.size = sizeof(observer);
  observer.stage_complete = protocol_network_e2e_business_stage;
  rc = turbo_flow_set_observer(flow, &observer, &fixture->business);
  if (rc != SALTS_OK) goto fail;
  generation_config.owner_capacity = 2u;
  generation_config.component_runtime = fixture->component_runtime;
  generation_config.provider_resolver = &fixture->provider_resolver;
  generation_config.resource_resolver = &fixture->resource_resolver;
  rc = turbo_flow_plugin_generation_create(fixture->catalog, &flow,
                                           &generation_config, NULL, &fixture->business_generation,
                                           &fixture->business_cleanup, &error);
  if (rc != SALTS_OK) {
    (void)fprintf(stderr, "jtt808 e2e business generation rc=%d path=%s reason=%s\n", rc,
                  error.path, error.message);
    goto fail;
  }
  rc = turbo_flow_start(turbo_flow_plugin_generation_flow(fixture->business_generation));
  if (rc != SALTS_OK) return rc;
  return SALTS_OK;

fail:
  if (flow) turbo_flow_destroy(flow);
  return rc;
}

static int protocol_network_e2e_jtt808_init_mode(protocol_network_e2e_fixture_t *fixture,
                                                 const char *cnet_module, const char *jtt808_module,
                                                 const char *durable_module,
                                                 protocol_network_e2e_storage_kind_t storage,
                                                 char *turbodb_path, int reply_enabled) {

  int rc;
  if (!fixture || !cnet_module || !cnet_module[0] || !jtt808_module || !jtt808_module[0] ||
      !durable_module || !durable_module[0])
    return SALTS_EINVAL;
  memset(fixture, 0, sizeof(*fixture));
  fixture->reply_enabled = reply_enabled ? 1 : 0;
  rc = protocol_network_e2e_storage_prepare(fixture, storage, turbodb_path);
  if (rc == SALTS_OK) rc = protocol_network_e2e_receiver_open(fixture);
  if (rc == SALTS_OK) rc = protocol_network_e2e_client_open(fixture);
  if (rc == SALTS_OK) rc = protocol_network_e2e_host_open(fixture, jtt808_module);
  if (rc == SALTS_OK)
    rc = protocol_network_deployment_open(fixture, cnet_module, durable_module, 0);
  if (rc == SALTS_OK) rc = protocol_network_e2e_business_open(fixture);
  if (rc == SALTS_OK) rc = protocol_network_e2e_intake_open(fixture);
  if (rc != SALTS_OK) {
    const int cleanup = protocol_network_e2e_destroy(fixture);
    if (cleanup != SALTS_OK) return cleanup;
  }
  return rc;
}

int protocol_network_e2e_jtt808_init(protocol_network_e2e_fixture_t *fixture,
                                     const char *cnet_module, const char *jtt808_module,
                                     const char *durable_module,
                                     protocol_network_e2e_storage_kind_t storage,
                                     char *turbodb_path) {
  return protocol_network_e2e_jtt808_init_mode(fixture, cnet_module, jtt808_module, durable_module,
                                               storage, turbodb_path, 0);
}

int protocol_network_e2e_jtt808_init_reply(protocol_network_e2e_fixture_t *fixture,
                                           const char *cnet_module, const char *jtt808_module,
                                           const char *durable_module,
                                           protocol_network_e2e_storage_kind_t storage,
                                           char *turbodb_path) {
  return protocol_network_e2e_jtt808_init_mode(fixture, cnet_module, jtt808_module, durable_module,
                                               storage, turbodb_path, 1);
}

int protocol_network_e2e_start(protocol_network_e2e_fixture_t *fixture,
                               turbo_flow_protocol_network_intake_snapshot_t *snapshot) {
  int rc;
  if (!fixture || !fixture->intake || !snapshot) return SALTS_EINVAL;
  rc = turbo_flow_protocol_network_intake_start(fixture->intake);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_protocol_network_intake_snapshot(fixture->intake, snapshot);
}

int protocol_network_e2e_poll(protocol_network_e2e_fixture_t *fixture, uint32_t timeout_ms,
                              turbo_flow_protocol_network_intake_snapshot_t *snapshot) {
  size_t events = 0u;
  int rc;
  if (!fixture || !fixture->intake || !snapshot) return SALTS_EINVAL;
  if (fixture->client_initialized) {
    rc = cnet_client_poll(&fixture->client, 0u, &events);
    if (rc != SALTS_OK) return rc;
  }
  if (fixture->receiver_initialized) {
    rc = cnet_datagram_poll(&fixture->receiver, 0u, &events);
    if (rc != SALTS_OK) return rc;
  }
  return turbo_flow_protocol_network_intake_poll(fixture->intake, timeout_ms, snapshot);
}

int protocol_network_e2e_tcp_connect(protocol_network_e2e_fixture_t *fixture, const char *endpoint,
                                     uint32_t timeout_ms) {
  cnet_connect_options options;
  turbo_flow_protocol_network_intake_snapshot_t snapshot =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
  size_t connected_before;
  uint64_t deadline;
  int rc;
  if (!fixture || !fixture->client_initialized || !endpoint || !endpoint[0]) return SALTS_EINVAL;
  memset(&options, 0, sizeof(options));
  options.uri = endpoint;
  options.observer.on_state = protocol_network_e2e_tcp_state;
  options.observer.on_receive = protocol_network_e2e_tcp_receive;
  options.observer.on_send = protocol_network_e2e_tcp_sent;
  options.observer.user = &fixture->tcp;
  connected_before = fixture->tcp.connected;
  memset(&fixture->connection, 0, sizeof(fixture->connection));
  rc = cnet_connect(&fixture->client, &options, &fixture->connection);
  if (rc != SALTS_OK) return rc;
  deadline = cmeta_monotonic_ms() + timeout_ms;
  while (fixture->tcp.connected == connected_before && fixture->tcp.failed == 0 &&
         cmeta_monotonic_ms() < deadline) {
    rc = protocol_network_e2e_poll(fixture, 1u, &snapshot);
    if (rc != SALTS_OK) return rc;
  }
  if (fixture->tcp.failed) return SALTS_EIO;
  return fixture->tcp.connected > connected_before ? SALTS_OK : SALTS_ETIMEDOUT;
}

int protocol_network_e2e_tcp_send(protocol_network_e2e_fixture_t *fixture, const void *data,
                                  size_t size) {
  mem_buffer_t *payload;
  mem_slice_t segment = {0};
  int rc;
  if (!fixture || !fixture->client_initialized || !data || size == 0u) return SALTS_EINVAL;

  payload = mem_get_buffer(mem_global(), size);
  if (!payload) return SALTS_ENOMEM;
  memcpy(mem_buffer_data(payload), data, size);
  mem_set_used(payload, size);
  segment = mem_slice(payload, 0u, size);
  if (!segment.buffer || segment.length != size) {
    mem_slice_release(&segment);
    mem_buffer_release(payload);
    return SALTS_EPROTO;
  }
  rc = cnet_send_slicev(&fixture->client, fixture->connection, &segment, 1u);
  mem_slice_release(&segment);
  mem_buffer_release(payload);
  return rc;
}

int protocol_network_e2e_tcp_close(protocol_network_e2e_fixture_t *fixture, uint32_t timeout_ms) {
  turbo_flow_protocol_network_intake_snapshot_t snapshot =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
  const size_t terminal_before = fixture ? fixture->tcp.terminal : 0u;
  uint64_t deadline;
  int rc;
  if (!fixture || !fixture->client_initialized) return SALTS_EINVAL;
  rc = cnet_close(&fixture->client, fixture->connection);
  if (rc != SALTS_OK && rc != SALTS_EALREADY) return rc;
  deadline = cmeta_monotonic_ms() + timeout_ms;
  while (fixture->tcp.terminal == terminal_before && cmeta_monotonic_ms() < deadline) {
    rc = protocol_network_e2e_poll(fixture, 1u, &snapshot);
    if (rc != SALTS_OK) return rc;
  }
  return fixture->tcp.terminal > terminal_before ? SALTS_OK : SALTS_ETIMEDOUT;
}

int protocol_network_e2e_business_request_and_drive(protocol_network_e2e_fixture_t *fixture,
                                                    uint32_t timeout_ms) {
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  size_t events = 0u;
  const size_t received_before = fixture ? fixture->udp.received : 0u;
  const size_t completed_before = fixture ? fixture->business.stage_completions : 0u;
  uint64_t deadline;
  int rc;
  if (!fixture || !fixture->business_generation) return SALTS_EINVAL;
  deadline = cmeta_monotonic_ms() + timeout_ms;
  while (cmeta_monotonic_ms() < deadline) {
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    rc = turbo_flow_plugin_generation_poll(fixture->business_generation, 1u, &error);
    if (rc != SALTS_OK) return rc;
    if (fixture->receiver_initialized) {
      rc = cnet_datagram_poll(&fixture->receiver, 0u, &events);
      if (rc != SALTS_OK) return rc;
    }
    if (fixture->business.stage_completions > completed_before &&
        fixture->udp.received > received_before)
      return SALTS_OK;
    cmeta_sleep_ms(1u);
  }
  return SALTS_ETIMEDOUT;
}

int protocol_network_e2e_destroy(protocol_network_e2e_fixture_t *fixture) {
  turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  int rc;
  if (!fixture) return SALTS_EINVAL;
  if (fixture->client_initialized) {
    rc = cnet_client_stop(&fixture->client, 1000u);
    if (rc != SALTS_OK && rc != SALTS_EALREADY) return rc;
    rc = cnet_client_destroy(&fixture->client);
    if (rc != SALTS_OK) return rc;
    fixture->client_initialized = 0;
  }
  if (fixture->intake) {
    rc = turbo_flow_protocol_network_intake_stop(fixture->intake, 1000u);
    if (rc != SALTS_OK && rc != SALTS_EALREADY) return rc;
    rc = turbo_flow_protocol_network_intake_destroy(fixture->intake);
    if (rc != SALTS_OK) return rc;
    fixture->intake = NULL;
  }
  if (fixture->business_generation) {
    rc = turbo_flow_plugin_generation_destroy(fixture->business_generation, 1000u, &config_error);
    if (rc != SALTS_OK) return rc;
    fixture->business_generation = NULL;
  }
  if (fixture->business_cleanup) {
    rc = turbo_flow_plugin_generation_destroy(fixture->business_cleanup, 1000u, &config_error);
    if (rc != SALTS_OK) return rc;
    fixture->business_cleanup = NULL;
  }
  rc = protocol_network_deployment_close(fixture);
  if (rc != SALTS_OK) return rc;
  if (fixture->catalog) {
    turbo_flow_plugin_catalog_snapshot_destroy(fixture->catalog);
    fixture->catalog = NULL;
  }
  if (fixture->receiver_initialized) {
    rc = cnet_datagram_stop(&fixture->receiver, 1000u);
    if (rc != SALTS_OK && rc != SALTS_EALREADY) return rc;
    rc = cnet_datagram_destroy(&fixture->receiver);
    if (rc != SALTS_OK) return rc;
    fixture->receiver_initialized = 0;
  }
  if (fixture->host) {
    rc = turbo_flow_plugin_host_destroy(fixture->host, 1000u, &plugin_error);
    if (rc != SALTS_OK) return rc;
    fixture->host = NULL;
  }
  protocol_network_e2e_storage_cleanup(fixture);
  return SALTS_OK;
}

size_t protocol_network_e2e_jtt808_frame(uint8_t *out, size_t capacity) {
  static const uint8_t unescaped[] = {0x02u, 0x00u, 0x40u, 0x00u, 0x01u, 0x00u, 0x00u, 0x00u, 0x00u,
                                      0x00u, 0x00u, 0x00u, 0x01u, 0x23u, 0x45u, 0x00u, 0x01u};
  uint8_t checksum = 0u;
  size_t written = 0u;
  if (!out || capacity < sizeof(unescaped) * 2u + 4u) return 0u;
  out[written++] = 0x7eu;
  for (size_t i = 0u; i < sizeof(unescaped); ++i) {
    checksum ^= unescaped[i];
    if (unescaped[i] == 0x7du || unescaped[i] == 0x7eu) return 0u;
    out[written++] = unescaped[i];
  }
  if (checksum == 0x7du) {
    out[written++] = 0x7du;
    out[written++] = 0x01u;
  } else if (checksum == 0x7eu) {
    out[written++] = 0x7du;
    out[written++] = 0x02u;
  } else {
    out[written++] = checksum;
  }
  out[written++] = 0x7eu;
  return written;
}