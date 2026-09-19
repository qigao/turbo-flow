#include "protocol_network_e2e_fixture.h"
#include "tinytest.h"

#include <salts/clock.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>


#if defined(FLOW_TURBODB_PLUGIN_MODULE)
static const char protocol_network_e2e_meta_ddl[] =
    "CREATE TABLE protocol_e2e_inbox_meta_v2 ("
    "singleton_id integer primary key not null, schema_magic text not null, "
    "schema_version integer not null, generation bigint not null, owner_state integer not null, "
    "next_record_id bigint not null, next_claim_token bigint not null, max_records bigint not "
    "null, max_total_bytes bigint not null, max_record_bytes bigint not null, max_claims bigint "
    "not null, records bigint not null, history_records bigint not null, pending_records bigint "
    "not null, failed_records bigint not null, in_flight_claims bigint not null, retained_bytes "
    "bigint not null, admitted bigint not null, completed bigint not null, failed bigint not null, "
    "retried bigint not null, discarded bigint not null)";

static const char protocol_network_e2e_records_ddl[] =
    "CREATE TABLE protocol_e2e_inbox_records_v2 ("
    "record_id bigint primary key not null, phase integer not null, claim_generation bigint not "
    "null, claim_token bigint not null, failure_status integer not null, failure_kind integer not "
    "null, terminal_kind integer not null, envelope_schema text not null, envelope_schema_version "
    "integer not null, source_id bytea not null, admission_id bytea not null, source_sequence_be "
    "bytea not null, timestamp_ns_be bytea not null, message_type bigint not null, message_flags "
    "bigint not null, content_domain integer not null, content_profile integer not null, "
    "content_encoding integer not null, content_flags bigint not null, content_schema_version "
    "bigint not null, content_media_type text not null, content_schema_name text not null, "
    "content_type_name text not null, content_identity text not null, correlation bytea not null, "
    "payload bytea not null, retained_bytes bigint not null)";

static const char protocol_network_e2e_admission_index_ddl[] =
    "CREATE UNIQUE INDEX protocol_e2e_inbox_records_v2_admission ON "
    "protocol_e2e_inbox_records_v2(source_id, admission_id)";

static const char protocol_network_e2e_phase_index_ddl[] =
    "CREATE INDEX protocol_e2e_inbox_records_v2_phase ON "
    "protocol_e2e_inbox_records_v2(phase, record_id)";

static const char protocol_network_e2e_meta_row[] =
    "INSERT INTO protocol_e2e_inbox_meta_v2 VALUES "
    "(1, 'turbo-flow.turbodb.inbox', 2, 0, 0, 1, 1, 8, 32768, 4096, 1, "
    "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)";

static int protocol_network_e2e_sql(orm_connection_t *db, const char *text) {
  orm_error_t error;
  orm_query_t *query = NULL;
  orm_result_t *result = NULL;
  int rc = SALTS_EIO;
  if (!db || !text) return SALTS_EINVAL;
  orm_error_init(&error);
  if (orm_raw(db, orm_view(text), &query, &error) != ORM_STATUS_OK) goto done;
  if (orm_query_execute(query, &result, &error) != ORM_STATUS_OK) goto done;
  rc = SALTS_OK;
done:
  if (result) orm_result_destroy(result);
  if (query) orm_query_destroy(query);
  return rc;
}
#endif

int protocol_network_e2e_storage_prepare(protocol_network_e2e_fixture_t *fixture,
                                         protocol_network_e2e_storage_kind_t storage) {
  if (!fixture) return SALTS_EINVAL;
  if (storage == PROTOCOL_NETWORK_E2E_STORAGE_MEMORY) return SALTS_OK;
#if defined(FLOW_TURBODB_PLUGIN_MODULE)
  if (storage == PROTOCOL_NETWORK_E2E_STORAGE_TURBODB) {
    orm_error_t error;
    int rc;
    fixture->turbodb_path = tt_make_temp_file("turbo-flow-protocol-e2e", ".sqlite3");
    if (!fixture->turbodb_path) return SALTS_ENOMEM;
    orm_config(&fixture->turbodb_database);
    fixture->turbodb_filename.keyword = orm_view("filename");
    fixture->turbodb_filename.value = orm_view(fixture->turbodb_path);
    fixture->turbodb_database.driver = orm_view("sqlite");
    fixture->turbodb_database.options = &fixture->turbodb_filename;
    fixture->turbodb_database.option_count = 1u;
    orm_error_init(&error);
    if (orm_connect(&fixture->turbodb_database, &fixture->turbodb_db, &error) != ORM_STATUS_OK) {
      protocol_network_e2e_storage_cleanup(fixture);
      return SALTS_EIO;
    }
    rc = protocol_network_e2e_sql(fixture->turbodb_db, protocol_network_e2e_meta_ddl);
    if (rc == SALTS_OK)
      rc = protocol_network_e2e_sql(fixture->turbodb_db, protocol_network_e2e_records_ddl);
    if (rc == SALTS_OK)
      rc = protocol_network_e2e_sql(fixture->turbodb_db,
                                    protocol_network_e2e_admission_index_ddl);
    if (rc == SALTS_OK)
      rc = protocol_network_e2e_sql(fixture->turbodb_db, protocol_network_e2e_phase_index_ddl);
    if (rc == SALTS_OK)
      rc = protocol_network_e2e_sql(fixture->turbodb_db, protocol_network_e2e_meta_row);
    orm_disconnect(fixture->turbodb_db);
    fixture->turbodb_db = NULL;
    if (rc != SALTS_OK) protocol_network_e2e_storage_cleanup(fixture);
    return rc;
  }
#else
  if (storage == PROTOCOL_NETWORK_E2E_STORAGE_TURBODB) return SALTS_ENOTSUP;
#endif
  return SALTS_EINVAL;
}

int protocol_network_e2e_storage_rewrite_yaml(protocol_network_e2e_fixture_t *fixture,
                                              protocol_network_e2e_storage_kind_t storage,
                                              char *yaml, size_t capacity) {
  char *channels;
  size_t prefix;
  int written;
  if (!fixture || !yaml || capacity == 0u) return SALTS_EINVAL;
  if (storage == PROTOCOL_NETWORK_E2E_STORAGE_MEMORY) return SALTS_OK;
#if defined(FLOW_TURBODB_PLUGIN_MODULE)
  if (storage != PROTOCOL_NETWORK_E2E_STORAGE_TURBODB || !fixture->turbodb_path)
    return SALTS_EINVAL;
  channels = strstr(yaml, "channels:\n");
  if (!channels) return SALTS_EPROTO;
  prefix = (size_t)(channels - yaml);
  if (prefix >= capacity) return SALTS_ENOSPC;
  written = snprintf(
      channels, capacity - prefix,
      "channels:\n"
      "  intake.store:\n"
      "    kind: flow.durable.turbodb\n"
      "    config:\n"
      "      schema_version: 2\n"
      "      identity_mode: stable_required\n"
      "      filename: '%s'\n"
      "      namespace: protocol_e2e\n"
      "      max_message_bytes: 4096\n"
      "      max_records: 8\n"
      "      max_total_bytes: 32768\n"
      "      max_record_bytes: 4096\n"
      "      max_claims: 1\n"
      "      connection_count: 4\n"
      "      open_mode: exclusive\n"
      "      expected_generation: 0\n",
      fixture->turbodb_path);
  return written < 0 || (size_t)written >= capacity - prefix ? SALTS_ENOSPC : SALTS_OK;
#else
  (void)channels;
  (void)prefix;
  (void)written;
  return storage == PROTOCOL_NETWORK_E2E_STORAGE_TURBODB ? SALTS_ENOTSUP : SALTS_EINVAL;
#endif
}

void protocol_network_e2e_storage_cleanup(protocol_network_e2e_fixture_t *fixture) {
#if defined(FLOW_TURBODB_PLUGIN_MODULE)
  if (!fixture) return;
  if (fixture->turbodb_db) {
    orm_disconnect(fixture->turbodb_db);
    fixture->turbodb_db = NULL;
  }
  if (fixture->turbodb_path) {
    (void)tt_remove_file(fixture->turbodb_path);
    free(fixture->turbodb_path);
    fixture->turbodb_path = NULL;
  }
#else
  (void)fixture;
#endif
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

static const char *protocol_network_e2e_backend_name(void) {
#if defined(_WIN32)
  return "iocp";
#elif defined(__APPLE__)
  return "kqueue";
#else
  return "epoll";
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
  (void)user;
  (void)connection;
  (void)view;
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
  protocol_network_e2e_business_probe_t *probe =
      (protocol_network_e2e_business_probe_t *)ctx;
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

static int protocol_network_e2e_yaml(protocol_network_e2e_fixture_t *fixture,
                                     protocol_network_e2e_storage_kind_t storage, char *out,
                                     size_t capacity) {
  const char *backend = protocol_network_e2e_backend_name();
  int written;
  if (!fixture || !out || capacity == 0u || fixture->receiver_port == 0u) return SALTS_EINVAL;
  written = snprintf(
      out, capacity,
      "version: 1\n"
      "adapters:\n"
      "  tcp.input:\n"
      "    kind: cnet.listener_source\n"
      "    config:\n"
      "      schema_version: 1\n"
      "      backend: %s\n"
      "      bind_host: \"127.0.0.1\"\n"
      "      bind_port: 0\n"
      "      backlog: 4\n"
      "      reuse_port: false\n"
      "      connection_capacity: 1\n"
      "      command_capacity: 8\n"
      "      request_capacity: 8\n"
      "      completion_batch_capacity: 4\n"
      "      event_capacity: 8\n"
      "      max_send_bytes: 1024\n"
      "      receive_buffer_bytes: 1024\n"
      "      connect_timeout_ms: 0\n"
      "      read_timeout_ms: 0\n"
      "      write_timeout_ms: 0\n"
      "      tls_io_buffer_bytes: 0\n"
      "      tls_handshake_timeout_ms: 0\n"
      "      command_buffer_bytes: 0\n"
      "      event_buffer_bytes: 0\n"
      "      socket_receive_buffer_bytes: 0\n"
      "      socket_send_buffer_bytes: 0\n"
      "      keepalive: false\n"
      "      keepalive_idle_ms: 0\n"
      "      keepalive_interval_ms: 0\n"
      "      keepalive_count: 0\n"
      "      linger: false\n"
      "      linger_ms: 0\n"
      "      tls_enabled: false\n"
      "      tls_ca_file: \"\"\n"
      "      tls_ca_path: \"\"\n"
      "      tls_cert_file: \"\"\n"
      "      tls_key_file: \"\"\n"
      "      tls_key_password: \"\"\n"
      "      tls_client_auth: none\n"
      "      tls_alpn: []\n"
      "      max_connections: 1\n"
      "      max_message_bytes: 1024\n"
      "      scheduler_capacity: 8\n"
      "      scheduler_max_steps_per_poll: 32\n"
      "      first_message_id: 1\n"
      "      initial_demand: 8\n"
      "      stop_timeout_ms: 1000\n"
      "  protocol.decode:\n"
      "    kind: protocol.decode\n"
      "    config:\n"
      "      schema_version: 2\n"
      "      protocol_provider: jtt808\n"
      "      protocol_kind: jtt808\n"
      "      protocol_version: 2019-A1\n"
      "      source_id: fleet.primary\n"
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
      "      stop_timeout_ms: 1000\n"
      "channels:\n"
      "  intake.store:\n"
      "    kind: flow.durable.memory\n"
      "    config:\n"
      "      schema_version: 1\n"
      "      identity_mode: stable_required\n"
      "      max_message_bytes: 4096\n"
      "      max_records: 8\n"
      "      max_total_bytes: 32768\n"
      "      max_record_bytes: 4096\n"
      "      max_claims: 1\n",
      backend, backend, (unsigned)fixture->receiver_port);
  if (written < 0 || (size_t)written >= capacity) return SALTS_ENOSPC;
  return protocol_network_e2e_storage_rewrite_yaml(fixture, storage, out, capacity);
}

static int protocol_network_e2e_host_open(protocol_network_e2e_fixture_t *fixture,
                                          const char *cnet_module, const char *jtt808_module,
                                          const char *durable_module) {
  turbo_flow_plugin_host_config_t config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  int rc;
  config.module_capacity = 3u;
  config.adapter_provider_capacity = 0u;
  config.resource_provider_capacity = 0u;
  config.protocol_provider_capacity = 1u;
  config.business_provider_capacity = 0u;
  config.transactional_adapter_provider_capacity = 6u;
  config.transactional_resource_provider_capacity = 1u;
  config.schema_capacity = 0u;
  config.operation_capacity = 0u;
  rc = turbo_flow_plugin_host_create(&config, &fixture->host, &error);
  if (rc == SALTS_OK) rc = turbo_flow_plugin_host_load(fixture->host, cnet_module, &error);
  if (rc == SALTS_OK) rc = turbo_flow_plugin_host_load(fixture->host, jtt808_module, &error);
  if (rc == SALTS_OK) rc = turbo_flow_plugin_host_load(fixture->host, durable_module, &error);
  if (rc == SALTS_OK)
    rc = turbo_flow_plugin_catalog_snapshot_create(fixture->host, &fixture->catalog, &error);
  return rc;
}

static int protocol_network_e2e_intake_open(protocol_network_e2e_fixture_t *fixture) {
  static const char graph[] = "source wire adapter tcp.input\n"
                              "stage decode adapter protocol.decode\n"
                              "stage main {\n"
                              "  wire -> decode\n"
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
  config.downstream_flow =
      turbo_flow_plugin_generation_flow(fixture->business_generation);
  config.source_adapter_name = "tcp.input";
  config.decoder_adapter_name = "protocol.decode";
  config.decoded_source_name = "decoded";
  rc = turbo_flow_protocol_network_intake_create(&config, &flow, &fixture->intake, &error);
  if (rc != SALTS_OK)
    (void)fprintf(stderr, "jtt808 e2e intake create rc=%d path=%s reason=%s\n",
                  rc, error.path, error.message);
  if (flow) turbo_flow_destroy(flow);
  return rc;
}

static int protocol_network_e2e_business_open(protocol_network_e2e_fixture_t *fixture) {
  static const char graph[] = "source decoded\n"
                              "buffer intake resource intake.store\n"
                              "stage output adapter udp.output\n"
                              "stage main {\n"
                              "  decoded -> intake -> output\n"
                              "}\n";
  turbo_flow_plugin_generation_config_t generation_config =
      TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_observer_ops_t observer;
  turbo_flow_t *flow = turbo_flow_create();
  int rc;
  if (!flow) return SALTS_ENOMEM;
  rc = turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u);
  if (rc != SALTS_OK) goto fail;
  memset(&observer, 0, sizeof(observer));
  observer.size = sizeof(observer);
  observer.stage_complete = protocol_network_e2e_business_stage;
  rc = turbo_flow_set_observer(flow, &observer, &fixture->business);
  if (rc != SALTS_OK) goto fail;
  generation_config.owner_capacity = 2u;
  rc = turbo_flow_plugin_generation_create(fixture->catalog, fixture->resolved, &flow,
                                           &generation_config, NULL,
                                           &fixture->business_generation,
                                           &fixture->business_cleanup, &error);
  if (rc != SALTS_OK) {
    (void)fprintf(stderr, "jtt808 e2e business generation rc=%d path=%s reason=%s\n",
                  rc, error.path, error.message);
    goto fail;
  }
  rc = turbo_flow_start(turbo_flow_plugin_generation_flow(fixture->business_generation));
  if (rc != SALTS_OK) return rc;
  return SALTS_OK;

fail:
  if (flow) turbo_flow_destroy(flow);
  return rc;
}

int protocol_network_e2e_jtt808_init(protocol_network_e2e_fixture_t *fixture,
                                     const char *cnet_module, const char *jtt808_module,
                                     const char *durable_module,
                                     protocol_network_e2e_storage_kind_t storage) {
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  char yaml[16384];
  int rc;
  if (!fixture || !cnet_module || !cnet_module[0] || !jtt808_module || !jtt808_module[0] ||
      !durable_module || !durable_module[0])
    return SALTS_EINVAL;
  memset(fixture, 0, sizeof(*fixture));
  rc = protocol_network_e2e_storage_prepare(fixture, storage);
  if (rc == SALTS_OK) rc = protocol_network_e2e_receiver_open(fixture);
  if (rc == SALTS_OK) rc = protocol_network_e2e_client_open(fixture);
  if (rc == SALTS_OK)
    rc = protocol_network_e2e_host_open(fixture, cnet_module, jtt808_module,
                                        durable_module);
  if (rc == SALTS_OK) rc = protocol_network_e2e_yaml(fixture, storage, yaml, sizeof(yaml));
  if (rc == SALTS_OK)
    rc = turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &fixture->resolved, &error);
  if (rc == SALTS_OK) rc = protocol_network_e2e_business_open(fixture);
  if (rc == SALTS_OK) rc = protocol_network_e2e_intake_open(fixture);
  if (rc != SALTS_OK) protocol_network_e2e_destroy(fixture);
  return rc;
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

int protocol_network_e2e_tcp_connect(protocol_network_e2e_fixture_t *fixture,
                                     const char *endpoint, uint32_t timeout_ms) {
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
  deadline = salts_monotonic_ms() + timeout_ms;
  while (fixture->tcp.connected == connected_before && fixture->tcp.failed == 0 &&
         salts_monotonic_ms() < deadline) {
    rc = protocol_network_e2e_poll(fixture, 1u, &snapshot);
    if (rc != SALTS_OK) return rc;
  }
  if (fixture->tcp.failed) return SALTS_EIO;
  return fixture->tcp.connected > connected_before ? SALTS_OK : SALTS_ETIMEDOUT;
}

int protocol_network_e2e_tcp_send(protocol_network_e2e_fixture_t *fixture, const void *data,
                                  size_t size) {
  if (!fixture || !fixture->client_initialized || !data || size == 0u) return SALTS_EINVAL;
  return cnet_send(&fixture->client, fixture->connection, data, size);
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
  deadline = salts_monotonic_ms() + timeout_ms;
  while (fixture->tcp.terminal == terminal_before && salts_monotonic_ms() < deadline) {
    rc = protocol_network_e2e_poll(fixture, 1u, &snapshot);
    if (rc != SALTS_OK) return rc;
  }
  return fixture->tcp.terminal > terminal_before ? SALTS_OK : SALTS_ETIMEDOUT;
}

int protocol_network_e2e_business_request_and_drive(
    protocol_network_e2e_fixture_t *fixture, uint32_t timeout_ms) {
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  size_t events = 0u;
  const size_t received_before = fixture ? fixture->udp.received : 0u;
  const size_t completed_before = fixture ? fixture->business.stage_completions : 0u;
  uint64_t deadline;
  int rc;
  if (!fixture || !fixture->business_generation) return SALTS_EINVAL;
  deadline = salts_monotonic_ms() + timeout_ms;
  while (salts_monotonic_ms() < deadline) {
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
    salts_sleep_ms(1u);
  }
  return SALTS_ETIMEDOUT;
}

void protocol_network_e2e_destroy(protocol_network_e2e_fixture_t *fixture) {
  turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  if (!fixture) return;

  if (fixture->client_initialized) {
    (void)cnet_client_stop(&fixture->client, 1000u);
    (void)cnet_client_destroy(&fixture->client);
    fixture->client_initialized = 0;
  }
  if (fixture->intake) {
    (void)turbo_flow_protocol_network_intake_stop(fixture->intake, 1000u);
    (void)turbo_flow_protocol_network_intake_destroy(fixture->intake);
    fixture->intake = NULL;
  }
  if (fixture->business_generation) {
    (void)turbo_flow_plugin_generation_destroy(fixture->business_generation, 1000u, &config_error);
    fixture->business_generation = NULL;
  }
  if (fixture->business_cleanup) {
    config_error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    (void)turbo_flow_plugin_generation_destroy(fixture->business_cleanup, 1000u, &config_error);
    fixture->business_cleanup = NULL;
  }
  if (fixture->catalog) {
    turbo_flow_plugin_catalog_snapshot_destroy(fixture->catalog);
    fixture->catalog = NULL;
  }
  turbo_flow_resolved_config_destroy(fixture->resolved);
  fixture->resolved = NULL;
  if (fixture->receiver_initialized) {
    (void)cnet_datagram_stop(&fixture->receiver, 1000u);
    (void)cnet_datagram_destroy(&fixture->receiver);
    fixture->receiver_initialized = 0;
  }
  if (fixture->host) {
    (void)turbo_flow_plugin_host_destroy(fixture->host, 1000u, &plugin_error);
    fixture->host = NULL;
  }
  protocol_network_e2e_storage_cleanup(fixture);
}

size_t protocol_network_e2e_jtt808_frame(uint8_t *out, size_t capacity) {
  static const uint8_t unescaped[] = {
      0x02u, 0x00u, 0x40u, 0x00u, 0x01u, 0x00u, 0x00u, 0x00u, 0x00u,
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