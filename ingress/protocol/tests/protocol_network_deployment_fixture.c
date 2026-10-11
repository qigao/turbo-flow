#include "protocol_network_deployment.h"
#include "turbo_flow_cnet_resource.h"
#include "turbo_flow_durable_memory_resource.h"
#include <salts/plugin.h>
#include <salts/thread.h>
#include <stdbool.h>
#include <tstr.h>
#ifdef FLOW_E2E_RESOURCE_TURBODB
  #include "turbo_flow_turbodb_resource.h"
  #include "turbodb_test_runtime.h"
static const char protocol_network_e2e_meta_ddl[] =
    "CREATE TABLE protocol_e2e_inbox_meta_v3 ("
    "singleton_id integer primary key not null, schema_magic text not null, "
    "schema_version integer not null, generation bigint not null, owner_state integer not null, "
    "next_record_id bigint not null, next_claim_token bigint not null, max_records bigint not "
    "null, max_total_bytes bigint not null, max_record_bytes bigint not null, max_claims bigint "
    "not null, records bigint not null, history_records bigint not null, pending_records bigint "
    "not null, failed_records bigint not null, in_flight_claims bigint not null, retained_bytes "
    "bigint not null, admitted bigint not null, completed bigint not null, failed bigint not null, "
    "retried bigint not null, discarded bigint not null)";

static const char protocol_network_e2e_records_ddl[] =
    "CREATE TABLE protocol_e2e_inbox_records_v3 ("
    "record_id bigint primary key not null, phase integer not null, claim_generation bigint not "
    "null, claim_token bigint not null, failure_status integer not null, failure_kind integer not "
    "null, terminal_kind integer not null, envelope_schema text not null, envelope_schema_version "
    "integer not null, source_id bytea not null, partition_key bytea not null, admission_id bytea "
    "not null, source_sequence_be "
    "bytea not null, timestamp_ns_be bytea not null, message_type bigint not null, message_flags "
    "bigint not null, content_domain integer not null, content_profile integer not null, "
    "content_encoding integer not null, content_flags bigint not null, content_schema_version "
    "bigint not null, content_media_type text not null, content_schema_name text not null, "
    "content_type_name text not null, content_identity text not null, correlation bytea not null, "
    "payload bytea not null, retained_bytes bigint not null)";

static const char protocol_network_e2e_admission_index_ddl[] =
    "CREATE UNIQUE INDEX protocol_e2e_inbox_records_v3_admission ON "
    "protocol_e2e_inbox_records_v3(source_id, admission_id)";

static const char protocol_network_e2e_phase_index_ddl[] =
    "CREATE INDEX protocol_e2e_inbox_records_v3_phase ON "
    "protocol_e2e_inbox_records_v3(phase, record_id)";

static const char protocol_network_e2e_meta_row[] =
    "INSERT INTO protocol_e2e_inbox_meta_v3 VALUES "
    "(1, 'turbo-flow.turbodb.inbox', 3, 0, 0, 1, 1, 8, 32768, 4096, 1, "
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

typedef struct deployment_s {
  bool started, stopping, configured;
  unsigned receiver_port;
#ifdef FLOW_E2E_RESOURCE_TURBODB
  tstr filename_text;
  orm_runtime_t *runtime;
  orm_option_t filename;
  orm_config_t database;
#endif
} deployment_t;
static deployment_t deployments[2];
typedef struct network_resource_s {
  deployment_t *owner;
  bool peer;
} network_resource_t;
static network_resource_t networks[2][3];

static int configure(void *self, unsigned slot, unsigned receiver_port, const char *filename) {
  deployment_t *state;
  (void)self;
  if (slot >= 2u) return SALTS_EINVAL;
  state = &deployments[slot];
  if (!state || !state->started || state->stopping || state->configured || receiver_port == 0u ||
      receiver_port > UINT16_MAX)
    return SALTS_EINVAL;
#ifdef FLOW_E2E_RESOURCE_TURBODB
  if (filename) {
    orm_error_t error;
    orm_connection_t *connection = NULL;
    int rc;
    if (state->runtime) return SALTS_EBUSY;
    state->filename_text = tstr_dup(filename);
    if (!state->filename_text) return SALTS_ENOMEM;
    orm_error_init(&error);
    if (turbodb_test_sqlite_runtime_open(&state->runtime, &error) != ORM_STATUS_OK)
      return SALTS_EIO;
    orm_config(&state->database);
    state->filename.keyword = orm_view("filename");
    state->filename.value = orm_view(state->filename_text);
    state->database.driver = orm_view("sqlite");
    state->database.options = &state->filename;
    state->database.option_count = 1u;
    if (orm_runtime_connect(state->runtime, &state->database, &connection, &error) != ORM_STATUS_OK)
      return SALTS_EIO;
    rc = protocol_network_e2e_sql(connection, protocol_network_e2e_meta_ddl);
    if (rc == SALTS_OK) rc = protocol_network_e2e_sql(connection, protocol_network_e2e_records_ddl);
    if (rc == SALTS_OK)
      rc = protocol_network_e2e_sql(connection, protocol_network_e2e_admission_index_ddl);
    if (rc == SALTS_OK)
      rc = protocol_network_e2e_sql(connection, protocol_network_e2e_phase_index_ddl);
    if (rc == SALTS_OK) rc = protocol_network_e2e_sql(connection, protocol_network_e2e_meta_row);
    orm_disconnect(connection);
    if (rc != SALTS_OK) return rc;
  }
#else
  if (filename) return SALTS_ENOTSUP;
#endif
  state->receiver_port = receiver_port;
  state->configured = true;
  return SALTS_OK;
}

static int network_snapshot(void *self, turbo_flow_cnet_deployment_view_t *out) {
  network_resource_t *resource = self;
  deployment_t *state = resource->owner;
  if (!state->started || !state->configured || state->stopping || !out || out->size != sizeof(*out))
    return SALTS_EINVAL;
  *out = (turbo_flow_cnet_deployment_view_t)TURBO_FLOW_CNET_DEPLOYMENT_VIEW_INIT;
  out->bind_host = "127.0.0.1";
  if (resource->peer) {
    out->peer_host = "127.0.0.1";
    out->peer_port = (uint16_t)state->receiver_port;
  }
  return SALTS_OK;
}
static int memory_snapshot(void *self, turbo_flow_durable_memory_resource_view_t *out) {
  deployment_t *state = self;
  if (!state->started || !state->configured || state->stopping || !out || out->size != sizeof(*out))
    return SALTS_EINVAL;
  *out = (turbo_flow_durable_memory_resource_view_t){sizeof(*out), 8u, 32768u, 4096u, 1u};
  return SALTS_OK;
}
#ifdef FLOW_E2E_RESOURCE_TURBODB
static int database_snapshot(void *self, turbo_flow_turbodb_database_view_t *out) {
  deployment_t *state = self;
  if (!state->started || !state->configured || state->stopping || !state->runtime || !out ||
      out->size != sizeof(*out))
    return SALTS_EINVAL;
  *out = (turbo_flow_turbodb_database_view_t){sizeof(*out), state->runtime, &state->database,
                                              "protocol_e2e"};
  return SALTS_OK;
}
CMETA_IMPLEMENTS(turbo_flow_turbodb_database_resource, database_impl,
                 TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONNECT, .snapshot = database_snapshot);
static turbo_flow_turbodb_database_resource database_handles[2];
#endif
CMETA_IMPLEMENTS(protocol_network_deployment_control, control_impl, 0u, .configure = configure);
CMETA_IMPLEMENTS(turbo_flow_cnet_deployment_resource, listener_impl,
                 TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT | TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
                     TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
                 .snapshot = network_snapshot);
CMETA_IMPLEMENTS(turbo_flow_cnet_deployment_resource, packet_impl,
                 TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT | TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL |
                     TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
                 .snapshot = network_snapshot);
CMETA_IMPLEMENTS(turbo_flow_cnet_deployment_resource, datagram_impl,
                 TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT | TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT |
                     TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
                 .snapshot = network_snapshot);
CMETA_IMPLEMENTS(turbo_flow_durable_memory_resource, memory_impl,
                 TURBO_FLOW_DURABLE_MEMORY_RESOURCE_LIMITS, .snapshot = memory_snapshot);
static protocol_network_deployment_control control_handle;
static turbo_flow_cnet_deployment_resource listener_handles[2], packet_handles[2],
    datagram_handles[2];
static turbo_flow_durable_memory_resource memory_handles[2];
static cmeta_plugin_export exports[11];
static cmeta_plugin_manifest manifest = {.struct_size = CMETA_PLUGIN_MANIFEST_SIZE,
                                         .abi_version = CMETA_PLUGIN_ABI_VERSION,
                                         .plugin_id = "test.protocol.network.resources",
                                         .version = {1u, 0u, 0u},
                                         .self = deployments};
static cmeta_once_t once = SALTS_ONCE_INIT;
static void export_interface(size_t i, const char *id, const char *contract, unsigned version,
                             uint64_t caps, const cmeta_interface_desc *desc, void *value) {
  exports[i] = (cmeta_plugin_export){.struct_size = CMETA_PLUGIN_EXPORT_SIZE,
                                     .kind = CMETA_PLUGIN_EXPORT_INTERFACE,
                                     .contract_version = version,
                                     .capabilities = caps,
                                     .export_id = id,
                                     .contract_id = contract,
                                     .value.interface = {.desc = desc, .value = value}};
}
static void initialize(void) {
  static const char *const names[2][5] = {
      {"fixture.listener.0", "fixture.packet.0", "fixture.output.0", "fixture.memory.0",
       "fixture.database.0"},
      {"fixture.listener.1", "fixture.packet.1", "fixture.output.1", "fixture.memory.1",
       "fixture.database.1"}};
  size_t index = 0;
  control_handle = control_impl_as_protocol_network_deployment_control(deployments);
  export_interface(index++, "fixture.configure", PROTOCOL_NETWORK_DEPLOYMENT_CONTROL_ID, 1u, 0u,
                   protocol_network_deployment_control_interface(), &control_handle);
  for (size_t i = 0; i < 2u; ++i) {
    deployment_t *state = &deployments[i];
    for (size_t j = 0; j < 3u; ++j)
      networks[i][j] = (network_resource_t){state, j == 2u};
    listener_handles[i] = listener_impl_as_turbo_flow_cnet_deployment_resource(&networks[i][0]);
    packet_handles[i] = packet_impl_as_turbo_flow_cnet_deployment_resource(&networks[i][1]);
    datagram_handles[i] = datagram_impl_as_turbo_flow_cnet_deployment_resource(&networks[i][2]);
    memory_handles[i] = memory_impl_as_turbo_flow_durable_memory_resource(state);
    export_interface(index++, names[i][0], TURBO_FLOW_CNET_DEPLOYMENT_RESOURCE_CONTRACT_ID,
                     TURBO_FLOW_CNET_RESOURCE_CONTRACT_VERSION,
                     TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
                         TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
                         TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
                     turbo_flow_cnet_deployment_resource_interface(), &listener_handles[i]);
    export_interface(index++, names[i][1], TURBO_FLOW_CNET_DEPLOYMENT_RESOURCE_CONTRACT_ID,
                     TURBO_FLOW_CNET_RESOURCE_CONTRACT_VERSION,
                     TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
                         TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL |
                         TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
                     turbo_flow_cnet_deployment_resource_interface(), &packet_handles[i]);
    export_interface(index++, names[i][2], TURBO_FLOW_CNET_DEPLOYMENT_RESOURCE_CONTRACT_ID,
                     TURBO_FLOW_CNET_RESOURCE_CONTRACT_VERSION,
                     TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
                         TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT |
                         TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND,
                     turbo_flow_cnet_deployment_resource_interface(), &datagram_handles[i]);
    export_interface(index++, names[i][3], TURBO_FLOW_DURABLE_MEMORY_RESOURCE_CONTRACT_ID,
                     TURBO_FLOW_DURABLE_MEMORY_RESOURCE_CONTRACT_VERSION,
                     TURBO_FLOW_DURABLE_MEMORY_RESOURCE_LIMITS,
                     turbo_flow_durable_memory_resource_interface(), &memory_handles[i]);
#ifdef FLOW_E2E_RESOURCE_TURBODB
    database_handles[i] = database_impl_as_turbo_flow_turbodb_database_resource(state);
    export_interface(index++, names[i][4], TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONTRACT_ID,
                     TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONTRACT_VERSION,
                     TURBO_FLOW_TURBODB_DATABASE_RESOURCE_CONNECT,
                     turbo_flow_turbodb_database_resource_interface(), &database_handles[i]);
#endif
  }
  manifest.exports = exports;
  manifest.export_count = index;
}
static cmeta_plugin_status CMETA_PLUGIN_CALL start(void *self) {
  deployment_t *state = self;
  for (size_t i = 0; i < 2u; ++i)
    state[i].started = true;
  return CMETA_PLUGIN_OK;
}
static cmeta_plugin_status CMETA_PLUGIN_CALL stop(void *self) {
  deployment_t *state = self;
  for (size_t i = 0; i < 2u; ++i)
    state[i].stopping = true;
  return CMETA_PLUGIN_OK;
}
static bool CMETA_PLUGIN_CALL quiet(const void *self) {
  deployment_t *state = (deployment_t *)self;
  if (!state->stopping) return false;
#ifdef FLOW_E2E_RESOURCE_TURBODB
  orm_error_t error;
  orm_error_init(&error);
  for (size_t i = 0; i < 2u; ++i)
    if (turbodb_test_runtime_close(&state[i].runtime, &error) != ORM_STATUS_OK) return false;
#endif
  return true;
}
static void CMETA_PLUGIN_CALL destroy(void *self) {
  deployment_t *state = self;
#ifdef FLOW_E2E_RESOURCE_TURBODB
  for (size_t i = 0; i < 2u; ++i)
    tstr_free(state[i].filename_text);
#endif
  state->started = false;
}
CMETA_PLUGIN_QUERY_EXPORT const cmeta_plugin_manifest *CMETA_PLUGIN_CALL
cmeta_plugin_query(uint32_t abi) {
  if (abi != CMETA_PLUGIN_ABI_VERSION) return NULL;
  cmeta_once(&once, initialize);
  manifest.start = start;
  manifest.request_stop = stop;
  manifest.is_quiescent = quiet;
  manifest.destroy = destroy;
  return &manifest;
}
