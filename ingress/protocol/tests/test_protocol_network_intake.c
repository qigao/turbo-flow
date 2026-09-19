#include "tinytest.h"

#include "turbo_flow_protocol_network_intake.h"
#include "turbo_flow_durable_buffer.h"

#include <stdio.h>
#include <string.h>

#ifndef FLOW_PROTOCOL_NETWORK_SOURCE_FIXTURE
  #error FLOW_PROTOCOL_NETWORK_SOURCE_FIXTURE is required
#endif
#ifndef FLOW_PROTOCOL_JTT808_MODULE
  #error FLOW_PROTOCOL_JTT808_MODULE is required
#endif

extern int protocol_network_intake_header_cpp_probe(void);

typedef struct intake_owner_fixture_s {
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *catalog;
  turbo_flow_resolved_config_t *resolved;
  turbo_flow_inbox_t inbox;
  turbo_flow_t *flow;
  turbo_flow_t *downstream_flow;
  turbo_flow_durable_buffer_binding_t *durable_binding;
} intake_owner_fixture_t;

static void intake_owner_yaml(char *out, size_t capacity, const char *source_name,
                              const char *protocol_version) {
  check_true(snprintf(out, capacity,
                      "version: 1\n"
                      "adapters:\n"
                      "  %s:\n"
                      "    kind: cnet.listener_source\n"
                      "    config:\n"
                      "      max_connections: 1\n"
                      "      max_message_bytes: 64\n"
                      "      scheduler_max_steps_per_poll: 2\n"
                      "  protocol.decode:\n"
                      "    kind: protocol.decode\n"
                      "    config:\n"
                      "      schema_version: 2\n"
                      "      protocol_provider: jtt808\n"
                      "      protocol_kind: jtt808\n"
                      "      protocol_version: %s\n"
                      "      source_id: fixture.source\n"
                      "      max_sessions: 1\n"
                      "      max_frame_size: 64\n"
                      "      max_pending_claims: 4\n"
                      "      max_pending_bytes: 256\n",
                      source_name, protocol_version) > 0);
}

static void intake_owner_graph(char *out, size_t capacity, const char *source_name) {
  check_true(snprintf(out, capacity,
                      "source wire adapter %s\n"
                      "stage decode adapter protocol.decode\n"
                      "stage main {\n"
                      "  wire -> decode\n"
                      "}\n",
                      source_name) > 0);
}

static int intake_owner_downstream_open(intake_owner_fixture_t *fixture) {
  static const char graph[] = "source decoded\n"
                              "buffer intake resource protocol.store\n"
                              "stage main {\n"
                              "  decoded -> intake\n"
                              "}\n";
  turbo_flow_durable_buffer_binding_config_t binding =
      TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  fixture->downstream_flow = turbo_flow_create();
  if (!fixture->downstream_flow) return SALTS_ENOMEM;
  int rc = turbo_flow_parse_string(fixture->downstream_flow, graph, sizeof(graph) - 1u);
  if (rc != SALTS_OK) return rc;
  binding.resource_name = "protocol.store";
  binding.inbox = &fixture->inbox;
  binding.identity_mode = TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED;
  binding.max_message_bytes = 4096u;
  rc = turbo_flow_durable_buffer_bind(fixture->downstream_flow, &binding,
                                      &fixture->durable_binding);
  if (rc == SALTS_OK) rc = turbo_flow_compile(fixture->downstream_flow);
  if (rc == SALTS_OK) rc = turbo_flow_start(fixture->downstream_flow);
  return rc;
}

static intake_owner_fixture_t intake_owner_fixture(const char *source_name,
                                                   const char *protocol_version) {
  turbo_flow_plugin_host_config_t host_config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_inbox_memory_config_t inbox_config = turbo_flow_inbox_memory_config_default();
  intake_owner_fixture_t fixture;
  char yaml[4096];
  char graph[512];
  memset(&fixture, 0, sizeof(fixture));
  fixture.inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
  host_config.module_capacity = 2u;
  host_config.adapter_provider_capacity = 0u;
  host_config.resource_provider_capacity = 0u;
  host_config.protocol_provider_capacity = 1u;
  host_config.business_provider_capacity = 0u;
  host_config.transactional_adapter_provider_capacity = 1u;
  host_config.transactional_resource_provider_capacity = 0u;
  host_config.schema_capacity = 0u;
  host_config.operation_capacity = 0u;
  check_equal(turbo_flow_plugin_host_create(&host_config, &fixture.host, &plugin_error), SALTS_OK);
  check_equal(turbo_flow_plugin_host_load(fixture.host, FLOW_PROTOCOL_NETWORK_SOURCE_FIXTURE,
                                          &plugin_error),
              SALTS_OK);
  check_equal(turbo_flow_plugin_host_load(fixture.host, FLOW_PROTOCOL_JTT808_MODULE, &plugin_error),
              SALTS_OK);
  check_equal(turbo_flow_plugin_catalog_snapshot_create(fixture.host, &fixture.catalog,
                                                        &plugin_error),
              SALTS_OK);
  intake_owner_yaml(yaml, sizeof(yaml), source_name, protocol_version);
  check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &fixture.resolved, &config_error),
              SALTS_OK);
  inbox_config.max_records = 8u;
  inbox_config.max_total_bytes = 8192u;
  inbox_config.max_record_bytes = 4096u;
  inbox_config.max_claims = 8u;
  check_equal(turbo_flow_inbox_memory_create(&inbox_config, &fixture.inbox), SALTS_OK);
  check_equal(intake_owner_downstream_open(&fixture), SALTS_OK);
  fixture.flow = turbo_flow_create();
  check_not_null(fixture.flow);
  intake_owner_graph(graph, sizeof(graph), source_name);
  check_equal(turbo_flow_parse_string(fixture.flow, graph, strlen(graph)), SALTS_OK);
  return fixture;
}

static turbo_flow_protocol_network_intake_config_t
intake_owner_config(intake_owner_fixture_t *fixture, const char *source_name) {
  turbo_flow_protocol_network_intake_config_t config =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT;
  config.catalog = fixture->catalog;
  config.resolved = fixture->resolved;
  config.downstream_flow = fixture->downstream_flow;
  config.source_adapter_name = source_name;
  config.decoder_adapter_name = "protocol.decode";
  config.decoded_source_name = "decoded";
  return config;
}

static void intake_owner_fixture_release_inputs(intake_owner_fixture_t *fixture) {
  if (!fixture) return;
  if (fixture->flow) {
    turbo_flow_destroy(fixture->flow);
    fixture->flow = NULL;
  }
  if (fixture->downstream_flow) {
    if (turbo_flow_state(fixture->downstream_flow) == TURBO_FLOW_STATE_STARTED)
      check_equal(turbo_flow_stop(fixture->downstream_flow), SALTS_OK);
    if (fixture->durable_binding) {
      check_equal(turbo_flow_durable_buffer_unbind(fixture->durable_binding), SALTS_OK);
      fixture->durable_binding = NULL;
    }
    turbo_flow_destroy(fixture->downstream_flow);
    fixture->downstream_flow = NULL;
  }
  turbo_flow_resolved_config_destroy(fixture->resolved);
  fixture->resolved = NULL;
  if (fixture->catalog) {
    turbo_flow_plugin_catalog_snapshot_destroy(fixture->catalog);
    fixture->catalog = NULL;
  }
  if (fixture->inbox.ops) {
    check_equal(turbo_flow_inbox_close(&fixture->inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&fixture->inbox), SALTS_OK);
  }
}

static void intake_owner_host_destroy(intake_owner_fixture_t *fixture, int expected) {
  turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  check_equal(turbo_flow_plugin_host_destroy(fixture->host, 1000u, &error), expected);
  if (expected == SALTS_OK) fixture->host = NULL;
}

spec("ProtocolNetworkIntake owner") {
  it("exposes exact C and C++ size-versioned ABI") {
    turbo_flow_protocol_network_intake_config_t config =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT;
    turbo_flow_protocol_network_intake_snapshot_t snapshot =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    check_equal(config.size, sizeof(config));
    check_equal(config.version, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION);
    check_equal(snapshot.size, sizeof(snapshot));
    check_equal(snapshot.version, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION);
    check_equal(snapshot.state, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED);
    check_equal(protocol_network_intake_header_cpp_probe(), 0);
  }

  it("retains catalog ownership and enforces compiled running stopped lifecycle") {
    intake_owner_fixture_t fixture = intake_owner_fixture("wire.input", "2019-A1");
    turbo_flow_protocol_network_intake_config_t config =
        intake_owner_config(&fixture, "wire.input");
    turbo_flow_protocol_network_intake_t *intake = NULL;
    turbo_flow_protocol_network_intake_snapshot_t snapshot =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(turbo_flow_protocol_network_intake_create(&config, &fixture.flow, &intake, &error),
                SALTS_OK);
    check_null(fixture.flow);
    check_not_null(intake);
    check_equal(turbo_flow_protocol_network_intake_snapshot(intake, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_COMPILED);
    check_equal(snapshot.source_polls, (uint64_t)0u);
    check_equal(turbo_flow_protocol_network_intake_poll(intake, 0u, &snapshot), SALTS_EBUSY);
    check_equal(snapshot.source_polls, (uint64_t)0u);
    check_equal(turbo_flow_protocol_network_intake_destroy(intake), SALTS_EBUSY);

    turbo_flow_plugin_catalog_snapshot_destroy(fixture.catalog);
    fixture.catalog = NULL;
    intake_owner_host_destroy(&fixture, SALTS_EBUSY);

    check_equal(turbo_flow_protocol_network_intake_start(intake), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_snapshot(intake, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_RUNNING);
    check_equal(strcmp(snapshot.source_endpoint, "tcp://127.0.0.1:12345"), 0);
    check_equal(turbo_flow_protocol_network_intake_poll(intake, 0u, &snapshot), SALTS_OK);
    check_equal(snapshot.source_polls, (uint64_t)1u);
    check_equal(turbo_flow_protocol_network_intake_stop(intake, 1000u), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_snapshot(intake, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPED);
    check_equal(turbo_flow_protocol_network_intake_destroy(intake), SALTS_OK);

    intake_owner_fixture_release_inputs(&fixture);
    intake_owner_host_destroy(&fixture, SALTS_OK);
  }

  it("rejects public ABI mismatch before transferring Flow") {
    intake_owner_fixture_t fixture = intake_owner_fixture("wire.input", "2019-A1");
    turbo_flow_protocol_network_intake_config_t config =
        intake_owner_config(&fixture, "wire.input");
    turbo_flow_protocol_network_intake_t *intake = (turbo_flow_protocol_network_intake_t *)1;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    config.version += 1u;
    check_equal(turbo_flow_protocol_network_intake_create(&config, &fixture.flow, &intake, &error),
                SALTS_EINVAL);
    check_not_null(fixture.flow);
    check_null(intake);
    intake_owner_fixture_release_inputs(&fixture);
    intake_owner_host_destroy(&fixture, SALTS_OK);
  }

  it("keeps Flow caller-owned on Source preflight failure") {
    intake_owner_fixture_t fixture = intake_owner_fixture("preflight.fail", "2019-A1");
    turbo_flow_protocol_network_intake_config_t config =
        intake_owner_config(&fixture, "preflight.fail");
    turbo_flow_protocol_network_intake_t *intake = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_protocol_network_intake_create(&config, &fixture.flow, &intake, &error),
                SALTS_EIO);
    check_not_null(fixture.flow);
    check_null(intake);
    intake_owner_fixture_release_inputs(&fixture);
    intake_owner_host_destroy(&fixture, SALTS_OK);
  }

  it("rejects protocol version before a failing Source materializer can run") {
    intake_owner_fixture_t fixture = intake_owner_fixture("materialize.fail", "2013");
    turbo_flow_protocol_network_intake_config_t config =
        intake_owner_config(&fixture, "materialize.fail");
    turbo_flow_protocol_network_intake_t *intake = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_protocol_network_intake_create(&config, &fixture.flow, &intake, &error),
                SALTS_ENOTSUP);
    check_not_null(fixture.flow);
    check_null(intake);
    intake_owner_fixture_release_inputs(&fixture);
    intake_owner_host_destroy(&fixture, SALTS_OK);
  }

  it("unwinds transferred Flow and retained modules on Source materialize failure") {
    intake_owner_fixture_t fixture = intake_owner_fixture("materialize.fail", "2019-A1");
    turbo_flow_protocol_network_intake_config_t config =
        intake_owner_config(&fixture, "materialize.fail");
    turbo_flow_protocol_network_intake_t *intake = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_protocol_network_intake_create(&config, &fixture.flow, &intake, &error),
                SALTS_EIO);
    check_null(fixture.flow);
    check_null(intake);
    intake_owner_fixture_release_inputs(&fixture);
    intake_owner_host_destroy(&fixture, SALTS_OK);
  }

  it("counts one failing external Source poll and preserves its exact terminal error") {
    intake_owner_fixture_t fixture = intake_owner_fixture("poll.fail", "2019-A1");
    turbo_flow_protocol_network_intake_config_t config =
        intake_owner_config(&fixture, "poll.fail");
    turbo_flow_protocol_network_intake_t *intake = NULL;
    turbo_flow_protocol_network_intake_snapshot_t snapshot =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_protocol_network_intake_create(&config, &fixture.flow, &intake, &error),
                SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_start(intake), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_poll(intake, 0u, &snapshot), SALTS_EIO);
    check_equal(snapshot.state, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_FAILED);
    check_equal(snapshot.status, SALTS_EIO);
    check_equal(snapshot.source_polls, (uint64_t)1u);
    check_equal(turbo_flow_protocol_network_intake_stop(intake, 1000u), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_destroy(intake), SALTS_OK);
    fixture.flow = NULL;
    intake_owner_fixture_release_inputs(&fixture);
    intake_owner_host_destroy(&fixture, SALTS_OK);
  }

  it("stops a compiled owner without starting or polling the Source") {
    intake_owner_fixture_t fixture = intake_owner_fixture("start.fail", "2019-A1");
    turbo_flow_protocol_network_intake_config_t config =
        intake_owner_config(&fixture, "start.fail");
    turbo_flow_protocol_network_intake_t *intake = NULL;
    turbo_flow_protocol_network_intake_snapshot_t snapshot =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(turbo_flow_protocol_network_intake_create(&config, &fixture.flow, &intake, &error),
                SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_stop(intake, 1000u), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_snapshot(intake, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPED);
    check_equal(snapshot.source_polls, (uint64_t)0u);
    check_equal(turbo_flow_protocol_network_intake_destroy(intake), SALTS_OK);
    fixture.flow = NULL;
    intake_owner_fixture_release_inputs(&fixture);
    intake_owner_host_destroy(&fixture, SALTS_OK);
  }
}
