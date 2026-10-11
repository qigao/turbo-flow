#include "tinytest.h"
#include "turbo_flow_protocol_network_intake.h"
#include "turbo_flow_durable_buffer.h"
#include "../../../turbo_flow/tests/adapter_component_fixture.h"

#include <stdio.h>
#include <string.h>

extern int protocol_network_intake_header_cpp_probe(void);

typedef struct intake_owner_fixture_s {
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *catalog;
  turbo_flow_inbox_t inbox;
  turbo_flow_t *flow;
  turbo_flow_t *downstream_flow;
  turbo_flow_durable_buffer_binding_t *durable_binding;
  cmeta_plugin_registry registry;
  cmeta_plugin_ref provider;
  int registry_live;
  int provider_live;
  adapter_component_fixture_t component;
  turbo_flow_provider_resolver_v2_t resolver;
  size_t provider_resolutions;
} intake_owner_fixture_t;
static intake_owner_fixture_t fixture;
static turbo_flow_protocol_network_intake_t *intake;

static int resolve_provider(void *ctx, const char *identity,
    turbo_flow_provider_candidate_v2_t *out, turbo_flow_config_error_t *error) {
  (void)error;
  if (ctx != &fixture || !identity || strcmp(identity, "fixture.source") != 0)
    return SALTS_ENOENT;
  ++fixture.provider_resolutions;
  out->component_identity = "AdapterProviderFixture";
  out->module_identity = "fixture.network.source";
  return SALTS_OK;
}

static int intake_owner_downstream_open(intake_owner_fixture_t *state) {
  static const char graph[] = "source decoded\n"
                              "buffer intake provider flow.durable.memory resource protocol_store\n"
                              "stage main {\n"
                              "  decoded -> intake\n"
                              "}\n";
  turbo_flow_durable_buffer_binding_config_t binding =
      TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  state->downstream_flow = turbo_flow_create();
  if (!state->downstream_flow) return SALTS_ENOMEM;
  int rc = turbo_flow_parse_string(state->downstream_flow, graph, sizeof(graph) - 1u);
  if (rc != SALTS_OK) return rc;
  binding.resource_name = "protocol_store";
  binding.inbox = &state->inbox;
  binding.identity_mode = TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED;
  binding.max_message_bytes = 4096u;
  rc = turbo_flow_durable_buffer_bind(state->downstream_flow, &binding,
                                      &state->durable_binding);
  if (rc == SALTS_OK) rc = turbo_flow_compile(state->downstream_flow);
  if (rc == SALTS_OK) rc = turbo_flow_start(state->downstream_flow);
  return rc;
}


static void open_fixture(unsigned mode, const char *version) {
  turbo_flow_plugin_host_config_t host_config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_inbox_memory_config_t inbox_config = turbo_flow_inbox_memory_config_default();
  cmeta_plugin_registry_config registry_config = {1u};
  char graph[2048];
  host_config.module_capacity = 1u;
  host_config.protocol_provider_capacity = 1u;
  host_config.transactional_adapter_provider_capacity = 0u;
  check_equal(turbo_flow_plugin_host_create(&host_config, &fixture.host, &plugin_error), SALTS_OK);
  check_equal(turbo_flow_plugin_host_load(fixture.host, FLOW_PROTOCOL_JTT808_MODULE, &plugin_error), SALTS_OK);
  check_equal(turbo_flow_plugin_catalog_snapshot_create(fixture.host, &fixture.catalog, &plugin_error), SALTS_OK);
  check_equal(cmeta_plugin_registry_init(&fixture.registry, &registry_config), CMETA_PLUGIN_OK);
  fixture.registry_live = 1;
  check_equal(cmeta_plugin_registry_load(&fixture.registry, FLOW_PROTOCOL_NETWORK_SOURCE_FIXTURE,
                                        &fixture.provider), CMETA_PLUGIN_OK);
  fixture.provider_live = 1;
  check_equal(cmeta_plugin_registry_start(&fixture.registry, fixture.provider), CMETA_PLUGIN_OK);
  check_equal(adapter_component_open(&fixture.component, &fixture.registry, fixture.provider,
                                     "fixture.source"), SALTS_OK);
  fixture.resolver = (turbo_flow_provider_resolver_v2_t)TURBO_FLOW_PROVIDER_RESOLVER_V2_INIT;
  fixture.resolver.ctx = &fixture;
  fixture.resolver.resolve = resolve_provider;
  inbox_config.max_records = 8u;
  inbox_config.max_total_bytes = 8192u;
  inbox_config.max_record_bytes = 4096u;
  inbox_config.max_claims = 8u;
  check_equal(turbo_flow_inbox_memory_create(&inbox_config, &fixture.inbox), SALTS_OK);
  check_equal(intake_owner_downstream_open(&fixture), SALTS_OK);
  check_true(
      snprintf(graph, sizeof(graph),
               "source wire adapter fixture.source {\n  mode %u\n}\n"
               "stage decode adapter protocol.decode {\n"
               "  schema_version 1\n  protocol_provider \"jtt808\"\n  protocol_kind \"jtt808\"\n"
               "  protocol_version \"%s\"\n  source_id \"fixture.source\"\n  max_sessions 1\n"
               "  max_frame_size 64\n  max_pending_claims 4\n  max_pending_bytes 256\n"
               "}\nstage main {\n  wire -> decode\n}\n",
               mode, version) > 0);
  fixture.flow = turbo_flow_create();
  check_not_null(fixture.flow);
  check_equal(turbo_flow_parse_string(fixture.flow, graph, strlen(graph)), SALTS_OK);
}

static turbo_flow_protocol_network_intake_config_t intake_config(void) {
  turbo_flow_protocol_network_intake_config_t config = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT;
  config.catalog = fixture.catalog;
  config.downstream_flow = fixture.downstream_flow;
  config.source_stage_name = "wire";
  config.decoder_stage_name = "decode";
  config.decoded_source_name = "decoded";
  config.component_runtime = &fixture.component.component.runtime;
  config.provider_resolver = &fixture.resolver;
  return config;
}

static int create_intake(turbo_flow_config_error_t *error) {
  turbo_flow_protocol_network_intake_config_t config = intake_config();
  return turbo_flow_protocol_network_intake_create(&config, &fixture.flow, &intake, error);
}

static void close_intake(void) {
  int rc;
  if (!intake) return;
  rc = turbo_flow_protocol_network_intake_stop(intake, 1000u);
  if (rc == SALTS_EBUSY) rc = turbo_flow_protocol_network_intake_stop(intake, 1000u);
  check_true(rc == SALTS_OK || rc == SALTS_EALREADY);
  rc = turbo_flow_protocol_network_intake_destroy(intake);
  if (rc == SALTS_EBUSY) rc = turbo_flow_protocol_network_intake_destroy(intake);
  check_equal(rc, SALTS_OK);
  intake = NULL;
}

spec("canonical ProtocolNetworkIntake owner") {
  before_each() {
    memset(&fixture, 0, sizeof(fixture));
    fixture.inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
    intake = NULL;
  }
  after_each() {
    turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    bool quiet = false;
    close_intake();
    turbo_flow_destroy(fixture.flow);
    if (fixture.downstream_flow) {
      check_equal(turbo_flow_stop(fixture.downstream_flow), SALTS_OK);
      if (fixture.durable_binding) check_equal(turbo_flow_durable_buffer_unbind(fixture.durable_binding), SALTS_OK);
      turbo_flow_destroy(fixture.downstream_flow);
    }
    if (fixture.inbox.ops) {
      check_equal(turbo_flow_inbox_close(&fixture.inbox), SALTS_OK);
      check_equal(turbo_flow_inbox_destroy(&fixture.inbox), SALTS_OK);
    }
    if (fixture.catalog) turbo_flow_plugin_catalog_snapshot_destroy(fixture.catalog);
    if (fixture.host) check_equal(turbo_flow_plugin_host_destroy(fixture.host, 1000u, &error), SALTS_OK);
    if (fixture.registry_live) {
      check_equal(adapter_component_close(&fixture.component), SALTS_OK);
      if (fixture.provider_live) {
        check_equal(cmeta_plugin_registry_request_stop(&fixture.registry, fixture.provider), CMETA_PLUGIN_OK);
        check_equal(cmeta_plugin_registry_poll_quiescent(&fixture.registry, fixture.provider, &quiet), CMETA_PLUGIN_OK);
        check_true(quiet);
        check_equal(cmeta_plugin_registry_unload(&fixture.registry, fixture.provider), CMETA_PLUGIN_OK);
      }
      check_equal(cmeta_plugin_registry_destroy(&fixture.registry), CMETA_PLUGIN_OK);
    }
  }

  it("exposes the v4 C and C++ contract and rejects old APIs before transfer") {
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_protocol_network_intake_config_t config;
    check_equal(protocol_network_intake_header_cpp_probe(), 0);
    open_fixture(0u, "2019-A1");
    config = intake_config();
    for (uint32_t version = 2u; version < 4u; ++version) {
      config.version = version;
      check_equal(
          turbo_flow_protocol_network_intake_create(&config, &fixture.flow, &intake, &error),
          SALTS_EINVAL);
      check_not_null(fixture.flow);
      check_null(intake);
      check_equal(fixture.provider_resolutions, (size_t)0u);
    }
  }

  it("rejects an incomplete decoder Message before resolving any Source provider") {
    static const char graph[] = "source wire adapter fixture.source {\n  mode 2\n}\n"
                                "stage decode adapter protocol.decode {\n  schema_version 1\n}\n"
                                "stage main {\n  wire -> decode\n}\n";
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    open_fixture(2u, "2019-A1");
    turbo_flow_destroy(fixture.flow);
    fixture.flow = turbo_flow_create();
    check_not_null(fixture.flow);
    check_equal(turbo_flow_parse_string(fixture.flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(create_intake(&error), SALTS_EPROTO);
    check_contains(error.path, "$.stages.decode");
    check_equal(fixture.provider_resolutions, (size_t)0u);
    check_not_null(fixture.flow);
    check_null(intake);
  }

  it("pins provider scope through start stop and destroy without a Source catalog entry") {
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_protocol_network_intake_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    salts_component_plugin_generation *previous = NULL;
    open_fixture(0u, "2019-A1");
    check_equal(create_intake(&error), SALTS_OK);
    check_null(fixture.flow);
    check_equal(turbo_flow_protocol_network_intake_poll(intake, 0u, &snapshot), SALTS_EBUSY);
    check_equal(turbo_flow_protocol_network_intake_destroy(intake), SALTS_EBUSY);
    turbo_flow_plugin_catalog_snapshot_destroy(fixture.catalog); fixture.catalog = NULL;
    check_equal(turbo_flow_plugin_host_destroy(fixture.host, 0u, &plugin_error), SALTS_EBUSY);
    check_equal(salts_component_plugin_scope_release(&fixture.component.component.scope), SALTS_COMPONENT_PLUGIN_OK);
    check_equal(salts_component_plugin_runtime_close(&fixture.component.component.runtime, &previous), SALTS_COMPONENT_PLUGIN_OK);
    check_equal(salts_component_plugin_generation_drain(&fixture.component.component.runtime, previous), SALTS_COMPONENT_PLUGIN_BUSY);
    check_equal(turbo_flow_protocol_network_intake_start(intake), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_poll(intake, 0u, &snapshot), SALTS_OK);
    check_equal(snapshot.source_polls, (uint64_t)1u);
    check_equal(snapshot.source_endpoint, "tcp://127.0.0.1:12345");
    check_equal(turbo_flow_protocol_network_intake_stop(intake, 1000u), SALTS_OK);
    check_equal(salts_component_plugin_generation_drain(&fixture.component.component.runtime, previous), SALTS_COMPONENT_PLUGIN_BUSY);
    check_equal(turbo_flow_protocol_network_intake_destroy(intake), SALTS_OK); intake = NULL;
    check_equal(salts_component_plugin_generation_drain(&fixture.component.component.runtime, previous), SALTS_COMPONENT_PLUGIN_OK);
  }

  it("keeps the parsed Flow caller-owned when typed provider preflight fails") {
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    open_fixture(1u, "2019-A1");
    check_equal(create_intake(&error), SALTS_EIO);
    check_not_null(fixture.flow); check_null(intake);
  }

  it("rejects unsupported protocol versions before materialization") {
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    open_fixture(2u, "2013-A1");
    check_equal(create_intake(&error), SALTS_ENOTSUP);
    check_equal(fixture.provider_resolutions, (size_t)0u);
    check_not_null(fixture.flow); check_null(intake);
  }

  it("releases the transferred Flow and scope on materializer failure") {
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    open_fixture(2u, "2019-A1");
    check_equal(create_intake(&error), SALTS_EIO);
    check_null(fixture.flow); check_null(intake);
  }

  it("counts a failing external poll and preserves the error") {
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_protocol_network_intake_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    open_fixture(3u, "2019-A1");
    check_equal(create_intake(&error), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_start(intake), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_poll(intake, 0u, &snapshot), SALTS_EIO);
    check_equal(snapshot.status, SALTS_EIO);
    check_equal(snapshot.state, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_FAILED);
    check_equal(snapshot.source_polls, (uint64_t)1u);
  }

  it("stops a compiled owner without starting it") {
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    open_fixture(4u, "2019-A1");
    check_equal(create_intake(&error), SALTS_OK);
    close_intake();
  }

  it("retains the owner and scope when drain needs retry") {
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    open_fixture(5u, "2019-A1");
    check_equal(create_intake(&error), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_start(intake), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_stop(intake, 0u), SALTS_EBUSY);
    check_equal(turbo_flow_protocol_network_intake_destroy(intake), SALTS_EBUSY);
    check_equal(turbo_flow_protocol_network_intake_stop(intake, 1000u), SALTS_OK);
  }

  it("keeps a failed start available for explicit stop and destruction") {
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    open_fixture(4u, "2019-A1");
    check_equal(create_intake(&error), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_start(intake), SALTS_EIO);
    check_equal(turbo_flow_protocol_network_intake_destroy(intake), SALTS_EBUSY);
    check_equal(turbo_flow_protocol_network_intake_stop(intake, 1000u), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_destroy(intake), SALTS_OK);
    intake = NULL;
  }

  it("retries shutdown without releasing the compiled owner early") {
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    open_fixture(6u, "2019-A1");
    check_equal(create_intake(&error), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_stop(intake, 1000u), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_destroy(intake), SALTS_EBUSY);
    check_equal(turbo_flow_protocol_network_intake_destroy(intake), SALTS_OK); intake = NULL;
  }

  it("returns a retry owner when unsupported transport rollback cannot drain") {
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    open_fixture(7u, "2019-A1");
    check_equal(create_intake(&error), SALTS_EBUSY);
    check_null(fixture.flow); check_not_null(intake);
    check_contains(error.path, "cleanup");
    check_equal(turbo_flow_protocol_network_intake_start(intake), SALTS_EBUSY);
    check_equal(turbo_flow_protocol_network_intake_stop(intake, 1000u), SALTS_OK);
    check_equal(turbo_flow_protocol_network_intake_destroy(intake), SALTS_OK); intake = NULL;
  }
}
