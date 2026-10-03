#include "tinytest.h"

#include "turbo_flow_protocol_network_intake.h"
#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_cnet_resource.h"

#include <salts/native_io.h>
#include <salts/plugin.h>

#include <stdio.h>
#include <string.h>

#ifndef FLOW_CNET_PLUGIN_MODULE
  #error FLOW_CNET_PLUGIN_MODULE is required
#endif
#ifndef FLOW_CNET_RESOURCE_FIXTURE
  #error FLOW_CNET_RESOURCE_FIXTURE is required
#endif
#ifndef FLOW_PROTOCOL_JTT808_MODULE
  #error FLOW_PROTOCOL_JTT808_MODULE is required
#endif

extern int protocol_network_intake_header_cpp_probe(void);

typedef struct resolver_fixture_s {
  salts_plugin_registry *registry;
  salts_plugin_ref provider;
  salts_plugin_ref resource;
} resolver_fixture_t;

typedef struct intake_owner_fixture_s {
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *catalog;
  turbo_flow_resolved_config_t *resolved;
  turbo_flow_inbox_t inbox;
  turbo_flow_t *flow;
  turbo_flow_t *downstream_flow;
  turbo_flow_durable_buffer_binding_t *durable_binding;

  salts_plugin_registry registry;
  int registry_initialized;
  salts_plugin_ref provider_ref;
  salts_plugin_ref resource_ref;
  int provider_loaded;
  int resource_loaded;
  int provider_started;
  int resource_started;
  resolver_fixture_t resolver;
} intake_owner_fixture_t;

static uint32_t supported_backend(void) {
  if (native_io_backend_kind_supported(NATIVE_IO_BACKEND_IOCP)) return 0u;
  if (native_io_backend_kind_supported(NATIVE_IO_BACKEND_EPOLL)) return 1u;
  if (native_io_backend_kind_supported(NATIVE_IO_BACKEND_KQUEUE)) return 2u;
  return 3u;
}

static int resolve_provider(
    void *ctx, const char *identity,
    turbo_flow_provider_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  resolver_fixture_t *fixture = (resolver_fixture_t *)ctx;
  if (!fixture || !identity || !out || out->size != sizeof(*out))
    return SALTS_EINVAL;
  if (strcmp(identity, "cnet.listener_source") != 0) {
    if (error && error->size == sizeof(*error)) error->status = SALTS_ENOENT;
    return SALTS_ENOENT;
  }
  out->module_identity = "turbo-flow.cnet";
  out->registry = fixture->registry;
  out->plugin = fixture->provider;
  return SALTS_OK;
}

static int resolve_resource(
    void *ctx, const char *name,
    const turbo_flow_provider_resource_requirement_v1_t *requirement,
    turbo_flow_resource_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  resolver_fixture_t *fixture = (resolver_fixture_t *)ctx;
  if (!fixture || !name || !requirement || !out ||
      out->size != sizeof(*out))
    return SALTS_EINVAL;
  if (strcmp(name, "listener_net") != 0 ||
      !requirement->contract_id ||
      strcmp(requirement->contract_id,
             TURBO_FLOW_CNET_DEPLOYMENT_RESOURCE_CONTRACT_ID) != 0) {
    if (error && error->size == sizeof(*error)) error->status = SALTS_ENOENT;
    return SALTS_ENOENT;
  }
  out->identity = "listener_net";
  out->registry = fixture->registry;
  out->plugin = fixture->resource;
  out->export_id = "fixture.cnet.listener";
  return SALTS_OK;
}

static void decoder_yaml(
    char *out, size_t capacity, const char *protocol_version) {
  check_true(
      snprintf(
          out, capacity,
          "version: 1\n"
          "adapters:\n"
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
          protocol_version) > 0);
}

static void intake_graph(char *out, size_t capacity) {
  check_true(
      snprintf(
          out, capacity,
          "source wire adapter cnet.listener_source {\n"
          "  resource listener_net\n"
          "  schema_version 2\n"
          "  backend %u\n"
          "  tls_enabled 0\n"
          "  connection_capacity 1\n"
          "  command_capacity 8\n"
          "  request_capacity 8\n"
          "  completion_batch_capacity 4\n"
          "  event_capacity 8\n"
          "  max_send_bytes 1024\n"
          "  receive_buffer_bytes 1024\n"
          "  connect_timeout_ms 0\n"
          "  read_timeout_ms 0\n"
          "  write_timeout_ms 0\n"
          "  tls_io_buffer_bytes 0\n"
          "  tls_handshake_timeout_ms 0\n"
          "  command_buffer_bytes 2048\n"
          "  event_buffer_bytes 2048\n"
          "  socket_receive_buffer_bytes 0\n"
          "  socket_send_buffer_bytes 0\n"
          "  keepalive 0\n"
          "  keepalive_idle_ms 0\n"
          "  keepalive_interval_ms 0\n"
          "  keepalive_count 0\n"
          "  linger 0\n"
          "  linger_ms 0\n"
          "  backlog 4\n"
          "  reuse_port 0\n"
          "  tls_client_auth 0\n"
          "  max_connections 1\n"
          "  max_message_bytes 64\n"
          "  scheduler_capacity 8\n"
          "  scheduler_max_steps_per_poll 2\n"
          "  first_message_id 1\n"
          "  initial_demand 8\n"
          "  stop_timeout_ms 1000\n"
          "  content_encoding 5\n"
          "  content_schema_version 1\n"
          "  content_media_type \"application/octet-stream\"\n"
          "  content_schema \"CNetRaw\"\n"
          "  content_type \"Bytes\"\n"
          "}\n"
          "stage decode adapter protocol.decode\n"
          "stage main {\n"
          "  wire -> decode\n"
          "}\n",
          supported_backend()) > 0);
}

static int downstream_open(intake_owner_fixture_t *fixture) {
  static const char graph[] =
      "source decoded\n"
      "buffer intake resource protocol.store\n"
      "stage main {\n"
      "  decoded -> intake\n"
      "}\n";
  turbo_flow_durable_buffer_binding_config_t binding =
      TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  int rc;

  fixture->downstream_flow = turbo_flow_create();
  if (!fixture->downstream_flow) return SALTS_ENOMEM;
  rc = turbo_flow_parse_string(
      fixture->downstream_flow, graph, sizeof(graph) - 1u);
  if (rc != SALTS_OK) return rc;
  binding.resource_name = "protocol.store";
  binding.inbox = &fixture->inbox;
  binding.identity_mode = TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED;
  binding.max_message_bytes = 4096u;
  rc = turbo_flow_durable_buffer_bind(
      fixture->downstream_flow, &binding, &fixture->durable_binding);
  if (rc == SALTS_OK) rc = turbo_flow_compile(fixture->downstream_flow);
  if (rc == SALTS_OK) rc = turbo_flow_start(fixture->downstream_flow);
  return rc;
}

static int fixture_init(
    intake_owner_fixture_t *fixture, const char *protocol_version) {
  turbo_flow_plugin_host_config_t host_config =
      TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_inbox_memory_config_t inbox_config =
      turbo_flow_inbox_memory_config_default();
  salts_plugin_registry_config registry_config = {2u};
  char yaml[4096];
  char graph[4096];
  int rc;

  memset(fixture, 0, sizeof(*fixture));
  fixture->inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;

  host_config.module_capacity = 1u;
  host_config.protocol_provider_capacity = 1u;
  rc = turbo_flow_plugin_host_create(
      &host_config, &fixture->host, &plugin_error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_host_load(
      fixture->host, FLOW_PROTOCOL_JTT808_MODULE, &plugin_error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_plugin_catalog_snapshot_create(
      fixture->host, &fixture->catalog, &plugin_error);
  if (rc != SALTS_OK) return rc;

  decoder_yaml(yaml, sizeof(yaml), protocol_version);
  rc = turbo_flow_config_resolve_yaml(
      yaml, strlen(yaml), &fixture->resolved, &config_error);
  if (rc != SALTS_OK) return rc;

  inbox_config.max_records = 8u;
  inbox_config.max_total_bytes = 8192u;
  inbox_config.max_record_bytes = 4096u;
  inbox_config.max_claims = 8u;
  rc = turbo_flow_inbox_memory_create(&inbox_config, &fixture->inbox);
  if (rc != SALTS_OK) return rc;
  rc = downstream_open(fixture);
  if (rc != SALTS_OK) return rc;

  rc = salts_plugin_registry_init(&fixture->registry, &registry_config);
  if (rc != SALTS_PLUGIN_OK) return rc;
  fixture->registry_initialized = 1;
  rc = salts_plugin_registry_load(
      &fixture->registry, FLOW_CNET_PLUGIN_MODULE, &fixture->provider_ref);
  if (rc != SALTS_PLUGIN_OK) return rc;
  fixture->provider_loaded = 1;
  rc = salts_plugin_registry_load(
      &fixture->registry, FLOW_CNET_RESOURCE_FIXTURE,
      &fixture->resource_ref);
  if (rc != SALTS_PLUGIN_OK) return rc;
  fixture->resource_loaded = 1;
  rc = salts_plugin_registry_start(
      &fixture->registry, fixture->provider_ref);
  if (rc != SALTS_PLUGIN_OK) return rc;
  fixture->provider_started = 1;
  rc = salts_plugin_registry_start(
      &fixture->registry, fixture->resource_ref);
  if (rc != SALTS_PLUGIN_OK) return rc;
  fixture->resource_started = 1;

  fixture->resolver.registry = &fixture->registry;
  fixture->resolver.provider = fixture->provider_ref;
  fixture->resolver.resource = fixture->resource_ref;

  fixture->flow = turbo_flow_create();
  if (!fixture->flow) return SALTS_ENOMEM;
  intake_graph(graph, sizeof(graph));
  return turbo_flow_parse_string(fixture->flow, graph, strlen(graph));
}

static turbo_flow_protocol_network_intake_config_t intake_config(
    intake_owner_fixture_t *fixture,
    turbo_flow_provider_resolver_v1_t *provider_resolver,
    turbo_flow_resource_resolver_v1_t *resource_resolver) {
  turbo_flow_protocol_network_intake_config_t config =
      TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT;
  *provider_resolver =
      (turbo_flow_provider_resolver_v1_t)TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
  provider_resolver->ctx = &fixture->resolver;
  provider_resolver->resolve = resolve_provider;
  *resource_resolver =
      (turbo_flow_resource_resolver_v1_t)TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
  resource_resolver->ctx = &fixture->resolver;
  resource_resolver->resolve = resolve_resource;

  config.catalog = fixture->catalog;
  config.resolved = fixture->resolved;
  config.provider_resolver = provider_resolver;
  config.resource_resolver = resource_resolver;
  config.downstream_flow = fixture->downstream_flow;
  config.source_stage_name = "wire";
  config.decoder_adapter_name = "protocol.decode";
  config.decoded_source_name = "decoded";
  return config;
}

static void fixture_cleanup(intake_owner_fixture_t *fixture) {
  turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
  bool quiescent = false;

  if (!fixture) return;
  if (fixture->flow) {
    turbo_flow_destroy(fixture->flow);
    fixture->flow = NULL;
  }
  if (fixture->downstream_flow) {
    if (turbo_flow_state(fixture->downstream_flow) ==
        TURBO_FLOW_STATE_STARTED)
      (void)turbo_flow_stop(fixture->downstream_flow);
    if (fixture->durable_binding) {
      (void)turbo_flow_durable_buffer_unbind(fixture->durable_binding);
      fixture->durable_binding = NULL;
    }
    turbo_flow_destroy(fixture->downstream_flow);
    fixture->downstream_flow = NULL;
  }
  if (fixture->resolved) {
    turbo_flow_resolved_config_destroy(fixture->resolved);
    fixture->resolved = NULL;
  }
  if (fixture->catalog) {
    turbo_flow_plugin_catalog_snapshot_destroy(fixture->catalog);
    fixture->catalog = NULL;
  }
  if (fixture->inbox.ops) {
    (void)turbo_flow_inbox_close(&fixture->inbox);
    (void)turbo_flow_inbox_destroy(&fixture->inbox);
  }
  if (fixture->host) {
    (void)turbo_flow_plugin_host_destroy(
        fixture->host, 1000u, &plugin_error);
    fixture->host = NULL;
  }

  if (fixture->registry_initialized) {
    if (fixture->provider_started) {
      (void)salts_plugin_registry_request_stop(
          &fixture->registry, fixture->provider_ref);
      fixture->provider_started = 0;
    }
    if (fixture->resource_started) {
      (void)salts_plugin_registry_request_stop(
          &fixture->registry, fixture->resource_ref);
      fixture->resource_started = 0;
    }
    if (fixture->provider_loaded &&
        salts_plugin_registry_poll_quiescent(
            &fixture->registry, fixture->provider_ref, &quiescent) ==
            SALTS_PLUGIN_OK &&
        quiescent) {
      if (salts_plugin_registry_unload(
              &fixture->registry, fixture->provider_ref) == SALTS_PLUGIN_OK)
        fixture->provider_loaded = 0;
    }
    quiescent = false;
    if (fixture->resource_loaded &&
        salts_plugin_registry_poll_quiescent(
            &fixture->registry, fixture->resource_ref, &quiescent) ==
            SALTS_PLUGIN_OK &&
        quiescent) {
      if (salts_plugin_registry_unload(
              &fixture->registry, fixture->resource_ref) == SALTS_PLUGIN_OK)
        fixture->resource_loaded = 0;
    }
    if (!fixture->provider_loaded && !fixture->resource_loaded) {
      (void)salts_plugin_registry_destroy(&fixture->registry);
      fixture->registry_initialized = 0;
    }
  }
}

spec("ProtocolNetworkIntake canonical source owner") {
  it("exposes exact C and C++ v3 ABI") {
    turbo_flow_protocol_network_intake_config_t config =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_CONFIG_INIT;
    turbo_flow_protocol_network_intake_snapshot_t snapshot =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    check_equal(config.size, sizeof(config));
    check_equal(config.version, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION);
    check_equal(snapshot.size, sizeof(snapshot));
    check_equal(snapshot.version, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_API_VERSION);
    check_equal(protocol_network_intake_header_cpp_probe(), 0);
  }

  it("owns a real canonical CNet listener Source through stop and destroy") {
    intake_owner_fixture_t fixture;
    turbo_flow_provider_resolver_v1_t provider_resolver;
    turbo_flow_resource_resolver_v1_t resource_resolver;
    turbo_flow_protocol_network_intake_config_t config;
    turbo_flow_protocol_network_intake_t *intake = NULL;
    turbo_flow_protocol_network_intake_snapshot_t snapshot =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(fixture_init(&fixture, "2019-A1"), SALTS_OK);
    config = intake_config(
        &fixture, &provider_resolver, &resource_resolver);
    check_equal(
        turbo_flow_protocol_network_intake_create(
            &config, &fixture.flow, &intake, &error),
        SALTS_OK);
    check_null(fixture.flow);
    check_not_null(intake);
    check_equal(
        turbo_flow_protocol_network_intake_start(intake), SALTS_OK);
    check_equal(
        turbo_flow_protocol_network_intake_snapshot(intake, &snapshot),
        SALTS_OK);
    check_true(strncmp(snapshot.source_endpoint, "tcp://127.0.0.1:",
                       strlen("tcp://127.0.0.1:")) == 0);
    check_equal(
        turbo_flow_protocol_network_intake_poll(intake, 0u, &snapshot),
        SALTS_OK);
    check_equal(snapshot.source_polls, (uint64_t)1u);
    check_equal(
        turbo_flow_protocol_network_intake_stop(intake, 1000u), SALTS_OK);
    check_equal(
        turbo_flow_protocol_network_intake_destroy(intake), SALTS_OK);

    fixture_cleanup(&fixture);
  }

  it("keeps caller Flow owned when decoder protocol preflight rejects") {
    intake_owner_fixture_t fixture;
    turbo_flow_provider_resolver_v1_t provider_resolver;
    turbo_flow_resource_resolver_v1_t resource_resolver;
    turbo_flow_protocol_network_intake_config_t config;
    turbo_flow_protocol_network_intake_t *intake = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(fixture_init(&fixture, "2013-A1"), SALTS_OK);
    config = intake_config(
        &fixture, &provider_resolver, &resource_resolver);
    check_equal(
        turbo_flow_protocol_network_intake_create(
            &config, &fixture.flow, &intake, &error),
        SALTS_ENOTSUP);
    check_not_null(fixture.flow);
    check_null(intake);

    fixture_cleanup(&fixture);
  }

  it("retires a compiled canonical Source before start") {
    intake_owner_fixture_t fixture;
    turbo_flow_provider_resolver_v1_t provider_resolver;
    turbo_flow_resource_resolver_v1_t resource_resolver;
    turbo_flow_protocol_network_intake_config_t config;
    turbo_flow_protocol_network_intake_t *intake = NULL;
    turbo_flow_protocol_network_intake_snapshot_t snapshot =
        TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_SNAPSHOT_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(fixture_init(&fixture, "2019-A1"), SALTS_OK);
    config = intake_config(
        &fixture, &provider_resolver, &resource_resolver);
    check_equal(
        turbo_flow_protocol_network_intake_create(
            &config, &fixture.flow, &intake, &error),
        SALTS_OK);
    check_equal(
        turbo_flow_protocol_network_intake_stop(intake, 1000u), SALTS_OK);
    check_equal(
        turbo_flow_protocol_network_intake_snapshot(intake, &snapshot),
        SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_PROTOCOL_NETWORK_INTAKE_STOPPED);
    check_equal(snapshot.source_polls, (uint64_t)0u);
    check_equal(
        turbo_flow_protocol_network_intake_destroy(intake), SALTS_OK);

    fixture_cleanup(&fixture);
  }
}
