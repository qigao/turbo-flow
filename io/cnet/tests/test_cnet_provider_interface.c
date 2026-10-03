#include "../../../tests/flow_operation_fixture.h"
#include "cnet_provider_config_native.h"
#include "tinytest.h"
#include "turbo_flow_cnet_resource.h"
#include "turbo_flow_provider_binding.h"
#include "turbo_flow_resource.h"

#include <salts/native_io.h>
#include <salts/plugin.h>

#include <string.h>

#ifndef FLOW_CNET_PROVIDER_FIXTURE
#error "FLOW_CNET_PROVIDER_FIXTURE is required"
#endif
#ifndef FLOW_CNET_RESOURCE_FIXTURE
#error "FLOW_CNET_RESOURCE_FIXTURE is required"
#endif

enum {
  CASE_STREAM_SOURCE = 0,
  CASE_LISTENER_SOURCE,
  CASE_PACKET_SOURCE,
  CASE_STREAM_SINK,
  CASE_DATAGRAM_SINK,
  CASE_PACKET_SINK,
  CASE_COUNT
};

typedef struct resolver_fixture_s {
  salts_plugin_registry *registry;
  salts_plugin_ref provider;
  salts_plugin_ref resource;
} resolver_fixture_t;

typedef struct acceptance_s {
  turbo_flow_t *flow;
  salts_plugin_registry registry;
  int registry_initialized;
  salts_plugin_ref provider_ref;
  salts_plugin_ref resource_ref;
  int provider_loaded;
  int resource_loaded;
  int provider_started;
  int resource_started;
  turbo_flow_provider_binding_t *provider_binding;
  turbo_flow_resource_binding_t *resource_binding;
  turbo_flow_runtime_owner owner;
  int owner_live;
  unsigned kind;
  union {
    CNetStreamSourceConfig_t stream_source;
    CNetListenerSourceConfig_t listener_source;
    CNetPacketSourceConfig_t packet_source;
    CNetStreamSinkConfig_t stream_sink;
    CNetDatagramSinkConfig_t datagram_sink;
    CNetPacketSinkConfig_t packet_sink;
  } typed;
  resolver_fixture_t resolver;
} acceptance_t;

static acceptance_t acceptance;

static uint32_t supported_backend(void) {
  if (native_io_backend_kind_supported(NATIVE_IO_BACKEND_IOCP)) return 0u;
  if (native_io_backend_kind_supported(NATIVE_IO_BACKEND_EPOLL)) return 1u;
  if (native_io_backend_kind_supported(NATIVE_IO_BACKEND_KQUEUE)) return 2u;
  return 3u;
}

#define FILL_CLIENT(CONFIG)                                                     \
  do {                                                                          \
    (CONFIG)->backend = supported_backend();                                    \
    (CONFIG)->connection_capacity = 4u;                                         \
    (CONFIG)->command_capacity = 8u;                                            \
    (CONFIG)->request_capacity = 8u;                                            \
    (CONFIG)->completion_batch_capacity = 4u;                                   \
    (CONFIG)->event_capacity = 8u;                                              \
    (CONFIG)->max_send_bytes = 8192u;                                           \
    (CONFIG)->receive_buffer_bytes = 8192u;                                     \
    (CONFIG)->connect_timeout_ms = 1000u;                                       \
    (CONFIG)->read_timeout_ms = 1000u;                                          \
    (CONFIG)->write_timeout_ms = 1000u;                                         \
    (CONFIG)->tls_io_buffer_bytes = 0u;                                         \
    (CONFIG)->tls_handshake_timeout_ms = 0u;                                    \
    (CONFIG)->command_buffer_bytes = 16384u;                                    \
    (CONFIG)->event_buffer_bytes = 16384u;                                      \
    (CONFIG)->socket_receive_buffer_bytes = 0u;                                 \
    (CONFIG)->socket_send_buffer_bytes = 0u;                                    \
    (CONFIG)->keepalive = 0u;                                                   \
    (CONFIG)->keepalive_idle_ms = 0u;                                           \
    (CONFIG)->keepalive_interval_ms = 0u;                                       \
    (CONFIG)->keepalive_count = 0u;                                             \
    (CONFIG)->linger = 0u;                                                      \
    (CONFIG)->linger_ms = 0u;                                                   \
  } while (0)

#define FILL_SOURCE_TAIL(CONFIG)                                                \
  do {                                                                          \
    (CONFIG)->max_message_bytes = 1024u;                                        \
    (CONFIG)->scheduler_capacity = 8u;                                          \
    (CONFIG)->scheduler_max_steps_per_poll = 8u;                                \
    (CONFIG)->first_message_id = 1u;                                            \
    (CONFIG)->initial_demand = 1u;                                              \
    (CONFIG)->stop_timeout_ms = 1000u;                                          \
    (CONFIG)->content_encoding = 1u;                                            \
    (CONFIG)->content_media_type = tstr_dup("application/json");               \
    (CONFIG)->content_schema = tstr_dup("Fixture");                            \
    (CONFIG)->content_type = tstr_dup("Message");                              \
    (CONFIG)->content_schema_version = 1u;                                      \
  } while (0)

#define FILL_SINK_TAIL(CONFIG)                                                  \
  do {                                                                          \
    (CONFIG)->max_message_bytes = 1024u;                                        \
    (CONFIG)->actor_command_capacity = 8u;                                      \
    (CONFIG)->actor_max_steps_per_poll = 8u;                                    \
    (CONFIG)->stop_timeout_ms = 1000u;                                          \
  } while (0)

#define FILL_DATAGRAM(CONFIG)                                                   \
  do {                                                                          \
    (CONFIG)->backend = supported_backend();                                    \
    (CONFIG)->datagram_send_capacity = 4u;                                      \
    (CONFIG)->request_capacity = 8u;                                            \
    (CONFIG)->completion_batch_capacity = 4u;                                   \
    (CONFIG)->max_datagram_bytes = 1200u;                                       \
    (CONFIG)->receive_buffer_bytes = 1200u;                                     \
    (CONFIG)->reuse_port = 0u;                                                  \
  } while (0)

#define FILL_PACKET(CONFIG)                                                     \
  do {                                                                          \
    FILL_DATAGRAM(CONFIG);                                                      \
    (CONFIG)->packet_mode = 0u;                                                 \
    (CONFIG)->session_capacity = 4u;                                            \
    (CONFIG)->kcp_mtu = 0u;                                                     \
    (CONFIG)->kcp_send_window = 0u;                                             \
    (CONFIG)->kcp_receive_window = 0u;                                          \
    (CONFIG)->kcp_interval_ms = 0u;                                             \
    (CONFIG)->kcp_fast_resend = 0u;                                             \
    (CONFIG)->kcp_no_congestion_window = 0u;                                    \
    (CONFIG)->kcp_stream_mode = 0u;                                             \
    (CONFIG)->kcp_send_segment_capacity = 0u;                                   \
    (CONFIG)->kcp_max_message_bytes = 0u;                                       \
    (CONFIG)->security_mode = 0u;                                               \
    (CONFIG)->handshake_retry_ms = 0u;                                          \
    (CONFIG)->fec_backend = 0u;                                                 \
    (CONFIG)->fec_data_shards = 0u;                                             \
    (CONFIG)->fec_parity_shards = 0u;                                           \
    (CONFIG)->fec_max_payload_bytes = 0u;                                       \
    (CONFIG)->fec_receive_group_count = 0u;                                     \
  } while (0)

static const char *provider_identity(unsigned kind) {
  static const char *const names[CASE_COUNT] = {
      "cnet.stream_source", "cnet.listener_source", "cnet.packet_source",
      "cnet.stream_sink", "cnet.datagram_sink", "cnet.packet_sink"};
  return kind < CASE_COUNT ? names[kind] : NULL;
}

static const char *resource_name(unsigned kind) {
  static const char *const names[CASE_COUNT] = {
      "stream_net", "listener_net", "packet_source_net",
      "stream_net", "datagram_net", "packet_sink_net"};
  return kind < CASE_COUNT ? names[kind] : NULL;
}

static const char *resource_export(unsigned kind) {
  static const char *const names[CASE_COUNT] = {
      "fixture.cnet.stream", "fixture.cnet.listener",
      "fixture.cnet.packet_source", "fixture.cnet.stream",
      "fixture.cnet.datagram", "fixture.cnet.packet_sink"};
  return kind < CASE_COUNT ? names[kind] : NULL;
}

static uint64_t required_capabilities(unsigned kind) {
  switch (kind) {
    case CASE_STREAM_SOURCE:
    case CASE_STREAM_SINK:
      return TURBO_FLOW_CNET_RESOURCE_CLIENT_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
             TURBO_FLOW_CNET_RESOURCE_SHAREABLE_SNAPSHOT;
    case CASE_LISTENER_SOURCE:
      return TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_TLS_MATERIAL |
             TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND;
    case CASE_PACKET_SOURCE:
      return TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL |
             TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND;
    case CASE_DATAGRAM_SINK:
      return TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND;
    case CASE_PACKET_SINK:
      return TURBO_FLOW_CNET_RESOURCE_BIND_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_PEER_ENDPOINT |
             TURBO_FLOW_CNET_RESOURCE_PSK_MATERIAL |
             TURBO_FLOW_CNET_RESOURCE_EXCLUSIVE_BIND;
    default:
      return 0u;
  }
}

static const DataBindMessageNativeArtifact *expected_artifact(unsigned kind) {
  switch (kind) {
    case CASE_STREAM_SOURCE: return CNetStreamSourceConfig_native_artifact();
    case CASE_LISTENER_SOURCE: return CNetListenerSourceConfig_native_artifact();
    case CASE_PACKET_SOURCE: return CNetPacketSourceConfig_native_artifact();
    case CASE_STREAM_SINK: return CNetStreamSinkConfig_native_artifact();
    case CASE_DATAGRAM_SINK: return CNetDatagramSinkConfig_native_artifact();
    case CASE_PACKET_SINK: return CNetPacketSinkConfig_native_artifact();
    default: return NULL;
  }
}

static size_t typed_size(unsigned kind) {
  switch (kind) {
    case CASE_STREAM_SOURCE: return sizeof(CNetStreamSourceConfig_t);
    case CASE_LISTENER_SOURCE: return sizeof(CNetListenerSourceConfig_t);
    case CASE_PACKET_SOURCE: return sizeof(CNetPacketSourceConfig_t);
    case CASE_STREAM_SINK: return sizeof(CNetStreamSinkConfig_t);
    case CASE_DATAGRAM_SINK: return sizeof(CNetDatagramSinkConfig_t);
    case CASE_PACKET_SINK: return sizeof(CNetPacketSinkConfig_t);
    default: return 0u;
  }
}

static void *typed_value(unsigned kind) {
  switch (kind) {
    case CASE_STREAM_SOURCE: return &acceptance.typed.stream_source;
    case CASE_LISTENER_SOURCE: return &acceptance.typed.listener_source;
    case CASE_PACKET_SOURCE: return &acceptance.typed.packet_source;
    case CASE_STREAM_SINK: return &acceptance.typed.stream_sink;
    case CASE_DATAGRAM_SINK: return &acceptance.typed.datagram_sink;
    case CASE_PACKET_SINK: return &acceptance.typed.packet_sink;
    default: return NULL;
  }
}

static void fill_typed(unsigned kind) {
  acceptance.kind = kind;
  switch (kind) {
    case CASE_STREAM_SOURCE:
      CNetStreamSourceConfig_init(&acceptance.typed.stream_source);
      acceptance.typed.stream_source.schema_version = 2u;
      FILL_CLIENT(&acceptance.typed.stream_source);
      FILL_SOURCE_TAIL(&acceptance.typed.stream_source);
      break;
    case CASE_LISTENER_SOURCE:
      CNetListenerSourceConfig_init(&acceptance.typed.listener_source);
      acceptance.typed.listener_source.schema_version = 2u;
      acceptance.typed.listener_source.tls_enabled = 0u;
      FILL_CLIENT(&acceptance.typed.listener_source);
      acceptance.typed.listener_source.backlog = 16u;
      acceptance.typed.listener_source.reuse_port = 0u;
      acceptance.typed.listener_source.tls_client_auth = 0u;
      acceptance.typed.listener_source.max_connections = 4u;
      FILL_SOURCE_TAIL(&acceptance.typed.listener_source);
      break;
    case CASE_PACKET_SOURCE:
      CNetPacketSourceConfig_init(&acceptance.typed.packet_source);
      acceptance.typed.packet_source.schema_version = 2u;
      FILL_PACKET(&acceptance.typed.packet_source);
      acceptance.typed.packet_source.queue_capacity = 8u;
      FILL_SOURCE_TAIL(&acceptance.typed.packet_source);
      break;
    case CASE_STREAM_SINK:
      CNetStreamSinkConfig_init(&acceptance.typed.stream_sink);
      acceptance.typed.stream_sink.schema_version = 2u;
      FILL_CLIENT(&acceptance.typed.stream_sink);
      FILL_SINK_TAIL(&acceptance.typed.stream_sink);
      break;
    case CASE_DATAGRAM_SINK:
      CNetDatagramSinkConfig_init(&acceptance.typed.datagram_sink);
      acceptance.typed.datagram_sink.schema_version = 2u;
      FILL_DATAGRAM(&acceptance.typed.datagram_sink);
      FILL_SINK_TAIL(&acceptance.typed.datagram_sink);
      break;
    case CASE_PACKET_SINK:
      CNetPacketSinkConfig_init(&acceptance.typed.packet_sink);
      acceptance.typed.packet_sink.schema_version = 2u;
      FILL_PACKET(&acceptance.typed.packet_sink);
      acceptance.typed.packet_sink.conversation = 0u;
      acceptance.typed.packet_sink.adapter_send_capacity = 4u;
      FILL_SINK_TAIL(&acceptance.typed.packet_sink);
      break;
  }
}

static void clear_typed(void) {
  switch (acceptance.kind) {
    case CASE_STREAM_SOURCE:
      CNetStreamSourceConfig_clear(&acceptance.typed.stream_source); break;
    case CASE_LISTENER_SOURCE:
      CNetListenerSourceConfig_clear(&acceptance.typed.listener_source); break;
    case CASE_PACKET_SOURCE:
      CNetPacketSourceConfig_clear(&acceptance.typed.packet_source); break;
    case CASE_STREAM_SINK:
      CNetStreamSinkConfig_clear(&acceptance.typed.stream_sink); break;
    case CASE_DATAGRAM_SINK:
      CNetDatagramSinkConfig_clear(&acceptance.typed.datagram_sink); break;
    case CASE_PACKET_SINK:
      CNetPacketSinkConfig_clear(&acceptance.typed.packet_sink); break;
    default:
      break;
  }
  acceptance.kind = CASE_COUNT;
}

static int output(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  (void)ctx;
  return SALTS_OK;
}

static int resolve_provider(
    void *ctx, const char *identity,
    turbo_flow_provider_candidate_v1_t *out,
    turbo_flow_config_error_t *error) {
  resolver_fixture_t *fixture = (resolver_fixture_t *)ctx;
  int known = 0;
  if (!fixture || !out || out->size != sizeof(*out)) return SALTS_EINVAL;
  for (unsigned i = 0u; i < CASE_COUNT; ++i)
    if (identity && strcmp(identity, provider_identity(i)) == 0) known = 1;
  if (!known) {
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
  unsigned kind = CASE_COUNT;
  if (!fixture || !requirement || !out || out->size != sizeof(*out))
    return SALTS_EINVAL;
  for (unsigned i = 0u; i < CASE_COUNT; ++i)
    if (name && strcmp(name, resource_name(i)) == 0) {
      kind = i;
      break;
    }
  if (kind == CASE_COUNT || !requirement->contract_id ||
      strcmp(requirement->contract_id,
             TURBO_FLOW_CNET_DEPLOYMENT_RESOURCE_CONTRACT_ID) != 0) {
    if (error && error->size == sizeof(*error)) error->status = SALTS_ENOENT;
    return SALTS_ENOENT;
  }
  out->identity = name;
  out->registry = fixture->registry;
  out->plugin = fixture->resource;
  out->export_id = resource_export(kind);
  return SALTS_OK;
}

static int setup_registry(void) {
  salts_plugin_registry_config config = {2u};
  int rc;
  rc = salts_plugin_registry_init(&acceptance.registry, &config);
  if (rc != SALTS_PLUGIN_OK) return rc;
  acceptance.registry_initialized = 1;
  rc = salts_plugin_registry_load(
      &acceptance.registry, FLOW_CNET_PROVIDER_FIXTURE,
      &acceptance.provider_ref);
  if (rc != SALTS_PLUGIN_OK) return rc;
  acceptance.provider_loaded = 1;
  rc = salts_plugin_registry_load(
      &acceptance.registry, FLOW_CNET_RESOURCE_FIXTURE,
      &acceptance.resource_ref);
  if (rc != SALTS_PLUGIN_OK) return rc;
  acceptance.resource_loaded = 1;
  rc = salts_plugin_registry_start(
      &acceptance.registry, acceptance.provider_ref);
  if (rc != SALTS_PLUGIN_OK) return rc;
  acceptance.provider_started = 1;
  rc = salts_plugin_registry_start(
      &acceptance.registry, acceptance.resource_ref);
  if (rc != SALTS_PLUGIN_OK) return rc;
  acceptance.resource_started = 1;
  acceptance.resolver.registry = &acceptance.registry;
  acceptance.resolver.provider = acceptance.provider_ref;
  acceptance.resolver.resource = acceptance.resource_ref;
  return SALTS_OK;
}

static void cleanup(void) {
  if (acceptance.owner_live &&
      turbo_flow_runtime_owner_contract_valid(&acceptance.owner)) {
    (void)turbo_flow_runtime_owner_quiesce(&acceptance.owner, 10u);
    (void)turbo_flow_runtime_owner_drain(&acceptance.owner, 10u);
    (void)turbo_flow_runtime_owner_shutdown(&acceptance.owner);
  }
  if (acceptance.flow) {
    turbo_flow_destroy(acceptance.flow);
    acceptance.flow = NULL;
  }
  if (acceptance.owner_live &&
      turbo_flow_runtime_owner_contract_valid(&acceptance.owner)) {
    turbo_flow_runtime_owner_destroy(&acceptance.owner);
    memset(&acceptance.owner, 0, sizeof(acceptance.owner));
    acceptance.owner_live = 0;
  }
  if (acceptance.resource_binding)
    (void)turbo_flow_resource_binding_release(&acceptance.resource_binding);
  if (acceptance.provider_binding)
    (void)turbo_flow_provider_binding_release(&acceptance.provider_binding);
  clear_typed();

  if (acceptance.registry_initialized) {
    bool quiescent = false;
    if (acceptance.provider_started) {
      (void)salts_plugin_registry_request_stop(
          &acceptance.registry, acceptance.provider_ref);
      acceptance.provider_started = 0;
    }
    if (acceptance.resource_started) {
      (void)salts_plugin_registry_request_stop(
          &acceptance.registry, acceptance.resource_ref);
      acceptance.resource_started = 0;
    }
    if (acceptance.provider_loaded &&
        salts_plugin_registry_poll_quiescent(
            &acceptance.registry, acceptance.provider_ref,
            &quiescent) == SALTS_PLUGIN_OK &&
        quiescent) {
      if (salts_plugin_registry_unload(
              &acceptance.registry, acceptance.provider_ref) ==
          SALTS_PLUGIN_OK)
        acceptance.provider_loaded = 0;
    }
    quiescent = false;
    if (acceptance.resource_loaded &&
        salts_plugin_registry_poll_quiescent(
            &acceptance.registry, acceptance.resource_ref,
            &quiescent) == SALTS_PLUGIN_OK &&
        quiescent) {
      if (salts_plugin_registry_unload(
              &acceptance.registry, acceptance.resource_ref) ==
          SALTS_PLUGIN_OK)
        acceptance.resource_loaded = 0;
    }
    if (!acceptance.provider_loaded && !acceptance.resource_loaded) {
      (void)salts_plugin_registry_destroy(&acceptance.registry);
      acceptance.registry_initialized = 0;
    }
  }
}

static int bind_view(
    const DataBindMessageNativeArtifact *artifact, const void *value,
    size_t value_bytes, turbo_flow_provider_config_view_v1_t *view) {
  DataBindNativeTypeBinding binding =
      DATA_BIND_NATIVE_TYPE_BINDING_INIT(NULL, NULL);
  DataBindError error = DATA_BIND_ERROR_INIT;
  if (!artifact || !value || !view) return SALTS_EINVAL;
  if (artifact->native_binding(&binding, &error) != DATA_BIND_OK)
    return SALTS_EPROTO;
  *view =
      (turbo_flow_provider_config_view_v1_t)
          TURBO_FLOW_PROVIDER_CONFIG_VIEW_V1_INIT;
  view->type_name = artifact->type_name;
  view->data = binding.data;
  view->value = value;
  view->value_bytes = value_bytes;
  return SALTS_OK;
}

static int prepare_bindings(
    unsigned kind, turbo_flow_provider_contract_v1_t *contract,
    turbo_flow_provider_resource_view_v1_t *resource_view,
    turbo_flow_config_error_t *error) {
  turbo_flow_provider_resolver_v1_t provider_resolver =
      TURBO_FLOW_PROVIDER_RESOLVER_V1_INIT;
  turbo_flow_resource_resolver_v1_t resource_resolver =
      TURBO_FLOW_RESOURCE_RESOLVER_V1_INIT;
  int rc;
  provider_resolver.ctx = &acceptance.resolver;
  provider_resolver.resolve = resolve_provider;
  resource_resolver.ctx = &acceptance.resolver;
  resource_resolver.resolve = resolve_resource;

  rc = turbo_flow_provider_binding_acquire(
      &provider_resolver, provider_identity(kind),
      &acceptance.provider_binding, error);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_provider_binding_contract(
      acceptance.provider_binding, contract);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_resource_binding_acquire(
      &resource_resolver, resource_name(kind), &contract->resource,
      &acceptance.resource_binding, error);
  if (rc != SALTS_OK) return rc;
  return turbo_flow_resource_binding_view(
      acceptance.resource_binding, resource_view);
}

static int run_case(unsigned kind) {
  turbo_flow_provider_contract_v1_t contract =
      TURBO_FLOW_PROVIDER_CONTRACT_V1_INIT;
  turbo_flow_provider_resource_view_v1_t resource_view =
      TURBO_FLOW_PROVIDER_RESOURCE_VIEW_V1_INIT;
  turbo_flow_provider_instance_v1_t instance =
      TURBO_FLOW_PROVIDER_INSTANCE_V1_INIT;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  flow_test_operation_t operation =
      flow_test_operation_init("test.output", output, NULL);
  const DataBindMessageNativeArtifact *artifact = expected_artifact(kind);
  char src[1024];
  int rc;

  if (kind <= CASE_PACKET_SOURCE) {
    (void)snprintf(
        src, sizeof(src),
        "source io adapter %s {\n"
        "  resource %s\n"
        "}\n"
        "stage output operation test.output\n"
        "stage main {\n"
        "  io -> output\n"
        "}\n",
        provider_identity(kind), resource_name(kind));
  } else {
    (void)snprintf(
        src, sizeof(src),
        "source input\n"
        "stage io adapter %s {\n"
        "  resource %s\n"
        "}\n"
        "stage main {\n"
        "  input -> io\n"
        "}\n",
        provider_identity(kind), resource_name(kind));
  }

  acceptance.flow = turbo_flow_create();
  if (!acceptance.flow) return SALTS_ENOMEM;
  if (kind <= CASE_PACKET_SOURCE) {
    rc = flow_test_operation_register(acceptance.flow, &operation);
    if (rc != SALTS_OK) return rc;
  }
  rc = turbo_flow_parse_string(acceptance.flow, src, strlen(src));
  if (rc != SALTS_OK) return rc;

  fill_typed(kind);
  rc = prepare_bindings(kind, &contract, &resource_view, &error);
  if (rc != SALTS_OK) return rc;
  if (!artifact || !contract.config.message_artifact ||
      strcmp(contract.config.message_artifact->type_name,
             artifact->type_name) != 0 ||
      strcmp(contract.resource.contract_id,
             TURBO_FLOW_CNET_DEPLOYMENT_RESOURCE_CONTRACT_ID) != 0 ||
      contract.resource.required_capabilities !=
          required_capabilities(kind))
    return SALTS_EPROTO;

  instance.instance_name = "io";
  rc = bind_view(
      contract.config.message_artifact, typed_value(kind), typed_size(kind),
      &instance.config);
  if (rc != SALTS_OK) return rc;
  instance.resource = &resource_view;

  rc = turbo_flow_provider_binding_preflight(
      acceptance.provider_binding, &instance, &error);
  if (rc != SALTS_OK) {
    info("CNet preflight kind=%u status=%d path=%s reason=%s",
         kind, rc, error.path, error.message);
    return rc;
  }
  rc = turbo_flow_provider_binding_materialize(
      acceptance.provider_binding, acceptance.flow, &instance,
      &acceptance.owner, &error);
  if (rc != SALTS_OK) {
    info("CNet materialize kind=%u status=%d path=%s reason=%s",
         kind, rc, error.path, error.message);
    return rc;
  }
  acceptance.owner_live = 1;
  if (!turbo_flow_runtime_owner_contract_valid(&acceptance.owner))
    return SALTS_EPROTO;
  rc = turbo_flow_compile(acceptance.flow);
  if (rc != SALTS_OK) return rc;

  if (kind == CASE_LISTENER_SOURCE || kind == CASE_PACKET_SOURCE) {
    rc = turbo_flow_transport_reply_supported_stage(
        acceptance.flow, "io");
    if (rc != SALTS_OK) return rc;
    if (turbo_flow_transport_reply_supported(
            acceptance.flow, provider_identity(kind)) != SALTS_ENOTSUP)
      return SALTS_EPROTO;
  }

  return SALTS_OK;
}

spec("CNet canonical Salts providers") {
  before_each() {
    memset(&acceptance, 0, sizeof(acceptance));
    acceptance.kind = CASE_COUNT;
  }
  after_each() { cleanup(); }

  it("materializes stream source") {
    check_equal(setup_registry(), SALTS_PLUGIN_OK);
    check_equal(run_case(CASE_STREAM_SOURCE), SALTS_OK);
  }

  it("materializes listener source with exact-stage reply capability") {
    check_equal(setup_registry(), SALTS_PLUGIN_OK);
    check_equal(run_case(CASE_LISTENER_SOURCE), SALTS_OK);
  }

  it("materializes packet source with exact-stage reply capability") {
    check_equal(setup_registry(), SALTS_PLUGIN_OK);
    check_equal(run_case(CASE_PACKET_SOURCE), SALTS_OK);
  }

  it("materializes stream sink") {
    check_equal(setup_registry(), SALTS_PLUGIN_OK);
    check_equal(run_case(CASE_STREAM_SINK), SALTS_OK);
  }

  it("materializes datagram sink") {
    check_equal(setup_registry(), SALTS_PLUGIN_OK);
    check_equal(run_case(CASE_DATAGRAM_SINK), SALTS_OK);
  }

  it("materializes packet sink") {
    check_equal(setup_registry(), SALTS_PLUGIN_OK);
    check_equal(run_case(CASE_PACKET_SINK), SALTS_OK);
  }
}

#undef FILL_PACKET
#undef FILL_DATAGRAM
#undef FILL_SINK_TAIL
#undef FILL_SOURCE_TAIL
#undef FILL_CLIENT
