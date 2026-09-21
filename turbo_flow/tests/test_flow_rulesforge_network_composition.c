#include "../../tests/flow_operation_fixture.h"
#include "tinytest.h"
#include "turbo_flow_plugin_generation.h"
#include "turbo_flow_rulesforge_plugin.h"

#include <cnet/cnet.h>
#include <salts/clock.h>

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef TURBO_FLOW_CNET_PLUGIN
  #error TURBO_FLOW_CNET_PLUGIN is required
#endif
#ifndef FLOW_RULESFORGE_PLUGIN
  #error FLOW_RULESFORGE_PLUGIN is required
#endif

#if defined(_WIN32)
  #define COMPOSITION_BACKEND "iocp"
#elif defined(__APPLE__)
  #define COMPOSITION_BACKEND "kqueue"
#else
  #define COMPOSITION_BACKEND "epoll"
#endif

enum {
  COMPOSITION_TIMEOUT_MS = 5000,
  COMPOSITION_MESSAGE_CAPACITY = 8,
  COMPOSITION_PAYLOAD_CAPACITY = 16
};

#define COMPOSITION_CLIENT_YAML                                                                  \
  "      backend: " COMPOSITION_BACKEND "\n"                                                   \
  "      connection_capacity: 4\n"                                                              \
  "      command_capacity: 8\n"                                                                 \
  "      request_capacity: 8\n"                                                                 \
  "      completion_batch_capacity: 8\n"                                                        \
  "      event_capacity: 8\n"                                                                   \
  "      max_send_bytes: 1024\n"                                                                \
  "      receive_buffer_bytes: 1024\n"                                                          \
  "      connect_timeout_ms: 0\n"                                                               \
  "      read_timeout_ms: 0\n"                                                                  \
  "      write_timeout_ms: 0\n"                                                                 \
  "      tls_io_buffer_bytes: 0\n"                                                              \
  "      tls_handshake_timeout_ms: 0\n"                                                         \
  "      command_buffer_bytes: 0\n"                                                             \
  "      event_buffer_bytes: 0\n"

#define COMPOSITION_SOCKET_YAML                                                                  \
  "      socket_receive_buffer_bytes: 0\n"                                                      \
  "      socket_send_buffer_bytes: 0\n"                                                         \
  "      keepalive: false\n"                                                                    \
  "      keepalive_idle_ms: 0\n"                                                                \
  "      keepalive_interval_ms: 0\n"                                                            \
  "      keepalive_count: 0\n"                                                                  \
  "      linger: false\n"                                                                       \
  "      linger_ms: 0\n"

#define COMPOSITION_DATAGRAM_YAML                                                                \
  "      backend: " COMPOSITION_BACKEND "\n"                                                   \
  "      bind_host: \"127.0.0.1\"\n"                                                          \
  "      bind_port: 0\n"                                                                        \
  "      datagram_send_capacity: 8\n"                                                           \
  "      request_capacity: 16\n"                                                                \
  "      completion_batch_capacity: 16\n"                                                       \
  "      max_datagram_bytes: 1024\n"                                                            \
  "      receive_buffer_bytes: 1024\n"                                                          \
  "      reuse_port: false\n"

#define COMPOSITION_SOURCE_TAIL_YAML                                                             \
  "      max_message_bytes: 1024\n"                                                             \
  "      scheduler_capacity: 16\n"                                                              \
  "      scheduler_max_steps_per_poll: 64\n"                                                    \
  "      first_message_id: 1\n"                                                                 \
  "      initial_demand: 8\n"                                                                   \
  "      stop_timeout_ms: 1000\n"

#define COMPOSITION_SINK_TAIL_YAML                                                               \
  "      max_message_bytes: 1024\n"                                                             \
  "      actor_command_capacity: 8\n"                                                           \
  "      actor_max_steps_per_poll: 64\n"                                                        \
  "      stop_timeout_ms: 1000\n"

typedef struct decision_probe_s {
  size_t count;
  int matched[COMPOSITION_MESSAGE_CAPACITY];
  int fired[COMPOSITION_MESSAGE_CAPACITY];
  int ages[COMPOSITION_MESSAGE_CAPACITY];
} decision_probe_t;

typedef struct datagram_probe_s {
  size_t count;
  size_t sizes[COMPOSITION_MESSAGE_CAPACITY];
  unsigned char payloads[COMPOSITION_MESSAGE_CAPACITY][COMPOSITION_PAYLOAD_CAPACITY];
} datagram_probe_t;

typedef struct tcp_probe_s {
  size_t connected;
  size_t sent;
  int failed;
} tcp_probe_t;

static native_io_backend_kind composition_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#else
  return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static int applicant_clone(const void *value, void *ctx, void **out) {
  turbo_flow_rulesforge_applicant *copy;
  (void)ctx;
  if (!value || !out) return SALTS_EINVAL;
  *out = NULL;
  copy = (turbo_flow_rulesforge_applicant *)malloc(sizeof(*copy));
  if (!copy) return SALTS_ENOMEM;
  *copy = *(const turbo_flow_rulesforge_applicant *)value;
  *out = copy;
  return SALTS_OK;
}

static void applicant_destroy(void *value, void *ctx) {
  (void)ctx;
  free(value);
}

static int normalize_applicant(turbo_flow_msg_t *message, void *ctx) {
  char text[COMPOSITION_PAYLOAD_CAPACITY];
  char *end = NULL;
  long age;
  turbo_flow_rulesforge_applicant *applicant;
  int rc;
  (void)ctx;
  if (!message || !message->payload.data || message->payload.len == 0u ||
      message->payload.len >= sizeof(text))
    return SALTS_EINVAL;
  memcpy(text, message->payload.data, message->payload.len);
  text[message->payload.len] = '\0';
  age = strtol(text, &end, 10);
  if (!end || *end != '\0' || age < INT_MIN || age > INT_MAX) return SALTS_EINVAL;
  applicant = (turbo_flow_rulesforge_applicant *)malloc(sizeof(*applicant));
  if (!applicant) return SALTS_ENOMEM;
  applicant->age = (int)age;
  rc = turbo_flow_msg_bind_typed_projection(
      message, &turbo_flow_rulesforge_applicant_schema, &turbo_flow_rulesforge_applicant_data,
      applicant, applicant_clone, applicant_destroy, NULL);
  if (rc != SALTS_OK) free(applicant);
  return rc;
}

static int verify_decision(turbo_flow_msg_t *message, void *ctx) {
  decision_probe_t *probe = (decision_probe_t *)ctx;
  const turbo_flow_data_schema_t *schema = NULL;
  const cmeta_data_desc *data = NULL;
  const turbo_flow_rulesforge_decision *decision;
  char text[COMPOSITION_PAYLOAD_CAPACITY];
  char *end = NULL;
  long age;
  if (!message || !probe || probe->count >= COMPOSITION_MESSAGE_CAPACITY ||
      !message->payload.data || message->payload.len == 0u || message->payload.len >= sizeof(text))
    return SALTS_EINVAL;
  decision = (const turbo_flow_rulesforge_decision *)turbo_flow_msg_result(message, &schema, &data);
  if (!decision ||
      turbo_flow_data_schema_match(&turbo_flow_rulesforge_decision_schema,
                                   &turbo_flow_rulesforge_decision_data, schema, data) != SALTS_OK)
    return SALTS_EPROTO;
  memcpy(text, message->payload.data, message->payload.len);
  text[message->payload.len] = '\0';
  age = strtol(text, &end, 10);
  if (!end || *end != '\0' || age < INT_MIN || age > INT_MAX) return SALTS_EPROTO;
  probe->ages[probe->count] = (int)age;
  probe->matched[probe->count] = decision->matched;
  probe->fired[probe->count] = decision->fired;
  ++probe->count;
  return SALTS_OK;
}

static void datagram_receive(void *ctx, cnet_datagram *datagram, const cnet_datagram_peer *peer,
                             const cnet_receive_view *view) {
  datagram_probe_t *probe = (datagram_probe_t *)ctx;
  size_t index;
  (void)datagram;
  (void)peer;
  if (!probe || !view || probe->count >= COMPOSITION_MESSAGE_CAPACITY ||
      view->size >= COMPOSITION_PAYLOAD_CAPACITY)
    return;
  index = probe->count++;
  probe->sizes[index] = view->size;
  if (view->size) memcpy(probe->payloads[index], view->data, view->size);
  probe->payloads[index][view->size] = '\0';
}

static void datagram_send_complete(void *ctx, cnet_datagram *datagram,
                                   const cnet_datagram_peer *peer, size_t size, int status,
                                   uint64_t tag) {
  (void)ctx;
  (void)datagram;
  (void)peer;
  (void)size;
  (void)status;
  (void)tag;
}

static cnet_datagram_peer ipv4_peer(uint16_t port) {
  cnet_datagram_peer peer = {0};
  peer.family = CNET_DATAGRAM_ADDRESS_IPV4;
  peer.port = port;
  peer.address[0] = 127u;
  peer.address[3] = 1u;
  return peer;
}

static void tcp_state(void *ctx, cnet_connection connection, cnet_connection_state state,
                      const cnet_error *error) {
  tcp_probe_t *probe = (tcp_probe_t *)ctx;
  (void)connection;
  if (!probe) return;
  if (state == CNET_CONNECTION_CONNECTED) ++probe->connected;
  if (state == CNET_CONNECTION_FAILED || error) probe->failed = 1;
}

static void tcp_receive(void *ctx, cnet_connection connection, const cnet_receive_view *view) {
  (void)ctx;
  (void)connection;
  (void)view;
}

static void tcp_send_complete(void *ctx, cnet_connection connection, size_t size) {
  tcp_probe_t *probe = (tcp_probe_t *)ctx;
  (void)connection;
  if (!probe) return;
  if (!size) probe->failed = 1;
  ++probe->sent;
}

static uint16_t endpoint_port(const char *endpoint, const char *scheme) {
  const char *colon;
  char *end = NULL;
  unsigned long value;
  if (!endpoint || !scheme || strncmp(endpoint, scheme, strlen(scheme)) != 0) return 0u;
  colon = strrchr(endpoint, ':');
  if (!colon || !colon[1]) return 0u;
  value = strtoul(colon + 1, &end, 10);
  if (!end || *end != '\0' || value == 0u || value > UINT16_MAX) return 0u;
  return (uint16_t)value;
}

static void pump_until(turbo_flow_plugin_generation_t *generation, cnet_client *tcp,
                       cnet_packet_endpoint *udp_peer, cnet_datagram *datagram,
                       const decision_probe_t *decision, const datagram_probe_t *output,
                       size_t expected) {
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  uint64_t deadline = salts_monotonic_ms() + COMPOSITION_TIMEOUT_MS;
  while ((decision->count < expected || output->count < expected) &&
         salts_monotonic_ms() < deadline) {
    size_t events = 0u;
    check_equal(cnet_client_poll(tcp, 1u, &events), SALTS_OK);
    check_equal(cnet_packet_poll(udp_peer, 1u, &events), SALTS_OK);
    check_equal(cnet_datagram_poll(datagram, 1u, &events), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_poll(generation, 1u, &error), SALTS_OK);
  }
  check_equal(decision->count, expected);
  check_equal(output->count, expected);
}

static void normalize_path(char *path) {
  if (!path) return;
  for (size_t i = 0u; path[i] != '\0'; ++i)
    if (path[i] == '\\') path[i] = '/';
}

static turbo_flow_operation_descriptor_t rulesforge_operation_metadata(void) {
  turbo_flow_operation_descriptor_t descriptor = {0};
  descriptor.size = sizeof(descriptor);
  descriptor.name = TURBO_FLOW_RULESFORGE_OPERATION;
  descriptor.version = 1u;
  descriptor.domain = TURBO_FLOW_DOMAIN_DATA;
  descriptor.input_domain = TURBO_FLOW_DOMAIN_DATA;
  descriptor.input_type = "Message";
  descriptor.output_domain = TURBO_FLOW_DOMAIN_DATA;
  descriptor.output_type = "Message";
  descriptor.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  descriptor.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  descriptor.flags = TURBO_FLOW_OPERATION_STAGE;
  descriptor.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  return descriptor;
}

spec("RulesForge real network composition") {
  it("shares one ABI3 business operation and one explicit sink across TCP and UDP sources") {
    static const char schema_text[] =
        "schema TurboFlowRules [id(73), version(1), byte_order(little)]; "
        "message Applicant { int32 age; }";
    static const char graph_text[] =
        "source tcp adapter listener.source\n"
        "source udp adapter packet.source\n"
        "stage normalize operation test.normalize_applicant\n"
        "stage rules operation rulesforge.apply resource rules.adult\n"
        "stage verify operation test.verify_decision\n"
        "stage output adapter datagram.sink\n"
        "stage main {\n"
        "  tcp -> normalize\n"
        "  udp -> normalize\n"
        "  normalize -> rules\n"
        "  rules -> verify\n"
        "  verify -> output\n"
        "}\n";
    char *schema_path = tt_make_temp_file("turbo_flow_rulesforge_composition", ".schema");
    char *rfl_path = tt_make_temp_file("turbo_flow_rulesforge_composition", ".rfl");
    char rfl[2048];
    char yaml[16384];
    char tcp_uri[96];
    const char *cnet_plugin = getenv("FLOW_CNET_PLUGIN_PATH");
    const char *rulesforge_plugin = getenv("FLOW_RULESFORGE_PLUGIN_PATH");
    turbo_flow_plugin_host_config_t host_config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_result_domain_t *result_domain = NULL;
    turbo_flow_plugin_generation_config_t generation_config =
        TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_generation_t *generation = NULL;
    turbo_flow_plugin_generation_t *cleanup_generation = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    decision_probe_t decisions = {0};
    datagram_probe_t outputs = {0};
    tcp_probe_t tcp_probe = {0};
    cnet_datagram datagram = {0};
    cnet_datagram_config datagram_config = CNET_DATAGRAM_CONFIG_INIT;
    cnet_packet_endpoint udp_peer = {0};
    cnet_packet_endpoint_config udp_config = CNET_PACKET_ENDPOINT_CONFIG_INIT;
    cnet_packet_session udp_session = {0};
    cnet_datagram_peer udp_destination;
    cnet_client tcp = {0};
    cnet_client_config tcp_config = {0};
    cnet_connect_options connect = {0};
    cnet_connection tcp_connection = {0};
    uint16_t output_port = 0u;
    uint16_t tcp_port = 0u;
    uint16_t udp_port = 0u;
    int count;

    check_not_null(schema_path);
    check_not_null(rfl_path);
    check_not_null(flow);
    normalize_path(schema_path);
    normalize_path(rfl_path);
    check_equal(tt_write_file(schema_path, schema_text, sizeof(schema_text) - 1u), 0);
    count = snprintf(rfl, sizeof(rfl),
                     "import \"%s\";\n"
                     "rule \"Adult applicant\"\n"
                     "when\n"
                     "  Applicant(age >= 18)\n"
                     "then\n"
                     "end\n",
                     schema_path);
    check_greater(count, 0);
    check_less(count, (int)sizeof(rfl));
    check_equal(tt_write_file(rfl_path, rfl, (size_t)count), 0);

    datagram_config.backend = composition_backend();
    datagram_config.host = "127.0.0.1";
    datagram_config.port = 0u;
    datagram_config.send_capacity = 8u;
    datagram_config.request_capacity = 16u;
    datagram_config.completion_batch_capacity = 16u;
    datagram_config.max_datagram_bytes = 1024u;
    datagram_config.receive_buffer_bytes = 1024u;
    datagram_config.observer.on_receive = datagram_receive;
    datagram_config.observer.on_send = datagram_send_complete;
    datagram_config.observer.user = &outputs;
    check_equal(cnet_datagram_init(&datagram, &datagram_config), SALTS_OK);
    check_equal(cnet_datagram_port(&datagram, &output_port), SALTS_OK);
    check_true(output_port != 0u);
    check_equal(cnet_datagram_receive(&datagram, COMPOSITION_MESSAGE_CAPACITY), SALTS_OK);

    count = snprintf(
        yaml, sizeof(yaml),
        "version: 1\n"
        "adapters:\n"
        "  listener.source:\n"
        "    kind: cnet.listener_source\n"
        "    config:\n"
        "      schema_version: 1\n" COMPOSITION_CLIENT_YAML
        "      bind_host: \"127.0.0.1\"\n"
        "      bind_port: 0\n"
        "      backlog: 4\n"
        "      reuse_port: false\n" COMPOSITION_SOCKET_YAML
        "      tls_enabled: false\n"
        "      tls_ca_file: \"\"\n"
        "      tls_ca_path: \"\"\n"
        "      tls_cert_file: \"\"\n"
        "      tls_key_file: \"\"\n"
        "      tls_key_password: \"\"\n"
        "      tls_client_auth: none\n"
        "      tls_alpn: []\n"
        "      max_connections: 4\n" COMPOSITION_SOURCE_TAIL_YAML
        "  packet.source:\n"
        "    kind: cnet.packet_source\n"
        "    config:\n"
        "      schema_version: 1\n" COMPOSITION_DATAGRAM_YAML
        "      packet_mode: udp\n"
        "      session_capacity: 4\n"
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
        "      queue_capacity: 8\n" COMPOSITION_SOURCE_TAIL_YAML
        "  datagram.sink:\n"
        "    kind: cnet.datagram_sink\n"
        "    config:\n"
        "      schema_version: 1\n" COMPOSITION_DATAGRAM_YAML
        "      peer_host: \"127.0.0.1\"\n"
        "      peer_port: %u\n"
        "      peer_scope_id: 0\n" COMPOSITION_SINK_TAIL_YAML
        "channels:\n"
        "  rules.adult:\n"
        "    kind: %s\n"
        "    config:\n"
        "      rfl_file: \"%s\"\n"
        "      fact_type: Applicant\n"
        "operation_bindings:\n"
        "  - operation: %s\n"
        "    resource: rules.adult\n"
        "    plugin: %s\n"
        "    version: 1\n"
        "    input_schema: %s\n"
        "    input_schema_version: 1\n"
        "    output_schema: %s\n"
        "    output_schema_version: 1\n"
        "    permissions: []\n"
        "    execution: inline\n"
        "    threading: thread_safe\n"
        "    cancellation: none\n"
        "    max_inflight: 8\n"
        "    max_input_bytes: %zu\n"
        "    max_result_bytes: %zu\n"
        "    max_retained_bytes: 64\n"
        "    max_steps: 16\n"
        "    deadline_ms: 0\n",
        (unsigned)output_port, TURBO_FLOW_RULESFORGE_RESOURCE_KIND, rfl_path,
        TURBO_FLOW_RULESFORGE_OPERATION, TURBO_FLOW_RULESFORGE_PLUGIN_ID,
        TURBO_FLOW_RULESFORGE_INPUT_SCHEMA_ID, TURBO_FLOW_RULESFORGE_OUTPUT_SCHEMA_ID,
        sizeof(turbo_flow_rulesforge_applicant), sizeof(turbo_flow_rulesforge_decision));
    check_greater(count, 0);
    check_less(count, (int)sizeof(yaml));

    if (!cnet_plugin || !cnet_plugin[0]) cnet_plugin = TURBO_FLOW_CNET_PLUGIN;
    if (!rulesforge_plugin || !rulesforge_plugin[0]) rulesforge_plugin = FLOW_RULESFORGE_PLUGIN;
    host_config.module_capacity = 2u;
    host_config.adapter_provider_capacity = 0u;
    host_config.resource_provider_capacity = 0u;
    host_config.protocol_provider_capacity = 0u;
    host_config.business_provider_capacity = 0u;
    host_config.transactional_adapter_provider_capacity = 6u;
    host_config.transactional_resource_provider_capacity = 0u;
    host_config.schema_capacity = 2u;
    host_config.operation_capacity = 1u;
    check_equal(turbo_flow_plugin_host_create(&host_config, &host, &plugin_error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, cnet_plugin, &plugin_error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, rulesforge_plugin, &plugin_error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &plugin_error), SALTS_OK);
    check_equal(turbo_flow_plugin_result_domain_create(snapshot, COMPOSITION_MESSAGE_CAPACITY,
                                                       &result_domain, &plugin_error),
                SALTS_OK);
    check_equal(turbo_flow_config_resolve_yaml(yaml, (size_t)count, &resolved, &error), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, graph_text, sizeof(graph_text) - 1u), SALTS_OK);
    turbo_flow_operation_descriptor_t rulesforge_metadata = rulesforge_operation_metadata();
    check_equal(turbo_flow_register_operation(flow, &rulesforge_metadata), SALTS_OK);
    flow_test_operation_t normalize =
        flow_test_operation_init("test.normalize_applicant", normalize_applicant, NULL);
    normalize.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    normalize.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &normalize), SALTS_OK);
    flow_test_operation_t verify =
        flow_test_operation_init("test.verify_decision", verify_decision, &decisions);
    verify.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
    verify.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
    check_equal(flow_test_operation_register(flow, &verify), SALTS_OK);
    generation_config.owner_capacity = 4u;
    check_equal(turbo_flow_plugin_generation_create(
                    snapshot, resolved, &flow, &generation_config, result_domain, &generation,
                    &cleanup_generation, &error),
                SALTS_OK);
    check_null(flow);
    check_not_null(generation);
    turbo_flow_resolved_config_destroy(resolved);
    resolved = NULL;
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);

    for (size_t i = 0u; i < turbo_flow_adapter_count(turbo_flow_plugin_generation_flow(generation));
         ++i) {
      turbo_flow_connection_snapshot_t connection;
      memset(&connection, 0, sizeof(connection));
      if (turbo_flow_adapter_connection_snapshot_at(turbo_flow_plugin_generation_flow(generation),
                                                    i, &connection) != SALTS_OK)
        continue;
      if (connection.adapter_name && strcmp(connection.adapter_name, "listener.source") == 0)
        tcp_port = endpoint_port(connection.endpoint, "tcp://");
      if (connection.adapter_name && strcmp(connection.adapter_name, "packet.source") == 0)
        udp_port = endpoint_port(connection.endpoint, "udp://");
    }
    check_true(tcp_port != 0u);
    check_true(udp_port != 0u);

    udp_config.protocol = CNET_PACKET_UDP;
    udp_config.session_capacity = 2u;
    udp_config.datagram.backend = composition_backend();
    udp_config.datagram.host = "127.0.0.1";
    udp_config.datagram.port = 0u;
    udp_config.datagram.send_capacity = 4u;
    udp_config.datagram.request_capacity = 8u;
    udp_config.datagram.completion_batch_capacity = 8u;
    udp_config.datagram.max_datagram_bytes = 1024u;
    udp_config.datagram.receive_buffer_bytes = 1024u;
    udp_config.datagram.reuse_port = false;
    check_equal(cnet_packet_endpoint_init(&udp_peer, &udp_config), SALTS_OK);
    udp_destination = ipv4_peer(udp_port);
    check_equal(cnet_packet_session_open(&udp_peer, &udp_destination, 0u, &udp_session), SALTS_OK);

    tcp_config.backend = composition_backend();
    tcp_config.connection_capacity = 2u;
    tcp_config.command_capacity = 8u;
    tcp_config.request_capacity = 8u;
    tcp_config.completion_batch_capacity = 4u;
    tcp_config.event_capacity = 8u;
    tcp_config.max_send_bytes = 256u;
    tcp_config.receive_buffer_bytes = 256u;
    check_equal(cnet_client_init(&tcp, &tcp_config), SALTS_OK);
    check_greater(snprintf(tcp_uri, sizeof(tcp_uri), "tcp://127.0.0.1:%u", (unsigned)tcp_port), 0);
    connect.uri = tcp_uri;
    connect.observer = (cnet_observer){.on_state = tcp_state,
                                       .on_receive = tcp_receive,
                                       .on_send = tcp_send_complete,
                                       .user = &tcp_probe};
    check_equal(cnet_connect(&tcp, &connect, &tcp_connection), SALTS_OK);
    {
      uint64_t deadline = salts_monotonic_ms() + COMPOSITION_TIMEOUT_MS;
      while (tcp_probe.connected == 0u && salts_monotonic_ms() < deadline) {
        size_t events = 0u;
        check_equal(cnet_client_poll(&tcp, 1u, &events), SALTS_OK);
        check_equal(turbo_flow_plugin_generation_poll(generation, 1u, &error), SALTS_OK);
      }
    }
    check_equal(tcp_probe.connected, (size_t)1u);
    check_equal(tcp_probe.failed, 0);

    check_equal(cnet_send(&tcp, tcp_connection, "21", 2u), SALTS_OK);
    pump_until(generation, &tcp, &udp_peer, &datagram, &decisions, &outputs, 1u);
    check_equal(decisions.ages[0], 21);
    check_equal(decisions.matched[0], 1);
    check_equal(decisions.fired[0], 1);
    check_equal(outputs.sizes[0], (size_t)2u);
    check_equal(memcmp(outputs.payloads[0], "21", 2u), 0);

    check_equal(cnet_packet_send(&udp_peer, udp_session, "21", 2u), SALTS_OK);
    pump_until(generation, &tcp, &udp_peer, &datagram, &decisions, &outputs, 2u);
    check_equal(decisions.ages[1], 21);
    check_equal(decisions.matched[1], decisions.matched[0]);
    check_equal(decisions.fired[1], decisions.fired[0]);
    check_equal(outputs.sizes[1], (size_t)2u);
    check_equal(memcmp(outputs.payloads[1], "21", 2u), 0);

    check_equal(cnet_send(&tcp, tcp_connection, "17", 2u), SALTS_OK);
    pump_until(generation, &tcp, &udp_peer, &datagram, &decisions, &outputs, 3u);
    check_equal(decisions.ages[2], 17);
    check_equal(decisions.matched[2], 0);
    check_equal(decisions.fired[2], 0);

    check_equal(cnet_packet_send(&udp_peer, udp_session, "17", 2u), SALTS_OK);
    pump_until(generation, &tcp, &udp_peer, &datagram, &decisions, &outputs, 4u);
    check_equal(decisions.ages[3], 17);
    check_equal(decisions.matched[3], decisions.matched[2]);
    check_equal(decisions.fired[3], decisions.fired[2]);

    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &plugin_error), SALTS_EBUSY);
    check_equal(cnet_client_stop(&tcp, COMPOSITION_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_client_destroy(&tcp), SALTS_OK);
    check_equal(cnet_packet_endpoint_stop(&udp_peer, COMPOSITION_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_packet_endpoint_destroy(&udp_peer), SALTS_OK);
    check_equal(turbo_flow_plugin_generation_destroy(generation, COMPOSITION_TIMEOUT_MS, &error),
                SALTS_OK);
    generation = NULL;
    if (cleanup_generation) {
      check_equal(turbo_flow_plugin_generation_destroy(cleanup_generation, COMPOSITION_TIMEOUT_MS,
                                                       &error),
                  SALTS_OK);
      cleanup_generation = NULL;
    }
    check_equal(turbo_flow_plugin_result_domain_destroy(result_domain, &plugin_error), SALTS_OK);
    result_domain = NULL;
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    snapshot = NULL;
    check_equal(turbo_flow_plugin_host_destroy(host, COMPOSITION_TIMEOUT_MS, &plugin_error),
                SALTS_OK);
    host = NULL;
    check_equal(cnet_datagram_stop(&datagram, COMPOSITION_TIMEOUT_MS), SALTS_OK);
    check_equal(cnet_datagram_destroy(&datagram), SALTS_OK);
    check_equal(tt_remove_file(rfl_path), 0);
    check_equal(tt_remove_file(schema_path), 0);
    free(rfl_path);
    free(schema_path);
  }
}
