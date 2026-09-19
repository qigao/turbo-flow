#include "tinytest.h"

#include "flow_protocol_network_intake_internal.h"

#include <string.h>

static const char listener_yaml[] =
    "version: 1\n"
    "adapters:\n"
    "  tcp.input:\n"
    "    kind: cnet.listener_source\n"
    "    config:\n"
    "      max_connections: 4\n"
    "      max_message_bytes: 1024\n"
    "      scheduler_max_steps_per_poll: 32\n"
    "  protocol.decode:\n"
    "    kind: protocol.decode\n"
    "    config:\n"
    "      schema_version: 2\n"
    "      protocol_provider: jtt808\n"
    "      protocol_kind: jtt808\n"
    "      protocol_version: 2019-A1\n"
    "      source_id: fleet.primary\n"
    "      max_sessions: 4\n"
    "      max_frame_size: 1024\n"
    "      max_pending_claims: 64\n"
    "      max_pending_bytes: 65536\n";

static const char packet_yaml[] =
    "version: 1\n"
    "adapters:\n"
    "  udp.input:\n"
    "    kind: cnet.packet_source\n"
    "    config:\n"
    "      packet_mode: udp\n"
    "      session_capacity: 4\n"
    "      max_message_bytes: 1024\n"
    "      scheduler_max_steps_per_poll: 32\n"
    "  protocol.decode:\n"
    "    kind: protocol.decode\n"
    "    config:\n"
    "      schema_version: 2\n"
    "      protocol_provider: coap\n"
    "      protocol_kind: coap\n"
    "      protocol_version: RFC7252\n"
    "      source_id: coap.primary\n"
    "      max_sessions: 4\n"
    "      max_frame_size: 1024\n"
    "      max_pending_claims: 64\n"
    "      max_pending_bytes: 65536\n";

static const char listener_graph[] =
    "source wire adapter tcp.input\n"
    "stage decode adapter protocol.decode\n"
    "stage main {\n"
    "  wire -> decode\n"
    "}\n";

static const char packet_graph[] =
    "source wire adapter udp.input\n"
    "stage decode adapter protocol.decode\n"
    "stage main {\n"
    "  wire -> decode\n"
    "}\n";

static int replace_once(const char *input, const char *needle, const char *replacement,
                        char *output, size_t capacity) {
  const char *match;
  size_t prefix;
  size_t suffix;
  size_t replacement_size;
  if (!input || !needle || !needle[0] || !replacement || !output || capacity == 0u)
    return SALTS_EINVAL;
  match = strstr(input, needle);
  if (!match) return SALTS_ENOENT;
  prefix = (size_t)(match - input);
  suffix = strlen(match + strlen(needle));
  replacement_size = strlen(replacement);
  if (prefix > SIZE_MAX - replacement_size || prefix + replacement_size > SIZE_MAX - suffix ||
      prefix + replacement_size + suffix >= capacity)
    return SALTS_ENOSPC;
  memcpy(output, input, prefix);
  memcpy(output + prefix, replacement, replacement_size);
  memcpy(output + prefix + replacement_size, match + strlen(needle), suffix + 1u);
  return SALTS_OK;
}

static turbo_flow_t *parsed_flow(const char *graph) {
  turbo_flow_t *flow = turbo_flow_create();
  check_not_null(flow);
  check_equal(turbo_flow_parse_string(flow, graph, strlen(graph)), SALTS_OK);
  return flow;
}

static turbo_flow_resolved_config_t *resolved_yaml(const char *yaml) {
  turbo_flow_resolved_config_t *resolved = NULL;
  turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
  check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &resolved, &error), SALTS_OK);
  check_not_null(resolved);
  return resolved;
}

static int run_preflight(const char *yaml, const char *graph, const char *source_name,
                         flow_protocol_network_intake_settings_t *settings,
                         turbo_flow_config_error_t *error) {
  turbo_flow_resolved_config_t *resolved = resolved_yaml(yaml);
  turbo_flow_t *flow = parsed_flow(graph);
  int rc = flow_protocol_network_intake_preflight(resolved, flow, source_name, "protocol.decode",
                                                  settings, error);
  turbo_flow_destroy(flow);
  turbo_flow_resolved_config_destroy(resolved);
  return rc;
}

spec("protocol network intake configuration") {
  it("accepts exact JT/T808 listener and CoAP UDP pairings") {
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    memset(&settings, 0, sizeof(settings));
    check_equal(run_preflight(listener_yaml, listener_graph, "tcp.input", &settings, &error),
                SALTS_OK);
    check_equal(settings.protocol_kind, TURBO_FLOW_PROTOCOL_JTT_808);
    check_equal(settings.transport_kind, FLOW_PROTOCOL_NETWORK_TRANSPORT_LISTENER_TCP);
    check_equal(strcmp(settings.protocol_provider, "jtt808"), 0);
    check_equal(strcmp(settings.protocol_version, "2019-A1"), 0);
    check_equal(strcmp(settings.source_id, "fleet.primary"), 0);
    check_equal(settings.max_sessions, 4u);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    memset(&settings, 0, sizeof(settings));
    check_equal(run_preflight(packet_yaml, packet_graph, "udp.input", &settings, &error), SALTS_OK);
    check_equal(settings.protocol_kind, TURBO_FLOW_PROTOCOL_COAP);
    check_equal(settings.transport_kind, FLOW_PROTOCOL_NETWORK_TRANSPORT_PACKET_UDP);
    check_equal(strcmp(settings.protocol_provider, "coap"), 0);
    check_equal(strcmp(settings.protocol_version, "RFC7252"), 0);
  }

  it("rejects unknown and missing intake fields") {
    char yaml[4096];
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(replace_once(listener_yaml, "      max_pending_bytes: 65536\n",
                             "      max_pending_bytes: 65536\n      unexpected: 1\n", yaml,
                             sizeof(yaml)),
                SALTS_OK);
    check_equal(run_preflight(yaml, listener_graph, "tcp.input", &settings, &error), SALTS_EINVAL);
    check_contains(error.path, "unexpected");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(listener_yaml, "      source_id: fleet.primary\n", "", yaml,
                             sizeof(yaml)),
                SALTS_OK);
    check_equal(run_preflight(yaml, listener_graph, "tcp.input", &settings, &error), SALTS_EINVAL);
  }

  it("rejects unsupported pairings versions and KCP") {
    char yaml[4096];
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(replace_once(listener_yaml, "      protocol_provider: jtt808\n",
                             "      protocol_provider: coap\n", yaml, sizeof(yaml)), SALTS_OK);
    check_equal(run_preflight(yaml, listener_graph, "tcp.input", &settings, &error), SALTS_EINVAL);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(listener_yaml, "      protocol_version: 2019-A1\n",
                             "      protocol_version: 2013-A1\n", yaml, sizeof(yaml)), SALTS_OK);
    check_equal(run_preflight(yaml, listener_graph, "tcp.input", &settings, &error), SALTS_ENOTSUP);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(packet_yaml, "      packet_mode: udp\n", "      packet_mode: kcp\n",
                             yaml, sizeof(yaml)), SALTS_OK);
    check_equal(run_preflight(yaml, packet_graph, "udp.input", &settings, &error), SALTS_ENOTSUP);
  }

  it("rejects zero mismatched and insufficient bounds") {
    char yaml[4096];
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(replace_once(listener_yaml, "      max_sessions: 4\n", "      max_sessions: 0\n",
                             yaml, sizeof(yaml)), SALTS_OK);
    check_equal(run_preflight(yaml, listener_graph, "tcp.input", &settings, &error), SALTS_ERANGE);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(listener_yaml, "      max_sessions: 4\n", "      max_sessions: 3\n",
                             yaml, sizeof(yaml)), SALTS_OK);
    check_equal(run_preflight(yaml, listener_graph, "tcp.input", &settings, &error), SALTS_ERANGE);
    check_contains(error.path, "max_sessions");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(listener_yaml, "      max_sessions: 4\n", "      max_sessions: 5\n",
                             yaml, sizeof(yaml)), SALTS_OK);
    check_equal(run_preflight(yaml, listener_graph, "tcp.input", &settings, &error), SALTS_ERANGE);
    check_contains(error.path, "max_sessions");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(listener_yaml, "      max_pending_claims: 64\n",
                             "      max_pending_claims: 63\n", yaml, sizeof(yaml)), SALTS_OK);
    check_equal(run_preflight(yaml, listener_graph, "tcp.input", &settings, &error), SALTS_ERANGE);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(listener_yaml, "      max_pending_bytes: 65536\n",
                             "      max_pending_bytes: 65535\n", yaml, sizeof(yaml)), SALTS_OK);
    check_equal(run_preflight(yaml, listener_graph, "tcp.input", &settings, &error), SALTS_ERANGE);
  }

  it("rejects wrong adapter names non-parsed flows and non-two-stage topology") {
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_config_t *resolved = resolved_yaml(listener_yaml);
    turbo_flow_t *flow = parsed_flow(listener_graph);
    static const char extra_graph[] =
        "source wire adapter tcp.input\n"
        "stage middle\n"
        "stage decode adapter protocol.decode\n"
        "stage main {\n"
        "  wire -> middle -> decode\n"
        "}\n";
    turbo_flow_t *extra = parsed_flow(extra_graph);
    turbo_flow_t *unparsed = turbo_flow_create();

    check_equal(flow_protocol_network_intake_preflight(resolved, flow, "missing", "protocol.decode",
                                                       &settings, &error),
                SALTS_EINVAL);
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(flow_protocol_network_intake_preflight(resolved, extra, "tcp.input",
                                                       "protocol.decode", &settings, &error),
                SALTS_EINVAL);
    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(flow_protocol_network_intake_preflight(resolved, unparsed, "tcp.input",
                                                       "protocol.decode", &settings, &error),
                SALTS_EINVAL);

    turbo_flow_destroy(unparsed);
    turbo_flow_destroy(extra);
    turbo_flow_destroy(flow);
    turbo_flow_resolved_config_destroy(resolved);
  }
}
