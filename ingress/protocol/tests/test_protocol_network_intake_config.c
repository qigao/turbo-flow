#include "tinytest.h"

#include "flow_protocol_network_intake_internal.h"

#include <string.h>

#define DECODER_LIMITS                                                                             \
  "  max_sessions 4\n"                                                                             \
  "  max_frame_size 1024\n"                                                                        \
  "  max_pending_claims 64\n"                                                                      \
  "  max_pending_bytes 65536\n"
#define COAP_DECODER_FIELDS                                                                        \
  "  schema_version 1\n"                                                                           \
  "  protocol_provider \"coap\"\n"                                                                 \
  "  protocol_kind \"coap\"\n"                                                                     \
  "  protocol_version \"RFC7252\"\n"                                                               \
  "  source_id \"coap.primary\"\n" DECODER_LIMITS
static const char listener_config[] = "stage decode adapter protocol.decode {\n"
                                      "  schema_version 1\n"
                                      "  protocol_provider \"jtt808\"\n"
                                      "  protocol_kind \"jtt808\"\n"
                                      "  protocol_version \"2019-A1\"\n"
                                      "  source_id \"fleet.primary\"\n" DECODER_LIMITS "}\n";
static const char packet_config[] =
    "stage decode adapter protocol.decode {\n" COAP_DECODER_FIELDS "}\n";
static const char mapped_packet_config[] =
    "stage decode adapter protocol.decode {\n" COAP_DECODER_FIELDS
    "  mapper { plugin \"applicant.mapper\", name \"applicant\", profile \"applicant-json\", "
    "message_type 2, semantic_type 50, semantic_media_type \"application/json\", "
    "max_semantic_bytes 1024, max_output_bytes 2048 }\n"
    "}\n";

static const char listener_graph[] =
    "source wire adapter cnet.listener_source\n"
    "stage decode adapter protocol.decode\n"
    "stage main {\n"
    "  wire -> decode\n"
    "}\n";

static const char packet_graph[] =
    "source wire adapter cnet.packet_source\n"
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

static int run_named_preflight(const char *config, const char *graph, const char *source_name,
                               const char *decoder_name, int parse,
                               flow_protocol_network_intake_settings_t *settings,
                               turbo_flow_config_error_t *error) {
  char source[8192];
  turbo_flow_t *flow = turbo_flow_create();
  turbo_flow_source_transport_contract_t transport = TURBO_FLOW_SOURCE_TRANSPORT_CONTRACT_INIT;
  int rc;
  if (!flow) return SALTS_ENOMEM;
  rc =
      replace_once(graph, "stage decode adapter protocol.decode\n", config, source, sizeof(source));
  if (rc == SALTS_OK && parse) rc = turbo_flow_parse_string(flow, source, strlen(source));
  if (rc == SALTS_OK)
    rc = flow_protocol_network_intake_preflight(flow, "wire", decoder_name, settings, error);
  transport.transport = strcmp(source_name, "tcp.input") == 0
      ? TURBO_FLOW_SOURCE_TRANSPORT_TCP_LISTENER
      : (strcmp(source_name, "kcp.input") == 0 ? TURBO_FLOW_SOURCE_TRANSPORT_KCP_PACKET
                                              : TURBO_FLOW_SOURCE_TRANSPORT_UDP_PACKET);
  transport.session_capacity = 4u;
  transport.max_message_bytes = 1024u;
  transport.max_messages_per_poll = 64u;
  if (rc == SALTS_OK) rc = flow_protocol_network_intake_transport_bind(&transport, settings, error);
  turbo_flow_destroy(flow);
  return rc;
}

static int run_preflight(const char *config, const char *graph, const char *source_name,
                         flow_protocol_network_intake_settings_t *settings,
                         turbo_flow_config_error_t *error) {
  return run_named_preflight(config, graph, source_name, "decode", 1, settings, error);
}

spec("protocol network intake configuration") {
  it("accepts exact JT/T808 listener and CoAP UDP pairings") {
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    memset(&settings, 0, sizeof(settings));
    check_equal(run_preflight(listener_config, listener_graph, "tcp.input", &settings, &error),
                SALTS_OK);
    check_equal(settings.protocol_kind, TURBO_FLOW_PROTOCOL_JTT_808);
    check_equal(settings.transport_kind, FLOW_PROTOCOL_NETWORK_TRANSPORT_LISTENER_TCP);
    check_equal(strcmp(settings.protocol_provider, "jtt808"), 0);
    check_equal(strcmp(settings.protocol_version, "2019-A1"), 0);
    check_equal(strcmp(settings.source_id, "fleet.primary"), 0);
    check_equal(settings.max_sessions, 4u);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    memset(&settings, 0, sizeof(settings));
    check_equal(run_preflight(packet_config, packet_graph, "udp.input", &settings, &error),
                SALTS_OK);
    check_equal(settings.protocol_kind, TURBO_FLOW_PROTOCOL_COAP);
    check_equal(settings.transport_kind, FLOW_PROTOCOL_NETWORK_TRANSPORT_PACKET_UDP);
    check_equal(strcmp(settings.protocol_provider, "coap"), 0);
    check_equal(strcmp(settings.protocol_version, "RFC7252"), 0);
  }

  it("accepts a typed mapper record and rejects incomplete or oversized semantic bounds") {
    char config_text[4096];
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    memset(&settings, 0, sizeof(settings));
    check_equal(run_preflight(mapped_packet_config, packet_graph, "udp.input", &settings, &error),
                SALTS_OK);
    check_equal(settings.mapper_enabled, 1);
    check_equal(strcmp(settings.mapper_plugin, "applicant.mapper"), 0);
    check_equal(strcmp(settings.mapper_name, "applicant"), 0);
    check_equal(strcmp(settings.mapper_profile, "applicant-json"), 0);
    check_equal(settings.mapper_message_type, 2u);
    check_equal(settings.mapper_semantic_type, 50u);
    check_equal(strcmp(settings.mapper_semantic_media_type, "application/json"), 0);
    check_equal(settings.mapper_max_semantic_bytes, (size_t)1024u);
    check_equal(settings.mapper_max_output_bytes, (size_t)2048u);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(mapped_packet_config, "profile \"applicant-json\", ", "", config_text,
                             sizeof(config_text)),
                SALTS_OK);
    check_equal(run_preflight(config_text, packet_graph, "udp.input", &settings, &error),
                SALTS_EPROTO);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(mapped_packet_config, "max_semantic_bytes 1024",
                             "max_semantic_bytes 1025", config_text, sizeof(config_text)),
                SALTS_OK);
    check_equal(run_preflight(config_text, packet_graph, "udp.input", &settings, &error),
                SALTS_ERANGE);
    check_contains(error.path, "mapper.max_semantic_bytes");
  }

  it("rejects unknown and missing intake fields") {
    char config_text[4096];
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(replace_once(listener_config, "  max_pending_bytes 65536\n",
                             "  max_pending_bytes 65536\n  unexpected 1\n", config_text,
                             sizeof(config_text)),
                SALTS_OK);
    check_equal(run_preflight(config_text, listener_graph, "tcp.input", &settings, &error),
                SALTS_EPROTO);
    check_contains(error.path, "$.stages.decode");
    check_true(error.message[0] != '\0');

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(listener_config, "  source_id \"fleet.primary\"\n", "", config_text,
                             sizeof(config_text)),
                SALTS_OK);
    check_equal(run_preflight(config_text, listener_graph, "tcp.input", &settings, &error),
                SALTS_EPROTO);
  }

  it("rejects unsupported pairings versions and KCP") {
    char config_text[4096];
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(replace_once(listener_config, "  protocol_provider \"jtt808\"\n",
                             "  protocol_provider \"coap\"\n", config_text, sizeof(config_text)),
                SALTS_OK);
    check_equal(run_preflight(config_text, listener_graph, "tcp.input", &settings, &error),
                SALTS_EINVAL);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(listener_config, "  protocol_version \"2019-A1\"\n",
                             "  protocol_version \"2013-A1\"\n", config_text, sizeof(config_text)),
                SALTS_OK);
    check_equal(run_preflight(config_text, listener_graph, "tcp.input", &settings, &error),
                SALTS_ENOTSUP);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(run_preflight(packet_config, packet_graph, "kcp.input", &settings, &error),
                SALTS_ENOTSUP);
  }

  it("rejects zero mismatched and insufficient bounds") {
    char config_text[4096];
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    check_equal(replace_once(listener_config, "  max_sessions 4\n", "  max_sessions 0\n",
                             config_text, sizeof(config_text)),
                SALTS_OK);
    check_equal(run_preflight(config_text, listener_graph, "tcp.input", &settings, &error),
                SALTS_EPROTO);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(listener_config, "  max_sessions 4\n", "  max_sessions 3\n",
                             config_text, sizeof(config_text)),
                SALTS_OK);
    check_equal(run_preflight(config_text, listener_graph, "tcp.input", &settings, &error),
                SALTS_ERANGE);
    check_contains(error.path, "max_sessions");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(listener_config, "  max_sessions 4\n", "  max_sessions 5\n",
                             config_text, sizeof(config_text)),
                SALTS_OK);
    check_equal(run_preflight(config_text, listener_graph, "tcp.input", &settings, &error),
                SALTS_ERANGE);
    check_contains(error.path, "max_sessions");

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(listener_config, "  max_pending_claims 64\n",
                             "  max_pending_claims 63\n", config_text, sizeof(config_text)),
                SALTS_OK);
    check_equal(run_preflight(config_text, listener_graph, "tcp.input", &settings, &error),
                SALTS_ERANGE);

    error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(replace_once(listener_config, "  max_pending_bytes 65536\n",
                             "  max_pending_bytes 65535\n", config_text, sizeof(config_text)),
                SALTS_OK);
    check_equal(run_preflight(config_text, listener_graph, "tcp.input", &settings, &error),
                SALTS_ERANGE);
  }

  it("uses the exact provider message budget rather than scheduler steps") {
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_source_transport_contract_t transport = TURBO_FLOW_SOURCE_TRANSPORT_CONTRACT_INIT;
    check_equal(run_preflight(listener_config, listener_graph, "tcp.input", &settings, &error),
                SALTS_OK);
    transport.transport = TURBO_FLOW_SOURCE_TRANSPORT_TCP_LISTENER;
    transport.session_capacity = 4u;
    transport.max_message_bytes = 1024u;
    transport.max_messages_per_poll = 64u;
    check_equal(flow_protocol_network_intake_transport_bind(&transport, &settings, &error), SALTS_OK);
    check_equal(settings.source_max_messages_per_poll, (size_t)64u);
    transport.max_messages_per_poll = 65u;
    check_equal(flow_protocol_network_intake_transport_bind(&transport, &settings, &error), SALTS_ERANGE);
    check_contains(error.path, "max_pending_claims");
    check_equal(settings.source_max_messages_per_poll, (size_t)64u);
  }

  it("rejects invalid transport ABI and zero limits without changing settings") {
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_source_transport_contract_t transport = TURBO_FLOW_SOURCE_TRANSPORT_CONTRACT_INIT;
    check_equal(run_preflight(listener_config, listener_graph, "tcp.input", &settings, &error),
                SALTS_OK);
    transport.transport = TURBO_FLOW_SOURCE_TRANSPORT_TCP_LISTENER;
    transport.session_capacity = 4u;
    transport.max_message_bytes = 1024u;
    transport.max_messages_per_poll = 64u;
    for (unsigned field = 0u; field < 5u; ++field) {
      turbo_flow_source_transport_contract_t invalid = transport;
      switch (field) {
        case 0u: --invalid.size; break;
        case 1u: ++invalid.version; break;
        case 2u: invalid.session_capacity = 0u; break;
        case 3u: invalid.max_message_bytes = 0u; break;
        default: invalid.max_messages_per_poll = 0u; break;
      }
      check_equal(flow_protocol_network_intake_transport_bind(&invalid, &settings, &error), SALTS_EINVAL);
      check_equal(settings.source_max_message_bytes, (size_t)1024u);
      check_equal(settings.source_max_messages_per_poll, (size_t)64u);
    }
  }

  it("rejects byte and parser storage overflow before committing transport bounds") {
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_source_transport_contract_t transport = TURBO_FLOW_SOURCE_TRANSPORT_CONTRACT_INIT;
    check_equal(run_preflight(listener_config, listener_graph, "tcp.input", &settings, &error),
                SALTS_OK);
    transport.transport = TURBO_FLOW_SOURCE_TRANSPORT_TCP_LISTENER;
    transport.session_capacity = 4u;
    transport.max_message_bytes = SIZE_MAX / settings.max_pending_claims + 1u;
    transport.max_messages_per_poll = 64u;
    check_equal(flow_protocol_network_intake_transport_bind(&transport, &settings, &error), SALTS_ERANGE);
    check_contains(error.path, "max_pending_bytes");
    check_equal(settings.source_max_message_bytes, (size_t)1024u);
    transport.max_message_bytes = 1024u;
    transport.session_capacity = SIZE_MAX;
    settings.max_sessions = SIZE_MAX;
    check_equal(flow_protocol_network_intake_transport_bind(&transport, &settings, &error), SALTS_ERANGE);
    check_contains(error.path, "max_frame_size");
  }

  it("selects the exact decoder stage and rejects invalid topology") {
    flow_protocol_network_intake_settings_t settings;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    static const char extra_graph[] = "source wire adapter cnet.listener_source\n"
                                      "stage middle\n"
                                      "stage decode adapter protocol.decode\n"
                                      "stage main {\n  wire -> middle -> decode\n}\n";
    check_equal(run_named_preflight(listener_config, listener_graph, "tcp.input", "missing", 1,
                                    &settings, &error),
                SALTS_EINVAL);
    check_equal(run_named_preflight(listener_config, listener_graph, "tcp.input", "protocol.decode",
                                    1, &settings, &error),
                SALTS_EINVAL);
    check_equal(run_named_preflight(listener_config, listener_graph, "tcp.input", "wire", 1,
                                    &settings, &error),
                SALTS_EINVAL);
    check_equal(run_named_preflight(listener_config, listener_graph, "tcp.input", "decode", 0,
                                    &settings, &error),
                SALTS_EINVAL);
    check_equal(run_preflight(listener_config, extra_graph, "tcp.input", &settings, &error),
                SALTS_EINVAL);
  }

  it("rejects wrong literal types obsolete schemas and partial mapper objects") {
    static const struct {
      const char *base;
      const char *before;
      const char *after;
    } cases[] = {
        {listener_config, "max_sessions 4", "max_sessions true"},
        {listener_config, "schema_version 1", "schema_version 2"},
        {listener_config, "schema_version 1", "schema_version 3"},
        {listener_config, "source_id \"fleet.primary\"", "source_id \"\""},
        {mapped_packet_config, "profile \"applicant-json\", ", ""},
        {mapped_packet_config, "message_type 2", "message_type 4294967296"},
        {mapped_packet_config, "max_output_bytes 2048", "max_output_bytes 0"},
        {mapped_packet_config, "max_output_bytes 2048", "max_output_bytes 2048, unexpected 1"}};
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      char source[4096];
      flow_protocol_network_intake_settings_t settings;
      turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
      check_equal(
          replace_once(cases[i].base, cases[i].before, cases[i].after, source, sizeof(source)),
          SALTS_OK);
      check_equal(run_preflight(source, listener_graph, "udp.input", &settings, &error),
                  SALTS_EPROTO);
      check_contains(error.path, "$.stages.decode");
      check_equal(settings.max_sessions, (size_t)0u);
    }
  }

  it("rejects decoder provider substitutions and undeclared resource use") {
    static const char *const replacements[] = {
        "stage decode adapter other.decode {",
        "stage decode adapter protocol.decode {\n  resource hidden"};
    for (size_t i = 0; i < sizeof(replacements) / sizeof(replacements[0]); ++i) {
      char source[4096];
      flow_protocol_network_intake_settings_t settings;
      turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
      check_equal(replace_once(listener_config, "stage decode adapter protocol.decode {",
                               replacements[i], source, sizeof(source)),
                  SALTS_OK);
      check_equal(run_preflight(source, listener_graph, "tcp.input", &settings, &error),
                  SALTS_EINVAL);
    }
  }
}
