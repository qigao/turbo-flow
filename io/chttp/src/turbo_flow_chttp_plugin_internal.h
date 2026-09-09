#ifndef TURBO_FLOW_CHTTP_PLUGIN_INTERNAL_H
#define TURBO_FLOW_CHTTP_PLUGIN_INTERNAL_H
#include "turbo_flow_chttp.h"
#include "turbo_flow_plugin_generation.h"

enum {
  CHTTP_PLUGIN_CLIENT = 1,
  CHTTP_PLUGIN_SERVER = 2,
  CHTTP_PLUGIN_WEBSOCKET = 4,
  CHTTP_PLUGIN_KIND_COUNT = 3,
  CHTTP_PLUGIN_MAX_OWNERS = 256,
  CHTTP_PLUGIN_TEXT_BYTES = 512,
  CHTTP_PLUGIN_HEADER_CAPACITY = 64,
  CHTTP_PLUGIN_ALPN_CAPACITY = 2
};

/* Control-thread owned immutable configuration; embedded strings outlive Graph detachment.
 * Native adapters alone own lifecycle and accepted work. Root holds <=256 owner pointers.
 * Publish transfers cleanup to generation; destruction occurs only after registry detach. */
typedef struct chttp_plugin_config_s {
  unsigned kind;
  cnet_client_config network;
  chttp_client_config client;
  chttp_server_config server;
  cnet_tls_client_config tls_client;
  cnet_tls_server_config tls_server;
  turbo_flow_chttp_client_config_t client_adapter;
  turbo_flow_chttp_server_config_t server_adapter;
  turbo_flow_chttp_websocket_server_config_t websocket_adapter;
  uint32_t schema_version, poll_budget_ms;
  int tls_enabled;
  char backend[CHTTP_PLUGIN_TEXT_BYTES], protocol[CHTTP_PLUGIN_TEXT_BYTES];
  char connection_uri[CHTTP_PLUGIN_TEXT_BYTES], authority[CHTTP_PLUGIN_TEXT_BYTES];
  char target[CHTTP_PLUGIN_TEXT_BYTES], method[CHTTP_PLUGIN_TEXT_BYTES];
  char bind_host[CHTTP_PLUGIN_TEXT_BYTES], path[CHTTP_PLUGIN_TEXT_BYTES];
  char response_content_type[CHTTP_PLUGIN_TEXT_BYTES], error_content_type[CHTTP_PLUGIN_TEXT_BYTES];
  char graph_error_body[CHTTP_PLUGIN_TEXT_BYTES], subprotocol[CHTTP_PLUGIN_TEXT_BYTES];
  char tls_ca_file[CHTTP_PLUGIN_TEXT_BYTES], tls_ca_path[CHTTP_PLUGIN_TEXT_BYTES];
  char tls_cert_file[CHTTP_PLUGIN_TEXT_BYTES], tls_key_file[CHTTP_PLUGIN_TEXT_BYTES];
  char tls_key_password[CHTTP_PLUGIN_TEXT_BYTES], tls_server_name[CHTTP_PLUGIN_TEXT_BYTES];
  char tls_client_auth[CHTTP_PLUGIN_TEXT_BYTES];
  const char *alpn[CHTTP_PLUGIN_ALPN_CAPACITY];
  char alpn_storage[CHTTP_PLUGIN_ALPN_CAPACITY][CHTTP_PLUGIN_TEXT_BYTES];
  size_t alpn_count;
  chttp_header headers[CHTTP_PLUGIN_HEADER_CAPACITY];
  char header_names[CHTTP_PLUGIN_HEADER_CAPACITY][CHTTP_PLUGIN_TEXT_BYTES];
  char header_values[CHTTP_PLUGIN_HEADER_CAPACITY][CHTTP_PLUGIN_TEXT_BYTES];
} chttp_plugin_config_t;
const char *chttp_plugin_kind_name(unsigned kind);
int chttp_plugin_config_read(const turbo_flow_resolved_config_t *resolved, const char *name,
                             unsigned kind, chttp_plugin_config_t *config,
                             turbo_flow_config_error_t *error);
#endif
