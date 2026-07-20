#ifndef TURBO_FLOW_HTTP_TYPES_H
#define TURBO_FLOW_HTTP_TYPES_H

#define TURBO_FLOW_HTTP_MODULE_VERSION 1u
#define TURBO_FLOW_HTTP_CLIENT_MODULE "io.http.client"
#define TURBO_FLOW_HTTP_CLIENT_REQUEST_OPERATION "http.client.request"
#define TURBO_FLOW_HTTP_CLIENT_POLL_OPERATION "http.client.poll"
#define TURBO_FLOW_HTTP_CLIENT_PRIMITIVE_TYPE "HttpClientConnection"
#define TURBO_FLOW_HTTP_SERVER_MODULE "io.http.server"
#define TURBO_FLOW_HTTP_SERVER_REQUEST_OPERATION "http.server.request"
#define TURBO_FLOW_HTTP_SERVER_REPLY_OPERATION "http.server.reply"
#define TURBO_FLOW_HTTP_SERVER_PRIMITIVE_TYPE "HttpServerEndpoint"
#define TURBO_FLOW_HTTP_TLS_PATH_LIMIT 4096u
#define TURBO_FLOW_HTTP_TLS_SECRET_LIMIT 4096u

/**
 * Verified HTTPS client identity. File paths are copied by provider creation.
 * client_cert_file and client_key_file form one atomic pair. An encrypted key
 * password is referenced through the product key provider and never stored in YAML.
 */
typedef struct turbo_flow_http_tls_client_config_s {
  const char *ca_file;
  const char *client_cert_file;
  const char *client_key_file;
  const char *client_key_password_ref;
} turbo_flow_http_tls_client_config_t;

#define TURBO_FLOW_HTTP_TLS_CLIENT_CONFIG_INIT {NULL, NULL, NULL, NULL}

typedef enum turbo_flow_http_method_e {
  TURBO_FLOW_HTTP_GET = 0,
  TURBO_FLOW_HTTP_POST,
  TURBO_FLOW_HTTP_PUT,
  TURBO_FLOW_HTTP_PATCH,
  TURBO_FLOW_HTTP_DELETE
} turbo_flow_http_method_t;

#endif /* TURBO_FLOW_HTTP_TYPES_H */
