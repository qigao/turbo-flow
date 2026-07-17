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

typedef enum turbo_flow_http_method_e {
  TURBO_FLOW_HTTP_GET = 0,
  TURBO_FLOW_HTTP_POST,
  TURBO_FLOW_HTTP_PUT,
  TURBO_FLOW_HTTP_PATCH,
  TURBO_FLOW_HTTP_DELETE
} turbo_flow_http_method_t;

#endif /* TURBO_FLOW_HTTP_TYPES_H */
