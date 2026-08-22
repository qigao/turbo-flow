#include "data_bind.h"
#include "turbo_flow_http_server.h"
#include "turbo_flow_observe.h"
#include "turbo_flow_s3.h"

#include "iris/iris_app.h"
#include "iris/router.h"
#include "tinytest.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
#endif

typedef struct s3_capture_s {
  char payload[128];
  size_t payload_len;
  atomic_int called;
  turbo_flow_content_profile_t profile;
  turbo_flow_data_encoding_t encoding;
  char identity[TURBO_FLOW_CONTENT_IDENTITY_MAX + 1u];
  char schema_name[TURBO_FLOW_CONTENT_SCHEMA_NAME_MAX + 1u];
  uint32_t schema_version;
} s3_capture_t;

static int s3_noop(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return TURBO_OK;
}

static int s3_capture(turbo_flow_msg_t *msg, void *ctx) {
  s3_capture_t *capture = (s3_capture_t *)ctx;
  if (!msg || !capture || msg->payload.len > sizeof(capture->payload)) return TURBO_EINVAL;
  if (msg->payload.len > 0) memcpy(capture->payload, msg->payload.data, msg->payload.len);
  capture->payload_len = msg->payload.len;
  {
    const turbo_flow_content_descriptor_t *descriptor = turbo_flow_msg_content_descriptor(msg);
    if (descriptor) {
      capture->profile = descriptor->profile;
      capture->encoding = descriptor->encoding;
      memcpy(capture->identity, descriptor->identity, sizeof(capture->identity));
      memcpy(capture->schema_name, descriptor->schema_name, sizeof(capture->schema_name));
      capture->schema_version = descriptor->schema_version;
    }
  }
  atomic_fetch_add_explicit(&capture->called, 1, memory_order_release);
  return TURBO_OK;
}

static int s3_static_response(turbo_flow_msg_t *msg, void *ctx) {
  const char *body = (const char *)ctx;
  tstr payload;
  if (!msg || !body) return TURBO_EINVAL;
  payload = tstr_dup(body);
  if (!payload) return TURBO_ENOMEM;
  tstr_freep(&msg->owned_payload);
  mem_buffer_release(msg->buffer);
  msg->buffer = NULL;
  msg->owned_payload = payload;
  msg->payload = tstr_to_v(payload);
  return TURBO_OK;
}

static void s3_stat_response(Req *req, Res *res) {
  (void)req;
  reply(res, 200, "application/json", "", 0u);
}

static unsigned short s3_test_port(void) {
  struct sockaddr_in addr;
#ifdef _WIN32
  SOCKET socket_handle = INVALID_SOCKET;
  int addr_len = (int)sizeof(addr);
  WSADATA data;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0;
#else
  int socket_handle = -1;
  socklen_t addr_len = (socklen_t)sizeof(addr);
#endif
  unsigned short port = 0;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#ifdef _WIN32
  if (socket_handle == INVALID_SOCKET) return 0;
#else
  if (socket_handle < 0) return 0;
#endif
  if (bind(socket_handle, (struct sockaddr *)&addr, sizeof(addr)) == 0 &&
      getsockname(socket_handle, (struct sockaddr *)&addr, &addr_len) == 0) {
    port = ntohs(addr.sin_port);
  }
#ifdef _WIN32
  closesocket(socket_handle);
#else
  close(socket_handle);
#endif
  return port;
}

static turbo_flow_s3_config_t s3_test_config(void) {
  turbo_flow_s3_config_t config;
  memset(&config, 0, sizeof(config));
  config.host = "127.0.0.1";
  config.port = 9000;
  config.region = "us-east-1";
  config.bucket = "flow-test";
  config.object = "message.bin";
  config.credentials = TURBO_FLOW_S3_CREDENTIALS_STATIC;
  config.access_key = "test-access";
  config.secret_key = "test-secret";
  return config;
}

spec("turbo_flow_s3") {
  it("exposes a typed credential-free S3 client status document") {
    turbo_flow_s3_config_t config = s3_test_config();
    turbo_flow_resource_metadata_t before = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_metadata_t after = TURBO_FLOW_RESOURCE_METADATA_INIT;
    turbo_flow_resource_document_t document = TURBO_FLOW_RESOURCE_DOCUMENT_INIT;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBind *codec = NULL;
    DataBindValue *value = NULL;
    tstr payload = NULL;
    int32_t last_status = 0;
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    check_equal(turbo_flow_s3_register_client_adapter(flow, "s3.private", &config), TURBO_OK);
    check_equal(turbo_flow_resource_metadata_count(flow), 1u);
    check_equal(turbo_flow_resource_metadata_at(flow, 0u, &before), TURBO_OK);
    check_equal(before.uid, "s3:s3.private");
    check_equal(before.owner_name, "s3.private");
    check_equal(before.domain, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE);
    check_equal(before.kind, TURBO_FLOW_RESOURCE_CONNECTION);
    check_equal(turbo_flow_resource_document_at(
                     flow, 0u, TURBO_FLOW_RESOURCE_DOCUMENT_STATUS, &document),
                 TURBO_OK);
    check_equal(document.schema->schema_name, "TurboFlowS3Resource");
    check_equal(document.schema->type_name, "S3ClientStatus");
    check_equal(document.schema->schema_id, 201u);
    check_equal(document.schema->schema_version, 1u);
    check_equal(turbo_flow_resource_document_validate(&document, document.schema), TURBO_OK);
    {
      turbo_flow_resource_schema_t unknown = *document.schema;
      unknown.schema_id = 999u;
      check_equal(turbo_flow_resource_document_validate(&document, &unknown), TURBO_EPROTO);
    }
    check_equal(data_bind_create_from_text(document.schema->schema_text,
                                            strlen(document.schema->schema_text), &codec, &error),
                 DATA_BIND_OK);
    check_equal(data_bind_parse_json(codec, document.schema->type_name,
                                      mem_buffer_const_data(document.payload),
                                      mem_buffer_used(document.payload), &value, &error),
                 DATA_BIND_OK);
    check_equal(data_bind_validate_json(
                     codec, document.schema->type_name,
                     "{\"state\":0,\"last_status\":-1}",
                     sizeof("{\"state\":0,\"last_status\":-1}") - 1u, &error),
                 DATA_BIND_ERR_TYPE_MISMATCH);
    check_equal(data_bind_validate_json(
                     codec, document.schema->type_name,
                     "{\"state\":\"bad\",\"last_status\":-1,"
                     "\"connections_current\":\"0\",\"connection_limit\":\"1\","
                     "\"in_flight_messages\":\"0\",\"in_flight_bytes\":\"0\"}",
                     sizeof("{\"state\":\"bad\",\"last_status\":-1,"
                            "\"connections_current\":\"0\",\"connection_limit\":\"1\","
                            "\"in_flight_messages\":\"0\",\"in_flight_bytes\":\"0\"}") -
                         1u,
                     &error),
                 DATA_BIND_ERR_TYPE_MISMATCH);
    check_equal(data_bind_value_get_int32(data_bind_value_get(value, "last_status"),
                                           &last_status),
                 DATA_BIND_OK);
    check_equal(last_status, TURBO_ENOTCONN);
    payload = tstr_new_len(mem_buffer_const_data(document.payload),
                           mem_buffer_used(document.payload));
    check_not_null(payload);
    check_null(strstr(payload, config.access_key));
    check_null(strstr(payload, config.secret_key));
    check_null(strstr(payload, config.bucket));
    check_null(strstr(payload, config.object));
    check_equal(turbo_flow_resource_metadata_at(flow, 0u, &after), TURBO_OK);
    check_equal(after.generation, before.generation);
    check_equal(after.observed_generation, before.observed_generation);

    tstr_freep(&payload);
    data_bind_value_free(value);
    data_bind_free(codec);
    turbo_flow_resource_document_cleanup(&document);
    turbo_flow_destroy(flow);
  }

  it("registers PUT mode as an output sink") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_s3_config_t config = s3_test_config();
    const turbo_flow_adapter_schema_t *schema;
    check_not_null(flow);
    check_equal(turbo_flow_s3_register_client_adapter(flow, "store", &config), TURBO_OK);
    schema = turbo_flow_find_adapter_schema(flow, "store");
    check_not_null(schema);
    check_equal(schema->kind, TURBO_FLOW_ADAPTER_KIND_S3);
    check_equal(schema->roles, TURBO_FLOW_ADAPTER_SINK);
    check_equal(schema->direction, TURBO_FLOW_ADAPTER_OUTPUT);
    {
      turbo_flow_connection_snapshot_t connection;
      check_equal(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &connection), TURBO_OK);
      check_equal(connection.adapter_name, "store");
      check_equal(connection.endpoint, "http://127.0.0.1:9000/flow-test/message.bin");
      check_equal(connection.state, TURBO_FLOW_CONNECTION_STOPPED);
      check_equal(connection.connections_current, 0u);
      check_equal(connection.connection_limit, 1u);
    }
    turbo_flow_destroy(flow);
  }

  it("registers periodic GET mode as an input source") {
    static const char *dsl = "source object adapter s3.client.poll\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  object -> capture\n"
                             "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_s3_config_t config = s3_test_config();
    const turbo_flow_adapter_schema_t *schema;
    check_not_null(flow);
    config.poll_interval_ms = 100;
    check_equal(turbo_flow_s3_register_client_adapter(flow, "s3.client.poll", &config), TURBO_OK);
    schema = turbo_flow_find_adapter_schema(flow, "s3.client.poll");
    check_not_null(schema);
    check_equal(schema->roles, TURBO_FLOW_ADAPTER_SOURCE);
    check_equal(schema->direction, TURBO_FLOW_ADAPTER_INPUT);
    check_equal(turbo_flow_register_stage_ex(flow, "capture", s3_noop, NULL, NULL), TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("keeps unknown S3 content opaque unless a schema is declared") {
    turbo_flow_s3_config_t config = s3_test_config();
    turbo_flow_content_binding_t binding = TURBO_FLOW_CONTENT_BINDING_INIT;
    turbo_flow_schema_registry_t *registry = turbo_flow_schema_registry_create();
    turbo_flow_t *flow;

    check_not_null(registry);
    config.content_type = "application/x-s3-opaque";
    binding.registry = registry;
    config.content_binding = &binding;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_equal(turbo_flow_s3_register_client_adapter(flow, "s3.opaque", &config), TURBO_OK);
    turbo_flow_destroy(flow);

    binding.schema.schema_name = "s3.objects";
    binding.schema.type_name = "StoredObject";
    binding.schema.schema_version = 1u;
    flow = turbo_flow_create();
    check_not_null(flow);
    check_equal(turbo_flow_s3_register_client_adapter(flow, "s3.declared", &config), TURBO_EPROTO);
    turbo_flow_destroy(flow);
    turbo_flow_schema_registry_destroy(registry);
  }

  it("rejects incomplete static credentials") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_s3_config_t config = s3_test_config();
    check_not_null(flow);
    config.secret_key = NULL;
    check_equal(turbo_flow_s3_register_client_adapter(flow, "store", &config), TURBO_EINVAL);
    turbo_flow_destroy(flow);
  }

  it("publishes credential-free HTTPS endpoints and rejects oversized endpoints") {
    char oversized_object[TURBO_FLOW_ENDPOINT_MAX + 1u];
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_s3_config_t config = s3_test_config();
    turbo_flow_connection_snapshot_t connection;
    check_not_null(flow);
    config.use_https = 1;
    check_equal(turbo_flow_s3_register_client_adapter(flow, "secure", &config), TURBO_OK);
    check_equal(turbo_flow_adapter_connection_snapshot_at(flow, 0u, &connection), TURBO_OK);
    check_equal(connection.endpoint, "https://127.0.0.1:9000/flow-test/message.bin");
    check_false(strstr(connection.endpoint, config.access_key) != NULL);
    check_false(strstr(connection.endpoint, config.secret_key) != NULL);
    memset(oversized_object, 'x', sizeof(oversized_object) - 1u);
    oversized_object[sizeof(oversized_object) - 1u] = '\0';
    config.object = oversized_object;
    check_equal(turbo_flow_s3_register_client_adapter(flow, "oversized", &config), TURBO_ENOSPC);
    turbo_flow_destroy(flow);
  }

  it("uploads sink payloads with PutObject") {
    static const char *server_dsl = "source request adapter api\n"
                                    "stage capture\n"
                                    "stage response adapter api\n"
                                    "stage main {\n"
                                    "  request -> capture -> response\n"
                                    "}\n";
    static const char *client_dsl = "source input\n"
                                    "stage store adapter s3.client.put\n"
                                    "stage main {\n"
                                    "  input -> store\n"
                                    "}\n";
    const char body[] = "stored-object";
    unsigned short port = s3_test_port();
    turbo_flow_t *server_flow = turbo_flow_create();
    turbo_flow_t *client_flow = turbo_flow_create();
    turbo_flow_http_server_config_t server_config;
    turbo_flow_s3_config_t config = s3_test_config();
    turbo_flow_content_descriptor_t incompatible_descriptor;
    turbo_flow_msg_t incompatible_msg;
    turbo_flow_msg_t msg;
    turbo_flow_observe_t *observe = turbo_flow_observe_create(NULL);
    turbo_flow_observe_graph_snapshot_t graph;
    turbo_flow_observe_control_facts_snapshot_t facts_snapshot;
    turbo_flow_control_facts_t facts = TURBO_FLOW_CONTROL_FACTS_INIT;
    turbo_flow_error_t error;
    s3_capture_t capture;

    check_greater(port, 0);
    check_not_null(observe);
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.called, 0);
    memset(&server_config, 0, sizeof(server_config));
    server_config.port = port;
    server_config.route = "/flow-test/message.bin";
    server_config.method = TURBO_FLOW_HTTP_PUT;
    check_equal(turbo_flow_http_register_server_adapter(server_flow, "api", &server_config),
                 TURBO_OK);
    check_equal(turbo_flow_register_stage_ex(server_flow, "capture", s3_capture, &capture, NULL),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(server_flow, server_dsl, strlen(server_dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(server_flow), TURBO_OK);
    check_equal(turbo_flow_start(server_flow), TURBO_OK);

    config.port = port;
    check_equal(turbo_flow_s3_register_client_adapter(client_flow, "s3.client.put", &config),
                 TURBO_OK);
    check_equal(turbo_flow_observe_attach(observe, client_flow), TURBO_OK);
    check_equal(turbo_flow_parse_string(client_flow, client_dsl, strlen(client_dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(client_flow), TURBO_OK);
    check_equal(turbo_flow_start(client_flow), TURBO_OK);
    check_equal(turbo_flow_observe_graph_snapshot(observe, &graph), TURBO_OK);
    check_equal(graph.connection_providers, 1u);
    check_equal(graph.connections_current, 0u);
    check_equal(turbo_flow_observe_control_facts(observe, &facts_snapshot, &facts), TURBO_OK);
    check_equal(turbo_flow_control_ex(
                     client_flow,
                     "when graph.connection_providers == 1 then adapter s3.client.put quiesce",
                     sizeof("when graph.connection_providers == 1 then adapter "
                            "s3.client.put quiesce") -
                         1u,
                     &facts, &error),
                 TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup(body);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_equal(turbo_flow_publish(client_flow, "input", &msg), TURBO_EBUSY);
    check_equal(turbo_flow_control(client_flow, "adapter s3.client.put resume",
                                    sizeof("adapter s3.client.put resume") - 1u),
                 TURBO_OK);
    check_equal(turbo_flow_content_descriptor_init(
                     &incompatible_descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                     TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_JSON,
                     "application/json", "orders.created"),
                 TURBO_OK);
    turbo_flow_msg_init(&incompatible_msg);
    incompatible_msg.owned_payload = tstr_dup("{}");
    incompatible_msg.payload = tstr_to_v(incompatible_msg.owned_payload);
    check_equal(turbo_flow_msg_set_content_descriptor(&incompatible_msg, &incompatible_descriptor),
                 TURBO_OK);
    check_equal(turbo_flow_publish(client_flow, "input", &incompatible_msg), TURBO_EPROTO);
    check_equal(atomic_load_explicit(&capture.called, memory_order_acquire), 0);
    turbo_flow_msg_cleanup(&incompatible_msg);

    check_equal(turbo_flow_publish(client_flow, "input", &msg), TURBO_OK);
    check_equal(atomic_load_explicit(&capture.called, memory_order_acquire), 1);
    check_equal(capture.payload_len, sizeof(body) - 1);
    check_equal(capture.payload, body, sizeof(body) - 1);
    check_equal(capture.profile, TURBO_FLOW_CONTENT_PROFILE_HTTP_REQUEST_BODY);
    check_equal(capture.encoding, TURBO_FLOW_DATA_ENCODING_OPAQUE);
    check_equal(capture.identity, "/flow-test/message.bin");
    {
      turbo_flow_connection_snapshot_t connection;
      check_equal(turbo_flow_adapter_connection_snapshot_at(client_flow, 0u, &connection),
                   TURBO_OK);
      check_equal(connection.state, TURBO_FLOW_CONNECTION_READY);
      check_equal(connection.connections_current, 0u);
      check_equal(connection.in_flight_messages, 0u);
      check_equal(connection.in_flight_bytes, 0u);
      check_equal(connection.last_status, TURBO_OK);
    }
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_stop(client_flow), TURBO_OK);
    {
      turbo_flow_connection_snapshot_t connection;
      check_equal(turbo_flow_adapter_connection_snapshot_at(client_flow, 0u, &connection),
                   TURBO_OK);
      check_equal(connection.state, TURBO_FLOW_CONNECTION_STOPPED);
      check_equal(connection.last_status, TURBO_ESHUTDOWN);
    }
    check_equal(turbo_flow_stop(server_flow), TURBO_OK);
    check_equal(turbo_flow_observe_detach(observe), TURBO_OK);
    turbo_flow_destroy(client_flow);
    turbo_flow_destroy(server_flow);
    check_equal(turbo_flow_observe_destroy(observe), TURBO_OK);
  }

  it("publishes periodic GetObject responses") {
    static const char *server_dsl = "source request adapter api\n"
                                    "stage provide\n"
                                    "stage response adapter api\n"
                                    "stage main {\n"
                                    "  request -> provide -> response\n"
                                    "}\n";
    static const char *client_dsl = "source object adapter s3.client.poll\n"
                                    "stage capture\n"
                                    "stage main {\n"
                                    "  object -> capture\n"
                                    "}\n";
    const char body[] = "polled-object";
    unsigned short port = s3_test_port();
    turbo_flow_t *server_flow = turbo_flow_create();
    turbo_flow_t *client_flow = turbo_flow_create();
    turbo_flow_http_server_config_t server_config;
    turbo_flow_s3_config_t config = s3_test_config();
    turbo_flow_content_descriptor_t match;
    turbo_flow_data_schema_t schema = {sizeof(turbo_flow_data_schema_t),
                                       TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
                                       TURBO_FLOW_DATA_ENCODING_JSON,
                                       "s3.objects",
                                       "StoredObject",
                                       "test.object",
                                       1u,
                                       2u,
                                       NULL};
    turbo_flow_content_binding_t binding = TURBO_FLOW_CONTENT_BINDING_INIT;
    turbo_flow_schema_registry_t *registry = turbo_flow_schema_registry_create();
    s3_capture_t capture;
    iris_app_t *app = iris_app_create();

    check_greater(port, 0);
    check_not_null(registry);
    check_not_null(app);
    memset(&capture, 0, sizeof(capture));
    atomic_init(&capture.called, 0);
    memset(&server_config, 0, sizeof(server_config));
    server_config.port = port;
    server_config.route = "/flow-test/message.bin";
    server_config.method = TURBO_FLOW_HTTP_GET;
    server_config.app = app;
    server_config.take_app_ownership = 1;
    iris_app_route(app, "HEAD", server_config.route, NO_MW, s3_stat_response);
    check_equal(turbo_flow_http_register_server_adapter(server_flow, "api", &server_config),
                 TURBO_OK);
    check_equal(turbo_flow_register_stage_ex(server_flow, "provide", s3_static_response,
                                              (void *)body, NULL),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(server_flow, server_dsl, strlen(server_dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(server_flow), TURBO_OK);
    check_equal(turbo_flow_start(server_flow), TURBO_OK);

    config.port = port;
    config.poll_interval_ms = 20;
    config.content_type = "application/octet-stream";
    check_equal(turbo_flow_content_descriptor_init(&match, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
                                                    TURBO_FLOW_CONTENT_PROFILE_S3_OBJECT,
                                                    TURBO_FLOW_DATA_ENCODING_JSON,
                                                    "application/json", NULL),
                 TURBO_OK);
    check_equal(turbo_flow_schema_registry_register(registry, &match, &schema), TURBO_OK);
    binding.registry = registry;
    binding.schema.schema_name = schema.schema_name;
    binding.schema.type_name = schema.type_name;
    binding.schema.schema_version = schema.schema_version;
    config.content_binding = &binding;
    check_equal(turbo_flow_s3_register_client_adapter(client_flow, "s3.client.poll", &config),
                 TURBO_OK);
    check_equal(turbo_flow_register_stage_ex(client_flow, "capture", s3_capture, &capture, NULL),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(client_flow, client_dsl, strlen(client_dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(client_flow), TURBO_OK);
    check_equal(turbo_flow_start(client_flow), TURBO_OK);
    for (int i = 0; i < 200 && atomic_load_explicit(&capture.called, memory_order_acquire) == 0;
         ++i) {
      turbo_sleep_ms(10);
    }
    check_greater(atomic_load_explicit(&capture.called, memory_order_acquire), 0);
    check_equal(capture.payload_len, sizeof(body) - 1);
    check_equal(capture.payload, body, sizeof(body) - 1);
    check_equal(capture.profile, TURBO_FLOW_CONTENT_PROFILE_S3_OBJECT);
    check_equal(capture.encoding, TURBO_FLOW_DATA_ENCODING_JSON);
    check_equal(capture.identity, "flow-test/message.bin");
    check_equal(capture.schema_name, "s3.objects");
    check_equal(capture.schema_version, 2u);
    check_equal(turbo_flow_stop(client_flow), TURBO_OK);
    check_equal(turbo_flow_stop(server_flow), TURBO_OK);
    turbo_flow_destroy(client_flow);
    turbo_flow_destroy(server_flow);
    turbo_flow_schema_registry_destroy(registry);
  }
}
