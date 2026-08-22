#include "turbo_flow_email.h"

#include "CoroNet/turbo_coro_socket.h"
#include "email/email_smtp.h"
#include "tinytest.h"
#include "turbo_flow_control.h"
#include "turbo_str.h"
#include "turbo_thread.h"

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

typedef struct smtp_server_state_s {
  char mail_from[128];
  char rcpt_to[128];
  char data[2048];
  size_t data_len;
  int handler_called;
  int mail_seen;
  int rcpt_seen;
  int data_seen;
  int quit_seen;
} smtp_server_state_t;

typedef struct pop3_stall_server_s {
  coro_context_t *ctx;
  atomic_int accepted;
} pop3_stall_server_t;

static void pop3_stall_handler(coro_socket_t *client, void *arg) {
  pop3_stall_server_t *server = (pop3_stall_server_t *)arg;
  char *data = NULL;
  size_t len = 0;
  if (!client || !server) return;
  atomic_store_explicit(&server->accepted, 1, memory_order_release);
  (void)coro_socket_recv(client, &data, &len);
  if (data) coro_socket_free_recv(data);
  coro_socket_destroy(client);
}

static void pop3_stall_server_thread(void *arg) {
  pop3_stall_server_t *server = (pop3_stall_server_t *)arg;
  if (server && server->ctx) (void)coro_context_run(server->ctx, TURBO_RUN_DEFAULT);
}

static unsigned short test_pick_loopback_port(void) {
  unsigned short port = 0;
  struct sockaddr_in addr;
  int attempt;
#ifdef _WIN32
  WSADATA wsa_data;
  if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) return 0;
#endif
  for (attempt = 0; attempt < 16 && port == 0; ++attempt) {
#ifdef _WIN32
    int addr_len = (int)sizeof(addr);
    SOCKET sock = INVALID_SOCKET;
#else
    socklen_t addr_len = (socklen_t)sizeof(addr);
    int sock = -1;
#endif

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(0);

    sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#ifdef _WIN32
    if (sock == INVALID_SOCKET) continue;
#else
    if (sock < 0) continue;
#endif

    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0 &&
        getsockname(sock, (struct sockaddr *)&addr, &addr_len) == 0) {
      port = ntohs(addr.sin_port);
    }

#ifdef _WIN32
    closesocket(sock);
#else
    close(sock);
#endif
  }
#ifdef _WIN32
  WSACleanup();
#endif
  return port;
}

static int smtp_buffer_pop_line(tstr *buffer, char *line, size_t line_cap) {
  size_t pos;
  size_t line_len;
  tstr rest;

  if (!buffer || !*buffer || !line || line_cap == 0) return 0;
  pos = tstr_find_char(*buffer, '\n');
  if (pos == VSTR_NPOS) return 0;

  line_len = pos + 1u;
  if (line_len >= line_cap) line_len = line_cap - 1u;
  memcpy(line, *buffer, line_len);
  line[line_len] = '\0';

  rest = tstr_from_v(vstr_from_buf(*buffer + pos + 1u, tstr_len(*buffer) - pos - 1u));
  tstr_freep(buffer);
  *buffer = rest;
  return *buffer ? 1 : 0;
}

static void smtp_capture_line(char *dst, size_t dst_cap, const char *line) {
  size_t len;

  if (!dst || dst_cap == 0 || !line) return;
  len = strlen(line);
  while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
    --len;
  if (len >= dst_cap) len = dst_cap - 1u;
  memcpy(dst, line, len);
  dst[len] = '\0';
}

static void smtp_server_handler(coro_socket_t *client, void *arg) {
  smtp_server_state_t *state = (smtp_server_state_t *)arg;
  tstr buffer = tstr_new();
  int in_data = 0;

  if (!client || !state || !buffer) return;
  state->handler_called = 1;
  coro_socket_set_timeout(client, 1000);
  (void)coro_socket_send(client, "220 flow-mail.local ESMTP\r\n", 27);

  while (!state->quit_seen) {
    char *chunk = NULL;
    size_t chunk_len = 0;
    int rc = coro_socket_recv(client, &chunk, &chunk_len);
    if (rc != TURBO_OK || !chunk || chunk_len == 0) {
      if (chunk) coro_socket_free_recv(chunk);
      break;
    }

    buffer = tstr_cat_len(buffer, chunk, chunk_len);
    coro_socket_free_recv(chunk);
    if (!buffer) break;

    while (buffer && tstr_len(buffer) > 0) {
      if (in_data) {
        size_t end = tstr_find_v(buffer, vstr_from_buf("\r\n.\r\n", 5));
        tstr rest;

        if (end == VSTR_NPOS) break;
        state->data_len = end < sizeof(state->data) - 1u ? end : sizeof(state->data) - 1u;
        memcpy(state->data, buffer, state->data_len);
        state->data[state->data_len] = '\0';
        state->data_seen = 1;
        rest = tstr_from_v(vstr_from_buf(buffer + end + 5u, tstr_len(buffer) - end - 5u));
        tstr_freep(&buffer);
        buffer = rest;
        in_data = 0;
        (void)coro_socket_send(client, "250 queued\r\n", 12);
        continue;
      }

      {
        char line[256];
        if (!smtp_buffer_pop_line(&buffer, line, sizeof(line))) break;
        if (strncmp(line, "HELO ", 5) == 0 || strncmp(line, "EHLO ", 5) == 0) {
          (void)coro_socket_send(client, "250 flow-mail.local\r\n", 21);
        } else if (strncmp(line, "MAIL FROM:", 10) == 0) {
          state->mail_seen = 1;
          smtp_capture_line(state->mail_from, sizeof(state->mail_from), line);
          (void)coro_socket_send(client, "250 sender ok\r\n", 15);
        } else if (strncmp(line, "RCPT TO:", 8) == 0) {
          state->rcpt_seen = 1;
          smtp_capture_line(state->rcpt_to, sizeof(state->rcpt_to), line);
          (void)coro_socket_send(client, "250 recipient ok\r\n", 18);
        } else if (strncmp(line, "DATA", 4) == 0) {
          in_data = 1;
          (void)coro_socket_send(client, "354 end with dot\r\n", 18);
        } else if (strncmp(line, "QUIT", 4) == 0) {
          state->quit_seen = 1;
          (void)coro_socket_send(client, "221 bye\r\n", 9);
        } else {
          (void)coro_socket_send(client, "500 unsupported\r\n", 17);
        }
      }
    }
  }

  tstr_freep(&buffer);
  coro_socket_destroy(client);
}

static void smtp_stall_server_handler(coro_socket_t *client, void *arg) {
  int *called = (int *)arg;
  char *data = NULL;
  size_t len = 0;
  if (!client || !called) return;
  *called = 1;
  (void)coro_socket_recv(client, &data, &len);
  if (data) coro_socket_free_recv(data);
}

typedef struct mime_capture_s {
  int headers;
  int bodies;
  int complete;
} mime_capture_t;

typedef struct mime_extract_capture_s {
  int calls;
  size_t root_headers;
  size_t part_count;
  mime_encoding_t root_encoding;
  mime_disposition_type_t second_disposition;
  char root_content_type[128];
  char root_charset[64];
  char root_body[128];
  size_t root_body_len;
  char first_body[128];
  size_t first_body_len;
  unsigned char second_body[128];
  size_t second_body_len;
  char second_filename[64];
} mime_extract_capture_t;

typedef struct email_payload_capture_s {
  int calls;
  tstr payload;
} email_payload_capture_t;

static int email_payload_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  email_payload_capture_t *capture = (email_payload_capture_t *)ctx;
  if (!capture || !msg || (msg->payload.len > 0 && !msg->payload.data)) return TURBO_EINVAL;
  tstr_freep(&capture->payload);
  capture->payload = tstr_new_len(msg->payload.data ? msg->payload.data : "", msg->payload.len);
  if (!capture->payload) return TURBO_ENOMEM;
  capture->calls++;
  return TURBO_OK;
}

static void mime_copy_view(char *dst, size_t dst_size, vstr view) {
  size_t len;
  if (!dst || dst_size == 0) return;
  len = view.len < dst_size - 1u ? view.len : dst_size - 1u;
  if (len > 0) memcpy(dst, view.data, len);
  dst[len] = '\0';
}

static int mime_extract_capture_stage(turbo_flow_msg_t *msg, void *ctx) {
  mime_extract_capture_t *capture = (mime_extract_capture_t *)ctx;
  const turbo_flow_email_mime_message_t *message = turbo_flow_email_msg_mime(msg);
  const turbo_flow_email_mime_entity_t *root;
  const turbo_flow_email_mime_entity_t *part;
  vstr body;
  if (!capture || !message) return TURBO_EINVAL;
  root = turbo_flow_email_mime_root(message);
  if (!root) return TURBO_EINVAL;
  capture->calls++;
  capture->root_headers = turbo_flow_email_mime_entity_header_count(root);
  capture->part_count = turbo_flow_email_mime_part_count(message);
  capture->root_encoding = turbo_flow_email_mime_entity_encoding(root);
  mime_copy_view(capture->root_content_type, sizeof(capture->root_content_type),
                 turbo_flow_email_mime_entity_content_type(root));
  mime_copy_view(capture->root_charset, sizeof(capture->root_charset),
                 turbo_flow_email_mime_entity_charset(root));
  body = turbo_flow_email_mime_entity_body(root);
  capture->root_body_len = body.len;
  mime_copy_view(capture->root_body, sizeof(capture->root_body), body);
  if (capture->part_count > 0) {
    part = turbo_flow_email_mime_part_at(message, 0);
    body = turbo_flow_email_mime_entity_body(part);
    capture->first_body_len = body.len;
    mime_copy_view(capture->first_body, sizeof(capture->first_body), body);
  }
  if (capture->part_count > 1) {
    part = turbo_flow_email_mime_part_at(message, 1);
    body = turbo_flow_email_mime_entity_body(part);
    capture->second_body_len =
        body.len < sizeof(capture->second_body) ? body.len : sizeof(capture->second_body);
    if (capture->second_body_len > 0) {
      memcpy(capture->second_body, body.data, capture->second_body_len);
    }
    capture->second_disposition = turbo_flow_email_mime_entity_disposition(part);
    mime_copy_view(capture->second_filename, sizeof(capture->second_filename),
                   turbo_flow_email_mime_entity_filename(part));
  }
  return TURBO_OK;
}

static int mime_header(mime_parser_t *parser, const char *data, size_t len) {
  mime_capture_t *capture = (mime_capture_t *)parser->data;
  (void)data;
  (void)len;
  ++capture->headers;
  return 0;
}

static int mime_body(mime_parser_t *parser, const char *data, size_t len) {
  mime_capture_t *capture = (mime_capture_t *)parser->data;
  (void)data;
  (void)len;
  ++capture->bodies;
  return 0;
}

static int mime_complete(mime_parser_t *parser) {
  mime_capture_t *capture = (mime_capture_t *)parser->data;
  ++capture->complete;
  return 0;
}

spec("turbo_flow_email") {
  it("rejects invalid SMTP and POP3 numeric or transport options") {
    turbo_flow_email_smtp_config_t smtp = {0};
    turbo_flow_email_pop3_config_t pop3 = {0};
    turbo_flow_t *flow = turbo_flow_create();

    check_not_null(flow);
    smtp.host = "127.0.0.1";
    smtp.port = 65536;
    smtp.from_email = "sender@example.com";
    smtp.to_email = "recipient@example.com";
    check_equal(turbo_flow_email_register_smtp_sink_adapter(flow, "smtp.invalid", &smtp),
                 TURBO_EINVAL);
    smtp.port = 25;
    smtp.timeout_ms = -1;
    check_equal(turbo_flow_email_register_smtp_sink_adapter(flow, "smtp.invalid", &smtp),
                 TURBO_EINVAL);
    smtp.timeout_ms = 0;
    smtp.auth_method = SMTP_AUTH_CRAM_MD5 + 1;
    check_equal(turbo_flow_email_register_smtp_sink_adapter(flow, "smtp.invalid", &smtp),
                 TURBO_EINVAL);
    smtp.auth_method = SMTP_AUTH_NONE;
    smtp.use_tls = 1;
    smtp.use_starttls = 1;
    check_equal(turbo_flow_email_register_smtp_sink_adapter(flow, "smtp.invalid", &smtp),
                 TURBO_EINVAL);

    pop3.host = "127.0.0.1";
    pop3.port = 110;
    pop3.poll_interval_ms = 1;
    pop3.timeout_ms = -1;
    check_equal(turbo_flow_email_register_pop3_source_adapter(flow, "pop3.invalid", &pop3),
                 TURBO_EINVAL);
    pop3.timeout_ms = 0;
    pop3.use_tls = 1;
    pop3.use_stls = 1;
    check_equal(turbo_flow_email_register_pop3_source_adapter(flow, "pop3.invalid", &pop3),
                 TURBO_EINVAL);

    turbo_flow_destroy(flow);
  }

  it("stops a pending POP3 source through the shared connection interrupt") {
    static const char *src = "source mail adapter pop3.pending\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  mail -> capture\n"
                             "}\n";
    turbo_flow_email_pop3_config_t config = {0};
    email_payload_capture_t capture = {0};
    pop3_stall_server_t stall = {0};
    turbo_flow_connection_snapshot_t connection;
    turbo_thread_t server_thread;
    turbo_flow_t *flow = turbo_flow_create();
    coro_socket_t *server;
    unsigned short port = test_pick_loopback_port();
    uint64_t started;
    char endpoint[128];

    check_not_null(flow);
    check_greater(port, 0);
    stall.ctx = coro_context_create(NULL);
    atomic_init(&stall.accepted, 0);
    check_not_null(stall.ctx);
    server = coro_socket_create_tcpv4(stall.ctx);
    check_not_null(server);
    check_equal(coro_socket_listen_on(server, "127.0.0.1", port, pop3_stall_handler, &stall),
                 TURBO_OK);
    check_equal(turbo_thread_create(&server_thread, pop3_stall_server_thread, &stall), TURBO_OK);

    config.host = "127.0.0.1";
    config.port = (int)port;
    config.timeout_ms = 30000;
    config.poll_interval_ms = 1000;
    check_equal(turbo_flow_email_register_pop3_source_adapter(flow, "pop3.pending", &config),
                 TURBO_OK);
    memset(&connection, 0, sizeof(connection));
    check_equal(turbo_flow_adapter_connection_snapshot_at(flow, 0, &connection), TURBO_OK);
    check_greater(snprintf(endpoint, sizeof(endpoint), "pop3://127.0.0.1:%u",
                          (unsigned int)port), 0);
    check_equal(connection.endpoint, endpoint);
    check_equal(connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_equal(
        turbo_flow_register_stage_ex(flow, "capture", email_payload_capture_stage, &capture, NULL),
        TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    while (!atomic_load_explicit(&stall.accepted, memory_order_acquire)) turbo_thread_yield();
    check_equal(turbo_flow_control(flow, "adapter pop3.pending quiesce",
                                    sizeof("adapter pop3.pending quiesce") - 1u),
                 TURBO_OK);
    check_equal(turbo_flow_control(flow, "adapter pop3.pending resume",
                                    sizeof("adapter pop3.pending resume") - 1u),
                 TURBO_OK);
    started = turbo_hrtime();
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    check_true(turbo_hrtime() - started < UINT64_C(500000000));
    check_equal(capture.calls, 0);
    turbo_flow_destroy(flow);
    coro_context_stop(stall.ctx);
    (void)turbo_thread_join(&server_thread);
    coro_socket_destroy(server);
    coro_context_destroy(stall.ctx);
  }

  it("owns pending SMTP requests after the publish pump budget expires") {
    static const char *src = "source input\n"
                             "stage email_out adapter smtp.pending\n"
                             "stage main {\n"
                             "  input -> email_out\n"
                             "}\n";
    turbo_flow_email_smtp_config_t config;
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    coro_context_t *ctx = coro_context_create(NULL);
    coro_socket_t *server;
    unsigned short port = test_pick_loopback_port();
    int server_called = 0;

    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(ctx);
    check_greater(port, 0);
    server = coro_socket_create_tcpv4(ctx);
    check_not_null(server);
    check_equal(coro_socket_listen_on(server, "127.0.0.1", port, smtp_stall_server_handler,
                                       &server_called),
                 TURBO_OK);
    config.context = ctx;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.from_email = "sender@example.com";
    config.to_email = "recipient@example.com";
    config.timeout_ms = 100;
    config.max_pump_iterations = 1;
    check_equal(turbo_flow_email_register_smtp_sink_adapter(flow, "smtp.pending", &config),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup("pending body");
    msg.payload = tstr_to_v(msg.owned_payload);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_ETIMEDOUT);
    for (int i = 0; i < 1000 && !server_called; ++i) {
      (void)coro_context_run(ctx, TURBO_RUN_ONCE);
    }
    check_equal(server_called, 1);
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
    coro_socket_destroy(server);
    coro_context_destroy(ctx);
  }

  it("sends flow payloads through an SMTP sink adapter") {
    static const char *src = "source input\n"
                             "stage email_out adapter smtp\n"
                             "stage main {\n"
                             "  input -> email_out\n"
                             "}\n";
    char raw[] = "flow payload body";
    turbo_flow_msg_t msg;
    mem_buffer_t *buffer = mem_wrap_external(raw, sizeof(raw) - 1, NULL, NULL);
    coro_context_t *ctx = coro_context_create(NULL);
    coro_socket_t *server = NULL;
    unsigned short port = 0;
    smtp_server_state_t server_state;
    turbo_flow_email_smtp_config_t config;
    turbo_flow_t *flow = turbo_flow_create();
    const turbo_flow_adapter_schema_t *adapter_schema;
    turbo_flow_connection_snapshot_t connection;
    char expected_endpoint[128];
    size_t option_index;
    int password_is_secret = 0;

    memset(&server_state, 0, sizeof(server_state));
    memset(&config, 0, sizeof(config));
    check_not_null(flow);
    check_not_null(buffer);
    check_not_null(ctx);

    port = test_pick_loopback_port();
    check_greater(port, 0);
    server = coro_socket_create_tcpv4(ctx);
    check_not_null(server);
    check_equal(
        coro_socket_listen_on(server, "127.0.0.1", port, smtp_server_handler, &server_state),
        TURBO_OK);

    config.context = ctx;
    config.host = "127.0.0.1";
    config.port = (int)port;
    config.from_name = "Flow Sender";
    config.from_email = "sender@example.com";
    config.to_name = "Flow Recipient";
    config.to_email = "recipient@example.com";
    config.subject = "Flow SMTP";
    config.timeout_ms = 1000;

    turbo_flow_msg_init(&msg);
    msg.buffer = buffer;
    msg.payload = vstr_from_buf(raw, sizeof(raw) - 1);

    check_equal(turbo_flow_email_register_smtp_sink_adapter(flow, "smtp", &config), TURBO_OK);
    adapter_schema = turbo_flow_find_adapter_schema(flow, "smtp");
    check_not_null(adapter_schema);
    check_equal(adapter_schema->kind, TURBO_FLOW_ADAPTER_KIND_EMAIL);
    check_equal(adapter_schema->roles, TURBO_FLOW_ADAPTER_SINK);
    for (option_index = 0; option_index < adapter_schema->field_count; ++option_index) {
      if (strcmp(adapter_schema->fields[option_index].name, "password") == 0) {
        password_is_secret =
            (adapter_schema->fields[option_index].flags & TURBO_FLOW_OPTION_SECRET_VALUE) != 0;
      }
    }
    check_equal(password_is_secret, 1);
    memset(&connection, 0, sizeof(connection));
    check_equal(turbo_flow_adapter_connection_snapshot_at(flow, 0, &connection), TURBO_OK);
    check_equal(connection.state, TURBO_FLOW_CONNECTION_STOPPED);
    check_greater(snprintf(expected_endpoint, sizeof(expected_endpoint), "smtp://127.0.0.1:%u",
                          (unsigned int)port), 0);
    check_equal(connection.endpoint, expected_endpoint);
    check_equal(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);

    for (int i = 0; i < 1000 && !server_state.quit_seen; ++i) {
      (void)coro_context_run(ctx, TURBO_RUN_ONCE);
    }

    check_equal(server_state.handler_called, 1);
    check_equal(server_state.mail_seen, 1);
    check_equal(server_state.rcpt_seen, 1);
    check_equal(server_state.data_seen, 1);
    check_contains(server_state.mail_from, "sender@example.com");
    check_contains(server_state.rcpt_to, "recipient@example.com");
    check_contains(server_state.data, "Subject: Flow SMTP");
    check_contains(server_state.data, "flow payload body");

    check_equal(turbo_flow_stop(flow), TURBO_OK);

    turbo_flow_msg_cleanup(&msg);
    turbo_flow_destroy(flow);
    coro_socket_destroy(server);
    coro_context_destroy(ctx);
  }

  it("parses MIME messages through TurboNet MimeParser callbacks") {
    static const char *dsl = "source input\n"
                             "stage parse adapter mime.parse\n"
                             "stage main {\n"
                             "  input -> parse\n"
                             "}\n";
    static const char message[] = "Content-Type: text/plain\r\nSubject: test\r\n\r\nHello";
    turbo_flow_email_mime_config_t config;
    mime_capture_t capture = {0};
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    memset(&config, 0, sizeof(config));
    config.settings.on_header_field = mime_header;
    config.settings.on_header_value = mime_header;
    config.settings.on_body = mime_body;
    config.settings.on_message_complete = mime_complete;
    config.user_data = &capture;
    check_not_null(flow);
    check_equal(turbo_flow_email_register_mime_parser_adapter(flow, "mime.parse", &config),
                 TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup(message);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
    check_equal(capture.headers, 4);
    check_equal(capture.bodies, 1);
    check_equal(capture.complete, 1);
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("extracts an owned decoded MIME root entity") {
    static const char *dsl = "source input\n"
                             "stage parse adapter mime.extract\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> parse -> capture\n"
                             "}\n";
    static const char message[] = "Content-Type: text/plain; charset=utf-8\r\n"
                                  "Content-Transfer-Encoding: base64\r\n"
                                  "Subject: owned\r\n\r\n"
                                  "SGVsbG8h";
    mime_extract_capture_t capture = {0};
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    check_equal(turbo_flow_email_register_mime_extract_adapter(flow, "mime.extract", NULL),
                 TURBO_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "capture", mime_extract_capture_stage, &capture, NULL),
        TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup(message);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
    check_equal(capture.calls, 1);
    check_equal(capture.root_headers, 3);
    check_equal(capture.part_count, 0);
    check_equal(capture.root_encoding, MIME_ENCODING_BASE64);
    check_equal(capture.root_content_type, "text/plain; charset=utf-8");
    check_equal(capture.root_charset, "utf-8");
    check_equal(capture.root_body_len, 6);
    check_equal(capture.root_body, "Hello!");
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("extracts decoded multipart bodies and attachment metadata") {
    static const char *dsl = "source input\n"
                             "stage parse adapter mime.extract\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> parse -> capture\n"
                             "}\n";
    static const char message[] =
        "Content-Type: multipart/mixed; boundary=flow-boundary\r\nSubject: parts\r\n\r\n"
        "--flow-boundary\r\nContent-Type: text/plain; charset=utf-8\r\n"
        "Content-Transfer-Encoding: quoted-printable\r\n\r\nHello=20World\r\n"
        "--flow-boundary\r\nContent-Type: application/octet-stream\r\n"
        "Content-Transfer-Encoding: base64\r\n"
        "Content-Disposition: attachment; filename=sample.bin\r\n\r\nAQID\r\n"
        "--flow-boundary--\r\n";
    static const unsigned char expected_binary[] = {1u, 2u, 3u};
    mime_extract_capture_t capture = {0};
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    check_equal(turbo_flow_email_register_mime_extract_adapter(flow, "mime.extract", NULL),
                 TURBO_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "capture", mime_extract_capture_stage, &capture, NULL),
        TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup(message);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
    check_equal(capture.calls, 1);
    check_equal(capture.part_count, 2);
    check_equal(capture.first_body, "Hello World");
    check_equal(capture.first_body_len, 11);
    check_equal(capture.second_body_len, sizeof(expected_binary));
    check_equal(memcmp(capture.second_body, expected_binary, sizeof(expected_binary)), 0);
    check_equal(capture.second_disposition, MIME_DISPOSITION_ATTACHMENT);
    check_equal(capture.second_filename, "sample.bin");
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects MIME extraction that exceeds configured ownership limits") {
    static const char *dsl = "source input\n"
                             "stage parse adapter mime.extract\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> parse -> capture\n"
                             "}\n";
    static const char message[] = "Content-Type: text/plain\r\nSubject: too-many\r\n\r\nbody";
    turbo_flow_email_mime_extract_config_t config = {0};
    mime_extract_capture_t capture = {0};
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    config.max_headers = 1;
    check_not_null(flow);
    check_equal(turbo_flow_email_register_mime_extract_adapter(flow, "mime.extract", &config),
                 TURBO_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "capture", mime_extract_capture_stage, &capture, NULL),
        TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup(message);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_EFBIG);
    check_equal(capture.calls, 0);
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects MIME bodies whose decoded representation exceeds its quota") {
    static const char *dsl = "source input\n"
                             "stage parse adapter mime.extract\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> parse -> capture\n"
                             "}\n";
    static const char message[] = "Content-Transfer-Encoding: base64\r\n\r\nSGVsbG8h";
    turbo_flow_email_mime_extract_config_t config = {0};
    mime_extract_capture_t capture = {0};
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    config.max_decoded_bytes = 5;
    check_not_null(flow);
    check_equal(turbo_flow_email_register_mime_extract_adapter(flow, "mime.extract", &config),
                 TURBO_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "capture", mime_extract_capture_stage, &capture, NULL),
        TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup(message);
    msg.payload = tstr_to_v(msg.owned_payload);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_EFBIG);
    check_equal(capture.calls, 0);
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("encodes payload and copied attachments as an RFC MIME message") {
    static const char *dsl = "source input\n"
                             "stage encode adapter mime.encode\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> encode -> capture\n"
                             "}\n";
    char filename[] = "note.bin";
    char attachment_data[] = {'a', 'b', 'c'};
    turbo_flow_email_attachment_config_t attachment = {0};
    turbo_flow_email_mime_encode_config_t config = {0};
    email_payload_capture_t capture = {0};
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    attachment.filename = filename;
    attachment.content_type = "application/octet-stream";
    attachment.data = attachment_data;
    attachment.data_len = sizeof(attachment_data);
    config.from_name = "Flow Sender";
    config.from_email = "sender@example.com";
    config.to_name = "Flow Receiver";
    config.to_email = "receiver@example.com";
    config.subject = "Encoded Flow";
    config.attachments = &attachment;
    config.attachment_count = 1;
    check_not_null(flow);
    check_equal(turbo_flow_email_register_mime_encode_adapter(flow, "mime.encode", &config),
                 TURBO_OK);
    filename[0] = 'X';
    attachment_data[0] = 'z';
    check_equal(
        turbo_flow_register_stage_ex(flow, "capture", email_payload_capture_stage, &capture, NULL),
        TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup("message body");
    msg.payload = tstr_to_v(msg.owned_payload);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
    check_equal(capture.calls, 1);
    check_contains(capture.payload, "From: \"Flow Sender\" <sender@example.com>\r\n");
    check_contains(capture.payload, "To: \"Flow Receiver\" <receiver@example.com>\r\n");
    check_contains(capture.payload, "Subject: Encoded Flow\r\n");
    check_contains(capture.payload, "Content-Type: multipart/mixed;");
    check_contains(capture.payload, "filename=\"note.bin\"");
    check_contains(capture.payload, "YWJj\r\n");
    check_contains(capture.payload, "message body");
    turbo_flow_msg_cleanup(&msg);
    tstr_freep(&capture.payload);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("encodes root HTML and copied resources as RFC MHTML") {
    static const char *dsl = "source input\n"
                             "stage encode adapter mhtml.encode\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> encode -> capture\n"
                             "}\n";
    char location[] = "asset.bin";
    unsigned char resource_data[] = {0x00u, 0x01u, 0xfeu, 0xffu};
    turbo_flow_email_mhtml_resource_config_t resource = {0};
    turbo_flow_email_mhtml_encode_config_t config = {0};
    email_payload_capture_t capture = {0};
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    resource.content_type = "application/octet-stream";
    resource.content_location = location;
    resource.data = (const char *)resource_data;
    resource.data_len = sizeof(resource_data);
    config.charset = "utf-8";
    config.resources = &resource;
    config.resource_count = 1;
    check_not_null(flow);
    check_equal(turbo_flow_email_register_mhtml_encode_adapter(flow, "mhtml.encode", &config),
                 TURBO_OK);
    location[0] = 'X';
    resource_data[1] = 0x7f;
    check_equal(
        turbo_flow_register_stage_ex(flow, "capture", email_payload_capture_stage, &capture, NULL),
        TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup("<html><body>page</body></html>");
    msg.payload = tstr_to_v(msg.owned_payload);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
    check_equal(capture.calls, 1);
    check_contains(capture.payload, "MIME-Version: 1.0\r\n");
    check_contains(capture.payload, "Content-Type: multipart/related; type=\"text/html\";");
    check_contains(capture.payload, "Content-Type: text/html; charset=utf-8\r\n");
    check_contains(capture.payload, "Content-Location: asset.bin\r\n");
    check_contains(capture.payload, "AAH+/w==\r\n");
    check_contains(capture.payload, "<html><body>page</body></html>");
    turbo_flow_msg_cleanup(&msg);
    tstr_freep(&capture.payload);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }

  it("rejects encoded MIME output that exceeds its configured quota") {
    static const char *dsl = "source input\n"
                             "stage encode adapter mime.encode\n"
                             "stage capture\n"
                             "stage main {\n"
                             "  input -> encode -> capture\n"
                             "}\n";
    turbo_flow_email_mime_encode_config_t config = {0};
    email_payload_capture_t capture = {0};
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    config.from_email = "sender@example.com";
    config.to_email = "receiver@example.com";
    config.max_output_size = 32;
    check_not_null(flow);
    check_equal(turbo_flow_email_register_mime_encode_adapter(flow, "mime.encode", &config),
                 TURBO_OK);
    check_equal(
        turbo_flow_register_stage_ex(flow, "capture", email_payload_capture_stage, &capture, NULL),
        TURBO_OK);
    check_equal(turbo_flow_parse_string(flow, dsl, strlen(dsl)), TURBO_OK);
    check_equal(turbo_flow_compile(flow), TURBO_OK);
    check_equal(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup("body");
    msg.payload = tstr_to_v(msg.owned_payload);
    check_equal(turbo_flow_publish(flow, "input", &msg), TURBO_EFBIG);
    check_equal(capture.calls, 0);
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_destroy(flow);
  }
}
