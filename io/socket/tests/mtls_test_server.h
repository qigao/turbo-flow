#ifndef TURBO_FLOW_MTLS_TEST_SERVER_H
#define TURBO_FLOW_MTLS_TEST_SERVER_H

#include "platform.h"
#include "tls_test_support.h"
#include "turbo_thread.h"

#include <openssl/pem.h>
#include <openssl/ssl.h>

#ifdef _WIN32
#  include <winsock2.h>
#  include <ws2tcpip.h>
typedef SOCKET flow_mtls_test_socket_t;
#  define FLOW_MTLS_TEST_INVALID_SOCKET INVALID_SOCKET
#  define flow_mtls_test_close_socket closesocket
#else
#  include <arpa/inet.h>
#  include <netinet/in.h>
#  include <signal.h>
#  include <sys/select.h>
#  include <sys/socket.h>
#  include <unistd.h>
typedef int flow_mtls_test_socket_t;
#  define FLOW_MTLS_TEST_INVALID_SOCKET (-1)
#  define flow_mtls_test_close_socket close
#endif

#define FLOW_MTLS_TEST_REQUEST_CAPACITY 4096u

typedef struct flow_mtls_test_server_s {
  flow_mtls_test_socket_t listener;
  turbo_thread_t thread;
  const uint8_t *response;
  size_t response_size;
  uint32_t response_delay_ms;
  unsigned short port;
  int started;
  int status;
  int peer_verified;
  uint8_t request[FLOW_MTLS_TEST_REQUEST_CAPACITY];
  size_t request_size;
} flow_mtls_test_server_t;

static SSL_CTX *flow_mtls_test_server_context(void) {
  SSL_CTX *ctx = NULL;
  BIO *cert_bio = NULL;
  BIO *key_bio = NULL;
  X509 *cert = NULL;
  EVP_PKEY *key = NULL;

  ctx = SSL_CTX_new(TLS_server_method());
  cert_bio = BIO_new_mem_buf(s_tls_test_cert_pem, -1);
  key_bio = BIO_new_mem_buf(s_tls_test_key_pem, -1);
  if (!ctx || !cert_bio || !key_bio) goto fail;
  cert = PEM_read_bio_X509(cert_bio, NULL, NULL, NULL);
  key = PEM_read_bio_PrivateKey(key_bio, NULL, NULL, NULL);
  if (!cert || !key || SSL_CTX_use_certificate(ctx, cert) != 1 ||
      SSL_CTX_use_PrivateKey(ctx, key) != 1 || SSL_CTX_check_private_key(ctx) != 1 ||
      X509_STORE_add_cert(SSL_CTX_get_cert_store(ctx), cert) != 1)
    goto fail;
  SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);
  X509_free(cert);
  EVP_PKEY_free(key);
  BIO_free(cert_bio);
  BIO_free(key_bio);
  return ctx;

fail:
  X509_free(cert);
  EVP_PKEY_free(key);
  BIO_free(cert_bio);
  BIO_free(key_bio);
  SSL_CTX_free(ctx);
  return NULL;
}

static int flow_mtls_test_wait_readable(flow_mtls_test_socket_t socket_handle) {
  fd_set readfds;
  struct timeval timeout;
  FD_ZERO(&readfds);
  FD_SET(socket_handle, &readfds);
  timeout.tv_sec = 5;
  timeout.tv_usec = 0;
  return select((int)(socket_handle + 1), &readfds, NULL, NULL, &timeout) > 0;
}

static int flow_mtls_test_request_complete(const uint8_t *request, size_t size) {
  if (!request || size == 0u) return 0;
  if ((request[0] & 0xf0u) == 0x10u) {
    size_t multiplier = 1u;
    size_t remaining = 0u;
    size_t index = 1u;
    uint8_t encoded;
    do {
      if (index >= size || multiplier > 128u * 128u * 128u) return 0;
      encoded = request[index++];
      remaining += (size_t)(encoded & 0x7fu) * multiplier;
      multiplier *= 128u;
    } while ((encoded & 0x80u) != 0u);
    return size >= index + remaining;
  }
  if (size >= 4u) {
    const char *text = (const char *)request;
    const char *headers_end = strstr(text, "\r\n\r\n");
    if (headers_end) {
      const char *length_header = strstr(text, "Content-Length:");
      size_t header_size = (size_t)(headers_end + 4 - text);
      size_t content_size = length_header ? (size_t)strtoull(length_header + 15, NULL, 10) : 0u;
      return size >= header_size + content_size;
    }
  }
  return 0;
}

static void flow_mtls_test_server_main(void *arg) {
  flow_mtls_test_server_t *server = (flow_mtls_test_server_t *)arg;
  flow_mtls_test_socket_t client = FLOW_MTLS_TEST_INVALID_SOCKET;
  SSL_CTX *ctx = NULL;
  SSL *ssl = NULL;
  X509 *peer = NULL;
  uint8_t request[FLOW_MTLS_TEST_REQUEST_CAPACITY];
  size_t request_size = 0u;

  server->status = -1;
  ctx = flow_mtls_test_server_context();
  if (!ctx || !flow_mtls_test_wait_readable(server->listener)) goto done;
  client = accept(server->listener, NULL, NULL);
  if (client == FLOW_MTLS_TEST_INVALID_SOCKET) goto done;
  ssl = SSL_new(ctx);
  if (!ssl || SSL_set_fd(ssl, (int)client) != 1 || SSL_accept(ssl) != 1) goto done;
  peer = SSL_get_peer_certificate(ssl);
  server->peer_verified =
      peer != NULL && SSL_get_verify_result(ssl) == X509_V_OK;
  if (!server->peer_verified) goto done;
  do {
    int received = SSL_read(ssl, request + request_size,
                            (int)(sizeof(request) - 1u - request_size));
    if (received <= 0) goto done;
    request_size += (size_t)received;
    request[request_size] = '\0';
  } while (!flow_mtls_test_request_complete(request, request_size) &&
           request_size < sizeof(request) - 1u);
  if (!flow_mtls_test_request_complete(request, request_size)) goto done;
  memcpy(server->request, request, request_size + 1u);
  server->request_size = request_size;
  if (server->response_delay_ms != 0u) turbo_sleep_ms(server->response_delay_ms);
  if (server->response_size != 0u &&
      SSL_write(ssl, server->response, (int)server->response_size) !=
          (int)server->response_size)
    goto done;
  server->status = 0;

done:
  X509_free(peer);
  if (ssl) SSL_shutdown(ssl);
  SSL_free(ssl);
  if (client != FLOW_MTLS_TEST_INVALID_SOCKET) flow_mtls_test_close_socket(client);
  SSL_CTX_free(ctx);
}

static int flow_mtls_test_server_start_delayed(flow_mtls_test_server_t *server,
                                               const uint8_t *response, size_t response_size,
                                               uint32_t response_delay_ms) {
  struct sockaddr_in address;
#ifdef _WIN32
  int address_size = (int)sizeof(address);
  WSADATA data;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return -1;
#else
  socklen_t address_size = (socklen_t)sizeof(address);
  /* OpenSSL SSL_write cannot carry MSG_NOSIGNAL; invalid-response tests may
   * close the peer before the server drains an oversized response. */
  signal(SIGPIPE, SIG_IGN);
#endif
  if (!server || (!response && response_size != 0u)) return -1;
  memset(server, 0, sizeof(*server));
  server->listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (server->listener == FLOW_MTLS_TEST_INVALID_SOCKET) return -1;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind(server->listener, (const struct sockaddr *)&address, sizeof(address)) != 0 ||
      listen(server->listener, 1) != 0 ||
      getsockname(server->listener, (struct sockaddr *)&address, &address_size) != 0) {
    flow_mtls_test_close_socket(server->listener);
    server->listener = FLOW_MTLS_TEST_INVALID_SOCKET;
    return -1;
  }
  server->port = ntohs(address.sin_port);
  server->response = response;
  server->response_size = response_size;
  server->response_delay_ms = response_delay_ms;
  if (turbo_thread_create(&server->thread, flow_mtls_test_server_main, server) != 0) {
    flow_mtls_test_close_socket(server->listener);
    server->listener = FLOW_MTLS_TEST_INVALID_SOCKET;
    return -1;
  }
  server->started = 1;
  return 0;
}

static int flow_mtls_test_server_start(flow_mtls_test_server_t *server,
                                       const uint8_t *response, size_t response_size) {
  return flow_mtls_test_server_start_delayed(server, response, response_size, 0u);
}

static void flow_mtls_test_server_join(flow_mtls_test_server_t *server) {
  if (!server) return;
  if (server->started) (void)turbo_thread_join(&server->thread);
  if (server->listener != FLOW_MTLS_TEST_INVALID_SOCKET)
    flow_mtls_test_close_socket(server->listener);
  server->listener = FLOW_MTLS_TEST_INVALID_SOCKET;
  server->started = 0;
}

#endif
