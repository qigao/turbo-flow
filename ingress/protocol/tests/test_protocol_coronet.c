#include "tinytest.h"
#include "tls_test_support.h"
#include "turbo_error.h"
#include "turbo_flow_protocol_coronet.h"
#include "turbo_thread.h"

#include <inttypes.h>
#include <stdatomic.h>
#include <string.h>

#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
typedef SOCKET protocol_test_socket_t;
  #define PROTOCOL_TEST_INVALID_SOCKET INVALID_SOCKET
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
typedef int protocol_test_socket_t;
  #define PROTOCOL_TEST_INVALID_SOCKET (-1)
#endif

enum {
  PROTOCOL_TEST_FRAME_CAPACITY = 128,
  PROTOCOL_TEST_RECONNECT_ITERATIONS = 4,
  PROTOCOL_TEST_WAIT_MS = 3000,
  PROTOCOL_TEST_SHUTDOWN_WAIT_MS = 10000
};

typedef struct protocol_coronet_probe_s {
  atomic_int publishes;
  atomic_ullong delivery_id;
  atomic_int verified_identities;
  int pending;
} protocol_coronet_probe_t;

typedef struct protocol_ws_client_s {
  coro_context_t *context;
  int port;
  int status;
  int done;
  const char *payload;
  size_t payload_size;
  int secure;
  int direct_tls;
  int wait_for_rejection;
  int hold_after_send;
  atomic_int release_after_send;
  int run_status;
  const char *ca_file;
  const char *cert_file;
  const char *key_file;
} protocol_ws_client_t;

static void protocol_coronet_probe_init(protocol_coronet_probe_t *probe) {
  memset(probe, 0, sizeof(*probe));
  atomic_init(&probe->publishes, 0);
  atomic_init(&probe->delivery_id, 0u);
  atomic_init(&probe->verified_identities, 0);
}

static int protocol_coronet_publish(void *ctx, const turbo_flow_protocol_publish_request_t *request,
                                   turbo_flow_protocol_publish_disposition_t *disposition) {
  protocol_coronet_probe_t *probe = (protocol_coronet_probe_t *)ctx;
  if (!probe || !request || !request->message || !disposition) return TURBO_EINVAL;
  atomic_store_explicit(&probe->delivery_id, request->delivery_id, memory_order_release);
  atomic_fetch_add_explicit(&probe->publishes, 1, memory_order_release);
  *disposition =
      probe->pending ? TURBO_FLOW_PROTOCOL_PUBLISH_PENDING : TURBO_FLOW_PROTOCOL_PUBLISH_SETTLED;
  return TURBO_OK;
}

static int protocol_coronet_identity(void *ctx,
                                    const turbo_flow_protocol_coronet_identity_request_t *request,
                                    char *device_id, size_t capacity) {
  const char *identity = (const char *)ctx;
  size_t length;
  if (!request || !request->socket || !identity || !device_id) return TURBO_EINVAL;
  length = strlen(identity);
  if (length >= capacity) return TURBO_EMSGSIZE;
  memcpy(device_id, identity, length + 1u);
  return TURBO_OK;
}

static int
protocol_coronet_mtls_identity(void *ctx,
                              const turbo_flow_protocol_coronet_identity_request_t *request,
                              char *device_id, size_t capacity) {
  static const char identity[] = "mtls-device";
  protocol_coronet_probe_t *probe = (protocol_coronet_probe_t *)ctx;
  char fingerprint[CORO_TLS_PEER_CERT_SHA256_CAPACITY];
  int rc;
  if (!probe || !request || !request->socket || !device_id) return TURBO_EINVAL;
  rc = coro_socket_tls_get_verified_peer_certificate_sha256(request->socket, fingerprint);
  if (rc != TURBO_OK) return rc;
  if (strncmp(fingerprint, "sha256:", 7u) != 0) return TURBO_EPROTO;
  if (sizeof(identity) > capacity) return TURBO_EMSGSIZE;
  memcpy(device_id, identity, sizeof(identity));
  atomic_fetch_add_explicit(&probe->verified_identities, 1, memory_order_release);
  return TURBO_OK;
}

static int protocol_coronet_plugin_open(const char *module, const char *name,
                                       turbo_flow_protocol_kind_t kind, const char *version,
                                       turbo_flow_protocol_registry_t **registry_out,
                                       turbo_flow_protocol_owner_t **owner_out,
                                       turbo_flow_protocol_t **protocol_out) {
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_protocol_registry_t *registry = NULL;
  turbo_flow_protocol_owner_t *owner = NULL;
  turbo_flow_protocol_t *protocol = NULL;
  char reason[256];
  int rc;
  if (registry_out) *registry_out = NULL;
  if (owner_out) *owner_out = NULL;
  if (protocol_out) *protocol_out = NULL;
  if (!module || !name || !registry_out || !owner_out || !protocol_out) return TURBO_EINVAL;
  request.protocol = kind;
  request.protocol_version = version;
  request.max_frame_size = PROTOCOL_TEST_FRAME_CAPACITY;
  rc = turbo_flow_protocol_registry_create(1u, &registry);
  if (rc == TURBO_OK)
    rc = turbo_flow_protocol_registry_load(registry, module, reason, sizeof(reason));
  if (rc == TURBO_OK)
    rc = turbo_flow_protocol_owner_create_registered(registry, name, &request, &owner);
  if (rc == TURBO_OK) rc = turbo_flow_protocol_owner_instance(owner, kind, &protocol);
  if (rc != TURBO_OK) {
    turbo_flow_protocol_owner_destroy(owner);
    if (registry) (void)turbo_flow_protocol_registry_destroy(registry);
    return rc;
  }
  *registry_out = registry;
  *owner_out = owner;
  *protocol_out = protocol;
  return TURBO_OK;
}

static void protocol_coronet_plugin_close(turbo_flow_protocol_registry_t *registry,
                                         turbo_flow_protocol_owner_t *owner) {
  turbo_flow_protocol_owner_destroy(owner);
  if (registry) (void)turbo_flow_protocol_registry_destroy(registry);
}

static void protocol_coronet_config_init(turbo_flow_protocol_coronet_config_t *config,
                                        turbo_flow_protocol_t *protocol,
                                        turbo_flow_protocol_coronet_transport_t transport, int port,
                                        protocol_coronet_probe_t *probe) {
  *config = (turbo_flow_protocol_coronet_config_t)TURBO_FLOW_PROTOCOL_CORONET_CONFIG_INIT;
  config->protocol = protocol;
  config->transport = transport;
  config->port = port;
  config->runtime.max_sessions = 4u;
  config->runtime.max_frame_size = PROTOCOL_TEST_FRAME_CAPACITY;
  config->runtime.max_buffered_bytes =
      (config->runtime.max_sessions + 1u) * config->runtime.max_frame_size;
  config->runtime_ops.publish = protocol_coronet_publish;
  config->runtime_ctx = probe;
  config->resolve_identity = protocol_coronet_identity;
  config->identity_ctx = (void *)"device-1";
}

static void protocol_test_socket_close(protocol_test_socket_t socket_handle) {
  if (socket_handle == PROTOCOL_TEST_INVALID_SOCKET) return;
#ifdef _WIN32
  closesocket(socket_handle);
#else
  close(socket_handle);
#endif
}

static unsigned short protocol_test_pick_port(int datagram) {
  struct sockaddr_in address;
  unsigned short port = 0u;
#ifdef _WIN32
  WSADATA wsa_data;
  int address_size = (int)sizeof(address);
  SOCKET socket_handle = INVALID_SOCKET;
  if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) return 0u;
#else
  socklen_t address_size = (socklen_t)sizeof(address);
  int socket_handle = -1;
#endif
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(0);
  socket_handle =
      socket(AF_INET, datagram ? SOCK_DGRAM : SOCK_STREAM, datagram ? IPPROTO_UDP : IPPROTO_TCP);
  if (socket_handle != PROTOCOL_TEST_INVALID_SOCKET) {
    if (bind(socket_handle, (struct sockaddr *)&address, sizeof(address)) == 0 &&
        getsockname(socket_handle, (struct sockaddr *)&address, &address_size) == 0)
      port = ntohs(address.sin_port);
    protocol_test_socket_close(socket_handle);
  }
#ifdef _WIN32
  WSACleanup();
#endif
  return port;
}

static protocol_test_socket_t protocol_test_connect_tcp(unsigned short port) {
  struct sockaddr_in address;
  protocol_test_socket_t socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket_handle == PROTOCOL_TEST_INVALID_SOCKET) return PROTOCOL_TEST_INVALID_SOCKET;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (connect(socket_handle, (const struct sockaddr *)&address, sizeof(address)) != 0) {
    protocol_test_socket_close(socket_handle);
    return PROTOCOL_TEST_INVALID_SOCKET;
  }
  return socket_handle;
}

static int protocol_test_send_udp(unsigned short port, const uint8_t *data, size_t size) {
  struct sockaddr_in address;
  protocol_test_socket_t socket_handle = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
#ifdef _WIN32
  int sent;
#else
  ssize_t sent;
#endif
  if (socket_handle == PROTOCOL_TEST_INVALID_SOCKET) return TURBO_EIO;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
#ifdef _WIN32
  sent = sendto(socket_handle, (const char *)data, (int)size, 0, (const struct sockaddr *)&address,
                sizeof(address));
#else
  sent = sendto(socket_handle, data, size, 0, (const struct sockaddr *)&address, sizeof(address));
#endif
  protocol_test_socket_close(socket_handle);
  return sent == (int)size ? TURBO_OK : TURBO_EIO;
}

static int protocol_test_send_tcp(protocol_test_socket_t socket_handle, const uint8_t *data,
                                 size_t size) {
#ifdef _WIN32
  int sent = send(socket_handle, (const char *)data, (int)size, 0);
#else
  ssize_t sent = send(socket_handle, data, size, 0);
#endif
  return sent == (int)size ? TURBO_OK : TURBO_EIO;
}

static int protocol_test_wait_socket_readable(protocol_test_socket_t socket_handle,
                                             unsigned timeout_ms) {
  fd_set read_set;
  struct timeval timeout;
  int ready;
  if (socket_handle == PROTOCOL_TEST_INVALID_SOCKET) return TURBO_EINVAL;
  FD_ZERO(&read_set);
  FD_SET(socket_handle, &read_set);
  timeout.tv_sec = (long)(timeout_ms / 1000u);
  timeout.tv_usec = (long)((timeout_ms % 1000u) * 1000u);
#ifdef _WIN32
  ready = select(0, &read_set, NULL, NULL, &timeout);
#else
  ready = select(socket_handle + 1, &read_set, NULL, NULL, &timeout);
#endif
  if (ready == 0) return TURBO_ETIMEDOUT;
  return ready > 0 ? TURBO_OK : TURBO_EIO;
}

static int protocol_test_recv_exact(protocol_test_socket_t socket_handle, uint8_t *data,
                                   size_t size) {
  size_t received = 0u;
  if (!data || size == 0u) return TURBO_EINVAL;
  while (received < size) {
#ifdef _WIN32
    int count = recv(socket_handle, (char *)data + received, (int)(size - received), 0);
#else
    ssize_t count = recv(socket_handle, data + received, size - received, 0);
#endif
    if (count <= 0) return TURBO_EIO;
    received += (size_t)count;
  }
  return TURBO_OK;
}

static int protocol_test_wait_publishes(protocol_coronet_probe_t *probe, int expected) {
  const uint64_t deadline = turbo_monotonic_ms() + PROTOCOL_TEST_WAIT_MS;
  while (atomic_load_explicit(&probe->publishes, memory_order_acquire) < expected &&
         turbo_monotonic_ms() < deadline)
    turbo_sleep_ms(1u);
  return atomic_load_explicit(&probe->publishes, memory_order_acquire) >= expected
             ? TURBO_OK
             : TURBO_ETIMEDOUT;
}

static int protocol_test_wait_quiescent(turbo_flow_protocol_coronet_server_t *server,
                                       turbo_flow_protocol_coronet_snapshot_t *out) {
  const uint64_t deadline = turbo_monotonic_ms() + PROTOCOL_TEST_WAIT_MS;
  turbo_flow_protocol_coronet_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_CORONET_SNAPSHOT_INIT;
  int rc;
  if (!server) return TURBO_EINVAL;
  do {
    snapshot = (turbo_flow_protocol_coronet_snapshot_t)TURBO_FLOW_PROTOCOL_CORONET_SNAPSHOT_INIT;
    rc = turbo_flow_protocol_coronet_server_snapshot(server, &snapshot);
    if (rc != TURBO_OK) return rc;
    if (snapshot.active_handlers == 0u && snapshot.runtime.active_sessions == 0u &&
        snapshot.runtime.pending_settlements == 0u && snapshot.runtime.buffered_bytes == 0u) {
      if (out) *out = snapshot;
      return TURBO_OK;
    }
    turbo_sleep_ms(1u);
  } while (turbo_monotonic_ms() < deadline);
  if (out) *out = snapshot;
  return TURBO_ETIMEDOUT;
}

static void protocol_test_gbt_frame(uint8_t frame[25]) {
  static const char vin[] = "L1234567890123456";
  uint8_t checksum = 0u;
  memset(frame, 0, 25u);
  frame[0] = 0x23u;
  frame[1] = 0x23u;
  frame[2] = 0x02u;
  frame[3] = 0xfeu;
  memcpy(frame + 4u, vin, sizeof(vin) - 1u);
  frame[21] = 0x01u;
  frame[22] = 0u;
  frame[23] = 0u;
  for (size_t i = 2u; i < 24u; ++i)
    checksum ^= frame[i];
  frame[24] = checksum;
}

static void protocol_ws_client_task(coro_t *coroutine, void *arg) {
  protocol_ws_client_t *client = (protocol_ws_client_t *)arg;
  coro_socket_t *socket_handle;
  turbo_tls_client_config_t tls = {0};
  (void)coroutine;
  socket_handle = client->secure ? coro_socket_create(client->context, CORO_SOCKET_TLS)
                                 : coro_socket_create_tcpv4(client->context);
  if (!socket_handle) {
    client->status = TURBO_ENOMEM;
    client->done = 1;
    return;
  }
  coro_socket_set_timeout(socket_handle, PROTOCOL_TEST_WAIT_MS);
  if (client->secure) {
    tls.ca_file = client->ca_file;
    tls.cert_file = client->cert_file;
    tls.key_file = client->key_file;
    tls.verify_peer = 1;
    client->status = coro_socket_set_tls_client_config(socket_handle, &tls);
  }
  if (client->status == TURBO_OK) {
    if (client->direct_tls)
      client->status = coro_socket_connect(socket_handle, "localhost", client->port);
    else
      client->status =
          coro_socket_connect_ws_ex(socket_handle, client->secure ? "localhost" : "127.0.0.1",
                                    client->port, "/ocpp", client->secure, "ocpp2.0.1");
  }
  if (client->status == TURBO_OK)
    client->status = client->direct_tls
                         ? coro_socket_send(socket_handle, client->payload,
                                            client->payload_size ? client->payload_size
                                                                 : strlen(client->payload))
                         : coro_socket_send_ws_text(socket_handle, client->payload,
                                                    client->payload_size ? client->payload_size
                                                                         : strlen(client->payload));
  while (client->hold_after_send && client->status == TURBO_OK &&
         !atomic_load_explicit(&client->release_after_send, memory_order_acquire))
    coro_sleep(client->context, 1u);
  if (client->wait_for_rejection && client->status == TURBO_OK) {
    char *unexpected = NULL;
    size_t unexpected_size = 0u;
    client->status = coro_socket_recv(socket_handle, &unexpected, &unexpected_size);
    if (unexpected) coro_socket_free_recv(unexpected);
    if (client->status == TURBO_OK) client->status = TURBO_EPROTO;
  }
  coro_socket_destroy(socket_handle);
  client->done = 1;
}

static int protocol_test_run_coronet_client(protocol_ws_client_t *client) {
  coro_context_t *context;
  int rc;
  if (!client) return TURBO_EINVAL;
  context = coro_context_create(NULL);
  if (!context) return TURBO_ENOMEM;
  client->context = context;
  client->status = TURBO_OK;
  client->done = 0;
  rc = coro_context_spawn(context, protocol_ws_client_task, client);
  if (rc == TURBO_OK) rc = coro_context_run(context, TURBO_RUN_DEFAULT);
  coro_context_destroy(context);
  return rc;
}

static void protocol_test_coronet_client_thread(void *arg) {
  protocol_ws_client_t *client = (protocol_ws_client_t *)arg;
  if (client) client->run_status = protocol_test_run_coronet_client(client);
}

static int protocol_test_coronet_server_cleanup(turbo_flow_protocol_coronet_server_t *server) {
  int rc;
  if (!server) return TURBO_OK;
  rc = turbo_flow_protocol_coronet_server_destroy(server);
  if (rc != TURBO_EBUSY) return rc;
  rc = turbo_flow_protocol_coronet_server_begin_shutdown(server);
  if (rc == TURBO_OK)
    rc = turbo_flow_protocol_coronet_server_wait_shutdown(server, PROTOCOL_TEST_SHUTDOWN_WAIT_MS);
  if (rc == TURBO_ETIMEDOUT)
    rc = turbo_flow_protocol_coronet_server_force_shutdown(
        server, TURBO_ECANCELED, PROTOCOL_TEST_SHUTDOWN_WAIT_MS);
  if (rc == TURBO_OK) rc = turbo_flow_protocol_coronet_server_destroy(server);
  return rc;
}

typedef enum protocol_test_mtls_fault_e {
  PROTOCOL_TEST_MTLS_NO_FAULT = 0,
  PROTOCOL_TEST_MTLS_FAIL_AFTER_PLUGIN_OPEN
} protocol_test_mtls_fault_t;

static int protocol_test_wss_mtls_identity_trace(protocol_test_mtls_fault_t fault) {
  static const char payload[] = "[2,\"84\",\"BootNotification\",{}]";
  char cert_file[512] = {0};
  char key_file[512] = {0};
  protocol_coronet_probe_t probe;
  protocol_ws_client_t missing_client;
  protocol_ws_client_t verified_client;
  turbo_flow_protocol_registry_t *registry = NULL;
  turbo_flow_protocol_owner_t *owner = NULL;
  turbo_flow_protocol_t *protocol = NULL;
  turbo_flow_protocol_coronet_server_t *server = NULL;
  turbo_flow_protocol_coronet_config_t config;
  const unsigned short port = protocol_test_pick_port(0);
  int cleanup_rc;
  int rc = TURBO_OK;

  protocol_coronet_probe_init(&probe);
  memset(&missing_client, 0, sizeof(missing_client));
  memset(&verified_client, 0, sizeof(verified_client));
  if (port == 0u) {
    rc = TURBO_EIO;
    goto cleanup;
  }
  rc = tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file));
  if (rc != TURBO_OK) goto cleanup;
  rc = protocol_coronet_plugin_open(FLOW_PROTOCOL_OCPP_MODULE, "ocpp",
                                   TURBO_FLOW_PROTOCOL_OCPP, "2.0.1", &registry, &owner,
                                   &protocol);
  if (rc != TURBO_OK) goto cleanup;
  if (fault == PROTOCOL_TEST_MTLS_FAIL_AFTER_PLUGIN_OPEN) {
    rc = TURBO_ECANCELED;
    goto cleanup;
  }

  protocol_coronet_config_init(&config, protocol, TURBO_FLOW_PROTOCOL_CORONET_WSS, port, &probe);
  config.path = "/ocpp";
  config.subprotocol = "ocpp2.0.1";
  config.tls.cert_file = cert_file;
  config.tls.key_file = key_file;
  config.tls.ca_file = cert_file;
  config.tls.client_auth = TURBO_TLS_CLIENT_AUTH_REQUIRED;
  config.resolve_identity = protocol_coronet_mtls_identity;
  config.identity_ctx = &probe;
  rc = turbo_flow_protocol_coronet_server_create(&config, &server);
  if (rc != TURBO_OK) goto cleanup;
  rc = turbo_flow_protocol_coronet_server_start(server);
  if (rc != TURBO_OK) goto cleanup;

  missing_client.port = port;
  missing_client.payload = payload;
  missing_client.secure = 1;
  missing_client.ca_file = cert_file;
  missing_client.wait_for_rejection = 1;
  rc = protocol_test_run_coronet_client(&missing_client);
  if (rc != TURBO_OK) goto cleanup;
  if (!missing_client.done || missing_client.status == TURBO_OK) {
    rc = TURBO_EPROTO;
    goto cleanup;
  }
  turbo_sleep_ms(20u);
  if (atomic_load_explicit(&probe.publishes, memory_order_acquire) != 0 ||
      atomic_load_explicit(&probe.verified_identities, memory_order_acquire) != 0) {
    rc = TURBO_EPROTO;
    goto cleanup;
  }

  verified_client.port = port;
  verified_client.payload = payload;
  verified_client.secure = 1;
  verified_client.ca_file = cert_file;
  verified_client.cert_file = cert_file;
  verified_client.key_file = key_file;
  rc = protocol_test_run_coronet_client(&verified_client);
  if (rc != TURBO_OK) goto cleanup;
  if (!verified_client.done || verified_client.status != TURBO_OK) {
    rc = TURBO_EPROTO;
    goto cleanup;
  }
  rc = protocol_test_wait_publishes(&probe, 1);
  if (rc != TURBO_OK) goto cleanup;
  if (atomic_load_explicit(&probe.verified_identities, memory_order_acquire) != 1) {
    rc = TURBO_EPROTO;
    goto cleanup;
  }

cleanup:
  if (server) {
    cleanup_rc = protocol_test_coronet_server_cleanup(server);
    if (rc == TURBO_OK && cleanup_rc != TURBO_OK) rc = cleanup_rc;
  }
  protocol_coronet_plugin_close(registry, owner);
  tls_test_remove_file(key_file);
  tls_test_remove_file(cert_file);
  return rc;
}

static int protocol_test_wss_mtls_reconnect_trace(void) {
  static const char payload[] = "[2,\"reconnect\",\"BootNotification\",{}]";
  char cert_file[512] = {0};
  char key_file[512] = {0};
  protocol_coronet_probe_t probe;
  turbo_flow_protocol_registry_t *registry = NULL;
  turbo_flow_protocol_owner_t *owner = NULL;
  turbo_flow_protocol_t *protocol = NULL;
  turbo_flow_protocol_coronet_server_t *server = NULL;
  turbo_flow_protocol_coronet_config_t config;
  const unsigned short port = protocol_test_pick_port(0);
  const char *stage = "pick-port";
  int iteration = -1;
  int cleanup_rc;
  int rc = TURBO_OK;

  protocol_coronet_probe_init(&probe);
  if (port == 0u) {
    rc = TURBO_EIO;
    goto cleanup;
  }
  stage = "write-credentials";
  rc = tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file));
  if (rc != TURBO_OK) goto cleanup;
  stage = "plugin-open";
  rc = protocol_coronet_plugin_open(FLOW_PROTOCOL_OCPP_MODULE, "ocpp",
                                   TURBO_FLOW_PROTOCOL_OCPP, "2.0.1", &registry, &owner,
                                   &protocol);
  if (rc != TURBO_OK) goto cleanup;
  protocol_coronet_config_init(&config, protocol, TURBO_FLOW_PROTOCOL_CORONET_WSS, port, &probe);
  config.path = "/ocpp";
  config.subprotocol = "ocpp2.0.1";
  config.tls.cert_file = cert_file;
  config.tls.key_file = key_file;
  config.tls.ca_file = cert_file;
  config.tls.client_auth = TURBO_TLS_CLIENT_AUTH_REQUIRED;
  config.resolve_identity = protocol_coronet_mtls_identity;
  config.identity_ctx = &probe;
  stage = "server-create";
  rc = turbo_flow_protocol_coronet_server_create(&config, &server);
  if (rc != TURBO_OK) goto cleanup;
  stage = "server-start";
  rc = turbo_flow_protocol_coronet_server_start(server);
  if (rc != TURBO_OK) goto cleanup;

  for (iteration = 0; iteration < PROTOCOL_TEST_RECONNECT_ITERATIONS; ++iteration) {
    protocol_ws_client_t client;
    turbo_thread_t client_thread;
    turbo_flow_protocol_coronet_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_CORONET_SNAPSHOT_INIT;
    int join_rc;
    memset(&client, 0, sizeof(client));
    client.port = port;
    client.payload = payload;
    client.secure = 1;
    client.ca_file = cert_file;
    client.cert_file = cert_file;
    client.key_file = key_file;
    client.hold_after_send = 1;
    atomic_init(&client.release_after_send, 0);
    stage = "client-thread-create";
    rc = turbo_thread_create(&client_thread, protocol_test_coronet_client_thread, &client);
    if (rc != TURBO_OK) goto cleanup;
    stage = "publish";
    rc = protocol_test_wait_publishes(&probe, iteration + 1);
    atomic_store_explicit(&client.release_after_send, 1, memory_order_release);
    join_rc = turbo_thread_join(&client_thread);
    turbo_thread_destroy(&client_thread);
    if (rc == TURBO_OK && join_rc != TURBO_OK) {
      stage = "client-thread-join";
      rc = join_rc;
    }
    if (rc != TURBO_OK) goto cleanup;
    if (client.run_status != TURBO_OK) {
      stage = "client-run";
      rc = client.run_status;
      goto cleanup;
    }
    if (!client.done) {
      stage = "client-completion";
      rc = TURBO_EPROTO;
      goto cleanup;
    }
    if (client.status != TURBO_OK) {
      stage = "client-status";
      rc = client.status;
      goto cleanup;
    }
    stage = "quiescence";
    rc = protocol_test_wait_quiescent(server, &snapshot);
    if (rc != TURBO_OK) goto cleanup;
    if (snapshot.active_handlers != 0u || snapshot.runtime.active_sessions != 0u ||
        snapshot.runtime.pending_settlements != 0u || snapshot.runtime.buffered_bytes != 0u ||
        !snapshot.runtime.accepting) {
      stage = "quiescent-snapshot";
      rc = TURBO_EPROTO;
      goto cleanup;
    }
  }
  stage = "publish-count";
  if (atomic_load_explicit(&probe.publishes, memory_order_acquire) !=
      PROTOCOL_TEST_RECONNECT_ITERATIONS) {
    rc = TURBO_EPROTO;
    goto cleanup;
  }
  stage = "identity-count";
  if (atomic_load_explicit(&probe.verified_identities, memory_order_acquire) !=
      PROTOCOL_TEST_RECONNECT_ITERATIONS) {
    rc = TURBO_EPROTO;
    goto cleanup;
  }

cleanup:
  cleanup_rc = protocol_test_coronet_server_cleanup(server);
  if (cleanup_rc != TURBO_OK) {
    fprintf(stderr,
            "PROTOCOL_TRACE_CLEANUP_FAILURE trace=wss-mtls-reconnect stage=%s "
            "iteration=%d status=%d cleanup_status=%d\n",
            stage, iteration, rc, cleanup_rc);
    if (rc == TURBO_OK) rc = cleanup_rc;
  }
  protocol_coronet_plugin_close(registry, owner);
  tls_test_remove_file(key_file);
  tls_test_remove_file(cert_file);
  if (rc != TURBO_OK)
    fprintf(stderr,
            "PROTOCOL_TRACE_FAILURE trace=wss-mtls-reconnect stage=%s iteration=%d status=%d\n",
            stage, iteration, rc);
  return rc;
}

static int protocol_test_tcp_settlement_shutdown_trace(void) {
  uint8_t frame[25];
  uint8_t reply[25];
  protocol_coronet_probe_t probe;
  turbo_flow_protocol_registry_t *registry = NULL;
  turbo_flow_protocol_owner_t *owner = NULL;
  turbo_flow_protocol_t *protocol = NULL;
  turbo_flow_protocol_coronet_server_t *server = NULL;
  turbo_flow_protocol_coronet_config_t config;
  turbo_flow_protocol_feed_result_t result =
      TURBO_FLOW_PROTOCOL_FEED_RESULT_INIT;
  protocol_test_socket_t client = PROTOCOL_TEST_INVALID_SOCKET;
  const unsigned short port = protocol_test_pick_port(0);
  const char *stage = "pick-port";
  uint64_t delivery_id = 0u;
  int cleanup_rc;
  int rc = TURBO_OK;

  protocol_test_gbt_frame(frame);
  protocol_coronet_probe_init(&probe);
  probe.pending = 1;
  if (port == 0u) {
    rc = TURBO_EIO;
    goto cleanup;
  }
  stage = "plugin-open";
  rc = protocol_coronet_plugin_open(
      FLOW_PROTOCOL_GBT32960_MODULE, "gbt32960",
      TURBO_FLOW_PROTOCOL_GBT_32960, "2025", &registry, &owner,
      &protocol);
  if (rc != TURBO_OK) goto cleanup;
  protocol_coronet_config_init(&config, protocol,
                              TURBO_FLOW_PROTOCOL_CORONET_TCP, port, &probe);
  config.resolve_identity = NULL;
  config.identity_ctx = NULL;
  stage = "server-create";
  rc = turbo_flow_protocol_coronet_server_create(&config, &server);
  if (rc != TURBO_OK) goto cleanup;
  stage = "server-start";
  rc = turbo_flow_protocol_coronet_server_start(server);
  if (rc != TURBO_OK) goto cleanup;
  stage = "client-connect";
  client = protocol_test_connect_tcp(port);
  if (client == PROTOCOL_TEST_INVALID_SOCKET) {
    rc = TURBO_ECONNREFUSED;
    goto cleanup;
  }
  stage = "client-send";
  rc = protocol_test_send_tcp(client, frame, sizeof(frame));
  if (rc != TURBO_OK) goto cleanup;
  stage = "publish";
  rc = protocol_test_wait_publishes(&probe, 1);
  if (rc != TURBO_OK) goto cleanup;
  delivery_id =
      atomic_load_explicit(&probe.delivery_id, memory_order_acquire);
  if (delivery_id == 0u) {
    rc = TURBO_EPROTO;
    goto cleanup;
  }
  stage = "reply-before-settlement";
  rc = protocol_test_wait_socket_readable(client, 20u);
  if (rc != TURBO_ETIMEDOUT) {
    if (rc == TURBO_OK) rc = TURBO_EPROTO;
    goto cleanup;
  }
  stage = "begin-shutdown";
  rc = turbo_flow_protocol_coronet_server_begin_shutdown(server);
  if (rc != TURBO_OK) goto cleanup;
  stage = "pending-shutdown-fence";
  rc = turbo_flow_protocol_coronet_server_wait_shutdown(server, 20u);
  if (rc != TURBO_ETIMEDOUT) {
    if (rc == TURBO_OK) rc = TURBO_EPROTO;
    goto cleanup;
  }
  stage = "settle";
  rc = turbo_flow_protocol_coronet_server_settle(server, delivery_id,
                                                TURBO_OK, &result);
  if (rc != TURBO_OK) goto cleanup;
  stage = "reply-ready";
  rc = protocol_test_wait_socket_readable(client, PROTOCOL_TEST_WAIT_MS);
  if (rc != TURBO_OK) goto cleanup;
  stage = "reply-receive";
  rc = protocol_test_recv_exact(client, reply, sizeof(reply));
  if (rc != TURBO_OK) goto cleanup;
  stage = "reply-validate";
  if (reply[0] != UINT8_C(0x23) || reply[1] != UINT8_C(0x23) ||
      reply[2] != frame[2] || reply[3] != UINT8_C(0x01) ||
      memcmp(reply + 4u, frame + 4u, 17u) != 0) {
    rc = TURBO_EPROTO;
    goto cleanup;
  }
  stage = "shutdown-drain";
  rc = turbo_flow_protocol_coronet_server_wait_shutdown(
      server, PROTOCOL_TEST_WAIT_MS);

cleanup:
  if (client != PROTOCOL_TEST_INVALID_SOCKET)
    protocol_test_socket_close(client);
  cleanup_rc = protocol_test_coronet_server_cleanup(server);
  if (cleanup_rc != TURBO_OK) {
    fprintf(stderr,
            "PROTOCOL_TRACE_CLEANUP_FAILURE "
            "trace=tcp-settlement-shutdown stage=%s status=%d "
            "cleanup_status=%d delivery_id=%" PRIu64 "\n",
            stage, rc, cleanup_rc, delivery_id);
    if (rc == TURBO_OK) rc = cleanup_rc;
  }
  protocol_coronet_plugin_close(registry, owner);
  if (rc != TURBO_OK)
    fprintf(stderr,
            "PROTOCOL_TRACE_FAILURE trace=tcp-settlement-shutdown "
            "stage=%s status=%d delivery_id=%" PRIu64 "\n",
            stage, rc, delivery_id);
  return rc;
}

spec("protocol CoroNet transport owner") {
  it("rejects incompatible transports and implicit LwM2M downgrade") {
    protocol_coronet_probe_t probe;
    turbo_flow_protocol_registry_t *registry = NULL;
    turbo_flow_protocol_owner_t *owner = NULL;
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_coronet_server_t *server = NULL;
    turbo_flow_protocol_coronet_config_t config;
    protocol_coronet_probe_init(&probe);
    check_equal(protocol_coronet_plugin_open(FLOW_PROTOCOL_LWM2M_MODULE, "lwm2m",
                                             TURBO_FLOW_PROTOCOL_LWM2M, "1.2.2", &registry,
                                             &owner, &protocol),
                 TURBO_OK);
    protocol_coronet_config_init(&config, protocol, TURBO_FLOW_PROTOCOL_CORONET_UDP, 0, &probe);
    check_equal(turbo_flow_protocol_coronet_server_create(&config, &server), TURBO_EPERM);
    check_null(server);
    config.allow_insecure_lwm2m = 1u;
    check_equal(turbo_flow_protocol_coronet_server_create(&config, &server), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_destroy(server), TURBO_OK);
    protocol_coronet_plugin_close(registry, owner);
  }

  it("maps one CoAP datagram on a bounded UDP session") {
    static const uint8_t frame[] = {0x40u, 0x01u, 0x12u, 0x34u};
    protocol_coronet_probe_t probe;
    turbo_flow_protocol_coronet_snapshot_t snapshot =
        TURBO_FLOW_PROTOCOL_CORONET_SNAPSHOT_INIT;
    turbo_flow_protocol_registry_t *registry = NULL;
    turbo_flow_protocol_owner_t *owner = NULL;
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_coronet_server_t *server = NULL;
    turbo_flow_protocol_coronet_config_t config;
    unsigned short port = protocol_test_pick_port(1);
    int shutdown_rc;
    protocol_coronet_probe_init(&probe);
    check_true(port != 0u);
    check_equal(protocol_coronet_plugin_open(FLOW_PROTOCOL_COAP_MODULE, "coap",
                                             TURBO_FLOW_PROTOCOL_COAP, "RFC7252", &registry,
                                             &owner, &protocol),
                 TURBO_OK);
    protocol_coronet_config_init(&config, protocol, TURBO_FLOW_PROTOCOL_CORONET_UDP, port, &probe);
    check_equal(turbo_flow_protocol_coronet_server_create(&config, &server), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_start(server), TURBO_OK);
    check_equal(protocol_test_send_udp(port, frame, sizeof(frame)), TURBO_OK);
    check_equal(protocol_test_wait_publishes(&probe, 1), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_begin_shutdown(server), TURBO_OK);
    shutdown_rc =
        turbo_flow_protocol_coronet_server_wait_shutdown(server,
                                                        PROTOCOL_TEST_WAIT_MS);
    if (shutdown_rc != TURBO_OK &&
        turbo_flow_protocol_coronet_server_snapshot(server, &snapshot) ==
            TURBO_OK)
      fprintf(stderr,
              "PROTOCOL_SHUTDOWN_FAILURE transport=udp status=%d "
              "handlers=%zu sessions=%zu pending=%zu buffered=%zu "
              "network_stopped=%u\n",
              shutdown_rc, snapshot.active_handlers,
              snapshot.runtime.active_sessions,
              snapshot.runtime.pending_settlements,
              snapshot.runtime.buffered_bytes, snapshot.network_stopped);
    check_equal(shutdown_rc, TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_destroy(server), TURBO_OK);
    protocol_coronet_plugin_close(registry, owner);
  }

  it("keeps the event loop live until a pending TCP publication settles") {
    check_equal(protocol_test_tcp_settlement_shutdown_trace(), TURBO_OK);
  }

  it("enforces the OCPP path and subprotocol on WebSocket ingress") {
    static const char payload[] = "[2,\"42\",\"BootNotification\",{}]";
    protocol_coronet_probe_t probe;
    protocol_ws_client_t client;
    turbo_flow_protocol_registry_t *registry = NULL;
    turbo_flow_protocol_owner_t *owner = NULL;
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_coronet_server_t *server = NULL;
    turbo_flow_protocol_coronet_config_t config;
    unsigned short port = protocol_test_pick_port(0);
    protocol_coronet_probe_init(&probe);
    memset(&client, 0, sizeof(client));
    check_true(port != 0u);
    check_equal(protocol_coronet_plugin_open(FLOW_PROTOCOL_OCPP_MODULE, "ocpp",
                                             TURBO_FLOW_PROTOCOL_OCPP, "2.0.1", &registry,
                                             &owner, &protocol),
                 TURBO_OK);
    protocol_coronet_config_init(&config, protocol, TURBO_FLOW_PROTOCOL_CORONET_WS, port, &probe);
    config.path = "/ocpp";
    config.subprotocol = "ocpp2.0.1";
    check_equal(turbo_flow_protocol_coronet_server_create(&config, &server), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_start(server), TURBO_OK);
    client.port = port;
    client.payload = payload;
    check_equal(protocol_test_run_coronet_client(&client), TURBO_OK);
    check_true(client.done);
    check_equal(client.status, TURBO_OK);
    check_equal(protocol_test_wait_publishes(&probe, 1), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_begin_shutdown(server), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_wait_shutdown(server, PROTOCOL_TEST_WAIT_MS),
                 TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_destroy(server), TURBO_OK);
    protocol_coronet_plugin_close(registry, owner);
  }

  it("maps GB/T 32960 over an explicitly configured TLS listener") {
    uint8_t frame[25];
    char cert_file[512] = {0};
    char key_file[512] = {0};
    protocol_coronet_probe_t probe;
    protocol_ws_client_t client;
    turbo_flow_protocol_registry_t *registry = NULL;
    turbo_flow_protocol_owner_t *owner = NULL;
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_coronet_server_t *server = NULL;
    turbo_flow_protocol_coronet_config_t config;
    unsigned short port = protocol_test_pick_port(0);
    protocol_test_gbt_frame(frame);
    protocol_coronet_probe_init(&probe);
    memset(&client, 0, sizeof(client));
    check_true(port != 0u);
    check_equal(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)),
        TURBO_OK);
    check_equal(protocol_coronet_plugin_open(FLOW_PROTOCOL_GBT32960_MODULE, "gbt32960",
                                             TURBO_FLOW_PROTOCOL_GBT_32960, "2025",
                                             &registry, &owner, &protocol),
                 TURBO_OK);
    protocol_coronet_config_init(&config, protocol, TURBO_FLOW_PROTOCOL_CORONET_TLS, port, &probe);
    config.resolve_identity = NULL;
    config.identity_ctx = NULL;
    config.tls.cert_file = cert_file;
    config.tls.key_file = key_file;
    check_equal(turbo_flow_protocol_coronet_server_create(&config, &server), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_start(server), TURBO_OK);
    client.port = port;
    client.payload = (const char *)frame;
    client.payload_size = sizeof(frame);
    client.secure = 1;
    client.direct_tls = 1;
    client.ca_file = cert_file;
    check_equal(protocol_test_run_coronet_client(&client), TURBO_OK);
    check_true(client.done);
    check_equal(client.status, TURBO_OK);
    check_equal(protocol_test_wait_publishes(&probe, 1), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_begin_shutdown(server), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_wait_shutdown(server, PROTOCOL_TEST_WAIT_MS),
                 TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_destroy(server), TURBO_OK);
    protocol_coronet_plugin_close(registry, owner);
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
  }

  it("reconnects TLS clients without retaining session resources") {
    uint8_t frame[25];
    char cert_file[512] = {0};
    char key_file[512] = {0};
    protocol_coronet_probe_t probe;
    turbo_flow_protocol_registry_t *registry = NULL;
    turbo_flow_protocol_owner_t *owner = NULL;
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_coronet_server_t *server = NULL;
    turbo_flow_protocol_coronet_config_t config;
    unsigned short port = protocol_test_pick_port(0);
    protocol_test_gbt_frame(frame);
    protocol_coronet_probe_init(&probe);
    check_true(port != 0u);
    check_equal(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)),
        TURBO_OK);
    check_equal(protocol_coronet_plugin_open(FLOW_PROTOCOL_GBT32960_MODULE, "gbt32960",
                                             TURBO_FLOW_PROTOCOL_GBT_32960, "2025",
                                             &registry, &owner, &protocol),
                 TURBO_OK);
    protocol_coronet_config_init(&config, protocol, TURBO_FLOW_PROTOCOL_CORONET_TLS, port, &probe);
    config.resolve_identity = NULL;
    config.identity_ctx = NULL;
    config.tls.cert_file = cert_file;
    config.tls.key_file = key_file;
    check_equal(turbo_flow_protocol_coronet_server_create(&config, &server), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_start(server), TURBO_OK);
    for (int iteration = 0; iteration < PROTOCOL_TEST_RECONNECT_ITERATIONS; ++iteration) {
      protocol_ws_client_t client;
      turbo_flow_protocol_coronet_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_CORONET_SNAPSHOT_INIT;
      memset(&client, 0, sizeof(client));
      client.port = port;
      client.payload = (const char *)frame;
      client.payload_size = sizeof(frame);
      client.secure = 1;
      client.direct_tls = 1;
      client.ca_file = cert_file;
      check_equal(protocol_test_run_coronet_client(&client), TURBO_OK);
      check_true(client.done);
      check_equal(client.status, TURBO_OK);
      check_equal(protocol_test_wait_publishes(&probe, iteration + 1), TURBO_OK);
      check_equal(protocol_test_wait_quiescent(server, &snapshot), TURBO_OK);
      check_equal(snapshot.active_handlers, 0u);
      check_equal(snapshot.runtime.active_sessions, 0u);
      check_equal(snapshot.runtime.pending_settlements, 0u);
      check_equal(snapshot.runtime.buffered_bytes, 0u);
      check_true(snapshot.runtime.accepting);
    }
    check_equal(atomic_load_explicit(&probe.publishes, memory_order_acquire),
                 PROTOCOL_TEST_RECONNECT_ITERATIONS);
    check_equal(turbo_flow_protocol_coronet_server_begin_shutdown(server), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_wait_shutdown(server, PROTOCOL_TEST_WAIT_MS),
                 TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_destroy(server), TURBO_OK);
    protocol_coronet_plugin_close(registry, owner);
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
  }

  it("releases a failed TLS startup before same-port recovery") {
    static const char malformed_pem[] = "not a PEM credential\n";
    char cert_file[512] = {0};
    char key_file[512] = {0};
    protocol_coronet_probe_t probe;
    turbo_flow_protocol_registry_t *registry = NULL;
    turbo_flow_protocol_owner_t *owner = NULL;
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_coronet_server_t *server = NULL;
    turbo_flow_protocol_coronet_config_t config;
    unsigned short port = protocol_test_pick_port(0);
    protocol_coronet_probe_init(&probe);
    check_true(port != 0u);
    check_equal(tls_test_write_temp_file(cert_file, sizeof(cert_file), "bad-crt", malformed_pem),
                 TURBO_OK);
    check_equal(tls_test_write_temp_file(key_file, sizeof(key_file), "bad-key", malformed_pem),
                 TURBO_OK);
    check_equal(protocol_coronet_plugin_open(FLOW_PROTOCOL_GBT32960_MODULE, "gbt32960",
                                             TURBO_FLOW_PROTOCOL_GBT_32960, "2025",
                                             &registry, &owner, &protocol),
                 TURBO_OK);
    protocol_coronet_config_init(&config, protocol, TURBO_FLOW_PROTOCOL_CORONET_TLS, port, &probe);
    config.resolve_identity = NULL;
    config.identity_ctx = NULL;
    config.tls.cert_file = cert_file;
    config.tls.key_file = key_file;
    check_equal(turbo_flow_protocol_coronet_server_create(&config, &server), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_start(server), TURBO_EIO);
    check_equal(turbo_flow_protocol_coronet_server_destroy(server), TURBO_OK);
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
    check_equal(
        tls_test_write_server_files(cert_file, sizeof(cert_file), key_file, sizeof(key_file)),
        TURBO_OK);
    config.tls.cert_file = cert_file;
    config.tls.key_file = key_file;
    server = NULL;
    check_equal(turbo_flow_protocol_coronet_server_create(&config, &server), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_start(server), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_begin_shutdown(server), TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_wait_shutdown(server, PROTOCOL_TEST_WAIT_MS),
                 TURBO_OK);
    check_equal(turbo_flow_protocol_coronet_server_destroy(server), TURBO_OK);
    protocol_coronet_plugin_close(registry, owner);
    tls_test_remove_file(key_file);
    tls_test_remove_file(cert_file);
  }

  it("requires a verified client certificate before WSS identity admission") {
    check_equal(protocol_test_wss_mtls_identity_trace(PROTOCOL_TEST_MTLS_NO_FAULT), TURBO_OK);
  }

  it("releases WSS plugin ownership when the identity trace aborts after open") {
    check_equal(protocol_test_wss_mtls_identity_trace(PROTOCOL_TEST_MTLS_FAIL_AFTER_PLUGIN_OPEN),
                 TURBO_ECANCELED);
  }

  it("reconnects authenticated WSS clients without retaining session resources") {
    check_equal(protocol_test_wss_mtls_reconnect_trace(), TURBO_OK);
  }
}
