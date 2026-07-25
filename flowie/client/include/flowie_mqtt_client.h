#ifndef FLOWIE_MQTT_CLIENT_H
#define FLOWIE_MQTT_CLIENT_H

#include "flowie_mqtt_types.h"

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32) || defined(__CYGWIN__)
  #if defined(FLOWIE_MQTT_CLIENT_BUILD)
    #define FLOWIE_MQTT_CLIENT_API __declspec(dllexport)
  #else
    #define FLOWIE_MQTT_CLIENT_API __declspec(dllimport)
  #endif
#elif defined(__GNUC__) && __GNUC__ >= 4
  #define FLOWIE_MQTT_CLIENT_API __attribute__((visibility("default")))
#else
  #define FLOWIE_MQTT_CLIENT_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_MQTT_CLIENT_DEFAULT_PORT 1883
#define FLOWIE_MQTT_CLIENT_DEFAULT_TLS_PORT 8883
#define FLOWIE_MQTT_CLIENT_DEFAULT_TIMEOUT_MS 30000u
#define FLOWIE_MQTT_CLIENT_DEFAULT_MAX_PACKET_SIZE (1024u * 1024u)
#define FLOWIE_MQTT_CLIENT_DEFAULT_MAX_INBOUND_QOS2 64u
#define FLOWIE_MQTT_CLIENT_DEFAULT_COMMAND_QUEUE_CAPACITY 64u
#define FLOWIE_MQTT_CLIENT_DEFAULT_COMMAND_QUEUE_BYTES (4u * 1024u * 1024u)
#define FLOWIE_MQTT_CLIENT_MIN_STREAM_RECV_BUFFER_SIZE 1024u
#define FLOWIE_MQTT_CLIENT_MAX_STREAM_RECV_BUFFER_SIZE (1024u * 1024u)

typedef struct flowie_mqtt_client_s flowie_mqtt_client_t;

typedef enum flowie_mqtt_client_transport_e {
  FLOWIE_MQTT_CLIENT_TRANSPORT_TCP = 1,
  FLOWIE_MQTT_CLIENT_TRANSPORT_TLS,
  FLOWIE_MQTT_CLIENT_TRANSPORT_WS,
  FLOWIE_MQTT_CLIENT_TRANSPORT_WSS
} flowie_mqtt_client_transport_t;

/**
 * Optional verified TLS/WSS client identity. Strings are copied at client creation.
 * cert_file and key_file must be configured together. key_password is wiped when
 * the client is destroyed. Peer verification cannot be disabled through this API.
 */
typedef struct flowie_mqtt_client_tls_config_s {
  const char *ca_file;
  const char *cert_file;
  const char *key_file;
  const char *key_password;
} flowie_mqtt_client_tls_config_t;

/** One caller-owned topic entry in a client publish request. */
typedef struct flowie_mqtt_client_publish_topic_s {
  uint8_t qos;
  uint8_t retain;
  uint8_t duplicate;
  flowie_mqtt_span_t topic;
  flowie_mqtt_span_t properties;
  flowie_mqtt_span_t payload;
} flowie_mqtt_client_publish_topic_t;

/** Caller-owned vector view of publish topics; data is a borrowed array. */
typedef struct flowie_mqtt_client_publish_topic_vec_s {
  size_t size;
  flowie_mqtt_version_t version;
  const flowie_mqtt_client_publish_topic_t *data;
  size_t count;
} flowie_mqtt_client_publish_topic_vec_t;

#define FLOWIE_MQTT_CLIENT_PUBLISH_TOPIC_VEC_INIT                                                  \
  {sizeof(flowie_mqtt_client_publish_topic_vec_t)}

/**
 * Called on the DLL-owned worker thread for an inbound PUBLISH.
 * Every view is borrowed and remains valid only for the callback duration.
 * Commands may be enqueued from the callback.
 */
typedef int (*flowie_mqtt_client_message_fn)(flowie_mqtt_client_t *client,
                                             const flowie_mqtt_publish_view_t *message,
                                             void *user_data);

/** One topic-filter to inbound-message-handler mapping configured at client creation. */
typedef struct flowie_mqtt_client_topic_handler_s {
  flowie_mqtt_span_t filter;
  flowie_mqtt_client_message_fn on_message;
} flowie_mqtt_client_topic_handler_t;

/**
 * Caller-owned map view; duplicate filters are invalid. Every matching handler
 * is called in map order, stopping on the first non-TURBO_OK result.
 */
typedef struct flowie_mqtt_client_topic_handler_map_s {
  const flowie_mqtt_client_topic_handler_t *data;
  size_t count;
} flowie_mqtt_client_topic_handler_map_t;

/**
 * Completes one accepted command on the DLL-owned worker thread.
 * response is NULL when the operation has no MQTT control response. Otherwise
 * it and all nested spans remain valid only for the callback duration.
 * Commands may be submitted from this callback; client destruction may not be
 * called from it.
 */
typedef void (*flowie_mqtt_client_completion_fn)(flowie_mqtt_client_t *client, int status,
                                                 const flowie_mqtt_control_packet_view_t *response,
                                                 void *user_data);

/**
 * Reports a background error that is not owned by an accepted command, such as
 * an unsolicited broker disconnect or receive failure. The callback runs after
 * the transport is closed on the DLL-owned worker.
 */
typedef void (*flowie_mqtt_client_error_fn)(flowie_mqtt_client_t *client, int status,
                                            void *user_data);

/** One synchronous response to an MQTT 5 Continue Authentication challenge. */
typedef struct flowie_mqtt_client_auth_response_s {
  size_t size;
  /** Must be Continue Authentication (0x18). */
  uint8_t reason_code;
  /**
   * Caller-owned storage. It must remain valid until the next callback for this client begins;
   * callback-local stack storage is not valid here.
   */
  flowie_mqtt_span_t properties;
} flowie_mqtt_client_auth_response_t;

#define FLOWIE_MQTT_CLIENT_AUTH_RESPONSE_INIT                                                      \
  {sizeof(flowie_mqtt_client_auth_response_t), 0x18u, {NULL, 0u}}

/**
 * Produce the next MQTT 5 AUTH response on the client worker thread. `challenge` is borrowed only
 * for this call. Response properties follow the longer lifetime documented on
 * flowie_mqtt_client_auth_response_t. Returning an error fails closed and terminates the current
 * CONNECT or re-authentication exchange.
 */
typedef int (*flowie_mqtt_client_auth_challenge_fn)(
    flowie_mqtt_client_t *client, const flowie_mqtt_control_packet_view_t *challenge,
    flowie_mqtt_client_auth_response_t *response, void *user_data);

typedef struct flowie_mqtt_client_config_s {
  size_t size;
  flowie_mqtt_client_transport_t transport;
  /** Copied by flowie_mqtt_client_create(). */
  const char *host;
  int port;
  /** Copied WS/WSS path; NULL selects "/mqtt". Ignored for TCP/TLS. */
  const char *path;
  uint64_t timeout_ms;
  size_t max_packet_size;
  size_t max_inbound_qos2;
  /** Copied topic-filter map used to dispatch inbound PUBLISH messages. */
  flowie_mqtt_client_topic_handler_map_t topic_handlers;
  flowie_mqtt_client_completion_fn on_connect;
  flowie_mqtt_client_completion_fn on_publish;
  flowie_mqtt_client_completion_fn on_subscribe;
  flowie_mqtt_client_completion_fn on_unsubscribe;
  flowie_mqtt_client_completion_fn on_ping;
  flowie_mqtt_client_completion_fn on_disconnect;
  flowie_mqtt_client_error_fn on_error;
  /** Shared by all callbacks and retained until client destruction. */
  void *user_data;
  /** Bounded command count; zero selects the default. */
  size_t command_queue_capacity;
  /** Maximum total bytes owned by queued commands; zero selects the default. */
  size_t command_queue_max_bytes;
  /** Used only by TLS/WSS; all strings are copied. */
  flowie_mqtt_client_tls_config_t tls;
  /** Required when CONNECT or re-authentication can receive AUTH 0x18. */
  flowie_mqtt_client_auth_challenge_fn on_auth_challenge;
  /** Completion for flowie_mqtt_client_authenticate(). */
  flowie_mqtt_client_completion_fn on_auth;
  /**
   * Capacity of each of the two CoroNet user-space receive buffers.
   * 0 keeps the CoroNet default.
   */
  size_t stream_recv_buffer_bytes;
  /** Requested OS SO_RCVBUF bytes; 0 preserves the OS default. */
  size_t socket_recv_buffer_bytes;
  /** Requested OS SO_SNDBUF bytes; 0 preserves the OS default. */
  size_t socket_send_buffer_bytes;
} flowie_mqtt_client_config_t;

#define FLOWIE_MQTT_CLIENT_CONFIG_INIT                                                             \
  {sizeof(flowie_mqtt_client_config_t),                                                            \
   FLOWIE_MQTT_CLIENT_TRANSPORT_TCP,                                                               \
   NULL,                                                                                           \
   FLOWIE_MQTT_CLIENT_DEFAULT_PORT,                                                                \
   NULL,                                                                                           \
   FLOWIE_MQTT_CLIENT_DEFAULT_TIMEOUT_MS,                                                          \
   FLOWIE_MQTT_CLIENT_DEFAULT_MAX_PACKET_SIZE,                                                     \
   FLOWIE_MQTT_CLIENT_DEFAULT_MAX_INBOUND_QOS2,                                                    \
   {NULL, 0u},                                                                                     \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   NULL,                                                                                           \
   FLOWIE_MQTT_CLIENT_DEFAULT_COMMAND_QUEUE_CAPACITY,                                              \
   FLOWIE_MQTT_CLIENT_DEFAULT_COMMAND_QUEUE_BYTES,                                                 \
   {NULL, NULL, NULL, NULL},                                                                       \
   NULL,                                                                                           \
   NULL,                                                                                           \
   0u,                                                                                             \
   0u,                                                                                             \
   0u}

/**
 * Create a callback-driven client. The DLL owns its CoroNet context, worker
 * thread, receive loop, and bounded command queue. On failure, out is set to
 * NULL.
 */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_create(const flowie_mqtt_client_config_t *config,
                                                     flowie_mqtt_client_t **out);

/**
 * Destroy the client outside its callbacks. Destruction stops admission,
 * interrupts current I/O, completes accepted queued commands with
 * TURBO_ESHUTDOWN, and joins its worker.
 */
FLOWIE_MQTT_CLIENT_API void flowie_mqtt_client_destroy(flowie_mqtt_client_t *client);

/** Read-only connection state; this does not perform I/O. */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_is_connected(const flowie_mqtt_client_t *client);

/**
 * Callback-driven operations. Returning TURBO_OK transfers a deep copy of the
 * complete packet description to the client. Each command guarantees one
 * matching config.on_xxx callback, except publish, which guarantees one
 * on_publish callback per topic in input order. A missing matching callback
 * returns TURBO_ENOTSUP.
 * Queue-full, shutdown, structurally invalid input, and invalid client state
 * fail immediately without invoking a callback. Protocol or network errors are
 * delivered through the matching callback.
 */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_connect(flowie_mqtt_client_t *client,
                                                      const flowie_mqtt_connect_packet_t *packet);
/** Atomically admits all topics or none; topics.count must be greater than zero. */
FLOWIE_MQTT_CLIENT_API int
flowie_mqtt_client_publish(flowie_mqtt_client_t *client,
                           const flowie_mqtt_client_publish_topic_vec_t *topics);
FLOWIE_MQTT_CLIENT_API int
flowie_mqtt_client_subscribe(flowie_mqtt_client_t *client,
                             const flowie_mqtt_subscribe_packet_t *packet);
FLOWIE_MQTT_CLIENT_API int
flowie_mqtt_client_unsubscribe(flowie_mqtt_client_t *client,
                               const flowie_mqtt_unsubscribe_packet_t *packet);
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_ping(flowie_mqtt_client_t *client);
/**
 * Start MQTT 5 re-authentication with AUTH reason 0x19. Properties must contain the selected
 * Authentication Method and may contain Authentication Data. Completion receives the final
 * successful AUTH packet; protocol, callback, or transport failures close the connection.
 */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_authenticate(flowie_mqtt_client_t *client,
                                                           flowie_mqtt_span_t properties);
/**
 * Complete after the transport is closed. EOF/connection-reset observed while
 * performing this no-response MQTT shutdown is treated as successful closure.
 */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_disconnect(flowie_mqtt_client_t *client,
                                                         uint8_t reason_code,
                                                         flowie_mqtt_span_t properties);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_MQTT_CLIENT_H */
