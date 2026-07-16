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

/**
 * Opaque optional external event-loop context. Include
 * CoroNet/turbo_coro_context.h only when creating or driving this context.
 */
typedef struct coro_context_s coro_context_t;

#define FLOWIE_MQTT_CLIENT_ABI_V1 1u
#define FLOWIE_MQTT_CLIENT_ABI_V2 2u
#define FLOWIE_MQTT_CLIENT_ABI_CURRENT FLOWIE_MQTT_CLIENT_ABI_V2
#define FLOWIE_MQTT_CLIENT_DEFAULT_PORT 1883
#define FLOWIE_MQTT_CLIENT_DEFAULT_TLS_PORT 8883
#define FLOWIE_MQTT_CLIENT_DEFAULT_TIMEOUT_MS 30000u
#define FLOWIE_MQTT_CLIENT_DEFAULT_MAX_PACKET_SIZE (1024u * 1024u)
#define FLOWIE_MQTT_CLIENT_DEFAULT_MAX_INBOUND_QOS2 64u
#define FLOWIE_MQTT_CLIENT_DEFAULT_COMMAND_QUEUE_CAPACITY 64u
#define FLOWIE_MQTT_CLIENT_DEFAULT_COMMAND_QUEUE_BYTES (4u * 1024u * 1024u)

typedef struct flowie_mqtt_client_s flowie_mqtt_client_t;

typedef enum flowie_mqtt_client_transport_e {
  FLOWIE_MQTT_CLIENT_TRANSPORT_TCP = 1,
  FLOWIE_MQTT_CLIENT_TRANSPORT_TLS,
  FLOWIE_MQTT_CLIENT_TRANSPORT_WS,
  FLOWIE_MQTT_CLIENT_TRANSPORT_WSS
} flowie_mqtt_client_transport_t;

/**
 * Called synchronously on the client's CoroNet context for an inbound PUBLISH.
 * Every view is borrowed and remains valid only for the callback duration.
 * Synchronous re-entry on the same client returns TURBO_EBUSY. Managed-mode
 * async commands may be enqueued from the callback.
 */
typedef int (*flowie_mqtt_client_publish_fn)(flowie_mqtt_client_t *client,
                                             const flowie_mqtt_publish_view_t *publish,
                                             void *user_data);

/**
 * Completes one accepted managed-mode command on the DLL-owned worker thread.
 * response is NULL when the operation has no MQTT control response. Otherwise
 * it and all nested spans remain valid only for the callback duration.
 * Async commands may be submitted from this callback; synchronous I/O and
 * client destruction may not be called from it.
 */
typedef void (*flowie_mqtt_client_completion_fn)(flowie_mqtt_client_t *client, int status,
                                                 const flowie_mqtt_control_packet_view_t *response,
                                                 void *user_data);

/**
 * Reports an unsolicited broker disconnect or receive failure in managed mode.
 * The callback runs after the transport is closed on the DLL-owned worker.
 */
typedef void (*flowie_mqtt_client_disconnect_fn)(flowie_mqtt_client_t *client, int status,
                                                 void *user_data);

typedef struct flowie_mqtt_client_config_s {
  size_t size;
  uint32_t abi_version;
  /** Borrowed context. The caller must keep it alive longer than the client. */
  coro_context_t *context;
  flowie_mqtt_client_transport_t transport;
  /** Copied by flowie_mqtt_client_create(). */
  const char *host;
  int port;
  /** Copied WS/WSS path; NULL selects "/mqtt". Ignored for TCP/TLS. */
  const char *path;
  uint64_t timeout_ms;
  size_t max_packet_size;
  size_t max_inbound_qos2;
  /** Optional. Receiving PUBLISH without a callback returns TURBO_ENOTSUP and closes the transport.
   */
  flowie_mqtt_client_publish_fn on_publish;
  void *user_data;
  /** V2: bounded managed-mode command count; zero selects the default. */
  size_t command_queue_capacity;
  /** V2: optional managed-mode notification for unsolicited connection loss. */
  flowie_mqtt_client_disconnect_fn on_disconnect;
  /** V2: maximum total bytes owned by queued async commands; zero selects the default. */
  size_t command_queue_max_bytes;
} flowie_mqtt_client_config_t;

#define FLOWIE_MQTT_CLIENT_CONFIG_INIT                                                             \
  {sizeof(flowie_mqtt_client_config_t),                                                            \
   FLOWIE_MQTT_CLIENT_ABI_CURRENT,                                                                 \
   NULL,                                                                                           \
   FLOWIE_MQTT_CLIENT_TRANSPORT_TCP,                                                               \
   NULL,                                                                                           \
   FLOWIE_MQTT_CLIENT_DEFAULT_PORT,                                                                \
   NULL,                                                                                           \
   FLOWIE_MQTT_CLIENT_DEFAULT_TIMEOUT_MS,                                                          \
   FLOWIE_MQTT_CLIENT_DEFAULT_MAX_PACKET_SIZE,                                                     \
   FLOWIE_MQTT_CLIENT_DEFAULT_MAX_INBOUND_QOS2,                                                    \
   NULL,                                                                                           \
   NULL,                                                                                           \
   FLOWIE_MQTT_CLIENT_DEFAULT_COMMAND_QUEUE_CAPACITY,                                              \
   NULL,                                                                                           \
   FLOWIE_MQTT_CLIENT_DEFAULT_COMMAND_QUEUE_BYTES}

/**
 * Create a client. V1 requires a caller-owned context and preserves the
 * synchronous coroutine API. With a V2 config, context == NULL creates managed
 * mode: the DLL owns a CoroNet context, worker thread, receive loop, and bounded
 * async command queue. On failure, out is set to NULL.
 */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_create(const flowie_mqtt_client_config_t *config,
                                                     flowie_mqtt_client_t **out);

/**
 * Destroy the client outside its callbacks. External-context mode requires no
 * operation in flight. Managed mode stops admission, interrupts current I/O,
 * completes accepted queued commands with TURBO_ESHUTDOWN, and joins its worker.
 */
FLOWIE_MQTT_CLIENT_API void flowie_mqtt_client_destroy(flowie_mqtt_client_t *client);

/**
 * Connect the configured transport, send CONNECT, and wait for CONNACK.
 * The packet version becomes the version for this connection. A valid rejected
 * CONNACK returns TURBO_OK with reason_code != 0 and leaves the client disconnected.
 * All I/O functions must run inside a coroutine on config.context.
 * connack spans are borrowed until the next client I/O call or client destruction.
 */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_connect(flowie_mqtt_client_t *client,
                                                      const flowie_mqtt_connect_packet_t *packet,
                                                      flowie_mqtt_control_packet_view_t *connack);

/**
 * Publish and complete the selected QoS exchange. The client owns packet-id
 * allocation, so packet.packet_id must be zero. ack may be NULL; for QoS 2 it
 * receives PUBREC on rejection and PUBCOMP after a successful exchange.
 * ack spans are borrowed until the next client I/O call or client destruction.
 */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_publish(flowie_mqtt_client_t *client,
                                                      const flowie_mqtt_publish_packet_t *packet,
                                                      flowie_mqtt_control_packet_view_t *ack);

/**
 * Subscribe and wait for the matching SUBACK. packet.packet_id must be zero.
 * A publish callback is not required to register a subscription.
 * suback spans are borrowed until the next client I/O call or client destruction.
 */
FLOWIE_MQTT_CLIENT_API int
flowie_mqtt_client_subscribe(flowie_mqtt_client_t *client,
                             const flowie_mqtt_subscribe_packet_t *packet,
                             flowie_mqtt_control_packet_view_t *suback);

/**
 * Unsubscribe and wait for the matching UNSUBACK. packet.packet_id must be zero.
 * unsuback spans are borrowed until the next client I/O call or client destruction.
 */
FLOWIE_MQTT_CLIENT_API int
flowie_mqtt_client_unsubscribe(flowie_mqtt_client_t *client,
                               const flowie_mqtt_unsubscribe_packet_t *packet,
                               flowie_mqtt_control_packet_view_t *unsuback);

/** Send PINGREQ and wait for PINGRESP while continuing to service inbound PUBLISH packets. */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_ping(flowie_mqtt_client_t *client);

/** Wait for and process one broker-initiated packet. received_type may be NULL. */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_poll(flowie_mqtt_client_t *client,
                                                   flowie_mqtt_packet_type_t *received_type);

/** Send DISCONNECT and close the transport. MQTT 3.1.1 requires zero reason/properties. */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_disconnect(flowie_mqtt_client_t *client,
                                                         uint8_t reason_code,
                                                         flowie_mqtt_span_t properties);

/** Read-only connection state; this does not perform I/O. */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_is_connected(const flowie_mqtt_client_t *client);

/** Return non-zero when the DLL owns the CoroNet context and worker thread. */
FLOWIE_MQTT_CLIENT_API int flowie_mqtt_client_is_managed(const flowie_mqtt_client_t *client);

/**
 * Managed-mode asynchronous operations. Returning TURBO_OK transfers a deep
 * copy of the complete packet description to the client and guarantees one
 * completion callback. Queue-full, shutdown, structurally invalid input, and
 * external-context mode fail immediately without invoking the callback.
 * Protocol-semantic or network errors are delivered through the callback.
 */
FLOWIE_MQTT_CLIENT_API int
flowie_mqtt_client_connect_async(flowie_mqtt_client_t *client,
                                 const flowie_mqtt_connect_packet_t *packet,
                                 flowie_mqtt_client_completion_fn completion, void *user_data);
FLOWIE_MQTT_CLIENT_API int
flowie_mqtt_client_publish_async(flowie_mqtt_client_t *client,
                                 const flowie_mqtt_publish_packet_t *packet,
                                 flowie_mqtt_client_completion_fn completion, void *user_data);
FLOWIE_MQTT_CLIENT_API int
flowie_mqtt_client_subscribe_async(flowie_mqtt_client_t *client,
                                   const flowie_mqtt_subscribe_packet_t *packet,
                                   flowie_mqtt_client_completion_fn completion, void *user_data);
FLOWIE_MQTT_CLIENT_API int
flowie_mqtt_client_unsubscribe_async(flowie_mqtt_client_t *client,
                                     const flowie_mqtt_unsubscribe_packet_t *packet,
                                     flowie_mqtt_client_completion_fn completion, void *user_data);
FLOWIE_MQTT_CLIENT_API int
flowie_mqtt_client_ping_async(flowie_mqtt_client_t *client,
                              flowie_mqtt_client_completion_fn completion, void *user_data);
/**
 * Complete after the transport is closed. EOF/connection-reset observed while
 * performing this no-response MQTT shutdown is treated as successful closure.
 */
FLOWIE_MQTT_CLIENT_API int
flowie_mqtt_client_disconnect_async(flowie_mqtt_client_t *client, uint8_t reason_code,
                                    flowie_mqtt_span_t properties,
                                    flowie_mqtt_client_completion_fn completion, void *user_data);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_MQTT_CLIENT_H */
