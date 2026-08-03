#ifndef FLOWIE_PROXY_PROTOCOL_INTERNAL_H
#define FLOWIE_PROXY_PROTOCOL_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#include "CoroNet.h"
#include "flowie.h"
#include "turbo_str.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_PROXY_PROTOCOL_V2_ABI_V1 1u
#define FLOWIE_PROXY_PROTOCOL_V2_FIXED_SIZE 16u
#define FLOWIE_PROXY_PROTOCOL_V2_MAX_WIRE_SIZE (FLOWIE_PROXY_PROTOCOL_V2_FIXED_SIZE + UINT16_MAX)
#define FLOWIE_PROXY_PROTOCOL_V1_MAX_WIRE_SIZE 107u
#define FLOWIE_PROXY_PROTOCOL_ADDRESS_TEXT_SIZE 46u
#define FLOWIE_PROXY_PROTOCOL_INCOMPLETE 1

typedef struct flowie_proxy_protocol_policy_s flowie_proxy_protocol_policy_t;

/** Handler-lifetime addresses established by trusted PROXY admission. */
typedef struct flowie_proxy_protocol_connection_context_s {
  char remote_address[CORO_SOCKET_ADDRESS_TEXT_CAPACITY];
  char transport_peer_address[CORO_SOCKET_ADDRESS_TEXT_CAPACITY];
  /** Owned exact TLV block; values remain opaque advisory metadata. */
  tstr_t tlvs;
} flowie_proxy_protocol_connection_context_t;

typedef enum flowie_proxy_protocol_command_e {
  FLOWIE_PROXY_PROTOCOL_COMMAND_LOCAL = 0,
  FLOWIE_PROXY_PROTOCOL_COMMAND_PROXY = 1
} flowie_proxy_protocol_command_t;

typedef enum flowie_proxy_protocol_address_family_e {
  FLOWIE_PROXY_PROTOCOL_ADDRESS_UNSPECIFIED = 0,
  FLOWIE_PROXY_PROTOCOL_ADDRESS_IPV4,
  FLOWIE_PROXY_PROTOCOL_ADDRESS_IPV6
} flowie_proxy_protocol_address_family_t;

/** Owned canonical result of one complete text PROXY v1 header. */
typedef struct flowie_proxy_protocol_v1_view_s {
  size_t size;
  uint32_t abi_version;
  flowie_proxy_protocol_command_t command;
  flowie_proxy_protocol_address_family_t address_family;
  char source_address[FLOWIE_PROXY_PROTOCOL_ADDRESS_TEXT_SIZE];
  char destination_address[FLOWIE_PROXY_PROTOCOL_ADDRESS_TEXT_SIZE];
  uint16_t source_port;
  uint16_t destination_port;
  size_t header_size;
} flowie_proxy_protocol_v1_view_t;

#define FLOWIE_PROXY_PROTOCOL_V1_VIEW_INIT                                                        \
  {sizeof(flowie_proxy_protocol_v1_view_t), FLOWIE_PROXY_PROTOCOL_V2_ABI_V1,                     \
   FLOWIE_PROXY_PROTOCOL_COMMAND_LOCAL, FLOWIE_PROXY_PROTOCOL_ADDRESS_UNSPECIFIED, {0}, {0},     \
   0u, 0u, 0u}

/** Strict, incremental parser for the bounded Nginx-compatible text format. */
int flowie_proxy_protocol_v1_parse(const void *data, size_t data_size, size_t max_header_size,
                                   flowie_proxy_protocol_v1_view_t *out, size_t *consumed);

/**
 * Borrowed view into one complete PROXY v2 header. Address and TLV views remain
 * valid only while the input bytes remain unchanged. LOCAL never exposes an
 * advertised address because the direct socket peer stays authoritative.
 */
typedef struct flowie_proxy_protocol_v2_view_s {
  size_t size;
  uint32_t abi_version;
  flowie_proxy_protocol_command_t command;
  flowie_proxy_protocol_address_family_t address_family;
  const uint8_t *source_address;
  const uint8_t *destination_address;
  size_t address_size;
  uint16_t source_port;
  uint16_t destination_port;
  const uint8_t *tlvs;
  size_t tlvs_size;
  size_t header_size;
} flowie_proxy_protocol_v2_view_t;

#define FLOWIE_PROXY_PROTOCOL_V2_VIEW_INIT                                                        \
  {sizeof(flowie_proxy_protocol_v2_view_t), FLOWIE_PROXY_PROTOCOL_V2_ABI_V1,                     \
   FLOWIE_PROXY_PROTOCOL_COMMAND_LOCAL, FLOWIE_PROXY_PROTOCOL_ADDRESS_UNSPECIFIED, NULL, NULL,    \
   0u, 0u, 0u, NULL, 0u, 0u}

/** Borrowed view of one validated PROXY v2 TLV value. */
typedef struct flowie_proxy_protocol_v2_tlv_s {
  uint8_t type;
  const uint8_t *value;
  size_t value_size;
} flowie_proxy_protocol_v2_tlv_t;

#define FLOWIE_PROXY_PROTOCOL_V2_TLV_INIT {0u, NULL, 0u}

/**
 * Caller-owned, allocation-free cursor over a parsed PROXY v2 view. The
 * cursor and returned TLV values borrow the original header bytes.
 */
typedef struct flowie_proxy_protocol_v2_tlv_cursor_s {
  const uint8_t *data;
  size_t data_size;
  size_t offset;
} flowie_proxy_protocol_v2_tlv_cursor_t;

#define FLOWIE_PROXY_PROTOCOL_V2_TLV_CURSOR_INIT {NULL, 0u, 0u}

/**
 * Incrementally parse exactly one mandatory PROXY v2 header. The caller must
 * enable this parser only after authenticating the direct peer as trusted.
 *
 * Returns TURBO_OK with *consumed set to the exact header size,
 * FLOWIE_PROXY_PROTOCOL_INCOMPLETE for a valid prefix that needs more bytes,
 * TURBO_EMSGSIZE when the advertised header exceeds max_header_size, or a
 * protocol error for any invalid/missing v2 header. Bytes after *consumed are
 * application/TLS bytes and are never inspected.
 */
int flowie_proxy_protocol_v2_parse(const void *data, size_t data_size, size_t max_header_size,
                                   flowie_proxy_protocol_v2_view_t *out, size_t *consumed);

/** Initialize a cursor from a successfully parsed PROXY command view. */
int flowie_proxy_protocol_v2_tlv_cursor_init(
    const flowie_proxy_protocol_v2_view_t *view,
    flowie_proxy_protocol_v2_tlv_cursor_t *cursor);

/** Return the next TLV, or TURBO_ENOENT after the last TLV. */
int flowie_proxy_protocol_v2_tlv_next(flowie_proxy_protocol_v2_tlv_cursor_t *cursor,
                                      flowie_proxy_protocol_v2_tlv_t *out);

/** Parse and copy one endpoint's trusted proxy policy. */
int flowie_proxy_protocol_policy_create(
    const flowie_endpoint_proxy_binding_t *binding,
    flowie_proxy_protocol_policy_t **out);

void flowie_proxy_protocol_policy_destroy(flowie_proxy_protocol_policy_t *policy);

/** Export the immutable policy as a CoroNet pre-TLS admission configuration. */
int flowie_proxy_protocol_policy_coronet_config(
    flowie_proxy_protocol_policy_t *policy,
    coro_server_pre_tls_admission_config_t *out);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_PROXY_PROTOCOL_INTERNAL_H */
