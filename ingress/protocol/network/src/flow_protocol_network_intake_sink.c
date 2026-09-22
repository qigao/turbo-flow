#include "flow_protocol_network_intake_internal.h"

#include "flow_protocol_envelope_internal.h"
#include "turbo_flow_cnet.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

enum { INTAKE_RESOURCE_GENERATION = 1u };

static const char INTAKE_MEDIA_TYPE[] = "application/octet-stream";
static const char INTAKE_CONTENT_IDENTITY[] = "protocol.network.intake";
static const char INTAKE_SCHEMA_NAME[] = "ProtocolNetworkIntakeTransport";
static const char INTAKE_TYPE_NAME[] = "MessageOwnedBytes";

typedef struct intake_parser_slot_s {
  int in_use;
  uint32_t transport_slot;
  uint64_t generation;
  uint64_t last_message_id;
  uint64_t protocol_session_id;
  char device_id[TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX + 1u];
} intake_parser_slot_t;

typedef struct intake_pending_claim_s {
  turbo_flow_async_terminal_claim_t claim;
  size_t retained_bytes;
} intake_pending_claim_t;

struct flow_protocol_network_intake_sink_s {
  turbo_flow_t *flow;
  turbo_flow_protocol_t *protocol;
  turbo_flow_t *downstream_flow;
  turbo_flow_protocol_source_t *protocol_source;
  flow_protocol_network_intake_settings_t settings;
  turbo_flow_protocol_mapper_v1_t mapper;
  turbo_flow_protocol_mapper_contract_t mapper_contract;
  int mapper_bound;
  tstr adapter_name;
  tstr decoded_source_name;
  uint8_t *envelope_scratch;
  size_t max_envelope_bytes;
  uint8_t *mapped_scratch;
  size_t mapped_capacity;
  intake_parser_slot_t *parser_slots;
  intake_pending_claim_t *pending;
  size_t pending_head;
  size_t pending_count;
  size_t pending_bytes;
  uint64_t blocked_session_id;
  uint64_t blocked_generation;
  uint64_t frames_admitted;
  uint64_t accepted;
  uint64_t completed;
  uint64_t rejected;
  int blocked;
  int terminal_status;
  int registered;
  int detached;
  char admission_id[TURBO_FLOW_DURABLE_ADMISSION_ID_MAX + 1u];
};

static void intake_counter_add(uint64_t *counter, uint64_t amount) {
  if (!counter || amount == 0u || *counter == UINT64_MAX) return;
  *counter = amount > UINT64_MAX - *counter ? UINT64_MAX : *counter + amount;
}

static size_t intake_text_size(const char *text, size_t capacity) {
  size_t size = 0u;
  if (!text) return 0u;
  while (size < capacity && text[size] != '\0') ++size;
  return size;
}

static int intake_identity_resolve(
    flow_protocol_network_intake_sink_t *sink,
    const turbo_flow_protocol_message_output_t *message,
    turbo_flow_durable_identity_t *identity) {
  const turbo_flow_protocol_metadata_t *metadata;
  const char *protocol_name;
  XXH128_hash_t digest;
  size_t correlation_size;
  int count;
  if (!sink || !message || !identity ||
      identity->size != sizeof(*identity) ||
      identity->version != TURBO_FLOW_DURABLE_BUFFER_API_VERSION)
    return SALTS_EINVAL;
  metadata = &message->metadata;
  protocol_name = turbo_flow_protocol_kind_name(metadata->protocol);
  if (!protocol_name || metadata->device_id[0] == '\0' ||
      (!message->payload && message->payload_size != 0u))
    return SALTS_EPROTO;
  digest = XXH3_128bits(message->payload, message->payload_size);
  count = snprintf(sink->admission_id, sizeof(sink->admission_id),
                   "%s/%s/%" PRIu32 "/%" PRIu64 "/%016" PRIx64 "%016" PRIx64,
                   protocol_name, metadata->device_id, metadata->message_type, metadata->sequence,
                   digest.high64, digest.low64);
  if (count < 0 || (size_t)count >= sizeof(sink->admission_id)) return SALTS_EMSGSIZE;
  correlation_size = intake_text_size(metadata->correlation_id, sizeof(metadata->correlation_id));
  if (correlation_size == sizeof(metadata->correlation_id)) return SALTS_EPROTO;
  *identity = (turbo_flow_durable_identity_t)TURBO_FLOW_DURABLE_IDENTITY_INIT;
  identity->source_id =
      vstr_from_buf(sink->settings.source_id, strlen(sink->settings.source_id));
  identity->admission_id = vstr_from_buf(sink->admission_id, (size_t)count);
  if (correlation_size > 0u)
    identity->correlation = vstr_from_buf(metadata->correlation_id, correlation_size);
  identity->source_sequence = metadata->sequence;
  return SALTS_OK;
}

static int intake_mapper_content_exact(
    const turbo_flow_content_descriptor_t *expected,
    const turbo_flow_content_descriptor_t *actual) {
  if (!expected || !actual ||
      turbo_flow_content_descriptor_check(expected) != SALTS_OK ||
      turbo_flow_content_descriptor_check(actual) != SALTS_OK ||
      turbo_flow_content_descriptor_validate(expected, actual) != SALTS_OK)
    return 0;
  return expected->domain == actual->domain &&
         expected->profile == actual->profile &&
         expected->encoding == actual->encoding &&
         expected->flags == actual->flags &&
         expected->schema_version == actual->schema_version &&
         strcmp(expected->media_type, actual->media_type) == 0 &&
         strcmp(expected->schema_name, actual->schema_name) == 0 &&
         strcmp(expected->type_name, actual->type_name) == 0 &&
         strcmp(expected->identity, actual->identity) == 0;
}

static int intake_map_business(
    flow_protocol_network_intake_sink_t *sink,
    const turbo_flow_protocol_source_admit_request_t *admit,
    size_t *payload_size,
    turbo_flow_content_descriptor_t *content) {
  turbo_flow_protocol_mapper_request_t request = TURBO_FLOW_PROTOCOL_MAPPER_REQUEST_INIT;
  turbo_flow_protocol_mapper_output_t output = TURBO_FLOW_PROTOCOL_MAPPER_OUTPUT_INIT;
  uint8_t *owned_payload;
  size_t owned_capacity;
  int rc;
  if (payload_size) *payload_size = 0u;
  if (!sink || !sink->mapper_bound || !admit || !admit->message || !admit->semantic ||
      !payload_size || !content || !sink->mapped_scratch || sink->mapped_capacity == 0u)
    return SALTS_EINVAL;
  if (admit->semantic->size != sizeof(*admit->semantic) ||
      admit->semantic->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION ||
      (!admit->semantic->data && admit->semantic->data_size != 0u) ||
      admit->semantic->data_size > sink->mapper_contract.max_semantic_bytes ||
      admit->message->metadata.protocol != sink->mapper_contract.protocol ||
      admit->message->metadata.message_type != sink->mapper_contract.message_type ||
      admit->semantic->semantic_type != sink->mapper_contract.semantic_type ||
      strcmp(admit->semantic->media_type, sink->settings.mapper_semantic_media_type) != 0)
    return SALTS_EPROTO;

  request.protocol = sink->mapper_contract.protocol;
  request.profile = sink->mapper_contract.profile;
  request.metadata = admit->message->metadata;
  request.semantic_data = admit->semantic->data;
  request.semantic_size = admit->semantic->data_size;
  request.semantic_type = admit->semantic->semantic_type;
  request.semantic_media_type = admit->semantic->media_type;

  output.payload = sink->mapped_scratch;
  output.payload_capacity = sink->mapped_capacity;
  owned_payload = output.payload;
  owned_capacity = output.payload_capacity;
  rc = sink->mapper.map(sink->mapper.ctx, &request, &output);
  if (rc != SALTS_OK) {
    output.payload_size = 0u;
    return rc;
  }
  if (output.size != sizeof(output) ||
      output.abi_version != TURBO_FLOW_PROTOCOL_MAPPER_ABI_VERSION ||
      output.payload != owned_payload || output.payload_capacity != owned_capacity ||
      output.payload_size > output.payload_capacity ||
      output.payload_size > sink->mapper_contract.max_output_bytes ||
      !intake_mapper_content_exact(&sink->mapper_contract.content, &output.content))
    return SALTS_EPROTO;

  *payload_size = output.payload_size;
  *content = output.content;
  return SALTS_OK;
}

static int intake_decoded_admit(void *ctx,
                                const turbo_flow_protocol_source_admit_request_t *request) {
  flow_protocol_network_intake_sink_t *sink = (flow_protocol_network_intake_sink_t *)ctx;
  turbo_flow_durable_identity_t identity = TURBO_FLOW_DURABLE_IDENTITY_INIT;
  turbo_flow_content_descriptor_t content = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
  turbo_flow_msg_t message;
  mem_buffer_t *buffer = NULL;
  const uint8_t *payload_data = NULL;
  size_t encoded_size = 0u;
  int rc;
  if (!sink || !request || request->size != sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION ||
      request->delivery_id == 0u || request->session_id == 0u ||
      request->session_generation == 0u || !request->message)
    return SALTS_EINVAL;
  if ((sink->mapper_bound && !request->semantic) ||
      (!sink->mapper_bound && request->semantic))
    return SALTS_EPROTO;

  rc = intake_identity_resolve(sink, request->message, &identity);
  if (rc != SALTS_OK) return rc;

  if (sink->mapper_bound) {
    rc = intake_map_business(sink, request, &encoded_size, &content);
    if (rc != SALTS_OK) return rc;
    payload_data = sink->mapped_scratch;
  } else {
    rc = flow_protocol_envelope_encode(request->message, sink->settings.max_frame_size,
                                       sink->envelope_scratch, sink->max_envelope_bytes,
                                       &encoded_size);
    if (rc != SALTS_OK) return rc;
    rc = flow_protocol_envelope_content_descriptor(&content);
    if (rc != SALTS_OK) return rc;
    payload_data = sink->envelope_scratch;
  }

  buffer = mem_wrap_external((void *)payload_data, encoded_size, NULL, NULL);
  if (!buffer) return SALTS_ENOMEM;
  turbo_flow_msg_init(&message);
  message.id = request->delivery_id;
  message.ts_ns = 0u;
  message.type = request->message->metadata.message_type;
  message.buffer = buffer;
  message.payload = vstr_from_buf((const char *)payload_data, encoded_size);
  rc = turbo_flow_msg_copy_content_descriptor(&message, &content);
  if (rc == SALTS_OK) rc = turbo_flow_msg_set_durable_identity(&message, &identity);
  if (rc == SALTS_OK)
    rc = turbo_flow_publish(sink->downstream_flow, sink->decoded_source_name, &message);
  turbo_flow_msg_cleanup(&message);
  return rc;
}

static int intake_ipv4_device_id(const cnet_datagram_peer *peer, char *out, size_t capacity) {
  int count;
  if (!peer || !out || capacity == 0u || peer->port == 0u) return SALTS_EINVAL;
  count = snprintf(out, capacity, "udp4-%02x%02x%02x%02x-%u", (unsigned)peer->address[0],
                   (unsigned)peer->address[1], (unsigned)peer->address[2],
                   (unsigned)peer->address[3], (unsigned)peer->port);
  return count < 0 || (size_t)count >= capacity ? SALTS_EMSGSIZE : SALTS_OK;
}

static int intake_ipv6_device_id(const cnet_datagram_peer *peer, char *out, size_t capacity) {
  static const char hex[] = "0123456789abcdef";
  char address[33];
  int count;
  if (!peer || !out || capacity == 0u || peer->port == 0u) return SALTS_EINVAL;
  for (size_t i = 0u; i < 16u; ++i) {
    address[i * 2u] = hex[(peer->address[i] >> 4u) & 0x0fu];
    address[i * 2u + 1u] = hex[peer->address[i] & 0x0fu];
  }
  address[32] = '\0';
  count = snprintf(out, capacity, "udp6-%s-%u-%u", address, (unsigned)peer->port,
                   (unsigned)peer->scope_id);
  return count < 0 || (size_t)count >= capacity ? SALTS_EMSGSIZE : SALTS_OK;
}

static int intake_packet_device_id(const turbo_flow_cnet_packet_message_context_t *context,
                                   char *out, size_t capacity) {
  if (!context || context->info.protocol != CNET_PACKET_UDP) return SALTS_EPROTO;
  if (context->info.peer.family == CNET_DATAGRAM_ADDRESS_IPV4)
    return intake_ipv4_device_id(&context->info.peer, out, capacity);
  if (context->info.peer.family == CNET_DATAGRAM_ADDRESS_IPV6)
    return intake_ipv6_device_id(&context->info.peer, out, capacity);
  return SALTS_EPROTO;
}

static int intake_transport_identity(flow_protocol_network_intake_sink_t *sink,
                                     const turbo_flow_msg_t *message, uint32_t *slot_out,
                                     uint64_t *generation_out, char *device_id,
                                     size_t device_capacity) {
  if (!sink || !message || !slot_out || !generation_out || !device_id || device_capacity == 0u ||
      message->id == 0u)
    return SALTS_EINVAL;
  device_id[0] = '\0';
  if (sink->settings.transport_kind == FLOW_PROTOCOL_NETWORK_TRANSPORT_LISTENER_TCP) {
    const turbo_flow_cnet_listener_message_context_t *context =
        turbo_flow_cnet_listener_message_context(message);
    if (!context || context->connection.slot == 0u) return SALTS_EPROTO;
    *slot_out = context->connection.slot - 1u;
    *generation_out = context->connection.generation;
  } else if (sink->settings.transport_kind == FLOW_PROTOCOL_NETWORK_TRANSPORT_PACKET_UDP) {
    const turbo_flow_cnet_packet_message_context_t *context =
        turbo_flow_cnet_packet_message_context(message);
    int rc;
    if (!context || context->session.slot == 0u) return SALTS_EPROTO;
    *slot_out = context->session.slot - 1u;
    *generation_out = context->session.generation;
    rc = intake_packet_device_id(context, device_id, device_capacity);
    if (rc != SALTS_OK) return rc;
  } else {
    return SALTS_ENOTSUP;
  }
  if (*generation_out == 0u || (size_t)*slot_out >= sink->settings.max_sessions) return SALTS_EPROTO;
  return SALTS_OK;
}

static int intake_session_open(flow_protocol_network_intake_sink_t *sink, intake_parser_slot_t *slot,
                               uint32_t transport_slot, uint64_t generation,
                               const char *device_id) {
  turbo_flow_protocol_source_session_open_request_t request =
      TURBO_FLOW_PROTOCOL_SOURCE_SESSION_OPEN_REQUEST_INIT;
  int rc;
  if (!sink || !slot || generation == 0u) return SALTS_EINVAL;
  request.session_id = (uint64_t)transport_slot + 1u;
  request.generation = generation;
  request.device_id = device_id && device_id[0] != '\0' ? device_id : NULL;
  request.protocol_version = sink->settings.protocol_version;
  rc = turbo_flow_protocol_source_session_open(sink->protocol_source, &request);
  if (rc != SALTS_OK) return rc;
  memset(slot, 0, sizeof(*slot));
  slot->in_use = 1;
  slot->transport_slot = transport_slot;
  slot->generation = generation;
  slot->protocol_session_id = request.session_id;
  if (device_id && device_id[0] != '\0') {
    const size_t size = strlen(device_id);
    if (size >= sizeof(slot->device_id)) {
      (void)turbo_flow_protocol_source_session_close(sink->protocol_source, request.session_id,
                                                     generation, SALTS_EMSGSIZE);
      memset(slot, 0, sizeof(*slot));
      return SALTS_EMSGSIZE;
    }
    memcpy(slot->device_id, device_id, size + 1u);
  }
  return SALTS_OK;
}

static int intake_session_prepare(flow_protocol_network_intake_sink_t *sink,
                                  const turbo_flow_msg_t *message, uint64_t *session_id_out,
                                  uint64_t *generation_out) {
  char device_id[TURBO_FLOW_PROTOCOL_DEVICE_ID_MAX + 1u];
  intake_parser_slot_t *slot;
  uint32_t transport_slot = 0u;
  uint64_t generation = 0u;
  int rc = intake_transport_identity(sink, message, &transport_slot, &generation, device_id,
                                     sizeof(device_id));
  if (rc != SALTS_OK) return rc;
  slot = &sink->parser_slots[transport_slot];
  if (!slot->in_use) {
    rc = intake_session_open(sink, slot, transport_slot, generation, device_id);
    if (rc != SALTS_OK) return rc;
  } else if (generation < slot->generation) {
    return SALTS_EPROTO;
  } else if (generation > slot->generation) {
    if (message->id <= slot->last_message_id) return SALTS_EPROTO;
    rc = turbo_flow_protocol_source_session_close(sink->protocol_source, slot->protocol_session_id,
                                                  slot->generation, SALTS_ECANCELED);
    if (rc != SALTS_OK) return rc;
    rc = intake_session_open(sink, slot, transport_slot, generation, device_id);
    if (rc != SALTS_OK) return rc;
  } else {
    if (message->id <= slot->last_message_id) return SALTS_EPROTO;
    if (device_id[0] != '\0' && strcmp(slot->device_id, device_id) != 0) return SALTS_EPROTO;
  }
  slot->last_message_id = message->id;
  *session_id_out = slot->protocol_session_id;
  *generation_out = slot->generation;
  return SALTS_OK;
}

static size_t intake_pending_index(const flow_protocol_network_intake_sink_t *sink, size_t offset) {
  return (sink->pending_head + offset) % sink->settings.max_pending_claims;
}

static intake_pending_claim_t *intake_pending_front(flow_protocol_network_intake_sink_t *sink) {
  if (!sink || sink->pending_count == 0u) return NULL;
  return &sink->pending[sink->pending_head];
}

static int intake_claim_complete(flow_protocol_network_intake_sink_t *sink,
                                 intake_pending_claim_t *entry, int status) {
  int rc;
  if (!sink || !entry || !entry->claim._impl) return SALTS_EINVAL;
  rc = turbo_flow_async_terminal_complete(&entry->claim, status, NULL);
  if (rc == SALTS_OK) intake_counter_add(&sink->completed, 1u);
  return rc;
}

static void intake_pending_pop(flow_protocol_network_intake_sink_t *sink) {
  intake_pending_claim_t *entry;
  if (!sink || sink->pending_count == 0u) return;
  entry = &sink->pending[sink->pending_head];
  if (entry->retained_bytes <= sink->pending_bytes) sink->pending_bytes -= entry->retained_bytes;
  else sink->pending_bytes = 0u;
  *entry = (intake_pending_claim_t){TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT, 0u};
  sink->pending_head = (sink->pending_head + 1u) % sink->settings.max_pending_claims;
  --sink->pending_count;
}

static int intake_process_front(flow_protocol_network_intake_sink_t *sink) {
  intake_pending_claim_t *entry = intake_pending_front(sink);
  const turbo_flow_msg_t *message;
  turbo_flow_protocol_source_feed_result_t result = TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
  uint64_t session_id = 0u;
  uint64_t generation = 0u;
  int rc;
  int complete_rc;
  if (!entry) return SALTS_OK;
  if (sink->blocked) {
    rc = turbo_flow_protocol_source_session_feed(sink->protocol_source, sink->blocked_session_id,
                                                 sink->blocked_generation, NULL, 0u, &result);
  } else {
    message = turbo_flow_async_terminal_claim_message(&entry->claim);
    if (!message || (!message->payload.data && message->payload.len != 0u) ||
        message->payload.len == 0u)
      rc = SALTS_EPROTO;
    else {
      rc = intake_session_prepare(sink, message, &session_id, &generation);
      if (rc == SALTS_OK)
        rc = turbo_flow_protocol_source_session_feed(
            sink->protocol_source, session_id, generation,
            (const uint8_t *)message->payload.data, message->payload.len, &result);
    }
  }
  intake_counter_add(&sink->frames_admitted, result.frames_admitted);
  if (rc != SALTS_OK) {
    complete_rc = intake_claim_complete(sink, entry, rc);
    if (complete_rc != SALTS_OK) {
      sink->terminal_status = complete_rc;
      return complete_rc;
    }
    intake_pending_pop(sink);
    sink->blocked = 0;
    sink->blocked_session_id = 0u;
    sink->blocked_generation = 0u;
    return SALTS_OK;
  }
  if (result.backpressured) {
    if (!sink->blocked) {
      sink->blocked_session_id = session_id;
      sink->blocked_generation = generation;
    }
    sink->blocked = 1;
    return SALTS_OK;
  }
  sink->blocked = 0;
  sink->blocked_session_id = 0u;
  sink->blocked_generation = 0u;
  complete_rc = intake_claim_complete(sink, entry, SALTS_OK);
  if (complete_rc != SALTS_OK) {
    sink->terminal_status = complete_rc;
    return complete_rc;
  }
  intake_pending_pop(sink);
  return SALTS_OK;
}

static int intake_drive(flow_protocol_network_intake_sink_t *sink) {
  int rc;
  if (!sink) return SALTS_EINVAL;
  while (sink->pending_count > 0u) {
    const size_t before = sink->pending_count;
    rc = intake_process_front(sink);
    if (rc != SALTS_OK) return rc;
    if (sink->blocked || sink->pending_count == before) return SALTS_OK;
  }
  return SALTS_OK;
}

static int intake_pending_append(flow_protocol_network_intake_sink_t *sink,
                                 turbo_flow_async_terminal_claim_t *owned, size_t bytes) {
  intake_pending_claim_t *entry;
  size_t tail;
  int rc;
  if (!sink || !owned || !owned->_impl || bytes == 0u) return SALTS_EINVAL;
  if (sink->pending_count >= sink->settings.max_pending_claims) return SALTS_ENOSPC;
  if (sink->pending_bytes > sink->settings.max_pending_bytes ||
      bytes > sink->settings.max_pending_bytes - sink->pending_bytes)
    return SALTS_ENOSPC;
  tail = intake_pending_index(sink, sink->pending_count);
  entry = &sink->pending[tail];
  entry->claim = (turbo_flow_async_terminal_claim_t)TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  rc = turbo_flow_async_terminal_claim_move(&entry->claim, owned);
  if (rc != SALTS_OK) return rc;
  entry->retained_bytes = bytes;
  ++sink->pending_count;
  sink->pending_bytes += bytes;
  return SALTS_OK;
}

static int intake_async_submit(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                               const turbo_flow_msg_t *message,
                               turbo_flow_async_terminal_claim_t *claim) {
  flow_protocol_network_intake_sink_t *sink = (flow_protocol_network_intake_sink_t *)ctx;
  turbo_flow_async_terminal_claim_t owned = TURBO_FLOW_ASYNC_TERMINAL_CLAIM_INIT;
  int rc;
  (void)stage;
  if (!sink || flow != sink->flow || !message || !claim || !claim->_impl || sink->detached) {
    if (sink) intake_counter_add(&sink->rejected, 1u);
    return SALTS_EINVAL;
  }
  if (sink->terminal_status != SALTS_OK) {
    intake_counter_add(&sink->rejected, 1u);
    return sink->terminal_status;
  }
  if (!message->payload.data || message->payload.len == 0u) {
    intake_counter_add(&sink->rejected, 1u);
    return SALTS_EINVAL;
  }
  if (sink->pending_count >= sink->settings.max_pending_claims ||
      sink->pending_bytes > sink->settings.max_pending_bytes ||
      message->payload.len > sink->settings.max_pending_bytes - sink->pending_bytes) {
    intake_counter_add(&sink->rejected, 1u);
    return SALTS_ENOSPC;
  }
  rc = turbo_flow_async_terminal_claim_move(&owned, claim);
  if (rc != SALTS_OK) {
    intake_counter_add(&sink->rejected, 1u);
    return rc;
  }
  rc = intake_pending_append(sink, &owned, message->payload.len);
  if (rc != SALTS_OK) {
    (void)turbo_flow_async_terminal_claim_move(claim, &owned);
    intake_counter_add(&sink->rejected, 1u);
    return rc;
  }
  intake_counter_add(&sink->accepted, 1u);
  if (sink->blocked) return SALTS_OK;
  return intake_drive(sink);
}

static void intake_registry_shutdown(void *ctx) {
  flow_protocol_network_intake_sink_t *sink = (flow_protocol_network_intake_sink_t *)ctx;
  if (sink) sink->detached = 1;
}

static int intake_boundary_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  flow_protocol_network_intake_sink_t *sink = (flow_protocol_network_intake_sink_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int count;
  if (!sink || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  metadata.domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
  metadata.kind = TURBO_FLOW_RESOURCE_PROTOCOL_AGGREGATE;
  count = snprintf(metadata.uid, sizeof(metadata.uid), "protocol-intake:%s", sink->adapter_name);
  if (count < 0 || (size_t)count >= sizeof(metadata.uid)) return SALTS_ERANGE;
  if (strlen(sink->adapter_name) >= sizeof(metadata.owner_name)) return SALTS_ERANGE;
  memcpy(metadata.owner_name, sink->adapter_name, strlen(sink->adapter_name) + 1u);
  metadata.generation = INTAKE_RESOURCE_GENERATION;
  metadata.observed_generation = INTAKE_RESOURCE_GENERATION;
  *out = metadata;
  return SALTS_OK;
}

static int intake_boundary_descriptor(void *ctx, turbo_flow_managed_boundary_descriptor_t *out) {
  flow_protocol_network_intake_sink_t *sink = (flow_protocol_network_intake_sink_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_managed_boundary_descriptor_t descriptor = TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
  int rc;
  if (!sink || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  rc = intake_boundary_metadata(sink, &metadata);
  if (rc != SALTS_OK) return rc;
  descriptor.domain = metadata.domain;
  descriptor.kind = metadata.kind;
  memcpy(descriptor.uid, metadata.uid, strlen(metadata.uid) + 1u);
  memcpy(descriptor.owner_name, metadata.owner_name, strlen(metadata.owner_name) + 1u);
  descriptor.role_flags = TURBO_FLOW_MANAGED_BOUNDARY_SINK;
  descriptor.capability_flags = 0u;
  rc = turbo_flow_content_descriptor_init(&descriptor.input, TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                                          TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                                          TURBO_FLOW_DATA_ENCODING_OPAQUE, INTAKE_MEDIA_TYPE,
                                          INTAKE_CONTENT_IDENTITY);
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_content_descriptor_declare_schema(&descriptor.input, INTAKE_SCHEMA_NAME,
                                                    INTAKE_TYPE_NAME, 1u);
  if (rc != SALTS_OK) return rc;
  *out = descriptor;
  return SALTS_OK;
}

static int intake_boundary_snapshot(void *ctx, turbo_flow_managed_boundary_snapshot_t *out) {
  flow_protocol_network_intake_sink_t *sink = (flow_protocol_network_intake_sink_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_managed_boundary_snapshot_t snapshot = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
  int rc;
  if (!sink || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  rc = intake_boundary_metadata(sink, &metadata);
  if (rc != SALTS_OK) return rc;
  memcpy(snapshot.uid, metadata.uid, strlen(metadata.uid) + 1u);
  snapshot.generation = metadata.generation;
  snapshot.observed_generation = metadata.observed_generation;
  snapshot.state = sink->terminal_status != SALTS_OK
                       ? TURBO_FLOW_MANAGED_BOUNDARY_FAILED
                       : (sink->detached ? TURBO_FLOW_MANAGED_BOUNDARY_STOPPED
                                         : TURBO_FLOW_MANAGED_BOUNDARY_RUNNING);
  snapshot.queue_depth = (uint64_t)sink->pending_count;
  snapshot.queue_capacity = (uint64_t)sink->settings.max_pending_claims;
  snapshot.in_flight = sink->blocked ? 1u : 0u;
  snapshot.accepted = sink->accepted;
  snapshot.completed = sink->completed;
  snapshot.rejected = sink->rejected;
  snapshot.backpressured = sink->blocked || sink->pending_count >= sink->settings.max_pending_claims;
  snapshot.last_status = sink->terminal_status;
  *out = snapshot;
  return SALTS_OK;
}

static int intake_downstream_buffer_valid(turbo_flow_t *flow, const char *source_name) {
  const turbo_flow_stage_plan_t *source;
  const turbo_flow_stage_plan_t *buffer;
  uint32_t selected_to_stage = 0u;
  turbo_flow_edge_kind_t selected_kind = TURBO_FLOW_EDGE_UNCONDITIONAL;
  int source_index;
  size_t outgoing = 0u;
  if (!flow || !source_name || !source_name[0] ||
      turbo_flow_state(flow) != TURBO_FLOW_STATE_STARTED)
    return 0;
  source_index = turbo_flow_find_stage(flow, source_name);
  if (source_index < 0) return 0;
  source = turbo_flow_stage_at(flow, (size_t)source_index);
  if (!source || !source->is_source || source->adapter_name) return 0;
  for (size_t index = 0u; index < turbo_flow_edge_count(flow); ++index) {
    const turbo_flow_edge_plan_t *edge = turbo_flow_edge_at(flow, index);
    if (!edge || edge->from_stage != (uint32_t)source_index) continue;
    ++outgoing;
    selected_to_stage = edge->to_stage;
    selected_kind = edge->kind;
  }
  if (outgoing != 1u || selected_kind != TURBO_FLOW_EDGE_UNCONDITIONAL) return 0;
  buffer = turbo_flow_stage_at(flow, selected_to_stage);
  return buffer && buffer->is_buffer && buffer->resource_name && buffer->resource_name[0];
}

int flow_protocol_network_intake_sink_create(
    const flow_protocol_network_intake_sink_config_t *config,
    flow_protocol_network_intake_sink_t **out) {
  flow_protocol_network_intake_sink_t *sink;
  turbo_flow_protocol_source_config_t source_config = TURBO_FLOW_PROTOCOL_SOURCE_CONFIG_INIT;
  turbo_flow_protocol_source_ops_t source_ops = TURBO_FLOW_PROTOCOL_SOURCE_OPS_INIT;
  size_t source_buffer_bytes;
  int rc;
  if (out) *out = NULL;
  if (!config || !out || !config->flow || !config->adapter_name || !config->adapter_name[0] ||
      !config->protocol || !config->downstream_flow || !config->decoded_source_name ||
      !config->decoded_source_name[0] || !config->settings ||
      config->settings->max_sessions == 0u || config->settings->max_frame_size == 0u ||
      config->settings->max_pending_claims == 0u || config->settings->max_pending_bytes == 0u ||
      strcmp(config->adapter_name, config->settings->decoder_adapter_name) != 0 ||
      !intake_downstream_buffer_valid(config->downstream_flow, config->decoded_source_name) ||
      (config->settings->schema_version == 3u &&
       (!config->mapper || !config->mapper_contract)) ||
      (config->settings->schema_version == 2u &&
       (config->mapper || config->mapper_contract)))
    return SALTS_EINVAL;
  if (config->settings->max_sessions == SIZE_MAX ||
      config->settings->max_frame_size > SIZE_MAX / (config->settings->max_sessions + 1u) ||
      config->settings->max_pending_claims > SIZE_MAX / sizeof(intake_pending_claim_t) ||
      config->settings->max_sessions > SIZE_MAX / sizeof(intake_parser_slot_t) ||
      config->settings->max_frame_size > SIZE_MAX - TURBO_FLOW_PROTOCOL_ENVELOPE_OVERHEAD)
    return SALTS_ERANGE;
  source_buffer_bytes = (config->settings->max_sessions + 1u) * config->settings->max_frame_size;
  if (config->mapper_contract &&
      config->mapper_contract->max_semantic_bytes > SIZE_MAX - source_buffer_bytes)
    return SALTS_ERANGE;
  if (config->mapper_contract)
    source_buffer_bytes += config->mapper_contract->max_semantic_bytes;
  sink = (flow_protocol_network_intake_sink_t *)calloc(1u, sizeof(*sink));
  if (!sink) return SALTS_ENOMEM;
  sink->flow = config->flow;
  sink->protocol = config->protocol;
  sink->downstream_flow = config->downstream_flow;
  sink->settings = *config->settings;
  if (config->mapper && config->mapper_contract) {
    sink->mapper = *config->mapper;
    sink->mapper_contract = *config->mapper_contract;
    sink->mapper_bound = 1;
  }
  sink->max_envelope_bytes = sink->mapper_bound
                                 ? 0u
                                 : sink->settings.max_frame_size +
                                       TURBO_FLOW_PROTOCOL_ENVELOPE_OVERHEAD;
  sink->mapped_capacity =
      sink->mapper_bound ? sink->mapper_contract.max_output_bytes : 0u;
  sink->adapter_name = tstr_dup(config->adapter_name);
  sink->decoded_source_name = tstr_dup(config->decoded_source_name);
  sink->envelope_scratch =
      sink->mapper_bound ? NULL : (uint8_t *)malloc(sink->max_envelope_bytes);
  sink->mapped_scratch =
      sink->mapper_bound ? (uint8_t *)malloc(sink->mapped_capacity) : NULL;
  sink->parser_slots =
      (intake_parser_slot_t *)calloc(sink->settings.max_sessions, sizeof(*sink->parser_slots));
  sink->pending =
      (intake_pending_claim_t *)calloc(sink->settings.max_pending_claims, sizeof(*sink->pending));
  sink->terminal_status = SALTS_OK;
  if (!sink->adapter_name || !sink->decoded_source_name ||
      (!sink->mapper_bound && !sink->envelope_scratch) ||
      (sink->mapper_bound && !sink->mapped_scratch) ||
      !sink->parser_slots || !sink->pending) {
    tstr_free(sink->adapter_name);
    tstr_free(sink->decoded_source_name);
    free(sink->envelope_scratch);
    free(sink->mapped_scratch);
    free(sink->parser_slots);
    free(sink->pending);
    free(sink);
    return SALTS_ENOMEM;
  }
  source_config.max_sessions = sink->settings.max_sessions;
  source_config.max_frame_size = sink->settings.max_frame_size;
  source_config.max_buffered_bytes = source_buffer_bytes;
  source_config.decode_mode = sink->mapper_bound
                                  ? TURBO_FLOW_PROTOCOL_SOURCE_DECODE_SEMANTIC
                                  : TURBO_FLOW_PROTOCOL_SOURCE_DECODE_RAW;
  source_config.max_semantic_bytes =
      sink->mapper_bound ? sink->mapper_contract.max_semantic_bytes : 0u;
  source_ops.admit = intake_decoded_admit;
  rc = turbo_flow_protocol_source_create(sink->protocol, &source_config, &source_ops,
                                         sink, &sink->protocol_source);
  if (rc != SALTS_OK) goto fail;
  *out = sink;
  return SALTS_OK;
fail:
  tstr_free(sink->adapter_name);
  tstr_free(sink->decoded_source_name);
  free(sink->envelope_scratch);
  free(sink->mapped_scratch);
  free(sink->parser_slots);
  free(sink->pending);
  free(sink);
  return rc;
}

int flow_protocol_network_intake_sink_register(flow_protocol_network_intake_sink_t *sink) {
  turbo_flow_adapter_ops_t adapter_ops = {0};
  turbo_flow_async_terminal_adapter_ops_t async_ops = TURBO_FLOW_ASYNC_TERMINAL_ADAPTER_OPS_INIT;
  turbo_flow_adapter_schema_t schema = {0};
  turbo_flow_managed_boundary_provider_ops_t boundary =
      TURBO_FLOW_MANAGED_BOUNDARY_PROVIDER_OPS_INIT;
  turbo_flow_managed_async_terminal_registration_t registration =
      TURBO_FLOW_MANAGED_ASYNC_TERMINAL_REGISTRATION_INIT;
  int rc;
  if (!sink || sink->registered || sink->detached) return SALTS_EINVAL;
  adapter_ops.shutdown = intake_registry_shutdown;
  async_ops.submit = intake_async_submit;
  schema.kind = TURBO_FLOW_ADAPTER_KIND_CUSTOM;
  schema.roles = TURBO_FLOW_ADAPTER_SINK;
  schema.direction = TURBO_FLOW_ADAPTER_OUTPUT;
  boundary.resource.metadata = intake_boundary_metadata;
  boundary.descriptor = intake_boundary_descriptor;
  boundary.snapshot = intake_boundary_snapshot;
  registration.adapter_name = sink->adapter_name;
  registration.adapter_ops = &adapter_ops;
  registration.async_ops = &async_ops;
  registration.schema = &schema;
  registration.owner_name = sink->adapter_name;
  registration.boundary_ops = &boundary;
  registration.ctx = sink;
  rc = turbo_flow_register_managed_async_terminal_adapter(sink->flow, &registration);
  if (rc == SALTS_OK) sink->registered = 1;
  return rc;
}

int flow_protocol_network_intake_sink_retry(flow_protocol_network_intake_sink_t *sink) {
  if (!sink) return SALTS_EINVAL;
  if (sink->terminal_status != SALTS_OK) return sink->terminal_status;
  return intake_drive(sink);
}

void flow_protocol_network_intake_sink_cancel(flow_protocol_network_intake_sink_t *sink,
                                              int status) {
  if (!sink || status == SALTS_OK) return;
  if (sink->terminal_status == SALTS_OK) sink->terminal_status = status;
  if (sink->protocol_source)
    (void)turbo_flow_protocol_source_force_shutdown(sink->protocol_source, status);
  while (sink->pending_count > 0u) {
    intake_pending_claim_t *entry = intake_pending_front(sink);
    if (entry) (void)intake_claim_complete(sink, entry, status);
    intake_pending_pop(sink);
  }
  memset(sink->parser_slots, 0, sink->settings.max_sessions * sizeof(*sink->parser_slots));
  sink->blocked = 0;
  sink->blocked_session_id = 0u;
  sink->blocked_generation = 0u;
}

void flow_protocol_network_intake_sink_metrics(
    const flow_protocol_network_intake_sink_t *sink,
    flow_protocol_network_intake_sink_metrics_t *metrics) {
  if (!metrics) return;
  memset(metrics, 0, sizeof(*metrics));
  if (!sink) {
    metrics->terminal_status = SALTS_EINVAL;
    return;
  }
  for (size_t i = 0u; i < sink->settings.max_sessions; ++i)
    if (sink->parser_slots[i].in_use) ++metrics->active_sessions;
  metrics->pending_claims = sink->pending_count;
  metrics->pending_bytes = sink->pending_bytes;
  metrics->frames_admitted = sink->frames_admitted;
  metrics->backpressured = sink->blocked;
  metrics->terminal_status = sink->terminal_status;
}

void flow_protocol_network_intake_sink_destroy(flow_protocol_network_intake_sink_t *sink) {
  if (!sink) return;
  if (sink->registered && !sink->detached) return;
  if (sink->protocol_source) {
    if (sink->terminal_status == SALTS_OK)
      (void)turbo_flow_protocol_source_force_shutdown(sink->protocol_source, SALTS_ECANCELED);
    (void)turbo_flow_protocol_source_destroy(sink->protocol_source);
  }
  while (sink->pending_count > 0u) {
    intake_pending_claim_t *entry = intake_pending_front(sink);
    if (entry && entry->claim._impl)
      (void)turbo_flow_async_terminal_complete(&entry->claim, SALTS_ECANCELED, NULL);
    intake_pending_pop(sink);
  }
  tstr_free(sink->adapter_name);
  tstr_free(sink->decoded_source_name);
  free(sink->envelope_scratch);
  free(sink->mapped_scratch);
  free(sink->parser_slots);
  free(sink->pending);
  free(sink);
}
