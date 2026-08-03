#ifndef FLOWIE_CLUSTER_DELIVERY_ACTION_INTERNAL_H
#define FLOWIE_CLUSTER_DELIVERY_ACTION_INTERNAL_H

#include "flowie_cluster_publish_event_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_DELIVERY_ACTION_VERSION 1u
#define FLOWIE_CLUSTER_DELIVERY_ACTION_HEADER_SIZE 72u
#define FLOWIE_CLUSTER_DELIVERY_ACTION_OUTBOX_EVENT_TYPE 6u

/** Borrowed destination identity and decoded sequenced TFEA socket action. */
typedef struct flowie_cluster_delivery_action_view_s {
  size_t size;
  uint32_t abi_version;
  tstr_v edge_node_id;
  const uint8_t *edge_boot_id;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t session_id;
  uint64_t session_generation;
  tstr_v encoded_action;
  flowie_cluster_peer_edge_action_t edge_action;
} flowie_cluster_delivery_action_view_t;

#define FLOWIE_CLUSTER_DELIVERY_ACTION_VIEW_INIT                                                  \
  {sizeof(flowie_cluster_delivery_action_view_t), FLOWIE_CLUSTER_DELIVERY_ACTION_VERSION,        \
   {NULL, 0u}, NULL, 0u, 0u, 0u, 0u, {NULL, 0u}, FLOWIE_CLUSTER_PEER_EDGE_ACTION_INIT}

int flowie_cluster_delivery_action_encode(
    tstr_v edge_node_id, const uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
    uint64_t connection_id, uint64_t connection_generation, uint64_t session_id,
    uint64_t session_generation, uint64_t action_sequence, flowie_mqtt_version_t mqtt_version,
    flowie_mqtt_span_t packet, size_t max_payload_size, tstr_t *out);

int flowie_cluster_delivery_action_decode(const void *data, size_t data_size,
                                          size_t max_payload_size,
                                          flowie_cluster_delivery_action_view_t *out);

/**
 * Build one target-version PUBLISH without mutating session state. An expired
 * message returns OK with *expired=1 and no packet. expiry_at is absolute and
 * remains meaningful for MQTT 3 targets even though their wire has no property.
 */
int flowie_cluster_delivery_packet_encode(
    const flowie_cluster_publish_event_view_t *event, flowie_mqtt_version_t target_version,
    uint8_t maximum_qos, uint8_t retain_as_published, uint16_t packet_id,
    const uint32_t *subscription_identifiers, size_t subscription_identifier_count,
    uint64_t now_epoch_seconds, size_t max_packet_size, tstr_t *out,
    uint64_t *expiry_at_epoch_seconds, int *expired);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_DELIVERY_ACTION_INTERNAL_H */
