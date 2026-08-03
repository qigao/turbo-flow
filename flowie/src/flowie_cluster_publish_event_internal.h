#ifndef FLOWIE_CLUSTER_PUBLISH_EVENT_INTERNAL_H
#define FLOWIE_CLUSTER_PUBLISH_EVENT_INTERNAL_H

#include "flowie_cluster_peer_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_PUBLISH_EVENT_VERSION 2u
#define FLOWIE_CLUSTER_PUBLISH_EVENT_HEADER_SIZE 88u
#define FLOWIE_CLUSTER_PUBLISH_EVENT_V1_HEADER_SIZE 80u
#define FLOWIE_CLUSTER_PUBLISH_OUTBOX_EVENT_TYPE 5u

/** Borrowed, fully validated view of one durable broadcast ingress event. */
typedef struct flowie_cluster_publish_event_view_s {
  size_t size;
  uint32_t abi_version;
  turbo_flow_protocol_settlement_point_t requested_settlement;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t session_id;
  uint64_t session_generation;
  uint64_t accepted_at_epoch_seconds;
  const uint8_t *edge_boot_id;
  tstr_v edge_node_id;
  flowie_cluster_peer_mqtt_command_view_t publish;
} flowie_cluster_publish_event_view_t;

#define FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT                                                     \
  {sizeof(flowie_cluster_publish_event_view_t), FLOWIE_CLUSTER_PUBLISH_EVENT_VERSION,             \
   (turbo_flow_protocol_settlement_point_t)0, 0u, 0u, 0u, 0u, 0u, NULL, {NULL, 0u},              \
   FLOWIE_CLUSTER_PEER_MQTT_COMMAND_VIEW_INIT}

int flowie_cluster_publish_event_encode(
    flowie_mqtt_version_t mqtt_version, turbo_flow_protocol_settlement_point_t requested_settlement,
    uint64_t connection_id, uint64_t connection_generation, uint64_t session_id,
    uint64_t session_generation, uint64_t accepted_at_epoch_seconds, tstr_v edge_node_id,
    const uint8_t edge_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE], flowie_mqtt_span_t client_id,
    flowie_mqtt_span_t packet, size_t max_payload_size, tstr_t *out);

int flowie_cluster_publish_event_decode(const void *data, size_t data_size, size_t max_payload_size,
                                        flowie_cluster_publish_event_view_t *out);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_PUBLISH_EVENT_INTERNAL_H */
