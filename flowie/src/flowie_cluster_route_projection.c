#include "flowie_cluster_route_projection_internal.h"

#include "flowie_cluster_peer_wire_internal.h"

#include <stdlib.h>
#include <string.h>

typedef struct flowie_cluster_route_event_view_s {
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t session_generation;
  const uint8_t *edge_boot_id;
  tstr_v edge_node_id;
} flowie_cluster_route_event_view_t;

struct flowie_cluster_route_projector_s {
  flowie_cluster_session_bind_config_t session;
  flowie_cluster_route_store_t *route_store;
  flowie_cluster_route_fact_get_fn fact_get;
  void *fact_get_ctx;
  flowie_cluster_route_member_resolve_fn member_resolve;
  void *member_resolve_ctx;
};

static int flowie_cluster_route_projection_nonzero(const uint8_t *bytes, size_t size) {
  uint8_t combined = 0u;
  if (!bytes) return 0;
  for (size_t index = 0u; index < size; ++index) combined |= bytes[index];
  return combined != 0u;
}

static int flowie_cluster_route_member_identity_validate(
    const flowie_cluster_pgsql_member_t *member, tstr_v node_id,
    const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  if (!member || member->size != sizeof(*member) ||
      member->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 || !node_id.data || !boot_id ||
      member->node_id_size != node_id.len ||
      memcmp(member->node_id, node_id.data, node_id.len) != 0 ||
      memcmp(member->boot_id, boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0 ||
      member->state < FLOWIE_CLUSTER_NODE_STARTING ||
      member->state > FLOWIE_CLUSTER_NODE_EXPIRED)
    return TURBO_EPROTO;
  return TURBO_OK;
}

static int flowie_cluster_route_member_routing_validate(
    const flowie_cluster_pgsql_member_t *member) {
  if (!member ||
      (member->state != FLOWIE_CLUSTER_NODE_READY &&
       member->state != FLOWIE_CLUSTER_NODE_DRAINING) ||
      member->advertised_endpoint_size == 0u ||
      member->advertised_endpoint_size > FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX ||
      memchr(member->advertised_endpoint, '\0', member->advertised_endpoint_size) ||
      member->lease_deadline_epoch_ms == 0u)
    return TURBO_EPROTO;
  return TURBO_OK;
}

static int flowie_cluster_route_projection_event_decode(
    const flowie_cluster_route_projector_t *projector,
    const flowie_cluster_pgsql_outbox_event_t *event,
    flowie_cluster_route_event_view_t *out) {
  static const uint8_t bound_magic[4] = {'T', 'F', 'B', 'E'};
  static const uint8_t updated_magic[4] = {'T', 'F', 'U', 'E'};
  static const uint8_t lost_magic[4] = {'T', 'F', 'L', 'E'};
  static const uint8_t takeover_magic[4] = {'T', 'F', 'T', 'E'};
  const uint8_t *expected_magic;
  const uint8_t *bytes;
  size_t payload_size;
  size_t edge_node_size;
  if (!projector || !event || event->size < sizeof(*event) ||
      event->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 || event->event_index != 0u ||
      event->shard_id >= FLOWIE_CLUSTER_SHARD_COUNT_MAX || event->event_owner_epoch == 0u ||
      event->fact_revision == 0u || event->record_kind != FLOWIE_CLUSTER_KEY_SESSION ||
      !event->record_key || tstr_len(event->record_key) == 0u ||
      tstr_len(event->record_key) > FLOWIE_CLUSTER_KEY_MAX || !event->payload || !out)
    return TURBO_EINVAL;
  switch (event->event_type) {
  case FLOWIE_CLUSTER_SESSION_EVENT_BOUND:
    expected_magic = bound_magic;
    break;
  case FLOWIE_CLUSTER_SESSION_EVENT_UPDATED:
    expected_magic = updated_magic;
    break;
  case FLOWIE_CLUSTER_SESSION_EVENT_CONNECTION_LOST:
    expected_magic = lost_magic;
    break;
  case FLOWIE_CLUSTER_SESSION_EVENT_TAKEN_OVER:
    expected_magic = takeover_magic;
    break;
  default:
    return TURBO_EINVAL;
  }
  payload_size = tstr_len(event->payload);
  if (payload_size > projector->session.max_event_payload_size) return TURBO_EMSGSIZE;
  if (payload_size < FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE) return TURBO_EPROTO;
  bytes = (const uint8_t *)event->payload;
  edge_node_size = flowie_cluster_peer_wire_read_u16(bytes + 64u);
  if (memcmp(bytes, expected_magic, sizeof(bound_magic)) != 0 ||
      flowie_cluster_peer_wire_read_u16(bytes + 4u) != FLOWIE_CLUSTER_SESSION_BOUND_EVENT_VERSION ||
      flowie_cluster_peer_wire_read_u16(bytes + 6u) !=
          FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE ||
      flowie_cluster_peer_wire_read_u32(bytes + 8u) != payload_size || edge_node_size == 0u ||
      edge_node_size > FLOWIE_CLUSTER_NODE_ID_MAX ||
      payload_size != FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE + edge_node_size ||
      bytes[13] != 0u || bytes[14] != 0u || bytes[15] != 0u ||
      flowie_cluster_peer_wire_read_u16(bytes + 66u) != 0u)
    return TURBO_EPROTO;
  memset(out, 0, sizeof(*out));
  out->connection_id = flowie_cluster_peer_wire_read_u64(bytes + 16u);
  out->connection_generation = flowie_cluster_peer_wire_read_u64(bytes + 24u);
  out->session_generation = flowie_cluster_peer_wire_read_u64(bytes + 40u);
  out->edge_boot_id = bytes + 48u;
  out->edge_node_id = tstr_v_from_buf(
      (const char *)bytes + FLOWIE_CLUSTER_SESSION_BOUND_EVENT_HEADER_SIZE, edge_node_size);
  if (out->connection_id == 0u || out->connection_generation == 0u ||
      out->session_generation == 0u ||
      !flowie_cluster_route_projection_nonzero(out->edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) ||
      memchr(out->edge_node_id.data, '\0', out->edge_node_id.len))
    return TURBO_EPROTO;
  return TURBO_OK;
}

int flowie_cluster_route_projector_create(
    const flowie_cluster_route_projector_config_t *config,
    flowie_cluster_route_projector_t **out) {
  flowie_cluster_route_projector_t *projector;
  if (out) *out = NULL;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_ROUTE_PROJECTION_ABI_V1 || !config->session ||
      flowie_cluster_session_bind_config_validate(config->session) != TURBO_OK ||
      !config->route_store || !config->fact_get || !config->member_resolve || !out)
    return TURBO_EINVAL;
  projector = (flowie_cluster_route_projector_t *)calloc(1u, sizeof(*projector));
  if (!projector) return TURBO_ENOMEM;
  projector->session = *config->session;
  projector->route_store = config->route_store;
  projector->fact_get = config->fact_get;
  projector->fact_get_ctx = config->fact_get_ctx;
  projector->member_resolve = config->member_resolve;
  projector->member_resolve_ctx = config->member_resolve_ctx;
  *out = projector;
  return TURBO_OK;
}

void flowie_cluster_route_projector_destroy(flowie_cluster_route_projector_t *projector) {
  free(projector);
}

static int flowie_cluster_route_projector_active(
    flowie_cluster_route_projector_t *projector,
    const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_pgsql_outbox_event_t *event,
    const flowie_cluster_route_event_view_t *event_view) {
  flowie_cluster_pgsql_fact_record_t fact = FLOWIE_CLUSTER_PGSQL_FACT_RECORD_INIT;
  flowie_cluster_session_fact_route_view_t binding =
      FLOWIE_CLUSTER_SESSION_FACT_ROUTE_VIEW_INIT;
  flowie_cluster_pgsql_member_t member = FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
  flowie_cluster_route_projection_t projection = FLOWIE_CLUSTER_ROUTE_PROJECTION_INIT;
  flowie_cluster_route_project_result_t result = 0;
  int rc = projector->fact_get(
      projector->fact_get_ctx, current_owner, FLOWIE_CLUSTER_KEY_SESSION,
      (const uint8_t *)event->record_key, tstr_len(event->record_key), &fact);
  if (rc != TURBO_OK) return rc;
  if (fact.revision < event->fact_revision) {
    rc = TURBO_EPROTO;
    goto done;
  }
  rc = flowie_cluster_session_fact_route_decode(&projector->session, &fact, &binding);
  if (rc != TURBO_OK) goto done;
  if (fact.revision == event->fact_revision &&
      (event->event_type == FLOWIE_CLUSTER_SESSION_EVENT_BOUND ||
       event->event_type == FLOWIE_CLUSTER_SESSION_EVENT_UPDATED) &&
      (binding.connection_id != event_view->connection_id ||
       binding.connection_generation != event_view->connection_generation ||
       binding.session_generation != event_view->session_generation ||
       binding.edge_node_id.len != event_view->edge_node_id.len ||
       memcmp(binding.edge_node_id.data, event_view->edge_node_id.data,
              binding.edge_node_id.len) != 0 ||
       memcmp(binding.edge_boot_id, event_view->edge_boot_id,
              FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0)) {
    rc = TURBO_EPROTO;
    goto done;
  }
  rc = projector->member_resolve(projector->member_resolve_ctx, binding.edge_node_id,
                                 binding.edge_boot_id, &member);
  if (rc != TURBO_OK) goto done;
  rc = flowie_cluster_route_member_identity_validate(&member, binding.edge_node_id,
                                                     binding.edge_boot_id);
  if (rc != TURBO_OK) goto done;
  rc = flowie_cluster_route_member_routing_validate(&member);
  if (rc != TURBO_OK) goto done;
  projection.client_id = binding.client_id;
  projection.edge_node_id = binding.edge_node_id;
  memcpy(projection.edge_boot_id, binding.edge_boot_id, sizeof(projection.edge_boot_id));
  projection.advertised_endpoint =
      tstr_v_from_buf(member.advertised_endpoint, member.advertised_endpoint_size);
  projection.session_shard = fact.shard_id;
  projection.owner_epoch = fact.owner_epoch;
  projection.fact_revision = fact.revision;
  projection.connection_id = binding.connection_id;
  projection.connection_generation = binding.connection_generation;
  projection.session_generation = binding.session_generation;
  projection.lease_deadline_epoch_ms = member.lease_deadline_epoch_ms;
  projection.active = 1u;
  rc = flowie_cluster_route_store_project(projector->route_store, &projection, &result);

done:
  flowie_cluster_pgsql_fact_record_cleanup(&fact);
  return rc;
}

static int flowie_cluster_route_projector_tombstone(
    flowie_cluster_route_projector_t *projector,
    const flowie_cluster_pgsql_outbox_event_t *event,
    const flowie_cluster_route_event_view_t *event_view) {
  flowie_cluster_route_projection_t projection = FLOWIE_CLUSTER_ROUTE_PROJECTION_INIT;
  flowie_cluster_route_project_result_t result = 0;
  projection.client_id =
      (flowie_mqtt_span_t){(const uint8_t *)event->record_key, tstr_len(event->record_key)};
  projection.edge_node_id = event_view->edge_node_id;
  memcpy(projection.edge_boot_id, event_view->edge_boot_id, sizeof(projection.edge_boot_id));
  projection.session_shard = event->shard_id;
  projection.owner_epoch = event->event_owner_epoch;
  projection.fact_revision = event->fact_revision;
  projection.connection_id = event_view->connection_id;
  projection.connection_generation = event_view->connection_generation;
  projection.session_generation = event_view->session_generation;
  projection.active = 0u;
  return flowie_cluster_route_store_project(projector->route_store, &projection, &result);
}

int flowie_cluster_route_projector_project(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_pgsql_outbox_event_t *event) {
  flowie_cluster_route_projector_t *projector = (flowie_cluster_route_projector_t *)ctx;
  flowie_cluster_route_event_view_t decoded;
  int rc;
  if (!projector || !current_owner ||
      !event ||
      current_owner->shard_id != event->shard_id)
    return TURBO_EINVAL;
  rc = flowie_cluster_route_projection_event_decode(projector, event, &decoded);
  if (rc != TURBO_OK) return rc;
  return event->event_type == FLOWIE_CLUSTER_SESSION_EVENT_CONNECTION_LOST
             ? flowie_cluster_route_projector_tombstone(projector, event, &decoded)
             : flowie_cluster_route_projector_active(projector, current_owner, event, &decoded);
}

int flowie_cluster_route_reconcile(flowie_cluster_route_store_t *route_store,
                                   flowie_cluster_route_member_resolve_fn member_resolve,
                                   void *member_resolve_ctx, size_t *out_refreshed) {
  flowie_cluster_route_snapshot_t *snapshot = NULL;
  size_t refreshed = 0u;
  int rc;
  if (out_refreshed) *out_refreshed = 0u;
  if (!route_store || !member_resolve || !out_refreshed) return TURBO_EINVAL;
  rc = flowie_cluster_route_store_snapshot_create(route_store, &snapshot);
  for (size_t index = 0u; rc == TURBO_OK &&
                          index < flowie_cluster_route_snapshot_count(snapshot);
       ++index) {
    flowie_cluster_route_projection_t projection = FLOWIE_CLUSTER_ROUTE_PROJECTION_INIT;
    flowie_cluster_pgsql_member_t member = FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
    flowie_cluster_route_project_result_t result = 0;
    rc = flowie_cluster_route_snapshot_at(snapshot, index, &projection);
    if (rc != TURBO_OK || !projection.active) continue;
    rc = member_resolve(member_resolve_ctx, projection.edge_node_id,
                        projection.edge_boot_id, &member);
    if (rc == TURBO_ENOENT) {
      rc = TURBO_OK;
      continue;
    }
    if (rc != TURBO_OK) break;
    rc = flowie_cluster_route_member_identity_validate(
        &member, projection.edge_node_id, projection.edge_boot_id);
    if (rc != TURBO_OK) break;
    if (member.state != FLOWIE_CLUSTER_NODE_READY &&
        member.state != FLOWIE_CLUSTER_NODE_DRAINING)
      continue;
    rc = flowie_cluster_route_member_routing_validate(&member);
    if (rc != TURBO_OK) break;
    if (member.lease_deadline_epoch_ms <= projection.lease_deadline_epoch_ms) continue;
    projection.advertised_endpoint =
        tstr_v_from_buf(member.advertised_endpoint, member.advertised_endpoint_size);
    projection.lease_deadline_epoch_ms = member.lease_deadline_epoch_ms;
    rc = flowie_cluster_route_store_project(route_store, &projection, &result);
    if (rc == TURBO_OK && result == FLOWIE_CLUSTER_ROUTE_PROJECT_APPLIED) ++refreshed;
  }
  flowie_cluster_route_snapshot_destroy(snapshot);
  if (rc == TURBO_OK) *out_refreshed = refreshed;
  return rc;
}

int flowie_cluster_route_projector_reconcile(flowie_cluster_route_projector_t *projector,
                                             size_t *out_refreshed) {
  if (!projector) {
    if (out_refreshed) *out_refreshed = 0u;
    return TURBO_EINVAL;
  }
  return flowie_cluster_route_reconcile(projector->route_store, projector->member_resolve,
                                        projector->member_resolve_ctx, out_refreshed);
}

int flowie_cluster_route_projector_pgsql_fact_get(
    void *ctx, const flowie_cluster_owner_token_t *current_owner,
    flowie_cluster_key_kind_t key_kind, const uint8_t *key, size_t key_size,
    flowie_cluster_pgsql_fact_record_t *out) {
  return flowie_cluster_pgsql_fact_get((flowie_cluster_pgsql_fact_store_t *)ctx, current_owner,
                                       key_kind, key, key_size, out);
}

int flowie_cluster_route_projector_pgsql_member_resolve(
    void *ctx, tstr_v node_id, const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
    flowie_cluster_pgsql_member_t *out) {
  return flowie_cluster_pgsql_member_resolve((flowie_cluster_pgsql_coordinator_t *)ctx, node_id,
                                             boot_id, out);
}
