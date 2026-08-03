#include "flowie_cluster_route_store_internal.h"

#include "flowie_cluster_peer_wire_internal.h"
#include "turbo_error.h"

#include <stdlib.h>
#include <string.h>

static const uint8_t FLOWIE_CLUSTER_ROUTE_RECORD_MAGIC[4] = {'T', 'F', 'C', 'R'};

enum {
  FLOWIE_CLUSTER_ROUTE_RECORD_FLAG_ACTIVE = 1u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_VERSION = 4u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_HEADER_SIZE = 6u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_TOTAL_SIZE = 8u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_FLAGS = 12u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_SHARD = 16u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_RESERVED_1 = 20u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_OWNER_EPOCH = 24u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_FACT_REVISION = 32u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_CONNECTION_ID = 40u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_CONNECTION_GENERATION = 48u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_SESSION_GENERATION = 56u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_LEASE_DEADLINE = 64u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_NODE_SIZE = 72u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_ENDPOINT_SIZE = 74u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_RESERVED_2 = 76u,
  FLOWIE_CLUSTER_ROUTE_OFFSET_BOOT_ID = 80u
};

struct flowie_cluster_route_store_s {
  turbo_flow_state_store_t *state;
  size_t max_client_id_size;
  size_t max_endpoint_size;
  size_t max_cas_attempts;
};

typedef struct flowie_cluster_route_snapshot_entry_s {
  size_t key_offset;
  size_t key_size;
  size_t value_offset;
  size_t value_size;
} flowie_cluster_route_snapshot_entry_t;

struct flowie_cluster_route_snapshot_s {
  const flowie_cluster_route_store_t *store;
  flowie_cluster_route_snapshot_entry_t *entries;
  uint8_t *bytes;
  size_t entry_count;
  size_t entry_capacity;
  size_t bytes_used;
  size_t bytes_capacity;
};

typedef struct flowie_cluster_route_decoded_s {
  const uint8_t *bytes;
  size_t size;
  tstr_v edge_node_id;
  tstr_v advertised_endpoint;
  const uint8_t *edge_boot_id;
  uint32_t session_shard;
  uint64_t owner_epoch;
  uint64_t fact_revision;
  uint64_t connection_id;
  uint64_t connection_generation;
  uint64_t session_generation;
  uint64_t lease_deadline_epoch_ms;
  int active;
} flowie_cluster_route_decoded_t;

static int flowie_cluster_route_decode(const flowie_cluster_route_store_t *store,
                                       const void *data, size_t data_size,
                                       flowie_cluster_route_decoded_t *out);

static int flowie_cluster_route_nonzero(const uint8_t *bytes, size_t size) {
  uint8_t combined = 0u;
  size_t index;
  if (!bytes) return 0;
  for (index = 0u; index < size; ++index) combined |= bytes[index];
  return combined != 0u;
}

int flowie_cluster_route_store_config_validate(
    const flowie_cluster_route_store_config_t *config) {
  return config && config->size == sizeof(*config) &&
                 config->abi_version == FLOWIE_CLUSTER_ROUTE_STORE_ABI_V1 && config->state &&
                 config->max_client_id_size != 0u &&
                 config->max_client_id_size <= FLOWIE_CLUSTER_KEY_MAX &&
                 config->max_endpoint_size != 0u &&
                 config->max_endpoint_size <= FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX &&
                 config->max_cas_attempts != 0u &&
                 config->max_cas_attempts <= FLOWIE_CLUSTER_ROUTE_CAS_ATTEMPTS_MAX
             ? TURBO_OK
             : TURBO_EINVAL;
}

int flowie_cluster_route_store_create(const flowie_cluster_route_store_config_t *config,
                                      flowie_cluster_route_store_t **out) {
  flowie_cluster_route_store_t *store;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_route_store_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  store = (flowie_cluster_route_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->state = config->state;
  store->max_client_id_size = config->max_client_id_size;
  store->max_endpoint_size = config->max_endpoint_size;
  store->max_cas_attempts = config->max_cas_attempts;
  *out = store;
  return TURBO_OK;
}

void flowie_cluster_route_store_destroy(flowie_cluster_route_store_t *store) { free(store); }

static int flowie_cluster_route_snapshot_visit(void *ctx, turbo_flow_store_bytes_t key,
                                               turbo_flow_store_bytes_t value,
                                               uint64_t revision) {
  flowie_cluster_route_snapshot_t *snapshot = (flowie_cluster_route_snapshot_t *)ctx;
  flowie_cluster_route_snapshot_entry_t *entry;
  flowie_cluster_route_decoded_t decoded;
  size_t required;
  (void)revision;
  if (!snapshot || !key.data || key.size == 0u || !value.data || value.size == 0u ||
      snapshot->entry_count >= snapshot->entry_capacity ||
      key.size > snapshot->bytes_capacity - snapshot->bytes_used)
    return TURBO_ENOSPC;
  required = snapshot->bytes_used + key.size;
  if (value.size > snapshot->bytes_capacity - required) return TURBO_ENOSPC;
  if (flowie_cluster_route_decode(snapshot->store, value.data, value.size, &decoded) != TURBO_OK)
    return TURBO_EPROTO;
  entry = &snapshot->entries[snapshot->entry_count];
  entry->key_offset = snapshot->bytes_used;
  entry->key_size = key.size;
  memcpy(snapshot->bytes + snapshot->bytes_used, key.data, key.size);
  snapshot->bytes_used = required;
  entry->value_offset = snapshot->bytes_used;
  entry->value_size = value.size;
  memcpy(snapshot->bytes + snapshot->bytes_used, value.data, value.size);
  snapshot->bytes_used += value.size;
  ++snapshot->entry_count;
  return TURBO_OK;
}

void flowie_cluster_route_snapshot_destroy(flowie_cluster_route_snapshot_t *snapshot) {
  if (!snapshot) return;
  free(snapshot->bytes);
  free(snapshot->entries);
  free(snapshot);
}

int flowie_cluster_route_store_snapshot_create(flowie_cluster_route_store_t *store,
                                               flowie_cluster_route_snapshot_t **out) {
  size_t attempt;
  int rc = TURBO_EBUSY;
  if (out) *out = NULL;
  if (!store || !out) return TURBO_EINVAL;
  for (attempt = 0u; attempt < store->max_cas_attempts; ++attempt) {
    turbo_flow_store_stats_t stats = TURBO_FLOW_STORE_STATS_INIT;
    flowie_cluster_route_snapshot_t *snapshot;
    rc = turbo_flow_state_store_stats(store->state, &stats);
    if (rc != TURBO_OK) return rc;
    snapshot = (flowie_cluster_route_snapshot_t *)calloc(1u, sizeof(*snapshot));
    if (!snapshot) return TURBO_ENOMEM;
    snapshot->store = store;
    snapshot->entry_capacity = stats.records;
    snapshot->bytes_capacity = stats.bytes;
    if (stats.records != 0u) {
      if (stats.records > SIZE_MAX / sizeof(*snapshot->entries)) {
        flowie_cluster_route_snapshot_destroy(snapshot);
        return TURBO_ERANGE;
      }
      snapshot->entries = (flowie_cluster_route_snapshot_entry_t *)calloc(
          stats.records, sizeof(*snapshot->entries));
      if (!snapshot->entries) {
        flowie_cluster_route_snapshot_destroy(snapshot);
        return TURBO_ENOMEM;
      }
    }
    if (stats.bytes != 0u) {
      snapshot->bytes = (uint8_t *)malloc(stats.bytes);
      if (!snapshot->bytes) {
        flowie_cluster_route_snapshot_destroy(snapshot);
        return TURBO_ENOMEM;
      }
    }
    rc = turbo_flow_state_store_visit(store->state, flowie_cluster_route_snapshot_visit, snapshot);
    if (rc == TURBO_OK) {
      *out = snapshot;
      return TURBO_OK;
    }
    flowie_cluster_route_snapshot_destroy(snapshot);
    if (rc != TURBO_ENOSPC) return rc;
  }
  return rc;
}

size_t flowie_cluster_route_snapshot_count(const flowie_cluster_route_snapshot_t *snapshot) {
  return snapshot ? snapshot->entry_count : 0u;
}

int flowie_cluster_route_snapshot_at(const flowie_cluster_route_snapshot_t *snapshot, size_t index,
                                     flowie_cluster_route_projection_t *out) {
  const flowie_cluster_route_snapshot_entry_t *entry;
  flowie_cluster_route_decoded_t decoded;
  if (out) *out = (flowie_cluster_route_projection_t)FLOWIE_CLUSTER_ROUTE_PROJECTION_INIT;
  if (!snapshot || !out || index >= snapshot->entry_count) return TURBO_EINVAL;
  entry = &snapshot->entries[index];
  if (flowie_cluster_route_decode(snapshot->store, snapshot->bytes + entry->value_offset,
                                  entry->value_size, &decoded) != TURBO_OK)
    return TURBO_EPROTO;
  out->client_id = (flowie_mqtt_span_t){snapshot->bytes + entry->key_offset, entry->key_size};
  out->edge_node_id = decoded.edge_node_id;
  memcpy(out->edge_boot_id, decoded.edge_boot_id, sizeof(out->edge_boot_id));
  out->advertised_endpoint = decoded.advertised_endpoint;
  out->session_shard = decoded.session_shard;
  out->owner_epoch = decoded.owner_epoch;
  out->fact_revision = decoded.fact_revision;
  out->connection_id = decoded.connection_id;
  out->connection_generation = decoded.connection_generation;
  out->session_generation = decoded.session_generation;
  out->lease_deadline_epoch_ms = decoded.lease_deadline_epoch_ms;
  out->active = (uint8_t)decoded.active;
  return TURBO_OK;
}

static int flowie_cluster_route_projection_validate(
    const flowie_cluster_route_store_t *store,
    const flowie_cluster_route_projection_t *projection) {
  if (!store || !projection || projection->size != sizeof(*projection) ||
      projection->abi_version != FLOWIE_CLUSTER_ROUTE_STORE_ABI_V1 ||
      !projection->client_id.data || projection->client_id.size == 0u ||
      projection->client_id.size > store->max_client_id_size ||
      !projection->edge_node_id.data || projection->edge_node_id.len == 0u ||
      projection->edge_node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX ||
      memchr(projection->edge_node_id.data, '\0', projection->edge_node_id.len) ||
      !flowie_cluster_route_nonzero(projection->edge_boot_id,
                                    sizeof(projection->edge_boot_id)) ||
      projection->session_shard >= FLOWIE_CLUSTER_SHARD_COUNT_MAX ||
      projection->owner_epoch == 0u || projection->fact_revision == 0u ||
      projection->connection_id == 0u || projection->connection_generation == 0u ||
      projection->session_generation == 0u || projection->active > 1u)
    return TURBO_EINVAL;
  if (projection->active) {
    if (!projection->advertised_endpoint.data || projection->advertised_endpoint.len == 0u ||
        projection->advertised_endpoint.len > store->max_endpoint_size ||
        memchr(projection->advertised_endpoint.data, '\0',
               projection->advertised_endpoint.len) ||
        projection->lease_deadline_epoch_ms == 0u)
      return TURBO_EINVAL;
  } else if (projection->advertised_endpoint.len != 0u ||
             projection->advertised_endpoint.data != NULL ||
             projection->lease_deadline_epoch_ms != 0u) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static int flowie_cluster_route_encode(const flowie_cluster_route_store_t *store,
                                       const flowie_cluster_route_projection_t *projection,
                                       tstr_t *out) {
  size_t total = FLOWIE_CLUSTER_ROUTE_RECORD_HEADER_SIZE;
  uint8_t *bytes;
  int rc = flowie_cluster_route_projection_validate(store, projection);
  if (out) *out = NULL;
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  if (projection->edge_node_id.len > SIZE_MAX - total) return TURBO_ERANGE;
  total += projection->edge_node_id.len;
  if (projection->advertised_endpoint.len > SIZE_MAX - total || total > UINT32_MAX)
    return TURBO_ERANGE;
  total += projection->advertised_endpoint.len;
  if (total > UINT32_MAX) return TURBO_ERANGE;
  *out = tstr_new_len(NULL, total);
  if (!*out) return TURBO_ENOMEM;
  bytes = (uint8_t *)*out;
  memcpy(bytes, FLOWIE_CLUSTER_ROUTE_RECORD_MAGIC, sizeof(FLOWIE_CLUSTER_ROUTE_RECORD_MAGIC));
  flowie_cluster_peer_wire_write_u16(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_VERSION,
                                     FLOWIE_CLUSTER_ROUTE_RECORD_VERSION);
  flowie_cluster_peer_wire_write_u16(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_HEADER_SIZE,
                                     FLOWIE_CLUSTER_ROUTE_RECORD_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_TOTAL_SIZE,
                                     (uint32_t)total);
  flowie_cluster_peer_wire_write_u32(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_FLAGS,
                                     projection->active
                                         ? FLOWIE_CLUSTER_ROUTE_RECORD_FLAG_ACTIVE
                                         : 0u);
  flowie_cluster_peer_wire_write_u32(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_SHARD,
                                     projection->session_shard);
  flowie_cluster_peer_wire_write_u32(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_RESERVED_1, 0u);
  flowie_cluster_peer_wire_write_u64(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_OWNER_EPOCH,
                                     projection->owner_epoch);
  flowie_cluster_peer_wire_write_u64(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_FACT_REVISION,
                                     projection->fact_revision);
  flowie_cluster_peer_wire_write_u64(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_CONNECTION_ID,
                                     projection->connection_id);
  flowie_cluster_peer_wire_write_u64(
      bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_CONNECTION_GENERATION,
      projection->connection_generation);
  flowie_cluster_peer_wire_write_u64(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_SESSION_GENERATION,
                                     projection->session_generation);
  flowie_cluster_peer_wire_write_u64(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_LEASE_DEADLINE,
                                     projection->lease_deadline_epoch_ms);
  flowie_cluster_peer_wire_write_u16(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_NODE_SIZE,
                                     (uint16_t)projection->edge_node_id.len);
  flowie_cluster_peer_wire_write_u16(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_ENDPOINT_SIZE,
                                     (uint16_t)projection->advertised_endpoint.len);
  flowie_cluster_peer_wire_write_u32(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_RESERVED_2, 0u);
  memcpy(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_BOOT_ID, projection->edge_boot_id,
         sizeof(projection->edge_boot_id));
  memcpy(bytes + FLOWIE_CLUSTER_ROUTE_RECORD_HEADER_SIZE, projection->edge_node_id.data,
         projection->edge_node_id.len);
  if (projection->advertised_endpoint.len != 0u)
    memcpy(bytes + FLOWIE_CLUSTER_ROUTE_RECORD_HEADER_SIZE + projection->edge_node_id.len,
           projection->advertised_endpoint.data, projection->advertised_endpoint.len);
  return TURBO_OK;
}

static int flowie_cluster_route_decode(const flowie_cluster_route_store_t *store,
                                       const void *data, size_t data_size,
                                       flowie_cluster_route_decoded_t *out) {
  const uint8_t *bytes = (const uint8_t *)data;
  uint32_t flags;
  size_t node_size;
  size_t endpoint_size;
  if (!store || !bytes || data_size < FLOWIE_CLUSTER_ROUTE_RECORD_HEADER_SIZE || !out)
    return TURBO_EPROTO;
  memset(out, 0, sizeof(*out));
  if (memcmp(bytes, FLOWIE_CLUSTER_ROUTE_RECORD_MAGIC,
             sizeof(FLOWIE_CLUSTER_ROUTE_RECORD_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_VERSION) !=
          FLOWIE_CLUSTER_ROUTE_RECORD_VERSION ||
      flowie_cluster_peer_wire_read_u16(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_HEADER_SIZE) !=
          FLOWIE_CLUSTER_ROUTE_RECORD_HEADER_SIZE ||
      flowie_cluster_peer_wire_read_u32(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_TOTAL_SIZE) !=
          data_size ||
      flowie_cluster_peer_wire_read_u32(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_RESERVED_1) != 0u ||
      flowie_cluster_peer_wire_read_u32(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_RESERVED_2) != 0u)
    return TURBO_EPROTO;
  flags = flowie_cluster_peer_wire_read_u32(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_FLAGS);
  node_size = flowie_cluster_peer_wire_read_u16(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_NODE_SIZE);
  endpoint_size =
      flowie_cluster_peer_wire_read_u16(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_ENDPOINT_SIZE);
  if ((flags & ~FLOWIE_CLUSTER_ROUTE_RECORD_FLAG_ACTIVE) != 0u || node_size == 0u ||
      node_size > FLOWIE_CLUSTER_NODE_ID_MAX || endpoint_size > store->max_endpoint_size ||
      node_size > data_size - FLOWIE_CLUSTER_ROUTE_RECORD_HEADER_SIZE ||
      endpoint_size != data_size - FLOWIE_CLUSTER_ROUTE_RECORD_HEADER_SIZE - node_size)
    return TURBO_EPROTO;
  out->bytes = bytes;
  out->size = data_size;
  out->active = (flags & FLOWIE_CLUSTER_ROUTE_RECORD_FLAG_ACTIVE) != 0u;
  out->session_shard =
      flowie_cluster_peer_wire_read_u32(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_SHARD);
  out->owner_epoch =
      flowie_cluster_peer_wire_read_u64(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_OWNER_EPOCH);
  out->fact_revision =
      flowie_cluster_peer_wire_read_u64(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_FACT_REVISION);
  out->connection_id =
      flowie_cluster_peer_wire_read_u64(bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_CONNECTION_ID);
  out->connection_generation = flowie_cluster_peer_wire_read_u64(
      bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_CONNECTION_GENERATION);
  out->session_generation = flowie_cluster_peer_wire_read_u64(
      bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_SESSION_GENERATION);
  out->lease_deadline_epoch_ms = flowie_cluster_peer_wire_read_u64(
      bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_LEASE_DEADLINE);
  out->edge_boot_id = bytes + FLOWIE_CLUSTER_ROUTE_OFFSET_BOOT_ID;
  out->edge_node_id =
      tstr_v_from_buf((const char *)(bytes + FLOWIE_CLUSTER_ROUTE_RECORD_HEADER_SIZE), node_size);
  out->advertised_endpoint = tstr_v_from_buf(
      (const char *)(bytes + FLOWIE_CLUSTER_ROUTE_RECORD_HEADER_SIZE + node_size), endpoint_size);
  if (out->session_shard >= FLOWIE_CLUSTER_SHARD_COUNT_MAX || out->owner_epoch == 0u ||
      out->fact_revision == 0u || out->connection_id == 0u ||
      out->connection_generation == 0u || out->session_generation == 0u ||
      !flowie_cluster_route_nonzero(out->edge_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) ||
      memchr(out->edge_node_id.data, '\0', out->edge_node_id.len) ||
      (out->active &&
       (out->advertised_endpoint.len == 0u || out->lease_deadline_epoch_ms == 0u ||
        memchr(out->advertised_endpoint.data, '\0', out->advertised_endpoint.len))) ||
      (!out->active &&
       (out->advertised_endpoint.len != 0u || out->lease_deadline_epoch_ms != 0u)))
    return TURBO_EPROTO;
  return TURBO_OK;
}

static int flowie_cluster_route_version_compare(
    const flowie_cluster_route_projection_t *incoming,
    const flowie_cluster_route_decoded_t *current) {
  if (incoming->owner_epoch != current->owner_epoch)
    return incoming->owner_epoch < current->owner_epoch ? -1 : 1;
  if (incoming->fact_revision != current->fact_revision)
    return incoming->fact_revision < current->fact_revision ? -1 : 1;
  return 0;
}

static int flowie_cluster_route_identity_equal(
    const flowie_cluster_route_projection_t *incoming,
    const flowie_cluster_route_decoded_t *current) {
  return incoming->session_shard == current->session_shard &&
         incoming->connection_id == current->connection_id &&
         incoming->connection_generation == current->connection_generation &&
         incoming->session_generation == current->session_generation &&
         incoming->active == current->active &&
         incoming->edge_node_id.len == current->edge_node_id.len &&
         memcmp(incoming->edge_node_id.data, current->edge_node_id.data,
                current->edge_node_id.len) == 0 &&
         memcmp(incoming->edge_boot_id, current->edge_boot_id,
                FLOWIE_CLUSTER_BOOT_ID_SIZE) == 0;
}

int flowie_cluster_route_store_project(flowie_cluster_route_store_t *store,
                                       const flowie_cluster_route_projection_t *projection,
                                       flowie_cluster_route_project_result_t *out_result) {
  turbo_flow_store_bytes_t key;
  turbo_flow_store_bytes_t value;
  tstr_t encoded = NULL;
  size_t attempt;
  int rc;
  if (out_result) *out_result = 0;
  if (!store || !out_result) return TURBO_EINVAL;
  rc = flowie_cluster_route_encode(store, projection, &encoded);
  if (rc != TURBO_OK) return rc;
  key = (turbo_flow_store_bytes_t){projection->client_id.data, projection->client_id.size};
  value = (turbo_flow_store_bytes_t){(const uint8_t *)encoded, tstr_len(encoded)};
  for (attempt = 0u; attempt < store->max_cas_attempts; ++attempt) {
    turbo_flow_state_record_t current_record = TURBO_FLOW_STATE_RECORD_INIT;
    flowie_cluster_route_decoded_t current;
    uint64_t expected_revision = 0u;
    uint64_t new_revision = 0u;
    int order = 1;
    rc = turbo_flow_state_store_get(store->state, key, &current_record);
    if (rc == TURBO_OK) {
      rc = !current_record.value
               ? TURBO_EPROTO
               : flowie_cluster_route_decode(store, mem_buffer_const_data(current_record.value),
                                              mem_buffer_used(current_record.value), &current);
      if (rc == TURBO_OK) {
        expected_revision = current_record.revision;
        order = flowie_cluster_route_version_compare(projection, &current);
        if (order < 0) {
          *out_result = FLOWIE_CLUSTER_ROUTE_PROJECT_STALE;
          turbo_flow_state_record_cleanup(&current_record);
          rc = TURBO_OK;
          break;
        }
        if (order == 0) {
          if (current.size == value.size &&
              memcmp(current.bytes, value.data, value.size) == 0) {
            *out_result = FLOWIE_CLUSTER_ROUTE_PROJECT_UNCHANGED;
            turbo_flow_state_record_cleanup(&current_record);
            rc = TURBO_OK;
            break;
          }
          if (!flowie_cluster_route_identity_equal(projection, &current) ||
              !projection->active ||
              projection->lease_deadline_epoch_ms <= current.lease_deadline_epoch_ms) {
            turbo_flow_state_record_cleanup(&current_record);
            rc = TURBO_EPROTO;
            break;
          }
        }
      }
      turbo_flow_state_record_cleanup(&current_record);
      if (rc != TURBO_OK) break;
    } else if (rc != TURBO_ENOENT) {
      break;
    }
    rc = turbo_flow_state_store_put(store->state, key, value, expected_revision, &new_revision);
    if (rc == TURBO_OK) {
      *out_result = FLOWIE_CLUSTER_ROUTE_PROJECT_APPLIED;
      break;
    }
    if (rc != TURBO_EBUSY) break;
  }
  if (rc == TURBO_EBUSY && attempt == store->max_cas_attempts) rc = TURBO_EBUSY;
  tstr_free(encoded);
  return rc;
}

int flowie_cluster_route_store_resolve(flowie_cluster_route_store_t *store,
                                       flowie_mqtt_span_t client_id, uint64_t now_epoch_ms,
                                       flowie_cluster_route_record_t *out) {
  turbo_flow_state_record_t stored = TURBO_FLOW_STATE_RECORD_INIT;
  flowie_cluster_route_decoded_t decoded = {0};
  turbo_flow_store_bytes_t key;
  int rc;
  if (out) *out = (flowie_cluster_route_record_t)FLOWIE_CLUSTER_ROUTE_RECORD_INIT;
  if (!store || !client_id.data || client_id.size == 0u ||
      client_id.size > store->max_client_id_size || now_epoch_ms == 0u || !out)
    return TURBO_EINVAL;
  key = (turbo_flow_store_bytes_t){client_id.data, client_id.size};
  rc = turbo_flow_state_store_get(store->state, key, &stored);
  if (rc != TURBO_OK) return rc;
  rc = !stored.value
           ? TURBO_EPROTO
           : flowie_cluster_route_decode(store, mem_buffer_const_data(stored.value),
                                         mem_buffer_used(stored.value), &decoded);
  if (rc == TURBO_OK &&
      (!decoded.active || decoded.lease_deadline_epoch_ms <= now_epoch_ms))
    rc = TURBO_ENOENT;
  if (rc == TURBO_OK) {
    out->edge_node_id = tstr_from_v(decoded.edge_node_id);
    out->advertised_endpoint = tstr_from_v(decoded.advertised_endpoint);
    if (!out->edge_node_id || !out->advertised_endpoint) {
      rc = TURBO_ENOMEM;
    } else {
      memcpy(out->edge_boot_id, decoded.edge_boot_id, sizeof(out->edge_boot_id));
      out->session_shard = decoded.session_shard;
      out->owner_epoch = decoded.owner_epoch;
      out->fact_revision = decoded.fact_revision;
      out->connection_id = decoded.connection_id;
      out->connection_generation = decoded.connection_generation;
      out->session_generation = decoded.session_generation;
      out->lease_deadline_epoch_ms = decoded.lease_deadline_epoch_ms;
    }
  }
  turbo_flow_state_record_cleanup(&stored);
  if (rc != TURBO_OK) flowie_cluster_route_record_cleanup(out);
  return rc;
}

void flowie_cluster_route_record_cleanup(flowie_cluster_route_record_t *record) {
  if (!record || record->size != sizeof(*record) ||
      record->abi_version != FLOWIE_CLUSTER_ROUTE_STORE_ABI_V1)
    return;
  tstr_free(record->edge_node_id);
  tstr_free(record->advertised_endpoint);
  *record = (flowie_cluster_route_record_t)FLOWIE_CLUSTER_ROUTE_RECORD_INIT;
}
