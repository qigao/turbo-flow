#include "flowie_cluster_redis_bus_internal.h"

#include "flowie_cluster_internal.h"
#include "fmt.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_vec.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_CLUSTER_REDIS_CONNECTION_TEXT_MAX 4096u
#define FLOWIE_CLUSTER_REDIS_DATABASE_MAX 15
#define FLOWIE_CLUSTER_REDIS_GROUP_START_ID "0"
#define FLOWIE_CLUSTER_REDIS_DEFAULT_FIELD "payload"
#define FLOWIE_CLUSTER_REDIS_TARGET_ACTIVE_CLAIMS 1u
#define FLOWIE_CLUSTER_REDIS_TARGET_SOURCE_POLL_MARKER_MS 1u

struct flowie_cluster_redis_bus_s {
  const flowie_cluster_redis_api_t *api;
  turbo_flow_redis_stream_publisher_t *publisher;
  turbo_flow_state_store_t *route_state;
  flowie_cluster_route_store_t *route_store;
  tstr_t cluster_id;
  tstr_t listener_id;
  tstr_t host;
  tstr_t username;
  tstr_t password;
  tstr_t stream;
  tstr_t field;
  uint16_t port;
  int database;
  uint32_t timeout_ms;
  uint32_t target_block_ms;
  size_t maxlen;
  size_t max_payload_size;
  uint32_t shard_count;
  int create_groups;
  turbo_vec_t targets;
  int targets_initialized;
};

struct flowie_cluster_redis_target_s {
  flowie_cluster_redis_bus_t *bus;
  turbo_flow_redis_stream_owner_t *owner;
  uint32_t shard_id;
  tstr_t group;
  tstr_t consumer;
};

static const flowie_cluster_redis_api_t FLOWIE_CLUSTER_REDIS_DEFAULT_API = {
    sizeof(flowie_cluster_redis_api_t),
    FLOWIE_CLUSTER_REDIS_BUS_ABI_V1,
    turbo_flow_redis_stream_publisher_create,
    turbo_flow_redis_stream_publisher_destroy,
    turbo_flow_redis_stream_publisher_publish,
    turbo_flow_redis_stream_owner_create_ex,
    turbo_flow_redis_stream_owner_destroy,
    turbo_flow_redis_stream_owner_claim,
    turbo_flow_redis_stream_owner_ack,
    turbo_flow_redis_stream_owner_requeue};

static int flowie_cluster_redis_text_valid(const char *text, size_t max_size, int allow_empty) {
  size_t size;
  if (!text) return allow_empty;
  size = strnlen(text, max_size + 1u);
  return size <= max_size && (allow_empty || size != 0u);
}

static int flowie_cluster_redis_api_validate(const flowie_cluster_redis_api_t *api) {
  if (!api || api->size != sizeof(*api) ||
      api->abi_version != FLOWIE_CLUSTER_REDIS_BUS_ABI_V1 || !api->publisher_create ||
      !api->publisher_destroy || !api->publisher_publish || !api->owner_create_ex ||
      !api->owner_destroy || !api->owner_claim || !api->owner_ack || !api->owner_requeue)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowie_cluster_redis_bus_config_validate(
    const flowie_cluster_redis_bus_config_t *config) {
  const turbo_flow_redis_stream_publisher_config_t *stream;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_REDIS_BUS_ABI_V1 ||
      !flowie_cluster_redis_text_valid(config->cluster_id, FLOWIE_CLUSTER_ID_MAX, 0) ||
      !flowie_cluster_redis_text_valid(config->listener_id, FLOWIE_CLUSTER_LISTENER_ID_MAX, 0) ||
      config->shard_count == 0u || config->shard_count > FLOWIE_CLUSTER_SHARD_COUNT_MAX ||
      config->target_block_ms > TURBO_FLOW_REDIS_MAX_BLOCK_MS ||
      (config->route_enabled &&
       (!config->route_state.key || !config->route_state.key[0] ||
        config->route_max_client_id_size == 0u || config->route_max_endpoint_size == 0u ||
        config->route_max_cas_attempts == 0u ||
        turbo_flow_store_limits_validate(&config->route_limits, 0) != TURBO_OK)))
    return TURBO_EINVAL;
  stream = &config->stream;
  if (stream->size != sizeof(*stream) ||
      stream->version != TURBO_FLOW_REDIS_STREAM_PUBLISHER_API_VERSION ||
      !flowie_cluster_redis_text_valid(stream->host,
                                       FLOWIE_CLUSTER_REDIS_CONNECTION_TEXT_MAX, 0) ||
      !flowie_cluster_redis_text_valid(stream->username,
                                       FLOWIE_CLUSTER_REDIS_CONNECTION_TEXT_MAX, 1) ||
      !flowie_cluster_redis_text_valid(stream->password,
                                       FLOWIE_CLUSTER_REDIS_CONNECTION_TEXT_MAX, 1) ||
      stream->port == 0u || stream->database < 0 ||
      stream->database > FLOWIE_CLUSTER_REDIS_DATABASE_MAX ||
      stream->timeout_ms > INT_MAX ||
      !flowie_cluster_redis_text_valid(stream->stream, TURBO_FLOW_REDIS_MAX_KEY_SIZE, 0) ||
      !flowie_cluster_redis_text_valid(stream->field, TURBO_FLOW_REDIS_MAX_KEY_SIZE, 1) ||
      (stream->field && !stream->field[0]) || stream->maxlen == 0u ||
      stream->max_payload_size == 0u ||
      stream->max_payload_size > TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowie_cluster_redis_bus_copy_config(
    flowie_cluster_redis_bus_t *bus, const flowie_cluster_redis_bus_config_t *config) {
  bus->cluster_id = tstr_dup(config->cluster_id);
  bus->listener_id = tstr_dup(config->listener_id);
  bus->host = tstr_dup(config->stream.host);
  bus->username = config->stream.username ? tstr_dup(config->stream.username) : NULL;
  bus->password = config->stream.password ? tstr_dup(config->stream.password) : NULL;
  bus->stream = tstr_dup(config->stream.stream);
  bus->field = tstr_dup(config->stream.field ? config->stream.field
                                             : FLOWIE_CLUSTER_REDIS_DEFAULT_FIELD);
  if (!bus->cluster_id || !bus->listener_id || !bus->host ||
      (config->stream.username && !bus->username) ||
      (config->stream.password && !bus->password) || !bus->stream || !bus->field)
    return TURBO_ENOMEM;
  bus->port = config->stream.port;
  bus->database = config->stream.database;
  bus->timeout_ms = config->stream.timeout_ms;
  bus->target_block_ms = config->target_block_ms;
  bus->maxlen = config->stream.maxlen;
  bus->max_payload_size = config->stream.max_payload_size;
  bus->shard_count = config->shard_count;
  bus->create_groups = config->create_groups != 0;
  return TURBO_OK;
}

static void flowie_cluster_redis_bus_free(flowie_cluster_redis_bus_t *bus) {
  if (!bus) return;
  if (bus->publisher) bus->api->publisher_destroy(bus->publisher);
  if (bus->route_store) flowie_cluster_route_store_destroy(bus->route_store);
  if (bus->route_state) turbo_flow_state_store_destroy(bus->route_state);
  if (bus->targets_initialized) turbo_vec_destroy(&bus->targets);
  tstr_freep(&bus->field);
  tstr_freep(&bus->stream);
  tstr_freep(&bus->password);
  tstr_freep(&bus->username);
  tstr_freep(&bus->host);
  tstr_freep(&bus->listener_id);
  tstr_freep(&bus->cluster_id);
  free(bus);
}

static flowie_cluster_redis_target_t **flowie_cluster_redis_target_slot(
    flowie_cluster_redis_bus_t *bus, uint32_t shard_id) {
  if (!bus || shard_id >= bus->shard_count) return NULL;
  return (flowie_cluster_redis_target_t **)turbo_vec_at(&bus->targets, shard_id);
}

static int flowie_cluster_redis_target_name(const flowie_cluster_redis_bus_t *bus,
                                            uint32_t shard_id, const char *suffix, tstr_t *out) {
  tstr_t name;
  if (!bus || shard_id >= bus->shard_count || !suffix || !out) return TURBO_EINVAL;
  *out = NULL;
  name = tstr_format("flowie.mqtt.c{}:{}.l{}:{}.s{}{}", tstr_len(bus->cluster_id),
                     bus->cluster_id, tstr_len(bus->listener_id), bus->listener_id, shard_id,
                     suffix);
  if (!name) return TURBO_ENOMEM;
  if (tstr_len(name) > TURBO_FLOW_REDIS_MAX_KEY_SIZE) {
    tstr_free(name);
    return TURBO_EINVAL;
  }
  *out = name;
  return TURBO_OK;
}

int flowie_cluster_redis_bus_create_with_api(const flowie_cluster_redis_bus_config_t *config,
                                             const flowie_cluster_redis_api_t *api,
                                             flowie_cluster_redis_bus_t **out) {
  turbo_flow_redis_stream_publisher_config_t publisher_config =
      TURBO_FLOW_REDIS_STREAM_PUBLISHER_CONFIG_INIT;
  flowie_cluster_redis_bus_t *bus = NULL;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_redis_bus_config_validate(config);
  if (rc != TURBO_OK || !out) return TURBO_EINVAL;
  rc = flowie_cluster_redis_api_validate(api);
  if (rc != TURBO_OK) return rc;
  bus = (flowie_cluster_redis_bus_t *)calloc(1u, sizeof(*bus));
  if (!bus) return TURBO_ENOMEM;
  bus->api = api;
  rc = flowie_cluster_redis_bus_copy_config(bus, config);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_vec_init(&bus->targets, sizeof(flowie_cluster_redis_target_t *));
  if (rc != TURBO_OK) goto fail;
  bus->targets_initialized = 1;
  rc = turbo_vec_resize(&bus->targets, bus->shard_count);
  if (rc != TURBO_OK) goto fail;
  memset(turbo_vec_data(&bus->targets), 0,
         (size_t)bus->shard_count * sizeof(flowie_cluster_redis_target_t *));

  publisher_config.host = bus->host;
  publisher_config.port = bus->port;
  publisher_config.username = bus->username;
  publisher_config.password = bus->password;
  publisher_config.database = bus->database;
  publisher_config.timeout_ms = bus->timeout_ms;
  publisher_config.stream = bus->stream;
  publisher_config.field = bus->field;
  publisher_config.maxlen = bus->maxlen;
  publisher_config.max_payload_size = bus->max_payload_size;
  rc = bus->api->publisher_create(&publisher_config, &bus->publisher);
  if (rc != TURBO_OK) goto fail;
  if (!bus->publisher) {
    rc = TURBO_EPROTO;
    goto fail;
  }
  if (config->route_enabled) {
    flowie_cluster_route_store_config_t route_config = FLOWIE_CLUSTER_ROUTE_STORE_CONFIG_INIT;
    rc = turbo_flow_redis_state_store_create(&config->route_state, &config->route_limits,
                                             &bus->route_state);
    if (rc != TURBO_OK) goto fail;
    route_config.state = bus->route_state;
    route_config.max_client_id_size = config->route_max_client_id_size;
    route_config.max_endpoint_size = config->route_max_endpoint_size;
    route_config.max_cas_attempts = config->route_max_cas_attempts;
    rc = flowie_cluster_route_store_create(&route_config, &bus->route_store);
    if (rc != TURBO_OK) goto fail;
  }
  *out = bus;
  return TURBO_OK;

fail:
  flowie_cluster_redis_bus_free(bus);
  return rc;
}

int flowie_cluster_redis_bus_create(const flowie_cluster_redis_bus_config_t *config,
                                    flowie_cluster_redis_bus_t **out) {
  return flowie_cluster_redis_bus_create_with_api(config, &FLOWIE_CLUSTER_REDIS_DEFAULT_API, out);
}

int flowie_cluster_redis_bus_publish(void *ctx, const void *payload, size_t payload_size) {
  flowie_cluster_redis_bus_t *bus = (flowie_cluster_redis_bus_t *)ctx;
  if (!bus || !bus->publisher || (!payload && payload_size != 0u)) return TURBO_EINVAL;
  if (payload_size > bus->max_payload_size) return TURBO_EMSGSIZE;
  return bus->api->publisher_publish(bus->publisher, payload, payload_size);
}

flowie_cluster_route_store_t *flowie_cluster_redis_bus_route_store(
    flowie_cluster_redis_bus_t *bus) {
  return bus ? bus->route_store : NULL;
}

int flowie_cluster_redis_target_create(flowie_cluster_redis_bus_t *bus, uint32_t shard_id,
                                       flowie_cluster_redis_target_t **out) {
  turbo_flow_redis_stream_claim_owner_config_t claim_config =
      TURBO_FLOW_REDIS_STREAM_CLAIM_OWNER_CONFIG_INIT;
  turbo_flow_redis_stream_config_t stream_config;
  flowie_cluster_redis_target_t **slot;
  flowie_cluster_redis_target_t *target = NULL;
  int rc;
  if (out) *out = NULL;
  if (!bus || !out || !(slot = flowie_cluster_redis_target_slot(bus, shard_id)))
    return TURBO_EINVAL;
  if (*slot) return TURBO_EALREADY;
  target = (flowie_cluster_redis_target_t *)calloc(1u, sizeof(*target));
  if (!target) return TURBO_ENOMEM;
  target->bus = bus;
  target->shard_id = shard_id;
  rc = flowie_cluster_redis_target_name(bus, shard_id, "", &target->group);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_cluster_redis_target_name(bus, shard_id, ".owner", &target->consumer);
  if (rc != TURBO_OK) goto fail;
  memset(&stream_config, 0, sizeof(stream_config));
  stream_config.host = bus->host;
  stream_config.port = bus->port;
  stream_config.username = bus->username;
  stream_config.password = bus->password;
  stream_config.database = bus->database;
  stream_config.timeout_ms = bus->timeout_ms;
  stream_config.stream = bus->stream;
  stream_config.field = bus->field;
  stream_config.maxlen = bus->maxlen;
  stream_config.poll_interval_ms = FLOWIE_CLUSTER_REDIS_TARGET_SOURCE_POLL_MARKER_MS;
  stream_config.group = target->group;
  stream_config.consumer = target->consumer;
  stream_config.group_start_id = FLOWIE_CLUSTER_REDIS_GROUP_START_ID;
  stream_config.read_count = 1u;
  stream_config.block_ms = bus->target_block_ms;
  stream_config.create_group = bus->create_groups;
  claim_config.max_active_claims = FLOWIE_CLUSTER_REDIS_TARGET_ACTIVE_CLAIMS;
  rc = bus->api->owner_create_ex(&stream_config, &claim_config, &target->owner);
  if (rc != TURBO_OK) goto fail;
  if (!target->owner) {
    rc = TURBO_EPROTO;
    goto fail;
  }
  *slot = target;
  *out = target;
  return TURBO_OK;

fail:
  if (target->owner) bus->api->owner_destroy(target->owner);
  tstr_freep(&target->consumer);
  tstr_freep(&target->group);
  free(target);
  return rc;
}

int flowie_cluster_redis_target_claim(void *ctx,
                                      flowie_cluster_broadcast_target_claim_t *out) {
  flowie_cluster_redis_target_t *target = (flowie_cluster_redis_target_t *)ctx;
  turbo_flow_redis_stream_claim_t claim = TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
  int rc;
  if (!target || !target->owner || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_BROADCAST_TARGET_DISPATCH_ABI_V1)
    return TURBO_EINVAL;
  rc = target->bus->api->owner_claim(target->owner, &claim);
  if (rc != TURBO_OK) return rc;
  if (claim.size < sizeof(claim) || claim.token == 0u ||
      (!claim.payload.data && claim.payload.len != 0u)) {
    if (claim.token != 0u) (void)target->bus->api->owner_requeue(target->owner, claim.token);
    return TURBO_EPROTO;
  }
  *out = (flowie_cluster_broadcast_target_claim_t)FLOWIE_CLUSTER_BROADCAST_TARGET_CLAIM_INIT;
  out->token = claim.token;
  out->payload = claim.payload;
  return TURBO_OK;
}

int flowie_cluster_redis_target_ack(void *ctx, uint64_t token) {
  flowie_cluster_redis_target_t *target = (flowie_cluster_redis_target_t *)ctx;
  if (!target || !target->owner || token == 0u) return TURBO_EINVAL;
  return target->bus->api->owner_ack(target->owner, token);
}

int flowie_cluster_redis_target_requeue(void *ctx, uint64_t token) {
  flowie_cluster_redis_target_t *target = (flowie_cluster_redis_target_t *)ctx;
  if (!target || !target->owner || token == 0u) return TURBO_EINVAL;
  return target->bus->api->owner_requeue(target->owner, token);
}

int flowie_cluster_redis_target_identity(const flowie_cluster_redis_target_t *target,
                                         tstr_v *group, tstr_v *consumer) {
  if (!target || !group || !consumer) return TURBO_EINVAL;
  *group = tstr_to_v(target->group);
  *consumer = tstr_to_v(target->consumer);
  return TURBO_OK;
}

void flowie_cluster_redis_target_destroy(flowie_cluster_redis_target_t *target) {
  flowie_cluster_redis_target_t **slot;
  if (!target) return;
  slot = flowie_cluster_redis_target_slot(target->bus, target->shard_id);
  if (slot && *slot == target) *slot = NULL;
  if (target->owner) target->bus->api->owner_destroy(target->owner);
  tstr_freep(&target->consumer);
  tstr_freep(&target->group);
  free(target);
}

int flowie_cluster_redis_bus_destroy(flowie_cluster_redis_bus_t *bus) {
  size_t target_count;
  if (!bus) return TURBO_EINVAL;
  target_count = turbo_vec_size(&bus->targets);
  for (size_t i = 0u; i < target_count; ++i) {
    flowie_cluster_redis_target_t **slot =
        (flowie_cluster_redis_target_t **)turbo_vec_at(&bus->targets, i);
    if (slot && *slot) return TURBO_EBUSY;
  }
  flowie_cluster_redis_bus_free(bus);
  return TURBO_OK;
}
