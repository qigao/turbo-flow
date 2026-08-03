#include "flowie_cluster_redis_bus_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct fake_redis_owner_s {
  uint64_t next_token;
  uint64_t active_token;
  const char *payload;
  size_t payload_size;
  int claim_available;
  int ack_calls;
  int requeue_calls;
} fake_redis_owner_t;

typedef struct fake_redis_state_s {
  int publisher_cookie;
  int publisher_create_status;
  int owner_create_status;
  int publish_status;
  int publisher_create_calls;
  int publisher_destroy_calls;
  int owner_create_calls;
  int owner_destroy_calls;
  int publish_calls;
  size_t max_payload_size;
  char stream[128];
  char field[128];
  char last_group[1024];
  char last_consumer[1024];
  char group_start_id[16];
  size_t read_count;
  size_t max_active_claims;
  uint32_t block_ms;
  int create_group;
  char published[128];
  size_t published_size;
  fake_redis_owner_t *last_owner;
} fake_redis_state_t;

static fake_redis_state_t fake_redis;

static void fake_redis_reset(void) {
  memset(&fake_redis, 0, sizeof(fake_redis));
  fake_redis.publisher_create_status = TURBO_OK;
  fake_redis.owner_create_status = TURBO_OK;
  fake_redis.publish_status = TURBO_OK;
}

static int fake_publisher_create(const turbo_flow_redis_stream_publisher_config_t *config,
                                 turbo_flow_redis_stream_publisher_t **out) {
  fake_redis.publisher_create_calls += 1;
  if (out) *out = NULL;
  if (fake_redis.publisher_create_status != TURBO_OK)
    return fake_redis.publisher_create_status;
  if (!config || !out) return TURBO_EINVAL;
  fake_redis.max_payload_size = config->max_payload_size;
  (void)snprintf(fake_redis.stream, sizeof(fake_redis.stream), "%s", config->stream);
  (void)snprintf(fake_redis.field, sizeof(fake_redis.field), "%s", config->field);
  *out = (turbo_flow_redis_stream_publisher_t *)&fake_redis.publisher_cookie;
  return TURBO_OK;
}

static void fake_publisher_destroy(turbo_flow_redis_stream_publisher_t *publisher) {
  if (publisher) fake_redis.publisher_destroy_calls += 1;
}

static int fake_publisher_publish(void *publisher, const void *payload, size_t payload_size) {
  if (publisher != &fake_redis.publisher_cookie || (!payload && payload_size != 0u))
    return TURBO_EINVAL;
  fake_redis.publish_calls += 1;
  if (payload_size > sizeof(fake_redis.published)) return TURBO_EMSGSIZE;
  if (payload_size != 0u) memcpy(fake_redis.published, payload, payload_size);
  fake_redis.published_size = payload_size;
  return fake_redis.publish_status;
}

static int fake_owner_create_ex(
    const turbo_flow_redis_stream_config_t *config,
    const turbo_flow_redis_stream_claim_owner_config_t *claim_config,
    turbo_flow_redis_stream_owner_t **out) {
  fake_redis_owner_t *owner;
  fake_redis.owner_create_calls += 1;
  if (out) *out = NULL;
  if (fake_redis.owner_create_status != TURBO_OK) return fake_redis.owner_create_status;
  if (!config || !claim_config || !out) return TURBO_EINVAL;
  owner = (fake_redis_owner_t *)calloc(1u, sizeof(*owner));
  if (!owner) return TURBO_ENOMEM;
  owner->next_token = 40u;
  (void)snprintf(fake_redis.last_group, sizeof(fake_redis.last_group), "%s", config->group);
  (void)snprintf(fake_redis.last_consumer, sizeof(fake_redis.last_consumer), "%s",
                 config->consumer);
  (void)snprintf(fake_redis.group_start_id, sizeof(fake_redis.group_start_id), "%s",
                 config->group_start_id);
  fake_redis.read_count = config->read_count;
  fake_redis.block_ms = config->block_ms;
  fake_redis.create_group = config->create_group;
  fake_redis.max_active_claims = claim_config->max_active_claims;
  fake_redis.last_owner = owner;
  *out = (turbo_flow_redis_stream_owner_t *)owner;
  return TURBO_OK;
}

static void fake_owner_destroy(turbo_flow_redis_stream_owner_t *owner) {
  fake_redis_owner_t *typed = (fake_redis_owner_t *)owner;
  if (!typed) return;
  if (fake_redis.last_owner == typed) fake_redis.last_owner = NULL;
  fake_redis.owner_destroy_calls += 1;
  free(typed);
}

static int fake_owner_claim(turbo_flow_redis_stream_owner_t *owner,
                            turbo_flow_redis_stream_claim_t *claim) {
  fake_redis_owner_t *typed = (fake_redis_owner_t *)owner;
  if (!typed || !claim || claim->size < sizeof(*claim)) return TURBO_EINVAL;
  if (typed->active_token != 0u) return TURBO_EBUSY;
  if (!typed->claim_available) return TURBO_ENOENT;
  typed->claim_available = 0;
  typed->active_token = ++typed->next_token;
  *claim = (turbo_flow_redis_stream_claim_t)TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
  claim->token = typed->active_token;
  claim->entry_id = "1-0";
  claim->payload = tstr_v_from_buf(typed->payload, typed->payload_size);
  return TURBO_OK;
}

static int fake_owner_ack(turbo_flow_redis_stream_owner_t *owner, uint64_t token) {
  fake_redis_owner_t *typed = (fake_redis_owner_t *)owner;
  if (!typed || token == 0u) return TURBO_EINVAL;
  if (typed->active_token != token) return TURBO_EALREADY;
  typed->ack_calls += 1;
  typed->active_token = 0u;
  return TURBO_OK;
}

static int fake_owner_requeue(turbo_flow_redis_stream_owner_t *owner, uint64_t token) {
  fake_redis_owner_t *typed = (fake_redis_owner_t *)owner;
  if (!typed || token == 0u) return TURBO_EINVAL;
  if (typed->active_token != token) return TURBO_EALREADY;
  typed->requeue_calls += 1;
  typed->active_token = 0u;
  typed->claim_available = 1;
  return TURBO_OK;
}

static const flowie_cluster_redis_api_t FAKE_REDIS_API = {
    sizeof(flowie_cluster_redis_api_t),
    FLOWIE_CLUSTER_REDIS_BUS_ABI_V1,
    fake_publisher_create,
    fake_publisher_destroy,
    fake_publisher_publish,
    fake_owner_create_ex,
    fake_owner_destroy,
    fake_owner_claim,
    fake_owner_ack,
    fake_owner_requeue};

static flowie_cluster_redis_bus_config_t redis_bus_config(void) {
  flowie_cluster_redis_bus_config_t config = FLOWIE_CLUSTER_REDIS_BUS_CONFIG_INIT;
  config.cluster_id = "alpha";
  config.listener_id = "mqtt";
  config.shard_count = 4u;
  config.stream.host = "127.0.0.1";
  config.stream.port = 6379u;
  config.stream.database = 2;
  config.stream.timeout_ms = 1500u;
  config.stream.stream = "flowie.cluster.broadcast";
  config.stream.field = "tfbe";
  config.stream.maxlen = 4096u;
  config.stream.max_payload_size = 8192u;
  config.target_block_ms = 25u;
  config.create_groups = 1;
  return config;
}

spec("Flowie cluster Redis broadcast bus") {
  before_each() { fake_redis_reset(); }

  group("configuration") {
    it("rejects an invalid shard topology before opening Redis") {
      flowie_cluster_redis_bus_config_t config = redis_bus_config();
      flowie_cluster_redis_bus_t *bus = NULL;
      config.shard_count = 0u;

      check_int_eq(flowie_cluster_redis_bus_create_with_api(&config, &FAKE_REDIS_API, &bus),
                   TURBO_EINVAL);
      check_null(bus);
      check_int_eq(fake_redis.publisher_create_calls, 0);
    }

    it("propagates publisher creation failure without exposing a partial bus") {
      flowie_cluster_redis_bus_config_t config = redis_bus_config();
      flowie_cluster_redis_bus_t *bus = NULL;
      fake_redis.publisher_create_status = TURBO_EIO;

      check_int_eq(flowie_cluster_redis_bus_create_with_api(&config, &FAKE_REDIS_API, &bus),
                   TURBO_EIO);
      check_null(bus);
      check_int_eq(fake_redis.publisher_create_calls, 1);
      check_int_eq(fake_redis.publisher_destroy_calls, 0);
    }
  }

  group("per-shard ownership") {
    it("assigns stable distinct groups and keeps bus destruction ordered") {
      flowie_cluster_redis_bus_config_t config = redis_bus_config();
      flowie_cluster_redis_bus_t *bus = NULL;
      flowie_cluster_redis_target_t *target_one = NULL;
      flowie_cluster_redis_target_t *target_two = NULL;
      flowie_cluster_redis_target_t *duplicate = NULL;
      tstr_v group_one;
      tstr_v consumer_one;
      tstr_v group_two;
      tstr_v consumer_two;

      check_int_eq(flowie_cluster_redis_bus_create_with_api(&config, &FAKE_REDIS_API, &bus),
                   TURBO_OK);
      check_int_eq(flowie_cluster_redis_target_create(bus, 1u, &target_one), TURBO_OK);
      check_int_eq(flowie_cluster_redis_target_identity(target_one, &group_one, &consumer_one),
                   TURBO_OK);
      check_str_eq(group_one.data, "flowie.mqtt.c5:alpha.l4:mqtt.s1");
      check_str_eq(consumer_one.data, "flowie.mqtt.c5:alpha.l4:mqtt.s1.owner");
      check_str_eq(fake_redis.group_start_id, "0");
      check_size_eq(fake_redis.read_count, 1u);
      check_size_eq(fake_redis.max_active_claims, 1u);
      check_uint_eq(fake_redis.block_ms, 25u);
      check_int_eq(fake_redis.create_group, 1);

      check_int_eq(flowie_cluster_redis_target_create(bus, 2u, &target_two), TURBO_OK);
      check_int_eq(flowie_cluster_redis_target_identity(target_two, &group_two, &consumer_two),
                   TURBO_OK);
      check_str_eq(group_two.data, "flowie.mqtt.c5:alpha.l4:mqtt.s2");
      check_str_eq(consumer_two.data, "flowie.mqtt.c5:alpha.l4:mqtt.s2.owner");
      check_str_ne(group_one.data, group_two.data);
      check_int_eq(flowie_cluster_redis_target_create(bus, 2u, &duplicate), TURBO_EALREADY);
      check_null(duplicate);
      check_int_eq(flowie_cluster_redis_bus_destroy(bus), TURBO_EBUSY);

      flowie_cluster_redis_target_destroy(target_two);
      flowie_cluster_redis_target_destroy(target_one);
      check_int_eq(flowie_cluster_redis_bus_destroy(bus), TURBO_OK);
      check_int_eq(fake_redis.owner_destroy_calls, 2);
      check_int_eq(fake_redis.publisher_destroy_calls, 1);
    }

    it("releases a failed target reservation so the shard can retry") {
      flowie_cluster_redis_bus_config_t config = redis_bus_config();
      flowie_cluster_redis_bus_t *bus = NULL;
      flowie_cluster_redis_target_t *target = NULL;

      check_int_eq(flowie_cluster_redis_bus_create_with_api(&config, &FAKE_REDIS_API, &bus),
                   TURBO_OK);
      fake_redis.owner_create_status = TURBO_EIO;
      check_int_eq(flowie_cluster_redis_target_create(bus, 3u, &target), TURBO_EIO);
      check_null(target);
      fake_redis.owner_create_status = TURBO_OK;
      check_int_eq(flowie_cluster_redis_target_create(bus, 3u, &target), TURBO_OK);

      flowie_cluster_redis_target_destroy(target);
      check_int_eq(flowie_cluster_redis_bus_destroy(bus), TURBO_OK);
    }
  }

  group("dispatcher ports") {
    it("maps publisher bytes and exact Redis claim tokens") {
      static const char event[] = {'T', 'F', 'B', 'E', '\0', 'x'};
      flowie_cluster_redis_bus_config_t config = redis_bus_config();
      flowie_cluster_broadcast_target_claim_t claim =
          FLOWIE_CLUSTER_BROADCAST_TARGET_CLAIM_INIT;
      flowie_cluster_redis_bus_t *bus = NULL;
      flowie_cluster_redis_target_t *target = NULL;
      fake_redis_owner_t *owner;

      check_int_eq(flowie_cluster_redis_bus_create_with_api(&config, &FAKE_REDIS_API, &bus),
                   TURBO_OK);
      check_str_eq(fake_redis.stream, "flowie.cluster.broadcast");
      check_str_eq(fake_redis.field, "tfbe");
      check_size_eq(fake_redis.max_payload_size, 8192u);
      check_int_eq(flowie_cluster_redis_bus_publish(bus, event, sizeof(event)), TURBO_OK);
      check_size_eq(fake_redis.published_size, sizeof(event));
      check_mem_eq(fake_redis.published, event, sizeof(event));

      check_int_eq(flowie_cluster_redis_target_create(bus, 0u, &target), TURBO_OK);
      owner = fake_redis.last_owner;
      check_not_null(owner);
      owner->payload = event;
      owner->payload_size = sizeof(event);
      owner->claim_available = 1;
      check_int_eq(flowie_cluster_redis_target_claim(target, &claim), TURBO_OK);
      check_hex64_eq(claim.token, 41u);
      check_size_eq(claim.payload.len, sizeof(event));
      check_mem_eq(claim.payload.data, event, sizeof(event));
      check_int_eq(flowie_cluster_redis_target_ack(target, claim.token), TURBO_OK);
      check_int_eq(owner->ack_calls, 1);

      owner->claim_available = 1;
      claim = (flowie_cluster_broadcast_target_claim_t)
          FLOWIE_CLUSTER_BROADCAST_TARGET_CLAIM_INIT;
      check_int_eq(flowie_cluster_redis_target_claim(target, &claim), TURBO_OK);
      check_hex64_eq(claim.token, 42u);
      check_int_eq(flowie_cluster_redis_target_requeue(target, claim.token), TURBO_OK);
      check_int_eq(owner->requeue_calls, 1);

      flowie_cluster_redis_target_destroy(target);
      check_int_eq(flowie_cluster_redis_bus_destroy(bus), TURBO_OK);
    }
  }
}
