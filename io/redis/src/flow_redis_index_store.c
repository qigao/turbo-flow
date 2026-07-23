#include "flow_redis_storage_internal.h"
#include "turbo_flow_store_redis.h"

#include "flow_redis_internal.h"
#include "turbo_flow_index_store_provider.h"

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_REDIS_LUA_MAX_EXACT_INTEGER UINT64_C(9007199254740991)
#define FLOW_REDIS_INDEX_KEY_SUFFIX ":set:"
#define FLOW_REDIS_INDEX_META_SUFFIX ":meta"

typedef struct flow_redis_index_store_s {
  flow_redis_store_client_t *client;
  turbo_flow_store_limits_t limits;
  turbo_flow_store_stats_t stats;
  char *prefix;
  size_t prefix_size;
  char *meta_key;
  size_t meta_key_size;
  size_t max_index_name_size;
  size_t max_member_size;
  int closed;
} flow_redis_index_store_t;

typedef struct flow_redis_integer_reply_s {
  int64_t value;
} flow_redis_integer_reply_t;

typedef struct flow_redis_members_reply_s {
  turbo_flow_index_visit_fn visit;
  void *ctx;
  size_t max_records;
  size_t max_member_size;
  size_t count;
} flow_redis_members_reply_t;

typedef struct flow_redis_index_stats_reply_s {
  size_t records;
  size_t bytes;
} flow_redis_index_stats_reply_t;

static int flow_redis_size_parse(const redis_reply_t *reply, size_t *out) {
  uint64_t value = 0u;
  if (!reply || !out || reply->type != REDIS_REPLY_BULK_STRING || !reply->str || reply->len == 0u) {
    return TURBO_EPROTO;
  }
  for (size_t i = 0u; i < reply->len; ++i) {
    const unsigned int digit = (unsigned int)(reply->str[i] - '0');
    if (reply->str[i] < '0' || reply->str[i] > '9' || value > ((uint64_t)SIZE_MAX - digit) / 10u) {
      return TURBO_EPROTO;
    }
    value = value * 10u + digit;
  }
  *out = (size_t)value;
  return TURBO_OK;
}

static int flow_redis_integer_apply(void *ctx, const redis_reply_t *reply) {
  flow_redis_integer_reply_t *result = (flow_redis_integer_reply_t *)ctx;
  if (!result || !reply || reply->type != REDIS_REPLY_INTEGER) return TURBO_EPROTO;
  result->value = reply->integer;
  return TURBO_OK;
}

static int flow_redis_members_apply(void *ctx, const redis_reply_t *reply) {
  flow_redis_members_reply_t *result = (flow_redis_members_reply_t *)ctx;
  if (!result || !reply || reply->type != REDIS_REPLY_ARRAY ||
      reply->element_count > result->max_records) {
    return TURBO_EPROTO;
  }
  for (size_t i = 0u; i < reply->element_count; ++i) {
    const redis_reply_t *member = reply->elements[i];
    turbo_flow_store_bytes_t view;
    int rc;
    if (!member || member->type != REDIS_REPLY_BULK_STRING || (!member->str && member->len > 0u) ||
        member->len == 0u || member->len > result->max_member_size) {
      return TURBO_EPROTO;
    }
    view.data = (const uint8_t *)member->str;
    view.size = member->len;
    if (result->visit) {
      rc = result->visit(result->ctx, view);
      if (rc != TURBO_OK) return rc;
    }
  }
  result->count = reply->element_count;
  return TURBO_OK;
}

static int flow_redis_index_stats_apply(void *ctx, const redis_reply_t *reply) {
  flow_redis_index_stats_reply_t *result = (flow_redis_index_stats_reply_t *)ctx;
  int saw_records = 0;
  int saw_bytes = 0;
  if (!result || !reply || reply->type != REDIS_REPLY_ARRAY || (reply->element_count & 1u) != 0u ||
      reply->element_count > 4u) {
    return TURBO_EPROTO;
  }
  for (size_t i = 0u; i < reply->element_count; i += 2u) {
    const redis_reply_t *field = reply->elements[i];
    const redis_reply_t *value = reply->elements[i + 1u];
    if (!field || field->type != REDIS_REPLY_BULK_STRING || !field->str) return TURBO_EPROTO;
    if (field->len == 7u && memcmp(field->str, "records", 7u) == 0) {
      if (saw_records || flow_redis_size_parse(value, &result->records) != TURBO_OK)
        return TURBO_EPROTO;
      saw_records = 1;
    } else if (field->len == 5u && memcmp(field->str, "bytes", 5u) == 0) {
      if (saw_bytes || flow_redis_size_parse(value, &result->bytes) != TURBO_OK)
        return TURBO_EPROTO;
      saw_bytes = 1;
    } else {
      return TURBO_EPROTO;
    }
  }
  return saw_records == saw_bytes ? TURBO_OK : TURBO_EPROTO;
}

static char flow_redis_hex_digit(unsigned int value) {
  return (char)(value < 10u ? '0' + value : 'a' + value - 10u);
}

static int flow_redis_namespace_prefix(const char *name, size_t name_size, char **out,
                                       size_t *out_size) {
  size_t required;
  char *prefix;
  if (!name || name_size == 0u || !out || !out_size || name_size > (SIZE_MAX - 2u) / 2u)
    return TURBO_EINVAL;
  required = name_size * 2u + 2u;
  prefix = (char *)malloc(required + 1u);
  if (!prefix) return TURBO_ENOMEM;
  prefix[0] = '{';
  for (size_t i = 0u; i < name_size; ++i) {
    const uint8_t byte = (uint8_t)name[i];
    prefix[1u + i * 2u] = flow_redis_hex_digit(byte >> 4u);
    prefix[1u + i * 2u + 1u] = flow_redis_hex_digit(byte & 0x0fu);
  }
  prefix[required - 1u] = '}';
  prefix[required] = '\0';
  *out = prefix;
  *out_size = required;
  return TURBO_OK;
}

static int flow_redis_hex_key(const char *prefix, size_t prefix_size, const char *suffix,
                              const uint8_t *data, size_t data_size, char **out, size_t *out_size) {
  const size_t suffix_size = strlen(suffix);
  size_t required;
  char *key;
  if (!prefix || !suffix || !out || !out_size || (!data && data_size > 0u) ||
      data_size > (SIZE_MAX - prefix_size - suffix_size) / 2u) {
    return TURBO_EINVAL;
  }
  required = prefix_size + suffix_size + data_size * 2u;
  if (required > TURBO_FLOW_REDIS_MAX_KEY_SIZE) return TURBO_EFBIG;
  key = (char *)malloc(required + 1u);
  if (!key) return TURBO_ENOMEM;
  memcpy(key, prefix, prefix_size);
  memcpy(key + prefix_size, suffix, suffix_size);
  for (size_t i = 0u; i < data_size; ++i) {
    key[prefix_size + suffix_size + i * 2u] = flow_redis_hex_digit(data[i] >> 4u);
    key[prefix_size + suffix_size + i * 2u + 1u] = flow_redis_hex_digit(data[i] & 0x0fu);
  }
  key[required] = '\0';
  *out = key;
  *out_size = required;
  return TURBO_OK;
}

static int flow_redis_index_key(flow_redis_index_store_t *store, turbo_flow_store_bytes_t index,
                                char **out, size_t *out_size) {
  if (!store || !index.data || index.size == 0u) return TURBO_EINVAL;
  if (index.size > store->max_index_name_size) return TURBO_EFBIG;
  return flow_redis_hex_key(store->prefix, store->prefix_size, FLOW_REDIS_INDEX_KEY_SUFFIX,
                            index.data, index.size, out, out_size);
}

static int flow_redis_index_member_validate(const flow_redis_index_store_t *store,
                                            turbo_flow_store_bytes_t index,
                                            turbo_flow_store_bytes_t member) {
  if (!store || !index.data || index.size == 0u || !member.data || member.size == 0u)
    return TURBO_EINVAL;
  if (index.size > store->max_index_name_size || member.size > store->max_member_size ||
      index.size > SIZE_MAX - member.size ||
      index.size + member.size > store->limits.max_item_bytes) {
    return TURBO_EFBIG;
  }
  return TURBO_OK;
}

static int flow_redis_index_command(flow_redis_index_store_t *store, int argc, const char **argv,
                                    const size_t *lengths, flow_redis_store_reply_fn apply,
                                    void *ctx) {
  const int rc = flow_redis_store_command(store->client, argc, argv, lengths, apply, ctx);
  if (rc != TURBO_OK) store->stats.backend_errors++;
  return rc;
}

static int flow_redis_index_close(void *ctx) {
  flow_redis_index_store_t *store = (flow_redis_index_store_t *)ctx;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_EALREADY;
  store->closed = 1;
  return TURBO_OK;
}

static void flow_redis_index_destroy(void *ctx) {
  flow_redis_index_store_t *store = (flow_redis_index_store_t *)ctx;
  if (!store) return;
  flow_redis_store_client_destroy(store->client);
  free(store->meta_key);
  free(store->prefix);
  free(store);
}

static int flow_redis_index_add(void *ctx, turbo_flow_store_bytes_t index,
                                turbo_flow_store_bytes_t member) {
  static const char script[] =
      "local c=redis.call('SISMEMBER',KEYS[1],ARGV[1]); if c==1 then return 0 end; "
      "local r=tonumber(redis.call('HGET',KEYS[2],'records') or '0'); "
      "local b=tonumber(redis.call('HGET',KEYS[2],'bytes') or '0'); "
      "local d=tonumber(ARGV[3]); if redis.call('SCARD',KEYS[1])==0 then "
      "d=d+tonumber(ARGV[2]) end; if r+1>tonumber(ARGV[4]) or "
      "b+d>tonumber(ARGV[5]) then return -1 end; redis.call('SADD',KEYS[1],ARGV[1]); "
      "redis.call('HSET',KEYS[2],'records',r+1,'bytes',b+d); return 1";
  flow_redis_index_store_t *store = (flow_redis_index_store_t *)ctx;
  flow_redis_integer_reply_t reply = {0};
  char index_size[32];
  char member_size[32];
  char max_records[32];
  char max_bytes[32];
  char *set_key = NULL;
  size_t set_key_size = 0u;
  int rc;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  rc = flow_redis_index_member_validate(store, index, member);
  if (rc != TURBO_OK) {
    if (rc == TURBO_EFBIG) store->stats.rejects++;
    return rc;
  }
  rc = flow_redis_index_key(store, index, &set_key, &set_key_size);
  if (rc != TURBO_OK) return rc;
  (void)snprintf(index_size, sizeof(index_size), "%zu", index.size);
  (void)snprintf(member_size, sizeof(member_size), "%zu", member.size);
  (void)snprintf(max_records, sizeof(max_records), "%zu", store->limits.max_records);
  (void)snprintf(max_bytes, sizeof(max_bytes), "%zu", store->limits.max_bytes);
  {
    const char *argv[] = {
        "EVAL",     script,      "2",         set_key,  store->meta_key, (const char *)member.data,
        index_size, member_size, max_records, max_bytes};
    const size_t lengths[] = {4u,
                              sizeof(script) - 1u,
                              1u,
                              set_key_size,
                              store->meta_key_size,
                              member.size,
                              strlen(index_size),
                              strlen(member_size),
                              strlen(max_records),
                              strlen(max_bytes)};
    rc = flow_redis_index_command(store, 10, argv, lengths, flow_redis_integer_apply, &reply);
  }
  free(set_key);
  if (rc != TURBO_OK) return rc;
  if (reply.value == 0) return TURBO_EALREADY;
  if (reply.value == -1) {
    store->stats.rejects++;
    return TURBO_ENOSPC;
  }
  if (reply.value != 1) return TURBO_EPROTO;
  store->stats.writes++;
  return TURBO_OK;
}

static int flow_redis_index_remove(void *ctx, turbo_flow_store_bytes_t index,
                                   turbo_flow_store_bytes_t member) {
  static const char script[] = "if redis.call('SISMEMBER',KEYS[1],ARGV[1])==0 then return 0 end; "
                               "local r=tonumber(redis.call('HGET',KEYS[2],'records') or '-1'); "
                               "local b=tonumber(redis.call('HGET',KEYS[2],'bytes') or '-1'); "
                               "if r<1 or b<0 then return -1 end; local d=tonumber(ARGV[3]); "
                               "if redis.call('SCARD',KEYS[1])==1 then d=d+tonumber(ARGV[2]) end; "
                               "if b<d then return -1 end; redis.call('SREM',KEYS[1],ARGV[1]); "
                               "redis.call('HSET',KEYS[2],'records',r-1,'bytes',b-d); "
                               "if r==1 then redis.call('DEL',KEYS[2]) end; return 1";
  flow_redis_index_store_t *store = (flow_redis_index_store_t *)ctx;
  flow_redis_integer_reply_t reply = {0};
  char index_size[32];
  char member_size[32];
  char *set_key = NULL;
  size_t set_key_size = 0u;
  int rc;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  rc = flow_redis_index_member_validate(store, index, member);
  if (rc != TURBO_OK) return rc;
  rc = flow_redis_index_key(store, index, &set_key, &set_key_size);
  if (rc != TURBO_OK) return rc;
  (void)snprintf(index_size, sizeof(index_size), "%zu", index.size);
  (void)snprintf(member_size, sizeof(member_size), "%zu", member.size);
  {
    const char *argv[] = {"EVAL",     script,          "2",
                          set_key,    store->meta_key, (const char *)member.data,
                          index_size, member_size};
    const size_t lengths[] = {4u,
                              sizeof(script) - 1u,
                              1u,
                              set_key_size,
                              store->meta_key_size,
                              member.size,
                              strlen(index_size),
                              strlen(member_size)};
    rc = flow_redis_index_command(store, 8, argv, lengths, flow_redis_integer_apply, &reply);
  }
  free(set_key);
  if (rc != TURBO_OK) return rc;
  if (reply.value == 0) return TURBO_ENOENT;
  if (reply.value == -1) return TURBO_EPROTO;
  if (reply.value != 1) return TURBO_EPROTO;
  store->stats.writes++;
  return TURBO_OK;
}

static int flow_redis_index_contains(void *ctx, turbo_flow_store_bytes_t index,
                                     turbo_flow_store_bytes_t member, int *out) {
  flow_redis_index_store_t *store = (flow_redis_index_store_t *)ctx;
  flow_redis_integer_reply_t reply = {0};
  char *set_key = NULL;
  size_t set_key_size = 0u;
  int rc;
  if (!store || !out) return TURBO_EINVAL;
  rc = flow_redis_index_member_validate(store, index, member);
  if (rc != TURBO_OK) return rc;
  rc = flow_redis_index_key(store, index, &set_key, &set_key_size);
  if (rc != TURBO_OK) return rc;
  {
    const char *argv[] = {"SISMEMBER", set_key, (const char *)member.data};
    const size_t lengths[] = {9u, set_key_size, member.size};
    rc = flow_redis_index_command(store, 3, argv, lengths, flow_redis_integer_apply, &reply);
  }
  free(set_key);
  store->stats.queries++;
  if (rc != TURBO_OK) return rc;
  if (reply.value != 0 && reply.value != 1) return TURBO_EPROTO;
  *out = reply.value == 1;
  return TURBO_OK;
}

static int flow_redis_index_count(void *ctx, turbo_flow_store_bytes_t index, size_t *out) {
  flow_redis_index_store_t *store = (flow_redis_index_store_t *)ctx;
  flow_redis_integer_reply_t reply = {0};
  char *set_key = NULL;
  size_t set_key_size = 0u;
  int rc;
  if (!store || !out) return TURBO_EINVAL;
  rc = flow_redis_index_key(store, index, &set_key, &set_key_size);
  if (rc != TURBO_OK) return rc;
  {
    const char *argv[] = {"SCARD", set_key};
    const size_t lengths[] = {5u, set_key_size};
    rc = flow_redis_index_command(store, 2, argv, lengths, flow_redis_integer_apply, &reply);
  }
  free(set_key);
  store->stats.queries++;
  if (rc != TURBO_OK) return rc;
  if (reply.value < 0 || (uint64_t)reply.value > (uint64_t)SIZE_MAX) return TURBO_EPROTO;
  *out = (size_t)reply.value;
  return TURBO_OK;
}

static int flow_redis_index_visit(void *ctx, turbo_flow_store_bytes_t index,
                                  turbo_flow_index_visit_fn visit, void *visit_ctx) {
  flow_redis_index_store_t *store = (flow_redis_index_store_t *)ctx;
  flow_redis_members_reply_t reply;
  char *set_key = NULL;
  size_t set_key_size = 0u;
  int rc;
  if (!store || !visit) return TURBO_EINVAL;
  rc = flow_redis_index_key(store, index, &set_key, &set_key_size);
  if (rc != TURBO_OK) return rc;
  memset(&reply, 0, sizeof(reply));
  reply.visit = visit;
  reply.ctx = visit_ctx;
  reply.max_records = store->limits.max_records;
  reply.max_member_size = store->max_member_size;
  {
    const char *argv[] = {"SMEMBERS", set_key};
    const size_t lengths[] = {8u, set_key_size};
    rc = flow_redis_index_command(store, 2, argv, lengths, flow_redis_members_apply, &reply);
  }
  free(set_key);
  store->stats.queries++;
  if (rc != TURBO_OK) return rc;
  return reply.count == 0u ? TURBO_ENOENT : TURBO_OK;
}

static int flow_redis_index_intersection_count(void *ctx, const turbo_flow_store_bytes_t *indices,
                                               size_t index_count, size_t *out) {
  flow_redis_index_store_t *store = (flow_redis_index_store_t *)ctx;
  flow_redis_members_reply_t reply;
  const char **argv;
  size_t *lengths;
  int rc = TURBO_OK;
  if (!store || !indices || index_count == 0u || !out || index_count > INT_MAX - 1)
    return TURBO_EINVAL;
  argv = (const char **)calloc(index_count + 1u, sizeof(*argv));
  lengths = (size_t *)calloc(index_count + 1u, sizeof(*lengths));
  if (!argv || !lengths) {
    free(lengths);
    free(argv);
    return TURBO_ENOMEM;
  }
  argv[0] = "SINTER";
  lengths[0] = 6u;
  for (size_t i = 0u; i < index_count; ++i) {
    char *key = NULL;
    rc = flow_redis_index_key(store, indices[i], &key, &lengths[i + 1u]);
    if (rc != TURBO_OK) break;
    argv[i + 1u] = key;
  }
  memset(&reply, 0, sizeof(reply));
  reply.max_records = store->limits.max_records;
  reply.max_member_size = store->max_member_size;
  if (rc == TURBO_OK)
    rc = flow_redis_index_command(store, (int)index_count + 1, argv, lengths,
                                  flow_redis_members_apply, &reply);
  for (size_t i = 0u; i < index_count; ++i)
    free((void *)argv[i + 1u]);
  free(lengths);
  free(argv);
  store->stats.queries++;
  if (rc != TURBO_OK) return rc;
  *out = reply.count;
  return TURBO_OK;
}

static int flow_redis_index_stats(const void *ctx, turbo_flow_store_stats_t *out) {
  flow_redis_index_store_t *store = (flow_redis_index_store_t *)ctx;
  flow_redis_index_stats_reply_t reply = {0};
  size_t output_size;
  int rc;
  if (!store || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_STORE_ABI_VERSION) {
    return TURBO_EINVAL;
  }
  {
    const char *argv[] = {"HGETALL", store->meta_key};
    const size_t lengths[] = {7u, store->meta_key_size};
    rc = flow_redis_index_command(store, 2, argv, lengths, flow_redis_index_stats_apply, &reply);
  }
  if (rc != TURBO_OK) return rc;
  store->stats.records = reply.records;
  store->stats.bytes = reply.bytes;
  if (reply.records > store->stats.peak_records) store->stats.peak_records = reply.records;
  if (reply.bytes > store->stats.peak_bytes) store->stats.peak_bytes = reply.bytes;
  output_size = out->size;
  *out = store->stats;
  out->size = output_size;
  return TURBO_OK;
}

static const turbo_flow_index_store_provider_ops_t FLOW_REDIS_INDEX_OPS = {
    sizeof(turbo_flow_index_store_provider_ops_t),
    TURBO_FLOW_INDEX_STORE_PROVIDER_API_VERSION,
    flow_redis_index_close,
    flow_redis_index_destroy,
    flow_redis_index_add,
    flow_redis_index_remove,
    flow_redis_index_contains,
    flow_redis_index_count,
    flow_redis_index_visit,
    flow_redis_index_intersection_count,
    flow_redis_index_stats};

int flow_redis_index_store_create(const turbo_flow_redis_index_store_config_t *config,
                                  const turbo_flow_store_limits_t *limits,
                                  turbo_flow_index_store_t **out) {
  flow_redis_store_client_config_t client_config;
  flow_redis_index_store_t *store;
  const size_t max_index_name_size = config && config->max_index_name_size
                                         ? config->max_index_name_size
                                         : TURBO_FLOW_REDIS_INDEX_DEFAULT_MAX_NAME_SIZE;
  const size_t max_member_size = config && config->max_member_size
                                     ? config->max_member_size
                                     : TURBO_FLOW_REDIS_INDEX_DEFAULT_MAX_MEMBER_SIZE;
  size_t namespace_size;
  int rc;
  if (!config || !limits || !out || !config->key || !config->key[0]) return TURBO_EINVAL;
  *out = NULL;
  rc = turbo_flow_store_limits_validate(limits, 0);
  if (rc != TURBO_OK) return rc;
  namespace_size = strlen(config->key);
  if (namespace_size > (TURBO_FLOW_REDIS_MAX_KEY_SIZE - 8u) / 2u ||
      max_index_name_size > (TURBO_FLOW_REDIS_MAX_KEY_SIZE - (namespace_size * 2u + 2u) -
                             strlen(FLOW_REDIS_INDEX_KEY_SUFFIX)) /
                                2u ||
      max_member_size > TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE ||
      limits->max_records > FLOW_REDIS_LUA_MAX_EXACT_INTEGER ||
      limits->max_bytes > FLOW_REDIS_LUA_MAX_EXACT_INTEGER) {
    return TURBO_EINVAL;
  }
  store = (flow_redis_index_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->limits = *limits;
  store->stats = (turbo_flow_store_stats_t)TURBO_FLOW_STORE_STATS_INIT;
  store->max_index_name_size = max_index_name_size;
  store->max_member_size = max_member_size;
  rc =
      flow_redis_namespace_prefix(config->key, namespace_size, &store->prefix, &store->prefix_size);
  if (rc == TURBO_OK)
    rc = flow_redis_hex_key(store->prefix, store->prefix_size, FLOW_REDIS_INDEX_META_SUFFIX, NULL,
                            0u, &store->meta_key, &store->meta_key_size);
  memset(&client_config, 0, sizeof(client_config));
  client_config.host = config->host;
  client_config.port = config->port;
  client_config.username = config->username;
  client_config.password = config->password;
  client_config.database = config->database;
  client_config.timeout_ms = config->timeout_ms;
  if (rc == TURBO_OK) rc = flow_redis_store_client_create(&client_config, &store->client);
  if (rc == TURBO_OK)
    rc = turbo_flow_index_store_create_provider(&FLOW_REDIS_INDEX_OPS, store, out);
  if (rc != TURBO_OK) flow_redis_index_destroy(store);
  return rc;
}
