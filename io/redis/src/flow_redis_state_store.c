#include "flow_redis_storage_internal.h"
#include "turbo_flow_store_redis.h"

#include "flow_redis_internal.h"
#include "turbo_flow_state_store_provider.h"
#include "turbo_thread.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_REDIS_STATE_LUA_MAX_EXACT_INTEGER UINT64_C(9007199254740991)

typedef struct flow_redis_state_store_s {
  turbo_flow_record_store_t records;
  turbo_flow_store_limits_t limits;
  turbo_flow_store_stats_t stats;
  char *record_key;
  char *meta_key;
  size_t record_key_size;
  size_t meta_key_size;
  turbo_mutex_t mutex;
  int mutex_initialized;
  int closed;
} flow_redis_state_store_t;

typedef struct flow_redis_state_mutation_reply_s {
  int64_t status;
  uint64_t revision;
  size_t records;
  size_t bytes;
} flow_redis_state_mutation_reply_t;

typedef struct flow_redis_state_scan_s {
  turbo_flow_store_bytes_t target;
  turbo_flow_state_record_t *record;
  turbo_flow_state_visit_fn visit;
  void *visit_ctx;
  size_t records;
  size_t bytes;
  size_t found_value_size;
  uint64_t found_revision;
  int found;
} flow_redis_state_scan_t;

static int flow_redis_state_size_add(size_t left, size_t right, size_t *out) {
  if (!out || left > SIZE_MAX - right) return TURBO_EFBIG;
  *out = left + right;
  return TURBO_OK;
}

static int flow_redis_state_buffer_copy(const uint8_t *data, size_t size, mem_buffer_t **out) {
  mem_buffer_t *buffer;
  if (!out || (size > 0u && !data)) return TURBO_EINVAL;
  *out = NULL;
  if (size == 0u) return TURBO_OK;
  buffer = mem_get_buffer(mem_global(), size);
  if (!buffer) return TURBO_ENOMEM;
  memcpy(mem_buffer_data(buffer), data, size);
  mem_set_used(buffer, size);
  *out = buffer;
  return TURBO_OK;
}

static int flow_redis_state_reply_u64(const redis_reply_t *reply, uint64_t *out) {
  uint64_t value = 0u;
  if (!reply || !out) return TURBO_EPROTO;
  if (reply->type == REDIS_REPLY_INTEGER) {
    if (reply->integer < 0) return TURBO_EPROTO;
    *out = (uint64_t)reply->integer;
    return TURBO_OK;
  }
  if (reply->type != REDIS_REPLY_BULK_STRING || !reply->str || reply->len == 0u)
    return TURBO_EPROTO;
  for (size_t index = 0u; index < reply->len; ++index) {
    const unsigned int digit = (unsigned int)(reply->str[index] - '0');
    if (reply->str[index] < '0' || reply->str[index] > '9' || value > (UINT64_MAX - digit) / 10u)
      return TURBO_EPROTO;
    value = value * 10u + digit;
  }
  *out = value;
  return TURBO_OK;
}

static int flow_redis_state_mutation_apply(void *ctx, const redis_reply_t *reply) {
  flow_redis_state_mutation_reply_t *result = (flow_redis_state_mutation_reply_t *)ctx;
  uint64_t records = 0u;
  uint64_t bytes = 0u;
  if (!result || !reply || reply->type != REDIS_REPLY_ARRAY || reply->element_count != 4u ||
      !reply->elements[0] || reply->elements[0]->type != REDIS_REPLY_INTEGER)
    return TURBO_EPROTO;
  result->status = reply->elements[0]->integer;
  if (result->status <= 0) return TURBO_OK;
  if (flow_redis_state_reply_u64(reply->elements[1], &result->revision) != TURBO_OK ||
      flow_redis_state_reply_u64(reply->elements[2], &records) != TURBO_OK ||
      flow_redis_state_reply_u64(reply->elements[3], &bytes) != TURBO_OK || records > SIZE_MAX ||
      bytes > SIZE_MAX)
    return TURBO_EPROTO;
  result->records = (size_t)records;
  result->bytes = (size_t)bytes;
  return TURBO_OK;
}

static int flow_redis_state_has_hash_tag(const char *key) {
  const char *open;
  const char *close;
  if (!key) return 0;
  open = strchr(key, '{');
  close = open ? strchr(open + 1, '}') : NULL;
  return open && close && close > open + 1;
}

static int flow_redis_state_meta_key(const char *key, char **out, size_t *out_size) {
  static const char suffix[] = ":state-meta";
  const size_t key_size = key ? strlen(key) : 0u;
  const size_t suffix_size = sizeof(suffix) - 1u;
  char *meta;
  if (!key || key_size == 0u || !out || !out_size ||
      key_size > TURBO_FLOW_REDIS_MAX_KEY_SIZE - suffix_size)
    return TURBO_EINVAL;
  meta = (char *)malloc(key_size + suffix_size + 1u);
  if (!meta) return TURBO_ENOMEM;
  memcpy(meta, key, key_size);
  memcpy(meta + key_size, suffix, suffix_size + 1u);
  *out = meta;
  *out_size = key_size + suffix_size;
  return TURBO_OK;
}

static int flow_redis_state_command(flow_redis_state_store_t *store, int argc, const char **argv,
                                    const size_t *lengths, flow_redis_store_reply_fn apply,
                                    void *ctx) {
  int rc = flow_redis_store_command((flow_redis_store_client_t *)store->records.ctx, argc, argv,
                                    lengths, apply, ctx);
  if (rc != TURBO_OK) store->stats.backend_errors++;
  return rc;
}

static int flow_redis_state_scan_record(void *ctx, const turbo_flow_record_view_t *record) {
  flow_redis_state_scan_t *scan = (flow_redis_state_scan_t *)ctx;
  size_t item_bytes;
  int rc;
  if (!scan || !record || record->size < sizeof(*record)) return TURBO_EPROTO;
  rc = flow_redis_state_size_add(record->key_size, record->value_size, &item_bytes);
  if (rc == TURBO_OK) rc = flow_redis_state_size_add(scan->bytes, item_bytes, &scan->bytes);
  if (rc != TURBO_OK) return rc;
  scan->records++;
  if (scan->visit) {
    turbo_flow_store_bytes_t key = {record->key, record->key_size};
    turbo_flow_store_bytes_t value = {record->value, record->value_size};
    rc = scan->visit(scan->visit_ctx, key, value, record->revision);
    if (rc != TURBO_OK) return rc;
  }
  if (scan->target.size != record->key_size ||
      memcmp(scan->target.data, record->key, record->key_size) != 0) {
    return TURBO_OK;
  }
  scan->found = 1;
  scan->found_value_size = record->value_size;
  scan->found_revision = record->revision;
  if (scan->record) {
    const size_t output_size = scan->record->size;
    turbo_flow_state_record_cleanup(scan->record);
    *scan->record = (turbo_flow_state_record_t)TURBO_FLOW_STATE_RECORD_INIT;
    scan->record->size = output_size;
    scan->record->revision = record->revision;
    rc = flow_redis_state_buffer_copy(record->value, record->value_size, &scan->record->value);
  }
  return rc;
}

static int flow_redis_state_scan(flow_redis_state_store_t *store, turbo_flow_store_bytes_t target,
                                 turbo_flow_state_record_t *record, turbo_flow_state_visit_fn visit,
                                 void *visit_ctx, flow_redis_state_scan_t *out) {
  flow_redis_state_scan_t scan;
  int rc;
  if (!store || !out) return TURBO_EINVAL;
  memset(&scan, 0, sizeof(scan));
  scan.target = target;
  scan.record = record;
  scan.visit = visit;
  scan.visit_ctx = visit_ctx;
  rc = store->records.scan(store->records.ctx, flow_redis_state_scan_record, &scan);
  if (rc != TURBO_OK) {
    store->stats.backend_errors++;
    return rc;
  }
  *out = scan;
  return TURBO_OK;
}

static int flow_redis_state_key_validate(const flow_redis_state_store_t *store,
                                         turbo_flow_store_bytes_t key) {
  if (!store || !key.data || key.size == 0u) return TURBO_EINVAL;
  return key.size <= store->records.max_key_size ? TURBO_OK : TURBO_EFBIG;
}

static int flow_redis_state_close(void *ctx) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  int rc = TURBO_OK;
  if (!store) return TURBO_EINVAL;
  turbo_mutex_lock(&store->mutex);
  if (store->closed)
    rc = TURBO_EALREADY;
  else
    store->closed = 1;
  turbo_mutex_unlock(&store->mutex);
  return rc;
}

static void flow_redis_state_destroy(void *ctx) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  if (!store) return;
  flow_redis_record_store_destroy(&store->records);
  if (store->mutex_initialized) turbo_mutex_destroy(&store->mutex);
  free(store->meta_key);
  free(store->record_key);
  free(store);
}

static int flow_redis_state_mutate(flow_redis_state_store_t *store, int remove,
                                   turbo_flow_store_bytes_t key, turbo_flow_store_bytes_t value,
                                   uint64_t expected_revision, uint64_t *new_revision) {
  static const char script[] =
      "local z=string.char(0); local mr=tonumber(ARGV[6]); local mb=tonumber(ARGV[7]); "
      "local mi=ARGV[8]; local schema=redis.call('HGET',KEYS[2],'schema'); "
      "if schema then if schema~='1' or redis.call('HGET',KEYS[2],'max_records')~=ARGV[6] or "
      "redis.call('HGET',KEYS[2],'max_bytes')~=ARGV[7] or "
      "redis.call('HGET',KEYS[2],'max_item_bytes')~=mi then return {-5,0,0,0} end "
      "else local n=redis.call('HLEN',KEYS[1]); if n>mr then return {-2,0,0,0} end; "
      "local all=redis.call('HGETALL',KEYS[1]); local b=0; "
      "for i=1,#all,2 do local s=string.find(all[i+1],z,1,true); "
      "if not s or s==1 or not string.match(string.sub(all[i+1],1,s-1),'^%d+$') "
      "then return {-3,0,0,0} end; b=b+string.len(all[i])+string.len(all[i+1])-s end; "
      "if b>mb then return {-2,0,0,0} end; redis.call('HSET',KEYS[2],'schema','1',"
      "'max_records',ARGV[6],'max_bytes',ARGV[7],'max_item_bytes',mi,'records',n,'bytes',b) end; "
      "local r=tonumber(redis.call('HGET',KEYS[2],'records') or '-1'); "
      "local b=tonumber(redis.call('HGET',KEYS[2],'bytes') or '-1'); "
      "if not r or not b or r<0 or b<0 then return {-3,0,0,0} end; "
      "local c=redis.call('HGET',KEYS[1],ARGV[2]); local old=0; "
      "if not c and ARGV[1]=='2' then return {0,0,r,b} end; "
      "if c then local s=string.find(c,z,1,true); if not s or s==1 then return {-3,0,0,0} end; "
      "if string.sub(c,1,s-1)~=ARGV[3] then return {-1,0,0,0} end; old=string.len(ARGV[2])+"
      "string.len(c)-s elseif ARGV[3]~='0' then return {-1,0,0,0} end; "
      "if ARGV[1]=='2' then "
      "redis.call('HDEL',KEYS[1],ARGV[2]); r=r-1; b=b-old; "
      "redis.call('HSET',KEYS[2],'records',r,'bytes',b); return {1,0,r,b} end; "
      "if c and ARGV[3]==ARGV[9] then return {-4,0,0,0} end; "
      "local nb=b-old+string.len(ARGV[2])+string.len(ARGV[5]); local nr=r; "
      "if not c then nr=nr+1 end; if nr>mr or nb>mb then return {-2,0,0,0} end; "
      "redis.call('HSET',KEYS[1],ARGV[2],ARGV[4]..z..ARGV[5]); "
      "redis.call('HSET',KEYS[2],'records',nr,'bytes',nb); return {1,ARGV[4],nr,nb}";
  flow_redis_state_mutation_reply_t reply;
  char expected[32];
  char next[32];
  char max_records[32];
  char max_bytes[32];
  char max_item_bytes[32];
  char max_revision[32];
  const char operation[] = {'1', '\0'};
  const char remove_operation[] = {'2', '\0'};
  const char *argv[] = {"EVAL",
                        script,
                        "2",
                        store->record_key,
                        store->meta_key,
                        remove ? remove_operation : operation,
                        (const char *)key.data,
                        expected,
                        next,
                        value.size ? (const char *)value.data : "",
                        max_records,
                        max_bytes,
                        max_item_bytes,
                        max_revision};
  size_t lengths[] = {4u,
                      sizeof(script) - 1u,
                      1u,
                      store->record_key_size,
                      store->meta_key_size,
                      1u,
                      key.size,
                      0u,
                      0u,
                      value.size,
                      0u,
                      0u,
                      0u,
                      0u};
  int rc;
  memset(&reply, 0, sizeof(reply));
  if (snprintf(expected, sizeof(expected), "%llu", (unsigned long long)expected_revision) <= 0 ||
      snprintf(next, sizeof(next), "%llu", (unsigned long long)(expected_revision + 1u)) <= 0 ||
      snprintf(max_records, sizeof(max_records), "%zu", store->limits.max_records) <= 0 ||
      snprintf(max_bytes, sizeof(max_bytes), "%zu", store->limits.max_bytes) <= 0 ||
      snprintf(max_item_bytes, sizeof(max_item_bytes), "%zu", store->limits.max_item_bytes) <= 0 ||
      snprintf(max_revision, sizeof(max_revision), "%llu",
               (unsigned long long)TURBO_FLOW_RECORD_REVISION_MAX) <= 0)
    return TURBO_ERANGE;
  for (size_t index = 7u; index < sizeof(lengths) / sizeof(lengths[0]); ++index)
    if (index != 9u) lengths[index] = strlen(argv[index]);
  rc = flow_redis_state_command(store, (int)(sizeof(argv) / sizeof(argv[0])), argv, lengths,
                                flow_redis_state_mutation_apply, &reply);
  if (rc != TURBO_OK) return rc;
  if (reply.status == -1 || reply.status == -5) {
    store->stats.conflicts++;
    return TURBO_EBUSY;
  }
  if (reply.status == -2) {
    store->stats.rejects++;
    return TURBO_ENOSPC;
  }
  if (reply.status == -3) return TURBO_EPROTO;
  if (reply.status == -4) return TURBO_ERANGE;
  if (reply.status == 0) return TURBO_ENOENT;
  if (reply.status != 1) return TURBO_EPROTO;
  store->stats.writes++;
  store->stats.records = reply.records;
  store->stats.bytes = reply.bytes;
  if (reply.records > store->stats.peak_records) store->stats.peak_records = reply.records;
  if (reply.bytes > store->stats.peak_bytes) store->stats.peak_bytes = reply.bytes;
  if (!remove && new_revision) *new_revision = reply.revision;
  return TURBO_OK;
}

static int flow_redis_state_put(void *ctx, turbo_flow_store_bytes_t key,
                                turbo_flow_store_bytes_t value, uint64_t expected_revision,
                                uint64_t *new_revision) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  size_t item_bytes;
  int rc;
  if (!store || !new_revision || (value.size > 0u && !value.data)) return TURBO_EINVAL;
  turbo_mutex_lock(&store->mutex);
  if (store->closed) {
    turbo_mutex_unlock(&store->mutex);
    return TURBO_ESHUTDOWN;
  }
  rc = flow_redis_state_key_validate(store, key);
  if (rc != TURBO_OK) goto done;
  if (value.size > store->records.max_value_size ||
      flow_redis_state_size_add(key.size, value.size, &item_bytes) != TURBO_OK ||
      item_bytes > store->limits.max_item_bytes) {
    store->stats.rejects++;
    rc = TURBO_EFBIG;
    goto done;
  }
  if (expected_revision == TURBO_FLOW_RECORD_REVISION_MAX)
    rc = TURBO_ERANGE;
  else
    rc = flow_redis_state_mutate(store, 0, key, value, expected_revision, new_revision);
done:
  turbo_mutex_unlock(&store->mutex);
  return rc;
}

static int flow_redis_state_get(void *ctx, turbo_flow_store_bytes_t key,
                                turbo_flow_state_record_t *out) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  flow_redis_state_scan_t lookup;
  int rc;
  if (!store || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_STORE_ABI_VERSION) {
    return TURBO_EINVAL;
  }
  turbo_mutex_lock(&store->mutex);
  if (store->closed) {
    turbo_mutex_unlock(&store->mutex);
    return TURBO_ESHUTDOWN;
  }
  rc = flow_redis_state_key_validate(store, key);
  if (rc != TURBO_OK) {
    turbo_mutex_unlock(&store->mutex);
    return rc;
  }
  memset(&lookup, 0, sizeof(lookup));
  lookup.target = key;
  lookup.record = out;
  rc = flow_redis_record_store_get(&store->records, key.data, key.size,
                                   flow_redis_state_scan_record, &lookup);
  store->stats.queries++;
  if (rc != TURBO_OK && rc != TURBO_ENOENT) store->stats.backend_errors++;
  turbo_mutex_unlock(&store->mutex);
  return rc;
}

static int flow_redis_state_remove(void *ctx, turbo_flow_store_bytes_t key,
                                   uint64_t expected_revision) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  int rc;
  if (!store || expected_revision == 0u) return TURBO_EINVAL;
  turbo_mutex_lock(&store->mutex);
  if (store->closed) {
    turbo_mutex_unlock(&store->mutex);
    return TURBO_ESHUTDOWN;
  }
  rc = flow_redis_state_key_validate(store, key);
  if (rc == TURBO_OK)
    rc = flow_redis_state_mutate(store, 1, key,
                                 (turbo_flow_store_bytes_t)TURBO_FLOW_STORE_BYTES_INIT,
                                 expected_revision, NULL);
  turbo_mutex_unlock(&store->mutex);
  return rc;
}

static int flow_redis_state_visit(void *ctx, turbo_flow_state_visit_fn visit, void *visit_ctx) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  flow_redis_state_scan_t scan;
  int rc;
  if (!store || !visit) return TURBO_EINVAL;
  turbo_mutex_lock(&store->mutex);
  if (store->closed) {
    turbo_mutex_unlock(&store->mutex);
    return TURBO_ESHUTDOWN;
  }
  rc = flow_redis_state_scan(store, (turbo_flow_store_bytes_t)TURBO_FLOW_STORE_BYTES_INIT, NULL,
                             visit, visit_ctx, &scan);
  store->stats.queries++;
  turbo_mutex_unlock(&store->mutex);
  return rc;
}

static int flow_redis_state_stats(const void *ctx, turbo_flow_store_stats_t *out) {
  flow_redis_state_store_t *store = (flow_redis_state_store_t *)ctx;
  flow_redis_state_scan_t scan;
  size_t output_size;
  int rc;
  if (!store || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_STORE_ABI_VERSION) {
    return TURBO_EINVAL;
  }
  turbo_mutex_lock(&store->mutex);
  if (store->closed) {
    turbo_mutex_unlock(&store->mutex);
    return TURBO_ESHUTDOWN;
  }
  rc = flow_redis_state_scan(store, (turbo_flow_store_bytes_t)TURBO_FLOW_STORE_BYTES_INIT, NULL,
                             NULL, NULL, &scan);
  if (rc != TURBO_OK) {
    turbo_mutex_unlock(&store->mutex);
    return rc;
  }
  store->stats.records = scan.records;
  store->stats.bytes = scan.bytes;
  if (scan.records > store->stats.peak_records) store->stats.peak_records = scan.records;
  if (scan.bytes > store->stats.peak_bytes) store->stats.peak_bytes = scan.bytes;
  output_size = out->size;
  *out = store->stats;
  out->size = output_size;
  turbo_mutex_unlock(&store->mutex);
  return TURBO_OK;
}

static const turbo_flow_state_store_provider_ops_t FLOW_REDIS_STATE_OPS = {
    sizeof(turbo_flow_state_store_provider_ops_t),
    TURBO_FLOW_STATE_STORE_PROVIDER_API_VERSION,
    flow_redis_state_close,
    flow_redis_state_destroy,
    flow_redis_state_put,
    flow_redis_state_get,
    flow_redis_state_remove,
    flow_redis_state_visit,
    flow_redis_state_stats};

int flow_redis_state_store_create(const turbo_flow_redis_record_store_config_t *config,
                                  const turbo_flow_store_limits_t *limits,
                                  turbo_flow_state_store_t **out) {
  flow_redis_state_store_t *store;
  size_t max_item_bytes;
  const size_t max_key_size = config && config->max_record_key_size
                                  ? config->max_record_key_size
                                  : TURBO_FLOW_REDIS_RECORD_STORE_MAX_RECORD_KEY_SIZE;
  const size_t max_value_size = config && config->max_value_size
                                    ? config->max_value_size
                                    : TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE;
  int rc;
  if (!config || !limits || !out || !config->key || !config->key[0]) return TURBO_EINVAL;
  *out = NULL;
  rc = turbo_flow_store_limits_validate(limits, 0);
  if (rc != TURBO_OK) return rc;
  if (flow_redis_state_size_add(max_key_size, max_value_size, &max_item_bytes) != TURBO_OK ||
      limits->max_records != config->max_records || limits->max_item_bytes > max_item_bytes ||
      limits->max_records > FLOW_REDIS_STATE_LUA_MAX_EXACT_INTEGER ||
      limits->max_bytes > FLOW_REDIS_STATE_LUA_MAX_EXACT_INTEGER ||
      limits->max_item_bytes > FLOW_REDIS_STATE_LUA_MAX_EXACT_INTEGER ||
      (config->connection.version == TURBO_FLOW_REDIS_CONNECTION_CONFIG_VERSION &&
       config->connection.deployment == TURBO_FLOW_REDIS_DEPLOYMENT_CLUSTER &&
       !flow_redis_state_has_hash_tag(config->key))) {
    return TURBO_EINVAL;
  }
  store = (flow_redis_state_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->records = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
  store->limits = *limits;
  store->stats = (turbo_flow_store_stats_t)TURBO_FLOW_STORE_STATS_INIT;
  turbo_mutex_init(&store->mutex);
  store->mutex_initialized = 1;
  store->record_key = (char *)malloc(strlen(config->key) + 1u);
  if (store->record_key) memcpy(store->record_key, config->key, strlen(config->key) + 1u);
  if (!store->record_key) rc = TURBO_ENOMEM;
  else {
    store->record_key_size = strlen(store->record_key);
    rc = flow_redis_state_meta_key(store->record_key, &store->meta_key, &store->meta_key_size);
  }
  if (rc == TURBO_OK) rc = flow_redis_record_store_create(config, &store->records);
  if (rc == TURBO_OK)
    rc = turbo_flow_state_store_create_provider(&FLOW_REDIS_STATE_OPS, store, out);
  if (rc != TURBO_OK) flow_redis_state_destroy(store);
  return rc;
}
