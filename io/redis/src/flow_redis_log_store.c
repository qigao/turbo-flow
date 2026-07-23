#include "flow_redis_storage_internal.h"
#include "turbo_flow_store_redis.h"

#include "flow_redis_internal.h"
#include "turbo_flow_log_store_provider.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_REDIS_LOG_MAX_EXACT_INTEGER UINT64_C(9007199254740991)
#define FLOW_REDIS_LOG_STREAM_SUFFIX ":log"
#define FLOW_REDIS_LOG_ORDER_SUFFIX ":order"
#define FLOW_REDIS_LOG_META_SUFFIX ":meta"

typedef struct flow_redis_log_store_s {
  flow_redis_store_client_t *client;
  turbo_flow_store_limits_t limits;
  turbo_flow_store_stats_t stats;
  char *stream_key;
  char *order_key;
  char *meta_key;
  size_t stream_key_size;
  size_t order_key_size;
  size_t meta_key_size;
  size_t max_operation_records;
  int closed;
} flow_redis_log_store_t;

typedef struct flow_redis_log_array_reply_s {
  int64_t values[5];
  size_t count;
} flow_redis_log_array_reply_t;

typedef struct flow_redis_log_read_reply_s {
  turbo_flow_log_record_t *records;
  size_t capacity;
  size_t count;
  int stale;
} flow_redis_log_read_reply_t;

static int flow_redis_log_u64_parse(const char *data, size_t size, uint64_t *out) {
  uint64_t value = 0u;
  if (!data || size == 0u || !out) return TURBO_EPROTO;
  for (size_t i = 0u; i < size; ++i) {
    unsigned int digit;
    if (data[i] < '0' || data[i] > '9') return TURBO_EPROTO;
    digit = (unsigned int)(data[i] - '0');
    if (value > (UINT64_MAX - digit) / 10u) return TURBO_EPROTO;
    value = value * 10u + digit;
  }
  *out = value;
  return TURBO_OK;
}

static int flow_redis_log_size_from_i64(int64_t value, size_t *out) {
  if (!out || value < 0 || (uint64_t)value > (uint64_t)SIZE_MAX) return TURBO_EPROTO;
  *out = (size_t)value;
  return TURBO_OK;
}

static int flow_redis_log_cursor_parse(const redis_reply_t *reply, uint64_t *out) {
  const char *separator;
  uint64_t sequence;
  int rc;
  if (!reply || reply->type != REDIS_REPLY_BULK_STRING || !reply->str || reply->len < 3u)
    return TURBO_EPROTO;
  separator = (const char *)memchr(reply->str, '-', reply->len);
  if (!separator || separator == reply->str) return TURBO_EPROTO;
  rc = flow_redis_log_u64_parse(reply->str, (size_t)(separator - reply->str), out);
  if (rc != TURBO_OK) return rc;
  rc = flow_redis_log_u64_parse(separator + 1u, reply->len - (size_t)(separator - reply->str) - 1u,
                                &sequence);
  return rc == TURBO_OK && sequence == 0u ? TURBO_OK : TURBO_EPROTO;
}

static int flow_redis_log_buffer_copy(const redis_reply_t *reply, mem_buffer_t **out) {
  mem_buffer_t *buffer;
  if (!reply || reply->type != REDIS_REPLY_BULK_STRING || (!reply->str && reply->len > 0u) || !out)
    return TURBO_EPROTO;
  *out = NULL;
  if (reply->len == 0u) return TURBO_OK;
  buffer = mem_get_buffer(mem_global(), reply->len);
  if (!buffer) return TURBO_ENOMEM;
  memcpy(mem_buffer_data(buffer), reply->str, reply->len);
  mem_set_used(buffer, reply->len);
  *out = buffer;
  return TURBO_OK;
}

static int flow_redis_log_entry_parse(const redis_reply_t *entry, turbo_flow_log_record_t *out) {
  const redis_reply_t *fields;
  uint64_t encoded_size = 0u;
  size_t output_size;
  int rc;
  if (!entry || entry->type != REDIS_REPLY_ARRAY || entry->element_count != 2u || !out ||
      out->size < sizeof(*out) || out->abi_version != TURBO_FLOW_STORE_ABI_VERSION)
    return TURBO_EPROTO;
  fields = entry->elements[1];
  if (!fields || fields->type != REDIS_REPLY_ARRAY || fields->element_count != 6u ||
      !fields->elements[0] || fields->elements[0]->type != REDIS_REPLY_BULK_STRING ||
      fields->elements[0]->len != 1u || memcmp(fields->elements[0]->str, "t", 1u) != 0 ||
      !fields->elements[1] || fields->elements[1]->type != REDIS_REPLY_BULK_STRING ||
      !fields->elements[2] || fields->elements[2]->type != REDIS_REPLY_BULK_STRING ||
      fields->elements[2]->len != 1u || memcmp(fields->elements[2]->str, "p", 1u) != 0 ||
      !fields->elements[3] || fields->elements[3]->type != REDIS_REPLY_BULK_STRING ||
      !fields->elements[4] || fields->elements[4]->type != REDIS_REPLY_BULK_STRING ||
      fields->elements[4]->len != 1u || memcmp(fields->elements[4]->str, "n", 1u) != 0 ||
      !fields->elements[5] || fields->elements[5]->type != REDIS_REPLY_BULK_STRING)
    return TURBO_EPROTO;
  output_size = out->size;
  turbo_flow_log_record_cleanup(out);
  *out = (turbo_flow_log_record_t)TURBO_FLOW_LOG_RECORD_INIT;
  out->size = output_size;
  rc = flow_redis_log_cursor_parse(entry->elements[0], &out->cursor);
  if (rc == TURBO_OK)
    rc = flow_redis_log_u64_parse(fields->elements[1]->str, fields->elements[1]->len,
                                  &out->timestamp_ms);
  if (rc == TURBO_OK)
    rc =
        flow_redis_log_u64_parse(fields->elements[5]->str, fields->elements[5]->len, &encoded_size);
  if (rc == TURBO_OK && encoded_size != fields->elements[3]->len) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) rc = flow_redis_log_buffer_copy(fields->elements[3], &out->payload);
  if (rc != TURBO_OK) turbo_flow_log_record_cleanup(out);
  return rc;
}

static int flow_redis_log_array_apply(void *ctx, const redis_reply_t *reply) {
  flow_redis_log_array_reply_t *result = (flow_redis_log_array_reply_t *)ctx;
  if (!result || !reply || reply->type != REDIS_REPLY_ARRAY ||
      reply->element_count > sizeof(result->values) / sizeof(result->values[0]))
    return TURBO_EPROTO;
  for (size_t i = 0u; i < reply->element_count; ++i) {
    if (!reply->elements[i] || reply->elements[i]->type != REDIS_REPLY_INTEGER) return TURBO_EPROTO;
    result->values[i] = reply->elements[i]->integer;
  }
  result->count = reply->element_count;
  return TURBO_OK;
}

static int flow_redis_log_read_apply(void *ctx, const redis_reply_t *reply) {
  flow_redis_log_read_reply_t *result = (flow_redis_log_read_reply_t *)ctx;
  const redis_reply_t *entries;
  if (!result || !reply || reply->type != REDIS_REPLY_ARRAY || reply->element_count != 2u ||
      !reply->elements[0] || reply->elements[0]->type != REDIS_REPLY_INTEGER)
    return TURBO_EPROTO;
  if (reply->elements[0]->integer == 1) {
    result->stale = 1;
    return TURBO_OK;
  }
  if (reply->elements[0]->integer != 0) return TURBO_EPROTO;
  entries = reply->elements[1];
  if (!entries || entries->type != REDIS_REPLY_ARRAY || entries->element_count > result->capacity)
    return TURBO_EPROTO;
  for (size_t i = 0u; i < entries->element_count; ++i) {
    int rc = flow_redis_log_entry_parse(entries->elements[i], &result->records[i]);
    if (rc != TURBO_OK) {
      for (size_t j = 0u; j < i; ++j)
        turbo_flow_log_record_cleanup(&result->records[j]);
      return rc;
    }
  }
  result->count = entries->element_count;
  return TURBO_OK;
}

static int flow_redis_log_prefix_create(const char *name, char **out, size_t *out_size) {
  static const char hex[] = "0123456789abcdef";
  const size_t name_size = name ? strlen(name) : 0u;
  size_t required;
  char *prefix;
  if (!name || name_size == 0u || !out || !out_size || name_size > (SIZE_MAX - 2u) / 2u)
    return TURBO_EINVAL;
  required = name_size * 2u + 2u;
  if (required + strlen(FLOW_REDIS_LOG_ORDER_SUFFIX) > TURBO_FLOW_REDIS_MAX_KEY_SIZE)
    return TURBO_EFBIG;
  prefix = (char *)malloc(required + 1u);
  if (!prefix) return TURBO_ENOMEM;
  prefix[0] = '{';
  for (size_t i = 0u; i < name_size; ++i) {
    const uint8_t byte = (uint8_t)name[i];
    prefix[1u + i * 2u] = hex[byte >> 4u];
    prefix[1u + i * 2u + 1u] = hex[byte & 0x0fu];
  }
  prefix[required - 1u] = '}';
  prefix[required] = '\0';
  *out = prefix;
  *out_size = required;
  return TURBO_OK;
}

static int flow_redis_log_key_create(const char *prefix, size_t prefix_size, const char *suffix,
                                     char **out, size_t *out_size) {
  const size_t suffix_size = suffix ? strlen(suffix) : 0u;
  char *key;
  if (!prefix || !suffix || !out || !out_size || prefix_size > SIZE_MAX - suffix_size ||
      prefix_size + suffix_size > TURBO_FLOW_REDIS_MAX_KEY_SIZE)
    return TURBO_EINVAL;
  key = (char *)malloc(prefix_size + suffix_size + 1u);
  if (!key) return TURBO_ENOMEM;
  memcpy(key, prefix, prefix_size);
  memcpy(key + prefix_size, suffix, suffix_size + 1u);
  *out = key;
  *out_size = prefix_size + suffix_size;
  return TURBO_OK;
}

static int flow_redis_log_command(flow_redis_log_store_t *store, int argc, const char **argv,
                                  const size_t *lengths, flow_redis_store_reply_fn apply,
                                  void *ctx) {
  int rc = flow_redis_store_command(store->client, argc, argv, lengths, apply, ctx);
  if (rc != TURBO_OK) store->stats.backend_errors++;
  return rc;
}

static int flow_redis_log_close(void *ctx) {
  flow_redis_log_store_t *store = (flow_redis_log_store_t *)ctx;
  if (!store) return TURBO_EINVAL;
  if (store->closed) return TURBO_EALREADY;
  store->closed = 1;
  return TURBO_OK;
}

static void flow_redis_log_destroy(void *ctx) {
  flow_redis_log_store_t *store = (flow_redis_log_store_t *)ctx;
  if (!store) return;
  flow_redis_store_client_destroy(store->client);
  free(store->meta_key);
  free(store->order_key);
  free(store->stream_key);
  free(store);
}

/**
 * Capacity planning is O(n) only when retention or full-policy trimming is active; n is bounded by
 * max_records. The mutation phase is atomic and only deletes the planned prefix.
 */
static int flow_redis_log_append(void *ctx, uint64_t timestamp_ms, turbo_flow_store_bytes_t payload,
                                 uint64_t *cursor) {
  static const char script[] =
      "local st=redis.call('TYPE',KEYS[1]).ok; local ot=redis.call('TYPE',KEYS[2]).ok; "
      "local mt=redis.call('TYPE',KEYS[3]).ok; "
      "if (st~='none' and st~='stream') or (ot~='none' and ot~='zset') or "
      "(mt~='none' and mt~='hash') then return {-4} end; "
      "local schema=redis.call('HGET',KEYS[3],'schema'); "
      "if schema then if schema~='1' or redis.call('HGET',KEYS[3],'max_records')~=ARGV[3] or "
      "redis.call('HGET',KEYS[3],'max_bytes')~=ARGV[4] or "
      "redis.call('HGET',KEYS[3],'max_item_bytes')~=ARGV[9] or "
      "redis.call('HGET',KEYS[3],'retention_ms')~=ARGV[10] or "
      "redis.call('HGET',KEYS[3],'full_policy')~=ARGV[7] then return {-5} end "
      "elseif st~='none' or ot~='none' or mt~='none' then return {-4} end; "
      "local r=tonumber(redis.call('HGET',KEYS[3],'records') or '0'); "
      "local b=tonumber(redis.call('HGET',KEYS[3],'bytes') or '0'); "
      "local nx=tonumber(redis.call('HGET',KEYS[3],'next') or '1'); "
      "local lt=tonumber(redis.call('HGET',KEYS[3],'last_ts') or '0'); "
      "local ts=tonumber(ARGV[1]); local sz=tonumber(ARGV[2]); "
      "if not r or not b or not nx or not lt or r<0 or b<0 or nx<1 or ts<lt or "
      "redis.call('ZCARD',KEYS[2])~=r or redis.call('XLEN',KEYS[1])~=r then "
      "if ts<lt then return {-2} end; return {-4} end; "
      "local last=0; if st=='stream' then local info=redis.call('XINFO','STREAM',KEYS[1]); "
      "local id=nil; for i=1,#info,2 do if info[i]=='last-generated-id' then id=info[i+1] end end; "
      "local c=nil; local s=nil; if id then c,s=string.match(id,'^(%d+)%-(%d+)$') end; "
      "if not c or tonumber(s)~=0 then return {-4} end; last=tonumber(c) end; "
      "if last~=nx-1 then return {-4} end; "
      "local trim={}; local tb=0; "
      "local floor=tonumber(ARGV[6]); local pol=ARGV[7]; "
      "local mr=tonumber(ARGV[3]); local mb=tonumber(ARGV[4]); "
      "if floor>0 or r+1>mr or sz>mb-b then "
      "local all=redis.call('ZRANGE',KEYS[2],0,-1); "
      "for _,m in ipairs(all) do local c,t,n=string.match(m,'^(%d+):(%d+):(%d+)$'); "
      "if not c then return {-4} end; local expired=floor>0 and tonumber(t)<floor; "
      "local full=(r-#trim)+1>mr or sz>mb-(b-tb); "
      "if expired or (full and pol=='2') then table.insert(trim,m); tb=tb+tonumber(n) "
      "else break end end end; "
      "if r-#trim+1>mr or sz>mb-(b-tb) then return {-1} end; "
      "if nx>tonumber(ARGV[8]) then return {-3} end; "
      "for _,m in ipairs(trim) do local c=string.match(m,'^(%d+):'); "
      "redis.call('XDEL',KEYS[1],c..'-0'); redis.call('ZREM',KEYS[2],m) end; "
      "local ns=string.format('%.0f',nx); "
      "redis.call('XADD',KEYS[1],ns..'-0','t',ARGV[1],'p',ARGV[5],'n',ARGV[2]); "
      "redis.call('ZADD',KEYS[2],nx,ns..':'..ARGV[1]..':'..ARGV[2]); "
      "r=r-#trim+1; b=b-tb+sz; local first=redis.call('ZRANGE',KEYS[2],0,0,'WITHSCORES'); "
      "local head=tonumber(first[2]); redis.call('HSET',KEYS[3],'schema','1',"
      "'max_records',ARGV[3],'max_bytes',ARGV[4],'max_item_bytes',ARGV[9],"
      "'retention_ms',ARGV[10],'full_policy',ARGV[7],'next',nx+1,'last_ts',ts,"
      "'records',r,'bytes',b,'head',head,'tail',nx); return {nx,r,b,#trim}";
  flow_redis_log_store_t *store = (flow_redis_log_store_t *)ctx;
  flow_redis_log_array_reply_t reply = {{0}, 0u};
  char timestamp[32], payload_size[32], max_records[32], max_bytes[32], floor_text[32];
  char policy[2], max_cursor[32], max_item_bytes[32], retention_ms[32];
  uint64_t floor = 0u;
  size_t records;
  size_t bytes;
  size_t trimmed;
  int rc;
  if (!store || !cursor || (!payload.data && payload.size > 0u)) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  if (timestamp_ms > FLOW_REDIS_LOG_MAX_EXACT_INTEGER) return TURBO_ERANGE;
  if (payload.size > store->limits.max_item_bytes) {
    store->stats.rejects++;
    return TURBO_EFBIG;
  }
  if (store->limits.retention_ms > 0u && timestamp_ms > store->limits.retention_ms)
    floor = timestamp_ms - store->limits.retention_ms;
  (void)snprintf(timestamp, sizeof(timestamp), "%llu", (unsigned long long)timestamp_ms);
  (void)snprintf(payload_size, sizeof(payload_size), "%zu", payload.size);
  (void)snprintf(max_records, sizeof(max_records), "%zu", store->limits.max_records);
  (void)snprintf(max_bytes, sizeof(max_bytes), "%zu", store->limits.max_bytes);
  (void)snprintf(floor_text, sizeof(floor_text), "%llu", (unsigned long long)floor);
  (void)snprintf(policy, sizeof(policy), "%u", (unsigned int)store->limits.full_policy);
  (void)snprintf(max_cursor, sizeof(max_cursor), "%llu",
                 (unsigned long long)FLOW_REDIS_LOG_MAX_EXACT_INTEGER);
  (void)snprintf(max_item_bytes, sizeof(max_item_bytes), "%zu", store->limits.max_item_bytes);
  (void)snprintf(retention_ms, sizeof(retention_ms), "%llu",
                 (unsigned long long)store->limits.retention_ms);
  {
    const char *argv[] = {"EVAL",
                          script,
                          "3",
                          store->stream_key,
                          store->order_key,
                          store->meta_key,
                          timestamp,
                          payload_size,
                          max_records,
                          max_bytes,
                          payload.size ? (const char *)payload.data : "",
                          floor_text,
                          policy,
                          max_cursor,
                          max_item_bytes,
                          retention_ms};
    const size_t lengths[] = {4u,
                              sizeof(script) - 1u,
                              1u,
                              store->stream_key_size,
                              store->order_key_size,
                              store->meta_key_size,
                              strlen(timestamp),
                              strlen(payload_size),
                              strlen(max_records),
                              strlen(max_bytes),
                              payload.size,
                              strlen(floor_text),
                              strlen(policy),
                              strlen(max_cursor),
                              strlen(max_item_bytes),
                              strlen(retention_ms)};
    rc = flow_redis_log_command(store, 16, argv, lengths, flow_redis_log_array_apply, &reply);
  }
  if (rc != TURBO_OK) return rc;
  if (reply.count == 1u) {
    if (reply.values[0] == -1) {
      store->stats.rejects++;
      return TURBO_ENOSPC;
    }
    if (reply.values[0] == -2 || reply.values[0] == -3) return TURBO_ERANGE;
    if (reply.values[0] == -5) return TURBO_EBUSY;
    return TURBO_EPROTO;
  }
  if (reply.count != 4u || reply.values[0] <= 0 ||
      flow_redis_log_size_from_i64(reply.values[1], &records) != TURBO_OK ||
      flow_redis_log_size_from_i64(reply.values[2], &bytes) != TURBO_OK ||
      flow_redis_log_size_from_i64(reply.values[3], &trimmed) != TURBO_OK)
    return TURBO_EPROTO;
  store->stats.writes++;
  store->stats.records = records;
  store->stats.bytes = bytes;
  store->stats.trims += trimmed;
  if (records > store->stats.peak_records) store->stats.peak_records = records;
  if (bytes > store->stats.peak_bytes) store->stats.peak_bytes = bytes;
  *cursor = (uint64_t)reply.values[0];
  return TURBO_OK;
}

static int flow_redis_log_read(void *ctx, uint64_t cursor, turbo_flow_log_record_t *records,
                               size_t capacity, size_t *count) {
  static const char script[] = "local r=tonumber(redis.call('HGET',KEYS[2],'records') or '0'); "
                               "if r==0 or tonumber(ARGV[2])==0 then return {0,{}} end; "
                               "local h=tonumber(redis.call('HGET',KEYS[2],'head') or '1'); "
                               "local c=tonumber(ARGV[1]); if c>0 and c<h then return {1,{}} end; "
                               "local s='-'; if c>0 then s=ARGV[1]..'-0' end; "
                               "return {0,redis.call('XRANGE',KEYS[1],s,'+','COUNT',ARGV[2])}";
  flow_redis_log_store_t *store = (flow_redis_log_store_t *)ctx;
  flow_redis_log_read_reply_t reply;
  char cursor_text[32], capacity_text[32];
  int rc;
  if (!store || !count || (capacity > 0u && !records) || capacity > store->max_operation_records)
    return TURBO_EINVAL;
  if (cursor > FLOW_REDIS_LOG_MAX_EXACT_INTEGER) return TURBO_ERANGE;
  *count = 0u;
  (void)snprintf(cursor_text, sizeof(cursor_text), "%llu", (unsigned long long)cursor);
  (void)snprintf(capacity_text, sizeof(capacity_text), "%zu", capacity);
  memset(&reply, 0, sizeof(reply));
  reply.records = records;
  reply.capacity = capacity;
  {
    const char *argv[] = {"EVAL",          script,      "2",          store->stream_key,
                          store->meta_key, cursor_text, capacity_text};
    const size_t lengths[] = {4u,
                              sizeof(script) - 1u,
                              1u,
                              store->stream_key_size,
                              store->meta_key_size,
                              strlen(cursor_text),
                              strlen(capacity_text)};
    rc = flow_redis_log_command(store, 7, argv, lengths, flow_redis_log_read_apply, &reply);
  }
  store->stats.queries++;
  if (rc != TURBO_OK) return rc;
  if (reply.stale) return TURBO_ERANGE;
  *count = reply.count;
  return TURBO_OK;
}

/** O(n) over the bounded namespace, followed by one atomic prefix deletion. */
static int flow_redis_log_trim_common(flow_redis_log_store_t *store, uint64_t threshold,
                                      int by_time, size_t *trimmed) {
  static const char script[] =
      "local st=redis.call('TYPE',KEYS[1]).ok; local ot=redis.call('TYPE',KEYS[2]).ok; "
      "local mt=redis.call('TYPE',KEYS[3]).ok; "
      "if (st~='none' and st~='stream') or (ot~='none' and ot~='zset') or "
      "(mt~='none' and mt~='hash') then return {-1} end; "
      "local schema=redis.call('HGET',KEYS[3],'schema'); "
      "if not schema then if st=='none' and ot=='none' and mt=='none' then "
      "return {0,0,0,1,0} end; return {-1} end; "
      "if schema~='1' or redis.call('HGET',KEYS[3],'max_records')~=ARGV[3] or "
      "redis.call('HGET',KEYS[3],'max_bytes')~=ARGV[4] or "
      "redis.call('HGET',KEYS[3],'max_item_bytes')~=ARGV[5] or "
      "redis.call('HGET',KEYS[3],'retention_ms')~=ARGV[6] or "
      "redis.call('HGET',KEYS[3],'full_policy')~=ARGV[7] then return {-2} end; "
      "local r=tonumber(redis.call('HGET',KEYS[3],'records')); "
      "local b=tonumber(redis.call('HGET',KEYS[3],'bytes')); "
      "if not r or not b or r<0 or b<0 or redis.call('ZCARD',KEYS[2])~=r or "
      "redis.call('XLEN',KEYS[1])~=r then return {-1} end; "
      "local all=redis.call('ZRANGE',KEYS[2],0,-1); local del={}; local db=0; local total=0; "
      "local selecting=true; local th=tonumber(ARGV[1]); for _,m in ipairs(all) do "
      "local c,t,n=string.match(m,'^(%d+):(%d+):(%d+)$'); if not c then return {-1} end; "
      "local bytes=tonumber(n); total=total+bytes; "
      "local v=ARGV[2]=='1' and tonumber(t) or tonumber(c); "
      "if selecting and v<th then table.insert(del,m); db=db+bytes else selecting=false end end; "
      "if total~=b then return {-1} end; "
      "for _,m in ipairs(del) do local c=string.match(m,'^(%d+):'); "
      "redis.call('XDEL',KEYS[1],c..'-0'); redis.call('ZREM',KEYS[2],m) end; "
      "r=r-#del; b=b-db; "
      "local nx=tonumber(redis.call('HGET',KEYS[3],'next') or '1'); local h=nx; local tail=0; "
      "local first=redis.call('ZRANGE',KEYS[2],0,0,'WITHSCORES'); "
      "local last=redis.call('ZREVRANGE',KEYS[2],0,0,'WITHSCORES'); "
      "if #first>0 then h=tonumber(first[2]); tail=tonumber(last[2]) end; "
      "redis.call('HSET',KEYS[3],'records',r,'bytes',b,'head',h,'tail',tail); "
      "return {#del,r,b,h,tail}";
  flow_redis_log_array_reply_t reply = {{0}, 0u};
  char threshold_text[32], mode[2], max_records[32], max_bytes[32], max_item_bytes[32];
  char retention_ms[32], policy[2];
  size_t removed;
  size_t records;
  size_t bytes;
  int rc;
  if (!store || !trimmed) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  if (threshold > FLOW_REDIS_LOG_MAX_EXACT_INTEGER) return TURBO_ERANGE;
  (void)snprintf(threshold_text, sizeof(threshold_text), "%llu", (unsigned long long)threshold);
  (void)snprintf(mode, sizeof(mode), "%u", by_time ? 1u : 0u);
  (void)snprintf(max_records, sizeof(max_records), "%zu", store->limits.max_records);
  (void)snprintf(max_bytes, sizeof(max_bytes), "%zu", store->limits.max_bytes);
  (void)snprintf(max_item_bytes, sizeof(max_item_bytes), "%zu", store->limits.max_item_bytes);
  (void)snprintf(retention_ms, sizeof(retention_ms), "%llu",
                 (unsigned long long)store->limits.retention_ms);
  (void)snprintf(policy, sizeof(policy), "%u", (unsigned int)store->limits.full_policy);
  {
    const char *argv[] = {
        "EVAL",          script,         "3",   store->stream_key, store->order_key,
        store->meta_key, threshold_text, mode,  max_records,       max_bytes,
        max_item_bytes,  retention_ms,   policy};
    const size_t lengths[] = {4u,
                              sizeof(script) - 1u,
                              1u,
                              store->stream_key_size,
                              store->order_key_size,
                              store->meta_key_size,
                              strlen(threshold_text),
                              strlen(mode),
                              strlen(max_records),
                              strlen(max_bytes),
                              strlen(max_item_bytes),
                              strlen(retention_ms),
                              strlen(policy)};
    rc = flow_redis_log_command(store, 13, argv, lengths, flow_redis_log_array_apply, &reply);
  }
  if (rc != TURBO_OK) return rc;
  if (reply.count == 1u && reply.values[0] == -2) return TURBO_EBUSY;
  if (reply.count != 5u || flow_redis_log_size_from_i64(reply.values[0], &removed) != TURBO_OK ||
      flow_redis_log_size_from_i64(reply.values[1], &records) != TURBO_OK ||
      flow_redis_log_size_from_i64(reply.values[2], &bytes) != TURBO_OK)
    return TURBO_EPROTO;
  if (removed > 0u) store->stats.writes++;
  store->stats.records = records;
  store->stats.bytes = bytes;
  store->stats.trims += removed;
  *trimmed = removed;
  return TURBO_OK;
}

static int flow_redis_log_trim_before_cursor(void *ctx, uint64_t cursor, size_t *trimmed) {
  return flow_redis_log_trim_common((flow_redis_log_store_t *)ctx, cursor, 0, trimmed);
}

static int flow_redis_log_trim_before_time(void *ctx, uint64_t timestamp_ms, size_t *trimmed) {
  return flow_redis_log_trim_common((flow_redis_log_store_t *)ctx, timestamp_ms, 1, trimmed);
}

static int flow_redis_log_bounds(void *ctx, uint64_t *head, uint64_t *tail) {
  static const char script[] = "local r=tonumber(redis.call('HGET',KEYS[1],'records') or '0'); "
                               "local nx=tonumber(redis.call('HGET',KEYS[1],'next') or '1'); "
                               "if r==0 then return {nx,0} end; "
                               "return {tonumber(redis.call('HGET',KEYS[1],'head')),"
                               "tonumber(redis.call('HGET',KEYS[1],'tail'))}";
  flow_redis_log_store_t *store = (flow_redis_log_store_t *)ctx;
  flow_redis_log_array_reply_t reply = {{0}, 0u};
  int rc;
  if (!store || !head || !tail) return TURBO_EINVAL;
  {
    const char *argv[] = {"EVAL", script, "1", store->meta_key};
    const size_t lengths[] = {4u, sizeof(script) - 1u, 1u, store->meta_key_size};
    rc = flow_redis_log_command(store, 4, argv, lengths, flow_redis_log_array_apply, &reply);
  }
  store->stats.queries++;
  if (rc != TURBO_OK) return rc;
  if (reply.count != 2u || reply.values[0] < 0 || reply.values[1] < 0) return TURBO_EPROTO;
  *head = (uint64_t)reply.values[0];
  *tail = (uint64_t)reply.values[1];
  return TURBO_OK;
}

static int flow_redis_log_stats(const void *ctx, turbo_flow_store_stats_t *out) {
  static const char script[] = "return {tonumber(redis.call('HGET',KEYS[1],'records') or '0'),"
                               "tonumber(redis.call('HGET',KEYS[1],'bytes') or '0')}";
  flow_redis_log_store_t *store = (flow_redis_log_store_t *)ctx;
  flow_redis_log_array_reply_t reply = {{0}, 0u};
  size_t records;
  size_t bytes;
  size_t output_size;
  int rc;
  if (!store || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_STORE_ABI_VERSION)
    return TURBO_EINVAL;
  {
    const char *argv[] = {"EVAL", script, "1", store->meta_key};
    const size_t lengths[] = {4u, sizeof(script) - 1u, 1u, store->meta_key_size};
    rc = flow_redis_log_command(store, 4, argv, lengths, flow_redis_log_array_apply, &reply);
  }
  if (rc != TURBO_OK) return rc;
  if (reply.count != 2u || flow_redis_log_size_from_i64(reply.values[0], &records) != TURBO_OK ||
      flow_redis_log_size_from_i64(reply.values[1], &bytes) != TURBO_OK)
    return TURBO_EPROTO;
  store->stats.records = records;
  store->stats.bytes = bytes;
  if (records > store->stats.peak_records) store->stats.peak_records = records;
  if (bytes > store->stats.peak_bytes) store->stats.peak_bytes = bytes;
  output_size = out->size;
  *out = store->stats;
  out->size = output_size;
  return TURBO_OK;
}

static const turbo_flow_log_store_provider_ops_t FLOW_REDIS_LOG_OPS = {
    sizeof(turbo_flow_log_store_provider_ops_t),
    TURBO_FLOW_LOG_STORE_PROVIDER_API_VERSION,
    flow_redis_log_close,
    flow_redis_log_destroy,
    flow_redis_log_append,
    flow_redis_log_read,
    flow_redis_log_trim_before_cursor,
    flow_redis_log_trim_before_time,
    flow_redis_log_bounds,
    flow_redis_log_stats};

int flow_redis_log_store_create(const turbo_flow_redis_log_store_config_t *config,
                                const turbo_flow_store_limits_t *limits,
                                turbo_flow_log_store_t **out) {
  flow_redis_store_client_config_t client_config;
  flow_redis_log_store_t *store;
  char *prefix = NULL;
  size_t prefix_size = 0u;
  const size_t max_operation_records = config && config->max_operation_records
                                           ? config->max_operation_records
                                           : TURBO_FLOW_REDIS_LOG_DEFAULT_MAX_OPERATION_RECORDS;
  int rc;
  if (!config || !limits || !out || !config->key || !config->key[0]) return TURBO_EINVAL;
  *out = NULL;
  rc = turbo_flow_store_limits_validate(limits, 1);
  if (rc != TURBO_OK) return rc;
  if (limits->max_item_bytes > TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE ||
      limits->max_records > FLOW_REDIS_LOG_MAX_EXACT_INTEGER ||
      limits->max_bytes > FLOW_REDIS_LOG_MAX_EXACT_INTEGER ||
      max_operation_records < limits->max_records ||
      max_operation_records > TURBO_FLOW_REDIS_LOG_MAX_OPERATION_RECORDS)
    return TURBO_EINVAL;
  store = (flow_redis_log_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->limits = *limits;
  store->stats = (turbo_flow_store_stats_t)TURBO_FLOW_STORE_STATS_INIT;
  store->max_operation_records = max_operation_records;
  rc = flow_redis_log_prefix_create(config->key, &prefix, &prefix_size);
  if (rc == TURBO_OK)
    rc = flow_redis_log_key_create(prefix, prefix_size, FLOW_REDIS_LOG_STREAM_SUFFIX,
                                   &store->stream_key, &store->stream_key_size);
  if (rc == TURBO_OK)
    rc = flow_redis_log_key_create(prefix, prefix_size, FLOW_REDIS_LOG_ORDER_SUFFIX,
                                   &store->order_key, &store->order_key_size);
  if (rc == TURBO_OK)
    rc = flow_redis_log_key_create(prefix, prefix_size, FLOW_REDIS_LOG_META_SUFFIX,
                                   &store->meta_key, &store->meta_key_size);
  free(prefix);
  memset(&client_config, 0, sizeof(client_config));
  client_config.host = config->host;
  client_config.port = config->port;
  client_config.username = config->username;
  client_config.password = config->password;
  client_config.database = config->database;
  client_config.timeout_ms = config->timeout_ms;
  if (rc == TURBO_OK) rc = flow_redis_store_client_create(&client_config, &store->client);
  if (rc == TURBO_OK) rc = turbo_flow_log_store_create_provider(&FLOW_REDIS_LOG_OPS, store, out);
  if (rc != TURBO_OK) flow_redis_log_destroy(store);
  return rc;
}
