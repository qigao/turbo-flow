#include "turbo_flow_redis.h"

#include "CoroNet.h"
#include "flow_connection.h"
#include "flow_timer.h"
#include "redis_client.h"
#include "turbo_hash.h"
#include "turbo_heap.h"
#include "turbo_thread.h"
#include "turbo_vec.h"

#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_REDIS_DEFAULT_TIMEOUT_MS 5000u
#define FLOW_REDIS_DEFAULT_READ_COUNT 16u
#define FLOW_REDIS_DEFAULT_BLOCK_MS 250u

typedef enum flow_redis_adapter_mode_e {
  FLOW_REDIS_MODE_STREAM_SINK = 1,
  FLOW_REDIS_MODE_STREAM_SOURCE,
  FLOW_REDIS_MODE_DATA_SET,
  FLOW_REDIS_MODE_DATA_GET,
  FLOW_REDIS_MODE_RECORD_STORE
} flow_redis_adapter_mode_t;

typedef struct flow_redis_adapter_s {
  tstr_t stream;
  tstr_t field;
  tstr_t group;
  tstr_t consumer;
  tstr_t group_start_id;
  tstr_t key;
  flow_redis_adapter_mode_t mode;
  size_t max_value_size;
  size_t record_max_key_size;
  size_t record_max_batch_size;
  size_t record_max_records;
  turbo_hash_map_t record_mutation_keys;
  int record_mutation_keys_initialized;
  size_t maxlen;
  uint32_t poll_interval_ms;
  size_t read_count;
  uint32_t block_ms;
  int create_group;
  coro_context_t *context;
  redis_client_t *client;
  turbo_mutex_t lock;
  int lock_initialized;
  tf_timer_t poll_wait;
  int poll_wait_initialized;
  turbo_thread_t thread;
  int thread_started;
  int connected;
  atomic_int started;
  atomic_int quiesced;
  tf_connection_state_t connection;
  turbo_flow_t *flow;
  tstr_t source_name;
} flow_redis_adapter_t;

typedef struct flow_redis_claim_record_s {
  int occupied;
  int active;
  int ambiguous_ack;
  uint64_t token;
  uint64_t order;
  tstr_t id;
  tstr_t payload;
} flow_redis_claim_record_t;

static int flow_redis_claim_order_compare(const void *left, const void *right, void *ctx) {
  const flow_redis_claim_record_t *const *left_record =
      (const flow_redis_claim_record_t *const *)left;
  const flow_redis_claim_record_t *const *right_record =
      (const flow_redis_claim_record_t *const *)right;
  (void)ctx;
  if ((*left_record)->order < (*right_record)->order) return -1;
  if ((*left_record)->order > (*right_record)->order) return 1;
  return 0;
}

TURBO_VEC_DEFINE(flow_redis_claim_records, flow_redis_claim_record_t)
TURBO_VEC_DEFINE(flow_redis_claim_slots, size_t)
TURBO_HASH_MAP_DEFINE(flow_redis_claim_index, uint64_t, size_t)
TURBO_HEAP_DEFINE(flow_redis_requeued, flow_redis_claim_record_t *, flow_redis_claim_order_compare)

struct turbo_flow_redis_stream_owner_s {
  flow_redis_adapter_t *adapter;
  flow_redis_claim_records records;
  flow_redis_claim_slots free_slots;
  flow_redis_claim_index claim_index;
  flow_redis_requeued requeued;
  tstr_t replay_cursor;
  size_t max_active_claims;
  size_t active_claims;
  uint64_t claim_generation;
  uint64_t order;
  int replay_pending;
};

typedef enum flow_redis_task_kind_e {
  FLOW_REDIS_TASK_CONNECT,
  FLOW_REDIS_TASK_XADD,
  FLOW_REDIS_TASK_XREADGROUP,
  FLOW_REDIS_TASK_XACK,
  FLOW_REDIS_TASK_XPENDING_EXACT,
  FLOW_REDIS_TASK_XGROUP,
  FLOW_REDIS_TASK_SET,
  FLOW_REDIS_TASK_GET,
  FLOW_REDIS_TASK_STATE_GET,
  FLOW_REDIS_TASK_STATE_COMMIT,
  FLOW_REDIS_TASK_RECORD_SCAN,
  FLOW_REDIS_TASK_RECORD_COMMIT
} flow_redis_task_kind_t;

typedef struct flow_redis_task_s {
  flow_redis_adapter_t *adapter;
  flow_redis_task_kind_t kind;
  const char *payload;
  size_t payload_len;
  const char *id;
  const char *state_key;
  turbo_flow_claim_commit_action_t commit_action;
  const turbo_flow_record_mutation_t *record_mutations;
  size_t record_mutation_count;
  turbo_flow_record_visit_fn record_visit;
  void *record_visit_ctx;
  redis_stream_result_t *results;
  size_t result_count;
  tstr_t response;
  redis_command_outcome_t outcome;
  redis_server_error_t server_error;
  int present;
  int status;
  int done;
} flow_redis_task_t;

static const turbo_flow_option_field_t FLOW_REDIS_FIELDS[] = {
    {"host", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"port", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 65535,
     NULL, 0},
    {"username", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"password", TURBO_FLOW_OPTION_SECRET, TURBO_FLOW_OPTION_SECRET_VALUE, 0, 0, NULL, 0},
    {"database", TURBO_FLOW_OPTION_U32, TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 0,
     15, NULL, 0},
    {"timeout_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"stream", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"field", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"maxlen", TURBO_FLOW_OPTION_U32, 0, 0, 0, NULL, 0},
    {"poll_interval_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"group", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"consumer", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"group_start_id", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"read_count", TURBO_FLOW_OPTION_U32, 0, 1, 0, NULL, 0},
    {"block_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"create_group", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0}};
static const char *const FLOW_REDIS_DATA_OPERATION_VALUES[] = {"set", "get"};
static const turbo_flow_option_field_t FLOW_REDIS_DATA_FIELDS[] = {
    {"host", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"port", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 65535,
     NULL, 0},
    {"username", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"password", TURBO_FLOW_OPTION_SECRET, TURBO_FLOW_OPTION_SECRET_VALUE, 0, 0, NULL, 0},
    {"database", TURBO_FLOW_OPTION_U32, TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 0,
     15, NULL, 0},
    {"timeout_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"key", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"operation", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0,
     FLOW_REDIS_DATA_OPERATION_VALUES, 2u},
    {"max_value_size", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1,
     TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE, NULL, 0}};
static const turbo_flow_adapter_schema_t FLOW_REDIS_INPUT_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_REDIS,
    TURBO_FLOW_ADAPTER_SOURCE,
    TURBO_FLOW_ADAPTER_INPUT,
    FLOW_REDIS_FIELDS,
    sizeof(FLOW_REDIS_FIELDS) / sizeof(FLOW_REDIS_FIELDS[0])};
static const turbo_flow_adapter_schema_t FLOW_REDIS_OUTPUT_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_REDIS,
    TURBO_FLOW_ADAPTER_SINK,
    TURBO_FLOW_ADAPTER_OUTPUT,
    FLOW_REDIS_FIELDS,
    sizeof(FLOW_REDIS_FIELDS) / sizeof(FLOW_REDIS_FIELDS[0])};
static const turbo_flow_adapter_schema_t FLOW_REDIS_DATA_SET_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_REDIS,
    TURBO_FLOW_ADAPTER_SINK,
    TURBO_FLOW_ADAPTER_OUTPUT,
    FLOW_REDIS_DATA_FIELDS,
    sizeof(FLOW_REDIS_DATA_FIELDS) / sizeof(FLOW_REDIS_DATA_FIELDS[0])};
static const turbo_flow_adapter_schema_t FLOW_REDIS_DATA_GET_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_REDIS,
    TURBO_FLOW_ADAPTER_TRANSFORM,
    TURBO_FLOW_ADAPTER_BIDIRECTIONAL,
    FLOW_REDIS_DATA_FIELDS,
    sizeof(FLOW_REDIS_DATA_FIELDS) / sizeof(FLOW_REDIS_DATA_FIELDS[0])};

static int flow_redis_record_decode(const flow_redis_adapter_t *adapter,
                                    const redis_reply_t *key_reply,
                                    const redis_reply_t *value_reply,
                                    turbo_flow_record_view_t *out) {
  const char *separator;
  uint64_t revision = 0u;
  size_t revision_size;
  if (!adapter || !key_reply || !value_reply || !out ||
      key_reply->type != REDIS_REPLY_BULK_STRING ||
      value_reply->type != REDIS_REPLY_BULK_STRING || !key_reply->str || !value_reply->str ||
      key_reply->len == 0u || key_reply->len > adapter->record_max_key_size)
    return TURBO_EPROTO;
  separator = (const char *)memchr(value_reply->str, '\0', value_reply->len);
  if (!separator) return TURBO_EPROTO;
  revision_size = (size_t)(separator - value_reply->str);
  if (revision_size == 0u || revision_size > 19u ||
      value_reply->len - revision_size - 1u > adapter->max_value_size)
    return TURBO_EPROTO;
  for (size_t i = 0u; i < revision_size; ++i) {
    unsigned int digit;
    if (value_reply->str[i] < '0' || value_reply->str[i] > '9') return TURBO_EPROTO;
    digit = (unsigned int)(value_reply->str[i] - '0');
    if (revision > ((uint64_t)TURBO_FLOW_RECORD_REVISION_MAX - digit) / 10u)
      return TURBO_EPROTO;
    revision = revision * 10u + digit;
  }
  if (revision == TURBO_FLOW_RECORD_REVISION_ABSENT) return TURBO_EPROTO;
  *out = (turbo_flow_record_view_t)TURBO_FLOW_RECORD_VIEW_INIT;
  out->key = (const uint8_t *)key_reply->str;
  out->key_size = key_reply->len;
  out->revision = revision;
  out->value = (const uint8_t *)separator + 1u;
  out->value_size = value_reply->len - revision_size - 1u;
  return TURBO_OK;
}

static int flow_redis_command_apply(flow_redis_task_t *task, const redis_command_result_t *result) {
  const redis_reply_t *reply;
  if (!task || !result) return TURBO_EINVAL;
  task->outcome = result->outcome;
  task->server_error = result->server_error;
  if (task->kind == FLOW_REDIS_TASK_XGROUP && result->server_error == REDIS_SERVER_ERROR_BUSY_GROUP)
    return TURBO_OK;
  if (result->status != TURBO_OK) return result->status;
  reply = result->reply;
  if (!reply) return TURBO_EPROTO;
  if (task->kind == FLOW_REDIS_TASK_XACK)
    return reply->type == REDIS_REPLY_INTEGER && reply->integer == 1 ? TURBO_OK : TURBO_EPROTO;
  if (task->kind == FLOW_REDIS_TASK_STATE_COMMIT) {
    if (reply->type != REDIS_REPLY_INTEGER) return TURBO_EPROTO;
    if (reply->integer == 1 || reply->integer == 2) return TURBO_OK;
    if (reply->integer == -1) return TURBO_EALREADY;
    if (reply->integer == -2) return TURBO_EBUSY;
    return TURBO_EPROTO;
  }
  if (task->kind == FLOW_REDIS_TASK_RECORD_COMMIT) {
    if (reply->type != REDIS_REPLY_INTEGER) return TURBO_EPROTO;
    if (reply->integer == 1) return TURBO_OK;
    if (reply->integer == -1) return TURBO_EBUSY;
    if (reply->integer == -2) return TURBO_ENOSPC;
    return TURBO_EPROTO;
  }
  if (task->kind == FLOW_REDIS_TASK_RECORD_SCAN) {
    if (reply->type != REDIS_REPLY_ARRAY || (reply->element_count & 1u) != 0u)
      return TURBO_EPROTO;
    if (reply->element_count / 2u > task->adapter->record_max_records) return TURBO_ENOSPC;
    for (size_t i = 0u; i < reply->element_count; i += 2u) {
      turbo_flow_record_view_t record = TURBO_FLOW_RECORD_VIEW_INIT;
      int rc = flow_redis_record_decode(task->adapter, reply->elements[i],
                                        reply->elements[i + 1u], &record);
      if (rc != TURBO_OK) return rc;
      rc = task->record_visit(task->record_visit_ctx, &record);
      if (rc != TURBO_OK) return rc;
    }
    return TURBO_OK;
  }
  if (task->kind == FLOW_REDIS_TASK_XPENDING_EXACT) {
    const redis_reply_t *entry;
    if (reply->type != REDIS_REPLY_ARRAY || reply->element_count > 1u) return TURBO_EPROTO;
    task->present = reply->element_count == 1u;
    if (!task->present) return TURBO_OK;
    entry = reply->elements[0];
    if (!entry || entry->type != REDIS_REPLY_ARRAY || entry->element_count != 4u ||
        !entry->elements[0] || entry->elements[0]->type != REDIS_REPLY_BULK_STRING ||
        !entry->elements[0]->str || strcmp(entry->elements[0]->str, task->id) != 0 ||
        !entry->elements[1] || entry->elements[1]->type != REDIS_REPLY_BULK_STRING ||
        !entry->elements[1]->str || !entry->elements[2] ||
        entry->elements[2]->type != REDIS_REPLY_INTEGER || !entry->elements[3] ||
        entry->elements[3]->type != REDIS_REPLY_INTEGER) {
      return TURBO_EPROTO;
    }
    return strcmp(entry->elements[1]->str, task->adapter->consumer) == 0 ? TURBO_OK : TURBO_EBUSY;
  }
  if (task->kind == FLOW_REDIS_TASK_XADD)
    return reply->type == REDIS_REPLY_BULK_STRING ? TURBO_OK : TURBO_EPROTO;
  if (task->kind == FLOW_REDIS_TASK_XGROUP)
    return reply->type == REDIS_REPLY_STRING ? TURBO_OK : TURBO_EPROTO;
  if (task->kind == FLOW_REDIS_TASK_SET)
    return reply->type == REDIS_REPLY_STRING ? TURBO_OK : TURBO_EPROTO;
  if (task->kind != FLOW_REDIS_TASK_GET && task->kind != FLOW_REDIS_TASK_STATE_GET) return TURBO_OK;
  if (reply->type == REDIS_REPLY_NULL) return TURBO_ENOENT;
  if (reply->type != REDIS_REPLY_BULK_STRING) return TURBO_EPROTO;
  if (reply->len > task->adapter->max_value_size) return TURBO_EMSGSIZE;
  task->response = tstr_new_len(reply->str ? reply->str : "", reply->len);
  return task->response ? TURBO_OK : TURBO_ENOMEM;
}

static int flow_redis_connect(flow_redis_adapter_t *adapter) {
  int rc;
  if (adapter->connected) return TURBO_OK;
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_OK);
  rc = redis_client_connect(adapter->client, NULL, NULL);
  if (rc != TURBO_OK) {
    tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, rc);
    return rc;
  }
  adapter->connected = 1;
  tf_connection_set_usage(&adapter->connection, 1u, 0u, 0u);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
  return TURBO_OK;
}

static void flow_redis_task_run(coro_t *co, void *arg) {
  flow_redis_task_t *task = (flow_redis_task_t *)arg;
  flow_redis_adapter_t *adapter = task->adapter;
  redis_command_result_t command = REDIS_COMMAND_RESULT_INIT;
  redis_stream_read_result_t stream_read = REDIS_STREAM_READ_RESULT_INIT;
  const char *keys[] = {adapter->stream};
  const char *ids[] = {task->id ? task->id : ">"};
  const char *fields[] = {adapter->field};
  const char *values[] = {task->payload};
  size_t lens[] = {task->payload_len};
  const char *set_argv[] = {"SET", adapter->key, task->payload};
  size_t set_lens[] = {3u, adapter->key ? tstr_len(adapter->key) : 0u, task->payload_len};
  const char *get_argv[] = {"GET", adapter->key};
  size_t get_lens[] = {3u, adapter->key ? tstr_len(adapter->key) : 0u};
  const char *state_get_argv[] = {"GET", task->state_key};
  size_t state_get_lens[] = {3u, task->state_key ? strlen(task->state_key) : 0u};
  (void)co;

  task->status = flow_redis_connect(adapter);
  if (task->status != TURBO_OK) goto done;
  switch (task->kind) {
  case FLOW_REDIS_TASK_CONNECT:
    break;
  case FLOW_REDIS_TASK_XADD:
    (void)redis_xadd_result(adapter->client, adapter->stream, adapter->maxlen, 1, fields, values,
                            lens, &command);
    task->status = flow_redis_command_apply(task, &command);
    break;
  case FLOW_REDIS_TASK_XREADGROUP:
    task->status = redis_xreadgroup_result(adapter->client, adapter->group, adapter->consumer,
                                           adapter->read_count, (int)adapter->block_ms, 1, keys,
                                           ids, &stream_read);
    task->outcome = stream_read.command.outcome;
    task->server_error = stream_read.command.server_error;
    if (task->status == TURBO_OK) {
      task->results = stream_read.streams;
      task->result_count = stream_read.stream_count;
      stream_read.streams = NULL;
      stream_read.stream_count = 0u;
    }
    break;
  case FLOW_REDIS_TASK_XACK: {
    const char *ack_ids[] = {task->id};
    (void)redis_xack_result(adapter->client, adapter->stream, adapter->group, 1, ack_ids, &command);
    task->status = flow_redis_command_apply(task, &command);
    break;
  }
  case FLOW_REDIS_TASK_XPENDING_EXACT: {
    const char *pending_argv[] = {"XPENDING", adapter->stream, adapter->group,
                                  task->id,   task->id,        "1"};
    (void)redis_commandv_result(adapter->client, 6, pending_argv, NULL, &command);
    task->status = flow_redis_command_apply(task, &command);
    break;
  }
  case FLOW_REDIS_TASK_XGROUP:
    (void)redis_xgroup_create_result(adapter->client, adapter->stream, adapter->group,
                                     adapter->group_start_id, 1, &command);
    task->status = flow_redis_command_apply(task, &command);
    break;
  case FLOW_REDIS_TASK_SET:
    (void)redis_commandv_result(adapter->client, 3, set_argv, set_lens, &command);
    task->status = flow_redis_command_apply(task, &command);
    break;
  case FLOW_REDIS_TASK_GET:
    (void)redis_commandv_result(adapter->client, 2, get_argv, get_lens, &command);
    task->status = flow_redis_command_apply(task, &command);
    break;
  case FLOW_REDIS_TASK_STATE_GET:
    (void)redis_commandv_result(adapter->client, 2, state_get_argv, state_get_lens, &command);
    task->status = flow_redis_command_apply(task, &command);
    break;
  case FLOW_REDIS_TASK_STATE_COMMIT: {
    static const char script[] =
        "local a=ARGV[1] "
        "if a=='0' then redis.call('SET',KEYS[2],ARGV[5]); return 1 end "
        "local p=redis.call('XPENDING',KEYS[1],ARGV[2],ARGV[4],ARGV[4],1) "
        "if #p==0 then local s=redis.call('GET',KEYS[2]); "
        "if s and s==ARGV[5] then return 2 end; return -1 end "
        "if p[1][2]~=ARGV[3] then return -2 end "
        "redis.call('SET',KEYS[2],ARGV[5]) "
        "if a=='2' then return 1 end "
        "if redis.call('XACK',KEYS[1],ARGV[2],ARGV[4])~=1 then return -3 end "
        "return 1";
    char action[2] = {(char)('0' + (int)task->commit_action), '\0'};
    const char *commit_argv[] = {"EVAL",
                                 script,
                                 "2",
                                 adapter->stream,
                                 task->state_key,
                                 action,
                                 adapter->group,
                                 adapter->consumer,
                                 task->id ? task->id : "",
                                 task->payload};
    size_t commit_lens[] = {4u,
                            sizeof(script) - 1u,
                            1u,
                            tstr_len(adapter->stream),
                            strlen(task->state_key),
                            1u,
                            tstr_len(adapter->group),
                            tstr_len(adapter->consumer),
                            task->id ? strlen(task->id) : 0u,
                            task->payload_len};
    (void)redis_commandv_result(adapter->client, 10, commit_argv, commit_lens, &command);
    task->status = flow_redis_command_apply(task, &command);
    break;
  }
  case FLOW_REDIS_TASK_RECORD_SCAN: {
    const char *scan_argv[] = {"HGETALL", adapter->key};
    size_t scan_lens[] = {7u, adapter->key ? tstr_len(adapter->key) : 0u};
    (void)redis_commandv_result(adapter->client, 2, scan_argv, scan_lens, &command);
    task->status = flow_redis_command_apply(task, &command);
    break;
  }
  case FLOW_REDIS_TASK_RECORD_COMMIT: {
    static const char script[] =
        "local m=tonumber(ARGV[1]); local n=tonumber(ARGV[2]); local z=string.char(0); "
        "local d=0; local p=3; "
        "for i=1,n do local k=ARGV[p]; local f=ARGV[p+1]; local e=ARGV[p+2]; "
        "local c=redis.call('HGET',KEYS[1],f); "
        "if e=='0' then if c then return -1 end; d=d+1 "
        "else if not c then return -1 end; local s=string.find(c,z,1,true); "
        "if not s then return -3 end; if string.sub(c,1,s-1)~=e then return -1 end end; "
        "if k=='2' then d=d-1 end; p=p+5 end; "
        "if redis.call('HLEN',KEYS[1])+d>m then return -2 end; p=3; "
        "for i=1,n do local k=ARGV[p]; local f=ARGV[p+1]; local x=ARGV[p+3]; "
        "local v=ARGV[p+4]; if k=='1' then redis.call('HSET',KEYS[1],f,x..z..v) "
        "else redis.call('HDEL',KEYS[1],f) end; p=p+5 end; return 1";
    typedef struct flow_redis_record_revision_text_s {
      char kind[2];
      char expected[21];
      char next[21];
    } flow_redis_record_revision_text_t;
    const size_t argument_count = 6u + task->record_mutation_count * 5u;
    const char **arguments = (const char **)calloc(argument_count, sizeof(*arguments));
    size_t *lengths = (size_t *)calloc(argument_count, sizeof(*lengths));
    flow_redis_record_revision_text_t *revisions =
        (flow_redis_record_revision_text_t *)calloc(task->record_mutation_count,
                                                    sizeof(*revisions));
    char max_records[32];
    char mutation_count[32];
    if (!arguments || !lengths || !revisions ||
        snprintf(max_records, sizeof(max_records), "%zu", adapter->record_max_records) < 0 ||
        snprintf(mutation_count, sizeof(mutation_count), "%zu", task->record_mutation_count) <
            0) {
      free(revisions);
      free(lengths);
      free(arguments);
      task->status = TURBO_ENOMEM;
      break;
    }
    arguments[0] = "EVAL";
    lengths[0] = 4u;
    arguments[1] = script;
    lengths[1] = sizeof(script) - 1u;
    arguments[2] = "1";
    lengths[2] = 1u;
    arguments[3] = adapter->key;
    lengths[3] = tstr_len(adapter->key);
    arguments[4] = max_records;
    lengths[4] = strlen(max_records);
    arguments[5] = mutation_count;
    lengths[5] = strlen(mutation_count);
    for (size_t i = 0u; i < task->record_mutation_count; ++i) {
      const turbo_flow_record_mutation_t *mutation = &task->record_mutations[i];
      size_t offset = 6u + i * 5u;
      revisions[i].kind[0] =
          mutation->kind == TURBO_FLOW_RECORD_PUT ? (char)'1' : (char)'2';
      (void)snprintf(revisions[i].expected, sizeof(revisions[i].expected), "%llu",
                     (unsigned long long)mutation->expected_revision);
      (void)snprintf(revisions[i].next, sizeof(revisions[i].next), "%llu",
                     (unsigned long long)mutation->next_revision);
      arguments[offset] = revisions[i].kind;
      lengths[offset] = 1u;
      arguments[offset + 1u] = (const char *)mutation->key;
      lengths[offset + 1u] = mutation->key_size;
      arguments[offset + 2u] = revisions[i].expected;
      lengths[offset + 2u] = strlen(revisions[i].expected);
      arguments[offset + 3u] = revisions[i].next;
      lengths[offset + 3u] = strlen(revisions[i].next);
      arguments[offset + 4u] =
          mutation->value_size != 0u ? (const char *)mutation->value : "";
      lengths[offset + 4u] = mutation->value_size;
    }
    (void)redis_commandv_result(adapter->client, (int)argument_count, arguments, lengths,
                                &command);
    task->status = flow_redis_command_apply(task, &command);
    free(revisions);
    free(lengths);
    free(arguments);
    break;
  }
  }
done:
  if (task->kind != FLOW_REDIS_TASK_CONNECT && task->outcome != REDIS_COMMAND_REPLIED &&
      task->status != TURBO_OK) {
    adapter->connected = 0;
    tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, task->status);
  }
  redis_command_result_clear(&command);
  redis_stream_read_result_clear(&stream_read);
  task->done = 1;
}

static int flow_redis_run(flow_redis_adapter_t *adapter, flow_redis_task_t *task) {
  int rc;
  task->adapter = adapter;
  tf_connection_set_usage(&adapter->connection, adapter->connected ? 1u : 0u, 1u,
                          task->payload_len);
  if (coro_context_spawn(adapter->context, flow_redis_task_run, task) != TURBO_OK) {
    tf_connection_set_usage(&adapter->connection, adapter->connected ? 1u : 0u, 0u, 0u);
    atomic_store_explicit(&adapter->connection.last_status, TURBO_EINVAL, memory_order_relaxed);
    return TURBO_EINVAL;
  }
  while (!task->done)
    (void)coro_context_run(adapter->context, TURBO_RUN_ONCE);
  rc = task->status;
  tf_connection_set_usage(&adapter->connection, adapter->connected ? 1u : 0u, 0u, 0u);
  atomic_store_explicit(&adapter->connection.last_status, rc, memory_order_relaxed);
  return rc;
}

static int flow_redis_connection_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  flow_redis_adapter_t *adapter = (flow_redis_adapter_t *)ctx;
  return adapter ? tf_connection_snapshot(&adapter->connection, out) : TURBO_EINVAL;
}

static const redis_stream_entry_t *flow_redis_entries_field(const redis_stream_entry_t *entry,
                                                            const char *field, size_t *index) {
  if (!entry || !field || !index) return NULL;
  for (size_t i = 0; i < entry->field_count; ++i) {
    if (entry->fields[i] && strcmp(entry->fields[i], field) == 0) {
      *index = i;
      return entry;
    }
  }
  return NULL;
}

static int flow_redis_publish_results(flow_redis_adapter_t *adapter, flow_redis_task_t *read) {
  int rc = TURBO_OK;
  for (size_t i = 0; i < read->result_count && rc == TURBO_OK; ++i) {
    redis_stream_result_t *result = &read->results[i];
    for (size_t j = 0; j < result->entry_count && rc == TURBO_OK; ++j) {
      redis_stream_entry_t *entry = &result->entries[j];
      size_t field_index = 0;
      turbo_flow_msg_t msg;
      flow_redis_task_t ack;
      if (!flow_redis_entries_field(entry, adapter->field, &field_index)) {
        rc = TURBO_EPROTO;
        break;
      }
      turbo_flow_msg_init(&msg);
      msg.owned_payload = tstr_new_len(entry->values[field_index], entry->value_lens[field_index]);
      if (!msg.owned_payload) return TURBO_ENOMEM;
      msg.payload = tstr_to_v(msg.owned_payload);
      rc = turbo_flow_publish(adapter->flow, adapter->source_name, &msg);
      turbo_flow_msg_cleanup(&msg);
      if (rc != TURBO_OK) break;
      memset(&ack, 0, sizeof(ack));
      ack.kind = FLOW_REDIS_TASK_XACK;
      ack.id = entry->id;
      rc = flow_redis_run(adapter, &ack);
    }
  }
  return rc;
}

static int flow_redis_results_have_entries(const flow_redis_task_t *read) {
  if (!read || !read->results) return 0;
  for (size_t i = 0; i < read->result_count; ++i) {
    if (read->results[i].entry_count > 0u) return 1;
  }
  return 0;
}

static void flow_redis_source_thread(void *arg) {
  flow_redis_adapter_t *adapter = (flow_redis_adapter_t *)arg;
  int replay_pending = 1;
  while (atomic_load_explicit(&adapter->started, memory_order_acquire) && adapter->flow &&
         turbo_flow_state(adapter->flow) != TURBO_FLOW_STATE_STARTED)
    turbo_sleep_ms(1);
  if (adapter->create_group) {
    flow_redis_task_t group;
    memset(&group, 0, sizeof(group));
    group.kind = FLOW_REDIS_TASK_XGROUP;
    turbo_mutex_lock(&adapter->lock);
    int group_rc = flow_redis_run(adapter, &group);
    turbo_mutex_unlock(&adapter->lock);
    if (group_rc != TURBO_OK) return;
  }
  while (atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    flow_redis_task_t read;
    int rc;
    memset(&read, 0, sizeof(read));
    read.kind = FLOW_REDIS_TASK_XREADGROUP;
    read.id = replay_pending ? "0" : ">";
    turbo_mutex_lock(&adapter->lock);
    rc = flow_redis_run(adapter, &read);
    if (rc == TURBO_OK && replay_pending && !flow_redis_results_have_entries(&read)) {
      replay_pending = 0;
    }
    if (rc == TURBO_OK) rc = flow_redis_publish_results(adapter, &read);
    turbo_mutex_unlock(&adapter->lock);
    redis_stream_result_free(read.results, read.result_count);
    if (rc != TURBO_OK) break;
    if (tf_timer_wait_for_ms(&adapter->poll_wait, adapter->poll_interval_ms) == TURBO_ESHUTDOWN) {
      break;
    }
  }
}

static int flow_redis_start(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_redis_adapter_t *adapter = (flow_redis_adapter_t *)ctx;
  if (!adapter || !stage || ((adapter->poll_interval_ms > 0) != (stage->is_source != 0)))
    return TURBO_EINVAL;
  atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_OK);
  tf_timer_reset(&adapter->poll_wait);
  if (adapter->poll_interval_ms == 0) return TURBO_OK;
  adapter->flow = flow;
  tstr_freep(&adapter->source_name);
  adapter->source_name = tstr_dup(stage->name);
  if (!adapter->source_name) {
    atomic_store_explicit(&adapter->started, 0, memory_order_release);
    return TURBO_ENOMEM;
  }
  if (turbo_thread_create(&adapter->thread, flow_redis_source_thread, adapter) != TURBO_OK) {
    atomic_store_explicit(&adapter->started, 0, memory_order_release);
    return TURBO_EINVAL;
  }
  adapter->thread_started = 1;
  return TURBO_OK;
}

static int flow_redis_consume(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                              turbo_flow_msg_t *msg) {
  flow_redis_adapter_t *adapter = (flow_redis_adapter_t *)ctx;
  const turbo_flow_protocol_settlement_envelope_t *settlement;
  flow_redis_task_t task;
  int rc;
  (void)stage;
  if (!adapter || !msg || adapter->poll_interval_ms > 0 ||
      !atomic_load_explicit(&adapter->started, memory_order_acquire))
    return TURBO_EINVAL;
  if (atomic_load_explicit(&adapter->quiesced, memory_order_acquire)) return TURBO_EBUSY;
  if (adapter->mode == FLOW_REDIS_MODE_DATA_SET && msg->payload.len > adapter->max_value_size) {
    return TURBO_EMSGSIZE;
  }
  settlement = turbo_flow_msg_protocol_settlement(msg);
  if (settlement &&
      (adapter->mode != FLOW_REDIS_MODE_STREAM_SINK || settlement->settled_point != 0 ||
       settlement->requested_point != TURBO_FLOW_PROTOCOL_SETTLE_DURABLE))
    return TURBO_ENOTSUP;
  memset(&task, 0, sizeof(task));
  task.kind = adapter->mode == FLOW_REDIS_MODE_STREAM_SINK ? FLOW_REDIS_TASK_XADD
              : adapter->mode == FLOW_REDIS_MODE_DATA_SET  ? FLOW_REDIS_TASK_SET
                                                           : FLOW_REDIS_TASK_GET;
  task.payload = msg->payload.data ? msg->payload.data : "";
  task.payload_len = msg->payload.len;
  turbo_mutex_lock(&adapter->lock);
  rc = flow_redis_run(adapter, &task);
  turbo_mutex_unlock(&adapter->lock);
  if (rc == TURBO_OK && adapter->mode == FLOW_REDIS_MODE_DATA_GET) {
    turbo_flow_msg_clear_content(msg);
    tstr_freep(&msg->owned_payload);
    mem_buffer_release(msg->buffer);
    msg->buffer = NULL;
    msg->owned_payload = task.response;
    task.response = NULL;
    msg->payload = tstr_to_v(msg->owned_payload);
  }
  if (rc == TURBO_OK && settlement) {
    const turbo_flow_protocol_route_t *route = turbo_flow_msg_protocol_route(msg);
    turbo_flow_protocol_settlement_request_t request =
        TURBO_FLOW_PROTOCOL_SETTLEMENT_REQUEST_INIT;
    if (!route) rc = TURBO_EPROTO;
    else {
      request.message = settlement->message;
      request.point = settlement->requested_point;
      request.status = TURBO_OK;
      request.message_id = msg->id;
      request.attempt = msg->execution_attempt ? msg->execution_attempt : 1u;
      rc = turbo_flow_protocol_route_settle(flow, route, &request);
      if (rc == TURBO_OK)
        rc = turbo_flow_msg_complete_protocol_settlement(msg, request.point);
    }
  }
  tstr_freep(&task.response);
  return rc;
}

static void flow_redis_stop(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_redis_adapter_t *adapter = (flow_redis_adapter_t *)ctx;
  (void)flow;
  (void)stage;
  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CLOSING, TURBO_OK);
  if (adapter->poll_wait_initialized) tf_timer_stop(&adapter->poll_wait);
  (void)redis_client_interrupt(adapter->client, TURBO_ESHUTDOWN);
  if (adapter->thread_started) {
    (void)turbo_thread_join(&adapter->thread);
    adapter->thread_started = 0;
  }
  redis_client_disconnect(adapter->client);
  adapter->connected = 0;
  tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_STOPPED, TURBO_ESHUTDOWN);
}

static int flow_redis_command(void *ctx, turbo_flow_t *flow,
                              const turbo_flow_adapter_command_t *command) {
  flow_redis_adapter_t *adapter = (flow_redis_adapter_t *)ctx;
  (void)flow;
  if (!adapter || !command || !atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    return TURBO_EINVAL;
  }
  if (adapter->poll_interval_ms > 0) return TURBO_ENOTSUP;
  switch (command->kind) {
  case TURBO_FLOW_ADAPTER_QUIESCE:
    atomic_store_explicit(&adapter->quiesced, 1, memory_order_release);
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_RESUME:
    atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT:
    return TURBO_ENOTSUP;
  default:
    return TURBO_EINVAL;
  }
}

static void flow_redis_shutdown(void *ctx) {
  flow_redis_adapter_t *adapter = (flow_redis_adapter_t *)ctx;
  if (!adapter) return;
  flow_redis_stop(adapter, NULL, NULL);
  redis_client_destroy(adapter->client);
  coro_context_destroy(adapter->context);
  if (adapter->poll_wait_initialized) tf_timer_destroy(&adapter->poll_wait);
  if (adapter->lock_initialized) turbo_mutex_destroy(&adapter->lock);
  tstr_freep(&adapter->source_name);
  tstr_freep(&adapter->stream);
  tstr_freep(&adapter->field);
  tstr_freep(&adapter->group);
  tstr_freep(&adapter->consumer);
  tstr_freep(&adapter->group_start_id);
  tstr_freep(&adapter->key);
  if (adapter->record_mutation_keys_initialized)
    turbo_hash_map_destroy(&adapter->record_mutation_keys);
  free(adapter);
}

static flow_redis_adapter_t *flow_redis_adapter_create(const char *host, uint16_t port,
                                                       const char *username, const char *password,
                                                       int database, uint32_t timeout_ms,
                                                       int *status_out) {
  flow_redis_adapter_t *adapter;
  redis_config_t redis_config;
  char endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
  int written;
  if (!status_out) return NULL;
  if (!host || !host[0] || port == 0 || (username && !password)) {
    *status_out = TURBO_EINVAL;
    return NULL;
  }
  *status_out = TURBO_ENOMEM;
  adapter = (flow_redis_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return NULL;
  atomic_init(&adapter->started, 0);
  atomic_init(&adapter->quiesced, 0);
  written =
      snprintf(endpoint, sizeof(endpoint), "redis://%s:%u/%d", host, (unsigned int)port, database);
  if (written < 0 || (size_t)written >= sizeof(endpoint) ||
      tf_connection_init(&adapter->connection, endpoint, 1u) != TURBO_OK) {
    *status_out = TURBO_ENOSPC;
    free(adapter);
    return NULL;
  }
  adapter->context = coro_context_create(NULL);
  memset(&redis_config, 0, sizeof(redis_config));
  redis_config.host = host;
  redis_config.port = port;
  redis_config.username = username;
  redis_config.password = password;
  redis_config.database = database;
  redis_config.timeout_ms = timeout_ms ? timeout_ms : FLOW_REDIS_DEFAULT_TIMEOUT_MS;
  redis_config.command_timeout_ms = redis_config.timeout_ms;
  adapter->client = redis_client_create_with_config(&redis_config);
  if (!adapter->context || !adapter->client) {
    flow_redis_shutdown(adapter);
    return NULL;
  }
  turbo_mutex_init(&adapter->lock);
  adapter->lock_initialized = 1;
  if (tf_timer_init(&adapter->poll_wait) != TURBO_OK) {
    flow_redis_shutdown(adapter);
    return NULL;
  }
  adapter->poll_wait_initialized = 1;
  *status_out = TURBO_OK;
  return adapter;
}

static void flow_redis_claim_record_clear(flow_redis_claim_record_t *record) {
  if (!record) return;
  tstr_freep(&record->id);
  tstr_freep(&record->payload);
  memset(record, 0, sizeof(*record));
}

static int flow_redis_stream_owner_read(turbo_flow_redis_stream_owner_t *owner, const char *id,
                                        flow_redis_claim_record_t *record) {
  flow_redis_task_t read;
  const redis_stream_entry_t *selected = NULL;
  size_t field_index = 0u;
  int saw_entry = 0;
  int rc;
  if (!record || record->occupied) return TURBO_EPROTO;
  memset(&read, 0, sizeof(read));
  read.kind = FLOW_REDIS_TASK_XREADGROUP;
  read.id = id;
  rc = flow_redis_run(owner->adapter, &read);
  if (rc != TURBO_OK) goto done;
  for (size_t i = 0u; i < read.result_count && !selected; ++i) {
    for (size_t j = 0u; j < read.results[i].entry_count; ++j) {
      saw_entry = 1;
      if (flow_redis_entries_field(&read.results[i].entries[j], owner->adapter->field,
                                   &field_index)) {
        selected = &read.results[i].entries[j];
        break;
      }
    }
  }
  if (!selected) {
    rc = saw_entry ? TURBO_EPROTO : TURBO_ENOENT;
    goto done;
  }
  record->id = tstr_dup(selected->id);
  record->payload = tstr_new_len(selected->values[field_index], selected->value_lens[field_index]);
  if (!record->id || !record->payload) {
    flow_redis_claim_record_clear(record);
    rc = TURBO_ENOMEM;
    goto done;
  }
  record->occupied = 1;
  record->order = ++owner->order;
  rc = TURBO_OK;

done:
  redis_stream_result_free(read.results, read.result_count);
  return rc;
}

int turbo_flow_redis_stream_owner_create_ex(
    const turbo_flow_redis_stream_config_t *config,
    const turbo_flow_redis_stream_claim_owner_config_t *claim_config,
    turbo_flow_redis_stream_owner_t **out) {
  turbo_flow_redis_stream_owner_t *owner;
  flow_redis_adapter_t *adapter;
  flow_redis_task_t group;
  int rc;
  if (out) *out = NULL;
  if (!out || !config || !claim_config || claim_config->size < sizeof(*claim_config) ||
      claim_config->version != TURBO_FLOW_REDIS_STREAM_CLAIM_OWNER_API_VERSION ||
      claim_config->max_active_claims == 0u ||
      claim_config->max_active_claims > TURBO_FLOW_REDIS_STREAM_MAX_ACTIVE_CLAIMS ||
      !config->host || !config->host[0] || config->port == 0u || !config->stream ||
      !config->stream[0] || !config->group || !config->group[0] || !config->consumer ||
      !config->consumer[0] || config->database < 0 || config->database > 15 ||
      config->timeout_ms > INT_MAX || config->block_ms > TURBO_FLOW_REDIS_MAX_BLOCK_MS) {
    return TURBO_EINVAL;
  }
  adapter = flow_redis_adapter_create(config->host, config->port, config->username,
                                      config->password, config->database, config->timeout_ms, &rc);
  if (!adapter) return rc;
  adapter->stream = tstr_dup(config->stream);
  adapter->field = tstr_dup(config->field ? config->field : "payload");
  adapter->group = tstr_dup(config->group);
  adapter->consumer = tstr_dup(config->consumer);
  adapter->group_start_id = tstr_dup(config->group_start_id ? config->group_start_id : "$");
  adapter->mode = FLOW_REDIS_MODE_STREAM_SOURCE;
  adapter->read_count = 1u;
  adapter->block_ms = config->block_ms ? config->block_ms : FLOW_REDIS_DEFAULT_BLOCK_MS;
  adapter->create_group = config->create_group != 0;
  adapter->max_value_size = TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE;
  if (!adapter->stream || !adapter->field || !adapter->group || !adapter->consumer ||
      !adapter->group_start_id) {
    flow_redis_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  if (adapter->create_group) {
    memset(&group, 0, sizeof(group));
    group.kind = FLOW_REDIS_TASK_XGROUP;
    rc = flow_redis_run(adapter, &group);
    if (rc != TURBO_OK) {
      flow_redis_shutdown(adapter);
      return rc;
    }
  }
  owner = (turbo_flow_redis_stream_owner_t *)calloc(1, sizeof(*owner));
  if (!owner) {
    flow_redis_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  owner->adapter = adapter;
  if (flow_redis_claim_records_init(&owner->records) != TURBO_OK ||
      flow_redis_claim_slots_init(&owner->free_slots) != TURBO_OK ||
      flow_redis_claim_index_init(&owner->claim_index) != TURBO_OK ||
      flow_redis_requeued_init(&owner->requeued) != TURBO_OK ||
      flow_redis_claim_records_reserve(&owner->records, claim_config->max_active_claims) !=
          TURBO_OK ||
      flow_redis_claim_slots_reserve(&owner->free_slots, claim_config->max_active_claims) !=
          TURBO_OK ||
      turbo_hash_map_reserve(&owner->claim_index.raw, claim_config->max_active_claims) !=
          TURBO_OK ||
      turbo_heap_reserve(&owner->requeued.raw, claim_config->max_active_claims) != TURBO_OK ||
      turbo_vec_resize(&owner->records.raw, claim_config->max_active_claims) != TURBO_OK) {
    turbo_flow_redis_stream_owner_destroy(owner);
    return TURBO_ENOMEM;
  }
  memset(flow_redis_claim_records_data(&owner->records), 0,
         claim_config->max_active_claims * sizeof(flow_redis_claim_record_t));
  for (size_t i = claim_config->max_active_claims; i > 0u; --i) {
    if (flow_redis_claim_slots_push(&owner->free_slots, i - 1u) != TURBO_OK) {
      turbo_flow_redis_stream_owner_destroy(owner);
      return TURBO_ENOMEM;
    }
  }
  owner->replay_cursor = tstr_dup("0");
  if (!owner->replay_cursor) {
    turbo_flow_redis_stream_owner_destroy(owner);
    return TURBO_ENOMEM;
  }
  owner->max_active_claims = claim_config->max_active_claims;
  owner->replay_pending = 1;
  *out = owner;
  return TURBO_OK;
}

int turbo_flow_redis_stream_owner_create(const turbo_flow_redis_stream_config_t *config,
                                         turbo_flow_redis_stream_owner_t **out) {
  turbo_flow_redis_stream_claim_owner_config_t claim_config =
      TURBO_FLOW_REDIS_STREAM_CLAIM_OWNER_CONFIG_INIT;
  return turbo_flow_redis_stream_owner_create_ex(config, &claim_config, out);
}

void turbo_flow_redis_stream_owner_destroy(turbo_flow_redis_stream_owner_t *owner) {
  if (!owner) return;
  for (size_t i = 0u; i < flow_redis_claim_records_size(&owner->records); ++i)
    flow_redis_claim_record_clear(flow_redis_claim_records_at(&owner->records, i));
  flow_redis_claim_records_destroy(&owner->records);
  flow_redis_claim_slots_destroy(&owner->free_slots);
  flow_redis_claim_index_destroy(&owner->claim_index);
  flow_redis_requeued_destroy(&owner->requeued);
  tstr_freep(&owner->replay_cursor);
  flow_redis_shutdown(owner->adapter);
  free(owner);
}

int turbo_flow_redis_stream_owner_claim(turbo_flow_redis_stream_owner_t *owner,
                                        turbo_flow_redis_stream_claim_t *claim) {
  flow_redis_claim_record_t *record = NULL;
  size_t slot;
  uint64_t token;
  int rc;
  if (!owner || !claim || claim->size < sizeof(*claim)) return TURBO_EINVAL;
  if (owner->active_claims >= owner->max_active_claims) return TURBO_EBUSY;
  if (owner->claim_generation == UINT64_MAX) return TURBO_ERANGE;
  if (flow_redis_requeued_pop(&owner->requeued, &record)) {
    slot = (size_t)(record - flow_redis_claim_records_data(&owner->records));
    if (slot >= owner->max_active_claims || !record->occupied || record->active)
      return TURBO_EPROTO;
  } else {
    tstr_t next_cursor = NULL;
    if (!flow_redis_claim_slots_pop(&owner->free_slots, &slot)) return TURBO_EPROTO;
    record = flow_redis_claim_records_at(&owner->records, slot);
    if (!record || record->occupied) {
      (void)flow_redis_claim_slots_push(&owner->free_slots, slot);
      return TURBO_EPROTO;
    }
    if (owner->replay_pending) {
      rc = flow_redis_stream_owner_read(owner, owner->replay_cursor, record);
      if (rc == TURBO_OK) {
        next_cursor = tstr_dup(record->id);
        if (!next_cursor) rc = TURBO_ENOMEM;
      } else if (rc == TURBO_ENOENT) {
        owner->replay_pending = 0;
        rc = flow_redis_stream_owner_read(owner, ">", record);
      }
    } else {
      rc = flow_redis_stream_owner_read(owner, ">", record);
    }
    if (rc != TURBO_OK) {
      flow_redis_claim_record_clear(record);
      (void)flow_redis_claim_slots_push(&owner->free_slots, slot);
      tstr_freep(&next_cursor);
      return rc;
    }
    if (next_cursor) {
      tstr_freep(&owner->replay_cursor);
      owner->replay_cursor = next_cursor;
    }
  }
  token = owner->claim_generation + 1u;
  if (flow_redis_claim_index_put(&owner->claim_index, token, slot) != TURBO_OK) {
    if (flow_redis_requeued_push(&owner->requeued, record) != TURBO_OK) return TURBO_EPROTO;
    return TURBO_ENOMEM;
  }
  owner->claim_generation = token;
  owner->active_claims += 1u;
  record->active = 1;
  record->ambiguous_ack = 0;
  record->token = token;
  *claim = (turbo_flow_redis_stream_claim_t)TURBO_FLOW_REDIS_STREAM_CLAIM_INIT;
  claim->token = token;
  claim->entry_id = record->id;
  claim->payload = tstr_to_v(record->payload);
  return TURBO_OK;
}

static int flow_redis_stream_owner_finish_claim(turbo_flow_redis_stream_owner_t *owner,
                                                flow_redis_claim_record_t *record, size_t slot,
                                                uint64_t token) {
  if (!flow_redis_claim_index_remove(&owner->claim_index, token, NULL)) return TURBO_EPROTO;
  owner->active_claims -= 1u;
  flow_redis_claim_record_clear(record);
  return flow_redis_claim_slots_push(&owner->free_slots, slot) == TURBO_OK ? TURBO_OK
                                                                           : TURBO_EPROTO;
}

static int flow_redis_stream_owner_requeue_memory(turbo_flow_redis_stream_owner_t *owner,
                                                  flow_redis_claim_record_t *record, size_t slot,
                                                  uint64_t token) {
  if (!flow_redis_claim_index_remove(&owner->claim_index, token, NULL)) return TURBO_EPROTO;
  record->active = 0;
  record->token = 0u;
  record->ambiguous_ack = 0;
  owner->active_claims -= 1u;
  if (flow_redis_requeued_push(&owner->requeued, record) != TURBO_OK) {
    record->active = 1;
    record->token = token;
    owner->active_claims += 1u;
    (void)flow_redis_claim_index_put(&owner->claim_index, token, slot);
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int flow_redis_stream_owner_reconcile_ack(turbo_flow_redis_stream_owner_t *owner,
                                                 flow_redis_claim_record_t *record, int *present) {
  flow_redis_task_t pending;
  int rc;
  memset(&pending, 0, sizeof(pending));
  pending.kind = FLOW_REDIS_TASK_XPENDING_EXACT;
  pending.id = record->id;
  rc = flow_redis_run(owner->adapter, &pending);
  if (rc != TURBO_OK) return rc;
  *present = pending.present;
  return TURBO_OK;
}

int turbo_flow_redis_stream_owner_ack(turbo_flow_redis_stream_owner_t *owner, uint64_t token) {
  flow_redis_claim_record_t *record;
  const size_t *found;
  size_t slot;
  flow_redis_task_t ack;
  int present;
  int rc;
  if (!owner || token == 0u) return TURBO_EINVAL;
  found = flow_redis_claim_index_get_const(&owner->claim_index, token);
  if (!found) return TURBO_EALREADY;
  slot = *found;
  record = flow_redis_claim_records_at(&owner->records, slot);
  if (!record || !record->occupied || !record->active || record->token != token)
    return TURBO_EPROTO;
  if (record->ambiguous_ack) {
    rc = flow_redis_stream_owner_reconcile_ack(owner, record, &present);
    if (rc != TURBO_OK) return rc;
    if (!present) return flow_redis_stream_owner_finish_claim(owner, record, slot, token);
    record->ambiguous_ack = 0;
  }
  memset(&ack, 0, sizeof(ack));
  ack.kind = FLOW_REDIS_TASK_XACK;
  ack.id = record->id;
  rc = flow_redis_run(owner->adapter, &ack);
  if (rc == TURBO_OK) {
    return flow_redis_stream_owner_finish_claim(owner, record, slot, token);
  }
  if (ack.outcome == REDIS_COMMAND_SEND_UNCERTAIN || ack.outcome == REDIS_COMMAND_REPLY_UNKNOWN) {
    record->ambiguous_ack = 1;
  }
  return rc;
}

int turbo_flow_redis_stream_owner_drop(turbo_flow_redis_stream_owner_t *owner, uint64_t token) {
  return turbo_flow_redis_stream_owner_ack(owner, token);
}

int turbo_flow_redis_stream_owner_requeue(turbo_flow_redis_stream_owner_t *owner, uint64_t token) {
  flow_redis_claim_record_t *record;
  const size_t *found;
  size_t slot;
  int present;
  int rc;
  if (!owner || token == 0u) return TURBO_EINVAL;
  found = flow_redis_claim_index_get_const(&owner->claim_index, token);
  if (!found) return TURBO_EALREADY;
  slot = *found;
  record = flow_redis_claim_records_at(&owner->records, slot);
  if (!record || !record->occupied || !record->active || record->token != token)
    return TURBO_EPROTO;
  if (record->ambiguous_ack) {
    rc = flow_redis_stream_owner_reconcile_ack(owner, record, &present);
    if (rc != TURBO_OK) return rc;
    if (!present) {
      rc = flow_redis_stream_owner_finish_claim(owner, record, slot, token);
      return rc == TURBO_OK ? TURBO_EALREADY : rc;
    }
    record->ambiguous_ack = 0;
  }
  return flow_redis_stream_owner_requeue_memory(owner, record, slot, token);
}

static int flow_redis_stream_owner_load_state(void *ctx, const char *key, uint8_t *out,
                                              size_t capacity, size_t *out_size) {
  turbo_flow_redis_stream_owner_t *owner = (turbo_flow_redis_stream_owner_t *)ctx;
  flow_redis_task_t task;
  size_t size;
  int rc;
  if (!owner || !key || !key[0] || strlen(key) > TURBO_FLOW_REDIS_MAX_KEY_SIZE || !out_size ||
      (!out && capacity > 0u)) {
    return TURBO_EINVAL;
  }
  memset(&task, 0, sizeof(task));
  task.kind = FLOW_REDIS_TASK_STATE_GET;
  task.state_key = key;
  turbo_mutex_lock(&owner->adapter->lock);
  rc = flow_redis_run(owner->adapter, &task);
  turbo_mutex_unlock(&owner->adapter->lock);
  if (rc != TURBO_OK) {
    tstr_freep(&task.response);
    return rc;
  }
  size = tstr_len(task.response);
  *out_size = size;
  if (size > capacity) {
    tstr_freep(&task.response);
    return TURBO_ENOSPC;
  }
  if (size > 0u) memcpy(out, task.response, size);
  tstr_freep(&task.response);
  return TURBO_OK;
}

static int flow_redis_stream_owner_commit_state(void *ctx, uint64_t token,
                                                turbo_flow_claim_commit_action_t action,
                                                const char *key, const uint8_t *state,
                                                size_t state_size) {
  turbo_flow_redis_stream_owner_t *owner = (turbo_flow_redis_stream_owner_t *)ctx;
  flow_redis_claim_record_t *record = NULL;
  const size_t *found = NULL;
  size_t slot = 0u;
  flow_redis_task_t task;
  int rc;
  if (!owner || !key || !key[0] || strlen(key) > TURBO_FLOW_REDIS_MAX_KEY_SIZE ||
      (!state && state_size > 0u) || state_size > owner->adapter->max_value_size ||
      action < TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY || action > TURBO_FLOW_CLAIM_COMMIT_DROP ||
      ((action == TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY) != (token == 0u))) {
    return TURBO_EINVAL;
  }
  if (token != 0u) {
    found = flow_redis_claim_index_get_const(&owner->claim_index, token);
    if (!found) return TURBO_EALREADY;
    slot = *found;
    record = flow_redis_claim_records_at(&owner->records, slot);
    if (!record || !record->occupied || !record->active || record->token != token)
      return TURBO_EPROTO;
  }
  memset(&task, 0, sizeof(task));
  task.kind = FLOW_REDIS_TASK_STATE_COMMIT;
  task.state_key = key;
  task.commit_action = action;
  task.id = record ? record->id : NULL;
  task.payload = state_size > 0u ? (const char *)state : "";
  task.payload_len = state_size;
  turbo_mutex_lock(&owner->adapter->lock);
  rc = flow_redis_run(owner->adapter, &task);
  turbo_mutex_unlock(&owner->adapter->lock);
  if (rc != TURBO_OK) return rc;
  if (action == TURBO_FLOW_CLAIM_COMMIT_ACK || action == TURBO_FLOW_CLAIM_COMMIT_DROP)
    return flow_redis_stream_owner_finish_claim(owner, record, slot, token);
  if (action == TURBO_FLOW_CLAIM_COMMIT_REQUEUE)
    return flow_redis_stream_owner_requeue_memory(owner, record, slot, token);
  return TURBO_OK;
}

static int flow_redis_stream_owner_ack_adapter(void *ctx, uint64_t token) {
  return turbo_flow_redis_stream_owner_ack((turbo_flow_redis_stream_owner_t *)ctx, token);
}

static int flow_redis_stream_owner_requeue_adapter(void *ctx, uint64_t token) {
  return turbo_flow_redis_stream_owner_requeue((turbo_flow_redis_stream_owner_t *)ctx, token);
}

static int flow_redis_stream_owner_drop_adapter(void *ctx, uint64_t token) {
  return turbo_flow_redis_stream_owner_drop((turbo_flow_redis_stream_owner_t *)ctx, token);
}

int turbo_flow_redis_stream_owner_settler(turbo_flow_redis_stream_owner_t *owner,
                                          turbo_flow_claim_settler_t *out) {
  if (!owner || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  *out = (turbo_flow_claim_settler_t)TURBO_FLOW_CLAIM_SETTLER_INIT;
  out->ctx = owner;
  out->ack = flow_redis_stream_owner_ack_adapter;
  out->requeue = flow_redis_stream_owner_requeue_adapter;
  out->drop = flow_redis_stream_owner_drop_adapter;
  out->max_state_size = owner->adapter->max_value_size;
  out->load_state = flow_redis_stream_owner_load_state;
  out->commit_state = flow_redis_stream_owner_commit_state;
  return TURBO_OK;
}

static int flow_redis_register_initialized(turbo_flow_t *flow, const char *name,
                                           flow_redis_adapter_t *adapter,
                                           const turbo_flow_adapter_schema_t *schema) {
  turbo_flow_adapter_ops_t ops;
  int rc;
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_redis_start;
  ops.consume = flow_redis_consume;
  ops.stop = flow_redis_stop;
  ops.shutdown = flow_redis_shutdown;
  ops.connection_snapshot = flow_redis_connection_snapshot;
  ops.command = flow_redis_command;
  rc = turbo_flow_register_adapter_ex(flow, name, &ops, adapter, schema);
  if (rc != TURBO_OK) flow_redis_shutdown(adapter);
  return rc;
}

int turbo_flow_redis_register_stream_adapter(turbo_flow_t *flow, const char *name,
                                             const turbo_flow_redis_stream_config_t *config) {
  flow_redis_adapter_t *adapter;
  int rc;
  if (!flow || !name || !*name || !config || !config->host || !*config->host || config->port == 0 ||
      !config->stream || !*config->stream || (config->database < 0 || config->database > 15) ||
      config->timeout_ms > INT_MAX || config->block_ms > TURBO_FLOW_REDIS_MAX_BLOCK_MS ||
      (config->poll_interval_ms > 0 &&
       (!config->group || !*config->group || !config->consumer || !*config->consumer)))
    return TURBO_EINVAL;
  adapter = flow_redis_adapter_create(config->host, config->port, config->username,
                                      config->password, config->database, config->timeout_ms, &rc);
  if (!adapter) return rc;
  adapter->stream = tstr_dup(config->stream);
  adapter->field = tstr_dup(config->field ? config->field : "payload");
  adapter->group = config->group ? tstr_dup(config->group) : NULL;
  adapter->consumer = config->consumer ? tstr_dup(config->consumer) : NULL;
  adapter->group_start_id = tstr_dup(config->group_start_id ? config->group_start_id : "$");
  adapter->mode =
      config->poll_interval_ms > 0 ? FLOW_REDIS_MODE_STREAM_SOURCE : FLOW_REDIS_MODE_STREAM_SINK;
  adapter->maxlen = config->maxlen;
  adapter->poll_interval_ms = config->poll_interval_ms;
  adapter->read_count = config->read_count ? config->read_count : FLOW_REDIS_DEFAULT_READ_COUNT;
  adapter->block_ms = config->block_ms ? config->block_ms : FLOW_REDIS_DEFAULT_BLOCK_MS;
  adapter->create_group = config->create_group != 0;
  if (!adapter->stream || !adapter->field || !adapter->group_start_id ||
      (config->group && !adapter->group) || (config->consumer && !adapter->consumer)) {
    flow_redis_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  return flow_redis_register_initialized(flow, name, adapter,
                                         config->poll_interval_ms > 0 ? &FLOW_REDIS_INPUT_SCHEMA
                                                                      : &FLOW_REDIS_OUTPUT_SCHEMA);
}

int turbo_flow_redis_register_data_adapter(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_redis_data_config_t *config) {
  flow_redis_adapter_t *adapter;
  size_t max_value_size;
  int rc;
  if (!flow || !name || !name[0] || !config || !config->host || !config->host[0] ||
      config->port == 0u || config->database < 0 || config->database > 15 ||
      config->timeout_ms > INT_MAX || !config->key || !config->key[0] ||
      strlen(config->key) > TURBO_FLOW_REDIS_MAX_KEY_SIZE ||
      config->operation < TURBO_FLOW_REDIS_DATA_SET ||
      config->operation > TURBO_FLOW_REDIS_DATA_GET ||
      config->max_value_size > TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE) {
    return TURBO_EINVAL;
  }
  max_value_size =
      config->max_value_size ? config->max_value_size : TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE;
  adapter = flow_redis_adapter_create(config->host, config->port, config->username,
                                      config->password, config->database, config->timeout_ms, &rc);
  if (!adapter) return rc;
  adapter->key = tstr_dup(config->key);
  adapter->mode = config->operation == TURBO_FLOW_REDIS_DATA_SET ? FLOW_REDIS_MODE_DATA_SET
                                                                 : FLOW_REDIS_MODE_DATA_GET;
  adapter->max_value_size = max_value_size;
  if (!adapter->key) {
    flow_redis_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  return flow_redis_register_initialized(flow, name, adapter,
                                         config->operation == TURBO_FLOW_REDIS_DATA_SET
                                             ? &FLOW_REDIS_DATA_SET_SCHEMA
                                             : &FLOW_REDIS_DATA_GET_SCHEMA);
}

static int flow_redis_blob_store_key(const flow_redis_adapter_t *adapter, const char *key) {
  if (!adapter || !adapter->key || !key || strcmp(adapter->key, key) != 0) return TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_redis_blob_store_load(void *ctx, const char *key, uint8_t *out, size_t capacity,
                                      size_t *out_size) {
  flow_redis_adapter_t *adapter = (flow_redis_adapter_t *)ctx;
  flow_redis_task_t task;
  int rc;
  if (!out_size || (!out && capacity > 0u)) return TURBO_EINVAL;
  *out_size = 0u;
  rc = flow_redis_blob_store_key(adapter, key);
  if (rc != TURBO_OK) return rc;
  memset(&task, 0, sizeof(task));
  task.kind = FLOW_REDIS_TASK_GET;
  turbo_mutex_lock(&adapter->lock);
  rc = flow_redis_run(adapter, &task);
  turbo_mutex_unlock(&adapter->lock);
  if (rc != TURBO_OK) {
    tstr_freep(&task.response);
    return rc;
  }
  *out_size = tstr_len(task.response);
  if (!out || capacity < *out_size) {
    tstr_freep(&task.response);
    return TURBO_ENOSPC;
  }
  if (*out_size > 0u) memcpy(out, task.response, *out_size);
  tstr_freep(&task.response);
  return TURBO_OK;
}

static int flow_redis_blob_store_commit(void *ctx, const char *key, const uint8_t *data,
                                        size_t data_size) {
  flow_redis_adapter_t *adapter = (flow_redis_adapter_t *)ctx;
  flow_redis_task_t task;
  int rc = flow_redis_blob_store_key(adapter, key);
  if (rc != TURBO_OK || (!data && data_size > 0u)) return rc != TURBO_OK ? rc : TURBO_EINVAL;
  if (data_size > adapter->max_value_size) return TURBO_EMSGSIZE;
  memset(&task, 0, sizeof(task));
  task.kind = FLOW_REDIS_TASK_SET;
  task.payload = data_size > 0u ? (const char *)data : "";
  task.payload_len = data_size;
  turbo_mutex_lock(&adapter->lock);
  rc = flow_redis_run(adapter, &task);
  turbo_mutex_unlock(&adapter->lock);
  return rc;
}

int turbo_flow_redis_blob_store_create(const turbo_flow_redis_blob_store_config_t *config,
                                       turbo_flow_blob_store_t *out) {
  flow_redis_adapter_t *adapter;
  size_t max_value_size;
  int rc;
  if (!config || !out || out->size < sizeof(*out) || out->ctx || !config->host ||
      !config->host[0] || config->port == 0u || config->database < 0 || config->database > 15 ||
      config->timeout_ms > INT_MAX || !config->key || !config->key[0] ||
      strlen(config->key) > TURBO_FLOW_REDIS_MAX_KEY_SIZE ||
      config->max_value_size > TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE) {
    return TURBO_EINVAL;
  }
  max_value_size =
      config->max_value_size ? config->max_value_size : TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE;
  adapter = flow_redis_adapter_create(config->host, config->port, config->username,
                                      config->password, config->database, config->timeout_ms, &rc);
  if (!adapter) return rc;
  adapter->key = tstr_dup(config->key);
  adapter->max_value_size = max_value_size;
  adapter->mode = FLOW_REDIS_MODE_DATA_SET;
  if (!adapter->key) {
    flow_redis_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  out->max_value_size = max_value_size;
  out->ctx = adapter;
  out->load = flow_redis_blob_store_load;
  out->commit = flow_redis_blob_store_commit;
  return TURBO_OK;
}

void turbo_flow_redis_blob_store_destroy(turbo_flow_blob_store_t *store) {
  if (!store || store->size < sizeof(*store)) return;
  flow_redis_shutdown(store->ctx);
  *store = (turbo_flow_blob_store_t)TURBO_FLOW_BLOB_STORE_INIT;
}

static size_t flow_redis_record_key_hash(const void *key, size_t key_size, void *ctx) {
  const tstr_v *view = (const tstr_v *)key;
  (void)key_size;
  (void)ctx;
  return turbo_hash_bytes(view->data, view->len, NULL);
}

static bool flow_redis_record_key_equal(const void *left, const void *right, size_t key_size,
                                        void *ctx) {
  const tstr_v *a = (const tstr_v *)left;
  const tstr_v *b = (const tstr_v *)right;
  (void)key_size;
  (void)ctx;
  return a->len == b->len && (a->len == 0u || memcmp(a->data, b->data, a->len) == 0);
}

static int flow_redis_record_mutations_validate(
    flow_redis_adapter_t *adapter, const turbo_flow_record_mutation_t *mutations,
    size_t mutation_count) {
  static const uint8_t present = 1u;
  if (!adapter || !mutations || mutation_count == 0u ||
      mutation_count > adapter->record_max_batch_size)
    return TURBO_EINVAL;
  turbo_hash_map_clear(&adapter->record_mutation_keys);
  for (size_t i = 0u; i < mutation_count; ++i) {
    const turbo_flow_record_mutation_t *mutation = &mutations[i];
    tstr_v key;
    int rc;
    if (mutation->size < sizeof(*mutation) || !mutation->key || mutation->key_size == 0u ||
        mutation->key_size > adapter->record_max_key_size ||
        mutation->expected_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
        mutation->next_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX)
      return TURBO_EINVAL;
    if (mutation->kind == TURBO_FLOW_RECORD_PUT) {
      if ((!mutation->value && mutation->value_size != 0u) ||
          mutation->value_size > adapter->max_value_size ||
          mutation->next_revision <= mutation->expected_revision)
        return TURBO_EINVAL;
    } else if (mutation->kind == TURBO_FLOW_RECORD_DELETE) {
      if (mutation->expected_revision == TURBO_FLOW_RECORD_REVISION_ABSENT ||
          mutation->next_revision != TURBO_FLOW_RECORD_REVISION_ABSENT || mutation->value ||
          mutation->value_size != 0u)
        return TURBO_EINVAL;
    } else {
      return TURBO_EINVAL;
    }
    key = tstr_v_from_buf((const char *)mutation->key, mutation->key_size);
    if (turbo_hash_map_contains(&adapter->record_mutation_keys, &key)) return TURBO_EINVAL;
    rc = turbo_hash_map_put(&adapter->record_mutation_keys, &key, &present);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int flow_redis_record_store_scan(void *ctx, turbo_flow_record_visit_fn visit,
                                        void *visit_ctx) {
  flow_redis_adapter_t *adapter = (flow_redis_adapter_t *)ctx;
  flow_redis_task_t task;
  int rc;
  if (!adapter || !visit || adapter->mode != FLOW_REDIS_MODE_RECORD_STORE) return TURBO_EINVAL;
  memset(&task, 0, sizeof(task));
  task.kind = FLOW_REDIS_TASK_RECORD_SCAN;
  task.record_visit = visit;
  task.record_visit_ctx = visit_ctx;
  turbo_mutex_lock(&adapter->lock);
  rc = flow_redis_run(adapter, &task);
  turbo_mutex_unlock(&adapter->lock);
  return rc;
}

static int flow_redis_record_store_commit(void *ctx,
                                          const turbo_flow_record_mutation_t *mutations,
                                          size_t mutation_count) {
  flow_redis_adapter_t *adapter = (flow_redis_adapter_t *)ctx;
  flow_redis_task_t task;
  int rc;
  if (!adapter || adapter->mode != FLOW_REDIS_MODE_RECORD_STORE) return TURBO_EINVAL;
  turbo_mutex_lock(&adapter->lock);
  rc = flow_redis_record_mutations_validate(adapter, mutations, mutation_count);
  if (rc == TURBO_OK) {
    memset(&task, 0, sizeof(task));
    task.kind = FLOW_REDIS_TASK_RECORD_COMMIT;
    task.record_mutations = mutations;
    task.record_mutation_count = mutation_count;
    rc = flow_redis_run(adapter, &task);
  }
  turbo_hash_map_clear(&adapter->record_mutation_keys);
  turbo_mutex_unlock(&adapter->lock);
  return rc;
}

int turbo_flow_redis_record_store_create(const turbo_flow_redis_record_store_config_t *config,
                                         turbo_flow_record_store_t *out) {
  flow_redis_adapter_t *adapter;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_batch_size;
  int rc;
  if (!config || !out || out->size < sizeof(*out) || out->ctx || !config->host ||
      !config->host[0] || config->port == 0u || config->database < 0 || config->database > 15 ||
      config->timeout_ms > INT_MAX || !config->key || !config->key[0] ||
      strlen(config->key) > TURBO_FLOW_REDIS_MAX_KEY_SIZE ||
      config->max_record_key_size > TURBO_FLOW_REDIS_RECORD_STORE_MAX_RECORD_KEY_SIZE ||
      config->max_value_size > TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE ||
      config->max_batch_size > UINT16_MAX || config->max_records == 0u ||
      config->max_records > INT_MAX)
    return TURBO_EINVAL;
  max_key_size = config->max_record_key_size
                     ? config->max_record_key_size
                     : TURBO_FLOW_REDIS_RECORD_STORE_MAX_RECORD_KEY_SIZE;
  max_value_size =
      config->max_value_size ? config->max_value_size : TURBO_FLOW_REDIS_DEFAULT_MAX_VALUE_SIZE;
  max_batch_size = config->max_batch_size ? config->max_batch_size
                                          : TURBO_FLOW_REDIS_RECORD_STORE_DEFAULT_MAX_BATCH_SIZE;
  if (max_key_size == 0u || max_value_size == 0u || max_batch_size == 0u) return TURBO_EINVAL;
  adapter = flow_redis_adapter_create(config->host, config->port, config->username,
                                      config->password, config->database, config->timeout_ms, &rc);
  if (!adapter) return rc;
  adapter->key = tstr_dup(config->key);
  adapter->mode = FLOW_REDIS_MODE_RECORD_STORE;
  adapter->record_max_key_size = max_key_size;
  adapter->max_value_size = max_value_size;
  adapter->record_max_batch_size = max_batch_size;
  adapter->record_max_records = config->max_records;
  rc = turbo_hash_map_init(&adapter->record_mutation_keys, sizeof(tstr_v), sizeof(uint8_t),
                           flow_redis_record_key_hash, flow_redis_record_key_equal, NULL);
  if (rc == TURBO_OK) {
    adapter->record_mutation_keys_initialized = 1;
    rc = turbo_hash_map_reserve(&adapter->record_mutation_keys, max_batch_size);
  }
  if (!adapter->key || rc != TURBO_OK) {
    int failure = !adapter->key ? TURBO_ENOMEM : rc;
    flow_redis_shutdown(adapter);
    return failure;
  }
  out->api_version = TURBO_FLOW_RECORD_STORE_API_VERSION;
  out->capabilities = TURBO_FLOW_RECORD_STORE_DURABLE | TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH;
  out->max_key_size = max_key_size;
  out->max_value_size = max_value_size;
  out->max_batch_size = max_batch_size;
  out->max_records = config->max_records;
  out->ctx = adapter;
  out->scan = flow_redis_record_store_scan;
  out->commit = flow_redis_record_store_commit;
  return TURBO_OK;
}

void turbo_flow_redis_record_store_destroy(turbo_flow_record_store_t *store) {
  if (!store || store->size < sizeof(*store) || !store->ctx) return;
  flow_redis_shutdown(store->ctx);
  *store = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
}
