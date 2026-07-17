#include "turbo_flow_queue.h"

#include "fmt.h"
#include "sqlite3.h"
#include "turbo_deque.h"
#include "turbo_error.h"
#include "turbo_hash.h"
#include "turbo_heap.h"
#include "turbo_str.h"
#include "turbo_thread.h"
#include "turbo_vec.h"

#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *const FLOW_QUEUE_POLICY_VALUES[] = {"fail", "block", "drop_oldest"};
static const char *const FLOW_QUEUE_PATTERN_VALUES[] = {"push_pull"};
static const char FLOW_QUEUE_STATUS_SCHEMA_TEXT[] =
    "schema TurboFlowQueueResource [id(1), version(2)];\n"
    "message QueueStatus {\n"
    "  string depth;\n"
    "  string capacity;\n"
    "  string enqueued;\n"
    "  string delivered;\n"
    "  string dropped_oldest;\n"
    "  string enqueue_failures;\n"
    "  string publish_failures;\n"
    "  string accept_acks;\n"
    "  string delivery_acks;\n"
    "  string delivery_requeues;\n"
    "  string delivery_requeue_failures;\n"
    "  bool saturated;\n"
    "}\n";
static const turbo_flow_resource_schema_t FLOW_QUEUE_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
    TURBO_FLOW_RESOURCE_QUEUE_BUFFER,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowQueueResource",
    "QueueStatus",
    2u,
    1u,
    FLOW_QUEUE_STATUS_SCHEMA_TEXT};
static const turbo_flow_option_field_t FLOW_QUEUE_FIELDS[] = {
    {"queue", TURBO_FLOW_OPTION_HOST_OBJECT,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL, 0},
    {"pattern", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0, FLOW_QUEUE_PATTERN_VALUES,
     1},
    {"capacity", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1,
     TURBO_FLOW_QUEUE_MAX_CAPACITY, NULL, 0},
    {"max_payload_size", TURBO_FLOW_OPTION_SIZE,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1,
     TURBO_FLOW_QUEUE_MAX_PAYLOAD_SIZE, NULL, 0},
    {"full_policy", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0,
     FLOW_QUEUE_POLICY_VALUES, 3},
    {"enqueue_timeout_ms", TURBO_FLOW_OPTION_DURATION_MS, TURBO_FLOW_OPTION_HAS_MAX, 0,
     TURBO_FLOW_QUEUE_MAX_TIMEOUT_MS, NULL, 0}};

typedef struct flow_queue_item_s {
  turbo_flow_msg_t msg;
  sqlite3_int64 storage_id;
  uint64_t enqueue_sequence;
} flow_queue_item_t;

TURBO_DEQUE_DEFINE(flow_queue_items, flow_queue_item_t)

static int flow_queue_item_sequence_compare(const void *left, const void *right, void *ctx) {
  const flow_queue_item_t *left_item = (const flow_queue_item_t *)left;
  const flow_queue_item_t *right_item = (const flow_queue_item_t *)right;
  (void)ctx;
  if (left_item->enqueue_sequence < right_item->enqueue_sequence) return -1;
  if (left_item->enqueue_sequence > right_item->enqueue_sequence) return 1;
  return 0;
}

TURBO_HEAP_DEFINE(flow_queue_requeued, flow_queue_item_t, flow_queue_item_sequence_compare)

typedef struct flow_queue_claim_record_s {
  int active;
  uint64_t token;
  flow_queue_item_t item;
} flow_queue_claim_record_t;

TURBO_VEC_DEFINE(flow_queue_claim_records, flow_queue_claim_record_t)
TURBO_VEC_DEFINE(flow_queue_claim_slots, size_t)
TURBO_HASH_MAP_DEFINE(flow_queue_claim_index, uint64_t, size_t)

typedef enum flow_queue_adapter_role_e {
  FLOW_QUEUE_ADAPTER_SOURCE = 1,
  FLOW_QUEUE_ADAPTER_SINK
} flow_queue_adapter_role_t;

typedef struct flow_queue_adapter_s flow_queue_adapter_t;

typedef enum flow_queue_backend_e {
  FLOW_QUEUE_BACKEND_MEMORY = 1,
  FLOW_QUEUE_BACKEND_SQLITE
} flow_queue_backend_t;

struct turbo_flow_queue_s {
  flow_queue_items items;
  flow_queue_requeued requeued;
  flow_queue_claim_records claim_records;
  flow_queue_claim_slots free_claim_slots;
  flow_queue_claim_index claim_index;
  turbo_mutex_t mutex;
  turbo_cond_t not_empty;
  turbo_cond_t not_full;
  size_t capacity;
  size_t max_payload_size;
  turbo_flow_queue_full_policy_t full_policy;
  uint64_t enqueue_timeout_ms;
  tstr_t resource_uid;
  tstr_t owner_name;
  tstr_t channel_name;
  size_t references;
  int source_in_flight;
  uint64_t claim_generation;
  uint64_t enqueue_sequence;
  size_t max_active_claims;
  size_t active_claims;
  flow_queue_adapter_t *active_source;
  flow_queue_backend_t backend;
  sqlite3 *sqlite_db;
  tstr_t sqlite_path;
  tstr_t sqlite_queue_name;
  size_t sqlite_depth;
  size_t sqlite_max_state_size;
  uint64_t enqueued;
  uint64_t delivered;
  uint64_t dropped_oldest;
  uint64_t enqueue_failures;
  uint64_t publish_failures;
  uint64_t delivery_requeues;
  uint64_t delivery_requeue_failures;
};

struct flow_queue_adapter_s {
  turbo_flow_queue_t *queue;
  turbo_flow_t *flow;
  tstr_t source_name;
  flow_queue_adapter_role_t role;
  atomic_int started;
  int thread_started;
  turbo_thread_t thread;
};

static int flow_queue_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out);
static void flow_queue_snapshot_locked(const turbo_flow_queue_t *queue,
                                       turbo_flow_queue_snapshot_t *out);
static void flow_queue_ack_snapshot_locked(const turbo_flow_queue_t *queue,
                                           turbo_flow_queue_ack_snapshot_t *out);

#define FLOW_QUEUE_SQLITE_SCHEMA_VERSION 2u

static const char FLOW_QUEUE_SQLITE_META_SCHEMA[] =
    "CREATE TABLE IF NOT EXISTS turbo_flow_queue_schema ("
    "schema_name TEXT PRIMARY KEY NOT NULL,version INTEGER NOT NULL);";

static const char FLOW_QUEUE_SQLITE_SCHEMA[] =
    "CREATE TABLE IF NOT EXISTS turbo_flow_queue_messages ("
    "sequence INTEGER PRIMARY KEY AUTOINCREMENT,"
    "queue_name TEXT NOT NULL,"
    "message_id INTEGER NOT NULL,"
    "timestamp_ns INTEGER NOT NULL,"
    "message_type INTEGER NOT NULL,"
    "flags INTEGER NOT NULL,"
    "status INTEGER NOT NULL,"
    "payload BLOB NOT NULL,"
    "state INTEGER NOT NULL CHECK(state IN (0, 1))"
    ");"
    "CREATE INDEX IF NOT EXISTS turbo_flow_queue_pending "
    "ON turbo_flow_queue_messages(queue_name, state, sequence);"
    "CREATE TABLE IF NOT EXISTS turbo_flow_queue_claim_state ("
    "queue_name TEXT NOT NULL,"
    "state_key TEXT NOT NULL,"
    "value BLOB NOT NULL,"
    "PRIMARY KEY(queue_name,state_key)"
    ");";

static int flow_queue_sqlite_status(int status) {
  return status == SQLITE_BUSY || status == SQLITE_LOCKED ? TURBO_EBUSY : TURBO_EIO;
}

static void flow_queue_sqlite_rollback(turbo_flow_queue_t *queue) {
  if (queue && queue->sqlite_db) (void)sqlite3_exec(queue->sqlite_db, "ROLLBACK", NULL, NULL, NULL);
}

static int flow_queue_sqlite_begin(turbo_flow_queue_t *queue) {
  int status = sqlite3_exec(queue->sqlite_db, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  return status == SQLITE_OK ? TURBO_OK : flow_queue_sqlite_status(status);
}

static int flow_queue_sqlite_commit(turbo_flow_queue_t *queue) {
  int status = sqlite3_exec(queue->sqlite_db, "COMMIT", NULL, NULL, NULL);
  return status == SQLITE_OK ? TURBO_OK : flow_queue_sqlite_status(status);
}

static int flow_queue_sqlite_initialize(turbo_flow_queue_t *queue, int busy_timeout_ms) {
  sqlite3_stmt *stmt = NULL;
  int schema_version = 0;
  int status;
  if (!queue || !queue->sqlite_path || !queue->sqlite_queue_name || busy_timeout_ms < 0) {
    return TURBO_EINVAL;
  }
  status =
      sqlite3_open_v2(queue->sqlite_path, &queue->sqlite_db,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
  if (status != SQLITE_OK) return flow_queue_sqlite_status(status);
  if (busy_timeout_ms > 0 && sqlite3_busy_timeout(queue->sqlite_db, busy_timeout_ms) != SQLITE_OK) {
    return TURBO_EIO;
  }
  status = sqlite3_exec(queue->sqlite_db, "PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;", NULL,
                        NULL, NULL);
  if (status != SQLITE_OK) return flow_queue_sqlite_status(status);
  if (flow_queue_sqlite_begin(queue) != TURBO_OK) return TURBO_EBUSY;
  status = sqlite3_exec(queue->sqlite_db, FLOW_QUEUE_SQLITE_META_SCHEMA, NULL, NULL, NULL);
  if (status != SQLITE_OK) goto schema_fail;
  status = sqlite3_prepare_v2(
      queue->sqlite_db, "SELECT version FROM turbo_flow_queue_schema WHERE schema_name='queue'", -1,
      &stmt, NULL);
  if (status != SQLITE_OK) goto schema_fail;
  status = sqlite3_step(stmt);
  if (status == SQLITE_ROW) schema_version = sqlite3_column_int(stmt, 0);
  else if (status != SQLITE_DONE) goto schema_fail;
  (void)sqlite3_finalize(stmt);
  stmt = NULL;
  if (schema_version < 0 || schema_version > (int)FLOW_QUEUE_SQLITE_SCHEMA_VERSION) {
    flow_queue_sqlite_rollback(queue);
    return TURBO_EPROTO;
  }
  status = sqlite3_exec(queue->sqlite_db, FLOW_QUEUE_SQLITE_SCHEMA, NULL, NULL, NULL);
  if (status != SQLITE_OK) goto schema_fail;
  status = sqlite3_prepare_v2(
      queue->sqlite_db,
      "INSERT INTO turbo_flow_queue_schema(schema_name,version) VALUES('queue',?1) "
      "ON CONFLICT(schema_name) DO UPDATE SET version=excluded.version",
      -1, &stmt, NULL);
  if (status != SQLITE_OK ||
      sqlite3_bind_int(stmt, 1, (int)FLOW_QUEUE_SQLITE_SCHEMA_VERSION) != SQLITE_OK)
    goto schema_fail;
  status = sqlite3_step(stmt);
  (void)sqlite3_finalize(stmt);
  stmt = NULL;
  if (status != SQLITE_DONE) goto schema_fail;
  if (flow_queue_sqlite_commit(queue) != TURBO_OK) {
    flow_queue_sqlite_rollback(queue);
    return TURBO_EIO;
  }
  status = sqlite3_prepare_v2(
      queue->sqlite_db,
      "UPDATE turbo_flow_queue_messages SET state=0 WHERE queue_name=?1 AND state=1", -1, &stmt,
      NULL);
  if (status != SQLITE_OK) return flow_queue_sqlite_status(status);
  (void)sqlite3_bind_text(stmt, 1, queue->sqlite_queue_name, -1, SQLITE_STATIC);
  status = sqlite3_step(stmt);
  (void)sqlite3_finalize(stmt);
  stmt = NULL;
  if (status != SQLITE_DONE) return flow_queue_sqlite_status(status);
  status = sqlite3_prepare_v2(queue->sqlite_db,
                              "SELECT COUNT(*) FROM turbo_flow_queue_messages WHERE queue_name=?1",
                              -1, &stmt, NULL);
  if (status != SQLITE_OK) return flow_queue_sqlite_status(status);
  (void)sqlite3_bind_text(stmt, 1, queue->sqlite_queue_name, -1, SQLITE_STATIC);
  status = sqlite3_step(stmt);
  if (status == SQLITE_ROW) {
    sqlite3_int64 depth = sqlite3_column_int64(stmt, 0);
    if (depth < 0 || (uint64_t)depth > SIZE_MAX) {
      (void)sqlite3_finalize(stmt);
      return TURBO_ERANGE;
    }
    queue->sqlite_depth = (size_t)depth;
  }
  (void)sqlite3_finalize(stmt);
  return status == SQLITE_ROW ? TURBO_OK : flow_queue_sqlite_status(status);

schema_fail:
  if (stmt) (void)sqlite3_finalize(stmt);
  flow_queue_sqlite_rollback(queue);
  return flow_queue_sqlite_status(status);
}

static int flow_queue_sqlite_enqueue_locked(turbo_flow_queue_t *queue,
                                            const flow_queue_item_t *item, int replace_oldest) {
  sqlite3_stmt *stmt = NULL;
  int status;
  int rc = flow_queue_sqlite_begin(queue);
  if (rc != TURBO_OK) return rc;
  if (replace_oldest) {
    status = sqlite3_prepare_v2(
        queue->sqlite_db,
        "DELETE FROM turbo_flow_queue_messages WHERE sequence=(SELECT sequence FROM "
        "turbo_flow_queue_messages WHERE queue_name=?1 AND state=0 ORDER BY sequence LIMIT 1)",
        -1, &stmt, NULL);
    if (status != SQLITE_OK) goto sqlite_fail;
    (void)sqlite3_bind_text(stmt, 1, queue->sqlite_queue_name, -1, SQLITE_STATIC);
    status = sqlite3_step(stmt);
    (void)sqlite3_finalize(stmt);
    stmt = NULL;
    if (status != SQLITE_DONE || sqlite3_changes(queue->sqlite_db) != 1) {
      flow_queue_sqlite_rollback(queue);
      return status == SQLITE_DONE ? TURBO_ENOSPC : flow_queue_sqlite_status(status);
    }
  }
  status = sqlite3_prepare_v2(
      queue->sqlite_db,
      "INSERT INTO turbo_flow_queue_messages(queue_name,message_id,timestamp_ns,message_type,"
      "flags,status,payload,state) VALUES(?1,?2,?3,?4,?5,?6,?7,0)",
      -1, &stmt, NULL);
  if (status != SQLITE_OK) goto sqlite_fail;
  (void)sqlite3_bind_text(stmt, 1, queue->sqlite_queue_name, -1, SQLITE_STATIC);
  (void)sqlite3_bind_int64(stmt, 2, (sqlite3_int64)item->msg.id);
  (void)sqlite3_bind_int64(stmt, 3, (sqlite3_int64)item->msg.ts_ns);
  (void)sqlite3_bind_int64(stmt, 4, (sqlite3_int64)item->msg.type);
  (void)sqlite3_bind_int64(stmt, 5, (sqlite3_int64)item->msg.flags);
  (void)sqlite3_bind_int(stmt, 6, item->msg.status);
  (void)sqlite3_bind_blob(stmt, 7, item->msg.payload.data ? item->msg.payload.data : "",
                          (int)item->msg.payload.len, SQLITE_TRANSIENT);
  status = sqlite3_step(stmt);
  (void)sqlite3_finalize(stmt);
  stmt = NULL;
  if (status != SQLITE_DONE) goto sqlite_fail;
  rc = flow_queue_sqlite_commit(queue);
  if (rc != TURBO_OK) {
    flow_queue_sqlite_rollback(queue);
    return rc;
  }
  if (!replace_oldest) queue->sqlite_depth += 1u;
  return TURBO_OK;

sqlite_fail:
  if (stmt) (void)sqlite3_finalize(stmt);
  flow_queue_sqlite_rollback(queue);
  return flow_queue_sqlite_status(status);
}

static int flow_queue_sqlite_claim_locked(turbo_flow_queue_t *queue, flow_queue_item_t *item) {
  sqlite3_stmt *stmt = NULL;
  int status;
  int rc = flow_queue_sqlite_begin(queue);
  if (rc != TURBO_OK) return rc;
  turbo_flow_msg_init(&item->msg);
  status = sqlite3_prepare_v2(
      queue->sqlite_db,
      "SELECT sequence,message_id,timestamp_ns,message_type,flags,status,payload FROM "
      "turbo_flow_queue_messages WHERE queue_name=?1 AND state=0 ORDER BY sequence LIMIT 1",
      -1, &stmt, NULL);
  if (status != SQLITE_OK) goto sqlite_fail;
  (void)sqlite3_bind_text(stmt, 1, queue->sqlite_queue_name, -1, SQLITE_STATIC);
  status = sqlite3_step(stmt);
  if (status == SQLITE_DONE) {
    (void)sqlite3_finalize(stmt);
    flow_queue_sqlite_rollback(queue);
    return TURBO_ENOENT;
  }
  if (status != SQLITE_ROW) goto sqlite_fail;
  item->storage_id = sqlite3_column_int64(stmt, 0);
  item->msg.id = (uint64_t)sqlite3_column_int64(stmt, 1);
  item->msg.ts_ns = (uint64_t)sqlite3_column_int64(stmt, 2);
  item->msg.type = (uint32_t)sqlite3_column_int64(stmt, 3);
  item->msg.flags = (uint32_t)sqlite3_column_int64(stmt, 4);
  item->msg.status = sqlite3_column_int(stmt, 5);
  {
    const void *payload = sqlite3_column_blob(stmt, 6);
    int payload_size = sqlite3_column_bytes(stmt, 6);
    item->msg.owned_payload = tstr_new_len(payload ? payload : "", (size_t)payload_size);
    if (!item->msg.owned_payload) {
      (void)sqlite3_finalize(stmt);
      flow_queue_sqlite_rollback(queue);
      return TURBO_ENOMEM;
    }
    item->msg.payload = tstr_to_v(item->msg.owned_payload);
  }
  (void)sqlite3_finalize(stmt);
  stmt = NULL;
  status = sqlite3_prepare_v2(
      queue->sqlite_db,
      "UPDATE turbo_flow_queue_messages SET state=1 WHERE sequence=?1 AND queue_name=?2 AND "
      "state=0",
      -1, &stmt, NULL);
  if (status != SQLITE_OK) goto sqlite_fail;
  (void)sqlite3_bind_int64(stmt, 1, item->storage_id);
  (void)sqlite3_bind_text(stmt, 2, queue->sqlite_queue_name, -1, SQLITE_STATIC);
  status = sqlite3_step(stmt);
  (void)sqlite3_finalize(stmt);
  stmt = NULL;
  if (status != SQLITE_DONE || sqlite3_changes(queue->sqlite_db) != 1) goto sqlite_fail;
  rc = flow_queue_sqlite_commit(queue);
  if (rc != TURBO_OK) {
    flow_queue_sqlite_rollback(queue);
    turbo_flow_msg_cleanup(&item->msg);
    return rc;
  }
  return TURBO_OK;

sqlite_fail:
  if (stmt) (void)sqlite3_finalize(stmt);
  flow_queue_sqlite_rollback(queue);
  turbo_flow_msg_cleanup(&item->msg);
  return flow_queue_sqlite_status(status);
}

static int flow_queue_sqlite_finish_locked(turbo_flow_queue_t *queue, const flow_queue_item_t *item,
                                           int acknowledge) {
  sqlite3_stmt *stmt = NULL;
  const char *sql = acknowledge
                        ? "DELETE FROM turbo_flow_queue_messages WHERE sequence=?1 AND "
                          "queue_name=?2 AND state=1"
                        : "UPDATE turbo_flow_queue_messages SET state=0 WHERE sequence=?1 AND "
                          "queue_name=?2 AND state=1";
  int status;
  int rc = flow_queue_sqlite_begin(queue);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_prepare_v2(queue->sqlite_db, sql, -1, &stmt, NULL);
  if (status != SQLITE_OK) goto sqlite_fail;
  (void)sqlite3_bind_int64(stmt, 1, item->storage_id);
  (void)sqlite3_bind_text(stmt, 2, queue->sqlite_queue_name, -1, SQLITE_STATIC);
  status = sqlite3_step(stmt);
  (void)sqlite3_finalize(stmt);
  stmt = NULL;
  if (status != SQLITE_DONE || sqlite3_changes(queue->sqlite_db) != 1) goto sqlite_fail;
  rc = flow_queue_sqlite_commit(queue);
  if (rc != TURBO_OK) {
    flow_queue_sqlite_rollback(queue);
    return rc;
  }
  if (acknowledge) queue->sqlite_depth -= 1u;
  return TURBO_OK;

sqlite_fail:
  if (stmt) (void)sqlite3_finalize(stmt);
  flow_queue_sqlite_rollback(queue);
  return status == SQLITE_DONE ? TURBO_EPROTO : flow_queue_sqlite_status(status);
}

static int flow_queue_claim_storage_init(turbo_flow_queue_t *queue, size_t max_active_claims) {
  if (flow_queue_claim_records_init(&queue->claim_records) != TURBO_OK ||
      flow_queue_claim_slots_init(&queue->free_claim_slots) != TURBO_OK ||
      flow_queue_claim_index_init(&queue->claim_index) != TURBO_OK ||
      flow_queue_claim_records_reserve(&queue->claim_records, max_active_claims) != TURBO_OK ||
      flow_queue_claim_slots_reserve(&queue->free_claim_slots, max_active_claims) != TURBO_OK ||
      turbo_hash_map_reserve(&queue->claim_index.raw, max_active_claims) != TURBO_OK ||
      turbo_vec_resize(&queue->claim_records.raw, max_active_claims) != TURBO_OK) {
    return TURBO_ENOMEM;
  }
  memset(flow_queue_claim_records_data(&queue->claim_records), 0,
         max_active_claims * sizeof(flow_queue_claim_record_t));
  for (size_t i = max_active_claims; i > 0u; --i) {
    if (flow_queue_claim_slots_push(&queue->free_claim_slots, i - 1u) != TURBO_OK)
      return TURBO_ENOMEM;
  }
  queue->max_active_claims = max_active_claims;
  return TURBO_OK;
}

static void flow_queue_claim_storage_destroy(turbo_flow_queue_t *queue) {
  flow_queue_claim_records_destroy(&queue->claim_records);
  flow_queue_claim_slots_destroy(&queue->free_claim_slots);
  flow_queue_claim_index_destroy(&queue->claim_index);
  queue->max_active_claims = 0u;
}

static int flow_queue_memory_pop_pending(turbo_flow_queue_t *queue, flow_queue_item_t *out) {
  const flow_queue_item_t *front = flow_queue_items_front_const(&queue->items);
  const flow_queue_item_t *requeued = flow_queue_requeued_peek(&queue->requeued);
  if (!front && !requeued) return TURBO_ENOENT;
  if (requeued && (!front || requeued->enqueue_sequence < front->enqueue_sequence))
    return flow_queue_requeued_pop(&queue->requeued, out) ? TURBO_OK : TURBO_EPROTO;
  return flow_queue_items_pop_front(&queue->items, out) ? TURBO_OK : TURBO_EPROTO;
}

static int flow_queue_memory_restore_pending(turbo_flow_queue_t *queue, flow_queue_item_t *item) {
  int rc = flow_queue_requeued_push(&queue->requeued, *item);
  if (rc == TURBO_OK) turbo_flow_msg_init(&item->msg);
  return rc;
}

static size_t flow_queue_occupancy(const turbo_flow_queue_t *queue) {
  if (queue->backend == FLOW_QUEUE_BACKEND_SQLITE) return queue->sqlite_depth;
  return flow_queue_items_size(&queue->items) + flow_queue_requeued_size(&queue->requeued) +
         queue->active_claims + (queue->source_in_flight ? 1u : 0u);
}

static int flow_queue_valid_config(const turbo_flow_queue_config_t *config) {
  if (!config || !config->resource_uid || !config->resource_uid[0] || !config->owner_name ||
      !config->owner_name[0] || strlen(config->resource_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      strlen(config->owner_name) > TURBO_FLOW_RESOURCE_OWNER_MAX || config->capacity == 0 ||
      config->capacity > TURBO_FLOW_QUEUE_MAX_CAPACITY || config->max_payload_size == 0 ||
      config->max_payload_size > TURBO_FLOW_QUEUE_MAX_PAYLOAD_SIZE ||
      config->full_policy < TURBO_FLOW_QUEUE_FULL_FAIL ||
      config->full_policy > TURBO_FLOW_QUEUE_FULL_DROP_OLDEST) {
    return 0;
  }
  if (config->full_policy == TURBO_FLOW_QUEUE_FULL_BLOCK) {
    return config->enqueue_timeout_ms > 0 &&
           config->enqueue_timeout_ms <= TURBO_FLOW_QUEUE_MAX_TIMEOUT_MS;
  }
  return config->enqueue_timeout_ms == 0;
}

static int flow_queue_retain(turbo_flow_queue_t *queue) {
  if (!queue) return TURBO_EINVAL;
  turbo_mutex_lock(&queue->mutex);
  queue->references += 1u;
  turbo_mutex_unlock(&queue->mutex);
  return TURBO_OK;
}

static void flow_queue_release(turbo_flow_queue_t *queue) {
  if (!queue) return;
  turbo_mutex_lock(&queue->mutex);
  if (queue->references > 0) queue->references -= 1u;
  turbo_mutex_unlock(&queue->mutex);
}

static int flow_queue_clone_message(flow_queue_item_t *item, const turbo_flow_msg_t *msg,
                                    size_t max_payload_size, int durable) {
  const turbo_flow_protocol_settlement_envelope_t *settlement;
  const turbo_flow_protocol_route_t *route;
  int rc;
  if (!item || !msg) return TURBO_EINVAL;
  if (msg->payload.len > 0 && !msg->payload.data) return TURBO_EFAULT;
  if (msg->payload.len > max_payload_size) return TURBO_EMSGSIZE;
  settlement = turbo_flow_msg_protocol_settlement(msg);
  route = turbo_flow_msg_protocol_route(msg);
  if (msg->transport_context ||
      (turbo_flow_msg_content_descriptor(msg) && !turbo_flow_msg_content_descriptor_owned(msg)) ||
      turbo_flow_msg_content_state(msg) == TURBO_FLOW_CONTENT_SCHEMA_BOUND ||
      (durable && turbo_flow_msg_content_descriptor(msg)) ||
      (durable && route &&
       (!settlement || settlement->requested_point != TURBO_FLOW_PROTOCOL_SETTLE_DURABLE))) {
    return TURBO_ENOTSUP;
  }
  turbo_flow_msg_init(&item->msg);
  rc = turbo_flow_msg_clone(&item->msg, msg);
  if (rc != TURBO_OK) return rc;
  if (msg->payload.len > 0 && !item->msg.buffer && !item->msg.owned_payload) {
    item->msg.owned_payload = tstr_new_len(msg->payload.data, msg->payload.len);
    if (!item->msg.owned_payload) {
      turbo_flow_msg_cleanup(&item->msg);
      return TURBO_ENOMEM;
    }
    item->msg.payload = tstr_to_v(item->msg.owned_payload);
  }
  item->msg.transport_context = NULL;
  if (durable && route) turbo_flow_msg_clear_protocol_route(&item->msg);
  return TURBO_OK;
}

static int flow_queue_wait_for_capacity(flow_queue_adapter_t *adapter) {
  turbo_flow_queue_t *queue = adapter->queue;
  uint64_t timeout_ns = queue->enqueue_timeout_ms * UINT64_C(1000000);
  uint64_t started_at = turbo_hrtime();
  while (flow_queue_occupancy(queue) >= queue->capacity &&
         atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    uint64_t elapsed = turbo_hrtime() - started_at;
    if (elapsed >= timeout_ns) return TURBO_ETIMEDOUT;
    (void)turbo_cond_timedwait(&queue->not_full, &queue->mutex, timeout_ns - elapsed);
  }
  return atomic_load_explicit(&adapter->started, memory_order_acquire) ? TURBO_OK : TURBO_ESHUTDOWN;
}

static int flow_queue_sink_consume(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  flow_queue_adapter_t *adapter = (flow_queue_adapter_t *)ctx;
  turbo_flow_queue_t *queue;
  flow_queue_item_t incoming;
  flow_queue_item_t dropped;
  int has_dropped = 0;
  int replace_oldest = 0;
  int rc;
  const turbo_flow_protocol_settlement_envelope_t *settlement;
  (void)flow;
  (void)stage;
  if (!adapter || adapter->role != FLOW_QUEUE_ADAPTER_SINK || !msg ||
      !atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    return TURBO_ESHUTDOWN;
  }
  queue = adapter->queue;
  settlement = turbo_flow_msg_protocol_settlement(msg);
  if (settlement) {
    if (settlement->settled_point != 0) return TURBO_ENOTSUP;
    if (settlement->requested_point == TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED &&
        queue->backend != FLOW_QUEUE_BACKEND_MEMORY)
      return TURBO_ENOTSUP;
    if (settlement->requested_point == TURBO_FLOW_PROTOCOL_SETTLE_DURABLE &&
        queue->backend != FLOW_QUEUE_BACKEND_SQLITE)
      return TURBO_ENOTSUP;
    if (settlement->requested_point != TURBO_FLOW_PROTOCOL_SETTLE_ACCEPTED &&
        settlement->requested_point != TURBO_FLOW_PROTOCOL_SETTLE_DURABLE)
      return TURBO_ENOTSUP;
  }
  memset(&incoming, 0, sizeof(incoming));
  memset(&dropped, 0, sizeof(dropped));
  rc = flow_queue_clone_message(&incoming, msg, queue->max_payload_size,
                                queue->backend == FLOW_QUEUE_BACKEND_SQLITE);
  if (rc != TURBO_OK) {
    turbo_mutex_lock(&queue->mutex);
    queue->enqueue_failures += 1u;
    turbo_mutex_unlock(&queue->mutex);
    return rc;
  }
  turbo_flow_msg_clear_protocol_settlement(&incoming.msg);

  turbo_mutex_lock(&queue->mutex);
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    rc = TURBO_ESHUTDOWN;
    goto fail_locked;
  }
  if (flow_queue_occupancy(queue) >= queue->capacity) {
    if (queue->full_policy == TURBO_FLOW_QUEUE_FULL_FAIL) {
      rc = TURBO_ENOSPC;
      goto fail_locked;
    }
    if (queue->full_policy == TURBO_FLOW_QUEUE_FULL_BLOCK) {
      rc = flow_queue_wait_for_capacity(adapter);
      if (rc != TURBO_OK) goto fail_locked;
    } else {
      if (queue->backend == FLOW_QUEUE_BACKEND_SQLITE) {
        replace_oldest = 1;
      } else {
        if (flow_queue_memory_pop_pending(queue, &dropped) != TURBO_OK) {
          rc = TURBO_ENOSPC;
          goto fail_locked;
        }
        has_dropped = 1;
      }
    }
  }
  if (queue->backend == FLOW_QUEUE_BACKEND_MEMORY) {
    if (queue->enqueue_sequence == UINT64_MAX) {
      rc = TURBO_ERANGE;
      if (has_dropped && flow_queue_memory_restore_pending(queue, &dropped) == TURBO_OK)
        has_dropped = 0;
      goto fail_locked;
    }
    incoming.enqueue_sequence = queue->enqueue_sequence + 1u;
  }
  rc = queue->backend == FLOW_QUEUE_BACKEND_SQLITE
           ? flow_queue_sqlite_enqueue_locked(queue, &incoming, replace_oldest)
           : flow_queue_items_push_back(&queue->items, incoming);
  if (rc != TURBO_OK) {
    if (has_dropped && flow_queue_memory_restore_pending(queue, &dropped) == TURBO_OK)
      has_dropped = 0;
    goto fail_locked;
  }
  if (queue->backend == FLOW_QUEUE_BACKEND_MEMORY) {
    queue->enqueue_sequence += 1u;
    turbo_flow_msg_init(&incoming.msg);
  }
  if (has_dropped || replace_oldest) queue->dropped_oldest += 1u;
  queue->enqueued += 1u;
  turbo_cond_signal(&queue->not_empty);
  turbo_mutex_unlock(&queue->mutex);
  if (has_dropped) turbo_flow_msg_cleanup(&dropped.msg);
  turbo_flow_msg_cleanup(&incoming.msg);
  if (settlement) {
    const turbo_flow_protocol_route_t *route = turbo_flow_msg_protocol_route(msg);
    turbo_flow_protocol_settlement_request_t request =
        TURBO_FLOW_PROTOCOL_SETTLEMENT_REQUEST_INIT;
    if (!route) return TURBO_EPROTO;
    request.message = settlement->message;
    request.point = settlement->requested_point;
    request.status = TURBO_OK;
    request.message_id = msg->id;
    request.attempt = msg->execution_attempt ? msg->execution_attempt : 1u;
    rc = turbo_flow_protocol_route_settle(flow, route, &request);
    if (rc != TURBO_OK) return rc;
    rc = turbo_flow_msg_complete_protocol_settlement(msg, request.point);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;

fail_locked:
  queue->enqueue_failures += 1u;
  turbo_mutex_unlock(&queue->mutex);
  if (has_dropped) turbo_flow_msg_cleanup(&dropped.msg);
  turbo_flow_msg_cleanup(&incoming.msg);
  return rc;
}

static void flow_queue_source_thread(void *ctx) {
  flow_queue_adapter_t *adapter = (flow_queue_adapter_t *)ctx;
  turbo_flow_queue_t *queue = adapter->queue;
  while (atomic_load_explicit(&adapter->started, memory_order_acquire) && adapter->flow &&
         turbo_flow_state(adapter->flow) != TURBO_FLOW_STATE_STARTED) {
    turbo_sleep_ms(1);
  }
  while (atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    flow_queue_item_t item;
    int rc;
    memset(&item, 0, sizeof(item));
    turbo_mutex_lock(&queue->mutex);
    while (flow_queue_occupancy(queue) == 0u &&
           atomic_load_explicit(&adapter->started, memory_order_acquire)) {
      turbo_cond_wait(&queue->not_empty, &queue->mutex);
    }
    if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) {
      turbo_mutex_unlock(&queue->mutex);
      break;
    }
    rc = queue->backend == FLOW_QUEUE_BACKEND_SQLITE ? flow_queue_sqlite_claim_locked(queue, &item)
                                                     : flow_queue_memory_pop_pending(queue, &item);
    if (rc != TURBO_OK) {
      turbo_mutex_unlock(&queue->mutex);
      if (rc != TURBO_ENOENT) break;
      continue;
    }
    queue->source_in_flight = 1;
    turbo_mutex_unlock(&queue->mutex);

    rc = turbo_flow_publish(adapter->flow, adapter->source_name, &item.msg);
    turbo_mutex_lock(&queue->mutex);
    queue->source_in_flight = 0;
    if (rc == TURBO_OK && queue->backend == FLOW_QUEUE_BACKEND_SQLITE) {
      rc = flow_queue_sqlite_finish_locked(queue, &item, 1);
    }
    if (rc == TURBO_OK) {
      queue->delivered += 1u;
      turbo_cond_broadcast(&queue->not_full);
      turbo_mutex_unlock(&queue->mutex);
      turbo_flow_msg_cleanup(&item.msg);
      continue;
    }
    queue->publish_failures += 1u;
    if ((queue->backend == FLOW_QUEUE_BACKEND_SQLITE
             ? flow_queue_sqlite_finish_locked(queue, &item, 0)
             : flow_queue_memory_restore_pending(queue, &item)) == TURBO_OK) {
      queue->delivery_requeues += 1u;
      turbo_cond_signal(&queue->not_empty);
    } else {
      queue->delivery_requeue_failures += 1u;
    }
    turbo_mutex_unlock(&queue->mutex);
    turbo_flow_msg_cleanup(&item.msg);
    break;
  }
}

static int flow_queue_adapter_start(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_stage_plan_t *stage) {
  flow_queue_adapter_t *adapter = (flow_queue_adapter_t *)ctx;
  turbo_flow_queue_t *queue;
  if (!adapter || !flow || !stage) return TURBO_EINVAL;
  if (atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_EALREADY;
  if ((adapter->role == FLOW_QUEUE_ADAPTER_SOURCE && !stage->is_source) ||
      (adapter->role == FLOW_QUEUE_ADAPTER_SINK && stage->is_source)) {
    return TURBO_EINVAL;
  }
  queue = adapter->queue;
  if (adapter->role == FLOW_QUEUE_ADAPTER_SINK) {
    atomic_store_explicit(&adapter->started, 1, memory_order_release);
    return TURBO_OK;
  }
  tstr_freep(&adapter->source_name);
  adapter->source_name = tstr_dup(stage->name);
  if (!adapter->source_name) return TURBO_ENOMEM;
  turbo_mutex_lock(&queue->mutex);
  if (queue->active_source || queue->source_in_flight || queue->active_claims > 0u) {
    turbo_mutex_unlock(&queue->mutex);
    return TURBO_EBUSY;
  }
  queue->active_source = adapter;
  turbo_mutex_unlock(&queue->mutex);
  adapter->flow = flow;
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  if (turbo_thread_create(&adapter->thread, flow_queue_source_thread, adapter) != TURBO_OK) {
    atomic_store_explicit(&adapter->started, 0, memory_order_release);
    turbo_mutex_lock(&queue->mutex);
    queue->active_source = NULL;
    turbo_mutex_unlock(&queue->mutex);
    return TURBO_EIO;
  }
  adapter->thread_started = 1;
  return TURBO_OK;
}

static void flow_queue_adapter_stop(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_stage_plan_t *stage) {
  flow_queue_adapter_t *adapter = (flow_queue_adapter_t *)ctx;
  turbo_flow_queue_t *queue;
  (void)flow;
  (void)stage;
  if (!adapter) return;
  queue = adapter->queue;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  turbo_mutex_lock(&queue->mutex);
  turbo_cond_broadcast(&queue->not_empty);
  turbo_cond_broadcast(&queue->not_full);
  turbo_mutex_unlock(&queue->mutex);
  if (adapter->thread_started) {
    (void)turbo_thread_join(&adapter->thread);
    adapter->thread_started = 0;
  }
  if (adapter->role == FLOW_QUEUE_ADAPTER_SOURCE) {
    turbo_mutex_lock(&queue->mutex);
    if (queue->active_source == adapter) queue->active_source = NULL;
    turbo_mutex_unlock(&queue->mutex);
  }
}

static void flow_queue_adapter_shutdown(void *ctx) {
  flow_queue_adapter_t *adapter = (flow_queue_adapter_t *)ctx;
  if (!adapter) return;
  flow_queue_adapter_stop(adapter, NULL, NULL);
  tstr_freep(&adapter->source_name);
  flow_queue_release(adapter->queue);
  free(adapter);
}

static int flow_queue_resource_snapshot(void *ctx, turbo_flow_resource_snapshot_t *out) {
  flow_queue_adapter_t *adapter = (flow_queue_adapter_t *)ctx;
  turbo_flow_queue_snapshot_t queue = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int rc;
  if (!adapter || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  rc = turbo_flow_queue_snapshot(adapter->queue, &queue);
  if (rc != TURBO_OK) return rc;
  rc = flow_queue_resource_metadata(adapter, &metadata);
  if (rc != TURBO_OK) return rc;
  *out = (turbo_flow_resource_snapshot_t)TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  out->domain = metadata.domain;
  out->kind = TURBO_FLOW_RESOURCE_QUEUE_BUFFER;
  memcpy(out->uid, metadata.uid, sizeof(out->uid));
  memcpy(out->owner_name, metadata.owner_name, sizeof(out->owner_name));
  out->generation = metadata.generation;
  out->observed_generation = metadata.observed_generation;
  out->load = queue.depth;
  out->capacity = queue.capacity;
  out->saturated = queue.capacity > 0u && queue.depth >= queue.capacity;
  out->last_status = TURBO_OK;
  return TURBO_OK;
}

static int flow_queue_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  flow_queue_adapter_t *adapter = (flow_queue_adapter_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int written;
  if (!adapter || !adapter->queue || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  metadata.domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
  metadata.kind = TURBO_FLOW_RESOURCE_QUEUE_BUFFER;
  metadata.generation = 1u;
  metadata.observed_generation = 1u;
  written = snprintf(metadata.uid, sizeof(metadata.uid), "%s", adapter->queue->resource_uid);
  if (written < 0 || (size_t)written >= sizeof(metadata.uid)) return TURBO_ENAMETOOLONG;
  written =
      snprintf(metadata.owner_name, sizeof(metadata.owner_name), "%s", adapter->queue->owner_name);
  if (written < 0 || (size_t)written >= sizeof(metadata.owner_name)) return TURBO_ENAMETOOLONG;
  *out = metadata;
  return TURBO_OK;
}

static int flow_queue_resource_document(void *ctx,
                                        turbo_flow_resource_document_kind_t document_kind,
                                        turbo_flow_resource_document_t *out) {
  flow_queue_adapter_t *adapter = (flow_queue_adapter_t *)ctx;
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_queue_snapshot_t snapshot = TURBO_FLOW_QUEUE_SNAPSHOT_INIT;
  turbo_flow_queue_ack_snapshot_t acknowledgements = TURBO_FLOW_QUEUE_ACK_SNAPSHOT_INIT;
  tstr_t payload;
  tstr_t updated;
  int rc;
  if (!adapter || !out || out->size < sizeof(*out) || out->payload) return TURBO_EINVAL;
  if (document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return TURBO_ENOTSUP;
  rc = flow_queue_resource_metadata(adapter, &metadata);
  if (rc != TURBO_OK) return rc;
  turbo_mutex_lock(&adapter->queue->mutex);
  flow_queue_snapshot_locked(adapter->queue, &snapshot);
  flow_queue_ack_snapshot_locked(adapter->queue, &acknowledgements);
  turbo_mutex_unlock(&adapter->queue->mutex);
  payload =
      tstr_format("{\"depth\":\"{}\",\"capacity\":\"{}\",\"enqueued\":\"{}\","
                  "\"delivered\":\"{}\",\"dropped_oldest\":\"{}\","
                  "\"enqueue_failures\":\"{}\",\"publish_failures\":\"{}\",",
                  snapshot.depth, snapshot.capacity, snapshot.enqueued, snapshot.delivered,
                  snapshot.dropped_oldest, snapshot.enqueue_failures, snapshot.publish_failures);
  if (!payload) return TURBO_ENOMEM;
  updated = tstr_append_format(payload,
                               "\"accept_acks\":\"{}\",\"delivery_acks\":\"{}\","
                               "\"delivery_requeues\":\"{}\",\"delivery_requeue_failures\":\"{}\","
                               "\"saturated\":{}}",
                               acknowledgements.accept_acks, acknowledgements.delivery_acks,
                               acknowledgements.delivery_requeues,
                               acknowledgements.delivery_requeue_failures,
                               snapshot.capacity > 0u && snapshot.depth >= snapshot.capacity);
  if (!updated) {
    tstr_free(payload);
    return TURBO_ENOMEM;
  }
  payload = updated;
  rc = turbo_flow_resource_document_set_payload_copy(out, &metadata, &FLOW_QUEUE_STATUS_SCHEMA,
                                                     payload, tstr_len(payload));
  tstr_free(payload);
  return rc;
}

static int flow_queue_register_adapter(turbo_flow_t *flow, const char *name,
                                       turbo_flow_queue_t *queue, flow_queue_adapter_role_t role) {
  static const char *const primitive_types[] = {TURBO_FLOW_QUEUE_PRIMITIVE_TYPE};
  static const char *const operation_names[] = {TURBO_FLOW_QUEUE_DEQUEUE_OPERATION,
                                                TURBO_FLOW_QUEUE_ENQUEUE_OPERATION};
  flow_queue_adapter_t *adapter;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_resource_provider_registration_t resource =
      TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
  turbo_flow_module_adapter_registration_t registration =
      TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT;
  turbo_flow_operation_descriptor_t operations[2];
  turbo_flow_module_descriptor_t module;
  turbo_flow_primitive_descriptor_t primitive;
  turbo_flow_adapter_schema_t schema;
  const char *selected_operation[1];
  const char *selected_resource[1];
  int rc;
  int resource_registered = 0;
  if (!flow || !name || name[0] == '\0' || !queue) return TURBO_EINVAL;
  memset(operations, 0, sizeof(operations));
  memset(&module, 0, sizeof(module));
  for (size_t i = 0; i < 2u; ++i) {
    operations[i].size = sizeof(operations[i]);
    operations[i].name = operation_names[i];
    operations[i].version = 1u;
    operations[i].domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
    operations[i].resource_domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
    operations[i].resource_type = TURBO_FLOW_QUEUE_PRIMITIVE_TYPE;
    operations[i].resource_min_version = 1u;
    operations[i].resource_max_version = 1u;
    operations[i].scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
    operations[i].scope.state = TURBO_FLOW_STATE_SCOPE_RESOURCE_OWNER;
    operations[i].scope.lifetime = i == 0u ? TURBO_FLOW_LIFETIME_DISPATCH
                                            : TURBO_FLOW_LIFETIME_CALL;
    operations[i].scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
    operations[i].scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
    operations[i].flags = (i == 0u ? TURBO_FLOW_OPERATION_SOURCE
                                   : TURBO_FLOW_OPERATION_STAGE) |
                          TURBO_FLOW_OPERATION_BRIDGE;
    operations[i].execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  }
  operations[0].output_domain = TURBO_FLOW_DOMAIN_DATA;
  operations[0].output_type = "Message";
  operations[1].input_domain = TURBO_FLOW_DOMAIN_DATA;
  operations[1].input_type = "Message";
  module.size = sizeof(module);
  module.name = TURBO_FLOW_QUEUE_MODULE;
  module.version = 1u;
  module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                            TURBO_FLOW_MODULE_MANAGED_RESOURCES |
                            TURBO_FLOW_MODULE_NATIVE_API;
  module.primitive_types = primitive_types;
  module.primitive_type_count = 1u;
  module.operation_names = operation_names;
  module.operation_count = 2u;
  rc = turbo_flow_register_module_contract(flow, &module, operations, 2u);
  if (rc != TURBO_OK) return rc;
  adapter = (flow_queue_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  adapter->queue = queue;
  adapter->role = role;
  atomic_init(&adapter->started, 0);
  flow_queue_retain(queue);
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_queue_adapter_start;
  ops.consume = role == FLOW_QUEUE_ADAPTER_SINK ? flow_queue_sink_consume : NULL;
  ops.stop = flow_queue_adapter_stop;
  ops.shutdown = flow_queue_adapter_shutdown;
  memset(&schema, 0, sizeof(schema));
  schema.kind = TURBO_FLOW_ADAPTER_KIND_QUEUE;
  schema.roles =
      role == FLOW_QUEUE_ADAPTER_SOURCE ? TURBO_FLOW_ADAPTER_SOURCE : TURBO_FLOW_ADAPTER_SINK;
  schema.direction =
      role == FLOW_QUEUE_ADAPTER_SOURCE ? TURBO_FLOW_ADAPTER_INPUT : TURBO_FLOW_ADAPTER_OUTPUT;
  schema.fields = FLOW_QUEUE_FIELDS;
  schema.field_count = sizeof(FLOW_QUEUE_FIELDS) / sizeof(FLOW_QUEUE_FIELDS[0]);
  memset(&primitive, 0, sizeof(primitive));
  primitive.size = sizeof(primitive);
  primitive.name = queue->owner_name;
  primitive.type_name = TURBO_FLOW_QUEUE_PRIMITIVE_TYPE;
  primitive.version = 1u;
  primitive.domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
  primitive.kind = TURBO_FLOW_PRIMITIVE_RESOURCE;
  selected_operation[0] = role == FLOW_QUEUE_ADAPTER_SOURCE
                              ? TURBO_FLOW_QUEUE_DEQUEUE_OPERATION
                              : TURBO_FLOW_QUEUE_ENQUEUE_OPERATION;
  selected_resource[0] = queue->owner_name;
  registration.module_name = TURBO_FLOW_QUEUE_MODULE;
  registration.adapter_name = name;
  registration.ops = &ops;
  registration.ctx = adapter;
  registration.schema = &schema;
  registration.operation_names = selected_operation;
  registration.operation_count = 1u;
  registration.operation_resource_names = selected_resource;
  registration.primitives = &primitive;
  registration.primitive_count = 1u;
  for (size_t i = 0; i < turbo_flow_resource_metadata_count(flow); ++i) {
    turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
    rc = turbo_flow_resource_metadata_at(flow, i, &metadata);
    if (rc != TURBO_OK) {
      flow_queue_adapter_shutdown(adapter);
      return rc;
    }
    if (strcmp(metadata.uid, queue->resource_uid) == 0) {
      if (metadata.kind != TURBO_FLOW_RESOURCE_QUEUE_BUFFER ||
          strcmp(metadata.owner_name, queue->owner_name) != 0) {
        flow_queue_adapter_shutdown(adapter);
        return TURBO_EPROTO;
      }
      resource_registered = 1;
      break;
    }
  }
  if (resource_registered) {
    rc = turbo_flow_register_module_adapter(flow, &registration);
    if (rc != TURBO_OK) flow_queue_adapter_shutdown(adapter);
    return rc;
  }
  resource.owner_name = queue->owner_name;
  resource.ops.metadata = flow_queue_resource_metadata;
  resource.ops.snapshot = flow_queue_resource_snapshot;
  resource.ops.document = flow_queue_resource_document;
  resource.ctx = adapter;
  registration.resources = &resource;
  registration.resource_count = 1u;
  rc = turbo_flow_register_module_adapter(flow, &registration);
  if (rc != TURBO_OK) flow_queue_adapter_shutdown(adapter);
  return rc;
}

turbo_flow_queue_t *turbo_flow_queue_create(const turbo_flow_queue_config_t *config) {
  turbo_flow_queue_t *queue;
  if (!flow_queue_valid_config(config)) return NULL;
  queue = (turbo_flow_queue_t *)calloc(1, sizeof(*queue));
  if (!queue) return NULL;
  if (flow_queue_items_init(&queue->items) != TURBO_OK ||
      flow_queue_items_reserve(&queue->items, config->capacity) != TURBO_OK ||
      flow_queue_requeued_init(&queue->requeued) != TURBO_OK ||
      turbo_heap_reserve(&queue->requeued.raw, config->capacity) != TURBO_OK ||
      flow_queue_claim_storage_init(queue, 1u) != TURBO_OK) {
    flow_queue_items_destroy(&queue->items);
    flow_queue_requeued_destroy(&queue->requeued);
    flow_queue_claim_storage_destroy(queue);
    free(queue);
    return NULL;
  }
  turbo_mutex_init(&queue->mutex);
  turbo_cond_init(&queue->not_empty);
  turbo_cond_init(&queue->not_full);
  queue->backend = FLOW_QUEUE_BACKEND_MEMORY;
  queue->capacity = config->capacity;
  queue->max_payload_size = config->max_payload_size;
  queue->full_policy = config->full_policy;
  queue->enqueue_timeout_ms = config->enqueue_timeout_ms;
  queue->resource_uid = tstr_dup(config->resource_uid);
  queue->owner_name = tstr_dup(config->owner_name);
  if (!queue->resource_uid || !queue->owner_name) {
    tstr_freep(&queue->resource_uid);
    tstr_freep(&queue->owner_name);
    flow_queue_items_destroy(&queue->items);
    flow_queue_requeued_destroy(&queue->requeued);
    flow_queue_claim_storage_destroy(queue);
    turbo_cond_destroy(&queue->not_empty);
    turbo_cond_destroy(&queue->not_full);
    turbo_mutex_destroy(&queue->mutex);
    free(queue);
    return NULL;
  }
  return queue;
}

turbo_flow_queue_t *turbo_flow_sqlite_queue_create(const turbo_flow_sqlite_queue_config_t *config) {
  turbo_flow_queue_t *queue;
  int rc;
  if (!config || !flow_queue_valid_config(&config->queue) || !config->database_path ||
      !config->database_path[0] || !config->queue_name || !config->queue_name[0] ||
      strlen(config->queue_name) > TURBO_FLOW_QUEUE_NAME_MAX || config->busy_timeout_ms < 0 ||
      config->max_state_size > INT_MAX) {
    return NULL;
  }
  queue = (turbo_flow_queue_t *)calloc(1, sizeof(*queue));
  if (!queue) return NULL;
  if (flow_queue_items_init(&queue->items) != TURBO_OK ||
      flow_queue_requeued_init(&queue->requeued) != TURBO_OK ||
      flow_queue_claim_storage_init(queue, 1u) != TURBO_OK) {
    flow_queue_items_destroy(&queue->items);
    flow_queue_requeued_destroy(&queue->requeued);
    flow_queue_claim_storage_destroy(queue);
    free(queue);
    return NULL;
  }
  turbo_mutex_init(&queue->mutex);
  turbo_cond_init(&queue->not_empty);
  turbo_cond_init(&queue->not_full);
  queue->backend = FLOW_QUEUE_BACKEND_SQLITE;
  queue->capacity = config->queue.capacity;
  queue->max_payload_size = config->queue.max_payload_size;
  queue->full_policy = config->queue.full_policy;
  queue->enqueue_timeout_ms = config->queue.enqueue_timeout_ms;
  queue->resource_uid = tstr_dup(config->queue.resource_uid);
  queue->owner_name = tstr_dup(config->queue.owner_name);
  queue->sqlite_path = tstr_dup(config->database_path);
  queue->sqlite_queue_name = tstr_dup(config->queue_name);
  queue->sqlite_max_state_size = config->max_state_size
                                     ? config->max_state_size
                                     : TURBO_FLOW_SQLITE_QUEUE_DEFAULT_MAX_STATE_SIZE;
  if (!queue->resource_uid || !queue->owner_name || !queue->sqlite_path ||
      !queue->sqlite_queue_name) {
    rc = TURBO_ENOMEM;
  } else {
    rc = flow_queue_sqlite_initialize(queue, config->busy_timeout_ms);
    if (rc == TURBO_OK && queue->sqlite_depth > queue->capacity) rc = TURBO_ENOSPC;
  }
  if (rc != TURBO_OK) {
    if (queue->sqlite_db) (void)sqlite3_close(queue->sqlite_db);
    tstr_freep(&queue->sqlite_queue_name);
    tstr_freep(&queue->sqlite_path);
    tstr_freep(&queue->resource_uid);
    tstr_freep(&queue->owner_name);
    flow_queue_items_destroy(&queue->items);
    flow_queue_requeued_destroy(&queue->requeued);
    flow_queue_claim_storage_destroy(queue);
    turbo_cond_destroy(&queue->not_empty);
    turbo_cond_destroy(&queue->not_full);
    turbo_mutex_destroy(&queue->mutex);
    free(queue);
    return NULL;
  }
  return queue;
}

int turbo_flow_queue_destroy(turbo_flow_queue_t *queue) {
  flow_queue_item_t item;
  if (!queue) return TURBO_EINVAL;
  turbo_mutex_lock(&queue->mutex);
  if (queue->references > 0 || queue->active_source || queue->source_in_flight ||
      queue->active_claims > 0u) {
    turbo_mutex_unlock(&queue->mutex);
    return TURBO_EBUSY;
  }
  turbo_mutex_unlock(&queue->mutex);
  while (flow_queue_items_pop_front(&queue->items, &item)) {
    turbo_flow_msg_cleanup(&item.msg);
  }
  while (flow_queue_requeued_pop(&queue->requeued, &item)) {
    turbo_flow_msg_cleanup(&item.msg);
  }
  if (queue->sqlite_db && sqlite3_close(queue->sqlite_db) != SQLITE_OK) return TURBO_EBUSY;
  queue->sqlite_db = NULL;
  flow_queue_items_destroy(&queue->items);
  flow_queue_requeued_destroy(&queue->requeued);
  flow_queue_claim_storage_destroy(queue);
  turbo_cond_destroy(&queue->not_empty);
  turbo_cond_destroy(&queue->not_full);
  turbo_mutex_destroy(&queue->mutex);
  tstr_freep(&queue->resource_uid);
  tstr_freep(&queue->owner_name);
  tstr_freep(&queue->channel_name);
  tstr_freep(&queue->sqlite_path);
  tstr_freep(&queue->sqlite_queue_name);
  free(queue);
  return TURBO_OK;
}

int turbo_flow_queue_register_source_adapter(turbo_flow_t *flow, const char *name,
                                             turbo_flow_queue_t *queue) {
  return flow_queue_register_adapter(flow, name, queue, FLOW_QUEUE_ADAPTER_SOURCE);
}

int turbo_flow_queue_register_sink_adapter(turbo_flow_t *flow, const char *name,
                                           turbo_flow_queue_t *queue) {
  return flow_queue_register_adapter(flow, name, queue, FLOW_QUEUE_ADAPTER_SINK);
}

int turbo_flow_queue_configure_claims(turbo_flow_queue_t *queue,
                                      const turbo_flow_queue_claim_owner_config_t *config) {
  turbo_flow_queue_t replacement;
  if (!queue || !config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_QUEUE_CLAIM_OWNER_API_VERSION ||
      config->max_active_claims == 0u || config->max_active_claims > queue->capacity) {
    return TURBO_EINVAL;
  }
  turbo_mutex_lock(&queue->mutex);
  if (queue->active_source || queue->source_in_flight || queue->active_claims > 0u) {
    turbo_mutex_unlock(&queue->mutex);
    return TURBO_EBUSY;
  }
  if (queue->max_active_claims == config->max_active_claims) {
    turbo_mutex_unlock(&queue->mutex);
    return TURBO_OK;
  }
  memset(&replacement, 0, sizeof(replacement));
  if (flow_queue_claim_storage_init(&replacement, config->max_active_claims) != TURBO_OK) {
    flow_queue_claim_storage_destroy(&replacement);
    turbo_mutex_unlock(&queue->mutex);
    return TURBO_ENOMEM;
  }
  flow_queue_claim_storage_destroy(queue);
  queue->claim_records = replacement.claim_records;
  queue->free_claim_slots = replacement.free_claim_slots;
  queue->claim_index = replacement.claim_index;
  queue->max_active_claims = replacement.max_active_claims;
  turbo_mutex_unlock(&queue->mutex);
  return TURBO_OK;
}

static int flow_queue_claim_release_locked(turbo_flow_queue_t *queue, size_t slot,
                                           int cleanup_item) {
  flow_queue_claim_record_t *record = flow_queue_claim_records_at(&queue->claim_records, slot);
  if (!record || !record->active ||
      !flow_queue_claim_index_remove(&queue->claim_index, record->token, NULL)) {
    return TURBO_EPROTO;
  }
  if (cleanup_item) turbo_flow_msg_cleanup(&record->item.msg);
  memset(record, 0, sizeof(*record));
  if (flow_queue_claim_slots_push(&queue->free_claim_slots, slot) != TURBO_OK) return TURBO_EPROTO;
  queue->active_claims -= 1u;
  return TURBO_OK;
}

int turbo_flow_queue_claim(turbo_flow_queue_t *queue, turbo_flow_queue_claim_t *claim) {
  flow_queue_item_t item;
  flow_queue_claim_record_t *record;
  size_t slot;
  uint64_t token;
  int rc;
  if (!queue || !claim || claim->size < sizeof(*claim)) return TURBO_EINVAL;
  memset(&item, 0, sizeof(item));
  turbo_mutex_lock(&queue->mutex);
  if (queue->active_source || queue->source_in_flight ||
      queue->active_claims >= queue->max_active_claims) {
    turbo_mutex_unlock(&queue->mutex);
    return TURBO_EBUSY;
  }
  if (queue->claim_generation == UINT64_MAX) {
    turbo_mutex_unlock(&queue->mutex);
    return TURBO_ERANGE;
  }
  rc = queue->backend == FLOW_QUEUE_BACKEND_SQLITE ? flow_queue_sqlite_claim_locked(queue, &item)
                                                   : flow_queue_memory_pop_pending(queue, &item);
  if (rc != TURBO_OK) {
    turbo_mutex_unlock(&queue->mutex);
    return rc;
  }
  if (!flow_queue_claim_slots_pop(&queue->free_claim_slots, &slot)) {
    rc = TURBO_EPROTO;
    goto restore;
  }
  record = flow_queue_claim_records_at(&queue->claim_records, slot);
  if (!record || record->active) {
    (void)flow_queue_claim_slots_push(&queue->free_claim_slots, slot);
    rc = TURBO_EPROTO;
    goto restore;
  }
  token = queue->claim_generation + 1u;
  record->active = 1;
  record->token = token;
  record->item = item;
  turbo_flow_msg_init(&item.msg);
  if (flow_queue_claim_index_put(&queue->claim_index, token, slot) != TURBO_OK) {
    item = record->item;
    memset(record, 0, sizeof(*record));
    (void)flow_queue_claim_slots_push(&queue->free_claim_slots, slot);
    rc = TURBO_ENOMEM;
    goto restore;
  }
  queue->claim_generation = token;
  queue->active_claims += 1u;
  *claim = (turbo_flow_queue_claim_t)TURBO_FLOW_QUEUE_CLAIM_INIT;
  claim->token = token;
  claim->message = &record->item.msg;
  turbo_mutex_unlock(&queue->mutex);
  return TURBO_OK;

restore:
  if (queue->backend == FLOW_QUEUE_BACKEND_SQLITE)
    (void)flow_queue_sqlite_finish_locked(queue, &item, 0);
  else (void)flow_queue_memory_restore_pending(queue, &item);
  turbo_flow_msg_cleanup(&item.msg);
  turbo_mutex_unlock(&queue->mutex);
  return rc;
}

static int flow_queue_claim_finish(turbo_flow_queue_t *queue, uint64_t token, int delivered) {
  flow_queue_claim_record_t *record;
  const size_t *found;
  size_t slot;
  int rc = TURBO_OK;
  if (!queue || token == 0u) return TURBO_EINVAL;
  turbo_mutex_lock(&queue->mutex);
  found = flow_queue_claim_index_get_const(&queue->claim_index, token);
  if (!found) {
    turbo_mutex_unlock(&queue->mutex);
    return TURBO_EALREADY;
  }
  slot = *found;
  record = flow_queue_claim_records_at(&queue->claim_records, slot);
  if (!record || !record->active || record->token != token) {
    turbo_mutex_unlock(&queue->mutex);
    return TURBO_EPROTO;
  }
  if (queue->backend == FLOW_QUEUE_BACKEND_SQLITE) {
    rc = flow_queue_sqlite_finish_locked(queue, &record->item, 1);
  }
  if (rc == TURBO_OK) {
    rc = flow_queue_claim_release_locked(queue, slot, 1);
  }
  if (rc == TURBO_OK) {
    if (delivered) queue->delivered += 1u;
    turbo_cond_broadcast(&queue->not_full);
  }
  turbo_mutex_unlock(&queue->mutex);
  return rc;
}

int turbo_flow_queue_claim_ack(turbo_flow_queue_t *queue, uint64_t token) {
  return flow_queue_claim_finish(queue, token, 1);
}

int turbo_flow_queue_claim_drop(turbo_flow_queue_t *queue, uint64_t token) {
  return flow_queue_claim_finish(queue, token, 0);
}

int turbo_flow_queue_claim_requeue(turbo_flow_queue_t *queue, uint64_t token) {
  flow_queue_claim_record_t *record;
  const size_t *found;
  size_t slot;
  int rc;
  if (!queue || token == 0u) return TURBO_EINVAL;
  turbo_mutex_lock(&queue->mutex);
  found = flow_queue_claim_index_get_const(&queue->claim_index, token);
  if (!found) {
    turbo_mutex_unlock(&queue->mutex);
    return TURBO_EALREADY;
  }
  slot = *found;
  record = flow_queue_claim_records_at(&queue->claim_records, slot);
  if (!record || !record->active || record->token != token) {
    turbo_mutex_unlock(&queue->mutex);
    return TURBO_EPROTO;
  }
  rc = queue->backend == FLOW_QUEUE_BACKEND_SQLITE
           ? flow_queue_sqlite_finish_locked(queue, &record->item, 0)
           : flow_queue_memory_restore_pending(queue, &record->item);
  if (rc == TURBO_OK) rc = flow_queue_claim_release_locked(queue, slot, 1);
  if (rc == TURBO_OK) queue->delivery_requeues += 1u;
  else queue->delivery_requeue_failures += 1u;
  if (rc == TURBO_OK) turbo_cond_signal(&queue->not_empty);
  turbo_mutex_unlock(&queue->mutex);
  return rc;
}

static int flow_queue_claim_ack_adapter(void *ctx, uint64_t token) {
  return turbo_flow_queue_claim_ack((turbo_flow_queue_t *)ctx, token);
}

static int flow_queue_claim_requeue_adapter(void *ctx, uint64_t token) {
  return turbo_flow_queue_claim_requeue((turbo_flow_queue_t *)ctx, token);
}

static int flow_queue_claim_drop_adapter(void *ctx, uint64_t token) {
  return turbo_flow_queue_claim_drop((turbo_flow_queue_t *)ctx, token);
}

static int flow_queue_sqlite_state_read_locked(turbo_flow_queue_t *queue, const char *key,
                                               uint8_t *out, size_t capacity, size_t *out_size,
                                               int *present) {
  sqlite3_stmt *statement = NULL;
  int status;
  int rc = TURBO_EIO;
  *out_size = 0u;
  *present = 0;
  status = sqlite3_prepare_v2(
      queue->sqlite_db,
      "SELECT value FROM turbo_flow_queue_claim_state WHERE queue_name=?1 AND state_key=?2", -1,
      &statement, NULL);
  if (status != SQLITE_OK) goto done;
  if (sqlite3_bind_text(statement, 1, queue->sqlite_queue_name, -1, SQLITE_STATIC) != SQLITE_OK ||
      sqlite3_bind_text(statement, 2, key, -1, SQLITE_STATIC) != SQLITE_OK)
    goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (status != SQLITE_ROW) goto done;
  {
    int value_size = sqlite3_column_bytes(statement, 0);
    const void *value = sqlite3_column_blob(statement, 0);
    if (value_size < 0 || (size_t)value_size > queue->sqlite_max_state_size) {
      rc = TURBO_EMSGSIZE;
      goto done;
    }
    *present = 1;
    *out_size = (size_t)value_size;
    if (!out || capacity < *out_size) {
      rc = TURBO_ENOSPC;
      goto done;
    }
    if (*out_size > 0u) memcpy(out, value, *out_size);
  }
  rc = TURBO_OK;
done:
  if (statement) (void)sqlite3_finalize(statement);
  return rc;
}

static int flow_queue_sqlite_state_equal_locked(turbo_flow_queue_t *queue, const char *key,
                                                const uint8_t *state, size_t state_size,
                                                int *equal) {
  sqlite3_stmt *statement = NULL;
  int status;
  int rc = TURBO_EIO;
  *equal = 0;
  status = sqlite3_prepare_v2(
      queue->sqlite_db,
      "SELECT value FROM turbo_flow_queue_claim_state WHERE queue_name=?1 AND state_key=?2", -1,
      &statement, NULL);
  if (status != SQLITE_OK) goto done;
  if (sqlite3_bind_text(statement, 1, queue->sqlite_queue_name, -1, SQLITE_STATIC) != SQLITE_OK ||
      sqlite3_bind_text(statement, 2, key, -1, SQLITE_STATIC) != SQLITE_OK)
    goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_OK;
    goto done;
  }
  if (status != SQLITE_ROW) goto done;
  {
    int value_size = sqlite3_column_bytes(statement, 0);
    const void *value = sqlite3_column_blob(statement, 0);
    if (value_size < 0 || (size_t)value_size > queue->sqlite_max_state_size) {
      rc = TURBO_EMSGSIZE;
      goto done;
    }
    *equal = (size_t)value_size == state_size &&
             (state_size == 0u || memcmp(value, state, state_size) == 0);
  }
  rc = TURBO_OK;
done:
  if (statement) (void)sqlite3_finalize(statement);
  return rc;
}

static int flow_queue_sqlite_row_state_locked(turbo_flow_queue_t *queue, sqlite3_int64 storage_id,
                                              int *present, int *row_state) {
  sqlite3_stmt *statement = NULL;
  int status;
  int rc = TURBO_EIO;
  *present = 0;
  *row_state = -1;
  status = sqlite3_prepare_v2(
      queue->sqlite_db,
      "SELECT state FROM turbo_flow_queue_messages WHERE sequence=?1 AND queue_name=?2", -1,
      &statement, NULL);
  if (status != SQLITE_OK) goto done;
  if (sqlite3_bind_int64(statement, 1, storage_id) != SQLITE_OK ||
      sqlite3_bind_text(statement, 2, queue->sqlite_queue_name, -1, SQLITE_STATIC) != SQLITE_OK)
    goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_OK;
    goto done;
  }
  if (status != SQLITE_ROW) goto done;
  *present = 1;
  *row_state = sqlite3_column_int(statement, 0);
  rc = (*row_state == 0 || *row_state == 1) ? TURBO_OK : TURBO_EPROTO;
done:
  if (statement) (void)sqlite3_finalize(statement);
  return rc;
}

static int flow_queue_sqlite_state_upsert_locked(turbo_flow_queue_t *queue, const char *key,
                                                 const uint8_t *state, size_t state_size) {
  static const uint8_t empty = 0u;
  sqlite3_stmt *statement = NULL;
  int status;
  status = sqlite3_prepare_v2(
      queue->sqlite_db,
      "INSERT INTO turbo_flow_queue_claim_state(queue_name,state_key,value) VALUES(?1,?2,?3) "
      "ON CONFLICT(queue_name,state_key) DO UPDATE SET value=excluded.value",
      -1, &statement, NULL);
  if (status != SQLITE_OK) goto fail;
  if (sqlite3_bind_text(statement, 1, queue->sqlite_queue_name, -1, SQLITE_STATIC) != SQLITE_OK ||
      sqlite3_bind_text(statement, 2, key, -1, SQLITE_STATIC) != SQLITE_OK ||
      sqlite3_bind_blob(statement, 3, state_size > 0u ? state : &empty, (int)state_size,
                        SQLITE_TRANSIENT) != SQLITE_OK)
    goto fail;
  status = sqlite3_step(statement);
  (void)sqlite3_finalize(statement);
  return status == SQLITE_DONE ? TURBO_OK : flow_queue_sqlite_status(status);
fail:
  if (statement) (void)sqlite3_finalize(statement);
  return flow_queue_sqlite_status(status);
}

static int flow_queue_sqlite_state_load_adapter(void *ctx, const char *key, uint8_t *out,
                                                size_t capacity, size_t *out_size) {
  turbo_flow_queue_t *queue = (turbo_flow_queue_t *)ctx;
  int present = 0;
  int rc;
  if (!queue || queue->backend != FLOW_QUEUE_BACKEND_SQLITE || !key || !key[0] ||
      strlen(key) > TURBO_FLOW_SQLITE_BLOB_STORE_KEY_MAX || !out_size || (!out && capacity > 0u))
    return TURBO_EINVAL;
  turbo_mutex_lock(&queue->mutex);
  rc = flow_queue_sqlite_state_read_locked(queue, key, out, capacity, out_size, &present);
  turbo_mutex_unlock(&queue->mutex);
  return rc;
}

static int flow_queue_sqlite_state_commit_adapter(void *ctx, uint64_t token,
                                                  turbo_flow_claim_commit_action_t action,
                                                  const char *key, const uint8_t *state,
                                                  size_t state_size) {
  turbo_flow_queue_t *queue = (turbo_flow_queue_t *)ctx;
  flow_queue_claim_record_t *record = NULL;
  const size_t *found = NULL;
  sqlite3_stmt *statement = NULL;
  size_t slot = 0u;
  int row_present = 0;
  int row_state = -1;
  int state_equal = 0;
  int desired_applied = 0;
  int status = SQLITE_OK;
  int rc;
  if (!queue || queue->backend != FLOW_QUEUE_BACKEND_SQLITE || !key || !key[0] ||
      strlen(key) > TURBO_FLOW_SQLITE_BLOB_STORE_KEY_MAX || (!state && state_size > 0u) ||
      state_size > queue->sqlite_max_state_size || state_size > INT_MAX ||
      action < TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY || action > TURBO_FLOW_CLAIM_COMMIT_DROP ||
      ((action == TURBO_FLOW_CLAIM_COMMIT_STATE_ONLY) != (token == 0u)))
    return TURBO_EINVAL;
  turbo_mutex_lock(&queue->mutex);
  if (token != 0u) {
    found = flow_queue_claim_index_get_const(&queue->claim_index, token);
    if (!found) {
      turbo_mutex_unlock(&queue->mutex);
      return TURBO_EALREADY;
    }
    slot = *found;
    record = flow_queue_claim_records_at(&queue->claim_records, slot);
    if (!record || !record->active || record->token != token) {
      turbo_mutex_unlock(&queue->mutex);
      return TURBO_EPROTO;
    }
  }
  rc = flow_queue_sqlite_begin(queue);
  if (rc != TURBO_OK) goto done;
  if (record) {
    rc = flow_queue_sqlite_row_state_locked(queue, record->item.storage_id, &row_present,
                                            &row_state);
    if (rc != TURBO_OK) goto rollback;
    rc = flow_queue_sqlite_state_equal_locked(queue, key, state, state_size, &state_equal);
    if (rc != TURBO_OK) goto rollback;
    desired_applied =
        action == TURBO_FLOW_CLAIM_COMMIT_REQUEUE ? row_present && row_state == 0 : !row_present;
    if (desired_applied) {
      if (!state_equal) {
        rc = TURBO_EALREADY;
        goto rollback;
      }
      flow_queue_sqlite_rollback(queue);
      rc = TURBO_OK;
      goto apply_memory;
    }
    if (!row_present || row_state != 1) {
      rc = TURBO_EALREADY;
      goto rollback;
    }
  }
  rc = flow_queue_sqlite_state_upsert_locked(queue, key, state, state_size);
  if (rc != TURBO_OK) goto rollback;
  if (record) {
    const char *sql = action == TURBO_FLOW_CLAIM_COMMIT_REQUEUE
                          ? "UPDATE turbo_flow_queue_messages SET state=0 WHERE sequence=?1 AND "
                            "queue_name=?2 AND state=1"
                          : "DELETE FROM turbo_flow_queue_messages WHERE sequence=?1 AND "
                            "queue_name=?2 AND state=1";
    status = sqlite3_prepare_v2(queue->sqlite_db, sql, -1, &statement, NULL);
    if (status != SQLITE_OK) {
      rc = flow_queue_sqlite_status(status);
      goto rollback;
    }
    if (sqlite3_bind_int64(statement, 1, record->item.storage_id) != SQLITE_OK ||
        sqlite3_bind_text(statement, 2, queue->sqlite_queue_name, -1, SQLITE_STATIC) != SQLITE_OK) {
      rc = TURBO_EIO;
      goto rollback;
    }
    status = sqlite3_step(statement);
    (void)sqlite3_finalize(statement);
    statement = NULL;
    if (status != SQLITE_DONE || sqlite3_changes(queue->sqlite_db) != 1) {
      rc = status == SQLITE_DONE ? TURBO_EALREADY : flow_queue_sqlite_status(status);
      goto rollback;
    }
  }
  rc = flow_queue_sqlite_commit(queue);
  if (rc != TURBO_OK) {
    flow_queue_sqlite_rollback(queue);
    goto done;
  }
apply_memory:
  if (record) {
    if (action == TURBO_FLOW_CLAIM_COMMIT_ACK) queue->delivered += 1u;
    if (action == TURBO_FLOW_CLAIM_COMMIT_REQUEUE) queue->delivery_requeues += 1u;
    if (action != TURBO_FLOW_CLAIM_COMMIT_REQUEUE) queue->sqlite_depth -= 1u;
    rc = flow_queue_claim_release_locked(queue, slot, 1);
    if (rc != TURBO_OK) goto done;
    if (action == TURBO_FLOW_CLAIM_COMMIT_REQUEUE) turbo_cond_signal(&queue->not_empty);
    else turbo_cond_broadcast(&queue->not_full);
  }
  rc = TURBO_OK;
  goto done;
rollback:
  if (statement) (void)sqlite3_finalize(statement);
  flow_queue_sqlite_rollback(queue);
done:
  turbo_mutex_unlock(&queue->mutex);
  return rc;
}

int turbo_flow_queue_claim_settler(turbo_flow_queue_t *queue, turbo_flow_claim_settler_t *out) {
  if (!queue || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  *out = (turbo_flow_claim_settler_t)TURBO_FLOW_CLAIM_SETTLER_INIT;
  out->ctx = queue;
  out->ack = flow_queue_claim_ack_adapter;
  out->requeue = flow_queue_claim_requeue_adapter;
  out->drop = flow_queue_claim_drop_adapter;
  if (queue->backend == FLOW_QUEUE_BACKEND_SQLITE) {
    out->max_state_size = queue->sqlite_max_state_size;
    out->load_state = flow_queue_sqlite_state_load_adapter;
    out->commit_state = flow_queue_sqlite_state_commit_adapter;
  }
  return TURBO_OK;
}

static void flow_queue_snapshot_locked(const turbo_flow_queue_t *queue,
                                       turbo_flow_queue_snapshot_t *out) {
  out->depth = flow_queue_occupancy(queue);
  out->capacity = queue->capacity;
  out->enqueued = queue->enqueued;
  out->delivered = queue->delivered;
  out->dropped_oldest = queue->dropped_oldest;
  out->enqueue_failures = queue->enqueue_failures;
  out->publish_failures = queue->publish_failures;
}

static void flow_queue_ack_snapshot_locked(const turbo_flow_queue_t *queue,
                                           turbo_flow_queue_ack_snapshot_t *out) {
  out->accept_acks = queue->enqueued;
  out->delivery_acks = queue->delivered;
  out->delivery_requeues = queue->delivery_requeues;
  out->delivery_requeue_failures = queue->delivery_requeue_failures;
}

int turbo_flow_queue_snapshot(const turbo_flow_queue_t *queue, turbo_flow_queue_snapshot_t *out) {
  turbo_flow_queue_t *mutable_queue = (turbo_flow_queue_t *)queue;
  if (!queue || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  turbo_mutex_lock(&mutable_queue->mutex);
  flow_queue_snapshot_locked(queue, out);
  turbo_mutex_unlock(&mutable_queue->mutex);
  return TURBO_OK;
}

int turbo_flow_queue_ack_snapshot(const turbo_flow_queue_t *queue,
                                  turbo_flow_queue_ack_snapshot_t *out) {
  turbo_flow_queue_t *mutable_queue = (turbo_flow_queue_t *)queue;
  if (!queue || !out || out->size < sizeof(*out)) return TURBO_EINVAL;
  turbo_mutex_lock(&mutable_queue->mutex);
  flow_queue_ack_snapshot_locked(queue, out);
  turbo_mutex_unlock(&mutable_queue->mutex);
  return TURBO_OK;
}

int flow_queue_bind_channel(turbo_flow_queue_t *queue, const char *channel_name) {
  tstr_t copy;
  if (!queue || !channel_name || !channel_name[0]) return TURBO_EINVAL;
  copy = tstr_dup(channel_name);
  if (!copy) return TURBO_ENOMEM;
  turbo_mutex_lock(&queue->mutex);
  if (queue->channel_name) {
    int rc = strcmp(queue->channel_name, channel_name) == 0 ? TURBO_OK : TURBO_EALREADY;
    turbo_mutex_unlock(&queue->mutex);
    tstr_free(copy);
    return rc;
  }
  queue->channel_name = copy;
  turbo_mutex_unlock(&queue->mutex);
  return TURBO_OK;
}

int flow_queue_channel_matches(const turbo_flow_queue_t *queue, const char *channel_name) {
  turbo_flow_queue_t *mutable_queue = (turbo_flow_queue_t *)queue;
  int matches;
  if (!queue || !channel_name || !channel_name[0]) return 0;
  turbo_mutex_lock(&mutable_queue->mutex);
  matches = queue->channel_name && strcmp(queue->channel_name, channel_name) == 0;
  turbo_mutex_unlock(&mutable_queue->mutex);
  return matches;
}

typedef struct flow_sqlite_blob_store_s {
  sqlite3 *database;
  tstr_t key;
  size_t max_value_size;
} flow_sqlite_blob_store_t;

static int flow_sqlite_blob_store_key(const flow_sqlite_blob_store_t *store, const char *key) {
  return store && store->key && key && strcmp(store->key, key) == 0 ? TURBO_OK : TURBO_EINVAL;
}

static int flow_sqlite_blob_store_load(void *ctx, const char *key, uint8_t *out, size_t capacity,
                                       size_t *out_size) {
  flow_sqlite_blob_store_t *store = (flow_sqlite_blob_store_t *)ctx;
  sqlite3_stmt *statement = NULL;
  int status;
  int rc = TURBO_EIO;
  if (!out_size || (!out && capacity > 0u)) return TURBO_EINVAL;
  *out_size = 0u;
  if (flow_sqlite_blob_store_key(store, key) != TURBO_OK) return TURBO_EINVAL;
  status = sqlite3_prepare_v2(store->database,
                              "SELECT value FROM turbo_flow_blob_store WHERE store_key=?1", -1,
                              &statement, NULL);
  if (status != SQLITE_OK) goto done;
  if (sqlite3_bind_text(statement, 1, store->key, -1, SQLITE_STATIC) != SQLITE_OK) goto done;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) {
    rc = TURBO_ENOENT;
    goto done;
  }
  if (status != SQLITE_ROW) goto done;
  {
    int value_size = sqlite3_column_bytes(statement, 0);
    const void *value = sqlite3_column_blob(statement, 0);
    if (value_size < 0 || (size_t)value_size > store->max_value_size) {
      rc = TURBO_EMSGSIZE;
      goto done;
    }
    *out_size = (size_t)value_size;
    if (!out || capacity < *out_size) {
      rc = TURBO_ENOSPC;
      goto done;
    }
    if (*out_size > 0u) memcpy(out, value, *out_size);
  }
  rc = TURBO_OK;
done:
  if (statement) (void)sqlite3_finalize(statement);
  return rc;
}

static int flow_sqlite_blob_store_commit(void *ctx, const char *key, const uint8_t *data,
                                         size_t data_size) {
  static const uint8_t empty_value = 0u;
  flow_sqlite_blob_store_t *store = (flow_sqlite_blob_store_t *)ctx;
  sqlite3_stmt *statement = NULL;
  int status;
  int rc = TURBO_EIO;
  if (flow_sqlite_blob_store_key(store, key) != TURBO_OK || (!data && data_size > 0u))
    return TURBO_EINVAL;
  if (data_size > store->max_value_size || data_size > INT_MAX) return TURBO_EMSGSIZE;
  if (sqlite3_exec(store->database, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK)
    return TURBO_EIO;
  status = sqlite3_prepare_v2(store->database,
                              "INSERT INTO turbo_flow_blob_store(store_key,value) VALUES(?1,?2) "
                              "ON CONFLICT(store_key) DO UPDATE SET value=excluded.value",
                              -1, &statement, NULL);
  if (status != SQLITE_OK) goto rollback;
  if (sqlite3_bind_text(statement, 1, store->key, -1, SQLITE_STATIC) != SQLITE_OK ||
      sqlite3_bind_blob(statement, 2, data_size > 0u ? data : &empty_value, (int)data_size,
                        SQLITE_TRANSIENT) != SQLITE_OK)
    goto rollback;
  status = sqlite3_step(statement);
  (void)sqlite3_finalize(statement);
  statement = NULL;
  if (status != SQLITE_DONE) goto rollback;
  if (sqlite3_exec(store->database, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto rollback;
  return TURBO_OK;

rollback:
  if (statement) (void)sqlite3_finalize(statement);
  (void)sqlite3_exec(store->database, "ROLLBACK", NULL, NULL, NULL);
  return rc;
}

int turbo_flow_sqlite_blob_store_create(const turbo_flow_sqlite_blob_store_config_t *config,
                                        turbo_flow_blob_store_t *out) {
  static const char schema[] = "PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;"
                               "CREATE TABLE IF NOT EXISTS turbo_flow_blob_store("
                               "store_key TEXT PRIMARY KEY NOT NULL,value BLOB NOT NULL);";
  flow_sqlite_blob_store_t *store;
  size_t max_value_size;
  int status;
  if (!config || !out || out->size < sizeof(*out) || out->ctx || !config->database_path ||
      !config->database_path[0] || !config->key || !config->key[0] ||
      strlen(config->key) > TURBO_FLOW_SQLITE_BLOB_STORE_KEY_MAX || config->busy_timeout_ms < 0 ||
      config->max_value_size > INT_MAX) {
    return TURBO_EINVAL;
  }
  max_value_size = config->max_value_size ? config->max_value_size
                                          : TURBO_FLOW_SQLITE_BLOB_STORE_DEFAULT_MAX_VALUE_SIZE;
  store = (flow_sqlite_blob_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  status =
      sqlite3_open_v2(config->database_path, &store->database,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
  if (status != SQLITE_OK ||
      (config->busy_timeout_ms > 0 &&
       sqlite3_busy_timeout(store->database, config->busy_timeout_ms) != SQLITE_OK) ||
      sqlite3_exec(store->database, schema, NULL, NULL, NULL) != SQLITE_OK) {
    if (store->database) (void)sqlite3_close(store->database);
    free(store);
    return TURBO_EIO;
  }
  store->key = tstr_dup(config->key);
  store->max_value_size = max_value_size;
  if (!store->key) {
    (void)sqlite3_close(store->database);
    free(store);
    return TURBO_ENOMEM;
  }
  out->max_value_size = max_value_size;
  out->ctx = store;
  out->load = flow_sqlite_blob_store_load;
  out->commit = flow_sqlite_blob_store_commit;
  return TURBO_OK;
}

void turbo_flow_sqlite_blob_store_destroy(turbo_flow_blob_store_t *store) {
  flow_sqlite_blob_store_t *sqlite_store;
  if (!store || store->size < sizeof(*store) || !store->ctx) return;
  sqlite_store = (flow_sqlite_blob_store_t *)store->ctx;
  (void)sqlite3_close(sqlite_store->database);
  tstr_freep(&sqlite_store->key);
  free(sqlite_store);
  *store = (turbo_flow_blob_store_t)TURBO_FLOW_BLOB_STORE_INIT;
}

typedef struct flow_sqlite_record_store_s {
  sqlite3 *database;
  tstr_t namespace_name;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_batch_size;
  size_t max_records;
  turbo_hash_map_t mutation_keys;
  int mutation_keys_initialized;
} flow_sqlite_record_store_t;

static size_t flow_record_key_hash(const void *key, size_t key_size, void *ctx) {
  const tstr_v *view = (const tstr_v *)key;
  (void)key_size;
  (void)ctx;
  return turbo_hash_bytes(view->data, view->len, NULL);
}

static bool flow_record_key_equal(const void *left, const void *right, size_t key_size, void *ctx) {
  const tstr_v *a = (const tstr_v *)left;
  const tstr_v *b = (const tstr_v *)right;
  (void)key_size;
  (void)ctx;
  return a->len == b->len && (a->len == 0u || memcmp(a->data, b->data, a->len) == 0);
}

static int flow_sqlite_record_mutations_validate(
    flow_sqlite_record_store_t *store, const turbo_flow_record_mutation_t *mutations,
    size_t mutation_count) {
  static const uint8_t present = 1u;
  if (!store || !mutations || mutation_count == 0u || mutation_count > store->max_batch_size)
    return TURBO_EINVAL;
  turbo_hash_map_clear(&store->mutation_keys);
  for (size_t i = 0u; i < mutation_count; ++i) {
    const turbo_flow_record_mutation_t *mutation = &mutations[i];
    tstr_v key;
    int rc;
    if (mutation->size < sizeof(*mutation) || !mutation->key || mutation->key_size == 0u ||
        mutation->key_size > store->max_key_size ||
        mutation->expected_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
        mutation->next_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX)
      return TURBO_EINVAL;
    if (mutation->kind == TURBO_FLOW_RECORD_PUT) {
      if ((!mutation->value && mutation->value_size != 0u) ||
          mutation->value_size > store->max_value_size ||
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
    if (turbo_hash_map_contains(&store->mutation_keys, &key)) return TURBO_EINVAL;
    rc = turbo_hash_map_put(&store->mutation_keys, &key, &present);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int flow_sqlite_record_current_revision(flow_sqlite_record_store_t *store,
                                               sqlite3_stmt *statement,
                                               const turbo_flow_record_mutation_t *mutation,
                                               uint64_t *revision) {
  int status;
  if (!store || !statement || !mutation || !revision) return TURBO_EINVAL;
  *revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
  sqlite3_reset(statement);
  sqlite3_clear_bindings(statement);
  if (sqlite3_bind_text(statement, 1, store->namespace_name, -1, SQLITE_STATIC) != SQLITE_OK ||
      sqlite3_bind_blob(statement, 2, mutation->key, (int)mutation->key_size, SQLITE_STATIC) !=
          SQLITE_OK)
    return TURBO_EIO;
  status = sqlite3_step(statement);
  if (status == SQLITE_DONE) return TURBO_OK;
  if (status != SQLITE_ROW)
    return status == SQLITE_BUSY || status == SQLITE_LOCKED ? TURBO_EBUSY : TURBO_EIO;
  if (sqlite3_column_type(statement, 0) != SQLITE_INTEGER) return TURBO_EPROTO;
  {
    sqlite3_int64 stored = sqlite3_column_int64(statement, 0);
    if (stored <= 0) return TURBO_EPROTO;
    *revision = (uint64_t)stored;
  }
  return TURBO_OK;
}

static int flow_sqlite_record_store_scan(void *ctx, turbo_flow_record_visit_fn visit,
                                         void *visit_ctx) {
  flow_sqlite_record_store_t *store = (flow_sqlite_record_store_t *)ctx;
  sqlite3_stmt *statement = NULL;
  size_t count = 0u;
  int status;
  int rc = TURBO_EIO;
  if (!store || !visit) return TURBO_EINVAL;
  status = sqlite3_exec(store->database, "BEGIN", NULL, NULL, NULL);
  if (status != SQLITE_OK) return flow_queue_sqlite_status(status);
  status = sqlite3_prepare_v2(
      store->database,
      "SELECT record_key,revision,value FROM turbo_flow_record_store "
      "WHERE namespace_name=?1 ORDER BY record_key",
      -1, &statement, NULL);
  if (status != SQLITE_OK ||
      sqlite3_bind_text(statement, 1, store->namespace_name, -1, SQLITE_STATIC) != SQLITE_OK)
    goto rollback;
  while ((status = sqlite3_step(statement)) == SQLITE_ROW) {
    turbo_flow_record_view_t record = TURBO_FLOW_RECORD_VIEW_INIT;
    int key_size = sqlite3_column_bytes(statement, 0);
    int value_size = sqlite3_column_bytes(statement, 2);
    sqlite3_int64 revision = sqlite3_column_int64(statement, 1);
    if (count >= store->max_records) {
      rc = TURBO_ENOSPC;
      goto rollback;
    }
    if (key_size <= 0 || (size_t)key_size > store->max_key_size || value_size < 0 ||
        (size_t)value_size > store->max_value_size || revision <= 0 ||
        sqlite3_column_type(statement, 0) != SQLITE_BLOB ||
        sqlite3_column_type(statement, 1) != SQLITE_INTEGER ||
        sqlite3_column_type(statement, 2) != SQLITE_BLOB) {
      rc = TURBO_EPROTO;
      goto rollback;
    }
    record.key = (const uint8_t *)sqlite3_column_blob(statement, 0);
    record.key_size = (size_t)key_size;
    record.revision = (uint64_t)revision;
    record.value = (const uint8_t *)sqlite3_column_blob(statement, 2);
    record.value_size = (size_t)value_size;
    rc = visit(visit_ctx, &record);
    if (rc != TURBO_OK) goto rollback;
    ++count;
  }
  if (status != SQLITE_DONE) {
    rc = flow_queue_sqlite_status(status);
    goto rollback;
  }
  (void)sqlite3_finalize(statement);
  statement = NULL;
  status = sqlite3_exec(store->database, "COMMIT", NULL, NULL, NULL);
  if (status == SQLITE_OK) return TURBO_OK;
  rc = flow_queue_sqlite_status(status);

rollback:
  if (statement) (void)sqlite3_finalize(statement);
  (void)sqlite3_exec(store->database, "ROLLBACK", NULL, NULL, NULL);
  return rc;
}

static int flow_sqlite_record_store_commit(void *ctx,
                                           const turbo_flow_record_mutation_t *mutations,
                                           size_t mutation_count) {
  static const uint8_t empty_value = 0u;
  flow_sqlite_record_store_t *store = (flow_sqlite_record_store_t *)ctx;
  sqlite3_stmt *revision_statement = NULL;
  sqlite3_stmt *count_statement = NULL;
  sqlite3_stmt *put_statement = NULL;
  sqlite3_stmt *delete_statement = NULL;
  sqlite3_int64 record_count;
  ptrdiff_t count_delta = 0;
  int status;
  int rc;
  rc = flow_sqlite_record_mutations_validate(store, mutations, mutation_count);
  if (rc != TURBO_OK) return rc;
  status = sqlite3_exec(store->database, "BEGIN IMMEDIATE", NULL, NULL, NULL);
  if (status != SQLITE_OK) return flow_queue_sqlite_status(status);
  status = sqlite3_prepare_v2(
      store->database,
      "SELECT revision FROM turbo_flow_record_store WHERE namespace_name=?1 AND record_key=?2",
      -1, &revision_statement, NULL);
  if (status != SQLITE_OK) goto sqlite_fail;
  for (size_t i = 0u; i < mutation_count; ++i) {
    uint64_t current_revision;
    rc = flow_sqlite_record_current_revision(store, revision_statement, &mutations[i],
                                             &current_revision);
    if (rc != TURBO_OK) goto rollback;
    if (current_revision != mutations[i].expected_revision) {
      rc = TURBO_EBUSY;
      goto rollback;
    }
    if (mutations[i].kind == TURBO_FLOW_RECORD_PUT &&
        current_revision == TURBO_FLOW_RECORD_REVISION_ABSENT)
      ++count_delta;
    else if (mutations[i].kind == TURBO_FLOW_RECORD_DELETE)
      --count_delta;
  }
  (void)sqlite3_finalize(revision_statement);
  revision_statement = NULL;
  status = sqlite3_prepare_v2(
      store->database,
      "SELECT COUNT(*) FROM turbo_flow_record_store WHERE namespace_name=?1", -1,
      &count_statement, NULL);
  if (status != SQLITE_OK ||
      sqlite3_bind_text(count_statement, 1, store->namespace_name, -1, SQLITE_STATIC) != SQLITE_OK)
    goto sqlite_fail;
  status = sqlite3_step(count_statement);
  if (status != SQLITE_ROW || sqlite3_column_type(count_statement, 0) != SQLITE_INTEGER)
    goto sqlite_fail;
  record_count = sqlite3_column_int64(count_statement, 0);
  (void)sqlite3_finalize(count_statement);
  count_statement = NULL;
  if (record_count < 0 || count_delta < -(ptrdiff_t)record_count ||
      (count_delta > 0 && (uint64_t)record_count + (uint64_t)count_delta > store->max_records)) {
    rc = TURBO_ENOSPC;
    goto rollback;
  }
  status = sqlite3_prepare_v2(
      store->database,
      "INSERT INTO turbo_flow_record_store(namespace_name,record_key,revision,value) "
      "VALUES(?1,?2,?3,?4) ON CONFLICT(namespace_name,record_key) DO UPDATE SET "
      "revision=excluded.revision,value=excluded.value",
      -1, &put_statement, NULL);
  if (status != SQLITE_OK) goto sqlite_fail;
  status = sqlite3_prepare_v2(
      store->database,
      "DELETE FROM turbo_flow_record_store WHERE namespace_name=?1 AND record_key=?2 "
      "AND revision=?3",
      -1, &delete_statement, NULL);
  if (status != SQLITE_OK) goto sqlite_fail;
  for (size_t i = 0u; i < mutation_count; ++i) {
    const turbo_flow_record_mutation_t *mutation = &mutations[i];
    sqlite3_stmt *statement = mutation->kind == TURBO_FLOW_RECORD_PUT ? put_statement
                                                                      : delete_statement;
    sqlite3_reset(statement);
    sqlite3_clear_bindings(statement);
    if (sqlite3_bind_text(statement, 1, store->namespace_name, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_blob(statement, 2, mutation->key, (int)mutation->key_size, SQLITE_STATIC) !=
            SQLITE_OK)
      goto sqlite_fail;
    if (mutation->kind == TURBO_FLOW_RECORD_PUT) {
      if (sqlite3_bind_int64(statement, 3, (sqlite3_int64)mutation->next_revision) != SQLITE_OK ||
          sqlite3_bind_blob(statement, 4,
                            mutation->value_size != 0u ? mutation->value : &empty_value,
                            (int)mutation->value_size, SQLITE_STATIC) != SQLITE_OK)
        goto sqlite_fail;
    } else if (sqlite3_bind_int64(statement, 3,
                                  (sqlite3_int64)mutation->expected_revision) != SQLITE_OK) {
      goto sqlite_fail;
    }
    status = sqlite3_step(statement);
    if (status != SQLITE_DONE ||
        (mutation->kind == TURBO_FLOW_RECORD_DELETE && sqlite3_changes(store->database) != 1))
      goto sqlite_fail;
  }
  (void)sqlite3_finalize(put_statement);
  put_statement = NULL;
  (void)sqlite3_finalize(delete_statement);
  delete_statement = NULL;
  status = sqlite3_exec(store->database, "COMMIT", NULL, NULL, NULL);
  if (status == SQLITE_OK) return TURBO_OK;
  rc = flow_queue_sqlite_status(status);
  goto rollback;

sqlite_fail:
  rc = status == SQLITE_BUSY || status == SQLITE_LOCKED ? TURBO_EBUSY : TURBO_EIO;
rollback:
  if (revision_statement) (void)sqlite3_finalize(revision_statement);
  if (count_statement) (void)sqlite3_finalize(count_statement);
  if (put_statement) (void)sqlite3_finalize(put_statement);
  if (delete_statement) (void)sqlite3_finalize(delete_statement);
  (void)sqlite3_exec(store->database, "ROLLBACK", NULL, NULL, NULL);
  return rc;
}

int turbo_flow_sqlite_record_store_create(
    const turbo_flow_sqlite_record_store_config_t *config, turbo_flow_record_store_t *out) {
  static const char schema[] =
      "PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;"
      "CREATE TABLE IF NOT EXISTS turbo_flow_record_store("
      "namespace_name TEXT NOT NULL,record_key BLOB NOT NULL,revision INTEGER NOT NULL "
      "CHECK(revision>0),value BLOB NOT NULL,PRIMARY KEY(namespace_name,record_key)) WITHOUT ROWID;";
  flow_sqlite_record_store_t *store;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_batch_size;
  int status;
  int rc;
  if (!config || !out || out->size < sizeof(*out) || out->ctx || !config->database_path ||
      !config->database_path[0] || !config->namespace_name || !config->namespace_name[0] ||
      strlen(config->namespace_name) > TURBO_FLOW_SQLITE_RECORD_STORE_NAMESPACE_MAX ||
      config->busy_timeout_ms < 0 || config->max_key_size > INT_MAX ||
      config->max_value_size > INT_MAX ||
      config->max_batch_size > TURBO_FLOW_QUEUE_MAX_CAPACITY ||
      config->max_records == 0u || config->max_records > INT_MAX)
    return TURBO_EINVAL;
  max_key_size = config->max_key_size ? config->max_key_size
                                      : TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_KEY_SIZE;
  max_value_size = config->max_value_size
                       ? config->max_value_size
                       : TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_VALUE_SIZE;
  max_batch_size = config->max_batch_size
                       ? config->max_batch_size
                       : TURBO_FLOW_SQLITE_RECORD_STORE_DEFAULT_MAX_BATCH_SIZE;
  if (max_key_size == 0u || max_value_size == 0u || max_batch_size == 0u) return TURBO_EINVAL;
  store = (flow_sqlite_record_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  status = sqlite3_open_v2(config->database_path, &store->database,
                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                           NULL);
  if (status != SQLITE_OK ||
      (config->busy_timeout_ms > 0 &&
       sqlite3_busy_timeout(store->database, config->busy_timeout_ms) != SQLITE_OK) ||
      sqlite3_exec(store->database, schema, NULL, NULL, NULL) != SQLITE_OK) {
    if (store->database) (void)sqlite3_close(store->database);
    free(store);
    return TURBO_EIO;
  }
  store->namespace_name = tstr_dup(config->namespace_name);
  store->max_key_size = max_key_size;
  store->max_value_size = max_value_size;
  store->max_batch_size = max_batch_size;
  store->max_records = config->max_records;
  rc = turbo_hash_map_init(&store->mutation_keys, sizeof(tstr_v), sizeof(uint8_t),
                           flow_record_key_hash, flow_record_key_equal, NULL);
  if (rc == TURBO_OK) {
    store->mutation_keys_initialized = 1;
    rc = turbo_hash_map_reserve(&store->mutation_keys, max_batch_size);
  }
  if (!store->namespace_name || rc != TURBO_OK) {
    int failure = !store->namespace_name ? TURBO_ENOMEM : rc;
    if (store->mutation_keys_initialized) turbo_hash_map_destroy(&store->mutation_keys);
    tstr_freep(&store->namespace_name);
    (void)sqlite3_close(store->database);
    free(store);
    return failure;
  }
  out->api_version = TURBO_FLOW_RECORD_STORE_API_VERSION;
  out->capabilities = TURBO_FLOW_RECORD_STORE_DURABLE | TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH;
  out->max_key_size = max_key_size;
  out->max_value_size = max_value_size;
  out->max_batch_size = max_batch_size;
  out->max_records = config->max_records;
  out->ctx = store;
  out->scan = flow_sqlite_record_store_scan;
  out->commit = flow_sqlite_record_store_commit;
  return TURBO_OK;
}

void turbo_flow_sqlite_record_store_destroy(turbo_flow_record_store_t *store) {
  flow_sqlite_record_store_t *sqlite_store;
  if (!store || store->size < sizeof(*store) || !store->ctx) return;
  sqlite_store = (flow_sqlite_record_store_t *)store->ctx;
  if (sqlite_store->mutation_keys_initialized)
    turbo_hash_map_destroy(&sqlite_store->mutation_keys);
  (void)sqlite3_close(sqlite_store->database);
  tstr_freep(&sqlite_store->namespace_name);
  free(sqlite_store);
  *store = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
}
