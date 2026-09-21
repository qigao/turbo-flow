#include "turbo_flow_inbox.h"

#include "turbo_flow_stl_error_internal.h"

#include <cstl/vec.h>
#include <salts_buffer.h>
#include <salts_error.h>
#include <salts_thread.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef enum flow_inbox_record_phase_e {
  FLOW_INBOX_RECORD_PENDING = 0,
  FLOW_INBOX_RECORD_CLAIMED,
  FLOW_INBOX_RECORD_FAILED,
  FLOW_INBOX_RECORD_TOMBSTONE
} flow_inbox_record_phase_t;

typedef struct flow_inbox_memory_record_s {
  uint64_t record_id;
  uint64_t claim_token;
  int failure_status;
  turbo_flow_inbox_failure_kind_t failure_kind;
  turbo_flow_inbox_terminal_kind_t terminal_kind;
  flow_inbox_record_phase_t phase;
  size_t retained_bytes;
  mem_buffer_t *storage;
  turbo_flow_inbox_record_t view;
} flow_inbox_memory_record_t;

typedef struct flow_inbox_memory_slot_s {
  flow_inbox_memory_record_t *record;
  bool reserved;
} flow_inbox_memory_slot_t;

typedef struct flow_inbox_memory_s {
  salts_mutex_t mutex;
  vec_t records;
  turbo_flow_inbox_memory_config_t config;
  size_t retained_bytes;
  size_t reserved_bytes;
  size_t reserved_records;
  size_t live_records;
  size_t history_records;
  size_t in_flight_claims;
  uint64_t next_record_id;
  uint64_t next_claim_token;
  uint64_t admitted;
  uint64_t completed;
  uint64_t failed;
  uint64_t retried;
  uint64_t discarded;
  bool accepting;
  bool record_id_exhausted;
  bool claim_token_exhausted;
} flow_inbox_memory_t;

static int flow_inbox_view_equal(vstr left, vstr right);

static int flow_inbox_handle_valid(const turbo_flow_inbox_t *inbox) {
  const turbo_flow_inbox_ops_v2_t *ops;
  if (!inbox || inbox->size != sizeof(*inbox) || inbox->version != TURBO_FLOW_INBOX_API_VERSION ||
      !inbox->ops || !inbox->ctx) {
    return 0;
  }
  ops = inbox->ops;
  return ops->size == sizeof(*ops) && ops->version == TURBO_FLOW_INBOX_API_VERSION && ops->admit &&
         ops->claim && ops->claim_ex && ops->complete && ops->fail && ops->retry && ops->discard &&
         ops->forget && ops->scan_failed && ops->scan_history && ops->close && ops->snapshot &&
         ops->destroy;
}

static int flow_inbox_claim_request_valid(const turbo_flow_inbox_claim_request_t *request) {
  size_t total_bytes = 0u;
  if (!request || request->size != sizeof(*request) ||
      request->version != TURBO_FLOW_INBOX_API_VERSION)
    return 0;
  if (request->ordering == TURBO_FLOW_INBOX_CLAIM_ORDER_GLOBAL)
    return request->excluded_partition_count == 0u;
  if (request->ordering != TURBO_FLOW_INBOX_CLAIM_ORDER_PARTITION_KEY ||
      request->excluded_partition_count > TURBO_FLOW_INBOX_CLAIM_MAX_EXCLUDED_PARTITIONS ||
      (request->excluded_partition_count != 0u && !request->excluded_partitions))
    return 0;
  for (size_t i = 0u; i < request->excluded_partition_count; ++i) {
    const vstr key = request->excluded_partitions[i];
    if (!key.data || key.len == 0u ||
        key.len > TURBO_FLOW_INBOX_CLAIM_MAX_EXCLUDED_BYTES - total_bytes)
      return 0;
    total_bytes += key.len;
    for (size_t j = 0u; j < i; ++j)
      if (flow_inbox_view_equal(key, request->excluded_partitions[j])) return 0;
  }
  return 1;
}

static int flow_inbox_claim_partition_excluded(
    const turbo_flow_inbox_record_t *record,
    const turbo_flow_inbox_claim_request_t *request) {
  if (!record || !request ||
      request->ordering != TURBO_FLOW_INBOX_CLAIM_ORDER_PARTITION_KEY)
    return 0;
  for (size_t i = 0u; i < request->excluded_partition_count; ++i)
    if (flow_inbox_view_equal(record->partition_key, request->excluded_partitions[i])) return 1;
  return 0;
}

static int flow_inbox_record_valid(const turbo_flow_inbox_record_t *record) {
  if (!record || record->size != sizeof(*record) ||
      record->version != TURBO_FLOW_INBOX_API_VERSION) {
    return SALTS_EINVAL;
  }
  if (!record->envelope_schema ||
      strcmp(record->envelope_schema, TURBO_FLOW_INBOX_RECORD_SCHEMA) != 0 ||
      record->envelope_schema_version != TURBO_FLOW_INBOX_RECORD_SCHEMA_VERSION) {
    return SALTS_EPROTO;
  }
  if (!record->source_id.data || record->source_id.len == 0u ||
      !record->partition_key.data || record->partition_key.len == 0u ||
      !record->admission_id.data || record->admission_id.len == 0u ||
      (record->correlation.len != 0u && !record->correlation.data) ||
      (record->payload.len != 0u && !record->payload.data)) {
    return SALTS_EINVAL;
  }
  if (record->content.size != sizeof(record->content) ||
      record->content.domain != TURBO_FLOW_DOMAIN_DATA ||
      (record->content.flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) == 0u) {
    return SALTS_EINVAL;
  }
  return turbo_flow_content_descriptor_check(&record->content);
}

void turbo_flow_inbox_record_init(turbo_flow_inbox_record_t *record) {
  if (!record) return;
  memset(record, 0, sizeof(*record));
  record->size = sizeof(*record);
  record->version = TURBO_FLOW_INBOX_API_VERSION;
  record->envelope_schema = TURBO_FLOW_INBOX_RECORD_SCHEMA;
  record->envelope_schema_version = TURBO_FLOW_INBOX_RECORD_SCHEMA_VERSION;
  record->content = (turbo_flow_content_descriptor_t)TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
}

turbo_flow_inbox_memory_config_t turbo_flow_inbox_memory_config_default(void) {
  turbo_flow_inbox_memory_config_t config;
  memset(&config, 0, sizeof(config));
  config.size = sizeof(config);
  config.version = TURBO_FLOW_INBOX_API_VERSION;
  config.max_records = TURBO_FLOW_INBOX_MEMORY_DEFAULT_MAX_RECORDS;
  config.max_total_bytes = TURBO_FLOW_INBOX_MEMORY_DEFAULT_MAX_TOTAL_BYTES;
  config.max_record_bytes = TURBO_FLOW_INBOX_MEMORY_DEFAULT_MAX_RECORD_BYTES;
  config.max_claims = TURBO_FLOW_INBOX_MEMORY_DEFAULT_MAX_CLAIMS;
  return config;
}

static int flow_inbox_memory_config_valid(const turbo_flow_inbox_memory_config_t *config) {
  return config && config->size == sizeof(*config) &&
         config->version == TURBO_FLOW_INBOX_API_VERSION && config->max_records != 0u &&
         config->max_records <= TURBO_FLOW_INBOX_MEMORY_MAX_RECORDS &&
         config->max_records <= SIZE_MAX / sizeof(flow_inbox_memory_slot_t) &&
         config->max_total_bytes != 0u &&
         config->max_total_bytes <= TURBO_FLOW_INBOX_MEMORY_MAX_TOTAL_BYTES &&
         config->max_record_bytes != 0u && config->max_record_bytes <= config->max_total_bytes &&
         config->max_claims != 0u && config->max_claims <= config->max_records;
}

static void flow_inbox_memory_record_destroy(flow_inbox_memory_record_t *record) {
  if (!record) return;
  mem_buffer_release(record->storage);
  free(record);
}

static int flow_inbox_view_equal(vstr left, vstr right) {
  return left.len == right.len && (left.len == 0u || memcmp(left.data, right.data, left.len) == 0);
}

static int flow_inbox_record_equal(const turbo_flow_inbox_record_t *left,
                                   const turbo_flow_inbox_record_t *right) {
  const turbo_flow_content_descriptor_t *a = &left->content;
  const turbo_flow_content_descriptor_t *b = &right->content;
  return left->source_sequence == right->source_sequence &&
         left->timestamp_ns == right->timestamp_ns && left->message_type == right->message_type &&
         left->message_flags == right->message_flags &&
         flow_inbox_view_equal(left->source_id, right->source_id) &&
         flow_inbox_view_equal(left->partition_key, right->partition_key) &&
         flow_inbox_view_equal(left->admission_id, right->admission_id) &&
         flow_inbox_view_equal(left->correlation, right->correlation) &&
         flow_inbox_view_equal(left->payload, right->payload) && a->domain == b->domain &&
         a->profile == b->profile && a->encoding == b->encoding && a->flags == b->flags &&
         a->schema_version == b->schema_version && strcmp(a->media_type, b->media_type) == 0 &&
         strcmp(a->schema_name, b->schema_name) == 0 && strcmp(a->type_name, b->type_name) == 0 &&
         strcmp(a->identity, b->identity) == 0;
}

static int flow_inbox_memory_record_copy(const turbo_flow_inbox_record_t *source,
                                         size_t retained_bytes, flow_inbox_memory_record_t **out) {
  flow_inbox_memory_record_t *record;
  size_t allocation_size;
  char *bytes;
  if (!source || !out) return SALTS_EINVAL;
  *out = NULL;
  allocation_size = retained_bytes == 0u ? 1u : retained_bytes;
  record = (flow_inbox_memory_record_t *)calloc(1u, sizeof(*record));
  if (!record) return SALTS_ENOMEM;
  record->storage = mem_get_buffer(mem_global(), allocation_size);
  if (!record->storage) {
    free(record);
    return SALTS_ENOMEM;
  }
  bytes = mem_buffer_data(record->storage);
  memcpy(bytes, source->source_id.data, source->source_id.len);
  memcpy(bytes + source->source_id.len, source->partition_key.data, source->partition_key.len);
  memcpy(bytes + source->source_id.len + source->partition_key.len,
         source->admission_id.data, source->admission_id.len);
  if (source->correlation.len != 0u)
    memcpy(bytes + source->source_id.len + source->partition_key.len +
               source->admission_id.len,
           source->correlation.data, source->correlation.len);
  if (source->payload.len != 0u)
    memcpy(bytes + source->source_id.len + source->partition_key.len +
               source->admission_id.len + source->correlation.len,
           source->payload.data, source->payload.len);
  mem_set_used(record->storage, retained_bytes);
  record->phase = FLOW_INBOX_RECORD_PENDING;
  record->failure_status = SALTS_OK;
  record->failure_kind = TURBO_FLOW_INBOX_FAILURE_PROCESSING;
  record->retained_bytes = retained_bytes;
  record->view = *source;
  record->view.envelope_schema = TURBO_FLOW_INBOX_RECORD_SCHEMA;
  record->view.source_id = vstr_from_buf(bytes, source->source_id.len);
  record->view.partition_key =
      vstr_from_buf(bytes + source->source_id.len, source->partition_key.len);
  record->view.admission_id =
      vstr_from_buf(bytes + source->source_id.len + source->partition_key.len,
                    source->admission_id.len);
  record->view.correlation =
      vstr_from_buf(bytes + source->source_id.len + source->partition_key.len +
                        source->admission_id.len,
                    source->correlation.len);
  record->view.payload =
      vstr_from_buf(bytes + source->source_id.len + source->partition_key.len +
                        source->admission_id.len + source->correlation.len,
                    source->payload.len);
  *out = record;
  return SALTS_OK;
}

static flow_inbox_memory_record_t *flow_inbox_memory_find(flow_inbox_memory_t *memory,
                                                          uint64_t record_id, size_t *index_out) {
  size_t count = vec_size(&memory->records);
  for (size_t index = 0u; index < count; ++index) {
    flow_inbox_memory_slot_t *slot = (flow_inbox_memory_slot_t *)vec_at(&memory->records, index);
    if (slot && slot->record && slot->record->record_id == record_id) {
      if (index_out) *index_out = index;
      return slot->record;
    }
  }
  return NULL;
}

static flow_inbox_memory_record_t *
flow_inbox_memory_find_admission(flow_inbox_memory_t *memory,
                                 const turbo_flow_inbox_record_t *record) {
  size_t count = vec_size(&memory->records);
  for (size_t index = 0u; index < count; ++index) {
    flow_inbox_memory_slot_t *slot = (flow_inbox_memory_slot_t *)vec_at(&memory->records, index);
    if (slot && slot->record &&
        flow_inbox_view_equal(slot->record->view.source_id, record->source_id) &&
        flow_inbox_view_equal(slot->record->view.admission_id, record->admission_id)) {
      return slot->record;
    }
  }
  return NULL;
}

static void flow_inbox_memory_release_reservation(flow_inbox_memory_t *memory, size_t slot_index,
                                                  size_t retained_bytes) {
  flow_inbox_memory_slot_t *slot = (flow_inbox_memory_slot_t *)vec_at(&memory->records, slot_index);
  if (slot) slot->reserved = false;
  --memory->reserved_records;
  memory->reserved_bytes -= retained_bytes;
}

static int flow_inbox_memory_admit(void *ctx, const turbo_flow_inbox_record_t *source,
                                   turbo_flow_inbox_receipt_t *receipt) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  flow_inbox_memory_record_t *existing;
  flow_inbox_memory_record_t *record = NULL;
  flow_inbox_memory_slot_t *reserved_slot;
  size_t retained_bytes = 0u;
  size_t reserved_slot_index = SIZE_MAX;
  uint64_t reserved_record_id = 0u;
  int size_status = SALTS_OK;
  int rc = SALTS_OK;

  if (source->source_id.len > SIZE_MAX - source->partition_key.len) {
    size_status = SALTS_ERANGE;
  } else {
    retained_bytes = source->source_id.len + source->partition_key.len;
  }
  if (size_status == SALTS_OK) {
    if (source->admission_id.len > SIZE_MAX - retained_bytes)
      size_status = SALTS_ERANGE;
    else
      retained_bytes += source->admission_id.len;
  }
  if (size_status == SALTS_OK) {
    if (source->correlation.len > SIZE_MAX - retained_bytes)
      size_status = SALTS_ERANGE;
    else
      retained_bytes += source->correlation.len;
  }
  if (size_status == SALTS_OK) {
    if (source->payload.len > SIZE_MAX - retained_bytes)
      size_status = SALTS_ERANGE;
    else
      retained_bytes += source->payload.len;
  }

  salts_mutex_lock(&memory->mutex);
  existing = flow_inbox_memory_find_admission(memory, source);
  if (existing) {
    rc = flow_inbox_record_equal(&existing->view, source) ? SALTS_OK : SALTS_EPROTO;
    if (rc == SALTS_OK) receipt->record_id = existing->record_id;
  } else if (!memory->accepting) rc = SALTS_ESHUTDOWN;
  else if (size_status != SALTS_OK) rc = size_status;
  else if (retained_bytes > memory->config.max_record_bytes) rc = SALTS_ENOSPC;
  else if (memory->admitted == UINT64_MAX || memory->record_id_exhausted) rc = SALTS_ERANGE;
  else if (memory->live_records + memory->history_records >= memory->config.max_records ||
           retained_bytes > memory->config.max_total_bytes - memory->retained_bytes) {
    rc = SALTS_ENOSPC;
  } else if (memory->reserved_records >=
                 memory->config.max_records - memory->live_records - memory->history_records ||
             memory->reserved_bytes > memory->config.max_total_bytes - memory->retained_bytes ||
             retained_bytes >
                 memory->config.max_total_bytes - memory->retained_bytes - memory->reserved_bytes) {
    rc = SALTS_EBUSY;
  } else {
    for (size_t index = 0u; index < vec_size(&memory->records); ++index) {
      flow_inbox_memory_slot_t *slot = (flow_inbox_memory_slot_t *)vec_at(&memory->records, index);
      if (slot && !slot->record && !slot->reserved) {
        slot->reserved = true;
        reserved_slot_index = index;
        break;
      }
    }
    if (reserved_slot_index == SIZE_MAX) {
      salts_mutex_unlock(&memory->mutex);
      return SALTS_EPROTO;
    }
    reserved_record_id = memory->next_record_id;
    if (memory->next_record_id == UINT64_MAX) memory->record_id_exhausted = true;
    else ++memory->next_record_id;
    ++memory->reserved_records;
    memory->reserved_bytes += retained_bytes;
  }
  salts_mutex_unlock(&memory->mutex);
  if (rc != SALTS_OK || reserved_record_id == 0u) return rc;

  rc = flow_inbox_memory_record_copy(source, retained_bytes, &record);
  if (rc != SALTS_OK) {
    salts_mutex_lock(&memory->mutex);
    flow_inbox_memory_release_reservation(memory, reserved_slot_index, retained_bytes);
    salts_mutex_unlock(&memory->mutex);
    return rc;
  }
  record->record_id = reserved_record_id;

  salts_mutex_lock(&memory->mutex);
  existing = flow_inbox_memory_find_admission(memory, source);
  if (existing) {
    rc = flow_inbox_record_equal(&existing->view, source) ? SALTS_OK : SALTS_EPROTO;
    if (rc == SALTS_OK) receipt->record_id = existing->record_id;
  } else {
    reserved_slot = (flow_inbox_memory_slot_t *)vec_at(&memory->records, reserved_slot_index);
    if (!reserved_slot || !reserved_slot->reserved || reserved_slot->record) {
      rc = SALTS_EPROTO;
    } else {
      reserved_slot->record = record;
      memory->retained_bytes += retained_bytes;
      ++memory->live_records;
      ++memory->admitted;
      receipt->record_id = record->record_id;
      record = NULL;
      rc = SALTS_OK;
    }
  }
  flow_inbox_memory_release_reservation(memory, reserved_slot_index, retained_bytes);
  salts_mutex_unlock(&memory->mutex);
  flow_inbox_memory_record_destroy(record);
  return rc;
}

static int flow_inbox_memory_claim_select(
    void *ctx, const turbo_flow_inbox_claim_request_t *request,
    turbo_flow_inbox_claim_t *claim) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  flow_inbox_memory_record_t *record = NULL;
  int rc = SALTS_ENOENT;
  if (!flow_inbox_claim_request_valid(request)) return SALTS_EINVAL;
  salts_mutex_lock(&memory->mutex);
  for (size_t index = 0u; index < vec_size(&memory->records); ++index) {
    flow_inbox_memory_slot_t *slot = (flow_inbox_memory_slot_t *)vec_at(&memory->records, index);
    if (slot && slot->record && slot->record->phase == FLOW_INBOX_RECORD_PENDING &&
        !flow_inbox_claim_partition_excluded(&slot->record->view, request) &&
        (!record || slot->record->record_id < record->record_id)) {
      record = slot->record;
    }
  }
  if (record && memory->in_flight_claims >= memory->config.max_claims) rc = SALTS_ENOSPC;
  else if (record && memory->claim_token_exhausted) rc = SALTS_ERANGE;
  else if (record) {
    record->claim_token = memory->next_claim_token;
    if (memory->next_claim_token == UINT64_MAX) memory->claim_token_exhausted = true;
    else ++memory->next_claim_token;
    record->phase = FLOW_INBOX_RECORD_CLAIMED;
    ++memory->in_flight_claims;
    claim->record_id = record->record_id;
    claim->claim_token = record->claim_token;
    claim->record = record->view;
    rc = SALTS_OK;
  }
  salts_mutex_unlock(&memory->mutex);
  return rc;
}

static int flow_inbox_memory_claim(void *ctx, turbo_flow_inbox_claim_t *claim) {
  const turbo_flow_inbox_claim_request_t request = TURBO_FLOW_INBOX_CLAIM_REQUEST_INIT;
  return flow_inbox_memory_claim_select(ctx, &request, claim);
}

static int flow_inbox_memory_claim_ex(void *ctx,
                                      const turbo_flow_inbox_claim_request_t *request,
                                      turbo_flow_inbox_claim_t *claim) {
  return flow_inbox_memory_claim_select(ctx, request, claim);
}

static int flow_inbox_memory_complete(void *ctx, uint64_t record_id, uint64_t claim_token) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  flow_inbox_memory_record_t *record;
  int rc;
  salts_mutex_lock(&memory->mutex);
  record = flow_inbox_memory_find(memory, record_id, NULL);
  if (!record || record->phase != FLOW_INBOX_RECORD_CLAIMED || record->claim_token != claim_token) {
    rc = SALTS_EALREADY;
  } else if (memory->completed == UINT64_MAX) {
    rc = SALTS_ERANGE;
  } else {
    record->phase = FLOW_INBOX_RECORD_TOMBSTONE;
    record->claim_token = 0u;
    record->terminal_kind = TURBO_FLOW_INBOX_TERMINAL_COMPLETED;
    --memory->in_flight_claims;
    --memory->live_records;
    ++memory->history_records;
    ++memory->completed;
    rc = SALTS_OK;
  }
  salts_mutex_unlock(&memory->mutex);
  return rc;
}

static int flow_inbox_memory_fail(void *ctx, uint64_t record_id, uint64_t claim_token, int status) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  flow_inbox_memory_record_t *record;
  int rc;
  salts_mutex_lock(&memory->mutex);
  record = flow_inbox_memory_find(memory, record_id, NULL);
  if (!record || record->phase != FLOW_INBOX_RECORD_CLAIMED || record->claim_token != claim_token) {
    rc = SALTS_EALREADY;
  } else if (memory->failed == UINT64_MAX) {
    rc = SALTS_ERANGE;
  } else {
    record->phase = FLOW_INBOX_RECORD_FAILED;
    record->failure_status = status;
    record->failure_kind = TURBO_FLOW_INBOX_FAILURE_PROCESSING;
    --memory->in_flight_claims;
    ++memory->failed;
    rc = SALTS_OK;
  }
  salts_mutex_unlock(&memory->mutex);
  return rc;
}

static int flow_inbox_memory_retry(void *ctx, uint64_t record_id) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  flow_inbox_memory_record_t *record;
  int rc;
  salts_mutex_lock(&memory->mutex);
  record = flow_inbox_memory_find(memory, record_id, NULL);
  if (!record) rc = SALTS_ENOENT;
  else if (record->phase != FLOW_INBOX_RECORD_FAILED) rc = SALTS_EBUSY;
  else if (memory->retried == UINT64_MAX) rc = SALTS_ERANGE;
  else {
    record->phase = FLOW_INBOX_RECORD_PENDING;
    record->failure_status = SALTS_OK;
    record->failure_kind = TURBO_FLOW_INBOX_FAILURE_PROCESSING;
    ++memory->retried;
    rc = SALTS_OK;
  }
  salts_mutex_unlock(&memory->mutex);
  return rc;
}

static int flow_inbox_memory_discard(void *ctx, uint64_t record_id) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  flow_inbox_memory_record_t *record;
  int rc;
  salts_mutex_lock(&memory->mutex);
  record = flow_inbox_memory_find(memory, record_id, NULL);
  if (!record) rc = SALTS_ENOENT;
  else if (record->phase != FLOW_INBOX_RECORD_FAILED) rc = SALTS_EBUSY;
  else if (memory->discarded == UINT64_MAX) rc = SALTS_ERANGE;
  else {
    record->phase = FLOW_INBOX_RECORD_TOMBSTONE;
    record->terminal_kind = TURBO_FLOW_INBOX_TERMINAL_DISCARDED;
    --memory->live_records;
    ++memory->history_records;
    ++memory->discarded;
    rc = SALTS_OK;
  }
  salts_mutex_unlock(&memory->mutex);
  return rc;
}

static int flow_inbox_memory_forget(void *ctx, uint64_t record_id) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  flow_inbox_memory_record_t *record;
  size_t index = 0u;
  int rc;
  salts_mutex_lock(&memory->mutex);
  record = flow_inbox_memory_find(memory, record_id, &index);
  if (!record) rc = SALTS_ENOENT;
  else if (record->phase != FLOW_INBOX_RECORD_TOMBSTONE) rc = SALTS_EBUSY;
  else {
    flow_inbox_memory_slot_t *slot = (flow_inbox_memory_slot_t *)vec_at(&memory->records, index);
    if (!slot || slot->record != record || slot->reserved) {
      rc = SALTS_EPROTO;
    } else {
      slot->record = NULL;
      --memory->history_records;
      memory->retained_bytes -= record->retained_bytes;
      rc = SALTS_OK;
    }
  }
  salts_mutex_unlock(&memory->mutex);
  if (rc == SALTS_OK) flow_inbox_memory_record_destroy(record);
  return rc;
}

static int flow_inbox_memory_scan_failed(void *ctx, uint64_t after_record_id,
                                         turbo_flow_inbox_failed_entry_t *entries, size_t capacity,
                                         size_t *out_count) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  size_t count = 0u;
  salts_mutex_lock(&memory->mutex);
  while (count < capacity) {
    flow_inbox_memory_record_t *next = NULL;
    for (size_t index = 0u; index < vec_size(&memory->records); ++index) {
      flow_inbox_memory_slot_t *slot = (flow_inbox_memory_slot_t *)vec_at(&memory->records, index);
      if (slot && slot->record && slot->record->phase == FLOW_INBOX_RECORD_FAILED &&
          slot->record->record_id > after_record_id &&
          (!next || slot->record->record_id < next->record_id)) {
        next = slot->record;
      }
    }
    if (!next) break;
    entries[count].record_id = next->record_id;
    entries[count].status = next->failure_status;
    entries[count].kind = next->failure_kind;
    after_record_id = next->record_id;
    ++count;
  }
  salts_mutex_unlock(&memory->mutex);
  *out_count = count;
  return SALTS_OK;
}

static int flow_inbox_memory_scan_history(void *ctx, uint64_t after_record_id,
                                          turbo_flow_inbox_history_entry_t *entries,
                                          size_t capacity, size_t *out_count) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  size_t count = 0u;
  salts_mutex_lock(&memory->mutex);
  while (count < capacity) {
    flow_inbox_memory_record_t *next = NULL;
    for (size_t index = 0u; index < vec_size(&memory->records); ++index) {
      flow_inbox_memory_slot_t *slot = (flow_inbox_memory_slot_t *)vec_at(&memory->records, index);
      if (slot && slot->record && slot->record->phase == FLOW_INBOX_RECORD_TOMBSTONE &&
          slot->record->record_id > after_record_id &&
          (!next || slot->record->record_id < next->record_id)) {
        next = slot->record;
      }
    }
    if (!next) break;
    entries[count].record_id = next->record_id;
    entries[count].kind = next->terminal_kind;
    after_record_id = next->record_id;
    ++count;
  }
  salts_mutex_unlock(&memory->mutex);
  *out_count = count;
  return SALTS_OK;
}

static int flow_inbox_memory_close(void *ctx) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  int rc;
  salts_mutex_lock(&memory->mutex);
  if (memory->reserved_records != 0u) {
    rc = SALTS_EBUSY;
  } else {
    memory->accepting = false;
    rc = SALTS_OK;
  }
  salts_mutex_unlock(&memory->mutex);
  return rc;
}

static int flow_inbox_memory_snapshot(void *ctx, turbo_flow_inbox_snapshot_t *snapshot) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  salts_mutex_lock(&memory->mutex);
  snapshot->generation = 1u;
  snapshot->accepting = memory->accepting ? 1 : 0;
  snapshot->records = memory->live_records;
  snapshot->history_records = memory->history_records;
  snapshot->in_flight_claims = memory->in_flight_claims;
  snapshot->retained_bytes = memory->retained_bytes;
  snapshot->admitted = memory->admitted;
  snapshot->completed = memory->completed;
  snapshot->failed = memory->failed;
  snapshot->retried = memory->retried;
  snapshot->discarded = memory->discarded;
  for (size_t index = 0u; index < vec_size(&memory->records); ++index) {
    flow_inbox_memory_slot_t *slot = (flow_inbox_memory_slot_t *)vec_at(&memory->records, index);
    if (!slot || !slot->record) continue;
    if (slot->record->phase == FLOW_INBOX_RECORD_PENDING) ++snapshot->pending_records;
    else if (slot->record->phase == FLOW_INBOX_RECORD_FAILED) ++snapshot->failed_records;
  }
  salts_mutex_unlock(&memory->mutex);
  return SALTS_OK;
}

static int flow_inbox_memory_destroy(void *ctx) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  int rc;
  salts_mutex_lock(&memory->mutex);
  rc = memory->accepting || memory->live_records != 0u || memory->reserved_records != 0u ||
               memory->in_flight_claims != 0u
           ? SALTS_EBUSY
           : SALTS_OK;
  salts_mutex_unlock(&memory->mutex);
  if (rc != SALTS_OK) return rc;
  for (size_t index = 0u; index < vec_size(&memory->records); ++index) {
    flow_inbox_memory_slot_t *slot = (flow_inbox_memory_slot_t *)vec_at(&memory->records, index);
    if (slot) flow_inbox_memory_record_destroy(slot->record);
  }
  vec_destroy(&memory->records);
  salts_mutex_destroy(&memory->mutex);
  free(memory);
  return SALTS_OK;
}

static const turbo_flow_inbox_ops_v2_t flow_inbox_memory_ops = {
    .size = sizeof(turbo_flow_inbox_ops_v2_t),
    .version = TURBO_FLOW_INBOX_API_VERSION,
    .admit = flow_inbox_memory_admit,
    .claim = flow_inbox_memory_claim,
    .claim_ex = flow_inbox_memory_claim_ex,
    .complete = flow_inbox_memory_complete,
    .fail = flow_inbox_memory_fail,
    .retry = flow_inbox_memory_retry,
    .discard = flow_inbox_memory_discard,
    .forget = flow_inbox_memory_forget,
    .scan_failed = flow_inbox_memory_scan_failed,
    .scan_history = flow_inbox_memory_scan_history,
    .close = flow_inbox_memory_close,
    .snapshot = flow_inbox_memory_snapshot,
    .destroy = flow_inbox_memory_destroy};

int turbo_flow_inbox_memory_create(const turbo_flow_inbox_memory_config_t *config,
                                   turbo_flow_inbox_t *out) {
  flow_inbox_memory_t *memory;
  if (!out || out->size != sizeof(*out) || out->version != TURBO_FLOW_INBOX_API_VERSION ||
      out->ops || out->ctx || !flow_inbox_memory_config_valid(config)) {
    return SALTS_EINVAL;
  }
  memory = (flow_inbox_memory_t *)calloc(1u, sizeof(*memory));
  if (!memory) return SALTS_ENOMEM;
  memory->config = *config;
  memory->accepting = true;
  memory->next_record_id = 1u;
  memory->next_claim_token = 1u;
  salts_mutex_init(&memory->mutex);
  if (!memory->mutex) {
    free(memory);
    return SALTS_ENOMEM;
  }
  if (turbo_flow_stl_error(vec_init_bytes(&memory->records, sizeof(flow_inbox_memory_slot_t),
                                          _Alignof(flow_inbox_memory_slot_t),
                                          config->max_records)) != SALTS_OK ||
      turbo_flow_stl_error(vec_resize(&memory->records, config->max_records)) != SALTS_OK) {
    vec_destroy(&memory->records);
    salts_mutex_destroy(&memory->mutex);
    free(memory);
    return SALTS_ENOMEM;
  }
  out->ops = &flow_inbox_memory_ops;
  out->ctx = memory;
  return SALTS_OK;
}

int turbo_flow_inbox_admit(turbo_flow_inbox_t *inbox, const turbo_flow_inbox_record_t *record,
                           turbo_flow_inbox_receipt_t *receipt) {
  int rc;
  if (!receipt || receipt->size != sizeof(*receipt) ||
      receipt->version != TURBO_FLOW_INBOX_API_VERSION) {
    return SALTS_EINVAL;
  }
  *receipt = (turbo_flow_inbox_receipt_t)TURBO_FLOW_INBOX_RECEIPT_INIT;
  if (!flow_inbox_handle_valid(inbox)) return SALTS_EINVAL;
  rc = flow_inbox_record_valid(record);
  if (rc != SALTS_OK) return rc;
  rc = inbox->ops->admit(inbox->ctx, record, receipt);
  if (rc != SALTS_OK) {
    *receipt = (turbo_flow_inbox_receipt_t)TURBO_FLOW_INBOX_RECEIPT_INIT;
    return rc;
  }
  if (receipt->size != sizeof(*receipt) || receipt->version != TURBO_FLOW_INBOX_API_VERSION ||
      receipt->record_id == 0u) {
    *receipt = (turbo_flow_inbox_receipt_t)TURBO_FLOW_INBOX_RECEIPT_INIT;
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

int turbo_flow_inbox_claim(turbo_flow_inbox_t *inbox, turbo_flow_inbox_claim_t *claim) {
  int rc;
  if (!claim || claim->size != sizeof(*claim) || claim->version != TURBO_FLOW_INBOX_API_VERSION) {
    return SALTS_EINVAL;
  }
  if (claim->record_id != 0u || claim->claim_token != 0u) return SALTS_EBUSY;
  *claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
  if (!flow_inbox_handle_valid(inbox)) return SALTS_EINVAL;
  rc = inbox->ops->claim(inbox->ctx, claim);
  if (rc != SALTS_OK) {
    *claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
    return rc;
  }
  if (claim->size != sizeof(*claim) || claim->version != TURBO_FLOW_INBOX_API_VERSION ||
      claim->record_id == 0u || claim->claim_token == 0u ||
      flow_inbox_record_valid(&claim->record) != SALTS_OK) {
    *claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

int turbo_flow_inbox_claim_ex(turbo_flow_inbox_t *inbox,
                              const turbo_flow_inbox_claim_request_t *request,
                              turbo_flow_inbox_claim_t *claim) {
  int rc;
  if (!claim || claim->size != sizeof(*claim) || claim->version != TURBO_FLOW_INBOX_API_VERSION ||
      !flow_inbox_claim_request_valid(request))
    return SALTS_EINVAL;
  if (claim->record_id != 0u || claim->claim_token != 0u) return SALTS_EBUSY;
  *claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
  if (!flow_inbox_handle_valid(inbox)) return SALTS_EINVAL;
  rc = inbox->ops->claim_ex(inbox->ctx, request, claim);
  if (rc != SALTS_OK) {
    *claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
    return rc;
  }
  if (claim->size != sizeof(*claim) || claim->version != TURBO_FLOW_INBOX_API_VERSION ||
      claim->record_id == 0u || claim->claim_token == 0u ||
      flow_inbox_record_valid(&claim->record) != SALTS_OK) {
    *claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

static int flow_inbox_claim_valid(const turbo_flow_inbox_claim_t *claim) {
  return claim && claim->size == sizeof(*claim) && claim->version == TURBO_FLOW_INBOX_API_VERSION &&
         claim->record_id != 0u && claim->claim_token != 0u;
}

static int flow_inbox_snapshot_valid(const turbo_flow_inbox_snapshot_t *snapshot) {
  size_t unclassified;
  if (!snapshot || snapshot->generation == 0u ||
      (snapshot->accepting != 0 && snapshot->accepting != 1) ||
      snapshot->pending_records > snapshot->records) {
    return 0;
  }
  unclassified = snapshot->records - snapshot->pending_records;
  if (snapshot->failed_records > unclassified) return 0;
  if (snapshot->in_flight_claims != unclassified - snapshot->failed_records) return 0;
  if (snapshot->records > snapshot->admitted ||
      snapshot->history_records > snapshot->admitted - snapshot->records) {
    return 0;
  }
  if (snapshot->completed > snapshot->admitted ||
      snapshot->discarded > snapshot->admitted - snapshot->completed) {
    return 0;
  }
  if (snapshot->history_records > snapshot->completed + snapshot->discarded ||
      snapshot->failed_records > snapshot->failed || snapshot->retried > snapshot->failed ||
      snapshot->discarded > snapshot->failed) {
    return 0;
  }
  return 1;
}

int turbo_flow_inbox_complete(turbo_flow_inbox_t *inbox, turbo_flow_inbox_claim_t *claim) {
  int rc;
  if (!flow_inbox_handle_valid(inbox) || !flow_inbox_claim_valid(claim)) return SALTS_EINVAL;
  rc = inbox->ops->complete(inbox->ctx, claim->record_id, claim->claim_token);
  if (rc == SALTS_OK || rc == SALTS_ECANCELED)
    *claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
  return rc;
}

int turbo_flow_inbox_fail(turbo_flow_inbox_t *inbox, turbo_flow_inbox_claim_t *claim, int status) {
  int rc;
  if (!flow_inbox_handle_valid(inbox) || !flow_inbox_claim_valid(claim) || status == SALTS_OK) {
    return SALTS_EINVAL;
  }
  rc = inbox->ops->fail(inbox->ctx, claim->record_id, claim->claim_token, status);
  if (rc == SALTS_OK || rc == SALTS_ECANCELED)
    *claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
  return rc;
}

int turbo_flow_inbox_retry(turbo_flow_inbox_t *inbox, uint64_t record_id) {
  if (!flow_inbox_handle_valid(inbox) || record_id == 0u) return SALTS_EINVAL;
  return inbox->ops->retry(inbox->ctx, record_id);
}

int turbo_flow_inbox_discard(turbo_flow_inbox_t *inbox, uint64_t record_id) {
  if (!flow_inbox_handle_valid(inbox) || record_id == 0u) return SALTS_EINVAL;
  return inbox->ops->discard(inbox->ctx, record_id);
}

int turbo_flow_inbox_forget(turbo_flow_inbox_t *inbox, uint64_t record_id) {
  if (!flow_inbox_handle_valid(inbox) || record_id == 0u) return SALTS_EINVAL;
  return inbox->ops->forget(inbox->ctx, record_id);
}

int turbo_flow_inbox_scan_failed(const turbo_flow_inbox_t *inbox, uint64_t after_record_id,
                                 turbo_flow_inbox_failed_entry_t *entries, size_t capacity,
                                 size_t *out_count) {
  int rc;
  if (!out_count || (capacity != 0u && !entries)) return SALTS_EINVAL;
  *out_count = 0u;
  for (size_t index = 0u; index < capacity; ++index) {
    if (entries[index].size != sizeof(entries[index]) ||
        entries[index].version != TURBO_FLOW_INBOX_API_VERSION) {
      return SALTS_EINVAL;
    }
    entries[index] = (turbo_flow_inbox_failed_entry_t)TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
  }
  if (!flow_inbox_handle_valid(inbox)) return SALTS_EINVAL;
  rc = inbox->ops->scan_failed(inbox->ctx, after_record_id, entries, capacity, out_count);
  if (rc != SALTS_OK) {
    *out_count = 0u;
    for (size_t index = 0u; index < capacity; ++index)
      entries[index] = (turbo_flow_inbox_failed_entry_t)TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    return rc;
  }
  if (*out_count > capacity) rc = SALTS_EPROTO;
  for (size_t index = 0u; rc == SALTS_OK && index < *out_count; ++index) {
    if (entries[index].size != sizeof(entries[index]) ||
        entries[index].version != TURBO_FLOW_INBOX_API_VERSION ||
        entries[index].record_id <= after_record_id || entries[index].status == SALTS_OK ||
        (entries[index].kind != TURBO_FLOW_INBOX_FAILURE_PROCESSING &&
         entries[index].kind != TURBO_FLOW_INBOX_FAILURE_OWNER_LOST_UNKNOWN) ||
        (index != 0u && entries[index - 1u].record_id >= entries[index].record_id)) {
      rc = SALTS_EPROTO;
    }
  }
  if (rc != SALTS_OK) {
    *out_count = 0u;
    for (size_t index = 0u; index < capacity; ++index)
      entries[index] = (turbo_flow_inbox_failed_entry_t)TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
  }
  return rc;
}

int turbo_flow_inbox_scan_history(const turbo_flow_inbox_t *inbox, uint64_t after_record_id,
                                  turbo_flow_inbox_history_entry_t *entries, size_t capacity,
                                  size_t *out_count) {
  int rc;
  if (!out_count || (capacity != 0u && !entries)) return SALTS_EINVAL;
  *out_count = 0u;
  for (size_t index = 0u; index < capacity; ++index) {
    if (entries[index].size != sizeof(entries[index]) ||
        entries[index].version != TURBO_FLOW_INBOX_API_VERSION) {
      return SALTS_EINVAL;
    }
    entries[index] = (turbo_flow_inbox_history_entry_t)TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
  }
  if (!flow_inbox_handle_valid(inbox)) return SALTS_EINVAL;
  rc = inbox->ops->scan_history(inbox->ctx, after_record_id, entries, capacity, out_count);
  if (rc != SALTS_OK) {
    *out_count = 0u;
    for (size_t index = 0u; index < capacity; ++index)
      entries[index] = (turbo_flow_inbox_history_entry_t)TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
    return rc;
  }
  if (*out_count > capacity) rc = SALTS_EPROTO;
  for (size_t index = 0u; rc == SALTS_OK && index < *out_count; ++index) {
    if (entries[index].size != sizeof(entries[index]) ||
        entries[index].version != TURBO_FLOW_INBOX_API_VERSION ||
        entries[index].record_id <= after_record_id ||
        (entries[index].kind != TURBO_FLOW_INBOX_TERMINAL_COMPLETED &&
         entries[index].kind != TURBO_FLOW_INBOX_TERMINAL_DISCARDED) ||
        (index != 0u && entries[index - 1u].record_id >= entries[index].record_id)) {
      rc = SALTS_EPROTO;
    }
  }
  if (rc != SALTS_OK) {
    *out_count = 0u;
    for (size_t index = 0u; index < capacity; ++index)
      entries[index] = (turbo_flow_inbox_history_entry_t)TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
  }
  return rc;
}

int turbo_flow_inbox_close(turbo_flow_inbox_t *inbox) {
  if (!flow_inbox_handle_valid(inbox)) return SALTS_EINVAL;
  return inbox->ops->close(inbox->ctx);
}

int turbo_flow_inbox_snapshot(const turbo_flow_inbox_t *inbox,
                              turbo_flow_inbox_snapshot_t *snapshot) {
  int rc;
  if (!snapshot || snapshot->size != sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_INBOX_API_VERSION) {
    return SALTS_EINVAL;
  }
  *snapshot = (turbo_flow_inbox_snapshot_t)TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  if (!flow_inbox_handle_valid(inbox)) return SALTS_EINVAL;
  rc = inbox->ops->snapshot(inbox->ctx, snapshot);
  if (rc != SALTS_OK) {
    *snapshot = (turbo_flow_inbox_snapshot_t)TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    return rc;
  }
  if (snapshot->size != sizeof(*snapshot) || snapshot->version != TURBO_FLOW_INBOX_API_VERSION ||
      !flow_inbox_snapshot_valid(snapshot)) {
    *snapshot = (turbo_flow_inbox_snapshot_t)TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

int turbo_flow_inbox_destroy(turbo_flow_inbox_t *inbox) {
  int rc;
  if (!flow_inbox_handle_valid(inbox)) return SALTS_EINVAL;
  rc = inbox->ops->destroy(inbox->ctx);
  if (rc == SALTS_OK) *inbox = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
  return rc;
}
