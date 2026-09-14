#include "turbo_flow_inbox.h"

#include "turbo_flow_stl_error_internal.h"

#include <cstl/vec.h>
#include <salts_error.h>
#include <salts_thread.h>
#include <salts_buffer.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef enum flow_inbox_record_phase_e {
  FLOW_INBOX_RECORD_PENDING = 0,
  FLOW_INBOX_RECORD_CLAIMED,
  FLOW_INBOX_RECORD_FAILED
} flow_inbox_record_phase_t;

typedef struct flow_inbox_memory_record_s {
  uint64_t record_id;
  uint64_t claim_token;
  int failure_status;
  flow_inbox_record_phase_t phase;
  size_t retained_bytes;
  mem_buffer_t *storage;
  turbo_flow_inbox_record_t view;
} flow_inbox_memory_record_t;

typedef struct flow_inbox_memory_s {
  salts_mutex_t mutex;
  vec_t records;
  turbo_flow_inbox_memory_config_t config;
  size_t retained_bytes;
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

static int flow_inbox_handle_valid(const turbo_flow_inbox_t *inbox) {
  const turbo_flow_inbox_ops_v1_t *ops;
  if (!inbox || inbox->size != sizeof(*inbox) ||
      inbox->version != TURBO_FLOW_INBOX_API_VERSION || !inbox->ops || !inbox->ctx) {
    return 0;
  }
  ops = inbox->ops;
  return ops->size == sizeof(*ops) && ops->version == TURBO_FLOW_INBOX_API_VERSION &&
         ops->admit && ops->claim && ops->complete && ops->fail && ops->retry &&
         ops->discard && ops->close && ops->snapshot && ops->destroy;
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
  if ((record->correlation.len != 0u && !record->correlation.data) ||
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
         config->max_records <= SIZE_MAX / sizeof(flow_inbox_memory_record_t *) &&
         config->max_total_bytes != 0u &&
         config->max_total_bytes <= TURBO_FLOW_INBOX_MEMORY_MAX_TOTAL_BYTES &&
         config->max_record_bytes != 0u &&
         config->max_record_bytes <= config->max_total_bytes && config->max_claims != 0u &&
         config->max_claims <= config->max_records;
}

static void flow_inbox_memory_record_destroy(flow_inbox_memory_record_t *record) {
  if (!record) return;
  mem_buffer_release(record->storage);
  free(record);
}

static int flow_inbox_memory_record_copy(const turbo_flow_inbox_record_t *source,
                                         size_t retained_bytes,
                                         flow_inbox_memory_record_t **out) {
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
  if (source->correlation.len != 0u) {
    memcpy(bytes, source->correlation.data, source->correlation.len);
  }
  if (source->payload.len != 0u) {
    memcpy(bytes + source->correlation.len, source->payload.data, source->payload.len);
  }
  mem_set_used(record->storage, retained_bytes);
  record->phase = FLOW_INBOX_RECORD_PENDING;
  record->failure_status = SALTS_OK;
  record->retained_bytes = retained_bytes;
  record->view = *source;
  record->view.envelope_schema = TURBO_FLOW_INBOX_RECORD_SCHEMA;
  record->view.correlation = vstr_from_buf(bytes, source->correlation.len);
  record->view.payload =
      vstr_from_buf(bytes + source->correlation.len, source->payload.len);
  *out = record;
  return SALTS_OK;
}

static flow_inbox_memory_record_t *flow_inbox_memory_find(flow_inbox_memory_t *memory,
                                                          uint64_t record_id,
                                                          size_t *index_out) {
  size_t count = vec_size(&memory->records);
  for (size_t index = 0u; index < count; ++index) {
    flow_inbox_memory_record_t **slot =
        (flow_inbox_memory_record_t **)vec_at(&memory->records, index);
    if (slot && *slot && (*slot)->record_id == record_id) {
      if (index_out) *index_out = index;
      return *slot;
    }
  }
  return NULL;
}

static int flow_inbox_memory_admit(void *ctx, const turbo_flow_inbox_record_t *source,
                                   turbo_flow_inbox_receipt_t *receipt) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  flow_inbox_memory_record_t *record = NULL;
  size_t retained_bytes;
  int rc;
  if (source->correlation.len > SIZE_MAX - source->payload.len) return SALTS_ERANGE;
  retained_bytes = source->correlation.len + source->payload.len;
  if (retained_bytes > memory->config.max_record_bytes) return SALTS_ENOSPC;
  rc = flow_inbox_memory_record_copy(source, retained_bytes, &record);
  if (rc != SALTS_OK) return rc;

  salts_mutex_lock(&memory->mutex);
  if (!memory->accepting) rc = SALTS_ESHUTDOWN;
  else if (memory->record_id_exhausted) rc = SALTS_ERANGE;
  else if (vec_size(&memory->records) >= memory->config.max_records ||
           retained_bytes > memory->config.max_total_bytes - memory->retained_bytes) {
    rc = SALTS_ENOSPC;
  } else {
    record->record_id = memory->next_record_id;
    if (memory->next_record_id == UINT64_MAX) memory->record_id_exhausted = true;
    else ++memory->next_record_id;
    rc = turbo_flow_stl_error(vec_push(&memory->records, &record));
    if (rc == SALTS_OK) {
      memory->retained_bytes += retained_bytes;
      ++memory->admitted;
      receipt->record_id = record->record_id;
      record = NULL;
    }
  }
  salts_mutex_unlock(&memory->mutex);
  flow_inbox_memory_record_destroy(record);
  return rc;
}

static int flow_inbox_memory_claim(void *ctx, turbo_flow_inbox_claim_t *claim) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  flow_inbox_memory_record_t *record = NULL;
  int rc = SALTS_ENOENT;
  salts_mutex_lock(&memory->mutex);
  for (size_t index = 0u; index < vec_size(&memory->records); ++index) {
    flow_inbox_memory_record_t **slot =
        (flow_inbox_memory_record_t **)vec_at(&memory->records, index);
    if (slot && *slot && (*slot)->phase == FLOW_INBOX_RECORD_PENDING) {
      record = *slot;
      break;
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

static int flow_inbox_memory_complete(void *ctx, uint64_t record_id, uint64_t claim_token) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  flow_inbox_memory_record_t *record;
  size_t index = 0u;
  int rc;
  salts_mutex_lock(&memory->mutex);
  record = flow_inbox_memory_find(memory, record_id, &index);
  if (!record || record->phase != FLOW_INBOX_RECORD_CLAIMED ||
      record->claim_token != claim_token) {
    rc = SALTS_EALREADY;
  } else {
    rc = turbo_flow_stl_error(vec_erase(&memory->records, index, NULL));
    if (rc == SALTS_OK) {
      --memory->in_flight_claims;
      memory->retained_bytes -= record->retained_bytes;
      ++memory->completed;
    }
  }
  salts_mutex_unlock(&memory->mutex);
  if (rc == SALTS_OK) flow_inbox_memory_record_destroy(record);
  return rc;
}

static int flow_inbox_memory_fail(void *ctx, uint64_t record_id, uint64_t claim_token,
                                  int status) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  flow_inbox_memory_record_t *record;
  int rc;
  salts_mutex_lock(&memory->mutex);
  record = flow_inbox_memory_find(memory, record_id, NULL);
  if (!record || record->phase != FLOW_INBOX_RECORD_CLAIMED ||
      record->claim_token != claim_token) {
    rc = SALTS_EALREADY;
  } else {
    record->phase = FLOW_INBOX_RECORD_FAILED;
    record->failure_status = status;
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
  else {
    record->phase = FLOW_INBOX_RECORD_PENDING;
    record->failure_status = SALTS_OK;
    ++memory->retried;
    rc = SALTS_OK;
  }
  salts_mutex_unlock(&memory->mutex);
  return rc;
}

static int flow_inbox_memory_discard(void *ctx, uint64_t record_id) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  flow_inbox_memory_record_t *record;
  size_t index = 0u;
  int rc;
  salts_mutex_lock(&memory->mutex);
  record = flow_inbox_memory_find(memory, record_id, &index);
  if (!record) rc = SALTS_ENOENT;
  else if (record->phase != FLOW_INBOX_RECORD_FAILED) rc = SALTS_EBUSY;
  else {
    rc = turbo_flow_stl_error(vec_erase(&memory->records, index, NULL));
    if (rc == SALTS_OK) {
      memory->retained_bytes -= record->retained_bytes;
      ++memory->discarded;
    }
  }
  salts_mutex_unlock(&memory->mutex);
  if (rc == SALTS_OK) flow_inbox_memory_record_destroy(record);
  return rc;
}

static int flow_inbox_memory_close(void *ctx) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  salts_mutex_lock(&memory->mutex);
  memory->accepting = false;
  salts_mutex_unlock(&memory->mutex);
  return SALTS_OK;
}

static int flow_inbox_memory_snapshot(void *ctx, turbo_flow_inbox_snapshot_t *snapshot) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  salts_mutex_lock(&memory->mutex);
  snapshot->accepting = memory->accepting ? 1 : 0;
  snapshot->records = vec_size(&memory->records);
  snapshot->in_flight_claims = memory->in_flight_claims;
  snapshot->retained_bytes = memory->retained_bytes;
  snapshot->admitted = memory->admitted;
  snapshot->completed = memory->completed;
  snapshot->failed = memory->failed;
  snapshot->retried = memory->retried;
  snapshot->discarded = memory->discarded;
  for (size_t index = 0u; index < vec_size(&memory->records); ++index) {
    flow_inbox_memory_record_t **slot =
        (flow_inbox_memory_record_t **)vec_at(&memory->records, index);
    if (!slot || !*slot) continue;
    if ((*slot)->phase == FLOW_INBOX_RECORD_PENDING) ++snapshot->pending_records;
    else if ((*slot)->phase == FLOW_INBOX_RECORD_FAILED) ++snapshot->failed_records;
  }
  salts_mutex_unlock(&memory->mutex);
  return SALTS_OK;
}

static int flow_inbox_memory_destroy(void *ctx) {
  flow_inbox_memory_t *memory = (flow_inbox_memory_t *)ctx;
  int rc;
  salts_mutex_lock(&memory->mutex);
  rc = memory->accepting || vec_size(&memory->records) != 0u ||
               memory->in_flight_claims != 0u
           ? SALTS_EBUSY
           : SALTS_OK;
  salts_mutex_unlock(&memory->mutex);
  if (rc != SALTS_OK) return rc;
  vec_destroy(&memory->records);
  salts_mutex_destroy(&memory->mutex);
  free(memory);
  return SALTS_OK;
}

static const turbo_flow_inbox_ops_v1_t flow_inbox_memory_ops = {
    sizeof(turbo_flow_inbox_ops_v1_t), TURBO_FLOW_INBOX_API_VERSION,
    flow_inbox_memory_admit,          flow_inbox_memory_claim,
    flow_inbox_memory_complete,       flow_inbox_memory_fail,
    flow_inbox_memory_retry,          flow_inbox_memory_discard,
    flow_inbox_memory_close,          flow_inbox_memory_snapshot,
    flow_inbox_memory_destroy};

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
  if (turbo_flow_stl_error(vec_init_bytes(&memory->records,
                                          sizeof(flow_inbox_memory_record_t *),
                                          _Alignof(flow_inbox_memory_record_t *),
                                          config->max_records)) != SALTS_OK ||
      turbo_flow_stl_error(vec_reserve(&memory->records, config->max_records)) != SALTS_OK) {
    vec_destroy(&memory->records);
    salts_mutex_destroy(&memory->mutex);
    free(memory);
    return SALTS_ENOMEM;
  }
  out->ops = &flow_inbox_memory_ops;
  out->ctx = memory;
  return SALTS_OK;
}

int turbo_flow_inbox_admit(turbo_flow_inbox_t *inbox,
                           const turbo_flow_inbox_record_t *record,
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
  if (receipt->size != sizeof(*receipt) ||
      receipt->version != TURBO_FLOW_INBOX_API_VERSION || receipt->record_id == 0u) {
    *receipt = (turbo_flow_inbox_receipt_t)TURBO_FLOW_INBOX_RECEIPT_INIT;
    return SALTS_EPROTO;
  }
  return SALTS_OK;
}

int turbo_flow_inbox_claim(turbo_flow_inbox_t *inbox, turbo_flow_inbox_claim_t *claim) {
  int rc;
  if (!claim || claim->size != sizeof(*claim) ||
      claim->version != TURBO_FLOW_INBOX_API_VERSION) {
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

static int flow_inbox_claim_valid(const turbo_flow_inbox_claim_t *claim) {
  return claim && claim->size == sizeof(*claim) &&
         claim->version == TURBO_FLOW_INBOX_API_VERSION && claim->record_id != 0u &&
         claim->claim_token != 0u;
}

static int flow_inbox_snapshot_valid(const turbo_flow_inbox_snapshot_t *snapshot) {
  size_t unclassified;
  if (!snapshot || (snapshot->accepting != 0 && snapshot->accepting != 1) ||
      snapshot->pending_records > snapshot->records) {
    return 0;
  }
  unclassified = snapshot->records - snapshot->pending_records;
  if (snapshot->failed_records > unclassified) return 0;
  return snapshot->in_flight_claims == unclassified - snapshot->failed_records;
}

int turbo_flow_inbox_complete(turbo_flow_inbox_t *inbox, turbo_flow_inbox_claim_t *claim) {
  int rc;
  if (!flow_inbox_handle_valid(inbox) || !flow_inbox_claim_valid(claim)) return SALTS_EINVAL;
  rc = inbox->ops->complete(inbox->ctx, claim->record_id, claim->claim_token);
  if (rc == SALTS_OK) *claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
  return rc;
}

int turbo_flow_inbox_fail(turbo_flow_inbox_t *inbox, turbo_flow_inbox_claim_t *claim,
                          int status) {
  int rc;
  if (!flow_inbox_handle_valid(inbox) || !flow_inbox_claim_valid(claim) ||
      status == SALTS_OK) {
    return SALTS_EINVAL;
  }
  rc = inbox->ops->fail(inbox->ctx, claim->record_id, claim->claim_token, status);
  if (rc == SALTS_OK) *claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
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
  if (snapshot->size != sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_INBOX_API_VERSION ||
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
