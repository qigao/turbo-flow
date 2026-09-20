#include "tinytest.h"
#include "turbo_flow_inbox.h"

#include <salts_error.h>
#include <salts_thread.h>

#include <stdint.h>
#include <string.h>

enum { INBOX_TEST_ADMIT_THREADS = 4, INBOX_TEST_ADMITS_PER_THREAD = 8 };

typedef struct inbox_admit_worker_s {
  turbo_flow_inbox_t *inbox;
  const turbo_flow_inbox_record_t *record;
  size_t count;
  uint64_t record_id;
  int status;
} inbox_admit_worker_t;

static int inbox_malformed_admit(void *ctx, const turbo_flow_inbox_record_t *record,
                                 turbo_flow_inbox_receipt_t *receipt) {
  (void)ctx;
  (void)record;
  receipt->record_id = 0u;
  return SALTS_OK;
}

static int inbox_malformed_claim(void *ctx, turbo_flow_inbox_claim_t *claim) {
  (void)ctx;
  claim->record_id = 0u;
  claim->claim_token = 0u;
  return SALTS_OK;
}

static int inbox_malformed_claim_ex(void *ctx,
                                    const turbo_flow_inbox_claim_request_t *request,
                                    turbo_flow_inbox_claim_t *claim) {
  (void)request;
  return inbox_malformed_claim(ctx, claim);
}

static int inbox_malformed_settle(void *ctx, uint64_t record_id, uint64_t claim_token) {
  (void)ctx;
  (void)record_id;
  (void)claim_token;
  return SALTS_OK;
}

static int inbox_malformed_fail(void *ctx, uint64_t record_id, uint64_t claim_token, int status) {
  (void)status;
  return inbox_malformed_settle(ctx, record_id, claim_token);
}

static int inbox_malformed_record_action(void *ctx, uint64_t record_id) {
  (void)ctx;
  (void)record_id;
  return SALTS_OK;
}

static int inbox_malformed_scan_failed(void *ctx, uint64_t after_record_id,
                                       turbo_flow_inbox_failed_entry_t *entries, size_t capacity,
                                       size_t *out_count) {
  (void)after_record_id;
  switch (*(const int *)ctx) {
  case 4:
    *out_count = capacity + 1u;
    break;
  case 5:
    entries[0].size = 0u;
    entries[0].record_id = 1u;
    entries[0].status = SALTS_EIO;
    *out_count = 1u;
    break;
  case 6:
    entries[0].record_id = 2u;
    entries[0].status = SALTS_EIO;
    entries[1].record_id = 1u;
    entries[1].status = SALTS_EIO;
    *out_count = 2u;
    break;
  case 7:
    entries[0].record_id = 1u;
    entries[0].status = SALTS_EIO;
    *out_count = 1u;
    return SALTS_EIO;
  default:
    *out_count = 0u;
    break;
  }
  return SALTS_OK;
}

static int inbox_malformed_scan_history(void *ctx, uint64_t after_record_id,
                                        turbo_flow_inbox_history_entry_t *entries, size_t capacity,
                                        size_t *out_count) {
  (void)after_record_id;
  switch (*(const int *)ctx) {
  case 8:
    *out_count = capacity + 1u;
    break;
  case 9:
    entries[0].size = 0u;
    entries[0].record_id = 1u;
    entries[0].kind = TURBO_FLOW_INBOX_TERMINAL_COMPLETED;
    *out_count = 1u;
    break;
  case 10:
    entries[0].record_id = 2u;
    entries[0].kind = TURBO_FLOW_INBOX_TERMINAL_COMPLETED;
    entries[1].record_id = 1u;
    entries[1].kind = TURBO_FLOW_INBOX_TERMINAL_DISCARDED;
    *out_count = 2u;
    break;
  case 11:
    entries[0].record_id = 1u;
    entries[0].kind = (turbo_flow_inbox_terminal_kind_t)0;
    *out_count = 1u;
    break;
  case 12:
    entries[0].record_id = 1u;
    entries[0].kind = TURBO_FLOW_INBOX_TERMINAL_COMPLETED;
    *out_count = 1u;
    return SALTS_EIO;
  default:
    *out_count = 0u;
    break;
  }
  return SALTS_OK;
}

static int inbox_malformed_lifecycle(void *ctx) {
  (void)ctx;
  return SALTS_OK;
}

static int inbox_malformed_snapshot(void *ctx, turbo_flow_inbox_snapshot_t *snapshot) {
  int mode = *(const int *)ctx;
  snapshot->generation = 1u;
  if (mode == 1) {
    snapshot->size = 0u;
  } else if (mode == 2) {
    snapshot->accepting = 2;
  } else if (mode == 3) {
    snapshot->records = 1u;
    snapshot->pending_records = 1u;
    snapshot->in_flight_claims = 1u;
  } else if (mode == 13) {
    snapshot->history_records = 1u;
  } else if (mode == 14) {
    snapshot->admitted = 1u;
    snapshot->completed = 1u;
    snapshot->discarded = 1u;
  } else if (mode == 15) {
    snapshot->admitted = 1u;
    snapshot->history_records = 1u;
  } else if (mode == 16) {
    snapshot->failed_records = 1u;
    snapshot->records = 1u;
  } else if (mode == 17) {
    snapshot->retried = 1u;
  } else if (mode == 18) {
    snapshot->discarded = 1u;
  }
  return SALTS_OK;
}

static const turbo_flow_inbox_ops_v2_t inbox_malformed_ops = {
    .size = sizeof(turbo_flow_inbox_ops_v2_t),
    .version = TURBO_FLOW_INBOX_API_VERSION,
    .admit = inbox_malformed_admit,
    .claim = inbox_malformed_claim,
    .claim_ex = inbox_malformed_claim_ex,
    .complete = inbox_malformed_settle,
    .fail = inbox_malformed_fail,
    .retry = inbox_malformed_record_action,
    .discard = inbox_malformed_record_action,
    .forget = inbox_malformed_record_action,
    .scan_failed = inbox_malformed_scan_failed,
    .scan_history = inbox_malformed_scan_history,
    .close = inbox_malformed_lifecycle,
    .snapshot = inbox_malformed_snapshot,
    .destroy = inbox_malformed_lifecycle};

static void inbox_admit_worker_run(void *arg) {
  inbox_admit_worker_t *worker = (inbox_admit_worker_t *)arg;
  worker->status = SALTS_OK;
  for (size_t index = 0u; index < worker->count; ++index) {
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    int rc = turbo_flow_inbox_admit(worker->inbox, worker->record, &receipt);
    if (rc != SALTS_OK) {
      worker->status = rc;
      return;
    }
    if (worker->record_id == 0u) worker->record_id = receipt.record_id;
    else if (worker->record_id != receipt.record_id) {
      worker->status = SALTS_EPROTO;
      return;
    }
  }
}

static turbo_flow_inbox_record_t inbox_test_record(char *correlation, char *payload) {
  static const char source_id[] = "test-source";
  turbo_flow_inbox_record_t record;
  turbo_flow_inbox_record_init(&record);
  record.source_id = vstr_from_buf(source_id, sizeof(source_id) - 1u);
  record.partition_key = record.source_id;
  record.admission_id = vstr_from_buf(correlation, strlen(correlation));
  record.source_sequence = 41u;
  record.timestamp_ns = 42u;
  record.message_type = 7u;
  record.message_flags = 9u;
  check_equal(turbo_flow_content_descriptor_init(
                  &record.content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                  TURBO_FLOW_DATA_ENCODING_JSON, "application/json", "orders.v1"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&record.content, "OrderCreated",
                                                           "OrderCreated", 1u),
              SALTS_OK);
  record.correlation = vstr_from_buf(correlation, strlen(correlation));
  record.payload = vstr_from_buf(payload, strlen(payload));
  return record;
}

static size_t inbox_test_record_bytes(const turbo_flow_inbox_record_t *record) {
  return record->source_id.len + record->partition_key.len + record->admission_id.len +
         record->correlation.len + record->payload.len;
}

static turbo_flow_inbox_memory_config_t inbox_test_config(void) {
  turbo_flow_inbox_memory_config_t config = turbo_flow_inbox_memory_config_default();
  config.max_records = 2u;
  config.max_total_bytes = 128u;
  config.max_record_bytes = 64u;
  config.max_claims = 1u;
  return config;
}

spec("flow intake inbox") {
  it("rejects malformed success outputs from a provider vtable") {
    char correlation[] = "order-41";
    char payload[] = "payload";
    int provider_context = 1;
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t inbox = {sizeof(inbox), TURBO_FLOW_INBOX_API_VERSION, &inbox_malformed_ops,
                                &provider_context};

    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_EPROTO);
    check_equal(receipt.record_id, (uint64_t)0u);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_EPROTO);
    check_equal(claim.record_id, (uint64_t)0u);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_EPROTO);
    check_equal(snapshot.size, sizeof(snapshot));
    provider_context = 2;
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_EPROTO);
    check_equal(snapshot.accepting, 0);
    provider_context = 3;
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_EPROTO);
    check_equal(snapshot.records, (size_t)0u);
    for (provider_context = 13; provider_context <= 18; ++provider_context) {
      check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_EPROTO);
      check_equal(snapshot.records, (size_t)0u);
      check_equal(snapshot.history_records, (size_t)0u);
    }
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("rejects short output ABI values before provider mutation") {
    char correlation[] = "order-41";
    char payload[] = "payload";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;

    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    receipt.size -= 1u;
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_EINVAL);
    receipt = (turbo_flow_inbox_receipt_t)TURBO_FLOW_INBOX_RECEIPT_INIT;
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    claim.size -= 1u;
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_EINVAL);
    claim = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    snapshot.size -= 1u;
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_EINVAL);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("rejects malformed failed scans and clears partial provider output") {
    int provider_context = 4;
    turbo_flow_inbox_failed_entry_t entries[2] = {TURBO_FLOW_INBOX_FAILED_ENTRY_INIT,
                                                  TURBO_FLOW_INBOX_FAILED_ENTRY_INIT};
    turbo_flow_inbox_t inbox = {sizeof(inbox), TURBO_FLOW_INBOX_API_VERSION, &inbox_malformed_ops,
                                &provider_context};
    size_t count = 9u;

    check_equal(turbo_flow_inbox_scan_failed(&inbox, 0u, entries, 2u, &count), SALTS_EPROTO);
    check_equal(count, (size_t)0u);
    provider_context = 5;
    check_equal(turbo_flow_inbox_scan_failed(&inbox, 0u, entries, 2u, &count), SALTS_EPROTO);
    check_equal(count, (size_t)0u);
    provider_context = 6;
    check_equal(turbo_flow_inbox_scan_failed(&inbox, 0u, entries, 2u, &count), SALTS_EPROTO);
    check_equal(count, (size_t)0u);
    provider_context = 7;
    check_equal(turbo_flow_inbox_scan_failed(&inbox, 0u, entries, 2u, &count), SALTS_EIO);
    check_equal(count, (size_t)0u);
    check_equal(entries[0].record_id, (uint64_t)0u);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("rejects malformed history scans and clears partial provider output") {
    int provider_context = 8;
    turbo_flow_inbox_history_entry_t entries[2] = {TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT,
                                                   TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT};
    turbo_flow_inbox_t inbox = {sizeof(inbox), TURBO_FLOW_INBOX_API_VERSION, &inbox_malformed_ops,
                                &provider_context};
    size_t count = 9u;

    check_equal(turbo_flow_inbox_scan_history(&inbox, 0u, entries, 2u, &count), SALTS_EPROTO);
    check_equal(count, (size_t)0u);
    provider_context = 9;
    check_equal(turbo_flow_inbox_scan_history(&inbox, 0u, entries, 2u, &count), SALTS_EPROTO);
    check_equal(count, (size_t)0u);
    provider_context = 10;
    check_equal(turbo_flow_inbox_scan_history(&inbox, 0u, entries, 2u, &count), SALTS_EPROTO);
    check_equal(count, (size_t)0u);
    provider_context = 11;
    check_equal(turbo_flow_inbox_scan_history(&inbox, 0u, entries, 2u, &count), SALTS_EPROTO);
    check_equal(count, (size_t)0u);
    provider_context = 12;
    check_equal(turbo_flow_inbox_scan_history(&inbox, 0u, entries, 2u, &count), SALTS_EIO);
    check_equal(count, (size_t)0u);
    check_equal(entries[0].record_id, (uint64_t)0u);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("rejects zero bounds and old envelope schemas") {
    char correlation[] = "order-41";
    char payload[] = "payload";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;

    config.max_records = 0u;
    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_EINVAL);
    check_null(inbox.ops);

    config = inbox_test_config();
    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    record.size -= 1u;
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_EINVAL);
    record = inbox_test_record(correlation, payload);
    record.envelope_schema = "turbo-flow.inbox.legacy";
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_EPROTO);
    record.envelope_schema = TURBO_FLOW_INBOX_RECORD_SCHEMA;
    record.envelope_schema_version = TURBO_FLOW_INBOX_RECORD_SCHEMA_VERSION + 1u;
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_EPROTO);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("copies admitted bytes and rejects the record beyond capacity") {
    char first_correlation[] = "order-41";
    char first_payload[] = "payload";
    char second_correlation[] = "order-42";
    char second_payload[] = "body";
    char third_correlation[] = "order-43";
    char third_payload[] = "last";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t first = inbox_test_record(first_correlation, first_payload);
    turbo_flow_inbox_record_t second = inbox_test_record(second_correlation, second_payload);
    turbo_flow_inbox_record_t third = inbox_test_record(third_correlation, third_payload);
    turbo_flow_inbox_receipt_t first_receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t second_receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t rejected_receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;

    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &first, &first_receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &second, &second_receipt), SALTS_OK);
    check_not_equal(first_receipt.record_id, second_receipt.record_id);
    check_equal(turbo_flow_inbox_admit(&inbox, &third, &rejected_receipt), SALTS_ENOSPC);
    check_equal(rejected_receipt.record_id, (uint64_t)0u);

    memcpy(first_correlation, "changed!", sizeof(first_correlation) - 1u);
    memcpy(first_payload, "xxxxxxx", sizeof(first_payload) - 1u);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(claim.record_id, first_receipt.record_id);
    check_equal(claim.record.source_id.len, sizeof("test-source") - 1u);
    check_equal(memcmp(claim.record.source_id.data, "test-source", sizeof("test-source") - 1u), 0);
    check_equal(claim.record.admission_id.len, sizeof("order-41") - 1u);
    check_equal(memcmp(claim.record.admission_id.data, "order-41", sizeof("order-41") - 1u), 0);
    check_equal(claim.record.correlation.len, sizeof("order-41") - 1u);
    check_equal(memcmp(claim.record.correlation.data, "order-41", sizeof("order-41") - 1u), 0);
    check_equal(claim.record.payload.len, sizeof("payload") - 1u);
    check_equal(memcmp(claim.record.payload.data, "payload", sizeof("payload") - 1u), 0);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);

    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(claim.record_id, second_receipt.record_id);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("deduplicates exact admission replays and rejects conflicting content") {
    char correlation[] = "order-41";
    char payload[] = "payload";
    char conflicting_payload[] = "changed";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    turbo_flow_inbox_record_t conflict = record;
    turbo_flow_inbox_record_t partition_conflict = record;
    turbo_flow_inbox_receipt_t first = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t replay = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t rejected = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t partition_rejected = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;

    conflict.payload = vstr_from_buf(conflicting_payload, sizeof(conflicting_payload) - 1u);
    partition_conflict.partition_key =
        vstr_from_buf("other-partition", sizeof("other-partition") - 1u);
    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &first), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &replay), SALTS_OK);
    check_equal(replay.record_id, first.record_id);
    check_equal(turbo_flow_inbox_admit(&inbox, &conflict, &rejected), SALTS_EPROTO);
    check_equal(rejected.record_id, (uint64_t)0u);
    check_equal(turbo_flow_inbox_admit(&inbox, &partition_conflict, &partition_rejected),
                SALTS_EPROTO);
    check_equal(partition_rejected.record_id, (uint64_t)0u);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.generation, (uint64_t)1u);
    check_equal(snapshot.records, (size_t)1u);
    check_equal(snapshot.admitted, (uint64_t)1u);
    check_equal(snapshot.retained_bytes, inbox_test_record_bytes(&record));
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("retains terminal admission identity until explicit forget") {
    char correlation[] = "order-41";
    char payload[] = "payload";
    char conflicting_payload[] = "changed";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    turbo_flow_inbox_record_t conflict = record;
    turbo_flow_inbox_receipt_t first = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t replay = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t fresh = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_history_entry_t history = TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    size_t history_count = 0u;

    config.max_records = 1u;
    config.max_total_bytes = inbox_test_record_bytes(&record);
    config.max_record_bytes = config.max_total_bytes;
    config.max_claims = 1u;
    conflict.payload = vstr_from_buf(conflicting_payload, sizeof(conflicting_payload) - 1u);

    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &first), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.records, (size_t)0u);
    check_equal(snapshot.history_records, (size_t)1u);
    check_equal(snapshot.retained_bytes, inbox_test_record_bytes(&record));
    check_equal(turbo_flow_inbox_scan_history(&inbox, 0u, &history, 1u, &history_count), SALTS_OK);
    check_equal(history_count, (size_t)1u);
    check_equal(history.record_id, first.record_id);
    check_equal(history.kind, TURBO_FLOW_INBOX_TERMINAL_COMPLETED);

    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &replay), SALTS_OK);
    check_equal(replay.record_id, first.record_id);
    check_equal(turbo_flow_inbox_admit(&inbox, &conflict, &replay), SALTS_EPROTO);
    check_equal(turbo_flow_inbox_forget(&inbox, first.record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_forget(&inbox, first.record_id), SALTS_ENOENT);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.history_records, (size_t)0u);
    check_equal(snapshot.retained_bytes, (size_t)0u);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &fresh), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("forgets only terminal tombstones") {
    char correlation[] = "order-41";
    char payload[] = "payload";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;

    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_forget(&inbox, receipt.record_id), SALTS_EBUSY);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_forget(&inbox, receipt.record_id), SALTS_EBUSY);
    check_equal(turbo_flow_inbox_fail(&inbox, &claim, SALTS_EIO), SALTS_OK);
    check_equal(turbo_flow_inbox_forget(&inbox, receipt.record_id), SALTS_EBUSY);
    check_equal(turbo_flow_inbox_discard(&inbox, receipt.record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_forget(&inbox, receipt.record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("replays discarded terminal identity and rejects conflicting content") {
    char correlation[] = "order-41";
    char payload[] = "payload";
    char conflicting_payload[] = "changed";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    turbo_flow_inbox_record_t conflict = record;
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t replay = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t rejected = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_history_entry_t history = TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    size_t count = 0u;

    conflict.payload = vstr_from_buf(conflicting_payload, sizeof(conflicting_payload) - 1u);
    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_fail(&inbox, &claim, SALTS_EIO), SALTS_OK);
    check_equal(turbo_flow_inbox_discard(&inbox, receipt.record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);

    check_equal(turbo_flow_inbox_admit(&inbox, &record, &replay), SALTS_OK);
    check_equal(replay.record_id, receipt.record_id);
    check_equal(turbo_flow_inbox_admit(&inbox, &conflict, &rejected), SALTS_EPROTO);
    check_equal(rejected.record_id, (uint64_t)0u);
    check_equal(turbo_flow_inbox_scan_history(&inbox, 0u, &history, 1u, &count), SALTS_OK);
    check_equal(count, (size_t)1u);
    check_equal(history.record_id, receipt.record_id);
    check_equal(history.kind, TURBO_FLOW_INBOX_TERMINAL_DISCARDED);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("keeps failed records unavailable until explicit retry") {
    char correlation[] = "order-41";
    char payload[] = "payload";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_claim_t blocked_claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_claim_t stale = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;

    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    stale = claim;
    check_equal(turbo_flow_inbox_claim(&inbox, &blocked_claim), SALTS_ENOENT);
    check_equal(turbo_flow_inbox_fail(&inbox, &claim, SALTS_EIO), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&inbox, &stale), SALTS_EALREADY);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_ENOENT);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.failed_records, (size_t)1u);
    check_equal(snapshot.in_flight_claims, (size_t)0u);

    check_equal(turbo_flow_inbox_retry(&inbox, receipt.record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_not_equal(claim.claim_token, stale.claim_token);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("scans failed records in stable pages with processing failure kinds") {
    char first_correlation[] = "order-41";
    char first_payload[] = "first";
    char second_correlation[] = "order-42";
    char second_payload[] = "second";
    char third_correlation[] = "order-43";
    char third_payload[] = "third";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t records[] = {inbox_test_record(first_correlation, first_payload),
                                           inbox_test_record(second_correlation, second_payload),
                                           inbox_test_record(third_correlation, third_payload)};
    turbo_flow_inbox_receipt_t receipts[3] = {TURBO_FLOW_INBOX_RECEIPT_INIT,
                                              TURBO_FLOW_INBOX_RECEIPT_INIT,
                                              TURBO_FLOW_INBOX_RECEIPT_INIT};
    turbo_flow_inbox_failed_entry_t page[2] = {TURBO_FLOW_INBOX_FAILED_ENTRY_INIT,
                                               TURBO_FLOW_INBOX_FAILED_ENTRY_INIT};
    turbo_flow_inbox_history_entry_t history[3] = {TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT,
                                                   TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT,
                                                   TURBO_FLOW_INBOX_HISTORY_ENTRY_INIT};
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    const int statuses[] = {SALTS_EIO, SALTS_EPROTO, SALTS_EINVAL};
    size_t count = 99u;

    config.max_records = 3u;
    config.max_claims = 1u;
    config.max_total_bytes = inbox_test_record_bytes(&records[0]) +
                             inbox_test_record_bytes(&records[1]) +
                             inbox_test_record_bytes(&records[2]);
    config.max_record_bytes = inbox_test_record_bytes(&records[1]);
    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    for (size_t index = 0u; index < 3u; ++index) {
      check_equal(turbo_flow_inbox_admit(&inbox, &records[index], &receipts[index]), SALTS_OK);
      check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
      check_equal(turbo_flow_inbox_fail(&inbox, &claim, statuses[index]), SALTS_OK);
    }

    check_equal(turbo_flow_inbox_scan_failed(&inbox, 0u, page, 2u, &count), SALTS_OK);
    check_equal(count, (size_t)2u);
    check_equal(page[0].record_id, receipts[0].record_id);
    check_equal(page[0].status, statuses[0]);
    check_equal(page[0].kind, TURBO_FLOW_INBOX_FAILURE_PROCESSING);
    check_equal(page[1].record_id, receipts[1].record_id);
    check_equal(page[1].status, statuses[1]);
    check_equal(page[1].kind, TURBO_FLOW_INBOX_FAILURE_PROCESSING);

    page[0] = (turbo_flow_inbox_failed_entry_t)TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    page[1] = (turbo_flow_inbox_failed_entry_t)TURBO_FLOW_INBOX_FAILED_ENTRY_INIT;
    check_equal(turbo_flow_inbox_scan_failed(&inbox, receipts[1].record_id, page, 2u, &count),
                SALTS_OK);
    check_equal(count, (size_t)1u);
    check_equal(page[0].record_id, receipts[2].record_id);
    check_equal(page[0].status, statuses[2]);
    check_equal(page[0].kind, TURBO_FLOW_INBOX_FAILURE_PROCESSING);
    check_equal(turbo_flow_inbox_scan_failed(&inbox, receipts[2].record_id, NULL, 0u, &count),
                SALTS_OK);
    check_equal(count, (size_t)0u);

    for (size_t index = 0u; index < 3u; ++index) {
      check_equal(turbo_flow_inbox_discard(&inbox, receipts[index].record_id), SALTS_OK);
    }
    check_equal(turbo_flow_inbox_scan_history(&inbox, 0u, history, 3u, &count), SALTS_OK);
    check_equal(count, (size_t)3u);
    for (size_t index = 0u; index < 3u; ++index) {
      check_equal(history[index].record_id, receipts[index].record_id);
      check_equal(history[index].kind, TURBO_FLOW_INBOX_TERMINAL_DISCARDED);
    }
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("preserves a live claim when its owner attempts another claim") {
    char correlation[] = "order-41";
    char payload[] = "payload";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    uint64_t claim_token;

    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    claim_token = claim.claim_token;
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_EBUSY);
    check_equal(claim.record_id, receipt.record_id);
    check_equal(claim.claim_token, claim_token);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("claims the oldest eligible canonical partition without source-id coupling") {
    static const char source[] = "same-source";
    static const char key_a[] = "partition-A";
    static const char key_b[] = "partition-B";
    char a1_id[] = "a-1";
    char a2_id[] = "a-2";
    char b1_id[] = "b-1";
    char payload[] = "p";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t a1 = inbox_test_record(a1_id, payload);
    turbo_flow_inbox_record_t a2 = inbox_test_record(a2_id, payload);
    turbo_flow_inbox_record_t b1 = inbox_test_record(b1_id, payload);
    turbo_flow_inbox_receipt_t a1_receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t a2_receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t b1_receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t first = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_claim_t second = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_claim_t blocked = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_claim_request_t request = TURBO_FLOW_INBOX_CLAIM_REQUEST_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    vstr excluded[1];
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;

    a1.source_id = vstr_from_buf(source, sizeof(source) - 1u);
    a1.partition_key = vstr_from_buf(key_a, sizeof(key_a) - 1u);
    a2.source_id = vstr_from_buf(source, sizeof(source) - 1u);
    a2.partition_key = vstr_from_buf(key_a, sizeof(key_a) - 1u);
    b1.source_id = vstr_from_buf(source, sizeof(source) - 1u);
    b1.partition_key = vstr_from_buf(key_b, sizeof(key_b) - 1u);
    config.max_records = 4u;
    config.max_total_bytes = 256u;
    config.max_record_bytes = 64u;
    config.max_claims = 2u;
    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &a1, &a1_receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &a2, &a2_receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &b1, &b1_receipt), SALTS_OK);

    request.ordering = TURBO_FLOW_INBOX_CLAIM_ORDER_PARTITION_KEY;
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &first), SALTS_OK);
    check_equal(first.record_id, a1_receipt.record_id);
    excluded[0] = first.record.partition_key;
    request.excluded_partitions = excluded;
    request.excluded_partition_count = 1u;
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &second), SALTS_OK);
    check_equal(second.record_id, b1_receipt.record_id);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.pending_records, (size_t)1u);
    check_equal(snapshot.in_flight_claims, (size_t)2u);

    blocked = (turbo_flow_inbox_claim_t)TURBO_FLOW_INBOX_CLAIM_INIT;
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &blocked), SALTS_ENOENT);
    check_equal(blocked.record_id, (uint64_t)0u);
    check_equal(turbo_flow_inbox_complete(&inbox, &second), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.pending_records, (size_t)1u);
    check_equal(snapshot.in_flight_claims, (size_t)1u);
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &blocked), SALTS_ENOENT);

    check_equal(turbo_flow_inbox_complete(&inbox, &first), SALTS_OK);
    request.excluded_partitions = NULL;
    request.excluded_partition_count = 0u;
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &blocked), SALTS_OK);
    check_equal(blocked.record_id, a2_receipt.record_id);
    check_equal(turbo_flow_inbox_complete(&inbox, &blocked), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("rejects malformed or unbounded partition claim selectors before provider mutation") {
    static const char source[] = "source-A";
    char id[] = "one";
    char payload[] = "p";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(id, payload);
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_claim_request_t request = TURBO_FLOW_INBOX_CLAIM_REQUEST_INIT;
    turbo_flow_inbox_snapshot_t before = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_snapshot_t after = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    vstr keys[2];
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;

    record.source_id = vstr_from_buf(source, sizeof(source) - 1u);
    record.partition_key = record.source_id;
    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &before), SALTS_OK);
    keys[0] = record.partition_key;
    keys[1] = record.partition_key;

    request.excluded_partitions = keys;
    request.excluded_partition_count = 1u;
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &claim), SALTS_EINVAL);
    request.ordering = TURBO_FLOW_INBOX_CLAIM_ORDER_PARTITION_KEY;
    request.excluded_partition_count = 2u;
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &claim), SALTS_EINVAL);
    request.excluded_partition_count = TURBO_FLOW_INBOX_CLAIM_MAX_EXCLUDED_PARTITIONS + 1u;
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &claim), SALTS_EINVAL);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &after), SALTS_OK);
    check_equal(after.pending_records, before.pending_records);
    check_equal(after.in_flight_claims, before.in_flight_claims);

    request = (turbo_flow_inbox_claim_request_t)TURBO_FLOW_INBOX_CLAIM_REQUEST_INIT;
    check_equal(turbo_flow_inbox_claim_ex(&inbox, &request, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("enforces retained-byte and concurrent-claim bounds independently") {
    char first_correlation[] = "order-41";
    char first_payload[] = "payload";
    char second_correlation[] = "order-42";
    char second_payload[] = "body";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t first = inbox_test_record(first_correlation, first_payload);
    turbo_flow_inbox_record_t second = inbox_test_record(second_correlation, second_payload);
    turbo_flow_inbox_receipt_t first_receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_receipt_t second_receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t first_claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_claim_t blocked_claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;

    config.max_total_bytes = inbox_test_record_bytes(&first);
    config.max_record_bytes = config.max_total_bytes;
    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &first, &first_receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &second, &second_receipt), SALTS_ENOSPC);
    check_equal(turbo_flow_inbox_claim(&inbox, &first_claim), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&inbox, &first_claim), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &second, &second_receipt), SALTS_ENOSPC);
    check_equal(turbo_flow_inbox_forget(&inbox, first_receipt.record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &second, &second_receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &first_claim), SALTS_OK);
    check_equal(turbo_flow_inbox_fail(&inbox, &first_claim, SALTS_EIO), SALTS_OK);
    check_equal(turbo_flow_inbox_discard(&inbox, second_receipt.record_id), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);

    config = inbox_test_config();
    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &first, &first_receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &second, &second_receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &first_claim), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &blocked_claim), SALTS_ENOSPC);
    check_equal(turbo_flow_inbox_complete(&inbox, &first_claim), SALTS_OK);
    check_equal(turbo_flow_inbox_claim(&inbox, &blocked_claim), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&inbox, &blocked_claim), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }

  it("drains accepted records after close and rejects new admission") {
    char correlation[] = "order-41";
    char payload[] = "payload";
    char new_correlation[] = "order-42";
    char new_payload[] = "new";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    turbo_flow_inbox_record_t new_record = inbox_test_record(new_correlation, new_payload);
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;

    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &new_record, &receipt), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_EBUSY);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.generation, (uint64_t)1u);
    check_equal(snapshot.accepting, 0);
    check_equal(snapshot.records, (size_t)0u);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    check_null(inbox.ops);
  }

  it("serializes concurrent exact replays into one bounded record") {
    char correlation[] = "order-41";
    char payload[] = "payload";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    inbox_admit_worker_t workers[INBOX_TEST_ADMIT_THREADS] = {0};
    salts_thread_t threads[INBOX_TEST_ADMIT_THREADS] = {0};
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    const size_t attempted_admissions = INBOX_TEST_ADMIT_THREADS * INBOX_TEST_ADMITS_PER_THREAD;

    config.max_records = attempted_admissions;
    config.max_total_bytes = attempted_admissions * inbox_test_record_bytes(&record);
    config.max_record_bytes = inbox_test_record_bytes(&record);
    config.max_claims = INBOX_TEST_ADMIT_THREADS;
    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    for (size_t index = 0u; index < INBOX_TEST_ADMIT_THREADS; ++index) {
      workers[index].inbox = &inbox;
      workers[index].record = &record;
      workers[index].count = INBOX_TEST_ADMITS_PER_THREAD;
      workers[index].status = SALTS_EALREADY;
      check_equal(salts_thread_create(&threads[index], inbox_admit_worker_run, &workers[index]),
                  SALTS_OK);
    }
    for (size_t index = 0u; index < INBOX_TEST_ADMIT_THREADS; ++index) {
      check_equal(salts_thread_join(&threads[index]), SALTS_OK);
      check_equal(workers[index].status, SALTS_OK);
      check_not_equal(workers[index].record_id, (uint64_t)0u);
      if (index != 0u) check_equal(workers[index].record_id, workers[0].record_id);
    }
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.generation, (uint64_t)1u);
    check_equal(snapshot.records, (size_t)1u);
    check_equal(snapshot.admitted, (uint64_t)1u);
    {
      turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
      check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
      check_equal(claim.record_id, workers[0].record_id);
      check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    }
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }
}
