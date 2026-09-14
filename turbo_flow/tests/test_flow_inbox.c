#include "tinytest.h"
#include "turbo_flow_inbox.h"

#include <salts_error.h>
#include <salts_thread.h>

#include <stdint.h>
#include <string.h>

enum {
  INBOX_TEST_ADMIT_THREADS = 4,
  INBOX_TEST_ADMITS_PER_THREAD = 8
};

typedef struct inbox_admit_worker_s {
  turbo_flow_inbox_t *inbox;
  const turbo_flow_inbox_record_t *record;
  size_t count;
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

static int inbox_malformed_lifecycle(void *ctx) {
  (void)ctx;
  return SALTS_OK;
}

static int inbox_malformed_snapshot(void *ctx, turbo_flow_inbox_snapshot_t *snapshot) {
  int mode = *(const int *)ctx;
  if (mode == 1) {
    snapshot->size = 0u;
  } else if (mode == 2) {
    snapshot->accepting = 2;
  } else {
    snapshot->records = 1u;
    snapshot->pending_records = 1u;
    snapshot->in_flight_claims = 1u;
  }
  return SALTS_OK;
}

static const turbo_flow_inbox_ops_v1_t inbox_malformed_ops = {
    sizeof(turbo_flow_inbox_ops_v1_t), TURBO_FLOW_INBOX_API_VERSION,
    inbox_malformed_admit,             inbox_malformed_claim,
    inbox_malformed_settle,            inbox_malformed_fail,
    inbox_malformed_record_action,     inbox_malformed_record_action,
    inbox_malformed_lifecycle,         inbox_malformed_snapshot,
    inbox_malformed_lifecycle};

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
  }
}

static turbo_flow_inbox_record_t inbox_test_record(char *correlation, char *payload) {
  turbo_flow_inbox_record_t record;
  turbo_flow_inbox_record_init(&record);
  record.source_sequence = 41u;
  record.timestamp_ns = 42u;
  record.message_type = 7u;
  record.message_flags = 9u;
  check_equal(turbo_flow_content_descriptor_init(
                  &record.content, TURBO_FLOW_DOMAIN_DATA,
                  TURBO_FLOW_CONTENT_PROFILE_GENERIC, TURBO_FLOW_DATA_ENCODING_JSON,
                  "application/json", "orders.v1"),
              SALTS_OK);
  check_equal(turbo_flow_content_descriptor_declare_schema(&record.content, "OrderCreated",
                                                           "OrderCreated", 1u),
              SALTS_OK);
  record.correlation = vstr_from_buf(correlation, strlen(correlation));
  record.payload = vstr_from_buf(payload, strlen(payload));
  return record;
}

static turbo_flow_inbox_memory_config_t inbox_test_config(void) {
  turbo_flow_inbox_memory_config_t config = turbo_flow_inbox_memory_config_default();
  config.max_records = 2u;
  config.max_total_bytes = 32u;
  config.max_record_bytes = 24u;
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
    turbo_flow_inbox_t inbox = {sizeof(inbox), TURBO_FLOW_INBOX_API_VERSION,
                                &inbox_malformed_ops, &provider_context};

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

    config.max_total_bytes = first.correlation.len + first.payload.len;
    config.max_record_bytes = config.max_total_bytes;
    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &first, &first_receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &second, &second_receipt), SALTS_ENOSPC);
    check_equal(turbo_flow_inbox_claim(&inbox, &first_claim), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&inbox, &first_claim), SALTS_OK);
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
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    turbo_flow_inbox_receipt_t receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;

    check_equal(turbo_flow_inbox_memory_create(&config, &inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_OK);
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_admit(&inbox, &record, &receipt), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_EBUSY);
    check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.accepting, 0);
    check_equal(snapshot.records, (size_t)0u);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
    check_null(inbox.ops);
  }

  it("serializes concurrent Source admission into one bounded owner") {
    char correlation[] = "order-41";
    char payload[] = "payload";
    turbo_flow_inbox_memory_config_t config = inbox_test_config();
    turbo_flow_inbox_record_t record = inbox_test_record(correlation, payload);
    inbox_admit_worker_t workers[INBOX_TEST_ADMIT_THREADS] = {0};
    salts_thread_t threads[INBOX_TEST_ADMIT_THREADS] = {0};
    turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
    turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
    const size_t expected_records =
        INBOX_TEST_ADMIT_THREADS * INBOX_TEST_ADMITS_PER_THREAD;

    config.max_records = expected_records;
    config.max_total_bytes = expected_records * (record.correlation.len + record.payload.len);
    config.max_record_bytes = record.correlation.len + record.payload.len;
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
    }
    check_equal(turbo_flow_inbox_snapshot(&inbox, &snapshot), SALTS_OK);
    check_equal(snapshot.records, expected_records);
    check_equal(snapshot.admitted, (uint64_t)expected_records);
    for (uint64_t expected_id = 1u; expected_id <= expected_records; ++expected_id) {
      turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
      check_equal(turbo_flow_inbox_claim(&inbox, &claim), SALTS_OK);
      check_equal(claim.record_id, expected_id);
      check_equal(turbo_flow_inbox_complete(&inbox, &claim), SALTS_OK);
    }
    check_equal(turbo_flow_inbox_close(&inbox), SALTS_OK);
    check_equal(turbo_flow_inbox_destroy(&inbox), SALTS_OK);
  }
}
