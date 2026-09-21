#include "turbo_flow_turbodb.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define CRASH_MAX_RECORDS 4u
#define CRASH_MAX_TOTAL_BYTES 256u
#define CRASH_MAX_RECORD_BYTES 128u
#define CRASH_MAX_CLAIMS 2u

static int crash_record(turbo_flow_inbox_record_t *record, const char *admission_id,
                        const char *payload, uint64_t sequence) {
  int rc;
  if (!record || !admission_id || !payload) return SALTS_EINVAL;
  turbo_flow_inbox_record_init(record);
  record->source_id = vstr_from_buf("crash.orders", sizeof("crash.orders") - 1u);
  record->partition_key = record->source_id;
  record->admission_id = vstr_from_buf(admission_id, strlen(admission_id));
  record->source_sequence = sequence;
  record->timestamp_ns = UINT64_C(9000000000) + sequence;
  record->message_type = 17u;
  rc = turbo_flow_content_descriptor_init(
      &record->content, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
      TURBO_FLOW_DATA_ENCODING_JSON, "application/json", "orders/crash-recovery");
  if (rc != SALTS_OK) return rc;
  rc = turbo_flow_content_descriptor_declare_schema(
      &record->content, "orders.crash.v1", "CrashOrder", 1u);
  if (rc != SALTS_OK) return rc;
  record->correlation = vstr_from_buf(admission_id, strlen(admission_id));
  record->payload = vstr_from_buf(payload, strlen(payload));
  return SALTS_OK;
}

int main(int argc, char **argv) {
  turbo_flow_turbodb_inbox_config_t config = turbo_flow_turbodb_inbox_config_default();
  turbo_flow_inbox_record_t claimed_record;
  turbo_flow_inbox_record_t pending_record;
  turbo_flow_inbox_receipt_t claimed_receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
  turbo_flow_inbox_receipt_t pending_receipt = TURBO_FLOW_INBOX_RECEIPT_INIT;
  turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
  turbo_flow_inbox_t inbox = TURBO_FLOW_INBOX_INIT;
  orm_option_t filename;
  orm_config_t database;
  orm_error_t error;
  int rc;

  if (argc != 2 || !argv[1] || !argv[1][0]) return 2;

  orm_config(&database);
  filename.keyword = orm_view("filename");
  filename.value = orm_view(argv[1]);
  database.driver = orm_view("sqlite");
  database.options = &filename;
  database.option_count = 1u;

  config.database = &database;
  config.namespace_name = "orders";
  config.max_records = CRASH_MAX_RECORDS;
  config.max_total_bytes = CRASH_MAX_TOTAL_BYTES;
  config.max_record_bytes = CRASH_MAX_RECORD_BYTES;
  config.max_claims = CRASH_MAX_CLAIMS;
  config.connection_count = 2u;

  rc = crash_record(&claimed_record, "claimed-before-crash", "claimed", 1u);
  if (rc != SALTS_OK) return 10;
  rc = crash_record(&pending_record, "pending-after-crash", "pending", 2u);
  if (rc != SALTS_OK) return 11;
  rc = turbo_flow_turbodb_inbox_create(&config, &inbox, &error);
  if (rc != SALTS_OK) return 12;
  rc = turbo_flow_inbox_admit(&inbox, &claimed_record, &claimed_receipt);
  if (rc != SALTS_OK) return 13;
  rc = turbo_flow_inbox_admit(&inbox, &pending_record, &pending_receipt);
  if (rc != SALTS_OK) return 14;
  rc = turbo_flow_inbox_claim(&inbox, &claim);
  if (rc != SALTS_OK) return 15;
  if (claim.record_id != claimed_receipt.record_id ||
      pending_receipt.record_id == claimed_receipt.record_id)
    return 16;

  /*
   * Literal abnormal process death: do not close or destroy Inbox/provider/ORM
   * state. The parent process validates the committed SQLite state and then
   * performs an explicit generation takeover.
   */
  abort();
  return 17;
}
