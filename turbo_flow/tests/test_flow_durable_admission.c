#include "tinytest.h"
#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_projection.h"
#include "../src/flow_internal.h"

#include <stdlib.h>
#include <string.h>

/* Exercise real bounded storage while injecting only the provider boundary faults. */
typedef struct admission_fixture_s {
  turbo_flow_t *flow;
  turbo_flow_inbox_t backing;
  turbo_flow_inbox_t provider;
  turbo_flow_durable_buffer_binding_t *binding;
  size_t calls;
  size_t sinks;
  size_t owner_releases;
  int admit_status;
  int snapshot_status;
  uint64_t generation;
} admission_fixture_t;

static int probe_admit(void *ctx, const turbo_flow_inbox_record_t *record,
                       turbo_flow_inbox_receipt_t *receipt) {
  admission_fixture_t *f = ctx;
  ++f->calls;
  if (f->admit_status != SALTS_OK) return f->admit_status;
  return turbo_flow_inbox_admit(&f->backing, record, receipt);
}
static int probe_snapshot(void *ctx, turbo_flow_inbox_snapshot_t *snapshot) {
  admission_fixture_t *f = ctx;
  int rc;
  if (f->snapshot_status != SALTS_OK) return f->snapshot_status;
  rc = turbo_flow_inbox_snapshot(&f->backing, snapshot);
  if (rc == SALTS_OK) snapshot->generation = f->generation;
  return rc;
}
static int probe_claim(void *ctx, turbo_flow_inbox_claim_t *claim) {
  return turbo_flow_inbox_claim(&((admission_fixture_t *)ctx)->backing, claim);
}
static int probe_complete(void *ctx, uint64_t id, uint64_t token) {
  admission_fixture_t *f = ctx;
  return f->backing.ops->complete(f->backing.ctx, id, token);
}
static int probe_fail(void *ctx, uint64_t id, uint64_t token, int status) {
  admission_fixture_t *f = ctx;
  return f->backing.ops->fail(f->backing.ctx, id, token, status);
}
static int probe_retry(void *ctx, uint64_t id) {
  return turbo_flow_inbox_retry(&((admission_fixture_t *)ctx)->backing, id);
}
static int probe_discard(void *ctx, uint64_t id) {
  return turbo_flow_inbox_discard(&((admission_fixture_t *)ctx)->backing, id);
}
static int probe_forget(void *ctx, uint64_t id) {
  return turbo_flow_inbox_forget(&((admission_fixture_t *)ctx)->backing, id);
}
static int probe_scan_failed(void *ctx, uint64_t after, turbo_flow_inbox_failed_entry_t *entries,
                              size_t capacity, size_t *count) {
  return turbo_flow_inbox_scan_failed(&((admission_fixture_t *)ctx)->backing,
                                      after, entries, capacity, count);
}
static int probe_scan_history(void *ctx, uint64_t after, turbo_flow_inbox_history_entry_t *entries,
                               size_t capacity, size_t *count) {
  return turbo_flow_inbox_scan_history(&((admission_fixture_t *)ctx)->backing,
                                       after, entries, capacity, count);
}
static int probe_close(void *ctx) {
  return turbo_flow_inbox_close(&((admission_fixture_t *)ctx)->backing);
}
static int probe_destroy(void *ctx) {
  return turbo_flow_inbox_destroy(&((admission_fixture_t *)ctx)->backing);
}
static const turbo_flow_inbox_ops_v2_t probe_ops = {
  sizeof(turbo_flow_inbox_ops_v2_t), TURBO_FLOW_INBOX_API_VERSION,
  probe_admit, probe_claim, probe_complete, probe_fail, probe_retry, probe_discard, probe_forget,
  probe_scan_failed, probe_scan_history, probe_close, probe_snapshot, probe_destroy
};
static int count_sink(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                       turbo_flow_msg_t *msg) {
  (void)flow; (void)stage; (void)msg;
  ++((admission_fixture_t *)ctx)->sinks;
  return SALTS_OK;
}
static void fixture_create(admission_fixture_t *f, const turbo_flow_inbox_memory_config_t *limits) {
  static const char graph[] =
      "buffer intake resource intake.store\n"
      "source telemetry\n"
      "stage downstream adapter sink\n"
      "stage main {\n telemetry -> intake\n intake -> downstream\n}\n";
  turbo_flow_adapter_ops_t ops = {0};
  memset(f, 0, sizeof(*f));
  f->generation = 1u;
  f->flow = turbo_flow_create();
  check_not_null(f->flow);
  f->backing = (turbo_flow_inbox_t)TURBO_FLOW_INBOX_INIT;
  check_equal(turbo_flow_inbox_memory_create(limits, &f->backing), SALTS_OK);
  f->provider = (turbo_flow_inbox_t){sizeof(f->provider), TURBO_FLOW_INBOX_API_VERSION,
                                   &probe_ops, f};
  check_equal(turbo_flow_parse_string(f->flow, graph, sizeof(graph) - 1u), SALTS_OK);
  ops.consume = count_sink;
  check_equal(turbo_flow_register_adapter(f->flow, "sink", &ops, f), SALTS_OK);
}
static int fixture_bind(admission_fixture_t *f, turbo_flow_durable_identity_mode_t mode,
                         size_t max_bytes) {
  turbo_flow_durable_buffer_binding_config_t config = TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
  config.resource_name = "intake.store";
  config.inbox = &f->provider;
  config.identity_mode = mode;
  config.max_message_bytes = max_bytes;
  return turbo_flow_durable_buffer_bind(f->flow, &config, &f->binding);
}
static void fixture_start(admission_fixture_t *f) {
  check_equal(turbo_flow_compile(f->flow), SALTS_OK);
  check_equal(turbo_flow_start(f->flow), SALTS_OK);
}
static void fixture_open(admission_fixture_t *f, turbo_flow_durable_identity_mode_t mode) {
  turbo_flow_inbox_memory_config_t limits = turbo_flow_inbox_memory_config_default();
  fixture_create(f, &limits);
  check_equal(fixture_bind(f, mode, TURBO_FLOW_DURABLE_BUFFER_DEFAULT_MAX_MESSAGE_BYTES), SALTS_OK);
  fixture_start(f);
}
static void fixture_close(admission_fixture_t *f) {
  turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
  int rc;
  if (f->flow->state == TURBO_FLOW_STATE_STARTED) check_equal(turbo_flow_stop(f->flow), SALTS_OK);
  if (f->binding) check_equal(turbo_flow_durable_buffer_unbind(f->binding), SALTS_OK);
  check_equal(turbo_flow_inbox_close(&f->backing), SALTS_OK);
  while ((rc = turbo_flow_inbox_claim(&f->backing, &claim)) == SALTS_OK)
    check_equal(turbo_flow_inbox_complete(&f->backing, &claim), SALTS_OK);
  check_equal(rc, SALTS_ENOENT);
  check_equal(turbo_flow_inbox_destroy(&f->backing), SALTS_OK);
  turbo_flow_destroy(f->flow);
}
static void message_init(turbo_flow_msg_t *msg) {
  turbo_flow_msg_init(msg);
  msg->owned_payload = tstr_dup("data");
  check_not_null(msg->owned_payload);
  msg->payload = tstr_to_v(msg->owned_payload);
  msg->ts_ns = 23u; msg->type = 5u; msg->flags = 7u;
}
static void stable_identity(turbo_flow_msg_t *msg, const char *id) {
  turbo_flow_durable_identity_t identity = TURBO_FLOW_DURABLE_IDENTITY_INIT;
  identity.source_id = vstr_from_buf("src", 3u);
  identity.admission_id = vstr_from_buf(id, strlen(id));
  identity.correlation = vstr_from_buf("corr", 4u);
  identity.source_sequence = 17u;
  check_equal(turbo_flow_msg_set_durable_identity(msg, &identity), SALTS_OK);
}
static void check_storage(admission_fixture_t *f, size_t records) {
  turbo_flow_inbox_snapshot_t snapshot = TURBO_FLOW_INBOX_SNAPSHOT_INIT;
  check_equal(turbo_flow_inbox_snapshot(&f->backing, &snapshot), SALTS_OK);
  check_equal(snapshot.pending_records, records);
  check_equal(snapshot.admitted, (uint64_t)records);
  check_equal(f->sinks, 0u);
}
static const turbo_flow_data_schema_t int_schema = {
  sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
  "cmeta.int.data", "Integer", "int", 7u, 3u, NULL
};
static int clone_int(const void *value, void *ctx, void **out) {
  (void)ctx;
  *out = malloc(sizeof(int));
  if (!*out) return SALTS_ENOMEM;
  *(int *)*out = *(const int *)value;
  return SALTS_OK;
}
static void destroy_int(void *value, void *ctx) { (void)ctx; free(value); }
static int release_owner(void *ctx) {
  ++((admission_fixture_t *)ctx)->owner_releases;
  return SALTS_OK;
}
static void bind_projection(turbo_flow_msg_t *msg, int with_descriptor) {
  int *value = malloc(sizeof(int));
  check_not_null(value); *value = 42;
  check_equal(turbo_flow_msg_bind_typed_projection(msg, &int_schema, &cmeta_data_int,
                                                   value, clone_int, destroy_int, NULL), SALTS_OK);
  if (with_descriptor) {
    turbo_flow_content_descriptor_t descriptor = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
    check_equal(turbo_flow_content_descriptor_init(&descriptor, TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_CONTENT_PROFILE_GENERIC, TURBO_FLOW_DATA_ENCODING_OPAQUE,
        "application/octet-stream", "intake"), SALTS_OK);
    check_equal(turbo_flow_content_descriptor_declare_schema(&descriptor,
        int_schema.schema_name, int_schema.type_name, int_schema.schema_version), SALTS_OK);
    check_equal(turbo_flow_msg_copy_content_descriptor(msg, &descriptor), SALTS_OK);
  }
}

spec("Graph durable admission boundaries") {
  it("rejects missing stable identity before calling the provider") {
    admission_fixture_t f; turbo_flow_msg_t msg;
    fixture_open(&f, TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED); message_init(&msg);
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_EINVAL);
    check_equal(f.calls, 0u); check_storage(&f, 0u);
    turbo_flow_msg_cleanup(&msg); fixture_close(&f);
  }
  it("rejects malformed payload and transport context without losing caller ownership") {
    admission_fixture_t f; turbo_flow_msg_t msg;
    fixture_open(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED); message_init(&msg);
    msg.payload.len++;
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_EINVAL);
    msg.payload.data = NULL;
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_EINVAL);
    msg.payload = tstr_to_v(msg.owned_payload); msg.transport_context = &f;
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_ENOTSUP);
    check_equal(f.calls, 0u); check_storage(&f, 0u);
    check_equal(msg.payload.data, "data", 4u);
    msg.transport_context = NULL; turbo_flow_msg_cleanup(&msg); fixture_close(&f);
  }
  it("rejects a projection without its descriptor or canonical payload") {
    for (int with_descriptor = 0; with_descriptor < 2; ++with_descriptor) {
      admission_fixture_t f; turbo_flow_msg_t msg;
      fixture_open(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED);
      if (with_descriptor) turbo_flow_msg_init(&msg); else message_init(&msg);
      bind_projection(&msg, with_descriptor);
      check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_ENOTSUP);
      check_equal(f.calls, 0u); check_storage(&f, 0u);
      turbo_flow_msg_cleanup(&msg); fixture_close(&f);
    }
  }
  it("copies canonical payload and descriptor without persisting a projection pointer") {
    admission_fixture_t f; turbo_flow_msg_t msg;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    fixture_open(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED); message_init(&msg);
    bind_projection(&msg, 1);
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_OK);
    check_storage(&f, 1u); turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_inbox_claim(&f.backing, &claim), SALTS_OK);
    check_equal(claim.record.payload.len, 4u);
    check_equal(claim.record.payload.data, "data", 4u);
    check_equal(claim.record.content.schema_name, "cmeta.int.data");
    check_equal(turbo_flow_inbox_complete(&f.backing, &claim), SALTS_OK);
    fixture_close(&f);
  }
  it("rejects a committed result and an active result claim") {
    admission_fixture_t f; turbo_flow_msg_t msg;
    turbo_flow_projection_owner_config_t config = TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT;
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_result_claim_t *claim = NULL;
    void *value = malloc(sizeof(int));
    check_not_null(value); *(int *)value = 9;
    fixture_open(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED); message_init(&msg);
    config.flags = TURBO_FLOW_PROJECTION_IMMUTABLE | TURBO_FLOW_PROJECTION_CROSS_THREAD |
                   TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT;
    config.capacity = 4u; config.max_result_bytes = sizeof(int);
    config.max_retained_bytes = 4u * sizeof(int); config.schema = &int_schema;
    config.clone = clone_int; config.destroy = destroy_int;
    config.ctx = &f; config.release_context = release_owner;
    check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
    check_equal(turbo_flow_msg_result_claim(&msg, owner, &cmeta_data_int, &claim), SALTS_OK);
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_EBUSY);
    turbo_flow_msg_result_abort(&claim);
    check_null(claim);
    turbo_flow_msg_t copy;
    check_equal(turbo_flow_msg_clone(&copy, &msg), SALTS_OK);
    turbo_flow_msg_cleanup(&copy);
    check_equal(turbo_flow_msg_result_claim(&msg, owner, &cmeta_data_int, &claim), SALTS_OK);
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_EBUSY);
    check_equal(turbo_flow_msg_result_commit(&claim, &value), SALTS_OK);
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_ENOTSUP);
    check_equal(f.calls, 0u); check_storage(&f, 0u);
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(f.owner_releases, 1u);
    fixture_close(&f);
  }
  it("preserves stable replay identity and rejects changed records under the same identity") {
    admission_fixture_t f; turbo_flow_msg_t msg;
    turbo_flow_inbox_claim_t claim = TURBO_FLOW_INBOX_CLAIM_INIT;
    fixture_open(&f, TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED); message_init(&msg);
    stable_identity(&msg, "a");
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_OK);
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_OK);
    msg.flags++;
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_EPROTO);
    msg.flags--; check_storage(&f, 1u); turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_inbox_claim(&f.backing, &claim), SALTS_OK);
    check_equal(claim.record.source_id.data, "src", 3u);
    check_equal(claim.record.admission_id.data, "a", 1u);
    check_equal(claim.record.correlation.data, "corr", 4u);
    check_equal(claim.record.source_sequence, UINT64_C(17));
    check_equal(claim.record.timestamp_ns, UINT64_C(23));
    check_equal(claim.record.message_type, 5u); check_equal(claim.record.message_flags, 7u);
    check_equal(turbo_flow_inbox_complete(&f.backing, &claim), SALTS_OK);
    fixture_close(&f);
  }
  it("enforces record capacity at one and N without executing downstream on overflow") {
    const size_t capacities[] = {1u, 3u};
    for (size_t c = 0u; c < sizeof(capacities) / sizeof(capacities[0]); ++c) {
      admission_fixture_t f; turbo_flow_msg_t msg;
      turbo_flow_inbox_memory_config_t limits = turbo_flow_inbox_memory_config_default();
      limits.max_records = capacities[c]; limits.max_claims = 1u;
      fixture_create(&f, &limits);
      check_equal(fixture_bind(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED, 1024u), SALTS_OK);
      fixture_start(&f); message_init(&msg); check_storage(&f, 0u);
      for (size_t i = 0u; i < capacities[c]; ++i)
        check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_OK);
      check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_ENOSPC);
      check_storage(&f, capacities[c]); turbo_flow_msg_cleanup(&msg); fixture_close(&f);
    }
  }
  it("enforces binding, record and retained byte limits at the exact bound") {
    /* src(3) + admission(1) + correlation(4) + payload(4) = 12 variable bytes. */
    enum { RECORD_BYTES = 12 };
    for (int bound = 0; bound < 3; ++bound) {
      admission_fixture_t f; turbo_flow_msg_t msg;
      turbo_flow_inbox_memory_config_t limits = turbo_flow_inbox_memory_config_default();
      if (bound == 1) limits.max_record_bytes = RECORD_BYTES;
      if (bound == 2) limits.max_total_bytes = limits.max_record_bytes = RECORD_BYTES;
      fixture_create(&f, &limits);
      check_equal(fixture_bind(&f, TURBO_FLOW_DURABLE_IDENTITY_STABLE_REQUIRED,
                                bound == 0 ? RECORD_BYTES : 1024u), SALTS_OK);
      fixture_start(&f); message_init(&msg); stable_identity(&msg, "a");
      check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_OK);
      stable_identity(&msg, bound == 2 ? "b" : "bb");
      check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_ENOSPC);
      check_storage(&f, 1u);
      check_equal(f.calls, bound == 0 ? 1u : 2u);
      turbo_flow_msg_cleanup(&msg); fixture_close(&f);
    }
  }
  it("returns exact provider errors without retry or downstream bypass") {
    const int errors[] = {SALTS_EIO, SALTS_EBUSY, SALTS_ENOSPC, SALTS_ESHUTDOWN};
    admission_fixture_t f; turbo_flow_msg_t msg;
    fixture_open(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED); message_init(&msg);
    for (size_t i = 0u; i < sizeof(errors) / sizeof(errors[0]); ++i) {
      f.admit_status = errors[i];
      check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), errors[i]);
      check_equal(f.calls, i + 1u); check_storage(&f, 0u);
    }
    turbo_flow_msg_cleanup(&msg); fixture_close(&f);
  }
  it("rejects generation changes and snapshot failure before provider admission") {
    admission_fixture_t f; turbo_flow_msg_t msg;
    fixture_open(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED); message_init(&msg);
    f.generation++;
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_ECANCELED);
    f.generation--; f.snapshot_status = SALTS_EIO;
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_EIO);
    check_equal(f.calls, 0u); check_storage(&f, 0u);
    f.snapshot_status = SALTS_OK; turbo_flow_msg_cleanup(&msg); fixture_close(&f);
  }
  it("rejects a provider handle replaced after binding") {
    admission_fixture_t f; turbo_flow_msg_t msg; turbo_flow_inbox_t saved;
    fixture_open(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED); message_init(&msg);
    saved = f.provider; f.provider = f.backing;
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_ECANCELED);
    check_storage(&f, 0u);
    f.provider = saved; turbo_flow_msg_cleanup(&msg); fixture_close(&f);
  }
  it("rejects stale generation at compile and restart") {
    admission_fixture_t f;
    turbo_flow_inbox_memory_config_t limits = turbo_flow_inbox_memory_config_default();
    fixture_create(&f, &limits);
    check_equal(fixture_bind(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED, 1024u), SALTS_OK);
    f.generation++;
    check_equal(turbo_flow_compile(f.flow), SALTS_ECANCELED);
    f.generation--; fixture_start(&f); check_equal(turbo_flow_stop(f.flow), SALTS_OK);
    f.generation++;
    check_equal(turbo_flow_start(f.flow), SALTS_ECANCELED);
    check_storage(&f, 0u); fixture_close(&f);
  }
  it("does not reuse generated admission identities after unbind and rebind") {
    admission_fixture_t f; turbo_flow_msg_t msg;
    fixture_open(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED); message_init(&msg);
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_OK);
    check_equal(turbo_flow_stop(f.flow), SALTS_OK);
    check_equal(turbo_flow_durable_buffer_unbind(f.binding), SALTS_OK); f.binding = NULL;
    check_equal(fixture_bind(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED, 1024u), SALTS_OK);
    fixture_start(&f);
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_OK);
    check_storage(&f, 2u);
    turbo_flow_msg_cleanup(&msg); fixture_close(&f);
  }
  it("rejects duplicate provider aliases and binding mutation while started") {
    admission_fixture_t f; turbo_flow_inbox_t alias;
    turbo_flow_durable_buffer_binding_t *extra = NULL;
    turbo_flow_durable_buffer_binding_config_t config = TURBO_FLOW_DURABLE_BUFFER_BINDING_CONFIG_INIT;
    turbo_flow_inbox_memory_config_t limits = turbo_flow_inbox_memory_config_default();
    fixture_create(&f, &limits);
    check_equal(fixture_bind(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED, 1024u), SALTS_OK);
    alias = f.provider; config.inbox = &alias; config.resource_name = "other.store";
    check_equal(turbo_flow_durable_buffer_bind(f.flow, &config, &extra), SALTS_EALREADY);
    if (extra) check_equal(turbo_flow_durable_buffer_unbind(extra), SALTS_OK);
    config.inbox = &f.provider; config.resource_name = "intake.store";
    check_equal(turbo_flow_durable_buffer_bind(f.flow, &config, &extra), SALTS_EALREADY);
    check_null(extra); fixture_start(&f);
    check_equal(turbo_flow_durable_buffer_bind(f.flow, &config, &extra), SALTS_EBUSY);
    check_null(extra); check_equal(turbo_flow_durable_buffer_unbind(f.binding), SALTS_EBUSY);
    check_storage(&f, 0u); fixture_close(&f);
  }
  it("rejects zero or unbounded limits and invalid provider generation at bind") {
    admission_fixture_t f;
    turbo_flow_inbox_memory_config_t limits = turbo_flow_inbox_memory_config_default();
    fixture_create(&f, &limits);
    check_equal(fixture_bind(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED, 0u), SALTS_EINVAL);
    check_null(f.binding);
    check_equal(fixture_bind(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED, SIZE_MAX), SALTS_EINVAL);
    check_null(f.binding); f.generation = 0u;
    check_equal(fixture_bind(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED, 1024u), SALTS_EPROTO);
    check_null(f.binding); f.generation = 1u; f.snapshot_status = SALTS_EIO;
    check_equal(fixture_bind(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED, 1024u), SALTS_EIO);
    check_null(f.binding); f.snapshot_status = SALTS_OK;
    check_storage(&f, 0u); fixture_close(&f);
  }
  it("rejects generated sequence overflow before provider admission") {
    admission_fixture_t f; turbo_flow_msg_t msg;
    fixture_open(&f, TURBO_FLOW_DURABLE_IDENTITY_GENERATED); message_init(&msg);
    atomic_store_explicit(&f.binding->next_sequence, UINT64_MAX, memory_order_relaxed);
    check_equal(turbo_flow_publish(f.flow, "telemetry", &msg), SALTS_ERANGE);
    check_equal(f.calls, 0u); check_storage(&f, 0u);
    turbo_flow_msg_cleanup(&msg); fixture_close(&f);
  }
}
