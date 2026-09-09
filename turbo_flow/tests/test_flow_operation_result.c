#include "tinytest.h"
#include "turbo_flow_projection.h"
#include <stdint.h>
#include <stdlib.h>

enum { CLONE_ALLOCATE, CLONE_ALIAS, CLONE_NULL, CLONE_FAIL_VALUE, CLONE_LAST_VALUE };
typedef struct counts_s { int cloned; int destroyed; int released; int clone_mode; void *alias; } counts_t;
static void *last_cloned_value;
static const turbo_flow_data_schema_t int_schema = {
  sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
  "cmeta.int.data", "Integer", "int", 7u, 3u, NULL
};
static const turbo_flow_data_schema_t long_schema = {
  sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
  "cmeta.long.data", "Long", "long", 8u, 3u, NULL
};
static int clone_int(const void *v, void *ctx, void **out) {
  counts_t *counts = (counts_t *)ctx;
  ++counts->cloned;
  if (counts->clone_mode == CLONE_ALIAS) { *out = counts->alias; return SALTS_OK; }
  if (counts->clone_mode == CLONE_LAST_VALUE) { *out = last_cloned_value; return SALTS_OK; }
  if (counts->clone_mode == CLONE_NULL) { *out = NULL; return SALTS_OK; }
  *out = malloc(sizeof(int)); if (!*out) return SALTS_ENOMEM;
  *(int *)*out = *(const int *)v; last_cloned_value = *out;
  return counts->clone_mode == CLONE_FAIL_VALUE ? SALTS_EIO : SALTS_OK;
}
static void check_msg_fields_equal(const turbo_flow_msg_t *actual,
                                   const turbo_flow_msg_t *expected) {
  check_equal(actual->id, expected->id);
  check_equal(actual->ts_ns, expected->ts_ns);
  check_equal(actual->type, expected->type);
  check_equal(actual->flags, expected->flags);
  check_equal((uintptr_t)actual->buffer, (uintptr_t)expected->buffer);
  check_equal((uintptr_t)actual->payload.data, (uintptr_t)expected->payload.data);
  check_equal(actual->payload.len, expected->payload.len);
  check_equal((uintptr_t)actual->owned_payload, (uintptr_t)expected->owned_payload);
  check_equal((uintptr_t)actual->transport_context, (uintptr_t)expected->transport_context);
  check_equal((uintptr_t)actual->_content_handle, (uintptr_t)expected->_content_handle);
  check_equal(actual->status, expected->status);
  check_equal(actual->execution_attempt, expected->execution_attempt);
  check_equal((uintptr_t)actual->failure.stage_name, (uintptr_t)expected->failure.stage_name);
  check_equal((uintptr_t)actual->failure.adapter_name, (uintptr_t)expected->failure.adapter_name);
  check_equal((uintptr_t)actual->failure.route_name, (uintptr_t)expected->failure.route_name);
  check_equal(actual->failure.code, expected->failure.code);
  check_equal(actual->failure.attempt, expected->failure.attempt);
  check_equal(actual->data_decision.size, expected->data_decision.size);
  check_equal(actual->data_decision.stage_index, expected->data_decision.stage_index);
  check_equal(actual->data_decision.evaluation_status, expected->data_decision.evaluation_status);
  check_equal(actual->data_decision.match_count, expected->data_decision.match_count);
  check_equal(actual->data_decision.evaluation_error, expected->data_decision.evaluation_error);
  check_equal(actual->data_decision.dropped, expected->data_decision.dropped);
  check_equal(actual->data_decision.dead_letter, expected->data_decision.dead_letter);
  check_equal(actual->data_decision.dead_letter_status, expected->data_decision.dead_letter_status);
  check_equal(actual->data_decision.route, expected->data_decision.route,
              sizeof(actual->data_decision.route));
  check_equal(actual->data_decision.batch_key, expected->data_decision.batch_key,
              sizeof(actual->data_decision.batch_key));
  check_equal(actual->data_decision.retry_class, expected->data_decision.retry_class,
              sizeof(actual->data_decision.retry_class));
}
static void destroy_int(void *v, void *ctx) { ++((counts_t *)ctx)->destroyed; free(v); }
static int release_ctx(void *ctx) { ++((counts_t *)ctx)->released; return SALTS_OK; }
static void record_destroy(void *v, void *ctx) { (void)v; ++((counts_t *)ctx)->destroyed; }
static turbo_flow_projection_owner_t *make_owner(counts_t *counts, size_t capacity) {
  turbo_flow_projection_owner_config_t c = TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT;
  turbo_flow_projection_owner_t *owner = NULL;
  c.flags = TURBO_FLOW_PROJECTION_IMMUTABLE | TURBO_FLOW_PROJECTION_CROSS_THREAD |
            TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT;
  c.capacity = capacity; c.max_result_bytes = sizeof(int);
  c.max_retained_bytes = capacity * sizeof(int); c.schema = &int_schema;
  c.clone = clone_int; c.destroy = destroy_int; c.ctx = counts; c.release_context = release_ctx;
  check_equal(turbo_flow_projection_owner_create(&c, &owner), SALTS_OK);
  return owner;
}

spec("flow atomic typed result") {
  it("checks complete half-open ranges") {
    uintptr_t max = UINTPTR_MAX;
    char bytes[8];
    check_equal(turbo_flow_value_require_disjoint(bytes, 4, bytes + 4, 4), SALTS_OK);
    check_equal(turbo_flow_value_require_disjoint(bytes, 4, bytes + 3, 2), SALTS_EPROTO);
    check_equal(turbo_flow_value_require_disjoint(bytes, 8, bytes + 2, 2), SALTS_EPROTO);
    check_equal(turbo_flow_value_require_disjoint(bytes + 2, 2, bytes, 8), SALTS_EPROTO);
    check_equal(turbo_flow_value_require_disjoint(bytes + 4, 4, bytes, 5), SALTS_EPROTO);
    check_equal(turbo_flow_value_require_disjoint(bytes, 2, bytes + 6, 2), SALTS_OK);
    check_equal(turbo_flow_value_require_disjoint(NULL, 1, bytes, 1), SALTS_EINVAL);
    check_equal(turbo_flow_value_require_disjoint(bytes, 0, bytes + 1, 1), SALTS_EINVAL);
    check_equal(turbo_flow_value_require_disjoint(bytes, 1, NULL, 1), SALTS_EINVAL);
    check_equal(turbo_flow_value_require_disjoint((void *)max, 2, bytes, 1), SALTS_EINVAL);
  }
  it("publishes only an independent committed value and preserves projection") {
    counts_t input_counts = {0}, result_counts = {0};
    turbo_flow_projection_owner_t *result_owner = make_owner(&result_counts, 2);
    turbo_flow_projection_owner_snapshot_t owner_state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    turbo_flow_msg_t msg; turbo_flow_result_claim_t *claim = NULL;
    int *input = malloc(sizeof(int)), *result = NULL, *alias;
    char payload_bytes[4] = {1, 2, 3, 4};
    *input = 11; turbo_flow_msg_init(&msg);
    msg.buffer = mem_wrap_external(payload_bytes, sizeof(payload_bytes), NULL, NULL);
    check_not_null(msg.buffer);
    msg.payload = vstr_from_buf(payload_bytes, sizeof(payload_bytes));
    check_equal(turbo_flow_msg_bind_typed_projection(&msg, &int_schema, &cmeta_data_int,
                input, clone_int, destroy_int, &input_counts), SALTS_OK);
    check_equal(turbo_flow_msg_result_claim(&msg, result_owner, &cmeta_data_int, &claim), SALTS_OK);
    alias = input;
    check_equal(turbo_flow_msg_result_commit(&claim, (void **)&alias), SALTS_EPROTO);
    check_not_null(claim); check_null(turbo_flow_msg_result(&msg, NULL, NULL));
    check_equal(turbo_flow_msg_projection(&msg, NULL), input);
    turbo_flow_msg_result_abort(&claim);
    check_null(claim);
    check_equal(turbo_flow_msg_result_claim(&msg, result_owner, &cmeta_data_int, &claim), SALTS_OK);
    result = malloc(sizeof(int)); *result = 23;
    check_equal(turbo_flow_msg_result_commit(&claim, (void **)&result), SALTS_OK);
    check_null(result); check_equal(*(const int *)turbo_flow_msg_result(&msg, NULL, NULL), 23);
    {
      turbo_flow_msg_t copy, moved;
      turbo_flow_msg_init(&copy); turbo_flow_msg_init(&moved);
      check_equal(turbo_flow_msg_clone(&copy, &msg), SALTS_OK);
      check_equal(*(const int *)turbo_flow_msg_result(&copy, NULL, NULL), 23);
      check_equal(turbo_flow_msg_move(&moved, &copy), SALTS_OK);
      check_null(turbo_flow_msg_result(&copy, NULL, NULL));
      check_equal(*(const int *)turbo_flow_msg_result(&moved, NULL, NULL), 23);
      turbo_flow_msg_cleanup(&moved);
    }
    {
      turbo_flow_msg_t view;
      turbo_flow_msg_init(&view); view.id = 99;
      {
        turbo_flow_msg_t source_before = msg, destination_before = view;
        check_equal(turbo_flow_msg_retain_view(&view, &msg), SALTS_EINVAL);
        check_msg_fields_equal(&msg, &source_before);
        check_msg_fields_equal(&view, &destination_before);
      }
      check_equal(mem_buffer_ref_count(msg.buffer), (uint32_t)1);
      check_equal(input_counts.cloned, 1); check_equal(result_counts.cloned, 1);
      check_equal(input_counts.destroyed, 1); check_equal(result_counts.destroyed, 1);
      check_equal(turbo_flow_projection_owner_snapshot(result_owner, &owner_state), SALTS_OK);
      check_equal(owner_state.outstanding, (size_t)1);
      check_equal(owner_state.retained_bytes, sizeof(int));
      check_equal(*(const int *)turbo_flow_msg_result(&msg, NULL, NULL), 23);
      check_equal(msg.payload.data[2], (char)3);
      turbo_flow_msg_clear_projection(&msg);
      {
        turbo_flow_msg_t source_before = msg, destination_before = view;
        check_equal(turbo_flow_msg_retain_view(&view, &msg), SALTS_EINVAL);
        check_msg_fields_equal(&msg, &source_before);
        check_msg_fields_equal(&view, &destination_before);
      }
      check_equal(mem_buffer_ref_count(msg.buffer), (uint32_t)1);
      check_equal(input_counts.cloned, 1); check_equal(result_counts.cloned, 1);
      check_equal(input_counts.destroyed, 2); check_equal(result_counts.destroyed, 1);
      check_equal(turbo_flow_projection_owner_snapshot(result_owner, &owner_state), SALTS_OK);
      check_equal(owner_state.outstanding, (size_t)1);
      check_equal(owner_state.retained_bytes, sizeof(int));
      check_equal(*(const int *)turbo_flow_msg_result(&msg, NULL, NULL), 23);
      check_equal(msg.payload.data[2], (char)3);
      turbo_flow_msg_clear_content(&msg);
      {
        turbo_flow_msg_t source_before = msg, destination_before = view;
        check_equal(turbo_flow_msg_retain_view(&view, &msg), SALTS_EINVAL);
        check_msg_fields_equal(&msg, &source_before);
        check_msg_fields_equal(&view, &destination_before);
      }
      check_equal(mem_buffer_ref_count(msg.buffer), (uint32_t)1);
      check_equal(input_counts.cloned, 1); check_equal(result_counts.cloned, 1);
      check_equal(input_counts.destroyed, 2); check_equal(result_counts.destroyed, 1);
      check_equal(turbo_flow_projection_owner_snapshot(result_owner, &owner_state), SALTS_OK);
      check_equal(owner_state.outstanding, (size_t)1);
      check_equal(owner_state.retained_bytes, sizeof(int));
      check_equal(*(const int *)turbo_flow_msg_result(&msg, NULL, NULL), 23);
      check_equal(msg.payload.data[2], (char)3);
      turbo_flow_msg_clear_result(&msg);
      check_equal(turbo_flow_msg_retain_view(&view, &msg), SALTS_OK);
      check_equal(mem_buffer_ref_count(msg.buffer), (uint32_t)2);
      turbo_flow_msg_cleanup(&view);
      check_equal(mem_buffer_ref_count(msg.buffer), (uint32_t)1);
    }
    check_equal(result_counts.destroyed, 2);
    check_equal(input_counts.destroyed, 2);
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_projection_owner_stop(result_owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(result_owner), SALTS_OK);
  }
  it("allows a reserved claim to commit after owner stop") {
    counts_t input_counts = {0}, result_counts = {0};
    turbo_flow_projection_owner_t *owner = make_owner(&result_counts, 1);
    turbo_flow_msg_t msg; turbo_flow_result_claim_t *claim = NULL;
    int *input = malloc(sizeof(int)), *result = malloc(sizeof(int));
    *input = 1; *result = 2; turbo_flow_msg_init(&msg);
    check_equal(turbo_flow_msg_bind_typed_projection(&msg, &int_schema, &cmeta_data_int,
                input, clone_int, destroy_int, &input_counts), SALTS_OK);
    check_equal(turbo_flow_msg_result_claim(&msg, owner, &cmeta_data_int, &claim), SALTS_OK);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_msg_result_commit(&claim, (void **)&result), SALTS_OK);
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
  }
  it("rejects payload aliases, null candidates, stopped owners and undercharged layouts") {
    counts_t counts = {0}, input_counts = {0};
    turbo_flow_projection_owner_t *owner = make_owner(&counts, 2);
    turbo_flow_msg_t msg;
    turbo_flow_result_claim_t *claim = NULL;
    int input = 1;
    int payload[2] = {3, 4};
    void *candidate;
    turbo_flow_msg_init(&msg);
    msg.payload = vstr_from_buf((const char *)payload, sizeof(payload));
    check_equal(turbo_flow_msg_bind_typed_projection(&msg, &int_schema, &cmeta_data_int,
                &input, clone_int, record_destroy, &input_counts), SALTS_OK);
    check_equal(turbo_flow_msg_result_claim(&msg, owner, &cmeta_data_int, &claim), SALTS_OK);
    candidate = &payload[1];
    check_equal(turbo_flow_msg_result_commit(&claim, &candidate), SALTS_EPROTO);
    check_not_null(claim); check_equal(candidate, (void *)&payload[1]);
    candidate = NULL;
    check_equal(turbo_flow_msg_result_commit(&claim, &candidate), SALTS_EPROTO);
    turbo_flow_msg_result_abort(&claim);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_msg_result_claim(&msg, owner, &cmeta_data_int, &claim), SALTS_EBUSY);
    check_equal(counts.destroyed, 0);
    turbo_flow_msg_clear_projection(&msg);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
  }
  it("reports deterministic bounded metadata costs") {
    turbo_flow_result_memory_requirements_t r;
    turbo_flow_result_memory_requirements_init(&r);
    check_equal(turbo_flow_result_memory_requirements(3, 17, &r), SALTS_OK);
    check_equal(r.peak_metadata_bytes,
                r.owner_bytes + 3 * (r.claim_bytes + r.message_bytes));
    check_equal(r.payload_bound_bytes, (size_t)51);
    check_equal(turbo_flow_result_memory_requirements(0, 17, &r), SALTS_EINVAL);
    check_equal(r.peak_metadata_bytes, (size_t)0);
    turbo_flow_result_memory_requirements_init(&r); r.size++;
    check_equal(turbo_flow_result_memory_requirements(1, 1, &r), SALTS_EINVAL);
    turbo_flow_result_memory_requirements_init(&r); r.size--;
    check_equal(turbo_flow_result_memory_requirements(1, 1, &r), SALTS_EINVAL);
    turbo_flow_result_memory_requirements_init(&r); r.abi_minor++;
    check_equal(turbo_flow_result_memory_requirements(1, 1, &r), SALTS_EINVAL);
    turbo_flow_result_memory_requirements_init(&r);
    check_equal(turbo_flow_result_memory_requirements(SIZE_MAX, SIZE_MAX, &r), SALTS_EINVAL);
    check_equal(r.owner_bytes, (size_t)0);
  }
  it("never destroys source or temporary aliases returned by clone callbacks") {
    counts_t input_counts = {0}, result_counts = {0};
    turbo_flow_projection_owner_t *owner = make_owner(&result_counts, 3);
    turbo_flow_msg_t source, copy;
    turbo_flow_result_claim_t *claim = NULL;
    int *input = malloc(sizeof(int)), *result = malloc(sizeof(int));
    *input = 5; *result = 6; turbo_flow_msg_init(&source); turbo_flow_msg_init(&copy);
    check_equal(turbo_flow_msg_bind_typed_projection(&source, &int_schema, &cmeta_data_int,
                input, clone_int, destroy_int, &input_counts), SALTS_OK);
    check_equal(turbo_flow_msg_result_claim(&source, owner, &cmeta_data_int, &claim), SALTS_OK);
    check_equal(turbo_flow_msg_result_commit(&claim, (void **)&result), SALTS_OK);
    input_counts.clone_mode = CLONE_ALIAS;
    input_counts.alias = (unsigned char *)input + 1;
    check_equal(turbo_flow_msg_clone(&copy, &source), SALTS_EPROTO);
    check_equal(input_counts.destroyed, 0);
    input_counts.clone_mode = CLONE_ALLOCATE;
    result_counts.clone_mode = CLONE_LAST_VALUE;
    check_equal(turbo_flow_msg_clone(&copy, &source), SALTS_EPROTO);
    check_equal(result_counts.destroyed, 0);
    check_equal(input_counts.destroyed, 1);
    result_counts.clone_mode = CLONE_NULL;
    check_equal(turbo_flow_msg_clone(&copy, &source), SALTS_EPROTO);
    check_equal(input_counts.destroyed, 2);
    check_equal(result_counts.destroyed, 0);
    result_counts.clone_mode = CLONE_FAIL_VALUE;
    check_equal(turbo_flow_msg_clone(&copy, &source), SALTS_EIO);
    check_equal(input_counts.destroyed, 3);
    check_equal(result_counts.destroyed, 1);
    result_counts.clone_mode = CLONE_ALLOCATE;
    turbo_flow_msg_cleanup(&source);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
  }
  it("supports empty-content claims and rejects full or under-sized owners") {
    counts_t counts = {0};
    turbo_flow_projection_owner_t *owner = make_owner(&counts, 1);
    turbo_flow_msg_t empty;
    turbo_flow_result_claim_t *claim = NULL;
    int *result = malloc(sizeof(int));
    *result = 9; turbo_flow_msg_init(&empty);
    check_equal(turbo_flow_msg_result_claim(&empty, owner, &cmeta_data_int, &claim), SALTS_OK);
    check_equal(turbo_flow_msg_result_commit(&claim, (void **)&result), SALTS_OK);
    check_equal(*(const int *)turbo_flow_msg_result(&empty, NULL, NULL), 9);
    check_equal(turbo_flow_msg_result_claim(&empty, owner, &cmeta_data_int, &claim), SALTS_EALREADY);
    {
      turbo_flow_msg_t second;
      turbo_flow_msg_init(&second);
      check_equal(turbo_flow_msg_result_claim(&second, owner, &cmeta_data_int, &claim), SALTS_ENOSPC);
      check_null(claim);
    }
    turbo_flow_msg_cleanup(&empty);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    {
      turbo_flow_projection_owner_config_t c = TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT;
      turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
      c.flags = TURBO_FLOW_PROJECTION_IMMUTABLE | TURBO_FLOW_PROJECTION_CROSS_THREAD |
                TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT;
      c.capacity = 1; c.max_result_bytes = 1; c.max_retained_bytes = 1;
      c.schema = &long_schema; c.clone = clone_int; c.destroy = destroy_int;
      c.ctx = &counts; c.release_context = release_ctx;
      check_equal(turbo_flow_projection_owner_create(&c, &owner), SALTS_OK);
      turbo_flow_msg_init(&empty);
      check_equal(turbo_flow_msg_result_claim(&empty, owner, &cmeta_data_long, &claim), SALTS_EPROTO);
      check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
      check_equal(state.outstanding, (size_t)0);
      check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
      check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    }
  }
  it("retains caller ownership on typed bind failure and rejects ordinary projection provenance") {
    counts_t counts = {0};
    turbo_flow_projection_owner_t *owner = make_owner(&counts, 1);
    turbo_flow_data_schema_t bad = int_schema;
    turbo_flow_result_claim_t *claim = NULL;
    turbo_flow_msg_t msg;
    int *value = malloc(sizeof(int));
    *value = 12; bad.schema_name = "other"; turbo_flow_msg_init(&msg);
    check_equal(turbo_flow_msg_bind_typed_projection(&msg, &bad, &cmeta_data_int,
                value, clone_int, destroy_int, &counts), SALTS_EINVAL);
    check_equal(counts.destroyed, 0); check_equal(*value, 12);
    check_equal(turbo_flow_msg_bind_projection(&msg, &int_schema, value,
                clone_int, destroy_int, &counts), SALTS_OK);
    check_null(turbo_flow_msg_projection_data(&msg));
    check_equal(turbo_flow_msg_result_claim(&msg, owner, &cmeta_data_int, &claim), SALTS_ENOTSUP);
    check_null(claim);
    turbo_flow_msg_cleanup(&msg);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
  }
  it("rejects cloning a result combined with an untyped projection before callbacks") {
    counts_t projection_counts = {0}, result_counts = {0};
    turbo_flow_projection_owner_t *owner = make_owner(&result_counts, 2);
    turbo_flow_result_claim_t *claim = NULL;
    turbo_flow_msg_t source, copy;
    int *result = malloc(sizeof(int)), *projection = malloc(sizeof(int));
    int rc;
    *result = 31; *projection = 32;
    turbo_flow_msg_init(&source); turbo_flow_msg_init(&copy);
    check_equal(turbo_flow_msg_result_claim(&source, owner, &cmeta_data_int, &claim), SALTS_OK);
    check_equal(turbo_flow_msg_result_commit(&claim, (void **)&result), SALTS_OK);
    check_equal(turbo_flow_msg_bind_projection(&source, &int_schema, projection,
                clone_int, destroy_int, &projection_counts), SALTS_OK);
    result_counts.clone_mode = CLONE_LAST_VALUE;
    rc = turbo_flow_msg_clone(&copy, &source);
    check_equal(rc, SALTS_ENOTSUP);
    check_equal(projection_counts.destroyed, 0);
    check_equal(result_counts.destroyed, 0);
    if (rc != SALTS_OK) turbo_flow_msg_cleanup(&source);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    if (rc != SALTS_OK) check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
  }
  it("rejects an unrepresentable result candidate even when the message has no borrowed span") {
    counts_t counts = {0};
    turbo_flow_projection_owner_t *owner = make_owner(&counts, 1);
    turbo_flow_result_claim_t *claim = NULL;
    turbo_flow_msg_t msg;
    void *candidate = (void *)UINTPTR_MAX;
    int rc;
    turbo_flow_msg_init(&msg);
    check_equal(turbo_flow_msg_result_claim(&msg, owner, &cmeta_data_int, &claim), SALTS_OK);
    rc = turbo_flow_msg_result_commit(&claim, &candidate);
    check_equal(rc, SALTS_EPROTO);
    check_not_null(claim); check_equal(candidate, (void *)UINTPTR_MAX);
    if (rc != SALTS_OK) {
      turbo_flow_msg_result_abort(&claim);
      check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
      check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    }
  }
}
