#include "turbo_flow_projection.h"
#include "tinytest.h"
#include <salts/thread.h>
#include <stdlib.h>

enum { TEST_VALUE = 41, TEST_CAPACITY = 2 };
int flow_projection_cpp_example(void);
enum { CLONE_NORMAL, CLONE_ERROR_VALUE, CLONE_NULL, CLONE_ALIAS, CLONE_ERROR_ALIAS };
typedef struct callback_gate_s {
  salts_mutex_t mutex;
  salts_cond_t cond;
  int entered;
  int released;
  int block_destroy;
} callback_gate_t;
typedef struct projection_counts_s {
  int payloads;
  int contexts;
  int clone_mode;
  int release_error;
  callback_gate_t *gate;
} projection_counts_t;
static void gate_enter(callback_gate_t *gate) {
  salts_mutex_lock(&gate->mutex);
  ++gate->entered;
  salts_cond_broadcast(&gate->cond);
  while (!gate->released) salts_cond_wait(&gate->cond, &gate->mutex);
  salts_mutex_unlock(&gate->mutex);
}
static const turbo_flow_data_schema_t schema = {
    sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
    TURBO_FLOW_DATA_ENCODING_OPAQUE, "retained", "Integer", "test.int", 1u, 1u, NULL};
static int *new_value(void) {
  int *value = (int *)malloc(sizeof(*value));
  if (value) *value = TEST_VALUE;
  return value;
}
static int clone_value(const void *value, void *ctx, void **out) {
  projection_counts_t *counts = (projection_counts_t *)ctx;
  if (counts->gate && !counts->gate->block_destroy) gate_enter(counts->gate);
  if (counts->clone_mode == CLONE_NULL) return SALTS_OK;
  if (counts->clone_mode == CLONE_ALIAS || counts->clone_mode == CLONE_ERROR_ALIAS) {
    *out = (void *)value;
    return counts->clone_mode == CLONE_ALIAS ? SALTS_OK : SALTS_EIO;
  }
  *out = new_value();
  if (!*out) return SALTS_ENOMEM;
  *(int *)*out = *(const int *)value;
  return counts->clone_mode == CLONE_ERROR_VALUE ? SALTS_EIO : SALTS_OK;
}
static void destroy_value(void *value, void *ctx) {
  projection_counts_t *counts = (projection_counts_t *)ctx;
  if (counts->gate && counts->gate->block_destroy) gate_enter(counts->gate);
  if (counts->gate) salts_mutex_lock(&counts->gate->mutex);
  ++counts->payloads;
  if (counts->gate) salts_mutex_unlock(&counts->gate->mutex);
  free(value);
}
static int release_context(void *ctx) {
  ++((projection_counts_t *)ctx)->contexts;
  if (((projection_counts_t *)ctx)->release_error) return SALTS_EIO;
  return SALTS_OK;
}
static turbo_flow_projection_owner_config_t config_for(projection_counts_t *counts) {
  turbo_flow_projection_owner_config_t config = TURBO_FLOW_PROJECTION_OWNER_CONFIG_INIT;
  config.flags = TURBO_FLOW_PROJECTION_IMMUTABLE | TURBO_FLOW_PROJECTION_CROSS_THREAD |
                 TURBO_FLOW_PROJECTION_INDEPENDENT_CONTEXT;
  config.capacity = TEST_CAPACITY;
  config.max_result_bytes = sizeof(int);
  config.max_retained_bytes = TEST_CAPACITY * sizeof(int);
  config.schema = &schema;
  config.clone = clone_value;
  config.destroy = destroy_value;
  config.ctx = counts;
  config.release_context = release_context;
  return config;
}

typedef struct projection_worker_s {
  turbo_flow_msg_t *source;
  int cleanup_only;
  int rc;
  int observed;
} projection_worker_t;
static void projection_worker(void *arg) {
  projection_worker_t *worker = (projection_worker_t *)arg;
  turbo_flow_msg_t copy;
  if (worker->cleanup_only) {
    turbo_flow_msg_cleanup(worker->source);
    worker->rc = SALTS_OK;
    return;
  }
  worker->rc = turbo_flow_msg_clone(&copy, worker->source);
  if (worker->rc == SALTS_OK) {
    worker->observed = *(const int *)turbo_flow_msg_projection(&copy, NULL);
    turbo_flow_msg_cleanup(&copy);
  }
}

spec("retained projection owner") {
  it("copies config and schema wrappers and preserves failed bind inputs") {
    projection_counts_t counts = {0};
    struct { turbo_flow_data_schema_t schema; int extension; } caller = {schema, TEST_VALUE};
    turbo_flow_projection_owner_config_t config = config_for(&counts);
    turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    turbo_flow_projection_owner_t *owner = NULL;
    const turbo_flow_data_schema_t *actual = NULL;
    turbo_flow_content_descriptor_t descriptor;
    turbo_flow_msg_t src, dst;
    int *value = new_value();
    int *rejected = new_value();
    check_not_null(value);
    check_not_null(rejected);
    caller.schema.size = sizeof(caller);
    config.schema = &caller.schema;
    check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
    caller.schema.schema_name = "changed";
    config.clone = NULL;
    config.destroy = NULL;
    config.ctx = NULL;
    turbo_flow_msg_init(&src);
    turbo_flow_msg_init(&dst);
    check_equal(turbo_flow_msg_bind_retained_projection(&src, owner, value), SALTS_OK);
    check_equal(*(const int *)turbo_flow_msg_projection(&src, &actual), TEST_VALUE);
    check_equal(actual->schema_name, "retained");
    check_equal(actual->size, sizeof(turbo_flow_data_schema_t));
    check_not_equal((const void *)actual, (const void *)&caller.schema);
    check_equal(turbo_flow_msg_bind_retained_projection(&src, owner, rejected), SALTS_EBUSY);
    check_equal(*rejected, TEST_VALUE);
    check_equal(turbo_flow_content_descriptor_init(&descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_JSON,
                "application/json", "retained"), SALTS_OK);
    descriptor.flags |= TURBO_FLOW_CONTENT_SCHEMA_DECLARED;
    memcpy(descriptor.schema_name, "other", sizeof("other"));
    memcpy(descriptor.type_name, "Integer", sizeof("Integer"));
    descriptor.schema_version = 1;
    check_equal(turbo_flow_msg_set_content_descriptor(&dst, &descriptor), SALTS_OK);
    check_equal(turbo_flow_msg_bind_retained_projection(&dst, owner, rejected), SALTS_EPROTO);
    check_equal((const void *)turbo_flow_msg_content_descriptor(&dst), (const void *)&descriptor);
    check_null(turbo_flow_msg_projection(&dst, NULL));
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)1);
    check_equal(state.retained_bytes, sizeof(int));
    turbo_flow_msg_cleanup(&dst);
    check_equal(turbo_flow_msg_clone(&dst, &src), SALTS_OK);
    check_equal(*(const int *)turbo_flow_msg_projection(&dst, NULL), TEST_VALUE);
    free(rejected);
    turbo_flow_msg_cleanup(&src);
    turbo_flow_msg_cleanup(&dst);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(counts.payloads, 2);
    check_equal(counts.contexts, 1);
  }

  it("runs the public C++ lifecycle example linked only to Graph") {
    check_equal(flow_projection_cpp_example(), SALTS_OK);
  }
  it("validates snapshot ABI without changing caller storage") {
    projection_counts_t counts = {0};
    turbo_flow_projection_owner_config_t config = config_for(&counts);
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
    state.size = 0;
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_EINVAL);
    check_equal(state.size, (size_t)0);
    state = (turbo_flow_projection_owner_snapshot_t)TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    ++state.abi_major;
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_EINVAL);
    state = (turbo_flow_projection_owner_snapshot_t)TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    ++state.abi_minor;
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_EINVAL);
    check_equal(turbo_flow_projection_owner_snapshot(NULL, &state), SALTS_EINVAL);
    check_equal(turbo_flow_projection_owner_snapshot(owner, NULL), SALTS_EINVAL);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
  }

  it("counts concurrent clone callbacks as busy through stop") {
    enum { WORKERS = 2, CONCURRENT_CAPACITY = 4 };
    callback_gate_t gate = {0};
    projection_counts_t counts = {0};
    turbo_flow_projection_owner_config_t config = config_for(&counts);
    turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_msg_t sources[WORKERS];
    salts_thread_t threads[WORKERS] = {0};
    projection_worker_t workers[WORKERS] = {0};
    salts_mutex_init(&gate.mutex);
    salts_cond_init(&gate.cond);
    check_not_null(gate.mutex);
    check_not_null(gate.cond);
    counts.gate = &gate;
    config.capacity = CONCURRENT_CAPACITY;
    config.max_retained_bytes = CONCURRENT_CAPACITY * sizeof(int);
    check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
    for (int i = 0; i < WORKERS; ++i) {
      int *value = new_value();
      check_not_null(value);
      turbo_flow_msg_init(&sources[i]);
      check_equal(turbo_flow_msg_bind_retained_projection(&sources[i], owner, value), SALTS_OK);
      workers[i].source = &sources[i];
      check_equal(salts_thread_create(&threads[i], projection_worker, &workers[i]), 0);
    }
    salts_mutex_lock(&gate.mutex);
    while (gate.entered != WORKERS) salts_cond_wait(&gate.cond, &gate.mutex);
    salts_mutex_unlock(&gate.mutex);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)CONCURRENT_CAPACITY);
    check_equal(state.retained_bytes, CONCURRENT_CAPACITY * sizeof(int));
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_EBUSY);
    check_equal(counts.contexts, 0);
    salts_mutex_lock(&gate.mutex);
    gate.released = 1;
    salts_cond_broadcast(&gate.cond);
    salts_mutex_unlock(&gate.mutex);
    for (int i = 0; i < WORKERS; ++i) {
      check_equal(salts_thread_join(&threads[i]), 0);
      salts_thread_destroy(&threads[i]);
      check_equal(workers[i].rc, SALTS_OK);
      check_equal(workers[i].observed, TEST_VALUE);
      turbo_flow_msg_cleanup(&sources[i]);
    }
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)0);
    check_equal(state.peak_outstanding, (size_t)CONCURRENT_CAPACITY);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(counts.payloads, CONCURRENT_CAPACITY);
    check_equal(counts.contexts, 1);
    salts_cond_destroy(&gate.cond);
    salts_mutex_destroy(&gate.mutex);
  }

  it("keeps the final lease busy until payload destroy returns") {
    callback_gate_t gate = {0};
    projection_counts_t counts = {0};
    turbo_flow_projection_owner_config_t config = config_for(&counts);
    turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_msg_t source;
    salts_thread_t thread = NULL;
    projection_worker_t worker = {0};
    int *value = new_value();
    check_not_null(value);
    salts_mutex_init(&gate.mutex);
    salts_cond_init(&gate.cond);
    check_not_null(gate.mutex);
    check_not_null(gate.cond);
    gate.block_destroy = 1;
    counts.gate = &gate;
    turbo_flow_msg_init(&source);
    check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
    check_equal(turbo_flow_msg_bind_retained_projection(&source, owner, value), SALTS_OK);
    worker.source = &source;
    worker.cleanup_only = 1;
    check_equal(salts_thread_create(&thread, projection_worker, &worker), 0);
    salts_mutex_lock(&gate.mutex);
    while (!gate.entered) salts_cond_wait(&gate.cond, &gate.mutex);
    salts_mutex_unlock(&gate.mutex);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)1);
    check_equal(state.retained_bytes, sizeof(int));
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_EBUSY);
    check_equal(counts.contexts, 0);
    salts_mutex_lock(&gate.mutex);
    gate.released = 1;
    salts_cond_signal(&gate.cond);
    salts_mutex_unlock(&gate.mutex);
    check_equal(salts_thread_join(&thread), 0);
    salts_thread_destroy(&thread);
    check_equal(worker.rc, SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(counts.payloads, 1);
    check_equal(counts.contexts, 1);
    salts_cond_destroy(&gate.cond);
    salts_mutex_destroy(&gate.mutex);
  }

  it("rejects malformed configs without consuming context") {
    enum { BAD_SIZE, BAD_MAJOR, BAD_MINOR, BAD_FLAGS, UNKNOWN_FLAGS, NO_SCHEMA, SMALL_SCHEMA,
           NO_DOMAIN, INVALID_DOMAIN, NO_NAME, EMPTY_TYPE, NO_PROJECTION_TYPE, NO_VERSION, BAD_ENCODING,
           NO_CAPACITY, NO_BYTES, NO_TOTAL, SMALL_TOTAL, PAYLOAD_OVERFLOW, WRAPPER_OVERFLOW,
           NO_DESTROY, NO_RELEASE, INVALID_COUNT };
    projection_counts_t counts = {0};
    for (int invalid = 0; invalid < INVALID_COUNT; ++invalid) {
      turbo_flow_data_schema_t bad_schema = schema;
      turbo_flow_projection_owner_config_t config = config_for(&counts);
      turbo_flow_projection_owner_t *owner = NULL;
      int rc;
      int expected = SALTS_EINVAL;
      config.schema = &bad_schema;
      switch (invalid) {
        case BAD_SIZE: config.size = 0; break;
        case BAD_MAJOR: ++config.abi_major; break;
        case BAD_MINOR: ++config.abi_minor; break;
        case BAD_FLAGS: config.flags = 0; expected = SALTS_ENOTSUP; break;
        case UNKNOWN_FLAGS: config.flags |= 8u; expected = SALTS_ENOTSUP; break;
        case NO_SCHEMA: config.schema = NULL; break;
        case SMALL_SCHEMA: bad_schema.size = 0; break;
        case NO_DOMAIN: bad_schema.domain = TURBO_FLOW_DOMAIN_NONE; break;
        case INVALID_DOMAIN: bad_schema.domain = (turbo_flow_domain_t)-1; break;
        case NO_NAME: bad_schema.schema_name = NULL; break;
        case EMPTY_TYPE: bad_schema.type_name = ""; break;
        case NO_PROJECTION_TYPE: bad_schema.projection_type = NULL; break;
        case NO_VERSION: bad_schema.schema_version = 0; break;
        case BAD_ENCODING: bad_schema.encoding = (turbo_flow_data_encoding_t)-1; break;
        case NO_CAPACITY: config.capacity = 0; break;
        case NO_BYTES: config.max_result_bytes = 0; break;
        case NO_TOTAL: config.max_retained_bytes = 0; break;
        case SMALL_TOTAL: config.max_retained_bytes = 1; break;
        case PAYLOAD_OVERFLOW: config.capacity = SIZE_MAX; break;
        case WRAPPER_OVERFLOW:
          config.capacity = SIZE_MAX; config.max_result_bytes = 1; break;
        case NO_DESTROY: config.destroy = NULL; break;
        case NO_RELEASE: config.release_context = NULL; break;
      }
      rc = turbo_flow_projection_owner_create(&config, &owner);
      check_equal(rc, expected);
      check_null(owner);
      check_equal(counts.contexts, 0);
    }
  }

  it("bounds count and bytes and closes admission after stop") {
    for (int byte_limit = 0; byte_limit <= 1; ++byte_limit) {
      projection_counts_t counts = {0};
      turbo_flow_projection_owner_config_t config = config_for(&counts);
      turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
      turbo_flow_projection_owner_t *owner = NULL;
      turbo_flow_msg_t src, dst;
      int *value = new_value();
      int *rejected = new_value();
      check_not_null(value);
      check_not_null(rejected);
      if (byte_limit) config.max_retained_bytes = sizeof(int);
      else config.capacity = 1;
      turbo_flow_msg_init(&src);
      turbo_flow_msg_init(&dst);
      check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
      check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_EBUSY);
      check_equal(turbo_flow_msg_bind_retained_projection(&src, owner, value), SALTS_OK);
      check_equal(turbo_flow_msg_clone(&dst, &src), SALTS_ENOSPC);
      check_null(turbo_flow_msg_projection(&dst, NULL));
      check_equal(turbo_flow_msg_bind_retained_projection(&dst, owner, rejected), SALTS_ENOSPC);
      check_equal(*rejected, TEST_VALUE);
      check_equal(*(const int *)turbo_flow_msg_projection(&src, NULL), TEST_VALUE);
      check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
      check_equal(state.outstanding, (size_t)1);
      check_equal(state.retained_bytes, sizeof(int));
      check_equal(state.peak_outstanding, (size_t)1);
      check_equal(state.peak_retained_bytes, sizeof(int));
      check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
      check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
      check_equal(turbo_flow_msg_clone(&dst, &src), SALTS_ECANCELED);
      check_equal(turbo_flow_msg_bind_retained_projection(&dst, owner, rejected), SALTS_ECANCELED);
      check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_EBUSY);
      turbo_flow_msg_cleanup(&src);
      free(rejected);
      check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
      check_equal(state.accepting, 0);
      check_equal(state.outstanding, (size_t)0);
      check_equal(state.retained_bytes, (size_t)0);
      check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    }
  }

  it("rolls back failed and malformed clone results without destroying aliases") {
    for (int mode = CLONE_ERROR_VALUE; mode <= CLONE_ERROR_ALIAS; ++mode) {
      projection_counts_t counts = {0};
      turbo_flow_projection_owner_config_t config = config_for(&counts);
      turbo_flow_projection_owner_snapshot_t state = TURBO_FLOW_PROJECTION_OWNER_SNAPSHOT_INIT;
      turbo_flow_projection_owner_t *owner = NULL;
      turbo_flow_msg_t src, dst;
      int *value = new_value();
      check_not_null(value);
      counts.clone_mode = mode;
      turbo_flow_msg_init(&src);
      check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
      check_equal(turbo_flow_msg_bind_retained_projection(&src, owner, value), SALTS_OK);
      check_equal(turbo_flow_msg_clone(&dst, &src),
                  mode == CLONE_ERROR_VALUE || mode == CLONE_ERROR_ALIAS ? SALTS_EIO
                                                                         : SALTS_EPROTO);
      check_null(turbo_flow_msg_projection(&dst, NULL));
      check_equal(counts.payloads, mode == CLONE_ERROR_VALUE ? 1 : 0);
      check_equal(*(const int *)turbo_flow_msg_projection(&src, NULL), TEST_VALUE);
      check_equal(turbo_flow_projection_owner_snapshot(owner, &state), SALTS_OK);
      check_equal(state.outstanding, (size_t)1);
      check_equal(state.retained_bytes, sizeof(int));
      check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
      turbo_flow_msg_cleanup(&src);
      check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    }
  }

  it("supports no-clone owners and retryable control-thread context release") {
    projection_counts_t counts = {0};
    turbo_flow_projection_owner_config_t config = config_for(&counts);
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_msg_t src, dst;
    int *value = new_value();
    check_not_null(value);
    config.clone = NULL;
    turbo_flow_msg_init(&src);
    check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
    check_equal(turbo_flow_msg_bind_retained_projection(&src, owner, value), SALTS_OK);
    check_equal(turbo_flow_msg_clone(&dst, &src), SALTS_ENOTSUP);
    check_null(turbo_flow_msg_projection(&dst, NULL));
    turbo_flow_msg_cleanup(&src);
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    counts.release_error = 1;
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_EIO);
    check_equal(counts.contexts, 1);
    counts.release_error = 0;
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(counts.contexts, 2);
    check_equal(counts.payloads, 1);
  }

  it("moves ownership and clears projection before descriptor-only retain and clone") {
    projection_counts_t counts = {0};
    turbo_flow_projection_owner_config_t config = config_for(&counts);
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_content_descriptor_t descriptor;
    turbo_flow_msg_t src, moved, view, copy;
    int *value = new_value();
    check_not_null(value);
    turbo_flow_msg_init(&src);
    check_equal(turbo_flow_content_descriptor_init(&descriptor, TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
                TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA, TURBO_FLOW_DATA_ENCODING_JSON,
                "application/json", "retained"), SALTS_OK);
    check_equal(turbo_flow_msg_copy_content_descriptor(&src, &descriptor), SALTS_OK);
    check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
    check_equal(turbo_flow_msg_bind_retained_projection(&src, owner, value), SALTS_OK);
    check_equal(turbo_flow_msg_move(&moved, &src), SALTS_OK);
    check_null(turbo_flow_msg_projection(&src, NULL));
    turbo_flow_msg_clear_projection(&moved);
    check_equal(counts.payloads, 1);
    check_not_null(turbo_flow_msg_content_descriptor(&moved));
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(turbo_flow_msg_retain_view(&view, &moved), SALTS_OK);
    check_equal(turbo_flow_msg_clone(&copy, &moved), SALTS_OK);
    turbo_flow_msg_cleanup(&moved);
    check_equal(turbo_flow_msg_content_descriptor(&view)->identity, "retained");
    check_equal(turbo_flow_msg_content_descriptor(&copy)->identity, "retained");
    turbo_flow_msg_cleanup(&view);
    turbo_flow_msg_cleanup(&copy);
    turbo_flow_msg_cleanup(&src);
    check_equal(counts.payloads, 1);
  }

  it("clear_content returns the lease while preserving payload bytes") {
    projection_counts_t counts = {0};
    turbo_flow_projection_owner_config_t config = config_for(&counts);
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_msg_t src;
    int *value = new_value();
    check_not_null(value);
    turbo_flow_msg_init(&src);
    src.owned_payload = tstr_dup("original");
    check_not_null(src.owned_payload);
    src.payload = tstr_to_v(src.owned_payload);
    check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
    check_equal(turbo_flow_msg_bind_retained_projection(&src, owner, value), SALTS_OK);
    turbo_flow_msg_clear_content(&src);
    check_null(turbo_flow_msg_projection(&src, NULL));
    check_equal(src.payload.data, "original");
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    turbo_flow_msg_cleanup(&src);
    check_equal(counts.payloads, 1);
  }

  it("keeps independent clone alive until both payloads finish") {
    projection_counts_t counts = {0};
    turbo_flow_projection_owner_config_t config = config_for(&counts);
    turbo_flow_projection_owner_t *owner = NULL;
    turbo_flow_msg_t src, dst, rejected;
    int *value = new_value();
    check_not_null(value);
    turbo_flow_msg_init(&src);
    check_equal(turbo_flow_projection_owner_create(&config, &owner), SALTS_OK);
    check_equal(turbo_flow_msg_bind_retained_projection(&src, owner, value), SALTS_OK);
    check_equal(turbo_flow_msg_clone(&dst, &src), SALTS_OK);
    check_equal(turbo_flow_msg_clone(&rejected, &src), SALTS_ENOSPC);
    check_null(turbo_flow_msg_projection(&rejected, NULL));
    check_equal(turbo_flow_projection_owner_stop(owner), SALTS_OK);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_EBUSY);
    turbo_flow_msg_cleanup(&src);
    check_equal(*(const int *)turbo_flow_msg_projection(&dst, NULL), TEST_VALUE);
    turbo_flow_msg_cleanup(&dst);
    check_equal(turbo_flow_projection_owner_destroy(owner), SALTS_OK);
    check_equal(counts.payloads, 2);
    check_equal(counts.contexts, 1);
  }
}
