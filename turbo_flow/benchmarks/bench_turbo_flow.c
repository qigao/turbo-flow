#include "platform.h"
#include "flow_expr_internal.h"
#include "flow_internal.h"
#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_expr.h"
#include "turbo_flow_security.h"
#include "turbo_thread.h"

#include <inttypes.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
  #include <windows.h>
#endif

#define FLOW_BENCH_COMPILE_ITERS 1000
#define FLOW_BENCH_PUBLISH_ITERS 100000
#define FLOW_BENCH_EXECUTOR_ITERS 10000
#define FLOW_BENCH_WARMUP_ITERS 1000
#define FLOW_BENCH_TEARDOWN_ITERS 100
#define FLOW_BENCH_IDLE_WAIT_MS 500u
#define FLOW_BENCH_EXPR_COMPILE_ITERS 200
#define FLOW_BENCH_EXPR_EVAL_ITERS 100000
#define FLOW_BENCH_ASYNC_INGRESS_ITERS 10000
#define FLOW_BENCH_SECURITY_EVAL_ITERS 100000
#define FLOW_BENCH_SECURITY_CANDIDATE_ITERS 10000

static atomic_size_t g_flow_bench_count = 0;

static void bench_security_copy(char *output, size_t capacity, const char *value) {
  size_t size = strlen(value);
  check_true(size < capacity);
  memcpy(output, value, size + 1u);
}

typedef enum bench_security_layout_e {
  BENCH_SECURITY_DISTINCT_EXACT = 0,
  BENCH_SECURITY_SHARED_EXACT,
  BENCH_SECURITY_SHARED_PREFIX,
  BENCH_SECURITY_SHARED_ADAPTER
} bench_security_layout_t;

typedef struct bench_security_adapter_entry_s {
  const char *pattern;
  size_t candidate_position;
} bench_security_adapter_entry_t;

typedef struct bench_security_adapter_leaf_s {
  bench_security_adapter_entry_t *entries;
  size_t entry_count;
} bench_security_adapter_leaf_t;

static int bench_security_adapter_compare(const void *left, const void *right) {
  const bench_security_adapter_entry_t *a = (const bench_security_adapter_entry_t *)left;
  const bench_security_adapter_entry_t *b = (const bench_security_adapter_entry_t *)right;
  return strcmp(a->pattern, b->pattern);
}

static int bench_security_adapter_compile(void *ctx,
                                          const turbo_flow_security_matcher_leaf_t *input,
                                          void **compiled_leaf_out) {
  bench_security_adapter_leaf_t *leaf;
  (void)ctx;
  if (compiled_leaf_out) *compiled_leaf_out = NULL;
  if (!input || input->size < sizeof(*input) || !input->rules || !input->candidate_rule_indices ||
      input->candidate_count == 0u || !compiled_leaf_out)
    return TURBO_EINVAL;
  leaf = (bench_security_adapter_leaf_t *)calloc(1u, sizeof(*leaf));
  if (!leaf) return TURBO_ENOMEM;
  leaf->entries =
      (bench_security_adapter_entry_t *)calloc(input->candidate_count, sizeof(*leaf->entries));
  if (!leaf->entries) {
    free(leaf);
    return TURBO_ENOMEM;
  }
  leaf->entry_count = input->candidate_count;
  for (size_t i = 0u; i < input->candidate_count; ++i) {
    size_t rule_index = input->candidate_rule_indices[i];
    if (rule_index >= input->rule_count) {
      free(leaf->entries);
      free(leaf);
      return TURBO_EPROTO;
    }
    leaf->entries[i].pattern = input->rules[rule_index].pattern;
    leaf->entries[i].candidate_position = i;
  }
  qsort(leaf->entries, leaf->entry_count, sizeof(*leaf->entries), bench_security_adapter_compare);
  *compiled_leaf_out = leaf;
  return TURBO_OK;
}

static int bench_security_adapter_evaluate(void *ctx, const void *compiled_leaf,
                                           const turbo_flow_security_request_t *request,
                                           turbo_flow_security_match_emit_fn emit, void *emit_ctx) {
  const bench_security_adapter_leaf_t *leaf = (const bench_security_adapter_leaf_t *)compiled_leaf;
  size_t first = 0u;
  size_t count;
  (void)ctx;
  if (!leaf || !request || !request->resource || !emit) return TURBO_EINVAL;
  count = leaf->entry_count;
  while (first < count) {
    size_t middle = first + (count - first) / 2u;
    if (strcmp(leaf->entries[middle].pattern, request->resource) < 0) first = middle + 1u;
    else count = middle;
  }
  while (first < leaf->entry_count &&
         strcmp(leaf->entries[first].pattern, request->resource) == 0) {
    int rc = emit(emit_ctx, leaf->entries[first].candidate_position);
    if (rc != TURBO_OK) return rc;
    ++first;
  }
  return TURBO_OK;
}

static void bench_security_adapter_destroy(void *ctx, void *compiled_leaf) {
  bench_security_adapter_leaf_t *leaf = (bench_security_adapter_leaf_t *)compiled_leaf;
  (void)ctx;
  if (!leaf) return;
  free(leaf->entries);
  free(leaf);
}

static const char *bench_security_layout_name(bench_security_layout_t layout) {
  static const char *const names[] = {"acl-subject-index", "acl-exact-index", "acl-prefix-index",
                                      "acl-adapter-compiled"};
  return layout <= BENCH_SECURITY_SHARED_ADAPTER ? names[layout] : "acl-invalid";
}

static turbo_flow_security_realm_t *bench_security_realm(size_t rule_count,
                                                         bench_security_layout_t layout,
                                                         turbo_flow_security_rule_t **rules_out) {
  turbo_flow_security_realm_config_t config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
  turbo_flow_security_rule_t *rules;
  turbo_flow_security_realm_t *realm = NULL;
  char subject[32];
  char pattern[64];
  check_true(rule_count > 0u && rule_count <= TURBO_FLOW_SECURITY_MAX_RULES);
  rules = (turbo_flow_security_rule_t *)calloc(rule_count, sizeof(*rules));
  check_not_null(rules);
  for (size_t i = 0u; i < rule_count; ++i) {
    int written;
    rules[i] = (turbo_flow_security_rule_t)TURBO_FLOW_SECURITY_RULE_INIT;
    rules[i].effect = TURBO_FLOW_SECURITY_DENY;
    rules[i].subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
    if (layout != BENCH_SECURITY_DISTINCT_EXACT) {
      bench_security_copy(rules[i].subject, sizeof(rules[i].subject), "writer");
    } else {
      written = snprintf(subject, sizeof(subject), "subject-%llu", (unsigned long long)i);
      check_true(written > 0 && (size_t)written < sizeof(subject));
      bench_security_copy(rules[i].subject, sizeof(rules[i].subject), subject);
    }
    bench_security_copy(rules[i].root_group_id, sizeof(rules[i].root_group_id), "root-0");
    rules[i].action_mask = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    rules[i].resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    rules[i].match_kind = layout == BENCH_SECURITY_SHARED_PREFIX ? TURBO_FLOW_SECURITY_MATCH_PREFIX
                          : layout == BENCH_SECURITY_SHARED_ADAPTER
                              ? TURBO_FLOW_SECURITY_MATCH_ADAPTER
                              : TURBO_FLOW_SECURITY_MATCH_EXACT;
    written = snprintf(pattern, sizeof(pattern),
                       layout == BENCH_SECURITY_SHARED_PREFIX ? "resource/%llu/" : "resource/%llu",
                       (unsigned long long)i);
    check_true(written > 0 && (size_t)written < sizeof(pattern));
    bench_security_copy(rules[i].pattern, sizeof(rules[i].pattern), pattern);
  }
  rules[rule_count - 1u].effect = TURBO_FLOW_SECURITY_ALLOW;
  bench_security_copy(rules[rule_count - 1u].subject, sizeof(rules[rule_count - 1u].subject),
                      "writer");
  bench_security_copy(rules[rule_count - 1u].root_group_id,
                      sizeof(rules[rule_count - 1u].root_group_id), "root-0");
  rules[rule_count - 1u].action_mask = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
  rules[rule_count - 1u].resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
  bench_security_copy(rules[rule_count - 1u].pattern, sizeof(rules[rule_count - 1u].pattern),
                      layout == BENCH_SECURITY_SHARED_PREFIX ? "target/" : "target/topic");
  config.resource_uid = "security:benchmark";
  config.owner_name = "security.benchmark";
  config.policy_version = 1u;
  config.rules = rules;
  config.rule_count = rule_count;
  if (layout == BENCH_SECURITY_SHARED_ADAPTER) {
    config.matcher = (turbo_flow_security_matcher_t)TURBO_FLOW_SECURITY_MATCHER_INIT;
    config.matcher.compile_leaf = bench_security_adapter_compile;
    config.matcher.evaluate_leaf = bench_security_adapter_evaluate;
    config.matcher.destroy_leaf = bench_security_adapter_destroy;
  }
  check_int_eq(turbo_flow_security_realm_create(&config, &realm), TURBO_OK);
  check_not_null(realm);
  *rules_out = rules;
  return realm;
}

typedef struct flow_bench_live_jobs_s {
  atomic_int release;
  atomic_uint started;
  atomic_uint completed;
} flow_bench_live_jobs_t;

typedef struct flow_bench_publish_worker_s {
  turbo_flow_t *flow;
  size_t payload_size;
  size_t iterations;
  uint64_t *latencies;
  atomic_uint *ready;
  atomic_int *start;
  size_t completed;
  int status;
} flow_bench_publish_worker_t;

typedef struct flow_bench_emitter_s {
  uint32_t output_count;
  size_t sink_count;
} flow_bench_emitter_t;

typedef struct flow_bench_keyed_s {
  turbo_flow_keyed_state_store_t *store;
  size_t sink_count;
  uint64_t window_size;
} flow_bench_keyed_t;

typedef struct flow_bench_async_completion_s {
  atomic_size_t completed;
  atomic_int status;
} flow_bench_async_completion_t;

static void bench_async_publish_complete(void *ctx, const turbo_flow_publish_result_t *result) {
  flow_bench_async_completion_t *completion = (flow_bench_async_completion_t *)ctx;
  if (!result || result->status != TURBO_OK) {
    atomic_store_explicit(&completion->status, result ? result->status : TURBO_EINVAL,
                          memory_order_release);
  }
  atomic_fetch_add_explicit(&completion->completed, 1u, memory_order_acq_rel);
}

static void bench_live_job(void *arg) {
  flow_bench_live_jobs_t *jobs = (flow_bench_live_jobs_t *)arg;
  atomic_fetch_add_explicit(&jobs->started, 1u, memory_order_release);
  while (!atomic_load_explicit(&jobs->release, memory_order_acquire))
    turbo_sleep_ms(1);
  atomic_fetch_add_explicit(&jobs->completed, 1u, memory_order_release);
}

static void bench_release_live_jobs(void *arg) {
  flow_bench_live_jobs_t *jobs = (flow_bench_live_jobs_t *)arg;
  turbo_sleep_ms(1);
  atomic_store_explicit(&jobs->release, 1, memory_order_release);
}

static const flow_threadpool_adapter_t *bench_threadpool_adapter(const turbo_flow_t *flow,
                                                                 uint32_t stage_index) {
  for (size_t i = 0; i < turbo_vec_size(&flow->threadpool_adapters); ++i) {
    const flow_threadpool_adapter_t *adapter =
        (const flow_threadpool_adapter_t *)turbo_vec_at_const(&flow->threadpool_adapters, i);
    if (adapter && adapter->stage_index == stage_index) return adapter;
  }
  return NULL;
}

static int bench_stage(turbo_flow_msg_t *msg, void *ctx) {
  atomic_size_t *count = (atomic_size_t *)ctx;
  (void)msg;
  atomic_fetch_add_explicit(count, 1u, memory_order_relaxed);
  return TURBO_OK;
}

static int bench_emitting_stage(const turbo_flow_msg_t *input, turbo_flow_emitter_t *emitter,
                                void *ctx) {
  flow_bench_emitter_t *bench = (flow_bench_emitter_t *)ctx;
  for (uint32_t i = 0u; i < bench->output_count; ++i) {
    turbo_flow_msg_t output;
    int rc;
    turbo_flow_msg_init(&output);
    output.id = input->id + i;
    rc = turbo_flow_emitter_emit_move(emitter, &output);
    turbo_flow_msg_cleanup(&output);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int bench_emitter_sink(turbo_flow_msg_t *msg, void *ctx) {
  flow_bench_emitter_t *bench = (flow_bench_emitter_t *)ctx;
  (void)msg;
  bench->sink_count += 1u;
  return TURBO_OK;
}

static int bench_keyed_select(const turbo_flow_msg_t *msg, tstr_v *key, void *ctx) {
  (void)ctx;
  *key = tstr_v_from_buf((const char *)&msg->id, sizeof(msg->id));
  return TURBO_OK;
}

static int bench_keyed_increment(turbo_flow_msg_t *msg, turbo_flow_keyed_state_t *state,
                                 void *ctx) {
  tstr_v value;
  uint64_t count = 0u;
  int rc = turbo_flow_keyed_state_get(state, &value, NULL);
  (void)ctx;
  if (rc == TURBO_OK) {
    if (value.len != sizeof(count)) return TURBO_EPROTO;
    memcpy(&count, value.data, sizeof(count));
  } else if (rc != TURBO_ENOENT) {
    return rc;
  }
  count += 1u;
  msg->type = (uint32_t)count;
  return turbo_flow_keyed_state_put(state, tstr_v_from_buf((const char *)&count, sizeof(count)));
}

static int bench_keyed_baseline_stage(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return TURBO_OK;
}

static int bench_keyed_window(const turbo_flow_msg_t *msg, turbo_flow_keyed_state_t *state,
                              turbo_flow_emitter_t *emitter, void *ctx) {
  flow_bench_keyed_t *bench = (flow_bench_keyed_t *)ctx;
  tstr_v value;
  uint64_t count = 0u;
  int rc = turbo_flow_keyed_state_get(state, &value, NULL);

  if (rc == TURBO_OK) {
    if (value.len != sizeof(count)) return TURBO_EPROTO;
    memcpy(&count, value.data, sizeof(count));
  } else if (rc != TURBO_ENOENT) {
    return rc;
  }
  count += 1u;
  if (count < bench->window_size) {
    return turbo_flow_keyed_state_put(state, tstr_v_from_buf((const char *)&count, sizeof(count)));
  }
  rc = turbo_flow_keyed_state_delete(state);
  if (rc != TURBO_OK) return rc;
  {
    turbo_flow_msg_t output;
    turbo_flow_msg_init(&output);
    output.id = msg->id;
    output.type = (uint32_t)count;
    rc = turbo_flow_emitter_emit_move(emitter, &output);
    turbo_flow_msg_cleanup(&output);
  }
  return rc;
}

static int bench_keyed_sink(turbo_flow_msg_t *msg, void *ctx) {
  flow_bench_keyed_t *bench = (flow_bench_keyed_t *)ctx;
  (void)msg;
  bench->sink_count += 1u;
  return TURBO_OK;
}

static int bench_event_window_accumulate(const turbo_flow_msg_t *msg,
                                         const turbo_flow_event_time_window_t *window,
                                         turbo_flow_keyed_state_t *state, void *ctx) {
  uint64_t count = 0u;
  (void)msg;
  (void)ctx;
  if (window->aggregate.len > 0u) {
    if (window->aggregate.len != sizeof(count)) return TURBO_EPROTO;
    memcpy(&count, window->aggregate.data, sizeof(count));
  }
  count += 1u;
  return turbo_flow_keyed_state_put(state, tstr_v_from_buf((const char *)&count, sizeof(count)));
}

static int bench_event_window_close(const turbo_flow_event_time_window_t *window,
                                    turbo_flow_emitter_t *emitter, void *ctx) {
  turbo_flow_msg_t output;
  uint64_t key;
  uint64_t count;
  int rc;
  (void)ctx;
  if (window->key.len != sizeof(key) || window->aggregate.len != sizeof(count)) {
    return TURBO_EPROTO;
  }
  memcpy(&key, window->key.data, sizeof(key));
  memcpy(&count, window->aggregate.data, sizeof(count));
  turbo_flow_msg_init(&output);
  output.id = key;
  output.ts_ns = window->start_ns;
  output.type = (uint32_t)count;
  rc = turbo_flow_emitter_emit_move(emitter, &output);
  turbo_flow_msg_cleanup(&output);
  return rc;
}

static void bench_register_stage(turbo_flow_t *flow, const char *name) {
  check_int_eq(
      turbo_flow_register_stage_ex(flow, name, bench_stage, (void *)&g_flow_bench_count, NULL),
      TURBO_OK);
}

static int bench_u64_compare(const void *lhs, const void *rhs) {
  uint64_t left = *(const uint64_t *)lhs;
  uint64_t right = *(const uint64_t *)rhs;
  return left < right ? -1 : left > right ? 1 : 0;
}

static uint64_t bench_percentile(const uint64_t *sorted, size_t count, size_t percent) {
  size_t index;
  if (!sorted || count == 0 || percent > 100) return 0;
  index = ((count - 1u) * percent + 99u) / 100u;
  return sorted[index];
}

static void bench_publish_worker(void *arg) {
  flow_bench_publish_worker_t *worker = (flow_bench_publish_worker_t *)arg;
  turbo_flow_msg_t msg;

  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_new_len(NULL, worker->payload_size);
  if (!msg.owned_payload) {
    worker->status = TURBO_ENOMEM;
    atomic_fetch_add_explicit(worker->ready, 1u, memory_order_release);
    return;
  }
  memset(msg.owned_payload, 'x', worker->payload_size);
  msg.payload = tstr_to_v(msg.owned_payload);
  worker->status = TURBO_OK;
  atomic_fetch_add_explicit(worker->ready, 1u, memory_order_release);
  while (!atomic_load_explicit(worker->start, memory_order_acquire))
    turbo_thread_yield();

  for (size_t i = 0; i < worker->iterations; ++i) {
    uint64_t begin = turbo_hrtime();
    int rc = turbo_flow_publish(worker->flow, "input", &msg);
    worker->latencies[i] = turbo_hrtime() - begin;
    if (rc != TURBO_OK) {
      worker->status = rc;
      break;
    }
    ++worker->completed;
  }
  turbo_flow_msg_cleanup(&msg);
}

static void bench_report_concurrent_publish(turbo_flow_t *flow, const char *stage_plan,
                                            const char *executor, uint32_t workers,
                                            uint32_t producers, size_t payload_size,
                                            size_t iterations) {
  flow_bench_publish_worker_t *contexts;
  turbo_thread_t *threads;
  uint64_t *latencies;
  atomic_uint ready;
  atomic_int start;
  uint64_t total_start;
  uint64_t total_elapsed;
  size_t completed = 0;
  double throughput;

  contexts = (flow_bench_publish_worker_t *)calloc(producers, sizeof(*contexts));
  threads = (turbo_thread_t *)calloc(producers, sizeof(*threads));
  latencies = (uint64_t *)calloc(iterations, sizeof(*latencies));
  check_not_null(contexts);
  check_not_null(threads);
  check_not_null(latencies);
  if (!contexts || !threads || !latencies) goto cleanup;
  atomic_init(&ready, 0u);
  atomic_init(&start, 0);

  for (uint32_t i = 0; i < producers; ++i) {
    size_t begin = (iterations * i) / producers;
    size_t end = (iterations * (i + 1u)) / producers;
    contexts[i].flow = flow;
    contexts[i].payload_size = payload_size;
    contexts[i].iterations = end - begin;
    contexts[i].latencies = latencies + begin;
    contexts[i].ready = &ready;
    contexts[i].start = &start;
    contexts[i].status = TURBO_EBUSY;
    check_int_eq(turbo_thread_create(&threads[i], bench_publish_worker, &contexts[i]), TURBO_OK);
  }
  while (atomic_load_explicit(&ready, memory_order_acquire) != producers)
    turbo_thread_yield();
  total_start = turbo_hrtime();
  atomic_store_explicit(&start, 1, memory_order_release);
  for (uint32_t i = 0; i < producers; ++i) {
    check_int_eq(turbo_thread_join(&threads[i]), TURBO_OK);
    check_int_eq(contexts[i].status, TURBO_OK);
    completed += contexts[i].completed;
  }
  total_elapsed = turbo_hrtime() - total_start;
  check_size_eq(completed, iterations);
  if (completed == 0u) goto cleanup;

  qsort(latencies, completed, sizeof(*latencies), bench_u64_compare);
  throughput = total_elapsed > 0 ? ((double)completed * 1000000000.0) / (double)total_elapsed : 0.0;
  printf("BENCH_RESULT stage_plan=%s executor=%s workers=%" PRIu32 " producers=%" PRIu32
         " payload_bytes=%zu iterations=%zu throughput_msg_s=%.2f"
         " p50_ns=%" PRIu64 " p95_ns=%" PRIu64 " p99_ns=%" PRIu64 "\n",
         stage_plan, executor, workers, producers, payload_size, completed, throughput,
         bench_percentile(latencies, completed, 50), bench_percentile(latencies, completed, 95),
         bench_percentile(latencies, completed, 99));

cleanup:
  free(latencies);
  free(threads);
  free(contexts);
}

static void bench_report_publish(turbo_flow_t *flow, const char *stage_plan, const char *executor,
                                 uint32_t workers, size_t payload_size, size_t iterations) {
  turbo_flow_msg_t msg;
  uint64_t *latencies;
  uint64_t total_start;
  uint64_t total_elapsed;
  size_t completed = 0;
  double throughput;
  int rc = TURBO_OK;

  latencies = (uint64_t *)calloc(iterations, sizeof(*latencies));
  check_not_null(latencies);
  if (!latencies) return;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_new_len(NULL, payload_size);
  check_not_null(msg.owned_payload);
  if (!msg.owned_payload) {
    free(latencies);
    return;
  }
  memset(msg.owned_payload, 'x', payload_size);
  msg.payload = tstr_to_v(msg.owned_payload);

  for (size_t i = 0; i < FLOW_BENCH_WARMUP_ITERS; ++i) {
    rc = turbo_flow_publish(flow, "input", &msg);
    if (rc != TURBO_OK) break;
  }
  check_int_eq(rc, TURBO_OK);

  total_start = turbo_hrtime();
  for (size_t i = 0; i < iterations && rc == TURBO_OK; ++i) {
    uint64_t start = turbo_hrtime();
    rc = turbo_flow_publish(flow, "input", &msg);
    latencies[i] = turbo_hrtime() - start;
    if (rc == TURBO_OK) completed += 1u;
  }
  total_elapsed = turbo_hrtime() - total_start;
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK || completed == 0) {
    turbo_flow_msg_cleanup(&msg);
    free(latencies);
    return;
  }
  qsort(latencies, completed, sizeof(*latencies), bench_u64_compare);
  throughput = total_elapsed > 0 ? ((double)completed * 1000000000.0) / (double)total_elapsed : 0.0;
  printf("BENCH_RESULT stage_plan=%s executor=%s workers=%" PRIu32
         " payload_bytes=%zu iterations=%zu throughput_msg_s=%.2f"
         " p50_ns=%" PRIu64 " p95_ns=%" PRIu64 " p99_ns=%" PRIu64 "\n",
         stage_plan, executor, workers, payload_size, completed, throughput,
         bench_percentile(latencies, completed, 50), bench_percentile(latencies, completed, 95),
         bench_percentile(latencies, completed, 99));

  turbo_flow_msg_cleanup(&msg);
  free(latencies);
}

static uint64_t bench_process_cpu_ns(void) {
#ifdef _WIN32
  FILETIME created;
  FILETIME exited;
  FILETIME kernel;
  FILETIME user;
  ULARGE_INTEGER kernel_ticks;
  ULARGE_INTEGER user_ticks;
  if (!GetProcessTimes(GetCurrentProcess(), &created, &exited, &kernel, &user)) return 0u;
  kernel_ticks.LowPart = kernel.dwLowDateTime;
  kernel_ticks.HighPart = kernel.dwHighDateTime;
  user_ticks.LowPart = user.dwLowDateTime;
  user_ticks.HighPart = user.dwHighDateTime;
  return (kernel_ticks.QuadPart + user_ticks.QuadPart) * UINT64_C(100);
#else
  struct timespec value;
  if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &value) != 0) return 0u;
  return (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
#endif
}

static void bench_report_idle_cpu(turbo_flow_t *flow, const char *stage_plan, const char *executor,
                                  uint32_t workers) {
  uint64_t wall_start = turbo_hrtime();
  uint64_t cpu_start = bench_process_cpu_ns();
  double wall_ms;
  double cpu_ms;
  double cpu_wall_ratio;

  turbo_sleep_ms(FLOW_BENCH_IDLE_WAIT_MS);
  wall_ms = (double)(turbo_hrtime() - wall_start) / 1000000.0;
  cpu_ms = (double)(bench_process_cpu_ns() - cpu_start) / 1000000.0;
  cpu_wall_ratio = wall_ms > 0.0 ? cpu_ms / wall_ms : 0.0;
  printf("BENCH_IDLE stage_plan=%s executor=%s workers=%" PRIu32
         " wall_ms=%.2f cpu_ms=%.2f cpu_wall_ratio=%.4f\n",
         stage_plan, executor, workers, wall_ms, cpu_ms, cpu_wall_ratio);
  (void)flow;
}

static turbo_flow_t *bench_create_executor_flow(const char *exec_spec) {
  char src[256];
  turbo_flow_t *flow = turbo_flow_create();
  int written;
  check_not_null(flow);
  if (!flow) return NULL;
  written = snprintf(src, sizeof(src),
                     "source input\n"
                     "stage work %s\n"
                     "stage main {\n"
                     "  input -> work\n"
                     "}\n",
                     exec_spec ? exec_spec : "");
  check_true(written > 0 && (size_t)written < sizeof(src));
  check_int_eq(turbo_flow_parse_string(flow, src, (size_t)written), TURBO_OK);
  bench_register_stage(flow, "work");
  check_int_eq(turbo_flow_compile(flow), TURBO_OK);
  check_int_eq(turbo_flow_start(flow), TURBO_OK);
  return flow;
}

static turbo_flow_t *bench_create_started_flow(const char *src, const char *const *stage_names,
                                               size_t stage_count) {
  turbo_flow_t *flow = turbo_flow_create();
  check_not_null(flow);
  check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
  for (size_t i = 0; i < stage_count; ++i) {
    bench_register_stage(flow, stage_names[i]);
  }
  check_int_eq(turbo_flow_compile(flow), TURBO_OK);
  check_int_eq(turbo_flow_start(flow), TURBO_OK);
  return flow;
}

static turbo_flow_t *bench_create_emitting_flow(flow_bench_emitter_t *bench) {
  static const char *src = "source input operation data.input\n"
                           "stage expand operation data.expand\n"
                           "stage sink\n"
                           "stage main {\n"
                           "  input -> expand -> sink\n"
                           "}\n";
  turbo_flow_operation_descriptor_t input;
  turbo_flow_operation_descriptor_t expand;
  turbo_flow_emitting_operation_provider_registration_t provider =
      TURBO_FLOW_EMITTING_OPERATION_PROVIDER_REGISTRATION_INIT;
  turbo_flow_t *flow = turbo_flow_create();

  memset(&input, 0, sizeof(input));
  input.size = sizeof(input);
  input.name = "data.input";
  input.version = 1u;
  input.domain = TURBO_FLOW_DOMAIN_DATA;
  input.output_domain = TURBO_FLOW_DOMAIN_DATA;
  input.output_type = "Message";
  input.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  input.scope.lifetime = TURBO_FLOW_LIFETIME_DISPATCH;
  input.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  input.scope.authority = TURBO_FLOW_AUTHORITY_PURE;
  input.flags = TURBO_FLOW_OPERATION_SOURCE;
  input.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  expand = input;
  expand.name = "data.expand";
  expand.input_domain = TURBO_FLOW_DOMAIN_DATA;
  expand.input_type = "Message";
  expand.flags = TURBO_FLOW_OPERATION_STAGE;
  provider.operation_name = expand.name;
  provider.fn = bench_emitting_stage;
  provider.ctx = bench;
  provider.max_outputs = bench->output_count;

  check_not_null(flow);
  check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
  check_int_eq(turbo_flow_register_operation(flow, &expand), TURBO_OK);
  check_int_eq(turbo_flow_register_emitting_operation_provider(flow, &provider), TURBO_OK);
  check_int_eq(turbo_flow_register_stage_ex(flow, "sink", bench_emitter_sink, bench, NULL),
               TURBO_OK);
  check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
  check_int_eq(turbo_flow_compile(flow), TURBO_OK);
  check_int_eq(turbo_flow_start(flow), TURBO_OK);
  return flow;
}

static turbo_flow_t *bench_create_keyed_flow(flow_bench_keyed_t *bench, size_t max_entries,
                                             int emitting) {
  static const char *src = "source input operation data.input\n"
                           "stage count operation data.count\n"
                           "stage sink\n"
                           "stage main {\n"
                           "  input -> count -> sink\n"
                           "}\n";
  turbo_flow_keyed_state_store_config_t config = TURBO_FLOW_KEYED_STATE_STORE_CONFIG_INIT;
  turbo_flow_operation_descriptor_t input;
  turbo_flow_operation_descriptor_t count;
  turbo_flow_keyed_operation_provider_registration_t provider =
      TURBO_FLOW_KEYED_OPERATION_PROVIDER_REGISTRATION_INIT;
  turbo_flow_keyed_emitting_operation_provider_registration_t emitting_provider =
      TURBO_FLOW_KEYED_EMITTING_OPERATION_PROVIDER_REGISTRATION_INIT;
  turbo_flow_t *flow = turbo_flow_create();

  config.max_entries = max_entries;
  config.max_key_size = sizeof(uint64_t);
  config.max_value_size = sizeof(uint64_t);
  config.max_total_bytes = max_entries * sizeof(uint64_t) * 2u;
  bench->store = turbo_flow_keyed_state_store_create(&config);
  memset(&input, 0, sizeof(input));
  input.size = sizeof(input);
  input.name = "data.input";
  input.version = 1u;
  input.domain = TURBO_FLOW_DOMAIN_DATA;
  input.output_domain = TURBO_FLOW_DOMAIN_DATA;
  input.output_type = "Message";
  input.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  input.scope.lifetime = TURBO_FLOW_LIFETIME_DISPATCH;
  input.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  input.scope.authority = TURBO_FLOW_AUTHORITY_PURE;
  input.flags = TURBO_FLOW_OPERATION_SOURCE;
  input.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  count = input;
  count.name = "data.count";
  count.input_domain = TURBO_FLOW_DOMAIN_DATA;
  count.input_type = "Message";
  count.scope.state = TURBO_FLOW_STATE_SCOPE_NODE;
  count.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  count.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  count.flags = TURBO_FLOW_OPERATION_STAGE;
  check_not_null(flow);
  check_not_null(bench->store);
  check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
  check_int_eq(turbo_flow_register_operation(flow, &count), TURBO_OK);
  if (emitting) {
    emitting_provider.operation_name = count.name;
    emitting_provider.key_selector = bench_keyed_select;
    emitting_provider.fn = bench_keyed_window;
    emitting_provider.ctx = bench;
    emitting_provider.store = bench->store;
    emitting_provider.max_outputs = 1u;
    check_int_eq(turbo_flow_register_keyed_emitting_operation_provider(flow, &emitting_provider),
                 TURBO_OK);
  } else {
    provider.operation_name = count.name;
    provider.key_selector = bench_keyed_select;
    provider.fn = bench_keyed_increment;
    provider.ctx = bench;
    provider.store = bench->store;
    check_int_eq(turbo_flow_register_keyed_operation_provider(flow, &provider), TURBO_OK);
  }
  check_int_eq(turbo_flow_register_stage_ex(flow, "sink", bench_keyed_sink, bench, NULL), TURBO_OK);
  check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
  check_int_eq(turbo_flow_compile(flow), TURBO_OK);
  check_int_eq(turbo_flow_start(flow), TURBO_OK);
  return flow;
}

static turbo_flow_t *bench_create_event_window_flow(flow_bench_keyed_t *bench, size_t max_windows,
                                                    uint64_t window_size_ns) {
  static const char *src = "source input operation data.input\n"
                           "stage window operation data.event_window\n"
                           "stage sink\n"
                           "stage main {\n"
                           "  input -> window -> sink\n"
                           "}\n";
  turbo_flow_event_time_window_store_config_t config =
      TURBO_FLOW_EVENT_TIME_WINDOW_STORE_CONFIG_INIT;
  turbo_flow_operation_descriptor_t input;
  turbo_flow_operation_descriptor_t window;
  turbo_flow_event_time_window_provider_registration_t provider =
      TURBO_FLOW_EVENT_TIME_WINDOW_PROVIDER_REGISTRATION_INIT;
  turbo_flow_t *flow = turbo_flow_create();

  config.max_windows = max_windows;
  config.max_key_size = sizeof(uint64_t);
  config.max_value_size = sizeof(uint64_t);
  config.max_total_bytes = max_windows * sizeof(uint64_t) * 3u;
  config.window_size_ns = window_size_ns;
  config.allowed_lateness_ns = 0u;
  bench->store = turbo_flow_event_time_window_store_create(&config);
  memset(&input, 0, sizeof(input));
  input.size = sizeof(input);
  input.name = "data.input";
  input.version = 1u;
  input.domain = TURBO_FLOW_DOMAIN_DATA;
  input.output_domain = TURBO_FLOW_DOMAIN_DATA;
  input.output_type = "Message";
  input.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  input.scope.lifetime = TURBO_FLOW_LIFETIME_DISPATCH;
  input.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  input.scope.authority = TURBO_FLOW_AUTHORITY_PURE;
  input.flags = TURBO_FLOW_OPERATION_SOURCE;
  input.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  window = input;
  window.name = "data.event_window";
  window.input_domain = TURBO_FLOW_DOMAIN_DATA;
  window.input_type = "Message";
  window.scope.state = TURBO_FLOW_STATE_SCOPE_NODE;
  window.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  window.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  window.flags = TURBO_FLOW_OPERATION_STAGE;
  provider.operation_name = window.name;
  provider.key_selector = bench_keyed_select;
  provider.on_event = bench_event_window_accumulate;
  provider.on_close = bench_event_window_close;
  provider.ctx = bench;
  provider.store = bench->store;
  provider.max_outputs = 1u;
  check_not_null(flow);
  check_not_null(bench->store);
  check_int_eq(turbo_flow_register_operation(flow, &input), TURBO_OK);
  check_int_eq(turbo_flow_register_operation(flow, &window), TURBO_OK);
  check_int_eq(turbo_flow_register_event_time_window_provider(flow, &provider), TURBO_OK);
  check_int_eq(turbo_flow_register_stage_ex(flow, "sink", bench_keyed_sink, bench, NULL), TURBO_OK);
  check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
  check_int_eq(turbo_flow_compile(flow), TURBO_OK);
  check_int_eq(turbo_flow_start(flow), TURBO_OK);
  return flow;
}

static turbo_flow_t *bench_create_keyed_baseline_flow(flow_bench_keyed_t *bench) {
  static const char *src = "source input\n"
                           "stage count\n"
                           "stage sink\n"
                           "stage main {\n"
                           "  input -> count -> sink\n"
                           "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  check_not_null(flow);
  check_int_eq(turbo_flow_register_stage_ex(flow, "count", bench_keyed_baseline_stage, bench, NULL),
               TURBO_OK);
  check_int_eq(turbo_flow_register_stage_ex(flow, "sink", bench_keyed_sink, bench, NULL), TURBO_OK);
  check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
  check_int_eq(turbo_flow_compile(flow), TURBO_OK);
  check_int_eq(turbo_flow_start(flow), TURBO_OK);
  return flow;
}

static void bench_destroy_started_flow(turbo_flow_t *flow) {
  check_int_eq(turbo_flow_stop(flow), TURBO_OK);
  turbo_flow_destroy(flow);
}

static void bench_publish_message(turbo_flow_t *flow, const char *source_name,
                                  turbo_flow_msg_t *msg) {
  check_int_eq(turbo_flow_publish(flow, source_name, msg), TURBO_OK);
}

spec("Turbo Flow Bench") {
  bench("compile") {
    static const char *src = "source input\n"
                             "stage parse\n"
                             "stage validate\n"
                             "stage sink\n"
                             "stage main {\n"
                             "  input -> parse -> validate -> sink\n"
                             "}\n";

    benchmark("stage_plan=linear-3-stage executor=none workers=0 payload_bytes=0 compile",
              FLOW_BENCH_COMPILE_ITERS, 1) {
      turbo_flow_t *flow = turbo_flow_create();
      check_not_null(flow);
      check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
      bench_register_stage(flow, "parse");
      bench_register_stage(flow, "validate");
      bench_register_stage(flow, "sink");
      check_int_eq(turbo_flow_compile(flow), TURBO_OK);
      turbo_flow_destroy(flow);
    }
  }

  bench("publish") {
    static const char *linear_src = "source input\n"
                                    "stage parse\n"
                                    "stage validate\n"
                                    "stage sink\n"
                                    "stage main {\n"
                                    "  input -> parse -> validate -> sink\n"
                                    "}\n";
    static const char *diamond_src = "source input\n"
                                     "stage parse\n"
                                     "stage validate\n"
                                     "stage enrich\n"
                                     "stage sink\n"
                                     "stage main {\n"
                                     "  input -> parse -> [validate, enrich] -> sink\n"
                                     "}\n";
    static const char *worker_src = "source input\n"
                                    "stage enrich worker 4\n"
                                    "stage sink\n"
                                    "stage main {\n"
                                    "  input -> enrich -> sink\n"
                                    "}\n";
    const char *linear_stages[] = {"parse", "validate", "sink"};
    const char *diamond_stages[] = {"parse", "validate", "enrich", "sink"};
    const char *worker_stages[] = {"enrich", "sink"};
    char raw[] = "payload";
    turbo_flow_msg_t msg;

    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup(raw);
    msg.payload = tstr_to_v(msg.owned_payload);

    {
      turbo_flow_t *flow = bench_create_started_flow(
          linear_src, linear_stages, sizeof(linear_stages) / sizeof(linear_stages[0]));
      benchmark("stage_plan=linear-3-stage executor=inline workers=1 payload_bytes=7 publish",
                FLOW_BENCH_PUBLISH_ITERS, 1) {
        bench_publish_message(flow, "input", &msg);
      }
      bench_destroy_started_flow(flow);
    }

    {
      turbo_flow_t *flow = bench_create_started_flow(
          diamond_src, diamond_stages, sizeof(diamond_stages) / sizeof(diamond_stages[0]));
      benchmark("stage_plan=diamond-4-stage executor=inline workers=1 payload_bytes=7 publish",
                FLOW_BENCH_PUBLISH_ITERS, 1) {
        bench_publish_message(flow, "input", &msg);
      }
      bench_destroy_started_flow(flow);
    }

    {
      turbo_flow_t *flow = bench_create_started_flow(
          worker_src, worker_stages, sizeof(worker_stages) / sizeof(worker_stages[0]));
      benchmark("stage_plan=linear-2-stage executor=inline workers=4 payload_bytes=7 publish",
                FLOW_BENCH_PUBLISH_ITERS, 1) {
        bench_publish_message(flow, "input", &msg);
      }
      bench_destroy_started_flow(flow);
    }

    for (uint32_t output_count = 1u; output_count <= 4u; output_count *= 4u) {
      flow_bench_emitter_t emitter = {output_count, 0u};
      turbo_flow_t *flow = bench_create_emitting_flow(&emitter);
      if (output_count == 1u) {
        benchmark("stage_plan=emit-1-output executor=inline workers=1 payload_bytes=7 publish",
                  FLOW_BENCH_EXECUTOR_ITERS, 1) {
          bench_publish_message(flow, "input", &msg);
        }
      } else {
        benchmark("stage_plan=emit-4-output executor=inline workers=1 payload_bytes=7 publish",
                  FLOW_BENCH_EXECUTOR_ITERS, 1) {
          bench_publish_message(flow, "input", &msg);
        }
      }
      check_true(emitter.sink_count >= output_count * FLOW_BENCH_EXECUTOR_ITERS);
      bench_destroy_started_flow(flow);
    }

    {
      flow_bench_keyed_t baseline = {NULL, 0u, 0u};
      turbo_flow_t *flow = bench_create_keyed_baseline_flow(&baseline);
      int publish_status = TURBO_OK;
      msg.id = 1u;
      benchmark_ops(
          "stage_plan=keyed-count-baseline state=none executor=inline payload_bytes=7 publish",
          FLOW_BENCH_EXECUTOR_ITERS, 1u) {
        publish_status = turbo_flow_publish(flow, "input", &msg);
      }
      check_int_eq(publish_status, TURBO_OK);
      check_size_eq(baseline.sink_count, FLOW_BENCH_EXECUTOR_ITERS);
      bench_destroy_started_flow(flow);
    }

    {
      flow_bench_keyed_t keyed = {NULL, 0u, 0u};
      turbo_flow_t *flow = bench_create_keyed_flow(&keyed, 1u, 0);
      int publish_status = TURBO_OK;
      msg.id = 1u;
      benchmark_ops("stage_plan=keyed-count keys=1 executor=inline payload_bytes=7 publish",
                    FLOW_BENCH_EXECUTOR_ITERS, 1u) {
        publish_status = turbo_flow_publish(flow, "input", &msg);
      }
      check_int_eq(publish_status, TURBO_OK);
      check_size_eq(keyed.sink_count, FLOW_BENCH_EXECUTOR_ITERS);
      bench_destroy_started_flow(flow);
      turbo_flow_keyed_state_store_destroy(keyed.store);
    }

    {
      enum { FLOW_BENCH_KEY_COUNT = 1024 };
      flow_bench_keyed_t keyed = {NULL, 0u, 0u};
      turbo_flow_t *flow = bench_create_keyed_flow(&keyed, FLOW_BENCH_KEY_COUNT, 0);
      size_t next_key = 0u;
      int publish_status = TURBO_OK;
      benchmark_ops("stage_plan=keyed-count keys=1024 executor=inline payload_bytes=7 publish",
                    FLOW_BENCH_EXECUTOR_ITERS, 1u) {
        msg.id = next_key++ % FLOW_BENCH_KEY_COUNT;
        publish_status = turbo_flow_publish(flow, "input", &msg);
      }
      check_int_eq(publish_status, TURBO_OK);
      check_size_eq(keyed.sink_count, FLOW_BENCH_EXECUTOR_ITERS);
      check_size_eq(turbo_flow_keyed_state_store_size(keyed.store), FLOW_BENCH_KEY_COUNT);
      bench_destroy_started_flow(flow);
      turbo_flow_keyed_state_store_destroy(keyed.store);
    }

    {
      flow_bench_keyed_t window = {NULL, 0u, 100u};
      turbo_flow_t *flow = bench_create_keyed_flow(&window, 1u, 1);
      int publish_status = TURBO_OK;
      msg.id = 1u;
      benchmark_ops(
          "stage_plan=keyed-window window=count-100 keys=1 executor=inline payload_bytes=7 publish",
          FLOW_BENCH_EXECUTOR_ITERS, 1u) {
        publish_status = turbo_flow_publish(flow, "input", &msg);
      }
      check_int_eq(publish_status, TURBO_OK);
      check_size_eq(window.sink_count, FLOW_BENCH_EXECUTOR_ITERS / window.window_size);
      bench_destroy_started_flow(flow);
      turbo_flow_keyed_state_store_destroy(window.store);
    }

    {
      flow_bench_keyed_t window = {NULL, 0u, 0u};
      turbo_flow_t *flow = bench_create_event_window_flow(&window, 1u, UINT64_C(1000000000));
      int publish_status = TURBO_OK;
      msg.id = 1u;
      msg.ts_ns = 1u;
      benchmark_ops("stage_plan=event-time-tumbling-window windows=1 keys=1 executor=inline "
                    "payload_bytes=7 publish",
                    FLOW_BENCH_EXECUTOR_ITERS, 1u) {
        publish_status = turbo_flow_publish(flow, "input", &msg);
      }
      check_int_eq(publish_status, TURBO_OK);
      check_size_eq(turbo_flow_keyed_state_store_size(window.store), 1u);
      bench_destroy_started_flow(flow);
      turbo_flow_event_time_window_store_destroy(window.store);
    }

    {
      enum { FLOW_BENCH_EVENT_WINDOWS = 1024 };
      flow_bench_keyed_t window = {NULL, 0u, 0u};
      turbo_flow_t *flow = bench_create_event_window_flow(&window, FLOW_BENCH_EVENT_WINDOWS, 10u);
      size_t next_window = 0u;
      size_t closed = 0u;
      int watermark_status = TURBO_OK;
      msg.id = 1u;
      for (size_t index = 0u; index < FLOW_BENCH_EVENT_WINDOWS; ++index) {
        msg.ts_ns = index * 10u;
        check_int_eq(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      }
      benchmark_ops("stage_plan=event-time-tumbling-window windows=1024 keys=1 executor=inline "
                    "close-one-watermark",
                    FLOW_BENCH_EVENT_WINDOWS, 1u) {
        next_window += 1u;
        watermark_status =
            turbo_flow_advance_event_time_watermark(flow, window.store, next_window * 10u, &closed);
      }
      check_int_eq(watermark_status, TURBO_OK);
      check_size_eq(window.sink_count, FLOW_BENCH_EVENT_WINDOWS);
      check_size_eq(turbo_flow_keyed_state_store_size(window.store), 0u);
      bench_destroy_started_flow(flow);
      turbo_flow_event_time_window_store_destroy(window.store);
    }

    turbo_flow_msg_cleanup(&msg);
  }

  bench("async ingress") {
    static const char *src = "source input\n"
                             "stage sink\n"
                             "stage main {\n"
                             "  input -> sink\n"
                             "}\n";
    turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
    flow_bench_async_completion_t completion;
    turbo_flow_msg_t msg;
    turbo_flow_t *flow = turbo_flow_create();
    int submit_status = TURBO_OK;

    ingress.queue_capacity = 16384u;
    atomic_init(&completion.completed, 0u);
    atomic_init(&completion.status, TURBO_OK);
    check_not_null(flow);
    check_int_eq(turbo_flow_configure_async_ingress(flow, &ingress), TURBO_OK);
    check_int_eq(turbo_flow_parse_string(flow, src, strlen(src)), TURBO_OK);
    bench_register_stage(flow, "sink");
    check_int_eq(turbo_flow_compile(flow), TURBO_OK);
    check_int_eq(turbo_flow_start(flow), TURBO_OK);
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_dup("payload");
    msg.payload = tstr_to_v(msg.owned_payload);

    benchmark_ops(
        "stage_plan=linear-1-stage ingress=async workers=1 capacity=16384 payload_bytes=7 submit",
        FLOW_BENCH_ASYNC_INGRESS_ITERS, 1u) {
      submit_status =
          turbo_flow_publish_async(flow, "input", &msg, bench_async_publish_complete, &completion);
    }
    check_int_eq(submit_status, TURBO_OK);
    while (atomic_load_explicit(&completion.completed, memory_order_acquire) <
           FLOW_BENCH_ASYNC_INGRESS_ITERS) {
      turbo_thread_yield();
    }
    check_int_eq(atomic_load_explicit(&completion.status, memory_order_acquire), TURBO_OK);
    check_size_eq(atomic_load_explicit(&completion.completed, memory_order_acquire),
                  FLOW_BENCH_ASYNC_INGRESS_ITERS);
    check_int_eq(turbo_flow_stop(flow), TURBO_OK);
    turbo_flow_msg_cleanup(&msg);
    turbo_flow_destroy(flow);
  }

  bench("executor metrics") {
    static const uint32_t worker_counts[] = {1, 2, 4, 8};
    static const size_t payload_size = 256;

    for (size_t i = 0; i < sizeof(worker_counts) / sizeof(worker_counts[0]); ++i) {
      char exec_spec[64];
      turbo_flow_t *flow;
      int written =
          snprintf(exec_spec, sizeof(exec_spec), "exec thread workers %" PRIu32, worker_counts[i]);
      check_true(written > 0 && (size_t)written < sizeof(exec_spec));
      flow = bench_create_executor_flow(exec_spec);
      bench_report_publish(flow, "linear-1-stage", "thread", worker_counts[i], payload_size,
                           FLOW_BENCH_EXECUTOR_ITERS);
      bench_destroy_started_flow(flow);
    }

    {
      turbo_flow_t *flow = bench_create_executor_flow("exec coro lanes 2");
      bench_report_publish(flow, "linear-1-stage", "coro-unpooled", 2, payload_size,
                           FLOW_BENCH_EXECUTOR_ITERS);
      bench_destroy_started_flow(flow);
    }

    {
      turbo_flow_t *flow = bench_create_executor_flow("exec coro lanes 2 pool 128");
      bench_report_publish(flow, "linear-1-stage", "coro-pooled", 2, payload_size,
                           FLOW_BENCH_EXECUTOR_ITERS);
      bench_destroy_started_flow(flow);
    }

    {
      turbo_flow_t *flow = bench_create_executor_flow("worker 4");
      bench_report_publish(flow, "worker-cross-ring", "inline", 4, payload_size,
                           FLOW_BENCH_EXECUTOR_ITERS);
      bench_destroy_started_flow(flow);
    }

    {
      turbo_flow_t *flow = bench_create_executor_flow("worker 4 capacity 1024");
      bench_report_idle_cpu(flow, "worker-cross-ring", "disruptor-parked", 4);
      bench_report_concurrent_publish(flow, "worker-cross-ring", "inline", 4, 4, payload_size,
                                      FLOW_BENCH_EXECUTOR_ITERS);
      bench_destroy_started_flow(flow);
    }

    {
      turbo_flow_t *flow = bench_create_executor_flow("exec coro lanes 4 pool 128");
      bench_report_concurrent_publish(flow, "linear-1-stage", "coro-pooled", 4, 4, payload_size,
                                      FLOW_BENCH_EXECUTOR_ITERS);
      bench_destroy_started_flow(flow);
    }
  }

  bench("teardown") {
    benchmark(
        "stage_plan=linear-1-stage executor=thread workers=4 jobs=idle payload_bytes=0 teardown",
        FLOW_BENCH_TEARDOWN_ITERS, 1) {
      turbo_flow_t *flow = bench_create_executor_flow("exec thread workers 4");
      bench_destroy_started_flow(flow);
    }

    benchmark("stage_plan=linear-1-stage executor=thread workers=4 jobs=completed-1 "
              "payload_bytes=7 teardown",
              FLOW_BENCH_TEARDOWN_ITERS, 1) {
      turbo_flow_msg_t msg;
      turbo_flow_t *flow = bench_create_executor_flow("exec thread workers 4");
      turbo_flow_msg_init(&msg);
      msg.owned_payload = tstr_dup("payload");
      msg.payload = tstr_to_v(msg.owned_payload);
      check_int_eq(turbo_flow_publish(flow, "input", &msg), TURBO_OK);
      turbo_flow_msg_cleanup(&msg);
      bench_destroy_started_flow(flow);
    }

    benchmark(
        "stage_plan=linear-1-stage executor=thread workers=4 jobs=live payload_bytes=0 teardown",
        FLOW_BENCH_TEARDOWN_ITERS, 1) {
      turbo_flow_t *flow = bench_create_executor_flow("exec thread workers 4");
      uint32_t stage_index = (uint32_t)turbo_flow_find_stage(flow, "work");
      const flow_threadpool_adapter_t *adapter = bench_threadpool_adapter(flow, stage_index);
      flow_bench_live_jobs_t jobs;
      turbo_thread_t releaser;
      atomic_init(&jobs.release, 0);
      atomic_init(&jobs.started, 0u);
      atomic_init(&jobs.completed, 0u);
      check_not_null(adapter);
      for (uint32_t i = 0; i < 4; ++i)
        check_int_eq(turbo_threadpool_submit(adapter->pool, bench_live_job, &jobs), TURBO_OK);
      while (atomic_load_explicit(&jobs.started, memory_order_acquire) == 0u)
        turbo_sleep_ms(1);
      check_int_eq(turbo_thread_create(&releaser, bench_release_live_jobs, &jobs), TURBO_OK);
      bench_destroy_started_flow(flow);
      check_int_eq(turbo_thread_join(&releaser), TURBO_OK);
      check_int_eq(atomic_load_explicit(&jobs.completed, memory_order_acquire), 4);
    }
  }

  bench("expression") {
    static const char *expression =
        "msg.status == 0 and (msg.flags % 2 == 0 or msg.payload == \"ok\")";
    turbo_flow_expr_compile_options_t options = TURBO_FLOW_EXPR_COMPILE_OPTIONS_INIT;

    benchmark("expr=typed-ir nodes=fields-arithmetic-logic backend=none parse-type",
              FLOW_BENCH_EXPR_COMPILE_ITERS, 1) {
      flow_expr_ast_t ast;
      turbo_flow_error_t error;
      check_int_eq(flow_expr_parse(expression, strlen(expression), &ast, &error), TURBO_OK);
      check_int_eq(flow_expr_type_check(&ast, NULL, &error), TURBO_OK);
      flow_expr_ast_destroy(&ast);
    }

    options.backend = TURBO_FLOW_EXPR_MIR_INTERP;
    benchmark("expr=typed-ir nodes=fields-arithmetic-logic backend=mir-interp compile",
              FLOW_BENCH_EXPR_COMPILE_ITERS, 1) {
      turbo_flow_expr_t *expr = NULL;
      turbo_flow_error_t error;
      check_int_eq(
          turbo_flow_expr_compile_ex(expression, strlen(expression), NULL, &options, &expr, &error),
          TURBO_OK);
      turbo_flow_expr_destroy(expr);
    }

    if (turbo_flow_expr_jit_available()) {
      options.backend = TURBO_FLOW_EXPR_MIR_JIT;
      benchmark("expr=typed-ir nodes=fields-arithmetic-logic backend=mir-jit compile",
                FLOW_BENCH_EXPR_COMPILE_ITERS, 1) {
        turbo_flow_expr_t *expr = NULL;
        turbo_flow_error_t error;
        check_int_eq(turbo_flow_expr_compile_ex(expression, strlen(expression), NULL, &options,
                                                &expr, &error),
                     TURBO_OK);
        turbo_flow_expr_destroy(expr);
      }
    }

    for (int backend = TURBO_FLOW_EXPR_MIR_INTERP; backend <= TURBO_FLOW_EXPR_MIR_JIT; ++backend) {
      turbo_flow_expr_t *expr = NULL;
      turbo_flow_expr_eval_context_t context = TURBO_FLOW_EXPR_EVAL_CONTEXT_INIT;
      turbo_flow_expr_value_t value;
      turbo_flow_error_t error;
      turbo_flow_msg_t msg;
      if (backend == TURBO_FLOW_EXPR_MIR_JIT && !turbo_flow_expr_jit_available()) continue;
      memset(&value, 0, sizeof(value));
      options.backend = (turbo_flow_expr_backend_t)backend;
      turbo_flow_msg_init(&msg);
      msg.status = 0;
      msg.flags = 2;
      msg.owned_payload = tstr_dup("payload");
      msg.payload = tstr_to_v(msg.owned_payload);
      context.message = &msg;
      check_int_eq(
          turbo_flow_expr_compile_ex(expression, strlen(expression), NULL, &options, &expr, &error),
          TURBO_OK);
      if (backend == TURBO_FLOW_EXPR_MIR_INTERP) {
        benchmark("expr=typed-ir nodes=fields-arithmetic-logic backend=mir-interp evaluate",
                  FLOW_BENCH_EXPR_EVAL_ITERS, 1) {
          check_int_eq(turbo_flow_expr_evaluate(expr, &context, &value), TURBO_OK);
        }
      } else {
        benchmark("expr=typed-ir nodes=fields-arithmetic-logic backend=mir-jit evaluate",
                  FLOW_BENCH_EXPR_EVAL_ITERS, 1) {
          check_int_eq(turbo_flow_expr_evaluate(expr, &context, &value), TURBO_OK);
        }
      }
      check_int_eq(value.type, TURBO_FLOW_EXPR_TYPE_BOOL);
      check_int_eq(value.as.boolean, 1);
      turbo_flow_expr_destroy(expr);
      turbo_flow_msg_cleanup(&msg);
    }
  }

  bench("security") {
    static const size_t rule_counts[] = {64u, 512u, TURBO_FLOW_SECURITY_MAX_RULES};
    turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
    turbo_flow_security_request_t request = TURBO_FLOW_SECURITY_REQUEST_INIT;
    turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;
    bench_security_copy(principal.principal_id, sizeof(principal.principal_id), "device-7");
    bench_security_copy(principal.principal_type, sizeof(principal.principal_type), "device");
    bench_security_copy(principal.root_group_id, sizeof(principal.root_group_id), "root-0");
    bench_security_copy(principal.auth_method, sizeof(principal.auth_method), "token");
    principal.scope = TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP;
    principal.role_count = 1u;
    bench_security_copy(principal.roles[0], sizeof(principal.roles[0]), "writer");
    principal.group_count = 1u;
    bench_security_copy(principal.groups[0], sizeof(principal.groups[0]), "root-0");
    principal.policy_version = 1u;
    request.principal = &principal;
    request.root_group_id = "root-0";
    request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
    request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    request.resource = "target/topic";
    for (size_t i = 0u; i < sizeof(rule_counts) / sizeof(rule_counts[0]); ++i) {
      for (bench_security_layout_t layout = BENCH_SECURITY_DISTINCT_EXACT;
           layout <= BENCH_SECURITY_SHARED_ADAPTER; ++layout) {
        turbo_flow_security_rule_t *rules = NULL;
        turbo_flow_security_realm_t *realm = bench_security_realm(rule_counts[i], layout, &rules);
        char label[96];
        int written =
            snprintf(label, sizeof(label), "%s rules=%llu authorize",
                     bench_security_layout_name(layout), (unsigned long long)rule_counts[i]);
        check_true(written > 0 && (size_t)written < sizeof(label));
        benchmark_ops(label,
                      layout == BENCH_SECURITY_DISTINCT_EXACT ? FLOW_BENCH_SECURITY_EVAL_ITERS
                                                              : FLOW_BENCH_SECURITY_CANDIDATE_ITERS,
                      1u) {
          decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
          check_int_eq(turbo_flow_security_realm_authorize(realm, &request, 100u, &decision),
                       TURBO_OK);
        }
        check_int_eq(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
        turbo_flow_security_realm_destroy(realm);
        free(rules);
      }
    }
  }
}
