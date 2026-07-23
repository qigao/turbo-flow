#include "flowie_rule_internal.h"
#include "flowie_security_internal.h"
#include "flowie_session_internal.h"
#include "flowie_test_socket.h"
#include "flowie_topic_index_internal.h"

#include "flowie.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"
#include "turbo_vec.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_BENCH_CAPACITY 100000u
#define FLOWIE_BENCH_TOPIC_BUFFER_SIZE 128u
#define FLOWIE_BENCH_FANOUT_SUBSCRIBERS 16u
#define FLOWIE_BENCH_FANOUT_SAMPLES 1000u
#define FLOWIE_BENCH_PIPELINE_MESSAGES 64u
#define FLOWIE_BENCH_PIPELINE_SAMPLES 500u
#define FLOWIE_BENCH_CHURN_SAMPLES 500u
#define FLOWIE_BENCH_REBUILD_SAMPLES 8u
#define FLOWIE_BENCH_CANDIDATE_MATCH_SAMPLES 256u
#define FLOWIE_BENCH_SECURITY_SAMPLES 1000u
#define FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE 64u
#define FLOWIE_BENCH_PROJECTION_SAMPLES 1000u
#define FLOWIE_BENCH_PROJECTION_OPS_PER_SAMPLE 64u
#define FLOWIE_BENCH_STALL_SAMPLES 8u
#define FLOWIE_BENCH_STALL_MESSAGES 4u
#define FLOWIE_BENCH_STALL_PAYLOAD_BYTES (512u * 1024u)
#define FLOWIE_BENCH_STALL_RECV_BUFFER_BYTES 1024u
#define FLOWIE_BENCH_STALL_PACKET_CAPACITY (FLOWIE_BENCH_STALL_PAYLOAD_BYTES + 64u)
#define FLOWIE_BENCH_STALL_SEND_HWM_BYTES (768u * 1024u)
#define FLOWIE_BENCH_WAIT_TIMEOUT_NS UINT64_C(2000000000)
#define FLOWIE_BENCH_LIVE_TCP_CAPACITY 100000u
#define FLOWIE_BENCH_LIVE_TCP_ALIASES 16u
#define FLOWIE_BENCH_LIVE_TCP_CONNECTIONS_PER_ALIAS 8192u
#define FLOWIE_BENCH_LIVE_TCP_SOURCE_PORT_BASE 20000u
#define FLOWIE_BENCH_LIVE_WAIT_TIMEOUT_NS UINT64_C(300000000000)
#define FLOWIE_BENCH_LIVE_COROUTINE_STACK_SIZE FLOWIE_MIN_COROUTINE_STACK_SIZE

static int flowie_bench_u64_compare(const void *lhs, const void *rhs) {
  const uint64_t left = *(const uint64_t *)lhs;
  const uint64_t right = *(const uint64_t *)rhs;
  return left < right ? -1 : left > right ? 1 : 0;
}

static uint64_t flowie_bench_percentile(const uint64_t *sorted, size_t count, size_t percent) {
  size_t index;
  if (!sorted || count == 0u || percent > 100u) return 0u;
  index = ((count - 1u) * percent + 99u) / 100u;
  return sorted[index];
}

static flowie_mqtt_span_t flowie_bench_span(const char *value) {
  return (flowie_mqtt_span_t){(const uint8_t *)value, strlen(value)};
}

static void flowie_bench_mqtt_projection(void) {
  static const uint8_t publish[] = {0x30u, 0x16u, 0x00u, 0x0bu, 'b', 'e', 'n', 'c',
                                    'h',   '/',   't',   'o',   'p', 'i', 'c', 0x00u,
                                    'p',   'a',   'y',   'l',   'o', 'a', 'd', '!'};
  const turbo_flow_expr_schema_t *schema = flowie_mqtt_rule_schema();
  const turbo_flow_expr_value_t *values = NULL;
  turbo_flow_msg_t opaque;
  turbo_flow_msg_t projected;
  size_t value_count = 0u;
  int rc;

  turbo_flow_msg_init(&opaque);
  opaque.type = FLOWIE_MQTT_PACKET_PUBLISH;
  opaque.payload = tstr_v_from_buf((const char *)publish, sizeof(publish));
  rc = flowie_mqtt_message_flags_encode(FLOWIE_MQTT_VERSION_5, publish[0] & 0x0fu, &opaque.flags);
  check_int_eq(rc, TURBO_OK);

  turbo_flow_msg_init(&projected);
  projected.type = opaque.type;
  projected.flags = opaque.flags;
  projected.buffer = mem_get_buffer(mem_global(), sizeof(publish));
  check_not_null(projected.buffer);
  if (!projected.buffer) return;
  memcpy(mem_buffer_data(projected.buffer), publish, sizeof(publish));
  mem_set_used(projected.buffer, sizeof(publish));
  projected.payload = tstr_v_from_buf(mem_buffer_data(projected.buffer), sizeof(publish));
  rc = flowie_mqtt_rule_bind_projection(&projected, NULL);
  check_int_eq(rc, TURBO_OK);

  benchmark_ops("MQTT opaque facts parse", FLOWIE_BENCH_PROJECTION_SAMPLES,
                FLOWIE_BENCH_PROJECTION_OPS_PER_SAMPLE) {
    for (size_t operation = 0u;
         rc == TURBO_OK && operation < FLOWIE_BENCH_PROJECTION_OPS_PER_SAMPLE; ++operation)
      rc = flowie_mqtt_rule_facts_provider(&opaque, schema, &values, &value_count, NULL);
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(value_count, schema->field_count);

  benchmark_ops("MQTT bound projection facts", FLOWIE_BENCH_PROJECTION_SAMPLES,
                FLOWIE_BENCH_PROJECTION_OPS_PER_SAMPLE) {
    for (size_t operation = 0u;
         rc == TURBO_OK && operation < FLOWIE_BENCH_PROJECTION_OPS_PER_SAMPLE; ++operation)
      rc = flowie_mqtt_rule_facts_provider(&projected, schema, &values, &value_count, NULL);
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(value_count, schema->field_count);
  turbo_flow_msg_cleanup(&projected);
  turbo_flow_msg_cleanup(&opaque);
}

static void flowie_bench_copy(char *output, size_t capacity, const char *value) {
  const size_t size = strlen(value);
  check_size_lt(size, capacity);
  memcpy(output, value, size + 1u);
}

typedef struct flowie_bench_security_s {
  turbo_flow_security_rule_t *rules;
  turbo_flow_security_realm_t *realm;
  turbo_flow_security_principal_t principal;
  turbo_flow_security_request_t request;
  flowie_mqtt_security_context_t protocol_context;
} flowie_bench_security_t;

typedef struct flowie_bench_security_visit_s {
  size_t count;
} flowie_bench_security_visit_t;

static int flowie_bench_security_visit(void *ctx, size_t entry_index) {
  flowie_bench_security_visit_t *visit = (flowie_bench_security_visit_t *)ctx;
  (void)entry_index;
  if (!visit) return TURBO_EINVAL;
  ++visit->count;
  return TURBO_OK;
}

static int flowie_bench_security_init(flowie_bench_security_t *bench, size_t rule_count) {
  turbo_flow_security_matcher_t matcher = TURBO_FLOW_SECURITY_MATCHER_INIT;
  turbo_flow_security_realm_config_t config = TURBO_FLOW_SECURITY_REALM_CONFIG_INIT;
  int rc;
  if (!bench || rule_count == 0u || rule_count > TURBO_FLOW_SECURITY_MAX_RULES) return TURBO_EINVAL;
  memset(bench, 0, sizeof(*bench));
  bench->principal = (turbo_flow_security_principal_t)TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  bench->request = (turbo_flow_security_request_t)TURBO_FLOW_SECURITY_REQUEST_INIT;
  bench->protocol_context = (flowie_mqtt_security_context_t)FLOWIE_MQTT_SECURITY_CONTEXT_INIT;
  bench->rules = (turbo_flow_security_rule_t *)calloc(rule_count, sizeof(*bench->rules));
  if (!bench->rules) return TURBO_ENOMEM;
  for (size_t i = 0u; i < rule_count; ++i) {
    char pattern[FLOWIE_BENCH_TOPIC_BUFFER_SIZE];
    const int written = snprintf(pattern, sizeof(pattern), "root-a/device-%zu/events/#", i);
    if (written <= 0 || (size_t)written >= sizeof(pattern)) return TURBO_EMSGSIZE;
    bench->rules[i] = (turbo_flow_security_rule_t)TURBO_FLOW_SECURITY_RULE_INIT;
    bench->rules[i].effect = TURBO_FLOW_SECURITY_DENY;
    bench->rules[i].subject_kind = TURBO_FLOW_SECURITY_SUBJECT_ROLE;
    flowie_bench_copy(bench->rules[i].subject, sizeof(bench->rules[i].subject), "writer");
    flowie_bench_copy(bench->rules[i].root_group_id, sizeof(bench->rules[i].root_group_id),
                      "root-a");
    bench->rules[i].action_mask =
        TURBO_FLOW_SECURITY_ACTION_PUBLISH | TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
    bench->rules[i].resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
    bench->rules[i].match_kind = TURBO_FLOW_SECURITY_MATCH_ADAPTER;
    flowie_bench_copy(bench->rules[i].pattern, sizeof(bench->rules[i].pattern), pattern);
  }
  bench->rules[rule_count - 1u].effect = TURBO_FLOW_SECURITY_ALLOW;
  flowie_bench_copy(bench->rules[rule_count - 1u].pattern,
                    sizeof(bench->rules[rule_count - 1u].pattern), "root-a/target/events/#");
  rc = flowie_mqtt_security_matcher_init(&matcher);
  if (rc != TURBO_OK) return rc;
  config.resource_uid = "security:flowie-benchmark";
  config.owner_name = "flowie.security-benchmark";
  config.policy_version = 1u;
  config.rules = bench->rules;
  config.rule_count = rule_count;
  config.matcher = matcher;
  rc = turbo_flow_security_realm_create(&config, &bench->realm);
  if (rc != TURBO_OK) return rc;
  flowie_bench_copy(bench->principal.principal_id, sizeof(bench->principal.principal_id),
                    "device-1");
  flowie_bench_copy(bench->principal.principal_type, sizeof(bench->principal.principal_type),
                    "device");
  flowie_bench_copy(bench->principal.root_group_id, sizeof(bench->principal.root_group_id),
                    "root-a");
  flowie_bench_copy(bench->principal.auth_method, sizeof(bench->principal.auth_method), "token");
  bench->principal.scope = TURBO_FLOW_SECURITY_SCOPE_ROOT_GROUP;
  bench->principal.role_count = 1u;
  flowie_bench_copy(bench->principal.roles[0], sizeof(bench->principal.roles[0]), "writer");
  bench->principal.group_count = 1u;
  flowie_bench_copy(bench->principal.groups[0], sizeof(bench->principal.groups[0]), "root-a");
  bench->principal.policy_version = 1u;
  bench->request.principal = &bench->principal;
  bench->request.root_group_id = "root-a";
  bench->request.resource_type = TURBO_FLOW_SECURITY_RESOURCE_MQTT_TOPIC;
  return TURBO_OK;
}

static void flowie_bench_security_destroy(flowie_bench_security_t *bench) {
  if (!bench) return;
  turbo_flow_security_realm_destroy(bench->realm);
  free(bench->rules);
  memset(bench, 0, sizeof(*bench));
}

static void flowie_bench_security_matcher(size_t rule_count) {
  flowie_bench_security_t bench;
  flowie_mqtt_validated_security_context_t validated_context =
      FLOWIE_MQTT_VALIDATED_SECURITY_CONTEXT_INIT;
  turbo_flow_security_decision_t decision = TURBO_FLOW_SECURITY_DECISION_INIT;
  tstr_t validated_resource = NULL;
  char label[96];
  int rc = flowie_bench_security_init(&bench, rule_count);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) {
    flowie_bench_security_destroy(&bench);
    return;
  }
  bench.request.action = TURBO_FLOW_SECURITY_ACTION_PUBLISH;
  bench.request.resource = "root-a/target/events/temperature";
  rc = turbo_flow_security_realm_authorize(bench.realm, &bench.request, 1u, &decision);
  check_int_eq(rc, TURBO_OK);
  check_int_eq(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
  (void)snprintf(label, sizeof(label), "MQTT publish compiled ACL rules=%zu", rule_count);
  benchmark_ops(label, FLOWIE_BENCH_SECURITY_SAMPLES, FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE) {
    for (size_t operation = 0u;
         rc == TURBO_OK && operation < FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE; ++operation)
      rc = turbo_flow_security_realm_authorize(bench.realm, &bench.request, 1u, &decision);
  }
  check_int_eq(rc, TURBO_OK);

  validated_resource = tstr_new_len(bench.request.resource, strlen(bench.request.resource));
  check_not_null(validated_resource);
  if (validated_resource) {
    rc = flowie_mqtt_validated_security_context_init(&validated_context, FLOWIE_MQTT_SECURITY_TOPIC,
                                                     validated_resource);
    check_int_eq(rc, TURBO_OK);
    bench.request.resource = validated_resource;
    bench.request.protocol_context = &validated_context;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    (void)snprintf(label, sizeof(label), "MQTT publish parser-validated evaluate rules=%zu",
                   rule_count);
    benchmark_ops(label, FLOWIE_BENCH_SECURITY_SAMPLES, FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE) {
      for (size_t operation = 0u;
           rc == TURBO_OK && operation < FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE; ++operation)
        rc = turbo_flow_security_realm_evaluate(bench.realm, &bench.request, 1u, &decision);
    }
    check_int_eq(rc, TURBO_OK);
    check_int_eq(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    (void)snprintf(label, sizeof(label), "MQTT publish parser-validated ACL rules=%zu", rule_count);
    benchmark_ops(label, FLOWIE_BENCH_SECURITY_SAMPLES, FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE) {
      for (size_t operation = 0u;
           rc == TURBO_OK && operation < FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE; ++operation)
        rc = turbo_flow_security_realm_authorize(bench.realm, &bench.request, 1u, &decision);
    }
    check_int_eq(rc, TURBO_OK);
    check_int_eq(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
  }
  tstr_freep(&validated_resource);

  bench.protocol_context.kind = FLOWIE_MQTT_SECURITY_TOPIC_FILTER;
  bench.request.action = TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE;
  bench.request.resource = "root-a/target/events/+";
  bench.request.protocol_context = &bench.protocol_context;
  decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
  rc = turbo_flow_security_realm_authorize(bench.realm, &bench.request, 1u, &decision);
  check_int_eq(rc, TURBO_OK);
  check_int_eq(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
  (void)snprintf(label, sizeof(label), "MQTT subscribe containment ACL rules=%zu", rule_count);
  benchmark_ops(label, FLOWIE_BENCH_SECURITY_SAMPLES, FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE) {
    for (size_t operation = 0u;
         rc == TURBO_OK && operation < FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE; ++operation)
      rc = turbo_flow_security_realm_authorize(bench.realm, &bench.request, 1u, &decision);
  }
  check_int_eq(rc, TURBO_OK);

  validated_resource = tstr_new_len(bench.request.resource, strlen(bench.request.resource));
  check_not_null(validated_resource);
  if (validated_resource) {
    rc = flowie_mqtt_validated_security_context_init(
        &validated_context, FLOWIE_MQTT_SECURITY_TOPIC_FILTER, validated_resource);
    check_int_eq(rc, TURBO_OK);
    bench.request.resource = validated_resource;
    bench.request.protocol_context = &validated_context;
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    (void)snprintf(label, sizeof(label), "MQTT subscribe parser-validated evaluate rules=%zu",
                   rule_count);
    benchmark_ops(label, FLOWIE_BENCH_SECURITY_SAMPLES, FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE) {
      for (size_t operation = 0u;
           rc == TURBO_OK && operation < FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE; ++operation)
        rc = turbo_flow_security_realm_evaluate(bench.realm, &bench.request, 1u, &decision);
    }
    check_int_eq(rc, TURBO_OK);
    check_int_eq(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
    decision = (turbo_flow_security_decision_t)TURBO_FLOW_SECURITY_DECISION_INIT;
    (void)snprintf(label, sizeof(label), "MQTT subscribe parser-validated ACL rules=%zu",
                   rule_count);
    benchmark_ops(label, FLOWIE_BENCH_SECURITY_SAMPLES, FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE) {
      for (size_t operation = 0u;
           rc == TURBO_OK && operation < FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE; ++operation)
        rc = turbo_flow_security_realm_authorize(bench.realm, &bench.request, 1u, &decision);
    }
    check_int_eq(rc, TURBO_OK);
    check_int_eq(decision.effect, TURBO_FLOW_SECURITY_ALLOW);
  }
  tstr_freep(&validated_resource);
  flowie_bench_security_destroy(&bench);
}

static void flowie_bench_security_cost_breakdown(void) {
  static const char publish_topic[] = "root-a/target/events/temperature";
  static const char subscribe_filter[] = "root-a/target/events/+";
  flowie_bench_security_t bench;
  flowie_bench_security_visit_t visit = {0u};
  flowie_topic_index_t topics;
  flowie_mqtt_span_t publish = flowie_bench_span(publish_topic);
  flowie_mqtt_span_t subscribe = flowie_bench_span(subscribe_filter);
  int valid = 1;
  int rc = flowie_bench_security_init(&bench, TURBO_FLOW_SECURITY_MAX_RULES);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) {
    flowie_bench_security_destroy(&bench);
    return;
  }
  memset(&topics, 0, sizeof(topics));
  rc = flowie_topic_index_init(&topics);
  for (size_t i = 0u; rc == TURBO_OK && i < TURBO_FLOW_SECURITY_MAX_RULES; ++i) {
    rc = flowie_topic_index_insert(&topics, flowie_bench_span(bench.rules[i].pattern), i);
  }
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;

  benchmark_ops("MQTT publish topic validation", FLOWIE_BENCH_SECURITY_SAMPLES,
                FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE) {
    for (size_t operation = 0u; operation < FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE; ++operation)
      valid = valid && flowie_mqtt_topic_name_validate(publish);
  }
  check_true(valid);
  benchmark_ops("MQTT subscribe filter validation", FLOWIE_BENCH_SECURITY_SAMPLES,
                FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE) {
    for (size_t operation = 0u; operation < FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE; ++operation)
      valid = valid && flowie_mqtt_topic_filter_validate(subscribe);
  }
  check_true(valid);

  benchmark_ops("MQTT publish trie visitor with validation", FLOWIE_BENCH_SECURITY_SAMPLES,
                FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE) {
    for (size_t operation = 0u;
         rc == TURBO_OK && operation < FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE; ++operation)
      rc = flowie_topic_index_visit_topic(&topics, publish, flowie_bench_security_visit, &visit);
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(visit.count,
                FLOWIE_BENCH_SECURITY_SAMPLES * FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE);
  visit.count = 0u;
  benchmark_ops("MQTT publish validated trie traversal", FLOWIE_BENCH_SECURITY_SAMPLES,
                FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE) {
    for (size_t operation = 0u;
         rc == TURBO_OK && operation < FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE; ++operation)
      rc = flowie_topic_index_visit_validated_topic(&topics, publish, flowie_bench_security_visit,
                                                    &visit);
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(visit.count,
                FLOWIE_BENCH_SECURITY_SAMPLES * FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE);
  visit.count = 0u;
  benchmark_ops("MQTT subscribe trie visitor with validation", FLOWIE_BENCH_SECURITY_SAMPLES,
                FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE) {
    for (size_t operation = 0u;
         rc == TURBO_OK && operation < FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE; ++operation)
      rc = flowie_topic_index_visit_containing_filters(&topics, subscribe,
                                                       flowie_bench_security_visit, &visit);
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(visit.count,
                FLOWIE_BENCH_SECURITY_SAMPLES * FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE);
  visit.count = 0u;
  benchmark_ops("MQTT subscribe validated trie traversal", FLOWIE_BENCH_SECURITY_SAMPLES,
                FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE) {
    for (size_t operation = 0u;
         rc == TURBO_OK && operation < FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE; ++operation)
      rc = flowie_topic_index_visit_validated_containing_filters(
          &topics, subscribe, flowie_bench_security_visit, &visit);
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(visit.count,
                FLOWIE_BENCH_SECURITY_SAMPLES * FLOWIE_BENCH_SECURITY_OPS_PER_SAMPLE);

done:
  flowie_topic_index_destroy(&topics);
  flowie_bench_security_destroy(&bench);
}

static int flowie_bench_filter(char *out, size_t capacity, size_t index) {
  int written;
  switch (index % 4u) {
  case 0u:
    written = snprintf(out, capacity, "root/%zu/device/+/state", index);
    break;
  case 1u:
    written = snprintf(out, capacity, "root/%zu/#", index);
    break;
  case 2u:
    written =
        snprintf(out, capacity, "$share/group%zu/root/%zu/device/+/state", index % 64u, index);
    break;
  default:
    written = snprintf(out, capacity, "root/%zu/device/%zu/state", index, index);
    break;
  }
  return written > 0 && (size_t)written < capacity ? TURBO_OK : TURBO_EMSGSIZE;
}

static void flowie_bench_topic_index(void) {
  flowie_topic_index_t index;
  turbo_vec_t filters;
  turbo_vec_t matches;
  flowie_topic_index_binding_t *bindings = NULL;
  uint64_t *latencies = NULL;
  uint64_t build_begin;
  uint64_t build_elapsed;
  uint64_t match_begin;
  uint64_t match_elapsed;
  uint64_t remove_begin;
  uint64_t remove_elapsed;
  size_t total_matches = 0u;
  int rc = TURBO_OK;
  memset(&index, 0, sizeof(index));
  check_int_eq(turbo_vec_init(&filters, sizeof(tstr_t)), TURBO_OK);
  check_int_eq(turbo_vec_init(&matches, sizeof(size_t)), TURBO_OK);
  check_int_eq(flowie_topic_index_init(&index), TURBO_OK);
  latencies = (uint64_t *)calloc(FLOWIE_BENCH_CAPACITY, sizeof(*latencies));
  bindings = (flowie_topic_index_binding_t *)calloc(FLOWIE_BENCH_CAPACITY, sizeof(*bindings));
  check_not_null(latencies);
  check_not_null(bindings);
  if (!latencies || !bindings) goto done;

  build_begin = turbo_hrtime();
  for (size_t i = 0u; i < FLOWIE_BENCH_CAPACITY; ++i) {
    char text[FLOWIE_BENCH_TOPIC_BUFFER_SIZE];
    tstr_t filter;
    rc = flowie_bench_filter(text, sizeof(text), i);
    if (rc != TURBO_OK) break;
    filter = tstr_new_len(text, strlen(text));
    if (!filter) {
      rc = TURBO_ENOMEM;
      break;
    }
    rc = turbo_vec_push(&filters, &filter);
    if (rc != TURBO_OK) {
      tstr_free(filter);
      break;
    }
    rc = flowie_topic_index_insert_bound(&index, flowie_bench_span(filter), i, &bindings[i]);
    if (rc != TURBO_OK) break;
  }
  build_elapsed = turbo_hrtime() - build_begin;
  check_int_eq(rc, TURBO_OK);
  check_size_eq(turbo_vec_size(&filters), FLOWIE_BENCH_CAPACITY);
  if (rc != TURBO_OK) goto done;
  printf("FLOWIE_BENCH_RESULT operation=topic_index_build filters=%u elapsed_ns=%" PRIu64
         " throughput_filter_s=%.2f\n",
         FLOWIE_BENCH_CAPACITY, build_elapsed,
         build_elapsed ? ((double)FLOWIE_BENCH_CAPACITY * 1000000000.0) / (double)build_elapsed
                       : 0.0);

  match_begin = turbo_hrtime();
  for (size_t i = 0u; i < FLOWIE_BENCH_CAPACITY; ++i) {
    char topic[FLOWIE_BENCH_TOPIC_BUFFER_SIZE];
    uint64_t begin;
    const int written = snprintf(topic, sizeof(topic), "root/%zu/device/%zu/state", i, i);
    if (written <= 0 || (size_t)written >= sizeof(topic)) {
      rc = TURBO_EMSGSIZE;
      break;
    }
    turbo_vec_clear(&matches);
    begin = turbo_hrtime();
    rc = flowie_topic_index_match(&index, flowie_bench_span(topic), &matches);
    latencies[i] = turbo_hrtime() - begin;
    if (rc != TURBO_OK) break;
    total_matches += turbo_vec_size(&matches);
  }
  match_elapsed = turbo_hrtime() - match_begin;
  check_int_eq(rc, TURBO_OK);
  check_size_gt(total_matches, 0u);
  if (rc == TURBO_OK) {
    qsort(latencies, FLOWIE_BENCH_CAPACITY, sizeof(*latencies), flowie_bench_u64_compare);
    printf(
        "FLOWIE_BENCH_RESULT operation=topic_index_match filters=%u matches=%zu elapsed_ns=%" PRIu64
        " throughput_match_s=%.2f p50_ns=%" PRIu64 " p95_ns=%" PRIu64 " p99_ns=%" PRIu64 "\n",
        FLOWIE_BENCH_CAPACITY, total_matches, match_elapsed,
        match_elapsed ? ((double)FLOWIE_BENCH_CAPACITY * 1000000000.0) / (double)match_elapsed
                      : 0.0,
        flowie_bench_percentile(latencies, FLOWIE_BENCH_CAPACITY, 50u),
        flowie_bench_percentile(latencies, FLOWIE_BENCH_CAPACITY, 95u),
        flowie_bench_percentile(latencies, FLOWIE_BENCH_CAPACITY, 99u));
  }

  remove_begin = turbo_hrtime();
  for (size_t i = FLOWIE_BENCH_CAPACITY; rc == TURBO_OK && i > 0u; --i) {
    size_t moved = FLOWIE_TOPIC_INDEX_NO_ENTRY;
    rc = flowie_topic_index_remove(&index, &bindings[i - 1u], i - 1u, &moved);
    if (rc == TURBO_OK && moved != FLOWIE_TOPIC_INDEX_NO_ENTRY) rc = TURBO_EPROTO;
  }
  remove_elapsed = turbo_hrtime() - remove_begin;
  check_int_eq(rc, TURBO_OK);
  if (rc == TURBO_OK) {
    printf("FLOWIE_BENCH_RESULT operation=topic_index_remove filters=%u elapsed_ns=%" PRIu64
           " throughput_filter_s=%.2f\n",
           FLOWIE_BENCH_CAPACITY, remove_elapsed,
           remove_elapsed ? ((double)FLOWIE_BENCH_CAPACITY * 1000000000.0) / (double)remove_elapsed
                          : 0.0);
  }

done:
  free(bindings);
  free(latencies);
  for (size_t i = 0u; i < turbo_vec_size(&filters); ++i) {
    tstr_t *filter = (tstr_t *)turbo_vec_at(&filters, i);
    if (filter) tstr_freep(filter);
  }
  flowie_topic_index_destroy(&index);
  turbo_vec_destroy(&matches);
  turbo_vec_destroy(&filters);
}

static flowie_mqtt_span_t flowie_bench_candidate_filter(size_t index) {
  static const char *const filters[] = {"root/common/device/+/state", "root/common/#",
                                        "$share/workers/root/common/device/+/state",
                                        "root/common/device/42/state"};
  return flowie_bench_span(filters[index % (sizeof(filters) / sizeof(filters[0]))]);
}

static void flowie_bench_topic_rebuild_fanout(void) {
  static const char topic[] = "root/common/device/42/state";
  flowie_topic_index_t index;
  turbo_vec_t matches;
  uint64_t rebuild_latencies[FLOWIE_BENCH_REBUILD_SAMPLES] = {0};
  uint64_t match_latencies[FLOWIE_BENCH_CANDIDATE_MATCH_SAMPLES] = {0};
  size_t rebuild_sample = 0u;
  size_t match_sample = 0u;
  int rc;
  memset(&index, 0, sizeof(index));
  check_int_eq(turbo_vec_init(&matches, sizeof(size_t)), TURBO_OK);
  rc = flowie_topic_index_init(&index);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) {
    turbo_vec_destroy(&matches);
    return;
  }

  benchmark_ops("100k wildcard/shared index rebuild", FLOWIE_BENCH_REBUILD_SAMPLES,
                FLOWIE_BENCH_CAPACITY) {
    const uint64_t begin = turbo_hrtime();
    if (rc == TURBO_OK) {
      flowie_topic_index_destroy(&index);
      rc = flowie_topic_index_init(&index);
      for (size_t i = 0u; rc == TURBO_OK && i < FLOWIE_BENCH_CAPACITY; ++i)
        rc = flowie_topic_index_insert(&index, flowie_bench_candidate_filter(i), i);
      rebuild_latencies[rebuild_sample] = turbo_hrtime() - begin;
    }
    ++rebuild_sample;
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(rebuild_sample, FLOWIE_BENCH_REBUILD_SAMPLES);
  if (rc == TURBO_OK) {
    qsort(rebuild_latencies, FLOWIE_BENCH_REBUILD_SAMPLES, sizeof(*rebuild_latencies),
          flowie_bench_u64_compare);
    printf("FLOWIE_BENCH_RESULT operation=topic_index_rebuild filters=%u samples=%u p50_ns=%" PRIu64
           " p95_ns=%" PRIu64 " p99_ns=%" PRIu64 "\n",
           FLOWIE_BENCH_CAPACITY, FLOWIE_BENCH_REBUILD_SAMPLES,
           flowie_bench_percentile(rebuild_latencies, FLOWIE_BENCH_REBUILD_SAMPLES, 50u),
           flowie_bench_percentile(rebuild_latencies, FLOWIE_BENCH_REBUILD_SAMPLES, 95u),
           flowie_bench_percentile(rebuild_latencies, FLOWIE_BENCH_REBUILD_SAMPLES, 99u));
  }

  benchmark_ops("100k-candidate wildcard/shared match", FLOWIE_BENCH_CANDIDATE_MATCH_SAMPLES,
                FLOWIE_BENCH_CAPACITY) {
    const uint64_t begin = turbo_hrtime();
    if (rc == TURBO_OK) {
      turbo_vec_clear(&matches);
      rc = flowie_topic_index_match(&index, flowie_bench_span(topic), &matches);
      match_latencies[match_sample] = turbo_hrtime() - begin;
      if (rc == TURBO_OK && turbo_vec_size(&matches) != FLOWIE_BENCH_CAPACITY) rc = TURBO_EPROTO;
    }
    ++match_sample;
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(match_sample, FLOWIE_BENCH_CANDIDATE_MATCH_SAMPLES);
  check_size_eq(turbo_vec_size(&matches), FLOWIE_BENCH_CAPACITY);
  if (rc == TURBO_OK) {
    qsort(match_latencies, FLOWIE_BENCH_CANDIDATE_MATCH_SAMPLES, sizeof(*match_latencies),
          flowie_bench_u64_compare);
    printf("FLOWIE_BENCH_RESULT operation=topic_index_candidate_fanout candidates=%u samples=%u "
           "p50_ns=%" PRIu64 " p95_ns=%" PRIu64 " p99_ns=%" PRIu64 "\n",
           FLOWIE_BENCH_CAPACITY, FLOWIE_BENCH_CANDIDATE_MATCH_SAMPLES,
           flowie_bench_percentile(match_latencies, FLOWIE_BENCH_CANDIDATE_MATCH_SAMPLES, 50u),
           flowie_bench_percentile(match_latencies, FLOWIE_BENCH_CANDIDATE_MATCH_SAMPLES, 95u),
           flowie_bench_percentile(match_latencies, FLOWIE_BENCH_CANDIDATE_MATCH_SAMPLES, 99u));
  }

  flowie_topic_index_destroy(&index);
  turbo_vec_destroy(&matches);
}

static void flowie_bench_sessions(void) {
  turbo_vec_t owners;
  uint64_t begin;
  uint64_t elapsed;
  int rc = TURBO_OK;
  check_int_eq(turbo_vec_init(&owners, sizeof(flowie_session_owner_t *)), TURBO_OK);
  begin = turbo_hrtime();
  for (size_t i = 0u; i < FLOWIE_BENCH_CAPACITY; ++i) {
    char client_id[FLOWIE_BENCH_TOPIC_BUFFER_SIZE];
    flowie_session_config_t config = FLOWIE_SESSION_CONFIG_INIT;
    flowie_mqtt_connect_view_t connect = FLOWIE_MQTT_CONNECT_VIEW_INIT;
    flowie_session_connect_result_t decision = FLOWIE_SESSION_CONNECT_RESULT_INIT;
    flowie_session_owner_t *owner;
    const int written = snprintf(client_id, sizeof(client_id), "bench-client-%zu", i);
    if (written <= 0 || (size_t)written >= sizeof(client_id)) {
      rc = TURBO_EMSGSIZE;
      break;
    }
    config.owner_instance_id = 1u;
    config.session_id = i + 1u;
    config.max_subscriptions = 1u;
    config.max_inflight = 1u;
    owner = flowie_session_owner_create(&config);
    if (!owner) {
      rc = TURBO_ENOMEM;
      break;
    }
    connect.version = FLOWIE_MQTT_VERSION_5;
    connect.properties = (flowie_mqtt_property_block_view_t)FLOWIE_MQTT_PROPERTY_BLOCK_VIEW_INIT;
    connect.client_id = flowie_bench_span(client_id);
    connect.keep_alive = 60u;
    rc = flowie_session_owner_connect(owner, &connect, &decision);
    if (rc == TURBO_OK && !decision.accepted) rc = TURBO_EPROTO;
    if (rc == TURBO_OK) rc = turbo_vec_push(&owners, &owner);
    if (rc != TURBO_OK) {
      flowie_session_owner_destroy(owner);
      break;
    }
  }
  elapsed = turbo_hrtime() - begin;
  check_int_eq(rc, TURBO_OK);
  check_size_eq(turbo_vec_size(&owners), FLOWIE_BENCH_CAPACITY);
  if (rc == TURBO_OK) {
    printf("FLOWIE_BENCH_RESULT operation=session_create_connect sessions=%u elapsed_ns=%" PRIu64
           " throughput_session_s=%.2f\n",
           FLOWIE_BENCH_CAPACITY, elapsed,
           elapsed ? ((double)FLOWIE_BENCH_CAPACITY * 1000000000.0) / (double)elapsed : 0.0);
  }
  for (size_t i = 0u; i < turbo_vec_size(&owners); ++i) {
    flowie_session_owner_t **owner = (flowie_session_owner_t **)turbo_vec_at(&owners, i);
    if (owner) flowie_session_owner_destroy(*owner);
  }
  turbo_vec_destroy(&owners);
}

static turbo_flow_t *flowie_bench_fanout_flow_with_limits(unsigned short port,
                                                          uint32_t max_connections,
                                                          size_t max_packet_size,
                                                          size_t send_hwm_bytes) {
  static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                              "stage mqtt_fanout adapter flowie.endpoint\n"
                              "stage main {\n"
                              "  mqtt_in -> mqtt_fanout\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow) return NULL;
  config.host = "127.0.0.1";
  config.port = (int)port;
  config.max_packet_size = max_packet_size;
  config.max_connections = max_connections;
  config.recv_timeout_ms = 5000u;
  config.send_hwm_bytes = send_hwm_bytes;
  config.manage_sessions = 1;
  config.max_sessions = max_connections;
  config.max_subscriptions_per_session = 1u;
  config.max_inflight_per_session = 1u;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *flowie_bench_fanout_flow(unsigned short port) {
  return flowie_bench_fanout_flow_with_limits(port, FLOWIE_BENCH_FANOUT_SUBSCRIBERS + 1u, 4096u,
                                              8u * 1024u * 1024u);
}

static turbo_flow_t *flowie_bench_live_fanout_flow(unsigned short port, size_t connections) {
  static const char graph[] = "source mqtt_in adapter flowie.endpoint\n"
                              "stage mqtt_fanout adapter flowie.endpoint\n"
                              "stage main {\n"
                              "  mqtt_in -> mqtt_fanout\n"
                              "}\n";
  flowie_endpoint_config_t config = FLOWIE_ENDPOINT_CONFIG_INIT;
  turbo_flow_t *flow;
  if (connections == 0u || connections > UINT32_MAX) return NULL;
  flow = turbo_flow_create();
  if (!flow) return NULL;
  config.host = "0.0.0.0";
  config.port = (int)port;
  config.max_packet_size = 256u;
  config.max_connections = (uint32_t)connections;
  config.coroutine_stack_size = FLOWIE_BENCH_LIVE_COROUTINE_STACK_SIZE;
  config.recv_buffer_size = FLOWIE_DEFAULT_RECV_BUFFER_SIZE;
  /* Capacity setup is intentionally serial and may exceed ordinary idle
   * timeouts before the final subscriber is admitted. Keep live sessions
   * open so this benchmark measures concurrent capacity and fan-out. */
  config.recv_timeout_ms = 0u;
  config.send_hwm_bytes = 256u;
  config.manage_sessions = 1;
  config.max_sessions = connections;
  config.max_subscriptions_per_session = 1u;
  config.max_inflight_per_session = 1u;
  if (flowie_register_endpoint(flow, "flowie.endpoint", &config) != TURBO_OK ||
      turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u) != TURBO_OK ||
      turbo_flow_compile(flow) != TURBO_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static int flowie_bench_connect(flowie_test_socket_t socket_handle, const uint8_t client_id[3]) {
  static const uint8_t connect_template[] = {0x10u, 0x10u, 0x00u, 0x04u, 'M',   'Q',
                                             'T',   'T',   0x05u, 0x02u, 0x00u, 0x3cu,
                                             0x00u, 0x00u, 0x03u, 0x00u, 0x00u, 0x00u};
  static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
  uint8_t packet[sizeof(connect_template)];
  uint8_t reply[sizeof(connack)];
  int rc;
  memcpy(packet, connect_template, sizeof(packet));
  memcpy(packet + sizeof(packet) - 3u, client_id, 3u);
  rc = flowie_test_send(socket_handle, packet, sizeof(packet));
  if (rc == TURBO_OK) rc = flowie_test_recv_exact(socket_handle, reply, sizeof(reply));
  return rc == TURBO_OK && memcmp(reply, connack, sizeof(reply)) == 0 ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_bench_connect_live(flowie_test_socket_t socket_handle,
                                     const uint8_t client_id[8]) {
  static const uint8_t connect_template[] = {0x10u, 0x15u, 0x00u, 0x04u, 'M',   'Q',   'T',   'T',
                                             0x05u, 0x02u, 0x00u, 0x3cu, 0x00u, 0x00u, 0x08u, 0x00u,
                                             0x00u, 0x00u, 0x00u, 0x00u, 0x00u, 0x00u, 0x00u};
  static const uint8_t connack[] = {0x20u, 0x03u, 0x00u, 0x00u, 0x00u};
  uint8_t packet[sizeof(connect_template)];
  uint8_t reply[sizeof(connack)];
  int rc;
  memcpy(packet, connect_template, sizeof(packet));
  memcpy(packet + sizeof(packet) - 8u, client_id, 8u);
  rc = flowie_test_send(socket_handle, packet, sizeof(packet));
  if (rc == TURBO_OK) rc = flowie_test_recv_exact(socket_handle, reply, sizeof(reply));
  return rc == TURBO_OK && memcmp(reply, connack, sizeof(reply)) == 0 ? TURBO_OK : TURBO_EPROTO;
}

static void flowie_bench_client_id(size_t index, uint8_t output[8]) {
  static const uint8_t digits[] = "0123456789abcdef";
  for (size_t i = 0u; i < 8u; ++i) {
    const size_t shift = (7u - i) * 4u;
    output[i] = digits[(index >> shift) & 0x0fu];
  }
}

static flowie_test_socket_t flowie_bench_connect_alias(unsigned short port, size_t index) {
  struct sockaddr_in source;
  struct sockaddr_in address;
  const size_t alias =
      (index / FLOWIE_BENCH_LIVE_TCP_CONNECTIONS_PER_ALIAS) % FLOWIE_BENCH_LIVE_TCP_ALIASES;
  const uint32_t source_address =
      UINT32_C(0x7f010001) + ((uint32_t)(port % 200u) << 8u) + (uint32_t)alias;
  const unsigned short source_port =
      (unsigned short)(FLOWIE_BENCH_LIVE_TCP_SOURCE_PORT_BASE +
                       index % FLOWIE_BENCH_LIVE_TCP_CONNECTIONS_PER_ALIAS);
  flowie_test_socket_t socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket_handle == FLOWIE_TEST_INVALID_SOCKET) return FLOWIE_TEST_INVALID_SOCKET;
  memset(&source, 0, sizeof(source));
  source.sin_family = AF_INET;
  source.sin_port = htons(source_port);
  source.sin_addr.s_addr = htonl(source_address);
  if (bind(socket_handle, (struct sockaddr *)&source, sizeof(source)) != 0) {
    flowie_test_socket_close(socket_handle);
    return FLOWIE_TEST_INVALID_SOCKET;
  }
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(UINT32_C(0x7f000001));
  if (connect(socket_handle, (struct sockaddr *)&address, sizeof(address)) != 0) {
    flowie_test_socket_close(socket_handle);
    return FLOWIE_TEST_INVALID_SOCKET;
  }
  return socket_handle;
}

static int flowie_bench_wait_connections_with_timeout(turbo_flow_t *flow, size_t expected,
                                                      uint64_t timeout_ns) {
  turbo_flow_connection_snapshot_t snapshot = {0};
  uint64_t begin = turbo_hrtime();
  int rc = TURBO_OK;
  while (turbo_hrtime() - begin < timeout_ns) {
    rc = turbo_flow_adapter_connection_snapshot_at(flow, 0u, &snapshot);
    if (rc != TURBO_OK || snapshot.connections_current == expected) return rc;
    turbo_thread_yield();
  }
  return TURBO_ETIMEDOUT;
}

static int flowie_bench_wait_connections(turbo_flow_t *flow, size_t expected) {
  return flowie_bench_wait_connections_with_timeout(flow, expected, FLOWIE_BENCH_WAIT_TIMEOUT_NS);
}

static void flowie_bench_tcp_churn(void) {
  turbo_flow_t *flow = NULL;
  uint64_t *latencies = NULL;
  unsigned short port = flowie_test_port();
  size_t sample_index = 0u;
  int rc = TURBO_OK;
  check_int_gt(port, 0);
  flow = flowie_bench_fanout_flow(port);
  check_not_null(flow);
  if (!flow) return;
  check_int_eq(turbo_flow_start(flow), TURBO_OK);
  latencies = (uint64_t *)calloc(FLOWIE_BENCH_CHURN_SAMPLES, sizeof(*latencies));
  check_not_null(latencies);
  if (!latencies) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  benchmark_batch("TCP and MQTT connect-close churn", FLOWIE_BENCH_CHURN_SAMPLES) {
    uint64_t begin = turbo_hrtime();
    flowie_test_socket_t client = FLOWIE_TEST_INVALID_SOCKET;
    if (rc == TURBO_OK) {
      client = flowie_test_connect(port);
      rc = client == FLOWIE_TEST_INVALID_SOCKET
               ? TURBO_ENOTCONN
               : flowie_bench_connect(client, (const uint8_t *)"chr");
      flowie_test_socket_close(client);
      if (rc == TURBO_OK) rc = flowie_bench_wait_connections(flow, 0u);
      latencies[sample_index] = turbo_hrtime() - begin;
    }
    ++sample_index;
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(sample_index, FLOWIE_BENCH_CHURN_SAMPLES);
  if (rc == TURBO_OK) {
    qsort(latencies, FLOWIE_BENCH_CHURN_SAMPLES, sizeof(*latencies), flowie_bench_u64_compare);
    printf("FLOWIE_BENCH_RESULT operation=tcp_connection_churn samples=%u p50_ns=%" PRIu64
           " p95_ns=%" PRIu64 " p99_ns=%" PRIu64 "\n",
           FLOWIE_BENCH_CHURN_SAMPLES,
           flowie_bench_percentile(latencies, FLOWIE_BENCH_CHURN_SAMPLES, 50u),
           flowie_bench_percentile(latencies, FLOWIE_BENCH_CHURN_SAMPLES, 95u),
           flowie_bench_percentile(latencies, FLOWIE_BENCH_CHURN_SAMPLES, 99u));
  }

done:
  free(latencies);
  if (flow) check_int_eq(turbo_flow_stop(flow), TURBO_OK);
  turbo_flow_destroy(flow);
}

static void flowie_bench_tcp_fanout(void) {
  static const uint8_t subscribe[] = {0x82u, 0x0du, 0x00u, 0x01u, 0x00u, 0x00u, 0x07u, 'b',
                                      'e',   'n',   'c',   'h',   '/',   '#',   0x00u};
  static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
  static const uint8_t publish_template[] = {0x30u, 0x12u, 0x00u, 0x0bu, 'b',   'e',  'n',
                                             'c',   'h',   '/',   't',   'o',   'p',  'i',
                                             'c',   0x00u, 0x00u, 0x00u, 0x00u, 0x00u};
  static const uint8_t digits[] = "0123456789abcdef";
  flowie_test_socket_t subscribers[FLOWIE_BENCH_FANOUT_SUBSCRIBERS];
  flowie_test_socket_t publisher = FLOWIE_TEST_INVALID_SOCKET;
  turbo_flow_t *flow = NULL;
  uint64_t *latencies = NULL;
  uint8_t packet[sizeof(publish_template)];
  uint8_t received[sizeof(publish_template)];
  uint8_t reply[sizeof(suback)];
  unsigned short port = flowie_test_port();
  size_t sample_index = 0u;
  int rc = TURBO_OK;
  for (size_t i = 0u; i < FLOWIE_BENCH_FANOUT_SUBSCRIBERS; ++i)
    subscribers[i] = FLOWIE_TEST_INVALID_SOCKET;
  check_int_gt(port, 0);
  flow = flowie_bench_fanout_flow(port);
  check_not_null(flow);
  if (!flow) return;
  check_int_eq(turbo_flow_start(flow), TURBO_OK);
  publisher = flowie_test_connect(port);
  check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
  if (publisher == FLOWIE_TEST_INVALID_SOCKET) {
    rc = TURBO_ENOTCONN;
    goto done;
  }
  rc = flowie_bench_connect(publisher, (const uint8_t *)"pub");
  for (size_t i = 0u; rc == TURBO_OK && i < FLOWIE_BENCH_FANOUT_SUBSCRIBERS; ++i) {
    const uint8_t client_id[3] = {'s', digits[(i >> 4u) & 0x0fu], digits[i & 0x0fu]};
    subscribers[i] = flowie_test_connect(port);
    if (subscribers[i] == FLOWIE_TEST_INVALID_SOCKET) {
      rc = TURBO_ENOTCONN;
      break;
    }
    rc = flowie_bench_connect(subscribers[i], client_id);
    if (rc == TURBO_OK) rc = flowie_test_send(subscribers[i], subscribe, sizeof(subscribe));
    if (rc == TURBO_OK) rc = flowie_test_recv_exact(subscribers[i], reply, sizeof(reply));
    if (rc == TURBO_OK && memcmp(reply, suback, sizeof(reply)) != 0) rc = TURBO_EPROTO;
  }
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;
  latencies = (uint64_t *)calloc(FLOWIE_BENCH_FANOUT_SAMPLES, sizeof(*latencies));
  check_not_null(latencies);
  if (!latencies) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  memcpy(packet, publish_template, sizeof(packet));
  benchmark_io("TCP fan-out to 16 subscribers", FLOWIE_BENCH_FANOUT_SAMPLES,
               FLOWIE_BENCH_FANOUT_SUBSCRIBERS,
               sizeof(packet) * (FLOWIE_BENCH_FANOUT_SUBSCRIBERS + 1u)) {
    uint64_t begin = turbo_hrtime();
    if (rc == TURBO_OK) {
      packet[sizeof(packet) - 4u] = (uint8_t)(sample_index >> 24u);
      packet[sizeof(packet) - 3u] = (uint8_t)(sample_index >> 16u);
      packet[sizeof(packet) - 2u] = (uint8_t)(sample_index >> 8u);
      packet[sizeof(packet) - 1u] = (uint8_t)sample_index;
      rc = flowie_test_send(publisher, packet, sizeof(packet));
      for (size_t i = 0u; rc == TURBO_OK && i < FLOWIE_BENCH_FANOUT_SUBSCRIBERS; ++i) {
        rc = flowie_test_recv_exact(subscribers[i], received, sizeof(received));
        if (rc == TURBO_OK && memcmp(received, packet, sizeof(packet)) != 0) rc = TURBO_EPROTO;
      }
      latencies[sample_index] = turbo_hrtime() - begin;
    }
    ++sample_index;
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(sample_index, FLOWIE_BENCH_FANOUT_SAMPLES);
  if (rc == TURBO_OK) {
    qsort(latencies, FLOWIE_BENCH_FANOUT_SAMPLES, sizeof(*latencies), flowie_bench_u64_compare);
    printf("FLOWIE_BENCH_RESULT operation=tcp_fanout subscribers=%u samples=%u p50_ns=%" PRIu64
           " p95_ns=%" PRIu64 " p99_ns=%" PRIu64 "\n",
           FLOWIE_BENCH_FANOUT_SUBSCRIBERS, FLOWIE_BENCH_FANOUT_SAMPLES,
           flowie_bench_percentile(latencies, FLOWIE_BENCH_FANOUT_SAMPLES, 50u),
           flowie_bench_percentile(latencies, FLOWIE_BENCH_FANOUT_SAMPLES, 95u),
           flowie_bench_percentile(latencies, FLOWIE_BENCH_FANOUT_SAMPLES, 99u));
  }

done:
  free(latencies);
  for (size_t i = 0u; i < FLOWIE_BENCH_FANOUT_SUBSCRIBERS; ++i)
    flowie_test_socket_close(subscribers[i]);
  flowie_test_socket_close(publisher);
  if (flow) check_int_eq(turbo_flow_stop(flow), TURBO_OK);
  turbo_flow_destroy(flow);
}

static void flowie_bench_tcp_pipeline_burst(void) {
  static const uint8_t subscribe[] = {0x82u, 0x0du, 0x00u, 0x01u, 0x00u, 0x00u, 0x07u, 'b',
                                      'e',   'n',   'c',   'h',   '/',   '#',   0x00u};
  static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
  static const uint8_t publish_template[] = {0x30u, 0x12u, 0x00u, 0x0bu, 'b',   'e',  'n',
                                             'c',   'h',   '/',   't',   'o',   'p',  'i',
                                             'c',   0x00u, 0x00u, 0x00u, 0x00u, 0x00u};
  flowie_test_socket_t subscriber = FLOWIE_TEST_INVALID_SOCKET;
  flowie_test_socket_t publisher = FLOWIE_TEST_INVALID_SOCKET;
  turbo_flow_t *flow = NULL;
  uint64_t *latencies = NULL;
  uint8_t packets[FLOWIE_BENCH_PIPELINE_MESSAGES][sizeof(publish_template)];
  uint8_t received[sizeof(packets)];
  uint8_t reply[sizeof(suback)];
  unsigned short port = flowie_test_port();
  size_t sample_index = 0u;
  int rc = TURBO_OK;

  check_int_gt(port, 0);
  flow = flowie_bench_fanout_flow(port);
  check_not_null(flow);
  if (!flow) return;
  check_int_eq(turbo_flow_start(flow), TURBO_OK);
  publisher = flowie_test_connect(port);
  subscriber = flowie_test_connect(port);
  check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
  check_true(subscriber != FLOWIE_TEST_INVALID_SOCKET);
  if (publisher == FLOWIE_TEST_INVALID_SOCKET || subscriber == FLOWIE_TEST_INVALID_SOCKET) {
    rc = TURBO_ENOTCONN;
    goto done;
  }
  rc = flowie_bench_connect(publisher, (const uint8_t *)"pub");
  if (rc == TURBO_OK) rc = flowie_bench_connect(subscriber, (const uint8_t *)"sub");
  if (rc == TURBO_OK) rc = flowie_test_send(subscriber, subscribe, sizeof(subscribe));
  if (rc == TURBO_OK) rc = flowie_test_recv_exact(subscriber, reply, sizeof(reply));
  if (rc == TURBO_OK && memcmp(reply, suback, sizeof(reply)) != 0) rc = TURBO_EPROTO;
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;
  for (size_t i = 0u; i < FLOWIE_BENCH_PIPELINE_MESSAGES; ++i)
    memcpy(packets[i], publish_template, sizeof(publish_template));
  rc = flowie_test_send(publisher, &packets[0][0], sizeof(packets));
  if (rc == TURBO_OK) rc = flowie_test_recv_exact(subscriber, received, sizeof(received));
  check_int_eq(rc, TURBO_OK);
  check_mem_eq(received, packets, sizeof(packets));
  if (rc != TURBO_OK) goto done;

  latencies = (uint64_t *)calloc(FLOWIE_BENCH_PIPELINE_SAMPLES, sizeof(*latencies));
  check_not_null(latencies);
  if (!latencies) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  benchmark_io("TCP pipeline burst to one subscriber", FLOWIE_BENCH_PIPELINE_SAMPLES,
               FLOWIE_BENCH_PIPELINE_MESSAGES, sizeof(packets) * 2u) {
    uint64_t begin = turbo_hrtime();
    if (rc == TURBO_OK) {
      for (size_t i = 0u; i < FLOWIE_BENCH_PIPELINE_MESSAGES; ++i) {
        const uint32_t sequence = (uint32_t)(sample_index * FLOWIE_BENCH_PIPELINE_MESSAGES + i);
        packets[i][sizeof(publish_template) - 4u] = (uint8_t)(sequence >> 24u);
        packets[i][sizeof(publish_template) - 3u] = (uint8_t)(sequence >> 16u);
        packets[i][sizeof(publish_template) - 2u] = (uint8_t)(sequence >> 8u);
        packets[i][sizeof(publish_template) - 1u] = (uint8_t)sequence;
      }
      rc = flowie_test_send(publisher, &packets[0][0], sizeof(packets));
      if (rc == TURBO_OK) rc = flowie_test_recv_exact(subscriber, received, sizeof(received));
      if (rc == TURBO_OK && memcmp(received, packets, sizeof(packets)) != 0) rc = TURBO_EPROTO;
      latencies[sample_index] = turbo_hrtime() - begin;
    }
    ++sample_index;
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(sample_index, FLOWIE_BENCH_PIPELINE_SAMPLES);
  if (rc == TURBO_OK) {
    qsort(latencies, FLOWIE_BENCH_PIPELINE_SAMPLES, sizeof(*latencies), flowie_bench_u64_compare);
    printf("FLOWIE_BENCH_RESULT operation=tcp_pipeline_burst messages_per_sample=%u samples=%u"
           " p50_ns=%" PRIu64 " p95_ns=%" PRIu64 " p99_ns=%" PRIu64 "\n",
           FLOWIE_BENCH_PIPELINE_MESSAGES, FLOWIE_BENCH_PIPELINE_SAMPLES,
           flowie_bench_percentile(latencies, FLOWIE_BENCH_PIPELINE_SAMPLES, 50u),
           flowie_bench_percentile(latencies, FLOWIE_BENCH_PIPELINE_SAMPLES, 95u),
           flowie_bench_percentile(latencies, FLOWIE_BENCH_PIPELINE_SAMPLES, 99u));
  }

done:
  free(latencies);
  flowie_test_socket_close(subscriber);
  flowie_test_socket_close(publisher);
  if (flow) check_int_eq(turbo_flow_stop(flow), TURBO_OK);
  turbo_flow_destroy(flow);
}

static size_t flowie_bench_live_tcp_capacity(void) {
  const char *configured = getenv("FLOWIE_BENCH_LIVE_TCP_CONNECTIONS");
  char *end = NULL;
  unsigned long long value;
  if (!configured || configured[0] == '\0') return FLOWIE_BENCH_LIVE_TCP_CAPACITY;
  value = strtoull(configured, &end, 10);
  if (!end || *end != '\0' || value == 0u || value > FLOWIE_BENCH_LIVE_TCP_CAPACITY) return 0u;
  return (size_t)value;
}

/**
 * Real endpoint capacity proof. Setup, selector rebuild, fan-out and verification are O(N);
 * socket/session/subscription/reply state is O(N), bounded by the configured 100k maximum.
 */
static void flowie_bench_live_tcp_capacity_fanout(void) {
  static const uint8_t publisher_id[8] = {'p', 'u', 'b', '0', '0', '0', '0', '0'};
  static const uint8_t subscribe[] = {0x82u, 0x0du, 0x00u, 0x01u, 0x00u, 0x00u, 0x07u, 'b',
                                      'e',   'n',   'c',   'h',   '/',   '#',   0x00u};
  static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
  static const uint8_t publish[] = {0x30u, 0x13u, 0x00u, 0x0bu, 'b',   'e', 'n', 'c', 'h', '/', 't',
                                    'o',   'p',   'i',   'c',   0x00u, 'l', 'i', 'v', 'e', '1'};
  flowie_test_socket_t *subscribers = NULL;
  flowie_test_socket_t publisher = FLOWIE_TEST_INVALID_SOCKET;
  turbo_flow_resource_snapshot_t protocol = TURBO_FLOW_RESOURCE_SNAPSHOT_INIT;
  turbo_flow_t *flow = NULL;
  uint8_t reply[sizeof(suback)];
  uint8_t received[sizeof(publish)];
  const size_t capacity = flowie_bench_live_tcp_capacity();
  const size_t total_connections = capacity + 1u;
  unsigned short port = flowie_test_port();
  uint64_t setup_begin;
  uint64_t setup_elapsed = 0u;
  uint64_t fanout_begin;
  uint64_t fanout_elapsed = 0u;
  size_t opened = 0u;
  size_t connected = 0u;
  size_t delivered = 0u;
  int started = 0;
  int rc = TURBO_OK;
  check_size_gt(capacity, 0u);
  check_int_gt(port, 0);
  if (capacity == 0u || port == 0u) return;
  subscribers = (flowie_test_socket_t *)malloc(capacity * sizeof(*subscribers));
  check_not_null(subscribers);
  if (!subscribers) return;
  for (size_t i = 0u; i < capacity; ++i)
    subscribers[i] = FLOWIE_TEST_INVALID_SOCKET;
  flow = flowie_bench_live_fanout_flow(port, total_connections);
  if (!flow) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  rc = turbo_flow_start(flow);
  if (rc != TURBO_OK) goto done;
  started = 1;
  publisher = flowie_test_connect(port);
  if (publisher == FLOWIE_TEST_INVALID_SOCKET) {
    rc = TURBO_ENOTCONN;
    goto done;
  }
  rc = flowie_bench_connect_live(publisher, publisher_id);
  setup_begin = turbo_hrtime();
  for (size_t i = 0u; rc == TURBO_OK && i < capacity; ++i) {
    uint8_t client_id[8];
    flowie_bench_client_id(i, client_id);
    subscribers[i] = flowie_bench_connect_alias(port, i);
    if (subscribers[i] == FLOWIE_TEST_INVALID_SOCKET) {
      rc = TURBO_ENOTCONN;
      break;
    }
    opened = i + 1u;
    rc = flowie_bench_connect_live(subscribers[i], client_id);
    if (rc == TURBO_OK) rc = flowie_test_send(subscribers[i], subscribe, sizeof(subscribe));
    if (rc == TURBO_OK) rc = flowie_test_recv_exact(subscribers[i], reply, sizeof(reply));
    if (rc == TURBO_OK && memcmp(reply, suback, sizeof(reply)) != 0) rc = TURBO_EPROTO;
    if (rc == TURBO_OK) connected = i + 1u;
  }
  setup_elapsed = turbo_hrtime() - setup_begin;
  if (rc == TURBO_OK)
    rc = flowie_bench_wait_connections_with_timeout(flow, total_connections,
                                                    FLOWIE_BENCH_LIVE_WAIT_TIMEOUT_NS);
  if (rc == TURBO_OK) rc = turbo_flow_resource_snapshot_at(flow, 2u, &protocol);
  if (rc == TURBO_OK &&
      (protocol.load != total_connections || protocol.capacity != total_connections))
    rc = TURBO_EPROTO;
  if (rc != TURBO_OK) goto done;

  fanout_begin = turbo_hrtime();
  rc = flowie_test_send(publisher, publish, sizeof(publish));
  for (size_t i = 0u; rc == TURBO_OK && i < capacity; ++i) {
    rc = flowie_test_recv_exact(subscribers[i], received, sizeof(received));
    if (rc == TURBO_OK && memcmp(received, publish, sizeof(received)) != 0) rc = TURBO_EPROTO;
    if (rc == TURBO_OK) delivered = i + 1u;
  }
  fanout_elapsed = turbo_hrtime() - fanout_begin;
  if (rc == TURBO_OK) {
    const size_t aliases = (capacity + FLOWIE_BENCH_LIVE_TCP_CONNECTIONS_PER_ALIAS - 1u) /
                           FLOWIE_BENCH_LIVE_TCP_CONNECTIONS_PER_ALIAS;
    printf("FLOWIE_BENCH_RESULT operation=live_tcp_capacity connections=%zu subscribers=%zu "
           "aliases=%zu setup_ns=%" PRIu64 " setup_connection_s=%.2f fanout_ns=%" PRIu64
           " fanout_delivery_s=%.2f\n",
           total_connections, capacity, aliases, setup_elapsed,
           setup_elapsed ? ((double)capacity * 1000000000.0) / (double)setup_elapsed : 0.0,
           fanout_elapsed,
           fanout_elapsed ? ((double)capacity * 1000000000.0) / (double)fanout_elapsed : 0.0);
  }

done:
  printf("FLOWIE_BENCH_PROGRESS operation=live_tcp_capacity requested=%zu connected=%zu "
         "delivered=%zu status=%d\n",
         capacity, connected, delivered, rc);
  flowie_test_socket_close(publisher);
  for (size_t i = 0u; i < opened; ++i)
    flowie_test_socket_close(subscribers[i]);
  if (flow && started) {
    int wait_rc =
        flowie_bench_wait_connections_with_timeout(flow, 0u, FLOWIE_BENCH_LIVE_WAIT_TIMEOUT_NS);
    int stop_rc = turbo_flow_stop(flow);
    if (rc == TURBO_OK && wait_rc != TURBO_OK) rc = wait_rc;
    if (rc == TURBO_OK && stop_rc != TURBO_OK) rc = stop_rc;
  }
  turbo_flow_destroy(flow);
  free(subscribers);
  check_int_eq(rc, TURBO_OK);
  check_size_eq(connected, capacity);
  check_size_eq(delivered, capacity);
}

static void flowie_bench_tcp_stalled_subscriber(void) {
  static const uint8_t subscribe[] = {0x82u, 0x0du, 0x00u, 0x01u, 0x00u, 0x00u, 0x07u, 'b',
                                      'e',   'n',   'c',   'h',   '/',   '#',   0x00u};
  static const uint8_t suback[] = {0x90u, 0x04u, 0x00u, 0x01u, 0x00u, 0x00u};
  flowie_mqtt_publish_packet_t publish = FLOWIE_MQTT_PUBLISH_PACKET_INIT;
  flowie_test_socket_t publisher = FLOWIE_TEST_INVALID_SOCKET;
  flowie_test_socket_t fast = FLOWIE_TEST_INVALID_SOCKET;
  turbo_flow_t *flow = NULL;
  uint8_t *payload = NULL;
  uint8_t *wire = NULL;
  uint8_t *received = NULL;
  uint64_t latencies[FLOWIE_BENCH_STALL_SAMPLES] = {0};
  uint8_t reply[sizeof(suback)];
  size_t wire_size = 0u;
  size_t sample_index = 0u;
  unsigned short port = flowie_test_port();
  int started = 0;
  int rc = TURBO_OK;
  check_int_gt(port, 0);
  flow = flowie_bench_fanout_flow_with_limits(port, 3u, FLOWIE_BENCH_STALL_PACKET_CAPACITY,
                                              FLOWIE_BENCH_STALL_SEND_HWM_BYTES);
  check_not_null(flow);
  if (!flow) return;
  payload = (uint8_t *)calloc(FLOWIE_BENCH_STALL_PAYLOAD_BYTES, 1u);
  wire = (uint8_t *)malloc(FLOWIE_BENCH_STALL_PACKET_CAPACITY);
  received = (uint8_t *)malloc(FLOWIE_BENCH_STALL_PACKET_CAPACITY);
  check_not_null(payload);
  check_not_null(wire);
  check_not_null(received);
  if (!payload || !wire || !received) {
    rc = TURBO_ENOMEM;
    goto done;
  }
  publish.version = FLOWIE_MQTT_VERSION_5;
  publish.topic = flowie_bench_span("bench/stall");
  publish.payload = (flowie_mqtt_span_t){payload, FLOWIE_BENCH_STALL_PAYLOAD_BYTES};
  rc = flowie_mqtt_publish_packet_encode(&publish, wire, FLOWIE_BENCH_STALL_PACKET_CAPACITY,
                                         &wire_size);
  check_int_eq(rc, FLOWIE_MQTT_PARSE_OK);
  if (rc != FLOWIE_MQTT_PARSE_OK) {
    rc = TURBO_EPROTO;
    goto done;
  }
  check_size_gt(wire_size, FLOWIE_BENCH_STALL_SEND_HWM_BYTES / 2u);
  check_size_lt(wire_size, FLOWIE_BENCH_STALL_SEND_HWM_BYTES);
  rc = turbo_flow_start(flow);
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;
  started = 1;
  publisher = flowie_test_connect(port);
  fast = flowie_test_connect(port);
  check_true(publisher != FLOWIE_TEST_INVALID_SOCKET);
  check_true(fast != FLOWIE_TEST_INVALID_SOCKET);
  if (publisher == FLOWIE_TEST_INVALID_SOCKET || fast == FLOWIE_TEST_INVALID_SOCKET) {
    rc = TURBO_ENOTCONN;
    goto done;
  }
  rc = flowie_bench_connect(publisher, (const uint8_t *)"pub");
  if (rc == TURBO_OK) rc = flowie_bench_connect(fast, (const uint8_t *)"fst");
  if (rc == TURBO_OK) rc = flowie_test_send(fast, subscribe, sizeof(subscribe));
  if (rc == TURBO_OK) rc = flowie_test_recv_exact(fast, reply, sizeof(reply));
  if (rc == TURBO_OK && memcmp(reply, suback, sizeof(reply)) != 0) rc = TURBO_EPROTO;
  check_int_eq(rc, TURBO_OK);
  if (rc != TURBO_OK) goto done;

  benchmark_io("stalled subscriber isolation cycles", FLOWIE_BENCH_STALL_SAMPLES,
               FLOWIE_BENCH_STALL_MESSAGES, wire_size * FLOWIE_BENCH_STALL_MESSAGES * 2u) {
    flowie_test_socket_t slow = FLOWIE_TEST_INVALID_SOCKET;
    if (rc == TURBO_OK) {
      slow = flowie_test_connect(port);
      if (slow == FLOWIE_TEST_INVALID_SOCKET) rc = TURBO_ENOTCONN;
      if (rc == TURBO_OK)
        rc = flowie_test_socket_set_recv_buffer(slow, FLOWIE_BENCH_STALL_RECV_BUFFER_BYTES);
      if (rc == TURBO_OK) rc = flowie_bench_connect(slow, (const uint8_t *)"slw");
      if (rc == TURBO_OK) rc = flowie_test_send(slow, subscribe, sizeof(subscribe));
      if (rc == TURBO_OK) rc = flowie_test_recv_exact(slow, reply, sizeof(reply));
      if (rc == TURBO_OK && memcmp(reply, suback, sizeof(reply)) != 0) rc = TURBO_EPROTO;
      if (rc == TURBO_OK) {
        const uint64_t begin = turbo_hrtime();
        for (size_t message = 0u; rc == TURBO_OK && message < FLOWIE_BENCH_STALL_MESSAGES;
             ++message) {
          wire[wire_size - 2u] = (uint8_t)sample_index;
          wire[wire_size - 1u] = (uint8_t)message;
          rc = flowie_test_send(publisher, wire, wire_size);
          if (rc == TURBO_OK) rc = flowie_test_recv_exact(fast, received, wire_size);
          if (rc == TURBO_OK && memcmp(received, wire, wire_size) != 0) rc = TURBO_EPROTO;
        }
        latencies[sample_index] = turbo_hrtime() - begin;
      }
      if (rc == TURBO_OK) rc = flowie_bench_wait_connections(flow, 2u);
    }
    flowie_test_socket_close(slow);
    ++sample_index;
  }
  check_int_eq(rc, TURBO_OK);
  check_size_eq(sample_index, FLOWIE_BENCH_STALL_SAMPLES);
  if (rc == TURBO_OK) {
    qsort(latencies, FLOWIE_BENCH_STALL_SAMPLES, sizeof(*latencies), flowie_bench_u64_compare);
    printf("FLOWIE_BENCH_RESULT operation=tcp_stalled_subscriber samples=%u messages=%u "
           "packet_bytes=%zu p50_ns=%" PRIu64 " p95_ns=%" PRIu64 " p99_ns=%" PRIu64 "\n",
           FLOWIE_BENCH_STALL_SAMPLES, FLOWIE_BENCH_STALL_MESSAGES, wire_size,
           flowie_bench_percentile(latencies, FLOWIE_BENCH_STALL_SAMPLES, 50u),
           flowie_bench_percentile(latencies, FLOWIE_BENCH_STALL_SAMPLES, 95u),
           flowie_bench_percentile(latencies, FLOWIE_BENCH_STALL_SAMPLES, 99u));
  }

done:
  flowie_test_socket_close(fast);
  flowie_test_socket_close(publisher);
  if (flow && started) check_int_eq(turbo_flow_stop(flow), TURBO_OK);
  turbo_flow_destroy(flow);
  free(received);
  free(wire);
  free(payload);
}

spec("flowie capacity benchmarks") {
  bench("MQTT typed projection facts") { flowie_bench_mqtt_projection(); }

  bench("compiled MQTT security matcher") {
    flowie_bench_security_cost_breakdown();
    flowie_bench_security_matcher(64u);
    flowie_bench_security_matcher(512u);
    flowie_bench_security_matcher(TURBO_FLOW_SECURITY_MAX_RULES);
  }

  bench("100k session and topic-index capacity") {
    flowie_bench_sessions();
    flowie_bench_topic_index();
  }

  bench("100k wildcard/shared rebuild and candidate fan-out") {
    flowie_bench_topic_rebuild_fanout();
  }

  bench("real TCP MQTT fan-out") { flowie_bench_tcp_fanout(); }

  bench("real TCP MQTT pipeline burst") { flowie_bench_tcp_pipeline_burst(); }

  bench("real TCP MQTT connection churn") { flowie_bench_tcp_churn(); }

  bench("100k live TCP MQTT selector and packet fan-out") {
    flowie_bench_live_tcp_capacity_fanout();
  }

  bench("real TCP stalled-subscriber isolation") { flowie_bench_tcp_stalled_subscriber(); }
}
