#include "../src/flow_plugin_operation_internal.h"
#include "plugin_operation_fixture.h"
#include "tinytest.h"
#include <salts/clock.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char operation_yaml[] =
    "version: 1\noperation_bindings:\n"
    "  - operation: fixture.double\n    plugin: fixture.operation\n    version: 1\n"
    "    input_schema: cmeta.int.data\n    input_schema_version: 1\n"
    "    output_schema: cmeta.int.data\n    output_schema_version: 1\n"
    "    execution: inline\n    threading: thread_safe\n    cancellation: none\n"
    "    permissions: []\n    max_inflight: 1\n    max_input_bytes: 8\n"
    "    max_result_bytes: 8\n    max_retained_bytes: 32\n    max_steps: 2\n    deadline_ms: 0\n";
static const char operation_dsl[] = "source input\nstage calculate operation fixture.double\n"
                                    "stage output operation fixture.capture\nstage main {\n input "
                                    "-> calculate\n calculate -> output\n}\n";
static const turbo_flow_data_schema_t input_schema = {sizeof(turbo_flow_data_schema_t),
                                                      TURBO_FLOW_DOMAIN_DATA,
                                                      TURBO_FLOW_DATA_ENCODING_OPAQUE,
                                                      "cmeta.int.data",
                                                      "Integer",
                                                      "int",
                                                      7u,
                                                      1u,
                                                      NULL};
static int copy_int(const void *value, void *ctx, void **out) {
  (void)ctx;
  *out = malloc(sizeof(int));
  if (!*out) return SALTS_ENOMEM;
  *(int *)*out = *(const int *)value;
  return SALTS_OK;
}
static void free_int(void *value, void *ctx) {
  (void)ctx;
  free(value);
}
static int copy_record(const void *value, void *ctx, void **out) {
  (void)ctx;
  *out = malloc(sizeof(operation_record));
  if (!*out) return SALTS_ENOMEM;
  memcpy(*out, value, sizeof(operation_record));
  return SALTS_OK;
}
static int capture_result(turbo_flow_msg_t *msg, void *ctx) {
  return turbo_flow_msg_clone(ctx, msg);
}
static SALTS_THREAD_LOCAL unsigned runtime_thread_identity;

typedef struct runtime_test_s {
  turbo_flow_plugin_host_t *host;
  turbo_flow_plugin_catalog_snapshot_t *snapshot;
  turbo_flow_plugin_result_domain_t *domain;
  turbo_flow_plugin_generation_t *generation, *cleanup;
  turbo_flow_resolved_config_t *resolved;
  turbo_flow_t *flow;
  turbo_flow_msg_t input, result;
  turbo_flow_plugin_generation_config_t config;
  turbo_flow_config_error_t error;
  operation_fixture_t *observer;
  const void *control_thread;
  atomic_uint lifecycle_wrong_thread, unloads;
} runtime_test_t;
static void runtime_lifecycle(void *ctx, turbo_flow_plugin_lifecycle_event_t event, const char *id,
                              int status) {
  runtime_test_t *t = ctx;
  (void)id;
  (void)status;
  if (t->control_thread != &runtime_thread_identity)
    atomic_fetch_add(&t->lifecycle_wrong_thread, 1);
  if (event == TURBO_FLOW_PLUGIN_LIFECYCLE_UNLOAD) atomic_fetch_add(&t->unloads, 1);
}
enum { RUNTIME_YAML_BYTES = 4096, RUNTIME_BARRIER_TIMEOUT_MS = 2000 };
static void runtime_yaml(char *out, const char *field, const char *replacement) {
  const char *at = strstr(operation_yaml, field);
  check_not_null(at);
  size_t prefix = (size_t)(at - operation_yaml);
  check_less(prefix + strlen(replacement) + strlen(at + strlen(field)), (size_t)RUNTIME_YAML_BYTES);
  memcpy(out, operation_yaml, prefix);
  strcpy(out + prefix, replacement);
  strcpy(out + prefix + strlen(replacement), at + strlen(field));
}
static int runtime_open_contract(runtime_test_t *t, const char *path, const char *yaml,
                                 const char *dsl, int fault) {
  turbo_flow_plugin_host_config_t hc = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_plugin_operation_catalog_v3_t catalog;
  turbo_flow_operation_descriptor_t m = {0};
  turbo_flow_operation_provider_registration_t capture =
      TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
  int rc;
  memset(t, 0, sizeof(*t));
  t->control_thread = &runtime_thread_identity;
  atomic_init(&t->lifecycle_wrong_thread, 0);
  atomic_init(&t->unloads, 0);
  hc.lifecycle_observer = runtime_lifecycle;
  hc.lifecycle_observer_ctx = t;
  t->config = (turbo_flow_plugin_generation_config_t)TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
  t->error = (turbo_flow_config_error_t)TURBO_FLOW_CONFIG_ERROR_INIT;
  turbo_flow_msg_init(&t->input);
  turbo_flow_msg_init(&t->result);
  rc = turbo_flow_plugin_host_create(&hc, &t->host, &pe);
  if (rc) return rc;
  rc = turbo_flow_plugin_host_load(t->host, path, &pe);
  if (rc) return rc;
  rc = turbo_flow_plugin_catalog_snapshot_create(t->host, &t->snapshot, &pe);
  if (rc) return rc;
  turbo_flow_plugin_operation_catalog_v3_init(&catalog);
  rc = turbo_flow_plugin_catalog_snapshot_operation_catalog(t->snapshot, &catalog);
  if (rc) return rc;
  t->observer = catalog.entries[0].operation.factory_ctx;
  rc = turbo_flow_plugin_result_domain_create(t->snapshot, 2, &t->domain, &pe);
  if (rc) return rc;
  rc = turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &t->resolved, &t->error);
  if (rc) return rc;
  t->flow = turbo_flow_create();
  if (!t->flow) return SALTS_ENOMEM;
  m.size = sizeof(m);
  m.name = "fixture.double";
  m.version = 1;
  m.domain = m.input_domain = m.output_domain = TURBO_FLOW_DOMAIN_DATA;
  m.input_type = m.output_type = "Message";
  m.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  m.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
  m.flags = TURBO_FLOW_OPERATION_STAGE;
  m.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  switch (fault) {
  case 1:
    m.version = 2;
    break;
  case 2:
    m.output_type = "changed";
    break;
  case 3:
    m.scope.state = TURBO_FLOW_STATE_SCOPE_NODE;
    break;
  case 4:
    m.scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
    break;
  case 5:
    m.scope.authority = TURBO_FLOW_AUTHORITY_PURE;
    break;
  case 6:
    m.runtime.settlement = 1;
    break;
  case 7:
    m.runtime.deadline_ms = 1;
    break;
  case 8:
    m.execution_mask |= TURBO_FLOW_OPERATION_EXEC_THREAD;
    break;
  case 9:
    m.runtime.handoff = TURBO_FLOW_HANDOFF_BOUNDED;
    break;
  case 13:
    --m.size;
    break;
  case 14:
    m.version = 0;
    break;
  default:
    break;
  }
  rc = turbo_flow_register_operation(t->flow, &m);
  if (rc) return rc;
  m.name = "fixture.capture";
  rc = turbo_flow_register_operation(t->flow, &m);
  if (rc) return rc;
  capture.operation_name = m.name;
  capture.fn = capture_result;
  capture.ctx = &t->result;
  rc = turbo_flow_register_operation_provider(t->flow, &capture);
  if (rc) return rc;
  return turbo_flow_parse_string(t->flow, dsl, strlen(dsl));
}
static int runtime_open(runtime_test_t *t, const char *path, const char *yaml, const char *dsl) {
  return runtime_open_contract(t, path, yaml, dsl, 0);
}
static int runtime_create(runtime_test_t *t) {
  return turbo_flow_plugin_generation_create(t->snapshot, t->resolved, &t->flow, &t->config,
                                             t->domain, &t->generation, &t->cleanup, &t->error);
}
static int runtime_close(runtime_test_t *t) {
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  int rc;
  if (t->cleanup) {
    rc = turbo_flow_plugin_generation_destroy(t->cleanup, 0, &t->error);
    if (rc) return rc;
    t->cleanup = NULL;
  }
  if (t->generation) {
    rc = turbo_flow_plugin_generation_destroy(t->generation, 0, &t->error);
    if (rc) return rc;
    t->generation = NULL;
  }
  turbo_flow_destroy(t->flow);
  t->flow = NULL;
  turbo_flow_msg_cleanup(&t->result);
  turbo_flow_msg_cleanup(&t->input);
  if (t->domain) {
    rc = turbo_flow_plugin_result_domain_destroy(t->domain, &pe);
    if (rc) return rc;
    t->domain = NULL;
  }
  turbo_flow_resolved_config_destroy(t->resolved);
  t->resolved = NULL;
  turbo_flow_plugin_catalog_snapshot_destroy(t->snapshot);
  t->snapshot = NULL;
  if (t->host) {
    rc = turbo_flow_plugin_host_destroy(t->host, 0, &pe);
    if (rc) return rc;
    t->host = NULL;
  }
  check_equal(atomic_load(&t->lifecycle_wrong_thread), 0u);
  return SALTS_OK;
}
static void runtime_bind(runtime_test_t *t, int record) {
  turbo_flow_content_descriptor_t descriptor = TURBO_FLOW_CONTENT_DESCRIPTOR_INIT;
  static const turbo_flow_data_schema_t record_projection = {sizeof(turbo_flow_data_schema_t),
                                                             TURBO_FLOW_DOMAIN_DATA,
                                                             TURBO_FLOW_DATA_ENCODING_OPAQUE,
                                                             "operation.Record.data",
                                                             "Record",
                                                             "operation_record",
                                                             8u,
                                                             1u,
                                                             NULL};
  const cmeta_data_desc *data = record ? &operation_record_data : &cmeta_data_int;
  int *value = calloc(1, data->storage_type->size);
  check_not_null(value);
  value[0] = 7;
  if (record) value[1] = 19;
  /* Records are only used by alias-rejection tests; their clone copies the complete fixed span. */
  check_equal(turbo_flow_msg_bind_typed_projection(
                  &t->input, record ? &record_projection : &input_schema, data, value,
                  record ? copy_record : copy_int, free_int, NULL),
              SALTS_OK);
  t->input.owned_payload = tstr_dup("original-wire-bytes");
  check_not_null(t->input.owned_payload);
  t->input.payload = tstr_to_v(t->input.owned_payload);
  check_equal(turbo_flow_content_descriptor_init(
                  &descriptor, TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_CONTENT_PROFILE_GENERIC,
                  TURBO_FLOW_DATA_ENCODING_OPAQUE, "application/octet-stream", "fixture.original"),
              SALTS_OK);
  check_equal(turbo_flow_msg_copy_content_descriptor(&t->input, &descriptor), SALTS_OK);
}

typedef struct runtime_worker_s {
  runtime_test_t *test;
  int point, status, observed;
  const void *thread;
} runtime_worker_t;
static void runtime_reject_error_header(uint32_t phase, int mode, int fault, int callback_status) {
  runtime_test_t t;
  char yaml[RUNTIME_YAML_BYTES];
  turbo_flow_plugin_operation_error_v3_t diagnostic;
  turbo_flow_plugin_result_domain_snapshot_v3_t state;
  turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
  turbo_flow_config_error_t cleanup_error = TURBO_FLOW_CONFIG_ERROR_INIT;
  int alias = mode == OP_FIXTURE_EXECUTE_ALIAS;
  runtime_yaml(yaml, "input_schema: cmeta.int.data",
               alias ? "input_schema: operation.Record.data" : "input_schema: cmeta.int.data");
  check_equal(runtime_open(&t, alias ? FLOW_OPERATION_EXECUTE_ALIAS : FLOW_OPERATION_OK, yaml,
                           operation_dsl),
              SALTS_OK);
  t.observer->mode = mode;
  t.observer->error_phase = phase;
  t.observer->error_fault = fault;
  t.observer->error_status = callback_status;
  t.observer->fail_session_release = 1;
  t.observer->fail_context_release = 1;
  int rc = runtime_create(&t);
  int flow_preserved = t.flow != NULL;
  turbo_flow_config_error_t create_error = t.error;
  int input_unchanged = 1, result_absent = 1;
  if (phase == TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE) {
    check_equal(rc, SALTS_OK);
    runtime_bind(&t, alias);
    turbo_flow_msg_t before = t.input;
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(t.generation)), SALTS_OK);
    rc = turbo_flow_publish(turbo_flow_plugin_generation_flow(t.generation), "input", &t.input);
    input_unchanged = memcmp(&before, &t.input, sizeof(before)) == 0 &&
                      *(const int *)turbo_flow_msg_projection(&t.input, NULL) == 7;
    result_absent = turbo_flow_msg_result(&t.input, NULL, NULL) == NULL &&
                    turbo_flow_msg_result(&t.result, NULL, NULL) == NULL;
  }
  unsigned preflights = atomic_load(&t.observer->preflights);
  unsigned contexts = atomic_load(&t.observer->context_creates);
  unsigned sessions = atomic_load(&t.observer->session_creates);
  unsigned executes = atomic_load(&t.observer->executes);
  unsigned destroys = atomic_load(&t.observer->destroys);
  int had_cleanup = t.cleanup != NULL;
  turbo_flow_plugin_operation_error_v3_init(&diagnostic);
  if (t.generation || t.cleanup)
    check_equal(turbo_flow_plugin_generation_operation_error(
                    t.generation ? t.generation : t.cleanup, 0, &diagnostic),
                SALTS_OK);
  if (t.cleanup)
    check_equal(turbo_flow_plugin_generation_cleanup_error(t.cleanup, &cleanup_error), SALTS_OK);
  turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
  check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
  size_t outstanding = state.outstanding, retained = state.retained_bytes;
  turbo_flow_plugin_generation_t *generation = t.generation ? t.generation : t.cleanup;
  if (generation) {
    int release_rc = turbo_flow_plugin_generation_destroy(generation, 0, &t.error);
    if (release_rc == SALTS_EIO)
      release_rc = turbo_flow_plugin_generation_destroy(generation, 0, &t.error);
    check_equal(release_rc, SALTS_OK);
    t.generation = t.cleanup = NULL;
  }
  turbo_flow_msg_cleanup(&t.input);
  turbo_flow_msg_cleanup(&t.result);
  turbo_flow_plugin_catalog_snapshot_destroy(t.snapshot);
  t.snapshot = NULL;
  int domain_rc = turbo_flow_plugin_result_domain_destroy(t.domain, &pe);
  size_t retry_owners = 0;
  int retry_status = SALTS_OK, pinned = SALTS_OK;
  if (domain_rc == SALTS_EIO) {
    check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
    retry_owners = state.owner_count;
    retry_status = state.last_cleanup_status;
    pinned = turbo_flow_plugin_host_destroy(t.host, 0, &pe);
    check_equal(pinned, SALTS_EBUSY);
    check_equal(turbo_flow_plugin_result_domain_destroy(t.domain, &pe), SALTS_OK);
  } else check_equal(domain_rc, SALTS_OK);
  t.domain = NULL;
  unsigned session_releases = atomic_load(&t.observer->session_releases);
  unsigned context_releases = atomic_load(&t.observer->context_releases);
  unsigned live_sessions = atomic_load(&t.observer->sessions);
  unsigned live_contexts = atomic_load(&t.observer->contexts);
  check_equal(runtime_close(&t), SALTS_OK);
  info("returned error phase=%u fault=%d callback=%d mode=%d rc=%d", phase, fault, callback_status,
       mode, rc);
  check_equal(rc, fault == OP_ERROR_VALID ? callback_status : SALTS_EINVAL);
  check_equal(preflights, 1u);
  check_equal(contexts, phase == TURBO_FLOW_PLUGIN_OPERATION_PHASE_PREFLIGHT ? 0u : 1u);
  check_equal(sessions, phase <= TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT_CONTEXT ? 0u : 1u);
  check_equal(executes, phase == TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE ? 1u : 0u);
  check_equal(flow_preserved, phase == TURBO_FLOW_PLUGIN_OPERATION_PHASE_PREFLIGHT);
  check_equal(input_unchanged, 1);
  check_equal(result_absent, 1);
  check_equal(outstanding, (size_t)0);
  check_equal(retained, (size_t)0);
  check_equal(destroys, phase == TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE && mode == OP_FIXTURE_OK
                            ? 1u
                            : 0u);
  check_equal(live_sessions, 0u);
  check_equal(live_contexts, 0u);
  int owned_session = phase == TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE ||
                      (phase == TURBO_FLOW_PLUGIN_OPERATION_PHASE_SESSION && mode == OP_FIXTURE_OK);
  int owned_context =
      phase > TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT_CONTEXT ||
      (phase == TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT_CONTEXT && mode == OP_FIXTURE_OK);
  check_equal(session_releases, owned_session ? 2u : 0u);
  check_equal(context_releases, owned_context ? 2u : 0u);
  check_equal(had_cleanup, phase == TURBO_FLOW_PLUGIN_OPERATION_PHASE_SESSION && owned_session);
  if (had_cleanup) check_equal(cleanup_error.status, SALTS_EIO);
  if (owned_context) {
    check_equal(domain_rc, SALTS_EIO);
    check_equal(retry_owners, (size_t)1);
    check_equal(retry_status, SALTS_EIO);
    check_equal(pinned, SALTS_EBUSY);
  }
  if (phase == TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE || had_cleanup) {
    check_equal(diagnostic.size, sizeof(diagnostic));
    check_equal(diagnostic.abi_major, 3u);
    check_equal(diagnostic.abi_minor, 0u);
    if (fault != OP_ERROR_VALID) {
      check_equal(diagnostic.status, SALTS_EINVAL);
      check_equal(diagnostic.phase, phase);
      check_equal(diagnostic.engine_status, (int64_t)0);
      check_not_equal(diagnostic.message[0], 'X');
    } else {
      check_equal(diagnostic.phase, (uint32_t)TURBO_FLOW_PLUGIN_OPERATION_PHASE_RELEASE);
      check_equal(diagnostic.engine_status, (int64_t)SALTS_EIO);
      check_equal(diagnostic.message, "fixture diagnostic");
    }
  }
  if (phase != TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE) {
    const char *paths[] = {"", "$.operation_bindings[0].preflight",
                           "$.operation_bindings[0].result_context",
                           "$.operation_bindings[0].session"};
    check_equal(create_error.status, fault == OP_ERROR_VALID ? callback_status : SALTS_EINVAL);
    check_equal(create_error.path, paths[phase]);
  }
  check_equal(atomic_load(&t.unloads), 1u);
}
static void runtime_error_header_matrix(uint32_t phase, const int *modes, size_t count) {
  const int statuses[] = {SALTS_OK, SALTS_EIO};
  for (int fault = OP_ERROR_SHORT; fault <= OP_ERROR_NEW_MINOR; ++fault)
    for (size_t status = 0; status < sizeof(statuses) / sizeof(statuses[0]); ++status)
      for (size_t mode = 0; mode < count; ++mode)
        runtime_reject_error_header(phase, modes[mode], fault, statuses[status]);
  for (size_t mode = 0; mode < count; ++mode)
    runtime_reject_error_header(phase, modes[mode], OP_ERROR_VALID, SALTS_EIO);
}
static void runtime_worker(void *ctx) {
  runtime_worker_t *w = ctx;
  w->thread = &runtime_thread_identity;
  turbo_flow_msg_t clone;
  turbo_flow_msg_init(&clone);
  if (w->point == OP_BARRIER_EXECUTE)
    w->status = turbo_flow_publish(turbo_flow_plugin_generation_flow(w->test->generation), "input",
                                   &w->test->input);
  else if (w->point == OP_BARRIER_DESTROY) {
    turbo_flow_msg_cleanup(&w->test->result);
    w->status = SALTS_OK;
  } else {
    w->status = turbo_flow_msg_clone(&clone, &w->test->result);
    if (w->status == SALTS_OK) {
      w->observed = *(const int *)turbo_flow_msg_result(&clone, NULL, NULL);
      turbo_flow_msg_cleanup(&clone);
    }
  }
}

spec("ABI3 generation operation runtime") {
  it("rejects malformed returned preflight error headers before every factory") {
    const int modes[] = {OP_FIXTURE_OK};
    runtime_error_header_matrix(TURBO_FLOW_PLUGIN_OPERATION_PHASE_PREFLIGHT, modes,
                                sizeof(modes) / sizeof(modes[0]));
  }
  it("rejects malformed returned result-context error headers and retries only owned contexts") {
    const int modes[] = {OP_FIXTURE_OK, OP_FIXTURE_CONTEXT_FAIL, OP_FIXTURE_CONTEXT_FACTORY_ALIAS};
    runtime_error_header_matrix(TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT_CONTEXT, modes,
                                sizeof(modes) / sizeof(modes[0]));
  }
  it("rejects malformed returned session error headers and retries only owned sessions") {
    const int modes[] = {OP_FIXTURE_OK, OP_FIXTURE_SESSION_FAIL, OP_FIXTURE_SESSION_FACTORY_ALIAS,
                         OP_FIXTURE_SESSION_RESULT_ALIAS};
    runtime_error_header_matrix(TURBO_FLOW_PLUGIN_OPERATION_PHASE_SESSION, modes,
                                sizeof(modes) / sizeof(modes[0]));
  }
  it("rejects malformed returned execute error headers before commit without freeing aliases") {
    const int modes[] = {OP_FIXTURE_OK, OP_FIXTURE_EXECUTE_NULL, OP_FIXTURE_EXECUTE_ALIAS};
    runtime_error_header_matrix(TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE, modes,
                                sizeof(modes) / sizeof(modes[0]));
  }
  it("rejects duplicate binding YAML before producing a resolved document or calling factories") {
    runtime_test_t t;
    turbo_flow_resolved_config_t *duplicate = NULL;
    char yaml[RUNTIME_YAML_BYTES];
    check_equal(runtime_open(&t, FLOW_OPERATION_OK, operation_yaml, operation_dsl), SALTS_OK);
    check_less(snprintf(yaml, sizeof(yaml), "%s%s", operation_yaml,
                        strstr(operation_yaml, "  - operation:")),
               (int)sizeof(yaml));
    check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &duplicate, &t.error),
                SALTS_EALREADY);
    check_null(duplicate);
    check_equal(atomic_load(&t.observer->context_creates), 0u);
    check_equal(atomic_load(&t.observer->session_creates), 0u);
    check_equal(turbo_flow_state(t.flow), TURBO_FLOW_STATE_PARSED);
    check_equal(runtime_close(&t), SALTS_OK);
  }
  it("preserves an operation compile failure through cleanup and rejects reuse of its consumed "
     "domain") {
    static const char invalid_dsl[] = "source input\nstage calculate operation fixture.double\n"
                                      "stage missing operation fixture.missing\nstage main {\n "
                                      "input -> calculate -> missing\n}\n";
    runtime_test_t t;
    turbo_flow_plugin_result_domain_snapshot_v3_t state;
    turbo_flow_config_error_t cleanup_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(runtime_open(&t, FLOW_OPERATION_OK, operation_yaml, invalid_dsl), SALTS_OK);
    turbo_flow_operation_descriptor_t metadata =
        *turbo_flow_find_operation(t.flow, "fixture.double");
    metadata.name = "fixture.double";
    metadata.input_type = metadata.output_type = "Message";
    t.observer->mode = OP_FIXTURE_RELEASE_SESSION_ONCE;
    check_equal(runtime_create(&t), SALTS_EINVAL);
    check_equal(t.error.path, "$.graph");
    check_null(t.flow);
    check_null(t.generation);
    check_not_null(t.cleanup);
    check_equal(atomic_load(&t.observer->context_creates), 1u);
    check_equal(atomic_load(&t.observer->session_creates), 1u);
    check_equal(atomic_load(&t.observer->session_releases), 1u);
    turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
    check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
    check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED);
    check_equal(turbo_flow_plugin_generation_cleanup_error(t.cleanup, &cleanup_error), SALTS_OK);
    check_equal(cleanup_error.status, SALTS_EIO);
    check_equal(turbo_flow_plugin_generation_destroy(t.cleanup, 0u, &cleanup_error), SALTS_OK);
    t.cleanup = NULL;
    check_equal(atomic_load(&t.observer->session_releases), 2u);
    check_equal(atomic_load(&t.observer->sessions), 0u);
    check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
    check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_DETACHED);
    t.flow = turbo_flow_create();
    check_not_null(t.flow);
    check_equal(turbo_flow_register_operation(t.flow, &metadata), SALTS_OK);
    check_equal(turbo_flow_parse_string(t.flow, invalid_dsl, sizeof(invalid_dsl) - 1u), SALTS_OK);
    turbo_flow_t *original = t.flow;
    check_equal(runtime_create(&t), SALTS_EINVAL);
    check_equal(t.error.path, "$.operation_bindings[0].result_domain");
    check(t.flow == original);
    check_null(t.generation);
    check_null(t.cleanup);
    check_equal(atomic_load(&t.observer->context_creates), 1u);
    check_equal(atomic_load(&t.observer->session_creates), 1u);
    check_equal(runtime_close(&t), SALTS_OK);
  }
  it("rejects a distinct snapshot identity before consuming the domain") {
    runtime_test_t t;
    turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_catalog_snapshot_t *other = NULL;
    turbo_flow_plugin_result_domain_snapshot_v3_t state;
    check_equal(runtime_open(&t, FLOW_OPERATION_OK, operation_yaml, operation_dsl), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(t.host, &other, &pe), SALTS_OK);
    turbo_flow_plugin_catalog_snapshot_destroy(t.snapshot);
    t.snapshot = other;
    turbo_flow_t *original = t.flow;
    check_equal(runtime_create(&t), SALTS_EINVAL);
    check(t.flow == original);
    check_null(t.generation);
    check_null(t.cleanup);
    check_equal(atomic_load(&t.observer->context_creates), 0u);
    check_equal(atomic_load(&t.observer->session_creates), 0u);
    turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
    check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
    check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_READY);
    check_equal(runtime_close(&t), SALTS_OK);
  }
  it("rejects absent, untyped and mismatched input before execute or result reservation") {
    for (int kind = 0; kind < 3; ++kind) {
      runtime_test_t t;
      turbo_flow_plugin_result_domain_snapshot_v3_t state;
      turbo_flow_plugin_operation_error_v3_t error;
      check_equal(runtime_open(&t, FLOW_OPERATION_OK, operation_yaml, operation_dsl), SALTS_OK);
      check_equal(runtime_create(&t), SALTS_OK);
      if (kind == 1) {
        int *value = malloc(sizeof(int));
        check_not_null(value);
        *value = 7;
        check_equal(turbo_flow_msg_bind_projection(&t.input, &input_schema, value, copy_int,
                                                   free_int, NULL),
                    SALTS_OK);
      } else if (kind == 2) runtime_bind(&t, 1);
      check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(t.generation)), SALTS_OK);
      check_equal(
          turbo_flow_publish(turbo_flow_plugin_generation_flow(t.generation), "input", &t.input),
          kind == 2 ? SALTS_EPROTO : SALTS_ENOTSUP);
      check_equal(atomic_load(&t.observer->executes), 0u);
      check_equal(atomic_load(&t.observer->destroys), 0u);
      turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
      check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
      check_equal(state.outstanding, (size_t)0);
      check_equal(state.retained_bytes, (size_t)0);
      turbo_flow_plugin_operation_error_v3_init(&error);
      check_equal(turbo_flow_plugin_generation_operation_error(t.generation, 0, &error), SALTS_OK);
      check_equal(error.phase, (uint32_t)TURBO_FLOW_PLUGIN_OPERATION_PHASE_PREFLIGHT);
      check_equal(runtime_close(&t), SALTS_OK);
    }
  }
  it("validates Graph contracts and exact generation descriptors before transfer") {
    for (int fault = 1; fault <= 14; ++fault) {
      runtime_test_t t;
      turbo_flow_plugin_result_domain_snapshot_v3_t state;
      int open_rc =
          runtime_open_contract(&t, FLOW_OPERATION_OK, operation_yaml, operation_dsl, fault);
      check_equal(open_rc, fault == 9 || fault == 14 ? SALTS_EINVAL : SALTS_OK);
      if (fault == 10) --t.config.size;
      if (fault == 11) ++t.config.size;
      if (fault == 12) t.config.abi_major = 2;
      turbo_flow_t *original = t.flow;
      int rc = open_rc == SALTS_OK ? runtime_create(&t) : open_rc;
      info("Graph/config fault %d: %d %s", fault, rc, t.error.path);
      check_equal(rc, fault == 13  ? SALTS_OK
                      : fault <= 2 ? SALTS_EPROTO
                      : fault <= 8 ? SALTS_ENOTSUP
                                   : SALTS_EINVAL);
      if (fault == 13) {
        check_equal(runtime_close(&t), SALTS_OK);
        continue;
      }
      check(t.flow == original);
      check_null(t.generation);
      check_null(t.cleanup);
      check_equal(atomic_load(&t.observer->context_creates), 0u);
      check_equal(atomic_load(&t.observer->session_creates), 0u);
      turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
      check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
      check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_READY);
      check_equal(runtime_close(&t), SALTS_OK);
    }
  }
  it("stops every owner before releasing any context and preserves second-factory failure "
     "ownership") {
    static const char dsl[] =
        "source input\nsource idle\nstage calculate operation fixture.double\n"
        "stage other operation fixture.other\nstage output operation fixture.capture\n"
        "stage main {\n input -> calculate -> output\n idle -> other\n}\n";
    for (int fail_second = 0; fail_second < 3; ++fail_second) {
      runtime_test_t t;
      char yaml[RUNTIME_YAML_BYTES], second[RUNTIME_YAML_BYTES];
      turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_result_domain_snapshot_v3_t state;
      runtime_yaml(second, "fixture.double", "fixture.other");
      check_less(
          snprintf(yaml, sizeof(yaml), "%s%s", operation_yaml, strstr(second, "  - operation:")),
          (int)sizeof(yaml));
      check_equal(runtime_open(&t, FLOW_OPERATION_TWO_NAMES, yaml, dsl), SALTS_OK);
      turbo_flow_operation_descriptor_t metadata =
          *turbo_flow_find_operation(t.flow, "fixture.double");
      metadata.name = "fixture.other";
      check_equal(turbo_flow_register_operation(t.flow, &metadata), SALTS_OK);
      if (fail_second == 2) {
        check_equal(turbo_flow_plugin_result_domain_destroy(t.domain, &pe), SALTS_OK);
        t.domain = NULL;
        check_equal(turbo_flow_plugin_result_domain_create(t.snapshot, 1, &t.domain, &pe),
                    SALTS_OK);
        turbo_flow_t *original = t.flow;
        check_equal(runtime_create(&t), SALTS_ENOSPC);
        check(t.flow == original);
        check_null(t.generation);
        check_null(t.cleanup);
        check_equal(atomic_load(&t.observer->context_creates), 0u);
        check_equal(atomic_load(&t.observer->session_creates), 0u);
        turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
        check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
        check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_READY);
        check_equal(runtime_close(&t), SALTS_OK);
        continue;
      }
      if (fail_second) t.observer->mode = OP_FIXTURE_SECOND_SESSION_FAIL;
      check_equal(runtime_create(&t), fail_second ? SALTS_EIO : SALTS_OK);
      check_equal(atomic_load(&t.observer->context_creates), 2u);
      check_equal(atomic_load(&t.observer->session_creates), 2u);
      if (!fail_second) {
        runtime_bind(&t, 0);
        check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(t.generation)), SALTS_OK);
        check_equal(
            turbo_flow_publish(turbo_flow_plugin_generation_flow(t.generation), "input", &t.input),
            SALTS_OK);
        check_equal(turbo_flow_plugin_generation_destroy(t.generation, 0, &t.error), SALTS_OK);
        t.generation = NULL;
        check_equal(turbo_flow_plugin_result_domain_destroy(t.domain, &pe), SALTS_EBUSY);
        check_equal(atomic_load(&t.observer->context_releases), 0u);
      } else {
        check_null(t.generation);
        check_null(t.cleanup);
        check_equal(atomic_load(&t.observer->session_releases), 1u);
      }
      turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
      check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
      check_equal(state.owner_count, (size_t)2);
      check_equal(runtime_close(&t), SALTS_OK);
    }
  }
  it("keeps raw context ownership when the real Graph owner rejects a faulted DLL wrapper") {
    runtime_test_t t;
    turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_result_domain_snapshot_v3_t state;
    check_equal(runtime_open(&t, FLOW_OPERATION_OWNER_FAIL, operation_yaml, operation_dsl),
                SALTS_OK);
    check_equal(runtime_create(&t), SALTS_EINVAL);
    check_null(t.flow);
    check_null(t.generation);
    check_null(t.cleanup);
    check_equal(atomic_load(&t.observer->session_creates), 0u);
    check_equal(atomic_load(&t.observer->contexts), 1u);
    t.observer->fault_schema.size = sizeof(turbo_flow_data_schema_t);
    t.observer->mode = OP_FIXTURE_RELEASE_CONTEXT_ONCE;
    turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
    check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
    check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_DETACHED);
    check_equal(state.owner_count, (size_t)1);
    check_equal(state.outstanding, (size_t)0);
    turbo_flow_plugin_catalog_snapshot_destroy(t.snapshot);
    t.snapshot = NULL;
    check_equal(turbo_flow_plugin_result_domain_destroy(t.domain, &pe), SALTS_EIO);
    check_equal(atomic_load(&t.observer->context_releases), 1u);
    check_equal(atomic_load(&t.observer->contexts), 1u);
    check_equal(turbo_flow_plugin_host_destroy(t.host, 0, &pe), SALTS_EBUSY);
    check_equal(runtime_close(&t), SALTS_OK);
  }
  it("rejects aliased output slots without factory side effects and checks Graph cost overflow") {
    runtime_test_t t;
    turbo_flow_result_memory_requirements_t cost;
    turbo_flow_result_memory_requirements_init(&cost);
    check_equal(turbo_flow_result_memory_requirements(SIZE_MAX, 8, &cost), SALTS_EINVAL);
    check_equal(turbo_flow_result_memory_requirements(2, SIZE_MAX, &cost), SALTS_EINVAL);
    check_equal(runtime_open(&t, FLOW_OPERATION_OK, operation_yaml, operation_dsl), SALTS_OK);
    turbo_flow_t *original = t.flow;
    check_equal(turbo_flow_plugin_generation_create(t.snapshot, t.resolved, &t.flow, &t.config,
                                                    t.domain, &t.generation, &t.generation,
                                                    &t.error),
                SALTS_EINVAL);
    check(t.flow == original);
    check_null(t.generation);
    check_equal(atomic_load(&t.observer->preflights), 0u);
    check_equal(runtime_close(&t), SALTS_OK);
  }
  it("retains DLL state across execute, clone and destroy callback barriers until join") {
    for (int point = OP_BARRIER_EXECUTE; point <= OP_BARRIER_DESTROY; ++point) {
      runtime_test_t t;
      salts_thread_t thread;
      turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
      turbo_flow_plugin_result_domain_snapshot_v3_t state;
      runtime_worker_t worker = {&t, point, SALTS_EIO, 0};
      runtime_worker_t second = {&t, OP_BARRIER_EXECUTE, SALTS_EIO, 0};
      salts_thread_t second_thread;
      int second_started = 0, admission_status = SALTS_OK;
      int second_create = 0, generation_destroy = SALTS_OK;
      unsigned session_releases = 0;
      check_equal(runtime_open(&t, FLOW_OPERATION_OK, operation_yaml, operation_dsl), SALTS_OK);
      check_equal(runtime_create(&t), SALTS_OK);
      runtime_bind(&t, 0);
      /* Borrowed context selects Graph's existing direct inline path, not its serialized broadcast
       * ring. */
      if (point == OP_BARRIER_EXECUTE) t.input.transport_context = &t;
      check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(t.generation)), SALTS_OK);
      if (point != OP_BARRIER_EXECUTE) {
        check_equal(
            turbo_flow_publish(turbo_flow_plugin_generation_flow(t.generation), "input", &t.input),
            SALTS_OK);
        check_equal(turbo_flow_plugin_generation_destroy(t.generation, 0, &t.error), SALTS_OK);
        t.generation = NULL;
      } else check_equal(turbo_flow_plugin_generation_lease_acquire(t.generation), SALTS_OK);
      turbo_flow_resolved_config_destroy(t.resolved);
      t.resolved = NULL;
      /* Snapshot mutation is control-owned; worker bodies only invoke Graph message operations. */
      check(t.control_thread == &runtime_thread_identity);
      turbo_flow_plugin_catalog_snapshot_destroy(t.snapshot);
      t.snapshot = NULL;
      salts_mutex_lock(&t.observer->mutex);
      t.observer->barrier = point;
      salts_mutex_unlock(&t.observer->mutex);
      check_equal(salts_thread_create(&thread, runtime_worker, &worker), 0);
      salts_mutex_lock(&t.observer->mutex);
      uint64_t barrier_deadline = salts_monotonic_ms() + RUNTIME_BARRIER_TIMEOUT_MS;
      while (!t.observer->entered && salts_monotonic_ms() < barrier_deadline)
        salts_cond_timedwait(&t.observer->cond, &t.observer->mutex, UINT64_C(1000000));
      int entered = t.observer->entered;
      salts_mutex_unlock(&t.observer->mutex);
      if (!entered) {
        salts_mutex_lock(&t.observer->mutex);
        t.observer->proceed = 1;
        salts_cond_broadcast(&t.observer->cond);
        salts_mutex_unlock(&t.observer->mutex);
        check_equal(salts_thread_join(&thread), 0);
        check_equal(entered, 1);
      }
      const int domain_destroy = turbo_flow_plugin_result_domain_destroy(t.domain, &pe);
      const int host_destroy = turbo_flow_plugin_host_destroy(t.host, 0, &pe);
      const unsigned context_releases = atomic_load(&t.observer->context_releases);
      turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
      const int domain_snapshot = turbo_flow_plugin_result_domain_snapshot(t.domain, &state);
      if (point == OP_BARRIER_EXECUTE) {
        turbo_flow_plugin_operation_error_v3_t recent;
        turbo_flow_plugin_operation_error_v3_init(&recent);
        second_create = salts_thread_create(&second_thread, runtime_worker, &second);
        second_started = second_create == 0;
        uint64_t deadline = salts_monotonic_ms() + RUNTIME_BARRIER_TIMEOUT_MS;
        do {
          int query_rc = turbo_flow_plugin_generation_operation_error(t.generation, 0, &recent);
          if (query_rc != SALTS_OK) {
            admission_status = query_rc;
            break;
          }
          admission_status = recent.status;
          if (admission_status == SALTS_OK) salts_sleep_ms(1u);
        } while (second_started && admission_status == SALTS_OK && salts_monotonic_ms() < deadline);
        generation_destroy = turbo_flow_plugin_generation_destroy(t.generation, 0, &t.error);
        session_releases = atomic_load(&t.observer->session_releases);
      }
      salts_mutex_lock(&t.observer->mutex);
      t.observer->proceed = 1;
      salts_cond_broadcast(&t.observer->cond);
      salts_mutex_unlock(&t.observer->mutex);
      const int first_join = salts_thread_join(&thread);
      const int second_join = second_started ? salts_thread_join(&second_thread) : 0;
      /* All worker joins precede assertions that may leave this test through TinyTest. */
      check_equal(first_join, 0);
      check_equal(second_join, 0);
      check_equal(domain_destroy, SALTS_EBUSY);
      check_equal(host_destroy, SALTS_EBUSY);
      check_equal(context_releases, 0u);
      check_equal(domain_snapshot, SALTS_OK);
      check_greater(state.outstanding, (size_t)0);
      if (point == OP_BARRIER_EXECUTE) {
        check_equal(second_create, 0);
        check_equal(generation_destroy, SALTS_EBUSY);
        check_equal(session_releases, 0u);
      }
      check_equal(worker.status, SALTS_OK);
      check(worker.thread != t.control_thread);
      if (second_started) {
        info("second publish: observed admission=%d completed=%d", admission_status, second.status);
        check_equal(admission_status, SALTS_ENOSPC);
        check_equal(second.status, SALTS_ENOSPC);
        check_equal(atomic_load(&t.observer->executes), 1u);
      }
      if (point == OP_BARRIER_CLONE) check_equal(worker.observed, 14);
      if (point == OP_BARRIER_EXECUTE)
        check_equal(turbo_flow_plugin_generation_lease_release(t.generation), SALTS_OK);
      check_equal(runtime_close(&t), SALTS_OK);
      check_equal(atomic_load(&t.unloads), 1u);
    }
  }
  it("keeps a detached context after EBUSY and failed release and never reattaches it") {
    runtime_test_t t;
    turbo_flow_msg_t clone;
    turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_plugin_result_domain_snapshot_v3_t state;
    check_equal(runtime_open(&t, FLOW_OPERATION_OK, operation_yaml, operation_dsl), SALTS_OK);
    check_equal(runtime_create(&t), SALTS_OK);
    runtime_bind(&t, 0);
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(t.generation)), SALTS_OK);
    check_equal(
        turbo_flow_publish(turbo_flow_plugin_generation_flow(t.generation), "input", &t.input),
        SALTS_OK);
    check_equal(turbo_flow_plugin_result_domain_destroy(t.domain, &pe), SALTS_EBUSY);
    turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
    check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
    check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED);
    check_equal(turbo_flow_plugin_generation_destroy(t.generation, 0, &t.error), SALTS_OK);
    t.generation = NULL;
    check_equal(atomic_load(&t.observer->session_releases), 1u);
    turbo_flow_msg_init(&clone);
    check_equal(turbo_flow_msg_clone(&clone, &t.result), SALTS_OK);
    check_equal(*(const int *)turbo_flow_msg_result(&clone, NULL, NULL), 14);
    t.flow = turbo_flow_create();
    check_not_null(t.flow);
    check_equal(turbo_flow_parse_string(t.flow, operation_dsl, strlen(operation_dsl)), SALTS_OK);
    turbo_flow_t *unconsumed = t.flow;
    check_equal(runtime_create(&t), SALTS_EINVAL);
    check(t.flow == unconsumed);
    check_null(t.generation);
    check_null(t.cleanup);
    turbo_flow_resolved_config_destroy(t.resolved);
    t.resolved = NULL;
    turbo_flow_plugin_catalog_snapshot_destroy(t.snapshot);
    t.snapshot = NULL;
    check_equal(turbo_flow_plugin_result_domain_destroy(t.domain, &pe), SALTS_EBUSY);
    turbo_flow_msg_cleanup(&clone);
    turbo_flow_msg_cleanup(&t.result);
    t.observer->mode = OP_FIXTURE_RELEASE_CONTEXT_ONCE;
    check_equal(turbo_flow_plugin_result_domain_destroy(t.domain, &pe), SALTS_EIO);
    check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
    check_equal(state.last_cleanup_status, SALTS_EIO);
    check_equal(state.owner_count, (size_t)1);
    check_equal(state.outstanding, (size_t)0);
    check_equal(state.retained_bytes, (size_t)0);
    check_equal(atomic_load(&t.observer->context_releases), 1u);
    check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
    check_equal(atomic_load(&t.observer->context_releases), 1u);
    check_equal(turbo_flow_plugin_host_destroy(t.host, 0, &pe), SALTS_EBUSY);
    check_equal(runtime_close(&t), SALTS_OK);
  }
  it("rejects configuration before transfer and before every factory") {
    static const struct {
      const char *before, *after;
      int status;
    } cases[] = {{"fixture.double", "fixture.missing", SALTS_EINVAL},
                 {"fixture.operation", "fixture.missing", SALTS_EINVAL},
                 {"    version: 1", "    version: 2", SALTS_EINVAL},
                 {"input_schema: cmeta.int.data", "input_schema: missing", SALTS_EPROTO},
                 {"output_schema_version: 1", "output_schema_version: 2", SALTS_EPROTO},
                 {"thread_safe", "owner", SALTS_ENOTSUP},
                 {"execution: inline", "execution: thread", SALTS_ENOTSUP},
                 {"execution: inline", "execution: coro", SALTS_ENOTSUP},
                 {"cancellation: none", "cancellation: cooperative", SALTS_ENOTSUP},
                 {"deadline_ms: 0", "deadline_ms: 1", SALTS_ENOTSUP},
                 {"permissions: []", "permissions: [network]", SALTS_ENOTSUP},
                 {"max_inflight: 1", "max_inflight: 9", SALTS_ENOSPC},
                 {"max_input_bytes: 8", "max_input_bytes: 65", SALTS_ENOSPC},
                 {"max_result_bytes: 8\n    max_retained_bytes: 32",
                  "max_result_bytes: 65\n    max_retained_bytes: 128", SALTS_ENOSPC},
                 {"max_retained_bytes: 32", "max_retained_bytes: 513", SALTS_ENOSPC},
                 {"max_steps: 2", "max_steps: 9", SALTS_ENOSPC},
                 {"max_inflight: 1", "max_inflight: 5", SALTS_ENOSPC}};
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
      runtime_test_t t;
      char yaml[RUNTIME_YAML_BYTES];
      turbo_flow_plugin_result_domain_snapshot_v3_t state;
      runtime_yaml(yaml, cases[i].before, cases[i].after);
      check_equal(runtime_open(&t, FLOW_OPERATION_OK, yaml, operation_dsl), SALTS_OK);
      turbo_flow_t *original = t.flow;
      int rc = runtime_create(&t);
      info("case %zu: %d %s", i, rc, t.error.path);
      check_equal(rc, cases[i].status);
      check(t.flow == original);
      if (i == 1) check_equal(t.error.path, "$.operation_bindings[0].plugin");
      if (i == 2) check_equal(t.error.path, "$.operation_bindings[0].version");
      check_null(t.generation);
      check_null(t.cleanup);
      check_equal(atomic_load(&t.observer->context_creates), 0u);
      check_equal(atomic_load(&t.observer->session_creates), 0u);
      turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
      check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
      check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_READY);
      check_equal(runtime_close(&t), SALTS_OK);
    }
  }
  it("uses the Graph public cost query for exact budget admission") {
    for (int doubled = 0; doubled < 2; ++doubled) {
      for (int less = 0; less < 2; ++less) {
        runtime_test_t t;
        turbo_flow_plugin_operation_catalog_v3_t catalog;
        turbo_flow_result_memory_requirements_t cost;
        turbo_flow_plugin_result_domain_snapshot_v3_t state;
        char yaml[RUNTIME_YAML_BYTES];
        runtime_yaml(yaml, "max_retained_bytes: 32",
                     doubled ? "max_retained_bytes: 64" : "max_retained_bytes: 32");
        check_equal(runtime_open(&t, FLOW_OPERATION_OK, yaml, operation_dsl), SALTS_OK);
        turbo_flow_plugin_operation_catalog_v3_init(&catalog);
        check_equal(turbo_flow_plugin_catalog_snapshot_operation_catalog(t.snapshot, &catalog),
                    SALTS_OK);
        turbo_flow_result_memory_requirements_init(&cost);
        check(catalog.entries[0].operation.input.data != &cmeta_data_int);
        check_equal(turbo_flow_data_schema_match(catalog.entries[0].operation.input.projection,
                                                 catalog.entries[0].operation.input.data,
                                                 &input_schema, &cmeta_data_int),
                    SALTS_OK);
        check_equal(turbo_flow_result_memory_requirements(doubled ? 8 : 4, 8, &cost), SALTS_OK);
        info("Graph costs O=%zu K=%zu M=%zu peak=%zu; Host H=%zu", cost.owner_bytes,
             cost.claim_bytes, cost.message_bytes, cost.peak_metadata_bytes,
             sizeof(flow_plugin_operation_binding_t) + sizeof(flow_plugin_result_entry_t));
        t.config.operation_memory_budget_bytes =
            cost.peak_metadata_bytes + sizeof(flow_plugin_operation_binding_t) +
            sizeof(flow_plugin_result_entry_t) + catalog.entries[0].operation.max_session_bytes +
            catalog.entries[0].operation.max_result_context_bytes - less;
        turbo_flow_t *original = t.flow;
        check_equal(runtime_create(&t), less ? SALTS_ENOSPC : SALTS_OK);
        if (less) {
          check(t.flow == original);
          check_null(t.generation);
          check_null(t.cleanup);
          check_equal(atomic_load(&t.observer->context_creates), 0u);
          check_equal(atomic_load(&t.observer->session_creates), 0u);
          turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
          check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
          check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_READY);
        } else check_null(t.flow);
        check_equal(runtime_close(&t), SALTS_OK);
      }
    }
  }
  it("never implicitly replaces a result at a repeated operation stage") {
    static const char dsl[] =
        "source input\nstage first operation fixture.double\n"
        "stage second operation fixture.double\nstage main {\n input -> first -> second\n}\n";
    runtime_test_t t;
    turbo_flow_plugin_result_domain_snapshot_v3_t state;
    check_equal(runtime_open(&t, FLOW_OPERATION_OK, operation_yaml, dsl), SALTS_OK);
    check_equal(runtime_create(&t), SALTS_OK);
    runtime_bind(&t, 0);
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(t.generation)), SALTS_OK);
    check_equal(
        turbo_flow_publish(turbo_flow_plugin_generation_flow(t.generation), "input", &t.input),
        SALTS_EALREADY);
    check_equal(atomic_load(&t.observer->executes), 1u);
    turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
    check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
    check_equal(state.outstanding, (size_t)0);
    check_equal(state.retained_bytes, (size_t)0);
    check_equal(runtime_close(&t), SALTS_OK);
  }
  it("preserves factory errors and owns only independent returned contexts") {
    const int modes[] = {OP_FIXTURE_PREFLIGHT_FAIL,        OP_FIXTURE_CONTEXT_FAIL,
                         OP_FIXTURE_CONTEXT_FAIL_VALUE,    OP_FIXTURE_SESSION_FAIL,
                         OP_FIXTURE_SESSION_FAIL_VALUE,    OP_FIXTURE_CONTEXT_FACTORY_ALIAS,
                         OP_FIXTURE_SESSION_FACTORY_ALIAS, OP_FIXTURE_SESSION_RESULT_ALIAS};
    for (size_t i = 0; i < sizeof(modes) / sizeof(modes[0]); ++i) {
      runtime_test_t t;
      turbo_flow_plugin_result_domain_snapshot_v3_t state;
      check_equal(runtime_open(&t, FLOW_OPERATION_OK, operation_yaml, operation_dsl), SALTS_OK);
      t.observer->mode = modes[i];
      turbo_flow_t *original = t.flow;
      check_equal(runtime_create(&t), i < 5 ? SALTS_EIO : SALTS_EPROTO);
      check_null(t.generation);
      check_null(t.cleanup);
      if (!i) check(t.flow == original);
      else check_null(t.flow);
      check_equal(atomic_load(&t.observer->sessions), 0u);
      check_equal(atomic_load(&t.observer->session_releases), i == 4 ? 1u : 0u);
      turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
      check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
      check_equal(state.state, (uint32_t)(i ? TURBO_FLOW_PLUGIN_RESULT_DOMAIN_DETACHED
                                            : TURBO_FLOW_PLUGIN_RESULT_DOMAIN_READY));
      check_equal(runtime_close(&t), SALTS_OK);
    }
  }
  it("keeps the original registration collision while cleanup retries exactly once") {
    runtime_test_t t;
    turbo_flow_operation_provider_registration_t provider =
        TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
    turbo_flow_plugin_result_domain_snapshot_v3_t state;
    turbo_flow_config_error_t cleanup_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    check_equal(runtime_open(&t, FLOW_OPERATION_OK, operation_yaml, operation_dsl), SALTS_OK);
    provider.operation_name = "fixture.double";
    provider.fn = capture_result;
    provider.ctx = &t.result;
    check_equal(turbo_flow_register_operation_provider(t.flow, &provider), SALTS_OK);
    t.observer->mode = OP_FIXTURE_RELEASE_SESSION_ONCE;
    check_equal(runtime_create(&t), SALTS_EALREADY);
    check_equal(t.error.status, SALTS_EALREADY);
    check_null(t.generation);
    check_null(t.flow);
    check_not_null(t.cleanup);
    check_null(turbo_flow_plugin_generation_flow(t.cleanup));
    check_equal(turbo_flow_plugin_generation_state(t.cleanup),
                TURBO_FLOW_PLUGIN_GENERATION_FAILED_CLEANUP);
    check_equal(turbo_flow_plugin_generation_cleanup_error(t.cleanup, &cleanup_error), SALTS_OK);
    check_equal(cleanup_error.status, SALTS_EIO);
    check_equal(turbo_flow_plugin_generation_poll(t.cleanup, 0, &cleanup_error), SALTS_EBUSY);
    turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
    check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
    check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED);
    check_equal(atomic_load(&t.observer->session_releases), 1u);
    check_equal(turbo_flow_plugin_generation_destroy(t.cleanup, 0, &cleanup_error), SALTS_OK);
    t.cleanup = NULL;
    check_equal(atomic_load(&t.observer->session_releases), 2u);
    check_equal(atomic_load(&t.observer->contexts), 1u);
    check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
    check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_DETACHED);
    check_equal(runtime_close(&t), SALTS_OK);
  }
  it("charges sticky budgets and rejects NULL or internal-field aliases without destroying "
     "borrowed memory") {
    const int modes[] = {OP_FIXTURE_EXECUTE_FAIL_VALUE,
                         OP_FIXTURE_EXECUTE_NULL,
                         OP_FIXTURE_EXECUTE_ALIAS,
                         OP_FIXTURE_EXECUTE_FAIL_ALIAS,
                         OP_FIXTURE_SWALLOW_CHARGE,
                         OP_FIXTURE_ZERO_CHARGE,
                         OP_FIXTURE_OK,
                         OP_FIXTURE_EXECUTE_FAIL_NULL};
    const int statuses[] = {SALTS_EIO,    SALTS_EPROTO, SALTS_EPROTO, SALTS_EIO,
                            SALTS_ENOSPC, SALTS_EINVAL, SALTS_ENOSPC, SALTS_EIO};
    const unsigned destroys[] = {1, 0, 0, 0, 1, 0, 0, 0};
    uint32_t phases[sizeof(modes) / sizeof(modes[0])] = {0};
    for (size_t i = 0; i < sizeof(modes) / sizeof(modes[0]); ++i) {
      runtime_test_t t;
      char yaml[RUNTIME_YAML_BYTES];
      turbo_flow_plugin_operation_error_v3_t error;
      turbo_flow_plugin_result_domain_snapshot_v3_t state;
      int alias = i == 2 || i == 3;
      const char *path = i == 2   ? FLOW_OPERATION_EXECUTE_ALIAS
                         : i == 3 ? FLOW_OPERATION_EXECUTE_FAIL_ALIAS
                                  : FLOW_OPERATION_OK;
      runtime_yaml(yaml, alias ? "input_schema: cmeta.int.data" : "max_steps: 2",
                   alias ? "input_schema: operation.Record.data"
                         : (i == 4 || i == 6 ? "max_steps: 1" : "max_steps: 2"));
      check_equal(runtime_open(&t, path, yaml, operation_dsl), SALTS_OK);
      t.observer->mode = modes[i];
      check_equal(runtime_create(&t), SALTS_OK);
      runtime_bind(&t, alias);
      const void *original = turbo_flow_msg_projection(&t.input, NULL);
      void *content = t.input._content_handle;
      turbo_flow_msg_t before = t.input;
      const turbo_flow_content_descriptor_t *descriptor =
          turbo_flow_msg_content_descriptor(&t.input);
      turbo_flow_content_descriptor_t descriptor_before = *descriptor;
      check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(t.generation)), SALTS_OK);
      int rc =
          turbo_flow_publish(turbo_flow_plugin_generation_flow(t.generation), "input", &t.input);
      info("execute mode %d: %d", modes[i], rc);
      check_equal(rc, statuses[i]);
      check_equal(atomic_load(&t.observer->executes), 1u);
      check_equal(atomic_load(&t.observer->destroys), destroys[i]);
      check(turbo_flow_msg_projection(&t.input, NULL) == original);
      check(t.input._content_handle == content);
      check_equal(memcmp(&t.input, &before, sizeof(before)), 0);
      check_equal(memcmp(descriptor, &descriptor_before, sizeof(descriptor_before)), 0);
      check_equal(t.input.payload.len, strlen("original-wire-bytes"));
      check_equal(memcmp(t.input.payload.data, "original-wire-bytes", t.input.payload.len), 0);
      check_equal(*(const int *)original, 7);
      if (alias) check_equal(((const int *)original)[1], 19);
      check_null(turbo_flow_msg_result(&t.result, NULL, NULL));
      turbo_flow_plugin_result_domain_snapshot_v3_init(&state);
      check_equal(turbo_flow_plugin_result_domain_snapshot(t.domain, &state), SALTS_OK);
      check_equal(state.outstanding, (size_t)0);
      check_equal(state.retained_bytes, (size_t)0);
      turbo_flow_plugin_operation_error_v3_init(&error);
      check_equal(turbo_flow_plugin_generation_operation_error(t.generation, 0, &error), SALTS_OK);
      check_equal(error.status, statuses[i]);
      phases[i] = error.phase;
      info("execute mode %d phase %u", modes[i], error.phase);
      if (modes[i] == OP_FIXTURE_EXECUTE_FAIL_NULL) check_equal(error.engine_status, SALTS_EIO);
      check_equal(runtime_close(&t), SALTS_OK);
    }
    for (size_t i = sizeof(modes) / sizeof(modes[0]); i-- > 0;)
      check_equal(phases[i],
                  (uint32_t)(i == 1 || i == 2 ? TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT
                                              : TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE));
  }
  it("executes the real DLL and keeps results valid beyond generation retirement") {
    turbo_flow_plugin_host_config_t hc = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
    turbo_flow_plugin_generation_config_t gc = TURBO_FLOW_PLUGIN_GENERATION_CONFIG_INIT;
    turbo_flow_plugin_error_t pe = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t ce = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_result_domain_t *domain = NULL;
    turbo_flow_plugin_generation_t *generation = NULL, *cleanup = NULL;
    turbo_flow_resolved_config_t *resolved = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t input, result, clone;
    turbo_flow_operation_descriptor_t metadata = {0};
    turbo_flow_operation_provider_registration_t capture =
        TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
    turbo_flow_plugin_result_domain_snapshot_v3_t ds;
    int *value = malloc(sizeof(int));
    *value = 7;
    turbo_flow_msg_init(&input);
    turbo_flow_msg_init(&result);
    turbo_flow_msg_init(&clone);
    metadata.size = sizeof(metadata);
    metadata.name = "fixture.double";
    metadata.version = 1;
    metadata.domain = metadata.input_domain = metadata.output_domain = TURBO_FLOW_DOMAIN_DATA;
    metadata.input_type = metadata.output_type = "Message";
    metadata.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
    metadata.scope.authority = TURBO_FLOW_AUTHORITY_DATA_MUTATION;
    metadata.flags = TURBO_FLOW_OPERATION_STAGE;
    metadata.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
    check_equal(turbo_flow_register_operation(flow, &metadata), SALTS_OK);
    metadata.name = "fixture.capture";
    check_equal(turbo_flow_register_operation(flow, &metadata), SALTS_OK);
    capture.operation_name = metadata.name;
    capture.fn = capture_result;
    capture.ctx = &result;
    check_equal(turbo_flow_register_operation_provider(flow, &capture), SALTS_OK);
    check_equal(turbo_flow_parse_string(flow, operation_dsl, sizeof(operation_dsl) - 1), SALTS_OK);
    check_equal(turbo_flow_plugin_host_create(&hc, &host, &pe), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, FLOW_OPERATION_OK, &pe), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &pe), SALTS_OK);
    check_equal(turbo_flow_plugin_result_domain_create(snapshot, 1, &domain, &pe), SALTS_OK);
    check_equal(
        turbo_flow_config_resolve_yaml(operation_yaml, sizeof(operation_yaml) - 1, &resolved, &ce),
        SALTS_OK);
    int create_rc = turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &gc, domain,
                                                        &generation, &cleanup, &ce);
    info("generation create: %d %s %s", create_rc, ce.path, ce.message);
    check_equal(create_rc, SALTS_OK);
    check_null(flow);
    check_null(cleanup);
    turbo_flow_plugin_result_domain_snapshot_v3_init(&ds);
    check_equal(turbo_flow_plugin_result_domain_snapshot(domain, &ds), SALTS_OK);
    check_equal(ds.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED);
    check_equal(turbo_flow_plugin_result_domain_destroy(domain, &pe), SALTS_EBUSY);
    check_equal(turbo_flow_msg_bind_typed_projection(&input, &input_schema, &cmeta_data_int, value,
                                                     copy_int, free_int, NULL),
                SALTS_OK);
    check_equal(turbo_flow_start(turbo_flow_plugin_generation_flow(generation)), SALTS_OK);
    check_equal(turbo_flow_publish(turbo_flow_plugin_generation_flow(generation), "input", &input),
                SALTS_OK);
    check_equal(*(const int *)turbo_flow_msg_result(&result, NULL, NULL), 14);
    check_equal(*(const int *)turbo_flow_msg_projection(&result, NULL), 7);
    check_equal(turbo_flow_plugin_generation_destroy(generation, 0, &ce), SALTS_OK);
    generation = NULL;
    turbo_flow_resolved_config_destroy(resolved);
    resolved = NULL;
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    snapshot = NULL;
    check_equal(turbo_flow_msg_clone(&clone, &result), SALTS_OK);
    check_equal(*(const int *)turbo_flow_msg_result(&clone, NULL, NULL), 14);
    check_equal(turbo_flow_plugin_result_domain_destroy(domain, &pe), SALTS_EBUSY);
    turbo_flow_msg_cleanup(&clone);
    turbo_flow_msg_cleanup(&result);
    turbo_flow_msg_cleanup(&input);
    check_equal(turbo_flow_plugin_result_domain_destroy(domain, &pe), SALTS_OK);
    check_equal(turbo_flow_plugin_host_destroy(host, 0, &pe), SALTS_OK);
  }
}
