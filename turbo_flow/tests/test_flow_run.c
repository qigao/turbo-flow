#include "../../tests/flow_operation_fixture.h"
#include "tinytest.h"
#include "turbo_flow.h"

#include <cflow/publishers.h>

#include <stddef.h>
#include <stdatomic.h>
#include <string.h>

int flow_run_cpp_header_probe(void);

typedef struct flow_run_probe_s {
  size_t calls;
  uint64_t ids[4];
  int status;
} flow_run_probe_t;

typedef struct flow_run_wait_source_s {
  turbo_flow_msg_t message;
  cflow_waker waker;
  atomic_size_t resumes;
  atomic_size_t arms;
  atomic_size_t wait_cancels;
  atomic_size_t source_cancels;
  atomic_size_t destroys;
  atomic_int ready;
  int return_done;
  int return_error;
} flow_run_wait_source_t;

static bool flow_run_waitable_arm(void *state, cflow_waker waker) {
  flow_run_wait_source_t *source = (flow_run_wait_source_t *)state;
  source->waker = waker;
  (void)atomic_fetch_add_explicit(&source->arms, 1u, memory_order_relaxed);
  return true;
}

static void flow_run_waitable_cancel(void *state) {
  flow_run_wait_source_t *source = (flow_run_wait_source_t *)state;
  source->waker = (cflow_waker){0};
  (void)atomic_fetch_add_explicit(&source->wait_cancels, 1u, memory_order_relaxed);
}

CMETA_IMPLEMENTS(cflow_waitable, flow_run_waitable, 0,
                 .arm = flow_run_waitable_arm,
                 .cancel = flow_run_waitable_cancel);

static const char *flow_run_wait_source_name(void *state) {
  (void)state;
  return "flow-run-wait-source";
}

static const cmeta_type_desc *flow_run_wait_source_type(void *state) {
  (void)state;
  return turbo_flow_message_type();
}

static cflow_step flow_run_wait_source_resume(void *state, cflow_publish_context *context,
                                              void *out_value) {
  flow_run_wait_source_t *source = (flow_run_wait_source_t *)state;
  (void)context;
  (void)atomic_fetch_add_explicit(&source->resumes, 1u, memory_order_relaxed);
  if (source->return_error) {
    return (cflow_step){CFLOW_STEP_ERROR, {0}, "publisher failed"};
  }
  if (source->return_done) return (cflow_step){CFLOW_STEP_DONE, {0}, NULL};
  if (!atomic_load_explicit(&source->ready, memory_order_acquire)) {
    return (cflow_step){CFLOW_STEP_WAIT,
                        flow_run_waitable_as_cflow_waitable(source), NULL};
  }
  if (turbo_flow_msg_clone((turbo_flow_msg_t *)out_value, &source->message) != SALTS_OK) {
    return (cflow_step){CFLOW_STEP_ERROR, {0}, "message clone failed"};
  }
  return (cflow_step){CFLOW_STEP_VALUE_AND_DONE, {0}, NULL};
}

static void flow_run_wait_source_cancel(void *state) {
  flow_run_wait_source_t *source = (flow_run_wait_source_t *)state;
  (void)atomic_fetch_add_explicit(&source->source_cancels, 1u, memory_order_relaxed);
}

static void flow_run_wait_source_destroy(void *state) {
  flow_run_wait_source_t *source = (flow_run_wait_source_t *)state;
  (void)atomic_fetch_add_explicit(&source->destroys, 1u, memory_order_relaxed);
}

static void flow_run_wait_source_bind(void *state, cflow_waker waker) {
  (void)state;
  (void)waker;
}

static cflow_publisher_terminal flow_run_wait_source_poll(void *state, const char **error) {
  (void)state;
  if (error) *error = NULL;
  return CFLOW_PUBLISHER_OPEN;
}

CMETA_IMPLEMENTS(cflow_publisher, flow_run_wait_source,
                 CFLOW_PUBLISHER_CAP_CONSTRUCTS_VALUES,
                 .name = flow_run_wait_source_name,
                 .output_type = flow_run_wait_source_type,
                 .resume = flow_run_wait_source_resume,
                 .cancel = flow_run_wait_source_cancel,
                 .destroy = flow_run_wait_source_destroy,
                 .bind_terminal_waker = flow_run_wait_source_bind,
                 .poll_terminal = flow_run_wait_source_poll);

static void flow_run_noop_task(void *ctx) { (void)ctx; }

static int flow_run_probe_stage(turbo_flow_msg_t *message, void *ctx) {
  flow_run_probe_t *probe = (flow_run_probe_t *)ctx;
  if (probe->calls < sizeof(probe->ids) / sizeof(probe->ids[0])) {
    probe->ids[probe->calls] = message->id;
  }
  ++probe->calls;
  return probe->status;
}

static turbo_flow_t *flow_run_test_flow_with_capacity(flow_run_probe_t *probe, size_t capacity) {
  static const char source[] = "source input\n"
                               "stage sink operation test.sink\n"
                               "stage main {\n"
                               "  input -> sink\n"
                               "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  turbo_flow_async_ingress_config_t ingress = TURBO_FLOW_ASYNC_INGRESS_CONFIG_INIT;
  ingress.workers = 1u;
  ingress.queue_capacity = capacity;
  flow_test_operation_t operation_sink =
      flow_test_operation_init("test.sink", flow_run_probe_stage, probe);
  operation_sink.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
  operation_sink.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
  if (!flow || turbo_flow_configure_async_ingress(flow, &ingress) != SALTS_OK ||
      turbo_flow_parse_string(flow, source, strlen(source)) != SALTS_OK ||
      flow_test_operation_register(flow, &operation_sink) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

static turbo_flow_t *flow_run_test_flow(flow_run_probe_t *probe) {
  return flow_run_test_flow_with_capacity(probe, TURBO_FLOW_ASYNC_INGRESS_DEFAULT_CAPACITY);
}

spec("flow_run") {
  it("exposes a size and version checked opaque run contract") {
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_config_t config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_publisher publisher = {0};

    check_null(run);
    check_equal(config.size, sizeof(config));
    check_equal(config.version, TURBO_FLOW_RUN_API_VERSION);
    check_null(config.scheduler);
    check_equal(config.deadline_ms, 0u);
    check_equal(result.size, sizeof(result));
    check_equal(result.version, TURBO_FLOW_RUN_API_VERSION);
    check_equal(result.state, TURBO_FLOW_RUN_OPEN);
    check_equal(result.status, SALTS_OK);
    check_equal(result.values, 0u);
    check_equal(result.outstanding_demand, 0u);
    check_equal(flow_run_cpp_header_probe(), 0);
    check_equal(turbo_flow_run_open(NULL, "input", &publisher, &config, &run), SALTS_EINVAL);
    check_equal(turbo_flow_run_request(NULL, 1u), SALTS_EINVAL);
    check_equal(turbo_flow_run_snapshot(NULL, &result), SALTS_EINVAL);
    check_equal(turbo_flow_run_wait(NULL, 0u, &result), SALTS_EINVAL);
    check_equal(turbo_flow_run_cancel(NULL), SALTS_EINVAL);
    turbo_flow_run_close(NULL);
  }

  it("advances only requested values and completes VALUE_AND_DONE exactly once") {
    flow_run_probe_t probe = {0};
    turbo_flow_msg_t messages[2];
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_config_t config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_scheduler scheduler = {0};
    cflow_publisher publisher = {0};
    turbo_flow_t *flow = flow_run_test_flow(&probe);

    check_not_null(flow);
    turbo_flow_msg_init(&messages[0]);
    turbo_flow_msg_init(&messages[1]);
    messages[0].id = 17u;
    messages[1].id = 23u;
    check_true(cflow_scheduler_inline_init(&scheduler));
    check_true(cflow_publisher_from_array(&publisher, turbo_flow_message_type(), messages, 2u));
    config.scheduler = &scheduler;

    check_equal(turbo_flow_run_open(flow, "input", &publisher, &config, &run), SALTS_OK);
    check_not_null(run);
    check_false(cflow_publisher_valid(&publisher));
    check_equal(turbo_flow_run_snapshot(run, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_RUN_OPEN);
    check_equal(result.values, 0u);
    check_equal(probe.calls, 0u);

    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check_equal(turbo_flow_run_wait(run, 0u, &result), SALTS_ETIMEDOUT);
    check_equal(result.state, TURBO_FLOW_RUN_ACTIVE);
    check_equal(result.values, 1u);
    check_equal(result.outstanding_demand, 0u);
    check_equal(probe.calls, 1u);
    check_equal(probe.ids[0], 17u);

    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_RUN_COMPLETED);
    check_equal(result.values, 2u);
    check_equal(probe.calls, 2u);
    check_equal(probe.ids[1], 23u);

    turbo_flow_run_close(run);
    cflow_scheduler_destroy(&scheduler);
    turbo_flow_msg_cleanup(&messages[0]);
    turbo_flow_msg_cleanup(&messages[1]);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("keeps graph failure distinct from admission and stores the first error") {
    flow_run_probe_t probe = {0};
    turbo_flow_msg_t message;
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_config_t config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_scheduler scheduler = {0};
    cflow_publisher publisher = {0};
    turbo_flow_t *flow;

    probe.status = SALTS_EPROTO;
    flow = flow_run_test_flow(&probe);
    check_not_null(flow);
    turbo_flow_msg_init(&message);
    message.id = 41u;
    check_true(cflow_scheduler_inline_init(&scheduler));
    check_true(cflow_publisher_from_array(&publisher, turbo_flow_message_type(), &message, 1u));
    config.scheduler = &scheduler;

    check_equal(turbo_flow_run_open(flow, "input", &publisher, &config, &run), SALTS_OK);
    check_equal(turbo_flow_run_request(run, 1u), SALTS_EPROTO);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_EPROTO);
    check_equal(result.state, TURBO_FLOW_RUN_FAILED);
    check_equal(result.status, SALTS_EPROTO);
    check_equal(result.error.code, SALTS_EPROTO);
    check_equal(result.values, 0u);
    check_equal(probe.calls, 1u);

    turbo_flow_run_close(run);
    cflow_scheduler_destroy(&scheduler);
    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("resumes WAIT only through its waker and cancels the registration") {
    flow_run_probe_t probe = {0};
    flow_run_wait_source_t source = {0};
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_config_t config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_scheduler scheduler = {0};
    cflow_publisher publisher;
    turbo_flow_t *flow = flow_run_test_flow(&probe);

    check_not_null(flow);
    turbo_flow_msg_init(&source.message);
    source.message.id = 73u;
    publisher = flow_run_wait_source_as_cflow_publisher(&source);
    check_true(cflow_scheduler_inline_init(&scheduler));
    config.scheduler = &scheduler;
    check_equal(turbo_flow_run_open(flow, "input", &publisher, &config, &run), SALTS_OK);
    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check_equal(atomic_load(&source.resumes), 1u);
    check_equal(atomic_load(&source.arms), 1u);
    check_not_null(source.waker.wake);
    check_equal(turbo_flow_run_wait(run, 0u, &result), SALTS_ETIMEDOUT);
    check_equal(result.outstanding_demand, 1u);
    check_equal(probe.calls, 0u);

    atomic_store(&source.ready, 1);
    source.waker.wake(source.waker.user);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_RUN_COMPLETED);
    check_equal(result.values, 1u);
    check_equal(probe.calls, 1u);
    check_equal(probe.ids[0], 73u);

    turbo_flow_run_close(run);
    check_equal(atomic_load(&source.destroys), 1u);
    cflow_scheduler_destroy(&scheduler);
    turbo_flow_msg_cleanup(&source.message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("retains demand after full admission and distinguishes closed admission") {
    flow_run_probe_t probe = {0};
    turbo_flow_msg_t message;
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_config_t config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_scheduler scheduler = {0};
    cflow_publisher publisher = {0};
    turbo_flow_t *flow = flow_run_test_flow(&probe);

    check_not_null(flow);
    turbo_flow_msg_init(&message);
    message.id = 89u;
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 1u));
    check(cflow_scheduler_post(&scheduler, flow_run_noop_task, NULL) != 0u);
    check_true(cflow_publisher_from_array(&publisher, turbo_flow_message_type(), &message, 1u));
    config.scheduler = &scheduler;
    check_equal(turbo_flow_run_open(flow, "input", &publisher, &config, &run), SALTS_OK);
    check_equal(turbo_flow_run_request(run, 1u), SALTS_ENOSPC);
    check_equal(turbo_flow_run_snapshot(run, &result), SALTS_OK);
    check_equal(result.outstanding_demand, 1u);
    check_equal(cflow_scheduler_run_until_idle(&scheduler, 0u), 1u);
    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check(cflow_scheduler_run_until_idle(&scheduler, 0u) >= 1u);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_OK);
    check_equal(result.values, 1u);
    turbo_flow_run_close(run);
    cflow_scheduler_destroy(&scheduler);

    run = NULL;
    publisher = (cflow_publisher){0};
    check_true(cflow_scheduler_manual_init_with_capacity(&scheduler, 1u));
    check_true(cflow_publisher_from_array(&publisher, turbo_flow_message_type(), &message, 1u));
    config.scheduler = &scheduler;
    check_equal(turbo_flow_run_open(flow, "input", &publisher, &config, &run), SALTS_OK);
    check_true(cflow_scheduler_shutdown(&scheduler));
    check_equal(turbo_flow_run_request(run, 1u), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_run_cancel(run), SALTS_OK);
    turbo_flow_run_close(run);
    cflow_scheduler_destroy(&scheduler);

    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("cancels WAIT on deadline and on flow stop with one terminal result") {
    flow_run_probe_t probe = {0};
    flow_run_wait_source_t deadline_source = {0};
    flow_run_wait_source_t stop_source = {0};
    turbo_flow_run_t *deadline_run = NULL;
    turbo_flow_run_t *stop_run = NULL;
    turbo_flow_run_config_t deadline_config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_publisher deadline_publisher;
    cflow_publisher stop_publisher;
    turbo_flow_t *flow = flow_run_test_flow(&probe);

    check_not_null(flow);
    turbo_flow_msg_init(&deadline_source.message);
    turbo_flow_msg_init(&stop_source.message);
    deadline_publisher = flow_run_wait_source_as_cflow_publisher(&deadline_source);
    deadline_config.deadline_ms = 10u;
    check_equal(turbo_flow_run_open(flow, "input", &deadline_publisher, &deadline_config,
                                    &deadline_run), SALTS_OK);
    check_equal(turbo_flow_run_request(deadline_run, 1u), SALTS_OK);
    check_equal(turbo_flow_run_wait(deadline_run, 2000u, &result), SALTS_ETIMEDOUT);
    check_equal(result.state, TURBO_FLOW_RUN_FAILED);
    check_equal(result.status, SALTS_ETIMEDOUT);
    check_equal(turbo_flow_run_cancel(deadline_run), SALTS_EALREADY);
    check(atomic_load(&deadline_source.wait_cancels) >= 1u);
    turbo_flow_run_close(deadline_run);
    check_equal(atomic_load(&deadline_source.source_cancels), 1u);

    stop_publisher = flow_run_wait_source_as_cflow_publisher(&stop_source);
    check_equal(turbo_flow_run_open(flow, "input", &stop_publisher, NULL, &stop_run), SALTS_OK);
    check_equal(turbo_flow_run_request(stop_run, 1u), SALTS_OK);
    for (size_t i = 0u; i < 2000u && atomic_load(&stop_source.arms) == 0u; ++i)
      salts_sleep_ms(1u);
    check_equal(atomic_load(&stop_source.arms), 1u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    result = (turbo_flow_run_result_t)TURBO_FLOW_RUN_RESULT_INIT;
    check_equal(turbo_flow_run_wait(stop_run, UINT64_MAX, &result), SALTS_ECANCELED);
    check_equal(result.state, TURBO_FLOW_RUN_CANCELED);
    check_equal(result.status, SALTS_ECANCELED);
    check_equal(turbo_flow_run_cancel(stop_run), SALTS_EALREADY);
    check_equal(atomic_load(&stop_source.source_cancels), 1u);
    check(atomic_load(&stop_source.wait_cancels) >= 1u);
    turbo_flow_run_close(stop_run);

    turbo_flow_msg_cleanup(&deadline_source.message);
    turbo_flow_msg_cleanup(&stop_source.message);
    turbo_flow_destroy(flow);
  }

  it("keeps a detached terminal handle valid after flow destruction") {
    flow_run_probe_t probe = {0};
    flow_run_wait_source_t source = {0};
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_config_t config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_scheduler scheduler = {0};
    cflow_publisher publisher;
    turbo_flow_t *flow = flow_run_test_flow(&probe);

    check_not_null(flow);
    turbo_flow_msg_init(&source.message);
    source.return_done = 1;
    publisher = flow_run_wait_source_as_cflow_publisher(&source);
    check_true(cflow_scheduler_inline_init(&scheduler));
    config.scheduler = &scheduler;
    check_equal(turbo_flow_run_open(flow, "input", &publisher, &config, &run),
                SALTS_OK);
    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);

    result = (turbo_flow_run_result_t)TURBO_FLOW_RUN_RESULT_INIT;
    check_equal(turbo_flow_run_snapshot(run, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_RUN_COMPLETED);
    turbo_flow_run_close(run);
    cflow_scheduler_destroy(&scheduler);
    turbo_flow_msg_cleanup(&source.message);
  }

  it("maps Publisher DONE and ERROR terminal steps without fabricating values") {
    flow_run_probe_t probe = {0};
    flow_run_wait_source_t done_source = {0};
    flow_run_wait_source_t error_source = {0};
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_config_t config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_scheduler scheduler = {0};
    cflow_publisher publisher;
    turbo_flow_t *flow = flow_run_test_flow(&probe);

    check_not_null(flow);
    turbo_flow_msg_init(&done_source.message);
    turbo_flow_msg_init(&error_source.message);
    done_source.return_done = 1;
    error_source.return_error = 1;
    check_true(cflow_scheduler_inline_init(&scheduler));
    config.scheduler = &scheduler;

    publisher = flow_run_wait_source_as_cflow_publisher(&done_source);
    check_equal(turbo_flow_run_open(flow, "input", &publisher, &config, &run), SALTS_OK);
    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_RUN_COMPLETED);
    check_equal(result.values, 0u);
    turbo_flow_run_close(run);

    run = NULL;
    result = (turbo_flow_run_result_t)TURBO_FLOW_RUN_RESULT_INIT;
    publisher = flow_run_wait_source_as_cflow_publisher(&error_source);
    check_equal(turbo_flow_run_open(flow, "input", &publisher, &config, &run), SALTS_OK);
    check_equal(turbo_flow_run_request(run, 1u), SALTS_EIO);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_EIO);
    check_equal(result.state, TURBO_FLOW_RUN_FAILED);
    check_equal(result.status, SALTS_EIO);
    check_contains(result.error.message, "publisher failed");
    check_equal(result.values, 0u);
    turbo_flow_run_close(run);

    cflow_scheduler_destroy(&scheduler);
    turbo_flow_msg_cleanup(&done_source.message);
    turbo_flow_msg_cleanup(&error_source.message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("fails bounded open and unsupported deadline before Publisher ownership transfers") {
    flow_run_probe_t probe = {0};
    flow_run_wait_source_t first_source = {0};
    flow_run_wait_source_t second_source = {0};
    turbo_flow_run_t *first_run = NULL;
    turbo_flow_run_t *second_run = NULL;
    turbo_flow_run_config_t config = TURBO_FLOW_RUN_CONFIG_INIT;
    cflow_scheduler inline_scheduler = {0};
    cflow_publisher first_publisher;
    cflow_publisher second_publisher;
    turbo_flow_t *flow = flow_run_test_flow_with_capacity(&probe, 1u);

    check_not_null(flow);
    turbo_flow_msg_init(&first_source.message);
    turbo_flow_msg_init(&second_source.message);
    first_publisher = flow_run_wait_source_as_cflow_publisher(&first_source);
    second_publisher = flow_run_wait_source_as_cflow_publisher(&second_source);
    check_equal(turbo_flow_run_open(flow, "input", &first_publisher, NULL, &first_run), SALTS_OK);
    check_equal(turbo_flow_run_open(flow, "input", &second_publisher, NULL, &second_run),
                SALTS_ENOSPC);
    check_null(second_run);
    check_true(cflow_publisher_valid(&second_publisher));
    turbo_flow_run_close(first_run);

    check_true(cflow_scheduler_inline_init(&inline_scheduler));
    config.scheduler = &inline_scheduler;
    config.deadline_ms = 1u;
    check_equal(turbo_flow_run_open(flow, "input", &second_publisher, &config, &second_run),
                SALTS_ENOTSUP);
    check_null(second_run);
    check_true(cflow_publisher_valid(&second_publisher));
    cflow_publisher_destroy(&second_publisher);
    cflow_scheduler_destroy(&inline_scheduler);

    turbo_flow_msg_cleanup(&first_source.message);
    turbo_flow_msg_cleanup(&second_source.message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }
}
