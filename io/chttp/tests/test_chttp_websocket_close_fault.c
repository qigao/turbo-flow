#include "tinytest.h"
#include "turbo_flow_chttp.h"
#include "../../../tests/flow_operation_fixture.h"
#include "../../../turbo_flow/src/flow_internal.h"
#include "salts/thread.h"
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

enum { CLOSE_TEST_TIMEOUT_MS = 5000 };
static atomic_int close_failure;
static atomic_uint close_calls;
static atomic_int allocation_failure;
static atomic_int allocation_gate;
static atomic_int allocation_entered;
static atomic_int allocation_allowed;
static atomic_int publish_failure;
static atomic_int publish_gate;
static atomic_int publish_entered;
static atomic_int publish_allowed;
static atomic_int completion_called;
static atomic_int operation_status;

typedef struct websocket_registration_fault_s {
  flow_registration_checkpoint_t fail_at;
  size_t calls[FLOW_REGISTRATION_CHECKPOINT_COUNT];
} websocket_registration_fault_t;

static int websocket_fail_registration(void *ctx, flow_registration_checkpoint_t checkpoint) {
  websocket_registration_fault_t *fault = (websocket_registration_fault_t *)ctx;
  fault->calls[checkpoint] += 1u;
  return checkpoint == fault->fail_at ? SALTS_ENOMEM : SALTS_OK;
}

static int websocket_registration_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  const turbo_flow_resource_metadata_t *metadata = (const turbo_flow_resource_metadata_t *)ctx;
  if (!metadata || !out || out->size < sizeof(*out)) return SALTS_EINVAL;
  *out = *metadata;
  return SALTS_OK;
}
int chttp_test_delayed_completion_close(turbo_flow_chttp_websocket_server_t *server);
void chttp_test_websocket_fail_calloc_call(size_t call);
int chttp_test_websocket_set_managed_generation(
    turbo_flow_chttp_websocket_server_t *server, uint64_t generation);
int chttp_test_websocket_set_control_state(turbo_flow_chttp_websocket_server_t *server,
                                           turbo_flow_chttp_websocket_server_state_t state);
int chttp_test_websocket_direct_command(turbo_flow_chttp_websocket_server_t *server,
                                        const turbo_flow_resource_command_t *command);
int chttp_test_websocket_set_snapshot_state(turbo_flow_chttp_websocket_server_t *server,
                                            size_t active_sessions, size_t in_flight_frames,
                                            size_t session_frames, int frame_occupied,
                                            size_t pending_publications, uint64_t accepted,
                                            uint64_t completed);
int chttp_test_websocket_saturate_managed_counters(
    turbo_flow_chttp_websocket_server_t *server);

mem_buffer_t *chttp_test_websocket_mem_get_buffer(mem_pool_t *pool, size_t min_size) {
  if (atomic_load_explicit(&allocation_gate, memory_order_acquire)) {
    atomic_store_explicit(&allocation_entered, 1, memory_order_release);
    while (!atomic_load_explicit(&allocation_allowed, memory_order_acquire)) salts_thread_yield();
  }
  if (atomic_exchange_explicit(&allocation_failure, SALTS_OK, memory_order_relaxed) != SALTS_OK)
    return NULL;
  return mem_get_buffer(pool, min_size);
}

typedef struct websocket_fault_completion_s {
  turbo_flow_publish_completion_fn completion;
  void *ctx;
} websocket_fault_completion_t;

static websocket_fault_completion_t publish_completion;

static void websocket_fault_publication_complete(void *ctx,
                                                 const turbo_flow_publish_result_t *result) {
  websocket_fault_completion_t *saved = (websocket_fault_completion_t *)ctx;
  saved->completion(saved->ctx, result);
  atomic_store_explicit(&completion_called, 1, memory_order_release);
}

int chttp_test_websocket_publish_async(turbo_flow_t *flow, const char *source_name,
                                       const turbo_flow_msg_t *message,
                                       turbo_flow_publish_completion_fn completion, void *ctx) {
  int status = atomic_exchange_explicit(&publish_failure, SALTS_OK, memory_order_relaxed);
  if (status != SALTS_OK) return status;
  if (!atomic_load_explicit(&publish_gate, memory_order_acquire))
    return turbo_flow_publish_async(flow, source_name, message, completion, ctx);
  publish_completion.completion = completion;
  publish_completion.ctx = ctx;
  status = turbo_flow_publish_async(flow, source_name, message,
                                    websocket_fault_publication_complete, &publish_completion);
  if (status == SALTS_OK && atomic_load_explicit(&publish_gate, memory_order_acquire)) {
    atomic_store_explicit(&publish_entered, 1, memory_order_release);
    while (!atomic_load_explicit(&publish_allowed, memory_order_acquire)) salts_thread_yield();
  }
  return status;
}

static int websocket_fault_operation(turbo_flow_msg_t *message, void *ctx) {
  (void)message;
  (void)ctx;
  return atomic_load_explicit(&operation_status, memory_order_relaxed);
}

static native_io_backend_kind websocket_fault_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#else
  return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static chttp_server_config websocket_fault_server_config(void) {
  const chttp_server_config config = {
      .host = "127.0.0.1",
      .backlog = 2u,
      .network = {.backend = websocket_fault_backend(),
                  .connection_capacity = 2u,
                  .command_capacity = 8u,
                  .command_buffer_bytes = 8192u,
                  .request_capacity = 8u,
                  .completion_batch_capacity = 8u,
                  .event_capacity = 8u,
                  .event_buffer_bytes = 8192u,
                  .max_send_bytes = 8192u,
                  .receive_buffer_bytes = 4096u},
      .route_capacity = 1u,
      .middleware_capacity = 1u,
      .max_route_middleware_count = 1u,
      .max_route_param_count = 1u,
      .max_route_param_bytes = 128u,
      .max_target_bytes = 256u,
      .max_header_count = 16u,
      .max_header_bytes = 4096u,
      .max_request_body_bytes = 4096u,
      .max_response_header_count = 16u,
      .max_response_header_bytes = 1024u,
      .max_response_body_bytes = 4096u,
      .max_buffered_response_body_bytes = 4096u,
      .poll_slice_ms = 1u};
  return config;
}

static turbo_flow_chttp_websocket_server_config_t
websocket_fault_adapter_config(turbo_flow_t *flow, chttp_server_config *native,
                               const char *adapter_name) {
  turbo_flow_chttp_websocket_server_config_t config =
      TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
  config.flow = flow;
  config.adapter_name = adapter_name;
  config.source_name = "input";
  config.path = "/flow";
  config.server = native;
  config.session_capacity = 2u;
  config.frame_capacity = 2u;
  config.max_frame_bytes = 4096u;
  config.max_message_bytes = 4096u;
  config.max_buffered_input_bytes = 8192u;
  return config;
}

/* The test support target redirects the adapter's native call here. */
int chttp_test_websocket_close(const chttp_server_websocket_session *session, uint16_t code,
                               const void *reason, size_t reason_size) {
  int status = atomic_exchange_explicit(&close_failure, SALTS_OK, memory_order_relaxed);
  atomic_fetch_add_explicit(&close_calls, 1u, memory_order_relaxed);
  return status == SALTS_OK ? chttp_server_websocket_close(session, code, reason, reason_size)
                            : status;
}

spec("CHTTP WebSocket close admission failure") {
  it("rolls back allocation and duplicate registration failures without managed residue") {
    for (size_t call = 1u; call <= 3u; ++call) {
      chttp_server_config native = websocket_fault_server_config();
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_chttp_websocket_server_config_t config =
          websocket_fault_adapter_config(flow, &native, "ws");
      turbo_flow_chttp_websocket_server_t *server =
          (turbo_flow_chttp_websocket_server_t *)(uintptr_t)1u;
      check_not_null(flow);
      chttp_test_websocket_fail_calloc_call(call);
      check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_ENOMEM);
      check_null(server);
      check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
      turbo_flow_destroy(flow);
    }
    {
      chttp_server_config native = websocket_fault_server_config();
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_chttp_websocket_server_config_t config =
          websocket_fault_adapter_config(flow, &native, "ws");
      turbo_flow_chttp_websocket_server_t *first = NULL;
      turbo_flow_chttp_websocket_server_t *duplicate =
          (turbo_flow_chttp_websocket_server_t *)(uintptr_t)1u;
      chttp_test_websocket_fail_calloc_call(0u);
      check_equal(turbo_flow_chttp_websocket_server_register(&config, &first), SALTS_OK);
      check_equal(turbo_flow_chttp_websocket_server_register(&config, &duplicate),
                  SALTS_EALREADY);
      check_null(duplicate);
      check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
      turbo_flow_destroy(flow);
      check_equal(turbo_flow_chttp_websocket_server_destroy(first), SALTS_OK);
    }
    {
      static const flow_registration_checkpoint_t checkpoints[] = {
          FLOW_REGISTRATION_ALLOC_ADAPTER_VECTOR,
          FLOW_REGISTRATION_ALLOC_RESOURCE_OWNER_NAME,
          FLOW_REGISTRATION_ALLOC_RESOURCE_VECTOR};
      for (size_t index = 0u; index < sizeof(checkpoints) / sizeof(checkpoints[0]); ++index) {
        chttp_server_config native = websocket_fault_server_config();
        turbo_flow_t *flow = turbo_flow_create();
        turbo_flow_chttp_websocket_server_config_t config =
            websocket_fault_adapter_config(flow, &native, "ws");
        turbo_flow_chttp_websocket_server_t *server =
            (turbo_flow_chttp_websocket_server_t *)(uintptr_t)1u;
        websocket_registration_fault_t fault = {.fail_at = checkpoints[index]};
        check_not_null(flow);
        flow->registration_fault.before_commit = websocket_fail_registration;
        flow->registration_fault.ctx = &fault;
        check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_ENOMEM);
        check_null(server);
        check_equal(fault.calls[checkpoints[index]], (size_t)1u);
        check_null(turbo_flow_find_adapter_schema(flow, "ws"));
        check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
        check_equal(turbo_flow_resource_metadata_count(flow), (size_t)0u);
        turbo_flow_destroy(flow);
      }
    }
    {
      turbo_flow_resource_provider_ops_t resource_ops = TURBO_FLOW_RESOURCE_PROVIDER_OPS_INIT;
      turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
      chttp_server_config native = websocket_fault_server_config();
      turbo_flow_t *uid_flow = turbo_flow_create();
      turbo_flow_chttp_websocket_server_config_t config =
          websocket_fault_adapter_config(uid_flow, &native, "ws");
      turbo_flow_chttp_websocket_server_t *server =
          (turbo_flow_chttp_websocket_server_t *)(uintptr_t)1u;
      check_not_null(uid_flow);
      resource_ops.metadata = websocket_registration_metadata;
      metadata.domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
      metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
      metadata.generation = 1u;
      metadata.observed_generation = 1u;
      memcpy(metadata.owner_name, "existing-owner", sizeof("existing-owner"));
      memcpy(metadata.uid, "chttp-websocket:ws", sizeof("chttp-websocket:ws"));
      check_equal(turbo_flow_register_resource_provider(uid_flow, metadata.owner_name,
                                                        &resource_ops, &metadata),
                  SALTS_OK);
      check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_EALREADY);
      check_null(server);
      check_null(turbo_flow_find_adapter_schema(uid_flow, "ws"));
      check_equal(turbo_flow_managed_boundary_count(uid_flow), (size_t)0u);
      check_equal(turbo_flow_resource_metadata_count(uid_flow), (size_t)1u);
      turbo_flow_destroy(uid_flow);
    }
  }

  it("rejects unstable and contradictory snapshots and saturates managed counters") {
    chttp_server_config native = websocket_fault_server_config();
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_chttp_websocket_server_config_t config =
        websocket_fault_adapter_config(flow, &native, "ws");
    turbo_flow_chttp_websocket_server_t *server = NULL;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;

    chttp_test_websocket_fail_calloc_call(0u);
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(chttp_test_websocket_set_snapshot_state(server, 0u, 0u, 0u, 0, 1u, 0u, 0u),
                SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_EBUSY);
    check_equal(chttp_test_websocket_set_snapshot_state(server, 0u, 0u, 0u, 0, 3u, 0u, 0u),
                SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_EPROTO);
    check_equal(chttp_test_websocket_set_snapshot_state(server, 1u, 0u, 1u, 1, 0u, 0u, 0u),
                SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_EPROTO);
    check_equal(chttp_test_websocket_set_snapshot_state(server, 0u, 0u, 0u, 0, 0u, 0u, 1u),
                SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_EPROTO);
    check_equal(chttp_test_websocket_set_snapshot_state(server, 0u, 0u, 0u, 0, 0u, 0u, 0u),
                SALTS_OK);
    check_equal(chttp_test_websocket_saturate_managed_counters(server), SALTS_OK);
    managed = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.accepted, UINT64_MAX);
    check_equal(managed.completed, UINT64_MAX);
    check_equal(managed.rejected, UINT64_MAX);
    check_equal(chttp_test_websocket_set_managed_generation(server, UINT64_MAX), SALTS_OK);
    check_equal(chttp_test_websocket_set_control_state(
                    server, TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_QUIESCED),
                SALTS_OK);
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_RESUME;
    command.expected_generation = UINT64_MAX;
    check_equal(chttp_test_websocket_direct_command(server, &command), SALTS_ERANGE);
    managed = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.generation, UINT64_MAX);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_QUIESCENT);
    check_equal(chttp_test_websocket_set_control_state(
                    server, TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_REGISTERED),
                SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
  }

  it("accounts make publish and terminal failures through the real owner path") {
    static const char graph[] = "source input adapter ws\n"
                                "stage fault operation test.websocket.fault\n"
                                "stage output adapter ws\n"
                                "stage main {\n  input -> fault -> output\n}\n";
    for (size_t mode = 0u; mode < 3u; ++mode) {
      chttp_server_config native = websocket_fault_server_config();
      chttp_websocket_client_config client_config = {.size = sizeof(client_config)};
      turbo_flow_t *flow = turbo_flow_create();
      turbo_flow_chttp_websocket_server_config_t config =
          websocket_fault_adapter_config(flow, &native, "ws");
      turbo_flow_chttp_websocket_server_t *server = NULL;
      turbo_flow_chttp_websocket_server_snapshot_t snapshot =
          TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
      turbo_flow_managed_boundary_snapshot_t managed =
          TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
      chttp_websocket_client client = {0};
      chttp_websocket_connect_options options = {.size = sizeof(options)};
      chttp_websocket_event event = {0};
      unsigned int http_status = 0u;
      char uri[128];

      client_config.network = native.network;
      client_config.max_frame_bytes = 4096u;
      client_config.max_message_bytes = 4096u;
      client_config.max_buffered_input_bytes = 8192u;
      client_config.max_handshake_header_bytes = 4096u;
      client_config.event_capacity = 8u;
      atomic_store_explicit(&allocation_failure, mode == 0u ? SALTS_ENOMEM : SALTS_OK,
                            memory_order_relaxed);
      atomic_store_explicit(&allocation_gate, 0, memory_order_relaxed);
      atomic_store_explicit(&publish_failure, mode == 1u ? SALTS_ENOSPC : SALTS_OK,
                            memory_order_relaxed);
      atomic_store_explicit(&publish_gate, 0, memory_order_relaxed);
      atomic_store_explicit(&operation_status, mode == 2u ? SALTS_ENOBUFS : SALTS_OK,
                            memory_order_relaxed);
      chttp_test_websocket_fail_calloc_call(0u);
      check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
      check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
      flow_test_operation_t operation = flow_test_operation_init(
          "test.websocket.fault", websocket_fault_operation, NULL);
      operation.descriptor.scope.state = TURBO_FLOW_STATE_SCOPE_GRAPH;
      operation.descriptor.scope.lifetime = TURBO_FLOW_LIFETIME_RUNTIME_GENERATION;
      check_equal(flow_test_operation_register(flow, &operation), SALTS_OK);
      check_equal(turbo_flow_compile(flow), SALTS_OK);
      check_equal(turbo_flow_start(flow), SALTS_OK);
      check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
      check_greater(snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/flow", snapshot.bound_port),
                    0);
      options.uri = uri;
      options.timeout_ms = CLOSE_TEST_TIMEOUT_MS;
      check_equal(chttp_websocket_client_init(&client, &client_config), SALTS_OK);
      check_equal(chttp_websocket_client_connect(&client, &options, &http_status), SALTS_OK);
      check_equal(chttp_websocket_client_send_text(&client, "fault", sizeof("fault") - 1u,
                                                   CLOSE_TEST_TIMEOUT_MS),
                  SALTS_OK);
      check_equal(chttp_websocket_client_receive(&client, CLOSE_TEST_TIMEOUT_MS, &event),
                  SALTS_OK);
      check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_CLOSE);
      check_equal(chttp_websocket_client_destroy(&client, CLOSE_TEST_TIMEOUT_MS), SALTS_OK);
      for (unsigned int elapsed = 0u; elapsed < CLOSE_TEST_TIMEOUT_MS; ++elapsed) {
        managed =
            (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
        if (turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed) == SALTS_OK &&
            managed.in_flight == 0u)
          break;
        salts_sleep_ms(1u);
      }
      check_equal(managed.in_flight, (uint64_t)0u);
      if (mode < 2u) {
        check_true(managed.rejected >= 1u);
      } else {
        check_true(managed.accepted >= 1u);
        check_equal(managed.completed, managed.accepted);
        check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
        check_true(snapshot.frames_rejected >= 1u);
      }
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
      check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
    }
  }

  it("retains failed close ownership and retries only on explicit quiesce") {
    static const char graph[] = "source input adapter ws\n"
                                "stage output adapter ws\n"
                                "stage main {\n  input -> output\n}\n";
    chttp_server_config native = websocket_fault_server_config();
    chttp_websocket_client_config client_config = {.size = sizeof(client_config)};
    turbo_flow_chttp_websocket_server_config_t config =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_CONFIG_INIT;
    turbo_flow_chttp_websocket_server_snapshot_t snapshot =
        TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_SNAPSHOT_INIT;
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
    turbo_flow_resource_command_result_t result = TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    turbo_flow_chttp_websocket_server_t *server = NULL;
    turbo_flow_t *flow = turbo_flow_create();
    chttp_websocket_client client = {0};
    chttp_websocket_connect_options options = {.size = sizeof(options)};
    chttp_websocket_event event = {0};
    unsigned int http_status = 0u;
    char uri[128];
    client_config.network = native.network;
    client_config.max_frame_bytes = 4096u;
    client_config.max_message_bytes = 4096u;
    client_config.max_buffered_input_bytes = 8192u;
    client_config.max_handshake_header_bytes = 4096u;
    client_config.event_capacity = 8u;
    config = websocket_fault_adapter_config(flow, &native, "ws");
    atomic_init(&close_failure, SALTS_OK);
    atomic_init(&close_calls, 0u);
    atomic_init(&allocation_failure, SALTS_OK);
    atomic_init(&allocation_gate, 0);
    atomic_init(&allocation_entered, 0);
    atomic_init(&allocation_allowed, 1);
    atomic_init(&publish_failure, SALTS_OK);
    atomic_init(&publish_gate, 0);
    atomic_init(&publish_entered, 0);
    atomic_init(&publish_allowed, 1);
    atomic_init(&completion_called, 0);
    chttp_test_websocket_fail_calloc_call(0u);
    check_equal(turbo_flow_chttp_websocket_server_register(&config, &server), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_quiesce(server), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_managed_boundary_descriptor_at(flow, 0u, &descriptor), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_REGISTERED);
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    command.expected_generation = managed.generation;
    memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
    memcpy(command.idempotency_key, "websocket-registered-quiesce",
           sizeof("websocket-registered-quiesce"));
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ESHUTDOWN);
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_greater(snprintf(uri, sizeof(uri), "ws://127.0.0.1:%u/flow", snapshot.bound_port), 0);
    options.uri = uri;
    options.timeout_ms = CLOSE_TEST_TIMEOUT_MS;
    check_equal(chttp_websocket_client_init(&client, &client_config), SALTS_OK);
    check_equal(chttp_websocket_client_connect(&client, &options, &http_status), SALTS_OK);
    atomic_store_explicit(&allocation_allowed, 0, memory_order_release);
    atomic_store_explicit(&allocation_gate, 1, memory_order_release);
    check_equal(chttp_websocket_client_send_text(&client, "make-window",
                                                 sizeof("make-window") - 1u,
                                                 CLOSE_TEST_TIMEOUT_MS),
                SALTS_OK);
    for (unsigned int elapsed = 0u; elapsed < CLOSE_TEST_TIMEOUT_MS; ++elapsed) {
      if (atomic_load_explicit(&allocation_entered, memory_order_acquire)) break;
      salts_sleep_ms(1u);
    }
    check_equal(atomic_load_explicit(&allocation_entered, memory_order_acquire), 1);
    managed = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_EBUSY);
    atomic_store_explicit(&allocation_gate, 0, memory_order_release);
    atomic_store_explicit(&allocation_allowed, 1, memory_order_release);
    check_equal(chttp_websocket_client_receive(&client, CLOSE_TEST_TIMEOUT_MS, &event), SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_MESSAGE);

    atomic_store_explicit(&publish_entered, 0, memory_order_relaxed);
    atomic_store_explicit(&completion_called, 0, memory_order_relaxed);
    atomic_store_explicit(&publish_allowed, 0, memory_order_release);
    atomic_store_explicit(&publish_gate, 1, memory_order_release);
    check_equal(chttp_websocket_client_send_text(&client, "callback-window",
                                                 sizeof("callback-window") - 1u,
                                                 CLOSE_TEST_TIMEOUT_MS),
                SALTS_OK);
    for (unsigned int elapsed = 0u; elapsed < CLOSE_TEST_TIMEOUT_MS; ++elapsed) {
      if (atomic_load_explicit(&publish_entered, memory_order_acquire) &&
          atomic_load_explicit(&completion_called, memory_order_acquire))
        break;
      salts_sleep_ms(1u);
    }
    check_equal(atomic_load_explicit(&publish_entered, memory_order_acquire), 1);
    check_equal(atomic_load_explicit(&completion_called, memory_order_acquire), 1);
    managed = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_EBUSY);
    atomic_store_explicit(&publish_gate, 0, memory_order_release);
    atomic_store_explicit(&publish_allowed, 1, memory_order_release);
    check_equal(chttp_websocket_client_receive(&client, CLOSE_TEST_TIMEOUT_MS, &event), SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_MESSAGE);
    managed = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    command.expected_generation = managed.generation;
    command.deadline_ns = 1u;
    memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
    memcpy(command.idempotency_key, "websocket-expired-quiesce",
           sizeof("websocket-expired-quiesce"));
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ETIMEDOUT);
    command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_REPLACE_ENDPOINT;
    command.expected_generation = managed.generation;
    memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
    memcpy(command.idempotency_key, "websocket-replace-masked",
           sizeof("websocket-replace-masked"));
    memcpy(command.endpoint_host, "127.0.0.1", sizeof("127.0.0.1"));
    command.endpoint_port = 80;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ENOTSUP);
    atomic_store_explicit(&close_failure, SALTS_ENOBUFS, memory_order_relaxed);
    managed = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    command.expected_generation = managed.generation;
    memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
    memcpy(command.idempotency_key, "websocket-failed-quiesce",
           sizeof("websocket-failed-quiesce"));
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ENOBUFS);
    check_equal(result.generation_after, command.expected_generation + 1u);
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_ENOBUFS);
    check_equal(result.replayed, 1);
    check_equal(result.generation_after, command.expected_generation + 1u);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_QUIESCED);
    check_equal(snapshot.last_status, SALTS_ENOBUFS);
    check_equal(snapshot.active_sessions, (size_t)1u);
    check_equal(atomic_load_explicit(&close_calls, memory_order_relaxed), 1u);
    managed = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_DRAINING);
    check_equal(managed.generation, command.expected_generation + 1u);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_EBUSY);
    check_equal(chttp_test_delayed_completion_close(server), SALTS_OK);
    check_equal(atomic_load_explicit(&close_calls, memory_order_relaxed), 1u);
    check_equal(chttp_websocket_client_send_text(&client, "late", sizeof("late") - 1u,
                                                 CLOSE_TEST_TIMEOUT_MS), SALTS_OK);
    for (unsigned int elapsed = 0u; elapsed < CLOSE_TEST_TIMEOUT_MS; ++elapsed) {
      check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
      if (snapshot.frames_rejected != 0u) break;
      salts_sleep_ms(1u);
    }
    check_equal(snapshot.frames_rejected, (uint64_t)1u);
    check_equal(atomic_load_explicit(&close_calls, memory_order_relaxed), 1u);
    command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    command.expected_generation = managed.generation - 1u;
    memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
    memcpy(command.idempotency_key, "websocket-stale-quiesce",
           sizeof("websocket-stale-quiesce"));
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_EBUSY);
    check_equal(atomic_load_explicit(&close_calls, memory_order_relaxed), 1u);
    command = (turbo_flow_resource_command_t)TURBO_FLOW_RESOURCE_COMMAND_INIT;
    result = (turbo_flow_resource_command_result_t)TURBO_FLOW_RESOURCE_COMMAND_RESULT_INIT;
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    command.expected_generation = managed.generation;
    memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
    memcpy(command.idempotency_key, "websocket-retry-quiesce",
           sizeof("websocket-retry-quiesce"));
    check_equal(turbo_flow_resource_command(flow, &command, &result), SALTS_OK);
    check_equal(result.generation_after, managed.generation);
    check_equal(atomic_load_explicit(&close_calls, memory_order_relaxed), 2u);
    check_equal(turbo_flow_chttp_websocket_server_quiesce(server), SALTS_OK);
    check_equal(atomic_load_explicit(&close_calls, memory_order_relaxed), 2u);
    managed = (turbo_flow_managed_boundary_snapshot_t)TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    check_equal(turbo_flow_managed_boundary_snapshot_at(flow, 0u, &managed), SALTS_OK);
    check_equal(managed.generation, result.generation_after);
    check_equal(chttp_websocket_client_receive(&client, CLOSE_TEST_TIMEOUT_MS, &event), SALTS_OK);
    check_equal(event.kind, CHTTP_WEBSOCKET_EVENT_CLOSE);
    check_equal(event.close_code, (uint16_t)1013u);
    check_equal(chttp_websocket_client_destroy(&client, CLOSE_TEST_TIMEOUT_MS), SALTS_OK);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_equal(turbo_flow_chttp_websocket_server_snapshot(server, &snapshot), SALTS_OK);
    check_equal(snapshot.state, TURBO_FLOW_CHTTP_WEBSOCKET_SERVER_RUNNING);
    check_equal(snapshot.active_sessions, (size_t)0u);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_websocket_server_destroy(server), SALTS_OK);
  }
}
