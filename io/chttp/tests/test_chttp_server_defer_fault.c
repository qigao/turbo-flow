#include "tinytest.h"
#include "turbo_flow_chttp.h"

#include <salts/thread.h>

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

enum { CHTTP_FAULT_TIMEOUT_MS = 5000 };

static atomic_int defer_status;
static atomic_int reply_status;
static atomic_int cancel_status;
static atomic_int publish_gate;
static atomic_int publish_entered;
static atomic_int publish_allowed;
static atomic_int completion_gate;
static atomic_int completion_entered;
static atomic_int completion_allowed;
static turbo_flow_publish_completion_fn gated_completion;
static void *gated_completion_ctx;

int chttp_test_server_retry_first(turbo_flow_chttp_server_t *server);
int chttp_test_server_set_managed_generation(turbo_flow_chttp_server_t *server,
                                             uint64_t generation);
int chttp_test_server_direct_command(turbo_flow_chttp_server_t *server,
                                     const turbo_flow_resource_command_t *command);
void chttp_test_server_fail_calloc_call(size_t call);
int chttp_test_server_set_snapshot_state(turbo_flow_chttp_server_t *server, int occupied,
                                         int managed_admitted, size_t active_requests,
                                         uint64_t accepted, uint64_t completed,
                                         uint64_t rejected);
int chttp_test_server_saturate_managed_counters(turbo_flow_chttp_server_t *server);

int chttp_test_server_response_defer(chttp_server_response *response,
                                     chttp_server_deferred *out_deferred) {
  const int status = atomic_load_explicit(&defer_status, memory_order_relaxed);
  return status == SALTS_OK ? chttp_server_response_defer(response, out_deferred) : status;
}

int chttp_test_server_deferred_reply(chttp_server_deferred *deferred,
                                     const chttp_server_deferred_response *response) {
  const int status = atomic_load_explicit(&reply_status, memory_order_relaxed);
  if (status == SALTS_OK) return chttp_server_deferred_reply(deferred, response);
  if (status == SALTS_ENOENT || status == SALTS_EALREADY)
    (void)chttp_server_deferred_cancel(deferred);
  return status;
}

int chttp_test_server_deferred_cancel(chttp_server_deferred *deferred) {
  const int status = atomic_load_explicit(&cancel_status, memory_order_relaxed);
  if (status == SALTS_OK) return chttp_server_deferred_cancel(deferred);
  if (status == SALTS_ENOENT) (void)chttp_server_deferred_cancel(deferred);
  return status;
}

static void chttp_fault_gated_completion(void *ctx,
                                         const turbo_flow_publish_result_t *result) {
  (void)ctx;
  atomic_store_explicit(&completion_entered, 1, memory_order_release);
  while (!atomic_load_explicit(&completion_allowed, memory_order_acquire)) salts_thread_yield();
  gated_completion(gated_completion_ctx, result);
}

int chttp_test_publish_async(turbo_flow_t *flow, const char *source_name,
                             const turbo_flow_msg_t *message,
                             turbo_flow_publish_completion_fn completion, void *ctx) {
  int status;
  if (atomic_load_explicit(&completion_gate, memory_order_acquire)) {
    gated_completion = completion;
    gated_completion_ctx = ctx;
    status = turbo_flow_publish_async(flow, source_name, message, chttp_fault_gated_completion,
                                      NULL);
  } else {
    status = turbo_flow_publish_async(flow, source_name, message, completion, ctx);
  }
  if (status == SALTS_OK && atomic_load_explicit(&publish_gate, memory_order_acquire)) {
    atomic_store_explicit(&publish_entered, 1, memory_order_release);
    while (!atomic_load_explicit(&publish_allowed, memory_order_acquire)) salts_thread_yield();
  }
  return status;
}

static native_io_backend_kind chttp_fault_backend(void) {
#if defined(_WIN32)
  return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
  return NATIVE_IO_BACKEND_EPOLL;
#else
  return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static chttp_server_config chttp_fault_server_config(void) {
  const chttp_server_config config = {
      .host = "127.0.0.1",
      .backlog = 2u,
      .network = {.backend = chttp_fault_backend(),
                  .connection_capacity = 1u,
                  .command_capacity = 8u,
                  .request_capacity = 4u,
                  .completion_batch_capacity = 4u,
                  .event_capacity = 8u,
                  .max_send_bytes = 8192u,
                  .receive_buffer_bytes = 4096u,
                  .connect_timeout_ms = CHTTP_FAULT_TIMEOUT_MS,
                  .read_timeout_ms = CHTTP_FAULT_TIMEOUT_MS,
                  .write_timeout_ms = CHTTP_FAULT_TIMEOUT_MS},
      .route_capacity = 1u,
      .middleware_capacity = 1u,
      .max_route_middleware_count = 1u,
      .max_route_param_count = 1u,
      .max_route_param_bytes = 128u,
      .max_target_bytes = 256u,
      .max_header_count = 16u,
      .max_header_bytes = 2048u,
      .max_request_body_bytes = 4096u,
      .max_response_header_count = 16u,
      .max_response_header_bytes = 2048u,
      .max_response_body_bytes = 4096u,
      .max_buffered_response_body_bytes = 4096u,
      .poll_slice_ms = 1u};
  return config;
}

static chttp_client_config chttp_fault_client_config(void) {
  chttp_client_config config = {0};
  config.network = chttp_fault_server_config().network;
  config.request_capacity = 1u;
  config.max_start_line_bytes = 256u;
  config.max_header_count = 16u;
  config.max_header_bytes = 2048u;
  config.max_request_body_bytes = 4096u;
  config.max_response_body_bytes = 4096u;
  config.max_informational_responses = 2u;
  return config;
}

typedef struct chttp_fault_owner_s {
  turbo_flow_t *flow;
  turbo_flow_chttp_server_t *server;
  uint16_t port;
} chttp_fault_owner_t;

typedef struct chttp_fault_client_s {
  uint16_t port;
  int status;
  atomic_int completed;
} chttp_fault_client_t;

static void chttp_fault_reset(void) {
  atomic_store_explicit(&defer_status, SALTS_OK, memory_order_relaxed);
  atomic_store_explicit(&reply_status, SALTS_OK, memory_order_relaxed);
  atomic_store_explicit(&cancel_status, SALTS_OK, memory_order_relaxed);
  atomic_store_explicit(&publish_gate, 0, memory_order_relaxed);
  atomic_store_explicit(&publish_entered, 0, memory_order_relaxed);
  atomic_store_explicit(&publish_allowed, 1, memory_order_relaxed);
  atomic_store_explicit(&completion_gate, 0, memory_order_relaxed);
  atomic_store_explicit(&completion_entered, 0, memory_order_relaxed);
  atomic_store_explicit(&completion_allowed, 1, memory_order_relaxed);
  gated_completion = NULL;
  gated_completion_ctx = NULL;
  chttp_test_server_fail_calloc_call(0u);
}

static int chttp_fault_owner_open(chttp_fault_owner_t *owner) {
  static const char graph[] = "source input adapter server\n"
                              "stage output adapter server\n"
                              "stage main {\n  input -> output\n}\n";
  chttp_server_config native = chttp_fault_server_config();
  turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
  turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
  int status;
  memset(owner, 0, sizeof(*owner));
  owner->flow = turbo_flow_create();
  if (!owner->flow) return SALTS_ENOMEM;
  config.flow = owner->flow;
  config.adapter_name = "server";
  config.source_name = "input";
  config.server = &native;
  config.method = CHTTP_METHOD_POST;
  config.path = "/flow";
  status = turbo_flow_chttp_server_register(&config, &owner->server);
  if (status == SALTS_OK)
    status = turbo_flow_parse_string(owner->flow, graph, sizeof(graph) - 1u);
  if (status == SALTS_OK) status = turbo_flow_compile(owner->flow);
  if (status == SALTS_OK) status = turbo_flow_start(owner->flow);
  if (status == SALTS_OK) status = turbo_flow_chttp_server_snapshot(owner->server, &snapshot);
  if (status == SALTS_OK) owner->port = snapshot.bound_port;
  return status;
}

static void chttp_fault_owner_close(chttp_fault_owner_t *owner) {
  check_equal(turbo_flow_stop(owner->flow), SALTS_OK);
  turbo_flow_destroy(owner->flow);
  check_equal(turbo_flow_chttp_server_destroy(owner->server), SALTS_OK);
  memset(owner, 0, sizeof(*owner));
}

static int chttp_fault_call(uint16_t port) {
  chttp_client_config config = chttp_fault_client_config();
  chttp_client client = {0};
  chttp_options options = {0};
  chttp_response response = {0};
  chttp_error error = {0};
  char uri[64];
  int status = chttp_client_init(&client, &config);
  if (status != SALTS_OK) return status;
  if (snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned int)port) <= 0)
    status = SALTS_EINVAL;
  options.connection_uri = uri;
  options.authority = "127.0.0.1";
  options.target = "/flow";
  options.body = "ping";
  options.body_size = sizeof("ping") - 1u;
  options.timeout_ms = CHTTP_FAULT_TIMEOUT_MS;
  if (status == SALTS_OK) status = chttp_post(&client, &options, &response, &error);
  chttp_response_destroy(&response);
  {
    const int destroy_status = chttp_client_destroy(&client, CHTTP_FAULT_TIMEOUT_MS);
    if (status == SALTS_OK) status = destroy_status;
  }
  return status;
}

static void chttp_fault_client_thread(void *ctx) {
  chttp_fault_client_t *client = (chttp_fault_client_t *)ctx;
  client->status = chttp_fault_call(client->port);
  atomic_store_explicit(&client->completed, 1, memory_order_release);
}

static int chttp_fault_wait_active(turbo_flow_chttp_server_t *server, size_t expected) {
  for (unsigned int elapsed = 0u; elapsed < CHTTP_FAULT_TIMEOUT_MS; ++elapsed) {
    turbo_flow_chttp_server_snapshot_t snapshot = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    const int status = turbo_flow_chttp_server_snapshot(server, &snapshot);
    if (status != SALTS_OK) return status;
    if (snapshot.active_requests == expected) return SALTS_OK;
    salts_sleep_ms(1u);
  }
  return SALTS_ETIMEDOUT;
}

spec("CHTTP managed deferred server fault boundaries") {
  before_each() { chttp_fault_reset(); }

  it("returns ENOMEM without registry residue when request-slot allocation fails") {
    chttp_server_config native = chttp_fault_server_config();
    turbo_flow_chttp_server_config_t config = TURBO_FLOW_CHTTP_SERVER_CONFIG_INIT;
    turbo_flow_chttp_server_t *server = (turbo_flow_chttp_server_t *)(uintptr_t)1u;
    turbo_flow_t *flow = turbo_flow_create();
    check_not_null(flow);
    config.flow = flow;
    config.adapter_name = "server";
    config.source_name = "input";
    config.server = &native;
    config.method = CHTTP_METHOD_POST;
    config.path = "/flow";
    chttp_test_server_fail_calloc_call(2u);
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_ENOMEM);
    check_null(server);
    check_equal(turbo_flow_adapter_count(flow), (size_t)0u);
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)0u);
    chttp_test_server_fail_calloc_call(0u);
    check_equal(turbo_flow_chttp_server_register(&config, &server), SALTS_OK);
    check_not_null(server);
    check_equal(turbo_flow_adapter_count(flow), (size_t)1u);
    check_equal(turbo_flow_managed_boundary_count(flow), (size_t)1u);
    turbo_flow_destroy(flow);
    check_equal(turbo_flow_chttp_server_destroy(server), SALTS_OK);
  }

  it("settles managed admission when native defer fails after publish") {
    chttp_fault_owner_t owner;
    chttp_fault_client_t client = {0};
    turbo_flow_chttp_server_snapshot_t native = TURBO_FLOW_CHTTP_SERVER_SNAPSHOT_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    salts_thread_t thread = NULL;
    check_equal(chttp_fault_owner_open(&owner), SALTS_OK);
    atomic_store_explicit(&defer_status, SALTS_EIO, memory_order_relaxed);
    atomic_store_explicit(&completion_allowed, 0, memory_order_relaxed);
    atomic_store_explicit(&completion_gate, 1, memory_order_release);
    client.port = owner.port;
    check_equal(salts_thread_create(&thread, chttp_fault_client_thread, &client), SALTS_OK);
    for (unsigned int elapsed = 0u;
         elapsed < CHTTP_FAULT_TIMEOUT_MS &&
         !atomic_load_explicit(&completion_entered, memory_order_acquire);
         ++elapsed)
      salts_sleep_ms(1u);
    check_equal(atomic_load_explicit(&completion_entered, memory_order_acquire), 1);
    for (unsigned int elapsed = 0u;
         elapsed < CHTTP_FAULT_TIMEOUT_MS &&
         !atomic_load_explicit(&client.completed, memory_order_acquire);
         ++elapsed)
      salts_sleep_ms(1u);
    check_equal(atomic_load_explicit(&client.completed, memory_order_acquire), 1);
    check_equal(turbo_flow_chttp_server_snapshot(owner.server, &native), SALTS_OK);
    check_equal(native.active_requests, (size_t)1u);
    check_equal(native.rejected_requests, (uint64_t)0u);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_OK);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)0u);
    check_equal(managed.in_flight, (uint64_t)1u);
    atomic_store_explicit(&completion_allowed, 1, memory_order_release);
    check_equal(salts_thread_join(&thread), SALTS_OK);
    check_equal(client.status, SALTS_OK);
    check_equal(chttp_fault_wait_active(owner.server, 0u), SALTS_OK);
    check_equal(turbo_flow_chttp_server_snapshot(owner.server, &native), SALTS_OK);
    check_equal(native.admitted_requests, (uint64_t)0u);
    check_equal(native.completed_requests, (uint64_t)0u);
    check_equal(native.rejected_requests, (uint64_t)1u);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_OK);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.rejected, (uint64_t)0u);
    check_equal(managed.in_flight, (uint64_t)0u);
    chttp_fault_owner_close(&owner);
  }

  it("projects slot capacity, full backpressure, and idle quiescence") {
    chttp_fault_owner_t owner;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    check_equal(chttp_fault_owner_open(&owner), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_OK);
    check_equal(managed.queue_depth, (uint64_t)0u);
    check_equal(managed.queue_capacity, (uint64_t)1u);
    check_equal(managed.in_flight, (uint64_t)0u);
    check_equal(managed.backpressured, 0);
    check_equal(chttp_test_server_set_snapshot_state(owner.server, 1, 1, 1u, 1u, 0u, 0u),
                SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_OK);
    check_equal(managed.queue_depth, (uint64_t)0u);
    check_equal(managed.queue_capacity, (uint64_t)1u);
    check_equal(managed.in_flight, (uint64_t)1u);
    check_equal(managed.backpressured, 1);
    check_equal(chttp_test_server_set_snapshot_state(owner.server, 0, 0, 0u, 1u, 1u, 0u),
                SALTS_OK);
    check_equal(turbo_flow_chttp_server_quiesce(owner.server), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_OK);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_QUIESCENT);
    check_equal(managed.queue_depth, (uint64_t)0u);
    check_equal(managed.queue_capacity, (uint64_t)1u);
    check_equal(managed.in_flight, (uint64_t)0u);
    check_equal(managed.backpressured, 0);
    chttp_fault_owner_close(&owner);
  }

  it("rejects both stable slot snapshot contradictions") {
    chttp_fault_owner_t owner;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    check_equal(chttp_fault_owner_open(&owner), SALTS_OK);
    check_equal(chttp_test_server_set_snapshot_state(owner.server, 0, 1, 0u, 1u, 0u, 0u),
                SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_EPROTO);
    check_equal(chttp_test_server_set_snapshot_state(owner.server, 1, 1, 0u, 1u, 0u, 0u),
                SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_EPROTO);
    check_equal(chttp_test_server_set_snapshot_state(owner.server, 0, 0, 0u, 1u, 1u, 0u),
                SALTS_OK);
    chttp_fault_owner_close(&owner);
  }

  it("saturates managed counters without wrapping") {
    chttp_fault_owner_t owner;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    check_equal(chttp_fault_owner_open(&owner), SALTS_OK);
    check_equal(chttp_test_server_saturate_managed_counters(owner.server), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_OK);
    check_equal(managed.accepted, UINT64_MAX);
    check_equal(managed.completed, UINT64_MAX);
    check_equal(managed.rejected, UINT64_MAX);
    chttp_fault_owner_close(&owner);
  }

  it("reports the post-publish pre-admission window as busy") {
    chttp_fault_owner_t owner;
    chttp_fault_client_t client = {0};
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    salts_thread_t thread = NULL;
    check_equal(chttp_fault_owner_open(&owner), SALTS_OK);
    atomic_store_explicit(&publish_allowed, 0, memory_order_relaxed);
    atomic_store_explicit(&publish_gate, 1, memory_order_release);
    client.port = owner.port;
    check_equal(salts_thread_create(&thread, chttp_fault_client_thread, &client), SALTS_OK);
    for (unsigned int elapsed = 0u;
         elapsed < CHTTP_FAULT_TIMEOUT_MS &&
         !atomic_load_explicit(&publish_entered, memory_order_acquire);
         ++elapsed)
      salts_sleep_ms(1u);
    check_equal(atomic_load_explicit(&publish_entered, memory_order_acquire), 1);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_EBUSY);
    atomic_store_explicit(&publish_allowed, 1, memory_order_release);
    check_equal(salts_thread_join(&thread), SALTS_OK);
    check_equal(client.status, SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_OK);
    check_equal(managed.accepted, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)1u);
    chttp_fault_owner_close(&owner);
  }

  it("settles reply and cancel terminal statuses exactly once across slot reuse") {
    static const int reply_terminals[] = {SALTS_ENOENT, SALTS_EALREADY};
    static const int cancel_terminals[] = {SALTS_OK, SALTS_ENOENT};
    for (size_t index = 0u; index < 4u; ++index) {
      chttp_fault_owner_t owner;
      turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
      check_equal(chttp_fault_owner_open(&owner), SALTS_OK);
      if (index < 2u) {
        atomic_store_explicit(&reply_status, reply_terminals[index], memory_order_relaxed);
      } else {
        atomic_store_explicit(&reply_status, SALTS_ENOBUFS, memory_order_relaxed);
        atomic_store_explicit(&cancel_status, cancel_terminals[index - 2u], memory_order_relaxed);
      }
      (void)chttp_fault_call(owner.port);
      check_equal(chttp_fault_wait_active(owner.server, 0u), SALTS_OK);
      check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_OK);
      check_equal(managed.accepted, (uint64_t)1u);
      check_equal(managed.completed, (uint64_t)1u);
      check_equal(managed.in_flight, (uint64_t)0u);
      atomic_store_explicit(&reply_status, SALTS_OK, memory_order_relaxed);
      atomic_store_explicit(&cancel_status, SALTS_OK, memory_order_relaxed);
      check_equal(chttp_fault_call(owner.port), SALTS_OK);
      check_equal(chttp_fault_wait_active(owner.server, 0u), SALTS_OK);
      check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_OK);
      check_equal(managed.accepted, index < 2u ? (uint64_t)2u : (uint64_t)1u);
      check_equal(managed.completed, index < 2u ? (uint64_t)2u : (uint64_t)1u);
      check_equal(managed.rejected, index < 2u ? (uint64_t)0u : (uint64_t)1u);
      chttp_fault_owner_close(&owner);
    }
  }

  it("retains a slot while reply and cancel are both nonterminal") {
    chttp_fault_owner_t owner;
    chttp_fault_client_t client = {0};
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    salts_thread_t thread = NULL;
    check_equal(chttp_fault_owner_open(&owner), SALTS_OK);
    atomic_store_explicit(&reply_status, SALTS_ENOBUFS, memory_order_relaxed);
    atomic_store_explicit(&cancel_status, SALTS_EIO, memory_order_relaxed);
    client.port = owner.port;
    check_equal(salts_thread_create(&thread, chttp_fault_client_thread, &client), SALTS_OK);
    for (unsigned int elapsed = 0u; elapsed < CHTTP_FAULT_TIMEOUT_MS; ++elapsed) {
      if (turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed) == SALTS_OK &&
          managed.in_flight == 1u)
        break;
      salts_sleep_ms(1u);
    }
    check_equal(managed.in_flight, (uint64_t)1u);
    check_equal(managed.completed, (uint64_t)0u);
    atomic_store_explicit(&reply_status, SALTS_OK, memory_order_relaxed);
    atomic_store_explicit(&cancel_status, SALTS_OK, memory_order_relaxed);
    check_equal(chttp_test_server_retry_first(owner.server), SALTS_OK);
    check_equal(salts_thread_join(&thread), SALTS_OK);
    check_equal(client.status, SALTS_OK);
    check_equal(chttp_fault_wait_active(owner.server, 0u), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_OK);
    check_equal(managed.completed, (uint64_t)1u);
    check_equal(managed.in_flight, (uint64_t)0u);
    chttp_fault_owner_close(&owner);
  }

  it("rejects generation overflow and stale owner commands under the owner lock") {
    chttp_fault_owner_t owner;
    turbo_flow_managed_boundary_descriptor_t descriptor =
        TURBO_FLOW_MANAGED_BOUNDARY_DESCRIPTOR_INIT;
    turbo_flow_managed_boundary_snapshot_t managed = TURBO_FLOW_MANAGED_BOUNDARY_SNAPSHOT_INIT;
    turbo_flow_resource_command_t command = TURBO_FLOW_RESOURCE_COMMAND_INIT;
    check_equal(chttp_fault_owner_open(&owner), SALTS_OK);
    check_equal(turbo_flow_managed_boundary_descriptor_at(owner.flow, 0u, &descriptor), SALTS_OK);
    check_equal(chttp_test_server_set_managed_generation(owner.server, UINT64_MAX), SALTS_OK);
    check_equal(turbo_flow_chttp_server_quiesce(owner.server), SALTS_ERANGE);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_OK);
    check_equal(managed.generation, UINT64_MAX);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_RUNNING);
    command.kind = TURBO_FLOW_RESOURCE_COMMAND_QUIESCE;
    command.expected_generation = UINT64_MAX;
    memcpy(command.target_uid, descriptor.uid, strlen(descriptor.uid) + 1u);
    memcpy(command.idempotency_key, "overflow", sizeof("overflow"));
    check_equal(chttp_test_server_direct_command(owner.server, &command), SALTS_ERANGE);
    check_equal(chttp_test_server_set_managed_generation(owner.server, 7u), SALTS_OK);
    command.expected_generation = 6u;
    check_equal(chttp_test_server_direct_command(owner.server, &command), SALTS_EBUSY);
    check_equal(turbo_flow_managed_boundary_snapshot_at(owner.flow, 0u, &managed), SALTS_OK);
    check_equal(managed.generation, (uint64_t)7u);
    check_equal(managed.state, TURBO_FLOW_MANAGED_BOUNDARY_RUNNING);
    chttp_fault_owner_close(&owner);
  }
}
