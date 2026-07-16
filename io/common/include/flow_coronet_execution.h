#ifndef FLOW_CORONET_EXECUTION_H
#define FLOW_CORONET_EXECUTION_H

#include "CoroNet/turbo_coro_object_pool.h"
#include "turbo_flow_coronet_execution.h"
#include "turbo_thread.h"

typedef struct tf_coronet_execution_s {
  coro_context_t *context;
  turbo_flow_coronet_execution_kind_t kind;
  int drives_context;
  int owns_context;
  int loop_thread_started;
  turbo_thread_t loop_thread;
} tf_coronet_execution_t;

typedef int (*tf_coronet_execution_call_fn)(void *arg);

int tf_coronet_execution_init(tf_coronet_execution_t *execution,
                              const turbo_flow_coronet_execution_binding_t *binding);
int tf_coronet_execution_init_with_pool(
    tf_coronet_execution_t *execution,
    const turbo_flow_coronet_execution_binding_t *binding,
    const coro_object_pool_config_t *private_pool_config);
int tf_coronet_execution_start(tf_coronet_execution_t *execution);
int tf_coronet_execution_post(tf_coronet_execution_t *execution, coro_post_fn fn, void *arg1,
                              void *arg2);
int tf_coronet_execution_call(tf_coronet_execution_t *execution, tf_coronet_execution_call_fn fn,
                              void *arg, uint64_t timeout_ns);
void tf_coronet_execution_stop(tf_coronet_execution_t *execution);
void tf_coronet_execution_destroy(tf_coronet_execution_t *execution);

#endif /* FLOW_CORONET_EXECUTION_H */
