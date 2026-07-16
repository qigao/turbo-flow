#ifndef FLOW_CORONET_ACTOR_H
#define FLOW_CORONET_ACTOR_H

#include "flow_coronet_execution.h"
#include "turbo_thread.h"

#include <stddef.h>
#include <stdint.h>

#define TF_CORONET_ACTOR_DEFAULT_CAPACITY 256u
#define TF_CORONET_ACTOR_DEFAULT_MAX_COMMAND_SIZE 4096u

typedef struct tf_coronet_actor_reply_s tf_coronet_actor_reply_t;

/**
 * Execute one copied command on the actor's CoroNet owner lane.
 *
 * command is immutable and valid only for the call. command_id is unique for
 * the actor lifetime. The handler must not retain either value.
 */
typedef int (*tf_coronet_actor_handler_fn)(void *ctx, uint64_t command_id, const void *command,
                                           size_t command_size);

/** Internal owner-lane actor. Lifecycle calls are host-serialized. */
typedef struct tf_coronet_actor_s {
  tf_coronet_execution_t *execution;
  tf_coronet_actor_handler_fn handler;
  void *ctx;
  size_t capacity;
  size_t max_command_size;
  size_t pending;
  uint64_t next_command_id;
  turbo_mutex_t mutex;
  turbo_cond_t drained;
  int initialized;
  int accepting;
} tf_coronet_actor_t;

int tf_coronet_actor_init(tf_coronet_actor_t *actor, tf_coronet_execution_t *execution,
                          tf_coronet_actor_handler_fn handler, void *ctx, size_t capacity,
                          size_t max_command_size);

/**
 * Copy and post one pointer-free command. deadline_ns is absolute turbo_hrtime
 * or UINT64_MAX. Returns TURBO_ENOSPC at capacity and TURBO_ESHUTDOWN after close.
 */
int tf_coronet_actor_submit(tf_coronet_actor_t *actor, const void *command, size_t command_size,
                            uint64_t deadline_ns, tf_coronet_actor_reply_t **out);

/**
 * Compatibility wrapper for a synchronous owner command.
 *
 * If the command starts before timeout_ns expires, this waits for its terminal
 * reply because an executing owner mutation cannot be canceled safely.
 */
int tf_coronet_actor_call(tf_coronet_actor_t *actor, const void *command, size_t command_size,
                          uint64_t timeout_ns);

uint64_t tf_coronet_actor_reply_command_id(const tf_coronet_actor_reply_t *reply);

/** Return TURBO_EBUSY until complete; command status is written to status. */
int tf_coronet_actor_reply_poll(tf_coronet_actor_reply_t *reply, int *status);

/**
 * Wait for a terminal reply without canceling the command on timeout.
 * timeout_ns is relative; UINT64_MAX waits without a deadline.
 */
int tf_coronet_actor_reply_wait(tf_coronet_actor_reply_t *reply, uint64_t timeout_ns, int *status);

void tf_coronet_actor_reply_destroy(tf_coronet_actor_reply_t *reply);

/** Stop command admission. Already accepted commands still produce replies. */
void tf_coronet_actor_close(tf_coronet_actor_t *actor);

/** Wait until every accepted handler has returned. */
int tf_coronet_actor_drain(tf_coronet_actor_t *actor, uint64_t timeout_ns);

/** Destroy a closed, drained actor; otherwise returns TURBO_EBUSY. */
int tf_coronet_actor_destroy(tf_coronet_actor_t *actor);

#endif /* FLOW_CORONET_ACTOR_H */
