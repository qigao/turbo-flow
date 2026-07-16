#include "flow_coronet_actor.h"

#include "platform.h"
#include "turbo_error.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

struct tf_coronet_actor_reply_s {
  turbo_mutex_t mutex;
  turbo_cond_t completed;
  atomic_int refs;
  uint64_t command_id;
  int started;
  int done;
  int status;
};

typedef struct tf_coronet_actor_job_s {
  tf_coronet_actor_t *actor;
  tf_coronet_actor_reply_t *reply;
  uint64_t deadline_ns;
  size_t command_size;
  unsigned char command[];
} tf_coronet_actor_job_t;

static void tf_coronet_actor_reply_release(tf_coronet_actor_reply_t *reply) {
  if (!reply || atomic_fetch_sub_explicit(&reply->refs, 1, memory_order_acq_rel) != 1) return;
  turbo_cond_destroy(&reply->completed);
  turbo_mutex_destroy(&reply->mutex);
  free(reply);
}

static void tf_coronet_actor_finish(tf_coronet_actor_job_t *job, int status, int started) {
  tf_coronet_actor_t *actor = job->actor;
  tf_coronet_actor_reply_t *reply = job->reply;

  turbo_mutex_lock(&reply->mutex);
  reply->started = started;
  reply->status = status;
  reply->done = 1;
  turbo_cond_broadcast(&reply->completed);
  turbo_mutex_unlock(&reply->mutex);

  turbo_mutex_lock(&actor->mutex);
  if (actor->pending > 0u) actor->pending -= 1u;
  if (actor->pending == 0u) turbo_cond_broadcast(&actor->drained);
  turbo_mutex_unlock(&actor->mutex);

  tf_coronet_actor_reply_release(reply);
  free(job);
}

static void tf_coronet_actor_run(void *arg1, void *arg2) {
  tf_coronet_actor_job_t *job = (tf_coronet_actor_job_t *)arg1;
  tf_coronet_actor_t *actor;
  int status;
  (void)arg2;

  if (!job || !job->actor || !job->reply) return;
  actor = job->actor;
  if (job->deadline_ns != UINT64_MAX && turbo_hrtime() >= job->deadline_ns) {
    tf_coronet_actor_finish(job, TURBO_ETIMEDOUT, 0);
    return;
  }

  turbo_mutex_lock(&job->reply->mutex);
  job->reply->started = 1;
  turbo_cond_broadcast(&job->reply->completed);
  turbo_mutex_unlock(&job->reply->mutex);
  status = actor->handler(actor->ctx, job->reply->command_id, job->command, job->command_size);
  tf_coronet_actor_finish(job, status, 1);
}

static int tf_coronet_actor_wait_locked(tf_coronet_actor_reply_t *reply, uint64_t timeout_ns,
                                        int wait_started, int *status) {
  uint64_t deadline_ns = UINT64_MAX;
  int rc = TURBO_OK;

  if (timeout_ns != UINT64_MAX) {
    uint64_t now_ns = turbo_hrtime();
    deadline_ns = timeout_ns > UINT64_MAX - now_ns ? UINT64_MAX : now_ns + timeout_ns;
  }
  while (!reply->done && (!wait_started || !reply->started)) {
    if (deadline_ns == UINT64_MAX) {
      turbo_cond_wait(&reply->completed, &reply->mutex);
    } else {
      uint64_t now_ns = turbo_hrtime();
      if (now_ns >= deadline_ns) {
        rc = TURBO_ETIMEDOUT;
        break;
      }
      (void)turbo_cond_timedwait(&reply->completed, &reply->mutex, deadline_ns - now_ns);
    }
  }
  if (reply->done) {
    *status = reply->status;
    return TURBO_OK;
  }
  return rc;
}

int tf_coronet_actor_init(tf_coronet_actor_t *actor, tf_coronet_execution_t *execution,
                          tf_coronet_actor_handler_fn handler, void *ctx, size_t capacity,
                          size_t max_command_size) {
  if (!actor || !execution || !execution->context || !handler || capacity == 0u ||
      max_command_size == 0u) {
    return TURBO_EINVAL;
  }
  memset(actor, 0, sizeof(*actor));
  actor->execution = execution;
  actor->handler = handler;
  actor->ctx = ctx;
  actor->capacity = capacity;
  actor->max_command_size = max_command_size;
  actor->next_command_id = 1u;
  turbo_mutex_init(&actor->mutex);
  turbo_cond_init(&actor->drained);
  actor->initialized = 1;
  actor->accepting = 1;
  return TURBO_OK;
}

int tf_coronet_actor_submit(tf_coronet_actor_t *actor, const void *command, size_t command_size,
                            uint64_t deadline_ns, tf_coronet_actor_reply_t **out) {
  tf_coronet_actor_reply_t *reply;
  tf_coronet_actor_job_t *job;
  uint64_t command_id = 0u;
  int rc;

  if (!actor || !actor->initialized || !command || command_size == 0u || !out) {
    return TURBO_EINVAL;
  }
  *out = NULL;
  if (command_size > actor->max_command_size) return TURBO_EMSGSIZE;
  if (deadline_ns != UINT64_MAX && turbo_hrtime() >= deadline_ns) return TURBO_ETIMEDOUT;

  reply = (tf_coronet_actor_reply_t *)calloc(1, sizeof(*reply));
  if (!reply) return TURBO_ENOMEM;
  job = (tf_coronet_actor_job_t *)malloc(sizeof(*job) + command_size);
  if (!job) {
    free(reply);
    return TURBO_ENOMEM;
  }
  turbo_mutex_init(&reply->mutex);
  turbo_cond_init(&reply->completed);
  atomic_init(&reply->refs, 2);
  reply->status = TURBO_EALREADY;

  turbo_mutex_lock(&actor->mutex);
  if (!actor->accepting) {
    rc = TURBO_ESHUTDOWN;
  } else if (actor->pending >= actor->capacity) {
    rc = TURBO_ENOSPC;
  } else if (actor->next_command_id == 0u) {
    rc = TURBO_ERANGE;
  } else {
    rc = TURBO_OK;
    command_id = actor->next_command_id++;
    actor->pending += 1u;
  }
  turbo_mutex_unlock(&actor->mutex);
  if (rc != TURBO_OK) {
    tf_coronet_actor_reply_release(reply);
    tf_coronet_actor_reply_release(reply);
    free(job);
    return rc;
  }

  reply->command_id = command_id;
  job->actor = actor;
  job->reply = reply;
  job->deadline_ns = deadline_ns;
  job->command_size = command_size;
  memcpy(job->command, command, command_size);
  rc = tf_coronet_execution_post(actor->execution, tf_coronet_actor_run, job, NULL);
  if (rc != TURBO_OK) {
    turbo_mutex_lock(&actor->mutex);
    actor->pending -= 1u;
    if (actor->pending == 0u) turbo_cond_broadcast(&actor->drained);
    turbo_mutex_unlock(&actor->mutex);
    tf_coronet_actor_reply_release(reply);
    tf_coronet_actor_reply_release(reply);
    free(job);
    return rc;
  }
  *out = reply;
  return TURBO_OK;
}

int tf_coronet_actor_call(tf_coronet_actor_t *actor, const void *command, size_t command_size,
                          uint64_t timeout_ns) {
  tf_coronet_actor_reply_t *reply = NULL;
  uint64_t deadline_ns;
  int status = TURBO_EALREADY;
  int rc;

  if (timeout_ns == 0u) return TURBO_EINVAL;
  if (timeout_ns == UINT64_MAX) {
    deadline_ns = UINT64_MAX;
  } else {
    uint64_t now_ns = turbo_hrtime();
    deadline_ns = timeout_ns > UINT64_MAX - now_ns ? UINT64_MAX : now_ns + timeout_ns;
  }
  rc = tf_coronet_actor_submit(actor, command, command_size, deadline_ns, &reply);
  if (rc != TURBO_OK) return rc;

  turbo_mutex_lock(&reply->mutex);
  if (deadline_ns == UINT64_MAX) {
    rc = tf_coronet_actor_wait_locked(reply, UINT64_MAX, 1, &status);
  } else {
    uint64_t now_ns = turbo_hrtime();
    rc = tf_coronet_actor_wait_locked(reply, now_ns < deadline_ns ? deadline_ns - now_ns : 0u, 1,
                                      &status);
  }
  if (rc == TURBO_OK && !reply->done && reply->started) {
    while (!reply->done)
      turbo_cond_wait(&reply->completed, &reply->mutex);
    status = reply->status;
  }
  turbo_mutex_unlock(&reply->mutex);
  tf_coronet_actor_reply_destroy(reply);
  return rc == TURBO_OK ? status : rc;
}

uint64_t tf_coronet_actor_reply_command_id(const tf_coronet_actor_reply_t *reply) {
  return reply ? reply->command_id : 0u;
}

int tf_coronet_actor_reply_poll(tf_coronet_actor_reply_t *reply, int *status) {
  int done;
  if (!reply || !status) return TURBO_EINVAL;
  turbo_mutex_lock(&reply->mutex);
  done = reply->done;
  if (done) *status = reply->status;
  turbo_mutex_unlock(&reply->mutex);
  return done ? TURBO_OK : TURBO_EBUSY;
}

int tf_coronet_actor_reply_wait(tf_coronet_actor_reply_t *reply, uint64_t timeout_ns, int *status) {
  int rc;
  if (!reply || !status) return TURBO_EINVAL;
  turbo_mutex_lock(&reply->mutex);
  rc = tf_coronet_actor_wait_locked(reply, timeout_ns, 0, status);
  turbo_mutex_unlock(&reply->mutex);
  return rc;
}

void tf_coronet_actor_reply_destroy(tf_coronet_actor_reply_t *reply) {
  tf_coronet_actor_reply_release(reply);
}

void tf_coronet_actor_close(tf_coronet_actor_t *actor) {
  if (!actor || !actor->initialized) return;
  turbo_mutex_lock(&actor->mutex);
  actor->accepting = 0;
  turbo_mutex_unlock(&actor->mutex);
}

int tf_coronet_actor_drain(tf_coronet_actor_t *actor, uint64_t timeout_ns) {
  uint64_t deadline_ns = UINT64_MAX;
  int rc = TURBO_OK;
  if (!actor || !actor->initialized) return TURBO_EINVAL;
  if (timeout_ns != UINT64_MAX) {
    uint64_t now_ns = turbo_hrtime();
    deadline_ns = timeout_ns > UINT64_MAX - now_ns ? UINT64_MAX : now_ns + timeout_ns;
  }
  turbo_mutex_lock(&actor->mutex);
  while (actor->pending != 0u) {
    if (deadline_ns == UINT64_MAX) {
      turbo_cond_wait(&actor->drained, &actor->mutex);
    } else {
      uint64_t now_ns = turbo_hrtime();
      if (now_ns >= deadline_ns) {
        rc = TURBO_ETIMEDOUT;
        break;
      }
      (void)turbo_cond_timedwait(&actor->drained, &actor->mutex, deadline_ns - now_ns);
    }
  }
  turbo_mutex_unlock(&actor->mutex);
  return rc;
}

int tf_coronet_actor_destroy(tf_coronet_actor_t *actor) {
  if (!actor || !actor->initialized) return TURBO_EINVAL;
  turbo_mutex_lock(&actor->mutex);
  if (actor->accepting || actor->pending != 0u) {
    turbo_mutex_unlock(&actor->mutex);
    return TURBO_EBUSY;
  }
  actor->initialized = 0;
  turbo_mutex_unlock(&actor->mutex);
  turbo_cond_destroy(&actor->drained);
  turbo_mutex_destroy(&actor->mutex);
  memset(actor, 0, sizeof(*actor));
  return TURBO_OK;
}
