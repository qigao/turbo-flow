#ifndef FLOWIE_TASK_GROUP_INTERNAL_H
#define FLOWIE_TASK_GROUP_INTERNAL_H

#include "turbo_error.h"
#include "turbo_thread.h"

#include <stddef.h>

typedef struct flowie_task_group_s {
  turbo_mutex_t mutex;
  turbo_cond_t drained;
  size_t count;
  int admission_open;
} flowie_task_group_t;

static inline void flowie_task_group_init(flowie_task_group_t *group) {
  turbo_mutex_init(&group->mutex);
  turbo_cond_init(&group->drained);
  group->count = 0u;
  group->admission_open = 0;
}

static inline void flowie_task_group_destroy(flowie_task_group_t *group) {
  turbo_cond_destroy(&group->drained);
  turbo_mutex_destroy(&group->mutex);
}

static inline int flowie_task_group_open(flowie_task_group_t *group) {
  int rc = TURBO_OK;
  turbo_mutex_lock(&group->mutex);
  if (group->count != 0u) {
    rc = TURBO_EBUSY;
  } else {
    group->admission_open = 1;
  }
  turbo_mutex_unlock(&group->mutex);
  return rc;
}

static inline void flowie_task_group_close(flowie_task_group_t *group) {
  turbo_mutex_lock(&group->mutex);
  group->admission_open = 0;
  turbo_mutex_unlock(&group->mutex);
}

static inline int flowie_task_group_try_begin(flowie_task_group_t *group) {
  int rc = TURBO_OK;
  turbo_mutex_lock(&group->mutex);
  if (!group->admission_open) {
    rc = TURBO_ESHUTDOWN;
  } else {
    group->count += 1u;
  }
  turbo_mutex_unlock(&group->mutex);
  return rc;
}

static inline void flowie_task_group_end(flowie_task_group_t *group) {
  turbo_mutex_lock(&group->mutex);
  if (group->count > 0u) group->count -= 1u;
  if (group->count == 0u) turbo_cond_broadcast(&group->drained);
  turbo_mutex_unlock(&group->mutex);
}

static inline void flowie_task_group_wait(flowie_task_group_t *group) {
  turbo_mutex_lock(&group->mutex);
  while (group->count != 0u) turbo_cond_wait(&group->drained, &group->mutex);
  turbo_mutex_unlock(&group->mutex);
}

#endif
