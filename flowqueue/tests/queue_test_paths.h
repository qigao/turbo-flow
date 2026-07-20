#ifndef QUEUE_TEST_PATHS_H
#define QUEUE_TEST_PATHS_H

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_fs.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>

static void queue_test_database_path(char *path, size_t path_size) {
  static atomic_int sequence = 0;
  char directory[TURBO_FS_MAX_PATH];
  char filename[128];
  int id;
  check_int_eq(turbo_fs_get_tmpdir(directory, sizeof(directory)), TURBO_OK);
  id = atomic_fetch_add_explicit(&sequence, 1, memory_order_relaxed);
  (void)snprintf(filename, sizeof(filename), "turbo_flow_queue_%d_%d.sqlite3", turbo_getpid(), id);
  check_int_eq(turbo_fs_path_join(path, path_size, directory, filename), TURBO_OK);
  if (turbo_fs_access(path, TURBO_FS_ACCESS_EXISTS) == TURBO_OK) {
    check_int_eq(turbo_fs_unlink(path), TURBO_OK);
  }
}

static void queue_remove_database(const char *path) {
  char sidecar[TURBO_FS_MAX_PATH];
  if (turbo_fs_access(path, TURBO_FS_ACCESS_EXISTS) == TURBO_OK) (void)turbo_fs_unlink(path);
  (void)snprintf(sidecar, sizeof(sidecar), "%s-wal", path);
  if (turbo_fs_access(sidecar, TURBO_FS_ACCESS_EXISTS) == TURBO_OK) (void)turbo_fs_unlink(sidecar);
  (void)snprintf(sidecar, sizeof(sidecar), "%s-shm", path);
  if (turbo_fs_access(sidecar, TURBO_FS_ACCESS_EXISTS) == TURBO_OK) (void)turbo_fs_unlink(sidecar);
}

#endif /* QUEUE_TEST_PATHS_H */
