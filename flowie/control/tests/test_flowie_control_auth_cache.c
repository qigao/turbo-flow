#include "flowie_control_auth_cache_internal.h"
#include "flowie_control_credential_internal.h"
#include "flowie_control_store_internal.h"

#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static int auth_cache_root_create(flowie_control_store_t *store) {
  flowie_control_root_group_create_command_t command =
      FLOWIE_CONTROL_ROOT_GROUP_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.actor = "admin-1";
  command.request_id = "request-root-a";
  command.occurred_at = 1000u;
  return flowie_control_store_root_group_create(store, &command, &result);
}

static flowie_control_store_t *auth_cache_store_open(char **path_out) {
  flowie_control_store_config_t config = FLOWIE_CONTROL_STORE_CONFIG_INIT;
  flowie_control_store_t *store = NULL;
  *path_out = tt_make_temp_file("flowie-auth-cache", ".sqlite3");
  check_not_null(*path_out);
  config.database_path = *path_out;
  check_int_eq(flowie_control_store_open(&config, &store), TURBO_OK);
  check_not_null(store);
  check_int_eq(auth_cache_root_create(store), TURBO_OK);
  return store;
}

static void auth_cache_store_close(flowie_control_store_t *store, char *path) {
  flowie_control_store_destroy(store);
  check_int_eq(tt_remove_file(path), 0);
  free(path);
}

static int auth_cache_user_create(flowie_control_store_t *store, const char *principal_id,
                                  const char *request_id, uint64_t expected_revision) {
  flowie_control_user_create_command_t command = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.principal_id = principal_id;
  command.principal_type = "device";
  command.actor = "admin-1";
  command.request_id = request_id;
  command.expected_revision = expected_revision;
  command.occurred_at = 2000u + expected_revision;
  return flowie_control_store_user_create(store, &command, &result);
}

static int auth_cache_credential_generate(flowie_control_store_t *store, const char *principal_id,
                                          const char *request_id, uint64_t expected_revision,
                                          flowie_control_generated_credential_t *generated) {
  flowie_control_credential_issue_command_t command = FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  command.root_group_id = "root-a";
  command.principal_id = principal_id;
  command.actor = "admin-1";
  command.request_id = request_id;
  command.expected_revision = expected_revision;
  command.occurred_at = 3000u + expected_revision;
  return flowie_control_store_credential_generate(store, &command, generated);
}

static int auth_cache_credential_rotate(flowie_control_store_t *store, const char *principal_id,
                                        const char *request_id, uint64_t expected_revision,
                                        flowie_control_generated_credential_t *generated) {
  flowie_control_credential_issue_command_t command = FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
  command.root_group_id = "root-a";
  command.principal_id = principal_id;
  command.actor = "admin-1";
  command.request_id = request_id;
  command.expected_revision = expected_revision;
  command.occurred_at = 4000u + expected_revision;
  return flowie_control_store_credential_rotate(store, &command, generated);
}

static int auth_cache_credential_revoke(flowie_control_store_t *store, const char *principal_id,
                                        uint64_t expected_revision) {
  flowie_control_credential_revoke_command_t command =
      FLOWIE_CONTROL_CREDENTIAL_REVOKE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.principal_id = principal_id;
  command.actor = "admin-1";
  command.request_id = "request-revoke";
  command.expected_revision = expected_revision;
  command.occurred_at = 5000u + expected_revision;
  return flowie_control_store_credential_revoke(store, &command, &result);
}

static int auth_cache_user_disable(flowie_control_store_t *store, const char *principal_id,
                                   uint64_t expected_revision) {
  flowie_control_user_disable_command_t command = FLOWIE_CONTROL_USER_DISABLE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  command.root_group_id = "root-a";
  command.principal_id = principal_id;
  command.actor = "admin-1";
  command.request_id = "request-disable";
  command.expected_revision = expected_revision;
  command.occurred_at = 6000u + expected_revision;
  return flowie_control_store_user_disable(store, &command, &result);
}

static uint64_t auth_cache_test_clock(void *ctx) { return *(const uint64_t *)ctx; }

typedef struct auth_cache_concurrent_task_s {
  flowie_control_auth_cache_t *cache;
  flowie_control_store_t *store;
  const uint8_t *secret;
  size_t secret_size;
  atomic_int failures;
  atomic_int hits;
} auth_cache_concurrent_task_t;

static void auth_cache_concurrent_verify(void *arg) {
  enum { VERIFY_COUNT = 16 };
  auth_cache_concurrent_task_t *task = (auth_cache_concurrent_task_t *)arg;
  for (int index = 0; index < VERIFY_COUNT; ++index) {
    flowie_control_credential_verify_result_t verified =
        FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
    int cache_hit = 0;
    int rc =
        flowie_control_auth_cache_verify(task->cache, task->store, "root-a", "device-a",
                                         task->secret, task->secret_size, &verified, &cache_hit);
    if (rc != TURBO_OK || verified.user_revision != 2u || verified.credential_revision != 3u)
      atomic_fetch_add_explicit(&task->failures, 1, memory_order_relaxed);
    if (cache_hit) atomic_fetch_add_explicit(&task->hits, 1, memory_order_relaxed);
  }
}

spec("Flowie control authentication cache") {
  it("uses a short positive TTL and invalidates rotated revoked or disabled credentials") {
    char *path = NULL;
    flowie_control_store_t *store = auth_cache_store_open(&path);
    flowie_control_generated_credential_t first = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    flowie_control_generated_credential_t second = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    flowie_control_generated_credential_t third = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    flowie_control_credential_verify_result_t verified =
        FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
    flowie_control_auth_cache_config_t config = FLOWIE_CONTROL_AUTH_CACHE_CONFIG_INIT;
    flowie_control_auth_cache_t *cache = NULL;
    uint8_t wrong[FLOWIE_CONTROL_CREDENTIAL_SECRET_SIZE];
    uint64_t now_ms = 1000u;
    int cache_hit = -1;

    check_int_eq(auth_cache_user_create(store, "device-a", "request-user-a", 1u), TURBO_OK);
    check_int_eq(
        auth_cache_credential_generate(store, "device-a", "request-generate-a", 2u, &first),
        TURBO_OK);
    config.capacity = 2u;
    config.ttl_ms = 100u;
    config.clock_ms = auth_cache_test_clock;
    config.clock_ctx = &now_ms;
    check_int_eq(flowie_control_auth_cache_create(&config, &cache), TURBO_OK);

    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a", first.secret,
                                                  first.secret_size, &verified, &cache_hit),
                 TURBO_OK);
    check_false(cache_hit);
    check_uint_eq(verified.user_revision, 2u);
    check_uint_eq(verified.credential_revision, 3u);
    check_size_eq(flowie_control_auth_cache_size(cache), 1u);
    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a", first.secret,
                                                  first.secret_size, &verified, &cache_hit),
                 TURBO_OK);
    check_true(cache_hit);

    memcpy(wrong, first.secret, sizeof(wrong));
    wrong[0] ^= 0x40u;
    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a", wrong,
                                                  sizeof(wrong), &verified, &cache_hit),
                 TURBO_EPERM);
    check_false(cache_hit);
    check_size_eq(flowie_control_auth_cache_size(cache), 1u);
    flowie_control_credential_wipe(wrong, sizeof(wrong));

    now_ms = 1100u;
    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a", first.secret,
                                                  first.secret_size, &verified, &cache_hit),
                 TURBO_OK);
    check_false(cache_hit);

    check_int_eq(auth_cache_credential_rotate(store, "device-a", "request-rotate-a", 3u, &second),
                 TURBO_OK);
    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a", first.secret,
                                                  first.secret_size, &verified, &cache_hit),
                 TURBO_EPERM);
    check_false(cache_hit);
    check_size_eq(flowie_control_auth_cache_size(cache), 0u);
    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a", second.secret,
                                                  second.secret_size, &verified, &cache_hit),
                 TURBO_OK);
    check_false(cache_hit);
    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a", second.secret,
                                                  second.secret_size, &verified, &cache_hit),
                 TURBO_OK);
    check_true(cache_hit);

    check_int_eq(auth_cache_credential_revoke(store, "device-a", 4u), TURBO_OK);
    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a", second.secret,
                                                  second.secret_size, &verified, &cache_hit),
                 TURBO_EPERM);
    check_false(cache_hit);
    check_size_eq(flowie_control_auth_cache_size(cache), 0u);

    check_int_eq(
        auth_cache_credential_rotate(store, "device-a", "request-reactivate-a", 5u, &third),
        TURBO_OK);
    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a", third.secret,
                                                  third.secret_size, &verified, &cache_hit),
                 TURBO_OK);
    check_false(cache_hit);
    check_int_eq(auth_cache_user_disable(store, "device-a", 6u), TURBO_OK);
    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a", third.secret,
                                                  third.secret_size, &verified, &cache_hit),
                 TURBO_EPERM);
    check_false(cache_hit);
    check_size_eq(flowie_control_auth_cache_size(cache), 0u);

    flowie_control_auth_cache_destroy(cache);
    flowie_control_generated_credential_wipe(&first);
    flowie_control_generated_credential_wipe(&second);
    flowie_control_generated_credential_wipe(&third);
    auth_cache_store_close(store, path);
  }

  it("evicts the least recently used digest at the configured capacity") {
    char *path = NULL;
    flowie_control_store_t *store = auth_cache_store_open(&path);
    flowie_control_generated_credential_t first = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    flowie_control_generated_credential_t second = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    flowie_control_credential_verify_result_t verified =
        FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
    flowie_control_auth_cache_config_t config = FLOWIE_CONTROL_AUTH_CACHE_CONFIG_INIT;
    flowie_control_auth_cache_t *cache = NULL;
    uint64_t now_ms = 500u;
    int cache_hit = -1;

    check_int_eq(auth_cache_user_create(store, "device-a", "request-user-a", 1u), TURBO_OK);
    check_int_eq(
        auth_cache_credential_generate(store, "device-a", "request-generate-a", 2u, &first),
        TURBO_OK);
    check_int_eq(auth_cache_user_create(store, "device-b", "request-user-b", 3u), TURBO_OK);
    check_int_eq(
        auth_cache_credential_generate(store, "device-b", "request-generate-b", 4u, &second),
        TURBO_OK);
    config.capacity = 1u;
    config.clock_ms = auth_cache_test_clock;
    config.clock_ctx = &now_ms;
    check_int_eq(flowie_control_auth_cache_create(&config, &cache), TURBO_OK);

    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a", first.secret,
                                                  first.secret_size, &verified, &cache_hit),
                 TURBO_OK);
    check_false(cache_hit);
    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-b", second.secret,
                                                  second.secret_size, &verified, &cache_hit),
                 TURBO_OK);
    check_false(cache_hit);
    check_size_eq(flowie_control_auth_cache_size(cache), 1u);
    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a", first.secret,
                                                  first.secret_size, &verified, &cache_hit),
                 TURBO_OK);
    check_false(cache_hit);
    check_size_eq(flowie_control_auth_cache_size(cache), 1u);

    flowie_control_auth_cache_destroy(cache);
    flowie_control_generated_credential_wipe(&first);
    flowie_control_generated_credential_wipe(&second);
    auth_cache_store_close(store, path);
  }

  it("serves concurrent positive hits while keeping store checks outside the cache lock") {
    enum { THREAD_COUNT = 4, VERIFY_COUNT = 16 };
    char *path = NULL;
    flowie_control_store_t *store = auth_cache_store_open(&path);
    flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    flowie_control_credential_verify_result_t verified =
        FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
    flowie_control_auth_cache_config_t config = FLOWIE_CONTROL_AUTH_CACHE_CONFIG_INIT;
    flowie_control_auth_cache_t *cache = NULL;
    auth_cache_concurrent_task_t task;
    turbo_thread_t threads[THREAD_COUNT] = {0};
    int cache_hit = -1;

    check_int_eq(auth_cache_user_create(store, "device-a", "request-user-a", 1u), TURBO_OK);
    check_int_eq(
        auth_cache_credential_generate(store, "device-a", "request-generate-a", 2u, &generated),
        TURBO_OK);
    check_int_eq(flowie_control_auth_cache_create(&config, &cache), TURBO_OK);
    check_int_eq(flowie_control_auth_cache_verify(cache, store, "root-a", "device-a",
                                                  generated.secret, generated.secret_size,
                                                  &verified, &cache_hit),
                 TURBO_OK);
    check_false(cache_hit);

    task.cache = cache;
    task.store = store;
    task.secret = generated.secret;
    task.secret_size = generated.secret_size;
    atomic_init(&task.failures, 0);
    atomic_init(&task.hits, 0);
    for (int index = 0; index < THREAD_COUNT; ++index)
      check_int_eq(turbo_thread_create(&threads[index], auth_cache_concurrent_verify, &task), 0);
    for (int index = 0; index < THREAD_COUNT; ++index) {
      check_int_eq(turbo_thread_join(&threads[index]), 0);
      turbo_thread_destroy(&threads[index]);
    }
    check_int_eq(atomic_load_explicit(&task.failures, memory_order_relaxed), 0);
    check_int_eq(atomic_load_explicit(&task.hits, memory_order_relaxed),
                 THREAD_COUNT * VERIFY_COUNT);
    check_size_eq(flowie_control_auth_cache_size(cache), 1u);

    flowie_control_auth_cache_destroy(cache);
    flowie_control_generated_credential_wipe(&generated);
    auth_cache_store_close(store, path);
  }

  it("rejects unbounded capacity or TTL configuration") {
    flowie_control_auth_cache_config_t config = FLOWIE_CONTROL_AUTH_CACHE_CONFIG_INIT;
    flowie_control_auth_cache_t *cache = NULL;

    config.capacity = 0u;
    check_int_eq(flowie_control_auth_cache_create(&config, &cache), TURBO_EINVAL);
    check_null(cache);
    config.capacity = FLOWIE_CONTROL_AUTH_CACHE_MAX_CAPACITY + 1u;
    check_int_eq(flowie_control_auth_cache_create(&config, &cache), TURBO_EINVAL);
    config.capacity = FLOWIE_CONTROL_AUTH_CACHE_DEFAULT_CAPACITY;
    config.ttl_ms = FLOWIE_CONTROL_AUTH_CACHE_MAX_TTL_MS + 1u;
    check_int_eq(flowie_control_auth_cache_create(&config, &cache), TURBO_EINVAL);
    check_null(cache);
  }
}
