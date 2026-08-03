#include "flowie_cluster_pgsql_internal.h"

#include "monocypher.h"
#include "turbo_deque.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

typedef struct flowie_cluster_pgsql_fact_request_s {
  flowie_cluster_pgsql_fact_command_t command;
  flowie_cluster_pgsql_event_dedupe_t dedupe;
  flowie_cluster_pgsql_fact_completion_fn completion;
  void *completion_ctx;
  size_t reserved_bytes;
} flowie_cluster_pgsql_fact_request_t;

TURBO_DEQUE_DEFINE(flowie_cluster_pgsql_fact_request_queue_t, flowie_cluster_pgsql_fact_request_t *)

struct flowie_cluster_pgsql_fact_worker_s {
  flowie_cluster_pgsql_config_t coordinator_config;
  flowie_cluster_pgsql_fact_config_t fact_config;
  tstr_t conninfo;
  tstr_t schema_name;
  tstr_t cluster_id;
  tstr_t listener_id;
  tstr_t node_id;
  tstr_t advertised_endpoint;
  flowie_cluster_pgsql_fact_store_t *store;
  flowie_cluster_pgsql_fact_request_queue_t queue;
  size_t max_queue_entries;
  size_t max_queue_bytes;
  size_t queue_bytes;
  int closing;
  int closed;
  int sync_initialized;
  int queue_initialized;
  int thread_started;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
  turbo_thread_t thread;
};

int flowie_cluster_pgsql_fact_worker_config_validate(
    const flowie_cluster_pgsql_fact_worker_config_t *config) {
  int rc;
  if (!config || config->size < sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 || !config->fact ||
      config->max_queue_entries == 0u || config->max_queue_entries > SIZE_MAX / sizeof(void *) ||
      config->max_queue_bytes == 0u)
    return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_fact_config_validate(config->fact);
  if (rc != TURBO_OK) return rc;
  if (config->max_queue_bytes <
      sizeof(flowie_cluster_pgsql_fact_request_t) + sizeof(flowie_cluster_pgsql_fact_mutation_t))
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowie_cluster_pgsql_fact_worker_config_copy(
    flowie_cluster_pgsql_fact_worker_t *worker,
    const flowie_cluster_pgsql_fact_worker_config_t *config) {
  const flowie_cluster_pgsql_config_t *source = config->fact->coordinator;
  worker->conninfo = tstr_dup(source->conninfo);
  worker->schema_name = tstr_dup(source->schema_name);
  worker->cluster_id = tstr_dup(source->cluster_id);
  worker->listener_id = tstr_dup(source->listener_id);
  worker->node_id = tstr_dup(source->node_id);
  worker->advertised_endpoint = tstr_dup(source->advertised_endpoint);
  if (!worker->conninfo || !worker->schema_name || !worker->cluster_id || !worker->listener_id ||
      !worker->node_id || !worker->advertised_endpoint)
    return TURBO_ENOMEM;
  worker->coordinator_config = *source;
  worker->coordinator_config.conninfo = worker->conninfo;
  worker->coordinator_config.schema_name = worker->schema_name;
  worker->coordinator_config.cluster_id = worker->cluster_id;
  worker->coordinator_config.listener_id = worker->listener_id;
  worker->coordinator_config.node_id = worker->node_id;
  worker->coordinator_config.advertised_endpoint = worker->advertised_endpoint;
  worker->fact_config = *config->fact;
  worker->fact_config.coordinator = &worker->coordinator_config;
  worker->max_queue_entries = config->max_queue_entries;
  worker->max_queue_bytes = config->max_queue_bytes;
  return TURBO_OK;
}

static void
flowie_cluster_pgsql_fact_request_destroy(flowie_cluster_pgsql_fact_request_t *request) {
  if (!request) return;
  crypto_wipe(request, request->reserved_bytes);
  free(request);
}

static int
flowie_cluster_pgsql_fact_request_size(const flowie_cluster_pgsql_fact_command_t *command,
                                       size_t *out) {
  size_t total;
  if (!command || !out ||
      command->mutation_count > (SIZE_MAX - sizeof(flowie_cluster_pgsql_fact_request_t)) /
                                    sizeof(flowie_cluster_pgsql_fact_mutation_t))
    return TURBO_ERANGE;
  total = sizeof(flowie_cluster_pgsql_fact_request_t) +
          command->mutation_count * sizeof(flowie_cluster_pgsql_fact_mutation_t);
  for (size_t index = 0u; index < command->mutation_count; ++index) {
    const flowie_cluster_pgsql_fact_mutation_t *mutation = &command->mutations[index];
    const size_t sizes[4] = {mutation->shard_key_size, mutation->record.key_size,
                             mutation->record.value_size, mutation->event_payload_size};
    for (size_t part = 0u; part < sizeof(sizes) / sizeof(sizes[0]); ++part) {
      if (sizes[part] > SIZE_MAX - total) return TURBO_ERANGE;
      total += sizes[part];
    }
  }
  if (command->dedupe && command->dedupe->shared_filter_size != 0u) {
    if (command->dedupe->shared_filter_size > SIZE_MAX - total) return TURBO_ERANGE;
    total += command->dedupe->shared_filter_size;
  }
  *out = total;
  return TURBO_OK;
}

static uint8_t *flowie_cluster_pgsql_fact_copy_bytes(uint8_t *cursor, const uint8_t *source,
                                                     size_t size, const uint8_t **out) {
  if (size != 0u) {
    memcpy(cursor, source, size);
    *out = cursor;
    return cursor + size;
  }
  *out = NULL;
  return cursor;
}

static int
flowie_cluster_pgsql_fact_request_copy(const flowie_cluster_pgsql_fact_command_t *command,
                                       flowie_cluster_pgsql_fact_completion_fn completion,
                                       void *completion_ctx,
                                       flowie_cluster_pgsql_fact_request_t **out) {
  flowie_cluster_pgsql_fact_request_t *request;
  flowie_cluster_pgsql_fact_mutation_t *mutations;
  uint8_t *cursor;
  size_t allocation_size = 0u;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_pgsql_fact_request_size(command, &allocation_size);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  request = (flowie_cluster_pgsql_fact_request_t *)calloc(1u, allocation_size);
  if (!request) return TURBO_ENOMEM;
  mutations = (flowie_cluster_pgsql_fact_mutation_t *)(request + 1);
  cursor = (uint8_t *)(mutations + command->mutation_count);
  request->command = *command;
  request->command.mutations = mutations;
  if (command->dedupe) {
    request->dedupe = *command->dedupe;
    request->command.dedupe = &request->dedupe;
    cursor = flowie_cluster_pgsql_fact_copy_bytes(
        cursor, command->dedupe->shared_filter, command->dedupe->shared_filter_size,
        &request->dedupe.shared_filter);
  }
  request->completion = completion;
  request->completion_ctx = completion_ctx;
  request->reserved_bytes = allocation_size;
  for (size_t index = 0u; index < command->mutation_count; ++index) {
    const flowie_cluster_pgsql_fact_mutation_t *source = &command->mutations[index];
    flowie_cluster_pgsql_fact_mutation_t *target = &mutations[index];
    *target = *source;
    cursor = flowie_cluster_pgsql_fact_copy_bytes(cursor, source->shard_key, source->shard_key_size,
                                                  &target->shard_key);
    cursor = flowie_cluster_pgsql_fact_copy_bytes(cursor, source->record.key,
                                                  source->record.key_size, &target->record.key);
    cursor = flowie_cluster_pgsql_fact_copy_bytes(cursor, source->record.value,
                                                  source->record.value_size, &target->record.value);
    cursor = flowie_cluster_pgsql_fact_copy_bytes(
        cursor, source->event_payload, source->event_payload_size, &target->event_payload);
  }
  if ((size_t)(cursor - (uint8_t *)request) != allocation_size) {
    flowie_cluster_pgsql_fact_request_destroy(request);
    return TURBO_EPROTO;
  }
  *out = request;
  return TURBO_OK;
}

static int flowie_cluster_pgsql_fact_worker_reopen(flowie_cluster_pgsql_fact_worker_t *worker) {
  return !worker ? TURBO_EINVAL : flowie_cluster_pgsql_fact_store_reopen(&worker->store);
}

static int
flowie_cluster_pgsql_fact_worker_execute(flowie_cluster_pgsql_fact_worker_t *worker,
                                         const flowie_cluster_pgsql_fact_command_t *command) {
  int rc = flowie_cluster_pgsql_fact_commit(worker->store, command);
  if (rc == TURBO_OK) return TURBO_OK;
  /* A target-event replay must not publish newly staged in-memory session state. */
  if (rc == TURBO_EALREADY) return command->dedupe ? TURBO_EALREADY : TURBO_OK;
  if (rc != TURBO_EIO && rc != TURBO_ETIMEDOUT) return rc;
  rc = flowie_cluster_pgsql_fact_worker_reopen(worker);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_pgsql_fact_confirm(worker->store, command);
  if (rc == TURBO_OK) return TURBO_OK;
  if (rc != TURBO_ENOENT) return rc;
  rc = flowie_cluster_pgsql_fact_commit(worker->store, command);
  return rc == TURBO_EALREADY && !command->dedupe ? TURBO_OK : rc;
}

static void flowie_cluster_pgsql_fact_worker_run(void *ctx) {
  flowie_cluster_pgsql_fact_worker_t *worker = (flowie_cluster_pgsql_fact_worker_t *)ctx;
  for (;;) {
    flowie_cluster_pgsql_fact_request_t *request = NULL;
    int rc;
    turbo_mutex_lock(&worker->mutex);
    while (flowie_cluster_pgsql_fact_request_queue_t_empty(&worker->queue) && !worker->closing)
      turbo_cond_wait(&worker->changed, &worker->mutex);
    if (flowie_cluster_pgsql_fact_request_queue_t_empty(&worker->queue) && worker->closing) {
      worker->closed = 1;
      turbo_cond_broadcast(&worker->changed);
      turbo_mutex_unlock(&worker->mutex);
      return;
    }
    if (!flowie_cluster_pgsql_fact_request_queue_t_pop_front(&worker->queue, &request) ||
        !request) {
      worker->closing = 1;
      turbo_mutex_unlock(&worker->mutex);
      continue;
    }
    worker->queue_bytes -= request->reserved_bytes;
    turbo_mutex_unlock(&worker->mutex);
    rc = flowie_cluster_pgsql_fact_worker_execute(worker, &request->command);
    request->completion(request->completion_ctx, request->command.command_id, rc);
    flowie_cluster_pgsql_fact_request_destroy(request);
  }
}

static void
flowie_cluster_pgsql_fact_worker_storage_destroy(flowie_cluster_pgsql_fact_worker_t *worker) {
  flowie_cluster_pgsql_fact_request_t *request = NULL;
  if (!worker) return;
  if (worker->thread_started) {
    (void)turbo_thread_join(&worker->thread);
    turbo_thread_destroy(&worker->thread);
  }
  if (worker->queue_initialized) {
    while (flowie_cluster_pgsql_fact_request_queue_t_pop_front(&worker->queue, &request))
      flowie_cluster_pgsql_fact_request_destroy(request);
    flowie_cluster_pgsql_fact_request_queue_t_destroy(&worker->queue);
  }
  flowie_cluster_pgsql_fact_store_destroy(worker->store);
  if (worker->conninfo) crypto_wipe(worker->conninfo, tstr_len(worker->conninfo));
  tstr_freep(&worker->conninfo);
  tstr_freep(&worker->schema_name);
  tstr_freep(&worker->cluster_id);
  tstr_freep(&worker->listener_id);
  tstr_freep(&worker->node_id);
  tstr_freep(&worker->advertised_endpoint);
  if (worker->sync_initialized) {
    turbo_cond_destroy(&worker->changed);
    turbo_mutex_destroy(&worker->mutex);
  }
  free(worker);
}

int flowie_cluster_pgsql_fact_worker_create(const flowie_cluster_pgsql_fact_worker_config_t *config,
                                            flowie_cluster_pgsql_fact_worker_t **out) {
  flowie_cluster_pgsql_fact_worker_t *worker;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_pgsql_fact_worker_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  worker = (flowie_cluster_pgsql_fact_worker_t *)calloc(1u, sizeof(*worker));
  if (!worker) return TURBO_ENOMEM;
  turbo_mutex_init(&worker->mutex);
  turbo_cond_init(&worker->changed);
  worker->sync_initialized = 1;
  rc = flowie_cluster_pgsql_fact_request_queue_t_init(&worker->queue);
  if (rc != TURBO_OK) goto fail;
  worker->queue_initialized = 1;
  rc = flowie_cluster_pgsql_fact_request_queue_t_reserve(&worker->queue, config->max_queue_entries);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_cluster_pgsql_fact_worker_config_copy(worker, config);
  if (rc != TURBO_OK) goto fail;
  rc = flowie_cluster_pgsql_fact_store_open(&worker->fact_config, &worker->store);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_thread_create(&worker->thread, flowie_cluster_pgsql_fact_worker_run, worker);
  if (rc != TURBO_OK) goto fail;
  worker->thread_started = 1;
  *out = worker;
  return TURBO_OK;

fail:
  flowie_cluster_pgsql_fact_worker_storage_destroy(worker);
  return rc;
}

int flowie_cluster_pgsql_fact_worker_submit(flowie_cluster_pgsql_fact_worker_t *worker,
                                            const flowie_cluster_pgsql_fact_command_t *command,
                                            flowie_cluster_pgsql_fact_completion_fn completion,
                                            void *completion_ctx) {
  flowie_cluster_pgsql_fact_request_t *request = NULL;
  size_t queue_entries;
  int rc;
  if (!worker || !completion) return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_fact_command_validate(&worker->fact_config, command);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_pgsql_fact_request_copy(command, completion, completion_ctx, &request);
  if (rc != TURBO_OK) return rc;
  turbo_mutex_lock(&worker->mutex);
  queue_entries = flowie_cluster_pgsql_fact_request_queue_t_size(&worker->queue);
  if (worker->closing || worker->closed) rc = TURBO_ESHUTDOWN;
  else if (queue_entries >= worker->max_queue_entries ||
           request->reserved_bytes > worker->max_queue_bytes - worker->queue_bytes)
    rc = TURBO_ENOSPC;
  else {
    rc = flowie_cluster_pgsql_fact_request_queue_t_push_back(&worker->queue, request);
    if (rc == TURBO_OK) {
      worker->queue_bytes += request->reserved_bytes;
      turbo_cond_signal(&worker->changed);
    }
  }
  turbo_mutex_unlock(&worker->mutex);
  if (rc != TURBO_OK) flowie_cluster_pgsql_fact_request_destroy(request);
  return rc;
}

int flowie_cluster_pgsql_fact_worker_close(flowie_cluster_pgsql_fact_worker_t *worker) {
  if (!worker) return TURBO_EINVAL;
  turbo_mutex_lock(&worker->mutex);
  if (!worker->thread_started) {
    turbo_mutex_unlock(&worker->mutex);
    return TURBO_EALREADY;
  }
  if (worker->closing) {
    turbo_mutex_unlock(&worker->mutex);
    return TURBO_EALREADY;
  }
  worker->closing = 1;
  turbo_cond_broadcast(&worker->changed);
  turbo_mutex_unlock(&worker->mutex);
  (void)turbo_thread_join(&worker->thread);
  turbo_thread_destroy(&worker->thread);
  worker->thread_started = 0;
  return TURBO_OK;
}

void flowie_cluster_pgsql_fact_worker_destroy(flowie_cluster_pgsql_fact_worker_t *worker) {
  if (!worker) return;
  if (worker->thread_started) (void)flowie_cluster_pgsql_fact_worker_close(worker);
  flowie_cluster_pgsql_fact_worker_storage_destroy(worker);
}
