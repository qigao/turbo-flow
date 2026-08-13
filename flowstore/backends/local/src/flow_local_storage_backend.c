#include "turbo_flow_local_storage_backend.h"

#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_index_store.h"
#include "turbo_flow_log_store.h"
#include "turbo_flow_series_store.h"
#include "turbo_flow_state_store.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_LOCAL_DEFAULT_MAX_KEY_SIZE (128u * 1024u)
#define FLOW_LOCAL_DEFAULT_MAX_VALUE_SIZE (2u * 1024u * 1024u)
#define FLOW_LOCAL_DEFAULT_MAX_BATCH_SIZE 256u
#define FLOW_LOCAL_DEFAULT_MAX_RECORDS 4096u
#define FLOW_LOCAL_DEFAULT_MAX_BYTES (64u * 1024u * 1024u)
#define FLOW_LOCAL_DEFAULT_MAX_ITEM_BYTES (2u * 1024u * 1024u)

typedef struct flow_local_record_entry_s {
  uint8_t *key;
  size_t key_size;
  uint8_t *value;
  size_t value_size;
  uint64_t revision;
  struct flow_local_record_entry_s *next;
} flow_local_record_entry_t;

typedef struct flow_local_record_store_s {
  flow_local_record_entry_t *head;
  size_t records;
  size_t bytes;
  size_t max_key_size;
  size_t max_value_size;
  size_t max_batch_size;
  size_t max_records;
  size_t max_bytes;
  size_t max_item_bytes;
  int closed;
} flow_local_record_store_t;

typedef struct flow_local_storage_instance_s {
  turbo_flow_storage_model_t model;
  union {
    turbo_flow_record_store_t record;
    turbo_flow_state_store_t *state;
    turbo_flow_index_store_t *index;
    turbo_flow_log_store_t *log;
    turbo_flow_series_store_t *series;
  } service;
} flow_local_storage_instance_t;

static int flow_local_size_add(size_t left, size_t right, size_t *out) {
  if (!out || left > SIZE_MAX - right) return TURBO_ERANGE;
  *out = left + right;
  return TURBO_OK;
}

static void flow_local_record_entry_destroy(flow_local_record_entry_t *entry) {
  if (!entry) return;
  free(entry->key);
  free(entry->value);
  free(entry);
}

static void flow_local_record_list_destroy(flow_local_record_entry_t *head) {
  while (head) {
    flow_local_record_entry_t *next = head->next;
    flow_local_record_entry_destroy(head);
    head = next;
  }
}

static flow_local_record_entry_t *flow_local_record_find(flow_local_record_entry_t *head,
                                                         const uint8_t *key, size_t key_size) {
  for (; head; head = head->next) {
    if (head->key_size == key_size && memcmp(head->key, key, key_size) == 0) return head;
  }
  return NULL;
}

static int flow_local_record_validate_mutation(const flow_local_record_store_t *store,
                                               const turbo_flow_record_mutation_t *mutation) {
  if (!store || !mutation || mutation->size < sizeof(*mutation) || !mutation->key ||
      mutation->key_size == 0u || mutation->key_size > store->max_key_size ||
      mutation->expected_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX ||
      mutation->next_revision > (uint64_t)TURBO_FLOW_RECORD_REVISION_MAX)
    return TURBO_EINVAL;
  if (mutation->kind == TURBO_FLOW_RECORD_PUT) {
    if ((!mutation->value && mutation->value_size != 0u) ||
        mutation->value_size > store->max_value_size ||
        mutation->next_revision <= mutation->expected_revision)
      return TURBO_EINVAL;
  } else if (mutation->kind == TURBO_FLOW_RECORD_DELETE) {
    if (mutation->expected_revision == TURBO_FLOW_RECORD_REVISION_ABSENT ||
        mutation->next_revision != TURBO_FLOW_RECORD_REVISION_ABSENT || mutation->value ||
        mutation->value_size != 0u)
      return TURBO_EINVAL;
  } else {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static int flow_local_record_clone(const flow_local_record_store_t *source,
                                   flow_local_record_store_t *target) {
  flow_local_record_entry_t **tail;
  if (!source || !target) return TURBO_EINVAL;
  *target = *source;
  target->head = NULL;
  tail = &target->head;
  for (const flow_local_record_entry_t *entry = source->head; entry; entry = entry->next) {
    flow_local_record_entry_t *copy = (flow_local_record_entry_t *)calloc(1u, sizeof(*copy));
    if (!copy) {
      flow_local_record_list_destroy(target->head);
      target->head = NULL;
      return TURBO_ENOMEM;
    }
    copy->key = (uint8_t *)malloc(entry->key_size);
    copy->value = (uint8_t *)malloc(entry->value_size ? entry->value_size : 1u);
    if (!copy->key || !copy->value) {
      flow_local_record_entry_destroy(copy);
      flow_local_record_list_destroy(target->head);
      target->head = NULL;
      return TURBO_ENOMEM;
    }
    memcpy(copy->key, entry->key, entry->key_size);
    if (entry->value_size > 0u) memcpy(copy->value, entry->value, entry->value_size);
    copy->key_size = entry->key_size;
    copy->value_size = entry->value_size;
    copy->revision = entry->revision;
    *tail = copy;
    tail = &copy->next;
  }
  return TURBO_OK;
}

static int flow_local_record_apply(flow_local_record_store_t *store,
                                   const turbo_flow_record_mutation_t *mutation) {
  flow_local_record_entry_t *entry;
  if (!store || !mutation) return TURBO_EINVAL;
  entry = flow_local_record_find(store->head, mutation->key, mutation->key_size);
  if ((entry ? entry->revision : TURBO_FLOW_RECORD_REVISION_ABSENT) !=
      mutation->expected_revision)
    return TURBO_EBUSY;
  if (mutation->kind == TURBO_FLOW_RECORD_DELETE) {
    flow_local_record_entry_t **link = &store->head;
    while (*link && *link != entry) link = &(*link)->next;
    if (!entry || !*link) return TURBO_EBUSY;
    *link = entry->next;
    store->records--;
    store->bytes -= entry->key_size + entry->value_size;
    flow_local_record_entry_destroy(entry);
    return TURBO_OK;
  }
  if (!entry) {
    if (store->records >= store->max_records || store->max_records - store->records == 0u)
      return TURBO_ENOSPC;
    {
      size_t item_bytes;
      size_t next_bytes;
      if (flow_local_size_add(mutation->key_size, mutation->value_size, &item_bytes) != TURBO_OK)
        return TURBO_EFBIG;
      if (item_bytes > store->max_item_bytes) return TURBO_EFBIG;
      if (flow_local_size_add(store->bytes, item_bytes, &next_bytes) != TURBO_OK ||
          next_bytes > store->max_bytes)
        return TURBO_ENOSPC;
    }
    entry = (flow_local_record_entry_t *)calloc(1u, sizeof(*entry));
    if (!entry) return TURBO_ENOMEM;
    entry->key = (uint8_t *)malloc(mutation->key_size);
    entry->value = (uint8_t *)malloc(mutation->value_size ? mutation->value_size : 1u);
    if (!entry->key || !entry->value) {
      flow_local_record_entry_destroy(entry);
      return TURBO_ENOMEM;
    }
    memcpy(entry->key, mutation->key, mutation->key_size);
    if (mutation->value_size > 0u) memcpy(entry->value, mutation->value, mutation->value_size);
    entry->key_size = mutation->key_size;
    entry->value_size = mutation->value_size;
    entry->revision = mutation->next_revision;
    entry->next = store->head;
    store->head = entry;
    store->records++;
    store->bytes += entry->key_size + entry->value_size;
    return TURBO_OK;
  }
  {
    size_t item_bytes;
    size_t next_bytes;
    if (flow_local_size_add(entry->key_size, mutation->value_size, &item_bytes) != TURBO_OK)
      return TURBO_EFBIG;
    if (item_bytes > store->max_item_bytes) return TURBO_EFBIG;
    if (flow_local_size_add(store->bytes - entry->value_size, mutation->value_size, &next_bytes) !=
            TURBO_OK ||
        next_bytes > store->max_bytes)
      return TURBO_ENOSPC;
  }
  {
    uint8_t *value = (uint8_t *)malloc(mutation->value_size ? mutation->value_size : 1u);
    if (!value) return TURBO_ENOMEM;
    if (mutation->value_size > 0u) memcpy(value, mutation->value, mutation->value_size);
    free(entry->value);
    entry->value = value;
    store->bytes = store->bytes - entry->value_size + mutation->value_size;
    entry->value_size = mutation->value_size;
    entry->revision = mutation->next_revision;
  }
  return TURBO_OK;
}

static int flow_local_record_scan(void *ctx, turbo_flow_record_visit_fn visit, void *visit_ctx) {
  flow_local_record_store_t *store = (flow_local_record_store_t *)ctx;
  if (!store || !visit) return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  for (flow_local_record_entry_t *entry = store->head; entry; entry = entry->next) {
    turbo_flow_record_view_t view = TURBO_FLOW_RECORD_VIEW_INIT;
    view.key = entry->key;
    view.key_size = entry->key_size;
    view.value = entry->value;
    view.value_size = entry->value_size;
    view.revision = entry->revision;
    {
      int rc = visit(visit_ctx, &view);
      if (rc != TURBO_OK) return rc;
    }
  }
  return TURBO_OK;
}

static int flow_local_record_commit(void *ctx, const turbo_flow_record_mutation_t *mutations,
                                    size_t mutation_count) {
  flow_local_record_store_t *store = (flow_local_record_store_t *)ctx;
  flow_local_record_store_t next;
  int rc;
  if (!store || !mutations || mutation_count == 0u || mutation_count > store->max_batch_size)
    return TURBO_EINVAL;
  if (store->closed) return TURBO_ESHUTDOWN;
  for (size_t i = 0u; i < mutation_count; ++i) {
    rc = flow_local_record_validate_mutation(store, &mutations[i]);
    if (rc != TURBO_OK) return rc;
    for (size_t j = 0u; j < i; ++j) {
      if (mutations[i].key_size == mutations[j].key_size &&
          memcmp(mutations[i].key, mutations[j].key, mutations[i].key_size) == 0)
        return TURBO_EINVAL;
    }
  }
  rc = flow_local_record_clone(store, &next);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < mutation_count; ++i) {
    rc = flow_local_record_apply(&next, &mutations[i]);
    if (rc != TURBO_OK) {
      flow_local_record_list_destroy(next.head);
      return rc;
    }
  }
  flow_local_record_list_destroy(store->head);
  store->head = next.head;
  store->records = next.records;
  store->bytes = next.bytes;
  return TURBO_OK;
}

static int flow_local_record_create(const turbo_flow_local_storage_backend_options_t *options,
                                    turbo_flow_record_store_t *out) {
  flow_local_record_store_t *store;
  const turbo_flow_store_limits_t *limits;
  size_t max_key_size = options && options->max_key_size ? options->max_key_size
                                                         : FLOW_LOCAL_DEFAULT_MAX_KEY_SIZE;
  size_t max_value_size = options && options->max_value_size ? options->max_value_size
                                                             : FLOW_LOCAL_DEFAULT_MAX_VALUE_SIZE;
  size_t max_batch_size = options && options->max_batch_size ? options->max_batch_size
                                                             : FLOW_LOCAL_DEFAULT_MAX_BATCH_SIZE;
  size_t max_records;
  size_t max_bytes;
  size_t max_item_bytes;
  if (!options || !options->limits) return TURBO_EINVAL;
  limits = options->limits;
  max_records = options->max_records ? options->max_records : limits->max_records;
  max_bytes = limits->max_bytes;
  max_item_bytes = limits->max_item_bytes;
  if (options->max_records && options->max_records > limits->max_records) return TURBO_EINVAL;
  if (!out || out->size < sizeof(*out) || out->ctx || max_key_size == 0u ||
      max_value_size == 0u || max_batch_size == 0u || max_records == 0u || max_bytes == 0u ||
      max_item_bytes == 0u)
    return TURBO_EINVAL;
  store = (flow_local_record_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->max_key_size = max_key_size;
  store->max_value_size = max_value_size;
  store->max_batch_size = max_batch_size;
  store->max_records = max_records;
  store->max_bytes = max_bytes;
  store->max_item_bytes = max_item_bytes;
  out->api_version = TURBO_FLOW_RECORD_STORE_API_VERSION;
  out->capabilities = TURBO_FLOW_RECORD_STORE_ATOMIC_BATCH;
  out->max_key_size = max_key_size;
  out->max_value_size = max_value_size;
  out->max_batch_size = max_batch_size;
  out->max_records = max_records;
  out->ctx = store;
  out->scan = flow_local_record_scan;
  out->commit = flow_local_record_commit;
  return TURBO_OK;
}

static void flow_local_record_destroy(turbo_flow_record_store_t *store) {
  flow_local_record_store_t *local;
  if (!store || store->size < sizeof(*store) || !store->ctx) return;
  local = (flow_local_record_store_t *)store->ctx;
  flow_local_record_list_destroy(local->head);
  free(local);
  *store = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
}

static int flow_local_limits_default(turbo_flow_store_limits_t *limits) {
  if (!limits) return TURBO_EINVAL;
  *limits = (turbo_flow_store_limits_t)TURBO_FLOW_STORE_LIMITS_INIT;
  limits->max_records = FLOW_LOCAL_DEFAULT_MAX_RECORDS;
  limits->max_bytes = FLOW_LOCAL_DEFAULT_MAX_BYTES;
  limits->max_item_bytes = FLOW_LOCAL_DEFAULT_MAX_ITEM_BYTES;
  return TURBO_OK;
}

static int flow_local_options(const turbo_flow_storage_backend_open_request_t *request,
                              turbo_flow_local_storage_backend_options_t *options) {
  if (!request || !options) return TURBO_EINVAL;
  *options = (turbo_flow_local_storage_backend_options_t)TURBO_FLOW_LOCAL_STORAGE_BACKEND_OPTIONS_INIT;
  if (!request->options) return TURBO_OK;
  if (request->options_size < sizeof(*options)) return TURBO_EINVAL;
  memcpy(options, request->options, sizeof(*options));
  if (options->size < sizeof(*options) ||
      options->version != TURBO_FLOW_LOCAL_STORAGE_BACKEND_OPTIONS_VERSION)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flow_local_storage_open(void *ctx, const turbo_flow_storage_backend_open_request_t *request,
                                   turbo_flow_storage_backend_service_t *service,
                                   turbo_flow_config_error_t *error) {
  flow_local_storage_instance_t *instance;
  turbo_flow_local_storage_backend_options_t options;
  turbo_flow_store_limits_t default_limits;
  const char *backend = NULL;
  turbo_flow_resolved_channel_view_t view = TURBO_FLOW_RESOLVED_CHANNEL_VIEW_INIT;
  int rc;
  (void)ctx;
  if (!request || !service || service->size < sizeof(*service) ||
      service->abi_version != TURBO_FLOW_STORAGE_BACKEND_ABI_VERSION ||
      request->model < TURBO_FLOW_STORAGE_MODEL_RECORD ||
      request->model > TURBO_FLOW_STORAGE_MODEL_SERIES)
    return TURBO_EINVAL;
  if (request->resolved) {
    rc = turbo_flow_resolved_config_channel(request->resolved, request->channel_name, &view);
    if (rc == TURBO_OK && strcmp(view.kind,
                                 request->model == TURBO_FLOW_STORAGE_MODEL_RECORD
                                     ? "record_store"
                                     : request->model == TURBO_FLOW_STORAGE_MODEL_STATE
                                           ? "state_store"
                                           : request->model == TURBO_FLOW_STORAGE_MODEL_INDEX
                                                 ? "index_store"
                                                 : request->model == TURBO_FLOW_STORAGE_MODEL_LOG
                                                       ? "log_store"
                                                       : "series_store") != 0)
      rc = TURBO_EINVAL;
    if (rc == TURBO_OK) rc = turbo_flow_resolved_channel_get_string(&view, "backend", &backend);
    if (rc == TURBO_OK && strcmp(backend, "local") != 0) rc = TURBO_ENOTSUP;
    if (rc != TURBO_OK) return rc;
  }
  rc = flow_local_options(request, &options);
  if (rc != TURBO_OK) return rc;
  rc = flow_local_limits_default(&default_limits);
  if (rc != TURBO_OK) return rc;
  if (!options.limits) options.limits = &default_limits;
  if (request->model != TURBO_FLOW_STORAGE_MODEL_SERIES || !options.series_config) {
    rc = turbo_flow_store_limits_validate(
        options.limits, request->model == TURBO_FLOW_STORAGE_MODEL_LOG);
    if (rc != TURBO_OK) return rc;
  }
  instance = (flow_local_storage_instance_t *)calloc(1u, sizeof(*instance));
  if (!instance) return TURBO_ENOMEM;
  instance->model = request->model;
  switch (request->model) {
  case TURBO_FLOW_STORAGE_MODEL_RECORD:
    instance->service.record = (turbo_flow_record_store_t)TURBO_FLOW_RECORD_STORE_INIT;
    rc = flow_local_record_create(&options, &instance->service.record);
    if (rc == TURBO_OK) service->instance = &instance->service.record;
    break;
  case TURBO_FLOW_STORAGE_MODEL_STATE:
    rc = turbo_flow_store_limits_validate(options.limits, 0);
    if (rc == TURBO_OK)
      rc = turbo_flow_state_store_create_memory(options.limits, &instance->service.state);
    if (rc == TURBO_OK) service->instance = instance->service.state;
    break;
  case TURBO_FLOW_STORAGE_MODEL_INDEX:
    rc = turbo_flow_store_limits_validate(options.limits, 0);
    if (rc == TURBO_OK)
      rc = turbo_flow_index_store_create_memory(options.limits, &instance->service.index);
    if (rc == TURBO_OK) service->instance = instance->service.index;
    break;
  case TURBO_FLOW_STORAGE_MODEL_LOG:
    rc = turbo_flow_store_limits_validate(options.limits, 1);
    if (rc == TURBO_OK)
      rc = turbo_flow_log_store_create_memory(options.limits, &instance->service.log);
    if (rc == TURBO_OK) service->instance = instance->service.log;
    break;
  case TURBO_FLOW_STORAGE_MODEL_SERIES:
    if (options.series_config) {
      rc = turbo_flow_series_store_create_memory(options.series_config, &instance->service.series);
    } else {
      turbo_flow_series_config_t config = TURBO_FLOW_SERIES_CONFIG_INIT;
      config.limits = *options.limits;
      rc = turbo_flow_series_store_create_memory(&config, &instance->service.series);
    }
    if (rc == TURBO_OK) service->instance = instance->service.series;
    break;
  default:
    rc = TURBO_ENOTSUP;
    break;
  }
  if (rc != TURBO_OK) {
    free(instance);
    return rc;
  }
  service->model = request->model;
  service->owner = instance;
  (void)error;
  return TURBO_OK;
}

static void flow_local_storage_close(void *ctx, turbo_flow_storage_backend_service_t *service) {
  flow_local_storage_instance_t *instance;
  (void)ctx;
  if (!service || !service->owner) return;
  instance = (flow_local_storage_instance_t *)service->owner;
  switch (service->model) {
  case TURBO_FLOW_STORAGE_MODEL_RECORD:
    flow_local_record_destroy(&instance->service.record);
    break;
  case TURBO_FLOW_STORAGE_MODEL_STATE:
    (void)turbo_flow_state_store_close(instance->service.state);
    turbo_flow_state_store_destroy(instance->service.state);
    break;
  case TURBO_FLOW_STORAGE_MODEL_INDEX:
    (void)turbo_flow_index_store_close(instance->service.index);
    turbo_flow_index_store_destroy(instance->service.index);
    break;
  case TURBO_FLOW_STORAGE_MODEL_LOG:
    (void)turbo_flow_log_store_close(instance->service.log);
    turbo_flow_log_store_destroy(instance->service.log);
    break;
  case TURBO_FLOW_STORAGE_MODEL_SERIES:
    (void)turbo_flow_series_store_close(instance->service.series);
    turbo_flow_series_store_destroy(instance->service.series);
    break;
  default:
    break;
  }
  service->instance = NULL;
  service->owner = NULL;
  free(instance);
}

static const turbo_flow_storage_backend_plugin_api_t FLOW_LOCAL_STORAGE_BACKEND_API = {
    sizeof(turbo_flow_storage_backend_plugin_api_t),
    TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MAJOR,
    TURBO_FLOW_STORAGE_BACKEND_PLUGIN_API_VERSION_MINOR,
    "local",
    TURBO_FLOW_STORAGE_CAP_RECORD | TURBO_FLOW_STORAGE_CAP_STATE | TURBO_FLOW_STORAGE_CAP_INDEX |
        TURBO_FLOW_STORAGE_CAP_LOG | TURBO_FLOW_STORAGE_CAP_SERIES,
    NULL,
    flow_local_storage_open,
    flow_local_storage_close};

const turbo_flow_storage_backend_plugin_api_t *turbo_flow_local_storage_backend_api(void) {
  return &FLOW_LOCAL_STORAGE_BACKEND_API;
}

/* Keep the builtin DLL loadable through the same canonical symbol as Redis/PG. */
const turbo_flow_storage_backend_plugin_api_t *
turbo_flow_storage_backend_plugin_get_api(void) {
  return &FLOW_LOCAL_STORAGE_BACKEND_API;
}
