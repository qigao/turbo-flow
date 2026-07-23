#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_config.h"
#include "turbo_flow_local_storage_backend.h"
#include "turbo_flow_storage_backend.h"

#include <string.h>

static int local_record_count(void *ctx, const turbo_flow_record_view_t *record) {
  size_t *count = (size_t *)ctx;
  if (!count || !record || record->key_size == 0u) return TURBO_EINVAL;
  (*count)++;
  return TURBO_OK;
}

spec("turbo_flow_local_storage_backend") {
  it("opens an in-memory record service through the common owner ABI") {
    turbo_flow_storage_backend_registry_t *registry = NULL;
    turbo_flow_storage_backend_owner_t *owner = NULL;
    turbo_flow_storage_backend_open_request_t request =
        TURBO_FLOW_STORAGE_BACKEND_OPEN_REQUEST_INIT;
    turbo_flow_local_storage_backend_options_t options =
        TURBO_FLOW_LOCAL_STORAGE_BACKEND_OPTIONS_INIT;
    turbo_flow_record_store_t *store = NULL;
    turbo_flow_record_mutation_t mutation = TURBO_FLOW_RECORD_MUTATION_INIT;
    turbo_flow_record_store_t *service = NULL;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    size_t count = 0u;
    static const uint8_t key[] = {'k'};
    static const uint8_t value[] = {'v'};

    options.max_key_size = 8u;
    options.max_value_size = 8u;
    options.max_batch_size = 2u;
    options.max_records = 2u;
    check_int_eq(turbo_flow_storage_backend_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_registry_register(
                     registry, turbo_flow_local_storage_backend_api()),
                 TURBO_OK);
    request.model = TURBO_FLOW_STORAGE_MODEL_RECORD;
    request.options = &options;
    request.options_size = sizeof(options);
    check_int_eq(turbo_flow_storage_backend_owner_create_registered(
                     registry, "local", &request, &owner, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_owner_service(
                     owner, TURBO_FLOW_STORAGE_MODEL_RECORD, (void **)&service),
                 TURBO_OK);
    store = service;
    mutation.key = key;
    mutation.key_size = sizeof(key);
    mutation.next_revision = 1u;
    mutation.value = value;
    mutation.value_size = sizeof(value);
    check_int_eq(store->commit(store->ctx, &mutation, 1u), TURBO_OK);
    check_int_eq(store->scan(store->ctx, local_record_count, &count), TURBO_OK);
    check_size_eq(count, 1u);
    turbo_flow_storage_backend_owner_destroy(owner);
    check_int_eq(turbo_flow_storage_backend_registry_destroy(registry), TURBO_OK);
  }

  it("keeps record batches atomic and opens every model with default limits") {
    turbo_flow_storage_backend_registry_t *registry = NULL;
    turbo_flow_storage_backend_owner_t *owner = NULL;
    turbo_flow_storage_backend_open_request_t request =
        TURBO_FLOW_STORAGE_BACKEND_OPEN_REQUEST_INIT;
    turbo_flow_local_storage_backend_options_t options =
        TURBO_FLOW_LOCAL_STORAGE_BACKEND_OPTIONS_INIT;
    turbo_flow_store_limits_t limits = TURBO_FLOW_STORE_LIMITS_INIT;
    turbo_flow_record_store_t *store = NULL;
    turbo_flow_record_mutation_t mutations[2] = {TURBO_FLOW_RECORD_MUTATION_INIT,
                                                   TURBO_FLOW_RECORD_MUTATION_INIT};
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
    size_t count = 0u;
    static const uint8_t key_a[] = {'a'};
    static const uint8_t key_b[] = {'b'};
    static const uint8_t value[] = {'v'};

    limits.max_records = 1u;
    limits.max_bytes = 2u;
    limits.max_item_bytes = 2u;
    options.limits = &limits;
    options.max_records = 1u;
    check_int_eq(turbo_flow_storage_backend_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_registry_register(
                     registry, turbo_flow_local_storage_backend_api()),
                 TURBO_OK);
    request.model = TURBO_FLOW_STORAGE_MODEL_RECORD;
    request.options = &options;
    request.options_size = sizeof(options);
    check_int_eq(turbo_flow_storage_backend_owner_create_registered(
                     registry, "local", &request, &owner, &error),
                 TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_owner_service(
                     owner, TURBO_FLOW_STORAGE_MODEL_RECORD, (void **)&store),
                 TURBO_OK);
    mutations[0].key = key_a;
    mutations[0].key_size = sizeof(key_a);
    mutations[0].next_revision = 1u;
    mutations[0].value = value;
    mutations[0].value_size = sizeof(value);
    mutations[1].key = key_b;
    mutations[1].key_size = sizeof(key_b);
    mutations[1].next_revision = 1u;
    mutations[1].value = value;
    mutations[1].value_size = sizeof(value);
    check_int_eq(store->commit(store->ctx, mutations, 2u), TURBO_ENOSPC);
    check_int_eq(store->scan(store->ctx, local_record_count, &count), TURBO_OK);
    check_size_eq(count, 0u);
    turbo_flow_storage_backend_owner_destroy(owner);
    check_int_eq(turbo_flow_storage_backend_registry_destroy(registry), TURBO_OK);

    options = (turbo_flow_local_storage_backend_options_t)
        TURBO_FLOW_LOCAL_STORAGE_BACKEND_OPTIONS_INIT;
    check_int_eq(turbo_flow_storage_backend_registry_create(1u, &registry), TURBO_OK);
    check_int_eq(turbo_flow_storage_backend_registry_register(
                     registry, turbo_flow_local_storage_backend_api()),
                 TURBO_OK);
    request.options = &options;
    request.options_size = sizeof(options);
    for (request.model = TURBO_FLOW_STORAGE_MODEL_STATE;
         request.model <= TURBO_FLOW_STORAGE_MODEL_SERIES; ++request.model) {
      check_int_eq(turbo_flow_storage_backend_owner_create_registered(
                       registry, "local", &request, &owner, &error),
                   TURBO_OK);
      turbo_flow_storage_backend_owner_destroy(owner);
      owner = NULL;
    }
    check_int_eq(turbo_flow_storage_backend_registry_destroy(registry), TURBO_OK);
  }
}
