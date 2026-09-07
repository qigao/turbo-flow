#include "turbo_flow_turbodb.h"

#include <cmeta/cmeta.h>
#include <salts_error.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TURBO_FLOW_TURBODB_ERROR_CAPACITY 192u

typedef struct turbo_flow_turbodb_publisher_s {
  cflow_publisher inner;
  const cmeta_type_desc *row_type;
  const turbo_flow_data_schema_t *schema;
  uint64_t next_message_id;
  uint32_t message_type;
  uint32_t message_flags;
  bool message_id_exhausted;
  bool failed;
  char error[TURBO_FLOW_TURBODB_ERROR_CAPACITY];
} turbo_flow_turbodb_publisher_t;

static bool turbodb_is_power_of_two(size_t value) {
  return value != 0u && (value & (value - 1u)) == 0u;
}

static void *turbodb_row_allocate(const cmeta_type_desc *type) {
  uintptr_t candidate;
  uintptr_t aligned;
  void *base;
  size_t overhead;
  size_t total;

  if (!type || !turbodb_is_power_of_two(type->align)) return NULL;
  overhead = sizeof(void *) + type->align - 1u;
  if (type->size > SIZE_MAX - overhead) return NULL;
  total = type->size + overhead;
  base = malloc(total);
  if (!base) return NULL;
  candidate = (uintptr_t)base + sizeof(void *);
  aligned = (candidate + type->align - 1u) & ~(uintptr_t)(type->align - 1u);
  ((void **)aligned)[-1] = base;
  return (void *)aligned;
}

static void turbodb_row_release(void *row) {
  if (row) free(((void **)row)[-1]);
}

static bool turbodb_row_copy(const cmeta_type_desc *type, void *destination, const void *source) {
  if ((type->traits->flags & CMETA_TRAIT_TRIVIAL_COPY) != 0u) {
    memcpy(destination, source, type->size);
    return true;
  }
  return type->traits->copy_construct(destination, source);
}

static void turbodb_row_destroy(const cmeta_type_desc *type, void *row) {
  if ((type->traits->flags & CMETA_TRAIT_TRIVIAL_DESTROY) == 0u) {
    type->traits->destroy(row);
  }
  turbodb_row_release(row);
}

static int turbodb_projection_clone(const void *value, void *ctx, void **out) {
  const cmeta_type_desc *type = (const cmeta_type_desc *)ctx;
  void *copy;

  if (!value || !type || !out) return SALTS_EINVAL;
  *out = NULL;
  copy = turbodb_row_allocate(type);
  if (!copy) return SALTS_ENOMEM;
  if (!turbodb_row_copy(type, copy, value)) {
    turbodb_row_release(copy);
    return SALTS_ENOMEM;
  }
  *out = copy;
  return SALTS_OK;
}

static void turbodb_projection_destroy(void *value, void *ctx) {
  turbodb_row_destroy((const cmeta_type_desc *)ctx, value);
}

static const char *turbodb_publisher_name(void *state) {
  (void)state;
  return "turbodb-orm-message-source";
}

static const cmeta_type_desc *turbodb_publisher_type(void *state) {
  (void)state;
  return turbo_flow_message_type();
}

static cflow_step turbodb_publisher_resume(void *state, cflow_publish_context *context,
                                           void *out_value) {
  turbo_flow_turbodb_publisher_t *publisher = (turbo_flow_turbodb_publisher_t *)state;
  turbo_flow_msg_t *message = (turbo_flow_msg_t *)out_value;
  cflow_step step;
  void *row;
  int rc;

  if (!publisher || !message) {
    return (cflow_step){CFLOW_STEP_ERROR, {0}, "invalid TurboDb adapter"};
  }
  if (publisher->failed) {
    return (cflow_step){CFLOW_STEP_ERROR, {0}, publisher->error};
  }
  row = turbodb_row_allocate(publisher->row_type);
  if (!row) {
    publisher->failed = true;
    (void)snprintf(publisher->error, sizeof(publisher->error),
                   "TurboDb adapter could not allocate one row");
    return (cflow_step){CFLOW_STEP_ERROR, {0}, publisher->error};
  }

  step = cflow_publisher_resume(&publisher->inner, context, row);
  if (step.kind != CFLOW_STEP_VALUE && step.kind != CFLOW_STEP_VALUE_AND_DONE) {
    turbodb_row_release(row);
    return step;
  }
  if (publisher->message_id_exhausted) {
    turbodb_row_destroy(publisher->row_type, row);
    publisher->failed = true;
    (void)snprintf(publisher->error, sizeof(publisher->error),
                   "TurboDb adapter message ID range exhausted");
    return (cflow_step){CFLOW_STEP_ERROR, {0}, publisher->error};
  }

  turbo_flow_msg_init(message);
  message->id = publisher->next_message_id;
  message->type = publisher->message_type;
  message->flags = publisher->message_flags;
  rc = turbo_flow_msg_bind_projection(message, publisher->schema, row, turbodb_projection_clone,
                                      turbodb_projection_destroy, (void *)publisher->row_type);
  if (rc != SALTS_OK) {
    turbodb_row_destroy(publisher->row_type, row);
    publisher->failed = true;
    (void)snprintf(publisher->error, sizeof(publisher->error),
                   "TurboDb adapter could not bind row projection: %d", rc);
    return (cflow_step){CFLOW_STEP_ERROR, {0}, publisher->error};
  }
  publisher->message_id_exhausted = publisher->next_message_id == UINT64_MAX;
  if (!publisher->message_id_exhausted) ++publisher->next_message_id;
  return step;
}

static void turbodb_publisher_cancel(void *state) {
  turbo_flow_turbodb_publisher_t *publisher = (turbo_flow_turbodb_publisher_t *)state;
  if (publisher && cflow_publisher_valid(&publisher->inner)) {
    cflow_publisher_cancel(&publisher->inner);
  }
}

static void turbodb_publisher_destroy(void *state) {
  turbo_flow_turbodb_publisher_t *publisher = (turbo_flow_turbodb_publisher_t *)state;
  if (!publisher) return;
  if (cflow_publisher_valid(&publisher->inner)) {
    cflow_publisher_destroy(&publisher->inner);
  }
  free(publisher);
}

static void turbodb_publisher_bind_terminal_waker(void *state, cflow_waker waker) {
  turbo_flow_turbodb_publisher_t *publisher = (turbo_flow_turbodb_publisher_t *)state;
  if (publisher && cflow_publisher_valid(&publisher->inner)) {
    cflow_publisher_bind_terminal_waker(&publisher->inner, waker);
  }
}

static cflow_publisher_terminal turbodb_publisher_poll_terminal(void *state, const char **error) {
  turbo_flow_turbodb_publisher_t *publisher = (turbo_flow_turbodb_publisher_t *)state;
  if (error) *error = NULL;
  if (!publisher) {
    if (error) *error = "invalid TurboDb adapter";
    return CFLOW_PUBLISHER_ERROR;
  }
  if (publisher->failed) {
    if (error) *error = publisher->error;
    return CFLOW_PUBLISHER_ERROR;
  }
  return cflow_publisher_poll_terminal(&publisher->inner, error);
}

CMETA_IMPLEMENTS(cflow_publisher, turbodb_message_publisher, CFLOW_PUBLISHER_CAP_CONSTRUCTS_VALUES,
                 .name = turbodb_publisher_name, .output_type = turbodb_publisher_type,
                 .resume = turbodb_publisher_resume, .cancel = turbodb_publisher_cancel,
                 .destroy = turbodb_publisher_destroy,
                 .bind_terminal_waker = turbodb_publisher_bind_terminal_waker,
                 .poll_terminal = turbodb_publisher_poll_terminal);

static bool turbodb_type_has_lifecycle(const cmeta_type_desc *type) {
  cmeta_trait_flags flags;
  bool can_copy;
  bool can_destroy;

  if (!cmeta_type_desc_valid(type) || !type->traits || type->size == 0u ||
      !turbodb_is_power_of_two(type->align)) {
    return false;
  }
  flags = type->traits->flags;
  can_copy = (flags & CMETA_TRAIT_TRIVIAL_COPY) != 0u ||
             ((flags & CMETA_TRAIT_COPY) != 0u && type->traits->copy_construct != NULL);
  can_destroy = (flags & CMETA_TRAIT_TRIVIAL_DESTROY) != 0u ||
                ((flags & CMETA_TRAIT_DESTROY) != 0u && type->traits->destroy != NULL);
  return can_copy && can_destroy;
}

static int turbodb_source_config_validate_basic(const turbo_flow_turbodb_source_config_t *config) {
  const turbo_flow_data_schema_t *schema;

  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_TURBODB_API_VERSION || config->first_message_id == 0u) {
    return SALTS_EINVAL;
  }
  schema = config->projection_schema;
  if (!schema || schema->size < sizeof(*schema) || schema->domain != TURBO_FLOW_DOMAIN_DATA ||
      !schema->schema_name || schema->schema_name[0] == '\0' || !schema->type_name ||
      schema->type_name[0] == '\0' || !schema->projection_type ||
      schema->projection_type[0] == '\0' || schema->schema_version == 0u ||
      schema->encoding < TURBO_FLOW_DATA_ENCODING_TBE ||
      schema->encoding > TURBO_FLOW_DATA_ENCODING_OPAQUE) {
    return SALTS_EINVAL;
  }
  return SALTS_OK;
}

static int turbodb_source_config_validate(const turbo_flow_turbodb_source_config_t *config,
                                          const cmeta_type_desc *row_type) {
  int rc = turbodb_source_config_validate_basic(config);
  if (rc != SALTS_OK) return rc;
  if (!row_type || !turbodb_type_has_lifecycle(row_type) ||
      strcmp(config->projection_schema->projection_type, row_type->name) != 0) {
    return SALTS_EINVAL;
  }
  return SALTS_OK;
}

turbo_flow_turbodb_source_config_t turbo_flow_turbodb_source_config_default(void) {
  turbo_flow_turbodb_source_config_t config = {0};
  config.size = sizeof(config);
  config.version = TURBO_FLOW_TURBODB_API_VERSION;
  config.first_message_id = 1u;
  return config;
}

int turbo_flow_turbodb_publisher_wrap(cflow_publisher *typed_publisher,
                                      const turbo_flow_turbodb_source_config_t *config,
                                      cflow_publisher *message_publisher) {
  turbo_flow_turbodb_publisher_t *state;
  const cmeta_type_desc *row_type;
  int rc;

  if (!typed_publisher || !message_publisher || message_publisher->self ||
      message_publisher->vtable || !cflow_publisher_valid(typed_publisher) ||
      !cflow_publisher_has(typed_publisher, CFLOW_PUBLISHER_CAP_CONSTRUCTS_VALUES)) {
    return SALTS_EINVAL;
  }
  row_type = cflow_publisher_output_type(typed_publisher);
  rc = turbodb_source_config_validate(config, row_type);
  if (rc != SALTS_OK) return rc;

  state = (turbo_flow_turbodb_publisher_t *)calloc(1, sizeof(*state));
  if (!state) return SALTS_ENOMEM;
  state->row_type = row_type;
  state->schema = config->projection_schema;
  state->next_message_id = config->first_message_id;
  state->message_type = config->message_type;
  state->message_flags = config->message_flags;
  state->inner = *typed_publisher;
  *typed_publisher = (cflow_publisher){0};
  *message_publisher = turbodb_message_publisher_as_cflow_publisher(state);
  return SALTS_OK;
}

static int turbodb_orm_status(orm_status_t status) {
  switch (status) {
  case ORM_STATUS_OK:
    return SALTS_OK;
  case ORM_STATUS_INVALID_ARGUMENT:
    return SALTS_EINVAL;
  case ORM_STATUS_ABI_MISMATCH:
  case ORM_STATUS_TYPE_ERROR:
  case ORM_STATUS_NULL_VALUE:
  case ORM_STATUS_CONSTRAINT:
    return SALTS_EPROTO;
  case ORM_STATUS_OUT_OF_MEMORY:
    return SALTS_ENOMEM;
  case ORM_STATUS_OUT_OF_RANGE:
    return SALTS_ERANGE;
  case ORM_STATUS_LIMIT_EXCEEDED:
    return SALTS_ENOSPC;
  case ORM_STATUS_BUSY:
  case ORM_STATUS_INVALID_STATE:
    return SALTS_EBUSY;
  case ORM_STATUS_UNSUPPORTED:
    return SALTS_ENOTSUP;
  case ORM_STATUS_CONNECTION_ERROR:
  case ORM_STATUS_SQL_ERROR:
  case ORM_STATUS_INTERNAL_ERROR:
  case ORM_STATUS_DATASTORE_ERROR:
  default:
    return SALTS_EIO;
  }
}

static int turbodb_wrap_opened(orm_status_t status, cflow_publisher *typed,
                               const turbo_flow_turbodb_source_config_t *config,
                               cflow_publisher *message_publisher) {
  int rc;
  if (status != ORM_STATUS_OK) {
    if (cflow_publisher_valid(typed)) cflow_publisher_destroy(typed);
    return turbodb_orm_status(status);
  }
  rc = turbo_flow_turbodb_publisher_wrap(typed, config, message_publisher);
  if (rc != SALTS_OK && cflow_publisher_valid(typed)) {
    cflow_publisher_destroy(typed);
  }
  return rc;
}

int turbo_flow_turbodb_query_open(orm_query_t *query, const orm_flow_config_t *orm_config,
                                  const turbo_flow_turbodb_source_config_t *source_config,
                                  cflow_publisher *message_publisher, orm_error_t *orm_error) {
  cflow_publisher typed = {0};
  orm_status_t status;
  int rc;
  if (!query || !orm_config || !message_publisher || message_publisher->self ||
      message_publisher->vtable || !orm_error) {
    return SALTS_EINVAL;
  }
  if (orm_config->struct_size < sizeof(*orm_config) ||
      orm_config->abi_version != ORM_C_ABI_VERSION || !orm_config->row_shape ||
      !orm_config->row_shape->storage_type) {
    return SALTS_EINVAL;
  }
  rc = turbodb_source_config_validate(source_config, orm_config->row_shape->storage_type);
  if (rc != SALTS_OK) return rc;
  status = orm_query_open_flow(query, orm_config, &typed, orm_error);
  return turbodb_wrap_opened(status, &typed, source_config, message_publisher);
}

int turbo_flow_turbodb_query_open_in_transaction(
    orm_query_t *query, orm_transaction_t *transaction, const orm_flow_config_t *orm_config,
    const turbo_flow_turbodb_source_config_t *source_config, cflow_publisher *message_publisher,
    orm_error_t *orm_error) {
  cflow_publisher typed = {0};
  orm_status_t status;
  int rc;
  if (!query || !transaction || !orm_config || !message_publisher || message_publisher->self ||
      message_publisher->vtable || !orm_error) {
    return SALTS_EINVAL;
  }
  if (orm_config->struct_size < sizeof(*orm_config) ||
      orm_config->abi_version != ORM_C_ABI_VERSION || !orm_config->row_shape ||
      !orm_config->row_shape->storage_type) {
    return SALTS_EINVAL;
  }
  rc = turbodb_source_config_validate(source_config, orm_config->row_shape->storage_type);
  if (rc != SALTS_OK) return rc;
  status = orm_query_open_flow_in_transaction(query, transaction, orm_config, &typed, orm_error);
  return turbodb_wrap_opened(status, &typed, source_config, message_publisher);
}

int turbo_flow_turbodb_command_open(orm_query_t *query,
                                    const turbo_flow_turbodb_source_config_t *source_config,
                                    cflow_publisher *message_publisher, orm_error_t *orm_error) {
  cflow_publisher typed = {0};
  orm_status_t status;
  if (!query || !message_publisher || message_publisher->self || message_publisher->vtable ||
      !orm_error) {
    return SALTS_EINVAL;
  }
  if (turbodb_source_config_validate_basic(source_config) != SALTS_OK) {
    return SALTS_EINVAL;
  }
  status = orm_query_open_command_flow(query, &typed, orm_error);
  return turbodb_wrap_opened(status, &typed, source_config, message_publisher);
}

int turbo_flow_turbodb_command_open_in_transaction(
    orm_query_t *query, orm_transaction_t *transaction,
    const turbo_flow_turbodb_source_config_t *source_config, cflow_publisher *message_publisher,
    orm_error_t *orm_error) {
  cflow_publisher typed = {0};
  orm_status_t status;
  if (!query || !transaction || !message_publisher || message_publisher->self ||
      message_publisher->vtable || !orm_error) {
    return SALTS_EINVAL;
  }
  if (turbodb_source_config_validate_basic(source_config) != SALTS_OK) {
    return SALTS_EINVAL;
  }
  status = orm_query_open_command_flow_in_transaction(query, transaction, &typed, orm_error);
  return turbodb_wrap_opened(status, &typed, source_config, message_publisher);
}
