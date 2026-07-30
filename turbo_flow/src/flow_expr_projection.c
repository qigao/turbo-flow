#include "flow_internal.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int flow_expr_projection_schema_valid(const turbo_flow_data_schema_t *schema) {
  return schema && schema->size >= sizeof(*schema) && schema->domain != TURBO_FLOW_DOMAIN_NONE &&
         schema->encoding >= TURBO_FLOW_DATA_ENCODING_TBE &&
         schema->encoding <= TURBO_FLOW_DATA_ENCODING_OPAQUE && schema->schema_name &&
         schema->schema_name[0] != '\0' && schema->type_name && schema->type_name[0] != '\0' &&
         schema->projection_type && schema->projection_type[0] != '\0' &&
         schema->schema_version != 0u;
}

static int flow_expr_projection_schema_equal(
    const turbo_flow_data_schema_t *left, const turbo_flow_data_schema_t *right) {
  return left && right && left->domain == right->domain && left->encoding == right->encoding &&
         left->schema_id == right->schema_id && left->schema_version == right->schema_version &&
         strcmp(left->schema_name, right->schema_name) == 0 &&
         strcmp(left->type_name, right->type_name) == 0 &&
         strcmp(left->projection_type, right->projection_type) == 0;
}

static void flow_expr_projection_registration_destroy(
    flow_expr_projection_registration_t *registration) {
  size_t i;
  if (!registration) return;
  for (i = 0; i < registration->field_count; ++i) {
    tstr_freep(&registration->fields[i].path);
  }
  free(registration->fields);
  tstr_freep(&registration->schema_name);
  tstr_freep(&registration->type_name);
  tstr_freep(&registration->projection_type);
  memset(registration, 0, sizeof(*registration));
}

void flow_expr_projection_clear(turbo_flow_t *flow) {
  size_t i;
  if (!flow) return;
  for (i = 0; i < turbo_vec_size(&flow->expr_projection_registrations); ++i) {
    flow_expr_projection_registration_t *registration =
        (flow_expr_projection_registration_t *)turbo_vec_at(
            &flow->expr_projection_registrations, i);
    flow_expr_projection_registration_destroy(registration);
  }
  turbo_vec_clear(&flow->expr_projection_registrations);
}

static int flow_expr_projection_fields_compatible(
    const turbo_flow_t *flow, const turbo_flow_expr_schema_t *schema) {
  size_t registration_index;
  size_t field_index;
  for (field_index = 0; field_index < schema->field_count; ++field_index) {
    const turbo_flow_expr_schema_field_t *candidate = &schema->fields[field_index];
    for (registration_index = 0;
         registration_index < turbo_vec_size(&flow->expr_projection_registrations);
         ++registration_index) {
      const flow_expr_projection_registration_t *registration =
          (const flow_expr_projection_registration_t *)turbo_vec_at_const(
              &flow->expr_projection_registrations, registration_index);
      size_t existing_index;
      for (existing_index = 0; existing_index < registration->field_count; ++existing_index) {
        const flow_expr_projection_field_t *existing = &registration->fields[existing_index];
        const int same_path = strcmp(existing->path, candidate->path) == 0;
        const int same_id = existing->field_id == candidate->field_id;
        if ((same_path && (!same_id || existing->type != candidate->type)) ||
            (same_id && !same_path)) {
          return 0;
        }
      }
    }
  }
  return 1;
}

static int flow_expr_projection_copy_registration(
    flow_expr_projection_registration_t *out,
    const turbo_flow_expr_projection_registration_t *source) {
  size_t i;
  memset(out, 0, sizeof(*out));
  out->schema_name = tstr_dup(source->projection_schema->schema_name);
  out->type_name = tstr_dup(source->projection_schema->type_name);
  out->projection_type = tstr_dup(source->projection_schema->projection_type);
  if (!out->schema_name || !out->type_name || !out->projection_type) goto nomem;
  out->fields =
      (flow_expr_projection_field_t *)calloc(source->expr_schema->field_count, sizeof(*out->fields));
  if (!out->fields) goto nomem;
  out->field_count = source->expr_schema->field_count;
  for (i = 0; i < out->field_count; ++i) {
    out->fields[i].path = tstr_dup(source->expr_schema->fields[i].path);
    if (!out->fields[i].path) goto nomem;
    out->fields[i].type = source->expr_schema->fields[i].type;
    out->fields[i].field_id = source->expr_schema->fields[i].field_id;
  }
  out->schema = *source->projection_schema;
  out->schema.size = sizeof(out->schema);
  out->schema.schema_name = out->schema_name;
  out->schema.type_name = out->type_name;
  out->schema.projection_type = out->projection_type;
  out->schema.schema_text = NULL;
  out->read_field = source->read_field;
  out->ctx = source->ctx;
  return TURBO_OK;

nomem:
  flow_expr_projection_registration_destroy(out);
  return TURBO_ENOMEM;
}

int turbo_flow_register_expr_projection(
    turbo_flow_t *flow, const turbo_flow_expr_projection_registration_t *registration) {
  flow_expr_projection_registration_t copy;
  turbo_flow_expr_t *validation = NULL;
  turbo_flow_error_t error;
  size_t i;
  int rc;
  if (!flow || !registration || registration->size < sizeof(*registration) ||
      !flow_expr_projection_schema_valid(registration->projection_schema) ||
      !registration->expr_schema || !registration->expr_schema->fields ||
      registration->expr_schema->field_count == 0u || !registration->read_field) {
    return TURBO_EINVAL;
  }
  if (flow->state == TURBO_FLOW_STATE_COMPILED || flow->state == TURBO_FLOW_STATE_STARTED) {
    return flow_set_error_keep_state(flow, TURBO_EBUSY, 0u, 0u,
                                     "cannot register expression projection after compile");
  }
  for (i = 0; i < turbo_vec_size(&flow->expr_projection_registrations); ++i) {
    const flow_expr_projection_registration_t *existing =
        (const flow_expr_projection_registration_t *)turbo_vec_at_const(
            &flow->expr_projection_registrations, i);
    if (flow_expr_projection_schema_equal(&existing->schema, registration->projection_schema)) {
      return flow_set_error_keep_state(flow, TURBO_EALREADY, 0u, 0u,
                                       "duplicate expression projection schema");
    }
  }
  memset(&error, 0, sizeof(error));
  rc = turbo_flow_expr_compile("true", 4u, registration->expr_schema, &validation, &error);
  turbo_flow_expr_destroy(validation);
  if (rc != TURBO_OK) {
    return flow_set_error_keep_state(flow, rc, 0u, 0u,
                                     error.message[0] ? error.message
                                                      : "invalid expression projection schema");
  }
  if (!flow_expr_projection_fields_compatible(flow, registration->expr_schema)) {
    return flow_set_error_keep_state(flow, TURBO_EPROTO, 0u, 0u,
                                     "conflicting expression projection field");
  }
  rc = flow_expr_projection_copy_registration(&copy, registration);
  if (rc != TURBO_OK) return flow_set_error(flow, rc, 0u, 0u, "out of memory");
  if (turbo_vec_push(&flow->expr_projection_registrations, &copy) != TURBO_OK) {
    flow_expr_projection_registration_destroy(&copy);
    return flow_set_error(flow, TURBO_ENOMEM, 0u, 0u, "out of memory");
  }
  return TURBO_OK;
}

static int flow_expr_projection_schema_fields(
    const turbo_flow_t *flow, turbo_flow_expr_schema_field_t **fields_out, size_t *count_out) {
  turbo_flow_expr_schema_field_t *fields;
  size_t capacity = 0u;
  size_t count = 0u;
  size_t registration_index;
  if (!flow || !fields_out || !count_out) return TURBO_EINVAL;
  *fields_out = NULL;
  *count_out = 0u;
  for (registration_index = 0;
       registration_index < turbo_vec_size(&flow->expr_projection_registrations);
       ++registration_index) {
    const flow_expr_projection_registration_t *registration =
        (const flow_expr_projection_registration_t *)turbo_vec_at_const(
            &flow->expr_projection_registrations, registration_index);
    if (registration->field_count > SIZE_MAX - capacity) return TURBO_ERANGE;
    capacity += registration->field_count;
  }
  if (capacity == 0u) return TURBO_OK;
  fields = (turbo_flow_expr_schema_field_t *)calloc(capacity, sizeof(*fields));
  if (!fields) return TURBO_ENOMEM;
  for (registration_index = 0;
       registration_index < turbo_vec_size(&flow->expr_projection_registrations);
       ++registration_index) {
    const flow_expr_projection_registration_t *registration =
        (const flow_expr_projection_registration_t *)turbo_vec_at_const(
            &flow->expr_projection_registrations, registration_index);
    size_t field_index;
    for (field_index = 0; field_index < registration->field_count; ++field_index) {
      const flow_expr_projection_field_t *candidate = &registration->fields[field_index];
      size_t existing_index;
      for (existing_index = 0; existing_index < count; ++existing_index) {
        if (fields[existing_index].field_id == candidate->field_id) break;
      }
      if (existing_index < count) continue;
      fields[count].path = candidate->path;
      fields[count].type = candidate->type;
      fields[count].field_id = candidate->field_id;
      ++count;
    }
  }
  *fields_out = fields;
  *count_out = count;
  return TURBO_OK;
}

int flow_expr_projection_compile(turbo_flow_t *flow, const char *text, size_t len,
                                 turbo_flow_expr_t **out, turbo_flow_error_t *error) {
  turbo_flow_expr_schema_field_t *fields = NULL;
  turbo_flow_expr_schema_t schema;
  size_t field_count = 0u;
  int rc;
  if (!flow) return TURBO_EINVAL;
  rc = flow_expr_projection_schema_fields(flow, &fields, &field_count);
  if (rc != TURBO_OK) {
    if (error) {
      memset(error, 0, sizeof(*error));
      error->code = rc;
      snprintf(error->message, sizeof(error->message), "%s",
               rc == TURBO_ENOMEM ? "out of memory" : "expression projection schema is too large");
    }
    if (out) *out = NULL;
    return rc;
  }
  schema.fields = fields;
  schema.field_count = field_count;
  rc = turbo_flow_expr_compile(text, len, field_count > 0u ? &schema : NULL, out, error);
  free(fields);
  return rc;
}

static int flow_expr_projection_read(void *ctx, uint32_t field_id,
                                     turbo_flow_expr_value_t *out) {
  flow_expr_projection_eval_binding_t *binding = (flow_expr_projection_eval_binding_t *)ctx;
  if (!binding || !binding->registration || !binding->projection || !out) return TURBO_EINVAL;
  return binding->registration->read_field(binding->projection, field_id, out,
                                           binding->registration->ctx);
}

void flow_expr_projection_bind_eval(
    const turbo_flow_t *flow, const turbo_flow_msg_t *msg,
    flow_expr_projection_eval_binding_t *binding, turbo_flow_expr_eval_context_t *context) {
  const turbo_flow_data_schema_t *schema = NULL;
  const void *projection;
  size_t i;
  if (!binding || !context) return;
  memset(binding, 0, sizeof(*binding));
  if (!flow || !msg) return;
  projection = turbo_flow_msg_projection(msg, &schema);
  if (!projection || !schema) return;
  for (i = 0; i < turbo_vec_size(&flow->expr_projection_registrations); ++i) {
    const flow_expr_projection_registration_t *registration =
        (const flow_expr_projection_registration_t *)turbo_vec_at_const(
            &flow->expr_projection_registrations, i);
    if (!flow_expr_projection_schema_equal(&registration->schema, schema)) continue;
    binding->registration = registration;
    binding->projection = projection;
    context->read_schema_field = flow_expr_projection_read;
    context->schema_ctx = binding;
    return;
  }
}
