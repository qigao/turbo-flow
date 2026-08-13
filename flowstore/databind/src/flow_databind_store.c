#include "turbo_flow_databind_store.h"

#include <stdlib.h>
#include <string.h>

enum {
  FLOW_REPOSITORY_VALUE_INVALID,
  FLOW_REPOSITORY_VALUE_BOOL,
  FLOW_REPOSITORY_VALUE_INT64,
  FLOW_REPOSITORY_VALUE_UINT64,
  FLOW_REPOSITORY_VALUE_DOUBLE,
  FLOW_REPOSITORY_VALUE_STRING,
  FLOW_REPOSITORY_VALUE_BYTES
};

/* DataBind accepts a non-null buffer even when an empty binary message is valid. */
static const uint8_t flow_binding_empty_binary[1] = {0u};

struct turbo_flow_databind_binding_s {
  DataBind *codec;
  char *type_name;
  qvm_limits_t query_limits;
};

typedef struct flow_binding_scan_context_s {
  turbo_flow_databind_binding_t *binding;
  turbo_flow_databind_visit_fn visit;
  void *visit_ctx;
} flow_binding_scan_context_t;

typedef struct flow_binding_get_context_s {
  turbo_flow_databind_binding_t *binding;
  const uint8_t *key;
  size_t key_size;
  DataBindRecord **out;
  uint64_t *revision;
  int found;
} flow_binding_get_context_t;

typedef struct flow_binding_query_context_s {
  flow_binding_scan_context_t scan;
  const turbo_flow_databind_query_t *query;
  size_t matched;
  const DataBindValue *value;
} flow_binding_query_context_t;

static int flow_binding_data_bind_status(DataBindStatus status) {
  if (status == DATA_BIND_ERR_OOM) return TURBO_ENOMEM;
  if (status == DATA_BIND_ERR_LIMIT) return TURBO_EFBIG;
  return TURBO_EPROTO;
}

static int flow_binding_copy_string(const char *source, char **out) {
  size_t size;
  char *copy;
  if (!source || !out) return TURBO_EINVAL;
  *out = NULL;
  size = strlen(source);
  if (size == SIZE_MAX) return TURBO_ERANGE;
  copy = (char *)malloc(size + 1u);
  if (!copy) return TURBO_ENOMEM;
  memcpy(copy, source, size + 1u);
  *out = copy;
  return TURBO_OK;
}

static int flow_binding_record_decode(turbo_flow_databind_binding_t *binding,
                                         const turbo_flow_record_view_t *source,
                                         DataBindRecord **out) {
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindStatus status;
  if (!binding || !source || !out || (source->value_size != 0u && !source->value)) {
    return TURBO_EINVAL;
  }
  *out = NULL;
  status = data_bind_record_from_bin(binding->codec, binding->type_name,
                                     source->value_size == 0u ? flow_binding_empty_binary
                                                              : source->value,
                                     source->value_size, out, &error);
  return status == DATA_BIND_OK ? TURBO_OK : flow_binding_data_bind_status(status);
}

static int flow_binding_scan_record(void *ctx, const turbo_flow_record_view_t *source) {
  flow_binding_scan_context_t *scan = (flow_binding_scan_context_t *)ctx;
  turbo_flow_databind_view_t view = TURBO_FLOW_DATABIND_VIEW_INIT;
  DataBindRecord *record = NULL;
  int rc;
  if (!scan || !scan->binding || !scan->visit || !source || !source->key ||
      source->key_size == 0u)
    return TURBO_EINVAL;
  rc = flow_binding_record_decode(scan->binding, source, &record);
  if (rc != TURBO_OK) return rc;
  view.key = source->key;
  view.key_size = source->key_size;
  view.revision = source->revision;
  view.record = record;
  rc = scan->visit(scan->visit_ctx, &view);
  data_bind_record_free(record);
  return rc;
}

static int flow_binding_get_record(void *ctx, const turbo_flow_record_view_t *source) {
  flow_binding_get_context_t *get = (flow_binding_get_context_t *)ctx;
  int rc;
  if (!get || !source || !source->key || source->key_size != get->key_size ||
      memcmp(source->key, get->key, get->key_size) != 0)
    return get ? TURBO_OK : TURBO_EINVAL;
  if (get->found) return TURBO_EPROTO;
  rc = flow_binding_record_decode(get->binding, source, get->out);
  if (rc != TURBO_OK) return rc;
  *get->revision = source->revision;
  get->found = 1;
  return TURBO_OK;
}

static void flow_binding_make_invalid(void *ctx, qvm_value_t *out) {
  (void)ctx;
  memset(out, 0, sizeof(*out));
  out->type = FLOW_REPOSITORY_VALUE_INVALID;
}

static void flow_binding_make_bool(void *ctx, int value, qvm_value_t *out) {
  (void)ctx;
  memset(out, 0, sizeof(*out));
  out->type = FLOW_REPOSITORY_VALUE_BOOL;
  out->boolean = value != 0;
}

static void flow_binding_make_number(void *ctx, double value, qvm_value_t *out) {
  (void)ctx;
  memset(out, 0, sizeof(*out));
  out->type = FLOW_REPOSITORY_VALUE_DOUBLE;
  out->number = value;
}

static void flow_binding_make_string(void *ctx, const char *value, size_t length,
                                        qvm_value_t *out) {
  (void)ctx;
  memset(out, 0, sizeof(*out));
  out->type = FLOW_REPOSITORY_VALUE_STRING;
  out->str = value;
  out->length = length;
}

static int flow_binding_value_from_data_bind(const DataBindValue *value, qvm_value_t *out) {
  DataBindStatus status;
  if (!value || !out) return 0;
  memset(out, 0, sizeof(*out));
  switch (data_bind_value_kind(value)) {
  case DATA_BIND_VALUE_BOOL:
    out->type = FLOW_REPOSITORY_VALUE_BOOL;
    status = data_bind_value_get_bool(value, &out->boolean);
    return status == DATA_BIND_OK;
  case DATA_BIND_VALUE_INT:
  case DATA_BIND_VALUE_INT64:
    out->type = FLOW_REPOSITORY_VALUE_INT64;
    status = data_bind_value_get_int64(value, &out->integer);
    return status == DATA_BIND_OK;
  case DATA_BIND_VALUE_UINT64:
    out->type = FLOW_REPOSITORY_VALUE_UINT64;
    status = data_bind_value_get_uint64(value, &out->uinteger);
    return status == DATA_BIND_OK;
  case DATA_BIND_VALUE_DOUBLE:
    out->type = FLOW_REPOSITORY_VALUE_DOUBLE;
    status = data_bind_value_get_double(value, &out->number);
    return status == DATA_BIND_OK;
  case DATA_BIND_VALUE_STRING:
    out->type = FLOW_REPOSITORY_VALUE_STRING;
    status = data_bind_value_get_string(value, &out->str, &out->length);
    return status == DATA_BIND_OK;
  case DATA_BIND_VALUE_BYTES:
    out->type = FLOW_REPOSITORY_VALUE_BYTES;
    status = data_bind_value_get_bytes(value, (const uint8_t **)&out->str, &out->length);
    return status == DATA_BIND_OK;
  default: return 0;
  }
}

static int flow_binding_resolve(void *ctx, uint32_t operand_index, qvm_value_t *out) {
  flow_binding_query_context_t *query = (flow_binding_query_context_t *)ctx;
  const turbo_flow_databind_operand_t *operand;
  const DataBindValue *value;
  if (!query || !out || operand_index >= query->query->operand_count) return 0;
  operand = &query->query->operands[operand_index];
  if (operand->size < sizeof(*operand)) return 0;
  switch (operand->kind) {
  case TURBO_FLOW_DATABIND_OPERAND_FIELD:
    if (!operand->value.field_name || !operand->value.field_name[0]) return 0;
    value = data_bind_value_get(query->value, operand->value.field_name);
    if (!value) {
      flow_binding_make_invalid(ctx, out);
      return 1;
    }
    if (!flow_binding_value_from_data_bind(value, out)) {
      flow_binding_make_invalid(ctx, out);
    }
    return 1;
  case TURBO_FLOW_DATABIND_OPERAND_BOOL:
    flow_binding_make_bool(ctx, operand->value.boolean, out);
    return 1;
  case TURBO_FLOW_DATABIND_OPERAND_INT64:
    memset(out, 0, sizeof(*out));
    out->type = FLOW_REPOSITORY_VALUE_INT64;
    out->integer = operand->value.integer;
    return 1;
  case TURBO_FLOW_DATABIND_OPERAND_UINT64:
    memset(out, 0, sizeof(*out));
    out->type = FLOW_REPOSITORY_VALUE_UINT64;
    out->uinteger = operand->value.uinteger;
    return 1;
  case TURBO_FLOW_DATABIND_OPERAND_DOUBLE:
    flow_binding_make_number(ctx, operand->value.number, out);
    return 1;
  case TURBO_FLOW_DATABIND_OPERAND_STRING:
    if (!operand->value.string.data && operand->value.string.size != 0u) return 0;
    flow_binding_make_string(ctx, operand->value.string.data, operand->value.string.size, out);
    return 1;
  default: return 0;
  }
}

static int flow_binding_truthy(void *ctx, const qvm_value_t *value) {
  (void)ctx;
  if (!value) return 0;
  switch (value->type) {
  case FLOW_REPOSITORY_VALUE_BOOL: return value->boolean != 0;
  case FLOW_REPOSITORY_VALUE_INT64: return value->integer != 0;
  case FLOW_REPOSITORY_VALUE_UINT64: return value->uinteger != 0u;
  case FLOW_REPOSITORY_VALUE_DOUBLE: return value->number != 0.0;
  case FLOW_REPOSITORY_VALUE_STRING:
  case FLOW_REPOSITORY_VALUE_BYTES: return value->length != 0u;
  default: return 0;
  }
}

static int flow_binding_integer_compare(const qvm_value_t *left, const qvm_value_t *right,
                                           int *comparison) {
  if (left->type == FLOW_REPOSITORY_VALUE_INT64 && right->type == FLOW_REPOSITORY_VALUE_INT64) {
    *comparison = left->integer < right->integer ? -1 : left->integer > right->integer;
    return 1;
  }
  if (left->type == FLOW_REPOSITORY_VALUE_UINT64 && right->type == FLOW_REPOSITORY_VALUE_UINT64) {
    *comparison = left->uinteger < right->uinteger ? -1 : left->uinteger > right->uinteger;
    return 1;
  }
  if (left->type == FLOW_REPOSITORY_VALUE_INT64 && right->type == FLOW_REPOSITORY_VALUE_UINT64) {
    if (left->integer < 0) *comparison = -1;
    else *comparison = (uint64_t)left->integer < right->uinteger
                            ? -1
                            : (uint64_t)left->integer > right->uinteger;
    return 1;
  }
  if (left->type == FLOW_REPOSITORY_VALUE_UINT64 && right->type == FLOW_REPOSITORY_VALUE_INT64) {
    if (right->integer < 0) *comparison = 1;
    else *comparison = left->uinteger < (uint64_t)right->integer
                            ? -1
                            : left->uinteger > (uint64_t)right->integer;
    return 1;
  }
  return 0;
}

static int flow_binding_compare(const qvm_value_t *left, const qvm_value_t *right,
                                   int *comparison) {
  size_t common;
  int result;
  if (!left || !right || !comparison) return 0;
  if (flow_binding_integer_compare(left, right, comparison)) return 1;
  if (left->type == FLOW_REPOSITORY_VALUE_DOUBLE && right->type == FLOW_REPOSITORY_VALUE_DOUBLE) {
    *comparison = left->number < right->number ? -1 : left->number > right->number;
    return 1;
  }
  if (left->type == FLOW_REPOSITORY_VALUE_BOOL && right->type == FLOW_REPOSITORY_VALUE_BOOL) {
    *comparison = left->boolean < right->boolean ? -1 : left->boolean > right->boolean;
    return 1;
  }
  if (left->type != right->type ||
      (left->type != FLOW_REPOSITORY_VALUE_STRING && left->type != FLOW_REPOSITORY_VALUE_BYTES))
    return 0;
  common = left->length < right->length ? left->length : right->length;
  result = common == 0u ? 0 : memcmp(left->str, right->str, common);
  if (result < 0) *comparison = -1;
  else if (result > 0) *comparison = 1;
  else *comparison = left->length < right->length ? -1 : left->length > right->length;
  return 1;
}

static int flow_binding_binary(void *ctx, qvm_opcode_t op, uint32_t arg,
                                  const qvm_value_t *left, const qvm_value_t *right,
                                  qvm_value_t *out) {
  int comparison;
  int value;
  if (op != QVM_OP_CMP || !flow_binding_compare(left, right, &comparison)) return 0;
  switch (arg) {
  case TURBO_FLOW_DATABIND_COMPARE_EQ: value = comparison == 0; break;
  case TURBO_FLOW_DATABIND_COMPARE_NE: value = comparison != 0; break;
  case TURBO_FLOW_DATABIND_COMPARE_LT: value = comparison < 0; break;
  case TURBO_FLOW_DATABIND_COMPARE_LE: value = comparison <= 0; break;
  case TURBO_FLOW_DATABIND_COMPARE_GT: value = comparison > 0; break;
  case TURBO_FLOW_DATABIND_COMPARE_GE: value = comparison >= 0; break;
  default: return 0;
  }
  flow_binding_make_bool(ctx, value, out);
  return 1;
}

static int flow_binding_exists(void *ctx, uint32_t operand_index, int *out) {
  qvm_value_t value;
  if (!out || !flow_binding_resolve(ctx, operand_index, &value)) return 0;
  *out = value.type != FLOW_REPOSITORY_VALUE_INVALID;
  return 1;
}

static int flow_binding_length(void *ctx, uint32_t operand_index, qvm_value_t *out) {
  qvm_value_t value;
  if (!out || !flow_binding_resolve(ctx, operand_index, &value) ||
      (value.type != FLOW_REPOSITORY_VALUE_STRING && value.type != FLOW_REPOSITORY_VALUE_BYTES))
    return 0;
  flow_binding_make_number(ctx, (double)value.length, out);
  return 1;
}

static int flow_binding_count(void *ctx, uint32_t operand_index, qvm_value_t *out) {
  flow_binding_query_context_t *query = (flow_binding_query_context_t *)ctx;
  const turbo_flow_databind_operand_t *operand;
  const DataBindValue *value;
  if (!query || !out || operand_index >= query->query->operand_count) return 0;
  operand = &query->query->operands[operand_index];
  if (operand->size < sizeof(*operand) ||
      operand->kind != TURBO_FLOW_DATABIND_OPERAND_FIELD ||
      !operand->value.field_name)
    return 0;
  value = data_bind_value_get(query->value, operand->value.field_name);
  if (!value) return 0;
  switch (data_bind_value_kind(value)) {
  case DATA_BIND_VALUE_LIST:
  case DATA_BIND_VALUE_SET:
  case DATA_BIND_VALUE_MAP: flow_binding_make_number(ctx, (double)data_bind_value_count(value), out); return 1;
  default: return 0;
  }
}

static qvm_exec_ops_t flow_binding_query_ops(void) {
  qvm_exec_ops_t ops;
  memset(&ops, 0, sizeof(ops));
  ops.resolve = flow_binding_resolve;
  ops.truthy = flow_binding_truthy;
  ops.binary = flow_binding_binary;
  ops.exists = flow_binding_exists;
  ops.length = flow_binding_length;
  ops.count = flow_binding_count;
  ops.make_invalid = flow_binding_make_invalid;
  ops.make_bool = flow_binding_make_bool;
  ops.make_number = flow_binding_make_number;
  ops.make_string = flow_binding_make_string;
  return ops;
}

static int flow_binding_query_opcode_supported(qvm_opcode_t opcode) {
  switch (opcode) {
  case QVM_OP_LOAD_PATH:
  case QVM_OP_LOAD_CONST:
  case QVM_OP_LOAD_INVALID:
  case QVM_OP_EXISTS:
  case QVM_OP_NOT_EXISTS:
  case QVM_OP_LENGTH:
  case QVM_OP_COUNT:
  case QVM_OP_CMP:
  case QVM_OP_NOT:
  case QVM_OP_JMP_FALSE:
  case QVM_OP_JMP_TRUE:
  case QVM_OP_JMP:
  case QVM_OP_TRUE:
  case QVM_OP_FALSE:
  case QVM_OP_SELECT: return 1;
  default: return 0;
  }
}

static int flow_binding_query_validate_operands(
    const turbo_flow_databind_query_t *query) {
  for (uint32_t i = 0u; i < query->operand_count; ++i) {
    const turbo_flow_databind_operand_t *operand = &query->operands[i];
    if (operand->size < sizeof(*operand)) return TURBO_EINVAL;
    switch (operand->kind) {
    case TURBO_FLOW_DATABIND_OPERAND_FIELD:
      if (!operand->value.field_name || !operand->value.field_name[0]) return TURBO_EINVAL;
      break;
    case TURBO_FLOW_DATABIND_OPERAND_BOOL:
    case TURBO_FLOW_DATABIND_OPERAND_INT64:
    case TURBO_FLOW_DATABIND_OPERAND_UINT64:
    case TURBO_FLOW_DATABIND_OPERAND_DOUBLE: break;
    case TURBO_FLOW_DATABIND_OPERAND_STRING:
      if (!operand->value.string.data && operand->value.string.size != 0u) return TURBO_EINVAL;
      break;
    default: return TURBO_EINVAL;
    }
  }
  return TURBO_OK;
}

static int flow_binding_query_validate_opcodes(
    const turbo_flow_databind_query_t *query) {
  uint32_t end = query->offset + query->length;
  for (uint32_t pc = query->offset; pc < end; ++pc) {
    if (!flow_binding_query_opcode_supported((qvm_opcode_t)query->instructions[pc].op))
      return TURBO_ENOTSUP;
  }
  return TURBO_OK;
}

static int flow_binding_query_record(void *ctx, const turbo_flow_record_view_t *source) {
  flow_binding_query_context_t *query = (flow_binding_query_context_t *)ctx;
  turbo_flow_databind_view_t view = TURBO_FLOW_DATABIND_VIEW_INIT;
  qvm_exec_ops_t ops = flow_binding_query_ops();
  qvm_value_t result;
  DataBindRecord *record = NULL;
  qvm_diagnostic_t diagnostic;
  int status;
  int rc;
  if (!query || !source) return TURBO_EINVAL;
  rc = flow_binding_record_decode(query->scan.binding, source, &record);
  if (rc != TURBO_OK) return rc;
  query->value = data_bind_object_value(record);
  status = qvm_execute_ex(query->query->instructions, query->query->instruction_count,
                          query->query->offset, query->query->length, &ops, query, NULL, &result,
                          &query->scan.binding->query_limits, &diagnostic);
  if (status != QVM_STATUS_OK) {
    data_bind_record_free(record);
    return status == QVM_STATUS_RESOURCE_LIMIT ? TURBO_EFBIG : TURBO_EPROTO;
  }
  if (flow_binding_truthy(query, &result)) {
    if (query->matched == SIZE_MAX) {
      data_bind_record_free(record);
      return TURBO_ERANGE;
    }
    view.key = source->key;
    view.key_size = source->key_size;
    view.revision = source->revision;
    view.record = record;
    ++query->matched;
    rc = query->scan.visit(query->scan.visit_ctx, &view);
  }
  data_bind_record_free(record);
  return rc;
}

int turbo_flow_databind_binding_create(const turbo_flow_databind_binding_config_t *config,
                                        turbo_flow_databind_binding_t **out) {
  turbo_flow_databind_binding_t *binding;
  DataBindSchemaType schema_type = DATA_BIND_SCHEMA_TYPE_INIT;
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindStatus status;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  if (!config || config->size < sizeof(*config) ||
      config->abi_version != TURBO_FLOW_DATABIND_BINDING_ABI_VERSION || !config->schema_text ||
      config->schema_size == 0u || !config->type_name || !config->type_name[0] ||
      config->query_limits.max_instructions == 0u || config->query_limits.max_operands == 0u ||
      config->query_limits.max_regexes == 0u || config->query_limits.max_steps == 0u)
    return TURBO_EINVAL;
  binding = (turbo_flow_databind_binding_t *)calloc(1u, sizeof(*binding));
  if (!binding) return TURBO_ENOMEM;
  status = data_bind_create_from_text(config->schema_text, config->schema_size, &binding->codec,
                                      &error);
  if (status != DATA_BIND_OK) {
    rc = flow_binding_data_bind_status(status);
    free(binding);
    return rc;
  }
  if (!data_bind_schema_find_type(binding->codec, config->type_name, &schema_type)) {
    data_bind_free(binding->codec);
    free(binding);
    return TURBO_EINVAL;
  }
  rc = flow_binding_copy_string(config->type_name, &binding->type_name);
  if (rc != TURBO_OK) {
    data_bind_free(binding->codec);
    free(binding);
    return rc;
  }
  binding->query_limits = config->query_limits;
  *out = binding;
  return TURBO_OK;
}

void turbo_flow_databind_binding_destroy(turbo_flow_databind_binding_t *binding) {
  if (!binding) return;
  data_bind_free(binding->codec);
  free(binding->type_name);
  free(binding);
}

int turbo_flow_databind_put(turbo_flow_store_t *store, turbo_flow_databind_binding_t *binding,
                            const uint8_t *key, size_t key_size, uint64_t expected_revision,
                            uint64_t next_revision, const DataBindRecord *record) {
  DataBindError error = DATA_BIND_ERROR_INIT;
  uint8_t *value = NULL;
  size_t value_size = 0u;
  DataBindStatus status;
  int rc;
  if (!store || !binding || !key || key_size == 0u ||
      !record || !data_bind_record_type_name(record) ||
      strcmp(data_bind_record_type_name(record), binding->type_name) != 0)
    return TURBO_EINVAL;
  status = data_bind_record_serialize_bin(binding->codec, record, &value, &value_size, &error);
  if (status != DATA_BIND_OK) return flow_binding_data_bind_status(status);
  if (value_size != 0u && !value) {
    data_bind_binary_free(value);
    return TURBO_EPROTO;
  }
  rc = turbo_flow_store_put(store, key, key_size, expected_revision, next_revision, value,
                            value_size);
  data_bind_binary_free(value);
  return rc;
}

int turbo_flow_databind_get(turbo_flow_store_t *store, turbo_flow_databind_binding_t *binding,
                            const uint8_t *key, size_t key_size, DataBindRecord **out,
                            uint64_t *revision) {
  flow_binding_get_context_t context;
  int rc;
  if (!store || !binding || !key || key_size == 0u || !out || !revision)
    return TURBO_EINVAL;
  *out = NULL;
  *revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
  memset(&context, 0, sizeof(context));
  context.binding = binding;
  context.key = key;
  context.key_size = key_size;
  context.out = out;
  context.revision = revision;
  rc = turbo_flow_store_scan(store, flow_binding_get_record, &context);
  if (rc != TURBO_OK) {
    data_bind_record_free(*out);
    *out = NULL;
    *revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
    return rc;
  }
  return context.found ? TURBO_OK : TURBO_ENOENT;
}

int turbo_flow_databind_scan(turbo_flow_store_t *store, turbo_flow_databind_binding_t *binding,
                             turbo_flow_databind_visit_fn visit, void *ctx) {
  flow_binding_scan_context_t scan;
  if (!store || !binding || !visit) return TURBO_EINVAL;
  scan.binding = binding;
  scan.visit = visit;
  scan.visit_ctx = ctx;
  return turbo_flow_store_scan(store, flow_binding_scan_record, &scan);
}

int turbo_flow_databind_query(turbo_flow_store_t *store, turbo_flow_databind_binding_t *binding,
                              const turbo_flow_databind_query_t *query,
                              turbo_flow_databind_visit_fn visit, void *ctx, size_t *matched) {
  flow_binding_query_context_t context;
  qvm_diagnostic_t diagnostic;
  int status;
  int rc;
  if (!store || !binding || !query || query->size < sizeof(*query) || !query->instructions ||
      query->instruction_count == 0u || query->length == 0u || !query->operands ||
      query->operand_count == 0u || !visit)
    return TURBO_EINVAL;
  if (matched) *matched = 0u;
  rc = flow_binding_query_validate_operands(query);
  if (rc != TURBO_OK) return rc;
  status = qvm_verify_slice_ex(query->instructions, query->instruction_count, query->offset,
                               query->length, QVM_MAX_REGISTERS, query->operand_count,
                               query->regex_count, &binding->query_limits, &diagnostic);
  if (status != QVM_STATUS_OK) return status == QVM_STATUS_RESOURCE_LIMIT ? TURBO_EFBIG : TURBO_EPROTO;
  rc = flow_binding_query_validate_opcodes(query);
  if (rc != TURBO_OK) return rc;
  memset(&context, 0, sizeof(context));
  context.scan.binding = binding;
  context.scan.visit = visit;
  context.scan.visit_ctx = ctx;
  context.query = query;
  rc = turbo_flow_store_scan(store, flow_binding_query_record, &context);
  if (rc == TURBO_OK && matched) *matched = context.matched;
  return rc;
}
