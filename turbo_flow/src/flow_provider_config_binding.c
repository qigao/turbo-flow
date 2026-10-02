#include "flow_provider_config_internal.h"

#include "salts_error.h"

#include <cmeta/cmeta.h>

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum {
  FLOW_PROVIDER_READER_OUTER_BEGIN = 0u,
  FLOW_PROVIDER_READER_FIELD_KEY,
  FLOW_PROVIDER_READER_FIELD_VALUE,
  FLOW_PROVIDER_READER_DONE
};

enum {
  FLOW_PROVIDER_STRUCTURED_MAP = 1u,
  FLOW_PROVIDER_STRUCTURED_ARRAY = 2u
};

static void flow_provider_config_structured_skip(
    flow_provider_config_reader_t *reader) {
  const char *cursor;
  if (!reader || !reader->structured_cursor || !reader->structured_limit)
    return;
  cursor = reader->structured_cursor;
  for (;;) {
    while (cursor < reader->structured_limit &&
           (*cursor == ' ' || *cursor == '\t' ||
            *cursor == '\r' || *cursor == '\n' || *cursor == ','))
      ++cursor;
    if (cursor < reader->structured_limit && *cursor == '#') {
      while (cursor < reader->structured_limit &&
             *cursor != '\r' && *cursor != '\n')
        ++cursor;
      continue;
    }
    if (cursor + 1 < reader->structured_limit &&
        cursor[0] == '%' && cursor[1] == '%') {
      cursor += 2;
      while (cursor < reader->structured_limit &&
             *cursor != '\r' && *cursor != '\n')
        ++cursor;
      continue;
    }
    break;
  }
  reader->structured_cursor = cursor;
}

static int flow_provider_config_ident_start(char value) {
  return (value >= 'A' && value <= 'Z') ||
         (value >= 'a' && value <= 'z') || value == '_';
}

static int flow_provider_config_ident_continue(char value) {
  return flow_provider_config_ident_start(value) ||
         (value >= '0' && value <= '9');
}

static void flow_provider_config_structured_value_complete(
    flow_provider_config_reader_t *reader) {
  const uint32_t top =
      reader && reader->structured_depth != 0u
          ? reader->structured_depth - 1u
          : 0u;
  if (reader && reader->structured_depth != 0u &&
      reader->structured_kind[top] == FLOW_PROVIDER_STRUCTURED_MAP &&
      reader->structured_map_expect_key[top] == 0u)
    reader->structured_map_expect_key[top] = 1u;
}

static cserde_status flow_provider_config_structured_next(
    flow_provider_config_reader_t *reader, cserde_token *out) {
  const char *cursor;
  const char *start;
  uint32_t top;
  int map_key = 0;

  if (!reader || !out || !reader->structured_cursor ||
      !reader->structured_limit ||
      reader->structured_cursor > reader->structured_limit)
    return CSERDE_INVALID_ARGUMENT;

  flow_provider_config_structured_skip(reader);
  cursor = reader->structured_cursor;
  if (cursor == reader->structured_limit)
    return reader->structured_depth == 0u
               ? CSERDE_DONE
               : CSERDE_INVALID_STATE;

  if (reader->structured_tokens >=
      FLOW_PROVIDER_CONFIG_MAX_STRUCTURED_TOKENS)
    return CSERDE_LIMIT_EXCEEDED;

  if (reader->structured_depth != 0u) {
    top = reader->structured_depth - 1u;
    map_key =
        reader->structured_kind[top] == FLOW_PROVIDER_STRUCTURED_MAP &&
        reader->structured_map_expect_key[top] != 0u;
  }

  memset(out, 0, sizeof(*out));

  if (*cursor == '[' || *cursor == '{') {
    const uint8_t kind = *cursor == '{'
                             ? FLOW_PROVIDER_STRUCTURED_MAP
                             : FLOW_PROVIDER_STRUCTURED_ARRAY;
    if (map_key ||
        reader->structured_depth >=
            FLOW_PROVIDER_CONFIG_MAX_STRUCTURED_DEPTH)
      return map_key ? CSERDE_INVALID_STATE : CSERDE_LIMIT_EXCEEDED;

    flow_provider_config_structured_value_complete(reader);
    top = reader->structured_depth++;
    reader->structured_kind[top] = kind;
    reader->structured_map_expect_key[top] =
        kind == FLOW_PROVIDER_STRUCTURED_MAP ? 1u : 0u;
    out->kind = kind == FLOW_PROVIDER_STRUCTURED_MAP
                    ? CSERDE_MAP_BEGIN
                    : CSERDE_ARRAY_BEGIN;
    reader->structured_cursor = cursor + 1;
    ++reader->structured_tokens;
    return CSERDE_OK;
  }

  if (*cursor == ']' || *cursor == '}') {
    const uint8_t expected =
        *cursor == '}' ? FLOW_PROVIDER_STRUCTURED_MAP
                       : FLOW_PROVIDER_STRUCTURED_ARRAY;
    if (reader->structured_depth == 0u)
      return CSERDE_INVALID_STATE;
    top = reader->structured_depth - 1u;
    if (reader->structured_kind[top] != expected ||
        (expected == FLOW_PROVIDER_STRUCTURED_MAP &&
         reader->structured_map_expect_key[top] == 0u))
      return CSERDE_INVALID_STATE;

    --reader->structured_depth;
    out->kind = expected == FLOW_PROVIDER_STRUCTURED_MAP
                    ? CSERDE_MAP_END
                    : CSERDE_ARRAY_END;
    reader->structured_cursor = cursor + 1;
    ++reader->structured_tokens;
    return CSERDE_OK;
  }

  if (*cursor == '"') {
    start = ++cursor;
    while (cursor < reader->structured_limit &&
           *cursor != '"' && *cursor != '\r' && *cursor != '\n')
      ++cursor;
    if (cursor >= reader->structured_limit || *cursor != '"')
      return CSERDE_INVALID_STATE;

    out->kind = CSERDE_STRING;
    out->value.slice.data = (const unsigned char *)start;
    out->value.slice.size = (size_t)(cursor - start);
    out->value.slice.lifetime = CSERDE_VIEW_STABLE;
    reader->structured_cursor = cursor + 1;
    ++reader->structured_tokens;
    if (map_key)
      reader->structured_map_expect_key[
          reader->structured_depth - 1u] = 0u;
    else
      flow_provider_config_structured_value_complete(reader);
    return CSERDE_OK;
  }

  if (*cursor >= '0' && *cursor <= '9') {
    uint64_t value = 0u;
    if (map_key) return CSERDE_INVALID_STATE;
    start = cursor;
    while (cursor < reader->structured_limit &&
           *cursor >= '0' && *cursor <= '9') {
      const uint64_t digit = (uint64_t)(*cursor - '0');
      if (value > (UINT64_MAX - digit) / UINT64_C(10))
        return CSERDE_LIMIT_EXCEEDED;
      value = value * UINT64_C(10) + digit;
      ++cursor;
    }
    if (cursor == start) return CSERDE_INVALID_STATE;
    out->kind = CSERDE_UINT;
    out->value.uint = value;
    reader->structured_cursor = cursor;
    ++reader->structured_tokens;
    flow_provider_config_structured_value_complete(reader);
    return CSERDE_OK;
  }

  if (flow_provider_config_ident_start(*cursor)) {
    start = cursor++;
    while (cursor < reader->structured_limit &&
           flow_provider_config_ident_continue(*cursor))
      ++cursor;

    if (map_key) {
      out->kind = CSERDE_STRING;
      out->value.slice.data = (const unsigned char *)start;
      out->value.slice.size = (size_t)(cursor - start);
      out->value.slice.lifetime = CSERDE_VIEW_STABLE;
      reader->structured_map_expect_key[
          reader->structured_depth - 1u] = 0u;
    } else if ((size_t)(cursor - start) == 4u &&
               memcmp(start, "true", 4u) == 0) {
      out->kind = CSERDE_BOOL;
      out->value.boolean = true;
      flow_provider_config_structured_value_complete(reader);
    } else if ((size_t)(cursor - start) == 5u &&
               memcmp(start, "false", 5u) == 0) {
      out->kind = CSERDE_BOOL;
      out->value.boolean = false;
      flow_provider_config_structured_value_complete(reader);
    } else {
      out->kind = CSERDE_STRING;
      out->value.slice.data = (const unsigned char *)start;
      out->value.slice.size = (size_t)(cursor - start);
      out->value.slice.lifetime = CSERDE_VIEW_STABLE;
      flow_provider_config_structured_value_complete(reader);
    }
    reader->structured_cursor = cursor;
    ++reader->structured_tokens;
    return CSERDE_OK;
  }

  return CSERDE_INVALID_STATE;
}

static cserde_status flow_provider_config_reader_next(
    void *context, cserde_token *out) {
  flow_provider_config_reader_t *reader =
      (flow_provider_config_reader_t *)context;
  const flow_provider_config_literal_t *literal;
  size_t count;

  if (!reader || !reader->literals || !out)
    return CSERDE_INVALID_ARGUMENT;
  count = vec_size(reader->literals);
  memset(out, 0, sizeof(*out));

  if (reader->state == FLOW_PROVIDER_READER_OUTER_BEGIN) {
    out->kind = CSERDE_MAP_BEGIN;
    reader->state = FLOW_PROVIDER_READER_FIELD_KEY;
    return CSERDE_OK;
  }

  if (reader->state == FLOW_PROVIDER_READER_DONE)
    return CSERDE_DONE;

  if (reader->state == FLOW_PROVIDER_READER_FIELD_KEY) {
    if (reader->literal_index >= count) {
      out->kind = CSERDE_MAP_END;
      reader->state = FLOW_PROVIDER_READER_DONE;
      return CSERDE_OK;
    }
    literal =
        (const flow_provider_config_literal_t *)vec_at_const(
            reader->literals, reader->literal_index);
    if (!literal || !literal->name) return CSERDE_INVALID_STATE;
    out->kind = CSERDE_STRING;
    out->value.slice.data =
        (const unsigned char *)literal->name;
    out->value.slice.size = tstr_len(literal->name);
    out->value.slice.lifetime = CSERDE_VIEW_STABLE;
    reader->state = FLOW_PROVIDER_READER_FIELD_VALUE;
    return CSERDE_OK;
  }

  if (reader->state != FLOW_PROVIDER_READER_FIELD_VALUE ||
      reader->literal_index >= count)
    return CSERDE_INVALID_STATE;

  literal =
      (const flow_provider_config_literal_t *)vec_at_const(
          reader->literals, reader->literal_index);
  if (!literal) return CSERDE_INVALID_STATE;

  if (literal->kind == FLOW_PROVIDER_CONFIG_LITERAL_STRUCTURED) {
    cserde_status status;
    if (!literal->text ||
        tstr_len(literal->text) == 0u ||
        tstr_len(literal->text) >
            FLOW_PROVIDER_CONFIG_MAX_STRUCTURED_BYTES)
      return CSERDE_INVALID_STATE;

    if (!reader->structured_cursor) {
      reader->structured_cursor = literal->text;
      reader->structured_limit =
          literal->text + tstr_len(literal->text);
      reader->structured_tokens = 0u;
      reader->structured_depth = 0u;
      memset(reader->structured_kind, 0,
             sizeof(reader->structured_kind));
      memset(reader->structured_map_expect_key, 0,
             sizeof(reader->structured_map_expect_key));
    }

    status = flow_provider_config_structured_next(reader, out);
    if (status != CSERDE_OK) return status;

    if (reader->structured_depth == 0u &&
        reader->structured_cursor == reader->structured_limit) {
      reader->structured_cursor = NULL;
      reader->structured_limit = NULL;
      reader->structured_tokens = 0u;
      ++reader->literal_index;
      reader->state = FLOW_PROVIDER_READER_FIELD_KEY;
    }
    return CSERDE_OK;
  }

  switch (literal->kind) {
  case FLOW_PROVIDER_CONFIG_LITERAL_TEXT:
    if (!literal->text) return CSERDE_INVALID_STATE;
    out->kind = CSERDE_STRING;
    out->value.slice.data =
        (const unsigned char *)literal->text;
    out->value.slice.size = tstr_len(literal->text);
    out->value.slice.lifetime = CSERDE_VIEW_STABLE;
    break;
  case FLOW_PROVIDER_CONFIG_LITERAL_UINT:
    out->kind = CSERDE_UINT;
    out->value.uint = literal->uint_value;
    break;
  case FLOW_PROVIDER_CONFIG_LITERAL_BOOL:
    out->kind = CSERDE_BOOL;
    out->value.boolean = literal->bool_value != 0;
    break;
  default:
    return CSERDE_INVALID_STATE;
  }

  ++reader->literal_index;
  reader->state = FLOW_PROVIDER_READER_FIELD_KEY;
  return CSERDE_OK;
}

static const cserde_reader_ops FLOW_PROVIDER_CONFIG_READER_OPS = {
    offsetof(cserde_reader_ops, next) +
        sizeof(((cserde_reader_ops *)0)->next),
    CSERDE_READER_OPS_ABI_VERSION,
    flow_provider_config_reader_next};

int flow_provider_config_reader_init(
    flow_provider_config_reader_t *config_reader,
    const flow_stage_plan_impl_t *stage) {
  if (!config_reader || !stage) return SALTS_EINVAL;
  memset(config_reader, 0, sizeof(*config_reader));
  config_reader->literals = &stage->provider_config_literals;
  return cserde_reader_init(
             &config_reader->reader, &FLOW_PROVIDER_CONFIG_READER_OPS,
             config_reader) == CSERDE_OK
             ? SALTS_OK
             : SALTS_EINVAL;
}


static int flow_provider_config_status(DataBindStatus status) {
  switch (status) {
    case DATA_BIND_OK:
      return SALTS_OK;
    case DATA_BIND_ERR_INVALID_ARG:
      return SALTS_EINVAL;
    case DATA_BIND_ERR_OOM:
      return SALTS_ENOMEM;
    case DATA_BIND_ERR_LIMIT:
    case DATA_BIND_ERR_BUFFER_TOO_SMALL:
      return SALTS_ENOSPC;
    case DATA_BIND_ERR_CANCELED:
      return SALTS_ECANCELED;
    case DATA_BIND_ERR_IO:
    case DATA_BIND_ERR_PARSE:
    case DATA_BIND_ERR_SCHEMA:
    case DATA_BIND_ERR_TYPE_NOT_FOUND:
    case DATA_BIND_ERR_TYPE_MISMATCH:
    case DATA_BIND_ERR_RUNTIME:
    case DATA_BIND_ERR_VALIDATION:
      return SALTS_EPROTO;
  }
  return SALTS_EPROTO;
}

static int flow_provider_config_allocate_aligned(
    size_t size, size_t alignment, void **allocation_out, void **value_out) {
  uintptr_t base;
  uintptr_t aligned;
  size_t total;
  void *allocation;

  if (allocation_out) *allocation_out = NULL;
  if (value_out) *value_out = NULL;
  if (!allocation_out || !value_out || size == 0u || alignment == 0u ||
      (alignment & (alignment - 1u)) != 0u)
    return SALTS_EINVAL;
  if (size > SIZE_MAX - (alignment - 1u)) return SALTS_ERANGE;

  total = size + alignment - 1u;
  allocation = calloc(1u, total);
  if (!allocation) return SALTS_ENOMEM;

  base = (uintptr_t)allocation;
  aligned = (base + (uintptr_t)alignment - 1u) &
            ~((uintptr_t)alignment - 1u);
  *allocation_out = allocation;
  *value_out = (void *)aligned;
  return SALTS_OK;
}

int flow_provider_typed_config_bind(
    const flow_stage_plan_impl_t *stage,
    flow_provider_config_codec_factory_fn codec_factory,
    const DataBindMessageNativeArtifact *artifact,
    flow_provider_typed_config_t *out,
    DataBindMessagePlanDiagnostic *diagnostic) {
  DataBind *codec = NULL;
  DataBindError error = DATA_BIND_ERROR_INIT;
  DataBindMessagePlanDiagnostic local_diagnostic =
      DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
  flow_provider_config_reader_t reader;
  const cmeta_type_desc *storage_type;
  DataBindStatus status;
  int rc;

  if (!stage || !codec_factory || !artifact || !out)
    return SALTS_EINVAL;
  memset(out, 0, sizeof(*out));
  if (diagnostic) {
    if (diagnostic->size != sizeof(*diagnostic) ||
        diagnostic->abi_version != DATA_BIND_MESSAGE_PLAN_ABI_VERSION)
      return SALTS_EINVAL;
    *diagnostic = local_diagnostic;
  }

  if (!data_bind_message_native_artifact_valid(artifact))
    return SALTS_EPROTO;

  out->artifact = artifact;
  status = artifact->native_binding(&out->native, &error);
  if (status != DATA_BIND_OK) {
    memset(out, 0, sizeof(*out));
    return flow_provider_config_status(status);
  }
  if (!cmeta_data_desc_valid(out->native.data) ||
      out->native.data->kind != CMETA_DATA_STRUCT ||
      !out->native.data->storage_type ||
      !out->native.idl_type_name ||
      strcmp(out->native.idl_type_name, artifact->type_name) != 0) {
    memset(out, 0, sizeof(*out));
    return SALTS_EPROTO;
  }

  status = codec_factory(&codec, &error);
  if (status != DATA_BIND_OK || !codec) {
    if (codec) data_bind_free(codec);
    memset(out, 0, sizeof(*out));
    return status == DATA_BIND_OK ? SALTS_EPROTO
                                  : flow_provider_config_status(status);
  }

  status = data_bind_message_plan_compile(
      codec, artifact->type_name, &out->native, &out->plan,
      diagnostic ? diagnostic : &local_diagnostic);
  data_bind_free(codec);
  codec = NULL;
  if (status != DATA_BIND_OK) {
    memset(out, 0, sizeof(*out));
    return flow_provider_config_status(status);
  }

  storage_type = out->native.data->storage_type;
  if (!storage_type || storage_type->size == 0u || storage_type->align == 0u) {
    flow_provider_typed_config_destroy(out);
    return SALTS_EPROTO;
  }
  rc = flow_provider_config_allocate_aligned(
      storage_type->size, storage_type->align,
      &out->storage_allocation, &out->value);
  if (rc != SALTS_OK) {
    flow_provider_typed_config_destroy(out);
    return rc;
  }
  out->value_bytes = storage_type->size;

  out->workspace = malloc(FLOW_PROVIDER_CONFIG_WORKSPACE_BYTES);
  if (!out->workspace) {
    flow_provider_typed_config_destroy(out);
    return SALTS_ENOMEM;
  }
  out->workspace_bytes = FLOW_PROVIDER_CONFIG_WORKSPACE_BYTES;
  out->options = (DataBindNativeOptions)DATA_BIND_NATIVE_OPTIONS_INIT;
  out->options.workspace = out->workspace;
  out->options.workspace_bytes = out->workspace_bytes;
  out->options.max_depth = FLOW_PROVIDER_CONFIG_MAX_DEPTH;
  out->options.max_items = FLOW_PROVIDER_CONFIG_MAX_ITEMS;
  out->options.max_owned_bytes = FLOW_PROVIDER_CONFIG_MAX_OWNED_BYTES;

  rc = flow_provider_config_reader_init(&reader, stage);
  if (rc != SALTS_OK) {
    flow_provider_typed_config_destroy(out);
    return rc;
  }
  status = data_bind_message_plan_decode_native(
      out->plan, &out->options, &reader.reader, out->value,
      out->value_bytes, diagnostic ? diagnostic : &local_diagnostic);
  if (status != DATA_BIND_OK) {
    flow_provider_typed_config_destroy(out);
    return flow_provider_config_status(status);
  }

  out->value_live = 1;
  return SALTS_OK;
}

int flow_provider_typed_config_view(
    const flow_provider_typed_config_t *config,
    flow_provider_typed_config_view_t *out) {
  if (!config || !out || !config->value_live || !config->artifact ||
      !config->native.data || !config->value || config->value_bytes == 0u)
    return SALTS_EINVAL;
  out->type_name = config->artifact->type_name;
  out->data = config->native.data;
  out->value = config->value;
  out->value_bytes = config->value_bytes;
  return SALTS_OK;
}

int flow_provider_typed_config_destroy(flow_provider_typed_config_t *config) {
  int rc = SALTS_OK;

  if (!config) return SALTS_EINVAL;
  if (config->value_live && config->value && config->native.data) {
    DataBindNativeDiagnostic diagnostic = DATA_BIND_NATIVE_DIAGNOSTIC_INIT;
    DataBindStatus status = data_bind_native_clear(
        &config->options, config->native.data, config->value,
        config->value_bytes, &diagnostic);
    if (status != DATA_BIND_OK) rc = flow_provider_config_status(status);
  }

  data_bind_message_plan_free(config->plan);
  free(config->workspace);
  free(config->storage_allocation);
  memset(config, 0, sizeof(*config));
  return rc;
}
