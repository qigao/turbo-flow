#include "flow_provider_config_internal.h"

#include "salts_error.h"
#include "turbo_flow_stl_error_internal.h"

#include <cmeta/cmeta.h>

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int flow_provider_config_literals_init(vec_t *literals) {
  if (!literals) return SALTS_EINVAL;
  memset(literals, 0, sizeof(*literals));
  return turbo_flow_stl_error(vec_init_bytes(
      literals, sizeof(flow_provider_config_literal_t),
      _Alignof(turbo_flow_max_align_t), FLOW_PROVIDER_CONFIG_MAX_FIELDS));
}

void flow_provider_config_literals_destroy(vec_t *literals) {
  size_t i;
  if (!literals) return;
  for (i = 0; i < vec_size(literals); ++i) {
    flow_provider_config_literal_t *literal =
        (flow_provider_config_literal_t *)vec_at(literals, i);
    if (!literal) continue;
    tstr_freep(&literal->name);
    tstr_freep(&literal->text);
  }
  vec_destroy(literals);
  memset(literals, 0, sizeof(*literals));
}

int flow_provider_config_literals_copy(vec_t *destination, const vec_t *source) {
  size_t i;
  int rc;

  if (!destination || !source) return SALTS_EINVAL;
  rc = flow_provider_config_literals_init(destination);
  if (rc != SALTS_OK) return rc;

  for (i = 0; i < vec_size(source); ++i) {
    const flow_provider_config_literal_t *src =
        (const flow_provider_config_literal_t *)vec_at_const(source, i);
    flow_provider_config_literal_t copy;
    if (!src || !src->name) {
      flow_provider_config_literals_destroy(destination);
      return SALTS_EPROTO;
    }

    memset(&copy, 0, sizeof(copy));
    copy.kind = src->kind;
    copy.uint_value = src->uint_value;
    copy.bool_value = src->bool_value;
    copy.line = src->line;
    copy.column = src->column;
    copy.name = tstr_from_v(tstr_to_v(src->name));
    if (!copy.name) {
      flow_provider_config_literals_destroy(destination);
      return SALTS_ENOMEM;
    }
    if (src->text) {
      copy.text = tstr_from_v(tstr_to_v(src->text));
      if (!copy.text) {
        tstr_freep(&copy.name);
        flow_provider_config_literals_destroy(destination);
        return SALTS_ENOMEM;
      }
    }

    if (turbo_flow_stl_error(vec_push(destination, &copy)) != SALTS_OK) {
      tstr_freep(&copy.name);
      tstr_freep(&copy.text);
      flow_provider_config_literals_destroy(destination);
      return SALTS_ENOMEM;
    }
  }
  return SALTS_OK;
}

static cserde_status flow_provider_config_reader_next(
    void *context, cserde_token *out) {
  flow_provider_config_reader_t *reader =
      (flow_provider_config_reader_t *)context;
  size_t count;
  size_t position;

  if (!reader || !reader->literals || !out) return CSERDE_INVALID_ARGUMENT;
  count = vec_size(reader->literals);
  position = reader->token_index;

  memset(out, 0, sizeof(*out));
  if (position == 0u) {
    out->kind = CSERDE_MAP_BEGIN;
    ++reader->token_index;
    return CSERDE_OK;
  }

  --position;
  if (position < count * 2u) {
    const size_t literal_index = position / 2u;
    const flow_provider_config_literal_t *literal =
        (const flow_provider_config_literal_t *)vec_at_const(
            reader->literals, literal_index);
    if (!literal || !literal->name) return CSERDE_INVALID_STATE;

    if ((position & 1u) == 0u) {
      out->kind = CSERDE_STRING;
      out->value.slice.data = (const unsigned char *)literal->name;
      out->value.slice.size = tstr_len(literal->name);
      out->value.slice.lifetime = CSERDE_VIEW_STABLE;
    } else {
      switch (literal->kind) {
        case FLOW_PROVIDER_CONFIG_LITERAL_TEXT:
          if (!literal->text) return CSERDE_INVALID_STATE;
          out->kind = CSERDE_STRING;
          out->value.slice.data = (const unsigned char *)literal->text;
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
    }
    ++reader->token_index;
    return CSERDE_OK;
  }

  if (position == count * 2u) {
    out->kind = CSERDE_MAP_END;
    ++reader->token_index;
    return CSERDE_OK;
  }

  return CSERDE_DONE;
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
