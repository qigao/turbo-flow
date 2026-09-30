#include "flow_provider_config_internal.h"

#include "salts_error.h"
#include "turbo_flow_stl_error_internal.h"

#include <stddef.h>
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
