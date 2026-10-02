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
      _Alignof(turbo_flow_max_align_t), FLOW_PROVIDER_CONFIG_MAX_EVENTS));
}

void flow_provider_config_literals_destroy(vec_t *literals) {
  size_t i;
  if (!literals) return;
  for (i = 0; i < vec_size(literals); ++i) {
    flow_provider_config_literal_t *literal =
        (flow_provider_config_literal_t *)vec_at(literals, i);
    if (!literal) continue;
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
    if (!src) {
      flow_provider_config_literals_destroy(destination);
      return SALTS_EPROTO;
    }

    memset(&copy, 0, sizeof(copy));
    copy.kind = src->kind;
    copy.uint_value = src->uint_value;
    copy.bool_value = src->bool_value;
    copy.line = src->line;
    copy.column = src->column;
    if (src->kind == FLOW_PROVIDER_CONFIG_LITERAL_TEXT) {
      if (!src->text) {
        flow_provider_config_literals_destroy(destination);
        return SALTS_EPROTO;
      }
      copy.text = tstr_from_v(tstr_to_v(src->text));
      if (!copy.text) {
        flow_provider_config_literals_destroy(destination);
        return SALTS_ENOMEM;
      }
    }

    if (turbo_flow_stl_error(vec_push(destination, &copy)) != SALTS_OK) {
      tstr_freep(&copy.text);
      flow_provider_config_literals_destroy(destination);
      return SALTS_ENOMEM;
    }
  }
  return SALTS_OK;
}
