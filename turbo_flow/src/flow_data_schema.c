#include "flow_internal.h"
#include "turbo_flow_projection.h"

#include <stdint.h>
#include <limits.h>
#include <stdbool.h>
#include <string.h>

static int flow_schema_text_equal(const char *a, const char *b) {
  return a && b && strcmp(a, b) == 0;
}

static int flow_schema_identity_valid(const turbo_flow_data_schema_t *schema,
                                      const cmeta_data_desc *data) {
  return schema && schema->size >= sizeof(*schema) && schema->domain > TURBO_FLOW_DOMAIN_NONE &&
         schema->domain <= TURBO_FLOW_DOMAIN_MANAGEMENT && schema->encoding >= TURBO_FLOW_DATA_ENCODING_TBE &&
         schema->encoding <= TURBO_FLOW_DATA_ENCODING_OPAQUE && schema->schema_id &&
         schema->schema_version && schema->schema_name && schema->type_name &&
         schema->projection_type && data && data->stable_id &&
         data->storage_type && data->storage_type->name &&
         flow_schema_text_equal(schema->schema_name, data->stable_id) &&
         flow_schema_text_equal(schema->projection_type, data->storage_type->name);
}

static int flow_data_kind_supported(cmeta_data_kind kind) {
  return kind == CMETA_DATA_BOOL || kind == CMETA_DATA_SINT || kind == CMETA_DATA_UINT ||
         kind == CMETA_DATA_FLOAT || kind == CMETA_DATA_STRUCT;
}

static int flow_data_precheck(const cmeta_data_desc *data) {
  const cmeta_data_struct_shape *shape;
  if (!data || data->struct_size < offsetof(cmeta_data_desc, shape) + sizeof(data->shape) ||
      data->abi_version != CMETA_DATA_DESC_ABI_VERSION || !cmeta_data_kind_valid(data->kind))
    return SALTS_EINVAL;
  if (!flow_data_kind_supported(data->kind)) return SALTS_ENOTSUP;
  if (data->kind != CMETA_DATA_STRUCT) return SALTS_OK;
  shape = (const cmeta_data_struct_shape *)data->shape;
  if (!shape || !shape->layout) return SALTS_EINVAL;
  if (shape->field_count > FLOW_DATA_SCHEMA_MAX_FIELDS ||
      shape->layout->field_count > FLOW_DATA_SCHEMA_MAX_FIELDS) return SALTS_ENOTSUP;
  if ((shape->field_count && !shape->fields) ||
      (shape->layout->field_count && !shape->layout->fields)) return SALTS_EINVAL;
  return SALTS_OK;
}

static int flow_data_match(const cmeta_data_desc *a, const cmeta_data_desc *b,
                           unsigned depth, unsigned *nodes) {
  size_t i;
  int precheck;
  if (++*nodes > FLOW_DATA_SCHEMA_MAX_NODES || depth > FLOW_DATA_SCHEMA_MAX_DEPTH) return SALTS_ENOTSUP;
  precheck = flow_data_precheck(a);
  if (precheck != SALTS_OK) return precheck;
  precheck = flow_data_precheck(b);
  if (precheck != SALTS_OK) return precheck;
  if (!cmeta_data_desc_valid(a) || !cmeta_data_desc_valid(b)) return SALTS_EINVAL;
  if (a->kind != b->kind || !flow_schema_text_equal(a->stable_id, b->stable_id) ||
      !a->storage_type || !b->storage_type ||
      a->storage_type->size != b->storage_type->size ||
      a->storage_type->align != b->storage_type->align ||
      !cmeta_type_equal(a->storage_type, b->storage_type)) return SALTS_EPROTO;
  switch (a->kind) {
    case CMETA_DATA_BOOL:
      return a->storage_type->size == sizeof(bool) &&
             a->storage_type->align == _Alignof(bool) ? SALTS_OK : SALTS_EPROTO;
    case CMETA_DATA_SINT:
    case CMETA_DATA_UINT:
      if (a->storage_type->size > SIZE_MAX / CHAR_BIT) return SALTS_EPROTO;
      return ((const cmeta_data_integer_shape *)a->shape)->bits ==
                     ((const cmeta_data_integer_shape *)b->shape)->bits &&
             ((const cmeta_data_integer_shape *)a->shape)->bits ==
                     a->storage_type->size * CHAR_BIT ? SALTS_OK : SALTS_EPROTO;
    case CMETA_DATA_FLOAT:
      if (a->storage_type->size > SIZE_MAX / CHAR_BIT) return SALTS_EPROTO;
      return ((const cmeta_data_float_shape *)a->shape)->bits ==
                     ((const cmeta_data_float_shape *)b->shape)->bits &&
             ((const cmeta_data_float_shape *)a->shape)->bits ==
                     a->storage_type->size * CHAR_BIT ? SALTS_OK : SALTS_EPROTO;
    case CMETA_DATA_STRUCT: {
      const cmeta_data_struct_shape *sa = (const cmeta_data_struct_shape *)a->shape;
      const cmeta_data_struct_shape *sb = (const cmeta_data_struct_shape *)b->shape;
      if (sa->field_count > FLOW_DATA_SCHEMA_MAX_FIELDS || sb->field_count > FLOW_DATA_SCHEMA_MAX_FIELDS)
        return SALTS_ENOTSUP;
      if (sa->field_count != sb->field_count || !sa->layout || !sb->layout ||
          sa->layout->size != a->storage_type->size || sb->layout->size != b->storage_type->size ||
          sa->layout->align != a->storage_type->align || sb->layout->align != b->storage_type->align)
        return SALTS_EPROTO;
      for (i = 0; i < sa->field_count; ++i) {
        int rc;
        const cmeta_data_field_desc *fa = &sa->fields[i], *fb = &sb->fields[i];
        if (!fa->value || !fb->value || !flow_schema_text_equal(fa->stable_id, fb->stable_id) ||
            !flow_schema_text_equal(fa->name, fb->name) || fa->offset != fb->offset)
          return SALTS_EPROTO;
        rc = flow_data_match(fa->value, fb->value, depth + 1u, nodes);
        if (rc != SALTS_OK) return rc;
        if (fa->offset > a->storage_type->size ||
            fa->value->storage_type->size > a->storage_type->size - fa->offset ||
            fb->offset > b->storage_type->size ||
            fb->value->storage_type->size > b->storage_type->size - fb->offset)
          return SALTS_EPROTO;
      }
      return SALTS_OK;
    }
    default:
      return SALTS_ENOTSUP;
  }
}

int turbo_flow_data_schema_match(const turbo_flow_data_schema_t *a, const cmeta_data_desc *ad,
                                 const turbo_flow_data_schema_t *b, const cmeta_data_desc *bd) {
  unsigned nodes = 0;
  int rc = flow_data_precheck(ad);
  if (rc != SALTS_OK) return rc;
  rc = flow_data_precheck(bd);
  if (rc != SALTS_OK) return rc;
  if (!flow_schema_identity_valid(a, ad) || !flow_schema_identity_valid(b, bd)) return SALTS_EINVAL;
  if (a->domain != b->domain || a->encoding != b->encoding || a->schema_id != b->schema_id ||
      a->schema_version != b->schema_version || !flow_schema_text_equal(a->schema_name, b->schema_name) ||
      !flow_schema_text_equal(a->type_name, b->type_name) ||
      !flow_schema_text_equal(a->projection_type, b->projection_type)) return SALTS_EPROTO;
  return flow_data_match(ad, bd, 1u, &nodes);
}

int turbo_flow_value_require_disjoint(const void *borrowed, size_t bn,
                                      const void *candidate, size_t cn) {
  uintptr_t a, b;
  if (!borrowed || !candidate || !bn || !cn) return SALTS_EINVAL;
  a = (uintptr_t)borrowed; b = (uintptr_t)candidate;
  if (bn > UINTPTR_MAX - a || cn > UINTPTR_MAX - b) return SALTS_EINVAL;
  return a + bn <= b || b + cn <= a ? SALTS_OK : SALTS_EPROTO;
}
