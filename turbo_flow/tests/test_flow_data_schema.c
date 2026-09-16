#include "operation_schema_fixture.h"
#include "tinytest.h"
#include "turbo_flow_projection.h"
#include <stdint.h>
#include <limits.h>

static const turbo_flow_data_schema_t int_schema = {
  sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
  "cmeta.int.data", "Integer", "int", 7u, 3u, NULL
};
static const turbo_flow_data_schema_t record_schema = {
  sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
  "operation.Record.data", "Record", "operation_record", 8u, 3u, NULL
};

spec("flow typed data schema") {
  it("matches semantic CMeta across translation units and ignores schema text") {
    turbo_flow_data_schema_t other = int_schema;
    other.schema_text = "not identity";
    check_equal(turbo_flow_data_schema_match(&int_schema, &cmeta_data_int,
                &other, operation_schema_fixture_int()), SALTS_OK);
    other.schema_id++;
    check_equal(turbo_flow_data_schema_match(&int_schema, &cmeta_data_int,
                &other, operation_schema_fixture_int()), SALTS_EPROTO);
  }
  it("matches a reflected struct across translation units and detects layout semantics") {
    cmeta_data_desc changed = operation_record_data;
    cmeta_data_struct_shape shape = operation_record_shape;
    cmeta_data_field_desc fields[2] = {operation_record_fields[0], operation_record_fields[1]};
    check_not_equal((const void *)&operation_record_data,
                    (const void *)operation_schema_fixture_record());
    check_equal(turbo_flow_data_schema_match(&record_schema, &operation_record_data,
                &record_schema, operation_schema_fixture_record()), SALTS_OK);
    fields[0] = operation_record_fields[1];
    fields[1] = operation_record_fields[0];
    shape.fields = fields; changed.shape = &shape;
    check_equal(turbo_flow_data_schema_match(&record_schema, &operation_record_data,
                &record_schema, &changed), SALTS_EPROTO);
    check_equal(turbo_flow_data_schema_match(&record_schema, &operation_record_data,
                &record_schema, &operation_record_offset_data), SALTS_EPROTO);
  }
  it("rejects malformed and unsupported descriptors") {
    cmeta_data_desc bad = cmeta_data_int;
    cmeta_data_desc unsupported = cmeta_data_int;
    bad.abi_version++;
    check_equal(turbo_flow_data_schema_match(&int_schema, &bad, &int_schema, &cmeta_data_int),
                SALTS_EINVAL);
    unsupported.kind = CMETA_DATA_CUSTOM;
    check_equal(turbo_flow_data_schema_match(&int_schema, &unsupported,
                &int_schema, &unsupported), SALTS_ENOTSUP);
  }
  it("rejects unsupported or malformed struct children without dereferencing storage") {
    cmeta_data_desc parent = operation_record_data;
    cmeta_data_struct_shape shape = operation_record_shape;
    cmeta_data_field_desc fields[2] = {operation_record_fields[0], operation_record_fields[1]};
    cmeta_data_desc malformed = cmeta_data_int;
    fields[0].value = &cmeta_data_sequence; shape.fields = fields; parent.shape = &shape;
    check_equal(turbo_flow_data_schema_match(&record_schema, &parent, &record_schema, &parent),
                SALTS_ENOTSUP);
    malformed.storage_type = NULL; fields[0].value = &malformed;
    check_equal(turbo_flow_data_schema_match(&record_schema, &parent, &record_schema, &parent),
                SALTS_EINVAL);
  }
  it("requires exact physical scalar layout even when semantic identity matches") {
    cmeta_type_desc altered_type = cmeta_type_int;
    cmeta_data_desc altered = cmeta_data_int;
    altered_type.size += sizeof(int); altered.storage_type = &altered_type;
    check_equal(turbo_flow_data_schema_match(&int_schema, &cmeta_data_int,
                &int_schema, &altered), SALTS_EPROTO);
    {
      cmeta_data_integer_shape bits = {64u};
      altered = cmeta_data_int; altered.shape = &bits;
      check_equal(turbo_flow_data_schema_match(&int_schema, &cmeta_data_int,
                  &int_schema, &altered), SALTS_EPROTO);
    }
    altered_type = cmeta_type_int; altered = cmeta_data_int;
    altered_type.size *= 2u; altered_type.align *= 2u; altered.storage_type = &altered_type;
    check_equal(turbo_flow_data_schema_match(&int_schema, &cmeta_data_int,
                &int_schema, &altered), SALTS_EPROTO);
    altered_type = cmeta_type_int; altered = cmeta_data_int;
    altered_type.align /= 2u; altered.storage_type = &altered_type;
    check_equal(turbo_flow_data_schema_match(&int_schema, &cmeta_data_int,
                &int_schema, &altered), SALTS_EPROTO);
    {
      turbo_flow_data_schema_t renamed_schema = int_schema;
      altered = cmeta_data_int; altered.stable_id = "renamed.int.data";
      renamed_schema.schema_name = altered.stable_id;
      check_equal(turbo_flow_data_schema_match(&int_schema, &cmeta_data_int,
                  &renamed_schema, &altered), SALTS_EPROTO);
    }
  }
  it("rejects scalar width arithmetic that cannot be represented") {
    cmeta_type_desc huge_type = cmeta_type_int;
    cmeta_data_desc huge = cmeta_data_int;
    huge_type.size = SIZE_MAX / CHAR_BIT + 5u;
    huge.storage_type = &huge_type;
    check_equal(turbo_flow_data_schema_match(&int_schema, &huge, &int_schema, &huge),
                SALTS_EPROTO);
  }
  it("bounds recursive cycles before exceeding the graph depth limit") {
    cmeta_data_desc cycle = operation_record_data;
    cmeta_data_struct_shape shape = operation_record_shape;
    cmeta_data_field_desc fields[2] = {operation_record_fields[0], operation_record_fields[1]};
    fields[0].value = &cycle; shape.fields = fields; cycle.shape = &shape;
    check_equal(turbo_flow_data_schema_match(&record_schema, &cycle,
                &record_schema, &cycle), SALTS_ENOTSUP);
  }
  it("rejects field and aggregate node work beyond public limits") {
    cmeta_data_desc wide = operation_record_data;
    cmeta_data_struct_shape wide_shape = operation_record_shape;
    cmeta_struct_desc wide_layout = *StructMeta(operation_record);
    cmeta_data_field_desc wide_fields[65];
    cmeta_field_desc wide_layout_fields[65];
    cmeta_data_desc levels[16];
    cmeta_data_struct_shape shapes[16];
    cmeta_data_field_desc fields[16][2];
    cmeta_struct_desc layouts[16];
    cmeta_field_desc layout_fields[16][2];
    size_t i;
    for (i = 0; i < 65u; ++i) {
      wide_fields[i] = operation_record_fields[0];
      wide_layout_fields[i] = StructMeta(operation_record)->fields[0];
    }
    wide_shape.fields = wide_fields; wide_shape.field_count = 64u;
    wide_layout.fields = wide_layout_fields; wide_layout.field_count = 64u;
    wide_shape.layout = &wide_layout; wide.shape = &wide_shape;
    check_equal(turbo_flow_data_schema_match(&record_schema, &wide,
                &record_schema, &wide), SALTS_OK);
    wide_shape.field_count = 65u; wide_layout.field_count = 65u;
    check_equal(turbo_flow_data_schema_match(&record_schema, &wide,
                &record_schema, &wide), SALTS_ENOTSUP);
    for (i = 0; i < 16u; ++i) {
      levels[i] = operation_record_data; shapes[i] = operation_record_shape;
      fields[i][0] = operation_record_fields[0]; fields[i][1] = operation_record_fields[1];
      layouts[i] = *StructMeta(operation_record);
      layout_fields[i][0] = StructMeta(operation_record)->fields[0];
      layout_fields[i][1] = StructMeta(operation_record)->fields[1];
      fields[i][0].offset = fields[i][1].offset = 0u;
      layout_fields[i][0].offset = layout_fields[i][1].offset = 0u;
      layouts[i].fields = layout_fields[i];
      shapes[i].layout = &layouts[i]; shapes[i].fields = fields[i]; levels[i].shape = &shapes[i];
    }
    for (i = 0; i < 8u; ++i) {
      fields[i][0].value = &levels[i + 1u]; fields[i][1].value = &levels[i + 1u];
    }
    check_equal(turbo_flow_data_schema_match(&record_schema, &levels[0],
                &record_schema, &levels[0]), SALTS_ENOTSUP);
    for (i = 0; i < 6u; ++i) {
      fields[i][0].value = &levels[i + 1u]; fields[i][1].value = &levels[i + 1u];
    }
    fields[6][0].value = fields[6][1].value = &cmeta_data_int;
    {
      cmeta_data_desc unary = operation_record_data;
      cmeta_data_struct_shape unary_shape = operation_record_shape;
      cmeta_data_field_desc unary_field = operation_record_fields[0];
      unary_field.offset = 0u; unary_field.value = &levels[0];
      unary_shape.fields = &unary_field; unary_shape.field_count = 1u;
      unary.shape = &unary_shape;
      check_equal(turbo_flow_data_schema_match(&record_schema, &unary,
                  &record_schema, &unary), SALTS_OK);
      {
        cmeta_data_desc over = operation_record_data;
        cmeta_data_struct_shape over_shape = operation_record_shape;
        cmeta_data_field_desc over_field = operation_record_fields[0];
        over_field.offset = 0u; over_field.value = &unary;
        over_shape.fields = &over_field; over_shape.field_count = 1u;
        over.shape = &over_shape;
        check_equal(turbo_flow_data_schema_match(&record_schema, &over,
                    &record_schema, &over), SALTS_ENOTSUP);
      }
    }
    for (i = 0; i < 15u; ++i) {
      fields[i][0].value = &levels[i + 1u]; fields[i][1].value = &cmeta_data_int;
    }
    fields[15][0].value = fields[15][1].value = &cmeta_data_int;
    check_equal(turbo_flow_data_schema_match(&record_schema, &levels[1],
                &record_schema, &levels[1]), SALTS_OK);
    check_equal(turbo_flow_data_schema_match(&record_schema, &levels[0],
                &record_schema, &levels[0]), SALTS_ENOTSUP);
  }
}
