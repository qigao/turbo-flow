#ifndef OPERATION_SCHEMA_FIXTURE_H
#define OPERATION_SCHEMA_FIXTURE_H
#include <cmeta/data.h>
Struct(operation_record, (int, first), (int, second));
Struct(operation_record_offset, (int, second), (int, first));
static const cmeta_type_desc operation_record_type = {
  "operation_record", sizeof(operation_record), _Alignof(operation_record), CMETA_T_OBJECT,
  NULL, NULL, NULL
};
static const cmeta_data_field_desc operation_record_fields[] = {
  {"operation.Record.first", "first", offsetof(operation_record, first), &cmeta_data_int},
  {"operation.Record.second", "second", offsetof(operation_record, second), &cmeta_data_int}
};
static const cmeta_data_struct_shape operation_record_shape = {
  StructMeta(operation_record), operation_record_fields, 2u
};
static const cmeta_data_desc operation_record_data = {
  sizeof(cmeta_data_desc), CMETA_DATA_DESC_ABI_VERSION, "operation.Record.data", "Record",
  CMETA_DATA_STRUCT, &operation_record_type, &operation_record_shape, NULL, NULL, NULL
};
static const cmeta_type_desc operation_record_offset_type = {
  "operation_record", sizeof(operation_record_offset), _Alignof(operation_record_offset),
  CMETA_T_OBJECT, NULL, NULL, NULL
};
static const cmeta_data_field_desc operation_record_offset_fields[] = {
  {"operation.Record.first", "first", offsetof(operation_record_offset, first), &cmeta_data_int},
  {"operation.Record.second", "second", offsetof(operation_record_offset, second), &cmeta_data_int}
};
static const cmeta_data_struct_shape operation_record_offset_shape = {
  StructMeta(operation_record_offset), operation_record_offset_fields, 2u
};
static const cmeta_data_desc operation_record_offset_data = {
  sizeof(cmeta_data_desc), CMETA_DATA_DESC_ABI_VERSION, "operation.Record.data", "Record",
  CMETA_DATA_STRUCT, &operation_record_offset_type, &operation_record_offset_shape, NULL, NULL, NULL
};
const cmeta_data_desc *operation_schema_fixture_int(void);
const cmeta_data_desc *operation_schema_fixture_record(void);
#endif
