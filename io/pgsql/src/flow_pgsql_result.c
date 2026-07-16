#include "flow_pgsql_internal.h"

#include "turbo_parser.h"
#include "turbo_str.h"

#include <stdio.h>
#include <string.h>

#define FLOW_PGSQL_MAX_COLUMNS 1024
#define FLOW_PGSQL_ROWSET_MEDIA_TYPE "application/vnd.turboflow.pg-rowset+json"
#define FLOW_PGSQL_COMMAND_MEDIA_TYPE "application/vnd.turboflow.pg-command+json"
#define FLOW_PGSQL_CSV_MEDIA_TYPE "text/csv"

struct turbo_flow_pgsql_rowset_view_s {
  const PGresult *result;
  size_t rows;
  size_t columns;
};

size_t turbo_flow_pgsql_row_count(const turbo_flow_pgsql_rowset_view_t *rows) {
  return rows ? rows->rows : 0u;
}

size_t turbo_flow_pgsql_column_count(const turbo_flow_pgsql_rowset_view_t *rows) {
  return rows ? rows->columns : 0u;
}

const char *turbo_flow_pgsql_column_name(const turbo_flow_pgsql_rowset_view_t *rows,
                                         size_t column) {
  return rows && column < rows->columns ? PQfname(rows->result, (int)column) : NULL;
}

uint32_t turbo_flow_pgsql_column_oid(const turbo_flow_pgsql_rowset_view_t *rows, size_t column) {
  return rows && column < rows->columns ? (uint32_t)PQftype(rows->result, (int)column) : 0u;
}

int turbo_flow_pgsql_column_format(const turbo_flow_pgsql_rowset_view_t *rows, size_t column) {
  return rows && column < rows->columns ? PQfformat(rows->result, (int)column) : -1;
}

int turbo_flow_pgsql_cell(const turbo_flow_pgsql_rowset_view_t *rows, size_t row, size_t column,
                          const char **data, size_t *len, int *is_null) {
  int length;
  if (data) *data = NULL;
  if (len) *len = 0u;
  if (is_null) *is_null = 0;
  if (!rows || !data || !len || !is_null || row >= rows->rows || column >= rows->columns) {
    return TURBO_EINVAL;
  }
  *is_null = PQgetisnull(rows->result, (int)row, (int)column);
  if (*is_null) return TURBO_OK;
  length = PQgetlength(rows->result, (int)row, (int)column);
  if (length < 0) return TURBO_EPROTO;
  *data = PQgetvalue(rows->result, (int)row, (int)column);
  *len = (size_t)length;
  return *data ? TURBO_OK : TURBO_EPROTO;
}

static int flow_pgsql_json_object_add(json_value_t *object, const char *name, json_value_t *value) {
  if (!object || !name || !value) return TURBO_ENOMEM;
  turbo_json_object_add(object, name, value);
  return turbo_json_object_get(object, name) == value ? TURBO_OK : TURBO_ENOMEM;
}

static int flow_pgsql_json_array_add(json_value_t *array, json_value_t *value) {
  size_t before;
  if (!array || !value) return TURBO_ENOMEM;
  before = turbo_json_array_size(array);
  turbo_json_array_add(array, value);
  return turbo_json_array_size(array) == before + 1u ? TURBO_OK : TURBO_ENOMEM;
}

static json_value_t *flow_pgsql_json_decimal_string(uint64_t value) {
  char text[32];
  int written = snprintf(text, sizeof(text), "%llu", (unsigned long long)value);
  return written > 0 && (size_t)written < sizeof(text) ? turbo_json_create_string(text) : NULL;
}

static int flow_pgsql_build_columns(const PGresult *result, int fields, json_value_t **out) {
  json_value_t *columns = turbo_json_create_array();
  int rc = columns ? TURBO_OK : TURBO_ENOMEM;
  for (int field = 0; field < fields && rc == TURBO_OK; ++field) {
    json_value_t *column = turbo_json_create_object();
    json_value_t *name = turbo_json_create_string(PQfname(result, field));
    json_value_t *oid = flow_pgsql_json_decimal_string((uint64_t)PQftype(result, field));
    json_value_t *format = turbo_json_create_number((double)PQfformat(result, field));
    rc = flow_pgsql_json_object_add(column, "name", name);
    if (rc == TURBO_OK) rc = flow_pgsql_json_object_add(column, "oid", oid);
    if (rc == TURBO_OK) rc = flow_pgsql_json_object_add(column, "format", format);
    if (rc == TURBO_OK) rc = flow_pgsql_json_array_add(columns, column);
    if (rc != TURBO_OK) {
      if (column && turbo_json_array_size(columns) == (size_t)field) turbo_free_json(&column);
      if (name && (!column || turbo_json_object_get(column, "name") != name))
        turbo_free_json(&name);
      if (oid && (!column || turbo_json_object_get(column, "oid") != oid)) turbo_free_json(&oid);
      if (format && (!column || turbo_json_object_get(column, "format") != format)) {
        turbo_free_json(&format);
      }
    }
  }
  if (rc != TURBO_OK) {
    turbo_free_json(&columns);
    return rc;
  }
  *out = columns;
  return TURBO_OK;
}

static int flow_pgsql_build_rows(const PGresult *result, int rows, int fields, json_value_t **out) {
  json_value_t *items = turbo_json_create_array();
  int rc = items ? TURBO_OK : TURBO_ENOMEM;
  for (int row = 0; row < rows && rc == TURBO_OK; ++row) {
    json_value_t *values = turbo_json_create_array();
    if (!values) rc = TURBO_ENOMEM;
    for (int field = 0; field < fields && rc == TURBO_OK; ++field) {
      json_value_t *value;
      if (PQfformat(result, field) != 0) {
        rc = TURBO_ENOTSUP;
        break;
      }
      if (PQgetisnull(result, row, field)) {
        value = turbo_json_create_null();
      } else {
        const char *data = PQgetvalue(result, row, field);
        int length = PQgetlength(result, row, field);
        if (!data || length < 0 || strlen(data) != (size_t)length) {
          rc = TURBO_EPROTO;
          break;
        }
        value = turbo_json_create_string(data);
      }
      rc = flow_pgsql_json_array_add(values, value);
      if (rc != TURBO_OK && value && turbo_json_array_size(values) == (size_t)field) {
        turbo_free_json(&value);
      }
    }
    if (rc == TURBO_OK) rc = flow_pgsql_json_array_add(items, values);
    if (rc != TURBO_OK && values && turbo_json_array_size(items) == (size_t)row) {
      turbo_free_json(&values);
    }
  }
  if (rc != TURBO_OK) {
    turbo_free_json(&items);
    return rc;
  }
  *out = items;
  return TURBO_OK;
}

static int flow_pgsql_csv_column_names_valid(const PGresult *result, int fields) {
  for (int field = 0; field < fields; ++field) {
    const char *name = PQfname(result, field);
    if (!name || !name[0] || PQfformat(result, field) != 0) return 0;
    for (int previous = 0; previous < field; ++previous) {
      if (strcmp(name, PQfname(result, previous)) == 0) return 0;
    }
  }
  return 1;
}

static int flow_pgsql_csv_append_raw(tstr_t *csv, const char *data, size_t len, size_t max_bytes) {
  tstr_t next;
  if (!csv || !*csv || (!data && len > 0u)) return TURBO_EINVAL;
  if (tstr_len(*csv) > max_bytes || len > max_bytes - tstr_len(*csv)) return TURBO_EFBIG;
  next = tstr_cat_len(*csv, data ? data : "", len);
  if (!next) return TURBO_ENOMEM;
  *csv = next;
  return TURBO_OK;
}

static int flow_pgsql_csv_append_cell(tstr_t *csv, const char *data, size_t len, size_t max_bytes) {
  int quote = 0;
  if (!csv || !*csv || (!data && len > 0u)) return TURBO_EINVAL;
  for (size_t i = 0; i < len; ++i) {
    if (data[i] == ',' || data[i] == '"' || data[i] == '\r' || data[i] == '\n') {
      quote = 1;
      break;
    }
  }
  if (!quote) return flow_pgsql_csv_append_raw(csv, data, len, max_bytes);
  {
    int rc = flow_pgsql_csv_append_raw(csv, "\"", 1u, max_bytes);
    if (rc != TURBO_OK) return rc;
  }
  for (size_t i = 0; i < len; ++i) {
    const char *part = data[i] == '"' ? "\"\"" : &data[i];
    size_t part_len = data[i] == '"' ? 2u : 1u;
    int rc = flow_pgsql_csv_append_raw(csv, part, part_len, max_bytes);
    if (rc != TURBO_OK) return rc;
  }
  return flow_pgsql_csv_append_raw(csv, "\"", 1u, max_bytes);
}

static int flow_pgsql_result_to_csv(PGresult *result, size_t max_rows, size_t max_result_bytes,
                                    tstr_t *out) {
  int rows = PQntuples(result);
  int fields = PQnfields(result);
  tstr_t csv;
  int rc = TURBO_OK;
  if (rows < 0 || fields <= 0 || fields > FLOW_PGSQL_MAX_COLUMNS || (size_t)rows > max_rows) {
    return fields > FLOW_PGSQL_MAX_COLUMNS || (size_t)rows > max_rows ? TURBO_ENOSPC : TURBO_EPROTO;
  }
  if (!flow_pgsql_csv_column_names_valid(result, fields)) return TURBO_ENOTSUP;
  csv = tstr_new_len("", 0u);
  if (!csv) return TURBO_ENOMEM;
  for (int field = 0; field < fields && rc == TURBO_OK; ++field) {
    if (field > 0) rc = flow_pgsql_csv_append_raw(&csv, ",", 1u, max_result_bytes);
    if (rc == TURBO_OK) {
      const char *name = PQfname(result, field);
      rc = flow_pgsql_csv_append_cell(&csv, name, strlen(name), max_result_bytes);
    }
  }
  if (rc == TURBO_OK) rc = flow_pgsql_csv_append_raw(&csv, "\n", 1u, max_result_bytes);
  for (int row = 0; row < rows && rc == TURBO_OK; ++row) {
    for (int field = 0; field < fields && rc == TURBO_OK; ++field) {
      const char *data;
      int len;
      if (field > 0) rc = flow_pgsql_csv_append_raw(&csv, ",", 1u, max_result_bytes);
      if (rc != TURBO_OK) break;
      if (PQgetisnull(result, row, field)) {
        rc = TURBO_ENOTSUP;
        break;
      }
      data = PQgetvalue(result, row, field);
      len = PQgetlength(result, row, field);
      if (!data || len < 0 || strlen(data) != (size_t)len) {
        rc = TURBO_EPROTO;
        break;
      }
      rc = flow_pgsql_csv_append_cell(&csv, data, (size_t)len, max_result_bytes);
    }
    if (rc == TURBO_OK) rc = flow_pgsql_csv_append_raw(&csv, "\n", 1u, max_result_bytes);
  }
  if (rc != TURBO_OK) {
    tstr_free(csv);
    return rc;
  }
  *out = csv;
  return TURBO_OK;
}

static int flow_pgsql_serialize_result(PGresult *result, size_t max_rows, size_t max_result_bytes,
                                       char **json_out, size_t *len_out,
                                       turbo_flow_content_profile_t *profile_out,
                                       const char **media_type_out) {
  ExecStatusType status = PQresultStatus(result);
  json_value_t *root = turbo_json_create_object();
  char *json = NULL;
  size_t len = 0;
  int rc = root ? TURBO_OK : TURBO_ENOMEM;
  if (status == PGRES_TUPLES_OK) {
    int rows = PQntuples(result);
    int fields = PQnfields(result);
    json_value_t *columns = NULL;
    json_value_t *items = NULL;
    if (rows < 0 || fields < 0 || fields > FLOW_PGSQL_MAX_COLUMNS || (size_t)rows > max_rows) {
      rc = fields > FLOW_PGSQL_MAX_COLUMNS || (size_t)rows > max_rows ? TURBO_ENOSPC : TURBO_EPROTO;
    }
    if (rc == TURBO_OK) rc = flow_pgsql_build_columns(result, fields, &columns);
    if (rc == TURBO_OK) rc = flow_pgsql_build_rows(result, rows, fields, &items);
    if (rc == TURBO_OK) rc = flow_pgsql_json_object_add(root, "columns", columns);
    if (rc == TURBO_OK) rc = flow_pgsql_json_object_add(root, "rows", items);
    if (rc == TURBO_OK) {
      *profile_out = TURBO_FLOW_CONTENT_PROFILE_DATABASE_ROWSET;
      *media_type_out = FLOW_PGSQL_ROWSET_MEDIA_TYPE;
    }
    if (rc != TURBO_OK) {
      if (columns && turbo_json_object_get(root, "columns") != columns) turbo_free_json(&columns);
      if (items && turbo_json_object_get(root, "rows") != items) turbo_free_json(&items);
    }
  } else if (status == PGRES_COMMAND_OK) {
    const char *command = PQcmdStatus(result);
    const char *affected = PQcmdTuples(result);
    json_value_t *command_value = turbo_json_create_string(command ? command : "");
    json_value_t *affected_value = turbo_json_create_string(affected ? affected : "");
    rc = flow_pgsql_json_object_add(root, "command", command_value);
    if (rc == TURBO_OK) rc = flow_pgsql_json_object_add(root, "affectedRows", affected_value);
    if (rc == TURBO_OK) {
      *profile_out = TURBO_FLOW_CONTENT_PROFILE_DATABASE_COMMAND_RESULT;
      *media_type_out = FLOW_PGSQL_COMMAND_MEDIA_TYPE;
    }
    if (rc != TURBO_OK) {
      if (command_value && turbo_json_object_get(root, "command") != command_value) {
        turbo_free_json(&command_value);
      }
      if (affected_value && turbo_json_object_get(root, "affectedRows") != affected_value) {
        turbo_free_json(&affected_value);
      }
    }
  } else {
    rc = TURBO_EIO;
  }
  if (rc == TURBO_OK) {
    json = turbo_json_serialize(root, &len);
    if (!json) rc = TURBO_ENOMEM;
    else if (len > max_result_bytes) rc = TURBO_EFBIG;
  }
  turbo_free_json(&root);
  if (rc != TURBO_OK) {
    turbo_json_serialize_free(json);
    return rc;
  }
  *json_out = json;
  *len_out = len;
  return TURBO_OK;
}

int flow_pgsql_result_to_message(PGresult *result, const char *statement_name, size_t max_rows,
                                 size_t max_result_bytes,
                                 turbo_flow_pgsql_result_format_t result_format,
                                 const turbo_flow_content_descriptor_t *rowset_descriptor,
                                 const turbo_flow_content_descriptor_t *command_descriptor,
                                 turbo_flow_msg_t *msg) {
  turbo_flow_content_profile_t profile;
  const char *media_type = NULL;
  char *json = NULL;
  size_t len = 0;
  tstr_t payload = NULL;
  int rc;
  if (!result || !statement_name || !statement_name[0] || !rowset_descriptor ||
      !command_descriptor || !msg || max_rows == 0u || max_result_bytes == 0u ||
      result_format < TURBO_FLOW_PGSQL_RESULT_ROWSET_JSON ||
      result_format > TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV) {
    return TURBO_EINVAL;
  }
  if (result_format == TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV &&
      PQresultStatus(result) == PGRES_TUPLES_OK) {
    rc = flow_pgsql_result_to_csv(result, max_rows, max_result_bytes, &payload);
    profile = TURBO_FLOW_CONTENT_PROFILE_DATABASE_ROWSET;
    media_type = FLOW_PGSQL_CSV_MEDIA_TYPE;
  } else {
    rc = flow_pgsql_serialize_result(result, max_rows, max_result_bytes, &json, &len, &profile,
                                     &media_type);
    if (rc == TURBO_OK) {
      payload = tstr_new_len(json, len);
      if (!payload) rc = TURBO_ENOMEM;
    }
    turbo_json_serialize_free(json);
  }
  if (rc != TURBO_OK) return rc;
  turbo_flow_msg_clear_projection(msg);
  tstr_freep(&msg->owned_payload);
  mem_buffer_release(msg->buffer);
  msg->buffer = NULL;
  msg->owned_payload = payload;
  msg->payload = tstr_to_v(payload);
  return turbo_flow_msg_set_content_descriptor(
      msg, profile == TURBO_FLOW_CONTENT_PROFILE_DATABASE_ROWSET ? rowset_descriptor
                                                                 : command_descriptor);
}

int flow_pgsql_result_bind_projection(PGresult *result, turbo_flow_msg_t *msg,
                                      const turbo_flow_schema_registry_t *registry,
                                      const char *schema_name, const char *type_name,
                                      uint32_t schema_version,
                                      const turbo_flow_pgsql_row_mapper_t *mapper) {
  turbo_flow_pgsql_rowset_view_t rows;
  const turbo_flow_content_descriptor_t *current_descriptor;
  const turbo_flow_data_schema_t *schema = NULL;
  void *projection = NULL;
  int rc;
  if (!result || !msg || PQresultStatus(result) != PGRES_TUPLES_OK) return TURBO_EINVAL;
  if (!schema_name && !type_name && schema_version == 0u && !registry && !mapper) return TURBO_OK;
  if (!schema_name || !type_name || schema_version == 0u || !registry ||
      (mapper && (mapper->size < sizeof(*mapper) || !mapper->map || !mapper->destroy))) {
    return TURBO_EINVAL;
  }
  current_descriptor = turbo_flow_msg_content_descriptor(msg);
  if (!current_descriptor) return TURBO_EINVAL;
  if ((current_descriptor->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) == 0u ||
      current_descriptor->schema_version != schema_version ||
      strcmp(current_descriptor->schema_name, schema_name) != 0 ||
      strcmp(current_descriptor->type_name, type_name) != 0) {
    return TURBO_EPROTO;
  }
  rc = turbo_flow_schema_registry_resolve(registry, current_descriptor, &schema);
  if (rc != TURBO_OK) return rc;
  if (!mapper) return TURBO_OK;
  rows.result = result;
  rows.rows = (size_t)PQntuples(result);
  rows.columns = (size_t)PQnfields(result);
  rc = mapper->map(mapper->ctx, &rows, schema, &projection);
  if (rc != TURBO_OK) return rc;
  if (!projection) return TURBO_EPROTO;
  rc = turbo_flow_msg_bind_projection(msg, schema, projection, mapper->clone, mapper->destroy,
                                      mapper->ctx);
  if (rc != TURBO_OK) mapper->destroy(projection, mapper->ctx);
  return rc;
}
