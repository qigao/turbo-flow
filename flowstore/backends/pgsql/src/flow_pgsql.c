#include "turbo_flow_pgsql.h"

#include "flow_pgsql_internal.h"

#include "fmt.h"
#include "libpq-fe.h"
#include "turbo_thread.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct flow_pgsql_sink_s {
  tstr resource_uid;
  tstr resource_owner;
  tstr conninfo;
  tstr statement;
  int binary_payload;
  PGconn *connection;
  turbo_mutex_t lock;
  int lock_initialized;
  int started;
  turbo_flow_content_descriptor_t parameter_descriptor;
  int has_parameter_descriptor;
} flow_pgsql_sink_t;

typedef struct flow_pgsql_query_s {
  tstr resource_uid;
  tstr resource_owner;
  tstr conninfo;
  tstr statement;
  tstr statement_name;
  tstr result_schema_name;
  tstr result_type_name;
  int bind_payload;
  int binary_payload;
  turbo_flow_pgsql_result_format_t result_format;
  size_t max_rows;
  size_t max_result_bytes;
  uint32_t result_schema_version;
  const turbo_flow_schema_registry_t *schema_registry;
  turbo_flow_pgsql_row_mapper_t row_mapper;
  int has_row_mapper;
  PGconn *connection;
  turbo_mutex_t lock;
  int lock_initialized;
  int started;
  turbo_flow_content_descriptor_t rowset_descriptor;
  turbo_flow_content_descriptor_t command_descriptor;
} flow_pgsql_query_t;

static const turbo_flow_option_field_t FLOW_PGSQL_FIELDS[] = {
    {"conninfo", TURBO_FLOW_OPTION_SECRET,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_SECRET_VALUE, 0, 0, NULL, 0},
    {"statement", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"binary_payload", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"parameter_content_type", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"parameter_content_binding", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE,
     0, 0, NULL, 0}};
static const turbo_flow_adapter_schema_t FLOW_PGSQL_SCHEMA = {NULL,
                                                              TURBO_FLOW_ADAPTER_KIND_POSTGRESQL,
                                                              TURBO_FLOW_ADAPTER_SINK,
                                                              TURBO_FLOW_ADAPTER_OUTPUT,
                                                              FLOW_PGSQL_FIELDS,
                                                              sizeof(FLOW_PGSQL_FIELDS) /
                                                                  sizeof(FLOW_PGSQL_FIELDS[0])};

static const char *const FLOW_PGSQL_RESULT_FORMAT_VALUES[] = {"rowset_json", "databind_csv"};
static const turbo_flow_option_field_t FLOW_PGSQL_QUERY_FIELDS[] = {
    {"conninfo", TURBO_FLOW_OPTION_SECRET,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_SECRET_VALUE, 0, 0, NULL, 0},
    {"statement", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"statement_name", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"bind_payload", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"binary_payload", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"result_format", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_PGSQL_RESULT_FORMAT_VALUES, 2},
    {"max_rows", TURBO_FLOW_OPTION_SIZE, TURBO_FLOW_OPTION_HAS_MIN, 1, 0, NULL, 0},
    {"max_result_bytes", TURBO_FLOW_OPTION_SIZE, TURBO_FLOW_OPTION_HAS_MIN, 1, 0, NULL, 0},
    {"result_schema_name", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"result_type_name", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"result_schema_version", TURBO_FLOW_OPTION_U32, 0, 0, 0, NULL, 0},
    {"schema_registry", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0,
     NULL, 0},
    {"row_mapper", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL,
     0}};
static const turbo_flow_adapter_schema_t FLOW_PGSQL_QUERY_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_POSTGRESQL,
    TURBO_FLOW_ADAPTER_TRANSFORM,
    TURBO_FLOW_ADAPTER_BIDIRECTIONAL,
    FLOW_PGSQL_QUERY_FIELDS,
    sizeof(FLOW_PGSQL_QUERY_FIELDS) / sizeof(FLOW_PGSQL_QUERY_FIELDS[0])};

static const char FLOW_PGSQL_SINK_RESOURCE_SCHEMA_TEXT[] =
    "schema TurboFlowDatabaseResource [id(301), version(1)];\n"
    "message PostgreSqlSinkStatus { bool started; uint32 mode; }\n";

static const char FLOW_PGSQL_QUERY_RESOURCE_SCHEMA_TEXT[] =
    "schema TurboFlowDatabaseResource [id(302), version(1)];\n"
    "message PostgreSqlQueryStatus { bool started; uint32 mode; }\n";

static const turbo_flow_resource_schema_t FLOW_PGSQL_SINK_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
    TURBO_FLOW_RESOURCE_STORAGE,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowDatabaseResource",
    "PostgreSqlSinkStatus",
    301u,
    1u,
    FLOW_PGSQL_SINK_RESOURCE_SCHEMA_TEXT};

static const turbo_flow_resource_schema_t FLOW_PGSQL_QUERY_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
    TURBO_FLOW_RESOURCE_STORAGE,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowDatabaseResource",
    "PostgreSqlQueryStatus",
    302u,
    1u,
    FLOW_PGSQL_QUERY_RESOURCE_SCHEMA_TEXT};

static int flow_pgsql_resource_metadata(const char *uid, const char *owner,
                                        turbo_flow_resource_metadata_t *out) {
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int written;
  if (!uid || !*uid || !owner || !*owner || !out || out->size < sizeof(*out)) {
    return TURBO_EINVAL;
  }
  metadata.domain = TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
  metadata.kind = TURBO_FLOW_RESOURCE_STORAGE;
  metadata.generation = 1u;
  metadata.observed_generation = 1u;
  written = snprintf(metadata.uid, sizeof(metadata.uid), "%s", uid);
  if (written < 0 || (size_t)written >= sizeof(metadata.uid)) return TURBO_ENAMETOOLONG;
  written = snprintf(metadata.owner_name, sizeof(metadata.owner_name), "%s", owner);
  if (written < 0 || (size_t)written >= sizeof(metadata.owner_name)) return TURBO_ENAMETOOLONG;
  *out = metadata;
  return TURBO_OK;
}

static int flow_pgsql_status_document(const char *uid, const char *owner, int started,
                                      uint32_t mode,
                                      const turbo_flow_resource_schema_t *schema,
                                      turbo_flow_resource_document_kind_t document_kind,
                                      turbo_flow_resource_document_t *out) {
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  const char *payload;
  int rc;
  if (document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return TURBO_ENOTSUP;
  rc = flow_pgsql_resource_metadata(uid, owner, &metadata);
  if (rc != TURBO_OK) return rc;
  payload = mode == 1u ? (started ? "{\"started\":true,\"mode\":1}"
                                  : "{\"started\":false,\"mode\":1}")
                       : (started ? "{\"started\":true,\"mode\":0}"
                                  : "{\"started\":false,\"mode\":0}");
  return turbo_flow_resource_document_set_payload_copy(out, &metadata, schema, payload,
                                                       strlen(payload));
}

static int flow_pgsql_sink_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  flow_pgsql_sink_t *sink = (flow_pgsql_sink_t *)ctx;
  return sink ? flow_pgsql_resource_metadata(sink->resource_uid, sink->resource_owner, out)
              : TURBO_EINVAL;
}

static int flow_pgsql_sink_resource_document(
    void *ctx, turbo_flow_resource_document_kind_t document_kind,
    turbo_flow_resource_document_t *out) {
  flow_pgsql_sink_t *sink = (flow_pgsql_sink_t *)ctx;
  int started;
  if (!sink || !sink->lock_initialized) return TURBO_EINVAL;
  turbo_mutex_lock(&sink->lock);
  started = sink->started;
  turbo_mutex_unlock(&sink->lock);
  return flow_pgsql_status_document(sink->resource_uid, sink->resource_owner, started, 0u,
                                    &FLOW_PGSQL_SINK_STATUS_SCHEMA, document_kind, out);
}

static int flow_pgsql_start(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_pgsql_sink_t *sink = (flow_pgsql_sink_t *)ctx;
  (void)flow;
  if (!sink || !stage || stage->is_source) return TURBO_EINVAL;
  turbo_mutex_lock(&sink->lock);
  sink->connection = PQconnectdb(sink->conninfo);
  if (!sink->connection || PQstatus(sink->connection) != CONNECTION_OK) {
    if (sink->connection) PQfinish(sink->connection);
    sink->connection = NULL;
    turbo_mutex_unlock(&sink->lock);
    return TURBO_EIO;
  }
  sink->started = 1;
  turbo_mutex_unlock(&sink->lock);
  return TURBO_OK;
}

static int flow_pgsql_consume(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                              turbo_flow_msg_t *msg) {
  flow_pgsql_sink_t *sink = (flow_pgsql_sink_t *)ctx;
  const char *values[1];
  int lengths[1];
  int formats[1];
  PGresult *result;
  ExecStatusType status;
  tstr text_payload = NULL;
  int rc;
  (void)flow;
  (void)stage;
  if (!sink || !msg || msg->payload.len > INT_MAX) return TURBO_EINVAL;
  if (sink->has_parameter_descriptor) {
    const turbo_flow_content_descriptor_t *actual = turbo_flow_msg_content_descriptor(msg);
    int content_rc =
        actual ? turbo_flow_content_descriptor_validate_payload(&sink->parameter_descriptor, actual)
               : turbo_flow_msg_set_content_descriptor(msg, &sink->parameter_descriptor);
    if (content_rc != TURBO_OK) return content_rc;
  }
  if (!sink->binary_payload) {
    text_payload = tstr_new_len(msg->payload.data ? msg->payload.data : "", msg->payload.len);
    if (!text_payload) return TURBO_ENOMEM;
  }
  values[0] = sink->binary_payload ? (msg->payload.data ? msg->payload.data : "") : text_payload;
  lengths[0] = (int)msg->payload.len;
  formats[0] = sink->binary_payload ? 1 : 0;
  turbo_mutex_lock(&sink->lock);
  if (!sink->started || !sink->connection) {
    turbo_mutex_unlock(&sink->lock);
    tstr_freep(&text_payload);
    return TURBO_ESHUTDOWN;
  }
  result = PQexecParams(sink->connection, sink->statement, 1, NULL, values, lengths, formats, 0);
  status = result ? PQresultStatus(result) : PGRES_FATAL_ERROR;
  rc = status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK ? TURBO_OK : TURBO_EIO;
  if (result) PQclear(result);
  turbo_mutex_unlock(&sink->lock);
  tstr_freep(&text_payload);
  return rc;
}

static void flow_pgsql_stop(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_pgsql_sink_t *sink = (flow_pgsql_sink_t *)ctx;
  (void)flow;
  (void)stage;
  if (!sink) return;
  turbo_mutex_lock(&sink->lock);
  sink->started = 0;
  if (sink->connection) PQfinish(sink->connection);
  sink->connection = NULL;
  turbo_mutex_unlock(&sink->lock);
}

static void flow_pgsql_shutdown(void *ctx) {
  flow_pgsql_sink_t *sink = (flow_pgsql_sink_t *)ctx;
  if (!sink) return;
  if (sink->lock_initialized) {
    flow_pgsql_stop(sink, NULL, NULL);
    turbo_mutex_destroy(&sink->lock);
  }
  tstr_freep(&sink->resource_uid);
  tstr_freep(&sink->resource_owner);
  tstr_freep(&sink->conninfo);
  tstr_freep(&sink->statement);
  free(sink);
}

int turbo_flow_pgsql_register_sink_adapter(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_pgsql_sink_config_t *config) {
  flow_pgsql_sink_t *sink;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_resource_provider_registration_t resource =
      TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
  int rc;
  if (!flow || !name || !*name || !config || !config->conninfo || !*config->conninfo ||
      !config->statement || !*config->statement || !strstr(config->statement, "$1"))
    return TURBO_EINVAL;
  sink = (flow_pgsql_sink_t *)calloc(1, sizeof(*sink));
  if (!sink) return TURBO_ENOMEM;
  sink->resource_uid = tstr_format("postgresql:{}", name);
  sink->resource_owner = tstr_dup(name);
  sink->conninfo = tstr_dup(config->conninfo);
  sink->statement = tstr_dup(config->statement);
  sink->binary_payload = config->binary_payload != 0;
  if (config->parameter_content_type) {
    rc = turbo_flow_content_descriptor_from_media(
        &sink->parameter_descriptor, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
        TURBO_FLOW_CONTENT_PROFILE_DATABASE_PARAMETERS, config->parameter_content_type, name,
        config->parameter_content_binding);
    if (rc == TURBO_OK) {
      sink->has_parameter_descriptor = 1;
    } else if (rc != TURBO_ENOENT ||
               (config->parameter_content_binding &&
                config->parameter_content_binding->schema.schema_version != 0u)) {
      flow_pgsql_shutdown(sink);
      return rc == TURBO_ENOENT ? TURBO_EPROTO : rc;
    }
  }
  if (!sink->resource_uid || !sink->resource_owner || !sink->conninfo || !sink->statement) {
    flow_pgsql_shutdown(sink);
    return TURBO_ENOMEM;
  }
  if (tstr_len(sink->resource_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      tstr_len(sink->resource_owner) > TURBO_FLOW_RESOURCE_OWNER_MAX) {
    flow_pgsql_shutdown(sink);
    return TURBO_ENAMETOOLONG;
  }
  turbo_mutex_init(&sink->lock);
  sink->lock_initialized = 1;
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_pgsql_start;
  ops.consume = flow_pgsql_consume;
  ops.stop = flow_pgsql_stop;
  ops.shutdown = flow_pgsql_shutdown;
  resource.owner_name = name;
  resource.ops.metadata = flow_pgsql_sink_resource_metadata;
  resource.ops.document = flow_pgsql_sink_resource_document;
  resource.ctx = sink;
  rc = turbo_flow_register_adapter_with_resources(flow, name, &ops, sink, &FLOW_PGSQL_SCHEMA,
                                                  &resource, 1u);
  return rc;
}

static int flow_pgsql_query_start(void *ctx, turbo_flow_t *flow,
                                  const turbo_flow_stage_plan_t *stage) {
  flow_pgsql_query_t *query = (flow_pgsql_query_t *)ctx;
  (void)flow;
  if (!query || !stage || stage->is_source) return TURBO_EINVAL;
  turbo_mutex_lock(&query->lock);
  query->connection = PQconnectdb(query->conninfo);
  if (!query->connection || PQstatus(query->connection) != CONNECTION_OK) {
    if (query->connection) PQfinish(query->connection);
    query->connection = NULL;
    turbo_mutex_unlock(&query->lock);
    return TURBO_EIO;
  }
  query->started = 1;
  turbo_mutex_unlock(&query->lock);
  return TURBO_OK;
}

static int flow_pgsql_query_consume(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  flow_pgsql_query_t *query = (flow_pgsql_query_t *)ctx;
  turbo_flow_msg_t output;
  const char *values[1];
  int lengths[1];
  int formats[1];
  tstr text_payload = NULL;
  PGresult *result = NULL;
  int rc;
  (void)flow;
  (void)stage;
  if (!query || !msg || (msg->payload.len > 0u && !msg->payload.data) ||
      msg->payload.len > INT_MAX) {
    return TURBO_EINVAL;
  }
  if (query->bind_payload && !query->binary_payload) {
    text_payload = tstr_new_len(msg->payload.data ? msg->payload.data : "", msg->payload.len);
    if (!text_payload) return TURBO_ENOMEM;
  }
  values[0] = query->binary_payload ? (msg->payload.data ? msg->payload.data : "") : text_payload;
  lengths[0] = (int)msg->payload.len;
  formats[0] = query->binary_payload ? 1 : 0;
  turbo_flow_msg_init(&output);
  output.id = msg->id;
  output.ts_ns = msg->ts_ns;
  output.type = msg->type;
  output.flags = msg->flags;
  output.transport_context = msg->transport_context;
  output.execution_attempt = msg->execution_attempt;
  turbo_mutex_lock(&query->lock);
  if (!query->started || !query->connection) {
    rc = TURBO_ESHUTDOWN;
  } else {
    result = PQexecParams(query->connection, query->statement, query->bind_payload ? 1 : 0, NULL,
                          query->bind_payload ? values : NULL, query->bind_payload ? lengths : NULL,
                          query->bind_payload ? formats : NULL, 0);
    if (!result) {
      rc = TURBO_EIO;
    } else rc = TURBO_OK;
  }
  turbo_mutex_unlock(&query->lock);
  if (rc == TURBO_OK) {
    rc = flow_pgsql_result_to_message(
        result, query->statement_name, query->max_rows, query->max_result_bytes,
        query->result_format, &query->rowset_descriptor, &query->command_descriptor, &output);
    if (rc == TURBO_OK && PQresultStatus(result) == PGRES_TUPLES_OK) {
      rc = flow_pgsql_result_bind_projection(result, &output, query->schema_registry,
                                             query->result_schema_name, query->result_type_name,
                                             query->result_schema_version,
                                             query->has_row_mapper ? &query->row_mapper : NULL);
    }
  }
  if (result) PQclear(result);
  tstr_freep(&text_payload);
  if (rc != TURBO_OK) {
    turbo_flow_msg_cleanup(&output);
    return rc;
  }
  turbo_flow_msg_cleanup(msg);
  return turbo_flow_msg_move(msg, &output);
}

static void flow_pgsql_query_stop(void *ctx, turbo_flow_t *flow,
                                  const turbo_flow_stage_plan_t *stage) {
  flow_pgsql_query_t *query = (flow_pgsql_query_t *)ctx;
  (void)flow;
  (void)stage;
  if (!query) return;
  turbo_mutex_lock(&query->lock);
  query->started = 0;
  if (query->connection) PQfinish(query->connection);
  query->connection = NULL;
  turbo_mutex_unlock(&query->lock);
}

static void flow_pgsql_query_shutdown(void *ctx) {
  flow_pgsql_query_t *query = (flow_pgsql_query_t *)ctx;
  if (!query) return;
  if (query->lock_initialized) {
    flow_pgsql_query_stop(query, NULL, NULL);
    turbo_mutex_destroy(&query->lock);
  }
  tstr_freep(&query->resource_uid);
  tstr_freep(&query->resource_owner);
  tstr_freep(&query->conninfo);
  tstr_freep(&query->statement);
  tstr_freep(&query->statement_name);
  tstr_freep(&query->result_schema_name);
  tstr_freep(&query->result_type_name);
  free(query);
}

static int flow_pgsql_query_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  flow_pgsql_query_t *query = (flow_pgsql_query_t *)ctx;
  return query ? flow_pgsql_resource_metadata(query->resource_uid, query->resource_owner, out)
               : TURBO_EINVAL;
}

static int flow_pgsql_query_resource_document(
    void *ctx, turbo_flow_resource_document_kind_t document_kind,
    turbo_flow_resource_document_t *out) {
  flow_pgsql_query_t *query = (flow_pgsql_query_t *)ctx;
  int started;
  if (!query || !query->lock_initialized) return TURBO_EINVAL;
  turbo_mutex_lock(&query->lock);
  started = query->started;
  turbo_mutex_unlock(&query->lock);
  return flow_pgsql_status_document(query->resource_uid, query->resource_owner, started, 1u,
                                    &FLOW_PGSQL_QUERY_STATUS_SCHEMA, document_kind, out);
}

int turbo_flow_pgsql_register_query_adapter(turbo_flow_t *flow, const char *name,
                                            const turbo_flow_pgsql_query_config_t *config) {
  flow_pgsql_query_t *query;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_resource_provider_registration_t resource =
      TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
  int schema_fields;
  int rc;
  if (!flow || !name || !name[0] || !config || !config->conninfo || !config->conninfo[0] ||
      !config->statement || !config->statement[0] || !config->statement_name ||
      !config->statement_name[0] || config->result_format < TURBO_FLOW_PGSQL_RESULT_ROWSET_JSON ||
      config->result_format > TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV ||
      (config->bind_payload && !strstr(config->statement, "$1")) ||
      (!config->bind_payload && config->binary_payload)) {
    return TURBO_EINVAL;
  }
  schema_fields = (config->result_schema_name && config->result_schema_name[0]) +
                  (config->result_type_name && config->result_type_name[0]) +
                  (config->result_schema_version != 0u) + (config->schema_registry != NULL);
  if (schema_fields != 0 && schema_fields != 4) return TURBO_EINVAL;
  if (config->row_mapper && schema_fields != 4) return TURBO_EINVAL;
  query = (flow_pgsql_query_t *)calloc(1, sizeof(*query));
  if (!query) return TURBO_ENOMEM;
  query->resource_uid = tstr_format("postgresql:{}", name);
  query->resource_owner = tstr_dup(name);
  query->conninfo = tstr_dup(config->conninfo);
  query->statement = tstr_dup(config->statement);
  query->statement_name = tstr_dup(config->statement_name);
  if (config->result_schema_name) query->result_schema_name = tstr_dup(config->result_schema_name);
  if (config->result_type_name) query->result_type_name = tstr_dup(config->result_type_name);
  query->bind_payload = config->bind_payload != 0;
  query->binary_payload = config->binary_payload != 0;
  query->result_format = config->result_format;
  query->max_rows = config->max_rows ? config->max_rows : TURBO_FLOW_PGSQL_DEFAULT_MAX_ROWS;
  query->max_result_bytes = config->max_result_bytes ? config->max_result_bytes
                                                     : TURBO_FLOW_PGSQL_DEFAULT_MAX_RESULT_BYTES;
  query->result_schema_version = config->result_schema_version;
  query->schema_registry = config->schema_registry;
  {
    turbo_flow_content_binding_t binding = TURBO_FLOW_CONTENT_BINDING_INIT;
    const turbo_flow_content_binding_t *binding_ptr = NULL;
    if (schema_fields == 4) {
      binding.registry = config->schema_registry;
      binding.schema.schema_name = config->result_schema_name;
      binding.schema.type_name = config->result_type_name;
      binding.schema.schema_version = config->result_schema_version;
      binding_ptr = &binding;
    }
    rc = turbo_flow_content_descriptor_from_media(
        &query->rowset_descriptor, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
        TURBO_FLOW_CONTENT_PROFILE_DATABASE_ROWSET,
        config->result_format == TURBO_FLOW_PGSQL_RESULT_DATABIND_CSV
            ? "text/csv"
            : "application/vnd.turboflow.pg-rowset+json",
        config->statement_name, binding_ptr);
    if (rc == TURBO_OK) query->rowset_descriptor.flags |= TURBO_FLOW_CONTENT_BATCH;
    if (rc == TURBO_OK) {
      rc = turbo_flow_content_descriptor_from_media(
          &query->command_descriptor, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
          TURBO_FLOW_CONTENT_PROFILE_DATABASE_COMMAND_RESULT,
          "application/vnd.turboflow.pg-command+json", config->statement_name, NULL);
    }
    if (rc != TURBO_OK) {
      flow_pgsql_query_shutdown(query);
      return rc;
    }
  }
  if (config->row_mapper) {
    if (config->row_mapper->size < sizeof(*config->row_mapper) || !config->row_mapper->map ||
        !config->row_mapper->destroy) {
      flow_pgsql_query_shutdown(query);
      return TURBO_EINVAL;
    }
    query->row_mapper = *config->row_mapper;
    query->has_row_mapper = 1;
  }
  if (!query->resource_uid || !query->resource_owner || !query->conninfo || !query->statement ||
      !query->statement_name ||
      (config->result_schema_name && !query->result_schema_name) ||
      (config->result_type_name && !query->result_type_name)) {
    flow_pgsql_query_shutdown(query);
    return TURBO_ENOMEM;
  }
  if (tstr_len(query->resource_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      tstr_len(query->resource_owner) > TURBO_FLOW_RESOURCE_OWNER_MAX) {
    flow_pgsql_query_shutdown(query);
    return TURBO_ENAMETOOLONG;
  }
  turbo_mutex_init(&query->lock);
  query->lock_initialized = 1;
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_pgsql_query_start;
  ops.consume = flow_pgsql_query_consume;
  ops.stop = flow_pgsql_query_stop;
  ops.shutdown = flow_pgsql_query_shutdown;
  resource.owner_name = name;
  resource.ops.metadata = flow_pgsql_query_resource_metadata;
  resource.ops.document = flow_pgsql_query_resource_document;
  resource.ctx = query;
  rc = turbo_flow_register_adapter_with_resources(flow, name, &ops, query, &FLOW_PGSQL_QUERY_SCHEMA,
                                                  &resource, 1u);
  return rc;
}
