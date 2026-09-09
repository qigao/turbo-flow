#include <tinytest.h>
#include <turbo_flow_turbodb.h>

#include <cbind/status.h>
#include <cflow/publishers.h>
#include <cmeta/data.h>
#include <cmeta/struct.h>
#include <salts_error.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TEST_ROW_DATA_PREFIX_SIZE                                                                  \
  (offsetof(cmeta_data_desc, shape) + sizeof(((cmeta_data_desc *)0)->shape))

Struct(test_db_row, (int, id), (long, score));
Struct(test_db_nested_inner, (int, value));
Struct(test_db_nested_row, (test_db_nested_inner, inner));

static const cmeta_type_identity TEST_DB_ROW_IDENTITY =
    CMETA_TYPE_ID_ATOM_INIT("turboflow.test.DbRow");
static const cmeta_type_traits TEST_DB_ROW_TRAITS = {.flags = CMETA_TRAIT_TRIVIAL_COPY |
                                                              CMETA_TRAIT_TRIVIAL_DESTROY};
static const cmeta_type_desc TEST_DB_ROW_TYPE = {.name = "test_db_row",
                                                 .size = sizeof(test_db_row),
                                                 .align = _Alignof(test_db_row),
                                                 .kind = CMETA_T_OBJECT,
                                                 .traits = &TEST_DB_ROW_TRAITS,
                                                 .identity = &TEST_DB_ROW_IDENTITY};
static const cmeta_data_field_desc TEST_DB_ROW_FIELDS[] = {
    {"turboflow.test.DbRow.id", "id", offsetof(test_db_row, id), &cmeta_data_int},
    {"turboflow.test.DbRow.score", "score", offsetof(test_db_row, score), &cmeta_data_long}};
static const cmeta_data_struct_shape TEST_DB_ROW_SHAPE = {
    .layout = StructMeta(test_db_row),
    .fields = TEST_DB_ROW_FIELDS,
    .field_count = sizeof(TEST_DB_ROW_FIELDS) / sizeof(TEST_DB_ROW_FIELDS[0])};
static const cmeta_data_desc TEST_DB_ROW_DATA = {.struct_size = TEST_ROW_DATA_PREFIX_SIZE,
                                                 .abi_version = CMETA_DATA_DESC_ABI_VERSION,
                                                 .stable_id = "turboflow.test.DbRow.data",
                                                 .display_name = "TurboFlow test database row",
                                                 .kind = CMETA_DATA_STRUCT,
                                                 .storage_type = &TEST_DB_ROW_TYPE,
                                                 .shape = &TEST_DB_ROW_SHAPE};

static const cmeta_type_identity TEST_DB_NESTED_INNER_IDENTITY =
    CMETA_TYPE_ID_ATOM_INIT("turboflow.test.DbNestedInner");
static const cmeta_type_desc TEST_DB_NESTED_INNER_TYPE = {.name = "test_db_nested_inner",
                                                          .size = sizeof(test_db_nested_inner),
                                                          .align = _Alignof(test_db_nested_inner),
                                                          .kind = CMETA_T_OBJECT,
                                                          .traits = &TEST_DB_ROW_TRAITS,
                                                          .identity =
                                                              &TEST_DB_NESTED_INNER_IDENTITY};
static const cmeta_data_field_desc TEST_DB_NESTED_INNER_FIELDS[] = {
    {"turboflow.test.DbNestedInner.value", "value", offsetof(test_db_nested_inner, value),
     &cmeta_data_int}};
static const cmeta_data_struct_shape TEST_DB_NESTED_INNER_SHAPE = {
    .layout = StructMeta(test_db_nested_inner),
    .fields = TEST_DB_NESTED_INNER_FIELDS,
    .field_count = sizeof(TEST_DB_NESTED_INNER_FIELDS) / sizeof(TEST_DB_NESTED_INNER_FIELDS[0])};
static const cmeta_data_desc TEST_DB_NESTED_INNER_DATA = {
    .struct_size = TEST_ROW_DATA_PREFIX_SIZE,
    .abi_version = CMETA_DATA_DESC_ABI_VERSION,
    .stable_id = "turboflow.test.DbNestedInner.data",
    .display_name = "TurboFlow test nested database inner row",
    .kind = CMETA_DATA_STRUCT,
    .storage_type = &TEST_DB_NESTED_INNER_TYPE,
    .shape = &TEST_DB_NESTED_INNER_SHAPE};

static const cmeta_type_identity TEST_DB_NESTED_ROW_IDENTITY =
    CMETA_TYPE_ID_ATOM_INIT("turboflow.test.DbNestedRow");
static const cmeta_type_desc TEST_DB_NESTED_ROW_TYPE = {.name = "test_db_nested_row",
                                                        .size = sizeof(test_db_nested_row),
                                                        .align = _Alignof(test_db_nested_row),
                                                        .kind = CMETA_T_OBJECT,
                                                        .traits = &TEST_DB_ROW_TRAITS,
                                                        .identity = &TEST_DB_NESTED_ROW_IDENTITY};
static const cmeta_data_field_desc TEST_DB_NESTED_ROW_FIELDS[] = {
    {"turboflow.test.DbNestedRow.inner", "inner", offsetof(test_db_nested_row, inner),
     &TEST_DB_NESTED_INNER_DATA}};
static const cmeta_data_struct_shape TEST_DB_NESTED_ROW_SHAPE = {
    .layout = StructMeta(test_db_nested_row),
    .fields = TEST_DB_NESTED_ROW_FIELDS,
    .field_count = sizeof(TEST_DB_NESTED_ROW_FIELDS) / sizeof(TEST_DB_NESTED_ROW_FIELDS[0])};
static const cmeta_data_desc TEST_DB_NESTED_ROW_DATA = {
    .struct_size = TEST_ROW_DATA_PREFIX_SIZE,
    .abi_version = CMETA_DATA_DESC_ABI_VERSION,
    .stable_id = "turboflow.test.DbNestedRow.data",
    .display_name = "TurboFlow test nested database row",
    .kind = CMETA_DATA_STRUCT,
    .storage_type = &TEST_DB_NESTED_ROW_TYPE,
    .shape = &TEST_DB_NESTED_ROW_SHAPE};

static const turbo_flow_data_schema_t TEST_DB_ROW_SCHEMA = {
    .size = sizeof(turbo_flow_data_schema_t),
    .domain = TURBO_FLOW_DOMAIN_DATA,
    .encoding = TURBO_FLOW_DATA_ENCODING_TBE,
    .schema_name = "test.db.row",
    .type_name = "TestDbRow",
    .projection_type = "test_db_row",
    .schema_id = 2u,
    .schema_version = 1u};

static const turbo_flow_data_schema_t TEST_DB_COMMAND_SCHEMA = {
    .size = sizeof(turbo_flow_data_schema_t),
    .domain = TURBO_FLOW_DOMAIN_DATA,
    .encoding = TURBO_FLOW_DATA_ENCODING_TBE,
    .schema_name = "test.db.command",
    .type_name = "OrmCommandResult",
    .projection_type = "orm_command_result_t",
    .schema_id = 3u,
    .schema_version = 1u};

static const turbo_flow_data_schema_t TEST_DB_NESTED_ROW_SCHEMA = {
    .size = sizeof(turbo_flow_data_schema_t),
    .domain = TURBO_FLOW_DOMAIN_DATA,
    .encoding = TURBO_FLOW_DATA_ENCODING_TBE,
    .schema_name = "test.db.nested.row",
    .type_name = "TestDbNestedRow",
    .projection_type = "test_db_nested_row",
    .schema_id = 4u,
    .schema_version = 1u};

static const turbo_flow_data_schema_t INT_SCHEMA = {.size = sizeof(turbo_flow_data_schema_t),
                                                    .domain = TURBO_FLOW_DOMAIN_DATA,
                                                    .encoding = TURBO_FLOW_DATA_ENCODING_TBE,
                                                    .schema_name = "test.int",
                                                    .type_name = "TestInt",
                                                    .projection_type = "int",
                                                    .schema_id = 1u,
                                                    .schema_version = 1u};

typedef struct managed_row_s {
  int *value;
} managed_row_t;

static size_t managed_row_live_allocations;

static bool managed_row_copy(void *destination, const void *source) {
  managed_row_t *copy = (managed_row_t *)destination;
  const managed_row_t *original = (const managed_row_t *)source;
  copy->value = (int *)malloc(sizeof(*copy->value));
  if (!copy->value) return false;
  *copy->value = *original->value;
  ++managed_row_live_allocations;
  return true;
}

static void managed_row_move(void *destination, void *source) {
  *(managed_row_t *)destination = *(managed_row_t *)source;
  ((managed_row_t *)source)->value = NULL;
}

static void managed_row_destroy(void *value) {
  managed_row_t *row = (managed_row_t *)value;
  if (!row->value) return;
  free(row->value);
  row->value = NULL;
  --managed_row_live_allocations;
}

static const cmeta_type_identity MANAGED_ROW_IDENTITY =
    CMETA_TYPE_ID_ATOM_INIT("turboflow.test.ManagedRow");
static const cmeta_type_traits MANAGED_ROW_TRAITS = {.flags = CMETA_TRAIT_COPY | CMETA_TRAIT_MOVE |
                                                              CMETA_TRAIT_DESTROY,
                                                     .copy_construct = managed_row_copy,
                                                     .move_construct = managed_row_move,
                                                     .destroy = managed_row_destroy};
static const cmeta_type_desc MANAGED_ROW_TYPE = {.name = "managed_row_t",
                                                 .size = sizeof(managed_row_t),
                                                 .align = _Alignof(managed_row_t),
                                                 .kind = CMETA_T_OBJECT,
                                                 .traits = &MANAGED_ROW_TRAITS,
                                                 .identity = &MANAGED_ROW_IDENTITY};
static const cmeta_type_traits MISSING_LIFECYCLE_TRAITS = {0};
static const cmeta_type_desc MISSING_LIFECYCLE_TYPE = {.name = "missing_lifecycle_row_t",
                                                       .size = sizeof(int),
                                                       .align = _Alignof(int),
                                                       .kind = CMETA_T_OBJECT,
                                                       .traits = &MISSING_LIFECYCLE_TRAITS};
static const turbo_flow_data_schema_t MANAGED_ROW_SCHEMA = {
    .size = sizeof(turbo_flow_data_schema_t),
    .domain = TURBO_FLOW_DOMAIN_DATA,
    .encoding = TURBO_FLOW_DATA_ENCODING_TBE,
    .schema_name = "test.managed.row",
    .type_name = "ManagedRow",
    .projection_type = "managed_row_t",
    .schema_id = 4u,
    .schema_version = 1u};

int turbodb_adapter_header_cpp_probe(void);

typedef struct wait_source_s {
  size_t downstream_demand;
  cflow_waker terminal_waker;
  cflow_waker waker;
  size_t resumes;
  size_t arms;
  size_t wait_cancels;
  size_t source_cancels;
  size_t destroys;
  int value;
  bool ready;
  bool invalid_waitable;
} wait_source_t;

static bool wait_source_waitable_arm(void *state, cflow_waker waker) {
  wait_source_t *source = (wait_source_t *)state;
  source->waker = waker;
  ++source->arms;
  return true;
}

static void wait_source_waitable_cancel(void *state) {
  wait_source_t *source = (wait_source_t *)state;
  source->waker = (cflow_waker){0};
  ++source->wait_cancels;
}

CMETA_IMPLEMENTS(cflow_waitable, test_waitable, 0, .arm = wait_source_waitable_arm,
                 .cancel = wait_source_waitable_cancel);

static const char *wait_source_name(void *state) {
  (void)state;
  return "test-wait-source";
}

static const cmeta_type_desc *wait_source_type(void *state) {
  (void)state;
  return &cmeta_type_int;
}

static const cmeta_type_desc *missing_lifecycle_source_type(void *state) {
  (void)state;
  return &MISSING_LIFECYCLE_TYPE;
}

static cflow_step wait_source_resume(void *state, cflow_publish_context *context, void *out_value) {
  wait_source_t *source = (wait_source_t *)state;
  ++source->resumes;
  source->downstream_demand = context ? context->downstream_demand : 0u;
  if (source->invalid_waitable) return (cflow_step){CFLOW_STEP_WAIT, {0}, NULL};
  if (source->ready) {
    *(int *)out_value = source->value;
    return (cflow_step){CFLOW_STEP_VALUE_AND_DONE, {0}, NULL};
  }
  return (cflow_step){CFLOW_STEP_WAIT, test_waitable_as_cflow_waitable(source), NULL};
}

static void wait_source_cancel(void *state) {
  wait_source_t *source = (wait_source_t *)state;
  ++source->source_cancels;
}

static void wait_source_destroy(void *state) {
  wait_source_t *source = (wait_source_t *)state;
  ++source->destroys;
}

static void wait_source_bind_terminal_waker(void *state, cflow_waker waker) {
  ((wait_source_t *)state)->terminal_waker = waker;
}

static cflow_publisher_terminal wait_source_poll_terminal(void *state, const char **error) {
  (void)state;
  if (error) *error = NULL;
  return CFLOW_PUBLISHER_OPEN;
}

CMETA_IMPLEMENTS(cflow_publisher, wait_source, CFLOW_PUBLISHER_CAP_CONSTRUCTS_VALUES,
                 .name = wait_source_name, .output_type = wait_source_type,
                 .resume = wait_source_resume, .cancel = wait_source_cancel,
                 .destroy = wait_source_destroy,
                 .bind_terminal_waker = wait_source_bind_terminal_waker,
                 .poll_terminal = wait_source_poll_terminal);

CMETA_IMPLEMENTS(cflow_publisher, missing_lifecycle_source, CFLOW_PUBLISHER_CAP_CONSTRUCTS_VALUES,
                 .name = wait_source_name, .output_type = missing_lifecycle_source_type,
                 .resume = wait_source_resume, .cancel = wait_source_cancel,
                 .destroy = wait_source_destroy,
                 .bind_terminal_waker = wait_source_bind_terminal_waker,
                 .poll_terminal = wait_source_poll_terminal);

static turbo_flow_turbodb_source_config_t int_source_config(uint64_t first_id) {
  turbo_flow_turbodb_source_config_t config = turbo_flow_turbodb_source_config_default();
  config.projection_schema = &INT_SCHEMA;
  config.first_message_id = first_id;
  config.message_type = 17u;
  config.message_flags = 23u;
  return config;
}

static void ignore_wake(void *user) { (void)user; }

static void wait_source_signal(wait_source_t *source) {
  cflow_waker waker = source->waker;
  source->waker = (cflow_waker){0};
  waker.wake(waker.user);
}

static orm_connection_t *open_test_database_with_limits(orm_error_t *error, uint64_t max_rows,
                                                        uint64_t max_bytes) {
  orm_config_t config;
  orm_option_t filename;
  orm_connection_t *connection = NULL;

  orm_config(&config);
  filename.keyword = orm_view("filename");
  filename.value = orm_view(":memory:");
  config.driver = orm_view("sqlite");
  config.options = &filename;
  config.option_count = 1u;
  if (max_rows != 0u) config.max_result_rows = max_rows;
  if (max_bytes != 0u) config.max_result_bytes = max_bytes;
  check_equal(orm_connect(&config, &connection, error), ORM_STATUS_OK);
  return connection;
}

static orm_connection_t *open_test_database(orm_error_t *error) {
  return open_test_database_with_limits(error, 0u, 0u);
}

static void execute_test_sql(orm_connection_t *connection, const char *sql, orm_error_t *error) {
  orm_query_t *query = NULL;
  orm_result_t *result = NULL;

  check_equal(orm_raw(connection, orm_view(sql), &query, error), ORM_STATUS_OK);
  check_equal(orm_query_execute(query, &result, error), ORM_STATUS_OK);
  orm_result_destroy(result);
  orm_query_destroy(query);
}

typedef struct db_graph_probe_s {
  size_t count;
  uint64_t ids[2];
  int values[2];
} db_graph_probe_t;

typedef struct int_graph_probe_s {
  size_t count;
  uint64_t id;
  int value;
} int_graph_probe_t;

typedef struct count_graph_probe_s {
  size_t count;
} count_graph_probe_t;

static int db_graph_probe_stage(turbo_flow_msg_t *message, void *ctx) {
  db_graph_probe_t *probe = (db_graph_probe_t *)ctx;
  const test_db_row *row = (const test_db_row *)turbo_flow_msg_projection(message, NULL);
  if (!probe || !row || probe->count >= 2u) return SALTS_EPROTO;
  probe->ids[probe->count] = message->id;
  probe->values[probe->count] = row->id;
  ++probe->count;
  return SALTS_OK;
}

static int int_graph_probe_stage(turbo_flow_msg_t *message, void *ctx) {
  int_graph_probe_t *probe = (int_graph_probe_t *)ctx;
  const int *value = (const int *)turbo_flow_msg_projection(message, NULL);
  if (!probe || !value || probe->count != 0u) return SALTS_EPROTO;
  probe->id = message->id;
  probe->value = *value;
  ++probe->count;
  return SALTS_OK;
}

static int count_graph_probe_stage(turbo_flow_msg_t *message, void *ctx) {
  count_graph_probe_t *probe = (count_graph_probe_t *)ctx;
  if (!probe || !message) return SALTS_EPROTO;
  ++probe->count;
  return SALTS_OK;
}

static turbo_flow_t *open_graph(turbo_flow_stage_fn stage, void *stage_context) {
  static const char source[] = "source input\n"
                               "stage sink\n"
                               "stage main {\n"
                               "  input -> sink\n"
                               "}\n";
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || turbo_flow_parse_string(flow, source, strlen(source)) != SALTS_OK ||
      turbo_flow_register_stage_ex(flow, "sink", stage, stage_context, NULL) != SALTS_OK ||
      turbo_flow_compile(flow) != SALTS_OK || turbo_flow_start(flow) != SALTS_OK) {
    turbo_flow_destroy(flow);
    return NULL;
  }
  return flow;
}

typedef struct tidesdb_busy_fixture_s {
  char *path;
  orm_connection_t *connection;
  orm_transaction_t *transaction;
  orm_query_t *query;
  cflow_publisher messages;
  turbo_flow_t *flow;
  turbo_flow_run_t *run;
  cflow_scheduler scheduler;
  count_graph_probe_t probe;
  bool transaction_active;
  bool scheduler_initialized;
} tidesdb_busy_fixture_t;

static bool tidesdb_busy_fixture_open(tidesdb_busy_fixture_t *fixture, bool fail_binding,
                                      orm_error_t *error) {
  orm_option_t options[2];
  orm_config_t database_config;
  orm_flow_config_t flow_config;
  turbo_flow_turbodb_source_config_t source_config = turbo_flow_turbodb_source_config_default();
  turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
  cflow_publisher command_source = {0};
  orm_command_result_t command = ORM_COMMAND_RESULT_INIT;
  cflow_step step;

  if (!fixture || !error) return false;
  memset(fixture, 0, sizeof(*fixture));
  fixture->path = tt_make_temp_dir("turbo-flow-tidesdb-busy");
  if (!fixture->path) return false;

  orm_config(&database_config);
  options[0] = (orm_option_t){orm_view("path"), orm_view(fixture->path)};
  options[1] = (orm_option_t){orm_view("column_family"), orm_view("turbo_flow_busy")};
  database_config.driver = orm_view("tidesdb");
  database_config.options = options;
  database_config.option_count = 2u;
  if (orm_connect(&database_config, &fixture->connection, error) != ORM_STATUS_OK) return false;
  if (orm_transaction_begin(fixture->connection, ORM_ISOLATION_SERIALIZABLE, &fixture->transaction,
                            error) != ORM_STATUS_OK)
    return false;
  fixture->transaction_active = true;

  if (orm_insert(fixture->connection, orm_view("people"), &fixture->query, error) !=
          ORM_STATUS_OK ||
      orm_query_set(fixture->query, orm_view("id"), orm_i64(7), error) != ORM_STATUS_OK ||
      orm_query_set(fixture->query, orm_view("score"), orm_i64(19), error) != ORM_STATUS_OK ||
      orm_query_open_command_flow_in_transaction(fixture->query, fixture->transaction,
                                                 &command_source, error) != ORM_STATUS_OK)
    return false;
  step = cflow_publisher_resume(&command_source, NULL, &command);
  cflow_publisher_destroy(&command_source);
  if (step.kind != CFLOW_STEP_VALUE_AND_DONE || command.affected_rows != 1u) return false;
  orm_query_destroy(fixture->query);
  fixture->query = NULL;

  if (orm_query_create(fixture->connection, orm_view("people"), &fixture->query, error) !=
          ORM_STATUS_OK ||
      orm_query_add_column(fixture->query, orm_view("id"), error) != ORM_STATUS_OK ||
      orm_query_add_column(fixture->query, orm_view("score"), error) != ORM_STATUS_OK ||
      orm_query_where(fixture->query, orm_view("id"), ORM_COMPARE_EQUAL, orm_i64(7), error) !=
          ORM_STATUS_OK)
    return false;

  orm_flow_config(&flow_config, &TEST_DB_ROW_DATA);
  if (fail_binding) flow_config.scratch_bytes = 0u;
  source_config.projection_schema = &TEST_DB_ROW_SCHEMA;
  if (turbo_flow_turbodb_query_open_in_transaction(fixture->query, fixture->transaction,
                                                   &flow_config, &source_config, &fixture->messages,
                                                   error) != SALTS_OK)
    return false;

  fixture->flow = open_graph(count_graph_probe_stage, &fixture->probe);
  if (!fixture->flow || !cflow_scheduler_inline_init(&fixture->scheduler)) return false;
  fixture->scheduler_initialized = true;
  run_config.scheduler = &fixture->scheduler;
  if (turbo_flow_run_open(fixture->flow, "input", &fixture->messages, &run_config, &fixture->run) !=
      SALTS_OK)
    return false;
  return true;
}

static orm_status_t tidesdb_busy_fixture_commit(tidesdb_busy_fixture_t *fixture,
                                                orm_error_t *error) {
  const orm_status_t status = orm_transaction_commit(fixture->transaction, error);
  if (status == ORM_STATUS_OK) fixture->transaction_active = false;
  return status;
}

static void tidesdb_busy_fixture_close_run(tidesdb_busy_fixture_t *fixture) {
  if (!fixture || !fixture->run) return;
  turbo_flow_run_close(fixture->run);
  fixture->run = NULL;
}

static bool tidesdb_busy_fixture_destroy(tidesdb_busy_fixture_t *fixture) {
  bool clean = true;
  if (!fixture) return false;
  tidesdb_busy_fixture_close_run(fixture);
  if (cflow_publisher_valid(&fixture->messages)) cflow_publisher_destroy(&fixture->messages);
  if (fixture->scheduler_initialized) cflow_scheduler_destroy(&fixture->scheduler);
  if (fixture->query) orm_query_destroy(fixture->query);
  if (fixture->flow) {
    if (turbo_flow_stop(fixture->flow) != SALTS_OK) clean = false;
    turbo_flow_destroy(fixture->flow);
  }
  if (fixture->transaction) {
    if (fixture->transaction_active &&
        orm_transaction_rollback(fixture->transaction, NULL) != ORM_STATUS_OK)
      clean = false;
    orm_transaction_destroy(fixture->transaction);
  }
  if (fixture->connection) orm_disconnect(fixture->connection);
  if (fixture->path) {
    if (tt_remove_tree(fixture->path) != 0) clean = false;
    free(fixture->path);
  }
  memset(fixture, 0, sizeof(*fixture));
  return clean;
}

spec("TurboDb ORM Publisher adapter") {
  it("provides versioned source defaults") {
    turbo_flow_turbodb_source_config_t config = turbo_flow_turbodb_source_config_default();

    check_equal(config.size, sizeof(config));
    check_equal(config.version, TURBO_FLOW_TURBODB_API_VERSION);
    check_equal(config.first_message_id, 1u);
    check_equal(turbodb_adapter_header_cpp_probe(), 0);
  }

  it("moves typed rows into owned cloneable message projections") {
    const int rows[] = {7, 11};
    turbo_flow_turbodb_source_config_t config = int_source_config(41u);
    cflow_publish_context context = {.downstream_demand = 2u};
    cflow_publisher typed = {0};
    cflow_publisher messages = {0};
    turbo_flow_msg_t first;
    turbo_flow_msg_t cloned;
    turbo_flow_msg_t second;
    const turbo_flow_data_schema_t *schema = NULL;
    const int *projection;
    cflow_step step;

    check_true(cflow_publisher_from_array(&typed, &cmeta_type_int, rows, 2u));
    check_equal(turbo_flow_turbodb_publisher_wrap(&typed, &config, &messages), SALTS_OK);
    check_false(cflow_publisher_valid(&typed));
    check_true(cflow_publisher_valid(&messages));
    check_true(cmeta_type_equal(cflow_publisher_output_type(&messages), turbo_flow_message_type()));

    step = cflow_publisher_resume(&messages, &context, &first);
    check_equal(step.kind, CFLOW_STEP_VALUE);
    check_equal(first.id, 41u);
    check_equal(first.type, 17u);
    check_equal(first.flags, 23u);
    projection = (const int *)turbo_flow_msg_projection(&first, &schema);
    check_not_null(projection);
    check_equal(*projection, 7);
    check_true(schema == &INT_SCHEMA);

    check_equal(turbo_flow_msg_clone(&cloned, &first), SALTS_OK);
    projection = (const int *)turbo_flow_msg_projection(&cloned, &schema);
    check_not_null(projection);
    check_equal(*projection, 7);
    check_true(schema == &INT_SCHEMA);
    turbo_flow_msg_cleanup(&first);
    turbo_flow_msg_cleanup(&cloned);

    step = cflow_publisher_resume(&messages, &context, &second);
    check_equal(step.kind, CFLOW_STEP_VALUE_AND_DONE);
    check_equal(second.id, 42u);
    projection = (const int *)turbo_flow_msg_projection(&second, NULL);
    check_not_null(projection);
    check_equal(*projection, 11);
    turbo_flow_msg_cleanup(&second);
    cflow_publisher_destroy(&messages);
  }

  it("forwards wait demand terminal wakers cancellation and destruction") {
    turbo_flow_turbodb_source_config_t config = int_source_config(1u);
    wait_source_t source = {0};
    cflow_publisher typed = wait_source_as_cflow_publisher(&source);
    cflow_publisher messages = {0};
    cflow_publish_context context = {.downstream_demand = 19u};
    cflow_waker terminal_waker = {.wake = ignore_wake, .user = &source};
    turbo_flow_msg_t empty;
    cflow_step step;

    check_equal(turbo_flow_turbodb_publisher_wrap(&typed, &config, &messages), SALTS_OK);
    memset(&empty, 0, sizeof(empty));
    step = cflow_publisher_resume(&messages, &context, &empty);
    check_equal(step.kind, CFLOW_STEP_WAIT);
    check_equal(step.waitable.self, &source);
    check_equal(source.downstream_demand, 19u);

    cflow_publisher_bind_terminal_waker(&messages, terminal_waker);
    check_true(source.terminal_waker.wake == ignore_wake);
    check_equal(source.terminal_waker.user, &source);
    cflow_publisher_cancel(&messages);
    check_equal(source.source_cancels, 1u);
    cflow_publisher_destroy(&messages);
    check_equal(source.destroys, 1u);
  }

  it("wakes an adapted WAIT run once and preserves demand through the graph") {
    turbo_flow_turbodb_source_config_t config = int_source_config(301u);
    wait_source_t source = {.value = 47};
    int_graph_probe_t probe = {0};
    cflow_publisher typed = wait_source_as_cflow_publisher(&source);
    cflow_publisher messages = {0};
    cflow_scheduler scheduler = {0};
    turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    turbo_flow_run_t *run = NULL;
    turbo_flow_t *flow;

    check_equal(turbo_flow_turbodb_publisher_wrap(&typed, &config, &messages), SALTS_OK);
    flow = open_graph(int_graph_probe_stage, &probe);
    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    run_config.scheduler = &scheduler;
    check_equal(turbo_flow_run_open(flow, "input", &messages, &run_config, &run), SALTS_OK);
    check_equal(source.resumes, 0u);
    check_equal(source.arms, 0u);
    check_equal(probe.count, 0u);

    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check_equal(source.resumes, 1u);
    check_equal(source.arms, 1u);
    check_equal(source.downstream_demand, 1u);
    check_not_null(source.waker.wake);
    check_equal(turbo_flow_run_wait(run, 0u, &result), SALTS_ETIMEDOUT);
    check_equal(result.outstanding_demand, 1u);
    check_equal(probe.count, 0u);

    source.ready = true;
    wait_source_signal(&source);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_RUN_COMPLETED);
    check_equal(result.values, 1u);
    check_equal(source.resumes, 2u);
    check_equal(source.arms, 1u);
    check_equal(source.wait_cancels, 0u);
    check_null(source.waker.wake);
    check_equal(probe.count, 1u);
    check_equal(probe.id, 301u);
    check_equal(probe.value, 47);

    turbo_flow_run_close(run);
    check_equal(source.destroys, 1u);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("cancels an adapted active WAIT and inner Publisher exactly once") {
    turbo_flow_turbodb_source_config_t config = int_source_config(1u);
    wait_source_t source = {0};
    int_graph_probe_t probe = {0};
    cflow_publisher typed = wait_source_as_cflow_publisher(&source);
    cflow_publisher messages = {0};
    cflow_scheduler scheduler = {0};
    turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    turbo_flow_run_t *run = NULL;
    turbo_flow_t *flow;

    check_equal(turbo_flow_turbodb_publisher_wrap(&typed, &config, &messages), SALTS_OK);
    flow = open_graph(int_graph_probe_stage, &probe);
    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    run_config.scheduler = &scheduler;
    check_equal(turbo_flow_run_open(flow, "input", &messages, &run_config, &run), SALTS_OK);
    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check_equal(source.resumes, 1u);
    check_equal(source.arms, 1u);
    check_not_null(source.waker.wake);

    check_equal(turbo_flow_run_cancel(run), SALTS_OK);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_ECANCELED);
    check_equal(result.state, TURBO_FLOW_RUN_CANCELED);
    check_equal(result.values, 0u);
    check_equal(source.wait_cancels, 1u);
    check_equal(source.source_cancels, 1u);
    check_null(source.waker.wake);
    check_equal(probe.count, 0u);

    turbo_flow_run_close(run);
    check_equal(source.wait_cancels, 1u);
    check_equal(source.source_cancels, 1u);
    check_equal(source.destroys, 1u);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("fails an adapted run on invalid WAIT without polling or fabricating a value") {
    turbo_flow_turbodb_source_config_t config = int_source_config(1u);
    wait_source_t source = {.invalid_waitable = true};
    int_graph_probe_t probe = {0};
    cflow_publisher typed = wait_source_as_cflow_publisher(&source);
    cflow_publisher messages = {0};
    cflow_scheduler scheduler = {0};
    turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    turbo_flow_run_t *run = NULL;
    turbo_flow_t *flow;

    check_equal(turbo_flow_turbodb_publisher_wrap(&typed, &config, &messages), SALTS_OK);
    flow = open_graph(int_graph_probe_stage, &probe);
    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    run_config.scheduler = &scheduler;
    check_equal(turbo_flow_run_open(flow, "input", &messages, &run_config, &run), SALTS_OK);
    check_equal(turbo_flow_run_request(run, 1u), SALTS_EIO);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_EIO);
    check_equal(result.state, TURBO_FLOW_RUN_FAILED);
    check_equal(result.status, SALTS_EIO);
    check_contains(result.error.message, "WAIT step has no armable waitable");
    check_equal(result.values, 0u);
    check_equal(source.resumes, 1u);
    check_equal(source.arms, 0u);
    check_equal(source.wait_cancels, 0u);
    check_equal(probe.count, 0u);

    turbo_flow_run_close(run);
    check_equal(source.destroys, 1u);
    cflow_scheduler_destroy(&scheduler);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("uses managed CMeta copy and destroy traits for projections") {
    managed_row_t row = {0};
    turbo_flow_turbodb_source_config_t config = turbo_flow_turbodb_source_config_default();
    cflow_publisher typed = {0};
    cflow_publisher messages = {0};
    cflow_publish_context context = {.downstream_demand = 1u};
    turbo_flow_msg_t message;
    turbo_flow_msg_t clone;
    const managed_row_t *projection;
    cflow_step step;

    managed_row_live_allocations = 0u;
    row.value = (int *)malloc(sizeof(*row.value));
    check_not_null(row.value);
    *row.value = 37;
    ++managed_row_live_allocations;
    config.projection_schema = &MANAGED_ROW_SCHEMA;
    check_true(cflow_publisher_from_array(&typed, &MANAGED_ROW_TYPE, &row, 1u));
    check_equal(turbo_flow_turbodb_publisher_wrap(&typed, &config, &messages), SALTS_OK);
    step = cflow_publisher_resume(&messages, &context, &message);
    check_equal(step.kind, CFLOW_STEP_VALUE_AND_DONE);
    projection = (const managed_row_t *)turbo_flow_msg_projection(&message, NULL);
    check_not_null(projection);
    check_equal(*projection->value, 37);
    check_equal(turbo_flow_msg_clone(&clone, &message), SALTS_OK);
    projection = (const managed_row_t *)turbo_flow_msg_projection(&clone, NULL);
    check_not_null(projection);
    check_equal(*projection->value, 37);
    check_equal(managed_row_live_allocations, 3u);

    turbo_flow_msg_cleanup(&message);
    turbo_flow_msg_cleanup(&clone);
    cflow_publisher_destroy(&messages);
    managed_row_destroy(&row);
    check_equal(managed_row_live_allocations, 0u);
  }

  it("fails on message ID overflow only when another row arrives") {
    const int rows[] = {3, 5};
    turbo_flow_turbodb_source_config_t config = int_source_config(UINT64_MAX);
    cflow_publish_context context = {0};
    cflow_publisher typed = {0};
    cflow_publisher messages = {0};
    turbo_flow_msg_t message;
    cflow_step step;

    check_true(cflow_publisher_from_array(&typed, &cmeta_type_int, rows, 2u));
    check_equal(turbo_flow_turbodb_publisher_wrap(&typed, &config, &messages), SALTS_OK);
    step = cflow_publisher_resume(&messages, &context, &message);
    check_equal(step.kind, CFLOW_STEP_VALUE);
    check_equal(message.id, UINT64_MAX);
    turbo_flow_msg_cleanup(&message);
    step = cflow_publisher_resume(&messages, &context, &message);
    check_equal(step.kind, CFLOW_STEP_ERROR);
    check_not_null(step.error);
    cflow_publisher_destroy(&messages);
  }

  it("keeps source ownership when schema validation fails") {
    const int rows[] = {7};
    turbo_flow_data_schema_t wrong_schema = INT_SCHEMA;
    turbo_flow_turbodb_source_config_t config = int_source_config(1u);
    cflow_publisher typed = {0};
    cflow_publisher messages = {0};

    wrong_schema.projection_type = "double";
    config.projection_schema = &wrong_schema;
    check_true(cflow_publisher_from_array(&typed, &cmeta_type_int, rows, 1u));
    check_equal(turbo_flow_turbodb_publisher_wrap(&typed, &config, &messages), SALTS_EINVAL);
    check_true(cflow_publisher_valid(&typed));
    check_false(cflow_publisher_valid(&messages));
    cflow_publisher_destroy(&typed);
  }

  it("rejects a Publisher whose CMeta type lacks copy and destroy traits") {
    turbo_flow_data_schema_t schema = INT_SCHEMA;
    turbo_flow_turbodb_source_config_t config = int_source_config(1u);
    wait_source_t source = {0};
    cflow_publisher typed = missing_lifecycle_source_as_cflow_publisher(&source);
    cflow_publisher messages = {0};

    schema.projection_type = "missing_lifecycle_row_t";
    config.projection_schema = &schema;
    check_equal(turbo_flow_turbodb_publisher_wrap(&typed, &config, &messages), SALTS_EINVAL);
    check_true(cflow_publisher_valid(&typed));
    check_false(cflow_publisher_valid(&messages));
    cflow_publisher_destroy(&typed);
    check_equal(source.destroys, 1u);
  }

  it("opens native ORM row and command Publishers without materialization") {
    orm_error_t error;
    orm_connection_t *connection;
    orm_query_t *query = NULL;
    orm_flow_config_t flow_config;
    turbo_flow_turbodb_source_config_t config = turbo_flow_turbodb_source_config_default();
    cflow_publisher messages = {0};
    cflow_publish_context context = {.downstream_demand = 1u};
    db_graph_probe_t graph_probe = {0};
    turbo_flow_t *flow;
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t run_result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_scheduler scheduler = {0};
    turbo_flow_msg_t message;
    const orm_command_result_t *command_result;
    cflow_step step;

    orm_error_init(&error);
    connection = open_test_database(&error);
    check_not_null(connection);
    execute_test_sql(connection, "create table adapter_rows(id integer, score integer)", &error);
    execute_test_sql(connection, "insert into adapter_rows values(7, 19), (11, 29)", &error);

    check_equal(orm_raw(connection, orm_view("select id, score from adapter_rows order by id"),
                        &query, &error),
                ORM_STATUS_OK);
    orm_flow_config(&flow_config, &TEST_DB_ROW_DATA);
    config.projection_schema = &TEST_DB_ROW_SCHEMA;
    config.first_message_id = 100u;
    check_equal(turbo_flow_turbodb_query_open(query, &flow_config, &config, &messages, &error),
                SALTS_OK);
    flow = open_graph(db_graph_probe_stage, &graph_probe);
    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    run_config.scheduler = &scheduler;
    check_equal(turbo_flow_run_open(flow, "input", &messages, &run_config, &run), SALTS_OK);
    check_false(cflow_publisher_valid(&messages));

    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check_equal(turbo_flow_run_snapshot(run, &run_result), SALTS_OK);
    check_equal(run_result.values, 1u);
    check_equal(graph_probe.count, 1u);
    check_equal(graph_probe.ids[0], 100u);
    check_equal(graph_probe.values[0], 7);

    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check_equal(turbo_flow_run_snapshot(run, &run_result), SALTS_OK);
    check_equal(run_result.values, 2u);
    check_equal(graph_probe.count, 2u);
    check_equal(graph_probe.ids[1], 101u);
    check_equal(graph_probe.values[1], 11);

    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &run_result), SALTS_OK);
    check_equal(run_result.state, TURBO_FLOW_RUN_COMPLETED);
    check_equal(run_result.values, 2u);
    check_equal(graph_probe.count, 2u);
    turbo_flow_run_close(run);
    cflow_scheduler_destroy(&scheduler);
    orm_query_destroy(query);
    query = NULL;
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);

    check_equal(
        orm_raw(connection, orm_view("insert into adapter_rows values(13, 31)"), &query, &error),
        ORM_STATUS_OK);
    config.projection_schema = &TEST_DB_COMMAND_SCHEMA;
    config.first_message_id = 200u;
    check_equal(turbo_flow_turbodb_command_open(query, &config, &messages, &error), SALTS_OK);
    step = cflow_publisher_resume(&messages, &context, &message);
    check_equal(step.kind, CFLOW_STEP_VALUE_AND_DONE);
    command_result = (const orm_command_result_t *)turbo_flow_msg_projection(&message, NULL);
    check_not_null(command_result);
    check_equal(command_result->affected_rows, 1u);
    turbo_flow_msg_cleanup(&message);
    cflow_publisher_destroy(&messages);
    orm_query_destroy(query);
    orm_disconnect(connection);
  }

  it("accepts exact ORM row byte depth and scratch capacities through a Graph run") {
    orm_error_t error;
    orm_connection_t *connection;
    orm_query_t *query = NULL;
    orm_flow_config_t flow_config;
    turbo_flow_turbodb_source_config_t config = turbo_flow_turbodb_source_config_default();
    cflow_publisher messages = {0};
    db_graph_probe_t graph_probe = {0};
    turbo_flow_t *flow;
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_scheduler scheduler = {0};

    orm_error_init(&error);
    connection = open_test_database_with_limits(&error, 1u, sizeof(int64_t) * 2u);
    check_not_null(connection);
    check_equal(orm_raw(connection, orm_view("select 7 as id, 19 as score"), &query, &error),
                ORM_STATUS_OK);
    orm_flow_config(&flow_config, &TEST_DB_ROW_DATA);
    flow_config.scratch_bytes = 1u;
    flow_config.max_depth = 1u;
    config.projection_schema = &TEST_DB_ROW_SCHEMA;
    check_equal(turbo_flow_turbodb_query_open(query, &flow_config, &config, &messages, &error),
                SALTS_OK);
    flow = open_graph(db_graph_probe_stage, &graph_probe);
    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    run_config.scheduler = &scheduler;
    check_equal(turbo_flow_run_open(flow, "input", &messages, &run_config, &run), SALTS_OK);
    check_false(cflow_publisher_valid(&messages));

    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check_equal(turbo_flow_run_snapshot(run, &result), SALTS_OK);
    check_equal(result.values, 1u);
    check_equal(graph_probe.count, 1u);
    check_equal(graph_probe.values[0], 7);
    check_equal(turbo_flow_run_request(run, 1u), SALTS_OK);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_OK);
    check_equal(result.state, TURBO_FLOW_RUN_COMPLETED);
    check_equal(result.values, 1u);
    check_equal(graph_probe.count, 1u);

    turbo_flow_run_close(run);
    cflow_scheduler_destroy(&scheduler);
    orm_query_destroy(query);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    orm_disconnect(connection);
  }

  it("fails a Graph run when cumulative ORM row capacity is exceeded") {
    orm_error_t error;
    orm_connection_t *connection;
    orm_query_t *query = NULL;
    orm_flow_config_t flow_config;
    turbo_flow_turbodb_source_config_t config = turbo_flow_turbodb_source_config_default();
    cflow_publisher messages = {0};
    db_graph_probe_t graph_probe = {0};
    turbo_flow_t *flow;
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_scheduler scheduler = {0};

    orm_error_init(&error);
    connection = open_test_database_with_limits(&error, 1u, 0u);
    check_not_null(connection);
    check_equal(orm_raw(connection,
                        orm_view("select 7 as id, 19 as score "
                                 "union all select 11, 29"),
                        &query, &error),
                ORM_STATUS_OK);
    orm_flow_config(&flow_config, &TEST_DB_ROW_DATA);
    config.projection_schema = &TEST_DB_ROW_SCHEMA;
    check_equal(turbo_flow_turbodb_query_open(query, &flow_config, &config, &messages, &error),
                SALTS_OK);
    flow = open_graph(db_graph_probe_stage, &graph_probe);
    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    run_config.scheduler = &scheduler;
    check_equal(turbo_flow_run_open(flow, "input", &messages, &run_config, &run), SALTS_OK);

    check_equal(turbo_flow_run_request(run, 2u), SALTS_EIO);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_EIO);
    check_equal(result.state, TURBO_FLOW_RUN_FAILED);
    check_equal(result.status, SALTS_EIO);
    check_contains(result.error.message, "SQLite result exceeds configured bounds");
    check_equal(result.values, 1u);
    check_equal(graph_probe.count, 1u);
    check_equal(graph_probe.values[0], 7);

    turbo_flow_run_close(run);
    cflow_scheduler_destroy(&scheduler);
    orm_query_destroy(query);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    orm_disconnect(connection);
  }

  it("fails a Graph run before projection when ORM byte capacity is exceeded") {
    orm_error_t error;
    orm_connection_t *connection;
    orm_query_t *query = NULL;
    orm_flow_config_t flow_config;
    turbo_flow_turbodb_source_config_t config = turbo_flow_turbodb_source_config_default();
    cflow_publisher messages = {0};
    count_graph_probe_t graph_probe = {0};
    turbo_flow_t *flow;
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_scheduler scheduler = {0};

    orm_error_init(&error);
    connection = open_test_database_with_limits(&error, 0u, sizeof(int64_t) * 2u - 1u);
    check_not_null(connection);
    check_equal(orm_raw(connection, orm_view("select 7 as id, 19 as score"), &query, &error),
                ORM_STATUS_OK);
    orm_flow_config(&flow_config, &TEST_DB_ROW_DATA);
    config.projection_schema = &TEST_DB_ROW_SCHEMA;
    check_equal(turbo_flow_turbodb_query_open(query, &flow_config, &config, &messages, &error),
                SALTS_OK);
    flow = open_graph(count_graph_probe_stage, &graph_probe);
    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    run_config.scheduler = &scheduler;
    check_equal(turbo_flow_run_open(flow, "input", &messages, &run_config, &run), SALTS_OK);

    check_equal(turbo_flow_run_request(run, 1u), SALTS_EIO);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_EIO);
    check_equal(result.state, TURBO_FLOW_RUN_FAILED);
    check_equal(result.status, SALTS_EIO);
    check_contains(result.error.message, "SQLite result exceeds configured bounds");
    check_equal(result.values, 0u);
    check_equal(graph_probe.count, 0u);

    turbo_flow_run_close(run);
    cflow_scheduler_destroy(&scheduler);
    orm_query_destroy(query);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    orm_disconnect(connection);
  }

  it("fails a Graph run before projection when CBind scratch capacity is insufficient") {
    orm_error_t error;
    orm_connection_t *connection;
    orm_query_t *query = NULL;
    orm_flow_config_t flow_config;
    turbo_flow_turbodb_source_config_t config = turbo_flow_turbodb_source_config_default();
    cflow_publisher messages = {0};
    count_graph_probe_t graph_probe = {0};
    turbo_flow_t *flow;
    turbo_flow_run_t *run = NULL;
    turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    cflow_scheduler scheduler = {0};

    orm_error_init(&error);
    connection = open_test_database(&error);
    check_not_null(connection);
    check_equal(orm_raw(connection, orm_view("select 7 as id, 19 as score"), &query, &error),
                ORM_STATUS_OK);
    orm_flow_config(&flow_config, &TEST_DB_ROW_DATA);
    flow_config.scratch_bytes = 0u;
    config.projection_schema = &TEST_DB_ROW_SCHEMA;
    check_equal(turbo_flow_turbodb_query_open(query, &flow_config, &config, &messages, &error),
                SALTS_OK);
    flow = open_graph(count_graph_probe_stage, &graph_probe);
    check_not_null(flow);
    check_true(cflow_scheduler_inline_init(&scheduler));
    run_config.scheduler = &scheduler;
    check_equal(turbo_flow_run_open(flow, "input", &messages, &run_config, &run), SALTS_OK);

    check_equal(turbo_flow_run_request(run, 1u), SALTS_EIO);
    check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_EIO);
    check_equal(result.state, TURBO_FLOW_RUN_FAILED);
    check_equal(result.status, SALTS_EIO);
    check_contains(result.error.message, "row binding failed");
    check_equal(result.values, 0u);
    check_equal(graph_probe.count, 0u);

    turbo_flow_run_close(run);
    cflow_scheduler_destroy(&scheduler);
    orm_query_destroy(query);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
    orm_disconnect(connection);
  }

  it("distinguishes depth exhaustion from a nested row token mismatch before projection") {
    static const struct {
      size_t max_depth;
      cbind_status expected_status;
    } cases[] = {{1u, CBIND_LIMIT_EXCEEDED}, {2u, CBIND_TOKEN_MISMATCH}};
    for (size_t case_index = 0u; case_index < sizeof(cases) / sizeof(cases[0]); ++case_index) {
      orm_error_t error;
      orm_connection_t *connection;
      orm_query_t *query = NULL;
      orm_flow_config_t flow_config;
      turbo_flow_turbodb_source_config_t config = turbo_flow_turbodb_source_config_default();
      cflow_publisher messages = {0};
      count_graph_probe_t graph_probe = {0};
      turbo_flow_t *flow;
      turbo_flow_run_t *run = NULL;
      turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
      turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
      cflow_scheduler scheduler = {0};
      char expected_error[sizeof("row binding failed: cbind=-2147483648 ")];

      orm_error_init(&error);
      connection = open_test_database(&error);
      check_not_null(connection);
      check_equal(orm_raw(connection, orm_view("select 7 as inner"), &query, &error),
                  ORM_STATUS_OK);
      orm_flow_config(&flow_config, &TEST_DB_NESTED_ROW_DATA);
      flow_config.max_depth = cases[case_index].max_depth;
      config.projection_schema = &TEST_DB_NESTED_ROW_SCHEMA;
      check_equal(turbo_flow_turbodb_query_open(query, &flow_config, &config, &messages, &error),
                  SALTS_OK);
      flow = open_graph(count_graph_probe_stage, &graph_probe);
      check_not_null(flow);
      check_true(cflow_scheduler_inline_init(&scheduler));
      run_config.scheduler = &scheduler;
      check_equal(turbo_flow_run_open(flow, "input", &messages, &run_config, &run), SALTS_OK);

      check_equal(turbo_flow_run_request(run, 1u), SALTS_EIO);
      check_equal(turbo_flow_run_wait(run, UINT64_MAX, &result), SALTS_EIO);
      check_equal(result.state, TURBO_FLOW_RUN_FAILED);
      check_equal(result.status, SALTS_EIO);
      check_greater(snprintf(expected_error, sizeof(expected_error),
                             "row binding failed: cbind=%d ",
                             (int)cases[case_index].expected_status),
                    0);
      check_contains(result.error.message, expected_error);
      check_equal(result.values, 0u);
      check_equal(graph_probe.count, 0u);

      turbo_flow_run_close(run);
      cflow_scheduler_destroy(&scheduler);
      orm_query_destroy(query);
      check_equal(turbo_flow_stop(flow), SALTS_OK);
      turbo_flow_destroy(flow);
      orm_disconnect(connection);
    }
  }

  it("keeps a TidesDB transaction busy until the adapted run closes") {
    tidesdb_busy_fixture_t fixture;
    orm_error_t error;
    bool opened;

    orm_error_init(&error);
    opened = tidesdb_busy_fixture_open(&fixture, false, &error);
    check_true(opened);
    if (!opened) {
      check_true(tidesdb_busy_fixture_destroy(&fixture));
      return;
    }
    check_equal(tidesdb_busy_fixture_commit(&fixture, &error), ORM_STATUS_BUSY);
    check_contains(error.message, "close TidesDB row Publishers");

    tidesdb_busy_fixture_close_run(&fixture);
    check_equal(tidesdb_busy_fixture_commit(&fixture, &error), ORM_STATUS_OK);
    check_equal(fixture.probe.count, 0u);
    check_true(tidesdb_busy_fixture_destroy(&fixture));
  }

  it("releases a TidesDB transaction Publisher when its run is canceled") {
    tidesdb_busy_fixture_t fixture;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    orm_error_t error;
    bool opened;

    orm_error_init(&error);
    opened = tidesdb_busy_fixture_open(&fixture, false, &error);
    check_true(opened);
    if (!opened) {
      check_true(tidesdb_busy_fixture_destroy(&fixture));
      return;
    }
    check_equal(tidesdb_busy_fixture_commit(&fixture, &error), ORM_STATUS_BUSY);
    check_equal(turbo_flow_run_cancel(fixture.run), SALTS_OK);
    check_equal(turbo_flow_run_wait(fixture.run, UINT64_MAX, &result), SALTS_ECANCELED);
    check_equal(result.state, TURBO_FLOW_RUN_CANCELED);
    check_equal(tidesdb_busy_fixture_commit(&fixture, &error), ORM_STATUS_OK);
    check_equal(fixture.probe.count, 0u);
    check_true(tidesdb_busy_fixture_destroy(&fixture));
  }

  it("releases a TidesDB transaction Publisher when its run fails") {
    tidesdb_busy_fixture_t fixture;
    turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
    orm_error_t error;
    bool opened;

    orm_error_init(&error);
    opened = tidesdb_busy_fixture_open(&fixture, true, &error);
    check_true(opened);
    if (!opened) {
      check_true(tidesdb_busy_fixture_destroy(&fixture));
      return;
    }
    check_equal(tidesdb_busy_fixture_commit(&fixture, &error), ORM_STATUS_BUSY);
    check_equal(turbo_flow_run_request(fixture.run, 1u), SALTS_EIO);
    check_equal(turbo_flow_run_wait(fixture.run, UINT64_MAX, &result), SALTS_EIO);
    check_equal(result.state, TURBO_FLOW_RUN_FAILED);
    check_contains(result.error.message, "row binding failed");
    check_equal(tidesdb_busy_fixture_commit(&fixture, &error), ORM_STATUS_OK);
    check_equal(fixture.probe.count, 0u);
    check_true(tidesdb_busy_fixture_destroy(&fixture));
  }

  it("preserves transaction ownership for row and command Publishers") {
    orm_error_t error;
    orm_connection_t *connection;
    orm_transaction_t *transaction = NULL;
    orm_query_t *query = NULL;
    orm_flow_config_t flow_config;
    turbo_flow_turbodb_source_config_t config = turbo_flow_turbodb_source_config_default();
    cflow_publisher messages = {0};
    turbo_flow_msg_t message;
    const test_db_row *row;
    const orm_command_result_t *command_result;
    cflow_step step;

    orm_error_init(&error);
    connection = open_test_database(&error);
    check_not_null(connection);
    execute_test_sql(connection, "create table transaction_rows(id integer, score integer)",
                     &error);
    check_equal(orm_transaction_begin(connection, ORM_ISOLATION_SERIALIZABLE, &transaction, &error),
                ORM_STATUS_OK);

    check_equal(orm_raw(connection, orm_view("insert into transaction_rows values(17, 43)"), &query,
                        &error),
                ORM_STATUS_OK);
    config.projection_schema = &TEST_DB_COMMAND_SCHEMA;
    check_equal(turbo_flow_turbodb_command_open_in_transaction(query, transaction, &config,
                                                               &messages, &error),
                SALTS_OK);
    step = cflow_publisher_resume(&messages, NULL, &message);
    check_equal(step.kind, CFLOW_STEP_VALUE_AND_DONE);
    command_result = (const orm_command_result_t *)turbo_flow_msg_projection(&message, NULL);
    check_not_null(command_result);
    check_equal(command_result->affected_rows, 1u);
    turbo_flow_msg_cleanup(&message);
    cflow_publisher_destroy(&messages);
    messages = (cflow_publisher){0};
    orm_query_destroy(query);
    query = NULL;

    check_equal(
        orm_raw(connection, orm_view("select id, score from transaction_rows"), &query, &error),
        ORM_STATUS_OK);
    orm_flow_config(&flow_config, &TEST_DB_ROW_DATA);
    config.projection_schema = &TEST_DB_ROW_SCHEMA;
    check_equal(turbo_flow_turbodb_query_open_in_transaction(query, transaction, &flow_config,
                                                             &config, &messages, &error),
                SALTS_OK);
    step = cflow_publisher_resume(&messages, NULL, &message);
    check_true(step.kind == CFLOW_STEP_VALUE || step.kind == CFLOW_STEP_VALUE_AND_DONE);
    row = (const test_db_row *)turbo_flow_msg_projection(&message, NULL);
    check_not_null(row);
    check_equal(row->id, 17);
    check_equal(row->score, 43L);
    turbo_flow_msg_cleanup(&message);
    cflow_publisher_destroy(&messages);
    orm_query_destroy(query);

    check_equal(orm_transaction_rollback(transaction, &error), ORM_STATUS_OK);
    orm_transaction_destroy(transaction);
    orm_disconnect(connection);
  }
}
