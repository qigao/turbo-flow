#include "rulesforge_resource_fixture.h"
#include "tinytest.h"
#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_materializer.h"
#include "turbo_flow_rulesforge_plugin.h"

#include <cmeta_error.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct rulesforge_budget_s {
  uint32_t limit;
  uint32_t used;
} rulesforge_budget_t;

static int charge_budget(void *ctx, uint32_t steps) {
  rulesforge_budget_t *budget = (rulesforge_budget_t *)ctx;
  if (!budget || !steps || steps > budget->limit - budget->used) return SALTS_ENOSPC;
  budget->used += steps;
  return SALTS_OK;
}

static void normalize_path(char *path) {
  if (!path) return;
  for (size_t i = 0u; path[i] != '\0'; ++i)
    if (path[i] == '\\') path[i] = '/';
}

spec("RulesForge ABI3 provider") {
  it("loads an explicit RFL resource and evaluates isolated real sessions") {
    static const char schema_text[] =
        "schema TurboFlowRules [id(73), version(1), byte_order(little)]; "
        "message Applicant { int32 age; }";
    char *schema_path = tt_make_temp_file("turbo_flow_rulesforge_plugin", ".schema");
    char *rfl_path = tt_make_temp_file("turbo_flow_rulesforge_plugin", ".rfl");
    char rfl[2048];
    turbo_flow_plugin_host_config_t host_config = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
    turbo_flow_plugin_error_t plugin_error = TURBO_FLOW_PLUGIN_ERROR_INIT;
    turbo_flow_config_error_t config_error = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_plugin_host_t *host = NULL;
    turbo_flow_plugin_catalog_snapshot_t *snapshot = NULL;
    turbo_flow_plugin_operation_catalog_v3_t catalog;
    turbo_flow_plugin_materializer_catalog_v1_t materializers =
        TURBO_FLOW_PLUGIN_MATERIALIZER_CATALOG_V1_INIT;
    turbo_flow_plugin_materializer_input_v1_t encoded =
        TURBO_FLOW_PLUGIN_MATERIALIZER_INPUT_V1_INIT;
    rulesforge_resource_fixture_t deployment = {0};
    turbo_flow_resource_binding_t *resource = NULL;
    turbo_flow_provider_resource_view_v1_t resource_view =
        TURBO_FLOW_PROVIDER_RESOURCE_VIEW_V1_INIT;
    turbo_flow_plugin_operation_request_v3_t request;
    turbo_flow_plugin_operation_error_v3_t operation_error;
    turbo_flow_plugin_operation_input_v3_t input;
    turbo_flow_plugin_operation_budget_v3_t operation_budget;
    turbo_flow_rulesforge_applicant applicant = {0};
    turbo_flow_rulesforge_applicant decoded = {0};
    turbo_flow_rulesforge_decision *decision = NULL;
    rulesforge_budget_t budget = {16u, 0u};
    void *result_context = NULL;
    void *session = NULL;
    int count;

    check_not_null(schema_path);
    check_not_null(rfl_path);
    normalize_path(schema_path);
    normalize_path(rfl_path);
    check_equal(tt_write_file(schema_path, schema_text, sizeof(schema_text) - 1u), 0);

    count = snprintf(rfl, sizeof(rfl),
                     "import \"%s\";\n"
                     "rule \"Adult applicant\"\n"
                     "when\n"
                     "  Applicant(age >= 18)\n"
                     "then\n"
                     "end\n",
                     schema_path);
    check_greater(count, 0);
    check_less(count, (int)sizeof(rfl));
    check_equal(tt_write_file(rfl_path, rfl, (size_t)count), 0);

    check_equal(rulesforge_resource_fixture_open(&deployment, rfl_path), SALTS_OK);
    /* Deployment has frozen the KB: operation admission/execution must not reopen artifacts. */
    check_equal(tt_remove_file(rfl_path), 0);
    check_equal(tt_remove_file(schema_path), 0);

    const char *plugin_path = getenv("FLOW_RULESFORGE_PLUGIN_PATH");
    if (!plugin_path || !plugin_path[0]) plugin_path = FLOW_RULESFORGE_PLUGIN;

    check_equal(turbo_flow_plugin_host_create(&host_config, &host, &plugin_error), SALTS_OK);
    check_equal(turbo_flow_plugin_host_load(host, plugin_path, &plugin_error), SALTS_OK);
    check_equal(turbo_flow_plugin_catalog_snapshot_create(host, &snapshot, &plugin_error), SALTS_OK);
    turbo_flow_plugin_operation_catalog_v3_init(&catalog);
    check_equal(turbo_flow_plugin_catalog_snapshot_operation_catalog(snapshot, &catalog), SALTS_OK);
    check_equal(catalog.count, (size_t)1u);
    check_equal(catalog.entries[0].plugin_id, TURBO_FLOW_RULESFORGE_PLUGIN_ID);
    check_equal(catalog.entries[0].operation.operation_name, TURBO_FLOW_RULESFORGE_OPERATION);
    check_equal(catalog.entries[0].operation.input.data->stable_id,
                TURBO_FLOW_RULESFORGE_INPUT_SCHEMA_ID);
    check_equal(catalog.entries[0].operation.output.data->stable_id,
                TURBO_FLOW_RULESFORGE_OUTPUT_SCHEMA_ID);
    check_equal(catalog.entries[0].operation.input.projection->encoding,
                TURBO_FLOW_DATA_ENCODING_JSON);

    check_equal(turbo_flow_plugin_catalog_snapshot_materializer_catalog(
                    snapshot, &materializers),
                SALTS_OK);
    check_equal(materializers.count, (size_t)1u);
    check_equal(materializers.entries[0].plugin_id, TURBO_FLOW_RULESFORGE_PLUGIN_ID);
    check_equal(materializers.entries[0].materializer.schema.encoding,
                TURBO_FLOW_DATA_ENCODING_JSON);
    check_equal(strcmp(materializers.entries[0].materializer.schema.schema_name,
                       TURBO_FLOW_RULESFORGE_INPUT_SCHEMA_ID),
                0);
    check_equal(materializers.entries[0].materializer.native_bytes, sizeof(decoded));
    check_not_null(materializers.entries[0].materializer.materialize);

    encoded.data = "{\"age\":21}";
    encoded.data_size = strlen((const char *)encoded.data);
    check_equal(materializers.entries[0].materializer.materialize(
                    materializers.entries[0].materializer.ctx, &encoded,
                    &decoded, sizeof(decoded)),
                SALTS_OK);
    check_equal(decoded.age, 21);

    decoded.age = 0;
    encoded.data = "{\"age\":}";
    encoded.data_size = strlen((const char *)encoded.data);
    check_equal(materializers.entries[0].materializer.materialize(
                    materializers.entries[0].materializer.ctx, &encoded,
                    &decoded, sizeof(decoded)),
                SALTS_EPROTO);
    check_equal(decoded.age, 0);

    check_not_null(catalog.entries[0].operation.resource_requirement);
    check_equal(turbo_flow_resource_binding_acquire(
                    &deployment.resolver, "rules_adult",
                    catalog.entries[0].operation.resource_requirement, &resource, &config_error),
                SALTS_OK);
    check_equal(turbo_flow_resource_binding_view(resource, &resource_view), SALTS_OK);
    turbo_flow_plugin_operation_request_v3_init(&request);
    request.resource = &resource_view;
    request.operation_name = TURBO_FLOW_RULESFORGE_OPERATION;
    request.resource_name = "rules_adult";
    request.limits.max_inflight = 2u;
    request.limits.max_input_bytes = sizeof(applicant);
    request.limits.max_result_bytes = sizeof(*decision);
    request.limits.max_retained_bytes = 2u * sizeof(*decision);
    request.limits.max_steps = budget.limit;
    turbo_flow_plugin_operation_error_v3_init(&operation_error);

    check_equal(catalog.entries[0].operation.preflight(
                    catalog.entries[0].operation.factory_ctx, &request, &operation_error),
                SALTS_OK);
    check_equal(catalog.entries[0].operation.create_result_context(
                    catalog.entries[0].operation.factory_ctx, &request, &result_context,
                    &operation_error),
                SALTS_OK);
    check_not_null(result_context);
    check_equal(catalog.entries[0].operation.create_session(
                    catalog.entries[0].operation.factory_ctx, &request, result_context, &session,
                    &operation_error),
                SALTS_OK);
    check_not_null(session);

    turbo_flow_plugin_operation_input_v3_init(&input);
    input.value = &applicant;
    input.data = &turbo_flow_rulesforge_applicant_data;
    input.bytes = sizeof(applicant);
    turbo_flow_plugin_operation_budget_v3_init(&operation_budget);
    operation_budget.ctx = &budget;
    operation_budget.charge = charge_budget;

    applicant.age = 21;
    check_equal(catalog.entries[0].operation.vtable.execute(
                    session, &input, &operation_budget, (void **)&decision, &operation_error),
                SALTS_OK);
    check_not_null(decision);
    check_equal(decision->matched, 1);
    check_equal(decision->fired, 1);
    catalog.entries[0].operation.vtable.destroy_result(decision, result_context);
    decision = NULL;

    budget.used = 0u;
    applicant.age = 17;
    check_equal(catalog.entries[0].operation.vtable.execute(
                    session, &input, &operation_budget, (void **)&decision, &operation_error),
                SALTS_OK);
    check_not_null(decision);
    check_equal(decision->matched, 0);
    check_equal(decision->fired, 0);
    catalog.entries[0].operation.vtable.destroy_result(decision, result_context);
    decision = NULL;

    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &plugin_error), SALTS_EBUSY);
    check_equal(catalog.entries[0].operation.vtable.release_session(session), SALTS_OK);
    session = NULL;
    check_equal(catalog.entries[0].operation.vtable.release_result_context(result_context),
                SALTS_OK);
    result_context = NULL;
    check_equal(turbo_flow_resource_binding_release(&resource), SALTS_OK);
    check_equal(rulesforge_resource_fixture_close(&deployment), SALTS_OK);
    turbo_flow_plugin_catalog_snapshot_destroy(snapshot);
    snapshot = NULL;
    check_equal(turbo_flow_plugin_host_destroy(host, 0u, &plugin_error), SALTS_OK);
    host = NULL;

    free(rfl_path);
    free(schema_path);
  }
}
