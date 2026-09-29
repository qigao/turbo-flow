#include "flow_internal.h"
#include "tinytest.h"
#include "turbo_flow.h"
#include "turbo_flow_domain.h"

#include <cflow/function_projection.h>
#include <cmeta/data.h>
#include <cmeta/function.h>

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct reflected_managed_value_s {
  int *value;
} reflected_managed_value_t;

static size_t reflected_managed_copies;
static size_t reflected_managed_moves;
static size_t reflected_managed_destroys;
static size_t reflected_managed_live_resources;

static void reflected_managed_reset(void) {
  reflected_managed_copies = 0u;
  reflected_managed_moves = 0u;
  reflected_managed_destroys = 0u;
  reflected_managed_live_resources = 0u;
}

static reflected_managed_value_t reflected_managed_make(int value) {
  reflected_managed_value_t result = {0};
  result.value = (int *)malloc(sizeof(*result.value));
  if (result.value) {
    *result.value = value;
    ++reflected_managed_live_resources;
  }
  return result;
}

static bool reflected_managed_copy(void *destination, const void *source) {
  reflected_managed_value_t *dst = (reflected_managed_value_t *)destination;
  const reflected_managed_value_t *src = (const reflected_managed_value_t *)source;
  if (!dst || !src) return false;
  dst->value = NULL;
  if (src->value) {
    dst->value = (int *)malloc(sizeof(*dst->value));
    if (!dst->value) return false;
    *dst->value = *src->value;
    ++reflected_managed_live_resources;
  }
  ++reflected_managed_copies;
  return true;
}

static void reflected_managed_move(void *destination, void *source) {
  reflected_managed_value_t *dst = (reflected_managed_value_t *)destination;
  reflected_managed_value_t *src = (reflected_managed_value_t *)source;
  if (!dst || !src) return;
  dst->value = src->value;
  src->value = NULL;
  ++reflected_managed_moves;
}

static void reflected_managed_destroy(void *value) {
  reflected_managed_value_t *managed = (reflected_managed_value_t *)value;
  if (!managed) return;
  if (managed->value) {
    free(managed->value);
    managed->value = NULL;
    if (reflected_managed_live_resources > 0u) --reflected_managed_live_resources;
  }
  ++reflected_managed_destroys;
}

static const cmeta_type_traits reflected_managed_traits = {
    .flags = CMETA_TRAIT_COPY | CMETA_TRAIT_MOVE | CMETA_TRAIT_DESTROY,
    .copy_construct = reflected_managed_copy,
    .move_construct = reflected_managed_move,
    .destroy = reflected_managed_destroy};

static const cmeta_type_desc reflected_managed_type = {
    "reflected_managed_value",
    sizeof(reflected_managed_value_t),
    _Alignof(reflected_managed_value_t),
    CMETA_T_OBJECT,
    NULL,
    &reflected_managed_traits,
    NULL};

static const cmeta_type_desc reflected_unknown_lifecycle_type = {
    "reflected_unknown_lifecycle",
    sizeof(reflected_managed_value_t),
    _Alignof(reflected_managed_value_t),
    CMETA_T_OBJECT,
    NULL,
    NULL,
    NULL};

static const cmeta_struct_desc reflected_managed_layout = {
    "reflected_managed_value",
    sizeof(reflected_managed_value_t),
    _Alignof(reflected_managed_value_t),
    NULL,
    0u};

static const cmeta_data_struct_shape reflected_managed_shape = {
    &reflected_managed_layout, NULL, 0u};

static const cmeta_data_desc reflected_managed_data = {
    .struct_size = sizeof(cmeta_data_desc),
    .abi_version = CMETA_DATA_DESC_ABI_VERSION,
    .stable_id = "test.reflected.managed",
    .display_name = "Managed test value",
    .kind = CMETA_DATA_STRUCT,
    .storage_type = &reflected_managed_type,
    .shape = &reflected_managed_shape};

static int reflected_managed_projection_clone(
    const void *value, void *ctx, void **out) {
  reflected_managed_value_t *copy;
  (void)ctx;
  if (out) *out = NULL;
  if (!value || !out) return SALTS_EINVAL;
  copy = (reflected_managed_value_t *)malloc(sizeof(*copy));
  if (!copy) return SALTS_ENOMEM;
  if (!reflected_managed_copy(copy, value)) {
    free(copy);
    return SALTS_ENOMEM;
  }
  *out = copy;
  return SALTS_OK;
}

static void reflected_managed_projection_destroy(void *value, void *ctx) {
  (void)ctx;
  if (!value) return;
  reflected_managed_destroy(value);
  free(value);
}

FunctionDecl(value, int, reflected_increment,
    (int, input, CMETA_PARAM_IN));
int reflected_increment(int input) { return input + 1; }
CFLOW_REFLECTED_ADAPTER(reflected_increment);

FunctionDecl(stateful, int, reflected_stateful,
    (int, input, CMETA_PARAM_IN));
int reflected_stateful(int input) { return input; }
CFLOW_REFLECTED_ADAPTER(reflected_stateful);

FunctionDecl(async, int, reflected_async,
    (int, input, CMETA_PARAM_IN));
int reflected_async(int input) { return input; }
CFLOW_REFLECTED_ADAPTER(reflected_async);

FunctionDecl(io, int, reflected_io,
    (int, input, CMETA_PARAM_IN));
int reflected_io(int input) { return input; }
CFLOW_REFLECTED_ADAPTER(reflected_io);

FunctionDecl(unknown, int, reflected_unknown,
    (int, input, CMETA_PARAM_IN));
int reflected_unknown(int input) { return input; }
CFLOW_REFLECTED_ADAPTER(reflected_unknown);

FunctionDecl(fallible, void, reflected_join_out,
    (int, left, CMETA_PARAM_IN),
    (int, right, CMETA_PARAM_IN),
    (int *, output, CMETA_PARAM_OUT,
     &cmeta_type_int_ptr, CMETA_ABI_OBJECT_POINTER));
void reflected_join_out(int left, int right, int *output) {
  if (output) *output = left + right;
}

FunctionDecl(value, void, reflected_increment_out,
    (int, input, CMETA_PARAM_IN),
    (int *, output, CMETA_PARAM_OUT,
     &cmeta_type_int_ptr, CMETA_ABI_OBJECT_POINTER));
static size_t reflected_increment_out_calls = 0u;
static size_t reflected_increment_out_alias_calls = 0u;
void reflected_increment_out(int input, int *output) {
  if (!output) return;
  ++reflected_increment_out_calls;
  *output = input + 1;
}

static bool reflected_increment_out_invoke(
    const cmeta_callable *self, void *out, const void *const *args) {
  const cmeta_function_desc *function = FunctionMeta(reflected_increment_out);
  (void)self;
  if (!out || !args || !args[0] || !function) return false;
  if (out == args[0]) ++reflected_increment_out_alias_calls;
  reflected_increment_out(*(const int *)args[0], (int *)out);
  return true;
}

static cmeta_callable reflected_increment_out_adapter(void) {
  cmeta_callable adapter = {0};
  const cmeta_function_desc *function = FunctionMeta(reflected_increment_out);
  adapter.meta.effects = function->effects;
  adapter.meta.properties = function->properties;
  adapter.invoke = reflected_increment_out_invoke;
  adapter.dispatch = CMETA_CALLABLE_DISPATCH_ADAPTER;
  return adapter;
}
FunctionDecl(value, void, reflected_inout,
    (int *, value, CMETA_PARAM_INOUT,
     &cmeta_type_int_ptr, CMETA_ABI_OBJECT_POINTER));
void reflected_inout(int *value) {
  if (value) ++*value;
}

static bool reflected_inout_invoke(
    const cmeta_callable *self, void *out, const void *const *args) {
  (void)self;
  if (!out || !args || !args[0]) return false;
  *(int *)out = *(const int *)args[0] + 1;
  return true;
}

static cmeta_callable reflected_inout_adapter(void) {
  cmeta_callable adapter = {0};
  const cmeta_function_desc *function = FunctionMeta(reflected_inout);
  adapter.meta.effects = function->effects;
  adapter.meta.properties = function->properties;
  adapter.invoke = reflected_inout_invoke;
  adapter.dispatch = CMETA_CALLABLE_DISPATCH_ADAPTER;
  return adapter;
}

static int reflected_noop_stage(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return SALTS_OK;
}

static turbo_flow_operation_descriptor_t
reflected_operation_descriptor(const char *name) {
  turbo_flow_operation_descriptor_t operation;
  memset(&operation, 0, sizeof(operation));
  operation.size = sizeof(operation);
  operation.name = name;
  operation.version = 1u;
  operation.domain = TURBO_FLOW_DOMAIN_DATA;
  operation.scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
  operation.scope.state = TURBO_FLOW_STATE_SCOPE_NONE;
  operation.scope.lifetime = TURBO_FLOW_LIFETIME_DISPATCH;
  operation.scope.concurrency = TURBO_FLOW_CONCURRENCY_INLINE_LANE;
  operation.scope.authority = TURBO_FLOW_AUTHORITY_PURE;
  operation.flags = TURBO_FLOW_OPERATION_STAGE;
  operation.execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  operation.runtime.error_mode = TURBO_FLOW_ERROR_PROPAGATE;
  return operation;
}

static turbo_flow_operation_provider_registration_t
reflected_provider(const char *operation_name) {
  turbo_flow_operation_provider_registration_t provider =
      TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
  provider.operation_name = operation_name;
  provider.fn = reflected_noop_stage;
  return provider;
}

static turbo_flow_operation_port_binding_t
reflected_param_port(uint32_t port_index,
                     turbo_flow_operation_port_direction_t direction,
                     size_t parameter_index,
                     turbo_flow_operation_storage_t storage) {
  turbo_flow_operation_port_binding_t port =
      TURBO_FLOW_OPERATION_PORT_BINDING_INIT;
  port.port_index = port_index;
  port.domain = TURBO_FLOW_DOMAIN_DATA;
  port.direction = direction;
  port.value_kind = TURBO_FLOW_OPERATION_VALUE_PARAMETER;
  port.storage = storage;
  port.parameter_index = parameter_index;
  port.data = &cmeta_data_int;
  return port;
}

static turbo_flow_operation_port_binding_t
reflected_return_port(uint32_t port_index) {
  turbo_flow_operation_port_binding_t port =
      TURBO_FLOW_OPERATION_PORT_BINDING_INIT;
  port.port_index = port_index;
  port.domain = TURBO_FLOW_DOMAIN_DATA;
  port.direction = TURBO_FLOW_OPERATION_PORT_OUTPUT;
  port.value_kind = TURBO_FLOW_OPERATION_VALUE_RETURN;
  port.storage = TURBO_FLOW_OPERATION_STORAGE_DIRECT;
  port.parameter_index = SIZE_MAX;
  port.data = &cmeta_data_int;
  return port;
}

static int register_unary_reflected_mode(
    turbo_flow_t *flow,
    turbo_flow_operation_descriptor_t *operation,
    const cmeta_function_desc *function,
    const cmeta_function_abi_desc *abi,
    cmeta_callable adapter,
    int register_provider) {
  turbo_flow_operation_port_binding_t ports[2];
  turbo_flow_reflected_operation_registration_t registration =
      TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;
  turbo_flow_operation_provider_registration_t provider;

  ports[0] = reflected_param_port(
      0u, TURBO_FLOW_OPERATION_PORT_INPUT, 0u,
      TURBO_FLOW_OPERATION_STORAGE_DIRECT);
  ports[1] = reflected_return_port(0u);

  registration.operation = operation;
  registration.function = function;
  registration.abi = abi;
  registration.adapter = adapter;
  registration.ports = ports;
  registration.port_count = 2u;
  registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP;

  if (turbo_flow_register_reflected_operation(flow, &registration) != SALTS_OK)
    return 0;
  if (!register_provider) return 1;
  provider = reflected_provider(operation->name);
  return turbo_flow_register_operation_provider(flow, &provider) == SALTS_OK;
}

static int register_unary_reflected(
    turbo_flow_t *flow,
    turbo_flow_operation_descriptor_t *operation,
    const cmeta_function_desc *function,
    const cmeta_function_abi_desc *abi,
    cmeta_callable adapter) {
  return register_unary_reflected_mode(
      flow, operation, function, abi, adapter, 1);
}

typedef struct reflected_runtime_sink_probe_s {
  size_t calls;
  int value;
} reflected_runtime_sink_probe_t;

static int reflected_runtime_sink(turbo_flow_msg_t *msg, void *ctx) {
  reflected_runtime_sink_probe_t *probe =
      (reflected_runtime_sink_probe_t *)ctx;
  const cmeta_data_desc *data = turbo_flow_msg_projection_data(msg);
  const int *value = (const int *)turbo_flow_msg_projection(msg, NULL);
  if (!probe || !value || !data ||
      !cmeta_data_desc_equal(data, &cmeta_data_int))
    return SALTS_EPROTO;
  ++probe->calls;
  probe->value = *value;
  return SALTS_OK;
}

static int reflected_test_clone_int(const void *value, void *ctx, void **out) {
  int *copy;
  (void)ctx;
  if (!value || !out) return SALTS_EINVAL;
  *out = NULL;
  copy = (int *)malloc(sizeof(*copy));
  if (!copy) return SALTS_ENOMEM;
  *copy = *(const int *)value;
  *out = copy;
  return SALTS_OK;
}

static void reflected_test_destroy_int(void *value, void *ctx) {
  (void)ctx;
  free(value);
}

static void reflected_test_stage_observer(
    void *ctx, const char *stage_name, const char *adapter_name,
    const turbo_flow_msg_t *message, uint64_t duration_ns, int status) {
  (void)ctx;
  (void)stage_name;
  (void)adapter_name;
  (void)message;
  (void)duration_ns;
  (void)status;
}

static int register_native_int_stage(
    turbo_flow_t *flow, const char *name, turbo_flow_stage_fn fn, void *ctx) {
  turbo_flow_operation_descriptor_t operation =
      reflected_operation_descriptor(name);
  turbo_flow_operation_provider_registration_t provider =
      TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;

  operation.input_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.input_type = cmeta_data_int.stable_id;
  operation.output_domain = TURBO_FLOW_DOMAIN_DATA;
  operation.output_type = cmeta_data_int.stable_id;
  if (turbo_flow_register_operation(flow, &operation) != SALTS_OK) return 0;
  provider.operation_name = name;
  provider.fn = fn;
  provider.ctx = ctx;
  return turbo_flow_register_operation_provider(flow, &provider) == SALTS_OK;
}

static int bind_test_int_projection(
    turbo_flow_msg_t *message, const turbo_flow_data_schema_t *schema, int value) {
  int *storage = (int *)malloc(sizeof(*storage));
  int rc;
  if (!storage) return SALTS_ENOMEM;
  *storage = value;
  rc = turbo_flow_msg_bind_typed_projection(
      message, schema, &cmeta_data_int, storage,
      reflected_test_clone_int, reflected_test_destroy_int, NULL);
  if (rc != SALTS_OK) free(storage);
  return rc;
}

typedef struct reflected_batch_prepare_probe_s {
  const turbo_flow_data_schema_t *schema;
  size_t calls;
  size_t clone_calls;
  size_t fail_index;
  int fail_status;
} reflected_batch_prepare_probe_t;

static int reflected_batch_clone_forbidden(
    const void *value, void *ctx, void **out) {
  reflected_batch_prepare_probe_t *probe =
      (reflected_batch_prepare_probe_t *)ctx;
  (void)value;
  if (out) *out = NULL;
  if (probe) ++probe->clone_calls;
  return SALTS_EIO;
}

static int reflected_batch_prepare(
    void *ctx, size_t index, turbo_flow_msg_t *message) {
  reflected_batch_prepare_probe_t *probe =
      (reflected_batch_prepare_probe_t *)ctx;
  int *storage;
  int rc;
  if (!probe || !probe->schema || !message) return SALTS_EINVAL;
  ++probe->calls;
  if (index == probe->fail_index) return probe->fail_status;
  storage = (int *)malloc(sizeof(*storage));
  if (!storage) return SALTS_ENOMEM;
  *storage = (int)index;
  rc = turbo_flow_msg_bind_typed_projection(
      message, probe->schema, &cmeta_data_int, storage,
      reflected_batch_clone_forbidden, reflected_test_destroy_int, probe);
  if (rc != SALTS_OK) free(storage);
  return rc;
}

static const flow_stage_semantic_plan_t *
compile_single_reflected_stage(turbo_flow_t *flow, const char *operation_name) {
  static const char prefix[] =
      "source input\n"
      "stage reflected operation ";
  static const char suffix[] =
      "\n"
      "stage main {\n"
      "  input -> reflected\n"
      "}\n";
  char source[512];
  int stage_index;

  if (!flow || !operation_name ||
      snprintf(source, sizeof(source), "%s%s%s",
               prefix, operation_name, suffix) < 0)
    return NULL;
  if (turbo_flow_parse_string(flow, source, strlen(source)) != SALTS_OK)
    return NULL;
  {
    int compile_rc = turbo_flow_compile(flow);
    if (compile_rc != SALTS_OK) {
      const turbo_flow_error_t *error = turbo_flow_last_error(flow);
      fprintf(stderr, "reflected compile failed: rc=%d error=%d message=%s\n",
              compile_rc, error ? error->code : 0,
              error ? error->message : "<none>");
      return NULL;
    }
  }
  stage_index = turbo_flow_find_stage(flow, "reflected");
  if (stage_index < 0) return NULL;
  return (const flow_stage_semantic_plan_t *)vec_at_const(
      &flow->compiled_plan.stage_semantics, (size_t)stage_index);
}


static void reflected_plan_resource_release(void *ctx) {
  size_t *releases = (size_t *)ctx;
  if (releases) ++*releases;
}

suite("TurboFlow reflected operation semantics") {
  it("classifies CMeta slot lifecycle without granting unsafe reuse") {
    flow_cflow_value_slot_plan_t slot = {0};

    check_equal(flow_cflow_value_slot_plan_classify(
                    &cmeta_type_int, &cmeta_type_int, &slot),
                SALTS_OK);
    check_equal(slot.mode, FLOW_CFLOW_VALUE_SLOT_REUSE_INPUT);
    check_equal(slot.transfer, FLOW_CFLOW_VALUE_TRANSFER_TRIVIAL_COPY);
    check_equal(slot.extent, sizeof(int));
    check_equal(slot.alignment, cmeta_type_int.align);
    check_bits(slot.available_traits,
               CMETA_TRAIT_TRIVIAL_COPY | CMETA_TRAIT_TRIVIAL_DESTROY);
    check_equal(slot.required_traits,
                (cmeta_trait_flags)(CMETA_TRAIT_TRIVIAL_COPY |
                                    CMETA_TRAIT_TRIVIAL_DESTROY));
    check_equal(slot.source_destroy_after_transfer, 0);

    memset(&slot, 0, sizeof(slot));
    check_equal(flow_cflow_value_slot_plan_classify(
                    &reflected_managed_type, &reflected_managed_type, &slot),
                SALTS_OK);
    check_equal(slot.mode, FLOW_CFLOW_VALUE_SLOT_NONE);
    check_equal(slot.transfer, FLOW_CFLOW_VALUE_TRANSFER_MOVE_CONSTRUCT);
    check_bits(slot.available_traits,
               CMETA_TRAIT_COPY | CMETA_TRAIT_MOVE | CMETA_TRAIT_DESTROY);
    check_equal(slot.required_traits,
                (cmeta_trait_flags)(CMETA_TRAIT_MOVE | CMETA_TRAIT_DESTROY));
    check_equal(slot.source_destroy_after_transfer, 1);

    memset(&slot, 0, sizeof(slot));
    check_equal(flow_cflow_value_slot_plan_classify(
                    &reflected_unknown_lifecycle_type,
                    &reflected_unknown_lifecycle_type, &slot),
                SALTS_OK);
    check_equal(slot.mode, FLOW_CFLOW_VALUE_SLOT_NONE);
    check_equal(slot.transfer, FLOW_CFLOW_VALUE_TRANSFER_NONE);
    check_equal(slot.required_traits, (cmeta_trait_flags)0u);
    check_equal(slot.source_destroy_after_transfer, 0);
  }

  it("moves one managed CFlow result into message-owned aligned storage") {
    static const turbo_flow_data_schema_t schema = {
        sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_DATA_ENCODING_OPAQUE, "test.reflected.managed", "Managed",
        "reflected_managed_value", 81u, 1u, NULL};
    cflow_graph surface = {0};
    flow_cflow_region_plan_t region = {0};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_msg_t message;
    turbo_flow_msg_t clone;
    reflected_managed_value_t *input;
    const reflected_managed_value_t *output;
    const reflected_managed_value_t *cloned;
    const void *projection_before;

    check_not_null(flow);
    reflected_managed_reset();
    region.backend = FLOW_CFLOW_REGION_BACKEND_DIRECT;
    region.input_data = &reflected_managed_data;
    region.output_data = &reflected_managed_data;
    surface.root = CMETA_INVALID_ID;
    cflow_graph_init(&surface, &reflected_managed_type);
    check_null(surface.error);
    check_true(cflow_graph_take(&surface, 1u));
    check_true(cflow_plan_compile_surface(&region.plan, &surface, NULL));
    cflow_graph_destroy(&surface);
    check_not_null(region.plan.impl);
    check_true(cmeta_type_equal(region.plan.input_type, &reflected_managed_type));
    check_true(cmeta_type_equal(region.plan.output_type, &reflected_managed_type));
    check_equal(flow_cflow_value_slot_plan_classify(
                    &reflected_managed_type, &reflected_managed_type,
                    &region.value_slot),
                SALTS_OK);
    check_equal(region.value_slot.mode, FLOW_CFLOW_VALUE_SLOT_NONE);
    check_equal(region.value_slot.transfer,
                FLOW_CFLOW_VALUE_TRANSFER_MOVE_CONSTRUCT);
    region.value_slot.mode = FLOW_CFLOW_VALUE_SLOT_OWNED_OUTPUT;

    turbo_flow_msg_init(&message);
    turbo_flow_msg_init(&clone);
    input = (reflected_managed_value_t *)malloc(sizeof(*input));
    check_not_null(input);
    *input = reflected_managed_make(7);
    check_not_null(input->value);
    check_equal(reflected_managed_live_resources, (size_t)1u);
    check_equal(turbo_flow_msg_bind_typed_projection(
                    &message, &schema, &reflected_managed_data, input,
                    reflected_managed_projection_clone,
                    reflected_managed_projection_destroy, NULL),
                SALTS_OK);
    projection_before = turbo_flow_msg_projection(&message, NULL);
    check_not_null(projection_before);

    check_equal(flow_cflow_region_execute(flow, &region, &message), SALTS_OK);
    output = (const reflected_managed_value_t *)
        turbo_flow_msg_projection(&message, NULL);
    check_not_null(output);
    check_true((const void *)output != projection_before);
    check_equal((uintptr_t)output % reflected_managed_type.align,
                (uintptr_t)0u);
    check_not_null(output->value);
    check_equal(*output->value, 7);
    check_equal(reflected_managed_live_resources, (size_t)1u);
    check_true(reflected_managed_moves >= (size_t)1u);
    check_true(reflected_managed_destroys >= (size_t)2u);

    check_equal(turbo_flow_msg_clone(&clone, &message), SALTS_OK);
    cloned = (const reflected_managed_value_t *)
        turbo_flow_msg_projection(&clone, NULL);
    check_not_null(cloned);
    check_not_null(cloned->value);
    check_equal(*cloned->value, 7);
    check_true(cloned->value != output->value);
    check_equal(reflected_managed_live_resources, (size_t)2u);

    turbo_flow_msg_cleanup(&message);
    check_equal(reflected_managed_live_resources, (size_t)1u);
    turbo_flow_msg_cleanup(&clone);
    check_equal(reflected_managed_live_resources, (size_t)0u);
    cflow_plan_destroy(&region.plan);
    turbo_flow_destroy(flow);
  }

  it("uses FunctionDesc as the unary CFlow MAP semantic source") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.increment");
    turbo_flow_reflected_operation_view_t view =
        TURBO_FLOW_REFLECTED_OPERATION_VIEW_INIT;
    const flow_stage_semantic_plan_t *semantics;

    check_not_null(flow);
    check_true(register_unary_reflected(
        flow, &operation,
        FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment)));

    check_equal(turbo_flow_reflected_operation(
                    flow, operation.name, &view),
                SALTS_OK);
    check_true(cmeta_function_desc_equal(
        view.function, FunctionMeta(reflected_increment)));
    check_true(cmeta_function_abi_desc_equal(
        view.abi, FunctionAbi(reflected_increment)));
    check_equal(view.port_count, (size_t)2u);
    check_equal(view.lowering,
                TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP);

    semantics = compile_single_reflected_stage(flow, operation.name);
    check_not_null(semantics);
    check_true(semantics->reflected);
    check_true(semantics->typed);
    check_equal(semantics->effects,
                FunctionMeta(reflected_increment)->effects);
    check_equal(semantics->barriers,
                (uint32_t)FLOW_LOWERING_BARRIER_NONE);
    check_true(semantics->lowering_candidate);
    check_true(semantics->candidate_region != FLOW_PLAN_INDEX_NONE);
    check_equal(flow->compiled_plan.candidate_region_count, 1u);
    check_true(cmeta_type_equal(
        semantics->canonical_input_type, &cmeta_type_int));
    check_true(cmeta_type_equal(
        semantics->canonical_output_type, &cmeta_type_int));
    check_true(cmeta_callable_contract_valid(semantics->callable));

    turbo_flow_destroy(flow);
  }

  it("compiles one maximal reflected chain into an executable CFlow Plan") {
    static const char graph[] =
        "source input\n"
        "stage first operation test.reflected.region.first\n"
        "stage second operation test.reflected.region.second\n"
        "stage main {\n"
        "  input -> first -> second\n"
        "}\n";
    const int inputs[] = {0, 4, -2};
    const int expected[] = {2, 6, 0};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t first =
        reflected_operation_descriptor("test.reflected.region.first");
    turbo_flow_operation_descriptor_t second =
        reflected_operation_descriptor("test.reflected.region.second");
    cflow_result result = {0};
    const flow_cflow_region_plan_t *region;
    const uint32_t *first_region;
    const uint32_t *second_region;
    int first_stage;
    int second_stage;

    check_not_null(flow);
    check_true(register_unary_reflected(
        flow, &first, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment)));
    check_true(register_unary_reflected(
        flow, &second, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment)));
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    {
      int compile_rc = turbo_flow_compile(flow);
      if (compile_rc != SALTS_OK) {
        const turbo_flow_error_t *error = turbo_flow_last_error(flow);
        fprintf(stderr, "region compile failed: rc=%d error=%d message=%s\n",
                compile_rc, error ? error->code : 0,
                error ? error->message : "<none>");
      }
      check_equal(compile_rc, SALTS_OK);
    }

    first_stage = turbo_flow_find_stage(flow, "first");
    second_stage = turbo_flow_find_stage(flow, "second");
    check_true(first_stage >= 0);
    check_true(second_stage >= 0);
    check_equal(flow->compiled_plan.candidate_region_count, (uint32_t)1u);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)1u);

    first_region = (const uint32_t *)vec_at_const(
        &flow->compiled_plan.cflow_region_by_stage, (size_t)first_stage);
    second_region = (const uint32_t *)vec_at_const(
        &flow->compiled_plan.cflow_region_by_stage, (size_t)second_stage);
    check_not_null(first_region);
    check_not_null(second_region);
    check_equal(*first_region, (uint32_t)0u);
    check_equal(*second_region, (uint32_t)0u);

    region = (const flow_cflow_region_plan_t *)vec_at_const(
        &flow->compiled_plan.cflow_regions, 0u);
    check_not_null(region);
    check_equal(region->entry_stage, (uint32_t)first_stage);
    check_equal(region->exit_stage, (uint32_t)second_stage);
    check_equal(region->stage_count, (uint32_t)2u);
    check_true(region->plan.impl != NULL);
    check_true(cmeta_type_equal(region->plan.input_type, &cmeta_type_int));
    check_true(cmeta_type_equal(region->plan.output_type, &cmeta_type_int));
    check_true(region->stats.graph_nodes >= (size_t)2u);
    check_true(region->stats.instructions >= (size_t)1u);

    check_true(cflow_plan_eval_array(
        &region->plan, inputs, sizeof(inputs) / sizeof(inputs[0]), &result));
    check_equal(result.count, sizeof(expected) / sizeof(expected[0]));
    check_true(cmeta_type_equal(result.type, &cmeta_type_int));
    check_equal(result.data, expected, sizeof(expected));

    cflow_result_destroy(&result);
    turbo_flow_destroy(flow);
  }

  it("executes a provider-free reflected chain as one direct CFlow runtime region") {
    static const char graph[] =
        "source input\n"
        "stage first operation test.reflected.runtime.first\n"
        "stage second operation test.reflected.runtime.second\n"
        "stage sink operation test.reflected.runtime.sink\n"
        "stage main {\n"
        "  input -> first -> second -> sink\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t first =
        reflected_operation_descriptor("test.reflected.runtime.first");
    turbo_flow_operation_descriptor_t second =
        reflected_operation_descriptor("test.reflected.runtime.second");
    turbo_flow_operation_descriptor_t sink =
        reflected_operation_descriptor("test.reflected.runtime.sink");
    turbo_flow_operation_provider_registration_t sink_provider =
        TURBO_FLOW_OPERATION_PROVIDER_REGISTRATION_INIT;
    turbo_flow_data_schema_t schema = {
        sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_DATA_ENCODING_OPAQUE, "cmeta.int.data", "Integer",
        "int", 7u, 3u, NULL};
    reflected_runtime_sink_probe_t probe = {0};
    turbo_flow_observer_ops_t observer = {0};
    turbo_flow_msg_t message;
    int *input = NULL;
    int first_stage;
    int second_stage;
    const flow_executor_plan_t *first_executor;
    const flow_executor_plan_t *second_executor;
    const flow_cflow_region_plan_t *region;
    const void *projection_before;

    check_not_null(flow);
    check_true(register_unary_reflected_mode(
        flow, &first, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_unary_reflected_mode(
        flow, &second, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));

    sink.input_domain = TURBO_FLOW_DOMAIN_DATA;
    sink.input_type = cmeta_data_int.stable_id;
    check_equal(turbo_flow_register_operation(flow, &sink), SALTS_OK);
    sink_provider.operation_name = sink.name;
    sink_provider.fn = reflected_runtime_sink;
    sink_provider.ctx = &probe;
    check_equal(turbo_flow_register_operation_provider(flow, &sink_provider), SALTS_OK);

    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    first_stage = turbo_flow_find_stage(flow, "first");
    second_stage = turbo_flow_find_stage(flow, "second");
    check_true(first_stage >= 0);
    check_true(second_stage >= 0);
    first_executor = flow_executor_plan_for_stage(flow, (uint32_t)first_stage);
    second_executor = flow_executor_plan_for_stage(flow, (uint32_t)second_stage);
    check_not_null(first_executor);
    check_not_null(second_executor);
    check_null(first_executor->fn);
    check_null(second_executor->fn);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)1u);
    region = (const flow_cflow_region_plan_t *)vec_at_const(
        &flow->compiled_plan.cflow_regions, 0u);
    check_not_null(region);
    check_equal(region->backend, FLOW_CFLOW_REGION_BACKEND_DIRECT);
    check_equal(region->stage_count, (uint32_t)2u);
    check_true(cmeta_data_desc_equal(region->input_data, &cmeta_data_int));
    check_true(cmeta_data_desc_equal(region->output_data, &cmeta_data_int));
    check_equal(region->value_slot.mode, FLOW_CFLOW_VALUE_SLOT_REUSE_INPUT);
    check_equal(region->value_slot.transfer, FLOW_CFLOW_VALUE_TRANSFER_TRIVIAL_COPY);
    check_equal(region->value_slot.source_destroy_after_transfer, 0);
    check_equal(region->value_slot.extent, sizeof(int));
    check_equal(region->value_slot.alignment, cmeta_type_int.align);
    check_equal(
        region->value_slot.required_traits,
        (cmeta_trait_flags)(CMETA_TRAIT_TRIVIAL_COPY | CMETA_TRAIT_TRIVIAL_DESTROY));

    observer.size = sizeof(observer);
    observer.stage_complete = reflected_test_stage_observer;
    check_equal(turbo_flow_set_observer(flow, &observer, NULL), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_EBUSY);
    check_equal(turbo_flow_set_observer(flow, NULL, NULL), SALTS_OK);

    check_equal(turbo_flow_start(flow), SALTS_OK);
    check_null(flow->broadcast_ring);
    turbo_flow_msg_init(&message);
    input = (int *)malloc(sizeof(*input));
    check_not_null(input);
    *input = 7;
    check_equal(turbo_flow_msg_bind_typed_projection(
                    &message, &schema, &cmeta_data_int, input,
                    reflected_test_clone_int, reflected_test_destroy_int, NULL),
                SALTS_OK);
    input = NULL;

    projection_before = turbo_flow_msg_projection(&message, NULL);
    check_not_null(projection_before);
    check_equal(flow_cflow_region_execute(flow, region, &message), SALTS_OK);
    check_true(turbo_flow_msg_projection(&message, NULL) == projection_before);
    check_equal(*(const int *)turbo_flow_msg_projection(&message, NULL), 9);

    turbo_flow_msg_cleanup(&message);
    turbo_flow_msg_init(&message);
    input = (int *)malloc(sizeof(*input));
    check_not_null(input);
    *input = 7;
    check_equal(turbo_flow_msg_bind_typed_projection(
                    &message, &schema, &cmeta_data_int, input,
                    reflected_test_clone_int, reflected_test_destroy_int, NULL),
                SALTS_OK);
    input = NULL;

    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(probe.calls, (size_t)1u);
    check_equal(probe.value, 9);
    /* Publish clones the message; the caller's source projection stays 7. */
    check_equal(*(const int *)turbo_flow_msg_projection(&message, NULL), 7);

    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("executes a bounded PURE TOTAL MAP batch through one caller-owned CFlow workspace") {
    static const char graph[] =
        "source input\n"
        "stage first operation test.reflected.batch.first\n"
        "stage second operation test.reflected.batch.second\n"
        "stage main {\n"
        "  input -> first -> second\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t first =
        reflected_operation_descriptor("test.reflected.batch.first");
    turbo_flow_operation_descriptor_t second =
        reflected_operation_descriptor("test.reflected.batch.second");
    const flow_cflow_region_plan_t *region;
    flow_cflow_region_batch_workspace_t workspace;
    cflow_plan_batch_result result = {0};
    turbo_flow_msg_t messages[4];
    turbo_flow_data_schema_t schema = {
        sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_DATA_ENCODING_OPAQUE, "cmeta.int.data", "Integer",
        "int", 7u, 3u, NULL};
    const int expected[] = {2, 3, 4, 5};

    check_not_null(flow);
    check_true(register_unary_reflected_mode(
        flow, &first, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_unary_reflected_mode(
        flow, &second, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)1u);
    region = (const flow_cflow_region_plan_t *)vec_at_const(
        &flow->compiled_plan.cflow_regions, 0u);
    check_not_null(region);
    check_true(region->batch_safe);
    check_true(cflow_plan_batch_workspace_supported(&region->plan));

    memset(&workspace, 0, sizeof(workspace));
    check_equal(flow_cflow_region_batch_workspace_init(region, 4u, &workspace),
                SALTS_OK);
    check_equal(cflow_plan_batch_workspace_capacity(&workspace.plan), (size_t)4u);
    check_true(cflow_plan_batch_workspace_bytes(&workspace.plan) > 0u);
    check_equal(
        flow_cflow_region_batch_workspace_bytes(&workspace),
        workspace.input_allocation_bytes +
            cflow_plan_batch_workspace_bytes(&workspace.plan));

    for (size_t i = 0u; i < 4u; ++i) {
      turbo_flow_msg_init(&messages[i]);
      check_equal(bind_test_int_projection(&messages[i], &schema, (int)i), SALTS_OK);
      check_equal(flow_cflow_region_batch_stage_message(
                      region, &workspace, i, &messages[i]),
                  SALTS_OK);
    }
    check_equal(flow_cflow_region_execute_batch(
                    region, &workspace, 4u, &result),
                SALTS_OK);
    check_equal(result.count, (size_t)4u);
    check_true(cmeta_type_equal(result.type, &cmeta_type_int));
    check_equal(memcmp(result.data, expected, sizeof(expected)), 0);

    for (size_t i = 0u; i < 4u; ++i) turbo_flow_msg_cleanup(&messages[i]);
    flow_cflow_region_batch_workspace_destroy(&workspace);
    check_equal(cflow_plan_batch_workspace_capacity(&workspace.plan), (size_t)0u);
    check_equal(flow_cflow_region_batch_workspace_bytes(&workspace), (size_t)0u);
    turbo_flow_destroy(flow);
  }

  it("publishes terminal CFlow batches in bounded chunks without projection recloning") {
    static const char graph[] =
        "source input\n"
        "stage first operation test.reflected.publish.batch.first\n"
        "stage second operation test.reflected.publish.batch.second\n"
        "stage main {\n"
        "  input -> first -> second\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t first =
        reflected_operation_descriptor("test.reflected.publish.batch.first");
    turbo_flow_operation_descriptor_t second =
        reflected_operation_descriptor("test.reflected.publish.batch.second");
    turbo_flow_data_schema_t schema = {
        sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_DATA_ENCODING_OPAQUE, "cmeta.int.data", "Integer",
        "int", 7u, 3u, NULL};
    reflected_batch_prepare_probe_t probe = {
        &schema, 0u, 0u, SIZE_MAX, SALTS_EIO};
    turbo_flow_publish_batch_config_t config =
        TURBO_FLOW_PUBLISH_BATCH_CONFIG_INIT;
    size_t published = SIZE_MAX;

    check_not_null(flow);
    check_true(register_unary_reflected_mode(
        flow, &first, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_unary_reflected_mode(
        flow, &second, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(turbo_flow_start(flow), SALTS_OK);

    config.message_count = 130u;
    config.prepare = reflected_batch_prepare;
    config.ctx = &probe;
    check_equal(turbo_flow_publish_batch(flow, "input", &config, &published),
                SALTS_OK);
    check_equal(published, (size_t)130u);
    check_equal(probe.calls, (size_t)130u);
    check_equal(probe.clone_calls, (size_t)0u);

    probe.calls = 0u;
    probe.clone_calls = 0u;
    probe.fail_index = 70u;
    published = SIZE_MAX;
    check_equal(turbo_flow_publish_batch(flow, "input", &config, &published),
                SALTS_EIO);
    check_equal(published, (size_t)70u);
    check_equal(probe.calls, (size_t)71u);
    check_equal(probe.clone_calls, (size_t)0u);

    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("keeps CFlow regions on both sides of a native barrier") {
    static const char graph[] =
        "source input\n"
        "stage a operation test.reflected.diff.a\n"
        "stage b operation test.reflected.diff.b\n"
        "stage boundary operation test.reflected.diff.boundary\n"
        "stage c operation test.reflected.diff.c\n"
        "stage d operation test.reflected.diff.d\n"
        "stage sink operation test.reflected.diff.sink\n"
        "stage main {\n"
        "  input -> a -> b -> boundary -> c -> d -> sink\n"
        "}\n";
    static const turbo_flow_data_schema_t schema = {
        sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_DATA_ENCODING_OPAQUE, "cmeta.int.data", "Integer",
        "int", 7u, 3u, NULL};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t a =
        reflected_operation_descriptor("test.reflected.diff.a");
    turbo_flow_operation_descriptor_t b =
        reflected_operation_descriptor("test.reflected.diff.b");
    turbo_flow_operation_descriptor_t c_op =
        reflected_operation_descriptor("test.reflected.diff.c");
    turbo_flow_operation_descriptor_t d =
        reflected_operation_descriptor("test.reflected.diff.d");
    reflected_runtime_sink_probe_t probe = {0};
    turbo_flow_msg_t message;
    int boundary_stage;

    check_not_null(flow);
    check_true(register_unary_reflected_mode(
        flow, &a, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_unary_reflected_mode(
        flow, &b, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_native_int_stage(
        flow, "test.reflected.diff.boundary", reflected_noop_stage, NULL));
    check_true(register_unary_reflected_mode(
        flow, &c_op, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_unary_reflected_mode(
        flow, &d, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_native_int_stage(
        flow, "test.reflected.diff.sink", reflected_runtime_sink, &probe));

    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)2u);
    boundary_stage = turbo_flow_find_stage(flow, "boundary");
    check_true(boundary_stage >= 0);
    {
      const uint32_t *region = (const uint32_t *)vec_at_const(
          &flow->compiled_plan.cflow_region_by_stage, (size_t)boundary_stage);
      check_not_null(region);
      check_equal(*region, FLOW_PLAN_INDEX_NONE);
    }

    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(bind_test_int_projection(&message, &schema, 1), SALTS_OK);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(probe.calls, (size_t)1u);
    check_equal(probe.value, 5);
    check_equal(*(const int *)turbo_flow_msg_projection(&message, NULL), 1);

    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("stops a direct CFlow region before a native fan-out boundary") {
    static const char graph[] =
        "source input\n"
        "stage a operation test.reflected.fanout.a\n"
        "stage b operation test.reflected.fanout.b\n"
        "stage fork operation test.reflected.fanout.fork\n"
        "stage left operation test.reflected.fanout.left\n"
        "stage right operation test.reflected.fanout.right\n"
        "stage main {\n"
        "  input -> a -> b -> fork -> [left, right]\n"
        "}\n";
    static const turbo_flow_data_schema_t schema = {
        sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_DATA_ENCODING_OPAQUE, "cmeta.int.data", "Integer",
        "int", 7u, 3u, NULL};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t a =
        reflected_operation_descriptor("test.reflected.fanout.a");
    turbo_flow_operation_descriptor_t b =
        reflected_operation_descriptor("test.reflected.fanout.b");
    reflected_runtime_sink_probe_t left = {0};
    reflected_runtime_sink_probe_t right = {0};
    turbo_flow_msg_t message;
    int fork_stage;

    check_not_null(flow);
    check_true(register_unary_reflected_mode(
        flow, &a, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_unary_reflected_mode(
        flow, &b, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_native_int_stage(
        flow, "test.reflected.fanout.fork", reflected_noop_stage, NULL));
    check_true(register_native_int_stage(
        flow, "test.reflected.fanout.left", reflected_runtime_sink, &left));
    check_true(register_native_int_stage(
        flow, "test.reflected.fanout.right", reflected_runtime_sink, &right));

    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)1u);
    fork_stage = turbo_flow_find_stage(flow, "fork");
    check_true(fork_stage >= 0);
    {
      const flow_stage_semantic_plan_t *semantics =
          (const flow_stage_semantic_plan_t *)vec_at_const(
              &flow->compiled_plan.stage_semantics, (size_t)fork_stage);
      check_not_null(semantics);
      check_bits(semantics->barriers, FLOW_LOWERING_BARRIER_RELATION);
      check_false(semantics->lowering_candidate);
    }

    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(bind_test_int_projection(&message, &schema, 5), SALTS_OK);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(left.calls, (size_t)1u);
    check_equal(right.calls, (size_t)1u);
    check_equal(left.value, 7);
    check_equal(right.value, 7);

    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("keeps independent CFlow branches separate across a fan-in boundary") {
    static const char graph[] =
        "source input\n"
        "stage left operation test.reflected.fanin.left\n"
        "stage right operation test.reflected.fanin.right\n"
        "stage join operation test.reflected.fanin.join\n"
        "stage sink operation test.reflected.fanin.sink\n"
        "stage main {\n"
        "  input -> [left, right] -> join -> sink\n"
        "}\n";
    static const turbo_flow_data_schema_t schema = {
        sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_DATA_ENCODING_OPAQUE, "cmeta.int.data", "Integer",
        "int", 7u, 3u, NULL};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t left_op =
        reflected_operation_descriptor("test.reflected.fanin.left");
    turbo_flow_operation_descriptor_t right_op =
        reflected_operation_descriptor("test.reflected.fanin.right");
    reflected_runtime_sink_probe_t probe = {0};
    turbo_flow_msg_t message;
    int join_stage;

    check_not_null(flow);
    check_true(register_unary_reflected_mode(
        flow, &left_op, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_unary_reflected_mode(
        flow, &right_op, FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_true(register_native_int_stage(
        flow, "test.reflected.fanin.join", reflected_noop_stage, NULL));
    check_true(register_native_int_stage(
        flow, "test.reflected.fanin.sink", reflected_runtime_sink, &probe));

    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)2u);
    join_stage = turbo_flow_find_stage(flow, "join");
    check_true(join_stage >= 0);
    {
      const flow_stage_semantic_plan_t *semantics =
          (const flow_stage_semantic_plan_t *)vec_at_const(
              &flow->compiled_plan.stage_semantics, (size_t)join_stage);
      check_not_null(semantics);
      check_bits(semantics->barriers, FLOW_LOWERING_BARRIER_RELATION);
      check_false(semantics->lowering_candidate);
    }

    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(bind_test_int_projection(&message, &schema, 10), SALTS_OK);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(probe.calls, (size_t)1u);
    check_equal(probe.value, 12);

    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("lowers one-IN one-OUT native adapters into a direct CFlow region") {
    static const char graph[] =
        "source input\n"
        "stage op operation test.reflected.increment_out\n"
        "stage sink operation test.reflected.increment_out.sink\n"
        "stage main {\n"
        "  input -> op -> sink\n"
        "}\n";
    static const turbo_flow_data_schema_t schema = {
        sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA,
        TURBO_FLOW_DATA_ENCODING_OPAQUE, "cmeta.int.data", "Integer",
        "int", 7u, 3u, NULL};
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.increment_out");
    turbo_flow_operation_port_binding_t ports[2];
    turbo_flow_reflected_operation_registration_t registration =
        TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;
    reflected_runtime_sink_probe_t probe = {0};
    turbo_flow_msg_t message;
    const flow_stage_semantic_plan_t *semantics;
    int stage;

    check_not_null(flow);
    reflected_increment_out_calls = 0u;
    reflected_increment_out_alias_calls = 0u;
    ports[0] = reflected_param_port(
        0u, TURBO_FLOW_OPERATION_PORT_INPUT, 0u,
        TURBO_FLOW_OPERATION_STORAGE_DIRECT);
    ports[1] = reflected_param_port(
        0u, TURBO_FLOW_OPERATION_PORT_OUTPUT, 1u,
        TURBO_FLOW_OPERATION_STORAGE_POINTEE);
    registration.operation = &operation;
    registration.function = FunctionMeta(reflected_increment_out);
    registration.abi = FunctionAbi(reflected_increment_out);
    registration.adapter = reflected_increment_out_adapter();
    registration.ports = ports;
    registration.port_count = 2u;
    registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP;

    check_equal(turbo_flow_register_reflected_operation(
                    flow, &registration),
                SALTS_OK);
    check_true(register_native_int_stage(
        flow, "test.reflected.increment_out.sink",
        reflected_runtime_sink, &probe));
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_OK);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)1u);

    stage = turbo_flow_find_stage(flow, "op");
    check_true(stage >= 0);
    semantics = (const flow_stage_semantic_plan_t *)vec_at_const(
        &flow->compiled_plan.stage_semantics, (size_t)stage);
    check_not_null(semantics);
    check_true(semantics->reflected);
    check_true(semantics->reflected_typed_adapter);
    check_true(semantics->lowering_candidate);
    check_equal(
        semantics->barriers & FLOW_LOWERING_BARRIER_NATIVE_MUTATION,
        (uint32_t)0u);
    check_true(cflow_function_typed_adapter_projection_valid(
        &semantics->typed_adapter_projection));

    check_equal(turbo_flow_start(flow), SALTS_OK);
    turbo_flow_msg_init(&message);
    check_equal(bind_test_int_projection(&message, &schema, 41), SALTS_OK);
    check_equal(turbo_flow_publish(flow, "input", &message), SALTS_OK);
    check_equal(probe.calls, (size_t)1u);
    check_equal(probe.value, 42);
    check_equal(reflected_increment_out_calls, (size_t)1u);
    check_equal(reflected_increment_out_alias_calls, (size_t)0u);
    check_equal(*(const int *)turbo_flow_msg_projection(&message, NULL), 41);

    turbo_flow_msg_cleanup(&message);
    check_equal(turbo_flow_stop(flow), SALTS_OK);
    turbo_flow_destroy(flow);
  }

  it("keeps INOUT reflected parameters outside CFlow lowering") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.inout");
    turbo_flow_operation_port_binding_t ports[2];
    turbo_flow_reflected_operation_registration_t registration =
        TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;

    check_not_null(flow);
    ports[0] = reflected_param_port(
        0u, TURBO_FLOW_OPERATION_PORT_INPUT, 0u,
        TURBO_FLOW_OPERATION_STORAGE_POINTEE);
    ports[1] = reflected_param_port(
        0u, TURBO_FLOW_OPERATION_PORT_OUTPUT, 0u,
        TURBO_FLOW_OPERATION_STORAGE_POINTEE);
    registration.operation = &operation;
    registration.function = FunctionMeta(reflected_inout);
    registration.abi = FunctionAbi(reflected_inout);
    registration.adapter = reflected_inout_adapter();
    registration.ports = ports;
    registration.port_count = 2u;
    registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP;

    check_equal(turbo_flow_register_reflected_operation(
                    flow, &registration),
                SALTS_EINVAL);
    check_equal(turbo_flow_operation_count(flow), (size_t)0u);
    turbo_flow_destroy(flow);
  }

  it("maps multi-input and OUT parameters without duplicating native types") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.join_out");
    turbo_flow_operation_port_binding_t ports[3];
    turbo_flow_reflected_operation_registration_t registration =
        TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;
    turbo_flow_reflected_operation_view_t view =
        TURBO_FLOW_REFLECTED_OPERATION_VIEW_INIT;

    ports[0] = reflected_param_port(
        0u, TURBO_FLOW_OPERATION_PORT_INPUT, 0u,
        TURBO_FLOW_OPERATION_STORAGE_DIRECT);
    ports[1] = reflected_param_port(
        1u, TURBO_FLOW_OPERATION_PORT_INPUT, 1u,
        TURBO_FLOW_OPERATION_STORAGE_DIRECT);
    ports[2] = reflected_param_port(
        0u, TURBO_FLOW_OPERATION_PORT_OUTPUT, 2u,
        TURBO_FLOW_OPERATION_STORAGE_POINTEE);

    registration.operation = &operation;
    registration.function = FunctionMeta(reflected_join_out);
    registration.abi = FunctionAbi(reflected_join_out);
    registration.ports = ports;
    registration.port_count = 3u;
    registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_NONE;

    check_equal(turbo_flow_register_reflected_operation(
                    flow, &registration),
                SALTS_OK);
    check_equal(turbo_flow_reflected_operation(
                    flow, operation.name, &view),
                SALTS_OK);
    check_equal(view.port_count, (size_t)3u);
    check_true(cmeta_function_desc_equal(
        view.function, FunctionMeta(reflected_join_out)));
    check_equal(view.ports[0].parameter_index, (size_t)0u);
    check_equal(view.ports[1].parameter_index, (size_t)1u);
    check_equal(view.ports[2].parameter_index, (size_t)2u);
    check_equal(view.ports[2].storage,
                TURBO_FLOW_OPERATION_STORAGE_POINTEE);
    check_true(cmeta_type_equal(
        view.ports[2].data->storage_type, &cmeta_type_int));

    turbo_flow_destroy(flow);
  }

  it("rejects incomplete reflected port mappings before compile") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.invalid_ports");
    turbo_flow_operation_port_binding_t ports[2];
    turbo_flow_reflected_operation_registration_t registration =
        TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;

    ports[0] = reflected_param_port(
        0u, TURBO_FLOW_OPERATION_PORT_INPUT, 0u,
        TURBO_FLOW_OPERATION_STORAGE_DIRECT);
    ports[1] = reflected_param_port(
        1u, TURBO_FLOW_OPERATION_PORT_INPUT, 1u,
        TURBO_FLOW_OPERATION_STORAGE_DIRECT);

    registration.operation = &operation;
    registration.function = FunctionMeta(reflected_join_out);
    registration.abi = FunctionAbi(reflected_join_out);
    registration.ports = ports;
    registration.port_count = 2u;
    registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_NONE;

    check_equal(turbo_flow_register_reflected_operation(
                    flow, &registration),
                SALTS_EINVAL);
    check_equal(turbo_flow_operation_count(flow), (size_t)0u);

    turbo_flow_destroy(flow);
  }

  it("derives optimization barriers only from canonical FunctionDesc semantics") {
    uint32_t barriers;

    check_equal(flow_function_semantic_barriers(
                    FunctionMeta(reflected_increment)),
                (uint32_t)FLOW_LOWERING_BARRIER_NONE);

    barriers = flow_function_semantic_barriers(
        FunctionMeta(reflected_stateful));
    check_bits(barriers, FLOW_LOWERING_BARRIER_STATEFUL);

    barriers = flow_function_semantic_barriers(
        FunctionMeta(reflected_async));
    check_bits(barriers, FLOW_LOWERING_BARRIER_ASYNC);

    barriers = flow_function_semantic_barriers(
        FunctionMeta(reflected_io));
    check_bits(barriers, FLOW_LOWERING_BARRIER_EXTERNAL_IO);

    barriers = flow_function_semantic_barriers(
        FunctionMeta(reflected_unknown));
    check_bits(barriers, FLOW_LOWERING_BARRIER_SEMANTIC_UNKNOWN);

    barriers = flow_function_semantic_barriers(
        FunctionMeta(reflected_join_out));
    check_bits(barriers, FLOW_LOWERING_BARRIER_NATIVE_MUTATION);
    check_bits(barriers, FLOW_LOWERING_BARRIER_MAY_FAIL);
  }

  it("keeps operation deadlines outside direct CFlow regions") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.deadline");
    const flow_stage_semantic_plan_t *semantics;

    check_not_null(flow);
    operation.runtime.deadline_ms = 1u;
    check_true(register_unary_reflected(
        flow, &operation,
        FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment)));

    semantics = compile_single_reflected_stage(flow, operation.name);
    check_not_null(semantics);
    check_true(semantics->reflected);
    check_true(semantics->typed);
    check_bits(semantics->barriers, FLOW_LOWERING_BARRIER_DEADLINE);
    check_false(semantics->lowering_candidate);
    check_equal(semantics->candidate_region, FLOW_PLAN_INDEX_NONE);

    turbo_flow_destroy(flow);
  }

  it("fails compile transactionally when a provider-free reflected stage hits a native barrier") {
    static const char graph[] =
        "source input\n"
        "stage op operation test.reflected.provider_free_deadline\n"
        "stage main {\n"
        "  input -> op\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.provider_free_deadline");

    check_not_null(flow);
    operation.runtime.deadline_ms = 1u;
    check_true(register_unary_reflected_mode(
        flow, &operation,
        FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u), SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_ENOTSUP);
    check_false(flow->compiled_plan.sealed);
    check_equal(vec_size(&flow->compiled_plan.cflow_regions), (size_t)0u);

    turbo_flow_destroy(flow);
  }

  it("applies FunctionDesc state barriers to the compiled stage") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.stateful");
    const flow_stage_semantic_plan_t *semantics;

    check_not_null(flow);
    check_true(register_unary_reflected(
        flow, &operation,
        FunctionMeta(reflected_stateful),
        FunctionAbi(reflected_stateful),
        CFLOW_REFLECTED_CALLABLE(reflected_stateful)));

    semantics = compile_single_reflected_stage(flow, operation.name);
    check_not_null(semantics);
    check_true(semantics->reflected);
    check_true(semantics->typed);
    check_bits(semantics->barriers, FLOW_LOWERING_BARRIER_STATEFUL);
    check_false(semantics->lowering_candidate);
    check_equal(semantics->candidate_region, FLOW_PLAN_INDEX_NONE);
    check_equal(flow->compiled_plan.candidate_region_count, 0u);

    turbo_flow_destroy(flow);
  }

  it("transfers staged resources only into a successful sealed plan") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.plan_resource");
    const flow_stage_semantic_plan_t *semantics;
    size_t releases = 0u;

    check_not_null(flow);
    check_true(register_unary_reflected_mode(
        flow, &operation,
        FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_equal(flow_plan_owned_resource_stage(
                    flow, &releases, reflected_plan_resource_release),
                SALTS_OK);
    check_equal(vec_size(&flow->pending_plan_resources), (size_t)1u);
    semantics = compile_single_reflected_stage(flow, operation.name);
    check_not_null(semantics);
    check_true(flow->compiled_plan.sealed);
    check_equal(vec_size(&flow->pending_plan_resources), (size_t)0u);
    check_equal(vec_size(&flow->compiled_plan.owned_resources), (size_t)1u);
    check_equal(releases, (size_t)0u);

    flow_clear_runtime_plan(flow);
    check_equal(releases, (size_t)1u);
    check_equal(vec_size(&flow->compiled_plan.owned_resources), (size_t)0u);
    turbo_flow_destroy(flow);
    check_equal(releases, (size_t)1u);
  }

  it("keeps staged resources owned by the mutable flow after compile failure") {
    static const char graph[] =
        "source input\n"
        "stage op operation test.reflected.plan_resource_failure\n"
        "stage main {\n"
        "  input -> op\n"
        "}\n";
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor(
            "test.reflected.plan_resource_failure");
    size_t releases = 0u;

    check_not_null(flow);
    operation.runtime.deadline_ms = 1u;
    check_true(register_unary_reflected_mode(
        flow, &operation,
        FunctionMeta(reflected_increment),
        FunctionAbi(reflected_increment),
        CFLOW_REFLECTED_CALLABLE(reflected_increment), 0));
    check_equal(turbo_flow_parse_string(flow, graph, sizeof(graph) - 1u),
                SALTS_OK);
    check_equal(flow_plan_owned_resource_stage(
                    flow, &releases, reflected_plan_resource_release),
                SALTS_OK);
    check_equal(turbo_flow_compile(flow), SALTS_ENOTSUP);
    check_false(flow->compiled_plan.sealed);
    check_equal(vec_size(&flow->pending_plan_resources), (size_t)1u);
    check_equal(releases, (size_t)0u);

    flow_clear_plan(flow);
    check_equal(vec_size(&flow->pending_plan_resources), (size_t)0u);
    check_equal(releases, (size_t)1u);
    turbo_flow_destroy(flow);
    check_equal(releases, (size_t)1u);
  }

  it("rejects a reflected registration that repeats legacy graph type strings") {
    turbo_flow_t *flow = turbo_flow_create();
    turbo_flow_operation_descriptor_t operation =
        reflected_operation_descriptor("test.reflected.duplicate_types");
    turbo_flow_operation_port_binding_t ports[2];
    turbo_flow_reflected_operation_registration_t registration =
        TURBO_FLOW_REFLECTED_OPERATION_REGISTRATION_INIT;

    operation.input_domain = TURBO_FLOW_DOMAIN_DATA;
    operation.input_type = "legacy.Int";
    operation.output_domain = TURBO_FLOW_DOMAIN_DATA;
    operation.output_type = "legacy.Int";

    ports[0] = reflected_param_port(
        0u, TURBO_FLOW_OPERATION_PORT_INPUT, 0u,
        TURBO_FLOW_OPERATION_STORAGE_DIRECT);
    ports[1] = reflected_return_port(0u);

    registration.operation = &operation;
    registration.function = FunctionMeta(reflected_increment);
    registration.abi = FunctionAbi(reflected_increment);
    registration.adapter = CFLOW_REFLECTED_CALLABLE(reflected_increment);
    registration.ports = ports;
    registration.port_count = 2u;
    registration.lowering = TURBO_FLOW_REFLECTED_LOWERING_CFLOW_MAP;

    check_equal(turbo_flow_register_reflected_operation(
                    flow, &registration),
                SALTS_EINVAL);

    turbo_flow_destroy(flow);
  }
}
