#include "tinytest.h"
#include "turbo_flow_resolved_config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char fields[] =
    "    operation: decision.evaluate\n    plugin: fixture.typed\n    version: 1\n"
    "    input_schema: example.Input\n    input_schema_version: 1\n"
    "    output_schema: example.Decision\n    output_schema_version: 1\n"
    "    permissions: [read.data, audit]\n    execution: inline\n    threading: owner\n"
    "    cancellation: none\n    max_inflight: 1\n    max_input_bytes: 4096\n"
    "    max_result_bytes: 1024\n    max_retained_bytes: 8192\n    max_steps: 10000\n"
    "    deadline_ms: 0\n";

static char *document(const char *body) {
  const char
      *a = "version: 1\noperation_bindings:\n  -\n",
      *b = "channels:\n  routing:\n    kind: fixture.resource\n    config: {}\nadapters: {}\n";
  char *out = (char *)malloc(strlen(a) + strlen(body) + strlen(b) + 1u);
  if (out) sprintf(out, "%s%s%s", a, body, b);
  return out;
}
static char *replace(const char *s, const char *from, const char *to) {
  const char *p = strstr(s, from);
  if (!p) return NULL;
  size_t n = strlen(s) - strlen(from) + strlen(to) + 1u;
  char *out = (char *)malloc(n);
  if (out) sprintf(out, "%.*s%s%s", (int)(p - s), s, to, p + strlen(from));
  return out;
}
static void rejected(const char *yaml, int status, const char *path) {
  turbo_flow_resolved_config_t *c = (turbo_flow_resolved_config_t *)(uintptr_t)1u;
  turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
  check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &c, &e), status);
  check_null(c);
  check_equal(e.status, status);
  check_equal(e.path, path);
}
static char *many_bindings(size_t count) {
  size_t capacity = 256u + count * (strlen(fields) + 32u), used = 0u;
  char *out = (char *)malloc(capacity);
  if (!out) return NULL;
  used += (size_t)sprintf(out + used, "version: 1\noperation_bindings:\n");
  for (size_t i = 0; i < count; i++) {
    char op[64];
    sprintf(op, "decision.%zu", i);
    char *item = replace(fields, "decision.evaluate", op);
    used += (size_t)sprintf(out + used, "  -\n%s", item);
    free(item);
  }
  sprintf(out + used, "adapters: {}\n");
  return out;
}

spec("operation binding configuration") {
  it("projects the complete immutable binding") {
    char *yaml = document(fields);
    turbo_flow_resolved_config_t *c = NULL;
    turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
    size_t count = 9u;
    turbo_flow_resolved_operation_binding_view_t v =
        TURBO_FLOW_RESOLVED_OPERATION_BINDING_VIEW_INIT;
    const char *permission = NULL;
    check_equal(turbo_flow_config_resolve_yaml(yaml, strlen(yaml), &c, &e), SALTS_OK);
    check_equal(turbo_flow_resolved_config_operation_binding_count(c, &count), SALTS_OK);
    check_equal(count, (size_t)1);
    check_equal(turbo_flow_resolved_config_operation_binding_at(c, 0, &v), SALTS_OK);
    check_equal(v.operation, "decision.evaluate");
    check_null(v.resource);
    check_equal(v.plugin, "fixture.typed");
    check_equal(v.version, (uint32_t)1);
    check_equal(v.input_schema, "example.Input");
    check_equal(v.output_schema, "example.Decision");
    check_equal(v.input_schema_version, (uint32_t)1);
    check_equal(v.output_schema_version, (uint32_t)1);
    check_equal(v.execution, TURBO_FLOW_CONFIG_OPERATION_INLINE);
    check_equal(v.threading, TURBO_FLOW_CONFIG_OPERATION_OWNER);
    check_equal(v.cancellation, TURBO_FLOW_CONFIG_OPERATION_CANCEL_NONE);
    check_equal(v.max_inflight, (uint32_t)1);
    check_equal(v.max_input_bytes, (size_t)4096);
    check_equal(v.max_result_bytes, (size_t)1024);
    check_equal(v.max_retained_bytes, (size_t)8192);
    check_equal(v.max_steps, (uint32_t)10000);
    check_equal(v.deadline_ms, UINT64_C(0));
    check_equal(v.permission_count, (size_t)2);
    check_equal(turbo_flow_resolved_config_operation_binding_permission_at(c, 0, 1, &permission),
                SALTS_OK);
    check_equal(permission, "audit");
    turbo_flow_resolved_config_destroy(c);
    free(yaml);
  }
  it("preserves legacy omission and accepts an empty list") {
    const char *docs[] = {"version: 1\nadapters: {}\n",
                          "version: 1\noperation_bindings: []\nadapters: {}\n"};
    for (size_t i = 0; i < 2; i++) {
      turbo_flow_resolved_config_t *c = NULL;
      turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
      size_t n = 8;
      check_equal(turbo_flow_config_resolve_yaml(docs[i], strlen(docs[i]), &c, &e), SALTS_OK);
      check_equal(turbo_flow_resolved_config_operation_binding_count(c, &n), SALTS_OK);
      check_equal(n, (size_t)0);
      if (i == 0)
        check_null(strstr(turbo_flow_resolved_config_json(c, NULL), "operation_bindings"));
      turbo_flow_resolved_config_destroy(c);
    }
  }
  it("rejects collection shapes and reports paths") {
    rejected("version: 1\noperation_bindings: null\nadapters: {}\n", SALTS_EINVAL,
             "$.operation_bindings");
    rejected("version: 1\noperation_bindings: {}\nadapters: {}\n", SALTS_EINVAL,
             "$.operation_bindings");
    rejected("version: 1\noperation_bindings: [text]\nadapters: {}\n", SALTS_EINVAL,
             "$.operation_bindings[0]");
  }
  it("rejects every missing required field") {
    const char *names[] = {"operation",
                           "plugin",
                           "version",
                           "input_schema",
                           "input_schema_version",
                           "output_schema",
                           "output_schema_version",
                           "permissions",
                           "execution",
                           "threading",
                           "cancellation",
                           "max_inflight",
                           "max_input_bytes",
                           "max_result_bytes",
                           "max_retained_bytes",
                           "max_steps",
                           "deadline_ms"};
    char *base = document(fields);
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
      char key[80], path[128];
      sprintf(key, "    %s:", names[i]);
      char *p = strstr(base, key), *end = strchr(p, '\n');
      char line[256];
      sprintf(line, "%.*s\n", (int)(end - p), p);
      char *y = replace(base, line, "");
      sprintf(path, "$.operation_bindings[0].%s", names[i]);
      rejected(y, SALTS_EINVAL, path);
      free(y);
    }
    free(base);
  }
  it("rejects unknown fields identifiers resources and embedded NUL") {
    char *base = document(fields),
         *y = replace(base, "    operation:", "    surprise: x\n    operation:");
    rejected(y, SALTS_EINVAL, "$.operation_bindings[0].surprise");
    free(y);
    y = replace(base, "decision.evaluate", "bad/value");
    rejected(y, SALTS_EINVAL, "$.operation_bindings[0].operation");
    free(y);
    y = replace(base, "fixture.typed", "\"abc\\u0000def\"");
    rejected(y, SALTS_EINVAL, "$.operation_bindings[0].plugin");
    free(y);
    y = replace(base, "    plugin: fixture.typed\n",
                "    resource: absent\n    plugin: fixture.typed\n");
    rejected(y, SALTS_EINVAL, "$.operation_bindings[0].resource");
    free(y);
    free(base);
  }
  it("rejects invalid numbers enums permissions and byte relationships") {
    const char *from[] = {"    version: 1\n",   "max_inflight: 1",   "max_input_bytes: 4096",
                          "max_steps: 10000",   "deadline_ms: 0",    "max_result_bytes: 1024",
                          "execution: inline",  "threading: owner",  "cancellation: none",
                          "[read.data, audit]", "[read.data, audit]"};
    const char *to[] = {"    version: 1.5\n",
                        "max_inflight: 1048577",
                        "max_input_bytes: -1",
                        "max_steps: 4294967296",
                        "deadline_ms: 3600001",
                        "max_result_bytes: 9000",
                        "execution: bad",
                        "threading: bad",
                        "cancellation: bad",
                        "[read.data, read.data]",
                        "not-array"};
    const char *path[] = {"version",      "max_inflight",     "max_input_bytes", "max_steps",
                          "deadline_ms",  "max_result_bytes", "execution",       "threading",
                          "cancellation", "permissions[1]",   "permissions"};
    int status[] = {SALTS_EINVAL, SALTS_EINVAL,   SALTS_EINVAL, SALTS_EINVAL,
                    SALTS_EINVAL, SALTS_EINVAL,   SALTS_EINVAL, SALTS_EINVAL,
                    SALTS_EINVAL, SALTS_EALREADY, SALTS_EINVAL};
    char *base = document(fields);
    for (size_t i = 0; i < 11; i++) {
      char p[128];
      sprintf(p, "$.operation_bindings[0].%s", path[i]);
      char *y = replace(base, from[i], to[i]);
      rejected(y, status[i], p);
      free(y);
    }
    free(base);
  }
  it("rejects duplicate stateless pairs") {
    char *base = document(fields);
    const char *item = strstr(base, "  -\n"), *suffix = strstr(base, "channels:\n");
    size_t len = (size_t)(suffix - item), n = strlen(base) + len + 1;
    char *y = (char *)malloc(n);
    sprintf(y, "%.*s%.*s%s", (int)(suffix - base), base, (int)len, item, suffix);
    rejected(y, SALTS_EALREADY, "$.operation_bindings[1].operation");
    free(y);
    free(base);
  }
  it("enforces binding and permission capacity boundaries") {
    char *y = many_bindings(TURBO_FLOW_CONFIG_OPERATION_MAX_BINDINGS);
    turbo_flow_resolved_config_t *c = NULL;
    turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
    size_t n = 0;
    check_equal(turbo_flow_config_resolve_yaml(y, strlen(y), &c, &e), SALTS_OK);
    check_equal(turbo_flow_resolved_config_operation_binding_count(c, &n), SALTS_OK);
    check_equal(n, (size_t)TURBO_FLOW_CONFIG_OPERATION_MAX_BINDINGS);
    turbo_flow_resolved_config_destroy(c);
    free(y);
    y = many_bindings(TURBO_FLOW_CONFIG_OPERATION_MAX_BINDINGS + 1u);
    rejected(y, SALTS_ENOSPC, "$.operation_bindings");
    free(y);
    char perms[512] = "[";
    for (size_t i = 0; i < TURBO_FLOW_CONFIG_OPERATION_MAX_PERMISSIONS + 1u; i++) {
      char one[16];
      sprintf(one, "p%zu%s", i, i == TURBO_FLOW_CONFIG_OPERATION_MAX_PERMISSIONS ? "]" : ", ");
      strcat(perms, one);
    }
    char *base = document(fields);
    y = replace(base, "[read.data, audit]", perms);
    rejected(y, SALTS_ENOSPC, "$.operation_bindings[0].permissions");
    free(y);
    free(base);
  }
  it("accepts a referenced resource and enum alternatives") {
    char *base = document(fields);
    char *a = replace(base, "    plugin: fixture.typed\n",
                      "    resource: routing\n    plugin: fixture.typed\n");
    char *b = replace(a, "execution: inline", "execution: thread");
    char *coro = replace(b, "threading: owner", "threading: thread_safe");
    char *y = replace(coro, "cancellation: none", "cancellation: cooperative");
    turbo_flow_resolved_config_t *c = NULL;
    turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
    turbo_flow_resolved_operation_binding_view_t v =
        TURBO_FLOW_RESOLVED_OPERATION_BINDING_VIEW_INIT;
    check_equal(turbo_flow_config_resolve_yaml(y, strlen(y), &c, &e), SALTS_OK);
    check_equal(turbo_flow_resolved_config_operation_binding_at(c, 0, &v), SALTS_OK);
    check_equal(v.resource, "routing");
    check_equal(v.execution, TURBO_FLOW_CONFIG_OPERATION_THREAD);
    check_equal(v.threading, TURBO_FLOW_CONFIG_OPERATION_THREAD_SAFE);
    check_equal(v.cancellation, TURBO_FLOW_CONFIG_OPERATION_CANCEL_COOPERATIVE);
    turbo_flow_resolved_config_destroy(c);
    free(y);
    free(coro);
    free(b);
    free(a);
    free(base);
  }
  it("clears failed query outputs and does not overrun a short view") {
    char *y = document(fields);
    turbo_flow_resolved_config_t *c = NULL;
    turbo_flow_config_error_t e = TURBO_FLOW_CONFIG_ERROR_INIT;
    size_t n = 8;
    const char *p = "sentinel";
    turbo_flow_resolved_operation_binding_view_t v =
        TURBO_FLOW_RESOLVED_OPERATION_BINDING_VIEW_INIT;
    check_equal(turbo_flow_config_resolve_yaml(y, strlen(y), &c, &e), SALTS_OK);
    check_equal(turbo_flow_resolved_config_operation_binding_count(NULL, &n), SALTS_EINVAL);
    check_equal(n, (size_t)0);
    v.size = sizeof(v) - 1;
    v.operation = "sentinel";
    check_equal(turbo_flow_resolved_config_operation_binding_at(c, 0, &v), SALTS_EINVAL);
    check_equal(v.operation, "sentinel");
    v = (turbo_flow_resolved_operation_binding_view_t)
        TURBO_FLOW_RESOLVED_OPERATION_BINDING_VIEW_INIT;
    v.operation = "sentinel";
    check_equal(turbo_flow_resolved_config_operation_binding_at(c, 1, &v), SALTS_ENOENT);
    check_null(v.operation);
    check_equal(turbo_flow_resolved_config_operation_binding_permission_at(c, 0, 2, &p),
                SALTS_ENOENT);
    check_null(p);
    turbo_flow_resolved_config_destroy(c);
    free(y);
  }
}
