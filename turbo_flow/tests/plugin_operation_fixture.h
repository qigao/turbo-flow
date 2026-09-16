#ifndef FLOW_PLUGIN_OPERATION_FIXTURE_H
#define FLOW_PLUGIN_OPERATION_FIXTURE_H
#include "operation_schema_fixture.h"
#include "turbo_flow_plugin_generation.h"
#include <salts/thread.h>
#include <stdatomic.h>
enum {
  OP_FIXTURE_OK,
  OP_FIXTURE_DUPLICATE,
  OP_FIXTURE_DUPLICATE_CHANGED,
  OP_FIXTURE_TWO_VERSIONS,
  OP_FIXTURE_NO_SCHEMA,
  OP_FIXTURE_BAD_SHAPE,
  OP_FIXTURE_SWALLOW,
  OP_FIXTURE_PREFLIGHT_FAIL,
  OP_FIXTURE_CONTEXT_FAIL,
  OP_FIXTURE_CONTEXT_FAIL_VALUE,
  OP_FIXTURE_SESSION_FAIL,
  OP_FIXTURE_SESSION_FAIL_VALUE,
  OP_FIXTURE_CONTEXT_FACTORY_ALIAS,
  OP_FIXTURE_SESSION_FACTORY_ALIAS,
  OP_FIXTURE_SESSION_RESULT_ALIAS,
  OP_FIXTURE_EXECUTE_FAIL_VALUE,
  OP_FIXTURE_EXECUTE_NULL,
  OP_FIXTURE_EXECUTE_ALIAS,
  OP_FIXTURE_EXECUTE_FAIL_ALIAS,
  OP_FIXTURE_SWALLOW_CHARGE,
  OP_FIXTURE_RELEASE_SESSION_ONCE,
  OP_FIXTURE_RELEASE_CONTEXT_ONCE,
  OP_FIXTURE_ZERO_CHARGE,
  OP_FIXTURE_BAD_ABI,
  OP_FIXTURE_SHORT,
  OP_FIXTURE_LONG,
  OP_FIXTURE_BAD_VTABLE,
  OP_FIXTURE_NO_GUARANTEE,
  OP_FIXTURE_BAD_EFFECT,
  OP_FIXTURE_BAD_PERMISSION,
  OP_FIXTURE_DUP_PERMISSION,
  OP_FIXTURE_PERMISSION,
  OP_FIXTURE_BAD_LIMIT,
  OP_FIXTURE_MISSING_OUTPUT,
  OP_FIXTURE_MEMORY_OVERFLOW,
  OP_FIXTURE_OWNER_FAIL,
  OP_FIXTURE_TWO_NAMES,
  OP_FIXTURE_SECOND_SESSION_FAIL,
  OP_FIXTURE_EXECUTE_FAIL_NULL
};
enum { OP_BARRIER_NONE, OP_BARRIER_EXECUTE, OP_BARRIER_CLONE, OP_BARRIER_DESTROY };
enum {
  OP_ERROR_VALID,
  OP_ERROR_SHORT,
  OP_ERROR_TRUNCATED,
  OP_ERROR_LONG,
  OP_ERROR_OLD_MAJOR,
  OP_ERROR_NEW_MAJOR,
  OP_ERROR_NEW_MINOR
};
/* Test-only observation state, borrowed via the real catalog factory_ctx while its DLL is pinned.
 */
typedef struct operation_fixture_s {
  int mode;
  uint32_t error_phase;
  int error_fault, error_status, fail_session_release, fail_context_release;
  atomic_uint sessions, contexts, preflights, session_creates, context_creates;
  atomic_uint executes, clones, destroys, session_releases, context_releases;
  salts_mutex_t mutex;
  salts_cond_t cond;
  int barrier, entered, proceed;
  turbo_flow_data_schema_t fault_schema;
} operation_fixture_t;
#endif
