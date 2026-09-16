#ifndef FLOW_PLUGIN_GENERATION_OWNER_FIXTURE_H
#define FLOW_PLUGIN_GENERATION_OWNER_FIXTURE_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

enum {
  OWNER_FIXTURE_NORMAL,
  OWNER_FIXTURE_NO_QUIESCE,
  OWNER_FIXTURE_NO_DRAIN,
  OWNER_FIXTURE_NO_SHUTDOWN,
  OWNER_FIXTURE_SHORT,
  OWNER_FIXTURE_TINY,
  OWNER_FIXTURE_LONG,
  OWNER_FIXTURE_OLD_MAJOR,
  OWNER_FIXTURE_FUTURE_MAJOR,
  OWNER_FIXTURE_FUTURE_MINOR,
  OWNER_FIXTURE_NO_DESTROY,
  OWNER_FIXTURE_NO_CONTEXT
};

/* Test-only observation prefix borrowed from the catalog's provider ctx. */
typedef struct generation_owner_observer_s {
  int mode;
  size_t consumes, consumes_after_quiesce;
  int consume_status;
  int managed_source;
  int paused_request_status;
  struct turbo_flow_run_s *source_run;
  uint64_t source_deadline_ms;
  void *async_claim;
  atomic_int async_ready;
  int block_quiesce;
  size_t lifecycle_calls, destroys, graph_shutdowns, destroys_before_graph;
} generation_owner_observer_t;

#endif
