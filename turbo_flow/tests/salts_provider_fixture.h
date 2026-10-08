#ifndef FLOW_SALTS_PROVIDER_FIXTURE_H
#define FLOW_SALTS_PROVIDER_FIXTURE_H
#include <stdbool.h>

/* Shared test-only state layout. Access only through a live Component scope,
 * after synchronous graph publication or completion of owner control work. */
typedef struct fixture_provider_state_s {
  bool started;
  bool stopping;
  unsigned owner_quiesce_calls;
  unsigned owner_drain_calls;
  unsigned owner_shutdown_calls;
  unsigned owner_poll_calls;
  unsigned owner_destroy_calls;
  unsigned consumed;
  unsigned last_marker;
} fixture_provider_state_t;
#endif
