#ifndef TURBO_FLOW_CNET_TEST_INTERNAL_H
#define TURBO_FLOW_CNET_TEST_INTERNAL_H

#if !defined(TURBO_FLOW_CNET_INTERNAL_TESTING)
  #error "TurboFlow CNet test hooks are private to internal test targets"
#endif

#include "turbo_flow_cnet.h"
#include <stdbool.h>

/* One-shot faults at the adapter/native cleanup boundary; real SDK storage is
 * retained until the subsequent explicit cleanup attempt succeeds. */
typedef enum turbo_flow_cnet_test_cleanup_step_e {
  TF_CNET_TEST_CLIENT_STOP,
  TF_CNET_TEST_CLIENT_DESTROY,
  TF_CNET_TEST_LISTENER_CLOSE,
  TF_CNET_TEST_LISTENER_DESTROY,
  TF_CNET_TEST_TLS_DESTROY,
  TF_CNET_TEST_SCHEDULER_SHUTDOWN
} turbo_flow_cnet_test_cleanup_step;

typedef struct turbo_flow_cnet_test_cleanup_fault_s {
  turbo_flow_cnet_test_cleanup_step step;
  int status;
  bool after_drain;
} turbo_flow_cnet_test_cleanup_fault;

static inline int turbo_flow_cnet_test_cleanup_take(
    turbo_flow_cnet_test_cleanup_fault *fault, turbo_flow_cnet_test_cleanup_step step) {
  if (fault->step != step) return SALTS_OK;
  int status = fault->status;
  fault->status = SALTS_OK;
  return status;
}

int turbo_flow_cnet_test_stream_source_open(
    const turbo_flow_cnet_stream_source_config_t *config,
    turbo_flow_cnet_stream_source_t **source_out, turbo_flow_cnet_test_cleanup_fault fault);
int turbo_flow_cnet_test_stream_source_fail_next_cleanup(
    turbo_flow_cnet_stream_source_t *source, turbo_flow_cnet_test_cleanup_fault fault);

int turbo_flow_cnet_test_listener_source_open(
    const turbo_flow_cnet_listener_source_config_t *config,
    turbo_flow_cnet_listener_source_t **source_out, turbo_flow_cnet_test_cleanup_fault fault);
int turbo_flow_cnet_test_listener_source_fail_next_cleanup(
    turbo_flow_cnet_listener_source_t *source, turbo_flow_cnet_test_cleanup_fault fault);

int turbo_flow_cnet_test_packet_source_open(
    const turbo_flow_cnet_packet_source_config_t *config,
    turbo_flow_cnet_packet_source_t **source_out, int stop_status, int destroy_status);
int turbo_flow_cnet_test_packet_source_fail_next_stop(
    turbo_flow_cnet_packet_source_t *source, int status);

int turbo_flow_cnet_test_datagram_sink_fail_next_stop(
    turbo_flow_cnet_datagram_sink_t *sink, int status, bool after_drain);
int turbo_flow_cnet_test_datagram_sink_fail_next_destroy(
    turbo_flow_cnet_datagram_sink_t *sink, int status);
int turbo_flow_cnet_test_datagram_sink_submit_ready(turbo_flow_cnet_datagram_sink_t *sink);

int turbo_flow_cnet_test_stream_sink_fail_next_stop(
    turbo_flow_cnet_stream_sink_t *sink, int status, bool after_drain);
int turbo_flow_cnet_test_stream_sink_fail_next_destroy(
    turbo_flow_cnet_stream_sink_t *sink, int status);

int turbo_flow_cnet_test_packet_sink_fail_next_stop(
    turbo_flow_cnet_packet_sink_t *sink, int status, bool after_drain);
int turbo_flow_cnet_test_packet_sink_fail_next_destroy(
    turbo_flow_cnet_packet_sink_t *sink, int status);
int turbo_flow_cnet_test_packet_sink_submit_ready(turbo_flow_cnet_packet_sink_t *sink);

int turbo_flow_cnet_test_packet_sink_replay_last_terminal(
    turbo_flow_cnet_packet_sink_t *sink);

#endif
