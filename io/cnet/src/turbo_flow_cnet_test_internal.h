#ifndef TURBO_FLOW_CNET_TEST_INTERNAL_H
#define TURBO_FLOW_CNET_TEST_INTERNAL_H

#if !defined(TURBO_FLOW_CNET_INTERNAL_TESTING)
  #error "TurboFlow CNet test hooks are private to internal test targets"
#endif

#include "turbo_flow_cnet.h"

int turbo_flow_cnet_test_packet_sink_fail_next_stop(
    turbo_flow_cnet_packet_sink_t *sink, int status);

int turbo_flow_cnet_test_packet_sink_replay_last_terminal(
    turbo_flow_cnet_packet_sink_t *sink);

#endif
