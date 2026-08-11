#include "flowmq_coronet_transport.h"
#include "tinytest.h"
#include "turbo_error.h"

spec("flowmq_coronet_transport") {
  it("maps every FlowMQ transport to one CoroNet transport") {
    check_int_eq(flowmq_coronet_transport_coronet(FLOWMQ_TRANSPORT_TCP), TF_CORONET_TRANSPORT_TCP);
    check_int_eq(flowmq_coronet_transport_coronet(FLOWMQ_TRANSPORT_TLS), TF_CORONET_TRANSPORT_TLS);
    check_int_eq(flowmq_coronet_transport_coronet(FLOWMQ_TRANSPORT_UDP), TF_CORONET_TRANSPORT_UDP);
    check_int_eq(flowmq_coronet_transport_coronet(FLOWMQ_TRANSPORT_KCP), TF_CORONET_TRANSPORT_KCP);
    check_int_eq(flowmq_coronet_transport_coronet(FLOWMQ_TRANSPORT_PIPE),
                 TF_CORONET_TRANSPORT_PIPE);
    check_int_eq(flowmq_coronet_transport_coronet(FLOWMQ_TRANSPORT_WS), TF_CORONET_TRANSPORT_WS);
    check_int_eq(flowmq_coronet_transport_coronet(FLOWMQ_TRANSPORT_WSS), TF_CORONET_TRANSPORT_WSS);
    check_int_eq(flowmq_coronet_transport_coronet((flowmq_coronet_transport_t)0),
                 TF_CORONET_TRANSPORT_COUNT);
  }

  it("rejects invalid socket operations before CoroNet access") {
    tf_coronet_socket_timeout_config_t timeouts = {0};
    tf_coronet_udp_options_t udp = {0};
    tf_coronet_socket_options_t options = {0};

    check_int_eq(flowmq_coronet_transport_apply(NULL, FLOWMQ_TRANSPORT_TCP, NULL, 0, &options),
                 TURBO_EINVAL);
    check_int_eq(flowmq_coronet_transport_connect(NULL, FLOWMQ_TRANSPORT_TCP, "127.0.0.1", 7001,
                                                  NULL, NULL, &timeouts, &udp),
                 TURBO_EINVAL);
    check_int_eq(flowmq_coronet_transport_send(NULL, FLOWMQ_TRANSPORT_TCP, &timeouts, "x", 1u),
                 TURBO_EINVAL);
  }
}
