#include "flow_coronet_runtime.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

spec("flow_coronet_runtime") {
  it("exposes stable socket transport option values") {
    check_int_eq(TF_CORONET_TRANSPORT_VALUE_COUNT, 7);
    check_str_eq(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_TCP], "tcp");
    check_str_eq(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_UDP], "udp");
    check_str_eq(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_KCP], "kcp");
    check_str_eq(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_TLS], "tls");
    check_str_eq(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_WS], "ws");
    check_str_eq(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_WSS], "wss");
    check_str_eq(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_PIPE], "pipe");
    check_str_eq(TF_CORONET_KCP_FEC_BACKEND_VALUES[TURBO_KCP_FEC_BACKEND_NONE], "none");
    check_str_eq(TF_CORONET_KCP_FEC_BACKEND_VALUES[TURBO_KCP_FEC_BACKEND_WIREHAIR], "wirehair");
  }

  it("validates transport families") {
    check(tf_coronet_transport_valid(TF_CORONET_TRANSPORT_TCP));
    check(tf_coronet_transport_valid(TF_CORONET_TRANSPORT_PIPE));
    check(!tf_coronet_transport_valid(TF_CORONET_TRANSPORT_COUNT));
    check(tf_coronet_transport_is_ws(TF_CORONET_TRANSPORT_WS));
    check(tf_coronet_transport_is_ws(TF_CORONET_TRANSPORT_WSS));
    check(!tf_coronet_transport_is_ws(TF_CORONET_TRANSPORT_TCP));
    check(tf_coronet_transport_is_pipe(TF_CORONET_TRANSPORT_PIPE));
  }

  it("normalizes pipe and websocket endpoints") {
    check_str_eq(tf_coronet_endpoint("host-pipe", NULL), "host-pipe");
    check_str_eq(tf_coronet_endpoint("host-pipe", ""), "host-pipe");
    check_str_eq(tf_coronet_endpoint("host-pipe", "named-pipe"), "named-pipe");
    check_str_eq(tf_coronet_ws_path(NULL), "/");
    check_str_eq(tf_coronet_ws_path(""), "/");
    check_str_eq(tf_coronet_ws_path("/events"), "/events");
  }

  it("validates endpoint config shape by transport") {
    check_int_eq(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_TCP, "127.0.0.1", 7001, NULL),
        TURBO_OK);
    check_int_eq(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_UDP, "127.0.0.1", 7001, NULL),
        TURBO_OK);
    check_int_eq(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_KCP, "127.0.0.1", 7001, NULL),
        TURBO_OK);
    check_int_eq(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_PIPE, NULL, 0, "pipe://endpoint"),
        TURBO_OK);
    check_int_eq(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_PIPE, "pipe://endpoint", 0, NULL),
        TURBO_OK);
    check_int_eq(tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_TCP, NULL, 7001, NULL),
                 TURBO_EINVAL);
    check_int_eq(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_TCP, "127.0.0.1", 0, NULL),
        TURBO_EINVAL);
    check_int_eq(tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_PIPE, NULL, 0, NULL),
                 TURBO_EINVAL);
    check_int_eq(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_COUNT, "127.0.0.1", 7001, NULL),
        TURBO_EINVAL);
  }

  it("rejects invalid send arguments before touching sockets") {
    check_int_eq(tf_coronet_send_socket(NULL, TF_CORONET_TRANSPORT_TCP, "x", 1), TURBO_EINVAL);
    check_int_eq(tf_coronet_send_socket(NULL, TF_CORONET_TRANSPORT_COUNT, "x", 1), TURBO_EINVAL);
  }

  it("resolves timeout aliases with fallback defaults") {
    tf_coronet_socket_timeout_config_t timeouts = {0};
    tf_coronet_socket_timeout_config_t specific = {0};
    tf_coronet_socket_timeout_config_t handshake_only = {0};
    tf_coronet_socket_timeout_config_t connected = {0};
    tf_coronet_socket_timeouts_resolve(&timeouts, 1000);
    check_uint_eq(timeouts.connect_timeout_ms, 1000);
    check_uint_eq(timeouts.send_timeout_ms, 1000);
    check_uint_eq(timeouts.recv_timeout_ms, 1000);
    check_uint_eq(timeouts.handshake_timeout_ms, 1000);

    specific.connect_timeout_ms = 200;
    tf_coronet_socket_timeouts_resolve(&specific, 1000);
    check_uint_eq(specific.connect_timeout_ms, 200);
    check_uint_eq(specific.send_timeout_ms, 1000);

    handshake_only.timeout_ms = 3000;
    handshake_only.handshake_timeout_ms = 500;
    tf_coronet_socket_timeout_config_t resolved = handshake_only;
    tf_coronet_socket_timeouts_resolve(&resolved, 3000);
    check_uint_eq(resolved.handshake_timeout_ms, 500);
    check_uint_eq(resolved.send_timeout_ms, 3000);

    connected.connect_timeout_ms = 250;
    connected.send_timeout_ms = 750;
    connected.timeout_ms = 111;
    tf_coronet_socket_timeouts_resolve(&connected, 111);
    check_uint_eq(connected.connect_timeout_ms, 250);
    check_uint_eq(connected.send_timeout_ms, 750);
    check_uint_eq(connected.recv_timeout_ms, 111);
    check_uint_eq(connected.handshake_timeout_ms, 111);
  }

  it("preserves explicitly disabled operation timeouts") {
    tf_coronet_socket_timeout_config_t timeouts = {0};
    timeouts.timeout_ms = 900;
    timeouts.send_timeout_ms = 25;
    timeouts.set_flags = TF_CORONET_TIMEOUT_SET_DEFAULT | TF_CORONET_TIMEOUT_SET_CONNECT |
                         TF_CORONET_TIMEOUT_SET_SEND;

    tf_coronet_socket_timeouts_resolve(&timeouts, 1000);

    check_uint_eq(timeouts.timeout_ms, 900);
    check_uint_eq(timeouts.connect_timeout_ms, 0);
    check_uint_eq(timeouts.send_timeout_ms, 25);
    check_uint_eq(timeouts.recv_timeout_ms, 900);
    check_uint_eq(timeouts.handshake_timeout_ms, 900);
    check_uint_eq(timeouts.set_flags, TF_CORONET_TIMEOUT_SET_ALL);
  }

  it("allows an explicit zero default timeout") {
    tf_coronet_socket_timeout_config_t timeouts = {0};
    timeouts.set_flags = TF_CORONET_TIMEOUT_SET_DEFAULT;

    tf_coronet_socket_timeouts_resolve(&timeouts, 1000);

    check_uint_eq(timeouts.timeout_ms, 0);
    check_uint_eq(timeouts.connect_timeout_ms, 0);
    check_uint_eq(timeouts.send_timeout_ms, 0);
    check_uint_eq(timeouts.recv_timeout_ms, 0);
    check_uint_eq(timeouts.handshake_timeout_ms, 0);
  }

  it("resolves one connect and handshake deadline per CoroNet wait") {
    tf_coronet_socket_timeout_config_t timeouts = {0};
    uint64_t timeout_ms = 0;

    timeouts.timeout_ms = 1000;
    timeouts.connect_timeout_ms = 250;
    timeouts.set_flags = TF_CORONET_TIMEOUT_SET_DEFAULT | TF_CORONET_TIMEOUT_SET_CONNECT;
    tf_coronet_socket_timeouts_resolve(&timeouts, 1000);
    check_int_eq(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_TCP, &timeouts,
                                                        &timeout_ms),
                 TURBO_OK);
    check_uint_eq(timeout_ms, 250);
    check_int_eq(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_TLS, &timeouts,
                                                        &timeout_ms),
                 TURBO_OK);
    check_uint_eq(timeout_ms, 250);

    memset(&timeouts, 0, sizeof(timeouts));
    timeouts.timeout_ms = 1000;
    timeouts.handshake_timeout_ms = 400;
    timeouts.set_flags = TF_CORONET_TIMEOUT_SET_DEFAULT | TF_CORONET_TIMEOUT_SET_HANDSHAKE;
    tf_coronet_socket_timeouts_resolve(&timeouts, 1000);
    check_int_eq(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_WS, &timeouts,
                                                        &timeout_ms),
                 TURBO_OK);
    check_uint_eq(timeout_ms, 400);
    check_int_eq(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_WSS, &timeouts,
                                                        &timeout_ms),
                 TURBO_OK);
    check_uint_eq(timeout_ms, 400);
    check_int_eq(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_TCP, &timeouts,
                                                        &timeout_ms),
                 TURBO_EINVAL);

    timeouts.connect_timeout_ms = 200;
    timeouts.explicit_flags |= TF_CORONET_TIMEOUT_SET_CONNECT;
    check_int_eq(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_TLS, &timeouts,
                                                        &timeout_ms),
                 TURBO_EINVAL);
    timeouts.connect_timeout_ms = 400;
    check_int_eq(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_TLS, &timeouts,
                                                        &timeout_ms),
                 TURBO_OK);
    check_uint_eq(timeout_ms, 400);
  }

  it("validates KCP FEC options before socket bind or connect") {
    tf_coronet_kcp_fec_options_t options = {0};
    turbo_kcp_fec_config_t config;
    int configured = 1;
    int rc;

    check_int_eq(tf_coronet_kcp_fec_options_resolve(TF_CORONET_TRANSPORT_TCP, &options, &config,
                                                    &configured),
                 TURBO_OK);
    check_int_eq(configured, 0);
    check_int_eq(config.enabled, 0);

    options.enabled = 1;
    options.backend = TURBO_KCP_FEC_BACKEND_WIREHAIR;
    options.data_shards = 4;
    options.parity_shards = 2;
    options.max_payload_size = 1200;
    check_int_eq(tf_coronet_kcp_fec_options_resolve(TF_CORONET_TRANSPORT_TCP, &options, &config,
                                                    &configured),
                 TURBO_EINVAL);

    options.backend = TURBO_KCP_FEC_BACKEND_NONE;
    check_int_eq(tf_coronet_kcp_fec_options_resolve(TF_CORONET_TRANSPORT_KCP, &options, &config,
                                                    &configured),
                 TURBO_EINVAL);

    options.backend = TURBO_KCP_FEC_BACKEND_WIREHAIR;
    rc = tf_coronet_kcp_fec_options_resolve(TF_CORONET_TRANSPORT_KCP, &options, &config,
                                            &configured);
    if (turbo_kcp_fec_backend_available(TURBO_KCP_FEC_BACKEND_WIREHAIR)) {
      check_int_eq(rc, TURBO_OK);
      check_int_eq(configured, 1);
      check_int_eq(config.enabled, 1);
      check_int_eq(config.backend, TURBO_KCP_FEC_BACKEND_WIREHAIR);
      check_uint_eq(config.data_shards, 4);
      check_uint_eq(config.parity_shards, 2);
      check_uint_eq(config.max_payload_size, 1200);
    } else {
      check_int_eq(rc, TURBO_ENOTSUP);
    }

    check_int_eq(tf_coronet_apply_kcp_fec(NULL, TF_CORONET_TRANSPORT_KCP, &config, 0), TURBO_OK);
    check_int_eq(tf_coronet_apply_kcp_fec(NULL, TF_CORONET_TRANSPORT_KCP, &config, 1),
                 TURBO_EINVAL);
  }

  it("validates CoroNet socket primitive option matrices") {
    tf_coronet_socket_options_t options = {0};

    options.tcp_keepalive = 1;
    options.tcp_keepalive_idle_ms = 30000;
    options.tcp_keepalive_interval_ms = 5000;
    options.tcp_keepalive_count = 3;
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TCP, &options), TURBO_OK);
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TLS, &options), TURBO_OK);
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_WS, &options), TURBO_OK);
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_WSS, &options), TURBO_OK);
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_UDP, &options),
                 TURBO_EINVAL);
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_KCP, &options),
                 TURBO_EINVAL);
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_PIPE, &options),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.tcp_keepalive_idle_ms = 30000;
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TCP, &options),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.linger = 1;
    options.linger_ms = 0;
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TCP, &options), TURBO_OK);
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_PIPE, &options),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.send_hwm_bytes = 4096;
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TCP, &options), TURBO_OK);
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_PIPE, &options), TURBO_OK);
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_UDP, &options),
                 TURBO_EINVAL);
    check_int_eq(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_KCP, &options),
                 TURBO_EINVAL);

    check_int_eq(tf_coronet_apply_socket_options(NULL, TF_CORONET_TRANSPORT_TCP, &options),
                 TURBO_EINVAL);
  }

  it("restricts reuse-port to non-pipe listeners") {
    check_int_eq(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_TCP, 1, 1), TURBO_OK);
    check_int_eq(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_UDP, 1, 1), TURBO_OK);
    check_int_eq(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_KCP, 1, 1), TURBO_OK);
    check_int_eq(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_WSS, 1, 1), TURBO_OK);
    check_int_eq(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_TCP, 1, 0), TURBO_EINVAL);
    check_int_eq(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_PIPE, 1, 1), TURBO_EINVAL);
    check_int_eq(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_COUNT, 0, 0),
                 TURBO_EINVAL);
    check_int_eq(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_PIPE, 0, 0), TURBO_OK);
  }

  it("validates UDP multicast and broadcast option presence") {
    tf_coronet_udp_options_t options = {0};

    check_int_eq(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_OK);
    options.multicast_group = "239.255.0.1";
    check_int_eq(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_EINVAL);
    check_int_eq(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 1),
                 TURBO_OK);
    options.multicast_interface = "127.0.0.1";
    check_int_eq(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 1),
                 TURBO_OK);
    check_int_eq(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_TCP, &options, 1),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.multicast_interface = "127.0.0.1";
    check_int_eq(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 1),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.option_flags = TF_CORONET_UDP_OPTION_MULTICAST_LOOP |
                           TF_CORONET_UDP_OPTION_MULTICAST_TTL |
                           TF_CORONET_UDP_OPTION_BROADCAST;
    options.multicast_loop = 1;
    options.multicast_ttl = 255;
    options.broadcast = 1;
    check_int_eq(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_OK);
    options.multicast_ttl = 256;
    check_int_eq(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_ERANGE);
    options.multicast_ttl = 1;
    options.option_flags |= 1u << 31;
    check_int_eq(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.multicast_ttl = 1;
    check_int_eq(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_EINVAL);
    options.multicast_ttl = 0;
    options.broadcast = 1;
    check_int_eq(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_EINVAL);
  }
}
