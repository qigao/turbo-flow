#include "flow_coronet_runtime.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <limits.h>
#include <string.h>

spec("flow_coronet_runtime") {
  it("exposes stable socket transport option values") {
    check_equal(TF_CORONET_TRANSPORT_VALUE_COUNT, 7);
    check_equal(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_TCP], "tcp");
    check_equal(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_UDP], "udp");
    check_equal(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_KCP], "kcp");
    check_equal(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_TLS], "tls");
    check_equal(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_WS], "ws");
    check_equal(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_WSS], "wss");
    check_equal(TF_CORONET_TRANSPORT_VALUES[TF_CORONET_TRANSPORT_PIPE], "pipe");
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
    check_equal(tf_coronet_endpoint("host-pipe", NULL), "host-pipe");
    check_equal(tf_coronet_endpoint("host-pipe", ""), "host-pipe");
    check_equal(tf_coronet_endpoint("host-pipe", "named-pipe"), "named-pipe");
    check_equal(tf_coronet_ws_path(NULL), "/");
    check_equal(tf_coronet_ws_path(""), "/");
    check_equal(tf_coronet_ws_path("/events"), "/events");
  }

  it("validates endpoint config shape by transport") {
    check_equal(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_TCP, "127.0.0.1", 7001, NULL),
        TURBO_OK);
    check_equal(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_UDP, "127.0.0.1", 7001, NULL),
        TURBO_OK);
    check_equal(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_KCP, "127.0.0.1", 7001, NULL),
        TURBO_OK);
    check_equal(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_PIPE, NULL, 0, "pipe://endpoint"),
        TURBO_OK);
    check_equal(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_PIPE, "pipe://endpoint", 0, NULL),
        TURBO_OK);
    check_equal(tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_TCP, NULL, 7001, NULL),
                 TURBO_EINVAL);
    check_equal(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_TCP, "127.0.0.1", 0, NULL),
        TURBO_EINVAL);
    check_equal(tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_PIPE, NULL, 0, NULL),
                 TURBO_EINVAL);
    check_equal(
        tf_coronet_endpoint_config_validate(TF_CORONET_TRANSPORT_COUNT, "127.0.0.1", 7001, NULL),
        TURBO_EINVAL);
  }

  it("rejects invalid send arguments before touching sockets") {
    check_equal(tf_coronet_send_socket(NULL, TF_CORONET_TRANSPORT_TCP, "x", 1), TURBO_EINVAL);
    check_equal(tf_coronet_send_socket(NULL, TF_CORONET_TRANSPORT_COUNT, "x", 1), TURBO_EINVAL);
  }

  it("resolves timeout aliases with fallback defaults") {
    tf_coronet_socket_timeout_config_t timeouts = {0};
    tf_coronet_socket_timeout_config_t specific = {0};
    tf_coronet_socket_timeout_config_t handshake_only = {0};
    tf_coronet_socket_timeout_config_t connected = {0};
    tf_coronet_socket_timeouts_resolve(&timeouts, 1000);
    check_equal(timeouts.connect_timeout_ms, 1000);
    check_equal(timeouts.send_timeout_ms, 1000);
    check_equal(timeouts.recv_timeout_ms, 1000);
    check_equal(timeouts.handshake_timeout_ms, 1000);

    specific.connect_timeout_ms = 200;
    tf_coronet_socket_timeouts_resolve(&specific, 1000);
    check_equal(specific.connect_timeout_ms, 200);
    check_equal(specific.send_timeout_ms, 1000);

    handshake_only.timeout_ms = 3000;
    handshake_only.handshake_timeout_ms = 500;
    tf_coronet_socket_timeout_config_t resolved = handshake_only;
    tf_coronet_socket_timeouts_resolve(&resolved, 3000);
    check_equal(resolved.handshake_timeout_ms, 500);
    check_equal(resolved.send_timeout_ms, 3000);

    connected.connect_timeout_ms = 250;
    connected.send_timeout_ms = 750;
    connected.timeout_ms = 111;
    tf_coronet_socket_timeouts_resolve(&connected, 111);
    check_equal(connected.connect_timeout_ms, 250);
    check_equal(connected.send_timeout_ms, 750);
    check_equal(connected.recv_timeout_ms, 111);
    check_equal(connected.handshake_timeout_ms, 111);
  }

  it("preserves explicitly disabled operation timeouts") {
    tf_coronet_socket_timeout_config_t timeouts = {0};
    timeouts.timeout_ms = 900;
    timeouts.send_timeout_ms = 25;
    timeouts.set_flags = TF_CORONET_TIMEOUT_SET_DEFAULT | TF_CORONET_TIMEOUT_SET_CONNECT |
                         TF_CORONET_TIMEOUT_SET_SEND;

    tf_coronet_socket_timeouts_resolve(&timeouts, 1000);

    check_equal(timeouts.timeout_ms, 900);
    check_equal(timeouts.connect_timeout_ms, 0);
    check_equal(timeouts.send_timeout_ms, 25);
    check_equal(timeouts.recv_timeout_ms, 900);
    check_equal(timeouts.handshake_timeout_ms, 900);
    check_equal(timeouts.set_flags, TF_CORONET_TIMEOUT_SET_ALL);
  }

  it("allows an explicit zero default timeout") {
    tf_coronet_socket_timeout_config_t timeouts = {0};
    timeouts.set_flags = TF_CORONET_TIMEOUT_SET_DEFAULT;

    tf_coronet_socket_timeouts_resolve(&timeouts, 1000);

    check_equal(timeouts.timeout_ms, 0);
    check_equal(timeouts.connect_timeout_ms, 0);
    check_equal(timeouts.send_timeout_ms, 0);
    check_equal(timeouts.recv_timeout_ms, 0);
    check_equal(timeouts.handshake_timeout_ms, 0);
  }

  it("resolves one connect and handshake deadline per CoroNet wait") {
    tf_coronet_socket_timeout_config_t timeouts = {0};
    uint64_t timeout_ms = 0;

    timeouts.timeout_ms = 1000;
    timeouts.connect_timeout_ms = 250;
    timeouts.set_flags = TF_CORONET_TIMEOUT_SET_DEFAULT | TF_CORONET_TIMEOUT_SET_CONNECT;
    tf_coronet_socket_timeouts_resolve(&timeouts, 1000);
    check_equal(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_TCP, &timeouts,
                                                        &timeout_ms),
                 TURBO_OK);
    check_equal(timeout_ms, 250);
    check_equal(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_TLS, &timeouts,
                                                        &timeout_ms),
                 TURBO_OK);
    check_equal(timeout_ms, 250);

    memset(&timeouts, 0, sizeof(timeouts));
    timeouts.timeout_ms = 1000;
    timeouts.handshake_timeout_ms = 400;
    timeouts.set_flags = TF_CORONET_TIMEOUT_SET_DEFAULT | TF_CORONET_TIMEOUT_SET_HANDSHAKE;
    tf_coronet_socket_timeouts_resolve(&timeouts, 1000);
    check_equal(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_WS, &timeouts,
                                                        &timeout_ms),
                 TURBO_OK);
    check_equal(timeout_ms, 400);
    check_equal(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_WSS, &timeouts,
                                                        &timeout_ms),
                 TURBO_OK);
    check_equal(timeout_ms, 400);
    check_equal(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_TCP, &timeouts,
                                                        &timeout_ms),
                 TURBO_EINVAL);

    timeouts.connect_timeout_ms = 200;
    timeouts.explicit_flags |= TF_CORONET_TIMEOUT_SET_CONNECT;
    check_equal(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_TLS, &timeouts,
                                                        &timeout_ms),
                 TURBO_EINVAL);
    timeouts.connect_timeout_ms = 400;
    check_equal(tf_coronet_connection_timeout_resolve(TF_CORONET_TRANSPORT_TLS, &timeouts,
                                                        &timeout_ms),
                 TURBO_OK);
    check_equal(timeout_ms, 400);
  }

  it("validates one authenticated KCP config before socket bind or connect") {
    tf_coronet_kcp_options_t options = {0};
    turbo_kcp_config_t config;
    int configured = 1;

    check_equal(tf_coronet_kcp_options_resolve(TF_CORONET_TRANSPORT_TCP, &options, &config,
                                                &configured),
                 TURBO_OK);
    check_equal(configured, 0);

    memset(options.pre_shared_key, 0x5a, sizeof(options.pre_shared_key));
    options.mtu = 1200;
    options.send_window = 256;
    options.receive_window = 256;
    options.interval_ms = 5;
    options.handshake_retry_ms = 200;
    options.fast_resend = 2;
    options.no_congestion_window = 1;
    options.data_shards = 4;
    options.parity_shards = 2;
    options.max_payload_size = 1248;
    options.receive_group_count = 16;
    check_equal(tf_coronet_kcp_options_resolve(TF_CORONET_TRANSPORT_TCP, &options, &config,
                                                &configured),
                 TURBO_EINVAL);

    check_equal(tf_coronet_kcp_options_resolve(TF_CORONET_TRANSPORT_KCP, &options, &config,
                                                &configured),
                 TURBO_OK);
    check_equal(configured, 1);
    check_equal(config.mtu, 1200);
    check_equal(config.send_window, 256);
    check_equal(config.receive_window, 256);
    check_equal(config.interval_ms, 5);
    check_equal(config.handshake_retry_ms, 200);
    check_equal(config.fast_resend, 2);
    check_equal(config.no_congestion_window, 1);
    check_equal(config.fec.backend, TURBO_KCP_FEC_BACKEND_REED_SOLOMON);
    check_equal(config.fec.data_shards, 4);
    check_equal(config.fec.parity_shards, 2);
    check_equal(config.fec.max_payload_size, 1248);
    check_equal(config.fec.receive_group_count, 16);

    check_equal(tf_coronet_apply_kcp_config(NULL, TF_CORONET_TRANSPORT_KCP, &config, 0),
                 TURBO_OK);
    check_equal(tf_coronet_apply_kcp_config(NULL, TF_CORONET_TRANSPORT_KCP, &config, 1),
                 TURBO_EINVAL);
    turbo_kcp_config_wipe(&config);
  }

  it("validates CoroNet socket primitive option matrices") {
    tf_coronet_socket_options_t options = {0};

    options.tcp_keepalive = 1;
    options.tcp_keepalive_idle_ms = 30000;
    options.tcp_keepalive_interval_ms = 5000;
    options.tcp_keepalive_count = 3;
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TCP, &options), TURBO_OK);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TLS, &options), TURBO_OK);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_WS, &options), TURBO_OK);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_WSS, &options), TURBO_OK);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_UDP, &options),
                 TURBO_EINVAL);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_KCP, &options),
                 TURBO_EINVAL);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_PIPE, &options),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.tcp_keepalive_idle_ms = 30000;
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TCP, &options),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.linger = 1;
    options.linger_ms = 0;
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TCP, &options), TURBO_OK);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_PIPE, &options),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.send_hwm_bytes = 4096;
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TCP, &options), TURBO_OK);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_PIPE, &options), TURBO_OK);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_UDP, &options),
                 TURBO_EINVAL);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_KCP, &options),
                 TURBO_EINVAL);

    check_equal(tf_coronet_apply_socket_options(NULL, TF_CORONET_TRANSPORT_TCP, &options),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.socket_recv_buffer_bytes = 1024u * 1024u;
    options.socket_send_buffer_bytes = 512u * 1024u;
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TCP, &options), TURBO_OK);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TLS, &options), TURBO_OK);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_WS, &options), TURBO_OK);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_WSS, &options), TURBO_OK);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_UDP, &options),
                 TURBO_EINVAL);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_KCP, &options),
                 TURBO_EINVAL);
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_PIPE, &options),
                 TURBO_EINVAL);

    options.socket_recv_buffer_bytes = (size_t)INT_MAX + 1u;
    options.socket_send_buffer_bytes = 0;
    check_equal(tf_coronet_socket_options_validate(TF_CORONET_TRANSPORT_TCP, &options),
                 TURBO_ERANGE);
  }

  it("restricts reuse-port to non-pipe listeners") {
    check_equal(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_TCP, 1, 1), TURBO_OK);
    check_equal(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_UDP, 1, 1), TURBO_OK);
    check_equal(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_KCP, 1, 1), TURBO_OK);
    check_equal(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_WSS, 1, 1), TURBO_OK);
    check_equal(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_TCP, 1, 0), TURBO_EINVAL);
    check_equal(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_PIPE, 1, 1), TURBO_EINVAL);
    check_equal(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_COUNT, 0, 0),
                 TURBO_EINVAL);
    check_equal(tf_coronet_reuse_port_validate(TF_CORONET_TRANSPORT_PIPE, 0, 0), TURBO_OK);
  }

  it("validates UDP multicast and broadcast option presence") {
    tf_coronet_udp_options_t options = {0};

    check_equal(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_OK);
    options.multicast_group = "239.255.0.1";
    check_equal(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_EINVAL);
    check_equal(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 1),
                 TURBO_OK);
    options.multicast_interface = "127.0.0.1";
    check_equal(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 1),
                 TURBO_OK);
    check_equal(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_TCP, &options, 1),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.multicast_interface = "127.0.0.1";
    check_equal(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 1),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.option_flags = TF_CORONET_UDP_OPTION_MULTICAST_LOOP |
                           TF_CORONET_UDP_OPTION_MULTICAST_TTL |
                           TF_CORONET_UDP_OPTION_BROADCAST;
    options.multicast_loop = 1;
    options.multicast_ttl = 255;
    options.broadcast = 1;
    check_equal(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_OK);
    options.multicast_ttl = 256;
    check_equal(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_ERANGE);
    options.multicast_ttl = 1;
    options.option_flags |= 1u << 31;
    check_equal(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_EINVAL);

    memset(&options, 0, sizeof(options));
    options.multicast_ttl = 1;
    check_equal(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_EINVAL);
    options.multicast_ttl = 0;
    options.broadcast = 1;
    check_equal(tf_coronet_udp_options_validate(TF_CORONET_TRANSPORT_UDP, &options, 0),
                 TURBO_EINVAL);
  }
}
