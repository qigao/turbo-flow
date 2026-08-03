/*
 * Flowie / FlowMQ lifecycle model.
 *
 * Scope:
 * - Flowie TCP MQTT control flow: CONNECT, PINGREQ/PINGRESP, DISCONNECT, EOF.
 * - FlowMQ accepted async send: owner completion or shutdown completion.
 *
 * This is an abstraction of state ownership and ordering, not generated C code
 * and not a model of TCP buffering, MQTT parsing, or CoroNet implementation details.
 */

mtype = { CONNECT, CONNACK, PINGREQ, PINGRESP, DISCONNECT };

chan client_to_server = [1] of { mtype };
chan server_to_client = [1] of { mtype };

bool flowie_connected = false;
bool flowie_ping_response_seen = false;
bool flowie_disconnect_sent = false;
bool flowie_disconnect_received = false;
bool flowie_server_closed = false;

bool fmq_accepted = false;
bool fmq_shutdown_requested = false;
bool fmq_completed = false;
bool fmq_completed_success = false;
bool fmq_completed_shutdown = false;
byte fmq_completion_count = 0;

/* Mirrors the raw framing test's expected graceful control sequence. */
proctype FlowieClient()
{
  client_to_server!CONNECT;
  server_to_client?CONNACK;

  client_to_server!PINGREQ;
  server_to_client?PINGRESP;
  flowie_ping_response_seen = true;

  /* A PINGRESP is non-terminal; the peer must still accept DISCONNECT. */
  assert(!flowie_server_closed);
  client_to_server!DISCONNECT;
  flowie_disconnect_sent = true;
}

/* Flowie emits PINGRESP without a terminal-close request. */
proctype FlowieServer()
{
  client_to_server?CONNECT;
  flowie_connected = true;
  server_to_client!CONNACK;

  client_to_server?PINGREQ;
  server_to_client!PINGRESP;

  client_to_server?DISCONNECT;
  flowie_disconnect_received = true;
  flowie_server_closed = true;
}

/*
 * A producer may enqueue before shutdown. Once accepted, one terminal
 * completion is required regardless of whether shutdown wins the race.
 */
proctype FlowMQProducer()
{
  if
  :: !fmq_shutdown_requested -> fmq_accepted = true
  :: fmq_shutdown_requested -> skip
  fi
}

proctype FlowMQShutdown()
{
  fmq_shutdown_requested = true;
}

proctype FlowMQOwner()
{
  do
  :: fmq_accepted && !fmq_completed ->
    if
    :: fmq_shutdown_requested -> fmq_completed_shutdown = true
    :: !fmq_shutdown_requested -> fmq_completed_success = true
    fi;
    fmq_completion_count++;
    fmq_completed = true
  :: fmq_completed -> break
  od
}

/* Safety properties: no terminal close before DISCONNECT and no double completion. */
ltl no_early_flowie_eof {
  [] (!flowie_server_closed || flowie_disconnect_received)
}

ltl no_duplicate_flowmq_completion {
  [] (fmq_completion_count <= 1)
}

ltl flowie_disconnect_is_delivered {
  [] (flowie_disconnect_sent -> <> flowie_disconnect_received)
}

ltl accepted_flowmq_send_completes_once {
  [] (fmq_accepted -> <> (fmq_completed && fmq_completion_count == 1))
}

init
{
  atomic {
    run FlowieClient();
    run FlowieServer();
    run FlowMQProducer();
    run FlowMQShutdown();
    run FlowMQOwner();
  }
}
