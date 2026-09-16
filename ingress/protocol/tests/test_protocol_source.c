#include "salts_error.h"
#include "tinytest.h"
#include "turbo_flow_protocol.h"
#include "turbo_flow_protocol_plugin.h"
#include "turbo_flow_protocol_source.h"

#include <string.h>

typedef struct source_probe_s {
  size_t admit_calls;
  size_t successful_admits;
  size_t closes;
  size_t codec_replies;
  size_t capacity_failures;
  uint64_t last_delivery_id;
  uint64_t last_session_id;
  uint64_t last_generation;
  int inspect_status;
  int admit_status;
  int last_close_status;
  char last_operation[TURBO_FLOW_PROTOCOL_OPERATION_MAX + 1u];
  uint8_t last_payload[64];
  size_t last_payload_size;
} source_probe_t;

static int source_inspect(void *ctx, const char *configured_version,
                          const turbo_flow_protocol_frame_view_t *frame,
                          turbo_flow_protocol_metadata_t *metadata) {
  source_probe_t *probe = (source_probe_t *)ctx;
  (void)configured_version;
  if (!frame || !metadata || !frame->data || frame->data_size == 0u) return SALTS_EPROTO;
  if (probe && probe->inspect_status != SALTS_OK) return probe->inspect_status;
  metadata->message_type = frame->data[0];
  metadata->sequence = frame->data_size;
  memcpy(metadata->operation, "frame", sizeof("frame"));
  if (!frame->device_id) memcpy(metadata->device_id, "frame-device", sizeof("frame-device"));
  return SALTS_OK;
}

static int source_codec_reply(void *ctx, const char *configured_version,
                              const turbo_flow_protocol_frame_view_t *request, int status,
                              turbo_flow_protocol_frame_output_t *output) {
  source_probe_t *probe = (source_probe_t *)ctx;
  (void)configured_version;
  (void)request;
  (void)status;
  (void)output;
  if (probe) probe->codec_replies++;
  return SALTS_EPROTO;
}

static int source_admit(void *ctx, const turbo_flow_protocol_source_admit_request_t *request) {
  source_probe_t *probe = (source_probe_t *)ctx;
  if (!probe || !request || request->size != sizeof(*request) ||
      request->abi_version != TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION ||
      request->delivery_id == 0u || request->session_id == 0u ||
      request->session_generation == 0u || !request->message ||
      request->message->size != sizeof(*request->message) ||
      request->message->abi_version != TURBO_FLOW_PROTOCOL_ABI_VERSION)
    return SALTS_EINVAL;
  probe->admit_calls++;
  probe->last_delivery_id = request->delivery_id;
  probe->last_session_id = request->session_id;
  probe->last_generation = request->session_generation;
  if (probe->capacity_failures > 0u) {
    probe->capacity_failures--;
    return SALTS_ENOSPC;
  }
  if (probe->admit_status != SALTS_OK) return probe->admit_status;
  if (request->message->payload_size > sizeof(probe->last_payload)) return SALTS_EMSGSIZE;
  memcpy(probe->last_payload, request->message->payload, request->message->payload_size);
  probe->last_payload_size = request->message->payload_size;
  memcpy(probe->last_operation, request->message->metadata.operation,
         sizeof(probe->last_operation));
  probe->successful_admits++;
  return SALTS_OK;
}

static void source_closed(void *ctx, uint64_t session_id, uint64_t generation, int status) {
  source_probe_t *probe = (source_probe_t *)ctx;
  (void)session_id;
  (void)generation;
  probe->closes++;
  probe->last_close_status = status;
}

static int source_protocol_create(turbo_flow_protocol_kind_t protocol, source_probe_t *probe,
                                  turbo_flow_protocol_t **out) {
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_protocol_codec_ops_t ops = TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT;
  request.protocol = protocol;
  request.protocol_version = "test";
  request.max_frame_size = 64u;
  ops.inspect = source_inspect;
  ops.reply = source_codec_reply;
  return turbo_flow_protocol_create(
      &request, "source-test", "test",
      TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
          TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE | TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY,
      &ops, probe, out);
}

static int source_create(turbo_flow_protocol_t *protocol, source_probe_t *probe,
                         size_t max_sessions, turbo_flow_protocol_source_t **out) {
  turbo_flow_protocol_source_config_t config = TURBO_FLOW_PROTOCOL_SOURCE_CONFIG_INIT;
  turbo_flow_protocol_source_ops_t ops = TURBO_FLOW_PROTOCOL_SOURCE_OPS_INIT;
  config.max_sessions = max_sessions;
  config.max_frame_size = 64u;
  config.max_buffered_bytes = (max_sessions + 1u) * config.max_frame_size;
  ops.admit = source_admit;
  ops.session_closed = source_closed;
  return turbo_flow_protocol_source_create(protocol, &config, &ops, probe, out);
}

static int source_session_open(turbo_flow_protocol_source_t *source, uint64_t session_id,
                               const char *device_id) {
  turbo_flow_protocol_source_session_open_request_t request =
      TURBO_FLOW_PROTOCOL_SOURCE_SESSION_OPEN_REQUEST_INIT;
  request.session_id = session_id;
  request.generation = 1u;
  request.device_id = device_id;
  request.protocol_version = "test";
  return turbo_flow_protocol_source_session_open(source, &request);
}

static void source_gbt_frame(uint8_t frame[25], uint8_t marker) {
  memset(frame, marker, 25u);
  frame[0] = 0x23u;
  frame[1] = 0x23u;
  frame[22] = 0u;
  frame[23] = 0u;
}

spec("protocol source") {
  it("rejects every non-exact public ABI layout") {
    const uint8_t frame[] = {0x42u};
    source_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_source_t *source = NULL;
    turbo_flow_protocol_source_config_t config = TURBO_FLOW_PROTOCOL_SOURCE_CONFIG_INIT;
    turbo_flow_protocol_source_ops_t ops = TURBO_FLOW_PROTOCOL_SOURCE_OPS_INIT;
    turbo_flow_protocol_source_session_open_request_t open =
        TURBO_FLOW_PROTOCOL_SOURCE_SESSION_OPEN_REQUEST_INIT;
    turbo_flow_protocol_source_feed_result_t feed = TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    turbo_flow_protocol_source_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_SOURCE_SNAPSHOT_INIT;
    check_equal(TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION, 1u);
    check_equal(source_protocol_create(TURBO_FLOW_PROTOCOL_COAP, &probe, &protocol), SALTS_OK);
    config.max_sessions = 1u;
    config.max_frame_size = 64u;
    config.max_buffered_bytes = 128u;
    ops.admit = source_admit;
    config.size = sizeof(config) - 1u;
    check_equal(turbo_flow_protocol_source_create(protocol, &config, &ops, &probe, &source),
                SALTS_EINVAL);
    config.size = sizeof(config) + 1u;
    check_equal(turbo_flow_protocol_source_create(protocol, &config, &ops, &probe, &source),
                SALTS_EINVAL);
    config.size = sizeof(config);
    config.abi_version++;
    check_equal(turbo_flow_protocol_source_create(protocol, &config, &ops, &probe, &source),
                SALTS_EINVAL);
    config.abi_version = TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION;
    ops.size = sizeof(ops) - 1u;
    check_equal(turbo_flow_protocol_source_create(protocol, &config, &ops, &probe, &source),
                SALTS_EINVAL);
    ops.size = sizeof(ops) + 1u;
    check_equal(turbo_flow_protocol_source_create(protocol, &config, &ops, &probe, &source),
                SALTS_EINVAL);
    ops.size = sizeof(ops);
    ops.abi_version++;
    check_equal(turbo_flow_protocol_source_create(protocol, &config, &ops, &probe, &source),
                SALTS_EINVAL);
    ops.abi_version = TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION;
    check_equal(turbo_flow_protocol_source_create(protocol, &config, &ops, &probe, &source),
                SALTS_OK);
    open.session_id = 1u;
    open.generation = 1u;
    open.device_id = "sensor-1";
    open.protocol_version = "test";
    open.size = sizeof(open) - 1u;
    check_equal(turbo_flow_protocol_source_session_open(source, &open), SALTS_EINVAL);
    open.size = sizeof(open) + 1u;
    check_equal(turbo_flow_protocol_source_session_open(source, &open), SALTS_EINVAL);
    open.size = sizeof(open);
    open.abi_version++;
    check_equal(turbo_flow_protocol_source_session_open(source, &open), SALTS_EINVAL);
    open.abi_version = TURBO_FLOW_PROTOCOL_SOURCE_ABI_VERSION;
    check_equal(turbo_flow_protocol_source_session_open(source, &open), SALTS_OK);
    feed.size = sizeof(feed) - 1u;
    check_equal(
        turbo_flow_protocol_source_session_feed(source, 1u, 1u, frame, sizeof(frame), &feed),
        SALTS_EINVAL);
    feed = (turbo_flow_protocol_source_feed_result_t)TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    feed.size = sizeof(feed) + 1u;
    check_equal(
        turbo_flow_protocol_source_session_feed(source, 1u, 1u, frame, sizeof(frame), &feed),
        SALTS_EINVAL);
    feed = (turbo_flow_protocol_source_feed_result_t)TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    feed.abi_version++;
    check_equal(
        turbo_flow_protocol_source_session_feed(source, 1u, 1u, frame, sizeof(frame), &feed),
        SALTS_EINVAL);
    check_equal(probe.admit_calls, 0u);
    feed = (turbo_flow_protocol_source_feed_result_t)TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    check_equal(
        turbo_flow_protocol_source_session_feed(source, 1u, 1u, frame, sizeof(frame), &feed),
        SALTS_OK);
    check_equal(feed.accepted_size, sizeof(frame));
    check_equal(feed.frames_admitted, 1u);
    check_equal(probe.admit_calls, 1u);
    snapshot.size = sizeof(snapshot) - 1u;
    check_equal(turbo_flow_protocol_source_snapshot(source, &snapshot), SALTS_EINVAL);
    snapshot = (turbo_flow_protocol_source_snapshot_t)TURBO_FLOW_PROTOCOL_SOURCE_SNAPSHOT_INIT;
    snapshot.size = sizeof(snapshot) + 1u;
    check_equal(turbo_flow_protocol_source_snapshot(source, &snapshot), SALTS_EINVAL);
    snapshot = (turbo_flow_protocol_source_snapshot_t)TURBO_FLOW_PROTOCOL_SOURCE_SNAPSHOT_INIT;
    snapshot.abi_version++;
    check_equal(turbo_flow_protocol_source_snapshot(source, &snapshot), SALTS_EINVAL);
    snapshot = (turbo_flow_protocol_source_snapshot_t)TURBO_FLOW_PROTOCOL_SOURCE_SNAPSHOT_INIT;
    check_equal(turbo_flow_protocol_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.active_sessions, 1u);
    check_equal(snapshot.buffered_bytes, 0u);
    check_equal(turbo_flow_protocol_source_begin_shutdown(source), SALTS_OK);
    check_equal(turbo_flow_protocol_source_destroy(source), SALTS_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("validates the complete protocol version before comparing it") {
    char unterminated[TURBO_FLOW_PROTOCOL_VERSION_MAX + 1u];
    source_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_source_t *source = NULL;
    turbo_flow_protocol_source_session_open_request_t open =
        TURBO_FLOW_PROTOCOL_SOURCE_SESSION_OPEN_REQUEST_INIT;

    memset(unterminated, 'x', sizeof(unterminated));
    memcpy(unterminated, "test", sizeof("test") - 1u);
    check_equal(source_protocol_create(TURBO_FLOW_PROTOCOL_COAP, &probe, &protocol), SALTS_OK);
    check_equal(source_create(protocol, &probe, 1u, &source), SALTS_OK);
    open.session_id = 10u;
    open.generation = 1u;
    open.device_id = "sensor-10";
    open.protocol_version = unterminated;
    check_equal(turbo_flow_protocol_source_session_open(source, &open), SALTS_EMSGSIZE);
    check_equal(turbo_flow_protocol_source_begin_shutdown(source), SALTS_OK);
    check_equal(turbo_flow_protocol_source_destroy(source), SALTS_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("consumes a message frame only after synchronous admit succeeds") {
    const uint8_t frame[] = {0x42u};
    source_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_source_t *source = NULL;
    turbo_flow_protocol_source_feed_result_t result = TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    turbo_flow_protocol_source_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_SOURCE_SNAPSHOT_INIT;
    check_equal(source_protocol_create(TURBO_FLOW_PROTOCOL_COAP, &probe, &protocol), SALTS_OK);
    check_equal(source_create(protocol, &probe, 1u, &source), SALTS_OK);
    check_equal(source_session_open(source, 2u, "sensor-2"), SALTS_OK);
    check_equal(
        turbo_flow_protocol_source_session_feed(source, 2u, 1u, frame, sizeof(frame), &result),
        SALTS_OK);
    check_equal(result.accepted_size, sizeof(frame));
    check_equal(result.frames_admitted, 1u);
    check_false(result.backpressured);
    check_equal(probe.admit_calls, 1u);
    check_equal(probe.successful_admits, 1u);
    check_equal(probe.last_delivery_id, 1u);
    check_equal(probe.last_session_id, 2u);
    check_equal(probe.last_generation, 1u);
    check_equal(probe.last_operation, "frame");
    check_equal(probe.last_payload_size, sizeof(frame));
    check_equal(probe.last_payload, frame, sizeof(frame));
    check_equal(probe.codec_replies, 0u);
    check_equal(turbo_flow_protocol_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.buffered_bytes, 0u);
    check_equal(turbo_flow_protocol_source_begin_shutdown(source), SALTS_OK);
    check_equal(turbo_flow_protocol_source_destroy(source), SALTS_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("retains a capacity-rejected message frame until retry admits it") {
    const uint8_t frame[] = {0x43u};
    source_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_source_t *source = NULL;
    turbo_flow_protocol_source_feed_result_t first = TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    turbo_flow_protocol_source_feed_result_t retry = TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    turbo_flow_protocol_source_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_SOURCE_SNAPSHOT_INIT;
    probe.capacity_failures = 1u;
    check_equal(source_protocol_create(TURBO_FLOW_PROTOCOL_COAP, &probe, &protocol), SALTS_OK);
    check_equal(source_create(protocol, &probe, 1u, &source), SALTS_OK);
    check_equal(source_session_open(source, 3u, "sensor-3"), SALTS_OK);
    check_equal(
        turbo_flow_protocol_source_session_feed(source, 3u, 1u, frame, sizeof(frame), &first),
        SALTS_OK);
    check_equal(first.accepted_size, sizeof(frame));
    check_equal(first.frames_admitted, 0u);
    check_true(first.backpressured);
    check_equal(turbo_flow_protocol_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.buffered_bytes, sizeof(frame));
    check_equal(turbo_flow_protocol_source_session_feed(source, 3u, 1u, NULL, 0u, &retry),
                SALTS_OK);
    check_equal(retry.accepted_size, 0u);
    check_equal(retry.frames_admitted, 1u);
    check_false(retry.backpressured);
    check_equal(probe.admit_calls, 2u);
    check_equal(probe.successful_admits, 1u);
    snapshot = (turbo_flow_protocol_source_snapshot_t)TURBO_FLOW_PROTOCOL_SOURCE_SNAPSHOT_INIT;
    check_equal(turbo_flow_protocol_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.buffered_bytes, 0u);
    check_equal(turbo_flow_protocol_source_begin_shutdown(source), SALTS_OK);
    check_equal(turbo_flow_protocol_source_destroy(source), SALTS_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("never reinterprets codec capacity errors as Inbox backpressure") {
    static const int statuses[] = {SALTS_EBUSY, SALTS_ENOSPC, SALTS_ENOBUFS};
    const uint8_t frame[] = {0x46u};

    for (size_t index = 0u; index < sizeof(statuses) / sizeof(statuses[0]); ++index) {
      source_probe_t probe = {0};
      turbo_flow_protocol_t *protocol = NULL;
      turbo_flow_protocol_source_t *source = NULL;
      turbo_flow_protocol_source_feed_result_t result = TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
      turbo_flow_protocol_source_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_SOURCE_SNAPSHOT_INIT;

      probe.inspect_status = statuses[index];
      check_equal(source_protocol_create(TURBO_FLOW_PROTOCOL_COAP, &probe, &protocol), SALTS_OK);
      check_equal(source_create(protocol, &probe, 1u, &source), SALTS_OK);
      check_equal(source_session_open(source, 11u, "sensor-11"), SALTS_OK);
      check_equal(
          turbo_flow_protocol_source_session_feed(source, 11u, 1u, frame, sizeof(frame), &result),
          statuses[index]);
      check_false(result.backpressured);
      check_equal(result.frames_admitted, 0u);
      check_equal(probe.admit_calls, 0u);
      check_equal(turbo_flow_protocol_source_snapshot(source, &snapshot), SALTS_OK);
      check_equal(snapshot.buffered_bytes, sizeof(frame));
      check_equal(turbo_flow_protocol_source_session_close(source, 11u, 1u, statuses[index]),
                  SALTS_OK);
      check_equal(turbo_flow_protocol_source_begin_shutdown(source), SALTS_OK);
      check_equal(turbo_flow_protocol_source_destroy(source), SALTS_OK);
      turbo_flow_protocol_destroy(protocol);
    }
  }

  it("returns an ordinary provider failure and leaves its frame unconsumed") {
    const uint8_t frame[] = {0x44u};
    source_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_source_t *source = NULL;
    turbo_flow_protocol_source_feed_result_t result = TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    turbo_flow_protocol_source_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_SOURCE_SNAPSHOT_INIT;
    probe.admit_status = SALTS_EIO;
    check_equal(source_protocol_create(TURBO_FLOW_PROTOCOL_COAP, &probe, &protocol), SALTS_OK);
    check_equal(source_create(protocol, &probe, 1u, &source), SALTS_OK);
    check_equal(source_session_open(source, 4u, "sensor-4"), SALTS_OK);
    check_equal(
        turbo_flow_protocol_source_session_feed(source, 4u, 1u, frame, sizeof(frame), &result),
        SALTS_EIO);
    check_equal(result.accepted_size, sizeof(frame));
    check_equal(result.frames_admitted, 0u);
    check_false(result.backpressured);
    check_equal(turbo_flow_protocol_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.active_sessions, 1u);
    check_equal(snapshot.buffered_bytes, sizeof(frame));
    check_equal(turbo_flow_protocol_source_session_close(source, 4u, 1u, SALTS_EIO), SALTS_OK);
    check_equal(probe.closes, 1u);
    check_equal(probe.last_close_status, SALTS_EIO);
    check_equal(turbo_flow_protocol_source_begin_shutdown(source), SALTS_OK);
    check_equal(turbo_flow_protocol_source_destroy(source), SALTS_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("shutdown closes admitted sessions without waiting for graph work") {
    const uint8_t frame[] = {0x45u};
    source_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_source_t *source = NULL;
    turbo_flow_protocol_source_feed_result_t result = TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    turbo_flow_protocol_source_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_SOURCE_SNAPSHOT_INIT;
    check_equal(source_protocol_create(TURBO_FLOW_PROTOCOL_COAP, &probe, &protocol), SALTS_OK);
    check_equal(source_create(protocol, &probe, 1u, &source), SALTS_OK);
    check_equal(source_session_open(source, 5u, "sensor-5"), SALTS_OK);
    check_equal(
        turbo_flow_protocol_source_session_feed(source, 5u, 1u, frame, sizeof(frame), &result),
        SALTS_OK);
    check_equal(turbo_flow_protocol_source_begin_shutdown(source), SALTS_OK);
    check_equal(probe.closes, 1u);
    check_equal(probe.last_close_status, SALTS_ESHUTDOWN);
    check_equal(source_session_open(source, 6u, "sensor-6"), SALTS_ESHUTDOWN);
    result = (turbo_flow_protocol_source_feed_result_t)TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    check_equal(
        turbo_flow_protocol_source_session_feed(source, 5u, 1u, frame, sizeof(frame), &result),
        SALTS_ESHUTDOWN);
    check_equal(turbo_flow_protocol_source_snapshot(source, &snapshot), SALTS_OK);
    check_equal(snapshot.active_sessions, 0u);
    check_equal(snapshot.buffered_bytes, 0u);
    check_false(snapshot.accepting);
    check_equal(turbo_flow_protocol_source_destroy(source), SALTS_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("reassembles fragmented and coalesced stream frames") {
    uint8_t frames[50];
    source_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_source_t *source = NULL;
    turbo_flow_protocol_source_feed_result_t first = TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    turbo_flow_protocol_source_feed_result_t second = TURBO_FLOW_PROTOCOL_SOURCE_FEED_RESULT_INIT;
    source_gbt_frame(frames, 0x11u);
    source_gbt_frame(frames + 25u, 0x22u);
    check_equal(source_protocol_create(TURBO_FLOW_PROTOCOL_GBT_32960, &probe, &protocol), SALTS_OK);
    check_equal(source_create(protocol, &probe, 1u, &source), SALTS_OK);
    check_equal(source_session_open(source, 7u, NULL), SALTS_OK);
    check_equal(turbo_flow_protocol_source_session_feed(source, 7u, 1u, frames, 10u, &first),
                SALTS_OK);
    check_equal(first.frames_admitted, 0u);
    check_equal(turbo_flow_protocol_source_session_feed(source, 7u, 1u, frames + 10u, 40u, &second),
                SALTS_OK);
    check_equal(second.frames_admitted, 2u);
    check_equal(second.accepted_size, 40u);
    check_equal(probe.successful_admits, 2u);
    check_equal(probe.last_payload[24], frames[49]);
    check_equal(turbo_flow_protocol_source_begin_shutdown(source), SALTS_OK);
    check_equal(turbo_flow_protocol_source_destroy(source), SALTS_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("enforces configured session and memory bounds") {
    source_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_source_t *source = NULL;
    turbo_flow_protocol_source_config_t invalid = TURBO_FLOW_PROTOCOL_SOURCE_CONFIG_INIT;
    turbo_flow_protocol_source_ops_t ops = TURBO_FLOW_PROTOCOL_SOURCE_OPS_INIT;
    ops.admit = source_admit;
    invalid.max_sessions = 2u;
    invalid.max_frame_size = 64u;
    invalid.max_buffered_bytes = 64u;
    check_equal(source_protocol_create(TURBO_FLOW_PROTOCOL_COAP, &probe, &protocol), SALTS_OK);
    check_equal(turbo_flow_protocol_source_create(protocol, &invalid, &ops, &probe, &source),
                SALTS_ENOSPC);
    check_null(source);
    check_equal(source_create(protocol, &probe, 1u, &source), SALTS_OK);
    check_equal(source_session_open(source, 8u, "sensor-8"), SALTS_OK);
    check_equal(source_session_open(source, 9u, "sensor-9"), SALTS_ENOSPC);
    check_equal(turbo_flow_protocol_source_force_shutdown(source, SALTS_ECANCELED), SALTS_OK);
    check_equal(turbo_flow_protocol_source_destroy(source), SALTS_OK);
    turbo_flow_protocol_destroy(protocol);
  }
}
