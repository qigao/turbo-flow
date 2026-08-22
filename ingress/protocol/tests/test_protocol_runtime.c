#include "tinytest.h"
#include "turbo_error.h"
#include "turbo_flow_protocol.h"
#include "turbo_flow_protocol_plugin.h"
#include "turbo_flow_protocol_runtime.h"

#include <stddef.h>
#include <string.h>

typedef struct runtime_probe_s {
  size_t publishes;
  size_t settlements;
  size_t closes;
  size_t replies;
  size_t pending_publish;
  uint64_t last_delivery_id;
  int last_settlement_status;
  int last_close_status;
  char last_operation[TURBO_FLOW_PROTOCOL_OPERATION_MAX + 1u];
  uint8_t last_payload[64];
  size_t last_payload_size;
  uint8_t last_reply[8];
  size_t last_reply_size;
} runtime_probe_t;

static int runtime_inspect(void *ctx, const char *configured_version,
                           const turbo_flow_protocol_frame_view_t *frame,
                           turbo_flow_protocol_metadata_t *metadata) {
  (void)ctx;
  (void)configured_version;
  if (!frame || !metadata || !frame->data || frame->data_size == 0u) return TURBO_EPROTO;
  metadata->message_type = frame->data[0];
  metadata->sequence = frame->data_size;
  memcpy(metadata->operation, "frame", sizeof("frame"));
  if (!frame->device_id) memcpy(metadata->device_id, "frame-device", sizeof("frame-device"));
  return TURBO_OK;
}

static int runtime_codec_reply(void *ctx, const char *configured_version,
                               const turbo_flow_protocol_frame_view_t *request, int status,
                               turbo_flow_protocol_frame_output_t *output) {
  (void)ctx;
  (void)configured_version;
  if (!request || !output || !output->data || output->capacity < 2u) return TURBO_EINVAL;
  output->data[0] = UINT8_C(0xac);
  output->data[1] = status == TURBO_OK ? 0u : 1u;
  output->data_size = 2u;
  return TURBO_OK;
}

static int runtime_publish(void *ctx, const turbo_flow_protocol_publish_request_t *request,
                           turbo_flow_protocol_publish_disposition_t *disposition) {
  runtime_probe_t *probe = (runtime_probe_t *)ctx;
  if (!probe || !request || !request->message || !disposition) return TURBO_EINVAL;
  probe->publishes++;
  probe->last_delivery_id = request->delivery_id;
  if (request->message->payload_size > sizeof(probe->last_payload)) return TURBO_EMSGSIZE;
  memcpy(probe->last_payload, request->message->payload, request->message->payload_size);
  probe->last_payload_size = request->message->payload_size;
  memcpy(probe->last_operation, request->message->metadata.operation,
         sizeof(probe->last_operation));
  *disposition = probe->publishes == probe->pending_publish ? TURBO_FLOW_PROTOCOL_PUBLISH_PENDING
                                                            : TURBO_FLOW_PROTOCOL_PUBLISH_SETTLED;
  return TURBO_OK;
}

static void runtime_settled(void *ctx, uint64_t session_id, uint64_t generation,
                            uint64_t delivery_id, const turbo_flow_protocol_metadata_t *metadata,
                            int status) {
  runtime_probe_t *probe = (runtime_probe_t *)ctx;
  (void)session_id;
  (void)generation;
  (void)delivery_id;
  (void)metadata;
  probe->settlements++;
  probe->last_settlement_status = status;
}

static void runtime_closed(void *ctx, uint64_t session_id, uint64_t generation, int status) {
  runtime_probe_t *probe = (runtime_probe_t *)ctx;
  (void)session_id;
  (void)generation;
  probe->closes++;
  probe->last_close_status = status;
}

static int runtime_reply(void *ctx, uint64_t session_id, uint64_t generation, uint64_t delivery_id,
                         const turbo_flow_protocol_frame_output_t *frame) {
  runtime_probe_t *probe = (runtime_probe_t *)ctx;
  (void)session_id;
  (void)generation;
  (void)delivery_id;
  if (!probe || !frame || frame->data_size > sizeof(probe->last_reply)) return TURBO_EINVAL;
  memcpy(probe->last_reply, frame->data, frame->data_size);
  probe->last_reply_size = frame->data_size;
  probe->replies++;
  return TURBO_OK;
}

static int runtime_protocol_create(turbo_flow_protocol_kind_t protocol,
                                   turbo_flow_protocol_t **out) {
  turbo_flow_protocol_open_request_t request = TURBO_FLOW_PROTOCOL_OPEN_REQUEST_INIT;
  turbo_flow_protocol_codec_ops_t ops = TURBO_FLOW_PROTOCOL_CODEC_OPS_INIT;
  request.protocol = protocol;
  request.protocol_version = "test";
  request.max_frame_size = 64u;
  ops.inspect = runtime_inspect;
  ops.reply = runtime_codec_reply;
  return turbo_flow_protocol_create(
      &request, "runtime-test", "test",
      TURBO_FLOW_PROTOCOL_CAP_INGRESS | TURBO_FLOW_PROTOCOL_CAP_EGRESS |
          TURBO_FLOW_PROTOCOL_CAP_RAW_PRESERVE | TURBO_FLOW_PROTOCOL_CAP_PROTOCOL_REPLY,
      &ops, NULL, out);
}

static int runtime_create(turbo_flow_protocol_t *protocol, runtime_probe_t *probe,
                          size_t max_sessions, turbo_flow_protocol_runtime_t **out) {
  turbo_flow_protocol_runtime_config_t config = TURBO_FLOW_PROTOCOL_RUNTIME_CONFIG_INIT;
  turbo_flow_protocol_runtime_ops_t ops = TURBO_FLOW_PROTOCOL_RUNTIME_OPS_INIT;
  config.max_sessions = max_sessions;
  config.max_frame_size = 64u;
  config.max_buffered_bytes = (max_sessions + 1u) * config.max_frame_size;
  ops.publish = runtime_publish;
  ops.settled = runtime_settled;
  ops.session_closed = runtime_closed;
  ops.reply = runtime_reply;
  return turbo_flow_protocol_runtime_create(protocol, &config, &ops, probe, out);
}

static int runtime_session_open(turbo_flow_protocol_runtime_t *runtime, uint64_t session_id,
                                const char *device_id) {
  turbo_flow_protocol_session_open_request_t request =
      TURBO_FLOW_PROTOCOL_SESSION_OPEN_REQUEST_INIT;
  request.session_id = session_id;
  request.generation = 1u;
  request.device_id = device_id;
  request.protocol_version = "test";
  return turbo_flow_protocol_runtime_session_open(runtime, &request);
}

static void runtime_gbt_frame(uint8_t frame[25], uint8_t marker) {
  memset(frame, marker, 25u);
  frame[0] = 0x23u;
  frame[1] = 0x23u;
  frame[22] = 0u;
  frame[23] = 0u;
}

spec("protocol session runtime") {
  it("accepts the pre-reply runtime callback-table prefix") {
    runtime_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_runtime_t *runtime = NULL;
    turbo_flow_protocol_runtime_config_t config = TURBO_FLOW_PROTOCOL_RUNTIME_CONFIG_INIT;
    turbo_flow_protocol_runtime_ops_t ops = TURBO_FLOW_PROTOCOL_RUNTIME_OPS_INIT;
    check_equal(runtime_protocol_create(TURBO_FLOW_PROTOCOL_COAP, &protocol), TURBO_OK);
    config.max_sessions = 1u;
    config.max_frame_size = 64u;
    config.max_buffered_bytes = 128u;
    ops.size = offsetof(turbo_flow_protocol_runtime_ops_t, reply);
    ops.publish = runtime_publish;
    check_equal(turbo_flow_protocol_runtime_create(protocol, &config, &ops, &probe, &runtime),
                 TURBO_OK);
    check_equal(turbo_flow_protocol_runtime_begin_shutdown(runtime), TURBO_OK);
    check_equal(turbo_flow_protocol_runtime_destroy(runtime), TURBO_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("maps one message-boundary frame and settles it inline") {
    const uint8_t frame[] = {0x42u};
    runtime_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_runtime_t *runtime = NULL;
    turbo_flow_protocol_feed_result_t result = TURBO_FLOW_PROTOCOL_FEED_RESULT_INIT;
    check_equal(runtime_protocol_create(TURBO_FLOW_PROTOCOL_COAP, &protocol), TURBO_OK);
    check_equal(runtime_create(protocol, &probe, 2u, &runtime), TURBO_OK);
    check_equal(runtime_session_open(runtime, 1u, "sensor-1"), TURBO_OK);
    check_equal(
        turbo_flow_protocol_runtime_session_feed(runtime, 1u, 1u, frame, sizeof(frame), &result),
        TURBO_OK);
    check_equal(result.accepted_size, sizeof(frame));
    check_equal(result.frames_dispatched, 1u);
    check_equal(probe.publishes, 1u);
    check_equal(probe.settlements, 1u);
    check_equal(probe.replies, 1u);
    check_equal(probe.last_reply_size, 2u);
    check_equal(probe.last_reply[1], 0u);
    check_equal(probe.last_operation, "frame");
    check_equal(probe.last_payload_size, sizeof(frame));
    check_equal(probe.last_payload, frame, sizeof(frame));
    check_equal(turbo_flow_protocol_runtime_begin_shutdown(runtime), TURBO_OK);
    check_equal(turbo_flow_protocol_runtime_destroy(runtime), TURBO_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("reassembles a fragmented GB/T 32960 stream frame") {
    uint8_t frame[25];
    runtime_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_runtime_t *runtime = NULL;
    turbo_flow_protocol_feed_result_t first = TURBO_FLOW_PROTOCOL_FEED_RESULT_INIT;
    turbo_flow_protocol_feed_result_t second = TURBO_FLOW_PROTOCOL_FEED_RESULT_INIT;
    runtime_gbt_frame(frame, 0x11u);
    check_equal(runtime_protocol_create(TURBO_FLOW_PROTOCOL_GBT_32960, &protocol), TURBO_OK);
    check_equal(runtime_create(protocol, &probe, 1u, &runtime), TURBO_OK);
    check_equal(runtime_session_open(runtime, 2u, NULL), TURBO_OK);
    check_equal(turbo_flow_protocol_runtime_session_feed(runtime, 2u, 1u, frame, 10u, &first),
                 TURBO_OK);
    check_equal(first.frames_dispatched, 0u);
    check_equal(turbo_flow_protocol_runtime_session_feed(runtime, 2u, 1u, frame + 10u,
                                                          sizeof(frame) - 10u, &second),
                 TURBO_OK);
    check_equal(second.frames_dispatched, 1u);
    check_equal(probe.publishes, 1u);
    check_equal(turbo_flow_protocol_runtime_begin_shutdown(runtime), TURBO_OK);
    check_equal(turbo_flow_protocol_runtime_destroy(runtime), TURBO_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("consumes coalesced stream frames before compacting one fragmented tail") {
    uint8_t frames[50];
    runtime_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_runtime_t *runtime = NULL;
    turbo_flow_protocol_feed_result_t first = TURBO_FLOW_PROTOCOL_FEED_RESULT_INIT;
    turbo_flow_protocol_feed_result_t second = TURBO_FLOW_PROTOCOL_FEED_RESULT_INIT;
    runtime_gbt_frame(frames, 0x11u);
    runtime_gbt_frame(frames + 25u, 0x22u);
    check_equal(runtime_protocol_create(TURBO_FLOW_PROTOCOL_GBT_32960, &protocol), TURBO_OK);
    check_equal(runtime_create(protocol, &probe, 1u, &runtime), TURBO_OK);
    check_equal(runtime_session_open(runtime, 8u, NULL), TURBO_OK);
    check_equal(turbo_flow_protocol_runtime_session_feed(runtime, 8u, 1u, frames, 40u, &first),
                 TURBO_OK);
    check_equal(first.frames_dispatched, 1u);
    check_equal(first.accepted_size, 40u);
    check_equal(
        turbo_flow_protocol_runtime_session_feed(runtime, 8u, 1u, frames + 40u, 10u, &second),
        TURBO_OK);
    check_equal(second.frames_dispatched, 1u);
    check_equal(second.accepted_size, 10u);
    check_equal(probe.publishes, 2u);
    check_equal(probe.settlements, 2u);
    check_equal(probe.last_payload[24], frames[49]);
    check_equal(turbo_flow_protocol_runtime_begin_shutdown(runtime), TURBO_OK);
    check_equal(turbo_flow_protocol_runtime_destroy(runtime), TURBO_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("buffers a following stream frame behind one pending settlement") {
    uint8_t frames[50];
    runtime_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_runtime_t *runtime = NULL;
    turbo_flow_protocol_feed_result_t feed = TURBO_FLOW_PROTOCOL_FEED_RESULT_INIT;
    turbo_flow_protocol_feed_result_t settle = TURBO_FLOW_PROTOCOL_FEED_RESULT_INIT;
    turbo_flow_protocol_runtime_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_RUNTIME_SNAPSHOT_INIT;
    runtime_gbt_frame(frames, 0x11u);
    runtime_gbt_frame(frames + 25u, 0x22u);
    probe.pending_publish = 1u;
    check_equal(runtime_protocol_create(TURBO_FLOW_PROTOCOL_GBT_32960, &protocol), TURBO_OK);
    check_equal(runtime_create(protocol, &probe, 1u, &runtime), TURBO_OK);
    check_equal(runtime_session_open(runtime, 3u, NULL), TURBO_OK);
    check_equal(
        turbo_flow_protocol_runtime_session_feed(runtime, 3u, 1u, frames, sizeof(frames), &feed),
        TURBO_OK);
    check_equal(feed.frames_dispatched, 1u);
    check_true(feed.backpressured);
    check_equal(feed.pending_delivery_id, 1u);
    check_equal(probe.replies, 0u);
    check_equal(turbo_flow_protocol_runtime_snapshot(runtime, &snapshot), TURBO_OK);
    check_equal(snapshot.pending_settlements, 1u);
    check_equal(snapshot.buffered_bytes, sizeof(frames));
    check_equal(turbo_flow_protocol_runtime_settle(runtime, 1u, TURBO_OK, &settle), TURBO_OK);
    check_equal(settle.frames_dispatched, 1u);
    check_equal(probe.publishes, 2u);
    check_equal(probe.replies, 2u);
    snapshot = (turbo_flow_protocol_runtime_snapshot_t)TURBO_FLOW_PROTOCOL_RUNTIME_SNAPSHOT_INIT;
    check_equal(turbo_flow_protocol_runtime_snapshot(runtime, &snapshot), TURBO_OK);
    check_equal(snapshot.pending_settlements, 0u);
    check_equal(snapshot.buffered_bytes, 0u);
    check_equal(turbo_flow_protocol_runtime_begin_shutdown(runtime), TURBO_OK);
    check_equal(turbo_flow_protocol_runtime_destroy(runtime), TURBO_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("closes admission and waits only for admitted settlement ownership") {
    const uint8_t frame[] = {0x42u};
    runtime_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_runtime_t *runtime = NULL;
    turbo_flow_protocol_feed_result_t feed = TURBO_FLOW_PROTOCOL_FEED_RESULT_INIT;
    turbo_flow_protocol_feed_result_t settle = TURBO_FLOW_PROTOCOL_FEED_RESULT_INIT;
    turbo_flow_protocol_runtime_snapshot_t snapshot = TURBO_FLOW_PROTOCOL_RUNTIME_SNAPSHOT_INIT;
    probe.pending_publish = 1u;
    check_equal(runtime_protocol_create(TURBO_FLOW_PROTOCOL_COAP, &protocol), TURBO_OK);
    check_equal(runtime_create(protocol, &probe, 1u, &runtime), TURBO_OK);
    check_equal(runtime_session_open(runtime, 4u, "sensor-4"), TURBO_OK);
    check_equal(
        turbo_flow_protocol_runtime_session_feed(runtime, 4u, 1u, frame, sizeof(frame), &feed),
        TURBO_OK);
    check_equal(turbo_flow_protocol_runtime_begin_shutdown(runtime), TURBO_EBUSY);
    check_equal(runtime_session_open(runtime, 5u, "sensor-5"), TURBO_ESHUTDOWN);
    check_equal(
        turbo_flow_protocol_runtime_settle(runtime, feed.pending_delivery_id, TURBO_OK, &settle),
        TURBO_OK);
    check_equal(turbo_flow_protocol_runtime_snapshot(runtime, &snapshot), TURBO_OK);
    check_equal(snapshot.active_sessions, 0u);
    check_equal(snapshot.pending_settlements, 0u);
    check_false(snapshot.accepting);
    check_equal(probe.closes, 1u);
    check_equal(probe.last_close_status, TURBO_ESHUTDOWN);
    check_equal(turbo_flow_protocol_runtime_destroy(runtime), TURBO_OK);
    turbo_flow_protocol_destroy(protocol);
  }

  it("enforces the configured session and memory bounds") {
    runtime_probe_t probe = {0};
    turbo_flow_protocol_t *protocol = NULL;
    turbo_flow_protocol_runtime_t *runtime = NULL;
    turbo_flow_protocol_runtime_config_t invalid = TURBO_FLOW_PROTOCOL_RUNTIME_CONFIG_INIT;
    turbo_flow_protocol_runtime_ops_t ops = TURBO_FLOW_PROTOCOL_RUNTIME_OPS_INIT;
    ops.publish = runtime_publish;
    invalid.max_sessions = 2u;
    invalid.max_frame_size = 64u;
    invalid.max_buffered_bytes = 64u;
    check_equal(runtime_protocol_create(TURBO_FLOW_PROTOCOL_COAP, &protocol), TURBO_OK);
    check_equal(turbo_flow_protocol_runtime_create(protocol, &invalid, &ops, &probe, &runtime),
                 TURBO_ENOSPC);
    check_null(runtime);
    check_equal(runtime_create(protocol, &probe, 1u, &runtime), TURBO_OK);
    check_equal(runtime_session_open(runtime, 6u, "sensor-6"), TURBO_OK);
    check_equal(runtime_session_open(runtime, 7u, "sensor-7"), TURBO_ENOSPC);
    check_equal(turbo_flow_protocol_runtime_force_shutdown(runtime, TURBO_ECANCELED), TURBO_OK);
    check_equal(turbo_flow_protocol_runtime_destroy(runtime), TURBO_OK);
    turbo_flow_protocol_destroy(protocol);
  }
}
