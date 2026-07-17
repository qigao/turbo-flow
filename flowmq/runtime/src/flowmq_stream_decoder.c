#include "flowmq_stream_decoder.h"

#include "turbo_error.h"

#include <string.h>

int flowmq_stream_decoder_prepare(flowmq_stream_decoder_t *stream, size_t max_frame_size) {
  size_t encoded_limit;
  int rc;
  if (!stream || max_frame_size == 0u) return TURBO_EINVAL;
  if (stream->initialized)
    return stream->max_frame_size == max_frame_size ? TURBO_OK : TURBO_EALREADY;
  rc = flowmq_protocol_encoded_size_limit(max_frame_size, &encoded_limit);
  if (rc != TURBO_OK) return rc;
  rc = turbo_byte_buffer_init(&stream->buffer, encoded_limit);
  if (rc != TURBO_OK) return rc;
  stream->max_frame_size = max_frame_size;
  stream->initialized = 1;
  return TURBO_OK;
}

void flowmq_stream_decoder_destroy(flowmq_stream_decoder_t *stream) {
  if (!stream) return;
  if (stream->initialized) turbo_byte_buffer_destroy(&stream->buffer);
  memset(stream, 0, sizeof(*stream));
}

size_t flowmq_stream_decoder_available(const flowmq_stream_decoder_t *stream) {
  return stream && stream->initialized ? turbo_byte_buffer_available(&stream->buffer) : 0u;
}

int flowmq_stream_decoder_append(flowmq_stream_decoder_t *stream, const void *data, size_t size) {
  if (!stream || !stream->initialized) return TURBO_EINVAL;
  return turbo_byte_buffer_append(&stream->buffer, data, size);
}

int flowmq_stream_decoder_next(flowmq_stream_decoder_t *stream, flowmq_protocol_frame_t *frame,
                               size_t *consumed) {
  turbo_byte_buffer_view_t view;
  int rc;
  if (!stream || !stream->initialized || !frame || !consumed) return TURBO_EINVAL;
  rc = turbo_byte_buffer_view(&stream->buffer, &view);
  if (rc != TURBO_OK) return rc;
  return flowmq_protocol_decode_frame((const char *)view.data, view.size, stream->max_frame_size,
                                      frame, consumed);
}

int flowmq_stream_decoder_consume(flowmq_stream_decoder_t *stream, size_t count) {
  if (!stream || !stream->initialized) return TURBO_EINVAL;
  return turbo_byte_buffer_consume(&stream->buffer, count);
}
