#include <turbo_flow.h>
#include <turbo_flow_cnet.h>

int main(void) {
  turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
  turbo_flow_run_result_t run_result = TURBO_FLOW_RUN_RESULT_INIT;
  turbo_flow_cnet_stream_source_config_t cnet_config = TURBO_FLOW_CNET_STREAM_SOURCE_CONFIG_INIT;
  turbo_flow_cnet_listener_source_config_t listener_config =
      TURBO_FLOW_CNET_LISTENER_SOURCE_CONFIG_INIT;
  turbo_flow_cnet_listener_source_snapshot_t listener_snapshot =
      TURBO_FLOW_CNET_LISTENER_SOURCE_SNAPSHOT_INIT;
  int (*listener_open)(const turbo_flow_cnet_listener_source_config_t *,
                       turbo_flow_cnet_listener_source_t **) = turbo_flow_cnet_listener_source_open;
  int (*listener_request)(turbo_flow_cnet_listener_source_t *, size_t) =
      turbo_flow_cnet_listener_source_request;
  int (*listener_poll)(turbo_flow_cnet_listener_source_t *, uint32_t,
                       turbo_flow_cnet_listener_source_snapshot_t *) =
      turbo_flow_cnet_listener_source_poll;
  int (*listener_snapshot_copy)(const turbo_flow_cnet_listener_source_t *,
                                turbo_flow_cnet_listener_source_snapshot_t *) =
      turbo_flow_cnet_listener_source_snapshot;
  int (*listener_stop)(turbo_flow_cnet_listener_source_t *, uint32_t) =
      turbo_flow_cnet_listener_source_stop;
  int (*listener_destroy)(turbo_flow_cnet_listener_source_t *) =
      turbo_flow_cnet_listener_source_destroy;
  turbo_flow_t *flow = turbo_flow_create();
  if (!flow || run_config.version != TURBO_FLOW_RUN_API_VERSION ||
      run_result.version != TURBO_FLOW_RUN_API_VERSION ||
      cnet_config.version != TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION ||
      listener_config.version != TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION ||
      listener_snapshot.version != TURBO_FLOW_CNET_LISTENER_SOURCE_API_VERSION || !listener_open ||
      !listener_request || !listener_poll || !listener_snapshot_copy || !listener_stop ||
      !listener_destroy || !turbo_flow_message_type())
    return 1;
  turbo_flow_destroy(flow);
  return 0;
}
