#ifndef FLOW_PROTOCOL_PLUGIN_SUPPORT_H
#define FLOW_PROTOCOL_PLUGIN_SUPPORT_H

#include "turbo_flow_plugin_protocol.h"
#include "turbo_flow_protocol.h"
#include "turbo_flow_protocol_plugin.h"

typedef struct flow_protocol_plugin_descriptor_s {
  const char *name;
  turbo_flow_protocol_kind_t protocol;
  const char *default_version;
  const char *const *versions;
  size_t version_count;
  turbo_flow_protocol_inspect_fn inspect;
  turbo_flow_protocol_reply_fn reply;
  turbo_flow_protocol_encode_fn encode;
  void *inspect_ctx;
  /** Optional append-only semantic ingress decoder; NULL preserves legacy raw-only decode. */
  turbo_flow_protocol_decode_semantic_fn decode_semantic;
} flow_protocol_plugin_descriptor_t;

typedef struct flow_protocol_root_plugin_s {
  const turbo_flow_plugin_host_v1_t *host;
} flow_protocol_root_plugin_t;

static inline int flow_protocol_root_load(const turbo_flow_plugin_host_v1_t *host,
                                          void **plugin_out) {
  flow_protocol_root_plugin_t *plugin;
  if (!plugin_out) return SALTS_EINVAL;
  *plugin_out = NULL;
  if (!host || host->size != sizeof(*host) ||
      host->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||
      host->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR || !host->allocate ||
      !host->deallocate)
    return SALTS_EINVAL;
  plugin = (flow_protocol_root_plugin_t *)host->allocate(host->ctx, sizeof(*plugin));
  if (!plugin) return SALTS_ENOMEM;
  plugin->host = host;
  *plugin_out = plugin;
  return SALTS_OK;
}

static inline int flow_protocol_root_quiesce(void *plugin, uint64_t timeout_ms) {
  (void)timeout_ms;
  return plugin ? SALTS_OK : SALTS_EINVAL;
}

static inline int flow_protocol_root_shutdown(void *plugin) {
  return plugin ? SALTS_OK : SALTS_EINVAL;
}

static inline void flow_protocol_root_destroy(void *value) {
  flow_protocol_root_plugin_t *plugin = (flow_protocol_root_plugin_t *)value;
  const turbo_flow_plugin_host_v1_t *host;
  if (!plugin) return;
  host = plugin->host;
  plugin->host = NULL;
  host->deallocate(host->ctx, plugin);
}

#define FLOW_PROTOCOL_DEFINE_UNIFIED_ROOT_BASE(plugin_id_literal, plugin_version_literal,          \
                                               capability_value, provider_type, provider_init,     \
                                               add_provider_member, api_value)                     \
  static int flow_protocol_root_register(                                                          \
      void *plugin, const turbo_flow_plugin_registration_v1_t *registration) {                     \
    provider_type provider = provider_init;                                                        \
    if (!plugin || !registration || registration->size != sizeof(*registration) ||                 \
        registration->abi_major != TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR ||                          \
        registration->abi_minor != TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR ||                          \
        !registration->add_provider_member)                                                        \
      return SALTS_EINVAL;                                                                         \
    provider.provider = (api_value);                                                               \
    return registration->add_provider_member(registration->ctx, &provider);                        \
  }                                                                                                \
  static const turbo_flow_plugin_api_v1_t FLOW_PROTOCOL_ROOT_API = {                               \
      sizeof(turbo_flow_plugin_api_v1_t),                                                          \
      TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR,                                                         \
      TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR,                                                         \
      (plugin_id_literal),                                                                         \
      (plugin_version_literal),                                                                    \
      (capability_value),                                                                          \
      flow_protocol_root_load,                                                                     \
      flow_protocol_root_register,                                                                 \
      flow_protocol_root_quiesce,                                                                  \
      flow_protocol_root_shutdown,                                                                 \
      flow_protocol_root_destroy};                                                                 \
  TURBO_FLOW_PLUGIN_ENTRY const turbo_flow_plugin_api_v1_t *turbo_flow_plugin_get_api(void) {      \
    return &FLOW_PROTOCOL_ROOT_API;                                                                \
  }

#define FLOW_PROTOCOL_DEFINE_UNIFIED_ROOT(plugin_id_literal, plugin_version_literal, api_value)    \
  FLOW_PROTOCOL_DEFINE_UNIFIED_ROOT_BASE(                                                          \
      plugin_id_literal, plugin_version_literal, TURBO_FLOW_PLUGIN_CAP_PROTOCOL,                   \
      turbo_flow_plugin_protocol_provider_v1_t, TURBO_FLOW_PLUGIN_PROTOCOL_PROVIDER_V1_INIT,       \
      add_protocol_provider, api_value)

#define FLOW_PROTOCOL_BUSINESS_DEFINE_UNIFIED_ROOT(plugin_id_literal, plugin_version_literal,      \
                                                   api_value)                                      \
  FLOW_PROTOCOL_DEFINE_UNIFIED_ROOT_BASE(                                                          \
      plugin_id_literal, plugin_version_literal, TURBO_FLOW_PLUGIN_CAP_PROTOCOL_BUSINESS,          \
      turbo_flow_plugin_business_provider_v1_t, TURBO_FLOW_PLUGIN_BUSINESS_PROVIDER_V1_INIT,       \
      add_business_provider, api_value)

int flow_protocol_plugin_open(void *ctx, const turbo_flow_protocol_open_request_t *request,
                              turbo_flow_protocol_service_t *service);
void flow_protocol_plugin_close(void *ctx, turbo_flow_protocol_service_t *service);

int flow_protocol_metadata_text(char *out, size_t capacity, const char *text);
int flow_protocol_metadata_text_n(char *out, size_t capacity, const char *text, size_t length);
int flow_protocol_metadata_format(char *out, size_t capacity, const char *format, unsigned value);
int flow_protocol_coap_inspect(const turbo_flow_protocol_frame_view_t *frame,
                               turbo_flow_protocol_metadata_t *metadata, int lwm2m);
int flow_protocol_coap_reply(const turbo_flow_protocol_frame_view_t *request, int status,
                             turbo_flow_protocol_frame_output_t *output, int lwm2m,
                             uint16_t separate_message_id);
int flow_protocol_coap_encode(const turbo_flow_protocol_command_view_t *command,
                              turbo_flow_protocol_frame_output_t *output, int lwm2m);

#endif /* FLOW_PROTOCOL_PLUGIN_SUPPORT_H */
