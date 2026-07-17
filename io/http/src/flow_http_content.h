#ifndef FLOW_HTTP_CONTENT_H
#define FLOW_HTTP_CONTENT_H

#include "turbo_flow.h"
#include "turbo_thread.h"

typedef struct flow_http_content_cache_s {
  turbo_flow_content_descriptor_t descriptors[8];
  int states[8];
  turbo_flow_domain_t domain;
  turbo_flow_content_profile_t profile;
  char identity[TURBO_FLOW_CONTENT_IDENTITY_MAX + 1u];
  turbo_flow_content_binding_t binding;
  char schema_name[TURBO_FLOW_CONTENT_SCHEMA_NAME_MAX + 1u];
  char type_name[TURBO_FLOW_CONTENT_TYPE_NAME_MAX + 1u];
  turbo_mutex_t lock;
  int lock_initialized;
} flow_http_content_cache_t;

int flow_http_content_cache_init(flow_http_content_cache_t *cache,
                                 turbo_flow_content_profile_t profile, const char *identity,
                                 const turbo_flow_content_binding_t *binding);
void flow_http_content_cache_destroy(flow_http_content_cache_t *cache);
int flow_http_content_cache_get(flow_http_content_cache_t *cache, const char *media_type,
                                const turbo_flow_content_descriptor_t **descriptor_out);
int flow_http_register_client_module_contract(turbo_flow_t *flow);
int flow_http_register_server_module_contract(turbo_flow_t *flow);

#endif
