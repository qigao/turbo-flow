#include "flow_http_content.h"

#include <string.h>

static int flow_http_copy(char *dst, size_t capacity, const char *src) {
  size_t len;
  if (!dst || capacity == 0u) return TURBO_EINVAL;
  if (!src) {
    dst[0] = '\0';
    return TURBO_OK;
  }
  len = strlen(src);
  if (len >= capacity) return TURBO_ENOSPC;
  memcpy(dst, src, len + 1u);
  return TURBO_OK;
}

int flow_http_content_cache_init(flow_http_content_cache_t *cache,
                                 turbo_flow_content_profile_t profile, const char *identity,
                                 const turbo_flow_content_binding_t *binding) {
  int fields = 0;
  int rc;
  if (!cache || !identity || !identity[0]) return TURBO_EINVAL;
  memset(cache, 0, sizeof(*cache));
  cache->domain = TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  cache->profile = profile;
  rc = flow_http_copy(cache->identity, sizeof(cache->identity), identity);
  if (rc != TURBO_OK) return rc;
  cache->binding = (turbo_flow_content_binding_t)TURBO_FLOW_CONTENT_BINDING_INIT;
  if (binding) {
    if (binding->size < sizeof(*binding) || binding->schema.size < sizeof(binding->schema)) {
      return TURBO_EINVAL;
    }
    fields = (binding->schema.schema_name && binding->schema.schema_name[0]) +
             (binding->schema.type_name && binding->schema.type_name[0]) +
             (binding->schema.schema_version != 0u);
    if (fields != 0 && fields != 3) return TURBO_EINVAL;
    if (fields == 3 && !binding->registry) return TURBO_EINVAL;
    cache->binding.registry = binding->registry;
    if (fields == 3) {
      rc = flow_http_copy(cache->schema_name, sizeof(cache->schema_name),
                          binding->schema.schema_name);
      if (rc == TURBO_OK) {
        rc = flow_http_copy(cache->type_name, sizeof(cache->type_name), binding->schema.type_name);
      }
      if (rc != TURBO_OK) return rc;
      cache->binding.schema.schema_name = cache->schema_name;
      cache->binding.schema.type_name = cache->type_name;
      cache->binding.schema.schema_version = binding->schema.schema_version;
    }
  }
  turbo_mutex_init(&cache->lock);
  cache->lock_initialized = 1;
  return TURBO_OK;
}

void flow_http_content_cache_destroy(flow_http_content_cache_t *cache) {
  if (!cache) return;
  if (cache->lock_initialized) turbo_mutex_destroy(&cache->lock);
  memset(cache, 0, sizeof(*cache));
}

int flow_http_content_cache_get(flow_http_content_cache_t *cache, const char *media_type,
                                const turbo_flow_content_descriptor_t **descriptor_out) {
  turbo_flow_data_encoding_t encoding;
  const char *normalized = NULL;
  size_t slot;
  int rc;
  if (descriptor_out) *descriptor_out = NULL;
  if (!cache || !descriptor_out || !cache->lock_initialized) return TURBO_EINVAL;
  rc = turbo_flow_content_media_type_normalize(media_type, &encoding, &normalized);
  if (rc != TURBO_OK) {
    return rc == TURBO_ENOENT && cache->binding.schema.schema_version != 0u ? TURBO_EPROTO : rc;
  }
  if (strcmp(normalized, "application/json") == 0) slot = 0u;
  else if (strcmp(normalized, "text/csv") == 0) slot = 1u;
  else if (strcmp(normalized, "application/xml") == 0) slot = 2u;
  else if (strcmp(normalized, "text/plain") == 0) slot = 3u;
  else if (strcmp(normalized, "application/octet-stream") == 0) slot = 4u;
  else if (strcmp(normalized, "application/vnd.tbe") == 0) slot = 5u;
  else if (strcmp(normalized, "application/vnd.turboflow.pg-rowset+json") == 0) slot = 6u;
  else slot = 7u;
  if (slot >= sizeof(cache->states) / sizeof(cache->states[0])) return TURBO_EINVAL;
  turbo_mutex_lock(&cache->lock);
  if (cache->states[slot] == 0) {
    rc = turbo_flow_content_descriptor_from_media(&cache->descriptors[slot], cache->domain,
                                                  cache->profile, normalized, cache->identity,
                                                  &cache->binding);
    cache->states[slot] = rc == TURBO_OK ? 1 : rc;
  }
  rc = cache->states[slot] == 1 ? TURBO_OK : cache->states[slot];
  if (rc == TURBO_OK) *descriptor_out = &cache->descriptors[slot];
  turbo_mutex_unlock(&cache->lock);
  return rc;
}
